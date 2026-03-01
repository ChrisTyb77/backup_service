#!/usr/bin/env python3
"""rclone backup wrapper — runs backups defined in a YAML config file."""

import argparse
import json
import logging
import shutil
import subprocess
import sys
import tempfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import yaml

log = logging.getLogger("backup")

BACKUP_TIMESTAMP = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")


def load_config(path: Path) -> dict:
    with path.open() as f:
        return yaml.safe_load(f)


def build_rclone_cmd(
    source: str,
    destination: str,
    operation: str,
    global_opts: dict,
    job_opts: dict,
    dry_run: bool,
) -> list[str]:
    cmd = ["rclone", operation, source, destination]

    merged_opts = {**global_opts, **job_opts}
    for key, value in merged_opts.items():
        flag = f"--{key}"
        if isinstance(value, bool):
            if value:
                cmd.append(flag)
        elif isinstance(value, list):
            for item in value:
                cmd.extend([flag, str(item)])
        else:
            cmd.extend([flag, str(value)])

    if dry_run:
        cmd.append("--dry-run")

    return cmd


def _expand(path: str) -> str:
    """Expand ~ in local paths; leave rclone remote paths (containing ':') unchanged."""
    return str(Path(path).expanduser()) if ":" not in path else path


def _resolve_remotes(job: dict, remotes: dict) -> dict:
    """Returns {name: path_str} for remotes this job should use."""
    names = job.get("remotes")
    selected = {n: remotes[n] for n in names if n in remotes} if names else remotes
    return {name: _expand(remote["path"]) for name, remote in selected.items()}


def _run_rclone(cmd: list[str], remote_name: str, source: str) -> bool:
    log.debug("Command: %s", " ".join(cmd))
    result = subprocess.run(cmd)
    if result.returncode != 0:
        log.error("[%s] rclone failed (exit %d) for '%s'", remote_name, result.returncode, source)
        return False
    return True


def _is_version_dir(name: str) -> bool:
    try:
        datetime.strptime(name, "%Y-%m-%dT%H-%M-%S")
        return True
    except ValueError:
        return False


def _prune_old_versions(versions_root: str, keep: int, dry_run: bool) -> None:
    """Delete oldest timestamped subdirs under versions_root, keeping `keep` most recent."""
    local_root = Path(versions_root)
    if local_root.is_absolute():
        _prune_local(local_root, keep, dry_run)
    else:
        _prune_remote(versions_root, keep, dry_run)


def _prune_local(root: Path, keep: int, dry_run: bool) -> None:
    if not root.exists():
        return
    version_dirs = sorted(d.name for d in root.iterdir() if d.is_dir() and _is_version_dir(d.name))
    for dirname in version_dirs[:-keep] if len(version_dirs) > keep else []:
        target = root / dirname
        log.info("Pruning old version: %s", target)
        if not dry_run:
            shutil.rmtree(target)


def _prune_remote(versions_root: str, keep: int, dry_run: bool) -> None:
    result = subprocess.run(
        ["rclone", "lsjson", versions_root, "--dirs-only", "--no-modtime"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        log.warning("Could not list versions at %s: %s", versions_root, result.stderr.strip())
        return

    try:
        entries = json.loads(result.stdout)
    except json.JSONDecodeError:
        log.warning("Could not parse version listing for %s", versions_root)
        return

    version_dirs = sorted(e["Path"] for e in entries if _is_version_dir(e["Path"]))
    for dirname in version_dirs[:-keep] if len(version_dirs) > keep else []:
        target = f"{versions_root}/{dirname}"
        log.info("Pruning old version: %s", target)
        cmd = ["rclone", "purge", target]
        if dry_run:
            cmd.append("--dry-run")
        subprocess.run(cmd)


def _run_command(label: str, command: str, dry_run: bool) -> bool:
    log.info("Running %s: %s", label, command)
    if dry_run:
        return True
    result = subprocess.run(command, shell=True)
    if result.returncode != 0:
        log.error("%s failed (exit %d).", label, result.returncode)
        return False
    return True


def run_job(job: dict, remotes: dict, global_opts: dict, global_keep: int, dry_run: bool) -> bool:
    job_opts = job.get("options", {})
    target_remotes = _resolve_remotes(job, remotes)
    keep_versions = job.get("keep_versions", global_keep)

    job_id = job.get("name", job.get("source", "?"))
    if not target_remotes:
        log.warning("Job '%s': no matching remotes found, skipping.", job_id)
        return True

    pre = job.get("pre_command")
    post = job.get("post_command")

    if pre and not _run_command("pre_command", pre, dry_run):
        return False

    try:
        if "files" in job:
            return _run_files_job(job, target_remotes, global_opts, job_opts, keep_versions, dry_run)
        elif "source" in job:
            return _run_source_job(job, target_remotes, global_opts, job_opts, keep_versions, dry_run)
        else:
            log.error("Job '%s': must have either 'source' or 'files'.", job_id)
            return False
    finally:
        if post:
            _run_command("post_command", post, dry_run)


def _run_source_job(
    job: dict, target_remotes: dict, global_opts: dict, job_opts: dict, keep_versions: int, dry_run: bool
) -> bool:
    source = _expand(job["source"])
    operation = job.get("operation", "sync")
    success = True
    for remote_name, remote_path in target_remotes.items():
        versions_root = f"{remote_path}/{job['name']}"
        if keep_versions:
            destination = f"{versions_root}/{BACKUP_TIMESTAMP}"
            operation = "copy"  # sync makes no sense for versioned snapshots
        else:
            destination = versions_root
        cmd = build_rclone_cmd(source, destination, operation, global_opts, job_opts, dry_run)
        log.info("[%s] %s -> %s", remote_name, source, destination)
        ok = _run_rclone(cmd, remote_name, source)
        if ok and keep_versions:
            _prune_old_versions(versions_root, keep_versions, dry_run)
        if not ok:
            success = False
    return success


def _run_files_job(
    job: dict, target_remotes: dict, global_opts: dict, job_opts: dict, keep_versions: int, dry_run: bool
) -> bool:
    """Group files by parent directory, then use rclone copy --files-from per group."""
    files = [Path(f).expanduser().resolve() for f in job["files"]]

    groups: dict[Path, list[str]] = defaultdict(list)
    for f in files:
        if not f.exists():
            log.warning("File does not exist, skipping: %s", f)
            continue
        groups[f.parent].append(f.name)

    if not groups:
        log.warning("No existing files to back up.")
        return True

    success = True
    for parent, filenames in groups.items():
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as tmp:
            tmp.write("\n".join(filenames))
            tmp_path = tmp.name

        extra_opts = {**job_opts, "files-from": tmp_path}

        for remote_name, remote_path in target_remotes.items():
            rel = str(parent).lstrip("/")
            job_root = f"{remote_path}/{job['name']}"
            if keep_versions:
                destination = f"{job_root}/{BACKUP_TIMESTAMP}/{rel}"
            else:
                destination = f"{job_root}/{rel}"
            cmd = build_rclone_cmd(str(parent), destination, "copy", global_opts, extra_opts, dry_run)
            log.info("[%s] %s/{%s} -> %s", remote_name, parent, ", ".join(filenames), destination)
            if not _run_rclone(cmd, remote_name, str(parent)):
                success = False

        Path(tmp_path).unlink(missing_ok=True)

    if keep_versions and success:
        for remote_name, remote_path in target_remotes.items():
            _prune_old_versions(f"{remote_path}/{job['name']}", keep_versions, dry_run)

    return success


def main() -> int:
    parser = argparse.ArgumentParser(description="rclone backup wrapper")
    parser.add_argument("-c", "--config", default="config.yaml", type=Path, help="Path to YAML config file")
    parser.add_argument("-n", "--dry-run", action="store_true", help="Pass --dry-run to rclone")
    parser.add_argument("-j", "--job", help="Run only the job with this name")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if not args.config.exists():
        log.error("Config file not found: %s", args.config)
        return 1

    config = load_config(args.config)

    remotes: dict[str, dict] = {
        r["name"]: r
        for r in config.get("remotes", []) + config.get("locals", [])
    }
    if not remotes:
        log.error("No remotes defined in config.")
        return 1

    named_sources: dict[str, str] = {s["name"]: s["path"] for s in config.get("sources", [])}
    global_opts: dict = config.get("options", {})
    global_keep: int = config.get("keep_versions", 0)
    jobs: list[dict] = config.get("jobs", [])

    for job in jobs:
        if "source" in job and job["source"] in named_sources:
            job["source"] = named_sources[job["source"]]

    if args.job:
        jobs = [j for j in jobs if j.get("name") == args.job]
        if not jobs:
            log.error("No job named '%s' found in config.", args.job)
            return 1

    if not jobs:
        log.error("No jobs defined in config.")
        return 1

    all_ok = True
    for job in jobs:
        name = job.get("name", job.get("source", "?"))
        log.info("=== Job: %s ===", name)
        if not run_job(job, remotes, global_opts, global_keep, args.dry_run):
            all_ok = False

    return 0 if all_ok else 1


if __name__ == "__main__":
    sys.exit(main())
