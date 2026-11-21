#!/usr/bin/env python3
"""Delete a sim host's old build trees and run directories (mutation plan, R0).

The daily cleanup timer runs it on each host that builds the sim. It
deletes three kinds of directory under each `--root`:

- `target`: a cargo target directory, which holds the `CACHEDIR.TAG` and
  `.rustc_info.json` that cargo writes. Cargo's registry holds the tag
  without the rustc file, so the cleanup never takes it.
- `run`: a routine run under `<state>/runs/`, other than the runs that a
  `<state>/latest*.json` names.
- `builds`: the `builds/` directory of a campaign store, which holds a
  fuzzer binary per build.

It keeps every `--keep` path, and each `--state`'s cumulative reports,
`latest*.json`, `archive/` and `campaigns/`. The live campaign stores stay
whole, because `sim-campaign verify` reads the build of every run a store
records.

A directory is old when nothing under it changed for `--max-age-days`. A
`builds/` directory counts as changed when anything in its store changed,
because a resumed campaign writes to its store but never to its old builds,
and still needs them. The
cleanup deletes every old directory. If the disk is then fuller than
`--disk-limit` percent, it deletes the remaining directories, least recently
changed first, until the disk is below the limit. It never deletes a
directory that changed within `--busy-hours`, which a running job may still
use. It prints one JSON line per deletion and a summary.
"""

import argparse
from fnmatch import fnmatch
import json
import os
from pathlib import Path
import shutil
import stat
import sys
import time

CARGO_TAG = "CACHEDIR.TAG"
RUSTC_INFO = ".rustc_info.json"
STORE_LOCK = "store.lock"
STATE_KEPT = ("archive", "campaigns")


def scan(path):
    """Return the newest change time under `path` and its size in bytes, without following links or mounts."""
    top = os.lstat(path)
    newest, size = top.st_mtime, top.st_blocks * 512
    stack = [path]
    while stack:
        try:
            entries = list(os.scandir(stack.pop()))
        except OSError:
            continue
        for entry in entries:
            try:
                info = entry.stat(follow_symlinks=False)
            except OSError:
                continue
            newest = max(newest, info.st_mtime)
            size += info.st_blocks * 512
            if stat.S_ISDIR(info.st_mode) and info.st_dev == top.st_dev:
                stack.append(entry.path)
    return newest, size


def kept(path, keep):
    """Whether `path` matches a `--keep` pattern."""
    return any(fnmatch(path, pattern) for pattern in keep)


def state_runs(state):
    """Return each run directory under `<state>/runs/` that no `<state>/latest*.json` names."""
    named = set()
    for latest in Path(state).glob("latest*.json"):
        try:
            named.add(str(Path(json.loads(latest.read_text())["run"]).resolve()))
        except (OSError, ValueError, KeyError, TypeError):
            continue
    runs = Path(state) / "runs"
    if not runs.is_dir():
        return []
    return [str(run) for run in sorted(runs.iterdir())
            if run.is_dir() and not run.is_symlink() and str(run.resolve()) not in named]


def candidates(roots, states, keep):
    """Return `(kind, path)` for each directory the cleanup may delete."""
    states = [str(Path(state).resolve()) for state in states]
    skip = {os.path.join(state, name) for state in states for name in (*STATE_KEPT, "runs")}
    found = [("run", run) for state in states for run in state_runs(state)]
    for root in roots:
        root = str(Path(root).resolve())
        device = os.lstat(root).st_dev
        for directory, subdirs, files in os.walk(root):
            if CARGO_TAG in files and RUSTC_INFO in files:
                found.append(("target", directory))
                subdirs[:] = []
                continue
            if "builds" in subdirs and STORE_LOCK in files:
                found.append(("builds", os.path.join(directory, "builds")))
            subdirs[:] = [
                name for name in subdirs
                if name not in (".git", "builds")
                and not kept(os.path.join(directory, name), keep)
                and os.path.join(directory, name) not in skip
                and os.lstat(os.path.join(directory, name)).st_dev == device
            ]
    return [(kind, path) for kind, path in found if not kept(path, keep)]


def disk_usage(path):
    """Return the disk's used and available bytes, as `df` counts them."""
    disk = os.statvfs(path)
    return (disk.f_blocks - disk.f_bfree) * disk.f_frsize, disk.f_bavail * disk.f_frsize


def cleanup(roots, states, keep, max_age_days, disk_limit, busy_hours, dry_run=False, now=None, usage=disk_usage):
    """Delete old directories, then the least recently changed ones while the disk is over the limit."""
    now = time.time() if now is None else now
    # The disk's use before any deletion, less what the cleanup deletes, so
    # a dry run predicts the same deletions as a real one.
    used, available = usage(roots[0])
    entries = []
    for kind, path in candidates(roots, states, keep):
        newest, size = scan(path)
        if kind == "builds":
            newest = max(newest, scan(os.path.dirname(path))[0])
        entries.append({"kind": kind, "path": path, "age_days": (now - newest) / 86400, "bytes": size})
    entries.sort(key=lambda entry: -entry["age_days"])
    deleted = []

    def delete(entry, reason):
        if not dry_run:
            shutil.rmtree(entry["path"], ignore_errors=True)
        record = {**entry, "age_days": round(entry["age_days"], 2), "reason": reason}
        deleted.append(record)
        print(json.dumps(record))

    remaining = []
    for entry in entries:
        if entry["age_days"] > max_age_days:
            delete(entry, "age")
        else:
            remaining.append(entry)

    def percent():
        return 100 * (used - sum(entry["bytes"] for entry in deleted)) / (used + available)

    for entry in remaining:
        if percent() <= disk_limit or entry["age_days"] * 24 < busy_hours:
            break
        delete(entry, "disk")
    summary = {
        "deleted": len(deleted),
        "freed_bytes": sum(entry["bytes"] for entry in deleted),
        "disk_percent": round(percent(), 1),
        "dry_run": dry_run,
    }
    print(json.dumps(summary))
    return deleted


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--root", action="append", required=True,
                        help="a directory to search for build trees, repeatable; the first one's disk sets the limit")
    parser.add_argument("--state", action="append", default=[],
                        help="a routine state directory, repeatable: its runs go, its reports and stores stay")
    parser.add_argument("--keep", action="append", default=[], help="a path or glob never to delete, repeatable")
    parser.add_argument("--max-age-days", type=float, default=3, help="delete what didn't change for this long")
    parser.add_argument("--disk-limit", type=float, default=80, help="the disk use, in percent, to delete down to")
    parser.add_argument("--busy-hours", type=float, default=6, help="never delete what changed this recently")
    parser.add_argument("--dry-run", action="store_true", help="print what it would delete, and delete nothing")
    args = parser.parse_args(argv)
    cleanup(args.root, args.state, args.keep, args.max_age_days, args.disk_limit, args.busy_hours, args.dry_run)
    return 0


if __name__ == "__main__":
    sys.exit(main())
