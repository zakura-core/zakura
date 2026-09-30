#!/usr/bin/env python3
"""Bound launchd stdout/stderr logs without requiring child processes to reopen FDs."""
from pathlib import Path
import shutil

BASE = Path("/Library/Application Support/ZakuraVerifier/logs")
LABELS = ("dev.valargroup.zakura-verifier-node", "dev.valargroup.zakura-verifier-adapter",
          "dev.valargroup.zakura-verifier-tunnel")


def rotate(path, limit=10 * 1024 * 1024):
    if not path.exists() or path.stat().st_size < limit:
        return
    for index in range(4, 1, -1):
        src = Path(str(path) + f".{index - 1}")
        if src.exists():
            src.replace(str(path) + f".{index}")
    # Copy/truncate preserves the launchd child's existing output descriptor.
    # These diagnostic logs are best-effort; the comparator's fsynced journal is authoritative.
    shutil.copyfile(path, str(path) + ".1")
    with path.open("r+") as stream:
        stream.truncate(0)


if __name__ == "__main__":
    for label in LABELS:
        for suffix in ("out", "err"):
            rotate(BASE / f"{label}.{suffix}.log")
