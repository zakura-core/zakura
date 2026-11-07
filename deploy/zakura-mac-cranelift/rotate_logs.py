#!/usr/bin/env python3
"""Bound launchd stdout/stderr logs without requiring child processes to reopen FDs."""
from pathlib import Path
from common import rotate as rotate_file

BASE = Path("/Library/Application Support/ZakuraVerifier/logs")
LABELS = ("dev.valargroup.zakura-verifier-node",)


def rotate(path, limit=10 * 1024 * 1024):
    rotate_file(path, limit, segments=4, copy_truncate=True)


if __name__ == "__main__":
    for label in LABELS:
        for suffix in ("out", "err"):
            rotate(BASE / f"{label}.{suffix}.log")
