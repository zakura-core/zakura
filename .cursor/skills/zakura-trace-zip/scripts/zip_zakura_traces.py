#!/usr/bin/env python3
"""Create a shareable zip archive from a Zebra Zakura trace directory."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


TRACE_FILE_NAMES = {
    "block_sync.jsonl",
    "commit_state.jsonl",
    "header_sync.jsonl",
    "stream.jsonl",
    "conn.jsonl",
    "handshake.jsonl",
    "discovery.jsonl",
    "legacy_request.jsonl",
}


def infer_label(trace_dir: Path) -> str:
    name = trace_dir.name
    if name.startswith("feedrun-") and name.endswith("-traces"):
        return name.removeprefix("feedrun-").removesuffix("-traces")
    return name.removesuffix("-traces")


def default_output(trace_dir: Path, out_dir: Path) -> Path:
    return out_dir / f"{trace_dir.name}.zip"


def related_files(trace_dir: Path, label: str) -> list[Path]:
    candidates = [
        trace_dir.parent / f"feedrun-{label}.csv",
        trace_dir.parent / f"feedrun-{label}.log",
        Path("/root/wal-bench") / f"feedrun-{label}.csv",
        Path("/root/wal-bench") / f"feedrun-{label}.log",
        Path("/root/wal-bench") / f"cfg-feedrun-{label}.toml",
    ]

    seen = set()
    files = []
    for candidate in candidates:
        try:
            resolved = candidate.resolve()
        except FileNotFoundError:
            continue
        if resolved.exists() and resolved.is_file() and resolved not in seen:
            files.append(resolved)
            seen.add(resolved)
    return files


def validate_trace_dir(trace_dir: Path) -> None:
    if not trace_dir.is_dir():
        raise SystemExit(f"trace directory does not exist: {trace_dir}")

    present = {path.name for path in trace_dir.iterdir() if path.is_file()}
    if not (present & TRACE_FILE_NAMES):
        raise SystemExit(
            f"{trace_dir} does not look like a Zakura trace directory "
            f"(expected one of: {', '.join(sorted(TRACE_FILE_NAMES))})"
        )


def add_manifest(
    zip_file: ZipFile,
    trace_dir: Path,
    output_path: Path,
    trace_files: list[Path],
    included_related: list[Path],
) -> None:
    manifest = {
        "created_at_unix": int(time.time()),
        "source_trace_dir": str(trace_dir),
        "archive_path": str(output_path),
        "trace_file_count": len(trace_files),
        "trace_bytes": sum(path.stat().st_size for path in trace_files),
        "related_files": [str(path) for path in included_related],
    }
    zip_file.writestr(f"{trace_dir.name}/TRACE_ARCHIVE_MANIFEST.json", json.dumps(manifest, indent=2) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("trace_dir", help="Directory containing Zakura JSONL trace files")
    parser.add_argument("--out-dir", default="perf-artifacts", help="Directory for the zip archive")
    parser.add_argument("--output", help="Exact zip path. Overrides --out-dir.")
    parser.add_argument("--force", action="store_true", help="Overwrite an existing archive")
    parser.add_argument(
        "--include-related",
        action="store_true",
        help="Include matching feedrun CSV/log/config files when found",
    )
    args = parser.parse_args()

    trace_dir = Path(args.trace_dir).expanduser().resolve()
    validate_trace_dir(trace_dir)

    output_path = (
        Path(args.output).expanduser().resolve()
        if args.output
        else default_output(trace_dir, Path(args.out_dir).expanduser().resolve())
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if output_path.exists() and not args.force:
        raise SystemExit(f"archive already exists, use --force to overwrite: {output_path}")

    trace_files = sorted(path for path in trace_dir.rglob("*") if path.is_file())
    label = infer_label(trace_dir)
    included_related = related_files(trace_dir, label) if args.include_related else []

    with ZipFile(output_path, "w", compression=ZIP_DEFLATED, compresslevel=6) as zip_file:
        for path in trace_files:
            zip_file.write(path, path.relative_to(trace_dir.parent))

        for path in included_related:
            zip_file.write(path, Path("related") / path.name)

        add_manifest(zip_file, trace_dir, output_path, trace_files, included_related)

    size = output_path.stat().st_size
    print(output_path)
    print(f"trace_files={len(trace_files)} related_files={len(included_related)} size_bytes={size}")


if __name__ == "__main__":
    main()
