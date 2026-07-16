---
name: zakura-trace-zip
description: Create shareable zip archives from Zebra Zakura perf trace directories. Use when the user asks to zip, package, archive, share, or export a trace_dir such as feedrun-*-traces or Zakura JSONL traces.
---

# Zakura Trace Zip

## Quick Start

When the user gives a Zakura `trace_dir`, create a shareable archive with:

```bash
python3 .agents/skills/zakura-trace-zip/scripts/zip_zakura_traces.py TRACE_DIR --out-dir perf-artifacts
```

Default output:

```text
perf-artifacts/<trace-dir-name>.zip
```

The zip preserves the trace directory as the top-level folder inside the archive, matching archives like `perf-artifacts/feedrun-r1-traces.zip`.

## Options

Use `--output PATH` to choose the exact zip path:

```bash
python3 .agents/skills/zakura-trace-zip/scripts/zip_zakura_traces.py /mnt/roman-dev-2-data/feedrun-r1-traces --output perf-artifacts/feedrun-r1-traces.zip
```

Use `--force` to overwrite an existing archive.

Use `--include-related` to include nearby run files when they exist:

- `/root/wal-bench/feedrun-<label>.csv`
- trace parent `feedrun-<label>.csv`
- trace parent `feedrun-<label>.log`

Keep the default trace-only archive when the user asks for "the traces zip like this".

## Verification

After creating the archive, report:

- archive path
- compressed size
- number of files included
- whether related CSV/log files were included
