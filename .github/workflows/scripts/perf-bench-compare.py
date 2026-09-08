#!/usr/bin/env python3
"""Render the A/B step summary for the perf bench workflow's two legs.

Usage: perf-bench-compare.py PRIMARY_META BASELINE_META

Both paths are the `meta.json` a leg leaves in its artifact. The markdown goes
to stdout (the workflow appends it to the step summary); when GITHUB_OUTPUT is
set, `compare=true|false` is written to it. `false` means the legs are not
comparable because a run failed, the workloads differ, or recorded host and
snapshot identities do not match. The caller must skip the CPU profile diff.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path


def load_meta(path: str) -> dict | None:
    """Return a leg's meta.json, or None when there is no usable one.

    The producer tolerates a failed meta write (perf-bench-run.sh logs a
    warning and carries on), and json.dump truncates before it writes, so an
    empty or half-written file is a real outcome -- treat it like an absent
    one instead of failing the compare job with a traceback.
    """
    try:
        with open(path, encoding="utf-8") as meta_file:
            return json.load(meta_file)
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError) as err:
        print(f"{path}: unusable meta.json ({err})", file=sys.stderr)
        return None


def failed_legs(metas) -> list[str]:
    return [meta["leg"] for meta in metas if meta.get("node_exit_status", 0)]


def load_environment(meta_path: str) -> dict | None:
    """Read the host and snapshot identity recorded beside a leg's metadata."""
    directory = Path(meta_path).parent
    cpu = load_meta(str(directory / "cpu.json"))
    provisioning = load_meta(str(directory / "provisioning.json"))
    try:
        fields = {row["field"]: row["data"] for row in cpu["lscpu"]}
        identity = {
            key: fields[key]
            for key in ("Architecture:", "Model name:", "CPU(s):", "Thread(s) per core:")
        }
        identity.update({
            key: provisioning[key]
            for key in ("region", "size", "image_id", "state_snapshot_id")
        })
        return identity if all(identity.values()) else None
    except (KeyError, TypeError):
        return None


def render(primary: dict | None, baseline: dict | None) -> tuple[str, bool]:
    """Return the summary markdown and whether the legs are comparable."""
    if primary is None or baseline is None:
        return "one or both legs produced no meta.json; nothing to compare", False

    failed = failed_legs((primary, baseline))
    if failed:
        return f"No comparison: zakurad failed in {', '.join(failed)}.", False

    for meta in (primary, baseline):
        if meta.get("workload") == "historical_sync" and (
            not meta.get("clean_stop") or meta.get("end_height") != meta.get("stop_height", meta.get("end_height"))
        ):
            return f"No comparison: {meta['leg']} did not complete its requested range.", False
    if primary.get("workload") == "historical_sync" and any(
        primary.get(key) != baseline.get(key)
        for key in ("workload", "verify_mode", "start_height", "end_height", "storage_mode")
    ):
        return "No comparison: the workloads or block ranges differ.", False

    if "comparison" in primary or "comparison" in baseline:
        if not primary.get("environment") or not baseline.get("environment"):
            return "No comparison: host or snapshot identity is missing.", False
        if primary["environment"] != baseline["environment"]:
            return "No comparison: CPU, host configuration, or snapshot identity differs.", False

    # A zero-throughput baseline has no meaningful ratio; report it as nan
    # rather than crashing, and let the blocks/s column show what happened.
    speedup = primary["bps"] / baseline["bps"] if baseline["bps"] else float("nan")
    lines = [
        "## A/B result",
        "",
        "| leg | ref | blocks/s | post-commit blk/s | verdict |",
        "|---|---|---:|---:|---|",
    ]
    for meta in (baseline, primary):
        lines.append(
            f"| {meta['leg']} | `{meta['sha'][:9]}` | {meta['bps']} "
            f"| {meta['post_bps']} | {meta.get('verdict') or 'n/a'} |"
        )
    lines.append("")
    lines.append(
        f"**Speedup (primary vs baseline): {speedup:.2f}×** "
        f"({baseline['bps']} → {primary['bps']} blocks/s, "
        "separate benchmark runs; host and peer noise remain)"
    )
    if "comparison" in primary:
        lines.extend(["", "Configuration for each leg:"])
        for meta in (baseline, primary):
            lines.append(
                f"- {meta['leg']}: {meta.get('p2p_stack')}, VCT={meta.get('vct_fast_sync')}, "
                f"traces={meta.get('traces')}, storage={meta.get('storage_mode')}"
            )
    return "\n".join(lines), True


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print(f"usage: {argv[0]} PRIMARY_META BASELINE_META", file=sys.stderr)
        return 2

    metas = [load_meta(path) for path in argv[1:]]
    for path, meta in zip(argv[1:], metas):
        if meta is not None and "comparison" in meta:
            meta["environment"] = load_environment(path)
    markdown, comparable = render(*metas)

    # Flag first: if printing the summary fails, the caller must still see a
    # decision rather than silently skipping the CPU diff.
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a", encoding="utf-8") as out:
            out.write(f"compare={'true' if comparable else 'false'}\n")

    print(markdown)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
