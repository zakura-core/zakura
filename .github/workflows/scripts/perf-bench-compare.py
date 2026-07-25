#!/usr/bin/env python3
"""Render the A/B step summary for the perf bench workflow's two legs.

Usage: perf-bench-compare.py PRIMARY_META BASELINE_META

Both paths are the `meta.json` a leg leaves in its artifact. The markdown goes
to stdout (the workflow appends it to the step summary); when GITHUB_OUTPUT is
set, `compare=true|false` is written to it. `false` means the legs are not
comparable -- one produced no meta.json, or zakurad exited non-zero on one of
them -- and the caller must skip the CPU profile diff.
"""

from __future__ import annotations

import json
import os
import sys


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


def render(primary: dict | None, baseline: dict | None) -> tuple[str, bool]:
    """Return the summary markdown and whether the legs are comparable."""
    if primary is None or baseline is None:
        return "one or both legs produced no meta.json; nothing to compare", False

    failed = failed_legs((primary, baseline))
    if failed:
        return f"No comparison: zakurad failed in {', '.join(failed)}.", False

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
        "both legs on identical parallel droplets)"
    )
    return "\n".join(lines), True


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print(f"usage: {argv[0]} PRIMARY_META BASELINE_META", file=sys.stderr)
        return 2

    markdown, comparable = render(load_meta(argv[1]), load_meta(argv[2]))

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
