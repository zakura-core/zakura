#!/usr/bin/env python3
"""Ensure an existing zakura-dev config has network.zakura.trace_dir set."""

from __future__ import annotations

import os
import pathlib
import re
import sys


def main() -> None:
    config = pathlib.Path(os.environ["CONFIG"])
    text = config.read_text()
    trace_dir = os.environ["TRACE_DIR"]
    wanted = f'trace_dir = "{trace_dir}"'
    if re.search(r"(?m)^trace_dir\s*=", text):
        updated = re.sub(r"(?m)^trace_dir\s*=\s*.*$", wanted, text, count=1)
    else:
        needle = "[network.zakura]\n"
        if needle not in text:
            raise SystemExit("missing [network.zakura] section")
        updated = text.replace(
            needle,
            (
                needle
                + "# Structured Zakura JSONL traces "
                + "(block_sync, commit_state, header_sync, legacy_sync).\n"
                + wanted
                + "\n"
            ),
            1,
        )
    if wanted not in updated:
        raise SystemExit("failed to install trace_dir")
    if updated != text:
        config.write_text(updated)
    print(f"Ensured JSONL trace_dir={trace_dir}", file=sys.stderr)


if __name__ == "__main__":
    main()
