#!/usr/bin/env python3
"""Select native acceptance only when inputs changed for the event."""

import json
import os
from pathlib import Path
import subprocess


def needs_native_build(paths):
    return any(
        path.startswith(("crates/", ".cargo/", "deploy/zakura-mac-cranelift/cranelift/"))
        or path in {"Cargo.toml", "Cargo.lock", "rust-toolchain.toml",
                    "deploy/zakura-mac-cranelift/corpus.json",
                    "deploy/zakura-mac-cranelift/build_inputs.py",
                    ".github/workflows/zakura-mac-cranelift.yml",
                    ".github/workflows/build-zakura-mac-cranelift.yml"}
        for path in paths
    )


def event_revisions(event_name, event):
    """PRs compare their full diff so an earlier failed build cannot be bypassed."""
    if event_name == "workflow_dispatch":
        return None
    if event_name == "pull_request":
        pr = event["pull_request"]
        return pr["base"]["sha"], pr["head"]["sha"]
    return event.get("before"), event.get("after")


def main():
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
    revisions = event_revisions(os.environ["GITHUB_EVENT_NAME"], event)
    needed = True
    if revisions and all(revisions) and set(revisions[0]) != {"0"}:
        try:
            paths = subprocess.check_output(
                ["git", "diff", "--name-only", "-z", *revisions], timeout=30
            ).decode().split("\0")
            needed = needs_native_build(paths)
        except subprocess.SubprocessError:
            # Missing history must run acceptance rather than silently skip it.
            pass
    with open(os.environ["GITHUB_OUTPUT"], "a") as stream:
        stream.write(f"native={'true' if needed else 'false'}\n")


if __name__ == "__main__":
    main()
