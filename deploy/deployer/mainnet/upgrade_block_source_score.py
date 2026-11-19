"""Stage the PR 2595 scoring config for the binary-only systemd installer."""

import argparse
import re
import subprocess
import tomllib
from pathlib import Path

OLD = "block_source_score"
NEW = "block_download_score_and_hedge"


def upgraded_config(text):
    """Return an equivalent config with the two enabled flags merged, or None if current."""
    expected = tomllib.loads(text)
    network = expected.get("network", {})
    current = network.get(NEW)
    if (OLD not in network and isinstance(current, dict)
            and set(current) == {"enabled"} and current["enabled"] is True):
        return None
    old = network.get(OLD)
    if (not isinstance(old, dict) or set(old) != {"enabled", "second_request"}
            or not all(value is True for value in old.values()) or NEW in network):
        raise ValueError("expected both legacy scoring flags enabled or the merged setting enabled")

    # Keep comments and formatting elsewhere in the operator's config. Parse the
    # result again so a match inside a multiline string can never change its value.
    header = re.search(r"(?m)^[ \t]*\[network\.block_source_score\][ \t]*(?:#[^\n]*)?$", text)
    if header is None:
        raise ValueError("expected a [network.block_source_score] table")
    tail = text[header.end():]
    next_header = re.search(r"(?m)^[ \t]*\[", tail)
    end = next_header.start() if next_header else len(tail)
    body = re.sub(r"(?m)^[ \t]*second_request[ \t]*=[ \t]*true[ \t]*(?:#[^\n]*)?(?:\n|$)", "", tail[:end])
    result = text[:header.start()] + header.group().replace(OLD, NEW, 1) + body + tail[end:]
    del network[OLD]
    network[NEW] = {"enabled": True}
    if tomllib.loads(result) != expected:
        raise ValueError("could not update only the legacy scoring settings")
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("service")
    parser.add_argument("stage", type=Path)
    args = parser.parse_args(argv)
    shown = subprocess.check_output(
        ["systemctl", "show", "-p", "ExecStart", "--value", args.service], text=True, timeout=10
    )
    match = re.search(r"argv\[\]=[^;]*?\s(?:-c|--config)[ =](/[^\s;]+)", shown)
    if match is None:
        parser.exit(1, "could not locate the node config in systemd ExecStart\n")
    config = Path(match[1]).resolve(strict=True)
    original = config.read_bytes()
    try:
        updated = upgraded_config(original.decode())
    except ValueError as error:
        parser.exit(1, f"scoring config upgrade: {error}\n")
    if updated is not None:
        # The deployer created this private staging directory on the node.
        (args.stage / "scoring.old").write_bytes(original)
        (args.stage / "scoring.new").write_bytes(updated.encode())
        print(config)


if __name__ == "__main__":
    main()
