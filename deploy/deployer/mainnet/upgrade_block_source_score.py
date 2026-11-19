"""Prepare, install, or restore the fleet's legacy block-download settings."""

import argparse
import hashlib
import os
import re
import subprocess
import tempfile
import tomllib
from pathlib import Path

SETTING = "block_download_score_and_hedge"
# Settings superseded by the merged implementation, including the fixed-delay experiment.
TABLES = {"block_source_score": {"enabled", "second_request"},
          "block_hedge": {"enabled", "delay"}, SETTING: {"enabled"}}


def upgraded_config(text):
    """Enable the merged setting while preserving every unrelated TOML value."""
    expected = tomllib.loads(text)
    network = expected.setdefault("network", {})
    present = [name for name in TABLES if name in network]
    if present == [SETTING] and network[SETTING] == {"enabled": True} and network[SETTING]["enabled"] is True:
        return text

    edited = text
    for name in present:
        table = network.pop(name)
        if not isinstance(table, dict) or table.keys() - TABLES[name]:
            raise ValueError(f"unrecognized fields in network.{name}")
        header = re.search(rf"(?m)^[ \t]*\[network\.{name}\][ \t]*(?:#[^\n]*)?$", edited)
        if header is None:
            raise ValueError(f"expected a [network.{name}] table")
        tail = edited[header.end():]
        following = re.search(r"(?m)^[ \t]*\[", tail)
        edited = edited[:header.start()] + (tail[following.start():] if following else "")

    result = edited + f"\n[network.{SETTING}]\nenabled = true\n"
    network[SETTING] = {"enabled": True}
    if tomllib.loads(result) != expected:
        raise ValueError("could not update only the block-download settings")
    return result


def service_config(service):
    """Read the explicit config path used by this systemd service."""
    shown = subprocess.check_output(
        ["systemctl", "show", "-p", "ExecStart", "--value", service], text=True, timeout=10
    )
    match = re.search(r"argv\[\]=[^;]*?\s(?:-c|--config)[ =](/[^\s;]+)", shown)
    if match is None:
        raise ValueError("could not locate the explicit node config in systemd ExecStart")
    return Path(match[1])


def prepare(config, binary, stage, expected_digest):
    """Verify the candidate schema and save the two config versions in private staging."""
    generated = subprocess.check_output([str(binary), "-c", os.devnull, "generate"], timeout=30)
    if SETTING not in tomllib.loads(generated.decode()).get("network", {}):
        raise ValueError("candidate binary does not support the fleet block-download setting")
    config = config.resolve(strict=True)
    original = config.read_bytes()
    if expected_digest and hashlib.sha256(original).hexdigest() != expected_digest:
        raise ValueError("installed config differs from its deployment receipt")
    updated = upgraded_config(original.decode()).encode()
    if updated != original:
        (stage / "scoring.path").write_text(str(config))
        (stage / "scoring.old").write_bytes(original)
        (stage / "scoring.new").write_bytes(updated)
    return hashlib.sha256(updated).hexdigest()


def replace_config(stage, restore):
    """Replace only the staged version, preserving ownership and permissions on the host."""
    location = stage / "scoring.path"
    if not location.exists():
        return
    path = Path(location.read_text())
    before, after = (stage / "scoring.old").read_bytes(), (stage / "scoring.new").read_bytes()
    if restore:
        before, after = after, before
    current = path.read_bytes()
    if current == after:
        return
    if current != before:
        raise ValueError("node config changed after staging")
    with tempfile.TemporaryDirectory(dir=path.parent) as tmp:
        candidate = Path(tmp) / "config"
        subprocess.run(["cp", "-p", str(path), str(candidate)], check=True)
        candidate.write_bytes(after)
        candidate.replace(path)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    stage = commands.add_parser("prepare")
    source = stage.add_mutually_exclusive_group(required=True)
    source.add_argument("--service")
    source.add_argument("--config", type=Path)
    stage.add_argument("--binary", type=Path, required=True)
    stage.add_argument("--expected-digest")
    for command in (stage, commands.add_parser("apply"), commands.add_parser("restore")):
        command.add_argument("stage", type=Path)
    args = parser.parse_args(argv)
    os.umask(0o077)
    try:
        if args.action == "prepare":
            print(prepare(args.config or service_config(args.service), args.binary, args.stage, args.expected_digest))
        else:
            replace_config(args.stage, args.action == "restore")
    except (ValueError, OSError, subprocess.SubprocessError) as error:
        parser.exit(1, f"fleet scoring config: {error}\n")


if __name__ == "__main__":
    main()
