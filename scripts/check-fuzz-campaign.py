#!/usr/bin/env python3
"""Fail when a fuzz target or coverage row lacks its registration or campaign.

This reads `qa/suite.toml`, the fuzz packages' `Cargo.toml` tables, the fuzz workflows
and the crates' sources. It compiles nothing, so it runs on every pull request. The
simulator's suite manifest makes the checks that need the compiled harness registry.
"""

import argparse
import re
import sys
import tomllib
from pathlib import Path

SIM_PACKAGE = "sim"
# The workflow that schedules each fuzz package's campaign. The suite excludes every
# target of a package outside this table, as `rpc-fuzz.yml` keeps its own schedule.
WORKFLOWS = {
    "header-chain": "header-fuzz.yml",
    "p2p": "p2p-fuzz.yml",
    SIM_PACKAGE: "sim-fuzz.yml",
}
MATRIX_ROW = re.compile(r"^\s*- target: (\S+)\s*$")
MATRIX_FIELD = re.compile(r"^\s+([a-z_]+): (\S+)\s*$")
COVERAGE_PROPERTY = re.compile(r'^\s*property: "([^"]+)"', re.MULTILINE)


def matrix_rows(workflow_text):
    """Return each `- target:` row of a workflow's matrix, as its fields by target."""
    rows = {}
    current = None
    for line in workflow_text.splitlines():
        row = MATRIX_ROW.match(line)
        if row:
            current = rows.setdefault(row.group(1), {})
            continue
        field = MATRIX_FIELD.match(line)
        if field and current is not None:
            current[field.group(1)] = field.group(2)
        elif not line.lstrip().startswith("#"):
            current = None
    return rows


def suite_arg(target, flag):
    """Return the value of `-<flag>=` in a suite target's args, or None."""
    prefix = f"-{flag}="
    for arg in target.get("args", []):
        if arg.startswith(prefix):
            return arg[len(prefix) :]
    return None


def source_literals(root):
    """Return the text of every Rust source under `crates/`, without the coverage table."""
    coverage = root / "crates/zakura-sim/src/harness/coverage.rs"
    return "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted((root / "crates").rglob("*.rs"))
        if path != coverage and "target" not in path.relative_to(root).parts
    )


def package_problems(root, package, manifest, suite_names, excluded, sources):
    """Check that each binary of one fuzz package is scheduled and registered."""
    problems = []
    metadata = manifest.get("package", {}).get("metadata", {})
    harnesses = metadata.get("zakura-sim", {}).get("targets", {})
    machines = metadata.get("zakura-machine", {}).get("targets", {})
    bins = {binary["name"]: binary.get("path") for binary in manifest.get("bin", [])}
    where = f"qa/fuzz/{package}"

    for name, path in bins.items():
        if path and not (root / where / path).is_file():
            problems.append(f"{where}: the target `{name}` has no source at {path}")
        if name in excluded:
            continue
        if name not in suite_names:
            problems.append(f"{where}: qa/suite.toml neither runs nor excludes `{name}`")
        if package == SIM_PACKAGE:
            if name not in harnesses:
                problems.append(f"{where}: `{name}` maps to no harness")
        elif name not in machines:
            problems.append(f"{where}: `{name}` has no machine registration")

    for name, harness in harnesses.items():
        if name not in bins:
            problems.append(f"{where}: the harness table maps unknown target `{name}`")
        if f'"{harness}"' not in sources:
            problems.append(f"{where}: `{name}` names the unknown harness `{harness}`")

    for name, machine in machines.items():
        if name not in bins:
            problems.append(f"{where}: the machine table registers unknown target `{name}`")
        properties = machine.get("properties", [])
        if bool(properties) == ("unregistered" in machine):
            problems.append(f"{where}: `{name}` needs properties or a reason that it has none")
        if "decoder" not in machine:
            problems.append(f"{where}: `{name}` names no decoder version")
        for prop in properties:
            if f'"{prop}"' not in sources:
                problems.append(f"{where}: `{name}` names the unknown property `{prop}`")
    return problems


def schedule_problems(root, package, targets, machines):
    """Check that one package's workflow schedules exactly the suite's targets."""
    workflow = WORKFLOWS[package]
    path = root / ".github/workflows" / workflow
    if not path.is_file():
        return [f"{workflow}: the workflow for qa/fuzz/{package} doesn't exist"]
    text = path.read_text(encoding="utf-8")
    problems = []
    for trigger in ("pull_request", "merge_group", "push"):
        if re.search(rf"^\s*{trigger}:", text, re.MULTILINE):
            problems.append(f"{workflow}: a campaign must not run on {trigger}")
    for trigger in ("schedule", "workflow_dispatch"):
        if not re.search(rf"^\s*{trigger}:", text, re.MULTILINE):
            problems.append(f"{workflow}: a campaign must run on {trigger}")

    rows = matrix_rows(text)
    for name in sorted(rows.keys() - targets.keys()):
        problems.append(f"{workflow}: schedules `{name}`, which qa/suite.toml doesn't run")
    for name, target in sorted(targets.items()):
        row = rows.get(name)
        if row is None:
            problems.append(f"{workflow}: doesn't schedule `{name}`")
            continue
        for flag, field in (("rss_limit_mb", "rss_mb"), ("max_len", "max_len")):
            expected = suite_arg(target, flag)
            if expected is not None and row.get(field) != expected:
                problems.append(
                    f"{workflow}: `{name}` has {field} {row.get(field)}, "
                    f"and qa/suite.toml has {expected}"
                )
        decoder = machines.get(name, {}).get("decoder")
        if decoder is not None and row.get("decoder") != str(decoder):
            problems.append(
                f"{workflow}: `{name}` has decoder {row.get('decoder')}, "
                f"and its machine table has {decoder}"
            )
    return problems


def coverage_problems(root, suite, sources):
    """Check that each coverage row has a declaration and isn't also pending."""
    path = root / "crates/zakura-sim/src/harness/coverage.rs"
    if not path.is_file():
        return []
    problems = []
    rows = set(COVERAGE_PROPERTY.findall(path.read_text(encoding="utf-8")))
    for prop in sorted(rows):
        if f'"{prop}"' not in sources:
            problems.append(f"coverage.rs: the row `{prop}` has no property declaration")
    for pending in suite.get("pending", []):
        if pending.get("kind") == "property" and pending["id"] in rows:
            problems.append(f"qa/suite.toml: `{pending['id']}` is pending and has a coverage row")
    return problems


def problems_at(root):
    """Return every problem with the fuzz campaign wiring of the checkout at `root`."""
    suite = tomllib.loads((root / "qa/suite.toml").read_text(encoding="utf-8"))
    sources = source_literals(root)
    problems = coverage_problems(root, suite, sources)
    for manifest_path in sorted((root / "qa/fuzz").glob("*/Cargo.toml")):
        package = manifest_path.parent.name
        manifest = tomllib.loads(manifest_path.read_text(encoding="utf-8"))
        targets = {
            target["name"]: target
            for target in suite.get("target", [])
            if target["package"] == package
        }
        excluded = {
            target["name"]
            for target in suite.get("excluded-target", [])
            if target["package"] == package
        }
        problems += package_problems(root, package, manifest, targets, excluded, sources)
        if package in WORKFLOWS:
            metadata = manifest.get("package", {}).get("metadata", {})
            machines = metadata.get("zakura-machine", {}).get("targets", {})
            problems += schedule_problems(root, package, targets, machines)
        elif targets:
            problems.append(f"qa/fuzz/{package}: no campaign workflow schedules its targets")
    return problems


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("."))
    args = parser.parse_args(argv)
    problems = problems_at(args.root.resolve())
    for problem in problems:
        print(problem, file=sys.stderr)
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
