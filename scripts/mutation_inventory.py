#!/usr/bin/env python3
"""Inventory retained mutation bindings without applying patches or running builds."""

import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tomllib


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def planted_binding(root, patch):
    """Check the case, committed input, witness declaration, and fuzz target mapping."""
    case = tomllib.loads(patch.with_suffix(".toml").read_text())
    source = root / case["input"]
    sidecar = tomllib.loads(source.with_suffix(".toml").read_text())
    property_id = patch.parent.relative_to(root / "qa/planted").as_posix()
    errors = []
    if sha256(source) != sidecar["sha256"]:
        errors.append("input hash differs from sidecar")
    if not any(
        witness["property"] == property_id and witness["expect"] == "holds"
        for witness in sidecar.get("witness", [])
    ):
        errors.append("sidecar has no holds witness for the intended property")
    manifest = tomllib.loads((root / "qa/fuzz/sim/Cargo.toml").read_text())
    targets = manifest["package"]["metadata"]["zakura-sim"]["targets"]
    names = sorted(name for name, harness in targets.items() if harness == sidecar["harness"])
    binaries = {item["name"]: item for item in manifest["bin"]}
    for name in names:
        binary = binaries.get(name)
        if binary is None or not (root / "qa/fuzz/sim" / binary["path"]).is_file():
            errors.append(f"target {name} has no binary source")
    for check in case.get("checks", []):
        if not (root / check).exists():
            errors.append(f"missing dependency path: {check}")
    return {
        "property": property_id,
        "input": case["input"],
        "input_sha256": sha256(source),
        "harness": sidecar["harness"],
        "generator": sidecar["generator"],
        "fuzz_targets": names,
        "discovery_target": "mapped, not executed" if names else "missing",
        "errors": errors,
    }


def native_binding(root, case):
    """Check a declared source symbol; this does not prove executable selection."""
    if "gap" in case:
        return {"errors": [case["gap"]]}
    source = root / case["source"]
    test = case["test"]
    symbol = test.split("::")[-1]
    errors = []
    if not re.search(r"\bfn\s+" + re.escape(symbol) + r"\s*\(", source.read_text()):
        errors.append(f"test function not found in {case['source']}")
    crate = next(parent for parent in source.parents if (parent / "Cargo.toml").is_file())
    manifest = tomllib.loads((crate / "Cargo.toml").read_text())
    if manifest["package"]["name"] != case["package"]:
        errors.append("declared package differs from the source crate")
    if not case["required_failure"].strip():
        errors.append("missing intended assertion")
    target = ["--test", case["test_target"]] if "test_target" in case else ["--lib"]
    return {
        "package": case["package"],
        "test": test,
        "source": case["source"],
        "required_failure": case["required_failure"],
        "command": ["cargo", "test", "--locked", "-p", case["package"], *target, "--", test, "--exact"],
        "rustflags": case.get("rustflags", ""),
        "executable_selection": "not-run; source symbol check only",
        "errors": errors,
    }


def inventory(root):
    """Return one row per retained patch and every static validation failure."""
    native = json.loads((root / "qa/native-mutations/cases.json").read_text())
    mappings = {}
    errors = []
    for case in native:
        path = "qa/native-mutations/" + case["patch"]
        if path in mappings:
            errors.append(f"duplicate native mapping: {path}")
        mappings[path] = case
    patches = sorted([
        *(root / "qa/planted").rglob("*.patch"),
        *(root / "qa/native-mutations").rglob("*.patch"),
    ])
    rows = []
    for patch in patches:
        path = patch.relative_to(root).as_posix()
        row = {"patch": path, "patch_sha256": sha256(patch)}
        checked = subprocess.run(
            ["git", "apply", "--check", str(patch)], cwd=root,
            capture_output=True, text=True, check=False,
        )
        row["apply"] = "passed" if checked.returncode == 0 else "failed"
        row["apply_error"] = checked.stderr.strip() or None
        try:
            if path.startswith("qa/planted/"):
                row.update(planted_binding(root, patch))
            else:
                row.update(native_binding(root, mappings.pop(path)))
        except (OSError, KeyError, ValueError, StopIteration) as error:
            row["errors"] = [f"binding metadata: {error}"]
        row["compilation"] = "not-run"
        row["sensitivity"] = "not-run"
        rows.append(row)
    errors.extend(f"mapping has no patch: {path}" for path in sorted(mappings))
    return {
        "revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip(),
        "worktree_status": subprocess.check_output(
            ["git", "status", "--porcelain", "--untracked-files=all"], cwd=root, text=True,
        ).splitlines(),
        "scope": "Static inventory only. Apply success and source symbols do not prove compilation, executable selection, trigger reachability, or detection.",
        "errors": errors,
        "cases": rows,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    root = Path(__file__).resolve().parents[1]
    report = inventory(root)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    failures = len(report["errors"]) + sum(
        bool(row["errors"]) or row["apply"] != "passed" for row in report["cases"]
    )
    print(f"{len(report['cases'])} patches; {failures} metadata or application failures")
    return int(failures != 0)


if __name__ == "__main__":
    raise SystemExit(main())
