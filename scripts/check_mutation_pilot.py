#!/usr/bin/env python3
"""Verify the pilot catalogue's source bindings, baseline evidence and empty corpus."""

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tomllib


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate(root):
    """Return evidence inconsistencies without running mutants or discovery."""
    catalogue = json.loads((root / "qa/mutation-pilot/catalogue.json").read_text())
    cases = catalogue["cases"]
    errors = []

    def require(condition, message):
        if not condition:
            errors.append(message)

    def check_hash(path, expected):
        require(digest(root / path) == expected, f"hash differs: {path}")

    require(8 <= len(cases) <= 12, "pilot must contain 8–12 cases")
    require(len({case["id"] for case in cases}) == len(cases), "duplicate case ID")
    require(len({case["fault_class"] for case in cases}) >= 4, "fewer than four fault classes")
    targets = tomllib.loads((root / "qa/fuzz/sim/Cargo.toml").read_text())["package"]["metadata"]["zakura-sim"]["targets"]
    for case in cases:
        ident = case["id"]
        patch = root / case["patch"]
        check_hash(case["patch"], case["patch_sha256"])
        patch_sidecar = patch.with_suffix(".toml")
        check_hash(str(patch_sidecar.relative_to(root)), case["patch_sidecar_sha256"])
        binding = tomllib.loads(patch_sidecar.read_text())
        require(binding["input"] == case["witness"] and binding["checks"] == case["producer_check_dependencies"], f"{ident}: retained patch binding differs")
        changed = {line[6:] for line in patch.read_text().splitlines() if line.startswith("+++ b/")}
        require(changed == set(case["production_files"]), f"{ident}: changed files differ")
        for path, expected in case["production_files"].items():
            check_hash(path, expected)
        for path, expected in case["oracle_and_generator_files"].items():
            check_hash(path, expected)
        require(bool(case["required_observations"] and case["independent_check"]), f"{ident}: checker evidence missing")
        check_hash(case["witness"], case["witness_sha256"])
        sidecar = (root / case["witness"]).with_suffix(".toml")
        check_hash(str(sidecar.relative_to(root)), case["witness_sidecar_sha256"])
        seed = tomllib.loads(sidecar.read_text())
        require(seed["harness"] == case["harness"] and seed["generator"] == case["generator"], f"{ident}: generator binding differs")
        require(targets[case["target"]] == case["harness"], f"{ident}: target binding differs")
        require(any(w["property"] == case["property"] and w["expect"] == "holds" for w in seed["witness"]), f"{ident}: intended witness missing")
        baseline = case["baseline"]
        directory = root / baseline["directory"]
        check_hash(str(directory.relative_to(root) / "trace.jsonl"), baseline["trace_sha256"])
        check_hash(str(directory.relative_to(root) / "witness.json"), baseline["witness_result_sha256"])
        check_hash(str(directory.relative_to(root) / "input.bin"), case["witness_sha256"])
        trace = [json.loads(line) for line in (directory / "trace.jsonl").read_text().splitlines()]
        meta, end = trace[0]["meta"], trace[-1]["end"]
        require(meta["build"] == catalogue["baseline_build"], f"{ident}: baseline build differs")
        require(meta["sim"] == case["harness"] and meta["generator"] == case["generator"], f"{ident}: trace binding differs")
        require(meta["input"]["sha256"] == case["witness_sha256"], f"{ident}: trace input differs")
        require(any(row.get("step") == case["baseline_trigger_step"] for row in trace), f"{ident}: trigger step absent")
        applicable = [p["outcome"].get("held", {}).get("applicable", 0) for p in end["properties"] if p["property"] == case["property"]]
        require(applicable == [baseline["applicable"]] and baseline["applicable"] > 0, f"{ident}: intended property did not apply")
        require(all(p["outcome"] == "not-applicable" or (isinstance(p["outcome"], dict) and set(p["outcome"]) == {"held"}) for p in end["properties"]), f"{ident}: baseline contains another property failure")
        require(end["outcome"] == "idle", f"{ident}: baseline outcome is not idle")
        require(end["status"] == "quiescent" and not any(end["cleanup"].values()), f"{ident}: baseline did not settle cleanly")
        witness = json.loads((directory / "witness.json").read_text())
        require(witness == {"property": case["property"], "expect": "holds", "passed": True, "error": None}, f"{ident}: witness did not pass")
        require(baseline["record_exit"] == baseline["replay_exit"] == 0, f"{ident}: recording or replay failed")
    discovery = catalogue["discovery"]
    corpus = root / discovery["corpus"]
    require(corpus.is_dir(), "discovery corpus directory is missing")
    require(not list(corpus.rglob("*.bin")) and not list(corpus.rglob("*.toml")), "discovery corpus contains seed files")
    require(discovery["corpus_inputs"] == [], "cold-start corpus must be empty")
    require(discovery["corpus_sha256"] == hashlib.sha256(b"[]\n").hexdigest(), "empty corpus digest differs")
    build = catalogue["baseline_build"]
    require(build["git"] == catalogue["source_revision"] and not build["dirty"], "baseline source identity differs")
    check_hash("Cargo.lock", build["lockfile"])
    source_paths = ["crates", "Cargo.toml", "Cargo.lock", ".cargo", "rust-toolchain.toml"]
    compared = subprocess.run(["git", "diff", "--quiet", catalogue["source_revision"], "--", *source_paths], cwd=root, check=False)
    require(compared.returncode == 0, "current build inputs differ from the baseline source revision")
    untracked = subprocess.run(
        ["git", "ls-files", "--others", "--exclude-standard", "--", *source_paths],
        cwd=root, capture_output=True, text=True, check=False,
    )
    require(untracked.returncode == 0 and not untracked.stdout.strip(), "untracked build inputs invalidate baseline source identity")
    return errors


def main(argv=None):
    argparse.ArgumentParser(description=__doc__).parse_args(argv)
    root = Path(__file__).resolve().parents[1]
    try:
        errors = validate(root)
    except (OSError, KeyError, ValueError, TypeError) as error:
        errors = [f"invalid pilot evidence: {error}"]
    for error in errors:
        print(error)
    if not errors:
        print("Pilot source bindings, baseline evidence and cold-start corpus verified")
    return int(bool(errors))


if __name__ == "__main__":
    raise SystemExit(main())
