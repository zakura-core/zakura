"""Tests for scripts/check_mutation_pilot.py over a small generated checkout and bundle."""

import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / "check_mutation_pilot.py"
SPEC = importlib.util.spec_from_file_location("check_mutation_pilot", SCRIPT)
pilot = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(pilot)

PRODUCTION = "crates/node/src/lib.rs"
ORACLE = "crates/node/src/model.rs"
WITNESS = "crates/sim/tests/fixtures/witness.bin"
FAULT_CLASSES = ["ownership", "wakeup", "stale-result", "resource-lifetime"]


def sha(data):
    return hashlib.sha256(data).hexdigest()


class PilotCatalogueTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "repo"
        self.evidence = Path(self.temp.name) / "bundle"
        self.write("Cargo.toml", "[workspace]\n")
        self.write("Cargo.lock", "version = 4\n")
        self.write(PRODUCTION, "pub fn release() { used -= 1; }\n")
        self.write(ORACLE, "// model\n")
        self.write(WITNESS, "input")
        witness_sha = sha(b"input")
        self.write(Path(WITNESS).with_suffix(".toml"), f'harness = "h"\ngenerator = 1\nsha256 = "{witness_sha}"\n[[witness]]\nproperty = "h"\nexpect = "holds"\n')
        self.write("qa/fuzz/sim/Cargo.toml", '[package.metadata.zakura-sim.targets]\nt = "h"\n')
        self.write("qa/fuzz/sim/Cargo.lock", "version = 4\n")
        self.write("qa/mutation-pilot/discovery-empty/README.md", "No inputs.\n")
        patch = f"--- a/{PRODUCTION}\n+++ b/{PRODUCTION}\n@@ -1 +1 @@\n-pub fn release() {{ used -= 1; }}\n+pub fn release() {{ used -= 2; }}\n"
        cases = []
        for index in range(8):
            stem = f"qa/planted/h/case-{index}"
            self.write(f"{stem}.patch", patch)
            self.write(f"{stem}.toml", f'input = "{WITNESS}"\nchecks = ["crates/node"]\n')
            cases.append({
                "id": f"h/case-{index}",
                "fault_class": FAULT_CLASSES[index % 4],
                "patch": f"{stem}.patch",
                "patch_sha256": self.digest(f"{stem}.patch"),
                "patch_sidecar_sha256": self.digest(f"{stem}.toml"),
                "production_files": {PRODUCTION: self.digest(PRODUCTION)},
                "oracle_and_generator_files": {ORACLE: self.digest(ORACLE)},
                "property": "h",
                "harness": "h",
                "target": "t",
                "generator": 1,
                "baseline_trigger_step": 2,
                "witness": WITNESS,
                "witness_sha256": witness_sha,
                "witness_sidecar_sha256": self.digest(Path(WITNESS).with_suffix(".toml")),
                "producer_check_dependencies": ["crates/node"],
                "required_observations": "pool counts",
                "independent_check": "model handle graph",
                "baseline": {"directory": "baseline/witness", "applicable": 3, "steps": 2, "record_exit": 0, "replay_exit": 0},
            })
        for command in [
            ["git", "init", "-q"], ["git", "add", "."],
            ["git", "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "fixture"],
        ]:
            subprocess.run(command, cwd=self.root, check=True, capture_output=True)
        revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=self.root, text=True).strip()
        build = {"git": revision, "dirty": False, "lockfile": self.digest("Cargo.lock")}
        identity = {
            "target": "t", "harness": "h", "dirty": False, "git": revision, "source": "s",
            "fuzz_lockfile": self.digest("qa/fuzz/sim/Cargo.lock"), "lockfile": build["lockfile"],
            "rustc": "rustc", "cargo_fuzz": "cargo-fuzz", "libfuzzer_sys": "0.4", "rustflags": "--cfg zakura_sim", "cargo_args": [],
        }
        self.catalogue = {
            "source_revision": revision,
            "baseline_build": build,
            "discovery": {"corpus": "qa/mutation-pilot/discovery-empty", "corpus_inputs": []},
            "fuzz_build_inputs": {
                **{field: identity[field] for field in ["rustc", "cargo_fuzz", "libfuzzer_sys", "rustflags", "cargo_args"]},
                "source_revision": revision, "source_sha256": "s",
                "manifest_sha256": self.digest("qa/fuzz/sim/Cargo.toml"), "lockfile_sha256": identity["fuzz_lockfile"],
            },
            "cases": cases,
            "calibration": {"runs": [{
                "directory": "calibration/t", "target": "t", "harness": "h", "stats_file": "stats/s.json", "applicability": "observed",
            }]},
        }
        trace = [
            {"meta": {"build": build, "sim": "h", "generator": 1, "input": {"sha256": witness_sha}}},
            {"step": 1}, {"step": 2},
            {"end": {"outcome": "idle", "status": "quiescent", "steps": 2, "cleanup": {"leaked": 0},
                     "properties": [{"property": "h", "outcome": {"held": {"applicable": 3}}}]}},
        ]
        self.write_evidence("baseline/witness/trace.jsonl", "".join(json.dumps(row) + "\n" for row in trace))
        self.write_evidence("baseline/witness/witness.json", json.dumps({"property": "h", "expect": "holds", "passed": True, "error": None}))
        self.write_evidence("baseline/witness/input.bin", "input")
        self.write_evidence("calibration/t/run.log", "done\n")
        self.write_evidence("calibration/t/run.json", json.dumps({
            "target": "t", "exit_code": 0, "scored": False, "initial_corpus_inputs": [], "build": identity,
        }))
        self.write_evidence("calibration/t/stats/s.json", json.dumps({"harness": "h", "complete": True, "counts": {
            "runs": 4, "outcomes": {"clean": 4}, "cleanup": {"leaked_tasks": 0}, "drains_exhausted": 0,
            "properties": {"h": {"held": 3, "not_applicable": 1, "violated": 0, "inconclusive": {}}},
        }}))
        self.save()

    def write(self, path, text):
        target = self.root / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)

    def write_evidence(self, path, text):
        target = self.evidence / path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(text)

    def digest(self, path):
        return pilot.digest(self.root / path)

    def save(self):
        """Record the bundle's current hashes and write the catalogue."""
        for case in self.catalogue["cases"]:
            case["baseline"]["trace_sha256"] = pilot.digest(self.evidence / "baseline/witness/trace.jsonl")
            case["baseline"]["witness_result_sha256"] = pilot.digest(self.evidence / "baseline/witness/witness.json")
        for row in self.catalogue["calibration"]["runs"]:
            directory = self.evidence / row["directory"]
            row["run_sha256"] = pilot.digest(directory / "run.json")
            row["stats_sha256"] = pilot.digest(directory / row["stats_file"])
            row["log_sha256"] = pilot.digest(directory / "run.log")
        self.write(pilot.CATALOGUE, json.dumps(self.catalogue))

    def edit_json_evidence(self, path, change):
        target = self.evidence / path
        if target.suffix == ".jsonl":
            rows = [json.loads(line) for line in target.read_text().splitlines()]
            change(rows)
            target.write_text("".join(json.dumps(row) + "\n" for row in rows))
        else:
            data = json.loads(target.read_text())
            change(data)
            target.write_text(json.dumps(data))
        self.save()

    def test_intact_inputs_pass_without_a_bundle(self):
        shutil.rmtree(self.evidence)
        self.assertEqual(pilot.validate(self.root), [])

    def test_intact_bundle_passes(self):
        self.assertEqual(pilot.validate(self.root, self.evidence), [])

    def test_zero_applicability_fails_even_with_matching_hashes(self):
        def zero(trace):
            trace[-1]["end"]["properties"][0]["outcome"]["held"]["applicable"] = 0
        self.edit_json_evidence("baseline/witness/trace.jsonl", zero)
        self.assertIn("h/case-0: intended property did not apply", pilot.validate(self.root, self.evidence))

    def test_wrong_witness_id_fails_even_with_matching_hash(self):
        self.edit_json_evidence("baseline/witness/witness.json", lambda witness: witness.update(property="wrong"))
        self.assertIn("h/case-0: witness did not pass", pilot.validate(self.root, self.evidence))

    def test_inconclusive_property_invalidates_baseline(self):
        def inconclusive(trace):
            trace[-1]["end"]["properties"].append({"property": "other", "outcome": {"inconclusive": "budget"}})
        self.edit_json_evidence("baseline/witness/trace.jsonl", inconclusive)
        self.assertIn("h/case-0: baseline contains another property failure", pilot.validate(self.root, self.evidence))

    def test_tampered_bundle_fails_its_recorded_hash(self):
        (self.evidence / "calibration/t/run.log").write_text("edited\n")
        self.assertIn("hash differs: calibration/t/run.log", pilot.validate(self.root, self.evidence))

    def test_discovery_rejects_a_copied_sensitivity_input(self):
        shutil.copyfile(self.root / WITNESS, self.root / "qa/mutation-pilot/discovery-empty/leaked.bin")
        self.assertIn("discovery corpus contains seed files", pilot.validate(self.root))

    def test_missing_discovery_directory_fails(self):
        shutil.rmtree(self.root / "qa/mutation-pilot/discovery-empty")
        self.assertIn("discovery corpus directory is missing", pilot.validate(self.root))

    def test_untracked_build_input_fails(self):
        self.write("crates/node/src/extra.rs", "// untracked\n")
        self.assertIn("current build inputs differ from the baseline source revision", pilot.validate(self.root))

    def test_oracle_drift_fails(self):
        with (self.root / ORACLE).open("a") as stream:
            stream.write("// changed oracle\n")
        errors = pilot.validate(self.root)
        self.assertIn(f"hash differs: {ORACLE}", errors)
        self.assertIn("current build inputs differ from the baseline source revision", errors)

    def test_fuzz_lock_drift_fails(self):
        self.write("qa/fuzz/sim/Cargo.lock", "version = 5\n")
        errors = pilot.validate(self.root)
        self.assertIn("hash differs: qa/fuzz/sim/Cargo.lock", errors)
        self.assertIn("fuzz build inputs differ from the calibrated revision", errors)

    def test_zero_calibration_applicability_cannot_claim_coverage(self):
        def unapplied(stats):
            stats["counts"]["properties"]["h"].update(held=0, not_applicable=4)
        self.edit_json_evidence("calibration/t/stats/s.json", unapplied)
        self.assertIn("t: calibration applicability differs", pilot.validate(self.root, self.evidence))

    def test_calibration_cleanup_failure_fails_even_with_matching_hash(self):
        self.edit_json_evidence("calibration/t/stats/s.json", lambda stats: stats["counts"]["cleanup"].update(leaked_tasks=1))
        self.assertIn("t: calibration contains a failure", pilot.validate(self.root, self.evidence))


if __name__ == "__main__":
    unittest.main()
