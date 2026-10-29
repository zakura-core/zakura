import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).parents[1] / "sim-schedule-diff.py"
SPEC = importlib.util.spec_from_file_location("sim_schedule_diff", SCRIPT)
assert SPEC and SPEC.loader
schedule_diff = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = schedule_diff
SPEC.loader.exec_module(schedule_diff)


def trace(pick="task:wire-node.1:poll#1", draw_value="0x6a0dd7ac648655ab", obs="9fceee4cc7a5d858", build="aaaa",
          outcome="budget"):
    """A crafted trace in `zakura-sim record`'s shape: meta, init, three steps and end."""
    records = [
        {"meta": {"format": 18, "build": {"git": build}}},
        {"init": {"draws": [{"draw": "setup:hash-seed#1", "bound": "0x0", "value": "0xa6c3", "by": "bytes"}]}},
        {"step": 1, "pick": "task:header-node.1:poll#1", "options": 5, "by": "bytes", "offset": 8, "obs": obs},
        {"step": 2, "pick": pick, "options": 5, "by": "bytes", "offset": 9, "obs": "dff362da6b9f10ac"},
        {"step": 3, "pick": "task:wire-node.1:rmw#1", "options": 2, "by": "bytes", "offset": 12,
         "draws": [{"draw": "wire-node.1:rng#1", "bound": "0x0", "value": draw_value, "by": "bytes"}],
         "obs": "c304ab2559d393a8"},
        {"end": {"outcome": outcome, "steps": 3}},
    ]
    return [json.dumps(record) for record in records]


class TraceComparisonTests(unittest.TestCase):
    def test_identical_choices_pass(self):
        """Traces whose choices match pass, whatever their observation digests and build."""
        self.assertIsNone(schedule_diff.compare_traces(trace(), trace(obs="0000000000000000", build="bbbb")))

    def test_a_changed_pick_fails(self):
        """One step with a different pick fails."""
        difference = schedule_diff.compare_traces(trace(), trace(pick="task:header-node.1:poll#2"))
        self.assertIsNotNone(difference)
        self.assertIn("step 2 differs", difference)

    def test_a_changed_draw_fails(self):
        """One different draw fails."""
        difference = schedule_diff.compare_traces(trace(), trace(draw_value="0x1"))
        self.assertIsNotNone(difference)
        self.assertIn("step 3 differs", difference)

    def test_an_extra_step_fails(self):
        """A trace with one more step fails."""
        longer = trace()
        longer.insert(-1, json.dumps({"step": 4, "pick": "task:x.1:poll#1", "options": 1, "by": "forced"}))
        self.assertIn("4 choice records, head 5", schedule_diff.compare_traces(trace(), longer))

    def test_a_harness_failure_fails(self):
        """A run that ends in a harness failure fails, even with the same choices."""
        difference = schedule_diff.compare_traces(trace(), trace(outcome="harness"))
        self.assertIsNotNone(difference)
        self.assertIn("head run ended in a harness failure", difference)
        difference = schedule_diff.compare_traces(trace()[:-1], trace())
        self.assertIsNotNone(difference)
        self.assertIn("base run has no end record", difference)


class FixtureTests(unittest.TestCase):
    def test_a_removed_fixture_is_reported(self):
        """A fixture that base has and head doesn't is reported, and head's fixtures are the ones run."""
        with tempfile.TemporaryDirectory() as tmp:
            base, head = Path(tmp) / "base", Path(tmp) / "head"
            for tree, names in ((base, ["a.bin", "b.bin"]), (head, ["a.bin", "c.bin"])):
                (tree / "fixtures").mkdir(parents=True)
                for name in names:
                    (tree / "fixtures" / name).write_bytes(b"x")
            fixtures, missing = schedule_diff.find_fixtures(base, head, "fixtures/*.bin")
        self.assertEqual([path.name for path in fixtures], ["a.bin", "c.bin"])
        self.assertEqual(missing, ["fixtures/b.bin"])


if __name__ == "__main__":
    unittest.main()
