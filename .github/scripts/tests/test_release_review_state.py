"""Exercise typed progress, synthesis boundaries, and durable reservations."""

import json
import unittest
from dataclasses import asdict, replace
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from test_release_review import GitFixture, ScriptedProvider, candidate, completed, settings
from release_review.contracts import ModelRequest, ProviderError, ReviewError, parse_turn
from release_review.engine import deduplicate, review, synthesis_groups
from release_review.models import Spending
from release_review.reporting import write_checkpoint, write_report
from release_review.repository import ReviewUnit
from release_review.state import ReviewState


class StateTests(unittest.TestCase):
    def test_typed_turn_round_trips_the_existing_wire_format(self):
        for value in (
            completed([candidate()]),
            {"complete": False, "summary": "need source", "findings": [], "actions": [
                {"kind": "read", "revision": "head", "path": "validator.rs", "start": 1, "end": 3, "query": ""},
                {"kind": "search", "revision": "base", "path": "", "start": 1, "end": 1, "query": "validate"},
            ]},
        ):
            with self.subTest(value=value):
                self.assertEqual(asdict(parse_turn(json.dumps(value))), value)

    def test_saved_progress_does_not_change_when_review_continues(self):
        units = [ReviewUnit("one", "validator.rs", 1, "first"), ReviewUnit("two", "validator.rs", 2, "second")]
        state = ReviewState(coverage=[{"path": "validator.rs", "units": ["one", "two"], "status": "pending", "reason": ""}])
        state.record_batch(units[:1], parse_turn(json.dumps(completed([candidate()]))))
        first = state.snapshot()
        state.record_batch(units[1:], parse_turn(json.dumps(completed())))
        self.assertEqual(first["coverage"][0]["status"], "pending")
        self.assertEqual(first["coverage"][0]["reviewed_units"], 1)
        self.assertEqual(state.coverage[0]["status"], "reviewed")
        self.assertEqual(len(first["summaries"]), 1)
        self.assertEqual(first["candidates"], [candidate()])
        self.assertNotIn("completed", first)
        json.dumps(first)

    def test_deduplication_keeps_distinct_triggers_at_the_same_location(self):
        finding = parse_turn(json.dumps(completed([candidate()]))).findings[0]
        duplicate = replace(finding, title="Another description of the same bug")
        distinct = replace(finding, trigger="A different input triggers another bug")
        self.assertEqual(deduplicate([finding, duplicate, distinct]), [finding, distinct])

    def test_synthesis_grouping_preserves_order_and_enforces_reduction(self):
        nodes = [{"summary": str(index)} for index in range(5)]
        groups = synthesis_groups(nodes, lambda group: len(group) <= 2)
        self.assertEqual(groups, [nodes[:2], nodes[2:4], nodes[4:]])
        with self.assertRaisesRegex(ReviewError, "cannot reduce"):
            synthesis_groups(nodes, lambda group: len(group) <= 1)
        with self.assertRaisesRegex(ReviewError, "item exceeds"):
            synthesis_groups(nodes, lambda group: False)
        self.assertEqual(synthesis_groups([], lambda group: True), [])

    def test_checkpoint_write_does_not_render_documents(self):
        with TemporaryDirectory() as directory:
            output = Path(directory)
            report = {"head_sha": "a" * 40, "status": "incomplete"}
            write_report(output, report)
            original = (output / "report.md").read_text()
            report["errors"] = ["provider stopped"]
            with patch("release_review.reporting.render", side_effect=AssertionError("checkpoint rendered Markdown")):
                write_checkpoint(output, report)
            self.assertEqual(json.loads((output / "report.json").read_text()), report)
            self.assertEqual((output / "report.md").read_text(), original)
            write_report(output, report)
            self.assertIn("provider stopped", (output / "report.md").read_text())

    def test_failed_reservation_write_prevents_network_io(self):
        provider = ScriptedProvider([completed()])
        spending = Spending(settings())
        request = ModelRequest([{"role": "user", "content": "review"}], 4096)
        with patch("release_review.reporting.write_atomic", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                spending.call(provider, request, persist_reservation=lambda: write_checkpoint(Path("unused"), spending.report()))
        self.assertEqual(provider.requests, [])
        self.assertEqual(spending.uncertain_calls, 1)


class ReservationTests(GitFixture):
    def test_continuation_restores_typed_candidates_without_reviewing_completed_units(self):
        prior = self.metadata()
        review(self.repository, ScriptedProvider([completed([candidate()]), ProviderError("stop")]),
               settings(), prior, lambda report: None)
        self.assertEqual(prior["status"], "incomplete")
        original = json.dumps(prior, sort_keys=True)
        provider = ScriptedProvider([completed([candidate()])])
        report = self.metadata()
        review(self.repository, provider, settings(), report, lambda report: None, prior_report=prior)
        self.assertEqual(report["status"], "complete")
        self.assertEqual(report["findings"], [candidate()])
        self.assertEqual(len(provider.requests), 1)
        synthesis = json.loads(provider.requests[0].messages[-1]["content"])
        self.assertIn({"candidate": candidate()}, synthesis["review_results"])
        self.assertEqual(json.dumps(prior, sort_keys=True), original)

    def test_provider_sees_durable_reservations_before_each_retry(self):
        output = self.root / "report"
        observed = []

        class Provider:
            def complete(self, request):
                saved = json.loads((output / "report.json").read_text())
                observed.append(saved)
                if len(observed) == 1:
                    raise ProviderError("retry", retryable=True)
                raise ProviderError("stop")

        review(self.repository, Provider(), settings(), self.metadata(),
               lambda report: write_checkpoint(output, report))
        self.assertEqual([item["usage"]["uncertain_calls"] for item in observed], [1, 2])
        self.assertGreater(Decimal(observed[1]["usage"]["estimated_usd_including_uncertain_calls"]),
                           Decimal(observed[0]["usage"]["estimated_usd_including_uncertain_calls"]))
        saved = json.loads((output / "report.json").read_text())
        self.assertEqual(saved["status"], "incomplete")
        self.assertEqual(saved["errors"], ["stop"])
        self.assertEqual(saved["usage"]["uncertain_calls"], 2)
        self.assertFalse((output / "report.md").exists())
