"""Continuations reuse matching evidence and preserve the original API ceiling."""

import copy
import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from release_review.contracts import ProviderError, ReviewError
from release_review.engine import review
from test_release_review import GitFixture, ScriptedProvider, completed, settings


class CheckpointTests(GitFixture):
    def partial_review(self):
        file = self.root / "crates/zakura-chain/production.rs"
        file.parent.mkdir(parents=True)
        file.write_text("validate(block)\n")
        self.commit("production change")
        self.repository.pin(self.git("rev-parse", "HEAD").strip(), "")
        report = self.metadata() | {"control_sha": "first-reviewer"}
        provider = ScriptedProvider([completed(), ProviderError("temporary provider failure")])
        review(self.repository, provider, settings(), report, lambda _: None)
        self.assertEqual(report["status"], "incomplete")
        self.assertEqual(sum(entry["status"] == "reviewed" for entry in report["coverage"]), 1)
        return report

    def test_continue_only_unfinished_units_and_carry_all_usage(self):
        prior = self.partial_review()
        original = copy.deepcopy(prior)
        provider = ScriptedProvider([completed(), completed()])
        report = self.metadata() | {"control_sha": "next-reviewer"}
        review(self.repository, provider, settings(max_seconds=600), report, lambda _: None, prior_report=prior)
        self.assertEqual(report["status"], "complete")
        self.assertNotIn("production.rs", provider.requests[0].messages[-1]["content"])
        self.assertEqual(report["usage"]["calls"], prior["usage"]["calls"] + 2)
        self.assertEqual(report["usage"]["uncertain_calls"], prior["usage"]["uncertain_calls"])
        expected = Decimal(prior["usage"]["estimated_usd_including_uncertain_calls"]) + Decimal("0.0006")
        self.assertEqual(Decimal(report["usage"]["estimated_usd_including_uncertain_calls"]), expected)
        self.assertEqual(report["resume_history"][0]["control_sha"], "first-reviewer")
        self.assertEqual(prior, original)

    def test_changed_scope_prompt_model_or_pricing_refuses_reuse(self):
        prior = self.partial_review()
        for changes in ({"excluded_paths": ("release.txt",)}, {"prompt_digest": "different"},
                        {"model": "another-model"}, {"reasoning_effort": "high"},
                        {"input_price": Decimal(3)}, {"cached_input_price": Decimal("0.1")},
                        {"max_output_tokens": 8192}):
            provider = ScriptedProvider([])
            with self.subTest(changes=changes), self.assertRaisesRegex(ReviewError, "checkpoint revisions"):
                review(self.repository, provider, settings(**changes), self.metadata(), lambda _: None, prior_report=prior)
            self.assertEqual(provider.requests, [])

    def test_changed_revision_or_rendered_units_refuses_reuse(self):
        prior = self.partial_review()
        (self.root / "release.txt").write_text("another release\n")
        self.commit("new release head")
        self.repository.pin(self.git("rev-parse", "HEAD").strip(), "")
        with self.assertRaisesRegex(ReviewError, "checkpoint revisions"):
            review(self.repository, ScriptedProvider([]), settings(), self.metadata(), lambda _: None, prior_report=prior)

    def test_other_release_target_cannot_reuse_the_checkpoint(self):
        prior = self.partial_review()
        for key, value in (("repository", "another/repository"), ("pr_number", 999)):
            target = self.metadata() | {key: value}
            with self.subTest(key=key), self.assertRaisesRegex(ReviewError, "different release target"):
                review(self.repository, ScriptedProvider([]), settings(), target, lambda _: None, prior_report=prior)

    def test_exhausted_checkpoint_cannot_reset_the_budget(self):
        prior = self.partial_review()
        prior["usage"]["estimated_usd_including_uncertain_calls"] = "25"
        report = self.metadata()
        provider = ScriptedProvider([])
        review(self.repository, provider, settings(), report, lambda _: None, prior_report=prior)
        self.assertEqual(report["status"], "incomplete")
        self.assertIn("budget exhausted", report["errors"][0])
        self.assertEqual(provider.requests, [])
        self.assertEqual(report["usage"]["estimated_usd_including_uncertain_calls"], "25")

    def test_invalid_billing_and_duplicate_units_are_rejected(self):
        prior = self.partial_review()
        for value in ("-1", "NaN", "0", "26"):
            malformed = copy.deepcopy(prior)
            malformed["usage"]["estimated_usd_including_uncertain_calls"] = value
            with self.subTest(value=value), self.assertRaisesRegex(ReviewError, "billing"):
                review(self.repository, ScriptedProvider([]), settings(), self.metadata(), lambda _: None,
                       prior_report=malformed)
        prior["summaries"].append(prior["summaries"][0])
        with self.assertRaisesRegex(ReviewError, "duplicated completed units"):
            review(self.repository, ScriptedProvider([]), settings(), self.metadata(), lambda _: None, prior_report=prior)

    def test_cached_usage_survives_continuation_without_resetting_prior_cost(self):
        from release_review.checkpoints import contract_digest

        prior = self.partial_review()
        config = settings(cached_input_price=Decimal("0.1"))
        units, coverage = self.repository.units(config.unit_bytes, config.excluded_paths)
        prior["resume_contract_digest"] = contract_digest(self.repository, config, units, coverage)
        prior["usage"]["cached_input_tokens"] = 80
        original_charge = Decimal(prior["usage"]["estimated_usd_including_uncertain_calls"])
        report = self.metadata()
        review(self.repository, ScriptedProvider([completed(), completed()]), config, report, lambda _: None,
               prior_report=prior)
        self.assertEqual(report["status"], "complete")
        self.assertEqual(report["usage"]["cached_input_tokens"], 80)
        self.assertEqual(Decimal(report["usage"]["estimated_usd_including_uncertain_calls"]),
                         original_charge + Decimal("0.0006"))
