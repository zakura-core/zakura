"""Observable contracts for release range, provider spending, and publication."""

from __future__ import annotations

import copy
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from release_review.__main__ import main
from release_review.config import Settings, load_settings
from release_review.contracts import (
    ModelReply,
    ModelRequest,
    ProviderError,
    ReviewError,
    parse_turn,
)
from release_review.engine import batch_units, review, synthesize
from release_review.github import (
    COMMENT_MARKER,
    REQUEST_WORKFLOW,
    publish,
    resolve_request,
)
from release_review.models import (
    AbliteratedProvider,
    NoRedirect,
    OpenAIProvider,
    Spending,
)
from release_review.reporting import render, write_report
from release_review.repository import Repository, ReviewUnit
from release_review.scope import exclusion_paths, matching_exclusion

ROOT = Path(__file__).resolve().parents[2]
HEAD = "b" * 40
BASE = "a" * 40


def settings(**changes):
    base = Settings("openai", "fixture-model", "https://api.openai.com/v1/responses",
                    Decimal(1), Decimal(2), Decimal(25), 120000, 4096,
                    "medium", 24000, 300, "Find concrete bugs only.", "digest")
    return replace(base, **changes)


def completed(findings=None, summary="Checked the changed validation boundary."):
    return {"complete": True, "summary": summary, "actions": [], "findings": findings or []}


def candidate(path="validator.rs", quote="accept_unchecked(block)"):
    return {"severity": "Critical", "category": "consensus", "title": "Invalid blocks bypass validation",
            "trigger": "An untrusted peer supplies an invalid block.", "impact": "Invalid state can be committed.",
            "explanation": "The new path calls acceptance before checking the block.",
            "change_path": path, "release_connection": "The validation call was removed in this release.",
            "verification": "Submit a malformed block and assert rejection.",
            "reference": {"revision": "head", "path": path, "start": 1, "end": 1, "quote": quote}}


class ScriptedProvider:
    def __init__(self, turns):
        self.turns, self.requests = list(turns), []

    def complete(self, request):
        self.requests.append(request)
        result = self.turns.pop(0)
        if isinstance(result, Exception):
            raise result
        return ModelReply(json.dumps(result), 100, 100)


class Transport:
    def __init__(self, response):
        self.response, self.requests = response, []

    def post(self, endpoint, key, body):
        self.requests.append((endpoint, body))
        return self.response


class GitFixture(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.git("init", "-q", "-b", "main")
        self.git("config", "user.name", "Review fixture")
        self.git("config", "user.email", "fixture@example.invalid")
        self.git("config", "commit.gpgsign", "false")
        self.git("config", "core.hooksPath", os.devnull)
        (self.root / "validator.rs").write_text("validate(block)\n")
        (self.root / "unchanged.rs").write_text("fn related_validation() {}\n")
        self.commit("baseline")
        self.base = self.git("rev-parse", "HEAD").strip()
        self.git("tag", "v1.0.0")
        # This change is already on main, before release preparation.
        (self.root / "validator.rs").write_text("accept_unchecked(block)\n")
        self.commit("change already on main")
        (self.root / "release.txt").write_text("release v1.1.0\n")
        self.commit("prepare release")
        self.head = self.git("rev-parse", "HEAD").strip()
        self.repository = Repository(self.root)
        self.repository.pin(self.head, "")

    def git(self, *args):
        return subprocess.run(["git", *args], cwd=self.root, capture_output=True, text=True,
                              timeout=10, check=True).stdout

    def commit(self, message):
        self.git("add", "--all")
        self.git("commit", "-qm", message)

    def metadata(self):
        return {"repository": "zakura-core/zakura", "pr_number": 12, "head_sha": self.head,
                "base_sha": self.base, "base_tag": "v1.0.0", "run_number": 2, "run_attempt": 1,
                "run_url": "https://github.com/zakura-core/zakura/actions/runs/22"}


class RangeTests(GitFixture):
    def test_literal_exclusions_skip_diffs_but_keep_context_and_manifest(self):
        for path in (".github/workflows/review.yml", ".github/workflows/nested/test.yml",
                     ".github/workflows-old/review.yml", "release.txt.backup"):
            file = self.root / path
            file.parent.mkdir(parents=True, exist_ok=True)
            file.write_text("related context\n")
        self.commit("workflow examples")
        self.repository.pin(self.git("rev-parse", "HEAD").strip(), "")
        units, coverage = self.repository.units(24000, (".github/workflows/", "release.txt"))
        skipped = {entry["path"]: entry for entry in coverage if entry["status"] == "out_of_scope"}
        self.assertEqual(set(skipped), {".github/workflows/review.yml", ".github/workflows/nested/test.yml", "release.txt"})
        self.assertTrue(all(not entry["units"] for entry in skipped.values()))
        self.assertEqual(skipped[".github/workflows/review.yml"]["exclusion_rule"], ".github/workflows/")
        self.assertEqual({unit.path for unit in units},
                         {"validator.rs", ".github/workflows-old/review.yml", "release.txt.backup"})
        self.assertIn("related context", self.repository.read("head", ".github/workflows/review.yml", 1, 1)["text"])

    def test_renamed_file_leaving_excluded_path_is_reviewed_at_new_path(self):
        self.git("mv", "validator.rs", "renamed.rs")
        self.commit("move out of excluded path")
        self.repository.pin(self.git("rev-parse", "HEAD").strip(), "")
        units, coverage = self.repository.units(24000, ("validator.rs",))
        self.assertEqual({unit.path for unit in units}, {"renamed.rs", "release.txt"})
        deleted = next(entry for entry in coverage if entry["path"] == "validator.rs")
        self.assertEqual(deleted["status"], "out_of_scope")

    def test_repository_output_limits_are_enforced_and_search_marks_truncation(self):
        with self.assertRaisesRegex(ReviewError, "bounded repository read"):
            self.repository.git("show", self.head + ":validator.rs", max_bytes=3)
        (self.root / "matches.txt").write_text(("needle " + "x" * 9000 + "\n") * 12)
        self.commit("many search results")
        self.repository.pin(self.git("rev-parse", "HEAD").strip(), "")
        result = self.repository.search("head", "needle")
        self.assertTrue(result["truncated"])
        self.assertLessEqual(len(result["matches"]), 20000)

    def test_review_includes_code_already_on_main(self):
        units, coverage = self.repository.units(24000)
        self.assertEqual({entry["path"] for entry in coverage}, {"validator.rs", "release.txt"})
        self.assertIn("+accept_unchecked(block)", "".join(unit.patch for unit in units))

    def test_metadata_pins_tag_commit_and_detects_moved_tag(self):
        marker = '<!-- release-review-base: ' + json.dumps({"tag": "v1.0.0", "sha": self.base}) + ' -->'
        resolved = self.repository.pin(self.head, marker)
        self.assertEqual(resolved["baseline_source"], "release preparation metadata")
        self.git("tag", "-f", "v1.0.0", self.head)
        with self.assertRaisesRegex(ReviewError, "no longer matches"):
            self.repository.pin(self.head, marker)

    def test_nearest_tag_uses_ancestry_not_largest_version(self):
        self.git("tag", "v99.0.0", self.base)
        self.git("tag", "v1.0.1", "HEAD~1")
        self.assertEqual(self.repository.pin(self.head, "")["base_tag"], "v1.0.1")

    def test_nonancestor_and_option_like_tag_are_rejected(self):
        self.git("checkout", "--orphan", "unrelated")
        self.commit("unrelated root")
        self.git("tag", "v2.0.0")
        with self.assertRaises(ReviewError):
            self.repository.pin(self.head, "", "v2.0.0")
        with self.assertRaises(ReviewError):
            self.repository.pin(self.head, "", "--help")

    def test_context_reads_both_revisions_and_unchanged_files(self):
        self.assertIn("validate(block)", self.repository.read("base", "validator.rs", 1, 1)["text"])
        self.assertIn("related_validation", self.repository.read("head", "unchanged.rs", 1, 1)["text"])
        self.assertIn("validator.rs", self.repository.search("head", "accept_unchecked")["matches"])

    def test_invalid_paths_and_ranges_do_not_reach_host_files(self):
        for path in ("../secret", "/etc/passwd", "x/../../secret", "x\nsecret"):
            with self.subTest(path=path), self.assertRaises(ReviewError):
                self.repository.read("head", path, 1, 1)
        with self.assertRaises(ReviewError):
            self.repository.read("head", "validator.rs", 1, 1000)

    def test_symlink_binary_and_large_lines_are_explicit_gaps(self):
        (self.root / "link").symlink_to("/etc/passwd")
        (self.root / "binary").write_bytes(b"\0unreviewable")
        (self.root / "long.txt").write_text("x" * 2000)
        self.commit("unreviewable files")
        self.repository.pin(self.git("rev-parse", "HEAD").strip(), "")
        _, coverage = self.repository.units(1000)
        gaps = {entry["path"] for entry in coverage if entry["status"] == "excluded"}
        self.assertEqual(gaps, {"link", "binary", "long.txt"})

    def test_large_patch_split_preserves_all_patch_bytes(self):
        (self.root / "many.txt").write_text("".join(f"line {i}\n" for i in range(500)))
        self.commit("large diff")
        self.repository.pin(self.git("rev-parse", "HEAD").strip(), "")
        units, _ = self.repository.units(1000)
        parts = [unit for unit in units if unit.path == "many.txt"]
        self.assertGreater(len(parts), 1)
        self.assertTrue(all(len(unit.patch.encode()) <= 1000 for unit in parts))
        patch_text = self.repository.git("diff", "--no-ext-diff", "--no-textconv", "--no-renames", "--unified=12",
                                         self.repository.refs["base"], self.repository.refs["head"], "--", "many.txt").decode()
        self.assertEqual("".join(unit.patch for unit in parts), patch_text)

    def test_source_quote_and_changed_path_are_verified(self):
        self.repository.validate_finding(candidate(), {"validator.rs"})
        wrong = candidate(quote="invented source")
        with self.assertRaisesRegex(ReviewError, "quotation"):
            self.repository.validate_finding(wrong, {"validator.rs"})
        with self.assertRaisesRegex(ReviewError, "changed release file"):
            self.repository.validate_finding(candidate(), {"release.txt"})

    def test_rename_and_deletion_are_not_silently_lost(self):
        self.git("mv", "validator.rs", "renamed.rs")
        (self.root / "release.txt").unlink()
        self.commit("rename and delete")
        self.repository.pin(self.git("rev-parse", "HEAD").strip(), "")
        units, coverage = self.repository.units(24000)
        self.assertEqual({entry["path"] for entry in coverage}, {"validator.rs", "renamed.rs"})
        self.assertIn("-validate(block)", "".join(unit.patch for unit in units))


class ContractTests(unittest.TestCase):
    def test_supported_severities_and_domains(self):
        for severity in ("Critical", "High", "Medium"):
            for category in ("consensus", "cryptography", "mining", "propagation"):
                finding = candidate()
                finding.update(severity=severity, category=category)
                self.assertEqual(parse_turn(json.dumps(completed([finding])))["findings"], [finding])

    def test_nit_severity_and_missing_evidence_are_rejected(self):
        for key, value in (("severity", "Low"), ("category", "style"), ("trigger", ""), ("impact", "")):
            finding = candidate()
            finding[key] = value
            with self.subTest(key=key), self.assertRaises(ReviewError):
                parse_turn(json.dumps(completed([finding])))

    def test_invalid_json_and_extra_fields_are_rejected(self):
        for text in ("not json", "```json\n{}\n```", json.dumps({**completed(), "shell": "anything"})):
            with self.assertRaises(ReviewError):
                parse_turn(text)

    def test_complete_cannot_request_actions(self):
        action = {"kind": "read", "revision": "head", "path": "x", "start": 1, "end": 1, "query": ""}
        with self.assertRaises(ReviewError):
            parse_turn(json.dumps({**completed(), "actions": [action]}))


class ProviderTests(unittest.TestCase):
    def request(self):
        return ModelRequest([{"role": "user", "content": "review fixture"}], 4096)

    def test_openai_responses_schema_storage_and_usage(self):
        transport = Transport({"status": "completed", "service_tier": "default", "output": [
            {"type": "reasoning"}, {"type": "message", "content": [{"type": "output_text", "text": json.dumps(completed())}]}],
            "usage": {"input_tokens": 20, "output_tokens": 30}})
        reply = OpenAIProvider(settings(), "fixture", transport).complete(self.request())
        body = transport.requests[0][1]
        self.assertFalse(body["store"])
        self.assertEqual(body["service_tier"], "default")
        self.assertTrue(body["text"]["format"]["strict"])
        self.assertEqual(body["reasoning"]["effort"], "medium")
        self.assertEqual(reply.output_tokens, 30)

    def test_openai_unconfirmed_service_pricing_is_not_accepted(self):
        for tier in (None, "priority", "auto"):
            transport = Transport({"status": "completed", "service_tier": tier, "output": [],
                                   "usage": {"input_tokens": 20, "output_tokens": 30}})
            with self.subTest(tier=tier), self.assertRaisesRegex(ProviderError, "standard service pricing"):
                OpenAIProvider(settings(), "fixture", transport).complete(self.request())

    def test_openai_preserves_astra_and_high_reasoning(self):
        transport = Transport({"status": "completed", "service_tier": "default", "output": [],
                               "usage": {"input_tokens": 20, "output_tokens": 30}})
        config = settings(model="gpt-6-astra", reasoning_effort="high")
        OpenAIProvider(config, "fixture", transport).complete(self.request())
        body = transport.requests[0][1]
        self.assertEqual(body["model"], "gpt-6-astra")
        self.assertEqual(body["reasoning"], {"effort": "high"})

    def test_openai_incomplete_and_refusal_are_not_clean(self):
        for envelope in (
            {"status": "incomplete"},
            {"status": "completed", "output": [{"type": "message", "content": [{"type": "refusal"}]}]},
        ):
            with self.assertRaises(ProviderError):
                OpenAIProvider(settings(), "fixture", Transport(envelope)).complete(self.request())

    def test_abliterated_does_not_require_native_tool_support(self):
        transport = Transport({"choices": [{"finish_reason": "stop", "message": {"content": json.dumps(completed())}}],
                               "usage": {"prompt_tokens": 20, "completion_tokens": 30}})
        conf = settings(provider="abliterated", endpoint="https://example.invalid/v1/chat/completions")
        result = AbliteratedProvider(conf, "fixture", transport).complete(self.request())
        self.assertEqual(result.input_tokens, 20)
        self.assertEqual(transport.requests[0][0], conf.endpoint)
        self.assertNotIn("tools", transport.requests[0][1])
        self.assertNotIn("reasoning", transport.requests[0][1])

    def test_missing_usage_and_truncation_fail_closed(self):
        for usage in ({}, {"prompt_tokens": True, "completion_tokens": 10}):
            response = {"choices": [{"finish_reason": "stop", "message": {"content": "{}"}}], "usage": usage}
            with self.assertRaises(ProviderError):
                AbliteratedProvider(settings(), "fixture", Transport(response)).complete(self.request())

    def test_redirect_cannot_forward_credentials(self):
        with self.assertRaises(ProviderError):
            NoRedirect().redirect_request(None, None, 302, "redirect", {}, "https://other.invalid")

    def test_budget_stops_before_request(self):
        provider = ScriptedProvider([completed()])
        spending = Spending(settings(budget=Decimal("0.00001")))
        with self.assertRaisesRegex(ReviewError, "budget exhausted"):
            spending.call(provider, self.request())
        self.assertEqual(provider.requests, [])

    def test_retry_reserves_unknown_cost_and_counts_actual_success(self):
        provider = ScriptedProvider([ProviderError("timeout", retryable=True), completed()])
        spending = Spending(settings())
        spending.call(provider, self.request(), sleep=lambda _: None)
        self.assertEqual(spending.calls, 2)
        self.assertEqual(spending.uncertain_calls, 1)
        self.assertGreater(spending.charged, spending.price(100, 100))

    def test_retry_cannot_spend_past_the_remaining_budget(self):
        request = self.request()
        reserve = Spending(settings()).price(Spending.input_bound(request), request.max_output_tokens)
        spending = Spending(settings(budget=reserve * Decimal("1.5")))
        provider = ScriptedProvider([ProviderError("timeout", retryable=True), completed()])
        with self.assertRaisesRegex(ReviewError, "budget exhausted"):
            spending.call(provider, request, sleep=lambda _: None)
        self.assertEqual(spending.charged, reserve)
        self.assertEqual(len(provider.requests), 1)

    def test_inflight_call_is_checkpointed_as_uncertain_before_network_io(self):
        spending = Spending(settings())
        checkpoints = []
        provider = ScriptedProvider([completed()])
        spending.call(provider, self.request(), checkpoint=lambda: checkpoints.append(spending.report()))
        self.assertEqual(checkpoints[0]["uncertain_calls"], 1)
        self.assertGreater(Decimal(checkpoints[0]["estimated_usd_including_uncertain_calls"]), 0)
        self.assertEqual(spending.uncertain_calls, 0)

    def test_expired_review_does_not_start_a_request(self):
        provider = ScriptedProvider([])
        with self.assertRaisesRegex(ReviewError, "time budget"):
            Spending(settings()).call(provider, self.request(), deadline=time.monotonic() - 1)
        self.assertEqual(provider.requests, [])

    def test_auth_and_unknown_usage_are_not_retried(self):
        provider = ScriptedProvider([ProviderError("HTTP 401")])
        spending = Spending(settings())
        with self.assertRaises(ProviderError):
            spending.call(provider, self.request())
        self.assertEqual(spending.calls, 1)
        self.assertGreater(spending.charged, 0)

    def test_model_context_limit_is_checked_before_spending(self):
        provider = ScriptedProvider([])
        with self.assertRaisesRegex(ReviewError, "input allowance"):
            Spending(settings(max_input_bytes=100)).call(provider, self.request())
        self.assertEqual(provider.requests, [])


class EngineTests(GitFixture):
    def test_scope_exclusions_are_reported_separately_from_reviewed_files(self):
        provider = ScriptedProvider([completed([candidate()]), completed([candidate()])])
        report = self.metadata()
        review(self.repository, provider, settings(excluded_paths=("release.txt",)), report, lambda _: None)
        self.assertEqual(report["status"], "complete")
        self.assertEqual(report["excluded_paths"], ["release.txt"])
        self.assertEqual({entry["path"]: entry["status"] for entry in report["coverage"]},
                         {"release.txt": "out_of_scope", "validator.rs": "reviewed"})
        for request in provider.requests:
            self.assertEqual(json.loads(request.messages[-1]["content"])["excluded_paths"], ["release.txt"])
        markdown = render(report)
        self.assertIn("complete within configured scope", markdown)
        self.assertIn("1/1 in-scope changed files", markdown)
        self.assertIn("2 total changed files; 1 excluded by configuration", markdown)
        self.assertIn("### Configured exclusions", markdown)
        self.assertNotIn("### Coverage gaps", markdown)

    def test_excluding_every_change_is_incomplete_without_spending(self):
        provider = ScriptedProvider([])
        report = self.metadata()
        review(self.repository, provider, settings(excluded_paths=("release.txt", "validator.rs")), report, lambda _: None)
        self.assertEqual(report["status"], "incomplete")
        self.assertIn("No changed files remain", report["errors"][0])
        self.assertFalse(report["synthesis_complete"])
        self.assertEqual(provider.requests, [])
        self.assertIn("not a clean review", render(report))

    def test_excluded_change_cannot_anchor_a_finding(self):
        provider = ScriptedProvider([completed([candidate()])])
        report = self.metadata()
        review(self.repository, provider, settings(excluded_paths=("validator.rs",)), report, lambda _: None)
        self.assertEqual(report["status"], "incomplete")
        self.assertIn("changed release file", report["errors"][0])
        self.assertEqual(report["findings"], [])

    def test_unreadable_in_scope_files_remain_gaps_with_configured_exclusions(self):
        (self.root / "binary").write_bytes(b"\0unreviewable")
        self.commit("binary change")
        self.repository.pin(self.git("rev-parse", "HEAD").strip(), "")
        provider = ScriptedProvider([completed(), completed()])
        report = self.metadata()
        review(self.repository, provider, settings(excluded_paths=("release.txt",)), report, lambda _: None)
        self.assertEqual(report["status"], "incomplete")
        statuses = {entry["path"]: entry["status"] for entry in report["coverage"]}
        self.assertEqual(statuses["binary"], "excluded")
        self.assertEqual(statuses["release.txt"], "out_of_scope")
        markdown = render(report)
        self.assertIn("### Configured exclusions", markdown)
        self.assertIn("### Coverage gaps", markdown)
        self.assertIn("not a clean review", markdown)

    def test_batching_prioritizes_core_paths_and_preserves_every_unit_once(self):
        units = [ReviewUnit("doc", "docs/change.md", 1, "d" * 200),
                 ReviewUnit("network", "crates/zakura-network/src/lib.rs", 1, "n" * 600),
                 ReviewUnit("validation", "crates/zakura-consensus/src/lib.rs", 1, "c" * 600),
                 ReviewUnit("chain", "crates/zakura-chain/src/lib.rs", 1, "c" * 300),
                 ReviewUnit("benchmark", "crates/zakura-chain/benches/check.rs", 1, "b" * 100)]
        batches = list(batch_units(units, 1000))
        self.assertEqual({item.id for item in batches[0]}, {"chain", "validation"})
        self.assertEqual([item.id for batch in batches for item in batch],
                         ["chain", "validation", "network", "benchmark", "doc"])
        self.assertTrue(all(sum(len(item.patch.encode()) for item in batch) <= 1000 for batch in batches))

    def test_small_file_batches_leave_room_for_source_investigation(self):
        units = [ReviewUnit(str(i), f"crates/zakura-chain/src/file{i}.rs", 1, "small patch") for i in range(9)]
        batches = list(batch_units(units, 24000))
        self.assertEqual([len(batch) for batch in batches], [4, 4, 1])
        self.assertEqual([unit.id for batch in batches for unit in batch], [str(i) for i in range(9)])

    def test_small_files_share_a_request_but_keep_individual_coverage(self):
        provider = ScriptedProvider([completed(), completed()])
        report = self.metadata()
        review(self.repository, provider, settings(), report, lambda _: None)
        self.assertEqual(report["status"], "complete")
        self.assertEqual(len(provider.requests), 2)
        request_units = json.loads(provider.requests[0].messages[-1]["content"])["units"]
        self.assertEqual({item["path"] for item in request_units}, {"release.txt", "validator.rs"})
        self.assertTrue(all(item["status"] == "reviewed" for item in report["coverage"]))

    def test_large_synthesis_reduces_all_summaries_without_truncating_them(self):
        report = self.metadata()
        report.update(summaries=[{"unit": f"unit-{i}", "summary": "x" * 2000} for i in range(24)], candidates=[])
        conf = settings(max_input_bytes=30000)
        provider = ScriptedProvider([completed()] * 30)
        synthesize(self.repository, provider, conf, Spending(conf), report,
                   time.monotonic() + 60, {"validator.rs"}, lambda: None)
        self.assertGreater(len(provider.requests), 1)
        self.assertTrue(all(Spending.input_bound(request) <= conf.max_input_bytes for request in provider.requests))
        first_pass = [json.loads(request.messages[-1]["content"])["review_results"]
                      for request in provider.requests[:-1]]
        observed = {node["summary"]["unit"] for group in first_pass for node in group}
        self.assertEqual(observed, {f"unit-{i}" for i in range(24)})

    def test_full_finding_page_is_incomplete_instead_of_silently_dropping_bugs(self):
        report = self.metadata()
        provider = ScriptedProvider([completed([candidate()] * 12)])
        review(self.repository, provider, settings(), report, lambda _: None)
        self.assertEqual(report["status"], "incomplete")
        self.assertIn("finding limit", report["errors"][0])

    def test_complete_review_keeps_verified_finding_and_covers_every_unit(self):
        provider = ScriptedProvider([completed([candidate()]), completed([candidate()])])
        report, checkpoints = self.metadata(), []
        review(self.repository, provider, settings(), report, lambda value: checkpoints.append(copy.deepcopy(value)))
        self.assertEqual(report["status"], "complete")
        self.assertEqual(report["reasoning_effort"], "medium")
        self.assertTrue(all(item["status"] == "reviewed" for item in report["coverage"]))
        self.assertEqual(report["findings"], [candidate()])
        self.assertTrue(report["synthesis_complete"])
        self.assertEqual(checkpoints[0]["status"], "incomplete")

    def test_context_tools_do_not_execute_source(self):
        action = {"kind": "read", "revision": "base", "path": "validator.rs", "start": 1, "end": 1, "query": ""}
        provider = ScriptedProvider([{"complete": False, "summary": "Need baseline", "actions": [action], "findings": []},
                                     completed(), completed(), completed()])
        report = self.metadata()
        review(self.repository, provider, settings(), report, lambda _: None)
        self.assertEqual(report["status"], "complete")
        self.assertIn("validate(block)", provider.requests[1].messages[-1]["content"])

    def test_batch_and_synthesis_can_each_request_more_than_sixteen_context_turns(self):
        action = {"kind": "read", "revision": "base", "path": "validator.rs", "start": 1, "end": 1, "query": ""}
        context = {"complete": False, "summary": "Inspecting source evidence", "actions": [action], "findings": []}
        provider = ScriptedProvider([context] * 20 + [completed()] + [context] * 20 + [completed()])
        report = self.metadata()
        review(self.repository, provider, settings(), report, lambda _: None)
        self.assertEqual(len(provider.requests), 42)
        self.assertEqual(report["status"], "complete")
        self.assertTrue(report["synthesis_complete"])
        self.assertTrue(all(entry["status"] == "reviewed" for entry in report["coverage"]))
        self.assertNotIn("response_turn_limit", json.loads(provider.requests[0].messages[1]["content"]))
        self.assertNotIn("remaining_response_turns", json.loads(provider.requests[1].messages[-1]["content"]))

    def test_repeated_context_requests_stop_at_the_shared_cost_ceiling(self):
        action = {"kind": "read", "revision": "base", "path": "validator.rs", "start": 1, "end": 1, "query": ""}
        context = {"complete": False, "summary": "Inspecting source evidence", "actions": [action], "findings": []}
        provider = ScriptedProvider([context] * 1000)
        report = self.metadata()
        config = settings(budget=Decimal("0.08"))
        review(self.repository, provider, config, report, lambda _: None)
        self.assertGreater(len(provider.requests), 16)
        self.assertEqual(report["status"], "incomplete")
        self.assertEqual(report["errors"], ["API budget exhausted before the next request"])
        self.assertLessEqual(Decimal(report["usage"]["estimated_usd_including_uncertain_calls"]), config.budget)
        self.assertFalse(report["synthesis_complete"])
        self.assertTrue(all(entry["status"] == "pending" for entry in report["coverage"]))

    def test_compatible_provider_does_not_claim_an_openai_reasoning_setting(self):
        provider = ScriptedProvider([completed(), completed()])
        report = self.metadata()
        review(self.repository, provider, settings(provider="abliterated"), report, lambda _: None)
        self.assertEqual(report["reasoning_effort"], "provider default")

    def test_invalid_provider_output_preserves_incomplete_checkpoint(self):
        provider = ScriptedProvider([{"invalid": "schema"}])
        report = self.metadata()
        review(self.repository, provider, settings(), report, lambda _: None)
        self.assertEqual(report["status"], "incomplete")
        self.assertTrue(report["errors"])
        self.assertTrue(all(item["status"] == "pending" for item in report["coverage"]))

    def test_budget_exhaustion_never_becomes_clean(self):
        provider = ScriptedProvider([])
        report = self.metadata()
        review(self.repository, provider, settings(budget=Decimal("0.00001")), report, lambda _: None)
        self.assertEqual(report["status"], "incomplete")
        self.assertIn("not a clean review", render(report))

    def test_refuted_candidate_is_not_a_final_finding(self):
        provider = ScriptedProvider([completed([candidate()]), completed()])
        report = self.metadata()
        review(self.repository, provider, settings(), report, lambda _: None)
        self.assertEqual(report["findings"], [])
        self.assertEqual(len(report["candidates"]), 1)

    def test_invented_source_citation_causes_incomplete_review(self):
        provider = ScriptedProvider([completed([candidate(quote="invented")])])
        report = self.metadata()
        review(self.repository, provider, settings(), report, lambda _: None)
        self.assertEqual(report["status"], "incomplete")
        self.assertIn("quotation", report["errors"][0])

    def test_model_timeout_is_reported_without_raw_provider_payload(self):
        provider = ScriptedProvider([ProviderError("provider transport failed or timed out")])
        report = self.metadata()
        review(self.repository, provider, settings(), report, lambda _: None)
        self.assertEqual(report["status"], "incomplete")
        self.assertEqual(report["usage"]["uncertain_calls"], 1)


def pull_request():
    return {"number": 12, "state": "open", "draft": False, "body": "",
            "base": {"ref": "main"}, "head": {"ref": "release/v1.1.0", "sha": HEAD,
            "repo": {"full_name": "zakura-core/zakura"}}}


class GitHubFixture:
    repository = "zakura-core/zakura"

    def __init__(self):
        self.pr = pull_request()
        self.saved, self.writes = [], []
        self.run = {"event": "pull_request", "conclusion": "success", "path": REQUEST_WORKFLOW,
                    "head_repository": {"full_name": self.repository},
                    "head_branch": "release/v1.1.0", "head_sha": HEAD}

    def comments(self, number):
        return copy.deepcopy(self.saved)

    def api(self, path, *, method="GET", body=None):
        if method != "GET":
            self.writes.append((path, method, body))
            comment = {"id": 1, "user": {"login": "github-actions[bot]"}, "body": body["body"],
                       "html_url": f"https://github.com/{self.repository}/pull/12#issuecomment-1"}
            self.saved = [comment]
            return comment
        if path == "issues/comments/1":
            return copy.deepcopy(self.saved[0])
        if path.startswith("actions/runs/"):
            return copy.deepcopy(self.run)
        if path.startswith("pulls?"):
            return [copy.deepcopy(self.pr)]
        return copy.deepcopy(self.pr)


class GitHubTests(unittest.TestCase):
    def test_manual_drafts_allowed_automatic_drafts_skipped(self):
        github = GitHubFixture()
        github.pr["draft"] = True
        request = {"ref": "refs/heads/main", "inputs": {"pr_number": "12"}}
        self.assertEqual(resolve_request(github, "workflow_dispatch", request)["pr_number"], 12)
        self.assertIsNone(resolve_request(github, "workflow_run", {"workflow_run": {"id": 20}}))

    def test_forks_hotfixes_wrong_base_and_closed_prs_are_ineligible(self):
        for mutation in (
            lambda pr: pr["head"]["repo"].update(full_name="fork/zakura"),
            lambda pr: pr["head"].update(ref="hotfix/v1.1.0"),
            lambda pr: pr["base"].update(ref="other"),
            lambda pr: pr.update(state="closed"),
        ):
            github = GitHubFixture()
            mutation(github.pr)
            with self.assertRaises(ReviewError):
                resolve_request(github, "workflow_dispatch", {"ref": "refs/heads/main", "inputs": {"pr_number": "12"}})

    def test_manual_workflow_cannot_run_unmerged_control_code(self):
        with self.assertRaisesRegex(ReviewError, "from main"):
            resolve_request(GitHubFixture(), "workflow_dispatch", {"ref": "refs/heads/feature", "inputs": {"pr_number": "12"}})

    def test_automatic_trigger_is_revalidated_against_current_head(self):
        github = GitHubFixture()
        github.run["head_sha"] = BASE
        self.assertIsNone(resolve_request(github, "workflow_run", {"workflow_run": {"id": 20}}))
        github.run["head_sha"] = HEAD
        github.run["path"] = "unrelated.yml"
        self.assertIsNone(resolve_request(github, "workflow_run", {"workflow_run": {"id": 20}}))

    def test_rerun_updates_single_comment_and_older_result_cannot_replace_it(self):
        github = GitHubFixture()
        report = {"pr_number": 12, "head_sha": HEAD, "run_number": 20, "run_attempt": 1}
        self.assertEqual(publish(github, report, "first"), "published")
        report["run_attempt"] = 2
        self.assertEqual(publish(github, report, "rerun"), "published")
        self.assertEqual(len(github.saved), 1)
        self.assertEqual(report["comment_url"], "https://github.com/zakura-core/zakura/pull/12#issuecomment-1")
        self.assertEqual([write[1] for write in github.writes], ["POST", "PATCH"])
        report["run_attempt"] = 1
        self.assertIn("superseded", publish(github, report, "old"))
        self.assertEqual(len(github.writes), 2)

    def test_comment_publication_requires_matching_bot_body_and_pr_link(self):
        for changes in ({"body": "wrong content"}, {"html_url": "https://github.com/other/repo/pull/12#issuecomment-1"},
                        {"user": {"login": "someone"}}):
            github = GitHubFixture()
            original = github.api

            def api(path, original=original, changes=changes, **kwargs):
                result = original(path, **kwargs)
                return result | changes if path == "issues/comments/1" and not kwargs else result

            report = {"pr_number": 12, "head_sha": HEAD, "run_number": 20, "run_attempt": 1}
            with self.subTest(changes=changes), patch.object(github, "api", side_effect=api), \
                    self.assertRaisesRegex(ReviewError, "publication is unconfirmed"):
                publish(github, report, "completed review")
            self.assertNotIn("comment_url", report)

    def test_stale_head_never_publishes(self):
        github = GitHubFixture()
        report = {"pr_number": 12, "head_sha": BASE, "run_number": 20, "run_attempt": 1}
        self.assertIn("stale", publish(github, report, "old"))
        self.assertEqual(github.writes, [])

    def test_user_supplied_comment_marker_is_not_updated(self):
        github = GitHubFixture()
        github.saved = [{"id": 99, "user": {"login": "someone"}, "body": COMMENT_MARKER}]
        publish(github, {"pr_number": 12, "head_sha": HEAD, "run_number": 20, "run_attempt": 1}, "new")
        self.assertEqual(github.writes[0][1], "POST")


class ConfigurationAndReportTests(unittest.TestCase):
    def model_json(self, **changes):
        config = {"model": "explicit-model", "input_usd_per_million": "1", "output_usd_per_million": "2",
                  "max_input_bytes": 120000}
        config.update(changes)
        return json.dumps({"openai": config, "abliterated": config})

    def test_real_prompts_load_and_are_fingerprinted(self):
        conf = load_settings(ROOT / "release-review", "openai", "default", self.model_json())
        self.assertEqual(len(conf.prompt_digest), 64)
        self.assertIn("Do not invent measurements", conf.instructions)
        self.assertEqual(conf.budget, Decimal(25))
        for path in (".github/workflows/example.yml", "crates/zakura-sim/src/lib.rs",
                     "crates/zakura-rt/src/sim/task.rs", "crates/zakura-consensus/src/block/tests.rs",
                     "crates/zakurad/tests/acceptance.rs", "docs/specs/zakura-quic.md"):
            self.assertIsNotNone(matching_exclusion(path, conf.excluded_paths))
        for path in ("Cargo.lock", "crates/zakura-chain/Cargo.toml", "crates/zakura-rt/src/lib.rs",
                     "crates/zakura-network/src/zakura/block_sync.rs", "crates/xtask/src/main.rs",
                     "crates/zakura-state/src/service/finalized_state/vct/mainnet-subtrees.bin"):
            self.assertIsNone(matching_exclusion(path, conf.excluded_paths))

    def test_exclusions_reject_ambiguous_or_unsafe_configuration(self):
        for value in ("*.rs", [""], ["/"], ["/etc/passwd"], ["../file"], ["dir/../file"],
                      ["dir//file"], ["./file"], ["dir\\file"], ["*.rs"], ["file?"],
                      ["file[0]"], ["file\n"], [" file"], [True], ["x"] * 257):
            with self.subTest(value=value), self.assertRaises(ReviewError):
                exclusion_paths({"exclude_paths": value})
        with self.assertRaises(ReviewError):
            exclusion_paths({"exclude_path": ["file"]})
        self.assertEqual(exclusion_paths({}), ())
        self.assertEqual(exclusion_paths({"exclude_paths": ["file", "dir/", "file"]}), ("file", "dir/"))

    def test_comment_omits_exclusion_paths_but_preserves_counts_and_full_artifact(self):
        report = {"head_sha": HEAD, "status": "complete", "excluded_paths": ["docs/"],
                  "coverage": [{"path": f"docs/file-{i}.md", "status": "out_of_scope",
                                "exclusion_rule": "docs/"} for i in range(25)]}
        comment = render(report, comment=True)
        self.assertIn("25 excluded by configuration", comment)
        self.assertNotIn("Configured exclusions", comment)
        self.assertNotIn("docs/", comment)
        self.assertIn("Configured exclusions", render(report))
        self.assertIn("file\\-24", render(report))

    def test_comment_prioritizes_results_and_keeps_diagnostics_in_full_report(self):
        report = {"repository": "zakura-core/zakura", "head_sha": HEAD, "base_sha": BASE, "base_tag": "v1.0.0",
                  "status": "complete", "synthesis_complete": True, "model": "gpt-6.1-sol",
                  "reasoning_effort": "medium", "prompt_digest": "diagnostic-prompt-hash",
                  "coverage": [{"path": "validator.rs", "status": "reviewed"}],
                  "usage": {"estimated_usd_including_uncertain_calls": "18.1124639", "budget_usd": "25",
                            "cached_input_tokens": 4779214}, "duration_seconds": 120,
                  "resume_history": [{"duration_seconds": 60}]}
        comment = render(report, comment=True)
        self.assertIn("**No supported release-blocking bugs found in the reviewed scope.**", comment)
        self.assertIn("**1/1 files reviewed**", comment)
        self.assertIn("$18.11 / $25.00", comment)
        self.assertIn("3.0 minutes", comment)
        self.assertIn(f"/commit/{HEAD}", comment)
        self.assertIn(f"/compare/{BASE}...{HEAD}", comment)
        for diagnostic in ("diagnostic-prompt-hash", "4779214", "saved attempt", "Reviewer revision"):
            self.assertNotIn(diagnostic, comment)
        full = render(report)
        self.assertIn("diagnostic-prompt-hash", full)
        self.assertIn("4779214", full)
        self.assertEqual(report["usage"]["estimated_usd_including_uncertain_calls"], "18.1124639")

    def test_incomplete_comment_keeps_failure_visible_and_folds_file_list(self):
        report = {"head_sha": HEAD, "status": "incomplete", "errors": ["API budget exhausted"],
                  "coverage": [{"path": "validator.rs", "status": "pending", "reason": ""}]}
        comment = render(report, comment=True)
        self.assertIn("INCOMPLETE", comment)
        self.assertIn("not a clean review", comment)
        self.assertLess(comment.index("API budget exhausted"), comment.index("<details>"))
        self.assertIn("<summary>Coverage gaps (1 file)</summary>", comment)
        self.assertIn("Final findings are not available", comment)
        self.assertIn("Unavailable / $25.00", comment)

    def test_model_prices_and_context_are_explicit(self):
        for changes in ({"model": ""}, {"input_usd_per_million": None}, {"output_usd_per_million": "NaN"},
                        {"max_input_bytes": None}):
            with self.subTest(changes=changes), self.assertRaises(ReviewError):
                load_settings(ROOT / "release-review", "openai", "default", self.model_json(**changes))

    def test_missing_configuration_and_path_traversal_are_rejected(self):
        for profile, models in (("default", ""), ("../../elsewhere", self.model_json()),
                                ("default", '{"openai": null}')):
            with self.assertRaises(ReviewError):
                load_settings(ROOT / "release-review", "openai", profile, models)

    def test_abliterated_requires_https_without_embedded_credentials(self):
        for url in ("http://example.invalid", "https://user:password@example.invalid", "https://example.invalid?token=fixture"):
            with self.assertRaises(ReviewError):
                load_settings(ROOT / "release-review", "abliterated", "default", self.model_json(base_url=url))
        conf = load_settings(ROOT / "release-review", "abliterated", "default", self.model_json(base_url="https://example.invalid/v1"))
        self.assertEqual(conf.endpoint, "https://example.invalid/v1/chat/completions")

    def test_incomplete_report_is_saved_and_does_not_announce_clean_result(self):
        report = {"repository": "zakura-core/zakura", "head_sha": HEAD, "status": "incomplete",
                  "errors": ["model configuration missing"], "coverage": [], "findings": []}
        with tempfile.TemporaryDirectory() as directory:
            write_report(Path(directory), report)
            saved = json.loads((Path(directory) / "report.json").read_text())
            self.assertEqual(saved["status"], "incomplete")
            markdown = (Path(directory) / "comment.md").read_text()
            self.assertIn("not a clean review", markdown)
            self.assertIn("model configuration missing", markdown)

    def test_model_text_cannot_inject_mentions_html_or_links(self):
        finding = candidate()
        finding["title"] = "@everyone <script> [click](https://elsewhere.invalid)"
        report = {"repository": "zakura-core/zakura", "head_sha": HEAD, "base_sha": BASE,
                  "status": "complete", "findings": [finding], "coverage": [], "synthesis_complete": True}
        markdown = render(report)
        self.assertNotIn("@everyone", markdown)
        self.assertNotIn("<script>", markdown)
        self.assertNotIn("[click]", markdown)
        comment = render(report, comment=True)
        self.assertNotIn("@everyone", comment)
        self.assertNotIn("<script>", comment)
        self.assertNotIn("[click]", comment)
        self.assertIn("- **Trigger:**", comment)


class CommandTests(GitFixture):
    def test_missing_configuration_malformed_response_and_budget_save_incomplete_artifacts(self):
        self.git("update-ref", "refs/release-review/head", self.head)
        context = self.metadata() | {"pr_body": "", "base_override": "", "provider": "openai", "profile": "default"}
        context_path = self.root / "context.json"
        context_path.write_text(json.dumps(context))
        cases = [
            ("missing", None, ScriptedProvider([]), "configure"),
            ("malformed", settings(), ScriptedProvider([{"broken": True}]), "model output"),
            ("budget", settings(budget=Decimal("0.00001")), ScriptedProvider([]), "budget exhausted"),
        ]
        for name, conf, provider, error_text in cases:
            output = self.root / name
            with self.subTest(name=name), patch.dict(os.environ, {"REVIEW_API_KEY": "fixture", "RELEASE_REVIEW_MODELS": ""}), \
                    patch("release_review.__main__.fetch_head"), \
                    patch("release_review.__main__.OpenAIProvider", return_value=provider):
                arguments = ["review", "--repo", str(self.root), "--config", str(ROOT / "release-review"),
                             "--context", str(context_path), "--output", str(output)]
                if conf is None:
                    result = main(arguments)
                else:
                    with patch("release_review.__main__.load_settings", return_value=conf):
                        result = main(arguments)
                self.assertEqual(result, 2)
                report = json.loads((output / "report.json").read_text())
                self.assertEqual(report["head_sha"], self.head)
                self.assertEqual(report["base_sha"], self.base)
                self.assertEqual(report["status"], "incomplete")
                self.assertIn(error_text, " ".join(report["errors"]))
                self.assertIn("not a clean review", (output / "comment.md").read_text())

    def test_missing_report_publishes_failure_and_rerun_updates_the_same_comment(self):
        github = GitHubFixture()
        context = self.metadata() | {"head_sha": HEAD, "pr_body": ""}
        context_path = self.root / "context.json"
        context_path.write_text(json.dumps(context))
        output = self.root / "publication"
        with patch.dict(os.environ, {"GITHUB_REPOSITORY": github.repository}), \
                patch("release_review.__main__.GitHub", return_value=github):
            arguments = ["publish", "--context", str(context_path), "--output", str(output)]
            self.assertEqual(main(arguments), 0)
            self.assertEqual(main(arguments), 0)
        self.assertEqual(len(github.saved), 1)
        self.assertIn("INCOMPLETE", github.saved[0]["body"])
        self.assertIn("stopped before producing", github.saved[0]["body"])
        self.assertEqual([write[1] for write in github.writes], ["POST", "PATCH"])
        self.assertTrue((output / "report.md").is_file())
        self.assertEqual(json.loads((output / "report.json").read_text())["comment_url"],
                         "https://github.com/zakura-core/zakura/pull/12#issuecomment-1")


if __name__ == "__main__":
    unittest.main()
