#!/usr/bin/env python3
"""Regression tests for the approval boundary; no credentials or network used."""

from copy import deepcopy
import base64
import http.client
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import adapter


HEAD = "a" * 40
BASE = "b" * 40
BOT_ID = 12345
APP_ID = 6789
POLICY = adapter.Policy.load()
NATIVE = {"id": POLICY.data["codex_user_id"], "type": "Bot"}
ACTOR = {"databaseId": POLICY.data["codex_user_id"], "login": "chatgpt-codex-connector"}
START = "2026-09-01T10:00:00.123456Z"
FINISH = "2026-09-01T10:02:00.123456Z"


def summary_body(completed=True, sha=HEAD[:7], trigger="Draft marked ready"):
    # This is the native summary table observed on both auto and manual reviews.
    status = "✅ **Completed**" if completed else "🔄 **Running** since"
    timestamp = FINISH if completed else START
    return (adapter.SUMMARY_MARKER + "\n\n## Codex Review Summary\n\n"
            "| Review | Status | Commit | Review trigger |\n"
            "| --- | --- | --- | --- |\n"
            f'| 📝 **Code Review** | {status} <relative-time datetime="{timestamp}">'
            f"{timestamp}</relative-time> | `{sha}` | {trigger} |\n")


def evidence(trigger="Draft marked ready"):
    body = summary_body(trigger=trigger)
    return {
        "policy": POLICY,
        "pull": {"head": {"sha": HEAD}},
        "comments": [{"id": 100, "node_id": "IC_test", "body": body,
                      "user": deepcopy(NATIVE),
                      "performed_via_github_app": {"id": POLICY.data["codex_app_id"]}}],
        "summary": {"databaseId": 100, "body": body,
                    "author": deepcopy(ACTOR), "editor": deepcopy(ACTOR),
                    "lastEditedAt": "2026-09-01T10:02:00Z",
                    "userContentEdits": {"pageInfo": {"hasNextPage": False}, "nodes": [
                        {"diff": body, "editedAt": "2026-09-01T10:02:00Z", "editor": deepcopy(ACTOR)},
                        {"diff": summary_body(False, trigger=trigger),
                         "editedAt": "2026-09-01T10:00:02Z", "editor": deepcopy(ACTOR)},
                    ]}},
        # GitHub's reaction endpoint reports this Bot's type as User; its stable
        # numeric ID matches the authenticated App summary and GraphQL Bot.
        "reactions": [{"id": 200, "content": "+1", "user": {**NATIVE, "type": "User"},
                       "created_at": "2026-09-01T10:02:03Z"}],
        "reviews": [], "threads": [], "resolved_sha": HEAD, "authorized_requesters": {900},
    }


def command(body="@codex review", created="2026-09-01T09:59:58Z"):
    return {"id": 99, "body": body, "created_at": created, "updated_at": created,
            "user": {"id": 900, "type": "User", "login": "contributor"}}


def native_review(when="2026-09-01T10:01:30Z", commit=HEAD):
    # GitHub REST reviews have no performed_via_github_app field.
    return {"id": 300, "state": "COMMENTED", "user": deepcopy(NATIVE),
            "commit_id": commit, "submitted_at": when}


def thread(resolved=False, native=True):
    return {"isResolved": resolved, "isOutdated": True,
            "comments": {"nodes": [{"author": deepcopy(ACTOR) if native else {"login": "human"}}],
                         "pageInfo": {"hasNextPage": False}}}


class EvidenceTests(unittest.TestCase):
    def setUp(self):
        self.data = evidence()

    def reject(self, message):
        with self.assertRaisesRegex(adapter.Ineligible, message):
            adapter.check_evidence(**self.data)

    def replace_current_body(self, body):
        self.data["comments"][0]["body"] = body
        self.data["summary"]["body"] = body
        self.data["summary"]["userContentEdits"]["nodes"][0]["diff"] = body

    def test_native_automatic_review_yields_commit_and_episode_receipt(self):
        receipt = adapter.check_evidence(**self.data)
        self.assertEqual(receipt["head"], HEAD)
        self.assertEqual(receipt["reaction"], 200)
        self.assertEqual(receipt["policy"], POLICY.digest)

    def test_native_new_commits_review(self):
        self.assertEqual(adapter.check_evidence(**evidence("New commits"))["head"], HEAD)

    def test_author_can_request_another_normal_review(self):
        self.data = evidence("Manual request")
        self.data["comments"].extend([command(created="2026-09-01T09:30:00Z"), command()])
        self.data["reviews"] = [native_review("2026-09-01T09:31:30Z", "c" * 40)]
        self.data["threads"] = [thread(resolved=True)]
        self.assertEqual(adapter.check_evidence(**self.data)["head"], HEAD)

    def test_copying_bot_text_does_not_make_a_native_review(self):
        self.data["comments"][0]["user"] = {"id": 900, "type": "User", "login": ACTOR["login"]}
        self.reject("exactly one")

    def test_wrong_app_cannot_supply_summary(self):
        self.data["comments"][0]["performed_via_github_app"]["id"] = 1
        self.reject("exactly one")

    def test_human_editor_cannot_replace_summary(self):
        self.data["summary"]["editor"] = {"login": "human"}
        self.reject("last editor")

    def test_historical_human_edit_is_not_hidden_by_new_bot_edit(self):
        self.data["summary"]["userContentEdits"]["nodes"][1]["editor"] = {"login": "human"}
        self.reject("non-Codex editor")

    def test_reaction_update_timestamp_is_not_a_content_edit(self):
        self.data["comments"][0]["updated_at"] = "2026-09-07T20:00:00Z"
        self.assertEqual(adapter.check_evidence(**self.data)["head"], HEAD)

    def test_missing_or_truncated_history(self):
        for change in ("missing", "truncated"):
            with self.subTest(change=change):
                self.data = evidence()
                history = self.data["summary"]["userContentEdits"]
                if change == "missing":
                    history["nodes"] = history["nodes"][:1]
                else:
                    history["pageInfo"]["hasNextPage"] = True
                self.reject("history")

    def test_completed_without_immediately_preceding_running(self):
        self.data["summary"]["userContentEdits"]["nodes"][1]["diff"] = summary_body()
        self.reject("preceding Running")

    def test_changed_summary_between_rest_and_graphql_reads(self):
        self.data["summary"]["body"] = summary_body(False)
        self.reject("changed while")

    def test_running_failed_and_unknown_formats_withhold_approval(self):
        for body in (summary_body(False), summary_body().replace("Completed", "Failed"),
                     summary_body() + "| 🔒 **Security Review** | Running | `aaaaaaa` | Manual request |\n"):
            with self.subTest(body=body):
                self.data = evidence()
                self.replace_current_body(body)
                self.reject("Running|unsupported|Unknown")

    def test_mixed_commit_episode_is_rejected(self):
        self.data["summary"]["userContentEdits"]["nodes"][1]["diff"] = summary_body(False, sha="ccccccc")
        self.reject("changed commit")

    def test_old_head_or_abbreviated_sha_collision_is_rejected(self):
        for resolved in ("c" * 40, HEAD[:7] + "c" * 33, HEAD[:7]):
            with self.subTest(resolved=resolved):
                self.data["resolved_sha"] = resolved
                self.reject("ambiguous commit")

    def test_new_same_head_review_request_invalidates_clean_receipt(self):
        self.data["comments"].append(command(created="2026-09-01T10:03:00Z"))
        self.reject("newer or edited")

    def test_edited_command_invalidates_prior_receipt(self):
        request = command()
        request["updated_at"] = "2026-09-01T10:03:00Z"
        self.data["comments"].append(request)
        self.reject("newer or edited")

    def test_scoped_manual_review_cannot_approve_whole_pr(self):
        for body in ("@codex review only the README", "@codex security review"):
            with self.subTest(body=body):
                self.data = evidence("Manual request")
                self.data["comments"].append(command(body))
                self.reject("Scoped review")

    def test_missing_manual_request(self):
        self.data = evidence("Manual request")
        self.reject("request is missing")

    def test_unknown_auto_trigger(self):
        self.data = evidence("Future scoped review")
        self.reject("Unrecognized automatic")

    def test_old_missing_wrong_author_or_late_thumbs_up_is_not_clean(self):
        for change in ("old", "missing", "human", "late"):
            with self.subTest(change=change):
                self.data = evidence()
                reaction = self.data["reactions"][0]
                if change == "old":
                    reaction["created_at"] = "2026-09-01T09:59:59Z"
                elif change == "missing":
                    self.data["reactions"] = []
                elif change == "human":
                    reaction["user"]["id"] = 900
                else:
                    reaction["created_at"] = "2026-09-01T10:10:00Z"
                self.reject("fresh Codex thumbs-up")

    def test_running_reaction_with_lingering_thumbs_up(self):
        self.data["reactions"].append({"content": "eyes", "user": NATIVE})
        self.reject("running-review reaction")

    def test_resolving_current_findings_does_not_turn_review_clean(self):
        self.data["reviews"] = [native_review()]
        self.data["threads"] = [thread(resolved=True)]
        self.reject("posted findings")

    def test_outdated_unresolved_native_findings_still_block(self):
        self.data["threads"] = [thread()]
        self.reject("Unresolved Codex")

    def test_old_resolved_findings_allow_fresh_clean_review(self):
        self.data["reviews"] = [native_review("2026-09-01T09:00:00Z", "c" * 40)]
        self.data["threads"] = [thread(resolved=True), thread(native=False)]
        self.assertEqual(adapter.check_evidence(**self.data)["head"], HEAD)

    def test_incomplete_threads_fail_closed(self):
        self.data["threads"] = [thread(resolved=True)]
        self.data["threads"][0]["comments"]["pageInfo"]["hasNextPage"] = True
        self.reject("Incomplete review thread")


class PathTests(unittest.TestCase):
    def test_rust_and_cargo_changes_require_own_fragment(self):
        for path in ("deploy/zakura-watchdog/src/main.rs", "deploy/zakura-watchdog/Cargo.toml"):
            for status in ("modified", "removed"):
                with self.subTest(path=path, status=status):
                    files = [{"filename": path, "status": status}]
                    with self.assertRaisesRegex(adapter.Ineligible, "require this PR's changelog"):
                        POLICY.check_files(files, 1, 123)
                    files.append({"filename": "docs/changelog/unreleased/123.md", "status": "added"})
                    self.assertEqual(POLICY.check_files(files, 2, 123), files[1]["filename"])

    def test_eligible_watchdog_change_can_add_its_own_fragment(self):
        files = [{"filename": "deploy/zakura-watchdog/src/main.rs", "status": "modified"},
                 {"filename": "docs/changelog/unreleased/123.md", "status": "added"}]
        self.assertEqual(POLICY.check_files(files, 2, 123), "docs/changelog/unreleased/123.md")

    def test_fragment_does_not_make_application_or_release_changes_eligible(self):
        for path in ("crates/zakura-chain/src/lib.rs", ".github/workflows/create-release.yml"):
            with self.subTest(path=path):
                with self.assertRaises(adapter.Ineligible):
                    POLICY.check_files([{"filename": path, "status": "modified"},
                                        {"filename": "docs/changelog/unreleased/123.md", "status": "added"}], 2, 123)

    def test_only_own_new_fragment_is_exempt(self):
        for path, status in (("docs/changelog/unreleased/124.md", "added"),
                             ("docs/changelog/unreleased/123.md", "modified"),
                             ("docs/changelog/unreleased/123.md", "removed"),
                             ("docs/changelog/unreleased/123.md", "renamed"),
                             ("docs/changelog/unreleased/123.md", "copied"),
                             ("docs/changelog/unreleased/123-extra.md", "added"),
                             ("docs/changelog/unreleased/README.md", "modified"),
                             ("CHANGELOG.md", "modified")):
            with self.subTest(path=path, status=status):
                with self.assertRaises(adapter.Ineligible):
                    POLICY.check_files([{"filename": "deploy/a.py", "status": "modified"},
                                        {"filename": path, "status": status}], 2, 123)

    def test_fragment_alone_or_without_pr_identity_does_not_qualify(self):
        fragment = {"filename": "docs/changelog/unreleased/123.md", "status": "added"}
        with self.assertRaises(adapter.Ineligible):
            POLICY.check_files([fragment], 1, 123)
        with self.assertRaises(adapter.Ineligible):
            POLICY.check_files([{"filename": "deploy/a.py", "status": "modified"}, fragment], 2)

    def test_existing_files_in_all_three_roots(self):
        files = [{"filename": p, "status": "modified"} for p in (
            "deploy/deployer/deploy.py", ".github/workflows/lint.yml",
            ".github/scripts/upstream-sync-run.sh")]
        POLICY.check_files(files, 3)

    def test_release_and_adapter_controls_always_need_humans(self):
        paths = [p + "new-file.sh" if p.endswith("/") else p for p in POLICY.data["human_only"]]
        paths += ["scripts/sign-release.sh", ".github/review-policy/policy.json", ".github/CODEOWNERS",
                  ".github/actions/setup-zakura-build/action.yml", "crates/zakura-chain/src/lib.rs"]
        for path in paths:
            with self.subTest(path=path):
                with self.assertRaises(adapter.Ineligible):
                    POLICY.check_files([{"filename": path, "status": "modified"}], 1)

    def test_mixed_pr_needs_human(self):
        with self.assertRaises(adapter.Ineligible):
            POLICY.check_files([{"filename": "deploy/a.py", "status": "modified"},
                                {"filename": "Cargo.toml", "status": "modified"}], 2)

    def test_source_to_eligible_rename_cannot_hide_source_change(self):
        with self.assertRaisesRegex(adapter.Ineligible, "human review"):
            POLICY.check_files([{"filename": "deploy/notes.md", "previous_filename": "crates/lib.rs",
                                 "status": "renamed"}], 1)

    def test_release_rename_cannot_hide_release_change(self):
        with self.assertRaisesRegex(adapter.Ineligible, "human review"):
            POLICY.check_files([{"filename": ".github/workflows/ordinary.yml", "status": "renamed",
                                 "previous_filename": ".github/workflows/create-release.yml"}], 1)

    def test_new_and_renamed_files_need_classification(self):
        for status in ("added", "copied", "changed", "renamed"):
            with self.subTest(status=status):
                with self.assertRaises(adapter.Ineligible):
                    POLICY.check_files([{"filename": "deploy/new.py", "status": status,
                                         "previous_filename": "deploy/old.py"}], 1)

    def test_path_representation_and_prefix_confusion(self):
        for path in ("deploy-other/a", "deploy/../Cargo.toml", "deploy//a", "/deploy/a", "deploy/a\n",
                     "deploy/./a", "deploy/dir\\a", ".github/workflows-evil/a"):
            with self.subTest(path=path):
                self.assertFalse(POLICY.eligible_path(path))

    def test_empty_duplicate_and_truncated_file_lists(self):
        file = {"filename": "deploy/a", "status": "modified"}
        for files, count in (([], 0), ([file], 2), ([file, file], 2), ([file], 3000)):
            with self.subTest(count=count):
                with self.assertRaises(adapter.Ineligible):
                    POLICY.check_files(files, count)


class FragmentTests(unittest.TestCase):
    def setUp(self):
        self.api = Mock()
        self.worker = adapter.Adapter(self.api, POLICY, 123)
        self.content = "<!-- changelog: none -->\n\nInternal watchdog tests only.\n"
        self.responses = []
        for index, part in enumerate(("docs", "changelog", "unreleased", "123.md")):
            self.responses.append({"truncated": False, "tree": [{
                "path": part, "sha": str(index + 1) * 40,
                "type": "blob" if index == 3 else "tree",
                "mode": "100644" if index == 3 else "040000",
            }]})
        self.responses.append({"sha": "4" * 40, "encoding": "base64", "size": len(self.content),
                               "content": base64.b64encode(self.content.encode()).decode() + "\n"})

    def check(self):
        self.api.request.side_effect = self.responses
        self.worker.check_fragment("docs/changelog/unreleased/123.md", HEAD)

    def test_reads_regular_fragment_from_exact_head_tree(self):
        self.check()
        self.assertTrue(self.api.request.call_args_list[0].args[0].endswith("/git/trees/" + HEAD))
        self.assertTrue(self.api.request.call_args_list[-1].args[0].endswith("/git/blobs/" + "4" * 40))

    def test_release_waiver_requires_human_review(self):
        text = "<!-- release-readiness: allow-patch; reason: Compatible changes. -->\n"
        self.responses[-1].update(size=len(text), content=base64.b64encode(text.encode()).decode())
        with self.assertRaisesRegex(adapter.Ineligible, "release-policy"):
            self.check()

    def test_symlink_executable_and_submodule_cannot_be_fragments(self):
        for mode in ("120000", "100755", "160000"):
            with self.subTest(mode=mode):
                self.responses[3]["tree"][0]["mode"] = mode
                with self.assertRaisesRegex(adapter.Ineligible, "regular non-executable"):
                    self.check()

    def test_parent_symlink_and_truncated_tree_fail_closed(self):
        for change in ("symlink", "truncated"):
            with self.subTest(change=change):
                self.setUp()
                if change == "symlink":
                    self.responses[0]["tree"][0].update(type="blob", mode="120000")
                else:
                    self.responses[0]["truncated"] = True
                with self.assertRaises(adapter.Ineligible):
                    self.check()

    def test_malformed_oversized_and_non_utf8_blob_fail_closed(self):
        for change in ("sha", "size", "base64", "utf8"):
            with self.subTest(change=change):
                self.setUp()
                blob = self.responses[-1]
                if change == "sha":
                    blob["sha"] = "5" * 40
                elif change == "size":
                    blob["size"] = 65537
                elif change == "base64":
                    blob["content"] = "not base64!"
                else:
                    blob.update(size=1, content=base64.b64encode(b"\xff").decode())
                with self.assertRaises(adapter.Ineligible):
                    self.check()

    def test_evaluation_checks_fragment_before_native_approval_evidence(self):
        pull = {"state": "open", "draft": False, "head": {"sha": HEAD}, "changed_files": 2,
                "base": {"ref": "main", "sha": BASE, "repo": {"full_name": POLICY.data["repository"]}}}
        self.api.request.return_value = pull
        self.api.pages.return_value = [
            {"filename": "deploy/a.py", "status": "modified"},
            {"filename": "docs/changelog/unreleased/123.md", "status": "added"},
        ]
        self.worker.check_fragment = Mock(side_effect=adapter.Ineligible("Release waiver"))
        self.worker.check_author = Mock()
        self.worker.evidence = Mock()
        with self.assertRaisesRegex(adapter.Ineligible, "Release waiver"):
            self.worker.evaluate(enforce_rules=False)
        self.worker.check_fragment.assert_called_once_with("docs/changelog/unreleased/123.md", HEAD)
        self.worker.evidence.assert_not_called()


def rules_fixture():
    return [{"type": "pull_request", "ruleset_id": 1, "ruleset_source_type": "Repository",
             "parameters": {"required_approving_review_count": 1,
                            "dismiss_stale_reviews_on_push": False, "require_last_push_approval": False}},
            {"type": "required_status_checks",
             "parameters": {"required_status_checks": [{"context": "test success"}]}}]


class AuthorTests(unittest.TestCase):
    def setUp(self):
        self.author = {"id": 900, "login": "contributor", "type": "User"}
        self.pull = {"state": "open", "draft": False, "head": {"sha": HEAD},
                     "changed_files": 1, "user": self.author,
                     "base": {"ref": "main", "sha": BASE,
                              "repo": {"full_name": POLICY.data["repository"]}}}
        self.access = {"permission": "write", "role_name": "write", "user": self.author.copy()}
        self.api, self.writer = Mock(), Mock()
        self.worker = adapter.Adapter(self.api, POLICY, 123, writer=self.writer,
                                      app_id=APP_ID, bot_id=BOT_ID, trusted_sha=BASE)
        self.api.request.side_effect = self.request
        self.reviews = []
        self.api.pages.side_effect = lambda path: (
            [{"filename": "deploy/a.py", "status": "modified"}] if path.endswith("/files")
            else self.reviews)
        self.worker.evidence = Mock(return_value=receipt())
        self.writer.request.return_value = {"id": 500, "user": {"id": BOT_ID, "type": "Bot"}}

    def request(self, path):
        if path == self.worker.pull_path:
            return self.pull
        if path.endswith("/collaborators/contributor/permission"):
            return self.access
        if path.endswith("/rules/branches/main"):
            return rules_fixture()
        if path.endswith("/rulesets/1"):
            return {"enforcement": "active", "bypass_actors": []}
        if path.endswith("/commits/main"):
            return {"sha": BASE}
        raise AssertionError(path)

    def test_write_maintain_admin_and_custom_write_roles_qualify(self):
        for permission, role in (("write", "write"), ("write", "maintain"),
                                 ("admin", "admin"), ("write", "custom-developer")):
            with self.subTest(role=role):
                self.access.update(permission=permission, role_name=role)
                self.assertTrue(self.worker.author_gate()["reconcile"])
                self.assertEqual(self.worker.evaluate()["head"], HEAD)

    def test_outsider_read_triage_and_unknown_roles_cannot_approve(self):
        for permission in ("read", "none", "triage", "maintain", "unknown", None):
            with self.subTest(permission=permission):
                self.access["permission"] = permission
                # Neither organization association nor an admin event sender is authorization.
                self.pull.update(author_association="MEMBER", sender={"login": "admin"})
                self.assertFalse(self.worker.author_gate()["reconcile"])
                self.assertFalse(self.worker.reconcile()["approved"])
                self.worker.evidence.assert_not_called()
                self.writer.request.assert_not_called()

    def test_permission_response_must_match_author_immutable_id(self):
        self.access["user"]["id"] = 901
        with self.assertRaisesRegex(adapter.Ineligible, "does not match"):
            self.worker.evaluate()

    def test_author_gate_preserves_automatic_and_ordinary_manual_reviews(self):
        for trigger in ("New commits", "Manual request"):
            with self.subTest(trigger=trigger):
                self.setUp()
                data = evidence(trigger)
                if trigger == "Manual request":
                    data["comments"].append(command())
                self.worker.evidence.side_effect = lambda _: adapter.check_evidence(**data)
                self.assertTrue(self.worker.reconcile()["approved"])
                self.writer.reset_mock()
                self.access["permission"] = "read"
                self.assertFalse(self.worker.reconcile()["approved"])
                self.writer.request.assert_not_called()

    def test_bots_and_deleted_authors_cannot_qualify(self):
        for author in (None, {}, {**self.author, "type": "Bot"}):
            self.pull["user"] = author
            self.assertFalse(self.worker.author_gate()["reconcile"])

    def test_unavailable_permissions_never_qualify(self):
        self.api.request.side_effect = adapter.APIError("GitHub GET failed with HTTP 404")
        self.assertFalse(self.worker.author_gate()["reconcile"])
        self.assertFalse(self.worker.reconcile()["approved"])
        self.writer.request.assert_not_called()

    def test_revoked_author_keeps_cleanup_job_and_withdraws_approval(self):
        self.reviews = [owned_review()]
        self.access["permission"] = "read"
        self.assertTrue(self.worker.author_gate()["reconcile"])
        self.assertEqual(self.worker.reconcile()["dismissed"], 1)
        self.assertEqual([c.args[1] for c in self.writer.request.call_args_list], ["PUT"])

    def test_retargeted_pr_keeps_cleanup_job_but_cannot_receive_approval(self):
        self.pull["base"]["ref"] = "release/v1"
        self.reviews = [owned_review()]
        self.assertTrue(self.worker.author_gate()["reconcile"])
        self.assertEqual(self.worker.reconcile()["dismissed"], 1)
        self.assertEqual([c.args[1] for c in self.writer.request.call_args_list], ["PUT"])
        self.reviews = []
        self.writer.reset_mock()
        self.assertFalse(self.worker.reconcile()["approved"])
        self.writer.request.assert_not_called()

    def test_unavailable_permissions_keep_cleanup_of_existing_approval(self):
        self.reviews = [owned_review()]
        self.api.request.side_effect = adapter.APIError("Unavailable")
        self.assertTrue(self.worker.author_gate()["reconcile"])
        self.assertEqual(self.worker.reconcile()["dismissed"], 1)

    def test_preflight_does_not_authorize_a_later_approval(self):
        self.assertTrue(self.worker.author_gate()["reconcile"])
        self.access["permission"] = "none"
        self.assertFalse(self.worker.reconcile()["approved"])
        self.writer.request.assert_not_called()

    def test_revocation_during_evaluation_prevents_approval(self):
        def evidence(_):
            self.access["permission"] = "none"
            return receipt()
        self.worker.evidence.side_effect = evidence
        self.assertFalse(self.worker.reconcile()["approved"])
        self.writer.request.assert_not_called()

    def test_revocation_during_post_withdraws_new_approval(self):
        def post(*_):
            self.access["permission"] = "none"
            return {"id": 500, "user": {"id": BOT_ID, "type": "Bot"}}
        self.writer.request.side_effect = post
        with self.assertRaises(adapter.Ineligible):
            self.worker.reconcile()
        self.assertEqual([c.args[1] for c in self.writer.request.call_args_list], ["POST", "PUT"])


class RequesterTests(unittest.TestCase):
    def setUp(self):
        self.case = AuthorTests()
        self.case.setUp()
        self.worker = self.case.worker
        self.data = evidence()
        self.outsider = {"id": 901, "login": "outsider", "type": "User"}
        self.permissions = {"permission": "read", "role_name": "triage", "user": self.outsider}
        self.case.api.request.side_effect = self.request
        self.case.api.pages.side_effect = self.pages
        self.case.api.graphql.return_value = {"node": self.data["summary"]}
        self.worker.threads = Mock(return_value=[])
        self.worker.evidence = adapter.Adapter.evidence.__get__(self.worker)

    def request(self, path):
        if path.endswith("/collaborators/outsider/permission"):
            if isinstance(self.permissions, Exception):
                raise self.permissions
            return self.permissions
        if path.endswith("/commits/" + HEAD[:7]):
            return {"sha": HEAD}
        return self.case.request(path)

    def pages(self, path):
        if path.endswith("/comments"):
            return self.data["comments"]
        if path.endswith("/reactions"):
            return self.data["reactions"]
        if path.endswith("/reviews"):
            return self.case.reviews
        if path.endswith("/files"):
            return [{"filename": "deploy/a.py", "status": "modified"}]
        raise AssertionError(path)

    def outsider_command(self):
        comment = command(created="2026-09-01T10:03:00Z")
        comment.update(user=self.outsider, author_association="MEMBER")
        self.data["comments"].append(comment)
        return comment

    def test_outsider_new_or_edited_command_cannot_withdraw_approval(self):
        self.case.reviews = [owned_review()]
        comment = self.outsider_command()
        for created in ("2026-09-01T10:03:00Z", "2026-09-01T09:59:00Z"):
            with self.subTest(created=created):
                comment["created_at"] = created
                self.assertTrue(self.worker.reconcile()["approved"])
                self.case.writer.request.assert_not_called()

    def test_missing_collaborator_is_ignored(self):
        self.permissions = adapter.APIError("Not found", status=404)
        self.case.reviews = [owned_review()]
        self.outsider_command()
        self.assertTrue(self.worker.reconcile()["approved"])
        self.case.writer.request.assert_not_called()

    def test_outsider_cannot_supply_manual_request_evidence(self):
        self.data = evidence("Manual request")
        self.case.api.graphql.return_value = {"node": self.data["summary"]}
        comment = self.outsider_command()
        comment.update(created_at="2026-09-01T09:59:58Z", updated_at="2026-09-01T09:59:58Z")
        self.assertIn("Manual review request is missing", self.worker.reconcile()["reason"])
        self.case.writer.request.assert_not_called()

    def test_authorized_request_still_withdraws_old_approval(self):
        self.case.reviews = [owned_review()]
        self.data["comments"].append(command(created="2026-09-01T10:03:00Z"))
        self.assertEqual(self.worker.reconcile()["dismissed"], 1)
        self.assertEqual(self.case.writer.request.call_args.args[1], "PUT")

    def test_requester_permissions_are_cached_only_within_one_evaluation(self):
        self.outsider_command()
        self.outsider_command()
        self.worker.evidence(self.case.pull)
        lookups = [c for c in self.case.api.request.call_args_list
                   if c.args[0].endswith("/collaborators/outsider/permission")]
        self.assertEqual(len(lookups), 1)
        self.permissions["permission"] = "write"
        with self.assertRaisesRegex(adapter.Ineligible, "newer or edited"):
            self.worker.evidence(self.case.pull)

    def test_permission_service_failure_is_not_treated_as_denied_access(self):
        self.outsider_command()
        self.permissions = adapter.APIError("Unavailable", status=503)
        with self.assertRaises(adapter.APIError):
            self.worker.evidence(self.case.pull)


class RulesTests(unittest.TestCase):
    def setUp(self):
        self.rules = rules_fixture()
        self.full = {"enforcement": "active", "bypass_actors": []}
        self.api = Mock()
        self.api.request.side_effect = lambda path: self.full if "/rulesets/" in path else self.rules

    def test_normal_approval_rules_work_without_a_reviewer_team(self):
        adapter.check_rules(self.api, POLICY)

    def test_stale_review_settings_are_not_required_fields(self):
        del self.rules[0]["parameters"]["dismiss_stale_reviews_on_push"]
        del self.rules[0]["parameters"]["require_last_push_approval"]
        adapter.check_rules(self.api, POLICY)

    def test_existing_stale_review_preferences_are_accepted_and_preserved(self):
        for dismiss in (False, True):
            for last_push in (False, True):
                with self.subTest(dismiss=dismiss, last_push=last_push):
                    self.rules = rules_fixture()
                    self.rules[0]["parameters"]["dismiss_stale_reviews_on_push"] = dismiss
                    self.rules[0]["parameters"]["require_last_push_approval"] = last_push
                    before = deepcopy(self.rules)
                    adapter.check_rules(self.api, POLICY)
                    self.assertEqual(self.rules, before)

    def test_existing_reviewer_rules_are_preserved(self):
        self.rules[0]["parameters"]["required_reviewers"] = [
            {"file_patterns": ["crates/**"], "minimum_approvals": 1,
             "reviewer": {"id": 42, "type": "Team"}}]
        self.rules[0]["parameters"]["require_code_owner_review"] = True
        before = deepcopy(self.rules)
        adapter.check_rules(self.api, POLICY)
        self.assertEqual(self.rules, before)

    def test_missing_approval_test_gate_or_bypass_stops_approval(self):
        for change in ("approval", "ci", "bypass", "evaluate"):
            with self.subTest(change=change):
                self.setUp()
                if change == "approval":
                    self.rules[0]["parameters"]["required_approving_review_count"] = 0
                if change == "ci":
                    self.rules.pop()
                if change == "bypass":
                    self.full["bypass_actors"] = [{"actor_type": "Integration", "actor_id": APP_ID}]
                if change == "evaluate":
                    self.full["enforcement"] = "evaluate"
                with self.assertRaises(adapter.Ineligible):
                    adapter.check_rules(self.api, POLICY)


def receipt():
    return {**adapter.check_evidence(**evidence()), "base": BASE}


def owned_review(state="APPROVED", expected=None, identity=BOT_ID):
    expected = expected or receipt()
    return {"id": 400, "state": state, "user": {"id": identity, "type": "Bot"},
            "commit_id": expected["head"],
            "body": adapter.RECEIPT_MARKER + json.dumps(expected, sort_keys=True, separators=(",", ":")) + " -->\n"}


class ReconcileTests(unittest.TestCase):
    def setUp(self):
        self.api, self.writer = Mock(), Mock()
        self.api.pages.return_value = []
        self.worker = adapter.Adapter(self.api, POLICY, 1, writer=self.writer,
                                      app_id=APP_ID, bot_id=BOT_ID, trusted_sha=BASE)
        self.worker.check_trusted_revision = Mock()
        self.worker.evaluate = Mock(return_value=receipt())
        self.writer.request.return_value = {"id": 500, "user": {"id": BOT_ID, "type": "Bot"}}

    def writes(self):
        return [(c.args[1], c.args[0]) for c in self.writer.request.call_args_list]

    def test_approve_exact_commit_once(self):
        result = self.worker.reconcile()
        self.assertTrue(result["approved"])
        self.assertEqual(self.writer.request.call_args.args[2]["commit_id"], HEAD)
        self.assertEqual(self.writes(), [("POST", self.worker.pull_path + "/reviews")])
        self.assertEqual(self.worker.evaluate.call_count, 3)

    def test_existing_current_approval_is_idempotent(self):
        self.api.pages.return_value = [owned_review()]
        self.assertTrue(self.worker.reconcile()["approved"])
        self.writer.request.assert_not_called()

    def test_stale_approval_is_dismissed_without_touching_human_or_other_bot(self):
        self.api.pages.return_value = [owned_review(), owned_review(identity=900),
                                      {"id": 600, "state": "APPROVED", "body": "Looks good",
                                       "user": {"id": 901, "type": "User"}, "commit_id": HEAD}]
        self.worker.evaluate.side_effect = adapter.Ineligible("New head is not reviewed")
        result = self.worker.reconcile()
        self.assertFalse(result["approved"])
        self.assertEqual(self.writes(), [("PUT", self.worker.pull_path + "/reviews/400/dismissals")])

    def test_missing_rules_or_api_failure_withdraws_existing_approval(self):
        for error in (adapter.Ineligible("Rules missing"), adapter.APIError("Unavailable")):
            with self.subTest(error=error):
                self.setUp()
                self.api.pages.return_value = [owned_review()]
                self.worker.evaluate.side_effect = error
                self.assertEqual(self.worker.reconcile()["dismissed"], 1)

    def test_changed_receipt_is_replaced_after_dismissal(self):
        stale = {**receipt(), "head": "c" * 40}
        self.api.pages.return_value = [owned_review(expected=stale)]
        self.assertTrue(self.worker.reconcile()["approved"])
        self.assertEqual([method for method, _ in self.writes()], ["PUT", "POST"])

    def test_do_not_reapprove_an_explicitly_dismissed_episode(self):
        self.api.pages.return_value = [owned_review("DISMISSED")]
        self.assertFalse(self.worker.reconcile()["approved"])
        self.writer.request.assert_not_called()

    def dismissal_event(self, actor=BOT_ID):
        return {"event": "review_dismissed", "actor": {"id": actor, "type": "Bot"},
                "dismissed_review": {"review_id": 400, "state": "approved",
                                     "dismissal_message": adapter.WITHDRAWAL_MESSAGE}}

    def set_dismissal_history(self, events):
        self.api.pages.side_effect = lambda path: (
            events if path.endswith("/timeline") else [owned_review("DISMISSED")])

    def test_restore_own_withdrawal_after_api_recovers(self):
        self.api.pages.return_value = [owned_review()]
        self.worker.evaluate.side_effect = adapter.APIError("Temporary outage")
        self.assertEqual(self.worker.reconcile()["dismissed"], 1)
        self.worker.evaluate.side_effect = None
        self.writer.reset_mock()
        self.set_dismissal_history([self.dismissal_event()])
        self.assertTrue(self.worker.reconcile()["approved"])
        self.assertEqual(self.writes(), [("POST", self.worker.pull_path + "/reviews")])

    def test_dismissal_recovery_requires_unambiguous_adapter_actor_and_message(self):
        own = self.dismissal_event()
        wrong_message = deepcopy(own)
        wrong_message["dismissed_review"]["dismissal_message"] = "Operator dismissal"
        wrong_review = deepcopy(own)
        wrong_review["dismissed_review"]["review_id"] = 999
        missing_actor = {**own, "actor": None}
        for events in ([], [self.dismissal_event(900)], [wrong_message], [wrong_review], [missing_actor],
                       [own, own], [own, self.dismissal_event(900)]):
            with self.subTest(events=events):
                self.set_dismissal_history(events)
                self.assertFalse(self.worker.reconcile()["approved"])
                self.writer.request.assert_not_called()

    def test_restore_own_withdrawal_after_trusted_checkout_refresh(self):
        self.api.pages.return_value = [owned_review()]
        self.worker.check_trusted_revision.side_effect = adapter.Ineligible("Trusted branch advanced")
        self.assertEqual(self.worker.reconcile()["dismissed"], 1)
        self.worker.check_trusted_revision.side_effect = None
        self.set_dismissal_history([self.dismissal_event()])
        self.assertTrue(self.worker.reconcile()["approved"])

    def test_human_dismissal_blocks_even_with_an_older_automatic_withdrawal(self):
        human = {**owned_review("DISMISSED"), "id": 401}
        self.api.pages.side_effect = lambda path: (
            [self.dismissal_event()] if path.endswith("/timeline")
            else [owned_review("DISMISSED"), human])
        self.assertFalse(self.worker.reconcile()["approved"])
        self.writer.request.assert_not_called()

    def test_unavailable_dismissal_history_cannot_restore_approval(self):
        self.api.pages.side_effect = [[owned_review("DISMISSED")], adapter.APIError("Unavailable")]
        self.assertFalse(self.worker.reconcile()["approved"])
        self.writer.request.assert_not_called()

    def test_base_or_policy_update_does_not_override_episode_dismissal(self):
        for key in ("base", "policy"):
            with self.subTest(key=key):
                self.setUp()
                self.api.pages.return_value = [owned_review("DISMISSED", {**receipt(), key: "c" * 40})]
                self.assertFalse(self.worker.reconcile()["approved"])
                self.writer.request.assert_not_called()

    def test_push_or_new_review_before_post_never_approves(self):
        self.worker.evaluate.side_effect = [receipt(), adapter.Ineligible("New review is running")]
        self.assertFalse(self.worker.reconcile()["approved"])
        self.writer.request.assert_not_called()

    def test_push_during_post_dismisses_just_created_approval(self):
        self.worker.evaluate.side_effect = [receipt(), receipt(), {**receipt(), "head": "c" * 40}]
        with self.assertRaises(adapter.Ineligible):
            self.worker.reconcile()
        self.assertEqual([method for method, _ in self.writes()], ["POST", "PUT"])
        self.assertTrue(self.writes()[-1][1].endswith("/500/dismissals"))

    def test_ambiguous_post_is_reconciled_without_post_retry(self):
        self.writer.request.side_effect = [adapter.APIError("Timed out"), None]
        self.api.pages.side_effect = [[], [owned_review()]]
        with self.assertRaises(adapter.APIError):
            self.worker.reconcile()
        self.assertEqual([method for method, _ in self.writes()], ["POST", "PUT"])

    def test_different_bot_token_is_detected_and_its_review_removed(self):
        self.writer.request.return_value = {"id": 500, "user": {"id": 999, "type": "Bot"}}
        with self.assertRaisesRegex(adapter.Ineligible, "configured App"):
            self.worker.reconcile()
        self.assertEqual([method for method, _ in self.writes()], ["POST", "PUT"])

    def test_trusted_main_advancing_before_post_withholds_approval(self):
        self.worker.check_trusted_revision.side_effect = [None, adapter.Ineligible("Main advanced")]
        self.assertFalse(self.worker.reconcile()["approved"])
        self.writer.request.assert_not_called()

    def test_closed_draft_or_retargeted_pr_is_ineligible(self):
        original = {"state": "open", "draft": False, "head": {"sha": HEAD},
                    "base": {"ref": "main", "repo": {"full_name": POLICY.data["repository"]}}}
        for change in ("closed", "draft", "base"):
            with self.subTest(change=change):
                pull = deepcopy(original)
                if change == "closed":
                    pull["state"] = "closed"
                elif change == "draft":
                    pull["draft"] = True
                else:
                    pull["base"]["ref"] = "release/v1"
                self.api.request.return_value = pull
                with self.assertRaises(adapter.Ineligible):
                    adapter.Adapter(self.api, POLICY, 1).evaluate()


class APITests(unittest.TestCase):
    def test_http_status_survives_as_api_error(self):
        with adapter.urllib.error.HTTPError(
                "https://api.github.com/test", 404, "Not found", {}, None) as error, \
                patch("urllib.request.urlopen", side_effect=error):
            with self.assertRaises(adapter.APIError) as result:
                adapter.GitHub("unused").request("/test")
            self.assertEqual(result.exception.status, 404)

    def test_partial_network_response_is_an_api_error(self):
        with patch("urllib.request.urlopen") as open_url:
            open_url.return_value.__enter__.return_value.read.side_effect = http.client.IncompleteRead(b"")
            with self.assertRaises(adapter.APIError):
                adapter.GitHub("unused").request("/test", "POST", {})

    def test_rest_pagination_is_complete(self):
        api = adapter.GitHub("unused")
        api.request = Mock(side_effect=[[{}] * 100, [{"id": 1}]])
        self.assertEqual(len(api.pages("/test")), 101)
        self.assertIn("page=2", api.request.call_args.args[0])

    def test_excessive_pagination_fails_closed(self):
        api = adapter.GitHub("unused")
        api.request = Mock(return_value=[{}] * 100)
        with self.assertRaises(adapter.Ineligible):
            api.pages("/test")
        self.assertEqual(api.request.call_count, adapter.MAX_PAGES)

    def test_graphql_partial_data_is_an_error(self):
        api = adapter.GitHub("unused")
        api.request = Mock(return_value={"data": {"node": {}}, "errors": [{"message": "unavailable"}]})
        with self.assertRaises(adapter.APIError):
            api.graphql("query { node }")

    def test_event_targets_use_metadata_not_comment_text(self):
        api = Mock()
        numbers = adapter.target_numbers(api, POLICY, {"issue": {"number": 17, "pull_request": {"url": "unused"}},
                                                       "comment": {"body": "@codex review PR 1234"}})
        self.assertEqual(numbers, [17])
        api.pages.assert_not_called()

    def test_retargeted_pr_is_checked_by_event_and_periodic_cleanup(self):
        api = Mock()
        moved = {"number": 17, "base": {"ref": "release/v1"}}
        event = {"action": "edited", "pull_request": moved,
                 "changes": {"base": {"ref": {"from": "main"}}}}
        self.assertEqual(adapter.target_numbers(api, POLICY, event), [17])
        api.pages.assert_not_called()
        api.pages.return_value = [moved, {"number": 18, "base": {"ref": "main"}}]
        self.assertEqual(adapter.target_numbers(api, POLICY, {}), [17, 18])
        api.pages.assert_called_once_with(f"/repos/{POLICY.data['repository']}/pulls?state=open")


class CLITests(unittest.TestCase):
    def test_author_preflight_gates_single_and_scheduled_batches_without_writer(self):
        for decisions, expected in (([False], "false"), ([True], "true"),
                                    ([False, True], "true"), ([False, False], "false"), ([], "false")):
            with self.subTest(decisions=decisions), tempfile.TemporaryDirectory() as directory:
                output = Path(directory) / "output"
                with (patch.dict(os.environ, {"GH_TOKEN": "unused", "GITHUB_OUTPUT": str(output)}, clear=True),
                      patch("sys.argv", ["adapter.py", "--check-authors"]),
                      patch("adapter.GitHub") as github,
                      patch("adapter.target_numbers", return_value=list(range(1, len(decisions) + 1))),
                      patch("adapter.Adapter") as worker, patch("builtins.print")):
                    worker.return_value.author_gate.side_effect = [{"reconcile": d} for d in decisions]
                    self.assertEqual(adapter.main(), 0)
                    self.assertEqual(output.read_text(), f"run_reconcile={expected}\n")
                    self.assertEqual(github.call_count, 1)
                    worker.return_value.reconcile.assert_not_called()
                    worker.return_value.evaluate.assert_not_called()

    def test_default_mode_never_constructs_writer_or_reconciles(self):
        with (patch.dict(os.environ, {"GH_TOKEN": "unused"}, clear=True),
              patch("sys.argv", ["adapter.py", "--pr", "1"]),
              patch("adapter.GitHub") as github,
              patch("adapter.Adapter") as worker,
              patch("builtins.print")):
            worker.return_value.evaluate.return_value = receipt()
            self.assertEqual(adapter.main(), 0)
            self.assertEqual(github.call_count, 1)
            worker.return_value.reconcile.assert_not_called()
            self.assertIsNone(worker.call_args.kwargs["writer"])

    def test_apply_flag_cannot_override_disabled_repository_setting(self):
        with (patch.dict(os.environ, {"GH_TOKEN": "unused", "CODEX_APPROVAL_ENABLED": "false"}, clear=True),
              patch("sys.argv", ["adapter.py", "--pr", "1", "--apply"]),
              patch("adapter.GitHub") as github):
            with self.assertRaisesRegex(adapter.Ineligible, "disabled"):
                adapter.main()
            github.return_value.request.assert_not_called()


if __name__ == "__main__":
    unittest.main()
