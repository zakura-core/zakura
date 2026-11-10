"""Contracts for the release-notes drafter: changelog parsing, support window, and rendering."""

from __future__ import annotations

import json
import os
import re
import sys
import tempfile
import threading
import unittest
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import release_notes  # noqa: E402
from release_notes import (  # noqa: E402
    Item,
    ModelSettings,
    NotesError,
    OpenAIDrafter,
    RetryableError,
    Summary,
    SupportWindow,
)

PUBLISHED_AT = datetime(2026, 11, 10, 19, 36, 4, tzinfo=timezone.utc)

CHANGELOG = """# CHANGELOG

## [Unreleased]

## [1.2.0] - 2026-11-10

### Added

- Added `network.example` ([#11](https://github.com/zakura-core/zakura/pull/11)).

### Removed

- Removed the `old` subcommand
  ([#12](https://github.com/zakura-core/zakura/pull/12)).

<!-- release-readiness: allow-patch; reason: compatible. -->

### Security

- Bounded peer state ([#13](https://github.com/zakura-core/zakura/pull/13)).

## [1.2.0-rc1] - 2026-11-08

### Fixed

- Candidate fix ([#10](https://github.com/zakura-core/zakura/pull/10)).

## [1.1.0] - 2026-11-01

### Fixed

- Older fix ([#9](https://github.com/zakura-core/zakura/pull/9)).
"""

END_OF_SUPPORT = """
/// The estimated height that this release will be published.
pub const ESTIMATED_RELEASE_HEIGHT: u32 = {release_height};
pub const EOS_PANIC_AFTER: u32 = 45;
pub const EOS_WARN_AFTER: u32 = EOS_PANIC_AFTER - 3;
{cap}
"""

ACTIVATION_HEIGHTS = """
pub mod activation_heights {
    pub mod testnet {
        pub const BLOSSOM: Height = Height(584_000);
        pub(in crate::parameters) const NU7: Height = Height(4_465_026);
    }
    pub mod mainnet {
        pub const BLOSSOM: Height = Height(653_600);
        pub(in crate::parameters) const NU7: Height = Height(3_543_405);
    }
}
"""

NETWORK_UPGRADE = """
const PRE_BLOSSOM_POW_TARGET_SPACING: i64 = 150;
pub const POST_BLOSSOM_POW_TARGET_SPACING: u32 = 75;
pub const POST_NU7_POW_TARGET_SPACING: u32 = 25;

    pub fn target_spacings(
        network: &Network,
    ) -> impl Iterator<Item = (block::Height, Duration)> + '_ {
        [
            (NetworkUpgrade::Genesis, PRE_BLOSSOM_POW_TARGET_SPACING),
            (
                NetworkUpgrade::Blossom,
                POST_BLOSSOM_POW_TARGET_SPACING.into(),
            ),
            (NetworkUpgrade::Nu7, POST_NU7_POW_TARGET_SPACING.into()),
        ]
        .into_iter()
"""

CAP = "const MAINNET_EOS_HEIGHT: Height = Height(3_643_309);"


def sources(release_height: str = "3_559_602", cap: str = CAP, upgrades: str = NETWORK_UPGRADE) -> dict:
    """Return the release files as a path-to-text map."""
    return {
        release_notes.CHANGELOG: CHANGELOG,
        release_notes.END_OF_SUPPORT: END_OF_SUPPORT.format(release_height=release_height, cap=cap),
        release_notes.ACTIVATION_HEIGHTS: ACTIVATION_HEIGHTS,
        release_notes.NETWORK_UPGRADE: upgrades,
    }


def summary_json(**overrides) -> str:
    """Return a valid model reply for the 1.2.0 section, with field overrides."""
    reply = {
        "headline": "Bounded peer state and a new `network.example` setting. We recommend upgrading.",
        "compatibility_note": "No config changes are needed.",
        "before_you_upgrade": [
            {"title": "The `old` subcommand is gone", "detail": "Use `new` instead.", "prs": [12]},
        ],
        "highlights": [
            {"text": "**Security:** peers can no longer grow header sync state without bound.", "prs": [13]},
            {"text": "New `network.example` setting.", "prs": [11]},
        ],
        "library_users": {"text": "", "prs": []},
    }
    reply.update(overrides)
    return json.dumps(reply)


def release() -> release_notes.Release:
    """Return the parsed 1.2.0 section."""
    return release_notes.parse_release("v1.2.0", CHANGELOG)


class ChangelogTests(unittest.TestCase):
    def test_extracts_only_the_tagged_section(self):
        parsed = release()
        self.assertEqual(parsed.date, "2026-11-10")
        self.assertEqual(parsed.heading, "[1.2.0] - 2026-11-10")
        self.assertTrue(parsed.body.startswith("### Added"))
        self.assertNotIn("Candidate fix", parsed.body)
        self.assertEqual(parsed.prs, {11, 12, 13})
        self.assertEqual(parsed.covered, {"Removed": (12,), "Security": (13,)})
        self.assertFalse(parsed.is_prerelease)

    def test_release_candidates_use_their_own_section(self):
        parsed = release_notes.parse_release("v1.2.0-rc1", CHANGELOG)
        self.assertTrue(parsed.is_prerelease)
        self.assertEqual(parsed.core_version, "1.2.0")
        self.assertEqual(parsed.prs, {10})

    def test_missing_sections_and_bad_tags_are_errors(self):
        with self.assertRaisesRegex(NotesError, r"no `## \[1.3.0\]"):
            release_notes.parse_release("v1.3.0", CHANGELOG)
        for tag in ("1.2.0", "v1.2", "v1.2.0 ", "main"):
            with self.assertRaises(NotesError):
                release_notes.parse_release(tag, CHANGELOG)

    def test_anchors_match_github_heading_ids(self):
        self.assertEqual(release_notes.heading_anchor("[1.8.5] - 2026-11-07"), "185---2026-11-07")
        self.assertEqual(release_notes.heading_anchor("[1.8.0-rc1] - 2026-11-01"), "180-rc1---2026-11-01")

    def test_public_changelog_drops_comments_and_their_blank_lines(self):
        public = release_notes.public_changelog(release().body)
        self.assertNotIn("<!--", public)
        self.assertIn("([#12](https://github.com/zakura-core/zakura/pull/12)).\n\n### Security", public)


class SupportWindowTests(unittest.TestCase):
    def window(self, **kwargs) -> SupportWindow:
        files = sources(**kwargs)
        return release_notes.support_window(files.get, PUBLISHED_AT)

    def test_capped_window_matches_the_v1_8_6_notes(self):
        window = self.window()
        self.assertEqual(window.height, 3_643_309)
        self.assertEqual(window.reached_at, PUBLISHED_AT + timedelta(seconds=(3_643_309 - 3_559_602) * 25))
        self.assertEqual(
            window.sentence(),
            "Supported on Mainnet until block 3,643,309, around December 5, 2026. "
            "Upgrade warnings start about three days earlier.",
        )

    def test_uncapped_window_is_45_days_after_publication(self):
        window = self.window(release_height="3_600_000", cap="")
        self.assertEqual(window.height, 3_600_000 + 45 * 3456)
        self.assertEqual(window.reached_at, PUBLISHED_AT + timedelta(days=45))

    def test_window_follows_the_nu7_spacing_change(self):
        # 999 blocks at 75 seconds reach NU7 activation; the rest of the 45 days run at 25 seconds.
        window = self.window(release_height="3_542_405", cap="")
        self.assertEqual(window.height, 3_543_404 + (45 * 86400 - 999 * 75) // 25)
        self.assertEqual(window.reached_at, PUBLISHED_AT + timedelta(days=45))

    def test_new_target_spacings_fail_instead_of_skewing_the_date(self):
        upgrades = NETWORK_UPGRADE.replace(
            "(NetworkUpgrade::Nu7, POST_NU7_POW_TARGET_SPACING.into()),",
            "(NetworkUpgrade::Nu7, POST_NU7_POW_TARGET_SPACING.into()),\n"
            "            (NetworkUpgrade::Nu8, POST_NU8_POW_TARGET_SPACING.into()),",
        )
        with self.assertRaisesRegex(NotesError, "target_spacings"):
            self.window(upgrades=upgrades)

    def test_missing_constants_are_errors(self):
        with self.assertRaisesRegex(NotesError, "ESTIMATED_RELEASE_HEIGHT"):
            self.window(release_height="estimate()")

    def test_the_current_tree_still_parses(self):
        # Fails the PR that changes these Rust sources, not the next release.
        read = release_notes.source_reader(ref=None, source_dir=release_notes.REPOSITORY_ROOT)
        window = release_notes.support_window(read, datetime.now(timezone.utc))
        self.assertGreater(window.height, 0)
        self.assertGreater(window.warning_days, 0)


class SummaryTests(unittest.TestCase):
    def parse(self, text: str):
        return release_notes.parse_summary(text, release())

    def test_renders_the_fixed_layout(self):
        summary, errors, gaps = self.parse(summary_json())
        self.assertEqual((errors, gaps), ([], []))
        window = SupportWindow(3_643_309, datetime(2026, 12, 5, 0, 54, tzinfo=timezone.utc), 3)
        pr = "https://github.com/zakura-core/zakura/pull/"
        self.assertEqual(release_notes.render(release(), window, summary), f"""\
**Bounded peer state and a new `network.example` setting. We recommend upgrading.**

Supported on Mainnet until block 3,643,309, around December 5, 2026. Upgrade warnings start about three days earlier.

### Before you upgrade

- **The `old` subcommand is gone.** Use `new` instead ([#12]({pr}12)).

### Highlights

- **Security:** peers can no longer grow header sync state without bound ([#13]({pr}13)).
- New `network.example` setting ([#11]({pr}11)).

<details>
<summary>Full changelog</summary>

### Added

- Added `network.example` ([#11]({pr}11)).

### Removed

- Removed the `old` subcommand
  ([#12]({pr}12)).

### Security

- Bounded peer state ([#13]({pr}13)).

</details>

Also in [CHANGELOG.md](https://github.com/zakura-core/zakura/blob/v1.2.0/CHANGELOG.md#120---2026-11-10).

<!-- Drafted from CHANGELOG.md by the Release notes workflow. Edit freely. -->
""")

    def test_compatibility_note_and_library_section_render_without_upgrade_notes(self):
        summary, errors, _ = self.parse(summary_json(
            before_you_upgrade=[],
            library_users={"text": "`Old` is gone.", "prs": [12]},
        ))
        self.assertEqual(errors, [])
        notes = release_notes.render(release(), SupportWindow(1, PUBLISHED_AT, 3), summary)
        self.assertIn("three days earlier.\n\nNo config changes are needed.\n\n### Highlights", notes)
        self.assertIn("### Library users\n\n`Old` is gone ([#12](", notes)
        self.assertNotIn("### Before you upgrade", notes)

    def test_release_candidates_say_so_once(self):
        candidate = release_notes.parse_release("v1.2.0-rc1", CHANGELOG)
        summary = Summary("A fix.", "", (), (Item("A fix.", (10,)),), None)
        notes = release_notes.render(candidate, SupportWindow(1, PUBLISHED_AT, 3), summary)
        self.assertTrue(notes.startswith("**Release candidate for 1.2.0. A fix.**"))
        summary = Summary("Release candidate with a fix.", "", (), (Item("A fix.", (10,)),), None)
        notes = release_notes.render(candidate, SupportWindow(1, PUBLISHED_AT, 3), summary)
        self.assertTrue(notes.startswith("**Release candidate with a fix.**"))

    def test_rejects_uncited_links_and_inline_references(self):
        cases = {
            "cites 99": {"highlights": [{"text": "Faster.", "prs": [99]}]},
            "contains a link": {"highlights": [{"text": "See [docs](https://example.com).", "prs": [11]}]},
            "writes a PR number": {"highlights": [{"text": "Faster (#11).", "prs": [11]}]},
            "highlights is empty": {"highlights": []},
            "must not contain bold": {"headline": "A **big** change."},
            "Markdown block syntax": {"highlights": [{"text": "- nested", "prs": [11]}]},
        }
        for message, override in cases.items():
            with self.subTest(message):
                summary, errors, _ = self.parse(summary_json(**override))
                self.assertIsNone(summary)
                self.assertTrue(any(message in error for error in errors), errors)

    def test_normalizes_bold_wrappers_whitespace_and_periods(self):
        summary, errors, _ = self.parse(summary_json(
            headline="**Two\nlines**",
            before_you_upgrade=[{"title": "**Moved.**", "detail": "Use `new`.", "prs": [12, 12]}],
        ))
        self.assertEqual(errors, [])
        self.assertEqual(summary.headline, "Two lines.")
        self.assertEqual(summary.before_you_upgrade[0].title, "Moved")
        self.assertEqual(summary.before_you_upgrade[0].prs, (12,))
        self.assertEqual(summary.compatibility_note, "")

    def test_overlong_entries_are_warnings_not_errors(self):
        long_text = "Faster. " * 60
        summary, errors, warnings = self.parse(summary_json(highlights=[
            {"text": long_text, "prs": [11]}, {"text": "**Security:** bounded.", "prs": [13]},
        ]))
        self.assertEqual(errors, [])
        self.assertIsNotNone(summary)
        self.assertEqual(warnings, [f"highlights[0].text has {len(long_text.strip())} characters; "
                                    "shorten it to at most 400"])

    def test_reports_uncovered_security_and_removed_entries(self):
        summary, errors, gaps = self.parse(summary_json(
            before_you_upgrade=[], highlights=[{"text": "New setting.", "prs": [11]}],
        ))
        self.assertEqual(errors, [])
        self.assertIsNotNone(summary)
        self.assertEqual(gaps, ["Removed entry 12 is not cited by any summary entry",
                                "Security entry 13 is not cited by any summary entry"])

    def test_prompt_examples_pass_validation(self):
        examples = re.findall(r"```json\n(.*?)```", release_notes.PROMPT.read_text(), re.DOTALL)
        self.assertEqual(len(examples), 2)
        for example in examples:
            data = json.loads(example)
            entries = [*data["before_you_upgrade"], *data["highlights"], data["library_users"]]
            prs = frozenset(n for entry in entries for n in entry["prs"])
            example_release = release_notes.Release("v1.8.4", "1.8.4", "1.8.4", "2026-11-06",
                                                    "[1.8.4] - 2026-11-06", "body", prs, {})
            summary, errors, gaps = release_notes.parse_summary(example, example_release)
            self.assertEqual((errors, gaps), ([], []))
            self.assertIsNotNone(summary)


class DraftTests(unittest.TestCase):
    def draft(self, replies: list[str]):
        calls = []

        def complete(messages):
            calls.append(list(messages))
            return replies[len(calls) - 1]
        return release_notes.draft_summary(release(), "instructions", complete), calls

    def test_sends_the_changelog_and_must_cover_entries(self):
        (summary, gaps), calls = self.draft([summary_json()])
        self.assertEqual(gaps, [])
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0], {"role": "system", "content": "instructions"})
        message = calls[0][1]["content"]
        self.assertIn("Release tag: v1.2.0 (stable release)", message)
        self.assertIn("PR numbers you may cite: 11, 12, 13", message)
        self.assertIn("Security entries to cover: 13", message)
        self.assertIn("<changelog>\n### Added", message)

    def test_retries_once_with_the_problems(self):
        (summary, gaps), calls = self.draft(["not json", summary_json()])
        self.assertEqual(len(calls), 2)
        self.assertEqual(calls[1][2], {"role": "assistant", "content": "not json"})
        self.assertIn("- the reply is not valid JSON", calls[1][3]["content"])
        self.assertEqual(summary.highlights[1].prs, (11,))

    def test_keeps_a_usable_draft_when_the_retry_is_worse(self):
        uncovered = summary_json(highlights=[{"text": "**Security:** bounded.", "prs": [13]}],
                                 before_you_upgrade=[])
        (summary, gaps), calls = self.draft([uncovered, "not json"])
        self.assertEqual(len(calls), 2)
        self.assertIn("Removed entry 12", calls[1][3]["content"])
        self.assertEqual(gaps, ["Removed entry 12 is not cited by any summary entry"])
        self.assertEqual(summary.highlights[0].prs, (13,))

    def test_asks_once_to_shorten_then_keeps_the_draft(self):
        overlong = summary_json(library_users={"text": "Changed. " * 60, "prs": [12]})
        (summary, warnings), calls = self.draft([overlong, overlong])
        self.assertEqual(len(calls), 2)
        self.assertIn("library_users.text has 539 characters; shorten it to at most 500",
                      calls[1][3]["content"])
        self.assertEqual(len(warnings), 1)
        self.assertTrue(summary.library_users.text.startswith("Changed."))

    def test_fails_after_two_unusable_replies(self):
        with self.assertRaisesRegex(NotesError, "unusable: the reply is not valid JSON"):
            self.draft(["not json", "still not json"])


def openai_reply(text: str, status: str = "completed", **extra) -> dict:
    """Return a Responses API reply carrying `text`."""
    return {
        "status": status,
        "output": [{"type": "reasoning", "summary": []},
                   {"type": "message", "content": [{"type": "output_text", "text": text}]}],
        "usage": {"input_tokens": 1000, "output_tokens": 200},
        **extra,
    }


class OpenAIDrafterTests(unittest.TestCase):
    settings = ModelSettings("gpt-test", "medium", 16384)

    def test_requests_strict_unstored_json(self):
        requests = []

        def post(url, key, body):
            requests.append((url, key, body))
            return openai_reply("{}")
        drafter = OpenAIDrafter(self.settings, "key", post=post)
        self.assertEqual(drafter.complete([{"role": "user", "content": "hi"}]), "{}")
        url, key, body = requests[0]
        self.assertEqual((url, key), (release_notes.OPENAI_RESPONSES_URL, "key"))
        self.assertIs(body["store"], False)
        self.assertEqual(body["service_tier"], "default")
        self.assertEqual(body["reasoning"], {"effort": "medium"})
        self.assertEqual(body["text"]["format"]["schema"], release_notes.SUMMARY_SCHEMA)
        self.assertIs(body["text"]["format"]["strict"], True)
        self.assertEqual((drafter.usage.input_tokens, drafter.usage.output_tokens), (1000, 200))

    def test_retries_transient_failures(self):
        attempts, sleeps = [], []

        def post(url, key, body):
            attempts.append(1)
            if len(attempts) < 3:
                raise RetryableError("HTTP 503")
            return openai_reply("{}")
        drafter = OpenAIDrafter(self.settings, "key", post=post, sleep=sleeps.append)
        self.assertEqual(drafter.complete([]), "{}")
        self.assertEqual(sleeps, [10, 20])

        def failing(url, key, body):
            raise RetryableError("HTTP 503")
        with self.assertRaises(RetryableError):
            OpenAIDrafter(self.settings, "key", post=failing, sleep=lambda _: None).complete([])

    def test_incomplete_and_refused_replies_are_errors(self):
        incomplete = openai_reply("{", status="incomplete",
                                  incomplete_details={"reason": "max_output_tokens"})
        refused = openai_reply("")
        refused["output"][1]["content"] = [{"type": "refusal", "refusal": "no"}]
        for reply, message in ((incomplete, "incomplete"), (refused, "refused"), ([], "unexpected")):
            with self.subTest(message):
                drafter = OpenAIDrafter(self.settings, "key", post=lambda *_, r=reply: r)
                with self.assertRaisesRegex(NotesError, message):
                    drafter.complete([])

    def test_reads_the_release_review_openai_entry(self):
        settings = release_notes.model_settings(json.dumps({"openai": {
            "model": "gpt-6.1-sol", "input_usd_per_million": "2.50", "output_usd_per_million": "10",
            "max_input_bytes": 260000, "max_output_tokens": 16384, "reasoning_effort": "medium",
        }}))
        self.assertEqual((settings.model, settings.max_output_tokens), ("gpt-6.1-sol", 16384))
        usage = release_notes.Usage(input_tokens=20_000, output_tokens=5_000, calls=1)
        self.assertEqual(usage.describe(settings),
                         "`gpt-6.1-sol`, 1 call(s), 20,000 input and 5,000 output tokens, about $0.10")
        for config in ("", "{}", '{"openai": {"model": "bad model"}}'):
            with self.subTest(config), self.assertRaises(NotesError):
                release_notes.model_settings(config)


class PostJsonTests(unittest.TestCase):
    """`post_json` against a local server standing in for the model API."""

    def setUp(self):
        self.requests = []
        tests = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                length = int(self.headers["Content-Length"])
                tests.requests.append((self.headers["Authorization"], json.loads(self.rfile.read(length))))
                status, headers, body = tests.response
                self.send_response(status)
                for name, value in headers.items():
                    self.send_header(name, value)
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.server.server_port}/v1/responses"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()

    def post(self, status: int, body: bytes, headers: dict | None = None):
        self.response = (status, headers or {}, body)
        return release_notes.post_json(self.url, "test-key", {"model": "m"}, timeout=10)

    def test_sends_the_key_and_decodes_the_reply(self):
        self.assertEqual(self.post(200, b'{"status": "completed"}'), {"status": "completed"})
        self.assertEqual(self.requests, [("Bearer test-key", {"model": "m"})])

    def test_http_errors_never_echo_the_reply_body(self):
        for status, kind in ((503, RetryableError), (429, RetryableError), (400, NotesError)):
            with self.subTest(status), self.assertRaises(kind) as raised:
                self.post(status, b"secret detail")
            self.assertEqual(str(raised.exception), f"the model API returned HTTP {status}")
        with self.assertRaises(NotesError) as raised:
            self.post(400, b"secret detail")
        self.assertNotIsInstance(raised.exception, RetryableError)

    def test_refuses_redirects_and_invalid_json(self):
        with self.assertRaisesRegex(NotesError, "redirected"):
            self.post(307, b"", {"Location": "https://example.com/v1/responses"})
        with self.assertRaisesRegex(NotesError, "invalid JSON"):
            self.post(200, b"<html>")


class PlaceholderTests(unittest.TestCase):
    def test_placeholders_match_the_release_workflows(self):
        workflows = release_notes.REPOSITORY_ROOT / ".github" / "workflows"
        text = "".join((workflows / name).read_text()
                       for name in ("create-release.yml", "release-binaries.yml"))
        for body in release_notes.PLACEHOLDER_BODIES - {""}:
            with self.subTest(body):
                self.assertIn(f'"{body}"', text)

    def test_only_placeholders_are_replaceable(self):
        self.assertTrue(release_notes.is_placeholder(
            "Release created by the protected Create release workflow.\n"))
        self.assertTrue(release_notes.is_placeholder(" "))
        self.assertFalse(release_notes.is_placeholder("**Faster relay.**"))


class CommandLineTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        for path, text in sources().items():
            (self.root / path).parent.mkdir(parents=True, exist_ok=True)
            (self.root / path).write_text(text)

    def tearDown(self):
        self.directory.cleanup()

    def run_main(self, *arguments: str) -> int:
        return release_notes.main(["generate", "--tag", "v1.2.0", "--source-dir", str(self.root),
                                   "--published-at", "2026-11-10T19:36:04Z", *arguments])

    def test_skeleton_renders_the_fixed_parts(self):
        output, summary = self.root / "out" / "notes.md", self.root / "summary.md"
        self.assertEqual(self.run_main("--skeleton", "--output", str(output),
                                       "--summary", str(summary)), 0)
        notes = output.read_text()
        self.assertTrue(notes.startswith("**TODO: summarize the release in one sentence.**"))
        self.assertIn("until block 3,643,309, around December 5, 2026.", notes)
        self.assertIn("CHANGELOG.md#120---2026-11-10", notes)
        report = summary.read_text()
        self.assertIn("## Drafted release notes for v1.2.0", report)
        self.assertIn("````markdown\n**TODO", report)

    def test_drafting_needs_an_api_key(self):
        with patch.dict(os.environ, {"OPENAI_API_KEY": ""}), \
                patch("sys.stderr") as stderr:
            self.assertEqual(self.run_main(), 2)
        self.assertIn("OPENAI_API_KEY", "".join(call.args[0] for call in stderr.write.call_args_list))

    def test_is_placeholder_exit_codes(self):
        body = self.root / "body.md"
        body.write_text("Release created by release-binaries.")
        self.assertEqual(release_notes.main(["is-placeholder", str(body)]), 0)
        body.write_text("Edited notes.")
        self.assertEqual(release_notes.main(["is-placeholder", str(body)]), 1)

    def test_fences_outgrow_backtick_runs(self):
        self.assertEqual(release_notes.markdown_fence("plain"), "````")
        self.assertEqual(release_notes.markdown_fence("`````"), "``````")


if __name__ == "__main__":
    unittest.main()
