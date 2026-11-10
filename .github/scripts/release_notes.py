#!/usr/bin/env python3
"""Draft Zakura's GitHub release notes from the release's CHANGELOG.md section.

The model writes only the curated summary, as JSON that is checked against a
schema and against the changelog. Everything else is rendered here, so every
release gets the same sections in the same order. See
`.github/release-notes/README.md`.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Callable

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
PROMPT = REPOSITORY_ROOT / ".github" / "release-notes" / "prompt.md"
DEFAULT_REPOSITORY = "zakura-core/zakura"
OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"

CHANGELOG = "CHANGELOG.md"
END_OF_SUPPORT = "crates/zakurad/src/components/sync/end_of_support.rs"
ACTIVATION_HEIGHTS = "crates/zakura-chain/src/parameters/constants.rs"
NETWORK_UPGRADE = "crates/zakura-chain/src/parameters/network_upgrade.rs"

# Bodies that the release workflows publish before real notes exist. Only these
# are replaced without `overwrite`; tests check they still match the workflows.
PLACEHOLDER_BODIES = frozenset({
    "",
    "Release created by the protected Create release workflow.",
    "Source-first release: binaries are being built and attached by the tag-push release-binaries run.",
    "Release created by release-binaries.",
})

# Changelog categories whose entries the summary must cite somewhere: new
# features operators can't use unless they hear about them, and changes they
# can't afford to miss.
COVERED_CATEGORIES = ("Added", "Security", "Removed", "Deprecated")

# Character limits just above the longest entries of the hand-written v1.8
# notes. A longer entry gets one retry to shorten it, then is kept with a
# warning. The schema's `maxLength` values are only a backstop against runaway
# output, since strict decoding may cut a string off at that length.
LENGTH_LIMITS = {"headline": 150, "compatibility_note": 200, "title": 60,
                 "detail": 400, "highlight": 400, "library_users": 500}

RELEASE_TAG = re.compile(r"v(?P<version>(?P<core>[0-9]+\.[0-9]+\.[0-9]+)(?:-[0-9A-Za-z.-]+)?)")
SECTION_HEADING = re.compile(
    r"^## (?P<heading>\[(?P<version>[^\]\n]+)\] - (?P<date>[0-9]{4}-[0-9]{2}-[0-9]{2}))[ \t]*$",
    re.MULTILINE,
)
LEVEL_TWO_HEADING = re.compile(r"^## ", re.MULTILINE)
CATEGORY_HEADING = re.compile(r"^### (?P<name>.+?)[ \t]*$", re.MULTILINE)
PR_REFERENCE = re.compile(r"(?<![\w&])#[0-9]+\b")
NUMBER_WORDS = {1: "one", 2: "two", 3: "three", 4: "four", 5: "five",
                6: "six", 7: "seven", 8: "eight", 9: "nine"}
RETRYABLE_HTTP = frozenset({429, 500, 502, 503, 504})
MAX_ATTEMPTS = 2


class NotesError(RuntimeError):
    """Release notes could not be drafted; the message is safe to show in CI logs."""


@dataclass(frozen=True)
class Release:
    """The `CHANGELOG.md` section that a release's notes are built from."""

    tag: str
    version: str
    core_version: str
    date: str
    heading: str
    body: str
    prs: frozenset[int]
    covered: dict[str, tuple[int, ...]]

    @property
    def is_prerelease(self) -> bool:
        """Whether the tag names a release candidate or other pre-release."""
        return self.version != self.core_version


def parse_release(tag: str, changelog: str, repository: str = DEFAULT_REPOSITORY) -> Release:
    """Return the changelog section for `tag`, the PRs it links, and its must-cover PRs."""
    match = RELEASE_TAG.fullmatch(tag)
    if match is None:
        raise NotesError(f"{tag!r} is not a release tag such as v1.2.3 or v1.2.3-rc1")
    version = match["version"]
    heading = next((h for h in SECTION_HEADING.finditer(changelog) if h["version"] == version), None)
    if heading is None:
        raise NotesError(f"{CHANGELOG} has no `## [{version}] - YYYY-MM-DD` section")
    following = LEVEL_TWO_HEADING.search(changelog, heading.end())
    body = changelog[heading.end():following.start() if following else len(changelog)].strip()
    if not body:
        raise NotesError(f"the {CHANGELOG} section for {version} is empty")

    link = re.compile(rf"\]\(https://github\.com/{re.escape(repository)}/pull/([0-9]+)\)")
    categories = list(CATEGORY_HEADING.finditer(body))
    covered = {}
    for index, category in enumerate(categories):
        if category["name"] in COVERED_CATEGORIES:
            end = categories[index + 1].start() if index + 1 < len(categories) else len(body)
            numbers = sorted({int(n) for n in link.findall(body, category.end(), end)})
            covered[category["name"]] = tuple(numbers)
    return Release(
        tag=tag, version=version, core_version=match["core"], date=heading["date"],
        heading=heading["heading"], body=body,
        prs=frozenset(int(n) for n in link.findall(body)), covered=covered,
    )


def public_changelog(body: str) -> str:
    """Return the changelog section without HTML comments, such as release-readiness waivers."""
    without_comments = re.sub(r"[ \t]*<!--.*?-->[ \t]*", "", body, flags=re.DOTALL)
    return re.sub(r"\n{3,}", "\n\n", without_comments).strip()


def heading_anchor(heading: str) -> str:
    """Return the fragment GitHub generates for a Markdown heading's text."""
    return re.sub(r"[^\w\- ]", "", heading.strip().lower()).replace(" ", "-")


@dataclass(frozen=True)
class SupportWindow:
    """The last Mainnet height a release runs at, and roughly when it is reached."""

    height: int
    reached_at: datetime
    warning_days: int

    def sentence(self) -> str:
        """Return the fixed support-window sentence of the notes."""
        days = NUMBER_WORDS.get(self.warning_days, str(self.warning_days))
        unit = "day" if self.warning_days == 1 else "days"
        reached = self.reached_at
        return (
            f"Supported on Mainnet until block {self.height:,}, around "
            f"{reached:%B} {reached.day}, {reached.year}. "
            f"Upgrade warnings start about {days} {unit} earlier."
        )


def rust_integer(source: str, name: str, path: str, *, required: bool = True) -> int | None:
    """Return the literal assigned to Rust constant `name`, written as `N` or `Height(N)`."""
    match = re.search(
        rf"\bconst\s+{name}\s*:\s*\w+\s*=\s*(?:Height\(\s*)?([0-9][0-9_]*)\s*\)?\s*;", source
    )
    if match is None:
        if required:
            raise NotesError(f"cannot find the integer constant {name} in {path}")
        return None
    return int(match[1].replace("_", ""))


def mainnet_heights(source: str) -> str:
    """Return the `mainnet` activation-height module of `constants.rs`."""
    start = source.find("pub mod mainnet")
    if start < 0:
        raise NotesError(f"cannot find the Mainnet activation heights in {ACTIVATION_HEIGHTS}")
    end = source.find("pub mod ", start + 1)
    return source[start:end if end >= 0 else len(source)]


def spacing_schedule(upgrades: str, heights: str) -> tuple[tuple[int, int], ...]:
    """Return Mainnet's `(first height, target spacing in seconds)` pairs, oldest first.

    Refuses a `NetworkUpgrade::target_spacings` that lists other upgrades, so a
    new spacing change fails here instead of silently skewing the support window.
    """
    function = re.search(r"fn target_spacings\(.*?\.into_iter\(\)", upgrades, re.DOTALL)
    listed = re.findall(r"NetworkUpgrade::(\w+)", function[0]) if function else []
    if listed != ["Genesis", "Blossom", "Nu7"]:
        raise NotesError(
            f"`target_spacings` in {NETWORK_UPGRADE} changed; update the schedule in release_notes.py"
        )
    mainnet = mainnet_heights(heights)
    return (
        (0, rust_integer(upgrades, "PRE_BLOSSOM_POW_TARGET_SPACING", NETWORK_UPGRADE)),
        (rust_integer(mainnet, "BLOSSOM", ACTIVATION_HEIGHTS),
         rust_integer(upgrades, "POST_BLOSSOM_POW_TARGET_SPACING", NETWORK_UPGRADE)),
        (rust_integer(mainnet, "NU7", ACTIVATION_HEIGHTS),
         rust_integer(upgrades, "POST_NU7_POW_TARGET_SPACING", NETWORK_UPGRADE)),
    )


def height_after(start: int, seconds: int, schedule: tuple[tuple[int, int], ...]) -> int:
    """Port of `estimated_height_after_release` in `end_of_support.rs`."""
    height, remaining = start, seconds
    for index, (_, spacing) in enumerate(schedule):
        blocks = remaining // spacing
        if index + 1 < len(schedule):
            # The block at the next spacing's start height already uses that spacing.
            until_next = max(schedule[index + 1][0] - height - 1, 0)
            if until_next < blocks:
                height += until_next
                remaining -= until_next * spacing
                continue
        return height + blocks
    return height


def seconds_between(start: int, end: int, schedule: tuple[tuple[int, int], ...]) -> int:
    """Return the target time from block `start` to block `end`."""
    total = 0
    for index, (first, spacing) in enumerate(schedule):
        last = schedule[index + 1][0] - 1 if index + 1 < len(schedule) else end
        low, high = max(start + 1, first), min(end, last)
        if high >= low:
            total += (high - low + 1) * spacing
    return total


def support_window(read: Callable[[str], str | None], published_at: datetime) -> SupportWindow:
    """Compute the release's end-of-support height the way `zakurad` does, and date it.

    The date assumes the chain is at `ESTIMATED_RELEASE_HEIGHT` when the release
    is published, and target spacing after that.
    """
    eos = read(END_OF_SUPPORT)
    upgrades = read(NETWORK_UPGRADE)
    heights = read(ACTIVATION_HEIGHTS)
    if eos is None or upgrades is None or heights is None:
        raise NotesError("cannot read the end-of-support or network-upgrade sources")
    release_height = rust_integer(eos, "ESTIMATED_RELEASE_HEIGHT", END_OF_SUPPORT)
    panic_after = rust_integer(eos, "EOS_PANIC_AFTER", END_OF_SUPPORT)
    cap = rust_integer(eos, "MAINNET_EOS_HEIGHT", END_OF_SUPPORT, required=False)
    warn = re.search(
        r"\bconst\s+EOS_WARN_AFTER\s*:\s*u32\s*=\s*(?:EOS_PANIC_AFTER\s*-\s*([0-9_]+)|([0-9_]+))\s*;",
        eos,
    )
    if warn is None:
        raise NotesError(f"cannot find EOS_WARN_AFTER in {END_OF_SUPPORT}")
    warning_days = int(warn[1]) if warn[1] else panic_after - int(warn[2])

    schedule = spacing_schedule(upgrades, heights)
    height = height_after(release_height, panic_after * 24 * 60 * 60, schedule)
    if cap is not None:
        height = min(height, cap)

    if release_height >= height:
        raise NotesError(f"ESTIMATED_RELEASE_HEIGHT is already at end-of-support height {height}")
    reached_at = published_at + timedelta(seconds=seconds_between(release_height, height, schedule))
    return SupportWindow(height, reached_at.astimezone(timezone.utc), warning_days)


def string_schema(maximum: int) -> dict:
    """Return a JSON schema for a string of at most `maximum` characters."""
    return {"type": "string", "maxLength": maximum}


def object_schema(properties: dict) -> dict:
    """Return a strict JSON schema object that requires every property."""
    return {"type": "object", "properties": properties, "required": list(properties),
            "additionalProperties": False}


PRS_SCHEMA = {"type": "array", "maxItems": 12, "items": {"type": "integer"}}
SUMMARY_SCHEMA = object_schema({
    "headline": string_schema(240),
    "compatibility_note": string_schema(240),
    "before_you_upgrade": {"type": "array", "maxItems": 8, "items": object_schema({
        "title": string_schema(100), "detail": string_schema(900), "prs": PRS_SCHEMA,
    })},
    "highlights": {"type": "array", "maxItems": 10, "items": object_schema({
        "text": string_schema(700), "prs": PRS_SCHEMA,
    })},
    "library_users": object_schema({"text": string_schema(1200), "prs": PRS_SCHEMA}),
})


@dataclass(frozen=True)
class Item:
    """One summary entry and the changelog PRs it is based on."""

    text: str
    prs: tuple[int, ...]
    title: str = ""


@dataclass(frozen=True)
class Summary:
    """The curated part of the notes, as validated from the model's JSON."""

    headline: str
    compatibility_note: str
    before_you_upgrade: tuple[Item, ...]
    highlights: tuple[Item, ...]
    library_users: Item | None


def checked_text(value, where: str, limit: str, problems: tuple[list[str], list[str]], *,
                 required: bool = True, bold: bool = False) -> str:
    """Return `value` on one line, recording why it can't be rendered in the notes.

    `problems` is the `(errors, warnings)` pair: an error makes the draft unusable,
    while text over its `LENGTH_LIMITS[limit]` is only a warning. `bold` text is
    rendered inside `**`, so a wrapping `**` pair is dropped and any other `**`
    is an error.
    """
    errors, warnings = problems
    if not isinstance(value, str):
        errors.append(f"{where} must be a string")
        return ""
    text = " ".join(value.split())
    if bold:
        if text.startswith("**") and text.endswith("**"):
            text = text[2:-2].strip()
        if "**" in text:
            errors.append(f"{where} must not contain bold text")
    if not text:
        if required:
            errors.append(f"{where} is empty")
        return ""
    if len(text) > LENGTH_LIMITS[limit]:
        warnings.append(f"{where} has {len(text)} characters; shorten it to at most "
                        f"{LENGTH_LIMITS[limit]}")
    if "](" in text or "http://" in text or "https://" in text:
        errors.append(f"{where} contains a link; list PR numbers in `prs` instead")
    if PR_REFERENCE.search(text):
        errors.append(f"{where} writes a PR number in the text; list it in `prs` instead")
    if text.startswith(("#", "- ", "* ", ">")):
        errors.append(f"{where} starts with Markdown block syntax")
    return text


def checked_prs(value, where: str, release: Release, errors: list[str]) -> tuple[int, ...]:
    """Return the sorted unique PR numbers, recording any the changelog doesn't link."""
    if not isinstance(value, list) or not all(type(n) is int for n in value):
        errors.append(f"{where}.prs must be a list of PR numbers")
        return ()
    unknown = sorted(set(value) - release.prs)
    if unknown:
        listed = ", ".join(str(n) for n in unknown)
        errors.append(f"{where}.prs cites {listed}, which the changelog section does not link")
    return tuple(sorted(set(value) & release.prs))


def parse_summary(text: str, release: Release) -> tuple[Summary | None, list[str], list[str]]:
    """Validate the model's JSON reply.

    Returns the summary (or None), errors that make it unusable, and warnings
    worth one retry: overlong entries and must-cover changelog PRs that no
    entry cites.
    """
    errors: list[str] = []
    warnings: list[str] = []
    problems = (errors, warnings)
    try:
        data = json.loads(text)
    except ValueError:
        return None, ["the reply is not valid JSON"], []
    if not isinstance(data, dict) or set(data) != set(SUMMARY_SCHEMA["properties"]):
        return None, ["the reply does not match the schema's fields"], []

    headline = checked_text(data["headline"], "headline", "headline", problems, bold=True)
    if headline and not headline.endswith((".", "!", "?")):
        headline += "."
    before = []
    for index, entry in enumerate(_list(data["before_you_upgrade"], "before_you_upgrade", errors)):
        where = f"before_you_upgrade[{index}]"
        title = checked_text(entry.get("title"), f"{where}.title", "title", problems, bold=True)
        before.append(Item(
            title=title.rstrip(".").strip(),
            text=checked_text(entry.get("detail"), f"{where}.detail", "detail", problems),
            prs=checked_prs(entry.get("prs"), where, release, errors),
        ))
    highlights = []
    for index, entry in enumerate(_list(data["highlights"], "highlights", errors)):
        where = f"highlights[{index}]"
        highlights.append(Item(
            text=checked_text(entry.get("text"), f"{where}.text", "highlight", problems),
            prs=checked_prs(entry.get("prs"), where, release, errors),
        ))
    if not highlights:
        errors.append("highlights is empty")
    library = data["library_users"] if isinstance(data["library_users"], dict) else {}
    library_text = checked_text(library.get("text"), "library_users.text", "library_users",
                                problems, required=False)
    library_users = None
    if library_text:
        library_users = Item(library_text, checked_prs(library.get("prs"), "library_users", release, errors))
    compatibility = checked_text(data["compatibility_note"], "compatibility_note",
                                 "compatibility_note", problems, required=False)

    cited = {n for item in (*before, *highlights, *([library_users] if library_users else []))
             for n in item.prs}
    warnings += [
        f"{category} entry {n} is not cited by any summary entry"
        for category, numbers in release.covered.items() for n in numbers if n not in cited
    ]
    if errors:
        return None, errors, warnings
    summary = Summary(headline, "" if before else compatibility, tuple(before),
                      tuple(highlights), library_users)
    return summary, [], warnings


def _list(value, where: str, errors: list[str]) -> list[dict]:
    """Return `value` if it is a list of objects, recording an error otherwise."""
    if not isinstance(value, list) or not all(isinstance(entry, dict) for entry in value):
        errors.append(f"{where} must be a list of objects")
        return []
    return value


def cited_sentence(text: str, prs: tuple[int, ...], repository: str) -> str:
    """End `text` with its PR links before the final period, like the changelog entries."""
    sentence = text.rstrip()
    if sentence.endswith("."):
        sentence = sentence[:-1]
    if prs:
        links = ", ".join(f"[#{n}](https://github.com/{repository}/pull/{n})" for n in prs)
        sentence += f" ({links})"
    return sentence + "."


def render(release: Release, support: SupportWindow, summary: Summary,
           repository: str = DEFAULT_REPOSITORY) -> str:
    """Render the release notes in the fixed layout around the curated summary."""
    headline = summary.headline
    if release.is_prerelease and not headline.lower().startswith("release candidate"):
        headline = f"Release candidate for {release.core_version}. {headline}"
    lines = [f"**{headline}**", "", support.sentence(), ""]
    if summary.compatibility_note:
        lines += [summary.compatibility_note, ""]
    if summary.before_you_upgrade:
        lines += ["### Before you upgrade", ""]
        lines += [f"- **{item.title}.** {cited_sentence(item.text, item.prs, repository)}"
                  for item in summary.before_you_upgrade]
        lines.append("")
    lines += ["### Highlights", ""]
    lines += [f"- {cited_sentence(item.text, item.prs, repository)}" for item in summary.highlights]
    lines.append("")
    if summary.library_users:
        lines += ["### Library users", "",
                  cited_sentence(summary.library_users.text, summary.library_users.prs, repository), ""]
    changelog = f"https://github.com/{repository}/blob/{release.tag}/CHANGELOG.md#{heading_anchor(release.heading)}"
    lines += [
        "<details>", "<summary>Full changelog</summary>", "", public_changelog(release.body), "",
        "</details>", "",
        f"Also in [CHANGELOG.md]({changelog}).", "",
        "<!-- Drafted from CHANGELOG.md by the Release notes workflow. Edit freely. -->",
    ]
    return "\n".join(lines) + "\n"


SKELETON = Summary(
    headline="TODO: summarize the release in one sentence.",
    compatibility_note="",
    before_you_upgrade=(),
    highlights=(Item("TODO: list the highlights", ()),),
    library_users=None,
)


@dataclass(frozen=True)
class ModelSettings:
    """The OpenAI entry of `RELEASE_REVIEW_MODELS`, shared with the release review."""

    model: str
    reasoning_effort: str
    max_output_tokens: int
    input_price: Decimal | None = None
    output_price: Decimal | None = None


def model_settings(models_json: str) -> ModelSettings:
    """Parse the model, effort, output allowance, and optional prices for OpenAI."""
    try:
        config = json.loads(models_json)["openai"]
    except (ValueError, KeyError, TypeError) as error:
        raise NotesError("set RELEASE_REVIEW_MODELS to a JSON object with an `openai` entry") from error
    if not isinstance(config, dict):
        raise NotesError("the `openai` entry of RELEASE_REVIEW_MODELS must be an object")
    model = config.get("model")
    if not isinstance(model, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_./:@+-]{0,199}", model):
        raise NotesError("RELEASE_REVIEW_MODELS.openai needs an explicit `model` ID")
    effort = config.get("reasoning_effort", "medium")
    if effort not in {"none", "low", "medium", "high"}:
        raise NotesError("reasoning_effort must be none, low, medium, or high")
    max_output = config.get("max_output_tokens", 16384)
    if type(max_output) is not int or not 1024 <= max_output <= 32768:
        raise NotesError("max_output_tokens must be between 1024 and 32768")
    return ModelSettings(model, effort, max_output,
                         _price(config.get("input_usd_per_million")),
                         _price(config.get("output_usd_per_million")))


def _price(value) -> Decimal | None:
    """Return a positive per-million-token price, or None when unset or invalid."""
    try:
        price = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return price if price.is_finite() and price > 0 else None


class NoRedirect(urllib.request.HTTPRedirectHandler):
    """Refuse redirects, so the API key is only ever sent to the configured endpoint."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        """Fail instead of following the redirect."""
        raise NotesError("the model API redirected the request; refusing to follow it")


class RetryableError(NotesError):
    """A transient model API failure that is worth retrying."""


def post_json(url: str, key: str, body: dict, *, timeout: int = 300) -> dict:
    """POST `body` as JSON and return the decoded reply, never echoing error bodies."""
    request = urllib.request.Request(
        url, data=json.dumps(body).encode(),
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
    )
    try:
        with urllib.request.build_opener(NoRedirect).open(request, timeout=timeout) as response:
            payload = response.read(2_000_001)
    except urllib.error.HTTPError as error:
        error.close()
        kind = RetryableError if error.code in RETRYABLE_HTTP else NotesError
        raise kind(f"the model API returned HTTP {error.code}") from None
    except (urllib.error.URLError, TimeoutError, OSError):
        raise RetryableError("the model API request failed or timed out") from None
    if len(payload) > 2_000_000:
        raise NotesError("the model API reply exceeded 2 MB")
    try:
        return json.loads(payload)
    except ValueError:
        raise NotesError("the model API returned invalid JSON") from None


@dataclass
class Usage:
    """Token usage across the model calls of one draft."""

    input_tokens: int = 0
    output_tokens: int = 0
    calls: int = 0

    def describe(self, settings: ModelSettings) -> str:
        """Return a one-line usage report, with a cost estimate when prices are configured."""
        text = (f"`{settings.model}`, {self.calls} call(s), {self.input_tokens:,} input and "
                f"{self.output_tokens:,} output tokens")
        if settings.input_price and settings.output_price:
            cost = (self.input_tokens * settings.input_price
                    + self.output_tokens * settings.output_price) / 1_000_000
            text += f", about ${cost:.2f}"
        return text


class OpenAIDrafter:
    """Ask the OpenAI Responses API for the summary, as strict-schema JSON."""

    def __init__(self, settings: ModelSettings, key: str, *, post=post_json, sleep=time.sleep):
        self.settings, self.key, self.post, self.sleep = settings, key, post, sleep
        self.usage = Usage()

    def complete(self, messages: list[dict[str, str]]) -> str:
        """Return the reply text for `messages`, retrying transient failures twice."""
        body = {
            "model": self.settings.model, "input": messages, "store": False,
            "service_tier": "default", "max_output_tokens": self.settings.max_output_tokens,
            "text": {"format": {"type": "json_schema", "name": "release_notes",
                                "strict": True, "schema": SUMMARY_SCHEMA}},
        }
        if self.settings.reasoning_effort != "none":
            body["reasoning"] = {"effort": self.settings.reasoning_effort}
        for attempt in range(3):
            try:
                reply = self.post(OPENAI_RESPONSES_URL, self.key, body)
                break
            except RetryableError:
                if attempt == 2:
                    raise
                self.sleep(10 * (attempt + 1))
        self.usage.calls += 1
        usage = reply.get("usage") if isinstance(reply, dict) else None
        if isinstance(usage, dict):
            for name in ("input_tokens", "output_tokens"):
                if type(usage.get(name)) is int:
                    setattr(self.usage, name, getattr(self.usage, name) + usage[name])
        try:
            if reply.get("status") != "completed":
                reason = (reply.get("incomplete_details") or {}).get("reason", "unknown")
                raise NotesError(f"the model reply is incomplete ({reason})")
            parts = [part for item in reply["output"] if item.get("type") == "message"
                     for part in item.get("content", [])]
        except (KeyError, TypeError, AttributeError):
            raise NotesError("the model API returned an unexpected reply") from None
        if any(part.get("type") == "refusal" for part in parts):
            raise NotesError("the model refused to draft the notes")
        return "".join(part.get("text", "") for part in parts if part.get("type") == "output_text")


def request_message(release: Release) -> str:
    """Return the user message: the release facts and its changelog section."""
    kind = (f"release candidate for {release.core_version}" if release.is_prerelease
            else "stable release")
    lines = [
        f"Release tag: {release.tag} ({kind})",
        f"Changelog date: {release.date}",
        "PR numbers you may cite: " + (", ".join(str(n) for n in sorted(release.prs)) or "none"),
    ]
    for category, numbers in release.covered.items():
        if numbers:
            lines.append(f"{category} entries to cover: " + ", ".join(str(n) for n in numbers))
    lines += ["", f"{CHANGELOG} section:", "", "<changelog>", release.body, "</changelog>"]
    return "\n".join(lines)


def draft_summary(release: Release, instructions: str,
                  complete: Callable[[list[dict[str, str]]], str]) -> tuple[Summary, list[str]]:
    """Ask for the summary, retrying once with the validation problems as feedback.

    Returns the usable summary with the fewest warnings, and those warnings.
    """
    messages = [{"role": "system", "content": instructions},
                {"role": "user", "content": request_message(release)}]
    best: tuple[Summary, list[str]] | None = None
    errors: list[str] = []
    for attempt in range(MAX_ATTEMPTS):
        reply = complete(messages)
        summary, errors, warnings = parse_summary(reply, release)
        if summary is not None:
            if not warnings:
                return summary, []
            if best is None or len(warnings) < len(best[1]):
                best = (summary, warnings)
        if attempt + 1 < MAX_ATTEMPTS:
            feedback = "\n".join(f"- {problem}" for problem in (*errors, *warnings))
            messages += [{"role": "assistant", "content": reply},
                         {"role": "user", "content": "Fix these problems and return the complete "
                                                     "JSON again:\n" + feedback}]
    if best is not None:
        return best
    raise NotesError("the model's summary is unusable: " + "; ".join(errors))


def is_placeholder(body: str) -> bool:
    """Whether `body` is a placeholder that a release workflow published before notes existed."""
    return body.strip() in PLACEHOLDER_BODIES


def source_reader(*, ref: str | None, source_dir: Path | None) -> Callable[[str], str | None]:
    """Return a reader for release files at Git `ref`, or in `source_dir` when given."""
    if source_dir is not None:
        def read_file(path: str) -> str | None:
            file = source_dir / path
            return file.read_text() if file.is_file() else None
        return read_file

    def read_git(path: str) -> str | None:
        result = subprocess.run(["git", "show", f"{ref}:{path}"], cwd=REPOSITORY_ROOT,
                                capture_output=True, text=True)
        return result.stdout if result.returncode == 0 else None
    return read_git


def markdown_fence(text: str) -> str:
    """Return a backtick fence longer than any backtick run in `text`."""
    longest = max((len(run) for run in re.findall(r"`+", text)), default=0)
    return "`" * max(4, longest + 1)


def write_summary(path: Path, release: Release, notes: str, details: list[str]) -> None:
    """Append the drafted notes and their Markdown source to a GitHub job summary."""
    fence = markdown_fence(notes)
    lines = [f"## Drafted release notes for {release.tag}", ""]
    lines += [f"- {detail}" for detail in details]
    lines += ["", "---", "", notes, "---", "", "<details>",
              "<summary>Markdown source</summary>", "", f"{fence}markdown", notes.rstrip("\n"),
              fence, "", "</details>", ""]
    with path.open("a") as summary:
        summary.write("\n".join(lines))


def generate(args: argparse.Namespace) -> int:
    """Draft the notes for `args.tag` and write them to `args.output` or stdout."""
    read = source_reader(ref=args.ref or args.tag, source_dir=args.source_dir)
    changelog = read(CHANGELOG)
    if changelog is None:
        raise NotesError(f"cannot read {CHANGELOG} at {args.ref or args.tag}")
    release = parse_release(args.tag, changelog, args.repository)
    support = support_window(read, args.published_at)
    details = []
    if args.skeleton:
        summary, warnings = SKELETON, []
        details.append("Skeleton only: no model was called.")
    else:
        key = os.environ.get("OPENAI_API_KEY", "")
        if not key:
            raise NotesError("set OPENAI_API_KEY, or pass --skeleton to skip the model")
        settings = model_settings(os.environ.get("RELEASE_REVIEW_MODELS", ""))
        drafter = OpenAIDrafter(settings, key)
        try:
            summary, warnings = draft_summary(release, PROMPT.read_text(), drafter.complete)
        finally:
            print(f"Model usage: {drafter.usage.describe(settings)}", file=sys.stderr)
        details.append(f"Drafted with {drafter.usage.describe(settings)}.")
    for warning in warnings:
        print(f"::warning title=Release notes::{warning}", file=sys.stderr)
        details.append(f"Review: {warning}.")
    notes = render(release, support, summary, args.repository)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(notes)
    else:
        sys.stdout.write(notes)
    if args.summary:
        write_summary(args.summary, release, notes, details)
    return 0


def timestamp(value: str) -> datetime:
    """Parse an ISO 8601 timestamp that includes a time zone, such as GitHub's `published_at`."""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise argparse.ArgumentTypeError(f"{value!r} is not an ISO 8601 timestamp") from None
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError(f"{value!r} has no time zone")
    return parsed


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    """Parse the command line."""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    draft = commands.add_parser("generate", help="draft the notes for a release tag")
    draft.add_argument("--tag", required=True, help="release tag, for example v1.8.6")
    source = draft.add_mutually_exclusive_group()
    source.add_argument("--ref", help="Git ref to read the release files at (default: the tag)")
    source.add_argument("--source-dir", type=Path, help="checkout to read the release files from")
    # The changelog links PRs in the canonical repository, whichever repository runs this.
    draft.add_argument("--repository", default=DEFAULT_REPOSITORY,
                       help="repository whose PR links the changelog uses")
    draft.add_argument("--published-at", type=timestamp, default=datetime.now(timezone.utc),
                       help="when the release was published, as ISO 8601 (default: now)")
    draft.add_argument("--output", type=Path, help="write the notes here instead of stdout")
    draft.add_argument("--summary", type=Path, help="append the notes to this job summary file")
    draft.add_argument("--skeleton", action="store_true",
                       help="render only the fixed parts, without calling the model")
    placeholder = commands.add_parser(
        "is-placeholder", help="exit 0 if the release body in FILE is a workflow placeholder")
    placeholder.add_argument("body_file", type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run the command-line interface."""
    args = parse_args(argv)
    if args.command == "is-placeholder":
        return 0 if is_placeholder(args.body_file.read_text()) else 1
    try:
        return generate(args)
    except NotesError as error:
        print(f"::error title=Release notes::{error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
