#!/usr/bin/env python3
"""Compute release-version and Mainnet-height readiness values."""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
import tempfile
import unittest
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import changelog

BLOCKS_PER_DAY = 1152
MINOR_CATEGORIES = {"Added", "Changed", "Deprecated", "Removed"}
NETWORK_UPGRADE = re.compile(r"\b(?:mainnet\s+)?network upgrade\b", re.IGNORECASE)
VERSION = re.compile(
    r"^(?P<major>0|[1-9][0-9]*)\."
    r"(?P<minor>0|[1-9][0-9]*)\."
    r"(?P<patch>0|[1-9][0-9]*)"
    r"(?P<prerelease>-rc(?P<rc>[0-9]+))?$"
)


class ReadinessError(RuntimeError):
    """A release-readiness input or invariant is invalid."""


@dataclass(frozen=True, order=True)
class CoreVersion:
    major: int
    minor: int
    patch: int

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"


@dataclass(frozen=True)
class Version:
    core: CoreVersion
    rc: int | None


def parse_version(value: str) -> Version:
    match = VERSION.fullmatch(value)
    if match is None:
        raise ReadinessError(f"invalid version {value!r}; expected X.Y.Z or X.Y.Z-rcN")
    return Version(
        CoreVersion(
            int(match.group("major")),
            int(match.group("minor")),
            int(match.group("patch")),
        ),
        int(match.group("rc")) if match.group("rc") is not None else None,
    )


def changelog_context(
    repo_root: Path, base_version: str
) -> tuple[dict[str, list[str]], dict[str, object]]:
    root_path = repo_root / changelog.ROOT_CHANGELOG
    _, unreleased, suffix = changelog.split_unreleased(
        root_path.read_text(), root_path
    )
    unreleased_entries = changelog.parse_unreleased_body(unreleased, root_path)
    fragments = changelog.load_fragments(repo_root)
    entries: dict[str, list[str]] = {
        category: [body]
        for category, body in unreleased_entries.items()
    }
    for fragment in fragments:
        for category, body in fragment.entries.items():
            entries.setdefault(category, []).append(body)

    # Include assembled-but-untagged release sections. This matters when a
    # prepared release PR was merged but publication was stopped: a retry must
    # still see the entries that justified its release level.
    matches = list(changelog.VERSION_HEADING.finditer(suffix))
    found_base = False
    for index, match in enumerate(matches):
        version = match.group(1)
        if version == base_version:
            found_base = True
            break
        section_end = (
            matches[index + 1].start() if index + 1 < len(matches) else len(suffix)
        )
        body = suffix[match.end() : section_end].strip()
        for category, category_body in changelog.parse_category_body(
            body, root_path, f"release {version}"
        ).items():
            entries.setdefault(category, []).append(category_body)
    if not found_base:
        raise ReadinessError(
            f"root changelog has no section for base version {base_version}"
        )
    return entries, {
        "latest_version": changelog.latest_changelog_version(suffix, root_path),
        "unreleased_categories": sorted(unreleased_entries),
        "pending_fragments": len(fragments),
    }


def release_entries(repo_root: Path, base_version: str) -> dict[str, list[str]]:
    entries, _ = changelog_context(repo_root, base_version)
    return entries


def minimum_release(
    base: Version, target: Version, entries: dict[str, list[str]]
) -> tuple[str, CoreVersion, list[str]]:
    categories = sorted(entries)
    combined = "\n".join(body for bodies in entries.values() for body in bodies)

    # Later candidates and the stable release keep the core chosen by the
    # first candidate. New entries during the candidate cycle are folded into
    # that pending release rather than forcing another bump.
    if base.rc is not None and target.core == base.core:
        if target.rc is not None and target.rc <= base.rc:
            raise ReadinessError(
                f"target rc{target.rc} must advance from base rc{base.rc}"
            )
        return "continuation", base.core, categories

    if NETWORK_UPGRADE.search(combined):
        level = "major"
        minimum = CoreVersion(base.core.major + 1, 0, 0)
    elif MINOR_CATEGORIES.intersection(entries):
        level = "minor"
        minimum = CoreVersion(base.core.major, base.core.minor + 1, 0)
    else:
        level = "patch"
        minimum = CoreVersion(base.core.major, base.core.minor, base.core.patch + 1)

    if target.core < minimum:
        raise ReadinessError(
            f"target {target.core} is below the {minimum} {level} floor "
            f"required by changelog categories: {', '.join(categories) or 'none'}"
        )
    if target.core <= base.core:
        raise ReadinessError(
            f"target {target.core} must advance from released version {base.core}"
        )

    return level, minimum, categories


def version_report(repo_root: Path, base_version: str, release_tag: str) -> dict[str, object]:
    if not release_tag.startswith("v"):
        raise ReadinessError("release tag must start with 'v'")
    base = parse_version(base_version)
    target = parse_version(release_tag.removeprefix("v"))
    entries, context = changelog_context(repo_root, base_version)
    level, minimum, categories = minimum_release(base, target, entries)
    return {
        "base": base_version,
        "target": release_tag.removeprefix("v"),
        "minimum_level": level,
        "minimum_version": str(minimum),
        "categories": categories,
        "changelog": context,
    }


def parse_timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ReadinessError("generated_at must be an RFC 3339 timestamp") from error
    if parsed.tzinfo is None:
        raise ReadinessError("generated_at must include a timezone")
    return parsed


def estimated_height(
    bundle_height: int,
    generated_at: datetime,
    now: datetime,
    delay_days: int,
) -> int:
    if bundle_height <= 0 or bundle_height >= 2**32:
        raise ReadinessError("bundle height is outside the u32 block-height range")
    if delay_days < 0 or delay_days > 30:
        raise ReadinessError("expected tag delay must be between 0 and 30 days")
    if generated_at > now + timedelta(minutes=10):
        raise ReadinessError("bundle generation time is unexpectedly in the future")

    elapsed = max(timedelta(), now - generated_at) + timedelta(days=delay_days)
    projected_blocks = math.ceil(elapsed.total_seconds() * BLOCKS_PER_DAY / 86400)
    projected = bundle_height + projected_blocks
    if projected >= 2**32:
        raise ReadinessError("projected release height exceeds the u32 range")
    return projected


def height_report(
    resolution_path: Path,
    delay_days: int,
    now: datetime | None = None,
) -> dict[str, object]:
    try:
        resolution = json.loads(resolution_path.read_text())
        bundle_height = resolution["height"]
        generated_at_text = resolution["generated_at"]
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise ReadinessError(f"cannot read release-state resolution: {error}") from error
    if isinstance(bundle_height, bool) or not isinstance(bundle_height, int):
        raise ReadinessError("release-state resolution height must be an integer")
    generated_at = parse_timestamp(generated_at_text)
    now = now or datetime.now(timezone.utc)
    return {
        "bundle_height": bundle_height,
        "generated_at": generated_at_text,
        "expected_tag_delay_days": delay_days,
        "estimated_release_height": estimated_height(
            bundle_height, generated_at, now, delay_days
        ),
    }


class SelfTests(unittest.TestCase):
    def test_assembled_untagged_section_still_sets_release_floor(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "docs" / "changelog" / "unreleased").mkdir(parents=True)
            (root / "CHANGELOG.md").write_text(
                "# Changelog\n\n"
                "## [Unreleased]\n\n"
                "## [1.0.6-rc0] - 2026-08-01\n\n"
                "### Added\n\n"
                "- Added an RPC.\n\n"
                "## [1.0.5] - 2026-07-27\n\n"
                "### Fixed\n\n"
                "- Fixed an older bug.\n"
            )
            entries, context = changelog_context(root, "1.0.5")
            self.assertEqual(sorted(entries), ["Added"])
            self.assertEqual(context["latest_version"], "1.0.6-rc0")
            self.assertEqual(context["unreleased_categories"], [])
            self.assertEqual(context["pending_fragments"], 0)
            with self.assertRaisesRegex(ReadinessError, "minor floor"):
                minimum_release(
                    parse_version("1.0.5"),
                    parse_version("1.0.6-rc0"),
                    entries,
                )

    def test_minor_category_rejects_patch_target(self) -> None:
        with self.assertRaisesRegex(ReadinessError, "minor floor"):
            minimum_release(
                parse_version("1.0.5"),
                parse_version("1.0.6-rc0"),
                {"Added": ["- Added an RPC."]},
            )

    def test_fixed_category_allows_patch_target(self) -> None:
        level, minimum, _ = minimum_release(
            parse_version("1.0.5"),
            parse_version("1.0.6-rc0"),
            {"Fixed": ["- Fixed a bug."]},
        )
        self.assertEqual((level, str(minimum)), ("patch", "1.0.6"))

    def test_network_upgrade_requires_major_target(self) -> None:
        with self.assertRaisesRegex(ReadinessError, "major floor"):
            minimum_release(
                parse_version("1.4.2"),
                parse_version("1.5.0-rc0"),
                {"Added": ["- Added the Mainnet Network Upgrade."]},
            )

    def test_release_candidate_continuation_keeps_core(self) -> None:
        level, minimum, _ = minimum_release(
            parse_version("1.1.0-rc0"),
            parse_version("1.1.0-rc1"),
            {},
        )
        self.assertEqual((level, str(minimum)), ("continuation", "1.1.0"))

    def test_stable_release_continues_release_candidate_core(self) -> None:
        level, minimum, _ = minimum_release(
            parse_version("1.1.0-rc1"),
            parse_version("1.1.0"),
            {},
        )
        self.assertEqual((level, str(minimum)), ("continuation", "1.1.0"))

    def test_release_candidate_cannot_repeat(self) -> None:
        with self.assertRaisesRegex(ReadinessError, "must advance"):
            minimum_release(
                parse_version("1.1.0-rc1"),
                parse_version("1.1.0-rc1"),
                {},
            )

    def test_release_candidate_cannot_decrease(self) -> None:
        with self.assertRaisesRegex(ReadinessError, "must advance"):
            minimum_release(
                parse_version("1.1.0-rc2"),
                parse_version("1.1.0-rc1"),
                {},
            )

    def test_height_projection_includes_delay(self) -> None:
        generated = datetime(2026, 8, 1, tzinfo=timezone.utc)
        now = generated + timedelta(hours=12)
        self.assertEqual(estimated_height(3_400_000, generated, now, 1), 3_401_728)


def main() -> int:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)

    version_parser = subparsers.add_parser("version")
    version_parser.add_argument("--repo-root", type=Path, default=Path.cwd())
    version_parser.add_argument("--base-version", required=True)
    version_parser.add_argument("--release-tag", required=True)

    height_parser = subparsers.add_parser("height")
    height_parser.add_argument("--resolution", type=Path, required=True)
    height_parser.add_argument("--expected-tag-delay-days", type=int, default=0)

    subparsers.add_parser("self-test")

    args = parser.parse_args()
    try:
        if args.command == "version":
            report = version_report(args.repo_root, args.base_version, args.release_tag)
        elif args.command == "height":
            report = height_report(
                args.resolution,
                args.expected_tag_delay_days,
            )
        else:
            suite = unittest.defaultTestLoader.loadTestsFromTestCase(SelfTests)
            return 0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1
    except (ReadinessError, changelog.ChangelogError) as error:
        print(f"release readiness failed: {error}", file=sys.stderr)
        return 1

    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
