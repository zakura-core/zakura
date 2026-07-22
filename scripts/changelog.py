#!/usr/bin/env python3
"""Validate and assemble Zakura changelog fragments."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from pathlib import Path


FRAGMENT_DIRECTORY = "changelog-unreleased"
NO_CHANGELOG_MARKER = "<!-- changelog: none -->"
ROOT_CHANGELOG = "CHANGELOG.md"
STANDARD_CATEGORIES = (
    "Added",
    "Changed",
    "Deprecated",
    "Removed",
    "Fixed",
    "Security",
)
CATEGORY_HEADING = re.compile(r"^## (.+?)\s*$")
VERSION_HEADING = re.compile(
    r"^## \[([0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?)\]"
    r" - ([0-9]{4}-[0-9]{2}-[0-9]{2})$",
    re.MULTILINE,
)
RELEASE_TAG = re.compile(r"^v([0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?)$")


class ChangelogError(Exception):
    """A changelog fragment or release invariant is invalid."""


@dataclass(frozen=True)
class Fragment:
    path: Path
    entries: dict[str, str]


def fragment_paths(repo_root: Path) -> list[Path]:
    directory = repo_root / FRAGMENT_DIRECTORY
    if not directory.is_dir():
        raise ChangelogError(f"missing fragment directory: {directory}")

    invalid = sorted(
        path.name
        for path in directory.iterdir()
        if path.name != "README.md"
        and (not path.is_file() or path.is_symlink() or path.suffix != ".md")
    )
    if invalid:
        raise ChangelogError(
            "changelog fragments must be Markdown files: " + ", ".join(invalid)
        )

    return sorted(path for path in directory.glob("*.md") if path.name != "README.md")


def parse_fragment(path: Path) -> Fragment:
    if not path.stem.isdigit():
        raise ChangelogError(
            f"{path}: fragment name must be a pull request number, for example 123.md"
        )

    text = path.read_text()
    if NO_CHANGELOG_MARKER in text:
        if "## " in text:
            raise ChangelogError(
                f"{path}: a no-changelog fragment cannot also contain entries"
            )
        reason = text.replace(NO_CHANGELOG_MARKER, "")
        reason = re.sub(r"<!--.*?-->", "", reason, flags=re.DOTALL).strip()
        if not reason:
            raise ChangelogError(f"{path}: explain why no changelog entry is required")
        return Fragment(path, {})

    entries: dict[str, str] = {}
    category: str | None = None
    body: list[str] = []

    def store_body() -> None:
        nonlocal body
        if category is None:
            return
        rendered = "\n".join(body).strip()
        if not rendered:
            raise ChangelogError(f"{path}: {category} is empty")
        if not rendered.startswith("- "):
            raise ChangelogError(
                f"{path}: {category} must start with a Markdown list item"
            )
        entries[category] = rendered
        body = []

    for line in text.splitlines():
        category_match = CATEGORY_HEADING.match(line)
        if category_match:
            store_body()
            category = category_match.group(1)
            if category not in STANDARD_CATEGORIES:
                valid = ", ".join(STANDARD_CATEGORIES)
                raise ChangelogError(
                    f"{path}: invalid category {category!r}; expected one of: {valid}"
                )
            if category in entries:
                raise ChangelogError(f"{path}: duplicate {category} section")
        elif line.startswith("#"):
            raise ChangelogError(f"{path}: malformed heading: {line}")
        elif category is not None:
            body.append(line)
        elif line.strip() and not line.lstrip().startswith("<!--"):
            raise ChangelogError(f"{path}: content must be inside a category section")

    store_body()
    if not entries:
        raise ChangelogError(
            f"{path}: add a changelog entry or use {NO_CHANGELOG_MARKER}"
        )
    return Fragment(path, entries)


def load_fragments(repo_root: Path) -> list[Fragment]:
    return [parse_fragment(path) for path in fragment_paths(repo_root)]


def split_unreleased(text: str, path: Path) -> tuple[str, str, str]:
    marker = "## [Unreleased]"
    marker_start = text.find(marker)
    if marker_start < 0:
        raise ChangelogError(f"{path}: missing {marker}")
    marker_end = marker_start + len(marker)
    if text[marker_end : marker_end + 1] not in ("", "\n"):
        raise ChangelogError(f"{path}: malformed {marker} heading")

    next_heading = re.search(r"^## ", text[marker_end + 1 :], re.MULTILINE)
    if next_heading:
        suffix_start = marker_end + 1 + next_heading.start()
    else:
        suffix_start = len(text)

    prefix = text[:marker_end]
    body = text[marker_end:suffix_start].strip()
    suffix = text[suffix_start:].lstrip("\n")
    return prefix, body, suffix


def parse_category_body(body: str, path: Path, section: str) -> dict[str, str]:
    if not body:
        return {}

    categories: dict[str, str] = {}
    category: str | None = None
    lines: list[str] = []

    def store_body() -> None:
        nonlocal lines
        if category is None:
            return
        rendered = "\n".join(lines).strip()
        if not rendered:
            raise ChangelogError(f"{path}: empty {section} / {category} section")
        categories[category] = rendered
        lines = []

    for line in body.splitlines():
        match = re.match(r"^### (.+?)\s*$", line)
        if match:
            store_body()
            category = match.group(1)
            if category not in STANDARD_CATEGORIES:
                raise ChangelogError(f"{path}: invalid {section} category {category!r}")
            if category in categories:
                raise ChangelogError(
                    f"{path}: duplicate {section} / {category} section"
                )
        elif line.startswith("## ") or line.startswith("### "):
            raise ChangelogError(f"{path}: malformed {section} heading: {line}")
        elif category is None:
            if line.strip():
                raise ChangelogError(
                    f"{path}: {section} content must be in category sections"
                )
        else:
            lines.append(line)

    store_body()
    return categories


def parse_unreleased_body(body: str, path: Path) -> dict[str, str]:
    return parse_category_body(body, path, "Unreleased")


def promote_release_candidates(
    suffix: str, stable_version: str, path: Path
) -> tuple[dict[str, str], str]:
    matches = list(VERSION_HEADING.finditer(suffix))
    candidate_sections: list[dict[str, str]] = []
    kept: list[str] = []
    cursor = 0
    candidate_prefix = f"{stable_version}-rc"

    for index, match in enumerate(matches):
        section_end = (
            matches[index + 1].start() if index + 1 < len(matches) else len(suffix)
        )
        version = match.group(1)
        if version.startswith(candidate_prefix) and version != candidate_prefix:
            body = suffix[match.end() : section_end].strip()
            candidate_sections.append(
                parse_category_body(body, path, f"release {version}")
            )
            kept.append(suffix[cursor : match.start()])
            cursor = section_end

    kept.append(suffix[cursor:])

    additions: dict[str, list[str]] = defaultdict(list)
    for categories in reversed(candidate_sections):
        for category, body in categories.items():
            additions[category].append(body)

    promoted = {category: "\n".join(bodies) for category, bodies in additions.items()}
    return promoted, "".join(kept).lstrip("\n")


def render_categories(categories: dict[str, str]) -> str:
    return "\n\n".join(
        f"### {category}\n\n{categories[category]}"
        for category in STANDARD_CATEGORIES
        if category in categories
    )


def merge_entries(
    current: dict[str, str], additions: dict[str, list[str]]
) -> dict[str, str]:
    merged = dict(current)
    for category, bodies in additions.items():
        parts = []
        if category in merged:
            parts.append(merged[category])
        parts.extend(bodies)
        merged[category] = "\n".join(parts)
    return merged


def latest_changelog_version(suffix: str, path: Path) -> str:
    match = VERSION_HEADING.match(suffix)
    if not match:
        raise ChangelogError(f"{path}: missing a released version section")
    return match.group(1)


def render_unreleased(prefix: str, body: str, suffix: str) -> str:
    parts = [prefix, ""]
    if body:
        parts.extend([body, ""])
    if suffix:
        parts.append(suffix.rstrip("\n"))
    return "\n".join(parts) + "\n"


def render_release(
    prefix: str, body: str, suffix: str, version: str, release_date: str
) -> str:
    parts = [prefix, "", f"## [{version}] - {release_date}", "", body]
    if suffix:
        parts.extend(["", suffix.rstrip("\n")])
    return "\n".join(parts) + "\n"


def release_plan(
    repo_root: Path, release_tag: str, release_date: str
) -> tuple[dict[Path, str], list[Path]]:
    tag_match = RELEASE_TAG.match(release_tag)
    if not tag_match:
        raise ChangelogError(
            f"invalid release tag {release_tag!r}; expected v<major>.<minor>.<patch>"
        )
    try:
        parsed_date = date.fromisoformat(release_date)
    except ValueError as error:
        raise ChangelogError(
            f"invalid changelog date {release_date!r}; expected YYYY-MM-DD"
        ) from error
    if parsed_date.isoformat() != release_date:
        raise ChangelogError(
            f"invalid changelog date {release_date!r}; expected YYYY-MM-DD"
        )
    root_version = tag_match.group(1)
    fragments = load_fragments(repo_root)

    path = repo_root / ROOT_CHANGELOG
    original = path.read_text()
    prefix, unreleased, suffix = split_unreleased(original, path)
    current = parse_unreleased_body(unreleased, path)

    promoted: dict[str, str] = {}
    if "-" not in root_version:
        promoted, suffix = promote_release_candidates(suffix, root_version, path)

    additions: dict[str, list[str]] = defaultdict(list)
    for category, body in current.items():
        additions[category].append(body)
    for fragment in fragments:
        for category, body in fragment.entries.items():
            additions[category].append(body)

    merged = merge_entries(promoted, additions)
    body = render_categories(merged)
    latest = latest_changelog_version(suffix, path)

    if latest == root_version:
        if body:
            raise ChangelogError(
                f"{path}: release {root_version} already exists but Unreleased "
                "entries remain; bump the release version first"
            )
        rendered = render_unreleased(prefix, body, suffix)
    else:
        if not body:
            raise ChangelogError(
                f"{path}: release version is {root_version}, latest changelog "
                f"version is {latest}, but Unreleased is empty"
            )
        rendered = render_release(prefix, body, suffix, root_version, release_date)

    writes = {path: rendered} if rendered != original else {}

    return writes, [fragment.path for fragment in fragments]


def run_git(repo_root: Path, arguments: list[str]) -> str:
    result = subprocess.run(
        ["git", *arguments],
        cwd=repo_root,
        check=False,
        text=True,
        capture_output=True,
    )
    if result.returncode:
        raise ChangelogError(result.stderr.strip() or "git command failed")
    return result.stdout


def check_pull_request(
    repo_root: Path,
    base: str,
    head: str,
    pull_request: str,
    release_pr: bool,
    allow_missing: bool,
) -> None:
    load_fragments(repo_root)
    existing = {path.name for path in fragment_paths(repo_root)}
    if release_pr:
        if existing:
            raise ChangelogError(
                "release PRs must consume every fragment; remaining: "
                + ", ".join(sorted(existing))
            )
        return

    changed_paths = run_git(
        repo_root,
        ["diff", "--name-only", base, head],
    ).splitlines()
    changed = [
        path
        for path in changed_paths
        if path.startswith(f"{FRAGMENT_DIRECTORY}/")
        and path != f"{FRAGMENT_DIRECTORY}/README.md"
    ]
    expected = f"{FRAGMENT_DIRECTORY}/{pull_request}.md"
    unexpected = [path for path in changed if path != expected]
    if unexpected:
        raise ChangelogError(
            "each PR owns one fragment; unexpected fragment changes: "
            + ", ".join(unexpected)
        )
    changes_rust = any(
        Path(path).suffix == ".rs" or Path(path).name == "Cargo.toml"
        for path in changed_paths
    )
    if expected not in changed and not allow_missing and changes_rust:
        raise ChangelogError(
            f"add {expected}; use {NO_CHANGELOG_MARKER} for an internal-only PR"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("check", help="validate all pending fragments")

    check_pr = subparsers.add_parser(
        "check-pr", help="validate the fragment owned by a pull request"
    )
    check_pr.add_argument("--base", required=True)
    check_pr.add_argument("--head", required=True)
    check_pr.add_argument("--pr", required=True)
    check_pr.add_argument("--release-pr", action="store_true")
    check_pr.add_argument("--allow-missing", action="store_true")

    release = subparsers.add_parser(
        "release", help="assemble fragments into the versioned changelogs"
    )
    release.add_argument("release_tag")
    release.add_argument("--date", default=date.today().isoformat())
    release.add_argument(
        "--check",
        action="store_true",
        help="fail if release assembly would change tracked files",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parent.parent
    try:
        if args.command == "check":
            fragments = load_fragments(repo_root)
            print(f"validated {len(fragments)} changelog fragment(s)")
        elif args.command == "check-pr":
            check_pull_request(
                repo_root,
                args.base,
                args.head,
                args.pr,
                args.release_pr,
                args.allow_missing,
            )
            print("pull request changelog fragment is valid")
        elif args.command == "release":
            writes, removals = release_plan(repo_root, args.release_tag, args.date)
            if args.check:
                if writes or removals:
                    changed = [str(path.relative_to(repo_root)) for path in writes]
                    changed.extend(
                        str(path.relative_to(repo_root)) for path in removals
                    )
                    raise ChangelogError(
                        "release changelogs are not assembled; run "
                        f"make prepare-release-changelog RELEASE_TAG={args.release_tag}. "
                        "Pending paths: " + ", ".join(changed)
                    )
                print("release changelogs are assembled")
            else:
                for path, rendered in writes.items():
                    path.write_text(rendered)
                for path in removals:
                    path.unlink()
                print(
                    f"updated {len(writes)} changelog(s) and consumed "
                    f"{len(removals)} fragment(s)"
                )
        return 0
    except (ChangelogError, OSError) as error:
        print(f"changelog error: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
