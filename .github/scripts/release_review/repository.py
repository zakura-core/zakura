"""Read immutable Git objects without checking out or executing reviewed code."""

from __future__ import annotations

import difflib
import json
import os
import re
import selectors
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

from .assets import ASSET_PATHS, asset_text
from .contracts import ReviewError
from .scope import matching_exclusion, safe_path

TAG = re.compile(r"v[0-9]+\.[0-9]+\.[0-9]+(?:-rc[0-9]+)?")
SHA = re.compile(r"[0-9a-f]{40}")
BASE_MARKER = re.compile(r"<!-- release-review-base: (.*?) -->")
MAX_BLOB = 2_000_000


@dataclass(frozen=True)
class ReviewUnit:
    id: str
    path: str
    part: int
    patch: str


class Repository:
    def __init__(self, root: Path):
        self.root = root
        self.refs: dict[str, str] = {}
        self.trees: dict[str, dict[str, tuple[str, str]]] = {}

    def git(self, *args: str, allowed: tuple[int, ...] = (0,),
            max_bytes: int = 8_000_000, truncate: bool = False) -> bytes:
        """Run a fixed Git read command with hooks, pagers, and external diffs disabled."""
        env = {key: value for key, value in os.environ.items()
               if key in {"PATH", "HOME", "SYSTEMROOT", "TMPDIR"}}
        env.update(GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull,
                   GIT_TERMINAL_PROMPT="0", GIT_PAGER="cat", LC_ALL="C")
        try:
            with subprocess.Popen(
                ["git", "--literal-pathspecs", "-c", "core.hooksPath=/dev/null",
                 "-c", "core.fsmonitor=false", "-c", "core.pager=cat", *args],
                cwd=self.root, env=env, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            ) as process, selectors.DefaultSelector() as selector:
                selector.register(process.stdout, selectors.EVENT_READ)
                output = bytearray()
                deadline = time.monotonic() + 60
                try:
                    while True:
                        remaining = deadline - time.monotonic()
                        if remaining <= 0 or not selector.select(remaining):
                            raise ReviewError("Git read timed out")
                        chunk = os.read(process.stdout.fileno(), 65536)
                        if not chunk:
                            break
                        output.extend(chunk)
                        if len(output) > max_bytes:
                            if truncate:
                                return bytes(output[:max_bytes])
                            raise ReviewError("Git output exceeds the bounded repository read limit")
                    if process.wait(timeout=max(0.01, deadline - time.monotonic())) not in allowed:
                        raise ReviewError("Git could not read the requested revision or object")
                    return bytes(output)
                finally:
                    if process.poll() is None:
                        process.kill()
                    process.wait()
        except (OSError, subprocess.TimeoutExpired):
            raise ReviewError("Git read failed or timed out") from None

    def resolve(self, ref: str) -> str:
        result = self.git("rev-parse", "--verify", "--end-of-options", ref + "^{commit}").decode().strip()
        if not SHA.fullmatch(result):
            raise ReviewError("Git did not resolve an exact commit")
        return result

    def pin(self, head: str, body: str, override: str = "") -> dict:
        if not SHA.fullmatch(head):
            raise ReviewError("review head must be a full commit SHA")
        expected_base = None
        markers = BASE_MARKER.findall(body)
        if len(markers) > 1 and not override:
            raise ReviewError("release PR has multiple baseline markers")
        if override:
            tag, origin = override, "manual override"
        elif markers:
            try:
                marker = json.loads(markers[0])
                tag, expected_base = marker["tag"], marker["sha"]
                if not SHA.fullmatch(expected_base):
                    raise ValueError
            except (ValueError, KeyError, TypeError):
                raise ReviewError("release PR has an invalid baseline marker") from None
            origin = "release preparation metadata"
        else:
            # Exclude legacy upstream/valar tag formats. Git chooses by ancestry,
            # never by version sorting across unrelated historical release lines.
            tags = [tag for tag in self.git("tag", "--merged", head).decode().splitlines()
                    if TAG.fullmatch(tag)]
            if not tags:
                raise ReviewError("no reachable release tag; provide a base-tag override")
            matches = [argument for tag in tags for argument in ("--match", tag)]
            tag = self.git("describe", "--tags", "--abbrev=0", *matches, head).decode().strip()
            origin = "nearest reachable release tag"
        if not isinstance(tag, str) or not TAG.fullmatch(tag):
            raise ReviewError("baseline must be a vX.Y.Z or vX.Y.Z-rcN release tag")
        baseline = self.resolve("refs/tags/" + tag)
        if expected_base and baseline != expected_base:
            raise ReviewError("baseline tag no longer matches the release preparation commit")
        ancestor = self.git("merge-base", baseline, head).decode().strip()
        if ancestor != baseline:
            raise ReviewError("baseline is not an ancestor of the release head")
        self.refs = {"base": baseline, "head": self.resolve(head)}
        self.trees = {name: self.tree(sha) for name, sha in self.refs.items()}
        return {"base_tag": tag, "base_sha": baseline, "head_sha": head, "baseline_source": origin}

    def tree(self, sha: str) -> dict[str, tuple[str, str]]:
        files = {}
        for record in self.git("ls-tree", "-r", "-z", sha).split(b"\0"):
            if record:
                meta, path = record.split(b"\t", 1)
                mode, _, oid = meta.decode().split()
                files[path.decode("utf-8", errors="replace")] = (mode, oid)
        return files

    def blob(self, revision: str, path: str) -> bytes:
        """Read a bounded regular Git blob; never dereference a working-tree path."""
        if revision not in self.refs or not safe_path(path):
            raise ReviewError("invalid repository revision or path")
        entry = self.trees[revision].get(path)
        if entry is None or entry[0] not in {"100644", "100755"}:
            raise ReviewError("requested path is not a regular tracked file at this revision")
        size = int(self.git("cat-file", "-s", entry[1]))
        if size > MAX_BLOB:
            raise ReviewError("requested source exceeds the two-megabyte read limit")
        return self.git("cat-file", "blob", entry[1])

    def source(self, revision: str, path: str) -> str:
        blob = self.blob(revision, path)
        if b"\0" in blob:
            raise ReviewError("requested source is binary")
        try:
            return blob.decode("utf-8")
        except UnicodeError:
            raise ReviewError("requested source is not UTF-8 text") from None

    def review_text(self, revision: str, path: str) -> str:
        """Expose known embedded formats losslessly while other sources remain UTF-8."""
        if path in ASSET_PATHS:
            return asset_text(path, self.blob(revision, path))
        return self.source(revision, path)

    def read(self, revision: str, path: str, start: int, end: int) -> dict:
        if type(start) is not int or type(end) is not int or start < 1 or end < start or end - start >= 200:
            raise ReviewError("source reads must request between one and 200 lines")
        lines = self.review_text(revision, path).splitlines()
        if start > len(lines):
            raise ReviewError("requested source line does not exist")
        selected = "\n".join(f"{index + 1}: {line}" for index, line in enumerate(lines)
                             if start <= index + 1 <= end)
        return {"revision": revision, "path": path, "text": selected[:20000],
                "representation": "structured binary" if path in ASSET_PATHS else "UTF-8 source",
                "truncated": len(selected) > 20000 or end > len(lines)}

    def search(self, revision: str, query: str, path: str = "") -> dict:
        if revision not in self.refs or not query.strip() or len(query) > 200:
            raise ReviewError("invalid repository search")
        if path and (not safe_path(path) or path not in self.trees[revision]):
            raise ReviewError("search path must be a tracked file, or empty for the whole revision")
        args = ["grep", "-n", "-I", "-F", "-m", "10", "-e", query, self.refs[revision], "--"]
        if path:
            args.append(path)
        result = self.git(*args, allowed=(0, 1), max_bytes=80001, truncate=True).decode("utf-8", errors="replace")
        return {"revision": revision, "matches": result[:20000], "truncated": len(result) > 20000}

    def units(self, limit: int, excluded_paths: tuple[str, ...] = ()) -> tuple[list[ReviewUnit], list[dict]]:
        paths = self.git("diff", "--name-only", "-z", "--no-renames", self.refs["base"], self.refs["head"])
        units, coverage = [], []
        for raw in paths.split(b"\0"):
            if not raw:
                continue
            path = raw.decode("utf-8", errors="replace")
            entry = {"path": path, "units": [], "status": "pending", "reason": ""}
            coverage.append(entry)
            try:
                if not safe_path(path):
                    raise ReviewError("path cannot be represented safely")
                rule = matching_exclusion(path, excluded_paths)
                if rule:
                    entry.update(status="out_of_scope", reason="excluded by configuration",
                                 exclusion_rule=rule)
                    continue
                sources = {revision: self.review_text(revision, path) if path in self.trees[revision] else ""
                           for revision in ("base", "head")}
                if path in ASSET_PATHS:
                    entry["representation"] = "structured binary"
                    patch = "".join(difflib.unified_diff(
                        sources["base"].splitlines(keepends=True), sources["head"].splitlines(keepends=True),
                        fromfile="base/" + path, tofile="head/" + path, n=12))
                else:
                    patch = self.git("diff", "--no-ext-diff", "--no-textconv", "--no-renames",
                                     "--unified=12", self.refs["base"], self.refs["head"], "--", path).decode("utf-8")
                if len(patch.encode()) > 8_000_000:
                    raise ReviewError("file diff exceeds the eight-megabyte review limit")
                chunks, current, size = [], [], 0
                for line in patch.splitlines(keepends=True):
                    line_size = len(line.encode())
                    if line_size > limit:
                        raise ReviewError("a single diff line exceeds the review-unit limit")
                    if current and size + line_size > limit:
                        chunks.append("".join(current))
                        current, size = [], 0
                    current.append(line)
                    size += line_size
                if current:
                    chunks.append("".join(current))
                for part, chunk in enumerate(chunks, 1):
                    unit = ReviewUnit(f"unit-{len(units) + 1}", path, part, chunk)
                    units.append(unit)
                    entry["units"].append(unit.id)
                if not chunks:
                    entry.update(status="excluded", reason="no textual patch")
            except (ReviewError, UnicodeError) as error:
                entry.update(status="excluded", reason=str(error))
        return units, coverage

    def validate_finding(self, finding: dict, changed_paths: set[str]) -> None:
        if finding["change_path"] not in changed_paths:
            raise ReviewError("finding does not identify a changed release file")
        ref = finding["reference"]
        if ref["end"] < ref["start"] or ref["end"] - ref["start"] > 40:
            raise ReviewError("finding source range is invalid or too broad")
        lines = self.source(ref["revision"], ref["path"]).splitlines()
        if ref["end"] > len(lines):
            raise ReviewError("finding cites a nonexistent source line")
        excerpt = "\n".join(lines[ref["start"] - 1:ref["end"]])
        if ref["quote"].strip() not in excerpt:
            raise ReviewError("finding quotation does not match the pinned source")
