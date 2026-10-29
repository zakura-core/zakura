"""Render evidence without treating model text as Markdown, HTML, or commands."""

from __future__ import annotations

import html
import json
import math
import re
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.parse import quote

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from .contracts import SEVERITIES


def plain(value: str) -> str:
    value = html.escape(str(value)).replace("@", "&#64;")
    return re.sub(r"([\\`*_{}\[\]()#+.!|>~-])", r"\\\1", value)


def money(value) -> str:
    """Round known estimates for display without changing the stored ledger."""
    try:
        amount = Decimal(str(value))
        return f"${amount:.2f}" if amount.is_finite() and amount >= 0 else "Unavailable"
    except InvalidOperation:
        return "Unavailable"


def revision_link(report: dict, key: str, label: str) -> str:
    """Show a readable label while linking to the exact reviewed commit."""
    sha = report.get(key)
    if not sha or not report.get("repository"):
        return plain(label)
    url = f"https://github.com/{report['repository']}/commit/{quote(sha, safe='')}"
    return f"[{plain(label)}]({url})"


def template_context(report: dict) -> dict:
    """Compute shared display values once for both report layouts."""
    coverage = report.get("coverage", [])
    excluded = [entry for entry in coverage if entry["status"] == "out_of_scope"]
    durations = [report.get("duration_seconds"),
                 *(entry.get("duration_seconds") for entry in report.get("resume_history", []))]
    known = [value for value in durations if type(value) in (int, float) and math.isfinite(value) and value >= 0]
    diff_url = None
    if report.get("repository") and report.get("base_sha"):
        diff_url = (f"https://github.com/{report['repository']}/compare/"
                    f"{quote(report['base_sha'], safe='')}...{quote(report['head_sha'], safe='')}")
    findings = []
    for finding in report.get("findings", []):
        ref = finding["reference"]
        sha = report["base_sha"] if ref["revision"] == "base" else report["head_sha"]
        findings.append(finding | {"url": f"https://github.com/{report['repository']}/blob/{sha}/"
                                          f"{quote(ref['path'], safe='/')}#L{ref['start']}-L{ref['end']}"})
    return {
        "report": report,
        "complete": report.get("status") == "complete",
        "coverage": coverage,
        "excluded": excluded,
        "included": len(coverage) - len(excluded),
        "reviewed": sum(entry["status"] == "reviewed" for entry in coverage),
        "missing": [entry for entry in coverage if entry["status"] not in {"reviewed", "out_of_scope"}],
        "structured_binary": any(entry.get("representation") == "structured binary" for entry in coverage),
        "verdict": "complete within configured scope" if excluded else "complete",
        "usage": report.get("usage", {}),
        "effort": str(report.get("reasoning_effort", "unavailable")).capitalize(),
        "duration_minutes": sum(known) / 60 if known else None,
        "base_link": revision_link(report, "base_sha", report.get("base_tag", "Baseline")),
        "head_link": revision_link(report, "head_sha", report["head_sha"][:8]),
        "diff_url": diff_url,
        "findings": findings,
        "counts": " · ".join(f"{level}: {sum(f['severity'] == level for f in findings)}" for level in SEVERITIES),
    }


TEMPLATES = Environment(
    loader=FileSystemLoader(Path(__file__).with_name("templates")),
    undefined=StrictUndefined,
    autoescape=False,  # Markdown uses plain(), not HTML autoescaping.
    trim_blocks=True,
    lstrip_blocks=True,
    keep_trailing_newline=True,
)
TEMPLATES.filters.update(plain=plain, money=money)


def render(report: dict, *, comment: bool = False) -> str:
    """Render trusted templates with explicit escaping for model-authored text."""
    template = TEMPLATES.get_template("comment.md.jinja" if comment else "report.md.jinja")
    text = template.render(**template_context(report), comment=comment)
    if comment and len(text) > 55000:
        text = text[:50000] + "\n\n**Comment abbreviated. See the full workflow report artifact.**\n"
    return text


def write_atomic(path: Path, content: str) -> None:
    """Replace a report file only after its complete contents reach the temporary file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(content)
    temporary.replace(path)


def write_checkpoint(directory: Path, report: dict) -> None:
    """Persist recovery state before requests and after completed review steps."""
    write_atomic(directory / "report.json", json.dumps(report, indent=2))


def write_report(directory: Path, report: dict) -> None:
    """Save recovery state and render documents at CLI and publication boundaries."""
    write_checkpoint(directory, report)
    write_atomic(directory / "report.md", render(report))
    write_atomic(directory / "comment.md", render(report, comment=True))
