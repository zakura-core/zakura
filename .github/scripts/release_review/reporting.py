"""Render evidence without treating model text as Markdown, HTML, or commands."""

from __future__ import annotations

import html
import json
import re
from pathlib import Path
from urllib.parse import quote


def plain(value: str) -> str:
    value = html.escape(str(value)).replace("@", "&#64;")
    return re.sub(r"([\\`*_{}\[\]()#+.!|>~-])", r"\\\1", value)


def render(report: dict, *, comment: bool = False) -> str:
    complete = report.get("status") == "complete"
    coverage = report.get("coverage", [])
    excluded = [item for item in coverage if item["status"] == "out_of_scope"]
    verdict = "complete within configured scope" if excluded else "complete"
    lines = ["## Release AI review — " + (verdict if complete else "INCOMPLETE"), "",
             "Advisory, read-only review. No source code or tests were executed.", "",
             f"Range: `{report.get('base_tag', 'unresolved')}` (`{report.get('base_sha', 'unresolved')}`) → `{report['head_sha']}`.",
             (f"Reviewer revision: `{report.get('control_sha', 'unknown')}`. "
              f"Provider/model: {plain(report.get('provider', 'unconfigured'))} / {plain(report.get('model', 'unconfigured'))}. "
              f"Reasoning effort: {plain(report.get('reasoning_effort', 'unavailable'))}."),
             f"Prompt digest: `{report.get('prompt_digest', 'unavailable')}`.", ""]
    reviewed = sum(item["status"] == "reviewed" for item in coverage)
    lines.append(f"Coverage: {reviewed}/{len(coverage) - len(excluded)} in-scope changed files reviewed "
                 f"({len(coverage)} total changed files; {len(excluded)} excluded by configuration); "
                 f"cross-component synthesis {'completed' if report.get('synthesis_complete') else 'not completed'}.")
    if not comment and report.get("excluded_paths"):
        lines.append("Configured exclusions: " + ", ".join(plain(path) for path in report["excluded_paths"]) + ".")
    if excluded:
        lines.append("Excluded changes were not reviewed. Completion applies only to the configured scope.")
    if any(entry.get("representation") == "structured binary" for entry in coverage):
        lines.append("Embedded state assets are reviewed through lossless field/byte diffs. "
                     "Framing and the subtree checksum are checked; chain provenance and cryptographic roots are not authenticated.")
    usage = report.get("usage", {})
    lines.append(f"Estimated API cost, including uncertain requests: ${usage.get('estimated_usd_including_uncertain_calls', '0')} "
                 f"of ${usage.get('budget_usd', '25')}; {usage.get('calls', 0)} requests.")
    if report.get("run_url"):
        lines.extend(["", f"[Workflow run and full report artifacts]({report['run_url']})"])
    if not complete:
        lines.extend(["", "**This is not a clean review. Coverage or verification is unfinished.**"])
    for error in report.get("errors", []):
        lines.append("- " + plain(error))
    findings = report.get("findings", [])
    counts = {level: sum(f["severity"] == level for f in findings) for level in ("Critical", "High", "Medium")}
    lines.extend(["", "### Supported findings", "", " · ".join(f"{key}: {value}" for key, value in counts.items()), ""])
    if not findings:
        lines.append("No supported findings reported." if complete else "No final finding verdict is available for the unfinished review.")
    for index, finding in enumerate(findings, 1):
        ref = finding["reference"]
        sha = report["base_sha"] if ref["revision"] == "base" else report["head_sha"]
        url = f"https://github.com/{report['repository']}/blob/{sha}/{quote(ref['path'], safe='/')}#L{ref['start']}-L{ref['end']}"
        lines.extend([f"### {index}. {finding['severity']}: {plain(finding['title'])}", "",
                      f"[{plain(ref['path'])}:{ref['start']}]({url}) · {finding['category']} · supported by source evidence", ""])
        for label, key in (("Trigger", "trigger"), ("Impact", "impact"), ("Evidence", "explanation"),
                           ("Connection to this release", "release_connection"), ("Suggested verification", "verification")):
            lines.append(f"**{label}:** {plain(finding[key])}")
        lines.append("")
    if excluded and not comment:
        lines.extend(["", "### Configured exclusions", ""])
        for entry in excluded:
            lines.append(f"- {plain(entry['path'])}: matched {plain(entry['exclusion_rule'])}")
    missing = [entry for entry in coverage if entry["status"] not in {"reviewed", "out_of_scope"}]
    if missing:
        lines.extend(["", "### Coverage gaps", ""])
        for entry in missing[:20] if comment else missing:
            lines.append(f"- {plain(entry['path'])}: {plain(entry['reason'] or entry['status'])}")
        if comment and len(missing) > 20:
            lines.append(f"- {len(missing) - 20} additional gaps are listed in the report artifact.")
    if report.get("candidates") and not report.get("synthesis_complete"):
        lines.extend(["", f"{len(report['candidates'])} preliminary candidates remain unadjudicated in the JSON artifact."])
    text = "\n".join(lines) + "\n"
    if comment and len(text) > 55000:
        text = text[:50000] + "\n\n**Comment abbreviated. See the full workflow report artifact.**\n"
    return text


def write_report(directory: Path, report: dict) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    # Atomic checkpoints survive a cancelled model request or exhausted job budget.
    for name, content in (("report.json", json.dumps(report, indent=2)),
                          ("report.md", render(report)), ("comment.md", render(report, comment=True))):
        temporary = directory / (name + ".tmp")
        temporary.write_text(content)
        temporary.replace(directory / name)
