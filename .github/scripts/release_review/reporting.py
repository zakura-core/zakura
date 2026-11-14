"""Render evidence without treating model text as Markdown, HTML, or commands."""

from __future__ import annotations

import html
import json
import math
import re
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.parse import quote

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


def display_values(report: dict) -> dict:
    """Compute the values both report layouts share once."""
    coverage = report.get("coverage", [])
    excluded = [entry for entry in coverage if entry["status"] == "out_of_scope"]
    return {
        "complete": report.get("status") == "complete",
        "coverage": coverage,
        "excluded": excluded,
        "included": len(coverage) - len(excluded),
        "reviewed": sum(entry["status"] == "reviewed" for entry in coverage),
        "missing": [entry for entry in coverage if entry["status"] not in {"reviewed", "out_of_scope"}],
        "usage": report.get("usage", {}),
        "findings": report.get("findings", []),
    }


def report_intro(report: dict, values: dict) -> list[str]:
    """Keep diagnostic provenance and billing detail in the full report."""
    complete, excluded, usage = values["complete"], values["excluded"], values["usage"]
    verdict = "complete within configured scope" if excluded else "complete"
    lines = ["## Release AI review — " + (verdict if complete else "INCOMPLETE"), "",
             "Advisory, read-only review. No source code or tests were executed.", "",
             f"Range: `{report.get('base_tag', 'unresolved')}` (`{report.get('base_sha', 'unresolved')}`) → `{report['head_sha']}`.",
             (f"Reviewer revision: `{report.get('control_sha', 'unknown')}`. "
              f"Provider/model: {plain(report.get('provider', 'unconfigured'))} / {plain(report.get('model', 'unconfigured'))}. "
              f"Reasoning effort: {plain(report.get('reasoning_effort', 'unavailable'))}."),
             f"Prompt digest: `{report.get('prompt_digest', 'unavailable')}`.", "",
             (f"Coverage: {values['reviewed']}/{values['included']} in-scope changed files reviewed "
              f"({len(values['coverage'])} total changed files; {len(excluded)} excluded by configuration); "
              f"cross-component synthesis {'completed' if report.get('synthesis_complete') else 'not completed'}.")]
    if report.get("excluded_paths"):
        lines.append("Configured exclusions: " + ", ".join(plain(path) for path in report["excluded_paths"]) + ".")
    if excluded:
        lines.append("Excluded changes were not reviewed. Completion applies only to the configured scope.")
    if any(entry.get("representation") == "structured binary" for entry in values["coverage"]):
        lines.append("Embedded state assets are reviewed through lossless field/byte diffs. "
                     "Framing and the subtree checksum are checked; chain provenance and cryptographic roots are not authenticated.")
    lines.append(f"Estimated API cost, including uncertain requests: ${usage.get('estimated_usd_including_uncertain_calls', '0')} "
                 f"of ${usage.get('budget_usd', '25')}; {usage.get('calls', 0)} requests.")
    if report.get("resume_history"):
        lines.append(f"Continued from {len(report['resume_history'])} saved attempt(s); "
                     "coverage and usage include those attempts under the same API ceiling.")
    if usage.get("cached_input_tokens"):
        lines.append(f"Provider-reported cached input: {usage['cached_input_tokens']} tokens. "
                     "Any configured cache discount applies only after successful usage reporting.")
    if report.get("run_url"):
        lines.extend(["", f"[Workflow run and full report artifacts]({report['run_url']})"])
    if not complete:
        lines.extend(["", "**This is not a clean review. Coverage or verification is unfinished.**"])
    lines.extend("- " + plain(error) for error in report.get("errors", []))
    return lines


def comment_intro(report: dict, values: dict) -> list[str]:
    """Lead the PR summary with the verdict and the evidence needed to assess it."""
    complete, usage = values["complete"], values["usage"]
    lines = ["## Release AI review — " + ("Complete" if complete else "INCOMPLETE"), ""]
    if not complete:
        lines.append("**This is not a clean review. Coverage or final checks are unfinished.**")
    elif values["findings"]:
        lines.append("**Potential release-blocking bugs found. Review the findings below.**")
    else:
        lines.append("**No supported release-blocking bugs found in the reviewed scope.**")
    cost = money(usage.get("estimated_usd_including_uncertain_calls"))
    model = plain(report.get("model", "Not configured"))
    effort = plain(str(report.get("reasoning_effort", "unavailable")).capitalize())
    lines.extend(["", "| Review | Result |", "| --- | --- |",
                  (f"| Coverage | **{values['reviewed']}/{values['included']} files reviewed**; "
                   f"{len(values['excluded'])} excluded by configuration |"),
                  f"| Cross-component checks | {'Complete' if report.get('synthesis_complete') else 'Not completed'} |",
                  f"| Model | {model} · {effort} reasoning |",
                  f"| Estimated API cost | {cost} / {money(usage.get('budget_usd', '25'))} |"])
    durations = [report.get("duration_seconds"),
                 *(entry.get("duration_seconds") for entry in report.get("resume_history", []))]
    known = [value for value in durations if type(value) in (int, float) and math.isfinite(value) and value >= 0]
    if known:
        lines.append(f"| Review time | {sum(known) / 60:.1f} minutes |")
    base = revision_link(report, "base_sha", report.get("base_tag", "Baseline"))
    head = revision_link(report, "head_sha", report["head_sha"][:8])
    lines.extend(["", f"**Release range:** {base} → {head}."])
    if report.get("repository") and report.get("base_sha"):
        url = (f"https://github.com/{report['repository']}/compare/"
               f"{quote(report['base_sha'], safe='')}...{quote(report['head_sha'], safe='')}")
        lines.append(f"[View release diff]({url})")
    if report.get("run_url"):
        lines.extend(["", f"[Workflow run and full report]({report['run_url']})"])
    if report.get("errors"):
        lines.extend(["", "### Why the review is incomplete", ""])
        lines.extend("- " + plain(error) for error in report["errors"])
    return lines


def finding_lines(report: dict, index: int, finding: dict, *, comment: bool) -> list[str]:
    """Link one finding to the exact reviewed source and escape its model-authored text."""
    ref = finding["reference"]
    sha = report["base_sha"] if ref["revision"] == "base" else report["head_sha"]
    url = f"https://github.com/{report['repository']}/blob/{sha}/{quote(ref['path'], safe='/')}#L{ref['start']}-L{ref['end']}"
    lines = [f"### {index}. {finding['severity']}: {plain(finding['title'])}", "",
             f"[{plain(ref['path'])}:{ref['start']}]({url}) · {finding['category']} · supported by source evidence", ""]
    for label, key in (("Trigger", "trigger"), ("Impact", "impact"), ("Evidence", "explanation"),
                       ("Connection to this release", "release_connection"), ("Suggested verification", "verification")):
        lines.append(("- " if comment else "") + f"**{label}:** {plain(finding[key])}")
    return [*lines, ""]


def findings_section(report: dict, values: dict, *, comment: bool) -> list[str]:
    """List findings, exclusions, and coverage gaps, which both layouts share."""
    complete, findings, missing = values["complete"], values["findings"], values["missing"]
    lines = ["", "### Findings" if comment else "### Supported findings", "",
             " · ".join(f"{level}: {sum(f['severity'] == level for f in findings)}" for level in SEVERITIES), ""]
    if not findings and not comment:
        lines.append("No supported findings reported." if complete else "No final finding verdict is available for the unfinished review.")
    if not findings and comment and not complete:
        lines.append("Final findings are not available until the review finishes.")
    for index, finding in enumerate(findings, 1):
        lines.extend(finding_lines(report, index, finding, comment=comment))
    if values["excluded"] and not comment:
        lines.extend(["", "### Configured exclusions", ""])
        lines.extend(f"- {plain(entry['path'])}: matched {plain(entry['exclusion_rule'])}" for entry in values["excluded"])
    if missing:
        label = "file" if len(missing) == 1 else "files"
        lines.extend(["", f"<details><summary>Coverage gaps ({len(missing)} {label})</summary>" if comment
                      else "### Coverage gaps", ""])
        lines.extend(f"- {plain(entry['path'])}: {plain(entry['reason'] or entry['status'])}"
                     for entry in (missing[:20] if comment else missing))
        if comment and len(missing) > 20:
            lines.append(f"- {len(missing) - 20} additional gaps are listed in the report artifact.")
        if comment:
            lines.extend(["", "</details>"])
    if report.get("candidates") and not report.get("synthesis_complete"):
        lines.extend(["", f"{len(report['candidates'])} preliminary candidates remain unadjudicated in the JSON artifact."])
    return lines


def render(report: dict, *, comment: bool = False) -> str:
    """Render the PR comment or the full report, escaping all model-authored text."""
    values = display_values(report)
    lines = comment_intro(report, values) if comment else report_intro(report, values)
    lines += findings_section(report, values, comment=comment)
    if comment:
        lines.extend(["", ("Advisory source review of consensus, cryptography, mining, and block propagation. "
                           "No builds or tests were run. Excluded changes were not reviewed.")])
    text = "\n".join(lines) + "\n"
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
