"""Review every diff unit, then challenge candidates across component boundaries."""

from __future__ import annotations

import json
import time
from dataclasses import asdict

from .config import Settings
from .contracts import ModelProvider, ModelRequest, ReviewError, parse_turn
from .models import Spending
from .repository import Repository


def inspect_unit(repo: Repository, provider: ModelProvider, settings: Settings,
                 spending: Spending, content: dict, deadline: float, checkpoint) -> dict:
    messages = [
        {"role": "system", "content": settings.instructions},
        {"role": "user", "content": json.dumps(content, ensure_ascii=False)},
    ]
    for _ in range(settings.max_rounds):
        if time.monotonic() >= deadline:
            raise ReviewError("review time budget exhausted")
        reply = spending.call(provider, ModelRequest(messages, settings.max_output_tokens),
                              deadline=deadline, checkpoint=checkpoint)
        turn = parse_turn(reply.text)
        if turn["complete"]:
            if len(turn["findings"]) == 12:
                raise ReviewError("model reached the finding limit; additional findings may be missing")
            return turn
        messages.append({"role": "assistant", "content": reply.text})
        results = []
        for action in turn["actions"]:
            try:
                if action["kind"] == "read":
                    result = repo.read(action["revision"], action["path"], action["start"], action["end"])
                else:
                    result = repo.search(action["revision"], action["query"], action["path"])
            except ReviewError as error:
                result = {"error": str(error)}
            results.append({"request": action, "result": result})
        messages.append({"role": "user", "content": json.dumps({"repository_tool_results": results})})
    raise ReviewError("model did not finish within the configured context-request rounds")


def deduplicate(findings: list[dict]) -> list[dict]:
    """Collapse identical source/risk reports; retain distinct bugs at the same location."""
    result, seen = [], set()
    for finding in findings:
        key = json.dumps([finding["category"], finding["reference"], finding["trigger"]], sort_keys=True)
        if key not in seen:
            result.append(finding)
            seen.add(key)
    order = {"Critical": 0, "High": 1, "Medium": 2}
    return sorted(result, key=lambda finding: (order[finding["severity"]], finding["reference"]["path"]))


def synthesize(repo: Repository, provider: ModelProvider, settings: Settings,
               spending: Spending, report: dict, deadline: float, changed: set[str], checkpoint) -> list[dict]:
    """Reduce bounded groups before a final pass across every change's summary."""
    nodes = [{"summary": summary} for summary in report["summaries"]]
    nodes.extend({"candidate": finding} for finding in deduplicate(report["candidates"]))

    def content(group):
        return {
            "task": "Challenge candidates and inspect interactions across these release changes. "
                    "Reject nits, speculative risks, duplicate root causes, and unchanged baseline bugs. "
                    "Summarize the changed behavior and remaining interactions for the next synthesis pass. "
                    "Retain every substantiated bug; do not claim to have run tests.",
            "revisions": repo.refs, "review_results": group,
        }

    def fits(group):
        request = ModelRequest([
            {"role": "system", "content": settings.instructions},
            {"role": "user", "content": json.dumps(content(group), ensure_ascii=False)},
        ], settings.max_output_tokens)
        # Leave room for subsequent source requests and their results. Spending.call
        # still checks the full request before every actual provider call.
        return spending.input_bound(request) <= settings.max_input_bytes - min(20000, settings.max_input_bytes // 4)

    for level in range(8):
        groups, current = [], []
        for node in nodes:
            if current and not fits([*current, node]):
                groups.append(current)
                current = []
            current.append(node)
            if not fits(current):
                raise ReviewError("a synthesis item exceeds the configured model input allowance")
        if current:
            groups.append(current)
        if len(groups) > 1 and len(groups) >= len(nodes):
            raise ReviewError("synthesis cannot reduce the evidence within the configured input allowance")
        reduced = []
        for index, group in enumerate(groups):
            turn = inspect_unit(repo, provider, settings, spending, content(group), deadline, checkpoint)
            for finding in turn["findings"]:
                repo.validate_finding(finding, changed)
            report.setdefault("synthesis_steps", []).append({"level": level, "group": index, **turn})
            checkpoint()
            if len(groups) == 1:
                return deduplicate(turn["findings"])
            reduced.append({"summary": turn["summary"], "candidates": turn["findings"]})
        nodes = reduced
    raise ReviewError("synthesis did not converge within the bounded review passes")


def review(repo: Repository, provider: ModelProvider, settings: Settings,
           report: dict, checkpoint) -> dict:
    """Checkpoint coverage and candidates after each unit, including incomplete runs."""
    spending = Spending(settings)
    report.update(status="incomplete", findings=[], candidates=[], errors=[], summaries=[],
                  provider=settings.provider, model=settings.model, prompt_digest=settings.prompt_digest,
                  usage=spending.report(), synthesis_complete=False)
    deadline = time.monotonic() + settings.max_seconds
    units, coverage = repo.units(settings.unit_bytes)
    report["coverage"] = coverage
    changed = {entry["path"] for entry in coverage}
    completed = set()

    def save():
        for entry in coverage:
            if entry["status"] != "excluded":
                entry["status"] = "reviewed" if all(unit in completed for unit in entry["units"]) else "pending"
                entry["reviewed_units"] = sum(unit in completed for unit in entry["units"])
        report["usage"] = spending.report()
        checkpoint(report)

    save()
    try:
        for unit in units:
            turn = inspect_unit(repo, provider, settings, spending, {
                "task": "Review this release-diff unit; use repository reads to check context before reporting a bug.",
                "revisions": repo.refs, "unit": asdict(unit),
                "note": "This can be a continuation of a file diff. Line citations must be verified against source reads.",
            }, deadline, save)
            for finding in turn["findings"]:
                repo.validate_finding(finding, changed)
            report["candidates"].extend(turn["findings"])
            report["summaries"].append({"unit": unit.id, "path": unit.path, "summary": turn["summary"]})
            completed.add(unit.id)
            save()
        if units:
            report["findings"] = synthesize(repo, provider, settings, spending, report, deadline, changed, save)
        report["synthesis_complete"] = True
        report["status"] = "complete" if all(item["status"] == "reviewed" for item in coverage) else "incomplete"
    except ReviewError as error:
        report["errors"].append(str(error))
    finally:
        save()
    return report
