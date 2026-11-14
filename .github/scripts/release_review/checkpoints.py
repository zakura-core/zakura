"""Reuse completed review work while retaining the original spending ledger."""

from __future__ import annotations

import hashlib
import json
from decimal import Decimal, InvalidOperation

from .contracts import FINDING_SCHEMA, Finding, ReviewError, validate_schema
from .models import Spending
from .state import ReviewState


def contract_digest(repo, settings, units, coverage) -> str:
    """Bind reusable results to revisions, rendered diffs, scope, prompts, and pricing.

    Wall-time limits may change for a continuation; evidence, provider,
    effort, request limits, and the shared API ceiling must remain identical.
    """
    identity = {
        "revisions": repo.refs, "provider": settings.provider, "model": settings.model,
        "endpoint": settings.endpoint, "reasoning_effort": settings.reasoning_effort,
        "prompt_digest": settings.prompt_digest, "excluded_paths": settings.excluded_paths,
        "input_price": str(settings.input_price), "output_price": str(settings.output_price),
        "cached_input_price": str(settings.cached_input_price),
        "budget": str(settings.budget), "max_input_bytes": settings.max_input_bytes,
        "max_output_tokens": settings.max_output_tokens, "unit_bytes": settings.unit_bytes,
        "units": [(unit.id, unit.path, unit.part, hashlib.sha256(unit.patch.encode()).hexdigest()) for unit in units],
        "coverage": coverage,
    }
    return hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()


def validate_checkpoint_target(prior: dict, report: dict) -> None:
    """Reject evidence from another release target or a finished review."""
    if any(prior.get(key) != report.get(key) for key in ("repository", "pr_number", "head_sha", "base_sha")):
        raise ReviewError("checkpoint belongs to a different release target")
    if prior.get("resume_contract_digest") != report["resume_contract_digest"]:
        raise ReviewError("checkpoint revisions, scope, prompts, model, pricing, or diff units changed")
    if prior.get("status") != "incomplete":
        raise ReviewError("only an incomplete review needs continuation")


def restore_spending(usage: dict, settings) -> Spending:
    """Reconstruct the ledger without discarding known charges or uncertain calls."""
    try:
        charged = Decimal(usage["estimated_usd_including_uncertain_calls"])
        budget = Decimal(usage["budget_usd"])
    except (KeyError, InvalidOperation, TypeError, ValueError):
        raise ReviewError("checkpoint has invalid billing information") from None
    counts = {key: usage.get(key) for key in ("input_tokens", "output_tokens", "calls", "uncertain_calls")}
    counts["cached_input_tokens"] = usage.get("cached_input_tokens", 0)
    if (not charged.is_finite() or not 0 <= charged <= settings.budget or budget != settings.budget
            or any(type(value) is not int or value < 0 for value in counts.values())
            or counts["uncertain_calls"] > counts["calls"]
            or counts["cached_input_tokens"] > counts["input_tokens"]):
        raise ReviewError("checkpoint has invalid billing information")
    spending = Spending(settings, charged=charged, **counts)
    if spending.price(counts["input_tokens"], counts["output_tokens"], counts["cached_input_tokens"]) > charged:
        raise ReviewError("checkpoint billing omits known token usage")
    return spending


def restore_completed_units(summaries: list[dict], units) -> set[str]:
    """Validate each saved batch against the current unit IDs and paths."""
    known = {unit.id: unit.path for unit in units}
    completed = set()
    for summary in summaries:
        batch = summary.get("units") if isinstance(summary, dict) else None
        # Each check guards the next: batch entries are known strings before path lookup.
        if (not isinstance(batch, list) or not batch or not isinstance(summary.get("summary"), str)
                or any(not isinstance(unit, str) or unit not in known or unit in completed for unit in batch)
                or len(set(batch)) != len(batch)
                or summary.get("paths") != list(dict.fromkeys(known[unit] for unit in batch))):
            raise ReviewError("checkpoint contains invalid or duplicated completed units")
        completed.update(batch)
    return completed


def restore(prior: dict, report: dict, state: ReviewState, repo, settings, units,
            *, changed: set[str]) -> Spending:
    """Restore validated progress and spending under the original review contract."""
    validate_checkpoint_target(prior, report)
    usage = prior.get("usage", {})
    spending = restore_spending(usage, settings)
    summaries = prior.get("summaries", [])
    candidates = prior.get("candidates", [])
    history = prior.get("resume_history", [])
    if not isinstance(summaries, list) or not isinstance(candidates, list):
        raise ReviewError("checkpoint evidence must contain summary and candidate lists")
    if not isinstance(history, list):
        raise ReviewError("checkpoint continuation history must be a list")
    completed = restore_completed_units(summaries, units)
    findings = []
    for value in candidates:
        validate_schema(value, FINDING_SCHEMA)
        finding = Finding.from_validated(value)
        repo.validate_finding(finding, changed)
        findings.append(finding)
    # Review never mutates the prior report, so restored state can share its entries.
    state.summaries = list(summaries)
    state.candidates = findings
    state.completed = completed
    state.update_coverage()
    state.resume_history = [*history, {
        "control_sha": prior.get("control_sha"), "errors": prior.get("errors", []),
        "duration_seconds": prior.get("duration_seconds"), "usage": usage,
        "completed_units": len(completed),
        "checkpoint_sha256": hashlib.sha256(json.dumps(prior, sort_keys=True).encode()).hexdigest(),
    }]
    return spending
