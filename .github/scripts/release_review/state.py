"""Construct report state while preserving the checkpoint JSON format."""

from dataclasses import asdict, dataclass, field


@dataclass
class ReviewState:
    """Defaults shared by new reviews and reports for early failures."""

    status: str = "incomplete"
    findings: list[dict] = field(default_factory=list)
    candidates: list[dict] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    summaries: list[dict] = field(default_factory=list)
    coverage: list[dict] = field(default_factory=list)
    synthesis_complete: bool = False


def new_report(context: dict) -> dict:
    """Copy target metadata and initialize independent mutable review state."""
    metadata = {key: value for key, value in context.items() if key != "pr_body"}
    return metadata | asdict(ReviewState())


def update_coverage(coverage: list[dict], completed: set[str]) -> None:
    """Record completed units without changing exclusions or coverage gaps."""
    for entry in coverage:
        if entry["status"] in {"excluded", "out_of_scope"}:
            continue
        entry["reviewed_units"] = sum(unit in completed for unit in entry["units"])
        entry["status"] = "reviewed" if entry["reviewed_units"] == len(entry["units"]) else "pending"
