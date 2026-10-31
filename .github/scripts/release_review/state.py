"""Own mutable review progress and serialize it at checkpoint boundaries."""

from dataclasses import asdict, dataclass, field

from .contracts import Finding, ModelTurn
from .repository import ReviewUnit


@dataclass
class ReviewState:
    """Track evidence and coverage independently of target metadata and storage."""

    status: str = "incomplete"
    findings: list[Finding] = field(default_factory=list)
    candidates: list[Finding] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    summaries: list[dict] = field(default_factory=list)
    coverage: list[dict] = field(default_factory=list)
    synthesis_complete: bool = False
    synthesis_steps: list[dict] = field(default_factory=list)
    resume_history: list[dict] = field(default_factory=list)
    completed: set[str] = field(default_factory=set)

    def record_batch(self, batch: list[ReviewUnit], turn: ModelTurn) -> None:
        """Record verified evidence and mark every unit in the batch complete."""
        self.candidates.extend(turn.findings)
        self.summaries.append({
            "units": [unit.id for unit in batch],
            "paths": list(dict.fromkeys(unit.path for unit in batch)),
            "summary": turn.summary,
        })
        self.completed.update(unit.id for unit in batch)
        self.update_coverage()

    def update_coverage(self) -> None:
        """Recompute coverage after a completed batch or checkpoint restoration."""
        for entry in self.coverage:
            if entry["status"] in {"excluded", "out_of_scope"}:
                continue
            entry["reviewed_units"] = sum(unit in self.completed for unit in entry["units"])
            entry["status"] = "reviewed" if entry["reviewed_units"] == len(entry["units"]) else "pending"

    def snapshot(self) -> dict:
        """Serialize independent evidence using the existing report JSON fields."""
        result = asdict(self)
        del result["completed"]
        for optional in ("synthesis_steps", "resume_history"):
            if not result[optional]:
                del result[optional]
        return result


def new_report(context: dict) -> dict:
    """Copy target metadata and initialize independent mutable review state."""
    metadata = {key: value for key, value in context.items() if key != "pr_body"}
    return metadata | ReviewState().snapshot()
