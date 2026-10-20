"""Public NU7 dashboard availability and observation freshness."""

from __future__ import annotations

from dataclasses import dataclass
import math
from pathlib import Path
import tomllib
from typing import Any


@dataclass(frozen=True)
class Target:
    name: str
    url: str
    dashboard_url: str


def load_targets(config_path: Path) -> list[Target]:
    with config_path.open("rb") as config_file:
        raw_targets = tomllib.load(config_file).get("nu7_dashboard", [])
    targets = []
    names = set()
    for raw in raw_targets:
        if set(raw) != {"name", "url", "dashboard_url"}:
            raise SystemExit("NU7 dashboard requires name, url and dashboard_url only")
        if any(not isinstance(value, str) or not value for value in raw.values()):
            raise SystemExit("NU7 dashboard fields must be non-empty strings")
        if raw["name"] in names:
            raise SystemExit("duplicate NU7 dashboard name")
        if not all(raw[key].startswith("https://") for key in ("url", "dashboard_url")):
            raise SystemExit("NU7 dashboard URLs must use HTTPS")
        names.add(raw["name"])
        targets.append(Target(**raw))
    return targets


def failure(snapshot: dict[str, Any], now: float, max_age: float) -> tuple[str, float] | None:
    """Return a safe failure reason and its start, or None for fresh usable data.

    Stale observations anchor the incident to the oldest source timestamp, so
    an already frozen endpoint alerts on the first poll. Other failures begin
    at detection; the caller preserves a continuous incident across reasons.
    """
    if type(snapshot.get("schemaVersion")) is not int or snapshot["schemaVersion"] != 1:
        return "invalid dashboard schema", now
    status = snapshot.get("status")
    if (not isinstance(status, dict)
            or type(status.get("schemaVersion")) is not int
            or status["schemaVersion"] != 1):
        return "invalid observation schema", now
    if status.get("status") not in ("live", "degraded", "unavailable"):
        return "unrecognized observation status", now
    timestamps = (snapshot.get("generatedAt"), status.get("observedAt"))
    if any(type(value) not in (int, float) or value <= 0
           or (type(value) is float and not math.isfinite(value))
           for value in timestamps):
        return "invalid observation timestamps", now
    if max(timestamps) > now + 60:
        return "observation timestamp is more than 60s in the future", now
    oldest = min(timestamps)
    age = now - oldest
    if age >= max_age:
        return f"dashboard data is stale (oldest observation is {int(age)}s old)", oldest
    if status["status"] == "unavailable":
        return "dashboard reports unavailable observations", now
    return None
