"""Deployment alert suppression markers.

Two markers exist and they are deliberately independent:

- the fleet marker on us-east-0 (``/run/zakura-fleet-watchdog/...``), written by
  every restart deploy, defers fleet-lane failure alerts;
- the compatibility marker on zakura-compat
  (``/run/zakura-watchdog/deployment-suppressed-until``), written only by
  deploys that restart ``zakurad-compat``. Only it suppresses the
  compatibility lane, so an unrelated fleet deploy cannot hide a broken
  compatibility node.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path


COMPAT_SUPPRESSION_FILE = Path("/run/zakura-watchdog/deployment-suppressed-until")
COMPAT_MAX_SUPPRESSION_SECONDS = 1200
MAX_MARKER_BYTES = 64

# Marker classifications, reported as compatibility outcome metadata.
MISSING = "missing"
ACTIVE = "active"
EXPIRED = "expired"
MALFORMED = "malformed"
EXCESSIVE = "excessive"
UNREADABLE = "unreadable"
SUPPRESSION_STATES = frozenset({MISSING, ACTIVE, EXPIRED, MALFORMED, EXCESSIVE, UNREADABLE})


def suppression_until(path: Path) -> float | None:
    """Return the fleet marker's timestamp; the fleet lane compares it to now."""
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return None
    except OSError as error:
        print(f"warning: could not read suppression file {path}: {error}", file=sys.stderr)
        return None

    try:
        return float(raw)
    except ValueError:
        print(f"warning: invalid suppression timestamp in {path}: {raw}", file=sys.stderr)
        return None


@dataclass(frozen=True)
class CompatSuppression:
    """Classified compatibility marker. Only ``active`` suppresses transitions."""

    state: str
    until: int | None
    max_seconds: int

    @property
    def active(self) -> bool:
        return self.state == ACTIVE

    def as_dict(self) -> dict[str, object]:
        return {
            "state": self.state,
            "active": self.active,
            "until": self.until,
            "max_seconds": self.max_seconds,
        }


def compat_suppression(path: Path, now: float, max_seconds: int) -> CompatSuppression:
    """Classify the compatibility marker like the retired Rust watchdog did.

    The marker holds a Unix timestamp in whole seconds. It suppresses only while
    it is in the future and at most ``max_seconds`` ahead; a malformed or
    excessive marker never suppresses, so a stuck marker cannot mute the lane.
    """
    try:
        with path.open("rb") as marker:
            raw = marker.read(MAX_MARKER_BYTES + 1)
    except FileNotFoundError:
        return CompatSuppression(MISSING, None, max_seconds)
    except OSError:
        return CompatSuppression(UNREADABLE, None, max_seconds)

    try:
        text = raw.decode("ascii").strip()
    except UnicodeDecodeError:
        text = ""
    if len(raw) > MAX_MARKER_BYTES or not text.isdigit():
        return CompatSuppression(MALFORMED, None, max_seconds)

    until = int(text)
    if until > int(now) + max_seconds:
        return CompatSuppression(EXCESSIVE, until, max_seconds)
    if until > now:
        return CompatSuppression(ACTIVE, until, max_seconds)
    return CompatSuppression(EXPIRED, until, max_seconds)
