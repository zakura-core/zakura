"""Mainnet accepted-tip work headroom and freshness, using only fleet observations.

The fleet shares the admission floor's failure mode: once every node refuses a
block, accepted tips cannot reveal its target. Frozen tips therefore make this
observation unavailable, never a healthy recovery. Delivery belongs to the
watchdog's existing durable fleet queue.
"""

from __future__ import annotations

import re
from typing import Any

REFERENCE_BITS = 0x1B6AAC57
TARGET_MULTIPLIER = 10
FLOOR_HEIGHT = 3_535_411
MAX_AGE = 600
MAX_POLL_GAP = 180
HOLD_SECONDS = 900
HASH = re.compile(r"[0-9a-fA-F]{64}")
BITS = re.compile(r"[0-9a-fA-F]{8}")


class Unavailable(ValueError):
    """An observation failure whose source establishes when freshness ended."""

    def __init__(self, reason: str, since: float):
        super().__init__(reason)
        self.since = since


def expanded_target(bits: str) -> int:
    """Decode a positive compact target bounded by Mainnet's proof-of-work limit."""
    if not isinstance(bits, str) or not BITS.fullmatch(bits):
        raise ValueError("missing or malformed target")
    compact = int(bits, 16)
    exponent, mantissa = compact >> 24, compact & 0x7FFFFF
    if compact & 0x800000:
        raise ValueError("negative target")
    target = (mantissa >> (8 * (3 - exponent)) if exponent <= 3
              else mantissa << (8 * (exponent - 3)))
    if not 0 < target <= 2**243 - 1:
        raise ValueError("target outside Mainnet limits")
    return target


MAX_TARGET = expanded_target(f"{REFERENCE_BITS:08x}") * TARGET_MULTIPLIER


def fresh(stamp: Any, now: float) -> bool:
    return (type(stamp) in (int, float) and 0 < stamp <= now + 60
            and now - stamp < MAX_AGE)


def observation(snapshot: dict | None, now: float) -> dict:
    """Select the highest healthy tip and require corroborating fleet ancestry.

    Never fall back to a lower tip when the most advanced reporter lacks a
    target or disagrees. A lower node corroborates the selected tip only when
    that tip's sampled ancestors contain the lower node's accepted hash.
    """
    if not isinstance(snapshot, dict) or snapshot.get("network") != "mainnet":
        raise ValueError("Mainnet fleet data unavailable")
    if not fresh(snapshot.get("last_poll"), now):
        stamp = snapshot.get("last_poll")
        if type(stamp) in (int, float) and 0 < stamp <= now:
            raise Unavailable("fleet observation is stale", stamp)
        raise ValueError("fleet observation is stale or missing")
    rows = snapshot.get("rows")
    if not isinstance(rows, list) or not rows or len(rows) > 256:
        raise ValueError("invalid fleet rows")
    names = [row.get("name") for row in rows if isinstance(row, dict)]
    if len(names) != len(rows) or any(not isinstance(name, str) or not name for name in names):
        raise ValueError("invalid fleet node identity")
    if len(set(names)) != len(names):
        raise ValueError("duplicate fleet node identity")
    candidates = [row for row in rows if (row.get("healthy") is True or row.get("health") == "stale")
                  and row.get("rpc_ok") is True and row.get("rpc_chain") == "main"
                  and fresh(row.get("last_seen_at"), now)
                  and type(row.get("height")) is int and FLOOR_HEIGHT < row["height"] <= 0xFFFFFFFF
                  and isinstance(row.get("block_hash"), str) and HASH.fullmatch(row["block_hash"])]
    if not candidates:
        raise ValueError("no fresh healthy Mainnet tip")
    height = max(row["height"] for row in candidates)
    leaders = [row for row in candidates if row["height"] == height]
    hashes = {row["block_hash"].lower() for row in leaders}
    # A tip-only reporter can corroborate the header hash without exposing its
    # target. At least one reporter of that same highest hash must supply it.
    targets = {expanded_target(row["difficulty_bits"]) for row in leaders
               if row.get("difficulty_bits") is not None}
    if not targets:
        raise ValueError("missing or malformed target at highest Mainnet tip")
    if len(hashes) != 1 or len(targets) != 1:
        raise ValueError("highest Mainnet tips disagree")
    selected = leaders[0]
    support = [row["name"] for row in leaders]
    for row in candidates:
        if row["height"] == height:
            continue
        ancestors = selected.get("ancestor_hashes") or {}
        ancestor = ancestors.get(str(height - row["height"])) if isinstance(ancestors, dict) else None
        if ancestor is not None:
            if not isinstance(ancestor, str) or ancestor.lower() != row["block_hash"].lower():
                raise ValueError("Mainnet fleet ancestry disagrees")
            support.append(row["name"])
    if len(support) < 2:
        raise ValueError("highest Mainnet tip lacks a second fleet observation")
    if not all(fresh(row.get("last_advanced_at"), now) for row in leaders):
        stamps = [row.get("last_advanced_at") for row in leaders]
        if all(type(stamp) in (int, float) and 0 < stamp <= now for stamp in stamps):
            raise Unavailable("Mainnet accepted tip has stopped advancing", min(stamps))
        raise ValueError("Mainnet accepted tip has stopped advancing")
    return {"height": height, "hash": selected["block_hash"].lower(),
            "target": targets.pop(), "nodes": sorted(support)}


def update(state: dict, snapshot: dict | None, now: float) -> list[str]:
    """Advance one persisted difficulty incident and a separate availability incident.

    Return transition messages only. Warning/recovery need continuous, fresh
    observations; missing data retains severity and resets both hold timers.
    """
    messages = []
    previous_poll = state.get("last_observed_at")
    if previous_poll is not None and not 0 <= now - previous_poll <= MAX_POLL_GAP:
        state.pop("warning_since", None)
        state.pop("recovery_since", None)
    state["last_observed_at"] = now
    try:
        sample = observation(snapshot, now)
        if sample["height"] > state.get("max_height", -1):
            state["max_height"] = sample["height"]
            state["progress_at"] = now
        if now - state.get("progress_at", now) >= MAX_AGE:
            raise Unavailable("Mainnet accepted tip has stopped advancing", state["progress_at"])
    except ValueError as error:
        state.pop("warning_since", None)
        state.pop("recovery_since", None)
        since = min(state.setdefault("unavailable_since", now),
                    error.since if isinstance(error, Unavailable) else now)
        state["unavailable_since"] = since
        if now - since >= MAX_AGE and not state.get("unavailable_alerting"):
            messages.append(":warning: *Zakura Mainnet difficulty monitoring unavailable*\n"
                            f"reason: {error}\nAccepted fleet tips cannot establish current difficulty. "
                            "Check fleet progress and tip-stall alerts; any difficulty incident remains open.")
            state["unavailable_alerting"] = True
        return messages

    state.pop("unavailable_since", None)
    if state.pop("unavailable_alerting", False):
        messages.append(":white_check_mark: *Zakura Mainnet difficulty observations restored*\n"
                        "Fresh accepted-tip data is available; difficulty recovery is evaluated separately.")
    target = sample["target"]
    state["last_sample"] = sample
    severity = state.get("severity", 0)
    desired = 0
    if target > MAX_TARGET:
        desired = 3
    elif 5 * target > 4 * MAX_TARGET:
        desired = 2
    elif 2 * target > MAX_TARGET:
        since = state.setdefault("warning_since", now)
        if now - since >= HOLD_SECONDS:
            desired = 1
    else:
        state.pop("warning_since", None)

    if 9 * target < 4 * MAX_TARGET:
        since = state.setdefault("recovery_since", now)
        if severity and now - since >= HOLD_SECONDS:
            state["severity"] = 0
            messages.append(":white_check_mark: *Zakura Mainnet difficulty recovered*\n"
                            + sample_text(sample) + "\nHeadroom stayed above 2.25× for 15 minutes.")
    else:
        state.pop("recovery_since", None)

    if desired > severity:
        state["severity"] = desired
        label = {1: "approaching admission floor", 2: "near admission floor",
                 3: "below admission floor"}[desired]
        messages.append(f":rotating_light: *Zakura Mainnet difficulty {label}*\n"
                        + sample_text(sample)
                        + "\nAction: review Mainnet progress and prepare a reviewed admission-policy update. "
                        "The monitor does not change the floor.")
    return messages


def sample_text(sample: dict) -> str:
    """Render bounded node identities, exact target and approximate display headroom."""
    names = ", ".join(re.sub(r"[^A-Za-z0-9_-]", "_", name)[:64] for name in sample["nodes"][:8])
    return (f"Headroom: {MAX_TARGET / sample['target']:.2f}× · block {sample['height']} "
            f"({sample['hash'][:12]})\nFleet nodes: {names}\n"
            f"Target: {sample['target']:064x}")
