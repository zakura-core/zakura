"""Durable JSON state shared by every watchdog alert lane.

Each lane owns its own top-level namespaces. Loading keeps unknown top-level
fields, so a newer or older watchdog revision never drops another lane's
incidents or pending deliveries.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


STATE_VERSION = 1


def load_state(state_path: Path) -> dict[str, Any]:
    if not state_path.exists():
        return {
            "version": STATE_VERSION,
            "nodes": {},
            "fleets": {},
            "shared_stalls": {},
        }

    with state_path.open(encoding="utf-8") as state_file:
        state = json.load(state_file)

    if not isinstance(state, dict) or state.get("version") != STATE_VERSION:
        return {
            "version": STATE_VERSION,
            "nodes": {},
            "fleets": {},
            "shared_stalls": {},
        }

    state.setdefault("nodes", {})
    state.setdefault("fleets", {})
    state.setdefault("shared_stalls", {})
    state.setdefault("release_state", {})
    return state


def save_state(state_path: Path, state: dict[str, Any]) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = state_path.with_suffix(f"{state_path.suffix}.tmp")
    with tmp_path.open("w", encoding="utf-8") as state_file:
        json.dump(state, state_file, indent=2, sort_keys=True)
        state_file.write("\n")
    tmp_path.replace(state_path)
