"""Allowlisted file handoff from the fleet watchdog to the public dashboard."""
import json
import os
from pathlib import Path
import re
import tempfile

PUBLIC_STATUS = Path("/var/lib/zakura-mac-cranelift-public/status.json")


def public_status(status, identifier):
    if not isinstance(status, dict):
        raise ValueError("malformed status")
    if not re.fullmatch(r"verifier-[a-f0-9]{32}", identifier):
        raise ValueError("invalid opaque identifier")
    result = {"verifier_id": identifier, "schema_version": 1}
    # Never forward receipts, diagnostic strings, OS metadata, peer IDs or hosts.
    for key in ("sample_time", "coverage_start", "compared_through"):
        value = status.get(key)
        result[key] = value if type(value) in (int, float) else None
    for key in ("caught_up",):
        result[key] = status.get(key) is True
    for key, sample in (("mac_tip", status.get("verifier")), ("linux_tip", status.get("reference"))):
        sample = sample if isinstance(sample, dict) else {}
        tip = sample.get("tip", {}) if key == "mac_tip" else sample
        value = tip.get("height") if isinstance(tip, dict) else None
        result[key] = value if type(value) is int and value >= 0 else None
    sample = status.get("verifier")
    sample = sample if isinstance(sample, dict) else {}
    tip = sample.get("tip") or {}
    value = tip.get("hash") if isinstance(tip, dict) else None
    result["mac_tip_hash"] = value if isinstance(value, str) and re.fullmatch(r"[a-fA-F0-9]{64}", value) else ""
    receipt = sample.get("receipt") or {}
    value = receipt.get("source_sha") if isinstance(receipt, dict) else None
    result["source_sha"] = value if isinstance(value, str) and re.fullmatch(r"[a-f0-9]{40}", value) else ""
    resources = sample.get("resources") or {}
    for key in ("node_rss_bytes", "free_disk_bytes"):
        value = resources.get(key) if isinstance(resources, dict) else None
        result[key] = value if type(value) is int and value >= 0 else None
    anchor = sample.get("fork_anchor")
    if (isinstance(anchor, dict) and type(anchor.get("height")) is int and anchor["height"] >= 0
            and isinstance(anchor.get("hash"), str) and re.fullmatch(r"[a-fA-F0-9]{64}", anchor["hash"])):
        result["fork_anchor"] = {"height": anchor["height"], "hash": anchor["hash"]}
    result["active_incidents"] = len(status.get("incidents", {}))
    result["comparison_healthy"] = (status.get("caught_up") is True
        and status.get("error") is None
        and not status.get("incidents", {}))
    result["alerts_muted"] = status.get("alerts_muted", True) is True
    condition = status.get("condition")
    if condition in {"matching", "catching_up", "unavailable", "chain_disagreement", "tree_mismatch", "coverage_gap"}:
        result["condition"] = condition
    return result


def publish_status(status, identity_path, destination=PUBLIC_STATUS):
    identity = json.loads(Path(identity_path).read_text())
    payload = public_status(status, identity["verifier_id"])
    destination = Path(destination)
    # The deployment manager creates this dedicated directory; never expose the
    # private comparison directory to the dashboard account.
    fd, temporary = tempfile.mkstemp(dir=destination.parent, prefix=".status-")
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(payload, stream, allow_nan=False)
            stream.flush()
            os.fsync(stream.fileno())
            os.fchmod(stream.fileno(), 0o644)
        os.replace(temporary, destination)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)
