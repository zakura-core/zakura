"""Validate external spentness artifacts and render reviewed release descriptors."""

from __future__ import annotations

import hashlib
import json
import struct
from pathlib import Path

ARTIFACT = "mainnet-spentness-hints.bin"
COMMITMENT = "mainnet-spentness-hints.commitment.json"
VERIFICATION = "mainnet-spentness-hints.verification.json"
MANIFEST = Path(
    "crates/zakura-chain/src/parameters/spentness_hints/mainnet-manifest.json"
)
COMPILED = MANIFEST.with_name("commitments.rs")
MAX_BYTES = 512 * 1024 * 1024
HEADER = struct.Struct("<8sH32sI32sQ")
FIELDS = {
    "chain_identity",
    "terminal_height",
    "terminal_block_hash",
    "format_version",
    "output_count",
    "byte_len",
    "sha256",
}


def validate_commitment(pin: dict) -> None:
    if not isinstance(pin, dict) or set(pin) != FIELDS:
        raise ValueError("spentness commitment has unexpected fields")
    for field in ("chain_identity", "terminal_block_hash", "sha256"):
        value = pin[field]
        if (
            not isinstance(value, list)
            or len(value) != 32
            or any(type(n) is not int or not 0 <= n <= 255 for n in value)
        ):
            raise ValueError(f"spentness {field} must contain 32 bytes")
    for field, maximum in (
        ("terminal_height", 2**32 - 1),
        ("output_count", 2**64 - 1),
        ("byte_len", MAX_BYTES),
        ("format_version", 1),
    ):
        if type(pin[field]) is not int or not 0 <= pin[field] <= maximum:
            raise ValueError(f"invalid spentness {field}")
    if (
        pin["format_version"] != 1
        or pin["byte_len"] != HEADER.size + (pin["output_count"] + 7) // 8
    ):
        raise ValueError("spentness version, count, and length disagree")


def validate_bundle(bundle: Path, meta: dict) -> dict:
    """Verify all hint fields before a bundle can select a compiled descriptor."""
    pin = json.loads((bundle / COMMITMENT).read_text())
    validate_commitment(pin)
    if (
        pin["terminal_height"] != meta["height"]
        or bytes(pin["terminal_block_hash"])[::-1].hex() != meta["block_hash"]
    ):
        raise ValueError(
            "spentness and release bundle have different terminal checkpoints"
        )
    checkpoints = (bundle / "main-checkpoints.txt").read_text().splitlines()
    if (
        not checkpoints
        or checkpoints[0] != "0 " + bytes(pin["chain_identity"])[::-1].hex()
    ):
        raise ValueError("spentness chain identity differs from genesis checkpoint")
    if checkpoints[-1] != f"{meta['height']} {meta['block_hash']}":
        raise ValueError("spentness boundary differs from terminal checkpoint")
    report = json.loads((bundle / VERIFICATION).read_text())
    if (
        not isinstance(report, dict)
        or type(report.get("schema_version")) is not int
        or report.get("schema_version") != 1
        or report.get("commitment") != pin
        or report.get("oracle") != "transparent-replay-v1"
        or report.get("complete_entries") is not True
        or report.get("salted_multiset") is not True
    ):
        raise ValueError("spentness verification evidence is missing or mismatched")
    digest = hashlib.sha256()
    survivors = 0
    size = 0
    with (bundle / ARTIFACT).open("rb") as source:
        header = source.read(HEADER.size)
        if len(header) != HEADER.size:
            raise ValueError("truncated spentness header")
        magic, version, identity, height, block_hash, count = HEADER.unpack(header)
        if (magic, version, identity, height, block_hash, count) != (
            b"ZKSHINT\0",
            1,
            bytes(pin["chain_identity"]),
            pin["terminal_height"],
            bytes(pin["terminal_block_hash"]),
            pin["output_count"],
        ):
            raise ValueError("spentness header differs from commitment")
        digest.update(header)
        size += len(header)
        last = 0
        first = True
        while chunk := source.read(1024 * 1024):
            size += len(chunk)
            if size > pin["byte_len"]:
                raise ValueError("spentness artifact exceeds committed length")
            if first and chunk[0] & 1:
                raise ValueError("spentness artifact retains genesis")
            first = False
            last = chunk[-1]
            digest.update(chunk)
            survivors += sum(byte.bit_count() for byte in chunk)
    if size != pin["byte_len"] or list(digest.digest()) != pin["sha256"]:
        raise ValueError("spentness artifact digest or length mismatch")
    if count % 8 and last >> (count % 8):
        raise ValueError("spentness padding must be zero")
    if (
        type(report.get("survivor_count")) is not int
        or report["survivor_count"] != survivors
    ):
        raise ValueError("spentness survivor count differs from verification evidence")
    evidence = meta.get("spentness")
    if not isinstance(evidence, dict) or evidence.get("verification") != report:
        raise ValueError("bundle lacks spentness verification provenance")
    revision = evidence.get("generator_revision", "")
    if (
        not isinstance(revision, str)
        or len(revision) != 40
        or any(c not in "0123456789abcdef" for c in revision)
    ):
        raise ValueError("spentness generator revision is invalid")
    if (
        not isinstance(evidence.get("independent_source"), str)
        or not evidence["independent_source"]
    ):
        raise ValueError(
            "spentness bundle must identify its independently synchronized source"
        )
    if evidence.get("reproduced_sha256") != bytes(pin["sha256"]).hex():
        raise ValueError("independent source did not reproduce the spentness artifact")
    return {"commitment": pin, "provenance": evidence}


def render_commitments(manifest: dict) -> str:
    if (
        not isinstance(manifest, dict)
        or manifest.get("schema_version") != 1
        or not isinstance(manifest.get("artifacts"), list)
    ):
        raise ValueError("unsupported spentness release manifest")
    rows = [
        "//! Release-reviewed Mainnet descriptors. The release-state importer retains old pins.",
        "",
        "use super::Commitment;",
        "",
        "/// Recognized Mainnet artifacts, oldest first.",
        "#[rustfmt::skip]",
        "pub const MAINNET_COMMITMENTS: &[Commitment] = &[",
    ]
    for entry in manifest["artifacts"]:
        pin = entry["commitment"]
        validate_commitment(pin)
        rows.append("    Commitment {")
        for name in (
            "chain_identity",
            "terminal_height",
            "terminal_block_hash",
            "format_version",
            "output_count",
            "byte_len",
            "sha256",
        ):
            value = pin[name]
            # Keep the generated byte order visible in review.
            if isinstance(value, list):
                rows.append(f"        {name}: [")
                rows.extend(f"            {byte}," for byte in value)
                rows.append("        ],")
            else:
                rows.append(f"        {name}: {value},")
        rows.append("    },")
    rows.append("];")
    return "\n".join(rows) + "\n"


def prepare_import(repo: Path, bundle: Path, meta: dict) -> tuple[dict, str]:
    entry = validate_bundle(bundle, meta)
    path = repo / MANIFEST
    manifest = (
        json.loads(path.read_text())
        if path.exists()
        else {"schema_version": 1, "artifacts": []}
    )
    if manifest.get("schema_version") != 1 or not isinstance(
        manifest.get("artifacts"), list
    ):
        raise ValueError("unsupported spentness release manifest")
    for old in manifest["artifacts"]:
        if (
            old["commitment"]["terminal_height"]
            >= entry["commitment"]["terminal_height"]
        ):
            raise ValueError("spentness release descriptors must advance in height")
    manifest["artifacts"].append(entry)
    return manifest, render_commitments(manifest)
