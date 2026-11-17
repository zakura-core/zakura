"""Read-only, lossless text views of the two embedded mainnet state formats.

These views expose every encoded field or tree byte for diff review. Framing and
the subtree checksum are checked, but chain provenance and cryptographic roots
are not authenticated here. The reviewed Rust code is never loaded or executed.
"""

from __future__ import annotations

import hashlib
import struct

from .contracts import ReviewError

ASSET_DIRECTORY = "crates/zakura-state/src/service/finalized_state/vct/"
FRONTIER_PATH = ASSET_DIRECTORY + "mainnet-frontier.bin"
SUBTREES_PATH = ASSET_DIRECTORY + "mainnet-subtrees.bin"
ASSET_PATHS = {FRONTIER_PATH, SUBTREES_PATH}


def frontier_lines(blob: bytes) -> list[str]:
    """Decode the height and three or four length-prefixed tree byte strings."""
    if len(blob) < 4:
        raise ReviewError("final-frontier artifact has no complete height")
    lines = [f"height_u32_le={int.from_bytes(blob[:4], 'little')}"]
    offset = 4
    for pool in ("sapling", "orchard", "sprout", "ironwood"):
        if pool == "ironwood" and offset == len(blob):
            lines.append("ironwood=absent (legacy three-tree format)")
            break
        if offset + 4 > len(blob):
            raise ReviewError("final-frontier artifact is missing a tree length")
        size = int.from_bytes(blob[offset:offset + 4], "little")
        offset += 4
        if offset + size > len(blob):
            raise ReviewError("final-frontier artifact contains a truncated tree")
        lines.append(f"{pool}.length_u32_le={size}")
        tree = blob[offset:offset + size]
        for start in range(0, size, 32):
            lines.append(f"{pool}.bytes[{start}:{min(start + 32, size)}]={tree[start:start + 32].hex()}")
        offset += size
    if offset != len(blob):
        raise ReviewError("final-frontier artifact contains trailing bytes")
    return lines


def subtree_lines(blob: bytes) -> list[str]:
    """Decode the versioned frame and every indexed, height-stamped subtree root."""
    if len(blob) < 59 or blob[:8] != b"ZKVCTST1":
        raise ReviewError("subtree artifact has a truncated or unknown header")
    version, network, height, *counts = struct.unpack_from("<HBIIII", blob, 8)
    if version != 1 or network not in {1, 2}:
        raise ReviewError("subtree artifact version or network is unsupported")
    if any(count > 65536 for count in counts) or len(blob) != 59 + sum(counts) * 38:
        raise ReviewError("subtree artifact record counts do not match its bounded payload")
    digest = hashlib.sha256(blob[:27] + blob[59:]).digest()
    if digest != blob[27:59]:
        raise ReviewError("subtree artifact checksum does not match its header and payload")
    lines = ["magic=ZKVCTST1", f"version_u16_le={version}", f"network_u8={network}",
             f"last_checkpoint_u32_le={height}", f"checksum_sha256={digest.hex()}"]
    offset = 59
    for pool, count in zip(("sapling", "orchard", "ironwood"), counts):
        lines.append(f"{pool}.count_u32_le={count}")
        for number in range(count):
            index, end_height = struct.unpack_from("<HI", blob, offset)
            root = blob[offset + 6:offset + 38]
            lines.append(f"{pool}.record[{number}] index_u16_le={index} end_height_u32_le={end_height} root={root.hex()}")
            offset += 38
    return lines


def asset_text(path: str, blob: bytes) -> str:
    """Return a complete view or fail closed on an unsupported/malformed format."""
    if path == FRONTIER_PATH:
        lines = frontier_lines(blob)
    elif path == SUBTREES_PATH:
        lines = subtree_lines(blob)
    else:
        raise ReviewError("no trusted read-only representation for this binary asset")
    return "\n".join([
        "Embedded state artifact: lossless field/byte representation, not Rust source lines.",
        "Framing checked; chain provenance, decoded tree validity, and root authentication are not proven.",
        "Cite the consuming Rust source or manifest for findings; use this asset as change_path.",
        f"size_bytes={len(blob)} sha256={hashlib.sha256(blob).hexdigest()}", *lines, "",
    ])
