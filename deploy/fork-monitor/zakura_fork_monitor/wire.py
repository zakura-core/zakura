"""Zcash legacy P2P message framing and payload codecs (pure, no I/O).

Decoders treat every payload as hostile. Counts are checked against protocol caps
and the remaining length before any loop runs, and every malformed or oversized
input raises `WireError` (a `ValueError`). Hashes cross this API as display hex,
except inventory entries, which stay as raw wire-order bytes.

Notes:
- `parse_frame_header` takes an optional `max_payload` so callers can apply the
  1 KiB pre-handshake limit.
- `encode_getdata_blocks` rejects more than 16 hashes. Zakura and Zebra serve at
  most 16 per request and drop the rest with no `notfound`.
- `decode_version` treats a missing trailing relay byte as True, like pre-BIP37 peers.
"""

from __future__ import annotations

import ipaddress
import re
import struct
from dataclasses import dataclass

from .consensus import (
    BLOCK_HEADER_LEN,
    BlockHeader,
    ParseError,
    parse_header,
    raw_hash_key,
    read_compact_size,
    sha256d,
    write_compact_size,
)


PROTOCOL_VERSION = 170190
# Testnet peers below this are rejected by Zakura since NU6.3.
MIN_PEER_VERSION = 170160
USER_AGENT = "/zakura-fork-monitor:0.1.0/"
HEADER_LEN = 24
MAX_PAYLOAD_PRE_HANDSHAKE = 1024
MAX_PAYLOAD = 2 * 1024 * 1024

INV_ERROR, INV_TX, INV_BLOCK, INV_FILTERED_BLOCK, INV_WTX = 0, 1, 2, 3, 5
# MSG_WTX entries carry txid || auth digest; every other known type carries one 32-byte hash.
_INV_DIGEST_LEN = {INV_ERROR: 32, INV_TX: 32, INV_BLOCK: 32, INV_FILTERED_BLOCK: 32, INV_WTX: 64}

NODE_NETWORK = 1
# Zakura's v2-stack bit. Never advertise it: when both sides set it, Zakura starts
# a QUIC upgrade. It does identify Zakura v2 peers.
NODE_P2P_V2 = 1 << 24

# Protocol caps. Zakura disconnects peers that exceed them, so we reject the same inputs.
MAX_INV_ITEMS = 50_000
MAX_HEADERS = 160
MAX_LOCATOR_HASHES = 101
MAX_ADDRS = 1000
MAX_USER_AGENT_LEN = 256
# Zakura and Zebra serve at most 16 blocks per getdata and silently drop the rest.
MAX_GETDATA_BLOCKS = 16
# BIP155 upper bound on one addrv2 address.
MAX_ADDRV2_ADDR_LEN = 512
# addrv2 network ids a stdlib TCP client can dial, with their required address lengths.
_ADDRV2_IP_LENGTHS = {1: 4, 2: 16}

_ADDR_ENTRY_LEN = 30  # u32 time, u64 services, 16-byte IPv6, u16 big-endian port
_VERSION_FIXED_LEN = 80  # version, services, timestamp, two 26-byte net addrs, nonce
_IPV4_MAPPED_PREFIX = bytes(10) + b"\xff\xff"

_USER_AGENT_RE = re.compile(r"/([^:/()]{1,64}):([^/()]{0,64})")
_USER_AGENT_IMPLS = {
    "zakura": "zakura",
    "zebra": "zebra",
    "magicbean": "zcashd",
    "zeeder": "zeeder",
    "zakura-fork-monitor": "monitor",
    "fork-monitor-probe": "monitor",
}
_VERSION_JUNK_RE = re.compile(r"[^0-9A-Za-z.+_-]")
MAX_UA_VERSION_LEN = 32


class WireError(ValueError):
    """Raised for malformed, oversized or out-of-protocol P2P data."""


@dataclass(slots=True)
class VersionMsg:
    """The fields of a `version` message that the monitor sends or records."""

    version: int
    services: int
    timestamp: int
    recv_ip: str
    recv_port: int
    nonce: int
    user_agent: str
    start_height: int
    relay: bool


def frame(magic: bytes, command: str, payload: bytes) -> bytes:
    """Wrap `payload` in the 24-byte header: magic, command, length, checksum."""
    if len(magic) != 4:
        raise WireError("network magic must be 4 bytes")
    if len(payload) > MAX_PAYLOAD:
        raise WireError(f"payload of {len(payload)} bytes exceeds {MAX_PAYLOAD}")
    name = _ascii(command, "command")
    if len(name) > 12 or not _is_command_name(name):
        raise WireError(f"invalid command {command!r}")
    return magic + name.ljust(12, b"\0") + struct.pack("<I", len(payload)) + sha256d(payload)[:4] + payload


def parse_frame_header(
    magic: bytes, hdr: bytes, max_payload: int = MAX_PAYLOAD
) -> tuple[str, int, bytes]:
    """Validate a 24-byte message header; return (command, payload length, checksum)."""
    if len(hdr) != HEADER_LEN:
        raise WireError(f"message header must be {HEADER_LEN} bytes")
    if hdr[:4] != magic:
        raise WireError(f"bad network magic {bytes(hdr[:4]).hex()}")
    name, _, padding = bytes(hdr[4:16]).partition(b"\0")
    if not _is_command_name(name) or any(padding):
        raise WireError(f"malformed command field {bytes(hdr[4:16]).hex()}")
    (length,) = struct.unpack_from("<I", hdr, 16)
    if length > max_payload:
        raise WireError(f"{name.decode()} payload of {length} bytes exceeds {max_payload}")
    return name.decode("ascii"), length, bytes(hdr[20:24])


def _is_command_name(name: bytes) -> bool:
    """Return True for a non-empty run of printable, non-space ASCII."""
    return bool(name) and all(0x21 <= c <= 0x7E for c in name)


def verify_checksum(payload: bytes, checksum: bytes) -> bool:
    """Return True if `checksum` is the first 4 bytes of sha256d(payload)."""
    return sha256d(payload)[:4] == bytes(checksum)


def encode_version(v: VersionMsg) -> bytes:
    """Serialize a `version` payload (86 bytes plus the user agent)."""
    user_agent = _ascii(v.user_agent, "user agent")
    if len(user_agent) > MAX_USER_AGENT_LEN:
        raise WireError(f"user agent longer than {MAX_USER_AGENT_LEN} bytes")
    return b"".join(
        (
            _pack("<IQq", v.version, v.services, v.timestamp),
            _pack("<Q", NODE_NETWORK) + _encode_ip(v.recv_ip) + _pack(">H", v.recv_port),
            _pack("<Q", 0) + _encode_ip("0.0.0.0") + _pack(">H", 0),
            _pack("<Q", v.nonce),
            write_compact_size(len(user_agent)),
            user_agent,
            _pack("<I", v.start_height),
            b"\x01" if v.relay else b"\x00",
        )
    )


def decode_version(p: bytes) -> VersionMsg:
    """Parse a peer's `version` payload; WireError on truncation, long UA or bad relay byte."""
    if len(p) < _VERSION_FIXED_LEN:
        raise WireError("truncated version message")
    version, services, timestamp = struct.unpack_from("<IQq", p, 0)
    recv_ip = _decode_ip(p[28:44])
    (recv_port,) = struct.unpack_from(">H", p, 44)
    (nonce,) = struct.unpack_from("<Q", p, 72)
    ua_len, off = _read_count(p, _VERSION_FIXED_LEN, MAX_USER_AGENT_LEN, "user agent")
    if off + ua_len > len(p):
        raise WireError("truncated user agent")
    user_agent = bytes(p[off : off + ua_len]).decode("utf-8", errors="replace")
    ((start_height,), off) = _unpack("<I", p, off + ua_len)
    relay = True
    if off < len(p):
        if p[off] not in (0, 1):
            raise WireError(f"invalid relay byte {p[off]}")
        relay = p[off] == 1
    return VersionMsg(
        version=version,
        services=services,
        timestamp=timestamp,
        recv_ip=recv_ip,
        recv_port=recv_port,
        nonce=nonce,
        user_agent=user_agent,
        start_height=start_height,
        relay=relay,
    )


def encode_ping(nonce: int) -> bytes:
    """Serialize a `ping` or `pong` payload."""
    return _pack("<Q", nonce)


def decode_ping(p: bytes) -> int:
    """Return the nonce of a `ping` or `pong` payload."""
    ((nonce,), _) = _unpack("<Q", p, 0)
    return nonce


def encode_getheaders(version: int, locator: list[str], stop: str | None = None) -> bytes:
    """Serialize `getheaders`. Locator and stop hashes are display hex, newest first.

    `version` must be the negotiated version: Zakura drops the connection otherwise.
    """
    if len(locator) > MAX_LOCATOR_HASHES:
        raise WireError(f"locator has {len(locator)} hashes, max {MAX_LOCATOR_HASHES}")
    hashes = b"".join(_wire_hash(h) for h in locator)
    stop_hash = _wire_hash(stop) if stop else bytes(32)
    return _pack("<I", version) + write_compact_size(len(locator)) + hashes + stop_hash


def decode_headers(p: bytes) -> list[BlockHeader]:
    """Parse a `headers` payload: at most 160 headers, each followed by a tx-count CompactSize."""
    count, off = _read_count(p, 0, MAX_HEADERS, "headers")
    if count * (BLOCK_HEADER_LEN + 1) > len(p) - off:
        raise WireError("truncated headers message")
    headers = []
    for _ in range(count):
        try:
            header, off = parse_header(p, off)
            _, off = read_compact_size(p, off)
        except ParseError as exc:
            raise WireError(f"bad header in headers message: {exc}") from exc
        headers.append(header)
    return headers


def encode_inv(items: list[tuple[int, bytes]]) -> bytes:
    """Serialize `inv`/`getdata`/`notfound` entries of (type, wire-order digest)."""
    if len(items) > MAX_INV_ITEMS:
        raise WireError(f"{len(items)} inventory items exceeds {MAX_INV_ITEMS}")
    parts = [write_compact_size(len(items))]
    for kind, digest in items:
        if _INV_DIGEST_LEN.get(kind) != len(digest):
            raise WireError(f"inventory type {kind} cannot carry a {len(digest)}-byte digest")
        parts.append(struct.pack("<I", kind) + bytes(digest))
    return b"".join(parts)


def decode_inv(p: bytes) -> list[tuple[int, bytes]]:
    """Parse inventory entries as (type, wire-order digest); unknown types raise WireError."""
    count, off = _read_count(p, 0, MAX_INV_ITEMS, "inventory")
    if count * 36 > len(p) - off:
        raise WireError("truncated inventory message")
    items = []
    for _ in range(count):
        ((kind,), off) = _unpack("<I", p, off)
        size = _INV_DIGEST_LEN.get(kind)
        if size is None:
            raise WireError(f"unknown inventory type {kind}")
        if off + size > len(p):
            raise WireError("truncated inventory entry")
        items.append((kind, bytes(p[off : off + size])))
        off += size
    return items


def decode_notfound(p: bytes) -> list[tuple[int, bytes]]:
    """Parse a `notfound` payload, which uses the `inv` layout."""
    return decode_inv(p)


def inv_block_hashes(items: list[tuple[int, bytes]]) -> list[str]:
    """Return the display-hex hashes of the MSG_BLOCK entries in decoded inventory."""
    return [digest[::-1].hex() for kind, digest in items if kind == INV_BLOCK]


def encode_getdata_blocks(hashes: list[str]) -> bytes:
    """Serialize a `getdata` for 1-16 display-hex block hashes."""
    if not 1 <= len(hashes) <= MAX_GETDATA_BLOCKS:
        raise WireError(f"getdata needs 1-{MAX_GETDATA_BLOCKS} block hashes, got {len(hashes)}")
    return encode_inv([(INV_BLOCK, _wire_hash(h)) for h in hashes])


def decode_addr(p: bytes) -> list[tuple[str, int, int, int]]:
    """Parse an `addr` payload into (ip, port, services, time) tuples, at most 1000."""
    count, off = _read_count(p, 0, MAX_ADDRS, "addr")
    if count * _ADDR_ENTRY_LEN > len(p) - off:
        raise WireError("truncated addr message")
    peers = []
    for _ in range(count):
        time, services = struct.unpack_from("<IQ", p, off)
        ip = _decode_ip(p[off + 12 : off + 28])
        (port,) = struct.unpack_from(">H", p, off + 28)
        peers.append((ip, port, services, time))
        off += _ADDR_ENTRY_LEN
    return peers


def decode_addrv2(p: bytes) -> list[tuple[str, int, int, int]]:
    """Parse an `addrv2` (ZIP 155) payload like `decode_addr`, keeping only IPv4/IPv6 entries."""
    count, off = _read_count(p, 0, MAX_ADDRS, "addrv2")
    peers = []
    for _ in range(count):
        ((time,), off) = _unpack("<I", p, off)
        services, off = _read_count(p, off, (1 << 64) - 1, "addrv2 services")
        ((network_id,), off) = _unpack("<B", p, off)
        addr_len, off = _read_count(p, off, MAX_ADDRV2_ADDR_LEN, "addrv2 address")
        if off + addr_len > len(p):
            raise WireError("truncated addrv2 address")
        addr = bytes(p[off : off + addr_len])
        ((port,), off) = _unpack(">H", p, off + addr_len)
        expected_len = _ADDRV2_IP_LENGTHS.get(network_id)
        if expected_len is None:
            continue  # Tor, I2P and CJDNS are not reachable over plain TCP
        if addr_len != expected_len:
            raise WireError(f"addrv2 network {network_id} address has {addr_len} bytes")
        ip = str(ipaddress.IPv4Address(addr)) if network_id == 1 else _decode_ip(addr)
        peers.append((ip, port, services, time))
    return peers


def encode_empty_addr() -> bytes:
    """Return an `addr` payload with no entries (our reply to `getaddr`)."""
    return write_compact_size(0)


def encode_empty_inv() -> bytes:
    """Return an `inv` payload with no entries (our reply to `mempool` and `getblocks`)."""
    return write_compact_size(0)


def encode_empty_headers() -> bytes:
    """Return a `headers` payload with no entries (our reply to `getheaders`)."""
    return write_compact_size(0)


def classify_user_agent(ua: str) -> tuple[str, str]:
    """Map a BIP14 user agent to (implementation, version) from its first component.

    The implementation is one of zakura, zebra, zcashd, zeeder, monitor or other.
    The version keeps only [0-9A-Za-z.+_-] and is at most 32 characters.
    """
    match = _USER_AGENT_RE.match(ua[:MAX_USER_AGENT_LEN])
    if match is None:
        return "other", ""
    impl = _USER_AGENT_IMPLS.get(match.group(1).strip().lower(), "other")
    version = _VERSION_JUNK_RE.sub("", match.group(2))[:MAX_UA_VERSION_LEN]
    return impl, version


def _ascii(text: str, what: str) -> bytes:
    """Encode `text` as ASCII, raising WireError for other characters."""
    try:
        return text.encode("ascii")
    except UnicodeEncodeError as exc:
        raise WireError(f"{what} must be ASCII: {text!r}") from exc


def _pack(fmt: str, *values: int) -> bytes:
    """Call struct.pack, raising WireError for out-of-range fields."""
    try:
        return struct.pack(fmt, *values)
    except struct.error as exc:
        raise WireError(f"field out of range for {fmt}: {values}") from exc


def _unpack(fmt: str, buf: bytes, off: int) -> tuple[tuple, int]:
    """Call struct.unpack_from with a WireError on truncation; return (values, next offset)."""
    end = off + struct.calcsize(fmt)
    if end > len(buf):
        raise WireError("truncated payload")
    return struct.unpack_from(fmt, buf, off), end


def _read_count(buf: bytes, off: int, cap: int, what: str) -> tuple[int, int]:
    """Read a CompactSize count, raising WireError when it is malformed or above `cap`."""
    try:
        count, off = read_compact_size(buf, off)
    except ParseError as exc:
        raise WireError(f"bad {what} count: {exc}") from exc
    if count > cap:
        raise WireError(f"{what} count {count} exceeds {cap}")
    return count, off


def _wire_hash(display_hex: str) -> bytes:
    """Convert a display-hex block hash to wire byte order, raising WireError if malformed."""
    try:
        return raw_hash_key(display_hex)
    except (TypeError, ValueError) as exc:
        raise WireError(f"invalid block hash {display_hex!r}") from exc


def _encode_ip(ip: str) -> bytes:
    """Encode an IPv4 (as IPv4-mapped) or IPv6 address into 16 bytes."""
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError as exc:
        raise WireError(f"invalid IP address {ip!r}") from exc
    if addr.version == 4:
        return _IPV4_MAPPED_PREFIX + addr.packed
    return addr.packed


def _decode_ip(raw: bytes) -> str:
    """Decode a 16-byte address, returning dotted IPv4 for IPv4-mapped entries."""
    addr = ipaddress.IPv6Address(bytes(raw))
    return str(addr.ipv4_mapped or addr)
