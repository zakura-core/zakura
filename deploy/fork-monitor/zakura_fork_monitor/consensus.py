"""Pure Zcash consensus helpers: difficulty math, header/block/coinbase parsing, miner labels.

Everything here is side-effect free and safe on untrusted bytes: parsers raise
`ParseError` (a `ValueError`) instead of reading past their input, and every loop
is bounded by the input length or an explicit cap.

The difficulty helpers implement the post-Blossom rules and switch to ZIP 218's
at each network's NU7 height (`NetworkParams.difficulty_rules`), as Zakura
v1.6.0 does; pre-Blossom heights are out of scope.

Notes:
- `TESTNET.funding_stream_addresses` holds only `t2HifwjU...`; `t3cFfPt1...`
  is the Mainnet address (see `crates/zakura-chain/src/parameters/network/
  subsidy/constants/`), and 3000 live Testnet coinbases confirm it.
- `bits_to_target` raises `ParseError` for invalid compact values, and
  `parse_header` rejects headers whose nBits cannot be expanded.
- `identify_miner` puts the tag ahead of the extranonce prefix in its
  `shielded:` fallback, because Foundry-style extranonces change on every block.
"""

from __future__ import annotations

import hashlib
import re
import struct
from dataclasses import dataclass


BLOCK_HEADER_LEN = 1487
# CompactSize(1344): the Equihash (200, 9) solution length on Mainnet and Testnet.
SOLUTION_SIZE_PREFIX = b"\xfd\x40\x05"
# Equihash (200, 9): 512 indices of 21 bits, 20-bit collisions per tree level, and
# 50-byte BLAKE2b outputs that each hold the 25-byte hashes of two indices.
EQUIHASH_N, EQUIHASH_K = 200, 9
_EQ_COLLISION_BITS = EQUIHASH_N // (EQUIHASH_K + 1)
_EQ_INDEX_BITS = _EQ_COLLISION_BITS + 1
_EQ_HASH_LEN = EQUIHASH_N // 8
_EQ_PERSON = b"ZcashPoW" + struct.pack("<II", EQUIHASH_N, EQUIHASH_K)
# Bytes of the header the solution commits to: everything before the solution's length.
_EQ_INPUT_LEN = 140
# Consensus block size limit; a larger payload cannot be a valid block.
MAX_BLOCK_SIZE = 2_000_000
# Consensus upper bound on the coinbase scriptSig length.
MAX_COINBASE_SCRIPT_LEN = 100
# Consensus MAX_MONEY in zatoshis; output values outside [0, MAX_MONEY] are invalid.
MAX_MONEY = 21_000_000 * 100_000_000
# Real coinbases have 1-3 transparent outputs; later ones are never needed for attribution.
MAX_PAYOUTS = 16
# Non-standard payout scripts are labelled by at most this many script bytes.
MAX_SCRIPT_LABEL_BYTES = 32
MAX_TAG_LEN = 128
# Tag text inside a `shielded:` miner label is shortened to keep labels readable.
MAX_LABEL_TAG_LEN = 48
# Pushes this short that carry no template marker are rig/job ids. Foundry's
# 4-5 byte extranonce is sometimes entirely printable.
MAX_ID_PUSH_LEN = 8

ZAKURA_MARKER = bytes.fromhex("f09f8cb8")  # U+1F338 cherry blossom, pushed by Zakura templates
ZEBRA_MARKER = bytes.fromhex("f09fa693")  # U+1F993 zebra, pushed by upstream Zebra templates
# Tokens inside a coinbase scriptSig chunk. Printable runs shorter than 3 bytes
# are usually coincidences inside binary ids, so they stay in the extranonce.
# The merged-mining commitment is magic + aux merkle root + size + nonce (44 bytes).
_SCRIPT_TOKENS = re.compile(
    rb"(?P<zakura>\xf0\x9f\x8c\xb8)"
    rb"|(?P<zebra>\xf0\x9f\xa6\x93)"
    rb"|(?P<auxpow>\xfa\xbemm.{0,40})"
    rb"|(?P<text>[\x20-\x7e]{3,})",
    re.DOTALL,
)

# Case-insensitive tag substring -> stable miner label, most specific first.
MINER_TAG_FAMILIES = (
    ("zkcodexcoder", "zkcodexcoder"),
    ("foundry", "Foundry"),
    ("mariana", "MarianaTrench"),
    ("toalt23", "toalt23"),
    ("molepool", "molepool"),
    ("w.cash", "W.cash"),  # "Wolf: W.cash AuxPow" merged miner with a shielded payout
    ("open-krnx", "open-krnx-pool"),
)

_NULL_PREVOUT = bytes(32) + b"\xff\xff\xff\xff"
_COMPACT_WIDTHS = {0xFD: (2, 0xFD), 0xFE: (4, 0x1_0000), 0xFF: (8, 0x1_0000_0000)}
_BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


class ParseError(ValueError):
    """Raised when consensus data is truncated, non-canonical or out of range."""


# Difficulty-adjustment constants that NU7 keeps: PoWMedianBlockSpan, PoWDampingFactor, and
# PoWMaxAdjustUp and PoWMaxAdjustDown in percent.
MEDIAN_SPAN = 11
DAMPING = 4
MAX_ADJUST_UP_PERCENT = 16
MAX_ADJUST_DOWN_PERCENT = 32


@dataclass(frozen=True, slots=True)
class DifficultyRules:
    """Difficulty-adjustment parameters for a block, selected by the block's own height.

    They govern its whole adjustment, so the first blocks from NU7 average pre-NU7
    targets and times against the NU7 timespan; nothing is rescaled.
    """

    target_spacing: int  # PoWTargetSpacing, seconds
    averaging_window: int  # PoWAveragingWindow, blocks
    min_diff_gap_multiplier: int  # Testnet minimum-difficulty gap in target spacings (see `min_diff_gap`)

    @property
    def averaging_window_timespan(self) -> int:
        """Return the ideal duration of one averaging window in seconds."""
        return self.averaging_window * self.target_spacing

    @property
    def min_timespan(self) -> int:
        """Return the lower bound on the damped timespan (`MinActualTimespan`)."""
        return self.averaging_window_timespan * (100 - MAX_ADJUST_UP_PERCENT) // 100

    @property
    def max_timespan(self) -> int:
        """Return the upper bound on the damped timespan (`MaxActualTimespan`)."""
        return self.averaging_window_timespan * (100 + MAX_ADJUST_DOWN_PERCENT) // 100

    @property
    def min_diff_gap(self) -> int:
        """Return the parent gap in seconds above which a Testnet block must carry the PoW-limit nBits."""
        return self.min_diff_gap_multiplier * self.target_spacing

    @property
    def context_len(self) -> int:
        """Return how many ancestors' (bits, time) `expected_bits` reads."""
        return self.averaging_window + MEDIAN_SPAN


# ZIP 208's post-Blossom rules, then ZIP 218's from NU7: 25 s spacing and a 102-block window,
# with Testnet's minimum-difficulty gap kept at 450 s.
PRE_NU7_RULES = DifficultyRules(target_spacing=75, averaging_window=17, min_diff_gap_multiplier=6)
NU7_RULES = DifficultyRules(target_spacing=25, averaging_window=102, min_diff_gap_multiplier=18)


@dataclass(frozen=True, slots=True)
class NetworkParams:
    """Consensus and P2P constants for one Zcash network."""

    name: str
    magic: bytes
    default_port: int
    pow_limit: int
    pow_limit_bits: int
    # NU7 activation height; None while the network has not scheduled NU7.
    nu7_height: int | None
    # First height where the Testnet minimum-difficulty rule applies; None disables it.
    min_diff_after_height: int | None
    genesis_hash: str
    p2pkh_prefix: bytes
    p2sh_prefix: bytes
    funding_stream_addresses: frozenset[str]

    def difficulty_rules(self, height: int) -> DifficultyRules:
        """Return the difficulty-adjustment rules for a block at `height` (ZIP 218's `IsNU7Activated`)."""
        return NU7_RULES if self.nu7_height is not None and height >= self.nu7_height else PRE_NU7_RULES

    @property
    def max_context_len(self) -> int:
        """Return the most ancestors `expected_bits` reads at any height on this network."""
        rules = (PRE_NU7_RULES,) if self.nu7_height is None else (PRE_NU7_RULES, NU7_RULES)
        return max(r.context_len for r in rules)


TESTNET = NetworkParams(
    name="testnet",
    magic=bytes.fromhex("fa1af9bf"),
    default_port=18233,
    pow_limit=(1 << 251) - 1,
    pow_limit_bits=0x2007FFFF,
    nu7_height=4_465_026,
    min_diff_after_height=299_188,
    genesis_hash="05a60a92d99d85997cce3b87616c089f6124d7342af37106edc76126334a2c38",
    p2pkh_prefix=b"\x1d\x25",
    p2sh_prefix=b"\x1c\xba",
    funding_stream_addresses=frozenset({"t2HifwjUj9uyxr9bknR8LFuQbc98c3vkXtu"}),
)

MAINNET = NetworkParams(
    name="mainnet",
    magic=bytes.fromhex("24e92764"),
    default_port=8233,
    pow_limit=(1 << 243) - 1,
    pow_limit_bits=0x1F07FFFF,
    nu7_height=None,
    min_diff_after_height=None,
    genesis_hash="00040fe8ec8471911baa1db1266ea15dd06b4a8a5c453883c000b031973dce08",
    p2pkh_prefix=b"\x1c\xb8",
    p2sh_prefix=b"\x1c\xbd",
    funding_stream_addresses=frozenset({"t3cFfPt1Bcvgez9ZbMBFWeZsskxTkPzGCow"}),
)

NETWORKS = {params.name: params for params in (TESTNET, MAINNET)}


def sha256d(data: bytes) -> bytes:
    """Return SHA-256(SHA-256(data))."""
    return hashlib.sha256(hashlib.sha256(data).digest()).digest()


def read_compact_size(buf: bytes, off: int) -> tuple[int, int]:
    """Read a canonical CompactSize at `off`; return (value, offset after it)."""
    if not 0 <= off < len(buf):
        raise ParseError("truncated compact size")
    first = buf[off]
    if first < 0xFD:
        return first, off + 1
    width, minimum = _COMPACT_WIDTHS[first]
    end = off + 1 + width
    if end > len(buf):
        raise ParseError("truncated compact size")
    value = int.from_bytes(buf[off + 1 : end], "little")
    if value < minimum:
        raise ParseError("non-canonical compact size")
    return value, end


def write_compact_size(n: int) -> bytes:
    """Encode `n` as a canonical CompactSize."""
    if not 0 <= n < 1 << 64:
        raise ValueError(f"compact size out of range: {n}")
    if n < 0xFD:
        return bytes((n,))
    if n <= 0xFFFF:
        return b"\xfd" + n.to_bytes(2, "little")
    if n <= 0xFFFF_FFFF:
        return b"\xfe" + n.to_bytes(4, "little")
    return b"\xff" + n.to_bytes(8, "little")


def bits_to_target(bits: int) -> int:
    """Expand compact nBits into a target; ParseError for negative, zero or overflowing values."""
    if not 0 <= bits <= 0xFFFF_FFFF or bits & 0x0080_0000:
        raise ParseError(f"invalid compact difficulty {bits:#x}")
    mantissa = bits & 0x007F_FFFF
    exponent = bits >> 24
    if exponent <= 3:
        target = mantissa >> (8 * (3 - exponent))
    else:
        target = mantissa << (8 * (exponent - 3))
    if target == 0 or target >= 1 << 256:
        raise ParseError(f"invalid compact difficulty {bits:#x}")
    return target


def target_to_bits(target: int) -> int:
    """Compress a positive 256-bit target into compact nBits (Bitcoin `GetCompact`)."""
    if not 0 < target < 1 << 256:
        raise ValueError(f"target out of range: {target}")
    size = (target.bit_length() + 7) // 8
    if size <= 3:
        mantissa = target << (8 * (3 - size))
    else:
        mantissa = target >> (8 * (size - 3))
    if mantissa & 0x0080_0000:
        mantissa >>= 8
        size += 1
    return (size << 24) | mantissa


def work_from_bits(bits: int) -> int:
    """Return the block work floor(2**256 / (target + 1)) for nBits."""
    return (1 << 256) // (bits_to_target(bits) + 1)


def difficulty_from_bits(bits: int, params: NetworkParams) -> float:
    """Return the RPC-style difficulty: PoW-limit target over this block's target."""
    return bits_to_target(params.pow_limit_bits) / bits_to_target(bits)


def is_min_difficulty(params: NetworkParams, height: int, time: int, parent_time: int) -> bool:
    """Return True when the Testnet minimum-difficulty rule applies: the parent gap exceeds `min_diff_gap`."""
    start = params.min_diff_after_height
    gap_limit = params.difficulty_rules(height).min_diff_gap
    return start is not None and height >= start and time - parent_time > gap_limit


def expected_bits(params: NetworkParams, height: int, time: int, prev: list[tuple[int, int]]) -> int:
    """Predict nBits for a block at `height` with header `time`, under `params.difficulty_rules(height)`.

    `prev` holds (bits, time) of the previous blocks on the same branch, newest
    first (prev[0] is the parent); at least the rules' `context_len` (28 before
    NU7, 113 from NU7). Later entries are ignored.
    """
    rules = params.difficulty_rules(height)
    window = rules.averaging_window
    if len(prev) < rules.context_len:
        raise ValueError(f"need {rules.context_len} previous headers, got {len(prev)}")
    if is_min_difficulty(params, height, time, prev[0][1]):
        return params.pow_limit_bits
    mean_target = sum(bits_to_target(bits) for bits, _ in prev[:window]) // window
    newer = _median([t for _, t in prev[:MEDIAN_SPAN]])
    older = _median([t for _, t in prev[window : window + MEDIAN_SPAN]])
    ideal = rules.averaging_window_timespan
    damped = ideal + _trunc_div(newer - older - ideal, DAMPING)
    bounded = min(max(damped, rules.min_timespan), rules.max_timespan)
    limit = bits_to_target(params.pow_limit_bits)
    return target_to_bits(min(limit, mean_target // ideal * bounded))


def _median(values: list[int]) -> int:
    """Return the upper median, matching Zakura's `sorted[len / 2]`."""
    return sorted(values)[len(values) // 2]


def _trunc_div(numerator: int, denominator: int) -> int:
    """Integer division rounding toward zero, like Rust's `/` on signed integers."""
    quotient = abs(numerator) // denominator
    return quotient if numerator >= 0 else -quotient


@dataclass(frozen=True, slots=True)
class BlockHeader:
    """A parsed 1487-byte Zcash block header; hashes are display (reversed) hex."""

    hash: str
    prev_hash: str
    version: int
    merkle_root: str
    time: int
    bits: int
    nonce: str
    raw: bytes


def parse_header(buf: bytes, off: int = 0) -> tuple[BlockHeader, int]:
    """Parse a block header at `off`; return (header, offset after it)."""
    end = off + BLOCK_HEADER_LEN
    if off < 0 or end > len(buf):
        raise ParseError("truncated block header")
    raw = bytes(buf[off:end])
    if raw[140:143] != SOLUTION_SIZE_PREFIX:
        raise ParseError("Equihash solution is not 1344 bytes")
    (version,) = struct.unpack_from("<I", raw, 0)
    time, bits = struct.unpack_from("<II", raw, 100)
    bits_to_target(bits)
    header = BlockHeader(
        hash=sha256d(raw)[::-1].hex(),
        prev_hash=raw[4:36][::-1].hex(),
        version=version,
        merkle_root=raw[36:68][::-1].hex(),
        time=time,
        bits=bits,
        nonce=raw[108:140][::-1].hex(),
        raw=raw,
    )
    return header, end


def check_pow(header: BlockHeader, params: NetworkParams) -> bool:
    """Return True if the header meets its own target within the PoW limit and has a valid Equihash solution.

    Context-free: whether nBits is the expected difficulty depends on the parent chain (`expected_bits`).
    """
    target = bits_to_target(header.bits)
    return (
        target <= bits_to_target(params.pow_limit_bits)
        and int(header.hash, 16) <= target
        and check_equihash(header)
    )


def check_equihash(header: BlockHeader) -> bool:
    """Return True if the header carries a valid Equihash (200, 9) solution for its first 140 bytes.

    Follows zcashd's `IsValidSolution`: distinct indices, and at every tree level
    the left subtree's first index is below the right one's and the two XORs
    agree on the next 20 bits; the XOR of all 512 hashes is zero.
    """
    raw = header.raw
    count = 1 << EQUIHASH_K
    mask = (1 << _EQ_INDEX_BITS) - 1
    packed = int.from_bytes(raw[len(SOLUTION_SIZE_PREFIX) + _EQ_INPUT_LEN : BLOCK_HEADER_LEN], "big")
    indices = [(packed >> (_EQ_INDEX_BITS * (count - 1 - j))) & mask for j in range(count)]
    if len(set(indices)) != count:
        return False
    base = hashlib.blake2b(raw[:_EQ_INPUT_LEN], digest_size=2 * _EQ_HASH_LEN, person=_EQ_PERSON)
    digests: dict[int, bytes] = {}
    level = []
    for index in indices:
        digest = digests.get(index >> 1)
        if digest is None:
            state = base.copy()
            state.update(struct.pack("<I", index >> 1))
            digest = digests[index >> 1] = state.digest()
        start = (index & 1) * _EQ_HASH_LEN
        level.append((int.from_bytes(digest[start : start + _EQ_HASH_LEN], "big"), index))
    for depth in range(1, EQUIHASH_K + 1):
        shift = EQUIHASH_N - _EQ_COLLISION_BITS * depth
        merged = []
        for (left, first), (right, other) in zip(level[::2], level[1::2], strict=True):
            if first >= other or (left ^ right) >> shift:
                return False
            merged.append((left ^ right, first))
        level = merged
    return level[0][0] == 0


@dataclass(frozen=True, slots=True)
class Coinbase:
    """Attribution data from a block's coinbase transaction."""

    height: int | None
    script_sig: bytes
    template: str | None
    tag: str
    extranonce: str
    payouts: tuple[tuple[str, int], ...]
    tx_version: int


@dataclass(frozen=True, slots=True)
class Block:
    """A block header plus the coinbase fields the monitor needs."""

    header: BlockHeader
    size: int
    tx_count: int
    coinbase: Coinbase | None


def parse_block(raw: bytes, params: NetworkParams) -> Block:
    """Parse a serialized block's header, transaction count and coinbase transparent prefix.

    `coinbase` is None when the first transaction uses a version this parser does
    not know; any structural problem raises `ParseError`.
    """
    if len(raw) > MAX_BLOCK_SIZE:
        raise ParseError(f"block of {len(raw)} bytes exceeds the consensus limit")
    header, off = parse_header(raw)
    tx_count, off = read_compact_size(raw, off)
    if tx_count == 0 or tx_count > len(raw) - off:
        raise ParseError(f"implausible transaction count {tx_count}")
    coinbase = _parse_coinbase(raw, off, params)
    return Block(header=header, size=len(raw), tx_count=tx_count, coinbase=coinbase)


def _take(buf: bytes, off: int, n: int) -> tuple[bytes, int]:
    """Return (buf[off:off+n], off+n), raising ParseError if fewer than n bytes remain."""
    end = off + n
    if end > len(buf):
        raise ParseError("truncated transaction")
    return bytes(buf[off:end]), end


def _parse_coinbase(buf: bytes, off: int, params: NetworkParams) -> Coinbase | None:
    """Parse the coinbase transaction's version, single input and first outputs."""
    word, off = _take(buf, off, 4)
    header_word = int.from_bytes(word, "little")
    overwintered, version = header_word >> 31, header_word & 0x7FFF_FFFF
    # The transparent bundle precedes every shielded part in all versions.
    if not overwintered and version in (1, 2):
        skip = 0
    elif overwintered and version in (3, 4):
        skip = 4  # nVersionGroupId
    elif overwintered and version in (5, 6):
        skip = 16  # nVersionGroupId, nConsensusBranchId, nLockTime, nExpiryHeight
    else:
        return None
    _, off = _take(buf, off, skip)

    n_inputs, off = read_compact_size(buf, off)
    prevout, off = _take(buf, off, 36)
    if n_inputs != 1 or prevout != _NULL_PREVOUT:
        raise ParseError("first transaction is not a coinbase")
    script_len, off = read_compact_size(buf, off)
    if script_len > MAX_COINBASE_SCRIPT_LEN:
        raise ParseError(f"coinbase script of {script_len} bytes exceeds the consensus limit")
    script_sig, off = _take(buf, off, script_len)
    _, off = _take(buf, off, 4)  # nSequence

    n_outputs, off = read_compact_size(buf, off)
    if n_outputs * 9 > len(buf) - off:
        raise ParseError(f"implausible output count {n_outputs}")
    payouts = []
    for _ in range(min(n_outputs, MAX_PAYOUTS)):
        value_raw, off = _take(buf, off, 8)
        value = int.from_bytes(value_raw, "little", signed=True)
        if not 0 <= value <= MAX_MONEY:
            raise ParseError(f"coinbase output value {value} out of range")
        script_len, off = read_compact_size(buf, off)
        script_pubkey, off = _take(buf, off, script_len)
        payouts.append((_payee_label(script_pubkey, params), value))

    height, template, tag, extranonce = decode_coinbase_script(script_sig)
    return Coinbase(
        height=height,
        script_sig=script_sig,
        template=template,
        tag=tag,
        extranonce=extranonce,
        payouts=tuple(payouts),
        tx_version=version,
    )


def _payee_label(script_pubkey: bytes, params: NetworkParams) -> str:
    """Return the transparent address for P2PKH/P2SH scripts, else "script:<hex prefix>"."""
    spk = script_pubkey
    if len(spk) == 25 and spk[:3] == b"\x76\xa9\x14" and spk[23:] == b"\x88\xac":
        return encode_address(params.p2pkh_prefix, spk[3:23])
    if len(spk) == 23 and spk[:2] == b"\xa9\x14" and spk[22:] == b"\x87":
        return encode_address(params.p2sh_prefix, spk[2:22])
    return "script:" + spk[:MAX_SCRIPT_LABEL_BYTES].hex()


def encode_address(prefix: bytes, h160: bytes) -> str:
    """Base58Check-encode `prefix || h160` as a transparent Zcash address."""
    payload = bytes(prefix) + bytes(h160)
    data = payload + sha256d(payload)[:4]
    number = int.from_bytes(data, "big")
    digits = []
    while number:
        number, remainder = divmod(number, 58)
        digits.append(_BASE58_ALPHABET[remainder])
    leading_zeros = len(data) - len(data.lstrip(b"\0"))
    return "1" * leading_zeros + "".join(reversed(digits))


def decode_coinbase_script(script_sig: bytes) -> tuple[int | None, str | None, str, str]:
    """Split a coinbase scriptSig into (BIP34 height, template, miner tag, extranonce hex).

    After the height push, a byte below 0x20 that fits is read as a push length
    (it cannot start printable text); anything else starts raw miner bytes.
    Template markers are found in any chunk. Printable ASCII runs become the tag,
    and the remaining bytes (rig/job ids, merged-mining commitments) the extranonce.
    """
    height, rest = _split_bip34_height(script_sig)
    template: str | None = None
    texts: list[bytes] = []
    extranonce = bytearray()
    pos = 0
    while pos < len(rest):
        length = rest[pos]
        if 1 <= length < 0x20 and pos + 1 + length <= len(rest):
            chunk, pos = rest[pos + 1 : pos + 1 + length], pos + 1 + length
            has_marker = ZAKURA_MARKER in chunk or ZEBRA_MARKER in chunk
            if len(chunk) <= MAX_ID_PUSH_LEN and not has_marker:
                extranonce += chunk
                continue
        else:
            chunk, pos = rest[pos:], len(rest)
        last = 0
        for token in _SCRIPT_TOKENS.finditer(chunk):
            extranonce += chunk[last : token.start()]
            last = token.end()
            kind = token.lastgroup
            if kind in ("zakura", "zebra"):
                template = template or kind
            elif kind == "text":
                texts.append(token.group())
            else:
                extranonce += token.group()
        extranonce += chunk[last:]
    tag = " ".join(b" ".join(texts).decode("ascii").split()).strip(" :")
    return height, template, tag[:MAX_TAG_LEN], extranonce.hex()


def _split_bip34_height(script_sig: bytes) -> tuple[int | None, bytes]:
    """Return (BIP34 height, remaining script), or (None, whole script) if there is no height push."""
    if not script_sig:
        return None, b""
    op = script_sig[0]
    if 0x51 <= op <= 0x60:  # OP_1..OP_16
        return op - 0x50, script_sig[1:]
    if 1 <= op <= 5 and len(script_sig) > op:
        number = script_sig[1 : 1 + op]
        if number[-1] & 0x80:  # negative script number
            return None, script_sig
        return int.from_bytes(number, "little"), script_sig[1 + op :]
    return None, script_sig


def identify_miner(coinbase: Coinbase | None, params: NetworkParams) -> str:
    """Return a stable miner label for a coinbase.

    Order: a known tag family (`MINER_TAG_FAMILIES`), then the first paid output
    that is not a funding stream, then "shielded:<tag or extranonce prefix or
    'notag'>". A missing coinbase is "unknown".
    """
    if coinbase is None:
        return "unknown"
    lowered = coinbase.tag.lower()
    for needle, label in MINER_TAG_FAMILIES:
        if needle in lowered:
            return label
    for payee, value in coinbase.payouts:
        if value > 0 and payee not in params.funding_stream_addresses:
            return payee
    fallback = coinbase.tag[:MAX_LABEL_TAG_LEN] or coinbase.extranonce[:8] or "notag"
    return f"shielded:{fallback}"


def raw_hash_key(display_hex: str) -> bytes:
    """Return a display-hex block hash in internal (wire) byte order.

    Zakura and Zebra 6.3 break equal-work ties by the greater of these keys.
    """
    raw = bytes.fromhex(display_hex)
    if len(raw) != 32:
        raise ValueError(f"block hash must be 32 bytes, got {len(raw)}")
    return raw[::-1]
