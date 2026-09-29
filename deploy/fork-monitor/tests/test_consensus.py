"""Tests for zakura_fork_monitor.consensus against live-captured Testnet fixtures."""

from __future__ import annotations

import json
import math
import struct
import unittest
from pathlib import Path

from zakura_fork_monitor import consensus as c


FIXTURES = Path(__file__).with_name("fixtures")
FS = "t2HifwjUj9uyxr9bknR8LFuQbc98c3vkXtu"
FOUNDRY_ADDR = "tmJggjzf2qPBbmUFfr7eYdqFUV15y1MvvVu"
FOUNDRY_TAG = "Foundry Zcash Pool #PrivacyMatters"

# Captured once from `getblock <hash> 2` on zakura-testnet-1 (167.99.103.111:18232).
RPC_BLOCKS = {
    4410736: {
        "hash": "00000d8cb27b2e95b223afe7325fe411a4b355b82e5523ad09eff014112bb644",
        "bits": 0x1E0EC6E4,
        "time": 1790632996,
        "difficulty": 35480.13551145164,
        "size": 1949,
        "tx_count": 2,
        "nonce": "000000000000000000000000000d0000000000008e1f00c0000000003b000001",
        "merkle_root": "c3c708af058e2e1652ad2013f84ec340f940445e75683ba6c3f6d05e8a2a88ae",
        "coinbase_hex": "03704d4304f09f8cb805cd4124e400466f756e647279205a6361736820506f6f6c2023507269766163794d617474657273",
        "template": "zakura",
        "tag": FOUNDRY_TAG,
        "extranonce": "cd4124e400",
        "payouts": ((FOUNDRY_ADDR, 125020000), (FS, 12500000)),
        "miner": "Foundry",
    },
    4410737: {
        "hash": "00003e268eb0b585a2d6ade5ef2d1e098c3e35b247dcca07e18cf73033406804",
        "bits": 0x2007FFFF,
        "time": 1790633447,
        "difficulty": 1.0,
        "size": 14270,
        "tx_count": 3,
        "nonce": "00000000000000000000000000b40000000000008d000000000000003b000001",
        "merkle_root": "2b453cecf6b036b04d39e6febd2ab657c9c56225e21a5d00960993a2ec3ef443",
        "coinbase_hex": "03714d4304f09f8cb805fb4752e500466f756e647279205a6361736820506f6f6c2023507269766163794d617474657273",
        "template": "zakura",
        "tag": FOUNDRY_TAG,
        "extranonce": "fb4752e500",
        "payouts": ((FOUNDRY_ADDR, 125035000), (FS, 12500000)),
        "miner": "Foundry",
    },
    4410738: {
        "hash": "005e7e8ddca530f2601f2207930aefb412c796c304ad991f86fa0a5c055be611",
        "bits": 0x1F765A75,
        "time": 1790633300,
        "difficulty": 17.304082496981525,
        "size": 1630,
        "tx_count": 1,
        "nonce": "924c0e0000000000000000000000000000000000000000000000000075000000",
        "merkle_root": "5ee88254eabf3cc56e050c670fa6492bdd00896ef09ee52b940cd2c37508eedb",
        "coinbase_hex": "03724d4304f09fa693",
        "template": "zebra",
        "tag": "",
        "extranonce": "",
        "payouts": (("tmDDBnPEg12A4GYACyq9KwUEyq5vMiALZQR", 125000000), (FS, 12500000)),
        "miner": "tmDDBnPEg12A4GYACyq9KwUEyq5vMiALZQR",
    },
    4410755: {
        "hash": "0049697daa23e687f90e7563e8e1a72c2a726809f1d6794aa332108378885104",
        "bits": 0x20009FF4,
        "time": 1790633342,
        "difficulty": 12.803726677737618,
        "size": 7592,
        "tx_count": 1,
        "nonce": "0000000000000000000000000100000000000000000000000000007100000002",
        "merkle_root": "2b104686bd38c7705b94b51246b6898cb5fa0dd918c13e3d4e79e9663875aa8f",
        "coinbase_hex": "03834d4304f09f8cb87a6b636f646578636f646572",
        "template": "zakura",
        "tag": "zkcodexcoder",
        "extranonce": "",
        "payouts": ((FS, 12500000),),
        "miner": "zkcodexcoder",
    },
}


def fixture_block(height: int) -> bytes:
    """Return the raw bytes of a fixture block."""
    return bytes.fromhex((FIXTURES / f"block-test-{height}.hex").read_text().strip())


def base58check_decode(address: str) -> bytes:
    """Decode a Base58Check string and verify its checksum (test helper)."""
    number = 0
    for char in address:
        number = number * 58 + c._BASE58_ALPHABET.index(char)
    body = number.to_bytes((number.bit_length() + 7) // 8, "big")
    data = bytes(len(address) - len(address.lstrip("1"))) + body
    payload, checksum = data[:-4], data[-4:]
    assert c.sha256d(payload)[:4] == checksum, address
    return payload


def coinbase_tx(
    script_sig: bytes,
    outputs: list[tuple[bytes, int]],
    *,
    version: int = 6,
    overwintered: bool = True,
    n_inputs: int = 1,
    prevout: bytes = bytes(32) + b"\xff\xff\xff\xff",
) -> bytes:
    """Serialize a minimal coinbase transaction prefix for parser tests."""
    head = struct.pack("<I", (0x8000_0000 if overwintered else 0) | version)
    if overwintered and version in (3, 4):
        head += struct.pack("<I", 0x892F2085)
    elif overwintered and version in (5, 6):
        head += struct.pack("<IIII", 0xD884B698, 0x37A5165B, 0, 0)
    body = c.write_compact_size(n_inputs) + prevout
    body += c.write_compact_size(len(script_sig)) + script_sig + b"\xff\xff\xff\xff"
    body += c.write_compact_size(len(outputs))
    for script, value in outputs:
        body += struct.pack("<q", value) + c.write_compact_size(len(script)) + script
    return head + body + bytes(8)


def block_with(tx: bytes, tx_count: int = 1) -> bytes:
    """Return a block made of a real fixture header, `tx_count` and `tx`."""
    return fixture_block(4410755)[: c.BLOCK_HEADER_LEN] + c.write_compact_size(tx_count) + tx


P2PKH_FOUNDRY = bytes.fromhex("76a914626880df2ad8f35605e17dc8d2c18647d95cc42488ac")
P2SH_FS = bytes.fromhex("a9147a86d6c7eb12ce0aa309d7391a6f338eba3c242b87")


class HeaderTests(unittest.TestCase):
    def test_fixture_headers_match_rpc(self):
        for height, want in RPC_BLOCKS.items():
            with self.subTest(height=height):
                raw = fixture_block(height)
                header, end = c.parse_header(raw)
                self.assertEqual(end, c.BLOCK_HEADER_LEN)
                self.assertEqual(header.hash, want["hash"])
                self.assertEqual(header.bits, want["bits"])
                self.assertEqual(header.time, want["time"])
                self.assertEqual(header.nonce, want["nonce"])
                self.assertEqual(header.merkle_root, want["merkle_root"])
                self.assertEqual(header.version, 4)
                self.assertEqual(header.raw, raw[: c.BLOCK_HEADER_LEN])
                self.assertEqual(c.sha256d(header.raw)[::-1].hex(), want["hash"])
                self.assertTrue(c.check_pow(header, c.TESTNET))

    def test_fixture_parent_links(self):
        hashes = {h: c.parse_header(fixture_block(h))[0] for h in (4410736, 4410737, 4410738)}
        self.assertEqual(hashes[4410737].prev_hash, hashes[4410736].hash)
        self.assertEqual(hashes[4410738].prev_hash, hashes[4410737].hash)

    def test_parse_header_at_offset(self):
        raw = b"\xaa" * 7 + fixture_block(4410736)
        header, end = c.parse_header(raw, 7)
        self.assertEqual(header.hash, RPC_BLOCKS[4410736]["hash"])
        self.assertEqual(end, 7 + c.BLOCK_HEADER_LEN)

    def test_parse_header_rejects_bad_input(self):
        raw = bytearray(fixture_block(4410736)[: c.BLOCK_HEADER_LEN])
        with self.assertRaises(c.ParseError):
            c.parse_header(bytes(raw[:-1]))
        with self.assertRaises(c.ParseError):
            c.parse_header(bytes(raw), 1)
        bad_solution = bytearray(raw)
        bad_solution[141] = 0x41
        with self.assertRaises(c.ParseError):
            c.parse_header(bytes(bad_solution))
        zero_bits = bytearray(raw)
        zero_bits[104:108] = bytes(4)
        with self.assertRaises(c.ParseError):
            c.parse_header(bytes(zero_bits))

    def test_check_pow_rejects_tampered_headers(self):
        raw = bytearray(fixture_block(4410736)[: c.BLOCK_HEADER_LEN])
        raw[108] ^= 0xFF
        tampered, _ = c.parse_header(bytes(raw))
        self.assertFalse(c.check_pow(tampered, c.TESTNET))
        easy = bytearray(fixture_block(4410736)[: c.BLOCK_HEADER_LEN])
        easy[104:108] = struct.pack("<I", 0x2100FFFF)  # valid compact, above the PoW limit
        self.assertFalse(c.check_pow(c.parse_header(bytes(easy))[0], c.TESTNET))

    def test_check_pow_verifies_the_equihash_solution(self):
        # A header that meets the PoW-limit target (1 in 32 nonces) but carries no real solution.
        prev = c.parse_header(fixture_block(4410738))[0]
        for nonce in range(1 << 12):
            raw = (
                struct.pack("<I", 4) + bytes.fromhex(prev.hash)[::-1] + bytes(64)
                + struct.pack("<II", prev.time + 451, c.TESTNET.pow_limit_bits)
                + nonce.to_bytes(32, "little") + c.SOLUTION_SIZE_PREFIX + bytes(1344)
            )
            forged = c.parse_header(raw)[0]
            if int(forged.hash, 16) <= c.bits_to_target(forged.bits):
                break
        self.assertFalse(c.check_equihash(forged))
        self.assertFalse(c.check_pow(forged, c.TESTNET))

    def test_equihash_rejects_altered_solutions(self):
        header = c.parse_header(fixture_block(4410737))[0]
        self.assertTrue(c.check_equihash(header))

        def with_indices(indices):
            packed = 0
            for index in indices:
                packed = packed << 21 | index
            raw = header.raw[:143] + packed.to_bytes(1344, "big")
            return c.parse_header(raw)[0]

        packed = int.from_bytes(header.raw[143:], "big")
        indices = [(packed >> (21 * (511 - j))) & 0x1FFFFF for j in range(512)]
        self.assertTrue(c.check_equihash(with_indices(indices)))
        swapped = [indices[1], indices[0], *indices[2:]]  # same XORs, wrong subtree order
        self.assertFalse(c.check_equihash(with_indices(swapped)))
        repeated = [indices[0], *indices[:511]]
        self.assertFalse(c.check_equihash(with_indices(repeated)))
        other = bytearray(header.raw)
        other[4] ^= 1  # the solution no longer matches the header it commits to
        self.assertFalse(c.check_equihash(c.parse_header(bytes(other))[0]))


class BlockTests(unittest.TestCase):
    def test_fixture_blocks_parse(self):
        for height, want in RPC_BLOCKS.items():
            with self.subTest(height=height):
                block = c.parse_block(fixture_block(height), c.TESTNET)
                self.assertEqual(block.size, want["size"])
                self.assertEqual(block.tx_count, want["tx_count"])
                cb = block.coinbase
                self.assertIsNotNone(cb)
                self.assertEqual(cb.height, height)
                self.assertEqual(cb.tx_version, 6)
                self.assertEqual(cb.script_sig.hex(), want["coinbase_hex"])
                self.assertEqual(cb.template, want["template"])
                self.assertEqual(cb.tag, want["tag"])
                self.assertEqual(cb.extranonce, want["extranonce"])
                self.assertEqual(cb.payouts, want["payouts"])
                self.assertEqual(c.identify_miner(cb, c.TESTNET), want["miner"])

    def test_older_transaction_versions(self):
        outputs = [(P2PKH_FOUNDRY, 5), (P2SH_FS, 7)]
        for version, overwintered in ((1, False), (2, False), (3, True), (4, True), (5, True)):
            with self.subTest(version=version):
                tx = coinbase_tx(b"\x03\x01\x02\x03", outputs, version=version, overwintered=overwintered)
                cb = c.parse_block(block_with(tx), c.TESTNET).coinbase
                self.assertEqual(cb.tx_version, version)
                self.assertEqual(cb.height, 0x030201)
                self.assertEqual(cb.payouts, ((FOUNDRY_ADDR, 5), (FS, 7)))

    def test_unknown_transaction_version_has_no_coinbase(self):
        tx = coinbase_tx(b"\x03\x01\x02\x03", [(P2SH_FS, 1)], version=7)
        block = c.parse_block(block_with(tx), c.TESTNET)
        self.assertIsNone(block.coinbase)
        self.assertEqual(c.identify_miner(block.coinbase, c.TESTNET), "unknown")

    def test_payout_labels_and_cap(self):
        outputs = [(b"\x6a\x04abcd", 0), (b"\x51" * 40, 3)] + [(P2SH_FS, 1)] * 20
        cb = c.parse_block(block_with(coinbase_tx(b"\x51", outputs)), c.TESTNET).coinbase
        self.assertEqual(len(cb.payouts), c.MAX_PAYOUTS)
        self.assertEqual(cb.payouts[0], ("script:6a0461626364", 0))
        self.assertEqual(cb.payouts[1], ("script:" + "51" * c.MAX_SCRIPT_LABEL_BYTES, 3))
        self.assertEqual(cb.height, 1)
        self.assertEqual(c.identify_miner(cb, c.TESTNET), "script:" + "51" * c.MAX_SCRIPT_LABEL_BYTES)

    def test_malformed_blocks_raise(self):
        good = coinbase_tx(b"\x03\x01\x02\x03", [(P2SH_FS, 1)])
        cases = {
            "truncated coinbase": block_with(good)[:-20],
            "no transactions": block_with(good, tx_count=0),
            "absurd tx count": block_with(good, tx_count=1_000_000),
            "not a coinbase prevout": block_with(coinbase_tx(b"\x51", [], prevout=bytes(36))),
            "two inputs": block_with(coinbase_tx(b"\x51", [], n_inputs=2)),
            "script too long": block_with(coinbase_tx(b"\x51" * 101, [])),
            "negative value": block_with(coinbase_tx(b"\x51", [(P2SH_FS, -1)])),
            "value above MAX_MONEY": block_with(coinbase_tx(b"\x51", [(P2SH_FS, c.MAX_MONEY + 1)])),
            "oversized block": block_with(good) + bytes(c.MAX_BLOCK_SIZE),
            "header only": fixture_block(4410736)[: c.BLOCK_HEADER_LEN],
        }
        for name, raw in cases.items():
            with self.subTest(name), self.assertRaises(c.ParseError):
                c.parse_block(raw, c.TESTNET)

    def test_absurd_output_count_raises(self):
        tx = bytearray(coinbase_tx(b"\x51", []))
        count_at = tx.index(b"\xff\xff\xff\xff", 4 + 16 + 1 + 36) + 4
        tx[count_at : count_at + 1] = b"\xfe\xff\xff\xff\x7f"
        with self.assertRaises(c.ParseError):
            c.parse_block(block_with(bytes(tx)), c.TESTNET)


class CoinbaseScriptTests(unittest.TestCase):
    """Real scriptSig shapes seen on Testnet between heights 4354246 and 4414245."""

    CASES = {
        "zkcodexcoder": (
            "03c6704204f09f8cb87a6b636f646578636f646572",
            (4354246, "zakura", "zkcodexcoder", ""),
        ),
        "foundry printable extranonce": (
            "0300000004f09fa69304414f744f466f756e647279205a6361736820506f6f6c2023507269766163794d617474657273",
            (0, "zebra", FOUNDRY_TAG, "414f744f"),
        ),
        "mariana": (
            "0300000013f09fa6933a204d617269616e615472656e6368",
            (0, "zebra", "MarianaTrench", ""),
        ),
        "toalt23": ("0300000012f09f8cb83a20746f616c7432332054657374", (0, "zakura", "toalt23 Test", "")),
        "molepool": (
            "0300000014f09fa6933a202f6d6f6c65706f6f6c2e636f6d2f",
            (0, "zebra", "/molepool.com/", ""),
        ),
        "wolf auxpow": (
            "0300000018f09f90ba20576f6c663a20572e6361736820417578506f77fabe6d6d78418a4fad4d276ce2a5dcb15972d9d0ce909215d74d458afa71f15c6e65627e01000000ca0e0000",
            (
                0,
                None,
                "Wolf: W.cash AuxPow",
                "f09f90bafabe6d6d78418a4fad4d276ce2a5dcb15972d9d0ce909215d74d458afa71f15c6e65627e01000000ca0e0000",
            ),
        ),
        "open-krnx": (
            "030000002cfabe6d6d94827f4a0cba1d2d0be60a613d7c8c73f52b3986d71038fe75d7b6b19682199d0100000000000000"
            "2acf9725ad00000000000000002f6f70656e2d6b726e782d706f6f6c2f000000000000000000000000000004aebcb26a",
            (
                0,
                None,
                "/open-krnx-pool/",
                "2cfabe6d6d94827f4a0cba1d2d0be60a613d7c8c73f52b3986d71038fe75d7b6b19682199d0100000000000000"
                "2acf9725ad0000000000000000000000000000000000000000000004aebcb26a",
            ),
        ),
        "marker only": ("03724d4304f09fa693", (4410738, "zebra", "", "")),
        "empty": ("", (None, None, "", "")),
        "small height opcode": ("5a", (10, None, "", "")),
        "negative height push": ("0180", (None, None, "", "80")),
    }

    def test_real_shapes(self):
        for name, (script_hex, want) in self.CASES.items():
            with self.subTest(name):
                self.assertEqual(c.decode_coinbase_script(bytes.fromhex(script_hex)), want)

    def test_tag_is_capped(self):
        _, _, tag, _ = c.decode_coinbase_script(b"\x51" + b"a" * 300)
        self.assertEqual(tag, "a" * c.MAX_TAG_LEN)


def coinbase(tag: str = "", payouts=(), extranonce: str = "") -> c.Coinbase:
    """Build a Coinbase for identify_miner tests."""
    return c.Coinbase(
        height=1,
        script_sig=b"",
        template=None,
        tag=tag,
        extranonce=extranonce,
        payouts=tuple(payouts),
        tx_version=6,
    )


class IdentifyMinerTests(unittest.TestCase):
    def test_tag_families_are_case_insensitive(self):
        self.assertEqual(c.identify_miner(coinbase("FOUNDRY USA", [(FOUNDRY_ADDR, 1)]), c.TESTNET), "Foundry")
        self.assertEqual(c.identify_miner(coinbase("MarianaTrench"), c.TESTNET), "MarianaTrench")
        self.assertEqual(c.identify_miner(coinbase("Wolf: W.cash AuxPow"), c.TESTNET), "W.cash")

    def test_first_paid_non_funding_stream_address(self):
        payouts = [(FS, 12500000), ("script:6a00", 0), ("tmOther", 5), (FOUNDRY_ADDR, 9)]
        self.assertEqual(c.identify_miner(coinbase("some pool", payouts), c.TESTNET), "tmOther")

    def test_mainnet_funding_stream_is_skipped(self):
        payouts = [("t3cFfPt1Bcvgez9ZbMBFWeZsskxTkPzGCow", 1), ("t1fMAAnYrpwt1HQ8ZqxeFqVSSi6PQjwTLUm", 2)]
        self.assertEqual(c.identify_miner(coinbase("", payouts), c.MAINNET), "t1fMAAnYrpwt1HQ8ZqxeFqVSSi6PQjwTLUm")

    def test_shielded_fallbacks(self):
        fs_only = [(FS, 12500000)]
        self.assertEqual(c.identify_miner(coinbase("new pool", fs_only), c.TESTNET), "shielded:new pool")
        long_tag = "x" * 100
        self.assertEqual(c.identify_miner(coinbase(long_tag, fs_only), c.TESTNET), "shielded:" + "x" * 48)
        self.assertEqual(
            c.identify_miner(coinbase("", fs_only, extranonce="deadbeefcafe"), c.TESTNET), "shielded:deadbeef"
        )
        self.assertEqual(c.identify_miner(coinbase("", fs_only), c.TESTNET), "shielded:notag")
        self.assertEqual(c.identify_miner(None, c.TESTNET), "unknown")


class DifficultyTests(unittest.TestCase):
    def test_min_difficulty_block_4410737(self):
        parent = RPC_BLOCKS[4410736]
        block = RPC_BLOCKS[4410737]
        self.assertEqual(block["time"] - parent["time"], 451)
        self.assertTrue(c.is_min_difficulty(c.TESTNET, 4410737, block["time"], parent["time"]))
        self.assertEqual(block["bits"], c.TESTNET.pow_limit_bits)
        self.assertEqual(c.difficulty_from_bits(block["bits"], c.TESTNET), 1.0)
        # The child is dated 147 s before its min-difficulty parent.
        child = RPC_BLOCKS[4410738]
        self.assertFalse(c.is_min_difficulty(c.TESTNET, 4410738, child["time"], block["time"]))
        prev = [(parent["bits"], parent["time"])] * 28
        self.assertEqual(c.expected_bits(c.TESTNET, 4410737, block["time"], prev), 0x2007FFFF)

    def test_min_difficulty_rule_edges(self):
        self.assertFalse(c.is_min_difficulty(c.TESTNET, 4410737, 1450, 1000))  # gap must exceed 450
        self.assertTrue(c.is_min_difficulty(c.TESTNET, 4410737, 1451, 1000))
        self.assertFalse(c.is_min_difficulty(c.TESTNET, 299_187, 5000, 1000))
        self.assertTrue(c.is_min_difficulty(c.TESTNET, 299_188, 5000, 1000))
        self.assertFalse(c.is_min_difficulty(c.MAINNET, 3_000_000, 10_000, 1000))

    def test_expected_bits_over_headers_fixture(self):
        raw = json.loads((FIXTURES / "testnet-headers-4413473-4414272.json").read_text())
        chain = {int(h): (bits, time) for h, (bits, time) in raw.items()}
        first, last = min(chain), max(chain)
        self.assertEqual(last - first + 1, 800)
        predicted = min_diff = 0
        for height in range(first + 28, last + 1):
            prev = [chain[height - i] for i in range(1, 29)]
            bits, time = chain[height]
            self.assertEqual(c.expected_bits(c.TESTNET, height, time, prev), bits, height)
            predicted += 1
            min_diff += bits == c.TESTNET.pow_limit_bits
        self.assertEqual(predicted, 772)
        self.assertGreaterEqual(min_diff, 1)

    def test_expected_bits_needs_28_headers(self):
        with self.assertRaises(ValueError):
            c.expected_bits(c.TESTNET, 4410737, 0, [(0x1F765A75, 0)] * 27)

    def test_expected_bits_clamps_to_pow_limit(self):
        prev = [(0x2007FFFF, 10_000 - 1000 * i) for i in range(28)]  # very slow blocks
        self.assertEqual(c.expected_bits(c.TESTNET, 4410737, 10_001, prev), 0x2007FFFF)

    def test_bits_target_roundtrips(self):
        raw = json.loads((FIXTURES / "testnet-headers-4413473-4414272.json").read_text())
        for bits, _ in raw.values():
            self.assertEqual(c.target_to_bits(c.bits_to_target(bits)), bits)
        self.assertEqual(c.bits_to_target(0x2007FFFF), (1 << 251) - (1 << 232))
        self.assertEqual(c.target_to_bits(c.TESTNET.pow_limit), c.TESTNET.pow_limit_bits)
        self.assertEqual(c.target_to_bits(c.MAINNET.pow_limit), c.MAINNET.pow_limit_bits)
        self.assertEqual(c.target_to_bits(0x80), 0x02008000)  # mantissa sign bit forces a wider size
        self.assertEqual(c.bits_to_target(0x02008000), 0x80)
        self.assertEqual(c.bits_to_target(0x01120000), 0x12)
        self.assertEqual(c.target_to_bits(0x12), 0x01120000)

    def test_invalid_compact_values(self):
        for bits in (0, 0x04923456, 0x21010000, -1, 1 << 32, 0x03000000):
            with self.subTest(bits=bits), self.assertRaises(c.ParseError):
                c.bits_to_target(bits)
        for target in (0, -5, 1 << 256):
            with self.subTest(target=target), self.assertRaises(ValueError):
                c.target_to_bits(target)

    def test_work_and_difficulty(self):
        self.assertEqual(c.work_from_bits(0x2007FFFF), 32)
        for want in RPC_BLOCKS.values():
            target = c.bits_to_target(want["bits"])
            self.assertEqual(c.work_from_bits(want["bits"]), (1 << 256) // (target + 1))
            self.assertTrue(math.isclose(c.difficulty_from_bits(want["bits"], c.TESTNET), want["difficulty"], rel_tol=1e-12))
        self.assertGreater(c.work_from_bits(0x1E0EC6E4), c.work_from_bits(0x1F765A75))
        self.assertEqual(c.difficulty_from_bits(c.MAINNET.pow_limit_bits, c.MAINNET), 1.0)


class EncodingTests(unittest.TestCase):
    def test_compact_size_roundtrip(self):
        for n in (0, 1, 0xFC, 0xFD, 0xFFFF, 0x1_0000, 0xFFFF_FFFF, 0x1_0000_0000, (1 << 64) - 1):
            encoded = c.write_compact_size(n)
            self.assertEqual(c.read_compact_size(b"\x00" + encoded + b"\x00", 1), (n, 1 + len(encoded)))
        self.assertEqual(len(c.write_compact_size(0xFC)), 1)
        self.assertEqual(len(c.write_compact_size(0xFD)), 3)
        for n in (-1, 1 << 64):
            with self.assertRaises(ValueError):
                c.write_compact_size(n)

    def test_compact_size_rejects_bad_encodings(self):
        non_canonical_u64 = b"\xff" + (0xFFFF_FFFF).to_bytes(8, "little")
        for raw in (b"", b"\xfd\x01", b"\xfd\xfc\x00", b"\xfe\xff\xff\x00\x00", b"\xff" + bytes(7), non_canonical_u64):
            with self.subTest(raw=raw.hex()), self.assertRaises(c.ParseError):
                c.read_compact_size(raw, 0)
        with self.assertRaises(c.ParseError):
            c.read_compact_size(b"\x01", 1)

    def test_encode_address_matches_rpc(self):
        self.assertEqual(c.encode_address(c.TESTNET.p2pkh_prefix, P2PKH_FOUNDRY[3:23]), FOUNDRY_ADDR)
        self.assertEqual(c.encode_address(c.TESTNET.p2sh_prefix, P2SH_FS[2:22]), FS)
        self.assertEqual(c.encode_address(b"\x00", bytes(20)), "1111111111111111111114oLvT2")

    def test_address_prefixes(self):
        cases = {
            FS: c.TESTNET.p2sh_prefix,
            FOUNDRY_ADDR: c.TESTNET.p2pkh_prefix,
            "t3cFfPt1Bcvgez9ZbMBFWeZsskxTkPzGCow": c.MAINNET.p2sh_prefix,
            "t1fMAAnYrpwt1HQ8ZqxeFqVSSi6PQjwTLUm": c.MAINNET.p2pkh_prefix,
        }
        for address, prefix in cases.items():
            with self.subTest(address):
                payload = base58check_decode(address)
                self.assertEqual(payload[:2], prefix)
                self.assertEqual(c.encode_address(payload[:2], payload[2:]), address)

    def test_raw_hash_key(self):
        display = RPC_BLOCKS[4410736]["hash"]
        self.assertEqual(c.raw_hash_key(display), bytes.fromhex(display)[::-1])
        self.assertEqual(c.parse_header(fixture_block(4410737))[0].raw[4:36], c.raw_hash_key(display))
        low, high = "ff" + "00" * 31, "00" * 31 + "01"
        self.assertGreater(c.raw_hash_key(high), c.raw_hash_key(low))  # compares the last display byte first
        for bad in ("00" * 31, "zz" * 32):
            with self.assertRaises(ValueError):
                c.raw_hash_key(bad)

    def test_sha256d_and_network_params(self):
        self.assertEqual(c.sha256d(b"")[:4].hex(), "5df6e0e2")
        self.assertIs(c.NETWORKS["testnet"], c.TESTNET)
        self.assertEqual(c.TESTNET.averaging_window_timespan, 1275)
        self.assertEqual(c.TESTNET.min_timespan, 1275 * 84 // 100)
        self.assertEqual(c.TESTNET.max_timespan, 1275 * 132 // 100)
        self.assertEqual(c.MAINNET.magic.hex(), "24e92764")


if __name__ == "__main__":
    unittest.main()
