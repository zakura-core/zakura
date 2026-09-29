"""Tests for zakura_fork_monitor.wire message framing and payload codecs."""

from __future__ import annotations

import ipaddress
import struct
import unittest
from pathlib import Path

from zakura_fork_monitor import consensus as c
from zakura_fork_monitor import wire as w


FIXTURES = Path(__file__).with_name("fixtures")
MAGIC = c.TESTNET.magic
HASH_A = "00000d8cb27b2e95b223afe7325fe411a4b355b82e5523ad09eff014112bb644"
HASH_B = "00003e268eb0b585a2d6ade5ef2d1e098c3e35b247dcca07e18cf73033406804"
HASH_C = "005e7e8ddca530f2601f2207930aefb412c796c304ad991f86fa0a5c055be611"


def fixture_header(height: int) -> bytes:
    """Return the 1487-byte header of a fixture block."""
    raw = bytes.fromhex((FIXTURES / f"block-test-{height}.hex").read_text().strip())
    return raw[: c.BLOCK_HEADER_LEN]


def sample_version(**overrides) -> w.VersionMsg:
    """Return the version message the observer would send, with optional overrides."""
    fields = dict(
        version=w.PROTOCOL_VERSION,
        services=0,
        timestamp=1790633447,
        recv_ip="167.99.103.111",
        recv_port=18233,
        nonce=0x1122334455667788,
        user_agent=w.USER_AGENT,
        start_height=4414424,
        relay=False,
    )
    fields.update(overrides)
    return w.VersionMsg(**fields)


class FramingTests(unittest.TestCase):
    def test_verack_bytes(self):
        want = bytes.fromhex("fa1af9bf" "76657261636b000000000000" "00000000" "5df6e0e2")
        self.assertEqual(w.frame(MAGIC, "verack", b""), want)

    def test_roundtrip_and_checksum(self):
        payload = w.encode_ping(42)
        message = w.frame(MAGIC, "ping", payload)
        command, length, checksum = w.parse_frame_header(MAGIC, message[: w.HEADER_LEN])
        self.assertEqual((command, length), ("ping", 8))
        self.assertEqual(message[w.HEADER_LEN :], payload)
        self.assertTrue(w.verify_checksum(payload, checksum))
        self.assertFalse(w.verify_checksum(w.encode_ping(43), checksum))

    def test_unknown_commands_parse_so_callers_can_skip_them(self):
        message = w.frame(MAGIC, "sendaddrv2", b"")
        self.assertEqual(w.parse_frame_header(MAGIC, message[:24])[0], "sendaddrv2")

    def test_parse_frame_header_rejects(self):
        good = w.frame(MAGIC, "headers", b"\x00")[:24]
        bad_command = good[:4] + b"head\x00rs\x00\x00\x00\x00\x00" + good[16:]
        empty_command = good[:4] + bytes(12) + good[16:]
        control_command = good[:4] + b"head\x01" + bytes(7) + good[16:]
        too_long = good[:16] + struct.pack("<I", w.MAX_PAYLOAD + 1) + good[20:]
        cases = {
            "wrong magic": (c.MAINNET.magic, good),
            "short header": (MAGIC, good[:23]),
            "bytes after NUL": (MAGIC, bad_command),
            "empty command": (MAGIC, empty_command),
            "control byte": (MAGIC, control_command),
            "oversize": (MAGIC, too_long),
        }
        for name, (magic, header) in cases.items():
            with self.subTest(name), self.assertRaises(w.WireError):
                w.parse_frame_header(magic, header)

    def test_pre_handshake_limit(self):
        header = w.frame(MAGIC, "version", bytes(w.MAX_PAYLOAD_PRE_HANDSHAKE + 1))[:24]
        self.assertEqual(w.parse_frame_header(MAGIC, header)[1], w.MAX_PAYLOAD_PRE_HANDSHAKE + 1)
        with self.assertRaises(w.WireError):
            w.parse_frame_header(MAGIC, header, max_payload=w.MAX_PAYLOAD_PRE_HANDSHAKE)

    def test_frame_rejects(self):
        for magic, command, payload in (
            (MAGIC, "getheadersxyz", b""),
            (MAGIC, "", b""),
            (MAGIC, "pïng", b""),
            (MAGIC, "has space", b""),
            (MAGIC[:3], "ping", b""),
            (MAGIC, "block", bytes(w.MAX_PAYLOAD + 1)),
        ):
            with self.subTest(command=command, magic=magic.hex()), self.assertRaises(w.WireError):
                w.frame(magic, command, payload)


class VersionTests(unittest.TestCase):
    def test_roundtrip_and_layout(self):
        msg = sample_version()
        payload = w.encode_version(msg)
        self.assertEqual(len(payload), 86 + len(w.USER_AGENT))
        self.assertEqual(struct.unpack_from("<IQq", payload, 0), (170190, 0, 1790633447))
        self.assertEqual(struct.unpack_from("<Q", payload, 20)[0], w.NODE_NETWORK)
        self.assertEqual(payload[28:44], bytes(10) + b"\xff\xff" + bytes([167, 99, 103, 111]))
        self.assertEqual(payload[44:46], (18233).to_bytes(2, "big"))
        self.assertEqual(payload[46:72], bytes(8) + bytes(10) + b"\xff\xff" + bytes(4) + bytes(2))
        self.assertEqual(payload[-1], 0)
        self.assertEqual(w.decode_version(payload), msg)

    def test_ipv6_and_relay_roundtrip(self):
        msg = sample_version(recv_ip="2001:db8::1", relay=True, services=w.NODE_NETWORK | w.NODE_P2P_V2)
        self.assertEqual(w.decode_version(w.encode_version(msg)), msg)

    def test_decode_tolerates_missing_relay_and_trailing_bytes(self):
        payload = w.encode_version(sample_version())
        self.assertTrue(w.decode_version(payload[:-1]).relay)
        self.assertFalse(w.decode_version(payload + b"\x07\x07").relay)

    def test_decode_replaces_invalid_utf8(self):
        payload = bytearray(w.encode_version(sample_version(user_agent="/ab/")))
        payload[82] = 0xFF  # the 'a' after the length byte and '/'
        self.assertEqual(w.decode_version(bytes(payload)).user_agent, "/�b/")

    def test_decode_rejects(self):
        payload = w.encode_version(sample_version())
        long_ua = payload[:80] + c.write_compact_size(257) + b"a" * 257 + bytes(5)
        cases = {
            "invalid relay byte": payload[:-1] + b"\x02",
            "truncated fixed part": payload[:79],
            "truncated user agent": payload[:90],
            "truncated start height": payload[:-3],
            "user agent over 256 bytes": long_ua,
        }
        for name, raw in cases.items():
            with self.subTest(name), self.assertRaises(w.WireError):
                w.decode_version(raw)

    def test_encode_rejects(self):
        for overrides in (
            {"user_agent": "/" + "a" * 256 + "/"},
            {"user_agent": "/ünïcode:1/"},
            {"start_height": -1},
            {"recv_ip": "not-an-ip"},
            {"recv_port": 70000},
        ):
            with self.subTest(overrides=overrides), self.assertRaises(w.WireError):
                w.encode_version(sample_version(**overrides))


class PingTests(unittest.TestCase):
    def test_ping_roundtrip(self):
        for nonce in (0, 1, (1 << 64) - 1):
            self.assertEqual(w.decode_ping(w.encode_ping(nonce)), nonce)
        with self.assertRaises(w.WireError):
            w.decode_ping(b"\x00" * 7)
        with self.assertRaises(w.WireError):
            w.encode_ping(1 << 64)


class GetHeadersTests(unittest.TestCase):
    def test_locator_order_and_byte_order(self):
        payload = w.encode_getheaders(170160, [HASH_C, HASH_B, HASH_A])
        self.assertEqual(len(payload), 4 + 1 + 3 * 32 + 32)
        self.assertEqual(struct.unpack_from("<I", payload)[0], 170160)
        self.assertEqual(payload[4], 3)
        hashes = [payload[5 + 32 * i : 37 + 32 * i] for i in range(3)]
        self.assertEqual(hashes, [bytes.fromhex(h)[::-1] for h in (HASH_C, HASH_B, HASH_A)])
        self.assertEqual(payload[-32:], bytes(32))

    def test_stop_hash(self):
        payload = w.encode_getheaders(170190, [HASH_A], stop=HASH_B)
        self.assertEqual(payload[-32:], bytes.fromhex(HASH_B)[::-1])

    def test_locator_cap_and_validation(self):
        self.assertEqual(w.encode_getheaders(170190, [HASH_A] * 101)[4], 101)
        with self.assertRaises(w.WireError):
            w.encode_getheaders(170190, [HASH_A] * 102)
        for bad in ("zz" * 32, HASH_A[:-2], ""):
            with self.subTest(bad=bad), self.assertRaises(w.WireError):
                w.encode_getheaders(170190, [bad])


class HeadersTests(unittest.TestCase):
    def payload(self, raws: list[bytes], count: int | None = None) -> bytes:
        """Build a `headers` payload with a zero tx-count byte after each header."""
        n = len(raws) if count is None else count
        return c.write_compact_size(n) + b"".join(raw + b"\x00" for raw in raws)

    def test_decode_fixture_headers(self):
        raws = [fixture_header(h) for h in (4410736, 4410737, 4410738)]
        headers = w.decode_headers(self.payload(raws))
        self.assertEqual([h.hash for h in headers], [HASH_A, HASH_B, HASH_C])
        self.assertEqual(headers[1].prev_hash, HASH_A)
        self.assertEqual(headers[1].bits, 0x2007FFFF)

    def test_empty_headers(self):
        self.assertEqual(w.encode_empty_headers(), b"\x00")
        self.assertEqual(w.decode_headers(w.encode_empty_headers()), [])

    def test_decode_rejects(self):
        raw = fixture_header(4410736)
        bad_solution = bytearray(raw)
        bad_solution[141] = 0
        cases = {
            "161 headers": self.payload([], count=161),
            "truncated": self.payload([raw])[:-2],
            "missing tx count": c.write_compact_size(1) + raw,
            "count larger than body": self.payload([raw], count=2),
            "bad solution size": self.payload([bytes(bad_solution)]),
            "non-canonical count": b"\xfd\x01\x00" + raw + b"\x00",
        }
        for name, payload in cases.items():
            with self.subTest(name), self.assertRaises(w.WireError):
                w.decode_headers(payload)


class InventoryTests(unittest.TestCase):
    def test_mixed_entry_sizes(self):
        block = bytes.fromhex(HASH_A)[::-1]
        wtx = bytes(range(64))
        tx = bytes(range(32))
        items = [(w.INV_BLOCK, block), (w.INV_WTX, wtx), (w.INV_TX, tx)]
        payload = w.encode_inv(items)
        self.assertEqual(len(payload), 1 + 36 + 68 + 36)
        self.assertEqual(w.decode_inv(payload), items)
        self.assertEqual(w.decode_notfound(payload), items)
        self.assertEqual(w.inv_block_hashes(items), [HASH_A])

    def test_decode_rejects(self):
        entry = struct.pack("<I", w.INV_BLOCK) + bytes(32)
        cases = {
            "unknown type 4": b"\x01" + struct.pack("<I", 4) + bytes(32),
            "unknown type 0x40000002": b"\x01" + struct.pack("<I", 0x40000002) + bytes(32),
            "count over 50000": c.write_compact_size(50_001) + entry,
            "count larger than body": b"\x02" + entry,
            "truncated wtx entry": b"\x01" + struct.pack("<I", w.INV_WTX) + bytes(32),
            "empty payload": b"",
        }
        for name, payload in cases.items():
            with self.subTest(name), self.assertRaises(w.WireError):
                w.decode_inv(payload)

    def test_encode_rejects(self):
        for items in ([(4, bytes(32))], [(w.INV_WTX, bytes(32))], [(w.INV_BLOCK, bytes(31))]):
            with self.subTest(items=items), self.assertRaises(w.WireError):
                w.encode_inv(items)

    def test_getdata_blocks(self):
        payload = w.encode_getdata_blocks([HASH_A, HASH_B])
        self.assertEqual(payload, w.encode_inv([(w.INV_BLOCK, bytes.fromhex(h)[::-1]) for h in (HASH_A, HASH_B)]))
        self.assertEqual(w.inv_block_hashes(w.decode_inv(payload)), [HASH_A, HASH_B])
        for hashes in ([], [HASH_A] * 17, ["xyz"]):
            with self.subTest(n=len(hashes)), self.assertRaises(w.WireError):
                w.encode_getdata_blocks(hashes)

    def test_empty_replies(self):
        self.assertEqual(w.encode_empty_inv(), b"\x00")
        self.assertEqual(w.encode_empty_addr(), b"\x00")
        self.assertEqual(w.decode_inv(w.encode_empty_inv()), [])


def addr_entry(time: int, services: int, ip: str, port: int) -> bytes:
    """Serialize one `addr` v1 entry."""
    addr = ipaddress.ip_address(ip)
    packed = bytes(10) + b"\xff\xff" + addr.packed if addr.version == 4 else addr.packed
    return struct.pack("<IQ", time, services) + packed + struct.pack(">H", port)


def addrv2_entry(time: int, services: int, network_id: int, addr: bytes, port: int) -> bytes:
    """Serialize one ZIP 155 `addrv2` entry."""
    return (
        struct.pack("<I", time)
        + c.write_compact_size(services)
        + bytes((network_id,))
        + c.write_compact_size(len(addr))
        + addr
        + struct.pack(">H", port)
    )


class AddrTests(unittest.TestCase):
    def test_decode_addr(self):
        payload = b"\x02" + addr_entry(1790633447, 1, "1.2.3.4", 18233) + addr_entry(5, 0x1000001, "2001:db8::5", 8233)
        self.assertEqual(
            w.decode_addr(payload),
            [("1.2.3.4", 18233, 1, 1790633447), ("2001:db8::5", 8233, 0x1000001, 5)],
        )

    def test_decode_addr_rejects(self):
        entry = addr_entry(1, 1, "1.2.3.4", 1)
        for name, payload in {
            "count over 1000": c.write_compact_size(1001) + entry,
            "truncated": b"\x01" + entry[:-1],
            "empty": b"",
        }.items():
            with self.subTest(name), self.assertRaises(w.WireError):
                w.decode_addr(payload)

    def test_decode_addrv2(self):
        mapped = ipaddress.IPv6Address("::ffff:9.8.7.6").packed
        payload = b"\x04" + b"".join(
            (
                addrv2_entry(10, 1, 1, bytes([1, 2, 3, 4]), 18233),
                addrv2_entry(11, 0x1000001, 2, ipaddress.IPv6Address("2001:db8::7").packed, 8233),
                addrv2_entry(12, 1, 4, bytes(32), 9050),  # TorV3: skipped
                addrv2_entry(13, 0, 2, mapped, 1),
            )
        )
        self.assertEqual(
            w.decode_addrv2(payload),
            [
                ("1.2.3.4", 18233, 1, 10),
                ("2001:db8::7", 8233, 0x1000001, 11),
                ("9.8.7.6", 1, 0, 13),
            ],
        )

    def test_decode_addrv2_rejects(self):
        for name, payload in {
            "ipv4 with 16 bytes": b"\x01" + addrv2_entry(1, 1, 1, bytes(16), 1),
            "ipv6 with 4 bytes": b"\x01" + addrv2_entry(1, 1, 2, bytes(4), 1),
            "address over 512 bytes": b"\x01" + addrv2_entry(1, 1, 7, bytes(513), 1),
            "truncated port": b"\x01" + addrv2_entry(1, 1, 1, bytes(4), 1)[:-1],
            "count over 1000": c.write_compact_size(1001),
            "count larger than body": b"\x02" + addrv2_entry(1, 1, 1, bytes(4), 1),
        }.items():
            with self.subTest(name), self.assertRaises(w.WireError):
                w.decode_addrv2(payload)


class UserAgentTests(unittest.TestCase):
    def test_known_implementations(self):
        cases = {
            "/Zebra:6.4.2/": ("zebra", "6.4.2"),
            "/Zakura:1.5.0-rc0/": ("zakura", "1.5.0-rc0"),
            "/MagicBean:6.0.0/": ("zcashd", "6.0.0"),
            "/zeeder:0.3.0/": ("zeeder", "0.3.0"),
            w.USER_AGENT: ("monitor", "0.1.0"),
            "/Zebra:6.4.2-modified/": ("zebra", "6.4.2-modified"),
            "/MagicBean:5.4.2(bitcore)/": ("zcashd", "5.4.2"),
        }
        for ua, want in cases.items():
            with self.subTest(ua):
                self.assertEqual(w.classify_user_agent(ua), want)

    def test_other_and_hostile_user_agents(self):
        self.assertEqual(w.classify_user_agent("/Satoshi:0.21.0/"), ("other", "0.21.0"))
        self.assertEqual(w.classify_user_agent(""), ("other", ""))
        self.assertEqual(w.classify_user_agent("no slashes"), ("other", ""))
        self.assertEqual(w.classify_user_agent("/Zebra:6.4<b>x</b>/"), ("zebra", "6.4bx"))
        self.assertEqual(w.classify_user_agent("/Zakura:" + "9" * 10_000 + "/"), ("zakura", "9" * 32))
        self.assertEqual(w.classify_user_agent("/Zakura:" + "1" * 60 + "/"), ("zakura", "1" * 32))


if __name__ == "__main__":
    unittest.main()
