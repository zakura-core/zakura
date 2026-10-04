"""Tests for TOML config parsing, validation and CLI overrides."""

from __future__ import annotations

import tempfile
import tomllib
import unittest
from pathlib import Path

from zakura_fork_monitor.config import (
    DEFAULT_DB,
    DEFAULT_DNS_SEEDS,
    Config,
    load_config,
    parse_config,
    split_host_port,
    with_overrides,
)

EXAMPLE = Path(__file__).resolve().parents[1] / "fork-monitor.testnet.toml"


def parse(text: str) -> Config:
    """Parse a TOML snippet into a Config."""
    return parse_config(tomllib.loads(text), "test.toml")


class ExampleConfigTests(unittest.TestCase):
    """The shipped testnet example parses to the documented values."""

    def test_example_file(self) -> None:
        """Every section of the example file lands in the dataclasses."""
        config = load_config(EXAMPLE)
        self.assertEqual(config.network, "testnet")
        self.assertEqual(config.db, DEFAULT_DB)
        self.assertEqual((config.http.host, config.http.port), ("127.0.0.1", 8093))
        self.assertEqual(
            (config.chain.backfill_blocks, config.chain.memory_window, config.chain.settle_depth), (20000, 30000, 3)
        )
        fleet = ["zakura-testnet-1", "zakura-testnet-as", "zakura-testnet-eu"]
        self.assertEqual([e.name for e in config.rpc], [*fleet, "tazminer"])
        tazminer = config.rpc[-1]
        self.assertEqual((tazminer.interval, tazminer.fleet, tazminer.backfill), (5.0, False, False))
        self.assertEqual((tazminer.host, tazminer.source), ("lwd.tazminer.com", "rpc:tazminer"))
        self.assertTrue(all(e.backfill and e.fleet and e.interval == 1.0 for e in config.rpc[:-1]))
        self.assertEqual(config.fleet_hosts, frozenset({"167.99.103.111", "206.189.148.0", "164.92.209.78"}))
        self.assertTrue(config.p2p.enabled)
        self.assertEqual((config.p2p.max_peers, config.p2p.connect_rate, config.p2p.poll_interval), (300, 5.0, 15.0))
        self.assertEqual(config.p2p.probe_sample, 0.2)
        self.assertEqual(config.p2p.dns_seeds, ("dnsseed.testnet.z.cash", "testnet.seeder.zfnd.org"))
        self.assertEqual(config.p2p.static_peers, ())
        self.assertTrue(config.cipherscan.enabled)
        self.assertEqual(config.cipherscan.base_url, "https://api.testnet.cipherscan.app")
        self.assertEqual((config.cipherscan.interval, config.cipherscan.backfill_pages), (60.0, 0))
        self.assertEqual(config.retention.days, 30)
        self.assertEqual(config.retention.seconds, 30 * 86_400)

    def test_example_matches_defaults_except_rpc(self) -> None:
        """The example only spells out defaults apart from its RPC endpoints."""
        config = load_config(EXAMPLE)
        self.assertEqual(Config(rpc=config.rpc), config)


class DefaultTests(unittest.TestCase):
    """Defaults and network-dependent defaults."""

    def test_empty_document_uses_defaults(self) -> None:
        """An empty file is a valid P2P-only testnet config."""
        self.assertEqual(parse(""), Config())

    def test_rpc_defaults(self) -> None:
        """Optional [[rpc]] keys take the collector defaults."""
        endpoint = parse('[[rpc]]\nname = "a"\nurl = "https://example.org:8232/"').rpc[0]
        self.assertEqual(
            (endpoint.kind, endpoint.interval, endpoint.fleet, endpoint.backfill),
            ("zakura", 1.0, False, True),
        )
        self.assertEqual(
            (endpoint.chaintips_interval, endpoint.peerinfo_interval, endpoint.timeout), (3.0, 120.0, 10.0)
        )

    def test_mainnet_defaults(self) -> None:
        """Mainnet gets mainnet DNS seeds and CipherScan off unless configured."""
        config = parse('network = "mainnet"')
        self.assertEqual(config.p2p.dns_seeds, DEFAULT_DNS_SEEDS["mainnet"])
        self.assertFalse(config.cipherscan.enabled)
        configured = parse('network = "mainnet"\n[cipherscan]\nbase_url = "https://example.org"')
        self.assertTrue(configured.cipherscan.enabled)

    def test_integers_accepted_for_float_fields(self) -> None:
        """TOML integers are fine where a float is expected."""
        self.assertEqual(parse("[p2p]\npoll_interval = 30").p2p.poll_interval, 30.0)


class ValidationTests(unittest.TestCase):
    """Every invalid input exits with a message naming the key."""

    def assert_exit(self, text: str, *fragments: str) -> None:
        """Assert that parsing `text` raises SystemExit mentioning each fragment."""
        with self.assertRaises(SystemExit) as caught:
            parse(text)
        message = str(caught.exception)
        self.assertIn("test.toml", message)
        for fragment in fragments:
            self.assertIn(fragment, message)

    def test_unknown_keys(self) -> None:
        """Unknown keys are rejected at every level."""
        self.assert_exit("colour = 1", "unknown top-level key", "colour")
        self.assert_exit("[http]\nhots = 'x'", "unknown key(s) in http", "hots")
        self.assert_exit("[p2p]\nmax_peer = 3", "p2p", "max_peer")
        self.assert_exit('[[rpc]]\nname = "a"\nurl = "http://h/"\npassword = "x"', "rpc[0]", "password")
        self.assert_exit("[retention]\nweeks = 1", "retention", "weeks")

    def test_duplicate_rpc_names(self) -> None:
        """Two endpoints may not share a name (it keys the source row)."""
        self.assert_exit(
            '[[rpc]]\nname = "a"\nurl = "http://h1/"\n[[rpc]]\nname = "a"\nurl = "http://h2/"', "duplicate rpc name"
        )

    def test_rpc_required_keys_and_shape(self) -> None:
        """name and url are required and [[rpc]] must be an array of tables."""
        self.assert_exit('[[rpc]]\nurl = "http://h/"', "rpc[0]", "name")
        self.assert_exit('[[rpc]]\nname = "a"', "rpc[0]", "url")
        self.assert_exit('[rpc]\nname = "a"\nurl = "http://h/"', "array of tables")
        self.assert_exit('[[rpc]]\nname = "bad name"\nurl = "http://h/"', "rpc[0].name")
        self.assert_exit('[[rpc]]\nname = "a"\nurl = "http://h/"\nkind = "lnd"', "rpc[0].kind")

    def test_url_schemes(self) -> None:
        """Only absolute http(s) URLs with a host and valid port are accepted."""
        for url in ("ftp://h/", "h:18232", "http:///path", "http://h:99999/", "http://h:0/", "file:///etc/passwd"):
            with self.subTest(url=url):
                self.assert_exit(f'[[rpc]]\nname = "a"\nurl = "{url}"', "rpc[0].url")
        self.assert_exit('[cipherscan]\nbase_url = "gopher://x"', "cipherscan.base_url")

    def test_numeric_ranges_and_types(self) -> None:
        """Out-of-range numbers, bools for numbers and strings for numbers are rejected."""
        cases = [
            ("[http]\nport = 70000", "http.port"),
            ("[http]\nport = true", "http.port"),
            ('[http]\nport = "8093"', "http.port"),
            ("[chain]\nsettle_depth = -1", "chain.settle_depth"),
            ("[chain]\nmemory_window = 10", "chain.memory_window"),
            ("[p2p]\nprobe_sample = 1.5", "p2p.probe_sample"),
            ("[p2p]\nconnect_rate = 0", "p2p.connect_rate"),
            ("[p2p]\nmax_peers = 100000", "p2p.max_peers"),
            ("[p2p]\npoll_interval = nan", "p2p.poll_interval"),
            ("[p2p]\nenabled = 1", "p2p.enabled"),
            ('[[rpc]]\nname = "a"\nurl = "http://h/"\ninterval = 0.0', "rpc[0].interval"),
            ("[cipherscan]\nbackfill_pages = -1", "cipherscan.backfill_pages"),
            ("[retention]\ndays = 0", "retention.days"),
            ("[retention]\ndays = 1.5", "retention.days"),
        ]
        for text, key in cases:
            with self.subTest(text=text):
                self.assert_exit(text, key)

    def test_network_and_sections(self) -> None:
        """network must be known and sections must be tables."""
        self.assert_exit('network = "regtest"', "network")
        self.assert_exit('http = "x"', "http must be a table")
        self.assert_exit('db = ""', "db")

    def test_peer_lists(self) -> None:
        """Seeds and static peers are validated host[:port] strings."""
        config = parse('[p2p]\nstatic_peers = ["1.2.3.4:18233", "[2001:db8::1]:18233", "peer.example.org"]')
        self.assertEqual(len(config.p2p.static_peers), 3)
        self.assert_exit('[p2p]\nstatic_peers = ["1.2.3.4:99999"]', "p2p.static_peers[0]")
        self.assert_exit('[p2p]\nstatic_peers = ["bad host"]', "p2p.static_peers[0]")
        self.assert_exit('[p2p]\ndns_seeds = "dnsseed.z.cash"', "p2p.dns_seeds")
        self.assert_exit('[p2p]\ndns_seeds = ["a/b"]', "p2p.dns_seeds[0]")

    def test_nothing_to_monitor(self) -> None:
        """P2P disabled with no RPC endpoints leaves nothing to observe."""
        self.assert_exit("[p2p]\nenabled = false", "nothing to monitor")


class LoadAndOverrideTests(unittest.TestCase):
    """File loading errors and CLI overrides."""

    def test_missing_and_invalid_files(self) -> None:
        """Missing files and TOML syntax errors exit with the path."""
        with tempfile.TemporaryDirectory() as tmp:
            missing = Path(tmp) / "nope.toml"
            with self.assertRaises(SystemExit) as caught:
                load_config(missing)
            self.assertIn("not found", str(caught.exception))
            broken = Path(tmp) / "broken.toml"
            broken.write_text('name = "a"; url = "b"\n')
            with self.assertRaises(SystemExit) as caught:
                load_config(broken)
            self.assertIn("invalid TOML", str(caught.exception))

    def test_with_overrides(self) -> None:
        """CLI flags replace file values and None keeps them."""
        config = load_config(EXAMPLE)
        self.assertEqual(with_overrides(config), config)
        overridden = with_overrides(config, db="/tmp/m.sqlite3", host="0.0.0.0", port=0)
        self.assertEqual((overridden.db, overridden.http.host, overridden.http.port), ("/tmp/m.sqlite3", "0.0.0.0", 0))
        self.assertEqual(overridden.rpc, config.rpc)
        for kwargs in ({"port": 65536}, {"host": "bad host"}, {"db": ""}):
            with self.subTest(kwargs=kwargs), self.assertRaises(SystemExit):
                with_overrides(config, **kwargs)

    def test_split_host_port(self) -> None:
        """host[:port] parsing covers IPv4, bracketed and bare IPv6, and names."""
        self.assertEqual(split_host_port("1.2.3.4:8233", 18233), ("1.2.3.4", 8233))
        self.assertEqual(split_host_port("1.2.3.4", 18233), ("1.2.3.4", 18233))
        self.assertEqual(split_host_port("[2001:db8::1]:8233", 18233), ("2001:db8::1", 8233))
        self.assertEqual(split_host_port("[2001:db8::1]", 18233), ("2001:db8::1", 18233))
        self.assertEqual(split_host_port("2001:db8::1", 18233), ("2001:db8::1", 18233))
        for bad in ("", "h:", "h:0", "h:x", "[::1", "[::1]x", "a b", "h:123456"):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                split_host_port(bad, 18233)


if __name__ == "__main__":
    unittest.main()
