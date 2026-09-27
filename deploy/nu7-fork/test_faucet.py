"""Tests for public claim allocation, without a node or miner key."""

import http.client
import json
import threading
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import faucet
from faucet import Faucet, PAYOUT_ZAT
from test_dashboard import max_concurrent_handlers


class FaucetClaimsTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.faucet = Faucet(
            Path(self.temporary.name) / "claims.sqlite3", 18232, "miner", Path("sender"), Path("config")
        )
        self.address = "utest1" + "a" * 100
        self.patches = [
            patch.object(self.faucet, "validate_address"),
            patch.object(self.faucet, "funding_status", return_value={"ready": True, "maturedOutputs": 5}),
            patch("faucet.MIN_CLAIM_SPACING_SECONDS", 0),
        ]
        for item in self.patches:
            item.start()
            self.addCleanup(item.stop)

    def test_one_claim_per_address_and_two_per_ip(self):
        claim = self.faucet.reserve(self.address, "192.0.2.1")
        self.assertEqual(self.faucet.claim(claim)["status"], "queued")
        with self.assertRaisesRegex(PermissionError, "address"):
            self.faucet.reserve(self.address, "192.0.2.2")
        self.faucet.reserve(self.address + "b", "192.0.2.1")
        with self.assertRaisesRegex(PermissionError, "connection"):
            self.faucet.reserve(self.address + "c", "192.0.2.1")

    def test_daily_cap_counts_reserved_claims(self):
        with patch("faucet.IP_CLAIMS_PER_DAY", 1000), patch("faucet.MAX_QUEUE", 1000):
            for index in range(100):
                self.faucet.reserve(f"{self.address}{index}", f"192.0.2.{index + 1}")
            with self.assertRaisesRegex(PermissionError, "allocation"):
                self.faucet.reserve(self.address + "extra", "198.51.100.1")
        self.assertEqual(PAYOUT_ZAT * 100, 1_000_000_000)

    def test_processing_claim_requires_review_after_restart(self):
        claim = self.faucet.reserve(self.address, "192.0.2.1")
        with self.faucet.connect() as connection:
            connection.execute("UPDATE claims SET status = 'processing' WHERE id = ?", (claim,))
        restarted = Faucet(self.faucet.db, 18232, "miner", Path("sender"), Path("config"))
        self.assertEqual(restarted.claim(claim)["status"], "review")

    def test_invalid_requests_are_throttled_before_rpc_validation(self):
        with patch.object(self.faucet, "validate_address", side_effect=ValueError("invalid")) as validate:
            for _ in range(10):
                with self.assertRaisesRegex(ValueError, "invalid"):
                    self.faucet.reserve("bad", "192.0.2.1")
            with self.assertRaisesRegex(PermissionError, "Too many"):
                self.faucet.reserve("bad", "192.0.2.1")
            self.assertEqual(validate.call_count, 10)


class FaucetCorsTest(unittest.TestCase):
    def setUp(self):
        from unittest.mock import Mock
        self.fake = Mock()
        self.fake.status.return_value = {"ready": True}
        self.fake.reserve.return_value = "a" * 24
        self.fake.claim.return_value = {"status": "sent", "txid": "b" * 64}
        handler = type("TestHandler", (faucet.Handler,), {"faucet": self.fake,
                       "log_message": lambda *args: None})
        self.server = faucet.BoundedHTTPServer(("127.0.0.1", 0), handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.addCleanup(self.close)

    def close(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def request(self, method, path, origin=None, body=None):
        headers = {"Content-Type": "application/json"}
        if origin is not None:
            headers["Origin"] = origin
        connection = http.client.HTTPConnection(*self.server.server_address, timeout=3)
        try:
            connection.request(method, path, body, headers)
            response = connection.getresponse()
            return response.status, dict(response.getheaders()), response.read()
        finally:
            connection.close()

    def test_both_sites_can_preflight_submit_and_read_receipts(self):
        for origin in faucet.ALLOWED_ORIGINS:
            for method, path, body, expected in [
                ("OPTIONS", "/v1/faucet/claim", None, 204),
                ("GET", "/v1/faucet/status", None, 200),
                ("POST", "/v1/faucet/claim", json.dumps({"address": "utest1" + "a" * 100}), 202),
                ("GET", "/v1/faucet/claim/" + "a" * 24, None, 200),
            ]:
                with self.subTest(origin=origin, method=method):
                    status, headers, _ = self.request(method, path, origin, body)
                    self.assertEqual(status, expected)
                    self.assertEqual(headers["Access-Control-Allow-Origin"], origin)
                    self.assertEqual(headers["Vary"], "Origin")

    def test_unknown_origins_cannot_submit_or_read_cors_responses(self):
        for origin in ["https://zakura.com.evil.test", "http://zakura.com", "null", "http://localhost:8769"]:
            for method in ["OPTIONS", "POST"]:
                status, headers, _ = self.request(method, "/v1/faucet/claim", origin, '{}')
                self.assertEqual(status, 403)
                self.assertNotIn("Access-Control-Allow-Origin", headers)
            status, headers, _ = self.request("GET", "/v1/faucet/status", origin)
            self.assertEqual(status, 200)
            self.assertNotIn("Access-Control-Allow-Origin", headers)
        self.fake.reserve.assert_not_called()

    def test_errors_keep_cors_and_command_line_clients_still_work(self):
        for error, expected in [(ValueError("Bad address"), 400),
                                (PermissionError("Claim limit"), 429),
                                (RuntimeError("Not ready"), 503)]:
            self.fake.reserve.side_effect = error
            status, headers, _ = self.request("POST", "/v1/faucet/claim", "https://zakura.com",
                                              json.dumps({"address": "utest1" + "a" * 100}))
            self.assertEqual(status, expected)
            self.assertEqual(headers["Access-Control-Allow-Origin"], "https://zakura.com")
        self.fake.reserve.side_effect = None
        self.assertEqual(self.request("POST", "/v1/faucet/claim", body='{"address":"address"}')[0], 202)


class BoundedServerTest(unittest.TestCase):
    def test_concurrent_requests_are_capped(self):
        self.assertEqual(max_concurrent_handlers(faucet.BoundedHTTPServer, 2, 6), 2)

    def test_stalled_clients_time_out(self):
        self.assertEqual(faucet.Handler.timeout, faucet.REQUEST_TIMEOUT_SECONDS)


if __name__ == "__main__":
    unittest.main()
