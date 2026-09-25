"""Tests for public claim allocation, without a node or miner key."""

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from faucet import Faucet, PAYOUT_ZAT


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


if __name__ == "__main__":
    unittest.main()
