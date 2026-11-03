"""Provider-confirmed cache reads reduce costs without weakening reservations."""

import json
import sys
import unittest
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from release_review.config import load_settings
from release_review.contracts import ModelRequest, ProviderError, ReviewError
from release_review.models import OpenAIProvider, Spending
from test_release_review import ROOT, Transport, settings


class CacheAccountingTests(unittest.TestCase):
    def request(self):
        return ModelRequest([{"role": "user", "content": "review"}], 4096)

    def provider(self, details, config=None):
        usage = {"input_tokens": 1000, "output_tokens": 100}
        if details is not ...:
            usage["input_tokens_details"] = details
        return OpenAIProvider(config or settings(), "fixture", Transport({
            "status": "completed", "service_tier": "default", "output": [], "usage": usage,
        }))

    def test_discount_only_reported_cache_reads_after_full_reservation(self):
        config = settings(input_price=Decimal("2.5"), output_price=Decimal(10), cached_input_price=Decimal("0.1"))
        spending = Spending(config)
        checkpoints = []
        spending.call(self.provider({"cached_tokens": 800, "cache_write_tokens": 200}, config),
                      self.request(), checkpoint=lambda: checkpoints.append(spending.report()))
        self.assertEqual(spending.charged, Decimal("0.00158"))
        self.assertEqual(spending.cached_input_tokens, 800)
        reserve = spending.price(Spending.input_bound(self.request()), 4096)
        self.assertEqual(Decimal(checkpoints[0]["estimated_usd_including_uncertain_calls"]), reserve)
        self.assertEqual(checkpoints[0]["cached_input_tokens"], 0)

    def test_missing_cache_details_or_price_never_guesses_a_discount(self):
        for details, price in ((..., Decimal("0.1")), ({}, Decimal("0.1")), ({"cached_tokens": 800}, None)):
            config = settings(cached_input_price=price)
            spending = Spending(config)
            spending.call(self.provider(details, config), self.request())
            self.assertEqual(spending.charged, Decimal("0.0012"))

    def test_malformed_cache_usage_retains_the_full_reservation(self):
        for details in (None, [], {"cached_tokens": -1}, {"cached_tokens": True},
                        {"cached_tokens": "800"}, {"cached_tokens": 1001},
                        {"cached_tokens": 800, "cache_write_tokens": 201},
                        {"cache_write_tokens": -1}):
            spending = Spending(settings(cached_input_price=Decimal("0.1")))
            with self.subTest(details=details), self.assertRaises(ProviderError):
                spending.call(self.provider(details), self.request())
            self.assertEqual(spending.uncertain_calls, 1)
            self.assertEqual(spending.charged, spending.price(Spending.input_bound(self.request()), 4096))

    def test_expected_cache_hits_cannot_authorize_an_unfunded_request(self):
        config = settings(budget=Decimal("0.002"), cached_input_price=Decimal("0.1"))
        provider = self.provider({"cached_tokens": 1000}, config)
        with self.assertRaisesRegex(ReviewError, "budget exhausted"):
            Spending(config).call(provider, self.request())
        self.assertEqual(provider.transport.requests, [])

    def test_cache_pricing_must_be_explicit_positive_and_provider_supported(self):
        model = {"model": "fixture", "input_usd_per_million": "2.5", "output_usd_per_million": "10",
                 "max_input_bytes": 120000, "base_url": "https://example.invalid/v1"}
        for value in ("NaN", "-1", "0", "3"):
            model["cached_input_usd_per_million"] = value
            with self.subTest(value=value), self.assertRaises(ReviewError):
                load_settings(ROOT / "release-review", "openai", "default", json.dumps({"openai": model}))
        model["cached_input_usd_per_million"] = "0.1"
        config = load_settings(ROOT / "release-review", "openai", "default", json.dumps({"openai": model}))
        self.assertEqual(config.cached_input_price, Decimal("0.1"))
        with self.assertRaises(ReviewError):
            load_settings(ROOT / "release-review", "abliterated", "default", json.dumps({"abliterated": model}))
