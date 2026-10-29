"""HTTP adapters and conservative, retry-aware API spending accounting."""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from decimal import Decimal

from .config import Settings
from .contracts import (
    TURN_SCHEMA,
    ModelProvider,
    ModelReply,
    ModelRequest,
    ProviderError,
    ReviewError,
)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ProviderError("provider redirects are refused")


class JsonTransport:
    """Send credentials only to the configured endpoint; never expose error bodies."""

    def post(self, endpoint: str, key: str, body: dict) -> dict:
        request = urllib.request.Request(
            endpoint, data=json.dumps(body).encode(),
            headers={"Authorization": "Bearer " + key, "Content-Type": "application/json"},
        )
        try:
            with urllib.request.build_opener(NoRedirect).open(request, timeout=120) as response:
                payload = response.read(2_000_001)
        except urllib.error.HTTPError as error:
            raise ProviderError(
                f"provider returned HTTP {error.code}",
                retryable=error.code in {429, 500, 502, 503, 504},
            ) from None
        except (urllib.error.URLError, TimeoutError, OSError):
            raise ProviderError("provider transport failed or timed out", retryable=True) from None
        if len(payload) > 2_000_000:
            raise ProviderError("provider response exceeded the response limit")
        try:
            return json.loads(payload)
        except (ValueError, UnicodeError):
            raise ProviderError("provider returned invalid JSON") from None


def token_count(usage: dict, name: str) -> int:
    value = usage.get(name)
    if type(value) is not int or value < 0:
        raise ProviderError("provider omitted reliable billing usage; reserved cost is retained")
    return value


def cached_tokens(usage: dict, input_tokens: int) -> int:
    """Discount only explicitly reported cache reads; absent details earn no credit."""
    details = usage.get("input_tokens_details", {})
    if not isinstance(details, dict):
        raise ProviderError("provider returned invalid cached input usage")
    cached = details.get("cached_tokens", 0)
    written = details.get("cache_write_tokens", 0)
    if (type(cached) is not int or not 0 <= cached <= input_tokens
            or type(written) is not int or not 0 <= written <= input_tokens - cached):
        raise ProviderError("provider returned invalid cached input usage")
    return cached


class OpenAIProvider:
    def __init__(self, settings: Settings, key: str, transport: JsonTransport):
        self.settings, self.key, self.transport = settings, key, transport

    def complete(self, request: ModelRequest) -> ModelReply:
        body = {
            "model": self.settings.model, "input": request.messages, "store": False,
            "service_tier": "default",
            "max_output_tokens": request.max_output_tokens,
            "text": {"format": {"type": "json_schema", "name": "release_review",
                                "strict": True, "schema": TURN_SCHEMA}},
        }
        if self.settings.reasoning_effort != "none":
            body["reasoning"] = {"effort": self.settings.reasoning_effort}
        data = self.transport.post(self.settings.endpoint, self.key, body)
        try:
            if data.get("status") != "completed":
                raise ProviderError("OpenAI response was incomplete or refused")
            parts = [part for item in data["output"] if item.get("type") == "message"
                     for part in item.get("content", [])]
            if any(part.get("type") == "refusal" for part in parts):
                raise ProviderError("OpenAI refused the review request")
            if data.get("service_tier") != "default":
                raise ProviderError("OpenAI did not confirm standard service pricing; further calls stopped")
            text = "".join(part["text"] for part in parts if part.get("type") == "output_text")
            usage = data["usage"]
            input_tokens = token_count(usage, "input_tokens")
            return ModelReply(text, input_tokens, token_count(usage, "output_tokens"),
                              cached_tokens(usage, input_tokens))
        except (KeyError, TypeError, AttributeError):
            raise ProviderError("OpenAI returned an invalid response envelope") from None


class AbliteratedProvider:
    def __init__(self, settings: Settings, key: str, transport: JsonTransport):
        self.settings, self.key, self.transport = settings, key, transport

    def complete(self, request: ModelRequest) -> ModelReply:
        # Plain chat messages plus locally validated JSON work without provider-native tools.
        messages = [{"role": "system", "content": "Return only JSON matching this schema: "
                     + json.dumps(TURN_SCHEMA)}, *request.messages]
        data = self.transport.post(self.settings.endpoint, self.key, {
            "model": self.settings.model, "messages": messages,
            "max_tokens": request.max_output_tokens,
        })
        try:
            choice = data["choices"][0]
            if choice["finish_reason"] != "stop" or choice["message"].get("refusal"):
                raise ProviderError("Abliterated response was incomplete or refused")
            text = choice["message"]["content"]
            if not isinstance(text, str):
                raise ProviderError("Abliterated response lacks text")
            return ModelReply(text, token_count(data["usage"], "prompt_tokens"),
                              token_count(data["usage"], "completion_tokens"))
        except (KeyError, IndexError, TypeError, AttributeError):
            raise ProviderError("Abliterated returned an invalid response envelope") from None


@dataclass
class Spending:
    """Retain maximum reservations for calls whose billing outcome is unknown."""

    settings: Settings
    charged: Decimal = Decimal(0)
    input_tokens: int = 0
    output_tokens: int = 0
    calls: int = 0
    uncertain_calls: int = 0
    cached_input_tokens: int = 0

    def price(self, input_tokens: int, output_tokens: int, cached_input_tokens: int = 0) -> Decimal:
        cached_price = self.settings.cached_input_price or self.settings.input_price
        return (self.settings.input_price * (input_tokens - cached_input_tokens)
                + cached_price * cached_input_tokens
                + self.settings.output_price * output_tokens) / 1_000_000

    @staticmethod
    def input_bound(request: ModelRequest) -> int:
        # One token per UTF-8 JSON byte is intentionally conservative for these text-only
        # APIs. Include the schema and an 8192-token allowance for message framing.
        return len(json.dumps(request.messages).encode()) + len(json.dumps(TURN_SCHEMA).encode()) + 8192

    def call(self, provider: ModelProvider, request: ModelRequest, *, sleep=time.sleep,
             deadline: float | None = None, checkpoint=None) -> ModelReply:
        input_bound = self.input_bound(request)
        if input_bound > self.settings.max_input_bytes:
            raise ReviewError("request exceeds the configured model input allowance")
        reserve = self.price(input_bound, request.max_output_tokens)
        for attempt in range(3):
            if deadline is not None and time.monotonic() >= deadline:
                raise ReviewError("review time budget exhausted before the next request")
            if self.charged + reserve > self.settings.budget:
                raise ReviewError("API budget exhausted before the next request")
            self.charged += reserve
            self.calls += 1
            self.uncertain_calls += 1
            # Persist the reservation before network I/O so forced termination
            # cannot leave an artifact claiming that an in-flight call was free.
            if checkpoint is not None:
                checkpoint()
            try:
                reply = provider.complete(request)
            except ProviderError as error:
                if not error.retryable or attempt == 2:
                    raise
                sleep(2 ** attempt)
                continue
            if type(reply.cached_input_tokens) is not int or not 0 <= reply.cached_input_tokens <= reply.input_tokens:
                raise ReviewError("provider returned invalid cached input usage; reserved cost is retained")
            actual = self.price(reply.input_tokens, reply.output_tokens, reply.cached_input_tokens)
            self.uncertain_calls -= 1
            self.input_tokens += reply.input_tokens
            self.output_tokens += reply.output_tokens
            self.cached_input_tokens += reply.cached_input_tokens
            if reply.input_tokens > input_bound or reply.output_tokens > request.max_output_tokens:
                self.charged += max(Decimal(0), actual - reserve)
                raise ReviewError("provider usage exceeded its reservation; further calls stopped")
            self.charged += actual - reserve
            return reply
        raise ReviewError("provider retries were exhausted")

    def report(self) -> dict:
        return {"estimated_usd_including_uncertain_calls": str(self.charged),
                "budget_usd": str(self.settings.budget), "input_tokens": self.input_tokens,
                "output_tokens": self.output_tokens, "calls": self.calls,
                "uncertain_calls": self.uncertain_calls, "cached_input_tokens": self.cached_input_tokens}
