"""Load trusted review prompts and explicitly configured model prices."""

from __future__ import annotations

import hashlib
import json
import re
import tomllib
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.parse import urlsplit

from .contracts import ReviewError
from .scope import exclusion_paths


def positive_decimal(value, name: str) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise ReviewError(f"{name} must be an explicit positive number") from error
    if not number.is_finite() or number <= 0:
        raise ReviewError(f"{name} must be an explicit positive number")
    return number


@dataclass(frozen=True)
class Settings:
    provider: str
    model: str
    endpoint: str
    input_price: Decimal
    output_price: Decimal
    budget: Decimal
    max_input_bytes: int
    max_output_tokens: int
    reasoning_effort: str
    unit_bytes: int
    max_seconds: int
    instructions: str
    prompt_digest: str
    excluded_paths: tuple[str, ...] = ()
    cached_input_price: Decimal | None = None


def load_settings(root: Path, provider: str, profile: str, models_json: str) -> Settings:
    """Read only the trusted control checkout; PR content cannot select an endpoint."""
    if provider not in {"openai", "abliterated"}:
        raise ReviewError("unknown model provider")
    if not re.fullmatch(r"[a-z][a-z0-9_-]{0,39}", profile):
        raise ReviewError("invalid prompt profile")
    try:
        raw = tomllib.loads((root / "config.toml").read_text())
        files = raw["profiles"][profile]["prompts"]
        models = json.loads(models_json)
        model = models[provider]
    except (OSError, ValueError, KeyError, TypeError) as error:
        raise ReviewError("configure the prompt profile and RELEASE_REVIEW_MODELS first") from error
    if not isinstance(model, dict):
        raise ReviewError("provider configuration must be a JSON object")
    exclusions = exclusion_paths(raw.get("scope", {}))
    name = model.get("model", "")
    if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_./:@+-]{0,199}", name):
        raise ReviewError("an explicit model ID is required")
    endpoint = "https://api.openai.com/v1/responses"
    if provider == "abliterated":
        base = model.get("base_url", "")
        if not isinstance(base, str):
            raise ReviewError("Abliterated base_url must be a string")
        parsed = urlsplit(base)
        if (parsed.scheme != "https" or not parsed.hostname or parsed.username
                or parsed.password or parsed.query or parsed.fragment):
            raise ReviewError("Abliterated requires a configured HTTPS base_url without credentials")
        endpoint = base.rstrip("/") + "/chat/completions"
    input_price = positive_decimal(model.get("input_usd_per_million"), "input price")
    output_price = positive_decimal(model.get("output_usd_per_million"), "output price")
    cached_price = model.get("cached_input_usd_per_million")
    if cached_price is not None:
        cached_price = positive_decimal(cached_price, "cached input price")
        if provider != "openai" or cached_price > input_price:
            raise ReviewError("cached input pricing requires OpenAI and cannot exceed the input price")
    max_input = model.get("max_input_bytes")
    max_output = model.get("max_output_tokens", 4096)
    if type(max_input) is not int or not 16000 <= max_input <= 500000:
        raise ReviewError("configure max_input_bytes between 16000 and 500000 for this model")
    if type(max_output) is not int or not 1024 <= max_output <= 32768:
        raise ReviewError("max_output_tokens must be between 1024 and 32768")
    effort = model.get("reasoning_effort", "medium")
    if effort not in {"none", "low", "medium", "high"}:
        raise ReviewError("reasoning_effort must be none, low, medium, or high")
    instructions = []
    if not isinstance(files, list) or not files:
        raise ReviewError("the prompt profile must contain prompt files")
    for file in files:
        if not isinstance(file, str) or not re.fullmatch(r"prompts/[a-z0-9_-]+\.md", file):
            raise ReviewError("prompt paths must be Markdown files inside prompts/")
        path = root / file
        if path.is_symlink():
            raise ReviewError("prompt files must not be symbolic links")
        instructions.append(path.read_text())
    prompt = "\n\n".join(instructions)
    limits = raw["limits"]
    budget = positive_decimal(limits["budget_usd"], "budget")
    if budget > 25:
        raise ReviewError("this workflow is authorized for at most $25 per run")
    unit_bytes = limits["unit_bytes"]
    seconds = limits["max_seconds"]
    if (type(unit_bytes) is not int or not 1000 <= unit_bytes <= 48000
            or type(seconds) is not int or not 60 <= seconds <= 10800):
        raise ReviewError("invalid review work limits")
    return Settings(provider, name, endpoint, input_price, output_price, budget,
                    max_input, max_output, effort, unit_bytes, seconds,
                    prompt, hashlib.sha256(prompt.encode()).hexdigest(), exclusions, cached_price)
