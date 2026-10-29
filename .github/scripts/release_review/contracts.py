"""Model actions and findings accepted at the review boundary."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Protocol


class ReviewError(RuntimeError):
    """A review could not finish; its partial evidence must remain visible."""


class ProviderError(ReviewError):
    """A sanitized provider failure, optionally safe to retry within the budget."""

    def __init__(self, message: str, *, retryable: bool = False):
        super().__init__(message)
        self.retryable = retryable


@dataclass(frozen=True)
class ModelRequest:
    messages: list[dict[str, str]]
    max_output_tokens: int


@dataclass(frozen=True)
class ModelReply:
    text: str
    input_tokens: int
    output_tokens: int
    cached_input_tokens: int = 0


class ModelProvider(Protocol):
    def complete(self, request: ModelRequest) -> ModelReply:
        """Return a completed response with billable usage, or a sanitized error."""
        ...


def object_schema(properties: dict) -> dict:
    return {
        "type": "object", "properties": properties,
        "required": list(properties), "additionalProperties": False,
    }


def string_schema(*, maximum: int = 4000, choices: list[str] | None = None) -> dict:
    schema = {"type": "string", "maxLength": maximum}
    if choices is not None:
        schema["enum"] = choices
    return schema


REFERENCE_SCHEMA = object_schema({
    "revision": string_schema(choices=["base", "head"]),
    "path": string_schema(maximum=1024),
    "start": {"type": "integer", "minimum": 1},
    "end": {"type": "integer", "minimum": 1},
    "quote": string_schema(maximum=2000),
})
FINDING_SCHEMA = object_schema({
    "severity": string_schema(choices=["Critical", "High", "Medium"]),
    "category": string_schema(choices=["consensus", "cryptography", "mining", "propagation"]),
    "title": string_schema(maximum=160),
    "trigger": string_schema(),
    "impact": string_schema(),
    "explanation": string_schema(),
    "change_path": string_schema(maximum=1024),
    "release_connection": string_schema(),
    "verification": string_schema(),
    "reference": REFERENCE_SCHEMA,
})
ACTION_SCHEMA = object_schema({
    "kind": string_schema(choices=["read", "search"]),
    "revision": string_schema(choices=["base", "head"]),
    "path": string_schema(maximum=1024),
    "start": {"type": "integer", "minimum": 1},
    "end": {"type": "integer", "minimum": 1},
    "query": string_schema(maximum=200),
})
TURN_SCHEMA = object_schema({
    "complete": {"type": "boolean"},
    "summary": string_schema(maximum=2400),
    "actions": {"type": "array", "maxItems": 4, "items": ACTION_SCHEMA},
    "findings": {"type": "array", "maxItems": 12, "items": FINDING_SCHEMA},
})


def validate_schema(value, schema: dict) -> None:
    """Validate the deliberately small schema subset used by both adapters."""
    kind = schema["type"]
    valid_type = {
        "object": isinstance(value, dict), "array": isinstance(value, list),
        "string": isinstance(value, str), "integer": type(value) is int,
        "boolean": type(value) is bool,
    }[kind]
    if not valid_type:
        raise ReviewError("model output has an invalid field type")
    if kind == "object":
        if set(value) != set(schema["properties"]):
            raise ReviewError("model output has missing or unexpected fields")
        for key, child in schema["properties"].items():
            validate_schema(value[key], child)
    elif kind == "array":
        if len(value) > schema["maxItems"]:
            raise ReviewError("model output exceeds the item limit")
        for child in value:
            validate_schema(child, schema["items"])
    elif kind == "string":
        if len(value) > schema["maxLength"] or (
            "enum" in schema and value not in schema["enum"]
        ):
            raise ReviewError("model output has an invalid string or category")
    elif kind == "integer" and value < schema["minimum"]:
        raise ReviewError("model output has an invalid line number")


def parse_turn(text: str) -> dict:
    try:
        turn = json.loads(text)
    except (ValueError, TypeError) as error:
        raise ReviewError("model returned malformed JSON") from error
    validate_schema(turn, TURN_SCHEMA)
    if turn["complete"] and turn["actions"]:
        raise ReviewError("a completed review cannot request more context")
    if not turn["complete"] and (not turn["actions"] or turn["findings"]):
        raise ReviewError("unfinished model turns must request context, not report findings")
    for finding in turn["findings"]:
        for key in ("title", "trigger", "impact", "explanation", "change_path",
                    "release_connection", "verification"):
            if not finding[key].strip():
                raise ReviewError("a finding lacks required evidence")
        if not finding["reference"]["quote"].strip():
            raise ReviewError("a finding lacks a source quotation")
    return turn
