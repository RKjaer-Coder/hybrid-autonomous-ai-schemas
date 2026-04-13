from __future__ import annotations

import json
from typing import List

from council.types import ContextPacket, RoleOutput


def _extract_json_blob(raw: str) -> str:
    text = raw.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines)
        if text.startswith("json"):
            text = text[4:].lstrip()
    return text


def _validate_schema(value, schema: dict, path: str = "$") -> None:
    expected_type = schema.get("type")
    if expected_type == "object":
        if not isinstance(value, dict):
            raise ValueError(f"{path}: expected object")
        for key in schema.get("required", []):
            if key not in value:
                raise ValueError(f"{path}: missing required field '{key}'")
        props = schema.get("properties", {})
        for key, sub in props.items():
            if key in value:
                _validate_schema(value[key], sub, f"{path}.{key}")
    elif expected_type == "array":
        if not isinstance(value, list):
            raise ValueError(f"{path}: expected array")
        if "minItems" in schema and len(value) < schema["minItems"]:
            raise ValueError(f"{path}: requires at least {schema['minItems']} items")
        item_schema = schema.get("items")
        if item_schema:
            for i, item in enumerate(value):
                _validate_schema(item, item_schema, f"{path}[{i}]")
    elif expected_type == "string":
        if not isinstance(value, str):
            raise ValueError(f"{path}: expected string")
        if "const" in schema and value != schema["const"]:
            raise ValueError(f"{path}: expected const '{schema['const']}'")
        if "enum" in schema and value not in schema["enum"]:
            raise ValueError(f"{path}: expected one of {schema['enum']}")
        if "maxLength" in schema and len(value) > schema["maxLength"]:
            raise ValueError(f"{path}: exceeds maxLength")
    elif expected_type == "number":
        if not isinstance(value, (int, float)):
            raise ValueError(f"{path}: expected number")
        if "minimum" in schema and value < schema["minimum"]:
            raise ValueError(f"{path}: below minimum")
        if "maximum" in schema and value > schema["maximum"]:
            raise ValueError(f"{path}: above maximum")
    elif expected_type == "integer":
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError(f"{path}: expected integer")
        if "const" in schema and value != schema["const"]:
            raise ValueError(f"{path}: expected const {schema['const']}")
        if "minimum" in schema and value < schema["minimum"]:
            raise ValueError(f"{path}: below minimum")
        if "maximum" in schema and value > schema["maximum"]:
            raise ValueError(f"{path}: above maximum")
    elif expected_type == "boolean":
        if not isinstance(value, bool):
            raise ValueError(f"{path}: expected boolean")


def format_context_packet(packet: ContextPacket) -> str:
    return (
        f"Decision type: {packet.decision_type.value}\n"
        f"Subject: {packet.subject_id}\n"
        f"Token budget: {packet.token_count}/{packet.max_tokens}\n\n"
        f"Context:\n{packet.context_text}"
    )


def enforce_token_budget(text: str, max_tokens: int) -> str:
    max_words = max(1, int(max_tokens * 0.75))
    words = text.split()
    if len(words) <= max_words:
        return text
    return " ".join(words[:max_words]) + " [TRUNCATED]"


def format_batch_a_for_da(outputs: List[RoleOutput]) -> str:
    lines = []
    for output in outputs:
        safe_content = output.content.replace("{", "{{").replace("}", "}}")
        lines.append(f"[{output.role.value.upper()}]\n{safe_content}")
    return "\n\n".join(lines)


def parse_json_output(raw: str, schema: dict) -> dict:
    try:
        parsed = json.loads(_extract_json_blob(raw))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid JSON output: {exc}") from exc
    _validate_schema(parsed, schema)
    return parsed
