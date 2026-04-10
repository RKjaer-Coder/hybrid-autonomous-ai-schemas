from __future__ import annotations

import re

from immune.types import BlockReason, ImmuneConfig, SheriffPayload

UUID_V7_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)
SKILL_RE = re.compile(r"^[a-z0-9_]+$")
MAX_STRING_BYTES = 102_400
MAX_DEPTH = 20


def validate_structure(
    payload: SheriffPayload,
    config: ImmuneConfig,
) -> tuple[BlockReason, str] | None:
    """Return structural failure reason/detail, else None."""
    required = {
        "session_id": str,
        "skill_name": str,
        "tool_name": str,
        "arguments": dict,
    }
    for field_name, field_type in required.items():
        value = getattr(payload, field_name, None)
        if value is None or not isinstance(value, field_type):
            return (BlockReason.STRUCTURAL_MALFORMATION, f"Missing or invalid required field: {field_name}")
        if isinstance(value, str) and not value:
            return (BlockReason.STRUCTURAL_MALFORMATION, f"Missing or invalid required field: {field_name}")

    stack: list[tuple[str, object, int, frozenset[int]]] = [
        ("session_id", payload.session_id, 0, frozenset()),
        ("skill_name", payload.skill_name, 0, frozenset()),
        ("tool_name", payload.tool_name, 0, frozenset()),
        ("raw_prompt", payload.raw_prompt or "", 0, frozenset()),
        ("arguments", payload.arguments, 0, frozenset()),
    ]
    seen_ids: set[int] = set()

    while stack:
        path, value, depth, ancestors = stack.pop()
        if isinstance(value, str) and len(value.encode("utf-8")) > MAX_STRING_BYTES:
            return (BlockReason.STRUCTURAL_MALFORMATION, f"String field exceeds 100KB limit: {path}")

        if isinstance(value, (dict, list, tuple)):
            if depth > MAX_DEPTH:
                return (BlockReason.STRUCTURAL_MALFORMATION, "Arguments nesting depth exceeds 20")
            oid = id(value)
            if oid in ancestors:
                return (
                    BlockReason.STRUCTURAL_MALFORMATION,
                    "Circular reference detected in arguments",
                )
            if oid in seen_ids:
                continue
            seen_ids.add(oid)
            next_ancestors = set(ancestors)
            next_ancestors.add(oid)
            frozen_next = frozenset(next_ancestors)
            if isinstance(value, dict):
                for k, v in value.items():
                    stack.append((f"{path}.{k}", v, depth + 1, frozen_next))
            else:
                for i, item in enumerate(value):
                    stack.append((f"{path}[{i}]", item, depth + 1, frozen_next))

    if not UUID_V7_RE.match(payload.session_id):
        return (BlockReason.STRUCTURAL_MALFORMATION, "Invalid format: session_id")
    if not SKILL_RE.match(payload.skill_name):
        return (BlockReason.STRUCTURAL_MALFORMATION, "Invalid format: skill_name")

    if config.known_tool_registry and payload.tool_name not in config.known_tool_registry:
        return (BlockReason.STRUCTURAL_MALFORMATION, f"Unknown tool: {payload.tool_name}")
    return None


if __name__ == "__main__":
    print("ok")
