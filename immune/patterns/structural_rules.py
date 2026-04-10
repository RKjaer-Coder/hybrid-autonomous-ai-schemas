from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Optional


@dataclass(frozen=True)
class StructuralLimits:
    max_string_length: int = 100_000
    max_nesting_depth: int = 10
    max_total_size_bytes: int = 10_485_760
    required_fields: frozenset[str] = field(
        default_factory=lambda: frozenset({"session_id", "skill_name", "tool_name"})
    )
    id_format_pattern: re.Pattern[str] = field(
        default_factory=lambda: re.compile(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
        )
    )
    known_tools: frozenset[str] = field(default_factory=frozenset)


STRUCTURAL_LIMITS = StructuralLimits()


def check_required_fields(payload: dict, limits: StructuralLimits = STRUCTURAL_LIMITS) -> Optional[str]:
    for field_name in limits.required_fields:
        value = payload.get(field_name)
        if value is None or (isinstance(value, str) and not value.strip()):
            return f"Missing required field: {field_name}"
    return None


def _iter_values(value: object, depth: int = 0, path: str = "payload"):
    yield (path, value, depth)
    if isinstance(value, dict):
        for k, v in value.items():
            yield from _iter_values(v, depth + 1, f"{path}.{k}")
    elif isinstance(value, (list, tuple)):
        for i, item in enumerate(value):
            yield from _iter_values(item, depth + 1, f"{path}[{i}]")


def check_string_length(payload: dict, limits: StructuralLimits = STRUCTURAL_LIMITS) -> Optional[str]:
    for path, value, _ in _iter_values(payload):
        if isinstance(value, str) and len(value) > limits.max_string_length:
            return f"String exceeds {limits.max_string_length} chars at {path}"
    return None


def check_nesting_depth(payload: dict, limits: StructuralLimits = STRUCTURAL_LIMITS) -> Optional[str]:
    for path, _, depth in _iter_values(payload):
        if depth > limits.max_nesting_depth:
            return f"Nesting depth exceeds {limits.max_nesting_depth} at {path}"
    return None


def _walk_circular(value: object, ancestors: frozenset[int], seen: set[int]) -> bool:
    if isinstance(value, (dict, list, tuple)):
        oid = id(value)
        if oid in ancestors:
            return True
        if oid in seen:
            return False
        seen.add(oid)
        next_ancestors = set(ancestors)
        next_ancestors.add(oid)
        frozen_next = frozenset(next_ancestors)
        if isinstance(value, dict):
            return any(_walk_circular(v, frozen_next, seen) for v in value.values())
        return any(_walk_circular(v, frozen_next, seen) for v in value)
    return False


def check_circular_references(payload: dict) -> Optional[str]:
    if _walk_circular(payload, frozenset(), set()):
        return "Circular reference detected"
    return None


def check_id_format(payload: dict, limits: StructuralLimits = STRUCTURAL_LIMITS) -> Optional[str]:
    for path, value, _ in _iter_values(payload):
        if path.rsplit(".", 1)[-1].endswith("_id") and isinstance(value, str):
            if not limits.id_format_pattern.match(value):
                return f"Invalid ID format at {path}"
    return None


def check_known_tool(payload: dict, limits: StructuralLimits = STRUCTURAL_LIMITS) -> Optional[str]:
    if not limits.known_tools:
        return None
    tool_name = payload.get("tool_name")
    if tool_name not in limits.known_tools:
        return f"Unknown tool: {tool_name}"
    return None
