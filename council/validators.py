from __future__ import annotations

from typing import List, Tuple

from council.prompts.common import parse_json_output
from council.prompts.role_critic import CRITIC_OUTPUT_SCHEMA
from council.prompts.role_devils_advocate import DA_OUTPUT_SCHEMA
from council.prompts.role_realist import REALIST_OUTPUT_SCHEMA
from council.prompts.role_strategist import STRATEGIST_OUTPUT_SCHEMA
from council.prompts.synthesis import SYNTHESIS_OUTPUT_SCHEMA
from council.types import DecisionType, RoleName


def validate_verdict(verdict_data: dict, decision_type: DecisionType) -> List[str]:
    errors: List[str] = []
    try:
        if isinstance(verdict_data, dict):
            parsed = verdict_data
        else:
            parsed = parse_json_output(verdict_data, SYNTHESIS_OUTPUT_SCHEMA)
        if not isinstance(parsed, dict):
            errors.append("invalid verdict payload")
    except Exception:
        for field in SYNTHESIS_OUTPUT_SCHEMA["required"]:
            if field not in verdict_data:
                errors.append(f"missing field: {field}")
    conf = verdict_data.get("confidence")
    if not isinstance(conf, (int, float)) or not 0.0 <= conf <= 1.0:
        errors.append("confidence out of range")
    if verdict_data.get("decision_type") != decision_type.value:
        errors.append("decision_type mismatch")
    if verdict_data.get("recommendation") not in SYNTHESIS_OUTPUT_SCHEMA["properties"]["recommendation"]["enum"]:
        errors.append("invalid recommendation")
    if not verdict_data.get("da_assessment"):
        errors.append("da_assessment required")
    if verdict_data.get("degraded") and conf is not None and conf > 0.70:
        errors.append("degraded confidence cap violated")
    if verdict_data.get("tie_break") and isinstance(conf, (int, float)) and conf > 0.65:
        errors.append("warning: tie_break with high confidence")
    return errors


def validate_role_output(raw: str, role: RoleName) -> Tuple[dict, List[str]]:
    schema_map = {
        RoleName.STRATEGIST: STRATEGIST_OUTPUT_SCHEMA,
        RoleName.CRITIC: CRITIC_OUTPUT_SCHEMA,
        RoleName.REALIST: REALIST_OUTPUT_SCHEMA,
        RoleName.DEVILS_ADVOCATE: DA_OUTPUT_SCHEMA,
    }
    try:
        parsed = parse_json_output(raw, schema_map[role])
        return parsed, []
    except ValueError as exc:
        return {}, [str(exc)]
