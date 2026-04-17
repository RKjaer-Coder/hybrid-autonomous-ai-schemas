from __future__ import annotations

import json
import re
import time

from immune.types import (
    AlertSeverity,
    BlockReason,
    CheckType,
    ImmuneConfig,
    ImmuneVerdict,
    JudgeMode,
    JudgePayload,
    Outcome,
    Tier,
    generate_uuid_v7,
)

FALLBACK_UUID = "00000000-0000-7000-8000-000000000000"
SECRET_PATTERNS = [
    re.compile(r"sk-[A-Za-z0-9_-]+"),
    re.compile(r"api_key\s*=", re.IGNORECASE),
    re.compile(r"password\s*=", re.IGNORECASE),
    re.compile(r"secret\s*=", re.IGNORECASE),
    re.compile(r"token\s*=", re.IGNORECASE),
    re.compile(r"Bearer\s+[A-Za-z0-9._-]+"),
    re.compile(r"AKIA[0-9A-Z]{16}"),
    re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----"),
]
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
SSN_RE = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
CARD_RE = re.compile(r"\b\d(?:[ -]?\d){12,15}\b")
PROMPT_PATTERNS = [
    re.compile(r"You are a", re.IGNORECASE),
    re.compile(r"Your instructions are", re.IGNORECASE),
    re.compile(r"System prompt:", re.IGNORECASE),
]
SHELL_DANGER_PATTERNS = [
    re.compile(r"\brm\s+-rf\b", re.IGNORECASE),
    re.compile(r"\b(?:curl|wget)\b\s+\S+", re.IGNORECASE),
    re.compile(r"\b(?:bash|sh|zsh)\s+-c\b", re.IGNORECASE),
    re.compile(r"\bsudo\b", re.IGNORECASE),
    re.compile(r"(?:;|&&|\|\|)\s*(?:rm|curl|wget|bash|sh|zsh|python)\b", re.IGNORECASE),
]


def _validate_schema(output: object, schema: dict, path: str = "$") -> str | None:
    stype = schema.get("type")
    if stype == "object":
        if not isinstance(output, dict):
            return f"Type mismatch at {path}: expected object"
        req = schema.get("required", [])
        for key in req:
            if key not in output:
                return f"Missing required field at {path}.{key}"
        props = schema.get("properties", {})
        for key, subschema in props.items():
            if key in output and isinstance(subschema, dict):
                err = _validate_schema(output[key], subschema, f"{path}.{key}")
                if err:
                    return err
    elif stype == "array":
        if not isinstance(output, list):
            return f"Type mismatch at {path}: expected array"
        item_schema = schema.get("items")
        if isinstance(item_schema, dict):
            for idx, value in enumerate(output):
                err = _validate_schema(value, item_schema, f"{path}[{idx}]")
                if err:
                    return err
    elif stype == "string" and not isinstance(output, str):
        return f"Type mismatch at {path}: expected string"
    elif stype == "integer" and not isinstance(output, int):
        return f"Type mismatch at {path}: expected integer"
    elif stype == "number" and not isinstance(output, (int, float)):
        return f"Type mismatch at {path}: expected number"
    elif stype == "boolean" and not isinstance(output, bool):
        return f"Type mismatch at {path}: expected boolean"
    if "enum" in schema and output not in schema["enum"]:
        return f"Enum mismatch at {path}"
    return None


def _iter_strings(data: object) -> list[str]:
    result: list[str] = []
    stack: list[object] = [data]
    while stack:
        value = stack.pop()
        if isinstance(value, str):
            result.append(value)
        elif isinstance(value, dict):
            stack.extend(value.values())
        elif isinstance(value, (list, tuple)):
            stack.extend(value)
    return result


def _safe_scan(text: str) -> bool:
    lower = text.lower()
    if (
        "sk-" in text
        or "api_key" in lower
        or "password" in lower
        or "secret" in lower
        or "token" in lower
        or "bearer " in lower
        or "akia" in text
        or "-----begin" in lower
    ) and any(p.search(text) for p in SECRET_PATTERNS):
        return True
    if "@" in text and EMAIL_RE.search(text):
        return True
    if any(ch.isdigit() for ch in text) and (SSN_RE.search(text) or CARD_RE.search(text)):
        return True
    if (
        "you are a" in lower
        or "your instructions are" in lower
        or "system prompt:" in lower
    ) and any(p.search(text) for p in PROMPT_PATTERNS):
        return True
    return False


def _normal_judge_check(payload: JudgePayload, config: ImmuneConfig, start: int) -> ImmuneVerdict:
    if payload.expected_schema is not None:
        schema_err = _validate_schema(payload.output, payload.expected_schema)
        if schema_err:
            return ImmuneVerdict(
                generate_uuid_v7(),
                CheckType.JUDGE,
                Tier.FAST_PATH,
                payload.skill_name,
                payload.session_id,
                Outcome.BLOCK,
                BlockReason.SCHEMA_VIOLATION,
                schema_err,
                (time.monotonic_ns() - start) / 1_000_000,
                AlertSeverity.IMMUNE_BLOCK_FAST,
            )
    claimed = payload.output.get("claimed_trust_tier")
    if isinstance(claimed, int) and claimed > payload.max_trust_tier:
        return ImmuneVerdict(
            generate_uuid_v7(),
            CheckType.JUDGE,
            Tier.FAST_PATH,
            payload.skill_name,
            payload.session_id,
            Outcome.BLOCK,
            BlockReason.TRUST_TIER_VIOLATION,
            "Output claimed trust tier above max",
            (time.monotonic_ns() - start) / 1_000_000,
            AlertSeverity.IMMUNE_BLOCK_FAST,
        )
    for text in _iter_strings(payload.output):
        if _safe_scan(text):
            return ImmuneVerdict(
                generate_uuid_v7(),
                CheckType.JUDGE,
                Tier.FAST_PATH,
                payload.skill_name,
                payload.session_id,
                Outcome.BLOCK,
                BlockReason.CONTENT_SAFETY,
                "Content safety violation",
                (time.monotonic_ns() - start) / 1_000_000,
                AlertSeverity.SECURITY_ALERT,
            )
    size = len(json.dumps(payload.output))
    if size > 1_048_576:
        return ImmuneVerdict(
            generate_uuid_v7(),
            CheckType.JUDGE,
            Tier.FAST_PATH,
            payload.skill_name,
            payload.session_id,
            Outcome.BLOCK,
            BlockReason.POLICY_VIOLATION,
            "Output exceeds 1MB",
            (time.monotonic_ns() - start) / 1_000_000,
            AlertSeverity.IMMUNE_BLOCK_FAST,
        )
    elapsed_ms = (time.monotonic_ns() - start) / 1_000_000
    if elapsed_ms > config.judge_timeout_ms:
        return ImmuneVerdict(
            generate_uuid_v7(),
            CheckType.JUDGE,
            Tier.FAST_PATH,
            payload.skill_name,
            payload.session_id,
            Outcome.BLOCK,
            BlockReason.TIMEOUT,
            "Judge timeout",
            elapsed_ms,
            AlertSeverity.IMMUNE_TIMEOUT,
        )
    return ImmuneVerdict(
        generate_uuid_v7(),
        CheckType.JUDGE,
        Tier.FAST_PATH,
        payload.skill_name,
        payload.session_id,
        Outcome.PASS,
        latency_ms=elapsed_ms,
    )


def _structural_fallback_reason(trigger: str, detail: str | None) -> str:
    if detail:
        return f"Judge structural fallback: {trigger} ({detail})"
    return f"Judge structural fallback: {trigger}"


def _structural_fallback_check(payload: JudgePayload, start: int, trigger: str, detail: str | None = None) -> ImmuneVerdict:
    try:
        for text in _iter_strings(payload.output):
            if any(pattern.search(text) for pattern in SHELL_DANGER_PATTERNS):
                return ImmuneVerdict(
                    generate_uuid_v7(),
                    CheckType.JUDGE,
                    Tier.FAST_PATH,
                    payload.skill_name,
                    payload.session_id,
                    Outcome.BLOCK,
                    BlockReason.POLICY_VIOLATION,
                    "Structural fallback blocked shell-dangerous output",
                    (time.monotonic_ns() - start) / 1_000_000,
                    AlertSeverity.SECURITY_ALERT,
                    judge_mode=JudgeMode.FALLBACK,
                )
            if _safe_scan(text):
                return ImmuneVerdict(
                    generate_uuid_v7(),
                    CheckType.JUDGE,
                    Tier.FAST_PATH,
                    payload.skill_name,
                    payload.session_id,
                    Outcome.BLOCK,
                    BlockReason.CONTENT_SAFETY,
                    "Structural fallback blocked sensitive output",
                    (time.monotonic_ns() - start) / 1_000_000,
                    AlertSeverity.SECURITY_ALERT,
                    judge_mode=JudgeMode.FALLBACK,
                )
        return ImmuneVerdict(
            generate_uuid_v7(),
            CheckType.JUDGE,
            Tier.FAST_PATH,
            payload.skill_name,
            payload.session_id,
            Outcome.PASS,
            block_detail=_structural_fallback_reason(trigger, detail),
            latency_ms=(time.monotonic_ns() - start) / 1_000_000,
            judge_mode=JudgeMode.FALLBACK,
        )
    except Exception:
        return ImmuneVerdict(
            FALLBACK_UUID,
            CheckType.JUDGE,
            Tier.FAST_PATH,
            getattr(payload, "skill_name", "unknown"),
            getattr(payload, "session_id", "unknown"),
            Outcome.BLOCK,
            BlockReason.INTERNAL_ERROR,
            "Judge fallback internal error — fail closed",
            0.0,
            AlertSeverity.SECURITY_ALERT,
            judge_mode=JudgeMode.FALLBACK,
        )


def judge_check(payload: JudgePayload, config: ImmuneConfig) -> ImmuneVerdict:
    """Run Judge output checks, with explicit structural fallback on degraded paths."""
    try:
        start = time.monotonic_ns()
        if payload.force_structural_fallback:
            return _structural_fallback_check(payload, start, payload.fallback_reason or "forced")
        verdict = _normal_judge_check(payload, config, start)
        fallback_allowed = payload.allow_structural_fallback or config.judge_structural_fallback_enabled
        if (
            fallback_allowed
            and verdict.outcome == Outcome.BLOCK
            and verdict.block_reason in {BlockReason.TIMEOUT, BlockReason.INTERNAL_ERROR}
        ):
            return _structural_fallback_check(
                payload,
                start,
                verdict.block_reason.value,
                verdict.block_detail,
            )
        return verdict
    except Exception as exc:
        fallback_allowed = (
            getattr(payload, "allow_structural_fallback", False)
            or config.judge_structural_fallback_enabled
        )
        if fallback_allowed and payload is not None and hasattr(payload, "output"):
            return _structural_fallback_check(
                payload,
                time.monotonic_ns(),
                payload.fallback_reason or "normal_judge_unavailable",
                str(exc),
            )
        return ImmuneVerdict(
            FALLBACK_UUID,
            CheckType.JUDGE,
            Tier.FAST_PATH,
            getattr(payload, "skill_name", "unknown"),
            getattr(payload, "session_id", "unknown"),
            Outcome.BLOCK,
            BlockReason.INTERNAL_ERROR,
            "Judge internal error — fail closed",
            0.0,
            AlertSeverity.SECURITY_ALERT,
        )


if __name__ == "__main__":
    print("ok")
