from __future__ import annotations

import asyncio
import time

from immune.classifiers.ipi_classifier import classify_ipi
from immune.classifiers.policy_checker import check_policy
from immune.classifiers.structural_validator import validate_structure
from immune.context_params import ContextParams, check_context_params
from immune.deep_scan import DeepScanInterface
from immune.types import (
    AlertSeverity,
    BlockReason,
    CheckType,
    ImmuneConfig,
    ImmuneVerdict,
    Outcome,
    SheriffPayload,
    Tier,
    generate_uuid_v7,
)

FALLBACK_UUID = "00000000-0000-7000-8000-000000000000"


def _safe_verdict_id() -> str:
    try:
        return generate_uuid_v7()
    except Exception:
        return FALLBACK_UUID


def sheriff_check(payload: SheriffPayload, config: ImmuneConfig) -> ImmuneVerdict:
    """Run fast-path Sheriff checks. Fail closed on all exceptions."""
    try:
        start = time.monotonic_ns()
        for classifier in (
            lambda: validate_structure(payload, config),
            lambda: classify_ipi(payload, config),
            lambda: check_policy(payload, config),
        ):
            elapsed_ms = (time.monotonic_ns() - start) / 1_000_000
            if elapsed_ms > config.sheriff_fast_path_timeout_ms:
                return ImmuneVerdict(
                    verdict_id=generate_uuid_v7(),
                    check_type=CheckType.SHERIFF,
                    tier=Tier.FAST_PATH,
                    skill_name=payload.skill_name,
                    session_id=payload.session_id,
                    outcome=Outcome.BLOCK,
                    block_reason=BlockReason.TIMEOUT,
                    block_detail="Sheriff fast-path timeout",
                    latency_ms=elapsed_ms,
                    alert_severity=AlertSeverity.IMMUNE_TIMEOUT,
                )
            result = classifier()
            if result is not None:
                reason, detail = result
                return ImmuneVerdict(
                    verdict_id=generate_uuid_v7(),
                    check_type=CheckType.SHERIFF,
                    tier=Tier.FAST_PATH,
                    skill_name=payload.skill_name,
                    session_id=payload.session_id,
                    outcome=Outcome.BLOCK,
                    block_reason=reason,
                    block_detail=detail,
                    latency_ms=(time.monotonic_ns() - start) / 1_000_000,
                    alert_severity=AlertSeverity.IMMUNE_BLOCK_FAST,
                )
        if config.context_params_enabled:
            args = payload.arguments if isinstance(payload.arguments, dict) else {}
            c_result = check_context_params(
                ContextParams(
                    execution_trace_hash=args.get("execution_trace_hash"),
                    tool_window=tuple(args.get("tool_window", ())),
                    session_age_seconds=float(args.get("session_age_seconds", 0.0)),
                )
            )
            if c_result is not None:
                rule_name, action = c_result
                return ImmuneVerdict(
                    verdict_id=generate_uuid_v7(),
                    check_type=CheckType.SHERIFF,
                    tier=Tier.FAST_PATH,
                    skill_name=payload.skill_name,
                    session_id=payload.session_id,
                    outcome=Outcome.BLOCK,
                    block_reason=BlockReason.POLICY_VIOLATION,
                    block_detail=f"{rule_name}: {action}",
                    latency_ms=(time.monotonic_ns() - start) / 1_000_000,
                    alert_severity=AlertSeverity.IMMUNE_BLOCK_FAST,
                )
        elapsed_ms = (time.monotonic_ns() - start) / 1_000_000
        if elapsed_ms > config.sheriff_fast_path_timeout_ms:
            return ImmuneVerdict(
                verdict_id=generate_uuid_v7(),
                check_type=CheckType.SHERIFF,
                tier=Tier.FAST_PATH,
                skill_name=payload.skill_name,
                session_id=payload.session_id,
                outcome=Outcome.BLOCK,
                block_reason=BlockReason.TIMEOUT,
                block_detail="Sheriff fast-path timeout",
                latency_ms=elapsed_ms,
                alert_severity=AlertSeverity.IMMUNE_TIMEOUT,
            )
        return ImmuneVerdict(
            verdict_id=generate_uuid_v7(),
            check_type=CheckType.SHERIFF,
            tier=Tier.FAST_PATH,
            skill_name=payload.skill_name,
            session_id=payload.session_id,
            outcome=Outcome.PASS,
            latency_ms=elapsed_ms,
        )
    except Exception:
        return ImmuneVerdict(
            verdict_id=FALLBACK_UUID,
            check_type=CheckType.SHERIFF,
            tier=Tier.FAST_PATH,
            skill_name=getattr(payload, "skill_name", "unknown"),
            session_id=getattr(payload, "session_id", "unknown"),
            outcome=Outcome.BLOCK,
            block_reason=BlockReason.INTERNAL_ERROR,
            block_detail="Sheriff internal error — fail closed",
            latency_ms=0.0,
            alert_severity=AlertSeverity.SECURITY_ALERT,
        )


async def trigger_deep_scan(
    payload: SheriffPayload,
    config: ImmuneConfig,
    deep_scan_model: DeepScanInterface,
) -> ImmuneVerdict | None:
    """Run asynchronous deep-scan; inconclusive paths return None."""
    text = (payload.raw_prompt or "") + "\n" + str(payload.arguments)
    try:
        start = time.monotonic_ns()
        result = await asyncio.wait_for(
            deep_scan_model.classify(text, {"skill_name": payload.skill_name}),
            timeout=config.deep_scan_timeout_ms / 1000,
        )
        elapsed_ms = (time.monotonic_ns() - start) / 1_000_000
        if result.threat_detected and result.confidence > 0.7:
            return ImmuneVerdict(
                verdict_id=generate_uuid_v7(),
                check_type=CheckType.SHERIFF,
                tier=Tier.DEEP_SCAN,
                skill_name=payload.skill_name,
                session_id=payload.session_id,
                outcome=Outcome.BLOCK,
                block_reason=BlockReason.IPI_DETECTED,
                block_detail=f"Deep-scan threat: {result.threat_type}",
                latency_ms=elapsed_ms,
                alert_severity=AlertSeverity.IMMUNE_BLOCK_DEEP,
            )
        return ImmuneVerdict(
            verdict_id=generate_uuid_v7(),
            check_type=CheckType.SHERIFF,
            tier=Tier.DEEP_SCAN,
            skill_name=payload.skill_name,
            session_id=payload.session_id,
            outcome=Outcome.PASS,
            latency_ms=elapsed_ms,
        )
    except asyncio.TimeoutError:
        return None
    except Exception:
        return None


if __name__ == "__main__":
    print("ok")
