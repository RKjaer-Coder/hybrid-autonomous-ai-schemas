from __future__ import annotations

from abc import ABC, abstractmethod
import json
import uuid
from typing import List, Optional, Tuple

from council.context_budget import ROLE_OUTPUT_LIMITS
from council.da_scorer import parse_da_assessment, score_da_quality
from council.prompts.common import (
    format_batch_a_for_da,
    format_context_packet,
    parse_json_output,
)
from council.prompts.role_critic import CRITIC_SYSTEM_PROMPT
from council.prompts.role_devils_advocate import DEVILS_ADVOCATE_SYSTEM_PROMPT
from council.prompts.role_realist import REALIST_SYSTEM_PROMPT
from council.prompts.role_strategist import STRATEGIST_SYSTEM_PROMPT
from council.prompts.synthesis import SYNTHESIS_OUTPUT_SCHEMA, SYNTHESIS_SYSTEM_PROMPT
from council.prompts.tier2 import TIER2_MIXTURE_PROMPT, TIER2_OUTPUT_SCHEMA
from council.types import CouncilVerdict, DEFAULT_ROLE_WEIGHTS, DecisionType, RoleName, RoleOutput, Recommendation, iso_utc_now
from council.validators import validate_role_output, validate_verdict


class SubagentDispatcher(ABC):
    @abstractmethod
    def dispatch_parallel(self, prompts: List[Tuple[RoleName, str, str]]) -> List[RoleOutput]:
        ...

    @abstractmethod
    def dispatch_sequential(self, role: RoleName, system_prompt: str, user_prompt: str) -> RoleOutput:
        ...

    @abstractmethod
    def dispatch_synthesis(self, system_prompt: str, user_prompt: str) -> str:
        ...


class MixtureDispatcher(ABC):
    @abstractmethod
    def dispatch_mixture(self, prompt: str, models: List[str], rounds: int = 3) -> str:
        ...


class MockDispatcher(SubagentDispatcher):
    def __init__(self, identical: bool = False, bad_json: bool = False, low_confidence: bool = False) -> None:
        self.identical = identical
        self.bad_json = bad_json
        self.low_confidence = low_confidence
        self.parallel_called = 0
        self.sequential_called = 0
        self.synthesis_called = 0
        self.last_sequential_prompt = ""
        self.last_synthesis_prompt = ""

    def dispatch_parallel(self, prompts: List[Tuple[RoleName, str, str]]) -> List[RoleOutput]:
        self.parallel_called += 1
        out: List[RoleOutput] = []
        for role, _, _ in prompts:
            if self.identical:
                payload = {"role": role.value, "case_for": "x", "market_fit_score": 0.5, "timing_assessment": "x", "strategic_alignment": "x", "key_assumption": "x"}
            elif role == RoleName.STRATEGIST:
                payload = {"role": "strategist", "case_for": "Strong timing and distribution edge.", "market_fit_score": 0.78, "timing_assessment": "Demand inflecting now", "strategic_alignment": "Fits autonomous local-first stack", "key_assumption": "Conversion from pilot users"}
            elif role == RoleName.CRITIC:
                payload = {"role": "critic", "case_against": "Customer acquisition cost may exceed LTV.", "execution_risk": "Pipeline fragility", "market_risk": "Crowded alternatives", "fatal_dependency": "Stable third-party API", "risk_severity": 0.72}
            else:
                payload = {"role": "realist", "execution_requirements": "Needs robust ingestion and eval loop.", "compute_needs": "Hybrid local with occasional cloud burst", "time_to_revenue_days": 60, "capital_required_usd": 1200.0, "blocking_prerequisite": "Validated ICP list", "feasibility_score": 0.66}
            out.append(RoleOutput(role=role, content=json.dumps(payload), token_count=20, max_tokens=ROLE_OUTPUT_LIMITS[role]))
        return out

    def dispatch_sequential(self, role: RoleName, system_prompt: str, user_prompt: str) -> RoleOutput:
        self.sequential_called += 1
        self.last_sequential_prompt = system_prompt + "\n" + user_prompt
        payload = {
            "role": "devils_advocate",
            "shared_assumption": "All assume immediate distribution channel success",
            "novel_risk": "Platform policy shift can remove integration path",
            "material_disagreement": "Disagree with strategist market timing certainty",
            "alternative_interpretation": "If channels close, revenue is delayed >12 months",
        }
        return RoleOutput(role=role, content=json.dumps(payload), token_count=30, max_tokens=ROLE_OUTPUT_LIMITS[role])

    def dispatch_synthesis(self, system_prompt: str, user_prompt: str) -> str:
        self.synthesis_called += 1
        self.last_synthesis_prompt = system_prompt + "\n" + user_prompt
        if self.bad_json:
            return "not json"
        conf = 0.52 if self.low_confidence else 0.78
        return json.dumps(
            {
                "tier_used": 1,
                "decision_type": "opportunity_screen",
                "recommendation": "PURSUE",
                "confidence": conf,
                "reasoning_summary": "Critic dependency risk is manageable; realist prerequisites are already met.",
                "dissenting_views": "DA warns distribution channel policy could shift.",
                "da_assessment": [
                    {
                        "objection": "Platform policy shift",
                        "tag": "acknowledged",
                        "reasoning": "Novel but not recommendation changing.",
                    }
                ],
                "tie_break": False,
                "risk_watch": ["Channel policy updates"],
            }
        )


class MockMixtureDispatcher(MixtureDispatcher):
    def __init__(self, bad_json: bool = False, low_confidence: bool = False) -> None:
        self.bad_json = bad_json
        self.low_confidence = low_confidence
        self.calls: List[dict] = []

    def dispatch_mixture(self, prompt: str, models: List[str], rounds: int = 3) -> str:
        self.calls.append({"prompt": prompt, "models": list(models), "rounds": rounds})
        if self.bad_json:
            return "not json"
        conf = 0.56 if self.low_confidence else 0.84
        return json.dumps(
            {
                "tier_used": 2,
                "decision_type": "opportunity_screen",
                "recommendation": "PURSUE",
                "confidence": conf,
                "reasoning_summary": "Two models converged on timing and execution leverage, while one minority position argued the distribution dependency is underpriced.",
                "dissenting_views": "A dissenting model argues platform dependency could erase the moat before monetization stabilizes.",
                "minority_positions": [
                    "Reject until distribution risk is reduced through an owned channel.",
                ],
                "full_debate_record": "Round1: two pursue, one reject. Round2: disagreement centered on platform dependency. Round3: synthesis favored pursue with explicit risk watch.",
                "cost_usd": 0.0,
                "da_assessment": [
                    {
                        "objection": "Platform dependency remains underpriced.",
                        "tag": "acknowledged",
                        "reasoning": "Recorded as the main dissent and retained in risk watch, but not recommendation changing.",
                    }
                ],
                "tie_break": False,
                "risk_watch": ["Platform dependency", "Owned distribution progress"],
            }
        )


def _uuid7_str() -> str:
    generator = getattr(uuid, "uuid7", None)
    return str(generator() if callable(generator) else uuid.uuid4())


def _check_auto_escalation(verdict_data: dict) -> dict:
    if verdict_data.get("confidence", 0.0) < 0.60:
        verdict_data = dict(verdict_data)
        verdict_data["recommendation"] = Recommendation.ESCALATE.value
    return verdict_data


def _apply_confidence_cap(verdict_data: dict, cap: float = 0.70) -> dict:
    verdict_data = dict(verdict_data)
    if verdict_data.get("confidence", 0.0) > cap:
        verdict_data["confidence"] = cap
    return verdict_data


def _build_verdict(
    *,
    verdict_data: dict,
    context,
    tier_used: int,
    cost_usd: float,
    degraded: bool = False,
    confidence_cap: float | None = None,
) -> CouncilVerdict:
    raw_confidence = float(verdict_data["confidence"])
    final_confidence = min(raw_confidence, confidence_cap) if confidence_cap is not None else raw_confidence
    da_assessment = parse_da_assessment(verdict_data.get("da_assessment", []))
    da_quality_score = score_da_quality(da_assessment)
    return CouncilVerdict(
        verdict_id=_uuid7_str(),
        tier_used=tier_used,
        decision_type=context.decision_type,
        recommendation=Recommendation(verdict_data["recommendation"]),
        confidence=final_confidence,
        reasoning_summary=verdict_data["reasoning_summary"],
        dissenting_views=verdict_data["dissenting_views"],
        minority_positions=verdict_data.get("minority_positions"),
        full_debate_record=verdict_data.get("full_debate_record"),
        cost_usd=cost_usd,
        project_id=context.subject_id,
        da_assessment=da_assessment,
        da_quality_score=da_quality_score,
        tie_break=bool(verdict_data.get("tie_break", False)),
        degraded=degraded,
        confidence_cap=confidence_cap,
        created_at=iso_utc_now(),
    )


def _assert_noncollapse(batch_a_outputs: List[RoleOutput]) -> None:
    contents = [o.content for o in batch_a_outputs]
    if len(set(contents)) == 1:
        raise ValueError("Anti-collapse violation: Batch A outputs are identical")


def _verify_isolation(batch_a_outputs: List[RoleOutput]) -> None:
    role_names = {o.role.value.lower() for o in batch_a_outputs}
    for output in batch_a_outputs:
        content_lower = output.content.lower()
        for other in (role_names - {output.role.value.lower()}):
            if f"the {other}" in content_lower and f"{other}'s" in content_lower:
                raise ValueError(
                    f"Isolation violation: {output.role.value} references {other}'s output"
                )
    for i, first in enumerate(batch_a_outputs):
        first_words = set(first.content.lower().split())
        for j, second in enumerate(batch_a_outputs):
            if i >= j:
                continue
            second_words = set(second.content.lower().split())
            if not first_words or not second_words:
                continue
            similarity = len(first_words & second_words) / len(first_words | second_words)
            if similarity > 0.85:
                raise ValueError(
                    f"Isolation violation: {first.role.value} and {second.role.value} too similar ({similarity:.0%})"
                )


def run_tier2_deliberation(
    context,
    dispatcher: MixtureDispatcher,
    models: List[str],
    estimated_cost_usd: float = 0.0,
    tier1_verdict: CouncilVerdict | None = None,
) -> CouncilVerdict:
    if context.token_count > context.max_tokens:
        raise ValueError("Context token budget exceeded")
    if len(models) < 2:
        raise ValueError("Tier 2 requires at least two distinct models")
    if len(set(models)) != len(models):
        raise ValueError("Tier 2 models must be distinct")
    tier1_summary = "None"
    if tier1_verdict is not None:
        tier1_summary = json.dumps(
            {
                "recommendation": tier1_verdict.recommendation.value,
                "confidence": tier1_verdict.confidence,
                "reasoning_summary": tier1_verdict.reasoning_summary,
                "dissenting_views": tier1_verdict.dissenting_views,
                "tie_break": tier1_verdict.tie_break,
            },
            sort_keys=True,
        )
    prompt = TIER2_MIXTURE_PROMPT.format(
        context_packet=format_context_packet(context),
        tier1_verdict=tier1_summary,
        model_list=", ".join(models),
        decision_type=context.decision_type.value,
    )
    raw = dispatcher.dispatch_mixture(prompt, models, rounds=3)
    verdict_data = parse_json_output(raw, TIER2_OUTPUT_SCHEMA)
    verdict_data = _check_auto_escalation(verdict_data)
    errors = validate_verdict(verdict_data, context.decision_type)
    hard_errors = [e for e in errors if not e.startswith("warning")]
    if hard_errors:
        raise ValueError("; ".join(hard_errors))
    reported_cost = float(verdict_data.get("cost_usd", estimated_cost_usd))
    return _build_verdict(
        verdict_data=verdict_data,
        context=context,
        tier_used=2,
        cost_usd=max(estimated_cost_usd, reported_cost),
    )


def run_tier1_deliberation(
    context,
    dispatcher: SubagentDispatcher,
    role_weights: Optional[dict] = None,
    g3_denied: bool = False,
) -> CouncilVerdict:
    if context.token_count > context.max_tokens:
        raise ValueError("Context token budget exceeded")
    effective_weights = role_weights or DEFAULT_ROLE_WEIGHTS
    user_prompt = format_context_packet(context)
    prompts = [
        (RoleName.STRATEGIST, STRATEGIST_SYSTEM_PROMPT, user_prompt),
        (RoleName.CRITIC, CRITIC_SYSTEM_PROMPT, user_prompt),
        (RoleName.REALIST, REALIST_SYSTEM_PROMPT, user_prompt),
    ]
    batch_a = dispatcher.dispatch_parallel(prompts)
    _assert_noncollapse(batch_a)
    _verify_isolation(batch_a)
    for out in batch_a:
        if out.token_count > out.max_tokens:
            raise ValueError(f"{out.role.value} exceeded token budget ({out.token_count}>{out.max_tokens})")
        _, errors = validate_role_output(out.content, out.role)
        if errors:
            raise ValueError("; ".join(errors))

    batch_text = format_batch_a_for_da(batch_a)
    da_system = DEVILS_ADVOCATE_SYSTEM_PROMPT.format(batch_a_outputs=batch_text)
    da_out = dispatcher.dispatch_sequential(RoleName.DEVILS_ADVOCATE, da_system, user_prompt)
    if da_out.token_count > da_out.max_tokens:
        raise ValueError(f"{da_out.role.value} exceeded token budget ({da_out.token_count}>{da_out.max_tokens})")
    _, da_errors = validate_role_output(da_out.content, RoleName.DEVILS_ADVOCATE)
    if da_errors:
        raise ValueError("; ".join(da_errors))

    by_role = {o.role: o.content for o in batch_a}
    weight_text = ", ".join(f"{r.value}: {w:.2f}" for r, w in effective_weights.items())
    syn_system = SYNTHESIS_SYSTEM_PROMPT.format(
        strategist_output=by_role[RoleName.STRATEGIST],
        critic_output=by_role[RoleName.CRITIC],
        realist_output=by_role[RoleName.REALIST],
        da_output=da_out.content,
        decision_type=context.decision_type.value,
    ) + f"\n\nRole reliability weights: {weight_text}"
    raw = dispatcher.dispatch_synthesis(syn_system, user_prompt)
    verdict_data = parse_json_output(raw, SYNTHESIS_OUTPUT_SCHEMA)
    verdict_data = _check_auto_escalation(verdict_data)
    is_degraded = g3_denied
    confidence_cap = 0.70 if g3_denied else None

    errors = validate_verdict(verdict_data, context.decision_type)
    hard_errors = [e for e in errors if not e.startswith("warning")]
    if hard_errors:
        raise ValueError("; ".join(hard_errors))
    return _build_verdict(
        verdict_data=verdict_data,
        context=context,
        tier_used=1,
        cost_usd=0.0,
        degraded=is_degraded,
        confidence_cap=confidence_cap,
    )
