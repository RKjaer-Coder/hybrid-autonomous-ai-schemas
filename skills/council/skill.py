from __future__ import annotations

import json
import uuid
from dataclasses import asdict
from typing import Optional

from council.context_budget import build_context_packet
from council.orchestrator import run_tier1_deliberation
from council.types import CouncilVerdict, DecisionType
from harness_variants import HarnessVariantManager
from skills.db_manager import DatabaseManager
from skills.hermes_dispatcher import HermesSubagentDispatcher
from skills.hermes_interfaces import HermesDelegateAPI


class CouncilSkill:
    def __init__(self, delegate_api: HermesDelegateAPI, db_manager: DatabaseManager):
        self._dispatcher = HermesSubagentDispatcher(delegate_api)
        self._db = db_manager
        self._harness_variants = HarnessVariantManager(str(db_manager.data_dir / "telemetry.db"))

    def deliberate(self, decision_type: str, subject_id: str, context: str, source_briefs: list | None = None) -> CouncilVerdict:
        try:
            dt = DecisionType(decision_type)
            packet = build_context_packet(dt, subject_id, context, source_briefs)
            verdict = run_tier1_deliberation(packet, self._dispatcher)
        except Exception as exc:
            self._log_trace(
                task_id=subject_id,
                role="council_deliberation",
                action_name="deliberate",
                intent_goal=f"Deliberate {decision_type} for {subject_id}",
                payload={"error": str(exc), "decision_type": decision_type},
                context_assembled=context,
                retrieval_queries=list(source_briefs or []),
                judge_verdict="FAIL",
                judge_reasoning=f"Council deliberation failed: {exc}",
                source_chain_id=subject_id,
            )
            raise

        conn = self._db.get_connection("strategic_memory")
        conn.execute(
            "INSERT INTO council_verdicts (verdict_id, tier_used, decision_type, recommendation, confidence, reasoning_summary, dissenting_views, minority_positions, full_debate_record, cost_usd, project_id, outcome_record, da_quality_score, da_assessment, tie_break, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                verdict.verdict_id,
                verdict.tier_used,
                verdict.decision_type.value,
                verdict.recommendation.value,
                verdict.confidence,
                verdict.reasoning_summary,
                verdict.dissenting_views,
                json.dumps(verdict.minority_positions) if verdict.minority_positions else None,
                verdict.full_debate_record,
                verdict.cost_usd,
                verdict.project_id,
                json.dumps(verdict.outcome_record) if verdict.outcome_record else None,
                verdict.da_quality_score,
                json.dumps([{"objection": d.objection, "tag": d.tag.value, "reasoning": d.reasoning} for d in verdict.da_assessment] if verdict.da_assessment else None),
                1 if verdict.tie_break else 0,
                verdict.created_at,
            ),
        )
        conn.commit()
        self._log_trace(
            task_id=subject_id,
            role="council_deliberation",
            action_name="deliberate",
            intent_goal=f"Deliberate {decision_type} for {subject_id}",
            payload=asdict(verdict),
            context_assembled=context,
            retrieval_queries=list(source_briefs or []),
            judge_reasoning=f"Council produced {verdict.recommendation.value} at confidence {verdict.confidence:.2f}.",
            source_chain_id=subject_id,
        )
        return verdict

    def _log_trace(
        self,
        *,
        task_id: str,
        role: str,
        action_name: str,
        intent_goal: str,
        payload: object,
        context_assembled: str,
        retrieval_queries: list[str],
        judge_verdict: str = "PASS",
        judge_reasoning: str | None = None,
        source_chain_id: str | None = None,
    ) -> None:
        if not self._harness_variants.available:
            return
        self._harness_variants.log_skill_action_trace(
            task_id=task_id,
            role=role,
            skill_name="council",
            action_name=action_name,
            intent_goal=intent_goal,
            action_payload=payload,
            context_assembled=context_assembled,
            retrieval_queries=retrieval_queries,
            judge_verdict=judge_verdict,
            judge_reasoning=judge_reasoning,
            source_chain_id=source_chain_id,
            model_used="council-tier1",
        )


_SKILL: Optional[CouncilSkill] = None


def configure_skill(delegate_api: HermesDelegateAPI, db_manager: DatabaseManager):
    global _SKILL
    _SKILL = CouncilSkill(delegate_api, db_manager)


def council_entry(action: str, **kwargs):
    if _SKILL is None:
        raise RuntimeError("council skill not configured")
    if action == "deliberate":
        return _SKILL.deliberate(kwargs["decision_type"], kwargs["subject_id"], kwargs["context"], kwargs.get("source_briefs"))
    raise ValueError(f"Unknown action: {action}")
