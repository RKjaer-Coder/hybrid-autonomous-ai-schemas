from __future__ import annotations

import datetime
import json
import threading
import time
import uuid
from dataclasses import asdict
from typing import Optional

from council.context_budget import build_context_packet
from council.orchestrator import run_tier1_deliberation, run_tier2_deliberation
from council.types import CouncilDeliberationConfig, CouncilTier, CouncilVerdict, DecisionType, Recommendation
from harness_variants import HarnessVariantManager
from skills.db_manager import DatabaseManager
from skills.hermes_dispatcher import HermesMixtureDispatcher, HermesSubagentDispatcher
from skills.hermes_interfaces import HermesDelegateAPI, HermesMixtureAPI


class _Tier2Coordinator:
    """Single-process FIFO gate that enforces the spec's max-1 Tier 2 concurrency."""

    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._active: str | None = None
        self._queue: list[str] = []

    def acquire(self, request_id: str, timeout_seconds: float) -> tuple[bool, int]:
        start = time.monotonic()
        with self._condition:
            self._queue.append(request_id)
            backlog_depth = max(0, len(self._queue) - 1)
            while self._active is not None or self._queue[0] != request_id:
                remaining = timeout_seconds - (time.monotonic() - start)
                if remaining <= 0:
                    self._queue = [item for item in self._queue if item != request_id]
                    self._condition.notify_all()
                    return False, backlog_depth
                self._condition.wait(timeout=remaining)
            self._active = request_id
            self._queue.pop(0)
            return True, backlog_depth

    def release(self, request_id: str) -> None:
        with self._condition:
            if self._active == request_id:
                self._active = None
            self._condition.notify_all()


_TIER2_COORDINATOR = _Tier2Coordinator()


class CouncilSkill:
    def __init__(self, delegate_api: HermesDelegateAPI, db_manager: DatabaseManager, mixture_api: HermesMixtureAPI | None = None):
        if mixture_api is None and isinstance(delegate_api, HermesMixtureAPI):
            mixture_api = delegate_api
        self._dispatcher = HermesSubagentDispatcher(delegate_api)
        self._mixture_dispatcher = HermesMixtureDispatcher(mixture_api) if mixture_api is not None else None
        self._db = db_manager
        self._harness_variants = HarnessVariantManager(str(db_manager.data_dir / "telemetry.db"))

    def deliberate(
        self,
        decision_type: str,
        subject_id: str,
        context: str,
        source_briefs: list | None = None,
        deliberation_config: dict | CouncilDeliberationConfig | None = None,
    ) -> CouncilVerdict:
        try:
            dt = DecisionType(decision_type)
            packet = build_context_packet(dt, subject_id, context, source_briefs)
            config = self._normalize_config(deliberation_config)
            tier1_verdict = run_tier1_deliberation(packet, self._dispatcher)
            verdict = self._resolve_verdict(packet, tier1_verdict, context, config)
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

        self._persist_verdict(verdict)
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
            model_used=self._trace_model_used(verdict, config),
        )
        return verdict

    def _resolve_verdict(
        self,
        packet,
        tier1_verdict: CouncilVerdict,
        raw_context: str,
        config: CouncilDeliberationConfig,
    ) -> CouncilVerdict:
        if not self._should_attempt_tier2(packet, config, tier1_verdict):
            return tier1_verdict
        if self._mixture_dispatcher is None:
            return self._escalated_unavailable_verdict(
                tier1_verdict,
                "Tier 2 requested but Hermes mixture_of_agents runtime is not configured.",
            )
        models = list(config.tier2_models or [])
        if len(models) < 2:
            return self._escalated_unavailable_verdict(
                tier1_verdict,
                "Tier 2 requested but fewer than two distinct Tier 2 models were configured.",
            )

        estimated_cost = max(0.0, float(config.tier2_estimated_cost_usd))
        g3_status = (config.g3_status or "").upper()
        if estimated_cost > 0.0:
            if g3_status in {"DENIED", "DENY", "BLOCKED"}:
                denied = self._degraded_tier1_verdict(
                    tier1_verdict,
                    f"Tier 2 spend denied at G3; degraded to enhanced Tier 1 with original estimate ${estimated_cost:.2f}.",
                )
                self._insert_alert(
                    tier="T2",
                    alert_type="COUNCIL_DEGRADED_G3_DENIED",
                    content=(
                        f"{packet.decision_type.value} for {packet.subject_id} degraded after G3 denial; "
                        f"estimated_cost_usd={estimated_cost:.2f}"
                    ),
                )
                self._maybe_emit_cost_pattern_alert(packet.decision_type.value)
                return denied
            if g3_status not in {"APPROVED", "APPROVE"}:
                gate_id = self._ensure_g3_gate(packet, raw_context, tier1_verdict, models, estimated_cost)
                self._insert_alert(
                    tier="T2",
                    alert_type="COUNCIL_TIER2_G3_PENDING",
                    content=(
                        f"Tier 2 spend approval required for {packet.decision_type.value}/{packet.subject_id}; "
                        f"gate_id={gate_id} estimated_cost_usd={estimated_cost:.2f}"
                    ),
                )
                return self._escalated_unavailable_verdict(
                    tier1_verdict,
                    f"Tier 2 spend approval is pending at G3 (gate {gate_id}) for estimated cost ${estimated_cost:.2f}.",
                )

        request_id = f"{packet.subject_id}:{uuid.uuid4()}"
        acquired, backlog_depth = _TIER2_COORDINATOR.acquire(request_id, timeout_seconds=max(0.1, config.queue_timeout_seconds))
        if backlog_depth > 3:
            self._insert_alert(
                tier="T2",
                alert_type="COUNCIL_BACKLOG",
                content=f"Tier 2 backlog depth reached {backlog_depth} while handling {packet.decision_type.value}.",
            )
        if not acquired:
            return self._escalated_unavailable_verdict(
                tier1_verdict,
                f"Tier 2 queue wait exceeded {config.queue_timeout_seconds:.1f}s; returning Tier 1 escalation instead.",
            )
        try:
            return run_tier2_deliberation(
                packet,
                self._mixture_dispatcher,
                models=models,
                estimated_cost_usd=estimated_cost,
                tier1_verdict=tier1_verdict,
            )
        finally:
            _TIER2_COORDINATOR.release(request_id)

    @staticmethod
    def _normalize_config(raw: dict | CouncilDeliberationConfig | None) -> CouncilDeliberationConfig:
        if raw is None:
            return CouncilDeliberationConfig()
        if isinstance(raw, CouncilDeliberationConfig):
            return raw
        requested_tier = raw.get("requested_tier")
        if isinstance(requested_tier, str):
            normalized = requested_tier.strip().lower()
            if normalized in {"", "auto"}:
                requested_tier = None
            elif normalized in {"1", "tier1", "tier_1"}:
                requested_tier = CouncilTier.TIER_1
            elif normalized in {"2", "tier2", "tier_2"}:
                requested_tier = CouncilTier.TIER_2
            else:
                raise ValueError(f"Unknown requested_tier: {requested_tier}")
        elif isinstance(requested_tier, int):
            requested_tier = CouncilTier(requested_tier)
        elif requested_tier is not None and not isinstance(requested_tier, CouncilTier):
            raise ValueError(f"Unsupported requested_tier value: {requested_tier!r}")
        return CouncilDeliberationConfig(
            requested_tier=requested_tier,
            operator_requested=bool(raw.get("operator_requested", False)),
            tier2_models=list(raw.get("tier2_models") or []),
            tier2_estimated_cost_usd=float(raw.get("tier2_estimated_cost_usd", 0.0)),
            mechanism=raw.get("mechanism"),
            validated_mechanism_count=(
                int(raw["validated_mechanism_count"])
                if raw.get("validated_mechanism_count") is not None
                else None
            ),
            cashflow_estimate_high_usd=(
                float(raw["cashflow_estimate_high_usd"])
                if raw.get("cashflow_estimate_high_usd") is not None
                else None
            ),
            external_user_impact=bool(raw.get("external_user_impact", False)),
            security_sensitive=bool(raw.get("security_sensitive", False)),
            g3_status=raw.get("g3_status"),
            queue_timeout_seconds=float(raw.get("queue_timeout_seconds", 30.0)),
        )

    @staticmethod
    def _should_attempt_tier2(packet, config: CouncilDeliberationConfig, tier1_verdict: CouncilVerdict) -> bool:
        if config.requested_tier == CouncilTier.TIER_1:
            return False
        if config.requested_tier == CouncilTier.TIER_2:
            return True
        if config.operator_requested:
            return True
        if tier1_verdict.confidence < 0.60:
            return True
        if config.validated_mechanism_count is not None and config.validated_mechanism_count < 5:
            return True
        if config.tier2_estimated_cost_usd > 50.0:
            return True
        if config.security_sensitive or config.external_user_impact:
            return True
        if (config.cashflow_estimate_high_usd or 0.0) > 5000.0:
            return True
        return packet.decision_type in {
            DecisionType.GO_NO_GO,
            DecisionType.KILL_REC,
            DecisionType.SYSTEM_CRITICAL,
        } and bool(config.tier2_models)

    def _persist_verdict(self, verdict: CouncilVerdict) -> None:
        conn = self._db.get_connection("strategic_memory")
        conn.execute(
            """
            INSERT INTO council_verdicts (
                verdict_id, tier_used, decision_type, recommendation, confidence,
                reasoning_summary, dissenting_views, minority_positions,
                full_debate_record, cost_usd, project_id, outcome_record,
                da_quality_score, da_assessment, tie_break, degraded,
                confidence_cap, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
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
                1 if verdict.degraded else 0,
                verdict.confidence_cap,
                verdict.created_at,
            ),
        )
        conn.commit()

    def _ensure_g3_gate(
        self,
        packet,
        raw_context: str,
        tier1_verdict: CouncilVerdict,
        models: list[str],
        estimated_cost: float,
    ) -> str:
        operator = self._db.get_connection("operator_digest")
        trigger = f"council_tier2:{packet.decision_type.value}:{packet.subject_id}"
        existing = operator.execute(
            """
            SELECT gate_id
            FROM gate_log
            WHERE gate_type = 'G3' AND trigger_description = ? AND status = 'PENDING'
            ORDER BY created_at DESC, gate_id DESC
            LIMIT 1
            """,
            (trigger,),
        ).fetchone()
        if existing is not None:
            return str(existing["gate_id"])
        gate_id = str(uuid.uuid4())
        now = self._utc_now()
        expires_at = self._to_iso(self._parse_ts(now) + datetime.timedelta(hours=6))
        context_packet = {
            "subject_id": packet.subject_id,
            "decision_type": packet.decision_type.value,
            "tier2_models": models,
            "estimated_cost_usd": estimated_cost,
            "tier1_preassessment": {
                "verdict_id": tier1_verdict.verdict_id,
                "recommendation": tier1_verdict.recommendation.value,
                "confidence": tier1_verdict.confidence,
                "reasoning_summary": tier1_verdict.reasoning_summary,
            },
            "context_excerpt": raw_context[:1200],
        }
        operator.execute(
            """
            INSERT INTO gate_log (
                gate_id, gate_type, trigger_description, context_packet, project_id,
                status, timeout_hours, operator_response, created_at, responded_at, expires_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                gate_id,
                "G3",
                trigger,
                json.dumps(context_packet, sort_keys=True),
                packet.subject_id,
                "PENDING",
                6.0,
                None,
                now,
                None,
                expires_at,
            ),
        )
        operator.commit()
        return gate_id

    def _insert_alert(self, *, tier: str, alert_type: str, content: str) -> str:
        operator = self._db.get_connection("operator_digest")
        now = self._utc_now()
        recent_cutoff = self._to_iso(self._parse_ts(now) - datetime.timedelta(hours=6))
        existing = operator.execute(
            """
            SELECT alert_id
            FROM alert_log
            WHERE tier = ? AND alert_type = ? AND content = ? AND created_at >= ?
            ORDER BY created_at DESC, alert_id DESC
            LIMIT 1
            """,
            (tier, alert_type, content, recent_cutoff),
        ).fetchone()
        if existing is not None:
            return str(existing["alert_id"])
        alert_id = str(uuid.uuid4())
        operator.execute(
            """
            INSERT INTO alert_log (
                alert_id, tier, alert_type, content, channel_delivered,
                suppressed, acknowledged, acknowledged_at, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (alert_id, tier, alert_type, content, "CLI", 0, 0, None, now),
        )
        operator.commit()
        return alert_id

    def _maybe_emit_cost_pattern_alert(self, decision_type: str) -> None:
        operator = self._db.get_connection("operator_digest")
        since = self._to_iso(self._parse_ts(self._utc_now()) - datetime.timedelta(days=30))
        row = operator.execute(
            """
            SELECT COUNT(*) AS denied_count
            FROM alert_log
            WHERE alert_type = 'COUNCIL_DEGRADED_G3_DENIED'
              AND content LIKE ?
              AND created_at >= ?
            """,
            (f"{decision_type}%", since),
        ).fetchone()
        if int(row["denied_count"] or 0) > 3:
            self._insert_alert(
                tier="T2",
                alert_type="COUNCIL_COST_PATTERN",
                content=f"{decision_type} repeatedly triggered G3-denied Tier 2 degradation in the last 30 days.",
            )

    @staticmethod
    def _escalated_unavailable_verdict(base: CouncilVerdict, note: str) -> CouncilVerdict:
        return CouncilVerdict(
            verdict_id=base.verdict_id,
            tier_used=base.tier_used,
            decision_type=base.decision_type,
            recommendation=Recommendation.ESCALATE,
            confidence=min(base.confidence, 0.59),
            reasoning_summary=f"{base.reasoning_summary} Tier 2 follow-on required: {note}",
            dissenting_views=base.dissenting_views,
            minority_positions=base.minority_positions,
            full_debate_record=base.full_debate_record,
            cost_usd=base.cost_usd,
            project_id=base.project_id,
            outcome_record=base.outcome_record,
            brief_quality=base.brief_quality,
            da_assessment=base.da_assessment,
            da_quality_score=base.da_quality_score,
            tie_break=base.tie_break,
            degraded=base.degraded,
            confidence_cap=base.confidence_cap,
            created_at=base.created_at,
        )

    @staticmethod
    def _degraded_tier1_verdict(base: CouncilVerdict, note: str) -> CouncilVerdict:
        return CouncilVerdict(
            verdict_id=base.verdict_id,
            tier_used=1,
            decision_type=base.decision_type,
            recommendation=base.recommendation,
            confidence=min(base.confidence, 0.70),
            reasoning_summary=f"{base.reasoning_summary} {note}",
            dissenting_views=base.dissenting_views,
            minority_positions=base.minority_positions,
            full_debate_record=base.full_debate_record,
            cost_usd=0.0,
            project_id=base.project_id,
            outcome_record=base.outcome_record,
            brief_quality=base.brief_quality,
            da_assessment=base.da_assessment,
            da_quality_score=base.da_quality_score,
            tie_break=base.tie_break,
            degraded=True,
            confidence_cap=0.70,
            created_at=base.created_at,
        )

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
        model_used: str = "council-tier1",
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
            model_used=model_used,
        )

    @staticmethod
    def _trace_model_used(verdict: CouncilVerdict, config: CouncilDeliberationConfig) -> str:
        if verdict.tier_used == 2:
            return "council-tier2:" + ",".join(config.tier2_models or [])
        if verdict.degraded:
            return "council-tier1-degraded"
        return "council-tier1"

    @staticmethod
    def _utc_now() -> str:
        return datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()

    @staticmethod
    def _parse_ts(value: str) -> datetime.datetime:
        return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))

    @staticmethod
    def _to_iso(value: datetime.datetime) -> str:
        if value.tzinfo is None:
            value = value.replace(tzinfo=datetime.timezone.utc)
        return value.astimezone(datetime.timezone.utc).replace(microsecond=0).isoformat()


_SKILL: Optional[CouncilSkill] = None


def configure_skill(delegate_api: HermesDelegateAPI, db_manager: DatabaseManager):
    global _SKILL
    mixture_api = delegate_api if isinstance(delegate_api, HermesMixtureAPI) else None
    _SKILL = CouncilSkill(delegate_api, db_manager, mixture_api=mixture_api)


def council_entry(action: str, **kwargs):
    if _SKILL is None:
        raise RuntimeError("council skill not configured")
    if action == "deliberate":
        return _SKILL.deliberate(
            kwargs["decision_type"],
            kwargs["subject_id"],
            kwargs["context"],
            kwargs.get("source_briefs"),
            kwargs.get("deliberation_config"),
        )
    raise ValueError(f"Unknown action: {action}")
