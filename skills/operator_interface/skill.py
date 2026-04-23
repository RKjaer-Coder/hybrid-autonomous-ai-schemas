from __future__ import annotations

import datetime
import json
import uuid
from dataclasses import asdict, dataclass
from typing import Any, Optional

from harness_variants import HarnessVariantManager, VariantEvalResult
from financial_router.types import JWTClaims
from immune.config import load_config
from immune.judge_lifecycle import JudgeLifecycleManager
from runtime_control import RuntimeControlManager
from skills.db_manager import DatabaseManager
from skills.financial_router.skill import FinancialRouterSkill
from skills.config import IntegrationConfig
from skills.milestone_status import evaluate_milestone_status


DAILY_SECTION_ORDER = [
    "PORTFOLIO HEALTH",
    "PIPELINE STATUS",
    "INTELLIGENCE HIGHLIGHTS",
    "SYSTEM HEALTH",
    "PENDING DECISIONS",
    "FINANCIAL SUMMARY",
]

CRITICAL_SECTION_ORDER = [
    "PORTFOLIO HEALTH",
    "PENDING DECISIONS",
    "FINANCIAL SUMMARY",
]

SECTION_WORD_LIMITS = {
    "PORTFOLIO HEALTH": 80,
    "PIPELINE STATUS": 60,
    "INTELLIGENCE HIGHLIGHTS": 100,
    "SYSTEM HEALTH": 80,
    "PENDING DECISIONS": 60,
    "FINANCIAL SUMMARY": 60,
}


@dataclass(frozen=True)
class AlertRecord:
    alert_id: str
    tier: str
    alert_type: str
    content: str
    channel_delivered: str | None
    suppressed: bool
    acknowledged: bool
    created_at: str


@dataclass(frozen=True)
class DigestRecord:
    digest_id: str
    digest_type: str
    content: str
    sections_included: list[str]
    word_count: int
    operator_state: str
    created_at: str


class OperatorInterfaceSkill:
    def __init__(self, db_manager: DatabaseManager):
        self._db = db_manager
        self._financial_router = FinancialRouterSkill(db_manager)
        immune = self._db.get_connection("immune")
        self._judge_lifecycle = JudgeLifecycleManager(
            immune.execute("PRAGMA database_list").fetchone()[2],
            load_config(),
        )
        operator = self._db.get_connection("operator_digest")
        self._runtime_control = RuntimeControlManager(
            operator.execute("PRAGMA database_list").fetchone()[2]
        )
        telemetry = self._db.get_connection("telemetry")
        self._harness_variants = HarnessVariantManager(
            telemetry.execute("PRAGMA database_list").fetchone()[2]
        )

    def alert(
        self,
        tier: str,
        alert_type: str,
        content: str,
        *,
        channel_delivered: str | None = "CLI",
        suppressed: bool = False,
        reference_time: str | None = None,
    ) -> str:
        alert_id = str(uuid.uuid4())
        now = self._resolve_now(reference_time)
        conn = self._db.get_connection("operator_digest")
        delivered_channel = channel_delivered
        should_suppress = suppressed
        if tier != "T3" and not should_suppress:
            if self._should_suppress_duplicate(conn, alert_type, now):
                should_suppress = True
            elif tier == "T2" and self._delivered_t2_count(conn, now) >= 5:
                should_suppress = True
        if should_suppress:
            delivered_channel = None
        conn.execute(
            "INSERT INTO alert_log (alert_id, tier, alert_type, content, channel_delivered, suppressed, acknowledged, acknowledged_at, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (alert_id, tier, alert_type, content, delivered_channel, 1 if should_suppress else 0, 0, None, now),
        )
        conn.commit()
        return alert_id

    def acknowledge_alert(self, alert_id: str, *, reference_time: str | None = None) -> dict:
        now = self._resolve_now(reference_time)
        conn = self._db.get_connection("operator_digest")
        row = conn.execute(
            "SELECT alert_id, acknowledged FROM alert_log WHERE alert_id = ?",
            (alert_id,),
        ).fetchone()
        if row is None:
            raise KeyError(alert_id)
        conn.execute(
            """
            UPDATE alert_log
            SET acknowledged = 1,
                acknowledged_at = COALESCE(acknowledged_at, ?)
            WHERE alert_id = ?
            """,
            (now, alert_id),
        )
        conn.execute(
            "INSERT INTO operator_heartbeat (entry_id, interaction_type, channel, timestamp) VALUES (?, ?, ?, ?)",
            (str(uuid.uuid4()), "message", "CLI", now),
        )
        conn.commit()
        result = {
            "alert_id": alert_id,
            "acknowledged": True,
            "acknowledged_at": now if not bool(row["acknowledged"]) else None,
        }
        self._log_trace(
            task_id=alert_id,
            role="operator_alert_acknowledgement",
            action_name="acknowledge_alert",
            intent_goal=f"Acknowledge operator alert {alert_id}.",
            payload=result,
            context_assembled=f"alert_id={alert_id}; previously_acknowledged={bool(row['acknowledged'])}",
        )
        return result

    def list_alerts(
        self,
        *,
        limit: int = 20,
        tier: str | None = None,
        alert_type: str | None = None,
        unacknowledged_only: bool = False,
        include_suppressed: bool = True,
    ) -> list[dict]:
        conn = self._db.get_connection("operator_digest")
        where: list[str] = []
        params: list[object] = []
        if tier:
            where.append("tier = ?")
            params.append(tier)
        if alert_type:
            where.append("alert_type = ?")
            params.append(alert_type)
        if unacknowledged_only:
            where.append("acknowledged = 0")
        if not include_suppressed:
            where.append("suppressed = 0")
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        rows = conn.execute(
            f"""
            SELECT alert_id, tier, alert_type, content, channel_delivered, suppressed, acknowledged, created_at
            FROM alert_log
            {where_sql}
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (*params, limit),
        ).fetchall()
        return [
            asdict(
                AlertRecord(
                    alert_id=row["alert_id"],
                    tier=row["tier"],
                    alert_type=row["alert_type"],
                    content=row["content"],
                    channel_delivered=row["channel_delivered"],
                    suppressed=bool(row["suppressed"]),
                    acknowledged=bool(row["acknowledged"]),
                    created_at=row["created_at"],
                )
            )
            for row in rows
        ]

    def list_quarantined_responses(
        self,
        *,
        limit: int = 20,
        pending_review_only: bool = False,
    ) -> list[dict]:
        immune = self._db.get_connection("immune")
        where_sql = "WHERE review_status = 'PENDING'" if pending_review_only else ""
        rows = immune.execute(
            f"""
            SELECT *
            FROM quarantined_responses
            {where_sql}
            ORDER BY quarantined_at DESC, quarantine_id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [self._quarantine_row_to_dict(row) for row in rows]

    def list_g3_approval_requests(
        self,
        *,
        limit: int = 20,
        status: str | None = None,
        reference_time: str | None = None,
    ) -> list[dict[str, Any]]:
        return self._financial_router.list_g3_approval_requests(
            limit=limit,
            status=status,
            reference_time=reference_time,
        )

    def review_g3_approval_request(
        self,
        request_id: str,
        decision: str,
        *,
        operator_notes: str | None = None,
        reference_time: str | None = None,
    ) -> dict[str, Any]:
        now = self._resolve_now(reference_time)
        result = self._financial_router.review_g3_approval_request(
            request_id,
            decision,
            operator_notes=operator_notes,
            reference_time=now,
        )
        operator = self._db.get_connection("operator_digest")
        operator.execute(
            "INSERT INTO operator_heartbeat (entry_id, interaction_type, channel, timestamp) VALUES (?, ?, ?, ?)",
            (str(uuid.uuid4()), "gate_response", "CLI", now),
        )
        operator.commit()
        return result

    def dispatch_approved_paid_route(
        self,
        *,
        correlation_id: str,
        jwt_claims: dict[str, Any],
        reference_time: str | None = None,
    ) -> dict[str, Any]:
        now = self._resolve_now(reference_time)
        result = self._financial_router.dispatch_approved_paid_route(
            correlation_id=correlation_id,
            jwt=jwt_claims if isinstance(jwt_claims, JWTClaims) else JWTClaims(**jwt_claims),
            reference_time=now,
        )
        operator = self._db.get_connection("operator_digest")
        operator.execute(
            "INSERT INTO operator_heartbeat (entry_id, interaction_type, channel, timestamp) VALUES (?, ?, ?, ?)",
            (str(uuid.uuid4()), "command", "CLI", now),
        )
        operator.commit()
        return result

    def finalize_paid_dispatch(
        self,
        *,
        correlation_id: str,
        final_cost_usd: float,
        provider: str | None = None,
        reference_time: str | None = None,
    ) -> dict[str, Any]:
        now = self._resolve_now(reference_time)
        result = self._financial_router.finalize_paid_dispatch(
            correlation_id=correlation_id,
            final_cost_usd=final_cost_usd,
            provider=provider,
            reference_time=now,
        )
        operator = self._db.get_connection("operator_digest")
        operator.execute(
            "INSERT INTO operator_heartbeat (entry_id, interaction_type, channel, timestamp) VALUES (?, ?, ?, ?)",
            (str(uuid.uuid4()), "command", "CLI", now),
        )
        operator.commit()
        return result

    def review_quarantined_response(
        self,
        quarantine_id: str,
        decision: str,
        *,
        review_notes: str | None = None,
        review_digest_id: str | None = None,
        reference_time: str | None = None,
    ) -> dict:
        now = self._resolve_now(reference_time)
        normalized = decision.upper()
        status_by_decision = {
            "DISCARD": "DISCARDED",
            "REPROCESS": "REPROCESS_APPROVED",
            "REPROCESSED": "REPROCESSED",
        }
        if normalized not in status_by_decision:
            raise ValueError(f"Unknown quarantine review decision: {decision}")

        immune = self._db.get_connection("immune")
        row = immune.execute(
            "SELECT quarantine_id FROM quarantined_responses WHERE quarantine_id = ?",
            (quarantine_id,),
        ).fetchone()
        if row is None:
            raise KeyError(quarantine_id)
        immune.execute(
            """
            UPDATE quarantined_responses
            SET review_status = ?,
                operator_decision = ?,
                review_notes = ?,
                review_digest_id = ?,
                reviewed_at = ?
            WHERE quarantine_id = ?
            """,
            (
                status_by_decision[normalized],
                normalized,
                review_notes,
                review_digest_id,
                now,
                quarantine_id,
            ),
        )
        immune.commit()

        operator = self._db.get_connection("operator_digest")
        operator.execute(
            "INSERT INTO operator_heartbeat (entry_id, interaction_type, channel, timestamp) VALUES (?, ?, ?, ?)",
            (str(uuid.uuid4()), "command", "CLI", now),
        )
        operator.commit()
        updated = immune.execute(
            "SELECT * FROM quarantined_responses WHERE quarantine_id = ?",
            (quarantine_id,),
        ).fetchone()
        assert updated is not None
        result = self._quarantine_row_to_dict(updated)
        positive = normalized in {"REPROCESS", "REPROCESSED"}
        self._log_trace(
            task_id=result["task_id"] or quarantine_id,
            role="operator_quarantine_review",
            action_name="review_quarantined_response",
            intent_goal=f"Review quarantined response {quarantine_id} with decision {normalized}.",
            payload=result,
            context_assembled=(
                f"correlation_id={result['correlation_id']}; decision={normalized}; "
                f"review_digest_id={review_digest_id}"
            ),
            judge_verdict="PASS" if positive else "FAIL",
            judge_reasoning=(
                "Operator approved follow-up processing for quarantined response."
                if positive
                else "Operator discarded quarantined response."
            ),
            training_eligible=positive,
            retention_class="STANDARD" if positive else "FAILURE_AUDIT",
            outcome_score=1.0 if positive else 0.0,
        )
        return result

    def list_judge_fallback_events(self, *, limit: int = 20) -> list[dict]:
        return self._judge_lifecycle.list_events(limit=limit)

    def list_judge_fallback_review_queue(
        self,
        *,
        limit: int = 20,
        review_status: str | None = None,
    ) -> list[dict]:
        return self._judge_lifecycle.list_review_queue(limit=limit, review_status=review_status)

    def restart_judge_after_deadlock(
        self,
        *,
        event_id: str | None = None,
        reference_time: str | None = None,
    ) -> dict[str, Any]:
        now = self._resolve_now(reference_time)
        result = self._judge_lifecycle.restart_after_deadlock(
            event_id=event_id,
            reference_time=now,
        )
        operator = self._db.get_connection("operator_digest")
        operator.execute(
            "INSERT INTO operator_heartbeat (entry_id, interaction_type, channel, timestamp) VALUES (?, ?, ?, ?)",
            (str(uuid.uuid4()), "command", "CLI", now),
        )
        operator.commit()
        self._log_trace(
            task_id=result["event_id"],
            role="operator_judge_deadlock_restart",
            action_name="restart_judge_after_deadlock",
            intent_goal=f"Attempt operator restart for judge deadlock event {result['event_id']}.",
            payload=result,
            context_assembled=f"requested_event_id={event_id or result['event_id']}; status={result['status']}",
            judge_verdict="PASS" if result["status"] == "CLEARED" else "FAIL",
            judge_reasoning=(
                "Judge deadlock cleared and fallback mode exited."
                if result["status"] == "CLEARED"
                else "Judge restart requested but deadlock persisted."
            ),
            training_eligible=result["status"] == "CLEARED",
            retention_class="STANDARD" if result["status"] == "CLEARED" else "FAILURE_AUDIT",
            outcome_score=1.0 if result["status"] == "CLEARED" else 0.0,
        )
        return result

    def runtime_status(self) -> dict[str, Any]:
        return self._runtime_control.status()

    def milestone_status(self) -> dict[str, Any]:
        config = IntegrationConfig(data_dir=str(self._db.data_dir))
        return evaluate_milestone_status(config, db_manager=self._db)

    def workspace_overview(self) -> dict[str, Any]:
        now = self._utc_now()
        load_snapshot = self._record_operator_load_snapshot(now)
        health = self._system_health_snapshot(now, load_snapshot)
        operator = self._db.get_connection("operator_digest")
        pending = operator.execute(
            """
            SELECT item_type, label, priority, expires_at
            FROM (
                SELECT 'gate' AS item_type, gate_type AS label, gate_type AS priority, expires_at
                FROM gate_log
                WHERE status = 'PENDING'
                UNION ALL
                SELECT 'harvest' AS item_type, target_interface AS label, priority, expires_at
                FROM harvest_requests
                WHERE status = 'PENDING'
            )
            ORDER BY expires_at ASC
            LIMIT 10
            """
        ).fetchall()
        return {
            "runtime_status": self.runtime_status(),
            "pending_decisions": [dict(row) for row in pending],
            "pending_g3_requests": self.list_g3_approval_requests(limit=5, status="PENDING", reference_time=now),
            "pending_quarantines": self.list_quarantined_responses(limit=5, pending_review_only=True),
            "execution_traces": self.list_execution_traces(limit=5),
            "harness_frontier": self.harness_frontier(limit=5),
            "replay_readiness": health["harness_variants"]["execution_traces"]["replay_readiness"],
            "judge_deadlock": health["judge_deadlock"],
            "milestone_health": self.milestone_status(),
        }

    def list_runtime_halt_events(
        self,
        *,
        limit: int = 20,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        return self._runtime_control.list_halt_events(limit=limit, status=status)

    def list_runtime_restart_history(
        self,
        *,
        limit: int = 20,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        return self._runtime_control.list_restart_history(limit=limit, status=status)

    def restart_runtime_after_halt(
        self,
        *,
        halt_id: str | None = None,
        judge_event_id: str | None = None,
        restart_reason: str = "operator_runtime_restart",
        notes: str | None = None,
        reference_time: str | None = None,
    ) -> dict[str, Any]:
        now = self._resolve_now(reference_time)
        runtime_status = self._runtime_control.status(reference_time=now)
        active_halt = runtime_status["active_halt"]
        if active_halt is None:
            raise KeyError(halt_id or "active runtime halt")

        immune = self._db.get_connection("immune")
        pending_quarantine_count = int(
            immune.execute(
                "SELECT COUNT(*) FROM quarantined_responses WHERE review_status = 'PENDING'"
            ).fetchone()[0]
        )
        judge_status_before = self._judge_lifecycle.status(reference_time=now)
        preflight: dict[str, Any] = {
            "runtime_state_before": runtime_status["lifecycle_state"],
            "halt_id": active_halt["halt_id"],
            "halt_source": active_halt["source"],
            "pending_quarantine_review_count": pending_quarantine_count,
            "judge_deadlock_mode_before": judge_status_before["mode"],
        }
        judge_restart_result: dict[str, Any] | None = None
        if active_halt["source"] == "JUDGE_DEADLOCK":
            judge_restart_result = self._judge_lifecycle.restart_after_deadlock(
                event_id=judge_event_id or active_halt["trigger_event_id"],
                reference_time=now,
            )
            preflight["judge_restart_result"] = judge_restart_result
            if judge_restart_result["status"] != "CLEARED":
                blocked = self._runtime_control.record_blocked_restart(
                    halt_id=halt_id or active_halt["halt_id"],
                    restart_reason=restart_reason,
                    preflight=preflight,
                    notes=notes,
                    reference_time=now,
                )
                result = {
                    "status": "BLOCKED",
                    "runtime_restart": blocked,
                    "judge_restart": judge_restart_result,
                    "runtime_status": self._runtime_control.status(reference_time=now),
                }
                self._log_trace(
                    task_id=blocked["restart_id"],
                    role="operator_runtime_restart",
                    action_name="restart_runtime_after_halt",
                    intent_goal=f"Attempt runtime restart for halt {blocked['halt_id']} while judge deadlock persisted.",
                    payload=result,
                    context_assembled=(
                        f"halt_id={blocked['halt_id']}; restart_reason={restart_reason}; "
                        f"judge_restart_status={judge_restart_result['status']}"
                    ),
                    judge_verdict="FAIL",
                    judge_reasoning="Runtime restart blocked because judge deadlock remained active.",
                    training_eligible=False,
                    retention_class="FAILURE_AUDIT",
                    outcome_score=0.0,
                )
                return result
        if active_halt["source"] == "SECURITY_CASCADE" and pending_quarantine_count:
            preflight["blocked_reason"] = "pending_quarantine_reviews"
            blocked = self._runtime_control.record_blocked_restart(
                halt_id=halt_id or active_halt["halt_id"],
                restart_reason=restart_reason,
                preflight=preflight,
                notes=notes,
                reference_time=now,
            )
            result = {
                "status": "BLOCKED",
                "runtime_restart": blocked,
                "judge_restart": judge_restart_result,
                "runtime_status": self._runtime_control.status(reference_time=now),
            }
            self._log_trace(
                task_id=blocked["restart_id"],
                role="operator_runtime_restart",
                action_name="restart_runtime_after_halt",
                intent_goal=f"Attempt runtime restart for halt {blocked['halt_id']} while quarantine review remained pending.",
                payload=result,
                context_assembled=(
                    f"halt_id={blocked['halt_id']}; restart_reason={restart_reason}; "
                    f"pending_quarantine_review_count={pending_quarantine_count}"
                ),
                judge_verdict="FAIL",
                judge_reasoning="Runtime restart blocked because quarantine review was still pending.",
                training_eligible=False,
                retention_class="FAILURE_AUDIT",
                outcome_score=0.0,
            )
            return result

        completed = self._runtime_control.complete_restart(
            halt_id=halt_id or active_halt["halt_id"],
            restart_reason=restart_reason,
            preflight=preflight,
            notes=notes,
            reference_time=now,
        )
        operator = self._db.get_connection("operator_digest")
        operator.execute(
            "INSERT INTO operator_heartbeat (entry_id, interaction_type, channel, timestamp) VALUES (?, ?, ?, ?)",
            (str(uuid.uuid4()), "command", "CLI", now),
        )
        operator.commit()
        result = {
            "status": "COMPLETED",
            "runtime_restart": completed,
            "judge_restart": judge_restart_result,
            "runtime_status": self._runtime_control.status(reference_time=now),
        }
        self._log_trace(
            task_id=completed["restart_id"],
            role="operator_runtime_restart",
            action_name="restart_runtime_after_halt",
            intent_goal=f"Restart runtime after halt {completed['halt_id']}.",
            payload=result,
            context_assembled=(
                f"halt_id={completed['halt_id']}; restart_reason={restart_reason}; "
                f"runtime_state_after={result['runtime_status']['lifecycle_state']}"
            ),
            judge_verdict="PASS",
            judge_reasoning="Runtime restart completed and halt was cleared.",
            training_eligible=True,
            retention_class="STANDARD",
            outcome_score=1.0,
        )
        return result

    def list_execution_traces(
        self,
        *,
        limit: int = 20,
        skill_name: str | None = None,
        training_eligible: bool | None = None,
        judge_verdict: str | None = None,
    ) -> list[dict[str, Any]]:
        return self._harness_variants.list_execution_traces(
            limit=limit,
            skill_name=skill_name,
            training_eligible=training_eligible,
            judge_verdict=judge_verdict,
        )

    def list_harness_variants(
        self,
        *,
        limit: int = 20,
        skill_name: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        return self._harness_variants.list_variants(limit=limit, skill_name=skill_name, status=status)

    def harness_frontier(self, *, limit: int = 20, skill_name: str | None = None) -> list[dict[str, Any]]:
        return self._harness_variants.frontier(limit=limit, skill_name=skill_name)

    def propose_harness_variant(
        self,
        *,
        skill_name: str,
        parent_version: str,
        diff: str,
        source: str = "operator",
        prompt_prelude: str = "",
        retrieval_strategy_diff: str = "",
        scoring_formula_diff: str = "",
        context_assembly_diff: str = "",
        touches_infrastructure: bool = False,
        reference_time: str | None = None,
    ) -> dict[str, Any]:
        now = self._resolve_now(reference_time)
        result = self._harness_variants.propose_variant(
            skill_name=skill_name,
            parent_version=parent_version,
            diff=diff,
            source=source,
            prompt_prelude=prompt_prelude,
            retrieval_strategy_diff=retrieval_strategy_diff,
            scoring_formula_diff=scoring_formula_diff,
            context_assembly_diff=context_assembly_diff,
            touches_infrastructure=touches_infrastructure,
            reference_time=now,
        )
        operator = self._db.get_connection("operator_digest")
        operator.execute(
            "INSERT INTO operator_heartbeat (entry_id, interaction_type, channel, timestamp) VALUES (?, ?, ?, ?)",
            (str(uuid.uuid4()), "command", "CLI", now),
        )
        operator.commit()
        return result

    def start_harness_variant_shadow_eval(
        self,
        *,
        variant_id: str,
        reference_time: str | None = None,
    ) -> dict[str, Any]:
        now = self._resolve_now(reference_time)
        result = self._harness_variants.start_shadow_eval(variant_id, reference_time=now)
        operator = self._db.get_connection("operator_digest")
        operator.execute(
            "INSERT INTO operator_heartbeat (entry_id, interaction_type, channel, timestamp) VALUES (?, ?, ?, ?)",
            (str(uuid.uuid4()), "command", "CLI", now),
        )
        operator.commit()
        return result

    def record_harness_variant_eval(
        self,
        *,
        variant_id: str,
        benchmark_name: str,
        baseline_outcome_scores: list[float],
        variant_outcome_scores: list[float],
        regression_rate: float,
        gate_0_pass: bool,
        known_bad_block_rate: float,
        gate_1_pass: bool,
        baseline_mean_score: float,
        variant_mean_score: float,
        quality_delta: float,
        gate_2_pass: bool,
        baseline_std: float,
        variant_std: float,
        gate_3_pass: bool,
        regressed_trace_count: int,
        improved_trace_count: int,
        net_trace_gain: int,
        traces_evaluated: int,
        compute_cost_cu: float,
        eval_duration_ms: int,
        reference_time: str | None = None,
    ) -> dict[str, Any]:
        now = self._resolve_now(reference_time)
        matching = self._harness_variants.get_variant(variant_id)
        replay_readiness = self._harness_variants.replay_readiness_summary()
        result = self._harness_variants.record_eval_result(
            variant_id,
            VariantEvalResult(
                variant_id=variant_id,
                skill_name=matching["skill_name"],
                benchmark_name=benchmark_name,
                baseline_outcome_scores=baseline_outcome_scores,
                variant_outcome_scores=variant_outcome_scores,
                regression_rate=regression_rate,
                gate_0_pass=gate_0_pass,
                known_bad_block_rate=known_bad_block_rate,
                gate_1_pass=gate_1_pass,
                baseline_mean_score=baseline_mean_score,
                variant_mean_score=variant_mean_score,
                quality_delta=quality_delta,
                gate_2_pass=gate_2_pass,
                baseline_std=baseline_std,
                variant_std=variant_std,
                gate_3_pass=gate_3_pass,
                regressed_trace_count=regressed_trace_count,
                improved_trace_count=improved_trace_count,
                net_trace_gain=net_trace_gain,
                traces_evaluated=traces_evaluated,
                compute_cost_cu=compute_cost_cu,
                eval_duration_ms=eval_duration_ms,
                replay_readiness_status=replay_readiness["status"],
                replay_readiness_blockers=list(replay_readiness["blockers"]),
                operator_acknowledged_below_threshold=False,
                created_at=now,
            ),
            reference_time=now,
        )
        operator = self._db.get_connection("operator_digest")
        operator.execute(
            "INSERT INTO operator_heartbeat (entry_id, interaction_type, channel, timestamp) VALUES (?, ?, ?, ?)",
            (str(uuid.uuid4()), "command", "CLI", now),
        )
        operator.commit()
        return result

    def evaluate_harness_variant_from_traces(
        self,
        *,
        variant_id: str,
        sample_size: int = 50,
        minimum_trace_count: int = 3,
        minimum_known_bad_traces: int = 1,
        known_bad_score_threshold: float = 0.35,
        per_trace_cost_cu: float = 0.05,
        operator_acknowledged_below_threshold: bool = False,
        reference_time: str | None = None,
    ) -> dict[str, Any]:
        now = self._resolve_now(reference_time)
        replay_readiness = self._harness_variants.replay_readiness_summary()
        alert_id: str | None = None
        if replay_readiness["available"] and replay_readiness["status"] != "READY_FOR_BROADER_REPLAY":
            blocker_text = ", ".join(replay_readiness["blockers"])
            if not operator_acknowledged_below_threshold:
                alert_id = self.alert(
                    "T2",
                    "REPLAY_READINESS_ACK_REQUIRED",
                    (
                        "Replay evaluation blocked below activation threshold for "
                        f"{variant_id}: {blocker_text}"
                    ),
                    reference_time=now,
                )
                raise ValueError(
                    "Replay readiness below activation threshold; set "
                    "`operator_acknowledged_below_threshold=True` to proceed: "
                    + blocker_text
                )
            alert_id = self.alert(
                "T2",
                "REPLAY_READINESS_OVERRIDE",
                (
                    "Operator acknowledged narrow replay below activation threshold for "
                    f"{variant_id}: {blocker_text}"
                ),
                reference_time=now,
            )
        result = self._harness_variants.evaluate_variant_from_traces(
            variant_id,
            sample_size=sample_size,
            minimum_trace_count=minimum_trace_count,
            minimum_known_bad_traces=minimum_known_bad_traces,
            known_bad_score_threshold=known_bad_score_threshold,
            per_trace_cost_cu=per_trace_cost_cu,
            allow_below_activation_threshold=operator_acknowledged_below_threshold,
            reference_time=now,
        )
        operator = self._db.get_connection("operator_digest")
        operator.execute(
            "INSERT INTO operator_heartbeat (entry_id, interaction_type, channel, timestamp) VALUES (?, ?, ?, ?)",
            (str(uuid.uuid4()), "command", "CLI", now),
        )
        operator.commit()
        result["replay_readiness"] = replay_readiness
        if alert_id is not None:
            result["operator_alert_id"] = alert_id
        return result

    def record_heartbeat(self, interaction_type: str, channel: str = "CLI") -> str:
        entry_id = str(uuid.uuid4())
        now = self._utc_now()
        conn = self._db.get_connection("operator_digest")
        conn.execute(
            "INSERT INTO operator_heartbeat (entry_id, interaction_type, channel, timestamp) VALUES (?, ?, ?, ?)",
            (entry_id, interaction_type, channel, now),
        )
        conn.commit()
        return entry_id

    def acknowledge_digest(self, digest_id: str, *, reference_time: str | None = None) -> dict:
        now = self._resolve_now(reference_time)
        conn = self._db.get_connection("operator_digest")
        row = conn.execute(
            "SELECT digest_id, acknowledged_at FROM digest_history WHERE digest_id = ?",
            (digest_id,),
        ).fetchone()
        if row is None:
            raise KeyError(digest_id)
        conn.execute(
            """
            UPDATE digest_history
            SET acknowledged_at = COALESCE(acknowledged_at, ?)
            WHERE digest_id = ?
            """,
            (now, digest_id),
        )
        conn.execute(
            "INSERT INTO operator_heartbeat (entry_id, interaction_type, channel, timestamp) VALUES (?, ?, ?, ?)",
            (str(uuid.uuid4()), "digest_ack", "CLI", now),
        )
        conn.commit()
        result = {
            "digest_id": digest_id,
            "acknowledged_at": now if row["acknowledged_at"] is None else row["acknowledged_at"],
        }
        self._log_trace(
            task_id=digest_id,
            role="operator_digest_acknowledgement",
            action_name="acknowledge_digest",
            intent_goal=f"Acknowledge digest {digest_id}.",
            payload=result,
            context_assembled=f"digest_id={digest_id}; first_ack={row['acknowledged_at'] is None}",
        )
        return result

    def generate_digest(self, digest_type: str = "daily", operator_state: str | None = None) -> dict:
        now = self._utc_now()
        self._financial_router.expire_stale_g3_requests(reference_time=now)
        effective_state = operator_state or self._operator_state(now)
        load_snapshot = self._record_operator_load_snapshot(now)
        effective_type = digest_type
        if digest_type == "daily" and load_snapshot["critical_only_recommended"]:
            effective_type = "critical_only"
        sections, urgent_pending = self._build_digest_sections(
            now=now,
            digest_type=effective_type,
            operator_state=effective_state,
            load_snapshot=load_snapshot,
        )
        ordered_names = list(sections.keys())
        if urgent_pending and "PENDING DECISIONS" in ordered_names:
            ordered_names.remove("PENDING DECISIONS")
            ordered_names.insert(0, "PENDING DECISIONS")
        lines = [f"{name}: {sections[name]}" for name in ordered_names]
        content = "\n".join(lines)
        existing = self._find_existing_digest(content, effective_type, effective_state, now)
        if existing is not None:
            return existing
        digest_id = str(uuid.uuid4())
        conn = self._db.get_connection("operator_digest")
        conn.execute(
            """
            INSERT INTO digest_history (
                digest_id, digest_type, content, sections_included, word_count,
                operator_state, delivered_at, acknowledged_at, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                digest_id,
                effective_type,
                content,
                json.dumps(ordered_names),
                len(content.split()),
                effective_state,
                None,
                None,
                now,
            ),
        )
        conn.commit()
        return asdict(
            DigestRecord(
                digest_id=digest_id,
                digest_type=effective_type,
                content=content,
                sections_included=ordered_names,
                word_count=len(content.split()),
                operator_state=effective_state,
                created_at=now,
            )
        )

    def record_operator_load_snapshot(self, *, reference_time: str | None = None) -> dict[str, Any]:
        now = self._resolve_now(reference_time)
        return self._record_operator_load_snapshot(now)

    def _build_digest_sections(
        self,
        *,
        now: str,
        digest_type: str,
        operator_state: str,
        load_snapshot: dict[str, Any],
    ) -> tuple[dict[str, str], bool]:
        financial = self._db.get_connection("financial_ledger")
        strategic = self._db.get_connection("strategic_memory")
        operator = self._db.get_connection("operator_digest")
        telemetry = self._db.get_connection("telemetry")
        now_dt = self._parse_ts(now)
        if digest_type == "catch_up":
            last_heartbeat = operator.execute(
                "SELECT timestamp FROM operator_heartbeat ORDER BY timestamp DESC LIMIT 1"
            ).fetchone()
            window_start_dt = self._parse_ts(last_heartbeat["timestamp"]) if last_heartbeat is not None else now_dt - datetime.timedelta(days=7)
        else:
            window_start_dt = now_dt - datetime.timedelta(hours=24)
        window_start = self._to_iso(window_start_dt)

        portfolio_rows = financial.execute(
            """
            SELECT
                p.project_id,
                p.name,
                p.status,
                p.kill_score_watch,
                p.portfolio_weight,
                COALESCE(pnl.net_to_date, 0.0) AS net_to_date,
                COALESCE((
                    SELECT kr.kill_score
                    FROM kill_recommendations kr
                    WHERE kr.project_id = p.project_id
                    ORDER BY kr.created_at DESC
                    LIMIT 1
                ), 0.0) AS kill_score
            FROM projects p
            LEFT JOIN project_pnl pnl ON pnl.project_id = p.project_id
            WHERE p.status IN ('ACTIVE', 'PAUSED', 'KILL_RECOMMENDED')
            ORDER BY
                CASE p.status WHEN 'KILL_RECOMMENDED' THEN 0 WHEN 'ACTIVE' THEN 1 ELSE 2 END,
                p.created_at ASC
            LIMIT 5
            """
        ).fetchall()
        opportunity_rows = strategic.execute(
            "SELECT status, COUNT(*) AS count FROM opportunity_records WHERE status != 'CLOSED' GROUP BY status ORDER BY status"
        ).fetchall()
        new_opportunities = strategic.execute(
            "SELECT COUNT(*) FROM opportunity_records WHERE created_at >= ?",
            (window_start,),
        ).fetchone()[0]
        approaching_gate_count = operator.execute(
            "SELECT COUNT(*) FROM gate_log WHERE status = 'PENDING' AND expires_at <= ?",
            (self._to_iso(now_dt + datetime.timedelta(hours=24)),),
        ).fetchone()[0]
        recent_briefs = strategic.execute(
            """
            SELECT title, actionability, urgency
            FROM intelligence_briefs
            WHERE created_at >= ?
            ORDER BY
                CASE actionability
                    WHEN 'ACTION_REQUIRED' THEN 0
                    WHEN 'HARVEST_NEEDED' THEN 1
                    WHEN 'ACTION_RECOMMENDED' THEN 2
                    WHEN 'WATCH' THEN 3
                    ELSE 4
                END,
                created_at DESC
            LIMIT 3
            """,
            (window_start,),
        ).fetchall()
        action_required_count = strategic.execute(
            """
            SELECT COUNT(*)
            FROM intelligence_briefs
            WHERE created_at >= ? AND actionability IN ('ACTION_REQUIRED', 'HARVEST_NEEDED')
            """,
            (window_start,),
        ).fetchone()[0]
        pending_rows = operator.execute(
            """
            SELECT item_type, label, priority, expires_at
            FROM (
                SELECT gate_type AS label, gate_type AS priority, expires_at, 'gate' AS item_type
                FROM gate_log
                WHERE status = 'PENDING'
                UNION ALL
                SELECT target_interface AS label, priority, expires_at, 'harvest' AS item_type
                FROM harvest_requests
                WHERE status = 'PENDING'
            )
            ORDER BY expires_at ASC
            LIMIT 5
            """
        ).fetchall()
        spend_24h = financial.execute(
            "SELECT COALESCE(SUM(amount_usd), 0.0) FROM cost_records WHERE created_at >= ?",
            (window_start,),
        ).fetchone()[0]
        revenue_24h = financial.execute(
            "SELECT COALESCE(SUM(amount_usd), 0.0) FROM revenue_records WHERE created_at >= ?",
            (window_start,),
        ).fetchone()[0]
        treasury = financial.execute(
            "SELECT balance_after FROM treasury ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        operator_hours_30d = self._trailing_operator_hours()
        net_30d = financial.execute(
            """
            SELECT
                COALESCE((SELECT SUM(amount_usd) FROM revenue_records WHERE created_at >= ?), 0.0)
                - COALESCE((SELECT SUM(amount_usd) FROM cost_records WHERE created_at >= ?), 0.0)
            """,
            (self._to_iso(now_dt - datetime.timedelta(days=30)), self._to_iso(now_dt - datetime.timedelta(days=30))),
        ).fetchone()[0]
        olr = None if operator_hours_30d <= 0 else net_30d / operator_hours_30d

        health = self._system_health_snapshot(now, load_snapshot)
        sections = {
            "PORTFOLIO HEALTH": self._limit_words(self._portfolio_section(portfolio_rows), SECTION_WORD_LIMITS["PORTFOLIO HEALTH"]),
            "PIPELINE STATUS": self._limit_words(
                self._pipeline_section(opportunity_rows, new_opportunities, approaching_gate_count),
                SECTION_WORD_LIMITS["PIPELINE STATUS"],
            ),
            "INTELLIGENCE HIGHLIGHTS": self._limit_words(
                self._intelligence_section(recent_briefs, action_required_count),
                SECTION_WORD_LIMITS["INTELLIGENCE HIGHLIGHTS"],
            ),
            "SYSTEM HEALTH": self._limit_words(
                self._system_health_section(health, operator_state),
                SECTION_WORD_LIMITS["SYSTEM HEALTH"],
            ),
            "PENDING DECISIONS": self._limit_words(
                self._pending_section(pending_rows, now_dt),
                SECTION_WORD_LIMITS["PENDING DECISIONS"],
            ),
            "FINANCIAL SUMMARY": self._limit_words(
                self._financial_section(
                    spend_24h=spend_24h,
                    revenue_24h=revenue_24h,
                    treasury_balance=None if treasury is None else treasury["balance_after"],
                    olr=olr,
                ),
                SECTION_WORD_LIMITS["FINANCIAL SUMMARY"],
            ),
        }
        if digest_type == "critical_only":
            selected_names = list(CRITICAL_SECTION_ORDER)
            if (
                health["compound_breakers"]["recent"]
                or health["quarantined_responses"]["pending_review_count"]
                or health["g3_requests"]["pending_count"]
                or health["g3_requests"]["approved_24h"]
                or health["g3_requests"]["denied_24h"]
                or health["g3_requests"]["expired_24h"]
                or health["disputed_costs"]["count"]
                or health["judge_fallback"]["count"]
                or health["judge_deadlock"]["mode"] in {"FALLBACK", "HALTED"}
                or health["judge_deadlock"]["review_queue"]["pending"]
                or health["judge_deadlock"]["review_queue"]["blocked"]
                or health["council_health"]["tier2_24h"]
                or health["council_health"]["degraded_24h"]
                or health["council_health"]["pending_tier2_g3"]
                or health["council_health"]["backlog_alerts_24h"]
                or health["harness_variants"]["variants"]["active_count"]
                or health["harness_variants"]["variants"]["rejected_24h"]
            ) and "SYSTEM HEALTH" not in selected_names:
                selected_names.insert(1, "SYSTEM HEALTH")
            sections = {name: sections[name] for name in selected_names}
        urgent_pending = any(
            self._time_remaining_hours(now_dt, row["expires_at"]) is not None
            and self._time_remaining_hours(now_dt, row["expires_at"]) <= 6.0
            for row in pending_rows
            if row["item_type"] == "gate"
        )
        return sections, urgent_pending

    def _portfolio_section(self, rows: list[Any]) -> str:
        if not rows:
            return "No active or paused projects."
        parts: list[str] = []
        concentration_flag = False
        for row in rows:
            if row["status"] == "KILL_RECOMMENDED" or row["kill_score"] >= 0.7:
                signal = "RED"
            elif row["kill_score_watch"] or row["kill_score"] >= 0.4:
                signal = "YELLOW"
            else:
                signal = "GREEN"
            if row["portfolio_weight"] >= 0.5:
                concentration_flag = True
            parts.append(f"{signal} {row['name']} {row['status']} net=${row['net_to_date']:.2f}")
        if concentration_flag:
            parts.append("concentration warning")
        return " | ".join(parts)

    @staticmethod
    def _pipeline_section(opportunity_rows: list[Any], new_opportunities: int, approaching_gate_count: int) -> str:
        counts = "none open"
        if opportunity_rows:
            counts = ", ".join(f"{row['status']}={row['count']}" for row in opportunity_rows)
        return f"{counts}. new_window={new_opportunities}. gates<24h={approaching_gate_count}."

    @staticmethod
    def _intelligence_section(recent_briefs: list[Any], action_required_count: int) -> str:
        if not recent_briefs:
            return "No new briefs in scope."
        items = [f"{row['title']} ({row['actionability']})" for row in recent_briefs]
        if action_required_count > len(recent_briefs):
            items.append(f"+{action_required_count - len(recent_briefs)} more actionable item(s)")
        return " | ".join(items)

    @staticmethod
    def _financial_section(
        *,
        spend_24h: float,
        revenue_24h: float,
        treasury_balance: float | None,
        olr: float | None,
    ) -> str:
        net_24h = revenue_24h - spend_24h
        treasury_text = "n/a" if treasury_balance is None else f"${treasury_balance:.2f}"
        olr_text = "n/a" if olr is None else f"${olr:.2f}/h"
        return f"24h spend=${spend_24h:.2f} revenue=${revenue_24h:.2f} net=${net_24h:.2f}. treasury={treasury_text}. OLR={olr_text}."

    def _pending_section(self, pending_rows: list[Any], now_dt: datetime.datetime) -> str:
        if not pending_rows:
            return "No pending gates or harvests."
        items: list[str] = []
        for row in pending_rows:
            remaining = self._format_time_remaining(now_dt, row["expires_at"])
            label = row["label"] if row["item_type"] == "gate" else f"harvest {row['priority']}"
            items.append(f"{label} {remaining}")
        return " | ".join(items)

    def _system_health_section(self, health: dict[str, Any], operator_state: str) -> str:
        issues: list[str] = []
        compound_events = health["compound_breakers"]["recent"]
        if compound_events:
            primary = compound_events[0]
            summary = "+".join(primary["breaker_names"]) + "->" + primary["winning_action"]
            if len(compound_events) > 1:
                summary += f" (+{len(compound_events) - 1})"
            issues.append("compound=" + summary)
        if health["circuit_breakers"]["critical"]:
            issues.append("critical=" + ",".join(health["circuit_breakers"]["critical"]))
        if health["circuit_breakers"]["degraded"]:
            issues.append("degraded=" + ",".join(health["circuit_breakers"]["degraded"]))
        if health["circuit_breakers"]["logged_active"]:
            issues.append("breaker_log=" + ",".join(health["circuit_breakers"]["logged_active"]))
        if health["quarantined_responses"]["pending_review_count"]:
            issues.append(f"quarantine pending={health['quarantined_responses']['pending_review_count']}")
        if (
            health["g3_requests"]["pending_count"]
            or health["g3_requests"]["approved_24h"]
            or health["g3_requests"]["denied_24h"]
            or health["g3_requests"]["expired_24h"]
        ):
            issues.append(
                "g3="
                + f"pending:{health['g3_requests']['pending_count']}"
                + f"/approved24h:{health['g3_requests']['approved_24h']}"
                + f"/denied24h:{health['g3_requests']['denied_24h']}"
                + f"/expired24h:{health['g3_requests']['expired_24h']}"
            )
        if health["disputed_costs"]["count"]:
            issues.append(
                "disputed_spend="
                + f"${health['disputed_costs']['amount_usd']:.2f}/{health['disputed_costs']['count']}"
            )
        if health["judge_fallback"]["count"]:
            issues.append(
                "judge_fallback recent="
                + f"{health['judge_fallback']['count']} blocked={health['judge_fallback']['blocked_count']}"
            )
        if health["judge_deadlock"]["mode"] == "FALLBACK":
            expires_at = health["judge_deadlock"]["active_event"]["expires_at"]
            issues.append(f"judge_deadlock fallback until={expires_at}")
        if health["judge_deadlock"]["mode"] == "HALTED":
            issues.append("judge_deadlock HALTED operator_restart_required")
        if health["runtime_control"]["lifecycle_state"] == "HALTED":
            active_halt = health["runtime_control"]["active_halt"]
            if active_halt is None:
                issues.append("runtime HALTED operator_restart_required")
            else:
                issues.append(f"runtime HALTED source={active_halt['source']}")
        if health["runtime_control"]["blocked_restart_attempts"]:
            issues.append(f"runtime_restart blocked={health['runtime_control']['blocked_restart_attempts']}")
        if health["council_health"]["tier2_24h"] or health["council_health"]["degraded_24h"]:
            issues.append(
                "council="
                + f"tier2_24h:{health['council_health']['tier2_24h']}"
                + f"/degraded24h:{health['council_health']['degraded_24h']}"
            )
        if health["council_health"]["pending_tier2_g3"]:
            issues.append(f"council_g3 pending={health['council_health']['pending_tier2_g3']}")
        if health["council_health"]["backlog_alerts_24h"]:
            issues.append(f"council_backlog alerts24h={health['council_health']['backlog_alerts_24h']}")
        if health["judge_deadlock"]["review_queue"]["pending"]:
            issues.append(f"judge_review pending={health['judge_deadlock']['review_queue']['pending']}")
        if health["judge_deadlock"]["review_queue"]["blocked"]:
            issues.append(f"judge_review blocked={health['judge_deadlock']['review_queue']['blocked']}")
        if health["harness_variants"]["variants"]["active_count"]:
            issues.append(f"variants active={health['harness_variants']['variants']['active_count']}")
        if health["harness_variants"]["variants"]["rejected_24h"]:
            issues.append(f"variants rejected24h={health['harness_variants']['variants']['rejected_24h']}")
        if health["harness_variants"]["execution_traces"]["total_count"]:
            issues.append(
                "traces="
                + f"{health['harness_variants']['execution_traces']['total_count']}"
                + f"/eligible:{health['harness_variants']['execution_traces']['training_eligible_count']}"
            )
        replay_readiness = health["harness_variants"]["execution_traces"]["replay_readiness"]
        if replay_readiness["available"] and replay_readiness["status"] != "READY_FOR_BROADER_REPLAY":
            issues.append(
                "replay_readiness="
                + f"{replay_readiness['eligible_source_traces']}/{replay_readiness['minimum_eligible_traces']}"
                + f" bad:{replay_readiness['known_bad_source_traces']}/{replay_readiness['minimum_known_bad_traces']}"
                + f" skills:{replay_readiness['distinct_skill_count']}/{replay_readiness['minimum_distinct_skills']}"
            )
        if health["unacknowledged_t3_alerts"]:
            issues.append(f"T3 pending ack={health['unacknowledged_t3_alerts']}")
        if health["research_health"]["stale_tasks"] or health["research_health"]["failed_tasks"]:
            issues.append(
                f"research stale={health['research_health']['stale_tasks']} failed={health['research_health']['failed_tasks']}"
            )
        if health["operator_load"]["critical_only_recommended"]:
            issues.append(f"load={health['operator_load']['estimated_hours']:.1f}h/w")
        if not issues:
            return f"All green. state={operator_state}."
        return f"state={operator_state}. " + " ; ".join(issues)

    def _system_health_snapshot(self, now: str, load_snapshot: dict[str, Any]) -> dict[str, Any]:
        operator = self._db.get_connection("operator_digest")
        telemetry = self._db.get_connection("telemetry")
        strategic = self._db.get_connection("strategic_memory")
        immune = self._db.get_connection("immune")
        financial = self._db.get_connection("financial_ledger")
        g3_requests = self._financial_router.g3_request_summary(reference_time=now, recent_limit=3)
        degraded_rows = telemetry.execute(
            """
            SELECT step_type, skill, reliability_7d
            FROM reliability_by_step
            WHERE reliability_7d IS NOT NULL AND reliability_7d < 0.95
            ORDER BY reliability_7d ASC, skill ASC
            """
        ).fetchall()
        critical = [
            f"{row['step_type']}/{row['skill']}"
            for row in degraded_rows
            if row["reliability_7d"] < 0.90
        ]
        degraded = [
            f"{row['step_type']}/{row['skill']}"
            for row in degraded_rows
            if row["reliability_7d"] >= 0.90
        ]
        unacknowledged_t3 = operator.execute(
            "SELECT COUNT(*) FROM alert_log WHERE tier = 'T3' AND acknowledged = 0"
        ).fetchone()[0]
        logged_active_rows = immune.execute(
            """
            WITH latest_state AS (
                SELECT breaker_name, state, timestamp,
                       ROW_NUMBER() OVER (PARTITION BY breaker_name ORDER BY timestamp DESC, event_id DESC) AS rn
                FROM circuit_breaker_log
            )
            SELECT breaker_name
            FROM latest_state
            WHERE rn = 1 AND state != 'RESET'
            ORDER BY breaker_name ASC
            """
        ).fetchall()
        compound_rows = immune.execute(
            """
            SELECT breaker_names, winning_action, created_at
            FROM compound_breaker_events
            WHERE resolved_at IS NULL
            ORDER BY created_at DESC
            LIMIT 3
            """
        ).fetchall()
        quarantined_rows = immune.execute(
            """
            SELECT quarantine_id, correlation_id, project_id, task_id, review_status, quarantined_at
            FROM quarantined_responses
            WHERE review_status = 'PENDING'
            ORDER BY quarantined_at DESC, quarantine_id DESC
            LIMIT 3
            """
        ).fetchall()
        quarantine_summary = immune.execute(
            """
            SELECT COUNT(*) AS pending_count
            FROM quarantined_responses
            WHERE review_status = 'PENDING'
            """
        ).fetchone()
        disputed_rows = financial.execute(
            """
            SELECT correlation_id, project_id, amount_usd, created_at
            FROM cost_records
            WHERE cost_status = 'DISPUTED'
            ORDER BY created_at DESC, record_id DESC
            LIMIT 3
            """
        ).fetchall()
        disputed_summary = financial.execute(
            """
            SELECT COUNT(*) AS disputed_count, COALESCE(SUM(amount_usd), 0.0) AS disputed_amount
            FROM cost_records
            WHERE cost_status = 'DISPUTED'
            """
        ).fetchone()
        fallback_rows = immune.execute(
            """
            SELECT verdict_id, session_id, skill_name, result, match_pattern, timestamp
            FROM immune_verdicts
            WHERE verdict_type = 'judge_output' AND judge_mode = 'FALLBACK'
            ORDER BY timestamp DESC, verdict_id DESC
            LIMIT 3
            """
        ).fetchall()
        fallback_summary = immune.execute(
            """
            SELECT
                COUNT(*) AS fallback_count,
                SUM(CASE WHEN result = 'BLOCK' THEN 1 ELSE 0 END) AS blocked_count,
                MAX(timestamp) AS last_seen_at
            FROM immune_verdicts
            WHERE verdict_type = 'judge_output' AND judge_mode = 'FALLBACK'
            """
        ).fetchone()
        judge_deadlock = self._judge_lifecycle.status(reference_time=now)
        runtime_status = self._runtime_control.status(reference_time=now)
        blocked_restart_attempts = len(
            self._runtime_control.list_restart_history(limit=5, status="BLOCKED")
        )
        council_since = (
            datetime.datetime.fromisoformat(now.replace("Z", "+00:00")) - datetime.timedelta(hours=24)
        ).replace(microsecond=0).isoformat()
        council_summary = strategic.execute(
            """
            SELECT
                SUM(CASE WHEN tier_used = 2 AND created_at >= ? THEN 1 ELSE 0 END) AS tier2_24h,
                SUM(CASE WHEN degraded = 1 AND created_at >= ? THEN 1 ELSE 0 END) AS degraded_24h,
                SUM(CASE WHEN confidence < 0.60 AND created_at >= ? THEN 1 ELSE 0 END) AS low_confidence_24h
            FROM council_verdicts
            """,
            (council_since, council_since, council_since),
        ).fetchone()
        pending_tier2_g3 = operator.execute(
            """
            SELECT COUNT(*)
            FROM gate_log
            WHERE gate_type = 'G3' AND status = 'PENDING' AND trigger_description LIKE 'council_tier2:%'
            """
        ).fetchone()[0]
        council_backlog_alerts = operator.execute(
            """
            SELECT COUNT(*)
            FROM alert_log
            WHERE alert_type = 'COUNCIL_BACKLOG' AND created_at >= ?
            """,
            (council_since,),
        ).fetchone()[0]
        harness_variant_summary = {
            "execution_traces": self._harness_variants.execution_trace_summary(),
            "variants": self._harness_variants.summary(),
        }
        stale_tasks = strategic.execute(
            "SELECT COUNT(*) FROM research_tasks WHERE status = 'STALE'"
        ).fetchone()[0]
        failed_tasks = strategic.execute(
            "SELECT COUNT(*) FROM research_tasks WHERE status = 'FAILED'"
        ).fetchone()[0]
        return {
            "circuit_breakers": {
                "critical": critical,
                "degraded": degraded,
                "logged_active": [row["breaker_name"] for row in logged_active_rows],
            },
            "compound_breakers": {
                "recent": [
                    {
                        "breaker_names": json.loads(row["breaker_names"]),
                        "winning_action": row["winning_action"],
                        "created_at": row["created_at"],
                    }
                    for row in compound_rows
                ],
            },
            "quarantined_responses": {
                "pending_review_count": int(quarantine_summary["pending_count"]),
                "recent": [dict(row) for row in quarantined_rows],
            },
            "g3_requests": g3_requests,
            "disputed_costs": {
                "count": int(disputed_summary["disputed_count"]),
                "amount_usd": float(disputed_summary["disputed_amount"]),
                "recent": [dict(row) for row in disputed_rows],
            },
            "judge_fallback": {
                "count": int(fallback_summary["fallback_count"] or 0),
                "blocked_count": int(fallback_summary["blocked_count"] or 0),
                "last_seen_at": fallback_summary["last_seen_at"],
                "recent": [dict(row) for row in fallback_rows],
            },
            "judge_deadlock": judge_deadlock,
            "runtime_control": {
                **runtime_status,
                "blocked_restart_attempts": blocked_restart_attempts,
            },
            "council_health": {
                "tier2_24h": int(council_summary["tier2_24h"] or 0),
                "degraded_24h": int(council_summary["degraded_24h"] or 0),
                "low_confidence_24h": int(council_summary["low_confidence_24h"] or 0),
                "pending_tier2_g3": int(pending_tier2_g3 or 0),
                "backlog_alerts_24h": int(council_backlog_alerts or 0),
            },
            "harness_variants": harness_variant_summary,
            "unacknowledged_t3_alerts": unacknowledged_t3,
            "research_health": {
                "stale_tasks": stale_tasks,
                "failed_tasks": failed_tasks,
            },
            "operator_load": load_snapshot,
            "timestamp": now,
        }

    def _log_trace(
        self,
        *,
        task_id: str,
        role: str,
        action_name: str,
        intent_goal: str,
        payload: Any,
        context_assembled: str,
        judge_verdict: str = "PASS",
        judge_reasoning: str | None = None,
        training_eligible: bool | None = None,
        retention_class: str | None = None,
        outcome_score: float | None = None,
    ) -> None:
        if not self._harness_variants.available:
            return
        verdict = judge_verdict.upper()
        eligible = training_eligible if training_eligible is not None else verdict == "PASS"
        self._harness_variants.log_skill_action_trace(
            task_id=task_id,
            role=role,
            skill_name="operator_interface",
            action_name=action_name,
            intent_goal=intent_goal,
            action_payload=payload,
            context_assembled=context_assembled,
            retrieval_queries=None,
            judge_verdict=verdict,
            judge_reasoning=judge_reasoning,
            training_eligible=eligible,
            retention_class=retention_class or ("STANDARD" if verdict == "PASS" else "FAILURE_AUDIT"),
            outcome_score=outcome_score if outcome_score is not None else (1.0 if verdict == "PASS" else 0.0),
        )

    def _record_operator_load_snapshot(self, now: str) -> dict[str, Any]:
        conn = self._db.get_connection("operator_digest")
        now_dt = self._parse_ts(now)
        week_start = (now_dt - datetime.timedelta(days=now_dt.weekday())).date().isoformat()
        week_start_date = datetime.date.fromisoformat(week_start)
        week_start_ts = self._to_iso(datetime.datetime.combine(week_start_date, datetime.time.min, tzinfo=datetime.timezone.utc))
        gates_by_type = {
            row["gate_type"]: row["count"]
            for row in conn.execute(
                """
                SELECT gate_type, COUNT(*) AS count
                FROM gate_log
                WHERE created_at >= ?
                GROUP BY gate_type
                """,
                (week_start_ts,),
            ).fetchall()
        }
        gates_total = sum(gates_by_type.values())
        harvests_created = conn.execute(
            "SELECT COUNT(*) FROM harvest_requests WHERE created_at >= ?",
            (week_start_ts,),
        ).fetchone()[0]
        harvests_completed = conn.execute(
            "SELECT COUNT(*) FROM harvest_requests WHERE status IN ('DELIVERED', 'DELIVERED_PARTIAL') AND delivered_at >= ?",
            (week_start_ts,),
        ).fetchone()[0]
        harvests_expired = conn.execute(
            "SELECT COUNT(*) FROM harvest_requests WHERE status = 'EXPIRED' AND expires_at >= ?",
            (week_start_ts,),
        ).fetchone()[0]
        pending_decisions = conn.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM gate_log WHERE status = 'PENDING')
                + (SELECT COUNT(*) FROM harvest_requests WHERE status = 'PENDING')
            """
        ).fetchone()[0]
        estimated_hours = (
            gates_total * 0.5
            + harvests_created * (10.0 / 60.0)
            + (7 * 20.0 / 60.0)
            + pending_decisions * (15.0 / 60.0)
        )
        prior_overload = conn.execute(
            """
            SELECT overload_triggered
            FROM operator_load_tracking
            WHERE week_start < ?
            ORDER BY week_start DESC, created_at DESC, entry_id DESC
            LIMIT 1
            """,
            (week_start,),
        ).fetchone()
        overload_triggered = 1 if estimated_hours > 15.0 else 0
        conn.execute(
            """
            INSERT INTO operator_load_tracking (
                entry_id, week_start, gates_surfaced, harvests_created,
                harvests_completed, harvests_expired, estimated_hours,
                overload_triggered, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid.uuid4()),
                week_start,
                json.dumps({gate: gates_by_type.get(gate, 0) for gate in ("G1", "G2", "G3", "G4")}),
                harvests_created,
                harvests_completed,
                harvests_expired,
                estimated_hours,
                overload_triggered,
                now,
            ),
        )
        conn.commit()
        return {
            "week_start": week_start,
            "gates_surfaced": {gate: gates_by_type.get(gate, 0) for gate in ("G1", "G2", "G3", "G4")},
            "harvests_created": harvests_created,
            "harvests_completed": harvests_completed,
            "harvests_expired": harvests_expired,
            "estimated_hours": estimated_hours,
            "critical_only_recommended": estimated_hours > 15.0,
            "sustained_overload": estimated_hours > 15.0 and bool(prior_overload and prior_overload["overload_triggered"]),
        }

    @staticmethod
    def _quarantine_row_to_dict(row: Any) -> dict[str, Any]:
        return {
            "quarantine_id": row["quarantine_id"],
            "correlation_id": row["correlation_id"],
            "session_id": row["session_id"],
            "project_id": row["project_id"],
            "task_id": row["task_id"],
            "route_decision_id": row["route_decision_id"],
            "cost_record_id": row["cost_record_id"],
            "reservation_id": row["reservation_id"],
            "source_breaker": row["source_breaker"],
            "provider": row["provider"],
            "model_used": row["model_used"],
            "payload_format": row["payload_format"],
            "payload_text": row["payload_text"],
            "received_at": row["received_at"],
            "quarantined_at": row["quarantined_at"],
            "review_status": row["review_status"],
            "operator_decision": row["operator_decision"],
            "review_notes": row["review_notes"],
            "review_digest_id": row["review_digest_id"],
            "reviewed_at": row["reviewed_at"],
        }

    def _trailing_operator_hours(self) -> float:
        conn = self._db.get_connection("operator_digest")
        now = self._parse_ts(self._utc_now())
        threshold = (now - datetime.timedelta(days=30)).date().isoformat()
        rows = conn.execute(
            """
            SELECT week_start, estimated_hours, created_at, entry_id
            FROM operator_load_tracking
            WHERE week_start >= ?
            ORDER BY week_start DESC, created_at DESC, entry_id DESC
            """,
            (threshold,),
        ).fetchall()
        latest_by_week: dict[str, float] = {}
        for row in rows:
            latest_by_week.setdefault(row["week_start"], float(row["estimated_hours"]))
        return sum(latest_by_week.values())

    def _find_existing_digest(
        self,
        content: str,
        digest_type: str,
        operator_state: str,
        now: str,
    ) -> dict[str, Any] | None:
        day_start = self._to_iso(self._parse_ts(now).replace(hour=0, minute=0, second=0, microsecond=0))
        conn = self._db.get_connection("operator_digest")
        row = conn.execute(
            """
            SELECT digest_id, digest_type, content, sections_included, word_count, operator_state, created_at
            FROM digest_history
            WHERE digest_type = ? AND operator_state = ? AND created_at >= ? AND content = ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (digest_type, operator_state, day_start, content),
        ).fetchone()
        if row is None:
            return None
        return asdict(
            DigestRecord(
                digest_id=row["digest_id"],
                digest_type=row["digest_type"],
                content=row["content"],
                sections_included=json.loads(row["sections_included"]),
                word_count=row["word_count"],
                operator_state=row["operator_state"],
                created_at=row["created_at"],
            )
        )

    @staticmethod
    def _should_suppress_duplicate(conn, alert_type: str, now: str) -> bool:
        cutoff = OperatorInterfaceSkill._to_iso(
            OperatorInterfaceSkill._parse_ts(now) - datetime.timedelta(minutes=15)
        )
        count = conn.execute(
            """
            SELECT COUNT(*)
            FROM alert_log
            WHERE alert_type = ? AND suppressed = 0 AND created_at >= ?
            """,
            (alert_type, cutoff),
        ).fetchone()[0]
        return count > 0

    @staticmethod
    def _delivered_t2_count(conn, now: str) -> int:
        cutoff = OperatorInterfaceSkill._to_iso(
            OperatorInterfaceSkill._parse_ts(now) - datetime.timedelta(hours=1)
        )
        return conn.execute(
            """
            SELECT COUNT(*)
            FROM alert_log
            WHERE tier = 'T2' AND suppressed = 0 AND created_at >= ?
            """,
            (cutoff,),
        ).fetchone()[0]

    def _operator_state(self, now: str) -> str:
        conn = self._db.get_connection("operator_digest")
        heartbeat = conn.execute(
            "SELECT timestamp FROM operator_heartbeat ORDER BY timestamp DESC LIMIT 1"
        ).fetchone()
        return self._heartbeat_state(heartbeat["timestamp"] if heartbeat is not None else None, now)

    @staticmethod
    def _heartbeat_state(last_timestamp: str | None, now: str) -> str:
        if last_timestamp is None:
            return "ABSENT"
        current = OperatorInterfaceSkill._parse_ts(now)
        seen = OperatorInterfaceSkill._parse_ts(last_timestamp)
        hours = (current - seen).total_seconds() / 3600
        if hours < 72:
            return "ACTIVE"
        if hours < 168:
            return "CONSERVATIVE"
        return "ABSENT"

    @staticmethod
    def _format_time_remaining(now: datetime.datetime, expires_at: str) -> str:
        hours = OperatorInterfaceSkill._time_remaining_hours(now, expires_at)
        if hours is None:
            return "unknown"
        if hours < 0:
            return "overdue"
        if hours < 1:
            minutes = max(1, int(round(hours * 60)))
            return f"{minutes}m"
        return f"{int(hours)}h"

    @staticmethod
    def _time_remaining_hours(now: datetime.datetime, expires_at: str) -> float | None:
        if not expires_at:
            return None
        expiry = OperatorInterfaceSkill._parse_ts(expires_at)
        return (expiry - now).total_seconds() / 3600

    @staticmethod
    def _limit_words(text: str, max_words: int) -> str:
        words = text.split()
        if len(words) <= max_words:
            return text
        return " ".join(words[: max_words - 1] + ["..."])

    @staticmethod
    def _parse_ts(value: str) -> datetime.datetime:
        dt = datetime.datetime.fromisoformat(value)
        return dt if dt.tzinfo is not None else dt.replace(tzinfo=datetime.timezone.utc)

    @staticmethod
    def _to_iso(value: datetime.datetime) -> str:
        return value.astimezone(datetime.timezone.utc).replace(microsecond=0).isoformat()

    def _resolve_now(self, reference_time: str | None) -> str:
        if reference_time is None:
            return self._utc_now()
        return self._to_iso(self._parse_ts(reference_time))

    @staticmethod
    def _utc_now() -> str:
        return datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()


_SKILL: Optional[OperatorInterfaceSkill] = None


def configure_skill(db_manager: DatabaseManager):
    global _SKILL
    _SKILL = OperatorInterfaceSkill(db_manager)


def operator_interface_entry(action: str, **kwargs):
    if _SKILL is None:
        raise RuntimeError("operator interface skill not configured")
    if action == "alert":
        return _SKILL.alert(
            kwargs["tier"],
            kwargs["alert_type"],
            kwargs["content"],
            channel_delivered=kwargs.get("channel_delivered", "CLI"),
            suppressed=kwargs.get("suppressed", False),
            reference_time=kwargs.get("reference_time"),
        )
    if action == "acknowledge_alert":
        return _SKILL.acknowledge_alert(
            kwargs["alert_id"],
            reference_time=kwargs.get("reference_time"),
        )
    if action == "list_alerts":
        return _SKILL.list_alerts(
            limit=kwargs.get("limit", 20),
            tier=kwargs.get("tier"),
            alert_type=kwargs.get("alert_type"),
            unacknowledged_only=kwargs.get("unacknowledged_only", False),
            include_suppressed=kwargs.get("include_suppressed", True),
        )
    if action == "record_heartbeat":
        return _SKILL.record_heartbeat(kwargs["interaction_type"], kwargs.get("channel", "CLI"))
    if action == "acknowledge_digest":
        return _SKILL.acknowledge_digest(
            kwargs["digest_id"],
            reference_time=kwargs.get("reference_time"),
        )
    if action == "list_quarantined_responses":
        return _SKILL.list_quarantined_responses(
            limit=kwargs.get("limit", 20),
            pending_review_only=kwargs.get("pending_review_only", False),
        )
    if action == "list_g3_approval_requests":
        return _SKILL.list_g3_approval_requests(
            limit=kwargs.get("limit", 20),
            status=kwargs.get("status"),
            reference_time=kwargs.get("reference_time"),
        )
    if action == "review_g3_approval_request":
        return _SKILL.review_g3_approval_request(
            kwargs["request_id"],
            kwargs["decision"],
            operator_notes=kwargs.get("operator_notes"),
            reference_time=kwargs.get("reference_time"),
        )
    if action == "dispatch_approved_paid_route":
        return _SKILL.dispatch_approved_paid_route(
            correlation_id=kwargs["correlation_id"],
            jwt_claims=kwargs["jwt_claims"],
            reference_time=kwargs.get("reference_time"),
        )
    if action == "finalize_paid_dispatch":
        return _SKILL.finalize_paid_dispatch(
            correlation_id=kwargs["correlation_id"],
            final_cost_usd=kwargs["final_cost_usd"],
            provider=kwargs.get("provider"),
            reference_time=kwargs.get("reference_time"),
        )
    if action == "review_quarantined_response":
        return _SKILL.review_quarantined_response(
            kwargs["quarantine_id"],
            kwargs["decision"],
            review_notes=kwargs.get("review_notes"),
            review_digest_id=kwargs.get("review_digest_id"),
            reference_time=kwargs.get("reference_time"),
        )
    if action == "list_judge_fallback_events":
        return _SKILL.list_judge_fallback_events(limit=kwargs.get("limit", 20))
    if action == "list_judge_fallback_review_queue":
        return _SKILL.list_judge_fallback_review_queue(
            limit=kwargs.get("limit", 20),
            review_status=kwargs.get("review_status"),
        )
    if action == "restart_judge_after_deadlock":
        return _SKILL.restart_judge_after_deadlock(
            event_id=kwargs.get("event_id"),
            reference_time=kwargs.get("reference_time"),
        )
    if action == "runtime_status":
        return _SKILL.runtime_status()
    if action == "milestone_status":
        return _SKILL.milestone_status()
    if action == "workspace_overview":
        return _SKILL.workspace_overview()
    if action == "list_runtime_halt_events":
        return _SKILL.list_runtime_halt_events(
            limit=kwargs.get("limit", 20),
            status=kwargs.get("status"),
        )
    if action == "list_runtime_restart_history":
        return _SKILL.list_runtime_restart_history(
            limit=kwargs.get("limit", 20),
            status=kwargs.get("status"),
        )
    if action == "restart_runtime_after_halt":
        return _SKILL.restart_runtime_after_halt(
            halt_id=kwargs.get("halt_id"),
            judge_event_id=kwargs.get("judge_event_id"),
            restart_reason=kwargs.get("restart_reason", "operator_runtime_restart"),
            notes=kwargs.get("notes"),
            reference_time=kwargs.get("reference_time"),
        )
    if action == "list_execution_traces":
        return _SKILL.list_execution_traces(
            limit=kwargs.get("limit", 20),
            skill_name=kwargs.get("skill_name"),
            training_eligible=kwargs.get("training_eligible"),
            judge_verdict=kwargs.get("judge_verdict"),
        )
    if action == "list_harness_variants":
        return _SKILL.list_harness_variants(
            limit=kwargs.get("limit", 20),
            skill_name=kwargs.get("skill_name"),
            status=kwargs.get("status"),
        )
    if action == "harness_frontier":
        return _SKILL.harness_frontier(
            limit=kwargs.get("limit", 20),
            skill_name=kwargs.get("skill_name"),
        )
    if action == "propose_harness_variant":
        return _SKILL.propose_harness_variant(
            skill_name=kwargs["skill_name"],
            parent_version=kwargs["parent_version"],
            diff=kwargs["diff"],
            source=kwargs.get("source", "operator"),
            prompt_prelude=kwargs.get("prompt_prelude", ""),
            retrieval_strategy_diff=kwargs.get("retrieval_strategy_diff", ""),
            scoring_formula_diff=kwargs.get("scoring_formula_diff", ""),
            context_assembly_diff=kwargs.get("context_assembly_diff", ""),
            touches_infrastructure=kwargs.get("touches_infrastructure", False),
            reference_time=kwargs.get("reference_time"),
        )
    if action == "start_harness_variant_shadow_eval":
        return _SKILL.start_harness_variant_shadow_eval(
            variant_id=kwargs["variant_id"],
            reference_time=kwargs.get("reference_time"),
        )
    if action == "record_harness_variant_eval":
        return _SKILL.record_harness_variant_eval(
            variant_id=kwargs["variant_id"],
            benchmark_name=kwargs["benchmark_name"],
            baseline_outcome_scores=kwargs["baseline_outcome_scores"],
            variant_outcome_scores=kwargs["variant_outcome_scores"],
            regression_rate=kwargs["regression_rate"],
            gate_0_pass=kwargs["gate_0_pass"],
            known_bad_block_rate=kwargs["known_bad_block_rate"],
            gate_1_pass=kwargs["gate_1_pass"],
            baseline_mean_score=kwargs["baseline_mean_score"],
            variant_mean_score=kwargs["variant_mean_score"],
            quality_delta=kwargs["quality_delta"],
            gate_2_pass=kwargs["gate_2_pass"],
            baseline_std=kwargs["baseline_std"],
            variant_std=kwargs["variant_std"],
            gate_3_pass=kwargs["gate_3_pass"],
            regressed_trace_count=kwargs["regressed_trace_count"],
            improved_trace_count=kwargs["improved_trace_count"],
            net_trace_gain=kwargs["net_trace_gain"],
            traces_evaluated=kwargs["traces_evaluated"],
            compute_cost_cu=kwargs["compute_cost_cu"],
            eval_duration_ms=kwargs["eval_duration_ms"],
            reference_time=kwargs.get("reference_time"),
        )
    if action == "evaluate_harness_variant_from_traces":
        return _SKILL.evaluate_harness_variant_from_traces(
            variant_id=kwargs["variant_id"],
            sample_size=kwargs.get("sample_size", 50),
            minimum_trace_count=kwargs.get("minimum_trace_count", 3),
            minimum_known_bad_traces=kwargs.get("minimum_known_bad_traces", 1),
            known_bad_score_threshold=kwargs.get("known_bad_score_threshold", 0.35),
            per_trace_cost_cu=kwargs.get("per_trace_cost_cu", 0.05),
            operator_acknowledged_below_threshold=kwargs.get("operator_acknowledged_below_threshold", False),
            reference_time=kwargs.get("reference_time"),
        )
    if action == "generate_digest":
        return _SKILL.generate_digest(
            digest_type=kwargs.get("digest_type", "daily"),
            operator_state=kwargs.get("operator_state"),
        )
    if action == "record_operator_load_snapshot":
        return _SKILL.record_operator_load_snapshot(reference_time=kwargs.get("reference_time"))
    raise ValueError(f"Unknown action: {action}")
