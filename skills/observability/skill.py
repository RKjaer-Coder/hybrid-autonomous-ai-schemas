from __future__ import annotations

import datetime
import json
from typing import Any, Optional

from harness_variants import HarnessVariantManager
from immune.judge_lifecycle import JudgeLifecycleManager
from immune.config import load_config
from runtime_control import RuntimeControlManager
from skills.append_buffer import AppendBuffer
from skills.config import IntegrationConfig
from skills.db_manager import DatabaseManager
from skills.financial_router.skill import FinancialRouterSkill
from skills.milestone_status import evaluate_milestone_status


class ObservabilitySkill:
    def __init__(self, db_manager: DatabaseManager, telemetry_buffer: Optional[AppendBuffer], immune_buffer: Optional[AppendBuffer]):
        self._db = db_manager
        self._telemetry_buffer = telemetry_buffer
        self._immune_buffer = immune_buffer
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

    def query_immune_verdicts(
        self,
        limit: int = 20,
        outcome: str | None = None,
        judge_mode: str | None = None,
    ) -> list[dict]:
        conn = self._db.get_connection("immune")
        where: list[str] = []
        params: list[object] = []
        if outcome:
            where.append("result = ?")
            params.append(outcome)
        if judge_mode:
            where.append("judge_mode = ?")
            params.append(judge_mode)
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        rows = conn.execute(
            f"SELECT * FROM immune_verdicts {where_sql} ORDER BY timestamp DESC LIMIT ?",
            (*params, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def query_telemetry(
        self,
        skill_name: str | None = None,
        limit: int = 50,
        chain_id: str | None = None,
        outcome: str | None = None,
    ) -> list[dict]:
        conn = self._db.get_connection("telemetry")
        where: list[str] = []
        params: list[object] = []
        if skill_name:
            where.append("skill = ?")
            params.append(skill_name)
        if chain_id:
            where.append("chain_id = ?")
            params.append(chain_id)
        if outcome:
            where.append("outcome = ?")
            params.append(outcome)
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        rows = conn.execute(
            f"SELECT * FROM step_outcomes {where_sql} ORDER BY timestamp DESC LIMIT ?",
            (*params, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def query_council_verdicts(
        self,
        limit: int = 20,
        decision_type: str | None = None,
        project_id: str | None = None,
    ) -> list[dict]:
        conn = self._db.get_connection("strategic_memory")
        where: list[str] = []
        params: list[object] = []
        if decision_type:
            where.append("decision_type = ?")
            params.append(decision_type)
        if project_id:
            where.append("project_id = ?")
            params.append(project_id)
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        rows = conn.execute(
            f"""
            SELECT
                verdict_id, tier_used, decision_type, recommendation, confidence,
                reasoning_summary, dissenting_views, project_id, outcome_record,
                da_quality_score, tie_break, degraded, confidence_cap, created_at
            FROM council_verdicts
            {where_sql}
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (*params, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def query_alert_history(
        self,
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
            f"SELECT * FROM alert_log {where_sql} ORDER BY created_at DESC LIMIT ?",
            (*params, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def query_circuit_breakers(
        self,
        limit: int = 20,
        breaker_name: str | None = None,
        state: str | None = None,
    ) -> list[dict]:
        conn = self._db.get_connection("immune")
        where: list[str] = []
        params: list[object] = []
        if breaker_name:
            where.append("breaker_name = ?")
            params.append(breaker_name)
        if state:
            where.append("state = ?")
            params.append(state)
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        rows = conn.execute(
            f"SELECT * FROM circuit_breaker_log {where_sql} ORDER BY timestamp DESC LIMIT ?",
            (*params, limit),
        ).fetchall()
        return [dict(row) for row in rows]

    def recent_compound_breakers(self, limit: int = 10, unresolved_only: bool = False) -> list[dict]:
        conn = self._db.get_connection("immune")
        where_sql = "WHERE resolved_at IS NULL" if unresolved_only else ""
        rows = conn.execute(
            f"SELECT * FROM compound_breaker_events {where_sql} ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [
            {
                "event_id": row["event_id"],
                "breaker_names": json.loads(row["breaker_names"]),
                "winner_tier": row["winner_tier"],
                "winning_action": row["winning_action"],
                "applied_actions": json.loads(row["applied_actions"]),
                "suppressed_actions": json.loads(row["suppressed_actions"]),
                "requires_human": bool(row["requires_human"]),
                "window_seconds": row["window_seconds"],
                "window_started_at": row["window_started_at"],
                "window_ended_at": row["window_ended_at"],
                "resolution_notes": row["resolution_notes"],
                "resolved_at": row["resolved_at"],
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def recent_quarantined_responses(self, limit: int = 10, pending_review_only: bool = False) -> list[dict]:
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
        return [dict(row) for row in rows]

    def recent_disputed_costs(self, limit: int = 10) -> list[dict]:
        financial = self._db.get_connection("financial_ledger")
        rows = financial.execute(
            """
            SELECT record_id, project_id, amount_usd, provider, task_id, correlation_id,
                   route_decision_id, cost_status, created_at
            FROM cost_records
            WHERE cost_status = 'DISPUTED'
            ORDER BY created_at DESC, record_id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]

    def recent_g3_approval_requests(self, limit: int = 10, status: str | None = None) -> list[dict]:
        return self._financial_router.list_g3_approval_requests(limit=limit, status=status)

    def recent_fallback_judge_verdicts(self, limit: int = 10) -> list[dict]:
        return self.query_immune_verdicts(limit=limit, judge_mode="FALLBACK")

    def recent_judge_fallback_events(self, limit: int = 10) -> list[dict]:
        return self._judge_lifecycle.list_events(limit=limit)

    def judge_fallback_review_queue(self, limit: int = 10, review_status: str | None = None) -> list[dict]:
        return self._judge_lifecycle.list_review_queue(limit=limit, review_status=review_status)

    def judge_deadlock_status(self) -> dict:
        return self._judge_lifecycle.status()

    def runtime_status(self) -> dict:
        return self._runtime_control.status()

    def runtime_halt_events(self, limit: int = 10, status: str | None = None) -> list[dict]:
        return self._runtime_control.list_halt_events(limit=limit, status=status)

    def runtime_restart_history(self, limit: int = 10, status: str | None = None) -> list[dict]:
        return self._runtime_control.list_restart_history(limit=limit, status=status)

    def recent_digests(self, limit: int = 5, digest_type: str | None = None) -> list[dict]:
        conn = self._db.get_connection("operator_digest")
        where_sql = ""
        params: list[object] = []
        if digest_type:
            where_sql = "WHERE digest_type = ?"
            params.append(digest_type)
        rows = conn.execute(
            f"SELECT * FROM digest_history {where_sql} ORDER BY created_at DESC LIMIT ?",
            (*params, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def execution_traces(
        self,
        limit: int = 20,
        skill_name: str | None = None,
        training_eligible: bool | None = None,
        judge_verdict: str | None = None,
    ) -> list[dict]:
        return self._harness_variants.list_execution_traces(
            limit=limit,
            skill_name=skill_name,
            training_eligible=training_eligible,
            judge_verdict=judge_verdict,
        )

    def harness_variants(
        self,
        limit: int = 20,
        skill_name: str | None = None,
        status: str | None = None,
    ) -> list[dict]:
        return self._harness_variants.list_variants(limit=limit, skill_name=skill_name, status=status)

    def harness_frontier(self, limit: int = 20, skill_name: str | None = None) -> list[dict]:
        return self._harness_variants.frontier(limit=limit, skill_name=skill_name)

    def harness_variant_summary(self) -> dict:
        return {
            "execution_traces": self._harness_variants.execution_trace_summary(),
            "variants": self._harness_variants.summary(),
        }

    def reliability_dashboard(self, limit: int = 20) -> dict:
        telemetry = self._db.get_connection("telemetry")
        reliability_rows = telemetry.execute(
            """
            SELECT step_type, skill, reliability_7d, reliability_30d
            FROM reliability_by_step
            WHERE reliability_7d IS NOT NULL
            ORDER BY reliability_7d ASC, skill ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        chain_rows = telemetry.execute(
            """
            SELECT chain_type, chain_reliability_7d, chain_reliability_30d
            FROM chain_reliability
            ORDER BY chain_type ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        steps = [dict(row) for row in reliability_rows]
        return {
            "steps": steps,
            "chains": [dict(row) for row in chain_rows],
            "critical_steps": [row for row in steps if row["reliability_7d"] is not None and row["reliability_7d"] < 0.90],
            "degraded_steps": [row for row in steps if row["reliability_7d"] is not None and 0.90 <= row["reliability_7d"] < 0.95],
        }

    def buffer_stats(self) -> dict:
        return {
            "telemetry": self._telemetry_buffer.stats if self._telemetry_buffer else None,
            "immune": self._immune_buffer.stats if self._immune_buffer else None,
        }

    def circuit_breaker_status(self) -> dict:
        telemetry = self._db.get_connection("telemetry")
        operator = self._db.get_connection("operator_digest")
        immune = self._db.get_connection("immune")
        reliability_rows = telemetry.execute(
            """
            SELECT step_type, skill, reliability_7d
            FROM reliability_by_step
            WHERE reliability_7d IS NOT NULL AND reliability_7d < 0.95
            ORDER BY reliability_7d ASC, skill ASC
            """
        ).fetchall()
        critical = [
            f"{row['step_type']}/{row['skill']}"
            for row in reliability_rows
            if row["reliability_7d"] < 0.90
        ]
        degraded = [
            f"{row['step_type']}/{row['skill']}"
            for row in reliability_rows
            if row["reliability_7d"] >= 0.90
        ]
        unacknowledged_t3 = operator.execute(
            "SELECT COUNT(*) FROM alert_log WHERE tier = 'T3' AND acknowledged = 0"
        ).fetchone()[0]
        logged_active = immune.execute(
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
        overload = self._operator_load_snapshot()["critical_only_recommended"]
        compound_breakers = self.recent_compound_breakers(limit=3, unresolved_only=True)
        return {
            "critical": critical,
            "degraded": degraded,
            "unacknowledged_t3_alerts": unacknowledged_t3,
            "operator_overload": overload,
            "logged_active": [row["breaker_name"] for row in logged_active],
            "recent_compound_events": compound_breakers,
        }

    def system_health(self) -> dict:
        operator = self._db.get_connection("operator_digest")
        strategic = self._db.get_connection("strategic_memory")
        immune = self._db.get_connection("immune")
        financial = self._db.get_connection("financial_ledger")
        g3_requests = self._financial_router.g3_request_summary(recent_limit=3)
        heartbeat = operator.execute(
            "SELECT timestamp FROM operator_heartbeat ORDER BY timestamp DESC LIMIT 1"
        ).fetchone()
        alert_counts = operator.execute(
            "SELECT tier, COUNT(*) AS count FROM alert_log GROUP BY tier ORDER BY tier"
        ).fetchall()
        pending_gates = operator.execute(
            "SELECT COUNT(*) FROM gate_log WHERE status = 'PENDING'"
        ).fetchone()[0]
        pending_harvests = operator.execute(
            "SELECT COUNT(*) FROM harvest_requests WHERE status = 'PENDING'"
        ).fetchone()[0]
        unacknowledged_t3 = operator.execute(
            "SELECT COUNT(*) FROM alert_log WHERE tier = 'T3' AND acknowledged = 0"
        ).fetchone()[0]
        quarantine_summary = immune.execute(
            """
            SELECT COUNT(*) AS pending_count
            FROM quarantined_responses
            WHERE review_status = 'PENDING'
            """
        ).fetchone()
        disputed_summary = financial.execute(
            """
            SELECT COUNT(*) AS disputed_count, COALESCE(SUM(amount_usd), 0.0) AS disputed_amount
            FROM cost_records
            WHERE cost_status = 'DISPUTED'
            """
        ).fetchone()
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
        judge_deadlock = self._judge_lifecycle.status()
        runtime_status = self._runtime_control.status()
        council_since = (
            datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=24)
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
        council_pending_g3 = operator.execute(
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
        blocked_restart_attempts = len(
            self._runtime_control.list_restart_history(limit=5, status="BLOCKED")
        )
        heartbeat_state = self._heartbeat_state(heartbeat["timestamp"]) if heartbeat is not None else "ABSENT"
        operator_load = self._operator_load_snapshot()
        if heartbeat_state != "ACTIVE":
            recommended_digest_type = "catch_up"
        elif (
            runtime_status["lifecycle_state"] == "HALTED"
            or judge_deadlock["mode"] in {"FALLBACK", "HALTED"}
            or int(fallback_summary["fallback_count"] or 0)
        ):
            recommended_digest_type = "critical_only"
        elif operator_load["critical_only_recommended"]:
            recommended_digest_type = "critical_only"
        else:
            recommended_digest_type = "daily"
        return {
            "db_status": self._db.verify_all_databases(),
            "buffer_stats": self.buffer_stats(),
            "heartbeat_state": heartbeat_state,
            "last_heartbeat_at": heartbeat["timestamp"] if heartbeat is not None else None,
            "pending_gates": pending_gates,
            "pending_harvests": pending_harvests,
            "alert_counts": {row["tier"]: row["count"] for row in alert_counts},
            "unacknowledged_t3_alerts": unacknowledged_t3,
            "circuit_breakers": self.circuit_breaker_status(),
            "quarantined_responses": {
                "pending_review_count": int(quarantine_summary["pending_count"]),
                "recent": self.recent_quarantined_responses(limit=3, pending_review_only=True),
            },
            "g3_requests": g3_requests,
            "disputed_costs": {
                "count": int(disputed_summary["disputed_count"]),
                "amount_usd": float(disputed_summary["disputed_amount"]),
                "recent": self.recent_disputed_costs(limit=3),
            },
            "judge_fallback": {
                "count": int(fallback_summary["fallback_count"] or 0),
                "blocked_count": int(fallback_summary["blocked_count"] or 0),
                "last_seen_at": fallback_summary["last_seen_at"],
                "recent": self.recent_fallback_judge_verdicts(limit=3),
            },
            "judge_deadlock": judge_deadlock,
            "runtime_control": {
                **runtime_status,
                "blocked_restart_attempts": blocked_restart_attempts,
                "recent_halts": self.runtime_halt_events(limit=3),
                "recent_restarts": self.runtime_restart_history(limit=3),
            },
            "council_health": {
                "tier2_24h": int(council_summary["tier2_24h"] or 0),
                "degraded_24h": int(council_summary["degraded_24h"] or 0),
                "low_confidence_24h": int(council_summary["low_confidence_24h"] or 0),
                "pending_tier2_g3": int(council_pending_g3 or 0),
                "backlog_alerts_24h": int(council_backlog_alerts or 0),
            },
            "harness_variants": self.harness_variant_summary(),
            "research_health": {
                "pending_tasks": strategic.execute(
                    "SELECT COUNT(*) FROM research_tasks WHERE status = 'PENDING'"
                ).fetchone()[0],
                "stale_tasks": strategic.execute(
                    "SELECT COUNT(*) FROM research_tasks WHERE status = 'STALE'"
                ).fetchone()[0],
                "failed_tasks": strategic.execute(
                    "SELECT COUNT(*) FROM research_tasks WHERE status = 'FAILED'"
                ).fetchone()[0],
            },
            "operator_load": operator_load,
            "recommended_digest_type": recommended_digest_type,
        }

    def milestone_health(self) -> dict[str, Any]:
        config = IntegrationConfig(data_dir=str(self._db.data_dir))
        return evaluate_milestone_status(config, db_manager=self._db)

    def workspace_overview(self) -> dict[str, Any]:
        health = self.system_health()
        return {
            "system_health": health,
            "runtime_status": self.runtime_status(),
            "recent_quarantines": self.recent_quarantined_responses(limit=5, pending_review_only=True),
            "recent_g3_requests": self.recent_g3_approval_requests(limit=5, status="PENDING"),
            "recent_execution_traces": self.execution_traces(limit=5),
            "harness_frontier": self.harness_frontier(limit=5),
            "milestone_health": self.milestone_health(),
        }

    def _operator_load_snapshot(self) -> dict:
        operator = self._db.get_connection("operator_digest")
        now = datetime.datetime.now(datetime.timezone.utc)
        week_start = (now - datetime.timedelta(days=now.weekday())).date().isoformat()
        row = operator.execute(
            """
            SELECT week_start, gates_surfaced, harvests_created, harvests_completed,
                   harvests_expired, estimated_hours, overload_triggered, created_at
            FROM operator_load_tracking
            WHERE week_start = ?
            ORDER BY created_at DESC, entry_id DESC
            LIMIT 1
            """,
            (week_start,),
        ).fetchone()
        if row is None:
            pending_total = operator.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM gate_log WHERE status = 'PENDING')
                    + (SELECT COUNT(*) FROM harvest_requests WHERE status = 'PENDING')
                """
            ).fetchone()[0]
            estimated_hours = (7 * 20.0 / 60.0) + pending_total * (15.0 / 60.0)
            return {
                "week_start": week_start,
                "gates_surfaced": {"G1": 0, "G2": 0, "G3": 0, "G4": 0},
                "harvests_created": 0,
                "harvests_completed": 0,
                "harvests_expired": 0,
                "estimated_hours": estimated_hours,
                "critical_only_recommended": estimated_hours > 15.0,
                "sustained_overload": False,
            }
        previous = operator.execute(
            """
            SELECT overload_triggered
            FROM operator_load_tracking
            WHERE week_start < ?
            ORDER BY week_start DESC, created_at DESC, entry_id DESC
            LIMIT 1
            """,
            (week_start,),
        ).fetchone()
        return {
            "week_start": row["week_start"],
            "gates_surfaced": json.loads(row["gates_surfaced"]),
            "harvests_created": row["harvests_created"],
            "harvests_completed": row["harvests_completed"],
            "harvests_expired": row["harvests_expired"],
            "estimated_hours": row["estimated_hours"],
            "critical_only_recommended": row["estimated_hours"] > 15.0,
            "sustained_overload": row["estimated_hours"] > 15.0 and bool(previous and previous["overload_triggered"]),
        }

    @staticmethod
    def _heartbeat_state(last_timestamp: str) -> str:
        now = datetime.datetime.now(datetime.timezone.utc)
        seen = datetime.datetime.fromisoformat(last_timestamp)
        hours = (now - seen).total_seconds() / 3600
        if hours < 72:
            return "ACTIVE"
        if hours < 168:
            return "CONSERVATIVE"
        return "ABSENT"


_SKILL: Optional[ObservabilitySkill] = None


def configure_skill(db_manager: DatabaseManager, telemetry_buffer: Optional[AppendBuffer], immune_buffer: Optional[AppendBuffer]):
    global _SKILL
    _SKILL = ObservabilitySkill(db_manager, telemetry_buffer, immune_buffer)


def observability_entry(action: str, **kwargs):
    if _SKILL is None:
        raise RuntimeError("observability skill not configured")
    if action == "query_immune_verdicts":
        return _SKILL.query_immune_verdicts(
            kwargs.get("limit", 20),
            kwargs.get("outcome"),
            kwargs.get("judge_mode"),
        )
    if action == "query_telemetry":
        return _SKILL.query_telemetry(
            kwargs.get("skill_name"),
            kwargs.get("limit", 50),
            chain_id=kwargs.get("chain_id"),
            outcome=kwargs.get("outcome"),
        )
    if action == "query_council_verdicts":
        return _SKILL.query_council_verdicts(
            kwargs.get("limit", 20),
            decision_type=kwargs.get("decision_type"),
            project_id=kwargs.get("project_id"),
        )
    if action == "query_alert_history":
        return _SKILL.query_alert_history(
            kwargs.get("limit", 20),
            tier=kwargs.get("tier"),
            alert_type=kwargs.get("alert_type"),
            unacknowledged_only=kwargs.get("unacknowledged_only", False),
            include_suppressed=kwargs.get("include_suppressed", True),
        )
    if action == "query_circuit_breakers":
        return _SKILL.query_circuit_breakers(
            kwargs.get("limit", 20),
            breaker_name=kwargs.get("breaker_name"),
            state=kwargs.get("state"),
        )
    if action == "recent_compound_breakers":
        return _SKILL.recent_compound_breakers(
            kwargs.get("limit", 10),
            kwargs.get("unresolved_only", False),
        )
    if action == "recent_quarantined_responses":
        return _SKILL.recent_quarantined_responses(
            kwargs.get("limit", 10),
            kwargs.get("pending_review_only", False),
        )
    if action == "recent_disputed_costs":
        return _SKILL.recent_disputed_costs(kwargs.get("limit", 10))
    if action == "recent_g3_approval_requests":
        return _SKILL.recent_g3_approval_requests(
            kwargs.get("limit", 10),
            kwargs.get("status"),
        )
    if action == "recent_fallback_judge_verdicts":
        return _SKILL.recent_fallback_judge_verdicts(kwargs.get("limit", 10))
    if action == "recent_judge_fallback_events":
        return _SKILL.recent_judge_fallback_events(kwargs.get("limit", 10))
    if action == "judge_fallback_review_queue":
        return _SKILL.judge_fallback_review_queue(
            kwargs.get("limit", 10),
            kwargs.get("review_status"),
        )
    if action == "judge_deadlock_status":
        return _SKILL.judge_deadlock_status()
    if action == "runtime_status":
        return _SKILL.runtime_status()
    if action == "runtime_halt_events":
        return _SKILL.runtime_halt_events(
            kwargs.get("limit", 10),
            kwargs.get("status"),
        )
    if action == "runtime_restart_history":
        return _SKILL.runtime_restart_history(
            kwargs.get("limit", 10),
            kwargs.get("status"),
        )
    if action == "execution_traces":
        return _SKILL.execution_traces(
            kwargs.get("limit", 20),
            kwargs.get("skill_name"),
            kwargs.get("training_eligible"),
            kwargs.get("judge_verdict"),
        )
    if action == "harness_variants":
        return _SKILL.harness_variants(
            kwargs.get("limit", 20),
            kwargs.get("skill_name"),
            kwargs.get("status"),
        )
    if action == "harness_frontier":
        return _SKILL.harness_frontier(
            kwargs.get("limit", 20),
            kwargs.get("skill_name"),
        )
    if action == "harness_variant_summary":
        return _SKILL.harness_variant_summary()
    if action == "recent_digests":
        return _SKILL.recent_digests(kwargs.get("limit", 5), kwargs.get("digest_type"))
    if action == "reliability_dashboard":
        return _SKILL.reliability_dashboard(kwargs.get("limit", 20))
    if action == "buffer_stats":
        return _SKILL.buffer_stats()
    if action == "circuit_breaker_status":
        return _SKILL.circuit_breaker_status()
    if action == "system_health":
        return _SKILL.system_health()
    if action == "milestone_health":
        return _SKILL.milestone_health()
    if action == "workspace_overview":
        return _SKILL.workspace_overview()
    raise ValueError(f"Unknown action: {action}")
