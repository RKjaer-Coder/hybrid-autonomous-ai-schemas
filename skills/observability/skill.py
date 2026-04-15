from __future__ import annotations

import datetime
import json
from typing import Optional

from skills.append_buffer import AppendBuffer
from skills.db_manager import DatabaseManager


class ObservabilitySkill:
    def __init__(self, db_manager: DatabaseManager, telemetry_buffer: Optional[AppendBuffer], immune_buffer: Optional[AppendBuffer]):
        self._db = db_manager
        self._telemetry_buffer = telemetry_buffer
        self._immune_buffer = immune_buffer

    def query_immune_verdicts(self, limit: int = 20, outcome: str | None = None) -> list[dict]:
        conn = self._db.get_connection("immune")
        if outcome:
            rows = conn.execute("SELECT * FROM immune_verdicts WHERE result = ? ORDER BY timestamp DESC LIMIT ?", (outcome, limit)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM immune_verdicts ORDER BY timestamp DESC LIMIT ?", (limit,)).fetchall()
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
                da_quality_score, tie_break, created_at
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
        overload = self._operator_load_snapshot()["critical_only_recommended"]
        return {
            "critical": critical,
            "degraded": degraded,
            "unacknowledged_t3_alerts": unacknowledged_t3,
            "operator_overload": overload,
        }

    def system_health(self) -> dict:
        operator = self._db.get_connection("operator_digest")
        strategic = self._db.get_connection("strategic_memory")
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
        heartbeat_state = self._heartbeat_state(heartbeat["timestamp"]) if heartbeat is not None else "ABSENT"
        operator_load = self._operator_load_snapshot()
        if heartbeat_state != "ACTIVE":
            recommended_digest_type = "catch_up"
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
        return _SKILL.query_immune_verdicts(kwargs.get("limit", 20), kwargs.get("outcome"))
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
    raise ValueError(f"Unknown action: {action}")
