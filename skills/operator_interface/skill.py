from __future__ import annotations

import datetime
import json
import uuid
from dataclasses import asdict, dataclass
from typing import Any, Optional

from skills.db_manager import DatabaseManager


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
        return {
            "alert_id": alert_id,
            "acknowledged": True,
            "acknowledged_at": now if not bool(row["acknowledged"]) else None,
        }

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
        return {
            "digest_id": digest_id,
            "acknowledged_at": now if row["acknowledged_at"] is None else row["acknowledged_at"],
        }

    def generate_digest(self, digest_type: str = "daily", operator_state: str | None = None) -> dict:
        now = self._utc_now()
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
        ordered_names = list(CRITICAL_SECTION_ORDER if effective_type == "critical_only" else DAILY_SECTION_ORDER)
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
            sections = {name: sections[name] for name in CRITICAL_SECTION_ORDER}
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
        if health["circuit_breakers"]["critical"]:
            issues.append("critical=" + ",".join(health["circuit_breakers"]["critical"]))
        if health["circuit_breakers"]["degraded"]:
            issues.append("degraded=" + ",".join(health["circuit_breakers"]["degraded"]))
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
        stale_tasks = strategic.execute(
            "SELECT COUNT(*) FROM research_tasks WHERE status = 'STALE'"
        ).fetchone()[0]
        failed_tasks = strategic.execute(
            "SELECT COUNT(*) FROM research_tasks WHERE status = 'FAILED'"
        ).fetchone()[0]
        return {
            "circuit_breakers": {"critical": critical, "degraded": degraded},
            "unacknowledged_t3_alerts": unacknowledged_t3,
            "research_health": {
                "stale_tasks": stale_tasks,
                "failed_tasks": failed_tasks,
            },
            "operator_load": load_snapshot,
            "timestamp": now,
        }

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
    if action == "generate_digest":
        return _SKILL.generate_digest(
            digest_type=kwargs.get("digest_type", "daily"),
            operator_state=kwargs.get("operator_state"),
        )
    if action == "record_operator_load_snapshot":
        return _SKILL.record_operator_load_snapshot(reference_time=kwargs.get("reference_time"))
    raise ValueError(f"Unknown action: {action}")
