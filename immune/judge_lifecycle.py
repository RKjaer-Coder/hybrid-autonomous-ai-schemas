from __future__ import annotations

import datetime
import json
from pathlib import Path
import sqlite3
import uuid
from typing import Any

from immune.judge import judge_check
from immune.types import (
    AlertSeverity,
    BlockReason,
    CheckType,
    CircuitBreakerState,
    ImmuneConfig,
    ImmuneVerdict,
    JudgeMode,
    JudgePayload,
    Outcome,
    Tier,
    generate_uuid_v7,
)

_REQUIRED_TABLES = {
    "immune_verdicts",
    "circuit_breaker_log",
    "judge_fallback_events",
    "judge_fallback_review_queue",
}


def _parse_ts(value: str) -> datetime.datetime:
    parsed = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=datetime.timezone.utc)
    return parsed.astimezone(datetime.timezone.utc)


def _to_iso(value: datetime.datetime) -> str:
    return value.astimezone(datetime.timezone.utc).replace(microsecond=0).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


class JudgeLifecycleManager:
    """Explicit, auditable Judge fallback/deadlock lifecycle manager."""

    def __init__(self, immune_db_path: str, config: ImmuneConfig):
        self._immune_db_path = immune_db_path
        self._config = config
        operator_db = Path(immune_db_path).with_name("operator_digest.db")
        self._operator_db_path = str(operator_db) if operator_db.exists() else None
        self._available = self._verify_tables()

    @property
    def available(self) -> bool:
        return self._available

    def prepare_payload(self, payload: JudgePayload, *, reference_time: str | None = None) -> tuple[JudgePayload, dict[str, Any] | None]:
        if not self._available:
            return payload, None
        now = self._now(reference_time)
        with self._connect() as conn:
            self._resolve_expired_locked(conn, now)
            active = self._active_event_locked(conn)
            if active is None:
                return payload, None
            if active["status"] == "HALTED":
                return payload, self._row_to_event(active)
            if payload.force_structural_fallback:
                return payload, self._row_to_event(active)
            prepared = JudgePayload(
                session_id=payload.session_id,
                skill_name=payload.skill_name,
                tool_name=payload.tool_name,
                output=payload.output,
                task_type=payload.task_type,
                expected_schema=payload.expected_schema,
                max_trust_tier=payload.max_trust_tier,
                memory_write_target=payload.memory_write_target,
                allow_structural_fallback=True,
                force_structural_fallback=True,
                fallback_reason=payload.fallback_reason or f"judge_deadlock:{active['event_id']}",
            )
            return prepared, self._row_to_event(active)

    def halted_verdict(self, payload: JudgePayload) -> ImmuneVerdict:
        return ImmuneVerdict(
            generate_uuid_v7(),
            CheckType.JUDGE,
            Tier.FAST_PATH,
            payload.skill_name,
            payload.session_id,
            Outcome.BLOCK,
            BlockReason.INTERNAL_ERROR,
            "Judge deadlock halt active; operator restart required",
            0.0,
            AlertSeverity.SECURITY_ALERT,
            judge_mode=JudgeMode.NORMAL,
        )

    def record_verdict(
        self,
        payload: JudgePayload,
        verdict: ImmuneVerdict,
        *,
        reference_time: str | None = None,
    ) -> dict[str, Any] | None:
        if not self._available:
            return None
        now = self._now(reference_time)
        with self._connect() as conn:
            self._resolve_expired_locked(conn, now)
            active = self._active_event_locked(conn)
            if verdict.judge_mode == JudgeMode.FALLBACK and verdict.outcome == Outcome.PASS and active is not None:
                self._enqueue_review_locked(conn, active["event_id"], payload, verdict, now)
                conn.commit()
                return {"event_id": active["event_id"], "status": active["status"], "queued_for_review": True}
            if verdict.judge_mode != JudgeMode.NORMAL:
                return None
            metrics = self._deadlock_metrics_locked(conn, now, current_payload=payload, current_verdict=verdict)
            if not self._should_trigger_deadlock(metrics):
                return None
            if active is not None:
                return {"event_id": active["event_id"], "status": active["status"], "block_rate": metrics["block_rate"]}
            recent_guard = self._recent_guard_event_locked(conn, now)
            if recent_guard is not None:
                result = self._halt_locked(
                    conn,
                    now,
                    metrics,
                    reason="judge_deadlock_guard_retriggered",
                    prior_event_id=recent_guard["event_id"],
                )
                conn.commit()
                return result
            result = self._activate_fallback_locked(conn, now, metrics)
            conn.commit()
            return result

    def restart_after_deadlock(
        self,
        *,
        event_id: str | None = None,
        reference_time: str | None = None,
    ) -> dict[str, Any]:
        if not self._available:
            raise RuntimeError("Judge lifecycle tables are not available")
        now = self._now(reference_time)
        with self._connect() as conn:
            row = self._select_restart_target_locked(conn, event_id)
            if row is None:
                raise KeyError(event_id or "active judge fallback event")
            conn.execute(
                """
                UPDATE judge_fallback_events
                SET acknowledged_at = COALESCE(acknowledged_at, ?)
                WHERE event_id = ?
                """,
                (now, row["event_id"]),
            )
            metrics = self._deadlock_metrics_locked(conn, now)
            if self._should_trigger_deadlock(metrics):
                result = self._halt_locked(
                    conn,
                    now,
                    metrics,
                    reason="operator_restart_requested_but_deadlock_persisted",
                    prior_event_id=row["event_id"],
                    update_event_id=row["event_id"],
                )
                conn.commit()
                return result
            self._clear_locked(
                conn,
                row["event_id"],
                now,
                reason="operator_restart_restored",
            )
            conn.commit()
            return {
                "event_id": row["event_id"],
                "status": "CLEARED",
                "review_queue": self._review_queue_summary_locked(conn, row["event_id"]),
            }

    def status(self, *, reference_time: str | None = None) -> dict[str, Any]:
        if not self._available:
            return {
                "available": False,
                "mode": "NORMAL",
                "active_event": None,
                "guard_until": None,
                "review_queue": {"pending": 0, "blocked": 0, "passed": 0},
            }
        now = self._now(reference_time)
        with self._connect() as conn:
            self._resolve_expired_locked(conn, now)
            active = self._active_event_locked(conn)
            recent_guard = self._recent_guard_event_locked(conn, now)
            queue = self._review_queue_summary_locked(conn, active["event_id"] if active is not None else None)
            mode = "NORMAL"
            if active is not None and active["status"] == "ACTIVE":
                mode = "FALLBACK"
            elif active is not None and active["status"] == "HALTED":
                mode = "HALTED"
            return {
                "available": True,
                "mode": mode,
                "active_event": None if active is None else self._row_to_event(active),
                "guard_until": None
                if recent_guard is None
                else _to_iso(_parse_ts(recent_guard["started_at"]) + datetime.timedelta(hours=self._config.judge_deadlock_guard_hours)),
                "review_queue": queue,
            }

    def list_events(self, *, limit: int = 20, reference_time: str | None = None) -> list[dict[str, Any]]:
        if not self._available:
            return []
        now = self._now(reference_time)
        with self._connect() as conn:
            self._resolve_expired_locked(conn, now)
            rows = conn.execute(
                """
                SELECT *
                FROM judge_fallback_events
                ORDER BY started_at DESC, event_id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [self._row_to_event(row) for row in rows]

    def list_review_queue(
        self,
        *,
        limit: int = 20,
        review_status: str | None = None,
    ) -> list[dict[str, Any]]:
        if not self._available:
            return []
        where_sql = ""
        params: list[object] = []
        if review_status:
            where_sql = "WHERE review_status = ?"
            params.append(review_status)
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT *
                FROM judge_fallback_review_queue
                {where_sql}
                ORDER BY enqueued_at DESC, queue_id DESC
                LIMIT ?
                """,
                (*params, limit),
            ).fetchall()
        return [self._row_to_review(row) for row in rows]

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._immune_db_path, timeout=5.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        return conn

    def _verify_tables(self) -> bool:
        try:
            with self._connect() as conn:
                tables = {
                    row[0]
                    for row in conn.execute(
                        "SELECT name FROM sqlite_master WHERE type='table'"
                    ).fetchall()
                }
            return _REQUIRED_TABLES.issubset(tables)
        except Exception:
            return False

    def _now(self, reference_time: str | None) -> str:
        if reference_time is not None:
            return _to_iso(_parse_ts(reference_time))
        return _to_iso(datetime.datetime.now(datetime.timezone.utc))

    def _active_event_locked(self, conn: sqlite3.Connection) -> sqlite3.Row | None:
        return conn.execute(
            """
            SELECT *
            FROM judge_fallback_events
            WHERE status IN ('ACTIVE', 'HALTED')
            ORDER BY
                CASE status WHEN 'HALTED' THEN 0 ELSE 1 END,
                started_at DESC,
                event_id DESC
            LIMIT 1
            """
        ).fetchone()

    def _recent_guard_event_locked(self, conn: sqlite3.Connection, now: str) -> sqlite3.Row | None:
        cutoff = _to_iso(_parse_ts(now) - datetime.timedelta(hours=self._config.judge_deadlock_guard_hours))
        return conn.execute(
            """
            SELECT *
            FROM judge_fallback_events
            WHERE started_at >= ?
            ORDER BY started_at DESC, event_id DESC
            LIMIT 1
            """,
            (cutoff,),
        ).fetchone()

    def _select_restart_target_locked(self, conn: sqlite3.Connection, event_id: str | None) -> sqlite3.Row | None:
        if event_id is not None:
            return conn.execute(
                """
                SELECT *
                FROM judge_fallback_events
                WHERE event_id = ?
                LIMIT 1
                """,
                (event_id,),
            ).fetchone()
        return self._active_event_locked(conn)

    def _resolve_expired_locked(self, conn: sqlite3.Connection, now: str) -> None:
        rows = conn.execute(
            """
            SELECT *
            FROM judge_fallback_events
            WHERE status = 'ACTIVE' AND expires_at <= ?
            ORDER BY started_at ASC, event_id ASC
            """,
            (now,),
        ).fetchall()
        for row in rows:
            metrics = self._deadlock_metrics_locked(conn, now)
            if self._should_trigger_deadlock(metrics):
                self._halt_locked(
                    conn,
                    now,
                    metrics,
                    reason="judge_deadlock_fallback_expired_with_persistent_blocks",
                    prior_event_id=row["event_id"],
                    update_event_id=row["event_id"],
                )
            else:
                self._clear_locked(
                    conn,
                    row["event_id"],
                    now,
                    reason="judge_deadlock_fallback_expired_restored",
                )
        if rows:
            conn.commit()

    def _deadlock_metrics_locked(
        self,
        conn: sqlite3.Connection,
        now: str,
        *,
        current_payload: JudgePayload | None = None,
        current_verdict: ImmuneVerdict | None = None,
    ) -> dict[str, Any]:
        cutoff = _to_iso(_parse_ts(now) - datetime.timedelta(seconds=self._config.judge_deadlock_window_seconds))
        rows = conn.execute(
            """
            SELECT verdict_id, skill_name, result, timestamp
            FROM immune_verdicts
            WHERE verdict_type = 'judge_output'
              AND judge_mode = 'NORMAL'
              AND timestamp >= ?
            ORDER BY timestamp ASC, verdict_id ASC
            """,
            (cutoff,),
        ).fetchall()
        samples = [dict(row) for row in rows]
        if (
            current_payload is not None
            and current_verdict is not None
            and current_verdict.check_type == CheckType.JUDGE
            and current_verdict.judge_mode == JudgeMode.NORMAL
            and current_verdict.verdict_id not in {row["verdict_id"] for row in samples}
        ):
            samples.append(
                {
                    "verdict_id": current_verdict.verdict_id,
                    "skill_name": current_payload.task_type or current_payload.skill_name,
                    "result": "PASS" if current_verdict.outcome == Outcome.PASS else current_verdict.outcome.value,
                    "timestamp": now,
                }
            )
        total = len(samples)
        blocked = [
            row
            for row in samples
            if row["result"] in {"BLOCK", "TIMEOUT"}
        ]
        span_seconds = 0.0
        if total >= 2:
            span_seconds = (
                _parse_ts(samples[-1]["timestamp"]) - _parse_ts(samples[0]["timestamp"])
            ).total_seconds()
        distinct_task_types = sorted({row["skill_name"] for row in blocked})
        block_rate = 0.0 if total == 0 else len(blocked) / total
        return {
            "total": total,
            "blocked": len(blocked),
            "block_rate": block_rate,
            "distinct_task_types": distinct_task_types,
            "window_seconds": self._config.judge_deadlock_window_seconds,
            "span_seconds": span_seconds,
        }

    def _should_trigger_deadlock(self, metrics: dict[str, Any]) -> bool:
        return (
            metrics["total"] > 0
            and metrics["block_rate"] > self._config.judge_deadlock_block_rate_threshold
            and len(metrics["distinct_task_types"]) >= self._config.judge_deadlock_distinct_task_types
            and metrics["span_seconds"] >= self._config.judge_deadlock_window_seconds
        )

    def _activate_fallback_locked(
        self,
        conn: sqlite3.Connection,
        now: str,
        metrics: dict[str, Any],
    ) -> dict[str, Any]:
        event_id = str(uuid.uuid4())
        expires_at = _to_iso(_parse_ts(now) + datetime.timedelta(minutes=self._config.judge_deadlock_fallback_minutes))
        alert_id = self._write_operator_alert("T2", "JUDGE_DEADLOCK", metrics, now, expires_at)
        conn.execute(
            """
            INSERT INTO judge_fallback_events (
                event_id, trigger_source, status, trigger_reason, block_rate,
                blocked_count, total_count, distinct_task_types, started_at,
                expires_at, acknowledged_at, ended_at, end_reason, operator_alert_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                "JUDGE_DEADLOCK",
                "ACTIVE",
                "Judge deadlock detected from sustained false-positive regime",
                metrics["block_rate"],
                metrics["blocked"],
                metrics["total"],
                _json(metrics["distinct_task_types"]),
                now,
                expires_at,
                None,
                None,
                None,
                alert_id,
            ),
        )
        self._log_breaker_locked(
            conn,
            breaker_name="JUDGE_DEADLOCK",
            action_taken="IMMUNE_BYPASS_MODE",
            requires_human=False,
            timestamp=now,
        )
        return {
            "event_id": event_id,
            "status": "ACTIVE",
            "expires_at": expires_at,
            "block_rate": metrics["block_rate"],
            "distinct_task_types": metrics["distinct_task_types"],
        }

    def _halt_locked(
        self,
        conn: sqlite3.Connection,
        now: str,
        metrics: dict[str, Any],
        *,
        reason: str,
        prior_event_id: str,
        update_event_id: str | None = None,
    ) -> dict[str, Any]:
        event_id = update_event_id or str(uuid.uuid4())
        alert_id = self._write_operator_alert("T1", "JUDGE_DEADLOCK_HALT", metrics, now, None)
        if update_event_id is None:
            conn.execute(
                """
                INSERT INTO judge_fallback_events (
                    event_id, trigger_source, status, trigger_reason, block_rate,
                    blocked_count, total_count, distinct_task_types, started_at,
                    expires_at, acknowledged_at, ended_at, end_reason,
                    prior_event_id, operator_alert_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    "JUDGE_DEADLOCK",
                    "HALTED",
                    "Judge deadlock retriggered inside 24h guard",
                    metrics["block_rate"],
                    metrics["blocked"],
                    metrics["total"],
                    _json(metrics["distinct_task_types"]),
                    now,
                    now,
                    None,
                    now,
                    reason,
                    prior_event_id,
                    alert_id,
                ),
            )
        else:
            conn.execute(
                """
                UPDATE judge_fallback_events
                SET status = 'HALTED',
                    block_rate = ?,
                    blocked_count = ?,
                    total_count = ?,
                    distinct_task_types = ?,
                    ended_at = ?,
                    end_reason = ?,
                    prior_event_id = COALESCE(prior_event_id, ?),
                    operator_alert_id = COALESCE(operator_alert_id, ?)
                WHERE event_id = ?
                """,
                (
                    metrics["block_rate"],
                    metrics["blocked"],
                    metrics["total"],
                    _json(metrics["distinct_task_types"]),
                    now,
                    reason,
                    prior_event_id,
                    alert_id,
                    update_event_id,
                ),
            )
        self._log_breaker_locked(
            conn,
            breaker_name="JUDGE_DEADLOCK",
            action_taken="FULL_SYSTEM_HALT",
            requires_human=True,
            timestamp=now,
        )
        return {
            "event_id": event_id,
            "status": "HALTED",
            "block_rate": metrics["block_rate"],
            "distinct_task_types": metrics["distinct_task_types"],
        }

    def _clear_locked(self, conn: sqlite3.Connection, event_id: str, now: str, *, reason: str) -> None:
        conn.execute(
            """
            UPDATE judge_fallback_events
            SET status = 'CLEARED',
                ended_at = COALESCE(ended_at, ?),
                end_reason = COALESCE(end_reason, ?)
            WHERE event_id = ?
            """,
            (now, reason, event_id),
        )
        self._run_retroactive_reviews_locked(conn, event_id, now)

    def _enqueue_review_locked(
        self,
        conn: sqlite3.Connection,
        event_id: str,
        payload: JudgePayload,
        verdict: ImmuneVerdict,
        now: str,
    ) -> None:
        conn.execute(
            """
            INSERT OR IGNORE INTO judge_fallback_review_queue (
                queue_id, fallback_event_id, source_verdict_id, session_id,
                skill_name, tool_name, task_type, output_json, expected_schema_json,
                max_trust_tier, memory_write_target, enqueued_at, review_status,
                reviewed_at, review_verdict_id, review_outcome, review_reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid.uuid4()),
                event_id,
                verdict.verdict_id,
                payload.session_id,
                payload.skill_name,
                payload.tool_name,
                payload.task_type or payload.skill_name,
                _json(payload.output),
                None if payload.expected_schema is None else _json(payload.expected_schema),
                payload.max_trust_tier,
                payload.memory_write_target,
                now,
                "PENDING",
                None,
                None,
                None,
                None,
            ),
        )

    def _run_retroactive_reviews_locked(self, conn: sqlite3.Connection, event_id: str, now: str) -> None:
        rows = conn.execute(
            """
            SELECT *
            FROM judge_fallback_review_queue
            WHERE fallback_event_id = ? AND review_status = 'PENDING'
            ORDER BY enqueued_at ASC, queue_id ASC
            """,
            (event_id,),
        ).fetchall()
        strict_config = self._config.__class__(
            **{**self._config.__dict__, "judge_structural_fallback_enabled": False}
        )
        for row in rows:
            payload = JudgePayload(
                session_id=row["session_id"],
                skill_name=row["skill_name"],
                tool_name=row["tool_name"],
                output=json.loads(row["output_json"]),
                task_type=row["task_type"],
                expected_schema=None if row["expected_schema_json"] is None else json.loads(row["expected_schema_json"]),
                max_trust_tier=int(row["max_trust_tier"]),
                memory_write_target=row["memory_write_target"],
                allow_structural_fallback=False,
                force_structural_fallback=False,
            )
            review = judge_check(payload, strict_config)
            conn.execute(
                """
                UPDATE judge_fallback_review_queue
                SET review_status = ?,
                    reviewed_at = ?,
                    review_verdict_id = ?,
                    review_outcome = ?,
                    review_reason = ?
                WHERE queue_id = ?
                """,
                (
                    "PASS" if review.outcome == Outcome.PASS else "BLOCK",
                    now,
                    review.verdict_id,
                    review.outcome.value,
                    review.block_reason.value if review.block_reason else review.block_detail,
                    row["queue_id"],
                ),
            )

    def _review_queue_summary_locked(self, conn: sqlite3.Connection, event_id: str | None) -> dict[str, Any]:
        where_sql = ""
        params: list[object] = []
        if event_id is not None:
            where_sql = "WHERE fallback_event_id = ?"
            params.append(event_id)
        row = conn.execute(
            f"""
            SELECT
                SUM(CASE WHEN review_status = 'PENDING' THEN 1 ELSE 0 END) AS pending_count,
                SUM(CASE WHEN review_status = 'BLOCK' THEN 1 ELSE 0 END) AS blocked_count,
                SUM(CASE WHEN review_status = 'PASS' THEN 1 ELSE 0 END) AS passed_count,
                MAX(reviewed_at) AS last_reviewed_at
            FROM judge_fallback_review_queue
            {where_sql}
            """,
            tuple(params),
        ).fetchone()
        return {
            "pending": int(row["pending_count"] or 0),
            "blocked": int(row["blocked_count"] or 0),
            "passed": int(row["passed_count"] or 0),
            "last_reviewed_at": row["last_reviewed_at"],
        }

    def _log_breaker_locked(
        self,
        conn: sqlite3.Connection,
        *,
        breaker_name: str,
        action_taken: str,
        requires_human: bool,
        timestamp: str,
    ) -> None:
        conn.execute(
            """
            INSERT INTO circuit_breaker_log (
                event_id, breaker_name, state, trip_condition, action_taken,
                requires_human, auto_reset_at, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid.uuid4()),
                breaker_name,
                CircuitBreakerState.TRIPPED.value,
                "Judge deadlock lifecycle",
                action_taken,
                1 if requires_human else 0,
                None,
                timestamp,
            ),
        )

    def _write_operator_alert(
        self,
        tier: str,
        alert_type: str,
        metrics: dict[str, Any],
        now: str,
        expires_at: str | None,
    ) -> str | None:
        if self._operator_db_path is None:
            return None
        alert_id = str(uuid.uuid4())
        content = (
            f"Judge deadlock block_rate={metrics['block_rate']:.2f} "
            f"blocked={metrics['blocked']}/{metrics['total']} "
            f"task_types={','.join(metrics['distinct_task_types']) or 'none'}"
        )
        if expires_at is not None:
            content += f" fallback_expires_at={expires_at}"
        try:
            with sqlite3.connect(self._operator_db_path, timeout=5.0) as conn:
                conn.execute("PRAGMA journal_mode=WAL;")
                conn.execute("PRAGMA busy_timeout=5000;")
                conn.execute(
                    """
                    INSERT INTO alert_log (
                        alert_id, tier, alert_type, content, channel_delivered,
                        suppressed, acknowledged, acknowledged_at, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (alert_id, tier, alert_type, content, "CLI", 0, 0, None, now),
                )
                conn.commit()
        except Exception:
            return None
        return alert_id

    @staticmethod
    def _row_to_event(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "event_id": row["event_id"],
            "trigger_source": row["trigger_source"],
            "status": row["status"],
            "trigger_reason": row["trigger_reason"],
            "block_rate": float(row["block_rate"]),
            "blocked_count": int(row["blocked_count"]),
            "total_count": int(row["total_count"]),
            "distinct_task_types": json.loads(row["distinct_task_types"]),
            "started_at": row["started_at"],
            "expires_at": row["expires_at"],
            "acknowledged_at": row["acknowledged_at"],
            "ended_at": row["ended_at"],
            "end_reason": row["end_reason"],
            "prior_event_id": row["prior_event_id"],
            "operator_alert_id": row["operator_alert_id"],
        }

    @staticmethod
    def _row_to_review(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "queue_id": row["queue_id"],
            "fallback_event_id": row["fallback_event_id"],
            "source_verdict_id": row["source_verdict_id"],
            "session_id": row["session_id"],
            "skill_name": row["skill_name"],
            "tool_name": row["tool_name"],
            "task_type": row["task_type"],
            "enqueued_at": row["enqueued_at"],
            "review_status": row["review_status"],
            "reviewed_at": row["reviewed_at"],
            "review_verdict_id": row["review_verdict_id"],
            "review_outcome": row["review_outcome"],
            "review_reason": row["review_reason"],
        }
