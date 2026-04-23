from __future__ import annotations

import datetime
import json
from pathlib import Path
import sqlite3
import uuid
from typing import Any

from harness_variants import HarnessVariantManager


_REQUIRED_TABLES = {
    "runtime_control_state",
    "runtime_halt_events",
    "runtime_restart_history",
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


class RuntimeControlManager:
    """Single-writer runtime halt and restart state for operator-controlled recovery."""

    def __init__(self, operator_db_path: str):
        self._operator_db_path = operator_db_path
        telemetry_db = Path(operator_db_path).with_name("telemetry.db")
        self._harness_variants = (
            HarnessVariantManager(str(telemetry_db)) if telemetry_db.exists() else None
        )
        self._available = self._verify_tables()
        if self._available:
            with self._connect() as conn:
                self._ensure_state_row_locked(conn, self._now(None))
                conn.commit()

    @property
    def available(self) -> bool:
        return self._available

    def status(self, *, reference_time: str | None = None) -> dict[str, Any]:
        if not self._available:
            return {
                "available": False,
                "lifecycle_state": "ACTIVE",
                "active_halt": None,
                "last_restart": None,
            }
        now = self._now(reference_time)
        with self._connect() as conn:
            self._ensure_state_row_locked(conn, now)
            state = conn.execute(
                """
                SELECT lifecycle_state, active_halt_id, last_halt_reason, last_transition_at, last_restart_id
                FROM runtime_control_state
                WHERE state_id = 'runtime'
                LIMIT 1
                """
            ).fetchone()
            active_halt = self._active_halt_locked(conn)
            last_restart = self._last_restart_locked(conn)
        return {
            "available": True,
            "lifecycle_state": state["lifecycle_state"],
            "active_halt": None if active_halt is None else self._row_to_halt(active_halt),
            "last_halt_reason": state["last_halt_reason"],
            "last_transition_at": state["last_transition_at"],
            "last_restart": None if last_restart is None else self._row_to_restart(last_restart),
        }

    def activate_halt(
        self,
        *,
        source: str,
        halt_reason: str,
        trigger_event_id: str | None = None,
        halt_scope: str = "FULL_SYSTEM_HALT",
        requires_human: bool = True,
        reference_time: str | None = None,
    ) -> dict[str, Any]:
        if not self._available:
            raise RuntimeError("Runtime control tables are not available")
        now = self._now(reference_time)
        with self._connect() as conn:
            self._ensure_state_row_locked(conn, now)
            existing = self._active_halt_locked(conn)
            if existing is not None:
                return self._row_to_halt(existing)
            halt_id = str(uuid.uuid4())
            conn.execute(
                """
                INSERT INTO runtime_halt_events (
                    halt_id, halt_scope, source, trigger_event_id, halt_reason,
                    requires_human, created_at, cleared_at, clear_reason,
                    clear_restart_id, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    halt_id,
                    halt_scope,
                    source,
                    trigger_event_id,
                    halt_reason,
                    1 if requires_human else 0,
                    now,
                    None,
                    None,
                    None,
                    "ACTIVE",
                ),
            )
            conn.execute(
                """
                UPDATE runtime_control_state
                SET lifecycle_state = 'HALTED',
                    active_halt_id = ?,
                    last_halt_reason = ?,
                    last_transition_at = ?,
                    last_restart_id = NULL
                WHERE state_id = 'runtime'
                """,
                (halt_id, halt_reason, now),
            )
            conn.commit()
            halt_row = conn.execute(
                "SELECT * FROM runtime_halt_events WHERE halt_id = ? LIMIT 1",
                (halt_id,),
            ).fetchone()
        assert halt_row is not None
        result = self._row_to_halt(halt_row)
        self._log_trace(
            task_id=halt_id,
            role="runtime_halt_activation",
            action_name="activate_halt",
            intent_goal=f"Activate runtime halt {halt_id}.",
            payload=result,
            context_assembled=(
                f"source={source}; halt_reason={halt_reason}; halt_scope={halt_scope}; "
                f"requires_human={requires_human}"
            ),
            judge_verdict="FAIL",
            judge_reasoning="Runtime halt activated to preserve fail-closed recovery posture.",
            training_eligible=False,
            retention_class="FAILURE_AUDIT",
            outcome_score=0.0,
            created_at=now,
        )
        return result

    def record_blocked_restart(
        self,
        *,
        halt_id: str | None = None,
        restart_reason: str,
        preflight: dict[str, Any],
        notes: str | None = None,
        reference_time: str | None = None,
    ) -> dict[str, Any]:
        return self._record_restart(
            halt_id=halt_id,
            restart_reason=restart_reason,
            preflight=preflight,
            notes=notes,
            reference_time=reference_time,
            status="BLOCKED",
            clear_halt=False,
        )

    def complete_restart(
        self,
        *,
        halt_id: str | None = None,
        restart_reason: str,
        preflight: dict[str, Any],
        notes: str | None = None,
        reference_time: str | None = None,
    ) -> dict[str, Any]:
        return self._record_restart(
            halt_id=halt_id,
            restart_reason=restart_reason,
            preflight=preflight,
            notes=notes,
            reference_time=reference_time,
            status="COMPLETED",
            clear_halt=True,
        )

    def list_halt_events(self, *, limit: int = 20, status: str | None = None) -> list[dict[str, Any]]:
        if not self._available:
            return []
        where_sql = ""
        params: list[object] = []
        if status is not None:
            where_sql = "WHERE status = ?"
            params.append(status)
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT *
                FROM runtime_halt_events
                {where_sql}
                ORDER BY created_at DESC, halt_id DESC
                LIMIT ?
                """,
                (*params, limit),
            ).fetchall()
        return [self._row_to_halt(row) for row in rows]

    def list_restart_history(self, *, limit: int = 20, status: str | None = None) -> list[dict[str, Any]]:
        if not self._available:
            return []
        where_sql = ""
        params: list[object] = []
        if status is not None:
            where_sql = "WHERE status = ?"
            params.append(status)
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT *
                FROM runtime_restart_history
                {where_sql}
                ORDER BY requested_at DESC, restart_id DESC
                LIMIT ?
                """,
                (*params, limit),
            ).fetchall()
        return [self._row_to_restart(row) for row in rows]

    def _record_restart(
        self,
        *,
        halt_id: str | None,
        restart_reason: str,
        preflight: dict[str, Any],
        notes: str | None,
        reference_time: str | None,
        status: str,
        clear_halt: bool,
    ) -> dict[str, Any]:
        if not self._available:
            raise RuntimeError("Runtime control tables are not available")
        now = self._now(reference_time)
        with self._connect() as conn:
            self._ensure_state_row_locked(conn, now)
            halt_row = self._select_restart_target_locked(conn, halt_id)
            if halt_row is None:
                raise KeyError(halt_id or "active runtime halt")
            restart_id = str(uuid.uuid4())
            conn.execute(
                """
                INSERT INTO runtime_restart_history (
                    restart_id, halt_id, requested_at, completed_at, status,
                    restart_reason, preflight_json, notes
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    restart_id,
                    halt_row["halt_id"],
                    now,
                    now if clear_halt else None,
                    status,
                    restart_reason,
                    _json(preflight),
                    notes,
                ),
            )
            if clear_halt:
                conn.execute(
                    """
                    UPDATE runtime_halt_events
                    SET status = 'CLEARED',
                        cleared_at = ?,
                        clear_reason = ?,
                        clear_restart_id = ?
                    WHERE halt_id = ?
                    """,
                    (now, restart_reason, restart_id, halt_row["halt_id"]),
                )
                conn.execute(
                    """
                    UPDATE runtime_control_state
                    SET lifecycle_state = 'ACTIVE',
                        active_halt_id = NULL,
                        last_halt_reason = NULL,
                        last_transition_at = ?,
                        last_restart_id = ?
                    WHERE state_id = 'runtime'
                    """,
                    (now, restart_id),
                )
            conn.commit()
            restart_row = conn.execute(
                "SELECT * FROM runtime_restart_history WHERE restart_id = ? LIMIT 1",
                (restart_id,),
            ).fetchone()
        assert restart_row is not None
        result = self._row_to_restart(restart_row)
        self._log_trace(
            task_id=restart_id,
            role="runtime_restart_completed" if clear_halt else "runtime_restart_blocked",
            action_name="complete_restart" if clear_halt else "record_blocked_restart",
            intent_goal=(
                f"Clear runtime halt {halt_row['halt_id']} via restart {restart_id}."
                if clear_halt
                else f"Record blocked runtime restart {restart_id} for halt {halt_row['halt_id']}."
            ),
            payload=result,
            context_assembled=(
                f"halt_id={halt_row['halt_id']}; restart_reason={restart_reason}; "
                f"status={status}; clear_halt={clear_halt}"
            ),
            judge_verdict="PASS" if clear_halt else "FAIL",
            judge_reasoning=(
                "Runtime restart completed and the active halt was cleared."
                if clear_halt
                else "Runtime restart remained blocked and the halt stayed active."
            ),
            training_eligible=False,
            retention_class="FAILURE_AUDIT",
            outcome_score=1.0 if clear_halt else 0.0,
            created_at=now,
        )
        return result

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._operator_db_path, timeout=5.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        conn.execute("PRAGMA foreign_keys=ON;")
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

    def _ensure_state_row_locked(self, conn: sqlite3.Connection, now: str) -> None:
        conn.execute(
            """
            INSERT OR IGNORE INTO runtime_control_state (
                state_id, lifecycle_state, active_halt_id, last_halt_reason,
                last_transition_at, last_restart_id
            ) VALUES ('runtime', 'ACTIVE', NULL, NULL, ?, NULL)
            """,
            (now,),
        )

    @staticmethod
    def _active_halt_locked(conn: sqlite3.Connection) -> sqlite3.Row | None:
        return conn.execute(
            """
            SELECT *
            FROM runtime_halt_events
            WHERE status = 'ACTIVE'
            ORDER BY created_at DESC, halt_id DESC
            LIMIT 1
            """
        ).fetchone()

    @staticmethod
    def _last_restart_locked(conn: sqlite3.Connection) -> sqlite3.Row | None:
        return conn.execute(
            """
            SELECT *
            FROM runtime_restart_history
            ORDER BY requested_at DESC, restart_id DESC
            LIMIT 1
            """
        ).fetchone()

    def _select_restart_target_locked(
        self,
        conn: sqlite3.Connection,
        halt_id: str | None,
    ) -> sqlite3.Row | None:
        if halt_id is not None:
            return conn.execute(
                """
                SELECT *
                FROM runtime_halt_events
                WHERE halt_id = ? AND status = 'ACTIVE'
                LIMIT 1
                """,
                (halt_id,),
            ).fetchone()
        return self._active_halt_locked(conn)

    def _now(self, reference_time: str | None) -> str:
        if reference_time is not None:
            return _to_iso(_parse_ts(reference_time))
        return _to_iso(datetime.datetime.now(datetime.timezone.utc))

    @staticmethod
    def _row_to_halt(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "halt_id": row["halt_id"],
            "halt_scope": row["halt_scope"],
            "source": row["source"],
            "trigger_event_id": row["trigger_event_id"],
            "halt_reason": row["halt_reason"],
            "requires_human": bool(row["requires_human"]),
            "created_at": row["created_at"],
            "cleared_at": row["cleared_at"],
            "clear_reason": row["clear_reason"],
            "clear_restart_id": row["clear_restart_id"],
            "status": row["status"],
        }

    @staticmethod
    def _row_to_restart(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "restart_id": row["restart_id"],
            "halt_id": row["halt_id"],
            "requested_at": row["requested_at"],
            "completed_at": row["completed_at"],
            "status": row["status"],
            "restart_reason": row["restart_reason"],
            "preflight": json.loads(row["preflight_json"]),
            "notes": row["notes"],
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
        judge_verdict: str,
        judge_reasoning: str,
        training_eligible: bool,
        retention_class: str,
        outcome_score: float,
        created_at: str,
    ) -> None:
        if self._harness_variants is None or not self._harness_variants.available:
            return
        self._harness_variants.log_skill_action_trace(
            task_id=task_id,
            role=role,
            skill_name="runtime",
            action_name=action_name,
            intent_goal=intent_goal,
            action_payload=payload,
            context_assembled=context_assembled,
            retrieval_queries=None,
            judge_verdict=judge_verdict,
            judge_reasoning=judge_reasoning,
            training_eligible=training_eligible,
            retention_class=retention_class,
            outcome_score=outcome_score,
            created_at=created_at,
        )
