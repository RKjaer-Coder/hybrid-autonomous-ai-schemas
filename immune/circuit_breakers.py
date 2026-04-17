from __future__ import annotations

import datetime
import json
import sqlite3
import uuid
from dataclasses import dataclass
from typing import Iterable, Sequence

from immune.types import CircuitBreakerState

_BREAKER_TIERS = {
    "SECURITY_CASCADE": "S",
    "MEMORY_WRITE_STORM": "S",
    "BUDGET_HARD_CAP": "H",
    "RELIABILITY_CRITICAL": "H",
    "TOOL_FAILURE_STORM": "H",
    "CONTEXT_OVERFLOW": "D",
    "RELIABILITY_DEGRADED": "D",
    "EXECUTOR_SATURATION": "D",
    "TOOL_QUARANTINE": "D",
    "JUDGE_DEADLOCK": "D",
    "DEAD_MAN_S_SWITCH": "R",
    "INFINITE_LOOP": "R",
}
_TIER_RANK = {"S": 0, "H": 1, "D": 2, "R": 3}


def _normalize_timestamp(timestamp: str | None) -> str:
    if timestamp:
        return timestamp
    return datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()


def _parse_timestamp(timestamp: str) -> datetime.datetime:
    normalized = timestamp.replace("Z", "+00:00")
    parsed = datetime.datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=datetime.timezone.utc)
    return parsed.astimezone(datetime.timezone.utc)


@dataclass(frozen=True)
class CircuitBreakerEvent:
    breaker_name: str
    state: str
    trip_condition: str
    action_taken: str
    requires_human: bool
    auto_reset_at: str | None
    timestamp: str

    @classmethod
    def from_row(cls, row: sqlite3.Row | dict) -> "CircuitBreakerEvent":
        return cls(
            breaker_name=row["breaker_name"],
            state=row["state"],
            trip_condition=row["trip_condition"],
            action_taken=row["action_taken"],
            requires_human=bool(row["requires_human"]),
            auto_reset_at=row["auto_reset_at"],
            timestamp=row["timestamp"],
        )


@dataclass(frozen=True)
class CompoundBreakerResolution:
    breaker_names: list[str]
    winner_tier: str
    winning_action: str
    applied_actions: list[str]
    suppressed_actions: list[str]
    requires_human: bool
    window_seconds: int
    window_started_at: str
    window_ended_at: str


def resolve_compound_breaker(
    events: Sequence[CircuitBreakerEvent],
    *,
    window_seconds: int = 60,
) -> CompoundBreakerResolution | None:
    tripped_by_breaker: dict[str, CircuitBreakerEvent] = {}
    for event in events:
        if event.state != CircuitBreakerState.TRIPPED.value:
            continue
        existing = tripped_by_breaker.get(event.breaker_name)
        if existing is None or _parse_timestamp(event.timestamp) >= _parse_timestamp(existing.timestamp):
            tripped_by_breaker[event.breaker_name] = event
    if len(tripped_by_breaker) < 2:
        return None

    deduped = list(tripped_by_breaker.values())
    winner_tier = min((_BREAKER_TIERS.get(event.breaker_name, "R") for event in deduped), key=_TIER_RANK.__getitem__)
    winners = [event for event in deduped if _BREAKER_TIERS.get(event.breaker_name, "R") == winner_tier]
    suppressed = [event for event in deduped if _BREAKER_TIERS.get(event.breaker_name, "R") != winner_tier]
    applied_actions = sorted({event.action_taken for event in winners})
    suppressed_actions = sorted({event.action_taken for event in suppressed})
    timestamps = sorted(_parse_timestamp(event.timestamp) for event in deduped)
    return CompoundBreakerResolution(
        breaker_names=sorted(tripped_by_breaker),
        winner_tier=winner_tier,
        winning_action=applied_actions[0] if len(applied_actions) == 1 else "COMPOSED[" + "+".join(applied_actions) + "]",
        applied_actions=applied_actions,
        suppressed_actions=suppressed_actions,
        requires_human=any(event.requires_human for event in deduped),
        window_seconds=window_seconds,
        window_started_at=timestamps[0].replace(microsecond=0).isoformat(),
        window_ended_at=timestamps[-1].replace(microsecond=0).isoformat(),
    )


class CircuitBreakerLogger:
    def __init__(self, db_path: str):
        self._db_path = db_path

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path, timeout=5.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=5000;")
        return conn

    def log_breaker(
        self,
        breaker_name: str,
        state: str,
        trip_condition: str,
        action_taken: str,
        requires_human: bool,
        *,
        auto_reset_at: str | None = None,
        timestamp: str | None = None,
        window_seconds: int = 60,
    ) -> tuple[str, str | None]:
        event_id = str(uuid.uuid4())
        ts = _normalize_timestamp(timestamp)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO circuit_breaker_log (
                    event_id, breaker_name, state, trip_condition, action_taken,
                    requires_human, auto_reset_at, timestamp
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event_id,
                    breaker_name,
                    state,
                    trip_condition,
                    action_taken,
                    1 if requires_human else 0,
                    auto_reset_at,
                    ts,
                ),
            )
            compound_event_id = None
            if state == CircuitBreakerState.TRIPPED.value:
                compound_event_id = self._refresh_recent_compound_event(
                    conn,
                    anchor_timestamp=ts,
                    window_seconds=window_seconds,
                )
            conn.commit()
        return event_id, compound_event_id

    def recent_breakers(
        self,
        *,
        limit: int = 20,
        breaker_name: str | None = None,
        state: str | None = None,
    ) -> list[dict]:
        where: list[str] = []
        params: list[object] = []
        if breaker_name:
            where.append("breaker_name = ?")
            params.append(breaker_name)
        if state:
            where.append("state = ?")
            params.append(state)
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM circuit_breaker_log {where_sql} ORDER BY timestamp DESC LIMIT ?",
                (*params, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def recent_compound_events(
        self,
        *,
        limit: int = 10,
        unresolved_only: bool = False,
    ) -> list[dict]:
        where_sql = "WHERE resolved_at IS NULL" if unresolved_only else ""
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM compound_breaker_events {where_sql} ORDER BY created_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [_compound_row_to_dict(row) for row in rows]

    def _refresh_recent_compound_event(
        self,
        conn: sqlite3.Connection,
        *,
        anchor_timestamp: str,
        window_seconds: int,
    ) -> str | None:
        window_end = _parse_timestamp(anchor_timestamp)
        window_start = window_end - datetime.timedelta(seconds=window_seconds)
        rows = conn.execute(
            """
            SELECT breaker_name, state, trip_condition, action_taken, requires_human, auto_reset_at, timestamp
            FROM circuit_breaker_log
            WHERE timestamp >= ? AND timestamp <= ? AND state = ?
            ORDER BY timestamp ASC
            """,
            (
                window_start.replace(microsecond=0).isoformat(),
                window_end.replace(microsecond=0).isoformat(),
                CircuitBreakerState.TRIPPED.value,
            ),
        ).fetchall()
        resolution = resolve_compound_breaker(
            [CircuitBreakerEvent.from_row(row) for row in rows],
            window_seconds=window_seconds,
        )
        if resolution is None:
            return None
        return self._upsert_compound_event(conn, resolution)

    def _upsert_compound_event(
        self,
        conn: sqlite3.Connection,
        resolution: CompoundBreakerResolution,
    ) -> str:
        breaker_names_json = json.dumps(resolution.breaker_names, separators=(",", ":"))
        applied_actions_json = json.dumps(resolution.applied_actions, separators=(",", ":"))
        suppressed_actions_json = json.dumps(resolution.suppressed_actions, separators=(",", ":"))
        existing = conn.execute(
            """
            SELECT event_id, breaker_names
            FROM compound_breaker_events
            WHERE window_ended_at >= ? AND window_started_at <= ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (resolution.window_started_at, resolution.window_ended_at),
        ).fetchone()
        if existing is not None:
            existing_breakers = set(json.loads(existing["breaker_names"]))
            if existing_breakers.issubset(set(resolution.breaker_names)) or set(resolution.breaker_names).issubset(existing_breakers):
                conn.execute(
                    """
                    UPDATE compound_breaker_events
                    SET breaker_names = ?,
                        winner_tier = ?,
                        winning_action = ?,
                        applied_actions = ?,
                        suppressed_actions = ?,
                        requires_human = ?,
                        window_seconds = ?,
                        window_started_at = ?,
                        window_ended_at = ?
                    WHERE event_id = ?
                    """,
                    (
                        breaker_names_json,
                        resolution.winner_tier,
                        resolution.winning_action,
                        applied_actions_json,
                        suppressed_actions_json,
                        1 if resolution.requires_human else 0,
                        resolution.window_seconds,
                        resolution.window_started_at,
                        resolution.window_ended_at,
                        existing["event_id"],
                    ),
                )
                return str(existing["event_id"])
        event_id = str(uuid.uuid4())
        conn.execute(
            """
            INSERT INTO compound_breaker_events (
                event_id, breaker_names, winner_tier, winning_action,
                applied_actions, suppressed_actions, requires_human,
                window_seconds, window_started_at, window_ended_at,
                resolution_notes, resolved_at, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                breaker_names_json,
                resolution.winner_tier,
                resolution.winning_action,
                applied_actions_json,
                suppressed_actions_json,
                1 if resolution.requires_human else 0,
                resolution.window_seconds,
                resolution.window_started_at,
                resolution.window_ended_at,
                None,
                None,
                resolution.window_ended_at,
            ),
        )
        return event_id


def _compound_row_to_dict(row: sqlite3.Row) -> dict:
    return {
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
