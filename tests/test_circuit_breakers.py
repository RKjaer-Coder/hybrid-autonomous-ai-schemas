from __future__ import annotations

import sqlite3
from pathlib import Path

from immune.circuit_breakers import CircuitBreakerEvent, CircuitBreakerLogger, resolve_compound_breaker
from immune.types import CircuitBreakerState


def _init_immune_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "immune_system.db"
    schema = Path("schemas/immune_system.sql").read_text(encoding="utf-8")
    with sqlite3.connect(db_path) as conn:
        conn.executescript(schema)
        conn.commit()
    return db_path


def test_resolve_compound_breaker_prefers_highest_tier_and_suppresses_lower_actions():
    resolution = resolve_compound_breaker(
        [
            CircuitBreakerEvent(
                breaker_name="CONTEXT_OVERFLOW",
                state="TRIPPED",
                trip_condition="context >95%",
                action_taken="FORCE_SUMMARISE",
                requires_human=False,
                auto_reset_at=None,
                timestamp="2026-04-15T10:00:00+00:00",
            ),
            CircuitBreakerEvent(
                breaker_name="SECURITY_CASCADE",
                state="TRIPPED",
                trip_condition="3 security alerts",
                action_taken="FULL_SYSTEM_HALT",
                requires_human=True,
                auto_reset_at=None,
                timestamp="2026-04-15T10:00:20+00:00",
            ),
            CircuitBreakerEvent(
                breaker_name="TOOL_FAILURE_STORM",
                state="TRIPPED",
                trip_condition="30% failures",
                action_taken="HALT_EXECUTION",
                requires_human=False,
                auto_reset_at=None,
                timestamp="2026-04-15T10:00:40+00:00",
            ),
        ]
    )

    assert resolution is not None
    assert resolution.breaker_names == ["CONTEXT_OVERFLOW", "SECURITY_CASCADE", "TOOL_FAILURE_STORM"]
    assert resolution.winner_tier == "S"
    assert resolution.winning_action == "FULL_SYSTEM_HALT"
    assert resolution.applied_actions == ["FULL_SYSTEM_HALT"]
    assert resolution.suppressed_actions == ["FORCE_SUMMARISE", "HALT_EXECUTION"]
    assert resolution.requires_human is True


def test_circuit_breaker_logger_persists_and_updates_compound_event(tmp_path: Path):
    db_path = _init_immune_db(tmp_path)
    logger = CircuitBreakerLogger(str(db_path))

    _, compound_a = logger.log_breaker(
        "TOOL_FAILURE_STORM",
        CircuitBreakerState.TRIPPED.value,
        "storm",
        "HALT_EXECUTION",
        False,
        timestamp="2026-04-15T10:00:00+00:00",
    )
    _, compound_b = logger.log_breaker(
        "SECURITY_CASCADE",
        CircuitBreakerState.TRIPPED.value,
        "cascade",
        "FULL_SYSTEM_HALT",
        True,
        timestamp="2026-04-15T10:00:20+00:00",
    )
    _, compound_c = logger.log_breaker(
        "CONTEXT_OVERFLOW",
        CircuitBreakerState.TRIPPED.value,
        "overflow",
        "FORCE_SUMMARISE",
        False,
        timestamp="2026-04-15T10:00:45+00:00",
    )

    assert compound_a is None
    assert compound_b is not None
    assert compound_c == compound_b

    compounds = logger.recent_compound_events(limit=5, unresolved_only=True)
    assert len(compounds) == 1
    assert compounds[0]["breaker_names"] == ["CONTEXT_OVERFLOW", "SECURITY_CASCADE", "TOOL_FAILURE_STORM"]
    assert compounds[0]["winner_tier"] == "S"
    assert compounds[0]["winning_action"] == "FULL_SYSTEM_HALT"
    assert compounds[0]["suppressed_actions"] == ["FORCE_SUMMARISE", "HALT_EXECUTION"]

    breakers = logger.recent_breakers(limit=5, state=CircuitBreakerState.TRIPPED.value)
    assert [row["breaker_name"] for row in breakers] == [
        "CONTEXT_OVERFLOW",
        "SECURITY_CASCADE",
        "TOOL_FAILURE_STORM",
    ]
