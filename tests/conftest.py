from __future__ import annotations

import sqlite3
from pathlib import Path
import tempfile

import pytest

from immune.types import ImmuneConfig, JudgePayload, SheriffPayload, generate_uuid_v7


@pytest.fixture
def default_config() -> ImmuneConfig:
    return ImmuneConfig(known_tool_registry=frozenset({"safe_tool", "web_fetch", "shell_command"}))


@pytest.fixture
def clean_sheriff_payload() -> SheriffPayload:
    return SheriffPayload(
        session_id=generate_uuid_v7(),
        skill_name="immune_system",
        tool_name="safe_tool",
        arguments={"query": "hello"},
        raw_prompt="hello",
        source_trust_tier=4,
        jwt_claims={"max_tool_calls": 5, "current_tool_calls": 1},
    )


@pytest.fixture
def clean_judge_payload(clean_sheriff_payload: SheriffPayload) -> JudgePayload:
    return JudgePayload(
        session_id=clean_sheriff_payload.session_id,
        skill_name=clean_sheriff_payload.skill_name,
        tool_name=clean_sheriff_payload.tool_name,
        output={"ok": True, "claimed_trust_tier": 4},
        expected_schema={
            "type": "object",
            "required": ["ok"],
            "properties": {
                "ok": {"type": "boolean"},
                "claimed_trust_tier": {"type": "integer"},
            },
        },
    )


@pytest.fixture
def test_db() -> str:
    fd, path = tempfile.mkstemp(suffix=".db")
    Path(path).unlink(missing_ok=True)
    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE immune_verdicts (
            verdict_id TEXT, check_type TEXT, tier TEXT, skill_name TEXT,
            session_id TEXT, outcome TEXT, block_reason TEXT, block_detail TEXT,
            latency_ms REAL, alert_severity TEXT, created_at TEXT
        );
        CREATE TABLE skill_bypass_log (
            event_id TEXT, skill_name TEXT, session_id TEXT,
            bypass_type TEXT, details TEXT, created_at TEXT
        );
        CREATE TABLE canary_audits (id TEXT);
        CREATE TABLE circuit_breakers (id TEXT);
        CREATE TABLE circuit_breaker_events (id TEXT);
        """
    )
    conn.commit()
    conn.close()
    yield path


@pytest.fixture
def mock_dispatch():
    def _dispatch(*args, **kwargs):
        return {"ok": True, "claimed_trust_tier": kwargs.get("claimed_trust_tier", 4)}

    return _dispatch
