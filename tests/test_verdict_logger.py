from __future__ import annotations

import sqlite3
import threading
import tempfile
import logging

import pytest

from immune.types import CheckType, ImmuneVerdict, Outcome, Tier, generate_uuid_v7
from immune.verdict_logger import VerdictLogger


def _verdict() -> ImmuneVerdict:
    return ImmuneVerdict(
        verdict_id=generate_uuid_v7(),
        check_type=CheckType.SHERIFF,
        tier=Tier.FAST_PATH,
        skill_name="immune_system",
        session_id=generate_uuid_v7(),
        outcome=Outcome.PASS,
        latency_ms=1.0,
    )


def test_missing_table_raises(default_config):
    fd, path = tempfile.mkstemp(suffix=".db")
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE immune_verdicts (verdict_id TEXT, verdict_type TEXT, scan_tier TEXT, session_id TEXT, skill_name TEXT, result TEXT, match_pattern TEXT, latency_ms INTEGER, judge_mode TEXT, timestamp TEXT)"
    )
    conn.commit()
    conn.close()
    with pytest.raises(RuntimeError):
        VerdictLogger(path, default_config)


def test_log_adds_buffer(default_config, test_db):
    logger = VerdictLogger(test_db, default_config)
    logger.log_verdict(_verdict())
    assert len(logger._buffer) == 1


def test_capacity_triggers_flush(default_config, test_db):
    cfg = default_config.__class__(**{**default_config.__dict__, "verdict_buffer_size": 2})
    logger = VerdictLogger(test_db, cfg)
    logger.log_verdict(_verdict())
    logger.log_verdict(_verdict())
    logger.flush()
    c = sqlite3.connect(test_db)
    n = c.execute("select count(*) from immune_verdicts").fetchone()[0]
    assert n >= 2


def test_flush_writes_batch(default_config, test_db):
    logger = VerdictLogger(test_db, default_config)
    logger.log_verdict(_verdict())
    logger.flush()
    c = sqlite3.connect(test_db)
    n = c.execute("select count(*) from immune_verdicts").fetchone()[0]
    assert n == 1


def test_shutdown_flushes(default_config, test_db):
    logger = VerdictLogger(test_db, default_config)
    logger.log_verdict(_verdict())
    logger.shutdown()
    c = sqlite3.connect(test_db)
    n = c.execute("select count(*) from immune_verdicts").fetchone()[0]
    assert n == 1


def test_thread_safety(default_config, test_db):
    logger = VerdictLogger(test_db, default_config)

    def work():
        logger.log_verdict(_verdict())

    threads = [threading.Thread(target=work) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    logger.flush()
    c = sqlite3.connect(test_db)
    n = c.execute("select count(*) from immune_verdicts").fetchone()[0]
    assert n == 10


def test_log_bypass(default_config, test_db):
    logger = VerdictLogger(test_db, default_config)
    logger.log_bypass("immune_system", "s", "direct", "d")
    c = sqlite3.connect(test_db)
    n = c.execute("select count(*) from security_alerts where source = 'skill_bypass'").fetchone()[0]
    assert n == 1


def test_flush_logs_warning_on_failure(default_config, test_db, caplog):
    logger = VerdictLogger(test_db, default_config)
    logger.log_verdict(_verdict())
    logger._closed = True
    if logger._timer:
        logger._timer.cancel()
    logger._conn.close()
    with caplog.at_level(logging.WARNING):
        logger.flush()
    assert "Verdict flush failure #1" in caplog.text


def test_flush_raises_after_three_failures(default_config, test_db):
    logger = VerdictLogger(test_db, default_config)
    logger.log_verdict(_verdict())
    logger._closed = True
    if logger._timer:
        logger._timer.cancel()
    logger._conn.close()
    logger.flush()
    logger.flush()
    with pytest.raises(RuntimeError, match="Circuit breaker"):
        logger.flush()
