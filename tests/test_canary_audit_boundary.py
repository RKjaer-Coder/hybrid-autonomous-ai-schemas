from __future__ import annotations

import sqlite3

from immune.types import CheckType, ImmuneVerdict, Outcome, Tier, generate_uuid_v7
from immune.verdict_logger import VerdictLogger


def test_verdict_logger_does_not_write_canary_audits(default_config, test_db):
    logger = VerdictLogger(test_db, default_config)
    verdict = ImmuneVerdict(
        verdict_id=generate_uuid_v7(),
        check_type=CheckType.SHERIFF,
        tier=Tier.FAST_PATH,
        skill_name="immune_system",
        session_id=generate_uuid_v7(),
        outcome=Outcome.PASS,
        latency_ms=0.1,
    )
    logger.log_verdict(verdict)
    logger.flush()

    conn = sqlite3.connect(test_db)
    try:
        canary_count = conn.execute("SELECT COUNT(*) FROM canary_audits").fetchone()[0]
        verdict_count = conn.execute("SELECT COUNT(*) FROM immune_verdicts").fetchone()[0]
    finally:
        conn.close()
        logger.shutdown()

    assert verdict_count >= 1
    assert canary_count == 0
