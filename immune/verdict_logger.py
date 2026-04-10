from __future__ import annotations

import sqlite3
import threading
import time

from immune.types import ImmuneConfig, ImmuneVerdict

REQUIRED_TABLES = {
    "immune_verdicts",
    "skill_bypass_log",
    "canary_audits",
    "circuit_breakers",
    "circuit_breaker_events",
}


class VerdictLogger:
    """Thread-safe buffered SQLite logger for immune verdicts."""

    def __init__(self, db_path: str, config: ImmuneConfig):
        self._db_path = db_path
        self._config = config
        self._buffer: list[ImmuneVerdict] = []
        self._lock = threading.Lock()
        self._closed = False
        self._conn = sqlite3.connect(self._db_path, check_same_thread=False)
        self._verify_tables()
        self._timer: threading.Timer | None = None
        if not self._closed:
            self._schedule_flush()

    def _verify_tables(self) -> None:
        cur = self._conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {r[0] for r in cur.fetchall()}
        missing = REQUIRED_TABLES - tables
        if missing:
            raise RuntimeError(f"Missing required immune tables: {sorted(missing)}")

    def _schedule_flush(self) -> None:
        if self._closed:
            return
        self._timer = threading.Timer(self._config.verdict_flush_interval_ms / 1000.0, self.flush)
        self._timer.daemon = True
        self._timer.start()

    def log_verdict(self, verdict: ImmuneVerdict) -> None:
        """Append verdict to buffer and trigger flush when full."""
        with self._lock:
            self._buffer.append(verdict)
            should_flush = len(self._buffer) >= self._config.verdict_buffer_size
        if should_flush:
            t = threading.Thread(target=self.flush, daemon=True)
            t.start()

    def log_bypass(self, skill_name: str, session_id: str, bypass_type: str, details: str) -> None:
        """Write bypass forensic row."""
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with self._lock:
            self._conn.execute(
                "INSERT INTO skill_bypass_log "
                "(event_id, skill_name, session_id, bypass_type, details, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (f"bypass-{time.time_ns()}", skill_name, session_id, bypass_type, details, now),
            )
            self._conn.commit()

    def flush(self) -> None:
        """Flush buffered verdicts as batch insert."""
        with self._lock:
            if not self._buffer:
                if not self._closed:
                    self._schedule_flush()
                return
            rows = self._buffer[:]
            self._buffer.clear()
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        values = [
            (
                v.verdict_id,
                v.check_type.value,
                v.tier.value,
                v.skill_name,
                v.session_id,
                v.outcome.value,
                v.block_reason.value if v.block_reason else None,
                v.block_detail,
                v.latency_ms,
                v.alert_severity.value if v.alert_severity else None,
                now,
            )
            for v in rows
        ]
        with self._lock:
            self._conn.executemany(
                "INSERT INTO immune_verdicts "
                "(verdict_id, check_type, tier, skill_name, session_id, outcome, block_reason, "
                "block_detail, latency_ms, alert_severity, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                values,
            )
            self._conn.commit()
        self._schedule_flush()

    def shutdown(self) -> None:
        """Stop timer and flush all pending data."""
        self._closed = True
        if self._timer:
            self._timer.cancel()
        self.flush()
        with self._lock:
            self._conn.close()


if __name__ == "__main__":
    print("ok")
