from __future__ import annotations

import logging
import sqlite3
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Callable, Optional, Tuple

logger = logging.getLogger("hybrid_ai.append_buffer")


@dataclass
class BufferConfig:
    db_name: str
    table_name: str
    max_entries: int
    flush_interval_ms: int
    columns: Tuple[str, ...]


TELEMETRY_BUFFER_CONFIG = BufferConfig(
    db_name="telemetry",
    table_name="step_outcomes",
    max_entries=512,
    flush_interval_ms=500,
    columns=("event_id", "step_type", "skill", "chain_id", "outcome", "latency_ms", "quality_warning", "recovery_tier", "timestamp"),
)

IMMUNE_BUFFER_CONFIG = BufferConfig(
    db_name="immune",
    table_name="immune_verdicts",
    max_entries=64,
    flush_interval_ms=100,
    columns=("verdict_id", "verdict_type", "scan_tier", "session_id", "skill_name", "result", "match_pattern", "latency_ms", "timestamp"),
)


class AppendBuffer:
    def __init__(self, config: BufferConfig, get_connection: Callable[[], sqlite3.Connection]):
        self._config = config
        self._get_connection = get_connection
        self._buffer: deque = deque(maxlen=config.max_entries)
        self._lock = threading.Lock()
        self._dropped_count = 0
        self._dropped_since_flush = 0
        self._flush_count = 0
        self._running = False
        self._flush_thread: Optional[threading.Thread] = None

    def start(self):
        self._running = True
        self._flush_thread = threading.Thread(target=self._flush_loop, daemon=True, name=f"flush-{self._config.db_name}")
        self._flush_thread.start()

    def stop(self):
        self._running = False
        if self._flush_thread:
            self._flush_thread.join(timeout=5.0)
        self._flush_now()

    def append(self, row: tuple):
        with self._lock:
            was_full = len(self._buffer) >= self._config.max_entries
            self._buffer.append(row)
            if was_full:
                self._dropped_count += 1
                self._dropped_since_flush += 1
        if len(self._buffer) >= self._config.max_entries // 2:
            self._flush_now()

    def _flush_loop(self):
        interval_s = self._config.flush_interval_ms / 1000.0
        while self._running:
            time.sleep(interval_s)
            self._flush_now()

    def _flush_now(self):
        with self._lock:
            if not self._buffer:
                return
            rows = list(self._buffer)
            self._buffer.clear()

        try:
            conn = self._get_connection()
            placeholders = ",".join(["?"] * len(self._config.columns))
            col_names = ",".join(self._config.columns)
            sql = f"INSERT OR IGNORE INTO {self._config.table_name} ({col_names}) VALUES ({placeholders})"
            conn.executemany(sql, rows)
            conn.commit()
            self._flush_count += 1
            with self._lock:
                dropped_since_flush = self._dropped_since_flush
                self._dropped_since_flush = 0
            if dropped_since_flush:
                logger.warning(
                    "TELEMETRY_BACKPRESSURE: %s entries dropped before flushing %s",
                    dropped_since_flush,
                    self._config.db_name,
                )
        except Exception as e:  # noqa: BLE001
            logger.error("Flush failed for %s: %s", self._config.db_name, e)
            with self._lock:
                for row in reversed(rows):
                    if len(self._buffer) < self._config.max_entries:
                        self._buffer.appendleft(row)
                    else:
                        self._dropped_count += 1
                        self._dropped_since_flush += 1

    @property
    def stats(self) -> dict:
        with self._lock:
            return {
                "db_name": self._config.db_name,
                "current_size": len(self._buffer),
                "max_size": self._config.max_entries,
                "dropped_count": self._dropped_count,
                "flush_count": self._flush_count,
                "fill_pct": len(self._buffer) / self._config.max_entries * 100,
            }
