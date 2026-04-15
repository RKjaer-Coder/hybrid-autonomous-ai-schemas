from __future__ import annotations

import sqlite3
import threading
import time
import uuid
import logging

from skills.append_buffer import AppendBuffer, BufferConfig


def _mk_conn(tmp_path):
    p = tmp_path / "buf.db"
    conn = sqlite3.connect(str(p), check_same_thread=False)
    conn.execute("CREATE TABLE IF NOT EXISTS t (id TEXT PRIMARY KEY, v TEXT)")
    conn.commit()
    return conn


def test_append_adds_row(tmp_path):
    conn = _mk_conn(tmp_path)
    b = AppendBuffer(BufferConfig("x", "t", 4, 1000, ("id", "v")), lambda: conn)
    b.append(("1", "a"))
    assert b.stats["current_size"] == 1


def test_flush_writes_rows(tmp_path):
    conn = _mk_conn(tmp_path)
    b = AppendBuffer(BufferConfig("x", "t", 4, 1000, ("id", "v")), lambda: conn)
    b.append(("1", "a"))
    b._flush_now()
    n = conn.execute("SELECT COUNT(*) FROM t").fetchone()[0]
    assert n == 1


def test_evicts_oldest_when_full(tmp_path):
    conn = _mk_conn(tmp_path)
    b = AppendBuffer(BufferConfig("x", "t", 2, 1000, ("id", "v")), lambda: conn)
    b._flush_now = lambda: None
    b.append(("1", "a"))
    b.append(("2", "b"))
    b.append(("3", "c"))
    assert b.stats["dropped_count"] >= 1


def test_half_capacity_flush_trigger(tmp_path):
    conn = _mk_conn(tmp_path)
    b = AppendBuffer(BufferConfig("x", "t", 4, 1000, ("id", "v")), lambda: conn)
    b.append(("1", "a"))
    b.append(("2", "b"))
    n = conn.execute("SELECT COUNT(*) FROM t").fetchone()[0]
    assert n == 2


def test_flush_interval_triggers(tmp_path):
    conn = _mk_conn(tmp_path)
    b = AppendBuffer(BufferConfig("x", "t", 10, 30, ("id", "v")), lambda: conn)
    b.start()
    b.append(("1", "a"))
    time.sleep(0.08)
    b.stop()
    n = conn.execute("SELECT COUNT(*) FROM t").fetchone()[0]
    assert n >= 1


def test_stop_flushes_remaining(tmp_path):
    conn = _mk_conn(tmp_path)
    b = AppendBuffer(BufferConfig("x", "t", 10, 10000, ("id", "v")), lambda: conn)
    b.start()
    b.append(("1", "a"))
    b.stop()
    n = conn.execute("SELECT COUNT(*) FROM t").fetchone()[0]
    assert n == 1


def test_requeue_on_flush_failure(tmp_path):
    conn = _mk_conn(tmp_path)
    b = AppendBuffer(BufferConfig("x", "missing", 10, 1000, ("id", "v")), lambda: conn)
    b.append(("1", "a"))
    b._flush_now()
    assert b.stats["current_size"] == 1


def test_stats_reflect_state(tmp_path):
    conn = _mk_conn(tmp_path)
    b = AppendBuffer(BufferConfig("x", "t", 4, 1000, ("id", "v")), lambda: conn)
    b.append(("1", "a"))
    s = b.stats
    assert s["max_size"] == 4
    assert s["current_size"] == 1


def test_concurrent_appends(tmp_path):
    conn = _mk_conn(tmp_path)
    b = AppendBuffer(BufferConfig("x", "t", 500, 10000, ("id", "v")), lambda: conn)

    def worker(n):
        for i in range(n):
            b.append((str(uuid.uuid4()), str(i)))

    threads = [threading.Thread(target=worker, args=(50,)) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert b.stats["current_size"] <= 500


def test_flush_count_increments(tmp_path):
    conn = _mk_conn(tmp_path)
    b = AppendBuffer(BufferConfig("x", "t", 4, 1000, ("id", "v")), lambda: conn)
    b.append(("1", "a"))
    b._flush_now()
    assert b.stats["flush_count"] == 1


def test_fill_pct(tmp_path):
    conn = _mk_conn(tmp_path)
    b = AppendBuffer(BufferConfig("x", "t", 10, 1000, ("id", "v")), lambda: conn)
    for i in range(5):
        b.append((str(i), "x"))
    assert b.stats["fill_pct"] <= 100


def test_backpressure_warning_logged_on_flush(tmp_path, caplog):
    conn = _mk_conn(tmp_path)
    b = AppendBuffer(BufferConfig("x", "t", 2, 1000, ("id", "v")), lambda: conn)
    b._flush_now = lambda: None
    b.append(("1", "a"))
    b.append(("2", "b"))
    b.append(("3", "c"))
    b._flush_now = AppendBuffer._flush_now.__get__(b, AppendBuffer)
    with caplog.at_level(logging.WARNING):
        b._flush_now()
    assert "TELEMETRY_BACKPRESSURE" in caplog.text
