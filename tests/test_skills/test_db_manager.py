from __future__ import annotations

import threading

from skills.db_manager import DatabaseManager


def test_connection_creates_wal_mode(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    conn = db.get_connection("immune")
    mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    assert str(mode).lower() == "wal"


def test_connection_reuse_same_thread(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    c1 = db.get_connection("telemetry")
    c2 = db.get_connection("telemetry")
    assert c1 is c2


def test_connection_different_threads(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    main_conn = db.get_connection("telemetry")
    out = {}

    def worker():
        out["conn"] = db.get_connection("telemetry")

    t = threading.Thread(target=worker)
    t.start()
    t.join()
    assert out["conn"] is not main_conn


def test_missing_database_raises(tmp_path):
    db = DatabaseManager(str(tmp_path))
    try:
        db.get_connection("immune")
        raise AssertionError("expected FileNotFoundError")
    except FileNotFoundError:
        pass


def test_verify_all_databases_returns_status(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    status = db.verify_all_databases()
    assert set(status) == {"strategic_memory", "immune", "telemetry", "financial_ledger", "operator_digest"}
    assert all(status.values())
