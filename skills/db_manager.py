from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Dict


@dataclass(frozen=True)
class DatabaseConfig:
    name: str
    filename: str
    wal_mode: bool = True
    journal_size_limit: int = 67_108_864
    cache_size_pages: int = 2000
    busy_timeout_ms: int = 5000
    foreign_keys: bool = True


DATABASE_CONFIGS: Dict[str, DatabaseConfig] = {
    "kernel": DatabaseConfig("kernel", "kernel.db"),
    "strategic_memory": DatabaseConfig("strategic_memory", "strategic_memory.db"),
    "immune": DatabaseConfig("immune", "immune_system.db"),
    "immune_system": DatabaseConfig("immune_system", "immune_system.db"),
    "telemetry": DatabaseConfig("telemetry", "telemetry.db"),
    "financial_ledger": DatabaseConfig("financial_ledger", "financial_ledger.db"),
    "operator_digest": DatabaseConfig("operator_digest", "operator_digest.db"),
    # Compatibility aliases retained for code that still thinks in legacy module slices.
    "opportunity": DatabaseConfig("opportunity", "strategic_memory.db"),
    "project": DatabaseConfig("project", "financial_ledger.db"),
}

CANONICAL_DATABASES = (
    "strategic_memory",
    "immune",
    "telemetry",
    "financial_ledger",
    "operator_digest",
)


class DatabaseManager:
    def __init__(self, data_dir: str):
        self._data_dir = Path(data_dir)
        self._local = threading.local()

    @property
    def data_dir(self) -> Path:
        return self._data_dir

    def get_connection(self, db_name: str) -> sqlite3.Connection:
        if db_name not in DATABASE_CONFIGS:
            raise KeyError(f"Unknown database: {db_name}")
        thread_key = f"conn_{db_name}"
        conn = getattr(self._local, thread_key, None)
        if conn is None:
            config = DATABASE_CONFIGS[db_name]
            db_path = self._data_dir / config.filename
            if not db_path.exists():
                raise FileNotFoundError(f"Database {db_path} not found. Run migrate.py first.")
            conn = sqlite3.connect(str(db_path), timeout=config.busy_timeout_ms / 1000)
            conn.row_factory = sqlite3.Row
            if config.wal_mode:
                conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(f"PRAGMA journal_size_limit={config.journal_size_limit}")
            conn.execute(f"PRAGMA cache_size=-{config.cache_size_pages}")
            conn.execute(f"PRAGMA busy_timeout={config.busy_timeout_ms}")
            if config.foreign_keys:
                conn.execute("PRAGMA foreign_keys=ON")
            setattr(self._local, thread_key, conn)
        return conn

    def close_all(self):
        for db_name in DATABASE_CONFIGS:
            thread_key = f"conn_{db_name}"
            conn = getattr(self._local, thread_key, None)
            if conn is not None:
                conn.close()
                setattr(self._local, thread_key, None)

    def verify_all_databases(self) -> Dict[str, bool]:
        results: Dict[str, bool] = {}
        for db_name in CANONICAL_DATABASES:
            try:
                conn = self.get_connection(db_name)
                mode = str(conn.execute("PRAGMA journal_mode").fetchone()[0]).lower()
                results[db_name] = mode == "wal"
            except Exception:
                results[db_name] = False
        return results
