#!/usr/bin/env python3
"""Create/verify Hybrid Autonomous AI SQLite schema suite."""

from __future__ import annotations

import argparse
import hashlib
import sqlite3
from pathlib import Path

SCHEMAS = {
    "strategic_memory": "schemas/strategic_memory.sql",
    "telemetry": "schemas/telemetry.sql",
    "immune_system": "schemas/immune_system.sql",
    "financial_ledger": "schemas/financial_ledger.sql",
    "operator_digest": "schemas/operator_digest.sql",
}

EXPECTED_OBJECTS = {
    "strategic_memory": {
        "tables": {
            "opportunity_records", "council_verdicts", "brief_quality_signals", "calibration_records",
            "intelligence_briefs", "research_tasks", "idea_records", "market_signals", "capability_gaps",
            "source_reputations", "dedup_records", "deferred_research_entries", "model_scout_reports",
            "model_assess_reports", "shadow_trial_reports",
        },
        "indexes": {
            "idx_opportunity_records_status", "idx_opportunity_records_income_created", "idx_opportunity_records_project_id",
            "idx_council_verdicts_decision_created", "idx_council_verdicts_project_id", "idx_council_verdicts_tier_used",
            "idx_brief_quality_signals_brief_id", "idx_brief_quality_signals_verdict_id", "idx_brief_quality_signals_signal_created",
            "idx_calibration_records_decision_created", "idx_intelligence_briefs_domain_created", "idx_intelligence_briefs_actionability",
            "idx_intelligence_briefs_tags", "idx_intelligence_briefs_task_id", "idx_research_tasks_domain_priority_status",
            "idx_research_tasks_status_stale_after",
        },
    },
    "telemetry": {
        "tables": {"step_outcomes", "chain_definitions"},
        "indexes": {
            "idx_step_outcomes_step_skill_timestamp", "idx_step_outcomes_chain_id",
            "idx_step_outcomes_outcome_timestamp", "idx_step_outcomes_skill_timestamp",
        },
    },
    "immune_system": {
        "tables": {
            "immune_verdicts",
            "security_alerts",
            "circuit_breaker_log",
            "compound_breaker_events",
            "quarantined_responses",
            "judge_fallback_events",
            "judge_fallback_review_queue",
            "jwt_revocation_log",
            "skill_improvement_log",
        },
        "indexes": {
            "idx_immune_verdicts_skill_timestamp", "idx_immune_verdicts_result_timestamp", "idx_immune_verdicts_session_id",
            "idx_immune_verdicts_judge_mode_timestamp",
            "idx_security_alerts_timestamp",
            "idx_circuit_breaker_name_timestamp",
            "idx_circuit_breaker_state",
            "idx_compound_breaker_events_created",
            "idx_compound_breaker_events_winner_tier",
            "idx_quarantined_responses_correlation_id",
            "idx_quarantined_responses_review_status",
            "idx_quarantined_responses_quarantined_at",
            "idx_judge_fallback_events_status_started",
            "idx_judge_fallback_events_started_at",
            "idx_judge_fallback_review_status_enqueued",
            "idx_judge_fallback_review_event_status",
        },
    },
    "financial_ledger": {
        "tables": {
            "projects", "phases", "kill_signals", "kill_recommendations", "assets", "revenue_records",
            "cost_records", "treasury", "routing_decisions",
        },
        "indexes": {
            "idx_projects_status", "idx_projects_opportunity_id", "idx_projects_income_mechanism", "idx_phases_project_sequence",
            "idx_phases_status", "idx_kill_signals_project_created", "idx_assets_project_id", "idx_assets_reusable",
            "idx_revenue_records_project_period_start", "idx_cost_records_project_created", "idx_cost_records_cost_category",
            "idx_cost_records_correlation_id", "idx_cost_records_cost_status", "idx_treasury_created_at",
            "idx_treasury_entry_type", "idx_routing_decisions_role_created", "idx_routing_decisions_route_selected",
            "idx_routing_decisions_correlation_id", "idx_routing_decisions_cost_status",
        },
    },
    "operator_digest": {
        "tables": {"digest_history", "alert_log", "harvest_requests", "gate_log", "operator_heartbeat", "operator_load_tracking"},
        "indexes": {
            "idx_alert_log_tier_created", "idx_alert_log_type_created", "idx_harvest_requests_status_expires",
            "idx_harvest_requests_priority_status", "idx_harvest_requests_task_id", "idx_gate_log_status",
            "idx_gate_log_type_created", "idx_gate_log_project_id", "idx_operator_heartbeat_timestamp_desc",
            "idx_operator_load_tracking_week_start",
        },
    },
}


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info('{table_name}')").fetchall()}


def _ensure_column(conn: sqlite3.Connection, table_name: str, column_name: str, ddl: str) -> None:
    tables = {
        row[0]
        for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    }
    if table_name not in tables:
        return
    if column_name in _table_columns(conn, table_name):
        return
    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {ddl}")


def _preflight_schema_compat(conn: sqlite3.Connection, schema_name: str) -> None:
    if schema_name == "immune_system.sql":
        tables = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        _ensure_column(
            conn,
            "immune_verdicts",
            "judge_mode",
            "judge_mode TEXT NOT NULL DEFAULT 'NOT_APPLICABLE' CHECK (judge_mode IN ('NOT_APPLICABLE','NORMAL','FALLBACK'))",
        )
        if "immune_verdicts" in tables:
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_immune_verdicts_judge_mode_timestamp ON immune_verdicts(judge_mode, timestamp)"
            )
        return
    if schema_name == "financial_ledger.sql":
        _ensure_column(conn, "cost_records", "correlation_id", "correlation_id TEXT")
        _ensure_column(conn, "cost_records", "route_decision_id", "route_decision_id TEXT")
        _ensure_column(
            conn,
            "cost_records",
            "cost_status",
            "cost_status TEXT NOT NULL DEFAULT 'FINAL' CHECK (cost_status IN ('ESTIMATED','FINAL','DISPUTED'))",
        )
        _ensure_column(conn, "routing_decisions", "project_id", "project_id TEXT")
        _ensure_column(conn, "routing_decisions", "session_id", "session_id TEXT")
        _ensure_column(conn, "routing_decisions", "correlation_id", "correlation_id TEXT")
        _ensure_column(
            conn,
            "routing_decisions",
            "cost_status",
            "cost_status TEXT NOT NULL DEFAULT 'NOT_APPLICABLE' CHECK (cost_status IN ('NOT_APPLICABLE','ESTIMATED','FINAL','DISPUTED'))",
        )


def _schema_hash(schema_path: Path) -> str:
    return hashlib.sha256(schema_path.read_bytes()).hexdigest()


def _table_signature(conn: sqlite3.Connection, table_name: str) -> list[tuple]:
    cols = conn.execute(f"PRAGMA table_xinfo('{table_name}')").fetchall()
    # cid, name, type, notnull, dflt_value, pk, hidden
    return [(c[1], c[2], c[3], c[5], c[6]) for c in cols]


def _index_signature(conn: sqlite3.Connection, index_name: str) -> tuple[int, list[tuple]]:
    info = conn.execute(f"PRAGMA index_xinfo('{index_name}')").fetchall()
    # seqno, cid, name, desc, coll, key
    key_cols = [(r[2], r[3], r[4], r[5]) for r in info if r[5] == 1]
    row = conn.execute("SELECT sql FROM sqlite_master WHERE type='index' AND name = ?", (index_name,)).fetchone()
    is_unique = 1 if row and row[0] and "unique index" in row[0].lower() else 0
    return is_unique, key_cols


def _semantic_schema_signatures(schema_path: Path) -> tuple[dict[str, list[tuple]], dict[str, tuple[int, list[tuple]]]]:
    """
    Build semantic table/index signatures by applying schema into an isolated in-memory DB.
    This avoids brittle raw SQL text comparisons against sqlite_master canonicalization.
    """
    sql = schema_path.read_text(encoding="utf-8")
    with sqlite3.connect(":memory:") as conn:
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("BEGIN")
        for statement in sql.split(";"):
            statement = statement.strip()
            if statement:
                conn.execute(statement)
        conn.commit()
        objects = conn.execute(
            "SELECT type, name FROM sqlite_master WHERE type IN ('table','index') AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        tables = {name for obj_type, name in objects if obj_type == "table"}
        indexes = {name for obj_type, name in objects if obj_type == "index"}
        table_sigs = {name: _table_signature(conn, name) for name in tables}
        index_sigs = {name: _index_signature(conn, name) for name in indexes}
    return table_sigs, index_sigs


def apply_schema(db_path: Path, schema_path: Path) -> None:
    sql = schema_path.read_text(encoding="utf-8")
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        _preflight_schema_compat(conn, schema_path.name)
        conn.execute("BEGIN")
        for statement in sql.split(";"):
            statement = statement.strip()
            if statement:
                conn.execute(statement)
        conn.commit()
        _ensure_schema_meta_table(conn)
        _upsert_schema_meta(conn, schema_path.name, _schema_hash(schema_path))
        conn.commit()


def _ensure_schema_meta_table(conn: sqlite3.Connection) -> None:
    strict_ddl = """
    CREATE TABLE IF NOT EXISTS _schema_meta (
      schema_name TEXT PRIMARY KEY,
      schema_hash TEXT NOT NULL,
      applied_at TEXT NOT NULL DEFAULT (datetime('now'))
    ) STRICT
    """
    compatible_ddl = """
    CREATE TABLE IF NOT EXISTS _schema_meta (
      schema_name TEXT PRIMARY KEY,
      schema_hash TEXT NOT NULL,
      applied_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """
    try:
        conn.execute(strict_ddl)
    except sqlite3.OperationalError as exc:
        # SQLite <3.37 does not support STRICT tables.
        if "strict" not in str(exc).lower():
            raise
        conn.execute(compatible_ddl)


def _upsert_schema_meta(conn: sqlite3.Connection, schema_name: str, schema_hash: str) -> None:
    upsert_sql = """
    INSERT INTO _schema_meta(schema_name, schema_hash, applied_at)
    VALUES (?, ?, datetime('now'))
    ON CONFLICT(schema_name) DO UPDATE SET
      schema_hash=excluded.schema_hash,
      applied_at=excluded.applied_at
    """
    try:
        conn.execute(upsert_sql, (schema_name, schema_hash))
    except sqlite3.OperationalError as exc:
        # SQLite <3.24 may not support UPSERT syntax.
        msg = str(exc).lower()
        upsert_unsupported = ("upsert" in msg) or ("on conflict" in msg) or ("syntax error" in msg and "near \"on\"" in msg)
        if not upsert_unsupported:
            raise
        # Fall back to UPDATE+INSERT probe sequence.
        cursor = conn.execute(
            "UPDATE _schema_meta SET schema_hash=?, applied_at=datetime('now') WHERE schema_name=?",
            (schema_hash, schema_name),
        )
        if cursor.rowcount == 0:
            conn.execute(
                "INSERT INTO _schema_meta(schema_name, schema_hash, applied_at) VALUES (?, ?, datetime('now'))",
                (schema_name, schema_hash),
            )


def verify_database(db_path: Path, db_name: str, schema_path: Path) -> tuple[bool, list[str]]:
    expected = EXPECTED_OBJECTS[db_name]
    errors: list[str] = []
    expected_table_sigs, expected_index_sigs = _semantic_schema_signatures(schema_path)
    with sqlite3.connect(db_path) as conn:
        mode = conn.execute("PRAGMA journal_mode;").fetchone()[0].lower()
        if mode != "wal":
            errors.append(f"journal_mode expected wal, got {mode}")

        objects = conn.execute(
            "SELECT type, name FROM sqlite_master WHERE type IN ('table','index')"
        ).fetchall()
        tables = {name for obj_type, name in objects if obj_type == "table" and not name.startswith("sqlite_")}
        indexes = {name for obj_type, name in objects if obj_type == "index" and not name.startswith("sqlite_")}

        missing_tables = sorted(expected["tables"] - tables)
        missing_indexes = sorted(expected["indexes"] - indexes)
        if missing_tables:
            errors.append(f"missing tables: {', '.join(missing_tables)}")
        if missing_indexes:
            errors.append(f"missing indexes: {', '.join(missing_indexes)}")

        # Semantic schema fidelity check (column/index signatures), robust to SQL formatting/canonicalization.
        for table in sorted(expected["tables"] & expected_table_sigs.keys()):
            actual_sig = _table_signature(conn, table)
            if actual_sig != expected_table_sigs[table]:
                errors.append(f"table drift detected for {table}")

        for index in sorted(expected["indexes"] & expected_index_sigs.keys()):
            actual_idx = _index_signature(conn, index)
            if actual_idx != expected_index_sigs[index]:
                errors.append(f"index drift detected for {index}")

        meta = conn.execute(
            "SELECT schema_hash FROM _schema_meta WHERE schema_name = ?",
            (schema_path.name,),
        ).fetchone()
        if meta is None:
            errors.append("missing _schema_meta entry for schema")
        elif meta[0] != _schema_hash(schema_path):
            errors.append("schema hash mismatch")
    return (len(errors) == 0, errors)


def main() -> int:
    parser = argparse.ArgumentParser(description="Hybrid Autonomous AI schema migration runner")
    parser.add_argument("--db-dir", default="./data/", help="Directory for SQLite files")
    parser.add_argument("--verify", action="store_true", help="Verify required tables and indexes")
    args = parser.parse_args()

    root = Path(__file__).resolve().parent
    db_dir = (root / args.db_dir).resolve()
    db_dir.mkdir(parents=True, exist_ok=True)

    all_ok = True
    for db_name, schema_rel in SCHEMAS.items():
        db_path = db_dir / f"{db_name}.db"
        schema_path = root / schema_rel
        try:
            apply_schema(db_path, schema_path)
            print(f"[OK] migrated {db_name} -> {db_path}")
        except Exception as exc:  # noqa: BLE001
            all_ok = False
            print(f"[FAIL] migration {db_name}: {exc}")
            continue

        if args.verify:
            ok, errors = verify_database(db_path, db_name, schema_path)
            if ok:
                print(f"[OK] verified {db_name}")
            else:
                all_ok = False
                print(f"[FAIL] verify {db_name}: {'; '.join(errors)}")

    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
