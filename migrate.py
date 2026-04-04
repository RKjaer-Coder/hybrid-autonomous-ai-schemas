#!/usr/bin/env python3
"""Create/verify Hybrid Autonomous AI SQLite schema suite."""

from __future__ import annotations

import argparse
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
        "tables": {"immune_verdicts", "security_alerts", "circuit_breaker_log", "jwt_revocation_log", "skill_improvement_log"},
        "indexes": {
            "idx_immune_verdicts_skill_timestamp", "idx_immune_verdicts_result_timestamp", "idx_immune_verdicts_session_id",
            "idx_security_alerts_timestamp", "idx_circuit_breaker_name_timestamp", "idx_circuit_breaker_state",
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
            "idx_treasury_created_at", "idx_treasury_entry_type", "idx_routing_decisions_role_created",
            "idx_routing_decisions_route_selected",
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


def apply_schema(db_path: Path, schema_path: Path) -> None:
    sql = schema_path.read_text(encoding="utf-8")
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.executescript(sql)
        conn.commit()


def verify_database(db_path: Path, db_name: str) -> tuple[bool, list[str]]:
    expected = EXPECTED_OBJECTS[db_name]
    errors: list[str] = []
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
            ok, errors = verify_database(db_path, db_name)
            if ok:
                print(f"[OK] verified {db_name}")
            else:
                all_ok = False
                print(f"[FAIL] verify {db_name}: {'; '.join(errors)}")

    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
