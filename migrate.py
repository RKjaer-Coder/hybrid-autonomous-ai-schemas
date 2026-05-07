#!/usr/bin/env python3
"""Create/verify Hybrid Autonomous AI SQLite schema suite."""

from __future__ import annotations

import argparse
import hashlib
import re
import sqlite3
from pathlib import Path

LEGACY_SCHEMAS = {
    "strategic_memory": "schemas/strategic_memory.sql",
    "telemetry": "schemas/telemetry.sql",
    "immune_system": "schemas/immune_system.sql",
    "financial_ledger": "schemas/financial_ledger.sql",
    "operator_digest": "schemas/operator_digest.sql",
}

KERNEL_SCHEMAS = {
    "kernel": "schemas/kernel.sql",
}

SCHEMAS = {**KERNEL_SCHEMAS, **LEGACY_SCHEMAS}

EXPECTED_OBJECTS = {
    "kernel": {
        "tables": {
            "commands",
            "events",
            "research_requests",
            "source_plans",
            "source_acquisition_checks",
            "decisions",
            "quality_gate_events",
            "evidence_bundles",
            "commercial_decision_packets",
            "commercial_decision_recommendations",
            "projects",
            "project_tasks",
            "project_task_assignments",
            "project_outcomes",
            "project_artifact_receipts",
            "project_customer_feedback",
            "project_revenue_attributions",
            "project_operator_load",
            "project_commercial_rollups",
            "project_status_rollups",
            "project_close_decision_packets",
            "project_replay_projection_comparisons",
            "project_portfolio_decision_packets",
            "project_portfolio_replay_projection_comparisons",
            "project_scheduling_intents",
            "project_scheduling_priority_change_packets",
            "project_scheduling_priority_replay_projection_comparisons",
            "project_scheduling_replay_projection_comparisons",
            "project_customer_visible_packets",
            "project_customer_commitments",
            "project_customer_commitment_receipts",
            "project_customer_visible_replay_projection_comparisons",
            "model_task_classes",
            "model_candidates",
            "model_holdout_policies",
            "local_offload_eval_sets",
            "model_holdout_use_records",
            "model_eval_runs",
            "model_route_decisions",
            "model_promotion_decision_packets",
            "model_demotion_records",
            "model_routing_state",
            "capability_grants",
            "budgets",
            "budget_reservations",
            "artifact_refs",
            "side_effect_intents",
            "side_effect_receipts",
            "projection_outbox",
        },
        "indexes": {
            "idx_commands_idempotency_key",
            "idx_events_entity",
            "idx_events_command",
            "idx_research_requests_profile_status",
            "idx_research_requests_decision_target",
            "idx_source_plans_request",
            "idx_source_acquisition_checks_plan",
            "idx_quality_gate_events_request",
            "idx_decisions_type_status",
            "idx_decisions_authority",
            "idx_evidence_bundles_request",
            "idx_evidence_bundles_quality",
            "idx_commercial_decision_packets_target",
            "idx_commercial_decision_packets_bundle",
            "idx_commercial_decision_recommendations_packet",
            "idx_commercial_decision_recommendations_authority",
            "idx_projects_status",
            "idx_projects_packet",
            "idx_project_tasks_project_status",
            "idx_project_tasks_type_status",
            "idx_project_task_assignments_task",
            "idx_project_task_assignments_worker",
            "idx_project_outcomes_project",
            "idx_project_outcomes_task",
            "idx_project_artifact_receipts_project",
            "idx_project_artifact_receipts_task",
            "idx_project_customer_feedback_project",
            "idx_project_customer_feedback_artifact",
            "idx_project_revenue_attributions_project",
            "idx_project_revenue_attributions_status",
            "idx_project_operator_load_project",
            "idx_project_operator_load_type",
            "idx_project_commercial_rollups_project",
            "idx_project_status_rollups_project",
            "idx_project_close_decision_packets_project",
            "idx_project_replay_projection_comparisons_project",
            "idx_project_portfolio_decision_packets_status",
            "idx_project_portfolio_replay_projection_packet",
            "idx_project_scheduling_intents_packet",
            "idx_project_scheduling_intents_status",
            "idx_project_scheduling_priority_packets_intent",
            "idx_project_scheduling_priority_packets_status",
            "idx_project_scheduling_priority_replay_projection_packet",
            "idx_project_scheduling_replay_projection_intent",
            "idx_project_customer_visible_packets_project",
            "idx_project_customer_visible_packets_outcome",
            "idx_project_customer_commitments_project",
            "idx_project_customer_commitments_packet",
            "idx_project_customer_commitment_receipts_project",
            "idx_project_customer_commitment_receipts_commitment",
            "idx_project_customer_visible_replay_projection_packet",
            "idx_model_task_classes_status",
            "idx_model_candidates_state",
            "idx_model_holdout_policies_task",
            "idx_local_offload_eval_sets_task",
            "idx_model_holdout_use_records_eval",
            "idx_model_eval_runs_model_task",
            "idx_model_eval_runs_verdict",
            "idx_model_route_decisions_task",
            "idx_model_route_decisions_model",
            "idx_model_promotion_packets_task",
            "idx_model_promotion_packets_model",
            "idx_model_demotion_records_model",
            "idx_model_demotion_records_task",
            "idx_model_routing_state_role",
            "idx_model_routing_state_model",
            "idx_capability_grants_subject",
            "idx_budgets_owner",
            "idx_budget_reservations_budget_status",
            "idx_artifact_refs_data_class",
            "idx_side_effect_intents_status",
            "idx_side_effect_receipts_intent",
            "idx_projection_outbox_status",
        },
    },
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
        "tables": {"step_outcomes", "chain_definitions", "execution_traces", "harness_variants"},
        "indexes": {
            "idx_step_outcomes_step_skill_timestamp", "idx_step_outcomes_chain_id",
            "idx_step_outcomes_outcome_timestamp", "idx_step_outcomes_skill_timestamp",
            "idx_execution_traces_skill_created", "idx_execution_traces_training_created",
            "idx_execution_traces_retention_created", "idx_hv_skill_status",
            "idx_hv_created", "idx_hv_active_skill",
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
            "cost_records", "treasury", "routing_decisions", "g3_approval_requests",
        },
        "indexes": {
            "idx_projects_status", "idx_projects_opportunity_id", "idx_projects_income_mechanism", "idx_phases_project_sequence",
            "idx_phases_status", "idx_kill_signals_project_created", "idx_assets_project_id", "idx_assets_reusable",
            "idx_revenue_records_project_period_start", "idx_cost_records_project_created", "idx_cost_records_cost_category",
            "idx_cost_records_correlation_id", "idx_cost_records_cost_status", "idx_treasury_created_at",
            "idx_treasury_entry_type", "idx_routing_decisions_role_created", "idx_routing_decisions_route_selected",
            "idx_routing_decisions_correlation_id", "idx_routing_decisions_approval_request_id",
            "idx_routing_decisions_dispatch_status", "idx_routing_decisions_cost_status",
            "idx_g3_approval_requests_correlation_id", "idx_g3_approval_requests_status_requested",
            "idx_g3_approval_requests_expires_at", "idx_g3_approval_requests_project_status",
        },
    },
    "operator_digest": {
        "tables": {
            "digest_history",
            "alert_log",
            "harvest_requests",
            "gate_log",
            "operator_heartbeat",
            "operator_load_tracking",
            "operator_project_preferences",
            "operator_manual_tasks",
            "runtime_control_state",
            "runtime_halt_events",
            "runtime_restart_history",
        },
        "indexes": {
            "idx_alert_log_tier_created", "idx_alert_log_type_created", "idx_harvest_requests_status_expires",
            "idx_harvest_requests_priority_status", "idx_harvest_requests_task_id", "idx_gate_log_status",
            "idx_gate_log_type_created", "idx_gate_log_project_id", "idx_operator_heartbeat_timestamp_desc",
            "idx_operator_load_tracking_week_start", "idx_operator_project_preferences_priority_updated",
            "idx_operator_manual_tasks_status_priority", "idx_operator_manual_tasks_project_status",
            "idx_runtime_halt_events_status_created",
            "idx_runtime_halt_events_source_created", "idx_runtime_restart_history_status_requested",
            "idx_runtime_restart_history_halt_requested",
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
    if schema_name == "kernel.sql":
        _rebuild_kernel_events_for_research_entities(conn)
        _rebuild_kernel_projection_outbox_if_drifted(conn)
        _rebuild_project_outcomes_for_operate_followup(conn)
        return
    if schema_name == "strategic_memory.sql":
        _ensure_column(
            conn,
            "council_verdicts",
            "degraded",
            "degraded INTEGER NOT NULL DEFAULT 0 CHECK (degraded IN (0, 1))",
        )
        _ensure_column(
            conn,
            "council_verdicts",
            "confidence_cap",
            "confidence_cap REAL CHECK (confidence_cap IS NULL OR (confidence_cap >= 0.0 AND confidence_cap <= 1.0))",
        )
        return
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
        _ensure_column(conn, "immune_verdicts", "task_type", "task_type TEXT")
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
        _ensure_column(conn, "routing_decisions", "approval_request_id", "approval_request_id TEXT")
        _ensure_column(
            conn,
            "routing_decisions",
            "dispatch_status",
            "dispatch_status TEXT NOT NULL DEFAULT 'NOT_APPLICABLE' CHECK (dispatch_status IN ('NOT_APPLICABLE','AWAITING_APPROVAL','APPROVED_PENDING_DISPATCH','DISPATCHED','FINALIZED','DENIED','EXPIRED'))",
        )
        _ensure_column(conn, "routing_decisions", "dispatched_at", "dispatched_at TEXT")
        _ensure_column(conn, "routing_decisions", "finalized_at", "finalized_at TEXT")
        _ensure_column(conn, "routing_decisions", "final_cost_usd", "final_cost_usd REAL")
        _ensure_column(
            conn,
            "routing_decisions",
            "cost_status",
            "cost_status TEXT NOT NULL DEFAULT 'NOT_APPLICABLE' CHECK (cost_status IN ('NOT_APPLICABLE','ESTIMATED','FINAL','DISPUTED'))",
        )
        if "routing_decisions" in {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }:
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_routing_decisions_approval_request_id ON routing_decisions(approval_request_id)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_routing_decisions_dispatch_status ON routing_decisions(dispatch_status, created_at)"
            )
        return
    if schema_name == "operator_digest.sql":
        _rebuild_operator_heartbeat_for_dashboard_channel(conn)


def _normalized_sql(sql: str | None) -> str:
    if not sql:
        return ""
    return re.sub(r"\s+", " ", sql.strip()).lower()


def _object_sql(conn: sqlite3.Connection, obj_type: str, name: str) -> str:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type = ? AND name = ?",
        (obj_type, name),
    ).fetchone()
    return "" if row is None else str(row[0] or "")


def _rebuild_operator_heartbeat_for_dashboard_channel(conn: sqlite3.Connection) -> None:
    """Rebuild old operator_heartbeat CHECK constraints that predate Hermes dashboard."""
    existing_sql = _object_sql(conn, "table", "operator_heartbeat")
    if not existing_sql or "hermes_dashboard" in existing_sql:
        return
    conn.execute("ALTER TABLE operator_heartbeat RENAME TO operator_heartbeat__old")
    conn.execute(
        """
        CREATE TABLE operator_heartbeat (
          entry_id TEXT PRIMARY KEY,
          interaction_type TEXT NOT NULL CHECK (interaction_type IN ('message', 'gate_response', 'digest_ack', 'command')),
          channel TEXT NOT NULL CHECK (channel IN ('CLI', 'mission_control', 'hermes_dashboard', 'telegram', 'discord', 'slack')),
          timestamp TEXT NOT NULL
        ) STRICT
        """
    )
    conn.execute(
        """
        INSERT INTO operator_heartbeat(entry_id, interaction_type, channel, timestamp)
        SELECT entry_id, interaction_type, channel, timestamp
        FROM operator_heartbeat__old
        """
    )
    conn.execute("DROP TABLE operator_heartbeat__old")


def _rebuild_kernel_events_for_research_entities(conn: sqlite3.Connection) -> None:
    """Rebuild old kernel events CHECK constraints that predate newer research entities."""
    existing_sql = _object_sql(conn, "table", "events")
    if not existing_sql or ("evidence_bundle" in existing_sql and "source_plan" in existing_sql):
        return
    conn.execute("PRAGMA foreign_keys=OFF")
    try:
        conn.execute("ALTER TABLE events RENAME TO events__old")
        conn.execute(
            """
            CREATE TABLE events (
              event_seq INTEGER PRIMARY KEY AUTOINCREMENT,
              event_id TEXT NOT NULL UNIQUE,
              event_schema_version INTEGER NOT NULL,
              event_type TEXT NOT NULL,
              entity_type TEXT NOT NULL CHECK (entity_type IN ('task','research_request','source_plan','evidence_bundle','decision','project','model','budget','gate','capability','side_effect','policy','artifact')),
              entity_id TEXT NOT NULL,
              transaction_id TEXT NOT NULL,
              command_id TEXT REFERENCES commands(command_id),
              correlation_id TEXT,
              causation_event_id TEXT,
              actor_type TEXT NOT NULL CHECK (actor_type IN ('kernel','operator','agent','tool','model','scheduler')),
              actor_id TEXT NOT NULL,
              timestamp TEXT NOT NULL,
              policy_version TEXT NOT NULL,
              data_class TEXT NOT NULL CHECK (data_class IN ('public','internal','sensitive','secret_ref','regulated','client_confidential')),
              payload_hash TEXT NOT NULL,
              payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
              prev_event_hash TEXT,
              event_hash TEXT NOT NULL
            ) STRICT
            """
        )
        conn.execute(
            """
            INSERT INTO events (
              event_seq, event_id, event_schema_version, event_type, entity_type,
              entity_id, transaction_id, command_id, correlation_id, causation_event_id,
              actor_type, actor_id, timestamp, policy_version, data_class,
              payload_hash, payload_json, prev_event_hash, event_hash
            )
            SELECT
              event_seq, event_id, event_schema_version, event_type, entity_type,
              entity_id, transaction_id, command_id, correlation_id, causation_event_id,
              actor_type, actor_id, timestamp, policy_version, data_class,
              payload_hash, payload_json, prev_event_hash, event_hash
            FROM events__old
            """
        )
        conn.execute("DROP TABLE events__old")
    finally:
        conn.execute("PRAGMA foreign_keys=ON")


def _rebuild_kernel_projection_outbox_if_drifted(conn: sqlite3.Connection) -> None:
    """Normalize old projection_outbox table shapes before semantic verification."""
    existing_sql = _object_sql(conn, "table", "projection_outbox")
    if not existing_sql:
        return
    with sqlite3.connect(":memory:") as expected:
        expected.execute(
            """
            CREATE TABLE projection_outbox (
              outbox_id TEXT PRIMARY KEY,
              event_id TEXT NOT NULL REFERENCES events(event_id),
              projection_name TEXT NOT NULL,
              status TEXT NOT NULL CHECK (status IN ('pending','complete','failed','halted')),
              created_at TEXT NOT NULL,
              completed_at TEXT,
              error TEXT
            ) STRICT
            """
        )
        expected_sig = _table_signature(expected, "projection_outbox")
    if _table_signature(conn, "projection_outbox") == expected_sig:
        return
    conn.execute("PRAGMA foreign_keys=OFF")
    try:
        conn.execute("ALTER TABLE projection_outbox RENAME TO projection_outbox__old")
        conn.execute(
            """
            CREATE TABLE projection_outbox (
              outbox_id TEXT PRIMARY KEY,
              event_id TEXT NOT NULL REFERENCES events(event_id),
              projection_name TEXT NOT NULL,
              status TEXT NOT NULL CHECK (status IN ('pending','complete','failed','halted')),
              created_at TEXT NOT NULL,
              completed_at TEXT,
              error TEXT
            ) STRICT
            """
        )
        old_cols = _table_columns(conn, "projection_outbox__old")
        if {"outbox_id", "event_id", "status", "created_at"}.issubset(old_cols):
            projection_expr = "projection_name" if "projection_name" in old_cols else "'legacy_projection'"
            completed_expr = "completed_at" if "completed_at" in old_cols else "NULL"
            error_expr = "error" if "error" in old_cols else "NULL"
            conn.execute(
                f"""
                INSERT INTO projection_outbox(
                  outbox_id, event_id, projection_name, status, created_at, completed_at, error
                )
                SELECT outbox_id, event_id, {projection_expr}, status, created_at, {completed_expr}, {error_expr}
                FROM projection_outbox__old
                """
            )
        conn.execute("DROP TABLE projection_outbox__old")
    finally:
        conn.execute("PRAGMA foreign_keys=ON")


def _rebuild_project_outcomes_for_operate_followup(conn: sqlite3.Connection) -> None:
    """Rebuild old project_outcomes constraints for Operate follow-up receipts."""
    existing_sql = _object_sql(conn, "table", "project_outcomes")
    if not existing_sql:
        return
    existing_cols = _table_columns(conn, "project_outcomes")
    if (
        "operate_followup" in existing_sql
        and "side_effect_intent_id" in existing_cols
        and "side_effect_receipt_id" in existing_cols
    ):
        return
    conn.execute("PRAGMA foreign_keys=OFF")
    try:
        conn.execute("ALTER TABLE project_outcomes RENAME TO project_outcomes__old")
        conn.execute(
            """
            CREATE TABLE project_outcomes (
              outcome_id TEXT PRIMARY KEY,
              project_id TEXT NOT NULL REFERENCES projects(project_id),
              task_id TEXT REFERENCES project_tasks(task_id),
              phase_name TEXT,
              outcome_type TEXT NOT NULL CHECK (outcome_type IN ('validation','build_artifact','shipped_artifact','feedback','project_close','operate_followup')),
              summary TEXT NOT NULL,
              artifact_refs_json TEXT NOT NULL CHECK (json_valid(artifact_refs_json)),
              metrics_json TEXT NOT NULL CHECK (json_valid(metrics_json)),
              feedback_json TEXT NOT NULL CHECK (json_valid(feedback_json)),
              revenue_impact_json TEXT NOT NULL CHECK (json_valid(revenue_impact_json)),
              operator_load_actual TEXT,
              side_effect_intent_id TEXT REFERENCES side_effect_intents(intent_id),
              side_effect_receipt_id TEXT REFERENCES side_effect_receipts(receipt_id),
              status TEXT NOT NULL CHECK (status IN ('recorded','accepted','needs_followup')),
              created_at TEXT NOT NULL
            ) STRICT
            """
        )
        side_effect_intent_expr = "side_effect_intent_id" if "side_effect_intent_id" in existing_cols else "NULL"
        side_effect_receipt_expr = "side_effect_receipt_id" if "side_effect_receipt_id" in existing_cols else "NULL"
        conn.execute(
            f"""
            INSERT INTO project_outcomes (
              outcome_id, project_id, task_id, phase_name, outcome_type, summary,
              artifact_refs_json, metrics_json, feedback_json, revenue_impact_json,
              operator_load_actual, side_effect_intent_id, side_effect_receipt_id,
              status, created_at
            )
            SELECT
              outcome_id, project_id, task_id, phase_name, outcome_type, summary,
              artifact_refs_json, metrics_json, feedback_json, revenue_impact_json,
              operator_load_actual, {side_effect_intent_expr}, {side_effect_receipt_expr},
              status, created_at
            FROM project_outcomes__old
            """
        )
        conn.execute("DROP TABLE project_outcomes__old")
    finally:
        conn.execute("PRAGMA foreign_keys=ON")


def _db_name_for_schema(schema_name: str) -> str | None:
    for db_name, schema_rel in SCHEMAS.items():
        if Path(schema_rel).name == schema_name:
            return db_name
    return None


def _schema_hash(schema_path: Path) -> str:
    return hashlib.sha256(schema_path.read_bytes()).hexdigest()


def _table_signature(conn: sqlite3.Connection, table_name: str) -> list[tuple]:
    cols = conn.execute(f"PRAGMA table_xinfo('{table_name}')").fetchall()
    # cid, name, type, notnull, dflt_value, pk, hidden, plus table SQL for CHECK/FK/STRICT drift.
    return [
        ("__table_sql__", _normalized_sql(_object_sql(conn, "table", table_name))),
        *[(c[1], c[2], c[3], c[4], c[5], c[6]) for c in cols],
    ]


def _index_signature(conn: sqlite3.Connection, index_name: str) -> tuple[int, list[tuple]]:
    info = conn.execute(f"PRAGMA index_xinfo('{index_name}')").fetchall()
    # seqno, cid, name, desc, coll, key
    key_cols = [(r[2], r[3], r[4], r[5]) for r in info if r[5] == 1]
    row = conn.execute("SELECT sql FROM sqlite_master WHERE type='index' AND name = ?", (index_name,)).fetchone()
    is_unique = 1 if row and row[0] and "unique index" in row[0].lower() else 0
    return is_unique, key_cols, _normalized_sql(_object_sql(conn, "index", index_name))


def _schema_fidelity_errors(conn: sqlite3.Connection, db_name: str, schema_path: Path) -> list[str]:
    expected = EXPECTED_OBJECTS[db_name]
    errors: list[str] = []
    expected_table_sigs, expected_index_sigs = _semantic_schema_signatures(schema_path)

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

    for table in sorted(expected["tables"] & expected_table_sigs.keys()):
        actual_sig = _table_signature(conn, table)
        if actual_sig != expected_table_sigs[table]:
            errors.append(f"table drift detected for {table}")

    for index in sorted(expected["indexes"] & expected_index_sigs.keys()):
        actual_idx = _index_signature(conn, index)
        if actual_idx != expected_index_sigs[index]:
            errors.append(f"index drift detected for {index}")
    return errors


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
        conn.commit()
        conn.execute("BEGIN")
        for statement in sql.split(";"):
            statement = statement.strip()
            if statement:
                conn.execute(statement)
        conn.commit()
        _ensure_schema_meta_table(conn)
        db_name = _db_name_for_schema(schema_path.name)
        if db_name is None or not _schema_fidelity_errors(conn, db_name, schema_path):
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
    errors: list[str] = []
    with sqlite3.connect(db_path) as conn:
        mode = conn.execute("PRAGMA journal_mode;").fetchone()[0].lower()
        if mode != "wal":
            errors.append(f"journal_mode expected wal, got {mode}")

        # Semantic schema fidelity check includes column shape and normalized SQL so CHECK/FK/STRICT drift is visible.
        errors.extend(_schema_fidelity_errors(conn, db_name, schema_path))

        try:
            meta = conn.execute(
                "SELECT schema_hash FROM _schema_meta WHERE schema_name = ?",
                (schema_path.name,),
            ).fetchone()
        except sqlite3.OperationalError as exc:
            if "no such table" not in str(exc).lower():
                raise
            meta = None
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
