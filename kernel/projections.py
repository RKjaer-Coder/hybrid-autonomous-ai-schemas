from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from .records import canonical_json, now_iso
from .store import KernelStore

OPERATOR_MIGRATION_READINESS_PROJECTION = "migration_readiness_record_projection"


def _operator_projection_payload_from_row(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "record_id": row["record_id"],
        "surface_ref": row["surface_ref"],
        "component_type": row["component_type"],
        "ownership_action": row["ownership_action"],
        "owner_domain": row["owner_domain"],
        "summary": row["summary"],
        "blockers": json.loads(row["blockers_json"]),
        "evidence_refs": json.loads(row["evidence_refs_json"]),
        "next_operator_actions": json.loads(row["next_operator_actions_json"]),
        "readiness_status": row["readiness_status"],
        "live_controls_enabled": bool(row["live_controls_enabled"]),
        "created_at": row["created_at"],
    }


def apply_operator_digest_projection_outbox(
    kernel_db_path: str | Path,
    operator_digest_db_path: str | Path,
    *,
    limit: int | None = None,
) -> dict[str, Any]:
    """Apply supported kernel projection outbox rows into operator_digest.db.

    The operator database remains a legacy projection. This applier only handles
    read-only inspection rows derived from kernel events and marks unsupported
    outbox rows untouched for future projection workers.
    """

    kernel_db_path = Path(kernel_db_path)
    operator_digest_db_path = Path(operator_digest_db_path)
    if not kernel_db_path.is_file():
        raise FileNotFoundError(f"kernel database not found: {kernel_db_path}")
    if not operator_digest_db_path.is_file():
        raise FileNotFoundError(f"operator digest database not found: {operator_digest_db_path}")

    applied = 0
    failed = 0
    skipped = 0
    errors: list[dict[str, str]] = []
    row_limit = "" if limit is None else "LIMIT ?"
    params: tuple[Any, ...] = () if limit is None else (limit,)
    now = now_iso()

    with sqlite3.connect(kernel_db_path, timeout=5.0, isolation_level=None) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.execute("ATTACH DATABASE ? AS operator_digest", (str(operator_digest_db_path),))
        try:
            conn.execute("BEGIN IMMEDIATE")
            rows = conn.execute(
                f"""
                SELECT outbox_id, event_id, projection_name
                FROM projection_outbox
                WHERE status='pending'
                  AND projection_name=?
                ORDER BY created_at, outbox_id
                {row_limit}
                """,
                (OPERATOR_MIGRATION_READINESS_PROJECTION, *params),
            ).fetchall()
            for row in rows:
                event = conn.execute(
                    """
                    SELECT event_id, event_type, payload_json
                    FROM events
                    WHERE event_id=?
                    """,
                    (row["event_id"],),
                ).fetchone()
                if event is None:
                    failed += 1
                    error = "missing event for projection outbox row"
                    errors.append({"outbox_id": row["outbox_id"], "error": error})
                    conn.execute(
                        "UPDATE projection_outbox SET status='failed', completed_at=?, error=? WHERE outbox_id=?",
                        (now, error, row["outbox_id"]),
                    )
                    continue
                if event["event_type"] != "migration_readiness_recorded":
                    skipped += 1
                    continue
                payload = json.loads(event["payload_json"])
                if payload.get("live_controls_enabled"):
                    failed += 1
                    error = "operator projection refuses live-control-enabled records"
                    errors.append({"outbox_id": row["outbox_id"], "error": error})
                    conn.execute(
                        "UPDATE projection_outbox SET status='failed', completed_at=?, error=? WHERE outbox_id=?",
                        (now, error, row["outbox_id"]),
                    )
                    continue
                conn.execute(
                    """
                    INSERT INTO operator_digest.kernel_migration_readiness_projection (
                      surface_ref, record_id, component_type, ownership_action, owner_domain,
                      summary, blockers_json, evidence_refs_json, next_operator_actions_json,
                      readiness_status, live_controls_enabled, authoritative_source,
                      projection_event_id, created_at, projected_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 'kernel.events', ?, ?, ?)
                    ON CONFLICT(surface_ref) DO UPDATE SET
                      record_id=excluded.record_id,
                      component_type=excluded.component_type,
                      ownership_action=excluded.ownership_action,
                      owner_domain=excluded.owner_domain,
                      summary=excluded.summary,
                      blockers_json=excluded.blockers_json,
                      evidence_refs_json=excluded.evidence_refs_json,
                      next_operator_actions_json=excluded.next_operator_actions_json,
                      readiness_status=excluded.readiness_status,
                      live_controls_enabled=0,
                      authoritative_source='kernel.events',
                      projection_event_id=excluded.projection_event_id,
                      created_at=excluded.created_at,
                      projected_at=excluded.projected_at
                    """,
                    (
                        payload["surface_ref"],
                        payload["record_id"],
                        payload["component_type"],
                        payload["ownership_action"],
                        payload["owner_domain"],
                        payload["summary"],
                        canonical_json(payload["blockers"]),
                        canonical_json(payload["evidence_refs"]),
                        canonical_json(payload["next_operator_actions"]),
                        payload["readiness_status"],
                        event["event_id"],
                        payload["created_at"],
                        now,
                    ),
                )
                conn.execute(
                    "UPDATE projection_outbox SET status='complete', completed_at=?, error=NULL WHERE outbox_id=?",
                    (now, row["outbox_id"]),
                )
                applied += 1
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.execute("DETACH DATABASE operator_digest")
    return {
        "applied": applied,
        "failed": failed,
        "skipped": skipped,
        "errors": errors,
        "projection_name": OPERATOR_MIGRATION_READINESS_PROJECTION,
        "target_database": "operator_digest.db",
        "target_table": "kernel_migration_readiness_projection",
        "live_controls_enabled": False,
    }


def compare_operator_digest_migration_readiness_projection(
    kernel_db_path: str | Path,
    operator_digest_db_path: str | Path,
) -> dict[str, Any]:
    kernel_db_path = Path(kernel_db_path)
    operator_digest_db_path = Path(operator_digest_db_path)
    replay = KernelStore(kernel_db_path).replay_critical_state()
    replay_records = dict(sorted(replay.migration_readiness_records.items()))
    projection_records: dict[str, dict[str, Any]] = {}
    if operator_digest_db_path.is_file():
        with sqlite3.connect(operator_digest_db_path) as conn:
            conn.row_factory = sqlite3.Row
            try:
                rows = conn.execute(
                    """
                    SELECT *
                    FROM kernel_migration_readiness_projection
                    ORDER BY surface_ref
                    """
                ).fetchall()
            except sqlite3.OperationalError:
                rows = []
        projection_records = {
            row["surface_ref"]: _operator_projection_payload_from_row(row)
            for row in rows
        }
    mismatches: list[str] = []
    if replay_records != projection_records:
        missing = sorted(set(replay_records) - set(projection_records))
        extra = sorted(set(projection_records) - set(replay_records))
        changed = sorted(
            key
            for key in set(replay_records).intersection(projection_records)
            if replay_records[key] != projection_records[key]
        )
        if missing:
            mismatches.append("missing_operator_projection_records")
        if extra:
            mismatches.append("extra_operator_projection_records")
        if changed:
            mismatches.append("changed_operator_projection_records")
    return {
        "scope": "operator_digest.kernel_migration_readiness_projection",
        "matches": not mismatches,
        "mismatches": mismatches,
        "replay_record_count": len(replay_records),
        "projection_record_count": len(projection_records),
        "live_controls_enabled": False,
    }
