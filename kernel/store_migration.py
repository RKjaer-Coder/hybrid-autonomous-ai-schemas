from __future__ import annotations

import sqlite3
from typing import Any

from .records import (
    MigrationReadinessRecord,
    MigrationReadinessReplayProjectionComparison,
    canonical_json,
)
from .store_common import _loads


def _migration_readiness_record_payload(record: MigrationReadinessRecord) -> dict[str, Any]:
    return {
        "record_id": record.record_id,
        "surface_ref": record.surface_ref,
        "component_type": record.component_type,
        "ownership_action": record.ownership_action,
        "owner_domain": record.owner_domain,
        "summary": record.summary,
        "blockers": record.blockers,
        "evidence_refs": record.evidence_refs,
        "next_operator_actions": record.next_operator_actions,
        "readiness_status": record.readiness_status,
        "live_controls_enabled": record.live_controls_enabled,
        "created_at": record.created_at,
    }


def _migration_readiness_record_row_payload(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "record_id": row["record_id"],
        "surface_ref": row["surface_ref"],
        "component_type": row["component_type"],
        "ownership_action": row["ownership_action"],
        "owner_domain": row["owner_domain"],
        "summary": row["summary"],
        "blockers": _loads(row["blockers_json"]),
        "evidence_refs": _loads(row["evidence_refs_json"]),
        "next_operator_actions": _loads(row["next_operator_actions_json"]),
        "readiness_status": row["readiness_status"],
        "live_controls_enabled": bool(row["live_controls_enabled"]),
        "created_at": row["created_at"],
    }


def _migration_readiness_comparison_payload(
    comparison: MigrationReadinessReplayProjectionComparison,
) -> dict[str, Any]:
    return {
        "comparison_id": comparison.comparison_id,
        "scope": comparison.scope,
        "replay_records": comparison.replay_records,
        "projection_records": comparison.projection_records,
        "matches": comparison.matches,
        "mismatches": comparison.mismatches,
        "created_at": comparison.created_at,
    }


class MigrationKernelTransactionMixin:
    def record_migration_readiness(self, record: MigrationReadinessRecord) -> str:
        if self.command.requested_by in {"agent", "model", "tool"}:
            raise PermissionError("migration readiness records are kernel-owned read-only state")
        if record.live_controls_enabled:
            raise PermissionError("migration readiness records cannot enable live controls")
        if not record.surface_ref.strip() or not record.summary.strip() or not record.owner_domain.strip():
            raise ValueError("migration readiness records require surface_ref, owner_domain, and summary")
        payload = _migration_readiness_record_payload(record)
        event_id = self.append_event(
            "migration_readiness_recorded",
            "policy",
            record.record_id,
            payload,
            "internal",
        )
        self.conn.execute(
            """
            INSERT INTO migration_readiness_records (
              record_id, surface_ref, component_type, ownership_action, owner_domain,
              summary, blockers_json, evidence_refs_json, next_operator_actions_json,
              readiness_status, live_controls_enabled, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
              live_controls_enabled=excluded.live_controls_enabled,
              created_at=excluded.created_at
            """,
            (
                record.record_id,
                record.surface_ref,
                record.component_type,
                record.ownership_action,
                record.owner_domain,
                record.summary,
                canonical_json(record.blockers),
                canonical_json(record.evidence_refs),
                canonical_json(record.next_operator_actions),
                record.readiness_status,
                0,
                record.created_at,
            ),
        )
        self.enqueue_projection(event_id, "migration_readiness_record_projection")
        return record.record_id

    def compare_migration_readiness_replay_to_projection(
        self,
        scope: str = "legacy_repo",
    ) -> MigrationReadinessReplayProjectionComparison:
        replay = self.__class__._replay_from_connection(self.conn)
        replay_records = dict(sorted(replay.migration_readiness_records.items()))
        projection_rows = self.conn.execute(
            "SELECT * FROM migration_readiness_records ORDER BY surface_ref"
        ).fetchall()
        projection_records = {
            row["surface_ref"]: _migration_readiness_record_row_payload(row)
            for row in projection_rows
        }
        mismatches: list[str] = []
        if replay_records != projection_records:
            mismatches.append("migration_readiness_records")
        comparison = MigrationReadinessReplayProjectionComparison(
            scope=scope,
            replay_records=replay_records,
            projection_records=projection_records,
            matches=not mismatches,
            mismatches=mismatches,
        )
        payload = _migration_readiness_comparison_payload(comparison)
        event_id = self.append_event(
            "migration_readiness_replay_projection_compared",
            "policy",
            comparison.comparison_id,
            payload,
            "internal",
        )
        self.conn.execute(
            """
            INSERT INTO migration_readiness_replay_projection_comparisons (
              comparison_id, scope, replay_records_json, projection_records_json,
              matches, mismatches_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                comparison.comparison_id,
                comparison.scope,
                canonical_json(comparison.replay_records),
                canonical_json(comparison.projection_records),
                1 if comparison.matches else 0,
                canonical_json(comparison.mismatches),
                comparison.created_at,
            ),
        )
        self.enqueue_projection(event_id, "migration_readiness_replay_projection_comparison_projection")
        return comparison
