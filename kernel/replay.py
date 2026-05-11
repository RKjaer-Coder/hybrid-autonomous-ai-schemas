from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any

KERNEL_EVENT_SCHEMA_VERSION = 1
KERNEL_POLICY_VERSION = "v3.1-foundation"

LEGACY_BOUNDARIES: dict[str, str] = {
    "immune": "adapt: safety validation and broker-bypass helper only",
    "financial_router": "adapt: route and spend helper subordinate to kernel budgets",
    "skills/local_forward_proxy.py": "adapt: network/provider proxy behind grants",
    "council": "adapt: deliberation recommendation only",
    "eval": "adapt: replay/eval substrate, not promotion authority yet",
    "harness_variants.py": "adapt: eval substrate behind kernel decisions",
    "kernel/runtime_compat.py": "wrap: CLI/proof compatibility harness, never kernel authority",
    "skills/runtime.py": "wrap: thin compatibility entrypoint for kernel runtime",
    "schemas/*.sql": "convert-to-projection: legacy domain schemas are non-authoritative",
}


def create_kernel_database(db_path: str | Path) -> None:
    root = Path(__file__).resolve().parents[1]
    schema_path = root / "schemas" / "kernel.sql"
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA foreign_keys=ON;")
        conn.executescript(schema_path.read_text(encoding="utf-8"))
        conn.commit()


@dataclass
class ReplayState:
    budgets: dict[str, dict[str, Any]] = field(default_factory=dict)
    grants: dict[str, dict[str, Any]] = field(default_factory=dict)
    side_effects: dict[str, dict[str, Any]] = field(default_factory=dict)
    artifact_refs: dict[str, dict[str, Any]] = field(default_factory=dict)
    artifact_governance_records: dict[str, dict[str, Any]] = field(default_factory=dict)
    artifact_payload_metadata: dict[str, dict[str, Any]] = field(default_factory=dict)
    artifact_lifecycle_task_packets: dict[str, dict[str, Any]] = field(default_factory=dict)
    artifact_lifecycle_replay_projection_comparisons: dict[str, dict[str, Any]] = field(default_factory=dict)
    encrypted_storage_descriptors: dict[str, dict[str, Any]] = field(default_factory=dict)
    encrypted_storage_key_rotations: dict[str, dict[str, Any]] = field(default_factory=dict)
    payload_access_receipts: dict[str, dict[str, Any]] = field(default_factory=dict)
    encrypted_storage_access_verification_states: dict[str, dict[str, Any]] = field(default_factory=dict)
    encrypted_storage_replay_projection_comparisons: dict[str, dict[str, Any]] = field(default_factory=dict)
    backup_cadence_records: dict[str, dict[str, Any]] = field(default_factory=dict)
    restore_drill_packets: dict[str, dict[str, Any]] = field(default_factory=dict)
    recovery_checklist_receipts: dict[str, dict[str, Any]] = field(default_factory=dict)
    recovery_verification_states: dict[str, dict[str, Any]] = field(default_factory=dict)
    recovery_replay_projection_comparisons: dict[str, dict[str, Any]] = field(default_factory=dict)
    recovery_readiness_packets: dict[str, dict[str, Any]] = field(default_factory=dict)
    recovery_readiness_replay_projection_comparisons: dict[str, dict[str, Any]] = field(default_factory=dict)
    hermes_adapter_readiness_packets: dict[str, dict[str, Any]] = field(default_factory=dict)
    hermes_adapter_readiness_replay_projection_comparisons: dict[str, dict[str, Any]] = field(default_factory=dict)
    migration_readiness_records: dict[str, dict[str, Any]] = field(default_factory=dict)
    migration_readiness_replay_projection_comparisons: dict[str, dict[str, Any]] = field(default_factory=dict)
    research_requests: dict[str, dict[str, Any]] = field(default_factory=dict)
    source_plans: dict[str, dict[str, Any]] = field(default_factory=dict)
    source_acquisition_checks: dict[str, dict[str, Any]] = field(default_factory=dict)
    decisions: dict[str, dict[str, Any]] = field(default_factory=dict)
    quality_gate_events: dict[str, dict[str, Any]] = field(default_factory=dict)
    evidence_bundles: dict[str, dict[str, Any]] = field(default_factory=dict)
    commercial_decision_packets: dict[str, dict[str, Any]] = field(default_factory=dict)
    commercial_decision_recommendations: dict[str, dict[str, Any]] = field(default_factory=dict)
    projects: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_tasks: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_task_assignments: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_outcomes: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_artifact_receipts: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_customer_feedback: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_revenue_attributions: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_operator_load: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_commercial_rollups: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_status_rollups: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_close_decision_packets: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_replay_projection_comparisons: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_portfolio_decision_packets: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_portfolio_replay_projection_comparisons: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_scheduling_intents: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_scheduling_priority_change_packets: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_scheduling_priority_replay_projection_comparisons: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_scheduling_replay_projection_comparisons: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_customer_visible_packets: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_customer_commitments: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_customer_commitment_receipts: dict[str, dict[str, Any]] = field(default_factory=dict)
    project_customer_visible_replay_projection_comparisons: dict[str, dict[str, Any]] = field(default_factory=dict)
    model_task_classes: dict[str, dict[str, Any]] = field(default_factory=dict)
    model_candidates: dict[str, dict[str, Any]] = field(default_factory=dict)
    local_offload_eval_sets: dict[str, dict[str, Any]] = field(default_factory=dict)
    holdout_policies: dict[str, dict[str, Any]] = field(default_factory=dict)
    holdout_use_records: dict[str, dict[str, Any]] = field(default_factory=dict)
    model_eval_runs: dict[str, dict[str, Any]] = field(default_factory=dict)
    model_route_decisions: dict[str, dict[str, Any]] = field(default_factory=dict)
    model_promotion_decision_packets: dict[str, dict[str, Any]] = field(default_factory=dict)
    model_demotion_records: dict[str, dict[str, Any]] = field(default_factory=dict)
    model_routing_state: dict[str, dict[str, Any]] = field(default_factory=dict)
    inspection_tasks: list[dict[str, Any]] = field(default_factory=list)



def apply_replay_event(state: ReplayState, event_type: str, entity_id: str, payload: dict[str, Any]) -> None:
    if event_type == "budget_created":
        state.budgets[entity_id] = {
            "cap_usd": Decimal(payload["cap_usd"]),
            "spent_usd": Decimal(payload["spent_usd"]),
            "reserved_usd": Decimal(payload["reserved_usd"]),
            "status": payload["status"],
        }
    elif event_type == "spend_reserved":
        budget = state.budgets[payload["budget_id"]]
        budget["reserved_usd"] += Decimal(payload["amount_usd"])
    elif event_type == "capability_granted":
        state.grants[entity_id] = dict(payload)
    elif event_type == "capability_used":
        state.grants[entity_id]["used_count"] += 1
    elif event_type == "artifact_ref_created":
        state.artifact_refs[entity_id] = dict(payload)
    elif event_type == "artifact_governance_recorded":
        state.artifact_governance_records[entity_id] = dict(payload)
        artifact = state.artifact_refs.get(payload["artifact_id"])
        if artifact is not None and payload["status"] == "applied":
            if payload["action"] == "quarantine":
                artifact["encryption_status"] = "quarantined"
            elif payload["action"] in {"delete", "crypto_shred"}:
                artifact["encryption_status"] = "deleted"
    elif event_type == "artifact_payload_metadata_recorded":
        state.artifact_payload_metadata[entity_id] = dict(payload)
    elif event_type == "artifact_lifecycle_task_packet_created":
        state.artifact_lifecycle_task_packets[entity_id] = dict(payload)
        metadata = state.artifact_payload_metadata.get(payload["metadata_id"])
        if metadata is not None and payload["action"] in {"delete", "crypto_shred"}:
            metadata["status"] = "deletion_due"
            metadata["updated_at"] = payload["created_at"]
    elif event_type == "artifact_lifecycle_task_completed":
        packet = state.artifact_lifecycle_task_packets[entity_id]
        packet["status"] = payload["status"]
        packet["receipt_ref"] = payload["receipt_ref"]
        packet["receipt_hash"] = payload["receipt_hash"]
        packet["completed_at"] = payload["completed_at"]
        metadata = state.artifact_payload_metadata.get(payload["metadata_id"])
        if metadata is not None and payload["status"] == "completed":
            if payload["action"] == "quarantine":
                metadata["status"] = "quarantined"
                metadata["encryption_status"] = "quarantined"
            elif payload["action"] == "delete":
                metadata["status"] = "deleted"
                metadata["encryption_status"] = "deleted"
            elif payload["action"] == "crypto_shred":
                metadata["status"] = "crypto_shredded"
                metadata["encryption_status"] = "deleted"
            metadata["updated_at"] = payload["completed_at"]
    elif event_type == "artifact_lifecycle_replay_projection_compared":
        state.artifact_lifecycle_replay_projection_comparisons[entity_id] = dict(payload)
    elif event_type == "encrypted_storage_descriptor_recorded":
        state.encrypted_storage_descriptors[entity_id] = dict(payload)
    elif event_type == "encrypted_storage_key_rotated":
        state.encrypted_storage_key_rotations[entity_id] = dict(payload)
        descriptor = state.encrypted_storage_descriptors.get(payload["descriptor_id"])
        if descriptor is not None and payload["status"] == "applied":
            descriptor["key_ref"] = payload["new_key_ref"]
            descriptor["key_version"] = payload["new_key_version"]
            descriptor["key_status"] = "rotated"
            descriptor["status"] = "rotated"
            descriptor["updated_at"] = payload["created_at"]
    elif event_type == "payload_access_receipt_recorded":
        state.payload_access_receipts[entity_id] = dict(payload)
        descriptor = state.encrypted_storage_descriptors.get(payload["descriptor_id"])
        if descriptor is not None and payload["verification_status"] in {"failed", "blocked"}:
            descriptor["status"] = "inaccessible"
            descriptor["updated_at"] = payload["created_at"]
    elif event_type == "encrypted_storage_access_verification_recorded":
        state.encrypted_storage_access_verification_states[entity_id] = dict(payload)
        descriptor = state.encrypted_storage_descriptors.get(payload["descriptor_id"])
        if descriptor is not None and payload["fail_closed"]:
            descriptor["status"] = "inaccessible"
            descriptor["updated_at"] = payload["verified_at"]
    elif event_type == "encrypted_storage_replay_projection_compared":
        state.encrypted_storage_replay_projection_comparisons[entity_id] = dict(payload)
    elif event_type == "backup_cadence_recorded":
        state.backup_cadence_records[entity_id] = dict(payload)
    elif event_type == "restore_drill_packet_created":
        state.restore_drill_packets[entity_id] = dict(payload)
    elif event_type == "recovery_checklist_receipt_recorded":
        state.recovery_checklist_receipts[entity_id] = dict(payload)
    elif event_type == "recovery_verification_state_recorded":
        state.recovery_verification_states[entity_id] = dict(payload)
        packet = state.restore_drill_packets.get(payload["drill_id"])
        if packet is not None:
            packet["status"] = payload["status"]
            packet["completed_at"] = payload["verified_at"]
    elif event_type == "recovery_replay_projection_compared":
        state.recovery_replay_projection_comparisons[entity_id] = dict(payload)
    elif event_type == "recovery_readiness_packet_created":
        state.recovery_readiness_packets[entity_id] = dict(payload)
    elif event_type == "recovery_readiness_replay_projection_compared":
        state.recovery_readiness_replay_projection_comparisons[entity_id] = dict(payload)
    elif event_type == "hermes_adapter_readiness_packet_created":
        state.hermes_adapter_readiness_packets[entity_id] = dict(payload)
    elif event_type == "hermes_adapter_readiness_replay_projection_compared":
        state.hermes_adapter_readiness_replay_projection_comparisons[entity_id] = dict(payload)
    elif event_type == "migration_readiness_recorded":
        state.migration_readiness_records[payload["surface_ref"]] = dict(payload)
    elif event_type == "migration_readiness_replay_projection_compared":
        state.migration_readiness_replay_projection_comparisons[entity_id] = dict(payload)
    elif event_type == "research_request_created":
        state.research_requests[entity_id] = dict(payload)
    elif event_type == "research_request_transitioned":
        request = state.research_requests[entity_id]
        request["status"] = payload["status"]
        request["updated_at"] = payload["updated_at"]
    elif event_type == "source_plan_created":
        state.source_plans[entity_id] = dict(payload)
    elif event_type == "source_acquisition_checked":
        state.source_acquisition_checks[entity_id] = dict(payload)
    elif event_type == "decision_recorded":
        state.decisions[entity_id] = dict(payload)
    elif event_type == "decision_resolved":
        decision = state.decisions[entity_id]
        decision["status"] = payload["status"]
        decision["verdict"] = payload["verdict"]
        decision["confidence"] = payload["confidence"]
        decision["decided_at"] = payload["decided_at"]
        decision["resolution"] = dict(payload)
    elif event_type == "quality_gate_evaluated":
        state.quality_gate_events[entity_id] = dict(payload)
    elif event_type == "evidence_bundle_committed":
        state.evidence_bundles[entity_id] = dict(payload)
        state.research_requests[payload["request_id"]]["status"] = "completed"
        state.research_requests[payload["request_id"]]["updated_at"] = payload["created_at"]
    elif event_type == "commercial_decision_packet_created":
        state.commercial_decision_packets[entity_id] = dict(payload)
    elif event_type == "commercial_decision_recommendation_recorded":
        state.commercial_decision_recommendations[entity_id] = dict(payload)
    elif event_type == "project_created":
        state.projects[entity_id] = dict(payload)
    elif event_type == "project_task_created":
        state.project_tasks[entity_id] = dict(payload)
    elif event_type == "project_task_assigned":
        state.project_task_assignments[entity_id] = dict(payload)
        if payload["status"] == "accepted" and payload["task_id"] in state.project_tasks:
            task = state.project_tasks[payload["task_id"]]
            if task["status"] in {"queued", "blocked"}:
                task["status"] = "running"
                task["updated_at"] = payload["assigned_at"]
                task["last_assignment_id"] = entity_id
    elif event_type == "project_task_transitioned":
        task = state.project_tasks[entity_id]
        task["status"] = payload["status"]
        task["updated_at"] = payload["updated_at"]
        task["last_transition"] = dict(payload)
    elif event_type == "project_outcome_recorded":
        state.project_outcomes[entity_id] = dict(payload)
        if payload.get("task_id") in state.project_tasks:
            task = state.project_tasks[payload["task_id"]]
            if task["status"] not in {"completed", "failed", "cancelled"}:
                task["status"] = "completed"
                task["updated_at"] = payload["created_at"]
    elif event_type == "project_artifact_receipt_recorded":
        state.project_artifact_receipts[entity_id] = dict(payload)
    elif event_type == "project_customer_feedback_recorded":
        state.project_customer_feedback[entity_id] = dict(payload)
    elif event_type == "project_revenue_attribution_recorded":
        state.project_revenue_attributions[entity_id] = dict(payload)
    elif event_type == "project_operator_load_recorded":
        state.project_operator_load[entity_id] = dict(payload)
    elif event_type == "project_commercial_rollup_derived":
        state.project_commercial_rollups[entity_id] = dict(payload)
    elif event_type == "project_status_rollup_derived":
        state.project_status_rollups[entity_id] = dict(payload)
    elif event_type == "project_close_decision_packet_created":
        state.project_close_decision_packets[entity_id] = dict(payload)
    elif event_type == "project_close_decision_resolved":
        packet = state.project_close_decision_packets[payload["packet_id"]]
        packet["status"] = "decided"
        packet["verdict"] = payload["verdict"]
        packet["decided_by"] = payload["decided_by"]
        packet["decided_at"] = payload["decided_at"]
        packet["followup_task_id"] = payload.get("followup_task_id")
        project = state.projects[payload["project_id"]]
        project["status"] = payload["project_status"]
        project["updated_at"] = payload["updated_at"]
        project["last_close_decision_packet_id"] = payload["packet_id"]
    elif event_type == "project_replay_projection_compared":
        state.project_replay_projection_comparisons[entity_id] = dict(payload)
    elif event_type == "project_portfolio_decision_packet_created":
        state.project_portfolio_decision_packets[entity_id] = dict(payload)
    elif event_type == "project_portfolio_decision_resolved":
        packet = state.project_portfolio_decision_packets[payload["packet_id"]]
        packet["status"] = "decided"
        packet["verdict"] = payload["verdict"]
        packet["decided_by"] = payload["decided_by"]
        packet["decided_at"] = payload["decided_at"]
    elif event_type == "project_portfolio_replay_projection_compared":
        state.project_portfolio_replay_projection_comparisons[entity_id] = dict(payload)
    elif event_type == "project_scheduling_intent_recorded":
        state.project_scheduling_intents[entity_id] = dict(payload)
    elif event_type == "project_scheduling_priority_change_packet_created":
        state.project_scheduling_priority_change_packets[entity_id] = dict(payload)
    elif event_type == "project_scheduling_priority_change_packet_resolved":
        packet = state.project_scheduling_priority_change_packets[payload["packet_id"]]
        packet["status"] = "decided"
        packet["verdict"] = payload["verdict"]
        packet["decided_by"] = payload["decided_by"]
        packet["decided_at"] = payload["decided_at"]
        packet["applied_changes"] = payload["applied_changes"]
    elif event_type == "project_scheduling_priority_replay_projection_compared":
        state.project_scheduling_priority_replay_projection_comparisons[entity_id] = dict(payload)
    elif event_type == "project_scheduling_replay_projection_compared":
        state.project_scheduling_replay_projection_comparisons[entity_id] = dict(payload)
    elif event_type == "project_customer_visible_packet_created":
        state.project_customer_visible_packets[entity_id] = dict(payload)
    elif event_type == "project_customer_visible_packet_resolved":
        packet = state.project_customer_visible_packets[payload["packet_id"]]
        packet["status"] = "decided"
        packet["verdict"] = payload["verdict"]
        packet["decided_by"] = payload["decided_by"]
        packet["decided_at"] = payload["decided_at"]
    elif event_type == "project_customer_commitment_recorded":
        state.project_customer_commitments[entity_id] = dict(payload)
    elif event_type == "project_customer_commitment_receipt_recorded":
        state.project_customer_commitment_receipts[entity_id] = dict(payload)
    elif event_type == "project_customer_commitment_receipt_followup_completed":
        receipt = state.project_customer_commitment_receipts.get(payload["receipt_id"])
        if receipt is not None:
            receipt["action_required"] = False
            receipt["status"] = "accepted"
            receipt["followup_task_id"] = payload["followup_task_id"]
    elif event_type == "project_customer_visible_replay_projection_compared":
        state.project_customer_visible_replay_projection_comparisons[entity_id] = dict(payload)
    elif event_type == "model_task_class_registered":
        state.model_task_classes[entity_id] = dict(payload)
    elif event_type == "model_candidate_registered":
        state.model_candidates[entity_id] = dict(payload)
    elif event_type == "model_holdout_policy_created":
        state.holdout_policies[entity_id] = dict(payload)
    elif event_type == "local_offload_eval_set_registered":
        state.local_offload_eval_sets[entity_id] = dict(payload)
    elif event_type == "model_holdout_use_recorded":
        state.holdout_use_records[entity_id] = dict(payload)
    elif event_type == "model_eval_run_recorded":
        state.model_eval_runs[entity_id] = dict(payload)
    elif event_type == "model_route_decision_recorded":
        state.model_route_decisions[entity_id] = dict(payload)
    elif event_type == "model_promotion_decision_packet_created":
        state.model_promotion_decision_packets[entity_id] = dict(payload)
    elif event_type == "model_demoted":
        state.model_demotion_records[entity_id] = dict(payload)
        state.model_candidates[payload["model_id"]]["promotion_state"] = "demoted"
        state.model_candidates[payload["model_id"]]["last_verified_at"] = payload["created_at"]
        for routing_state in payload["routing_state_after"]:
            state.model_routing_state[routing_state["state_id"]] = dict(routing_state)
    elif event_type == "side_effect_intent_prepared":
        state.side_effects[entity_id] = {"intent": dict(payload), "receipt": None}
    elif event_type == "side_effect_receipt_recorded":
        intent_id = payload["intent_id"]
        state.side_effects.setdefault(intent_id, {"intent": None, "receipt": None})
        state.side_effects[intent_id]["receipt"] = dict(payload)
        if payload["receipt_type"] in {"failure", "timeout", "compensation_needed"}:
            state.inspection_tasks.append(
                {
                    "intent_id": intent_id,
                    "reason": payload["receipt_type"],
                    "replay_action": "inspect_or_compensate",
                }
            )
    elif event_type in {"projection_outbox_enqueued"}:
        return
    else:
        raise ValueError(f"unknown critical event type: {event_type}")
