from __future__ import annotations

import sqlite3
from decimal import Decimal
from typing import Any

from .records import (
    ArtifactGovernanceRecord,
    ArtifactLifecycleReplayProjectionComparison,
    ArtifactLifecycleTaskPacket,
    ArtifactPayloadMetadata,
    ArtifactRef,
    BackupCadenceRecord,
    Budget,
    CapabilityGrant,
    Command,
    CommercialDecisionRecommendationRecord,
    Decision,
    EncryptedStorageAccessVerificationState,
    EncryptedStorageDescriptor,
    EncryptedStorageKeyRotationRecord,
    EncryptedStorageReplayProjectionComparison,
    EvidenceBundle,
    Event,
    HoldoutPolicy,
    HoldoutUseRecord,
    LocalOffloadEvalSet,
    ModelCandidate,
    ModelDemotionRecord,
    ModelEvalRun,
    ModelPromotionDecisionPacket,
    ModelRouteDecision,
    ModelTaskClassRecord,
    OpportunityProjectDecisionPacket,
    Project,
    ProjectArtifactReceipt,
    ProjectCommercialRollup,
    ProjectCloseDecisionPacket,
    ProjectCustomerCommitment,
    ProjectCustomerCommitmentReceipt,
    ProjectCustomerFeedback,
    ProjectCustomerVisiblePacket,
    ProjectCustomerVisibleReplayProjectionComparison,
    ProjectOperatorLoadRecord,
    ProjectOutcome,
    ProjectPhaseRollup,
    ProjectPortfolioDecisionPacket,
    ProjectPortfolioReplayProjectionComparison,
    ProjectReplayProjectionComparison,
    ProjectRevenueAttribution,
    ProjectSchedulingIntent,
    ProjectSchedulingPriorityChangePacket,
    ProjectSchedulingPriorityReplayProjectionComparison,
    ProjectSchedulingReplayProjectionComparison,
    ProjectStatusRollup,
    ProjectTask,
    ProjectTaskAssignment,
    PayloadAccessReceipt,
    RecoveryChecklistReceipt,
    RecoveryReadinessPacket,
    RecoveryReadinessReplayProjectionComparison,
    RecoveryReplayProjectionComparison,
    RecoveryVerificationState,
    ResearchRequest,
    RestoreDrillPacket,
    SourceAcquisitionCheck,
    SourcePlan,
    SideEffectIntent,
    SideEffectReceipt,
    canonical_json,
    new_id,
    now_iso,
    payload_hash,
    sha256_text,
)
from .replay import KERNEL_POLICY_VERSION, ReplayState
from .store_common import (
    _loads,
    _project_payload,
    _project_task_payload,
    _project_task_assignment_payload,
    _project_outcome_payload,
    _project_artifact_receipt_payload,
    _project_customer_feedback_payload,
    _project_revenue_attribution_payload,
    _project_operator_load_payload,
    _operate_followup_type,
    _commitment_receipt_followup_type,
    _project_commercial_rollup_payload,
    _project_status_rollup_payload,
    _project_close_decision_packet_payload,
    _project_replay_projection_comparison_payload,
    _project_portfolio_decision_packet_payload,
    _project_portfolio_replay_projection_comparison_payload,
    _project_scheduling_intent_payload,
    _project_scheduling_priority_change_packet_payload,
    _project_scheduling_priority_replay_projection_comparison_payload,
    _project_scheduling_replay_projection_comparison_payload,
    _project_customer_visible_packet_payload,
    _project_customer_commitment_payload,
    _project_customer_commitment_receipt_payload,
    _project_customer_visible_replay_projection_comparison_payload,
    _portfolio_packet_from_row,
    _project_scheduling_intent_from_row,
    _project_scheduling_priority_change_packet_from_row,
    _project_customer_visible_packet_from_row,
    _project_customer_commitment_from_row,
    _project_customer_commitment_receipt_from_row,
    _portfolio_tradeoffs,
    _bounded_queue_adjustment,
    _priority_change_from_adjustment,
    _not_applied_priority_change,
    _priority_change_risk_flags,
    _scheduling_risk_flags,
    _portfolio_risk_flags,
    _portfolio_packet_recommendation,
    _rollup_from_row,
    _commercial_rollup_from_row,
    _latest_replay_project_commercial_rollup,
    _count_by_status,
    _with_ref,
    _merge_refs,
    _decimal_from,
)


class CommercialKernelTransactionMixin:
    def create_project(self, project: Project) -> str:
        if not project.name.strip() or not project.objective.strip():
            raise ValueError("project name and objective are required")
        if project.decision_packet_id:
            packet = self.conn.execute(
                """
                SELECT decision_id, recommendation, status, project_json
                FROM commercial_decision_packets
                WHERE packet_id=?
                """,
                (project.decision_packet_id,),
            ).fetchone()
            if packet is None:
                raise ValueError("project decision packet not found")
            if packet["status"] != "gated":
                raise ValueError("project creation requires a gated commercial decision packet")
            if packet["decision_id"] != project.decision_id:
                raise ValueError("project Decision id must match decision packet")
            decision = self.conn.execute(
                "SELECT status, verdict, required_authority FROM decisions WHERE decision_id=?",
                (project.decision_id,),
            ).fetchone()
            if decision is None:
                raise ValueError("project Decision record not found")
            if decision["required_authority"] != "operator_gate":
                raise PermissionError("G1 project creation requires operator-gate authority")
            if decision["status"] != "decided" or decision["verdict"] != "approve_validation":
                raise PermissionError("G1 project creation requires an approved validation verdict")
        if project.status not in {"proposed", "active"}:
            raise ValueError("new projects must start proposed or active")
        if not project.phases:
            raise ValueError("project requires at least one phase")
        payload = _project_payload(project)
        event_id = self.append_event("project_created", "project", project.project_id, payload)
        self.conn.execute(
            """
            INSERT INTO projects (
              project_id, opportunity_id, decision_packet_id, decision_id,
              name, objective, revenue_mechanism, operator_role,
              external_commitment_policy, budget_id, phases_json,
              success_metrics_json, kill_criteria_json, evidence_refs_json,
              status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                project.project_id,
                project.opportunity_id,
                project.decision_packet_id,
                project.decision_id,
                project.name,
                project.objective,
                project.revenue_mechanism,
                project.operator_role,
                project.external_commitment_policy,
                project.budget_id,
                canonical_json(project.phases),
                canonical_json(project.success_metrics),
                canonical_json(project.kill_criteria),
                canonical_json(project.evidence_refs),
                project.status,
                project.created_at,
                project.updated_at,
            ),
        )
        self.enqueue_projection(event_id, "project_projection")
        return project.project_id

    def create_project_task(self, task: ProjectTask) -> str:
        project = self.conn.execute(
            "SELECT project_id, status, budget_id FROM projects WHERE project_id=?",
            (task.project_id,),
        ).fetchone()
        if project is None:
            raise ValueError("project task requires an existing project")
        if project["status"] not in {"active", "paused", "blocked"}:
            raise ValueError(f"cannot create project task from project status {project['status']}")
        if task.task_type in {"build", "ship"} and task.authority_required not in {"single_agent", "council", "operator_gate"}:
            raise PermissionError("build and ship tasks require assigned non-rule authority")
        if task.task_type == "ship" and task.authority_required != "operator_gate":
            raise PermissionError("shipping tasks require operator-gate authority")
        if task.risk_level in {"high", "critical"} and task.authority_required not in {"council", "operator_gate"}:
            raise PermissionError("high-risk project tasks require council or operator-gate authority")
        if not task.objective.strip():
            raise ValueError("project task objective is required")
        if not task.required_capabilities:
            raise ValueError("project task must declare required capabilities, even when empty-by-policy")
        policy_version = task.policy_version or KERNEL_POLICY_VERSION
        command_id = task.command_id or self.command.command_id
        idempotency_key = task.idempotency_key or self.command.idempotency_key
        payload = _project_task_payload(
            task,
            command_id=command_id,
            policy_version=policy_version,
            idempotency_key=idempotency_key,
        )
        event_id = self.append_event("project_task_created", "task", task.task_id, payload)
        self.conn.execute(
            """
            INSERT INTO project_tasks (
              task_id, project_id, phase_name, task_type, autonomy_class, objective,
              inputs_json, expected_output_schema_json, risk_level,
              required_capabilities_json, model_requirement_json, budget_id,
              deadline, status, authority_required, recovery_policy, command_id,
              policy_version, idempotency_key, evidence_refs_json, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                task.task_id,
                task.project_id,
                task.phase_name,
                task.task_type,
                task.autonomy_class,
                task.objective,
                canonical_json(task.inputs),
                canonical_json(task.expected_output_schema) if task.expected_output_schema is not None else None,
                task.risk_level,
                canonical_json(task.required_capabilities),
                canonical_json(task.model_requirement),
                task.budget_id,
                task.deadline,
                task.status,
                task.authority_required,
                task.recovery_policy,
                command_id,
                policy_version,
                idempotency_key,
                canonical_json(task.evidence_refs),
                task.created_at,
                task.updated_at,
            ),
        )
        self.enqueue_projection(event_id, "project_task_projection")
        return task.task_id

    def assign_project_task(self, assignment: ProjectTaskAssignment) -> str:
        row = self.conn.execute(
            """
            SELECT task_id, project_id, status, required_capabilities_json,
                   inputs_json, budget_id
            FROM project_tasks
            WHERE task_id=?
            """,
            (assignment.task_id,),
        ).fetchone()
        if row is None:
            raise ValueError("project task assignment requires an existing task")
        if row["project_id"] != assignment.project_id:
            raise ValueError("project task assignment project/task mismatch")
        if not assignment.worker_id.strip():
            raise ValueError("project task assignment requires a worker id")
        existing = self.conn.execute(
            """
            SELECT assignment_id, task_id, project_id, worker_type, worker_id,
                   route_decision_id, grant_ids_json, status
            FROM project_task_assignments
            WHERE assignment_id=?
            """,
            (assignment.assignment_id,),
        ).fetchone()
        if existing is None:
            if row["status"] not in {"queued", "blocked"}:
                raise ValueError(f"cannot assign project task from status {row['status']}")
        else:
            if existing["status"] != "assigned":
                raise ValueError(f"cannot resolve project task assignment from status {existing['status']}")
            if assignment.status not in {"accepted", "rejected", "revoked"}:
                raise ValueError("existing project task assignments must resolve to accepted, rejected, or revoked")
            if existing["task_id"] != assignment.task_id or existing["project_id"] != assignment.project_id:
                raise ValueError("project task assignment resolution task/project mismatch")
            if existing["worker_type"] != assignment.worker_type or existing["worker_id"] != assignment.worker_id:
                raise PermissionError("project task assignment resolution worker mismatch")
            if _loads(existing["grant_ids_json"]) != assignment.grant_ids:
                raise PermissionError("project task assignment resolution cannot change grant evidence")
        if assignment.status == "accepted" and not assignment.accepted_capabilities:
            raise ValueError("accepted project task assignment must record accepted capabilities")
        inputs = _loads(row["inputs_json"])
        scheduling_created = bool(inputs.get("scheduling_priority_packet_id"))
        for grant_id in assignment.grant_ids:
            grant = self.conn.execute(
                """
                SELECT task_id, subject_type, subject_id, capability_type,
                       actions_json, status, policy_version
                FROM capability_grants
                WHERE grant_id=?
                """,
                (grant_id,),
            ).fetchone()
            if grant is None:
                raise PermissionError("project task assignment references unknown grant")
            if grant["task_id"] != assignment.task_id:
                raise PermissionError("project task assignment grant/task mismatch")
            if grant["status"] != "active" or grant["policy_version"] != KERNEL_POLICY_VERSION:
                raise PermissionError("project task assignment requires active current-policy grants")
            if scheduling_created and (
                grant["subject_type"] != assignment.worker_type or grant["subject_id"] != assignment.worker_id
            ):
                raise PermissionError("project task assignment grant/worker mismatch")
        self._validate_project_task_assignment_evidence(row, assignment)
        payload = _project_task_assignment_payload(assignment)
        event_id = self.append_event("project_task_assigned", "task", assignment.assignment_id, payload)
        if existing is None:
            self.conn.execute(
                """
                INSERT INTO project_task_assignments (
                  assignment_id, task_id, project_id, worker_type, worker_id,
                  route_decision_id, grant_ids_json, accepted_capabilities_json,
                  status, notes, assigned_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    assignment.assignment_id,
                    assignment.task_id,
                    assignment.project_id,
                    assignment.worker_type,
                    assignment.worker_id,
                    assignment.route_decision_id,
                    canonical_json(assignment.grant_ids),
                    canonical_json(assignment.accepted_capabilities),
                    assignment.status,
                    assignment.notes,
                    assignment.assigned_at,
                ),
            )
        else:
            self.conn.execute(
                """
                UPDATE project_task_assignments
                SET accepted_capabilities_json=?, status=?, notes=?, assigned_at=?
                WHERE assignment_id=?
                """,
                (
                    canonical_json(assignment.accepted_capabilities),
                    assignment.status,
                    assignment.notes,
                    assignment.assigned_at,
                    assignment.assignment_id,
                ),
            )
        if assignment.status == "accepted":
            self.conn.execute(
                "UPDATE project_tasks SET status='running', updated_at=? WHERE task_id=?",
                (assignment.assigned_at, assignment.task_id),
            )
        self.enqueue_projection(event_id, "project_task_assignment_projection")
        return assignment.assignment_id

    def _validate_project_task_assignment_evidence(
        self,
        task_row: sqlite3.Row,
        assignment: ProjectTaskAssignment,
    ) -> None:
        inputs = _loads(task_row["inputs_json"])
        required_capabilities = _loads(task_row["required_capabilities_json"])
        scheduling_created = bool(inputs.get("scheduling_priority_packet_id"))
        if scheduling_created:
            if inputs.get("customer_commitments_allowed") or inputs.get("customer_visible"):
                raise PermissionError("scheduling-created assignment cannot authorize customer commitments")
            if inputs.get("external_side_effects_authorized"):
                raise PermissionError("scheduling-created assignment cannot authorize side effects")
            if not task_row["budget_id"]:
                raise PermissionError("scheduling-created assignment requires durable project budget evidence")
            budget = self.conn.execute(
                """
                SELECT owner_type, owner_id, status, expires_at
                FROM budgets
                WHERE budget_id=?
                """,
                (task_row["budget_id"],),
            ).fetchone()
            if budget is None or budget["owner_type"] != "project" or budget["owner_id"] != task_row["project_id"]:
                raise PermissionError("scheduling-created assignment budget/project evidence mismatch")
            if budget["status"] != "active" or budget["expires_at"] <= now_iso():
                raise PermissionError("scheduling-created assignment requires an active budget")
        if not required_capabilities:
            return
        grant_rows = [
            self.conn.execute(
                """
                SELECT capability_type, actions_json
                FROM capability_grants
                WHERE grant_id=?
                """,
                (grant_id,),
            ).fetchone()
            for grant_id in assignment.grant_ids
        ]
        accepted = assignment.accepted_capabilities
        for required in required_capabilities:
            if not required.get("grant_required_before_run", True):
                continue
            capability_type = required.get("capability_type")
            actions = set(required.get("actions", []))
            has_grant = any(
                grant is not None
                and grant["capability_type"] == capability_type
                and actions.issubset(set(_loads(grant["actions_json"])))
                for grant in grant_rows
            )
            if not has_grant:
                raise PermissionError("project task assignment missing required capability grant evidence")
            if assignment.status == "accepted":
                has_acceptance = any(
                    item.get("capability_type") == capability_type
                    and actions.issubset(set(item.get("actions", [])))
                    for item in accepted
                )
                if not has_acceptance:
                    raise PermissionError("worker acceptance missing required capability evidence")

    def transition_project_task(self, task_id: str, status: str, reason: str) -> str:
        valid_statuses = {"queued", "running", "blocked", "completed", "failed", "cancelled"}
        if status not in valid_statuses:
            raise ValueError(f"unknown project task status: {status}")
        if not reason.strip():
            raise ValueError("project task transition requires a reason")
        row = self.conn.execute("SELECT status, authority_required FROM project_tasks WHERE task_id=?", (task_id,)).fetchone()
        if row is None:
            raise ValueError("project task not found")
        valid_transitions = {
            "queued": {"running", "blocked", "cancelled"},
            "running": {"completed", "blocked", "failed", "cancelled"},
            "blocked": {"running", "failed", "cancelled"},
            "completed": set(),
            "failed": set(),
            "cancelled": set(),
        }
        if status not in valid_transitions[row["status"]]:
            raise ValueError(f"invalid project task transition {row['status']} -> {status}")
        if status == "running":
            assignment = self.conn.execute(
                """
                SELECT assignment_id
                FROM project_task_assignments
                WHERE task_id=? AND status='accepted'
                ORDER BY assigned_at DESC
                LIMIT 1
                """,
                (task_id,),
            ).fetchone()
            if assignment is None:
                raise PermissionError("project tasks require an accepted assignment before running")
        if row["authority_required"] == "operator_gate" and status in {"running", "completed"} and self.command.requested_by != "operator":
            raise PermissionError("operator-gated project tasks require operator transition authority")
        updated_at = now_iso()
        payload = {
            "task_id": task_id,
            "previous_status": row["status"],
            "status": status,
            "reason": reason,
            "updated_at": updated_at,
            "authority_required": row["authority_required"],
        }
        event_id = self.append_event("project_task_transitioned", "task", task_id, payload)
        self.conn.execute(
            "UPDATE project_tasks SET status=?, updated_at=? WHERE task_id=?",
            (status, updated_at, task_id),
        )
        self.enqueue_projection(event_id, "project_task_projection")
        return task_id

    def record_project_followup_delivery(
        self,
        task_id: str,
        *,
        artifact_ref: str,
        summary: str,
        data_class: str = "internal",
        delivery_channel: str = "local_workspace",
        side_effect_intent_id: str | None = None,
        side_effect_receipt_id: str | None = None,
        customer_visible: bool = False,
        metrics: dict[str, Any] | None = None,
        feedback: dict[str, Any] | None = None,
        revenue_impact: dict[str, Any] | None = None,
        operator_load_actual: str | None = None,
        next_recommendation: str | None = None,
    ) -> dict[str, Any]:
        task = self.conn.execute(
            """
            SELECT task_id, project_id, phase_name, task_type, status, authority_required,
                   inputs_json, evidence_refs_json
            FROM project_tasks
            WHERE task_id=?
            """,
            (task_id,),
        ).fetchone()
        if task is None:
            raise ValueError("follow-up delivery requires an existing project task")
        if task["task_type"] not in {"build", "ship"}:
            raise ValueError("follow-up delivery only applies to build or ship tasks")
        if task["status"] != "running":
            raise ValueError("follow-up delivery requires a running assigned task")
        if not artifact_ref.strip() or not summary.strip():
            raise ValueError("follow-up delivery requires an artifact ref and summary")
        if task["task_type"] == "build" and (customer_visible or side_effect_receipt_id):
            raise PermissionError("build artifacts cannot be customer-visible or bind external side effects")
        if task["task_type"] == "ship":
            if self.command.requested_by != "operator" or self.command.requested_authority != "operator_gate":
                raise PermissionError("ship deliveries require operator-gate authority")
            if not side_effect_receipt_id:
                raise PermissionError("ship deliveries require a durable side-effect receipt")
            customer_visible = True

        artifact_kind = "shipped_artifact" if task["task_type"] == "ship" else "build_artifact"
        outcome_type = "shipped_artifact" if task["task_type"] == "ship" else "build_artifact"
        artifact = ProjectArtifactReceipt(
            project_id=task["project_id"],
            task_id=task_id,
            artifact_ref=artifact_ref,
            artifact_kind=artifact_kind,  # type: ignore[arg-type]
            summary=summary,
            data_class=data_class,  # type: ignore[arg-type]
            delivery_channel=delivery_channel,
            side_effect_intent_id=side_effect_intent_id,
            side_effect_receipt_id=side_effect_receipt_id,
            customer_visible=customer_visible,
            status="accepted",
        )
        artifact_receipt_id = self.record_project_artifact_receipt(artifact)

        outcome_feedback = dict(feedback or {})
        if next_recommendation is not None:
            outcome_feedback.setdefault("next_recommendation", next_recommendation)
        outcome = ProjectOutcome(
            project_id=task["project_id"],
            task_id=task_id,
            phase_name=task["phase_name"],
            outcome_type=outcome_type,  # type: ignore[arg-type]
            summary=summary,
            artifact_refs=[artifact_ref, f"kernel:project_artifact_receipts/{artifact_receipt_id}"],
            metrics=dict(metrics or {}),
            feedback=outcome_feedback,
            revenue_impact=dict(revenue_impact or {"amount": 0, "currency": "USD", "period": "one_time"}),
            operator_load_actual=operator_load_actual,
            status="accepted",
        )
        outcome_id = self.record_project_outcome(outcome)
        ship_task_id: str | None = None
        recommendation = (next_recommendation or outcome_feedback.get("next_recommendation") or "").lower()
        if task["task_type"] == "build" and any(term in recommendation for term in ("ship", "publish", "deploy")):
            ship_task_id = self._create_ship_task_from_build_delivery(
                project_id=task["project_id"],
                build_task_id=task_id,
                build_artifact_receipt_id=artifact_receipt_id,
                artifact_ref=artifact_ref,
                summary=summary,
                source_evidence_refs=_loads(task["evidence_refs_json"]),
            )
        return {
            "project_id": task["project_id"],
            "task_id": task_id,
            "artifact_receipt_id": artifact_receipt_id,
            "outcome_id": outcome_id,
            "ship_task_id": ship_task_id,
        }

    def record_project_operate_followup_outcome(
        self,
        task_id: str,
        *,
        summary: str,
        internal_result_ref: str,
        operator_load_minutes: int,
        operator_load_source: str,
        operate_followup_type: str | None = None,
        metrics: dict[str, Any] | None = None,
        result: dict[str, Any] | None = None,
        revenue_impact: dict[str, Any] | None = None,
        side_effect_intent_id: str | None = None,
        side_effect_receipt_id: str | None = None,
        external_commitment_change: bool = False,
        operator_load_notes: str | None = None,
    ) -> dict[str, Any]:
        task = self.conn.execute(
            """
            SELECT task_id, project_id, phase_name, task_type, status, inputs_json,
                   evidence_refs_json
            FROM project_tasks
            WHERE task_id=?
            """,
            (task_id,),
        ).fetchone()
        if task is None:
            raise ValueError("operate follow-up outcome requires an existing project task")
        if task["task_type"] != "operate" or task["phase_name"] != "Operate":
            raise ValueError("operate follow-up outcome only applies to Operate tasks")
        if task["status"] != "running":
            raise ValueError("operate follow-up outcome requires a running assigned task")
        if not summary.strip() or not internal_result_ref.strip():
            raise ValueError("operate follow-up outcome requires a summary and internal result ref")
        if operator_load_minutes < 0:
            raise ValueError("operate follow-up operator load minutes must be non-negative")
        if not operator_load_source.strip():
            raise ValueError("operate follow-up operator load source is required")

        inputs = _loads(task["inputs_json"])
        commitment_receipt_id = inputs.get("customer_commitment_receipt_id")
        commitment_id = inputs.get("commitment_id")
        receipt_row = None
        if commitment_receipt_id:
            receipt_row = self.conn.execute(
                """
                SELECT receipt_id, commitment_id, receipt_type, followup_task_id
                FROM project_customer_commitment_receipts
                WHERE receipt_id=?
                """,
                (commitment_receipt_id,),
            ).fetchone()
            if receipt_row is None:
                raise ValueError("operate follow-up outcome references unknown customer commitment receipt")
            if receipt_row["followup_task_id"] != task_id:
                raise PermissionError("customer commitment receipt follow-up task mismatch")
            if commitment_id and receipt_row["commitment_id"] != commitment_id:
                raise ValueError("customer commitment receipt/commitment mismatch")
        expected_followup_type = operate_followup_type or inputs.get("operate_followup_type")
        if expected_followup_type not in {
            "revenue_reconciliation",
            "retention",
            "maintenance",
            "customer_support",
        }:
            raise ValueError("operate follow-up outcome requires a known follow-up type")
        if operate_followup_type and inputs.get("operate_followup_type") and operate_followup_type != inputs["operate_followup_type"]:
            raise ValueError("operate follow-up outcome type does not match task input")

        resolved_intent_id = side_effect_intent_id
        if side_effect_receipt_id:
            if self.command.requested_by != "operator" or self.command.requested_authority != "operator_gate":
                raise PermissionError("operate follow-up side effects require operator-gate authority")
            side_effect = self._successful_task_side_effect(
                task_id=task_id,
                receipt_id=side_effect_receipt_id,
                intent_id=side_effect_intent_id,
            )
            resolved_intent_id = side_effect["intent_id"]
            external_commitment_change = True
        elif external_commitment_change:
            raise PermissionError("operate follow-up external commitments require a durable side-effect receipt")
        elif side_effect_intent_id:
            if commitment_receipt_id:
                raise PermissionError("customer commitment receipt follow-up side effects require a durable receipt")
            if self.command.requested_by != "operator" or self.command.requested_authority != "operator_gate":
                raise PermissionError("staged operate follow-up side-effect intents require operator-gate authority")
            self._require_task_side_effect_intent(task_id, side_effect_intent_id)

        output_result = dict(result or {})
        output_result.setdefault("operate_followup_type", expected_followup_type)
        output_result.setdefault("internal_result_ref", internal_result_ref)
        output_result["external_commitment_change"] = external_commitment_change
        output_result["side_effect_intent_id"] = resolved_intent_id
        output_result["side_effect_receipt_id"] = side_effect_receipt_id
        output_result["source_feedback_id"] = inputs.get("feedback_id")
        output_result["source_artifact_receipt_id"] = inputs.get("artifact_receipt_id")
        output_result["source_commitment_id"] = commitment_id
        output_result["customer_commitment_receipt_id"] = commitment_receipt_id
        output_result["receipt_type"] = receipt_row["receipt_type"] if receipt_row else inputs.get("receipt_type")
        output_result["source_outcome_id"] = inputs.get("source_outcome_id")
        output_result["evidence_refs"] = _merge_refs(
            _loads(task["evidence_refs_json"]),
            output_result.get("evidence_refs") or [],
            (
                [
                    f"kernel:project_customer_commitments/{commitment_id}",
                    f"kernel:project_customer_commitment_receipts/{commitment_receipt_id}",
                ]
                if commitment_receipt_id and commitment_id
                else []
            ),
        )

        artifact_refs = _merge_refs(
            [
                internal_result_ref,
                f"kernel:project_tasks/{task_id}",
            ],
            output_result["evidence_refs"],
        )
        if inputs.get("feedback_id"):
            artifact_refs.append(f"kernel:project_customer_feedback/{inputs['feedback_id']}")
        if resolved_intent_id:
            artifact_refs.append(f"kernel:side_effect_intents/{resolved_intent_id}")
        if side_effect_receipt_id:
            artifact_refs.append(f"kernel:side_effect_receipts/{side_effect_receipt_id}")

        outcome = ProjectOutcome(
            project_id=task["project_id"],
            task_id=task_id,
            phase_name="Operate",
            outcome_type="operate_followup",
            summary=summary,
            artifact_refs=artifact_refs,
            metrics=dict(metrics or {}),
            feedback=output_result,
            revenue_impact=dict(revenue_impact or {"amount": 0, "currency": "USD", "period": "one_time"}),
            operator_load_actual=f"{operator_load_minutes} minutes",
            side_effect_intent_id=resolved_intent_id,
            side_effect_receipt_id=side_effect_receipt_id,
            status="accepted",
        )
        outcome_id = self.record_project_outcome(outcome)
        if commitment_receipt_id:
            followup_payload = {
                "receipt_id": commitment_receipt_id,
                "commitment_id": commitment_id,
                "project_id": task["project_id"],
                "followup_task_id": task_id,
                "outcome_id": outcome_id,
                "status": "accepted",
                "action_required": False,
            }
            event_id = self.append_event(
                "project_customer_commitment_receipt_followup_completed",
                "project",
                commitment_receipt_id,
                followup_payload,
            )
            self.conn.execute(
                """
                UPDATE project_customer_commitment_receipts
                SET status='accepted', action_required=0
                WHERE receipt_id=? AND followup_task_id=?
                """,
                (commitment_receipt_id, task_id),
            )
            self.enqueue_projection(event_id, "project_customer_commitment_receipt_followup_projection")
        load_type = inputs.get("default_operator_load_type") or {
            "revenue_reconciliation": "reconciliation",
            "retention": "client_sales",
            "maintenance": "maintenance",
            "customer_support": "other",
        }[expected_followup_type]
        load_id = self.record_project_operator_load(
            ProjectOperatorLoadRecord(
                project_id=task["project_id"],
                task_id=task_id,
                outcome_id=outcome_id,
                minutes=operator_load_minutes,
                load_type=load_type,
                source=operator_load_source,
                notes=operator_load_notes or f"Operate follow-up {expected_followup_type} outcome",
            )
        )
        return {
            "project_id": task["project_id"],
            "task_id": task_id,
            "outcome_id": outcome_id,
            "operator_load_id": load_id,
            "operate_followup_type": expected_followup_type,
            "internal_result_ref": internal_result_ref,
            "external_commitment_change": external_commitment_change,
            "side_effect_intent_id": resolved_intent_id,
            "side_effect_receipt_id": side_effect_receipt_id,
        }

    def record_project_scheduling_task_outcome(
        self,
        task_id: str,
        *,
        summary: str,
        internal_result_ref: str,
        result: dict[str, Any] | None = None,
        metrics: dict[str, Any] | None = None,
        revenue_impact: dict[str, Any] | None = None,
        side_effect_intent_id: str | None = None,
        side_effect_receipt_id: str | None = None,
        external_commitment_change: bool = False,
    ) -> dict[str, Any]:
        task = self.conn.execute(
            """
            SELECT task_id, project_id, phase_name, task_type, status, budget_id,
                   inputs_json, evidence_refs_json
            FROM project_tasks
            WHERE task_id=?
            """,
            (task_id,),
        ).fetchone()
        if task is None:
            raise ValueError("scheduling task outcome requires an existing project task")
        inputs = _loads(task["inputs_json"])
        priority_packet_id = inputs.get("scheduling_priority_packet_id")
        if not priority_packet_id:
            raise ValueError("scheduling task outcome only applies to scheduling-created tasks")
        if task["status"] != "running":
            raise PermissionError("scheduling task outcome requires an accepted running assignment")
        if not summary.strip() or not internal_result_ref.strip():
            raise ValueError("scheduling task outcome requires a summary and internal result ref")
        if inputs.get("customer_commitments_allowed") or inputs.get("customer_visible"):
            raise PermissionError("scheduling-created outcomes cannot create customer-visible commitments")
        if inputs.get("external_side_effects_authorized"):
            raise PermissionError("scheduling-created outcomes cannot use autonomous side-effect authority")

        assignment = self.conn.execute(
            """
            SELECT assignment_id, grant_ids_json, accepted_capabilities_json, worker_type,
                   worker_id, assigned_at
            FROM project_task_assignments
            WHERE task_id=? AND status='accepted'
            ORDER BY assigned_at DESC, assignment_id DESC
            LIMIT 1
            """,
            (task_id,),
        ).fetchone()
        if assignment is None:
            raise PermissionError("scheduling task outcomes require accepted assignment evidence")
        grant_ids = _loads(assignment["grant_ids_json"])
        if not grant_ids:
            raise PermissionError("scheduling task outcomes require capability-grant evidence")
        if not task["budget_id"]:
            raise PermissionError("scheduling task outcomes require durable budget evidence")
        budget = self.conn.execute(
            """
            SELECT owner_type, owner_id, status
            FROM budgets
            WHERE budget_id=?
            """,
            (task["budget_id"],),
        ).fetchone()
        if budget is None or budget["owner_type"] != "project" or budget["owner_id"] != task["project_id"]:
            raise PermissionError("scheduling task outcome budget/project evidence mismatch")
        if budget["status"] != "active":
            raise PermissionError("scheduling task outcomes require an active budget")

        resolved_intent_id = side_effect_intent_id
        if side_effect_receipt_id:
            if self.command.requested_by != "operator" or self.command.requested_authority != "operator_gate":
                raise PermissionError("scheduling task side effects require operator-gate authority")
            side_effect = self._successful_task_side_effect(
                task_id=task_id,
                receipt_id=side_effect_receipt_id,
                intent_id=side_effect_intent_id,
            )
            resolved_intent_id = side_effect["intent_id"]
            external_commitment_change = True
        elif external_commitment_change:
            raise PermissionError("scheduling task customer commitments require a durable operator-gated receipt")
        elif side_effect_intent_id:
            raise PermissionError("scheduling task side effects require a durable operator-gated receipt")

        output_result = dict(result or {})
        output_result.setdefault("operate_followup_type", output_result.get("scheduling_outcome_type", "maintenance"))
        output_result.setdefault("internal_result_ref", internal_result_ref)
        output_result["external_commitment_change"] = external_commitment_change
        output_result["side_effect_intent_id"] = resolved_intent_id
        output_result["side_effect_receipt_id"] = side_effect_receipt_id
        output_result["scheduling_priority_packet_id"] = priority_packet_id
        output_result["scheduling_intent_id"] = inputs.get("scheduling_intent_id")
        output_result["portfolio_packet_id"] = inputs.get("portfolio_packet_id")
        output_result["priority_rank"] = inputs.get("priority_rank")
        output_result["queue_action"] = inputs.get("queue_action")
        output_result["assignment_id"] = assignment["assignment_id"]
        output_result["budget_id"] = task["budget_id"]
        output_result["grant_ids"] = grant_ids
        output_result["accepted_capabilities"] = _loads(assignment["accepted_capabilities_json"])
        output_result["worker_type"] = assignment["worker_type"]
        output_result["worker_id"] = assignment["worker_id"]
        output_result["evidence_refs"] = _merge_refs(
            _loads(task["evidence_refs_json"]),
            [
                f"kernel:project_task_assignments/{assignment['assignment_id']}",
                f"kernel:budgets/{task['budget_id']}",
                f"kernel:project_scheduling_priority_change_packets/{priority_packet_id}",
            ],
            [f"kernel:capability_grants/{grant_id}" for grant_id in grant_ids],
        )

        artifact_refs = _merge_refs(
            [internal_result_ref, f"kernel:project_tasks/{task_id}"],
            output_result["evidence_refs"],
        )
        if resolved_intent_id:
            artifact_refs.append(f"kernel:side_effect_intents/{resolved_intent_id}")
        if side_effect_receipt_id:
            artifact_refs.append(f"kernel:side_effect_receipts/{side_effect_receipt_id}")

        outcome = ProjectOutcome(
            project_id=task["project_id"],
            task_id=task_id,
            phase_name=task["phase_name"],
            outcome_type="operate_followup",
            summary=summary,
            artifact_refs=artifact_refs,
            metrics=dict(metrics or {}),
            feedback=output_result,
            revenue_impact=dict(revenue_impact or {"amount": 0, "currency": "USD", "period": "one_time"}),
            side_effect_intent_id=resolved_intent_id,
            side_effect_receipt_id=side_effect_receipt_id,
            status="accepted",
        )
        outcome_id = self.record_project_outcome(outcome)
        return {
            "project_id": task["project_id"],
            "task_id": task_id,
            "outcome_id": outcome_id,
            "internal_result_ref": internal_result_ref,
            "assignment_id": assignment["assignment_id"],
            "budget_id": task["budget_id"],
            "grant_ids": grant_ids,
            "scheduling_priority_packet_id": priority_packet_id,
            "scheduling_intent_id": inputs.get("scheduling_intent_id"),
            "external_commitment_change": external_commitment_change,
            "side_effect_intent_id": resolved_intent_id,
            "side_effect_receipt_id": side_effect_receipt_id,
        }

    def record_project_outcome(self, outcome: ProjectOutcome) -> str:
        project = self.conn.execute("SELECT status FROM projects WHERE project_id=?", (outcome.project_id,)).fetchone()
        if project is None:
            raise ValueError("project outcome requires an existing project")
        if outcome.task_id:
            task = self.conn.execute(
                "SELECT project_id, status FROM project_tasks WHERE task_id=?",
                (outcome.task_id,),
            ).fetchone()
            if task is None:
                raise ValueError("project outcome references unknown task")
            if task["project_id"] != outcome.project_id:
                raise ValueError("project outcome task/project mismatch")
            if task["status"] not in {"running", "completed"}:
                raise ValueError("project outcome task must be running or completed")
        if not outcome.summary.strip():
            raise ValueError("project outcome summary is required")
        payload = _project_outcome_payload(outcome)
        event_id = self.append_event("project_outcome_recorded", "project", outcome.outcome_id, payload)
        self.conn.execute(
            """
            INSERT INTO project_outcomes (
              outcome_id, project_id, task_id, phase_name, outcome_type, summary,
              artifact_refs_json, metrics_json, feedback_json, revenue_impact_json,
              operator_load_actual, side_effect_intent_id, side_effect_receipt_id,
              status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                outcome.outcome_id,
                outcome.project_id,
                outcome.task_id,
                outcome.phase_name,
                outcome.outcome_type,
                outcome.summary,
                canonical_json(outcome.artifact_refs),
                canonical_json(outcome.metrics),
                canonical_json(outcome.feedback),
                canonical_json(outcome.revenue_impact),
                outcome.operator_load_actual,
                outcome.side_effect_intent_id,
                outcome.side_effect_receipt_id,
                outcome.status,
                outcome.created_at,
            ),
        )
        if outcome.task_id:
            self.conn.execute(
                "UPDATE project_tasks SET status='completed', updated_at=? WHERE task_id=? AND status!='completed'",
                (outcome.created_at, outcome.task_id),
            )
        self.enqueue_projection(event_id, "project_outcome_projection")
        return outcome.outcome_id

    def record_project_artifact_receipt(self, receipt: ProjectArtifactReceipt) -> str:
        self._require_project(receipt.project_id)
        if receipt.task_id:
            self._require_project_task(receipt.project_id, receipt.task_id)
        if not receipt.artifact_ref.strip() or not receipt.summary.strip():
            raise ValueError("artifact receipt requires an artifact ref and summary")
        if receipt.customer_visible and receipt.artifact_kind != "shipped_artifact":
            raise ValueError("customer-visible artifact receipts must be shipped artifacts")
        if receipt.artifact_kind == "shipped_artifact" and not receipt.side_effect_receipt_id:
            raise PermissionError("shipped artifacts require a durable side-effect receipt")
        if receipt.side_effect_receipt_id:
            side_effect = self.conn.execute(
                """
                SELECT r.receipt_id, r.intent_id, r.receipt_type, i.task_id
                FROM side_effect_receipts r
                JOIN side_effect_intents i ON i.intent_id = r.intent_id
                WHERE r.receipt_id=?
                """,
                (receipt.side_effect_receipt_id,),
            ).fetchone()
            if side_effect is None:
                raise ValueError("artifact receipt references unknown side-effect receipt")
            if side_effect["receipt_type"] != "success":
                raise PermissionError("shipped artifact receipt requires successful side-effect execution")
            if receipt.task_id and side_effect["task_id"] != receipt.task_id:
                raise ValueError("artifact side-effect task does not match project task")
            if receipt.side_effect_intent_id and side_effect["intent_id"] != receipt.side_effect_intent_id:
                raise ValueError("artifact side-effect intent/receipt mismatch")
        payload = _project_artifact_receipt_payload(receipt)
        event_id = self.append_event(
            "project_artifact_receipt_recorded",
            "artifact",
            receipt.receipt_id,
            payload,
            receipt.data_class,
        )
        self.conn.execute(
            """
            INSERT INTO project_artifact_receipts (
              receipt_id, project_id, task_id, artifact_ref, artifact_kind, summary,
              data_class, delivery_channel, side_effect_intent_id,
              side_effect_receipt_id, customer_visible, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                receipt.receipt_id,
                receipt.project_id,
                receipt.task_id,
                receipt.artifact_ref,
                receipt.artifact_kind,
                receipt.summary,
                receipt.data_class,
                receipt.delivery_channel,
                receipt.side_effect_intent_id,
                receipt.side_effect_receipt_id,
                int(receipt.customer_visible),
                receipt.status,
                receipt.created_at,
            ),
        )
        self.enqueue_projection(event_id, "project_artifact_projection")
        return receipt.receipt_id

    def _successful_task_side_effect(
        self,
        *,
        task_id: str,
        receipt_id: str,
        intent_id: str | None = None,
    ) -> sqlite3.Row:
        side_effect = self.conn.execute(
            """
            SELECT r.receipt_id, r.intent_id, r.receipt_type, i.task_id, i.required_authority
            FROM side_effect_receipts r
            JOIN side_effect_intents i ON i.intent_id = r.intent_id
            WHERE r.receipt_id=?
            """,
            (receipt_id,),
        ).fetchone()
        if side_effect is None:
            raise ValueError("operate follow-up references unknown side-effect receipt")
        if side_effect["receipt_type"] != "success":
            raise PermissionError("operate follow-up side effects require a successful durable receipt")
        if side_effect["task_id"] != task_id:
            raise ValueError("operate follow-up side-effect task does not match project task")
        if side_effect["required_authority"] != "operator_gate":
            raise PermissionError("operate follow-up side effects require operator-gate side-effect authority")
        if intent_id and side_effect["intent_id"] != intent_id:
            raise ValueError("operate follow-up side-effect intent/receipt mismatch")
        return side_effect

    def _require_task_side_effect_intent(self, task_id: str, intent_id: str) -> None:
        intent = self.conn.execute(
            """
            SELECT intent_id, task_id, required_authority
            FROM side_effect_intents
            WHERE intent_id=?
            """,
            (intent_id,),
        ).fetchone()
        if intent is None:
            raise ValueError("operate follow-up references unknown side-effect intent")
        if intent["task_id"] != task_id:
            raise ValueError("operate follow-up side-effect intent task does not match project task")
        if intent["required_authority"] != "operator_gate":
            raise PermissionError("staged operate follow-up side-effect intents require operator-gate authority")

    def record_project_customer_feedback(self, feedback: ProjectCustomerFeedback) -> str:
        self._require_project(feedback.project_id)
        if feedback.task_id:
            self._require_project_task(feedback.project_id, feedback.task_id)
        if feedback.artifact_receipt_id:
            artifact = self.conn.execute(
                "SELECT project_id FROM project_artifact_receipts WHERE receipt_id=?",
                (feedback.artifact_receipt_id,),
            ).fetchone()
            if artifact is None:
                raise ValueError("feedback references unknown artifact receipt")
            if artifact["project_id"] != feedback.project_id:
                raise ValueError("feedback artifact/project mismatch")
        if feedback.source_type == "customer" and not (feedback.customer_ref or feedback.evidence_refs):
            raise ValueError("customer feedback requires a customer ref or evidence reference")
        if not feedback.summary.strip():
            raise ValueError("feedback summary is required")
        payload = _project_customer_feedback_payload(feedback)
        event_id = self.append_event("project_customer_feedback_recorded", "project", feedback.feedback_id, payload)
        self.conn.execute(
            """
            INSERT INTO project_customer_feedback (
              feedback_id, project_id, task_id, artifact_receipt_id, source_type,
              customer_ref, summary, sentiment, evidence_refs_json,
              action_required, operator_review_required, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                feedback.feedback_id,
                feedback.project_id,
                feedback.task_id,
                feedback.artifact_receipt_id,
                feedback.source_type,
                feedback.customer_ref,
                feedback.summary,
                feedback.sentiment,
                canonical_json(feedback.evidence_refs),
                int(feedback.action_required),
                int(feedback.operator_review_required),
                feedback.status,
                feedback.created_at,
            ),
        )
        self.enqueue_projection(event_id, "project_feedback_projection")
        return feedback.feedback_id

    def record_project_customer_commitment_receipt(
        self,
        receipt: ProjectCustomerCommitmentReceipt,
    ) -> dict[str, str | None]:
        if self.command.requested_by in {"agent", "model", "tool"}:
            raise PermissionError("workers cannot record customer commitment receipts")
        if self.command.payload.get("customer_commitment_requested") or self.command.payload.get("external_action_executed"):
            raise PermissionError("commitment receipt ingestion cannot create customer commitments or execute external actions")
        if not receipt.summary.strip():
            raise ValueError("customer commitment receipt summary is required")
        if receipt.receipt_type not in {"customer_response", "delivery_failure", "timeout", "compensation_needed"}:
            raise ValueError("unknown customer commitment receipt type")
        if receipt.source_type not in {"operator", "customer", "platform", "internal_signal"}:
            raise ValueError("unknown customer commitment receipt source type")
        if receipt.receipt_type in {"delivery_failure", "timeout", "compensation_needed"} and not receipt.action_required:
            raise PermissionError("failure, timeout, and compensation receipts require governed follow-up")

        commitment = self.conn.execute(
            """
            SELECT c.*, p.status AS packet_status, p.verdict AS packet_verdict
            FROM project_customer_commitments c
            JOIN project_customer_visible_packets p ON p.packet_id = c.packet_id
            WHERE c.commitment_id=?
            """,
            (receipt.commitment_id,),
        ).fetchone()
        if commitment is None:
            raise ValueError("customer commitment receipt requires an accepted commitment")
        if commitment["project_id"] != receipt.project_id:
            raise ValueError("customer commitment receipt project mismatch")
        if commitment["packet_status"] != "decided" or commitment["packet_verdict"] != "accept_customer_visible_packet":
            raise PermissionError("customer commitment receipts require an accepted customer-visible packet")

        customer_ref = receipt.customer_ref or commitment["customer_ref"]
        evidence_refs = _merge_refs(
            _loads(commitment["evidence_refs_json"]),
            receipt.evidence_refs,
            [
                f"kernel:project_customer_commitments/{receipt.commitment_id}",
                f"kernel:project_customer_visible_packets/{commitment['packet_id']}",
            ],
        )
        followup_task_id = receipt.followup_task_id
        if receipt.action_required and followup_task_id is None:
            followup_task_id = self._create_commitment_receipt_followup_task(
                commitment,
                receipt,
                customer_ref=customer_ref,
                evidence_refs=evidence_refs,
            )
        normalized = ProjectCustomerCommitmentReceipt(
            receipt_id=receipt.receipt_id,
            commitment_id=receipt.commitment_id,
            project_id=receipt.project_id,
            receipt_type=receipt.receipt_type,
            source_type=receipt.source_type,
            customer_ref=customer_ref,
            summary=receipt.summary,
            evidence_refs=evidence_refs,
            action_required=receipt.action_required,
            status=receipt.status,
            followup_task_id=followup_task_id,
            created_at=receipt.created_at,
        )
        payload = _project_customer_commitment_receipt_payload(normalized)
        event_id = self.append_event("project_customer_commitment_receipt_recorded", "project", normalized.receipt_id, payload)
        self.conn.execute(
            """
            INSERT INTO project_customer_commitment_receipts (
              receipt_id, commitment_id, project_id, receipt_type, source_type,
              customer_ref, summary, evidence_refs_json, action_required,
              status, followup_task_id, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                normalized.receipt_id,
                normalized.commitment_id,
                normalized.project_id,
                normalized.receipt_type,
                normalized.source_type,
                normalized.customer_ref,
                normalized.summary,
                canonical_json(normalized.evidence_refs),
                int(normalized.action_required),
                normalized.status,
                normalized.followup_task_id,
                normalized.created_at,
            ),
        )
        self.enqueue_projection(event_id, "project_customer_commitment_receipt_projection")
        return {
            "project_id": normalized.project_id,
            "commitment_id": normalized.commitment_id,
            "receipt_id": normalized.receipt_id,
            "followup_task_id": normalized.followup_task_id,
        }

    def record_project_revenue_attribution(self, attribution: ProjectRevenueAttribution) -> str:
        self._require_project(attribution.project_id)
        if attribution.task_id:
            self._require_project_task(attribution.project_id, attribution.task_id)
        if attribution.outcome_id:
            outcome = self.conn.execute(
                "SELECT project_id FROM project_outcomes WHERE outcome_id=?",
                (attribution.outcome_id,),
            ).fetchone()
            if outcome is None:
                raise ValueError("revenue attribution references unknown outcome")
            if outcome["project_id"] != attribution.project_id:
                raise ValueError("revenue attribution outcome/project mismatch")
        if attribution.artifact_receipt_id:
            artifact = self.conn.execute(
                "SELECT project_id FROM project_artifact_receipts WHERE receipt_id=?",
                (attribution.artifact_receipt_id,),
            ).fetchone()
            if artifact is None:
                raise ValueError("revenue attribution references unknown artifact receipt")
            if artifact["project_id"] != attribution.project_id:
                raise ValueError("revenue attribution artifact/project mismatch")
        if attribution.amount_usd < Decimal("0"):
            raise ValueError("revenue attribution amount must be non-negative")
        if not 0.0 <= attribution.confidence <= 1.0:
            raise ValueError("revenue attribution confidence must be between 0 and 1")
        if attribution.status == "reconciled" and not (attribution.external_ref or attribution.evidence_refs):
            raise ValueError("reconciled revenue attribution requires external ref or evidence")
        reconciliation_task_id = attribution.reconciliation_task_id
        if attribution.status == "needs_reconciliation" and reconciliation_task_id is None:
            task = ProjectTask(
                project_id=attribution.project_id,
                phase_name="Operate",
                task_type="operate",
                autonomy_class="A1",
                objective="Reconcile missing or low-confidence project revenue attribution evidence.",
                inputs={
                    "attribution_id": attribution.attribution_id,
                    "source": attribution.source,
                    "amount_usd": str(attribution.amount_usd),
                    "external_ref": attribution.external_ref,
                },
                expected_output_schema={
                    "type": "object",
                    "required": ["reconciliation_result", "evidence_refs", "operator_load_actual"],
                },
                risk_level="low",
                required_capabilities=[
                    {
                        "capability_type": "memory_write",
                        "actions": ["record"],
                        "scope": "project_revenue_reconciliation",
                        "grant_required_before_run": True,
                    }
                ],
                model_requirement={"task_class": "quick_research_summarization", "local_allowed_only_if_promoted": True},
                authority_required="rule",
                recovery_policy="ask_operator",
            )
            reconciliation_task_id = self.create_project_task(task)
        payload = _project_revenue_attribution_payload(attribution, reconciliation_task_id=reconciliation_task_id)
        event_id = self.append_event("project_revenue_attribution_recorded", "project", attribution.attribution_id, payload)
        self.conn.execute(
            """
            INSERT INTO project_revenue_attributions (
              attribution_id, project_id, task_id, outcome_id, artifact_receipt_id,
              amount_usd, source, attribution_period, external_ref, evidence_refs_json,
              confidence, reconciliation_task_id, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                attribution.attribution_id,
                attribution.project_id,
                attribution.task_id,
                attribution.outcome_id,
                attribution.artifact_receipt_id,
                str(attribution.amount_usd),
                attribution.source,
                attribution.attribution_period,
                attribution.external_ref,
                canonical_json(attribution.evidence_refs),
                attribution.confidence,
                reconciliation_task_id,
                attribution.status,
                attribution.created_at,
            ),
        )
        self.enqueue_projection(event_id, "project_revenue_projection")
        return attribution.attribution_id

    def record_project_operator_load(self, load: ProjectOperatorLoadRecord) -> str:
        self._require_project(load.project_id)
        if load.task_id:
            self._require_project_task(load.project_id, load.task_id)
        if load.outcome_id:
            outcome = self.conn.execute(
                "SELECT project_id FROM project_outcomes WHERE outcome_id=?",
                (load.outcome_id,),
            ).fetchone()
            if outcome is None:
                raise ValueError("operator load references unknown outcome")
            if outcome["project_id"] != load.project_id:
                raise ValueError("operator load outcome/project mismatch")
        if load.artifact_receipt_id:
            artifact = self.conn.execute(
                "SELECT project_id FROM project_artifact_receipts WHERE receipt_id=?",
                (load.artifact_receipt_id,),
            ).fetchone()
            if artifact is None:
                raise ValueError("operator load references unknown artifact receipt")
            if artifact["project_id"] != load.project_id:
                raise ValueError("operator load artifact/project mismatch")
        if load.minutes < 0:
            raise ValueError("operator load minutes must be non-negative")
        if not load.source.strip():
            raise ValueError("operator load source is required")
        payload = _project_operator_load_payload(load)
        event_id = self.append_event("project_operator_load_recorded", "project", load.load_id, payload)
        self.conn.execute(
            """
            INSERT INTO project_operator_load (
              load_id, project_id, task_id, outcome_id, artifact_receipt_id, minutes,
              load_type, source, notes, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                load.load_id,
                load.project_id,
                load.task_id,
                load.outcome_id,
                load.artifact_receipt_id,
                load.minutes,
                load.load_type,
                load.source,
                load.notes,
                load.created_at,
            ),
        )
        self.enqueue_projection(event_id, "project_operator_load_projection")
        return load.load_id

    def record_project_post_ship_evidence(
        self,
        artifact_receipt_id: str,
        *,
        feedback: ProjectCustomerFeedback,
        revenue: ProjectRevenueAttribution,
        operator_load: ProjectOperatorLoadRecord,
    ) -> dict[str, str]:
        artifact = self.conn.execute(
            """
            SELECT receipt_id, project_id, task_id, artifact_kind, customer_visible,
                   side_effect_receipt_id, status
            FROM project_artifact_receipts
            WHERE receipt_id=?
            """,
            (artifact_receipt_id,),
        ).fetchone()
        if artifact is None:
            raise ValueError("post-ship evidence requires a shipped artifact receipt")
        if not artifact["side_effect_receipt_id"]:
            raise PermissionError("post-ship evidence requires a shipped artifact with side-effect authority")
        if artifact["artifact_kind"] != "shipped_artifact" or not artifact["customer_visible"]:
            raise ValueError("post-ship evidence must be tied to a customer-visible shipped artifact")
        if artifact["status"] != "accepted":
            raise ValueError("post-ship evidence requires an accepted shipped artifact")
        for label, record in {
            "feedback": feedback,
            "revenue": revenue,
            "operator_load": operator_load,
        }.items():
            if record.project_id != artifact["project_id"]:
                raise ValueError(f"post-ship {label} project mismatch")
            if record.task_id and record.task_id != artifact["task_id"]:
                raise ValueError(f"post-ship {label} task mismatch")
            if getattr(record, "artifact_receipt_id", None) and getattr(record, "artifact_receipt_id") != artifact_receipt_id:
                raise ValueError(f"post-ship {label} artifact mismatch")
        feedback_id = self.record_project_customer_feedback(
            ProjectCustomerFeedback(
                feedback_id=feedback.feedback_id,
                project_id=feedback.project_id,
                task_id=feedback.task_id or artifact["task_id"],
                artifact_receipt_id=artifact_receipt_id,
                source_type=feedback.source_type,
                customer_ref=feedback.customer_ref,
                summary=feedback.summary,
                sentiment=feedback.sentiment,
                evidence_refs=_with_ref(feedback.evidence_refs, f"kernel:project_artifact_receipts/{artifact_receipt_id}"),
                action_required=feedback.action_required,
                operator_review_required=feedback.operator_review_required,
                status=feedback.status,
                created_at=feedback.created_at,
            )
        )
        revenue_id = self.record_project_revenue_attribution(
            ProjectRevenueAttribution(
                attribution_id=revenue.attribution_id,
                project_id=revenue.project_id,
                task_id=revenue.task_id or artifact["task_id"],
                outcome_id=revenue.outcome_id,
                artifact_receipt_id=artifact_receipt_id,
                amount_usd=revenue.amount_usd,
                source=revenue.source,
                attribution_period=revenue.attribution_period,
                external_ref=revenue.external_ref,
                evidence_refs=_with_ref(revenue.evidence_refs, f"kernel:project_artifact_receipts/{artifact_receipt_id}"),
                confidence=revenue.confidence,
                reconciliation_task_id=revenue.reconciliation_task_id,
                status=revenue.status,
                created_at=revenue.created_at,
            )
        )
        load_id = self.record_project_operator_load(
            ProjectOperatorLoadRecord(
                load_id=operator_load.load_id,
                project_id=operator_load.project_id,
                task_id=operator_load.task_id or artifact["task_id"],
                outcome_id=operator_load.outcome_id,
                artifact_receipt_id=artifact_receipt_id,
                minutes=operator_load.minutes,
                load_type=operator_load.load_type,
                source=operator_load.source,
                notes=operator_load.notes,
                created_at=operator_load.created_at,
            )
        )
        return {
            "project_id": artifact["project_id"],
            "artifact_receipt_id": artifact_receipt_id,
            "feedback_id": feedback_id,
            "revenue_attribution_id": revenue_id,
            "operator_load_id": load_id,
        }

    def _derive_project_commercial_rollup(self, project_id: str) -> ProjectCommercialRollup:
        rows = self.conn.execute(
            """
            SELECT outcome_id, task_id, summary, artifact_refs_json, feedback_json,
                   revenue_impact_json, side_effect_intent_id, side_effect_receipt_id
            FROM project_outcomes
            WHERE project_id=? AND outcome_type='operate_followup' AND status='accepted'
            ORDER BY created_at, outcome_id
            """,
            (project_id,),
        ).fetchall()
        revenue_reconciled = Decimal("0")
        revenue_unreconciled = Decimal("0")
        retained = 0
        at_risk = 0
        churned = 0
        support_resolved = 0
        support_open = 0
        maintenance_resolved = 0
        maintenance_open = 0
        external_commitments = 0
        receiptless_side_effects = 0
        evidence_refs: list[str] = []
        risk_flags: list[str] = []

        for row in rows:
            feedback = _loads(row["feedback_json"])
            revenue_impact = _loads(row["revenue_impact_json"])
            followup_type = feedback.get("operate_followup_type")
            evidence_refs = _merge_refs(
                evidence_refs,
                _loads(row["artifact_refs_json"]),
                feedback.get("evidence_refs") or [],
                [f"kernel:project_outcomes/{row['outcome_id']}"],
            )
            if row["side_effect_intent_id"] and not row["side_effect_receipt_id"]:
                receiptless_side_effects += 1
                if "receiptless_operate_side_effect_intent" not in risk_flags:
                    risk_flags.append("receiptless_operate_side_effect_intent")
            if feedback.get("external_commitment_change") and row["side_effect_receipt_id"]:
                external_commitments += 1

            if followup_type == "revenue_reconciliation":
                amount = _decimal_from(revenue_impact.get("amount_usd", revenue_impact.get("amount", "0")))
                reconciled = feedback.get("reconciliation_status") == "reconciled" or feedback.get("revenue_status") == "reconciled"
                if reconciled:
                    revenue_reconciled += amount
                else:
                    revenue_unreconciled += amount
                    if amount and "unreconciled_operate_revenue" not in risk_flags:
                        risk_flags.append("unreconciled_operate_revenue")
            elif followup_type == "retention":
                status = str(feedback.get("retention_status", feedback.get("customer_retention_status", ""))).lower()
                if status in {"retained", "renewed", "expanded"}:
                    retained += 1
                elif status in {"at_risk", "risk", "needs_operator"}:
                    at_risk += 1
                elif status in {"churned", "lost"}:
                    churned += 1
            elif followup_type == "customer_support":
                status = str(feedback.get("support_status", "")).lower()
                if status in {"answered", "resolved", "closed"}:
                    support_resolved += 1
                elif status in {"open", "pending", "escalated", "needs_operator"}:
                    support_open += 1
            elif followup_type == "maintenance":
                status = str(feedback.get("maintenance_status", "")).lower()
                if status in {"resolved", "fixed", "closed"}:
                    maintenance_resolved += 1
                elif status in {"open", "pending", "regression", "needs_operator"}:
                    maintenance_open += 1

        if at_risk and "retention_at_risk" not in risk_flags:
            risk_flags.append("retention_at_risk")
        if churned and "customer_churned" not in risk_flags:
            risk_flags.append("customer_churned")
        if support_open and "support_open" not in risk_flags:
            risk_flags.append("support_open")
        if maintenance_open and "maintenance_open" not in risk_flags:
            risk_flags.append("maintenance_open")
        commitment_receipt_rows = self.conn.execute(
            """
            SELECT receipt_id, receipt_type, action_required, status
            FROM project_customer_commitment_receipts
            WHERE project_id=?
            ORDER BY created_at, receipt_id
            """,
            (project_id,),
        ).fetchall()
        for receipt in commitment_receipt_rows:
            evidence_refs = _merge_refs(evidence_refs, [f"kernel:project_customer_commitment_receipts/{receipt['receipt_id']}"])
            if receipt["action_required"] or receipt["status"] == "needs_followup":
                flag = f"customer_commitment_{receipt['receipt_type']}_needs_followup"
                if flag not in risk_flags:
                    risk_flags.append(flag)

        rollup = ProjectCommercialRollup(
            project_id=project_id,
            revenue_reconciled_usd=revenue_reconciled,
            revenue_unreconciled_usd=revenue_unreconciled,
            retained_customer_count=retained,
            at_risk_customer_count=at_risk,
            churned_customer_count=churned,
            support_resolved_count=support_resolved,
            support_open_count=support_open,
            maintenance_resolved_count=maintenance_resolved,
            maintenance_open_count=maintenance_open,
            external_commitment_count=external_commitments,
            receiptless_side_effect_count=receiptless_side_effects,
            evidence_refs=evidence_refs,
            risk_flags=risk_flags,
        )
        payload = _project_commercial_rollup_payload(rollup)
        event_id = self.append_event("project_commercial_rollup_derived", "project", rollup.rollup_id, payload)
        self.conn.execute(
            """
            INSERT INTO project_commercial_rollups (
              rollup_id, project_id, revenue_reconciled_usd, revenue_unreconciled_usd,
              retained_customer_count, at_risk_customer_count, churned_customer_count,
              support_resolved_count, support_open_count, maintenance_resolved_count,
              maintenance_open_count, external_commitment_count, receiptless_side_effect_count,
              evidence_refs_json, risk_flags_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                rollup.rollup_id,
                rollup.project_id,
                str(rollup.revenue_reconciled_usd),
                str(rollup.revenue_unreconciled_usd),
                rollup.retained_customer_count,
                rollup.at_risk_customer_count,
                rollup.churned_customer_count,
                rollup.support_resolved_count,
                rollup.support_open_count,
                rollup.maintenance_resolved_count,
                rollup.maintenance_open_count,
                rollup.external_commitment_count,
                rollup.receiptless_side_effect_count,
                canonical_json(rollup.evidence_refs),
                canonical_json(rollup.risk_flags),
                rollup.created_at,
            ),
        )
        self.enqueue_projection(event_id, "project_commercial_rollup_projection")
        return rollup

    def derive_project_status_rollup(self, project_id: str) -> ProjectStatusRollup:
        project = self._require_project(project_id)
        phases = self._project_phase_names(project_id)
        phase_rollups = [self._derive_project_phase_rollup(project_id, phase_name) for phase_name in phases]
        task_counts = self._project_task_counts(project_id)
        outcome_counts = self._project_outcome_counts(project_id)
        artifact_count = self._count_project_rows("project_artifact_receipts", project_id)
        feedback_count = self._count_project_rows("project_customer_feedback", project_id)
        revenue_total = self._project_revenue_total(project_id)
        load_minutes = self._project_operator_load_minutes(project_id)
        post_ship = self._project_post_ship_evidence_summary(project_id)
        commitment_receipts = self._project_commitment_receipt_summary(project_id)
        commercial_rollup = self._derive_project_commercial_rollup(project_id)
        commercial_payload = _project_commercial_rollup_payload(commercial_rollup)
        risk_flags: list[str] = []
        if task_counts.get("failed", 0):
            risk_flags.append("failed_tasks")
        if task_counts.get("blocked", 0):
            risk_flags.append("blocked_tasks")
        if feedback_count and not revenue_total:
            risk_flags.append("feedback_without_revenue")
        if post_ship["shipped_artifact_count"] and not post_ship["feedback_count"]:
            risk_flags.append("post_ship_feedback_missing")
        if post_ship["negative_feedback_count"]:
            risk_flags.append("negative_post_ship_feedback")
        if post_ship["action_required_count"]:
            risk_flags.append("post_ship_action_required")
        if post_ship["operator_load_minutes"] >= 60 and post_ship["revenue_attributed_usd"] == Decimal("0"):
            risk_flags.append("post_ship_operator_load_without_revenue")
        if commitment_receipts["open_followup_count"]:
            risk_flags.append("customer_commitment_receipt_followup_open")
        if commitment_receipts["delivery_failure_count"]:
            risk_flags.append("customer_delivery_failure")
        if commitment_receipts["timeout_count"]:
            risk_flags.append("customer_commitment_timeout")
        if commitment_receipts["compensation_needed_count"]:
            risk_flags.append("customer_compensation_needed")
        risk_flags.extend(flag for flag in commercial_rollup.risk_flags if flag not in risk_flags)
        recommended_status = project["status"]
        close_recommendation = "continue"
        commercial_or_post_ship_revenue = (
            commercial_rollup.revenue_reconciled_usd > Decimal("0")
            or post_ship["revenue_attributed_usd"] > Decimal("0")
        )
        if task_counts.get("failed", 0) or any(phase.status == "failed" for phase in phase_rollups):
            recommended_status = "kill_recommended"
            close_recommendation = "kill"
        elif task_counts.get("blocked", 0):
            recommended_status = "blocked"
            close_recommendation = "pause"
        elif commercial_rollup.churned_customer_count:
            recommended_status = "kill_recommended"
            close_recommendation = "kill"
        elif commercial_rollup.at_risk_customer_count or commercial_rollup.maintenance_open_count:
            recommended_status = "paused"
            close_recommendation = "pause"
        elif commercial_rollup.support_open_count:
            recommended_status = "active"
            close_recommendation = "continue"
        elif commercial_or_post_ship_revenue and (
            commercial_rollup.retained_customer_count or commercial_rollup.support_resolved_count
        ):
            recommended_status = "complete"
            close_recommendation = "complete"
        elif post_ship["negative_feedback_count"] and post_ship["revenue_attributed_usd"] == Decimal("0"):
            recommended_status = "kill_recommended"
            close_recommendation = "kill"
        elif post_ship["negative_feedback_count"]:
            recommended_status = "paused"
            close_recommendation = "pause"
        elif post_ship["action_required_count"]:
            recommended_status = "active"
            close_recommendation = "continue"
        elif post_ship["operator_load_minutes"] >= 60 and post_ship["revenue_attributed_usd"] == Decimal("0"):
            recommended_status = "kill_recommended"
            close_recommendation = "kill"
        elif post_ship["shipped_artifact_count"] and post_ship["feedback_count"] and post_ship["revenue_attributed_usd"] > Decimal("0"):
            recommended_status = "complete"
            close_recommendation = "complete"
        elif post_ship["open_followup_count"]:
            recommended_status = "active"
            close_recommendation = "continue"
        elif post_ship["shipped_artifact_count"] and post_ship["feedback_count"] and post_ship["revenue_attributed_usd"] == Decimal("0"):
            recommended_status = "paused"
            close_recommendation = "pause"
        elif task_counts.get("completed", 0) and not any(
            task_counts.get(status, 0) for status in ("queued", "running", "blocked", "failed")
        ):
            recommended_status = "complete"
            close_recommendation = "complete"
        rationale = (
            f"{task_counts.get('completed', 0)} completed tasks, "
            f"{task_counts.get('blocked', 0)} blocked tasks, "
            f"{task_counts.get('failed', 0)} failed tasks, "
            f"{revenue_total} USD attributed, {load_minutes} operator minutes. "
            f"Post-ship evidence: {post_ship['feedback_count']} feedback records, "
            f"{post_ship['revenue_attributed_usd']} USD, "
            f"{post_ship['operator_load_minutes']} operator minutes. "
            f"Operate commercial rollup: {commercial_rollup.revenue_reconciled_usd} reconciled USD, "
            f"{commercial_rollup.retained_customer_count} retained customers, "
            f"{commercial_rollup.support_open_count} open support records."
        )
        rollup = ProjectStatusRollup(
            project_id=project_id,
            project_status=project["status"],
            phase_rollups=phase_rollups,
            task_counts=task_counts,
            outcome_counts=outcome_counts,
            artifact_count=artifact_count,
            customer_feedback_count=feedback_count,
            revenue_attributed_usd=revenue_total,
            operator_load_minutes=load_minutes,
            recommended_status=recommended_status,
            close_recommendation=close_recommendation,  # type: ignore[arg-type]
            rationale=rationale,
            risk_flags=risk_flags,
            commercial_rollup_id=commercial_rollup.rollup_id,
            commercial_rollup=commercial_payload,
        )
        payload = _project_status_rollup_payload(rollup)
        event_id = self.append_event("project_status_rollup_derived", "project", rollup.rollup_id, payload)
        self.conn.execute(
            """
            INSERT INTO project_status_rollups (
              rollup_id, project_id, project_status, phase_rollups_json,
              task_counts_json, outcome_counts_json, artifact_count,
              customer_feedback_count, revenue_attributed_usd, operator_load_minutes,
              recommended_status, close_recommendation, rationale, risk_flags_json,
              commercial_rollup_id, commercial_rollup_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                rollup.rollup_id,
                rollup.project_id,
                rollup.project_status,
                canonical_json(payload["phase_rollups"]),
                canonical_json(rollup.task_counts),
                canonical_json(rollup.outcome_counts),
                rollup.artifact_count,
                rollup.customer_feedback_count,
                str(rollup.revenue_attributed_usd),
                rollup.operator_load_minutes,
                rollup.recommended_status,
                rollup.close_recommendation,
                rollup.rationale,
                canonical_json(rollup.risk_flags),
                rollup.commercial_rollup_id,
                canonical_json(rollup.commercial_rollup),
                rollup.created_at,
            ),
        )
        self.enqueue_projection(event_id, "project_status_rollup_projection")
        return rollup

    def create_project_close_decision(self, project_id: str, *, rollup_id: str | None = None) -> ProjectCloseDecisionPacket:
        self._require_project(project_id)
        if rollup_id is None:
            rollup = self.derive_project_status_rollup(project_id)
        else:
            row = self.conn.execute(
                "SELECT * FROM project_status_rollups WHERE rollup_id=? AND project_id=?",
                (rollup_id, project_id),
            ).fetchone()
            if row is None:
                raise ValueError("project close decision requires a rollup for the project")
            rollup = _rollup_from_row(row)
        evidence_refs = _merge_refs(
            [f"kernel:project_status_rollups/{rollup.rollup_id}"],
            (
                [f"kernel:project_commercial_rollups/{rollup.commercial_rollup_id}"]
                if rollup.commercial_rollup_id
                else []
            ),
            rollup.commercial_rollup.get("evidence_refs", []),
        )
        decision = Decision(
            decision_type="project_close",
            question=f"Should project {project_id} close, pause, continue, or be killed?",
            options=[
                {"verdict": "continue", "effect": "keep project active"},
                {"verdict": "complete", "effect": "mark project complete after operator approval"},
                {"verdict": "pause", "effect": "pause project without external side effects"},
                {"verdict": "kill", "effect": "recommend kill path; no customer obligations are cancelled"},
            ],
            stakes="medium",
            evidence_bundle_ids=[],
            evidence_refs=evidence_refs,
            requested_by="project",
            required_authority="operator_gate",
            authority_policy_version=KERNEL_POLICY_VERSION,
            status="gated",
            recommendation=rollup.close_recommendation,
            confidence=0.75 if rollup.close_recommendation != "continue" else 0.6,
            decisive_factors=[rollup.rationale],
            risk_flags=rollup.risk_flags,
            default_on_timeout="continue",
            gate_packet={
                "project_id": project_id,
                "rollup_id": rollup.rollup_id,
                "evidence_refs": evidence_refs,
                "side_effects_authorized": [],
                "default_on_timeout": "continue",
            },
        )
        self.create_decision(decision)
        packet = ProjectCloseDecisionPacket(
            project_id=project_id,
            decision_id=decision.decision_id,
            rollup_id=rollup.rollup_id,
            recommendation=rollup.close_recommendation,
            required_authority="operator_gate",
            rationale=rollup.rationale,
            risk_flags=rollup.risk_flags,
            evidence_refs=evidence_refs,
            default_on_timeout="continue",
        )
        payload = _project_close_decision_packet_payload(packet)
        event_id = self.append_event("project_close_decision_packet_created", "decision", packet.packet_id, payload)
        self.conn.execute(
            """
            INSERT INTO project_close_decision_packets (
              packet_id, project_id, decision_id, rollup_id, recommendation,
              required_authority, rationale, risk_flags_json, evidence_refs_json,
              default_on_timeout, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                packet.packet_id,
                packet.project_id,
                packet.decision_id,
                packet.rollup_id,
                packet.recommendation,
                packet.required_authority,
                packet.rationale,
                canonical_json(packet.risk_flags),
                canonical_json(packet.evidence_refs),
                packet.default_on_timeout,
                packet.status,
                packet.created_at,
            ),
        )
        self.enqueue_projection(event_id, "project_close_decision_projection")
        return packet

    def resolve_project_close_decision(
        self,
        packet_id: str,
        *,
        verdict: str,
        decided_by: str = "operator",
        notes: str | None = None,
        confidence: float | None = None,
    ) -> dict[str, Any]:
        if verdict not in {"continue", "complete", "pause", "kill"}:
            raise ValueError("project close verdict must be continue, complete, pause, or kill")
        if self.command.requested_by != "operator":
            raise PermissionError("project close decisions require an operator command")
        if self.command.requested_authority != "operator_gate":
            raise PermissionError("project close resolution requires operator-gate authority")
        row = self.conn.execute(
            """
            SELECT p.packet_id, p.project_id, p.decision_id, p.rollup_id,
                   p.recommendation, p.status AS packet_status,
                   d.status AS decision_status, pr.status AS project_status
            FROM project_close_decision_packets p
            JOIN decisions d ON d.decision_id = p.decision_id
            JOIN projects pr ON pr.project_id = p.project_id
            WHERE p.packet_id=?
            """,
            (packet_id,),
        ).fetchone()
        if row is None:
            raise ValueError("project close decision packet not found")
        if row["packet_status"] != "gated":
            raise ValueError(f"cannot resolve project close packet from status {row['packet_status']}")
        if row["decision_status"] != "gated":
            raise ValueError(f"cannot resolve project close decision from status {row['decision_status']}")
        status_by_verdict = {
            "continue": "active",
            "complete": "complete",
            "pause": "paused",
            "kill": "killed",
        }
        previous_status = row["project_status"]
        project_status = status_by_verdict[verdict]
        if previous_status in {"complete", "killed"} and previous_status != project_status:
            raise ValueError(f"cannot resolve project close from terminal project status {previous_status}")

        self.resolve_decision(
            row["decision_id"],
            verdict=verdict,
            decided_by=decided_by,
            notes=notes,
            confidence=confidence,
        )
        decided_at = now_iso()
        followup_task_id: str | None = None
        if verdict == "continue":
            followup_task_id = self._create_post_ship_operate_followup_task(row["project_id"], packet_id, row["rollup_id"])
            if followup_task_id is None:
                followup_task_id = self._create_feedback_followup_task(row["project_id"], packet_id, row["rollup_id"])
        payload = {
            "packet_id": packet_id,
            "project_id": row["project_id"],
            "decision_id": row["decision_id"],
            "rollup_id": row["rollup_id"],
            "previous_project_status": previous_status,
            "project_status": project_status,
            "verdict": verdict,
            "recommendation": row["recommendation"],
            "decided_by": decided_by,
            "notes": notes,
            "followup_task_id": followup_task_id,
            "updated_at": decided_at,
            "decided_at": decided_at,
        }
        event_id = self.append_event("project_close_decision_resolved", "decision", packet_id, payload, actor_type="operator", actor_id=decided_by)
        self.conn.execute(
            "UPDATE project_close_decision_packets SET status='decided' WHERE packet_id=?",
            (packet_id,),
        )
        self.conn.execute(
            "UPDATE projects SET status=?, updated_at=? WHERE project_id=?",
            (project_status, decided_at, row["project_id"]),
        )
        self.enqueue_projection(event_id, "project_close_decision_projection")
        self.enqueue_projection(event_id, "project_projection")
        return {
            "packet_id": packet_id,
            "decision_id": row["decision_id"],
            "project_id": row["project_id"],
            "verdict": verdict,
            "project_status": project_status,
            "followup_task_id": followup_task_id,
        }

    def compare_project_replay_to_projection(self, project_id: str) -> ProjectReplayProjectionComparison:
        self._require_project(project_id)
        replay = self.__class__._replay_from_connection(self.conn)
        replay_project = replay.projects.get(project_id)
        replay_task_counts = _count_by_status(
            task for task in replay.project_tasks.values() if task.get("project_id") == project_id
        )
        projection_task_counts = self._project_task_counts(project_id)
        replay_revenue = sum(
            (Decimal(item["amount_usd"]) for item in replay.project_revenue_attributions.values() if item.get("project_id") == project_id),
            Decimal("0"),
        )
        projection_revenue = self._project_revenue_total(project_id)
        replay_load = sum(
            int(item["minutes"]) for item in replay.project_operator_load.values() if item.get("project_id") == project_id
        )
        projection_load = self._project_operator_load_minutes(project_id)
        replay_commercial_rollup = _latest_replay_project_commercial_rollup(replay, project_id)
        projection_commercial_rollup = self._latest_project_commercial_rollup_payload(project_id)
        projection_status = self.conn.execute(
            "SELECT status FROM projects WHERE project_id=?",
            (project_id,),
        ).fetchone()["status"]
        mismatches: list[str] = []
        if (replay_project or {}).get("status") != projection_status:
            mismatches.append("project_status")
        if replay_task_counts != projection_task_counts:
            mismatches.append("task_counts")
        if replay_revenue != projection_revenue:
            mismatches.append("revenue_attributed_usd")
        if replay_load != projection_load:
            mismatches.append("operator_load_minutes")
        if replay_commercial_rollup != projection_commercial_rollup:
            mismatches.append("commercial_rollup")
        comparison = ProjectReplayProjectionComparison(
            project_id=project_id,
            replay_project_status=(replay_project or {}).get("status"),
            projection_project_status=projection_status,
            replay_task_counts=replay_task_counts,
            projection_task_counts=projection_task_counts,
            replay_revenue_attributed_usd=replay_revenue,
            projection_revenue_attributed_usd=projection_revenue,
            replay_operator_load_minutes=replay_load,
            projection_operator_load_minutes=projection_load,
            replay_commercial_rollup=replay_commercial_rollup,
            projection_commercial_rollup=projection_commercial_rollup,
            matches=not mismatches,
            mismatches=mismatches,
        )
        payload = _project_replay_projection_comparison_payload(comparison)
        event_id = self.append_event("project_replay_projection_compared", "project", comparison.comparison_id, payload)
        self.conn.execute(
            """
            INSERT INTO project_replay_projection_comparisons (
              comparison_id, project_id, replay_project_status, projection_project_status,
              replay_task_counts_json, projection_task_counts_json,
              replay_revenue_attributed_usd, projection_revenue_attributed_usd,
              replay_operator_load_minutes, projection_operator_load_minutes,
              replay_commercial_rollup_json, projection_commercial_rollup_json,
              matches, mismatches_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                comparison.comparison_id,
                comparison.project_id,
                comparison.replay_project_status,
                comparison.projection_project_status,
                canonical_json(comparison.replay_task_counts),
                canonical_json(comparison.projection_task_counts),
                str(comparison.replay_revenue_attributed_usd),
                str(comparison.projection_revenue_attributed_usd),
                comparison.replay_operator_load_minutes,
                comparison.projection_operator_load_minutes,
                canonical_json(comparison.replay_commercial_rollup),
                canonical_json(comparison.projection_commercial_rollup),
                int(comparison.matches),
                canonical_json(comparison.mismatches),
                comparison.created_at,
            ),
        )
        self.enqueue_projection(event_id, "project_replay_projection_comparison_projection")
        return comparison

    def create_project_portfolio_decision_packet(
        self,
        project_ids: list[str],
        *,
        scope: str = "active_commercial_projects",
        constraints: dict[str, Any] | None = None,
    ) -> ProjectPortfolioDecisionPacket:
        constraints = constraints or {}
        if self.command.requested_by in {"agent", "model"}:
            raise PermissionError("workers cannot create portfolio reprioritization packets")
        if self.command.requested_authority != "operator_gate":
            raise PermissionError("portfolio prioritization packets require operator-gate authority")
        if self.command.payload.get("autonomous_reprioritization") or self.command.payload.get("customer_commitment_requested"):
            raise PermissionError("portfolio packets cannot authorize autonomous reprioritization or customer commitments")
        if not project_ids:
            raise ValueError("portfolio packet requires at least one project")

        unique_project_ids = list(dict.fromkeys(project_ids))
        projects: list[sqlite3.Row] = [self._require_project(project_id) for project_id in unique_project_ids]
        rollups: list[ProjectStatusRollup] = []
        for project_id in unique_project_ids:
            rollups.append(self.derive_project_status_rollup(project_id))

        recommendations = [
            self._portfolio_project_recommendation(project, rollup, constraints)
            for project, rollup in zip(projects, rollups)
        ]
        recommendations.sort(key=lambda item: (-item["priority_score"], item["operator_load_minutes"], item["project_id"]))
        for index, item in enumerate(recommendations, start=1):
            item["priority_rank"] = index

        tradeoffs = _portfolio_tradeoffs(recommendations, constraints)
        risk_flags = _portfolio_risk_flags(recommendations, tradeoffs, constraints)
        recommendation = _portfolio_packet_recommendation(recommendations, risk_flags)
        rollup_ids = [rollup.rollup_id for rollup in rollups]
        evidence_refs = _merge_refs(
            *(
                _merge_refs(
                    [f"kernel:project_status_rollups/{rollup.rollup_id}"],
                    (
                        [f"kernel:project_commercial_rollups/{rollup.commercial_rollup_id}"]
                        if rollup.commercial_rollup_id
                        else []
                    ),
                    rollup.commercial_rollup.get("evidence_refs", []),
                )
                for rollup in rollups
            )
        )
        packet_body = {
            "scope": scope,
            "project_count": len(unique_project_ids),
            "recommendation": recommendation,
            "ranked_projects": recommendations,
            "tradeoffs": tradeoffs,
            "constraints": constraints,
            "authority": {
                "required_authority": "operator_gate",
                "authority_policy_version": KERNEL_POLICY_VERSION,
                "agents_may_recommend": True,
                "agents_may_reprioritize": False,
                "agents_may_commit_customer_work": False,
                "side_effects_authorized": [],
                "external_commitment_policy": "operator_only",
            },
            "default_on_timeout": "defer",
        }
        decision = Decision(
            decision_type="commercial_strategy",
            question=f"Approve portfolio priority packet for {scope}?",
            options=[
                {"verdict": "accept_prioritization", "effect": "operator accepts packet as planning guidance only"},
                {"verdict": "revise_prioritization", "effect": "operator requests a revised packet"},
                {"verdict": "defer", "effect": "no portfolio priority changes are made"},
            ],
            stakes="high",
            evidence_bundle_ids=[],
            evidence_refs=evidence_refs,
            requested_by="project",
            required_authority="operator_gate",
            authority_policy_version=KERNEL_POLICY_VERSION,
            status="gated",
            recommendation=recommendation,
            confidence=0.72 if recommendation in {"prioritize", "balance"} else 0.62,
            decisive_factors=[
                f"{item['project_id']} score={item['priority_score']} action={item['recommended_action']}"
                for item in recommendations
            ],
            risk_flags=risk_flags,
            default_on_timeout="defer",
            gate_packet={
                "portfolio_packet": packet_body,
                "options": ["accept_prioritization", "revise_prioritization", "defer"],
                "side_effects_authorized": [],
                "default_on_timeout": "defer",
            },
        )
        self.create_decision(decision)
        packet = ProjectPortfolioDecisionPacket(
            decision_id=decision.decision_id,
            scope=scope,
            project_ids=unique_project_ids,
            rollup_ids=rollup_ids,
            recommendation=recommendation,
            required_authority="operator_gate",
            packet=packet_body,
            tradeoffs=tradeoffs,
            evidence_refs=evidence_refs,
            risk_flags=risk_flags,
            default_on_timeout="defer",
        )
        payload = _project_portfolio_decision_packet_payload(packet)
        event_id = self.append_event("project_portfolio_decision_packet_created", "decision", packet.packet_id, payload)
        self.conn.execute(
            """
            INSERT INTO project_portfolio_decision_packets (
              packet_id, decision_id, scope, project_ids_json, rollup_ids_json,
              recommendation, required_authority, packet_json, tradeoffs_json,
              evidence_refs_json, risk_flags_json, default_on_timeout, status,
              verdict, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                packet.packet_id,
                packet.decision_id,
                packet.scope,
                canonical_json(packet.project_ids),
                canonical_json(packet.rollup_ids),
                packet.recommendation,
                packet.required_authority,
                canonical_json(packet.packet),
                canonical_json(packet.tradeoffs),
                canonical_json(packet.evidence_refs),
                canonical_json(packet.risk_flags),
                packet.default_on_timeout,
                packet.status,
                packet.verdict,
                packet.created_at,
            ),
        )
        self.enqueue_projection(event_id, "project_portfolio_decision_packet_projection")
        return packet

    def resolve_project_portfolio_decision(
        self,
        packet_id: str,
        *,
        verdict: str,
        decided_by: str = "operator",
        notes: str | None = None,
        confidence: float | None = None,
    ) -> dict[str, Any]:
        if verdict not in {"accept_prioritization", "revise_prioritization", "defer"}:
            raise ValueError("portfolio verdict must be accept_prioritization, revise_prioritization, or defer")
        if self.command.requested_by != "operator":
            raise PermissionError("portfolio decisions require an operator command")
        if self.command.requested_authority != "operator_gate":
            raise PermissionError("portfolio decision resolution requires operator-gate authority")
        row = self.conn.execute(
            """
            SELECT p.packet_id, p.decision_id, p.status AS packet_status, d.status AS decision_status
            FROM project_portfolio_decision_packets p
            JOIN decisions d ON d.decision_id = p.decision_id
            WHERE p.packet_id=?
            """,
            (packet_id,),
        ).fetchone()
        if row is None:
            raise ValueError("portfolio decision packet not found")
        if row["packet_status"] != "gated" or row["decision_status"] != "gated":
            raise ValueError("portfolio decision packet is not gated")
        self.resolve_decision(
            row["decision_id"],
            verdict=verdict,
            decided_by=decided_by,
            notes=notes,
            confidence=confidence,
        )
        decided_at = now_iso()
        payload = {
            "packet_id": packet_id,
            "decision_id": row["decision_id"],
            "verdict": verdict,
            "decided_by": decided_by,
            "notes": notes,
            "authority_effect": "planning_guidance_only",
            "project_status_changes": [],
            "customer_commitments": [],
            "decided_at": decided_at,
        }
        event_id = self.append_event("project_portfolio_decision_resolved", "decision", packet_id, payload, actor_type="operator", actor_id=decided_by)
        self.conn.execute(
            "UPDATE project_portfolio_decision_packets SET status='decided', verdict=? WHERE packet_id=?",
            (verdict, packet_id),
        )
        self.enqueue_projection(event_id, "project_portfolio_decision_packet_projection")
        return {
            "packet_id": packet_id,
            "decision_id": row["decision_id"],
            "verdict": verdict,
            "authority_effect": "planning_guidance_only",
            "project_status_changes": [],
            "customer_commitments": [],
        }

    def compare_project_portfolio_replay_to_projection(self, packet_id: str) -> ProjectPortfolioReplayProjectionComparison:
        replay = self.__class__._replay_from_connection(self.conn)
        replay_packet = replay.project_portfolio_decision_packets.get(packet_id)
        row = self.conn.execute(
            "SELECT * FROM project_portfolio_decision_packets WHERE packet_id=?",
            (packet_id,),
        ).fetchone()
        if row is None:
            raise ValueError("portfolio decision packet not found")
        projection_packet = _portfolio_packet_from_row(row)
        mismatches: list[str] = []
        if replay_packet != projection_packet:
            mismatches.append("portfolio_packet")
        comparison = ProjectPortfolioReplayProjectionComparison(
            packet_id=packet_id,
            replay_packet=replay_packet or {},
            projection_packet=projection_packet,
            matches=not mismatches,
            mismatches=mismatches,
        )
        payload = _project_portfolio_replay_projection_comparison_payload(comparison)
        event_id = self.append_event("project_portfolio_replay_projection_compared", "project", comparison.comparison_id, payload)
        self.conn.execute(
            """
            INSERT INTO project_portfolio_replay_projection_comparisons (
              comparison_id, packet_id, replay_packet_json, projection_packet_json,
              matches, mismatches_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                comparison.comparison_id,
                comparison.packet_id,
                canonical_json(comparison.replay_packet),
                canonical_json(comparison.projection_packet),
                int(comparison.matches),
                canonical_json(comparison.mismatches),
                comparison.created_at,
            ),
        )
        self.enqueue_projection(event_id, "project_portfolio_replay_projection_comparison_projection")
        return comparison

    def create_project_scheduling_intent(
        self,
        packet_id: str,
        *,
        scheduling_window: str = "next_internal_cycle",
    ) -> ProjectSchedulingIntent:
        if self.command.requested_by in {"agent", "model", "tool"}:
            raise PermissionError("workers cannot create project scheduling intents")
        if self.command.requested_authority not in {None, "rule"}:
            raise PermissionError("scheduling intents are internal rule-governed records")
        blocked_flags = {
            "autonomous_reprioritization",
            "autonomous_cancellation",
            "customer_commitment_requested",
            "priority_change_requested",
        }
        if any(self.command.payload.get(flag) for flag in blocked_flags):
            raise PermissionError("scheduling intents cannot reprioritize, cancel, or commit customer-facing work")
        if not scheduling_window.strip():
            raise ValueError("scheduling window is required")
        row = self.conn.execute(
            "SELECT * FROM project_portfolio_decision_packets WHERE packet_id=?",
            (packet_id,),
        ).fetchone()
        if row is None:
            raise ValueError("portfolio decision packet not found")
        if row["status"] != "decided" or row["verdict"] != "accept_prioritization":
            raise PermissionError("scheduling intents require an accepted operator portfolio packet")

        packet = _portfolio_packet_from_row(row)
        packet_body = packet["packet"]
        ranked = packet_body.get("ranked_projects", [])
        queue_adjustments = [
            _bounded_queue_adjustment(item, rank=index, constraints=packet_body.get("constraints", {}))
            for index, item in enumerate(ranked, start=1)
        ]
        intent_body = {
            "portfolio_packet_id": packet_id,
            "source_decision_id": row["decision_id"],
            "scheduling_window": scheduling_window,
            "scope": row["scope"],
            "authority": {
                "required_authority": "rule",
                "authority_effect": "internal_scheduling_recommendations_only",
                "priority_changes_require_operator_gate": True,
                "cancellations_require_operator_gate": True,
                "customer_commitments_allowed": False,
                "side_effects_authorized": [],
            },
            "bounds": {
                "max_queue_delta_tasks_per_project": 1,
                "allowed_task_types": ["operate", "feedback"],
                "customer_visible_work": False,
                "mutates_project_status": False,
                "mutates_task_priority": False,
                "cancels_tasks": False,
            },
            "tradeoffs": packet["tradeoffs"],
            "queue_adjustment_count": len(queue_adjustments),
        }
        risk_flags = list(dict.fromkeys([*packet["risk_flags"], *(_scheduling_risk_flags(queue_adjustments))]))
        evidence_refs = list(dict.fromkeys([f"kernel:project_portfolio_decision_packets/{packet_id}", *packet["evidence_refs"]]))
        intent = ProjectSchedulingIntent(
            portfolio_packet_id=packet_id,
            source_decision_id=row["decision_id"],
            scope=row["scope"],
            project_ids=packet["project_ids"],
            scheduling_window=scheduling_window,
            intent=intent_body,
            queue_adjustments=queue_adjustments,
            evidence_refs=evidence_refs,
            risk_flags=risk_flags,
            required_authority="rule",
            authority_effect="internal_scheduling_recommendations_only",
        )
        payload = _project_scheduling_intent_payload(intent)
        event_id = self.append_event("project_scheduling_intent_recorded", "task", intent.intent_id, payload)
        self.conn.execute(
            """
            INSERT INTO project_scheduling_intents (
              intent_id, portfolio_packet_id, source_decision_id, scope,
              project_ids_json, scheduling_window, intent_json,
              queue_adjustments_json, evidence_refs_json, risk_flags_json,
              required_authority, authority_effect, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                intent.intent_id,
                intent.portfolio_packet_id,
                intent.source_decision_id,
                intent.scope,
                canonical_json(intent.project_ids),
                intent.scheduling_window,
                canonical_json(intent.intent),
                canonical_json(intent.queue_adjustments),
                canonical_json(intent.evidence_refs),
                canonical_json(intent.risk_flags),
                intent.required_authority,
                intent.authority_effect,
                intent.status,
                intent.created_at,
            ),
        )
        self.enqueue_projection(event_id, "project_scheduling_intent_projection")
        return intent

    def compare_project_scheduling_replay_to_projection(
        self,
        intent_id: str,
    ) -> ProjectSchedulingReplayProjectionComparison:
        replay = self.__class__._replay_from_connection(self.conn)
        replay_intent = replay.project_scheduling_intents.get(intent_id)
        row = self.conn.execute(
            "SELECT * FROM project_scheduling_intents WHERE intent_id=?",
            (intent_id,),
        ).fetchone()
        if row is None:
            raise ValueError("project scheduling intent not found")
        projection_intent = _project_scheduling_intent_from_row(row)
        mismatches: list[str] = []
        if replay_intent != projection_intent:
            mismatches.append("project_scheduling_intent")
        comparison = ProjectSchedulingReplayProjectionComparison(
            intent_id=intent_id,
            replay_intent=replay_intent or {},
            projection_intent=projection_intent,
            matches=not mismatches,
            mismatches=mismatches,
        )
        payload = _project_scheduling_replay_projection_comparison_payload(comparison)
        event_id = self.append_event("project_scheduling_replay_projection_compared", "task", comparison.comparison_id, payload)
        self.conn.execute(
            """
            INSERT INTO project_scheduling_replay_projection_comparisons (
              comparison_id, intent_id, replay_intent_json, projection_intent_json,
              matches, mismatches_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                comparison.comparison_id,
                comparison.intent_id,
                canonical_json(comparison.replay_intent),
                canonical_json(comparison.projection_intent),
                int(comparison.matches),
                canonical_json(comparison.mismatches),
                comparison.created_at,
            ),
        )
        self.enqueue_projection(event_id, "project_scheduling_replay_projection_comparison_projection")
        return comparison

    def create_project_scheduling_priority_change_packet(
        self,
        intent_id: str,
    ) -> ProjectSchedulingPriorityChangePacket:
        if self.command.requested_by in {"agent", "model", "tool"}:
            raise PermissionError("workers cannot create scheduling priority-change packets")
        if self.command.requested_authority not in {None, "operator_gate"}:
            raise PermissionError("priority-change packets must be prepared as operator-gated decisions")
        blocked_flags = {
            "autonomous_queue_mutation",
            "autonomous_reprioritization",
            "autonomous_cancellation",
            "customer_commitment_requested",
            "priority_change_apply_requested",
        }
        if any(self.command.payload.get(flag) for flag in blocked_flags):
            raise PermissionError("priority-change packet creation cannot mutate queues, cancel work, or commit customers")
        row = self.conn.execute(
            "SELECT * FROM project_scheduling_intents WHERE intent_id=?",
            (intent_id,),
        ).fetchone()
        if row is None:
            raise ValueError("project scheduling intent not found")
        if row["status"] != "recorded":
            raise ValueError("project scheduling intent is not active")
        existing = self.conn.execute(
            """
            SELECT packet_id FROM project_scheduling_priority_change_packets
            WHERE intent_id=? AND status='gated'
            """,
            (intent_id,),
        ).fetchone()
        if existing is not None:
            raise ValueError("scheduling intent already has a gated priority-change packet")

        intent = _project_scheduling_intent_from_row(row)
        proposed_changes = [
            _priority_change_from_adjustment(adjustment, scheduling_window=row["scheduling_window"])
            for adjustment in intent["queue_adjustments"]
        ]
        evidence_refs = list(
            dict.fromkeys(
                [
                    f"kernel:project_scheduling_intents/{intent_id}",
                    f"kernel:project_portfolio_decision_packets/{row['portfolio_packet_id']}",
                    *intent["evidence_refs"],
                ]
            )
        )
        risk_flags = list(dict.fromkeys([*intent["risk_flags"], *(_priority_change_risk_flags(proposed_changes))]))
        decision = Decision(
            decision_type="commercial_strategy",
            question=f"Apply bounded internal queue priority changes for {row['scheduling_window']}?",
            options=[
                {"verdict": "accept_priority_changes", "effect": "operator applies bounded internal queue changes only"},
                {"verdict": "reject_priority_changes", "effect": "no queue changes are made"},
                {"verdict": "defer", "effect": "no queue changes are made before another operator review"},
            ],
            stakes="high",
            evidence_bundle_ids=[],
            evidence_refs=evidence_refs,
            requested_by="scheduler",
            required_authority="operator_gate",
            authority_policy_version=KERNEL_POLICY_VERSION,
            status="gated",
            recommendation="accept_priority_changes" if proposed_changes else "defer",
            confidence=0.7,
            decisive_factors=[
                f"{change['project_id']} rank={change['priority_rank']} action={change['queue_action']}"
                for change in proposed_changes
            ],
            risk_flags=risk_flags,
            default_on_timeout="defer",
            gate_packet={
                "scheduling_priority_change_packet": {
                    "intent_id": intent_id,
                    "scheduling_window": row["scheduling_window"],
                    "proposed_changes": proposed_changes,
                    "authority": {
                        "required_authority": "operator_gate",
                        "mutates_queue_on_packet_creation": False,
                        "applies_only_on_accept": True,
                        "customer_commitments_allowed": False,
                        "cancellations_allowed": False,
                        "side_effects_authorized": [],
                    },
                },
                "options": ["accept_priority_changes", "reject_priority_changes", "defer"],
                "side_effects_authorized": [],
                "default_on_timeout": "defer",
            },
        )
        self.create_decision(decision)
        packet = ProjectSchedulingPriorityChangePacket(
            intent_id=intent_id,
            portfolio_packet_id=row["portfolio_packet_id"],
            source_decision_id=row["source_decision_id"],
            decision_id=decision.decision_id,
            scope=row["scope"],
            project_ids=intent["project_ids"],
            scheduling_window=row["scheduling_window"],
            proposed_changes=proposed_changes,
            evidence_refs=evidence_refs,
            risk_flags=risk_flags,
            required_authority="operator_gate",
            default_on_timeout="defer",
        )
        payload = _project_scheduling_priority_change_packet_payload(packet)
        event_id = self.append_event("project_scheduling_priority_change_packet_created", "decision", packet.packet_id, payload)
        self.conn.execute(
            """
            INSERT INTO project_scheduling_priority_change_packets (
              packet_id, intent_id, portfolio_packet_id, source_decision_id,
              decision_id, scope, project_ids_json, scheduling_window,
              proposed_changes_json, evidence_refs_json, risk_flags_json,
              required_authority, default_on_timeout, status, verdict,
              applied_changes_json, created_at, decided_by, decided_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                packet.packet_id,
                packet.intent_id,
                packet.portfolio_packet_id,
                packet.source_decision_id,
                packet.decision_id,
                packet.scope,
                canonical_json(packet.project_ids),
                packet.scheduling_window,
                canonical_json(packet.proposed_changes),
                canonical_json(packet.evidence_refs),
                canonical_json(packet.risk_flags),
                packet.required_authority,
                packet.default_on_timeout,
                packet.status,
                packet.verdict,
                canonical_json(packet.applied_changes),
                packet.created_at,
                packet.decided_by,
                packet.decided_at,
            ),
        )
        self.enqueue_projection(event_id, "project_scheduling_priority_change_packet_projection")
        return packet

    def resolve_project_scheduling_priority_change_packet(
        self,
        packet_id: str,
        *,
        verdict: str,
        decided_by: str = "operator",
        notes: str | None = None,
        confidence: float | None = None,
    ) -> dict[str, Any]:
        if verdict not in {"accept_priority_changes", "reject_priority_changes", "defer"}:
            raise ValueError("priority-change verdict must be accept_priority_changes, reject_priority_changes, or defer")
        if self.command.requested_by != "operator":
            raise PermissionError("priority-change packet resolution requires an operator command")
        if self.command.requested_authority != "operator_gate":
            raise PermissionError("priority-change packet resolution requires operator-gate authority")
        blocked_flags = {
            "autonomous_queue_mutation",
            "autonomous_reprioritization",
            "autonomous_cancellation",
            "customer_commitment_requested",
        }
        if any(self.command.payload.get(flag) for flag in blocked_flags):
            raise PermissionError("priority-change resolution cannot authorize autonomous mutation, cancellation, or customer commitments")
        row = self.conn.execute(
            """
            SELECT p.*, d.status AS decision_status
            FROM project_scheduling_priority_change_packets p
            JOIN decisions d ON d.decision_id = p.decision_id
            WHERE p.packet_id=?
            """,
            (packet_id,),
        ).fetchone()
        if row is None:
            raise ValueError("scheduling priority-change packet not found")
        if row["status"] != "gated" or row["decision_status"] != "gated":
            raise ValueError("scheduling priority-change packet is not gated")

        self.resolve_decision(
            row["decision_id"],
            verdict=verdict,
            decided_by=decided_by,
            notes=notes,
            confidence=confidence,
        )
        proposed_changes = _loads(row["proposed_changes_json"])
        applied_changes: list[dict[str, Any]] = []
        if verdict == "accept_priority_changes":
            for change in proposed_changes:
                if change["queue_action"] != "recommend_next_internal_task":
                    applied_changes.append(_not_applied_priority_change(change, reason="operator_accepted_no_queue_delta_for_action"))
                    continue
                if int(change["max_queue_delta_tasks"]) > 1:
                    raise PermissionError("priority-change packets may apply at most one queued task per project")
                task_id = self._create_scheduling_priority_task(row, change)
                applied_changes.append(
                    {
                        "project_id": change["project_id"],
                        "priority_rank": change["priority_rank"],
                        "queue_action": change["queue_action"],
                        "task_id": task_id,
                        "task_type": change["task_type"],
                        "status": "queued",
                        "customer_visible": False,
                        "external_side_effects_authorized": [],
                        "cancellation_applied": False,
                        "customer_commitment_applied": False,
                    }
                )
        else:
            applied_changes = [
                _not_applied_priority_change(change, reason=f"operator_{verdict}")
                for change in proposed_changes
            ]

        decided_at = now_iso()
        payload = {
            "packet_id": packet_id,
            "intent_id": row["intent_id"],
            "decision_id": row["decision_id"],
            "verdict": verdict,
            "decided_by": decided_by,
            "notes": notes,
            "authority_effect": "bounded_internal_queue_changes" if verdict == "accept_priority_changes" else "no_queue_changes",
            "applied_changes": applied_changes,
            "customer_commitments": [],
            "cancellations": [],
            "side_effects_authorized": [],
            "decided_at": decided_at,
        }
        event_id = self.append_event(
            "project_scheduling_priority_change_packet_resolved",
            "decision",
            packet_id,
            payload,
            actor_type="operator",
            actor_id=decided_by,
        )
        self.conn.execute(
            """
            UPDATE project_scheduling_priority_change_packets
            SET status='decided', verdict=?, applied_changes_json=?, decided_by=?, decided_at=?
            WHERE packet_id=?
            """,
            (verdict, canonical_json(applied_changes), decided_by, decided_at, packet_id),
        )
        self.enqueue_projection(event_id, "project_scheduling_priority_change_packet_projection")
        return {
            "packet_id": packet_id,
            "decision_id": row["decision_id"],
            "verdict": verdict,
            "authority_effect": payload["authority_effect"],
            "applied_changes": applied_changes,
            "customer_commitments": [],
            "cancellations": [],
        }

    def compare_project_scheduling_priority_replay_to_projection(
        self,
        packet_id: str,
    ) -> ProjectSchedulingPriorityReplayProjectionComparison:
        replay = self.__class__._replay_from_connection(self.conn)
        replay_packet = replay.project_scheduling_priority_change_packets.get(packet_id)
        row = self.conn.execute(
            "SELECT * FROM project_scheduling_priority_change_packets WHERE packet_id=?",
            (packet_id,),
        ).fetchone()
        if row is None:
            raise ValueError("scheduling priority-change packet not found")
        projection_packet = _project_scheduling_priority_change_packet_from_row(row)
        mismatches: list[str] = []
        if replay_packet != projection_packet:
            mismatches.append("project_scheduling_priority_change_packet")
        comparison = ProjectSchedulingPriorityReplayProjectionComparison(
            packet_id=packet_id,
            replay_packet=replay_packet or {},
            projection_packet=projection_packet,
            matches=not mismatches,
            mismatches=mismatches,
        )
        payload = _project_scheduling_priority_replay_projection_comparison_payload(comparison)
        event_id = self.append_event("project_scheduling_priority_replay_projection_compared", "task", comparison.comparison_id, payload)
        self.conn.execute(
            """
            INSERT INTO project_scheduling_priority_replay_projection_comparisons (
              comparison_id, packet_id, replay_packet_json, projection_packet_json,
              matches, mismatches_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                comparison.comparison_id,
                comparison.packet_id,
                canonical_json(comparison.replay_packet),
                canonical_json(comparison.projection_packet),
                int(comparison.matches),
                canonical_json(comparison.mismatches),
                comparison.created_at,
            ),
        )
        self.enqueue_projection(event_id, "project_scheduling_priority_replay_projection_comparison_projection")
        return comparison

    def create_project_customer_visible_packet(
        self,
        outcome_id: str,
        *,
        packet_type: str,
        customer_ref: str,
        channel: str,
        subject: str,
        summary: str,
        payload_ref: str,
        side_effect_intent_id: str,
    ) -> ProjectCustomerVisiblePacket:
        if self.command.requested_by in {"agent", "model", "tool"}:
            raise PermissionError("workers cannot create customer-visible packets")
        if self.command.requested_authority != "operator_gate":
            raise PermissionError("customer-visible packets require operator-gate authority")
        if self.command.payload.get("autonomous_customer_visible") or self.command.payload.get("external_action_executed"):
            raise PermissionError("customer-visible packets cannot be autonomous or record executed external actions")
        if packet_type not in {"customer_message", "customer_delivery"}:
            raise ValueError("customer-visible packet type must be customer_message or customer_delivery")
        required = [customer_ref, channel, subject, summary, payload_ref, side_effect_intent_id]
        if any(not value.strip() for value in required):
            raise ValueError("customer-visible packet requires customer, channel, subject, summary, payload ref, and intent")

        outcome = self.conn.execute(
            """
            SELECT outcome_id, project_id, task_id, status, outcome_type, summary,
                   artifact_refs_json, side_effect_intent_id, side_effect_receipt_id
            FROM project_outcomes
            WHERE outcome_id=?
            """,
            (outcome_id,),
        ).fetchone()
        if outcome is None:
            raise ValueError("customer-visible packet requires an existing internal outcome")
        if outcome["status"] != "accepted":
            raise PermissionError("customer-visible packets require accepted internal outcome evidence")
        if outcome["side_effect_receipt_id"]:
            raise PermissionError("customer-visible packets must be prepared from internal outcomes before external commitment")
        if not outcome["task_id"]:
            raise PermissionError("customer-visible packets require task-linked internal outcome evidence")

        intent = self.conn.execute(
            """
            SELECT intent_id, task_id, side_effect_type, required_authority, status
            FROM side_effect_intents
            WHERE intent_id=?
            """,
            (side_effect_intent_id,),
        ).fetchone()
        if intent is None:
            raise ValueError("customer-visible packet requires a durable side-effect intent")
        if intent["task_id"] != outcome["task_id"]:
            raise ValueError("customer-visible side-effect intent task does not match outcome task")
        if intent["required_authority"] != "operator_gate":
            raise PermissionError("customer-visible side-effect intent must require operator gate")
        if intent["status"] != "prepared":
            raise PermissionError("customer-visible packet requires a prepared side-effect intent")
        if packet_type == "customer_message" and intent["side_effect_type"] != "message":
            raise ValueError("customer message packets require a message side-effect intent")
        if packet_type == "customer_delivery" and intent["side_effect_type"] not in {"publish", "deploy", "message"}:
            raise ValueError("customer delivery packets require publish, deploy, or message side-effect intent")

        existing = self.conn.execute(
            """
            SELECT packet_id FROM project_customer_visible_packets
            WHERE outcome_id=? AND status='gated'
            """,
            (outcome_id,),
        ).fetchone()
        if existing is not None:
            raise ValueError("internal outcome already has a gated customer-visible packet")

        evidence_refs = _merge_refs(
            [f"kernel:project_outcomes/{outcome_id}", f"kernel:project_tasks/{outcome['task_id']}"],
            _loads(outcome["artifact_refs_json"]),
            [f"kernel:side_effect_intents/{side_effect_intent_id}"],
        )
        risk_flags = ["customer_visible_commitment_requires_receipt"]
        decision = Decision(
            decision_type="commercial_strategy",
            question=f"Approve {packet_type.replace('_', ' ')} for {customer_ref}?",
            options=[
                {"verdict": "accept_customer_visible_packet", "effect": "record commitment only with successful durable receipt"},
                {"verdict": "reject_customer_visible_packet", "effect": "no customer commitment or side effect is recorded"},
                {"verdict": "defer", "effect": "keep packet gated; no customer commitment or side effect is recorded"},
            ],
            stakes="high",
            evidence_bundle_ids=[],
            evidence_refs=evidence_refs,
            requested_by="project",
            required_authority="operator_gate",
            authority_policy_version=KERNEL_POLICY_VERSION,
            status="gated",
            recommendation="accept_customer_visible_packet",
            confidence=0.72,
            decisive_factors=[
                f"internal_outcome={outcome_id}",
                f"side_effect_intent={side_effect_intent_id}",
                "durable_receipt_required_before_customer_commitment",
            ],
            risk_flags=risk_flags,
            default_on_timeout="defer",
            gate_packet={
                "customer_visible_packet": {
                    "outcome_id": outcome_id,
                    "packet_type": packet_type,
                    "customer_ref": customer_ref,
                    "channel": channel,
                    "subject": subject,
                    "payload_ref": payload_ref,
                    "side_effect_intent_id": side_effect_intent_id,
                    "authority": {
                        "required_authority": "operator_gate",
                        "records_commitment_on_creation": False,
                        "receipt_required_before_commitment": True,
                        "replay_executes_external_effects": False,
                    },
                },
                "options": ["accept_customer_visible_packet", "reject_customer_visible_packet", "defer"],
                "side_effects_authorized": [side_effect_intent_id],
                "default_on_timeout": "defer",
            },
        )
        self.create_decision(decision)
        packet = ProjectCustomerVisiblePacket(
            project_id=outcome["project_id"],
            outcome_id=outcome_id,
            decision_id=decision.decision_id,
            packet_type=packet_type,  # type: ignore[arg-type]
            customer_ref=customer_ref,
            channel=channel,
            subject=subject,
            summary=summary,
            payload_ref=payload_ref,
            side_effect_intent_id=side_effect_intent_id,
            evidence_refs=evidence_refs,
            risk_flags=risk_flags,
            required_authority="operator_gate",
            default_on_timeout="defer",
        )
        payload = _project_customer_visible_packet_payload(packet)
        event_id = self.append_event("project_customer_visible_packet_created", "decision", packet.packet_id, payload)
        self.conn.execute(
            """
            INSERT INTO project_customer_visible_packets (
              packet_id, project_id, outcome_id, decision_id, packet_type,
              customer_ref, channel, subject, summary, payload_ref,
              side_effect_intent_id, evidence_refs_json, risk_flags_json,
              required_authority, default_on_timeout, status, verdict,
              created_at, decided_by, decided_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                packet.packet_id,
                packet.project_id,
                packet.outcome_id,
                packet.decision_id,
                packet.packet_type,
                packet.customer_ref,
                packet.channel,
                packet.subject,
                packet.summary,
                packet.payload_ref,
                packet.side_effect_intent_id,
                canonical_json(packet.evidence_refs),
                canonical_json(packet.risk_flags),
                packet.required_authority,
                packet.default_on_timeout,
                packet.status,
                packet.verdict,
                packet.created_at,
                packet.decided_by,
                packet.decided_at,
            ),
        )
        self.enqueue_projection(event_id, "project_customer_visible_packet_projection")
        return packet

    def resolve_project_customer_visible_packet(
        self,
        packet_id: str,
        *,
        verdict: str,
        side_effect_receipt_id: str | None = None,
        decided_by: str = "operator",
        notes: str | None = None,
        confidence: float | None = None,
    ) -> dict[str, Any]:
        if verdict not in {"accept_customer_visible_packet", "reject_customer_visible_packet", "defer"}:
            raise ValueError("customer-visible verdict must be accept_customer_visible_packet, reject_customer_visible_packet, or defer")
        if self.command.requested_by != "operator":
            raise PermissionError("customer-visible packet resolution requires an operator command")
        if self.command.requested_authority != "operator_gate":
            raise PermissionError("customer-visible packet resolution requires operator-gate authority")
        if self.command.payload.get("autonomous_customer_visible") or self.command.payload.get("external_action_executed"):
            raise PermissionError("customer-visible packet resolution cannot record autonomous external actions")
        row = self.conn.execute(
            """
            SELECT p.*, d.status AS decision_status
            FROM project_customer_visible_packets p
            JOIN decisions d ON d.decision_id = p.decision_id
            WHERE p.packet_id=?
            """,
            (packet_id,),
        ).fetchone()
        if row is None:
            raise ValueError("customer-visible packet not found")
        if row["status"] != "gated" or row["decision_status"] != "gated":
            raise ValueError("customer-visible packet is not gated")
        if verdict == "accept_customer_visible_packet" and not side_effect_receipt_id:
            raise PermissionError("accepted customer-visible packets require a durable side-effect receipt")
        if verdict != "accept_customer_visible_packet" and side_effect_receipt_id:
            raise PermissionError("rejected or deferred customer-visible packets cannot record side-effect receipts")

        self.resolve_decision(
            row["decision_id"],
            verdict=verdict,
            decided_by=decided_by,
            notes=notes,
            confidence=confidence,
        )
        decided_at = now_iso()
        commitment_id: str | None = None
        commitment_payload: dict[str, Any] | None = None
        if verdict == "accept_customer_visible_packet":
            side_effect = self._successful_task_side_effect(
                task_id=self._outcome_task_id(row["outcome_id"]),
                receipt_id=side_effect_receipt_id or "",
                intent_id=row["side_effect_intent_id"],
            )
            evidence_refs = _merge_refs(
                _loads(row["evidence_refs_json"]),
                [
                    f"kernel:project_customer_visible_packets/{packet_id}",
                    f"kernel:side_effect_receipts/{side_effect_receipt_id}",
                ],
            )
            commitment = ProjectCustomerCommitment(
                packet_id=packet_id,
                project_id=row["project_id"],
                outcome_id=row["outcome_id"],
                side_effect_intent_id=side_effect["intent_id"],
                side_effect_receipt_id=side_effect_receipt_id or "",
                customer_ref=row["customer_ref"],
                channel=row["channel"],
                commitment_type="message_sent" if row["packet_type"] == "customer_message" else "delivery_made",
                payload_ref=row["payload_ref"],
                summary=row["summary"],
                evidence_refs=evidence_refs,
            )
            commitment_payload = _project_customer_commitment_payload(commitment)
            commitment_event_id = self.append_event(
                "project_customer_commitment_recorded",
                "project",
                commitment.commitment_id,
                commitment_payload,
                actor_type="operator",
                actor_id=decided_by,
            )
            self.conn.execute(
                """
                INSERT INTO project_customer_commitments (
                  commitment_id, packet_id, project_id, outcome_id,
                  side_effect_intent_id, side_effect_receipt_id, customer_ref,
                  channel, commitment_type, payload_ref, summary,
                  evidence_refs_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    commitment.commitment_id,
                    commitment.packet_id,
                    commitment.project_id,
                    commitment.outcome_id,
                    commitment.side_effect_intent_id,
                    commitment.side_effect_receipt_id,
                    commitment.customer_ref,
                    commitment.channel,
                    commitment.commitment_type,
                    commitment.payload_ref,
                    commitment.summary,
                    canonical_json(commitment.evidence_refs),
                    commitment.created_at,
                ),
            )
            self.enqueue_projection(commitment_event_id, "project_customer_commitment_projection")
            commitment_id = commitment.commitment_id

        payload = {
            "packet_id": packet_id,
            "decision_id": row["decision_id"],
            "project_id": row["project_id"],
            "outcome_id": row["outcome_id"],
            "verdict": verdict,
            "decided_by": decided_by,
            "notes": notes,
            "side_effect_intent_id": row["side_effect_intent_id"],
            "side_effect_receipt_id": side_effect_receipt_id,
            "customer_commitment_id": commitment_id,
            "customer_commitments": [commitment_payload] if commitment_payload else [],
            "external_effects_executed_by_replay": False,
            "decided_at": decided_at,
        }
        event_id = self.append_event(
            "project_customer_visible_packet_resolved",
            "decision",
            packet_id,
            payload,
            actor_type="operator",
            actor_id=decided_by,
        )
        self.conn.execute(
            """
            UPDATE project_customer_visible_packets
            SET status='decided', verdict=?, decided_by=?, decided_at=?
            WHERE packet_id=?
            """,
            (verdict, decided_by, decided_at, packet_id),
        )
        self.enqueue_projection(event_id, "project_customer_visible_packet_projection")
        return {
            "packet_id": packet_id,
            "decision_id": row["decision_id"],
            "verdict": verdict,
            "customer_commitment_id": commitment_id,
            "customer_commitments": [commitment_payload] if commitment_payload else [],
            "side_effect_intent_id": row["side_effect_intent_id"],
            "side_effect_receipt_id": side_effect_receipt_id,
        }

    def compare_project_customer_visible_replay_to_projection(
        self,
        packet_id: str,
    ) -> ProjectCustomerVisibleReplayProjectionComparison:
        replay = self.__class__._replay_from_connection(self.conn)
        replay_packet = replay.project_customer_visible_packets.get(packet_id)
        row = self.conn.execute(
            "SELECT * FROM project_customer_visible_packets WHERE packet_id=?",
            (packet_id,),
        ).fetchone()
        if row is None:
            raise ValueError("customer-visible packet not found")
        projection_packet = _project_customer_visible_packet_from_row(row)
        replay_commitments = sorted(
            (
                commitment
                for commitment in replay.project_customer_commitments.values()
                if commitment.get("packet_id") == packet_id
            ),
            key=lambda item: item["commitment_id"],
        )
        projection_commitments = self._project_customer_commitments_for_packet(packet_id)
        replay_commitment_ids = {commitment["commitment_id"] for commitment in replay_commitments}
        projection_commitment_ids = {commitment["commitment_id"] for commitment in projection_commitments}
        replay_commitment_receipts = sorted(
            (
                receipt
                for receipt in replay.project_customer_commitment_receipts.values()
                if receipt.get("commitment_id") in replay_commitment_ids
            ),
            key=lambda item: item["receipt_id"],
        )
        projection_commitment_receipts = self._project_customer_commitment_receipts_for_commitments(
            projection_commitment_ids
        )
        mismatches: list[str] = []
        if replay_packet != projection_packet:
            mismatches.append("project_customer_visible_packet")
        if replay_commitments != projection_commitments:
            mismatches.append("project_customer_commitments")
        if replay_commitment_receipts != projection_commitment_receipts:
            mismatches.append("project_customer_commitment_receipts")
        comparison = ProjectCustomerVisibleReplayProjectionComparison(
            packet_id=packet_id,
            replay_packet=replay_packet or {},
            projection_packet=projection_packet,
            replay_commitments=replay_commitments,
            projection_commitments=projection_commitments,
            replay_commitment_receipts=replay_commitment_receipts,
            projection_commitment_receipts=projection_commitment_receipts,
            matches=not mismatches,
            mismatches=mismatches,
        )
        payload = _project_customer_visible_replay_projection_comparison_payload(comparison)
        event_id = self.append_event("project_customer_visible_replay_projection_compared", "project", comparison.comparison_id, payload)
        self.conn.execute(
            """
            INSERT INTO project_customer_visible_replay_projection_comparisons (
              comparison_id, packet_id, replay_packet_json, projection_packet_json,
              replay_commitments_json, projection_commitments_json,
              replay_commitment_receipts_json, projection_commitment_receipts_json,
              matches, mismatches_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                comparison.comparison_id,
                comparison.packet_id,
                canonical_json(comparison.replay_packet),
                canonical_json(comparison.projection_packet),
                canonical_json(comparison.replay_commitments),
                canonical_json(comparison.projection_commitments),
                canonical_json(comparison.replay_commitment_receipts),
                canonical_json(comparison.projection_commitment_receipts),
                int(comparison.matches),
                canonical_json(comparison.mismatches),
                comparison.created_at,
            ),
        )
        self.enqueue_projection(event_id, "project_customer_visible_replay_projection_comparison_projection")
        return comparison

    def _outcome_task_id(self, outcome_id: str) -> str:
        row = self.conn.execute("SELECT task_id FROM project_outcomes WHERE outcome_id=?", (outcome_id,)).fetchone()
        if row is None or not row["task_id"]:
            raise ValueError("customer-visible packet outcome lacks task evidence")
        return row["task_id"]

    def _project_customer_commitments_for_packet(self, packet_id: str) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            """
            SELECT * FROM project_customer_commitments
            WHERE packet_id=?
            ORDER BY commitment_id
            """,
            (packet_id,),
        ).fetchall()
        return [_project_customer_commitment_from_row(row) for row in rows]

    def _project_customer_commitment_receipts_for_commitments(self, commitment_ids: set[str]) -> list[dict[str, Any]]:
        if not commitment_ids:
            return []
        placeholders = ",".join("?" for _ in commitment_ids)
        rows = self.conn.execute(
            f"""
            SELECT * FROM project_customer_commitment_receipts
            WHERE commitment_id IN ({placeholders})
            ORDER BY receipt_id
            """,
            tuple(sorted(commitment_ids)),
        ).fetchall()
        return [_project_customer_commitment_receipt_from_row(row) for row in rows]

    def _create_scheduling_priority_task(self, packet_row: sqlite3.Row, change: dict[str, Any]) -> str:
        if change.get("customer_visible") or change.get("external_side_effects_authorized"):
            raise PermissionError("scheduling priority changes cannot create customer-visible or side-effecting work")
        task = ProjectTask(
            project_id=change["project_id"],
            phase_name="Operate",
            task_type=change["task_type"],
            autonomy_class="A1",
            objective=f"Execute internal scheduling priority rank {change['priority_rank']} for {packet_row['scheduling_window']}.",
            inputs={
                "scheduling_priority_packet_id": packet_row["packet_id"],
                "scheduling_intent_id": packet_row["intent_id"],
                "portfolio_packet_id": packet_row["portfolio_packet_id"],
                "priority_rank": change["priority_rank"],
                "queue_action": change["queue_action"],
                "tradeoff_drivers": change["tradeoff_drivers"],
                "customer_visible": False,
                "external_side_effects_authorized": [],
                "customer_commitments_allowed": False,
                "cancellation_allowed": False,
            },
            expected_output_schema={
                "type": "object",
                "required": ["internal_result_ref", "external_commitment_change"],
                "properties": {
                    "internal_result_ref": {"type": "string"},
                    "external_commitment_change": {"const": False},
                },
            },
            risk_level="low",
            required_capabilities=[
                {
                    "capability_type": "memory_write",
                    "actions": ["record"],
                    "scope": "project_internal_scheduling",
                    "grant_required_before_run": True,
                    "external_side_effects": "blocked_without_operator_gate_and_receipt",
                }
            ],
            model_requirement={
                "task_class": "quick_research_summarization",
                "local_allowed_only_if_promoted": True,
                "frontier_fallback_allowed_with_budget": False,
            },
            authority_required="rule",
            recovery_policy="ask_operator",
            budget_id=self._project_budget_id(change["project_id"]),
            idempotency_key=f"scheduling-priority:{packet_row['packet_id']}:{change['project_id']}:{change['priority_rank']}",
            evidence_refs=[
                f"kernel:project_scheduling_priority_change_packets/{packet_row['packet_id']}",
                f"kernel:project_scheduling_intents/{packet_row['intent_id']}",
                *change.get("evidence_refs", []),
            ],
        )
        return self.create_project_task(task)

    def _portfolio_project_recommendation(
        self,
        project: sqlite3.Row,
        rollup: ProjectStatusRollup,
        constraints: dict[str, Any],
    ) -> dict[str, Any]:
        commercial = rollup.commercial_rollup
        budget = self._project_budget_summary(self._project_budget_id(project["project_id"]))
        revenue = Decimal(commercial.get("revenue_reconciled_usd", "0"))
        unreconciled = Decimal(commercial.get("revenue_unreconciled_usd", "0"))
        retained = int(commercial.get("retained_customer_count", 0))
        at_risk = int(commercial.get("at_risk_customer_count", 0))
        churned = int(commercial.get("churned_customer_count", 0))
        support_open = int(commercial.get("support_open_count", 0))
        maintenance_open = int(commercial.get("maintenance_open_count", 0))
        receiptless = int(commercial.get("receiptless_side_effect_count", 0))
        load_minutes = int(rollup.operator_load_minutes)
        score = int(revenue) + retained * 150 - load_minutes * 2 - at_risk * 120 - churned * 300
        score -= support_open * 40 + maintenance_open * 80 + receiptless * 200 + int(unreconciled / Decimal("2"))
        min_remaining = Decimal(str(constraints.get("min_budget_remaining_usd", "0")))
        if budget["remaining_usd"] < min_remaining:
            score -= 100
        if rollup.close_recommendation == "complete":
            action = "harvest_or_complete"
        elif rollup.close_recommendation == "kill":
            action = "kill_or_stop_investment"
        elif rollup.close_recommendation == "pause":
            action = "pause_until_operator_review"
        elif score >= int(constraints.get("accelerate_score_threshold", 250)):
            action = "prioritize_next"
        else:
            action = "continue_bounded"
        return {
            "project_id": project["project_id"],
            "project_name": self._project_name(project["project_id"]),
            "project_status": project["status"],
            "rollup_id": rollup.rollup_id,
            "commercial_rollup_id": rollup.commercial_rollup_id,
            "recommended_action": action,
            "priority_score": score,
            "close_recommendation": rollup.close_recommendation,
            "budget": {
                **budget,
                "cap_usd": str(budget["cap_usd"]),
                "spent_usd": str(budget["spent_usd"]),
                "reserved_usd": str(budget["reserved_usd"]),
                "remaining_usd": str(budget["remaining_usd"]),
            },
            "operator_load_minutes": load_minutes,
            "retention": {
                "retained": retained,
                "at_risk": at_risk,
                "churned": churned,
            },
            "revenue": {
                "reconciled_usd": str(revenue),
                "unreconciled_usd": str(unreconciled),
            },
            "support_open_count": support_open,
            "maintenance_open_count": maintenance_open,
            "risk_flags": rollup.risk_flags,
            "evidence_refs": _merge_refs(
                [f"kernel:project_status_rollups/{rollup.rollup_id}"],
                (
                    [f"kernel:project_commercial_rollups/{rollup.commercial_rollup_id}"]
                    if rollup.commercial_rollup_id
                    else []
                ),
                rollup.commercial_rollup.get("evidence_refs", []),
            ),
        }

    def _project_budget_summary(self, budget_id: str | None) -> dict[str, Any]:
        if budget_id is None:
            return {
                "budget_id": None,
                "cap_usd": Decimal("0"),
                "spent_usd": Decimal("0"),
                "reserved_usd": Decimal("0"),
                "remaining_usd": Decimal("0"),
                "status": "none",
            }
        row = self.conn.execute(
            "SELECT budget_id, cap_usd, spent_usd, reserved_usd, status FROM budgets WHERE budget_id=?",
            (budget_id,),
        ).fetchone()
        if row is None:
            return {
                "budget_id": budget_id,
                "cap_usd": Decimal("0"),
                "spent_usd": Decimal("0"),
                "reserved_usd": Decimal("0"),
                "remaining_usd": Decimal("0"),
                "status": "missing",
            }
        cap = Decimal(row["cap_usd"])
        spent = Decimal(row["spent_usd"])
        reserved = Decimal(row["reserved_usd"])
        return {
            "budget_id": row["budget_id"],
            "cap_usd": cap,
            "spent_usd": spent,
            "reserved_usd": reserved,
            "remaining_usd": cap - spent - reserved,
            "status": row["status"],
        }

    def _project_budget_id(self, project_id: str) -> str | None:
        row = self.conn.execute("SELECT budget_id FROM projects WHERE project_id=?", (project_id,)).fetchone()
        return row["budget_id"] if row else None

    def _project_name(self, project_id: str) -> str:
        row = self.conn.execute("SELECT name FROM projects WHERE project_id=?", (project_id,)).fetchone()
        return row["name"] if row else project_id

    def _latest_project_commercial_rollup_payload(self, project_id: str) -> dict[str, Any]:
        row = self.conn.execute(
            """
            SELECT *
            FROM project_commercial_rollups
            WHERE project_id=?
            ORDER BY created_at DESC, rollup_id DESC
            LIMIT 1
            """,
            (project_id,),
        ).fetchone()
        if row is None:
            return {}
        return _project_commercial_rollup_payload(_commercial_rollup_from_row(row))

    def _project_phase_names(self, project_id: str) -> list[str]:
        row = self.conn.execute("SELECT phases_json FROM projects WHERE project_id=?", (project_id,)).fetchone()
        phases = _loads(row["phases_json"]) if row else []
        names = [phase.get("name") or phase.get("phase_name") for phase in phases if phase.get("name") or phase.get("phase_name")]
        task_rows = self.conn.execute(
            "SELECT DISTINCT phase_name FROM project_tasks WHERE project_id=? AND phase_name IS NOT NULL",
            (project_id,),
        ).fetchall()
        for task_row in task_rows:
            if task_row["phase_name"] not in names:
                names.append(task_row["phase_name"])
        return names or ["Unphased"]

    def _create_feedback_followup_task(self, project_id: str, packet_id: str, rollup_id: str) -> str | None:
        feedback = self.conn.execute(
            """
            SELECT f.feedback_id, f.task_id, f.artifact_receipt_id, f.source_type,
                   f.customer_ref, f.summary, f.sentiment, f.evidence_refs_json,
                   f.created_at, t.task_type AS source_task_type
            FROM project_customer_feedback f
            LEFT JOIN project_tasks t ON t.task_id = f.task_id
            WHERE f.project_id=?
              AND f.action_required=1
              AND f.status IN ('needs_followup', 'accepted')
              AND (t.task_type='validate' OR f.task_id IS NULL)
            ORDER BY f.created_at DESC, f.feedback_id DESC
            LIMIT 1
            """,
            (project_id,),
        ).fetchone()
        if feedback is None:
            return None
        task_key = f"commercial-feedback-followup:{project_id}:{feedback['feedback_id']}"
        existing = self.conn.execute(
            "SELECT task_id FROM project_tasks WHERE project_id=? AND idempotency_key=?",
            (project_id, task_key),
        ).fetchone()
        if existing is not None:
            return existing["task_id"]

        summary = feedback["summary"].strip()
        lower_summary = summary.lower()
        if any(term in lower_summary for term in ("build", "change", "revise", "iteration", "follow-up", "follow up")):
            task_type = "build"
        elif any(term in lower_summary for term in ("ship", "publish", "deploy", "send to customer")):
            task_type = "ship"
        else:
            task_type = "build"
        phase_name = "Ship" if task_type == "ship" else "Build"
        authority = "operator_gate" if task_type == "ship" else "single_agent"
        required_capabilities = [
            {
                "capability_type": "side_effect" if task_type == "ship" else "file",
                "actions": ["prepare"] if task_type == "ship" else ["read", "write"],
                "scope": "project_delivery" if task_type == "ship" else "project_workspace",
                "grant_required_before_run": True,
            }
        ]
        evidence_refs = [f"kernel:project_customer_feedback/{feedback['feedback_id']}", f"kernel:project_status_rollups/{rollup_id}"]
        evidence_refs.extend(_loads(feedback["evidence_refs_json"]))
        task = ProjectTask(
            project_id=project_id,
            phase_name=phase_name,
            task_type=task_type,  # type: ignore[arg-type]
            autonomy_class="A2",
            objective=f"Address accepted validation feedback: {summary}",
            inputs={
                "close_decision_packet_id": packet_id,
                "rollup_id": rollup_id,
                "feedback_id": feedback["feedback_id"],
                "source_task_id": feedback["task_id"],
                "artifact_receipt_id": feedback["artifact_receipt_id"],
                "source_type": feedback["source_type"],
                "customer_ref": feedback["customer_ref"],
                "sentiment": feedback["sentiment"],
                "summary": summary,
            },
            expected_output_schema={
                "type": "object",
                "required": ["artifact_ref", "change_summary", "operator_load_actual", "next_recommendation"],
            },
            risk_level="medium",
            required_capabilities=required_capabilities,
            model_requirement={
                "task_class": "coding_small_patch",
                "local_allowed_only_if_promoted": True,
                "frontier_fallback_allowed_with_budget": False,
            },
            authority_required=authority,  # type: ignore[arg-type]
            recovery_policy="ask_operator",
            idempotency_key=task_key,
            evidence_refs=evidence_refs,
        )
        return self.create_project_task(task)

    def _create_post_ship_operate_followup_task(self, project_id: str, packet_id: str, rollup_id: str) -> str | None:
        feedback = self.conn.execute(
            """
            SELECT f.feedback_id, f.task_id, f.artifact_receipt_id, f.source_type,
                   f.customer_ref, f.summary, f.sentiment, f.evidence_refs_json,
                   f.created_at
            FROM project_customer_feedback f
            JOIN project_artifact_receipts a ON a.receipt_id = f.artifact_receipt_id
            WHERE f.project_id=?
              AND f.action_required=1
              AND f.status='accepted'
              AND a.artifact_kind='shipped_artifact'
              AND a.customer_visible=1
              AND a.side_effect_receipt_id IS NOT NULL
              AND a.status='accepted'
            ORDER BY f.created_at DESC, f.feedback_id DESC
            LIMIT 1
            """,
            (project_id,),
        ).fetchone()
        if feedback is None:
            return None
        task_key = f"commercial-operate-followup:{project_id}:{feedback['feedback_id']}"
        existing = self.conn.execute(
            "SELECT task_id FROM project_tasks WHERE project_id=? AND idempotency_key=?",
            (project_id, task_key),
        ).fetchone()
        if existing is not None:
            return existing["task_id"]

        followup_type = _operate_followup_type(feedback["summary"])
        capability_scope_by_type = {
            "revenue_reconciliation": "project_revenue_reconciliation",
            "retention": "project_retention_analysis",
            "maintenance": "project_maintenance_triage",
            "customer_support": "project_customer_support_draft",
        }
        load_type_by_followup = {
            "revenue_reconciliation": "reconciliation",
            "retention": "client_sales",
            "maintenance": "maintenance",
            "customer_support": "other",
        }
        summary = feedback["summary"].strip()
        evidence_refs = [
            f"kernel:project_customer_feedback/{feedback['feedback_id']}",
            f"kernel:project_artifact_receipts/{feedback['artifact_receipt_id']}",
            f"kernel:project_status_rollups/{rollup_id}",
        ]
        evidence_refs.extend(_loads(feedback["evidence_refs_json"]))
        task = ProjectTask(
            project_id=project_id,
            phase_name="Operate",
            task_type="operate",
            autonomy_class="A1",
            objective=f"Prepare governed Operate follow-up for accepted post-ship feedback: {summary}",
            inputs={
                "close_decision_packet_id": packet_id,
                "rollup_id": rollup_id,
                "feedback_id": feedback["feedback_id"],
                "source_task_id": feedback["task_id"],
                "artifact_receipt_id": feedback["artifact_receipt_id"],
                "source_type": feedback["source_type"],
                "customer_ref": feedback["customer_ref"],
                "sentiment": feedback["sentiment"],
                "summary": summary,
                "operate_followup_type": followup_type,
                "external_commitment_policy": "draft_or_internal_only_without_side_effect_receipt",
                "default_operator_load_type": load_type_by_followup[followup_type],
            },
            expected_output_schema={
                "type": "object",
                "required": [
                    "operate_followup_type",
                    "internal_result_ref",
                    "evidence_refs",
                    "operator_load_actual",
                    "external_commitment_change",
                    "side_effect_receipt_id",
                ],
                "properties": {
                    "external_commitment_change": {"const": False},
                    "side_effect_receipt_id": {"type": ["string", "null"]},
                },
            },
            risk_level="low",
            required_capabilities=[
                {
                    "capability_type": "memory_write",
                    "actions": ["record"],
                    "scope": capability_scope_by_type[followup_type],
                    "grant_required_before_run": True,
                    "external_side_effects": "blocked_without_operator_gate_and_receipt",
                }
            ],
            model_requirement={
                "task_class": "quick_research_summarization",
                "local_allowed_only_if_promoted": True,
                "frontier_fallback_allowed_with_budget": False,
            },
            authority_required="rule",
            recovery_policy="ask_operator",
            idempotency_key=task_key,
            evidence_refs=evidence_refs,
        )
        return self.create_project_task(task)

    def _create_commitment_receipt_followup_task(
        self,
        commitment: sqlite3.Row,
        receipt: ProjectCustomerCommitmentReceipt,
        *,
        customer_ref: str,
        evidence_refs: list[str],
    ) -> str:
        task_key = f"commercial-commitment-receipt-followup:{receipt.project_id}:{receipt.receipt_id}"
        existing = self.conn.execute(
            "SELECT task_id FROM project_tasks WHERE project_id=? AND idempotency_key=?",
            (receipt.project_id, task_key),
        ).fetchone()
        if existing is not None:
            return existing["task_id"]
        followup_type = _commitment_receipt_followup_type(receipt.receipt_type, receipt.summary)
        load_type_by_followup = {
            "revenue_reconciliation": "reconciliation",
            "retention": "client_sales",
            "maintenance": "maintenance",
            "customer_support": "other",
        }
        capability_scope_by_type = {
            "revenue_reconciliation": "project_revenue_reconciliation",
            "retention": "project_retention_analysis",
            "maintenance": "project_maintenance_triage",
            "customer_support": "project_customer_support_draft",
        }
        task = ProjectTask(
            project_id=receipt.project_id,
            phase_name="Operate",
            task_type="operate",
            autonomy_class="A1",
            objective=f"Prepare governed Operate follow-up for customer commitment receipt: {receipt.summary.strip()}",
            inputs={
                "commitment_id": receipt.commitment_id,
                "customer_commitment_receipt_id": receipt.receipt_id,
                "source_outcome_id": commitment["outcome_id"],
                "customer_ref": customer_ref,
                "receipt_type": receipt.receipt_type,
                "source_type": receipt.source_type,
                "summary": receipt.summary,
                "operate_followup_type": followup_type,
                "external_commitment_policy": "draft_or_internal_only_without_side_effect_receipt",
                "default_operator_load_type": load_type_by_followup[followup_type],
            },
            expected_output_schema={
                "type": "object",
                "required": [
                    "operate_followup_type",
                    "internal_result_ref",
                    "evidence_refs",
                    "operator_load_actual",
                    "external_commitment_change",
                    "side_effect_receipt_id",
                ],
                "properties": {
                    "operate_followup_type": {"const": followup_type},
                    "external_commitment_change": {"const": False},
                    "side_effect_receipt_id": {"type": ["string", "null"]},
                },
            },
            risk_level="low",
            required_capabilities=[
                {
                    "capability_type": "memory_write",
                    "actions": ["record"],
                    "scope": capability_scope_by_type[followup_type],
                    "grant_required_before_run": True,
                    "external_side_effects": "blocked_without_operator_gate_and_receipt",
                }
            ],
            model_requirement={
                "task_class": "quick_research_summarization",
                "local_allowed_only_if_promoted": True,
                "frontier_fallback_allowed_with_budget": False,
            },
            authority_required="rule",
            recovery_policy="ask_operator",
            idempotency_key=task_key,
            evidence_refs=_merge_refs(
                evidence_refs,
                [
                    f"kernel:project_customer_commitment_receipts/{receipt.receipt_id}",
                    f"kernel:project_outcomes/{commitment['outcome_id']}",
                ],
            ),
        )
        return self.create_project_task(task)

    def _create_ship_task_from_build_delivery(
        self,
        *,
        project_id: str,
        build_task_id: str,
        build_artifact_receipt_id: str,
        artifact_ref: str,
        summary: str,
        source_evidence_refs: list[str],
    ) -> str:
        task_key = f"commercial-build-ship:{project_id}:{build_artifact_receipt_id}"
        existing = self.conn.execute(
            "SELECT task_id FROM project_tasks WHERE project_id=? AND idempotency_key=?",
            (project_id, task_key),
        ).fetchone()
        if existing is not None:
            return existing["task_id"]
        evidence_refs = [
            f"kernel:project_tasks/{build_task_id}",
            f"kernel:project_artifact_receipts/{build_artifact_receipt_id}",
        ]
        evidence_refs.extend(source_evidence_refs)
        task = ProjectTask(
            project_id=project_id,
            phase_name="Ship",
            task_type="ship",
            autonomy_class="A2",
            objective=f"Prepare operator-gated delivery for build artifact: {summary}",
            inputs={
                "build_task_id": build_task_id,
                "build_artifact_receipt_id": build_artifact_receipt_id,
                "artifact_ref": artifact_ref,
                "summary": summary,
            },
            expected_output_schema={
                "type": "object",
                "required": ["side_effect_receipt_id", "artifact_ref", "delivery_channel", "operator_load_actual"],
            },
            risk_level="medium",
            required_capabilities=[
                {
                    "capability_type": "side_effect",
                    "actions": ["prepare"],
                    "scope": "project_delivery",
                    "grant_required_before_run": True,
                }
            ],
            model_requirement={
                "task_class": "coding_small_patch",
                "local_allowed_only_if_promoted": True,
                "frontier_fallback_allowed_with_budget": False,
            },
            authority_required="operator_gate",
            recovery_policy="ask_operator",
            idempotency_key=task_key,
            evidence_refs=evidence_refs,
        )
        return self.create_project_task(task)

    def _derive_project_phase_rollup(self, project_id: str, phase_name: str) -> ProjectPhaseRollup:
        task_counts = self._project_task_counts(project_id, phase_name=phase_name)
        outcome_counts = self._project_outcome_counts(project_id, phase_name=phase_name)
        artifact_count = self._count_project_rows("project_artifact_receipts", project_id, phase_name=phase_name)
        feedback_count = self._count_project_rows("project_customer_feedback", project_id, phase_name=phase_name)
        revenue_total = self._project_revenue_total(project_id, phase_name=phase_name)
        load_minutes = self._project_operator_load_minutes(project_id, phase_name=phase_name)
        last_activity_at = self._project_phase_last_activity(project_id, phase_name)
        if not sum(task_counts.values()):
            status = "not_started"
        elif task_counts.get("failed", 0):
            status = "failed"
        elif task_counts.get("blocked", 0):
            status = "blocked"
        elif task_counts.get("running", 0) or task_counts.get("queued", 0):
            status = "active"
        elif task_counts.get("completed", 0):
            status = "complete"
        else:
            status = "at_risk"
        return ProjectPhaseRollup(
            phase_name=phase_name,
            task_counts=task_counts,
            outcome_counts=outcome_counts,
            artifact_count=artifact_count,
            customer_feedback_count=feedback_count,
            revenue_attributed_usd=revenue_total,
            operator_load_minutes=load_minutes,
            status=status,  # type: ignore[arg-type]
            last_activity_at=last_activity_at,
        )

    def _project_task_counts(self, project_id: str, *, phase_name: str | None = None) -> dict[str, int]:
        params: list[Any] = [project_id]
        clause = "project_id=?"
        if phase_name is not None:
            clause += " AND COALESCE(phase_name, 'Unphased')=?"
            params.append(phase_name)
        rows = self.conn.execute(
            f"SELECT status, COUNT(*) AS count FROM project_tasks WHERE {clause} GROUP BY status",
            params,
        ).fetchall()
        return {row["status"]: int(row["count"]) for row in rows}

    def _project_outcome_counts(self, project_id: str, *, phase_name: str | None = None) -> dict[str, int]:
        params: list[Any] = [project_id]
        clause = "project_id=?"
        if phase_name is not None:
            clause += " AND COALESCE(phase_name, 'Unphased')=?"
            params.append(phase_name)
        rows = self.conn.execute(
            f"SELECT outcome_type, COUNT(*) AS count FROM project_outcomes WHERE {clause} GROUP BY outcome_type",
            params,
        ).fetchall()
        return {row["outcome_type"]: int(row["count"]) for row in rows}

    def _count_project_rows(self, table: str, project_id: str, *, phase_name: str | None = None) -> int:
        allowed = {"project_artifact_receipts", "project_customer_feedback"}
        if table not in allowed:
            raise ValueError("unsupported project count table")
        if phase_name is None:
            return int(self.conn.execute(f"SELECT COUNT(*) AS count FROM {table} WHERE project_id=?", (project_id,)).fetchone()["count"])
        return int(
            self.conn.execute(
                f"""
                SELECT COUNT(*) AS count
                FROM {table} r
                LEFT JOIN project_tasks t ON t.task_id = r.task_id
                WHERE r.project_id=? AND COALESCE(t.phase_name, 'Unphased')=?
                """,
                (project_id, phase_name),
            ).fetchone()["count"]
        )

    def _project_revenue_total(self, project_id: str, *, phase_name: str | None = None) -> Decimal:
        if phase_name is None:
            rows = self.conn.execute(
                "SELECT amount_usd FROM project_revenue_attributions WHERE project_id=?",
                (project_id,),
            ).fetchall()
        else:
            rows = self.conn.execute(
                """
                SELECT r.amount_usd
                FROM project_revenue_attributions r
                LEFT JOIN project_tasks t ON t.task_id = r.task_id
                WHERE r.project_id=? AND COALESCE(t.phase_name, 'Unphased')=?
                """,
                (project_id, phase_name),
            ).fetchall()
        return sum((Decimal(row["amount_usd"]) for row in rows), Decimal("0"))

    def _project_operator_load_minutes(self, project_id: str, *, phase_name: str | None = None) -> int:
        if phase_name is None:
            row = self.conn.execute(
                "SELECT COALESCE(SUM(minutes), 0) AS minutes FROM project_operator_load WHERE project_id=?",
                (project_id,),
            ).fetchone()
        else:
            row = self.conn.execute(
                """
                SELECT COALESCE(SUM(l.minutes), 0) AS minutes
                FROM project_operator_load l
                LEFT JOIN project_tasks t ON t.task_id = l.task_id
                WHERE l.project_id=? AND COALESCE(t.phase_name, 'Unphased')=?
                """,
                (project_id, phase_name),
            ).fetchone()
        return int(row["minutes"])

    def _project_post_ship_evidence_summary(self, project_id: str) -> dict[str, Any]:
        shipped = self.conn.execute(
            """
            SELECT COUNT(*) AS count
            FROM project_artifact_receipts
            WHERE project_id=? AND artifact_kind='shipped_artifact' AND customer_visible=1
            """,
            (project_id,),
        ).fetchone()
        feedback = self.conn.execute(
            """
            SELECT
              COUNT(*) AS count,
              COALESCE(SUM(CASE WHEN sentiment IN ('negative', 'mixed') THEN 1 ELSE 0 END), 0) AS negative_count,
              COALESCE(SUM(CASE WHEN sentiment = 'positive' THEN 1 ELSE 0 END), 0) AS positive_count,
              COALESCE(SUM(CASE WHEN action_required = 1 AND status = 'accepted' THEN 1 ELSE 0 END), 0) AS action_count,
              COALESCE(SUM(CASE WHEN action_required = 1 OR status = 'needs_followup' THEN 1 ELSE 0 END), 0) AS open_followup_count
            FROM project_customer_feedback
            WHERE project_id=? AND artifact_receipt_id IN (
              SELECT receipt_id
              FROM project_artifact_receipts
              WHERE project_id=? AND artifact_kind='shipped_artifact' AND customer_visible=1
            )
            """,
            (project_id, project_id),
        ).fetchone()
        revenue_rows = self.conn.execute(
            """
            SELECT amount_usd
            FROM project_revenue_attributions
            WHERE project_id=? AND artifact_receipt_id IN (
              SELECT receipt_id
              FROM project_artifact_receipts
              WHERE project_id=? AND artifact_kind='shipped_artifact' AND customer_visible=1
            )
            """,
            (project_id, project_id),
        ).fetchall()
        load = self.conn.execute(
            """
            SELECT COALESCE(SUM(minutes), 0) AS minutes
            FROM project_operator_load
            WHERE project_id=? AND artifact_receipt_id IN (
              SELECT receipt_id
              FROM project_artifact_receipts
              WHERE project_id=? AND artifact_kind='shipped_artifact' AND customer_visible=1
            )
            """,
            (project_id, project_id),
        ).fetchone()
        return {
            "shipped_artifact_count": int(shipped["count"]),
            "feedback_count": int(feedback["count"]),
            "positive_feedback_count": int(feedback["positive_count"]),
            "negative_feedback_count": int(feedback["negative_count"]),
            "action_required_count": int(feedback["action_count"]),
            "open_followup_count": int(feedback["open_followup_count"]),
            "revenue_attributed_usd": sum((Decimal(row["amount_usd"]) for row in revenue_rows), Decimal("0")),
            "operator_load_minutes": int(load["minutes"]),
        }

    def _project_commitment_receipt_summary(self, project_id: str) -> dict[str, Any]:
        row = self.conn.execute(
            """
            SELECT
              COUNT(*) AS count,
              COALESCE(SUM(CASE WHEN action_required = 1 OR status = 'needs_followup' THEN 1 ELSE 0 END), 0) AS open_followup_count,
              COALESCE(SUM(CASE WHEN receipt_type = 'delivery_failure' THEN 1 ELSE 0 END), 0) AS delivery_failure_count,
              COALESCE(SUM(CASE WHEN receipt_type = 'timeout' THEN 1 ELSE 0 END), 0) AS timeout_count,
              COALESCE(SUM(CASE WHEN receipt_type = 'compensation_needed' THEN 1 ELSE 0 END), 0) AS compensation_needed_count
            FROM project_customer_commitment_receipts
            WHERE project_id=?
            """,
            (project_id,),
        ).fetchone()
        return {
            "count": int(row["count"]),
            "open_followup_count": int(row["open_followup_count"]),
            "delivery_failure_count": int(row["delivery_failure_count"]),
            "timeout_count": int(row["timeout_count"]),
            "compensation_needed_count": int(row["compensation_needed_count"]),
        }

    def _project_phase_last_activity(self, project_id: str, phase_name: str) -> str | None:
        rows = self.conn.execute(
            """
            SELECT MAX(created_at) AS last_activity_at FROM (
              SELECT created_at FROM project_tasks WHERE project_id=? AND COALESCE(phase_name, 'Unphased')=?
              UNION ALL
              SELECT created_at FROM project_outcomes WHERE project_id=? AND COALESCE(phase_name, 'Unphased')=?
            )
            """,
            (project_id, phase_name, project_id, phase_name),
        ).fetchone()
        return rows["last_activity_at"] if rows else None

    def _require_project(self, project_id: str) -> sqlite3.Row:
        project = self.conn.execute("SELECT project_id, status FROM projects WHERE project_id=?", (project_id,)).fetchone()
        if project is None:
            raise ValueError("project record requires an existing project")
        return project

    def _require_project_task(self, project_id: str, task_id: str) -> sqlite3.Row:
        task = self.conn.execute(
            "SELECT task_id, project_id FROM project_tasks WHERE task_id=?",
            (task_id,),
        ).fetchone()
        if task is None:
            raise ValueError("project record references unknown task")
        if task["project_id"] != project_id:
            raise ValueError("project record task/project mismatch")
        return task

