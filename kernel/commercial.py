from __future__ import annotations

from typing import Any

from .records import (
    Command,
    Decision,
    OpportunityProjectDecisionPacket,
    Project,
    ProjectArtifactReceipt,
    ProjectCloseDecisionPacket,
    ProjectCustomerFeedback,
    ProjectCustomerVisiblePacket,
    ProjectCustomerVisibleReplayProjectionComparison,
    ProjectOperatorLoadRecord,
    ProjectOutcome,
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
    new_id,
)
from .store import KERNEL_POLICY_VERSION, KernelStore


class KernelCommercialResearchWorkflow:
    """Project-pulled commercial workflow over kernel EvidenceBundles.

    This is deliberately deterministic. Worker synthesis may improve the
    EvidenceBundle, but this lane only packages already-committed evidence into
    a replayable project/opportunity decision record.
    """

    def __init__(self, store: KernelStore) -> None:
        self.store = store

    def create_decision_packet(
        self,
        command: Command,
        evidence_bundle_id: str,
        *,
        decision_target: str | None = None,
        project_name: str | None = None,
        revenue_mechanism: str = "software",
    ) -> OpportunityProjectDecisionPacket:
        with self.store.connect() as conn:
            row = conn.execute(
                """
                SELECT
                  r.request_id, r.profile, r.question, r.decision_target,
                  e.bundle_id, e.sources_json, e.claims_json, e.contradictions_json,
                  e.unsupported_claims_json, e.freshness_summary, e.confidence,
                  e.uncertainty, e.counter_thesis, e.quality_gate_result,
                  e.data_classes_json
                FROM evidence_bundles e
                JOIN research_requests r ON r.request_id = e.request_id
                WHERE e.bundle_id = ?
                """,
                (evidence_bundle_id,),
            ).fetchone()
        if row is None:
            raise ValueError("evidence bundle not found")
        if row["profile"] not in {"commercial", "project_support"}:
            raise ValueError("commercial decision packet requires commercial or project_support evidence")

        sources = _loads(row["sources_json"])
        claims = _loads(row["claims_json"])
        contradictions = _loads(row["contradictions_json"])
        unsupported_claims = _loads(row["unsupported_claims_json"])
        data_classes = _loads(row["data_classes_json"])
        target = decision_target or row["decision_target"]
        if not target:
            raise ValueError("project-pulled commercial workflow requires a decision target")

        recommendation = _recommendation(
            confidence=float(row["confidence"]),
            quality_gate_result=row["quality_gate_result"],
            contradictions=contradictions,
            unsupported_claims=unsupported_claims,
        )
        risk_flags = _risk_flags(
            confidence=float(row["confidence"]),
            quality_gate_result=row["quality_gate_result"],
            contradictions=contradictions,
            unsupported_claims=unsupported_claims,
            data_classes=data_classes,
            claims=claims,
        )
        title = project_name or _title_from_question(row["question"])
        evidence_used = [claim["claim_id"] for claim in claims if claim.get("importance") in {"high", "critical"}]
        if not evidence_used:
            evidence_used = [claim["claim_id"] for claim in claims[:3]]
        source_refs = [source["url_or_ref"] for source in sources]
        opportunity = {
            "source": "research_engine",
            "title": title,
            "thesis": _thesis(row["question"], claims),
            "target_customer": _first_matching_claim(claims, ("customer", "buyer", "client")),
            "revenue_mechanism": revenue_mechanism,
            "evidence_bundle_ids": [row["bundle_id"]],
            "validation_plan": _validation_plan(claims, unsupported_claims),
            "expected_operator_load": _operator_load(claims),
            "expected_build_complexity": _build_complexity(claims, risk_flags),
            "cashflow_estimate": _cashflow_estimate(claims),
            "status": "gated",
        }
        project = {
            "project_id": new_id(),
            "decision_target": target,
            "name": title,
            "objective": f"Validate whether to turn this evidence into a bounded commercial project: {title}",
            "revenue_mechanism": revenue_mechanism,
            "operator_role": "reviewer",
            "external_commitment_policy": "operator_only",
            "budget_id": None,
            "status": "proposed",
            "success_metrics": [
                "buyer/problem evidence confirmed",
                "validation artifact produced",
                "operator load remains within estimate",
            ],
            "kill_criteria": _kill_criteria(unsupported_claims, contradictions),
            "phases": [
                {"name": "Validate", "objective": opportunity["validation_plan"], "budget": {"max_cloud_spend_usd": 0}},
                {"name": "Build", "objective": "Produce the smallest useful artifact after G1 approval."},
                {"name": "Ship", "objective": "Put the artifact in front of the intended user or buyer."},
                {"name": "Operate", "objective": "Measure revenue, usage, maintenance, and operator load."},
            ],
        }
        gate_packet = {
            "question": f"Approve G1 validation project for {title}?",
            "recommendation": recommendation,
            "options": ["approve_validation", "pause_for_more_evidence", "reject"],
            "expected_upside": _expected_upside(claims),
            "expected_downside": _expected_downside(risk_flags, row["uncertainty"]),
            "cost_and_time_impact": {
                "validation_cost_usd": _validation_cost_estimate(claims),
                "operator_load": opportunity["expected_operator_load"],
                "build_complexity": opportunity["expected_build_complexity"],
            },
            "evidence_links": source_refs,
            "uncertainty": row["uncertainty"],
            "counter_thesis": row["counter_thesis"],
            "default_on_timeout": "pause",
            "side_effects_authorized": [],
            "rollback_or_compensation": "No external commitments are authorized by this packet; keep project proposed.",
            "authority_policy_version": KERNEL_POLICY_VERSION,
            "freshness_summary": row["freshness_summary"],
        }
        decision_id = new_id()
        decision = Decision(
            decision_id=decision_id,
            decision_type="project_approval",
            question=gate_packet["question"],
            options=[
                {"option_id": "approve_validation", "label": "Approve validation project"},
                {"option_id": "pause_for_more_evidence", "label": "Pause for more evidence"},
                {"option_id": "reject", "label": "Reject"},
            ],
            stakes="medium",
            evidence_bundle_ids=[row["bundle_id"]],
            evidence_refs=[f"kernel:evidence_bundles/{row['bundle_id']}"] + source_refs,
            requested_by="research",
            required_authority="operator_gate",
            authority_policy_version=KERNEL_POLICY_VERSION,
            status="gated",
            recommendation=recommendation,
            confidence=float(row["confidence"]),
            decisive_factors=evidence_used,
            decisive_uncertainty=row["uncertainty"],
            risk_flags=risk_flags,
            default_on_timeout="pause",
            gate_packet=gate_packet,
        )
        packet = OpportunityProjectDecisionPacket(
            packet_id=new_id(),
            request_id=row["request_id"],
            evidence_bundle_id=row["bundle_id"],
            decision_id=decision_id,
            decision_target=target,
            question=gate_packet["question"],
            recommendation=recommendation,
            required_authority="operator_gate",
            opportunity=opportunity,
            project=project,
            gate_packet=gate_packet,
            evidence_used=evidence_used,
            risk_flags=risk_flags,
            default_on_timeout="pause",
            status="gated",
        )
        self.store.execute_command(
            command,
            lambda tx: (tx.create_decision(decision), tx.create_commercial_decision_packet(packet))[1],
        )
        return packet

    def approve_g1_validation_project(
        self,
        command: Command,
        packet_id: str,
        *,
        operator_id: str = "operator",
        notes: str | None = None,
    ) -> dict[str, str]:
        """Resolve a G1 packet and create the first bounded validation task."""
        with self.store.connect() as conn:
            row = conn.execute(
                """
                SELECT packet_id, decision_id, evidence_bundle_id, project_json,
                       evidence_used_json, gate_packet_json
                FROM commercial_decision_packets
                WHERE packet_id=?
                """,
                (packet_id,),
            ).fetchone()
        if row is None:
            raise ValueError("commercial decision packet not found")

        project_spec = _loads(row["project_json"])
        gate_packet = _loads(row["gate_packet_json"])
        evidence_used = _loads(row["evidence_used_json"])
        project = Project(
            project_id=project_spec.get("project_id") or new_id(),
            decision_packet_id=packet_id,
            decision_id=row["decision_id"],
            name=project_spec["name"],
            objective=project_spec["objective"],
            revenue_mechanism=project_spec["revenue_mechanism"],
            operator_role=project_spec["operator_role"],
            external_commitment_policy=project_spec["external_commitment_policy"],
            budget_id=project_spec.get("budget_id"),
            phases=project_spec["phases"],
            success_metrics=project_spec["success_metrics"],
            kill_criteria=project_spec["kill_criteria"],
            evidence_refs=[f"kernel:evidence_bundles/{row['evidence_bundle_id']}"]
            + [f"kernel:claims/{claim_id}" for claim_id in evidence_used],
            status="active",
        )
        task = ProjectTask(
            project_id=project.project_id,
            phase_name="Validate",
            task_type="validate",
            autonomy_class="A2",
            objective=project_spec["phases"][0]["objective"],
            inputs={
                "decision_packet_id": packet_id,
                "decision_id": row["decision_id"],
                "gate_packet": gate_packet,
            },
            expected_output_schema={
                "type": "object",
                "required": ["artifact_ref", "validation_result", "operator_load_actual", "next_recommendation"],
            },
            risk_level="medium",
            required_capabilities=[
                {
                    "capability_type": "file",
                    "actions": ["read", "write"],
                    "scope": "project_workspace",
                    "grant_required_before_run": True,
                }
            ],
            model_requirement={
                "task_class": "coding_small_patch",
                "local_allowed_only_if_promoted": True,
                "frontier_fallback_allowed_with_budget": False,
            },
            budget_id=project.budget_id,
            authority_required="single_agent",
            recovery_policy="ask_operator",
            evidence_refs=project.evidence_refs,
        )

        def handler(tx):
            tx.resolve_decision(
                row["decision_id"],
                verdict="approve_validation",
                decided_by=operator_id,
                notes=notes,
                confidence=1.0,
            )
            tx.create_project(project)
            tx.create_project_task(task)
            return {"decision_id": row["decision_id"], "project_id": project.project_id, "task_id": task.task_id}

        return self.store.execute_command(command, handler)

    def record_project_outcome(self, command: Command, outcome: ProjectOutcome) -> str:
        return self.store.record_project_outcome(command, outcome)

    def record_project_artifact_receipt(self, command: Command, receipt: ProjectArtifactReceipt) -> str:
        return self.store.record_project_artifact_receipt(command, receipt)

    def record_project_customer_feedback(self, command: Command, feedback: ProjectCustomerFeedback) -> str:
        return self.store.record_project_customer_feedback(command, feedback)

    def record_project_revenue_attribution(self, command: Command, attribution: ProjectRevenueAttribution) -> str:
        return self.store.record_project_revenue_attribution(command, attribution)

    def record_project_operator_load(self, command: Command, load: ProjectOperatorLoadRecord) -> str:
        return self.store.record_project_operator_load(command, load)

    def record_project_post_ship_evidence(
        self,
        command: Command,
        artifact_receipt_id: str,
        *,
        feedback: ProjectCustomerFeedback,
        revenue: ProjectRevenueAttribution,
        operator_load: ProjectOperatorLoadRecord,
    ) -> dict[str, str]:
        return self.store.record_project_post_ship_evidence(
            command,
            artifact_receipt_id,
            feedback=feedback,
            revenue=revenue,
            operator_load=operator_load,
        )

    def record_project_followup_delivery(
        self,
        command: Command,
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
        return self.store.record_project_followup_delivery(
            command,
            task_id,
            artifact_ref=artifact_ref,
            summary=summary,
            data_class=data_class,
            delivery_channel=delivery_channel,
            side_effect_intent_id=side_effect_intent_id,
            side_effect_receipt_id=side_effect_receipt_id,
            customer_visible=customer_visible,
            metrics=metrics,
            feedback=feedback,
            revenue_impact=revenue_impact,
            operator_load_actual=operator_load_actual,
            next_recommendation=next_recommendation,
        )

    def record_project_operate_followup_outcome(
        self,
        command: Command,
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
        return self.store.record_project_operate_followup_outcome(
            command,
            task_id,
            summary=summary,
            internal_result_ref=internal_result_ref,
            operator_load_minutes=operator_load_minutes,
            operator_load_source=operator_load_source,
            operate_followup_type=operate_followup_type,
            metrics=metrics,
            result=result,
            revenue_impact=revenue_impact,
            side_effect_intent_id=side_effect_intent_id,
            side_effect_receipt_id=side_effect_receipt_id,
            external_commitment_change=external_commitment_change,
            operator_load_notes=operator_load_notes,
        )

    def derive_project_status_rollup(self, command: Command, project_id: str) -> ProjectStatusRollup:
        return self.store.derive_project_status_rollup(command, project_id)

    def create_project_close_decision(
        self,
        command: Command,
        project_id: str,
        *,
        rollup_id: str | None = None,
    ) -> ProjectCloseDecisionPacket:
        return self.store.create_project_close_decision(command, project_id, rollup_id=rollup_id)

    def resolve_project_close_decision(
        self,
        command: Command,
        packet_id: str,
        *,
        verdict: str,
        operator_id: str = "operator",
        notes: str | None = None,
        confidence: float | None = None,
    ) -> dict[str, Any]:
        return self.store.resolve_project_close_decision(
            command,
            packet_id,
            verdict=verdict,
            decided_by=operator_id,
            notes=notes,
            confidence=confidence,
        )

    def compare_project_replay_to_projection(
        self,
        command: Command,
        project_id: str,
    ) -> ProjectReplayProjectionComparison:
        return self.store.compare_project_replay_to_projection(command, project_id)

    def create_project_portfolio_decision_packet(
        self,
        command: Command,
        project_ids: list[str],
        *,
        scope: str = "active_commercial_projects",
        constraints: dict[str, Any] | None = None,
    ) -> ProjectPortfolioDecisionPacket:
        return self.store.create_project_portfolio_decision_packet(
            command,
            project_ids,
            scope=scope,
            constraints=constraints,
        )

    def resolve_project_portfolio_decision(
        self,
        command: Command,
        packet_id: str,
        *,
        verdict: str,
        operator_id: str = "operator",
        notes: str | None = None,
        confidence: float | None = None,
    ) -> dict[str, Any]:
        return self.store.resolve_project_portfolio_decision(
            command,
            packet_id,
            verdict=verdict,
            decided_by=operator_id,
            notes=notes,
            confidence=confidence,
        )

    def compare_project_portfolio_replay_to_projection(
        self,
        command: Command,
        packet_id: str,
    ) -> ProjectPortfolioReplayProjectionComparison:
        return self.store.compare_project_portfolio_replay_to_projection(command, packet_id)

    def create_project_scheduling_intent(
        self,
        command: Command,
        packet_id: str,
        *,
        scheduling_window: str = "next_internal_cycle",
    ) -> ProjectSchedulingIntent:
        return self.store.create_project_scheduling_intent(
            command,
            packet_id,
            scheduling_window=scheduling_window,
        )

    def compare_project_scheduling_replay_to_projection(
        self,
        command: Command,
        intent_id: str,
    ) -> ProjectSchedulingReplayProjectionComparison:
        return self.store.compare_project_scheduling_replay_to_projection(command, intent_id)

    def create_project_scheduling_priority_change_packet(
        self,
        command: Command,
        intent_id: str,
    ) -> ProjectSchedulingPriorityChangePacket:
        return self.store.create_project_scheduling_priority_change_packet(command, intent_id)

    def resolve_project_scheduling_priority_change_packet(
        self,
        command: Command,
        packet_id: str,
        *,
        verdict: str,
        operator_id: str = "operator",
        notes: str | None = None,
        confidence: float | None = None,
    ) -> dict[str, Any]:
        return self.store.resolve_project_scheduling_priority_change_packet(
            command,
            packet_id,
            verdict=verdict,
            decided_by=operator_id,
            notes=notes,
            confidence=confidence,
        )

    def compare_project_scheduling_priority_replay_to_projection(
        self,
        command: Command,
        packet_id: str,
    ) -> ProjectSchedulingPriorityReplayProjectionComparison:
        return self.store.compare_project_scheduling_priority_replay_to_projection(command, packet_id)

    def create_project_customer_visible_packet(
        self,
        command: Command,
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
        if command.requested_by in {"agent", "model", "tool"}:
            raise PermissionError("autonomous/customer-visible packet preparation is blocked")
        if command.requested_authority != "operator_gate":
            raise PermissionError("customer-visible packets require operator-gate authority")
        return self.store.create_project_customer_visible_packet(
            command,
            outcome_id,
            packet_type=packet_type,
            customer_ref=customer_ref,
            channel=channel,
            subject=subject,
            summary=summary,
            payload_ref=payload_ref,
            side_effect_intent_id=side_effect_intent_id,
        )

    def resolve_project_customer_visible_packet(
        self,
        command: Command,
        packet_id: str,
        *,
        verdict: str,
        side_effect_receipt_id: str | None = None,
        operator_id: str = "operator",
        notes: str | None = None,
        confidence: float | None = None,
    ) -> dict[str, Any]:
        if command.requested_by != "operator" or command.requested_authority != "operator_gate":
            raise PermissionError("customer-visible packet resolution requires operator-gate authority")
        return self.store.resolve_project_customer_visible_packet(
            command,
            packet_id,
            verdict=verdict,
            side_effect_receipt_id=side_effect_receipt_id,
            decided_by=operator_id,
            notes=notes,
            confidence=confidence,
        )

    def compare_project_customer_visible_replay_to_projection(
        self,
        command: Command,
        packet_id: str,
    ) -> ProjectCustomerVisibleReplayProjectionComparison:
        return self.store.compare_project_customer_visible_replay_to_projection(command, packet_id)

    def create_project_scheduling_worker_assignment_packet(
        self,
        command: Command,
        task_id: str,
        *,
        worker_id: str,
        grant_ids: list[str],
        worker_type: str = "agent",
        route_decision_id: str | None = None,
        notes: str | None = None,
    ) -> str:
        if command.requested_by in {"agent", "model"}:
            raise PermissionError("workers cannot self-assign scheduling-created tasks")
        if command.requested_authority != "rule":
            raise PermissionError("scheduling worker assignment packets require rule authority")
        blocked = {
            "autonomous_assignment",
            "autonomous_queue_mutation",
            "customer_commitment_requested",
            "external_side_effect_requested",
        }
        if any(command.payload.get(flag) for flag in blocked):
            raise PermissionError("scheduling assignment packets cannot authorize autonomous assignment or customer side effects")
        with self.store.connect() as conn:
            row = conn.execute(
                """
                SELECT project_id, inputs_json
                FROM project_tasks
                WHERE task_id=?
                """,
                (task_id,),
            ).fetchone()
        if row is None:
            raise ValueError("scheduling worker assignment requires an existing task")
        inputs = _loads(row["inputs_json"])
        if not inputs.get("scheduling_priority_packet_id"):
            raise ValueError("scheduling worker assignment packets are only for scheduling priority-created tasks")
        if inputs.get("customer_commitments_allowed") or inputs.get("customer_visible") or inputs.get("external_side_effects_authorized"):
            raise PermissionError("scheduling worker assignment packets cannot authorize customer commitments or side effects")
        assignment = ProjectTaskAssignment(
            task_id=task_id,
            project_id=row["project_id"],
            worker_type=worker_type,  # type: ignore[arg-type]
            worker_id=worker_id,
            route_decision_id=route_decision_id,
            grant_ids=grant_ids,
            accepted_capabilities=[],
            status="assigned",
            notes=notes or "scheduler prepared governed assignment packet; worker acceptance still required",
        )
        return self.store.assign_project_task(command, assignment)

    def resolve_project_scheduling_worker_assignment(
        self,
        command: Command,
        assignment_id: str,
        *,
        verdict: str,
        worker_id: str,
        accepted_capabilities: list[dict[str, Any]] | None = None,
        notes: str | None = None,
    ) -> str:
        if verdict not in {"accept", "reject"}:
            raise ValueError("scheduling worker assignment verdict must be accept or reject")
        if command.requested_by != "agent":
            raise PermissionError("scheduling worker assignment resolution requires worker acceptance evidence")
        if command.payload.get("customer_commitment_requested") or command.payload.get("external_side_effect_requested"):
            raise PermissionError("scheduling worker assignment resolution cannot authorize customer commitments or side effects")
        with self.store.connect() as conn:
            row = conn.execute(
                """
                SELECT assignment_id, task_id, project_id, worker_type, worker_id,
                       route_decision_id, grant_ids_json
                FROM project_task_assignments
                WHERE assignment_id=?
                """,
                (assignment_id,),
            ).fetchone()
        if row is None:
            raise ValueError("scheduling worker assignment packet not found")
        if row["worker_id"] != worker_id:
            raise PermissionError("worker acceptance evidence does not match assignment packet")
        assignment = ProjectTaskAssignment(
            assignment_id=assignment_id,
            task_id=row["task_id"],
            project_id=row["project_id"],
            worker_type=row["worker_type"],
            worker_id=row["worker_id"],
            route_decision_id=row["route_decision_id"],
            grant_ids=_loads(row["grant_ids_json"]),
            accepted_capabilities=accepted_capabilities or [],
            status="accepted" if verdict == "accept" else "rejected",
            notes=notes or f"worker {verdict}ed scheduling-created task",
        )
        return self.store.assign_project_task(command, assignment)

    def record_project_scheduling_task_outcome(
        self,
        command: Command,
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
        if command.requested_by in {"agent", "model"} and (
            command.payload.get("customer_commitment_requested")
            or command.payload.get("customer_visible")
            or command.payload.get("external_side_effect_requested")
            or external_commitment_change
            or side_effect_intent_id
            or side_effect_receipt_id
        ):
            raise PermissionError("autonomous scheduling outcomes cannot create customer commitments or side effects")
        return self.store.record_project_scheduling_task_outcome(
            command,
            task_id,
            summary=summary,
            internal_result_ref=internal_result_ref,
            result=result,
            metrics=metrics,
            revenue_impact=revenue_impact,
            side_effect_intent_id=side_effect_intent_id,
            side_effect_receipt_id=side_effect_receipt_id,
            external_commitment_change=external_commitment_change,
        )


def commercial_decision_packet_command(
    *,
    evidence_bundle_id: str,
    key: str | None = None,
    payload: dict[str, Any] | None = None,
    requester_id: str = "kernel",
) -> Command:
    return Command(
        command_type="commercial.decision_packet",
        requested_by="kernel",
        requester_id=requester_id,
        target_entity_type="decision",
        target_entity_id=evidence_bundle_id,
        requested_authority="operator_gate",
        idempotency_key=key or f"commercial-decision-packet:{evidence_bundle_id}:{new_id()}",
        payload=payload or {"evidence_bundle_id": evidence_bundle_id},
    )


def g1_project_approval_command(
    *,
    packet_id: str,
    key: str | None = None,
    payload: dict[str, Any] | None = None,
    requester_id: str = "operator",
) -> Command:
    return Command(
        command_type="commercial.g1_project_approval",
        requested_by="operator",
        requester_id=requester_id,
        target_entity_type="decision",
        target_entity_id=packet_id,
        requested_authority="operator_gate",
        idempotency_key=key or f"commercial-g1-approval:{packet_id}:{new_id()}",
        payload=payload or {"packet_id": packet_id, "verdict": "approve_validation"},
    )


def project_task_command(
    *,
    project_id: str,
    key: str | None = None,
    payload: dict[str, Any] | None = None,
    requester_id: str = "kernel",
    requested_by: str = "kernel",
    requested_authority: str | None = None,
) -> Command:
    return Command(
        command_type="commercial.project_task",
        requested_by=requested_by,  # type: ignore[arg-type]
        requester_id=requester_id,
        target_entity_type="task",
        target_entity_id=project_id,
        requested_authority=requested_authority,  # type: ignore[arg-type]
        idempotency_key=key or f"commercial-project-task:{project_id}:{new_id()}",
        payload=payload or {"project_id": project_id},
    )


def project_outcome_command(
    *,
    project_id: str,
    key: str | None = None,
    payload: dict[str, Any] | None = None,
    requester_id: str = "operator",
) -> Command:
    return Command(
        command_type="commercial.project_outcome",
        requested_by="operator",
        requester_id=requester_id,
        target_entity_type="project",
        target_entity_id=project_id,
        idempotency_key=key or f"commercial-project-outcome:{project_id}:{new_id()}",
        payload=payload or {"project_id": project_id},
    )


def project_artifact_receipt_command(
    *,
    project_id: str,
    key: str | None = None,
    payload: dict[str, Any] | None = None,
    requester_id: str = "kernel",
) -> Command:
    return Command(
        command_type="commercial.project_artifact_receipt",
        requested_by="kernel",
        requester_id=requester_id,
        target_entity_type="artifact",
        target_entity_id=project_id,
        idempotency_key=key or f"commercial-project-artifact:{project_id}:{new_id()}",
        payload=payload or {"project_id": project_id},
    )


def project_feedback_command(
    *,
    project_id: str,
    key: str | None = None,
    payload: dict[str, Any] | None = None,
    requester_id: str = "operator",
) -> Command:
    return Command(
        command_type="commercial.project_feedback",
        requested_by="operator",
        requester_id=requester_id,
        target_entity_type="project",
        target_entity_id=project_id,
        idempotency_key=key or f"commercial-project-feedback:{project_id}:{new_id()}",
        payload=payload or {"project_id": project_id},
    )


def project_revenue_attribution_command(
    *,
    project_id: str,
    key: str | None = None,
    payload: dict[str, Any] | None = None,
    requester_id: str = "operator",
) -> Command:
    return Command(
        command_type="commercial.project_revenue_attribution",
        requested_by="operator",
        requester_id=requester_id,
        target_entity_type="project",
        target_entity_id=project_id,
        idempotency_key=key or f"commercial-project-revenue:{project_id}:{new_id()}",
        payload=payload or {"project_id": project_id},
    )


def project_operator_load_command(
    *,
    project_id: str,
    key: str | None = None,
    payload: dict[str, Any] | None = None,
    requester_id: str = "operator",
) -> Command:
    return Command(
        command_type="commercial.project_operator_load",
        requested_by="operator",
        requester_id=requester_id,
        target_entity_type="project",
        target_entity_id=project_id,
        idempotency_key=key or f"commercial-project-load:{project_id}:{new_id()}",
        payload=payload or {"project_id": project_id},
    )


def project_post_ship_evidence_command(
    *,
    project_id: str,
    artifact_receipt_id: str,
    key: str | None = None,
    payload: dict[str, Any] | None = None,
    requester_id: str = "operator",
) -> Command:
    return Command(
        command_type="commercial.project_post_ship_evidence",
        requested_by="operator",
        requester_id=requester_id,
        target_entity_type="artifact",
        target_entity_id=artifact_receipt_id,
        idempotency_key=key or f"commercial-project-post-ship:{project_id}:{artifact_receipt_id}:{new_id()}",
        payload=payload or {"project_id": project_id, "artifact_receipt_id": artifact_receipt_id},
    )


def project_followup_delivery_command(
    *,
    project_id: str,
    task_id: str,
    key: str | None = None,
    payload: dict[str, Any] | None = None,
    requester_id: str = "kernel",
    requested_by: str = "kernel",
    requested_authority: str | None = None,
) -> Command:
    return Command(
        command_type="commercial.project_followup_delivery",
        requested_by=requested_by,  # type: ignore[arg-type]
        requester_id=requester_id,
        target_entity_type="artifact",
        target_entity_id=task_id,
        requested_authority=requested_authority,  # type: ignore[arg-type]
        idempotency_key=key or f"commercial-project-followup-delivery:{project_id}:{task_id}:{new_id()}",
        payload=payload or {"project_id": project_id, "task_id": task_id},
    )


def project_operate_followup_outcome_command(
    *,
    project_id: str,
    task_id: str,
    key: str | None = None,
    payload: dict[str, Any] | None = None,
    requester_id: str = "kernel",
    requested_by: str = "kernel",
    requested_authority: str | None = None,
) -> Command:
    return Command(
        command_type="commercial.project_operate_followup_outcome",
        requested_by=requested_by,  # type: ignore[arg-type]
        requester_id=requester_id,
        target_entity_type="project",
        target_entity_id=task_id,
        requested_authority=requested_authority,  # type: ignore[arg-type]
        idempotency_key=key or f"commercial-project-operate-followup-outcome:{project_id}:{task_id}:{new_id()}",
        payload=payload or {"project_id": project_id, "task_id": task_id},
    )


def project_status_rollup_command(
    *,
    project_id: str,
    key: str | None = None,
    payload: dict[str, Any] | None = None,
    requester_id: str = "kernel",
) -> Command:
    return Command(
        command_type="commercial.project_status_rollup",
        requested_by="kernel",
        requester_id=requester_id,
        target_entity_type="project",
        target_entity_id=project_id,
        idempotency_key=key or f"commercial-project-rollup:{project_id}:{new_id()}",
        payload=payload or {"project_id": project_id},
    )


def project_close_decision_command(
    *,
    project_id: str,
    key: str | None = None,
    payload: dict[str, Any] | None = None,
    requester_id: str = "kernel",
) -> Command:
    return Command(
        command_type="commercial.project_close_decision",
        requested_by="kernel",
        requester_id=requester_id,
        target_entity_type="decision",
        target_entity_id=project_id,
        requested_authority="operator_gate",
        idempotency_key=key or f"commercial-project-close:{project_id}:{new_id()}",
        payload=payload or {"project_id": project_id},
    )


def project_close_resolution_command(
    *,
    packet_id: str,
    verdict: str,
    key: str | None = None,
    payload: dict[str, Any] | None = None,
    requester_id: str = "operator",
) -> Command:
    return Command(
        command_type="commercial.project_close_resolution",
        requested_by="operator",
        requester_id=requester_id,
        target_entity_type="decision",
        target_entity_id=packet_id,
        requested_authority="operator_gate",
        idempotency_key=key or f"commercial-project-close-resolution:{packet_id}:{verdict}:{new_id()}",
        payload=payload or {"packet_id": packet_id, "verdict": verdict},
    )


def project_replay_comparison_command(
    *,
    project_id: str,
    key: str | None = None,
    payload: dict[str, Any] | None = None,
    requester_id: str = "kernel",
) -> Command:
    return Command(
        command_type="commercial.project_replay_comparison",
        requested_by="kernel",
        requester_id=requester_id,
        target_entity_type="project",
        target_entity_id=project_id,
        idempotency_key=key or f"commercial-project-replay-compare:{project_id}:{new_id()}",
        payload=payload or {"project_id": project_id},
    )


def project_portfolio_packet_command(
    *,
    project_ids: list[str],
    key: str | None = None,
    payload: dict[str, Any] | None = None,
    requester_id: str = "kernel",
    requested_by: str = "kernel",
    requested_authority: str = "operator_gate",
) -> Command:
    return Command(
        command_type="commercial.project_portfolio_packet",
        requested_by=requested_by,  # type: ignore[arg-type]
        requester_id=requester_id,
        target_entity_type="decision",
        target_entity_id="portfolio",
        requested_authority=requested_authority,  # type: ignore[arg-type]
        idempotency_key=key or f"commercial-project-portfolio:{new_id()}",
        payload=payload or {"project_ids": project_ids},
    )


def project_portfolio_resolution_command(
    *,
    packet_id: str,
    verdict: str,
    key: str | None = None,
    payload: dict[str, Any] | None = None,
    requester_id: str = "operator",
) -> Command:
    return Command(
        command_type="commercial.project_portfolio_resolution",
        requested_by="operator",
        requester_id=requester_id,
        target_entity_type="decision",
        target_entity_id=packet_id,
        requested_authority="operator_gate",
        idempotency_key=key or f"commercial-project-portfolio-resolution:{packet_id}:{verdict}:{new_id()}",
        payload=payload or {"packet_id": packet_id, "verdict": verdict},
    )


def project_portfolio_replay_comparison_command(
    *,
    packet_id: str,
    key: str | None = None,
    payload: dict[str, Any] | None = None,
    requester_id: str = "kernel",
) -> Command:
    return Command(
        command_type="commercial.project_portfolio_replay_comparison",
        requested_by="kernel",
        requester_id=requester_id,
        target_entity_type="project",
        target_entity_id=packet_id,
        idempotency_key=key or f"commercial-project-portfolio-compare:{packet_id}:{new_id()}",
        payload=payload or {"packet_id": packet_id},
    )


def project_scheduling_intent_command(
    *,
    packet_id: str,
    key: str | None = None,
    payload: dict[str, Any] | None = None,
    requester_id: str = "scheduler",
    requested_by: str = "scheduler",
    requested_authority: str = "rule",
) -> Command:
    return Command(
        command_type="commercial.project_scheduling_intent",
        requested_by=requested_by,  # type: ignore[arg-type]
        requester_id=requester_id,
        target_entity_type="task",
        target_entity_id=packet_id,
        requested_authority=requested_authority,  # type: ignore[arg-type]
        idempotency_key=key or f"commercial-project-scheduling:{packet_id}:{new_id()}",
        payload=payload or {"packet_id": packet_id},
    )


def project_scheduling_replay_comparison_command(
    *,
    intent_id: str,
    key: str | None = None,
    payload: dict[str, Any] | None = None,
    requester_id: str = "kernel",
) -> Command:
    return Command(
        command_type="commercial.project_scheduling_replay_comparison",
        requested_by="kernel",
        requester_id=requester_id,
        target_entity_type="task",
        target_entity_id=intent_id,
        idempotency_key=key or f"commercial-project-scheduling-compare:{intent_id}:{new_id()}",
        payload=payload or {"intent_id": intent_id},
    )


def project_scheduling_priority_packet_command(
    *,
    intent_id: str,
    key: str | None = None,
    payload: dict[str, Any] | None = None,
    requester_id: str = "scheduler",
    requested_by: str = "scheduler",
    requested_authority: str = "operator_gate",
) -> Command:
    return Command(
        command_type="commercial.project_scheduling_priority_packet",
        requested_by=requested_by,  # type: ignore[arg-type]
        requester_id=requester_id,
        target_entity_type="decision",
        target_entity_id=intent_id,
        requested_authority=requested_authority,  # type: ignore[arg-type]
        idempotency_key=key or f"commercial-project-scheduling-priority-packet:{intent_id}:{new_id()}",
        payload=payload or {"intent_id": intent_id},
    )


def project_scheduling_priority_resolution_command(
    *,
    packet_id: str,
    verdict: str,
    key: str | None = None,
    payload: dict[str, Any] | None = None,
    requester_id: str = "operator",
) -> Command:
    return Command(
        command_type="commercial.project_scheduling_priority_resolution",
        requested_by="operator",
        requester_id=requester_id,
        target_entity_type="decision",
        target_entity_id=packet_id,
        requested_authority="operator_gate",
        idempotency_key=key or f"commercial-project-scheduling-priority-resolution:{packet_id}:{verdict}:{new_id()}",
        payload=payload or {"packet_id": packet_id, "verdict": verdict},
    )


def project_scheduling_priority_replay_comparison_command(
    *,
    packet_id: str,
    key: str | None = None,
    payload: dict[str, Any] | None = None,
    requester_id: str = "kernel",
) -> Command:
    return Command(
        command_type="commercial.project_scheduling_priority_replay_comparison",
        requested_by="kernel",
        requester_id=requester_id,
        target_entity_type="task",
        target_entity_id=packet_id,
        idempotency_key=key or f"commercial-project-scheduling-priority-compare:{packet_id}:{new_id()}",
        payload=payload or {"packet_id": packet_id},
    )


def project_customer_visible_packet_command(
    *,
    outcome_id: str,
    key: str | None = None,
    payload: dict[str, Any] | None = None,
    requester_id: str = "kernel",
    requested_by: str = "kernel",
    requested_authority: str = "operator_gate",
) -> Command:
    return Command(
        command_type="commercial.project_customer_visible_packet",
        requested_by=requested_by,  # type: ignore[arg-type]
        requester_id=requester_id,
        target_entity_type="decision",
        target_entity_id=outcome_id,
        requested_authority=requested_authority,  # type: ignore[arg-type]
        idempotency_key=key or f"commercial-project-customer-visible-packet:{outcome_id}:{new_id()}",
        payload=payload or {"outcome_id": outcome_id},
    )


def project_customer_visible_resolution_command(
    *,
    packet_id: str,
    verdict: str,
    key: str | None = None,
    payload: dict[str, Any] | None = None,
    requester_id: str = "operator",
    requested_by: str = "operator",
    requested_authority: str = "operator_gate",
) -> Command:
    return Command(
        command_type="commercial.project_customer_visible_resolution",
        requested_by=requested_by,  # type: ignore[arg-type]
        requester_id=requester_id,
        target_entity_type="decision",
        target_entity_id=packet_id,
        requested_authority=requested_authority,  # type: ignore[arg-type]
        idempotency_key=key or f"commercial-project-customer-visible-resolution:{packet_id}:{verdict}:{new_id()}",
        payload=payload or {"packet_id": packet_id, "verdict": verdict},
    )


def project_customer_visible_replay_comparison_command(
    *,
    packet_id: str,
    key: str | None = None,
    payload: dict[str, Any] | None = None,
    requester_id: str = "kernel",
) -> Command:
    return Command(
        command_type="commercial.project_customer_visible_replay_comparison",
        requested_by="kernel",
        requester_id=requester_id,
        target_entity_type="project",
        target_entity_id=packet_id,
        idempotency_key=key or f"commercial-project-customer-visible-compare:{packet_id}:{new_id()}",
        payload=payload or {"packet_id": packet_id},
    )


def project_scheduling_assignment_packet_command(
    *,
    task_id: str,
    key: str | None = None,
    payload: dict[str, Any] | None = None,
    requester_id: str = "scheduler",
    requested_by: str = "scheduler",
    requested_authority: str = "rule",
) -> Command:
    return Command(
        command_type="commercial.project_scheduling_assignment_packet",
        requested_by=requested_by,  # type: ignore[arg-type]
        requester_id=requester_id,
        target_entity_type="task",
        target_entity_id=task_id,
        requested_authority=requested_authority,  # type: ignore[arg-type]
        idempotency_key=key or f"commercial-project-scheduling-assignment:{task_id}:{new_id()}",
        payload=payload or {"task_id": task_id},
    )


def project_scheduling_assignment_resolution_command(
    *,
    assignment_id: str,
    verdict: str,
    key: str | None = None,
    payload: dict[str, Any] | None = None,
    requester_id: str = "worker",
) -> Command:
    return Command(
        command_type="commercial.project_scheduling_assignment_resolution",
        requested_by="agent",
        requester_id=requester_id,
        target_entity_type="task",
        target_entity_id=assignment_id,
        requested_authority="rule",
        idempotency_key=key or f"commercial-project-scheduling-assignment-resolution:{assignment_id}:{verdict}:{new_id()}",
        payload=payload or {"assignment_id": assignment_id, "verdict": verdict},
    )


def project_scheduling_task_outcome_command(
    *,
    project_id: str,
    task_id: str,
    key: str | None = None,
    payload: dict[str, Any] | None = None,
    requested_by: str = "agent",
    requester_id: str = "worker",
    requested_authority: str = "rule",
) -> Command:
    return Command(
        command_type="commercial.project_scheduling_task_outcome",
        requested_by=requested_by,  # type: ignore[arg-type]
        requester_id=requester_id,
        target_entity_type="project",
        target_entity_id=project_id,
        requested_authority=requested_authority,  # type: ignore[arg-type]
        idempotency_key=key or f"commercial-project-scheduling-task-outcome:{task_id}:{new_id()}",
        payload=payload or {"project_id": project_id, "task_id": task_id},
    )


def _loads(value: str) -> Any:
    import json

    return json.loads(value)


def _recommendation(
    *,
    confidence: float,
    quality_gate_result: str,
    contradictions: list[dict[str, Any]],
    unsupported_claims: list[str],
) -> str:
    if quality_gate_result == "degraded" or len(unsupported_claims) >= 2:
        return "insufficient_evidence"
    if contradictions:
        return "pause"
    if confidence >= 0.70:
        return "pursue"
    if confidence < 0.50:
        return "reject"
    return "pause"


def _risk_flags(
    *,
    confidence: float,
    quality_gate_result: str,
    contradictions: list[dict[str, Any]],
    unsupported_claims: list[str],
    data_classes: list[str],
    claims: list[dict[str, Any]],
) -> list[str]:
    flags: list[str] = []
    if quality_gate_result == "degraded":
        flags.append("quality_gate_degraded")
    if confidence < 0.65:
        flags.append("low_confidence")
    if contradictions:
        flags.append("contradictory_evidence")
    if unsupported_claims:
        flags.append("unsupported_claims")
    if any(data_class != "public" for data_class in data_classes):
        flags.append("non_public_evidence")
    text = "\n".join(claim["text"].lower() for claim in claims)
    if "operator load" not in text and "operator-load" not in text and "operator_load" not in text:
        flags.append("operator_load_unclear")
    if not any(term in text for term in ("willingness-to-pay", "willingness to pay", "pricing", "buyer", "transaction")):
        flags.append("willingness_to_pay_unclear")
    return flags


def _title_from_question(question: str) -> str:
    cleaned = question.strip().rstrip(".?")
    if len(cleaned) <= 80:
        return cleaned
    return cleaned[:77].rstrip() + "..."


def _thesis(question: str, claims: list[dict[str, Any]]) -> str:
    high_claims = [claim["text"] for claim in claims if claim.get("importance") in {"high", "critical"}]
    if high_claims:
        return high_claims[0]
    return question


def _first_matching_claim(claims: list[dict[str, Any]], terms: tuple[str, ...]) -> str | None:
    for claim in claims:
        text = claim["text"]
        if any(term in text.lower() for term in terms):
            return text
    return None


def _validation_plan(claims: list[dict[str, Any]], unsupported_claims: list[str]) -> str:
    validation_claim = _first_matching_claim(claims, ("validation", "experiment", "pilot"))
    if validation_claim:
        return validation_claim
    if unsupported_claims:
        return f"Resolve unsupported commercial claim: {unsupported_claims[0]}"
    return "Run a bounded validation pass with buyer/problem evidence before build budget."


def _operator_load(claims: list[dict[str, Any]]) -> str:
    return _first_matching_claim(claims, ("operator load", "operator-load", "operator_load")) or "operator load estimate not explicit"


def _build_complexity(claims: list[dict[str, Any]], risk_flags: list[str]) -> str:
    text = "\n".join(claim["text"].lower() for claim in claims)
    if "high complexity" in text or "technical blocker" in text:
        return "high"
    if "low complexity" in text and "operator_load_unclear" not in risk_flags:
        return "low"
    return "medium"


def _cashflow_estimate(claims: list[dict[str, Any]]) -> dict[str, Any]:
    pricing_claim = _first_matching_claim(claims, ("pricing", "price", "willingness-to-pay", "willingness to pay"))
    return {
        "low": 0,
        "mid": 0,
        "high": 0,
        "currency": "USD",
        "period": "month",
        "basis": pricing_claim or "no numeric cashflow evidence in bundle",
    }


def _kill_criteria(unsupported_claims: list[str], contradictions: list[dict[str, Any]]) -> list[str]:
    criteria = [
        "No buyer/problem validation after the validation window.",
        "Operator load exceeds expected value.",
        "Required paid spend is not justified by evidence.",
    ]
    if unsupported_claims:
        criteria.append(f"Unsupported claim remains unresolved: {unsupported_claims[0]}")
    if contradictions:
        criteria.append("Contradictory evidence remains unresolved.")
    return criteria


def _expected_upside(claims: list[dict[str, Any]]) -> str:
    return _first_matching_claim(claims, ("pricing", "buyer", "willingness", "market", "revenue")) or "Potential revenue path if validation confirms demand."


def _expected_downside(risk_flags: list[str], uncertainty: str) -> str:
    if risk_flags:
        return f"{uncertainty} Risk flags: {', '.join(risk_flags)}."
    return uncertainty


def _validation_cost_estimate(claims: list[dict[str, Any]]) -> dict[str, Any]:
    cost_claim = _first_matching_claim(claims, ("validation cost", "validation-cost", "validation_cost"))
    return {"amount": 0, "currency": "USD", "basis": cost_claim or "default zero-spend validation"}


__all__ = [
    "KernelCommercialResearchWorkflow",
    "commercial_decision_packet_command",
    "g1_project_approval_command",
    "project_artifact_receipt_command",
    "project_customer_visible_packet_command",
    "project_customer_visible_replay_comparison_command",
    "project_customer_visible_resolution_command",
    "project_feedback_command",
    "project_followup_delivery_command",
    "project_operate_followup_outcome_command",
    "project_operator_load_command",
    "project_outcome_command",
    "project_portfolio_packet_command",
    "project_portfolio_replay_comparison_command",
    "project_portfolio_resolution_command",
    "project_post_ship_evidence_command",
    "project_close_decision_command",
    "project_close_resolution_command",
    "project_replay_comparison_command",
    "project_revenue_attribution_command",
    "project_scheduling_assignment_packet_command",
    "project_scheduling_assignment_resolution_command",
    "project_scheduling_intent_command",
    "project_scheduling_priority_packet_command",
    "project_scheduling_priority_replay_comparison_command",
    "project_scheduling_priority_resolution_command",
    "project_scheduling_replay_comparison_command",
    "project_scheduling_task_outcome_command",
    "project_status_rollup_command",
    "project_task_command",
]
