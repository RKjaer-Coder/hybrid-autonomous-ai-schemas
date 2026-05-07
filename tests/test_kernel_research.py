from __future__ import annotations

import json
import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from kernel import (
    Budget,
    CapabilityGrant,
    ClaimRecord,
    EvidenceBundle,
    KernelCommercialResearchWorkflow,
    KernelResearchEngine,
    KernelStore,
    ProjectResearchInput,
    Project,
    ProjectArtifactReceipt,
    ProjectCustomerCommitmentReceipt,
    ProjectCustomerFeedback,
    ProjectOperatorLoadRecord,
    ProjectOutcome,
    ProjectRevenueAttribution,
    ProjectTask,
    ProjectTaskAssignment,
    ResearchRequest,
    SideEffectIntent,
    SideEffectReceipt,
    SourceAcquisitionCheck,
    SourcePlan,
    SourceRecord,
)
from kernel.records import new_id, payload_hash, sha256_text
from kernel.store import KERNEL_POLICY_VERSION
from kernel.research import (
    evidence_bundle_command,
    research_request_command,
    retrieval_grant_command,
    source_acquisition_command,
    source_plan_command,
)
from kernel.commercial import (
    commercial_decision_packet_command,
    g1_project_approval_command,
    project_artifact_receipt_command,
    project_close_decision_command,
    project_close_resolution_command,
    project_customer_commitment_receipt_command,
    project_customer_visible_packet_command,
    project_customer_visible_replay_comparison_command,
    project_customer_visible_resolution_command,
    project_feedback_command,
    project_followup_delivery_command,
    project_operate_followup_outcome_command,
    project_operator_load_command,
    project_outcome_command,
    project_portfolio_packet_command,
    project_portfolio_replay_comparison_command,
    project_portfolio_resolution_command,
    project_post_ship_evidence_command,
    project_replay_comparison_command,
    project_revenue_attribution_command,
    project_scheduling_assignment_packet_command,
    project_scheduling_assignment_resolution_command,
    project_scheduling_intent_command,
    project_scheduling_priority_packet_command,
    project_scheduling_priority_replay_comparison_command,
    project_scheduling_priority_resolution_command,
    project_scheduling_replay_comparison_command,
    project_scheduling_task_outcome_command,
    project_status_rollup_command,
    project_task_command,
)
from migrate import apply_schema
from skills.db_manager import DatabaseManager


def request_command(key: str, payload: dict | None = None):
    return research_request_command(key=key, payload=payload or {"key": key})


class KernelResearchTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.store = KernelStore(self.root / "kernel.db")
        self.engine = KernelResearchEngine(self.store)
        self.commercial = KernelCommercialResearchWorkflow(self.store)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def request(self) -> ResearchRequest:
        return ResearchRequest(
            request_id=new_id(),
            profile="commercial",
            question="Validate demand for a local-first agent operations package.",
            decision_target="project-alpha",
            freshness_horizon="P30D",
            depth="standard",
            source_policy={
                "allowed_source_types": ["official", "primary_data", "reputable_media", "internal_record"],
                "blocked_source_types": ["model_generated"],
            },
            evidence_requirements={
                "minimum_sources": 2,
                "require_uncertainty": True,
                "high_stakes_claims_require_independent_sources": True,
            },
            max_cost_usd=Decimal("2.50"),
            max_latency="PT30M",
            autonomy_class="A2",
        )

    def bundle(self, request_id: str) -> EvidenceBundle:
        return self.bundle_for_plan(request_id, new_id())

    def plan(self, request_id: str) -> SourcePlan:
        return SourcePlan(
            source_plan_id=new_id(),
            request_id=request_id,
            profile="commercial",
            depth="standard",
            planned_sources=[
                {
                    "url_or_ref": "https://example.com/pricing",
                    "source_type": "official",
                    "access_method": "public_web",
                    "data_class": "public",
                    "purpose": "pricing signal",
                },
                {
                    "url_or_ref": "internal://operator/customer-call-1",
                    "source_type": "internal_record",
                    "access_method": "operator_provided",
                    "data_class": "internal",
                    "purpose": "buyer evidence",
                },
            ],
            retrieval_strategy="prefer official/public web first; use operator-provided notes only with grant",
            created_by="kernel",
        )

    def bundle_for_plan(self, request_id: str, source_plan_id: str) -> EvidenceBundle:
        official = SourceRecord(
            source_id=new_id(),
            url_or_ref="https://example.com/pricing",
            source_type="official",
            retrieved_at="2026-05-02T08:00:00Z",
            source_date="2026-05-01",
            relevance=0.91,
            reliability=0.95,
            content_hash=sha256_text("pricing"),
            access_method="public_web",
            data_class="public",
            license_or_tos_notes="metadata-only cache",
        )
        market = SourceRecord(
            source_id=new_id(),
            url_or_ref="internal://operator/customer-call-1",
            source_type="internal_record",
            retrieved_at="2026-05-02T08:01:00Z",
            source_date="2026-04-29",
            relevance=0.87,
            reliability=0.82,
            content_hash=sha256_text("customer-call"),
            access_method="operator_provided",
            data_class="internal",
        )
        return EvidenceBundle(
            bundle_id=new_id(),
            request_id=request_id,
            source_plan_id=source_plan_id,
            sources=[official, market],
            claims=[
                ClaimRecord(
                    text=(
                        "The package has plausible willingness-to-pay evidence from operator-provided customer notes, "
                        "with low expected operator load for validation."
                    ),
                    claim_type="interpretation",
                    source_ids=[official.source_id, market.source_id],
                    confidence=0.74,
                    freshness="current",
                    importance="high",
                )
            ],
            contradictions=[],
            unsupported_claims=["Exact conversion rate is not yet known."],
            freshness_summary="Both sources were retrieved within the 30 day horizon.",
            confidence=0.74,
            uncertainty="Demand breadth is still uncertain until more buyer conversations exist.",
            counter_thesis="The demand may be narrow consulting pull rather than repeatable product pull.",
            quality_gate_result="pass",
            data_classes=["public", "internal"],
            retention_policy="retain-90d",
        )

    def active_project_with_shipped_artifact(self, key: str) -> dict[str, str]:
        project = Project(
            name=f"Operate Follow-up {key}",
            objective="Exercise post-ship operate follow-up governance.",
            revenue_mechanism="software",
            operator_role="client_owner",
            external_commitment_policy="operator_only",
            phases=[
                {"name": "Validate", "objective": "Validate demand."},
                {"name": "Build", "objective": "Build artifact."},
                {"name": "Ship", "objective": "Ship artifact."},
                {"name": "Operate", "objective": "Operate customer-visible artifact."},
            ],
            success_metrics=["accepted customer feedback"],
            kill_criteria=["negative feedback without revenue"],
            status="active",
        )
        self.store.create_project(project_task_command(project_id=project.project_id, key=f"{key}-project"), project)
        task = ProjectTask(
            project_id=project.project_id,
            phase_name="Ship",
            task_type="ship",
            autonomy_class="A2",
            objective="Deliver a customer-visible artifact under operator gate.",
            inputs={"project_id": project.project_id},
            expected_output_schema={"type": "object", "required": ["side_effect_receipt_id"]},
            risk_level="medium",
            required_capabilities=[
                {
                    "capability_type": "side_effect",
                    "actions": ["prepare"],
                    "scope": "project_delivery",
                    "grant_required_before_run": True,
                }
            ],
            model_requirement={"task_class": "coding_small_patch", "local_allowed_only_if_promoted": True},
            authority_required="operator_gate",
            recovery_policy="ask_operator",
        )
        self.store.create_project_task(project_task_command(project_id=project.project_id, key=f"{key}-ship-task"), task)
        grant = CapabilityGrant(
            task_id=task.task_id,
            subject_type="adapter",
            subject_id="side_effect_broker",
            capability_type="side_effect",
            actions=["prepare"],
            resource={"kind": "publish", "artifact_ref": f"artifact://local/{key}/shipped"},
            scope={"project_id": project.project_id},
            conditions={"operator_approved": True},
            expires_at="2999-01-01T00:00:00Z",
            policy_version=KERNEL_POLICY_VERSION,
            max_uses=1,
        )
        grant_id = self.store.issue_capability_grant(
            project_task_command(project_id=project.project_id, key=f"{key}-ship-grant"),
            grant,
        )
        self.store.assign_project_task(
            project_task_command(project_id=project.project_id, key=f"{key}-ship-assignment"),
            ProjectTaskAssignment(
                task_id=task.task_id,
                project_id=project.project_id,
                worker_type="agent",
                worker_id="ship-worker",
                grant_ids=[grant_id],
                accepted_capabilities=[
                    {"capability_type": "side_effect", "actions": ["prepare"], "scope": "project_delivery"}
                ],
            ),
        )
        intent = SideEffectIntent(
            task_id=task.task_id,
            side_effect_type="publish",
            target={"channel": "customer_review"},
            payload_hash=payload_hash({"artifact_ref": f"artifact://local/{key}/shipped"}),
            required_authority="operator_gate",
            grant_id=grant_id,
            timeout_policy="ask_operator",
        )
        intent_id = self.store.prepare_side_effect(
            project_task_command(
                project_id=project.project_id,
                key=f"{key}-side-effect-intent",
                requested_by="operator",
                requested_authority="operator_gate",
            ),
            intent,
        )
        receipt_id = self.store.record_side_effect_receipt(
            project_task_command(project_id=project.project_id, key=f"{key}-side-effect-receipt"),
            SideEffectReceipt(
                intent_id=intent_id,
                receipt_type="success",
                receipt_hash=payload_hash({"published": True, "key": key}),
                details={"channel": "customer_review"},
            ),
        )
        artifact_id = self.commercial.record_project_artifact_receipt(
            project_artifact_receipt_command(project_id=project.project_id, key=f"{key}-shipped-artifact"),
            ProjectArtifactReceipt(
                project_id=project.project_id,
                task_id=task.task_id,
                artifact_ref=f"artifact://local/{key}/shipped",
                artifact_kind="shipped_artifact",
                summary="Accepted customer-visible shipped artifact.",
                data_class="internal",
                delivery_channel="customer_review",
                side_effect_intent_id=intent_id,
                side_effect_receipt_id=receipt_id,
                customer_visible=True,
                status="accepted",
            ),
        )
        return {
            "project_id": project.project_id,
            "task_id": task.task_id,
            "artifact_receipt_id": artifact_id,
            "side_effect_receipt_id": receipt_id,
        }

    def record_post_ship_evidence(
        self,
        key: str,
        shipped: dict[str, str],
        *,
        summary: str,
        sentiment: str = "positive",
        action_required: bool = True,
        revenue_amount: Decimal = Decimal("100"),
        revenue_status: str = "reconciled",
        revenue_confidence: float = 0.9,
        load_minutes: int = 5,
    ) -> dict[str, str]:
        return self.commercial.record_project_post_ship_evidence(
            project_post_ship_evidence_command(
                project_id=shipped["project_id"],
                artifact_receipt_id=shipped["artifact_receipt_id"],
                key=f"{key}-post-ship-evidence",
            ),
            shipped["artifact_receipt_id"],
            feedback=ProjectCustomerFeedback(
                project_id=shipped["project_id"],
                task_id=shipped["task_id"],
                source_type="customer",
                customer_ref=f"customer-{key}",
                summary=summary,
                sentiment=sentiment,  # type: ignore[arg-type]
                action_required=action_required,
                operator_review_required=False,
                status="accepted",
            ),
            revenue=ProjectRevenueAttribution(
                project_id=shipped["project_id"],
                task_id=shipped["task_id"],
                amount_usd=revenue_amount,
                source="operator_reported",
                attribution_period="2026-05",
                confidence=revenue_confidence,
                external_ref=f"operator://revenue/{key}" if revenue_status == "reconciled" else None,
                status=revenue_status,  # type: ignore[arg-type]
            ),
            operator_load=ProjectOperatorLoadRecord(
                project_id=shipped["project_id"],
                task_id=shipped["task_id"],
                minutes=load_minutes,
                load_type="client_sales",
                source="operator_reported",
                notes="Post-ship customer evidence review",
            ),
        )

    def running_operate_followup_task(self, key: str, *, summary: str) -> dict[str, str]:
        shipped = self.active_project_with_shipped_artifact(key)
        self.record_post_ship_evidence(key, shipped, summary=summary)
        rollup = self.commercial.derive_project_status_rollup(
            project_status_rollup_command(project_id=shipped["project_id"], key=f"{key}-rollup"),
            shipped["project_id"],
        )
        close_packet = self.commercial.create_project_close_decision(
            project_close_decision_command(project_id=shipped["project_id"], key=f"{key}-close"),
            shipped["project_id"],
            rollup_id=rollup.rollup_id,
        )
        resolution = self.commercial.resolve_project_close_decision(
            project_close_resolution_command(
                packet_id=close_packet.packet_id,
                verdict="continue",
                key=f"{key}-resolution",
            ),
            close_packet.packet_id,
            verdict="continue",
            operator_id="operator",
            notes="Continue with governed Operate follow-up.",
        )
        task_id = resolution["followup_task_id"]
        grant = CapabilityGrant(
            task_id=task_id,
            subject_type="agent",
            subject_id="operate-worker",
            capability_type="memory_write",
            actions=["record"],
            resource={"kind": "project_operate_followup"},
            scope={"project_id": shipped["project_id"]},
            conditions={"external_side_effects": "blocked_without_operator_gate_and_receipt"},
            expires_at="2999-01-01T00:00:00Z",
            policy_version=KERNEL_POLICY_VERSION,
            max_uses=1,
        )
        grant_id = self.store.issue_capability_grant(
            project_task_command(project_id=shipped["project_id"], key=f"{key}-operate-grant"),
            grant,
        )
        assignment_id = self.store.assign_project_task(
            project_task_command(project_id=shipped["project_id"], key=f"{key}-operate-assignment"),
            ProjectTaskAssignment(
                task_id=task_id,
                project_id=shipped["project_id"],
                worker_type="agent",
                worker_id="operate-worker",
                grant_ids=[grant_id],
                accepted_capabilities=[
                    {"capability_type": "memory_write", "actions": ["record"], "scope": "project_operate_followup"}
                ],
                notes="bounded operate worker accepted the follow-up",
            ),
        )
        return {
            **shipped,
            "followup_task_id": task_id,
            "operate_grant_id": grant_id,
            "operate_assignment_id": assignment_id,
        }

    def staged_operate_side_effect(self, key: str, project_id: str, task_id: str) -> dict[str, str]:
        grant = CapabilityGrant(
            task_id=task_id,
            subject_type="adapter",
            subject_id="side_effect_broker",
            capability_type="side_effect",
            actions=["prepare"],
            resource={"kind": "message", "target": f"customer-{key}"},
            scope={"project_id": project_id},
            conditions={"operator_approved": True},
            expires_at="2999-01-01T00:00:00Z",
            policy_version=KERNEL_POLICY_VERSION,
            max_uses=1,
        )
        grant_id = self.store.issue_capability_grant(
            project_task_command(project_id=project_id, key=f"{key}-operate-side-effect-grant"),
            grant,
        )
        intent_id = self.store.prepare_side_effect(
            project_task_command(
                project_id=project_id,
                key=f"{key}-operate-side-effect-intent",
                requested_by="operator",
                requested_authority="operator_gate",
            ),
            SideEffectIntent(
                task_id=task_id,
                side_effect_type="message",
                target={"customer_ref": f"customer-{key}", "channel": "support_desk"},
                payload_hash=payload_hash({"message_ref": f"artifact://local/{key}/support-response"}),
                required_authority="operator_gate",
                grant_id=grant_id,
                timeout_policy="ask_operator",
            ),
        )
        receipt_id = self.store.record_side_effect_receipt(
            project_task_command(project_id=project_id, key=f"{key}-operate-side-effect-receipt"),
            SideEffectReceipt(
                intent_id=intent_id,
                receipt_type="success",
                receipt_hash=payload_hash({"sent": True, "key": key}),
                details={"message_ref": f"artifact://local/{key}/support-response"},
            ),
        )
        return {"grant_id": grant_id, "intent_id": intent_id, "receipt_id": receipt_id}

    def budgeted_running_operate_task(
        self,
        key: str,
        *,
        budget_cap: Decimal,
        reserved_budget: Decimal = Decimal("0"),
        followup_type: str = "revenue_reconciliation",
    ) -> dict[str, str]:
        project_id = new_id()
        budget = Budget(
            owner_type="project",
            owner_id=project_id,
            approved_by="operator",
            cap_usd=budget_cap,
            expires_at="2999-01-01T00:00:00Z",
        )
        budget_id = self.store.create_budget(
            project_task_command(project_id=project_id, key=f"{key}-budget", requested_by="operator"),
            budget,
        )
        if reserved_budget:
            self.store.reserve_budget(
                project_task_command(project_id=project_id, key=f"{key}-budget-reserve", requested_by="operator"),
                budget_id,
                reserved_budget,
            )
        project = Project(
            project_id=project_id,
            name=f"Portfolio Project {key}",
            objective="Exercise portfolio tradeoff scoring.",
            revenue_mechanism="software",
            operator_role="client_owner",
            external_commitment_policy="operator_only",
            budget_id=budget_id,
            phases=[{"name": "Operate", "objective": "Operate customer-facing commercial loop."}],
            success_metrics=["reconciled revenue", "retained customers"],
            kill_criteria=["operator load exceeds value"],
            status="active",
        )
        self.store.create_project(project_task_command(project_id=project_id, key=f"{key}-project"), project)
        task = ProjectTask(
            project_id=project_id,
            phase_name="Operate",
            task_type="operate",
            autonomy_class="A1",
            objective="Record governed portfolio evidence.",
            inputs={
                "operate_followup_type": followup_type,
                "external_commitment_policy": "draft_or_internal_only_without_side_effect_receipt",
                "default_operator_load_type": "reconciliation" if followup_type == "revenue_reconciliation" else "client_sales",
            },
            expected_output_schema={"type": "object", "required": ["internal_result_ref"]},
            risk_level="low",
            required_capabilities=[
                {
                    "capability_type": "memory_write",
                    "actions": ["record"],
                    "scope": "project_operate_followup",
                    "grant_required_before_run": True,
                }
            ],
            model_requirement={"task_class": "quick_research_summarization", "local_allowed_only_if_promoted": True},
            budget_id=budget_id,
            authority_required="rule",
            recovery_policy="ask_operator",
        )
        self.store.create_project_task(project_task_command(project_id=project_id, key=f"{key}-task"), task)
        grant_id = self.store.issue_capability_grant(
            project_task_command(project_id=project_id, key=f"{key}-grant"),
            CapabilityGrant(
                task_id=task.task_id,
                subject_type="agent",
                subject_id="portfolio-worker",
                capability_type="memory_write",
                actions=["record"],
                resource={"kind": "portfolio_evidence"},
                scope={"project_id": project_id},
                conditions={"external_side_effects": "blocked_without_operator_gate_and_receipt"},
                expires_at="2999-01-01T00:00:00Z",
                policy_version=KERNEL_POLICY_VERSION,
                max_uses=1,
            ),
        )
        self.store.assign_project_task(
            project_task_command(project_id=project_id, key=f"{key}-assignment"),
            ProjectTaskAssignment(
                task_id=task.task_id,
                project_id=project_id,
                worker_type="agent",
                worker_id="portfolio-worker",
                grant_ids=[grant_id],
                accepted_capabilities=[
                    {"capability_type": "memory_write", "actions": ["record"], "scope": "project_operate_followup"}
                ],
            ),
        )
        return {"project_id": project_id, "task_id": task.task_id, "budget_id": budget_id}

    def accepted_priority_created_task(self, key: str, *, budget_cap: Decimal = Decimal("650")) -> dict[str, str]:
        running = self.budgeted_running_operate_task(key, budget_cap=budget_cap)
        self.commercial.record_project_operate_followup_outcome(
            project_operate_followup_outcome_command(
                project_id=running["project_id"],
                task_id=running["task_id"],
                key=f"{key}-priority-outcome",
            ),
            running["task_id"],
            summary="Reconciled revenue evidence should drive a scheduling-created internal queue item.",
            internal_result_ref=f"artifact://local/{key}/priority-revenue",
            operator_load_minutes=4,
            operator_load_source="operator_reported",
            result={"reconciliation_status": "reconciled"},
            revenue_impact={"amount_usd": "600", "currency": "USD", "period": "2026-05"},
        )
        packet = self.commercial.create_project_portfolio_decision_packet(
            project_portfolio_packet_command(project_ids=[running["project_id"]], key=f"{key}-priority-portfolio"),
            [running["project_id"]],
            constraints={"high_revenue_usd": "500"},
        )
        self.commercial.resolve_project_portfolio_decision(
            project_portfolio_resolution_command(
                packet_id=packet.packet_id,
                verdict="accept_prioritization",
                key=f"{key}-priority-portfolio-accepted",
            ),
            packet.packet_id,
            verdict="accept_prioritization",
        )
        intent = self.commercial.create_project_scheduling_intent(
            project_scheduling_intent_command(packet_id=packet.packet_id, key=f"{key}-priority-intent"),
            packet.packet_id,
        )
        priority_packet = self.commercial.create_project_scheduling_priority_change_packet(
            project_scheduling_priority_packet_command(intent_id=intent.intent_id, key=f"{key}-priority-packet"),
            intent.intent_id,
        )
        resolution = self.commercial.resolve_project_scheduling_priority_change_packet(
            project_scheduling_priority_resolution_command(
                packet_id=priority_packet.packet_id,
                verdict="accept_priority_changes",
                key=f"{key}-priority-resolution",
            ),
            priority_packet.packet_id,
            verdict="accept_priority_changes",
        )
        created = next(change for change in resolution["applied_changes"] if change["status"] == "queued")
        return {
            "project_id": running["project_id"],
            "source_task_id": running["task_id"],
            "task_id": created["task_id"],
            "budget_id": running["budget_id"],
            "priority_packet_id": priority_packet.packet_id,
        }

    def accepted_assigned_priority_created_task(self, key: str) -> dict[str, str]:
        created = self.accepted_priority_created_task(key)
        worker_id = f"{key}-worker"
        grant_id = self.store.issue_capability_grant(
            project_task_command(project_id=created["project_id"], key=f"{key}-worker-grant"),
            CapabilityGrant(
                task_id=created["task_id"],
                subject_type="agent",
                subject_id=worker_id,
                capability_type="memory_write",
                actions=["record"],
                resource={"kind": "project_internal_scheduling"},
                scope={"project_id": created["project_id"]},
                conditions={"external_side_effects": "blocked_without_operator_gate_and_receipt"},
                expires_at="2999-01-01T00:00:00Z",
                policy_version=KERNEL_POLICY_VERSION,
                max_uses=1,
            ),
        )
        assignment_id = self.commercial.create_project_scheduling_worker_assignment_packet(
            project_scheduling_assignment_packet_command(task_id=created["task_id"], key=f"{key}-assignment-packet"),
            created["task_id"],
            worker_id=worker_id,
            grant_ids=[grant_id],
        )
        self.commercial.resolve_project_scheduling_worker_assignment(
            project_scheduling_assignment_resolution_command(
                assignment_id=assignment_id,
                verdict="accept",
                key=f"{key}-assignment-accept",
                requester_id=worker_id,
            ),
            assignment_id,
            verdict="accept",
            worker_id=worker_id,
            accepted_capabilities=[
                {"capability_type": "memory_write", "actions": ["record"], "scope": "project_internal_scheduling"}
            ],
        )
        return {**created, "grant_id": grant_id, "assignment_id": assignment_id, "worker_id": worker_id}

    def completed_internal_scheduling_outcome(self, key: str) -> dict[str, str]:
        created = self.accepted_assigned_priority_created_task(key)
        outcome = self.commercial.record_project_scheduling_task_outcome(
            project_scheduling_task_outcome_command(
                project_id=created["project_id"],
                task_id=created["task_id"],
                key=f"{key}-internal-outcome",
                requester_id=created["worker_id"],
            ),
            created["task_id"],
            summary="Completed internal customer-support response draft with preserved scheduling evidence.",
            internal_result_ref=f"artifact://local/{key}/customer-response-draft",
            result={"scheduling_outcome_type": "customer_support", "support_status": "drafted"},
        )
        return {**created, "outcome_id": outcome["outcome_id"]}

    def staged_customer_visible_intent(self, key: str, project_id: str, task_id: str) -> dict[str, str]:
        grant_id = self.store.issue_capability_grant(
            project_task_command(project_id=project_id, key=f"{key}-customer-visible-grant"),
            CapabilityGrant(
                task_id=task_id,
                subject_type="adapter",
                subject_id="side_effect_broker",
                capability_type="side_effect",
                actions=["prepare"],
                resource={"kind": "message", "target": f"customer-{key}"},
                scope={"project_id": project_id},
                conditions={"operator_approved": True},
                expires_at="2999-01-01T00:00:00Z",
                policy_version=KERNEL_POLICY_VERSION,
                max_uses=1,
            ),
        )
        intent_id = self.store.prepare_side_effect(
            project_task_command(
                project_id=project_id,
                key=f"{key}-customer-visible-intent",
                requested_by="operator",
                requested_authority="operator_gate",
            ),
            SideEffectIntent(
                task_id=task_id,
                side_effect_type="message",
                target={"customer_ref": f"customer-{key}", "channel": "email"},
                payload_hash=payload_hash({"payload_ref": f"artifact://local/{key}/customer-response-draft"}),
                required_authority="operator_gate",
                grant_id=grant_id,
                timeout_policy="ask_operator",
            ),
        )
        return {"grant_id": grant_id, "intent_id": intent_id}

    def accepted_customer_visible_commitment(self, key: str) -> dict[str, str]:
        completed = self.completed_internal_scheduling_outcome(key)
        intent = self.staged_customer_visible_intent(key, completed["project_id"], completed["task_id"])
        packet = self.commercial.create_project_customer_visible_packet(
            project_customer_visible_packet_command(
                outcome_id=completed["outcome_id"],
                key=f"{key}-customer-visible-create",
            ),
            completed["outcome_id"],
            packet_type="customer_message",
            customer_ref=f"customer-{key}",
            channel="email",
            subject="Support response draft",
            summary="Operator packet for a customer-visible support response.",
            payload_ref=f"artifact://local/{key}/customer-response-draft",
            side_effect_intent_id=intent["intent_id"],
        )
        receipt_id = self.store.record_side_effect_receipt(
            project_task_command(project_id=completed["project_id"], key=f"{key}-customer-visible-receipt"),
            SideEffectReceipt(
                intent_id=intent["intent_id"],
                receipt_type="success",
                receipt_hash=payload_hash({"sent": True, "packet": packet.packet_id}),
                details={"message_ref": f"artifact://local/{key}/customer-response-draft"},
            ),
        )
        resolution = self.commercial.resolve_project_customer_visible_packet(
            project_customer_visible_resolution_command(
                packet_id=packet.packet_id,
                verdict="accept_customer_visible_packet",
                key=f"{key}-customer-visible-resolution",
            ),
            packet.packet_id,
            verdict="accept_customer_visible_packet",
            side_effect_receipt_id=receipt_id,
        )
        return {
            **completed,
            "packet_id": packet.packet_id,
            "intent_id": intent["intent_id"],
            "side_effect_receipt_id": receipt_id,
            "commitment_id": resolution["customer_commitment_id"],
        }

    def running_commitment_receipt_followup_task(
        self,
        key: str,
        *,
        receipt_type: str,
        summary: str,
        source_type: str = "platform",
    ) -> dict[str, str]:
        accepted = self.accepted_customer_visible_commitment(key)
        result = self.commercial.record_project_customer_commitment_receipt(
            project_customer_commitment_receipt_command(
                commitment_id=accepted["commitment_id"],
                key=f"{key}-receipt-record",
            ),
            ProjectCustomerCommitmentReceipt(
                commitment_id=accepted["commitment_id"],
                project_id=accepted["project_id"],
                receipt_type=receipt_type,  # type: ignore[arg-type]
                source_type=source_type,  # type: ignore[arg-type]
                summary=summary,
                evidence_refs=[f"platform://commitment-receipts/{key}"],
                action_required=True,
                status="needs_followup",
            ),
        )
        task_id = result["followup_task_id"]
        grant = CapabilityGrant(
            task_id=task_id,
            subject_type="agent",
            subject_id=f"{key}-receipt-worker",
            capability_type="memory_write",
            actions=["record"],
            resource={"kind": "project_commitment_receipt_followup"},
            scope={"project_id": accepted["project_id"]},
            conditions={"external_side_effects": "blocked_without_operator_gate_and_receipt"},
            expires_at="2999-01-01T00:00:00Z",
            policy_version=KERNEL_POLICY_VERSION,
            max_uses=1,
        )
        grant_id = self.store.issue_capability_grant(
            project_task_command(project_id=accepted["project_id"], key=f"{key}-receipt-grant"),
            grant,
        )
        assignment_id = self.store.assign_project_task(
            project_task_command(project_id=accepted["project_id"], key=f"{key}-receipt-assignment"),
            ProjectTaskAssignment(
                task_id=task_id,
                project_id=accepted["project_id"],
                worker_type="agent",
                worker_id=f"{key}-receipt-worker",
                grant_ids=[grant_id],
                accepted_capabilities=[
                    {
                        "capability_type": "memory_write",
                        "actions": ["record"],
                        "scope": "project_commitment_receipt_followup",
                    }
                ],
                notes="bounded worker accepted the customer commitment receipt follow-up",
            ),
        )
        return {
            **accepted,
            "receipt_id": result["receipt_id"],
            "followup_task_id": task_id,
            "receipt_grant_id": grant_id,
            "receipt_assignment_id": assignment_id,
        }

    def test_accepted_post_ship_feedback_creates_governed_operate_followup_types(self):
        cases = [
            ("revenue", "Please reconcile the invoice payment before we renew.", "revenue_reconciliation"),
            ("retention", "The team wants renewal and adoption follow-up for the next month.", "retention"),
            ("maintenance", "A slow error path needs a maintenance fix after launch.", "maintenance"),
            ("support", "Please help the customer understand the new report workflow.", "customer_support"),
        ]
        for key, summary, expected_type in cases:
            with self.subTest(expected_type=expected_type):
                shipped = self.active_project_with_shipped_artifact(f"operate-{key}")
                self.record_post_ship_evidence(f"operate-{key}", shipped, summary=summary)
                rollup = self.commercial.derive_project_status_rollup(
                    project_status_rollup_command(project_id=shipped["project_id"], key=f"operate-{key}-rollup"),
                    shipped["project_id"],
                )
                close_packet = self.commercial.create_project_close_decision(
                    project_close_decision_command(project_id=shipped["project_id"], key=f"operate-{key}-close"),
                    shipped["project_id"],
                    rollup_id=rollup.rollup_id,
                )
                resolution = self.commercial.resolve_project_close_decision(
                    project_close_resolution_command(
                        packet_id=close_packet.packet_id,
                        verdict="continue",
                        key=f"operate-{key}-resolution",
                    ),
                    close_packet.packet_id,
                    verdict="continue",
                    operator_id="operator",
                    notes="Continue only with internal Operate follow-up.",
                )
                comparison = self.commercial.compare_project_replay_to_projection(
                    project_replay_comparison_command(project_id=shipped["project_id"], key=f"operate-{key}-compare"),
                    shipped["project_id"],
                )

                with self.store.connect() as conn:
                    task = conn.execute(
                        """
                        SELECT phase_name, task_type, authority_required, risk_level,
                               inputs_json, expected_output_schema_json, required_capabilities_json
                        FROM project_tasks
                        WHERE task_id=?
                        """,
                        (resolution["followup_task_id"],),
                    ).fetchone()
                    project = conn.execute(
                        "SELECT status FROM projects WHERE project_id=?",
                        (shipped["project_id"],),
                    ).fetchone()
                inputs = json.loads(task["inputs_json"])
                expected_output_schema = json.loads(task["expected_output_schema_json"])
                capabilities = json.loads(task["required_capabilities_json"])

                self.assertEqual(rollup.close_recommendation, "continue")
                self.assertEqual(task["phase_name"], "Operate")
                self.assertEqual(task["task_type"], "operate")
                self.assertEqual(task["authority_required"], "rule")
                self.assertEqual(task["risk_level"], "low")
                self.assertEqual(inputs["operate_followup_type"], expected_type)
                self.assertEqual(
                    inputs["external_commitment_policy"],
                    "draft_or_internal_only_without_side_effect_receipt",
                )
                self.assertEqual(expected_output_schema["properties"]["external_commitment_change"]["const"], False)
                self.assertEqual(capabilities[0]["external_side_effects"], "blocked_without_operator_gate_and_receipt")
                self.assertEqual(project["status"], "active")
                self.assertTrue(comparison.matches)

                replay = self.store.replay_critical_state()
                self.assertEqual(
                    replay.project_tasks[resolution["followup_task_id"]]["inputs"]["operate_followup_type"],
                    expected_type,
                )
                self.assertTrue(replay.project_replay_projection_comparisons[comparison.comparison_id]["matches"])

    def test_operate_followup_outcome_records_internal_result_and_operator_load(self):
        running = self.running_operate_followup_task(
            "operate-outcome-internal",
            summary="Please reconcile the invoice payment before renewal.",
        )

        result = self.commercial.record_project_operate_followup_outcome(
            project_operate_followup_outcome_command(
                project_id=running["project_id"],
                task_id=running["followup_task_id"],
                key="operate-outcome-internal-record",
            ),
            running["followup_task_id"],
            summary="Reconciled the invoice internally and prepared the renewal note for operator review.",
            internal_result_ref="artifact://local/operate-outcome-internal/reconciliation-note",
            operator_load_minutes=12,
            operator_load_source="operator_reported",
            metrics={"invoice_status": "matched", "open_items": 0},
            result={"next_internal_action": "operator_review"},
            revenue_impact={"amount": "100", "currency": "USD", "period": "2026-05", "status": "reconciled"},
        )
        comparison = self.commercial.compare_project_replay_to_projection(
            project_replay_comparison_command(project_id=running["project_id"], key="operate-outcome-internal-compare"),
            running["project_id"],
        )

        with self.store.connect() as conn:
            outcome = conn.execute(
                """
                SELECT outcome_type, phase_name, feedback_json, operator_load_actual,
                       side_effect_intent_id, side_effect_receipt_id, status
                FROM project_outcomes
                WHERE outcome_id=?
                """,
                (result["outcome_id"],),
            ).fetchone()
            load = conn.execute(
                """
                SELECT task_id, outcome_id, minutes, load_type, source
                FROM project_operator_load
                WHERE load_id=?
                """,
                (result["operator_load_id"],),
            ).fetchone()
            task = conn.execute(
                "SELECT status FROM project_tasks WHERE task_id=?",
                (running["followup_task_id"],),
            ).fetchone()
        feedback = json.loads(outcome["feedback_json"])

        self.assertEqual(result["operate_followup_type"], "revenue_reconciliation")
        self.assertEqual(outcome["outcome_type"], "operate_followup")
        self.assertEqual(outcome["phase_name"], "Operate")
        self.assertEqual(outcome["operator_load_actual"], "12 minutes")
        self.assertIsNone(outcome["side_effect_intent_id"])
        self.assertIsNone(outcome["side_effect_receipt_id"])
        self.assertEqual(outcome["status"], "accepted")
        self.assertEqual(feedback["internal_result_ref"], "artifact://local/operate-outcome-internal/reconciliation-note")
        self.assertFalse(feedback["external_commitment_change"])
        self.assertEqual(load["task_id"], running["followup_task_id"])
        self.assertEqual(load["outcome_id"], result["outcome_id"])
        self.assertEqual(load["minutes"], 12)
        self.assertEqual(load["load_type"], "reconciliation")
        self.assertEqual(load["source"], "operator_reported")
        self.assertEqual(task["status"], "completed")
        self.assertTrue(comparison.matches)

        replay = self.store.replay_critical_state()
        self.assertEqual(replay.project_outcomes[result["outcome_id"]]["outcome_type"], "operate_followup")
        self.assertEqual(replay.project_outcomes[result["outcome_id"]]["feedback"]["external_commitment_change"], False)
        self.assertEqual(replay.project_operator_load[result["operator_load_id"]]["minutes"], 12)

    def test_operate_followup_side_effects_fail_closed_without_authority_or_receipt(self):
        missing_receipt = self.running_operate_followup_task(
            "operate-outcome-deny-receipt",
            summary="The customer wants renewal follow-up before the next billing cycle.",
        )
        with self.assertRaises(PermissionError):
            self.commercial.record_project_operate_followup_outcome(
                project_operate_followup_outcome_command(
                    project_id=missing_receipt["project_id"],
                    task_id=missing_receipt["followup_task_id"],
                    key="operate-outcome-deny-missing-receipt",
                ),
                missing_receipt["followup_task_id"],
                summary="Prepared renewal follow-up for the customer.",
                internal_result_ref="artifact://local/operate-outcome-deny-receipt/renewal-note",
                operator_load_minutes=8,
                operator_load_source="operator_reported",
                external_commitment_change=True,
            )

        missing_authority = self.running_operate_followup_task(
            "operate-outcome-deny-authority",
            summary="Please help the customer understand the new report workflow.",
        )
        side_effect = self.staged_operate_side_effect(
            "operate-outcome-deny-authority",
            missing_authority["project_id"],
            missing_authority["followup_task_id"],
        )
        with self.assertRaises(PermissionError):
            self.commercial.record_project_operate_followup_outcome(
                project_operate_followup_outcome_command(
                    project_id=missing_authority["project_id"],
                    task_id=missing_authority["followup_task_id"],
                    key="operate-outcome-deny-missing-authority",
                ),
                missing_authority["followup_task_id"],
                summary="Prepared a customer support response.",
                internal_result_ref="artifact://local/operate-outcome-deny-authority/support-response",
                operator_load_minutes=9,
                operator_load_source="operator_reported",
                side_effect_intent_id=side_effect["intent_id"],
                side_effect_receipt_id=side_effect["receipt_id"],
                external_commitment_change=True,
            )

        with self.store.connect() as conn:
            denied_outcomes = conn.execute(
                """
                SELECT COUNT(*) FROM project_outcomes
                WHERE task_id IN (?, ?)
                """,
                (missing_receipt["followup_task_id"], missing_authority["followup_task_id"]),
            ).fetchone()[0]
            denied_load = conn.execute(
                """
                SELECT COUNT(*) FROM project_operator_load
                WHERE task_id IN (?, ?)
                """,
                (missing_receipt["followup_task_id"], missing_authority["followup_task_id"]),
            ).fetchone()[0]
        self.assertEqual(denied_outcomes, 0)
        self.assertEqual(denied_load, 0)

    def test_authorized_operate_followup_side_effect_is_linked_and_replays_cleanly(self):
        running = self.running_operate_followup_task(
            "operate-outcome-authorized",
            summary="Please help the customer understand the new report workflow.",
        )
        side_effect = self.staged_operate_side_effect(
            "operate-outcome-authorized",
            running["project_id"],
            running["followup_task_id"],
        )

        result = self.commercial.record_project_operate_followup_outcome(
            project_operate_followup_outcome_command(
                project_id=running["project_id"],
                task_id=running["followup_task_id"],
                key="operate-outcome-authorized-record",
                requested_by="operator",
                requested_authority="operator_gate",
            ),
            running["followup_task_id"],
            summary="Sent the operator-approved support response and recorded the internal support result.",
            internal_result_ref="artifact://local/operate-outcome-authorized/support-response",
            operator_load_minutes=7,
            operator_load_source="operator_reported",
            result={"support_status": "answered"},
            side_effect_intent_id=side_effect["intent_id"],
            side_effect_receipt_id=side_effect["receipt_id"],
            external_commitment_change=True,
        )
        comparison = self.commercial.compare_project_replay_to_projection(
            project_replay_comparison_command(project_id=running["project_id"], key="operate-outcome-authorized-compare"),
            running["project_id"],
        )

        with self.store.connect() as conn:
            outcome = conn.execute(
                """
                SELECT outcome_type, feedback_json, side_effect_intent_id, side_effect_receipt_id
                FROM project_outcomes
                WHERE outcome_id=?
                """,
                (result["outcome_id"],),
            ).fetchone()
            load = conn.execute(
                "SELECT outcome_id, minutes, load_type FROM project_operator_load WHERE load_id=?",
                (result["operator_load_id"],),
            ).fetchone()
        feedback = json.loads(outcome["feedback_json"])

        self.assertEqual(outcome["outcome_type"], "operate_followup")
        self.assertEqual(outcome["side_effect_intent_id"], side_effect["intent_id"])
        self.assertEqual(outcome["side_effect_receipt_id"], side_effect["receipt_id"])
        self.assertTrue(feedback["external_commitment_change"])
        self.assertEqual(feedback["side_effect_intent_id"], side_effect["intent_id"])
        self.assertEqual(feedback["side_effect_receipt_id"], side_effect["receipt_id"])
        self.assertEqual(load["outcome_id"], result["outcome_id"])
        self.assertEqual(load["minutes"], 7)
        self.assertEqual(load["load_type"], "other")
        self.assertTrue(comparison.matches)

        replay = self.store.replay_critical_state()
        self.assertEqual(replay.side_effects[side_effect["intent_id"]]["receipt"]["receipt_id"], side_effect["receipt_id"])
        self.assertEqual(
            replay.project_outcomes[result["outcome_id"]]["side_effect_receipt_id"],
            side_effect["receipt_id"],
        )
        self.assertTrue(replay.project_replay_projection_comparisons[comparison.comparison_id]["matches"])

    def test_reconciled_operate_outcome_updates_commercial_rollup_and_projection_compare(self):
        running = self.running_operate_followup_task(
            "operate-commercial-revenue",
            summary="Please reconcile the paid invoice before renewal.",
        )
        result = self.commercial.record_project_operate_followup_outcome(
            project_operate_followup_outcome_command(
                project_id=running["project_id"],
                task_id=running["followup_task_id"],
                key="operate-commercial-revenue-record",
            ),
            running["followup_task_id"],
            summary="Reconciled the invoice against operator-provided payment evidence.",
            internal_result_ref="artifact://local/operate-commercial-revenue/reconciliation-note",
            operator_load_minutes=11,
            operator_load_source="operator_reported",
            result={
                "reconciliation_status": "reconciled",
                "evidence_refs": ["operator://invoice/operate-commercial-revenue"],
            },
            revenue_impact={"amount_usd": "320", "currency": "USD", "period": "2026-05"},
        )
        rollup = self.commercial.derive_project_status_rollup(
            project_status_rollup_command(project_id=running["project_id"], key="operate-commercial-revenue-final-rollup"),
            running["project_id"],
        )
        comparison = self.commercial.compare_project_replay_to_projection(
            project_replay_comparison_command(project_id=running["project_id"], key="operate-commercial-revenue-compare"),
            running["project_id"],
        )

        with self.store.connect() as conn:
            commercial_rollup = conn.execute(
                """
                SELECT revenue_reconciled_usd, revenue_unreconciled_usd,
                       external_commitment_count, receiptless_side_effect_count,
                       evidence_refs_json
                FROM project_commercial_rollups
                WHERE rollup_id=?
                """,
                (rollup.commercial_rollup_id,),
            ).fetchone()
            comparison_row = conn.execute(
                """
                SELECT matches, replay_commercial_rollup_json, projection_commercial_rollup_json
                FROM project_replay_projection_comparisons
                WHERE comparison_id=?
                """,
                (comparison.comparison_id,),
            ).fetchone()

        self.assertEqual(result["operate_followup_type"], "revenue_reconciliation")
        self.assertEqual(commercial_rollup["revenue_reconciled_usd"], "320")
        self.assertEqual(commercial_rollup["revenue_unreconciled_usd"], "0")
        self.assertEqual(commercial_rollup["external_commitment_count"], 0)
        self.assertEqual(commercial_rollup["receiptless_side_effect_count"], 0)
        self.assertIn(result["outcome_id"], commercial_rollup["evidence_refs_json"])
        self.assertEqual(rollup.commercial_rollup["revenue_reconciled_usd"], "320")
        self.assertTrue(comparison.matches)
        self.assertEqual(comparison_row["matches"], 1)
        self.assertEqual(comparison_row["replay_commercial_rollup_json"], comparison_row["projection_commercial_rollup_json"])

    def test_retention_and_support_rollups_feed_close_recommendations(self):
        cases = [
            (
                "retained",
                "The customer wants renewal and retention follow-up.",
                {"retention_status": "retained"},
                "complete",
                "retained_customer_count",
                1,
            ),
            (
                "retention-risk",
                "The customer is at churn risk before renewal.",
                {"retention_status": "at_risk"},
                "pause",
                "at_risk_customer_count",
                1,
            ),
            (
                "support-open",
                "Please help the customer understand the new report workflow.",
                {"support_status": "open"},
                "continue",
                "support_open_count",
                1,
            ),
            (
                "support-resolved",
                "Please help the customer understand the new report workflow.",
                {"support_status": "resolved"},
                "complete",
                "support_resolved_count",
                1,
            ),
        ]
        for key, summary, result_payload, expected_recommendation, field, expected_value in cases:
            with self.subTest(key=key):
                running = self.running_operate_followup_task(f"operate-commercial-{key}", summary=summary)
                self.commercial.record_project_operate_followup_outcome(
                    project_operate_followup_outcome_command(
                        project_id=running["project_id"],
                        task_id=running["followup_task_id"],
                        key=f"operate-commercial-{key}-record",
                    ),
                    running["followup_task_id"],
                    summary="Recorded governed customer-retention or support follow-up evidence.",
                    internal_result_ref=f"artifact://local/operate-commercial-{key}/result",
                    operator_load_minutes=6,
                    operator_load_source="operator_reported",
                    result=result_payload,
                )
                rollup = self.commercial.derive_project_status_rollup(
                    project_status_rollup_command(project_id=running["project_id"], key=f"operate-commercial-{key}-final-rollup"),
                    running["project_id"],
                )
                close_packet = self.commercial.create_project_close_decision(
                    project_close_decision_command(project_id=running["project_id"], key=f"operate-commercial-{key}-final-close"),
                    running["project_id"],
                    rollup_id=rollup.rollup_id,
                )
                comparison = self.commercial.compare_project_replay_to_projection(
                    project_replay_comparison_command(project_id=running["project_id"], key=f"operate-commercial-{key}-compare"),
                    running["project_id"],
                )

                self.assertEqual(rollup.close_recommendation, expected_recommendation)
                self.assertEqual(close_packet.recommendation, expected_recommendation)
                self.assertEqual(rollup.commercial_rollup[field], expected_value)
                self.assertTrue(comparison.matches)

    def test_unreconciled_or_receiptless_operate_outcomes_do_not_count_as_customer_commitments(self):
        running = self.running_operate_followup_task(
            "operate-commercial-unreconciled",
            summary="Please reconcile the invoice payment before renewal.",
        )
        grant = CapabilityGrant(
            task_id=running["followup_task_id"],
            subject_type="adapter",
            subject_id="side_effect_broker",
            capability_type="side_effect",
            actions=["prepare"],
            resource={"kind": "message", "target": "customer-unreconciled"},
            scope={"project_id": running["project_id"]},
            conditions={"operator_approved": True},
            expires_at="2999-01-01T00:00:00Z",
            policy_version=KERNEL_POLICY_VERSION,
            max_uses=1,
        )
        side_effect_grant_id = self.store.issue_capability_grant(
            project_task_command(project_id=running["project_id"], key="operate-commercial-unreconciled-side-effect-grant"),
            grant,
        )
        intent = SideEffectIntent(
            task_id=running["followup_task_id"],
            side_effect_type="message",
            target={"customer_ref": "customer-unreconciled", "channel": "support_desk"},
            payload_hash=payload_hash({"message_ref": "artifact://local/operate-commercial-unreconciled/draft"}),
            required_authority="operator_gate",
            grant_id=side_effect_grant_id,
            timeout_policy="ask_operator",
        )
        self.store.prepare_side_effect(
            project_task_command(
                project_id=running["project_id"],
                key="operate-commercial-unreconciled-receiptless-side-effect-intent",
                requested_by="operator",
                requested_authority="operator_gate",
            ),
            intent,
        )
        self.commercial.record_project_operate_followup_outcome(
            project_operate_followup_outcome_command(
                project_id=running["project_id"],
                task_id=running["followup_task_id"],
                key="operate-commercial-unreconciled-record",
                requested_by="operator",
                requested_authority="operator_gate",
            ),
            running["followup_task_id"],
            summary="Prepared an internal reconciliation draft without a customer commitment receipt.",
            internal_result_ref="artifact://local/operate-commercial-unreconciled/reconciliation-draft",
            operator_load_minutes=9,
            operator_load_source="operator_reported",
            result={"reconciliation_status": "needs_reconciliation"},
            revenue_impact={"amount_usd": "500", "currency": "USD", "period": "2026-05"},
            side_effect_intent_id=intent.intent_id,
        )
        rollup = self.commercial.derive_project_status_rollup(
            project_status_rollup_command(project_id=running["project_id"], key="operate-commercial-unreconciled-final-rollup"),
            running["project_id"],
        )
        comparison = self.commercial.compare_project_replay_to_projection(
            project_replay_comparison_command(project_id=running["project_id"], key="operate-commercial-unreconciled-compare"),
            running["project_id"],
        )

        self.assertEqual(rollup.commercial_rollup["revenue_reconciled_usd"], "0")
        self.assertEqual(rollup.commercial_rollup["revenue_unreconciled_usd"], "500")
        self.assertEqual(rollup.commercial_rollup["external_commitment_count"], 0)
        self.assertEqual(rollup.commercial_rollup["receiptless_side_effect_count"], 1)
        self.assertIn("unreconciled_operate_revenue", rollup.risk_flags)
        self.assertIn("receiptless_operate_side_effect_intent", rollup.risk_flags)
        self.assertTrue(comparison.matches)

    def test_commercial_rollups_feed_operator_facing_portfolio_packet(self):
        strong = self.budgeted_running_operate_task("portfolio-strong", budget_cap=Decimal("1000"))
        weak = self.budgeted_running_operate_task(
            "portfolio-weak",
            budget_cap=Decimal("80"),
            reserved_budget=Decimal("70"),
            followup_type="retention",
        )
        self.commercial.record_project_operate_followup_outcome(
            project_operate_followup_outcome_command(
                project_id=strong["project_id"],
                task_id=strong["task_id"],
                key="portfolio-strong-outcome",
            ),
            strong["task_id"],
            summary="Reconciled high-value invoice evidence.",
            internal_result_ref="artifact://local/portfolio-strong/revenue",
            operator_load_minutes=8,
            operator_load_source="operator_reported",
            result={"reconciliation_status": "reconciled"},
            revenue_impact={"amount_usd": "900", "currency": "USD", "period": "2026-05"},
        )
        self.commercial.record_project_operate_followup_outcome(
            project_operate_followup_outcome_command(
                project_id=weak["project_id"],
                task_id=weak["task_id"],
                key="portfolio-weak-outcome",
            ),
            weak["task_id"],
            summary="Retention signal shows customer at risk and high operator load.",
            internal_result_ref="artifact://local/portfolio-weak/retention",
            operator_load_minutes=180,
            operator_load_source="operator_reported",
            result={"retention_status": "at_risk"},
        )

        packet = self.commercial.create_project_portfolio_decision_packet(
            project_portfolio_packet_command(
                project_ids=[strong["project_id"], weak["project_id"]],
                key="portfolio-packet",
            ),
            [strong["project_id"], weak["project_id"]],
            constraints={"max_operator_load_minutes": 120, "min_budget_remaining_usd": "25"},
        )

        ranked = packet.packet["ranked_projects"]
        self.assertEqual(packet.required_authority, "operator_gate")
        self.assertEqual(packet.status, "gated")
        self.assertEqual(packet.packet["authority"]["side_effects_authorized"], [])
        self.assertFalse(packet.packet["authority"]["agents_may_reprioritize"])
        self.assertFalse(packet.packet["authority"]["agents_may_commit_customer_work"])
        self.assertEqual(ranked[0]["project_id"], strong["project_id"])
        self.assertEqual(ranked[0]["recommended_action"], "harvest_or_complete")
        self.assertEqual(ranked[1]["recommended_action"], "pause_until_operator_review")
        self.assertEqual(ranked[0]["revenue"]["reconciled_usd"], "900")
        self.assertEqual(ranked[1]["retention"]["at_risk"], 1)
        self.assertEqual(ranked[1]["budget"]["remaining_usd"], "10")
        self.assertIn("operator_load_over_constraint", packet.risk_flags)
        self.assertIn("budget_under_required_remaining", packet.risk_flags)
        self.assertTrue(any(ref.startswith("kernel:project_commercial_rollups/") for ref in packet.evidence_refs))

    def test_portfolio_packet_resolution_is_operator_gated_planning_only(self):
        running = self.budgeted_running_operate_task("portfolio-resolution", budget_cap=Decimal("500"))
        self.commercial.record_project_operate_followup_outcome(
            project_operate_followup_outcome_command(
                project_id=running["project_id"],
                task_id=running["task_id"],
                key="portfolio-resolution-outcome",
            ),
            running["task_id"],
            summary="Reconciled invoice evidence for planning packet.",
            internal_result_ref="artifact://local/portfolio-resolution/revenue",
            operator_load_minutes=5,
            operator_load_source="operator_reported",
            result={"reconciliation_status": "reconciled"},
            revenue_impact={"amount_usd": "300", "currency": "USD", "period": "2026-05"},
        )
        packet = self.commercial.create_project_portfolio_decision_packet(
            project_portfolio_packet_command(project_ids=[running["project_id"]], key="portfolio-resolution-packet"),
            [running["project_id"]],
        )
        blocked_command = project_portfolio_packet_command(
            project_ids=[running["project_id"]],
            key="portfolio-agent-blocked",
            requested_by="agent",
            requested_authority="operator_gate",
        )
        with self.assertRaises(PermissionError):
            self.commercial.create_project_portfolio_decision_packet(blocked_command, [running["project_id"]])
        commitment_command = project_portfolio_packet_command(
            project_ids=[running["project_id"]],
            key="portfolio-commitment-blocked",
            payload={"project_ids": [running["project_id"]], "customer_commitment_requested": True},
        )
        with self.assertRaises(PermissionError):
            self.commercial.create_project_portfolio_decision_packet(commitment_command, [running["project_id"]])

        resolution = self.commercial.resolve_project_portfolio_decision(
            project_portfolio_resolution_command(
                packet_id=packet.packet_id,
                verdict="accept_prioritization",
                key="portfolio-resolution-operator",
            ),
            packet.packet_id,
            verdict="accept_prioritization",
            operator_id="operator",
            notes="Accept planning guidance without external commitments.",
        )

        self.assertEqual(resolution["authority_effect"], "planning_guidance_only")
        self.assertEqual(resolution["project_status_changes"], [])
        self.assertEqual(resolution["customer_commitments"], [])
        with self.store.connect() as conn:
            project_status = conn.execute(
                "SELECT status FROM projects WHERE project_id=?",
                (running["project_id"],),
            ).fetchone()["status"]
        self.assertEqual(project_status, "active")

    def test_portfolio_packet_replay_projection_comparison_remains_clean(self):
        running = self.budgeted_running_operate_task("portfolio-replay", budget_cap=Decimal("500"))
        self.commercial.record_project_operate_followup_outcome(
            project_operate_followup_outcome_command(
                project_id=running["project_id"],
                task_id=running["task_id"],
                key="portfolio-replay-outcome",
            ),
            running["task_id"],
            summary="Reconciled invoice evidence for replay comparison.",
            internal_result_ref="artifact://local/portfolio-replay/revenue",
            operator_load_minutes=4,
            operator_load_source="operator_reported",
            result={"reconciliation_status": "reconciled"},
            revenue_impact={"amount_usd": "450", "currency": "USD", "period": "2026-05"},
        )
        packet = self.commercial.create_project_portfolio_decision_packet(
            project_portfolio_packet_command(project_ids=[running["project_id"]], key="portfolio-replay-packet"),
            [running["project_id"]],
        )
        comparison = self.commercial.compare_project_portfolio_replay_to_projection(
            project_portfolio_replay_comparison_command(packet_id=packet.packet_id, key="portfolio-replay-compare"),
            packet.packet_id,
        )

        self.assertTrue(comparison.matches)
        self.assertEqual(comparison.mismatches, [])
        replay = self.store.replay_critical_state()
        self.assertEqual(replay.project_portfolio_decision_packets[packet.packet_id]["packet"], packet.packet)
        self.assertTrue(
            replay.project_portfolio_replay_projection_comparisons[comparison.comparison_id]["matches"]
        )

    def test_accepted_portfolio_packet_produces_bounded_internal_scheduling_intent(self):
        strong = self.budgeted_running_operate_task("scheduling-strong", budget_cap=Decimal("900"))
        weak = self.budgeted_running_operate_task(
            "scheduling-weak",
            budget_cap=Decimal("75"),
            reserved_budget=Decimal("72"),
            followup_type="retention",
        )
        self.commercial.record_project_operate_followup_outcome(
            project_operate_followup_outcome_command(
                project_id=strong["project_id"],
                task_id=strong["task_id"],
                key="scheduling-strong-outcome",
            ),
            strong["task_id"],
            summary="Reconciled revenue evidence for next internal cycle.",
            internal_result_ref="artifact://local/scheduling-strong/revenue",
            operator_load_minutes=6,
            operator_load_source="operator_reported",
            result={"reconciliation_status": "reconciled"},
            revenue_impact={"amount_usd": "700", "currency": "USD", "period": "2026-05"},
        )
        self.commercial.record_project_operate_followup_outcome(
            project_operate_followup_outcome_command(
                project_id=weak["project_id"],
                task_id=weak["task_id"],
                key="scheduling-weak-outcome",
            ),
            weak["task_id"],
            summary="Retention risk and high operator load should hold new queue work.",
            internal_result_ref="artifact://local/scheduling-weak/retention",
            operator_load_minutes=150,
            operator_load_source="operator_reported",
            result={"retention_status": "at_risk"},
        )
        packet = self.commercial.create_project_portfolio_decision_packet(
            project_portfolio_packet_command(
                project_ids=[strong["project_id"], weak["project_id"]],
                key="scheduling-packet",
            ),
            [strong["project_id"], weak["project_id"]],
            constraints={"max_operator_load_minutes": 120, "min_budget_remaining_usd": "10", "high_revenue_usd": "500"},
        )
        self.commercial.resolve_project_portfolio_decision(
            project_portfolio_resolution_command(
                packet_id=packet.packet_id,
                verdict="accept_prioritization",
                key="scheduling-packet-accepted",
            ),
            packet.packet_id,
            verdict="accept_prioritization",
        )

        intent = self.commercial.create_project_scheduling_intent(
            project_scheduling_intent_command(packet_id=packet.packet_id, key="scheduling-intent"),
            packet.packet_id,
            scheduling_window="next_24h_internal_queue",
        )

        self.assertEqual(intent.required_authority, "rule")
        self.assertEqual(intent.authority_effect, "internal_scheduling_recommendations_only")
        self.assertEqual(intent.intent["bounds"]["max_queue_delta_tasks_per_project"], 1)
        self.assertFalse(intent.intent["bounds"]["customer_visible_work"])
        self.assertFalse(intent.intent["bounds"]["mutates_task_priority"])
        self.assertFalse(intent.intent["bounds"]["cancels_tasks"])
        self.assertEqual(len(intent.queue_adjustments), 2)
        by_project = {item["project_id"]: item for item in intent.queue_adjustments}
        self.assertEqual(by_project[strong["project_id"]]["queue_action"], "recommend_next_internal_task")
        self.assertIn("revenue_high", by_project[strong["project_id"]]["tradeoff_drivers"])
        self.assertEqual(by_project[weak["project_id"]]["queue_action"], "recommend_hold_new_internal_work")
        self.assertIn("budget_low", by_project[weak["project_id"]]["tradeoff_drivers"])
        self.assertIn("operator_load_high", by_project[weak["project_id"]]["tradeoff_drivers"])
        self.assertIn("retention_at_risk", by_project[weak["project_id"]]["tradeoff_drivers"])
        for adjustment in intent.queue_adjustments:
            self.assertFalse(adjustment["priority_change"]["applied"])
            self.assertTrue(adjustment["priority_change"]["requires_operator_gate"])
            self.assertFalse(adjustment["cancellation"]["applied"])
            self.assertTrue(adjustment["cancellation"]["requires_operator_gate"])
            self.assertFalse(adjustment["customer_commitment"]["applied"])
            self.assertFalse(adjustment["customer_commitment"]["allowed"])
            self.assertEqual(adjustment["external_side_effects_authorized"], [])
        self.assertIn("scheduling_budget_low", intent.risk_flags)
        self.assertTrue(any(ref.startswith("kernel:project_portfolio_decision_packets/") for ref in intent.evidence_refs))

    def test_scheduling_intent_fails_closed_before_acceptance_and_for_autonomous_commitments(self):
        running = self.budgeted_running_operate_task("scheduling-blocked", budget_cap=Decimal("250"))
        packet = self.commercial.create_project_portfolio_decision_packet(
            project_portfolio_packet_command(project_ids=[running["project_id"]], key="scheduling-blocked-packet"),
            [running["project_id"]],
        )
        with self.assertRaises(PermissionError):
            self.commercial.create_project_scheduling_intent(
                project_scheduling_intent_command(packet_id=packet.packet_id, key="scheduling-unaccepted"),
                packet.packet_id,
            )
        self.commercial.resolve_project_portfolio_decision(
            project_portfolio_resolution_command(
                packet_id=packet.packet_id,
                verdict="accept_prioritization",
                key="scheduling-blocked-accepted",
            ),
            packet.packet_id,
            verdict="accept_prioritization",
        )
        blocked_payloads = [
            {"packet_id": packet.packet_id, "autonomous_reprioritization": True},
            {"packet_id": packet.packet_id, "autonomous_cancellation": True},
            {"packet_id": packet.packet_id, "customer_commitment_requested": True},
            {"packet_id": packet.packet_id, "priority_change_requested": True},
        ]
        for index, payload in enumerate(blocked_payloads):
            with self.subTest(payload=payload):
                with self.assertRaises(PermissionError):
                    self.commercial.create_project_scheduling_intent(
                        project_scheduling_intent_command(
                            packet_id=packet.packet_id,
                            key=f"scheduling-blocked-{index}",
                            payload=payload,
                        ),
                        packet.packet_id,
                    )
        with self.assertRaises(PermissionError):
            self.commercial.create_project_scheduling_intent(
                project_scheduling_intent_command(
                    packet_id=packet.packet_id,
                    key="scheduling-agent-blocked",
                    requested_by="agent",
                ),
                packet.packet_id,
            )
        with self.store.connect() as conn:
            statuses = [
                row["status"]
                for row in conn.execute(
                    "SELECT status FROM project_tasks WHERE project_id=? ORDER BY created_at",
                    (running["project_id"],),
                ).fetchall()
            ]
        self.assertIn("running", statuses)
        self.assertNotIn("cancelled", statuses)

    def test_scheduling_intent_replay_projection_comparison_remains_clean(self):
        running = self.budgeted_running_operate_task("scheduling-replay", budget_cap=Decimal("500"))
        self.commercial.record_project_operate_followup_outcome(
            project_operate_followup_outcome_command(
                project_id=running["project_id"],
                task_id=running["task_id"],
                key="scheduling-replay-outcome",
            ),
            running["task_id"],
            summary="Reconciled invoice evidence for scheduling replay.",
            internal_result_ref="artifact://local/scheduling-replay/revenue",
            operator_load_minutes=4,
            operator_load_source="operator_reported",
            result={"reconciliation_status": "reconciled"},
            revenue_impact={"amount_usd": "450", "currency": "USD", "period": "2026-05"},
        )
        packet = self.commercial.create_project_portfolio_decision_packet(
            project_portfolio_packet_command(project_ids=[running["project_id"]], key="scheduling-replay-packet"),
            [running["project_id"]],
        )
        self.commercial.resolve_project_portfolio_decision(
            project_portfolio_resolution_command(
                packet_id=packet.packet_id,
                verdict="accept_prioritization",
                key="scheduling-replay-accepted",
            ),
            packet.packet_id,
            verdict="accept_prioritization",
        )
        intent = self.commercial.create_project_scheduling_intent(
            project_scheduling_intent_command(packet_id=packet.packet_id, key="scheduling-replay-intent"),
            packet.packet_id,
        )
        comparison = self.commercial.compare_project_scheduling_replay_to_projection(
            project_scheduling_replay_comparison_command(
                intent_id=intent.intent_id,
                key="scheduling-replay-compare",
            ),
            intent.intent_id,
        )

        self.assertTrue(comparison.matches)
        self.assertEqual(comparison.mismatches, [])
        replay = self.store.replay_critical_state()
        self.assertEqual(replay.project_scheduling_intents[intent.intent_id]["intent"], intent.intent)
        self.assertTrue(
            replay.project_scheduling_replay_projection_comparisons[comparison.comparison_id]["matches"]
        )

    def test_accepted_scheduling_intent_produces_operator_gated_priority_change_packet(self):
        running = self.budgeted_running_operate_task("priority-packet", budget_cap=Decimal("650"))
        self.commercial.record_project_operate_followup_outcome(
            project_operate_followup_outcome_command(
                project_id=running["project_id"],
                task_id=running["task_id"],
                key="priority-packet-outcome",
            ),
            running["task_id"],
            summary="Reconciled revenue evidence for priority packet creation.",
            internal_result_ref="artifact://local/priority-packet/revenue",
            operator_load_minutes=5,
            operator_load_source="operator_reported",
            result={"reconciliation_status": "reconciled"},
            revenue_impact={"amount_usd": "550", "currency": "USD", "period": "2026-05"},
        )
        packet = self.commercial.create_project_portfolio_decision_packet(
            project_portfolio_packet_command(project_ids=[running["project_id"]], key="priority-source-portfolio"),
            [running["project_id"]],
            constraints={"high_revenue_usd": "500"},
        )
        self.commercial.resolve_project_portfolio_decision(
            project_portfolio_resolution_command(
                packet_id=packet.packet_id,
                verdict="accept_prioritization",
                key="priority-source-accepted",
            ),
            packet.packet_id,
            verdict="accept_prioritization",
        )
        intent = self.commercial.create_project_scheduling_intent(
            project_scheduling_intent_command(packet_id=packet.packet_id, key="priority-source-intent"),
            packet.packet_id,
        )
        with self.store.connect() as conn:
            queued_before = conn.execute(
                "SELECT COUNT(*) FROM project_tasks WHERE project_id=? AND status='queued'",
                (running["project_id"],),
            ).fetchone()[0]

        priority_packet = self.commercial.create_project_scheduling_priority_change_packet(
            project_scheduling_priority_packet_command(intent_id=intent.intent_id, key="priority-packet-created"),
            intent.intent_id,
        )

        self.assertEqual(priority_packet.required_authority, "operator_gate")
        self.assertEqual(priority_packet.status, "gated")
        self.assertEqual(priority_packet.default_on_timeout, "defer")
        self.assertEqual(priority_packet.applied_changes, [])
        self.assertTrue(any(ref.startswith("kernel:project_scheduling_intents/") for ref in priority_packet.evidence_refs))
        self.assertFalse(priority_packet.proposed_changes[0]["mutates_queue_on_packet_creation"])
        self.assertTrue(priority_packet.proposed_changes[0]["applies_only_on_accept"])
        self.assertFalse(priority_packet.proposed_changes[0]["customer_commitment"]["allowed"])
        with self.store.connect() as conn:
            queued_after = conn.execute(
                "SELECT COUNT(*) FROM project_tasks WHERE project_id=? AND status='queued'",
                (running["project_id"],),
            ).fetchone()[0]
        self.assertEqual(queued_after, queued_before)

    def test_operator_approval_applies_bounded_internal_queue_change(self):
        running = self.budgeted_running_operate_task("priority-approve", budget_cap=Decimal("650"))
        self.commercial.record_project_operate_followup_outcome(
            project_operate_followup_outcome_command(
                project_id=running["project_id"],
                task_id=running["task_id"],
                key="priority-approve-outcome",
            ),
            running["task_id"],
            summary="Reconciled revenue evidence should drive the next internal queue item.",
            internal_result_ref="artifact://local/priority-approve/revenue",
            operator_load_minutes=4,
            operator_load_source="operator_reported",
            result={"reconciliation_status": "reconciled"},
            revenue_impact={"amount_usd": "600", "currency": "USD", "period": "2026-05"},
        )
        packet = self.commercial.create_project_portfolio_decision_packet(
            project_portfolio_packet_command(project_ids=[running["project_id"]], key="priority-approve-portfolio"),
            [running["project_id"]],
            constraints={"high_revenue_usd": "500"},
        )
        self.commercial.resolve_project_portfolio_decision(
            project_portfolio_resolution_command(
                packet_id=packet.packet_id,
                verdict="accept_prioritization",
                key="priority-approve-portfolio-accepted",
            ),
            packet.packet_id,
            verdict="accept_prioritization",
        )
        intent = self.commercial.create_project_scheduling_intent(
            project_scheduling_intent_command(packet_id=packet.packet_id, key="priority-approve-intent"),
            packet.packet_id,
        )
        priority_packet = self.commercial.create_project_scheduling_priority_change_packet(
            project_scheduling_priority_packet_command(intent_id=intent.intent_id, key="priority-approve-packet"),
            intent.intent_id,
        )
        with self.store.connect() as conn:
            queued_before = conn.execute(
                "SELECT COUNT(*) FROM project_tasks WHERE project_id=? AND status='queued'",
                (running["project_id"],),
            ).fetchone()[0]

        resolution = self.commercial.resolve_project_scheduling_priority_change_packet(
            project_scheduling_priority_resolution_command(
                packet_id=priority_packet.packet_id,
                verdict="accept_priority_changes",
                key="priority-approve-resolution",
            ),
            priority_packet.packet_id,
            verdict="accept_priority_changes",
        )

        self.assertEqual(resolution["authority_effect"], "bounded_internal_queue_changes")
        self.assertEqual(len([change for change in resolution["applied_changes"] if change["status"] == "queued"]), 1)
        with self.store.connect() as conn:
            queued_after = conn.execute(
                "SELECT COUNT(*) FROM project_tasks WHERE project_id=? AND status='queued'",
                (running["project_id"],),
            ).fetchone()[0]
            task_row = conn.execute(
                """
                SELECT inputs_json, evidence_refs_json, authority_required, task_type
                FROM project_tasks
                WHERE task_id=?
                """,
                (resolution["applied_changes"][0]["task_id"],),
            ).fetchone()
        self.assertEqual(queued_after, queued_before + 1)
        self.assertEqual(task_row["authority_required"], "rule")
        self.assertIn(task_row["task_type"], {"operate", "feedback"})
        self.assertFalse(json.loads(task_row["inputs_json"])["customer_commitments_allowed"])
        self.assertTrue(
            any(ref.startswith("kernel:project_scheduling_priority_change_packets/") for ref in json.loads(task_row["evidence_refs_json"]))
        )

    def test_priority_change_reject_or_defer_leaves_queue_unchanged(self):
        for verdict in ("reject_priority_changes", "defer"):
            with self.subTest(verdict=verdict):
                running = self.budgeted_running_operate_task(f"priority-{verdict}", budget_cap=Decimal("300"))
                packet = self.commercial.create_project_portfolio_decision_packet(
                    project_portfolio_packet_command(project_ids=[running["project_id"]], key=f"{verdict}-portfolio"),
                    [running["project_id"]],
                )
                self.commercial.resolve_project_portfolio_decision(
                    project_portfolio_resolution_command(
                        packet_id=packet.packet_id,
                        verdict="accept_prioritization",
                        key=f"{verdict}-portfolio-accepted",
                    ),
                    packet.packet_id,
                    verdict="accept_prioritization",
                )
                intent = self.commercial.create_project_scheduling_intent(
                    project_scheduling_intent_command(packet_id=packet.packet_id, key=f"{verdict}-intent"),
                    packet.packet_id,
                )
                priority_packet = self.commercial.create_project_scheduling_priority_change_packet(
                    project_scheduling_priority_packet_command(intent_id=intent.intent_id, key=f"{verdict}-packet"),
                    intent.intent_id,
                )
                with self.store.connect() as conn:
                    queued_before = conn.execute(
                        "SELECT COUNT(*) FROM project_tasks WHERE project_id=? AND status='queued'",
                        (running["project_id"],),
                    ).fetchone()[0]
                resolution = self.commercial.resolve_project_scheduling_priority_change_packet(
                    project_scheduling_priority_resolution_command(
                        packet_id=priority_packet.packet_id,
                        verdict=verdict,
                        key=f"{verdict}-resolution",
                    ),
                    priority_packet.packet_id,
                    verdict=verdict,
                )
                with self.store.connect() as conn:
                    queued_after = conn.execute(
                        "SELECT COUNT(*) FROM project_tasks WHERE project_id=? AND status='queued'",
                        (running["project_id"],),
                    ).fetchone()[0]
                self.assertEqual(queued_after, queued_before)
                self.assertEqual(resolution["authority_effect"], "no_queue_changes")
                self.assertTrue(all(change["status"] == "not_applied" for change in resolution["applied_changes"]))

    def test_priority_change_packets_fail_closed_for_autonomous_mutation_cancellation_and_commitments(self):
        running = self.budgeted_running_operate_task("priority-fail-closed", budget_cap=Decimal("300"))
        packet = self.commercial.create_project_portfolio_decision_packet(
            project_portfolio_packet_command(project_ids=[running["project_id"]], key="priority-fail-portfolio"),
            [running["project_id"]],
        )
        self.commercial.resolve_project_portfolio_decision(
            project_portfolio_resolution_command(
                packet_id=packet.packet_id,
                verdict="accept_prioritization",
                key="priority-fail-portfolio-accepted",
            ),
            packet.packet_id,
            verdict="accept_prioritization",
        )
        intent = self.commercial.create_project_scheduling_intent(
            project_scheduling_intent_command(packet_id=packet.packet_id, key="priority-fail-intent"),
            packet.packet_id,
        )
        blocked_payloads = [
            {"intent_id": intent.intent_id, "autonomous_queue_mutation": True},
            {"intent_id": intent.intent_id, "autonomous_reprioritization": True},
            {"intent_id": intent.intent_id, "autonomous_cancellation": True},
            {"intent_id": intent.intent_id, "customer_commitment_requested": True},
            {"intent_id": intent.intent_id, "priority_change_apply_requested": True},
        ]
        for index, payload in enumerate(blocked_payloads):
            with self.subTest(payload=payload):
                with self.assertRaises(PermissionError):
                    self.commercial.create_project_scheduling_priority_change_packet(
                        project_scheduling_priority_packet_command(
                            intent_id=intent.intent_id,
                            key=f"priority-packet-blocked-{index}",
                            payload=payload,
                        ),
                        intent.intent_id,
                    )
        priority_packet = self.commercial.create_project_scheduling_priority_change_packet(
            project_scheduling_priority_packet_command(intent_id=intent.intent_id, key="priority-fail-packet"),
            intent.intent_id,
        )
        for index, payload in enumerate(blocked_payloads[:4]):
            with self.subTest(resolution_payload=payload):
                with self.assertRaises(PermissionError):
                    self.commercial.resolve_project_scheduling_priority_change_packet(
                        project_scheduling_priority_resolution_command(
                            packet_id=priority_packet.packet_id,
                            verdict="accept_priority_changes",
                            key=f"priority-resolution-blocked-{index}",
                            payload={"packet_id": priority_packet.packet_id, **payload},
                        ),
                        priority_packet.packet_id,
                        verdict="accept_priority_changes",
                    )
        with self.store.connect() as conn:
            statuses = [
                row["status"]
                for row in conn.execute(
                    "SELECT status FROM project_tasks WHERE project_id=? ORDER BY created_at",
                    (running["project_id"],),
                ).fetchall()
            ]
        self.assertIn("running", statuses)
        self.assertNotIn("cancelled", statuses)

    def test_priority_change_replay_projection_comparison_remains_clean_for_accept_and_reject(self):
        for verdict in ("accept_priority_changes", "reject_priority_changes"):
            with self.subTest(verdict=verdict):
                running = self.budgeted_running_operate_task(f"priority-replay-{verdict}", budget_cap=Decimal("400"))
                self.commercial.record_project_operate_followup_outcome(
                    project_operate_followup_outcome_command(
                        project_id=running["project_id"],
                        task_id=running["task_id"],
                        key=f"{verdict}-replay-outcome",
                    ),
                    running["task_id"],
                    summary="Reconciled revenue evidence for priority replay comparison.",
                    internal_result_ref=f"artifact://local/{verdict}/priority-replay",
                    operator_load_minutes=3,
                    operator_load_source="operator_reported",
                    result={"reconciliation_status": "reconciled"},
                    revenue_impact={"amount_usd": "525", "currency": "USD", "period": "2026-05"},
                )
                packet = self.commercial.create_project_portfolio_decision_packet(
                    project_portfolio_packet_command(project_ids=[running["project_id"]], key=f"{verdict}-replay-portfolio"),
                    [running["project_id"]],
                    constraints={"high_revenue_usd": "500"},
                )
                self.commercial.resolve_project_portfolio_decision(
                    project_portfolio_resolution_command(
                        packet_id=packet.packet_id,
                        verdict="accept_prioritization",
                        key=f"{verdict}-replay-portfolio-accepted",
                    ),
                    packet.packet_id,
                    verdict="accept_prioritization",
                )
                intent = self.commercial.create_project_scheduling_intent(
                    project_scheduling_intent_command(packet_id=packet.packet_id, key=f"{verdict}-replay-intent"),
                    packet.packet_id,
                )
                priority_packet = self.commercial.create_project_scheduling_priority_change_packet(
                    project_scheduling_priority_packet_command(intent_id=intent.intent_id, key=f"{verdict}-replay-packet"),
                    intent.intent_id,
                )
                self.commercial.resolve_project_scheduling_priority_change_packet(
                    project_scheduling_priority_resolution_command(
                        packet_id=priority_packet.packet_id,
                        verdict=verdict,
                        key=f"{verdict}-replay-resolution",
                    ),
                    priority_packet.packet_id,
                    verdict=verdict,
                )
                comparison = self.commercial.compare_project_scheduling_priority_replay_to_projection(
                    project_scheduling_priority_replay_comparison_command(
                        packet_id=priority_packet.packet_id,
                        key=f"{verdict}-priority-replay-compare",
                    ),
                    priority_packet.packet_id,
                )
                self.assertTrue(comparison.matches)
                self.assertEqual(comparison.mismatches, [])
                replay = self.store.replay_critical_state()
                self.assertTrue(
                    replay.project_scheduling_priority_replay_projection_comparisons[comparison.comparison_id]["matches"]
                )

    def test_accepted_priority_created_task_produces_assignment_packet_and_requires_acceptance(self):
        created = self.accepted_priority_created_task("scheduling-assignment-packet")
        grant_id = self.store.issue_capability_grant(
            project_task_command(project_id=created["project_id"], key="scheduling-assignment-grant"),
            CapabilityGrant(
                task_id=created["task_id"],
                subject_type="agent",
                subject_id="scheduling-worker-1",
                capability_type="memory_write",
                actions=["record"],
                resource={"kind": "project_internal_scheduling"},
                scope={"project_id": created["project_id"]},
                conditions={"external_side_effects": "blocked_without_operator_gate_and_receipt"},
                expires_at="2999-01-01T00:00:00Z",
                policy_version=KERNEL_POLICY_VERSION,
                max_uses=1,
            ),
        )

        assignment_id = self.commercial.create_project_scheduling_worker_assignment_packet(
            project_scheduling_assignment_packet_command(task_id=created["task_id"], key="scheduling-assignment-packet"),
            created["task_id"],
            worker_id="scheduling-worker-1",
            grant_ids=[grant_id],
        )

        with self.store.connect() as conn:
            task = conn.execute("SELECT status, budget_id FROM project_tasks WHERE task_id=?", (created["task_id"],)).fetchone()
            assignment = conn.execute(
                "SELECT status, grant_ids_json, accepted_capabilities_json FROM project_task_assignments WHERE assignment_id=?",
                (assignment_id,),
            ).fetchone()
        comparison = self.commercial.compare_project_replay_to_projection(
            project_replay_comparison_command(project_id=created["project_id"], key="scheduling-assignment-packet-compare"),
            created["project_id"],
        )
        self.assertEqual(task["status"], "queued")
        self.assertEqual(task["budget_id"], created["budget_id"])
        self.assertEqual(assignment["status"], "assigned")
        self.assertEqual(json.loads(assignment["grant_ids_json"]), [grant_id])
        self.assertEqual(json.loads(assignment["accepted_capabilities_json"]), [])
        self.assertTrue(comparison.matches)
        self.assertEqual(comparison.mismatches, [])

    def test_worker_acceptance_with_valid_grants_runs_priority_created_task(self):
        created = self.accepted_priority_created_task("scheduling-assignment-accept")
        grant_id = self.store.issue_capability_grant(
            project_task_command(project_id=created["project_id"], key="scheduling-assignment-accept-worker-grant"),
            CapabilityGrant(
                task_id=created["task_id"],
                subject_type="agent",
                subject_id="scheduling-worker-accept",
                capability_type="memory_write",
                actions=["record"],
                resource={"kind": "project_internal_scheduling"},
                scope={"project_id": created["project_id"]},
                conditions={"external_side_effects": "blocked_without_operator_gate_and_receipt"},
                expires_at="2999-01-01T00:00:00Z",
                policy_version=KERNEL_POLICY_VERSION,
                max_uses=1,
            ),
        )
        assignment_id = self.commercial.create_project_scheduling_worker_assignment_packet(
            project_scheduling_assignment_packet_command(task_id=created["task_id"], key="scheduling-assignment-accept-packet"),
            created["task_id"],
            worker_id="scheduling-worker-accept",
            grant_ids=[grant_id],
        )

        self.commercial.resolve_project_scheduling_worker_assignment(
            project_scheduling_assignment_resolution_command(
                assignment_id=assignment_id,
                verdict="accept",
                key="scheduling-assignment-accept-resolution",
                requester_id="scheduling-worker-accept",
            ),
            assignment_id,
            verdict="accept",
            worker_id="scheduling-worker-accept",
            accepted_capabilities=[
                {"capability_type": "memory_write", "actions": ["record"], "scope": "project_internal_scheduling"}
            ],
        )

        replay = self.store.replay_critical_state()
        self.assertEqual(replay.project_tasks[created["task_id"]]["status"], "running")
        self.assertEqual(replay.project_task_assignments[assignment_id]["status"], "accepted")
        self.assertEqual(replay.project_task_assignments[assignment_id]["grant_ids"], [grant_id])

    def test_worker_rejection_leaves_priority_created_task_queued(self):
        created = self.accepted_priority_created_task("scheduling-assignment-reject")
        grant_id = self.store.issue_capability_grant(
            project_task_command(project_id=created["project_id"], key="scheduling-assignment-reject-worker-grant"),
            CapabilityGrant(
                task_id=created["task_id"],
                subject_type="agent",
                subject_id="scheduling-worker-reject",
                capability_type="memory_write",
                actions=["record"],
                resource={"kind": "project_internal_scheduling"},
                scope={"project_id": created["project_id"]},
                conditions={"external_side_effects": "blocked_without_operator_gate_and_receipt"},
                expires_at="2999-01-01T00:00:00Z",
                policy_version=KERNEL_POLICY_VERSION,
                max_uses=1,
            ),
        )
        assignment_id = self.commercial.create_project_scheduling_worker_assignment_packet(
            project_scheduling_assignment_packet_command(task_id=created["task_id"], key="scheduling-assignment-reject-packet"),
            created["task_id"],
            worker_id="scheduling-worker-reject",
            grant_ids=[grant_id],
        )

        self.commercial.resolve_project_scheduling_worker_assignment(
            project_scheduling_assignment_resolution_command(
                assignment_id=assignment_id,
                verdict="reject",
                key="scheduling-assignment-reject-resolution",
                requester_id="scheduling-worker-reject",
            ),
            assignment_id,
            verdict="reject",
            worker_id="scheduling-worker-reject",
            notes="worker unavailable",
        )

        with self.store.connect() as conn:
            task = conn.execute("SELECT status FROM project_tasks WHERE task_id=?", (created["task_id"],)).fetchone()
            assignment = conn.execute("SELECT status FROM project_task_assignments WHERE assignment_id=?", (assignment_id,)).fetchone()
        comparison = self.commercial.compare_project_replay_to_projection(
            project_replay_comparison_command(project_id=created["project_id"], key="scheduling-assignment-reject-compare"),
            created["project_id"],
        )
        self.assertEqual(task["status"], "queued")
        self.assertEqual(assignment["status"], "rejected")
        self.assertTrue(comparison.matches)
        self.assertEqual(comparison.mismatches, [])

    def test_missing_budget_or_capability_evidence_blocks_priority_created_assignment(self):
        created = self.accepted_priority_created_task("scheduling-assignment-blocked")
        with self.assertRaises(PermissionError):
            self.commercial.create_project_scheduling_worker_assignment_packet(
                project_scheduling_assignment_packet_command(task_id=created["task_id"], key="scheduling-assignment-no-grant"),
                created["task_id"],
                worker_id="scheduling-worker-missing-grant",
                grant_ids=[],
            )

        budgetless = self.accepted_priority_created_task("scheduling-assignment-no-budget")
        self.store.transition_project_task(
            project_task_command(project_id=budgetless["project_id"], key="scheduling-assignment-no-budget-block"),
            budgetless["task_id"],
            "blocked",
            "simulate missing budget evidence before assignment",
        )
        with self.store.connect() as conn:
            conn.execute("UPDATE project_tasks SET budget_id=NULL WHERE task_id=?", (budgetless["task_id"],))
            conn.commit()
        grant_id = self.store.issue_capability_grant(
            project_task_command(project_id=budgetless["project_id"], key="scheduling-assignment-no-budget-worker-grant"),
            CapabilityGrant(
                task_id=budgetless["task_id"],
                subject_type="agent",
                subject_id="scheduling-worker-no-budget",
                capability_type="memory_write",
                actions=["record"],
                resource={"kind": "project_internal_scheduling"},
                scope={"project_id": budgetless["project_id"]},
                conditions={"external_side_effects": "blocked_without_operator_gate_and_receipt"},
                expires_at="2999-01-01T00:00:00Z",
                policy_version=KERNEL_POLICY_VERSION,
                max_uses=1,
            ),
        )
        with self.assertRaises(PermissionError):
            self.commercial.create_project_scheduling_worker_assignment_packet(
                project_scheduling_assignment_packet_command(task_id=budgetless["task_id"], key="scheduling-assignment-missing-budget"),
                budgetless["task_id"],
                worker_id="scheduling-worker-no-budget",
                grant_ids=[grant_id],
            )

    def test_scheduling_assignment_customer_commitments_and_autonomous_paths_fail_closed(self):
        created = self.accepted_priority_created_task("scheduling-assignment-fail-closed")
        grant_id = self.store.issue_capability_grant(
            project_task_command(project_id=created["project_id"], key="scheduling-assignment-fail-worker-grant"),
            CapabilityGrant(
                task_id=created["task_id"],
                subject_type="agent",
                subject_id="scheduling-worker-fail",
                capability_type="memory_write",
                actions=["record"],
                resource={"kind": "project_internal_scheduling"},
                scope={"project_id": created["project_id"]},
                conditions={"external_side_effects": "blocked_without_operator_gate_and_receipt"},
                expires_at="2999-01-01T00:00:00Z",
                policy_version=KERNEL_POLICY_VERSION,
                max_uses=1,
            ),
        )
        blocked_payloads = [
            {"task_id": created["task_id"], "autonomous_assignment": True},
            {"task_id": created["task_id"], "customer_commitment_requested": True},
            {"task_id": created["task_id"], "external_side_effect_requested": True},
        ]
        for index, payload in enumerate(blocked_payloads):
            with self.subTest(payload=payload):
                with self.assertRaises(PermissionError):
                    self.commercial.create_project_scheduling_worker_assignment_packet(
                        project_scheduling_assignment_packet_command(
                            task_id=created["task_id"],
                            key=f"scheduling-assignment-packet-blocked-{index}",
                            payload=payload,
                        ),
                        created["task_id"],
                        worker_id="scheduling-worker-fail",
                        grant_ids=[grant_id],
                    )
        with self.assertRaises(PermissionError):
            self.commercial.create_project_scheduling_worker_assignment_packet(
                project_scheduling_assignment_packet_command(
                    task_id=created["task_id"],
                    key="scheduling-assignment-agent-self-assign",
                    requested_by="agent",
                ),
                created["task_id"],
                worker_id="scheduling-worker-fail",
                grant_ids=[grant_id],
            )

    def test_accepted_scheduling_assignment_records_internal_outcome_with_preserved_evidence(self):
        created = self.accepted_assigned_priority_created_task("scheduling-outcome-internal")

        result = self.commercial.record_project_scheduling_task_outcome(
            project_scheduling_task_outcome_command(
                project_id=created["project_id"],
                task_id=created["task_id"],
                key="scheduling-outcome-internal-record",
                requester_id=created["worker_id"],
            ),
            created["task_id"],
            summary="Recorded internal queue execution evidence for the next operating cycle.",
            internal_result_ref="artifact://local/scheduling-outcome/internal-note",
            result={"maintenance_status": "resolved"},
            metrics={"queue_delta": 1},
        )

        with self.store.connect() as conn:
            outcome = conn.execute(
                """
                SELECT outcome_type, status, artifact_refs_json, feedback_json,
                       side_effect_intent_id, side_effect_receipt_id
                FROM project_outcomes
                WHERE outcome_id=?
                """,
                (result["outcome_id"],),
            ).fetchone()
            task = conn.execute("SELECT status FROM project_tasks WHERE task_id=?", (created["task_id"],)).fetchone()
        feedback = json.loads(outcome["feedback_json"])
        artifact_refs = json.loads(outcome["artifact_refs_json"])

        self.assertEqual(outcome["outcome_type"], "operate_followup")
        self.assertEqual(outcome["status"], "accepted")
        self.assertEqual(task["status"], "completed")
        self.assertIsNone(outcome["side_effect_intent_id"])
        self.assertIsNone(outcome["side_effect_receipt_id"])
        self.assertEqual(feedback["assignment_id"], created["assignment_id"])
        self.assertEqual(feedback["budget_id"], created["budget_id"])
        self.assertEqual(feedback["grant_ids"], [created["grant_id"]])
        self.assertEqual(feedback["scheduling_priority_packet_id"], created["priority_packet_id"])
        self.assertFalse(feedback["external_commitment_change"])
        self.assertIn(f"kernel:project_task_assignments/{created['assignment_id']}", artifact_refs)
        self.assertIn(f"kernel:budgets/{created['budget_id']}", artifact_refs)
        self.assertIn(f"kernel:capability_grants/{created['grant_id']}", artifact_refs)

    def test_scheduling_outcome_blocks_customer_commitments_and_receiptless_side_effects(self):
        for index, kwargs in enumerate(
            [
                {"external_commitment_change": True},
                {"side_effect_intent_id": new_id()},
            ]
        ):
            with self.subTest(kwargs=kwargs):
                created = self.accepted_assigned_priority_created_task(f"scheduling-outcome-blocked-{index}")
                with self.assertRaises(PermissionError):
                    self.commercial.record_project_scheduling_task_outcome(
                        project_scheduling_task_outcome_command(
                            project_id=created["project_id"],
                            task_id=created["task_id"],
                            key=f"scheduling-outcome-blocked-{index}-record",
                            requester_id=created["worker_id"],
                            payload={
                                "project_id": created["project_id"],
                                "task_id": created["task_id"],
                                "customer_commitment_requested": True,
                            },
                        ),
                        created["task_id"],
                        summary="Attempted customer-visible scheduling outcome.",
                        internal_result_ref=f"artifact://local/scheduling-outcome-blocked/{index}",
                        **kwargs,
                    )
        created = self.accepted_assigned_priority_created_task("scheduling-outcome-operator-receiptless")
        with self.assertRaises(PermissionError):
            self.commercial.record_project_scheduling_task_outcome(
                project_scheduling_task_outcome_command(
                    project_id=created["project_id"],
                    task_id=created["task_id"],
                    key="scheduling-outcome-operator-receiptless-record",
                    requested_by="operator",
                ),
                created["task_id"],
                summary="Operator-gated side-effect intent still lacks durable receipt evidence.",
                internal_result_ref="artifact://local/scheduling-outcome-operator-receiptless",
                side_effect_intent_id=new_id(),
            )

    def test_completed_scheduling_task_updates_rollups_without_corrupting_scheduling_evidence(self):
        created = self.accepted_assigned_priority_created_task("scheduling-outcome-rollup")
        outcome = self.commercial.record_project_scheduling_task_outcome(
            project_scheduling_task_outcome_command(
                project_id=created["project_id"],
                task_id=created["task_id"],
                key="scheduling-outcome-rollup-record",
                requester_id=created["worker_id"],
            ),
            created["task_id"],
            summary="Completed internal maintenance work generated by accepted scheduling priority evidence.",
            internal_result_ref="artifact://local/scheduling-outcome-rollup/maintenance",
            result={"maintenance_status": "resolved"},
        )
        rollup = self.commercial.derive_project_status_rollup(
            project_status_rollup_command(project_id=created["project_id"], key="scheduling-outcome-rollup-final"),
            created["project_id"],
        )
        priority_comparison = self.commercial.compare_project_scheduling_priority_replay_to_projection(
            project_scheduling_priority_replay_comparison_command(
                packet_id=created["priority_packet_id"],
                key="scheduling-outcome-rollup-priority-compare",
            ),
            created["priority_packet_id"],
        )

        self.assertEqual(rollup.task_counts["completed"], 2)
        self.assertEqual(rollup.commercial_rollup["maintenance_resolved_count"], 1)
        self.assertIn(f"kernel:project_outcomes/{outcome['outcome_id']}", rollup.commercial_rollup["evidence_refs"])
        self.assertTrue(priority_comparison.matches)
        self.assertEqual(priority_comparison.mismatches, [])

    def test_completed_scheduling_outcome_feeds_close_and_portfolio_evidence(self):
        created = self.accepted_assigned_priority_created_task("scheduling-outcome-packets")
        outcome = self.commercial.record_project_scheduling_task_outcome(
            project_scheduling_task_outcome_command(
                project_id=created["project_id"],
                task_id=created["task_id"],
                key="scheduling-outcome-packets-record",
                requester_id=created["worker_id"],
            ),
            created["task_id"],
            summary="Completed scheduling-created revenue planning evidence for operator review.",
            internal_result_ref="artifact://local/scheduling-outcome-packets/revenue",
            result={"scheduling_outcome_type": "revenue_reconciliation", "reconciliation_status": "reconciled"},
            revenue_impact={"amount_usd": "325", "currency": "USD", "period": "2026-05"},
        )
        rollup = self.commercial.derive_project_status_rollup(
            project_status_rollup_command(project_id=created["project_id"], key="scheduling-outcome-packets-rollup"),
            created["project_id"],
        )
        close_packet = self.commercial.create_project_close_decision(
            project_close_decision_command(project_id=created["project_id"], key="scheduling-outcome-packets-close"),
            created["project_id"],
            rollup_id=rollup.rollup_id,
        )
        portfolio_packet = self.commercial.create_project_portfolio_decision_packet(
            project_portfolio_packet_command(project_ids=[created["project_id"]], key="scheduling-outcome-packets-portfolio"),
            [created["project_id"]],
        )
        portfolio_comparison = self.commercial.compare_project_portfolio_replay_to_projection(
            project_portfolio_replay_comparison_command(
                packet_id=portfolio_packet.packet_id,
                key="scheduling-outcome-packets-portfolio-compare",
            ),
            portfolio_packet.packet_id,
        )

        lineage_refs = [
            f"kernel:project_outcomes/{outcome['outcome_id']}",
            f"kernel:project_task_assignments/{created['assignment_id']}",
            f"kernel:project_scheduling_priority_change_packets/{created['priority_packet_id']}",
            f"kernel:capability_grants/{created['grant_id']}",
            f"kernel:budgets/{created['budget_id']}",
        ]
        for ref in lineage_refs:
            self.assertIn(ref, rollup.commercial_rollup["evidence_refs"])
            self.assertIn(ref, close_packet.evidence_refs)
            self.assertIn(ref, portfolio_packet.evidence_refs)
            self.assertIn(ref, portfolio_packet.packet["ranked_projects"][0]["evidence_refs"])
        self.assertEqual(rollup.commercial_rollup["revenue_reconciled_usd"], "925")
        self.assertTrue(portfolio_comparison.matches)
        self.assertEqual(portfolio_comparison.mismatches, [])

    def test_completed_scheduling_outcome_drives_internal_bounded_follow_on_planning(self):
        created = self.accepted_assigned_priority_created_task("scheduling-outcome-follow-on")
        outcome = self.commercial.record_project_scheduling_task_outcome(
            project_scheduling_task_outcome_command(
                project_id=created["project_id"],
                task_id=created["task_id"],
                key="scheduling-outcome-follow-on-record",
                requester_id=created["worker_id"],
            ),
            created["task_id"],
            summary="Completed scheduling-created internal evidence for the next bounded planning cycle.",
            internal_result_ref="artifact://local/scheduling-outcome-follow-on/revenue",
            result={"scheduling_outcome_type": "revenue_reconciliation", "reconciliation_status": "reconciled"},
            revenue_impact={"amount_usd": "450", "currency": "USD", "period": "2026-05"},
        )
        portfolio_packet = self.commercial.create_project_portfolio_decision_packet(
            project_portfolio_packet_command(project_ids=[created["project_id"]], key="scheduling-outcome-follow-on-portfolio"),
            [created["project_id"]],
            constraints={"high_revenue_usd": "250"},
        )
        self.commercial.resolve_project_portfolio_decision(
            project_portfolio_resolution_command(
                packet_id=portfolio_packet.packet_id,
                verdict="accept_prioritization",
                key="scheduling-outcome-follow-on-portfolio-accepted",
            ),
            portfolio_packet.packet_id,
            verdict="accept_prioritization",
        )
        intent = self.commercial.create_project_scheduling_intent(
            project_scheduling_intent_command(packet_id=portfolio_packet.packet_id, key="scheduling-outcome-follow-on-intent"),
            portfolio_packet.packet_id,
            scheduling_window="next_internal_planning_cycle",
        )
        priority_packet = self.commercial.create_project_scheduling_priority_change_packet(
            project_scheduling_priority_packet_command(intent_id=intent.intent_id, key="scheduling-outcome-follow-on-priority"),
            intent.intent_id,
        )
        resolution = self.commercial.resolve_project_scheduling_priority_change_packet(
            project_scheduling_priority_resolution_command(
                packet_id=priority_packet.packet_id,
                verdict="accept_priority_changes",
                key="scheduling-outcome-follow-on-priority-accepted",
            ),
            priority_packet.packet_id,
            verdict="accept_priority_changes",
        )
        scheduling_comparison = self.commercial.compare_project_scheduling_replay_to_projection(
            project_scheduling_replay_comparison_command(
                intent_id=intent.intent_id,
                key="scheduling-outcome-follow-on-scheduling-compare",
            ),
            intent.intent_id,
        )
        priority_comparison = self.commercial.compare_project_scheduling_priority_replay_to_projection(
            project_scheduling_priority_replay_comparison_command(
                packet_id=priority_packet.packet_id,
                key="scheduling-outcome-follow-on-priority-compare",
            ),
            priority_packet.packet_id,
        )

        applied = next(change for change in resolution["applied_changes"] if change["status"] == "queued")
        with self.store.connect() as conn:
            task = conn.execute(
                """
                SELECT authority_required, inputs_json, evidence_refs_json
                FROM project_tasks
                WHERE task_id=?
                """,
                (applied["task_id"],),
            ).fetchone()
        inputs = json.loads(task["inputs_json"])
        evidence_refs = json.loads(task["evidence_refs_json"])
        self.assertEqual(resolution["authority_effect"], "bounded_internal_queue_changes")
        self.assertEqual(applied["customer_visible"], False)
        self.assertEqual(applied["external_side_effects_authorized"], [])
        self.assertEqual(task["authority_required"], "rule")
        self.assertFalse(inputs["customer_visible"])
        self.assertFalse(inputs["customer_commitments_allowed"])
        self.assertEqual(inputs["external_side_effects_authorized"], [])
        self.assertIn(f"kernel:project_outcomes/{outcome['outcome_id']}", evidence_refs)
        self.assertTrue(scheduling_comparison.matches)
        self.assertEqual(scheduling_comparison.mismatches, [])
        self.assertTrue(priority_comparison.matches)
        self.assertEqual(priority_comparison.mismatches, [])

    def test_completed_internal_outcome_produces_operator_gated_customer_visible_packet_evidence(self):
        completed = self.completed_internal_scheduling_outcome("customer-visible-packet")
        intent = self.staged_customer_visible_intent(
            "customer-visible-packet",
            completed["project_id"],
            completed["task_id"],
        )

        packet = self.commercial.create_project_customer_visible_packet(
            project_customer_visible_packet_command(
                outcome_id=completed["outcome_id"],
                key="customer-visible-packet-create",
            ),
            completed["outcome_id"],
            packet_type="customer_message",
            customer_ref="customer-customer-visible-packet",
            channel="email",
            subject="Support response draft",
            summary="Operator packet for sending the completed support response draft.",
            payload_ref="artifact://local/customer-visible-packet/customer-response-draft",
            side_effect_intent_id=intent["intent_id"],
        )

        with self.store.connect() as conn:
            commitments = conn.execute("SELECT COUNT(*) FROM project_customer_commitments").fetchone()[0]
            receipts = conn.execute("SELECT COUNT(*) FROM side_effect_receipts").fetchone()[0]
        self.assertEqual(packet.required_authority, "operator_gate")
        self.assertEqual(packet.status, "gated")
        self.assertIn(f"kernel:project_outcomes/{completed['outcome_id']}", packet.evidence_refs)
        self.assertIn(f"kernel:side_effect_intents/{intent['intent_id']}", packet.evidence_refs)
        self.assertIn("customer_visible_commitment_requires_receipt", packet.risk_flags)
        self.assertEqual(commitments, 0)
        self.assertEqual(receipts, 0)

    def test_rejected_customer_visible_packet_records_no_commitment_or_external_receipt(self):
        completed = self.completed_internal_scheduling_outcome("customer-visible-rejected")
        intent = self.staged_customer_visible_intent(
            "customer-visible-rejected",
            completed["project_id"],
            completed["task_id"],
        )
        packet = self.commercial.create_project_customer_visible_packet(
            project_customer_visible_packet_command(
                outcome_id=completed["outcome_id"],
                key="customer-visible-rejected-create",
            ),
            completed["outcome_id"],
            packet_type="customer_message",
            customer_ref="customer-rejected",
            channel="email",
            subject="Support response draft",
            summary="Operator packet that should be rejected without customer commitment.",
            payload_ref="artifact://local/customer-visible-rejected/customer-response-draft",
            side_effect_intent_id=intent["intent_id"],
        )

        resolution = self.commercial.resolve_project_customer_visible_packet(
            project_customer_visible_resolution_command(
                packet_id=packet.packet_id,
                verdict="reject_customer_visible_packet",
                key="customer-visible-rejected-resolution",
            ),
            packet.packet_id,
            verdict="reject_customer_visible_packet",
            operator_id="operator",
            notes="Do not send this customer-visible communication.",
        )

        with self.store.connect() as conn:
            commitments = conn.execute("SELECT COUNT(*) FROM project_customer_commitments").fetchone()[0]
            receipts = conn.execute("SELECT COUNT(*) FROM side_effect_receipts").fetchone()[0]
            packet_row = conn.execute(
                "SELECT status, verdict FROM project_customer_visible_packets WHERE packet_id=?",
                (packet.packet_id,),
            ).fetchone()
        self.assertEqual(resolution["customer_commitments"], [])
        self.assertIsNone(resolution["customer_commitment_id"])
        self.assertEqual(commitments, 0)
        self.assertEqual(receipts, 0)
        self.assertEqual(packet_row["status"], "decided")
        self.assertEqual(packet_row["verdict"], "reject_customer_visible_packet")

    def test_accepted_customer_visible_packet_requires_receipt_before_commitment(self):
        completed = self.completed_internal_scheduling_outcome("customer-visible-accepted")
        intent = self.staged_customer_visible_intent(
            "customer-visible-accepted",
            completed["project_id"],
            completed["task_id"],
        )
        packet = self.commercial.create_project_customer_visible_packet(
            project_customer_visible_packet_command(
                outcome_id=completed["outcome_id"],
                key="customer-visible-accepted-create",
            ),
            completed["outcome_id"],
            packet_type="customer_message",
            customer_ref="customer-accepted",
            channel="email",
            subject="Support response draft",
            summary="Operator packet for a customer-visible support response.",
            payload_ref="artifact://local/customer-visible-accepted/customer-response-draft",
            side_effect_intent_id=intent["intent_id"],
        )

        with self.assertRaises(PermissionError):
            self.commercial.resolve_project_customer_visible_packet(
                project_customer_visible_resolution_command(
                    packet_id=packet.packet_id,
                    verdict="accept_customer_visible_packet",
                    key="customer-visible-accepted-no-receipt",
                ),
                packet.packet_id,
                verdict="accept_customer_visible_packet",
            )
        receipt_id = self.store.record_side_effect_receipt(
            project_task_command(project_id=completed["project_id"], key="customer-visible-accepted-receipt"),
            SideEffectReceipt(
                intent_id=intent["intent_id"],
                receipt_type="success",
                receipt_hash=payload_hash({"sent": True, "packet": packet.packet_id}),
                details={"message_ref": "artifact://local/customer-visible-accepted/customer-response-draft"},
            ),
        )
        resolution = self.commercial.resolve_project_customer_visible_packet(
            project_customer_visible_resolution_command(
                packet_id=packet.packet_id,
                verdict="accept_customer_visible_packet",
                key="customer-visible-accepted-resolution",
            ),
            packet.packet_id,
            verdict="accept_customer_visible_packet",
            side_effect_receipt_id=receipt_id,
            operator_id="operator",
        )

        with self.store.connect() as conn:
            commitment = conn.execute(
                """
                SELECT packet_id, outcome_id, side_effect_intent_id, side_effect_receipt_id,
                       commitment_type, evidence_refs_json
                FROM project_customer_commitments
                WHERE commitment_id=?
                """,
                (resolution["customer_commitment_id"],),
            ).fetchone()
        evidence_refs = json.loads(commitment["evidence_refs_json"])
        self.assertEqual(commitment["packet_id"], packet.packet_id)
        self.assertEqual(commitment["outcome_id"], completed["outcome_id"])
        self.assertEqual(commitment["side_effect_intent_id"], intent["intent_id"])
        self.assertEqual(commitment["side_effect_receipt_id"], receipt_id)
        self.assertEqual(commitment["commitment_type"], "message_sent")
        self.assertIn(f"kernel:project_outcomes/{completed['outcome_id']}", evidence_refs)
        self.assertIn(f"kernel:side_effect_receipts/{receipt_id}", evidence_refs)

    def test_customer_visible_autonomous_paths_fail_closed(self):
        completed = self.completed_internal_scheduling_outcome("customer-visible-autonomous")
        intent = self.staged_customer_visible_intent(
            "customer-visible-autonomous",
            completed["project_id"],
            completed["task_id"],
        )
        with self.assertRaises(PermissionError):
            self.commercial.create_project_customer_visible_packet(
                project_customer_visible_packet_command(
                    outcome_id=completed["outcome_id"],
                    key="customer-visible-agent-create",
                    requested_by="agent",
                    requested_authority="operator_gate",
                ),
                completed["outcome_id"],
                packet_type="customer_message",
                customer_ref="customer-autonomous",
                channel="email",
                subject="Blocked",
                summary="Blocked autonomous packet.",
                payload_ref="artifact://local/customer-visible-autonomous/customer-response-draft",
                side_effect_intent_id=intent["intent_id"],
            )
        packet = self.commercial.create_project_customer_visible_packet(
            project_customer_visible_packet_command(
                outcome_id=completed["outcome_id"],
                key="customer-visible-autonomous-create",
            ),
            completed["outcome_id"],
            packet_type="customer_message",
            customer_ref="customer-autonomous",
            channel="email",
            subject="Support response draft",
            summary="Packet stays gated until operator receipt evidence exists.",
            payload_ref="artifact://local/customer-visible-autonomous/customer-response-draft",
            side_effect_intent_id=intent["intent_id"],
        )
        with self.assertRaises(PermissionError):
            self.commercial.resolve_project_customer_visible_packet(
                project_customer_visible_resolution_command(
                    packet_id=packet.packet_id,
                    verdict="accept_customer_visible_packet",
                    key="customer-visible-agent-resolution",
                    requested_by="agent",
                    requested_authority="operator_gate",
                ),
                packet.packet_id,
                verdict="accept_customer_visible_packet",
            )

    def test_customer_visible_packet_replay_projection_comparison_remains_clean(self):
        completed = self.completed_internal_scheduling_outcome("customer-visible-replay")
        intent = self.staged_customer_visible_intent(
            "customer-visible-replay",
            completed["project_id"],
            completed["task_id"],
        )
        packet = self.commercial.create_project_customer_visible_packet(
            project_customer_visible_packet_command(
                outcome_id=completed["outcome_id"],
                key="customer-visible-replay-create",
            ),
            completed["outcome_id"],
            packet_type="customer_message",
            customer_ref="customer-replay",
            channel="email",
            subject="Support response draft",
            summary="Replay-safe customer-visible packet.",
            payload_ref="artifact://local/customer-visible-replay/customer-response-draft",
            side_effect_intent_id=intent["intent_id"],
        )
        receipt_id = self.store.record_side_effect_receipt(
            project_task_command(project_id=completed["project_id"], key="customer-visible-replay-receipt"),
            SideEffectReceipt(
                intent_id=intent["intent_id"],
                receipt_type="success",
                receipt_hash=payload_hash({"sent": True, "packet": packet.packet_id}),
                details={"message_ref": "artifact://local/customer-visible-replay/customer-response-draft"},
            ),
        )
        self.commercial.resolve_project_customer_visible_packet(
            project_customer_visible_resolution_command(
                packet_id=packet.packet_id,
                verdict="accept_customer_visible_packet",
                key="customer-visible-replay-resolution",
            ),
            packet.packet_id,
            verdict="accept_customer_visible_packet",
            side_effect_receipt_id=receipt_id,
        )
        comparison = self.commercial.compare_project_customer_visible_replay_to_projection(
            project_customer_visible_replay_comparison_command(
                packet_id=packet.packet_id,
                key="customer-visible-replay-compare",
            ),
            packet.packet_id,
        )
        replay = self.store.replay_critical_state()

        self.assertTrue(comparison.matches)
        self.assertEqual(comparison.mismatches, [])
        self.assertEqual(len(comparison.replay_commitments), 1)
        self.assertEqual(replay.side_effects[intent["intent_id"]]["receipt"]["receipt_id"], receipt_id)
        self.assertTrue(
            replay.project_customer_visible_replay_projection_comparisons[comparison.comparison_id]["matches"]
        )

    def test_customer_commitment_receipt_creates_governed_operate_followup(self):
        accepted = self.accepted_customer_visible_commitment("commitment-receipt-failure")
        result = self.commercial.record_project_customer_commitment_receipt(
            project_customer_commitment_receipt_command(
                commitment_id=accepted["commitment_id"],
                key="commitment-receipt-failure-record",
            ),
            ProjectCustomerCommitmentReceipt(
                commitment_id=accepted["commitment_id"],
                project_id=accepted["project_id"],
                receipt_type="delivery_failure",
                source_type="platform",
                summary="The email provider reported delivery failure for the customer response.",
                evidence_refs=["platform://email/delivery-failure/commitment-receipt-failure"],
                action_required=True,
                status="needs_followup",
            ),
        )
        rollup = self.commercial.derive_project_status_rollup(
            project_status_rollup_command(project_id=accepted["project_id"], key="commitment-receipt-failure-rollup"),
            accepted["project_id"],
        )
        comparison = self.commercial.compare_project_customer_visible_replay_to_projection(
            project_customer_visible_replay_comparison_command(
                packet_id=accepted["packet_id"],
                key="commitment-receipt-failure-compare",
            ),
            accepted["packet_id"],
        )

        with self.store.connect() as conn:
            receipt = conn.execute(
                """
                SELECT commitment_id, receipt_type, source_type, action_required,
                       status, followup_task_id
                FROM project_customer_commitment_receipts
                WHERE receipt_id=?
                """,
                (result["receipt_id"],),
            ).fetchone()
            task = conn.execute(
                """
                SELECT phase_name, task_type, authority_required, inputs_json,
                       expected_output_schema_json, required_capabilities_json,
                       evidence_refs_json
                FROM project_tasks
                WHERE task_id=?
                """,
                (result["followup_task_id"],),
            ).fetchone()
            commercial_rollup = conn.execute(
                "SELECT evidence_refs_json, risk_flags_json FROM project_commercial_rollups WHERE rollup_id=?",
                (rollup.commercial_rollup_id,),
            ).fetchone()

        inputs = json.loads(task["inputs_json"])
        expected_output_schema = json.loads(task["expected_output_schema_json"])
        capabilities = json.loads(task["required_capabilities_json"])
        evidence_refs = json.loads(task["evidence_refs_json"])
        commercial_evidence = json.loads(commercial_rollup["evidence_refs_json"])
        commercial_risks = json.loads(commercial_rollup["risk_flags_json"])
        self.assertEqual(receipt["commitment_id"], accepted["commitment_id"])
        self.assertEqual(receipt["receipt_type"], "delivery_failure")
        self.assertEqual(receipt["source_type"], "platform")
        self.assertEqual(receipt["action_required"], 1)
        self.assertEqual(receipt["status"], "needs_followup")
        self.assertEqual(receipt["followup_task_id"], result["followup_task_id"])
        self.assertEqual(task["phase_name"], "Operate")
        self.assertEqual(task["task_type"], "operate")
        self.assertEqual(task["authority_required"], "rule")
        self.assertEqual(inputs["commitment_id"], accepted["commitment_id"])
        self.assertEqual(inputs["customer_commitment_receipt_id"], result["receipt_id"])
        self.assertEqual(inputs["operate_followup_type"], "maintenance")
        self.assertEqual(
            expected_output_schema["properties"]["external_commitment_change"]["const"],
            False,
        )
        self.assertEqual(capabilities[0]["external_side_effects"], "blocked_without_operator_gate_and_receipt")
        self.assertIn(f"kernel:project_customer_commitments/{accepted['commitment_id']}", evidence_refs)
        self.assertIn(f"kernel:project_customer_commitment_receipts/{result['receipt_id']}", evidence_refs)
        self.assertIn("customer_delivery_failure", rollup.risk_flags)
        self.assertIn("customer_commitment_receipt_followup_open", rollup.risk_flags)
        self.assertIn(f"kernel:project_customer_commitment_receipts/{result['receipt_id']}", commercial_evidence)
        self.assertIn("customer_commitment_delivery_failure_needs_followup", commercial_risks)
        self.assertTrue(comparison.matches)
        self.assertEqual(len(comparison.replay_commitment_receipts), 1)
        self.assertEqual(comparison.replay_commitment_receipts, comparison.projection_commitment_receipts)

    def test_customer_commitment_receipts_fail_closed_before_accepted_commitment(self):
        completed = self.completed_internal_scheduling_outcome("commitment-receipt-blocked")
        intent = self.staged_customer_visible_intent(
            "commitment-receipt-blocked",
            completed["project_id"],
            completed["task_id"],
        )
        packet = self.commercial.create_project_customer_visible_packet(
            project_customer_visible_packet_command(
                outcome_id=completed["outcome_id"],
                key="commitment-receipt-blocked-create",
            ),
            completed["outcome_id"],
            packet_type="customer_message",
            customer_ref="customer-commitment-receipt-blocked",
            channel="email",
            subject="Support response draft",
            summary="Gated customer-visible packet without accepted commitment.",
            payload_ref="artifact://local/commitment-receipt-blocked/customer-response-draft",
            side_effect_intent_id=intent["intent_id"],
        )
        with self.assertRaises(ValueError):
            self.commercial.record_project_customer_commitment_receipt(
                project_customer_commitment_receipt_command(
                    commitment_id=packet.packet_id,
                    key="commitment-receipt-unknown-commitment",
                ),
                ProjectCustomerCommitmentReceipt(
                    commitment_id=packet.packet_id,
                    project_id=completed["project_id"],
                    receipt_type="timeout",
                    source_type="platform",
                    summary="No accepted commitment exists for this packet.",
                ),
            )
        accepted = self.accepted_customer_visible_commitment("commitment-receipt-agent-block")
        with self.assertRaises(PermissionError):
            self.commercial.record_project_customer_commitment_receipt(
                project_customer_commitment_receipt_command(
                    commitment_id=accepted["commitment_id"],
                    key="commitment-receipt-agent-block-record",
                    requested_by="agent",
                ),
                ProjectCustomerCommitmentReceipt(
                    commitment_id=accepted["commitment_id"],
                    project_id=accepted["project_id"],
                    receipt_type="compensation_needed",
                    source_type="customer",
                    summary="The customer requested compensation.",
                ),
            )
        with self.assertRaises(PermissionError):
            self.commercial.record_project_customer_commitment_receipt(
                project_customer_commitment_receipt_command(
                    commitment_id=accepted["commitment_id"],
                    key="commitment-receipt-no-followup-block",
                ),
                ProjectCustomerCommitmentReceipt(
                    commitment_id=accepted["commitment_id"],
                    project_id=accepted["project_id"],
                    receipt_type="timeout",
                    source_type="platform",
                    summary="The customer-visible commitment timed out.",
                    action_required=False,
                    status="recorded",
                ),
            )

    def test_commitment_receipt_followup_outcomes_complete_receipt_governance(self):
        cases = [
            (
                "customer-response",
                "customer_response",
                "customer",
                "The customer confirmed renewal interest and asked for adoption follow-up.",
                "retention",
                {"retention_status": "retained"},
                "retained_customer_count",
                1,
            ),
            (
                "delivery-failure",
                "delivery_failure",
                "platform",
                "The email provider reported delivery failure for the customer response.",
                "maintenance",
                {"maintenance_status": "resolved"},
                "maintenance_resolved_count",
                1,
            ),
            (
                "timeout",
                "timeout",
                "platform",
                "The customer-visible commitment timed out without a response.",
                "customer_support",
                {"support_status": "open"},
                "support_open_count",
                1,
            ),
            (
                "compensation",
                "compensation_needed",
                "customer",
                "The customer requested compensation after a failed delivery.",
                "customer_support",
                {"support_status": "resolved", "compensation_status": "operator_review_prepared"},
                "support_resolved_count",
                1,
            ),
        ]
        for key, receipt_type, source_type, summary, expected_type, result_payload, rollup_field, expected_value in cases:
            with self.subTest(receipt_type=receipt_type):
                running = self.running_commitment_receipt_followup_task(
                    f"commitment-receipt-outcome-{key}",
                    receipt_type=receipt_type,
                    source_type=source_type,
                    summary=summary,
                )
                result = self.commercial.record_project_operate_followup_outcome(
                    project_operate_followup_outcome_command(
                        project_id=running["project_id"],
                        task_id=running["followup_task_id"],
                        key=f"commitment-receipt-outcome-{key}-record",
                    ),
                    running["followup_task_id"],
                    summary="Completed governed internal follow-up for the customer commitment receipt.",
                    internal_result_ref=f"artifact://local/commitment-receipt-outcome-{key}/internal-result",
                    operator_load_minutes=5,
                    operator_load_source="operator_reported",
                    result=result_payload,
                )
                rollup = self.commercial.derive_project_status_rollup(
                    project_status_rollup_command(
                        project_id=running["project_id"],
                        key=f"commitment-receipt-outcome-{key}-rollup",
                    ),
                    running["project_id"],
                )
                comparison = self.commercial.compare_project_customer_visible_replay_to_projection(
                    project_customer_visible_replay_comparison_command(
                        packet_id=running["packet_id"],
                        key=f"commitment-receipt-outcome-{key}-compare",
                    ),
                    running["packet_id"],
                )

                with self.store.connect() as conn:
                    receipt = conn.execute(
                        """
                        SELECT action_required, status, followup_task_id
                        FROM project_customer_commitment_receipts
                        WHERE receipt_id=?
                        """,
                        (running["receipt_id"],),
                    ).fetchone()
                    outcome = conn.execute(
                        """
                        SELECT artifact_refs_json, feedback_json, side_effect_intent_id,
                               side_effect_receipt_id
                        FROM project_outcomes
                        WHERE outcome_id=?
                        """,
                        (result["outcome_id"],),
                    ).fetchone()
                    commercial_rollup = conn.execute(
                        "SELECT risk_flags_json FROM project_commercial_rollups WHERE rollup_id=?",
                        (rollup.commercial_rollup_id,),
                    ).fetchone()

                feedback = json.loads(outcome["feedback_json"])
                artifact_refs = json.loads(outcome["artifact_refs_json"])
                commercial_risks = json.loads(commercial_rollup["risk_flags_json"])
                self.assertEqual(result["operate_followup_type"], expected_type)
                self.assertEqual(receipt["action_required"], 0)
                self.assertEqual(receipt["status"], "accepted")
                self.assertEqual(receipt["followup_task_id"], running["followup_task_id"])
                self.assertEqual(feedback["customer_commitment_receipt_id"], running["receipt_id"])
                self.assertEqual(feedback["source_commitment_id"], running["commitment_id"])
                self.assertEqual(feedback["receipt_type"], receipt_type)
                self.assertFalse(feedback["external_commitment_change"])
                self.assertIsNone(outcome["side_effect_intent_id"])
                self.assertIsNone(outcome["side_effect_receipt_id"])
                self.assertIn(f"kernel:project_customer_commitments/{running['commitment_id']}", artifact_refs)
                self.assertIn(f"kernel:project_customer_commitment_receipts/{running['receipt_id']}", artifact_refs)
                self.assertNotIn("customer_commitment_receipt_followup_open", rollup.risk_flags)
                self.assertNotIn(f"customer_commitment_{receipt_type}_needs_followup", commercial_risks)
                self.assertEqual(rollup.commercial_rollup[rollup_field], expected_value)
                self.assertTrue(comparison.matches)
                self.assertEqual(comparison.mismatches, [])

    def test_commitment_receipt_followup_side_effect_intents_require_receipts(self):
        running = self.running_commitment_receipt_followup_task(
            "commitment-receipt-outcome-side-effect-blocked",
            receipt_type="timeout",
            source_type="platform",
            summary="The customer-visible commitment timed out without a response.",
        )
        side_effect = self.staged_operate_side_effect(
            "commitment-receipt-outcome-side-effect-blocked",
            running["project_id"],
            running["followup_task_id"],
        )
        with self.assertRaises(PermissionError):
            self.commercial.record_project_operate_followup_outcome(
                project_operate_followup_outcome_command(
                    project_id=running["project_id"],
                    task_id=running["followup_task_id"],
                    key="commitment-receipt-outcome-side-effect-blocked-record",
                    requested_by="operator",
                    requested_authority="operator_gate",
                ),
                running["followup_task_id"],
                summary="Prepared customer follow-up, but no durable execution receipt exists.",
                internal_result_ref="artifact://local/commitment-receipt-outcome-side-effect-blocked/result",
                operator_load_minutes=4,
                operator_load_source="operator_reported",
                side_effect_intent_id=side_effect["intent_id"],
            )

    def test_unaccepted_or_rejected_scheduling_assignments_cannot_record_outcomes(self):
        for verdict in ("assigned", "rejected"):
            with self.subTest(verdict=verdict):
                created = self.accepted_priority_created_task(f"scheduling-outcome-{verdict}")
                worker_id = f"scheduling-outcome-{verdict}-worker"
                grant_id = self.store.issue_capability_grant(
                    project_task_command(project_id=created["project_id"], key=f"scheduling-outcome-{verdict}-worker-grant"),
                    CapabilityGrant(
                        task_id=created["task_id"],
                        subject_type="agent",
                        subject_id=worker_id,
                        capability_type="memory_write",
                        actions=["record"],
                        resource={"kind": "project_internal_scheduling"},
                        scope={"project_id": created["project_id"]},
                        conditions={"external_side_effects": "blocked_without_operator_gate_and_receipt"},
                        expires_at="2999-01-01T00:00:00Z",
                        policy_version=KERNEL_POLICY_VERSION,
                        max_uses=1,
                    ),
                )
                assignment_id = self.commercial.create_project_scheduling_worker_assignment_packet(
                    project_scheduling_assignment_packet_command(
                        task_id=created["task_id"],
                        key=f"scheduling-outcome-{verdict}-{created['task_id']}-assignment",
                    ),
                    created["task_id"],
                    worker_id=worker_id,
                    grant_ids=[grant_id],
                )
                if verdict == "rejected":
                    self.commercial.resolve_project_scheduling_worker_assignment(
                        project_scheduling_assignment_resolution_command(
                            assignment_id=assignment_id,
                            verdict="reject",
                            key=f"scheduling-outcome-{verdict}-reject",
                            requester_id=worker_id,
                        ),
                        assignment_id,
                        verdict="reject",
                        worker_id=worker_id,
                    )
                with self.assertRaises(PermissionError):
                    self.commercial.record_project_scheduling_task_outcome(
                        project_scheduling_task_outcome_command(
                            project_id=created["project_id"],
                            task_id=created["task_id"],
                            key=f"scheduling-outcome-{verdict}-record",
                            requester_id=worker_id,
                        ),
                        created["task_id"],
                        summary="Attempted to complete without accepted assignment evidence.",
                        internal_result_ref=f"artifact://local/scheduling-outcome/{verdict}",
                    )

    def test_scheduling_task_outcome_replay_projection_comparison_remains_clean(self):
        created = self.accepted_assigned_priority_created_task("scheduling-outcome-replay")
        self.commercial.record_project_scheduling_task_outcome(
            project_scheduling_task_outcome_command(
                project_id=created["project_id"],
                task_id=created["task_id"],
                key="scheduling-outcome-replay-record",
                requester_id=created["worker_id"],
            ),
            created["task_id"],
            summary="Recorded replay-clean internal scheduling outcome.",
            internal_result_ref="artifact://local/scheduling-outcome-replay/internal",
            result={"maintenance_status": "resolved"},
        )
        self.commercial.derive_project_status_rollup(
            project_status_rollup_command(project_id=created["project_id"], key="scheduling-outcome-replay-rollup"),
            created["project_id"],
        )
        comparison = self.commercial.compare_project_replay_to_projection(
            project_replay_comparison_command(project_id=created["project_id"], key="scheduling-outcome-replay-compare"),
            created["project_id"],
        )

        self.assertTrue(comparison.matches)
        self.assertEqual(comparison.mismatches, [])
        replay = self.store.replay_critical_state()
        self.assertTrue(replay.project_replay_projection_comparisons[comparison.comparison_id]["matches"])

    def test_post_ship_negative_high_load_and_no_revenue_close_without_followup(self):
        cases = [
            ("negative", "The shipped artifact does not solve the support need.", "negative", 0, Decimal("0"), "kill"),
            ("high-load", "The shipped artifact needs too much manual operation.", "positive", 75, Decimal("0"), "kill"),
            ("no-revenue", "Customer accepted the artifact but no paid conversion happened.", "positive", 5, Decimal("0"), "pause"),
        ]
        for key, summary, sentiment, load_minutes, revenue_amount, expected_verdict in cases:
            with self.subTest(expected_verdict=expected_verdict):
                shipped = self.active_project_with_shipped_artifact(f"close-{key}")
                self.record_post_ship_evidence(
                    f"close-{key}",
                    shipped,
                    summary=summary,
                    sentiment=sentiment,
                    action_required=False,
                    revenue_amount=revenue_amount,
                    revenue_status="reconciled",
                    load_minutes=load_minutes,
                )
                rollup = self.commercial.derive_project_status_rollup(
                    project_status_rollup_command(project_id=shipped["project_id"], key=f"close-{key}-rollup"),
                    shipped["project_id"],
                )
                close_packet = self.commercial.create_project_close_decision(
                    project_close_decision_command(project_id=shipped["project_id"], key=f"close-{key}-packet"),
                    shipped["project_id"],
                    rollup_id=rollup.rollup_id,
                )
                resolution = self.commercial.resolve_project_close_decision(
                    project_close_resolution_command(
                        packet_id=close_packet.packet_id,
                        verdict=expected_verdict,
                        key=f"close-{key}-resolution",
                    ),
                    close_packet.packet_id,
                    verdict=expected_verdict,
                    operator_id="operator",
                    notes="Apply close-loop recommendation without external side effects.",
                )
                comparison = self.commercial.compare_project_replay_to_projection(
                    project_replay_comparison_command(project_id=shipped["project_id"], key=f"close-{key}-compare"),
                    shipped["project_id"],
                )

                self.assertEqual(rollup.close_recommendation, expected_verdict)
                self.assertEqual(close_packet.recommendation, expected_verdict)
                self.assertIsNone(resolution["followup_task_id"])
                self.assertEqual(resolution["project_status"], "killed" if expected_verdict == "kill" else "paused")
                self.assertTrue(comparison.matches)
                replay = self.store.replay_critical_state()
                self.assertEqual(replay.projects[shipped["project_id"]]["status"], resolution["project_status"])
                self.assertTrue(replay.project_replay_projection_comparisons[comparison.comparison_id]["matches"])

    def test_research_request_and_bundle_are_replayable_kernel_state(self):
        request = self.request()
        self.engine.create_request(request_command("research-create"), request)
        plan = self.plan(request.request_id)
        self.engine.create_source_plan(source_plan_command(request_id=request.request_id, key="source-plan-create"), plan)
        self.engine.start_collection(request_command("research-collect"), request.request_id)
        self.engine.start_synthesis(request_command("research-synthesize"), request.request_id)
        bundle = self.bundle_for_plan(request.request_id, plan.source_plan_id)
        bundle_id = self.engine.commit_evidence_bundle(
            evidence_bundle_command(request_id=request.request_id, key="evidence-commit"),
            bundle,
        )

        self.assertEqual(bundle_id, bundle.bundle_id)
        with self.store.connect() as conn:
            request_row = conn.execute(
                "SELECT profile, status, max_cost_usd FROM research_requests WHERE request_id=?",
                (request.request_id,),
            ).fetchone()
            bundle_row = conn.execute(
                "SELECT quality_gate_result, confidence FROM evidence_bundles WHERE bundle_id=?",
                (bundle.bundle_id,),
            ).fetchone()
            gate_row = conn.execute(
                "SELECT result, profile FROM quality_gate_events WHERE bundle_id=?",
                (bundle.bundle_id,),
            ).fetchone()
            events = [
                row["event_type"]
                for row in conn.execute("SELECT event_type FROM events ORDER BY event_seq").fetchall()
            ]

        self.assertEqual(request_row["profile"], "commercial")
        self.assertEqual(request_row["status"], "completed")
        self.assertEqual(request_row["max_cost_usd"], "2.50")
        self.assertEqual(bundle_row["quality_gate_result"], "pass")
        self.assertEqual(bundle_row["confidence"], 0.74)
        self.assertEqual(gate_row["result"], "pass")
        self.assertEqual(gate_row["profile"], "commercial")
        self.assertEqual(
            events,
            [
                "research_request_created",
                "source_plan_created",
                "research_request_transitioned",
                "research_request_transitioned",
                "quality_gate_evaluated",
                "evidence_bundle_committed",
            ],
        )

        replay = self.store.replay_critical_state()
        self.assertEqual(replay.research_requests[request.request_id]["status"], "completed")
        self.assertEqual(replay.source_plans[plan.source_plan_id]["request_id"], request.request_id)
        self.assertEqual(replay.evidence_bundles[bundle.bundle_id]["quality_gate_result"], "pass")
        self.assertEqual(next(iter(replay.quality_gate_events.values()))["result"], "pass")
        self.assertEqual(replay.evidence_bundles[bundle.bundle_id]["claims"][0]["source_ids"], [
            bundle.sources[0].source_id,
            bundle.sources[1].source_id,
        ])

    def test_source_plan_grants_and_acquisition_boundaries_are_kernel_authority(self):
        request = self.request()
        self.engine.create_request(request_command("research-create-boundary"), request)
        plan = self.plan(request.request_id)
        self.engine.create_source_plan(source_plan_command(request_id=request.request_id, key="source-plan-boundary"), plan)

        grant_ids = self.engine.issue_retrieval_grants(
            lambda grant, idx: retrieval_grant_command(grant_id=grant.grant_id, key=f"retrieval-grant-{idx}"),
            plan,
            expires_at="9999-12-31T23:59:59Z",
        )
        self.assertEqual(len(grant_ids), 1)

        blocked = SourceAcquisitionCheck(
            request_id=request.request_id,
            source_plan_id=plan.source_plan_id,
            source_ref="internal://operator/customer-call-1",
            access_method="operator_provided",
            data_class="internal",
            source_type="internal_record",
            result="allowed",
            reason="operator notes require explicit retrieval grant",
        )
        with self.assertRaises(PermissionError):
            self.engine.record_source_acquisition_check(
                source_acquisition_command(source_plan_id=plan.source_plan_id, key="source-check-blocked"),
                blocked,
            )

        allowed = SourceAcquisitionCheck(
            request_id=request.request_id,
            source_plan_id=plan.source_plan_id,
            source_ref="internal://operator/customer-call-1",
            access_method="operator_provided",
            data_class="internal",
            source_type="internal_record",
            result="allowed",
            reason="explicit retrieval grant covers operator-provided note metadata",
            grant_id=grant_ids[0],
        )
        check_id = self.engine.record_source_acquisition_check(
            source_acquisition_command(source_plan_id=plan.source_plan_id, key="source-check-allowed"),
            allowed,
        )

        with self.store.connect() as conn:
            grant_row = conn.execute("SELECT capability_type, used_count FROM capability_grants").fetchone()
            check_row = conn.execute("SELECT result, grant_id FROM source_acquisition_checks WHERE check_id=?", (check_id,)).fetchone()

        self.assertEqual(grant_row["capability_type"], "file")
        self.assertEqual(grant_row["used_count"], 0)
        self.assertEqual(check_row["result"], "allowed")
        self.assertEqual(check_row["grant_id"], grant_ids[0])
        replay = self.store.replay_critical_state()
        self.assertIn(check_id, replay.source_acquisition_checks)

    def test_project_pulled_commercial_inputs_synthesize_evidence_bundle_and_packet_lineage(self):
        request = self.request()
        self.engine.create_request(request_command("research-create-synthesis"), request)
        plan = self.plan(request.request_id)
        self.engine.create_source_plan(source_plan_command(request_id=request.request_id, key="source-plan-synthesis"), plan)
        self.engine.start_collection(request_command("research-collect-synthesis"), request.request_id)
        self.engine.start_synthesis(request_command("research-synthesize-synthesis"), request.request_id)

        bundle = self.engine.synthesize_project_commercial_evidence_bundle(
            evidence_bundle_command(request_id=request.request_id, key="evidence-synthesis"),
            request.request_id,
            plan.source_plan_id,
            [
                ProjectResearchInput(
                    url_or_ref="https://example.com/pricing",
                    text="Official pricing lists a $99 per month package for comparable teams.",
                    source_type="official",
                    source_date="2026-05-01",
                    access_method="public_web",
                    data_class="public",
                    retrieved_at="2026-05-02T08:00:00Z",
                    relevance=0.9,
                    reliability=0.95,
                    license_or_tos_notes="metadata-only cache",
                ),
                ProjectResearchInput(
                    url_or_ref="internal://operator/customer-call-1",
                    text=(
                        "Buyer customer notes show willingness-to-pay for local-first agent operations. "
                        "Validation can run as a one-week pilot with low operator load of two hours."
                    ),
                    source_type="primary_data",
                    source_date="2026-04-29",
                    access_method="operator_provided",
                    data_class="internal",
                    retrieved_at="2026-05-02T08:01:00Z",
                    relevance=0.88,
                    reliability=0.82,
                    artifact_ref="kernel:artifact_refs/customer-call-1",
                ),
            ],
        )

        packet = self.commercial.create_decision_packet(
            commercial_decision_packet_command(evidence_bundle_id=bundle.bundle_id, key="commercial-synthesis-packet"),
            bundle.bundle_id,
        )

        self.assertEqual(bundle.quality_gate_result, "pass")
        self.assertEqual(packet.recommendation, "pursue")
        self.assertEqual(packet.gate_packet["quality_gate_result"], "pass")
        self.assertIn(bundle.claims[0].claim_id, packet.evidence_used)
        self.assertEqual(bundle.sources[1].artifact_ref, "kernel:artifact_refs/customer-call-1")
        self.assertIn("kernel:artifact_refs/customer-call-1", [bundle.sources[1].artifact_ref])

        with self.store.connect() as conn:
            row = conn.execute(
                "SELECT sources_json, claims_json FROM evidence_bundles WHERE bundle_id=?",
                (bundle.bundle_id,),
            ).fetchone()
            decision_row = conn.execute(
                "SELECT evidence_refs_json, risk_flags_json FROM decisions WHERE decision_id=?",
                (packet.decision_id,),
            ).fetchone()

        sources = json.loads(row["sources_json"])
        claims = json.loads(row["claims_json"])
        decision_refs = json.loads(decision_row["evidence_refs_json"])
        self.assertEqual(sources[1]["artifact_ref"], "kernel:artifact_refs/customer-call-1")
        self.assertEqual(claims[0]["source_ids"], [sources[0]["source_id"]])
        self.assertIn("https://example.com/pricing", decision_refs)
        self.assertNotIn("quality_gate_failed", json.loads(decision_row["risk_flags_json"]))

    def test_failed_synthesized_commercial_quality_gate_feeds_insufficient_evidence_packet(self):
        request = self.request()
        self.engine.create_request(request_command("research-create-failed-synthesis"), request)
        plan = self.plan(request.request_id)
        self.engine.create_source_plan(source_plan_command(request_id=request.request_id, key="source-plan-failed-synthesis"), plan)
        self.engine.start_collection(request_command("research-collect-failed-synthesis"), request.request_id)
        self.engine.start_synthesis(request_command("research-synthesize-failed-synthesis"), request.request_id)

        bundle = self.engine.synthesize_project_commercial_evidence_bundle(
            evidence_bundle_command(request_id=request.request_id, key="evidence-failed-synthesis"),
            request.request_id,
            plan.source_plan_id,
            [
                {
                    "url_or_ref": "internal://operator/idea-only",
                    "text": "Maybe build this later.",
                    "source_type": "internal_record",
                    "source_date": "2026-05-01",
                    "access_method": "operator_provided",
                    "data_class": "internal",
                }
            ],
        )
        packet = self.commercial.create_decision_packet(
            commercial_decision_packet_command(evidence_bundle_id=bundle.bundle_id, key="commercial-failed-synthesis-packet"),
            bundle.bundle_id,
        )

        self.assertEqual(bundle.quality_gate_result, "fail")
        self.assertEqual(packet.recommendation, "insufficient_evidence")
        self.assertEqual(packet.gate_packet["quality_gate_result"], "fail")
        self.assertIn("Customer/problem evidence is missing.", packet.gate_packet["unsupported_claims"])
        self.assertIn("quality_gate_failed", packet.risk_flags)
        self.assertIn("unsupported_claims", packet.risk_flags)

        with self.store.connect() as conn:
            gate_row = conn.execute(
                "SELECT result, checks_json FROM quality_gate_events WHERE bundle_id=?",
                (bundle.bundle_id,),
            ).fetchone()
            packet_row = conn.execute(
                "SELECT recommendation, risk_flags_json FROM commercial_decision_packets WHERE packet_id=?",
                (packet.packet_id,),
            ).fetchone()
            recommendation_row = conn.execute(
                """
                SELECT recommendation_authority, recommendation, evidence_bundle_id,
                       quality_gate_context_json, risk_flags_json,
                       operator_gate_defaults_json, degraded
                FROM commercial_decision_recommendations
                WHERE packet_id=?
                """,
                (packet.packet_id,),
            ).fetchone()

        checks = json.loads(gate_row["checks_json"])
        self.assertEqual(gate_row["result"], "fail")
        self.assertIn("minimum_sources", {check["name"] for check in checks})
        self.assertEqual(packet_row["recommendation"], "insufficient_evidence")
        self.assertIn("quality_gate_failed", json.loads(packet_row["risk_flags_json"]))
        self.assertEqual(recommendation_row["recommendation_authority"], "single_agent")
        self.assertEqual(recommendation_row["recommendation"], "insufficient_evidence")
        self.assertEqual(recommendation_row["evidence_bundle_id"], bundle.bundle_id)
        self.assertEqual(json.loads(recommendation_row["quality_gate_context_json"])["result"], "fail")
        self.assertIn("quality_gate_failed", json.loads(recommendation_row["risk_flags_json"]))
        self.assertEqual(json.loads(recommendation_row["operator_gate_defaults_json"])["default_on_timeout"], "pause")
        self.assertEqual(recommendation_row["degraded"], 1)

    def test_commercial_workflow_creates_replayable_opportunity_project_decision_packet(self):
        request = self.request()
        self.engine.create_request(request_command("research-create-commercial-packet"), request)
        plan = self.plan(request.request_id)
        self.engine.create_source_plan(source_plan_command(request_id=request.request_id, key="source-plan-commercial-packet"), plan)
        self.engine.start_collection(request_command("research-collect-commercial-packet"), request.request_id)
        self.engine.start_synthesis(request_command("research-synthesize-commercial-packet"), request.request_id)
        bundle = self.bundle_for_plan(request.request_id, plan.source_plan_id)
        self.engine.commit_evidence_bundle(
            evidence_bundle_command(request_id=request.request_id, key="evidence-commercial-packet"),
            bundle,
        )

        packet = self.commercial.create_decision_packet(
            commercial_decision_packet_command(evidence_bundle_id=bundle.bundle_id, key="commercial-packet-create"),
            bundle.bundle_id,
            project_name="Local Agent Ops Package",
            revenue_mechanism="software",
        )

        self.assertEqual(packet.request_id, request.request_id)
        self.assertEqual(packet.evidence_bundle_id, bundle.bundle_id)
        self.assertTrue(packet.decision_id)
        self.assertEqual(packet.decision_target, request.decision_target)
        self.assertEqual(packet.required_authority, "operator_gate")
        self.assertEqual(packet.recommendation, "pursue")
        self.assertEqual(packet.default_on_timeout, "pause")
        self.assertEqual(packet.opportunity["status"], "gated")
        self.assertEqual(packet.project["status"], "proposed")
        self.assertEqual(packet.gate_packet["side_effects_authorized"], [])
        self.assertIn(bundle.claims[0].claim_id, packet.evidence_used)

        with self.store.connect() as conn:
            row = conn.execute(
                """
                SELECT decision_id, recommendation, required_authority, status, project_json, gate_packet_json
                FROM commercial_decision_packets
                WHERE packet_id=?
                """,
                (packet.packet_id,),
            ).fetchone()
            decision_row = conn.execute(
                """
                SELECT decision_type, required_authority, status, recommendation, default_on_timeout
                FROM decisions
                WHERE decision_id=?
                """,
                (packet.decision_id,),
            ).fetchone()
            events = [
                event["event_type"]
                for event in conn.execute("SELECT event_type FROM events ORDER BY event_seq").fetchall()
            ]

        self.assertEqual(row["decision_id"], packet.decision_id)
        self.assertEqual(decision_row["decision_type"], "project_approval")
        self.assertEqual(decision_row["required_authority"], "operator_gate")
        self.assertEqual(decision_row["status"], "gated")
        self.assertEqual(decision_row["recommendation"], "pursue")
        self.assertEqual(decision_row["default_on_timeout"], "pause")
        self.assertEqual(row["recommendation"], "pursue")
        self.assertEqual(row["required_authority"], "operator_gate")
        self.assertEqual(row["status"], "gated")
        self.assertIn("decision_recorded", events)
        self.assertIn("commercial_decision_packet_created", events)
        replay = self.store.replay_critical_state()
        self.assertEqual(replay.decisions[packet.decision_id]["decision_type"], "project_approval")
        self.assertEqual(replay.commercial_decision_packets[packet.packet_id]["recommendation"], "pursue")
        self.assertEqual(
            replay.commercial_decision_packets[packet.packet_id]["gate_packet"]["default_on_timeout"],
            "pause",
        )

    def test_degraded_commercial_bundle_produces_insufficient_evidence_packet(self):
        request = self.request()
        self.engine.create_request(request_command("research-create-degraded-commercial-packet"), request)
        plan = self.plan(request.request_id)
        self.engine.create_source_plan(source_plan_command(request_id=request.request_id, key="source-plan-degraded-commercial-packet"), plan)
        self.engine.start_collection(request_command("research-collect-degraded-commercial-packet"), request.request_id)
        self.engine.start_synthesis(request_command("research-synthesize-degraded-commercial-packet"), request.request_id)
        bundle = self.bundle_for_plan(request.request_id, plan.source_plan_id)
        degraded = EvidenceBundle(
            request_id=bundle.request_id,
            source_plan_id=bundle.source_plan_id,
            sources=bundle.sources,
            claims=bundle.claims,
            contradictions=bundle.contradictions,
            unsupported_claims=["Pricing sensitivity is unknown.", "Conversion rate is unknown."],
            freshness_summary=bundle.freshness_summary,
            confidence=bundle.confidence,
            uncertainty=bundle.uncertainty,
            counter_thesis=bundle.counter_thesis,
            quality_gate_result="degraded",
            data_classes=bundle.data_classes,
            retention_policy=bundle.retention_policy,
        )
        self.engine.commit_evidence_bundle(
            evidence_bundle_command(request_id=request.request_id, key="evidence-degraded-commercial-packet"),
            degraded,
        )

        packet = self.commercial.create_decision_packet(
            commercial_decision_packet_command(evidence_bundle_id=degraded.bundle_id, key="degraded-commercial-packet-create"),
            degraded.bundle_id,
        )

        self.assertEqual(packet.recommendation, "insufficient_evidence")
        self.assertIn("quality_gate_degraded", packet.risk_flags)
        self.assertIn("unsupported_claims", packet.risk_flags)
        with self.store.connect() as conn:
            recommendation_row = conn.execute(
                """
                SELECT recommendation_authority, quality_gate_context_json,
                       evidence_refs_json, operator_gate_defaults_json, degraded
                FROM commercial_decision_recommendations
                WHERE packet_id=?
                """,
                (packet.packet_id,),
            ).fetchone()
        self.assertEqual(recommendation_row["recommendation_authority"], "council")
        self.assertEqual(json.loads(recommendation_row["quality_gate_context_json"])["result"], "degraded")
        self.assertIn(f"kernel:evidence_bundles/{degraded.bundle_id}", json.loads(recommendation_row["evidence_refs_json"]))
        self.assertEqual(json.loads(recommendation_row["operator_gate_defaults_json"])["required_authority"], "operator_gate")
        self.assertEqual(recommendation_row["degraded"], 1)

    def test_high_uncertainty_commercial_packet_routes_to_council_recommendation_record(self):
        request = self.request()
        self.engine.create_request(request_command("research-create-high-uncertainty-packet"), request)
        plan = self.plan(request.request_id)
        self.engine.create_source_plan(source_plan_command(request_id=request.request_id, key="source-plan-high-uncertainty-packet"), plan)
        self.engine.start_collection(request_command("research-collect-high-uncertainty-packet"), request.request_id)
        self.engine.start_synthesis(request_command("research-synthesize-high-uncertainty-packet"), request.request_id)
        bundle = self.bundle_for_plan(request.request_id, plan.source_plan_id)
        uncertain = EvidenceBundle(
            request_id=bundle.request_id,
            source_plan_id=bundle.source_plan_id,
            sources=bundle.sources,
            claims=bundle.claims,
            contradictions=[],
            unsupported_claims=[],
            freshness_summary=bundle.freshness_summary,
            confidence=0.66,
            uncertainty="Buyer segment breadth is materially uncertain despite enough source coverage.",
            counter_thesis="Early demand may be custom consulting pull rather than product demand.",
            quality_gate_result="pass",
            data_classes=bundle.data_classes,
            retention_policy=bundle.retention_policy,
        )
        self.engine.commit_evidence_bundle(
            evidence_bundle_command(request_id=request.request_id, key="evidence-high-uncertainty-packet"),
            uncertain,
        )

        packet = self.commercial.create_decision_packet(
            commercial_decision_packet_command(evidence_bundle_id=uncertain.bundle_id, key="high-uncertainty-commercial-packet"),
            uncertain.bundle_id,
        )

        self.assertEqual(packet.recommendation, "pause")
        with self.store.connect() as conn:
            recommendation_row = conn.execute(
                """
                SELECT recommendation_authority, recommendation, confidence,
                       decisive_uncertainty, evidence_used_json,
                       quality_gate_context_json, operator_gate_defaults_json,
                       model_routes_used_json, degraded
                FROM commercial_decision_recommendations
                WHERE packet_id=?
                """,
                (packet.packet_id,),
            ).fetchone()

        self.assertEqual(recommendation_row["recommendation_authority"], "council")
        self.assertEqual(recommendation_row["recommendation"], "pause")
        self.assertAlmostEqual(recommendation_row["confidence"], 0.66)
        self.assertEqual(
            recommendation_row["decisive_uncertainty"],
            "Buyer segment breadth is materially uncertain despite enough source coverage.",
        )
        self.assertEqual(json.loads(recommendation_row["evidence_used_json"]), packet.evidence_used)
        self.assertEqual(json.loads(recommendation_row["quality_gate_context_json"])["result"], "pass")
        self.assertEqual(json.loads(recommendation_row["operator_gate_defaults_json"])["default_on_timeout"], "pause")
        self.assertEqual(json.loads(recommendation_row["operator_gate_defaults_json"])["side_effects_authorized"], [])
        self.assertEqual(json.loads(recommendation_row["model_routes_used_json"]), [])
        self.assertEqual(recommendation_row["degraded"], 0)
        replay = self.store.replay_critical_state()
        record = next(iter(replay.commercial_decision_recommendations.values()))
        self.assertEqual(record["packet_id"], packet.packet_id)
        self.assertEqual(record["recommendation_authority"], "council")

    def test_g1_approval_creates_replayable_project_task_and_outcome_loop(self):
        request = self.request()
        self.engine.create_request(request_command("research-create-project-loop"), request)
        plan = self.plan(request.request_id)
        self.engine.create_source_plan(source_plan_command(request_id=request.request_id, key="source-plan-project-loop"), plan)
        self.engine.start_collection(request_command("research-collect-project-loop"), request.request_id)
        self.engine.start_synthesis(request_command("research-synthesize-project-loop"), request.request_id)
        bundle = self.bundle_for_plan(request.request_id, plan.source_plan_id)
        self.engine.commit_evidence_bundle(
            evidence_bundle_command(request_id=request.request_id, key="evidence-project-loop"),
            bundle,
        )
        packet = self.commercial.create_decision_packet(
            commercial_decision_packet_command(evidence_bundle_id=bundle.bundle_id, key="commercial-project-loop"),
            bundle.bundle_id,
            project_name="Local Agent Ops Package",
        )

        kickoff = self.commercial.approve_g1_validation_project(
            g1_project_approval_command(packet_id=packet.packet_id, key="g1-project-loop"),
            packet.packet_id,
            notes="approve bounded zero-spend validation",
        )
        with self.assertRaises(PermissionError):
            self.store.transition_project_task(
                project_task_command(project_id=kickoff["project_id"], key="project-loop-task-running-without-assignment"),
                kickoff["task_id"],
                "running",
                "running requires an accepted worker assignment",
            )
        grant = CapabilityGrant(
            task_id=kickoff["task_id"],
            subject_type="agent",
            subject_id="validation-worker-1",
            capability_type="file",
            actions=["read", "write"],
            resource={"kind": "project_workspace"},
            scope={"project_id": kickoff["project_id"]},
            conditions={"external_side_effects": "blocked"},
            expires_at="2999-01-01T00:00:00Z",
            policy_version=KERNEL_POLICY_VERSION,
            max_uses=2,
        )
        grant_id = self.store.issue_capability_grant(
            project_task_command(project_id=kickoff["project_id"], key="project-loop-assignment-grant"),
            grant,
        )
        assignment = ProjectTaskAssignment(
            task_id=kickoff["task_id"],
            project_id=kickoff["project_id"],
            worker_type="agent",
            worker_id="validation-worker-1",
            grant_ids=[grant_id],
            accepted_capabilities=[
                {"capability_type": "file", "actions": ["read", "write"], "scope": "project_workspace"}
            ],
            notes="bounded validation worker accepted the task",
        )
        assignment_id = self.store.assign_project_task(
            project_task_command(project_id=kickoff["project_id"], key="project-loop-task-assignment"),
            assignment,
        )

        outcome = ProjectOutcome(
            project_id=kickoff["project_id"],
            task_id=kickoff["task_id"],
            phase_name="Validate",
            outcome_type="feedback",
            summary="Operator reviewed the validation artifact and accepted the first bounded loop.",
            artifact_refs=["artifact://local/project-loop/validation-note"],
            metrics={"validation_result": "accepted", "buyer_conversations": 1},
            feedback={"operator_rating": 0.8, "next_recommendation": "build_small_artifact"},
            revenue_impact={"amount": 0, "currency": "USD", "period": "one_time"},
            operator_load_actual="15 minutes",
            status="accepted",
        )
        outcome_id = self.commercial.record_project_outcome(
            project_outcome_command(project_id=kickoff["project_id"], key="project-loop-outcome"),
            outcome,
        )
        validation_artifact = ProjectArtifactReceipt(
            project_id=kickoff["project_id"],
            task_id=kickoff["task_id"],
            artifact_ref="artifact://local/project-loop/validation-note",
            artifact_kind="validation_artifact",
            summary="Validation note was recorded as a governed project artifact.",
            data_class="internal",
            delivery_channel="local_workspace",
            status="accepted",
        )
        validation_artifact_id = self.commercial.record_project_artifact_receipt(
            project_artifact_receipt_command(project_id=kickoff["project_id"], key="project-loop-validation-artifact"),
            validation_artifact,
        )

        side_effect_grant = CapabilityGrant(
            task_id=kickoff["task_id"],
            subject_type="adapter",
            subject_id="side_effect_broker",
            capability_type="side_effect",
            actions=["prepare"],
            resource={"kind": "publish", "artifact_ref": "artifact://local/project-loop/shipped-demo"},
            scope={"project_id": kickoff["project_id"]},
            conditions={"operator_approved": True},
            expires_at="2999-01-01T00:00:00Z",
            policy_version=KERNEL_POLICY_VERSION,
            max_uses=1,
        )
        side_effect_grant_id = self.store.issue_capability_grant(
            project_task_command(project_id=kickoff["project_id"], key="project-loop-side-effect-grant"),
            side_effect_grant,
        )
        side_effect_intent = SideEffectIntent(
            task_id=kickoff["task_id"],
            side_effect_type="publish",
            target={"channel": "operator_review_link"},
            payload_hash=payload_hash({"artifact_ref": "artifact://local/project-loop/shipped-demo"}),
            required_authority="operator_gate",
            grant_id=side_effect_grant_id,
            timeout_policy="ask_operator",
        )
        side_effect_intent_id = self.store.prepare_side_effect(
            project_task_command(
                project_id=kickoff["project_id"],
                key="project-loop-side-effect-intent",
                requested_by="operator",
                requested_authority="operator_gate",
            ),
            side_effect_intent,
        )
        side_effect_receipt = SideEffectReceipt(
            intent_id=side_effect_intent_id,
            receipt_type="success",
            receipt_hash=payload_hash({"published": True}),
            details={"artifact_ref": "artifact://local/project-loop/shipped-demo", "visible_to": "operator"},
        )
        side_effect_receipt_id = self.store.record_side_effect_receipt(
            project_task_command(project_id=kickoff["project_id"], key="project-loop-side-effect-receipt"),
            side_effect_receipt,
        )
        shipped_artifact = ProjectArtifactReceipt(
            project_id=kickoff["project_id"],
            task_id=kickoff["task_id"],
            artifact_ref="artifact://local/project-loop/shipped-demo",
            artifact_kind="shipped_artifact",
            summary="The validation demo was shipped to the operator review channel.",
            data_class="internal",
            delivery_channel="operator_review_link",
            side_effect_intent_id=side_effect_intent_id,
            side_effect_receipt_id=side_effect_receipt_id,
            customer_visible=True,
            status="accepted",
        )
        shipped_artifact_id = self.commercial.record_project_artifact_receipt(
            project_artifact_receipt_command(project_id=kickoff["project_id"], key="project-loop-shipped-artifact"),
            shipped_artifact,
        )
        feedback = ProjectCustomerFeedback(
            project_id=kickoff["project_id"],
            task_id=kickoff["task_id"],
            artifact_receipt_id=shipped_artifact_id,
            source_type="customer",
            customer_ref="operator-as-first-customer",
            summary="The first reviewer accepted the shipped artifact and asked for one scoped build follow-up.",
            sentiment="positive",
            evidence_refs=[f"kernel:project_artifact_receipts/{shipped_artifact_id}"],
            action_required=True,
            status="needs_followup",
        )
        feedback_id = self.commercial.record_project_customer_feedback(
            project_feedback_command(project_id=kickoff["project_id"], key="project-loop-feedback"),
            feedback,
        )
        revenue = ProjectRevenueAttribution(
            project_id=kickoff["project_id"],
            task_id=kickoff["task_id"],
            outcome_id=outcome_id,
            amount_usd=Decimal("0"),
            source="operator_reported",
            attribution_period="2026-05",
            confidence=0.35,
            status="needs_reconciliation",
        )
        revenue_id = self.commercial.record_project_revenue_attribution(
            project_revenue_attribution_command(project_id=kickoff["project_id"], key="project-loop-revenue"),
            revenue,
        )
        operator_load = ProjectOperatorLoadRecord(
            project_id=kickoff["project_id"],
            task_id=kickoff["task_id"],
            outcome_id=outcome_id,
            minutes=15,
            load_type="gate_review",
            source="operator_reported",
            notes="G1 approval and validation artifact review",
        )
        load_id = self.commercial.record_project_operator_load(
            project_operator_load_command(project_id=kickoff["project_id"], key="project-loop-operator-load"),
            operator_load,
        )
        rollup = self.commercial.derive_project_status_rollup(
            project_status_rollup_command(project_id=kickoff["project_id"], key="project-loop-rollup"),
            kickoff["project_id"],
        )
        close_packet = self.commercial.create_project_close_decision(
            project_close_decision_command(project_id=kickoff["project_id"], key="project-loop-close-decision"),
            kickoff["project_id"],
            rollup_id=rollup.rollup_id,
        )
        close_resolution = self.commercial.resolve_project_close_decision(
            project_close_resolution_command(
                packet_id=close_packet.packet_id,
                verdict="continue",
                key="project-loop-close-resolution",
            ),
            close_packet.packet_id,
            verdict="continue",
            operator_id="operator",
            notes="Continue with the scoped build follow-up requested in feedback.",
            confidence=0.85,
        )
        build_task_id = close_resolution["followup_task_id"]
        build_grant = CapabilityGrant(
            task_id=build_task_id,
            subject_type="agent",
            subject_id="build-worker-1",
            capability_type="file",
            actions=["read", "write"],
            resource={"kind": "project_workspace"},
            scope={"project_id": kickoff["project_id"]},
            conditions={"external_side_effects": "blocked"},
            expires_at="2999-01-01T00:00:00Z",
            policy_version=KERNEL_POLICY_VERSION,
            max_uses=2,
        )
        build_grant_id = self.store.issue_capability_grant(
            project_task_command(project_id=kickoff["project_id"], key="project-loop-build-grant"),
            build_grant,
        )
        build_assignment = ProjectTaskAssignment(
            task_id=build_task_id,
            project_id=kickoff["project_id"],
            worker_type="agent",
            worker_id="build-worker-1",
            grant_ids=[build_grant_id],
            accepted_capabilities=[
                {"capability_type": "file", "actions": ["read", "write"], "scope": "project_workspace"}
            ],
            notes="bounded build worker accepted the feedback follow-up",
        )
        build_assignment_id = self.store.assign_project_task(
            project_task_command(project_id=kickoff["project_id"], key="project-loop-build-assignment"),
            build_assignment,
        )
        build_delivery = self.commercial.record_project_followup_delivery(
            project_followup_delivery_command(
                project_id=kickoff["project_id"],
                task_id=build_task_id,
                key="project-loop-build-delivery",
            ),
            build_task_id,
            artifact_ref="artifact://local/project-loop/scoped-build",
            summary="Scoped build follow-up produced a governed local artifact for operator shipping review.",
            metrics={"changes_completed": 1},
            operator_load_actual="10 minutes",
            next_recommendation="ship_to_customer_review",
        )
        post_build_rollup = self.commercial.derive_project_status_rollup(
            project_status_rollup_command(project_id=kickoff["project_id"], key="project-loop-post-build-rollup"),
            kickoff["project_id"],
        )
        post_build_close_packet = self.commercial.create_project_close_decision(
            project_close_decision_command(project_id=kickoff["project_id"], key="project-loop-post-build-close"),
            kickoff["project_id"],
            rollup_id=post_build_rollup.rollup_id,
        )
        ship_task_id = build_delivery["ship_task_id"]
        ship_grant = CapabilityGrant(
            task_id=ship_task_id,
            subject_type="adapter",
            subject_id="side_effect_broker",
            capability_type="side_effect",
            actions=["prepare"],
            resource={"kind": "publish", "artifact_ref": "artifact://local/project-loop/scoped-build"},
            scope={"project_id": kickoff["project_id"]},
            conditions={"operator_approved": True},
            expires_at="2999-01-01T00:00:00Z",
            policy_version=KERNEL_POLICY_VERSION,
            max_uses=1,
        )
        ship_grant_id = self.store.issue_capability_grant(
            project_task_command(project_id=kickoff["project_id"], key="project-loop-ship-grant"),
            ship_grant,
        )
        ship_assignment = ProjectTaskAssignment(
            task_id=ship_task_id,
            project_id=kickoff["project_id"],
            worker_type="agent",
            worker_id="ship-worker-1",
            grant_ids=[ship_grant_id],
            accepted_capabilities=[
                {"capability_type": "side_effect", "actions": ["prepare"], "scope": "project_delivery"}
            ],
            notes="operator-gated ship worker accepted the delivery follow-up",
        )
        ship_assignment_id = self.store.assign_project_task(
            project_task_command(project_id=kickoff["project_id"], key="project-loop-ship-assignment"),
            ship_assignment,
        )
        with self.assertRaises(PermissionError):
            self.commercial.record_project_followup_delivery(
                project_followup_delivery_command(
                    project_id=kickoff["project_id"],
                    task_id=ship_task_id,
                    key="project-loop-ship-delivery-without-authority",
                ),
                ship_task_id,
                artifact_ref="artifact://local/project-loop/scoped-build",
                summary="Attempted customer-visible delivery without side-effect authority.",
                delivery_channel="customer_channel",
                customer_visible=True,
            )
        followup_side_effect_intent = SideEffectIntent(
            task_id=ship_task_id,
            side_effect_type="publish",
            target={"channel": "operator_review_link"},
            payload_hash=payload_hash({"artifact_ref": "artifact://local/project-loop/scoped-build"}),
            required_authority="operator_gate",
            grant_id=ship_grant_id,
            timeout_policy="ask_operator",
        )
        followup_side_effect_intent_id = self.store.prepare_side_effect(
            project_task_command(
                project_id=kickoff["project_id"],
                key="project-loop-followup-side-effect-intent",
                requested_by="operator",
                requested_authority="operator_gate",
            ),
            followup_side_effect_intent,
        )
        followup_side_effect_receipt = SideEffectReceipt(
            intent_id=followup_side_effect_intent_id,
            receipt_type="success",
            receipt_hash=payload_hash({"published": True, "artifact_ref": "artifact://local/project-loop/scoped-build"}),
            details={"artifact_ref": "artifact://local/project-loop/scoped-build", "visible_to": "operator"},
        )
        followup_side_effect_receipt_id = self.store.record_side_effect_receipt(
            project_task_command(project_id=kickoff["project_id"], key="project-loop-followup-side-effect-receipt"),
            followup_side_effect_receipt,
        )
        ship_delivery = self.commercial.record_project_followup_delivery(
            project_followup_delivery_command(
                project_id=kickoff["project_id"],
                task_id=ship_task_id,
                key="project-loop-ship-delivery",
                requested_by="operator",
                requested_authority="operator_gate",
            ),
            ship_task_id,
            artifact_ref="artifact://local/project-loop/scoped-build",
            summary="Operator-gated ship follow-up delivered the scoped build artifact to the review channel.",
            delivery_channel="operator_review_link",
            side_effect_intent_id=followup_side_effect_intent_id,
            side_effect_receipt_id=followup_side_effect_receipt_id,
            metrics={"delivery_completed": 1},
            operator_load_actual="5 minutes",
            next_recommendation="collect_feedback",
        )
        post_ship = self.commercial.record_project_post_ship_evidence(
            project_post_ship_evidence_command(
                project_id=kickoff["project_id"],
                artifact_receipt_id=ship_delivery["artifact_receipt_id"],
                key="project-loop-post-ship-evidence",
            ),
            ship_delivery["artifact_receipt_id"],
            feedback=ProjectCustomerFeedback(
                project_id=kickoff["project_id"],
                task_id=ship_task_id,
                source_type="customer",
                customer_ref="operator-as-first-customer",
                summary="The follow-on shipped artifact solved the scoped request and can close this loop.",
                sentiment="positive",
                action_required=False,
                operator_review_required=False,
                status="accepted",
            ),
            revenue=ProjectRevenueAttribution(
                project_id=kickoff["project_id"],
                task_id=ship_task_id,
                amount_usd=Decimal("250"),
                source="operator_reported",
                attribution_period="2026-05",
                confidence=0.9,
                evidence_refs=[f"kernel:side_effect_receipts/{followup_side_effect_receipt_id}"],
                status="reconciled",
            ),
            operator_load=ProjectOperatorLoadRecord(
                project_id=kickoff["project_id"],
                task_id=ship_task_id,
                minutes=5,
                load_type="client_sales",
                source="operator_reported",
                notes="Post-ship customer response and revenue attribution review",
            ),
        )
        with self.assertRaises(PermissionError):
            self.commercial.record_project_post_ship_evidence(
                project_post_ship_evidence_command(
                    project_id=kickoff["project_id"],
                    artifact_receipt_id=build_delivery["artifact_receipt_id"],
                    key="project-loop-post-ship-evidence-without-shipped-authority",
                ),
                build_delivery["artifact_receipt_id"],
                feedback=ProjectCustomerFeedback(
                    project_id=kickoff["project_id"],
                    source_type="customer",
                    customer_ref="customer-1",
                    summary="This cannot be attached to a non-shipped artifact.",
                    sentiment="positive",
                ),
                revenue=ProjectRevenueAttribution(
                    project_id=kickoff["project_id"],
                    amount_usd=Decimal("1"),
                    source="operator_reported",
                    attribution_period="2026-05",
                    confidence=0.8,
                    status="reconciled",
                ),
                operator_load=ProjectOperatorLoadRecord(
                    project_id=kickoff["project_id"],
                    minutes=1,
                    load_type="client_sales",
                    source="operator_reported",
                ),
            )
        post_ship_rollup = self.commercial.derive_project_status_rollup(
            project_status_rollup_command(project_id=kickoff["project_id"], key="project-loop-post-ship-rollup"),
            kickoff["project_id"],
        )
        post_ship_close_packet = self.commercial.create_project_close_decision(
            project_close_decision_command(project_id=kickoff["project_id"], key="project-loop-post-ship-close"),
            kickoff["project_id"],
            rollup_id=post_ship_rollup.rollup_id,
        )
        comparison = self.commercial.compare_project_replay_to_projection(
            project_replay_comparison_command(project_id=kickoff["project_id"], key="project-loop-replay-compare"),
            kickoff["project_id"],
        )

        with self.store.connect() as conn:
            decision_row = conn.execute(
                "SELECT status, verdict FROM decisions WHERE decision_id=?",
                (packet.decision_id,),
            ).fetchone()
            project_row = conn.execute(
                "SELECT status, decision_packet_id FROM projects WHERE project_id=?",
                (kickoff["project_id"],),
            ).fetchone()
            task_row = conn.execute(
                "SELECT status, task_type, authority_required FROM project_tasks WHERE task_id=?",
                (kickoff["task_id"],),
            ).fetchone()
            assignment_row = conn.execute(
                "SELECT status, worker_type, worker_id FROM project_task_assignments WHERE assignment_id=?",
                (assignment_id,),
            ).fetchone()
            outcome_row = conn.execute(
                "SELECT status, outcome_type FROM project_outcomes WHERE outcome_id=?",
                (outcome_id,),
            ).fetchone()
            shipped_row = conn.execute(
                "SELECT artifact_kind, customer_visible, side_effect_receipt_id FROM project_artifact_receipts WHERE receipt_id=?",
                (shipped_artifact_id,),
            ).fetchone()
            feedback_row = conn.execute(
                "SELECT source_type, sentiment, status FROM project_customer_feedback WHERE feedback_id=?",
                (feedback_id,),
            ).fetchone()
            revenue_row = conn.execute(
                "SELECT amount_usd, status, reconciliation_task_id FROM project_revenue_attributions WHERE attribution_id=?",
                (revenue_id,),
            ).fetchone()
            load_row = conn.execute(
                "SELECT minutes, load_type FROM project_operator_load WHERE load_id=?",
                (load_id,),
            ).fetchone()
            rollup_row = conn.execute(
                """
                SELECT recommended_status, close_recommendation, revenue_attributed_usd,
                       operator_load_minutes
                FROM project_status_rollups
                WHERE rollup_id=?
                """,
                (rollup.rollup_id,),
            ).fetchone()
            close_decision_row = conn.execute(
                """
                SELECT recommendation, required_authority, status
                FROM project_close_decision_packets
                WHERE packet_id=?
                """,
                (close_packet.packet_id,),
            ).fetchone()
            comparison_row = conn.execute(
                """
                SELECT matches, mismatches_json
                FROM project_replay_projection_comparisons
                WHERE comparison_id=?
                """,
                (comparison.comparison_id,),
            ).fetchone()
            followup_task_row = conn.execute(
                """
                SELECT task_id, status, task_type, phase_name, authority_required,
                       inputs_json, evidence_refs_json
                FROM project_tasks
                WHERE task_id=?
                """,
                (close_resolution["followup_task_id"],),
            ).fetchone()
            build_artifact_row = conn.execute(
                """
                SELECT artifact_kind, customer_visible, side_effect_receipt_id, status
                FROM project_artifact_receipts
                WHERE receipt_id=?
                """,
                (build_delivery["artifact_receipt_id"],),
            ).fetchone()
            build_outcome_row = conn.execute(
                "SELECT outcome_type, status FROM project_outcomes WHERE outcome_id=?",
                (build_delivery["outcome_id"],),
            ).fetchone()
            ship_task_row = conn.execute(
                """
                SELECT status, task_type, phase_name, authority_required,
                       inputs_json, evidence_refs_json
                FROM project_tasks
                WHERE task_id=?
                """,
                (build_delivery["ship_task_id"],),
            ).fetchone()
            followup_shipped_artifact_row = conn.execute(
                """
                SELECT artifact_kind, customer_visible, side_effect_receipt_id, status
                FROM project_artifact_receipts
                WHERE receipt_id=?
                """,
                (ship_delivery["artifact_receipt_id"],),
            ).fetchone()
            ship_outcome_row = conn.execute(
                "SELECT outcome_type, status FROM project_outcomes WHERE outcome_id=?",
                (ship_delivery["outcome_id"],),
            ).fetchone()
            post_build_close_row = conn.execute(
                """
                SELECT recommendation, required_authority, status
                FROM project_close_decision_packets
                WHERE packet_id=?
                """,
                (post_build_close_packet.packet_id,),
            ).fetchone()
            post_ship_close_row = conn.execute(
                """
                SELECT recommendation, required_authority, status
                FROM project_close_decision_packets
                WHERE packet_id=?
                """,
                (post_ship_close_packet.packet_id,),
            ).fetchone()
            post_ship_feedback_row = conn.execute(
                """
                SELECT artifact_receipt_id, sentiment, action_required, operator_review_required, status
                FROM project_customer_feedback
                WHERE feedback_id=?
                """,
                (post_ship["feedback_id"],),
            ).fetchone()
            post_ship_revenue_row = conn.execute(
                """
                SELECT artifact_receipt_id, amount_usd, status
                FROM project_revenue_attributions
                WHERE attribution_id=?
                """,
                (post_ship["revenue_attribution_id"],),
            ).fetchone()
            post_ship_load_row = conn.execute(
                """
                SELECT artifact_receipt_id, minutes, load_type
                FROM project_operator_load
                WHERE load_id=?
                """,
                (post_ship["operator_load_id"],),
            ).fetchone()

        self.assertEqual(decision_row["status"], "decided")
        self.assertEqual(decision_row["verdict"], "approve_validation")
        self.assertEqual(project_row["status"], "active")
        self.assertEqual(project_row["decision_packet_id"], packet.packet_id)
        self.assertEqual(task_row["task_type"], "validate")
        self.assertEqual(task_row["authority_required"], "single_agent")
        self.assertEqual(task_row["status"], "completed")
        self.assertEqual(assignment_row["status"], "accepted")
        self.assertEqual(assignment_row["worker_type"], "agent")
        self.assertEqual(assignment_row["worker_id"], "validation-worker-1")
        self.assertEqual(outcome_row["status"], "accepted")
        self.assertEqual(outcome_row["outcome_type"], "feedback")
        self.assertEqual(shipped_row["artifact_kind"], "shipped_artifact")
        self.assertEqual(shipped_row["customer_visible"], 1)
        self.assertEqual(shipped_row["side_effect_receipt_id"], side_effect_receipt_id)
        self.assertEqual(feedback_row["source_type"], "customer")
        self.assertEqual(feedback_row["sentiment"], "positive")
        self.assertEqual(feedback_row["status"], "needs_followup")
        self.assertEqual(revenue_row["amount_usd"], "0")
        self.assertEqual(revenue_row["status"], "needs_reconciliation")
        self.assertTrue(revenue_row["reconciliation_task_id"])
        self.assertEqual(load_row["minutes"], 15)
        self.assertEqual(load_row["load_type"], "gate_review")
        self.assertEqual(rollup_row["recommended_status"], "active")
        self.assertEqual(rollup_row["close_recommendation"], "continue")
        self.assertEqual(rollup_row["revenue_attributed_usd"], "0")
        self.assertEqual(rollup_row["operator_load_minutes"], 15)
        self.assertEqual(close_decision_row["recommendation"], "continue")
        self.assertEqual(close_decision_row["required_authority"], "operator_gate")
        self.assertEqual(close_decision_row["status"], "decided")
        self.assertEqual(close_resolution["project_status"], "active")
        self.assertTrue(close_resolution["followup_task_id"])
        self.assertEqual(followup_task_row["status"], "completed")
        self.assertEqual(followup_task_row["task_type"], "build")
        self.assertEqual(followup_task_row["phase_name"], "Build")
        self.assertEqual(followup_task_row["authority_required"], "single_agent")
        self.assertIn(feedback_id, followup_task_row["inputs_json"])
        self.assertIn(feedback_id, followup_task_row["evidence_refs_json"])
        self.assertEqual(build_artifact_row["artifact_kind"], "build_artifact")
        self.assertEqual(build_artifact_row["customer_visible"], 0)
        self.assertIsNone(build_artifact_row["side_effect_receipt_id"])
        self.assertEqual(build_artifact_row["status"], "accepted")
        self.assertEqual(build_outcome_row["outcome_type"], "build_artifact")
        self.assertEqual(build_outcome_row["status"], "accepted")
        self.assertEqual(ship_task_row["status"], "completed")
        self.assertEqual(ship_task_row["task_type"], "ship")
        self.assertEqual(ship_task_row["phase_name"], "Ship")
        self.assertEqual(ship_task_row["authority_required"], "operator_gate")
        self.assertIn(build_delivery["artifact_receipt_id"], ship_task_row["inputs_json"])
        self.assertIn(build_delivery["artifact_receipt_id"], ship_task_row["evidence_refs_json"])
        self.assertEqual(followup_shipped_artifact_row["artifact_kind"], "shipped_artifact")
        self.assertEqual(followup_shipped_artifact_row["customer_visible"], 1)
        self.assertEqual(followup_shipped_artifact_row["side_effect_receipt_id"], followup_side_effect_receipt_id)
        self.assertEqual(followup_shipped_artifact_row["status"], "accepted")
        self.assertEqual(ship_outcome_row["outcome_type"], "shipped_artifact")
        self.assertEqual(ship_outcome_row["status"], "accepted")
        self.assertEqual(post_build_close_row["recommendation"], "continue")
        self.assertEqual(post_build_close_row["required_authority"], "operator_gate")
        self.assertEqual(post_build_close_row["status"], "gated")
        self.assertEqual(post_ship["artifact_receipt_id"], ship_delivery["artifact_receipt_id"])
        self.assertEqual(post_ship_feedback_row["artifact_receipt_id"], ship_delivery["artifact_receipt_id"])
        self.assertEqual(post_ship_feedback_row["sentiment"], "positive")
        self.assertEqual(post_ship_feedback_row["action_required"], 0)
        self.assertEqual(post_ship_feedback_row["operator_review_required"], 0)
        self.assertEqual(post_ship_feedback_row["status"], "accepted")
        self.assertEqual(post_ship_revenue_row["artifact_receipt_id"], ship_delivery["artifact_receipt_id"])
        self.assertEqual(post_ship_revenue_row["amount_usd"], "250")
        self.assertEqual(post_ship_revenue_row["status"], "reconciled")
        self.assertEqual(post_ship_load_row["artifact_receipt_id"], ship_delivery["artifact_receipt_id"])
        self.assertEqual(post_ship_load_row["minutes"], 5)
        self.assertEqual(post_ship_load_row["load_type"], "client_sales")
        self.assertEqual(post_ship_close_row["recommendation"], "complete")
        self.assertEqual(post_ship_close_row["required_authority"], "operator_gate")
        self.assertEqual(post_ship_close_row["status"], "gated")
        self.assertEqual(comparison_row["matches"], 1)
        self.assertEqual(comparison_row["mismatches_json"], "[]")

        replay = self.store.replay_critical_state()
        self.assertEqual(replay.decisions[packet.decision_id]["verdict"], "approve_validation")
        self.assertEqual(replay.projects[kickoff["project_id"]]["status"], "active")
        self.assertEqual(replay.project_task_assignments[assignment_id]["grant_ids"], [grant_id])
        self.assertEqual(replay.project_tasks[kickoff["task_id"]]["status"], "completed")
        self.assertEqual(replay.project_outcomes[outcome_id]["feedback"]["next_recommendation"], "build_small_artifact")
        self.assertEqual(replay.project_artifact_receipts[validation_artifact_id]["status"], "accepted")
        self.assertEqual(replay.project_artifact_receipts[shipped_artifact_id]["side_effect_receipt_id"], side_effect_receipt_id)
        self.assertEqual(replay.project_customer_feedback[feedback_id]["action_required"], True)
        self.assertEqual(replay.project_revenue_attributions[revenue_id]["status"], "needs_reconciliation")
        self.assertIn(revenue_row["reconciliation_task_id"], replay.project_tasks)
        self.assertEqual(replay.project_operator_load[load_id]["minutes"], 15)
        self.assertEqual(replay.project_status_rollups[rollup.rollup_id]["close_recommendation"], "continue")
        self.assertEqual(replay.project_close_decision_packets[close_packet.packet_id]["recommendation"], "continue")
        self.assertEqual(replay.project_close_decision_packets[close_packet.packet_id]["status"], "decided")
        self.assertEqual(replay.project_close_decision_packets[close_packet.packet_id]["verdict"], "continue")
        self.assertEqual(replay.projects[kickoff["project_id"]]["last_close_decision_packet_id"], close_packet.packet_id)
        self.assertEqual(replay.project_tasks[close_resolution["followup_task_id"]]["task_type"], "build")
        self.assertEqual(
            replay.project_tasks[close_resolution["followup_task_id"]]["inputs"]["feedback_id"],
            feedback_id,
        )
        self.assertEqual(replay.project_task_assignments[build_assignment_id]["grant_ids"], [build_grant_id])
        self.assertEqual(replay.project_tasks[build_task_id]["status"], "completed")
        self.assertEqual(
            replay.project_artifact_receipts[build_delivery["artifact_receipt_id"]]["artifact_kind"],
            "build_artifact",
        )
        self.assertEqual(replay.project_outcomes[build_delivery["outcome_id"]]["outcome_type"], "build_artifact")
        self.assertEqual(replay.project_task_assignments[ship_assignment_id]["grant_ids"], [ship_grant_id])
        self.assertEqual(replay.project_tasks[build_delivery["ship_task_id"]]["task_type"], "ship")
        self.assertEqual(replay.project_tasks[build_delivery["ship_task_id"]]["authority_required"], "operator_gate")
        self.assertEqual(replay.project_tasks[ship_task_id]["status"], "completed")
        self.assertEqual(
            replay.project_artifact_receipts[ship_delivery["artifact_receipt_id"]]["side_effect_receipt_id"],
            followup_side_effect_receipt_id,
        )
        self.assertEqual(replay.project_outcomes[ship_delivery["outcome_id"]]["outcome_type"], "shipped_artifact")
        self.assertEqual(
            replay.project_customer_feedback[post_ship["feedback_id"]]["artifact_receipt_id"],
            ship_delivery["artifact_receipt_id"],
        )
        self.assertEqual(
            replay.project_revenue_attributions[post_ship["revenue_attribution_id"]]["artifact_receipt_id"],
            ship_delivery["artifact_receipt_id"],
        )
        self.assertEqual(
            replay.project_operator_load[post_ship["operator_load_id"]]["artifact_receipt_id"],
            ship_delivery["artifact_receipt_id"],
        )
        self.assertEqual(replay.project_revenue_attributions[post_ship["revenue_attribution_id"]]["amount_usd"], "250")
        self.assertEqual(replay.project_status_rollups[post_build_rollup.rollup_id]["close_recommendation"], "continue")
        self.assertEqual(replay.project_close_decision_packets[post_build_close_packet.packet_id]["status"], "gated")
        self.assertEqual(replay.project_status_rollups[post_ship_rollup.rollup_id]["close_recommendation"], "complete")
        self.assertEqual(replay.project_close_decision_packets[post_ship_close_packet.packet_id]["status"], "gated")
        self.assertTrue(replay.project_replay_projection_comparisons[comparison.comparison_id]["matches"])

    def test_bundle_rejects_unsupported_source_references(self):
        request = self.request()
        self.engine.create_request(request_command("research-create-missing-source"), request)
        plan = self.plan(request.request_id)
        self.engine.create_source_plan(source_plan_command(request_id=request.request_id, key="source-plan-missing-source"), plan)
        self.engine.start_collection(request_command("research-collect-missing-source"), request.request_id)
        self.engine.start_synthesis(request_command("research-synthesize-missing-source"), request.request_id)
        source = SourceRecord(
            url_or_ref="https://example.com/source",
            source_type="official",
            retrieved_at="2026-05-02T08:00:00Z",
            source_date="2026-05-01",
            relevance=0.9,
            reliability=0.9,
            content_hash=sha256_text("source"),
            access_method="public_web",
            data_class="public",
        )
        bad_bundle = EvidenceBundle(
            request_id=request.request_id,
            source_plan_id=plan.source_plan_id,
            sources=[source],
            claims=[
                ClaimRecord(
                    text="This claim points at a missing source.",
                    claim_type="fact",
                    source_ids=["missing-source"],
                    confidence=0.5,
                    freshness="unknown",
                    importance="medium",
                )
            ],
            contradictions=[],
            unsupported_claims=[],
            freshness_summary="unknown",
            confidence=0.5,
            uncertainty="source missing",
            counter_thesis=None,
            quality_gate_result="fail",
            data_classes=["public"],
            retention_policy="retain-30d",
        )

        with self.assertRaises(ValueError):
            self.engine.commit_evidence_bundle(
                evidence_bundle_command(request_id=request.request_id, key="bad-evidence"),
                bad_bundle,
            )

        with self.store.connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM evidence_bundles").fetchone()[0], 0)

    def test_profile_validator_rejects_commercial_willingness_to_pay_without_buyer_evidence(self):
        request = self.request()
        self.engine.create_request(request_command("research-create-profile-validator"), request)
        plan = self.plan(request.request_id)
        self.engine.create_source_plan(source_plan_command(request_id=request.request_id, key="source-plan-profile-validator"), plan)
        self.engine.start_collection(request_command("research-collect-profile-validator"), request.request_id)
        self.engine.start_synthesis(request_command("research-synthesize-profile-validator"), request.request_id)
        community = SourceRecord(
            url_or_ref="https://forum.example.com/thread",
            source_type="community",
            retrieved_at="2026-05-02T08:00:00Z",
            source_date="2026-05-01",
            relevance=0.4,
            reliability=0.3,
            content_hash=sha256_text("forum"),
            access_method="public_web",
            data_class="public",
        )
        bad_bundle = EvidenceBundle(
            request_id=request.request_id,
            source_plan_id=plan.source_plan_id,
            sources=[community],
            claims=[
                ClaimRecord(
                    text="There is willingness-to-pay for the package.",
                    claim_type="interpretation",
                    source_ids=[community.source_id],
                    confidence=0.5,
                    freshness="current",
                    importance="high",
                )
            ],
            contradictions=[],
            unsupported_claims=[],
            freshness_summary="fresh but weak",
            confidence=0.5,
            uncertainty="buyer evidence is not present",
            counter_thesis=None,
            quality_gate_result="pass",
            data_classes=["public"],
            retention_policy="retain-30d",
        )

        with self.assertRaises(ValueError):
            self.engine.commit_evidence_bundle(
                evidence_bundle_command(request_id=request.request_id, key="bad-commercial-quality"),
                bad_bundle,
            )

        with self.store.connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM quality_gate_events").fetchone()[0], 0)

    def test_legacy_projection_is_non_authoritative_compatibility_surface(self):
        request = self.request()
        self.engine.create_request(request_command("research-create-projection"), request)
        projection_data = self.root / "projection-data"
        projection_data.mkdir()
        repo_root = Path(__file__).resolve().parents[1]
        apply_schema(projection_data / "strategic_memory.db", repo_root / "schemas" / "strategic_memory.sql")
        db = DatabaseManager(str(projection_data))
        projection = self.engine.project_request_to_legacy_task(request.request_id, db)

        strategic = db.get_connection("strategic_memory")
        row = strategic.execute(
            "SELECT title, source, max_spend_usd, tags FROM research_tasks WHERE task_id=?",
            (projection.task_id,),
        ).fetchone()

        self.assertEqual(projection.request_id, request.request_id)
        self.assertEqual(row["title"], request.question)
        self.assertEqual(row["source"], "operator")
        self.assertEqual(row["max_spend_usd"], 2.5)
        self.assertIn(request.request_id, row["tags"])
        replay = self.store.replay_critical_state()
        self.assertIn(request.request_id, replay.research_requests)
        self.assertNotIn(projection.task_id, replay.research_requests)


if __name__ == "__main__":
    unittest.main()
