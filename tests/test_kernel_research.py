from __future__ import annotations

import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from kernel import (
    ClaimRecord,
    EvidenceBundle,
    KernelCommercialResearchWorkflow,
    KernelResearchEngine,
    KernelStore,
    ResearchRequest,
    SourceAcquisitionCheck,
    SourcePlan,
    SourceRecord,
)
from kernel.records import new_id, sha256_text
from kernel.research import (
    evidence_bundle_command,
    research_request_command,
    retrieval_grant_command,
    source_acquisition_command,
    source_plan_command,
)
from kernel.commercial import commercial_decision_packet_command
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
