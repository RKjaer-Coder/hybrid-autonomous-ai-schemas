from __future__ import annotations

import tempfile
import unittest
from decimal import Decimal
from pathlib import Path

from financial_router.router import route_task
from financial_router.types import BudgetState, JWTClaims, ModelInfo, SystemPhase, TaskMetadata
from immune.types import generate_uuid_v7
from kernel import Budget, Command, KernelRuntime, KernelStore, ProviderCallRequest
from kernel.runtime import (
    bootstrap_runtime_state,
    make_session_context,
    migrate_runtime_databases,
    prepare_runtime_directories,
    require_runtime_databases,
)
from kernel.records import new_id
from skills.config import IntegrationConfig
from skills.hermes_interfaces import MockHermesRuntime


def command(command_type: str, key: str, payload: dict | None = None) -> Command:
    return Command(
        command_type=command_type,
        requested_by="operator",
        requester_id="operator",
        target_entity_type="runtime",
        idempotency_key=key,
        payload=payload or {"key": key},
    )


def future() -> str:
    return "2999-01-01T00:00:00Z"


class KernelRuntimeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.store = KernelStore(self.root / "kernel.db")
        self.runtime = KernelRuntime(self.store)
        self.budget = Budget(
            budget_id=new_id(),
            owner_type="project",
            owner_id="project-runtime",
            approved_by="operator",
            cap_usd=Decimal("5.00"),
            expires_at=future(),
        )
        self.store.create_budget(command("budget.create", "runtime-budget"), self.budget)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def request(self, endpoint: str = "https://api.example.com/v1/responses") -> ProviderCallRequest:
        task = TaskMetadata(
            task_id=generate_uuid_v7(),
            task_type="provider_call_prepare",
            required_capability="reasoning",
            quality_threshold=0.90,
            estimated_task_value_usd=100.0,
            project_id="project-runtime",
            idempotency_key="provider-call-1",
            is_operating_phase=True,
        )
        return ProviderCallRequest(
            task=task,
            available_models=[
                ModelInfo(
                    model_id="paid-frontier",
                    tier="paid",
                    commercial_use_permitted=True,
                    quality_score=0.97,
                    cost_per_1k_tokens=0.10,
                )
            ],
            budget=BudgetState(
                project_cloud_spend_cap_usd=5.00,
                project_cloud_spend_current_usd=0.0,
                system_phase=SystemPhase.OPERATING,
            ),
            jwt=JWTClaims(
                session_id="session-runtime",
                max_api_spend_usd=5.00,
                current_session_spend_usd=0.0,
            ),
            budget_id=self.budget.budget_id,
            provider_endpoint=endpoint,
            provider_payload={"input": "summarize project evidence"},
            proxy_config={
                "bind_host": "127.0.0.1",
                "bind_port": 18080,
                "audit_log_path": str(self.root / "proxy-audit.jsonl"),
                "outbound_allowlist": {
                    "domains": ["example.com"],
                    "ports": [443],
                    "schemes": ["https"],
                },
            },
            session_id=generate_uuid_v7(),
        )

    def test_prepare_provider_call_is_kernel_authoritative(self):
        prepared = self.runtime.prepare_provider_call(
            command("runtime.prepare_provider_call", "provider-call-1"),
            self.request(),
        )

        self.assertEqual(prepared.model_id, "paid-frontier")
        self.assertEqual(prepared.routing_tier, "paid_cloud")
        self.assertEqual(prepared.estimated_cost_usd, Decimal("0.2"))
        self.assertEqual(prepared.budget_reservation_id, "provider-call-1")

        with self.store.connect() as conn:
            events = [
                row["event_type"]
                for row in conn.execute("SELECT event_type FROM events ORDER BY event_seq").fetchall()
            ]
            grant_rows = conn.execute(
                "SELECT capability_type, used_count, status FROM capability_grants ORDER BY capability_type"
            ).fetchall()
            intent = conn.execute(
                "SELECT side_effect_type, status, payload_hash FROM side_effect_intents WHERE intent_id=?",
                (prepared.side_effect_intent_id,),
            ).fetchone()
            budget = conn.execute(
                "SELECT reserved_usd FROM budgets WHERE budget_id=?",
                (self.budget.budget_id,),
            ).fetchone()

        self.assertIn("spend_reserved", events)
        self.assertIn("capability_granted", events)
        self.assertIn("capability_used", events)
        self.assertIn("side_effect_intent_prepared", events)
        self.assertEqual(budget["reserved_usd"], "0.2")
        self.assertEqual(intent["side_effect_type"], "provider_call")
        self.assertEqual(intent["status"], "prepared")
        self.assertEqual(intent["payload_hash"], prepared.side_effect_payload_hash)
        used_by_type = {row["capability_type"]: (row["used_count"], row["status"]) for row in grant_rows}
        self.assertEqual(used_by_type["network"], (1, "exhausted"))
        self.assertEqual(used_by_type["model"], (1, "exhausted"))
        self.assertEqual(used_by_type["side_effect"], (1, "exhausted"))

        replay = self.store.replay_critical_state()
        self.assertIn(prepared.side_effect_intent_id, replay.side_effects)
        self.assertEqual(replay.budgets[self.budget.budget_id]["reserved_usd"], Decimal("0.2"))

    def test_proxy_allowlist_blocks_before_any_runtime_events_commit(self):
        with self.assertRaises(PermissionError):
            self.runtime.prepare_provider_call(
                command("runtime.prepare_provider_call", "blocked-host"),
                self.request("https://evil.example.net/v1/responses"),
            )

        with self.store.connect() as conn:
            events = [
                row["event_type"]
                for row in conn.execute("SELECT event_type FROM events ORDER BY event_seq").fetchall()
            ]
        self.assertEqual(events, ["budget_created"])

    def test_router_alone_remains_non_authoritative_helper(self):
        request = self.request()
        decision = route_task(
            request.task,
            request.available_models,
            request.budget,
            request.jwt,
            request_id="router-only",
        )
        self.assertEqual(decision.model_id, "paid-frontier")

        with self.store.connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM side_effect_intents").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM budget_reservations").fetchone()[0], 0)
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM capability_grants").fetchone()[0], 0)

    def test_kernel_owns_runtime_bootstrap_lane(self):
        cfg = IntegrationConfig(
            data_dir=str(self.root / "bootstrap-data"),
            skills_dir=str(self.root / "bootstrap-skills"),
            checkpoints_dir=str(self.root / "bootstrap-skills" / "checkpoints"),
            alerts_dir=str(self.root / "bootstrap-alerts"),
        )

        resolved = prepare_runtime_directories(cfg)
        self.assertTrue(Path(resolved.data_dir).is_dir())
        self.assertTrue(Path(resolved.skills_dir).is_dir())
        self.assertTrue(Path(resolved.checkpoints_dir).is_dir())
        self.assertTrue(Path(resolved.alerts_dir).is_dir())

        status = migrate_runtime_databases(resolved)
        self.assertTrue(all(status.values()))
        self.assertEqual(status, require_runtime_databases(resolved))

        ctx = make_session_context(resolved, model_name="kernel-local")
        self.assertEqual(ctx.profile_name, resolved.profile_name)
        self.assertEqual(ctx.model_name, "kernel-local")
        runtime = MockHermesRuntime(data_dir=resolved.data_dir)
        state = bootstrap_runtime_state(runtime, config=resolved, session_context=ctx)
        self.assertTrue(state.ok)
        self.assertIs(state.session_context, ctx)
        self.assertIn("immune_system", state.registered_tools)


if __name__ == "__main__":
    unittest.main()
