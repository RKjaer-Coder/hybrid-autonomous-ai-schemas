import datetime
import os
import tempfile
import time
import unittest
import uuid
from unittest.mock import patch

import financial_router.router as router_module
from financial_router.router import (
    SqliteSpendReservationRegistry,
    commit_paid_reservation,
    finalize_paid_reservation,
    release_paid_reservation,
    route_task,
)
from financial_router.types import G3Path, G3Status, JWTClaims, BudgetState, ModelInfo, RoutingTier, SystemPhase, TaskMetadata


def generate_uuid_v7() -> str:
    """Generate a UUIDv7-like identifier using timestamp + randomness."""

    unix_ms = int(time.time() * 1000)
    time_high = unix_ms & ((1 << 48) - 1)
    rand_bits = uuid.uuid4().int & ((1 << 74) - 1)
    value = (time_high << 80) | (0x7 << 76) | rand_bits
    return str(uuid.UUID(int=value))


def make_task(**overrides) -> TaskMetadata:
    defaults = {
        "task_id": generate_uuid_v7(),
        "task_type": "code_generation",
        "required_capability": "code",
        "quality_threshold": 0.7,
        "estimated_task_value_usd": None,
        "project_id": generate_uuid_v7(),
        "idempotency_key": generate_uuid_v7(),
        "is_operating_phase": False,
        "is_council_tier1_preassessment": False,
    }
    defaults.update(overrides)
    return TaskMetadata(**defaults)


def make_model(**overrides) -> ModelInfo:
    defaults = {
        "model_id": f"model-{generate_uuid_v7()[:8]}",
        "tier": "local",
        "commercial_use_permitted": True,
        "quality_score": 0.8,
        "cost_per_1k_tokens": 0.0,
        "rate_limit_remaining": None,
        "quota_remaining": None,
    }
    defaults.update(overrides)
    return ModelInfo(**defaults)


def make_budget(**overrides) -> BudgetState:
    defaults = {
        "system_phase": SystemPhase.CONSTRUCTION,
        "project_cloud_spend_cap_usd": None,
        "project_cloud_spend_current_usd": 0.0,
        "project_cashflow_target_usd": None,
        "task_contribution_pct": 0.01,
        "g3_status": G3Status.NOT_REQUIRED,
        "g3_requested_at": None,
        "g3_timeout_hours": 6.0,
    }
    defaults.update(overrides)
    return BudgetState(**defaults)


def make_jwt(**overrides) -> JWTClaims:
    defaults = {
        "session_id": generate_uuid_v7(),
        "max_api_spend_usd": 0.00,
        "current_session_spend_usd": 0.0,
    }
    defaults.update(overrides)
    return JWTClaims(**defaults)


class TestCommercialUseGate(unittest.TestCase):
    def test_non_commercial_model_excluded_even_if_free(self):
        task = make_task()
        models = [make_model(tier="free_cloud", commercial_use_permitted=False, quality_score=0.95)]
        result = route_task(task, models, make_budget(), make_jwt())
        self.assertEqual(result.tier, RoutingTier.COMPUTE_STARVED)
        self.assertIn("commercial use", result.justification.lower())

    def test_non_commercial_skipped_but_commercial_alternative_used(self):
        task = make_task()
        models = [
            make_model(model_id="blocked", tier="free_cloud", commercial_use_permitted=False, quality_score=0.95),
            make_model(model_id="allowed", tier="free_cloud", commercial_use_permitted=True, quality_score=0.8),
        ]
        result = route_task(task, models, make_budget(), make_jwt())
        self.assertEqual(result.tier, RoutingTier.FREE_CLOUD)
        self.assertEqual(result.model_id, "allowed")
        self.assertIn("blocked", result.skipped_reasons.get("blocked", ""))

    def test_local_models_exempt_from_commercial_gate(self):
        task = make_task()
        models = [make_model(tier="local", commercial_use_permitted=False, quality_score=0.9)]
        result = route_task(task, models, make_budget(), make_jwt())
        self.assertEqual(result.tier, RoutingTier.LOCAL)


class TestLocalRouting(unittest.TestCase):
    def test_local_model_selected_when_meets_threshold(self):
        task = make_task(quality_threshold=0.7)
        models = [make_model(tier="local", quality_score=0.8)]
        result = route_task(task, models, make_budget(), make_jwt())
        self.assertEqual(result.tier, RoutingTier.LOCAL)
        self.assertEqual(result.estimated_cost_usd, 0.0)
        self.assertFalse(result.quality_warning)

    def test_local_model_rejected_below_threshold(self):
        task = make_task(quality_threshold=0.9)
        models = [
            make_model(tier="local", quality_score=0.5),
            make_model(tier="free_cloud", quality_score=0.95, commercial_use_permitted=True),
        ]
        result = route_task(task, models, make_budget(), make_jwt())
        self.assertEqual(result.tier, RoutingTier.FREE_CLOUD)
        self.assertIn("local", result.skipped_reasons)

    def test_best_local_model_selected_among_multiple(self):
        task = make_task(quality_threshold=0.6)
        models = [
            make_model(model_id="weak", tier="local", quality_score=0.65),
            make_model(model_id="strong", tier="local", quality_score=0.9),
        ]
        result = route_task(task, models, make_budget(), make_jwt())
        self.assertEqual(result.model_id, "strong")

    def test_no_local_model_available(self):
        task = make_task()
        models = [make_model(tier="free_cloud", commercial_use_permitted=True)]
        result = route_task(task, models, make_budget(), make_jwt())
        self.assertNotEqual(result.tier, RoutingTier.LOCAL)
        self.assertIn("local", result.skipped_reasons)


class TestFreeCloudRouting(unittest.TestCase):
    def test_free_cloud_selected_when_available(self):
        task = make_task(quality_threshold=0.7)
        models = [
            make_model(tier="local", quality_score=0.5),
            make_model(tier="free_cloud", quality_score=0.8, commercial_use_permitted=True),
        ]
        result = route_task(task, models, make_budget(), make_jwt())
        self.assertEqual(result.tier, RoutingTier.FREE_CLOUD)
        self.assertEqual(result.estimated_cost_usd, 0.0)

    def test_free_cloud_rejected_quota_exhausted(self):
        task = make_task(quality_threshold=0.7)
        models = [
            make_model(tier="free_cloud", quality_score=0.9, commercial_use_permitted=True, quota_remaining=0),
            make_model(tier="subscription", quality_score=0.8, commercial_use_permitted=True),
        ]
        result = route_task(task, models, make_budget(), make_jwt())
        self.assertEqual(result.tier, RoutingTier.SUBSCRIPTION)
        self.assertIn("free_cloud", result.skipped_reasons)

    def test_free_cloud_unlimited_quota_accepted(self):
        task = make_task(quality_threshold=0.7)
        models = [make_model(tier="free_cloud", quality_score=0.8, commercial_use_permitted=True, quota_remaining=None)]
        result = route_task(task, models, make_budget(), make_jwt())
        self.assertEqual(result.tier, RoutingTier.FREE_CLOUD)


class TestSubscriptionRouting(unittest.TestCase):
    def test_subscription_selected_when_higher_tiers_unavailable(self):
        task = make_task(quality_threshold=0.7)
        models = [make_model(tier="subscription", quality_score=0.85, commercial_use_permitted=True)]
        result = route_task(task, models, make_budget(), make_jwt())
        self.assertEqual(result.tier, RoutingTier.SUBSCRIPTION)
        self.assertEqual(result.estimated_cost_usd, 0.0)

    def test_subscription_rejected_rate_limit_exhausted(self):
        task = make_task(quality_threshold=0.7)
        models = [make_model(tier="subscription", quality_score=0.9, commercial_use_permitted=True, rate_limit_remaining=0)]
        result = route_task(task, models, make_budget(), make_jwt())
        self.assertNotEqual(result.tier, RoutingTier.SUBSCRIPTION)

    def test_subscription_unlimited_rate_limit_accepted(self):
        task = make_task(quality_threshold=0.7)
        models = [make_model(tier="subscription", quality_score=0.8, commercial_use_permitted=True, rate_limit_remaining=None)]
        result = route_task(task, models, make_budget(), make_jwt())
        self.assertEqual(result.tier, RoutingTier.SUBSCRIPTION)


class TestConstructionPhaseBlock(unittest.TestCase):
    def test_paid_model_blocked_in_construction_phase(self):
        task = make_task(quality_threshold=0.7, estimated_task_value_usd=1000.0, is_operating_phase=False)
        budget = make_budget(system_phase=SystemPhase.CONSTRUCTION, project_cloud_spend_cap_usd=500.0)
        jwt = make_jwt(max_api_spend_usd=100.0)
        models = [make_model(tier="paid", quality_score=0.95, commercial_use_permitted=True, cost_per_1k_tokens=0.01)]
        result = route_task(task, models, budget, jwt)
        self.assertNotEqual(result.tier, RoutingTier.PAID_CLOUD)
        self.assertIn("construction", result.justification.lower())

    def test_construction_phase_falls_to_default_when_only_paid_available(self):
        task = make_task(quality_threshold=0.7)
        models = [make_model(tier="paid", quality_score=0.95, commercial_use_permitted=True, cost_per_1k_tokens=0.02)]
        result = route_task(task, models, make_budget(), make_jwt())
        self.assertEqual(result.tier, RoutingTier.COMPUTE_STARVED)


class TestPaidCloudRouting(unittest.TestCase):
    def _operating_budget(self, **overrides):
        defaults = {
            "system_phase": SystemPhase.OPERATING,
            "project_cloud_spend_cap_usd": 100.0,
            "project_cloud_spend_current_usd": 10.0,
            "project_cashflow_target_usd": 50000.0,
        }
        defaults.update(overrides)
        return make_budget(**defaults)

    def _operating_jwt(self, **overrides):
        defaults = {"max_api_spend_usd": 50.0, "current_session_spend_usd": 0.0}
        defaults.update(overrides)
        return make_jwt(**defaults)

    def test_paid_cloud_path_a_within_budget(self):
        task = make_task(quality_threshold=0.7, estimated_task_value_usd=100.0, is_operating_phase=True)
        models = [make_model(tier="paid", quality_score=0.95, commercial_use_permitted=True, cost_per_1k_tokens=0.005)]
        result = route_task(task, models, self._operating_budget(), self._operating_jwt())
        self.assertEqual(result.tier, RoutingTier.PAID_CLOUD)
        self.assertEqual(result.g3_path, G3Path.WITHIN_BUDGET)
        self.assertFalse(result.requires_operator_approval)

    def test_paid_cloud_path_b_no_approved_budget(self):
        task = make_task(quality_threshold=0.7, estimated_task_value_usd=100.0, is_operating_phase=True)
        budget = self._operating_budget(project_cloud_spend_cap_usd=None)
        models = [make_model(tier="paid", quality_score=0.95, commercial_use_permitted=True, cost_per_1k_tokens=0.005)]
        result = route_task(task, models, budget, self._operating_jwt())
        self.assertEqual(result.tier, RoutingTier.PAID_CLOUD)
        self.assertEqual(result.g3_path, G3Path.OUTSIDE_BUDGET)
        self.assertTrue(result.requires_operator_approval)

    def test_paid_cloud_rejected_low_roi(self):
        task = make_task(quality_threshold=0.7, estimated_task_value_usd=5.0, is_operating_phase=True)
        models = [make_model(tier="paid", quality_score=0.95, commercial_use_permitted=True, cost_per_1k_tokens=0.50)]
        result = route_task(task, models, self._operating_budget(), self._operating_jwt())
        self.assertNotEqual(result.tier, RoutingTier.PAID_CLOUD)
        self.assertIn("roi", result.justification.lower())

    def test_paid_cloud_speculative_task_conservative_roi(self):
        task = make_task(quality_threshold=0.7, estimated_task_value_usd=None, is_operating_phase=True)
        budget = self._operating_budget(project_cashflow_target_usd=100000.0, task_contribution_pct=0.01)
        models = [make_model(tier="paid", quality_score=0.95, commercial_use_permitted=True, cost_per_1k_tokens=0.01)]
        result = route_task(task, models, budget, self._operating_jwt())
        self.assertEqual(result.tier, RoutingTier.PAID_CLOUD)

    def test_paid_cloud_speculative_no_cashflow_target_rejected(self):
        task = make_task(quality_threshold=0.7, estimated_task_value_usd=None, is_operating_phase=True)
        budget = self._operating_budget(project_cashflow_target_usd=None)
        models = [make_model(tier="paid", quality_score=0.95, commercial_use_permitted=True, cost_per_1k_tokens=0.01)]
        result = route_task(task, models, budget, self._operating_jwt())
        self.assertNotEqual(result.tier, RoutingTier.PAID_CLOUD)

    def test_paid_cloud_negative_model_cost_rejected(self):
        task = make_task(quality_threshold=0.7, estimated_task_value_usd=1000.0, is_operating_phase=True)
        models = [make_model(tier="paid", quality_score=0.95, commercial_use_permitted=True, cost_per_1k_tokens=-0.01)]
        result = route_task(task, models, self._operating_budget(), self._operating_jwt())
        self.assertNotEqual(result.tier, RoutingTier.PAID_CLOUD)
        self.assertIn("invalid negative model cost", result.skipped_reasons.get("paid_cloud", ""))

    def test_paid_cloud_budget_headroom_exhausted(self):
        task = make_task(quality_threshold=0.7, estimated_task_value_usd=10000.0, is_operating_phase=True)
        budget = self._operating_budget(project_cloud_spend_cap_usd=100.0, project_cloud_spend_current_usd=99.99)
        models = [make_model(tier="paid", quality_score=0.95, commercial_use_permitted=True, cost_per_1k_tokens=0.50)]
        result = route_task(task, models, budget, self._operating_jwt())
        self.assertNotEqual(result.tier, RoutingTier.PAID_CLOUD)
        self.assertIn("headroom", result.justification.lower())


class TestJWTSpendCap(unittest.TestCase):
    def test_jwt_cap_blocks_paid_route(self):
        task = make_task(quality_threshold=0.7, estimated_task_value_usd=10000.0, is_operating_phase=True)
        budget = make_budget(system_phase=SystemPhase.OPERATING, project_cloud_spend_cap_usd=1000.0)
        jwt = make_jwt(max_api_spend_usd=1.00, current_session_spend_usd=0.99)
        models = [make_model(tier="paid", quality_score=0.95, commercial_use_permitted=True, cost_per_1k_tokens=0.50)]
        result = route_task(task, models, budget, jwt)
        self.assertNotEqual(result.tier, RoutingTier.PAID_CLOUD)
        self.assertIn("jwt", result.justification.lower())

    def test_jwt_zero_cap_blocks_all_paid_construction(self):
        task = make_task(quality_threshold=0.7, is_operating_phase=False)
        jwt = make_jwt(max_api_spend_usd=0.00)
        models = [make_model(tier="paid", quality_score=0.95, commercial_use_permitted=True, cost_per_1k_tokens=0.01)]
        result = route_task(task, models, make_budget(), jwt)
        self.assertNotEqual(result.tier, RoutingTier.PAID_CLOUD)

    def test_jwt_cap_does_not_affect_free_tiers(self):
        task = make_task(quality_threshold=0.7)
        jwt = make_jwt(max_api_spend_usd=0.00)
        models = [make_model(tier="local", quality_score=0.8)]
        result = route_task(task, models, make_budget(), jwt)
        self.assertEqual(result.tier, RoutingTier.LOCAL)

    def test_jwt_cap_allows_exact_boundary(self):
        task = make_task(quality_threshold=0.7, estimated_task_value_usd=1000.0, is_operating_phase=True)
        budget = make_budget(system_phase=SystemPhase.OPERATING, project_cloud_spend_cap_usd=1000.0)
        jwt = make_jwt(max_api_spend_usd=1.00, current_session_spend_usd=0.99)
        models = [make_model(tier="paid", quality_score=0.95, commercial_use_permitted=True, cost_per_1k_tokens=0.005)]
        result = route_task(task, models, budget, jwt)
        self.assertEqual(result.tier, RoutingTier.PAID_CLOUD)


class TestG3Timeout(unittest.TestCase):
    def test_g3_timeout_falls_back_to_free_tiers(self):
        now = datetime.datetime(2026, 4, 7, 12, 0, 0, tzinfo=datetime.timezone.utc)
        six_hours_ago = now - datetime.timedelta(hours=7)
        task = make_task(quality_threshold=0.7, is_operating_phase=True)
        budget = make_budget(system_phase=SystemPhase.OPERATING, g3_status=G3Status.PENDING, g3_requested_at=six_hours_ago)
        models = [
            make_model(tier="paid", quality_score=0.95, commercial_use_permitted=True, cost_per_1k_tokens=0.5),
            make_model(tier="subscription", quality_score=0.75, commercial_use_permitted=True),
        ]
        result = route_task(task, models, budget, make_jwt(max_api_spend_usd=100.0), current_time=now)
        self.assertEqual(result.tier, RoutingTier.SUBSCRIPTION)
        self.assertIn("expired", result.justification.lower())

    def test_g3_timeout_no_fallback_compute_starved(self):
        now = datetime.datetime(2026, 4, 7, 12, 0, 0, tzinfo=datetime.timezone.utc)
        six_hours_ago = now - datetime.timedelta(hours=7)
        task = make_task(quality_threshold=0.7, is_operating_phase=True)
        budget = make_budget(system_phase=SystemPhase.OPERATING, g3_status=G3Status.PENDING, g3_requested_at=six_hours_ago)
        models = [make_model(tier="paid", quality_score=0.95, commercial_use_permitted=True, cost_per_1k_tokens=0.5)]
        result = route_task(task, models, budget, make_jwt(max_api_spend_usd=100.0), current_time=now)
        self.assertEqual(result.tier, RoutingTier.COMPUTE_STARVED)
        self.assertTrue(result.compute_starved)

    def test_g3_not_yet_timed_out(self):
        now = datetime.datetime(2026, 4, 7, 12, 0, 0, tzinfo=datetime.timezone.utc)
        one_hour_ago = now - datetime.timedelta(hours=1)
        task = make_task(quality_threshold=0.7, estimated_task_value_usd=10000.0, is_operating_phase=True)
        budget = make_budget(
            system_phase=SystemPhase.OPERATING,
            project_cloud_spend_cap_usd=None,
            g3_status=G3Status.PENDING,
            g3_requested_at=one_hour_ago,
        )
        models = [make_model(tier="paid", quality_score=0.95, commercial_use_permitted=True, cost_per_1k_tokens=0.005)]
        result = route_task(task, models, budget, make_jwt(max_api_spend_usd=100.0), current_time=now)
        self.assertEqual(result.tier, RoutingTier.PAID_CLOUD)
        self.assertTrue(result.requires_operator_approval)


class TestDefaultFallback(unittest.TestCase):
    def test_default_fallback_with_subscription_below_threshold(self):
        task = make_task(quality_threshold=0.95)
        models = [
            make_model(tier="local", quality_score=0.5),
            make_model(model_id="fallback-sub", tier="subscription", quality_score=0.6, commercial_use_permitted=True),
        ]
        result = route_task(task, models, make_budget(), make_jwt())
        self.assertEqual(result.tier, RoutingTier.DEFAULT_FALLBACK)
        self.assertTrue(result.quality_warning)
        self.assertEqual(result.model_id, "fallback-sub")

    def test_default_fallback_justification_explains_all_skips(self):
        task = make_task(quality_threshold=0.95)
        models = [
            make_model(tier="local", quality_score=0.5),
            make_model(tier="free_cloud", quality_score=0.4, commercial_use_permitted=True),
            make_model(model_id="sub", tier="subscription", quality_score=0.6, commercial_use_permitted=True),
        ]
        result = route_task(task, models, make_budget(), make_jwt())
        self.assertEqual(result.tier, RoutingTier.DEFAULT_FALLBACK)
        justification = result.justification.lower()
        self.assertIn("local", justification)
        self.assertIn("free", justification)
        self.assertIn("quality", justification)


class TestComputeStarved(unittest.TestCase):
    def test_empty_model_list(self):
        result = route_task(make_task(), [], make_budget(), make_jwt())
        self.assertEqual(result.tier, RoutingTier.COMPUTE_STARVED)
        self.assertIsNone(result.model_id)
        self.assertTrue(result.compute_starved)

    def test_all_models_non_commercial(self):
        task = make_task(quality_threshold=0.9)
        models = [
            make_model(tier="local", quality_score=0.3),
            make_model(tier="free_cloud", commercial_use_permitted=False, quality_score=0.95),
            make_model(tier="subscription", commercial_use_permitted=False, quality_score=0.95),
        ]
        result = route_task(task, models, make_budget(), make_jwt())
        self.assertEqual(result.tier, RoutingTier.COMPUTE_STARVED)

    def test_all_quotas_exhausted(self):
        task = make_task(quality_threshold=0.7)
        models = [
            make_model(tier="free_cloud", quality_score=0.9, commercial_use_permitted=True, quota_remaining=0),
            make_model(tier="subscription", quality_score=0.9, commercial_use_permitted=True, rate_limit_remaining=0),
        ]
        result = route_task(task, models, make_budget(), make_jwt())
        self.assertEqual(result.tier, RoutingTier.COMPUTE_STARVED)


class TestCouncilTier1Exception(unittest.TestCase):
    def test_tier1_preassessment_never_routes_paid(self):
        task = make_task(is_council_tier1_preassessment=True, quality_threshold=0.7, is_operating_phase=True)
        budget = make_budget(system_phase=SystemPhase.OPERATING, project_cloud_spend_cap_usd=1000.0)
        jwt = make_jwt(max_api_spend_usd=100.0)
        models = [
            make_model(tier="subscription", quality_score=0.8, commercial_use_permitted=True),
            make_model(tier="paid", quality_score=0.99, commercial_use_permitted=True, cost_per_1k_tokens=0.50),
        ]
        result = route_task(task, models, budget, jwt)
        self.assertEqual(result.tier, RoutingTier.SUBSCRIPTION)
        self.assertEqual(result.estimated_cost_usd, 0.0)

    def test_tier1_preassessment_uses_local_if_available(self):
        task = make_task(is_council_tier1_preassessment=True, quality_threshold=0.5)
        models = [
            make_model(tier="local", quality_score=0.7),
            make_model(tier="paid", quality_score=0.99, commercial_use_permitted=True, cost_per_1k_tokens=1.0),
        ]
        result = route_task(task, models, make_budget(), make_jwt())
        self.assertEqual(result.tier, RoutingTier.LOCAL)


class TestWaterfallPriority(unittest.TestCase):
    def test_local_preferred_over_higher_quality_cloud(self):
        task = make_task(quality_threshold=0.7)
        models = [
            make_model(model_id="local-good", tier="local", quality_score=0.75),
            make_model(model_id="cloud-great", tier="free_cloud", quality_score=0.99, commercial_use_permitted=True),
        ]
        result = route_task(task, models, make_budget(), make_jwt())
        self.assertEqual(result.tier, RoutingTier.LOCAL)
        self.assertEqual(result.model_id, "local-good")

    def test_free_cloud_preferred_over_subscription(self):
        task = make_task(quality_threshold=0.7)
        models = [
            make_model(model_id="free", tier="free_cloud", quality_score=0.8, commercial_use_permitted=True),
            make_model(model_id="sub", tier="subscription", quality_score=0.9, commercial_use_permitted=True),
        ]
        result = route_task(task, models, make_budget(), make_jwt())
        self.assertEqual(result.tier, RoutingTier.FREE_CLOUD)

    def test_subscription_preferred_over_paid(self):
        task = make_task(quality_threshold=0.7, is_operating_phase=True, estimated_task_value_usd=10000.0)
        budget = make_budget(system_phase=SystemPhase.OPERATING, project_cloud_spend_cap_usd=500.0)
        jwt = make_jwt(max_api_spend_usd=100.0)
        models = [
            make_model(model_id="sub", tier="subscription", quality_score=0.8, commercial_use_permitted=True),
            make_model(model_id="paid", tier="paid", quality_score=0.95, commercial_use_permitted=True, cost_per_1k_tokens=0.01),
        ]
        result = route_task(task, models, budget, jwt)
        self.assertEqual(result.tier, RoutingTier.SUBSCRIPTION)


class TestJustificationStrings(unittest.TestCase):
    def test_justification_not_empty(self):
        result = route_task(make_task(), [make_model()], make_budget(), make_jwt())
        self.assertTrue(len(result.justification) > 0)

    def test_default_justification_explains_all_skips(self):
        task = make_task(quality_threshold=0.99)
        models = [
            make_model(tier="local", quality_score=0.3),
            make_model(model_id="sub", tier="subscription", quality_score=0.5, commercial_use_permitted=True),
        ]
        result = route_task(task, models, make_budget(), make_jwt())
        self.assertEqual(result.tier, RoutingTier.DEFAULT_FALLBACK)
        justification = result.justification.lower()
        self.assertIn("local", justification)

    def test_skipped_reasons_dict_populated(self):
        task = make_task(quality_threshold=0.99)
        models = [make_model(model_id="sub", tier="subscription", quality_score=0.5, commercial_use_permitted=True)]
        result = route_task(task, models, make_budget(), make_jwt())
        self.assertIn("local", result.skipped_reasons)
        self.assertIn("free_cloud", result.skipped_reasons)


class TestEdgeCases(unittest.TestCase):
    def test_quality_threshold_exact_boundary(self):
        task = make_task(quality_threshold=0.8)
        models = [make_model(tier="local", quality_score=0.8)]
        result = route_task(task, models, make_budget(), make_jwt())
        self.assertEqual(result.tier, RoutingTier.LOCAL)

    def test_quality_threshold_just_below(self):
        task = make_task(quality_threshold=0.8)
        models = [
            make_model(tier="local", quality_score=0.7999),
            make_model(model_id="sub", tier="subscription", quality_score=0.85, commercial_use_permitted=True),
        ]
        result = route_task(task, models, make_budget(), make_jwt())
        self.assertNotEqual(result.tier, RoutingTier.LOCAL)

    def test_quality_threshold_above_one_is_normalized(self):
        task = make_task(quality_threshold=2.0)
        models = [make_model(tier="local", quality_score=1.0)]
        result = route_task(task, models, make_budget(), make_jwt())
        self.assertEqual(result.tier, RoutingTier.LOCAL)
        self.assertIn("validation", result.skipped_reasons)

    def test_zero_cost_paid_model(self):
        task = make_task(quality_threshold=0.7, estimated_task_value_usd=100.0, is_operating_phase=True)
        budget = make_budget(system_phase=SystemPhase.OPERATING, project_cloud_spend_cap_usd=100.0)
        jwt = make_jwt(max_api_spend_usd=100.0)
        models = [make_model(tier="paid", quality_score=0.9, commercial_use_permitted=True, cost_per_1k_tokens=0.0)]
        result = route_task(task, models, budget, jwt)
        self.assertEqual(result.tier, RoutingTier.PAID_CLOUD)

    def test_roi_exactly_five(self):
        task = make_task(quality_threshold=0.7, estimated_task_value_usd=0.06, is_operating_phase=True)
        budget = make_budget(system_phase=SystemPhase.OPERATING, project_cloud_spend_cap_usd=100.0)
        jwt = make_jwt(max_api_spend_usd=100.0)
        models = [make_model(tier="paid", quality_score=0.9, commercial_use_permitted=True, cost_per_1k_tokens=0.005)]
        result = route_task(task, models, budget, jwt)
        self.assertEqual(result.tier, RoutingTier.PAID_CLOUD)

    def test_roi_just_below_five(self):
        task = make_task(quality_threshold=0.7, estimated_task_value_usd=0.059, is_operating_phase=True)
        budget = make_budget(system_phase=SystemPhase.OPERATING, project_cloud_spend_cap_usd=100.0)
        jwt = make_jwt(max_api_spend_usd=100.0)
        models = [make_model(tier="paid", quality_score=0.9, commercial_use_permitted=True, cost_per_1k_tokens=0.005)]
        result = route_task(task, models, budget, jwt)
        self.assertNotEqual(result.tier, RoutingTier.PAID_CLOUD)

    def test_multiple_paid_models_selects_cheapest_with_roi(self):
        task = make_task(quality_threshold=0.7, estimated_task_value_usd=1000.0, is_operating_phase=True)
        budget = make_budget(system_phase=SystemPhase.OPERATING, project_cloud_spend_cap_usd=100.0)
        jwt = make_jwt(max_api_spend_usd=100.0)
        models = [
            make_model(model_id="expensive", tier="paid", quality_score=0.95, commercial_use_permitted=True, cost_per_1k_tokens=1.0),
            make_model(model_id="cheap", tier="paid", quality_score=0.85, commercial_use_permitted=True, cost_per_1k_tokens=0.01),
        ]
        result = route_task(task, models, budget, jwt)
        self.assertEqual(result.tier, RoutingTier.PAID_CLOUD)
        self.assertEqual(result.model_id, "expensive")

    def test_g3_timeout_boundary_exactly_6h(self):
        now = datetime.datetime(2026, 4, 7, 12, 0, 0, tzinfo=datetime.timezone.utc)
        exactly_6h_ago = now - datetime.timedelta(hours=6)
        budget = make_budget(system_phase=SystemPhase.OPERATING, g3_status=G3Status.PENDING, g3_requested_at=exactly_6h_ago)
        task = make_task(quality_threshold=0.7, is_operating_phase=True, estimated_task_value_usd=10000.0)
        models = [
            make_model(tier="paid", quality_score=0.95, commercial_use_permitted=True, cost_per_1k_tokens=0.5),
            make_model(model_id="sub", tier="subscription", quality_score=0.8, commercial_use_permitted=True),
        ]
        result = route_task(task, models, budget, make_jwt(max_api_spend_usd=100.0), current_time=now)
        self.assertEqual(result.tier, RoutingTier.SUBSCRIPTION)


class TestReservationRegistry(unittest.TestCase):
    def test_released_reservations_do_not_consume_cap(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "reservations.db")
            registry = SqliteSpendReservationRegistry(db_path)
            self.assertTrue(registry.reserve("session-1", "request-1", current_spend=0.0, cap=1.0, amount=0.5))
            registry.release("session-1", "request-1")
            self.assertTrue(registry.reserve("session-1", "request-2", current_spend=0.0, cap=1.0, amount=0.8))

    def test_reservation_allows_exact_cap_boundary(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "reservations.db")
            registry = SqliteSpendReservationRegistry(db_path)
            self.assertTrue(registry.reserve("session-1", "request-1", current_spend=0.99, cap=1.0, amount=0.01))

    def test_reservation_rejects_negative_amount(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "reservations.db")
            registry = SqliteSpendReservationRegistry(db_path)
            self.assertFalse(registry.reserve("session-1", "request-1", current_spend=0.0, cap=1.0, amount=-0.01))

    def test_reservation_rejects_negative_cap_or_current_spend(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "reservations.db")
            registry = SqliteSpendReservationRegistry(db_path)
            self.assertFalse(registry.reserve("session-1", "request-1", current_spend=-0.1, cap=1.0, amount=0.01))
            self.assertFalse(registry.reserve("session-1", "request-2", current_spend=0.0, cap=-1.0, amount=0.01))

    def test_released_request_id_cannot_be_reused(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "reservations.db")
            registry = SqliteSpendReservationRegistry(db_path)
            self.assertTrue(registry.reserve("session-1", "request-1", current_spend=0.0, cap=1.0, amount=0.5))
            registry.release("session-1", "request-1")
            self.assertFalse(registry.reserve("session-1", "request-1", current_spend=0.0, cap=1.0, amount=0.5))

    def test_finalize_helpers_return_transition_status(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "reservations.db")
            registry = SqliteSpendReservationRegistry(db_path)
            self.assertTrue(registry.reserve("session-1", "request-1", current_spend=0.0, cap=1.0, amount=0.5))
            self.assertTrue(finalize_paid_reservation("session-1", "request-1", success=False, registry=registry))
            self.assertFalse(release_paid_reservation("session-1", "request-1", registry=registry))
            self.assertFalse(commit_paid_reservation("session-1", "request-1", registry=registry))

    def test_committed_rows_are_not_ttl_deleted(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = os.path.join(tmp, "reservations.db")
            registry = SqliteSpendReservationRegistry(db_path)
            self.assertTrue(registry.reserve("session-1", "request-1", current_spend=0.0, cap=1.0, amount=0.5))
            self.assertTrue(registry.commit("session-1", "request-1"))
            with registry._connect() as conn:
                conn.execute(
                    "UPDATE spend_reservations SET expires_at = ? WHERE request_id = ?",
                    ("2000-01-01T00:00:00+00:00", "request-1"),
                )

            # Running reserve triggers TTL cleanup. request-1 must remain for idempotency safety.
            self.assertTrue(registry.reserve("session-1", "request-2", current_spend=0.0, cap=2.0, amount=0.2))
            self.assertTrue(registry.reserve("session-1", "request-1", current_spend=0.0, cap=2.0, amount=0.5))


class TestDefaultRegistryBootstrap(unittest.TestCase):
    def test_default_registry_falls_back_when_state_dir_creation_fails(self):
        with patch("financial_router.router.Path.mkdir", side_effect=PermissionError("readonly")):
            registry = router_module._build_default_registry()
        self.assertIsInstance(registry, SqliteSpendReservationRegistry)
        self.assertIn("hybrid_router_reservations_fallback.db", registry.db_path)


if __name__ == "__main__":
    unittest.main()
