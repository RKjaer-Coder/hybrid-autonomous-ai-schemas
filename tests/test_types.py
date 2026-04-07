import unittest

from financial_router.types import (
    BudgetState,
    G3Path,
    JWTClaims,
    RoutingDecision,
    RoutingTier,
    SystemPhase,
)
from tests.test_router import make_model, make_task


class TestTypeValidation(unittest.TestCase):
    def test_task_metadata_frozen(self):
        task = make_task()
        with self.assertRaises(AttributeError):
            task.task_id = "new-id"

    def test_model_info_frozen(self):
        model = make_model()
        with self.assertRaises(AttributeError):
            model.quality_score = 0.5

    def test_routing_decision_frozen(self):
        decision = RoutingDecision(
            tier=RoutingTier.LOCAL,
            model_id="test",
            g3_path=G3Path.NOT_APPLICABLE,
            estimated_cost_usd=0.0,
            quality_warning=False,
            justification="test",
            skipped_reasons={},
            requires_operator_approval=False,
            compute_starved=False,
        )
        with self.assertRaises(AttributeError):
            decision.tier = RoutingTier.PAID_CLOUD

    def test_budget_defaults_match_spec(self):
        budget = BudgetState(system_phase=SystemPhase.CONSTRUCTION)
        self.assertIsNone(budget.project_cloud_spend_cap_usd)
        self.assertEqual(budget.project_cloud_spend_current_usd, 0.0)

    def test_jwt_defaults_match_spec(self):
        jwt = JWTClaims(session_id="test")
        self.assertEqual(jwt.max_api_spend_usd, 0.00)
        self.assertEqual(jwt.current_session_spend_usd, 0.0)


if __name__ == "__main__":
    unittest.main()
