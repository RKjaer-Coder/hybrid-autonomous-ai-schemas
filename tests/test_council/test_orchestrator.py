import unittest

from council.context_budget import build_context_packet
from council.orchestrator import MockDispatcher, MockMixtureDispatcher, run_tier1_deliberation, run_tier2_deliberation
from council.types import DecisionType, Recommendation


class TestOrchestrator(unittest.TestCase):
    def test_pipeline_returns_verdict(self):
        dispatcher = MockDispatcher()
        ctx = build_context_packet(DecisionType.OPPORTUNITY_SCREEN, "id", "ctx")
        verdict = run_tier1_deliberation(ctx, dispatcher)
        self.assertEqual(verdict.tier_used, 1)

    def test_batch_a_parallel_called_once(self):
        dispatcher = MockDispatcher()
        ctx = build_context_packet(DecisionType.OPPORTUNITY_SCREEN, "id", "ctx")
        run_tier1_deliberation(ctx, dispatcher)
        self.assertEqual(dispatcher.parallel_called, 1)

    def test_batch_b_da_called_once(self):
        dispatcher = MockDispatcher()
        ctx = build_context_packet(DecisionType.OPPORTUNITY_SCREEN, "id", "ctx")
        run_tier1_deliberation(ctx, dispatcher)
        self.assertEqual(dispatcher.sequential_called, 1)

    def test_da_input_contains_batch(self):
        dispatcher = MockDispatcher()
        ctx = build_context_packet(DecisionType.OPPORTUNITY_SCREEN, "id", "ctx")
        run_tier1_deliberation(ctx, dispatcher)
        self.assertIn("[STRATEGIST]", dispatcher.last_sequential_prompt)

    def test_synthesis_receives_all_roles(self):
        dispatcher = MockDispatcher()
        ctx = build_context_packet(DecisionType.OPPORTUNITY_SCREEN, "id", "ctx")
        run_tier1_deliberation(ctx, dispatcher)
        for marker in ["strategist", "critic", "realist", "devils_advocate"]:
            self.assertIn(marker, dispatcher.last_synthesis_prompt)

    def test_auto_escalation_for_low_confidence(self):
        dispatcher = MockDispatcher(low_confidence=True)
        ctx = build_context_packet(DecisionType.OPPORTUNITY_SCREEN, "id", "ctx")
        verdict = run_tier1_deliberation(ctx, dispatcher)
        self.assertEqual(verdict.recommendation, Recommendation.ESCALATE)
        self.assertFalse(verdict.degraded)

    def test_synthesis_parse_failure_raises(self):
        dispatcher = MockDispatcher(bad_json=True)
        ctx = build_context_packet(DecisionType.OPPORTUNITY_SCREEN, "id", "ctx")
        with self.assertRaises(ValueError):
            run_tier1_deliberation(ctx, dispatcher)

    def test_identical_outputs_rejected(self):
        dispatcher = MockDispatcher(identical=True)
        ctx = build_context_packet(DecisionType.OPPORTUNITY_SCREEN, "id", "ctx")
        with self.assertRaises(ValueError):
            run_tier1_deliberation(ctx, dispatcher)

    def test_g3_denied_caps_confidence_and_marks_degraded(self):
        dispatcher = MockDispatcher()
        ctx = build_context_packet(DecisionType.OPPORTUNITY_SCREEN, "id", "ctx")
        verdict = run_tier1_deliberation(ctx, dispatcher, g3_denied=True)
        self.assertTrue(verdict.degraded)
        self.assertLessEqual(verdict.confidence, 0.70)

    def test_role_token_budget_enforced(self):
        class OverBudgetDispatcher(MockDispatcher):
            def dispatch_parallel(self, prompts):
                outputs = super().dispatch_parallel(prompts)
                first = outputs[0]
                outputs[0] = type(first)(
                    role=first.role,
                    content=first.content,
                    token_count=first.max_tokens + 1,
                    max_tokens=first.max_tokens,
                )
                return outputs

        dispatcher = OverBudgetDispatcher()
        ctx = build_context_packet(DecisionType.OPPORTUNITY_SCREEN, "id", "ctx")
        with self.assertRaises(ValueError):
            run_tier1_deliberation(ctx, dispatcher)

    def test_tier2_pipeline_returns_verdict(self):
        tier1 = run_tier1_deliberation(
            build_context_packet(DecisionType.OPPORTUNITY_SCREEN, "id", "ctx"),
            MockDispatcher(),
        )
        mixture = MockMixtureDispatcher()
        verdict = run_tier2_deliberation(
            build_context_packet(DecisionType.OPPORTUNITY_SCREEN, "id", "ctx"),
            mixture,
            models=["local-a", "free-b", "frontier-c"],
            estimated_cost_usd=0.0,
            tier1_verdict=tier1,
        )
        self.assertEqual(verdict.tier_used, 2)
        self.assertTrue(verdict.minority_positions)
        self.assertIsNotNone(verdict.full_debate_record)

    def test_tier2_requires_distinct_models(self):
        with self.assertRaises(ValueError):
            run_tier2_deliberation(
                build_context_packet(DecisionType.OPPORTUNITY_SCREEN, "id", "ctx"),
                MockMixtureDispatcher(),
                models=["same", "same"],
            )

    def test_tier2_low_confidence_auto_escalates(self):
        verdict = run_tier2_deliberation(
            build_context_packet(DecisionType.OPPORTUNITY_SCREEN, "id", "ctx"),
            MockMixtureDispatcher(low_confidence=True),
            models=["local-a", "free-b"],
        )
        self.assertEqual(verdict.recommendation, Recommendation.ESCALATE)
