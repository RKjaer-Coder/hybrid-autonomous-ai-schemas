import unittest

from council.types import DecisionType, RoleName
from council.validators import validate_role_output, validate_verdict


class TestValidators(unittest.TestCase):
    def _valid(self):
        return {
            "tier_used": 1,
            "decision_type": "opportunity_screen",
            "recommendation": "PURSUE",
            "confidence": 0.7,
            "reasoning_summary": "x",
            "dissenting_views": "y",
            "da_assessment": [{"objection": "o", "tag": "acknowledged", "reasoning": "r"}],
            "tie_break": False,
        }

    def test_valid_verdict_passes(self):
        self.assertEqual(validate_verdict(self._valid(), DecisionType.OPPORTUNITY_SCREEN), [])

    def test_missing_required_field(self):
        d = self._valid()
        del d["recommendation"]
        self.assertTrue(validate_verdict(d, DecisionType.OPPORTUNITY_SCREEN))

    def test_confidence_outside_range(self):
        d = self._valid()
        d["confidence"] = 1.4
        self.assertIn("confidence out of range", validate_verdict(d, DecisionType.OPPORTUNITY_SCREEN))

    def test_invalid_recommendation(self):
        d = self._valid()
        d["recommendation"] = "MAYBE"
        self.assertIn("invalid recommendation", validate_verdict(d, DecisionType.OPPORTUNITY_SCREEN))

    def test_degraded_confidence_cap_error(self):
        d = self._valid()
        d["degraded"] = True
        d["confidence"] = 0.9
        self.assertIn("degraded confidence cap violated", validate_verdict(d, DecisionType.OPPORTUNITY_SCREEN))

    def test_empty_da_assessment_error(self):
        d = self._valid()
        d["da_assessment"] = []
        self.assertIn("da_assessment required", validate_verdict(d, DecisionType.OPPORTUNITY_SCREEN))

    def test_valid_tier2_verdict_passes(self):
        d = self._valid()
        d["tier_used"] = 2
        d["minority_positions"] = ["minority"]
        d["full_debate_record"] = "round1 -> round2 -> round3"
        d["cost_usd"] = 0.0
        self.assertEqual(validate_verdict(d, DecisionType.OPPORTUNITY_SCREEN), [])

    def test_tie_break_high_confidence_warning(self):
        d = self._valid()
        d["tie_break"] = True
        d["confidence"] = 0.8
        self.assertIn("warning: tie_break with high confidence", validate_verdict(d, DecisionType.OPPORTUNITY_SCREEN))

    def test_validate_role_output_valid(self):
        raw = '{"role":"critic","case_against":"x","execution_risk":"x","market_risk":"x","fatal_dependency":"x","risk_severity":0.8}'
        _, errs = validate_role_output(raw, RoleName.CRITIC)
        self.assertFalse(errs)

    def test_validate_role_output_invalid(self):
        raw = '{"role":"critic"}'
        _, errs = validate_role_output(raw, RoleName.CRITIC)
        self.assertTrue(errs)
