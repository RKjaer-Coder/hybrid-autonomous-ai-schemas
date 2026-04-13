import unittest

from council.prompts.common import enforce_token_budget, format_batch_a_for_da, parse_json_output
from council.prompts.role_critic import CRITIC_OUTPUT_SCHEMA, CRITIC_SYSTEM_PROMPT
from council.prompts.role_devils_advocate import DEVILS_ADVOCATE_SYSTEM_PROMPT
from council.prompts.role_realist import REALIST_OUTPUT_SCHEMA, REALIST_SYSTEM_PROMPT
from council.prompts.role_strategist import STRATEGIST_OUTPUT_SCHEMA, STRATEGIST_SYSTEM_PROMPT
from council.prompts.synthesis import SYNTHESIS_OUTPUT_SCHEMA, SYNTHESIS_SYSTEM_PROMPT
from council.types import RoleName, RoleOutput


class TestPrompts(unittest.TestCase):
    def test_strategist_template_present(self):
        self.assertIn("strategic analyst", STRATEGIST_SYSTEM_PROMPT)

    def test_critic_template_present(self):
        self.assertIn("critical analyst", CRITIC_SYSTEM_PROMPT)

    def test_realist_template_present(self):
        self.assertIn("execution analyst", REALIST_SYSTEM_PROMPT)

    def test_da_placeholder_present(self):
        self.assertIn("{batch_a_outputs}", DEVILS_ADVOCATE_SYSTEM_PROMPT)

    def test_synthesis_placeholders_present(self):
        for p in ["{strategist_output}", "{critic_output}", "{realist_output}", "{da_output}"]:
            self.assertIn(p, SYNTHESIS_SYSTEM_PROMPT)

    def test_token_budget_truncates(self):
        text = "word " * 100
        out = enforce_token_budget(text, 20)
        self.assertIn("[TRUNCATED]", out)

    def test_token_budget_unchanged_under_limit(self):
        text = "small text"
        self.assertEqual(text, enforce_token_budget(text, 100))

    def test_schema_missing_required_rejected(self):
        with self.assertRaises(ValueError):
            parse_json_output('{"role":"critic"}', CRITIC_OUTPUT_SCHEMA)

    def test_schema_valid_strategist(self):
        payload = '{"role":"strategist","case_for":"x","market_fit_score":0.5,"timing_assessment":"x","strategic_alignment":"x","key_assumption":"x"}'
        parsed = parse_json_output(payload, STRATEGIST_OUTPUT_SCHEMA)
        self.assertEqual(parsed["role"], "strategist")

    def test_schema_valid_realist(self):
        payload = '{"role":"realist","execution_requirements":"x","compute_needs":"x","time_to_revenue_days":10,"capital_required_usd":0.0,"blocking_prerequisite":"x","feasibility_score":0.8}'
        parsed = parse_json_output(payload, REALIST_OUTPUT_SCHEMA)
        self.assertEqual(parsed["role"], "realist")

    def test_schema_valid_synthesis(self):
        payload = '{"tier_used":1,"decision_type":"opportunity_screen","recommendation":"PURSUE","confidence":0.8,"reasoning_summary":"x","dissenting_views":"y","da_assessment":[{"objection":"o","tag":"acknowledged","reasoning":"r"}],"tie_break":false}'
        parsed = parse_json_output(payload, SYNTHESIS_OUTPUT_SCHEMA)
        self.assertEqual(parsed["tier_used"], 1)

    def test_da_prompt_injection_escaped(self):
        out = RoleOutput(role=RoleName.STRATEGIST, content='{"x":"{batch_a_outputs=INJECTED}"}', token_count=1, max_tokens=10)
        formatted = format_batch_a_for_da([out])
        self.assertIn("{{batch_a_outputs=INJECTED}}", formatted)
