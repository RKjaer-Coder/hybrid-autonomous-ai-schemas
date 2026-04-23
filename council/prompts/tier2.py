TIER2_MIXTURE_PROMPT = """You are running a Tier 2 Council deliberation using multiple genuinely different models.

Objective:
- Produce a final CouncilVerdict for the decision below.
- Treat the supplied Tier 1 verdict as a pre-assessment, not as the answer.
- Preserve disagreement. Do not collapse the debate into bland consensus.

Execution protocol:
1. Round 1: each model forms an independent position from the same context.
2. Round 2: each model cross-examines the others and identifies the single
   strongest disagreement.
3. Round 3: synthesize a final verdict while preserving the strongest minority
   position and the most decision-relevant dissent.

Decision context:
{context_packet}

Tier 1 pre-assessment:
{tier1_verdict}

Selected models:
{model_list}

Critical rules:
1. Do not average positions.
2. Name the decisive argument and why it wins.
3. Preserve the strongest minority position in minority_positions.
4. full_debate_record must summarize round-1 positions, the key round-2 clash,
   and the synthesis rationale in a compact but auditable form.
5. If confidence < 0.60, recommendation should normally be ESCALATE unless the
   evidence clearly supports INSUFFICIENT_DATA.
6. da_assessment must score the strongest minority objections using:
   incorporated | acknowledged | dismissed.

Return JSON only in this format:

```json
{{
  "tier_used": 2,
  "decision_type": "{decision_type}",
  "recommendation": "PURSUE | REJECT | PAUSE | ESCALATE | INSUFFICIENT_DATA",
  "confidence": <0.0-1.0>,
  "reasoning_summary": "<2-4 sentences>",
  "dissenting_views": "<strongest counterargument>",
  "minority_positions": ["<minority position 1>", "<minority position 2>"],
  "full_debate_record": "<compact audit record of rounds 1-3>",
  "cost_usd": <non-negative number>,
  "da_assessment": [
    {{
      "objection": "<minority objection>",
      "tag": "incorporated | acknowledged | dismissed",
      "reasoning": "<why>"
    }}
  ],
  "tie_break": false,
  "risk_watch": ["<items to monitor>"]
}}
```"""


TIER2_OUTPUT_SCHEMA = {
    "type": "object",
    "required": [
        "tier_used",
        "decision_type",
        "recommendation",
        "confidence",
        "reasoning_summary",
        "dissenting_views",
        "minority_positions",
        "full_debate_record",
        "cost_usd",
        "da_assessment",
        "tie_break",
    ],
    "properties": {
        "tier_used": {"type": "integer", "const": 2},
        "decision_type": {"type": "string"},
        "recommendation": {"type": "string", "enum": ["PURSUE", "REJECT", "PAUSE", "ESCALATE", "INSUFFICIENT_DATA"]},
        "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
        "reasoning_summary": {"type": "string", "maxLength": 1000},
        "dissenting_views": {"type": "string", "maxLength": 700},
        "minority_positions": {
            "type": "array",
            "minItems": 1,
            "items": {"type": "string"},
        },
        "full_debate_record": {"type": "string", "maxLength": 4000},
        "cost_usd": {"type": "number", "minimum": 0.0},
        "da_assessment": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "required": ["objection", "tag", "reasoning"],
                "properties": {
                    "objection": {"type": "string"},
                    "tag": {"type": "string", "enum": ["incorporated", "acknowledged", "dismissed"]},
                    "reasoning": {"type": "string"},
                },
            },
        },
        "tie_break": {"type": "boolean"},
        "risk_watch": {"type": "array", "items": {"type": "string"}},
    },
}
