"""Fixture generator for M5 council + research milestone evaluation."""

from __future__ import annotations

from .common import DeterministicFactory, RESEARCH_DOMAINS, RESEARCH_SEED_QUERIES


def generate_test_opportunities(n: int = 5, seed: int = 42) -> list[dict]:
    if n != 5:
        raise ValueError("M5 opportunity set must be exactly 5")
    f = DeterministicFactory(seed)
    specs = [
        ("GOOD", "software_product", "PURSUE", 0.70, 0.95),
        ("GOOD", "client_work", "PURSUE", 0.65, 0.9),
        ("BAD", "market_opportunity", "REJECT", 0.65, 0.95),
        ("BAD", "software_product", "REJECT", 0.60, 0.9),
        ("AMBIGUOUS", "ip_asset", "any", 0.35, 0.64),
    ]
    out = []
    for i, (label, mech, rec, cmin, cmax) in enumerate(specs):
        out.append(
            {
                "opportunity_id": f.uuid_v7(),
                "income_mechanism": mech,
                "title": f"Opportunity {i+1}",
                "thesis": f"Opportunity thesis {i+1} with concrete market context, timing rationale, constraints, and monetization assumptions for calibration.",
                "detected_by": "research_loop",
                "cashflow_estimate": {"low": 200.0 * (i + 1), "mid": 500.0 * (i + 1), "high": 900.0 * (i + 1), "currency": "USD", "period": "monthly"},
                "status": "DETECTED",
                "context_packet": {
                    "market_data": "Market demand and benchmark pricing were reviewed.",
                    "competitive_landscape": "Includes incumbent analysis and moat assumptions.",
                    "technical_requirements": "Assesses implementation complexity and dependencies.",
                    "resource_assessment": "Assesses available execution bandwidth.",
                },
                "ground_truth": {
                    "label": label,
                    "expected_recommendation": rec,
                    "reasoning": "Calibrated ground truth for council scoring.",
                    "expected_min_confidence": cmin,
                    "expected_max_confidence": cmax,
                },
                "da_expectations": {"min_objections": 1, "expected_min_da_quality": 0.40},
            }
        )
    return out


def generate_council_expected_outputs(opportunities: list[dict], seed: int = 42) -> list[dict]:
    _ = seed
    out = []
    for opp in opportunities:
        gt = opp["ground_truth"]
        rec = gt["expected_recommendation"] if gt["expected_recommendation"] != "any" else "PAUSE"
        out.append(
            {
                "opportunity_id": opp["opportunity_id"],
                "expected_verdict": {
                    "tier_used": 1,
                    "decision_type": "opportunity_screen",
                    "recommendation": rec,
                    "confidence_range": [gt["expected_min_confidence"], gt["expected_max_confidence"]],
                    "must_have_dissenting_views": True,
                    "must_have_da_assessment": True,
                    "da_quality_score_min": 0.40,
                },
                "scoring_rubric": {
                    "recommendation_correct": 1.0,
                    "confidence_in_range": 0.5,
                    "da_quality_above_threshold": 0.3,
                    "dissent_present": 0.2,
                },
            }
        )
    return out


def generate_research_loop_scenarios(n: int = 5, seed: int = 42) -> list[dict]:
    if n != 5:
        raise ValueError("M5 research loop scenarios must be 5")
    f = DeterministicFactory(seed + 100)
    out = []
    for domain, name in RESEARCH_DOMAINS.items():
        out.append(
            {
                "scenario_id": f.uuid_v7(),
                "domain": domain,
                "domain_name": name,
                "seed_query": RESEARCH_SEED_QUERIES[domain][0],
                "expected_output_type": "IntelligenceBrief",
                "expected_depth_tier": "QUICK",
                "expected_actionability": "WATCH",
                "max_latency_minutes": 30,
                "expected_brief_fields": {
                    "has_summary": True,
                    "has_source_urls": True,
                    "has_confidence": True,
                    "has_actionability": True,
                },
            }
        )
    return out


def generate_m5_test_set(seed: int = 42) -> dict:
    opps = generate_test_opportunities(5, seed)
    return {
        "test_opportunities": opps,
        "expected_outputs": generate_council_expected_outputs(opps, seed),
        "research_scenarios": generate_research_loop_scenarios(5, seed),
        "eval_criteria": {
            "max_opportunity_fp_rate": 0.20,
            "max_opportunity_fn_rate": 0.20,
            "min_da_quality_score": 0.40,
            "min_research_loops_complete": 5,
            "research_latency_p95_min": 30,
            "min_brief_quality_sufficient": 3,
        },
    }
