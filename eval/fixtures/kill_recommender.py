"""Fixture generator for kill recommender calibration."""

from __future__ import annotations

from .common import DeterministicFactory, INCOME_MECHANISMS, weighted_sum


def generate_calibration_set(n: int = 20, seed: int = 42) -> list[dict]:
    if n != 20:
        raise ValueError("Kill calibration set must contain 20 fixtures")
    f = DeterministicFactory(seed)
    outcomes = (
        ["killed_correct"] * 5
        + ["continued_waste"] * 3
        + ["completed_profitable"] * 3
        + ["completed_marginal"] * 3
        + ["killed_premature"] * 3
        + ["active_mixed"] * 3
    )
    projects = []
    for i, outcome in enumerate(outcomes):
        should_kill = outcome in {"killed_correct", "continued_waste"}
        signals = [
            {"signal_type": "cashflow_vs_forecast", "weight": 0.30, "raw_score": 1.0 if should_kill else 0.5 if outcome == "completed_marginal" else 0.0, "evidence": "Cashflow trend versus target."},
            {"signal_type": "council_confidence", "weight": 0.20, "raw_score": 1.0 if outcome in {"continued_waste", "killed_correct"} else 0.5, "evidence": "Council confidence drift."},
            {"signal_type": "technical_blocker", "weight": 0.20, "raw_score": 1.0 if outcome in {"killed_correct", "killed_premature"} else 0.0, "evidence": "Blocker evidence from execution logs."},
            {"signal_type": "market_invalidation", "weight": 0.15, "raw_score": 0.5 if outcome in {"continued_waste", "active_mixed"} else 0.0, "evidence": "Signal of reduced demand."},
            {"signal_type": "asset_creation", "weight": 0.15, "raw_score": 0.0 if outcome in {"completed_profitable", "completed_marginal"} else 0.5, "evidence": "Asset progress and reusability."},
        ]
        kill_score = weighted_sum(signals)
        projects.append(
            {
                "project_id": f.uuid_v7(),
                "opportunity_id": f.uuid_v7(),
                "name": f"Project {i+1}",
                "income_mechanism": INCOME_MECHANISMS[i % len(INCOME_MECHANISMS)],
                "thesis": "Ship a narrow product iteration, measure conversion, and retain reusable assets.",
                "status": "ACTIVE" if outcome == "active_mixed" else ("KILLED" if "killed" in outcome else "COMPLETE"),
                "portfolio_weight": round(0.05 + (i % 5) * 0.05, 2),
                "cashflow_target_usd": 1200.0,
                "cashflow_actual_usd": 200.0 if should_kill else 1800.0 if outcome == "completed_profitable" else 900.0,
                "compute_budget": {"max_executor_hours": 60.0, "max_cloud_spend_usd": 300.0, "alert_at_pct": 0.90},
                "compute_consumed": {"executor_hours": 40.0 + i, "cloud_spend_usd": 120.0 + i * 2},
                "weeks_active": 4 + (i % 10),
                "phases_completed": i % 5,
                "kill_signals": signals,
                "kill_score": kill_score,
                "ground_truth": {
                    "should_have_killed": should_kill,
                    "optimal_kill_week": 6 if should_kill else None,
                    "actual_outcome": outcome,
                    "binary_outcome": 1.0 if outcome in {"completed_profitable", "killed_correct"} else 0.5 if outcome == "completed_marginal" else 0.0,
                    "reasoning": "Ground-truth label for calibration benchmarking.",
                },
            }
        )
    return projects
