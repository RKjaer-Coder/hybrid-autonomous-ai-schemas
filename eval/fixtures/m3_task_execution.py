"""Fixture generator for M3 task-execution milestone evaluation."""

from __future__ import annotations

from .common import DeterministicFactory, REALISTIC_TASK_BRIEFS


_TASK_TYPES = ["code_generation", "research_synthesis", "file_manipulation", "web_research", "data_analysis"]


def _steps(length: int, seed_skill: str) -> list[dict]:
    kinds = ["tool_call", "model_invocation", "schema_validation", "state_transition"]
    return [
        {
            "step_index": i,
            "step_type": kinds[i % len(kinds)],
            "skill": seed_skill,
            "skip_eligible": i > 0,
            "expected_outcome": "PASS",
            "max_latency_ms": 5000 + i * 200,
        }
        for i in range(length)
    ]


def generate_e2e_scenarios(n: int = 10, seed: int = 42) -> list[dict]:
    f = DeterministicFactory(seed)
    chain_lengths = [3, 5, 7, 4, 6, 2, 5, 4, 5, 6]
    out = []
    for i in range(n):
        t = _TASK_TYPES[i % len(_TASK_TYPES)]
        steps = _steps(chain_lengths[i % len(chain_lengths)], t)
        out.append(
            {
                "scenario_id": f.uuid_v7(),
                "task_type": t,
                "task_brief": f"{REALISTIC_TASK_BRIEFS[i]} Include inputs, transformations, validation, and final artifact handoff.",
                "expected_steps": steps,
                "expected_chain_length": len(steps),
                "expected_outputs": {
                    "primary_output_type": ["code_file", "intelligence_brief", "modified_file", "summary", "analysis_table"][i % 5],
                    "validation_criteria": "Output aligns with schema, safety, and task acceptance criteria.",
                },
                "force_failure": None,
                "telemetry_expectations": {"step_outcome_count": len(steps), "all_outcomes_logged": True},
            }
        )
    return out


def generate_failure_scenarios(n: int = 2, seed: int = 42) -> list[dict]:
    if n != 2:
        raise ValueError("M3 failure scenario count must be 2")
    f = DeterministicFactory(seed + 100)
    s1_steps = _steps(5, "web_research")
    s2_steps = _steps(6, "code_generation")
    return [
        {
            "scenario_id": f.uuid_v7(),
            "task_type": "web_research",
            "task_brief": "Search for recent AI model releases and summarise findings.",
            "force_failure": {
                "failure_step_index": 1,
                "failure_type": "timeout",
                "failure_count": 2,
                "expected_recovery_tier": 1,
                "expected_recovery_success": True,
                "max_retry_attempts": 3,
            },
            "expected_steps": s1_steps,
            "expected_chain_length": len(s1_steps),
            "telemetry_expectations": {"step_outcome_count": len(s1_steps) + 2, "degraded_steps": 0, "failed_steps": 2, "recovered": True},
        },
        {
            "scenario_id": f.uuid_v7(),
            "task_type": "code_generation",
            "task_brief": "Generate a data validation function with type checking.",
            "force_failure": {
                "failure_step_index": 2,
                "failure_type": "model_error",
                "failure_count": 4,
                "expected_recovery_tier": 3,
                "expected_recovery_success": True,
                "reroute_to": "local_fallback",
            },
            "expected_steps": s2_steps,
            "expected_chain_length": len(s2_steps),
            "telemetry_expectations": {"step_outcome_count": len(s2_steps) + 4, "degraded_steps": 1, "failed_steps": 4, "recovered": True},
        },
    ]


def generate_validation_test_outputs(n: int = 20, seed: int = 42) -> list[dict]:
    f = DeterministicFactory(seed + 200)
    out = []
    for i in range(n):
        is_invalid = i >= 18
        invalid_type = "schema" if i == 18 else "trust"
        out.append(
            {
                "output_id": f.uuid_v7(),
                "task_type": _TASK_TYPES[i % len(_TASK_TYPES)],
                "output_content": f"Generated output artifact {i+1}",
                "output_metadata": {
                    "schema_valid": not (is_invalid and invalid_type == "schema"),
                    "trust_tier_respected": not (is_invalid and invalid_type == "trust"),
                    "content_safe": True,
                },
                "expected_verdict": "FAIL" if is_invalid else "PASS",
                "failure_reason": "missing confidence field" if i == 18 else ("trust tier mismatch" if i == 19 else None),
            }
        )
    return out


def generate_m3_test_set(seed: int = 42) -> dict:
    return {
        "e2e_scenarios": generate_e2e_scenarios(10, seed),
        "failure_scenarios": generate_failure_scenarios(2, seed),
        "validation_outputs": generate_validation_test_outputs(20, seed),
        "eval_criteria": {
            "min_task_types": 3,
            "min_completions": 10,
            "min_recovery_successes": 2,
            "max_task_validation_fp_rate": 0.10,
            "max_task_validation_fn_rate": 0.05,
            "latency_p95_s": 120,
            "telemetry_coverage": 1.00,
        },
    }
