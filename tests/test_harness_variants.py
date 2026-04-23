from __future__ import annotations

import sqlite3
from pathlib import Path

from harness_variants import ExecutionTrace, ExecutionTraceStep, HarnessVariantManager, VariantEvalResult
from migrate import apply_schema


def _telemetry_manager(tmp_path: Path) -> HarnessVariantManager:
    db_path = tmp_path / "telemetry.db"
    apply_schema(db_path, Path("schemas/telemetry.sql"))
    return HarnessVariantManager(str(db_path))


def test_execution_trace_roundtrip_and_summary(tmp_path):
    manager = _telemetry_manager(tmp_path)

    first = manager.log_execution_trace(
        ExecutionTrace(
            trace_id="trace-1",
            task_id="task-1",
            role="runtime_contract",
            skill_name="runtime",
            harness_version="v1",
            intent_goal="prove contract",
            steps=[
                ExecutionTraceStep(
                    step_index=1,
                    tool_call="financial_router.route",
                    tool_result='{"tier":"paid_cloud"}',
                    tool_result_file=None,
                    tokens_in=0,
                    tokens_out=0,
                    latency_ms=4,
                    model_used="repo-contract",
                )
            ],
            prompt_template="contract harness",
            context_assembled="runtime+operator",
            retrieval_queries=[],
            judge_verdict="PASS",
            judge_reasoning="passed",
            outcome_score=1.0,
            cost_usd=0.0,
            duration_ms=12,
            training_eligible=True,
            retention_class="STANDARD",
            source_chain_id="chain-1",
            source_session_id="session-1",
            source_trace_id=None,
            created_at="2026-04-21T12:00:00+00:00",
        )
    )
    second = manager.log_execution_trace(
        ExecutionTrace(
            trace_id="trace-2",
            task_id="task-2",
            role="runtime_contract",
            skill_name="runtime",
            harness_version="v1",
            intent_goal="prove failure retention",
            steps=[],
            prompt_template="contract harness",
            context_assembled="runtime+operator",
            retrieval_queries=[],
            judge_verdict="FAIL",
            judge_reasoning="failed",
            outcome_score=0.0,
            cost_usd=0.0,
            duration_ms=8,
            training_eligible=False,
            retention_class="FAILURE_AUDIT",
            source_chain_id="chain-2",
            source_session_id="session-2",
            source_trace_id=None,
            created_at="2026-04-21T12:01:00+00:00",
        )
    )

    assert first["trace_id"] == "trace-1"
    assert second["retention_class"] == "FAILURE_AUDIT"
    traces = manager.list_execution_traces(limit=5, skill_name="runtime")
    assert [row["trace_id"] for row in traces] == ["trace-2", "trace-1"]
    summary = manager.execution_trace_summary()
    assert summary["total_count"] == 2
    assert summary["training_eligible_count"] == 1
    assert summary["failure_audit_count"] == 1


def test_harness_variant_lifecycle_and_frontier(tmp_path):
    manager = _telemetry_manager(tmp_path)

    proposed = manager.propose_variant(
        skill_name="research_domain",
        parent_version="abc123",
        diff="@@ -1 +1 @@\n-old\n+new\n",
        source="operator",
        prompt_prelude="Tighten harness prompt.",
        reference_time="2026-04-21T12:00:00+00:00",
    )
    assert proposed["status"] == "PROPOSED"

    concurrent = manager.propose_variant(
        skill_name="research_domain",
        parent_version="abc123",
        diff="@@ -1 +1 @@\n-old\n+alt\n",
        source="operator",
        reference_time="2026-04-21T12:01:00+00:00",
    )
    assert concurrent["status"] == "REJECTED"
    assert concurrent["reject_reason"] == "CONCURRENT_VARIANT"

    shadow = manager.start_shadow_eval(proposed["variant_id"], reference_time="2026-04-21T12:02:00+00:00")
    assert shadow["status"] == "SHADOW_EVAL"

    promoted = manager.record_eval_result(
        proposed["variant_id"],
        VariantEvalResult(
            variant_id=proposed["variant_id"],
            skill_name="research_domain",
            benchmark_name="shadow_replay_research_domain",
            baseline_outcome_scores=[0.7, 0.8, 0.75],
            variant_outcome_scores=[0.8, 0.82, 0.79],
            regression_rate=0.0,
            gate_0_pass=True,
            known_bad_block_rate=1.0,
            gate_1_pass=True,
            baseline_mean_score=0.75,
            variant_mean_score=0.8033,
            quality_delta=0.0533,
            gate_2_pass=True,
            baseline_std=0.04,
            variant_std=0.03,
            gate_3_pass=True,
            regressed_trace_count=0,
            improved_trace_count=3,
            net_trace_gain=3,
            traces_evaluated=3,
            compute_cost_cu=1.5,
            eval_duration_ms=250,
            replay_readiness_status="READY_FOR_BROADER_REPLAY",
            replay_readiness_blockers=[],
            operator_acknowledged_below_threshold=False,
            created_at="2026-04-21T12:03:00+00:00",
        ),
        reference_time="2026-04-21T12:03:00+00:00",
    )
    assert promoted["status"] == "PROMOTED"
    assert promoted["promoted_at"] == "2026-04-21T12:03:00+00:00"

    frontier = manager.frontier(limit=5, skill_name="research_domain")
    assert len(frontier) == 1
    assert frontier[0]["variant_id"] == proposed["variant_id"]

    rate_limited = manager.propose_variant(
        skill_name="research_domain",
        parent_version="def456",
        diff="@@ -1 +1 @@\n-old\n+later\n",
        source="operator",
        reference_time="2026-04-21T13:00:00+00:00",
    )
    assert rate_limited["status"] == "REJECTED"
    assert rate_limited["reject_reason"] == "RATE_LIMITED"

    scope_violation = manager.propose_variant(
        skill_name="operator_interface",
        parent_version="ghi789",
        diff="@@ -1 +1 @@\n-old\n+infra\n",
        source="operator",
        touches_infrastructure=True,
        reference_time="2026-04-21T12:05:00+00:00",
    )
    assert scope_violation["status"] == "REJECTED"
    assert scope_violation["reject_reason"] == "SCOPE_VIOLATION"

    summary = manager.summary(reference_time="2026-04-21T13:05:00+00:00")
    assert summary["active_count"] == 0
    assert summary["promoted_count"] == 1
    assert summary["rejected_24h"] == 3


def test_harness_variant_replay_eval_uses_execution_traces(tmp_path):
    manager = _telemetry_manager(tmp_path)

    for idx, score in enumerate((0.72, 0.75, 0.78), start=1):
        manager.log_execution_trace(
            ExecutionTrace(
                trace_id=f"baseline-{idx}",
                task_id=f"task-{idx}",
                role="runtime",
                skill_name="research_domain",
                harness_version="baseline-v1",
                intent_goal="baseline replay set",
                steps=[
                    ExecutionTraceStep(
                        step_index=1,
                        tool_call="research.run",
                        tool_result='{"ok":true}',
                        tool_result_file=None,
                        tokens_in=120,
                        tokens_out=80,
                        latency_ms=12,
                        model_used="local-default",
                    )
                ],
                prompt_template="baseline prompt",
                context_assembled="context " * 40,
                retrieval_queries=["market signal", "customer signal"],
                judge_verdict="PASS",
                judge_reasoning="good",
                outcome_score=score,
                cost_usd=0.0,
                duration_ms=25,
                training_eligible=True,
                retention_class="STANDARD",
                source_chain_id=f"chain-{idx}",
                source_session_id=f"session-{idx}",
                source_trace_id=None,
                created_at=f"2026-04-21T12:0{idx}:00+00:00",
            )
        )

    manager.log_execution_trace(
        ExecutionTrace(
            trace_id="known-bad-1",
            task_id="task-bad-1",
            role="runtime",
            skill_name="research_domain",
            harness_version="baseline-v1",
            intent_goal="known bad replay set",
            steps=[],
            prompt_template="baseline prompt",
            context_assembled="context " * 10,
            retrieval_queries=["unsafe expansion"],
            judge_verdict="FAIL",
            judge_reasoning="known bad",
            outcome_score=0.1,
            cost_usd=0.0,
            duration_ms=12,
            training_eligible=False,
            retention_class="FAILURE_AUDIT",
            source_chain_id="chain-bad-1",
            source_session_id="session-bad-1",
            source_trace_id=None,
            created_at="2026-04-21T12:09:00+00:00",
        )
    )

    proposed = manager.propose_variant(
        skill_name="research_domain",
        parent_version="baseline-v1",
        diff="@@ -1 +1 @@\n-old\n+new retrieval calibration\n",
        source="operator",
        prompt_prelude="Tighten evidence grounding and clarify the final answer rubric.",
        retrieval_strategy_diff="Use multi-query retrieval and rerank the strongest evidence first.",
        scoring_formula_diff="Calibrate thresholds and reward grounded evidence.",
        context_assembly_diff="Compress context and prioritize the most relevant snippets.",
        reference_time="2026-04-21T12:10:00+00:00",
    )

    replayed = manager.evaluate_variant_from_traces(
        proposed["variant_id"],
        sample_size=10,
        minimum_trace_count=3,
        minimum_known_bad_traces=1,
        allow_below_activation_threshold=True,
        reference_time="2026-04-21T12:11:00+00:00",
    )

    assert replayed["status"] == "PROMOTED"
    assert replayed["eval_result"] is not None
    assert replayed["eval_result"]["traces_evaluated"] == 3
    assert replayed["eval_result"]["known_bad_block_rate"] == 1.0
    assert replayed["eval_result"]["quality_delta"] > 0.0
    assert replayed["eval_result"]["replay_readiness_status"] == "IMPLEMENTED_BELOW_ACTIVATION_THRESHOLD"
    assert replayed["eval_result"]["operator_acknowledged_below_threshold"] is True
    replay_traces = manager.list_execution_traces(limit=20, skill_name="research_domain")
    replay_artifacts = [row for row in replay_traces if row["source_trace_id"] is not None]
    assert len(replay_artifacts) == 4
    assert all(row["harness_version"] == proposed["variant_id"] for row in replay_artifacts)


def test_harness_variant_replay_eval_requires_explicit_ack_below_threshold(tmp_path):
    manager = _telemetry_manager(tmp_path)

    manager.log_execution_trace(
        ExecutionTrace(
            trace_id="baseline-1",
            task_id="task-1",
            role="runtime",
            skill_name="research_domain",
            harness_version="baseline-v1",
            intent_goal="baseline replay set",
            steps=[],
            prompt_template="baseline prompt",
            context_assembled="context",
            retrieval_queries=["market signal"],
            judge_verdict="PASS",
            judge_reasoning="good",
            outcome_score=0.72,
            cost_usd=0.0,
            duration_ms=25,
            training_eligible=True,
            retention_class="STANDARD",
            source_chain_id="chain-1",
            source_session_id="session-1",
            source_trace_id=None,
            created_at="2026-04-21T12:01:00+00:00",
        )
    )

    proposed = manager.propose_variant(
        skill_name="research_domain",
        parent_version="baseline-v1",
        diff="@@ -1 +1 @@\n-old\n+new retrieval calibration\n",
        source="operator",
        reference_time="2026-04-21T12:10:00+00:00",
    )

    try:
        manager.evaluate_variant_from_traces(
            proposed["variant_id"],
            sample_size=10,
            minimum_trace_count=1,
            minimum_known_bad_traces=0,
            reference_time="2026-04-21T12:11:00+00:00",
        )
    except ValueError as exc:
        assert "explicit operator acknowledgement is required" in str(exc)
    else:
        raise AssertionError("expected below-threshold replay to require explicit acknowledgement")


def test_replay_readiness_excludes_control_plane_roles(tmp_path):
    manager = _telemetry_manager(tmp_path)

    manager.log_execution_trace(
        ExecutionTrace(
            trace_id="runtime-eligible-1",
            task_id="task-eligible-1",
            role="runtime_contract",
            skill_name="runtime",
            harness_version="contract-v1",
            intent_goal="seed activation corpus",
            steps=[],
            prompt_template="runtime contract",
            context_assembled="runtime corpus",
            retrieval_queries=[],
            judge_verdict="PASS",
            judge_reasoning="ok",
            outcome_score=1.0,
            cost_usd=0.0,
            duration_ms=10,
            training_eligible=True,
            retention_class="STANDARD",
            source_chain_id="chain-eligible-1",
            source_session_id="session-eligible-1",
            source_trace_id=None,
            created_at="2026-04-21T15:00:00+00:00",
        )
    )
    manager.log_execution_trace(
        ExecutionTrace(
            trace_id="operator-ack-1",
            task_id="task-operator-1",
            role="operator_digest_acknowledgement",
            skill_name="operator_interface",
            harness_version="operator_acknowledgement_v1",
            intent_goal="acknowledge digest",
            steps=[],
            prompt_template="acknowledge_digest",
            context_assembled="digest bookkeeping",
            retrieval_queries=[],
            judge_verdict="PASS",
            judge_reasoning="ok",
            outcome_score=1.0,
            cost_usd=0.0,
            duration_ms=5,
            training_eligible=True,
            retention_class="STANDARD",
            source_chain_id="chain-operator-1",
            source_session_id="session-operator-1",
            source_trace_id=None,
            created_at="2026-04-21T15:01:00+00:00",
        )
    )
    manager.log_execution_trace(
        ExecutionTrace(
            trace_id="runtime-halt-1",
            task_id="task-runtime-halt-1",
            role="runtime_halt_activation",
            skill_name="runtime",
            harness_version="runtime_activate_halt_v1",
            intent_goal="halt runtime",
            steps=[],
            prompt_template="activate_halt",
            context_assembled="runtime recovery",
            retrieval_queries=[],
            judge_verdict="FAIL",
            judge_reasoning="halted",
            outcome_score=0.0,
            cost_usd=0.0,
            duration_ms=5,
            training_eligible=False,
            retention_class="FAILURE_AUDIT",
            source_chain_id="chain-runtime-halt-1",
            source_session_id="session-runtime-halt-1",
            source_trace_id=None,
            created_at="2026-04-21T15:02:00+00:00",
        )
    )

    readiness = manager.replay_readiness_summary()
    report = manager.replay_readiness_report(limit=5)

    assert readiness["eligible_source_traces"] == 1
    assert readiness["known_bad_source_traces"] == 0
    assert readiness["distinct_skill_count"] == 1
    assert report["activation_source_trace_count"] == 1
    assert report["excluded_role_counts"][0]["role"] == "operator_digest_acknowledgement"
    assert report["coverage_gaps"][0]["metric"] == "eligible_source_traces"
    assert report["skills_without_known_bad"] == ["runtime"]
