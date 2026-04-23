from __future__ import annotations

import datetime
import json
import math
import sqlite3
import time
import uuid
from dataclasses import asdict, dataclass
from typing import Any


_REQUIRED_TABLES = {
    "execution_traces",
    "harness_variants",
}

_POSITIVE_VARIANT_CUES = {
    "tighten": 0.012,
    "clarify": 0.01,
    "focus": 0.012,
    "ground": 0.01,
    "rank": 0.008,
    "normalize": 0.01,
    "calibrate": 0.014,
    "rubric": 0.01,
}
_RETRIEVAL_VARIANT_CUES = {
    "retrieval": 0.014,
    "retrieve": 0.012,
    "rerank": 0.014,
    "multi": 0.01,
    "divers": 0.012,
    "dedupe": 0.008,
}
_CONTEXT_VARIANT_CUES = {
    "context": 0.008,
    "compress": 0.014,
    "summar": 0.012,
    "priorit": 0.012,
    "trim": 0.01,
    "budget": 0.008,
}
_SCORING_VARIANT_CUES = {
    "score": 0.014,
    "threshold": 0.012,
    "penal": 0.01,
    "reward": 0.008,
    "confidence": 0.008,
    "variance": 0.01,
}
_RISK_VARIANT_CUES = {
    "disable": 0.05,
    "bypass": 0.08,
    "skip": 0.06,
    "ignore": 0.06,
    "unsafe": 0.08,
    "raw": 0.03,
    "unfilter": 0.05,
    "shell": 0.04,
    "network": 0.03,
    "sudo": 0.1,
}
REPLAY_ACTIVATION_MIN_ELIGIBLE_TRACES = 500
REPLAY_ACTIVATION_MIN_KNOWN_BAD_TRACES = 25
REPLAY_ACTIVATION_MIN_DISTINCT_SKILLS = 3
DEFAULT_REPLAY_SAMPLE_TARGET = 50
REPLAY_ENFORCEMENT_MODE = "FAIL_CLOSED_UNLESS_OPERATOR_ACKNOWLEDGED"
REPLAY_ACTIVATION_EXCLUDED_ROLES = {
    "immune_judge_check",
    "immune_sheriff_check",
    "judge_deadlock_clear",
    "judge_deadlock_fallback_activation",
    "judge_deadlock_halt",
    "judge_deadlock_retro_review",
    "judge_deadlock_review_enqueue",
    "operator_alert_acknowledgement",
    "operator_digest_acknowledgement",
    "operator_judge_deadlock_restart",
    "operator_quarantine_review",
    "operator_runtime_restart",
    "runtime_halt_activation",
    "runtime_halt_reused",
    "runtime_restart_blocked",
    "runtime_restart_completed",
}


def _parse_ts(value: str) -> datetime.datetime:
    parsed = datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=datetime.timezone.utc)
    return parsed.astimezone(datetime.timezone.utc)


def _to_iso(value: datetime.datetime) -> str:
    return value.astimezone(datetime.timezone.utc).replace(microsecond=0).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _clamp(value: float, minimum: float = 0.0, maximum: float = 1.0) -> float:
    return max(minimum, min(maximum, value))


def _mean(values: list[float]) -> float:
    if not values:
        return 0.0
    return sum(values) / len(values)


def _population_std(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    average = _mean(values)
    variance = sum((value - average) ** 2 for value in values) / len(values)
    return math.sqrt(variance)


def _activation_role_sql(column: str = "role") -> str:
    excluded = ", ".join(f"'{role}'" for role in sorted(REPLAY_ACTIVATION_EXCLUDED_ROLES))
    return f"{column} NOT IN ({excluded})"


@dataclass(frozen=True)
class ExecutionTraceStep:
    step_index: int
    tool_call: str
    tool_result: str
    tool_result_file: str | None
    tokens_in: int
    tokens_out: int
    latency_ms: int
    model_used: str
    model_switch: dict[str, str] | None = None


@dataclass(frozen=True)
class ExecutionTrace:
    trace_id: str
    task_id: str
    role: str
    skill_name: str
    harness_version: str
    intent_goal: str
    steps: list[ExecutionTraceStep]
    prompt_template: str
    context_assembled: str
    retrieval_queries: list[str]
    judge_verdict: str
    judge_reasoning: str
    outcome_score: float
    cost_usd: float
    duration_ms: int
    training_eligible: bool
    retention_class: str
    source_chain_id: str | None
    source_session_id: str | None
    source_trace_id: str | None
    created_at: str


@dataclass(frozen=True)
class HarnessVariant:
    variant_id: str
    skill_name: str
    parent_version: str
    diff: str
    source: str
    status: str
    created_at: str
    prompt_prelude: str = ""
    retrieval_strategy_diff: str = ""
    scoring_formula_diff: str = ""
    context_assembly_diff: str = ""
    touches_infrastructure: bool = False
    reject_reason: str | None = None
    eval_result: dict[str, Any] | None = None
    promoted_at: str | None = None
    updated_at: str | None = None


@dataclass(frozen=True)
class VariantEvalResult:
    variant_id: str
    skill_name: str
    benchmark_name: str
    baseline_outcome_scores: list[float]
    variant_outcome_scores: list[float]
    regression_rate: float
    gate_0_pass: bool
    known_bad_block_rate: float
    gate_1_pass: bool
    baseline_mean_score: float
    variant_mean_score: float
    quality_delta: float
    gate_2_pass: bool
    baseline_std: float
    variant_std: float
    gate_3_pass: bool
    regressed_trace_count: int
    improved_trace_count: int
    net_trace_gain: int
    traces_evaluated: int
    compute_cost_cu: float
    eval_duration_ms: int
    replay_readiness_status: str
    replay_readiness_blockers: list[str]
    operator_acknowledged_below_threshold: bool
    created_at: str

    @property
    def all_gates_pass(self) -> bool:
        return self.gate_0_pass and self.gate_1_pass and self.gate_2_pass and self.gate_3_pass

    def ranking_key(self) -> tuple[int, float, int, float]:
        return (
            -self.regressed_trace_count,
            round(self.quality_delta, 10),
            self.net_trace_gain,
            round(-self.eval_duration_ms / 1000.0, 10),
        )


class HarnessVariantManager:
    """Persistence and lifecycle manager for initial §8.3b telemetry substrate."""

    def __init__(self, telemetry_db_path: str):
        self._telemetry_db_path = telemetry_db_path
        self._available = self._verify_tables()

    @property
    def available(self) -> bool:
        return self._available

    def get_variant(self, variant_id: str) -> dict[str, Any]:
        if not self._available:
            raise RuntimeError("Harness variant tables are not available")
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM harness_variants WHERE variant_id = ? LIMIT 1",
                (variant_id,),
            ).fetchone()
        if row is None:
            raise KeyError(variant_id)
        return self._variant_row_to_dict(row)

    def log_execution_trace(self, trace: ExecutionTrace) -> dict[str, Any]:
        if not self._available:
            raise RuntimeError("Execution trace tables are not available")
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO execution_traces (
                    trace_id, task_id, role, skill_name, harness_version, intent_goal,
                    steps_json, prompt_template, context_assembled, retrieval_queries_json,
                    judge_verdict, judge_reasoning, outcome_score, cost_usd, duration_ms,
                    training_eligible, retention_class, source_chain_id, source_session_id,
                    source_trace_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    trace.trace_id,
                    trace.task_id,
                    trace.role,
                    trace.skill_name,
                    trace.harness_version,
                    trace.intent_goal,
                    _json([asdict(step) for step in trace.steps]),
                    trace.prompt_template,
                    trace.context_assembled,
                    _json(trace.retrieval_queries),
                    trace.judge_verdict,
                    trace.judge_reasoning,
                    trace.outcome_score,
                    trace.cost_usd,
                    trace.duration_ms,
                    1 if trace.training_eligible else 0,
                    trace.retention_class,
                    trace.source_chain_id,
                    trace.source_session_id,
                    trace.source_trace_id,
                    trace.created_at,
                ),
            )
            conn.commit()
            row = conn.execute(
                "SELECT * FROM execution_traces WHERE trace_id = ? LIMIT 1",
                (trace.trace_id,),
            ).fetchone()
        assert row is not None
        return self._trace_row_to_dict(row)

    def log_skill_action_trace(
        self,
        *,
        task_id: str,
        role: str,
        skill_name: str,
        action_name: str,
        intent_goal: str,
        action_payload: Any,
        context_assembled: str,
        retrieval_queries: list[str] | None = None,
        harness_version: str | None = None,
        judge_verdict: str = "PASS",
        judge_reasoning: str | None = None,
        outcome_score: float | None = None,
        training_eligible: bool | None = None,
        retention_class: str | None = None,
        source_chain_id: str | None = None,
        source_session_id: str | None = None,
        source_trace_id: str | None = None,
        model_used: str = "repo-skill",
        duration_ms: int = 0,
        cost_usd: float = 0.0,
        created_at: str | None = None,
    ) -> dict[str, Any]:
        verdict = judge_verdict.upper()
        eligible = training_eligible if training_eligible is not None else verdict == "PASS"
        trace = ExecutionTrace(
            trace_id=str(uuid.uuid4()),
            task_id=task_id,
            role=role,
            skill_name=skill_name,
            harness_version=harness_version or f"{skill_name}_{action_name}_v1",
            intent_goal=intent_goal,
            steps=[
                ExecutionTraceStep(
                    step_index=1,
                    tool_call=f"{skill_name}.{action_name}",
                    tool_result=json.dumps(action_payload, sort_keys=True, default=str)[:4096],
                    tool_result_file=None,
                    tokens_in=0,
                    tokens_out=0,
                    latency_ms=duration_ms,
                    model_used=model_used,
                )
            ],
            prompt_template=action_name,
            context_assembled=context_assembled,
            retrieval_queries=list(retrieval_queries or []),
            judge_verdict=verdict,
            judge_reasoning=judge_reasoning or ("Trace logged successfully." if verdict == "PASS" else "Action failed."),
            outcome_score=outcome_score if outcome_score is not None else (1.0 if verdict == "PASS" else 0.0),
            cost_usd=cost_usd,
            duration_ms=duration_ms,
            training_eligible=eligible,
            retention_class=retention_class or ("STANDARD" if verdict == "PASS" else "FAILURE_AUDIT"),
            source_chain_id=source_chain_id,
            source_session_id=source_session_id,
            source_trace_id=source_trace_id,
            created_at=created_at or self._now(None),
        )
        return self.log_execution_trace(trace)

    def list_execution_traces(
        self,
        *,
        limit: int = 20,
        skill_name: str | None = None,
        training_eligible: bool | None = None,
        judge_verdict: str | None = None,
    ) -> list[dict[str, Any]]:
        if not self._available:
            return []
        where: list[str] = []
        params: list[object] = []
        if skill_name is not None:
            where.append("skill_name = ?")
            params.append(skill_name)
        if training_eligible is not None:
            where.append("training_eligible = ?")
            params.append(1 if training_eligible else 0)
        if judge_verdict is not None:
            where.append("judge_verdict = ?")
            params.append(judge_verdict)
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT *
                FROM execution_traces
                {where_sql}
                ORDER BY created_at DESC, trace_id DESC
                LIMIT ?
                """,
                (*params, limit),
            ).fetchall()
        return [self._trace_row_to_dict(row) for row in rows]

    def execution_trace_summary(self) -> dict[str, Any]:
        if not self._available:
            return {
                "available": False,
                "total_count": 0,
                "training_eligible_count": 0,
                "failure_audit_count": 0,
                "source_trace_count": 0,
                "replay_trace_count": 0,
                "distinct_skill_count": 0,
                "replay_readiness": self.replay_readiness_summary(),
                "recent": [],
            }
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    COUNT(*) AS total_count,
                    SUM(CASE WHEN training_eligible = 1 THEN 1 ELSE 0 END) AS training_eligible_count,
                    SUM(CASE WHEN retention_class = 'FAILURE_AUDIT' THEN 1 ELSE 0 END) AS failure_audit_count,
                    SUM(CASE WHEN source_trace_id IS NULL THEN 1 ELSE 0 END) AS source_trace_count,
                    SUM(CASE WHEN source_trace_id IS NOT NULL THEN 1 ELSE 0 END) AS replay_trace_count,
                    COUNT(DISTINCT skill_name) AS distinct_skill_count
                FROM execution_traces
                """
            ).fetchone()
        return {
            "available": True,
            "total_count": int(row["total_count"] or 0),
            "training_eligible_count": int(row["training_eligible_count"] or 0),
            "failure_audit_count": int(row["failure_audit_count"] or 0),
            "source_trace_count": int(row["source_trace_count"] or 0),
            "replay_trace_count": int(row["replay_trace_count"] or 0),
            "distinct_skill_count": int(row["distinct_skill_count"] or 0),
            "replay_readiness": self.replay_readiness_summary(),
            "recent": self.list_execution_traces(limit=3),
        }

    def replay_readiness_summary(self) -> dict[str, Any]:
        if not self._available:
            return {
                "available": False,
                "status": "UNAVAILABLE",
                "enforcement_mode": REPLAY_ENFORCEMENT_MODE,
                "operator_ack_required_below_threshold": True,
                "minimum_eligible_traces": REPLAY_ACTIVATION_MIN_ELIGIBLE_TRACES,
                "minimum_known_bad_traces": REPLAY_ACTIVATION_MIN_KNOWN_BAD_TRACES,
                "minimum_distinct_skills": REPLAY_ACTIVATION_MIN_DISTINCT_SKILLS,
                "sample_target": DEFAULT_REPLAY_SAMPLE_TARGET,
                "eligible_source_traces": 0,
                "known_bad_source_traces": 0,
                "distinct_skill_count": 0,
                "blockers": ["telemetry_unavailable"],
                "skill_coverage": [],
            }
        with self._connect() as conn:
            counts = conn.execute(
                f"""
                SELECT
                    SUM(
                        CASE
                            WHEN source_trace_id IS NULL
                             AND training_eligible = 1
                             AND judge_verdict = 'PASS'
                             AND {_activation_role_sql()}
                            THEN 1 ELSE 0
                        END
                    ) AS eligible_source_traces,
                    SUM(
                        CASE
                            WHEN source_trace_id IS NULL
                             AND {_activation_role_sql()}
                             AND (training_eligible = 0 OR judge_verdict != 'PASS' OR retention_class = 'FAILURE_AUDIT')
                            THEN 1 ELSE 0
                        END
                    ) AS known_bad_source_traces,
                    COUNT(DISTINCT CASE WHEN source_trace_id IS NULL AND {_activation_role_sql()} THEN skill_name END) AS distinct_skill_count
                FROM execution_traces
                """
            ).fetchone()
            coverage_rows = conn.execute(
                f"""
                SELECT
                    skill_name,
                    SUM(
                        CASE
                            WHEN source_trace_id IS NULL
                             AND training_eligible = 1
                             AND judge_verdict = 'PASS'
                             AND {_activation_role_sql()}
                            THEN 1 ELSE 0
                        END
                    ) AS eligible_source_traces,
                    SUM(
                        CASE
                            WHEN source_trace_id IS NULL
                             AND {_activation_role_sql()}
                             AND (training_eligible = 0 OR judge_verdict != 'PASS' OR retention_class = 'FAILURE_AUDIT')
                            THEN 1 ELSE 0
                        END
                    ) AS known_bad_source_traces
                FROM execution_traces
                WHERE source_trace_id IS NULL AND {_activation_role_sql()}
                GROUP BY skill_name
                ORDER BY eligible_source_traces DESC, known_bad_source_traces DESC, skill_name ASC
                LIMIT 10
                """
            ).fetchall()
        eligible = int(counts["eligible_source_traces"] or 0)
        known_bad = int(counts["known_bad_source_traces"] or 0)
        distinct_skills = int(counts["distinct_skill_count"] or 0)
        blockers: list[str] = []
        if eligible < REPLAY_ACTIVATION_MIN_ELIGIBLE_TRACES:
            blockers.append(f"eligible_traces {eligible}/{REPLAY_ACTIVATION_MIN_ELIGIBLE_TRACES}")
        if known_bad < REPLAY_ACTIVATION_MIN_KNOWN_BAD_TRACES:
            blockers.append(f"known_bad_traces {known_bad}/{REPLAY_ACTIVATION_MIN_KNOWN_BAD_TRACES}")
        if distinct_skills < REPLAY_ACTIVATION_MIN_DISTINCT_SKILLS:
            blockers.append(f"distinct_skills {distinct_skills}/{REPLAY_ACTIVATION_MIN_DISTINCT_SKILLS}")
        return {
            "available": True,
            "status": "READY_FOR_BROADER_REPLAY" if not blockers else "IMPLEMENTED_BELOW_ACTIVATION_THRESHOLD",
            "enforcement_mode": REPLAY_ENFORCEMENT_MODE,
            "operator_ack_required_below_threshold": True,
            "minimum_eligible_traces": REPLAY_ACTIVATION_MIN_ELIGIBLE_TRACES,
            "minimum_known_bad_traces": REPLAY_ACTIVATION_MIN_KNOWN_BAD_TRACES,
            "minimum_distinct_skills": REPLAY_ACTIVATION_MIN_DISTINCT_SKILLS,
            "sample_target": DEFAULT_REPLAY_SAMPLE_TARGET,
            "eligible_source_traces": eligible,
            "known_bad_source_traces": known_bad,
            "distinct_skill_count": distinct_skills,
            "blockers": blockers,
            "skill_coverage": [
                {
                    "skill_name": row["skill_name"],
                    "eligible_source_traces": int(row["eligible_source_traces"] or 0),
                    "known_bad_source_traces": int(row["known_bad_source_traces"] or 0),
                }
                for row in coverage_rows
            ],
        }

    def propose_variant(
        self,
        *,
        skill_name: str,
        parent_version: str,
        diff: str,
        source: str,
        prompt_prelude: str = "",
        retrieval_strategy_diff: str = "",
        scoring_formula_diff: str = "",
        context_assembly_diff: str = "",
        touches_infrastructure: bool = False,
        reference_time: str | None = None,
    ) -> dict[str, Any]:
        if not self._available:
            raise RuntimeError("Harness variant tables are not available")
        now = self._now(reference_time)
        status = "PROPOSED"
        reject_reason = None
        if touches_infrastructure:
            status = "REJECTED"
            reject_reason = "SCOPE_VIOLATION"
        elif self._has_active_variant(skill_name):
            status = "REJECTED"
            reject_reason = "CONCURRENT_VARIANT"
        elif self._variant_created_since(skill_name, _to_iso(_parse_ts(now) - datetime.timedelta(hours=24))):
            status = "REJECTED"
            reject_reason = "RATE_LIMITED"

        variant_id = str(uuid.uuid4())
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO harness_variants (
                    variant_id, skill_name, parent_version, diff, source, status,
                    prompt_prelude, retrieval_strategy_diff, scoring_formula_diff,
                    context_assembly_diff, touches_infrastructure, reject_reason,
                    eval_result_json, promoted_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    variant_id,
                    skill_name,
                    parent_version,
                    diff,
                    source,
                    status,
                    prompt_prelude,
                    retrieval_strategy_diff,
                    scoring_formula_diff,
                    context_assembly_diff,
                    1 if touches_infrastructure else 0,
                    reject_reason,
                    None,
                    None,
                    now,
                    now,
                ),
            )
            conn.commit()
            row = conn.execute(
                "SELECT * FROM harness_variants WHERE variant_id = ? LIMIT 1",
                (variant_id,),
            ).fetchone()
        assert row is not None
        return self._variant_row_to_dict(row)

    def start_shadow_eval(self, variant_id: str, *, reference_time: str | None = None) -> dict[str, Any]:
        if not self._available:
            raise RuntimeError("Harness variant tables are not available")
        now = self._now(reference_time)
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM harness_variants WHERE variant_id = ? LIMIT 1",
                (variant_id,),
            ).fetchone()
            if row is None:
                raise KeyError(variant_id)
            if row["status"] != "PROPOSED":
                return self._variant_row_to_dict(row)
            conn.execute(
                """
                UPDATE harness_variants
                SET status = 'SHADOW_EVAL',
                    updated_at = ?
                WHERE variant_id = ?
                """,
                (now, variant_id),
            )
            conn.commit()
            updated = conn.execute(
                "SELECT * FROM harness_variants WHERE variant_id = ? LIMIT 1",
                (variant_id,),
            ).fetchone()
        assert updated is not None
        return self._variant_row_to_dict(updated)

    def record_eval_result(
        self,
        variant_id: str,
        eval_result: VariantEvalResult,
        *,
        reference_time: str | None = None,
    ) -> dict[str, Any]:
        if not self._available:
            raise RuntimeError("Harness variant tables are not available")
        now = self._now(reference_time)
        status = "PROMOTED" if eval_result.all_gates_pass else "REJECTED"
        promoted_at = now if eval_result.all_gates_pass else None
        reject_reason = None if eval_result.all_gates_pass else "EVAL_GATE_FAILED"
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM harness_variants WHERE variant_id = ? LIMIT 1",
                (variant_id,),
            ).fetchone()
            if row is None:
                raise KeyError(variant_id)
            conn.execute(
                """
                UPDATE harness_variants
                SET status = ?,
                    reject_reason = COALESCE(?, reject_reason),
                    eval_result_json = ?,
                    promoted_at = ?,
                    updated_at = ?
                WHERE variant_id = ?
                """,
                (
                    status,
                    reject_reason,
                    _json(asdict(eval_result)),
                    promoted_at,
                    now,
                    variant_id,
                ),
            )
            conn.commit()
            updated = conn.execute(
                "SELECT * FROM harness_variants WHERE variant_id = ? LIMIT 1",
                (variant_id,),
            ).fetchone()
        assert updated is not None
        return self._variant_row_to_dict(updated)

    def evaluate_variant_from_traces(
        self,
        variant_id: str,
        *,
        benchmark_name: str | None = None,
        sample_size: int = 50,
        minimum_trace_count: int = 3,
        minimum_known_bad_traces: int = 1,
        known_bad_score_threshold: float = 0.35,
        per_trace_cost_cu: float = 0.05,
        allow_below_activation_threshold: bool = False,
        reference_time: str | None = None,
    ) -> dict[str, Any]:
        if not self._available:
            raise RuntimeError("Harness variant tables are not available")
        if sample_size <= 0:
            raise ValueError("sample_size must be positive")
        if minimum_trace_count <= 0:
            raise ValueError("minimum_trace_count must be positive")
        if minimum_known_bad_traces < 0:
            raise ValueError("minimum_known_bad_traces must be non-negative")

        now = self._now(reference_time)
        variant = self.get_variant(variant_id)
        if variant["status"] == "PROPOSED":
            variant = self.start_shadow_eval(variant_id, reference_time=now)
        elif variant["status"] != "SHADOW_EVAL":
            raise ValueError(f"Variant {variant_id} is not eligible for shadow replay: {variant['status']}")
        replay_readiness = self.replay_readiness_summary()
        replay_blockers = list(replay_readiness["blockers"])
        if replay_readiness["status"] != "READY_FOR_BROADER_REPLAY" and not allow_below_activation_threshold:
            raise ValueError(
                "Replay readiness below activation threshold; explicit operator acknowledgement is required: "
                + ", ".join(replay_blockers)
            )

        start = time.perf_counter()
        baseline_rows = self._select_replay_traces(
            skill_name=variant["skill_name"],
            limit=sample_size,
            reference_time=now,
            known_bad=False,
        )
        known_bad_rows = self._select_replay_traces(
            skill_name=variant["skill_name"],
            limit=sample_size,
            reference_time=now,
            known_bad=True,
        )
        profile = self._variant_replay_profile(variant)

        baseline_scores = [float(row["outcome_score"]) for row in baseline_rows]
        variant_scores: list[float] = []
        regressed = 0
        improved = 0
        for row in baseline_rows:
            replay = self._replay_variant_score(row, profile)
            variant_scores.append(replay["score"])
            if replay["score"] + 0.005 < float(row["outcome_score"]):
                regressed += 1
            elif replay["score"] > float(row["outcome_score"]) + 0.005:
                improved += 1
            self._log_replay_trace(
                variant=variant,
                source_trace=row,
                replay_score=replay["score"],
                known_bad=False,
                blocked=None,
                profile=profile,
                reference_time=now,
            )

        blocked_count = 0
        for row in known_bad_rows:
            replay = self._replay_variant_score(row, profile)
            blocked = replay["score"] <= known_bad_score_threshold
            if blocked:
                blocked_count += 1
            self._log_replay_trace(
                variant=variant,
                source_trace=row,
                replay_score=replay["score"],
                known_bad=True,
                blocked=blocked,
                profile=profile,
                reference_time=now,
            )

        traces_evaluated = len(baseline_scores)
        regression_rate = regressed / traces_evaluated if traces_evaluated else 1.0
        baseline_mean = _mean(baseline_scores)
        variant_mean = _mean(variant_scores)
        quality_delta = variant_mean - baseline_mean
        baseline_std = _population_std(baseline_scores)
        variant_std = _population_std(variant_scores)
        known_bad_count = len(known_bad_rows)
        known_bad_block_rate = blocked_count / known_bad_count if known_bad_count else 0.0
        gate_0_pass = traces_evaluated >= minimum_trace_count and regression_rate <= 0.03
        gate_1_pass = (
            known_bad_count >= minimum_known_bad_traces and known_bad_block_rate >= 1.0
        )
        gate_2_pass = traces_evaluated >= minimum_trace_count and quality_delta > 0.0
        gate_3_pass = traces_evaluated >= minimum_trace_count and (
            variant_std <= baseline_std + max(0.01, baseline_std)
        )
        eval_duration_ms = max(1, int((time.perf_counter() - start) * 1000))

        result = VariantEvalResult(
            variant_id=variant_id,
            skill_name=variant["skill_name"],
            benchmark_name=benchmark_name or f"shadow_replay_{variant['skill_name']}",
            baseline_outcome_scores=baseline_scores,
            variant_outcome_scores=variant_scores,
            regression_rate=regression_rate,
            gate_0_pass=gate_0_pass,
            known_bad_block_rate=known_bad_block_rate,
            gate_1_pass=gate_1_pass,
            baseline_mean_score=baseline_mean,
            variant_mean_score=variant_mean,
            quality_delta=quality_delta,
            gate_2_pass=gate_2_pass,
            baseline_std=baseline_std,
            variant_std=variant_std,
            gate_3_pass=gate_3_pass,
            regressed_trace_count=regressed,
            improved_trace_count=improved,
            net_trace_gain=improved - regressed,
            traces_evaluated=traces_evaluated,
            compute_cost_cu=round((traces_evaluated + known_bad_count) * per_trace_cost_cu, 6),
            eval_duration_ms=eval_duration_ms,
            replay_readiness_status=replay_readiness["status"],
            replay_readiness_blockers=replay_blockers,
            operator_acknowledged_below_threshold=bool(
                replay_readiness["status"] != "READY_FOR_BROADER_REPLAY" and allow_below_activation_threshold
            ),
            created_at=now,
        )
        return self.record_eval_result(variant_id, result, reference_time=now)

    def list_variants(
        self,
        *,
        limit: int = 20,
        skill_name: str | None = None,
        status: str | None = None,
    ) -> list[dict[str, Any]]:
        if not self._available:
            return []
        where: list[str] = []
        params: list[object] = []
        if skill_name is not None:
            where.append("skill_name = ?")
            params.append(skill_name)
        if status is not None:
            where.append("status = ?")
            params.append(status)
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT *
                FROM harness_variants
                {where_sql}
                ORDER BY created_at DESC, variant_id DESC
                LIMIT ?
                """,
                (*params, limit),
            ).fetchall()
        return [self._variant_row_to_dict(row) for row in rows]

    def frontier(self, *, limit: int = 20, skill_name: str | None = None) -> list[dict[str, Any]]:
        if not self._available:
            return []
        where_sql = ""
        params: list[object] = []
        if skill_name is not None:
            where_sql = "WHERE skill_name = ?"
            params.append(skill_name)
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT *
                FROM harness_frontier
                {where_sql}
                ORDER BY promoted_at DESC, variant_id DESC
                LIMIT ?
                """,
                (*params, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def summary(self, *, reference_time: str | None = None) -> dict[str, Any]:
        if not self._available:
            return {
                "available": False,
                "active_count": 0,
                "proposed_count": 0,
                "shadow_eval_count": 0,
                "promoted_count": 0,
                "rejected_24h": 0,
                "frontier": [],
                "recent": [],
            }
        now = (
            _parse_ts(reference_time)
            if reference_time is not None
            else datetime.datetime.now(datetime.timezone.utc)
        )
        cutoff = _to_iso(now - datetime.timedelta(hours=24))
        with self._connect() as conn:
            counts = conn.execute(
                """
                SELECT
                    SUM(CASE WHEN status = 'PROPOSED' THEN 1 ELSE 0 END) AS proposed_count,
                    SUM(CASE WHEN status = 'SHADOW_EVAL' THEN 1 ELSE 0 END) AS shadow_eval_count,
                    SUM(CASE WHEN status = 'PROMOTED' THEN 1 ELSE 0 END) AS promoted_count,
                    SUM(CASE WHEN status = 'REJECTED' AND created_at >= ? THEN 1 ELSE 0 END) AS rejected_24h
                FROM harness_variants
                """,
                (cutoff,),
            ).fetchone()
        proposed_count = int(counts["proposed_count"] or 0)
        shadow_eval_count = int(counts["shadow_eval_count"] or 0)
        return {
            "available": True,
            "active_count": proposed_count + shadow_eval_count,
            "proposed_count": proposed_count,
            "shadow_eval_count": shadow_eval_count,
            "promoted_count": int(counts["promoted_count"] or 0),
            "rejected_24h": int(counts["rejected_24h"] or 0),
            "frontier": self.frontier(limit=3),
            "recent": self.list_variants(limit=3),
        }

    def _has_active_variant(self, skill_name: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM harness_variants
                WHERE skill_name = ? AND status IN ('PROPOSED','SHADOW_EVAL')
                LIMIT 1
                """,
                (skill_name,),
            ).fetchone()
        return row is not None

    def _variant_created_since(self, skill_name: str, cutoff: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT 1
                FROM harness_variants
                WHERE skill_name = ? AND created_at > ?
                LIMIT 1
                """,
                (skill_name, cutoff),
            ).fetchone()
        return row is not None

    def _select_replay_traces(
        self,
        *,
        skill_name: str,
        limit: int,
        reference_time: str,
        known_bad: bool,
    ) -> list[dict[str, Any]]:
        if known_bad:
            where_sql = (
                "skill_name = ? AND source_trace_id IS NULL AND created_at <= ? "
                f"AND {_activation_role_sql()} "
                "AND (training_eligible = 0 OR judge_verdict != 'PASS' OR retention_class = 'FAILURE_AUDIT')"
            )
        else:
            where_sql = (
                "skill_name = ? AND training_eligible = 1 AND judge_verdict = 'PASS' "
                f"AND {_activation_role_sql()} "
                "AND source_trace_id IS NULL AND created_at <= ?"
            )
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT *
                FROM execution_traces
                WHERE {where_sql}
                ORDER BY created_at DESC, trace_id DESC
                LIMIT ?
                """,
                (skill_name, reference_time, limit),
            ).fetchall()
        return [self._trace_row_to_dict(row) for row in rows]

    def _variant_replay_profile(self, variant: dict[str, Any]) -> dict[str, float]:
        combined = " ".join(
            (
                variant["diff"],
                variant["prompt_prelude"],
                variant["retrieval_strategy_diff"],
                variant["scoring_formula_diff"],
                variant["context_assembly_diff"],
            )
        ).lower()
        return {
            "general_boost": min(0.08, self._cue_weight(combined, _POSITIVE_VARIANT_CUES)),
            "retrieval_boost": min(0.08, self._cue_weight(combined, _RETRIEVAL_VARIANT_CUES)),
            "context_boost": min(0.08, self._cue_weight(combined, _CONTEXT_VARIANT_CUES)),
            "scoring_boost": min(0.08, self._cue_weight(combined, _SCORING_VARIANT_CUES)),
            "risk_penalty": min(0.25, self._cue_weight(combined, _RISK_VARIANT_CUES)),
        }

    def _replay_variant_score(
        self,
        trace: dict[str, Any],
        profile: dict[str, float],
    ) -> dict[str, float]:
        baseline = float(trace["outcome_score"])
        query_factor = min(1.0, len(trace["retrieval_queries"]) / 3.0)
        context_factor = min(1.0, len(trace["context_assembled"]) / 600.0)
        step_factor = min(1.0, len(trace["steps"]) / 8.0)
        quality_headroom = max(0.0, 1.0 - baseline)
        calibration_zone = 1.0 - min(1.0, abs(0.7 - baseline) / 0.7)

        delta = 0.0
        delta += profile["general_boost"] * (0.35 + quality_headroom * 0.65)
        delta += profile["retrieval_boost"] * (0.2 + query_factor * 0.8)
        delta += profile["context_boost"] * (0.2 + context_factor * 0.8)
        delta += profile["scoring_boost"] * (0.3 + calibration_zone * 0.7)
        delta -= profile["risk_penalty"] * (0.45 + step_factor * 0.25 + query_factor * 0.15)

        if trace["judge_verdict"] != "PASS" or trace["retention_class"] == "FAILURE_AUDIT":
            delta -= profile["risk_penalty"] * 0.35

        return {
            "score": _clamp(baseline + delta),
            "delta": delta,
        }

    def _log_replay_trace(
        self,
        *,
        variant: dict[str, Any],
        source_trace: dict[str, Any],
        replay_score: float,
        known_bad: bool,
        blocked: bool | None,
        profile: dict[str, float],
        reference_time: str,
    ) -> None:
        if source_trace["source_trace_id"] is not None:
            return
        reason = (
            "Known-bad replay remained blocked."
            if blocked is True
            else "Known-bad replay escaped expected block threshold."
            if blocked is False
            else "Shadow replay completed."
        )
        judge_verdict = (
            "PASS" if blocked is not False else "FAIL"
        ) if known_bad else ("PASS" if replay_score >= source_trace["outcome_score"] else "FAIL")
        retention_class = "FAILURE_AUDIT" if judge_verdict == "FAIL" else "STANDARD"
        training_eligible = False
        step_payload = {
            "source_trace_id": source_trace["trace_id"],
            "variant_id": variant["variant_id"],
            "replay_score": round(replay_score, 6),
            "baseline_score": round(float(source_trace["outcome_score"]), 6),
            "known_bad": known_bad,
            "blocked": blocked,
            "profile": {key: round(value, 6) for key, value in profile.items()},
        }
        replay_trace = ExecutionTrace(
            trace_id=str(uuid.uuid4()),
            task_id=f"shadow-eval-{variant['variant_id']}",
            role="harness_shadow_eval_known_bad" if known_bad else "harness_shadow_eval",
            skill_name=variant["skill_name"],
            harness_version=variant["variant_id"],
            intent_goal=f"Replay variant {variant['variant_id']} against archived execution evidence.",
            steps=[
                ExecutionTraceStep(
                    step_index=1,
                    tool_call="harness_variants.shadow_replay",
                    tool_result=_json(step_payload),
                    tool_result_file=None,
                    tokens_in=0,
                    tokens_out=0,
                    latency_ms=0,
                    model_used="shadow-replay",
                )
            ],
            prompt_template=variant["prompt_prelude"] or variant["diff"],
            context_assembled=source_trace["context_assembled"],
            retrieval_queries=source_trace["retrieval_queries"],
            judge_verdict=judge_verdict,
            judge_reasoning=reason,
            outcome_score=replay_score,
            cost_usd=0.0,
            duration_ms=0,
            training_eligible=training_eligible,
            retention_class=retention_class,
            source_chain_id=source_trace["source_chain_id"],
            source_session_id=source_trace["source_session_id"],
            source_trace_id=source_trace["trace_id"],
            created_at=reference_time,
        )
        self.log_execution_trace(replay_trace)

    @staticmethod
    def _cue_weight(text: str, cues: dict[str, float]) -> float:
        return sum(weight for cue, weight in cues.items() if cue in text)

    def _trace_row_to_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "trace_id": row["trace_id"],
            "task_id": row["task_id"],
            "role": row["role"],
            "skill_name": row["skill_name"],
            "harness_version": row["harness_version"],
            "intent_goal": row["intent_goal"],
            "steps": json.loads(row["steps_json"]),
            "prompt_template": row["prompt_template"],
            "context_assembled": row["context_assembled"],
            "retrieval_queries": json.loads(row["retrieval_queries_json"]),
            "judge_verdict": row["judge_verdict"],
            "judge_reasoning": row["judge_reasoning"],
            "outcome_score": float(row["outcome_score"]),
            "cost_usd": float(row["cost_usd"]),
            "duration_ms": int(row["duration_ms"]),
            "training_eligible": bool(row["training_eligible"]),
            "retention_class": row["retention_class"],
            "source_chain_id": row["source_chain_id"],
            "source_session_id": row["source_session_id"],
            "source_trace_id": row["source_trace_id"],
            "created_at": row["created_at"],
        }

    def _variant_row_to_dict(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "variant_id": row["variant_id"],
            "skill_name": row["skill_name"],
            "parent_version": row["parent_version"],
            "diff": row["diff"],
            "source": row["source"],
            "status": row["status"],
            "prompt_prelude": row["prompt_prelude"],
            "retrieval_strategy_diff": row["retrieval_strategy_diff"],
            "scoring_formula_diff": row["scoring_formula_diff"],
            "context_assembly_diff": row["context_assembly_diff"],
            "touches_infrastructure": bool(row["touches_infrastructure"]),
            "reject_reason": row["reject_reason"],
            "eval_result": None if row["eval_result_json"] is None else json.loads(row["eval_result_json"]),
            "promoted_at": row["promoted_at"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._telemetry_db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def _verify_tables(self) -> bool:
        try:
            with self._connect() as conn:
                rows = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
        except sqlite3.DatabaseError:
            return False
        present = {row["name"] for row in rows}
        return _REQUIRED_TABLES.issubset(present)

    @staticmethod
    def _now(reference_time: str | None) -> str:
        if reference_time:
            return _to_iso(_parse_ts(reference_time))
        return _to_iso(datetime.datetime.now(datetime.timezone.utc))
