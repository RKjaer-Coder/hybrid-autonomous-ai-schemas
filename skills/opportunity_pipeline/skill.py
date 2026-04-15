from __future__ import annotations

import datetime
import json
import uuid
from dataclasses import asdict, dataclass
from typing import Any, Optional

from council.context_budget import build_context_packet
from council.types import DEFAULT_ROLE_WEIGHTS, DecisionType, Recommendation
from skills.db_manager import DatabaseManager


VALID_OPPORTUNITY_TRANSITIONS = {
    "DETECTED": {"SCREENED"},
    "SCREENED": {"REJECTED", "DEFERRED", "QUALIFIED"},
    "DEFERRED": {"SCREENED", "REJECTED"},
    "QUALIFIED": {"IN_VALIDATION"},
    "IN_VALIDATION": {"GO_NO_GO", "REJECTED"},
    "GO_NO_GO": {"ACTIVE", "REJECTED", "PAUSED"},
    "PAUSED": {"ACTIVE", "REJECTED"},
    "ACTIVE": {"CLOSED"},
    "REJECTED": {"DETECTED"},
    "CLOSED": set(),
}

PHASE_GATE_VERDICTS = {"CONTINUE", "PIVOT", "PAUSE", "KILL_RECOMMEND"}
PHASE_GATE_TRIGGER_TO_GATE = {
    "SUCCESS_CRITERIA_MET": "G1",
    "BUDGET_EXHAUSTED": "G1",
    "SPEND_CEILING": "G3",
    "BLOCKER": "G1",
}


@dataclass(frozen=True)
class OpportunityRecord:
    opportunity_id: str
    income_mechanism: str
    title: str
    thesis: str
    detected_by: str
    council_verdict_id: str | None
    validation_spend: float
    validation_report: str | None
    cashflow_estimate: dict[str, Any]
    status: str
    project_id: str | None
    learning_record: dict[str, Any] | None
    provenance_links: list[str]
    provenance_degraded: bool
    trust_tier: int
    created_at: str
    updated_at: str


class OpportunityPipelineSkill:
    def __init__(self, db_manager: DatabaseManager):
        self._db = db_manager

    def create_opportunity(
        self,
        title: str,
        thesis: str,
        income_mechanism: str = "software_product",
        *,
        detected_by: str = "operator",
        cashflow_estimate: dict[str, Any] | None = None,
        provenance_links: list[str] | None = None,
        trust_tier: int = 2,
    ) -> str:
        opportunity_id = str(uuid.uuid4())
        now = self._utc_now()
        conn = self._db.get_connection("strategic_memory")
        conn.execute(
            """
            INSERT INTO opportunity_records (
                opportunity_id, income_mechanism, title, thesis, detected_by,
                council_verdict_id, validation_spend, validation_report, cashflow_estimate,
                status, project_id, learning_record, provenance_links, provenance_degraded,
                trust_tier, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                opportunity_id,
                income_mechanism,
                title,
                thesis,
                detected_by,
                None,
                0.0,
                None,
                json.dumps(cashflow_estimate or {"low": 0, "mid": 0, "high": 0, "currency": "USD", "period": "month"}),
                "DETECTED",
                None,
                None,
                json.dumps(provenance_links or []),
                0,
                trust_tier,
                now,
                now,
            ),
        )
        conn.commit()
        return opportunity_id

    def get_opportunity(self, opportunity_id: str) -> dict[str, Any]:
        return self._fetch_opportunity(opportunity_id)

    def list_opportunities(self, *, status: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
        conn = self._db.get_connection("strategic_memory")
        where = "WHERE status = ?" if status else ""
        params: tuple[object, ...] = (status, limit) if status else (limit,)
        rows = conn.execute(
            f"""
            SELECT
                opportunity_id, income_mechanism, title, thesis, detected_by,
                council_verdict_id, validation_spend, validation_report, cashflow_estimate,
                status, project_id, learning_record, provenance_links, provenance_degraded,
                trust_tier, created_at, updated_at
            FROM opportunity_records
            {where}
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
        return [self._row_to_opportunity(row) for row in rows]

    def transition_opportunity(
        self,
        opportunity_id: str,
        new_status: str,
        *,
        council_verdict_id: str | None = None,
        validation_report: str | None = None,
        validation_spend: float | None = None,
        learning_record: dict[str, Any] | None = None,
        project_id: str | None = None,
    ) -> dict[str, Any]:
        now = self._utc_now()
        conn = self._db.get_connection("strategic_memory")
        row = conn.execute("SELECT * FROM opportunity_records WHERE opportunity_id = ?", (opportunity_id,)).fetchone()
        if row is None:
            raise KeyError(opportunity_id)
        current_status = row["status"]
        if new_status not in VALID_OPPORTUNITY_TRANSITIONS.get(current_status, set()):
            raise ValueError(f"invalid transition {current_status} -> {new_status}")
        conn.execute(
            """
            UPDATE opportunity_records
            SET status = ?,
                council_verdict_id = COALESCE(?, council_verdict_id),
                validation_report = COALESCE(?, validation_report),
                validation_spend = COALESCE(?, validation_spend),
                learning_record = COALESCE(?, learning_record),
                project_id = COALESCE(?, project_id),
                updated_at = ?
            WHERE opportunity_id = ?
            """,
            (
                new_status,
                council_verdict_id,
                validation_report,
                validation_spend,
                None if learning_record is None else json.dumps(learning_record),
                project_id,
                now,
                opportunity_id,
            ),
        )
        conn.commit()
        return self._fetch_opportunity(opportunity_id)

    def handoff_to_project(
        self,
        opportunity_id: str,
        *,
        project_name: str | None = None,
        success_criteria: dict[str, Any] | None = None,
        compute_budget: dict[str, Any] | None = None,
        portfolio_weight: float = 0.20,
    ) -> dict[str, Any]:
        now = self._utc_now()
        strategic = self._db.get_connection("strategic_memory")
        financial = self._db.get_connection("financial_ledger")
        row = strategic.execute(
            "SELECT * FROM opportunity_records WHERE opportunity_id = ?",
            (opportunity_id,),
        ).fetchone()
        if row is None:
            raise KeyError(opportunity_id)
        if row["status"] not in {"GO_NO_GO", "PAUSED"}:
            raise ValueError(f"opportunity must be GO_NO_GO or PAUSED before handoff, got {row['status']}")
        project_id = str(uuid.uuid4())
        financial.execute(
            """
            INSERT INTO projects (
                project_id, opportunity_id, name, income_mechanism, thesis,
                success_criteria, compute_budget, portfolio_weight, status,
                kill_score_watch, cashflow_actual_usd, council_verdict_id,
                pivot_log, created_at, closed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                project_id,
                opportunity_id,
                project_name or row["title"],
                row["income_mechanism"],
                row["thesis"],
                json.dumps(success_criteria or {
                    "primary": "cashflow_target",
                    "cashflow_target_usd": row["cashflow_estimate"] and json.loads(row["cashflow_estimate"]).get("mid", 0),
                    "cashflow_period": "monthly",
                    "secondary": ["hypothesis_validated", "asset_created"],
                }),
                json.dumps(compute_budget or {"max_executor_hours": 40, "max_cloud_spend_usd": 20, "alert_at_pct": 0.75}),
                portfolio_weight,
                "ACTIVE",
                0,
                0.0,
                row["council_verdict_id"],
                json.dumps([]),
                now,
                None,
            ),
        )
        phases = [
            ("VALIDATE", "ACTIVE", 0, "De-risk thesis before full build.", ["validation report", "updated cashflow estimate"], now, None, None),
            ("BUILD", "PENDING", 1, "Produce primary artifact.", ["primary artifact"], None, None, None),
            ("DEPLOY", "PENDING", 2, "Get artifact into the world.", ["deployed artifact"], None, None, None),
            ("OPERATE", "PENDING", 3, "Monitor and optimize for income.", ["revenue reports"], None, None, None),
        ]
        for name, status, sequence, scope, outputs, started_at, gate_triggered_at, completed_at in phases:
            financial.execute(
                """
                INSERT INTO phases (
                    phase_id, project_id, name, status, sequence, scope,
                    success_criteria, compute_budget, compute_consumed, outputs,
                    gate_result, started_at, gate_triggered_at, completed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    project_id,
                    name,
                    status,
                    sequence,
                    scope,
                    json.dumps(outputs),
                    json.dumps({"executor_hours_cap": 10, "cloud_spend_cap_usd": 0}),
                    json.dumps({"executor_hours": 0, "cloud_spend_usd": 0}),
                    json.dumps([]),
                    None,
                    started_at,
                    gate_triggered_at,
                    completed_at,
                ),
            )
        strategic.execute(
            """
            UPDATE opportunity_records
            SET status = 'ACTIVE',
                project_id = ?,
                updated_at = ?
            WHERE opportunity_id = ?
            """,
            (project_id, now, opportunity_id),
        )
        financial.commit()
        strategic.commit()
        return {
            "opportunity": self._fetch_opportunity(opportunity_id),
            "project_id": project_id,
        }

    def trigger_phase_gate(
        self,
        project_id: str,
        trigger: str,
        *,
        outputs_summary: str | None = None,
        cashflow_forecast_usd: float | None = None,
        kill_score_current: float | None = None,
        kill_signals: list[dict[str, Any]] | None = None,
        gate_type: str | None = None,
    ) -> dict[str, Any]:
        now = self._utc_now()
        financial = self._db.get_connection("financial_ledger")
        operator = self._db.get_connection("operator_digest")
        project, phase = self._fetch_project_and_phase(financial, project_id)
        context_packet, council_tier = self._assemble_phase_gate_context_packet(
            financial,
            project,
            phase,
            trigger=trigger,
            outputs_summary=outputs_summary,
            cashflow_forecast_usd=cashflow_forecast_usd,
            kill_score_current=kill_score_current,
            kill_signals=kill_signals,
        )
        gate_id = str(uuid.uuid4())
        effective_gate_type = gate_type or PHASE_GATE_TRIGGER_TO_GATE.get(trigger, "G1")
        financial.execute(
            """
            UPDATE phases
            SET status = 'GATE_PENDING',
                gate_triggered_at = COALESCE(gate_triggered_at, ?)
            WHERE phase_id = ?
            """,
            (now, phase["phase_id"]),
        )
        financial.execute(
            """
            UPDATE projects
            SET status = 'PAUSED'
            WHERE project_id = ?
            """,
            (project_id,),
        )
        operator.execute(
            """
            INSERT INTO gate_log (
                gate_id, gate_type, trigger_description, context_packet, project_id,
                status, timeout_hours, operator_response, created_at, responded_at, expires_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                gate_id,
                effective_gate_type,
                f"phase_gate:{phase['name']}:{trigger}",
                json.dumps(context_packet),
                project_id,
                "PENDING",
                24.0 if effective_gate_type == "G1" else 6.0 if effective_gate_type == "G3" else 48.0,
                None,
                now,
                None,
                self._offset_hours(now, 24.0 if effective_gate_type == "G1" else 6.0 if effective_gate_type == "G3" else 48.0),
            ),
        )
        financial.commit()
        operator.commit()
        return {
            "gate_id": gate_id,
            "project_id": project_id,
            "phase_id": phase["phase_id"],
            "project_status": "PAUSED",
            "phase_status": "GATE_PENDING",
            "gate_type": effective_gate_type,
            "council_tier": council_tier,
            "context_packet": context_packet,
        }

    def apply_phase_gate_verdict(
        self,
        project_id: str,
        verdict: str,
        confidence: float,
        rationale: str,
        *,
        next_phase_amendments: dict[str, Any] | None = None,
        dissent_log: list[str] | None = None,
        gate_id: str | None = None,
        new_thesis: str | None = None,
        failure_analysis: str | None = None,
    ) -> dict[str, Any]:
        if verdict not in PHASE_GATE_VERDICTS:
            raise ValueError(f"invalid phase gate verdict: {verdict}")
        now = self._utc_now()
        financial = self._db.get_connection("financial_ledger")
        strategic = self._db.get_connection("strategic_memory")
        operator = self._db.get_connection("operator_digest")
        project, phase = self._fetch_project_and_phase(financial, project_id, allowed_phase_statuses=("GATE_PENDING", "ACTIVE"))
        gate_row = None
        gate_context = None
        if gate_id is not None:
            gate_row = operator.execute(
                "SELECT gate_id, context_packet FROM gate_log WHERE gate_id = ? AND project_id = ?",
                (gate_id, project_id),
            ).fetchone()
            if gate_row is not None:
                gate_context = json.loads(gate_row["context_packet"])
        council_tier = (
            gate_context["council_tier"]
            if gate_context is not None
            else self._determine_phase_gate_tier(
                trigger="SUCCESS_CRITERIA_MET",
                phase_name=phase["name"],
                sequence=phase["sequence"],
                cashflow_actual_usd=project["cashflow_actual_usd"],
                cashflow_forecast_usd=None,
                kill_score_current=None,
                prior_gate_rows=[],
                kill_signals=[],
            )
        )
        gate_result = {
            "verdict": verdict,
            "confidence": confidence,
            "rationale": rationale,
            "next_phase_amendments": next_phase_amendments or {},
            "council_tier": council_tier,
            "dissent_log": dissent_log or [],
            "applied_at": now,
        }
        financial.execute(
            "UPDATE phases SET gate_result = ? WHERE phase_id = ?",
            (json.dumps(gate_result), phase["phase_id"]),
        )
        if gate_row is not None:
            operator.execute(
                """
                UPDATE gate_log
                SET status = ?,
                    operator_response = ?,
                    responded_at = ?
                WHERE gate_id = ?
                """,
                (self._phase_gate_log_status(verdict), verdict, now, gate_id),
            )
        if verdict == "CONTINUE":
            self._complete_phase_and_progress(financial, project_id, phase, now, kill_score_watch=0.55 <= confidence < 0.65)
        elif verdict == "PIVOT":
            self._append_pivot_log(financial, project, rationale, now, new_thesis=new_thesis)
            self._complete_phase_and_progress(
                financial,
                project_id,
                phase,
                now,
                kill_score_watch=False,
                next_phase_amendments=next_phase_amendments,
            )
        elif verdict == "PAUSE":
            financial.execute(
                "UPDATE projects SET status = 'PAUSED' WHERE project_id = ?",
                (project_id,),
            )
            financial.execute(
                """
                UPDATE phases
                SET status = 'GATE_PENDING',
                    gate_triggered_at = COALESCE(gate_triggered_at, ?)
                WHERE phase_id = ?
                """,
                (now, phase["phase_id"]),
            )
        else:
            recommendation_id, g2_gate_id = self._create_kill_recommendation(
                financial,
                strategic,
                operator,
                project,
                phase,
                confidence=confidence,
                rationale=rationale,
                failure_analysis=failure_analysis or rationale,
                created_at=now,
            )
            financial.execute(
                "UPDATE projects SET status = 'KILL_RECOMMENDED' WHERE project_id = ?",
                (project_id,),
            )
            financial.commit()
            strategic.commit()
            operator.commit()
            return {
                "project": self._fetch_project(financial, project_id),
                "phase": self._fetch_phase(financial, phase["phase_id"]),
                "recommendation_id": recommendation_id,
                "g2_gate_id": g2_gate_id,
            }
        financial.commit()
        strategic.commit()
        operator.commit()
        return {
            "project": self._fetch_project(financial, project_id),
            "phase": self._fetch_phase(financial, phase["phase_id"]),
        }

    def resume_project(
        self,
        project_id: str,
        *,
        reason: str = "operator_command",
    ) -> dict[str, Any]:
        now = self._utc_now()
        financial = self._db.get_connection("financial_ledger")
        operator = self._db.get_connection("operator_digest")
        project, phase = self._fetch_project_and_phase(financial, project_id, allowed_phase_statuses=("GATE_PENDING",))
        if project["status"] != "PAUSED":
            raise ValueError(f"project must be PAUSED to resume, got {project['status']}")
        financial.execute(
            "UPDATE projects SET status = 'ACTIVE' WHERE project_id = ?",
            (project_id,),
        )
        financial.execute(
            """
            UPDATE phases
            SET status = 'ACTIVE',
                started_at = COALESCE(started_at, ?)
            WHERE phase_id = ?
            """,
            (now, phase["phase_id"]),
        )
        operator.execute(
            """
            UPDATE gate_log
            SET status = 'APPROVED',
                operator_response = ?,
                responded_at = COALESCE(responded_at, ?)
            WHERE project_id = ? AND status IN ('PENDING', 'SUSPENDED')
            """,
            (f"resume:{reason}", now, project_id),
        )
        financial.commit()
        operator.commit()
        return {
            "project": self._fetch_project(financial, project_id),
            "phase": self._fetch_phase(financial, phase["phase_id"]),
        }

    def close_from_project(
        self,
        project_id: str,
        *,
        project_status: str,
        learning_record: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if project_status not in {"COMPLETE", "KILLED"}:
            raise ValueError("project_status must be COMPLETE or KILLED")
        now = self._utc_now()
        financial = self._db.get_connection("financial_ledger")
        strategic = self._db.get_connection("strategic_memory")
        project = financial.execute(
            """
            SELECT project_id, opportunity_id, name, thesis, success_criteria,
                   cashflow_actual_usd, council_verdict_id, pivot_log
            FROM projects
            WHERE project_id = ?
            """,
            (project_id,),
        ).fetchone()
        if project is None:
            raise KeyError(project_id)
        phases = financial.execute(
            """
            SELECT name, status, gate_result, completed_at
            FROM phases
            WHERE project_id = ?
            ORDER BY sequence ASC
            """,
            (project_id,),
        ).fetchall()
        pnl = financial.execute(
            """
            SELECT revenue_to_date, direct_cost, net_to_date
            FROM project_pnl
            WHERE project_id = ?
            """,
            (project_id,),
        ).fetchone()
        assets = financial.execute(
            """
            SELECT asset_type, name, reusable, location
            FROM assets
            WHERE project_id = ?
            ORDER BY created_at ASC, name ASC
            """,
            (project_id,),
        ).fetchall()
        financial.execute(
            """
            UPDATE projects
            SET status = ?, closed_at = COALESCE(closed_at, ?)
            WHERE project_id = ?
            """,
            (project_status, now, project_id),
        )
        outcome_record = self._project_outcome_record(
            project,
            phases,
            pnl,
            assets,
            project_status=project_status,
        )
        merged_learning_record = dict(outcome_record)
        if learning_record:
            merged_learning_record.update(learning_record)
            merged_learning_record["operator_learning_record"] = learning_record
        strategic.execute(
            """
            UPDATE opportunity_records
            SET status = 'CLOSED',
                learning_record = COALESCE(?, learning_record),
                updated_at = ?
            WHERE opportunity_id = ?
            """,
            (json.dumps(merged_learning_record), now, project["opportunity_id"]),
        )
        if project["council_verdict_id"]:
            strategic.execute(
                """
                UPDATE council_verdicts
                SET outcome_record = ?
                WHERE verdict_id = ?
                """,
                (json.dumps(outcome_record), project["council_verdict_id"]),
            )
            existing = strategic.execute(
                "SELECT 1 FROM calibration_records WHERE verdict_id = ?",
                (project["council_verdict_id"],),
            ).fetchone()
            if existing is None:
                verdict = strategic.execute(
                    """
                    SELECT recommendation, decision_type
                    FROM council_verdicts
                    WHERE verdict_id = ?
                    """,
                    (project["council_verdict_id"],),
                ).fetchone()
                if verdict is not None:
                    actual_outcome = outcome_record["actual_outcome"]
                    predicted_outcome = "REJECT" if verdict["recommendation"] == Recommendation.REJECT.value else "PURSUE"
                    prediction_correct = 1.0 if (
                        (predicted_outcome == "PURSUE" and actual_outcome >= 0.5)
                        or (predicted_outcome == "REJECT" and actual_outcome == 0.0)
                    ) else 0.0
                    strategic.execute(
                        """
                        INSERT INTO calibration_records (
                            calibration_id, verdict_id, decision_type, predicted_outcome,
                            actual_outcome, prediction_correct, role_weights_used,
                            which_role_was_right, tie_break, threshold_status, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            str(uuid.uuid4()),
                            project["council_verdict_id"],
                            verdict["decision_type"],
                            predicted_outcome,
                            actual_outcome,
                            prediction_correct,
                            json.dumps({role.value: weight for role, weight in DEFAULT_ROLE_WEIGHTS.items()}),
                            None,
                            0,
                            "PROVISIONAL",
                            now,
                        ),
                    )
        financial.commit()
        strategic.commit()
        return self._fetch_opportunity(project["opportunity_id"])

    def _assemble_phase_gate_context_packet(
        self,
        financial,
        project,
        phase,
        *,
        trigger: str,
        outputs_summary: str | None,
        cashflow_forecast_usd: float | None,
        kill_score_current: float | None,
        kill_signals: list[dict[str, Any]] | None,
    ) -> tuple[dict[str, Any], str]:
        prior_gate_rows = financial.execute(
            """
            SELECT gate_result, name
            FROM phases
            WHERE project_id = ? AND gate_result IS NOT NULL AND phase_id != ?
            ORDER BY sequence DESC
            """,
            (project["project_id"], phase["phase_id"]),
        ).fetchall()
        assets = financial.execute(
            """
            SELECT asset_type, name, reusable, location
            FROM assets
            WHERE project_id = ?
            ORDER BY created_at ASC, name ASC
            """,
            (project["project_id"],),
        ).fetchall()
        fields_compressed: list[str] = []
        outputs_summary_text = outputs_summary or self._phase_outputs_summary(phase)
        limited_outputs = self._limit_tokens(outputs_summary_text, 500)
        if limited_outputs != outputs_summary_text:
            fields_compressed.append("outputs_summary")
        prior_gate_results = self._format_prior_gate_results(prior_gate_rows)
        limited_prior = self._limit_tokens(prior_gate_results, 300)
        if limited_prior != prior_gate_results:
            fields_compressed.append("prior_gate_results")
        assets_summary = self._format_assets_summary(assets)
        limited_assets = self._limit_tokens(assets_summary, 200)
        if limited_assets != assets_summary:
            fields_compressed.append("assets_created")
        kill_score_context = json.dumps(
            {
                "current": 0.0 if kill_score_current is None else kill_score_current,
                "signals": (kill_signals or [])[:2],
            },
            sort_keys=True,
        )
        limited_kill = self._limit_tokens(kill_score_context, 100)
        if limited_kill != kill_score_context:
            fields_compressed.append("kill_score_context")
        tier = self._determine_phase_gate_tier(
            trigger=trigger,
            phase_name=phase["name"],
            sequence=phase["sequence"],
            cashflow_actual_usd=project["cashflow_actual_usd"],
            cashflow_forecast_usd=cashflow_forecast_usd,
            kill_score_current=kill_score_current,
            prior_gate_rows=prior_gate_rows,
            kill_signals=kill_signals or [],
        )
        raw_context = json.dumps(
            {
                "project": {
                    "project_id": project["project_id"],
                    "name": project["name"],
                    "thesis": project["thesis"],
                    "status": project["status"],
                },
                "phase": {
                    "phase_id": phase["phase_id"],
                    "name": phase["name"],
                    "sequence": phase["sequence"],
                    "scope": phase["scope"],
                    "success_criteria": json.loads(phase["success_criteria"]),
                    "compute_budget": json.loads(phase["compute_budget"]),
                    "compute_consumed": json.loads(phase["compute_consumed"]),
                    "outputs": json.loads(phase["outputs"]),
                },
                "trigger": trigger,
                "outputs_summary": limited_outputs,
                "cashflow_actual_usd": project["cashflow_actual_usd"],
                "cashflow_forecast_usd": cashflow_forecast_usd,
                "kill_score_context": json.loads(limited_kill),
                "prior_gate_results": limited_prior,
                "assets_created": limited_assets,
            },
            sort_keys=True,
        )
        packet = build_context_packet(DecisionType.PHASE_GATE, project["project_id"], raw_context)
        return (
            {
                "project": {
                    "project_id": project["project_id"],
                    "name": project["name"],
                    "thesis": project["thesis"],
                    "status": project["status"],
                },
                "phase": {
                    "phase_id": phase["phase_id"],
                    "name": phase["name"],
                    "sequence": phase["sequence"],
                    "scope": phase["scope"],
                    "success_criteria": json.loads(phase["success_criteria"]),
                    "compute_budget": json.loads(phase["compute_budget"]),
                    "compute_consumed": json.loads(phase["compute_consumed"]),
                    "outputs": json.loads(phase["outputs"]),
                },
                "trigger": trigger,
                "outputs_summary": limited_outputs,
                "cashflow_actual_usd": project["cashflow_actual_usd"],
                "cashflow_forecast_usd": cashflow_forecast_usd,
                "kill_score_context": json.loads(limited_kill),
                "prior_gate_results": limited_prior,
                "assets_created": limited_assets,
                "budget": {
                    "token_count": packet.token_count,
                    "max_tokens": packet.max_tokens,
                    "fields_compressed": fields_compressed,
                    "council_context": packet.context_text,
                },
                "council_tier": tier,
            },
            tier,
        )

    def _complete_phase_and_progress(
        self,
        financial,
        project_id: str,
        phase,
        now: str,
        *,
        kill_score_watch: bool,
        next_phase_amendments: dict[str, Any] | None = None,
    ) -> None:
        financial.execute(
            """
            UPDATE phases
            SET status = 'COMPLETE',
                completed_at = COALESCE(completed_at, ?)
            WHERE phase_id = ?
            """,
            (now, phase["phase_id"]),
        )
        next_phase = financial.execute(
            """
            SELECT phase_id, scope
            FROM phases
            WHERE project_id = ? AND sequence = ?
            """,
            (project_id, phase["sequence"] + 1),
        ).fetchone()
        if next_phase is not None:
            scope = next_phase["scope"]
            if next_phase_amendments and next_phase_amendments.get("scope_delta"):
                scope = f"{scope} Amendment: {next_phase_amendments['scope_delta']}"
            financial.execute(
                """
                UPDATE phases
                SET status = 'ACTIVE',
                    scope = ?,
                    started_at = COALESCE(started_at, ?)
                WHERE phase_id = ?
                """,
                (scope, now, next_phase["phase_id"]),
            )
        financial.execute(
            """
            UPDATE projects
            SET status = 'ACTIVE',
                kill_score_watch = CASE WHEN ? THEN 1 ELSE kill_score_watch END
            WHERE project_id = ?
            """,
            (1 if kill_score_watch else 0, project_id),
        )

    def _append_pivot_log(
        self,
        financial,
        project,
        rationale: str,
        now: str,
        *,
        new_thesis: str | None,
    ) -> None:
        pivot_log = json.loads(project["pivot_log"])
        pivot_log.append(
            {
                "timestamp": now,
                "old_thesis": project["thesis"],
                "new_thesis": new_thesis or project["thesis"],
                "rationale": rationale,
            }
        )
        financial.execute(
            """
            UPDATE projects
            SET thesis = ?,
                pivot_log = ?
            WHERE project_id = ?
            """,
            (new_thesis or project["thesis"], json.dumps(pivot_log), project["project_id"]),
        )

    def _create_kill_recommendation(
        self,
        financial,
        strategic,
        operator,
        project,
        phase,
        *,
        confidence: float,
        rationale: str,
        failure_analysis: str,
        created_at: str,
    ) -> tuple[str, str]:
        recommendation_id = str(uuid.uuid4())
        verdict_id = str(uuid.uuid4())
        g2_gate_id = str(uuid.uuid4())
        assets = financial.execute(
            """
            SELECT asset_type, name, reusable, location
            FROM assets
            WHERE project_id = ?
            ORDER BY created_at ASC, name ASC
            """,
            (project["project_id"],),
        ).fetchall()
        asset_inventory = [
            {
                "asset_type": row["asset_type"],
                "name": row["name"],
                "reusable": bool(row["reusable"]),
                "location": row["location"],
            }
            for row in assets
        ]
        strategic.execute(
            """
            INSERT INTO council_verdicts (
                verdict_id, tier_used, decision_type, recommendation, confidence,
                reasoning_summary, dissenting_views, minority_positions,
                full_debate_record, cost_usd, project_id, outcome_record,
                da_quality_score, da_assessment, tie_break, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                verdict_id,
                2,
                DecisionType.KILL_REC.value,
                Recommendation.REJECT.value,
                confidence,
                rationale,
                None,
                json.dumps([]),
                None,
                0.0,
                project["project_id"],
                None,
                None,
                json.dumps([]),
                0,
                created_at,
            ),
        )
        financial.execute(
            """
            INSERT INTO kill_recommendations (
                recommendation_id, project_id, kill_score, council_verdict_id,
                asset_inventory, thesis_summary, failure_analysis, g2_status,
                threshold_status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                recommendation_id,
                project["project_id"],
                self._latest_kill_score(financial, project["project_id"], confidence),
                verdict_id,
                json.dumps(asset_inventory),
                self._limit_words(project["thesis"], 40),
                self._limit_words(failure_analysis, 40),
                "PENDING",
                "PROVISIONAL",
                created_at,
            ),
        )
        operator.execute(
            """
            INSERT INTO gate_log (
                gate_id, gate_type, trigger_description, context_packet, project_id,
                status, timeout_hours, operator_response, created_at, responded_at, expires_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                g2_gate_id,
                "G2",
                f"kill_recommend:{phase['name']}",
                json.dumps(
                    {
                        "recommendation_id": recommendation_id,
                        "project_id": project["project_id"],
                        "project_name": project["name"],
                        "kill_score": self._latest_kill_score(financial, project["project_id"], confidence),
                        "failure_analysis": self._limit_words(failure_analysis, 80),
                        "asset_inventory": asset_inventory,
                    }
                ),
                project["project_id"],
                "PENDING",
                48.0,
                None,
                created_at,
                None,
                self._offset_hours(created_at, 48.0),
            ),
        )
        return recommendation_id, g2_gate_id

    @staticmethod
    def _phase_outputs_summary(phase) -> str:
        outputs = json.loads(phase["outputs"])
        if outputs:
            return "; ".join(str(item) for item in outputs)
        return f"No outputs recorded yet for phase {phase['name']}."

    @staticmethod
    def _format_prior_gate_results(rows) -> str:
        if not rows:
            return "No prior gate results."
        entries: list[str] = []
        for row in rows:
            gate_result = json.loads(row["gate_result"])
            entries.append(
                f"{row['name']}: {gate_result.get('verdict', 'UNKNOWN')} "
                f"({gate_result.get('confidence', 0.0):.2f}) - {gate_result.get('rationale', '')}"
            )
        return " | ".join(entries)

    @staticmethod
    def _format_assets_summary(rows) -> str:
        if not rows:
            return "No project assets recorded."
        return " | ".join(
            f"{row['name']}:{row['asset_type']}:{'reusable' if row['reusable'] else 'single_use'}"
            for row in rows
        )

    @staticmethod
    def _limit_tokens(text: str, max_tokens: int) -> str:
        max_words = max(1, int(max_tokens * 0.75))
        words = text.split()
        if len(words) <= max_words:
            return text
        return " ".join(words[:max_words]) + " [TRUNCATED]"

    @staticmethod
    def _limit_words(text: str, max_words: int) -> str:
        words = text.split()
        if len(words) <= max_words:
            return text
        return " ".join(words[: max_words - 1] + ["..."])

    @staticmethod
    def _phase_gate_log_status(verdict: str) -> str:
        if verdict == "PAUSE":
            return "SUSPENDED"
        if verdict == "KILL_RECOMMEND":
            return "APPROVED"
        return "APPROVED"

    @staticmethod
    def _latest_kill_score(financial, project_id: str, fallback_confidence: float) -> float:
        rows = financial.execute(
            """
            SELECT weight, raw_score
            FROM kill_signals
            WHERE project_id = ?
            ORDER BY created_at DESC
            """,
            (project_id,),
        ).fetchall()
        if not rows:
            return round(max(0.6, 1.0 - fallback_confidence), 4)
        return round(sum(row["weight"] * row["raw_score"] for row in rows), 4)

    @staticmethod
    def _determine_phase_gate_tier(
        *,
        trigger: str,
        phase_name: str,
        sequence: int,
        cashflow_actual_usd: float,
        cashflow_forecast_usd: float | None,
        kill_score_current: float | None,
        prior_gate_rows,
        kill_signals: list[dict[str, Any]],
    ) -> str:
        prior_pivot = any(
            json.loads(row["gate_result"]).get("verdict") == "PIVOT"
            for row in prior_gate_rows
        )
        kill_accumulating = sum(1 for signal in kill_signals if signal.get("raw_score", 0) >= 0.5) >= 2
        if trigger in {"BUDGET_EXHAUSTED", "BLOCKER"}:
            return "TIER_2"
        if kill_score_current is not None and kill_score_current >= 0.45:
            return "TIER_2"
        if cashflow_forecast_usd and sequence >= 1 and cashflow_actual_usd < 0.50 * cashflow_forecast_usd:
            return "TIER_2"
        if prior_pivot:
            return "TIER_2"
        if phase_name == "OPERATE" and kill_accumulating:
            return "TIER_2"
        return "TIER_1"

    def _project_outcome_record(self, project, phases, pnl, assets, *, project_status: str) -> dict[str, Any]:
        success_criteria = json.loads(project["success_criteria"])
        cashflow_target = success_criteria.get("cashflow_target_usd") if isinstance(success_criteria, dict) else None
        actual_outcome = self._actual_outcome(project_status, project["cashflow_actual_usd"], cashflow_target)
        return {
            "project_id": project["project_id"],
            "project_name": project["name"],
            "project_status": project_status,
            "thesis": project["thesis"],
            "cashflow_actual_usd": project["cashflow_actual_usd"],
            "cashflow_target_usd": cashflow_target,
            "actual_outcome": actual_outcome,
            "revenue_to_date": 0.0 if pnl is None else pnl["revenue_to_date"],
            "direct_cost_usd": 0.0 if pnl is None else pnl["direct_cost"],
            "net_to_date": 0.0 if pnl is None else pnl["net_to_date"],
            "pivot_log": json.loads(project["pivot_log"]),
            "phase_history": [
                {
                    "name": row["name"],
                    "status": row["status"],
                    "gate_result": None if row["gate_result"] is None else json.loads(row["gate_result"]),
                    "completed_at": row["completed_at"],
                }
                for row in phases
            ],
            "assets_preserved": [
                {
                    "asset_type": row["asset_type"],
                    "name": row["name"],
                    "reusable": bool(row["reusable"]),
                    "location": row["location"],
                }
                for row in assets
            ],
        }

    @staticmethod
    def _actual_outcome(project_status: str, cashflow_actual_usd: float, cashflow_target_usd: float | None) -> float:
        if project_status == "KILLED":
            return 0.0
        if not cashflow_target_usd:
            return 1.0 if project_status == "COMPLETE" else 0.0
        if cashflow_actual_usd >= 0.80 * cashflow_target_usd:
            return 1.0
        if cashflow_actual_usd >= 0.50 * cashflow_target_usd:
            return 0.5
        return 0.0

    @staticmethod
    def _offset_hours(timestamp: str, hours: float) -> str:
        return (
            datetime.datetime.fromisoformat(timestamp) + datetime.timedelta(hours=hours)
        ).replace(microsecond=0).isoformat()

    @staticmethod
    def _fetch_project(financial, project_id: str):
        row = financial.execute("SELECT * FROM projects WHERE project_id = ?", (project_id,)).fetchone()
        if row is None:
            raise KeyError(project_id)
        return dict(row)

    @staticmethod
    def _fetch_phase(financial, phase_id: str):
        row = financial.execute("SELECT * FROM phases WHERE phase_id = ?", (phase_id,)).fetchone()
        if row is None:
            raise KeyError(phase_id)
        return dict(row)

    def _fetch_project_and_phase(self, financial, project_id: str, allowed_phase_statuses: tuple[str, ...] = ("ACTIVE",)) -> tuple[Any, Any]:
        project = self._fetch_project(financial, project_id)
        phase = financial.execute(
            f"""
            SELECT *
            FROM phases
            WHERE project_id = ? AND status IN ({",".join("?" for _ in allowed_phase_statuses)})
            ORDER BY sequence ASC
            LIMIT 1
            """,
            (project_id, *allowed_phase_statuses),
        ).fetchone()
        if phase is None:
            raise ValueError(f"project {project_id} has no phase in statuses {allowed_phase_statuses}")
        return project, phase

    def _fetch_opportunity(self, opportunity_id: str) -> dict[str, Any]:
        conn = self._db.get_connection("strategic_memory")
        row = conn.execute(
            """
            SELECT
                opportunity_id, income_mechanism, title, thesis, detected_by,
                council_verdict_id, validation_spend, validation_report, cashflow_estimate,
                status, project_id, learning_record, provenance_links, provenance_degraded,
                trust_tier, created_at, updated_at
            FROM opportunity_records
            WHERE opportunity_id = ?
            """,
            (opportunity_id,),
        ).fetchone()
        if row is None:
            raise KeyError(opportunity_id)
        return self._row_to_opportunity(row)

    @staticmethod
    def _row_to_opportunity(row) -> dict[str, Any]:
        return asdict(
            OpportunityRecord(
                opportunity_id=row["opportunity_id"],
                income_mechanism=row["income_mechanism"],
                title=row["title"],
                thesis=row["thesis"],
                detected_by=row["detected_by"],
                council_verdict_id=row["council_verdict_id"],
                validation_spend=row["validation_spend"],
                validation_report=row["validation_report"],
                cashflow_estimate=json.loads(row["cashflow_estimate"]),
                status=row["status"],
                project_id=row["project_id"],
                learning_record=None if row["learning_record"] is None else json.loads(row["learning_record"]),
                provenance_links=json.loads(row["provenance_links"]),
                provenance_degraded=bool(row["provenance_degraded"]),
                trust_tier=row["trust_tier"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )
        )

    @staticmethod
    def _utc_now() -> str:
        return datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()


_SKILL: Optional[OpportunityPipelineSkill] = None


def configure_skill(db_manager: DatabaseManager):
    global _SKILL
    _SKILL = OpportunityPipelineSkill(db_manager)


def opportunity_pipeline_entry(action: str, **kwargs):
    if _SKILL is None:
        raise RuntimeError("opportunity pipeline skill not configured")
    if action == "create_opportunity":
        return _SKILL.create_opportunity(
            kwargs["title"],
            kwargs["thesis"],
            kwargs.get("income_mechanism", "software_product"),
            detected_by=kwargs.get("detected_by", "operator"),
            cashflow_estimate=kwargs.get("cashflow_estimate"),
            provenance_links=kwargs.get("provenance_links"),
            trust_tier=kwargs.get("trust_tier", 2),
        )
    if action == "get_opportunity":
        return _SKILL.get_opportunity(kwargs["opportunity_id"])
    if action == "list_opportunities":
        return _SKILL.list_opportunities(status=kwargs.get("status"), limit=kwargs.get("limit", 20))
    if action == "transition_opportunity":
        return _SKILL.transition_opportunity(
            kwargs["opportunity_id"],
            kwargs["new_status"],
            council_verdict_id=kwargs.get("council_verdict_id"),
            validation_report=kwargs.get("validation_report"),
            validation_spend=kwargs.get("validation_spend"),
            learning_record=kwargs.get("learning_record"),
            project_id=kwargs.get("project_id"),
        )
    if action == "handoff_to_project":
        return _SKILL.handoff_to_project(
            kwargs["opportunity_id"],
            project_name=kwargs.get("project_name"),
            success_criteria=kwargs.get("success_criteria"),
            compute_budget=kwargs.get("compute_budget"),
            portfolio_weight=kwargs.get("portfolio_weight", 0.20),
        )
    if action == "trigger_phase_gate":
        return _SKILL.trigger_phase_gate(
            kwargs["project_id"],
            kwargs["trigger"],
            outputs_summary=kwargs.get("outputs_summary"),
            cashflow_forecast_usd=kwargs.get("cashflow_forecast_usd"),
            kill_score_current=kwargs.get("kill_score_current"),
            kill_signals=kwargs.get("kill_signals"),
            gate_type=kwargs.get("gate_type"),
        )
    if action == "apply_phase_gate_verdict":
        return _SKILL.apply_phase_gate_verdict(
            kwargs["project_id"],
            kwargs["verdict"],
            kwargs["confidence"],
            kwargs["rationale"],
            next_phase_amendments=kwargs.get("next_phase_amendments"),
            dissent_log=kwargs.get("dissent_log"),
            gate_id=kwargs.get("gate_id"),
            new_thesis=kwargs.get("new_thesis"),
            failure_analysis=kwargs.get("failure_analysis"),
        )
    if action == "resume_project":
        return _SKILL.resume_project(
            kwargs["project_id"],
            reason=kwargs.get("reason", "operator_command"),
        )
    if action == "close_from_project":
        return _SKILL.close_from_project(
            kwargs["project_id"],
            project_status=kwargs["project_status"],
            learning_record=kwargs.get("learning_record"),
        )
    raise ValueError(f"Unknown action: {action}")
