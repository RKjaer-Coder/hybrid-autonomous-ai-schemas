from __future__ import annotations

from typing import Any

from .records import (
    Decision,
    SelfImprovementEvalRecord,
    SelfImprovementEvidencePipelineRun,
    SelfImprovementPatchReviewPacket,
    SelfImprovementPromotionPacket,
    SelfImprovementProposal,
    SelfImprovementReplayProjectionComparison,
    SelfImprovementRollbackRecord,
    sha256_text,
)
from .replay import KERNEL_POLICY_VERSION
from .store_common import (
    _loads,
    _self_improvement_comparison_payload,
    _self_improvement_eval_payload,
    _self_improvement_patch_review_payload,
    _self_improvement_pipeline_run_payload,
    _self_improvement_promotion_payload,
    _self_improvement_proposal_payload,
    _self_improvement_rollback_payload,
    canonical_json,
)


PINNED_POLICY_AREAS = {
    "control_kernel_policy",
    "spend_rules",
    "gate_rules",
    "operator_auth",
    "event_log_schema",
    "capability_broker",
    "critical_model_promotion_thresholds",
    "security_allowlists",
    "frozen_eval_holdouts",
    "data_retention_deletion_rules",
    "side_effect_authority_rules",
}


class SelfImprovementKernelTransactionMixin:
    def record_self_improvement_proposal(self, proposal: SelfImprovementProposal) -> str:
        if not proposal.problem_evidence:
            raise ValueError("self-improvement proposals require durable problem evidence")
        for field_name in ["proposed_change", "expected_benefit", "risk_assessment", "eval_plan", "rollback_plan"]:
            if not str(getattr(proposal, field_name)).strip():
                raise ValueError(f"self-improvement proposal requires {field_name}")
        if proposal.status not in {"proposed", "eval_running"}:
            raise PermissionError("new self-improvement proposals cannot start approved, promoted, or rolled back")
        if proposal.target_type in {"workflow", "policy"} and proposal.authority_required != "operator_gate":
            raise PermissionError("workflow and policy improvement proposals require operator gate authority")
        if PINNED_POLICY_AREAS.intersection(proposal.affected_policy_areas) and proposal.authority_required != "operator_gate":
            raise PermissionError("pinned policy areas require operator gate authority")
        if self.command.requested_by in {"agent", "model"} and proposal.authority_required in {"rule", "single_agent"}:
            raise PermissionError("workers cannot assign low authority to their own improvement proposal")
        if self.command.requested_authority and self.command.requested_authority != proposal.authority_required:
            raise PermissionError("command requested authority does not match self-improvement policy")
        payload = _self_improvement_proposal_payload(proposal)
        event_id = self.append_event("self_improvement_proposal_recorded", "self_improvement", proposal.proposal_id, payload)
        self.conn.execute(
            """
            INSERT INTO self_improvement_proposals (
              proposal_id, target_type, target_id, problem_evidence_json,
              proposed_change, expected_benefit, risk_assessment, eval_plan,
              rollback_plan, authority_required, proposer_type, proposer_id,
              affected_policy_areas_json, data_classes_json, status,
              created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                proposal.proposal_id,
                proposal.target_type,
                proposal.target_id,
                canonical_json(proposal.problem_evidence),
                proposal.proposed_change,
                proposal.expected_benefit,
                proposal.risk_assessment,
                proposal.eval_plan,
                proposal.rollback_plan,
                proposal.authority_required,
                proposal.proposer_type,
                proposal.proposer_id,
                canonical_json(proposal.affected_policy_areas),
                canonical_json(proposal.data_classes),
                proposal.status,
                proposal.created_at,
                proposal.updated_at,
            ),
        )
        self.enqueue_projection(event_id, "self_improvement_proposal_projection")
        return proposal.proposal_id

    def record_self_improvement_eval(self, record: SelfImprovementEvalRecord) -> str:
        proposal = self.conn.execute(
            "SELECT proposal_id, authority_required, status FROM self_improvement_proposals WHERE proposal_id=?",
            (record.proposal_id,),
        ).fetchone()
        if proposal is None:
            raise ValueError("self-improvement eval requires a recorded proposal")
        if proposal["status"] in {"promoted", "rolled_back", "rejected"}:
            raise ValueError("closed self-improvement proposals cannot receive new eval records")
        if record.authority_effect != "evidence_only":
            raise PermissionError("self-improvement eval records are evidence only")
        if not record.dataset_refs:
            raise ValueError("self-improvement eval requires governed dataset or trace references")
        if "overall" not in record.metrics:
            raise ValueError("self-improvement eval metrics require an overall score")
        if record.eval_type in {"replay", "shadow"} and record.side_effect_safety.get("reexecuted_side_effects") not in {False, 0}:
            raise PermissionError("self-improvement replay/shadow evals must not re-execute side effects")
        payload = _self_improvement_eval_payload(record)
        event_id = self.append_event("self_improvement_eval_recorded", "self_improvement", record.eval_id, payload)
        self.conn.execute(
            """
            INSERT INTO self_improvement_eval_records (
              eval_id, proposal_id, eval_type, baseline_ref, candidate_ref,
              dataset_refs_json, metrics_json, regression_thresholds_json,
              failure_examples_json, side_effect_safety_json, status,
              authority_effect, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.eval_id,
                record.proposal_id,
                record.eval_type,
                record.baseline_ref,
                record.candidate_ref,
                canonical_json(record.dataset_refs),
                canonical_json(record.metrics),
                canonical_json(record.regression_thresholds),
                canonical_json(record.failure_examples),
                canonical_json(record.side_effect_safety),
                record.status,
                record.authority_effect,
                record.created_at,
            ),
        )
        self.conn.execute(
            "UPDATE self_improvement_proposals SET status='eval_running', updated_at=? WHERE proposal_id=? AND status='proposed'",
            (record.created_at, record.proposal_id),
        )
        self.enqueue_projection(event_id, "self_improvement_eval_projection")
        return record.eval_id

    def create_self_improvement_promotion_packet(self, packet: SelfImprovementPromotionPacket) -> str:
        proposal = self.conn.execute(
            "SELECT proposal_id, authority_required, status FROM self_improvement_proposals WHERE proposal_id=?",
            (packet.proposal_id,),
        ).fetchone()
        if proposal is None:
            raise ValueError("self-improvement promotion packet requires a recorded proposal")
        if packet.required_authority != proposal["authority_required"]:
            raise PermissionError("promotion packet authority must match proposal authority")
        if packet.required_authority != "operator_gate":
            raise PermissionError("pre-Hermes self-improvement promotion remains operator-gated")
        if self.command.requested_by in {"agent", "model"}:
            raise PermissionError("workers may not create self-improvement promotion packets")
        if not packet.eval_record_ids:
            raise ValueError("promotion packet requires eval evidence")
        if not packet.evidence_refs:
            raise ValueError("promotion packet requires evidence refs")
        decision = self.conn.execute(
            "SELECT decision_type, required_authority, status, recommendation FROM decisions WHERE decision_id=?",
            (packet.decision_id,),
        ).fetchone()
        if decision is None:
            raise ValueError("promotion packet requires a Decision record")
        if decision["decision_type"] != "system_improvement":
            raise ValueError("promotion packet Decision must be system_improvement")
        if decision["required_authority"] != packet.required_authority:
            raise PermissionError("promotion packet Decision authority mismatch")
        if decision["status"] != packet.status:
            raise ValueError("promotion packet status must match Decision status")
        for eval_id in packet.eval_record_ids:
            eval_row = self.conn.execute(
                "SELECT proposal_id, status, authority_effect FROM self_improvement_eval_records WHERE eval_id=?",
                (eval_id,),
            ).fetchone()
            if eval_row is None:
                raise ValueError("promotion packet references unknown eval record")
            if eval_row["proposal_id"] != packet.proposal_id:
                raise ValueError("promotion packet eval proposal mismatch")
            if eval_row["authority_effect"] != "evidence_only":
                raise PermissionError("promotion packet eval evidence must be evidence-only")
            if packet.recommendation == "approve" and eval_row["status"] != "passed":
                raise ValueError("approval recommendation requires passed eval records")
        payload = _self_improvement_promotion_payload(packet)
        event_id = self.append_event("self_improvement_promotion_packet_created", "decision", packet.packet_id, payload)
        self.conn.execute(
            """
            INSERT INTO self_improvement_promotion_packets (
              packet_id, proposal_id, decision_id, recommendation,
              required_authority, eval_record_ids_json, evidence_refs_json,
              risk_flags_json, gate_packet_json, default_on_timeout, status,
              created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                packet.packet_id,
                packet.proposal_id,
                packet.decision_id,
                packet.recommendation,
                packet.required_authority,
                canonical_json(packet.eval_record_ids),
                canonical_json(packet.evidence_refs),
                canonical_json(packet.risk_flags),
                canonical_json(packet.gate_packet),
                packet.default_on_timeout,
                packet.status,
                packet.created_at,
            ),
        )
        self.enqueue_projection(event_id, "self_improvement_promotion_projection")
        return packet.packet_id

    def prepare_self_improvement_patch_review_packet(self, packet: SelfImprovementPatchReviewPacket) -> str:
        proposal = self.conn.execute(
            "SELECT proposal_id, authority_required, status FROM self_improvement_proposals WHERE proposal_id=?",
            (packet.proposal_id,),
        ).fetchone()
        if proposal is None:
            raise ValueError("patch review packet requires a recorded proposal")
        promotion = self.conn.execute(
            "SELECT packet_id, proposal_id, required_authority, status FROM self_improvement_promotion_packets WHERE packet_id=?",
            (packet.promotion_packet_id,),
        ).fetchone()
        if promotion is None or promotion["proposal_id"] != packet.proposal_id:
            raise ValueError("patch review packet requires a matching promotion packet")
        if packet.required_authority != "operator_gate" or packet.authority_effect != "review_only":
            raise PermissionError("self-improvement patch packets are operator-gated and review-only")
        if packet.status != "prepared":
            raise PermissionError("new self-improvement patch packets can only be prepared")
        if self.command.requested_by in {"agent", "model"}:
            raise PermissionError("workers may not prepare patch review packets")
        if not packet.patch_ref or not packet.patch_hash:
            raise ValueError("patch review packet requires a governed patch ref and hash")
        if not packet.changed_paths:
            raise ValueError("patch review packet requires changed paths")
        if any(path.startswith("/") or ".." in path.split("/") for path in packet.changed_paths):
            raise PermissionError("patch review changed paths must be repo-relative and bounded")
        if not packet.apply_instructions.strip() or not packet.verification_plan.strip() or not packet.rollback_ref.strip():
            raise ValueError("patch review packet requires apply instructions, verification plan, and rollback ref")
        required_blocks = {
            "active_behavior_mutation",
            "autonomous_patch_application",
            "frontier_route_update",
            "external_side_effect_reexecution",
        }
        if not required_blocks.issubset(set(packet.blocked_autonomous_actions)):
            raise PermissionError("patch review packet must block autonomous mutation and side-effect replay")
        payload = _self_improvement_patch_review_payload(packet)
        event_id = self.append_event(
            "self_improvement_patch_review_packet_prepared",
            "self_improvement",
            packet.patch_packet_id,
            payload,
        )
        self.conn.execute(
            """
            INSERT INTO self_improvement_patch_review_packets (
              patch_packet_id, proposal_id, promotion_packet_id, target_ref,
              patch_ref, patch_hash, changed_paths_json, apply_instructions,
              verification_plan, rollback_ref, evidence_refs_json,
              blocked_autonomous_actions_json, required_authority,
              authority_effect, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                packet.patch_packet_id,
                packet.proposal_id,
                packet.promotion_packet_id,
                packet.target_ref,
                packet.patch_ref,
                packet.patch_hash,
                canonical_json(packet.changed_paths),
                packet.apply_instructions,
                packet.verification_plan,
                packet.rollback_ref,
                canonical_json(packet.evidence_refs),
                canonical_json(packet.blocked_autonomous_actions),
                packet.required_authority,
                packet.authority_effect,
                packet.status,
                packet.created_at,
            ),
        )
        self.enqueue_projection(event_id, "self_improvement_patch_review_projection")
        return packet.patch_packet_id

    def record_self_improvement_rollback(self, record: SelfImprovementRollbackRecord) -> str:
        packet = self.conn.execute(
            "SELECT proposal_id FROM self_improvement_promotion_packets WHERE packet_id=?",
            (record.packet_id,),
        ).fetchone()
        if packet is None or packet["proposal_id"] != record.proposal_id:
            raise ValueError("rollback requires matching proposal and promotion packet")
        if record.status == "applied" and (not record.receipt_ref or not record.receipt_hash):
            raise PermissionError("applied rollback requires durable receipt reference and hash")
        payload = _self_improvement_rollback_payload(record)
        event_id = self.append_event("self_improvement_rollback_recorded", "self_improvement", record.rollback_id, payload)
        self.conn.execute(
            """
            INSERT INTO self_improvement_rollbacks (
              rollback_id, proposal_id, packet_id, previous_ref,
              rollback_reason, receipt_ref, receipt_hash, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                record.rollback_id,
                record.proposal_id,
                record.packet_id,
                record.previous_ref,
                record.rollback_reason,
                record.receipt_ref,
                record.receipt_hash,
                record.status,
                record.created_at,
            ),
        )
        if record.status == "applied":
            self.conn.execute(
                "UPDATE self_improvement_proposals SET status='rolled_back', updated_at=? WHERE proposal_id=?",
                (record.created_at, record.proposal_id),
            )
        self.enqueue_projection(event_id, "self_improvement_rollback_projection")
        return record.rollback_id

    def compare_self_improvement_replay_to_projection(self, scope: str = "self_improvement") -> SelfImprovementReplayProjectionComparison:
        replay = self._replay_from_connection(self.conn)
        proposal_rows = [self._self_improvement_proposal_row(row) for row in self.conn.execute("SELECT * FROM self_improvement_proposals ORDER BY proposal_id")]
        eval_rows = [self._self_improvement_eval_row(row) for row in self.conn.execute("SELECT * FROM self_improvement_eval_records ORDER BY eval_id")]
        packet_rows = [self._self_improvement_packet_row(row) for row in self.conn.execute("SELECT * FROM self_improvement_promotion_packets ORDER BY packet_id")]
        patch_rows = [self._self_improvement_patch_review_row(row) for row in self.conn.execute("SELECT * FROM self_improvement_patch_review_packets ORDER BY patch_packet_id")]
        rollback_rows = [self._self_improvement_rollback_row(row) for row in self.conn.execute("SELECT * FROM self_improvement_rollbacks ORDER BY rollback_id")]
        pipeline_rows = [self._self_improvement_pipeline_row(row) for row in self.conn.execute("SELECT * FROM self_improvement_evidence_pipeline_runs ORDER BY run_id")]
        replay_proposals = sorted(replay.self_improvement_proposals.values(), key=lambda item: item["proposal_id"])
        replay_evals = sorted(replay.self_improvement_eval_records.values(), key=lambda item: item["eval_id"])
        replay_packets = sorted(replay.self_improvement_promotion_packets.values(), key=lambda item: item["packet_id"])
        replay_patch_packets = sorted(replay.self_improvement_patch_review_packets.values(), key=lambda item: item["patch_packet_id"])
        replay_rollbacks = sorted(replay.self_improvement_rollbacks.values(), key=lambda item: item["rollback_id"])
        replay_pipeline_runs = sorted(replay.self_improvement_evidence_pipeline_runs.values(), key=lambda item: item["run_id"])
        mismatches: list[str] = []
        if replay_proposals != proposal_rows:
            mismatches.append("proposal_projection_mismatch")
        if replay_evals != eval_rows:
            mismatches.append("eval_projection_mismatch")
        if replay_packets != packet_rows:
            mismatches.append("promotion_packet_projection_mismatch")
        if replay_patch_packets != patch_rows:
            mismatches.append("patch_review_packet_projection_mismatch")
        if replay_rollbacks != rollback_rows:
            mismatches.append("rollback_projection_mismatch")
        if replay_pipeline_runs != pipeline_rows:
            mismatches.append("pipeline_run_projection_mismatch")
        comparison = SelfImprovementReplayProjectionComparison(
            scope=scope,
            replay_proposals=replay_proposals,
            projection_proposals=proposal_rows,
            replay_eval_records=replay_evals,
            projection_eval_records=eval_rows,
            replay_promotion_packets=replay_packets,
            projection_promotion_packets=packet_rows,
            replay_patch_review_packets=replay_patch_packets,
            projection_patch_review_packets=patch_rows,
            replay_rollbacks=replay_rollbacks,
            projection_rollbacks=rollback_rows,
            replay_pipeline_runs=replay_pipeline_runs,
            projection_pipeline_runs=pipeline_rows,
            matches=not mismatches,
            mismatches=mismatches,
        )
        payload = _self_improvement_comparison_payload(comparison)
        event_id = self.append_event("self_improvement_replay_projection_compared", "self_improvement", comparison.comparison_id, payload)
        self.conn.execute(
            """
            INSERT INTO self_improvement_replay_projection_comparisons (
              comparison_id, scope, replay_proposals_json, projection_proposals_json,
              replay_eval_records_json, projection_eval_records_json,
              replay_promotion_packets_json, projection_promotion_packets_json,
              replay_patch_review_packets_json, projection_patch_review_packets_json,
              replay_rollbacks_json, projection_rollbacks_json,
              replay_pipeline_runs_json, projection_pipeline_runs_json, matches,
              mismatches_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                comparison.comparison_id,
                comparison.scope,
                canonical_json(comparison.replay_proposals),
                canonical_json(comparison.projection_proposals),
                canonical_json(comparison.replay_eval_records),
                canonical_json(comparison.projection_eval_records),
                canonical_json(comparison.replay_promotion_packets),
                canonical_json(comparison.projection_promotion_packets),
                canonical_json(comparison.replay_patch_review_packets),
                canonical_json(comparison.projection_patch_review_packets),
                canonical_json(comparison.replay_rollbacks),
                canonical_json(comparison.projection_rollbacks),
                canonical_json(comparison.replay_pipeline_runs),
                canonical_json(comparison.projection_pipeline_runs),
                1 if comparison.matches else 0,
                canonical_json(comparison.mismatches),
                comparison.created_at,
            ),
        )
        self.enqueue_projection(event_id, "self_improvement_replay_projection_comparison")
        return comparison

    def run_self_improvement_evidence_pipeline(
        self,
        *,
        signals: list[dict[str, Any]],
        as_of: str,
        scope: str = "pre_hermes_self_improvement",
        run_id: str | None = None,
    ) -> SelfImprovementEvidencePipelineRun:
        if self.command.requested_by != "kernel":
            raise PermissionError("self-improvement evidence pipeline is kernel-owned")
        if self.command.requested_authority and self.command.requested_authority != "operator_gate":
            raise PermissionError("self-improvement evidence pipeline preserves operator-gate authority")
        blocked_actions = [
            "active_behavior_mutation",
            "policy_mutation",
            "spend_rule_mutation",
            "gate_rule_mutation",
            "holdout_mutation",
            "side_effect_replay",
            "autonomous_model_promotion",
        ]
        source_counts: dict[str, int] = {}
        proposal_ids: list[str] = []
        eval_record_ids: list[str] = []
        promotion_packet_ids: list[str] = []
        portfolio_items: list[dict[str, Any]] = []

        for index, signal in enumerate(signals):
            normalized = self._normalize_improvement_signal(signal, index=index, as_of=as_of)
            source = normalized["source"]
            source_counts[source] = source_counts.get(source, 0) + 1
            proposal = self._proposal_from_signal(normalized)
            proposal_id = self._ensure_self_improvement_proposal(proposal)
            proposal_ids.append(proposal_id)

            eval_record = self._eval_from_signal(normalized, proposal_id)
            eval_id = self._ensure_self_improvement_eval(eval_record)
            eval_record_ids.append(eval_id)

            decision = self._decision_from_signal(normalized, proposal, eval_id)
            decision_id = self._ensure_self_improvement_decision(decision)
            packet = self._packet_from_signal(normalized, proposal_id, eval_id, decision_id)
            packet_id = self._ensure_self_improvement_promotion_packet(packet)
            promotion_packet_ids.append(packet_id)
            portfolio_items.append(
                {
                    "packet_id": packet_id,
                    "proposal_id": proposal_id,
                    "target_type": proposal.target_type,
                    "target_id": proposal.target_id,
                    "source": source,
                    "recommendation": packet.recommendation,
                    "required_authority": packet.required_authority,
                    "eval_status": eval_record.status,
                    "risk_flags": packet.risk_flags,
                    "operator_action": normalized["operator_action"],
                    "live_controls_enabled": False,
                }
            )

        comparison_id: str | None = None
        if signals:
            comparison = self.compare_self_improvement_replay_to_projection(scope)
            comparison_id = comparison.comparison_id
        run = SelfImprovementEvidencePipelineRun(
            run_id=run_id or f"si-pipeline-{sha256_text(canonical_json({'signals': signals, 'as_of': as_of}))[:24]}",
            source_counts=source_counts,
            proposal_ids=proposal_ids,
            eval_record_ids=eval_record_ids,
            promotion_packet_ids=promotion_packet_ids,
            comparison_id=comparison_id,
            portfolio_items=portfolio_items,
            blocked_autonomous_actions=blocked_actions,
            status="recorded" if signals else "no_signals",
            created_at=as_of,
        )
        payload = _self_improvement_pipeline_run_payload(run)
        event_id = self.append_event("self_improvement_evidence_pipeline_recorded", "self_improvement", run.run_id, payload)
        self.conn.execute(
            """
            INSERT INTO self_improvement_evidence_pipeline_runs (
              run_id, source_counts_json, proposal_ids_json, eval_record_ids_json,
              promotion_packet_ids_json, comparison_id, portfolio_items_json,
              blocked_autonomous_actions_json, status, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run.run_id,
                canonical_json(run.source_counts),
                canonical_json(run.proposal_ids),
                canonical_json(run.eval_record_ids),
                canonical_json(run.promotion_packet_ids),
                run.comparison_id,
                canonical_json(run.portfolio_items),
                canonical_json(run.blocked_autonomous_actions),
                run.status,
                run.created_at,
            ),
        )
        self.enqueue_projection(event_id, "self_improvement_evidence_pipeline_projection")
        return run

    def _ensure_self_improvement_proposal(self, proposal: SelfImprovementProposal) -> str:
        row = self.conn.execute(
            "SELECT proposal_id FROM self_improvement_proposals WHERE proposal_id=?",
            (proposal.proposal_id,),
        ).fetchone()
        if row is not None:
            return row["proposal_id"]
        return self.record_self_improvement_proposal(proposal)

    def _ensure_self_improvement_eval(self, record: SelfImprovementEvalRecord) -> str:
        row = self.conn.execute(
            "SELECT eval_id FROM self_improvement_eval_records WHERE eval_id=?",
            (record.eval_id,),
        ).fetchone()
        if row is not None:
            return row["eval_id"]
        return self.record_self_improvement_eval(record)

    def _ensure_self_improvement_decision(self, decision: Decision) -> str:
        row = self.conn.execute("SELECT decision_id FROM decisions WHERE decision_id=?", (decision.decision_id,)).fetchone()
        if row is not None:
            return row["decision_id"]
        return self.create_decision(decision)

    def _ensure_self_improvement_promotion_packet(self, packet: SelfImprovementPromotionPacket) -> str:
        row = self.conn.execute(
            "SELECT packet_id FROM self_improvement_promotion_packets WHERE packet_id=?",
            (packet.packet_id,),
        ).fetchone()
        if row is not None:
            return row["packet_id"]
        return self.create_self_improvement_promotion_packet(packet)

    @staticmethod
    def _normalize_improvement_signal(signal: dict[str, Any], *, index: int, as_of: str) -> dict[str, Any]:
        source = str(signal.get("source") or "unknown")
        target_type = str(signal.get("target_type") or "harness")
        if target_type not in {"harness", "workflow", "tool", "model", "eval", "policy"}:
            raise ValueError("unsupported self-improvement target_type")
        target_id = str(signal.get("target_id") or f"{source}.{index}")
        evidence_refs = [str(item) for item in signal.get("evidence_refs", []) if str(item)]
        if not evidence_refs:
            evidence_refs = [f"kernel:self_improvement/signals/{source}/{sha256_text(canonical_json(signal))[:16]}"]
        authority_required = str(signal.get("authority_required") or "operator_gate")
        if authority_required != "operator_gate":
            raise PermissionError("pre-Hermes self-improvement signals must remain operator-gated")
        proposed_change = str(signal.get("proposed_change") or f"Review governed improvement candidate for {target_id}.")
        expected_benefit = str(signal.get("expected_benefit") or "Improve reliability using recorded kernel evidence.")
        risk_assessment = str(signal.get("risk_assessment") or "Evidence-only; no active behavior mutation is permitted.")
        eval_plan = str(signal.get("eval_plan") or "Replay, regression, known-bad, and side-effect safety checks before promotion.")
        rollback_plan = str(signal.get("rollback_plan") or "Keep current behavior unless an operator approves a reversible promotion.")
        metrics = dict(signal.get("metrics") or {})
        metrics.setdefault("overall", float(signal.get("overall", 0.0)))
        side_effect_safety = dict(signal.get("side_effect_safety") or {})
        side_effect_safety.setdefault("reexecuted_side_effects", False)
        side_effect_safety.setdefault("external_intents_reconstructed_only", True)
        return {
            **signal,
            "source": source,
            "target_type": target_type,
            "target_id": target_id,
            "evidence_refs": evidence_refs,
            "authority_required": authority_required,
            "proposed_change": proposed_change,
            "expected_benefit": expected_benefit,
            "risk_assessment": risk_assessment,
            "eval_plan": eval_plan,
            "rollback_plan": rollback_plan,
            "metrics": metrics,
            "regression_thresholds": dict(signal.get("regression_thresholds") or {"overall_min": 0.0}),
            "failure_examples": list(signal.get("failure_examples") or []),
            "side_effect_safety": side_effect_safety,
            "data_classes": list(signal.get("data_classes") or ["internal"]),
            "affected_policy_areas": list(signal.get("affected_policy_areas") or []),
            "eval_type": str(signal.get("eval_type") or "replay"),
            "eval_status": str(signal.get("eval_status") or "needs_more_data"),
            "recommendation": str(signal.get("recommendation") or "needs_more_data"),
            "risk_flags": list(signal.get("risk_flags") or ["operator_gate_required_before_active_change"]),
            "baseline_ref": str(signal.get("baseline_ref") or f"current://{target_id}"),
            "candidate_ref": str(signal.get("candidate_ref") or f"candidate://{target_id}"),
            "dataset_refs": list(signal.get("dataset_refs") or evidence_refs),
            "operator_action": str(signal.get("operator_action") or "review_evidence_packet"),
            "as_of": as_of,
        }

    @staticmethod
    def _signal_id(signal: dict[str, Any], prefix: str) -> str:
        stable = {
            "source": signal["source"],
            "target_type": signal["target_type"],
            "target_id": signal["target_id"],
            "evidence_refs": signal["evidence_refs"],
            "proposed_change": signal["proposed_change"],
        }
        return f"{prefix}-{sha256_text(canonical_json(stable))[:24]}"

    def _proposal_from_signal(self, signal: dict[str, Any]) -> SelfImprovementProposal:
        return SelfImprovementProposal(
            proposal_id=self._signal_id(signal, "si-proposal"),
            target_type=signal["target_type"],  # type: ignore[arg-type]
            target_id=signal["target_id"],
            problem_evidence=signal["evidence_refs"],
            proposed_change=signal["proposed_change"],
            expected_benefit=signal["expected_benefit"],
            risk_assessment=signal["risk_assessment"],
            eval_plan=signal["eval_plan"],
            rollback_plan=signal["rollback_plan"],
            authority_required="operator_gate",
            proposer_type="kernel",
            proposer_id="self-improvement-evidence-pipeline",
            affected_policy_areas=signal["affected_policy_areas"],
            data_classes=signal["data_classes"],
            created_at=signal["as_of"],
            updated_at=signal["as_of"],
        )

    def _eval_from_signal(self, signal: dict[str, Any], proposal_id: str) -> SelfImprovementEvalRecord:
        return SelfImprovementEvalRecord(
            eval_id=self._signal_id(signal, "si-eval"),
            proposal_id=proposal_id,
            eval_type=signal["eval_type"],  # type: ignore[arg-type]
            baseline_ref=signal["baseline_ref"],
            candidate_ref=signal["candidate_ref"],
            dataset_refs=signal["dataset_refs"],
            metrics=signal["metrics"],
            regression_thresholds=signal["regression_thresholds"],
            failure_examples=signal["failure_examples"],
            side_effect_safety=signal["side_effect_safety"],
            status=signal["eval_status"],  # type: ignore[arg-type]
            created_at=signal["as_of"],
        )

    def _decision_from_signal(self, signal: dict[str, Any], proposal: SelfImprovementProposal, eval_id: str) -> Decision:
        decision_id = self._signal_id(signal, "si-decision")
        return Decision(
            decision_id=decision_id,
            decision_type="system_improvement",
            question=f"Review governed improvement proposal for {proposal.target_type}:{proposal.target_id}?",
            options=[
                {"option_id": "approve", "label": "Approve gated promotion"},
                {"option_id": "reject", "label": "Reject proposal"},
                {"option_id": "needs_more_data", "label": "Request more evidence"},
                {"option_id": "rollback", "label": "Prepare rollback only"},
            ],
            stakes="high" if proposal.target_type in {"workflow", "policy", "model"} else "medium",
            evidence_bundle_ids=[],
            evidence_refs=[eval_id, *signal["evidence_refs"]],
            requested_by="kernel",
            required_authority="operator_gate",
            authority_policy_version=KERNEL_POLICY_VERSION,
            status="proposed",
            recommendation=signal["recommendation"],
            confidence=signal["metrics"].get("overall") if isinstance(signal["metrics"].get("overall"), float) else None,
            decisive_factors=[
                f"source={signal['source']}",
                f"target_type={proposal.target_type}",
                f"target_id={proposal.target_id}",
            ],
            risk_flags=signal["risk_flags"],
            default_on_timeout="keep_current_behavior",
            gate_packet={
                "decision_type": "system_improvement",
                "proposal_id": proposal.proposal_id,
                "source": signal["source"],
                "live_controls_enabled": False,
            },
            created_at=signal["as_of"],
        )

    def _packet_from_signal(
        self,
        signal: dict[str, Any],
        proposal_id: str,
        eval_id: str,
        decision_id: str,
    ) -> SelfImprovementPromotionPacket:
        recommendation = signal["recommendation"]
        if recommendation == "approve" and signal["eval_status"] != "passed":
            recommendation = "needs_more_data"
        return SelfImprovementPromotionPacket(
            packet_id=self._signal_id(signal, "si-packet"),
            proposal_id=proposal_id,
            decision_id=decision_id,
            recommendation=recommendation,  # type: ignore[arg-type]
            required_authority="operator_gate",
            eval_record_ids=[eval_id],
            evidence_refs=[eval_id, *signal["evidence_refs"]],
            risk_flags=signal["risk_flags"],
            gate_packet={
                "decision_type": "system_improvement",
                "proposal_id": proposal_id,
                "source": signal["source"],
                "operator_action": signal["operator_action"],
                "live_controls_enabled": False,
            },
            default_on_timeout="keep_current_behavior",
            status="proposed",
            created_at=signal["as_of"],
        )

    @staticmethod
    def _self_improvement_proposal_row(row: Any) -> dict[str, Any]:
        return {
            "proposal_id": row["proposal_id"],
            "target_type": row["target_type"],
            "target_id": row["target_id"],
            "problem_evidence": _loads(row["problem_evidence_json"]),
            "proposed_change": row["proposed_change"],
            "expected_benefit": row["expected_benefit"],
            "risk_assessment": row["risk_assessment"],
            "eval_plan": row["eval_plan"],
            "rollback_plan": row["rollback_plan"],
            "authority_required": row["authority_required"],
            "proposer_type": row["proposer_type"],
            "proposer_id": row["proposer_id"],
            "affected_policy_areas": _loads(row["affected_policy_areas_json"]),
            "data_classes": _loads(row["data_classes_json"]),
            "status": row["status"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    @staticmethod
    def _self_improvement_eval_row(row: Any) -> dict[str, Any]:
        return {
            "eval_id": row["eval_id"],
            "proposal_id": row["proposal_id"],
            "eval_type": row["eval_type"],
            "baseline_ref": row["baseline_ref"],
            "candidate_ref": row["candidate_ref"],
            "dataset_refs": _loads(row["dataset_refs_json"]),
            "metrics": _loads(row["metrics_json"]),
            "regression_thresholds": _loads(row["regression_thresholds_json"]),
            "failure_examples": _loads(row["failure_examples_json"]),
            "side_effect_safety": _loads(row["side_effect_safety_json"]),
            "status": row["status"],
            "authority_effect": row["authority_effect"],
            "created_at": row["created_at"],
        }

    @staticmethod
    def _self_improvement_packet_row(row: Any) -> dict[str, Any]:
        return {
            "packet_id": row["packet_id"],
            "proposal_id": row["proposal_id"],
            "decision_id": row["decision_id"],
            "recommendation": row["recommendation"],
            "required_authority": row["required_authority"],
            "eval_record_ids": _loads(row["eval_record_ids_json"]),
            "evidence_refs": _loads(row["evidence_refs_json"]),
            "risk_flags": _loads(row["risk_flags_json"]),
            "gate_packet": _loads(row["gate_packet_json"]),
            "default_on_timeout": row["default_on_timeout"],
            "status": row["status"],
            "created_at": row["created_at"],
        }

    @staticmethod
    def _self_improvement_patch_review_row(row: Any) -> dict[str, Any]:
        return {
            "patch_packet_id": row["patch_packet_id"],
            "proposal_id": row["proposal_id"],
            "promotion_packet_id": row["promotion_packet_id"],
            "target_ref": row["target_ref"],
            "patch_ref": row["patch_ref"],
            "patch_hash": row["patch_hash"],
            "changed_paths": _loads(row["changed_paths_json"]),
            "apply_instructions": row["apply_instructions"],
            "verification_plan": row["verification_plan"],
            "rollback_ref": row["rollback_ref"],
            "evidence_refs": _loads(row["evidence_refs_json"]),
            "blocked_autonomous_actions": _loads(row["blocked_autonomous_actions_json"]),
            "required_authority": row["required_authority"],
            "authority_effect": row["authority_effect"],
            "status": row["status"],
            "created_at": row["created_at"],
        }

    @staticmethod
    def _self_improvement_rollback_row(row: Any) -> dict[str, Any]:
        return {
            "rollback_id": row["rollback_id"],
            "proposal_id": row["proposal_id"],
            "packet_id": row["packet_id"],
            "previous_ref": row["previous_ref"],
            "rollback_reason": row["rollback_reason"],
            "receipt_ref": row["receipt_ref"],
            "receipt_hash": row["receipt_hash"],
            "status": row["status"],
            "created_at": row["created_at"],
        }

    @staticmethod
    def _self_improvement_pipeline_row(row: Any) -> dict[str, Any]:
        return {
            "run_id": row["run_id"],
            "source_counts": _loads(row["source_counts_json"]),
            "proposal_ids": _loads(row["proposal_ids_json"]),
            "eval_record_ids": _loads(row["eval_record_ids_json"]),
            "promotion_packet_ids": _loads(row["promotion_packet_ids_json"]),
            "comparison_id": row["comparison_id"],
            "portfolio_items": _loads(row["portfolio_items_json"]),
            "blocked_autonomous_actions": _loads(row["blocked_autonomous_actions_json"]),
            "status": row["status"],
            "created_at": row["created_at"],
        }
