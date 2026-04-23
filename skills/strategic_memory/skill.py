from __future__ import annotations

import datetime
import json
import uuid
from dataclasses import asdict, dataclass
from typing import Any, Optional

from harness_variants import HarnessVariantManager
from skills.db_manager import DatabaseManager


ACTIONABILITY_ORDER = {
    "INFORMATIONAL": 0,
    "WATCH": 1,
    "ACTION_RECOMMENDED": 2,
    "ACTION_REQUIRED": 3,
    "HARVEST_NEEDED": 4,
}


@dataclass(frozen=True)
class IntelligenceBriefRecord:
    brief_id: str
    task_id: str
    domain: int
    title: str
    summary: str
    detail: str | None
    confidence: float
    actionability: str
    urgency: str
    depth_tier: str
    action_type: str
    source_urls: list[str]
    source_assessments: list[dict[str, Any]]
    uncertainty_statement: str | None
    counter_thesis: str | None
    spawned_tasks: list[str]
    spawned_opportunity_id: str | None
    related_brief_ids: list[str]
    tags: list[str]
    quality_warning: bool
    source_diversity_hold: bool
    provenance_links: list[str]
    trust_tier: int
    created_at: str


class StrategicMemorySkill:
    def __init__(self, db_manager: DatabaseManager):
        self._db = db_manager
        self._harness_variants = HarnessVariantManager(str(db_manager.data_dir / "telemetry.db"))

    def write_brief(
        self,
        task_id: str,
        title: str,
        summary: str,
        confidence: float = 0.5,
        *,
        domain: int = 2,
        source: str = "operator",
        actionability: str = "INFORMATIONAL",
        urgency: str = "ROUTINE",
        depth_tier: str = "QUICK",
        action_type: str = "none",
        tags: list[str] | None = None,
        provenance_links: list[str] | None = None,
        detail: str | None = None,
        source_urls: list[str] | None = None,
        source_assessments: list[dict[str, Any]] | None = None,
        uncertainty_statement: str | None = None,
        counter_thesis: str | None = None,
        spawned_tasks: list[str] | None = None,
        spawned_opportunity_id: str | None = None,
        related_brief_ids: list[str] | None = None,
        trust_tier: int = 3,
    ) -> str:
        brief_id = str(uuid.uuid4())
        now = self._utc_now()
        tags_json = json.dumps(tags or [])
        provenance_json = json.dumps(provenance_links or [])
        source_urls_list = source_urls or []
        source_assessments_list = source_assessments or []
        spawned_tasks_list = spawned_tasks or []
        related_brief_ids_list = related_brief_ids or []
        effective_actionability, source_diversity_hold = self._apply_source_diversity_gate(
            actionability,
            urgency,
            source_assessments_list,
        )
        quality_warning = self._compute_quality_warning(
            depth_tier=depth_tier,
            confidence=confidence,
            source_assessments=source_assessments_list,
            uncertainty_statement=uncertainty_statement,
            counter_thesis=counter_thesis,
        )
        conn = self._db.get_connection("strategic_memory")
        task = conn.execute("SELECT task_id FROM research_tasks WHERE task_id = ?", (task_id,)).fetchone()
        if task is None:
            conn.execute(
                """
                INSERT INTO research_tasks (
                    task_id, domain, source, title, brief, priority, status,
                    max_spend_usd, actual_spend_usd, output_brief_id, follow_up_tasks,
                    stale_after, tags, depth_upgrade, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task_id,
                    domain,
                    source,
                    title,
                    summary,
                    "P1_HIGH" if depth_tier == "FULL" else "P2_NORMAL",
                    "PENDING",
                    0.0,
                    0.0,
                    brief_id,
                    json.dumps(spawned_tasks_list),
                    None,
                    tags_json,
                    1 if depth_tier == "FULL" and ACTIONABILITY_ORDER.get(actionability, 0) >= ACTIONABILITY_ORDER["ACTION_RECOMMENDED"] else 0,
                    now,
                    now,
                ),
            )
        else:
            conn.execute(
                """
                UPDATE research_tasks
                SET output_brief_id = ?,
                    tags = ?,
                    follow_up_tasks = ?,
                    depth_upgrade = CASE
                        WHEN ? = 'FULL' THEN 1
                        ELSE depth_upgrade
                    END,
                    updated_at = ?
                WHERE task_id = ?
                """,
                (brief_id, tags_json, json.dumps(spawned_tasks_list), depth_tier, now, task_id),
            )
        conn.execute(
            """
            INSERT INTO intelligence_briefs (
                brief_id, task_id, domain, title, summary, detail, source_urls,
                source_assessments, confidence, uncertainty_statement, counter_thesis,
                actionability, urgency, depth_tier, action_type, spawned_tasks,
                spawned_opportunity_id, related_brief_ids, tags, quality_warning,
                source_diversity_hold, provenance_links, trust_tier, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                brief_id,
                task_id,
                domain,
                title,
                summary,
                detail,
                json.dumps(source_urls_list),
                json.dumps(source_assessments_list),
                confidence,
                uncertainty_statement,
                counter_thesis,
                effective_actionability,
                urgency,
                depth_tier,
                action_type,
                json.dumps(spawned_tasks_list),
                spawned_opportunity_id,
                json.dumps(related_brief_ids_list),
                tags_json,
                1 if quality_warning else 0,
                1 if source_diversity_hold else 0,
                provenance_json,
                trust_tier,
                now,
            ),
        )
        if quality_warning:
            self._record_quality_signal(conn, brief_id, signal="incomplete", missing_dimension="quality_contract")
        if source_diversity_hold:
            self._record_quality_signal(conn, brief_id, signal="incomplete", missing_dimension="source_diversity")
        conn.commit()
        self._log_trace(
            task_id=task_id,
            role="strategic_memory_brief_write",
            action_name="write_brief",
            intent_goal=f"Persist intelligence brief {brief_id} for research task {task_id}.",
            payload={
                "brief_id": brief_id,
                "task_id": task_id,
                "domain": domain,
                "actionability": effective_actionability,
                "action_type": action_type,
                "quality_warning": quality_warning,
                "source_diversity_hold": source_diversity_hold,
                "spawned_tasks": spawned_tasks_list,
                "spawned_opportunity_id": spawned_opportunity_id,
            },
            context_assembled=(
                f"source={source}; depth={depth_tier}; urgency={urgency}; "
                f"trust_tier={trust_tier}; title={title}"
            ),
            retrieval_queries=list(dict.fromkeys([*source_urls_list, *(provenance_links or [])])),
        )
        return brief_id

    def read_brief(self, brief_id: str) -> dict[str, Any]:
        conn = self._db.get_connection("strategic_memory")
        row = conn.execute(
            """
            SELECT
                brief_id, task_id, domain, title, summary, detail, confidence, actionability,
                urgency, depth_tier, action_type, source_urls, source_assessments,
                uncertainty_statement, counter_thesis, spawned_tasks,
                spawned_opportunity_id, related_brief_ids, tags, quality_warning,
                source_diversity_hold, provenance_links, trust_tier, created_at
            FROM intelligence_briefs
            WHERE brief_id = ?
            """,
            (brief_id,),
        ).fetchone()
        if row is None:
            raise KeyError(brief_id)
        return self._row_to_brief(row)

    def list_briefs(
        self,
        *,
        limit: int = 20,
        task_id: str | None = None,
        actionability: str | None = None,
        source_diversity_hold: bool | None = None,
        quality_warning: bool | None = None,
    ) -> list[dict[str, Any]]:
        conn = self._db.get_connection("strategic_memory")
        where: list[str] = []
        params: list[Any] = []
        if task_id:
            where.append("task_id = ?")
            params.append(task_id)
        if actionability:
            where.append("actionability = ?")
            params.append(actionability)
        if source_diversity_hold is not None:
            where.append("source_diversity_hold = ?")
            params.append(1 if source_diversity_hold else 0)
        if quality_warning is not None:
            where.append("quality_warning = ?")
            params.append(1 if quality_warning else 0)
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        rows = conn.execute(
            f"""
            SELECT
                brief_id, task_id, domain, title, summary, detail, confidence, actionability,
                urgency, depth_tier, action_type, source_urls, source_assessments,
                uncertainty_statement, counter_thesis, spawned_tasks,
                spawned_opportunity_id, related_brief_ids, tags, quality_warning,
                source_diversity_hold, provenance_links, trust_tier, created_at
            FROM intelligence_briefs
            {where_sql}
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (*params, limit),
        ).fetchall()
        return [self._row_to_brief(row) for row in rows]

    def record_quality_signal(
        self,
        brief_id: str,
        signal: str,
        *,
        missing_dimension: str | None = None,
        verdict_id: str | None = None,
    ) -> dict[str, Any]:
        conn = self._db.get_connection("strategic_memory")
        brief = conn.execute(
            "SELECT brief_id FROM intelligence_briefs WHERE brief_id = ?",
            (brief_id,),
        ).fetchone()
        if brief is None:
            raise KeyError(brief_id)
        self._record_quality_signal(conn, brief_id, signal=signal, missing_dimension=missing_dimension, verdict_id=verdict_id)
        conn.commit()
        brief = self.read_brief(brief_id)
        self._log_trace(
            task_id=brief["task_id"],
            role="brief_quality_signal",
            action_name="record_quality_signal",
            intent_goal=f"Persist brief quality signal {signal} for brief {brief_id}.",
            payload={
                "brief_id": brief_id,
                "signal": signal,
                "missing_dimension": missing_dimension,
                "verdict_id": verdict_id,
            },
            context_assembled=f"actionability={brief['actionability']}; depth={brief['depth_tier']}; title={brief['title']}",
            retrieval_queries=list(dict.fromkeys([*brief["source_urls"], *brief["provenance_links"]])),
        )
        return {
            "brief_id": brief_id,
            "signal": signal,
            "missing_dimension": missing_dimension,
        }

    def route_brief(
        self,
        brief_id: str,
        *,
        target_interface: str = "ChatGPT Plus",
        harvest_prompt: str | None = None,
        include_council_review: bool = False,
    ) -> dict[str, Any]:
        brief: dict[str, Any] | None = None
        try:
            brief = self.read_brief(brief_id)
            conn = self._db.get_connection("strategic_memory")
            task = conn.execute(
                "SELECT task_id, priority, source, title, follow_up_tasks FROM research_tasks WHERE task_id = ?",
                (brief["task_id"],),
            ).fetchone()
            if task is None:
                raise KeyError(brief["task_id"])
            actions: list[dict[str, Any]] = []
            operator_state, last_heartbeat_at = self._operator_state()
            opportunity_id: str | None = None
            if brief["actionability"] == "HARVEST_NEEDED":
                harvest = self._create_harvest_request(
                    task_id=brief["task_id"],
                    title=brief["title"],
                    summary=brief["summary"],
                    priority=task["priority"],
                    target_interface=target_interface,
                    operator_state=operator_state,
                    last_heartbeat_at=last_heartbeat_at,
                    prompt_text=harvest_prompt,
                )
                actions.append(harvest)
            if (
                brief["action_type"] == "opportunity_feed"
                and brief["actionability"] == "WATCH"
                and (brief["source_diversity_hold"] or brief["quality_warning"])
            ):
                actions.extend(self._route_to_opportunity_feed(brief, task))
            if brief["actionability"] in {"ACTION_RECOMMENDED", "ACTION_REQUIRED"}:
                if brief["actionability"] == "ACTION_REQUIRED":
                    alert = self._create_brief_alert(brief, operator_state)
                    if alert is not None:
                        actions.append(alert)
                if brief["action_type"] == "opportunity_feed":
                    opportunity_action = self._route_to_opportunity_feed(brief, task)
                    actions.extend(opportunity_action)
                    opportunity_id = next(
                        (
                            item["opportunity_id"]
                            for item in opportunity_action
                            if item["type"] in {"opportunity_created", "opportunity_existing"}
                        ),
                        None,
                    )
            if include_council_review:
                actions.extend(
                    self._route_to_council(
                        brief,
                        task,
                        opportunity_id=opportunity_id,
                    )
                )
            result = {
                "brief_id": brief_id,
                "task_id": brief["task_id"],
                "actionability": brief["actionability"],
                "action_type": brief["action_type"],
                "operator_state": operator_state,
                "actions": actions,
            }
            self._log_trace(
                task_id=brief["task_id"],
                role="strategic_memory_routing",
                action_name="route_brief",
                intent_goal=f"Route intelligence brief {brief_id} into downstream governance and execution surfaces.",
                payload=result,
                context_assembled=(
                    f"actionability={brief['actionability']}; action_type={brief['action_type']}; "
                    f"operator_state={operator_state}; include_council_review={include_council_review}"
                ),
                retrieval_queries=list(
                    dict.fromkeys(
                        [
                            *brief["source_urls"],
                            *brief["provenance_links"],
                            *brief["related_brief_ids"],
                        ]
                    )
                ),
            )
            return result
        except Exception as exc:
            task_id = brief["task_id"] if brief is not None else brief_id
            retrieval_queries = (
                list(
                    dict.fromkeys(
                        [
                            *brief["source_urls"],
                            *brief["provenance_links"],
                            *brief["related_brief_ids"],
                        ]
                    )
                )
                if brief is not None
                else [brief_id]
            )
            context = (
                f"brief_id={brief_id}; include_council_review={include_council_review}"
                if brief is None
                else (
                    f"actionability={brief['actionability']}; action_type={brief['action_type']}; "
                    f"include_council_review={include_council_review}"
                )
            )
            self._log_trace(
                task_id=task_id,
                role="strategic_memory_routing",
                action_name="route_brief",
                intent_goal=f"Route intelligence brief {brief_id} into downstream governance and execution surfaces.",
                payload={"brief_id": brief_id, "error": str(exc)},
                context_assembled=context,
                retrieval_queries=retrieval_queries,
                judge_verdict="FAIL",
                judge_reasoning=f"Strategic memory routing failed: {exc}",
            )
            raise

    def _record_quality_signal(
        self,
        conn,
        brief_id: str,
        *,
        signal: str,
        missing_dimension: str | None = None,
        verdict_id: str | None = None,
    ) -> None:
        signal_id = str(uuid.uuid4())
        now = self._utc_now()
        effective_verdict_id = verdict_id
        if effective_verdict_id is None:
            placeholder = conn.execute(
                "SELECT verdict_id FROM council_verdicts WHERE decision_type = 'opportunity_screen' ORDER BY created_at ASC LIMIT 1"
            ).fetchone()
            if placeholder is None:
                effective_verdict_id = str(uuid.uuid4())
                conn.execute(
                    """
                    INSERT INTO council_verdicts (
                        verdict_id, tier_used, decision_type, recommendation, confidence,
                        reasoning_summary, dissenting_views, minority_positions,
                        full_debate_record, cost_usd, project_id, outcome_record,
                        da_quality_score, da_assessment, tie_break, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        effective_verdict_id,
                        1,
                        "opportunity_screen",
                        "INSUFFICIENT_DATA",
                        0.0,
                        "auto-generated quality placeholder",
                        None,
                        json.dumps([]),
                        None,
                        0.0,
                        None,
                        json.dumps({"generated_for": "brief_quality"}),
                        None,
                        json.dumps([]),
                        0,
                        now,
                    ),
                )
            else:
                effective_verdict_id = placeholder["verdict_id"]
        conn.execute(
            """
            INSERT INTO brief_quality_signals (
                signal_id, verdict_id, brief_id, signal, missing_dimension, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (signal_id, effective_verdict_id, brief_id, signal, missing_dimension, now),
        )

    def _create_harvest_request(
        self,
        *,
        task_id: str,
        title: str,
        summary: str,
        priority: str,
        target_interface: str,
        operator_state: str,
        last_heartbeat_at: str | None,
        prompt_text: str | None,
    ) -> dict[str, Any]:
        operator = self._db.get_connection("operator_digest")
        existing = operator.execute(
            """
            SELECT harvest_id, status, expires_at
            FROM harvest_requests
            WHERE task_id = ? AND target_interface = ? AND status = 'PENDING'
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (task_id, target_interface),
        ).fetchone()
        if existing is not None:
            return {
                "type": "harvest_request_existing",
                "harvest_id": existing["harvest_id"],
                "status": existing["status"],
                "expires_at": existing["expires_at"],
            }
        if operator_state == "ABSENT":
            return {
                "type": "harvest_request_skipped",
                "reason": "operator_absent",
            }
        now = self._utc_now()
        harvest_id = str(uuid.uuid4())
        expires_at = self._harvest_expiry(now, operator_state, last_heartbeat_at)
        operator.execute(
            """
            INSERT INTO harvest_requests (
                harvest_id, task_id, prompt_text, target_interface, context_summary,
                priority, status, expires_at, operator_result, relevance_score,
                clarification_sent, created_at, delivered_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                harvest_id,
                task_id,
                prompt_text or self._default_harvest_prompt(title, summary),
                target_interface,
                self._limit_words(summary, 100),
                priority,
                "PENDING",
                expires_at,
                None,
                None,
                0,
                now,
                None,
            ),
        )
        operator.commit()
        return {
            "type": "harvest_request_created",
            "harvest_id": harvest_id,
            "expires_at": expires_at,
            "target_interface": target_interface,
        }

    def _create_brief_alert(self, brief: dict[str, Any], operator_state: str) -> dict[str, Any] | None:
        if operator_state == "ABSENT":
            return None
        from skills.operator_interface.skill import OperatorInterfaceSkill

        operator = OperatorInterfaceSkill(self._db)
        tier = "T3" if brief["urgency"] == "CRITICAL" else "T2"
        alert_id = operator.alert(
            tier,
            "BRIEF_ACTION_REQUIRED",
            self._limit_words(f"{brief['title']}: {brief['summary']}", 40),
            channel_delivered="CLI" if operator_state == "ACTIVE" else None,
        )
        return {
            "type": "operator_alert_created",
            "alert_id": alert_id,
            "tier": tier,
        }

    def _route_to_council(
        self,
        brief: dict[str, Any],
        task,
        *,
        opportunity_id: str | None,
    ) -> list[dict[str, Any]]:
        from skills.council.skill import council_entry

        subject_id = opportunity_id or brief["brief_id"]
        decision_type = (
            "opportunity_screen"
            if opportunity_id is not None or brief["action_type"] == "opportunity_feed"
            else "system_critical"
            if brief["urgency"] == "CRITICAL"
            else "operator_strategic"
        )
        context = json.dumps(
            {
                "brief": {
                    "brief_id": brief["brief_id"],
                    "title": brief["title"],
                    "summary": self._limit_words(brief["summary"], 120),
                    "confidence": brief["confidence"],
                    "actionability": brief["actionability"],
                    "urgency": brief["urgency"],
                    "action_type": brief["action_type"],
                    "quality_warning": brief["quality_warning"],
                    "source_diversity_hold": brief["source_diversity_hold"],
                    "tags": brief["tags"],
                    "provenance_links": brief["provenance_links"],
                },
                "task": {
                    "task_id": task["task_id"],
                    "priority": task["priority"],
                    "source": task["source"],
                    "title": task["title"],
                },
                "opportunity": None
                if opportunity_id is None
                else {
                    "opportunity_id": opportunity_id,
                    "title": brief["title"],
                    "thesis": self._limit_words(brief["summary"], 80),
                },
            },
            sort_keys=True,
        )
        try:
            verdict = council_entry(
                action="deliberate",
                decision_type=decision_type,
                subject_id=subject_id,
                context=context,
                source_briefs=[brief["brief_id"]],
            )
        except RuntimeError as exc:
            if "not configured" in str(exc):
                return [{"type": "council_review_skipped", "reason": "council_not_configured"}]
            raise
        if opportunity_id is not None:
            conn = self._db.get_connection("strategic_memory")
            conn.execute(
                """
                UPDATE opportunity_records
                SET council_verdict_id = COALESCE(council_verdict_id, ?),
                    updated_at = ?
                WHERE opportunity_id = ?
                """,
                (verdict.verdict_id, self._utc_now(), opportunity_id),
            )
            conn.commit()
        return [
            {
                "type": "council_review_created",
                "decision_type": verdict.decision_type.value,
                "subject_id": subject_id,
                "verdict_id": verdict.verdict_id,
                "recommendation": verdict.recommendation.value,
                "confidence": verdict.confidence,
                "tier_used": verdict.tier_used,
            }
        ]

    def _route_to_opportunity_feed(self, brief: dict[str, Any], task) -> list[dict[str, Any]]:
        if brief["spawned_opportunity_id"]:
            return [
                {
                    "type": "opportunity_existing",
                    "opportunity_id": brief["spawned_opportunity_id"],
                }
            ]
        if brief["source_diversity_hold"] or brief["quality_warning"]:
            follow_up_task_id = self._create_corroboration_task(brief, task)
            self._append_spawned_task(brief["brief_id"], brief["task_id"], follow_up_task_id)
            return [
                {
                    "type": "opportunity_deferred",
                    "reason": "source_diversity_hold" if brief["source_diversity_hold"] else "quality_warning",
                    "follow_up_task_id": follow_up_task_id,
                }
            ]
        from skills.opportunity_pipeline.skill import OpportunityPipelineSkill

        pipeline = OpportunityPipelineSkill(self._db)
        opportunity_id = pipeline.create_opportunity(
            brief["title"],
            brief["summary"],
            income_mechanism=self._infer_income_mechanism(brief),
            detected_by=self._detected_by_for_brief(task["source"]),
            cashflow_estimate={"low": 0, "mid": 0, "high": 0, "currency": "USD", "period": "month"},
            provenance_links=[brief["brief_id"], *brief["provenance_links"]],
            trust_tier=brief["trust_tier"],
        )
        pipeline.transition_opportunity(
            opportunity_id,
            "SCREENED",
            validation_report=f"Auto-screened from brief {brief['brief_id']}.",
        )
        opportunity = pipeline.transition_opportunity(
            opportunity_id,
            "QUALIFIED",
            validation_report=f"Qualified from brief {brief['brief_id']}.",
        )
        conn = self._db.get_connection("strategic_memory")
        conn.execute(
            """
            UPDATE intelligence_briefs
            SET spawned_opportunity_id = ?
            WHERE brief_id = ?
            """,
            (opportunity_id, brief["brief_id"]),
        )
        conn.commit()
        return [
            {
                "type": "opportunity_created",
                "opportunity_id": opportunity_id,
                "status": opportunity["status"],
            }
        ]

    def _create_corroboration_task(self, brief: dict[str, Any], task) -> str:
        from skills.research_domain.skill import ResearchDomainSkill

        research = ResearchDomainSkill(self._db)
        reason = "corroborate single-source finding" if brief["source_diversity_hold"] else "fill brief quality gaps"
        return research.create_task(
            f"Corroborate brief: {brief['title']}",
            f"Follow up on brief {brief['brief_id']} to {reason}. Summary: {brief['summary']}",
            priority="P1_HIGH" if brief["actionability"] == "ACTION_REQUIRED" else "P2_NORMAL",
            domain=brief["domain"],
            source="council",
            tags=list(dict.fromkeys([*brief["tags"], "corroboration", "qualified-brief"])),
        )

    def _append_spawned_task(self, brief_id: str, task_id: str, spawned_task_id: str) -> None:
        conn = self._db.get_connection("strategic_memory")
        brief_row = conn.execute(
            "SELECT spawned_tasks FROM intelligence_briefs WHERE brief_id = ?",
            (brief_id,),
        ).fetchone()
        task_row = conn.execute(
            "SELECT follow_up_tasks FROM research_tasks WHERE task_id = ?",
            (task_id,),
        ).fetchone()
        brief_tasks = [] if brief_row is None else json.loads(brief_row["spawned_tasks"])
        task_follow_ups = [] if task_row is None else json.loads(task_row["follow_up_tasks"])
        if spawned_task_id not in brief_tasks:
            brief_tasks.append(spawned_task_id)
        if spawned_task_id not in task_follow_ups:
            task_follow_ups.append(spawned_task_id)
        conn.execute(
            "UPDATE intelligence_briefs SET spawned_tasks = ? WHERE brief_id = ?",
            (json.dumps(brief_tasks), brief_id),
        )
        conn.execute(
            "UPDATE research_tasks SET follow_up_tasks = ?, updated_at = ? WHERE task_id = ?",
            (json.dumps(task_follow_ups), self._utc_now(), task_id),
        )
        conn.commit()

    @staticmethod
    def _infer_income_mechanism(brief: dict[str, Any]) -> str:
        tags = set(brief["tags"])
        if "client" in tags or "service" in tags:
            return "client_work"
        if "ip" in tags or "template" in tags:
            return "ip_asset"
        if "market" in tags:
            return "market_opportunity"
        return "software_product"

    @staticmethod
    def _detected_by_for_brief(source: str) -> str:
        return "research_loop" if source == "autonomous_loop" else "research_prompted"

    def _operator_state(self) -> tuple[str, str | None]:
        conn = self._db.get_connection("operator_digest")
        row = conn.execute(
            "SELECT timestamp FROM operator_heartbeat ORDER BY timestamp DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return "ABSENT", None
        last = row["timestamp"]
        seen = datetime.datetime.fromisoformat(last)
        now = datetime.datetime.now(datetime.timezone.utc)
        hours = (now - seen).total_seconds() / 3600
        if hours < 72:
            return "ACTIVE", last
        if hours < 168:
            return "CONSERVATIVE", last
        return "ABSENT", last

    @staticmethod
    def _harvest_expiry(now: str, operator_state: str, last_heartbeat_at: str | None) -> str:
        current = datetime.datetime.fromisoformat(now)
        if operator_state == "CONSERVATIVE":
            return (current + datetime.timedelta(hours=96)).replace(microsecond=0).isoformat()
        if last_heartbeat_at is not None:
            seen = datetime.datetime.fromisoformat(last_heartbeat_at)
            hours_since = (current - seen).total_seconds() / 3600
            if hours_since > 48:
                remaining = max(0.0, 168.0 - hours_since)
                ttl = min(max(48.0, remaining + 12.0), 96.0)
                return (current + datetime.timedelta(hours=ttl)).replace(microsecond=0).isoformat()
        return (current + datetime.timedelta(hours=48)).replace(microsecond=0).isoformat()

    @staticmethod
    def _default_harvest_prompt(title: str, summary: str) -> str:
        return (
            f"Research task: {title}\n"
            f"Context: {summary}\n"
            "Please gather the missing information needed to resolve this brief."
        )

    @staticmethod
    def _limit_words(text: str, max_words: int) -> str:
        words = text.split()
        if len(words) <= max_words:
            return text
        return " ".join(words[: max_words - 1] + ["..."])

    def _row_to_brief(self, row) -> dict[str, Any]:
        return asdict(
            IntelligenceBriefRecord(
                brief_id=row["brief_id"],
                task_id=row["task_id"],
                domain=row["domain"],
                title=row["title"],
                summary=row["summary"],
                detail=row["detail"],
                confidence=row["confidence"],
                actionability=row["actionability"],
                urgency=row["urgency"],
                depth_tier=row["depth_tier"],
                action_type=row["action_type"],
                source_urls=json.loads(row["source_urls"]),
                source_assessments=json.loads(row["source_assessments"]),
                uncertainty_statement=row["uncertainty_statement"],
                counter_thesis=row["counter_thesis"],
                spawned_tasks=json.loads(row["spawned_tasks"]),
                spawned_opportunity_id=row["spawned_opportunity_id"],
                related_brief_ids=json.loads(row["related_brief_ids"]),
                tags=json.loads(row["tags"]),
                quality_warning=bool(row["quality_warning"]),
                source_diversity_hold=bool(row["source_diversity_hold"]),
                provenance_links=json.loads(row["provenance_links"]),
                trust_tier=row["trust_tier"],
                created_at=row["created_at"],
            )
        )

    @staticmethod
    def _apply_source_diversity_gate(
        actionability: str,
        urgency: str,
        source_assessments: list[dict[str, Any]],
    ) -> tuple[str, bool]:
        source_types = {
            item.get("source_type")
            for item in source_assessments
            if item.get("source_type")
        }
        if len(source_types) >= 2:
            return actionability, False
        if actionability == "ACTION_RECOMMENDED":
            return "WATCH", True
        if actionability == "ACTION_REQUIRED" and urgency != "CRITICAL":
            return "ACTION_RECOMMENDED", True
        return actionability, False

    @staticmethod
    def _compute_quality_warning(
        *,
        depth_tier: str,
        confidence: float,
        source_assessments: list[dict[str, Any]],
        uncertainty_statement: str | None,
        counter_thesis: str | None,
    ) -> bool:
        if depth_tier != "FULL":
            return False
        if not uncertainty_statement or len(uncertainty_statement.split()) < 10:
            return True
        if not counter_thesis or len(counter_thesis.split()) < 10:
            return True
        if not any(item.get("freshness") for item in source_assessments):
            return True
        source_types = {
            item.get("source_type")
            for item in source_assessments
            if item.get("source_type")
        }
        if confidence > 0.80 and len(source_types) < 2:
            return True
        return False

    def _log_trace(
        self,
        *,
        task_id: str,
        role: str,
        action_name: str,
        intent_goal: str,
        payload: Any,
        context_assembled: str,
        retrieval_queries: list[str] | None = None,
        judge_verdict: str = "PASS",
        judge_reasoning: str | None = None,
    ) -> None:
        if not self._harness_variants.available:
            return
        self._harness_variants.log_skill_action_trace(
            task_id=task_id,
            role=role,
            skill_name="strategic_memory",
            action_name=action_name,
            intent_goal=intent_goal,
            action_payload=payload,
            context_assembled=context_assembled,
            retrieval_queries=retrieval_queries,
            judge_verdict=judge_verdict,
            judge_reasoning=judge_reasoning,
        )

    @staticmethod
    def _utc_now() -> str:
        return datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()


_SKILL: Optional[StrategicMemorySkill] = None


def configure_skill(db_manager: DatabaseManager):
    global _SKILL
    _SKILL = StrategicMemorySkill(db_manager)


def strategic_memory_entry(action: str, **kwargs):
    if _SKILL is None:
        raise RuntimeError("strategic memory skill not configured")
    if action == "write_brief":
        return _SKILL.write_brief(
            kwargs["task_id"],
            kwargs["title"],
            kwargs["summary"],
            kwargs.get("confidence", 0.5),
            domain=kwargs.get("domain", 2),
            source=kwargs.get("source", "operator"),
            actionability=kwargs.get("actionability", "INFORMATIONAL"),
            urgency=kwargs.get("urgency", "ROUTINE"),
            depth_tier=kwargs.get("depth_tier", "QUICK"),
            action_type=kwargs.get("action_type", "none"),
            tags=kwargs.get("tags"),
            provenance_links=kwargs.get("provenance_links"),
            detail=kwargs.get("detail"),
            source_urls=kwargs.get("source_urls"),
            source_assessments=kwargs.get("source_assessments"),
            uncertainty_statement=kwargs.get("uncertainty_statement"),
            counter_thesis=kwargs.get("counter_thesis"),
            spawned_tasks=kwargs.get("spawned_tasks"),
            spawned_opportunity_id=kwargs.get("spawned_opportunity_id"),
            related_brief_ids=kwargs.get("related_brief_ids"),
            trust_tier=kwargs.get("trust_tier", 3),
        )
    if action == "read_brief":
        return _SKILL.read_brief(kwargs["brief_id"])
    if action == "list_briefs":
        return _SKILL.list_briefs(
            limit=kwargs.get("limit", 20),
            task_id=kwargs.get("task_id"),
            actionability=kwargs.get("actionability"),
            source_diversity_hold=kwargs.get("source_diversity_hold"),
            quality_warning=kwargs.get("quality_warning"),
        )
    if action == "record_quality_signal":
        return _SKILL.record_quality_signal(
            kwargs["brief_id"],
            kwargs["signal"],
            missing_dimension=kwargs.get("missing_dimension"),
            verdict_id=kwargs.get("verdict_id"),
        )
    if action == "route_brief":
        return _SKILL.route_brief(
            kwargs["brief_id"],
            target_interface=kwargs.get("target_interface", "ChatGPT Plus"),
            harvest_prompt=kwargs.get("harvest_prompt"),
            include_council_review=kwargs.get("include_council_review", False),
        )
    raise ValueError(f"Unknown action: {action}")
