from __future__ import annotations

import datetime
import json
import uuid
from dataclasses import asdict, dataclass
from typing import Any, Optional

from harness_variants import HarnessVariantManager
from skills.db_manager import DatabaseManager


VALID_TASK_TRANSITIONS = {
    "PENDING": {"ACTIVE", "COMPLETE", "CANCELLED", "STALE"},
    "ACTIVE": {"COMPLETE", "FAILED", "CANCELLED", "STALE"},
    "STALE": {"ACTIVE", "FAILED", "CANCELLED"},
    "FAILED": {"ACTIVE", "CANCELLED"},
    "COMPLETE": set(),
    "CANCELLED": set(),
}


@dataclass(frozen=True)
class ResearchTaskRecord:
    task_id: str
    domain: int
    source: str
    title: str
    brief: str
    priority: str
    status: str
    max_spend_usd: float
    actual_spend_usd: float
    output_brief_id: str | None
    follow_up_tasks: list[str]
    stale_after: str | None
    tags: list[str]
    depth_upgrade: bool
    created_at: str
    updated_at: str


class ResearchDomainSkill:
    def __init__(self, db_manager: DatabaseManager):
        self._db = db_manager
        self._harness_variants = HarnessVariantManager(str(db_manager.data_dir / "telemetry.db"))

    def create_task(
        self,
        title: str,
        brief: str,
        priority: str = "P2_NORMAL",
        *,
        domain: int = 2,
        source: str = "operator",
        tags: list[str] | None = None,
        max_spend_usd: float = 0.0,
        stale_after: str | None = None,
    ) -> str:
        task_id = str(uuid.uuid4())
        now = self._utc_now()
        conn = self._db.get_connection("strategic_memory")
        conn.execute(
            """
            INSERT INTO research_tasks (
                task_id, domain, source, title, brief, priority, status,
                max_spend_usd, actual_spend_usd, output_brief_id, follow_up_tasks,
                stale_after, tags, depth_upgrade, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (task_id, domain, source, title, brief, priority, "PENDING", max_spend_usd, 0.0, None, "[]", stale_after, json.dumps(tags or []), 0, now, now),
        )
        conn.commit()
        return task_id

    def list_tasks(
        self,
        *,
        limit: int = 20,
        status: str | None = None,
        domain: int | None = None,
        priority: str | None = None,
    ) -> list[dict[str, Any]]:
        conn = self._db.get_connection("strategic_memory")
        where: list[str] = []
        params: list[object] = []
        if status:
            where.append("status = ?")
            params.append(status)
        if domain is not None:
            where.append("domain = ?")
            params.append(domain)
        if priority:
            where.append("priority = ?")
            params.append(priority)
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        rows = conn.execute(
            f"""
            SELECT
                task_id, domain, source, title, brief, priority, status,
                max_spend_usd, actual_spend_usd, output_brief_id, follow_up_tasks,
                stale_after, tags, depth_upgrade, created_at, updated_at
            FROM research_tasks
            {where_sql}
            ORDER BY
                CASE priority
                    WHEN 'P0_IMMEDIATE' THEN 0
                    WHEN 'P1_HIGH' THEN 1
                    WHEN 'P2_NORMAL' THEN 2
                    ELSE 3
                END,
                created_at DESC
            LIMIT ?
            """,
            (*params, limit),
        ).fetchall()
        return [self._row_to_task(row) for row in rows]

    def get_task(self, task_id: str) -> dict[str, Any]:
        return self._fetch_task(task_id)

    def start_task(self, task_id: str) -> dict[str, Any]:
        return self._transition_task(task_id, "ACTIVE")

    def mark_stale(self, task_id: str) -> dict[str, Any]:
        return self._transition_task(task_id, "STALE")

    def fail_task(
        self,
        task_id: str,
        *,
        actual_spend_usd: float | None = None,
        follow_up_tasks: list[str] | None = None,
    ) -> dict[str, Any]:
        return self._transition_task(
            task_id,
            "FAILED",
            actual_spend_usd=actual_spend_usd,
            follow_up_tasks=follow_up_tasks,
        )

    def cancel_task(
        self,
        task_id: str,
        *,
        follow_up_tasks: list[str] | None = None,
    ) -> dict[str, Any]:
        return self._transition_task(task_id, "CANCELLED", follow_up_tasks=follow_up_tasks)

    def complete_task(
        self,
        task_id: str,
        *,
        output_brief_id: str | None = None,
        actual_spend_usd: float | None = None,
        follow_up_tasks: list[str] | None = None,
    ) -> dict[str, Any]:
        if output_brief_id is not None:
            conn = self._db.get_connection("strategic_memory")
            brief = conn.execute(
                "SELECT task_id FROM intelligence_briefs WHERE brief_id = ?",
                (output_brief_id,),
            ).fetchone()
            if brief is None:
                self._log_trace(
                    task_id=task_id,
                    role="research_task_completion",
                    action_name="complete_task",
                    intent_goal=f"Complete research task {task_id}",
                    payload={"error": f"missing_brief:{output_brief_id}"},
                    context_assembled=f"task_id={task_id}",
                    retrieval_queries=[],
                    judge_verdict="FAIL",
                    judge_reasoning=f"Brief {output_brief_id} was not found.",
                )
                raise KeyError(output_brief_id)
            if brief["task_id"] != task_id:
                self._log_trace(
                    task_id=task_id,
                    role="research_task_completion",
                    action_name="complete_task",
                    intent_goal=f"Complete research task {task_id}",
                    payload={"error": "brief_task_mismatch", "brief_id": output_brief_id},
                    context_assembled=f"task_id={task_id}",
                    retrieval_queries=[],
                    judge_verdict="FAIL",
                    judge_reasoning="Completion brief did not belong to the task.",
                )
                raise ValueError("brief does not belong to task")
        result = self._transition_task(
            task_id,
            "COMPLETE",
            output_brief_id=output_brief_id,
            actual_spend_usd=actual_spend_usd,
            follow_up_tasks=follow_up_tasks,
        )
        self._log_trace(
            task_id=task_id,
            role="research_task_completion",
            action_name="complete_task",
            intent_goal=f"Complete research task {result['title']}",
            payload={
                "status": result["status"],
                "output_brief_id": result["output_brief_id"],
                "actual_spend_usd": result["actual_spend_usd"],
                "follow_up_tasks": result["follow_up_tasks"],
            },
            context_assembled=result["brief"],
            retrieval_queries=result["tags"],
            judge_reasoning="Research task completed and persisted.",
            source_chain_id=task_id,
        )
        return result

    def route_task_output(
        self,
        task_id: str,
        *,
        target_interface: str = "ChatGPT Plus",
        harvest_prompt: str | None = None,
        include_council_review: bool = False,
    ) -> dict[str, Any]:
        task = self._fetch_task(task_id)
        if task["output_brief_id"] is None:
            self._log_trace(
                task_id=task_id,
                role="research_task_routing",
                action_name="route_task_output",
                intent_goal=f"Route research output for task {task['title']}",
                payload={"error": "missing_output_brief"},
                context_assembled=task["brief"],
                retrieval_queries=task["tags"],
                judge_verdict="FAIL",
                judge_reasoning="Task had no output brief to route.",
                source_chain_id=task_id,
            )
            raise ValueError("task has no output brief to route")
        from skills.strategic_memory.skill import StrategicMemorySkill

        memory = StrategicMemorySkill(self._db)
        result = memory.route_brief(
            task["output_brief_id"],
            target_interface=target_interface,
            harvest_prompt=harvest_prompt,
            include_council_review=include_council_review,
        )
        self._log_trace(
            task_id=task_id,
            role="research_task_routing",
            action_name="route_task_output",
            intent_goal=f"Route research output for task {task['title']}",
            payload=result,
            context_assembled=task["brief"],
            retrieval_queries=task["tags"],
            judge_reasoning="Research task output routed into downstream operator or opportunity flow.",
            source_chain_id=task_id,
        )
        return result

    def _transition_task(
        self,
        task_id: str,
        new_status: str,
        *,
        output_brief_id: str | None = None,
        actual_spend_usd: float | None = None,
        follow_up_tasks: list[str] | None = None,
    ) -> dict[str, Any]:
        now = self._utc_now()
        conn = self._db.get_connection("strategic_memory")
        row = conn.execute("SELECT * FROM research_tasks WHERE task_id = ?", (task_id,)).fetchone()
        if row is None:
            raise KeyError(task_id)
        current_status = row["status"]
        if new_status not in VALID_TASK_TRANSITIONS.get(current_status, set()):
            raise ValueError(f"invalid transition {current_status} -> {new_status}")
        next_follow_ups = follow_up_tasks if follow_up_tasks is not None else json.loads(row["follow_up_tasks"])
        conn.execute(
            """
            UPDATE research_tasks
            SET status = ?,
                output_brief_id = COALESCE(?, output_brief_id),
                actual_spend_usd = COALESCE(?, actual_spend_usd),
                follow_up_tasks = ?,
                updated_at = ?
            WHERE task_id = ?
            """,
            (new_status, output_brief_id, actual_spend_usd, json.dumps(next_follow_ups), now, task_id),
        )
        conn.commit()
        return self._fetch_task(task_id)

    def _fetch_task(self, task_id: str) -> dict[str, Any]:
        conn = self._db.get_connection("strategic_memory")
        row = conn.execute(
            """
            SELECT
                task_id, domain, source, title, brief, priority, status,
                max_spend_usd, actual_spend_usd, output_brief_id, follow_up_tasks,
                stale_after, tags, depth_upgrade, created_at, updated_at
            FROM research_tasks WHERE task_id = ?
            """,
            (task_id,),
        ).fetchone()
        if row is None:
            raise KeyError(task_id)
        return self._row_to_task(row)

    @staticmethod
    def _row_to_task(row) -> dict[str, Any]:
        return asdict(
            ResearchTaskRecord(
                task_id=row["task_id"],
                domain=row["domain"],
                source=row["source"],
                title=row["title"],
                brief=row["brief"],
                priority=row["priority"],
                status=row["status"],
                max_spend_usd=row["max_spend_usd"],
                actual_spend_usd=row["actual_spend_usd"],
                output_brief_id=row["output_brief_id"],
                follow_up_tasks=json.loads(row["follow_up_tasks"]),
                stale_after=row["stale_after"],
                tags=json.loads(row["tags"]),
                depth_upgrade=bool(row["depth_upgrade"]),
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )
        )

    @staticmethod
    def _utc_now() -> str:
        return datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()

    def _log_trace(
        self,
        *,
        task_id: str,
        role: str,
        action_name: str,
        intent_goal: str,
        payload: Any,
        context_assembled: str,
        retrieval_queries: list[str],
        judge_verdict: str = "PASS",
        judge_reasoning: str | None = None,
        source_chain_id: str | None = None,
    ) -> None:
        if not self._harness_variants.available:
            return
        self._harness_variants.log_skill_action_trace(
            task_id=task_id,
            role=role,
            skill_name="research_domain",
            action_name=action_name,
            intent_goal=intent_goal,
            action_payload=payload,
            context_assembled=context_assembled,
            retrieval_queries=retrieval_queries,
            judge_verdict=judge_verdict,
            judge_reasoning=judge_reasoning,
            source_chain_id=source_chain_id,
        )


_SKILL: Optional[ResearchDomainSkill] = None


def configure_skill(db_manager: DatabaseManager):
    global _SKILL
    _SKILL = ResearchDomainSkill(db_manager)


def research_domain_entry(action: str, **kwargs):
    if _SKILL is None:
        raise RuntimeError("research domain skill not configured")
    if action == "create_task":
        return _SKILL.create_task(
            kwargs["title"],
            kwargs["brief"],
            kwargs.get("priority", "P2_NORMAL"),
            domain=kwargs.get("domain", 2),
            source=kwargs.get("source", "operator"),
            tags=kwargs.get("tags"),
            max_spend_usd=kwargs.get("max_spend_usd", 0.0),
            stale_after=kwargs.get("stale_after"),
        )
    if action == "list_tasks":
        return _SKILL.list_tasks(
            limit=kwargs.get("limit", 20),
            status=kwargs.get("status"),
            domain=kwargs.get("domain"),
            priority=kwargs.get("priority"),
        )
    if action == "get_task":
        return _SKILL.get_task(kwargs["task_id"])
    if action == "start_task":
        return _SKILL.start_task(kwargs["task_id"])
    if action == "mark_stale":
        return _SKILL.mark_stale(kwargs["task_id"])
    if action == "fail_task":
        return _SKILL.fail_task(
            kwargs["task_id"],
            actual_spend_usd=kwargs.get("actual_spend_usd"),
            follow_up_tasks=kwargs.get("follow_up_tasks"),
        )
    if action == "cancel_task":
        return _SKILL.cancel_task(
            kwargs["task_id"],
            follow_up_tasks=kwargs.get("follow_up_tasks"),
        )
    if action == "complete_task":
        return _SKILL.complete_task(
            kwargs["task_id"],
            output_brief_id=kwargs.get("output_brief_id"),
            actual_spend_usd=kwargs.get("actual_spend_usd"),
            follow_up_tasks=kwargs.get("follow_up_tasks"),
        )
    if action == "route_task_output":
        return _SKILL.route_task_output(
            kwargs["task_id"],
            target_interface=kwargs.get("target_interface", "ChatGPT Plus"),
            harvest_prompt=kwargs.get("harvest_prompt"),
            include_council_review=kwargs.get("include_council_review", False),
        )
    raise ValueError(f"Unknown action: {action}")
