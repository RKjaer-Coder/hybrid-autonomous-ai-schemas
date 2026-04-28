from __future__ import annotations

import datetime
import json
import os
import platform
import subprocess
import uuid
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from skills.db_manager import DatabaseManager
from skills.observability.skill import ObservabilitySkill
from skills.operator_interface.skill import OperatorInterfaceSkill


PRIORITIES = ("P0_IMMEDIATE", "P1_HIGH", "P2_NORMAL", "P3_BACKGROUND")
MANUAL_TASK_STATUSES = ("TODO", "IN_PROGRESS", "BLOCKED", "DONE")
FINAL_DASHBOARD_CONTRACT = "hermes-dashboard-plugin-v1"
RESEARCH_DOMAIN_LABELS = {
    1: "Security",
    2: "Model Ecosystem",
    3: "Business & Market",
    4: "Regulatory & Compliance",
    5: "Intelligence Opportunity",
}
RESEARCH_WORKFLOW_DEFINITIONS = (
    ("model_radar", "Model & Tooling Radar", "New models, MLX feasibility, role fit, benchmark deltas"),
    ("system_architecture", "System Architecture", "Efficiency, reliability, Hermes fit, simplification, control surfaces"),
    ("business_market", "Business & Opportunity", "Market demand, monetization, competitors, opportunity feed"),
    ("security_compliance", "Security & Compliance", "CVE, policy, regulatory, patch urgency, compliance deadlines"),
    ("operator_prompts", "Operator Prompts", "Ad hoc research explicitly assigned by the operator"),
    ("standing_monitoring", "Standing Briefs", "Scheduled monitoring loops and recurring intelligence"),
    ("harvest_followups", "Harvest Follow-ups", "Requests for missing external/manual evidence"),
)
PROJECT_LANES = (
    "PIPELINE",
    "VALIDATE",
    "BUILD",
    "DEPLOY",
    "OPERATE",
    "PAUSED",
    "KILL_REVIEW",
    "DONE",
)
TASK_LANES = ("TODO", "IN_PROGRESS", "BLOCKED", "DONE")


def _utc_now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat()


def _priority_rank(priority: str | None) -> int:
    try:
        return PRIORITIES.index(priority or "P3_BACKGROUND")
    except ValueError:
        return len(PRIORITIES)


def _task_lane_from_status(source: str, status: str | None) -> str:
    normalized = (status or "").upper()
    if source == "manual":
        return normalized if normalized in TASK_LANES else "TODO"
    if normalized in {"ACTIVE", "DELIVERED_PARTIAL"}:
        return "IN_PROGRESS"
    if normalized in {"FAILED", "CANCELLED", "STALE", "EXPIRED"}:
        return "BLOCKED"
    if normalized in {"COMPLETE", "DELIVERED", "DONE"}:
        return "DONE"
    return "TODO"


def _counter(rows: Any, key: str = "count") -> dict[str, int]:
    return {row[0]: int(row[key]) for row in rows}


def _json_list(raw: str | None) -> list[Any]:
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return []
    return parsed if isinstance(parsed, list) else []


def _research_workflow_id(row: Any) -> str:
    title = str(row["title"] or "").lower()
    brief = str(row["brief"] or "").lower()
    tags = {str(tag).lower() for tag in _json_list(row["tags"])}
    haystack = " ".join([title, brief, " ".join(sorted(tags))])
    domain = int(row["domain"])
    if any(term in haystack for term in ("model", "mlx", "llm", "benchmark", "frontier", "inference", "embedding", "lora", "quant")):
        return "model_radar"
    if any(term in haystack for term in ("architecture", "architectural", "efficiency", "efficient", "reliability", "hermes", "runtime", "dashboard", "token", "simplification")):
        return "system_architecture"
    if domain in {1, 4}:
        return "security_compliance"
    if domain == 3 or any(term in haystack for term in ("market", "business", "monetization", "competitor", "pricing", "customer", "opportunity")):
        return "business_market"
    if row["source"] == "operator":
        return "operator_prompts"
    if domain in {2, 5}:
        return "model_radar"
    return "operator_prompts"


def _research_tags_for_workflow(workflow_id: str) -> list[str]:
    return {
        "model_radar": ["model", "scout"],
        "system_architecture": ["architecture", "system"],
        "business_market": ["market", "opportunity"],
        "security_compliance": ["security", "compliance"],
        "operator_prompts": ["operator_prompt"],
        "standing_monitoring": ["standing_brief"],
        "harvest_followups": ["harvest"],
    }.get(workflow_id, ["operator_prompt"])


def _research_domain_for_workflow(workflow_id: str) -> int:
    return {
        "model_radar": 5,
        "system_architecture": 2,
        "business_market": 3,
        "security_compliance": 1,
        "operator_prompts": 2,
        "standing_monitoring": 2,
        "harvest_followups": 2,
    }.get(workflow_id, 2)


def _system_resource_pressure() -> dict[str, Any]:
    cpu_count = os.cpu_count() or 1
    load_1m = os.getloadavg()[0] if hasattr(os, "getloadavg") else 0.0
    cpu_pressure = min(load_1m / cpu_count, 1.0)
    ram = _memory_pressure()
    return {
        "cpu": {
            "label": "CPU",
            "pressure": round(cpu_pressure, 3),
            "detail": f"1m load {load_1m:.2f} across {cpu_count} cores",
        },
        "ram": ram,
        "gpu": {
            "label": "GPU",
            "pressure": None,
            "detail": "Not sampled; Apple GPU utilization needs heavier system tooling.",
        },
    }


def _memory_pressure() -> dict[str, Any]:
    if platform.system() == "Darwin":
        try:
            vm_result = subprocess.run(["vm_stat"], capture_output=True, text=True, check=True, timeout=1)
            first_line = vm_result.stdout.splitlines()[0] if vm_result.stdout else ""
            page_size_digits = "".join(ch for ch in first_line if ch.isdigit())
            page_size = int(page_size_digits or os.sysconf("SC_PAGE_SIZE"))
            values: dict[str, int] = {}
            for line in vm_result.stdout.splitlines():
                if ":" not in line:
                    continue
                key, raw = line.split(":", 1)
                digits = "".join(ch for ch in raw if ch.isdigit())
                if digits:
                    values[key.strip()] = int(digits)
            free = values.get("Pages free", 0) + values.get("Pages inactive", 0) + values.get("Pages speculative", 0)
            used_keys = (
                "Pages active",
                "Pages wired down",
                "Pages occupied by compressor",
                "Pages throttled",
            )
            used = sum(values.get(key, 0) for key in used_keys)
            total = free + used
            pressure = (used / total) if total else 0.0
            return {
                "label": "RAM",
                "pressure": round(min(pressure, 1.0), 3),
                "detail": f"{used * page_size / (1024 ** 3):.1f}GB used of {total * page_size / (1024 ** 3):.1f}GB",
            }
        except Exception:
            pass
    try:
        meminfo = Path("/proc/meminfo").read_text(encoding="utf-8")
        values = {}
        for line in meminfo.splitlines():
            key, raw = line.split(":", 1)
            values[key] = int(raw.strip().split()[0])
        total = values.get("MemTotal", 0)
        available = values.get("MemAvailable", 0)
        used = max(total - available, 0)
        pressure = (used / total) if total else 0.0
        return {
            "label": "RAM",
            "pressure": round(min(pressure, 1.0), 3),
            "detail": f"{used / (1024 ** 2):.1f}GB used of {total / (1024 ** 2):.1f}GB",
        }
    except Exception:
        return {"label": "RAM", "pressure": None, "detail": "Unavailable"}


class MissionControlService:
    def __init__(self, db_manager: DatabaseManager, *, interaction_channel: str = "mission_control"):
        self._db = db_manager
        self._operator = OperatorInterfaceSkill(db_manager)
        self._observability = ObservabilitySkill(db_manager, telemetry_buffer=None, immune_buffer=None)
        self._interaction_channel = interaction_channel

    def snapshot(self) -> dict[str, Any]:
        health = self._observability.system_health()
        workspace = self._operator.workspace_overview()
        alerts = self._operator.list_alerts(limit=8, include_suppressed=False)
        latest_digest = self._latest_digest()
        board = self.project_board()
        tasks = self.task_board()
        decisions = self.decisions()
        workflow = self.workflow()
        operator_focus = self.operator_focus(board, tasks, decisions)
        council = self.council()
        research = self.research()
        finance = self.finance()
        replay = self.replay()
        system = self.system()
        model_assignments = self.model_assignments()
        resource_pressure = _system_resource_pressure()
        usage = self.usage(resource_pressure)
        area_status = self.area_status(
            board, tasks, decisions, council, research, finance, replay, system, model_assignments
        )
        overview_flow = self.overview_flow(board, tasks, decisions, council, research, model_assignments)
        return {
            "contract": FINAL_DASHBOARD_CONTRACT,
            "generated_at": _utc_now(),
            "runtime_posture": {
                "substrate": "Hermes dashboard plugin",
                "mode": "prebuilt_without_live_hermes",
                "gate_actions_enabled": False,
                "heavy_services": [],
                "poll_interval_seconds": 15,
            },
            "resource_pressure": resource_pressure,
            "usage": usage,
            "model_assignments": model_assignments,
            "area_status": area_status,
            "overview_flow": overview_flow,
            "overview": {
                "runtime_status": workspace["runtime_status"],
                "replay_readiness": workspace["replay_readiness"],
                "judge_deadlock": workspace["judge_deadlock"],
                "milestone_health": workspace["milestone_health"],
                "heartbeat_state": health["heartbeat_state"],
                "pending_gates": health["pending_gates"],
                "pending_harvests": health["pending_harvests"],
                "pending_quarantines": health["quarantined_responses"]["pending_review_count"],
                "unacknowledged_t3_alerts": health["unacknowledged_t3_alerts"],
                "operator_load_hours": health["operator_load"]["estimated_hours"],
                "recommended_digest_type": health["recommended_digest_type"],
            },
            "latest_digest": latest_digest,
            "alerts": alerts,
            "workflow": workflow,
            "system_map": self.system_map(workflow, board, tasks, decisions),
            "operator_focus": operator_focus,
            "project_board": board,
            "tasks": tasks,
            "decisions": decisions,
            "council": council,
            "research": research,
            "finance": finance,
            "replay": replay,
            "system": system,
        }

    def overview_flow(
        self,
        board: dict[str, Any],
        tasks: dict[str, Any],
        decisions: dict[str, Any],
        council: dict[str, Any],
        research: dict[str, Any],
        model_assignments: list[dict[str, Any]],
    ) -> dict[str, Any]:
        research_summary = research.get("summary") or {}
        conversion = research.get("conversion_flow") or {}
        conversion_stages = {stage.get("id"): stage for stage in conversion.get("stages", [])}
        task_cards = tasks.get("cards", [])
        backlog_cards = [card for card in task_cards if card.get("lane") in {"TODO", "IN_PROGRESS", "BLOCKED"}]
        pending_gates = decisions.get("pending_gates", [])
        pending_quarantines = decisions.get("pending_quarantines", [])
        pending_g3 = decisions.get("pending_g3_requests", [])
        council_summary = council.get("summary") or {}
        opportunity_count = int((conversion_stages.get("opportunity") or {}).get("count") or 0)
        pending_decisions = len(pending_gates) + len(pending_quarantines) + len(pending_g3)
        follow_up_count = int(research_summary.get("pending_harvests") or 0)
        follow_up_count += sum(int(item.get("blocked") or 0) for item in research.get("workflows", []))

        def stage(
            stage_id: str,
            label: str,
            detail: str,
            *,
            count: int,
            status: str,
            area: str,
            pending: int = 0,
            blocked: int = 0,
        ) -> dict[str, Any]:
            return {
                "id": stage_id,
                "label": label,
                "detail": detail,
                "count": count,
                "status": status,
                "pending": pending,
                "blocked": blocked,
                "models": self._models_for_area(model_assignments, area),
            }

        return {
            "status": "operator_needed" if pending_decisions else ("blocked" if follow_up_count else "flowing"),
            "summary": {
                "pending_decisions": pending_decisions,
                "active_research": int((conversion_stages.get("task") or {}).get("count") or 0),
                "actionable_findings": int((conversion_stages.get("action_signal") or {}).get("count") or 0),
                "opportunity_candidates": opportunity_count,
                "backlog_items": len(backlog_cards),
                "follow_up_research": follow_up_count,
            },
            "main_stages": [
                stage(
                    "research_task",
                    "Research Tasks",
                    "Assigned, standing, council-requested, or operator-prompted questions.",
                    count=int((conversion_stages.get("task") or {}).get("count") or 0),
                    pending=int((research_summary.get("tasks_by_status") or {}).get("PENDING", 0)),
                    blocked=sum(int(item.get("blocked") or 0) for item in research.get("workflows", [])),
                    status="active",
                    area="Research",
                ),
                stage(
                    "finding",
                    "Findings",
                    "Structured intelligence briefs with confidence, uncertainty, and actionability.",
                    count=int((conversion_stages.get("brief") or {}).get("count") or 0),
                    pending=int((conversion_stages.get("action_signal") or {}).get("count") or 0),
                    status="active" if int((conversion_stages.get("brief") or {}).get("count") or 0) else "quiet",
                    area="Research",
                ),
                stage(
                    "opportunity",
                    "Opportunities",
                    "Research findings that create or strengthen a candidate opportunity.",
                    count=opportunity_count,
                    pending=opportunity_count,
                    status="active" if opportunity_count else "quiet",
                    area="Projects",
                ),
            ],
            "branch_stages": [
                stage(
                    "council",
                    "Council Review",
                    "Important or risky findings are routed through deliberation and hard gates.",
                    count=int(council_summary.get("total_verdicts") or 0),
                    pending=pending_decisions,
                    status="attention" if pending_decisions else "active",
                    area="Council",
                ),
                stage(
                    "task_backlog",
                    "Task Backlog",
                    "Approved work becomes operator or system tasks for execution.",
                    count=len(backlog_cards),
                    pending=sum(1 for card in backlog_cards if card.get("lane") in {"TODO", "IN_PROGRESS"}),
                    blocked=sum(1 for card in backlog_cards if card.get("lane") == "BLOCKED"),
                    status="blocked" if any(card.get("lane") == "BLOCKED" for card in backlog_cards) else ("active" if backlog_cards else "quiet"),
                    area="Projects",
                ),
                stage(
                    "further_research",
                    "Further Research",
                    "Thin, stale, or manually blocked findings loop back for more evidence.",
                    count=follow_up_count,
                    pending=int(research_summary.get("pending_harvests") or 0),
                    blocked=max(follow_up_count - int(research_summary.get("pending_harvests") or 0), 0),
                    status="attention" if follow_up_count else "quiet",
                    area="Research",
                ),
            ],
        }

    def workflow(self) -> dict[str, Any]:
        strategic = self._db.get_connection("strategic_memory")
        financial = self._db.get_connection("financial_ledger")
        operator = self._db.get_connection("operator_digest")

        opportunity_rows = strategic.execute(
            "SELECT status, COUNT(*) AS count FROM opportunity_records GROUP BY status"
        ).fetchall()
        project_rows = financial.execute(
            "SELECT status, COUNT(*) AS count FROM projects GROUP BY status"
        ).fetchall()
        phase_rows = financial.execute(
            """
            SELECT name, status, COUNT(*) AS count
            FROM phases
            GROUP BY name, status
            ORDER BY name, status
            """
        ).fetchall()
        research_rows = strategic.execute(
            "SELECT status, COUNT(*) AS count FROM research_tasks GROUP BY status"
        ).fetchall()
        queue_summary = {
            "pending_gates": int(operator.execute("SELECT COUNT(*) FROM gate_log WHERE status = 'PENDING'").fetchone()[0]),
            "pending_harvests": int(operator.execute("SELECT COUNT(*) FROM harvest_requests WHERE status = 'PENDING'").fetchone()[0]),
            "pending_manual_tasks": int(
                operator.execute(
                    "SELECT COUNT(*) FROM operator_manual_tasks WHERE status != 'DONE'"
                ).fetchone()[0]
            ),
        }
        steps = [
            {
                "id": "opportunity",
                "label": "Opportunity Pipeline",
                "count": sum(int(row["count"]) for row in opportunity_rows if row["status"] not in {"REJECTED", "CLOSED"}),
                "detail": {row["status"]: int(row["count"]) for row in opportunity_rows},
            },
            {
                "id": "projects",
                "label": "Projects",
                "count": sum(int(row["count"]) for row in project_rows if row["status"] not in {"COMPLETE", "KILLED"}),
                "detail": {row["status"]: int(row["count"]) for row in project_rows},
            },
            {
                "id": "phases",
                "label": "Phase Engine",
                "count": sum(int(row["count"]) for row in phase_rows if row["status"] in {"ACTIVE", "GATE_PENDING"}),
                "detail": {
                    f"{row['name']}:{row['status']}": int(row["count"])
                    for row in phase_rows
                    if row["status"] in {"ACTIVE", "GATE_PENDING"}
                },
            },
            {
                "id": "research",
                "label": "Research Tasks",
                "count": sum(int(row["count"]) for row in research_rows if row["status"] in {"PENDING", "ACTIVE", "STALE"}),
                "detail": {row["status"]: int(row["count"]) for row in research_rows},
            },
            {
                "id": "operator",
                "label": "Operator Queues",
                "count": sum(queue_summary.values()),
                "detail": queue_summary,
            },
        ]
        return {
            "steps": steps,
            "opportunities": {row["status"]: int(row["count"]) for row in opportunity_rows},
            "projects": {row["status"]: int(row["count"]) for row in project_rows},
            "queues": queue_summary,
        }

    def system_map(
        self,
        workflow: dict[str, Any],
        board: dict[str, Any],
        tasks: dict[str, Any],
        decisions: dict[str, Any],
    ) -> dict[str, Any]:
        workflow_counts = {step["id"]: int(step["count"]) for step in workflow.get("steps", [])}
        task_cards = tasks.get("cards", [])
        blocked_tasks = sum(1 for card in task_cards if card.get("lane") == "BLOCKED")
        active_tasks = sum(1 for card in task_cards if card.get("lane") in {"TODO", "IN_PROGRESS"})
        p0_items = sum(1 for card in board.get("cards", []) if card.get("priority") == "P0_IMMEDIATE")
        p0_items += sum(1 for card in task_cards if card.get("priority") == "P0_IMMEDIATE")
        pending_decisions = sum(
            len(decisions.get(key, []))
            for key in ("pending_gates", "pending_g3_requests", "pending_quarantines", "runtime_halts")
        )
        nodes = [
            {
                "id": "sense",
                "label": "Sense",
                "detail": "Opportunity and research intake",
                "count": workflow_counts.get("opportunity", 0) + workflow_counts.get("research", 0),
                "state": "active" if workflow_counts.get("opportunity", 0) or workflow_counts.get("research", 0) else "quiet",
            },
            {
                "id": "decide",
                "label": "Decide",
                "detail": "Council, gates, and operator calls",
                "count": pending_decisions,
                "state": "attention" if pending_decisions else "clear",
            },
            {
                "id": "build",
                "label": "Build",
                "detail": "Projects and phase engine",
                "count": workflow_counts.get("projects", 0) + workflow_counts.get("phases", 0),
                "state": "active" if workflow_counts.get("projects", 0) or workflow_counts.get("phases", 0) else "quiet",
            },
            {
                "id": "operate",
                "label": "Operate",
                "detail": "Tasks, harvests, and manual work",
                "count": active_tasks,
                "state": "blocked" if blocked_tasks else ("active" if active_tasks else "quiet"),
            },
            {
                "id": "learn",
                "label": "Learn",
                "detail": "Harness traces, reliability, and self-improvement",
                "count": workflow_counts.get("operator", 0),
                "state": "attention" if p0_items else "quiet",
            },
        ]
        edges = [
            {"from": "sense", "to": "decide", "label": "screen"},
            {"from": "decide", "to": "build", "label": "approve"},
            {"from": "build", "to": "operate", "label": "ship"},
            {"from": "operate", "to": "learn", "label": "trace"},
            {"from": "learn", "to": "sense", "label": "refine"},
        ]
        return {
            "nodes": nodes,
            "edges": edges,
            "pressure": {
                "p0_items": p0_items,
                "blocked_tasks": blocked_tasks,
                "pending_decisions": pending_decisions,
            },
        }

    def operator_focus(
        self,
        board: dict[str, Any],
        tasks: dict[str, Any],
        decisions: dict[str, Any],
    ) -> dict[str, Any]:
        project_cards = sorted(
            board.get("cards", []),
            key=lambda item: (_priority_rank(item.get("priority")), -int(item.get("pending_gate_count") or 0), item.get("name", "").lower()),
        )
        task_cards = sorted(
            tasks.get("cards", []),
            key=lambda item: (_priority_rank(item.get("priority")), item.get("lane") == "DONE", item.get("title", "").lower()),
        )
        decision_items: list[dict[str, Any]] = []
        for gate in decisions.get("pending_gates", [])[:6]:
            decision_items.append(
                {
                    "kind": gate.get("gate_type", "gate"),
                    "title": gate.get("trigger_description", "Pending gate"),
                    "target": gate.get("project_name") or gate.get("project_id") or "Unassigned",
                    "priority": "P0_IMMEDIATE",
                }
            )
        for request in decisions.get("pending_g3_requests", [])[:4]:
            decision_items.append(
                {
                    "kind": "G3",
                    "title": request.get("justification") or request.get("reason") or "Spend approval",
                    "target": request.get("project_id") or "Finance",
                    "priority": "P0_IMMEDIATE",
                }
            )
        return {
            "projects": [
                {
                    "id": card.get("project_id"),
                    "title": card.get("name"),
                    "priority": card.get("priority"),
                    "lane": card.get("lane"),
                    "focus_note": card.get("focus_note") or "",
                    "pending_gate_count": card.get("pending_gate_count") or 0,
                }
                for card in project_cards
                if card.get("priority") in {"P0_IMMEDIATE", "P1_HIGH"} or int(card.get("pending_gate_count") or 0) > 0
            ][:8],
            "tasks": [
                {
                    "id": card.get("id"),
                    "kind": card.get("kind"),
                    "title": card.get("title"),
                    "priority": card.get("priority"),
                    "lane": card.get("lane"),
                    "source": card.get("source"),
                }
                for card in task_cards
                if card.get("lane") != "DONE" and card.get("priority") in {"P0_IMMEDIATE", "P1_HIGH"}
            ][:8],
            "decisions": decision_items[:8],
        }

    def area_status(
        self,
        board: dict[str, Any],
        tasks: dict[str, Any],
        decisions: dict[str, Any],
        council: dict[str, Any],
        research: dict[str, Any],
        finance: dict[str, Any],
        replay: dict[str, Any],
        system: dict[str, Any],
        model_assignments: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        pending_gates = decisions.get("pending_gates", [])
        pending_g3 = decisions.get("pending_g3_requests", [])
        pending_quarantines = decisions.get("pending_quarantines", [])
        runtime_halts = decisions.get("runtime_halts", [])
        research_active = sum(int(item.get("active") or 0) for item in research.get("workflows", []))
        research_blocked = sum(int(item.get("blocked") or 0) for item in research.get("workflows", []))
        active_projects = sum(1 for card in board.get("cards", []) if card.get("status") in {"ACTIVE", "PAUSED", "KILL_RECOMMENDED"})
        project_gate_count = sum(int(card.get("pending_gate_count") or 0) for card in board.get("cards", []))
        replay_readiness = replay.get("readiness", {})
        replay_below_threshold = bool(replay_readiness.get("operator_ack_required_below_threshold"))
        breaker = system.get("circuit_breakers", {})
        runtime = system.get("runtime_control", {})
        runtime_state = str(runtime.get("lifecycle_state") or "UNKNOWN")
        return [
            self._area(
                "Research",
                "Research tasks, standing briefs, harvest follow-ups",
                active=research_active,
                pending=int((research.get("summary") or {}).get("pending_harvests") or 0),
                blocked=research_blocked,
                operator_needed=False,
                models=self._models_for_area(model_assignments, "Research"),
            ),
            self._area(
                "Council",
                "Deliberation, escalation, and confirmation",
                active=int((council.get("summary") or {}).get("total_verdicts") or 0),
                pending=int((council.get("summary") or {}).get("pending_tier2_g3") or 0),
                blocked=0,
                operator_needed=bool((council.get("summary") or {}).get("pending_tier2_g3")),
                models=self._models_for_area(model_assignments, "Council"),
            ),
            self._area(
                "Projects",
                "Pipeline, build, deploy, operate",
                active=active_projects,
                pending=project_gate_count,
                blocked=sum(1 for card in board.get("cards", []) if card.get("lane") == "KILL_REVIEW"),
                operator_needed=bool(pending_gates),
                models=self._models_for_area(model_assignments, "Projects"),
            ),
            self._area(
                "Finance",
                "$0 autonomous paid spend posture and approvals",
                active=len(finance.get("route_mix", [])),
                pending=len(pending_g3),
                blocked=int((finance.get("summary") or {}).get("g3_by_status", {}).get("PENDING", 0)),
                operator_needed=bool(pending_g3),
                models=self._models_for_area(model_assignments, "Finance"),
            ),
            self._area(
                "Self-Improvement",
                "Hermes harness readiness, traces, and reliability",
                active=len(replay.get("recent_traces", [])),
                pending=0,
                blocked=1 if replay_below_threshold else 0,
                operator_needed=replay_below_threshold,
                models=self._models_for_area(model_assignments, "Self-Improvement"),
            ),
        ]

    @staticmethod
    def _area(
        name: str,
        detail: str,
        *,
        active: int,
        pending: int,
        blocked: int,
        operator_needed: bool,
        forced_state: str | None = None,
        models: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        state = forced_state or ("yellow" if operator_needed else ("red" if blocked > 0 and active == 0 else "green"))
        return {
            "name": name,
            "detail": detail,
            "state": state,
            "active": active,
            "pending": pending,
            "blocked": blocked,
            "operator_needed": operator_needed,
            "models": models or [],
        }

    @staticmethod
    def _models_for_area(assignments: list[dict[str, Any]], area: str) -> list[dict[str, Any]]:
        return [item for item in assignments if item.get("area") == area][:3]

    def model_assignments(self) -> list[dict[str, Any]]:
        financial = self._db.get_connection("financial_ledger")
        rows = financial.execute(
            """
            SELECT
                role,
                route_selected,
                COALESCE(NULLIF(model_used, ''), 'unassigned') AS model_used,
                COUNT(*) AS count,
                MAX(created_at) AS last_used_at
            FROM routing_decisions
            GROUP BY role, route_selected, COALESCE(NULLIF(model_used, ''), 'unassigned')
            ORDER BY last_used_at DESC, count DESC
            LIMIT 20
            """
        ).fetchall()
        area_by_role = {
            "Primary Reasoning": "Projects",
            "Execution": "Projects",
            "Validation": "Council",
            "Training/Reward": "Self-Improvement",
            "Embedding": "Research",
            "Cloud Escalation": "Finance",
        }
        return [
            {
                "area": area_by_role.get(row["role"], "Projects"),
                "role": row["role"],
                "route": row["route_selected"],
                "model": row["model_used"],
                "count": int(row["count"]),
                "last_used_at": row["last_used_at"],
            }
            for row in rows
        ]

    def council(self) -> dict[str, Any]:
        strategic = self._db.get_connection("strategic_memory")
        operator = self._db.get_connection("operator_digest")
        verdict_rows = strategic.execute(
            """
            SELECT
                verdict_id, tier_used, decision_type, recommendation, confidence,
                reasoning_summary, dissenting_views, project_id, da_quality_score,
                tie_break, degraded, confidence_cap, created_at
            FROM council_verdicts
            ORDER BY created_at DESC
            LIMIT 10
            """
        ).fetchall()
        by_type = strategic.execute(
            "SELECT decision_type, COUNT(*) AS count FROM council_verdicts GROUP BY decision_type"
        ).fetchall()
        quality = strategic.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN tier_used = 2 THEN 1 ELSE 0 END) AS tier2,
                SUM(CASE WHEN degraded = 1 THEN 1 ELSE 0 END) AS degraded,
                SUM(CASE WHEN tie_break = 1 THEN 1 ELSE 0 END) AS tie_breaks,
                AVG(confidence) AS avg_confidence,
                AVG(da_quality_score) AS avg_da_quality
            FROM council_verdicts
            """
        ).fetchone()
        pending_tier2 = operator.execute(
            """
            SELECT gate_id, gate_type, trigger_description, project_id, expires_at
            FROM gate_log
            WHERE gate_type = 'G3' AND status = 'PENDING' AND trigger_description LIKE 'council_tier2:%'
            ORDER BY expires_at ASC
            LIMIT 8
            """
        ).fetchall()
        return {
            "summary": {
                "total_verdicts": int(quality["total"] or 0),
                "tier2_verdicts": int(quality["tier2"] or 0),
                "degraded_verdicts": int(quality["degraded"] or 0),
                "tie_breaks": int(quality["tie_breaks"] or 0),
                "avg_confidence": float(quality["avg_confidence"] or 0.0),
                "avg_da_quality": float(quality["avg_da_quality"] or 0.0),
                "pending_tier2_g3": len(pending_tier2),
            },
            "by_decision_type": _counter(by_type, "count"),
            "recent_verdicts": [dict(row) for row in verdict_rows],
            "pending_tier2_gates": [dict(row) for row in pending_tier2],
        }

    def research(self) -> dict[str, Any]:
        strategic = self._db.get_connection("strategic_memory")
        operator = self._db.get_connection("operator_digest")
        task_rows = strategic.execute(
            """
            SELECT task_id, domain, source, title, brief, priority, status, tags,
                   depth_upgrade, created_at, updated_at
            FROM research_tasks
            ORDER BY
                CASE priority
                    WHEN 'P0_IMMEDIATE' THEN 0
                    WHEN 'P1_HIGH' THEN 1
                    WHEN 'P2_NORMAL' THEN 2
                    ELSE 3
                END,
                updated_at DESC
            LIMIT 80
            """
        ).fetchall()
        domain_rows = strategic.execute(
            """
            SELECT domain, status, COUNT(*) AS count
            FROM research_tasks
            GROUP BY domain, status
            ORDER BY domain, status
            """
        ).fetchall()
        brief_rows = strategic.execute(
            """
            SELECT brief_id, task_id, domain, title, summary, confidence, actionability,
                   urgency, depth_tier, quality_warning, source_diversity_hold, created_at
            FROM intelligence_briefs
            ORDER BY created_at DESC
            LIMIT 8
            """
        ).fetchall()
        standing_rows = strategic.execute(
            """
            SELECT standing_brief_id, domain, title, status, target_interface, last_run_at, updated_at
            FROM standing_briefs
            ORDER BY updated_at DESC
            LIMIT 8
            """
        ).fetchall()
        harvest_rows = operator.execute(
            """
            SELECT harvest_id, task_id, target_interface, priority, status, expires_at, created_at
            FROM harvest_requests
            WHERE status IN ('PENDING', 'DELIVERED_PARTIAL', 'EXPIRED')
            ORDER BY
                CASE priority
                    WHEN 'P0_IMMEDIATE' THEN 0
                    WHEN 'P1_HIGH' THEN 1
                    WHEN 'P2_NORMAL' THEN 2
                    ELSE 3
                END,
                expires_at ASC
            LIMIT 10
            """
        ).fetchall()
        actionable_rows = strategic.execute(
            """
            SELECT brief_id, task_id, domain, title, summary, confidence, actionability,
                   urgency, action_type, spawned_opportunity_id, created_at
            FROM intelligence_briefs
            WHERE actionability IN ('ACTION_RECOMMENDED','ACTION_REQUIRED','HARVEST_NEEDED')
               OR action_type IN ('council_review','opportunity_feed','operator_surface','security_escalation')
               OR spawned_opportunity_id IS NOT NULL
            ORDER BY created_at DESC
            LIMIT 8
            """
        ).fetchall()
        status_rows = strategic.execute(
            "SELECT status, COUNT(*) AS count FROM research_tasks GROUP BY status"
        ).fetchall()
        source_rows = strategic.execute(
            "SELECT source, COUNT(*) AS count FROM research_tasks GROUP BY source"
        ).fetchall()
        brief_quality = strategic.execute(
            """
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN actionability IN ('ACTION_RECOMMENDED','ACTION_REQUIRED','HARVEST_NEEDED') THEN 1 ELSE 0 END) AS actionable,
                SUM(CASE WHEN quality_warning = 1 OR source_diversity_hold = 1 THEN 1 ELSE 0 END) AS quality_holds,
                AVG(confidence) AS avg_confidence
            FROM intelligence_briefs
            """
        ).fetchone()
        opportunity_from_research = strategic.execute(
            """
            SELECT COUNT(*) AS count
            FROM opportunity_records
            WHERE detected_by IN ('research_loop', 'research_prompted')
            """
        ).fetchone()
        council_action_briefs = strategic.execute(
            """
            SELECT COUNT(*) AS count
            FROM intelligence_briefs
            WHERE action_type IN ('council_review', 'security_escalation')
            """
        ).fetchone()
        domain_matrix: dict[str, dict[str, int]] = {}
        for row in domain_rows:
            domain_matrix.setdefault(str(row["domain"]), {})[row["status"]] = int(row["count"])
        workflow_map = {
            workflow_id: {
                "id": workflow_id,
                "label": label,
                "purpose": purpose,
                "total": 0,
                "active": 0,
                "blocked": 0,
                "p0_p1": 0,
                "tasks": [],
            }
            for workflow_id, label, purpose in RESEARCH_WORKFLOW_DEFINITIONS
        }
        for row in task_rows:
            workflow_id = _research_workflow_id(row)
            workflow = workflow_map[workflow_id]
            status = str(row["status"])
            priority = str(row["priority"])
            workflow["total"] += 1
            workflow["active"] += 1 if status in {"PENDING", "ACTIVE", "STALE"} else 0
            workflow["blocked"] += 1 if status in {"FAILED", "CANCELLED", "STALE"} else 0
            workflow["p0_p1"] += 1 if priority in {"P0_IMMEDIATE", "P1_HIGH"} else 0
            if len(workflow["tasks"]) < 4:
                workflow["tasks"].append(
                    {
                        "task_id": row["task_id"],
                        "title": row["title"],
                        "priority": priority,
                        "status": status,
                        "source": row["source"],
                        "domain": int(row["domain"]),
                        "domain_label": RESEARCH_DOMAIN_LABELS.get(int(row["domain"]), f"Domain {row['domain']}"),
                    }
                )
        standing_active = sum(1 for row in standing_rows if row["status"] == "ACTIVE")
        standing_workflow = workflow_map["standing_monitoring"]
        standing_workflow["total"] = len(standing_rows)
        standing_workflow["active"] = standing_active
        standing_workflow["blocked"] = sum(1 for row in standing_rows if row["status"] in {"PAUSED", "ARCHIVED"})
        standing_workflow["p0_p1"] = standing_active
        standing_workflow["tasks"] = [
            {
                "task_id": row["standing_brief_id"],
                "title": row["title"],
                "priority": "P1_HIGH" if row["status"] == "ACTIVE" else "P3_BACKGROUND",
                "status": row["status"],
                "source": row["target_interface"],
                "domain": int(row["domain"]),
                "domain_label": RESEARCH_DOMAIN_LABELS.get(int(row["domain"]), f"Domain {row['domain']}"),
            }
            for row in standing_rows[:4]
        ]
        harvest_workflow = workflow_map["harvest_followups"]
        harvest_workflow["total"] = len(harvest_rows)
        harvest_workflow["active"] = sum(1 for row in harvest_rows if row["status"] in {"PENDING", "DELIVERED_PARTIAL"})
        harvest_workflow["blocked"] = sum(1 for row in harvest_rows if row["status"] == "EXPIRED")
        harvest_workflow["p0_p1"] = sum(1 for row in harvest_rows if row["priority"] in {"P0_IMMEDIATE", "P1_HIGH"})
        harvest_workflow["tasks"] = [
            {
                "task_id": row["harvest_id"],
                "title": row["target_interface"],
                "priority": row["priority"],
                "status": row["status"],
                "source": "harvest",
                "domain": None,
                "domain_label": "Manual evidence",
            }
            for row in harvest_rows[:4]
        ]
        model_lifecycle = {
            "scouted": int(strategic.execute("SELECT COUNT(*) FROM model_scout_reports").fetchone()[0]),
            "assessed": int(strategic.execute("SELECT COUNT(*) FROM model_assess_reports").fetchone()[0]),
            "shadow_trials": int(strategic.execute("SELECT COUNT(*) FROM shadow_trial_reports").fetchone()[0]),
        }
        return {
            "summary": {
                "tasks_by_status": _counter(status_rows, "count"),
                "tasks_by_source": _counter(source_rows, "count"),
                "active_standing_briefs": sum(1 for row in standing_rows if row["status"] == "ACTIVE"),
                "pending_harvests": sum(1 for row in harvest_rows if row["status"] == "PENDING"),
                "briefs_total": int(brief_quality["total"] or 0),
                "actionable_briefs": int(brief_quality["actionable"] or 0),
                "quality_holds": int(brief_quality["quality_holds"] or 0),
                "avg_brief_confidence": float(brief_quality["avg_confidence"] or 0.0),
            },
            "conversion_flow": {
                "stages": [
                    {
                        "id": "task",
                        "label": "Research Task",
                        "count": sum(1 for row in task_rows if row["status"] in {"PENDING", "ACTIVE", "STALE"}),
                        "detail": "Assigned, loop-generated, or council-requested question",
                    },
                    {
                        "id": "brief",
                        "label": "Intelligence Brief",
                        "count": int(brief_quality["total"] or 0),
                        "detail": "Structured finding with confidence and uncertainty",
                    },
                    {
                        "id": "action_signal",
                        "label": "Action Signal",
                        "count": int(brief_quality["actionable"] or 0),
                        "detail": "Finding recommends action, harvest, or operator attention",
                    },
                    {
                        "id": "opportunity",
                        "label": "Opportunity Candidate",
                        "count": int(opportunity_from_research["count"] or 0),
                        "detail": "Research creates or strengthens a commercial/system opportunity",
                    },
                    {
                        "id": "council",
                        "label": "Council Confirmation",
                        "count": int(council_action_briefs["count"] or 0),
                        "detail": "Important enough for deliberation or escalation",
                    },
                ],
                "actionable_briefs": [dict(row) for row in actionable_rows],
            },
            "domain_labels": {str(key): value for key, value in RESEARCH_DOMAIN_LABELS.items()},
            "domain_matrix": domain_matrix,
            "workflows": list(workflow_map.values()),
            "model_lifecycle": model_lifecycle,
            "recent_briefs": [dict(row) for row in brief_rows],
            "standing_briefs": [dict(row) for row in standing_rows],
            "harvest_queue": [dict(row) for row in harvest_rows],
        }

    def finance(self) -> dict[str, Any]:
        financial = self._db.get_connection("financial_ledger")
        pnl_rows = financial.execute(
            """
            SELECT project_id, name, revenue_to_date, direct_cost, net_to_date
            FROM project_pnl
            ORDER BY net_to_date DESC, name ASC
            LIMIT 10
            """
        ).fetchall()
        route_rows = financial.execute(
            """
            SELECT route_selected, COUNT(*) AS count, COALESCE(SUM(cost_usd), 0.0) AS cost_usd
            FROM routing_decisions
            GROUP BY route_selected
            ORDER BY count DESC
            """
        ).fetchall()
        spend = financial.execute(
            """
            SELECT
                COALESCE(SUM(amount_usd), 0.0) AS total_cost,
                COALESCE(SUM(CASE WHEN cost_category = 'cloud_api' THEN amount_usd ELSE 0.0 END), 0.0) AS cloud_cost,
                COALESCE(SUM(CASE WHEN cost_status = 'DISPUTED' THEN amount_usd ELSE 0.0 END), 0.0) AS disputed_cost
            FROM cost_records
            """
        ).fetchone()
        revenue = financial.execute(
            "SELECT COALESCE(SUM(amount_usd), 0.0) AS total_revenue FROM revenue_records"
        ).fetchone()
        g3_rows = financial.execute(
            "SELECT status, COUNT(*) AS count FROM g3_approval_requests GROUP BY status"
        ).fetchall()
        pending_g3 = self._operator.list_g3_approval_requests(limit=8, status="PENDING")
        return {
            "summary": {
                "total_revenue_usd": float(revenue["total_revenue"] or 0.0),
                "total_cost_usd": float(spend["total_cost"] or 0.0),
                "cloud_cost_usd": float(spend["cloud_cost"] or 0.0),
                "disputed_cost_usd": float(spend["disputed_cost"] or 0.0),
                "net_usd": float((revenue["total_revenue"] or 0.0) - (spend["total_cost"] or 0.0)),
                "g3_by_status": _counter(g3_rows, "count"),
                "autonomous_paid_spend_enabled": False,
            },
            "route_mix": [
                {"route": row["route_selected"], "count": int(row["count"]), "cost_usd": float(row["cost_usd"] or 0.0)}
                for row in route_rows
            ],
            "project_pnl": [dict(row) for row in pnl_rows],
            "pending_g3_requests": pending_g3,
        }

    def replay(self) -> dict[str, Any]:
        report = self._observability.replay_readiness_report()
        reliability = self._observability.reliability_dashboard(limit=10)
        traces = self._observability.execution_traces(limit=8)
        variants = self._observability.harness_variants(limit=8)
        frontier = self._observability.harness_frontier(limit=6)
        summary = self._observability.harness_variant_summary()
        return {
            "readiness": report,
            "reliability": reliability,
            "execution_trace_summary": summary["execution_traces"],
            "variant_summary": summary["variants"],
            "recent_traces": traces,
            "variants": variants,
            "frontier": frontier,
        }

    def usage(self, resource_pressure: dict[str, Any]) -> dict[str, Any]:
        telemetry = self._db.get_connection("telemetry")
        financial = self._db.get_connection("financial_ledger")
        trace_summary = telemetry.execute(
            """
            SELECT
                COUNT(*) AS trace_count,
                COALESCE(SUM(cost_usd), 0.0) AS trace_cost_usd,
                COALESCE(SUM(duration_ms), 0) AS duration_ms
            FROM execution_traces
            """
        ).fetchone()
        recent_traces = telemetry.execute(
            """
            SELECT trace_id, skill_name, role, steps_json, cost_usd, duration_ms, created_at
            FROM execution_traces
            ORDER BY created_at DESC
            LIMIT 40
            """
        ).fetchall()
        tokens_in = 0
        tokens_out = 0
        token_records = 0
        for row in recent_traces:
            extracted = self._extract_tokens(row["steps_json"])
            tokens_in += extracted["tokens_in"]
            tokens_out += extracted["tokens_out"]
            token_records += extracted["records"]
        route_rows = financial.execute(
            """
            SELECT route_selected, COUNT(*) AS count, COALESCE(SUM(cost_usd), 0.0) AS cost_usd
            FROM routing_decisions
            GROUP BY route_selected
            ORDER BY count DESC
            """
        ).fetchall()
        return {
            "resource_pressure": resource_pressure,
            "tokens": {
                "tracked": token_records > 0,
                "tokens_in": tokens_in,
                "tokens_out": tokens_out,
                "total": tokens_in + tokens_out,
                "records": token_records,
                "note": "Best-effort extraction from trace payloads; live Hermes token accounting is not attached yet.",
            },
            "traces": {
                "count": int(trace_summary["trace_count"] or 0),
                "cost_usd": float(trace_summary["trace_cost_usd"] or 0.0),
                "duration_ms": int(trace_summary["duration_ms"] or 0),
            },
            "routes": [
                {"route": row["route_selected"], "count": int(row["count"]), "cost_usd": float(row["cost_usd"] or 0.0)}
                for row in route_rows
            ],
        }

    @classmethod
    def _extract_tokens(cls, raw_json: str | None) -> dict[str, int]:
        try:
            payload = json.loads(raw_json or "[]")
        except json.JSONDecodeError:
            return {"tokens_in": 0, "tokens_out": 0, "records": 0}
        totals = {"tokens_in": 0, "tokens_out": 0, "records": 0}

        def walk(value: Any) -> None:
            if isinstance(value, dict):
                found = False
                if isinstance(value.get("tokens_in"), int):
                    totals["tokens_in"] += int(value["tokens_in"])
                    found = True
                if isinstance(value.get("tokens_out"), int):
                    totals["tokens_out"] += int(value["tokens_out"])
                    found = True
                if isinstance(value.get("token_count"), int) and not found:
                    totals["tokens_out"] += int(value["token_count"])
                    found = True
                if found:
                    totals["records"] += 1
                for child in value.values():
                    walk(child)
            elif isinstance(value, list):
                for child in value:
                    walk(child)

        walk(payload)
        return totals

    def system(self) -> dict[str, Any]:
        health = self._observability.system_health()
        return {
            "db_status": health["db_status"],
            "heartbeat_state": health["heartbeat_state"],
            "last_heartbeat_at": health["last_heartbeat_at"],
            "runtime_control": health["runtime_control"],
            "judge_deadlock": health["judge_deadlock"],
            "circuit_breakers": health["circuit_breakers"],
            "quarantined_responses": health["quarantined_responses"],
            "disputed_costs": health["disputed_costs"],
            "operator_load": health["operator_load"],
            "recommended_digest_type": health["recommended_digest_type"],
        }

    def project_board(self) -> dict[str, Any]:
        financial = self._db.get_connection("financial_ledger")
        operator = self._db.get_connection("operator_digest")
        preferences = {
            row["project_id"]: dict(row)
            for row in operator.execute(
                """
                SELECT project_id, priority, focus_note, updated_at
                FROM operator_project_preferences
                """
            ).fetchall()
        }
        pending_gate_counts = {
            row["project_id"]: int(row["pending_gate_count"])
            for row in operator.execute(
                """
                SELECT project_id, COUNT(*) AS pending_gate_count
                FROM gate_log
                WHERE status = 'PENDING' AND project_id IS NOT NULL
                GROUP BY project_id
                """
            ).fetchall()
        }
        rows = financial.execute(
            """
            WITH latest_phase AS (
                SELECT
                    p1.project_id,
                    p1.name,
                    p1.status,
                    p1.sequence,
                    p1.compute_budget,
                    p1.compute_consumed,
                    ROW_NUMBER() OVER (
                        PARTITION BY p1.project_id
                        ORDER BY
                            CASE p1.status
                                WHEN 'ACTIVE' THEN 0
                                WHEN 'GATE_PENDING' THEN 1
                                WHEN 'PENDING' THEN 2
                                WHEN 'COMPLETE' THEN 3
                                ELSE 4
                            END,
                            p1.sequence DESC
                    ) AS rn
                FROM phases p1
            ),
            latest_kill AS (
                SELECT
                    k1.project_id,
                    k1.kill_score,
                    k1.g2_status,
                    ROW_NUMBER() OVER (
                        PARTITION BY k1.project_id
                        ORDER BY k1.created_at DESC, k1.recommendation_id DESC
                    ) AS rn
                FROM kill_recommendations k1
            )
            SELECT
                p.project_id,
                p.opportunity_id,
                p.name,
                p.income_mechanism,
                p.thesis,
                p.portfolio_weight,
                p.status,
                p.kill_score_watch,
                p.cashflow_actual_usd,
                p.created_at,
                p.closed_at,
                lp.name AS phase_name,
                lp.status AS phase_status,
                lp.sequence AS phase_sequence,
                lp.compute_budget AS phase_budget,
                lp.compute_consumed AS phase_consumed,
                lk.kill_score,
                lk.g2_status
            FROM projects p
            LEFT JOIN latest_phase lp ON lp.project_id = p.project_id AND lp.rn = 1
            LEFT JOIN latest_kill lk ON lk.project_id = p.project_id AND lk.rn = 1
            ORDER BY p.created_at DESC, p.project_id DESC
            """
        ).fetchall()

        cards: list[dict[str, Any]] = []
        for row in rows:
            pref = preferences.get(row["project_id"], {})
            budget = json.loads(row["phase_budget"]) if row["phase_budget"] else {}
            consumed = json.loads(row["phase_consumed"]) if row["phase_consumed"] else {}
            lane = self._project_lane(row["status"], row["phase_name"], row["phase_status"])
            pending_gate_count = pending_gate_counts.get(row["project_id"], 0)
            priority = pref.get("priority") or self._derived_project_priority(
                {"status": row["status"], "pending_gate_count": pending_gate_count, "kill_score_watch": row["kill_score_watch"]}
            )
            executor_cap = float(budget.get("executor_hours_cap", 0.0) or 0.0)
            executor_used = float(consumed.get("executor_hours", 0.0) or 0.0)
            burn_ratio = round((executor_used / executor_cap), 3) if executor_cap > 0 else None
            cards.append(
                {
                    "project_id": row["project_id"],
                    "name": row["name"],
                    "income_mechanism": row["income_mechanism"],
                    "thesis": row["thesis"],
                    "status": row["status"],
                    "lane": lane,
                    "priority": priority,
                    "focus_note": pref.get("focus_note", ""),
                    "portfolio_weight": row["portfolio_weight"],
                    "cashflow_actual_usd": row["cashflow_actual_usd"],
                    "kill_score_watch": bool(row["kill_score_watch"]),
                    "kill_score": float(row["kill_score"]) if row["kill_score"] is not None else None,
                    "g2_status": row["g2_status"],
                    "phase_name": row["phase_name"],
                    "phase_status": row["phase_status"],
                    "phase_sequence": row["phase_sequence"],
                    "pending_gate_count": pending_gate_count,
                    "executor_burn_ratio": burn_ratio,
                    "created_at": row["created_at"],
                    "closed_at": row["closed_at"],
                }
            )

        cards.sort(
            key=lambda item: (
                PROJECT_LANES.index(item["lane"]) if item["lane"] in PROJECT_LANES else len(PROJECT_LANES),
                _priority_rank(item["priority"]),
                -item["pending_gate_count"],
                item["name"].lower(),
            )
        )
        lanes = []
        for lane in PROJECT_LANES:
            lane_cards = [card for card in cards if card["lane"] == lane]
            lanes.append(
                {
                    "id": lane,
                    "label": lane.replace("_", " ").title(),
                    "count": len(lane_cards),
                    "cards": lane_cards,
                }
            )
        return {"lanes": lanes, "cards": cards}

    def task_board(self) -> dict[str, Any]:
        operator = self._db.get_connection("operator_digest")
        strategic = self._db.get_connection("strategic_memory")
        financial = self._db.get_connection("financial_ledger")
        project_names = {
            row["project_id"]: row["name"]
            for row in financial.execute("SELECT project_id, name FROM projects").fetchall()
        }
        manual_rows = operator.execute(
            """
            SELECT task_id, project_id, title, details, status, priority, created_at, updated_at, completed_at
            FROM operator_manual_tasks
            ORDER BY
                CASE status
                    WHEN 'IN_PROGRESS' THEN 0
                    WHEN 'BLOCKED' THEN 1
                    WHEN 'TODO' THEN 2
                    ELSE 3
                END,
                CASE priority
                    WHEN 'P0_IMMEDIATE' THEN 0
                    WHEN 'P1_HIGH' THEN 1
                    WHEN 'P2_NORMAL' THEN 2
                    ELSE 3
                END,
                updated_at DESC
            LIMIT 40
            """
        ).fetchall()
        research_rows = strategic.execute(
            """
            SELECT task_id, domain, source, title, brief, priority, status, tags, created_at, updated_at
            FROM research_tasks
            WHERE status IN ('PENDING', 'ACTIVE', 'STALE', 'FAILED', 'COMPLETE')
            ORDER BY
                CASE priority
                    WHEN 'P0_IMMEDIATE' THEN 0
                    WHEN 'P1_HIGH' THEN 1
                    WHEN 'P2_NORMAL' THEN 2
                    ELSE 3
                END,
                updated_at DESC
            LIMIT 20
            """
        ).fetchall()
        harvest_rows = operator.execute(
            """
            SELECT harvest_id, task_id, prompt_text, target_interface, priority, status, expires_at, created_at
            FROM harvest_requests
            WHERE status IN ('PENDING', 'DELIVERED_PARTIAL', 'DELIVERED', 'EXPIRED')
            ORDER BY
                CASE priority
                    WHEN 'P0_IMMEDIATE' THEN 0
                    WHEN 'P1_HIGH' THEN 1
                    WHEN 'P2_NORMAL' THEN 2
                    ELSE 3
                END,
                created_at DESC
            LIMIT 20
            """
        ).fetchall()

        cards: list[dict[str, Any]] = []
        for row in manual_rows:
            workflow_id = "operator_manual"
            cards.append(
                {
                    "kind": "manual",
                    "workflow_id": workflow_id,
                    "workflow_label": "Operator Manual",
                    "id": row["task_id"],
                    "title": row["title"],
                    "details": row["details"],
                    "priority": row["priority"],
                    "status": row["status"],
                    "lane": _task_lane_from_status("manual", row["status"]),
                    "project_id": row["project_id"],
                    "project_name": project_names.get(row["project_id"]),
                    "source": "Manual",
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                }
            )
        for row in research_rows:
            workflow_id = _research_workflow_id(row)
            workflow_label = next(
                (label for current_id, label, _purpose in RESEARCH_WORKFLOW_DEFINITIONS if current_id == workflow_id),
                "Research",
            )
            cards.append(
                {
                    "kind": "research",
                    "workflow_id": workflow_id,
                    "workflow_label": workflow_label,
                    "id": row["task_id"],
                    "title": row["title"],
                    "details": row["brief"],
                    "priority": row["priority"],
                    "status": row["status"],
                    "lane": _task_lane_from_status("research", row["status"]),
                    "project_id": None,
                    "project_name": None,
                    "source": "Research",
                    "created_at": row["created_at"],
                    "updated_at": row["updated_at"],
                }
            )
        for row in harvest_rows:
            workflow_id = "harvest_followups"
            cards.append(
                {
                    "kind": "harvest",
                    "workflow_id": workflow_id,
                    "workflow_label": "Harvest Follow-ups",
                    "id": row["harvest_id"],
                    "title": row["target_interface"],
                    "details": row["prompt_text"],
                    "priority": row["priority"],
                    "status": row["status"],
                    "lane": _task_lane_from_status("harvest", row["status"]),
                    "project_id": None,
                    "project_name": None,
                    "source": "Harvest",
                    "created_at": row["created_at"],
                    "updated_at": row["expires_at"] or row["created_at"],
                }
            )
        cards.sort(
            key=lambda item: (
                TASK_LANES.index(item["lane"]) if item["lane"] in TASK_LANES else len(TASK_LANES),
                _priority_rank(item["priority"]),
                item["title"].lower(),
            )
        )
        lanes = []
        for lane in TASK_LANES:
            lane_cards = [card for card in cards if card["lane"] == lane]
            lanes.append(
                {
                    "id": lane,
                    "label": lane.replace("_", " ").title(),
                    "count": len(lane_cards),
                    "cards": lane_cards,
                }
            )
        workflow_defs = [
            ("operator_manual", "Operator Manual", "Tasks created directly by the operator."),
            *[
                (workflow_id, label, purpose)
                for workflow_id, label, purpose in RESEARCH_WORKFLOW_DEFINITIONS
            ],
        ]
        workflow_boards = []
        for workflow_id, label, purpose in workflow_defs:
            workflow_cards = [card for card in cards if card["workflow_id"] == workflow_id]
            if not workflow_cards and workflow_id not in {"operator_manual", "model_radar", "system_architecture", "business_market", "security_compliance", "harvest_followups"}:
                continue
            workflow_boards.append(
                {
                    "id": workflow_id,
                    "label": label,
                    "purpose": purpose,
                    "count": len(workflow_cards),
                    "lanes": [
                        {
                            "id": lane,
                            "label": lane.replace("_", " ").title(),
                            "count": len([card for card in workflow_cards if card["lane"] == lane]),
                            "cards": [card for card in workflow_cards if card["lane"] == lane],
                        }
                        for lane in TASK_LANES
                    ],
                }
            )
        source_counts = Counter(card["source"] for card in cards)
        return {
            "lanes": lanes,
            "workflow_boards": workflow_boards,
            "cards": cards,
            "source_counts": dict(source_counts),
        }

    def decisions(self) -> dict[str, Any]:
        operator = self._db.get_connection("operator_digest")
        financial = self._db.get_connection("financial_ledger")
        project_names = {
            row["project_id"]: row["name"]
            for row in financial.execute("SELECT project_id, name FROM projects").fetchall()
        }
        pending_gates = operator.execute(
            """
            SELECT gate_id, gate_type, trigger_description, project_id, status, timeout_hours, created_at, expires_at
            FROM gate_log
            WHERE status = 'PENDING'
            ORDER BY expires_at ASC, created_at ASC
            LIMIT 12
            """
        ).fetchall()
        return {
            "pending_gates": [
                {**dict(row), "project_name": project_names.get(row["project_id"])}
                for row in pending_gates
            ],
            "pending_g3_requests": self._operator.list_g3_approval_requests(limit=12, status="PENDING"),
            "pending_quarantines": self._operator.list_quarantined_responses(limit=12, pending_review_only=True),
            "runtime_halts": self._operator.list_runtime_halt_events(limit=6, status="ACTIVE"),
        }

    def set_project_priority(self, project_id: str, priority: str, focus_note: str = "") -> dict[str, Any]:
        if priority not in PRIORITIES:
            raise ValueError(f"Unknown project priority: {priority}")
        financial = self._db.get_connection("financial_ledger")
        project = financial.execute(
            "SELECT project_id, name FROM projects WHERE project_id = ?",
            (project_id,),
        ).fetchone()
        if project is None:
            raise KeyError(project_id)
        now = _utc_now()
        operator = self._db.get_connection("operator_digest")
        operator.execute(
            """
            INSERT INTO operator_project_preferences (project_id, priority, focus_note, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(project_id) DO UPDATE SET
                priority = excluded.priority,
                focus_note = excluded.focus_note,
                updated_at = excluded.updated_at
            """,
            (project_id, priority, focus_note, now),
        )
        self._record_heartbeat(operator, interaction_type="message", when=now, channel=self._interaction_channel)
        operator.commit()
        return {
            "project_id": project_id,
            "name": project["name"],
            "priority": priority,
            "focus_note": focus_note,
            "updated_at": now,
        }

    def create_manual_task(
        self,
        *,
        title: str,
        details: str = "",
        priority: str = "P2_NORMAL",
        status: str = "TODO",
        project_id: str | None = None,
    ) -> dict[str, Any]:
        if priority not in PRIORITIES:
            raise ValueError(f"Unknown manual task priority: {priority}")
        if status not in MANUAL_TASK_STATUSES:
            raise ValueError(f"Unknown manual task status: {status}")
        now = _utc_now()
        task_id = str(uuid.uuid4())
        operator = self._db.get_connection("operator_digest")
        operator.execute(
            """
            INSERT INTO operator_manual_tasks (
                task_id, project_id, title, details, status, priority, created_at, updated_at, completed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                task_id,
                project_id,
                title.strip(),
                details.strip(),
                status,
                priority,
                now,
                now,
                now if status == "DONE" else None,
            ),
        )
        self._record_heartbeat(operator, interaction_type="message", when=now, channel=self._interaction_channel)
        operator.commit()
        return {
            "task_id": task_id,
            "project_id": project_id,
            "title": title.strip(),
            "details": details.strip(),
            "status": status,
            "priority": priority,
            "created_at": now,
            "updated_at": now,
        }

    def create_research_task(
        self,
        *,
        title: str,
        brief: str,
        workflow_id: str = "operator_prompts",
        domain: int | None = None,
        priority: str = "P2_NORMAL",
        source: str = "operator",
        depth: str = "QUICK",
        stale_after: str | None = None,
    ) -> dict[str, Any]:
        if priority not in PRIORITIES:
            raise ValueError(f"Unknown research task priority: {priority}")
        if source not in {"autonomous_loop", "operator", "council"}:
            raise ValueError(f"Unknown research task source: {source}")
        resolved_domain = _research_domain_for_workflow(workflow_id) if domain is None else int(domain)
        if not 1 <= resolved_domain <= 5:
            raise ValueError(f"Unknown research domain: {domain}")
        if depth not in {"QUICK", "FULL"}:
            raise ValueError(f"Unknown research depth: {depth}")
        title = title.strip()
        brief = brief.strip()
        if not title:
            raise ValueError("Research task title is required")
        if not brief:
            brief = title
        from skills.research_domain.skill import ResearchDomainSkill

        tags = list(dict.fromkeys([workflow_id, *_research_tags_for_workflow(workflow_id), depth.lower()]))
        task_id = ResearchDomainSkill(self._db).create_task(
            title,
            brief,
            priority=priority,
            domain=resolved_domain,
            source=source,
            tags=tags,
            stale_after=stale_after or None,
        )
        strategic = self._db.get_connection("strategic_memory")
        if depth == "FULL":
            strategic.execute("UPDATE research_tasks SET depth_upgrade = 1 WHERE task_id = ?", (task_id,))
            strategic.commit()
        task = strategic.execute(
            """
            SELECT task_id, domain, source, title, brief, priority, status, tags,
                   depth_upgrade, created_at, updated_at
            FROM research_tasks
            WHERE task_id = ?
            """,
            (task_id,),
        ).fetchone()
        now = _utc_now()
        operator = self._db.get_connection("operator_digest")
        self._record_heartbeat(operator, interaction_type="message", when=now, channel=self._interaction_channel)
        operator.commit()
        assert task is not None
        return dict(task)

    def update_manual_task(
        self,
        task_id: str,
        *,
        status: str | None = None,
        priority: str | None = None,
        title: str | None = None,
        details: str | None = None,
    ) -> dict[str, Any]:
        if status is not None and status not in MANUAL_TASK_STATUSES:
            raise ValueError(f"Unknown manual task status: {status}")
        if priority is not None and priority not in PRIORITIES:
            raise ValueError(f"Unknown manual task priority: {priority}")
        operator = self._db.get_connection("operator_digest")
        row = operator.execute(
            "SELECT * FROM operator_manual_tasks WHERE task_id = ?",
            (task_id,),
        ).fetchone()
        if row is None:
            raise KeyError(task_id)
        now = _utc_now()
        next_status = status or row["status"]
        operator.execute(
            """
            UPDATE operator_manual_tasks
            SET title = ?, details = ?, status = ?, priority = ?, updated_at = ?, completed_at = ?
            WHERE task_id = ?
            """,
            (
                (title.strip() if title is not None else row["title"]),
                (details.strip() if details is not None else row["details"]),
                next_status,
                priority or row["priority"],
                now,
                now if next_status == "DONE" else None,
                task_id,
            ),
        )
        self._record_heartbeat(operator, interaction_type="message", when=now, channel=self._interaction_channel)
        operator.commit()
        updated = operator.execute(
            "SELECT * FROM operator_manual_tasks WHERE task_id = ?",
            (task_id,),
        ).fetchone()
        assert updated is not None
        return dict(updated)

    def update_system_task_priority(self, kind: str, task_id: str, priority: str) -> dict[str, Any]:
        if priority not in PRIORITIES:
            raise ValueError(f"Unknown task priority: {priority}")
        now = _utc_now()
        if kind == "research":
            strategic = self._db.get_connection("strategic_memory")
            row = strategic.execute(
                "SELECT task_id, title FROM research_tasks WHERE task_id = ?",
                (task_id,),
            ).fetchone()
            if row is None:
                raise KeyError(task_id)
            strategic.execute(
                "UPDATE research_tasks SET priority = ?, updated_at = ? WHERE task_id = ?",
                (priority, now, task_id),
            )
            strategic.commit()
            operator = self._db.get_connection("operator_digest")
            self._record_heartbeat(operator, interaction_type="message", when=now, channel=self._interaction_channel)
            operator.commit()
            return {"kind": kind, "task_id": task_id, "title": row["title"], "priority": priority}
        if kind == "harvest":
            operator = self._db.get_connection("operator_digest")
            row = operator.execute(
                "SELECT harvest_id, target_interface FROM harvest_requests WHERE harvest_id = ?",
                (task_id,),
            ).fetchone()
            if row is None:
                raise KeyError(task_id)
            operator.execute(
                "UPDATE harvest_requests SET priority = ? WHERE harvest_id = ?",
                (priority, task_id),
            )
            self._record_heartbeat(operator, interaction_type="message", when=now, channel=self._interaction_channel)
            operator.commit()
            return {"kind": kind, "task_id": task_id, "title": row["target_interface"], "priority": priority}
        raise ValueError(f"Unsupported task kind: {kind}")

    def acknowledge_alert(self, alert_id: str) -> dict[str, Any]:
        return self._operator.acknowledge_alert(alert_id)

    def review_g3(self, request_id: str, decision: str, operator_notes: str | None = None) -> dict[str, Any]:
        return self._operator.review_g3_approval_request(
            request_id,
            decision,
            operator_notes=operator_notes,
        )

    def review_quarantine(self, quarantine_id: str, decision: str, review_notes: str | None = None) -> dict[str, Any]:
        return self._operator.review_quarantined_response(
            quarantine_id,
            decision,
            review_notes=review_notes,
        )

    def _latest_digest(self) -> dict[str, Any] | None:
        recent = self._observability.recent_digests(limit=1)
        return recent[0] if recent else None

    @staticmethod
    def _project_lane(project_status: str, phase_name: str | None, phase_status: str | None) -> str:
        if project_status == "PIPELINE":
            return "PIPELINE"
        if project_status == "PAUSED":
            return "PAUSED"
        if project_status == "KILL_RECOMMENDED":
            return "KILL_REVIEW"
        if project_status in {"COMPLETE", "KILLED"}:
            return "DONE"
        if phase_name in {"VALIDATE", "BUILD", "DEPLOY", "OPERATE"}:
            return phase_name
        if phase_status == "GATE_PENDING":
            return "KILL_REVIEW"
        return "PIPELINE"

    @staticmethod
    def _derived_project_priority(row: Any) -> str:
        if row["status"] == "KILL_RECOMMENDED" or int(row["pending_gate_count"] or 0) > 0:
            return "P0_IMMEDIATE"
        if row["status"] == "PAUSED" or bool(row["kill_score_watch"]):
            return "P1_HIGH"
        if row["status"] == "ACTIVE":
            return "P2_NORMAL"
        return "P3_BACKGROUND"

    @staticmethod
    def _record_heartbeat(operator_conn: Any, *, interaction_type: str, when: str, channel: str) -> None:
        operator_conn.execute(
            "INSERT INTO operator_heartbeat (entry_id, interaction_type, channel, timestamp) VALUES (?, ?, ?, ?)",
            (str(uuid.uuid4()), interaction_type, channel, when),
        )


MISSION_CONTROL_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Mission Control</title>
  <style>
    :root {
      --bg: #f4efe4;
      --panel: rgba(255,255,255,0.82);
      --ink: #16211b;
      --muted: #5f665f;
      --line: rgba(22,33,27,0.12);
      --accent: #1e7a57;
      --accent-soft: #d9f0e5;
      --warn: #b45f06;
      --danger: #a6372b;
      --shadow: 0 14px 30px rgba(22,33,27,0.09);
      --radius: 18px;
      --radius-sm: 12px;
      --font-sans: "Avenir Next", "Segoe UI", "Helvetica Neue", sans-serif;
      --font-mono: "SF Mono", "Menlo", "Monaco", monospace;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: var(--font-sans);
      color: var(--ink);
      background:
        radial-gradient(circle at top left, rgba(30,122,87,0.18), transparent 28%),
        radial-gradient(circle at top right, rgba(180,95,6,0.14), transparent 20%),
        linear-gradient(180deg, #fbf7ef 0%, #f1ead9 100%);
    }
    .shell { max-width: 1520px; margin: 0 auto; padding: 28px 20px 40px; }
    .masthead {
      display: grid;
      grid-template-columns: 1.7fr 1fr;
      gap: 16px;
      margin-bottom: 18px;
    }
    .panel {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: var(--radius);
      box-shadow: var(--shadow);
      backdrop-filter: blur(10px);
    }
    .hero {
      padding: 22px;
      background:
        linear-gradient(140deg, rgba(30,122,87,0.95), rgba(18,53,41,0.93)),
        linear-gradient(140deg, #1e7a57, #123529);
      color: #f8fff9;
    }
    h1, h2, h3, p { margin: 0; }
    .hero h1 { font-size: 2rem; letter-spacing: -0.03em; }
    .hero p { margin-top: 10px; max-width: 64ch; color: rgba(248,255,249,0.85); }
    .hero .meta { margin-top: 14px; font-family: var(--font-mono); font-size: 0.85rem; color: rgba(248,255,249,0.74); }
    .pulse {
      padding: 18px;
      display: grid;
      gap: 12px;
      align-content: start;
    }
    .pulse-grid, .kpis {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
      gap: 12px;
    }
    .stat, .metric {
      padding: 14px;
      border-radius: var(--radius-sm);
      background: rgba(255,255,255,0.72);
      border: 1px solid var(--line);
    }
    .stat .label, .metric .label { color: var(--muted); font-size: 0.8rem; text-transform: uppercase; letter-spacing: 0.08em; }
    .stat .value, .metric .value { margin-top: 4px; font-size: 1.35rem; font-weight: 700; }
    .main-grid {
      display: grid;
      grid-template-columns: 2fr 1fr;
      gap: 16px;
      align-items: start;
    }
    .stack { display: grid; gap: 16px; }
    .section { padding: 18px; }
    .section-head {
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      gap: 12px;
      margin-bottom: 14px;
    }
    .section-head h2 { font-size: 1.1rem; letter-spacing: -0.02em; }
    .section-head span { color: var(--muted); font-size: 0.9rem; }
    .workflow {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 12px;
    }
    .workflow-card {
      padding: 14px;
      border: 1px solid var(--line);
      border-radius: var(--radius-sm);
      background: rgba(255,255,255,0.7);
    }
    .workflow-card .count {
      font-size: 1.8rem;
      font-weight: 700;
      margin: 8px 0 10px;
    }
    .detail-list, .mini-list {
      display: grid;
      gap: 8px;
      margin-top: 8px;
    }
    .detail-row, .mini-row {
      display: flex;
      justify-content: space-between;
      gap: 12px;
      font-size: 0.88rem;
      color: var(--muted);
    }
    .board {
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 12px;
    }
    .board-compact {
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }
    .lane {
      min-height: 180px;
      padding: 12px;
      border-radius: var(--radius-sm);
      background: rgba(255,255,255,0.62);
      border: 1px solid var(--line);
    }
    .lane h3 {
      font-size: 0.95rem;
      display: flex;
      justify-content: space-between;
      margin-bottom: 10px;
    }
    .lane-count {
      color: var(--muted);
      font-family: var(--font-mono);
      font-size: 0.8rem;
    }
    .cards { display: grid; gap: 10px; }
    .card {
      padding: 12px;
      border-radius: 14px;
      background: #fff;
      border: 1px solid rgba(22,33,27,0.08);
      box-shadow: 0 8px 18px rgba(22,33,27,0.06);
    }
    .card h4 { font-size: 0.96rem; margin-bottom: 6px; }
    .eyebrow {
      display: inline-flex;
      align-items: center;
      padding: 4px 8px;
      border-radius: 999px;
      background: var(--accent-soft);
      color: #114c36;
      font-size: 0.74rem;
      text-transform: uppercase;
      letter-spacing: 0.06em;
      margin-bottom: 8px;
    }
    .meta-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 6px 10px;
      margin-top: 10px;
      color: var(--muted);
      font-size: 0.83rem;
    }
    .controls {
      display: flex;
      flex-wrap: wrap;
      gap: 8px;
      margin-top: 10px;
      align-items: center;
    }
    select, input, textarea, button {
      font: inherit;
      border-radius: 10px;
      border: 1px solid rgba(22,33,27,0.18);
      padding: 8px 10px;
      background: #fff;
      color: var(--ink);
    }
    textarea { width: 100%; min-height: 72px; resize: vertical; }
    button {
      background: var(--ink);
      color: #fff;
      border: none;
      cursor: pointer;
    }
    button.secondary {
      background: rgba(22,33,27,0.08);
      color: var(--ink);
      border: 1px solid rgba(22,33,27,0.12);
    }
    .feed { display: grid; gap: 10px; }
    .feed-item {
      padding: 12px;
      border: 1px solid var(--line);
      border-radius: var(--radius-sm);
      background: rgba(255,255,255,0.74);
    }
    .feed-item p { margin-top: 6px; color: var(--muted); line-height: 1.45; }
    .split {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 12px;
    }
    .form-row {
      display: grid;
      gap: 10px;
      margin-top: 10px;
    }
    .small {
      color: var(--muted);
      font-size: 0.84rem;
      line-height: 1.45;
    }
    .danger { color: var(--danger); }
    .warning { color: var(--warn); }
    .empty {
      padding: 14px;
      border: 1px dashed rgba(22,33,27,0.18);
      border-radius: 12px;
      color: var(--muted);
      font-size: 0.9rem;
      background: rgba(255,255,255,0.55);
    }
    @media (max-width: 1120px) {
      .masthead, .main-grid, .split { grid-template-columns: 1fr; }
      .board { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .board-compact { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
    @media (max-width: 760px) {
      .board, .board-compact, .workflow { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <div class="shell">
    <div class="masthead">
      <section class="panel hero">
        <h1>Mission Control</h1>
        <p>Lean operator view over the autonomous workspace. One page for flow, board state, decisions, and task pressure without a heavy frontend stack.</p>
        <div class="meta" id="stamp">Loading snapshot...</div>
      </section>
      <aside class="panel pulse">
        <div class="section-head">
          <h2>Live Pulse</h2>
          <span id="pulse-runtime">Waiting</span>
        </div>
        <div class="pulse-grid" id="kpis"></div>
      </aside>
    </div>

    <div class="main-grid">
      <div class="stack">
        <section class="panel section">
          <div class="section-head">
            <h2>Workflow</h2>
            <span>How work moves through the system</span>
          </div>
          <div class="workflow" id="workflow"></div>
        </section>

        <section class="panel section">
          <div class="section-head">
            <h2>Project Board</h2>
            <span>Kanban mapped to real project and phase states</span>
          </div>
          <div class="board" id="project-board"></div>
        </section>

        <section class="panel section">
          <div class="section-head">
            <h2>Task Pressure</h2>
            <span>Manual tasks plus research and harvest queues</span>
          </div>
          <div class="split">
            <div>
              <div class="board board-compact" id="task-board"></div>
            </div>
            <div>
              <h3>Add Manual Task</h3>
              <div class="form-row">
                <input id="task-title" type="text" placeholder="Title">
                <textarea id="task-details" placeholder="What needs doing?"></textarea>
                <div class="split">
                  <select id="task-priority">
                    <option value="P0_IMMEDIATE">P0 Immediate</option>
                    <option value="P1_HIGH">P1 High</option>
                    <option value="P2_NORMAL" selected>P2 Normal</option>
                    <option value="P3_BACKGROUND">P3 Background</option>
                  </select>
                  <select id="task-status">
                    <option value="TODO" selected>Todo</option>
                    <option value="IN_PROGRESS">In Progress</option>
                    <option value="BLOCKED">Blocked</option>
                    <option value="DONE">Done</option>
                  </select>
                </div>
                <button id="create-task">Create Task</button>
                <div class="small">Manual tasks stay intentionally lightweight. System tasks keep their native state, but you can still reprioritize research and harvest work here.</div>
              </div>
            </div>
          </div>
        </section>
      </div>

      <div class="stack">
        <section class="panel section">
          <div class="section-head">
            <h2>Decisions</h2>
            <span>Queues that need operator attention</span>
          </div>
          <div class="feed" id="decisions"></div>
        </section>

        <section class="panel section">
          <div class="section-head">
            <h2>Alerts</h2>
            <span>Recent operator signals</span>
          </div>
          <div class="feed" id="alerts"></div>
        </section>

        <section class="panel section">
          <div class="section-head">
            <h2>Digest</h2>
            <span>Latest generated snapshot</span>
          </div>
          <div id="digest" class="feed"></div>
        </section>
      </div>
    </div>
  </div>

  <script>
    const PRIORITIES = ["P0_IMMEDIATE", "P1_HIGH", "P2_NORMAL", "P3_BACKGROUND"];
    const priorityOptions = PRIORITIES.map(value => `<option value="${value}">${value.replace("_", " ")}</option>`).join("");

    async function postJSON(url, payload) {
      const response = await fetch(url, {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify(payload || {})
      });
      if (!response.ok) {
        const text = await response.text();
        throw new Error(text || `Request failed: ${response.status}`);
      }
      return response.json();
    }

    function money(value) {
      return typeof value === "number" ? `$${value.toFixed(2)}` : "n/a";
    }

    function percent(value) {
      return typeof value === "number" ? `${Math.round(value * 100)}%` : "n/a";
    }

    function optionMarkup(selected) {
      return PRIORITIES.map(value => `<option value="${value}" ${value === selected ? "selected" : ""}>${value.replace("_", " ")}</option>`).join("");
    }

    function humanizeStatus(value) {
      return (value || "").replaceAll("_", " ").replaceAll(":", ": ");
    }

    function replayLabel(status) {
      if (status === "READY_FOR_BROADER_REPLAY") return "Ready";
      if (status === "IMPLEMENTED_BELOW_ACTIVATION_THRESHOLD") return "Below Threshold";
      return humanizeStatus(status);
    }

    function renderKPIs(data) {
      const o = data.overview;
      const cards = [
        ["Runtime", o.runtime_status.lifecycle_state],
        ["Heartbeat", o.heartbeat_state],
        ["Replay", replayLabel(data.overview.replay_readiness.status)],
        ["Pending Gates", String(o.pending_gates)],
        ["Pending Harvests", String(o.pending_harvests)],
        ["Load Hours", o.operator_load_hours.toFixed(1)]
      ];
      document.getElementById("pulse-runtime").textContent = `${o.runtime_status.lifecycle_state} / ${o.recommended_digest_type}`;
      document.getElementById("kpis").innerHTML = cards.map(([label, value]) => `
        <div class="metric">
          <div class="label">${label}</div>
          <div class="value">${value}</div>
        </div>
      `).join("");
    }

    function renderWorkflow(data) {
      document.getElementById("workflow").innerHTML = data.workflow.steps.map(step => `
        <article class="workflow-card">
          <div class="label">${step.label}</div>
          <div class="count">${step.count}</div>
          <div class="detail-list">
            ${Object.entries(step.detail).slice(0, 6).map(([key, value]) => `
              <div class="detail-row"><span>${humanizeStatus(key)}</span><strong>${value}</strong></div>
            `).join("")}
          </div>
        </article>
      `).join("");
    }

    function renderProjectBoard(data) {
      document.getElementById("project-board").innerHTML = data.project_board.lanes.map(lane => `
        <section class="lane">
          <h3>${lane.label}<span class="lane-count">${lane.count}</span></h3>
          <div class="cards">
            ${lane.cards.length ? lane.cards.map(card => `
              <article class="card">
                <div class="eyebrow">${card.priority.replaceAll("_", " ")}</div>
                <h4>${card.name}</h4>
                <div class="small">${card.income_mechanism.replaceAll("_", " ")}${card.phase_name ? ` / ${card.phase_name}` : ""}</div>
                <div class="meta-grid">
                  <span>Weight</span><strong>${Math.round(card.portfolio_weight * 100)}%</strong>
                  <span>Cashflow</span><strong>${money(card.cashflow_actual_usd)}</strong>
                  <span>Kill Score</span><strong>${card.kill_score == null ? "n/a" : card.kill_score.toFixed(2)}</strong>
                  <span>Executor Burn</span><strong>${percent(card.executor_burn_ratio)}</strong>
                </div>
                <div class="controls">
                  <select data-project-priority="${card.project_id}">
                    ${optionMarkup(card.priority)}
                  </select>
                </div>
              </article>
            `).join("") : `<div class="empty">No projects in this lane.</div>`}
          </div>
        </section>
      `).join("");
      document.querySelectorAll("[data-project-priority]").forEach(select => {
        select.addEventListener("change", async (event) => {
          await postJSON(`/api/projects/${event.target.dataset.projectPriority}/priority`, {priority: event.target.value});
          await refresh();
        });
      });
    }

    function taskControls(card) {
      if (card.kind === "manual") {
        return `
          <div class="controls">
            <select data-task-priority="${card.kind}:${card.id}">${optionMarkup(card.priority)}</select>
            <select data-task-status="${card.id}">
              ${["TODO","IN_PROGRESS","BLOCKED","DONE"].map(value => `<option value="${value}" ${value === card.status ? "selected" : ""}>${value.replaceAll("_", " ")}</option>`).join("")}
            </select>
          </div>
        `;
      }
      return `
        <div class="controls">
          <select data-task-priority="${card.kind}:${card.id}">${optionMarkup(card.priority)}</select>
        </div>
      `;
    }

    function renderTaskBoard(data) {
      document.getElementById("task-board").innerHTML = data.tasks.lanes.map(lane => `
        <section class="lane">
          <h3>${lane.label}<span class="lane-count">${lane.count}</span></h3>
          <div class="cards">
            ${lane.cards.length ? lane.cards.map(card => `
              <article class="card">
                <div class="eyebrow">${card.source}</div>
                <h4>${card.title}</h4>
                <div class="small">${card.project_name || card.status}</div>
                <p class="small">${(card.details || "").slice(0, 180)}</p>
                ${taskControls(card)}
              </article>
            `).join("") : `<div class="empty">No tasks in this lane.</div>`}
          </div>
        </section>
      `).join("");
      document.querySelectorAll("[data-task-priority]").forEach(select => {
        select.addEventListener("change", async (event) => {
          const [kind, id] = event.target.dataset.taskPriority.split(":");
          await postJSON("/api/tasks/priority", {kind, id, priority: event.target.value});
          await refresh();
        });
      });
      document.querySelectorAll("[data-task-status]").forEach(select => {
        select.addEventListener("change", async (event) => {
          await postJSON(`/api/manual-tasks/${event.target.dataset.taskStatus}`, {status: event.target.value});
          await refresh();
        });
      });
    }

    function renderDecisions(data) {
      const items = [];
      data.decisions.pending_gates.forEach(item => {
        items.push(`
          <div class="feed-item">
            <strong>${item.gate_type}</strong> · ${item.trigger_description}
            <p>Expires ${item.expires_at || "n/a"} · ${item.project_name || item.project_id || "n/a"}</p>
          </div>
        `);
      });
      data.decisions.pending_g3_requests.forEach(item => {
        items.push(`
          <div class="feed-item">
            <strong>G3 Approval</strong> · ${item.request_id}
            <p>${item.reason || item.model_id || "Pending spend review"}</p>
            <div class="controls">
              <button data-g3="${item.request_id}" data-decision="APPROVE">Approve</button>
              <button class="secondary" data-g3="${item.request_id}" data-decision="DENY">Deny</button>
            </div>
          </div>
        `);
      });
      data.decisions.pending_quarantines.forEach(item => {
        items.push(`
          <div class="feed-item">
            <strong>Quarantine</strong> · ${item.quarantine_id}
            <p>${item.reason || item.review_status || "Pending review"}</p>
            <div class="controls">
              <button data-quarantine="${item.quarantine_id}" data-decision="REPROCESS">Reprocess</button>
              <button class="secondary" data-quarantine="${item.quarantine_id}" data-decision="DISCARD">Discard</button>
            </div>
          </div>
        `);
      });
      data.decisions.runtime_halts.forEach(item => {
        items.push(`
          <div class="feed-item">
            <strong>Runtime Halt</strong> · ${item.source}
            <p>${item.halt_reason}</p>
          </div>
        `);
      });
      document.getElementById("decisions").innerHTML = items.length ? items.join("") : `<div class="empty">No pending operator decisions.</div>`;
      document.querySelectorAll("[data-g3]").forEach(button => {
        button.addEventListener("click", async (event) => {
          await postJSON(`/api/g3/${event.target.dataset.g3}/review`, {decision: event.target.dataset.decision});
          await refresh();
        });
      });
      document.querySelectorAll("[data-quarantine]").forEach(button => {
        button.addEventListener("click", async (event) => {
          await postJSON(`/api/quarantines/${event.target.dataset.quarantine}/review`, {decision: event.target.dataset.decision});
          await refresh();
        });
      });
    }

    function renderAlerts(data) {
      const alerts = data.alerts || [];
      document.getElementById("alerts").innerHTML = alerts.length ? alerts.map(item => `
        <div class="feed-item">
          <strong>${item.tier}</strong> · ${item.alert_type}
          <p>${item.content}</p>
          ${item.acknowledged ? `<div class="small">Acknowledged</div>` : `<div class="controls"><button data-alert-ack="${item.alert_id}">Acknowledge</button></div>`}
        </div>
      `).join("") : `<div class="empty">No recent alerts.</div>`;
      document.querySelectorAll("[data-alert-ack]").forEach(button => {
        button.addEventListener("click", async (event) => {
          await postJSON(`/api/alerts/${event.target.dataset.alertAck}/ack`, {});
          await refresh();
        });
      });
    }

    function renderDigest(data) {
      const digest = data.latest_digest;
      document.getElementById("digest").innerHTML = digest ? `
        <div class="feed-item">
          <strong>${digest.digest_type}</strong> · ${digest.operator_state}
          <p style="white-space: pre-wrap;">${digest.content}</p>
        </div>
      ` : `<div class="empty">No digest generated yet.</div>`;
    }

    async function refresh() {
      const response = await fetch("/api/snapshot");
      const data = await response.json();
      document.getElementById("stamp").textContent = `Snapshot ${data.generated_at}`;
      renderKPIs(data);
      renderWorkflow(data);
      renderProjectBoard(data);
      renderTaskBoard(data);
      renderDecisions(data);
      renderAlerts(data);
      renderDigest(data);
    }

    document.getElementById("create-task").addEventListener("click", async () => {
      const title = document.getElementById("task-title").value.trim();
      if (!title) return;
      await postJSON("/api/manual-tasks", {
        title,
        details: document.getElementById("task-details").value,
        priority: document.getElementById("task-priority").value,
        status: document.getElementById("task-status").value
      });
      document.getElementById("task-title").value = "";
      document.getElementById("task-details").value = "";
      document.getElementById("task-priority").value = "P2_NORMAL";
      document.getElementById("task-status").value = "TODO";
      await refresh();
    });

    refresh();
    setInterval(refresh, 30000);
  </script>
</body>
</html>
"""


class MissionControlHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, server_address: tuple[str, int], service: MissionControlService):
        self.service = service
        super().__init__(server_address, MissionControlHandler)


class MissionControlHandler(BaseHTTPRequestHandler):
    server: MissionControlHTTPServer

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path == "/":
            body = MISSION_CONTROL_HTML.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if parsed.path == "/health":
            self._json_response({"ok": True, "now": _utc_now()})
            return
        if parsed.path == "/api/snapshot":
            self._json_response(self.server.service.snapshot())
            return
        self._json_response({"error": "not found"}, status=404)

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        payload = self._read_json()
        try:
            if parsed.path.startswith("/api/projects/") and parsed.path.endswith("/priority"):
                project_id = parsed.path.split("/")[3]
                result = self.server.service.set_project_priority(
                    project_id,
                    payload["priority"],
                    focus_note=payload.get("focus_note", ""),
                )
                self._json_response(result)
                return
            if parsed.path == "/api/manual-tasks":
                result = self.server.service.create_manual_task(
                    title=payload["title"],
                    details=payload.get("details", ""),
                    priority=payload.get("priority", "P2_NORMAL"),
                    status=payload.get("status", "TODO"),
                    project_id=payload.get("project_id"),
                )
                self._json_response(result, status=201)
                return
            if parsed.path == "/api/research-tasks":
                result = self.server.service.create_research_task(
                    title=payload["title"],
                    brief=payload.get("brief", ""),
                    workflow_id=payload.get("workflow_id", "operator_prompts"),
                    domain=int(payload["domain"]) if payload.get("domain") not in (None, "") else None,
                    priority=payload.get("priority", "P2_NORMAL"),
                    source=payload.get("source", "operator"),
                    depth=payload.get("depth", "QUICK"),
                    stale_after=payload.get("stale_after"),
                )
                self._json_response(result, status=201)
                return
            if parsed.path.startswith("/api/manual-tasks/"):
                task_id = parsed.path.split("/")[3]
                result = self.server.service.update_manual_task(
                    task_id,
                    status=payload.get("status"),
                    priority=payload.get("priority"),
                    title=payload.get("title"),
                    details=payload.get("details"),
                )
                self._json_response(result)
                return
            if parsed.path == "/api/tasks/priority":
                result = self.server.service.update_system_task_priority(
                    payload["kind"],
                    payload["id"],
                    payload["priority"],
                )
                self._json_response(result)
                return
            if parsed.path.startswith("/api/alerts/") and parsed.path.endswith("/ack"):
                alert_id = parsed.path.split("/")[3]
                result = self.server.service.acknowledge_alert(alert_id)
                self._json_response(result)
                return
            if parsed.path.startswith("/api/g3/") and parsed.path.endswith("/review"):
                request_id = parsed.path.split("/")[3]
                result = self.server.service.review_g3(
                    request_id,
                    payload["decision"],
                    operator_notes=payload.get("operator_notes"),
                )
                self._json_response(result)
                return
            if parsed.path.startswith("/api/quarantines/") and parsed.path.endswith("/review"):
                quarantine_id = parsed.path.split("/")[3]
                result = self.server.service.review_quarantine(
                    quarantine_id,
                    payload["decision"],
                    review_notes=payload.get("review_notes"),
                )
                self._json_response(result)
                return
            self._json_response({"error": "not found"}, status=404)
        except KeyError as exc:
            self._json_response({"error": f"missing or unknown identifier: {exc}"}, status=404)
        except ValueError as exc:
            self._json_response({"error": str(exc)}, status=400)

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        return json.loads(raw.decode("utf-8"))

    def _json_response(self, payload: dict[str, Any], *, status: int = 200) -> None:
        body = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def run_mission_control_server(data_dir: str, *, host: str = "127.0.0.1", port: int = 8765) -> None:
    db = DatabaseManager(data_dir)
    server = MissionControlHTTPServer((host, port), MissionControlService(db))
    try:
        print(f"mission_control_url=http://{host}:{port}")
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        db.close_all()


def seed_demo_state(data_dir: str) -> dict[str, Any]:
    db = DatabaseManager(data_dir)
    now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
    strategic = db.get_connection("strategic_memory")
    financial = db.get_connection("financial_ledger")
    operator = db.get_connection("operator_digest")

    existing = {
        "opportunities": int(strategic.execute("SELECT COUNT(*) FROM opportunity_records").fetchone()[0]),
        "projects": int(financial.execute("SELECT COUNT(*) FROM projects").fetchone()[0]),
        "manual_tasks": int(operator.execute("SELECT COUNT(*) FROM operator_manual_tasks").fetchone()[0]),
    }
    if any(existing.values()):
        db.close_all()
        return {"seeded": False, "reason": "existing_data_present", **existing}

    opportunities = [
        ("opp-demo-1", "software_product", "Mission Control UI", "Build a lean operator control surface", "GO_NO_GO"),
        ("opp-demo-2", "client_work", "Operator Setup Sprint", "Package day-one deployment support", "IN_VALIDATION"),
        ("opp-demo-3", "ip_asset", "Workflow Templates", "Extract reusable operating templates", "SCREENED"),
        ("opp-demo-4", "ip_asset", "Memory Compaction Pattern", "Turn research on lower-token operations into a reusable system improvement", "QUALIFIED"),
    ]
    for idx, (opp_id, mechanism, title, thesis, status) in enumerate(opportunities):
        ts = (now - datetime.timedelta(hours=12 - idx)).isoformat()
        strategic.execute(
            """
            INSERT INTO opportunity_records (
                opportunity_id, income_mechanism, title, thesis, detected_by, council_verdict_id,
                validation_spend, validation_report, cashflow_estimate, status, project_id,
                learning_record, provenance_links, provenance_degraded, trust_tier, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                opp_id,
                mechanism,
                title,
                thesis,
                "research_prompted" if opp_id == "opp-demo-4" else "operator",
                None,
                0.0,
                None,
                json.dumps({"monthly_usd": 500 + idx * 250}),
                status,
                None,
                None,
                "[]",
                0,
                2,
                ts,
                ts,
            ),
        )

    research_tasks = [
        ("research-demo-1", 2, "Research operator dashboards competitors", "operator", "P1_HIGH", "ACTIVE", ["mission_control", "dashboard", "architecture"]),
        ("research-demo-2", 3, "Map monetization options for agent operations support", "autonomous_loop", "P2_NORMAL", "PENDING", ["market", "monetization"]),
        ("research-demo-3", 1, "Review local-first notification pathways", "operator", "P1_HIGH", "STALE", ["security", "operator_interface"]),
        ("research-demo-4", 5, "Scout Hermes-compatible frontier model candidates", "autonomous_loop", "P1_HIGH", "ACTIVE", ["model", "frontier", "scout"]),
        ("research-demo-5", 2, "Assess MLX inference architecture for council roles", "council", "P2_NORMAL", "PENDING", ["architecture", "mlx", "efficiency"]),
        ("research-demo-6", 4, "Check AI agent compliance obligations for EU operators", "autonomous_loop", "P2_NORMAL", "PENDING", ["regulatory", "compliance"]),
        ("research-demo-7", 2, "Compare token-saving memory compaction strategies", "operator", "P1_HIGH", "PENDING", ["architecture", "token", "memory"]),
    ]
    for idx, (task_id, domain, title, source, priority, status, tags) in enumerate(research_tasks):
        ts = (now - datetime.timedelta(hours=8 - idx)).isoformat()
        strategic.execute(
            """
            INSERT INTO research_tasks (
                task_id, domain, source, title, brief, priority, status, max_spend_usd,
                actual_spend_usd, output_brief_id, follow_up_tasks, stale_after, tags,
                depth_upgrade, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                task_id,
                domain,
                source,
                title,
                f"{title}. Keep the output tight and operator-oriented.",
                priority,
                status,
                0.0,
                0.0,
                None,
                "[]",
                (now + datetime.timedelta(days=1)).isoformat(),
                json.dumps(tags),
                0,
                ts,
                ts,
            ),
        )

    standing_briefs = [
        ("sb-demo-frontier", 5, "Frontier model watch", "Track model releases, free-tier changes, and role candidates.", "0 8 */14 * *", "mission_control", ["model", "standing_brief"]),
        ("sb-demo-architecture", 2, "System architecture radar", "Watch architecture changes that could simplify or improve the workspace.", "0 9 1 * *", "mission_control", ["architecture", "standing_brief"]),
    ]
    for standing_id, domain, title, brief, cron_expr, target_interface, tags in standing_briefs:
        strategic.execute(
            """
            INSERT INTO standing_briefs (
                standing_brief_id, domain, title, brief, cron_expr, target_interface,
                include_council_review, status, tags, last_task_id, last_job_id,
                last_run_at, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                standing_id,
                domain,
                title,
                brief,
                cron_expr,
                target_interface,
                1 if "architecture" in tags else 0,
                "ACTIVE",
                json.dumps(tags),
                None,
                None,
                None,
                (now - datetime.timedelta(days=1)).isoformat(),
                (now - datetime.timedelta(hours=4)).isoformat(),
            ),
        )

    strategic.execute(
        """
        INSERT INTO model_scout_reports (
            report_id, candidate_model_id, target_role, model_card_summary, licence,
            quantisation_available, memory_footprint_gb, benchmark_scores,
            plausible_fit, disqualifiers, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "model-scout-demo-1",
            "demo-frontier-local-32b",
            "Primary Reasoning",
            "Promising local reasoning candidate for shadow evaluation.",
            "Apache-2.0",
            1,
            22.0,
            json.dumps({"reasoning": 0.78, "tool_use": 0.72}),
            1,
            "[]",
            (now - datetime.timedelta(hours=5)).isoformat(),
        ),
    )

    intelligence_briefs = [
        (
            "brief-demo-model",
            "research-demo-4",
            5,
            "Frontier model candidate may deserve shadow eval",
            "A Hermes-compatible local model candidate appears plausible for primary reasoning after a small benchmark delta.",
            0.82,
            "ACTION_RECOMMENDED",
            "ELEVATED",
            "council_review",
            None,
            ["model", "scout"],
        ),
        (
            "brief-demo-architecture",
            "research-demo-7",
            2,
            "Memory compaction may reduce routine token load",
            "A staged memory compaction pattern could reduce repeated context without changing safety gates.",
            0.78,
            "ACTION_REQUIRED",
            "ELEVATED",
            "opportunity_feed",
            "opp-demo-4",
            ["architecture", "token", "memory"],
        ),
    ]
    for brief_id, task_id, domain, title, summary, confidence, actionability, urgency, action_type, spawned_opportunity_id, tags in intelligence_briefs:
        strategic.execute(
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
                summary + " Demo detail for Mission Control preview.",
                json.dumps(["https://example.com/research"]),
                json.dumps([{"url": "https://example.com/research", "relevance": 0.8, "freshness": now.date().isoformat(), "source_type": "tier2_web"}]),
                confidence,
                "Demo uncertainty: live evidence and broader benchmark coverage are still thin.",
                "The apparent gain may disappear under production workload.",
                actionability,
                urgency,
                "FULL",
                action_type,
                "[]",
                spawned_opportunity_id,
                "[]",
                json.dumps(tags),
                0,
                0,
                "[]",
                3,
                (now - datetime.timedelta(hours=2)).isoformat(),
            ),
        )
        strategic.execute(
            "UPDATE research_tasks SET output_brief_id = ?, status = 'COMPLETE', updated_at = ? WHERE task_id = ?",
            (brief_id, (now - datetime.timedelta(hours=1)).isoformat(), task_id),
        )

    projects = [
        ("proj-demo-1", "opp-demo-1", "Mission Control", "software_product", "Ship a useful operator console", "ACTIVE", "BUILD", "ACTIVE", 0.40, 180.0),
        ("proj-demo-2", "opp-demo-2", "Day-One Handoff", "client_work", "Package deployment readiness", "PAUSED", "VALIDATE", "GATE_PENDING", 0.25, 0.0),
        ("proj-demo-3", "opp-demo-3", "Workflow Library", "ip_asset", "Codify reusable playbooks", "KILL_RECOMMENDED", "DEPLOY", "GATE_PENDING", 0.12, 40.0),
    ]
    for idx, (project_id, opp_id, name, mechanism, thesis, project_status, phase_name, phase_status, weight, cashflow) in enumerate(projects):
        created_at = (now - datetime.timedelta(days=3 - idx)).isoformat()
        financial.execute(
            """
            INSERT INTO projects (
                project_id, opportunity_id, name, income_mechanism, thesis, success_criteria,
                compute_budget, portfolio_weight, status, kill_score_watch, cashflow_actual_usd,
                council_verdict_id, pivot_log, created_at, closed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                project_id,
                opp_id,
                name,
                mechanism,
                thesis,
                json.dumps({"primary": "cashflow_target"}),
                json.dumps({"max_executor_hours": 40, "max_cloud_spend_usd": 0}),
                weight,
                project_status,
                1 if project_status in {"PAUSED", "KILL_RECOMMENDED"} else 0,
                cashflow,
                None,
                "[]",
                created_at,
                None,
            ),
        )
        financial.execute(
            """
            INSERT INTO phases (
                phase_id, project_id, name, status, sequence, scope, success_criteria,
                compute_budget, compute_consumed, outputs, gate_result, started_at, gate_triggered_at, completed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"phase-{project_id}",
                project_id,
                phase_name,
                phase_status,
                {"VALIDATE": 0, "BUILD": 1, "DEPLOY": 2, "OPERATE": 3}[phase_name],
                f"Primary objective for {name}",
                json.dumps(["clear operator value"]),
                json.dumps({"executor_hours_cap": 20, "cloud_spend_cap_usd": 0}),
                json.dumps({"executor_hours": 5 + idx * 4, "cloud_spend_usd": 0}),
                json.dumps(["snapshot", "board"] if idx == 0 else ["brief"]),
                None,
                created_at,
                (now - datetime.timedelta(hours=idx + 2)).isoformat() if phase_status == "GATE_PENDING" else None,
                None,
            ),
        )

    route_rows = [
        ("route-demo-research", "research-demo-4", None, "Embedding", "local", "bge-m3-local", "Research brief retrieval and clustering."),
        ("route-demo-council", None, None, "Validation", "subscription", "gpt-5.2", "Council validation for elevated findings."),
        ("route-demo-execution", None, "proj-demo-1", "Execution", "local", "qwen3-coder-30b-a3b", "Mission Control implementation work."),
        ("route-demo-primary", None, "proj-demo-1", "Primary Reasoning", "local", "demo-frontier-local-32b", "Planning and synthesis shadow candidate."),
        ("route-demo-training", None, None, "Training/Reward", "local", "judge-replay-local", "Harness trace review below activation threshold."),
    ]
    for idx, (decision_id, task_id, project_id, role, route_selected, model_used, justification) in enumerate(route_rows):
        financial.execute(
            """
            INSERT INTO routing_decisions (
                decision_id, task_id, chain_id, role, route_selected, model_used,
                commercial_use_ok, quality_warning, cost_usd, justification,
                g3_required, g3_status, reservation_id, created_at, project_id,
                session_id, correlation_id, cost_status, approval_request_id,
                dispatch_status, dispatched_at, finalized_at, final_cost_usd
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                decision_id,
                task_id,
                "mission-control-preview",
                role,
                route_selected,
                model_used,
                1,
                0,
                0.0,
                justification,
                0,
                None,
                None,
                (now - datetime.timedelta(minutes=45 - idx * 3)).isoformat(),
                project_id,
                "preview-session",
                f"preview-{idx}",
                "NOT_APPLICABLE",
                None,
                "FINALIZED",
                (now - datetime.timedelta(minutes=44 - idx * 3)).isoformat(),
                (now - datetime.timedelta(minutes=43 - idx * 3)).isoformat(),
                0.0,
            ),
        )

    financial.execute(
        """
        INSERT INTO kill_recommendations (
            recommendation_id, project_id, kill_score, council_verdict_id, asset_inventory,
            thesis_summary, failure_analysis, g2_status, threshold_status, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "kill-demo-1",
            "proj-demo-3",
            0.74,
            "verdict-demo-1",
            "[]",
            "Workflow library has not found a clear route to operator value.",
            "Scope drift and weak buyer clarity.",
            "PENDING",
            "PROVISIONAL",
            (now - datetime.timedelta(hours=2)).isoformat(),
        ),
    )

    gates = [
        ("gate-demo-1", "G1", "Approve BUILD continuation for Mission Control", "proj-demo-1", 24.0, now + datetime.timedelta(hours=22)),
        ("gate-demo-2", "G2", "Kill review for Workflow Library", "proj-demo-3", 48.0, now + datetime.timedelta(hours=30)),
    ]
    for gate_id, gate_type, trigger_description, project_id, timeout_hours, expires_at in gates:
        operator.execute(
            """
            INSERT INTO gate_log (
                gate_id, gate_type, trigger_description, context_packet, project_id, status,
                timeout_hours, operator_response, created_at, responded_at, expires_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                gate_id,
                gate_type,
                trigger_description,
                json.dumps({"summary": trigger_description}),
                project_id,
                "PENDING",
                timeout_hours,
                None,
                (now - datetime.timedelta(hours=1)).isoformat(),
                None,
                expires_at.isoformat(),
            ),
        )

    harvests = [
        ("harvest-demo-1", "research-demo-1", "Review three UI references and note what feels calm but high signal.", "mission_control", "P1_HIGH", "PENDING"),
        ("harvest-demo-2", "research-demo-3", "Confirm if any local notification channels are mature enough for v1.", "mission_control", "P2_NORMAL", "DELIVERED_PARTIAL"),
    ]
    for harvest_id, task_id, prompt_text, target_interface, priority, status in harvests:
        operator.execute(
            """
            INSERT INTO harvest_requests (
                harvest_id, task_id, prompt_text, target_interface, context_summary, priority,
                status, expires_at, operator_result, relevance_score, clarification_sent, created_at, delivered_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                harvest_id,
                task_id,
                prompt_text,
                target_interface,
                "Mission control review support",
                priority,
                status,
                (now + datetime.timedelta(days=1)).isoformat(),
                None,
                None,
                0,
                (now - datetime.timedelta(hours=3)).isoformat(),
                None,
            ),
        )

    alerts = [
        ("alert-demo-1", "T2", "COUNCIL_BACKLOG", "Tier 2 backlog depth reached 4; review pacing before expanding the portfolio.", 0),
        ("alert-demo-2", "T1", "REPLAY_READINESS", "Replay corpus remains below broader activation threshold.", 0),
    ]
    for alert_id, tier, alert_type, content, acknowledged in alerts:
        operator.execute(
            """
            INSERT INTO alert_log (
                alert_id, tier, alert_type, content, channel_delivered, suppressed, acknowledged, acknowledged_at, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                alert_id,
                tier,
                alert_type,
                content,
                "mission_control",
                0,
                acknowledged,
                None,
                (now - datetime.timedelta(minutes=45)).isoformat(),
            ),
        )

    strategic.commit()
    financial.commit()
    operator.commit()

    service = MissionControlService(db)
    service.set_project_priority("proj-demo-1", "P0_IMMEDIATE", "Tighten the board and workflow framing.")
    service.set_project_priority("proj-demo-2", "P1_HIGH", "Pause until the gate packet is clearer.")
    service.create_manual_task(
        title="Review board columns and labels",
        details="Decide whether PAUSED and KILL REVIEW deserve stronger emphasis.",
        priority="P1_HIGH",
        status="TODO",
        project_id="proj-demo-1",
    )
    service.create_manual_task(
        title="Choose top-of-screen KPIs",
        details="Trim overview to the 5-6 numbers that truly matter.",
        priority="P2_NORMAL",
        status="IN_PROGRESS",
        project_id="proj-demo-1",
    )

    digest_content = (
        "PENDING DECISIONS: G1 Mission Control 22h left; G2 Workflow Library 30h left.\n"
        "PORTFOLIO HEALTH: Mission Control green; Day-One Handoff paused; Workflow Library red.\n"
        "FINANCIAL SUMMARY: spend $0.00; revenue $180.00; net positive on current demo data."
    )
    operator.execute(
        """
        INSERT INTO digest_history (
            digest_id, digest_type, content, sections_included, word_count,
            operator_state, delivered_at, acknowledged_at, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "digest-demo-1",
            "critical_only",
            digest_content,
            json.dumps(["PENDING DECISIONS", "PORTFOLIO HEALTH", "FINANCIAL SUMMARY"]),
            len(digest_content.split()),
            "ACTIVE",
            None,
            None,
            (now - datetime.timedelta(minutes=20)).isoformat(),
        ),
    )

    operator.commit()
    db.close_all()
    return {"seeded": True, "opportunities": len(opportunities), "projects": len(projects), "research_tasks": len(research_tasks)}
