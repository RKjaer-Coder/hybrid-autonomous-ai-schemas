from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException


router = APIRouter()


def _plugin_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _runtime_config() -> dict[str, Any]:
    config_path = _plugin_root() / "runtime_config.json"
    if config_path.is_file():
        return json.loads(config_path.read_text(encoding="utf-8"))
    return {}


def _repo_root(config: dict[str, Any]) -> Path:
    raw = os.environ.get("HYBRID_AI_REPO_ROOT") or config.get("repo_root")
    if raw:
        return Path(str(raw)).expanduser().resolve()
    return Path.cwd().resolve()


def _data_dir(config: dict[str, Any]) -> str:
    raw = os.environ.get("HYBRID_AI_DATA_DIR") or config.get("data_dir") or "~/.hermes/data"
    return str(Path(str(raw)).expanduser())


def _service():
    config = _runtime_config()
    repo_root = _repo_root(config)
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))
    from skills.db_manager import DatabaseManager
    from skills.mission_control import MissionControlService

    db = DatabaseManager(_data_dir(config))
    return MissionControlService(
        db,
        interaction_channel=str(config.get("interaction_channel") or "hermes_dashboard"),
    ), db


def _with_service(method_name: str, *args: Any, **kwargs: Any) -> Any:
    try:
        service, db = _service()
    except Exception as exc:  # pragma: no cover - exercised in live Hermes.
        raise HTTPException(status_code=503, detail=f"Mission Control unavailable: {exc}") from exc
    try:
        return getattr(service, method_name)(*args, **kwargs)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    finally:
        db.close_all()


@router.get("/health")
async def health() -> dict[str, Any]:
    config = _runtime_config()
    return {
        "ok": True,
        "plugin": "hybrid-mission-control",
        "repo_root": str(_repo_root(config)),
        "data_dir": _data_dir(config),
        "gate_actions_enabled": bool(config.get("gate_actions_enabled", False)),
    }


@router.get("/snapshot")
async def snapshot() -> dict[str, Any]:
    return _with_service("snapshot")


@router.post("/projects/{project_id}/priority")
async def set_project_priority(project_id: str, body: dict[str, Any]) -> dict[str, Any]:
    return _with_service(
        "set_project_priority",
        project_id,
        str(body.get("priority", "")),
        focus_note=str(body.get("focus_note", "")),
    )


@router.post("/manual-tasks")
async def create_manual_task(body: dict[str, Any]) -> dict[str, Any]:
    return _with_service(
        "create_manual_task",
        title=str(body.get("title", "")),
        details=str(body.get("details", "")),
        priority=str(body.get("priority", "P2_NORMAL")),
        status=str(body.get("status", "TODO")),
        project_id=body.get("project_id"),
    )


@router.post("/manual-tasks/{task_id}")
async def update_manual_task(task_id: str, body: dict[str, Any]) -> dict[str, Any]:
    return _with_service(
        "update_manual_task",
        task_id,
        status=body.get("status"),
        priority=body.get("priority"),
        title=body.get("title"),
        details=body.get("details"),
    )


@router.post("/research-tasks")
async def create_research_task(body: dict[str, Any]) -> dict[str, Any]:
    return _with_service(
        "create_research_task",
        title=str(body.get("title", "")),
        brief=str(body.get("brief", "")),
        workflow_id=str(body.get("workflow_id", "operator_prompts")),
        domain=int(body["domain"]) if body.get("domain") not in (None, "") else None,
        priority=str(body.get("priority", "P2_NORMAL")),
        source=str(body.get("source", "operator")),
        depth=str(body.get("depth", "QUICK")),
        stale_after=body.get("stale_after"),
    )


@router.post("/tasks/priority")
async def update_system_task_priority(body: dict[str, Any]) -> dict[str, Any]:
    return _with_service(
        "update_system_task_priority",
        str(body.get("kind", "")),
        str(body.get("id", "")),
        str(body.get("priority", "")),
    )


@router.post("/alerts/{alert_id}/ack")
async def acknowledge_alert(alert_id: str) -> dict[str, Any]:
    return _with_service("acknowledge_alert", alert_id)
