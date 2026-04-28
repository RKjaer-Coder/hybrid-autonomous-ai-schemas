from __future__ import annotations

import json
from pathlib import Path
from urllib import request as urllib_request

from skills.db_manager import DatabaseManager
from skills.mission_control import MissionControlHTTPServer, MissionControlService, seed_demo_state


REPO_ROOT = Path(__file__).resolve().parents[2]


def test_mission_control_snapshot_surfaces_workflow_board_and_tasks(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    strategic = db.get_connection("strategic_memory")
    financial = db.get_connection("financial_ledger")
    operator = db.get_connection("operator_digest")

    strategic.execute(
        """
        INSERT INTO opportunity_records (
            opportunity_id, income_mechanism, title, thesis, detected_by, council_verdict_id,
            validation_spend, validation_report, cashflow_estimate, status, project_id,
            learning_record, provenance_links, provenance_degraded, trust_tier, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "opp-1", "software_product", "Focused asset", "Build a focused asset", "operator", None,
            0.0, None, json.dumps({"monthly_usd": 500}), "ACTIVE", "proj-1", None, "[]", 0, 2,
            "2026-04-23T09:00:00+00:00", "2026-04-23T09:00:00+00:00",
        ),
    )
    strategic.execute(
        """
        INSERT INTO research_tasks (
            task_id, domain, source, title, brief, priority, status, max_spend_usd,
            actual_spend_usd, output_brief_id, follow_up_tasks, stale_after, tags,
            depth_upgrade, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "research-1", 2, "operator", "Research adjacent demand", "Map adjacent demand",
            "P1_HIGH", "PENDING", 0.0, 0.0, None, "[]", None, "[]", 0,
            "2026-04-23T09:10:00+00:00", "2026-04-23T09:10:00+00:00",
        ),
    )
    strategic.commit()

    financial.execute(
        """
        INSERT INTO projects (
            project_id, opportunity_id, name, income_mechanism, thesis, success_criteria,
            compute_budget, portfolio_weight, status, kill_score_watch, cashflow_actual_usd,
            council_verdict_id, pivot_log, created_at, closed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "proj-1", "opp-1", "Mission Control Asset", "software_product", "Build operator console",
            json.dumps({"primary": "cashflow_target"}), json.dumps({"max_executor_hours": 40}), 0.35,
            "ACTIVE", 0, 120.0, None, "[]", "2026-04-23T09:15:00+00:00", None,
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
            "phase-1", "proj-1", "BUILD", "ACTIVE", 1, "Ship lean mission control",
            json.dumps(["usable board"]), json.dumps({"executor_hours_cap": 20, "cloud_spend_cap_usd": 0}),
            json.dumps({"executor_hours": 6, "cloud_spend_usd": 0}), "[]", None,
            "2026-04-23T09:20:00+00:00", None, None,
        ),
    )
    financial.commit()

    operator.execute(
        """
        INSERT INTO harvest_requests (
            harvest_id, task_id, prompt_text, target_interface, context_summary, priority,
            status, expires_at, operator_result, relevance_score, clarification_sent, created_at, delivered_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "harvest-1", "research-1", "Review competitor screenshots", "mission_control",
            "Need visual references", "P2_NORMAL", "PENDING", "2026-04-24T09:30:00+00:00",
            None, None, 0, "2026-04-23T09:30:00+00:00", None,
        ),
    )
    operator.execute(
        """
        INSERT INTO gate_log (
            gate_id, gate_type, trigger_description, context_packet, project_id, status,
            timeout_hours, operator_response, created_at, responded_at, expires_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "gate-1", "G1", "go/no-go review", json.dumps({"summary": "review"}), "proj-1", "PENDING",
            24.0, None, "2026-04-23T09:35:00+00:00", None, "2026-04-24T09:35:00+00:00",
        ),
    )
    operator.commit()

    service = MissionControlService(db)
    service.set_project_priority("proj-1", "P0_IMMEDIATE")
    service.create_manual_task(title="Review workflow board", priority="P1_HIGH", status="TODO", project_id="proj-1")
    snapshot = service.snapshot()

    assert snapshot["contract"] == "hermes-dashboard-plugin-v1"
    assert snapshot["runtime_posture"]["mode"] == "prebuilt_without_live_hermes"
    assert snapshot["runtime_posture"]["heavy_services"] == []
    assert snapshot["overview"]["pending_gates"] == 1
    assert snapshot["workflow"]["projects"]["ACTIVE"] == 1
    assert "council" in snapshot
    assert "research" in snapshot
    assert "finance" in snapshot
    assert "replay" in snapshot
    assert "system" in snapshot
    assert snapshot["finance"]["summary"]["autonomous_paid_spend_enabled"] is False
    assert any(lane["id"] == "BUILD" and lane["count"] == 1 for lane in snapshot["project_board"]["lanes"])
    assert any(card["kind"] == "manual" for card in snapshot["tasks"]["cards"])
    assert any(card["priority"] == "P0_IMMEDIATE" for card in snapshot["project_board"]["cards"])


def test_mission_control_can_reprioritize_system_tasks(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    strategic = db.get_connection("strategic_memory")
    operator = db.get_connection("operator_digest")
    strategic.execute(
        """
        INSERT INTO research_tasks (
            task_id, domain, source, title, brief, priority, status, max_spend_usd,
            actual_spend_usd, output_brief_id, follow_up_tasks, stale_after, tags,
            depth_upgrade, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "research-2", 1, "operator", "Research pricing", "Map pricing", "P3_BACKGROUND", "PENDING",
            0.0, 0.0, None, "[]", None, "[]", 0, "2026-04-23T10:00:00+00:00", "2026-04-23T10:00:00+00:00",
        ),
    )
    strategic.commit()
    operator.execute(
        """
        INSERT INTO harvest_requests (
            harvest_id, task_id, prompt_text, target_interface, context_summary, priority,
            status, expires_at, operator_result, relevance_score, clarification_sent, created_at, delivered_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            "harvest-2", "research-2", "Collect examples", "mission_control", "Need examples",
            "P3_BACKGROUND", "PENDING", "2026-04-24T10:00:00+00:00", None, None, 0,
            "2026-04-23T10:00:00+00:00", None,
        ),
    )
    operator.commit()

    service = MissionControlService(db)
    research = service.update_system_task_priority("research", "research-2", "P0_IMMEDIATE")
    harvest = service.update_system_task_priority("harvest", "harvest-2", "P1_HIGH")

    assert research["priority"] == "P0_IMMEDIATE"
    assert harvest["priority"] == "P1_HIGH"
    assert strategic.execute("SELECT priority FROM research_tasks WHERE task_id = 'research-2'").fetchone()[0] == "P0_IMMEDIATE"
    assert operator.execute("SELECT priority FROM harvest_requests WHERE harvest_id = 'harvest-2'").fetchone()[0] == "P1_HIGH"


def test_mission_control_service_records_hermes_dashboard_channel(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    service = MissionControlService(db, interaction_channel="hermes_dashboard")
    service.create_manual_task(title="Check Hermes dashboard tab")
    operator = db.get_connection("operator_digest")

    row = operator.execute(
        "SELECT channel FROM operator_heartbeat ORDER BY timestamp DESC LIMIT 1"
    ).fetchone()

    assert row["channel"] == "hermes_dashboard"


def test_hermes_dashboard_plugin_artifacts_are_tiny_and_harness_backed():
    plugin_root = REPO_ROOT / "hermes_plugins" / "hybrid-mission-control"
    manifest = json.loads((plugin_root / "dashboard" / "manifest.json").read_text(encoding="utf-8"))
    index_js = (plugin_root / "dashboard" / "dist" / "index.js").read_text(encoding="utf-8")
    style_css = (plugin_root / "dashboard" / "dist" / "style.css").read_text(encoding="utf-8")
    plugin_api = (plugin_root / "dashboard" / "plugin_api.py").read_text(encoding="utf-8")

    assert manifest["name"] == "hybrid-mission-control"
    assert manifest["tab"]["path"] == "/mission-control"
    assert manifest["api"] == "plugin_api.py"
    assert "window.__HERMES_PLUGINS__.register(\"hybrid-mission-control\"" in index_js
    assert "/api/plugins/hybrid-mission-control" in index_js
    assert "Final plugin shape" in index_js
    assert "No bundled React, no Node bridge, no live stream server" in index_js
    assert "Below Threshold" in index_js
    assert "setInterval(refresh, 15000)" in index_js
    assert "\"council\", \"Council\"" in index_js
    assert "\"finance\", \"Finance\"" in index_js
    assert "\"replay\", \"Replay\"" in index_js
    assert "MissionControlService" in plugin_api
    assert "review_g3" not in plugin_api
    assert "review_quarantine" not in plugin_api
    assert "var(--color-card)" in style_css
    assert "letter-spacing: -" not in style_css
    assert "vw" not in style_css


def test_mission_control_http_server_serves_snapshot_api(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    server = MissionControlHTTPServer(("127.0.0.1", 0), MissionControlService(db))
    try:
        import threading

        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        with urllib_request.urlopen(f"http://{host}:{port}/api/snapshot") as response:
            payload = json.loads(response.read().decode("utf-8"))
        assert "overview" in payload
        assert "project_board" in payload
    finally:
        server.shutdown()
        server.server_close()
        db.close_all()


def test_seed_demo_state_populates_empty_runtime_data(test_data_dir):
    result = seed_demo_state(str(test_data_dir))
    db = DatabaseManager(str(test_data_dir))
    strategic = db.get_connection("strategic_memory")
    financial = db.get_connection("financial_ledger")
    operator = db.get_connection("operator_digest")

    assert result["seeded"] is True
    assert strategic.execute("SELECT COUNT(*) FROM opportunity_records").fetchone()[0] >= 3
    assert financial.execute("SELECT COUNT(*) FROM projects").fetchone()[0] >= 3
    assert operator.execute("SELECT COUNT(*) FROM operator_manual_tasks").fetchone()[0] >= 2
    db.close_all()
