from __future__ import annotations

import json
from pathlib import Path

from kernel.services.runtime_artifacts import (
    first_live_project_acceptance_check_packet,
    pre_live_artifact_controls_disabled,
    pre_live_closed_control_contract,
    pre_live_controls_are_closed,
    pre_live_evidence_crosswalk_row,
    pre_live_fail_closed_controls,
    runtime_evidence_manifest_item,
    runtime_artifact_path,
    target_machine_evidence_check_packet,
    write_runtime_artifact,
)
from skills.config import IntegrationConfig


def _config(tmp_path: Path) -> IntegrationConfig:
    return IntegrationConfig(
        data_dir=str(tmp_path / "data"),
        skills_dir=str(tmp_path / "skills"),
        checkpoints_dir=str(tmp_path / "skills" / "checkpoints"),
        alerts_dir=str(tmp_path / "alerts"),
    ).resolve_paths()


def test_runtime_artifact_writer_applies_closed_control_defaults(tmp_path):
    cfg = _config(tmp_path)
    path = write_runtime_artifact(cfg, "pre_live_mission_control", {"status": "ready"})

    payload = json.loads(path.read_text(encoding="utf-8"))

    assert path == runtime_artifact_path(cfg, "pre_live_mission_control")
    assert payload["artifact_path"] == str(path)
    assert payload["live_controls_enabled"] is False
    assert "generated_at" not in payload


def test_runtime_artifact_writer_applies_generated_at_defaults(tmp_path):
    cfg = _config(tmp_path)
    path = write_runtime_artifact(cfg, "replay_readiness_report", {"status": "ready"})

    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload["artifact_path"] == str(path)
    assert payload["generated_at"].endswith("+00:00")


def test_pre_live_closed_control_contract_is_shared_and_fail_closed():
    contract = pre_live_closed_control_contract()

    assert pre_live_controls_are_closed(contract) is True
    assert pre_live_artifact_controls_disabled(
        {"live_controls_enabled": False, "closed_control_contract": contract}
    ) is True
    assert "paid_provider_calls" in pre_live_fail_closed_controls()

    opened = dict(contract)
    opened["paid_provider_calls_enabled"] = True
    assert pre_live_controls_are_closed(opened) is False
    assert pre_live_artifact_controls_disabled(
        {"live_controls_enabled": False, "closed_control_contract": opened}
    ) is False
    assert pre_live_artifact_controls_disabled({"live_controls_enabled": True}) is False


def test_runtime_evidence_manifest_item_records_file_hash(tmp_path):
    artifact = tmp_path / "packet.json"
    artifact.write_text('{"ok": true}\n', encoding="utf-8")

    item = runtime_evidence_manifest_item("packet", artifact, packet_hash="abc")

    assert item["name"] == "packet"
    assert item["path"] == str(artifact)
    assert item["exists"] is True
    assert item["sha256"]
    assert item["packet_hash"] == "abc"
    assert item["required_before_live_authority"] is True


def test_target_machine_evidence_check_packet_fails_closed_on_missing_evidence(tmp_path):
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    packet_path = bundle / "target_machine_validation_run_packet.json"
    sha_path = bundle / "SHA256SUMS"
    packet = {
        "run_steps": [{"required_evidence": ["target_machine_artifact_bundle"]}],
        "evidence_manifest": [{"name": "missing", "path": str(bundle / "missing.json"), "sha256": "x"}],
        "live_controls_enabled": False,
    }

    payload = target_machine_evidence_check_packet(
        bundle=bundle,
        packet_path=packet_path,
        sha_path=sha_path,
        packet=packet,
        sha_entries={},
        evidence_records={},
        generated_at="2026-05-12T00:00:00+00:00",
        artifact_path=tmp_path / "out.json",
    )

    assert payload["status"] == "blocked"
    assert "required_evidence_missing" in payload["blockers"]
    assert "manifest_artifact_missing" in payload["blockers"]
    assert payload["live_controls_enabled"] is False


def test_first_live_project_acceptance_check_packet_preserves_gate_shape(tmp_path):
    packet = {
        "fixture_id": "fixture-1",
        "summary": {"local_artifact_only": True, "external_commitments_allowed": False},
        "artifact_contract": {"external_delivery": "prepared_intent_only_until_operator_gate"},
        "workflow": [{"operator_gate_required": True, "external_side_effects_executed": False}],
        "dry_run": {"close_path": {"feedback_ingested": True, "close_or_continue_requires_operator_gate": True}},
        "live_controls_enabled": False,
    }

    payload = first_live_project_acceptance_check_packet(
        packet=packet,
        generated_at="2026-05-12T00:00:00+00:00",
        artifact_path=tmp_path / "acceptance.json",
    )

    assert payload["status"] == "accepted_pre_live_local_only"
    assert payload["blockers"] == []
    assert all(payload["checks"].values())
    assert payload["activation_effect"] == "none"


def test_pre_live_evidence_crosswalk_row_fails_closed_on_open_control(tmp_path):
    artifact = tmp_path / "artifact.json"
    artifact.write_text("{}", encoding="utf-8")
    contract = pre_live_closed_control_contract()
    contract["paid_provider_calls_enabled"] = True

    row = pre_live_evidence_crosswalk_row(
        checklist_id="s10-test",
        requirement="Paid routes stay closed.",
        step_names=["provider_check"],
        artifact_names=["provider_packet"],
        closed_control_keys=["paid_provider_calls_enabled"],
        blocker_conditions=["closed_control_contract_opened_live_control"],
        steps_by_name={"provider_check": {"required_evidence": ["budget_grant_proof"]}},
        artifacts_by_name={
            "provider_packet": runtime_evidence_manifest_item("provider_packet", artifact)
        },
        closed_controls=contract,
    )

    assert row["ready"] is False
    assert row["opened_controls"] == ["paid_provider_calls_enabled"]
    assert row["missing_steps"] == []
    assert row["missing_artifacts"] == []


def test_review_loop_configuration_names_kernel_invariants():
    repo_root = Path(__file__).resolve().parents[1]
    config = json.loads((repo_root / ".greptile" / "config.json").read_text(encoding="utf-8"))
    rule_ids = {rule["id"] for rule in config["rules"]}

    assert "kernel-command-event-boundary" in rule_ids
    assert "runtime-surfaces-are-adapters" in rule_ids
    assert "self-improvement-review-only" in rule_ids
    assert "service-layer-stays-thin" in rule_ids
    assert "no-raw-secrets-in-events" in rule_ids
