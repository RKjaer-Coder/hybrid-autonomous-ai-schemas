from __future__ import annotations

import pytest

from harness_variants import HarnessVariantManager
from skills.db_manager import DatabaseManager
from skills.strategic_memory.skill import StrategicMemorySkill


def test_write_read_round_trip(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    s = StrategicMemorySkill(db)
    brief_id = s.write_brief("task-1", "Title", "Summary")
    out = s.read_brief(brief_id)
    assert out["brief_id"] == brief_id
    assert out["title"] == "Title"
    assert out["task_id"] == "task-1"
    assert out["actionability"] == "INFORMATIONAL"


def test_list_briefs_preserves_tags_and_provenance(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    s = StrategicMemorySkill(db)
    brief_id = s.write_brief(
        "task-2",
        "Tagged Brief",
        "Summary",
        tags=["ops", "runtime"],
        provenance_links=["source-1"],
        actionability="ACTION_RECOMMENDED",
    )

    rows = s.list_briefs(task_id="task-2", actionability="WATCH")

    assert rows[0]["brief_id"] == brief_id
    assert rows[0]["tags"] == ["ops", "runtime"]
    assert rows[0]["provenance_links"] == ["source-1"]


def test_full_brief_quality_rules_and_source_diversity_gate(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    s = StrategicMemorySkill(db)

    held_id = s.write_brief(
        "task-hold",
        "Single Source Brief",
        "Summary",
        confidence=0.91,
        actionability="ACTION_RECOMMENDED",
        depth_tier="FULL",
        source_urls=["https://example.com/a"],
        source_assessments=[
            {"url": "https://example.com/a", "relevance": 0.9, "freshness": "2026-04-14", "source_type": "tier2_web"},
        ],
        uncertainty_statement="too short",
        counter_thesis="also short",
    )
    passed_id = s.write_brief(
        "task-pass",
        "Two Source Brief",
        "Summary",
        confidence=0.72,
        actionability="ACTION_RECOMMENDED",
        depth_tier="FULL",
        source_urls=["https://example.com/a", "https://api.example.com/b"],
        source_assessments=[
            {"url": "https://example.com/a", "relevance": 0.8, "freshness": "2026-04-14", "source_type": "tier2_web"},
            {"url": "https://api.example.com/b", "relevance": 0.9, "freshness": "2026-04-14", "source_type": "tier1_api"},
        ],
        uncertainty_statement="There is still uncertainty around conversion quality and whether pricing pressure changes the conclusion.",
        counter_thesis="The strongest reason this could be wrong is that the second source may be lagging a rapidly changing market.",
    )

    held = s.read_brief(held_id)
    passed = s.read_brief(passed_id)
    held_rows = s.list_briefs(task_id="task-hold", source_diversity_hold=True, quality_warning=True)

    assert held["actionability"] == "WATCH"
    assert held["source_diversity_hold"] is True
    assert held["quality_warning"] is True
    assert passed["actionability"] == "ACTION_RECOMMENDED"
    assert passed["source_diversity_hold"] is False
    assert passed["quality_warning"] is False
    assert held_rows[0]["brief_id"] == held_id


def test_strategic_memory_emits_execution_traces_for_write_and_quality_signal(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    skill = StrategicMemorySkill(db)
    traces = HarnessVariantManager(str(test_data_dir / "telemetry.db"))

    brief_id = skill.write_brief(
        "task-trace",
        "Traceable Brief",
        "Summary",
        source_urls=["https://example.com/a"],
        provenance_links=["signal-1"],
    )
    signal = skill.record_quality_signal(brief_id, "incomplete", missing_dimension="source_diversity")

    strategic_traces = traces.list_execution_traces(limit=10, skill_name="strategic_memory")

    assert signal["brief_id"] == brief_id
    assert {row["role"] for row in strategic_traces} >= {
        "strategic_memory_brief_write",
        "brief_quality_signal",
    }


def test_route_brief_missing_brief_emits_failure_trace(test_data_dir):
    db = DatabaseManager(str(test_data_dir))
    skill = StrategicMemorySkill(db)
    traces = HarnessVariantManager(str(test_data_dir / "telemetry.db"))

    with pytest.raises(KeyError):
        skill.route_brief("missing-brief")

    strategic_traces = traces.list_execution_traces(limit=5, skill_name="strategic_memory")

    assert strategic_traces[0]["role"] == "strategic_memory_routing"
    assert strategic_traces[0]["judge_verdict"] == "FAIL"
    assert strategic_traces[0]["retention_class"] == "FAILURE_AUDIT"
