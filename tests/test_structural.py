from __future__ import annotations

from immune.classifiers.structural_validator import validate_structure
from immune.types import ImmuneConfig


def _payload(**kwargs):
    data = {
        "session_id": "018f0a7f-1234-7abc-8def-1234567890ab",
        "skill_name": "skill_a",
        "tool_name": "safe_tool",
        "arguments": {"k": "v"},
        "raw_prompt": "ok",
    }
    data.update(kwargs)
    return type("P", (), data)()


def test_required_fields_pass(default_config: ImmuneConfig):
    assert validate_structure(_payload(), default_config) is None


def test_required_fields_fail(default_config: ImmuneConfig):
    got = validate_structure(_payload(session_id=None), default_config)
    assert got and "required field" in got[1]


def test_string_length_pass(default_config: ImmuneConfig):
    assert validate_structure(_payload(raw_prompt="a" * 102400), default_config) is None


def test_string_length_fail(default_config: ImmuneConfig):
    got = validate_structure(_payload(raw_prompt="a" * 102401), default_config)
    assert got and "100KB" in got[1]


def test_depth_pass(default_config: ImmuneConfig):
    d = x = {}
    for _ in range(20):
        n = {}
        x["k"] = n
        x = n
    assert validate_structure(_payload(arguments=d), default_config) is None


def test_depth_fail(default_config: ImmuneConfig):
    d = x = {}
    for _ in range(21):
        n = {}
        x["k"] = n
        x = n
    got = validate_structure(_payload(arguments=d), default_config)
    assert got and "depth" in got[1]


def test_circular_fail(default_config: ImmuneConfig):
    l = []
    l.append(l)
    got = validate_structure(_payload(arguments={"x": l}), default_config)
    assert got and "Circular" in got[1]


def test_id_format_fail_uuid(default_config: ImmuneConfig):
    got = validate_structure(_payload(session_id="018f0a7f-1234-4abc-8def-1234567890ab"), default_config)
    assert got and "session_id" in got[1]


def test_skill_format_fail(default_config: ImmuneConfig):
    got = validate_structure(_payload(skill_name="Bad-Skill"), default_config)
    assert got and "skill_name" in got[1]


def test_known_tool_fail(default_config: ImmuneConfig):
    got = validate_structure(_payload(tool_name="unknown"), default_config)
    assert got and "Unknown tool" in got[1]


def test_known_tool_skipped_when_empty():
    cfg = ImmuneConfig(known_tool_registry=frozenset())
    assert validate_structure(_payload(tool_name="anything"), cfg) is None


def test_required_type_fail(default_config: ImmuneConfig):
    got = validate_structure(_payload(arguments=[]), default_config)
    assert got and "required field" in got[1]
