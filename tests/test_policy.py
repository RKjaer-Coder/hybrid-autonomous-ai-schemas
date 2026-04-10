from __future__ import annotations

from immune.classifiers.policy_checker import check_policy
from immune.types import ImmuneConfig, SheriffPayload, generate_uuid_v7


def payload(**kwargs) -> SheriffPayload:
    base = dict(
        session_id=generate_uuid_v7(),
        skill_name="immune_system",
        tool_name="safe_tool",
        arguments={},
        source_trust_tier=4,
    )
    base.update(kwargs)
    return SheriffPayload(**base)


def test_allowlist_pass(default_config: ImmuneConfig):
    p = payload(tool_name="web_fetch", arguments={"url": "http://localhost/x"})
    assert check_policy(p, default_config) is None


def test_allowlist_fail(default_config: ImmuneConfig):
    p = payload(tool_name="web_fetch", arguments={"url": "http://evil.com/x"})
    assert check_policy(p, default_config) is not None


def test_resource_limit_pass(default_config: ImmuneConfig):
    p = payload(jwt_claims={"max_tool_calls": 5, "current_tool_calls": 2})
    assert check_policy(p, default_config) is None


def test_resource_limit_fail(default_config: ImmuneConfig):
    p = payload(jwt_claims={"max_tool_calls": 1, "current_tool_calls": 2})
    assert check_policy(p, default_config) is not None


def test_trust_tier_pass(default_config: ImmuneConfig):
    p = payload(source_trust_tier=3, tool_name="shell_command", arguments={"command": "echo hi"})
    assert check_policy(p, default_config) is None


def test_trust_tier_fail(default_config: ImmuneConfig):
    p = payload(tool_name="shell_command", arguments={"command": "echo hi"})
    assert check_policy(p, default_config) is not None


def test_dangerous_pattern_pass(default_config: ImmuneConfig):
    p = payload(tool_name="shell_command", arguments={"command": "ls", "burner_room": True})
    assert check_policy(p, default_config) is None


def test_dangerous_pattern_fail(default_config: ImmuneConfig):
    p = payload(tool_name="shell_command", arguments={"command": "rm -rf /", "burner_room": True})
    assert check_policy(p, default_config) is not None
