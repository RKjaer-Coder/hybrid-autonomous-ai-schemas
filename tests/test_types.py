from __future__ import annotations

import pytest

from immune.types import BlockReason, CheckType, ImmuneVerdict, JudgeMode, Outcome, SheriffPayload, Tier, generate_uuid_v7


def test_frozen_dataclass_immutable(clean_sheriff_payload: SheriffPayload):
    with pytest.raises(Exception):
        clean_sheriff_payload.skill_name = "x"


def test_verdict_block_requires_reason(clean_sheriff_payload: SheriffPayload):
    with pytest.raises(ValueError):
        ImmuneVerdict(
            verdict_id=generate_uuid_v7(),
            check_type=CheckType.SHERIFF,
            tier=Tier.FAST_PATH,
            skill_name=clean_sheriff_payload.skill_name,
            session_id=clean_sheriff_payload.session_id,
            outcome=Outcome.BLOCK,
        )


def test_verdict_detail_truncation(clean_sheriff_payload: SheriffPayload):
    v = ImmuneVerdict(
        verdict_id=generate_uuid_v7(),
        check_type=CheckType.SHERIFF,
        tier=Tier.FAST_PATH,
        skill_name=clean_sheriff_payload.skill_name,
        session_id=clean_sheriff_payload.session_id,
        outcome=Outcome.BLOCK,
        block_reason=BlockReason.INTERNAL_ERROR,
        block_detail="x" * 500,
    )
    assert len(v.block_detail or "") == 200


def test_judge_verdict_defaults_to_normal_mode(clean_sheriff_payload: SheriffPayload):
    verdict = ImmuneVerdict(
        verdict_id=generate_uuid_v7(),
        check_type=CheckType.JUDGE,
        tier=Tier.FAST_PATH,
        skill_name=clean_sheriff_payload.skill_name,
        session_id=clean_sheriff_payload.session_id,
        outcome=Outcome.PASS,
    )
    assert verdict.judge_mode == JudgeMode.NORMAL
