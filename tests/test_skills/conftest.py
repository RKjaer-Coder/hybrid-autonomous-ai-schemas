from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from skills.hermes_interfaces import HermesSessionContext, MockHermesRuntime


@pytest.fixture()
def test_data_dir(tmp_path: Path) -> Path:
    mapping = {
        "strategic_memory.db": "schemas/strategic_memory.sql",
        "immune_system.db": "schemas/immune_system.sql",
        "telemetry.db": "schemas/telemetry.sql",
        "financial_ledger.db": "schemas/financial_ledger.sql",
        "operator_digest.db": "schemas/operator_digest.sql",
    }
    for db_name, schema_path in mapping.items():
        db_path = tmp_path / db_name
        sql = Path(schema_path).read_text(encoding="utf-8")
        conn = sqlite3.connect(str(db_path))
        conn.executescript(sql)
        conn.commit()
        conn.close()
    return tmp_path


@pytest.fixture()
def mock_runtime(test_data_dir: Path) -> MockHermesRuntime:
    return MockHermesRuntime(data_dir=str(test_data_dir))


@pytest.fixture()
def session_context(test_data_dir: Path) -> HermesSessionContext:
    return HermesSessionContext(
        session_id="s-1",
        profile_name="hybrid-autonomous-ai",
        model_name="gpt-test",
        jwt_claims={},
        data_dir=str(test_data_dir),
    )
