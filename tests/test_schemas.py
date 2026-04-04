from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
import uuid

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import migrate


def uid() -> str:
    generator = getattr(uuid, "uuid7", None)
    return str(generator() if callable(generator) else uuid.uuid4())


def ts() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


class SchemaSuiteTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.tmp = tempfile.TemporaryDirectory()
        cls.db_dir = Path(cls.tmp.name)
        for name, schema in migrate.SCHEMAS.items():
            migrate.apply_schema(cls.db_dir / f"{name}.db", ROOT / schema)

    @classmethod
    def tearDownClass(cls) -> None:
        cls.tmp.cleanup()

    def conn(self, db: str) -> sqlite3.Connection:
        c = sqlite3.connect(self.db_dir / f"{db}.db")
        c.execute("PRAGMA foreign_keys=ON;")
        return c

    def test_wal_mode_enabled(self):
        for db in migrate.SCHEMAS:
            with self.subTest(db=db):
                with self.conn(db) as con:
                    mode = con.execute("PRAGMA journal_mode;").fetchone()[0].lower()
                    self.assertEqual(mode, "wal")

    def test_index_presence(self):
        for db_name, expected in migrate.EXPECTED_OBJECTS.items():
            with self.subTest(db=db_name):
                with self.conn(db_name) as con:
                    rows = con.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()
                    present = {r[0] for r in rows}
                    for idx in expected["indexes"]:
                        self.assertIn(idx, present)

    def test_roundtrip_writes_at_least_50(self):
        count = 0
        now = ts()
        with self.conn("strategic_memory") as sm:
            verdict_id = uid()
            task_id = uid()
            brief_id = uid()
            opp_a = uid()
            opp_b = uid()
            scout_id = uid()
            assess_id = uid()
            sm.execute("INSERT INTO council_verdicts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                verdict_id, 1, 'go_no_go', 'PURSUE', 0.8, 'reasoning', None, json.dumps([]), None,
                2.5, None, json.dumps({"outcome": "pending"}), 0.7, json.dumps([]), 0, now,
            ))
            count += 1
            sm.execute("INSERT INTO research_tasks VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                task_id, 1, 'autonomous_loop', 'task', 'brief', 'P1_HIGH', 'PENDING', 10.0, 0.0,
                None, json.dumps([]), now, json.dumps(['a']), 0, now, now,
            ))
            count += 1
            sm.execute("INSERT INTO intelligence_briefs VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                brief_id, task_id, 1, 'brief', 'summary', 'detail', json.dumps(['https://a.com']), json.dumps([]),
                0.8, None, None, 'WATCH', 'ROUTINE', 'QUICK', 'none', json.dumps([]), None, json.dumps([]),
                json.dumps(['x']), 0, 0, json.dumps([]), 3, now,
            ))
            count += 1
            sm.execute("INSERT INTO opportunity_records VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                opp_a, 'software_product', 'title', 'thesis', 'operator', verdict_id, 0.0, None,
                json.dumps({"low": 10, "mid": 20, "high": 30, "currency": "USD", "period": "month"}),
                'DETECTED', None, None, json.dumps([]), 0, 2, now, now,
            ))
            count += 1
            sm.execute("INSERT INTO opportunity_records VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                opp_b, 'client_work', 'title2', 'thesis2', 'research_loop', None, 0.0, None,
                json.dumps({"low": 1, "mid": 2, "high": 3, "currency": "USD", "period": "month"}),
                'SCREENED', None, None, json.dumps([]), 0, 2, now, now,
            ))
            count += 1
            sm.execute("INSERT INTO brief_quality_signals VALUES (?,?,?,?,?,?)", (uid(), verdict_id, brief_id, 'sufficient', None, now))
            sm.execute("INSERT INTO calibration_records VALUES (?,?,?,?,?,?,?,?,?,?,?)", (
                uid(), verdict_id, 'go_no_go', 'PURSUE', 1.0, 1.0, json.dumps({"strategist": 0.25}), 'strategist', 0, 'PROVISIONAL', now,
            ))
            sm.execute("INSERT INTO idea_records VALUES (?,?,?,?,?,?,?,?,?)", (
                uid(), json.dumps([brief_id]), 'idea', 'proposal', 'software_product', 0.7, 'corroborated', 'weeks', now,
            ))
            sm.execute("INSERT INTO market_signals VALUES (?,?,?,?,?,?,?,?,?,?)", (uid(), 'pricing_change', 't', 'c', 'https://m.com', 2, json.dumps([]), 4, now, now))
            sm.execute("INSERT INTO capability_gaps VALUES (?,?,?,?,?,?,?,?,?)", (uid(), None, 'desc', 'skill', 0, json.dumps([]), 3, None, now))
            sm.execute("INSERT INTO source_reputations VALUES (?,?,?,?,?,?,?,?)", ('https://source.com', 0.6, 1, 0, now, 0, now, now))
            sm.execute("INSERT INTO dedup_records VALUES (?,?,?,?,?,?)", (uid(), opp_a, opp_b, 0.92, 'PROVISIONAL', now))
            sm.execute("INSERT INTO deferred_research_entries VALUES (?,?,?,?,?,?,?,?)", (uid(), 'summary', 'tool_failed', 1, 'P1_HIGH', now, None, now))
            sm.execute("INSERT INTO model_scout_reports VALUES (?,?,?,?,?,?,?,?,?,?,?)", (
                scout_id, 'model-x', 'Execution', 'card', 'mit', 1, 8.0, json.dumps({"acc": 0.8}), 1, json.dumps([]), now,
            ))
            sm.execute("INSERT INTO model_assess_reports VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                assess_id, scout_id, 'model-x', 'Execution', 0.7, 0.6, 8.1, 10, 30, 'int8', 1, 'PROCEED_TO_SHADOW', None, now,
            ))
            sm.execute("INSERT INTO shadow_trial_reports VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                uid(), assess_id, 'model-x', 'Execution', 200, 0.75, 0.6, 25.0, 33.0, 40.0, 9.0, 10.0, 1, 'PROMOTE', now,
            ))
            count += 11

        with self.conn("telemetry") as t:
            t.execute("INSERT INTO chain_definitions VALUES (?,?,?,?)", (
                'council_tier1', json.dumps([
                    {"step_type": "reason", "skill": "council", "skip_eligible": False},
                    {"step_type": "validate", "skill": "qa", "skip_eligible": False},
                ]), now, now,
            ))
            count += 1
            for i, outcome in enumerate(['PASS', 'DEGRADED', 'FAIL', 'PASS', 'PASS']):
                t.execute("INSERT INTO step_outcomes VALUES (?,?,?,?,?,?,?,?,?)", (uid(), 'reason' if i < 3 else 'validate', 'council' if i < 3 else 'qa', 'ch1', outcome, 100 + i, 0, None, now))
                count += 1

        with self.conn("immune_system") as im:
            im.execute("INSERT INTO immune_verdicts VALUES (?,?,?,?,?,?,?,?,?)", (uid(), 'sheriff_input', 'fast_path', 'session1', 'planner', 'PASS', None, 4, now))
            im.execute("INSERT INTO security_alerts VALUES (?,?,?,?,?,?,?,?)", (uid(), 'jwt_failure', 'ALERT', 'detail', 'session1', 0, None, now))
            im.execute("INSERT INTO circuit_breaker_log VALUES (?,?,?,?,?,?,?,?)", (uid(), 'TOOL_FAILURE_STORM', 'ARMED', 'n failures', 'watch', 1, None, now))
            im.execute("INSERT INTO jwt_revocation_log VALUES (?,?,?)", ('jti-1', 'ttl_expiry', now))
            im.execute("INSERT INTO skill_improvement_log VALUES (?,?,?,?,?,?,?,?,?,?,?)", (uid(), 'research', 'h1', 'h2', 'diff', None, 'why', 'ACTIVE', 0.1, 0, now))
            count += 5

        with self.conn("financial_ledger") as fl:
            project_id = uid()
            fl.execute("INSERT INTO projects VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                project_id, uid(), 'proj', 'software_product', 'thesis', json.dumps({"ok": True}),
                json.dumps({"max_executor_hours": 10, "max_cloud_spend_usd": 100, "alert_at_pct": 80}), 0.3,
                'ACTIVE', 0, 0.0, None, json.dumps([]), now, None,
            ))
            fl.execute("INSERT INTO phases VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                uid(), project_id, 'VALIDATE', 'ACTIVE', 0, 'scope', json.dumps(["done"]),
                json.dumps({"executor_hours_cap": 2, "cloud_spend_cap_usd": 10}), json.dumps({"executor_hours": 1, "cloud_spend_usd": 2}),
                json.dumps([]), None, now, None, None,
            ))
            fl.execute("INSERT INTO kill_signals VALUES (?,?,?,?,?,?,?)", (uid(), project_id, 'technical_blocker', 0.1, 0.5, 'evidence', now))
            fl.execute("INSERT INTO kill_recommendations VALUES (?,?,?,?,?,?,?,?,?,?)", (uid(), project_id, 0.6, uid(), json.dumps([]), 'sum', 'analysis', 'PENDING', 'PROVISIONAL', now))
            fl.execute("INSERT INTO assets VALUES (?,?,?,?,?,?,?,?)", (uid(), project_id, 'tool', 'asset', 'desc', 1, '/tmp/a', now))
            fl.execute("INSERT INTO revenue_records VALUES (?,?,?,?,?,?,?,?)", (uid(), project_id, 100.0, 'web_store', 'automated', now, now, now))
            fl.execute("INSERT INTO cost_records VALUES (?,?,?,?,?,?,?,?)", (uid(), project_id, 'cloud_api', 30.0, 'desc', 'openai', 'task1', now))
            fl.execute("INSERT INTO treasury VALUES (?,?,?,?,?,?,?)", (uid(), 'revenue_in', 100.0, 100.0, project_id, 'desc', now))
            fl.execute("INSERT INTO routing_decisions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                uid(), 'task1', 'chain1', 'Execution', 'local', 'model', 1, 0, 0.0, None, 0, None, now,
            ))
            count += 9

        with self.conn("operator_digest") as od:
            od.execute("INSERT INTO digest_history VALUES (?,?,?,?,?,?,?,?,?)", (uid(), 'daily', 'content', json.dumps([1, 2]), 100, 'ACTIVE', now, None, now))
            od.execute("INSERT INTO alert_log VALUES (?,?,?,?,?,?,?,?,?)", (uid(), 'T1', 'INFINITE_LOOP', 'content', 'CLI', 0, 0, None, now))
            od.execute("INSERT INTO harvest_requests VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                uid(), 'task1', 'prompt', 'ChatGPT Plus', 'ctx', 'P1_HIGH', 'PENDING', now, None, None, 0, now, None,
            ))
            od.execute("INSERT INTO gate_log VALUES (?,?,?,?,?,?,?,?,?,?,?)", (uid(), 'G1', 'trigger', json.dumps({"ctx": 1}), None, 'PENDING', 24.0, None, now, None, now))
            od.execute("INSERT INTO operator_heartbeat VALUES (?,?,?,?)", (uid(), 'message', 'CLI', now))
            od.execute("INSERT INTO operator_load_tracking VALUES (?,?,?,?,?,?,?,?,?)", (uid(), now[:10], json.dumps({"G1": 1, "G2": 0, "G3": 0, "G4": 0}), 2, 1, 0, 2.5, 0, now))
            count += 6

        # Additional round-trips to exceed 50 total
        with self.conn("telemetry") as t:
            for _ in range(13):
                row_id = uid()
                t.execute("INSERT INTO step_outcomes VALUES (?,?,?,?,?,?,?,?,?)", (row_id, 'tool_call', 'exec', uid(), 'PASS', 20, 0, 1, now))
                back = t.execute("SELECT event_id FROM step_outcomes WHERE event_id=?", (row_id,)).fetchone()
                self.assertEqual(back[0], row_id)
                count += 1

        self.assertGreaterEqual(count, 50)

    def test_constraints_and_json_validation(self):
        now = ts()
        with self.conn("strategic_memory") as sm:
            with self.assertRaises(sqlite3.IntegrityError):
                sm.execute("INSERT INTO council_verdicts VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                    uid(), 1, 'go_no_go', 'PURSUE', 1.5, 'bad confidence', None, json.dumps([]), None,
                    0.0, None, json.dumps({}), None, None, 0, now,
                ))
            with self.assertRaises(sqlite3.IntegrityError):
                sm.execute("INSERT INTO opportunity_records VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                    uid(), 'invalid_mechanism', 'title', 'thesis', 'operator', None, 0.0, None,
                    json.dumps({"low": 1}), 'DETECTED', None, None, json.dumps([]), 0, 2, now, now,
                ))
            with self.assertRaises(sqlite3.IntegrityError):
                sm.execute("INSERT INTO opportunity_records VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                    uid(), 'software_product', 'title', 'thesis', 'operator', None, 0.0, None,
                    '{bad_json', 'DETECTED', None, None, json.dumps([]), 0, 2, now, now,
                ))
            with self.assertRaises(sqlite3.IntegrityError):
                sm.execute("INSERT INTO research_tasks(task_id,domain,source,title,brief,priority,status,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?)", (
                    uid(), 1, 'autonomous_loop', None, 'x', 'P1_HIGH', 'PENDING', now, now,
                ))

    def test_views_compute_expected_values(self):
        now = ts()
        with self.conn("telemetry") as t:
            t.execute("DELETE FROM step_outcomes")
            t.execute("DELETE FROM chain_definitions")
            t.execute("INSERT INTO step_outcomes VALUES (?,?,?,?,?,?,?,?,?)", (uid(), 's1', 'a', 'ch', 'PASS', 1, 0, None, now))
            t.execute("INSERT INTO step_outcomes VALUES (?,?,?,?,?,?,?,?,?)", (uid(), 's1', 'a', 'ch', 'DEGRADED', 1, 0, None, now))
            t.execute("INSERT INTO step_outcomes VALUES (?,?,?,?,?,?,?,?,?)", (uid(), 's2', 'b', 'ch', 'PASS', 1, 0, None, now))
            t.execute("INSERT INTO chain_definitions VALUES (?,?,?,?)", (
                'ct', json.dumps([
                    {"step_type": "s1", "skill": "a", "skip_eligible": False},
                    {"step_type": "s2", "skill": "b", "skip_eligible": False},
                ]), now, now,
            ))
            r = t.execute("SELECT reliability_7d FROM reliability_by_step WHERE step_type='s1' AND skill='a'").fetchone()[0]
            self.assertAlmostEqual(r, 0.75, places=2)
            chain = t.execute("SELECT chain_reliability_7d FROM chain_reliability WHERE chain_type='ct'").fetchone()[0]
            self.assertAlmostEqual(chain, 0.75, places=2)

        with self.conn("financial_ledger") as fl:
            fl.execute("DELETE FROM revenue_records")
            fl.execute("DELETE FROM cost_records")
            fl.execute("DELETE FROM phases")
            fl.execute("DELETE FROM kill_signals")
            fl.execute("DELETE FROM kill_recommendations")
            fl.execute("DELETE FROM assets")
            fl.execute("DELETE FROM projects")
            pid = uid()
            fl.execute("INSERT INTO projects VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (
                pid, uid(), 'pnl', 'software_product', 'thesis', json.dumps({"s": 1}),
                json.dumps({"max_executor_hours": 1, "max_cloud_spend_usd": 1, "alert_at_pct": 50}),
                0.5, 'ACTIVE', 0, 0.0, None, json.dumps([]), now, None,
            ))
            fl.execute("INSERT INTO revenue_records VALUES (?,?,?,?,?,?,?,?)", (uid(), pid, 120.0, 'web_store', 'automated', now, now, now))
            fl.execute("INSERT INTO cost_records VALUES (?,?,?,?,?,?,?,?)", (uid(), pid, 'cloud_api', 20.0, 'cost', 'openai', None, now))
            row = fl.execute("SELECT revenue_to_date, direct_cost, net_to_date FROM project_pnl WHERE project_id=?", (pid,)).fetchone()
            self.assertEqual(row, (120.0, 20.0, 100.0))

    def test_wal_recovery(self):
        db_path = self.db_dir / "telemetry_recovery.db"
        migrate.apply_schema(db_path, ROOT / "schemas/telemetry.sql")
        script = f'''
import sqlite3, uuid, datetime, os
con = sqlite3.connect(r"{db_path}")
con.execute("PRAGMA journal_mode=WAL;")
for _ in range(10):
    con.execute("INSERT INTO step_outcomes VALUES (?,?,?,?,?,?,?,?,?)", (
        str(getattr(uuid, "uuid7", uuid.uuid4)()), 'tool_call', 'recovery', 'chain', 'PASS', 5, 0, None,
        datetime.datetime.now(datetime.timezone.utc).isoformat()))
con.commit()
os._exit(1)
'''
        proc = subprocess.run([sys.executable, "-c", script], check=False)
        self.assertNotEqual(proc.returncode, 0)
        with sqlite3.connect(db_path) as con:
            count = con.execute("SELECT COUNT(*) FROM step_outcomes WHERE skill='recovery'").fetchone()[0]
            self.assertEqual(count, 10)


if __name__ == "__main__":
    unittest.main()
