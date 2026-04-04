# Hybrid Autonomous AI SQLite Schema Suite

This repository defines a five-database SQLite schema bundle for the Hermes Agent architecture.

## Databases

## 1) `strategic_memory.db`
Purpose: institutional memory with provenance-aware strategic artifacts.

Tables:
- `opportunity_records`
- `council_verdicts`
- `brief_quality_signals`
- `calibration_records`
- `intelligence_briefs`
- `research_tasks`
- `idea_records`
- `market_signals`
- `capability_gaps`
- `source_reputations`
- `dedup_records`
- `deferred_research_entries`
- `model_scout_reports`
- `model_assess_reports`
- `shadow_trial_reports`

Common query patterns:
```sql
-- Active opportunities by status
SELECT opportunity_id, title FROM opportunity_records WHERE status = 'ACTIVE';

-- 30-day dedup window
SELECT *
FROM opportunity_records
WHERE income_mechanism = 'software_product'
  AND created_at >= datetime('now', '-30 days');

-- Task scheduling queue
SELECT *
FROM research_tasks
WHERE domain = 1 AND priority = 'P1_HIGH' AND status = 'PENDING';
```

## 2) `telemetry.db`
Purpose: reliability telemetry for March-of-Nines tracking.

Tables:
- `step_outcomes`
- `chain_definitions`

Views:
- `reliability_by_step`
- `chain_reliability`

Common query patterns:
```sql
SELECT *
FROM reliability_by_step
WHERE step_type = 'tool_call' AND skill = 'executor';

SELECT * FROM chain_reliability WHERE chain_type = 'council_tier1';
```

## 3) `immune_system.db`
Purpose: security verdicts, alerting, and circuit-breaker logging.

Tables:
- `immune_verdicts`
- `security_alerts`
- `circuit_breaker_log`
- `jwt_revocation_log`
- `skill_improvement_log`

Common query patterns:
```sql
-- Recent block-rate trend by skill
SELECT skill_name, result, timestamp
FROM immune_verdicts
WHERE timestamp >= datetime('now', '-7 days');

-- Alert storm detection window
SELECT * FROM security_alerts WHERE timestamp >= datetime('now', '-60 seconds');
```

## 4) `financial_ledger.db`
Purpose: treasury, cost/revenue attribution, and portfolio/project P&L.

Tables:
- `projects`
- `phases`
- `kill_signals`
- `kill_recommendations`
- `assets`
- `revenue_records`
- `cost_records`
- `treasury`
- `routing_decisions`

Views:
- `project_pnl`

Common query patterns:
```sql
SELECT * FROM project_pnl;

SELECT *
FROM routing_decisions
WHERE role = 'Execution' AND created_at >= datetime('now', '-30 days');
```

## 5) `operator_digest.db`
Purpose: operator-facing digest, gates, alerts, and load tracking data.

Tables:
- `digest_history`
- `alert_log`
- `harvest_requests`
- `gate_log`
- `operator_heartbeat`
- `operator_load_tracking`

Common query patterns:
```sql
SELECT * FROM gate_log WHERE status = 'PENDING';

SELECT *
FROM alert_log
WHERE alert_type = 'SECURITY_CASCADE'
  AND created_at >= datetime('now', '-15 minutes');
```

## Migration runner

Run migrations:
```bash
python migrate.py --db-dir ./data --verify
```

Behavior:
- Creates DB directory if needed.
- Creates/opens all 5 DB files.
- Enables WAL mode and foreign keys.
- Executes each SQL schema file.
- Optional `--verify` checks required tables/indexes and WAL mode.

## Testing

Run tests:
```bash
python -m unittest discover -s tests -v
```

Test coverage includes:
- round-trip writes (50+ records across tables)
- constraint validation errors
- JSON CHECK validation behavior
- index presence checks
- WAL mode checks
- WAL crash recovery behavior
- computed view correctness checks
