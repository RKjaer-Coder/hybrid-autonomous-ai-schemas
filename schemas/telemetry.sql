PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS step_outcomes (
  event_id TEXT PRIMARY KEY,
  step_type TEXT NOT NULL,
  skill TEXT NOT NULL,
  chain_id TEXT NOT NULL,
  outcome TEXT NOT NULL CHECK (outcome IN ('PASS', 'FAIL', 'DEGRADED')),
  latency_ms INTEGER NOT NULL,
  quality_warning INTEGER DEFAULT 0 CHECK (quality_warning IN (0, 1)),
  recovery_tier INTEGER CHECK (recovery_tier IS NULL OR recovery_tier BETWEEN 1 AND 5),
  timestamp TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_step_outcomes_step_skill_timestamp ON step_outcomes(step_type, skill, timestamp);
CREATE INDEX IF NOT EXISTS idx_step_outcomes_chain_id ON step_outcomes(chain_id);
CREATE INDEX IF NOT EXISTS idx_step_outcomes_outcome_timestamp ON step_outcomes(outcome, timestamp);
CREATE INDEX IF NOT EXISTS idx_step_outcomes_skill_timestamp ON step_outcomes(skill, timestamp);

CREATE TABLE IF NOT EXISTS chain_definitions (
  chain_type TEXT PRIMARY KEY,
  steps TEXT NOT NULL CHECK (json_valid(steps)),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
) STRICT;

-- Rolling reliability by step_type x skill over 7d and 30d windows.
CREATE VIEW IF NOT EXISTS reliability_by_step AS
SELECT
  step_type,
  skill,
  SUM(CASE WHEN timestamp >= datetime('now', '-7 days') THEN
    CASE outcome WHEN 'PASS' THEN 1.0 WHEN 'DEGRADED' THEN 0.5 ELSE 0.0 END
  ELSE 0.0 END)
  /
  NULLIF(SUM(CASE WHEN timestamp >= datetime('now', '-7 days') THEN 1 ELSE 0 END), 0) AS reliability_7d,
  SUM(CASE WHEN timestamp >= datetime('now', '-30 days') THEN
    CASE outcome WHEN 'PASS' THEN 1.0 WHEN 'DEGRADED' THEN 0.5 ELSE 0.0 END
  ELSE 0.0 END)
  /
  NULLIF(SUM(CASE WHEN timestamp >= datetime('now', '-30 days') THEN 1 ELSE 0 END), 0) AS reliability_30d
FROM step_outcomes
GROUP BY step_type, skill;

-- Product of per-step reliabilities per chain_type based on chain_definitions steps JSON.
CREATE VIEW IF NOT EXISTS chain_reliability AS
SELECT
  cd.chain_type,
  exp(SUM(CASE
      WHEN r.reliability_7d IS NULL OR r.reliability_7d <= 0 THEN NULL
      ELSE ln(r.reliability_7d)
  END)) AS chain_reliability_7d,
  exp(SUM(CASE
      WHEN r.reliability_30d IS NULL OR r.reliability_30d <= 0 THEN NULL
      ELSE ln(r.reliability_30d)
  END)) AS chain_reliability_30d
FROM chain_definitions cd
JOIN json_each(cd.steps) s
LEFT JOIN reliability_by_step r
  ON r.step_type = json_extract(s.value, '$.step_type')
 AND r.skill = json_extract(s.value, '$.skill')
GROUP BY cd.chain_type;

CREATE INDEX IF NOT EXISTS idx_chain_definitions_created_at ON chain_definitions(created_at);
