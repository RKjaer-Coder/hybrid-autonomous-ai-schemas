PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS immune_verdicts (
  verdict_id TEXT PRIMARY KEY,
  verdict_type TEXT NOT NULL CHECK (verdict_type IN ('sheriff_input', 'judge_output')),
  scan_tier TEXT NOT NULL CHECK (scan_tier IN ('fast_path', 'deep_scan')),
  session_id TEXT NOT NULL,
  skill_name TEXT NOT NULL,
  result TEXT NOT NULL CHECK (result IN ('PASS', 'BLOCK', 'INCONCLUSIVE', 'TIMEOUT')),
  match_pattern TEXT,
  latency_ms INTEGER NOT NULL,
  timestamp TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_immune_verdicts_skill_timestamp ON immune_verdicts(skill_name, timestamp);
CREATE INDEX IF NOT EXISTS idx_immune_verdicts_result_timestamp ON immune_verdicts(result, timestamp);
CREATE INDEX IF NOT EXISTS idx_immune_verdicts_session_id ON immune_verdicts(session_id);

CREATE TABLE IF NOT EXISTS security_alerts (
  alert_id TEXT PRIMARY KEY,
  source TEXT NOT NULL CHECK (source IN (
    'sheriff_ipi','judge_policy','memory_provenance','memory_integrity','jwt_failure','immune_timeout_sheriff','immune_timeout_judge',
    'gate_unverified','skill_bypass','burner_escape','training_violation'
  )),
  severity TEXT NOT NULL CHECK (severity IN ('WARNING', 'ALERT', 'CASCADE')),
  details TEXT NOT NULL,
  session_id TEXT,
  resolved INTEGER DEFAULT 0 CHECK (resolved IN (0, 1)),
  resolved_at TEXT,
  timestamp TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_security_alerts_timestamp ON security_alerts(timestamp);

CREATE TABLE IF NOT EXISTS circuit_breaker_log (
  event_id TEXT PRIMARY KEY,
  breaker_name TEXT NOT NULL CHECK (breaker_name IN (
    'INFINITE_LOOP','BUDGET_HARD_CAP','CONTEXT_OVERFLOW','TOOL_FAILURE_STORM','EXECUTOR_SATURATION','TOOL_QUARANTINE',
    'RELIABILITY_DEGRADED','RELIABILITY_CRITICAL','MEMORY_WRITE_STORM','DEAD_MAN_S_SWITCH','SECURITY_CASCADE'
  )),
  state TEXT NOT NULL CHECK (state IN ('ARMED', 'TRIPPED', 'COOLDOWN', 'RESET')),
  trip_condition TEXT NOT NULL,
  action_taken TEXT NOT NULL,
  requires_human INTEGER NOT NULL CHECK (requires_human IN (0, 1)),
  auto_reset_at TEXT,
  timestamp TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_circuit_breaker_name_timestamp ON circuit_breaker_log(breaker_name, timestamp);
CREATE INDEX IF NOT EXISTS idx_circuit_breaker_state ON circuit_breaker_log(state);

CREATE TABLE IF NOT EXISTS jwt_revocation_log (
  jti TEXT PRIMARY KEY,
  reason TEXT NOT NULL CHECK (reason IN ('ttl_expiry', 'circuit_breaker', 'budget_cap', 'human_revoke')),
  revoked_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS skill_improvement_log (
  improvement_id TEXT PRIMARY KEY,
  skill_name TEXT NOT NULL,
  before_hash TEXT NOT NULL,
  after_hash TEXT NOT NULL,
  diff_summary TEXT NOT NULL,
  trigger_task_id TEXT,
  improvement_rationale TEXT NOT NULL,
  probation_status TEXT DEFAULT 'ACTIVE' CHECK (probation_status IN ('ACTIVE', 'PASSED', 'ROLLED_BACK')),
  probation_block_rate REAL,
  emergency_override INTEGER DEFAULT 0 CHECK (emergency_override IN (0, 1)),
  timestamp TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_circuit_breaker_log_timestamp ON circuit_breaker_log(timestamp);
CREATE INDEX IF NOT EXISTS idx_skill_improvement_log_timestamp ON skill_improvement_log(timestamp);
