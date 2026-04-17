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
  timestamp TEXT NOT NULL,
  judge_mode TEXT NOT NULL DEFAULT 'NOT_APPLICABLE' CHECK (judge_mode IN ('NOT_APPLICABLE', 'NORMAL', 'FALLBACK'))
) STRICT;

CREATE INDEX IF NOT EXISTS idx_immune_verdicts_skill_timestamp ON immune_verdicts(skill_name, timestamp);
CREATE INDEX IF NOT EXISTS idx_immune_verdicts_result_timestamp ON immune_verdicts(result, timestamp);
CREATE INDEX IF NOT EXISTS idx_immune_verdicts_session_id ON immune_verdicts(session_id);
CREATE INDEX IF NOT EXISTS idx_immune_verdicts_judge_mode_timestamp ON immune_verdicts(judge_mode, timestamp);

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
    'RELIABILITY_DEGRADED','RELIABILITY_CRITICAL','MEMORY_WRITE_STORM','DEAD_MAN_S_SWITCH','SECURITY_CASCADE',
    'JUDGE_DEADLOCK'
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

CREATE TABLE IF NOT EXISTS compound_breaker_events (
  event_id TEXT PRIMARY KEY,
  breaker_names TEXT NOT NULL CHECK (json_valid(breaker_names)),
  winner_tier TEXT NOT NULL CHECK (winner_tier IN ('S', 'H', 'D', 'R')),
  winning_action TEXT NOT NULL,
  applied_actions TEXT NOT NULL CHECK (json_valid(applied_actions)),
  suppressed_actions TEXT NOT NULL CHECK (json_valid(suppressed_actions)),
  requires_human INTEGER NOT NULL CHECK (requires_human IN (0, 1)),
  window_seconds INTEGER NOT NULL CHECK (window_seconds > 0),
  window_started_at TEXT NOT NULL,
  window_ended_at TEXT NOT NULL,
  resolution_notes TEXT,
  resolved_at TEXT,
  created_at TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_compound_breaker_events_created ON compound_breaker_events(created_at);
CREATE INDEX IF NOT EXISTS idx_compound_breaker_events_winner_tier ON compound_breaker_events(winner_tier);

CREATE TABLE IF NOT EXISTS quarantined_responses (
  quarantine_id TEXT PRIMARY KEY,
  correlation_id TEXT NOT NULL,
  session_id TEXT NOT NULL,
  project_id TEXT,
  task_id TEXT,
  route_decision_id TEXT,
  cost_record_id TEXT,
  reservation_id TEXT,
  source_breaker TEXT NOT NULL CHECK (source_breaker IN ('SECURITY_CASCADE')),
  provider TEXT,
  model_used TEXT,
  payload_format TEXT NOT NULL CHECK (payload_format IN ('json', 'text')),
  payload_text TEXT NOT NULL,
  received_at TEXT NOT NULL,
  quarantined_at TEXT NOT NULL,
  review_status TEXT NOT NULL DEFAULT 'PENDING' CHECK (review_status IN ('PENDING', 'DISCARDED', 'REPROCESS_APPROVED', 'REPROCESSED')),
  operator_decision TEXT CHECK (operator_decision IS NULL OR operator_decision IN ('DISCARD', 'REPROCESS', 'REPROCESSED')),
  review_notes TEXT,
  review_digest_id TEXT,
  reviewed_at TEXT
) STRICT;

CREATE INDEX IF NOT EXISTS idx_quarantined_responses_correlation_id ON quarantined_responses(correlation_id);
CREATE INDEX IF NOT EXISTS idx_quarantined_responses_review_status ON quarantined_responses(review_status, quarantined_at);
CREATE INDEX IF NOT EXISTS idx_quarantined_responses_quarantined_at ON quarantined_responses(quarantined_at);

CREATE TABLE IF NOT EXISTS judge_fallback_events (
  event_id TEXT PRIMARY KEY,
  trigger_source TEXT NOT NULL CHECK (trigger_source IN ('JUDGE_DEADLOCK')),
  status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'HALTED', 'CLEARED')),
  trigger_reason TEXT NOT NULL,
  block_rate REAL NOT NULL CHECK (block_rate >= 0.0 AND block_rate <= 1.0),
  blocked_count INTEGER NOT NULL CHECK (blocked_count >= 0),
  total_count INTEGER NOT NULL CHECK (total_count >= 0),
  distinct_task_types TEXT NOT NULL CHECK (json_valid(distinct_task_types)),
  started_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  acknowledged_at TEXT,
  ended_at TEXT,
  end_reason TEXT,
  prior_event_id TEXT,
  operator_alert_id TEXT
) STRICT;

CREATE INDEX IF NOT EXISTS idx_judge_fallback_events_status_started ON judge_fallback_events(status, started_at);
CREATE INDEX IF NOT EXISTS idx_judge_fallback_events_started_at ON judge_fallback_events(started_at);

CREATE TABLE IF NOT EXISTS judge_fallback_review_queue (
  queue_id TEXT PRIMARY KEY,
  fallback_event_id TEXT NOT NULL,
  source_verdict_id TEXT NOT NULL UNIQUE,
  session_id TEXT NOT NULL,
  skill_name TEXT NOT NULL,
  tool_name TEXT NOT NULL,
  task_type TEXT NOT NULL,
  output_json TEXT NOT NULL CHECK (json_valid(output_json)),
  expected_schema_json TEXT CHECK (expected_schema_json IS NULL OR json_valid(expected_schema_json)),
  max_trust_tier INTEGER NOT NULL,
  memory_write_target TEXT,
  enqueued_at TEXT NOT NULL,
  review_status TEXT NOT NULL CHECK (review_status IN ('PENDING', 'PASS', 'BLOCK')),
  reviewed_at TEXT,
  review_verdict_id TEXT,
  review_outcome TEXT,
  review_reason TEXT,
  FOREIGN KEY (fallback_event_id) REFERENCES judge_fallback_events(event_id)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_judge_fallback_review_status_enqueued ON judge_fallback_review_queue(review_status, enqueued_at);
CREATE INDEX IF NOT EXISTS idx_judge_fallback_review_event_status ON judge_fallback_review_queue(fallback_event_id, review_status);

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
