PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS digest_history (
  digest_id TEXT PRIMARY KEY,
  digest_type TEXT NOT NULL CHECK (digest_type IN ('daily', 'catch_up', 'critical_only')),
  content TEXT NOT NULL,
  sections_included TEXT NOT NULL CHECK (json_valid(sections_included)),
  word_count INTEGER NOT NULL,
  operator_state TEXT NOT NULL CHECK (operator_state IN ('ACTIVE', 'CONSERVATIVE', 'ABSENT')),
  delivered_at TEXT,
  acknowledged_at TEXT,
  created_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS alert_log (
  alert_id TEXT PRIMARY KEY,
  tier TEXT NOT NULL CHECK (tier IN ('T1', 'T2', 'T3')),
  alert_type TEXT NOT NULL,
  content TEXT NOT NULL,
  channel_delivered TEXT,
  suppressed INTEGER DEFAULT 0 CHECK (suppressed IN (0, 1)),
  acknowledged INTEGER DEFAULT 0 CHECK (acknowledged IN (0, 1)),
  acknowledged_at TEXT,
  created_at TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_alert_log_tier_created ON alert_log(tier, created_at);
CREATE INDEX IF NOT EXISTS idx_alert_log_type_created ON alert_log(alert_type, created_at);

CREATE TABLE IF NOT EXISTS harvest_requests (
  harvest_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  prompt_text TEXT NOT NULL,
  target_interface TEXT NOT NULL,
  context_summary TEXT NOT NULL,
  priority TEXT NOT NULL CHECK (priority IN ('P0_IMMEDIATE','P1_HIGH','P2_NORMAL','P3_BACKGROUND')),
  status TEXT NOT NULL CHECK (status IN ('PENDING','DELIVERED','DELIVERED_PARTIAL','EXPIRED','CANCELLED')),
  expires_at TEXT NOT NULL,
  operator_result TEXT,
  relevance_score REAL CHECK (relevance_score IS NULL OR (relevance_score >= 0.0 AND relevance_score <= 1.0)),
  clarification_sent INTEGER DEFAULT 0 CHECK (clarification_sent IN (0, 1)),
  created_at TEXT NOT NULL,
  delivered_at TEXT
) STRICT;

CREATE INDEX IF NOT EXISTS idx_harvest_requests_status_expires ON harvest_requests(status, expires_at);
CREATE INDEX IF NOT EXISTS idx_harvest_requests_priority_status ON harvest_requests(priority, status);
CREATE INDEX IF NOT EXISTS idx_harvest_requests_task_id ON harvest_requests(task_id);

CREATE TABLE IF NOT EXISTS gate_log (
  gate_id TEXT PRIMARY KEY,
  gate_type TEXT NOT NULL CHECK (gate_type IN ('G1', 'G2', 'G3', 'G4')),
  trigger_description TEXT NOT NULL,
  context_packet TEXT NOT NULL CHECK (json_valid(context_packet)),
  project_id TEXT,
  status TEXT NOT NULL CHECK (status IN ('PENDING','APPROVED','REJECTED','BLOCKED','EXPIRED','SUSPENDED','FROZEN')),
  timeout_hours REAL NOT NULL,
  operator_response TEXT,
  created_at TEXT NOT NULL,
  responded_at TEXT,
  expires_at TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_gate_log_status ON gate_log(status);
CREATE INDEX IF NOT EXISTS idx_gate_log_type_created ON gate_log(gate_type, created_at);
CREATE INDEX IF NOT EXISTS idx_gate_log_project_id ON gate_log(project_id);

CREATE TABLE IF NOT EXISTS operator_heartbeat (
  entry_id TEXT PRIMARY KEY,
  interaction_type TEXT NOT NULL CHECK (interaction_type IN ('message', 'gate_response', 'digest_ack', 'command')),
  channel TEXT NOT NULL CHECK (channel IN ('CLI', 'mission_control', 'telegram', 'discord', 'slack')),
  timestamp TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_operator_heartbeat_timestamp_desc ON operator_heartbeat(timestamp DESC);

CREATE TABLE IF NOT EXISTS operator_load_tracking (
  entry_id TEXT PRIMARY KEY,
  week_start TEXT NOT NULL,
  gates_surfaced TEXT NOT NULL CHECK (json_valid(gates_surfaced)),
  harvests_created INTEGER NOT NULL,
  harvests_completed INTEGER NOT NULL,
  harvests_expired INTEGER NOT NULL,
  estimated_hours REAL NOT NULL,
  overload_triggered INTEGER DEFAULT 0 CHECK (overload_triggered IN (0, 1)),
  created_at TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_operator_load_tracking_week_start ON operator_load_tracking(week_start);

CREATE INDEX IF NOT EXISTS idx_digest_history_created_at ON digest_history(created_at);
CREATE INDEX IF NOT EXISTS idx_harvest_requests_created_at ON harvest_requests(created_at);
