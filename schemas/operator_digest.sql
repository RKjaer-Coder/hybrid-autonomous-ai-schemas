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
  channel TEXT NOT NULL CHECK (channel IN ('CLI', 'mission_control', 'hermes_dashboard', 'telegram', 'discord', 'slack')),
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

CREATE TABLE IF NOT EXISTS operator_project_preferences (
  project_id TEXT PRIMARY KEY,
  priority TEXT NOT NULL CHECK (priority IN ('P0_IMMEDIATE','P1_HIGH','P2_NORMAL','P3_BACKGROUND')),
  focus_note TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_operator_project_preferences_priority_updated
  ON operator_project_preferences(priority, updated_at);

CREATE TABLE IF NOT EXISTS operator_manual_tasks (
  task_id TEXT PRIMARY KEY,
  project_id TEXT,
  title TEXT NOT NULL,
  details TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL CHECK (status IN ('TODO','IN_PROGRESS','BLOCKED','DONE')),
  priority TEXT NOT NULL CHECK (priority IN ('P0_IMMEDIATE','P1_HIGH','P2_NORMAL','P3_BACKGROUND')),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  completed_at TEXT
) STRICT;

CREATE INDEX IF NOT EXISTS idx_operator_manual_tasks_status_priority
  ON operator_manual_tasks(status, priority);
CREATE INDEX IF NOT EXISTS idx_operator_manual_tasks_project_status
  ON operator_manual_tasks(project_id, status);

CREATE TABLE IF NOT EXISTS kernel_migration_readiness_projection (
  surface_ref TEXT PRIMARY KEY,
  record_id TEXT NOT NULL,
  component_type TEXT NOT NULL CHECK (component_type IN ('module', 'database', 'runtime_path', 'schema', 'artifact')),
  ownership_action TEXT NOT NULL CHECK (ownership_action IN ('adopt', 'adapt', 'wrap', 'convert-to-projection', 'retire')),
  owner_domain TEXT NOT NULL,
  summary TEXT NOT NULL,
  blockers_json TEXT NOT NULL CHECK (json_valid(blockers_json)),
  evidence_refs_json TEXT NOT NULL CHECK (json_valid(evidence_refs_json)),
  next_operator_actions_json TEXT NOT NULL CHECK (json_valid(next_operator_actions_json)),
  readiness_status TEXT NOT NULL CHECK (readiness_status IN ('ready', 'action_required', 'blocked', 'retired')),
  live_controls_enabled INTEGER NOT NULL DEFAULT 0 CHECK (live_controls_enabled = 0),
  authoritative_source TEXT NOT NULL DEFAULT 'kernel.events' CHECK (authoritative_source = 'kernel.events'),
  projection_event_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  projected_at TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_kernel_migration_projection_action_status
  ON kernel_migration_readiness_projection(ownership_action, readiness_status, component_type);

CREATE TABLE IF NOT EXISTS runtime_control_state (
  state_id TEXT PRIMARY KEY CHECK (state_id = 'runtime'),
  lifecycle_state TEXT NOT NULL CHECK (lifecycle_state IN ('ACTIVE', 'HALTED')),
  active_halt_id TEXT,
  last_halt_reason TEXT,
  last_transition_at TEXT NOT NULL,
  last_restart_id TEXT
) STRICT;

CREATE TABLE IF NOT EXISTS runtime_halt_events (
  halt_id TEXT PRIMARY KEY,
  halt_scope TEXT NOT NULL CHECK (halt_scope IN ('FULL_SYSTEM_HALT')),
  source TEXT NOT NULL CHECK (source IN ('JUDGE_DEADLOCK', 'SECURITY_CASCADE', 'MANUAL_TEST')),
  trigger_event_id TEXT,
  halt_reason TEXT NOT NULL,
  requires_human INTEGER NOT NULL CHECK (requires_human IN (0, 1)),
  created_at TEXT NOT NULL,
  cleared_at TEXT,
  clear_reason TEXT,
  clear_restart_id TEXT,
  status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'CLEARED'))
) STRICT;

CREATE TABLE IF NOT EXISTS runtime_restart_history (
  restart_id TEXT PRIMARY KEY,
  halt_id TEXT NOT NULL,
  requested_at TEXT NOT NULL,
  completed_at TEXT,
  status TEXT NOT NULL CHECK (status IN ('BLOCKED', 'COMPLETED')),
  restart_reason TEXT NOT NULL,
  preflight_json TEXT NOT NULL CHECK (json_valid(preflight_json)),
  notes TEXT,
  FOREIGN KEY (halt_id) REFERENCES runtime_halt_events(halt_id)
) STRICT;

CREATE INDEX IF NOT EXISTS idx_runtime_halt_events_status_created ON runtime_halt_events(status, created_at);
CREATE INDEX IF NOT EXISTS idx_runtime_halt_events_source_created ON runtime_halt_events(source, created_at);
CREATE INDEX IF NOT EXISTS idx_runtime_restart_history_status_requested ON runtime_restart_history(status, requested_at);
CREATE INDEX IF NOT EXISTS idx_runtime_restart_history_halt_requested ON runtime_restart_history(halt_id, requested_at);

CREATE INDEX IF NOT EXISTS idx_digest_history_created_at ON digest_history(created_at);
CREATE INDEX IF NOT EXISTS idx_harvest_requests_created_at ON harvest_requests(created_at);
