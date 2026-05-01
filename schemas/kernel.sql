-- v3.1 foundation kernel authority schema.
-- Existing domain schemas remain legacy projections unless future migrations
-- explicitly promote them under kernel command/event authority.

CREATE TABLE IF NOT EXISTS commands (
  command_id TEXT PRIMARY KEY,
  command_type TEXT NOT NULL,
  requested_by TEXT NOT NULL CHECK (requested_by IN ('operator','kernel','scheduler','agent','tool','model')),
  requester_id TEXT NOT NULL,
  target_entity_type TEXT NOT NULL,
  target_entity_id TEXT,
  requested_authority TEXT CHECK (requested_authority IS NULL OR requested_authority IN ('rule','single_agent','council','operator_gate')),
  payload_hash TEXT NOT NULL,
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
  idempotency_key TEXT NOT NULL UNIQUE,
  submitted_at TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('accepted','applied','rejected')),
  result_event_id TEXT
) STRICT;

CREATE TABLE IF NOT EXISTS events (
  event_seq INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL UNIQUE,
  event_schema_version INTEGER NOT NULL,
  event_type TEXT NOT NULL,
  entity_type TEXT NOT NULL CHECK (entity_type IN ('task','research_request','decision','project','model','budget','gate','capability','side_effect','policy','artifact')),
  entity_id TEXT NOT NULL,
  transaction_id TEXT NOT NULL,
  command_id TEXT REFERENCES commands(command_id),
  correlation_id TEXT,
  causation_event_id TEXT,
  actor_type TEXT NOT NULL CHECK (actor_type IN ('kernel','operator','agent','tool','model','scheduler')),
  actor_id TEXT NOT NULL,
  timestamp TEXT NOT NULL,
  policy_version TEXT NOT NULL,
  data_class TEXT NOT NULL CHECK (data_class IN ('public','internal','sensitive','secret_ref','regulated','client_confidential')),
  payload_hash TEXT NOT NULL,
  payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
  prev_event_hash TEXT,
  event_hash TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS capability_grants (
  grant_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  subject_type TEXT NOT NULL CHECK (subject_type IN ('agent','tool','model','adapter')),
  subject_id TEXT NOT NULL,
  capability_type TEXT NOT NULL CHECK (capability_type IN ('model','tool','file','network','spend','memory_write','side_effect')),
  actions_json TEXT NOT NULL CHECK (json_valid(actions_json)),
  resource_json TEXT NOT NULL CHECK (json_valid(resource_json)),
  scope_json TEXT NOT NULL CHECK (json_valid(scope_json)),
  conditions_json TEXT NOT NULL CHECK (json_valid(conditions_json)),
  issued_at TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  max_uses INTEGER,
  used_count INTEGER NOT NULL DEFAULT 0 CHECK (used_count >= 0),
  issuer TEXT NOT NULL CHECK (issuer = 'kernel'),
  policy_version TEXT NOT NULL,
  revalidate_on_use INTEGER NOT NULL CHECK (revalidate_on_use IN (0, 1)),
  status TEXT NOT NULL CHECK (status IN ('active','exhausted','revoked','expired'))
) STRICT;

CREATE TABLE IF NOT EXISTS budgets (
  budget_id TEXT PRIMARY KEY,
  owner_type TEXT NOT NULL CHECK (owner_type IN ('project','research_profile','system_maintenance')),
  owner_id TEXT NOT NULL,
  approved_by TEXT NOT NULL CHECK (approved_by = 'operator'),
  cap_usd TEXT NOT NULL,
  spent_usd TEXT NOT NULL,
  reserved_usd TEXT NOT NULL,
  expires_at TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('active','exhausted','expired','revoked'))
) STRICT;

CREATE TABLE IF NOT EXISTS budget_reservations (
  reservation_id TEXT PRIMARY KEY,
  budget_id TEXT NOT NULL REFERENCES budgets(budget_id),
  command_id TEXT NOT NULL REFERENCES commands(command_id),
  amount_usd TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('reserved','settled','released','incident')),
  created_at TEXT NOT NULL,
  closed_at TEXT
) STRICT;

CREATE TABLE IF NOT EXISTS artifact_refs (
  artifact_id TEXT PRIMARY KEY,
  artifact_uri TEXT NOT NULL,
  data_class TEXT NOT NULL CHECK (data_class IN ('public','internal','sensitive','secret_ref','regulated','client_confidential')),
  content_hash TEXT NOT NULL,
  retention_policy TEXT NOT NULL,
  deletion_policy TEXT NOT NULL,
  encryption_status TEXT NOT NULL CHECK (encryption_status IN ('unencrypted','encrypted','quarantined','deleted')),
  source_notes TEXT,
  created_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS side_effect_intents (
  intent_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  side_effect_type TEXT NOT NULL CHECK (side_effect_type IN ('message','publish','deploy','purchase','provider_call','account_change','financial','legal','other')),
  target_json TEXT NOT NULL CHECK (json_valid(target_json)),
  payload_hash TEXT NOT NULL,
  required_authority TEXT NOT NULL CHECK (required_authority IN ('rule','single_agent','council','operator_gate')),
  grant_id TEXT NOT NULL REFERENCES capability_grants(grant_id),
  timeout_policy TEXT NOT NULL CHECK (timeout_policy IN ('deny','pause','compensate','ask_operator')),
  status TEXT NOT NULL CHECK (status IN ('prepared','executed','failed','cancelled','compensation_needed'))
) STRICT;

CREATE TABLE IF NOT EXISTS side_effect_receipts (
  receipt_id TEXT PRIMARY KEY,
  intent_id TEXT NOT NULL REFERENCES side_effect_intents(intent_id),
  receipt_type TEXT NOT NULL CHECK (receipt_type IN ('success','failure','timeout','cancellation','compensation_needed')),
  receipt_hash TEXT NOT NULL,
  details_json TEXT NOT NULL CHECK (json_valid(details_json)),
  recorded_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS projection_outbox (
  outbox_id TEXT PRIMARY KEY,
  event_id TEXT NOT NULL REFERENCES events(event_id),
  projection_name TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('pending','complete','failed','halted')),
  created_at TEXT NOT NULL,
  completed_at TEXT,
  error TEXT
) STRICT;

CREATE INDEX IF NOT EXISTS idx_commands_idempotency_key ON commands(idempotency_key);
CREATE INDEX IF NOT EXISTS idx_events_entity ON events(entity_type, entity_id, event_seq);
CREATE INDEX IF NOT EXISTS idx_events_command ON events(command_id);
CREATE INDEX IF NOT EXISTS idx_capability_grants_subject ON capability_grants(subject_type, subject_id, capability_type, status);
CREATE INDEX IF NOT EXISTS idx_budgets_owner ON budgets(owner_type, owner_id, status);
CREATE INDEX IF NOT EXISTS idx_budget_reservations_budget_status ON budget_reservations(budget_id, status);
CREATE INDEX IF NOT EXISTS idx_artifact_refs_data_class ON artifact_refs(data_class, created_at);
CREATE INDEX IF NOT EXISTS idx_side_effect_intents_status ON side_effect_intents(status, side_effect_type);
CREATE INDEX IF NOT EXISTS idx_side_effect_receipts_intent ON side_effect_receipts(intent_id, recorded_at);
CREATE INDEX IF NOT EXISTS idx_projection_outbox_status ON projection_outbox(status, created_at);
