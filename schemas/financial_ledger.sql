PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

-- Valid transitions (application enforced):
-- PIPELINE -> ACTIVE
-- ACTIVE -> PAUSED | KILL_RECOMMENDED | COMPLETE
-- PAUSED -> ACTIVE | KILL_RECOMMENDED
-- KILL_RECOMMENDED -> KILLED | ACTIVE | PAUSED
CREATE TABLE IF NOT EXISTS projects (
  project_id TEXT PRIMARY KEY,
  opportunity_id TEXT NOT NULL,
  name TEXT NOT NULL,
  income_mechanism TEXT NOT NULL CHECK (income_mechanism IN ('software_product','client_work','market_opportunity','ip_asset')),
  thesis TEXT NOT NULL,
  success_criteria TEXT NOT NULL CHECK (json_valid(success_criteria)),
  compute_budget TEXT NOT NULL CHECK (json_valid(compute_budget)),
  portfolio_weight REAL NOT NULL CHECK (portfolio_weight >= 0.0 AND portfolio_weight <= 1.0),
  status TEXT NOT NULL CHECK (status IN ('PIPELINE','ACTIVE','PAUSED','KILL_RECOMMENDED','COMPLETE','KILLED')),
  kill_score_watch INTEGER DEFAULT 0 CHECK (kill_score_watch IN (0, 1)),
  cashflow_actual_usd REAL DEFAULT 0.00,
  council_verdict_id TEXT,
  pivot_log TEXT DEFAULT '[]' CHECK (json_valid(pivot_log)),
  created_at TEXT NOT NULL,
  closed_at TEXT
) STRICT;

CREATE INDEX IF NOT EXISTS idx_projects_status ON projects(status);
CREATE INDEX IF NOT EXISTS idx_projects_opportunity_id ON projects(opportunity_id);
CREATE INDEX IF NOT EXISTS idx_projects_income_mechanism ON projects(income_mechanism);

CREATE TABLE IF NOT EXISTS phases (
  phase_id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(project_id),
  name TEXT NOT NULL CHECK (name IN ('VALIDATE','BUILD','DEPLOY','OPERATE')),
  status TEXT NOT NULL CHECK (status IN ('PENDING','ACTIVE','GATE_PENDING','COMPLETE','KILLED')),
  sequence INTEGER NOT NULL CHECK (sequence BETWEEN 0 AND 3),
  scope TEXT NOT NULL,
  success_criteria TEXT NOT NULL CHECK (json_valid(success_criteria)),
  compute_budget TEXT NOT NULL CHECK (json_valid(compute_budget)),
  compute_consumed TEXT NOT NULL DEFAULT '{"executor_hours":0,"cloud_spend_usd":0}' CHECK (json_valid(compute_consumed)),
  outputs TEXT DEFAULT '[]' CHECK (json_valid(outputs)),
  gate_result TEXT CHECK (gate_result IS NULL OR json_valid(gate_result)),
  started_at TEXT,
  gate_triggered_at TEXT,
  completed_at TEXT
) STRICT;

CREATE INDEX IF NOT EXISTS idx_phases_project_sequence ON phases(project_id, sequence);
CREATE INDEX IF NOT EXISTS idx_phases_status ON phases(status);

CREATE TABLE IF NOT EXISTS kill_signals (
  signal_id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(project_id),
  signal_type TEXT NOT NULL CHECK (signal_type IN ('cashflow_vs_forecast','council_confidence','market_invalidation','technical_blocker','asset_creation')),
  weight REAL NOT NULL CHECK (weight >= 0.05 AND weight <= 0.35),
  raw_score REAL NOT NULL CHECK (raw_score IN (0.0, 0.5, 1.0)),
  evidence TEXT NOT NULL,
  created_at TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_kill_signals_project_created ON kill_signals(project_id, created_at);

CREATE TABLE IF NOT EXISTS kill_recommendations (
  recommendation_id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(project_id),
  kill_score REAL NOT NULL,
  council_verdict_id TEXT NOT NULL,
  asset_inventory TEXT DEFAULT '[]' CHECK (json_valid(asset_inventory)),
  thesis_summary TEXT NOT NULL,
  failure_analysis TEXT NOT NULL,
  g2_status TEXT NOT NULL CHECK (g2_status IN ('PENDING','CONFIRMED','OVERRIDDEN','TIMEOUT')),
  threshold_status TEXT DEFAULT 'PROVISIONAL' CHECK (threshold_status IN ('PROVISIONAL','VALIDATED')),
  created_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS assets (
  asset_id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(project_id),
  asset_type TEXT NOT NULL CHECK (asset_type IN ('skill','dataset','model_adapter','tool','template','documentation','codebase','deployment_config')),
  name TEXT NOT NULL,
  description TEXT NOT NULL,
  reusable INTEGER NOT NULL CHECK (reusable IN (0, 1)),
  location TEXT NOT NULL,
  created_at TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_assets_project_id ON assets(project_id);
CREATE INDEX IF NOT EXISTS idx_assets_reusable ON assets(reusable);

CREATE TABLE IF NOT EXISTS revenue_records (
  record_id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL REFERENCES projects(project_id),
  amount_usd REAL NOT NULL,
  source_type TEXT NOT NULL CHECK (source_type IN ('app_store','web_store','saas_billing','client_invoice','licensing','marketplace')),
  attribution_method TEXT NOT NULL CHECK (attribution_method IN ('automated','operator_reported')),
  period_start TEXT NOT NULL,
  period_end TEXT NOT NULL,
  created_at TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_revenue_records_project_period_start ON revenue_records(project_id, period_start);

CREATE TABLE IF NOT EXISTS cost_records (
  record_id TEXT PRIMARY KEY,
  project_id TEXT REFERENCES projects(project_id),
  cost_category TEXT NOT NULL CHECK (cost_category IN ('cloud_api','infrastructure','services','compute_local')),
  amount_usd REAL NOT NULL,
  description TEXT NOT NULL,
  provider TEXT,
  task_id TEXT,
  created_at TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_cost_records_project_created ON cost_records(project_id, created_at);
CREATE INDEX IF NOT EXISTS idx_cost_records_cost_category ON cost_records(cost_category);

CREATE TABLE IF NOT EXISTS treasury (
  entry_id TEXT PRIMARY KEY,
  entry_type TEXT NOT NULL CHECK (entry_type IN ('revenue_in','cost_out','reinvestment','injection','validation_reserve')),
  amount_usd REAL NOT NULL,
  balance_after REAL NOT NULL,
  reference_id TEXT,
  description TEXT NOT NULL,
  created_at TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_treasury_created_at ON treasury(created_at);
CREATE INDEX IF NOT EXISTS idx_treasury_entry_type ON treasury(entry_type);

CREATE TABLE IF NOT EXISTS routing_decisions (
  decision_id TEXT PRIMARY KEY,
  task_id TEXT,
  chain_id TEXT,
  role TEXT NOT NULL CHECK (role IN ('Primary Reasoning','Execution','Validation','Training/Reward','Embedding','Cloud Escalation')),
  route_selected TEXT NOT NULL CHECK (route_selected IN ('local','free_cloud','subscription','paid_cloud','operator_prompted')),
  model_used TEXT,
  commercial_use_ok INTEGER NOT NULL CHECK (commercial_use_ok IN (0, 1)),
  quality_warning INTEGER DEFAULT 0 CHECK (quality_warning IN (0, 1)),
  cost_usd REAL DEFAULT 0.00,
  justification TEXT,
  g3_required INTEGER DEFAULT 0 CHECK (g3_required IN (0, 1)),
  g3_status TEXT CHECK (g3_status IS NULL OR g3_status IN ('APPROVED','BLOCKED','EXPIRED')),
  created_at TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_routing_decisions_role_created ON routing_decisions(role, created_at);
CREATE INDEX IF NOT EXISTS idx_routing_decisions_route_selected ON routing_decisions(route_selected);

CREATE VIEW IF NOT EXISTS project_pnl AS
SELECT
  p.project_id,
  p.name,
  COALESCE((SELECT SUM(r.amount_usd) FROM revenue_records r WHERE r.project_id = p.project_id), 0.0) AS revenue_to_date,
  COALESCE((SELECT SUM(c.amount_usd) FROM cost_records c WHERE c.project_id = p.project_id AND c.cost_category = 'cloud_api'), 0.0) AS direct_cost,
  COALESCE((SELECT SUM(r.amount_usd) FROM revenue_records r WHERE r.project_id = p.project_id), 0.0)
    - COALESCE((SELECT SUM(c.amount_usd) FROM cost_records c WHERE c.project_id = p.project_id AND c.cost_category = 'cloud_api'), 0.0)
    AS net_to_date
FROM projects p
WHERE p.status = 'ACTIVE';

CREATE INDEX IF NOT EXISTS idx_projects_created_at ON projects(created_at);
CREATE INDEX IF NOT EXISTS idx_revenue_records_created_at ON revenue_records(created_at);
CREATE INDEX IF NOT EXISTS idx_kill_recommendations_created_at ON kill_recommendations(created_at);
