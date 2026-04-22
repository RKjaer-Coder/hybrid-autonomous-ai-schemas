PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

-- Strategic Memory: institutional memory with typed/provenance decision artifacts.

CREATE TABLE IF NOT EXISTS council_verdicts (
  verdict_id TEXT PRIMARY KEY,
  tier_used INTEGER NOT NULL CHECK (tier_used IN (1, 2)),
  decision_type TEXT NOT NULL CHECK (decision_type IN (
    'opportunity_screen','go_no_go','kill_rec','phase_gate','operator_strategic','system_critical','project_completion'
  )),
  recommendation TEXT NOT NULL CHECK (recommendation IN ('PURSUE','REJECT','PAUSE','ESCALATE','INSUFFICIENT_DATA')),
  confidence REAL NOT NULL CHECK (confidence >= 0.0 AND confidence <= 1.0),
  reasoning_summary TEXT NOT NULL,
  dissenting_views TEXT,
  minority_positions TEXT CHECK (minority_positions IS NULL OR json_valid(minority_positions)),
  full_debate_record TEXT,
  cost_usd REAL DEFAULT 0.00,
  project_id TEXT,
  outcome_record TEXT CHECK (outcome_record IS NULL OR json_valid(outcome_record)),
  da_quality_score REAL CHECK (da_quality_score IS NULL OR (da_quality_score >= 0.0 AND da_quality_score <= 1.0)),
  da_assessment TEXT CHECK (da_assessment IS NULL OR json_valid(da_assessment)),
  tie_break INTEGER DEFAULT 0 CHECK (tie_break IN (0, 1)),
  created_at TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_council_verdicts_decision_created ON council_verdicts(decision_type, created_at);
CREATE INDEX IF NOT EXISTS idx_council_verdicts_project_id ON council_verdicts(project_id);
CREATE INDEX IF NOT EXISTS idx_council_verdicts_tier_used ON council_verdicts(tier_used);

-- Valid transitions (application enforced):
-- DETECTED -> SCREENED
-- SCREENED -> REJECTED | DEFERRED | QUALIFIED
-- DEFERRED -> SCREENED | REJECTED (max 2 reassessments)
-- QUALIFIED -> IN_VALIDATION
-- IN_VALIDATION -> GO_NO_GO | REJECTED
-- GO_NO_GO -> ACTIVE | REJECTED | PAUSED
-- PAUSED -> ACTIVE | REJECTED
-- ACTIVE -> CLOSED
CREATE TABLE IF NOT EXISTS opportunity_records (
  opportunity_id TEXT PRIMARY KEY,
  income_mechanism TEXT NOT NULL CHECK (income_mechanism IN ('software_product','client_work','market_opportunity','ip_asset')),
  title TEXT NOT NULL,
  thesis TEXT NOT NULL,
  detected_by TEXT NOT NULL CHECK (detected_by IN ('research_loop','operator','project_byproduct','research_prompted')),
  council_verdict_id TEXT REFERENCES council_verdicts(verdict_id),
  validation_spend REAL DEFAULT 0.00,
  validation_report TEXT,
  cashflow_estimate TEXT NOT NULL CHECK (json_valid(cashflow_estimate)),
  status TEXT NOT NULL CHECK (status IN ('DETECTED','SCREENED','REJECTED','DEFERRED','QUALIFIED','IN_VALIDATION','GO_NO_GO','PAUSED','ACTIVE','CLOSED')),
  project_id TEXT,
  learning_record TEXT CHECK (learning_record IS NULL OR json_valid(learning_record)),
  provenance_links TEXT DEFAULT '[]' CHECK (json_valid(provenance_links)),
  provenance_degraded INTEGER DEFAULT 0 CHECK (provenance_degraded IN (0, 1)),
  trust_tier INTEGER DEFAULT 2 CHECK (trust_tier BETWEEN 1 AND 4),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_opportunity_records_status ON opportunity_records(status);
CREATE INDEX IF NOT EXISTS idx_opportunity_records_income_created ON opportunity_records(income_mechanism, created_at);
CREATE INDEX IF NOT EXISTS idx_opportunity_records_project_id ON opportunity_records(project_id);

CREATE TABLE IF NOT EXISTS research_tasks (
  task_id TEXT PRIMARY KEY,
  domain INTEGER NOT NULL CHECK (domain BETWEEN 1 AND 5),
  source TEXT NOT NULL CHECK (source IN ('autonomous_loop','operator','council')),
  title TEXT NOT NULL,
  brief TEXT NOT NULL,
  priority TEXT NOT NULL CHECK (priority IN ('P0_IMMEDIATE','P1_HIGH','P2_NORMAL','P3_BACKGROUND')),
  status TEXT NOT NULL CHECK (status IN ('PENDING','ACTIVE','COMPLETE','FAILED','CANCELLED','STALE')),
  max_spend_usd REAL DEFAULT 0.00,
  actual_spend_usd REAL DEFAULT 0.00,
  output_brief_id TEXT,
  follow_up_tasks TEXT DEFAULT '[]' CHECK (json_valid(follow_up_tasks)),
  stale_after TEXT,
  tags TEXT DEFAULT '[]' CHECK (json_valid(tags)),
  depth_upgrade INTEGER DEFAULT 0 CHECK (depth_upgrade IN (0, 1)),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_research_tasks_domain_priority_status ON research_tasks(domain, priority, status);
CREATE INDEX IF NOT EXISTS idx_research_tasks_status_stale_after ON research_tasks(status, stale_after);

CREATE TABLE IF NOT EXISTS standing_briefs (
  standing_brief_id TEXT PRIMARY KEY,
  domain INTEGER NOT NULL CHECK (domain BETWEEN 1 AND 5),
  title TEXT NOT NULL,
  brief TEXT NOT NULL,
  cron_expr TEXT NOT NULL,
  target_interface TEXT NOT NULL,
  include_council_review INTEGER DEFAULT 0 CHECK (include_council_review IN (0, 1)),
  status TEXT NOT NULL CHECK (status IN ('ACTIVE', 'PAUSED', 'ARCHIVED')),
  tags TEXT DEFAULT '[]' CHECK (json_valid(tags)),
  last_task_id TEXT,
  last_job_id TEXT,
  last_run_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_standing_briefs_status_domain ON standing_briefs(status, domain);
CREATE INDEX IF NOT EXISTS idx_standing_briefs_updated_at ON standing_briefs(updated_at);

CREATE TABLE IF NOT EXISTS intelligence_briefs (
  brief_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL REFERENCES research_tasks(task_id),
  domain INTEGER NOT NULL CHECK (domain BETWEEN 1 AND 5),
  title TEXT NOT NULL,
  summary TEXT NOT NULL,
  detail TEXT,
  source_urls TEXT DEFAULT '[]' CHECK (json_valid(source_urls)),
  source_assessments TEXT DEFAULT '[]' CHECK (json_valid(source_assessments)),
  confidence REAL NOT NULL CHECK (confidence >= 0.0 AND confidence <= 1.0),
  uncertainty_statement TEXT,
  counter_thesis TEXT,
  actionability TEXT NOT NULL CHECK (actionability IN ('INFORMATIONAL','WATCH','ACTION_RECOMMENDED','ACTION_REQUIRED','HARVEST_NEEDED')),
  urgency TEXT NOT NULL CHECK (urgency IN ('ROUTINE','ELEVATED','URGENT','CRITICAL')),
  depth_tier TEXT NOT NULL CHECK (depth_tier IN ('QUICK','FULL')),
  action_type TEXT NOT NULL CHECK (action_type IN ('none','council_review','operator_surface','opportunity_feed','security_escalation')),
  spawned_tasks TEXT DEFAULT '[]' CHECK (json_valid(spawned_tasks)),
  spawned_opportunity_id TEXT,
  related_brief_ids TEXT DEFAULT '[]' CHECK (json_valid(related_brief_ids)),
  tags TEXT DEFAULT '[]' CHECK (json_valid(tags)),
  quality_warning INTEGER DEFAULT 0 CHECK (quality_warning IN (0, 1)),
  source_diversity_hold INTEGER DEFAULT 0 CHECK (source_diversity_hold IN (0, 1)),
  provenance_links TEXT DEFAULT '[]' CHECK (json_valid(provenance_links)),
  trust_tier INTEGER DEFAULT 3 CHECK (trust_tier BETWEEN 1 AND 4),
  created_at TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_intelligence_briefs_domain_created ON intelligence_briefs(domain, created_at);
CREATE INDEX IF NOT EXISTS idx_intelligence_briefs_actionability ON intelligence_briefs(actionability);
CREATE INDEX IF NOT EXISTS idx_intelligence_briefs_tags ON intelligence_briefs(tags);
CREATE INDEX IF NOT EXISTS idx_intelligence_briefs_task_id ON intelligence_briefs(task_id);

CREATE TABLE IF NOT EXISTS brief_quality_signals (
  signal_id TEXT PRIMARY KEY,
  verdict_id TEXT NOT NULL REFERENCES council_verdicts(verdict_id),
  brief_id TEXT NOT NULL REFERENCES intelligence_briefs(brief_id),
  signal TEXT NOT NULL CHECK (signal IN ('sufficient','incomplete','misleading')),
  missing_dimension TEXT,
  created_at TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_brief_quality_signals_brief_id ON brief_quality_signals(brief_id);
CREATE INDEX IF NOT EXISTS idx_brief_quality_signals_verdict_id ON brief_quality_signals(verdict_id);
CREATE INDEX IF NOT EXISTS idx_brief_quality_signals_signal_created ON brief_quality_signals(signal, created_at);

CREATE TABLE IF NOT EXISTS calibration_records (
  calibration_id TEXT PRIMARY KEY,
  verdict_id TEXT NOT NULL REFERENCES council_verdicts(verdict_id),
  decision_type TEXT NOT NULL,
  predicted_outcome TEXT NOT NULL CHECK (predicted_outcome IN ('PURSUE','REJECT')),
  actual_outcome REAL CHECK (actual_outcome IS NULL OR actual_outcome IN (0.0, 0.5, 1.0)),
  prediction_correct REAL CHECK (prediction_correct IS NULL OR prediction_correct IN (0.0, 1.0)),
  role_weights_used TEXT NOT NULL CHECK (json_valid(role_weights_used)),
  which_role_was_right TEXT,
  tie_break INTEGER DEFAULT 0 CHECK (tie_break IN (0, 1)),
  threshold_status TEXT DEFAULT 'PROVISIONAL' CHECK (threshold_status IN ('PROVISIONAL','VALIDATED')),
  created_at TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_calibration_records_decision_created ON calibration_records(decision_type, created_at);

CREATE TABLE IF NOT EXISTS idea_records (
  idea_id TEXT PRIMARY KEY,
  brief_ids TEXT NOT NULL CHECK (json_valid(brief_ids)),
  title TEXT NOT NULL,
  proposal TEXT NOT NULL,
  income_mechanism TEXT NOT NULL CHECK (income_mechanism IN ('software_product','client_work','market_opportunity','ip_asset')),
  confidence REAL NOT NULL CHECK (confidence >= 0.0 AND confidence <= 1.0),
  evidence_strength TEXT NOT NULL CHECK (evidence_strength IN ('anecdotal','single_source','corroborated','validated')),
  estimated_effort TEXT NOT NULL CHECK (estimated_effort IN ('days','weeks','months')),
  created_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS market_signals (
  signal_id TEXT PRIMARY KEY,
  signal_type TEXT NOT NULL,
  title TEXT NOT NULL,
  content TEXT NOT NULL,
  source_url TEXT,
  domain INTEGER CHECK (domain IS NULL OR domain BETWEEN 1 AND 5),
  provenance_links TEXT DEFAULT '[]' CHECK (json_valid(provenance_links)),
  trust_tier INTEGER DEFAULT 4 CHECK (trust_tier BETWEEN 1 AND 4),
  expires_at TEXT NOT NULL,
  created_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS capability_gaps (
  gap_id TEXT PRIMARY KEY,
  project_id TEXT,
  description TEXT NOT NULL,
  skill_investment TEXT,
  resolved INTEGER DEFAULT 0 CHECK (resolved IN (0, 1)),
  provenance_links TEXT DEFAULT '[]' CHECK (json_valid(provenance_links)),
  trust_tier INTEGER DEFAULT 3 CHECK (trust_tier BETWEEN 1 AND 4),
  resolved_at TEXT,
  created_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS source_reputations (
  source_url TEXT PRIMARY KEY,
  reputation_score REAL NOT NULL DEFAULT 0.50 CHECK (reputation_score >= 0.05 AND reputation_score <= 0.95),
  is_primary INTEGER DEFAULT 0 CHECK (is_primary IN (0, 1)),
  on_trusted_list INTEGER DEFAULT 0 CHECK (on_trusted_list IN (0, 1)),
  last_event_at TEXT NOT NULL,
  archived INTEGER DEFAULT 0 CHECK (archived IN (0, 1)),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS dedup_records (
  dedup_id TEXT PRIMARY KEY,
  opportunity_id_kept TEXT NOT NULL REFERENCES opportunity_records(opportunity_id),
  opportunity_id_merged TEXT NOT NULL REFERENCES opportunity_records(opportunity_id),
  combined_similarity REAL NOT NULL,
  threshold_status TEXT DEFAULT 'PROVISIONAL' CHECK (threshold_status IN ('PROVISIONAL','VALIDATED')),
  created_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS deferred_research_entries (
  entry_id TEXT PRIMARY KEY,
  task_summary TEXT NOT NULL,
  reason TEXT NOT NULL CHECK (reason IN ('contention_deferred','harvest_expired','tool_failed')),
  domain INTEGER NOT NULL CHECK (domain BETWEEN 1 AND 5),
  original_priority TEXT NOT NULL,
  deferred_at TEXT NOT NULL,
  resolved_at TEXT,
  stale_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS model_scout_reports (
  report_id TEXT PRIMARY KEY,
  candidate_model_id TEXT NOT NULL,
  target_role TEXT NOT NULL CHECK (target_role IN ('Primary Reasoning','Execution','Validation','Training/Reward','Embedding','Cloud Escalation')),
  model_card_summary TEXT NOT NULL,
  licence TEXT NOT NULL,
  quantisation_available INTEGER NOT NULL CHECK (quantisation_available IN (0, 1)),
  memory_footprint_gb REAL,
  benchmark_scores TEXT NOT NULL CHECK (json_valid(benchmark_scores)),
  plausible_fit INTEGER NOT NULL CHECK (plausible_fit IN (0, 1)),
  disqualifiers TEXT DEFAULT '[]' CHECK (json_valid(disqualifiers)),
  created_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS model_assess_reports (
  report_id TEXT PRIMARY KEY,
  scout_report_id TEXT NOT NULL REFERENCES model_scout_reports(report_id),
  candidate_model_id TEXT NOT NULL,
  target_role TEXT NOT NULL,
  quick_eval_score REAL NOT NULL,
  incumbent_baseline REAL NOT NULL,
  ram_actual_gb REAL,
  latency_p50_ms REAL,
  latency_p95_ms REAL,
  quantisation_method TEXT,
  operator_approved_download INTEGER NOT NULL CHECK (operator_approved_download IN (0, 1)),
  assessment TEXT NOT NULL CHECK (assessment IN ('PROCEED_TO_SHADOW','REJECT')),
  reject_reason TEXT,
  created_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS shadow_trial_reports (
  report_id TEXT PRIMARY KEY,
  assess_report_id TEXT NOT NULL REFERENCES model_assess_reports(report_id),
  candidate_model_id TEXT NOT NULL,
  target_role TEXT NOT NULL,
  tasks_evaluated INTEGER NOT NULL CHECK (tasks_evaluated >= 200),
  candidate_score REAL NOT NULL,
  incumbent_score REAL NOT NULL,
  score_delta_pct REAL NOT NULL,
  latency_candidate_p95_ms REAL NOT NULL,
  latency_incumbent_p95_ms REAL NOT NULL,
  memory_candidate_gb REAL NOT NULL,
  memory_incumbent_gb REAL NOT NULL,
  edge_case_failures INTEGER NOT NULL,
  recommendation TEXT NOT NULL CHECK (recommendation IN ('PROMOTE','REJECT')),
  created_at TEXT NOT NULL
) STRICT;

CREATE INDEX IF NOT EXISTS idx_research_tasks_created_at ON research_tasks(created_at);
CREATE INDEX IF NOT EXISTS idx_market_signals_created_at ON market_signals(created_at);
CREATE INDEX IF NOT EXISTS idx_idea_records_created_at ON idea_records(created_at);
CREATE INDEX IF NOT EXISTS idx_dedup_records_created_at ON dedup_records(created_at);
