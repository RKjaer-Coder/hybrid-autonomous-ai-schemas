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
  entity_type TEXT NOT NULL CHECK (entity_type IN ('task','research_request','source_plan','evidence_bundle','decision','project','model','budget','gate','capability','side_effect','policy','artifact')),
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

CREATE TABLE IF NOT EXISTS research_requests (
  request_id TEXT PRIMARY KEY,
  profile TEXT NOT NULL CHECK (profile IN ('commercial','ai_models','financial_markets','system_improvement','security','regulatory','project_support','general')),
  question TEXT NOT NULL,
  decision_target TEXT,
  freshness_horizon TEXT NOT NULL,
  depth TEXT NOT NULL CHECK (depth IN ('quick','standard','deep')),
  source_policy_json TEXT NOT NULL CHECK (json_valid(source_policy_json)),
  evidence_requirements_json TEXT NOT NULL CHECK (json_valid(evidence_requirements_json)),
  max_cost_usd TEXT NOT NULL,
  max_latency TEXT,
  autonomy_class TEXT NOT NULL CHECK (autonomy_class IN ('A2','A3','A4')),
  status TEXT NOT NULL CHECK (status IN ('queued','collecting','synthesizing','review_needed','completed','failed')),
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS evidence_bundles (
  bundle_id TEXT PRIMARY KEY,
  request_id TEXT NOT NULL REFERENCES research_requests(request_id),
  source_plan_id TEXT NOT NULL,
  sources_json TEXT NOT NULL CHECK (json_valid(sources_json)),
  claims_json TEXT NOT NULL CHECK (json_valid(claims_json)),
  contradictions_json TEXT NOT NULL CHECK (json_valid(contradictions_json)),
  unsupported_claims_json TEXT NOT NULL CHECK (json_valid(unsupported_claims_json)),
  freshness_summary TEXT NOT NULL,
  confidence REAL NOT NULL CHECK (confidence >= 0.0 AND confidence <= 1.0),
  uncertainty TEXT NOT NULL,
  counter_thesis TEXT,
  quality_gate_result TEXT NOT NULL CHECK (quality_gate_result IN ('pass','fail','degraded')),
  data_classes_json TEXT NOT NULL CHECK (json_valid(data_classes_json)),
  retention_policy TEXT NOT NULL,
  created_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS source_plans (
  source_plan_id TEXT PRIMARY KEY,
  request_id TEXT NOT NULL REFERENCES research_requests(request_id),
  profile TEXT NOT NULL CHECK (profile IN ('commercial','ai_models','financial_markets','system_improvement','security','regulatory','project_support','general')),
  depth TEXT NOT NULL CHECK (depth IN ('quick','standard','deep')),
  planned_sources_json TEXT NOT NULL CHECK (json_valid(planned_sources_json)),
  retrieval_strategy TEXT NOT NULL,
  created_by TEXT NOT NULL CHECK (created_by IN ('kernel','operator','agent','scheduler')),
  status TEXT NOT NULL CHECK (status IN ('planned','collecting','completed','blocked')),
  created_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS source_acquisition_checks (
  check_id TEXT PRIMARY KEY,
  request_id TEXT NOT NULL REFERENCES research_requests(request_id),
  source_plan_id TEXT NOT NULL REFERENCES source_plans(source_plan_id),
  source_ref TEXT NOT NULL,
  access_method TEXT NOT NULL CHECK (access_method IN ('public_web','operator_provided','paid_source','local_file','internal_record','api')),
  data_class TEXT NOT NULL CHECK (data_class IN ('public','internal','sensitive','secret_ref','regulated','client_confidential')),
  source_type TEXT NOT NULL CHECK (source_type IN ('official','primary_data','reputable_media','community','model_card','paper','market_data','internal_record','other')),
  result TEXT NOT NULL CHECK (result IN ('allowed','blocked','requires_grant')),
  reason TEXT NOT NULL,
  grant_id TEXT REFERENCES capability_grants(grant_id),
  checked_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS decisions (
  decision_id TEXT PRIMARY KEY,
  decision_type TEXT NOT NULL CHECK (decision_type IN ('project_approval','kill','pivot','spend','architecture','model_promotion','security','commercial_strategy','system_improvement','model_demotion','routing','other')),
  question TEXT NOT NULL,
  options_json TEXT NOT NULL CHECK (json_valid(options_json)),
  stakes TEXT NOT NULL CHECK (stakes IN ('low','medium','high','critical')),
  evidence_bundle_ids_json TEXT NOT NULL CHECK (json_valid(evidence_bundle_ids_json)),
  evidence_refs_json TEXT NOT NULL CHECK (json_valid(evidence_refs_json)),
  requested_by TEXT NOT NULL CHECK (requested_by IN ('operator','kernel','project','research','model_intelligence','scheduler')),
  required_authority TEXT NOT NULL CHECK (required_authority IN ('rule','single_agent','council','operator_gate')),
  authority_policy_version TEXT NOT NULL,
  deadline TEXT,
  status TEXT NOT NULL CHECK (status IN ('proposed','deliberating','decided','gated','expired','cancelled')),
  recommendation TEXT,
  verdict TEXT,
  confidence REAL CHECK (confidence IS NULL OR (confidence >= 0.0 AND confidence <= 1.0)),
  decisive_factors_json TEXT NOT NULL CHECK (json_valid(decisive_factors_json)),
  decisive_uncertainty TEXT,
  risk_flags_json TEXT NOT NULL CHECK (json_valid(risk_flags_json)),
  default_on_timeout TEXT,
  gate_packet_json TEXT CHECK (gate_packet_json IS NULL OR json_valid(gate_packet_json)),
  created_at TEXT NOT NULL,
  decided_at TEXT
) STRICT;

CREATE TABLE IF NOT EXISTS quality_gate_events (
  gate_event_id TEXT PRIMARY KEY,
  request_id TEXT NOT NULL REFERENCES research_requests(request_id),
  bundle_id TEXT NOT NULL,
  source_plan_id TEXT NOT NULL REFERENCES source_plans(source_plan_id),
  profile TEXT NOT NULL CHECK (profile IN ('commercial','ai_models','financial_markets','system_improvement','security','regulatory','project_support','general')),
  result TEXT NOT NULL CHECK (result IN ('pass','fail','degraded')),
  confidence REAL NOT NULL CHECK (confidence >= 0.0 AND confidence <= 1.0),
  checks_json TEXT NOT NULL CHECK (json_valid(checks_json)),
  created_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS commercial_decision_packets (
  packet_id TEXT PRIMARY KEY,
  decision_id TEXT NOT NULL UNIQUE REFERENCES decisions(decision_id),
  request_id TEXT NOT NULL REFERENCES research_requests(request_id),
  evidence_bundle_id TEXT NOT NULL REFERENCES evidence_bundles(bundle_id),
  decision_target TEXT NOT NULL,
  question TEXT NOT NULL,
  recommendation TEXT NOT NULL CHECK (recommendation IN ('pursue','pause','reject','insufficient_evidence')),
  required_authority TEXT NOT NULL CHECK (required_authority IN ('rule','single_agent','council','operator_gate')),
  opportunity_json TEXT NOT NULL CHECK (json_valid(opportunity_json)),
  project_json TEXT NOT NULL CHECK (json_valid(project_json)),
  gate_packet_json TEXT NOT NULL CHECK (json_valid(gate_packet_json)),
  evidence_used_json TEXT NOT NULL CHECK (json_valid(evidence_used_json)),
  risk_flags_json TEXT NOT NULL CHECK (json_valid(risk_flags_json)),
  default_on_timeout TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('proposed','gated','decided','cancelled')),
  created_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS model_task_classes (
  task_class_id TEXT PRIMARY KEY,
  task_class TEXT NOT NULL UNIQUE CHECK (task_class IN ('quick_research_summarization','source_claim_extraction','coding_small_patch')),
  description TEXT NOT NULL,
  quality_threshold REAL NOT NULL CHECK (quality_threshold >= 0.0 AND quality_threshold <= 1.0),
  reliability_threshold REAL NOT NULL CHECK (reliability_threshold >= 0.0 AND reliability_threshold <= 1.0),
  latency_p95_ms INTEGER NOT NULL CHECK (latency_p95_ms > 0),
  local_offload_target REAL NOT NULL CHECK (local_offload_target >= 0.0 AND local_offload_target <= 1.0),
  allowed_data_classes_json TEXT NOT NULL CHECK (json_valid(allowed_data_classes_json)),
  promotion_authority TEXT NOT NULL CHECK (promotion_authority IN ('rule','single_agent','council','operator_gate')),
  expansion_allowed INTEGER NOT NULL CHECK (expansion_allowed IN (0, 1)),
  status TEXT NOT NULL CHECK (status IN ('seed','retired')),
  created_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS model_candidates (
  candidate_id TEXT PRIMARY KEY,
  model_id TEXT NOT NULL UNIQUE,
  provider TEXT NOT NULL CHECK (provider IN ('local','lm_studio','ollama','mlx','openrouter','nous','openai','anthropic','google','xai','other')),
  access_mode TEXT NOT NULL CHECK (access_mode IN ('local','free_api','subscription_tool','paid_api','operator_prompted')),
  source_ref TEXT NOT NULL,
  artifact_hash TEXT,
  license TEXT NOT NULL,
  commercial_use TEXT NOT NULL CHECK (commercial_use IN ('allowed','restricted','prohibited','unknown')),
  terms_verified_at TEXT,
  context_window INTEGER,
  modalities_json TEXT NOT NULL CHECK (json_valid(modalities_json)),
  hardware_fit TEXT NOT NULL CHECK (hardware_fit IN ('excellent','good','marginal','not_local')),
  sandbox_profile TEXT,
  data_residency TEXT NOT NULL CHECK (data_residency IN ('local_only','provider_retained','provider_no_train','unknown')),
  cost_profile_json TEXT NOT NULL CHECK (json_valid(cost_profile_json)),
  latency_profile_json TEXT NOT NULL CHECK (json_valid(latency_profile_json)),
  routing_metadata_json TEXT NOT NULL CHECK (json_valid(routing_metadata_json)),
  promotion_state TEXT NOT NULL CHECK (promotion_state IN ('discovered','queued_for_eval','shadow','promoted','demoted','rejected','retired')),
  last_verified_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS model_holdout_policies (
  policy_id TEXT PRIMARY KEY,
  task_class TEXT NOT NULL REFERENCES model_task_classes(task_class),
  dataset_version TEXT NOT NULL,
  access TEXT NOT NULL CHECK (access IN ('sealed','operator_only','scoring_service')),
  min_sample_count INTEGER NOT NULL CHECK (min_sample_count > 0),
  contamination_controls_json TEXT NOT NULL CHECK (json_valid(contamination_controls_json)),
  scorer_separation TEXT NOT NULL,
  promotion_requires_decision INTEGER NOT NULL CHECK (promotion_requires_decision IN (0, 1)),
  created_at TEXT NOT NULL,
  UNIQUE(task_class, dataset_version)
) STRICT;

CREATE TABLE IF NOT EXISTS local_offload_eval_sets (
  eval_set_id TEXT PRIMARY KEY,
  task_class TEXT NOT NULL REFERENCES model_task_classes(task_class),
  dataset_version TEXT NOT NULL,
  artifact_ref TEXT NOT NULL,
  split_counts_json TEXT NOT NULL CHECK (json_valid(split_counts_json)),
  data_classes_json TEXT NOT NULL CHECK (json_valid(data_classes_json)),
  retention_policy TEXT NOT NULL,
  scorer_profile_json TEXT NOT NULL CHECK (json_valid(scorer_profile_json)),
  holdout_policy_id TEXT NOT NULL REFERENCES model_holdout_policies(policy_id),
  status TEXT NOT NULL CHECK (status IN ('draft','active','retired')),
  created_at TEXT NOT NULL,
  UNIQUE(task_class, dataset_version)
) STRICT;

CREATE TABLE IF NOT EXISTS model_holdout_use_records (
  holdout_use_id TEXT PRIMARY KEY,
  policy_id TEXT NOT NULL REFERENCES model_holdout_policies(policy_id),
  eval_set_id TEXT NOT NULL REFERENCES local_offload_eval_sets(eval_set_id),
  task_class TEXT NOT NULL,
  dataset_version TEXT NOT NULL,
  requester_id TEXT NOT NULL,
  requester_change_ref TEXT,
  purpose TEXT NOT NULL CHECK (purpose IN ('development','regression','promotion_gate','audit')),
  verdict TEXT NOT NULL CHECK (verdict IN ('allowed','blocked')),
  reason TEXT NOT NULL,
  decision_id TEXT REFERENCES decisions(decision_id),
  created_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS model_eval_runs (
  eval_run_id TEXT PRIMARY KEY,
  model_id TEXT NOT NULL REFERENCES model_candidates(model_id),
  task_class TEXT NOT NULL REFERENCES model_task_classes(task_class),
  dataset_version TEXT NOT NULL,
  eval_set_id TEXT NOT NULL REFERENCES local_offload_eval_sets(eval_set_id),
  baseline_model_id TEXT REFERENCES model_candidates(model_id),
  route_version TEXT NOT NULL,
  route_metadata_json TEXT NOT NULL CHECK (json_valid(route_metadata_json)),
  sample_count INTEGER NOT NULL CHECK (sample_count > 0),
  quality_score REAL NOT NULL CHECK (quality_score >= 0.0 AND quality_score <= 1.0),
  reliability_score REAL NOT NULL CHECK (reliability_score >= 0.0 AND reliability_score <= 1.0),
  latency_p50_ms INTEGER NOT NULL CHECK (latency_p50_ms >= 0),
  latency_p95_ms INTEGER NOT NULL CHECK (latency_p95_ms >= 0),
  cost_per_1k_tasks TEXT NOT NULL,
  aggregate_scores_json TEXT NOT NULL CHECK (json_valid(aggregate_scores_json)),
  failure_categories_json TEXT NOT NULL CHECK (json_valid(failure_categories_json)),
  failure_modes_json TEXT NOT NULL CHECK (json_valid(failure_modes_json)),
  confidence_json TEXT NOT NULL CHECK (json_valid(confidence_json)),
  frozen_holdout_result_json TEXT NOT NULL CHECK (json_valid(frozen_holdout_result_json)),
  verdict TEXT NOT NULL CHECK (verdict IN ('supports_decision','shadow','reject','needs_more_data')),
  scorer_id TEXT NOT NULL,
  decision_id TEXT REFERENCES decisions(decision_id),
  authority_effect TEXT NOT NULL CHECK (authority_effect = 'evidence_only'),
  created_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS model_route_decisions (
  route_decision_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  task_class TEXT NOT NULL REFERENCES model_task_classes(task_class),
  data_class TEXT NOT NULL CHECK (data_class IN ('public','internal','sensitive','secret_ref','regulated','client_confidential')),
  risk_level TEXT NOT NULL CHECK (risk_level IN ('low','medium','high','critical')),
  selected_route TEXT NOT NULL CHECK (selected_route IN ('local','shadow','fallback','frontier','operator_prompted','blocked')),
  selected_model_id TEXT,
  candidate_model_id TEXT,
  eval_set_id TEXT REFERENCES local_offload_eval_sets(eval_set_id),
  reasons_json TEXT NOT NULL CHECK (json_valid(reasons_json)),
  required_authority TEXT NOT NULL CHECK (required_authority IN ('rule','single_agent','council','operator_gate')),
  decision_id TEXT REFERENCES decisions(decision_id),
  local_offload_estimate_json TEXT NOT NULL CHECK (json_valid(local_offload_estimate_json)),
  frontier_fallback_json TEXT NOT NULL CHECK (json_valid(frontier_fallback_json)),
  created_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS model_promotion_decision_packets (
  packet_id TEXT PRIMARY KEY,
  decision_id TEXT NOT NULL UNIQUE REFERENCES decisions(decision_id),
  model_id TEXT NOT NULL REFERENCES model_candidates(model_id),
  task_class TEXT NOT NULL REFERENCES model_task_classes(task_class),
  proposed_routing_role TEXT NOT NULL CHECK (proposed_routing_role IN ('primary_local','research_local','coding_local','validation_local','embeddings_local','frontier_escalation','cheap_cloud')),
  recommendation TEXT NOT NULL CHECK (recommendation IN ('promote','keep_shadow','reject','needs_more_data')),
  required_authority TEXT NOT NULL CHECK (required_authority IN ('rule','single_agent','council','operator_gate')),
  eval_run_ids_json TEXT NOT NULL CHECK (json_valid(eval_run_ids_json)),
  holdout_use_ids_json TEXT NOT NULL CHECK (json_valid(holdout_use_ids_json)),
  evidence_refs_json TEXT NOT NULL CHECK (json_valid(evidence_refs_json)),
  frozen_holdout_confidence REAL NOT NULL CHECK (frozen_holdout_confidence >= 0.0 AND frozen_holdout_confidence <= 1.0),
  confidence_threshold REAL NOT NULL CHECK (confidence_threshold >= 0.0 AND confidence_threshold <= 1.0),
  gate_packet_json TEXT NOT NULL CHECK (json_valid(gate_packet_json)),
  risk_flags_json TEXT NOT NULL CHECK (json_valid(risk_flags_json)),
  default_on_timeout TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('proposed','gated','decided','cancelled')),
  created_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS model_demotion_records (
  demotion_id TEXT PRIMARY KEY,
  model_id TEXT NOT NULL REFERENCES model_candidates(model_id),
  task_class TEXT NOT NULL REFERENCES model_task_classes(task_class),
  routing_roles_json TEXT NOT NULL CHECK (json_valid(routing_roles_json)),
  reasons_json TEXT NOT NULL CHECK (json_valid(reasons_json)),
  required_authority TEXT NOT NULL CHECK (required_authority = 'rule'),
  evidence_refs_json TEXT NOT NULL CHECK (json_valid(evidence_refs_json)),
  eval_run_ids_json TEXT NOT NULL CHECK (json_valid(eval_run_ids_json)),
  route_decision_ids_json TEXT NOT NULL CHECK (json_valid(route_decision_ids_json)),
  metrics_json TEXT NOT NULL CHECK (json_valid(metrics_json)),
  routing_state_update_json TEXT NOT NULL CHECK (json_valid(routing_state_update_json)),
  audit_notes TEXT NOT NULL,
  decision_id TEXT REFERENCES decisions(decision_id),
  authority_effect TEXT NOT NULL CHECK (authority_effect = 'immediate_routing_update'),
  created_at TEXT NOT NULL
) STRICT;

CREATE TABLE IF NOT EXISTS model_routing_state (
  state_id TEXT PRIMARY KEY,
  task_class TEXT NOT NULL REFERENCES model_task_classes(task_class),
  routing_role TEXT NOT NULL CHECK (routing_role IN ('primary_local','research_local','coding_local','validation_local','embeddings_local','frontier_escalation','cheap_cloud')),
  active_model_id TEXT REFERENCES model_candidates(model_id),
  status TEXT NOT NULL CHECK (status IN ('active','demoted','blocked')),
  route_version TEXT NOT NULL,
  replacement_model_id TEXT REFERENCES model_candidates(model_id),
  demotion_id TEXT REFERENCES model_demotion_records(demotion_id),
  previous_state_json TEXT NOT NULL CHECK (json_valid(previous_state_json)),
  fallback_route_json TEXT NOT NULL CHECK (json_valid(fallback_route_json)),
  reasons_json TEXT NOT NULL CHECK (json_valid(reasons_json)),
  updated_at TEXT NOT NULL,
  UNIQUE(task_class, routing_role)
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
CREATE INDEX IF NOT EXISTS idx_research_requests_profile_status ON research_requests(profile, status, created_at);
CREATE INDEX IF NOT EXISTS idx_research_requests_decision_target ON research_requests(decision_target, status);
CREATE INDEX IF NOT EXISTS idx_source_plans_request ON source_plans(request_id, status, created_at);
CREATE INDEX IF NOT EXISTS idx_source_acquisition_checks_plan ON source_acquisition_checks(source_plan_id, result, checked_at);
CREATE INDEX IF NOT EXISTS idx_quality_gate_events_request ON quality_gate_events(request_id, result, created_at);
CREATE INDEX IF NOT EXISTS idx_decisions_type_status ON decisions(decision_type, status, created_at);
CREATE INDEX IF NOT EXISTS idx_decisions_authority ON decisions(required_authority, status, created_at);
CREATE INDEX IF NOT EXISTS idx_evidence_bundles_request ON evidence_bundles(request_id, created_at);
CREATE INDEX IF NOT EXISTS idx_evidence_bundles_quality ON evidence_bundles(quality_gate_result, confidence);
CREATE INDEX IF NOT EXISTS idx_commercial_decision_packets_target ON commercial_decision_packets(decision_target, status, created_at);
CREATE INDEX IF NOT EXISTS idx_commercial_decision_packets_bundle ON commercial_decision_packets(evidence_bundle_id);
CREATE INDEX IF NOT EXISTS idx_model_task_classes_status ON model_task_classes(status, task_class);
CREATE INDEX IF NOT EXISTS idx_model_candidates_state ON model_candidates(promotion_state, provider, hardware_fit);
CREATE INDEX IF NOT EXISTS idx_model_holdout_policies_task ON model_holdout_policies(task_class, dataset_version);
CREATE INDEX IF NOT EXISTS idx_local_offload_eval_sets_task ON local_offload_eval_sets(task_class, status, dataset_version);
CREATE INDEX IF NOT EXISTS idx_model_holdout_use_records_eval ON model_holdout_use_records(eval_set_id, purpose, verdict, created_at);
CREATE INDEX IF NOT EXISTS idx_model_eval_runs_model_task ON model_eval_runs(model_id, task_class, created_at);
CREATE INDEX IF NOT EXISTS idx_model_eval_runs_verdict ON model_eval_runs(task_class, verdict, created_at);
CREATE INDEX IF NOT EXISTS idx_model_route_decisions_task ON model_route_decisions(task_class, selected_route, created_at);
CREATE INDEX IF NOT EXISTS idx_model_route_decisions_model ON model_route_decisions(selected_model_id, candidate_model_id, created_at);
CREATE INDEX IF NOT EXISTS idx_model_promotion_packets_task ON model_promotion_decision_packets(task_class, status, created_at);
CREATE INDEX IF NOT EXISTS idx_model_promotion_packets_model ON model_promotion_decision_packets(model_id, proposed_routing_role, created_at);
CREATE INDEX IF NOT EXISTS idx_model_demotion_records_model ON model_demotion_records(model_id, task_class, created_at);
CREATE INDEX IF NOT EXISTS idx_model_demotion_records_task ON model_demotion_records(task_class, created_at);
CREATE INDEX IF NOT EXISTS idx_model_routing_state_role ON model_routing_state(task_class, routing_role, status);
CREATE INDEX IF NOT EXISTS idx_model_routing_state_model ON model_routing_state(active_model_id, replacement_model_id, status);
CREATE INDEX IF NOT EXISTS idx_capability_grants_subject ON capability_grants(subject_type, subject_id, capability_type, status);
CREATE INDEX IF NOT EXISTS idx_budgets_owner ON budgets(owner_type, owner_id, status);
CREATE INDEX IF NOT EXISTS idx_budget_reservations_budget_status ON budget_reservations(budget_id, status);
CREATE INDEX IF NOT EXISTS idx_artifact_refs_data_class ON artifact_refs(data_class, created_at);
CREATE INDEX IF NOT EXISTS idx_side_effect_intents_status ON side_effect_intents(status, side_effect_type);
CREATE INDEX IF NOT EXISTS idx_side_effect_receipts_intent ON side_effect_receipts(intent_id, recorded_at);
CREATE INDEX IF NOT EXISTS idx_projection_outbox_status ON projection_outbox(status, created_at);
