CREATE TABLE IF NOT EXISTS market_data.fair_value_events (
  event_id String,
  canonical_contract_key String,
  underlying String,
  observed_at_ts Int64,
  fair_prob String,
  fair_prob_low String,
  fair_prob_high String,
  confidence String,
  model_version String,
  market_implied_prob String,
  payload_json String
) ENGINE = MergeTree
ORDER BY (canonical_contract_key, observed_at_ts, event_id);

CREATE TABLE IF NOT EXISTS market_data.signal_events (
  signal_id String,
  signal_type LowCardinality(String),
  canonical_contract_key String,
  underlying String,
  observed_at_ts Int64,
  accepted UInt8,
  gross_edge_bps String,
  net_edge_bps String,
  confidence String,
  payload_json String
) ENGINE = MergeTree
ORDER BY (canonical_contract_key, observed_at_ts, signal_id);

CREATE TABLE IF NOT EXISTS market_data.paper_execution_events (
  decision_id String,
  signal_id String,
  canonical_contract_key String,
  observed_at_ts Int64,
  traded UInt8,
  executed_qty String,
  fill_ratio String,
  slippage_bps String,
  payload_json String
) ENGINE = MergeTree
ORDER BY (canonical_contract_key, observed_at_ts, decision_id);

CREATE TABLE IF NOT EXISTS market_data.replay_run_events (
  run_id String,
  event_ts Int64,
  stage LowCardinality(String),
  payload_json String
) ENGINE = MergeTree
ORDER BY (run_id, event_ts, stage);

