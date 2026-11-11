CREATE DATABASE IF NOT EXISTS market_data;

CREATE TABLE IF NOT EXISTS market_data.raw_venue_events (
  event_id String,
  venue LowCardinality(String),
  event_type LowCardinality(String),
  received_at_ts Int64,
  source_ts Int64,
  payload_json String,
  connection_id String,
  sequence_hint String,
  ingestion_status LowCardinality(String)
) ENGINE = MergeTree
ORDER BY (venue, event_id, received_at_ts);

CREATE TABLE IF NOT EXISTS market_data.normalized_market_events (
  normalized_event_id String,
  canonical_contract_key String,
  venue LowCardinality(String),
  venue_market_id String,
  event_type LowCardinality(String),
  event_ts Int64,
  best_bid String,
  best_ask String,
  last_trade_price String,
  last_trade_size String,
  spread String,
  midpoint String,
  metadata_json String
) ENGINE = MergeTree
ORDER BY (canonical_contract_key, event_ts, normalized_event_id);

CREATE TABLE IF NOT EXISTS market_data.reference_price_events (
  reference_event_id String,
  source LowCardinality(String),
  symbol String,
  event_ts Int64,
  best_bid String,
  best_ask String,
  last_price String,
  mid_price String,
  metadata_json String
) ENGINE = MergeTree
ORDER BY (source, symbol, event_ts, reference_event_id);

CREATE TABLE IF NOT EXISTS market_data.orderbook_snapshots (
  snapshot_id String,
  venue LowCardinality(String),
  venue_market_id String,
  event_ts Int64,
  bids_json String,
  asks_json String,
  metadata_json String
) ENGINE = MergeTree
ORDER BY (venue, venue_market_id, event_ts, snapshot_id);

CREATE TABLE IF NOT EXISTS market_data.orderbook_deltas (
  delta_id String,
  venue LowCardinality(String),
  venue_market_id String,
  event_ts Int64,
  side LowCardinality(String),
  price String,
  size String,
  metadata_json String
) ENGINE = MergeTree
ORDER BY (venue, venue_market_id, event_ts, delta_id);

CREATE TABLE IF NOT EXISTS market_data.replay_segments (
  segment_id String,
  venue LowCardinality(String),
  venue_market_id String,
  from_ts Int64,
  to_ts Int64,
  record_count UInt64,
  checksum String,
  metadata_json String
) ENGINE = MergeTree
ORDER BY (venue, venue_market_id, from_ts, segment_id);

CREATE TABLE IF NOT EXISTS market_data.stream_health_history (
  service_name String,
  venue LowCardinality(String),
  connection_status LowCardinality(String),
  last_message_ts Int64,
  reconnect_count UInt64,
  dropped_message_estimate UInt64,
  current_latency_ms UInt64,
  health_state LowCardinality(String),
  observed_at_ts Int64
) ENGINE = MergeTree
ORDER BY (service_name, venue, observed_at_ts);

