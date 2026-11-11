CREATE TABLE IF NOT EXISTS canonical_contracts (
  canonical_contract_key TEXT PRIMARY KEY,
  venue TEXT NOT NULL,
  venue_market_id TEXT NOT NULL,
  underlying TEXT,
  contract_family TEXT,
  strike NUMERIC(38, 18),
  threshold_type TEXT,
  expiration_ts TIMESTAMPTZ,
  quote_currency TEXT,
  settlement_source TEXT,
  settlement_window_seconds BIGINT,
  tick_size NUMERIC(38, 18),
  fee_profile_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  reward_profile_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL DEFAULT 'active',
  metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS venue_market_mappings (
  id BIGSERIAL PRIMARY KEY,
  venue TEXT NOT NULL,
  venue_market_id TEXT NOT NULL,
  canonical_contract_key TEXT NOT NULL REFERENCES canonical_contracts(canonical_contract_key),
  mapping_state TEXT NOT NULL DEFAULT 'resolved',
  reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (venue, venue_market_id)
);

CREATE TABLE IF NOT EXISTS service_registry (
  id BIGSERIAL PRIMARY KEY,
  service_name TEXT NOT NULL,
  instance_id TEXT NOT NULL,
  build_version TEXT NOT NULL,
  status TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (service_name, instance_id)
);

CREATE TABLE IF NOT EXISTS runtime_config (
  key TEXT PRIMARY KEY,
  value_json JSONB NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS audit_events (
  id BIGSERIAL PRIMARY KEY,
  event_type TEXT NOT NULL,
  payload_json JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS schema_versions (
  component TEXT PRIMARY KEY,
  version INT NOT NULL,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO schema_versions (component, version)
VALUES ('phase1_base', 1)
ON CONFLICT (component) DO UPDATE SET version = EXCLUDED.version, applied_at = NOW();

