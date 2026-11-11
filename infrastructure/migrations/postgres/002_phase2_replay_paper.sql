CREATE TABLE IF NOT EXISTS replay_runs (
  run_id TEXT PRIMARY KEY,
  mode TEXT NOT NULL,
  underlying TEXT NOT NULL,
  from_ts TIMESTAMPTZ NOT NULL,
  to_ts TIMESTAMPTZ NOT NULL,
  speed TEXT NOT NULL,
  status TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS replay_run_metrics (
  run_id TEXT PRIMARY KEY REFERENCES replay_runs(run_id) ON DELETE CASCADE,
  accepted_signals BIGINT NOT NULL DEFAULT 0,
  rejected_signals BIGINT NOT NULL DEFAULT 0,
  traded_signals BIGINT NOT NULL DEFAULT 0,
  realized_pnl TEXT NOT NULL DEFAULT '0',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS paper_positions (
  canonical_contract_key TEXT PRIMARY KEY,
  qty TEXT NOT NULL,
  avg_price TEXT NOT NULL,
  realized_pnl TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS paper_signal_decisions (
  id BIGSERIAL PRIMARY KEY,
  signal_id TEXT NOT NULL,
  canonical_contract_key TEXT NOT NULL,
  accepted BOOLEAN NOT NULL,
  payload_json JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_paper_signal_decisions_signal_id ON paper_signal_decisions(signal_id);
CREATE INDEX IF NOT EXISTS idx_paper_signal_decisions_created_at ON paper_signal_decisions(created_at DESC);

