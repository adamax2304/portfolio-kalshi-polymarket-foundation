INSERT INTO runtime_config (key, value_json)
VALUES
  ('normalizer.schema_version', '{"value":1}'::jsonb),
  ('recorder.snapshot_interval_seconds', '{"value":30}'::jsonb),
  ('feature.live_trading_enabled', '{"value":false}'::jsonb)
ON CONFLICT (key) DO UPDATE SET value_json = EXCLUDED.value_json, updated_at = NOW();

INSERT INTO canonical_contracts (
  canonical_contract_key,
  venue,
  venue_market_id,
  underlying,
  contract_family,
  expiration_ts,
  quote_currency,
  tick_size,
  metadata_json
)
VALUES
  (
    'polymarket:poly_btc_2026',
    'polymarket',
    'POLY_BTC_2026',
    'BTC',
    'binary',
    NOW() + INTERVAL '1 day',
    'USD',
    0.01,
    '{"seed":true,"mapping_state":"resolved"}'::jsonb
  )
ON CONFLICT (canonical_contract_key) DO NOTHING;

