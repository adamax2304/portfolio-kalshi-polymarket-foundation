INSERT INTO market_data.stream_health_history
  (service_name, venue, connection_status, last_message_ts, reconnect_count, dropped_message_estimate, current_latency_ms, health_state, observed_at_ts)
VALUES
  ('market-gateway-binance', 'binance', 'connected', toUnixTimestamp64Milli(now64()), 0, 0, 0, 'healthy', toUnixTimestamp64Milli(now64()));

