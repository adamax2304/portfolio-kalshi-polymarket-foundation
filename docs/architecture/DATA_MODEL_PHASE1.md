# DATA_MODEL_PHASE1

## RawVenueEvent
- `event_id`
- `venue`
- `event_type`
- `received_at_ts`
- `source_ts`
- `payload_json`
- `connection_id`
- `sequence_hint`
- `ingestion_status`

## NormalizedMarketEvent
- `normalized_event_id`
- `canonical_contract_key`
- `venue`
- `venue_market_id`
- `event_type`
- `event_ts`
- `best_bid`
- `best_ask`
- `last_trade_price`
- `last_trade_size`
- `spread`
- `midpoint`
- `metadata_json`

## CanonicalContract
- `canonical_contract_key`
- `venue`
- `venue_market_id`
- `underlying`
- `contract_family`
- `strike`
- `threshold_type`
- `expiration_ts`
- `quote_currency`
- `settlement_source`
- `settlement_window_seconds`
- `tick_size`
- `fee_profile_json`
- `reward_profile_json`
- `status`
- `metadata_json`

## ReferencePriceEvent
- `reference_event_id`
- `source`
- `symbol`
- `event_ts`
- `best_bid`
- `best_ask`
- `last_price`
- `mid_price`
- `metadata_json`

## StreamHealth
- `service_name`
- `venue`
- `connection_status`
- `last_message_ts`
- `reconnect_count`
- `dropped_message_estimate`
- `current_latency_ms`
- `health_state`

## Replay Exports
- `/export/raw`: raw events by interval.
- `/export/normalized`: normalized events by interval.
- `/export/reference`: reference prices by interval.
- `/export/bundle`: merged replay bundle.

