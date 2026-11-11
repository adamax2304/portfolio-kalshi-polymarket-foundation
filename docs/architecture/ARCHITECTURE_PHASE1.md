# ARCHITECTURE_PHASE1

## Overview
Phase 1 builds a replay-first, deterministic foundation for Kalshi + Polymarket ingestion and normalization without any strategy or order execution logic.

## Services
- `market-gateway-polymarket`: raw Polymarket + RTDS ingestion, reconnect/resubscribe loop, health/metrics, no orders.
- `market-gateway-kalshi`: raw Kalshi ingestion + future queue-position adapter placeholders, no orders.
- `market-gateway-binance`: reference price ingestion for future fair-value.
- `market-gateway-deribit`: instrument metadata + base feed for long-dated scanner foundation.
- `recorder`: persists raw/normalized/reference events to ClickHouse, metadata to Postgres, latest state to Redis, replay export API.
- `normalizer`: maps venue-specific payloads to canonical schemas; unresolved mappings are flagged and audited.
- `metadata-service`: canonical contracts, mappings, runtime/service metadata API.

## Data Flow
1. Gateways ingest venue events.
2. Gateways publish raw events to Redis bus.
3. Recorder consumes raw stream and writes immutable event history.
4. Normalizer consumes raw stream replica and emits normalized/reference events.
5. Recorder persists normalized/reference streams.
6. Metadata-service serves canonical contract/mapping/audit views.
7. Dashboard and Ops CLI consume service APIs.

## Storage Design
- ClickHouse: append-only market/replay data.
- Postgres: canonical metadata, service registry, runtime config, audits, schema versions.
- Redis: bus transport, dedupe keys, latest hot state, heartbeats.

## Future Extension Points
- `FairValueProvider`
- `SignalProducer`
- `ExecutionAdapter`
- `RiskEvaluator`
- `ReplayDataProvider`
- `ReferencePriceProvider`
- `ContractMetadataProvider`

