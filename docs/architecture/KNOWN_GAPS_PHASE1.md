# KNOWN_GAPS_PHASE1

## Intentionally Not Built Yet
- No execution engine.
- No live order placement.
- No risk engine logic.
- No strategy/fair-value/signal decision logic.
- No full backtest engine.
- No position management.

## Current Risks
- Redis list bus is simple and not yet consumer-group backed.
- Recorder/normalizer retries are basic (no advanced dead-letter queues yet).
- Schema evolution policy exists but migration governance is still lightweight.
- Deribit/Kalshi/Polymarket parsers are phase-1 minimal and need richer DTO coverage.
- No auth hardening for internal HTTP APIs yet.

## Phase 2 Dependencies
- Robust event bus abstraction (Kafka/NATS/Redis Streams consumer groups).
- Advanced mapping lifecycle tooling and operator workflows.
- Fair-value + signal + execution + risk module implementation.
- Backfill and controlled resync orchestration.
- Stronger replay bundling and deterministic simulation harness.

