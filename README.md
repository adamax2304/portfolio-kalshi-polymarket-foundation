# Kalshi + Polymarket Foundation (Phase 1)

Production-grade foundation layer for multi-agent trading system development.

This phase includes:
- Rust ingestion services (Polymarket, Kalshi, Binance, Deribit)
- Normalizer + Recorder + Metadata Service
- ClickHouse/Postgres/Redis storage layer
- Replay export endpoints
- Ops CLI + Next.js dashboard
- Prometheus/Grafana observability
- Docker-compose infra + migration/seed scripts

Phase 2 research/paper loop includes:
- `fair-value-engine` (short-dated BTC/ETH fair probability v1)
- `signal-engine` (fair dislocation, cross-venue lead/lag, synthetic arb candidate)
- `paper-execution` (queue/slippage/no-fill aware simulation + paper risk gates)
- `replay-runner` and `paper-trader` apps
- `analytics-engine` with replay/paper metrics endpoints
- calibration profiles in `infrastructure/config-profiles`

No live order placement is implemented.

See:
- `docs/architecture/ARCHITECTURE_PHASE1.md`
- `docs/runbooks/RUN_LOCAL_PHASE1.md`
- `docs/architecture/DATA_MODEL_PHASE1.md`
- `docs/architecture/KNOWN_GAPS_PHASE1.md`
- `docs/strategy-specs/STRATEGY_SHORTDATED_CRYPTO_PHASE2.md`
- `docs/replay-and-paper/REPLAY_WORKFLOW_PHASE2.md`
- `docs/replay-and-paper/PAPER_TRADING_PHASE2.md`
- `docs/replay-and-paper/CALIBRATION_GUIDE_PHASE2.md`
