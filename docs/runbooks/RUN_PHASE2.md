# RUN_PHASE2

## 1) Start Phase 1 Foundation
```powershell
docker-compose up -d clickhouse postgres redis prometheus grafana
powershell -ExecutionPolicy Bypass -File infrastructure/scripts/run-local-phase1.ps1
```

## 2) Start Phase 2 Services
```powershell
powershell -ExecutionPolicy Bypass -File infrastructure/scripts/run-local-phase2.ps1
```

## 3) Replay BTC 15m
```powershell
cargo run -p replay-runner -- --from 2026-01-10T00:00:00Z --to 2026-01-10T02:00:00Z --underlying BTC --mode paper --speed x20
```

## 4) Paper Mode Monitoring
```powershell
cargo run -p paper-trader -- status
cargo run -p paper-trader -- positions
```

## 5) Ops CLI
```powershell
cargo run -p ops-cli -- signals stats
cargo run -p ops-cli -- paper status
cargo run -p ops-cli -- export analytics --run-id <run_id>
```
