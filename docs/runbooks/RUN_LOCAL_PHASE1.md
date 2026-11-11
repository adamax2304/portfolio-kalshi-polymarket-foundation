# RUN_LOCAL_PHASE1

## Prerequisites
- Rust toolchain (stable)
- Docker + docker-compose
- Node.js 20+ (for dashboard)

## 1) Setup Env
1. Copy `.env.example` to `.env`.
2. Fill placeholders if needed (keys can stay `REPLACE_ME` in mock mode).
3. Keep `MOCK_STREAMS=true` for local deterministic ingestion.

## 2) Start Infra
```powershell
docker-compose up -d clickhouse postgres redis prometheus grafana
```

## 3) Start Rust Services
```powershell
powershell -ExecutionPolicy Bypass -File infrastructure/scripts/run-local-phase1.ps1
```

## 4) Seed Dev Metadata (optional)
```powershell
powershell -ExecutionPolicy Bypass -File infrastructure/scripts/seed-dev.ps1
```

## 5) Start Dashboard
```powershell
cd apps/dashboard
npm install
npm run dev
```

## 6) Smoke Test
```powershell
powershell -ExecutionPolicy Bypass -File tests/smoke/phase1_smoke.ps1
```

