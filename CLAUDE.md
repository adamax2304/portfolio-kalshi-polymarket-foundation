# Kalshi + Polymarket Foundation System — Project Context

> **Для нового Claude**: полный контекст проекта.
> Пользователь — опытный разработчик, общение на русском, минимум вопросов.

---

## Что это за проект

**Production-grade инфраструктура** для multi-venue торговли на Kalshi + Polymarket.

Это **НЕ** простой бот. Это полноценная платформа с:
- Ingestion services (Rust, async Tokio)
- ClickHouse/Postgres/Redis storage
- Normalizer + Recorder pipeline
- Fair-value engine (Phase 2)
- Paper trading layer (Phase 2)
- Next.js dashboard + Prometheus/Grafana

**Язык**: Rust (все сервисы), TypeScript (dashboard), SQL (migrations)
**Инфраструктура**: Docker Compose

---

## Архитектура

```
services/                              — Rust micro-services
  market-gateway-polymarket/           — raw Polymarket + RTDS ingestion
  market-gateway-kalshi/               — raw Kalshi ingestion
  market-gateway-binance/              — reference price (BTC/ETH Binance futures)
  market-gateway-deribit/              — long-dated scanner foundation
  recorder/                            — пишет ClickHouse + Postgres + Redis
  normalizer/                          — venue → canonical schema
  metadata-service/                    — canonical contracts + mappings API
  signal-engine/                       — fair dislocation, cross-venue signals (Phase 2)
  fair-value-engine/                   — BTC/ETH fair probability (Phase 2)
  paper-execution/                     — paper trading simulation (Phase 2)
  analytics-engine/                    — replay + paper metrics (Phase 2)
  orchestrator/                        — координатор сервисов

apps/
  dashboard/                           — Next.js UI (Prometheus/Grafana интеграция)
  replay-runner/                       — replay исторических данных
  paper-trader/                        — paper trading runner

packages/                              — общие Rust крейты
infrastructure/
  docker-compose.yml                   — все сервисы + БД
  config-profiles/                     — calibration профили
  migrations/                          — ClickHouse + Postgres миграции

docs/
  architecture/                        — ARCHITECTURE_PHASE1.md, DATA_MODEL, KNOWN_GAPS
  runbooks/                            — RUN_LOCAL_PHASE1.md
  strategy-specs/                      — STRATEGY_SHORTDATED_CRYPTO_PHASE2.md
  replay-and-paper/                    — REPLAY, PAPER_TRADING, CALIBRATION guides
```

---

## Data Flow

```
Биржи (Polymarket/Kalshi/Binance/Deribit)
  ↓ WebSocket
market-gateways  →  Redis bus (raw events)
                 ↓
  normalizer     →  normalized events → Redis
  recorder       →  ClickHouse (raw/normalized history)
                    Postgres (metadata)
                    Redis (latest hot state)
                 ↓
  metadata-service  →  REST API
  signal-engine     →  fair value dislocations
  paper-execution   →  simulated orders (Phase 2)
```

---

## Фазы разработки

### Phase 1 (ГОТОВО): Foundation
- Ingestion: Polymarket, Kalshi, Binance, Deribit gateways
- Normalizer: venue → canonical schema
- Recorder: ClickHouse (market data) + Postgres (metadata) + Redis (bus)
- Replay export endpoints
- Next.js dashboard + Prometheus/Grafana
- Ops CLI
- Docker Compose infра

**Нет**: исполнения ордеров, fair-value логики, сигналов

### Phase 2 (В РАЗРАБОТКЕ): Research + Paper Loop
- `fair-value-engine`: краткосрочная fair probability для BTC/ETH (Polymarket)
- `signal-engine`: fair dislocation, cross-venue lead/lag, synthetic arb candidates
- `paper-execution`: queue/slippage/no-fill aware simulation
- `analytics-engine`: replay/paper metrics

---

## Запуск

### Первый раз (Docker)

```bash
cd "D:\Projects\kalshi+poly system"

# Скопируй .env.example в .env и заполни ключи
cp .env.example .env

# Поднять всю инфраструктуру
docker-compose up -d

# Проверить статус
docker-compose ps
```

### Локальный запуск сервисов (без Docker)

```bash
# Нужен: Rust, ClickHouse, Postgres, Redis

# Запустить конкретный сервис
cd services/market-gateway-polymarket
cargo run --release

# Или через workspace
cargo build --workspace --release
```

### Dashboard

```bash
cd apps/dashboard
npm install
npm run dev
# → http://localhost:3000
```

---

## Ключевые документы

Читай в таком порядке:
1. `docs/architecture/ARCHITECTURE_PHASE1.md` — общая архитектура
2. `docs/runbooks/RUN_LOCAL_PHASE1.md` — как запустить локально
3. `docs/architecture/DATA_MODEL_PHASE1.md` — схема данных
4. `docs/strategy-specs/STRATEGY_SHORTDATED_CRYPTO_PHASE2.md` — стратегия Phase 2
5. `docs/architecture/KNOWN_GAPS_PHASE1.md` — известные пробелы

---

## Отличие от poly-3rd-try-bot

| Характеристика | poly-3rd-try-bot | kalshi+poly system |
|---------------|------------------|--------------------|
| Сложность | Простой бот | Платформа |
| Язык | Rust + Python | Rust + TypeScript |
| Storage | Файловые логи | ClickHouse + Postgres + Redis |
| Live trading | ДА (сейчас) | НЕТ (Phase 1 = research only) |
| Цель | Быстрые деньги | Правильная архитектура |

---

## Статус на апрель 2026

- Phase 1 инфраструктура написана и протестирована
- Phase 2 research loop частично реализован
- Live торговля НЕ включена (намеренно — сначала research)

---

## Предпочтения пользователя
- Русский язык
- Минимум вопросов — делай сразу
- Можно запускать Bash, редактировать файлы
- Сохраняй этот CLAUDE.md актуальным после изменений
