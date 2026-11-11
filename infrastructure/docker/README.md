# Docker Notes

Phase 1 uses `docker-compose.yml` at repo root for infra stack:
- ClickHouse
- Postgres
- Redis
- Prometheus
- Grafana

Rust services are launched locally with `infrastructure/scripts/run-local-phase1.ps1`.

