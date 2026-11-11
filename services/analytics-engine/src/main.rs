use anyhow::Result;
use axum::{
    extract::{Path, State},
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use chrono::Utc;
use config::ServiceConfig;
use metrics::{AppMetrics, HealthPayload};
use serde::Serialize;
use serde_json::{json, Value};
use std::net::SocketAddr;
use storage::StorageClients;
use tower_http::trace::TraceLayer;
use tracing::info;

#[derive(Clone)]
struct AppState {
    metrics: AppMetrics,
    storage: StorageClients,
    config: ServiceConfig,
}

#[derive(Debug, Serialize)]
struct AnalyticsHealth {
    service: HealthPayload,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = ServiceConfig::from_env("analytics-engine", "PORT_ANALYTICS_ENGINE")?;
    logger::init(&config.service_name, &config.log_level);
    let metrics = AppMetrics::new(&config.prometheus_namespace, &config.service_name);
    let storage = StorageClients::connect(&config).await?;
    let state = AppState {
        metrics,
        storage,
        config,
    };

    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        .route("/analytics/overview", get(overview_handler))
        .route("/analytics/runs", get(runs_handler))
        .route("/analytics/run/:id", get(run_handler))
        .route("/analytics/rejected-signals", get(rejected_handler))
        .route("/analytics/pnl-by-underlying", get(pnl_by_underlying_handler))
        .route("/analytics/signal-performance", get(signal_performance_handler))
        .route("/analytics/per-regime", get(per_regime_handler))
        .with_state(state.clone())
        .layer(TraceLayer::new_for_http());
    let addr = SocketAddr::from(([0, 0, 0, 0], state.config.port));
    info!(address = %addr, "starting analytics-engine");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    Json(AnalyticsHealth {
        service: HealthPayload {
            service_name: state.config.service_name.clone(),
            instance_id: state.config.instance_id.clone(),
            build_version: state.config.build_version.clone(),
            config_fingerprint: state.config.fingerprint(),
            now_utc: Utc::now(),
            status: "ok".to_string(),
        },
    })
}

async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    state.metrics.render()
}

async fn overview_handler(State(state): State<AppState>) -> impl IntoResponse {
    let runs_count = scalar_count(&state, "SELECT COUNT(*) FROM replay_runs").await;
    let decisions_count = scalar_count(&state, "SELECT COUNT(*) FROM paper_signal_decisions").await;
    let positions_count = scalar_count(&state, "SELECT COUNT(*) FROM paper_positions").await;
    Json(json!({
        "runs": runs_count,
        "paper_decisions": decisions_count,
        "positions": positions_count,
        "generated_at": Utc::now()
    }))
}

async fn runs_handler(State(state): State<AppState>) -> impl IntoResponse {
    let rows = state
        .storage
        .postgres
        .query(
            "SELECT run_id, mode, underlying, from_ts, to_ts, speed, status, created_at FROM replay_runs ORDER BY created_at DESC LIMIT 200",
            &[],
        )
        .await;
    match rows {
        Ok(rows) => Json(json!({
            "data": rows.into_iter().map(|r| {
                json!({
                    "run_id": r.get::<_, String>(0),
                    "mode": r.get::<_, String>(1),
                    "underlying": r.get::<_, String>(2),
                    "from_ts": r.get::<_, chrono::DateTime<Utc>>(3),
                    "to_ts": r.get::<_, chrono::DateTime<Utc>>(4),
                    "speed": r.get::<_, String>(5),
                    "status": r.get::<_, String>(6),
                    "created_at": r.get::<_, chrono::DateTime<Utc>>(7),
                })
            }).collect::<Vec<Value>>()
        })),
        Err(err) => Json(json!({"data":[],"error":err.to_string()})),
    }
}

async fn run_handler(Path(id): Path<String>, State(state): State<AppState>) -> impl IntoResponse {
    let metric = state
        .storage
        .postgres
        .query_opt(
            "SELECT run_id, accepted_signals, rejected_signals, traded_signals, realized_pnl, created_at FROM replay_run_metrics WHERE run_id = $1",
            &[&id],
        )
        .await;
    match metric {
        Ok(Some(row)) => Json(json!({
            "run_id": row.get::<_, String>(0),
            "accepted_signals": row.get::<_, i64>(1),
            "rejected_signals": row.get::<_, i64>(2),
            "traded_signals": row.get::<_, i64>(3),
            "realized_pnl": row.get::<_, String>(4),
            "created_at": row.get::<_, chrono::DateTime<Utc>>(5),
        })),
        Ok(None) => Json(json!({"error":"run_not_found","run_id":id})),
        Err(err) => Json(json!({"error":err.to_string()})),
    }
}

async fn rejected_handler(State(state): State<AppState>) -> impl IntoResponse {
    let rows = state
        .storage
        .postgres
        .query(
            "SELECT signal_id, canonical_contract_key, payload_json, created_at FROM paper_signal_decisions \
             WHERE accepted = FALSE ORDER BY created_at DESC LIMIT 200",
            &[],
        )
        .await;
    match rows {
        Ok(rows) => Json(json!({
            "data": rows.into_iter().map(|r| {
                json!({
                    "signal_id": r.get::<_, String>(0),
                    "canonical_contract_key": r.get::<_, String>(1),
                    "payload_json": r.get::<_, Value>(2),
                    "created_at": r.get::<_, chrono::DateTime<Utc>>(3),
                })
            }).collect::<Vec<Value>>()
        })),
        Err(err) => Json(json!({"data":[],"error":err.to_string()})),
    }
}

async fn pnl_by_underlying_handler(State(state): State<AppState>) -> impl IntoResponse {
    let rows = state
        .storage
        .postgres
        .query("SELECT canonical_contract_key, realized_pnl FROM paper_positions", &[])
        .await;
    match rows {
        Ok(rows) => {
            let mut btc = 0.0f64;
            let mut eth = 0.0f64;
            let mut other = 0.0f64;
            for row in rows {
                let key: String = row.get(0);
                let pnl: String = row.get(1);
                let value = pnl.parse::<f64>().unwrap_or(0.0);
                let up = key.to_ascii_uppercase();
                if up.contains("BTC") {
                    btc += value;
                } else if up.contains("ETH") {
                    eth += value;
                } else {
                    other += value;
                }
            }
            Json(json!({
                "btc_realized_pnl": btc,
                "eth_realized_pnl": eth,
                "other_realized_pnl": other
            }))
        }
        Err(err) => Json(json!({"error":err.to_string()})),
    }
}

async fn signal_performance_handler(State(state): State<AppState>) -> impl IntoResponse {
    let rows = state
        .storage
        .postgres
        .query("SELECT accepted, payload_json FROM paper_signal_decisions ORDER BY created_at DESC LIMIT 5000", &[])
        .await;
    match rows {
        Ok(rows) => {
            let mut per_type: std::collections::BTreeMap<String, (u64, u64)> = std::collections::BTreeMap::new();
            for row in rows {
                let accepted: bool = row.get(0);
                let payload: Value = row.get(1);
                let signal_type = payload
                    .get("signal")
                    .and_then(|s| s.get("signal_type"))
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .to_string();
                let entry = per_type.entry(signal_type).or_insert((0, 0));
                if accepted {
                    entry.0 += 1;
                } else {
                    entry.1 += 1;
                }
            }
            Json(json!({
                "data": per_type.into_iter().map(|(k, (accepted, rejected))| {
                    json!({"signal_type":k, "accepted":accepted, "rejected":rejected})
                }).collect::<Vec<Value>>()
            }))
        }
        Err(err) => Json(json!({"error":err.to_string()})),
    }
}

async fn per_regime_handler(State(state): State<AppState>) -> impl IntoResponse {
    let rows = state
        .storage
        .postgres
        .query("SELECT payload_json FROM paper_signal_decisions ORDER BY created_at DESC LIMIT 5000", &[])
        .await;
    match rows {
        Ok(rows) => {
            let mut counts: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
            for row in rows {
                let payload: Value = row.get(0);
                let regimes = payload
                    .get("signal")
                    .and_then(|s| s.get("market_regime_flags"))
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                for regime in regimes {
                    let key = regime.as_str().unwrap_or("UNKNOWN").to_string();
                    *counts.entry(key).or_insert(0) += 1;
                }
            }
            Json(json!({
                "data": counts.into_iter().map(|(k,v)| json!({"regime":k, "count":v})).collect::<Vec<Value>>()
            }))
        }
        Err(err) => Json(json!({"error":err.to_string()})),
    }
}

async fn scalar_count(state: &AppState, query: &str) -> i64 {
    match state.storage.postgres.query_one(query, &[]).await {
        Ok(row) => row.get::<_, i64>(0),
        Err(_) => 0,
    }
}
