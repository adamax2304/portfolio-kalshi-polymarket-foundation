use anyhow::Result;
use axum::{
    extract::{Path, State},
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use chrono::Utc;
use config::{read_var, ServiceConfig};
use metrics::{AppMetrics, HealthPayload};
use pricing::{compute_fair_value, FairValueEvent, FairValueInput, FairValueModelConfig};
use rust_decimal::prelude::FromStr;
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::{json, Value};
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};
use storage::StorageClients;
use tokio::sync::RwLock;
use tower_http::trace::TraceLayer;
use tracing::{info, warn};
use utils::deterministic_id;

#[derive(Clone)]
struct AppState {
    metrics: AppMetrics,
    storage: StorageClients,
    config: ServiceConfig,
    fair_stream_key: String,
    latest_fair: Arc<RwLock<HashMap<String, FairValueEvent>>>,
    model_config: FairValueModelConfig,
}

#[derive(Debug, Serialize)]
struct FairEngineHealth {
    service: HealthPayload,
    contracts_with_fair: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = ServiceConfig::from_env("fair-value-engine", "PORT_FAIR_VALUE_ENGINE")?;
    logger::init(&config.service_name, &config.log_level);
    let metrics = AppMetrics::new(&config.prometheus_namespace, &config.service_name);
    let storage = StorageClients::connect(&config).await?;
    let fair_stream_key = read_var("REDIS_STREAM_FAIR_VALUES", Some("bus.fair_values"))?;
    let state = AppState {
        metrics,
        storage,
        config,
        fair_stream_key,
        latest_fair: Arc::new(RwLock::new(HashMap::new())),
        model_config: FairValueModelConfig::default(),
    };
    tokio::spawn(run_fair_loop(state.clone()));

    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        .route("/fair/list", get(list_fair_handler))
        .route("/fair/latest/:contract", get(latest_fair_handler))
        .with_state(state.clone())
        .layer(TraceLayer::new_for_http());

    let addr = SocketAddr::from(([0, 0, 0, 0], state.config.port));
    info!(address = %addr, "starting fair-value-engine");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn run_fair_loop(state: AppState) {
    loop {
        let _ = state.storage.set_service_heartbeat(&state.config.service_name).await;
        match state.storage.list_cache_keys("latest:normalized:*").await {
            Ok(keys) => {
                for key in keys {
                    if let Err(err) = process_contract(&state, &key).await {
                        warn!(error = %err, key, "fair value processing failed");
                        state
                            .metrics
                            .error_count
                            .with_label_values(&[&state.config.service_name, "fair_compute"])
                            .inc();
                    }
                }
            }
            Err(err) => {
                warn!(error = %err, "failed listing normalized cache keys");
            }
        }
        tokio::time::sleep(Duration::from_millis(750)).await;
    }
}

async fn process_contract(state: &AppState, normalized_key: &str) -> Result<()> {
    let Some(raw) = state.storage.get_cached(normalized_key).await? else {
        return Ok(());
    };
    let normalized: Value = serde_json::from_str(&raw)?;
    let canonical_contract_key = normalized
        .get("canonical_contract_key")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let underlying = detect_underlying(canonical_contract_key);
    if underlying != "BTC" && underlying != "ETH" {
        return Ok(());
    }
    let reference_symbol = if underlying == "BTC" { "BTCUSDT" } else { "ETHUSDT" };
    let Some(reference_raw) = state
        .storage
        .get_cached(&format!("latest:reference:{reference_symbol}"))
        .await?
    else {
        return Ok(());
    };
    let reference: Value = serde_json::from_str(&reference_raw)?;
    let spot_price = parse_decimal(reference.get("mid_price")).unwrap_or_else(|| parse_decimal(reference.get("last_price")).unwrap_or(Decimal::ZERO));
    if spot_price <= Decimal::ZERO {
        return Ok(());
    }

    let market_implied_prob = implied_probability(
        parse_decimal(normalized.get("best_bid")),
        parse_decimal(normalized.get("best_ask")),
        parse_decimal(normalized.get("midpoint")),
    );

    let venue_market_id = normalized
        .get("venue_market_id")
        .and_then(Value::as_str)
        .unwrap_or(canonical_contract_key);
    let strike_price = infer_strike(venue_market_id).unwrap_or(spot_price);
    let time_to_expiry_seconds = infer_time_to_expiry_seconds(normalized.get("metadata_json")).unwrap_or(900);
    let spread_bps = parse_decimal(normalized.get("spread")).unwrap_or(Decimal::new(15, 1)) * Decimal::new(100, 0);
    let depth_score = Decimal::new(55, 2);
    let fair_input = FairValueInput {
        observed_at: Utc::now(),
        canonical_contract_key: canonical_contract_key.to_string(),
        underlying: underlying.to_string(),
        spot_price,
        strike_price,
        time_to_expiry_seconds,
        realized_vol_30s: Decimal::new(12, 3),
        realized_vol_60s: Decimal::new(11, 3),
        realized_vol_180s: Decimal::new(10, 3),
        realized_vol_300s: Decimal::new(9, 3),
        short_momentum_bps: Decimal::new(4, 0),
        jumpiness_score: Decimal::new(20, 2),
        spread_bps,
        depth_score,
        l1_imbalance: Decimal::new(4, 2),
        l2_imbalance: Decimal::new(3, 2),
        microprice_deviation_bps: Decimal::new(2, 1),
        trade_pressure: Decimal::new(5, 2),
        reprice_speed: Decimal::new(15, 2),
        update_frequency_hz: Decimal::new(8, 0),
        reference_acceleration: Decimal::new(5, 2),
    };
    let fair = compute_fair_value(&fair_input, &state.model_config)?;
    let event = FairValueEvent {
        event_id: deterministic_id(&format!(
            "fair:{}:{}:{}",
            canonical_contract_key,
            fair_input.observed_at.timestamp_millis(),
            fair.input_snapshot_hash
        )),
        observed_at: fair_input.observed_at,
        canonical_contract_key: fair_input.canonical_contract_key.clone(),
        underlying: fair_input.underlying.clone(),
        source_venues: vec![
            normalized
                .get("venue")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string(),
        ],
        market_implied_prob,
        spot_price,
        strike_price,
        time_to_expiry_seconds,
        fair,
    };

    let payload = serde_json::to_string(&event)?;
    state
        .storage
        .push_bus_event(&state.fair_stream_key, &payload)
        .await?;
    state
        .storage
        .cache_latest(&format!("latest:fair:{}", event.canonical_contract_key), &payload)
        .await?;
    state
        .metrics
        .message_count
        .with_label_values(&[&state.config.service_name, "internal", "fair_value"])
        .inc();
    state
        .latest_fair
        .write()
        .await
        .insert(event.canonical_contract_key.clone(), event);
    Ok(())
}

fn parse_decimal(v: Option<&Value>) -> Option<Decimal> {
    match v {
        Some(Value::String(s)) => Decimal::from_str(s).ok(),
        Some(Value::Number(n)) => Decimal::from_str(&n.to_string()).ok(),
        _ => None,
    }
}

fn implied_probability(bid: Option<Decimal>, ask: Option<Decimal>, midpoint: Option<Decimal>) -> Decimal {
    let mut p = midpoint.or_else(|| match (bid, ask) {
        (Some(b), Some(a)) => Some((b + a) / Decimal::new(2, 0)),
        _ => bid.or(ask),
    });
    if let Some(mut val) = p.take() {
        if val > Decimal::ONE {
            val /= Decimal::new(100, 0);
        }
        return val.max(Decimal::new(1, 2)).min(Decimal::new(99, 2));
    }
    Decimal::new(50, 2)
}

fn detect_underlying(contract_key: &str) -> &'static str {
    let up = contract_key.to_ascii_uppercase();
    if up.contains("ETH") {
        "ETH"
    } else if up.contains("BTC") || up.contains("XBT") {
        "BTC"
    } else {
        "UNKNOWN"
    }
}

fn infer_strike(market_id: &str) -> Option<Decimal> {
    let mut max_number: Option<i64> = None;
    for token in market_id.split(|c: char| !c.is_ascii_digit()) {
        if token.len() >= 4 {
            if let Ok(v) = token.parse::<i64>() {
                max_number = Some(max_number.map_or(v, |m| m.max(v)));
            }
        }
    }
    max_number.map(|v| Decimal::new(v, 0))
}

fn infer_time_to_expiry_seconds(metadata: Option<&Value>) -> Option<i64> {
    let Some(meta) = metadata else {
        return None;
    };
    let expiration = meta.get("expiration_ts").and_then(Value::as_str)?;
    let parsed = chrono::DateTime::parse_from_rfc3339(expiration).ok()?;
    let diff = parsed.with_timezone(&Utc) - Utc::now();
    Some(diff.num_seconds().max(1))
}

async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    Json(FairEngineHealth {
        service: HealthPayload {
            service_name: state.config.service_name.clone(),
            instance_id: state.config.instance_id.clone(),
            build_version: state.config.build_version.clone(),
            config_fingerprint: state.config.fingerprint(),
            now_utc: Utc::now(),
            status: "ok".to_string(),
        },
        contracts_with_fair: state.latest_fair.read().await.len(),
    })
}

async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    state.metrics.render()
}

async fn list_fair_handler(State(state): State<AppState>) -> impl IntoResponse {
    let list: Vec<FairValueEvent> = state.latest_fair.read().await.values().cloned().collect();
    Json(json!({ "data": list }))
}

async fn latest_fair_handler(Path(contract): Path<String>, State(state): State<AppState>) -> impl IntoResponse {
    let data = state.latest_fair.read().await.get(&contract).cloned();
    Json(json!({ "data": data }))
}
