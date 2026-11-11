use anyhow::{anyhow, Result};
use axum::{extract::State, response::IntoResponse, routing::get, Json, Router};
use chrono::{DateTime, Utc};
use config::ServiceConfig;
use decimal::parse_decimal;
use metrics::{AppMetrics, HealthPayload};
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::{json, Value};
use shared_types::{
    EventType, HealthState, MappingState, NormalizedMarketEvent, RawVenueEvent, ReferencePriceEvent, Venue,
};
use std::{net::SocketAddr, sync::Arc, time::Duration};
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
    stats: Arc<RwLock<NormalizerStats>>,
    raw_consumer_key: String,
}

#[derive(Debug, Clone, Serialize, Default)]
struct NormalizerStats {
    processed: u64,
    normalized: u64,
    unresolved_mappings: u64,
    reference_events: u64,
    last_error: Option<String>,
    last_event_ts: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
struct NormalizerHealthResponse {
    service: HealthPayload,
    stats: NormalizerStats,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = ServiceConfig::from_env("normalizer", "PORT_NORMALIZER")?;
    logger::init(&config.service_name, &config.log_level);
    let metrics = AppMetrics::new(&config.prometheus_namespace, &config.service_name);
    let storage = StorageClients::connect(&config).await?;
    let state = AppState {
        metrics,
        storage: storage.clone(),
        config,
        stats: Arc::new(RwLock::new(NormalizerStats::default())),
        raw_consumer_key: format!("{}.normalizer", storage.raw_stream_key),
    };
    tokio::spawn(run_normalizer(state.clone()));

    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        .route("/stats", get(stats_handler))
        .with_state(state.clone())
        .layer(TraceLayer::new_for_http());
    let addr = SocketAddr::from(([0, 0, 0, 0], state.config.port));
    info!(address = %addr, "starting normalizer");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn run_normalizer(state: AppState) {
    loop {
        let _ = state.storage.set_service_heartbeat(&state.config.service_name).await;
        match state.storage.pop_bus_event(&state.raw_consumer_key, 1.0).await {
            Ok(Some(payload)) => match serde_json::from_str::<RawVenueEvent>(&payload) {
                Ok(raw) => {
                    if let Err(err) = handle_raw_event(&state, raw).await {
                        register_error(&state, format!("normalization error: {err}")).await;
                    }
                }
                Err(err) => register_error(&state, format!("raw decode error: {err}")).await,
            },
            Ok(None) => {}
            Err(err) => register_error(&state, format!("stream pop error: {err}")).await,
        }
        tokio::time::sleep(Duration::from_millis(80)).await;
    }
}

async fn handle_raw_event(state: &AppState, raw: RawVenueEvent) -> Result<()> {
    {
        let mut stats = state.stats.write().await;
        stats.processed += 1;
        stats.last_event_ts = Some(Utc::now());
    }

    match raw.venue {
        Venue::Binance => {
            let reference = normalize_reference(raw)?;
            let payload = serde_json::to_string(&reference)?;
            state
                .storage
                .push_bus_event(&state.storage.reference_stream_key, &payload)
                .await?;
            state
                .storage
                .cache_latest(&format!("latest:reference:{}", reference.symbol), &payload)
                .await?;
            {
                let mut stats = state.stats.write().await;
                stats.reference_events += 1;
            }
            state
                .metrics
                .message_count
                .with_label_values(&[&state.config.service_name, "binance", "reference"])
                .inc();
        }
        Venue::Polymarket | Venue::Kalshi | Venue::Deribit => {
            let maybe_normalized = normalize_market_event(&raw).await?;
            match maybe_normalized {
                Some(normalized) => {
                    let payload = serde_json::to_string(&normalized)?;
                    state
                        .storage
                        .push_bus_event(&state.storage.normalized_stream_key, &payload)
                        .await?;
                    let contract_payload = serde_json::to_string(&json!({
                        "canonical_contract_key": normalized.canonical_contract_key,
                        "venue_market_id": normalized.venue_market_id,
                        "schema_version": 1,
                        "status": "active"
                    }))?;
                    state
                        .storage
                        .upsert_canonical_contract(
                            &normalized.canonical_contract_key,
                            &format!("{:?}", normalized.venue).to_ascii_lowercase(),
                            &normalized.venue_market_id,
                            &contract_payload,
                        )
                        .await?;
                    state
                        .storage
                        .cache_latest(
                            &format!("latest:normalized:{}", normalized.canonical_contract_key),
                            &payload,
                        )
                        .await?;
                    {
                        let mut stats = state.stats.write().await;
                        stats.normalized += 1;
                    }
                    state
                        .metrics
                        .message_count
                        .with_label_values(&[&state.config.service_name, "internal", "normalized"])
                        .inc();
                }
                None => {
                    {
                        let mut stats = state.stats.write().await;
                        stats.unresolved_mappings += 1;
                    }
                    state
                        .metrics
                        .error_count
                        .with_label_values(&[&state.config.service_name, "unresolved_mapping"])
                        .inc();
                    write_unresolved_audit(state, &raw).await?;
                }
            }
        }
        Venue::Internal => {}
    }
    Ok(())
}

fn normalize_reference(raw: RawVenueEvent) -> Result<ReferencePriceEvent> {
    let symbol = raw
        .payload_json
        .get("symbol")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("missing symbol in binance payload"))?;
    let bid = raw.payload_json.get("bid_price").and_then(Value::as_str).unwrap_or("0");
    let ask = raw.payload_json.get("ask_price").and_then(Value::as_str).unwrap_or("0");
    let best_bid = parse_decimal(bid).ok();
    let best_ask = parse_decimal(ask).ok();
    let mid_price = match (best_bid, best_ask) {
        (Some(b), Some(a)) => Some((b + a) / Decimal::from(2)),
        _ => None,
    };

    Ok(ReferencePriceEvent {
        reference_event_id: deterministic_id(&format!("ref:{}:{}", raw.event_id, symbol)),
        source: "binance".to_string(),
        symbol: symbol.to_string(),
        event_ts: raw.source_ts.unwrap_or(raw.received_at_ts),
        best_bid,
        best_ask,
        last_price: raw
            .payload_json
            .get("last_price")
            .and_then(Value::as_str)
            .and_then(|x| parse_decimal(x).ok()),
        mid_price,
        metadata_json: json!({
            "raw_event_id": raw.event_id,
            "schema_version": 1
        }),
    })
}

async fn normalize_market_event(raw: &RawVenueEvent) -> Result<Option<NormalizedMarketEvent>> {
    let venue_market_id = match raw.venue {
        Venue::Polymarket => raw.payload_json.get("market").and_then(Value::as_str),
        Venue::Kalshi => raw.payload_json.get("market_ticker").and_then(Value::as_str),
        Venue::Deribit => raw.payload_json.get("instrument_name").and_then(Value::as_str),
        _ => None,
    };
    let venue_market_id = match venue_market_id {
        Some(v) if !v.trim().is_empty() => v,
        _ => return Ok(None),
    };

    let (best_bid, best_ask, last_trade_price) = match raw.venue {
        Venue::Polymarket => (
            raw.payload_json
                .get("best_bid")
                .and_then(Value::as_str)
                .and_then(|v| parse_decimal(v).ok()),
            raw.payload_json
                .get("best_ask")
                .and_then(Value::as_str)
                .and_then(|v| parse_decimal(v).ok()),
            raw.payload_json
                .get("last_trade_price")
                .and_then(Value::as_str)
                .and_then(|v| parse_decimal(v).ok()),
        ),
        Venue::Kalshi => (
            raw.payload_json
                .get("yes_bid")
                .and_then(Value::as_str)
                .and_then(|v| parse_decimal(v).ok()),
            raw.payload_json
                .get("yes_ask")
                .and_then(Value::as_str)
                .and_then(|v| parse_decimal(v).ok()),
            None,
        ),
        Venue::Deribit => (
            None,
            None,
            raw.payload_json
                .get("mark_price")
                .and_then(Value::as_str)
                .and_then(|v| parse_decimal(v).ok()),
        ),
        _ => (None, None, None),
    };

    let spread = match (best_bid, best_ask) {
        (Some(bid), Some(ask)) => Some(ask - bid),
        _ => None,
    };
    let midpoint = match (best_bid, best_ask) {
        (Some(bid), Some(ask)) => Some((bid + ask) / Decimal::from(2)),
        _ => None,
    };
    let canonical_contract_key = format!("{:?}:{}", raw.venue, venue_market_id).to_ascii_lowercase();
    Ok(Some(NormalizedMarketEvent {
        normalized_event_id: deterministic_id(&format!("n:{}:{}", raw.event_id, canonical_contract_key)),
        canonical_contract_key,
        venue: raw.venue.clone(),
        venue_market_id: venue_market_id.to_string(),
        event_type: EventType::OrderbookDelta,
        event_ts: raw.source_ts.unwrap_or(raw.received_at_ts),
        best_bid,
        best_ask,
        last_trade_price,
        last_trade_size: None,
        spread,
        midpoint,
        metadata_json: json!({
            "source_event_id": raw.event_id,
            "mapping_state": MappingState::Resolved,
            "schema_version": 1
        }),
    }))
}

async fn write_unresolved_audit(state: &AppState, raw: &RawVenueEvent) -> Result<()> {
    let payload = serde_json::to_string(&json!({
        "reason": "unresolved_mapping",
        "raw_event_id": raw.event_id,
        "venue": raw.venue,
        "event_type": raw.event_type,
        "payload": raw.payload_json
    }))?;
    state
        .storage
        .postgres
        .execute(
            "INSERT INTO audit_events (event_type, payload_json, created_at) VALUES ($1, $2::jsonb, NOW())",
            &[&"unresolved_mapping", &payload],
        )
        .await?;
    Ok(())
}

async fn register_error(state: &AppState, message: String) {
    warn!(error = %message, "normalizer failure");
    let mut stats = state.stats.write().await;
    stats.last_error = Some(message);
    state
        .metrics
        .error_count
        .with_label_values(&[&state.config.service_name, "normalization"])
        .inc();
}

async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    let stats = state.stats.read().await.clone();
    let health = if stats.last_error.is_some() {
        HealthState::Degraded
    } else {
        HealthState::Healthy
    };
    Json(NormalizerHealthResponse {
        service: HealthPayload {
            service_name: state.config.service_name.clone(),
            instance_id: state.config.instance_id.clone(),
            build_version: state.config.build_version.clone(),
            config_fingerprint: state.config.fingerprint(),
            now_utc: Utc::now(),
            status: format!("{:?}", health).to_ascii_lowercase(),
        },
        stats,
    })
}

async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    state.metrics.render()
}

async fn stats_handler(State(state): State<AppState>) -> impl IntoResponse {
    Json(state.stats.read().await.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use shared_types::{EventType, IngestionStatus, Venue};

    #[tokio::test]
    async fn normalizes_polymarket_payload() {
        let raw = RawVenueEvent {
            event_id: "raw-1".to_string(),
            venue: Venue::Polymarket,
            event_type: EventType::OrderbookDelta,
            received_at_ts: Utc::now(),
            source_ts: None,
            payload_json: json!({
                "market": "POLY_BTC_2026",
                "best_bid": "0.45",
                "best_ask": "0.47",
                "last_trade_price": "0.46"
            }),
            connection_id: "conn".to_string(),
            sequence_hint: None,
            ingestion_status: IngestionStatus::Accepted,
        };
        let normalized = normalize_market_event(&raw)
            .await
            .expect("normalization should succeed")
            .expect("mapping should be resolved");
        assert_eq!(normalized.venue_market_id, "POLY_BTC_2026");
        assert_eq!(normalized.canonical_contract_key, "polymarket:poly_btc_2026");
    }
}
