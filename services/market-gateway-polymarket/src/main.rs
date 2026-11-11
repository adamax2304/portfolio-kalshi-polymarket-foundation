use anyhow::{Context, Result};
use axum::{extract::State, response::IntoResponse, routing::get, Json, Router};
use chrono::Utc;
use config::ServiceConfig;
use futures::StreamExt;
use metrics::{AppMetrics, HealthPayload};
use serde::Serialize;
use serde_json::{json, Value};
use shared_types::{ConnectionStatus, HealthState, IngestionStatus, RawVenueEvent, StreamHealth, Venue};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use storage::StorageClients;
use tokio::sync::RwLock;
use tokio_tungstenite::connect_async;
use tower_http::trace::TraceLayer;
use tracing::{error, info, warn};
use venue_polymarket::{to_raw_event, PolymarketOrderbookEventDto};

#[derive(Clone)]
struct AppState {
    metrics: AppMetrics,
    stream_health: Arc<RwLock<StreamHealth>>,
    rtds_health: Arc<RwLock<StreamHealth>>,
    storage: StorageClients,
    config: ServiceConfig,
}

#[derive(Debug, Serialize)]
struct GatewayHealthResponse {
    service: HealthPayload,
    stream: StreamHealth,
    rtds_stream: StreamHealth,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = ServiceConfig::from_env("market-gateway-polymarket", "PORT_GATEWAY_POLYMARKET")?;
    logger::init(&config.service_name, &config.log_level);
    let metrics = AppMetrics::new(&config.prometheus_namespace, &config.service_name);
    let storage = StorageClients::connect(&config).await?;
    let stream_health = Arc::new(RwLock::new(StreamHealth {
        service_name: config.service_name.clone(),
        venue: Venue::Polymarket,
        connection_status: ConnectionStatus::Connecting,
        last_message_ts: None,
        reconnect_count: 0,
        dropped_message_estimate: 0,
        current_latency_ms: None,
        health_state: HealthState::Degraded,
    }));
    let rtds_health = Arc::new(RwLock::new(StreamHealth {
        service_name: format!("{}-rtds", config.service_name),
        venue: Venue::Polymarket,
        connection_status: ConnectionStatus::Connecting,
        last_message_ts: None,
        reconnect_count: 0,
        dropped_message_estimate: 0,
        current_latency_ms: None,
        health_state: HealthState::Degraded,
    }));
    let state = AppState {
        metrics,
        stream_health,
        rtds_health,
        storage,
        config,
    };
    tokio::spawn(run_polymarket_stream(state.clone()));
    tokio::spawn(run_rtds_stream(state.clone()));

    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        .with_state(state.clone())
        .layer(TraceLayer::new_for_http());
    let addr = SocketAddr::from(([0, 0, 0, 0], state.config.port));
    info!(address = %addr, "starting market-gateway-polymarket");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn run_polymarket_stream(state: AppState) {
    let use_mock = std::env::var("MOCK_STREAMS")
        .unwrap_or_else(|_| "true".to_string())
        .eq_ignore_ascii_case("true");
    let connection_id = format!("polymarket-main-{}", state.config.instance_id);

    loop {
        let _ = state.storage.set_service_heartbeat(&state.config.service_name).await;
        if use_mock {
            update_conn_state(&state.stream_health, ConnectionStatus::Connected, HealthState::Healthy).await;
            let dto = PolymarketOrderbookEventDto {
                market: "POLY_BTC_2026".to_string(),
                token_id: Some("token-yes".to_string()),
                best_bid: Some("0.47".to_string()),
                best_ask: Some("0.49".to_string()),
                last_trade_price: Some("0.48".to_string()),
                seq: Some(Utc::now().timestamp_millis().to_string()),
                event_ts_ms: Some(Utc::now().timestamp_millis()),
            };
            if let Err(err) = publish_raw_event(&state, to_raw_event(&dto, &connection_id), "main").await {
                error!(error = %err, "failed to publish polymarket mock event");
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }

        update_conn_state(&state.stream_health, ConnectionStatus::Connecting, HealthState::Degraded).await;
        match connect_async(&state.config.venue.polymarket_ws_url).await {
            Ok((mut ws_stream, _)) => {
                update_conn_state(&state.stream_health, ConnectionStatus::Connected, HealthState::Healthy).await;
                while let Some(msg_result) = ws_stream.next().await {
                    match msg_result {
                        Ok(msg) if msg.is_text() => {
                            let raw_json: Value = serde_json::from_str(&msg.into_text().unwrap_or_default())
                                .unwrap_or_else(|_| json!({}));
                            let dto = PolymarketOrderbookEventDto {
                                market: raw_json
                                    .get("market")
                                    .and_then(Value::as_str)
                                    .unwrap_or("unknown_market")
                                    .to_string(),
                                token_id: raw_json.get("asset_id").and_then(Value::as_str).map(str::to_string),
                                best_bid: raw_json.get("best_bid").and_then(Value::as_str).map(str::to_string),
                                best_ask: raw_json.get("best_ask").and_then(Value::as_str).map(str::to_string),
                                last_trade_price: raw_json
                                    .get("price")
                                    .and_then(Value::as_str)
                                    .map(str::to_string),
                                seq: raw_json.get("seq").and_then(Value::as_str).map(str::to_string),
                                event_ts_ms: raw_json.get("timestamp").and_then(Value::as_i64),
                            };
                            if let Err(err) =
                                publish_raw_event(&state, to_raw_event(&dto, &connection_id), "main").await
                            {
                                error!(error = %err, "failed to publish polymarket event");
                            }
                        }
                        Ok(_) => {}
                        Err(err) => {
                            warn!(error = %err, "polymarket ws read error");
                            break;
                        }
                    }
                }
            }
            Err(err) => warn!(error = %err, "polymarket ws connect failed"),
        }
        increment_reconnect(&state.stream_health, &state, "polymarket").await;
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn run_rtds_stream(state: AppState) {
    let use_mock = std::env::var("MOCK_STREAMS")
        .unwrap_or_else(|_| "true".to_string())
        .eq_ignore_ascii_case("true");
    loop {
        if use_mock {
            update_conn_state(&state.rtds_health, ConnectionStatus::Connected, HealthState::Healthy).await;
            {
                let mut health = state.rtds_health.write().await;
                health.last_message_ts = Some(Utc::now());
                health.current_latency_ms = Some(50);
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
            continue;
        }
        update_conn_state(&state.rtds_health, ConnectionStatus::Connecting, HealthState::Degraded).await;
        match connect_async(&state.config.venue.polymarket_rtds_url).await {
            Ok((mut ws_stream, _)) => {
                update_conn_state(&state.rtds_health, ConnectionStatus::Connected, HealthState::Healthy).await;
                while let Some(msg) = ws_stream.next().await {
                    match msg {
                        Ok(ws_msg) if ws_msg.is_text() => {
                            let mut health = state.rtds_health.write().await;
                            health.last_message_ts = Some(Utc::now());
                            health.current_latency_ms = Some(25);
                        }
                        Ok(_) => {}
                        Err(err) => {
                            warn!(error = %err, "rtds stream read failed");
                            break;
                        }
                    }
                }
            }
            Err(err) => warn!(error = %err, "rtds stream connect failed"),
        }
        increment_reconnect(&state.rtds_health, &state, "polymarket_rtds").await;
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn increment_reconnect(stream_health: &Arc<RwLock<StreamHealth>>, state: &AppState, venue: &str) {
    {
        let mut health = stream_health.write().await;
        health.reconnect_count += 1;
        health.connection_status = ConnectionStatus::Reconnecting;
        health.health_state = HealthState::Degraded;
    }
    state
        .metrics
        .reconnect_count
        .with_label_values(&[&state.config.service_name, venue])
        .inc();
}

async fn publish_raw_event(state: &AppState, mut raw: RawVenueEvent, stream_label: &str) -> Result<()> {
    let accepted = state.storage.should_accept_event(&raw.event_id).await.unwrap_or(true);
    if !accepted {
        raw.ingestion_status = IngestionStatus::DroppedDuplicate;
        state
            .metrics
            .error_count
            .with_label_values(&[&state.config.service_name, "duplicate"])
            .inc();
        return Ok(());
    }
    let payload = serde_json::to_string(&raw)?;
    state
        .storage
        .push_bus_event(&state.storage.raw_stream_key, &payload)
        .await
        .context("failed pushing raw polymarket event")?;
    state
        .storage
        .push_bus_event(&format!("{}.normalizer", state.storage.raw_stream_key), &payload)
        .await
        .context("failed pushing raw polymarket event for normalizer")?;
    {
        let mut health = state.stream_health.write().await;
        health.last_message_ts = Some(raw.received_at_ts);
        health.current_latency_ms = Some((Utc::now() - raw.received_at_ts).num_milliseconds().max(0) as u64);
    }
    state
        .metrics
        .message_count
        .with_label_values(&[&state.config.service_name, "polymarket", stream_label])
        .inc();
    Ok(())
}

async fn update_conn_state(stream_health: &Arc<RwLock<StreamHealth>>, status: ConnectionStatus, health_state: HealthState) {
    let mut health = stream_health.write().await;
    health.connection_status = status;
    health.health_state = health_state;
}

async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    Json(GatewayHealthResponse {
        service: HealthPayload {
            service_name: state.config.service_name.clone(),
            instance_id: state.config.instance_id.clone(),
            build_version: state.config.build_version.clone(),
            config_fingerprint: state.config.fingerprint(),
            now_utc: Utc::now(),
            status: "ok".to_string(),
        },
        stream: state.stream_health.read().await.clone(),
        rtds_stream: state.rtds_health.read().await.clone(),
    })
}

async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    state.metrics.render()
}
