use anyhow::{Context, Result};
use axum::{extract::State, response::IntoResponse, routing::get, Json, Router};
use chrono::Utc;
use config::ServiceConfig;
use futures::StreamExt;
use metrics::{AppMetrics, HealthPayload};
use serde::Serialize;
use serde_json::Value;
use shared_types::{ConnectionStatus, HealthState, IngestionStatus, RawVenueEvent, StreamHealth, Venue};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use storage::StorageClients;
use tokio::sync::RwLock;
use tokio_tungstenite::connect_async;
use tower_http::trace::TraceLayer;
use tracing::{error, info, warn};
use venue_binance::{to_raw_event, BinanceBookTickerDto};

#[derive(Clone)]
struct AppState {
    metrics: AppMetrics,
    stream_health: Arc<RwLock<StreamHealth>>,
    storage: StorageClients,
    config: ServiceConfig,
}

#[derive(Debug, Serialize)]
struct GatewayHealthResponse {
    service: HealthPayload,
    stream: StreamHealth,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = ServiceConfig::from_env("market-gateway-binance", "PORT_GATEWAY_BINANCE")?;
    logger::init(&config.service_name, &config.log_level);
    let metrics = AppMetrics::new(&config.prometheus_namespace, &config.service_name);
    let storage = StorageClients::connect(&config).await?;
    let stream_health = Arc::new(RwLock::new(StreamHealth {
        service_name: config.service_name.clone(),
        venue: Venue::Binance,
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
        storage,
        config,
    };
    tokio::spawn(run_binance_stream(state.clone()));

    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        .with_state(state.clone())
        .layer(TraceLayer::new_for_http());
    let addr = SocketAddr::from(([0, 0, 0, 0], state.config.port));
    info!(address = %addr, "starting market-gateway-binance");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn run_binance_stream(state: AppState) {
    let use_mock = std::env::var("MOCK_STREAMS")
        .unwrap_or_else(|_| "true".to_string())
        .eq_ignore_ascii_case("true");
    let connection_id = format!("binance-{}", state.config.instance_id);
    loop {
        if let Err(err) = state.storage.set_service_heartbeat(&state.config.service_name).await {
            warn!(error = %err, "heartbeat update failed");
        }

        if use_mock {
            update_conn_state(&state, ConnectionStatus::Connected, HealthState::Healthy).await;
            if let Err(err) = emit_mock_event(&state, &connection_id).await {
                error!(error = %err, "mock binance stream failed");
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }

        update_conn_state(&state, ConnectionStatus::Connecting, HealthState::Degraded).await;
        match connect_async(&state.config.venue.binance_ws_url).await {
            Ok((mut ws_stream, _)) => {
                info!("binance websocket connected");
                update_conn_state(&state, ConnectionStatus::Connected, HealthState::Healthy).await;
                while let Some(msg_result) = ws_stream.next().await {
                    match msg_result {
                        Ok(msg) if msg.is_text() => {
                            if let Err(err) = publish_binance_text(&state, msg.into_text().unwrap_or_default(), &connection_id).await
                            {
                                error!(error = %err, "publish binance event failed");
                                state
                                    .metrics
                                    .error_count
                                    .with_label_values(&[&state.config.service_name, "publish"])
                                    .inc();
                            }
                        }
                        Ok(_) => {}
                        Err(err) => {
                            warn!(error = %err, "binance websocket read failed");
                            break;
                        }
                    }
                }
            }
            Err(err) => {
                warn!(error = %err, "binance websocket connect failed");
            }
        }
        let reconnects = {
            let mut health = state.stream_health.write().await;
            health.reconnect_count += 1;
            health.reconnect_count
        };
        state
            .metrics
            .reconnect_count
            .with_label_values(&[&state.config.service_name, "binance"])
            .inc();
        warn!(reconnects, "reconnecting binance stream");
        update_conn_state(&state, ConnectionStatus::Reconnecting, HealthState::Degraded).await;
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn emit_mock_event(state: &AppState, connection_id: &str) -> Result<()> {
    let now = Utc::now().timestamp_millis();
    let dto = BinanceBookTickerDto {
        symbol: state.config.venue.binance_symbol.clone(),
        bid_price: "65000.10".to_string(),
        ask_price: "65000.30".to_string(),
        event_ts_ms: now,
    };
    let raw = to_raw_event(&dto, connection_id);
    publish_raw_event(state, raw).await
}

async fn publish_binance_text(state: &AppState, text: String, connection_id: &str) -> Result<()> {
    let raw_json: Value = serde_json::from_str(&text).context("failed to parse binance JSON")?;
    let dto = BinanceBookTickerDto {
        symbol: raw_json
            .get("s")
            .and_then(Value::as_str)
            .unwrap_or(&state.config.venue.binance_symbol)
            .to_string(),
        bid_price: raw_json.get("b").and_then(Value::as_str).unwrap_or("0").to_string(),
        ask_price: raw_json.get("a").and_then(Value::as_str).unwrap_or("0").to_string(),
        event_ts_ms: raw_json
            .get("E")
            .and_then(Value::as_i64)
            .unwrap_or_else(|| Utc::now().timestamp_millis()),
    };
    let raw = to_raw_event(&dto, connection_id);
    publish_raw_event(state, raw).await
}

async fn publish_raw_event(state: &AppState, mut raw: RawVenueEvent) -> Result<()> {
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
    let raw_payload = serde_json::to_string(&raw)?;
    state
        .storage
        .push_bus_event(&state.storage.raw_stream_key, &raw_payload)
        .await?;
    state
        .storage
        .push_bus_event(&format!("{}.normalizer", state.storage.raw_stream_key), &raw_payload)
        .await?;

    let latency = (Utc::now() - raw.received_at_ts).num_milliseconds().max(0) as u64;
    {
        let mut health = state.stream_health.write().await;
        health.last_message_ts = Some(raw.received_at_ts);
        health.current_latency_ms = Some(latency);
        health.health_state = HealthState::Healthy;
    }
    state
        .metrics
        .message_count
        .with_label_values(&[&state.config.service_name, "binance", "reference_price"])
        .inc();
    state
        .metrics
        .stream_latency_ms
        .with_label_values(&[&state.config.service_name, "binance"])
        .set(latency as i64);
    Ok(())
}

async fn update_conn_state(state: &AppState, status: ConnectionStatus, health_state: HealthState) {
    let mut health = state.stream_health.write().await;
    health.connection_status = status;
    health.health_state = health_state;
}

async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    let stream = state.stream_health.read().await.clone();
    Json(GatewayHealthResponse {
        service: HealthPayload {
            service_name: state.config.service_name.clone(),
            instance_id: state.config.instance_id.clone(),
            build_version: state.config.build_version.clone(),
            config_fingerprint: state.config.fingerprint(),
            now_utc: Utc::now(),
            status: "ok".to_string(),
        },
        stream,
    })
}

async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    state.metrics.render()
}
