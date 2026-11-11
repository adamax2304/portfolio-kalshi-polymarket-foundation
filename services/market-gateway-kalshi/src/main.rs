use anyhow::Result;
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
use venue_kalshi::{to_raw_event, KalshiOrderbookUpdateDto};

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
    let config = ServiceConfig::from_env("market-gateway-kalshi", "PORT_GATEWAY_KALSHI")?;
    logger::init(&config.service_name, &config.log_level);
    let metrics = AppMetrics::new(&config.prometheus_namespace, &config.service_name);
    let storage = StorageClients::connect(&config).await?;
    let stream_health = Arc::new(RwLock::new(StreamHealth {
        service_name: config.service_name.clone(),
        venue: Venue::Kalshi,
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
    tokio::spawn(run_kalshi_stream(state.clone()));

    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        .with_state(state.clone())
        .layer(TraceLayer::new_for_http());
    let addr = SocketAddr::from(([0, 0, 0, 0], state.config.port));
    info!(address = %addr, "starting market-gateway-kalshi");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn run_kalshi_stream(state: AppState) {
    let use_mock = std::env::var("MOCK_STREAMS")
        .unwrap_or_else(|_| "true".to_string())
        .eq_ignore_ascii_case("true");
    let connection_id = format!("kalshi-{}", state.config.instance_id);
    loop {
        let _ = state.storage.set_service_heartbeat(&state.config.service_name).await;
        if use_mock {
            update_conn_state(&state, ConnectionStatus::Connected, HealthState::Healthy).await;
            let dto = KalshiOrderbookUpdateDto {
                market_ticker: "KXBTC-26APR12-B65000".to_string(),
                yes_bid: Some("48".to_string()),
                yes_ask: Some("51".to_string()),
                no_bid: Some("49".to_string()),
                no_ask: Some("52".to_string()),
                seq: Some(Utc::now().timestamp_millis().to_string()),
                event_ts_ms: Some(Utc::now().timestamp_millis()),
            };
            if let Err(err) = publish_raw_event(&state, to_raw_event(&dto, &connection_id)).await {
                error!(error = %err, "kalshi mock publish failed");
            }
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }

        update_conn_state(&state, ConnectionStatus::Connecting, HealthState::Degraded).await;
        match connect_async(&state.config.venue.kalshi_ws_url).await {
            Ok((mut ws_stream, _)) => {
                update_conn_state(&state, ConnectionStatus::Connected, HealthState::Healthy).await;
                while let Some(msg_result) = ws_stream.next().await {
                    match msg_result {
                        Ok(msg) if msg.is_text() => {
                            let text = msg.into_text().unwrap_or_default();
                            let payload: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                            let dto = KalshiOrderbookUpdateDto {
                                market_ticker: payload
                                    .get("market_ticker")
                                    .and_then(Value::as_str)
                                    .unwrap_or("unknown_market")
                                    .to_string(),
                                yes_bid: payload.get("yes_bid").and_then(Value::as_str).map(str::to_string),
                                yes_ask: payload.get("yes_ask").and_then(Value::as_str).map(str::to_string),
                                no_bid: payload.get("no_bid").and_then(Value::as_str).map(str::to_string),
                                no_ask: payload.get("no_ask").and_then(Value::as_str).map(str::to_string),
                                seq: payload.get("seq").and_then(Value::as_str).map(str::to_string),
                                event_ts_ms: payload.get("ts").and_then(Value::as_i64),
                            };
                            if let Err(err) = publish_raw_event(&state, to_raw_event(&dto, &connection_id)).await {
                                error!(error = %err, "kalshi publish failed");
                            }
                        }
                        Ok(_) => {}
                        Err(err) => {
                            warn!(error = %err, "kalshi read failed");
                            break;
                        }
                    }
                }
            }
            Err(err) => warn!(error = %err, "kalshi connect failed"),
        }
        {
            let mut health = state.stream_health.write().await;
            health.reconnect_count += 1;
            health.connection_status = ConnectionStatus::Reconnecting;
            health.health_state = HealthState::Degraded;
        }
        state
            .metrics
            .reconnect_count
            .with_label_values(&[&state.config.service_name, "kalshi"])
            .inc();
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn publish_raw_event(state: &AppState, mut raw: RawVenueEvent) -> Result<()> {
    if !state.storage.should_accept_event(&raw.event_id).await.unwrap_or(true) {
        raw.ingestion_status = IngestionStatus::DroppedDuplicate;
        return Ok(());
    }
    let payload = serde_json::to_string(&raw)?;
    state
        .storage
        .push_bus_event(&state.storage.raw_stream_key, &payload)
        .await?;
    state
        .storage
        .push_bus_event(&format!("{}.normalizer", state.storage.raw_stream_key), &payload)
        .await?;
    {
        let mut health = state.stream_health.write().await;
        health.last_message_ts = Some(raw.received_at_ts);
        health.current_latency_ms = Some((Utc::now() - raw.received_at_ts).num_milliseconds().max(0) as u64);
        health.health_state = HealthState::Healthy;
    }
    state
        .metrics
        .message_count
        .with_label_values(&[&state.config.service_name, "kalshi", "orderbook"])
        .inc();
    Ok(())
}

async fn update_conn_state(state: &AppState, status: ConnectionStatus, health_state: HealthState) {
    let mut health = state.stream_health.write().await;
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
    })
}

async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    state.metrics.render()
}
