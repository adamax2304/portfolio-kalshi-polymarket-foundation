use anyhow::Result;
use axum::{
    extract::{Query, State},
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use chrono::{DateTime, Utc};
use config::ServiceConfig;
use metrics::{AppMetrics, HealthPayload};
use serde::{Deserialize, Serialize};
use shared_types::{HealthState, ReplayBundle, ReplayExportRequest, Venue};
use std::{collections::HashMap, net::SocketAddr, sync::Arc, time::Duration};
use storage::{ReplayExporter, StorageClients};
use tokio::sync::RwLock;
use tower_http::trace::TraceLayer;
use tracing::{error, info, warn};

#[derive(Clone)]
struct AppState {
    metrics: AppMetrics,
    storage: StorageClients,
    config: ServiceConfig,
    stats: Arc<RwLock<RecorderStats>>,
}

#[derive(Debug, Clone, Serialize, Default)]
struct RecorderStats {
    raw_persisted: u64,
    normalized_persisted: u64,
    reference_persisted: u64,
    last_error: Option<String>,
    last_persisted_ts: Option<DateTime<Utc>>,
}

#[derive(Debug, Serialize)]
struct RecorderHealthResponse {
    service: HealthPayload,
    stats: RecorderStats,
}

#[derive(Debug, Deserialize)]
struct ExportQuery {
    venue: Option<String>,
    market_id: Option<String>,
    from: String,
    to: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = ServiceConfig::from_env("recorder", "PORT_RECORDER")?;
    logger::init(&config.service_name, &config.log_level);
    let metrics = AppMetrics::new(&config.prometheus_namespace, &config.service_name);
    let storage = StorageClients::connect(&config).await?;
    let state = AppState {
        metrics,
        storage,
        config,
        stats: Arc::new(RwLock::new(RecorderStats::default())),
    };
    tokio::spawn(run_raw_recorder(state.clone()));
    tokio::spawn(run_normalized_recorder(state.clone()));
    tokio::spawn(run_reference_recorder(state.clone()));

    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        .route("/stats", get(stats_handler))
        .route("/export/raw", get(export_raw_handler))
        .route("/export/normalized", get(export_normalized_handler))
        .route("/export/reference", get(export_reference_handler))
        .route("/export/bundle", get(export_bundle_handler))
        .with_state(state.clone())
        .layer(TraceLayer::new_for_http());
    let addr = SocketAddr::from(([0, 0, 0, 0], state.config.port));
    info!(address = %addr, "starting recorder");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn run_raw_recorder(state: AppState) {
    loop {
        let _ = state.storage.set_service_heartbeat(&state.config.service_name).await;
        match state
            .storage
            .pop_bus_event(&state.storage.raw_stream_key, 1.0)
            .await
        {
            Ok(Some(payload)) => match serde_json::from_str::<shared_types::RawVenueEvent>(&payload) {
                Ok(event) => {
                    if let Err(err) = state.storage.insert_raw_event(&event).await {
                        register_error(&state, format!("raw insert error: {err}")).await;
                    } else {
                        register_raw_success(&state).await;
                        let _ = state
                            .storage
                            .cache_latest(
                                &format!(
                                    "latest:raw:{}:{}",
                                    format!("{:?}", event.venue).to_ascii_lowercase(),
                                    format!("{:?}", event.event_type).to_ascii_lowercase()
                                ),
                                &payload,
                            )
                            .await;
                    }
                }
                Err(err) => register_error(&state, format!("raw decode error: {err}")).await,
            },
            Ok(None) => {}
            Err(err) => register_error(&state, format!("raw stream pop error: {err}")).await,
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn run_normalized_recorder(state: AppState) {
    loop {
        match state
            .storage
            .pop_bus_event(&state.storage.normalized_stream_key, 1.0)
            .await
        {
            Ok(Some(payload)) => match serde_json::from_str::<shared_types::NormalizedMarketEvent>(&payload) {
                Ok(event) => {
                    if let Err(err) = state.storage.insert_normalized_event(&event).await {
                        register_error(&state, format!("normalized insert error: {err}")).await;
                    } else {
                        let mut stats = state.stats.write().await;
                        stats.normalized_persisted += 1;
                        stats.last_persisted_ts = Some(Utc::now());
                    }
                }
                Err(err) => register_error(&state, format!("normalized decode error: {err}")).await,
            },
            Ok(None) => {}
            Err(err) => register_error(&state, format!("normalized stream pop error: {err}")).await,
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn run_reference_recorder(state: AppState) {
    loop {
        match state
            .storage
            .pop_bus_event(&state.storage.reference_stream_key, 1.0)
            .await
        {
            Ok(Some(payload)) => match serde_json::from_str::<shared_types::ReferencePriceEvent>(&payload) {
                Ok(event) => {
                    if let Err(err) = state.storage.insert_reference_event(&event).await {
                        register_error(&state, format!("reference insert error: {err}")).await;
                    } else {
                        let mut stats = state.stats.write().await;
                        stats.reference_persisted += 1;
                        stats.last_persisted_ts = Some(Utc::now());
                    }
                }
                Err(err) => register_error(&state, format!("reference decode error: {err}")).await,
            },
            Ok(None) => {}
            Err(err) => register_error(&state, format!("reference stream pop error: {err}")).await,
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn register_raw_success(state: &AppState) {
    let mut stats = state.stats.write().await;
    stats.raw_persisted += 1;
    stats.last_persisted_ts = Some(Utc::now());
    state
        .metrics
        .message_count
        .with_label_values(&[&state.config.service_name, "internal", "raw_persisted"])
        .inc();
}

async fn register_error(state: &AppState, message: String) {
    warn!(error = %message, "recorder failure");
    let mut stats = state.stats.write().await;
    stats.last_error = Some(message.clone());
    state
        .metrics
        .error_count
        .with_label_values(&[&state.config.service_name, "storage"])
        .inc();
}

async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    let stats = state.stats.read().await.clone();
    let health_state = if stats.last_error.is_some() {
        HealthState::Degraded
    } else {
        HealthState::Healthy
    };
    Json(RecorderHealthResponse {
        service: HealthPayload {
            service_name: state.config.service_name.clone(),
            instance_id: state.config.instance_id.clone(),
            build_version: state.config.build_version.clone(),
            config_fingerprint: state.config.fingerprint(),
            now_utc: Utc::now(),
            status: format!("{:?}", health_state).to_ascii_lowercase(),
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

async fn export_raw_handler(
    State(state): State<AppState>,
    Query(query): Query<ExportQuery>,
) -> impl IntoResponse {
    match to_export_request(query) {
        Ok(request) => match state.storage.export_raw_events(&request).await {
            Ok(data) => Json(json_response(data)),
            Err(err) => {
                error!(error = %err, "raw export failed");
                Json(json_response(Vec::<serde_json::Value>::new()))
            }
        },
        Err(err) => {
            error!(error = %err, "invalid export request");
            Json(json_response(Vec::<serde_json::Value>::new()))
        }
    }
}

async fn export_normalized_handler(
    State(state): State<AppState>,
    Query(query): Query<ExportQuery>,
) -> impl IntoResponse {
    match to_export_request(query) {
        Ok(request) => match state.storage.export_normalized_events(&request).await {
            Ok(data) => Json(json_response(data)),
            Err(err) => {
                error!(error = %err, "normalized export failed");
                Json(json_response(Vec::<serde_json::Value>::new()))
            }
        },
        Err(err) => {
            error!(error = %err, "invalid export request");
            Json(json_response(Vec::<serde_json::Value>::new()))
        }
    }
}

async fn export_reference_handler(
    State(state): State<AppState>,
    Query(query): Query<ExportQuery>,
) -> impl IntoResponse {
    match to_export_request(query) {
        Ok(request) => match state.storage.export_reference_events(&request).await {
            Ok(data) => Json(json_response(data)),
            Err(err) => {
                error!(error = %err, "reference export failed");
                Json(json_response(Vec::<serde_json::Value>::new()))
            }
        },
        Err(err) => {
            error!(error = %err, "invalid export request");
            Json(json_response(Vec::<serde_json::Value>::new()))
        }
    }
}

async fn export_bundle_handler(
    State(state): State<AppState>,
    Query(query): Query<ExportQuery>,
) -> impl IntoResponse {
    match to_export_request(query) {
        Ok(request) => match state.storage.export_bundle(request).await {
            Ok(bundle) => Json(bundle),
            Err(err) => {
                error!(error = %err, "bundle export failed");
                Json(ReplayBundle {
                    raw_events: vec![],
                    normalized_events: vec![],
                    reference_events: vec![],
                })
            }
        },
        Err(err) => {
            error!(error = %err, "invalid export request");
            Json(ReplayBundle {
                raw_events: vec![],
                normalized_events: vec![],
                reference_events: vec![],
            })
        }
    }
}

fn json_response<T: Serialize>(data: T) -> HashMap<&'static str, serde_json::Value> {
    let mut map = HashMap::new();
    map.insert("data", serde_json::to_value(data).unwrap_or(serde_json::Value::Null));
    map.insert("generated_at", serde_json::json!(Utc::now()));
    map
}

fn to_export_request(query: ExportQuery) -> Result<ReplayExportRequest> {
    let venue = query.venue.and_then(|v| match v.to_ascii_lowercase().as_str() {
        "polymarket" => Some(Venue::Polymarket),
        "kalshi" => Some(Venue::Kalshi),
        "binance" => Some(Venue::Binance),
        "deribit" => Some(Venue::Deribit),
        _ => None,
    });
    Ok(ReplayExportRequest {
        venue,
        market_id: query.market_id,
        from_ts: DateTime::parse_from_rfc3339(&query.from)?.with_timezone(&Utc),
        to_ts: DateTime::parse_from_rfc3339(&query.to)?.with_timezone(&Utc),
    })
}
