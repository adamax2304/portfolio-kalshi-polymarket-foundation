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
use serde_json::Value;
use shared_types::HealthState;
use std::{net::SocketAddr, sync::Arc};
use storage::StorageClients;
use tokio::sync::RwLock;
use tower_http::trace::TraceLayer;
use tracing::{info, warn};

#[derive(Clone)]
struct AppState {
    metrics: AppMetrics,
    storage: StorageClients,
    config: ServiceConfig,
    last_error: Arc<RwLock<Option<String>>>,
}

#[derive(Debug, Serialize)]
struct MetadataHealthResponse {
    service: HealthPayload,
    last_error: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = ServiceConfig::from_env("metadata-service", "PORT_METADATA")?;
    logger::init(&config.service_name, &config.log_level);
    let metrics = AppMetrics::new(&config.prometheus_namespace, &config.service_name);
    let storage = StorageClients::connect(&config).await?;
    let state = AppState {
        metrics,
        storage,
        config,
        last_error: Arc::new(RwLock::new(None)),
    };
    register_service(&state).await;

    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        .route("/contracts", get(list_contracts_handler))
        .route("/contracts/:id", get(get_contract_handler))
        .route("/mappings/unresolved", get(unresolved_mappings_handler))
        .route("/services", get(list_services_handler))
        .with_state(state.clone())
        .layer(TraceLayer::new_for_http());

    let addr = SocketAddr::from(([0, 0, 0, 0], state.config.port));
    info!(address = %addr, "starting metadata-service");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn register_service(state: &AppState) {
    let result = state
        .storage
        .postgres
        .execute(
            "INSERT INTO service_registry (service_name, instance_id, build_version, status, updated_at) VALUES ($1, $2, $3, $4, NOW()) \
            ON CONFLICT (service_name, instance_id) DO UPDATE SET build_version = EXCLUDED.build_version, status = EXCLUDED.status, updated_at = NOW()",
            &[
                &state.config.service_name,
                &state.config.instance_id,
                &state.config.build_version,
                &"online",
            ],
        )
        .await;
    if let Err(err) = result {
        warn!(error = %err, "service registry upsert failed");
        *state.last_error.write().await = Some(err.to_string());
    }
}

async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    let last_error = state.last_error.read().await.clone();
    let health = if last_error.is_some() {
        HealthState::Degraded
    } else {
        HealthState::Healthy
    };
    Json(MetadataHealthResponse {
        service: HealthPayload {
            service_name: state.config.service_name.clone(),
            instance_id: state.config.instance_id.clone(),
            build_version: state.config.build_version.clone(),
            config_fingerprint: state.config.fingerprint(),
            now_utc: Utc::now(),
            status: format!("{:?}", health).to_ascii_lowercase(),
        },
        last_error,
    })
}

async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    state.metrics.render()
}

async fn list_contracts_handler(State(state): State<AppState>) -> impl IntoResponse {
    match state
        .storage
        .postgres
        .query(
            "SELECT canonical_contract_key, venue, venue_market_id, metadata_json, updated_at FROM canonical_contracts ORDER BY updated_at DESC LIMIT 500",
            &[],
        )
        .await
    {
        Ok(rows) => {
            let data: Vec<Value> = rows
                .into_iter()
                .map(|row| {
                    serde_json::json!({
                        "canonical_contract_key": row.get::<_, String>(0),
                        "venue": row.get::<_, String>(1),
                        "venue_market_id": row.get::<_, String>(2),
                        "metadata_json": row.get::<_, Value>(3),
                        "updated_at": row.get::<_, chrono::DateTime<Utc>>(4),
                    })
                })
                .collect();
            Json(serde_json::json!({ "data": data }))
        }
        Err(err) => {
            *state.last_error.write().await = Some(err.to_string());
            Json(serde_json::json!({ "data": [], "error": err.to_string() }))
        }
    }
}

async fn get_contract_handler(Path(id): Path<String>, State(state): State<AppState>) -> impl IntoResponse {
    match state
        .storage
        .postgres
        .query_opt(
            "SELECT canonical_contract_key, venue, venue_market_id, metadata_json, updated_at FROM canonical_contracts WHERE canonical_contract_key = $1",
            &[&id],
        )
        .await
    {
        Ok(Some(row)) => Json(serde_json::json!({
            "canonical_contract_key": row.get::<_, String>(0),
            "venue": row.get::<_, String>(1),
            "venue_market_id": row.get::<_, String>(2),
            "metadata_json": row.get::<_, Value>(3),
            "updated_at": row.get::<_, chrono::DateTime<Utc>>(4),
        })),
        Ok(None) => Json(serde_json::json!({ "error": "not_found", "id": id })),
        Err(err) => {
            *state.last_error.write().await = Some(err.to_string());
            Json(serde_json::json!({ "error": err.to_string() }))
        }
    }
}

async fn unresolved_mappings_handler(State(state): State<AppState>) -> impl IntoResponse {
    match state
        .storage
        .postgres
        .query(
            "SELECT id, event_type, payload_json, created_at FROM audit_events WHERE event_type = 'unresolved_mapping' ORDER BY created_at DESC LIMIT 500",
            &[],
        )
        .await
    {
        Ok(rows) => {
            let data: Vec<Value> = rows
                .into_iter()
                .map(|row| {
                    serde_json::json!({
                        "id": row.get::<_, i64>(0),
                        "event_type": row.get::<_, String>(1),
                        "payload_json": row.get::<_, Value>(2),
                        "created_at": row.get::<_, chrono::DateTime<Utc>>(3),
                    })
                })
                .collect();
            Json(serde_json::json!({ "data": data }))
        }
        Err(err) => {
            *state.last_error.write().await = Some(err.to_string());
            Json(serde_json::json!({ "data": [], "error": err.to_string() }))
        }
    }
}

async fn list_services_handler(State(state): State<AppState>) -> impl IntoResponse {
    match state
        .storage
        .postgres
        .query(
            "SELECT service_name, instance_id, build_version, status, updated_at FROM service_registry ORDER BY updated_at DESC",
            &[],
        )
        .await
    {
        Ok(rows) => {
            let data: Vec<Value> = rows
                .into_iter()
                .map(|row| {
                    serde_json::json!({
                        "service_name": row.get::<_, String>(0),
                        "instance_id": row.get::<_, String>(1),
                        "build_version": row.get::<_, String>(2),
                        "status": row.get::<_, String>(3),
                        "updated_at": row.get::<_, chrono::DateTime<Utc>>(4),
                    })
                })
                .collect();
            Json(serde_json::json!({ "data": data }))
        }
        Err(err) => {
            *state.last_error.write().await = Some(err.to_string());
            Json(serde_json::json!({ "data": [], "error": err.to_string() }))
        }
    }
}
