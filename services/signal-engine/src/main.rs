use anyhow::Result;
use axum::{
    extract::State,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use chrono::Utc;
use config::{read_var, ServiceConfig};
use metrics::{AppMetrics, HealthPayload};
use pricing::FairValueEvent;
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::json;
use signal_models::{
    generate_signals, DynamicSignalConfig, SignalDecision, SignalFeatureSnapshot, SignalGenerationInput,
};
use std::{collections::{HashMap, VecDeque}, net::SocketAddr, sync::Arc, time::Duration};
use storage::StorageClients;
use tokio::sync::RwLock;
use tower_http::trace::TraceLayer;
use tracing::{info, warn};

#[derive(Clone)]
struct AppState {
    metrics: AppMetrics,
    storage: StorageClients,
    config: ServiceConfig,
    fair_stream_key: String,
    signal_stream_key: String,
    latest_signals: Arc<RwLock<HashMap<String, SignalDecision>>>,
    signal_tape: Arc<RwLock<VecDeque<SignalDecision>>>,
    signal_config: DynamicSignalConfig,
}

#[derive(Debug, Serialize)]
struct SignalHealth {
    service: HealthPayload,
    tracked_signals: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = ServiceConfig::from_env("signal-engine", "PORT_SIGNAL_ENGINE")?;
    logger::init(&config.service_name, &config.log_level);
    let metrics = AppMetrics::new(&config.prometheus_namespace, &config.service_name);
    let storage = StorageClients::connect(&config).await?;
    let state = AppState {
        metrics,
        storage,
        config,
        fair_stream_key: read_var("REDIS_STREAM_FAIR_VALUES", Some("bus.fair_values"))?,
        signal_stream_key: read_var("REDIS_STREAM_SIGNALS", Some("bus.signals"))?,
        latest_signals: Arc::new(RwLock::new(HashMap::new())),
        signal_tape: Arc::new(RwLock::new(VecDeque::with_capacity(5000))),
        signal_config: DynamicSignalConfig::default(),
    };
    tokio::spawn(run_signal_loop(state.clone()));

    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        .route("/signals/latest", get(latest_signals_handler))
        .route("/signals/tail", get(signal_tape_handler))
        .route("/signals/stats", get(signal_stats_handler))
        .with_state(state.clone())
        .layer(TraceLayer::new_for_http());
    let addr = SocketAddr::from(([0, 0, 0, 0], state.config.port));
    info!(address = %addr, "starting signal-engine");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn run_signal_loop(state: AppState) {
    loop {
        let _ = state.storage.set_service_heartbeat(&state.config.service_name).await;
        match state.storage.pop_bus_event(&state.fair_stream_key, 1.0).await {
            Ok(Some(raw)) => match serde_json::from_str::<FairValueEvent>(&raw) {
                Ok(fair_event) => {
                    for decision in generate_from_fair(&fair_event, &state.signal_config) {
                        if let Err(err) = publish_signal(&state, decision).await {
                            warn!(error = %err, "signal publish failed");
                            state
                                .metrics
                                .error_count
                                .with_label_values(&[&state.config.service_name, "publish"])
                                .inc();
                        }
                    }
                }
                Err(err) => {
                    warn!(error = %err, "failed to parse fair event");
                }
            },
            Ok(None) => {}
            Err(err) => warn!(error = %err, "fair stream pop failed"),
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
}

fn generate_from_fair(fair: &FairValueEvent, config: &DynamicSignalConfig) -> Vec<SignalDecision> {
    let features = SignalFeatureSnapshot {
        spread_bps: Decimal::new(8, 1),
        realized_vol_60s: Decimal::new(11, 3),
        depth_score: Decimal::new(58, 2),
        time_to_expiry_seconds: fair.time_to_expiry_seconds,
        confidence_score: fair.fair.confidence_score,
        estimated_slippage_bps: Decimal::new(6, 0),
        estimated_fill_uncertainty_bps: Decimal::new(5, 0),
    };
    let venue_gap_bps = (fair.market_implied_prob - fair.fair.fair_prob).abs() * Decimal::new(10_000, 0);
    let synthetic_gross_edge_bps = venue_gap_bps - Decimal::new(4, 0);
    let input = SignalGenerationInput {
        observed_at: fair.observed_at,
        canonical_contract_key: fair.canonical_contract_key.clone(),
        underlying: fair.underlying.clone(),
        source_venues: fair.source_venues.clone(),
        fair_prob: fair.fair.fair_prob,
        fair_prob_low: fair.fair.fair_prob_low,
        fair_prob_high: fair.fair.fair_prob_high,
        market_implied_prob: fair.market_implied_prob,
        venue_gap_bps,
        synthetic_gross_edge_bps,
        confidence_score: fair.fair.confidence_score,
        market_regime_flags: fair.fair.market_regime_flags.clone(),
        features,
    };
    generate_signals(&input, config)
}

async fn publish_signal(state: &AppState, signal: SignalDecision) -> Result<()> {
    let payload = serde_json::to_string(&signal)?;
    state
        .storage
        .push_bus_event(&state.signal_stream_key, &payload)
        .await?;
    state
        .storage
        .cache_latest(&format!("latest:signal:{}", signal.canonical_contract_key), &payload)
        .await?;
    state
        .metrics
        .message_count
        .with_label_values(&[&state.config.service_name, "internal", "signal"])
        .inc();

    state
        .latest_signals
        .write()
        .await
        .insert(signal.canonical_contract_key.clone(), signal.clone());
    let mut tape = state.signal_tape.write().await;
    tape.push_front(signal);
    if tape.len() > 1000 {
        tape.pop_back();
    }
    Ok(())
}

async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    Json(SignalHealth {
        service: HealthPayload {
            service_name: state.config.service_name.clone(),
            instance_id: state.config.instance_id.clone(),
            build_version: state.config.build_version.clone(),
            config_fingerprint: state.config.fingerprint(),
            now_utc: Utc::now(),
            status: "ok".to_string(),
        },
        tracked_signals: state.latest_signals.read().await.len(),
    })
}

async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    state.metrics.render()
}

async fn latest_signals_handler(State(state): State<AppState>) -> impl IntoResponse {
    let data: Vec<SignalDecision> = state.latest_signals.read().await.values().cloned().collect();
    Json(json!({ "data": data }))
}

async fn signal_tape_handler(State(state): State<AppState>) -> impl IntoResponse {
    let data: Vec<SignalDecision> = state.signal_tape.read().await.iter().cloned().collect();
    Json(json!({ "data": data }))
}

async fn signal_stats_handler(State(state): State<AppState>) -> impl IntoResponse {
    let tape = state.signal_tape.read().await;
    let accepted = tape.iter().filter(|s| s.accepted).count();
    let rejected = tape.len().saturating_sub(accepted);
    Json(json!({
        "count_total": tape.len(),
        "count_accepted": accepted,
        "count_rejected": rejected,
    }))
}
