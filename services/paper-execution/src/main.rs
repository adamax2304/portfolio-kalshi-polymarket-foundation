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
use pnl::PositionPnlState;
use queue_model::{QueueContext, QueueModelParams};
use risk_models::{PaperRiskConfig, PaperRiskContext};
use rust_decimal::Decimal;
use serde::Serialize;
use serde_json::json;
use signal_models::SignalDecision;
use simulator::{simulate_signal, SimulationDecision, SimulationInput};
use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};
use storage::StorageClients;
use tokio::sync::RwLock;
use tower_http::trace::TraceLayer;
use tracing::{info, warn};

#[derive(Clone)]
struct AppState {
    metrics: AppMetrics,
    storage: StorageClients,
    config: ServiceConfig,
    signal_stream_key: String,
    paper_decisions_stream_key: String,
    positions: Arc<RwLock<HashMap<String, PositionPnlState>>>,
    decisions_tape: Arc<RwLock<VecDeque<SimulationDecision>>>,
    queue_params: QueueModelParams,
    risk_config: PaperRiskConfig,
}

#[derive(Debug, Serialize)]
struct PaperHealth {
    service: HealthPayload,
    open_positions: usize,
    decisions_buffered: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = ServiceConfig::from_env("paper-execution", "PORT_PAPER_EXECUTION")?;
    logger::init(&config.service_name, &config.log_level);
    let metrics = AppMetrics::new(&config.prometheus_namespace, &config.service_name);
    let storage = StorageClients::connect(&config).await?;
    let state = AppState {
        metrics,
        storage,
        config,
        signal_stream_key: read_var("REDIS_STREAM_SIGNALS", Some("bus.signals"))?,
        paper_decisions_stream_key: read_var("REDIS_STREAM_PAPER_DECISIONS", Some("bus.paper_decisions"))?,
        positions: Arc::new(RwLock::new(HashMap::new())),
        decisions_tape: Arc::new(RwLock::new(VecDeque::with_capacity(5000))),
        queue_params: QueueModelParams::default(),
        risk_config: PaperRiskConfig::default(),
    };
    tokio::spawn(run_paper_execution_loop(state.clone()));

    let app = Router::new()
        .route("/health", get(health_handler))
        .route("/metrics", get(metrics_handler))
        .route("/paper/status", get(status_handler))
        .route("/paper/positions", get(positions_handler))
        .route("/paper/decisions", get(decisions_handler))
        .with_state(state.clone())
        .layer(TraceLayer::new_for_http());
    let addr = SocketAddr::from(([0, 0, 0, 0], state.config.port));
    info!(address = %addr, "starting paper-execution");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn run_paper_execution_loop(state: AppState) {
    loop {
        let _ = state.storage.set_service_heartbeat(&state.config.service_name).await;
        match state.storage.pop_bus_event(&state.signal_stream_key, 1.0).await {
            Ok(Some(raw)) => match serde_json::from_str::<SignalDecision>(&raw) {
                Ok(signal) => {
                    if let Err(err) = process_signal(&state, signal).await {
                        warn!(error = %err, "paper execution failed");
                        state
                            .metrics
                            .error_count
                            .with_label_values(&[&state.config.service_name, "simulation"])
                            .inc();
                    }
                }
                Err(err) => warn!(error = %err, "invalid signal payload"),
            },
            Ok(None) => {}
            Err(err) => warn!(error = %err, "signal stream pop failed"),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn process_signal(state: &AppState, signal: SignalDecision) -> Result<()> {
    let (underlying_exposure, simultaneous_signals, mut position_state) = {
        let positions = state.positions.read().await;
        let exposure = positions
            .iter()
            .filter(|(k, _)| k.to_ascii_uppercase().contains(&signal.underlying))
            .map(|(_, p)| p.qty.abs() * Decimal::new(100, 0))
            .fold(Decimal::ZERO, |acc, n| acc + n);
        let current = positions
            .get(&signal.canonical_contract_key)
            .cloned()
            .unwrap_or_default();
        (exposure, positions.len(), current)
    };
    let queue_context = QueueContext {
        ts: Utc::now(),
        queue_ahead_notional: Decimal::new(500, 0),
        posted_notional: Decimal::new(250, 0),
        top_level_notional: Decimal::new(1800, 0),
        touched: true,
        time_to_expiry_seconds: signal.required_features_snapshot.time_to_expiry_seconds,
        recent_cancel_replace_count: 2,
        depth_score: signal.required_features_snapshot.depth_score,
    };
    let risk_context = PaperRiskContext {
        now: Utc::now(),
        market_notional: position_state.qty.abs() * Decimal::new(100, 0),
        underlying_exposure,
        simultaneous_signals,
        time_in_market_seconds: 120,
        near_expiry_unhedged_notional: Decimal::new(40, 0),
        reference_feed_stale: false,
        disconnected_venue_feed: false,
        unresolved_normalized_contract: false,
        bad_markout_streak: 0,
        settlement_ambiguity: false,
        metadata_incomplete: false,
    };
    let sim_input = SimulationInput {
        run_id: "paper-live-loop".to_string(),
        signal: signal.clone(),
        market_price: Decimal::new(50, 2),
        executable_price: Decimal::new(50, 2),
        queue_context,
        risk_context,
    };
    let decision = simulate_signal(
        &sim_input,
        &state.queue_params,
        &state.risk_config,
        &mut position_state,
    );
    {
        let mut positions = state.positions.write().await;
        positions.insert(signal.canonical_contract_key.clone(), position_state);
    }
    persist_decision(state, &signal, &decision).await?;
    state
        .storage
        .push_bus_event(
            &state.paper_decisions_stream_key,
            &serde_json::to_string(&json!({
                "signal": signal,
                "decision": decision
            }))?,
        )
        .await?;
    state
        .metrics
        .message_count
        .with_label_values(&[&state.config.service_name, "internal", "paper_decision"])
        .inc();
    let mut tape = state.decisions_tape.write().await;
    tape.push_front(decision);
    if tape.len() > 2000 {
        tape.pop_back();
    }
    Ok(())
}

async fn persist_decision(state: &AppState, signal: &SignalDecision, decision: &SimulationDecision) -> Result<()> {
    let decision_payload = serde_json::to_string(&json!({
        "signal": signal,
        "decision": decision
    }))?;
    state
        .storage
        .postgres
        .execute(
            "INSERT INTO paper_signal_decisions (signal_id, canonical_contract_key, accepted, payload_json, created_at) \
             VALUES ($1, $2, $3, $4::jsonb, NOW())",
            &[&signal.signal_id, &signal.canonical_contract_key, &signal.accepted, &decision_payload],
        )
        .await
        .ok();
    state
        .storage
        .postgres
        .execute(
            "INSERT INTO paper_positions (canonical_contract_key, qty, avg_price, realized_pnl, updated_at) \
             VALUES ($1, $2, $3, $4, NOW()) \
             ON CONFLICT (canonical_contract_key) DO UPDATE SET qty = EXCLUDED.qty, avg_price = EXCLUDED.avg_price, \
             realized_pnl = EXCLUDED.realized_pnl, updated_at = NOW()",
            &[
                &signal.canonical_contract_key,
                &decision.updated_position.qty.to_string(),
                &decision.updated_position.avg_price.to_string(),
                &decision.updated_position.realized_pnl.to_string(),
            ],
        )
        .await
        .ok();
    Ok(())
}

async fn health_handler(State(state): State<AppState>) -> impl IntoResponse {
    Json(PaperHealth {
        service: HealthPayload {
            service_name: state.config.service_name.clone(),
            instance_id: state.config.instance_id.clone(),
            build_version: state.config.build_version.clone(),
            config_fingerprint: state.config.fingerprint(),
            now_utc: Utc::now(),
            status: "ok".to_string(),
        },
        open_positions: state.positions.read().await.len(),
        decisions_buffered: state.decisions_tape.read().await.len(),
    })
}

async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    state.metrics.render()
}

async fn status_handler(State(state): State<AppState>) -> impl IntoResponse {
    let positions = state.positions.read().await;
    let gross_exposure = positions
        .values()
        .map(|p| p.qty.abs() * Decimal::new(100, 0))
        .fold(Decimal::ZERO, |acc, x| acc + x);
    Json(json!({
        "open_positions": positions.len(),
        "gross_exposure_proxy": gross_exposure,
        "recent_decisions": state.decisions_tape.read().await.len()
    }))
}

async fn positions_handler(State(state): State<AppState>) -> impl IntoResponse {
    let rows: Vec<serde_json::Value> = state
        .positions
        .read()
        .await
        .iter()
        .map(|(k, p)| {
            json!({
                "canonical_contract_key": k,
                "qty": p.qty,
                "avg_price": p.avg_price,
                "realized_pnl": p.realized_pnl,
                "fees": p.total_fees
            })
        })
        .collect();
    Json(json!({ "data": rows }))
}

async fn decisions_handler(State(state): State<AppState>) -> impl IntoResponse {
    let rows: Vec<SimulationDecision> = state.decisions_tape.read().await.iter().cloned().collect();
    Json(json!({ "data": rows }))
}
