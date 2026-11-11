use anyhow::Result;
use chrono::{DateTime, Utc};
use clap::Parser;
use clickhouse::Row;
use config::ServiceConfig;
use pnl::PositionPnlState;
use pricing::{compute_fair_value, FairValueInput, FairValueModelConfig};
use queue_model::{QueueContext, QueueModelParams};
use replay::{build_replay_plan, ReplayRequest, ReplaySpeed};
use risk_models::{PaperRiskConfig, PaperRiskContext};
use rust_decimal::Decimal;
use serde_json::json;
use signal_models::{generate_signals, DynamicSignalConfig, SignalFeatureSnapshot, SignalGenerationInput};
use simulator::{simulate_signal, SimulationInput};
use std::collections::HashMap;
use storage::StorageClients;

#[derive(Parser, Debug)]
#[command(name = "replay-runner")]
struct Cli {
    #[arg(long)]
    from: String,
    #[arg(long)]
    to: String,
    #[arg(long, default_value = "BTC")]
    underlying: String,
    #[arg(long, default_value = "paper")]
    mode: String,
    #[arg(long, default_value = "x5")]
    speed: String,
    #[arg(long)]
    scenario: Option<String>,
    #[arg(long)]
    profile: Option<String>,
}

#[derive(Debug, Clone, Row, serde::Serialize)]
struct ReplayRunEventRow {
    run_id: String,
    event_ts: i64,
    stage: String,
    payload_json: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config = ServiceConfig::from_env("replay-runner", "PORT_REPLAY_RUNNER")?;
    let storage = StorageClients::connect(&config).await?;
    let request = ReplayRequest {
        from_ts: DateTime::parse_from_rfc3339(&cli.from)?.with_timezone(&Utc),
        to_ts: DateTime::parse_from_rfc3339(&cli.to)?.with_timezone(&Utc),
        underlying_filter: Some(cli.underlying.to_ascii_uppercase()),
        market_filter: None,
        include_deribit_metadata: true,
        speed: parse_speed(&cli.speed),
    };
    if let Some(profile) = &cli.profile {
        println!("using profile={profile}");
    }
    if let Some(scenario) = &cli.scenario {
        if let Ok(raw) = std::fs::read_to_string(scenario) {
            println!("scenario={}", raw);
        } else {
            println!("scenario file not found: {scenario}");
        }
    }
    let plan = build_replay_plan(&storage, request).await?;
    write_run_event(
        &storage,
        &plan.run_id,
        "started",
        json!({
            "from": &cli.from,
            "to": &cli.to,
            "underlying": &cli.underlying,
            "mode": &cli.mode,
            "speed": &cli.speed,
            "profile": &cli.profile,
            "scenario": &cli.scenario
        }),
    )
    .await
    .ok();
    println!("run_id={} raw={} normalized={} reference={}", plan.run_id, plan.merged_bundle.raw_events.len(), plan.merged_bundle.normalized_events.len(), plan.merged_bundle.reference_events.len());

    let mut latest_ref: HashMap<String, Decimal> = HashMap::new();
    let mut ref_events = plan.merged_bundle.reference_events.clone();
    ref_events.sort_by_key(|e| e.event_ts);
    let mut ref_idx = 0usize;

    let mut normalized_events = plan.merged_bundle.normalized_events.clone();
    normalized_events.sort_by_key(|e| e.event_ts);

    let mut accepted = 0u64;
    let mut rejected = 0u64;
    let mut traded = 0u64;
    let mut positions: HashMap<String, PositionPnlState> = HashMap::new();
    let mut total_realized = Decimal::ZERO;

    for event in normalized_events {
        while ref_idx < ref_events.len() && ref_events[ref_idx].event_ts <= event.event_ts {
            let symbol = ref_events[ref_idx].symbol.clone();
            if let Some(mid) = ref_events[ref_idx].mid_price.or(ref_events[ref_idx].last_price) {
                latest_ref.insert(symbol, mid);
            }
            ref_idx += 1;
        }
        let underlying = detect_underlying(&event.canonical_contract_key);
        let symbol = if underlying == "ETH" { "ETHUSDT" } else { "BTCUSDT" };
        let Some(spot_price) = latest_ref.get(symbol).copied() else {
            continue;
        };
        let strike = infer_strike(&event.venue_market_id).unwrap_or(spot_price);
        let implied = implied_prob(event.best_bid, event.best_ask, event.midpoint);
        let fair_input = FairValueInput {
            observed_at: event.event_ts,
            canonical_contract_key: event.canonical_contract_key.clone(),
            underlying: underlying.to_string(),
            spot_price,
            strike_price: strike,
            time_to_expiry_seconds: 900,
            realized_vol_30s: Decimal::new(11, 3),
            realized_vol_60s: Decimal::new(10, 3),
            realized_vol_180s: Decimal::new(9, 3),
            realized_vol_300s: Decimal::new(8, 3),
            short_momentum_bps: Decimal::new(4, 0),
            jumpiness_score: Decimal::new(20, 2),
            spread_bps: event.spread.unwrap_or(Decimal::new(12, 2)) * Decimal::new(100, 0),
            depth_score: Decimal::new(60, 2),
            l1_imbalance: Decimal::new(5, 2),
            l2_imbalance: Decimal::new(4, 2),
            microprice_deviation_bps: Decimal::new(1, 1),
            trade_pressure: Decimal::new(6, 2),
            reprice_speed: Decimal::new(20, 2),
            update_frequency_hz: Decimal::new(10, 0),
            reference_acceleration: Decimal::new(6, 2),
        };
        let fair = compute_fair_value(&fair_input, &FairValueModelConfig::default())?;
        let signal_input = SignalGenerationInput {
            observed_at: event.event_ts,
            canonical_contract_key: event.canonical_contract_key.clone(),
            underlying: underlying.to_string(),
            source_venues: vec![format!("{:?}", event.venue).to_ascii_lowercase()],
            fair_prob: fair.fair_prob,
            fair_prob_low: fair.fair_prob_low,
            fair_prob_high: fair.fair_prob_high,
            market_implied_prob: implied,
            venue_gap_bps: (implied - fair.fair_prob).abs() * Decimal::new(10_000, 0),
            synthetic_gross_edge_bps: (implied - fair.fair_prob).abs() * Decimal::new(9_000, 0),
            confidence_score: fair.confidence_score,
            market_regime_flags: fair.market_regime_flags.clone(),
            features: SignalFeatureSnapshot {
                spread_bps: fair_input.spread_bps,
                realized_vol_60s: fair_input.realized_vol_60s,
                depth_score: fair_input.depth_score,
                time_to_expiry_seconds: fair_input.time_to_expiry_seconds,
                confidence_score: fair.confidence_score,
                estimated_slippage_bps: Decimal::new(6, 0),
                estimated_fill_uncertainty_bps: Decimal::new(5, 0),
            },
        };
        for signal in generate_signals(&signal_input, &DynamicSignalConfig::default()) {
            if signal.accepted {
                accepted += 1;
            } else {
                rejected += 1;
            }
            let simultaneous_signals = positions.len();
            let key = signal.canonical_contract_key.clone();
            let position = positions.entry(key.clone()).or_default();
            let decision = simulate_signal(
                &SimulationInput {
                    run_id: plan.run_id.clone(),
                    signal: signal.clone(),
                    market_price: implied,
                    executable_price: implied,
                    queue_context: QueueContext {
                        ts: event.event_ts,
                        queue_ahead_notional: Decimal::new(600, 0),
                        posted_notional: Decimal::new(250, 0),
                        top_level_notional: Decimal::new(1000, 0),
                        touched: true,
                        time_to_expiry_seconds: 900,
                        recent_cancel_replace_count: 2,
                        depth_score: Decimal::new(60, 2),
                    },
                    risk_context: PaperRiskContext {
                        now: event.event_ts,
                        market_notional: position.qty.abs() * Decimal::new(100, 0),
                        underlying_exposure: Decimal::new(0, 0),
                        simultaneous_signals,
                        time_in_market_seconds: 60,
                        near_expiry_unhedged_notional: Decimal::new(30, 0),
                        reference_feed_stale: false,
                        disconnected_venue_feed: false,
                        unresolved_normalized_contract: false,
                        bad_markout_streak: 0,
                        settlement_ambiguity: false,
                        metadata_incomplete: false,
                    },
                },
                &QueueModelParams::default(),
                &PaperRiskConfig::default(),
                position,
            );
            if decision.traded {
                traded += 1;
            }
        }
        if plan.request.speed.delay_millis() > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(plan.request.speed.delay_millis())).await;
        }
    }
    for p in positions.values() {
        total_realized += p.realized_pnl;
    }
    persist_run_summary(&storage, &plan.run_id, &cli, accepted, rejected, traded, total_realized).await?;
    write_run_event(
        &storage,
        &plan.run_id,
        "completed",
        json!({
            "accepted": accepted,
            "rejected": rejected,
            "traded": traded,
            "realized_pnl": total_realized.to_string()
        }),
    )
    .await
    .ok();

    println!(
        "replay complete run_id={} accepted={} rejected={} traded={} realized_pnl={}",
        plan.run_id, accepted, rejected, traded, total_realized
    );
    Ok(())
}

async fn write_run_event(storage: &StorageClients, run_id: &str, stage: &str, payload: serde_json::Value) -> Result<()> {
    let row = ReplayRunEventRow {
        run_id: run_id.to_string(),
        event_ts: Utc::now().timestamp_millis(),
        stage: stage.to_string(),
        payload_json: serde_json::to_string(&payload)?,
    };
    let mut insert = storage.clickhouse.insert("replay_run_events")?;
    insert.write(&row).await?;
    insert.end().await?;
    Ok(())
}

async fn persist_run_summary(
    storage: &StorageClients,
    run_id: &str,
    cli: &Cli,
    accepted: u64,
    rejected: u64,
    traded: u64,
    realized: Decimal,
) -> Result<()> {
    storage
        .postgres
        .execute(
            "INSERT INTO replay_runs (run_id, mode, underlying, from_ts, to_ts, speed, status, created_at) \
             VALUES ($1, $2, $3, $4::timestamptz, $5::timestamptz, $6, $7, NOW()) \
             ON CONFLICT (run_id) DO UPDATE SET status = EXCLUDED.status",
            &[&run_id, &cli.mode, &cli.underlying, &cli.from, &cli.to, &cli.speed, &"completed"],
        )
        .await
        .ok();
    storage
        .postgres
        .execute(
            "INSERT INTO replay_run_metrics (run_id, accepted_signals, rejected_signals, traded_signals, realized_pnl, created_at) \
             VALUES ($1, $2, $3, $4, $5, NOW()) \
             ON CONFLICT (run_id) DO UPDATE SET accepted_signals = EXCLUDED.accepted_signals, rejected_signals = EXCLUDED.rejected_signals, \
             traded_signals = EXCLUDED.traded_signals, realized_pnl = EXCLUDED.realized_pnl, created_at = NOW()",
            &[&run_id, &(accepted as i64), &(rejected as i64), &(traded as i64), &realized.to_string()],
        )
        .await
        .ok();
    Ok(())
}

fn detect_underlying(contract_key: &str) -> &'static str {
    let up = contract_key.to_ascii_uppercase();
    if up.contains("ETH") {
        "ETH"
    } else {
        "BTC"
    }
}

fn implied_prob(bid: Option<Decimal>, ask: Option<Decimal>, mid: Option<Decimal>) -> Decimal {
    let mut p = mid.or_else(|| match (bid, ask) {
        (Some(b), Some(a)) => Some((b + a) / Decimal::new(2, 0)),
        _ => bid.or(ask),
    });
    if let Some(mut v) = p.take() {
        if v > Decimal::ONE {
            v /= Decimal::new(100, 0);
        }
        return v.max(Decimal::new(1, 2)).min(Decimal::new(99, 2));
    }
    Decimal::new(50, 2)
}

fn infer_strike(market_id: &str) -> Option<Decimal> {
    for token in market_id.split(|c: char| !c.is_ascii_digit()) {
        if token.len() >= 4 {
            if let Ok(v) = token.parse::<i64>() {
                return Some(Decimal::new(v, 0));
            }
        }
    }
    None
}

fn parse_speed(raw: &str) -> ReplaySpeed {
    match raw.to_ascii_lowercase().as_str() {
        "x1" | "1x" => ReplaySpeed::X1,
        "x20" | "20x" => ReplaySpeed::X20,
        "max" => ReplaySpeed::Max,
        _ => ReplaySpeed::X5,
    }
}
