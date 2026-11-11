use chrono::{DateTime, Utc};
use pnl::{apply_fill, FillRecord, PositionPnlState};
use queue_model::{estimate_execution, QueueContext, QueueModelParams};
use risk_models::{evaluate_paper_risk, PaperRiskConfig, PaperRiskContext};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use signal_models::{PaperExecutionMode, SignalDecision, SignalDirection};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulationInput {
    pub run_id: String,
    pub signal: SignalDecision,
    pub market_price: Decimal,
    pub executable_price: Decimal,
    pub queue_context: QueueContext,
    pub risk_context: PaperRiskContext,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulationDecision {
    pub traded: bool,
    pub fill_ratio: Decimal,
    pub executed_qty: Decimal,
    pub fill_probability_score: Decimal,
    pub estimated_slippage_bps: Decimal,
    pub reasons: Vec<String>,
    pub updated_position: PositionPnlState,
}

pub fn simulate_signal(
    input: &SimulationInput,
    queue_params: &QueueModelParams,
    risk_config: &PaperRiskConfig,
    position_state: &mut PositionPnlState,
) -> SimulationDecision {
    let risk = evaluate_paper_risk(&input.signal, &input.risk_context, risk_config);
    if !risk.allow_trade || input.signal.recommended_paper_execution_mode == PaperExecutionMode::NoTrade {
        return SimulationDecision {
            traded: false,
            fill_ratio: Decimal::ZERO,
            executed_qty: Decimal::ZERO,
            fill_probability_score: Decimal::ZERO,
            estimated_slippage_bps: Decimal::ZERO,
            reasons: risk.reasons,
            updated_position: position_state.clone(),
        };
    }

    let estimate = estimate_execution(&input.queue_context, queue_params);
    let target_qty = signal_target_qty(&input.signal.recommended_paper_execution_mode);
    let executed_qty = (target_qty * estimate.expected_fill_ratio).max(Decimal::ZERO);
    if executed_qty <= Decimal::new(1, 3) {
        return SimulationDecision {
            traded: false,
            fill_ratio: Decimal::ZERO,
            executed_qty: Decimal::ZERO,
            fill_probability_score: estimate.fill_probability_score,
            estimated_slippage_bps: estimate.estimated_taker_slippage_bps,
            reasons: vec!["no_fill_outcome".to_string()],
            updated_position: position_state.clone(),
        };
    }

    let side = match input.signal.direction {
        SignalDirection::BuyYes | SignalDirection::FollowLeadVenue | SignalDirection::PairCandidate => "buy",
        SignalDirection::SellYes => "sell",
    };
    let fill_price = if side == "buy" {
        input.executable_price + estimate.estimated_taker_slippage_bps / Decimal::new(10_000, 0)
    } else {
        input.executable_price - estimate.estimated_taker_slippage_bps / Decimal::new(10_000, 0)
    };

    apply_fill(
        position_state,
        &FillRecord {
            ts: input.queue_context.ts,
            run_id: input.run_id.clone(),
            signal_id: input.signal.signal_id.clone(),
            canonical_contract_key: input.signal.canonical_contract_key.clone(),
            underlying: input.signal.underlying.clone(),
            venue: input
                .signal
                .source_venues
                .first()
                .cloned()
                .unwrap_or_else(|| "unknown".to_string()),
            execution_mode: format!("{:?}", input.signal.recommended_paper_execution_mode).to_ascii_lowercase(),
            qty: executed_qty,
            price: fill_price,
            fees: executed_qty * Decimal::new(1, 4),
            slippage_bps: estimate.estimated_taker_slippage_bps,
            side: side.to_string(),
        },
    );

    SimulationDecision {
        traded: true,
        fill_ratio: estimate.expected_fill_ratio,
        executed_qty,
        fill_probability_score: estimate.fill_probability_score,
        estimated_slippage_bps: estimate.estimated_taker_slippage_bps,
        reasons: vec![],
        updated_position: position_state.clone(),
    }
}

fn signal_target_qty(mode: &PaperExecutionMode) -> Decimal {
    match mode {
        PaperExecutionMode::PassiveMakerSim => Decimal::new(4, 0),
        PaperExecutionMode::AggressiveTakerSim => Decimal::new(6, 0),
        PaperExecutionMode::MakerThenTakerSim => Decimal::new(5, 0),
        PaperExecutionMode::NoTrade => Decimal::ZERO,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioDefinition {
    pub name: String,
    pub description: String,
    pub event_count: usize,
    pub created_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use signal_models::{SignalFeatureSnapshot, SignalType};

    fn sample_signal() -> SignalDecision {
        SignalDecision {
            signal_id: "sig1".to_string(),
            signal_type: SignalType::FairValueDislocation,
            canonical_contract_key: "kalshi:btc15m".to_string(),
            underlying: "BTC".to_string(),
            source_venues: vec!["kalshi".to_string()],
            direction: SignalDirection::BuyYes,
            confidence: Decimal::new(80, 2),
            gross_edge_bps: Decimal::new(50, 0),
            estimated_net_edge_bps: Decimal::new(25, 0),
            signal_ttl_ms: 3000,
            market_regime_flags: vec![],
            required_features_snapshot: SignalFeatureSnapshot {
                spread_bps: Decimal::new(12, 1),
                realized_vol_60s: Decimal::new(10, 3),
                depth_score: Decimal::new(60, 2),
                time_to_expiry_seconds: 500,
                confidence_score: Decimal::new(80, 2),
                estimated_slippage_bps: Decimal::new(4, 0),
                estimated_fill_uncertainty_bps: Decimal::new(3, 0),
            },
            recommended_paper_execution_mode: PaperExecutionMode::AggressiveTakerSim,
            accepted: true,
            rejected_reason: None,
        }
    }

    #[test]
    fn simulates_trade_when_risk_passes() {
        let input = SimulationInput {
            run_id: "run".to_string(),
            signal: sample_signal(),
            market_price: Decimal::new(48, 2),
            executable_price: Decimal::new(49, 2),
            queue_context: QueueContext {
                ts: Utc::now(),
                queue_ahead_notional: Decimal::new(100, 0),
                posted_notional: Decimal::new(200, 0),
                top_level_notional: Decimal::new(500, 0),
                touched: true,
                time_to_expiry_seconds: 500,
                recent_cancel_replace_count: 1,
                depth_score: Decimal::new(70, 2),
            },
            risk_context: PaperRiskContext {
                now: Utc::now(),
                market_notional: Decimal::new(50, 0),
                underlying_exposure: Decimal::new(100, 0),
                simultaneous_signals: 1,
                time_in_market_seconds: 20,
                near_expiry_unhedged_notional: Decimal::new(20, 0),
                reference_feed_stale: false,
                disconnected_venue_feed: false,
                unresolved_normalized_contract: false,
                bad_markout_streak: 0,
                settlement_ambiguity: false,
                metadata_incomplete: false,
            },
        };
        let mut state = PositionPnlState::default();
        let out = simulate_signal(
            &input,
            &QueueModelParams::default(),
            &PaperRiskConfig::default(),
            &mut state,
        );
        assert!(out.traded);
        assert!(out.executed_qty > Decimal::ZERO);
    }
}
