use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use signal_models::SignalDecision;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperRiskConfig {
    pub max_notional_per_market: Decimal,
    pub max_exposure_per_underlying: Decimal,
    pub max_simultaneous_signals: usize,
    pub max_time_in_market_seconds: i64,
    pub max_near_expiry_unhedged_notional: Decimal,
    pub min_confidence: Decimal,
    pub cooldown_bad_markout_streak: u32,
}

impl Default for PaperRiskConfig {
    fn default() -> Self {
        Self {
            max_notional_per_market: Decimal::new(1500, 0),
            max_exposure_per_underlying: Decimal::new(5000, 0),
            max_simultaneous_signals: 6,
            max_time_in_market_seconds: 900,
            max_near_expiry_unhedged_notional: Decimal::new(800, 0),
            min_confidence: Decimal::new(18, 2),
            cooldown_bad_markout_streak: 4,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaperRiskContext {
    pub now: DateTime<Utc>,
    pub market_notional: Decimal,
    pub underlying_exposure: Decimal,
    pub simultaneous_signals: usize,
    pub time_in_market_seconds: i64,
    pub near_expiry_unhedged_notional: Decimal,
    pub reference_feed_stale: bool,
    pub disconnected_venue_feed: bool,
    pub unresolved_normalized_contract: bool,
    pub bad_markout_streak: u32,
    pub settlement_ambiguity: bool,
    pub metadata_incomplete: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskDecision {
    pub allow_trade: bool,
    pub reasons: Vec<String>,
}

pub fn evaluate_paper_risk(signal: &SignalDecision, ctx: &PaperRiskContext, cfg: &PaperRiskConfig) -> RiskDecision {
    let mut reasons = vec![];
    if !signal.accepted {
        reasons.push("signal_not_accepted".to_string());
    }
    if signal.confidence < cfg.min_confidence {
        reasons.push("fair_confidence_below_threshold".to_string());
    }
    if ctx.market_notional >= cfg.max_notional_per_market {
        reasons.push("max_notional_per_market_exceeded".to_string());
    }
    if ctx.underlying_exposure >= cfg.max_exposure_per_underlying {
        reasons.push("max_exposure_per_underlying_exceeded".to_string());
    }
    if ctx.simultaneous_signals >= cfg.max_simultaneous_signals {
        reasons.push("max_simultaneous_signals_exceeded".to_string());
    }
    if ctx.time_in_market_seconds >= cfg.max_time_in_market_seconds {
        reasons.push("max_time_in_market_exceeded".to_string());
    }
    if ctx.near_expiry_unhedged_notional > cfg.max_near_expiry_unhedged_notional {
        reasons.push("near_expiry_unhedged_exposure_limit".to_string());
    }
    if ctx.reference_feed_stale {
        reasons.push("stale_reference_feed".to_string());
    }
    if ctx.disconnected_venue_feed {
        reasons.push("disconnected_venue_feed".to_string());
    }
    if ctx.unresolved_normalized_contract {
        reasons.push("unresolved_normalized_contract".to_string());
    }
    if ctx.metadata_incomplete {
        reasons.push("contract_metadata_incomplete".to_string());
    }
    if ctx.settlement_ambiguity {
        reasons.push("settlement_mapping_ambiguous".to_string());
    }
    if ctx.bad_markout_streak >= cfg.cooldown_bad_markout_streak {
        reasons.push("cooldown_after_bad_markouts".to_string());
    }

    RiskDecision {
        allow_trade: reasons.is_empty(),
        reasons,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use signal_models::{PaperExecutionMode, SignalDirection, SignalFeatureSnapshot, SignalType};

    fn accepted_signal() -> SignalDecision {
        SignalDecision {
            signal_id: "s1".to_string(),
            signal_type: SignalType::FairValueDislocation,
            canonical_contract_key: "k".to_string(),
            underlying: "BTC".to_string(),
            source_venues: vec!["kalshi".to_string()],
            direction: SignalDirection::BuyYes,
            confidence: Decimal::new(75, 2),
            gross_edge_bps: Decimal::new(40, 0),
            estimated_net_edge_bps: Decimal::new(25, 0),
            signal_ttl_ms: 2000,
            market_regime_flags: vec![],
            required_features_snapshot: SignalFeatureSnapshot {
                spread_bps: Decimal::new(10, 1),
                realized_vol_60s: Decimal::new(10, 3),
                depth_score: Decimal::new(70, 2),
                time_to_expiry_seconds: 600,
                confidence_score: Decimal::new(75, 2),
                estimated_slippage_bps: Decimal::new(4, 0),
                estimated_fill_uncertainty_bps: Decimal::new(3, 0),
            },
            recommended_paper_execution_mode: PaperExecutionMode::PassiveMakerSim,
            accepted: true,
            rejected_reason: None,
        }
    }

    #[test]
    fn blocks_stale_reference_feed() {
        let decision = evaluate_paper_risk(
            &accepted_signal(),
            &PaperRiskContext {
                now: Utc::now(),
                market_notional: Decimal::new(100, 0),
                underlying_exposure: Decimal::new(300, 0),
                simultaneous_signals: 1,
                time_in_market_seconds: 100,
                near_expiry_unhedged_notional: Decimal::new(20, 0),
                reference_feed_stale: true,
                disconnected_venue_feed: false,
                unresolved_normalized_contract: false,
                bad_markout_streak: 0,
                settlement_ambiguity: false,
                metadata_incomplete: false,
            },
            &PaperRiskConfig::default(),
        );
        assert!(!decision.allow_trade);
        assert!(decision.reasons.iter().any(|r| r == "stale_reference_feed"));
    }

    #[test]
    fn synthetic_candidate_downgraded_on_settlement_ambiguity() {
        let decision = evaluate_paper_risk(
            &accepted_signal(),
            &PaperRiskContext {
                now: Utc::now(),
                market_notional: Decimal::new(100, 0),
                underlying_exposure: Decimal::new(300, 0),
                simultaneous_signals: 1,
                time_in_market_seconds: 100,
                near_expiry_unhedged_notional: Decimal::new(20, 0),
                reference_feed_stale: false,
                disconnected_venue_feed: false,
                unresolved_normalized_contract: false,
                bad_markout_streak: 0,
                settlement_ambiguity: true,
                metadata_incomplete: false,
            },
            &PaperRiskConfig::default(),
        );
        assert!(!decision.allow_trade);
        assert!(decision
            .reasons
            .iter()
            .any(|reason| reason == "settlement_mapping_ambiguous"));
    }
}
