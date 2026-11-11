use chrono::{DateTime, Utc};
use market_regimes::MarketRegimeFlag;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SignalType {
    FairValueDislocation,
    CrossVenueLeadLag,
    SyntheticArbCandidate,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SignalDirection {
    BuyYes,
    SellYes,
    FollowLeadVenue,
    PairCandidate,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PaperExecutionMode {
    PassiveMakerSim,
    AggressiveTakerSim,
    MakerThenTakerSim,
    NoTrade,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalFeatureSnapshot {
    pub spread_bps: Decimal,
    pub realized_vol_60s: Decimal,
    pub depth_score: Decimal,
    pub time_to_expiry_seconds: i64,
    pub confidence_score: Decimal,
    pub estimated_slippage_bps: Decimal,
    pub estimated_fill_uncertainty_bps: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalGenerationInput {
    pub observed_at: DateTime<Utc>,
    pub canonical_contract_key: String,
    pub underlying: String,
    pub source_venues: Vec<String>,
    pub fair_prob: Decimal,
    pub fair_prob_low: Decimal,
    pub fair_prob_high: Decimal,
    pub market_implied_prob: Decimal,
    pub venue_gap_bps: Decimal,
    pub synthetic_gross_edge_bps: Decimal,
    pub confidence_score: Decimal,
    pub market_regime_flags: Vec<MarketRegimeFlag>,
    pub features: SignalFeatureSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DynamicSignalConfig {
    pub base_required_edge_bps: Decimal,
    pub spread_weight: Decimal,
    pub vol_weight: Decimal,
    pub depth_relief_weight: Decimal,
    pub confidence_penalty_weight: Decimal,
    pub fill_uncertainty_weight: Decimal,
    pub ttl_ms: i64,
}

impl Default for DynamicSignalConfig {
    fn default() -> Self {
        Self {
            base_required_edge_bps: Decimal::new(22, 0),
            spread_weight: Decimal::new(45, 2),
            vol_weight: Decimal::new(25, 2),
            depth_relief_weight: Decimal::new(12, 2),
            confidence_penalty_weight: Decimal::new(40, 2),
            fill_uncertainty_weight: Decimal::new(10, 1),
            ttl_ms: 3500,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalDecision {
    pub signal_id: String,
    pub signal_type: SignalType,
    pub canonical_contract_key: String,
    pub underlying: String,
    pub source_venues: Vec<String>,
    pub direction: SignalDirection,
    pub confidence: Decimal,
    pub gross_edge_bps: Decimal,
    pub estimated_net_edge_bps: Decimal,
    pub signal_ttl_ms: i64,
    pub market_regime_flags: Vec<MarketRegimeFlag>,
    pub required_features_snapshot: SignalFeatureSnapshot,
    pub recommended_paper_execution_mode: PaperExecutionMode,
    pub accepted: bool,
    pub rejected_reason: Option<String>,
}

pub fn generate_signals(input: &SignalGenerationInput, config: &DynamicSignalConfig) -> Vec<SignalDecision> {
    let mut decisions = vec![];
    decisions.push(build_dislocation_signal(input, config));
    decisions.push(build_lead_lag_signal(input, config));
    decisions.push(build_synthetic_signal(input, config));
    decisions
}

fn build_dislocation_signal(input: &SignalGenerationInput, config: &DynamicSignalConfig) -> SignalDecision {
    let spread = (input.market_implied_prob - input.fair_prob) * Decimal::new(10_000, 0);
    build_signal(
        input,
        config,
        SignalType::FairValueDislocation,
        if spread >= Decimal::ZERO {
            SignalDirection::SellYes
        } else {
            SignalDirection::BuyYes
        },
        spread.abs(),
        "dislocation",
    )
}

fn build_lead_lag_signal(input: &SignalGenerationInput, config: &DynamicSignalConfig) -> SignalDecision {
    build_signal(
        input,
        config,
        SignalType::CrossVenueLeadLag,
        SignalDirection::FollowLeadVenue,
        input.venue_gap_bps.abs(),
        "lead_lag",
    )
}

fn build_synthetic_signal(input: &SignalGenerationInput, config: &DynamicSignalConfig) -> SignalDecision {
    build_signal(
        input,
        config,
        SignalType::SyntheticArbCandidate,
        SignalDirection::PairCandidate,
        input.synthetic_gross_edge_bps.abs(),
        "synthetic",
    )
}

fn build_signal(
    input: &SignalGenerationInput,
    config: &DynamicSignalConfig,
    signal_type: SignalType,
    direction: SignalDirection,
    gross_edge_bps: Decimal,
    salt: &str,
) -> SignalDecision {
    let required_edge = dynamic_required_edge_bps(input, config);
    let estimated_net_edge_bps =
        gross_edge_bps - input.features.estimated_slippage_bps - input.features.estimated_fill_uncertainty_bps;
    let accepted = estimated_net_edge_bps > required_edge && input.confidence_score >= Decimal::new(15, 2);
    let rejected_reason = if accepted {
        None
    } else if input.confidence_score < Decimal::new(15, 2) {
        Some("low_confidence".to_string())
    } else {
        Some(format!(
            "edge_below_dynamic_threshold(net={}, required={})",
            estimated_net_edge_bps, required_edge
        ))
    };
    let recommended_paper_execution_mode = if !accepted {
        PaperExecutionMode::NoTrade
    } else if input.features.spread_bps <= Decimal::new(12, 1) && input.features.depth_score >= Decimal::new(60, 2) {
        PaperExecutionMode::PassiveMakerSim
    } else if input.market_regime_flags.contains(&MarketRegimeFlag::NearExpiry)
        || input.market_regime_flags.contains(&MarketRegimeFlag::Stressed)
    {
        PaperExecutionMode::MakerThenTakerSim
    } else {
        PaperExecutionMode::AggressiveTakerSim
    };

    SignalDecision {
        signal_id: format!(
            "{}:{}:{}:{}",
            salt,
            input.canonical_contract_key,
            input.observed_at.timestamp_millis(),
            gross_edge_bps
        ),
        signal_type,
        canonical_contract_key: input.canonical_contract_key.clone(),
        underlying: input.underlying.clone(),
        source_venues: input.source_venues.clone(),
        direction,
        confidence: input.confidence_score,
        gross_edge_bps,
        estimated_net_edge_bps,
        signal_ttl_ms: config.ttl_ms,
        market_regime_flags: input.market_regime_flags.clone(),
        required_features_snapshot: input.features.clone(),
        recommended_paper_execution_mode,
        accepted,
        rejected_reason,
    }
}

pub fn dynamic_required_edge_bps(input: &SignalGenerationInput, config: &DynamicSignalConfig) -> Decimal {
    let mut required = config.base_required_edge_bps;
    required += input.features.spread_bps * config.spread_weight;
    required += input.features.realized_vol_60s * config.vol_weight * Decimal::new(10, 0);
    required -= input.features.depth_score * config.depth_relief_weight;
    required += (Decimal::ONE - input.features.confidence_score) * config.confidence_penalty_weight;
    required += input.features.estimated_fill_uncertainty_bps * config.fill_uncertainty_weight;
    if input.market_regime_flags.contains(&MarketRegimeFlag::NearExpiry) {
        required += Decimal::new(12, 0);
    }
    if input.market_regime_flags.contains(&MarketRegimeFlag::Stressed) {
        required += Decimal::new(18, 0);
    }
    required.max(Decimal::new(5, 0))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_input() -> SignalGenerationInput {
        SignalGenerationInput {
            observed_at: Utc::now(),
            canonical_contract_key: "kalshi:btc15m".to_string(),
            underlying: "BTC".to_string(),
            source_venues: vec!["kalshi".to_string(), "polymarket".to_string()],
            fair_prob: Decimal::new(54, 2),
            fair_prob_low: Decimal::new(49, 2),
            fair_prob_high: Decimal::new(59, 2),
            market_implied_prob: Decimal::new(48, 2),
            venue_gap_bps: Decimal::new(28, 0),
            synthetic_gross_edge_bps: Decimal::new(35, 0),
            confidence_score: Decimal::new(72, 2),
            market_regime_flags: vec![MarketRegimeFlag::Calm],
            features: SignalFeatureSnapshot {
                spread_bps: Decimal::new(8, 1),
                realized_vol_60s: Decimal::new(12, 3),
                depth_score: Decimal::new(65, 2),
                time_to_expiry_seconds: 450,
                confidence_score: Decimal::new(72, 2),
                estimated_slippage_bps: Decimal::new(4, 0),
                estimated_fill_uncertainty_bps: Decimal::new(3, 0),
            },
        }
    }

    #[test]
    fn generates_three_signals() {
        let decisions = generate_signals(&sample_input(), &DynamicSignalConfig::default());
        assert_eq!(decisions.len(), 3);
        assert!(decisions.iter().any(|s| s.signal_type == SignalType::FairValueDislocation));
    }

    #[test]
    fn rejects_low_confidence_signal() {
        let mut input = sample_input();
        input.confidence_score = Decimal::new(10, 2);
        input.features.confidence_score = Decimal::new(10, 2);
        let decisions = generate_signals(&input, &DynamicSignalConfig::default());
        assert!(decisions.iter().all(|d| !d.accepted));
    }

    #[test]
    fn lead_lag_candidate_can_be_accepted() {
        let mut input = sample_input();
        input.venue_gap_bps = Decimal::new(90, 0);
        input.features.estimated_slippage_bps = Decimal::new(2, 0);
        input.features.estimated_fill_uncertainty_bps = Decimal::new(1, 0);
        let decisions = generate_signals(&input, &DynamicSignalConfig::default());
        let lead_lag = decisions
            .iter()
            .find(|s| s.signal_type == SignalType::CrossVenueLeadLag)
            .expect("lead/lag signal must exist");
        assert!(lead_lag.accepted);
    }
}
