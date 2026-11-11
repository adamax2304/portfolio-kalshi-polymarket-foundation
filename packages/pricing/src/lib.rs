use anyhow::Result;
use chrono::{DateTime, Utc};
use market_regimes::{classify_market_regime, MarketRegimeFlag, MarketRegimeInput};
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::f64::consts::E;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FairValueModelConfig {
    pub base_sensitivity: Decimal,
    pub min_probability: Decimal,
    pub max_probability: Decimal,
    pub micro_adjustment_cap: Decimal,
    pub base_band_bps: Decimal,
    pub near_expiry_penalty: Decimal,
}

impl Default for FairValueModelConfig {
    fn default() -> Self {
        Self {
            base_sensitivity: Decimal::new(4, 0),
            min_probability: Decimal::new(1, 2),
            max_probability: Decimal::new(99, 2),
            micro_adjustment_cap: Decimal::new(8, 2),
            base_band_bps: Decimal::new(6, 2),
            near_expiry_penalty: Decimal::new(10, 2),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FairValueInput {
    pub observed_at: DateTime<Utc>,
    pub canonical_contract_key: String,
    pub underlying: String,
    pub spot_price: Decimal,
    pub strike_price: Decimal,
    pub time_to_expiry_seconds: i64,
    pub realized_vol_30s: Decimal,
    pub realized_vol_60s: Decimal,
    pub realized_vol_180s: Decimal,
    pub realized_vol_300s: Decimal,
    pub short_momentum_bps: Decimal,
    pub jumpiness_score: Decimal,
    pub spread_bps: Decimal,
    pub depth_score: Decimal,
    pub l1_imbalance: Decimal,
    pub l2_imbalance: Decimal,
    pub microprice_deviation_bps: Decimal,
    pub trade_pressure: Decimal,
    pub reprice_speed: Decimal,
    pub update_frequency_hz: Decimal,
    pub reference_acceleration: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FairValueOutput {
    pub fair_prob: Decimal,
    pub fair_prob_low: Decimal,
    pub fair_prob_high: Decimal,
    pub confidence_score: Decimal,
    pub model_version: String,
    pub input_snapshot_hash: String,
    pub market_regime_flags: Vec<MarketRegimeFlag>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FairValueEvent {
    pub event_id: String,
    pub observed_at: DateTime<Utc>,
    pub canonical_contract_key: String,
    pub underlying: String,
    pub source_venues: Vec<String>,
    pub market_implied_prob: Decimal,
    pub spot_price: Decimal,
    pub strike_price: Decimal,
    pub time_to_expiry_seconds: i64,
    pub fair: FairValueOutput,
}

pub fn compute_fair_value(input: &FairValueInput, config: &FairValueModelConfig) -> Result<FairValueOutput> {
    let regime_input = MarketRegimeInput {
        observed_at: input.observed_at,
        realized_vol_30s: input.realized_vol_30s,
        spread_bps: input.spread_bps,
        depth_score: input.depth_score,
        update_frequency_hz: input.update_frequency_hz,
        reference_acceleration: input.reference_acceleration,
        distance_to_strike_bps: distance_to_strike_bps(input.spot_price, input.strike_price),
        time_to_expiry_seconds: input.time_to_expiry_seconds,
    };
    let regime = classify_market_regime(&regime_input);

    let base_prob = base_probability(input, config);
    let micro_delta = microstructure_adjustment(input, config);
    let mut fair_prob = clamp_probability(base_prob + micro_delta, config);
    if regime.flags.contains(&MarketRegimeFlag::NearExpiry) {
        fair_prob = clamp_probability(fair_prob - config.near_expiry_penalty * Decimal::new(1, 2), config);
    }

    let average_vol =
        (input.realized_vol_30s + input.realized_vol_60s + input.realized_vol_180s + input.realized_vol_300s)
            / Decimal::new(4, 0);
    let band = (config.base_band_bps
        + average_vol * Decimal::new(6, 1)
        + input.jumpiness_score * Decimal::new(5, 2)
        + input.spread_bps * Decimal::new(1, 2))
        * regime.uncertainty_band_multiplier;
    let band_prob = band / Decimal::new(100, 0);

    let fair_prob_low = clamp_probability(fair_prob - band_prob, config);
    let fair_prob_high = clamp_probability(fair_prob + band_prob, config);

    let confidence_base = Decimal::new(80, 2)
        - input.spread_bps * Decimal::new(12, 3)
        + input.depth_score * Decimal::new(15, 2)
        - input.jumpiness_score * Decimal::new(20, 2);
    let confidence_score = (confidence_base * regime.confidence_multiplier)
        .max(Decimal::new(5, 2))
        .min(Decimal::ONE);

    let input_snapshot_hash = hash_input(input)?;
    Ok(FairValueOutput {
        fair_prob,
        fair_prob_low,
        fair_prob_high,
        confidence_score,
        model_version: "fair_v1.0.0".to_string(),
        input_snapshot_hash,
        market_regime_flags: regime.flags,
    })
}

fn base_probability(input: &FairValueInput, config: &FairValueModelConfig) -> Decimal {
    let spot = input.spot_price.to_f64().unwrap_or(0.0).max(1.0);
    let strike = input.strike_price.to_f64().unwrap_or(spot);
    let distance = (spot - strike) / spot;
    let vol = ((input.realized_vol_30s + input.realized_vol_60s + input.realized_vol_180s + input.realized_vol_300s)
        / Decimal::new(4, 0))
    .to_f64()
    .unwrap_or(0.01)
    .max(0.0001);
    let t = (input.time_to_expiry_seconds.max(1) as f64) / 900.0;
    let momentum = input.short_momentum_bps.to_f64().unwrap_or(0.0) / 10_000.0;
    let z = (distance + momentum) / (vol * t.sqrt() + 0.0001);
    let sensitivity = config.base_sensitivity.to_f64().unwrap_or(4.0);
    let logistic = 1.0 / (1.0 + E.powf(-z * sensitivity));
    Decimal::from_f64(logistic).unwrap_or(Decimal::new(50, 2))
}

fn microstructure_adjustment(input: &FairValueInput, config: &FairValueModelConfig) -> Decimal {
    let adjustment = input.l1_imbalance * Decimal::new(4, 2)
        + input.l2_imbalance * Decimal::new(3, 2)
        + input.microprice_deviation_bps * Decimal::new(5, 4)
        + input.trade_pressure * Decimal::new(3, 2)
        + input.reprice_speed * Decimal::new(2, 3);
    adjustment
        .max(-config.micro_adjustment_cap)
        .min(config.micro_adjustment_cap)
        / Decimal::new(100, 0)
}

fn distance_to_strike_bps(spot: Decimal, strike: Decimal) -> Decimal {
    if spot <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    ((spot - strike) / spot).abs() * Decimal::new(10_000, 0)
}

fn clamp_probability(prob: Decimal, config: &FairValueModelConfig) -> Decimal {
    prob.max(config.min_probability).min(config.max_probability)
}

fn hash_input(input: &FairValueInput) -> Result<String> {
    let bytes = serde_json::to_vec(input)?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Ok(format!("{:x}", hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_input() -> FairValueInput {
        FairValueInput {
            observed_at: Utc::now(),
            canonical_contract_key: "polymarket:btc15m".to_string(),
            underlying: "BTC".to_string(),
            spot_price: Decimal::new(65000, 0),
            strike_price: Decimal::new(65200, 0),
            time_to_expiry_seconds: 420,
            realized_vol_30s: Decimal::new(12, 3),
            realized_vol_60s: Decimal::new(11, 3),
            realized_vol_180s: Decimal::new(9, 3),
            realized_vol_300s: Decimal::new(8, 3),
            short_momentum_bps: Decimal::new(4, 0),
            jumpiness_score: Decimal::new(20, 2),
            spread_bps: Decimal::new(8, 1),
            depth_score: Decimal::new(55, 2),
            l1_imbalance: Decimal::new(10, 2),
            l2_imbalance: Decimal::new(5, 2),
            microprice_deviation_bps: Decimal::new(2, 1),
            trade_pressure: Decimal::new(12, 2),
            reprice_speed: Decimal::new(30, 2),
            update_frequency_hz: Decimal::new(8, 0),
            reference_acceleration: Decimal::new(5, 2),
        }
    }

    #[test]
    fn produces_probability_and_band() {
        let output = compute_fair_value(&sample_input(), &FairValueModelConfig::default())
            .expect("fair value should compute");
        assert!(output.fair_prob >= Decimal::new(1, 2));
        assert!(output.fair_prob <= Decimal::new(99, 2));
        assert!(output.fair_prob_low <= output.fair_prob);
        assert!(output.fair_prob_high >= output.fair_prob);
    }
}
