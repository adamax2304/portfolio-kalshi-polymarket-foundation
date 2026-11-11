use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum MarketRegimeFlag {
    Calm,
    Trending,
    Jumpy,
    Illiquid,
    WideSpread,
    NearExpiry,
    Stressed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketRegimeInput {
    pub observed_at: DateTime<Utc>,
    pub realized_vol_30s: Decimal,
    pub spread_bps: Decimal,
    pub depth_score: Decimal,
    pub update_frequency_hz: Decimal,
    pub reference_acceleration: Decimal,
    pub distance_to_strike_bps: Decimal,
    pub time_to_expiry_seconds: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegimeAssessment {
    pub flags: Vec<MarketRegimeFlag>,
    pub threshold_multiplier: Decimal,
    pub confidence_multiplier: Decimal,
    pub uncertainty_band_multiplier: Decimal,
}

pub fn classify_market_regime(input: &MarketRegimeInput) -> RegimeAssessment {
    let mut flags = vec![];

    if input.time_to_expiry_seconds <= 180 {
        flags.push(MarketRegimeFlag::NearExpiry);
    }
    if input.spread_bps >= Decimal::new(30, 0) {
        flags.push(MarketRegimeFlag::WideSpread);
    }
    if input.depth_score <= Decimal::new(25, 2) {
        flags.push(MarketRegimeFlag::Illiquid);
    }
    if input.realized_vol_30s >= Decimal::new(25, 3) || input.reference_acceleration >= Decimal::new(40, 2) {
        flags.push(MarketRegimeFlag::Jumpy);
    }
    if input.reference_acceleration >= Decimal::new(18, 2) {
        flags.push(MarketRegimeFlag::Trending);
    }
    if flags.is_empty() {
        flags.push(MarketRegimeFlag::Calm);
    }
    if flags.contains(&MarketRegimeFlag::NearExpiry)
        && (flags.contains(&MarketRegimeFlag::Jumpy)
            || flags.contains(&MarketRegimeFlag::WideSpread)
            || flags.contains(&MarketRegimeFlag::Illiquid))
    {
        flags.push(MarketRegimeFlag::Stressed);
    }

    let mut threshold_multiplier = Decimal::ONE;
    let mut confidence_multiplier = Decimal::ONE;
    let mut uncertainty_band_multiplier = Decimal::ONE;

    for flag in &flags {
        match flag {
            MarketRegimeFlag::Calm => {}
            MarketRegimeFlag::Trending => {
                threshold_multiplier += Decimal::new(10, 2);
                uncertainty_band_multiplier += Decimal::new(8, 2);
            }
            MarketRegimeFlag::Jumpy => {
                threshold_multiplier += Decimal::new(25, 2);
                confidence_multiplier -= Decimal::new(15, 2);
                uncertainty_band_multiplier += Decimal::new(25, 2);
            }
            MarketRegimeFlag::Illiquid => {
                threshold_multiplier += Decimal::new(20, 2);
                confidence_multiplier -= Decimal::new(20, 2);
                uncertainty_band_multiplier += Decimal::new(20, 2);
            }
            MarketRegimeFlag::WideSpread => {
                threshold_multiplier += Decimal::new(15, 2);
                confidence_multiplier -= Decimal::new(10, 2);
                uncertainty_band_multiplier += Decimal::new(15, 2);
            }
            MarketRegimeFlag::NearExpiry => {
                threshold_multiplier += Decimal::new(12, 2);
                uncertainty_band_multiplier += Decimal::new(10, 2);
            }
            MarketRegimeFlag::Stressed => {
                threshold_multiplier += Decimal::new(35, 2);
                confidence_multiplier -= Decimal::new(25, 2);
                uncertainty_band_multiplier += Decimal::new(35, 2);
            }
        }
    }

    if confidence_multiplier < Decimal::new(15, 2) {
        confidence_multiplier = Decimal::new(15, 2);
    }

    RegimeAssessment {
        flags,
        threshold_multiplier,
        confidence_multiplier,
        uncertainty_band_multiplier,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_stressed_near_expiry() {
        let input = MarketRegimeInput {
            observed_at: Utc::now(),
            realized_vol_30s: Decimal::new(40, 3),
            spread_bps: Decimal::new(45, 0),
            depth_score: Decimal::new(10, 2),
            update_frequency_hz: Decimal::new(8, 0),
            reference_acceleration: Decimal::new(25, 2),
            distance_to_strike_bps: Decimal::new(7, 0),
            time_to_expiry_seconds: 120,
        };
        let assessment = classify_market_regime(&input);
        assert!(assessment.flags.contains(&MarketRegimeFlag::NearExpiry));
        assert!(assessment.flags.contains(&MarketRegimeFlag::Stressed));
        assert!(assessment.threshold_multiplier > Decimal::ONE);
    }
}
