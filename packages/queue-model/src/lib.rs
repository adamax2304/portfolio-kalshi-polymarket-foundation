use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueModelParams {
    pub touch_fill_penalty: Decimal,
    pub maker_persistence_bonus: Decimal,
    pub cancel_replace_delay_ms: i64,
    pub near_expiry_deterioration_bps: Decimal,
    pub taker_depth_impact_weight: Decimal,
}

impl Default for QueueModelParams {
    fn default() -> Self {
        Self {
            touch_fill_penalty: Decimal::new(30, 2),
            maker_persistence_bonus: Decimal::new(15, 2),
            cancel_replace_delay_ms: 180,
            near_expiry_deterioration_bps: Decimal::new(8, 0),
            taker_depth_impact_weight: Decimal::new(25, 2),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueContext {
    pub ts: DateTime<Utc>,
    pub queue_ahead_notional: Decimal,
    pub posted_notional: Decimal,
    pub top_level_notional: Decimal,
    pub touched: bool,
    pub time_to_expiry_seconds: i64,
    pub recent_cancel_replace_count: u32,
    pub depth_score: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionEstimate {
    pub fill_probability_score: Decimal,
    pub expected_fill_ratio: Decimal,
    pub estimated_taker_slippage_bps: Decimal,
    pub delayed_fill_penalty_bps: Decimal,
    pub no_fill_probability: Decimal,
}

pub fn estimate_execution(ctx: &QueueContext, params: &QueueModelParams) -> ExecutionEstimate {
    let queue_pressure = if ctx.top_level_notional > Decimal::ZERO {
        (ctx.queue_ahead_notional / ctx.top_level_notional).min(Decimal::new(5, 0))
    } else {
        Decimal::new(2, 0)
    };

    let touch_adjustment = if ctx.touched {
        Decimal::ONE - params.touch_fill_penalty
    } else {
        Decimal::new(55, 2)
    };
    let persistence_adjustment = (Decimal::ONE
        + params.maker_persistence_bonus
        - Decimal::new(ctx.recent_cancel_replace_count as i64, 2))
    .max(Decimal::new(5, 2));
    let depth_adjustment = ctx.depth_score.max(Decimal::new(5, 2));

    let mut fill_probability_score =
        (touch_adjustment * persistence_adjustment * depth_adjustment) / (Decimal::ONE + queue_pressure);
    if ctx.time_to_expiry_seconds <= 120 {
        fill_probability_score -= Decimal::new(15, 2);
    }
    fill_probability_score = fill_probability_score.max(Decimal::new(2, 2)).min(Decimal::new(95, 2));

    let expected_fill_ratio = (fill_probability_score - Decimal::new(20, 2)).max(Decimal::ZERO);
    let mut estimated_taker_slippage_bps = (Decimal::ONE - depth_adjustment)
        * Decimal::new(12, 0)
        + queue_pressure * params.taker_depth_impact_weight * Decimal::new(10, 0);
    let delayed_fill_penalty_bps = Decimal::new(params.cancel_replace_delay_ms, 0) / Decimal::new(100, 0)
        + if ctx.time_to_expiry_seconds <= 180 {
            params.near_expiry_deterioration_bps
        } else {
            Decimal::ZERO
        };
    estimated_taker_slippage_bps += delayed_fill_penalty_bps;
    let no_fill_probability = (Decimal::ONE - fill_probability_score).max(Decimal::new(5, 2));

    ExecutionEstimate {
        fill_probability_score,
        expected_fill_ratio,
        estimated_taker_slippage_bps,
        delayed_fill_penalty_bps,
        no_fill_probability,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn touch_does_not_imply_full_fill() {
        let estimate = estimate_execution(
            &QueueContext {
                ts: Utc::now(),
                queue_ahead_notional: Decimal::new(3000, 0),
                posted_notional: Decimal::new(500, 0),
                top_level_notional: Decimal::new(1000, 0),
                touched: true,
                time_to_expiry_seconds: 240,
                recent_cancel_replace_count: 6,
                depth_score: Decimal::new(22, 2),
            },
            &QueueModelParams::default(),
        );
        assert!(estimate.fill_probability_score < Decimal::new(70, 2));
        assert!(estimate.no_fill_probability > Decimal::new(20, 2));
    }
}
