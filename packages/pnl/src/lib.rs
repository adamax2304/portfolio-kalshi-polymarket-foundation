use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FillRecord {
    pub ts: DateTime<Utc>,
    pub run_id: String,
    pub signal_id: String,
    pub canonical_contract_key: String,
    pub underlying: String,
    pub venue: String,
    pub execution_mode: String,
    pub qty: Decimal,
    pub price: Decimal,
    pub fees: Decimal,
    pub slippage_bps: Decimal,
    pub side: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PositionPnlState {
    pub qty: Decimal,
    pub avg_price: Decimal,
    pub realized_pnl: Decimal,
    pub total_fees: Decimal,
    pub total_slippage_bps: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PnlSnapshot {
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub mark_to_mid_pnl: Decimal,
    pub mark_to_executable_pnl_proxy: Decimal,
    pub fees_proxy: Decimal,
    pub slippage_proxy: Decimal,
    pub edge_capture_ratio: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarkoutResult {
    pub markout_1s_bps: Decimal,
    pub markout_5s_bps: Decimal,
    pub markout_15s_bps: Decimal,
    pub markout_expiry_bps: Decimal,
}

pub fn apply_fill(state: &mut PositionPnlState, fill: &FillRecord) {
    let signed_qty = if fill.side.eq_ignore_ascii_case("buy") {
        fill.qty
    } else {
        -fill.qty
    };

    if state.qty == Decimal::ZERO
        || (state.qty > Decimal::ZERO && signed_qty > Decimal::ZERO)
        || (state.qty < Decimal::ZERO && signed_qty < Decimal::ZERO)
    {
        let total_notional = state.avg_price * state.qty.abs() + fill.price * fill.qty.abs();
        let total_qty = state.qty.abs() + fill.qty.abs();
        state.qty += signed_qty;
        state.avg_price = if total_qty > Decimal::ZERO {
            total_notional / total_qty
        } else {
            Decimal::ZERO
        };
    } else {
        let close_qty = signed_qty.abs().min(state.qty.abs());
        let pnl_piece = if state.qty > Decimal::ZERO {
            (fill.price - state.avg_price) * close_qty
        } else {
            (state.avg_price - fill.price) * close_qty
        };
        state.realized_pnl += pnl_piece;
        state.qty += signed_qty;
        if state.qty == Decimal::ZERO {
            state.avg_price = Decimal::ZERO;
        }
    }
    state.total_fees += fill.fees;
    state.total_slippage_bps += fill.slippage_bps;
}

pub fn snapshot_pnl(
    state: &PositionPnlState,
    mark_mid_price: Decimal,
    executable_price: Decimal,
    gross_edge_bps: Decimal,
) -> PnlSnapshot {
    let unrealized_mid = if state.qty >= Decimal::ZERO {
        (mark_mid_price - state.avg_price) * state.qty.abs()
    } else {
        (state.avg_price - mark_mid_price) * state.qty.abs()
    };
    let unrealized_exec = if state.qty >= Decimal::ZERO {
        (executable_price - state.avg_price) * state.qty.abs()
    } else {
        (state.avg_price - executable_price) * state.qty.abs()
    };
    let captured = if gross_edge_bps > Decimal::ZERO {
        ((state.realized_pnl + unrealized_exec) / gross_edge_bps).max(Decimal::ZERO)
    } else {
        Decimal::ZERO
    };
    PnlSnapshot {
        realized_pnl: state.realized_pnl,
        unrealized_pnl: unrealized_exec,
        mark_to_mid_pnl: state.realized_pnl + unrealized_mid,
        mark_to_executable_pnl_proxy: state.realized_pnl + unrealized_exec,
        fees_proxy: state.total_fees,
        slippage_proxy: state.total_slippage_bps,
        edge_capture_ratio: captured,
    }
}

pub fn markout_bps(entry_price: Decimal, px_1s: Decimal, px_5s: Decimal, px_15s: Decimal, px_expiry: Decimal) -> MarkoutResult {
    fn one(entry: Decimal, next: Decimal) -> Decimal {
        if entry == Decimal::ZERO {
            Decimal::ZERO
        } else {
            ((next - entry) / entry) * Decimal::new(10_000, 0)
        }
    }
    MarkoutResult {
        markout_1s_bps: one(entry_price, px_1s),
        markout_5s_bps: one(entry_price, px_5s),
        markout_15s_bps: one(entry_price, px_15s),
        markout_expiry_bps: one(entry_price, px_expiry),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn computes_realized_pnl_on_close() {
        let mut state = PositionPnlState::default();
        apply_fill(
            &mut state,
            &FillRecord {
                ts: Utc::now(),
                run_id: "r".to_string(),
                signal_id: "s".to_string(),
                canonical_contract_key: "c".to_string(),
                underlying: "BTC".to_string(),
                venue: "kalshi".to_string(),
                execution_mode: "maker".to_string(),
                qty: Decimal::new(10, 0),
                price: Decimal::new(45, 2),
                fees: Decimal::new(1, 2),
                slippage_bps: Decimal::new(2, 0),
                side: "buy".to_string(),
            },
        );
        apply_fill(
            &mut state,
            &FillRecord {
                ts: Utc::now(),
                run_id: "r".to_string(),
                signal_id: "s".to_string(),
                canonical_contract_key: "c".to_string(),
                underlying: "BTC".to_string(),
                venue: "kalshi".to_string(),
                execution_mode: "maker".to_string(),
                qty: Decimal::new(10, 0),
                price: Decimal::new(52, 2),
                fees: Decimal::new(1, 2),
                slippage_bps: Decimal::new(2, 0),
                side: "sell".to_string(),
            },
        );
        assert!(state.realized_pnl > Decimal::ZERO);
        assert_eq!(state.qty, Decimal::ZERO);
    }
}
