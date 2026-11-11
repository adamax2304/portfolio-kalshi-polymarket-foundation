use anyhow::{Context, Result};
use rust_decimal::Decimal;
use std::str::FromStr;

pub fn parse_decimal(raw: &str) -> Result<Decimal> {
    Decimal::from_str(raw).with_context(|| format!("failed to parse decimal from '{raw}'"))
}

pub fn parse_tick_size(raw: &str) -> Result<Decimal> {
    let tick = parse_decimal(raw)?;
    if tick <= Decimal::ZERO {
        anyhow::bail!("tick size must be > 0");
    }
    Ok(tick)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_tick_size() {
        let tick = parse_tick_size("0.01").expect("tick parse should succeed");
        assert_eq!(tick.to_string(), "0.01");
    }
}
