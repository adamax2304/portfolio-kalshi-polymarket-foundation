use anyhow::{Context, Result};
use chrono::{DateTime, TimeZone, Utc};

pub fn now_utc() -> DateTime<Utc> {
    Utc::now()
}

pub fn normalize_unix_ms(ms: i64) -> Result<DateTime<Utc>> {
    Utc.timestamp_millis_opt(ms)
        .single()
        .context("invalid unix ms timestamp")
}

pub fn parse_rfc3339_utc(raw: &str) -> Result<DateTime<Utc>> {
    let dt = DateTime::parse_from_rfc3339(raw).with_context(|| format!("invalid RFC3339: {raw}"))?;
    Ok(dt.with_timezone(&Utc))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_rfc3339() {
        let ts = parse_rfc3339_utc("2026-01-01T00:00:00Z").expect("timestamp should parse");
        assert_eq!(ts.timestamp(), 1_767_225_600);
    }
}
