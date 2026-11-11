use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::json;
use shared_types::{EventType, IngestionStatus, RawVenueEvent, Venue};
use utils::deterministic_id;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KalshiOrderbookUpdateDto {
    pub market_ticker: String,
    pub yes_bid: Option<String>,
    pub yes_ask: Option<String>,
    pub no_bid: Option<String>,
    pub no_ask: Option<String>,
    pub seq: Option<String>,
    pub event_ts_ms: Option<i64>,
}

pub fn to_raw_event(dto: &KalshiOrderbookUpdateDto, connection_id: &str) -> RawVenueEvent {
    let seed = format!(
        "kalshi:{}:{:?}:{:?}:{:?}:{:?}:{:?}",
        dto.market_ticker, dto.yes_bid, dto.yes_ask, dto.no_bid, dto.no_ask, dto.seq
    );
    RawVenueEvent {
        event_id: deterministic_id(&seed),
        venue: Venue::Kalshi,
        event_type: EventType::OrderbookDelta,
        received_at_ts: Utc::now(),
        source_ts: None,
        payload_json: json!({
            "market_ticker": dto.market_ticker,
            "yes_bid": dto.yes_bid,
            "yes_ask": dto.yes_ask,
            "no_bid": dto.no_bid,
            "no_ask": dto.no_ask,
            "seq": dto.seq,
            "event_ts_ms": dto.event_ts_ms
        }),
        connection_id: connection_id.to_owned(),
        sequence_hint: dto.seq.clone(),
        ingestion_status: IngestionStatus::Accepted,
    }
}
