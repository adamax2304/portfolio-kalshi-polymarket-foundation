use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::json;
use shared_types::{EventType, IngestionStatus, RawVenueEvent, Venue};
use utils::deterministic_id;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BinanceBookTickerDto {
    pub symbol: String,
    pub bid_price: String,
    pub ask_price: String,
    pub event_ts_ms: i64,
}

pub fn to_raw_event(dto: &BinanceBookTickerDto, connection_id: &str) -> RawVenueEvent {
    let seed = format!(
        "binance:{}:{}:{}:{}",
        dto.symbol, dto.bid_price, dto.ask_price, dto.event_ts_ms
    );
    RawVenueEvent {
        event_id: deterministic_id(&seed),
        venue: Venue::Binance,
        event_type: EventType::ReferencePrice,
        received_at_ts: Utc::now(),
        source_ts: None,
        payload_json: json!({
            "symbol": dto.symbol,
            "bid_price": dto.bid_price,
            "ask_price": dto.ask_price,
            "event_ts_ms": dto.event_ts_ms
        }),
        connection_id: connection_id.to_owned(),
        sequence_hint: None,
        ingestion_status: IngestionStatus::Accepted,
    }
}
