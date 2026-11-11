use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::json;
use shared_types::{EventType, IngestionStatus, RawVenueEvent, Venue};
use utils::deterministic_id;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PolymarketOrderbookEventDto {
    pub market: String,
    pub token_id: Option<String>,
    pub best_bid: Option<String>,
    pub best_ask: Option<String>,
    pub last_trade_price: Option<String>,
    pub seq: Option<String>,
    pub event_ts_ms: Option<i64>,
}

pub fn to_raw_event(dto: &PolymarketOrderbookEventDto, connection_id: &str) -> RawVenueEvent {
    let seed = format!(
        "polymarket:{}:{:?}:{:?}:{:?}:{:?}",
        dto.market, dto.best_bid, dto.best_ask, dto.last_trade_price, dto.seq
    );
    RawVenueEvent {
        event_id: deterministic_id(&seed),
        venue: Venue::Polymarket,
        event_type: EventType::OrderbookDelta,
        received_at_ts: Utc::now(),
        source_ts: None,
        payload_json: json!({
            "market": dto.market,
            "token_id": dto.token_id,
            "best_bid": dto.best_bid,
            "best_ask": dto.best_ask,
            "last_trade_price": dto.last_trade_price,
            "seq": dto.seq,
            "event_ts_ms": dto.event_ts_ms
        }),
        connection_id: connection_id.to_owned(),
        sequence_hint: dto.seq.clone(),
        ingestion_status: IngestionStatus::Accepted,
    }
}
