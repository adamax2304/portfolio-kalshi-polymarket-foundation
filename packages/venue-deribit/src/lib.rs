use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::json;
use shared_types::{EventType, IngestionStatus, RawVenueEvent, Venue};
use utils::deterministic_id;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeribitInstrumentEventDto {
    pub instrument_name: String,
    pub kind: String,
    pub expiration_timestamp_ms: Option<i64>,
    pub strike: Option<String>,
    pub mark_price: Option<String>,
}

pub fn to_raw_event(dto: &DeribitInstrumentEventDto, connection_id: &str) -> RawVenueEvent {
    let seed = format!(
        "deribit:{}:{}:{:?}:{:?}:{:?}",
        dto.instrument_name, dto.kind, dto.expiration_timestamp_ms, dto.strike, dto.mark_price
    );
    RawVenueEvent {
        event_id: deterministic_id(&seed),
        venue: Venue::Deribit,
        event_type: EventType::Metadata,
        received_at_ts: Utc::now(),
        source_ts: None,
        payload_json: json!({
            "instrument_name": dto.instrument_name,
            "kind": dto.kind,
            "expiration_timestamp_ms": dto.expiration_timestamp_ms,
            "strike": dto.strike,
            "mark_price": dto.mark_price
        }),
        connection_id: connection_id.to_owned(),
        sequence_hint: None,
        ingestion_status: IngestionStatus::Accepted,
    }
}
