use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Venue {
    Polymarket,
    Kalshi,
    Binance,
    Deribit,
    Internal,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum EventType {
    Market,
    OrderbookSnapshot,
    OrderbookDelta,
    Trade,
    Status,
    ReferencePrice,
    Metadata,
    Heartbeat,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum IngestionStatus {
    Accepted,
    DroppedDuplicate,
    Rejected,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum MappingState {
    Resolved,
    Unresolved,
    Flagged,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum HealthState {
    Healthy,
    Degraded,
    Unhealthy,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionStatus {
    Connected,
    Connecting,
    Reconnecting,
    Disconnected,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawVenueEvent {
    pub event_id: String,
    pub venue: Venue,
    pub event_type: EventType,
    pub received_at_ts: DateTime<Utc>,
    pub source_ts: Option<DateTime<Utc>>,
    pub payload_json: Value,
    pub connection_id: String,
    pub sequence_hint: Option<String>,
    pub ingestion_status: IngestionStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NormalizedMarketEvent {
    pub normalized_event_id: String,
    pub canonical_contract_key: String,
    pub venue: Venue,
    pub venue_market_id: String,
    pub event_type: EventType,
    pub event_ts: DateTime<Utc>,
    pub best_bid: Option<Decimal>,
    pub best_ask: Option<Decimal>,
    pub last_trade_price: Option<Decimal>,
    pub last_trade_size: Option<Decimal>,
    pub spread: Option<Decimal>,
    pub midpoint: Option<Decimal>,
    pub metadata_json: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanonicalContract {
    pub canonical_contract_key: String,
    pub venue: Venue,
    pub venue_market_id: String,
    pub underlying: String,
    pub contract_family: String,
    pub strike: Option<Decimal>,
    pub threshold_type: Option<String>,
    pub expiration_ts: DateTime<Utc>,
    pub quote_currency: String,
    pub settlement_source: Option<String>,
    pub settlement_window_seconds: Option<i64>,
    pub tick_size: Decimal,
    pub fee_profile_json: Value,
    pub reward_profile_json: Value,
    pub status: String,
    pub metadata_json: Value,
    pub mapping_state: MappingState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReferencePriceEvent {
    pub reference_event_id: String,
    pub source: String,
    pub symbol: String,
    pub event_ts: DateTime<Utc>,
    pub best_bid: Option<Decimal>,
    pub best_ask: Option<Decimal>,
    pub last_price: Option<Decimal>,
    pub mid_price: Option<Decimal>,
    pub metadata_json: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamHealth {
    pub service_name: String,
    pub venue: Venue,
    pub connection_status: ConnectionStatus,
    pub last_message_ts: Option<DateTime<Utc>>,
    pub reconnect_count: u64,
    pub dropped_message_estimate: u64,
    pub current_latency_ms: Option<u64>,
    pub health_state: HealthState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceInfo {
    pub name: String,
    pub instance_id: String,
    pub build_version: String,
    pub profile: String,
    pub started_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub service: ServiceInfo,
    pub health_state: HealthState,
    pub details: HashMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayExportRequest {
    pub venue: Option<Venue>,
    pub market_id: Option<String>,
    pub from_ts: DateTime<Utc>,
    pub to_ts: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayBundle {
    pub raw_events: Vec<RawVenueEvent>,
    pub normalized_events: Vec<NormalizedMarketEvent>,
    pub reference_events: Vec<ReferencePriceEvent>,
}

#[derive(Debug, thiserror::Error)]
pub enum ContractValidationError {
    #[error("tick_size must be positive")]
    InvalidTickSize,
    #[error("expiration_ts must be in UTC and parseable")]
    InvalidExpiration,
    #[error("canonical_contract_key must be non-empty")]
    MissingCanonicalKey,
}

impl CanonicalContract {
    pub fn validate(&self) -> Result<(), ContractValidationError> {
        if self.canonical_contract_key.trim().is_empty() {
            return Err(ContractValidationError::MissingCanonicalKey);
        }
        if self.tick_size <= Decimal::ZERO {
            return Err(ContractValidationError::InvalidTickSize);
        }
        if self.expiration_ts.timestamp() <= 0 {
            return Err(ContractValidationError::InvalidExpiration);
        }
        Ok(())
    }
}

#[async_trait]
pub trait FairValueProvider: Send + Sync {
    async fn fair_value(&self, canonical_contract_key: &str) -> anyhow::Result<Option<Decimal>>;
}

#[async_trait]
pub trait SignalProducer: Send + Sync {
    async fn produce_signal(&self, canonical_contract_key: &str) -> anyhow::Result<Value>;
}

#[async_trait]
pub trait ExecutionAdapter: Send + Sync {
    async fn submit_order(&self, _payload: Value) -> anyhow::Result<()> {
        Ok(())
    }
}

#[async_trait]
pub trait RiskEvaluator: Send + Sync {
    async fn evaluate(&self, signal: &Value) -> anyhow::Result<Value>;
}

#[async_trait]
pub trait ReplayDataProvider: Send + Sync {
    async fn export_bundle(&self, request: ReplayExportRequest) -> anyhow::Result<ReplayBundle>;
}

#[async_trait]
pub trait ReferencePriceProvider: Send + Sync {
    async fn latest_reference(&self, symbol: &str) -> anyhow::Result<Option<ReferencePriceEvent>>;
}

#[async_trait]
pub trait ContractMetadataProvider: Send + Sync {
    async fn get_contract(&self, canonical_contract_key: &str) -> anyhow::Result<Option<CanonicalContract>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_contract_validation_rejects_invalid_tick() {
        let contract = CanonicalContract {
            canonical_contract_key: "key".to_string(),
            venue: Venue::Polymarket,
            venue_market_id: "m1".to_string(),
            underlying: "BTC".to_string(),
            contract_family: "binary".to_string(),
            strike: None,
            threshold_type: None,
            expiration_ts: Utc::now(),
            quote_currency: "USD".to_string(),
            settlement_source: None,
            settlement_window_seconds: None,
            tick_size: Decimal::ZERO,
            fee_profile_json: Value::Null,
            reward_profile_json: Value::Null,
            status: "active".to_string(),
            metadata_json: Value::Null,
            mapping_state: MappingState::Resolved,
        };

        let validation = contract.validate();
        assert!(validation.is_err());
    }
}
