use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use shared_types::{ReplayBundle, ReplayExportRequest, Venue};
use storage::{ReplayExporter, StorageClients};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplaySpeed {
    X1,
    X5,
    X20,
    Max,
}

impl ReplaySpeed {
    pub fn delay_millis(&self) -> u64 {
        match self {
            ReplaySpeed::X1 => 1000,
            ReplaySpeed::X5 => 200,
            ReplaySpeed::X20 => 50,
            ReplaySpeed::Max => 0,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayRequest {
    pub from_ts: DateTime<Utc>,
    pub to_ts: DateTime<Utc>,
    pub underlying_filter: Option<String>,
    pub market_filter: Option<String>,
    pub include_deribit_metadata: bool,
    pub speed: ReplaySpeed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayPlan {
    pub run_id: String,
    pub request: ReplayRequest,
    pub merged_bundle: ReplayBundle,
}

pub async fn build_replay_plan(storage: &StorageClients, request: ReplayRequest) -> Result<ReplayPlan> {
    let run_id = deterministic_run_id(&request)?;
    let base_request = ReplayExportRequest {
        venue: None,
        market_id: request.market_filter.clone(),
        from_ts: request.from_ts,
        to_ts: request.to_ts,
    };
    let mut merged_bundle = storage.export_bundle(base_request).await?;

    if let Some(underlying) = &request.underlying_filter {
        let up = underlying.to_ascii_uppercase();
        merged_bundle.normalized_events.retain(|e| {
            e.canonical_contract_key.to_ascii_uppercase().contains(&up) || e.venue_market_id.to_ascii_uppercase().contains(&up)
        });
        merged_bundle.reference_events.retain(|e| e.symbol.to_ascii_uppercase().contains(&up));
        merged_bundle.raw_events.retain(|e| {
            e.payload_json
                .to_string()
                .to_ascii_uppercase()
                .contains(&up)
        });
    }
    if !request.include_deribit_metadata {
        merged_bundle
            .raw_events
            .retain(|e| !(matches!(e.venue, Venue::Deribit) && matches!(e.event_type, shared_types::EventType::Metadata)));
    }

    Ok(ReplayPlan {
        run_id,
        request,
        merged_bundle,
    })
}

pub fn deterministic_run_id(request: &ReplayRequest) -> Result<String> {
    let bytes = serde_json::to_vec(request)?;
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    Ok(format!("replay-{:x}", hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_request_produces_same_run_id() {
        let request = ReplayRequest {
            from_ts: Utc::now(),
            to_ts: Utc::now(),
            underlying_filter: Some("BTC".to_string()),
            market_filter: None,
            include_deribit_metadata: false,
            speed: ReplaySpeed::X5,
        };
        let first = deterministic_run_id(&request).expect("run id");
        let second = deterministic_run_id(&request).expect("run id");
        assert_eq!(first, second);
    }
}
