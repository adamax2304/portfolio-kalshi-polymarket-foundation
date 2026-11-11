use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::Utc;
use clickhouse::Row;
use config::ServiceConfig;
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use shared_types::{NormalizedMarketEvent, RawVenueEvent, ReferencePriceEvent, ReplayBundle, ReplayExportRequest};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_postgres::NoTls;
use tracing::warn;

#[derive(Clone)]
pub struct StorageClients {
    pub clickhouse: clickhouse::Client,
    pub postgres: Arc<tokio_postgres::Client>,
    pub redis: redis::Client,
    pub raw_stream_key: String,
    pub normalized_stream_key: String,
    pub reference_stream_key: String,
    pg_conn_guard: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

#[derive(Debug, Serialize, Deserialize, Row)]
struct RawVenueEventRow {
    event_id: String,
    venue: String,
    event_type: String,
    received_at_ts: i64,
    source_ts: i64,
    payload_json: String,
    connection_id: String,
    sequence_hint: String,
    ingestion_status: String,
}

#[derive(Debug, Serialize, Deserialize, Row)]
struct NormalizedMarketEventRow {
    normalized_event_id: String,
    canonical_contract_key: String,
    venue: String,
    venue_market_id: String,
    event_type: String,
    event_ts: i64,
    best_bid: String,
    best_ask: String,
    last_trade_price: String,
    last_trade_size: String,
    spread: String,
    midpoint: String,
    metadata_json: String,
}

#[derive(Debug, Serialize, Deserialize, Row)]
struct ReferencePriceEventRow {
    reference_event_id: String,
    source: String,
    symbol: String,
    event_ts: i64,
    best_bid: String,
    best_ask: String,
    last_price: String,
    mid_price: String,
    metadata_json: String,
}

impl StorageClients {
    pub async fn connect(config: &ServiceConfig) -> Result<Self> {
        let clickhouse = clickhouse::Client::default()
            .with_url(&config.clickhouse.url)
            .with_database(&config.clickhouse.database)
            .with_user(&config.clickhouse.user)
            .with_password(config.clickhouse.password.expose());

        let pg_conn_str = format!(
            "host={} port={} dbname={} user={} password={}",
            config.postgres.host,
            config.postgres.port,
            config.postgres.db,
            config.postgres.user,
            config.postgres.password.expose()
        );
        let (postgres, connection) = tokio_postgres::connect(&pg_conn_str, NoTls)
            .await
            .context("postgres connect failed")?;
        let pg_conn_guard = Arc::new(Mutex::new(Some(tokio::spawn(async move {
            if let Err(err) = connection.await {
                warn!(error = %err, "postgres connection ended");
            }
        }))));

        let redis = redis::Client::open(config.redis.url.clone()).context("redis client init failed")?;
        Ok(Self {
            clickhouse,
            postgres: Arc::new(postgres),
            redis,
            raw_stream_key: config.redis.raw_stream.clone(),
            normalized_stream_key: config.redis.normalized_stream.clone(),
            reference_stream_key: config.redis.reference_stream.clone(),
            pg_conn_guard,
        })
    }

    pub async fn push_bus_event(&self, stream_key: &str, payload_json: &str) -> Result<()> {
        let mut conn = self.redis.get_multiplexed_async_connection().await?;
        let _: i64 = conn.rpush(stream_key, payload_json).await?;
        Ok(())
    }

    pub async fn pop_bus_event(&self, stream_key: &str, timeout_secs: f64) -> Result<Option<String>> {
        let mut conn = self.redis.get_multiplexed_async_connection().await?;
        let value: Option<[String; 2]> = conn.blpop(stream_key, timeout_secs).await?;
        Ok(value.map(|arr| arr[1].clone()))
    }

    pub async fn set_service_heartbeat(&self, service_name: &str) -> Result<()> {
        let mut conn = self.redis.get_multiplexed_async_connection().await?;
        let key = format!("heartbeat:{service_name}");
        let _: () = conn
            .set_ex(key, Utc::now().to_rfc3339(), 30)
            .await
            .context("set heartbeat failed")?;
        Ok(())
    }

    pub async fn should_accept_event(&self, event_id: &str) -> Result<bool> {
        let mut conn = self.redis.get_multiplexed_async_connection().await?;
        let key = format!("dedupe:event:{event_id}");
        let accepted: bool = conn.set_nx(&key, 1).await?;
        if accepted {
            let _: bool = conn.expire(key, 86_400).await?;
        }
        Ok(accepted)
    }

    pub async fn insert_raw_event(&self, event: &RawVenueEvent) -> Result<()> {
        let row = RawVenueEventRow {
            event_id: event.event_id.clone(),
            venue: format!("{:?}", event.venue).to_ascii_lowercase(),
            event_type: format!("{:?}", event.event_type).to_ascii_lowercase(),
            received_at_ts: event.received_at_ts.timestamp_millis(),
            source_ts: event.source_ts.map(|x| x.timestamp_millis()).unwrap_or(0),
            payload_json: serde_json::to_string(&event.payload_json)?,
            connection_id: event.connection_id.clone(),
            sequence_hint: event.sequence_hint.clone().unwrap_or_default(),
            ingestion_status: format!("{:?}", event.ingestion_status).to_ascii_lowercase(),
        };
        let mut insert = self.clickhouse.insert("raw_venue_events")?;
        insert.write(&row).await?;
        insert.end().await?;
        Ok(())
    }

    pub async fn insert_normalized_event(&self, event: &NormalizedMarketEvent) -> Result<()> {
        let row = NormalizedMarketEventRow {
            normalized_event_id: event.normalized_event_id.clone(),
            canonical_contract_key: event.canonical_contract_key.clone(),
            venue: format!("{:?}", event.venue).to_ascii_lowercase(),
            venue_market_id: event.venue_market_id.clone(),
            event_type: format!("{:?}", event.event_type).to_ascii_lowercase(),
            event_ts: event.event_ts.timestamp_millis(),
            best_bid: event.best_bid.map(|x| x.to_string()).unwrap_or_default(),
            best_ask: event.best_ask.map(|x| x.to_string()).unwrap_or_default(),
            last_trade_price: event.last_trade_price.map(|x| x.to_string()).unwrap_or_default(),
            last_trade_size: event.last_trade_size.map(|x| x.to_string()).unwrap_or_default(),
            spread: event.spread.map(|x| x.to_string()).unwrap_or_default(),
            midpoint: event.midpoint.map(|x| x.to_string()).unwrap_or_default(),
            metadata_json: serde_json::to_string(&event.metadata_json)?,
        };
        let mut insert = self.clickhouse.insert("normalized_market_events")?;
        insert.write(&row).await?;
        insert.end().await?;
        Ok(())
    }

    pub async fn insert_reference_event(&self, event: &ReferencePriceEvent) -> Result<()> {
        let row = ReferencePriceEventRow {
            reference_event_id: event.reference_event_id.clone(),
            source: event.source.clone(),
            symbol: event.symbol.clone(),
            event_ts: event.event_ts.timestamp_millis(),
            best_bid: event.best_bid.map(|x| x.to_string()).unwrap_or_default(),
            best_ask: event.best_ask.map(|x| x.to_string()).unwrap_or_default(),
            last_price: event.last_price.map(|x| x.to_string()).unwrap_or_default(),
            mid_price: event.mid_price.map(|x| x.to_string()).unwrap_or_default(),
            metadata_json: serde_json::to_string(&event.metadata_json)?,
        };
        let mut insert = self.clickhouse.insert("reference_price_events")?;
        insert.write(&row).await?;
        insert.end().await?;
        Ok(())
    }

    pub async fn upsert_canonical_contract(
        &self,
        canonical_contract_key: &str,
        venue: &str,
        venue_market_id: &str,
        payload_json: &str,
    ) -> Result<()> {
        self.postgres
            .execute(
                "INSERT INTO canonical_contracts (canonical_contract_key, venue, venue_market_id, metadata_json, updated_at) \
                VALUES ($1, $2, $3, $4::jsonb, NOW()) \
                ON CONFLICT (canonical_contract_key) DO UPDATE SET metadata_json = EXCLUDED.metadata_json, updated_at = NOW()",
                &[&canonical_contract_key, &venue, &venue_market_id, &payload_json],
            )
            .await
            .context("upsert canonical_contracts failed")?;
        Ok(())
    }

    pub async fn cache_latest(&self, key: &str, value_json: &str) -> Result<()> {
        let mut conn = self.redis.get_multiplexed_async_connection().await?;
        let _: () = conn.set(key, value_json).await?;
        Ok(())
    }

    pub async fn get_cached(&self, key: &str) -> Result<Option<String>> {
        let mut conn = self.redis.get_multiplexed_async_connection().await?;
        let value: Option<String> = conn.get(key).await?;
        Ok(value)
    }

    pub async fn list_cache_keys(&self, pattern: &str) -> Result<Vec<String>> {
        let mut conn = self.redis.get_multiplexed_async_connection().await?;
        let keys: Vec<String> = conn.keys(pattern).await?;
        Ok(keys)
    }

    pub async fn export_raw_events(&self, request: &ReplayExportRequest) -> Result<Vec<RawVenueEvent>> {
        let query = format!(
            "SELECT event_id, venue, event_type, received_at_ts, source_ts, payload_json, connection_id, sequence_hint, ingestion_status \
             FROM raw_venue_events WHERE received_at_ts >= {} AND received_at_ts <= {} ORDER BY received_at_ts ASC LIMIT 200000",
            request.from_ts.timestamp_millis(),
            request.to_ts.timestamp_millis()
        );
        let rows = self.clickhouse.query(&query).fetch_all::<RawVenueEventRow>().await?;
        Ok(rows
            .into_iter()
            .map(|r| RawVenueEvent {
                event_id: r.event_id,
                venue: parse_venue(&r.venue),
                event_type: parse_event_type(&r.event_type),
                received_at_ts: chrono::DateTime::from_timestamp_millis(r.received_at_ts).unwrap_or_else(Utc::now),
                source_ts: chrono::DateTime::from_timestamp_millis(r.source_ts),
                payload_json: serde_json::from_str(&r.payload_json).unwrap_or(serde_json::Value::Null),
                connection_id: r.connection_id,
                sequence_hint: if r.sequence_hint.is_empty() {
                    None
                } else {
                    Some(r.sequence_hint)
                },
                ingestion_status: shared_types::IngestionStatus::Accepted,
            })
            .collect())
    }

    pub async fn export_normalized_events(&self, request: &ReplayExportRequest) -> Result<Vec<NormalizedMarketEvent>> {
        let query = format!(
            "SELECT normalized_event_id, canonical_contract_key, venue, venue_market_id, event_type, event_ts, \
             best_bid, best_ask, last_trade_price, last_trade_size, spread, midpoint, metadata_json \
             FROM normalized_market_events WHERE event_ts >= {} AND event_ts <= {} ORDER BY event_ts ASC LIMIT 200000",
            request.from_ts.timestamp_millis(),
            request.to_ts.timestamp_millis()
        );
        let rows = self
            .clickhouse
            .query(&query)
            .fetch_all::<NormalizedMarketEventRow>()
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| NormalizedMarketEvent {
                normalized_event_id: r.normalized_event_id,
                canonical_contract_key: r.canonical_contract_key,
                venue: parse_venue(&r.venue),
                venue_market_id: r.venue_market_id,
                event_type: parse_event_type(&r.event_type),
                event_ts: chrono::DateTime::from_timestamp_millis(r.event_ts).unwrap_or_else(Utc::now),
                best_bid: r.best_bid.parse().ok(),
                best_ask: r.best_ask.parse().ok(),
                last_trade_price: r.last_trade_price.parse().ok(),
                last_trade_size: r.last_trade_size.parse().ok(),
                spread: r.spread.parse().ok(),
                midpoint: r.midpoint.parse().ok(),
                metadata_json: serde_json::from_str(&r.metadata_json).unwrap_or(serde_json::Value::Null),
            })
            .collect())
    }

    pub async fn export_reference_events(&self, request: &ReplayExportRequest) -> Result<Vec<ReferencePriceEvent>> {
        let query = format!(
            "SELECT reference_event_id, source, symbol, event_ts, best_bid, best_ask, last_price, mid_price, metadata_json \
             FROM reference_price_events WHERE event_ts >= {} AND event_ts <= {} ORDER BY event_ts ASC LIMIT 200000",
            request.from_ts.timestamp_millis(),
            request.to_ts.timestamp_millis()
        );
        let rows = self
            .clickhouse
            .query(&query)
            .fetch_all::<ReferencePriceEventRow>()
            .await?;
        Ok(rows
            .into_iter()
            .map(|r| ReferencePriceEvent {
                reference_event_id: r.reference_event_id,
                source: r.source,
                symbol: r.symbol,
                event_ts: chrono::DateTime::from_timestamp_millis(r.event_ts).unwrap_or_else(Utc::now),
                best_bid: r.best_bid.parse().ok(),
                best_ask: r.best_ask.parse().ok(),
                last_price: r.last_price.parse().ok(),
                mid_price: r.mid_price.parse().ok(),
                metadata_json: serde_json::from_str(&r.metadata_json).unwrap_or(serde_json::Value::Null),
            })
            .collect())
    }
}

impl Drop for StorageClients {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.pg_conn_guard.try_lock() {
            if let Some(handle) = guard.take() {
                handle.abort();
            }
        }
    }
}

fn parse_venue(raw: &str) -> shared_types::Venue {
    match raw {
        "polymarket" => shared_types::Venue::Polymarket,
        "kalshi" => shared_types::Venue::Kalshi,
        "binance" => shared_types::Venue::Binance,
        "deribit" => shared_types::Venue::Deribit,
        _ => shared_types::Venue::Internal,
    }
}

fn parse_event_type(raw: &str) -> shared_types::EventType {
    match raw {
        "market" => shared_types::EventType::Market,
        "orderbooksnapshot" => shared_types::EventType::OrderbookSnapshot,
        "orderbookdelta" => shared_types::EventType::OrderbookDelta,
        "trade" => shared_types::EventType::Trade,
        "status" => shared_types::EventType::Status,
        "referenceprice" => shared_types::EventType::ReferencePrice,
        "metadata" => shared_types::EventType::Metadata,
        "heartbeat" => shared_types::EventType::Heartbeat,
        _ => shared_types::EventType::Unknown,
    }
}

#[async_trait]
pub trait ReplayExporter {
    async fn export_bundle(&self, request: ReplayExportRequest) -> Result<ReplayBundle>;
}

#[async_trait]
impl ReplayExporter for StorageClients {
    async fn export_bundle(&self, request: ReplayExportRequest) -> Result<ReplayBundle> {
        let raw_events = self.export_raw_events(&request).await?;
        let normalized_events = self.export_normalized_events(&request).await?;
        let reference_events = self.export_reference_events(&request).await?;
        Ok(ReplayBundle {
            raw_events,
            normalized_events,
            reference_events,
        })
    }
}
