use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookLevel {
    pub price: Decimal,
    pub size: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderbookSnapshot {
    pub venue: String,
    pub market_id: String,
    pub ts: DateTime<Utc>,
    pub bids: Vec<BookLevel>,
    pub asks: Vec<BookLevel>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderbookDelta {
    pub venue: String,
    pub market_id: String,
    pub ts: DateTime<Utc>,
    pub side: String,
    pub price: Decimal,
    pub size: Decimal,
}
