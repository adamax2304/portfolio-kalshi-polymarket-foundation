use serde::Serialize;
use uuid::Uuid;

const FOUNDATION_NAMESPACE: Uuid = Uuid::from_u128(0x9bfe4a1f2ecf4ca1bdc8f9b3f89a0bc9);

pub fn deterministic_id(seed: &str) -> String {
    Uuid::new_v5(&FOUNDATION_NAMESPACE, seed.as_bytes()).to_string()
}

pub fn json_string<T: Serialize>(value: &T) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "{}".to_string())
}
