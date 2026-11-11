use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::env;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnvironmentProfile {
    Local,
    Dev,
    Paper,
}

impl EnvironmentProfile {
    fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "local" => Ok(Self::Local),
            "dev" => Ok(Self::Dev),
            "paper" => Ok(Self::Paper),
            other => bail!("unsupported ENV_PROFILE: {other}"),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServiceConfig {
    pub profile: EnvironmentProfile,
    pub service_name: String,
    pub build_version: String,
    pub instance_id: String,
    pub port: u16,
    pub log_level: String,
    pub redis: RedisConfig,
    pub clickhouse: ClickHouseConfig,
    pub postgres: PostgresConfig,
    pub venue: VenueConfig,
    pub prometheus_namespace: String,
}

impl ServiceConfig {
    pub fn from_env(service_name: &str, port_env_key: &str) -> Result<Self> {
        let _ = dotenvy::dotenv();

        let profile = EnvironmentProfile::parse(&read_var("ENV_PROFILE", Some("local"))?)?;
        let port = read_var(port_env_key, None)?
            .parse::<u16>()
            .with_context(|| format!("failed to parse {port_env_key} as u16"))?;
        let config = Self {
            profile,
            service_name: service_name.to_owned(),
            build_version: read_var("BUILD_VERSION", Some("dev"))?,
            instance_id: read_var("INSTANCE_ID", Some("local"))?,
            port,
            log_level: read_var("LOG_LEVEL", Some("info"))?,
            redis: RedisConfig::from_env()?,
            clickhouse: ClickHouseConfig::from_env()?,
            postgres: PostgresConfig::from_env()?,
            venue: VenueConfig::from_env()?,
            prometheus_namespace: read_var("PROMETHEUS_NAMESPACE", Some("phase1"))?,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if self.service_name.trim().is_empty() {
            bail!("service_name is required");
        }
        if self.port == 0 {
            bail!("port must be > 0");
        }
        if self.log_level.trim().is_empty() {
            bail!("log_level is required");
        }
        self.redis.validate()?;
        self.clickhouse.validate()?;
        self.postgres.validate()?;
        Ok(())
    }

    pub fn fingerprint(&self) -> String {
        let payload = format!(
            "{}:{}:{}:{}:{:?}",
            self.service_name, self.instance_id, self.port, self.log_level, self.profile
        );
        let mut hasher = Sha256::new();
        hasher.update(payload.as_bytes());
        format!("{:x}", hasher.finalize())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisConfig {
    pub url: String,
    pub raw_stream: String,
    pub normalized_stream: String,
    pub reference_stream: String,
}

impl RedisConfig {
    fn from_env() -> Result<Self> {
        Ok(Self {
            url: read_var("REDIS_URL", None)?,
            raw_stream: read_var("REDIS_STREAM_RAW_EVENTS", Some("bus.raw_events"))?,
            normalized_stream: read_var("REDIS_STREAM_NORMALIZED_EVENTS", Some("bus.normalized_events"))?,
            reference_stream: read_var("REDIS_STREAM_REFERENCE_EVENTS", Some("bus.reference_events"))?,
        })
    }

    fn validate(&self) -> Result<()> {
        if !self.url.starts_with("redis://") {
            bail!("REDIS_URL must start with redis://");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClickHouseConfig {
    pub url: String,
    pub database: String,
    pub user: String,
    pub password: SecretValue,
}

impl ClickHouseConfig {
    fn from_env() -> Result<Self> {
        Ok(Self {
            url: read_var("CLICKHOUSE_URL", None)?,
            database: read_var("CLICKHOUSE_DATABASE", None)?,
            user: read_var("CLICKHOUSE_USER", Some("default"))?,
            password: SecretValue::from_env("CLICKHOUSE_PASSWORD", Some(""))?,
        })
    }

    fn validate(&self) -> Result<()> {
        if self.url.trim().is_empty() {
            bail!("CLICKHOUSE_URL is required");
        }
        if self.database.trim().is_empty() {
            bail!("CLICKHOUSE_DATABASE is required");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostgresConfig {
    pub host: String,
    pub port: u16,
    pub db: String,
    pub user: String,
    pub password: SecretValue,
}

impl PostgresConfig {
    fn from_env() -> Result<Self> {
        Ok(Self {
            host: read_var("POSTGRES_HOST", None)?,
            port: read_var("POSTGRES_PORT", None)?
                .parse::<u16>()
                .context("failed to parse POSTGRES_PORT as u16")?,
            db: read_var("POSTGRES_DB", None)?,
            user: read_var("POSTGRES_USER", None)?,
            password: SecretValue::from_env("POSTGRES_PASSWORD", None)?,
        })
    }

    fn validate(&self) -> Result<()> {
        if self.host.trim().is_empty() {
            bail!("POSTGRES_HOST is required");
        }
        if self.db.trim().is_empty() {
            bail!("POSTGRES_DB is required");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VenueConfig {
    pub polymarket_ws_url: String,
    pub polymarket_rtds_url: String,
    pub polymarket_api_key: SecretValue,
    pub polymarket_api_secret: SecretValue,
    pub polymarket_passphrase: SecretValue,
    pub kalshi_ws_url: String,
    pub kalshi_api_key: SecretValue,
    pub kalshi_api_secret: SecretValue,
    pub binance_ws_url: String,
    pub binance_symbol: String,
    pub deribit_ws_url: String,
    pub deribit_currency: String,
}

impl VenueConfig {
    fn from_env() -> Result<Self> {
        Ok(Self {
            polymarket_ws_url: read_var("POLYMARKET_WS_URL", None)?,
            polymarket_rtds_url: read_var("POLYMARKET_RTDS_URL", None)?,
            polymarket_api_key: SecretValue::from_env("POLYMARKET_API_KEY", Some("REPLACE_ME"))?,
            polymarket_api_secret: SecretValue::from_env("POLYMARKET_API_SECRET", Some("REPLACE_ME"))?,
            polymarket_passphrase: SecretValue::from_env("POLYMARKET_PASSPHRASE", Some("REPLACE_ME"))?,
            kalshi_ws_url: read_var("KALSHI_WS_URL", None)?,
            kalshi_api_key: SecretValue::from_env("KALSHI_API_KEY", Some("REPLACE_ME"))?,
            kalshi_api_secret: SecretValue::from_env("KALSHI_API_SECRET", Some("REPLACE_ME"))?,
            binance_ws_url: read_var("BINANCE_WS_URL", None)?,
            binance_symbol: read_var("BINANCE_SYMBOL", Some("BTCUSDT"))?,
            deribit_ws_url: read_var("DERIBIT_WS_URL", None)?,
            deribit_currency: read_var("DERIBIT_CURRENCY", Some("BTC"))?,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecretValue(String);

impl SecretValue {
    pub fn from_env(key: &str, default: Option<&str>) -> Result<Self> {
        Ok(Self(read_var(key, default)?))
    }

    pub fn expose(&self) -> &str {
        &self.0
    }

    pub fn redacted(&self) -> &'static str {
        "***"
    }
}

pub fn read_var(key: &str, default: Option<&str>) -> Result<String> {
    match env::var(key) {
        Ok(v) if !v.trim().is_empty() => Ok(v),
        Ok(_) => {
            if let Some(d) = default {
                Ok(d.to_owned())
            } else {
                Err(anyhow!("{key} is empty"))
            }
        }
        Err(_) => {
            if let Some(d) = default {
                Ok(d.to_owned())
            } else {
                Err(anyhow!("{key} is not set"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_profile() {
        assert!(matches!(
            EnvironmentProfile::parse("local").expect("profile parse should succeed"),
            EnvironmentProfile::Local
        ));
    }

    #[test]
    fn read_var_uses_default() {
        let key = "PHASE1_CFG_TEST_DEFAULT";
        std::env::remove_var(key);
        let value = read_var(key, Some("fallback")).expect("default should be returned");
        assert_eq!(value, "fallback");
    }
}
