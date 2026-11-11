use anyhow::Result;
use clap::{Parser, Subcommand};
use config::{read_var, ServiceConfig};
use redis::AsyncCommands;
use reqwest::Client;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::Path;
use tokio::process::Command;

#[derive(Parser)]
#[command(name = "ops-cli")]
#[command(about = "Phase 1 operational CLI for Kalshi + Polymarket foundation")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    SystemHealth,
    ListServices,
    ListStreams,
    ListContracts,
    InspectContract { id: String },
    InspectStream { venue: String },
    ExportReplay {
        #[arg(long)]
        venue: Option<String>,
        #[arg(long)]
        from: String,
        #[arg(long)]
        to: String,
    },
    TailAlerts,
    ResyncService { name: String },
    ClearCache { keyspace: String },
    Replay {
        #[command(subcommand)]
        command: ReplayCommands,
    },
    Paper {
        #[command(subcommand)]
        command: PaperCommands,
    },
    Signals {
        #[command(subcommand)]
        command: SignalsCommands,
    },
    Fair {
        #[command(subcommand)]
        command: FairCommands,
    },
    Regime {
        #[command(subcommand)]
        command: RegimeCommands,
    },
    Export {
        #[command(subcommand)]
        command: ExportCommands,
    },
    Run {
        #[command(subcommand)]
        command: RunCommands,
    },
}

#[derive(Subcommand)]
enum ReplayCommands {
    Run {
        #[arg(long)]
        from: String,
        #[arg(long)]
        to: String,
        #[arg(long, default_value = "BTC")]
        underlying: String,
        #[arg(long, default_value = "paper")]
        mode: String,
        #[arg(long, default_value = "x5")]
        speed: String,
    },
    Compare {
        #[arg(long)]
        run_a: String,
        #[arg(long)]
        run_b: String,
    },
}

#[derive(Subcommand)]
enum PaperCommands {
    Status,
    Positions,
}

#[derive(Subcommand)]
enum SignalsCommands {
    Tail,
    Stats,
}

#[derive(Subcommand)]
enum FairCommands {
    Inspect { contract: String },
}

#[derive(Subcommand)]
enum RegimeCommands {
    Inspect { contract: String },
}

#[derive(Subcommand)]
enum ExportCommands {
    Analytics { run_id: String },
}

#[derive(Subcommand)]
enum RunCommands {
    ParameterSweep { profile: String },
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    let cli = Cli::parse();
    let http = Client::new();
    match cli.command {
        Commands::SystemHealth => {
            for (name, url) in service_urls()? {
                print_fetch(&http, &format!("{url}/health"), &name).await;
            }
        }
        Commands::ListServices => {
            let metadata_url = read_var("METADATA_SERVICE_URL", Some("http://localhost:7012"))?;
            print_fetch(&http, &format!("{metadata_url}/services"), "services").await;
        }
        Commands::ListStreams => {
            for (name, url) in stream_urls()? {
                print_fetch(&http, &format!("{url}/health"), &name).await;
            }
        }
        Commands::ListContracts => {
            let metadata_url = read_var("METADATA_SERVICE_URL", Some("http://localhost:7012"))?;
            print_fetch(&http, &format!("{metadata_url}/contracts"), "contracts").await;
        }
        Commands::InspectContract { id } => {
            let metadata_url = read_var("METADATA_SERVICE_URL", Some("http://localhost:7012"))?;
            print_fetch(&http, &format!("{metadata_url}/contracts/{id}"), "contract").await;
        }
        Commands::InspectStream { venue } => {
            if let Some(url) = stream_urls()?.remove(&venue.to_ascii_lowercase()) {
                print_fetch(&http, &format!("{url}/health"), &venue).await;
            } else {
                println!("unknown venue: {venue}");
            }
        }
        Commands::ExportReplay { venue, from, to } => {
            let recorder_url = read_var("RECORDER_SERVICE_URL", Some("http://localhost:7010"))?;
            let mut url = format!("{recorder_url}/export/bundle?from={from}&to={to}");
            if let Some(v) = venue {
                url.push_str(&format!("&venue={v}"));
            }
            print_fetch(&http, &url, "replay_bundle").await;
        }
        Commands::TailAlerts => {
            let metadata_url = read_var("METADATA_SERVICE_URL", Some("http://localhost:7012"))?;
            print_fetch(&http, &format!("{metadata_url}/mappings/unresolved"), "alerts").await;
        }
        Commands::ResyncService { name } => {
            println!(
                "resync placeholder: service={name}. In phase 2 this will trigger targeted backfill/reconnect orchestration."
            );
        }
        Commands::ClearCache { keyspace } => {
            clear_cache_keyspace(&keyspace).await?;
        }
        Commands::Replay { command } => match command {
            ReplayCommands::Run {
                from,
                to,
                underlying,
                mode,
                speed,
            } => run_replay(&from, &to, &underlying, &mode, &speed).await?,
            ReplayCommands::Compare { run_a, run_b } => {
                let analytics = read_var("ANALYTICS_ENGINE_URL", Some("http://localhost:7016"))?;
                let a = fetch_json(&http, &format!("{analytics}/analytics/run/{run_a}")).await;
                let b = fetch_json(&http, &format!("{analytics}/analytics/run/{run_b}")).await;
                println!(
                    "{}",
                    serde_json::to_string_pretty(&serde_json::json!({
                        "run_a": a,
                        "run_b": b
                    }))?
                );
            }
        },
        Commands::Paper { command } => match command {
            PaperCommands::Status => {
                let url = read_var("PAPER_EXECUTION_URL", Some("http://localhost:7015"))?;
                print_fetch(&http, &format!("{url}/paper/status"), "paper_status").await;
            }
            PaperCommands::Positions => {
                let url = read_var("PAPER_EXECUTION_URL", Some("http://localhost:7015"))?;
                print_fetch(&http, &format!("{url}/paper/positions"), "paper_positions").await;
            }
        },
        Commands::Signals { command } => match command {
            SignalsCommands::Tail => {
                let url = read_var("SIGNAL_ENGINE_URL", Some("http://localhost:7014"))?;
                print_fetch(&http, &format!("{url}/signals/tail"), "signals_tail").await;
            }
            SignalsCommands::Stats => {
                let url = read_var("SIGNAL_ENGINE_URL", Some("http://localhost:7014"))?;
                print_fetch(&http, &format!("{url}/signals/stats"), "signals_stats").await;
            }
        },
        Commands::Fair { command } => match command {
            FairCommands::Inspect { contract } => {
                let url = read_var("FAIR_VALUE_ENGINE_URL", Some("http://localhost:7013"))?;
                print_fetch(&http, &format!("{url}/fair/latest/{contract}"), "fair_inspect").await;
            }
        },
        Commands::Regime { command } => match command {
            RegimeCommands::Inspect { contract } => {
                let url = read_var("FAIR_VALUE_ENGINE_URL", Some("http://localhost:7013"))?;
                print_fetch(&http, &format!("{url}/fair/latest/{contract}"), "regime_inspect").await;
            }
        },
        Commands::Export { command } => match command {
            ExportCommands::Analytics { run_id } => {
                let url = read_var("ANALYTICS_ENGINE_URL", Some("http://localhost:7016"))?;
                print_fetch(&http, &format!("{url}/analytics/run/{run_id}"), "run_analytics").await;
            }
        },
        Commands::Run { command } => match command {
            RunCommands::ParameterSweep { profile } => {
                run_parameter_sweep(&profile).await?;
            }
        }
    }
    Ok(())
}

async fn fetch_json(http: &Client, url: &str) -> Value {
    match http.get(url).send().await {
        Ok(resp) => resp.json::<Value>().await.unwrap_or_else(|_| serde_json::json!({"error":"invalid_json"})),
        Err(err) => serde_json::json!({"error":err.to_string()}),
    }
}

async fn print_fetch(http: &Client, url: &str, label: &str) {
    match http.get(url).send().await {
        Ok(resp) => match resp.json::<Value>().await {
            Ok(json) => println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({
                    "label": label,
                    "url": url,
                    "response": json
                }))
                .unwrap_or_else(|_| "{}".to_string())
            ),
            Err(err) => println!("{{\"label\":\"{label}\",\"url\":\"{url}\",\"error\":\"{err}\"}}"),
        },
        Err(err) => println!("{{\"label\":\"{label}\",\"url\":\"{url}\",\"error\":\"{err}\"}}"),
    }
}

fn service_urls() -> Result<BTreeMap<String, String>> {
    let mut map = BTreeMap::new();
    map.insert(
        "gateway_binance".to_string(),
        read_var("GATEWAY_BINANCE_URL", Some("http://localhost:7001"))?,
    );
    map.insert(
        "gateway_polymarket".to_string(),
        read_var("GATEWAY_POLYMARKET_URL", Some("http://localhost:7002"))?,
    );
    map.insert(
        "gateway_kalshi".to_string(),
        read_var("GATEWAY_KALSHI_URL", Some("http://localhost:7003"))?,
    );
    map.insert(
        "gateway_deribit".to_string(),
        read_var("GATEWAY_DERIBIT_URL", Some("http://localhost:7004"))?,
    );
    map.insert(
        "recorder".to_string(),
        read_var("RECORDER_SERVICE_URL", Some("http://localhost:7010"))?,
    );
    map.insert(
        "normalizer".to_string(),
        read_var("NORMALIZER_SERVICE_URL", Some("http://localhost:7011"))?,
    );
    map.insert(
        "metadata".to_string(),
        read_var("METADATA_SERVICE_URL", Some("http://localhost:7012"))?,
    );
    map.insert(
        "fair_value_engine".to_string(),
        read_var("FAIR_VALUE_ENGINE_URL", Some("http://localhost:7013"))?,
    );
    map.insert(
        "signal_engine".to_string(),
        read_var("SIGNAL_ENGINE_URL", Some("http://localhost:7014"))?,
    );
    map.insert(
        "paper_execution".to_string(),
        read_var("PAPER_EXECUTION_URL", Some("http://localhost:7015"))?,
    );
    map.insert(
        "analytics_engine".to_string(),
        read_var("ANALYTICS_ENGINE_URL", Some("http://localhost:7016"))?,
    );
    Ok(map)
}

fn stream_urls() -> Result<BTreeMap<String, String>> {
    let mut map = BTreeMap::new();
    map.insert(
        "binance".to_string(),
        read_var("GATEWAY_BINANCE_URL", Some("http://localhost:7001"))?,
    );
    map.insert(
        "polymarket".to_string(),
        read_var("GATEWAY_POLYMARKET_URL", Some("http://localhost:7002"))?,
    );
    map.insert(
        "kalshi".to_string(),
        read_var("GATEWAY_KALSHI_URL", Some("http://localhost:7003"))?,
    );
    map.insert(
        "deribit".to_string(),
        read_var("GATEWAY_DERIBIT_URL", Some("http://localhost:7004"))?,
    );
    Ok(map)
}

async fn clear_cache_keyspace(keyspace: &str) -> Result<()> {
    let config = ServiceConfig::from_env("ops-cli", "PORT_METADATA")?;
    let client = redis::Client::open(config.redis.url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;
    let pattern = format!("{keyspace}*");
    let keys: Vec<String> = conn.keys(pattern).await.unwrap_or_default();
    for key in &keys {
        let _: i64 = conn.del(key).await?;
    }
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "cleared": keys.len(),
            "keyspace": keyspace
        }))?
    );
    Ok(())
}

async fn run_replay(from: &str, to: &str, underlying: &str, mode: &str, speed: &str) -> Result<()> {
    let status = Command::new("cargo")
        .args([
            "run",
            "-p",
            "replay-runner",
            "--",
            "--from",
            from,
            "--to",
            to,
            "--underlying",
            underlying,
            "--mode",
            mode,
            "--speed",
            speed,
        ])
        .status()
        .await?;
    println!(
        "{}",
        serde_json::to_string_pretty(&serde_json::json!({
            "command": "replay-runner",
            "status": status.code()
        }))?
    );
    Ok(())
}

async fn run_parameter_sweep(profile: &str) -> Result<()> {
    let base = format!("infrastructure/config-profiles/{profile}");
    let profile_path = format!("{base}.json");
    if !Path::new(&profile_path).exists() {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "error": "profile_not_found",
                "profile": profile_path
            }))?
        );
        return Ok(());
    }
    let raw = std::fs::read_to_string(&profile_path)?;
    let json: Value = serde_json::from_str(&raw)?;
    let runs = json
        .get("runs")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    for run in runs {
        let from = run.get("from").and_then(Value::as_str).unwrap_or("2026-01-01T00:00:00Z");
        let to = run.get("to").and_then(Value::as_str).unwrap_or("2026-01-01T01:00:00Z");
        let underlying = run.get("underlying").and_then(Value::as_str).unwrap_or("BTC");
        let mode = run.get("mode").and_then(Value::as_str).unwrap_or("paper");
        let speed = run.get("speed").and_then(Value::as_str).unwrap_or("max");
        run_replay(from, to, underlying, mode, speed).await?;
    }
    Ok(())
}
