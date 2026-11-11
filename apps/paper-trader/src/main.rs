use anyhow::Result;
use clap::{Parser, Subcommand};
use config::read_var;
use reqwest::Client;
use serde_json::Value;

#[derive(Parser)]
#[command(name = "paper-trader")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Status,
    Positions,
    Decisions,
    Signals,
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = dotenvy::dotenv();
    let cli = Cli::parse();
    let paper_url = read_var("PAPER_EXECUTION_URL", Some("http://localhost:7015"))?;
    let signal_url = read_var("SIGNAL_ENGINE_URL", Some("http://localhost:7014"))?;
    let client = Client::new();

    match cli.command {
        Commands::Status => print_json(&client, &format!("{paper_url}/paper/status")).await,
        Commands::Positions => print_json(&client, &format!("{paper_url}/paper/positions")).await,
        Commands::Decisions => print_json(&client, &format!("{paper_url}/paper/decisions")).await,
        Commands::Signals => print_json(&client, &format!("{signal_url}/signals/tail")).await,
    }
    Ok(())
}

async fn print_json(client: &Client, url: &str) {
    match client.get(url).send().await {
        Ok(resp) => match resp.json::<Value>().await {
            Ok(json) => {
                println!("{}", serde_json::to_string_pretty(&json).unwrap_or_else(|_| "{}".to_string()));
            }
            Err(err) => println!("{{\"error\":\"{err}\"}}"),
        },
        Err(err) => println!("{{\"error\":\"{err}\"}}"),
    }
}
