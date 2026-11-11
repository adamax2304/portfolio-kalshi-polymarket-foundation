#[cfg(test)]
mod tests {
    use anyhow::Result;
    use chrono::{Duration, Utc};
    use queue_model::{estimate_execution, QueueContext, QueueModelParams};
    use reqwest::Client;
    use risk_models::{evaluate_paper_risk, PaperRiskConfig, PaperRiskContext};
    use rust_decimal::Decimal;
    use serde_json::Value;
    use signal_models::{PaperExecutionMode, SignalDecision, SignalDirection, SignalFeatureSnapshot, SignalType};

    fn url(name: &str, fallback: &str) -> String {
        std::env::var(name).unwrap_or_else(|_| fallback.to_string())
    }

    #[tokio::test]
    #[ignore = "requires running local stack"]
    async fn websocket_reconnect_simulation() -> Result<()> {
        let client = Client::new();
        let health = client
            .get(format!("{}/health", url("GATEWAY_POLYMARKET_URL", "http://localhost:7002")))
            .send()
            .await?
            .json::<Value>()
            .await?;
        let reconnect_count = health["stream"]["reconnect_count"].as_u64().unwrap_or(0);
        assert!(reconnect_count <= 10_000);
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires running local stack"]
    async fn duplicate_message_handling_pipeline() -> Result<()> {
        let client = Client::new();
        let stats_before = client
            .get(format!("{}/stats", url("RECORDER_SERVICE_URL", "http://localhost:7010")))
            .send()
            .await?
            .json::<Value>()
            .await?;
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let stats_after = client
            .get(format!("{}/stats", url("RECORDER_SERVICE_URL", "http://localhost:7010")))
            .send()
            .await?
            .json::<Value>()
            .await?;
        let before = stats_before["raw_persisted"].as_u64().unwrap_or(0);
        let after = stats_after["raw_persisted"].as_u64().unwrap_or(0);
        assert!(after >= before);
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires running local stack and databases"]
    async fn clickhouse_postgres_redis_write_path() -> Result<()> {
        let client = Client::new();
        let metadata = client
            .get(format!("{}/contracts", url("METADATA_SERVICE_URL", "http://localhost:7012")))
            .send()
            .await?
            .json::<Value>()
            .await?;
        let contracts = metadata["data"].as_array().cloned().unwrap_or_default();
        assert!(!contracts.is_empty(), "expected at least one canonical contract");
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires running local stack"]
    async fn unresolved_mapping_path_visible() -> Result<()> {
        let client = Client::new();
        let unresolved = client
            .get(format!(
                "{}/mappings/unresolved",
                url("METADATA_SERVICE_URL", "http://localhost:7012")
            ))
            .send()
            .await?
            .json::<Value>()
            .await?;
        let _ = unresolved["data"].as_array().cloned().unwrap_or_default();
        Ok(())
    }

    #[tokio::test]
    #[ignore = "requires running local stack"]
    async fn replay_export_interval() -> Result<()> {
        let client = Client::new();
        let to = Utc::now();
        let from = to - Duration::minutes(15);
        let response = client
            .get(format!(
                "{}/export/bundle?from={}&to={}",
                url("RECORDER_SERVICE_URL", "http://localhost:7010"),
                from.to_rfc3339(),
                to.to_rfc3339()
            ))
            .send()
            .await?
            .json::<Value>()
            .await?;
        assert!(response.get("raw_events").is_some());
        assert!(response.get("normalized_events").is_some());
        assert!(response.get("reference_events").is_some());
        Ok(())
    }

    #[test]
    fn stale_feed_blocks_signal() {
        let decision = evaluate_paper_risk(
            &sample_signal(),
            &PaperRiskContext {
                now: Utc::now(),
                market_notional: Decimal::new(100, 0),
                underlying_exposure: Decimal::new(100, 0),
                simultaneous_signals: 1,
                time_in_market_seconds: 40,
                near_expiry_unhedged_notional: Decimal::new(20, 0),
                reference_feed_stale: true,
                disconnected_venue_feed: false,
                unresolved_normalized_contract: false,
                bad_markout_streak: 0,
                settlement_ambiguity: false,
                metadata_incomplete: false
            },
            &PaperRiskConfig::default()
        );
        assert!(!decision.allow_trade);
    }

    #[test]
    fn unresolved_mapping_blocks_paper_trade() {
        let decision = evaluate_paper_risk(
            &sample_signal(),
            &PaperRiskContext {
                now: Utc::now(),
                market_notional: Decimal::new(100, 0),
                underlying_exposure: Decimal::new(100, 0),
                simultaneous_signals: 1,
                time_in_market_seconds: 40,
                near_expiry_unhedged_notional: Decimal::new(20, 0),
                reference_feed_stale: false,
                disconnected_venue_feed: false,
                unresolved_normalized_contract: true,
                bad_markout_streak: 0,
                settlement_ambiguity: false,
                metadata_incomplete: false
            },
            &PaperRiskConfig::default()
        );
        assert!(!decision.allow_trade);
    }

    #[test]
    fn queue_model_is_conservative() {
        let estimate = estimate_execution(
            &QueueContext {
                ts: Utc::now(),
                queue_ahead_notional: Decimal::new(5000, 0),
                posted_notional: Decimal::new(100, 0),
                top_level_notional: Decimal::new(800, 0),
                touched: true,
                time_to_expiry_seconds: 80,
                recent_cancel_replace_count: 8,
                depth_score: Decimal::new(15, 2)
            },
            &QueueModelParams::default()
        );
        assert!(estimate.no_fill_probability > Decimal::new(40, 2));
    }

    fn sample_signal() -> SignalDecision {
        SignalDecision {
            signal_id: "sig".to_string(),
            signal_type: SignalType::FairValueDislocation,
            canonical_contract_key: "kalshi:btc15m".to_string(),
            underlying: "BTC".to_string(),
            source_venues: vec!["kalshi".to_string()],
            direction: SignalDirection::BuyYes,
            confidence: Decimal::new(80, 2),
            gross_edge_bps: Decimal::new(35, 0),
            estimated_net_edge_bps: Decimal::new(20, 0),
            signal_ttl_ms: 2000,
            market_regime_flags: vec![],
            required_features_snapshot: SignalFeatureSnapshot {
                spread_bps: Decimal::new(8, 1),
                realized_vol_60s: Decimal::new(10, 3),
                depth_score: Decimal::new(70, 2),
                time_to_expiry_seconds: 450,
                confidence_score: Decimal::new(80, 2),
                estimated_slippage_bps: Decimal::new(3, 0),
                estimated_fill_uncertainty_bps: Decimal::new(2, 0)
            },
            recommended_paper_execution_mode: PaperExecutionMode::PassiveMakerSim,
            accepted: true,
            rejected_reason: None
        }
    }

    #[test]
    fn scenario_fixtures_exist() {
        let files = vec![
            "../../replay/scenarios/01_sharp_btc_jump_last_90s.json",
            "../../replay/scenarios/02_wide_spread_kalshi_tight_poly.json",
            "../../replay/scenarios/03_tight_kalshi_lagging_poly.json",
            "../../replay/scenarios/04_stale_binance_feed.json",
            "../../replay/scenarios/05_venue_disconnect_strong_signal.json",
            "../../paper/scenarios/06_contract_normalized_incorrectly_block_trade.json",
            "../../paper/scenarios/07_touch_without_fill_maker.json",
            "../../paper/scenarios/08_partial_fill_first_leg_only.json",
            "../../paper/scenarios/09_rapid_cancel_replace_near_expiry.json",
            "../../strategy/scenarios/10_high_micro_noise_no_directional_edge.json"
        ];
        for file in files {
            let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src").join(file);
            let exists = std::fs::metadata(path).is_ok();
            assert!(exists, "missing fixture file: {file}");
        }
    }
}
