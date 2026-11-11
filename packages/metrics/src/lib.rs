use axum::http::{header, HeaderMap, StatusCode};
use chrono::{DateTime, Utc};
use prometheus::{Encoder, IntCounterVec, IntGaugeVec, Opts, Registry, TextEncoder};
use serde::Serialize;

#[derive(Clone)]
pub struct AppMetrics {
    pub registry: Registry,
    pub reconnect_count: IntCounterVec,
    pub error_count: IntCounterVec,
    pub message_count: IntCounterVec,
    pub stream_latency_ms: IntGaugeVec,
}

impl AppMetrics {
    pub fn new(namespace: &str, service_name: &str) -> Self {
        let registry = Registry::new_custom(Some(namespace.to_owned()), None).expect("metrics registry");
        let reconnect_count =
            IntCounterVec::new(Opts::new("reconnect_total", "Reconnect attempts"), &["service", "venue"])
                .expect("reconnect counter");
        let error_count = IntCounterVec::new(Opts::new("error_total", "Errors"), &["service", "class"])
            .expect("error counter");
        let message_count = IntCounterVec::new(
            Opts::new("message_total", "Ingested messages"),
            &["service", "venue", "type"],
        )
        .expect("message counter");
        let stream_latency_ms = IntGaugeVec::new(
            Opts::new("stream_latency_ms", "Estimated stream latency in ms"),
            &["service", "venue"],
        )
        .expect("latency gauge");

        registry
            .register(Box::new(reconnect_count.clone()))
            .expect("register reconnect");
        registry
            .register(Box::new(error_count.clone()))
            .expect("register errors");
        registry
            .register(Box::new(message_count.clone()))
            .expect("register messages");
        registry
            .register(Box::new(stream_latency_ms.clone()))
            .expect("register latency");

        message_count
            .with_label_values(&[service_name, "internal", "startup"])
            .inc();

        Self {
            registry,
            reconnect_count,
            error_count,
            message_count,
            stream_latency_ms,
        }
    }

    pub fn render(&self) -> (StatusCode, HeaderMap, String) {
        let metric_families = self.registry.gather();
        let mut buf = Vec::<u8>::new();
        let encoder = TextEncoder::new();
        encoder.encode(&metric_families, &mut buf).expect("encode metrics");

        let mut headers = HeaderMap::new();
        headers.insert(header::CONTENT_TYPE, encoder.format_type().parse().expect("header parse"));
        (
            StatusCode::OK,
            headers,
            String::from_utf8(buf).unwrap_or_else(|_| String::new()),
        )
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct HealthPayload {
    pub service_name: String,
    pub instance_id: String,
    pub build_version: String,
    pub config_fingerprint: String,
    pub now_utc: DateTime<Utc>,
    pub status: String,
}
