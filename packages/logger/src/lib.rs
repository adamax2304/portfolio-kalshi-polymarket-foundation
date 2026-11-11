use tracing_subscriber::{fmt, EnvFilter};

pub fn init(service_name: &str, level: &str) {
    let filter = EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = fmt()
        .json()
        .with_env_filter(filter)
        .with_current_span(true)
        .with_target(true)
        .with_ansi(false)
        .with_file(false)
        .with_line_number(false)
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_writer(std::io::stdout)
        .try_init();

    tracing::info!(service = service_name, "structured logger initialized");
}
