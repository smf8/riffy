#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

mod analysis;
mod compare;
mod config;
mod endpoint;
mod error;
mod pipeline;
mod proxy;
mod redis;
mod telemetry;

use proxy::router::{create_router, AppState};
use proxy::UpstreamClient;
use std::sync::Arc;
use tokio::sync::mpsc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load config
    let cfg = config::load()?;
    init_tracing(&cfg.logging);

    tracing::info!(service = %cfg.service_name, "starting riffy");

    // Upstream client
    let upstream = UpstreamClient::new(
        cfg.upstream.primary.clone(),
        cfg.upstream.secondary.clone(),
        cfg.upstream.candidate.clone(),
        cfg.upstream.protocol.clone(),
        cfg.upstream.timeout,
    );

    // Analysis channel (bounded)
    let (analysis_tx, mut analysis_rx) = mpsc::channel::<proxy::AnalysisMessage>(1024);

    // Spawn analysis consumer (placeholder — phase 4 will implement)
    let consumer_handle = tokio::spawn(async move {
        while let Some(msg) = analysis_rx.recv().await {
            tracing::debug!(
                endpoint = %msg.endpoint,
                method = %msg.method,
                "received analysis message (consumer not yet implemented)"
            );
        }
    });

    let cfg = Arc::new(cfg);
    let upstream = Arc::new(upstream);

    // AppState
    let state = AppState {
        config: cfg.clone(),
        upstream: upstream.clone(),
        analysis_tx,
    };

    // Proxy server
    let proxy_addr = format!("0.0.0.0:{}", cfg.proxy.port);
    let proxy_app = create_router(state);
    let proxy_listener = tokio::net::TcpListener::bind(&proxy_addr).await?;
    tracing::info!(addr = %proxy_addr, "proxy server listening");

    // Admin server (healthz + metrics)
    let admin_port = cfg.server.port;
    let admin_app = axum::Router::new()
        .route("/healthz", axum::routing::get(healthz))
        .route("/metrics", axum::routing::get(metrics_handler));
    let admin_addr = format!("{}:{}", cfg.server.address, admin_port);
    let admin_listener = tokio::net::TcpListener::bind(&admin_addr).await?;
    tracing::info!(addr = %admin_addr, "admin server listening");

    // Graceful shutdown
    let shutdown = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for ctrl+c");
        tracing::info!("shutdown signal received");
    };

    // Run both servers concurrently
    let proxy_server = axum::serve(proxy_listener, proxy_app);
    let admin_server = axum::serve(admin_listener, admin_app);

    tokio::select! {
        r = proxy_server => {
            tracing::info!("proxy server stopped");
            r?;
        }
        r = admin_server => {
            tracing::info!("admin server stopped");
            r?;
        }
        _ = shutdown => {
            tracing::info!("shutting down");
        }
    }

    consumer_handle.abort();
    Ok(())
}

async fn healthz() -> axum::http::StatusCode {
    axum::http::StatusCode::NO_CONTENT
}

async fn metrics_handler() -> String {
    // Placeholder — phase 5 will wire Prometheus exporter
    "# riffy metrics placeholder\n".to_string()
}

fn init_tracing(logging: &config::Logging) {
    use std::str::FromStr;
    use tracing::Level;
    use tracing_subscriber::fmt::time::ChronoLocal;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::EnvFilter;

    let level = Level::from_str(&logging.level).expect("invalid log level");

    let console_format = tracing_subscriber::fmt::format()
        .with_ansi(true)
        .with_file(true)
        .with_line_number(true)
        .with_timer(ChronoLocal::default());

    let subscriber = tracing_subscriber::Registry::default().with(
        EnvFilter::default()
            .add_directive(level.into())
            .add_directive("hyper_util=info".parse().expect("invalid directive"))
            .add_directive("h2=info".parse().expect("invalid directive"))
            .add_directive("tower=info".parse().expect("invalid directive")),
    );

    let subscriber = subscriber.with(
        tracing_subscriber::fmt::layer()
            .json()
            .event_format(console_format),
    );

    subscriber.init();
}
