#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use anyhow::Context;
use clap::Parser;
use riffy::analysis::classify::EndpointClassifiers;
use riffy::analysis::engine::DiffEngine;
use riffy::analysis::suppress::SuppressRules;
use riffy::config::{CliOverrides, StorageBackend};
use riffy::endpoint::EndpointMatcher;
use riffy::http::router::{admin_router, create_router, AdminState, AppState};
use riffy::pipeline::consumer::Consumer;
use riffy::storage::{InMemorySampleStore, RedisSampleStore, SampleStore};
use riffy::upstream::UpstreamClient;
use riffy::{config, pipeline, telemetry};
use std::path::PathBuf;
use std::sync::Arc;

/// Riffy — reverse proxy with diffy-style statistical regression detection.
#[derive(Parser, Debug)]
#[command(name = "riffy", version, about)]
struct Cli {
    /// Path to a YAML config file. Overrides the default `config.yaml` lookup
    /// in the working directory.
    #[arg(short, long, value_name = "PATH")]
    config: Option<PathBuf>,
    /// Baseline upstream address (e.g. http://localhost:9100).
    #[arg(long, value_name = "ADDR")]
    baseline: Option<String>,
    /// Control upstream address.
    #[arg(long, value_name = "ADDR")]
    control: Option<String>,
    /// Candidate upstream address.
    #[arg(long, value_name = "ADDR")]
    candidate: Option<String>,
    /// Endpoint pattern to analyze; repeat for multiple (e.g.
    /// --endpoint /api/v1/users/:id). Replaces the configured endpoint list.
    #[arg(long = "endpoint", value_name = "PATTERN")]
    endpoints: Vec<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let cfg = config::load(&CliOverrides {
        config_path: cli.config,
        baseline: cli.baseline,
        control: cli.control,
        candidate: cli.candidate,
        endpoints: cli.endpoints,
    })?;
    let tracer_provider = telemetry::init_tracing(&cfg.logging, &cfg.jaeger)?;

    tracing::debug!("cfg loaded : {:?}", cfg);

    tracing::info!(service = riffy::SERVICE_NAME, "starting riffy");

    let upstream = UpstreamClient::new(
        cfg.upstream.baseline.clone(),
        cfg.upstream.control.clone(),
        cfg.upstream.candidate.clone(),
        cfg.upstream.timeout,
    );

    let (analysis_tx, analysis_rx) = pipeline::channel(cfg.pipeline.channel_capacity);

    let patterns: Vec<String> = cfg.endpoints.iter().map(|e| e.pattern.clone()).collect();
    let matcher = Arc::new(EndpointMatcher::new(&patterns));
    let engine = Arc::new(DiffEngine::new(
        SuppressRules::from_config(&cfg.endpoints).context("invalid suppress_paths")?,
        EndpointClassifiers::from_config(&cfg.endpoints),
    ));

    let store: Arc<dyn SampleStore> = match &cfg.storage.backend {
        StorageBackend::Redis { uri } => {
            let store = RedisSampleStore::connect(uri, cfg.storage.sample_cap, cfg.storage.window)
                .await
                .context("failed to connect to redis")?;
            Arc::new(store)
        }
        StorageBackend::InMemory => {
            tracing::info!("using in-memory sample store (no persistence)");
            Arc::new(InMemorySampleStore::with_retention(
                cfg.storage.sample_cap,
                cfg.storage.window,
            ))
        }
    };

    let consumer_handle = Consumer::new(
        analysis_rx,
        matcher.clone(),
        store.clone(),
        cfg.storage.max_body_bytes,
    )
    .spawn();

    let metrics_handle = if cfg.metrics.enabled {
        Some(telemetry::install_prometheus().context("failed to install prometheus recorder")?)
    } else {
        None
    };

    let cfg = Arc::new(cfg);
    let upstream = Arc::new(upstream);

    let state = AppState {
        config: cfg.clone(),
        upstream,
        analysis_tx,
        matcher,
    };

    let proxy_addr = format!("{}:{}", cfg.server.address, cfg.server.proxy_port);
    let proxy_app = create_router(state);
    let proxy_listener = tokio::net::TcpListener::bind(&proxy_addr).await?;
    tracing::info!(addr = %proxy_addr, "proxy server listening");

    let upstreams = Arc::new(riffy::http::query::UpstreamTargets::from_addresses(
        &cfg.upstream.baseline,
        &cfg.upstream.candidate,
        &cfg.upstream.control,
    ));
    let admin_app = admin_router(AdminState {
        metrics: metrics_handle,
        store,
        engine,
        upstreams,
    });
    let admin_addr = format!("{}:{}", cfg.server.address, cfg.server.admin_port);
    let admin_listener = tokio::net::TcpListener::bind(&admin_addr).await?;
    tracing::info!(addr = %admin_addr, "admin server listening");

    let shutdown = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to listen for ctrl+c");
        tracing::info!("shutdown signal received");
    };

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

    // AppState (and the analysis sender it holds) is dropped when select! returns,
    // closing the channel. The consumer drains it, flushes one final aggregation, then exits.
    match tokio::time::timeout(std::time::Duration::from_secs(5), consumer_handle).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::warn!(error = %e, "analysis consumer task failed"),
        Err(_) => tracing::warn!("analysis consumer did not stop within 5s"),
    }

    if let Some(provider) = tracer_provider {
        if let Err(e) = provider.shutdown() {
            tracing::warn!(error = %e, "failed to shut down tracer provider");
        }
    }

    Ok(())
}
