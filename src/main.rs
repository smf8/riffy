#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

use anyhow::Context;
use riffy::analysis::classify::EndpointClassifiers;
use riffy::analysis::counters::LiveCounters;
use riffy::config::StorageBackend;
use riffy::endpoint::EndpointMatcher;
use riffy::http::router::{admin_router, create_router, AdminState, AppState};
use riffy::pipeline::consumer::Consumer;
use riffy::storage::{DiffStore, InMemoryDiffStore, RedisDiffStore};
use riffy::upstream::UpstreamClient;
use riffy::{config, pipeline, telemetry};
use std::sync::Arc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load config
    let cfg = config::load()?;
    let tracer_provider = telemetry::init_tracing(&cfg.logging)?;

    tracing::info!(service = riffy::SERVICE_NAME, "starting riffy");

    // Upstream client
    let upstream = UpstreamClient::new(
        cfg.upstream.baseline.clone(),
        cfg.upstream.control.clone(),
        cfg.upstream.candidate.clone(),
        cfg.upstream.timeout,
    );

    // Analysis pipeline: bounded channel → single consumer task
    let (analysis_tx, analysis_rx) = pipeline::channel(cfg.pipeline.channel_capacity);

    let collector = Arc::new(LiveCounters::new());
    let patterns: Vec<String> = cfg.endpoints.iter().map(|e| e.pattern.clone()).collect();
    let matcher = Arc::new(EndpointMatcher::new(&patterns));
    let classifiers = Arc::new(EndpointClassifiers::from_config(&cfg.endpoints));

    // The store is shared between the consumer (writer) and the admin query API
    // (reader). Both backends share the aggregation interval and stream cap.
    let aggregation_interval = cfg.storage.aggregation_interval;
    let store: Arc<dyn DiffStore> = match &cfg.storage.backend {
        StorageBackend::Redis { uri } => {
            let store = RedisDiffStore::connect(uri, cfg.storage.stream_cap)
                .await
                .context("failed to connect to redis")?;
            Arc::new(store)
        }
        StorageBackend::InMemory => {
            tracing::info!("using in-memory diff store (no persistence)");
            Arc::new(InMemoryDiffStore::with_capacity(cfg.storage.stream_cap))
        }
    };

    let consumer_handle = Consumer::new(
        analysis_rx,
        matcher.clone(),
        collector.clone(),
        store.clone(),
        aggregation_interval,
    )
    .spawn();

    // Prometheus exporter (admin /metrics renders empty when disabled)
    let metrics_handle = if cfg.metrics.enabled {
        Some(
            telemetry::metrics::install_prometheus()
                .context("failed to install prometheus recorder")?,
        )
    } else {
        None
    };

    let cfg = Arc::new(cfg);
    let upstream = Arc::new(upstream);

    // AppState
    let state = AppState {
        config: cfg.clone(),
        upstream,
        analysis_tx,
        matcher,
    };

    // Proxy server
    let proxy_addr = format!("{}:{}", cfg.server.address, cfg.server.proxy_port);
    let proxy_app = create_router(state);
    let proxy_listener = tokio::net::TcpListener::bind(&proxy_addr).await?;
    tracing::info!(addr = %proxy_addr, "proxy server listening");

    // Admin server (healthz + metrics + diff query API). The query API applies
    // the per-endpoint classifiers at read time to derive regressions from the
    // stored raw counts.
    let admin_app = admin_router(AdminState {
        metrics: metrics_handle,
        store,
        classifiers,
        counters: collector,
    });
    let admin_addr = format!("{}:{}", cfg.server.address, cfg.server.admin_port);
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

    // The servers (and their AppState holding the analysis sender) are dropped
    // once select! returns; the consumer then drains the channel, flushes a
    // final aggregation snapshot, and exits.
    match tokio::time::timeout(std::time::Duration::from_secs(5), consumer_handle).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::warn!(error = %e, "analysis consumer task failed"),
        Err(_) => tracing::warn!("analysis consumer did not stop within 5s"),
    }

    // Flush any buffered spans to the OTLP collector before exiting.
    if let Some(provider) = tracer_provider {
        if let Err(e) = provider.shutdown() {
            tracing::warn!(error = %e, "failed to shut down tracer provider");
        }
    }

    Ok(())
}
