use crate::config::{Jaeger, Logging};
use anyhow::Context;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::runtime::Tokio;
use opentelemetry_sdk::trace::span_processor_with_async_runtime::BatchSpanProcessor;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use opentelemetry_sdk::Resource;

pub mod timer;

const LATENCY_BUCKETS: &[f64] = &[
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
];

pub fn install_prometheus() -> anyhow::Result<metrics_exporter_prometheus::PrometheusHandle> {
    // Set explicit buckets so histograms export as Prometheus histograms
    // (_bucket/_sum/_count) rather than the exporter's default summaries, whose
    // client-side quantiles cannot be aggregated across instances or recomputed
    // with histogram_quantile().
    Ok(metrics_exporter_prometheus::PrometheusBuilder::new()
        .set_buckets(LATENCY_BUCKETS)
        .context("setting prometheus histogram buckets")?
        .install_recorder()?)
}

pub fn init_tracing(
    logging: &Logging,
    jaeger: &Jaeger,
) -> anyhow::Result<Option<SdkTracerProvider>> {
    use std::str::FromStr;
    use tracing::Level;
    use tracing_subscriber::fmt::time::ChronoLocal;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;
    use tracing_subscriber::EnvFilter;

    let level = Level::from_str(&logging.level)
        .map_err(|_| anyhow::anyhow!("invalid log level '{}'", logging.level))?;

    let console_format = tracing_subscriber::fmt::format()
        .with_ansi(true)
        .with_file(true)
        .with_line_number(true)
        .with_timer(ChronoLocal::default());

    // Directive strings are hardcoded and always valid — the expects are unreachable.
    let env_filter = EnvFilter::default()
        .add_directive(level.into())
        .add_directive("hyper_util=info".parse().expect("valid directive"))
        .add_directive("h2=info".parse().expect("valid directive"))
        .add_directive("tower=info".parse().expect("valid directive"));

    let fmt_layer = tracing_subscriber::fmt::layer()
        .json()
        .event_format(console_format);

    let provider = if jaeger.enabled {
        Some(build_tracer_provider(
            &jaeger.endpoint,
            jaeger.sampling_rate,
        )?)
    } else {
        None
    };
    let otel_layer = provider
        .as_ref()
        .map(|p| tracing_opentelemetry::layer().with_tracer(p.tracer(crate::SERVICE_NAME)));

    tracing_subscriber::Registry::default()
        .with(env_filter)
        .with(fmt_layer)
        .with(otel_layer)
        .init();

    Ok(provider)
}

fn build_tracer_provider(endpoint: &str, sampling_rate: f64) -> anyhow::Result<SdkTracerProvider> {
    let exporter = SpanExporter::builder()
        .with_http()
        .with_endpoint(endpoint)
        .build()
        .context("failed to build OTLP span exporter")?;

    let sampler = Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(sampling_rate)));

    // Must use the Tokio-integrated batch processor: the sync fallback
    // (with_batch_exporter) uses futures_executor::block_on from a plain OS
    // thread, which deadlocks with reqwest because no Tokio reactor is present.
    let processor = BatchSpanProcessor::builder(exporter, Tokio).build();

    Ok(SdkTracerProvider::builder()
        .with_span_processor(processor)
        .with_sampler(sampler)
        .with_resource(
            Resource::builder()
                .with_service_name(crate::SERVICE_NAME)
                .build(),
        )
        .build())
}
