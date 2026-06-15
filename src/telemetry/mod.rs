use crate::config::Logging;
use anyhow::Context;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_otlp::{SpanExporter, WithExportConfig};
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;

pub mod metrics;

#[cfg(test)]
mod tests;

/// Initialize the global tracing subscriber: JSON events, local-time
/// timestamps, env-filterable levels with noisy HTTP internals capped at info.
/// When `logging.otlp.enabled`, spans are also exported to a Jaeger collector
/// over OTLP/HTTP; the returned provider must be kept alive and `shutdown()`
/// on exit to flush buffered spans.
pub fn init_tracing(logging: &Logging) -> anyhow::Result<Option<SdkTracerProvider>> {
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

    // The directive strings below are hardcoded and always valid, so the
    // `expect`s are unreachable.
    let env_filter = EnvFilter::default()
        .add_directive(level.into())
        .add_directive("hyper_util=info".parse().expect("valid directive"))
        .add_directive("h2=info".parse().expect("valid directive"))
        .add_directive("tower=info".parse().expect("valid directive"));

    let fmt_layer = tracing_subscriber::fmt::layer()
        .json()
        .event_format(console_format);

    // OTLP export is opt-in. When enabled, build a batch-exporting tracer
    // provider and bridge it into the subscriber via a tracing-opentelemetry
    // layer; `Option<Layer>` is a no-op layer when `None`.
    let provider = if logging.otlp.enabled {
        Some(build_tracer_provider(&logging.otlp.endpoint)?)
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

/// Build a batch-exporting OTLP/HTTP tracer provider pointed at `endpoint`
/// (the collector's OTLP receiver; the exporter appends `/v1/traces`).
fn build_tracer_provider(endpoint: &str) -> anyhow::Result<SdkTracerProvider> {
    let exporter = SpanExporter::builder()
        .with_http()
        .with_endpoint(endpoint)
        .build()
        .context("failed to build OTLP span exporter")?;

    Ok(SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(
            Resource::builder()
                .with_service_name(crate::SERVICE_NAME)
                .build(),
        )
        .build())
}
