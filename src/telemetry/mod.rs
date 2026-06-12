use crate::config::Logging;

pub mod metrics;

#[cfg(test)]
mod tests;

/// Initialize the global tracing subscriber: JSON events, local-time
/// timestamps, env-filterable levels with noisy HTTP internals capped at info.
pub fn init_tracing(logging: &Logging) {
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
