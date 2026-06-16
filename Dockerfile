# syntax=docker/dockerfile:1

# ---- Build stage -----------------------------------------------------------
# cargo-chef caches the dependency-compilation layer: rebuilds with unchanged
# Cargo.toml/Cargo.lock skip straight to compiling riffy's own sources.
FROM rust:1.94-slim-bookworm AS chef
RUN cargo install cargo-chef --locked
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --locked --recipe-path recipe.json
COPY . .
# Release profile (Cargo.toml) handles LTO + symbol stripping.
RUN cargo build --release --locked --bin riffy

# ---- Runtime stage ---------------------------------------------------------
# distroless/cc: no shell, no package manager, minimal attack surface; ships
# only glibc + libgcc, which the binary needs (jemalloc, zstd). TLS is rustls,
# so no OpenSSL is required. The :nonroot tag runs as UID 65532.
FROM gcr.io/distroless/cc-debian12:nonroot

LABEL org.opencontainers.image.title="riffy" \
      org.opencontainers.image.description="Reverse proxy with diffy-style statistical regression detection" \
      org.opencontainers.image.source="https://github.com/smf8/riffy"

COPY --from=builder /app/target/release/riffy /usr/local/bin/riffy

# Config is read from the working directory (config.yaml) or RIFFY_* env
# vars — mount config.yaml into /app; no config is baked into the image.
#
# OTLP trace export to Jaeger is opt-in. To enable it in a container, point the
# exporter at the collector — e.g. in the mounted config.yaml set
#   logging.otlp: { enabled: true, endpoint: "http://jaeger:4318" }
# (Jaeger's OTLP/HTTP receiver). reqwest+rustls is built in; no extra packages.
WORKDIR /app

# Proxy port and admin (healthz + metrics + UI) port; both configurable via the
# server.proxy-port / server.admin-port config (defaults 7677 / 7678).
EXPOSE 7677 7678

USER nonroot
ENTRYPOINT ["/usr/local/bin/riffy"]
