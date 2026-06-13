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
      org.opencontainers.image.source="https://github.com/snapp/riffy"

COPY --from=builder /app/target/release/riffy /usr/local/bin/riffy

# Config is read from the working directory (config.yaml) or RIFFY_* env
# vars — mount config.yaml into /app; no config is baked into the image.
WORKDIR /app

# Proxy port and admin (healthz + metrics) port; both configurable via config.
EXPOSE 8080 8081

USER nonroot
ENTRYPOINT ["/usr/local/bin/riffy"]
