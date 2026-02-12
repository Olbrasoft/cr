# Stage 1: Build
FROM rust:1.93-bookworm AS builder

WORKDIR /app

# Copy manifests first for dependency caching
COPY Cargo.toml Cargo.lock ./
COPY cr-domain/Cargo.toml cr-domain/Cargo.toml
COPY cr-app/Cargo.toml cr-app/Cargo.toml
COPY cr-infra/Cargo.toml cr-infra/Cargo.toml
COPY cr-web/Cargo.toml cr-web/Cargo.toml

# Create dummy source files to build dependencies
RUN mkdir -p cr-domain/src cr-app/src cr-infra/src cr-web/src && \
    echo "pub fn lib() {}" > cr-domain/src/lib.rs && \
    echo "pub fn lib() {}" > cr-app/src/lib.rs && \
    echo "pub fn lib() {}" > cr-infra/src/lib.rs && \
    echo "fn main() {}" > cr-web/src/main.rs && \
    mkdir -p cr-infra/src/bin && \
    echo "fn main() {}" > cr-infra/src/bin/import_csv.rs && \
    cargo build --release -p cr-web -p cr-infra 2>/dev/null || true

# Copy actual source code
COPY cr-domain/ cr-domain/
COPY cr-app/ cr-app/
COPY cr-infra/ cr-infra/
COPY cr-web/ cr-web/

# Touch source files to invalidate cache
RUN touch cr-domain/src/lib.rs cr-web/src/main.rs cr-infra/src/bin/import_csv.rs

# Build both binaries
ENV SQLX_OFFLINE=true
RUN cargo build --release -p cr-web && \
    cargo build --release -p cr-infra --bin import-csv

# Stage 2: Runtime
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binaries
COPY --from=builder /app/target/release/cr-web /app/cr-web
COPY --from=builder /app/target/release/import-csv /app/import-csv

# Copy static assets and data
COPY cr-web/static /app/static
COPY cr-web/templates /app/templates
COPY data/ /app/data/

ENV STATIC_DIR=/app/static
ENV RUST_LOG=info

EXPOSE 3000

CMD ["/app/cr-web"]
