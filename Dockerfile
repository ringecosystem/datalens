# syntax=docker/dockerfile:1

FROM rust:1.95-bookworm AS builder
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN cargo build --locked --release --package datalens-cli

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system datalens \
    && useradd --system --gid datalens --home-dir /var/lib/datalens datalens \
    && mkdir -p /etc/datalens /var/lib/datalens \
    && chown -R datalens:datalens /etc/datalens /var/lib/datalens

COPY --from=builder /app/target/release/datalens /usr/local/bin/datalens

USER datalens
WORKDIR /var/lib/datalens
ENTRYPOINT ["datalens"]
CMD ["serve", "--config", "/etc/datalens/datalens.toml"]
