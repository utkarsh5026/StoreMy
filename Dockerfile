# syntax=docker/dockerfile:1

FROM rust:1.89-bookworm AS builder

RUN apt-get update \
    && apt-get install -y --no-install-recommends pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

COPY rust-toolchain.toml Cargo.toml Cargo.lock ./
COPY db ./db
COPY storemy-codec-derive ./storemy-codec-derive

RUN cargo build -p storemy --release --locked --bins

FROM debian:bookworm-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd -g 1000 storemy \
    && useradd -r -u 1000 -g storemy storemy

WORKDIR /app

COPY --from=builder /build/target/release/storemy /build/target/release/metrics_exporter ./

RUN mkdir -p /app/data \
    && chown -R storemy:storemy /app

USER storemy

VOLUME ["/app/data"]

ENV DATA_DIR=/app/data

ENTRYPOINT ["./storemy"]

CMD ["repl"]
