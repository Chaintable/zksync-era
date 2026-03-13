FROM ghcr.io/matter-labs/zksync-build-base:latest AS builder

WORKDIR /usr/src/zksync
COPY . .
RUN apt-get update && apt-get install -y protobuf-compiler
RUN cargo build --manifest-path ./core/Cargo.toml --release --bin zksync_external_node --bin zksync_s3_backfill

FROM matterlabs/external-node:v29.4.1

RUN apt-get update && apt-get install -y --no-install-recommends postgresql-client && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/zksync/core/target/release/zksync_external_node /usr/bin
COPY --from=builder /usr/src/zksync/core/target/release/zksync_s3_backfill /usr/bin
