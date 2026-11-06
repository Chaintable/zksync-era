# Offline Lens rebuild tools; same builder as docker/external-node.
FROM ghcr.io/matter-labs/zksync-build-base:latest AS builder

WORKDIR /usr/src/zksync
COPY . .
RUN apt-get update && apt-get install -y protobuf-compiler cmake
RUN SQLX_OFFLINE=true cargo build --manifest-path ./core/Cargo.toml --release -p zksync_s3_backfill \
    --bin debank_execution_replay --bin debank_genesis_rebuild

FROM scratch
COPY --from=builder /usr/src/zksync/core/target/release/debank_execution_replay /usr/src/zksync/core/target/release/debank_genesis_rebuild /
