FROM ghcr.io/matter-labs/zksync-build-base:latest AS builder

WORKDIR /usr/src/zksync
COPY . .
RUN cargo build --manifest-path ./core/Cargo.toml --release --bin zksync_external_node

FROM matterlabs/external-node:v29.5.0

COPY --from=builder /usr/src/zksync/core/target/release/zksync_external_node /usr/bin
