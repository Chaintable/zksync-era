FROM ghcr.io/matter-labs/zksync-build-base:latest AS builder

WORKDIR /usr/src/zksync
COPY . .
RUN cargo build --release --bin zksync_external_node

FROM matterlabs/external-node:v25.4.0-alpha

COPY --from=builder /usr/src/zksync/target/release/zksync_external_node /usr/bin
