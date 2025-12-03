FROM matterlabs/zksync-build-base:latest as builder

WORKDIR /usr/src/zksync
COPY . .
RUN cargo build --manifest-path ./core/Cargo.toml --release --bin zksync_external_node

FROM matterlabs/external-node:v29.6.0-alpha

COPY --from=builder /usr/src/zksync/core/target/release/zksync_external_node /usr/bin
