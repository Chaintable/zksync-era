FROM matterlabs/zksync-build-base:latest as builder

WORKDIR /usr/src/zksync
COPY . .
RUN cargo build --manifest-path ./core/Cargo.toml --release --bin zksync_external_node --bin block_reverter

FROM matterlabs/external-node:v25.0.0-alpha

COPY --from=builder /usr/src/zksync/target/release/zksync_external_node /usr/bin
