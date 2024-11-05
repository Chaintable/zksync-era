FROM matterlabs/zksync-build-base:latest as builder

WORKDIR /usr/src/zksync
COPY . .

RUN cargo build --release --bin zksync_external_node

FROM matterlabs/external-node:v25.0.0-alpha

COPY --from=builder /usr/src/zksync/target/release/zksync_external_node /usr/bin
