# Will work locally only after prior contracts build

FROM matterlabs/zksync-build-base:latest as builder

WORKDIR /usr/src/zksync
COPY . .

RUN cargo build --release --bin zksync_external_node

FROM matterlabs/external-node:v22.1.0
COPY --from=builder /usr/src/zksync/target/release/zksync_external_node /usr/bin

