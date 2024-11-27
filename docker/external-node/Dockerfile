FROM ghcr.io/matter-labs/zksync-runtime-base:latest as builder

WORKDIR /usr/src/zksync
COPY . .
RUN cargo build --release --bin zksync_external_node

FROM ghcr.io/matter-labs/zksync-runtime-base:latest

COPY --from=builder /usr/src/zksync/target/release/zksync_external_node /usr/bin
COPY --from=builder /usr/src/zksync/target/release/block_reverter /usr/bin
COPY --from=builder /usr/local/cargo/bin/sqlx /usr/bin
COPY --from=builder /usr/src/zksync/docker/external-node/entrypoint.sh /usr/bin
COPY contracts/system-contracts/zkout/ /contracts/system-contracts/zkout/
COPY contracts/l1-contracts/out/ /contracts/l1-contracts/out/
COPY contracts/l2-contracts/zkout/ /contracts/l2-contracts/zkout/
COPY etc/tokens/ /etc/tokens/
COPY etc/ERC20/ /etc/ERC20/
COPY etc/multivm_bootloaders/ /etc/multivm_bootloaders/
COPY core/lib/dal/migrations/ /migrations

RUN chmod +x /usr/bin/entrypoint.sh

ENTRYPOINT [ "sh", "/usr/bin/entrypoint.sh"]