# Chaintable write node

> Fork of [matter-labs/zksync-era](https://github.com/matter-labs/zksync-era), with Chaintable pipeline patches.

## Architecture

This repo runs the chain's execution layer with the [Chaintable pipeline](https://github.com/Chaintable/pipeline) tracer embedded. The tracer extracts block data — block headers, transactions, call traces, receipts, events, and state diffs — and ships it to **S3 + Kafka** (see pipeline's [architecture](https://github.com/Chaintable/pipeline/blob/main/docs/architecture.md)). Two consumption paths:

- **Block headers + state diffs** → Kafka + S3 → [leafage-evm](https://github.com/Chaintable/leafage-evm): a lightweight EVM executor serving state queries (`eth_call`, `eth_estimateGas`, …), no P2P sync, no tx storage (see its [architecture](https://github.com/Chaintable/leafage-evm#architecture)).
- **Block files** (transactions · call traces · receipts · events) → S3 → Chaintable's transaction/trace indexing pipeline.

```
Chaintable write node (this repo · producer, embeds pipeline tracer)
        │
        ├─ block headers + state diffs ──────────────────→ Kafka + S3 ─→ leafage-evm (EVM state queries)
        │
        └─ block files (tx · trace · receipts · events) ──→ S3 ─→ Chaintable indexing pipeline (tx/trace data)
```

---

# ZKsync Era: A ZK Rollup For Scaling Ethereum

[![Logo](eraLogo.png)](https://zksync.io/)

ZKsync Era is a layer 2 rollup that uses zero-knowledge proofs to scale Ethereum without compromising on security or
decentralization. Since it's EVM compatible (Solidity/Vyper), 99% of Ethereum projects can redeploy without refactoring
or re-auditing a single line of code. ZKsync Era also uses an LLVM-based compiler that will eventually let developers
write smart contracts in C++, Rust and other popular languages.

## Documentation

The most recent documentation can be found here:

- [Core documentation](https://matter-labs.github.io/zksync-era/core/latest/)
- [Prover documentation](https://matter-labs.github.io/zksync-era/prover/latest/)

## Policies

- [Security policy](SECURITY.md)
- [Contribution policy](CONTRIBUTING.md)

## License

ZKsync Era is distributed under the terms of either

- Apache License, Version 2.0, ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <https://opensource.org/blog/license/mit/>)

at your option.

## Official Links

- [Website](https://zksync.io/)
- [GitHub](https://github.com/matter-labs)
- [ZK Credo](https://github.com/zksync/credo)
- [Twitter](https://twitter.com/zksync)
- [Twitter for Developers](https://twitter.com/zkSyncDevs)
- [Discord](https://join.zksync.dev/)
- [Mirror](https://zksync.mirror.xyz/)
- [Youtube](https://www.youtube.com/@zkSync-era)

## Disclaimer

ZKsync Era has been through lots of testing and audits. Although it is live, it is still in alpha state and will go
through more audits and bug bounty programs. We would love to hear our community's thoughts and suggestions about it! It
is important to state that forking it now can potentially lead to missing important security updates, critical features,
and performance improvements.
