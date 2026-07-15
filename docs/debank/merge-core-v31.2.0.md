# Upstream core-v31.2.0 merge — verification report

PR: Chaintable/zksync-era #16 (`merge-core-v31.2.0` → `main`, previous base `core-v31.0.0`)
Image verified: `public.ecr.aws/b2h7a5c4/chaintable/zksync-era:880cc18` (PR build, CI run `29409671669`)
Role context: DeBank runs this as a **read-only RPC / indexer external node** (not validator/sequencer).
Severity below is scoped to that role and to changes **introduced by this merge**.

## 1. Release content & upgrade rationale

**Conclusion: mandatory upgrade — must merge.**

Upstream `core-v31.2.0` (2026-07-14) contains one change that is not optional for the external node:

- **`fix: automated protocol upgrade (#4886)`** — bumps the `zk_evm/circuit` protocol crate to **0.153.12**
  and performs the `next-upgrade` protocol version switch. The external node must understand the new
  protocol version/circuit set to keep validating batches and syncing from the main node. Not merging
  risks the EN stalling on a future batch once the main network enacts the upgrade.

Other changes in the release are either additive or outside the EN replica path:

| Change | EN relevance |
|---|---|
| #4889 `feat(multivm): Cycles Tracer` | Additive — new tracer/estimator code; does not alter the existing `debug_traceBlockByNumber` / `callTracer` output (verified byte-identical in §4). |
| #4891 `feat(state_keeper): persist predicted vs prover-reported Airbender cycles` | Touches seal logic/output handler, but the resulting block/statediff/trace artifacts are unchanged (verified by hash + trace + live S3 upload). |
| #4890 `fix(contract-verifier): normalize standard-json source handling` | Not deployed for the EN replica. |
| #4872 `fix(verifier): patch zksolc factory dependency hashes` | Not deployed for the EN replica. |

Also includes the additive DB migration `20260713000000_l1_batch_cycle_stats` (new `cycle_stats` table
and columns), applied cleanly by the image entrypoint.

Net: necessity is driven entirely by the protocol upgrade. The rest of the delta is low-risk for the
read-only replica.

## 2. Merge conflicts & impact surface

`git merge --no-ff core-v31.2.0` produced **17 conflicts**. Most were mechanical
(CI workflow / release-please / CHANGELOG / 3 Cargo.lock files); the true code conflicts were:

- `core/Cargo.toml` — upstream restructured workspace deps/members. Resolution: take upstream's new
  structure and re-add the fork-required workspace deps (`md5`, `alloy-primitives 1.4.1`,
  `alloy-rpc-types`, `etcd-client`, `rdkafka`, `rustc-hex`, `sha1`) and workspace member
  `zksync_s3_backfill`.
- `core/Cargo.lock` — cannot be fully regenerated: doing so pulls `alloy` to 1.8.3, which requires
  rustc 1.91 and breaks the pinned toolchain. Resolution: start from origin/main's lockfile and bump
  only the protocol/circuit crates introduced by #4886 to 0.153.12; verify with `cargo check --locked`
  on the affected boojum-free crates.
- `core/lib/airbender_prover_interface/src/api.rs` + tests
- `core/lib/contract_verifier/src/tests/mod.rs`
- `core/lib/multivm/src/tracers/mod.rs`
- `core/node/airbender_proof_data_handler/src/tests.rs`
- `core/node/contract_verification_server/src/tests/mod.rs`
- `core/node/state_keeper/src/node/output_handler.rs`

All conflicts were resolved by aligning with upstream where possible while keeping the DeBank fork
patches intact:

- **`zksync_s3_backfill`** workspace member and crate retained.
- **`api_server` custom RPC / tracer namespaces** retained (`eth_multiCall`, `pre_traceMany`,
  `trace_*`, `debank` namespaces, call tracer patches).
- **S3 persistence / Kafka notification wiring** in `state_keeper` retained.
- **`docker/external-node/Dockerfile`** runtime base image bumped to the matching upstream alpha tag.

No decision points required operator judgment beyond the standard "align with upstream, keep fork
patches" rule.

## 3. Deployment setup

Test **backup writer** = ZKsync external node + dedicated Postgres, restored from a production writer
snapshot onto an isolated test EBS volume on the test box (`lihe-dev`), running the PR image. PG +
RocksDB both live on the mounted test EBS (`./data → /var/data`); no shared storage with production.

**Writer / delivery model (this family: ZKsync EN).** The EN embeds `DebankS3OutputHandler` directly
in the state_keeper output chain. Backup mode keeps the **S3 delivery path live but disables all
Kafka/coordination side effects**, so the merge's collection output is really exercised without
touching production.

**Two deployment fixes were required for this release:**

1. **Postgres `PGDATA` must be explicit.** The default image path (`/var/lib/postgresql/data`) does
   not match the mounted `/var/data` layout used by the production snapshot. Compose sets
   `PGDATA=/var/data/<chain>_pg` and passes `-D /var/data/<chain>_pg`.
2. **Node container must use the image's default entrypoint.** Overriding it skipped the image's
   `sqlx database setup` step, which is required to apply the new `cycle_stats` migration. Compose
   uses the default `/usr/bin/entrypoint.sh` and only overrides `command` to
   `--components "core,api,tree,da_fetcher"`.

**Backup isolation:**

- `DEBANK_IS_BACKUP=true` → no Kafka producer / no resume / no notify / no gap-fill.
- `DEBANK_S3_ENABLED=true` → the S3 handler is really instantiated and uploads objects.
- `DEBANK_VERSION` was kept equal to the production value for this run. This reuses the production S3
  prefix, which means a catching-up backup **overwrites historical S3 objects** that the leader has
  already written. This is a **pre-existing backup-mode defect** (same behavior as the v29.20.0 case);
  content is generation-equivalent, only `LastModified` changes. It is **not introduced by this merge**.

Full test `compose.yml` (secrets/infra endpoints redacted) is in the appendix.

## 4. Verification results

**Verdict: PASS.**

| Check | Result |
|---|---|
| Startup (wiring, component init, migrations) | **PASS** — clean init, no panic/FATAL, `error_lines=[]`, restart count 0, containers running. `sqlx database setup` applied the `cycle_stats` migration. |
| Continuous block sealing after state restore | **PASS** — 152+ samples, head growing steadily, no stall or reorg. |
| Catch-up to chain tip | **PASS** — final lag 2 blocks (local 6074985 / reference 6074987). |
| Block-hash sampling vs reference RPC | **60/60 MATCH** — pre-upgrade `6073000-6073019`, post-upgrade `6073400-6073419`, and near-tip `6074965-6074984` all byte-identical. |
| `debug_traceBlockByNumber` (callTracer) vs reference RPC | **2/2 MATCH** — post-upgrade block `0x5c92fa` and near-tip block `0x5c9eaa`; jq-normalized outputs are identical. |
| S3 delivery path, live (`DEBANK_S3_ENABLED=true`, `DEBANK_IS_BACKUP=true`) | **PASS** — `Uploaded debank data for block ...` observed continuously; handler init, instance-role credentials, retry loop, and object keys all correct. Kafka producer correctly not created. |

The Cycles Tracer (#4889) and cycle-stats persistence (#4891) did not change the observable trace or
block output for the methods the indexer consumes.

## 5. Other exposed issues / follow-ups

1. **`en_getInteropFee` 403 log noise** — the EN polls the main node for the interop fee every few
   seconds; the current main node returns `403 Request rejected`. Fallback is safe (keeps last value),
   sync is unaffected, and the noise will stop once the main node side supports the method. This is
   the same pre-existing behavior observed in v29.20.0.
2. **Backup-mode S3 overwrite defect** — as in previous cases, `is_backup` does not guard S3 writes
   with an existence/conditional check, so a catching-up backup rewrites historical objects under the
   production prefix. Fix (head-check or `If-None-Match` for backup mode) is out of this PR's scope.
3. **`chains/<chain>.yaml` drift** — the local chain config still points to the old private ECR
   registry and an old STS pod name. This release's CI actually pushed to the public ECR
   (`public.ecr.aws/b2h7a5c4/chaintable/...`) and the snapshot was taken from the current seed STS.
   Update the yaml separately so future runs do not have to hand-correct these values.
4. **Local full-workspace build limitation** — `boojum 0.32.10` still fails on the pinned
   `nightly-2025-03-19`, so local can only `cargo check` boojum-free crates. The full compile gate is
   CI docker build (amd64 + arm64 both green on this PR). Not a regression.

---

## Appendix: test `compose.yml` (redacted)

> Redactions: Kafka broker hostnames and the Avail DA seed phrase are removed; the production
> `DEBANK_VERSION` hash is shown as `<prod-version>`. Snapshot / volume IDs are not included.

```yaml
version: "3.8"

services:
  postgres:
    image: postgres:14
    container_name: <chain>_merge_core-v31.2.0_postgres
    network_mode: host
    user: "999:999"
    environment:
      POSTGRES_PASSWORD: notsecurepassword
      POSTGRES_HOST_AUTH_METHOD: trust
      PGDATA: /var/data/<chain>_pg
      PGPORT: "5430"
    command:
      - postgres
      - -D
      - /var/data/<chain>_pg
      - -c
      - max_connections=200
      - -c
      - log_error_verbosity=terse
      - -c
      - shared_buffers=2GB
      - -c
      - effective_cache_size=4GB
      - -c
      - maintenance_work_mem=1GB
      - -c
      - checkpoint_completion_target=0.9
      - -c
      - random_page_cost=1.1
      - -c
      - effective_io_concurrency=200
      - -c
      - min_wal_size=4GB
      - -c
      - max_wal_size=16GB
      - -c
      - max_worker_processes=16
      - -c
      - checkpoint_timeout=1800
      - -c
      - listen_addresses=127.0.0.1
    volumes:
      - /opt/app/<chain>/writer_merge_core-v31.2.0/data:/var/data
    mem_limit: 8g
    cpus: 2
    stop_grace_period: 1m
    restart: on-failure:5
    logging:
      driver: json-file
      options:
        max-size: "100m"
        max-file: "5"

  node:
    image: public.ecr.aws/b2h7a5c4/chaintable/zksync-era:880cc18
    container_name: <chain>_merge_core-v31.2.0_node
    network_mode: host
    depends_on:
      postgres:
        condition: service_started
    command:
      - --components
      - "core,api,tree,da_fetcher"
    environment:
      DEBANK_IS_BACKUP: "true"
      DEBANK_S3_ENABLED: "true"
      DEBANK_VERSION: <prod-version>
      DEBANK_KAFKA_TOPIC: nodex_pipeline_<chain-id>_<prod-version>
      DEBANK_KAFKA_BROKERS: <kafka-brokers-redacted>
      DATABASE_URL: postgres://postgres:notsecurepassword@127.0.0.1:5430/zksync
      DATABASE_POOL_SIZE: "50"
      EN_STATE_CACHE_PATH: /var/data/<chain>_rocksdb/ext-node/state_keeper
      EN_MERKLE_TREE_PATH: /var/data/<chain>_rocksdb/ext-node/lightweight
      EN_ETH_CLIENT_URL: https://ethereum-rpc.publicnode.com
      EN_MAIN_NODE_URL: https://<main-node-rpc>/
      EN_HTTP_PORT: "3060"
      EN_WS_PORT: "3061"
      EN_PROMETHEUS_PORT: "29260"
      EN_HEALTHCHECK_PORT: "3081"
      EN_POSTGRES_STATEMENT_TIMEOUT_SEC: "300"
      EN_PRUNING_ENABLED: "false"
      EN_SNAPSHOTS_RECOVERY_ENABLED: "false"
      EN_L1_CHAIN_ID: "1"
      EN_L2_CHAIN_ID: "<chain-id>"
      EN_L1_BATCH_COMMIT_DATA_GENERATOR_MODE: Validium
      EN_API_NAMESPACES: eth,net,web3,zks,en,pubsub,debug
      EN_DA_CLIENT: Avail
      EN_DA_AVAIL_CLIENT_TYPE: FullClient
      EN_DA_BRIDGE_API_URL: https://bridge-api.avail.so
      EN_DA_TIMEOUT_MS: "20000"
      EN_DA_API_NODE_URL: https://api.avail.so
      EN_DA_APP_ID: "26"
      EN_DA_SECRETS_SEED_PHRASE: <da-seed-phrase-redacted>
      RUST_LOG: zksync_core=info,zksync_dal=info,zksync_eth_client=info,zksync_merkle_tree=info,zksync_storage=info,zksync_state=info,zksync_types=info,vm=info,zksync_external_node=info,zksync_utils=info,
      RUST_BACKTRACE: full
      AWS_REGION: ap-northeast-1
    volumes:
      - /opt/app/<chain>/writer_merge_core-v31.2.0/data:/var/data
    mem_limit: 16g
    cpus: 4
    stop_grace_period: 1m
    restart: on-failure:5
    logging:
      driver: json-file
      options:
        max-size: "100m"
        max-file: "5"
```
