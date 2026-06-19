# Upstream core-v31.0.0 merge — verification report

PR: Chaintable/zksync-era #2 (`merge-core-v31.0.0` → `debank`, previous base `core-v29.20.0`)
Image verified: `chaintable/zksync-era:amd64-c4b3c95` (public ECR, PR build)
Role context: DeBank runs this as a **read-only RPC / indexer external node** (not validator/sequencer).
Severity below is scoped to that role and to changes **introduced by this merge** (pre-existing /
replica-inert items are called out as such and excluded from this merge's severity).

## 1. Release content & upgrade rationale

**Conclusion: worth merging if/when convenient; no urgency — effectively optional.** The
core-v31.0.0 release notes carry **no `mandatory` / `critical` / `ASAP` / hardfork wording and no
upgrade-priority table**, so by official stance this is a normal feature+bugfix release. The 29→31
version jump is a release-numbering artifact (no intermediate release tags between v29.20.0 and
v31.0.0; only ~7 PRs of actual delta), **not** a protocol-level breaking signal. No mandatory network
hardfork is required of this version; the external node follows the main node and does not
independently fall out of consensus.

Two independent axes — both apply:

- **Sensitive-path involvement: yes.** Upstream's changes land on files that also carry our patches
  (`api_server/web3/{eth.rs,mod.rs,metrics.rs}`, plus a new `Web3Error` variant in
  `web3_decl/src/error.rs`). Because the API surface is touched, *if* we merge we must regress the
  custom RPC / tracer paths (`eth_multiCall` / `pre_traceMany` / `trace_*`) and refund-related paths.
- **Upgrade necessity: low.** After filtering through the DeBank read-only-replica role, only two
  changes are actually relevant, and both are minor, additive API hardening:
  1. `eth_feeHistory` — caps `reward_percentiles` at ≤128 and validates each value is finite and
     within `[0,100]`, returning a new `InvalidFeeHistoryParams` error. A DoS-guard on a public RPC
     method (blocks `block_count × percentiles` matrix over-allocation). Real but small benefit —
     our replicas are mostly internal indexer services with limited exposure to untrusted callers.
  2. WS `max_connections` operator-precedence fix (`!is_http`) — corrects the connection cap for the
     WS server. Operational robustness; default config usually doesn't hit it.

Everything else is **`inert_for_replica`**: airbender (no prover here), `l2_da_commitment_scheme`
pre-upgrade config (sequencer/MainNode wiring, doesn't change collection output), `mempool_actor`
`enter_critical().await` (sequencer mempool; the external node uses `ExternalIO` and does not run the
mempool actor), contract-verifier solc / EraVM partial-match rejection (not deployed),
house_keeper / docker-compose / docs / zkstack_cli. **No mandatory hardfork, no changes to the
collection output (statediff / blockfile / header / trace / Kafka), no receipt or data-correctness
change.** The 62-file diffstat does not raise necessity — the bulk is lockfiles / airbender /
verifier / docker, all inert for the replica.

Net: necessity is low; a defensible alternative was to defer and only bump `metadata.version`. The
operator gated **go** (Step 1 gate, 2026-06-19), so the full merge+verify flow below was run.

## 2. Merge conflicts & impact surface

**Fully automatic clean merge (`--no-ff --no-commit`): zero text conflicts** — no unmerged paths, no
leftover conflict markers, no fork-feature-superseded-by-upstream case (so no deprecation alignment
needed). Forward analysis: collection hooks **unchanged** (`affects_collection=false`, severe=false).
There are **no decision points requiring operator judgment** — every upstream change is additive and
aligns with upstream by design.

By the merge principle, alignment with upstream needs no explanation; only the **retained fork
patches** (deviations from upstream) are justified here:

- **`zksync_s3_backfill`** — DeBank's S3 backfill / blockfile-repair tool crate, the only
  debank-added workspace member. Fully retained; upstream has no such crate, so there is no overlap.
- **`api_server` custom RPC / tracer** — `api_server/web3/{eth.rs,mod.rs,metrics.rs}`:
  `multi_call_impl` (`eth_multiCall`) and the other debank namespaces / tracer hooks
  (eth.rs +200 / mod.rs +34 / metrics.rs +67) are retained verbatim. These back the DeBank
  collection / leafage RPC service. They sit on the same files as upstream's additive changes but on
  **different lines** with no semantic crossover; merge probe is clean, `multi_call_impl` is present
  exactly once, and there are no duplicate free-function symbols (the `metrics.rs` `fmt`/`from`/`new`
  are distinct trait impls on different types — legal).
- **`docker/external-node/Dockerfile`** — DeBank build that compiles both `zksync_external_node` and
  `zksync_s3_backfill`. Retained; only the runtime base image is bumped
  `v29.20.0-alpha → v31.0.0-alpha` (tag confirmed present on Docker Hub, 2026-06-18).

One mechanical follow-up (lockfile-class, **not** a marker conflict): in `core/Cargo.lock`, git kept
`zksync_s3_backfill` at the stale `29.20.0` (upstream lacks the crate → git took ours), but the
workspace bumped to 31.0.0 and the crate is `version.workspace=true`. We adopted cargo's regenerate
result `31.0.0-non-semver-compat` (a single line), re-verified consistent under `cargo check
--locked`. Same-source recurrence as the previous merge; `prover` / `zkstack_cli` carry no debank
crate, so their lock changes are pure upstream and were not cargo-dirtied.

Alignment (one line): upstream's additive changes — `eth_feeHistory` percentile cap+range check, WS
`max_connections` precedence fix, the new `web3_decl` `Web3Error` variant — were taken as-is.

**Reverse risk (downstream → fork): one finding, pre-existing, not introduced by this merge.**
`eth.rs` `call_once_inner` slices `data[4..]` on the `0xeeee…eeee` synthetic-token `balanceOf`
branch; calldata to that target that decodes to <4 bytes and doesn't start with the expected
selectors panics. Confirmed pre-existing by byte-diff against the `core-v29.20.0` base
(`multi_call_impl` / `call_once_inner` region is byte-identical across the merge; the only upstream
delta in this file is the two feeHistory additions). Severity **low**: it lives only on DeBank's own
`multi_call` custom RPC path whose input is constructed by the DeBank pipeline itself; worst case is
a single-request handler panic with no state/sync pollution. Tracked as a hardening TODO (§5), **not
introduced or newly-triggered by this merge, does not block it.**

Build gate:
- Local: boojum-free / affected crates `zksync_web3_decl` (intersection), `zksync_types`,
  `zksync_config` `cargo check --locked` all pass (versions all 31.0.0).
- CI: boojum-dep crates (incl. intersection `zksync_node_api_server`, `state_keeper`,
  `zksync_server`) get the full-workspace compile gate from the release workflow (amd64 + arm64).
  Local boojum-dep failure is the **pre-existing `boojum 0.32.10`-on-pinned-nightly baseline**
  (base↔merged byte-identical; 133 errors unrelated to this merge) — not counted as a build failure.

## 3. Deployment setup

Test **backup writer** = ZKsync external node + a dedicated Postgres, restored from a production <chain>
writer snapshot onto an isolated test volume on the test box (`lihe-dev`), running the PR image. PG +
RocksDB both live on the mounted test EBS (`./data → /var/data`); no shared storage with production.

**Writer / delivery model (this family: ZKsync EN, no etcd, no p2p).** Unlike the etl-sidecar model,
the ZKsync EN embeds `DebankS3OutputHandler` directly in the state_keeper output chain. So:
- Production line: EN → `DebankS3OutputHandler` produces S3 artifacts + Kafka notifications.
- Backup mode here keeps the **S3 delivery path live but disables all Kafka/coordination side
  effects**, so the merge's collection output is really exercised without touching production.

**Backup isolation — two explicit guards (mechanism: `debank-is-backup-env`):**
- `DEBANK_IS_BACKUP=true` (read at `external_node/src/node_builder.rs`) → in
  `io/debank_s3_persistence.rs`: no Kafka producer created, no `resume_from_kafka`, and after a
  successful S3 upload `if is_backup { return None }` skips all Kafka notify / gap-fill. The S3
  upload itself (`upload_to_s3`) runs regardless of `is_backup` → the delivery output is genuinely
  verified. `DEBANK_S3_ENABLED=true` is kept so the handler is really instantiated (backup ≠
  delivery-off).
- `DEBANK_VERSION=<prod-version>-merge-v31` (vs production `<prod-version>`) → the S3 key is
  `{chain_id}/{DEBANK_VERSION}/...`, and `is_backup` does **not** change the key. Reusing the
  production version would make a catching-up backup re-process production blocks and **overwrite the
  same production keys** — and since the whole point is to verify the merge didn't break
  `build_debank_output`, we cannot assume idempotent/correct output. So the version suffix writes to
  a **disjoint prefix** `<chain-id>/<prod-version>-merge-v31/...`; Step 5 then diffs the isolated prefix vs
  the production prefix for the same block hashes.

**No contention with production:** ZKsync EN has no etcd leader election; the only shared resource is
S3 (isolated by version) and Kafka (off in backup mode); main-node / L1 / <DA> are read-only
fetches; PG + RocksDB are on the isolated test EBS. No nodekey cleanup is needed — the EN syncs via
main-node RPC, has no consensus component and no libp2p identity.

Startup-log confirmation of the mechanism: `Debank S3 output handler enabled for chain_id=<chain-id>,
version="<prod-version>-merge-v31", is_backup=true`; `backup mode — skipping Kafka producer`; and per
block `Uploaded debank data for block N to S3 (...)`. Absence-checks: no `Kafka producer created` /
`resumed from Kafka` / `Filling from S3` / `send Kafka notification`.

Full test `compose.yml` (secrets/infra endpoints redacted) is in the appendix.

## 4. Verification results

**Verdict: PENDING** — no evidence the merge breaks sync/consensus/correctness; everything actually
verified passes cleanly, but two items keep this short of a full PASS (neither is a failure signal).

| Check | Result |
|---|---|
| Startup (wiring, component init) | **PASS** — clean init, no panic, `error_lines=[]`, restart count 0, containers running |
| Continuous block import after state restore | **PASS** — 31 samples, head growing / in lockstep; steady import, no stall/reorg |
| Block-hash sampling vs reference (newly-synced range `5859146–5859165`) | **20/20 MATCH** |
| `trace_debankBlock` / delivery-path artifact compare | **SKIPPED** — no `trace_debank_block` config (needs a production-writer reference) |
| Catch-up to chain tip | **NOT REACHED** — lag 75782 (local 5887903 / ref 5963685); `head_growing=true` ⇒ "still catching up", **freshness, not stall** |

Reading: correctness and freshness are judged on separate axes. On correctness, the verified parts
are clean — clean startup, healthy continuous import, and 20/20 byte-level block-hash match on the
synced range. The lag is environmental, not a code defect: the restore snapshot is ~31 days old
(2026-05-19) and the test volume is IO-bound, so the bounded catch-up window couldn't close ~76k
blocks; `head_growing=true` confirms forward progress rather than a stuck node.

Why not a full PASS yet:
1. **Delivery-path correctness is not closed.** Per the Step 5 rule, correctness = checks 1–4 + hash
   + trace (or the live-tracer-mode blockfile/statediff artifact compare). The delivery-artifact leg
   was SKIPPED (no `trace_debank_block` config), so the one path that actually validates the merge's
   collection output (`build_debank_output`) is unverified.
2. **Catch-up not complete, and the waiver precondition isn't met.** Waiving catch-up requires hash
   **and** trace both confirmed (the v29.20.0 precedent waived only after hash 20/20 + trace 3/3
   byte-identical). With trace missing, neither a full PASS nor a confirmed post-waiver sign-off is
   justified.

Secondary: hash sampling covered only one segment (the catch-up region); a second segment near the
local head is still wanted to rule out a fork-off.

## 5. Other exposed issues / follow-ups

1. **metadata version discrepancy (Step 8 must correct).** metadata records `core-v29.17.0`, but the
   fork was actually merged to `core-v29.20.0` — metadata is **3 releases behind** the fork. The
   待合 range was still correctly anchored at v29.20.0→v31.0.0, so the merge analysis is unaffected,
   but Step 8 write-back must set `metadata.version` to the real `fork_merged` (v31.0.0 after this
   PR), not just increment from the stale value.
2. **Verification gaps to close before sign-off** (the §4 PENDING items): (a) run the delivery-path
   verification — confirm whether <chain> is ETL mode (port-forward to a production writer and compare
   `trace_debankBlock` on the same block) or live-tracer/blockfile mode (compare blockfile/statediff
   artifacts), and (b) either let catch-up complete or take an operator waiver, plus (c) add a second
   hash-sample segment near the local head. Then promote to PASS or documented waiver.
3. **`eth.rs` `data[4..]` panic — hardening TODO (pre-existing, low).** On the `0xeeee…eeee`
   synthetic-token `balanceOf` branch of the custom `multi_call` path, malformed calldata that
   decodes to <4 bytes panics the request handler. Pre-existing since the v29.20.0 baseline, **not**
   introduced by this merge; worst case is a single-request panic with no state/sync impact. Worth a
   `data.len() >= 4` guard in a follow-up, independent of this PR.
4. **Local full-workspace build limitation (recurring, not a regression).** boojum 0.32.10 does not
   compile on the pinned `nightly-2025-03-19`, so local can only `cargo check` boojum-free crates;
   the full compile gate is the CI docker build (both arches). This recurs every merge — keep
   relying on CI for the boojum-dep crates rather than treating local failure as a build break.
5. **Restore-snapshot freshness for verification.** The ~31-day-old snapshot made catch-up the
   bottleneck. For future merges, restore from a fresher backup (or raise the test volume IOPS /
   pre-warm) so the catch-up + delivery verification can complete inside the test window.

---

## Appendix: test `compose.yml` (redacted)

> Redactions: Kafka broker hostnames and the <DA> DA seed phrase are removed; the production
> `DEBANK_VERSION` hash is shown as `<prod-version>`. Snapshot / volume IDs are not included.

```yaml
# /opt/app/<chain>/writer_merge_core-v31.0.0/compose.yml
# Test backup writer (merge core-v31.0.0) for <chain>.
#
# backup != delivery-off: DebankS3OutputHandler is really instantiated and runs S3 upload
# (verifies the merge didn't break output); it only runs in backup identity:
#   - DEBANK_IS_BACKUP=true   -> no Kafka producer / no resume / no notify / no gap-fill
#   - DEBANK_VERSION isolated prefix -> S3 writes <chain-id>/<prod-version>-merge-v31/..., never overwrites
#                                       production <chain-id>/<prod-version>/...
# EN syncs via main-node RPC, no p2p -> no nodekey cleanup.
# PG/RocksDB both on the mounted test EBS (./data -> /var/data), no shared storage with production.

name: <chain>-merge-writer

services:
  postgresql:
    container_name: <chain>-merge-writer-pg
    image: postgres:14
    user: "999"                       # matches sts securityContext.runAsUser=999 (snapshot <chain>_pg is uid 999)
    entrypoint:
      - bash
      - "-c"
      - |
        set -eux

        _term() {
          echo "Caught SIGTERM signal!"
          kill -TERM "$child" 2>/dev/null
        }
        trap _term SIGTERM

        runmode=`cat /etc/podinfo/runmode`

        if [[ X${runmode} != Xnormal ]]
        then
          echo "entering $runmode mode."
          if [[ X${runmode} == Xprune ]]
          then
              touch /prune.done
              echo "prune done."
          fi
          touch /tmp/done.done
          tail -f /dev/null &
          child=$!
          wait "$child"
        fi
        exec postgres \
        -D /var/data/<chain>_pg \
        -c listen_addresses='127.0.0.1' \
        -c max_connections=200 \
        -c log_error_verbosity=terse \
        -c shared_buffers=2GB \
        -c effective_cache_size=4GB \
        -c maintenance_work_mem=1GB \
        -c checkpoint_completion_target=0.9 \
        -c random_page_cost=1.1 \
        -c effective_io_concurrency=200 \
        -c min_wal_size=4GB \
        -c max_wal_size=16GB \
        -c max_worker_processes=16 \
        -c checkpoint_timeout=1800
    environment:
      POSTGRES_PASSWORD: "notsecurepassword"
      POSTGRES_HOST_AUTH_METHOD: "trust"
      PGPORT: "5430"                  # CRITICAL: PG listen port; node DATABASE_URL must point at :5430
    volumes:
      - ./data:/var/data              # mounted test EBS (contains <chain>_pg/ and <chain>_rocksdb/)
      - ./podinfo:/etc/podinfo:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres -p 5430 || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 30
      start_period: 300s              # allow WAL replay after snapshot restore
    stop_grace_period: 120s
    restart: unless-stopped
    network_mode: host
    logging:
      driver: json-file
      options:
        max-size: "100m"
        max-file: "5"

  node:
    container_name: <chain>-merge-writer-node
    image: ${PR_IMAGE}                # injected at deploy (zksync_external PR build)
    # no user: -- sts securityContext=null, use image default user (rocksdb owned by that uid)
    depends_on:
      postgresql:
        condition: service_healthy    # avoid <chain> 2026-06-11 ~7min PG connect-timeout restart loop
    entrypoint:
      - bash
      - "-c"
      - |
        set -eux

        _term() {
          echo "Caught SIGTERM signal!"
          kill -TERM "$child" 2>/dev/null
        }
        trap _term SIGTERM

        runmode=`cat /etc/podinfo/runmode`

        if [[ X${runmode} != Xnormal ]]
        then
          echo "entering $runmode mode."
          if [[ X${runmode} == Xprune ]]
          then
              touch /prune.done
              echo "prune done."
          fi
          touch /done.done
          tail -f /dev/null &
          child=$!
          wait "$child"
        fi
        sqlx database setup

        exec zksync_external_node --components "core,api,tree,da_fetcher"
    environment:
      # ---- backup safety (two explicit guards against production pollution) ----
      DEBANK_IS_BACKUP: "true"                 # skip Kafka producer/notify/resume/gapfill
      DEBANK_S3_ENABLED: "true"                # keep delivery code live (verify merge output)
      DEBANK_VERSION: "<prod-version>-merge-v31"   # isolated S3 prefix (prod=<prod-version>)
      DEBANK_KAFKA_TOPIC: "nodex_pipeline_<chain-id>_<prod-version>"   # inert in backup mode (no producer)
      DEBANK_KAFKA_BROKERS: "<kafka-brokers-redacted>"
      # ---- DB wiring (<chain> 2026-06-11 case: port must be 5430) ----
      DATABASE_URL: "postgres://postgres:notsecurepassword@127.0.0.1:5430/zksync"
      DATABASE_POOL_SIZE: "50"
      EN_POSTGRES_STATEMENT_TIMEOUT_SEC: "300"
      # ---- storage (on test EBS) ----
      EN_STATE_CACHE_PATH: "/var/data/<chain>_rocksdb/ext-node/state_keeper"
      EN_MERKLE_TREE_PATH: "/var/data/<chain>_rocksdb/ext-node/lightweight"
      # ---- upstream (read-only) ----
      EN_ETH_CLIENT_URL: "https://ethereum-rpc.publicnode.com"
      EN_MAIN_NODE_URL: "https://<main-node-rpc>/"
      # ---- internal bind ports (container netns) ----
      EN_HTTP_PORT: "3060"
      EN_WS_PORT: "3061"
      EN_PROMETHEUS_PORT: "29261"
      EN_HEALTHCHECK_PORT: "3081"
      # ---- chain / mode ----
      EN_L1_CHAIN_ID: "1"
      EN_L2_CHAIN_ID: "<chain-id>"
      EN_L1_BATCH_COMMIT_DATA_GENERATOR_MODE: "Validium"
      EN_API_NAMESPACES: "eth,net,web3,zks,en,pubsub,debug"   # debug -> persists call_traces (S3 output dep)
      EN_PRUNING_ENABLED: "false"
      EN_SNAPSHOTS_RECOVERY_ENABLED: "false"
      # ---- <DA> DA (da_fetcher, read-only fetch) ----
      EN_DA_CLIENT: "<DA>"
      EN_DA_<DA>_CLIENT_TYPE: "FullClient"
      EN_DA_BRIDGE_API_URL: "https://<da-bridge>"
      EN_DA_TIMEOUT_MS: "20000"
      EN_DA_API_NODE_URL: "https://<da-node>"
      EN_DA_APP_ID: "26"
      EN_DA_SECRETS_SEED_PHRASE: "<da-seed-phrase-redacted>"
      # ---- misc ----
      AWS_REGION: "ap-northeast-1"
      RUST_LOG: "zksync_core=info,zksync_dal=info,zksync_eth_client=info,zksync_merkle_tree=info,zksync_storage=info,zksync_state=info,zksync_types=info,vm=info,zksync_external_node=info,zksync_utils=info,"
      RUST_BACKTRACE: "full"
    volumes:
      - ./data:/var/data
      - ./podinfo:/etc/podinfo:ro
      # - ${HOME}/.aws:/root/.aws:ro   # uncomment if instance role lacks S3 PutObject (see preflight)
    stop_grace_period: 120s
    restart: unless-stopped
    network_mode: host
    logging:
      driver: json-file
      options:
        max-size: "100m"
        max-file: "5"
```
