# Upstream core-v29.20.0 merge — verification report

PR: #123 (`merge/upstream-core-v29.20.0` → `debank`, previous base `core-v29.19.0`)
Image verified: `zksync_external:amd64-3b651bf` (PR build, amd64+arm64 both passed CI)

## 1. Release content & upgrade rationale

**Conclusion: worth merging; no urgency.** Upstream release notes (auto-generated changelogs) carry **no
recommended/critical/ASAP wording**, so the assessment below is our own analysis.

The increment spans three releases (29.19.1, 29.19.2, 29.20.0), six changes total:

| Change | EN relevance |
|---|---|
| #4835 `fix(api)`: handle NULL protocol_version when replaying batch for traces (29.19.1) | **Relevant** — fixes trace replay on historical blocks predating protocol versioning; applies to our trace APIs automatically (shared `replay_l1_batch` path, no duplicated copy in our patch surface) |
| #4841 `fix(en)`: ignore rpc failure for sync_state job (29.20.0) | **Relevant** — upstreams the RPC half of our own resilience patch (fe51d7a75); lets us retire that half of the fork patch |
| #4824 consensus: gate settlement_layer/interop_fee on protocol_version >= 31 (29.19.1) | Low — defensive gating |
| #4828, #4838, #4836 (airbender prover) | None — prover components, EN does not build/run them |

Also includes airbender DB migrations (new tables/columns only, additive, harmless for EN's local PG).

## 2. Merge conflicts & impact surface

One real conflict; everything else auto-merged and semantically reviewed.

- `core/node/node_sync/src/node/sync_state_updater.rs` — upstream #4841 implements the same
  RPC-failure tolerance our resilience patch added. Took upstream's version for the
  `get_block_number()` match. **Kept the DB-query tolerance half of our patch**: upstream still
  propagates DB errors via `?` there, and the original motivation (a transient DB connection error
  kills the task and cascades into full node shutdown — fe51d7a75) applies unchanged.
- `core/Cargo.lock` — auto-merged + one-line follow-up syncing our `zksync_s3_backfill` crate
  version after upstream's workspace bump.
- `docker/external-node/Dockerfile` — runtime base image bumped to the matching upstream alpha tag
  (same maintenance as PR #121).

Semantic review of shared files: upstream's changes in `blocks_dal.rs` are confined to
eth_sender/prover mappers (EN does not call them) and do not touch our added query; the delivery
path (`state_keeper` S3 persistence, Kafka notification, output handler wiring) is **untouched by
this increment**. Dependency reconciliation: all fork-added workspace members/deps survive.

Local full-workspace `cargo check` is blocked by a **pre-existing baseline issue** (`boojum 0.32.10`
fails on the pinned `nightly-2025-03-19` toolchain; reproduced identically on the `debank` baseline
in a clean worktree). Full-workspace compile gate = CI docker build (both arches green on this PR).

## 3. Verification setup & results

Backup-mode writer (external node + dedicated Postgres, restored from a production writer snapshot)
on an isolated test box, running the PR image. DB migrations (29.15-era snapshot schema →
29.20) applied cleanly via the image entrypoint's `sqlx database setup` (~0.4s for the largest one;
all `ADD COLUMN ... DEFAULT const` / new-table migrations).

| Check | Result |
|---|---|
| Startup (wiring, component init) | PASS — clean init, no panic; one INFO line false-flagged by the sampler's error regex |
| Continuous block sealing after state restore | PASS — steady progress; absolute rate was limited by snapshot lazy-restore IO on the test volume (environmental, not code) |
| Block-hash sampling vs official RPC (newly-synced range) | **20/20 MATCH** |
| `debug_traceBlockByNumber` (callTracer) vs official RPC, snapshot-era + newly-synced blocks | **3/3 byte-identical** (largest block ~172 KB of trace JSON) |
| `trace_debankBlock` smoke (our delivery-format API) | PASS — full `block_file` structure (header/txs/traces/events), block id matches chain |
| S3 delivery path, live (`DEBANK_S3_ENABLED=true`, `DEBANK_IS_BACKUP=true`, production version prefix) | PASS — **169 blocks uploaded end-to-end** (handler init, instance-role credentials, retry loop, objects landed with correct keys); Kafka producer correctly not created in backup mode |
| Catch-up to chain tip | **Waived by operator decision** — sync health was established by the above; full catch-up was IO-bound on the test volume and adds no signal for this merge |

## 4. Risks & observations for rollout

1. **`en_getInteropFee` 403 log noise**: 29.19+ EN polls the main node for interop fee every ~5s;
   until the chain's main node supports that method, the writer logs a WARN per poll. Fallback is
   safe (warn + keep last value; writers don't serve tx fee estimation), sync unaffected. Noise
   disappears when the main node side upgrades.
2. **Pre-existing backup-mode S3 overwrite defect (PR #113, NOT introduced here)**: backup mode
   uploads unconditionally (no existence check / conditional PUT), so a catching-up backup rewrites
   the leader's already-written historical objects (LastModified changes; content is
   generation-equivalent — the only delivery-path change since the production release is a retry
   tuning commit). Confirmed live during this verification. Fix (head-check or `If-None-Match` in
   backup mode) tracked separately; out of this PR's scope.
3. **Upgrade runbook — migrations**: this release requires `sqlx database setup` before EN start.
   The image entrypoint handles it, but k8s manifests that override the entrypoint with a shell
   wrapper (current production pattern keeps only a comment where the command used to be) must run
   it explicitly, or the node exits with `column "interop_fee" does not exist`.
4. Our debank/pre/trace API namespaces are gated under the `eth`/`debug` namespace switches; there
   is no standalone config value for them (config enum rejects unknown variants).

## 5. Conclusion

**PASS** (catch-up waived). Sync, trace correctness, delivery format, and the live S3 delivery path
all verified against the PR image. Recommend merging.
