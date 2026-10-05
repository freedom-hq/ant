# antd ↔ ant-ffi parity audit

- **Baseline:** `main` at `75d7328` (v0.5.47). File and line references are at that commit, i.e. *before* PR #97, which shifts some lines in `drive.rs`, `lib.rs`, `gateway.rs`, `stamps.rs`, `discover.rs` and `antd/src/main.rs`.
- **PR #97:** ships this doc and fixes F0 (persisted-issuer verification).
- **Date:** 2026-09-29.

**Why this exists:** the phantom-batch fix for issue #49 (v0.5.41) landed only in `antd`. `ant-ffi` has its own copy of the startup orchestration, so iOS kept offering batches that no longer existed on-chain until #97. This audit lists every orchestration step in both entry points so the next divergence is visible before it ships.

**Path shorthands used in this doc:**

- `antd` = `crates/antd/src/main.rs`
- `ffi` = `crates/ant-ffi/src/lib.rs`
- `ffi-gw` = `crates/ant-ffi/src/gateway.rs`
- `drive` = `crates/ant-ffi/src/drive.rs`
- `node` = `crates/ant-node/src/lib.rs`
- `beh` = `crates/ant-p2p/src/behaviour.rs`
- `cbstore` = `crates/ant-chain/src/chequebook_store.rs`
- `discover` = `crates/ant-chain/src/discover.rs`

**iOS profile used for severities.** Freedom Browser embeds ant-ffi with the `chain` feature (freedom-mobile-ffi enables `ant-ffi/chain` by default), in light mode, with the gateway on `127.0.0.1:1633`. The iOS side confirmed the call sequence (`SwarmNode.start`, [§6](#6-open-questions-for-the-ios-side)):

1. `ant_init`.
2. `ant_set_chain_transport`, which serves chain reads from the app's router. This is on branch `feat/ant-chain-transport`, in progress.
3. `ant_deploy_chequebook(handle, gnosis_rpc)` on every light-mode start. It is best-effort.
4. `ant_start_gateway(..., gnosis_rpc)`.
5. `ant_storage_discover(handle, gnosis_rpc)`, once after the gateway is up. This is also on an in-progress branch.

Stamps are bought **only** through the gateway: `POST /stamps/{amount}/{depth}` and `PATCH /stamps/topup|dilute`. The app never calls `ant_storage_buy*` or `ant_storage_connect_batch`.

## Legend

**Status:**

| Status | Meaning |
|---|---|
| **shared** | Implemented in a shared crate. Both entry points get it just by running `run_node` or `Gateway::serve`, or by calling the same helper. |
| **ported** | ant-ffi has its own implementation that mirrors antd's. It can drift. |
| **host** | ant-ffi exposes it as an FFI call the host must make; the call is named. |
| **missing** | antd does it and ant-ffi has no equivalent. |
| **n/a** | Not applicable on mobile; the reason is given. |
| **ffi-only** | The reverse gap: ant-ffi has it and antd does not. |

**iOS severity:**

| Severity | Meaning |
|---|---|
| **High** | User-visible failure or money at risk in normal use. |
| **Medium** | Breaks a recovery or edge flow (reinstall, restore from key, data loss), or degrades uploads. |
| **Low** | Diagnostics, cosmetic, or rare. |

---

## 1. Summary

**Gaps found:**

1. **Persisted-issuer on-chain verification** was missing in ant-ffi. #97 fixes it by moving the check into a shared helper, `ant_chain::discover::verify_persisted_batch`, and running it at `ant_start_gateway`.
2. **Startup rediscovery of owned batches** (antd step 3) exists in ant-ffi only behind `ant_storage_discover`. Severity: **Medium–High**; **host-covered for now**. The iOS app is starting to call `ant_storage_discover` once after the gateway starts (in-progress branch), which covers reinstall and restore-from-key. Without that call, funded on-chain batches don't show up.
3. **Outbound settlement is never enabled by the gateway buy path** (`POST /stamps` only sends `RegisterBatch`; `ant-gateway/src/chain.rs:451-490`). This affects both entry points:
   - **antd** resolves or auto-deploys a chequebook only at startup.
   - **ant-ffi** enables settlement only from `ant_storage_buy*`, `ant_storage_connect_batch` and `ant_storage_discover`, plus a persisted chequebook at init.

   iOS buys only through the gateway, but calls `ant_deploy_chequebook` at every light-mode start, so settlement comes up at init from the persisted `chequebook.json` from the next launch on. What remains is a **first-session window**: uploads in the session where the first batch is bought run without outbound SWAP. antd's own warning says sustained uploads then stall at about 20K chunks across the peer set (`antd:766-769`). Severity: **Medium**; it self-heals on the next launch. See F3/F5 for the two variants of the window.
4. **Chequebook factory and issuer verification** (`verify_then_build_swap`, `antd:1989-2099`) is missing in ant-ffi. **Low** for iOS, because ant-ffi only adopts chequebooks it deployed or rediscovered, and it checks the owner of the file.
5. **`ant_deploy_chequebook` doesn't turn settlement on** in the running node (`drive:1671-1705` never sends `EnablePushsyncSwap`). **Low–Medium; iOS is blocked on this** for a fresh install's first session: the chequebook is deployed at launch but settlement stays off until the next launch. F5 is enough only when the wallet is already funded at launch; see F3.
6. **The starting list of 15 symbols needs correcting.** 7 of the 15 are used by ant-ffi at `75d7328` (see [§5](#5-corrections-to-the-starting-symbol-list)). The two gaps that matter for iOS were identified correctly (verification and rediscovery). The chequebook rows (3–5) add to them.

**Reverse gaps:** ant-ffi has things antd lacks: account-state binding, an owner-checked chequebook load, suspend/resume/wake, a host chain transport, and runtime settlement enable. Each is listed below.

**Not parity issues, but found along the way (§4):**

- antd's `--network-id` never reaches the swarm.
- The peerstore is not flushed on antd SIGTERM or `ant_shutdown`.
- ant-ffi's malformed-`chequebook.json` handling contradicts its own comments.

### 1.1 Status after the parity catch-up (`fix/ffi-parity-catchup`)

The matrix below describes `75d7328`. The catch-up branch, stacked on #97, changes it as follows.

**Done:**

| Item | What changed |
|---|---|
| F1 / F2 | `ant_start_gateway` runs `ChainInit`: the #97 check, rediscovery of owned batches (once per handle), and adopting the persisted or on-chain chequebook to switch settlement on. It neither deploys nor funds. The host's `ant_storage_discover` call becomes redundant but stays harmless. |
| F3 (ant-ffi) | `GatewayHandle::on_batch_bought`; ant-ffi wires it to `ensure_settlement`, so the first gateway buy gets a chequebook and settlement. |
| F4 | `chequebook_store::check_chequebook` + `ChequebookChecks::verdict`, shared. antd's logs are unchanged; `issuer()` is only read when its answer is reported (`IssuerRead`). ant-ffi checks a persisted chequebook before enabling it; the one `ant_init` enabled unchecked (no RPC at init) is switched off again (`DisablePushsyncSwap`) if the check disqualifies it. A factory "no" for a chequebook whose recorded deploy tx a lagging backend hasn't served yet is not a disqualification. |
| F5 | `ant_deploy_chequebook` switches settlement on. |
| Chequebook setup | All settlement paths go through one locked routine (`setup_settlement`), so overlapping paths can't deploy twice. |
| S20 / S5 (antd) | antd uses the owner-checked chequebook loader. The path-trusting one is removed. |
| Corrupt `chequebook.json` | Resolved by on-chain rediscovery instead of erroring forever. When the scan finds nothing, no chequebook is deployed (the record may name a deposit-0 one the scan can't see); the user fixes or removes the file. |
| `--network-id` | Now reaches the swarm (`NodeConfig::with_network_id`). |
| Peerstore | Flushed when the swarm loop is dropped, so both antd's SIGTERM and `ant_shutdown` save it. The JNI `nativeShutdown` now joins the runtime like `ant_shutdown` (it used `shutdown_background`), so the flush lands before it returns. |
| Cleanups | The ignored-config-keys log, the `ant_settlement_*` doc drift, and the dead `default_backoff` are fixed. |
| Guard (§7) | The AGENTS.md rule and the `parity_guard` test are in, with an empty allowlist. |

**Done in the follow-up (`feat/antd-settlement-parity`, stacked on the catch-up):**

- **F3 (antd):** antd wires `on_batch_bought`. After a gateway buy, while settlement is still off, it re-runs its startup chequebook resolution (honouring `--no-auto-chequebook` and the manual flags) and enables settlement at runtime.
- **Deposit size:** one default, `chequebook_store::DEFAULT_CHEQUEBOOK_DEPOSIT_PLUR` = 0.001 xBZZ. antd's default drops from 0.1 (`--chequebook-deposit-plur` still overrides it).
- **Deposit top-up:** antd now tops an antd-managed chequebook back up to the target at startup and after every gateway buy, through the shared `top_up_chequebook` that ant-ffi uses on every buy too. Before this, antd never topped up. This matters for Freedom's setup order (xDAI, light mode, then xBZZ): the chequebook is deployed before the wallet holds any xBZZ, so it starts at zero.
- **Live chequebook address:** `ant_gateway::ChequebookSlot` is shared by the chain context and the writer. A chequebook set up after startup (a buy-triggered deploy, or ant-ffi's gateway-start adoption) shows in `/chequebook/*` and `/wallet`, and can receive `POST /chequebook/deposit`, without a restart, in both entry points. In ant-ffi that includes chequebooks set up by the C API (`ant_storage_buy*`, `ant_storage_connect_batch`, `ant_storage_discover`, `ant_deploy_chequebook`). A chequebook those calls (or `ant_storage_settlement_topup`) find disqualified on-chain is cleared from the slot, even though the call itself errors. Before v0.5.52 only the gateway's own after-buy hook and chain init updated the slot.
- **No deposit into a chequebook the chain rejects:** `top_up_chequebook` re-runs `check_chequebook` right before any transfer and sends only when both checks read "yes" (a failed read is an error, a "no" is `TopUp::Refused`). Both entry points treat a refusal as a disqualifying verdict: antd switches settlement off (`DisablePushsyncSwap`, unless `--chequebook-allow-unverified`) and clears the gateway slot; ant-ffi records it in `DISQUALIFIED` and switches settlement off. ant-ffi's gateway slot is one per handle, so it never reports (or lets `POST /chequebook/deposit` fund) a disqualified chequebook, also after an idempotent `ant_start_gateway` retry, and a failed read never lifts an earlier disqualification.

**Done in the xDAI funding follow-up (`feat/xdai-storage-funding`, stacked on the settlement follow-up):**

- **Storage funding is shared.** AntDrive's xDAI-only flow (price a plan against the node wallet, swap the missing xBZZ through the `BzzSwapHelper`, buy; the same for extending or resizing a batch and for the chequebook deposit) moves from ant-ffi's `drive.rs` into `ant_chain::funding`. ant-ffi's C functions keep their JSON and call it; so does the gateway's chain writer, which serves new ant-specific routes on both entry points: `GET /v0/storage/quote`, `POST /v0/storage/buy`, `POST /v0/storage/extend`, `GET`/`POST /v0/settlement/deposit` (R13).
- **One on-chain write at a time.** Every gateway write route answers `409` while another runs (a `WriteGate` on the chain context). Each route then holds the settlement branch's `WalletTxLock` across its read-then-write (balance or shortfall read → transactions), the same lock antd's after-buy/startup settlement and ant-ffi's C API and after-buy task hold, so background settlement makes a route wait rather than `409`. Innermost, `ant-chain` serialises every transaction a key signs, process-wide, from the nonce read to the receipt. Order: `WriteGate` → embedder settlement lock → `WalletTxLock` → per-key sender lock (R14).
- **Deposit safety on the new routes.** `funding::fund_deposit_with_xdai` transfers through the shared `top_up_chequebook` (its pre-transfer `check_chequebook`; `TopUp::Refused` sends nothing) and runs the same checks before any swap. A refusal goes through the shared `not_registered_may_be_lag` before anything is switched off: in ant-ffi (C API and the gateway's refused-chequebook hook) via `chequebook_refused` → `DISQUALIFIED` + `DisablePushsyncSwap`; in antd via `SettlementOnBuy::refused`. A disqualified chequebook is `ChequebookSlot::refuse`d, so `/v0/storage/quote` prices no deposit and `/v0/settlement/deposit` reports and funds none; ant-ffi's `ant_storage_quote` does the same from `DISQUALIFIED`.
- **No web-page spending of xDAI.** The xDAI-swapping writes (`POST /v0/storage/buy`, `POST /v0/storage/extend`, `POST /v0/settlement/deposit`) are query-only, so a page could send them as CORS-simple requests with no preflight. Both entry points share the router's `cors::wallet_spend_guard`: a request carrying a browser `Origin` (or a cross-origin `Sec-Fetch-Site`) gets `403` unless that origin is listed exactly in the CORS allow-list — `*` and `null` never count. Non-browser callers (Freedom's main process, the ant-ffi host, `curl`) are unaffected. The older bee routes (`/stamps`, `/chequebook/deposit`) stay as they were (issue #105).
- **Immutable by default.** `POST /stamps` reads bee-js's `immutable` header as well as the query and defaults to immutable, like bee; `POST /v0/storage/buy` does too.
- **Guard:** `ant_chain::funding` is an orchestration module. antd's side counts the gateway's chain writer (`ant-gateway/src/chainreader.rs`), which its HTTP routes run through. `top_up_batch` is listed as antd-only: bee's `PATCH /stamps/topup` pays in xBZZ, and the C API only extends with xDAI.

**Done in the startup-scan fix (`fix/startup-scan-cursor`, issue #118):**

- **One saved transfer scan (S16, S21).** Batch and chequebook rediscovery both read the node wallet's xBZZ `Transfer(from = node)` history. `discover::refresh_transfer_scan` keeps it in `<data-dir>/transfer-scan-<chain-id>-0x<eoa>.json` (keyed by the RPC's `eth_chainId` as well as the wallet, so a scan is never continued on another chain) and continues it from the last scanned block, minus a 1 024-block rescan tail, so only the first start reads the history from the xBZZ deploy block. The tail covers reorgs and a load-balanced RPC whose `eth_getLogs` backend lags the `eth_blockNumber` one and answers `[]` for blocks it hasn't seen. A miss below the tail (an RPC that answered a window incompletely) is what `discover::rescan_transfer_history` is for: it reads the whole history again and replaces the saved scan. It runs on antd's `--rescan-chain-history`, on ant-ffi's `ant_storage_discover_full` (a user's explicit "search again"; plain `ant_storage_discover` continues the saved scan, since hosts call it at every start), and inside `discover::find_owned_chequebook` before either entry point deploys a chequebook: a "none" that rests on routine continued scans is confirmed first, so a missed deposit can't be stranded behind a second chequebook. The saved scan records how far a confirming pass has read (`confirmed_through`: a scan from the xBZZ deploy block, then each confirming check's re-read of the blocks since), and the check reads again only the blocks above that mark — the whole history only when no pass has confirmed any (a scan file from before the mark). So a wallet whose deploy keeps failing (an unfunded fresh install) pays the full history once, not on every buy, connect or discover. A running scan saves its progress every 15 s and when it fails, so a first scan cut short resumes where it stopped; a full rescan that replaces a saved scan doesn't save its progress over it, so one that fails part-way leaves the previous complete scan in place. Scans run one at a time per process, so a stamp buy's chequebook check during the startup scan waits for it and then reads only the tail. `owned_batches_in` and `owned_chequebook_in` read it. The full-scan `discover_owned_batches` / `discover_owned_chequebook` are gone, and both entry points use the new helpers: antd's batch rediscovery and `resolve_chequebook` step 3 (`find_owned_chequebook`); ant-ffi's `ChainInit`, `ant_storage_discover`/`_full` and `resolve_or_deploy_chequebook` (`find_owned_chequebook`). The file is named by wallet, so `bind_account_state` doesn't need to park it, and a key swap starts a new scan. Before this, a wallet with history behind a range-capped log RPC paid two identical full-history scans per start, which took many minutes.
- **antd's `chainReady` no longer waits on the scans (R10).** With a write RPC, antd runs batch rediscovery and `resolve_chequebook` steps 3–4 (rediscover, else auto-deploy) in the background after the chain state is set, the way ant-ffi's `ChainInit` runs after its preset. The settlement part goes through `SettlementOnBuy`'s coalesced run, as a stamp buy would. The #49 issuer check and the persisted-chequebook check still run before `chainReady`. Without a write RPC, antd can only stamp with batches it already holds, so its rediscovery still runs first, as before. One difference remains: antd may auto-deploy, ant-ffi never does at gateway start. **Trade-off:** `chainReady` no longer implies every owned batch is registered. Until the background rediscovery logs "background batch rediscovery finished", `GET /stamps` lists only the batches reloaded from `postage/`; on the first start of a wallet with long history behind a range-capped RPC (no `postage/` store: a fresh data dir, a restore from key) that can be 15–25 minutes, and a client that buys whenever it sees no usable stamp can buy a batch the wallet already owns, or fail an upload for want of one. Restarts of the same data dir reload their batches before `chainReady` and aren't affected. ant-ffi's `ChainInit` has always had this window.

**Done in the wallet-scan status follow-up (`feat/wallet-scan-status`, freedom-browser#484):**

- **`/health.walletScan` (R10).** While an entry point rediscovers in the background, `/health` (and `/readiness`) report the rediscovery: `pending`, `scanning` with `from` / `scannedThrough` / `head`, `retrying` with `error` (URLs in it are replaced by `<url>`, since RPC URLs often carry API keys), then `done` once the batches it found are registered. A later short scan, such as a stamp buy's chequebook check, doesn't move `done` back. The field is absent when nothing is tracked: no logs RPC, or antd without a write RPC, which still rediscovers before `chainReady`. The status lives in `ant_chain::discover` (`wallet_scan_pending` / `_done` / `_failed` / `_status`). `refresh_transfer_scan` reports progress and failure, and the shared gateway handler reads it for the node's address. A host shows "looking for your existing storage" until `done`, instead of offering a plan the wallet may already have.
- **Both entry points announce `pending` before the chain reads as ready.** antd does it before it sets the gateway chain state; ant-ffi does it before it spawns the gateway, because its chain state is preset.
- **A failed rediscovery is retried in the background on both sides,** with the shared `rediscovery_retry_delay` (15 s, doubling, at most 5 minutes). The scan keeps its progress, so a retry reads only what's left.
  - antd's background task loops until batch rediscovery succeeds. Its settlement run happens once, after the first attempt.
  - ant-ffi's `ChainInit` runs at most one retry loop per handle, next to step 1's recheck. Before this, a failed scan in ant-ffi waited for the host's next `ant_start_gateway`. The host can still trigger an attempt sooner that way.

**Done in the unverified-first-scan follow-up (`feat/unverified-first-scan`, freedom-browser#484):**

- **Why:** under Freedom's verified-only routing, the only keyless providers that serve a wallet's whole xBZZ history in one `eth_getLogs` all run on the same backend (Tenderly). The independent ones stop at 10k blocks, so a first scan verified by two providers is ~3,200 windows, about an hour. Freedom's bridge gains a second independent full-history source (Blockscout's index), which makes the verified scan one request. This change is the fallback for when that source is down or disagrees.
- **`ChainClient::with_unverified_logs`** gives the transfer scan an explicitly unverified source: in antd via `--gnosis-unverified-logs-rpc-url`, in ant-ffi via `ant_set_unverified_logs_rpc`, which every `AntHandle::chain_client` then carries. A span the verified logs RPC can't serve in `MAX_VERIFIED_WINDOWS` (64) windows is read from it once, after a few instant range refusals, instead of window by window.
  - Each batch and chequebook found is still checked through the verified route.
  - A full rescan (`ant_storage_discover_full`, `--rescan-chain-history`) never uses it: it replaces a saved scan that may be confirmed, so it reads through the verified route only. A plain `ant_storage_discover` that reads unverified starts the handle's background confirmation too.
  - The blocks read that way aren't confirmed (`TransferScan::provisional_since`, stamped by the first window it actually serves).
  - While it fails, the pass reads on window by window through the verified route, as without it, trying it again every `MAX_VERIFIED_WINDOWS` windows: a first or long-offline scan still finishes while it's down instead of failing every retry, and one transient error (a 429) costs a stretch of verified windows, not the rest of the crawl. The blocks read verified above the unverified ones are recorded (`TransferScan::confirmed_above`), so a confirmation doesn't read them again.
  - `/health.walletScan` ends in a new state, `confirming`, instead of `done`.
- **`discover::confirm_transfer_scan`** re-reads the unconfirmed blocks through the verified route in at most `MAX_VERIFIED_WINDOWS` (64) windows per try — one request once the route has a second full-history source — never as a crawl. A try that fails or gives up part-way saves what it read, with the original `provisional_since`, so the next try resumes and the 6 h clock isn't restarted. Both entry points retry it in the background with `confirm_retry_delay` (1 min, doubling, at most 30 min), and once it's confirmed they register any batch the unverified read missed and move `walletScan` to `done`:
  - antd's background task, which then re-runs the chequebook resolution if settlement is still off;
  - ant-ffi's `ChainInit::confirm_unverified`, at most one loop per handle, which then adopts a chequebook if settlement isn't on yet and reports it to the gateway's slot. It never deploys at gateway start, as before.
- **`find_owned_chequebook`'s "none, so deploy"** never rests on unverified blocks.
  - Within `CONFIRM_CRAWL_AFTER_SECS` (6 h) of the unverified read, its confirming re-read gives up instead of crawling, and the node stays without settlement rather than deploying. A "none" with no unverified read behind it (only routine verified scans above the confirmed mark) is confirmed window by window at once, as without an unverified source.
  - After that, it reads the span window by window through the verified route, so a second source that stays down doesn't leave the node without settlement for good.
- **Only antd re-runs settlement once the 6 hours are up** (a 6 h settlement re-run in `confirm_unverified_scan`): only antd auto-deploys. In ant-ffi the crawl happens on the next call that may deploy (a storage buy, `ant_deploy_chequebook`).

**Deferred:**

- F6 (fd limit: measure on a device first).
- F7 (check whether freedom-mobile-ffi composes `log_layer()`).
- antd `Resume` trigger, antd host chain transport, and porting `bind_account_state` to antd.

---

## 2. Parity matrix

### 2.1 Startup orchestration

| # | Behaviour | antd | ant-ffi | Status | iOS severity / note |
|---|---|---|---|---|---|
| S1 | Log setup | `tracing_subscriber::fmt` + `EnvFilter`: `RUST_LOG`, else `--log-level` / config `verbosity`; stdout (`antd:347-351`) | `install_log_subscriber` → `log_layer()`: `ANT_LOG`, else `RUST_LOG`, else a default filter. `Once`-guarded; a host subscriber that is already installed wins. stderr, or logcat on Android (`ffi:699`, `ffi:3257-3283`) | **ported** (different code). Level is env-only; no C API. Rust hosts can compose `ant_ffi::log_layer()` (#94) | Low if freedom-mobile-ffi composes `log_layer()`. If it doesn't, WARNs such as #97's go to stderr, which the iOS simulator doesn't forward (see AGENTS.md) |
| S2 | Data-dir instance lock | `acquire_instance_lock(antd.lock)` (`antd:356`, `1053`) | none | **n/a** | Single host process; `StampIssuer` stores take their own lock (`ant-postage/src/lib.rs:995`) |
| S3 | Raise `RLIMIT_NOFILE` | `raise_nofile_soft_limit` (`antd:358`, `1094-1125`) | none | **missing** | **Unverified.** iOS default soft limits are low. With about 100 peers, in-flight dials and SQLite, fd exhaustion is possible. Measure `getrlimit` on device before deciding |
| S4 | Identity | bee v3 keystore `keys/swarm.key` + password, else `identity.json` / `--key-file` (`antd:360-375`, `1225-1290`) | `identity.json` (`DataDir`), or host-held `identity_json` via `ant_init_with_identity` that never touches disk (`ffi:704-707`, `3112-3155`) | **ported** (different model) | n/a. The Keychain replaces the keystore |
| S5 | Bind account-scoped state to the running key | none | `bind_account_state` parks `postage/`, `uploads/`, `chequebook.json`, `swap_credits.json`, `pushsync_outbound.json` (with its `.lost` marker and `.journal`/`.journal.old` cheque journal) under `accounts/<prev>/` (`ffi:519-603`, `720`) | **ffi-only** | Reverse gap. A swapped `keys/swarm.key` under antd reloads the previous account's postage and chequebook. With an RPC, the #49 check and the issuer check catch it; without one they don't |
| S6 | Status snapshot / `chain_ready` | starts `false`, flips when `LateChainInit` is applied (`antd:459`; `beh:1900-1903`, `2027-2029`) | hard-coded `true` (`ffi:744-746`) | **ported** (by design: no chain work at init) | n/a today. Revisit if chain work moves into `ant_start_gateway` (see F1) |
| S7 | Bootnodes | `--bootnodes`, else `ant_p2p::default_mainnet_bootnodes()` (`antd:390-393`) | default only (`ffi:758`) | **shared** (default) / not settable | n/a |
| S8 | Peerstore path / reset | `--peers-file`, `--no-peerstore`, `--reset-peerstore` (`antd:463-498`) | always `<data>/peers.json` (`ffi:759`); no FFI for `ResetPeerstore` | **shared** (ant-p2p persistence) | Low. No way to reset a poisoned peer list from the app |
| S9 | Disk chunk cache | `DiskChunkCache::open`, path and size configurable (default 10 GB), `--no-disk-cache` (`antd:520-558`) | `DiskChunkCache::open(<data>/chunks.sqlite, 512 MiB)` (`ffi:101`, `770-790`) | **shared** type, **ported** wiring | n/a. The size difference is intentional |
| S10 | Upload manager + rehydrate | `UploadManager::new(.., default_batch)` with disk cache and status watch; `rehydrate_from_disk(!--no-resume-uploads)`, where an error is fatal (`antd:589-624`) | the same, plus `with_source_root`; `rehydrate_from_disk(true)`, where an error only warns (`ffi:808-832`) | **shared** (`ant-node` uploads), **ported** wiring | n/a. `source_root` is ffi-only by design (container moves) |
| S11 | Post-upload heal / self-heal | automatic in the job driver | same | **shared** (`ant-node/src/uploads/mod.rs:2306-2350`) | — |
| S12 | SWAP inbound listener | `SwapConfig`, ledger `swap_credits.json` (`antd:631-636`) | same (`ffi:840-845`) | **ported** (identical 5-line struct) | — |
| S13 | Persisted postage reload | `StampIssuer::open_existing` over `postage/*.bin` (`antd:1372-1459`) | `drive::reload_persisted_issuers` (`drive:71-108`) | **ported** | — |
| S14 | Persisted-issuer on-chain verification (#49) | inline at startup when an RPC is set (`antd:1384-1435`) | none at `75d7328` | **missing** → shared helper in #97, run at `ant_start_gateway` | **High** (observed on the iOS simulator: two dead batches reported `usable: true`). Fixed by #97 |
| S15 | `--postage-batch` pre-registration | fetch meta, owner check, open store (`antd:1461-1517`) | `ant_storage_connect_batch(h, rpc, batch_id)` (`ffi:2385`; `drive:473-512`) | **host** | n/a. Freedom buys through the gateway |
| S16 | Startup rediscovery of owned batches (antd step 3) | `discover_owned_batches` on the logs RPC, which defaults to the public `https://rpc.gnosischain.com` (so it runs even without `--gnosis-rpc-url`) (`antd:1519-1570`, `223-228`) | only through `ant_storage_discover(h, rpc)` (`ffi:2422`; `drive:518-554`) | **host**; iOS is starting to call it once after gateway start (in-progress branch), so **host-covered for now** | **Medium–High** without the host call: after a reinstall, restore from key, or data loss, funded batches are invisible, `/stamps` is empty, and the user is pushed to buy again. It also sends `EnablePushsyncSwap` when it finds at least one batch (`drive:542-544`). See F2 |
| S17 | bee `stamperstore` bucket-counter recovery | `open_recovered_issuer` → `recover_bee_buckets` / `open_or_new_seeded` (`antd:1604-1666`) | none; `storage_discover` registers fresh issuers through `RegisterBatch` | **n/a** | There is no bee data dir on a phone. Caveat for both entry points: a rediscovered batch without counters restarts at index 0 and can reuse `(bucket, index)` slots stamped before (antd "option b", `antd:1649-1656`) |
| S18 | `--postage-owner-key` (separate stamp key) | `antd:1355-1367` | stamp key is always the node key (`ffi:804-805`) | **n/a** | Single-key model on mobile |
| S19 | Chequebook: manual `--chequebook` + `--swap-key` | `antd:1730-1765` | none | **n/a** | Operator feature |
| S20 | Chequebook: load persisted `chequebook.json` | `cbstore::load_persisted_chequebook` (no owner check); an error is fatal (`antd:1800`) | `load_persisted_chequebook_for(.., &eth)` (owner-checked); at init an error warns (`ffi:858-887`); in `resolve_or_deploy_chequebook` an error propagates (`drive:1743-1744`) | **ported**; the two differ | Reverse gap: antd should use the `_for` variant (see S5). For the ffi inconsistency see §4 |
| S21 | Chequebook: rediscover on-chain | at startup on the logs RPC (`antd:1828-1897`) | lazily inside `resolve_or_deploy_chequebook` (`drive:1754-1808`), reached only from `ant_storage_buy*`, `connect_batch`, `discover`, `ant_deploy_chequebook` | **host** (indirect) | **Medium**, together with S16: after a restore, `/chequebook/address` reads zero and settlement stays off until one of those calls. See F3 |
| S22 | Chequebook: factory + `issuer()` verification | `verify_then_build_swap` → `verify_chequebook_with_factory`, `read_chequebook_issuer` (`antd:1989-2099`) | none; the settlement config is built unconditionally (`ffi:869-875`; `drive:1302-1320`) | **missing** | **Low.** ffi only adopts chequebooks it deployed or rediscovered (rediscovery checks factory and issuer, `discover:~268+`) and checks the file owner. See F4 |
| S23 | Chequebook: first-run auto-deploy | at startup when light + RPC and not `--no-auto-chequebook`; deposit 0.1 xBZZ (`antd:1899-1979`, `46`) | inside `resolve_or_deploy_chequebook` during `ant_storage_buy*`, `connect_batch`, `discover` and `ant_deploy_chequebook`; deposit 0.001 xBZZ (`drive:1818-1837`, `623`) | **host** (indirect) | Spending gas at app start without a user action is a product decision; keep it host-driven. The deposit sizes differ (0.1 vs 0.001 xBZZ). See §4 |
| S24 | Outbound settlement enable | once, via `LateChainInit` (`antd:777-782`) | at init from a persisted chequebook (`ffi:858-887`), or at runtime via `EnablePushsyncSwap` from `ensure_settlement` (`drive:1274-1330`). `ensure_settlement` is reached from `ant_storage_buy*`, `connect_batch`, and `ant_storage_discover` when that finds at least one batch (`drive:542-544`), but **not** from `ant_deploy_chequebook` or the gateway buy | **ported** (different timing) | **Medium** (iOS: first-session window). See F3/F5 |
| S25 | Settlement deposit / top-up | only the initial deposit at auto-deploy; `POST /chequebook/deposit` (shared gateway) | `ant_storage_settlement_topup` / `_deposit` (`ffi:2145`, `2192`); `fund_chequebook_best_effort` for adopted chequebooks (`drive:1441-1495`) | **host** / **ffi-only** | — |
| S26 | Late chain init (swarm starts before chain reads) | `with_late_chain` + `LateChainInit` (`antd:649-665`, `777-782`) | none; no chain reads at init | **n/a** today | Becomes relevant if F1 moves chain work into `ant_start_gateway` |
| S27 | Gateway start | always (unless `--no-http-api`), bound before chain init; chain state set late through a `OnceLock` (`antd:692-720`, `822-825`) | `ant_start_gateway(h, api_addr, light_mode, gnosis_rpc)`; chain state preset (`ffi-gw:72-278`) | **host** | — |
| S28 | Gateway chain context | `chainreader::build(rpc, logs_rpc_fallback, --postage-contract, eth, resolved chequebook, …)` (`antd:788-815`) | `build_with_transport(gnosis_rpc, None, GNOSIS_POSTAGE_STAMP, eth, persisted chequebook, …, host transport)` (`ffi-gw:184-229`) | **shared** (`ant-gateway::chainreader`), **ported** arguments | Low. No read-only fallback RPC; iOS passes `gnosis_rpc`, so this is fine |
| S29 | Control socket (`antctl` / `antop`) | `bind_control_socket` + pointer file (`antd:419-430`, `677-680`, `892-961`) | none | **n/a** | The host calls C functions directly |
| S30 | Signals / shutdown | SIGTERM / SIGINT race (`antd:855-878`, `986-1017`) | `ant_shutdown`: drain the transport, then `shutdown_timeout(5s)` (`ffi:2874-2894`) | **n/a** (different lifecycle) | See §4 for the peerstore flush |

### 2.2 Runtime behaviours

| # | Behaviour | Where | antd | ant-ffi | Status | iOS note |
|---|---|---|---|---|---|---|
| R1 | Batch self-probe after a peer rejects a batch's stamps | `beh:4061-4354`; `usable = !rejected` in `PostageList` (`beh:3300-3323`) | active once `LateChainInit` delivers an upload runtime; a no-op on ultra-light (`beh:4298`) | always active (upload runtime always `Some`, `ffi:895`) | **shared** | — |
| R2 | Dial backoff + private-underlay filter; dial failures aren't charged (backoff / peerstore eviction) while our own link is down: no bzz peer, no pong within 10 s, or none since a stall / `Resume` / self-heal (issue #90). A tracked dial is also judged by the link state when it *started*: one started while the link was unproven, or before a later stall / `Resume` / self-heal, stays uncharged even if it only fails (OS SYN timeout, ~75-127 s) once the link is proven again | `beh:713-729`, `1100-1160`, `1204-1233`, `own_connectivity_down`, `note_dial_started`, `note_outgoing_dial_error`; `ant-p2p/src/underlay.rs:175-400`; `Liveness::network_unproven` | default; `--allow-private-dials` opt-in (`antd:655`) | default only | **shared** | n/a |
| R3 | Peerstore flush every 30 s + warm-start hints | `beh:359`, `1811-1869`, `1974-1981` | yes | yes | **shared** | See §4: no final flush on SIGTERM or `ant_shutdown` |
| R4 | Top-up / re-bootstrap | `beh:6644-6682`, `6861-6886` | yes | yes | **shared** | — |
| R5 | Resume after sleep / network change (`ControlCommand::Resume` → `force_resume`) | `beh:1998-2014`, `6704-6741` | **no trigger** (not in the control-socket protocol) | `ant_resume` (`ffi:1287`) | **ffi-only** / **host** | Reverse gap: desktop sleep/wake in antd relies only on the automatic recovery (R4, and R17's liveness pass since issue #83) |
| R6 | Upload suspend / wake | `node:395-413`; `ant-node/src/uploads/mod.rs:1471-1531` | no trigger | `ant_suspend` / `ant_wake` (`ffi:1332`, `1370`) | **ffi-only** / **host** | — |
| R7 | Runtime `RegisterBatch` | `beh:3324-3388` | via the gateway buy (`ant-gateway/src/chain.rs:451-490`) | via the gateway buy, plus `ant_storage_buy*`, `connect_batch`, `discover` | **shared** | — |
| R8 | Runtime `EnablePushsyncSwap` | `beh:3389-3432`; no factory or issuer check | not reachable (not in the control-socket `Request` enum; the gateway doesn't send it) | `drive:1305` only | **ffi-only** | See F3 |
| R9 | Host chain transport (issue #77) | `ant-chain/src/transport.rs`; `ChainClient::with_transport` | not present (6 plain `ChainClient::new` calls) | `ant_set_chain_transport`; every client built via `AntHandle::chain_client` (`ffi:219-225`) | **ffi-only** | — |
| R10 | Readiness / health | `ant-gateway/src/status.rs:43-117`; `/readiness` reads `RoutingInfo::serving` (`beh:serving_peer_count`, #78) | `/health.chainReady` flips late; `/node` answers 503 until then. `/health.walletScan` reports the background rediscovery (see §1.1) | preset, so `/node` is ready immediately; `light_mode` is whatever the host passes. `/health.walletScan` as in antd | **shared** handlers, **ported** wiring; `/readiness` **shared** (same node loop and bootnode dial on both) | n/a |
| R11 | `/stamps` chain enrichment | `ant-gateway/src/stamps.rs:208-232` | yes (the read fallback also gives ultra-light nodes a real TTL) | only when `gnosis_rpc` is set | **shared** | #97 adds `exists:false` / `usable:false` for batches confirmed missing on-chain |
| R12 | Gateway activity registry shared with the node (`antop` Retrieval tab) | `with_gateway_activity` (`antd:566`, `661`, `709`) | standalone `GatewayActivity::new()`; the node never reads it (`ffi-gw:242-244`) | **n/a** | No `antop` on mobile |
| R13 | xDAI storage funding: quote, buy, extend or resize, chequebook deposit | `ant-chain/src/funding.rs` | the gateway's `/v0/storage/*`, `/v0/settlement/deposit` (`ant-gateway/src/chain.rs`, `chainreader.rs`); the deposit is priced in unless `--no-auto-chequebook` or a manual `--chequebook` | the same routes on `ant_start_gateway`, plus `ant_storage_quote`, `ant_storage_buy_xdai`, `ant_storage_topup_*`, `ant_storage_settlement_*` (`drive.rs`) | **shared** (after `feat/xdai-storage-funding`) | The deposit top-up takes an optional amount (`POST /v0/settlement/deposit?amount=<PLUR>`, `ant_storage_settlement_topup_amount`): it deposits that much more whatever the target, through the same `funding::fund_deposit_with_xdai` |
| R14 | Write serialisation | `WriteGate`, `WalletTxLock` (`ant-gateway/src/chain.rs`); per-key `sender_lock` (`ant-chain/src/tx.rs`) | gateway writes answer `409` while one runs, then hold the context's `WalletTxLock` (shared with the after-buy/startup settlement via `WalletCoord`); every transaction takes the key's lock | the same gateway, with `drive::wallet_tx_lock(owner)` as its `WalletTxLock`; the C API and the background settlement take that lock and the key's lock | **shared** | — |
| R15 | A just-bought batch's propagation window | `ant-p2p/src/behaviour.rs` (`BATCH_PROPAGATION`, `BoughtBatch`); `RegisterBatch::bought_at_block` | the gateway buys (`POST /stamps`, `POST /v0/storage/buy`) register with the receipt's block | the same gateway, plus `ant_storage_buy*` | **shared** (the node) | For bee's confirmation window (10 blocks + its listener's 4-block tail, ~70 s) the batch reads `usable: false` (with `propagating: true` on `/stamps` from v0.5.52, so clients can tell it from a rejected batch) and its real `blockNumber`, and a peer's "not found on-chain" is waited out instead of failing the push or marking the batch phantom |
| R16 | SWAP payments (issues #121, #127): cheques for download *and* upload debt past the early-payment threshold, after the refresh, from one balance per peer as in bee | `ant-retrieval/src/accounting.rs` (`PeerBalance::cheque_due`, `RetrievalPayment`); `ant-p2p/src/push_pseudosettle.rs` (pushsync debits into that mirror) and `beh` `push_settlement`; `ant-p2p/src/pushsync_swap.rs` (the payer: bee-priced cheques, delivery check, ledger and chequebook); `beh` `sync_retrieval_payment` / `publish_settlement`; funds from `cbstore::watch_retrieval_funds` via `SetRetrievalFunds` | the watch starts with settlement (after `LateChainInit`, and after a buy enables it), over `--gnosis-rpc-url` or the logs RPC; switch: bee's `swap-enable` (`--swap-enable` / `ANT_SWAP_ENABLE` / the config key; flag wins; default on, so a funded chequebook pays — bee's default is off), runtime via `SetSwapEnabled` (control socket, or `PUT /v0/settlement/swap`); state in `StatusSnapshot::settlement` (`GET /node` `settlement`, `GET /v0/settlement/swap`); a lost ledger (`.lost` marker) is cleared by `--confirm-cheque-liability <chequebook>` | the watch starts on every successful `setup_settlement`, over the handle's chain client (host transport included); switch: `ant_set_swap_enabled` (or the in-process gateway's `PUT /v0/settlement/swap`); state: `ant_swap_status`; lost ledger: `ant_confirm_cheque_liability`. A chequebook reloaded at `ant_init` pays only after the first setup with an RPC, or the first successful `ant_storage_settlement_topup*` (which also publishes the new funds at once) | **shared** (decision and payer in the node, funds read by the shared helper, lost-ledger marker in `ant-p2p/src/swap.rs`, routes in the shared gateway) | Without a chequebook, with unknown or zero funds, with the switch off, or while the outbound ledger's figures for the chequebook are lost (moved aside as unparseable, until an operator confirms the liability), downloads and uploads use the free tier exactly as before. Until #127 upload cheques ignored the switch and the exchange rate. A cheque whose stream ends together with any of the peer's connections closing (within `DELIVERY_GRACE` — the one it went out on, or a dial-race duplicate / liveness-closed sibling, since the payer can't tell which carried it) or times out stays issued but lowers no debt (the next cheque pays those units again); a stream-only reset (bee refused the cheque, connection kept) still reads as delivered |
| R17 | Connection liveness (issue #83): ping every 5 s; a connection silent past its window is uncounted in `peers.connected` (`ant_peer_count`) and `RoutingInfo::serving` (`/readiness`, #78), then closed; a streak of retrieval link failures runs `force_resume` (once a minute at most); a loop stall (process frozen) gives every connection a fresh 12 s to answer | `ant-p2p/src/liveness.rs`; `beh` `maintain_liveness`, `handle_ping_event`; `RetrievalCounters::record_link_failure` (`ant-retrieval/src/counters.rs`) | yes | yes | **shared** (the node) | Covers the reaped-socket wedge without host help; `ant_resume` on foreground only makes recovery immediate instead of ~20-30 s later. A system suspend that freezes the monotonic clock shows no stall, but then the pongs don't look old either: dead sockets close within ~30 s of awake time |

### 2.3 Configuration surface

An embedded host can set only:

- `data_dir`, `source_root` and `identity_json` at `ant_init*`;
- `api_addr`, `light_mode` and `gnosis_rpc` at `ant_start_gateway`;
- a per-call `gnosis_rpc` on the `ant_storage_*` and `ant_deploy_chequebook` functions;
- a chain transport via `ant_set_chain_transport`;
- the env vars `ANT_LOG` / `RUST_LOG`.

| antd flag / env / config key | antd | ant-ffi | Class |
|---|---|---|---|
| `--config` | `antd:58`, merge `1146-1216` | — | n/a (the host passes parameters) |
| `--data-dir` / `data-dir` | `antd:73` | `data_dir` param (`ffi:342`, `367`, `415`) | FFI param |
| `--password`, `--password-file` / `password*` | `antd:64-70` | `identity_json` (`ffi:417`) replaces the keystore | n/a (different model) |
| `--network-id` / `network-id` / `mainnet` | `antd:77`; see §4 (never reaches the swarm) | hard-coded 1 (`ffi:713`, `728`) | hard-coded |
| `--bootnodes` | `antd:81` | `default_mainnet_bootnodes()` (`ffi:758`) | not settable |
| `--log-level` / `verbosity`, `RUST_LOG` | `antd:85`, `347-351` | `ANT_LOG` → `RUST_LOG` env (`ffi:3262-3264`); Rust `log_layer()` | env only; no C API |
| `--key-file` | `antd:89` | — | n/a |
| `--control-socket`, `--no-control-socket` | `antd:93-98` | — | n/a |
| `--api-addr` / `api-addr` | `antd:103` (config accepts `:port`) | `api_addr` (`ffi-gw:74`; plain `SocketAddr`, no `:port` normalisation) | FFI param |
| `--no-http-api` | `antd:109` | don't call `ant_start_gateway` | FFI (call or not) |
| `--cors-allowed-origins` | `antd:118` | hard-coded `["null"]` (`ffi-gw:249`) | hard-coded |
| `--external-address` | `antd:125`, `654` | — | not settable (n/a behind NAT) |
| `--allow-private-dials` | `antd:135`, `655` | — (default `false`) | not settable |
| `--peers-file`, `--no-peerstore`, `--reset-peerstore` | `antd:141-164` | fixed path; no reset | not settable |
| `--target-peers` | `antd:157`, `658` | — (100) | not settable (Low: battery tuning) |
| `--per-request-chunk-cache`, `--record-chunks` | `antd:172-183` | — | not settable (debug) |
| `--disk-cache-path`, `--disk-cache-max-gb`, `--no-disk-cache` | `antd:189-205` | fixed path, 512 MiB (`ffi:101`) | hard-coded |
| `--gnosis-rpc-url` / `GNOSIS_RPC_URL` / `blockchain-rpc-endpoint` | `antd:211`, `792`, `1338`, `1709` | `gnosis_rpc` on `ant_start_gateway` and each `ant_storage_*` call; no init-time RPC | FFI param (per call) |
| `--gnosis-logs-rpc-url` / `GNOSIS_LOGS_RPC_URL` (default public RPC; empty string disables recovery) | `antd:223-228`, `2104-2111` | — (log scans reuse the per-call `gnosis_rpc`; no gateway read fallback, `ffi-gw:210`) | not settable. Relevant to F2: a range-capped `gnosis_rpc` makes the discovery scan slow |
| `--gnosis-unverified-logs-rpc-url` / `GNOSIS_UNVERIFIED_LOGS_RPC_URL` (unset by default) | `antd` (`scan_client`) | `ant_set_unverified_logs_rpc` (`ffi`; every `AntHandle::chain_client` carries it) | **ported** (see §1.1, unverified first scan) |
| `--postage-contract` | `antd:232` | `GNOSIS_POSTAGE_STAMP` (same address) | hard-coded |
| `--postage-batch` / `STORAGE_STAMP_BATCH_ID` | `antd:238`, `578-593`, `1461-1517` | `ant_storage_connect_batch`, `ant_upload_start(batch_id)` | FFI (runtime) |
| `--postage-owner-key` / `STORAGE_STAMP_PRIVATE_KEY` | `antd:247` | — | n/a |
| `--no-resume-uploads` | `antd:257`, `610` | always auto-resume (`ffi:829`) | hard-coded |
| `--chequebook` / `CHEQUEBOOK_ADDRESS`, `--swap-key` / `SWAP_OWNER_KEY` / `WALLET_PRIVATE_KEY` | `antd:270-280`, `1713-1725` | — | n/a |
| `--chequebook-allow-unverified` | `antd:292` | — (no checks exist, S22) | n/a |
| `--swap-enable` / `ANT_SWAP_ENABLE` / config `swap-enable` (bee's switch; governs SWAP payments for downloads, issue #121, and uploads, issue #127) | `antd` `Opt::swap_enable`, `apply_config_file`; runtime `GET`/`PUT /v0/settlement/swap` (not persisted: the next start reads the flag / config file again) | `ant_set_swap_enabled` / `ant_swap_status` (runtime, not persisted), or the in-process gateway's route | FFI call |
| `--confirm-cheque-liability <chequebook>` (PR #126: clear a lost outbound ledger's `.lost` marker for one chequebook) | `antd` `confirm_cheque_liabilities` | `ant_confirm_cheque_liability` | FFI call |
| `--no-auto-chequebook` | `antd:305`, `1902` | — (no opt-out of the implicit deploy in `ensure_settlement`) | not settable |
| `--chequebook-deposit-plur` / `CHEQUEBOOK_DEPOSIT_PLUR` (0.1 xBZZ) | `antd:46`, `316` | `deposit::TARGET_PLUR` = 0.001 xBZZ (`drive:623`) | hard-coded (different value) |
| config `full-node`, `nat-addr`, other bee keys | `crates/antd/src/config.rs:56-71`; never read by antd | — | ignored on both sides |

ant-ffi-only inputs:

- `source_root` (`ffi:368`)
- `identity_json` (`ffi:417`)
- `light_mode` (`ffi-gw:75`; antd derives it as `upload.is_some()`, `antd:741`)
- `ant_set_chain_transport` (`crates/ant-ffi/src/chain_transport.rs:245`)
- the bench config (`ffi:2933`)

---

## 3. Missing-in-ffi rows: severity and cheapest fix

| ID | Gap | iOS severity | Cheapest fix | Shape |
|---|---|---|---|---|
| F0 | S14 persisted-issuer verification | High (observed) | Done in #97: `ant_chain::discover::verify_persisted_batch`, called by antd and by `ant_start_gateway` | shared helper |
| F1 | No "chain init" hook in ant-ffi. antd runs all chain-derived startup (S14, S16, S21, S22, S24) as one block before `LateChainInit`; ant-ffi scatters it across host calls or skips it | (umbrella for F2–F4) | Extract antd's chain-init block into one shared, chain-gated helper that returns the `LateChainInit` inputs (issuers to register or drop, resolved chequebook + verdict). antd calls it before `late_chain_tx.send`. ant-ffi calls it from `ant_start_gateway` (the #97 hook) and applies the result with `RegisterBatch` / unregister / `EnablePushsyncSwap`. Home: a `chain`-feature module in `ant-node` (it already depends on `ant-p2p`, which pulls in `ant-chain` and `ant-postage`), or `ant-chain` plus a thin `ant-postage` adapter | shared helper |
| F2 | S16 startup rediscovery of owned batches | **Medium–High**; **host-covered for now** (iOS calls `ant_storage_discover` once after gateway start, in-progress branch). A shared fix removes the dependence on the host | Inside F1: after #97's verification, run `discover_owned_batches` and register what it finds, skipping batches already registered, exactly like `antd:1537-1564`. Caveats: (a) the scan needs a wide `eth_getLogs` range. antd uses a separate public logs RPC for this (`antd:215-228`); ant-ffi should fall back to the same default when the host's `gnosis_rpc` is range-capped. `scan_logs` shrinks its window but can crawl on a 10-block cap. (b) It must run in the background (as #97 does) so gateway start doesn't wait. (c) A scan failure must not unregister anything | shared helper + existing hook |
| F3 | S21 + S24 settlement is not enabled unless the host makes a specific storage call; the gateway buy (`POST /stamps`) never enables it, in either entry point | **Medium** (confirmed: iOS buys only via the gateway, but deploys the chequebook at launch). Two variants of the first-session window: **(a)** the wallet is funded at launch, so the chequebook deploys but isn't enabled; F5 closes this. **(b)** A fresh install is unfunded at launch, so the deploy fails for lack of gas; the user funds the wallet and buys through the gateway in the same session, and no chequebook exists at all. F5 does **not** close (b). It needs the after-buy hook below, or the host calling `ant_deploy_chequebook` again after the first gateway buy (with F5 fixed) | (1) In F1, resolve the chequebook (persisted, then rediscover; **no** auto-deploy at gateway start) and send `EnablePushsyncSwap` when one is found. (2) For the buy-then-publish flow, give `GatewayHandle` an optional "after a batch is bought" hook. ant-ffi wires it to `ensure_settlement` (resolve or deploy, then enable); antd can wire it the same way, which also fixes antd's "unfunded at start, so no settlement until restart" case. Cheaper stop-gap: document that the host must call `ant_deploy_chequebook` after the first gateway buy, and fix F5 | shared helper + gateway hook |
| F4 | S22 chequebook factory + `issuer()` verification | Low | Move `verify_then_build_swap`'s checks into `cbstore` as `verify_chequebook(client, cb, signer_eoa) -> {Registered, NotRegistered, IssuerMismatch(addr), Unverified(err)}`, the same shape as #97's verdict. antd keeps its log lines and `--chequebook-allow-unverified`; ant-ffi runs it in F1 before `EnablePushsyncSwap` | shared helper |
| F5 | `ant_deploy_chequebook` doesn't enable settlement (`drive:1671-1705`) | Low–Medium. **iOS is blocked on this** for a fresh install's first session (variant (a) of F3) | After a successful resolve or deploy, send `EnablePushsyncSwap` (factor the tail of `ensure_settlement`, `drive:1302-1320`, into a function both call) | ffi-local |
| F6 | S3 `RLIMIT_NOFILE` | unverified | Measure the soft limit on device first. If it's low, move `raise_nofile_soft_limit` (`antd:1094-1125`) into a shared crate and call it from `init_inner` | shared helper |
| F7 | S1 no C-level log control | Low | Confirm freedom-mobile-ffi composes `ant_ffi::log_layer()`. If a C host ever needs it, add `ant_set_log_filter` | FFI entry (only if needed) |

Rows marked n/a in §2 are intentionally not proposed for porting.

**Reverse gaps worth closing in antd:**

- S20 / S5: switch to the owner-checked `load_persisted_chequebook_for`.
- R5 / R6: expose `Resume` over the control socket so a desktop host can signal wake.
- R9: host chain transport, if Freedom desktop wants a verified RPC source.

---

## 4. Findings that aren't parity gaps

- **antd `--network-id` never reaches the swarm.** `NodeConfig` has no network-id builder and `mainnet_default` hard-codes `1` (`node:134`). antd uses `opt.network_id` only for the displayed overlay (`antd:381`) and the status field (`antd:438`). `run_node` forwards `network_id: 1` (`node:280`), and the swarm computes its overlay and handshake from that (`beh:1692-1693`, `1829-1834`). Any `--network-id ≠ 1` gives an overlay that doesn't match the handshake. Fix: add `NodeConfig::with_network_id` and call it from antd.
- **No final peerstore flush on shutdown.** The only shutdown flush is the loop's own `ctrl_c` arm (`beh:2032-2035`). antd's SIGTERM path returns from `main`, and `ant_shutdown` cancels tasks (`ffi:2887-2892`), so neither reaches it. Up to 30 s of peer state is lost. On iOS, where the app is killed often, that means colder starts.
- **ant-ffi's malformed-`chequebook.json` handling contradicts its own comments.**
  - What the comments promise: `init_inner` warns and says "a fresh deploy on the next buy overwrites it" (`ffi:876-884`); `cbstore:98-103` says the same.
  - What the code does: `resolve_or_deploy_chequebook` propagates the error (`drive:1743-1744`). `ensure_settlement` then warns and returns on every buy, so the file is never overwritten and settlement never comes up.
  - No test covers this.
- **Chequebook deposit size diverges:** antd 0.1 xBZZ (`antd:46`), ant-ffi 0.001 xBZZ (`drive:623`). This may be intentional (a mobile wallet is thinner), but it isn't written down. *(Resolved in the follow-up: one shared 0.001 xBZZ default, plus a top-up to it in both entry points.)*
- **antd logs a config-file debug message before any subscriber exists** (`antd:1194-1198` runs before the `tracing_subscriber` init at `347`), so the "ignored config keys" message is always dropped.
- **Doc drift:** `crates/ant-ffi/src/chain_transport.rs:216` and `ffi-gw:53` refer to `ant_settlement_*`; the real names are `ant_storage_settlement_*`.
- **Unused helper:** `ant_p2p::default_backoff()` (`crates/ant-p2p/src/lib.rs:46-49`) has no callers.

---

## 5. Corrections to the starting symbol list

The starting list was "15 core-crate symbols antd uses that ant-ffi never touches". At `75d7328`, ant-ffi **does** use seven of them (grep: `grep -rnE '<symbol>' crates/ant-ffi/src`):

| Symbol | Used in ant-ffi at |
|---|---|
| `chequebook_store::auto_deploy_chequebook` | `drive:1818` (imported as `chequebook_store::`, so a full-path grep misses it) |
| `ChequebookFile::rediscovered` | `drive:1767` |
| `chequebook_store::persist_chequebook` | `drive:1765` |
| `ControlCommand::RegisterBatch` | `drive:567` |
| `StampIssuer::open_existing` | `drive:90` |
| `DiskChunkCache::open` | `ffi:771` |
| `discover::discover_owned_batches` (via `DiscoveredBatch` values) | `drive:525` (host-driven only) |

These are truly unused by ant-ffi:

- **Chequebook checks:** `load_persisted_chequebook` (ffi deliberately uses `_for`), `read_chequebook_issuer`, `verify_chequebook_with_factory`. These are F4.
- **Control socket:** `ant_control::{bind, BoundControlSocket, SOCKET_POINTER_FILE}`. n/a on mobile.
- **bee stamperstore recovery:** `ant_postage::beestore::recover_bee_buckets`, `StampIssuer::open_or_new_seeded`. n/a on mobile (S17).
- **Type name:** `DiscoveredBatch` as a named type.

The two iOS-relevant gaps named in the starting list, verification and startup rediscovery, are confirmed. F3 (settlement enable) is added to them.

---

## 6. Questions for the iOS side (answered 2026-09-29 unless marked open)

1. **Which storage calls does the app make?** It buys only through the gateway (`POST /stamps/{amount}/{depth}`, `PATCH /stamps/topup|dilute`). It never calls `ant_storage_buy*` or `ant_storage_connect_batch`. It calls `ant_deploy_chequebook` at every light-mode start (after `ant_init`, before `ant_start_gateway`, best-effort). On an in-progress branch it also calls `ant_storage_discover` once after the gateway is up. Effect: F3 → Medium, F5 is the iOS-blocking item for variant (a), and F2 is host-covered for now.
2. **Does freedom-mobile-ffi compose `ant_ffi::log_layer()`?** **Open.** The released aggregator (v0.12.3 = ant v0.5.47) exposes `freedom_mobile_init_logging`; whether that composes `log_layer()` is unverified. The local checkout at `freedom-mobile-ffi` is stale (it pins ant-ffi `v0.5.43` and has no logging code), so it can't answer this.
3. **What is the `RLIMIT_NOFILE` soft limit?** **Open.** The simulator inherits macOS's process default (soft limit 256 unless raised). A real device is unmeasured. A one-line `getrlimit` in the app's node log would answer it on the next device run.
4. **Is `ant_set_chain_transport` used?** Yes, on branch `feat/ant-chain-transport`. It is installed right after `ant_init`, before the chequebook step and `ant_start_gateway`. So every ant-ffi chain client goes through the app's router, via `AntHandle::chain_client`: the #97 check, `ant_storage_discover`, `ant_deploy_chequebook`, and the gateway's chain context.

---

## 7. Guard proposal

### 7.1 Repo rule (AGENTS.md)

> **One orchestration, two sequencers.** Any startup or chain-init step that both `antd` and `ant-ffi` need is a named `pub` helper in a shared crate. Today those modules are `ant_chain::discover`, `ant_chain::chequebook_store`, and the future `ant-node` chain-init module. `crates/antd/src/main.rs` and `crates/ant-ffi/src/` may only *sequence* those helpers, not re-implement their decisions (for example what counts as a dead batch, or when to auto-deploy). A fix to such a decision goes into the helper so both entry points get it. If a step genuinely applies to one entry point only, list it with a reason in the parity allowlist (7.2). `docs/ffi-parity-audit.md` is the reference matrix; update it when you add or change an orchestration step.

### 7.2 Automated check (runs in the existing gate, step 5)

A `#[test]` in `ant-ffi`'s lib tests, so it runs under `cargo test --workspace --lib` with no new CI step. antd is a binary crate, so its tests don't run there.

1. **Enumerate** every `pub fn` / `pub async fn` name in a fixed list of *orchestration modules*: `crates/ant-chain/src/discover.rs`, `crates/ant-chain/src/chequebook_store.rs`, and later the F1 module. It reads them with `include_str!` and a small regex over the source text; there's no parser dependency.
2. **Count** word-boundary references to each name in `include_str!("../../antd/src/main.rs")` and in ant-ffi's own `src/*.rs`, excluding `#[cfg(test)]` modules by cutting at `mod tests`.
3. **Fail** when a helper is referenced by exactly one entry point and isn't in a `PARITY_ALLOWLIST: &[(&str /*fn*/, &str /*side*/, &str /*reason*/)]`. Also fail on a stale allowlist entry (a helper now used by both sides, or no longer existing), so the list can't rot.
4. The failure message points at this document and the rule in 7.1.

**Allowlist at `main` + #97:**

| Helper | Side | Reason |
|---|---|---|
| `load_persisted_chequebook` | antd | to be replaced by `_for` (reverse gap) |
| `verify_chequebook_with_factory` | antd | F4 open |
| `read_chequebook_issuer` | antd | F4 open |

`recover_bee_buckets` lives in `ant-postage::beestore`, which isn't in the list; if it's added later, it gets an allowlist entry "bee data-dir migration, desktop only".

**Limits:**

- A grep-level check can't tell "called at startup" from "called behind a host FFI". `discover_owned_batches` passes today even though iOS never reaches it (S16).
- It only guards helpers that exist. The rule in 7.1 is what forces new orchestration into those modules, and review has to enforce that rule.
- #49 itself would not have been caught: both sides called `fetch_postage_batch_meta` for different purposes. The same change made after #97, with `verify_persisted_batch` in `discover.rs`, would be caught.

The check costs about 80 lines, and the allowlist update is part of any PR that adds an orchestration helper.

### 7.3 Optional stronger variant

Once F1 exists, antd and ant-ffi both call **one** chain-init function whose result type forces every field to be handled: registered issuers, dropped issuers with reasons, and a chequebook verdict. Parity then comes from the type system rather than grep. The grep check can stay as a backstop for helpers outside that function.
