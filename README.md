# Ant

Ant is a lightweight Swarm light node written in Rust, aimed at mobile and
single-binary desktop/server deployments.

**Project status:** both the ultra-light **read path (M2)** and the bulk
of the **upload path (M3)** are live and deployed — `antd` joins Swarm
mainnet, holds a stable ~100-peer warm set, serves bee-compatible
downloads over a local HTTP gateway, and now signs postage stamps,
pushes chunks via the bee-compatible pushsync protocol, builds
Mantaray manifests for multi-file uploads, and speaks the
`/swarm/swap/1.0.0/swap` cheque protocol. On-chain postage batch
management (create / top-up / dilute) is exposed through `antctl
postage`. The **mobile artefact** and a handful of M3 polish items
(automatic SWAP settlement triggers, live-mainnet outbound-cheque
interop, Reed-Solomon recovery) are still outstanding. Wire formats,
CLI flags, and on-disk files may still change.

See [`PLAN.md`](PLAN.md) for the full design; §9.0 "Current status" tracks
what's shipped, and the milestone-summary table at the end of §9 names
the still-open items.

## What works today

### Mainnet peering & read path (M1, M2)

- **Mainnet peering.** libp2p (TCP, DNS, Noise, Yamux, identify, ping,
  streams), outbound BZZ Swarm handshake on `/swarm/handshake/15.0.0`
  with a `14.0.0` fallback for bee 2.7.x peers (see PLAN.md Appendix I
  for the bee-2.8 cutover work), hive peer exchange on both
  `/swarm/hive/2.0.0/peers` and `/swarm/hive/1.1.0/peers`,
  forwarding-Kademlia routing bins. Bootstraps from
  `/dnsaddr/mainnet.ethswarm.org` by default; warm-restart peer snapshot
  lands you back at the previous set in seconds. Handshake now also
  recovers each peer's Ethereum EOA from the BZZ address signature so
  SWAP cheques can target the right beneficiary (PLAN.md M3 Phase 7).
- **Retrieval.** Bee-parity fetcher with cancel-tolerant hedging
  (Appendix G), admission-control mirror of bee's per-peer accounting
  (Appendix H), and client-side pseudosettle (Appendix F) — peer set
  stays full during sustained large-file streaming.
- **Two-tier chunk cache.** In-memory LRU plus a SQLite-backed persistent
  disk cache (10 GB default, configurable, per-request bypass).
- **Mantaray walker.** `bzz://<root>[/path]` resolution, Swarm-feed
  dereferencing, `website-index-document` fallback.
- **Ordered streaming joiner** with single-range (`Range: bytes=a-b`,
  suffix, open-ended) and `HEAD` support — bodies don't materialise in
  memory.
- **Bee-shaped HTTP gateway.** `127.0.0.1:1633` by default, so
  unmodified `bee-js` / `curl` / browser proxies work. Tier-A read
  endpoints **and** the Tier-B upload endpoints (`POST /chunks`,
  `POST /bzz` for single files and tar collections) are wired up.
  Endpoint matrix in [`crates/ant-gateway/README.md`](crates/ant-gateway/README.md).

### Upload path (M3 — Phases 5, 6, 8, 9 ✅ ; Phase 7 mostly ✅)

- **Gnosis chain client (`ant-chain`).** JSON-RPC client against any
  HTTPS endpoint (Alchemy, Pocket, self-hosted Erigon). Reads native
  xDAI balance, ERC-20 (BZZ) balance, PostageStamp batch state, and
  SimpleSwap chequebook state. Writes via signed EIP-155 legacy
  transactions: `approve` (BZZ), `createBatch`, `topUp`,
  `increaseDepth` (PostageStamp), and `cashChequeBeneficiary`
  (Chequebook).
- **Postage stamps (`ant-postage`).** On-device `StampIssuer` with
  bucket counters, EIP-191 stamp signing per chunk, and crash-safe
  JSON persistence (atomic `write tmp → fsync → rename`) — counters
  survive `antd` restarts and crashes mid-upload.
- **Pushsync (`ant-retrieval::pushsync`).** Client side of
  `/swarm/pushsync/1.3.1/pushsync`: stamp + push to the closest BZZ
  peer, parse the receipt, recover the storer signature.
- **Splitter + manifest writer (`ant-retrieval`).** Streaming
  bytes-to-chunks splitter (the inverse of the joiner) and a recursive
  Mantaray v0.2 writer that builds a proper radix trie. Multi-chunk
  manifests are emitted as a tree of CAC chunks; long path segments
  (>30 bytes) are chained transparently.
- **SWAP wire protocol (`/swarm/swap/1.0.0/swap`).** Full inbound
  listener with EIP-712 cheque verification, monotonicity enforcement,
  sticky issuer-EOA pinning, and crash-safe credit ledger. Outbound
  cheques for downloads and uploads alike, priced like bee's
  (`units × exchange + deduction`, capped by the price oracle and the
  chequebook's available balance) and switched by `swap-enable`
  (`OutboundLedger` + `PushsyncSwap`). As a light node `antd` is a net
  debtor — it issues outbound cheques for chunks it consumes; the
  inbound listener exists for symmetry + forward compatibility
  (PLAN.md M3 Phase 7).
- **Operator tooling — postage on-chain.** `antctl postage` talks
  directly to Gnosis (no daemon required) and exposes:
  - `antctl postage create   --depth N --amount-per-chunk W` — submits `createBatch(...)`.
  - `antctl postage top-up   --batch-id 0x… --amount W`     — submits `topUp(...)`.
  - `antctl postage dilute   --batch-id 0x… --new-depth N`  — submits `increaseDepth(...)`.
  - `antctl postage show     --batch-id 0x…`                — reads on-chain batch state.
  - `antctl postage balance  [--address 0x…]`               — reads xDAI + BZZ balance for the wallet (or any address).
- **Operator tooling — chequebook on-chain.** `antctl chequebook`
  also talks directly to Gnosis (no daemon required) and exposes:
  - `antctl chequebook show`                                — reads `issuer / balance / totalPaidOut / liquidBalance / paidOut(beneficiary)`.
  - `antctl chequebook cash-self [--submit]`                — Tier-1 SWAP self-test: signs a 1-PLUR EIP-712 cheque against `CHEQUEBOOK_ADDRESS`, eth_call's `cashChequeBeneficiary` from our own EOA (free), and with `--submit` actually broadcasts the tx (~$0.0001 in xDAI). Proves bit-exact bee EIP-712 / ECDSA compatibility — first live run on Gnosis mainnet at block `46019699`, tx `0x643bb08e…e308`.
- **Operator tooling — local.** `antctl status`, `antop` (live TUI
  with Peers / Routing / Retrieval / Gateway / Requests tabs), `antctl
  version`, `antctl peers reset`, `antctl get` for downloads with
  live progress, and `antctl pullsync` to probe the pullsync-client
  path against a live peer.

### Messaging (PSS + GSOC — send *and* receive on a light node)

- **PSS send (`/pss/send/{topic}/{targets}`).** Bee-compatible trojan
  chunks (ElGamal ECDH, keccak-CTR, nonce mining with a precomputed
  BMT sibling path), stamped and pushed from the light node.
  Interop locked by bee-generated golden vectors.
- **GSOC authoring.** bee-js-compatible `gsocMine` key mining +
  EIP-191 SOC signing (`ant-crypto::gsoc`), uploaded via `POST /soc`.
- **Receive — the lurker (`ant-p2p::lurker`).** Bee has no push
  delivery to light nodes, so ant *pulls*: the node drives bee's
  pullsync 1.4.0 protocol against peers covering the target
  neighborhood and decodes watched GSOC/PSS chunks locally. One
  shared pull pipeline per neighborhood fans out to all subscribers
  (`lurker_registry`), with admission caps, keep-alive'd WebSockets
  (`/gsoc/subscribe/{address}`, `/pss/subscribe/{topic}`), staleness
  rotation of dead peers, and at-least-once delivery semantics.
  Verified end-to-end on mainnet, including a symmetric many-to-many
  rendezvous between two light nodes.

## What's not here yet

- **Mobile artefact.** The hand-written `extern "C"` / JNI surface in
  [`ant-ffi`](crates/ant-ffi) has grown well past the original smoke
  test — downloads, streaming, uploads, storage plans, account info —
  and is exercised by three example apps
  ([`examples/ios-download/`](examples/ios-download/),
  [`examples/ios-drive/`](examples/ios-drive/),
  [`examples/android-download/`](examples/android-download/)). But
  there is no *releasable* artefact yet: the UniFFI `.xcframework` /
  Kotlin `.aar` that meets the Phase-4 size budget is not built, and
  the interim `AntFFI.xcframework` packaging track (PLAN.md § "Interim
  releasable iOS artifact") is planned but not started.
- **Retrieval-side SWAP settlement.** Upload-side settlement is
  done: the pushsync dialer signs + emits cheques when outbound debt
  crosses the payment threshold and drives bee's pseudosettle
  refreshments alongside (PLAN.md Phases 7b/7d — proven by a
  sustained 256 MiB / 66 K-chunk mainnet upload). The *retrieval*
  path still relies on pseudosettle alone (fine in light mode); a
  cheque-emitting hook in `ant-retrieval::accounting` for routine
  pay-as-you-fetch remains on the to-do list.
- **Live `cheque_smoke` re-run with the corrected EIP-712 digest.**
  Tier-1 (`antctl chequebook cash-self --submit`, see above) caught a
  real bug in our EIP-712 implementation — bee's chequebook domain is
  `EIP712Domain(string name,string version,uint256 chainId)`
  (3 fields, no `verifyingContract`), but our digest was using the
  4-field OpenZeppelin shape. Fixed in
  `ant-chain::chequebook::eip712_domain_separator`, proven on-chain at
  block `46019699`. The previous `cheque_smoke` "cold-peer rejection"
  diagnosis assumed the wire format was correct, so it's now in
  question — bee may have just been rejecting our (then-broken)
  digest. Re-running `cargo run -p ant-p2p --example cheque_smoke`
  against a mainnet bee bootnode is the natural next step; the
  outcome decides whether Phase 7 is essentially closed or whether we
  still need the auto-settlement trigger below.
- **No automatic chequebook deploy.** `deployChequebook` calldata is
  in `ant-chain` and exercised in unit tests, but
  `antctl chequebook deploy` is not wired up yet — operators
  provision the chequebook out of band (factory deploy from `bee` or
  directly from a wallet). `cash` shipped as `antctl chequebook
  cash-self` (see above).
- **Reed-Solomon recovery.** Files uploaded with redundancy can be
  read today only via `--allow-degraded-redundancy`, and only if every
  data chunk is reachable.
- **Streaming `antctl get`.** `antctl get` streams *progress* but the
  body still arrives as one terminal response over the control
  socket; browsers hitting the HTTP gateway get true byte streaming
  (Appendix E Phase 5 is the last read-path gap).
- **HTTP-API drop-in coverage.** The gateway covers bee's read path
  end-to-end and the upload primitives (`POST /chunks`,
  `POST /bzz`); the long tail bee-js touches —
  `/wallet` / `/balances` / `/settlements` / `/chequebook/*` reads,
  `/stamps` reads + writes, `/tags`, `/feeds`, `/soc`,
  `/stewardship`, `/envelope` — still answers `501`. The
  prioritised order to close that gap lives in
  [`PLAN.md` §9.0 "Drop-in light-node gap"](PLAN.md), and the
  per-endpoint matrix is in
  [`crates/ant-gateway/README.md`](crates/ant-gateway/README.md).

## Known issues

- **Large-file uploads stall at ~250 MiB+ on bee 2.8 mainnet.** The
  pushsync layer caps each chunk at 5 retries / 60 s wall. On the
  current bee 2.8 storer fleet at least one storer is slow enough
  per ~25 K chunks to exhaust that cap, so a 500 MiB
  `antctl upload start` reliably ends with
  `push chunk failed: exhausted 5 retries: push timed out after 60s`.
  We've reproduced the same failure mode on a stock bee 2.8.0
  client, so this is not an ant regression — it's the network
  catching up after the 2.7 → 2.8 cutover. Workarounds: chunk uploads
  client-side into ≤200 MiB ranges and stitch them with mantaray
  manifests, or wait for the storer fleet to age out. Tracking in
  [`PLAN.md` Appendix I.6](PLAN.md).
- **Push-vs-pull race on freshly-uploaded refs.** `antctl upload`
  returns `completed` once the local push pipeline drains, but the
  last 1-2 chunks per MiB often only land in their kademlia-closest
  storer one replication round later. A cold `antctl get` inside
  that ~60 s window can return `retrieve chunk: no peer found` /
  `storage: not found`. Workaround: re-upload the same file (it's
  content-addressed, so the reference doesn't change) or wait 60 s
  before publishing the reference. Bee 2.8 upstream, not an ant bug;
  details in [`PLAN.md` Appendix I.7](PLAN.md).
- **`ant-gateway` SOC retrieval has two stale test-asserts** for
  error-message wording (`post_soc_rejects_malformed_signature_hex`,
  `post_soc_rejects_missing_signature_header`). Pre-existing on
  `origin/main`, unrelated to the bee-2.8 cutover, surfacing in
  `cargo test --workspace`. Production HTTP behaviour is correct;
  only the test strings drifted.

## Workspace

| Crate | Purpose |
|---|---|
| [`antd`](crates/antd) | Daemon binary: CLI, config, signal handling, systemd/launchd shell. Loads the persistent `StampIssuer` and the SWAP credit ledger. |
| [`antctl`](crates/antctl) | Local control client: status, top TUI, `get`, `peers reset`, `postage` (on-chain create / top-up / dilute / show / balance), `chequebook` (on-chain show / cash-self). |
| [`ant-node`](crates/ant-node) | High-level orchestrator wiring `ant-p2p`, `ant-retrieval`, caches, gateway, `UploadRuntime`, and `SwapConfig`. |
| [`ant-p2p`](crates/ant-p2p) | libp2p host, BZZ handshake (now exposing peer EOA + welcome), Hive peer exchange, pseudosettle driver, `/swarm/swap/1.0.0/swap` listener + dialer with `CreditLedger` + `OutboundLedger`. |
| [`ant-retrieval`](crates/ant-retrieval) | Fetcher, ordered joiner with ranges, mantaray walker + recursive trie writer, splitter, pushsync client, in-memory + SQLite caches, bee-parity accounting mirror. |
| [`ant-gateway`](crates/ant-gateway) | Bee-shaped HTTP API (`/bytes`, `/bzz`, `/chunks`, HEAD, Range, plus Tier-A status / stub endpoints, plus `POST /chunks` and `POST /bzz` upload endpoints, plus PSS/GSOC messaging: `/pss/send` and the `/gsoc/subscribe` + `/pss/subscribe` WebSockets). |
| [`ant-control`](crates/ant-control) | Wire types for the `antd` ↔ `antctl` Unix-socket control protocol. |
| [`ant-crypto`](crates/ant-crypto) | secp256k1, keccak256, BMT, CAC/SOC validation, overlay derivation. |
| [`ant-postage`](crates/ant-postage) | EIP-191 stamp issuer with crash-safe bucket-counter persistence. |
| [`ant-chain`](crates/ant-chain) | Gnosis JSON-RPC client + ABI calldata builders + EIP-155 transaction signer; ERC-20, PostageStamp, SimpleSwap chequebook. |
| [`ant-ffi`](crates/ant-ffi) | Hand-written `extern "C"` + JNI surface (`ant.h`) embedding the full light-node in mobile apps: downloads, streaming, uploads, storage plans, account (see its [README](crates/ant-ffi/README.md)). Pre-UniFFI — superseded by the Phase-4 artefact when it lands. |

Current workspace version: **`0.5.16`** across all crates (the deployment
bump on each release to `vibing.at/ant`; see `AGENTS.md`).

## Requirements

- Rust stable (pinned in `rust-toolchain.toml`).
- Cargo.

## Development

```sh
cargo fmt --all
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Release binary:

```sh
cargo build --release -p antd
./target/release/antd --version
```

## Running `antd`

Start a mainnet light-node daemon with defaults:

```sh
cargo run -p antd
```

By default `antd`:

- stores identity, peer snapshot, and the persistent chunk cache under
  `~/.antd`,
- listens for `antctl` on `~/.antd/antd.sock`,
- serves the bee-shaped HTTP gateway on `127.0.0.1:1633`,
- bootstraps peers from `/dnsaddr/mainnet.ethswarm.org`.

Common options:

```sh
cargo run -p antd -- \
  --data-dir ~/.antd \
  --log-level info \
  --external-address /ip4/<public-ip>/tcp/<public-port> \
  --api-addr 127.0.0.1:1633 \
  --disk-cache-max-gb 10
```

- `--external-address` advertises a publicly routable multiaddr via
  libp2p-identify. Strongly recommended behind NAT — without it, bee
  bootnodes stall the handshake by ~10 s. `antd` also tries to discover
  one automatically via UPnP/IGD on the local home router; when that
  succeeds the flag is unnecessary, but corporate / cloud / CGNAT
  environments will need it set explicitly.
- `--no-http-api` disables the HTTP gateway for headless deployments.
- `--no-disk-cache` falls back to memory-only caching.
- `--no-peerstore` / `--reset-peerstore` control the warm-restart peer
  snapshot.

To enable uploads (`POST /chunks`, `POST /bzz`), point `antd` at a
funded postage batch:

```sh
antd \
  --gnosis-rpc-url      https://gnosis-mainnet.g.alchemy.com/v2/<key> \
  --postage-batch       0x<32-byte batch id> \
  --postage-owner-key   0x<32-byte secret>      # signs the stamps
```

The same values can be supplied via env vars (`GNOSIS_RPC_URL`,
`STORAGE_STAMP_BATCH_ID`, `STORAGE_STAMP_PRIVATE_KEY`) and are
read by both `antd` and the read-only `antctl postage` subcommands.
On boot `antd` cross-checks the configured signing key against the
on-chain `batchOwner(batch_id)` and refuses to start if they
disagree, then opens a persistent `StampIssuer` at
`<data_dir>/postage/<batch_id>.bin` so bucket counters survive
restarts. The SWAP credit ledger is always enabled and persists at
`<data_dir>/swap_credits.json`; the protocol is registered even
when uploads are off, so peers don't see a libp2p disconnect when
they emit cheques at us.

## Using `antctl`

```sh
# One-shot status and daemon version
cargo run -p antctl -- status
cargo run -p antctl -- version

# Live dashboard (peers, routing bins, retrieval, gateway, requests)
cargo run -p antctl -- top

# Drop the on-disk peer snapshot without killing current connections
cargo run -p antctl -- peers reset

# Download by chunk / bytes-tree / bzz-manifest reference
cargo run -p antctl -- get <32-byte-hex>                 # single chunk
cargo run -p antctl -- get bytes://<root>   -o blob.bin  # multi-chunk file
cargo run -p antctl -- get bzz://<root>/index.html       # manifest path
cargo run -p antctl -- get bzz://<root>/video.mp4 -o vid.mp4
```

`antctl get` shows a live progress line by default (chunks, bytes, rate,
peers); use `--progress-style visual` for a retro block-map renderer, or
`--no-progress` to silence it.

### Postage on Gnosis (`antctl postage`)

Postage subcommands talk **directly** to a Gnosis JSON-RPC endpoint —
`antd` does not need to be running. Connection params come from CLI
flags or environment variables (recommended: keep them in a `.env`):

```sh
export GNOSIS_RPC_URL=https://gnosis-mainnet.g.alchemy.com/v2/<key>
export POSTAGE_OWNER_PRIVATE_KEY=0x…   # signs createBatch / topUp / increaseDepth
export POSTAGE_STAMP_ADDRESS=0x…       # mainnet PostageStamp contract
export BZZ_TOKEN_ADDRESS=0x…           # mainnet BZZ ERC-20

# Read-only: how much xDAI + BZZ does the wallet hold?
antctl postage balance

# Buy a depth-22 batch at a given amount-per-chunk (in PLUR / BZZ wei).
# Approves the BZZ allowance first if needed, then submits createBatch.
antctl postage create --depth 22 --amount-per-chunk 414720

# Refill an existing batch (extends its TTL).
antctl postage top-up --batch-id 0x<32-byte-batch-id> --amount 414720

# Dilute (increase depth → halve effective amount-per-chunk → larger batch).
antctl postage dilute --batch-id 0x<32-byte-batch-id> --new-depth 23

# Read on-chain state for a batch you know the id of.
antctl postage show --batch-id 0x<32-byte-batch-id>
```

All write commands print the unsigned tx, the gas estimate, the signed
tx, the tx hash, and finally the receipt with block number + status.

### Chequebook on Gnosis (`antctl chequebook`)

Same shape as the postage commands — talks directly to Gnosis, no
daemon needed. Read-only `show` is free; `cash-self` is the Tier-1
SWAP self-test that proves our EIP-712 cheque format is bee
bit-compatible by round-tripping a 1-PLUR cheque through the
chequebook contract on-chain.

```sh
export GNOSIS_RPC_URL=https://gnosis-mainnet.g.alchemy.com/v2/<key>
export CHEQUEBOOK_ADDRESS=0x…              # the chequebook proxy
export STORAGE_STAMP_PRIVATE_KEY=0x…       # signs cheques (== chequebook.issuer())
export WALLET_PRIVATE_KEY=0x…              # beneficiary; sends the on-chain tx
export WALLET_ADDRESS=0x…                  # beneficiary EOA (derived from above)

# Read-only snapshot.
antctl chequebook show

# Free dry-run: signs a 1-PLUR cheque and eth_call-s
# cashChequeBeneficiary from WALLET_ADDRESS. PASS = the chequebook
# contract recovered our issuer; FAIL = digest mismatch.
antctl chequebook cash-self

# Same, but actually broadcast the tx (~$0.0001 in xDAI gas) — full
# on-chain state transition: paidOut[beneficiary] += 1 PLUR.
antctl chequebook cash-self --submit
```

The Tier-1 self-test caught a real EIP-712 bug for us — bee's
chequebook domain has only `name / version / chainId` fields (no
`verifyingContract`). The fix lives in
[`crates/ant-chain/src/chequebook.rs`](crates/ant-chain/src/chequebook.rs).
First passing live run on Swarm mainnet: tx
[`0x643bb08e…e308`](https://gnosisscan.io/tx/0x643bb08e4485042024604a8ac223a7ea89c594d1a22a27cbba01b4bc9982e308)
in block `46019699`.

## Using the HTTP gateway

The same commands work through `curl` against the running `antd`:

```sh
curl -fsS  http://127.0.0.1:1633/health
curl -fsS  http://127.0.0.1:1633/peers   | jq '.peers | length'
curl -fsSI http://127.0.0.1:1633/bzz/<root>/video.mp4
curl -fsSr 0-1048575 http://127.0.0.1:1633/bzz/<root>/video.mp4 -o head.bin
```

Uploading needs the `swarm-postage-batch-id` header (32-byte batch id
hex; create one with `antctl postage create`) and goes through the
bee-shaped Tier-B endpoints:

```sh
# Single chunk (raw 32-byte payload + span-prefixed wire bytes)
curl -fsS -X POST -H 'content-type: application/octet-stream' \
     -H "swarm-postage-batch-id: $BATCH" \
     --data-binary @chunk.bin \
     http://127.0.0.1:1633/chunks

# Single file as a Mantaray manifest
curl -fsS -X POST -H 'content-type: text/plain' \
     -H "swarm-postage-batch-id: $BATCH" \
     -H 'swarm-collection: false' \
     -H 'swarm-index-document: hello.txt' \
     --data-binary @hello.txt \
     'http://127.0.0.1:1633/bzz?name=hello.txt'

# Directory as a tar collection
tar -cf site.tar -C site .
curl -fsS -X POST -H 'content-type: application/x-tar' \
     -H "swarm-postage-batch-id: $BATCH" \
     -H 'swarm-collection: true' \
     -H 'swarm-index-document: index.html' \
     --data-binary @site.tar \
     http://127.0.0.1:1633/bzz
```

The response is bee-shaped: `201 Created` with `{"reference":"<hex>"}`.

Full endpoint matrix and compatibility notes are in
[`crates/ant-gateway/README.md`](crates/ant-gateway/README.md), and the
normative contract is in PLAN.md Appendix C.

## iOS smoke test

Throwaway SwiftUI app that embeds `ant-ffi` and lets you paste a
Swarm reference into a text field:

```sh
rustup target add aarch64-apple-ios-sim  # once per machine
cargo xtask build-ios-sim                # cross-compile libant_ffi.a
open examples/ios-download/AntDownload.xcodeproj
```

Pick an iPhone simulator and ⌘R. See
[`examples/ios-download/README.md`](examples/ios-download/README.md)
for troubleshooting and the ant / app responsibility split. The
bigger sibling, [`examples/ios-drive/`](examples/ios-drive/)
(AntDrive), exercises the full `ant-ffi` surface: uploads, storage
plans, and on-chain plan purchase. For embedding `ant-ffi` in your
own iOS app, start with
[`crates/ant-ffi/README.md`](crates/ant-ffi/README.md).

## Further reading

- [`PLAN.md`](PLAN.md) — full implementation plan, milestones, and
  engineering appendices (E: streaming gateway, F: pseudosettle,
  G: cancel-tolerant hedging, H: admission-control mirror).
- [`AGENTS.md`](AGENTS.md) — repo-level conventions (versioning policy,
  deployment notes).

## License

Licensed under either of

- Apache License, Version 2.0 ([`LICENSE-APACHE`](LICENSE-APACHE) or
  <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([`LICENSE-MIT`](LICENSE-MIT) or
  <http://opensource.org/licenses/MIT>)

at your option. This permissive dual-license is the Rust-ecosystem norm
and lets `ant` be embedded freely in other applications — including
[Freedom](https://freedom.baby) and any other open-source or proprietary
app — without source-disclosure obligations.

Unless you explicitly state otherwise, any contribution intentionally
submitted for inclusion in the work by you, as defined in the Apache-2.0
license, shall be dual licensed as above, without any additional terms or
conditions.
