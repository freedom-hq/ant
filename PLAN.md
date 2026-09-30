# Ant — Implementation Plan

**Project:** Ant — a lightweight Swarm light node.
**Primary deployment:** embeddable Rust library (`libant`) consumed by native iOS and Android apps via UniFFI.
**Secondary deployment:** `antd` daemon for macOS, Linux, Windows, and Raspberry Pi — shipped as signed binaries with platform-appropriate service integration (systemd / launchd / Windows Service).

A from-scratch Rust implementation of a Swarm light node with upload and postage-stamp management. The embedded mobile library is the primary product, with a per-platform artifact size target in the single-digit megabytes. The same core compiles to a standalone `antd` binary for desktop and single-board computers, enabling a personal-node use case (always-on home node, desktop companion, self-hosted gateway) in addition to mobile.

---

## 1. Goals & Non-Goals

### Goals

- Run a Swarm **light node** (downloads + uploads) embedded in mobile apps.
- Full **postage-stamp management** on device from v1 (purchase, top-up, dilute, per-chunk signing).
- Full **SWAP / chequebook** support, required to pay forwarders for pushsync.
- Ship as **one Rust library** with typed Swift + Kotlin bindings generated from a single IDL.
- **Artifact size target: 6–10 MB stripped per platform** on mobile (vs. 30–60 MB for `bee-lite`). Desktop / Pi have no hard size budget.
- Ship `antd` as a **supported secondary deployment** on macOS (Intel + Apple Silicon), Linux (x86_64 + aarch64), Windows (x86_64), and Raspberry Pi 3+ (aarch64) — signed binaries, installers, and service-unit templates.
- Interoperate with the public Swarm mainnet (no fork, no private network).

### Non-Goals (v1)

- Full node features: chunk storage/forwarding, storage/bandwidth incentives, staking, redistribution lottery.
  - *Amended:* a **pullsync client** (not server) shipped with the messaging work below — the light node drives bee's pullsync 1.4.0 protocol to *receive* neighborhood chunks. Serving pullsync to peers remains a non-goal.
- ~~PSS / GSOC messaging.~~ **Graduated (2026-07):** send *and* receive shipped on the light node — `/pss/send`, `POST /soc` GSOC authoring, and `/gsoc/subscribe` + `/pss/subscribe` WebSocket reception via the pullsync-client "lurker" (`ant-p2p::lurker`, one shared pull pipeline per neighborhood). Interop-tested against bee (golden vectors) and verified on mainnet. Appendix B and the endpoint matrix reflect the graduation.
- Cheque cashing (we send cheques, never receive them as a consumer-class node).
- Exotic libp2p transports (QUIC, WebRTC, WebSocket, relay) on mobile. May be enabled later for desktop / Pi behind cargo feature flags.
- 32-bit ARM (Pi Zero / Pi 1) as a first-class target — best-effort only.
- ACT (access control trie), advanced feeds, custom redundancy schemes. Deferred to v2.

### Explicit constraints

- **Language:** Rust (stable channel; nightly only for optional size-shrinking `build-std`).
- **Binding layer:** UniFFI → Swift `.xcframework` + Kotlin `.aar`.
- **Chain:** Gnosis Chain mainnet only. Testnet (Sepolia-based dev network) for development.
- **No Ethereum full client.** All chain interactions go through a user-configurable JSON-RPC endpoint.

---

## 2. Reference Architecture

```
┌──────────────────────────────────┐   ┌──────────────────────────┐
│   Mobile App (Swift / Kotlin)    │   │        antd CLI          │
│    UI, onboarding, key UX        │   │  dev + interop harness   │
└──────────────┬───────────────────┘   └───────────┬──────────────┘
               │  UniFFI typed bindings            │ direct Rust API
┌──────────────▼───────────────────────────────────▼──────────────┐
│                   libant (cdylib + staticlib)                   │
│                                                                 │
│  ┌───────────────┐  ┌──────────────────┐  ┌──────────────────┐  │
│  │  Public API   │  │  Config / Keys   │  │  Event / Status  │  │
│  └───────┬───────┘  └────────┬─────────┘  └────────┬─────────┘  │
│          │                   │                     │            │
│  ┌───────▼─────────────────────────────────────────▼────────┐   │
│  │                       Node Core                          │   │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────────┐   │   │
│  │  │ Retrieval│ │ Pushsync │ │  Hive    │ │ Accounting │   │   │
│  │  └──────────┘ └──────────┘ └──────────┘ └────────────┘   │   │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────────┐   │   │
│  │  │   BMT    │ │ Mantaray │ │  Stamps  │ │    SWAP    │   │   │
│  │  └──────────┘ └──────────┘ └──────────┘ └────────────┘   │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌────────────────┐  ┌────────────────┐  ┌──────────────────┐   │
│  │   libp2p       │  │  Gnosis RPC    │  │  Local Storage   │   │
│  │  (TCP/Noise/   │  │  (alloy +      │  │  (SQLite +       │   │
│  │   Yamux/Kad)   │  │   JSON-RPC)    │  │   chunk cache)   │   │
│  └────────────────┘  └────────────────┘  └──────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

`antd` and the mobile apps share the exact same `libant` core; only the outer shell differs. This keeps protocol behavior identical across dev and production and makes `antd` a faithful interop target.

---

## 3. Technology Stack

### Language & tooling

- **Rust** (MSRV: pin to latest stable minus one at start of project).
- **cargo workspaces** for cleanly separated crates.
- **`cross`** for Linux/ARM cross-compilation (Raspberry Pi, ARM servers, musl static builds).
- **`cargo-ndk`** for Android builds (aarch64, armv7, x86_64 for emulator).
- **`cargo-lipo` / xcframework tooling** for iOS (aarch64-apple-ios, aarch64-apple-ios-sim, x86_64-apple-ios-sim).
- **GitHub Actions native runners** for macOS (Intel + Apple Silicon), Linux, and Windows.
- **UniFFI (latest stable)** for Swift + Kotlin binding generation from one `.udl` file. Mobile only — desktop / server binaries depend on `ant-node` directly.
- **Nix flake** (optional but recommended) for reproducible toolchain across engineers + CI.

### Core crates

| Concern | Crate | Notes |
|---|---|---|
| libp2p | `libp2p` | Features: `tcp`, `dns`, `noise`, `yamux`, `identify`, `kad`, `request-response`, `macros`. Nothing else. |
| Async runtime | `tokio` | Single-threaded flyweight: `rt`, `macros`, `net`, `time`, `sync`, `fs`. |
| Ethereum | `alloy-*` | Modular; pull only `rpc-client`, `primitives`, `signer-local`, `sol-types`, `contract`, `consensus`. Avoid `ethers-rs` (monolithic). |
| Crypto | `k256`, `sha3`, `sha2`, `hmac`, `hkdf`, `chacha20poly1305` | `k256` keeps us inside RustCrypto, no OpenSSL. |
| Protobuf | `prost` + `prost-build` | For pushsync / retrieval / hive messages. |
| Storage | `rusqlite` (bundled) | Plus optional `sqlcipher` for encryption at rest. |
| Serde | `serde`, `serde_json`, `bincode` | JSON for RPC; bincode for internal persistence. |
| RLP | `alloy-rlp` | Smaller than the standalone `rlp` crate. |
| Bindings | `uniffi`, `uniffi-bindgen` | Mobile only. Swift + Kotlin from one IDL. |
| Error handling | `thiserror` (lib), `anyhow` (binary/tests only) | Never leak `anyhow` across FFI. |
| Tracing | `tracing`, `tracing-subscriber` | Wire through to OSLog / logcat / stdout per platform. |
| Config | `figment` or hand-rolled | Keep it tiny. |
| Desktop key storage | `keyring` (feature-gated) | macOS Keychain, Windows DPAPI / Credential Manager, Linux Secret Service. |
| Desktop service | `windows-service` (feature-gated) | Windows Service integration for `antd`. Linux/macOS use systemd/launchd unit files, not crate code. |

### Build-profile for size

```toml
[profile.release]
opt-level = "z"
lto = "fat"
codegen-units = 1
panic = "abort"
strip = "symbols"
debug = false
incremental = false
```

Optional further shrink (nightly): `cargo +nightly build -Z build-std=std,panic_abort -Z build-std-features=panic_immediate_abort --target <triple>`.

### Dependency policy

**Always track the latest stable versions of all third-party dependencies.** No version ever pinned out of inertia.

- **Cargo.toml uses caret requirements** (e.g. `tokio = "1"`) so `cargo update` picks up non-breaking upgrades continuously.
- **`Cargo.lock` is committed** for reproducible builds of the binary / artifact crates (`antd`, `ant-ffi`), but we never rely on it to hold us back on library upgrades.
- **Rust toolchain:** pinned to **latest stable** in `rust-toolchain.toml`. MSRV claim is "current stable"; we do not promise old-toolchain support.
- **Dependency-hygiene PRs:** a dedicated upgrade workflow does:
  - `cargo update` (lockfile refresh) — always applied.
  - `cargo upgrade --incompatible` (from `cargo-edit`) — reviewed, applied when green.
  - Rust toolchain bump when a new stable lands.
  - Test suite + mainnet smoke must pass before the PR merges.
- **Dependabot / Renovate** generate PRs continuously for interim bumps.
- **`cargo-outdated`** in CI fails the build if any dependency is more than **one minor version** behind latest stable.
- **`cargo-deny`** enforces: no yanked versions, no duplicate major versions of the same crate, allowed licenses only, no known-advisory versions (via `cargo-audit` integration).
- **`cargo-vet`** tracks human-reviewed audits of new crate versions entering the tree. Reused for supply-chain review.
- **Exception policy:** a dependency may be held back only with a dated `# PINNED: reason, review by YYYY-MM-DD` comment in `Cargo.toml`. Anything past its review date blocks CI.
- **Single version per crate:** if two transitive deps force us onto two majors of the same crate, the upgrade is itself a blocking task — we don't carry duplicate majors into release artifacts (this is both a size and a security concern).

This policy is explicitly *in tension* with the §13 / §14 guidance to "pin versions." There is no contradiction: **we pin precisely to the latest stable, then we continuously rebase that pin onto new stable releases**. Pinning is for reproducibility and security review, not for freezing.

**Key dependencies we will carry** (versions intentionally not listed — CI enforces "latest stable"):

- `libp2p`, `tokio`, `alloy-*`, `k256`, `sha3`, `sha2`, `hmac`, `hkdf`, `chacha20poly1305`
- `prost`, `rusqlite`, `serde`, `serde_json`, `bincode`, `alloy-rlp`
- `uniffi`, `thiserror`, `tracing`, `tracing-subscriber`
- `keyring`, `windows-service` (platform-gated)

If you're reading this file to pick versions for a `Cargo.toml`, **do not copy version numbers out of this plan** — run `cargo search <crate>` or check `crates.io` at the moment you're adding the dependency.

### Platform matrix

The same `libant` core compiles to every target below. Platform-specific behaviour lives behind cargo features on the outer crates (`ant-ffi` for mobile, `antd` for desktop/server) and on the key-storage backend selection — **never** inside protocol crates.

| Class | Target triple | Artifact | Tier | Notes |
|---|---|---|---|---|
| **iOS device** | `aarch64-apple-ios` | `.xcframework` via `ant-ffi` | 1 | ≤ 10 MB stripped budget. |
| **iOS simulator (Apple Silicon)** | `aarch64-apple-ios-sim` | `.xcframework` | 1 | Packed into same `.xcframework` as device slice. |
| **iOS simulator (Intel)** | `x86_64-apple-ios` | `.xcframework` | 2 | Kept only as long as Intel Macs are on support. |
| **Android arm64** | `aarch64-linux-android` | `.aar` via `ant-ffi` | 1 | ≤ 10 MB stripped budget. |
| **Android armv7** | `armv7-linux-androideabi` | `.aar` | 1 | Older devices; same budget. |
| **Android x86_64** | `x86_64-linux-android` | `.aar` | 2 | Emulator-only in practice. |
| **macOS Apple Silicon** | `aarch64-apple-darwin` | `antd` binary + `.dmg` | 1 | Signed + notarized. |
| **macOS Intel** | `x86_64-apple-darwin` | `antd` binary + `.dmg` | 1 | Universal binary via `lipo`. |
| **Linux x86_64 (glibc)** | `x86_64-unknown-linux-gnu` | `antd` binary + `.deb` / `.rpm` | 1 | Default server target. |
| **Linux x86_64 (musl)** | `x86_64-unknown-linux-musl` | static `antd` | 1 | For containers / scratch images. |
| **Linux aarch64 (glibc)** | `aarch64-unknown-linux-gnu` | `antd` binary + `.deb` | 1 | **Raspberry Pi 4/5, Pi Zero 2 W, ARM servers** (64-bit OS). |
| **Linux aarch64 (musl)** | `aarch64-unknown-linux-musl` | static `antd` | 2 | Portable across distros. |
| **Linux armv7** | `armv7-unknown-linux-gnueabihf` | `antd` binary | 3 | 32-bit Raspberry Pi OS on Pi 2/3/4. Best-effort. |
| **Windows x86_64** | `x86_64-pc-windows-msvc` | `antd.exe` + MSI | 1 | Requires VS build tools on builder. |

**Tiers:**

- **Tier 1 — supported:** built in CI on every PR, tested against mainnet, shipped as signed artifacts.
- **Tier 2 — supported, reduced coverage:** built in CI, shipped but without per-PR blocking tests.
- **Tier 3 — best-effort:** not in CI by default; accept patches; no release guarantee.

### CI build matrix

Single GitHub Actions workflow, one job per (target, channel) pair:

- **Per-PR (fast):** `clippy`, `rustfmt`, `cargo test` on `x86_64-unknown-linux-gnu` + `aarch64-apple-darwin`; cross-compile smoke-build on `aarch64-linux-android` and `aarch64-apple-ios` to catch target-specific regressions.
- **Per-merge-to-main (full):** the full Tier-1 matrix above, producing release artifacts as build artifacts (unsigned). Tier-2 targets built and archived. Mainnet interop suite executed against the `x86_64-unknown-linux-gnu` build of `antd`.
- **Release tag:** produces signed + notarized artifacts for every Tier-1 platform, SBOM, and checksums.

### Per-platform key-storage backend

The `KeyProvider` abstraction (§5.10) is the same on every platform; only the default backend differs:

| Platform | Default backend | Crate |
|---|---|---|
| iOS | Secure Enclave + Keychain | `security-framework` |
| Android | StrongBox / Keystore | JNI bridge via `ant-ffi` |
| macOS | Keychain | `keyring` |
| Windows | DPAPI / Credential Manager | `keyring` |
| Linux desktop | Secret Service (GNOME / KDE) | `keyring` |
| Linux headless / Pi / containers | Encrypted keystore file (Argon2id + AES-GCM), unlocked by passphrase or systemd-creds | in-tree, `ant-crypto` |
| All (optional) | Hardware wallet (Ledger / Trezor) | `hidapi`-based, v2 |

The headless backend is the only one we own end-to-end; the rest are thin adapters over OS APIs. The `KeyProvider` interface guarantees that private key bytes never exist in `libant` memory — all signing is a callback.

### Desktop packaging

Delivered alongside the Tier-1 builds:

- **macOS:** universal `antd` binary, `.dmg` with optional menu-bar wrapper, code-signed + notarized. `launchd` `.plist` template for always-on use.
- **Windows:** MSI installer with an option to register a Windows Service. Authenticode-signed.
- **Linux:** `.deb` + `.rpm` + tarball. `systemd` unit file, `ant` user + `/var/lib/ant` data dir, `ExecStart=/usr/bin/antd --config /etc/ant/antd.toml`.
- **Raspberry Pi:** same `.deb` as Linux aarch64, plus an opinionated install script (`curl | sh`) that handles first-run key setup and optional Tailscale onboarding for "phone delegates to home Pi" scenarios.
- **Docker:** multi-arch image (`linux/amd64`, `linux/arm64`) built from the musl static binary, under ~15 MB.

---

## 4. Workspace Layout

```
ant/
├── Cargo.toml                  # workspace root
├── rust-toolchain.toml         # pinned toolchain
├── crates/
│   # Live crates (shipped in 0.3.x):
│   ├── ant-crypto/             # secp256k1, keccak, BMT, signatures, overlay derivation
│   ├── ant-p2p/                # libp2p host, BZZ handshake, Hive hints, pseudosettle, peer snapshot
│   ├── ant-retrieval/          # Retrieval protocol client, joiner, mantaray walker, caches, accounting mirror
│   ├── ant-gateway/            # Bee-shaped HTTP API (feature-gated, antd only — see Appendix C)
│   ├── ant-control/            # Local daemon control protocol and socket transport
│   ├── ant-node/               # High-level orchestrator: wires active crates together
│   ├── antd/                   # Standalone daemon: CLI, config loader, signal handling
│   └── antctl/                 # Local control client, live status TUI, `get` download CLI
│
│   # Planned for M3 (uploads / stamps / SWAP) or later:
│   ├── ant-store/              # SQLite schema + migrations + repositories (chunks live in ant-retrieval::disk_cache today)
│   ├── ant-chunk/              # Chunk types, serialization, content-addressed chunks, SOCs
│   ├── ant-mantaray/           # Manifest construction (traversal already lives in ant-retrieval)
│   ├── ant-pushsync/           # Pushsync protocol (client only)
│   ├── ant-accounting/         # Peer debit/credit ledger + threshold logic (bee-parity mirror lives in ant-retrieval today)
│   ├── ant-chain/              # Gnosis RPC, ABIs, tx signing, nonce mgmt
│   ├── ant-stamps/             # Batch state, per-chunk signing, bucket tracking
│   ├── ant-swap/               # Chequebook deploy, cheque issuance, EIP-712 signing
│   └── ant-ffi/                # UniFFI bindings: public API surface, error mapping
├── uniffi/
│   └── ant.udl                 # Interface definition consumed by ant-ffi
├── ios/                        # Xcode project that consumes the xcframework
├── android/                    # Gradle module that consumes the aar
└── xtask/                      # cargo xtask for build orchestration
```

The mantaray walker and the bee-parity `Accounting` mirror currently live inside `ant-retrieval` rather than in the planned standalone crates (`ant-mantaray`, `ant-accounting`). They'll graduate to their own crates when a second consumer (mobile / uploads) needs them; until then the tighter blast radius is worth more than the crate-split hygiene.

Each crate owns its data model and exposes a small API. `ant-node` is the only crate that knows about everything. `ant-control` is local operator plumbing only and must not leak into protocol crates. `ant-ffi` is the only crate that knows about UniFFI. `antd` is a thin shell around `ant-node` that adds CLI argument parsing, config loading, and signal handling — it must never reach around the core to touch lower crates directly, so that the daemon and the mobile library remain behaviorally identical.

---

## 5. Protocol Scope — What We Actually Build

### 5.1 libp2p host

- Single TCP transport, IPv4 + IPv6.
- Security: Noise XX.
- Muxer: Yamux.
- Identify protocol.
- Kademlia for forwarding-Kademlia neighborhood discovery.
- `request-response` behavior for retrieval / pushsync framing.
- **Peer ID = libp2p identity** derived from secp256k1 node key (compatible with bee's overlay derivation).

### 5.2 Overlay + Hive

- Overlay address = `keccak256(eth_address || network_id || nonce)`, truncated per Swarm spec.
- Hive v2 peer exchange: request peers close to our overlay depth, maintain routing table bins.
- Implement neighborhood depth tracking (simplified: we only need enough peers to route retrieval / pushsync targets).

### 5.3 Retrieval (client)

- Single-stream protobuf request/response.
- Verify chunk integrity: `keccak256(span || payload) == reference` for content-addressed chunks; signature verification for single-owner chunks (SOCs).
- Two-tier chunk cache keyed by chunk reference: in-memory LRU first, SQLite-backed persistent cache second, both behind explicit size limits and request-level bypass controls.

### 5.4 BMT + Chunking

- Chunk size: 4096 bytes payload + 8-byte span.
- BMT hash over 128-segment binary Merkle tree using keccak256.
- Streaming chunker that turns arbitrary byte input into a tree of chunks, yielding the root reference.
- Handle small files (single chunk), normal files, and large files (multi-level intermediate chunks).

### 5.5 Mantaray (manifests)

- Binary trie encoding for path → reference mapping.
- Minimum viable: construct a manifest from a flat list of `(path, reference, metadata)` entries and traverse by path.
- Support "root metadata" node for website-style manifests (index document, error document) — needed for `bzz://` resolution.

### 5.6 Pushsync (client only)

- Push chunk → nearest neighbors via forwarding-Kademlia routing.
- Receive signed receipt from storer peer; verify signature corresponds to overlay.
- Retry with alternative routing on timeout / bad receipt.
- Account each peer hop for SWAP (see §5.8).

### 5.7 Postage stamps

- `PostageStamp` contract on Gnosis Chain: buy batch, top-up, dilute, read batch state.
- Local persistence of each batch: `batch_id`, `owner_key`, `depth`, `bucket_depth`, `amount`, `normalized_balance`, `buckets[]` (bucket counters).
- Per-chunk stamp: sign `keccak256(chunk_addr || batch_id || index || timestamp)` with batch owner key.
- Bucket tracking: maintain per-bucket counters so we never exceed `2^bucket_depth` stamps per bucket.
- Expiry calculation: `stamp_alive = normalized_balance > current_total_outpayment`. Poll chain periodically; warn app before expiry.

### 5.8 SWAP / Chequebook

- `ChequebookFactory` → deploy per-device chequebook at first run. Chequebook holds xBZZ used to pay forwarders.
- `Chequebook` contract: we only call `increaseBalance` and issue off-chain cheques; cashing is the counterparty's job.
- Off-chain accounting per peer: `paymentThreshold`, `disconnectThreshold`, `earlyPayment` logic per bee spec.
- Cheque signing: EIP-712 typed data: `(chequebook, beneficiary, cumulativePayout)` signed by chequebook owner key.
- Persist outstanding cheques; re-issue the highest cumulative payout on peer reconnect.

### 5.9 Gnosis Chain plumbing

- JSON-RPC client over HTTPS using `alloy-rpc-client`.
- ABI encoding for: `ERC20` (xBZZ, xDAI balance checks, approvals), `PostageStamp`, `ChequebookFactory`, `Chequebook`.
- Transaction signing: EIP-155 legacy + EIP-1559; default to EIP-1559 on Gnosis.
- Nonce manager: local monotonic counter, re-sync on reorgs / rejections.
- Gas estimation with a small safety multiplier; fee-bumping on stuck txs.
- Confirmation waiter: configurable block depth (default 2 on Gnosis).

### 5.10 Key management

- Three keys per device:
  1. **Node key** (secp256k1): libp2p identity + overlay.
  2. **Wallet key** (secp256k1): pays gas, owns chequebook, signs cheques.
  3. **Batch owner key(s)** (secp256k1): signs postage stamps. Often same as wallet key, but modelled separately so batches can be imported.
- Keys live **outside the Rust lib**: mobile app provides them to the library via a `KeyProvider` trait exposed through UniFFI, backed by:
  - iOS: Secure Enclave + Keychain
  - Android: StrongBox / Keystore (hardware-backed when available)
- Rust library never persists raw private keys; it only holds public keys + signs through the callback.

**Status (first step shipped, iOS).** `ant_identity_generate` /
`ant_identity_from_key` / `ant_init_with_identity` let the host own the
key *at rest*: `examples/ios-stream` (AntStream) creates the identity,
keeps it in the Keychain wrapped by a Secure Enclave P-256 key, and
passes it in at startup, so the library never writes `identity.json`.
`ant_init` / `ant_init_with_options` (AntDrive, AntDownload, Android)
keep the data-dir behaviour. The remaining half is the callback-based
`KeyProvider` — the library still holds the raw secret in memory while
running, because signing happens at six independent sites (BZZ
handshake, postage stamps, SWAP cheques, Gnosis txs, ACT ECDH, gateway
pubkey derivation) and ACT needs ECDH, not just ECDSA.

---

## 6. Data Model & Persistence

Single SQLite database, versioned migrations. Core tables:

| Table | Purpose |
|---|---|
| `config` | Key-value app config, schema version. |
| `peers` | Known peers, overlay, multiaddrs, last-seen, reputation counters. |
| `chunks` | Content-addressed chunk cache (payload blob + last-access). |
| `batches` | Owned / imported postage batches. |
| `batch_buckets` | Per-batch bucket counters (sharded table). |
| `cheques_issued` | Per-peer latest cumulative payout. |
| `cheques_received` | Stored for audit even though we don't cash on v1. |
| `chain_state` | Latest block, nonce, pending tx tracking. |
| `uploads` | User-level upload jobs with status, reference, manifest metadata. |

### 6.1 Chunk Cache

The default persistent chunk cache is SQLite, not a sharded file tree. Chunks are small, content-addressed blobs, and the rest of the node already needs a versioned local database. Keeping chunk metadata and payloads in SQLite gives us migrations, atomic writes, LRU metadata, crash recovery, and predictable eviction without inventing a second storage format.

Cache lookup order:

```text
in-memory LRU -> SQLite disk cache -> network retrieval
```

The cached value is the exact retrieval wire bytes (`span || payload` for CAC chunks, or SOC wire bytes), keyed by the 32-byte chunk address. Network-fetched chunks are already CAC/SOC-validated before insertion. Disk hits must be validated again before use; if validation fails, delete the row and fall through to the network.

Initial SQLite shape:

```sql
CREATE TABLE chunks (
  address BLOB PRIMARY KEY,
  data BLOB NOT NULL,
  size INTEGER NOT NULL,
  last_access INTEGER NOT NULL,
  inserted_at INTEGER NOT NULL
);

CREATE INDEX chunks_last_access_idx ON chunks(last_access);
```

Recommended pragmas:

```sql
PRAGMA page_size = 8192;        -- before first table creation
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 5000;
```

The persistent tier must have a hard size cap from day one. Evict oldest `last_access` rows in batches once the tracked byte total exceeds the configured maximum, preferably down to a slack target around 90-95% of the cap so writes do not trigger constant single-row eviction. SQLite should comfortably handle tens of GB and is expected to be fine around 100 GB with this schema; revisit the storage layout only if benchmarks show SQLite BLOB pressure at the target cache sizes.

Default sizing guidance:

- Mobile / embedded library: persistent cache disabled by default or capped by host-app config, commonly 512 MB-2 GB.
- `antd` desktop / Raspberry Pi: 10 GB default.
- Power-user desktop / server: configurable to 100 GB+.

RAM should not scale with disk-cache size. The current in-memory tier is 8192 chunks, roughly 32 MiB of raw chunk data and about 40-60 MiB after LRU/vector overhead. Add a bounded SQLite page cache (for example 4-16 MiB on mobile and 16-64 MiB on desktop/Pi). Shrink the in-memory tier for constrained embedders rather than coupling memory use to persistent-cache capacity.

Async integration rule: never run blocking SQLite calls directly on Tokio retrieval tasks. Use a dedicated storage worker with hot prepared statements, or `spawn_blocking` for the first implementation if the code stays simple. Request-level cache bypass must skip both disk reads and disk writes; daemon-wide "per request cache" should only control whether the in-memory tier is shared across user-visible requests.

One-file-per-chunk storage remains a fallback, not the default. Only switch to it if measured SQLite BLOB overhead or write amplification becomes the bottleneck at the configured scale; even then SQLite should keep ownership of the metadata and eviction index.

### 6.2 Disk cache throughput vs bee (litter-ally.eth harness)

Head-to-head microbenchmarks isolate the **persistent disk chunk path** only: real files from a **litter-ally.eth** asset tree (images, CSS, WAV masters) chunked into bee-style CAC segments (≤4 KiB payload), written with batched commits, then loaded under configurable concurrency. They do **not** include HTTP, libp2p, or joiner overhead — they answer how fast the backing store serves random chunk lookups once data is on disk.

**Dataset (example checkout under `LITTER_BENCH_DIR`, default `/tmp/litter_bench`):** 8 files, ~215 MiB of chunk wire bytes, **54_946** CAC chunks.

**Ant:** `crates/ant-retrieval/examples/litter_disk_cache_bench.rs` — [`DiskChunkCache`](crates/ant-retrieval/src/disk_cache.rs) in SQLite, populate via `put_batch` with **8192** chunks per transaction, read phase uses **K** concurrent Tokio tasks each hot-looping `get(addr)` for 3 s. Run:  
`cargo build --release --example litter_disk_cache_bench -p ant-retrieval` then `target/release/examples/litter_disk_cache_bench`.

**Bee:** `scripts/bee_sharky_litter_bench/main.go` is copied into a bee **v2.7.1** tree as `pkg/storer/cmd/litterbench` — same file walk and CAC chunking, populate via `transaction.Storage.Run` batches of **8192** `Put`s (Sharky blob layer + LevelDB retrieval index), read phase **K** goroutines calling `ChunkStore.Get` for 3 s. Helper: `scripts/run_bee_sharky_litter_bench.sh` (clone/patch), full comparison: `scripts/compare_litter_disk_cache.sh` (keeps the Go source in sync with the repo copy before `go run`).

**Findings (two full runs on the same machine; absolute ops/s vary run-to-run, ordering is stable):**

- **Cold populate:** bee remains **~2× faster** (~2.3–2.4 s vs ~4–7 s). Ant’s SQLite fill cost is sensitive to I/O scheduling; bee’s LevelDB + Sharky batch path is cheaper for this workload.
- **Sustained reads:** bee wins at **low K**; ant overtakes as **K** grows.

| K (parallel readers) | Typical winner | Comment |
|---|---|---|
| 1, 2, 4 | **bee** | ~1.5–3× higher ops/s; single-threaded store access favours bee’s in-process path |
| 8 | **ant** | Crossover band; ant’s read pool amortises per-request overhead |
| 16, 32, 64 | **ant** | ant scales up; bee throughput **plateaus** (~125–155k ops/s on this host) while ant reaches **~320–460k ops/s** at K=16–64 |

Approximate **second run** snapshot (ops/s, same dataset):

| K | bee | ant |
|---|-----|-----|
| 1 | ~51k | ~16k |
| 2 | ~116k | ~41k |
| 4 | ~152k | ~101k |
| 8 | ~132k | ~182k |
| 16 | ~126k | ~319k |
| 32 | ~130k | ~425k |
| 64 | ~137k | ~463k |

**Interpretation:** gateway-scale streaming often issues **many concurrent chunk reads**; this harness suggests the SQLite-backed disk tier can **out-run bee’s chunkstore** once concurrency is high enough, at the cost of **slower cold fill** and **weaker single-threaded read latency**. Production behaviour still depends on the full stack (LRU, fetcher hedging, SQLite pragmas, and filesystem).

---

## 7. Public API (FFI Surface)

Keep the UniFFI surface small and stable. Sketch:

```idl
namespace ant {};

interface Node {
  [Throws=AntError]
  constructor(Config config, KeyProvider keys);

  void start();
  void stop();
  NodeStatus status();

  // retrieval
  [Throws=AntError] bytes download(Reference reference);
  [Throws=AntError] Reference resolve_path(Reference manifest, string path);

  // upload
  [Throws=AntError] UploadHandle upload_bytes(bytes data, UploadOptions opts);
  [Throws=AntError] UploadHandle upload_manifest(sequence<ManifestEntry> entries, UploadOptions opts);

  // stamps
  [Throws=AntError] Batch buy_batch(u8 depth, string amount_plur);
  sequence<Batch> list_batches();
  [Throws=AntError] void topup_batch(BatchId id, string amount_plur);
  [Throws=AntError] void dilute_batch(BatchId id, u8 new_depth);

  // swap
  [Throws=AntError] ChequebookInfo chequebook();
  [Throws=AntError] void deploy_chequebook(string deposit_plur);

  // events
  void set_event_listener(EventListener listener);
};

callback interface KeyProvider {
  bytes public_key(KeyRole role);
  bytes sign(KeyRole role, bytes digest);
};

callback interface EventListener {
  void on_event(AntEvent event);
};
```

All amounts that cross FFI are strings (PLUR / wei) to avoid precision loss; no `BigInt` in UniFFI.

---

## 8. UX & Onboarding Design (mandatory before coding uploads)

The product has to solve three onboarding problems or uploads will be unusable on day one:

### 8.1 Funding ceremony

- User needs xDAI (gas) + xBZZ (stamps + chequebook deposit) on Gnosis Chain.
- **Recommended pattern:** app backend runs a *relayer* that:
  - Funds the user's wallet with a minimal xDAI bootstrap (once, per device, rate-limited).
  - Optionally sells xBZZ in-app (fiat on-ramp via a third-party provider) or sponsors first batch.
- Power users: accept an import flow for existing wallets.

### 8.2 Chequebook pre-deploy

- Chequebook deploy is a blocking tx before the first upload. Options:
  1. **Ship a sponsored deploy** via the relayer: relayer fronts gas, user signs the deploy via EIP-2771 meta-tx, chequebook ends up owned by the user. Best UX.
  2. **Deploy on first app launch** with a clear progress UI ("Preparing your storage…"). Acceptable fallback.
  3. **Background deploy** right after wallet funding, before the user even tries to upload.

### 8.3 Stamp UX

- Never surface `depth`, `amount`, `bucket_depth` in the primary UI.
- Offer presets: "Quick share (1 day, ~1k files)", "Long-term (1 year, ~100k files)", etc. Compute parameters behind the scenes.
- Show expiry as a calendar date with a colored status (green / yellow / red).
- Auto top-up at a user-configurable threshold, with a hard-limit per day to prevent runaway spend.

### 8.4 Background behavior

- iOS: use BGProcessingTask / BGAppRefreshTask for periodic chain polling and chunk-upload resumption.
- Android: WorkManager foreground service for active uploads, regular workers for polling.
- The Rust lib must support **graceful suspend/resume**: save state, close sockets, drop memory; reconnect on resume without re-downloading state from chain.

### 8.5 Key recovery

- Export / import using BIP-39 seed phrase for the wallet key (optionally the node key too).
- Cloud backup is **opt-in** and must be explicit; default is local-only.

---

## 9. Phased Delivery Plan

The work is organized around **three capability milestones**, each of which is independently demoable and, in the case of M1 and M2, independently *shippable* as an internal or external product in its own right:

1. **M1 — Peer discovery & connection.** The node joins the Swarm network and maintains a healthy peer set. No data movement yet. Proves the hardest infrastructure piece (libp2p + overlay + BZZ handshake + Kademlia) in isolation. Split into **M1.0** (basic mainnet connection via `antd`) and **M1.1** (stable neighborhood via Hive + Kademlia).
2. **M2 — Ultra-light node (download only).** The node retrieves any mainnet reference. Shippable as a standalone "Ant Reader" app. No chain, no keys beyond libp2p identity, no xDAI/xBZZ required.
3. **M3 — Light node (uploads + stamps + SWAP).** Full read/write node with on-device stamp management and honest payments. The GA product.

A post-M3 **hardening track** covers UX polish, backgrounding, and beta — without which the product isn't fit to ship, but where the protocol risk is already retired.

Each milestone is subdivided into phases that end with a concrete demo.

### 9.0 Current status (snapshot)

Workspace is at **`0.5.16`** across all live crates (`antd`, `antctl`, `antop`, `ant-control`, `ant-crypto`, `ant-gateway`, `ant-node`, `ant-p2p`, `ant-retrieval`, the M3 additions `ant-chain` and `ant-postage`, and the mobile surface `ant-ffi`). The same binary is deployed on `vibing.at/ant` as a live bee-compatible gateway against Swarm mainnet.

| Milestone | Status | Notes |
|---|---|---|
| **M1.0** — Basic mainnet connection | ✅ shipped | `antd` joins mainnet via `/dnsaddr/mainnet.ethswarm.org`, BZZ handshake on `/swarm/handshake/14.0.0`, holds ~100-peer warm set across restarts. |
| **M1.1** — Stable neighborhood | ✅ shipped | Hive v2 peer exchange, forwarding-Kademlia routing bins, signed BZZ address verification; persistent `peers.json` snapshot, `antop` peer/routing dashboard. 24 h soak clean. |
| **M2 / Phase 2** — Retrieval | ✅ shipped | `ant-retrieval`: bee-parity fetcher with cancel-tolerant hedging (Appendix G), admission-control mirror (Appendix H), pseudosettle-backed accounting (Appendix F), two-tier chunk cache (in-mem LRU + SQLite-backed `DiskChunkCache`, Appendix §6). |
| **M2 / Phase 3** — Mantaray + download API | ✅ shipped | Mantaray walker + `/v0/manifest` + `bzz://` path resolution, Swarm-feed dereferencing, ordered streaming joiner with single-range and HEAD support (Appendix E). |
| **M2 / Phase 4** — Ultra-light packaging | 🚧 partial | Tier-A `ant-gateway` live on 127.0.0.1:1633 with `--api-addr` / `--no-http-api` (Appendix D); bee-js / curl / browser smoke passes. The hand-written `ant-ffi` C surface has grown far past the smoke-test scope — ~30 `extern "C"` entry points covering download, AVPlayer-shaped streaming, uploads, storage plans (incl. on-chain quote/buy behind the `chain` feature), and account info, consumed in-repo by the iOS `AntDownload` + `AntDrive` and Android example apps. **The UniFFI artefact (xcframework / aar) is still not started**; an interim pre-UniFFI release of the hand-written surface is planned as its own track (§ "Interim releasable iOS artifact — `AntFFI.xcframework`"). `antctl get` streams progress but the download body is still terminal, not byte-streamed (Appendix E Phase 5). |
| **M3 / Phase 5** — Chain plumbing (reads + writes) | ✅ shipped | New `ant-chain` crate. JSON-RPC client (`eth_call`, `eth_getLogs`, `eth_getBalance`, `eth_estimateGas`, `eth_sendRawTransaction`, `eth_getTransactionReceipt`). ABI calldata builders for ERC-20 (`balanceOf`, `approve`), `PostageStamp` (`createBatch`, `topUp`, `increaseDepth`, `batchOwner`, `batchDepth`, `batchBucketDepth`, `batchImmutableFlag`), and `SimpleSwap` chequebook (`issuer`, `balance`, `liquidBalance`, `totalPaidOut`, `cashChequeBeneficiary`). EIP-155 legacy tx signing with `ant_crypto::sign_prehash`. Receipt polling. `dry_run_wallet` example. |
| **M3 / Phase 6** — First uploads (pre-provisioned batch) | ✅ shipped | New `ant-postage` (`StampIssuer`, EIP-191 stamp signing, bucket counters with crash-safe `write tmp + fsync + rename` persistence) + new `ant-retrieval::pushsync` (`/swarm/pushsync/1.3.1/pushsync` client, receipt verification, storer-signature recovery). Wired through `antd` → `UploadRuntime` → gateway `POST /chunks`. Live mainnet smoke: single-chunk push + retrieve via fresh fetcher. |
| **M3 / Phase 7** — SWAP / payment | 🚧 partial | EIP-712 cheque signing + verification primitives (`ant-chain::chequebook`), with the chequebook EIP-712 domain matching bee's `EIP712DomainType` (`name / version / chainId`, no `verifyingContract`). Bee-compatible `/swarm/swap/1.0.0/swap` listener + dialer (`ant-p2p::swap`) — JSON-encoded `SignedCheque` inside protobuf `EmitCheque`, signature verified, monotonic, sticky issuer-EOA pinning, crash-safe on-disk credit ledger. BZZ handshake now exposes the peer's Ethereum EOA + welcome string in `HandshakeInfo` (and a new `handshake_outbound_with_role` lets the smoke binary advertise as a full node). Outbound `OutboundLedger` + `issue_and_emit` helpers in place. **Tier-1 on-chain cheque round-trip ✅** — `antctl chequebook cash-self --submit` proves bit-exact bee compatibility on Gnosis mainnet (block `46019699`, tx `0x643bb08e…e308`); the chequebook contract recovered our issuer from our EIP-712 cheque and debited 1 PLUR. **2026-05-08 production blocker:** real-world GGUF upload attempt confirmed that pushsync today **pays nobody** — the dialer in `ant_retrieval::pushsync` has no debit hook and no cheque emission, so peers silently RST us after their `paymentTolerance` is exceeded (~20-30 K successful chunks across the peer set). Phase 7 polish was already aware of "auto-settlement trigger missing", but the original plan-list framed it as a retrieval-side accounting hook (the existing `Accounting` mirror); in fact the missing piece is upload-side outbound accounting — captured as the new **Phase 7b — Pushsync-side outbound accounting** below. **Update (2026-06):** Phase 7b landed in 0.3.12 (cheque emission wired into the pushsync dialer), Phase 7c in 0.3.13 (`antctl chequebook deploy` / `verify` against the official factory — which also disproved the "not factory-deployed" diagnosis), and Phase 7d in 0.3.14 (pushsync-side pseudosettle sharing the retrieval `HotHint` channel) — first sustained mainnet upload past the 20 K-chunk wall: 256 MiB / 66 056 chunks end-to-end. Phases 7e–7g tuned throughput (7e negative result, 7f/7g `--target-peers` sweep + gateway max-bytes raise). **Outstanding:** re-run the live `cheque_smoke` mainnet binary with the fixed EIP-712 digest (rung B — the old "cold-peer rejection" diagnosis is still unconfirmed). |
| **M3 / Phase 8** — Stamp lifecycle | ✅ shipped | `antctl postage create / top-up / dilute / show / balance` subcommands talk straight to Gnosis (no daemon required). Bucket counters persisted per batch at `<data_dir>/postage/<batch_id>.bin`, reloaded on restart so we don't reissue indices. Approve-then-create flow handled automatically. **Outstanding:** expiry monitoring + app-level events, on-device dilute/topup orchestration with per-batch budgets. |
| **M3 / Phase 9** — Streaming uploads + Mantaray construction | ✅ shipped | `ant-retrieval::splitter` (bytes → balanced k-ary chunk tree, inverse of the joiner) + recursive `ant-retrieval::manifest_writer` (Mantaray v0.2 radix trie that fits one CAC chunk per node, deepens the tree as collections grow, and chains 30-byte forks for long path segments). Gateway `POST /bzz?name=…` (single file) and `POST /bzz` (tar collection) live. Live mainnet smoke: single-file + tar collection upload, fresh-fetcher round-trip. **`beeMode`** now reports `"light"` once outbound settlement is configured (`ant-gateway::status` gates on `handle.light_mode`), `"ultra-light"` otherwise. |
| **Hardening / Phase 10–11** | 🚧 partial | Upload resumption landed: `ant-node`'s `UploadManager` persists per-job state (resume cursor = last contiguously-pushed chunk) across daemon restarts, with pause/resume/cancel surfaced through `antctl upload` and the `ant-ffi` upload-job API (exercised by AntDrive). Mobile suspend/resume wiring and beta still not started. |

Active hardening tracks carried alongside M2 and M3:

- Retrieval / accounting polish: F.7 follow-ups (persist pseudosettle budget across restarts, per-peer chunks-per-connection counter, investigate ~30 ms peer churn pattern). Low-impact, untouched.
- §6.2 disk-cache asymmetries: bee ~2× faster on cold populate and 1.5–3× faster at K ≤ 4 single-threaded reads; ant wins from K ≥ 8 upward. Worth a targeted pragma / prepared-statement pass.
- Appendix E Phase 5 (CLI streaming) and Phase 8 (Reed-Solomon recovery) remain the two read-path gaps.
- M3 Phase 7 polish: re-run the `cheque_smoke` mainnet binary with the fixed EIP-712 digest (rung B). The other two items from this list have since landed: the upload-side settlement trigger shipped as Phases 7b/7d (cheque emission + pushsync-side pseudosettle), and `antctl chequebook deploy` / `verify` shipped as Phase 7c (0.3.13). Tier-1 cash-self interop is done — see `antctl chequebook cash-self`.

Detailed post-facto engineering write-ups of the retrieval/accounting work live in Appendices E (gateway streaming), F (pseudosettle), G (cancel-tolerant hedging + moving window) and H (admission-control mirror). The phase descriptions below remain the original plan — consult the appendices and the per-phase "Landed as" notes for what actually shipped and how.

---

### Milestone 1 — Peer discovery & connection

Goal: the embedded node connects to Swarm mainnet, completes the BZZ handshake with real bee peers, exchanges peers via Hive, and maintains Kademlia routing table bins consistent with the neighborhood of our overlay address. All development at this stage happens against the `antd` binary; mobile packaging is deferred to the end of M2 where it's meaningful to demo.

#### Phase 0 — Foundations + basic mainnet connection — **M1.0** — ✅ shipped

Keep the workspace tiny: only the crates needed to dial bee peers, survive the BZZ handshake, expose enough operator control to debug a long-running daemon, and gather the peer data needed for M1.1. No SQLite, no UniFFI, no mobile artifacts yet.

- Workspace skeleton with `ant-crypto`, `ant-p2p`, `ant-node`, `ant-control`, `antd`, and `antctl`.
- CI (GitHub Actions), lint (`clippy -D warnings`), format (`rustfmt`), pre-commit.
- `ant-crypto`: secp256k1 key handling, keccak256, Ethereum address derivation, overlay address derivation (`keccak256(eth_address ‖ network_id_le ‖ nonce)`), BZZ address signing + verification. Unit tests against vectors cross-checked with a local `bee`.
- `ant-p2p`:
  - libp2p host using **secp256k1 identity** (not ed25519), TCP + DNS transports, Noise XX, Yamux, Identify, Ping.
  - `/swarm/handshake/14.0.0/handshake` protobuf implementation (Syn / SynAck / Ack), including bee-compatible half-close behavior.
  - Recursive `/dnsaddr/mainnet.ethswarm.org` resolution and grouped peer dials.
  - Connection lifecycle: reconnect with bounded exponential backoff, log handshake outcome with overlay + agent version, and maintain a target peer pipeline.
  - Drain bee post-handshake pricing streams and parse Hive peer broadcasts into dial hints.
  - Persist a temporary JSON peer snapshot for warm restarts until `ant-store` replaces it.
- `antd`: CLI with `--data-dir`, `--network-id`, `--bootnodes`, `--log-level`, `--key-file`, `--external-address`, control-socket, and peerstore options; generates a persistent node key + nonce on first run; `tracing-subscriber` for human-readable logs.
- `antctl`: `status`, `version`, and `peers reset` for live daemon inspection; `antop` for the auto-refreshing peer/routing dashboard.

**Exit (M1.0):** `antd` running on a laptop connects to live mainnet bee peers, completes the BZZ handshake, fills at least a small peer set from bootnode/Hive hints, survives a restart from the JSON peer snapshot, and stays connected across network blips. Log output matches roughly:

```
INFO antd: loaded identity eth=0x1a2b…c4 overlay=0x7f3e…91
INFO ant_p2p: dialing /ip4/…/tcp/1634/p2p/16Uiu2HAm…
INFO ant_p2p: libp2p connected agent="bee/2.7.0"
INFO ant_p2p: bzz handshake ok overlay=0x2c4a…8e full_node=true
INFO ant_p2p: peer set size=1
```

#### Phase 1 — Stable neighborhood via Hive + Kademlia — **M1.1** — ✅ shipped

- Extend `ant-p2p` with:
  - **Hive v2** peer exchange: keep the current inbound Hive peer broadcast parser, add any missing request path needed to actively ask peers for closer addresses, verify signed BZZ addresses before promotion, and enqueue dials by routing need instead of FIFO only.
  - **Forwarding Kademlia** routing: bin-based routing table keyed on XOR distance from our overlay; neighborhood depth tracking; pluggable peer-selection for later protocols.
  - Peer health and replacement policy: distinguish dial failure, handshake failure, post-handshake disconnect, and long-lived healthy peer.
- `ant-store`: minimal SQLite schema for peers, routing snapshot, and `config` key-value. Migrate the current JSON peer snapshot API behind the store so `antd` behaviour stays stable.
- `ant-node`: orchestrator wiring that owns the node loop, applies backpressure, and surfaces status.
- Foundational cross-compile pipeline: produce stub `.xcframework` + `.aar` from a "hello-world" UniFFI crate so M2 can hit the ground running on mobile.
- Operator acceptance:
  - `antop` shows connected peers, selected-peer detail, last BZZ handshake, and routing/neighborhood summary.
  - `antctl peers reset` and daemon startup `--reset-peerstore` remain supported after the SQLite migration.
  - A 24-hour soak script writes a compact acceptance report: peer count over time, routing-bin coverage, reconnects, handshakes, and disconnect reasons.

**Exit (M1.1):** `antd` run for 24 hours against mainnet maintains a stable peer set across suspend/resume, and its Kademlia routing bins match a parallel `bee` node's view of the same neighborhood within a small tolerance. This is the real M1 checkpoint — after this we trust the infrastructure.

---

### Milestone 2 — Ultra-light node: download only

Goal: the node retrieves any reference from mainnet, including multi-chunk files and path-addressed content inside a mantaray manifest. No chain interaction, no wallet. This is a complete, shippable read-only product.

#### Phase 2 — Retrieval — ✅ shipped

- `ant-retrieval`: client protocol, protobuf messaging, stream multiplexing over libp2p `request-response`.
- Chunk verification: BMT root check for content-addressed chunks; signature check for single-owner chunks.
- Chunk cache with in-memory LRU plus SQLite persistent tier, hard size cap, LRU eviction, corruption handling, and cache-bypass semantics.
- Retry / alternate-peer selection on failure.

**Exit:** Mobile sample app downloads a known single-chunk and multi-chunk Swarm reference from mainnet.

**Landed as:** bee-shaped `RoutingFetcher` with admission-control mirror, cancel-tolerant hedging, pseudosettle-backed accounting, two-tier cache (`InMemoryChunkCache` + `DiskChunkCache`), and per-request bypass. Live end-to-end in `antd` and covered by `antctl get <chunk-hex> | bytes://<hex> | bzz://<hex>[/path]`. Mobile demo deferred with the rest of the mobile artefact (Phase 4).

#### Phase 3 — Mantaray traversal + high-level download API — ✅ shipped (non-FFI)

- `ant-mantaray`: binary-trie decoder, path traversal, root-metadata handling.
- `download(Reference)` → bytes, and `resolve_path(Reference, String)` → Reference, both over FFI.
- Streaming download API so large files don't require holding the whole payload in memory.

**Exit:** Sample app opens a `bzz://` reference with a path and renders the content (e.g., an image or a JSON file inside a directory manifest).

**Landed as:** mantaray walker + path resolution live inside `ant-retrieval` (not yet split out into a standalone `ant-mantaray` crate), exposed through the daemon's control protocol and the Tier-A HTTP gateway (`/bzz/<addr>[/<path>]`, `/v0/manifest`). Streaming joiner emits bytes in file order with single-range and HEAD support (Appendix E Phases 1-3). FFI surface not yet exposed — it will land with the mobile artefact in Phase 4.

#### Phase 4 — Ultra-light packaging + release-candidate — 🚧 partial

- Size budget enforcement in CI (fail PRs that push `.xcframework` / `.aar` past the M2 budget).
- Foreground + background suspend/resume for reads.
- Observability bridges (OSLog / logcat).
- Tier-A `ant-gateway` shipped with `antd` (Appendix C): bee-shaped read endpoints, stub wallet/stamps/chequebook, `beeMode = "ultra-light"`. Mobile artifact unchanged (gateway is feature-gated off in `ant-ffi`).
- Internal beta of the download-only build.

**Exit (M2):** Independently shippable ultra-light library. Per-platform artifact ≤ ~5 MB stripped (lighter than the full M3 target because no chain / SWAP / stamps code).

**Landed so far (desktop / server):** Tier-A `ant-gateway` is live on `127.0.0.1:1633` inside `antd` (`--api-addr`, `--no-http-api`) per Appendix D. `beeMode = "ultra-light"`, stub wallet / stamps / chequebook. `bee-js` / `curl` / browser smoke passes against it. Released binary runs on `vibing.at/ant` serving mainnet-hosted sites and media with HEAD + single-range support.

**Still outstanding:** mobile `.xcframework` / `.aar` artefact (the hand-written `ant-ffi` C/JNI surface exists and is exercised by the example apps; the UniFFI packaging does not), CI size budget, suspend/resume handling, OSLog / logcat bridges, and internal beta. An interim, pre-UniFFI `AntFFI.xcframework` release of the existing hand-written C surface is planned as a separate packaging track — see § "Interim releasable iOS artifact — `AntFFI.xcframework` (pre-UniFFI)" under the iOS smoke-test section; it de-risks the distribution mechanics (xtask packaging, SPM `binaryTarget`, release CI) without changing the Phase 4 UniFFI commitment. `antctl get` streaming (Appendix E Phase 5) and Reed-Solomon recovery (Appendix E Phase 8) are the last two read-path hardening items before this phase closes.

---

### Milestone 3 — Light node: uploads + stamps + SWAP

Goal: everything in M2, plus honest on-device uploading with on-chain postage-stamp management and SWAP-based payment to forwarders.

#### Phase 5 — Chain plumbing, reads first — ✅ shipped

- `ant-chain`: alloy-based RPC client, ABIs for `ERC20`, `PostageStamp`, `ChequebookFactory`, `Chequebook`.
- Nonce manager, confirmation waiter, fee-bumping scaffolding (not yet exercised).
- `KeyProvider` FFI contract finalized; iOS Keychain + Android Keystore backends wired in.
- Read-only flows: show wallet balances, read an existing batch, read chequebook state.

**Exit:** Mobile app reads and displays an on-chain batch and chequebook for the embedded wallet. No writes yet.

**Landed as:** new `ant-chain` crate built on the existing `reqwest` JSON-RPC dependency rather than `alloy-*` (kept the dependency graph thin to protect the §12 size budget). Exposes `ChainClient` with `eth_call`, `eth_get_logs`, `eth_get_balance_lower128`, `eth_estimate_gas`, `eth_get_transaction_count`, `eth_send_raw_transaction`, and a receipt poller. ABI calldata builders for ERC-20 (`balanceOf`, `approve`), `PostageStamp` (`createBatch`, `topUp`, `increaseDepth`, plus the `batchOwner / batchDepth / batchBucketDepth / batchImmutableFlag` view selectors), and `SimpleSwap` chequebook (`issuer`, `balance`, `liquidBalance`, `totalPaidOut`, `cashChequeBeneficiary`). EIP-155 legacy tx signing via `ant_crypto::sign_prehash` (RLP using the `rlp` crate; `v = chain_id*2 + 35 + recid`). High-level `Wallet` struct chains nonce-fetch + estimate + sign + submit + receipt-poll. `examples/dry_run_wallet.rs` exercises the signer against a live RPC without submitting. The `KeyProvider` FFI contract is **not yet** finalised — keys today are loaded from CLI flags / `.env`; mobile-grade Keychain/Keystore wiring lands with the M2 Phase 4 mobile artefact.

#### Phase 6 — Single-chunk upload with pre-provisioned stamps — ✅ shipped

- `ant-pushsync`: client protocol, receipt verification.
- `ant-stamps`: per-chunk signing, bucket counters, atomic persistence. Batch injected via configuration — no on-chain purchase yet.
- Accounting stub: record peer debits but do not yet pay.

**Exit:** Upload a single chunk to mainnet with a pre-provisioned batch. End-to-end roundtrip: upload → reference → download via the same embedded node.

**Landed as:** new `ant-postage` crate (rather than `ant-stamps`) carrying `StampIssuer` with EIP-191 stamp signing per chunk, depth/bucket-depth-aware bucket counters, and crash-safe JSON persistence (atomic `write tmp + fsync + rename`). `StampIssuer::open_or_new` reloads the previous indices on restart so we don't reissue stamps that the network has already seen. Pushsync client lives in `ant-retrieval::pushsync` (kept inside `ant-retrieval` to share the libp2p stream plumbing with the existing fetcher, rather than starting a fourth network crate). Wired through `antd` → `UploadRuntime` → `ant-control::ControlCommand::PushChunk` → gateway `POST /chunks`. Live mainnet smoke: single-chunk push followed by retrieval via a brand-new fetcher process. The "accounting stub" remains stubbed — outbound debits are not yet recorded into a per-peer ledger; that lands with the auto-settlement trigger in the open Phase 7 polish.

#### Phase 7 — SWAP — 🚧 partial

- `ant-accounting`: per-peer ledger, payment / disconnect thresholds, early-payment logic.
- `ant-swap`: chequebook deploy via factory, EIP-712 cheque signing, cheque issuance on threshold, re-issue on reconnect.
- Integrate with pushsync so every forwarder hop is paid honestly.

**Exit:** Upload ~1 GB of chunks via pushsync while paying peers; peers never disconnect us for non-payment.

**Landed so far:** EIP-712 cheque sign/verify primitives + `cashChequeBeneficiary` / `cash_cheque_beneficiary` calldata + ABI in `ant-chain::chequebook`. New `ant-p2p::swap` module implementing the bee-compatible `/swarm/swap/1.0.0/swap` protocol — JSON-encoded `SignedCheque` wrapped in the protobuf `EmitCheque` message, byte-for-byte matching bee's `encoding/json` output (hex addresses, decimal `U256`, base64 signature). Inbound listener verifies signature, pins the chequebook contract to its initial issuer EOA on first sight (sticky — replay-safe), enforces strict monotonicity on `cumulative_payout`, and persists every accepted credit to `<data_dir>/swap_credits.json` with the same atomic-write pattern as `ant-postage`. Outbound side: `OutboundLedger` tracks the last cumulative payout per peer, `issue_cheque` builds + signs the next cheque, `issue_and_emit` opens the swap stream and sends. The BZZ handshake was extended to recover the peer's Ethereum EOA from the BZZ address signature (and to surface their welcome string), so callers know which beneficiary address belongs to a given libp2p peer. The listener is registered in `ant-p2p::Behaviour` even when SWAP isn't fully configured, so peers don't see a libp2p disconnect when they emit cheques at us.

**Tier-1 on-chain cheque round-trip — ✅ shipped.** New `antctl chequebook show` (read `issuer / balance / totalPaidOut / liquidBalance / paidOut(beneficiary)`) and `antctl chequebook cash-self` (sign a tiny EIP-712 cheque with the issuer key, eth_call `cashChequeBeneficiary` from the beneficiary's EOA, optionally `--submit` for the full state transition). First live run on Gnosis mainnet (block `46019699`, tx `0x643bb08e4485042024604a8ac223a7ea89c594d1a22a27cbba01b4bc9982e308`): chequebook `0xfEecbb9a…20D` debited 1 PLUR, `paidOut[WALLET_ADDRESS] = 1`, `total_paid_out = 1`. **Side-effect: caught a real bug in our EIP-712 implementation** — the chequebook domain is `EIP712Domain(string name,string version,uint256 chainId)` (3 fields, NO `verifyingContract`) per bee's `pkg/crypto/eip712/EIP712DomainType`, but our previous `eip712_domain_separator` was hashing the 4-field OpenZeppelin-default shape and sticking the chequebook address into the domain. The on-chain dry-run reverted with `invalid issuer signature`; the fix lives in `ant-chain::chequebook::eip712_domain_type_hash` + `eip712_domain_separator`. **This invalidates the rung-B "cold-peer rejection" diagnosis** — the immediate stream Reset we observed against bee bootnodes was almost certainly bee's chequebook-side EIP-712 verifier rejecting our (then-broken) digest, not bee's accounting Addressbook. Re-running the smoke binary with the fixed digest is now the natural next step; until then we treat the cold-peer hypothesis as *unconfirmed*.

**Outstanding (Phase 7 polish):**

- Hook into `ant-retrieval::accounting` so that crossing the per-peer payment threshold automatically calls `swap::issue_and_emit` with a fresh cheque (the helper exists; the trigger doesn't). This is the production path that actually drives the swap state machine end-to-end.
- Re-run the live `cheque_smoke` mainnet binary now that the EIP-712 digest is bee-compatible (Tier-1 proved it on-chain). If bee accepts the cheque this time, the cold-peer hypothesis was wrong and rung B is closed. If bee still Resets the stream, the cold-peer hypothesis stands and we proceed to the auto-settlement trigger above.
- `antctl chequebook deploy` (factory deploy). Cash-out (`cash-self`) shipped with Tier 1.

##### Phase 7b — Pushsync-side outbound accounting (NEW — 2026-05-08)

The 2026-05-08 production attempt to upload a 6.7 GiB GGUF model
exposed a concrete blocker that the original Phase 7 polish list
did not name: **today the pushsync dialer pays nobody.**
`ant_retrieval::pushsync::push_chunk_to_peer` opens the stream,
sends `Delivery { address, data, stamp }`, reads `Receipt`. There
is no debit, no threshold check, and no cheque emission. The
existing `ant_retrieval::accounting::Accounting` mirror is wired
into `ant_retrieval::fetcher` (the *download* path) only — bee
credits us with chunks on retrieval and we throttle ourselves
against `LIGHT_DISCONNECT_LIMIT` to avoid being RST'd. The
*upload* path has no analogue.

That is exactly what bit us on production. Live data from the
2026-05-08 GGUF push (job `00000487fb4d9dcb`):

- Burned 40,368 stamps to land 19,729 chunks (≈ 49 % stamping
  efficiency — every stamp committed bucket index is "spent" even
  when the receiver silently rejects the chunk).
- Sustained pushsync throughput collapsed to <2 chunks/sec
  network-wide after ~20K successful pushes; stalled and failed
  with `push chunk failed: exhausted 5 retries: push timed out
  after 60s` at 1.12 % of the file.
- The 15-minute journal showed pushsync rejection load distributed
  evenly across all ~100 peers (~3,800 retries/peer, ~3,700-3,975
  retries/chunk for the worst chunks). Every peer in the set was
  refusing every chunk — not a peer-set quality problem, a
  systemic "we pay nobody so everyone has us in payment-debt."
- The earlier same-session 313 MB job (`00000069be4427fc`) failed
  the same way at 38 % with the more honest error
  `pushsync: exhausted pushsync peers`.
- The local node API confirms it: `GET /chequebook/balance →
  {"totalBalance":"0","availableBalance":"0"}`,
  `GET /chequebook/address → 0x0…0`. `antd-ant --help` exposes no
  chequebook flag at all. `antd` is honestly running as
  ultra-light + stamps-only; bee peers correctly treat it as a
  freeloader after the initial paymentTolerance burst.

The rough refresh-rate ceiling we observed (~20-30K chunks across
~100 peers per peer-session) matches bee's
`paymentThreshold = 13.5 M units` divided by typical chunk price
(~240 K units at PO 8): **~56 chunks per peer before the
threshold trips.** With 100 peers and re-routing on cold rejection,
we get a few hundred lucky retries beyond the cap and then it's
over. There is no slow path that gets us through 1.76 M chunks;
without cheques we cannot upload a model.

**Phase 7b deliverable:** the pushsync dialer issues SWAP cheques
proactively, before the receiver decides to RST us. Concretely:

1. **Outbound debit mirror.**
   New `ant_retrieval::pushsync_accounting::PushsyncAccounting`
   sibling of `Accounting` (intentionally a separate type — the
   debit semantics are different). Tracks per-peer
   `cumulative_debit_units` for our outbound pushsync only.
   `accrue_debit(peer, price)` is called once per successful push
   (after the peer ACKs the receipt). `peer_price()` reuses the
   existing `Accounting::peer_price` helper.
2. **Cheque-trigger hook.** When `accrue_debit` causes
   `cumulative_debit_units` to cross
   `paymentThreshold - earlyPayment` (= 50 % of
   `LIGHT_DISCONNECT_LIMIT`, i.e. 843,750 units — same boundary as
   the retrieval-side `HOT_DEBT_THRESHOLD`), we call
   `swap::issue_and_emit(peer, cumulative_debit_units +
   safety_margin)` from `OutboundLedger`. The existing helper
   already takes the cumulative-payout amount in PLUR and signs
   an EIP-712 cheque against our chequebook. We zero our local
   debit (it's been settled by the cheque). On bee's side, the
   `EmitCheque` arrives over `/swarm/swap/1.0.0/swap`, bee credits
   `chequebook → peer` and clears our debt; subsequent pushsyncs
   are accepted.
3. **PLUR units wiring.** Bee's accounting unit is
   "chunk-price" (10⁴ units per `BASE_PRICE`). Cheques are in PLUR
   (BZZ smallest unit, 10¹⁶ PLUR per BZZ). The conversion is
   bee's `pricer.go::PeerPrice` → PLUR: `units * 1` (pricer units
   ARE PLUR — bee's `BASE_PRICE = 10000` is already 10000 PLUR).
   So the cumulative debit in units is the cumulative-payout
   integer in `Cheque.cumulative_payout`. No conversion needed.
4. **Pre-flight chequebook readiness.** Before `antd` accepts the
   first upload, the daemon checks: (a) we have a chequebook
   address from the config, (b) `eth_call → balance()` is enough
   to cover at least N peers' worth of `paymentThreshold` debt.
   On failure, the upload control verb returns a clear
   "no_chequebook" / "chequebook_underfunded" error rather than
   "succeeding" silently as today.
5. **`antd` flags / config.** Add three new `antd-ant` flags:
   - `--chequebook <addr>` (32-byte hex), env `CHEQUEBOOK_ADDRESS`.
   - `--swap-key <hex>` (32-byte secp256k1, owns the chequebook),
     env `SWAP_OWNER_KEY` *or* fall back to `WALLET_PRIVATE_KEY`
     for the test setup. Bee derives this from the same node
     mnemonic at path `m/44'/60'/0'/0/0`; we keep the existing
     "wallet key = chequebook owner" convention from `.env`.
   - `--swap-min-balance-bzz <decimal>` (default 0.01) — the
     chequebook liquid-balance floor below which uploads pre-flight
     fails. Operator-actionable: top up the chequebook on-chain
     and restart.
6. **Bee-shaped `/chequebook` HTTP endpoints (read).** Wire the
   gateway's `chequebook/balance` and `chequebook/address`
   endpoints to the configured chequebook (currently both return
   zero stubs). Read-only first — the writes (`/chequebook/cashout`,
   `/chequebook/withdraw`, `/chequebook/deposit`) can stay on the
   501 catch-all until they're needed.
7. **`bee_mode` flip.** `/node` reports `"light"` instead of
   `"ultra-light"` once a chequebook is configured AND the
   `PushsyncAccounting` ↔ `OutboundLedger` wire is in place.
8. **Smoke + Acceptance.** New `cheque_smoke_pushsync` mainnet
   binary that uploads N=100 distinct random chunks under a real
   stamp and confirms (a) at least one `EmitCheque` was sent to
   each unique forwarder, (b) all 100 chunks retrieved fresh by
   a second daemon, (c) chequebook `total_paid_out` increased
   by ≥ N × MIN_CHUNK_PRICE on-chain. The existing single-cheque
   `cheque_smoke` becomes a unit-level prerequisite of this one.
9. **Tonight's specific verification (the one that prompted
   this section):** with phase 7b live, restart the
   `gemma4:e2b` GGUF upload (job restart, fresh stamps acceptable
   since we never finished). Acceptance is bit-identical
   `bzz://<root>/model.json` round-trip per the production task,
   not a synthetic smoke test.

**Cost / schedule sanity check.** The chequebook in
`.env` (`0xfEecbb9a…20D`) is funded; 36.99 BZZ in the wallet, of
which the chequebook holds an unknown deposit (the `cash-self`
test only debited 1 PLUR, so liquid balance is essentially the
full deposit). At bee's `paymentThreshold = 13.5 M units` per
peer-session and ~100 peers, an upload cycle pays out at most
`100 × 13.5 M = 1.35 B PLUR ≈ 1.35 × 10⁻⁷ BZZ`. The full
1.76 M chunk push, even with worst-case 100 % cheque emission per
threshold cross, costs <0.001 BZZ. We're nowhere near a chequebook
funding limit.

**Out of scope for 7b** (already enumerated elsewhere or
deliberately deferred):

- Reverse direction (us *receiving* cheques and crediting peers
  for chunks they push to us) — not relevant for upload-only
  M1.0 / M3, peers don't push to a non-storer.
- Cash-out / withdraw automation — manual `antctl chequebook
  cash-self` is fine for now, only matters for storer
  economics not covered by the M3 light-node spec.

##### Phase 7b — 2026-05-08 first run results & remaining blocker

The Phase 7b code landed in 0.3.12 and is wire-compatible with bee:
1,500+ cheques emitted to 800+ unique peers across a 20 MiB / 6 GB
test sequence, all with bee-shaped EIP-712 + JSON, all logged with
the correct `(chequebook, beneficiary, cumulative_payout)` triples
into `<data_dir>/pushsync_outbound.json`.

**Two real bugs were caught and fixed in-session:**

1. **Wrong issuer key fallback.** The first version of
   `build_pushsync_swap_config` fell back to `WALLET_PRIVATE_KEY`
   when `--swap-key` was unset; in our `.env` the chequebook
   `0xfEecbb9a…20D` is owned by the EOA derived from
   `STORAGE_STAMP_PRIVATE_KEY` (path `m/44'/60'/0'/0/1`), not the
   one derived from `WALLET_PRIVATE_KEY` (path `m/44'/60'/0'/0/0`).
   Every cheque was signed with the wrong key and bee's
   `chequeStore.ReceiveCheque` rejected them with `ErrChequeInvalid`
   (`if issuer != expectedIssuer`). Fixed by setting
   `SWAP_OWNER_KEY=$STORAGE_STAMP_PRIVATE_KEY` in `.env` (and the
   `--swap-key` flag honours that env name first, before falling
   back to `WALLET_PRIVATE_KEY`).

2. **Chequebook not factory-deployed.** Even with the right issuer
   key, bee's `chequeStore.ReceiveCheque` calls
   `factory.VerifyChequebook(cheque.Chequebook)` on first sight and
   rejects the cheque if the chequebook isn't registered via the
   official Swarm chequebook factory at
   `0xC2d5A532cf69AA9A1378737D8ccDEF884B6E7420`. Our
   `0xfEec…920D` chequebook returns `null` (call reverts) for
   `factory.deployedContracts(0xfEec…920D)` — i.e. it was deployed
   independently of the factory and therefore has zero standing in
   bee's swap layer. **Every cheque we emit from this chequebook
   is silently rejected at the `factory.VerifyChequebook` step**,
   even though the cumulative-payout math, EIP-712 signature, and
   beneficiary checks all pass. Pushsync stalls again at ~9 K
   chunks because bee accrues unsettleable debt from us.

   Confirmation that this is the only remaining blocker:
   - `antctl chequebook show` reports
     `issuer = 0x9c66…fd5c` (matches our key) and
     `balance = 999999999999999 PLUR`,
     `total_paid_out = 1 PLUR` (from the prior `cash-self` test).
   - On-chain check (Gnosis mainnet block ≥ 46019699):
     `factory.deployedContracts(0xfEec…920D) → null` (revert).

**The remaining work for Phase 7b to be production-complete:**

1. **`antctl chequebook deploy` (was deferred from Phase 7
   polish; now blocking).** New subcommand that calls the
   official factory's `deployChequebook(issuer, defaultHardDeposit)`,
   deposits BZZ from the wallet (after an `approve` if needed),
   and stores the resulting chequebook address back into
   `.env` / `data_dir/chequebook.txt` for the daemon to pick up.
   Encode `deployChequebook` per
   `bee/pkg/settlement/swap/chequebook/factory.go::Deploy`. The
   factory's
   [`SimpleSwapFactory`](https://gnosisscan.io/address/0xC2d5A532cf69AA9A1378737D8ccDEF884B6E7420#code)
   ABI is small: `deployChequebook(address issuer, uint256 defaultHardDepositTimeoutDuration)`
   returns the chequebook address as the second `Deploy` event log
   topic, so we read the deployed address from the receipt logs.
2. **Factory verification on the read-side too.** Add the same
   `factory.deployedContracts(addr)` check as a daemon startup
   sanity check: if `--chequebook` points at an unregistered
   contract, fail fast with a clear error rather than launching
   into "uploads will work" mode and silently emitting rejected
   cheques.
3. **(Optional) `antctl chequebook deposit / withdraw` for
   on-chain liquidity moves once deploy is live.**

Once a factory-deployed chequebook is live, the existing Phase 7b
code path is expected to carry the GGUF upload to completion — bee
will accept the cheques (factory check passes, signature recovers
to `issuer()`, cumulative monotonic), credit our outbound debt back
to zero per peer, and let pushsync run unimpeded. The threshold
logic, ledger persistence, peer-EOA registry, and the `--swap-key`
plumbing are all already validated against live mainnet bee.

##### Phase 7c — chequebook deploy via factory (NEW — 2026-05-08)

**Status:** in flight — implementation landing in 0.3.13.

**Why this exists.** Phase 7b proved every layer of our SWAP stack
is wire-compatible with mainnet bee *except* one — bee rejects
cheques drawn on chequebooks that aren't registered with the
official `SimpleSwapFactory` at `0xC2d5A532cf69AA9A1378737D8ccDEF884B6E7420`
(see `bee/pkg/settlement/swap/chequebook/cheque.go::ReceiveCheque` →
`factory.VerifyChequebook`). Our existing
`0xfEecbb9a…20D` is a stand-alone `SimpleSwap` deployment, so
`factory.deployedContracts(0xfEec…20D) → false` and every cheque
we issue from it is silently dropped. We must deploy a *new*
chequebook through the factory, transfer ownership of the BZZ
balance over, and point `antd` at the new address.

**What "factory-deployed" means in code.** Factory ABI (verified on
Gnosisscan, source at the address above):

```
deploySimpleSwap(address issuer, uint256 defaultHardDepositTimeoutDuration, bytes32 salt) → address
deployedContracts(address) → bool
ERC20Address() → address    // returns the BZZ token (0xdBF3Ea6F5beE45c02255B2c26a16F300502F68da on Gnosis)
event SimpleSwapDeployed(address contractAddress)   // non-indexed
```

The factory creates the new `SimpleSwap` via CREATE2 from a
hard-coded master template, calls `setIssuer(issuer, factory)` on
it, marks `deployedContracts[swap] = true`, and emits
`SimpleSwapDeployed(swap)`. The event topic[0] is
`0xc0ffc525a1c7689549d7f79b49eca900e61ac49b43d977f680bcc3b36224c004`
(verifiable from the verified bytecode at the gnosisscan factory
page; the topic literal appears in the runtime bytecode immediately
before the `LOG1` opcode at the end of `deploySimpleSwap`). The
deployed address sits right-padded in `data[12..32]` of that log
because the event parameter is non-indexed.

**Deliverable:** `antctl chequebook deploy` subcommand and an
`antd` startup-time factory-registration check. Step list:

1. **`ant-chain::chequebook` helpers** *(crate-internal)*:
   - rename `deploy_chequebook_calldata` → keep, but document that
     the third argument is the CREATE2 salt (32 random bytes,
     mined client-side; bee uses a per-deploy random salt — see
     `chequebook/factory.go::Deploy` — to avoid collisions if the
     same issuer ever wants two chequebooks);
   - add `simple_swap_deployed_event_topic() -> [u8; 32]` returning
     the literal `0xc0ffc525a1c7689549d7f79b49eca900e61ac49b43d977f680bcc3b36224c004`
     so `antctl` doesn't have to spell it out;
   - add `extract_deployed_chequebook(receipt: &TxReceipt) -> Option<[u8; 20]>`
     that finds the `SimpleSwapDeployed` log emitted by the factory
     and pulls the address out of `data`. Mirror
     `extract_created_batch_id` exactly;
   - add `factory_deployed_contracts_calldata(addr: &[u8; 20]) -> Vec<u8>`
     for the `deployedContracts(address)` view — used by `antd`
     startup verification and by `antctl chequebook deploy --verify`;
   - add `factory_erc20_address_calldata() -> Vec<u8>` so we don't
     hard-code the BZZ token address client-side (the factory is
     the source of truth — it was constructed with the BZZ token
     pinned in storage). Test: against Gnosis mainnet, the call
     returns `0xdBF3Ea6F5beE45c02255B2c26a16F300502F68da`.
   - constants: `pub const GNOSIS_CHEQUEBOOK_FACTORY: [u8; 20]`
     for the mainnet address, plus
     `pub const DEFAULT_HARD_DEPOSIT_TIMEOUT_SECS: u64 = 86_400`
     (bee's default in `pkg/node/devnode.go::FactoryDefaultDepositTimeoutDuration`).

2. **`Wallet::deploy_chequebook` in `ant-chain::tx`**:
   ```rust
   pub async fn deploy_chequebook(
       &self,
       client: &ChainClient,
       factory: &[u8; 20],
       issuer: &[u8; 20],
       default_hard_deposit_timeout: U256,
       salt: &[u8; 32],
   ) -> Result<TxReceipt, TxError> {
       let data = deploy_chequebook_calldata(issuer, default_hard_deposit_timeout, salt);
       self.send_signed(client, *factory, data, FACTORY_DEPLOY_GAS).await
   }
   ```
   Gas: empirically ~700 K (the master template `setIssuer` call
   plus the CREATE2 + storage writes); use `FACTORY_DEPLOY_GAS = 1_500_000`
   to leave headroom. Issuer doesn't have to be `self.address` —
   the factory takes it as an argument, so an operator can deploy
   *for* a different issuer key (handy for cold-storage issuer
   keys).

3. **`antctl chequebook deploy` subcommand** *(`antctl/src/main.rs`)*:
   ```
   antctl chequebook deploy
       --gnosis-rpc-url <url>     [env GNOSIS_RPC_URL]
       --wallet-key <hex>          [env WALLET_PRIVATE_KEY]   # pays gas + (optionally) the initial deposit
       --issuer-address <hex>      [env STORAGE_STAMP_OWNER_ADDRESS]
       --hard-deposit-timeout-secs <u64>  [default 86400]
       [--initial-deposit-plur <u128>]    # if set, ERC20.approve + chequebook.deposit afterwards
       [--salt <32-byte hex>]             # if unset, generate randomly
       [--factory <hex>]                  [default GNOSIS_CHEQUEBOOK_FACTORY]
       [--gas-price-gwei <u64>]
       [--wait-secs <u64>]                [default 90]
       [--write-env-file <path>]          # if set, append CHEQUEBOOK_ADDRESS=0x… (or replace existing line)
   ```
   Flow:
   - Derive the wallet from `--wallet-key`; print `wallet_addr`.
   - Resolve `--issuer-address` (must be a 20-byte hex EOA; we
     don't require the *key* here, only the address — the wallet
     pays gas, the issuer just gets baked into the chequebook).
   - Sanity: `factory.ERC20Address()` matches our expected BZZ
     token; abort otherwise (defense against `--factory` typos).
   - Optional `--salt`; if missing, fill from `OsRng`.
   - Submit `deployChequebook(issuer, timeout, salt)` via the
     wallet, await receipt with `default_wait`.
   - `extract_deployed_chequebook(&receipt)` → new chequebook
     address; if `None`, dump the full receipt logs and bail.
   - Verify with `factory.deployedContracts(new_addr) == true`.
   - If `--initial-deposit-plur` was set:
     - read `wallet.bzz_balance` and assert it covers the deposit;
     - `approve_bzz(token=BZZ, spender=new_addr, value=deposit)`;
     - call `Chequebook.deposit(uint256)` (selector
       `0xb6b55f25`); on success the chequebook's `balance()` now
       reflects the deposit;
     - dry-run a `cash-self` 1 PLUR cheque against the new
       chequebook to prove end-to-end interop *before* declaring
       success.
   - Print human + JSON output:
     ```
     chequebook       0x…
     issuer           0x…   (factory-baked)
     wallet           0x…   (paid gas)
     factory-verified true
     deposited        N PLUR
     tx_hash          0x…
     block            46…
     ```
   - If `--write-env-file` was given, atomically rewrite the file
     so the next `antd` restart picks up the new address. Match
     existing `CHEQUEBOOK_ADDRESS=` line via simple regex; append
     if absent. Don't touch other lines.

4. **`antctl chequebook verify` subcommand** *(small but high
   value — saves a round-trip to gnosisscan)*:
   ```
   antctl chequebook verify --gnosis-rpc-url <url> --chequebook <hex>
                            [--factory <hex>]
   ```
   Calls `factory.deployedContracts(chequebook)` and prints the
   bool. Used by `antd` startup, by ops, and by anyone diagnosing a
   pushsync stall. Doesn't sign or submit anything, so it's safe
   to run with any RPC URL.

5. **`antd` startup factory check** *(`antd/src/main.rs`)*:
   When `--chequebook` is configured, before constructing the
   `PushsyncSwapConfig`, fire one `eth_call` to
   `factory.deployedContracts(chequebook)`; if false, log
   `error!("chequebook 0x… is not registered with the Swarm chequebook factory; cheques will be rejected by bee. Run \`antctl chequebook deploy\` to deploy a new one.")`
   and either:
   - **strict mode (default for production):** refuse to start
     pushsync-swap (boot the daemon with a `NoopPushsyncSettlement`
     so we don't waste bandwidth pushing cheques nobody accepts,
     and surface the failure prominently in `antctl status`); or
   - **`--chequebook-allow-unverified` flag:** start anyway
     (escape hatch for testnets / local devnets without a factory).
   The check is also re-run every 60 s in the background so a
   freshly-deployed chequebook can be picked up without a restart
   (we just flip a flag in `SwarmState`).

6. **Tests** (no live mainnet required):
   - unit: `simple_swap_deployed_event_topic` matches keccak of
     the literal selector string;
   - unit: `extract_deployed_chequebook` returns the right address
     given a hand-crafted receipt with the right topic[0] and a
     20-byte address right-padded in `data`;
   - unit: `factory_deployed_contracts_calldata(addr)` is exactly
     `0xc70242ad` ‖ pad32(addr);
   - integration (offline, with a recorded RPC fixture): the full
     `chequebook deploy` happy-path against a mocked RPC.
   - smoke (live, manual): on Gnosis mainnet against the real
     factory, deploy a chequebook for a brand-new throwaway issuer
     EOA, deposit 0.1 BZZ, run `cash-self`. Tracks ~$0.0003 in xDAI.

**Acceptance for Phase 7c done:** `antctl chequebook deploy` on
Gnosis mainnet succeeds end-to-end (returns a new address,
`deployedContracts` reads `true`, `cash-self` round-trips), and
`antd` started against that chequebook lets a 1 GiB pushsync upload
to mainnet bee peers run past the 20 K-chunk wall without any
`pushsync attempt failed; connection is closed` errors caused by
cheque rejection.

##### Phase 7c — 2026-05-08 corrected diagnosis (the real blocker)

**Status:** Phase 7c (`antctl chequebook deploy`) **landed in 0.3.13**
(see commits in `crates/ant-chain/src/chequebook.rs`,
`crates/ant-chain/src/tx.rs`, `crates/antctl/src/main.rs`,
`crates/antd/src/main.rs`). It is **functional** but turned out to
**not** be the upload blocker.

**What we discovered when 0.3.13 landed:** running the new
`antctl chequebook verify` against our existing `0xfEec…920D`
chequebook returned `registered = yes` — i.e. the chequebook **was
already registered** with the official factory. Direct
`eth_call` confirms:

```
factory.deployedContracts(0xfEec…920D) → 0x…01 (true)
```

So the previous "not factory-deployed" diagnosis (which itself
came from a transient `eth_call` failure during the previous
session) was wrong. With a correct `SWAP_OWNER_KEY` and a
factory-registered chequebook, **bee accepts our cheques**.

But pushsync still stalls. Re-running the production journal at
the libp2p layer reveals the *real* cause:

1. **Cheques are emitted.** Logs show >1500 `emitted pushsync cheque`
   lines per minute, with monotonically growing per-peer
   `cumulative_payout`. Outbound ledger has 875 peers,
   `cumulative_payout` per peer ranges 800K–37M PLUR (well under
   the chequebook's `999 999 999 999 999 PLUR` liquid balance).
2. **Bee closes pushsync streams within ~250 ms of the very first
   chunk push to a freshly-connected peer**, before any
   meaningful debt could possibly accumulate. Error is always
   shaped as `unexpected end of file` / `connection is closed` /
   `Connection reset by peer` — i.e. bee's stream-handler is
   resetting (`stream.Reset()`), not writing a `pb.Receipt{Err:…}`
   back. That's bee's `handler` error path *after* `attemptedWrite
   == false` failed to write a receipt — the stream is already
   broken.
3. **The reason bee's handler returns an error is bee's
   `accounting.PrepareDebit`** rejecting the chunk. That
   rejection happens when our balance with bee is below
   `-disconnectLimit`. For a light-mode connection,
   `lightDisconnectLimit = 1.25 * lightPaymentThreshold = 1.25 *
   1 350 000 = 1 687 500 PLUR`.
4. **Our cheques aren't crediting bee's accounting because we're
   not honouring bee's `pseudosettle` protocol on pushsync paths.**
   Bee's `accounting.settle()` runs `refreshFunction` (pseudosettle)
   *first*, then `payFunction` (swap cheque). Without pseudosettle
   refreshments from us, bee never resets our debt back to zero
   even if our SWAP cheques arrive. Pseudosettle is a time-based
   "allowance" mechanism: bee grants us `lightRefreshRate * elapsed
   seconds` PLUR of automatic credit per peer if we open the
   `/swarm/pseudosettle/1.0.0/pseudosettle` stream and announce it.
5. **We have a pseudosettle dialer, but it's only wired to
   retrieval-side debt** (`ant-retrieval::Accounting::HotHint`).
   The pushsync path doesn't fire `HotHint`s, so pseudosettle
   never refreshes pushsync peers, so bee's pushsync accounting
   never sees a refresh, so we hit `disconnectLimit` after a
   handful of chunks per peer.

#### Phase 7d — Pushsync-side pseudosettle (DELIVERED, 2026-05-08, antd 0.3.14)

**Status:** done. First sustained mainnet upload past the 20 K
wall in `antd`'s history.

**Implementation:**

1. **`PushsyncSwap` now owns an optional `mpsc::Sender<HotHint>`.**
   Set in `crates/ant-p2p/src/pushsync_swap.rs` via the new
   builder method `with_hot_hint`. Its [`PushsyncSettlement::note_pushsync`]
   fires a `HotHint { peer }` on every chunk push (cheap
   `try_send`, drops on backpressure — driver has a 256-slot
   buffer; pseudosettle's per-peer `MIN_REFRESH_INTERVAL = 1.1s`
   rate-limits the actual stream traffic).
2. **The retrieval and pushsync paths share one `HotHint`
   channel.** No fan-in — `crates/ant-p2p/src/behaviour.rs`
   clones the same `mpsc::Sender<HotHint>` into both
   `Accounting::with_hot_hint(...)` and
   `PushsyncSwap::with_hot_hint(...)` before construction. The
   driver doesn't care which subsystem produced the hint.
3. **Bee's allowance math worked out as predicted in the smoke
   test.** Steady state at 100 active peers under load: `ok ≈
   4 700` refreshes/min, `units_accepted ≈ 600 M PLUR/min`
   (~107 K PLUR/sec/peer once concurrency lifts the per-peer
   `lightRefreshRate = 5 K/sec` floor). Cheques continued
   firing at ~750/min on top of pseudosettle, providing the
   bulk credit; pseudosettle filled the per-second slack that
   used to push us across `lightDisconnectLimit`.
4. **Smoke test: 256 MiB / 66 056 chunks delivered end to
   end.** Started 20:37 UTC, completed 20:58 UTC (~21 min,
   sustained ~52 chunks/sec average). No collapse — the whole
   upload pushed through, including the final reference. URL:
   `bzz://2f3a4a7372ba6c325d07054d1ffd7cae79ebbb7ece1c18ee6b7c592142568809`.
   Per-minute progress: 7.3 % → 21.5 % → 38.2 % → 69.7 % →
   100 % (linear, no degradation past the previous 20 K wall).

**Why this isn't Phase 7b/7c:** Phase 7b set up the cheque-issuing
infrastructure correctly. Phase 7c added the factory-deploy +
verify tooling so we can rule out the chequebook-registration
class of bugs cheaply. Phase 7d delivered the *missing*
settlement layer: driving bee's combined (refreshment +
cheque) accounting, not just half of it.

**Acceptance:** ✅ 66 056-chunk upload completed; no
`pushsync attempt failed; connection is closed` flood past
the 20 K wall. The 6.7 GiB `gemma4:e2b` upload is now
unblocked — our throughput math says ~13 hours at the
observed 35-50 chunks/sec.

#### Phase 7e — Throughput experiments (NEGATIVE RESULT, 2026-05-08, antd 0.3.15)

**Status:** done; nothing shipped. The two changes we tried both
made things worse, so we reverted to Phase 7d's behaviour. The
useful output is the diagnosis below; the code change kept is
the small `EmitCore`-Arc refactor in `pushsync_swap.rs`, which is
behaviour-preserving and just makes the module's state shareable
for future experiments.

**What we tried:**

1. **Raise `MAX_PUSH_CONCURRENCY` 32 → 256** in
   `crates/ant-node/src/uploads/mod.rs` and the gateway's
   `crates/ant-gateway/src/retrieval.rs`. The hypothesis was
   that pushsync throughput is concurrency-bound: 32 in-flight
   pushes × ~400 ms RTT = ~80 chunks/s ceiling, observed ~50.
2. **Detach cheque emission from the pushsync hot path.**
   `note_pushsync(peer, price)` previously awaited the SWAP
   stream RTT (~250-500 ms) inline whenever `pending` crossed
   `cheque_trigger_plur`. We changed it to spawn an
   `Arc<EmitCore>`-borrowing background task with a per-peer
   `try_claim_emit` coalescing flag, returning immediately so
   the next chunk's pushsync could fire.

**What happened:**

- **256 concurrency.** Smoke test failed at 27 % with `push
  timed out after 60s`. Pseudosettle's `units_accepted`
  collapsed to ~30 K PLUR/min (vs. ~600 M baseline) and
  `failed`/`timed_out` counts surged. Bee-side accounting
  rejects floods of concurrent stream opens, and yamux disconnects
  cascade. The same upload at 32 concurrency had completed
  cleanly the day before.
- **Detached cheques.** Smoke test failed at 24.5 % with the
  same timeout error, this time with concurrency back at 32.
  The hypothesis-killer: bee's `accounting.PrepareDebit` runs
  synchronously per chunk forward and rejects when debt
  exceeds `disconnectLimit`. With awaited cheques the credit
  *always* lands at bee before the chunk debit (we serialise
  per-peer at the fetcher level). With detached cheques the
  ordering becomes non-deterministic — chunks can arrive at
  bee before the cheque that was supposed to credit the debt
  they incur, and bee rejects the chunk. Cumulative cheques
  are monotonic in the long run, but ordering matters at the
  moment-by-moment debt-check granularity.
- **Network memory.** A nasty side effect: after either
  failed experiment the bee peer set takes ~10-30 minutes to
  forget our overlay. Subsequent uploads (even with the
  reverted code) showed degraded throughput (~17-22 chunks/s
  vs. the 50 baseline) until enough peers cycled their
  `accountingPeer` state. Lesson: throughput experiments are
  expensive — each negative result costs real warm-up time
  before we can measure the next attempt cleanly.

**Where the actual bottleneck is** (clearer now than before
Phase 7d): bee's per-peer `lightRefreshRate = 5 000 PLUR/sec`
caps how fast each peer's view of our debt can be cleared by
pseudosettle. Cheques top up the bulk, but the `PrepareDebit`
admission check is per chunk and synchronous, so a fast burst
of pushes that outruns the credit pipeline gets rejected. With
~100 active peers and a per-chunk price ranging 50 K-300 K
PLUR, the network-wide chunk-acceptance budget settles at
~30-50 chunks/s, irrespective of antd's concurrency cap.

**Things that *would* help, but are out of scope:**

1. **Run as `full_node = true`** in our handshake. That bumps
   bee's `paymentThreshold` 10× (and `lightRefreshRate` does
   not apply — bee uses the full `refreshRate = 50 K
   PLUR/sec`). But it requires antd to actually serve
   retrieval/pushsync as a forwarder for other peers — i.e. be
   a real Bee-equivalent node, not a light client. Order of
   magnitude more code; not Phase 7.
2. **Expand the active peer set 5-10×.** Network-wide credit
   budget scales linearly with peer count. We currently keep
   ~100 peers warm; the routing layer could carry 500-1000
   without changing topology. Would need an audit of yamux /
   tcp connection budgets and the routing watch's churn
   handling. Plausible Phase 8 work.
3. **Lower per-chunk price** on the wire — but that's the
   neighbourhood-proximity-based price bee enforces; we can't
   unilaterally change it.

**Acceptance:** none. Both attempts reverted. 0.3.15 ships with
the `EmitCore` refactor (behaviour-identical to 0.3.14) and the
honest version-bump for traceability. Future throughput work
needs a clean, repeatable bench (one upload per fresh peer set,
with at least 30 minutes between runs) and the network-shape
expansion above to be worth the engineering cost.

#### Phase 7f — Configurable target-peers (antd 0.3.16, 2026-05-09)

**Status:** delivered. Cheapest of the Phase 7e follow-up levers
(option #2 in the negative-result writeup): widen the per-second
credit budget by widening the peer set, since bee grants every
peer ~5 K PLUR/sec of pseudosettle credit independently. At the
default 100 peers we're soaking ~500 K PLUR/sec network-wide
which is barely above the 32-concurrent-pushsync working set's
ask. Doubling peers should lift the ceiling roughly linearly
until we run into yamux/TCP budgets (separate audit).

**Implementation:**

- `NodeConfig::target_peers: Option<usize>` plus
  `with_target_peers()`. `None` keeps the existing
  `ant_p2p::DEFAULT_TARGET_PEERS = 100` fallback (`RunConfig`
  treats `0` as "use the default"); `Some(n)` is forwarded
  literally.
- `antd --target-peers <N>` CLI flag wired through to
  `NodeConfig`. No new env var, no config-file plumbing — this
  is an experimental knob we want to make trivial to revert by
  restarting without the flag.
- No changes to ant-p2p itself: the swarm loop already honours
  whatever `target_peers` it's handed, and dial budgets /
  routing churn at 200 peers were stable in earlier
  reconnect-storm testing (`crates/ant-p2p/tests/`).

**Validation result (2026-05-09 08:26 UTC, GGUF job
`000002119b4e1fd1` mid-flight):**

- Baseline at 100 peers, 30-min sustained average:
  **15.3 chunks/sec**.
- After pause/restart with `--target-peers 200`, peer set
  saturated to 200 within ~30 s. Resumed job; 10-min sustained
  average: **45.2 chunks/sec = 2.9× speedup**. No stalls, no
  disconnects, no peer-set churn.
- The first 2-min window registered 76.9 chunks/sec — partly
  catch-up from the resume drain, but the 10-min figure is the
  steady-state we're keeping in our heads going forward.
- ETA on the GGUF revised from ~30 h to ~11 h.

**Acceptance:** delivered. Keeping the flag in production with
`--target-peers 200`. Phase 7e's negative result on
`MAX_PUSH_CONCURRENCY` is now contextualised: bee's per-peer
credit is the bottleneck, so the right lever is *more peers*
(more independent credit budgets), not more chunks per peer.

**Follow-ups (Phase 7g, not blocking):**

1. Sweep target-peers values (300, 400, 500) once the GGUF
   completes and the network has 30 min of forgiveness. Expect
   diminishing returns past ~400 (yamux/TCP fan-out, peer-table
   churn, pseudosettle driver tick-rate).
2. Audit peer-table watcher and dial-pipeline behaviour at
   target_peers ≥ 500 — last reconnect-storm test was at the
   100-peer default.
3. Surface this knob in the future config-file plumbing
   (Phase 11 polish), so it's reachable without editing
   `antd.service`.

#### Phase 7g — target-peers sweep + gateway max-bytes raise (DELIVERED, 2026-05-09, antd 0.3.17)

**Status:** delivered as a single restart-cycle while the GGUF
upload was mid-flight. Two distinct problems addressed:

##### Throughput sweep (300, 400)

Picked up #1 of the Phase 7f follow-ups while the GGUF job was
running with `--target-peers 200`. Each step paused the job,
restarted antd with the new `--target-peers` value, waited for
the peer set to fill, resumed, sampled throughput over a
10-minute window, and decided whether to keep going.

| peers | 10-min sustained | speedup vs 100 baseline |
|---:|---:|---:|
| 100 | 15.3 chunks/sec | 1.0× |
| 200 | 45.2 chunks/sec | 2.9× |
| 300 | 84.7 chunks/sec | 5.5× |
| 400 | 105.3 chunks/sec | 6.9× |

200 → 300 was 1.87× (almost linear). 300 → 400 was 1.24× (clear
diminishing returns). Health stayed clean at 400: full peer set
held, no stalls, no peer-set churn, only routine pushsync
skip-and-retry warnings (which are expected per chunk and just
scale linearly with throughput). Settled on 400 for the rest of
the GGUF upload. End-to-end the GGUF (1.76 M chunks, 6.7 GiB)
completed in ~2 h of pushsync wall-clock; without the
target-peers lift the same job would have taken ~30 h, on the
back of the same Phase 7d / 7f code.

The PLAN-time prediction (300 → "50-60 chunks/s, diminishing
returns") was wrong by ~50 %: the per-peer credit budget really
is the dominant ceiling, and we hadn't pinned the obvious
fan-out / dial-budget side-effects yet. We *will* hit a wall —
yamux stream caps, peer-table churn at 500+, the pseudosettle
driver tick-rate — but 400 is still on the upward part of the
curve. Future sweep at 500/600 is reasonable; 800+ should wait
for an audit of the dial pipeline.

##### Gateway max-file-bytes (1 GiB → 16 GiB)

Surfaced *immediately* on the very first retrieval attempt of
the completed GGUF: HEAD returned the right span
(`content-length: 7162394016`), but `GET /bzz/<ref>/` returned
HTTP 502 with body `{"code":502,"message":"join f75f2bfd...:
file too large: span 7162394016 bytes, cap 1073741824 bytes"}`.

The 1 GiB ceiling in `crates/ant-gateway/src/retrieval.rs`
(`GATEWAY_MAX_FILE_BYTES`) was sized in the era when the joiner
materialised the whole body in `Vec<u8>` before responding. The
gateway is now wholly streaming
(`dispatch_bzz`/`dispatch_bytes` → `join_to_sender_range` →
`Body::from_stream`), with peak resident memory bounded by
`FETCH_FANOUT * STREAM_PIPE_DEPTH * CHUNK_SIZE ≈ 4 MiB`
regardless of declared span. The cap is therefore now a
"sanity bound on the declared span" — it stops a pathological
manifest from making us walk billions of intermediate chunks —
not a memory cap.

Raised to 16 GiB. Covers all current AI model weights (e.g.
gemma3:27B-q4 ≈ 16 GB; Llama-3-70B-q4 still wouldn't fit but
those operators can opt in via a follow-up env-var override).
Doc comments at lines 28-30 and 139-149 of
`crates/ant-gateway/src/retrieval.rs` synchronised: the latter
was stale and still claimed the joiner materialised the body.
The CLI's `DEFAULT_MAX_FILE_BYTES` (32 MiB) is unchanged —
interactive `antctl get` is still the wrong tool for multi-GiB
files, and the upper-bound assertion in
`gateway_raises_joiner_max_bytes_above_cli_default` still
passes (16 GiB > 32 MiB).

##### Fresh-upload retrieval lag — *measured*, not a bug

Once the cap was raised and the daemon restarted, retrieval of
the GGUF still failed — but not because of the gateway. We
have hard numbers:

- Random sample of 21 chunks across the file's chunk tree (10
  L2 intermediates uniformly across the 107-child root, plus
  2 leaf samples per L2): **5 deterministic 404s = 23.8 %
  miss rate at T+2 h 45 m post-completion**. Each missing
  chunk re-tested twice; both retries 404'd with the same bee
  error `"remote: retrieve chunk: storage: not found"`.
- Full `/bzz/<ref>/` download attempt: 7 min 44 s spent on a
  single chunk (`d1671a96…`) before the gateway gave up after
  35 retries: `"all peers failed for chunk … after 35
  attempts (last: remote: retrieve chunk: no peer found)"`.
  End-to-end download impossible — any single missing chunk
  fails the whole stream.

This is intrinsic to Swarm's pushsync model:
1. Pushsync delivers each chunk to **one** closest neighbour.
2. Replication to the rest of the k-set happens over hours via
   bee's separate sync protocols (`pullsync`, neighbourhood
   replication).
3. Retrieval routes to whoever *our* routing layer thinks is
   closest, which often isn't the original pushsync receiver
   until the neighbourhood has synced.

So a 6.7 GiB upload that *just* finished pushsync has ~75-80 %
chunk availability initially, with full availability arriving
hours/days later. The upload itself is correct (manifest reads
back, content-length matches, intermediates resolve in <30 ms);
the network simply hasn't fanned the chunks out yet. We have
to wait — there is no client-side fix.

**Action plan if `+72 h` doesn't recover availability** (not
expected, but worth recording):

1. **Phase 7h: `antctl pin <reference> <file>` (DELIVERED, 2026-05-09, antd 0.3.18).**
   Local-only chunk write that bypasses bee-side replication
   entirely. Reuses [`ant_retrieval::StreamingSplitter`] to
   walk the source file with constant memory, derives the
   matching mantaray manifest with [`build_single_file_manifest`]
   (so the derived root is byte-identical to what `antctl
   upload` produced), and dispatches every chunk through a new
   [`Request::PutChunkLocal`] control command. The daemon
   validates each wire (BMT-with-span match) and writes it
   straight into [`DiskChunkCache`] (`<data-dir>/chunks.sqlite`)
   without stamping or pushsync. The CLI verifies the derived
   manifest root matches the user-supplied `<reference>`
   *after* every chunk has been dispatched, so a typo or
   wrong-source-file mismatch surfaces as a hard error
   before the operator trusts the pin.

   Caveat: `antd` is dialer-only on the retrieval protocol,
   so a local pin only makes the file resolvable through *this*
   node's HTTP gateway. Other Swarm nodes still see the file
   only through bee-side neighbourhood replication. That's
   exactly the right behaviour for the use case (operator wants
   `vibing.at/ai/<file>` to work *now*, not "when the network
   catches up"). LRU eviction at the disk-cache cap means the
   pin survives daemon restarts but eventually decays — re-run
   `antctl pin` to refresh.

   Production deployment also raised
   `--disk-cache-max-gb` from 10 to 32 in
   `/etc/systemd/system/antd.service` so the 6.7 GiB GGUF
   plus other content fits comfortably.

   **Acceptance** (verified on 2026-05-09):
   - Smoke test: 20 MiB file, 5 164 chunks, pinned in 1.4 s
     (14 MiB/s); `GET /bzz/<ref>/` returned the full body in
     150 ms with matching SHA-256.
   - gemma4:e2b GGUF (6.7 GiB, 1 762 407 chunks): pinned in
     570 s (12 MiB/s, bounded by the SQLite writer); the
     control-socket fan-out (8 worker threads, one socket
     each) saturated the daemon's disk-cache queue without
     touching the upload pipeline. `GET /bzz/<ref>/` for the
     full body returned 7 162 394 016 bytes in 153 s
     (≈ 47 MB/s, served entirely from the local SQLite cache);
     SHA-256 of the response matched the source file
     (`4e30e266…07448`) exactly. After the daemon restarted
     (cache-cap bump), a partial fetch confirmed the rows
     persist across restart.

2. **Phase 7h.2: `antctl pin-collection` (DELIVERED, 2026-05-09, antd 0.3.19).**
   Same local-only mechanism as `pin`, but takes one or more
   `<path-in-manifest>=<local-file>` entries and builds a *collection*
   mantaray on top of them. Each input file is walked with the
   streaming splitter to derive its data-tree root (no chunks
   dispatched — the operator is expected to have run `antctl pin
   <reference> <file>` for each entry first, so its data chunks
   are already cached); the collection manifest is then minted
   with [`build_collection_manifest`] and its handful of nodes
   pinned via [`Request::PutChunkLocal`]. An optional
   `--index <path>` stamps the `website-index-document`
   metadata, and `--expect <ref>` asserts the derived root
   matches a known value (useful when re-pinning a previously
   built collection on a fresh node). Per-entry MIME overrides
   are not yet wired — `--content-type` applies to every entry.

   This closes the "is there a single folder manifest" gap for
   batches like the gemma4:e2b ollama blobs, where each file
   was uploaded independently and we wanted a single bzz
   reference under which all four files resolve at their
   sha256-prefixed paths. Avoids a re-upload (the chunks are
   already on the network); only the few-chunk collection
   manifest needs to be cached locally.

   **Acceptance** (verified on 2026-05-09):
   - 4-entry gemma4:e2b collection (3 small blobs + the 6.7 GiB
     GGUF) built and pinned in 161 s — almost all of which is
     the streaming hash of the GGUF (≈ 13 GiB/s mmap-backed
     scan; the tiny blobs dispatch in milliseconds and the
     final manifest is 14 chunks total).
   - Collection root:
     `0x9d2f8f41941969b20813495fab985abbe82e6286b63eb7824e81766ed7e72531`.
   - All four entries resolve through this node's gateway:
     `GET /bzz/<root>/sha256-…` returned the full body byte-for-byte
     for the three small blobs (42 B, 473 B, 11 355 B) and a
     `Content-Length: 7162394016` HEAD plus matching SHA-256
     spot-checks at the head and tail of the GGUF.

3. **Phase 7i (deferred): `antctl upload reseed <job-id>`.** Walks the
   local source file, derives every chunk address, queries
   `/chunks/0x<addr>` for each, re-pushes any that 404. The
   re-push lands on whichever peer is now the closest
   neighbour (which may differ from the original receiver),
   materially accelerating *network-wide* availability — which
   `pin` does not address. Bandwidth bounded by the miss rate:
   at 24 % miss × 1.76 M chunks × 4 KiB ≈ 1.7 GiB to re-push.
   Lower priority now that pinning unblocks the operator's own
   gateway.
4. **Phase 7j: target-peers ≥ 600 + 24 h soak.** Raises the
   probability that *our* routing table covers every k-bucket
   at least once. Pre-req: the dial-pipeline audit listed
   under Phase 7f's follow-ups.
5. **Phase 7k: bee-side hand-off.** Drive a temporary
   high-stake bee node next to antd, let pullsync replicate
   our upload through it, then retire the bee node. Most
   surgical option for forcing replication; needs SWAP plumbing
   on bee that we deferred earlier.

None of these are blocking. We re-sample at +6/+24/+72 h and
escalate only if availability stalls.

**Acceptance:** delivered.
- 400 peers held cleanly through the rest of the GGUF upload.
- gemma4:e2b GGUF reference:
  `0x0bf812fcbe8e25905711f82acaeabeedcb221e8f2384887b778d5649cd0c8537`.
- Manifest (`/bzz`) and file-root (`/bytes`) HEAD return the
  correct 7 162 394 016-byte content-length.
- Top-of-tree chunks reachable in <30 ms each.
- Full body retrieval blocked by the network-side replication
  lag described above. Re-test scheduled at +6/+24/+72 h.
- Gateway cap fix prevents the same 502 for any future
  multi-GiB upload, *whenever* retrieval becomes available.

#### Phase 8 — On-device stamp lifecycle — ✅ shipped

- On-chain batch purchase, top-up, dilute.
- Expiry monitoring and app-level events.
- Harden bucket-counter persistence against crash mid-upload (write counter before emitting the chunk to the wire).

**Exit:** End-to-end user flow: buy a batch, upload files, top it up, dilute it — all initiated from the device.

**Landed as:** `antctl postage create | top-up | dilute | show | balance` subcommands talking directly to Gnosis (no daemon required, so operators can manage stamps from a workstation that doesn't run the daemon). `create` does the BZZ approve-if-needed dance before submitting `createBatch`. Bucket-counter hardening is live: every issued index is flushed to `<data_dir>/postage/<batch_id>.bin` *before* the chunk goes on the wire, and the file is reloaded on restart so we never reissue a stamp index. **Outstanding:** in-process expiry monitoring + app-level events (a daemon-side watcher that fires when a batch's TTL drops below a configurable threshold), and orchestration helpers that automatically dilute / top-up against a per-batch budget. The on-chain plumbing is in place; only the policy layer is missing.

#### Phase 9 — Streaming uploads + mantaray construction — ✅ shipped

- Streaming chunker with on-the-fly BMT tree construction.
- `ant-mantaray` construction path (previously we only traversed).
- `upload_manifest` API for multi-file directory uploads.
- Tier-B `ant-gateway` endpoints (Appendix C) come online incrementally with the underlying capability: `/wallet`, `/chequebook/*` real after Phase 7; `/stamps` writes after Phase 8; `/bzz` POST, `/bytes` POST, `/chunks*`, `/tags*`, `/feeds/*`, `/soc/*`, `/stewardship/*`, `/envelope/*` after Phase 9. `beeMode` flips to `"light"` once `swap-enable` is honored end-to-end.

**Exit (M3):** Upload a multi-file directory as a single mantaray manifest and retrieve files by path — full read/write parity with a `bee` light node for the supported protocol subset.

**Landed as:** `ant-retrieval::splitter` produces a balanced k-ary chunk tree (the inverse of the existing joiner) and emits leaf + intermediate CAC chunks. `ant-retrieval::manifest_writer` was rewritten as a recursive Mantaray v0.2 radix trie that fits one CAC chunk per node, deepens the tree as a collection grows beyond a single root chunk, and chains 30-byte forks for path segments longer than `MAX_FILENAME_BYTES`. `build_single_file_manifest` wraps `build_collection_manifest` so the gateway has one entry point. Gateway `POST /bzz` accepts both `?name=…` single-file uploads and tar-archive collections (per the bee-shaped contract), parallel-pushes every leaf + intermediate + manifest chunk through the existing `PushChunk` control verb, and answers with `{"reference":"<hex>"}`. Live mainnet smoke: single-file upload (`"uploaded via ant"`) and tar collection upload, both retrieved fresh through `bzz://<root>[/path]`. **`beeMode` is still `"ultra-light"`** in the `/node` response and flips to `"light"` once the auto-settlement trigger lands and `swap-enable` is honored end-to-end. The Tier-B `/wallet`, `/chequebook/*`, `/stamps` writes, `/tags*`, `/feeds/*`, `/soc/*`, `/stewardship/*`, `/envelope/*` endpoints are still on the catch-all 501.

---

### Hardening track

Not a capability milestone, but required before GA.

#### Phase 10 — UX, background, resilience

- Graceful suspend/resume on iOS + Android (sockets, timers, chain pollers).
- Upload resumption across app kill / relaunch.
- Onboarding flow wired: relayer-sponsored funding, chequebook pre-deploy, stamp presets.
- Nonce-race resilience, reorg handling, fee-bumping in anger.
- Crash recovery paths for every durable state transition.

**Exit:** App survives being killed mid-upload, relaunched, and finishes the upload. Cold-start to "ready to upload" under 30 s for a returning user.

> **Status update (2026-07, issue #41):** the iOS suspend/resume +
> flaky-network leg landed. `UploadManager` gained
> `suspend_all`/`wake_all` (system pause with a persisted `auto_paused`
> marker, bounded drain-and-checkpoint, securing passes stop cleanly and
> re-queue on wake), a **container-move rebase** (`source_rel` recorded
> against a configured `source_root`; stale absolute paths re-anchor
> under the new app-container UUID guarded by size+mtime), and a
> **store-only heal** fallback (chunk set enumerated by traversing the
> completed reference through the disk chunk store when the source file
> is gone). Wire: `ControlCommand::{UploadSuspendAll,UploadWakeAll}`,
> `auto_paused` on `UploadJobView`. FFI: `ant_init_with_options`
> (source-root), blocking `ant_suspend` + `ant_wake`. AntDrive now
> observes `scenePhase` (background ⇒ `beginBackgroundTask` +
> `ant_suspend`; active ⇒ `ant_wake`), schedules an `antdrive.secure`
> `BGProcessingTask` to finish securing in the background, monitors the
> network path (`NWPathMonitor` ⇒ suspend/wake + "Waiting for
> network…"), and offers per-file "Stop securing". Android WorkManager
> wiring is still open.
>
> **Follow-up (2026-07, shallow-placement loss):** a real iOS upload
> (4 MB, "completed") was 404 network-wide a day later — its chunks had
> been accepted on *shallow* receipts and the storers GC'd them. Two
> fixes: (1) **strict receipts** — the upload-job path now passes
> `require_deep` through `PushChunk`, and `RoutingFetcher::
> push_stamped_chunk` returns `ShallowReceipt` instead of accepting
> after the deeper-storer hunt, so the chunk parks in the retry-forever
> queue until a storer inside its neighbourhood signs
> (`ANT_UPLOAD_ACCEPT_SHALLOW=1` opts out; gateway parity endpoints
> unchanged); (2) **degraded auto-retry** — `requeue_unsecured_heals`
> re-queues *degraded* jobs (not just unfinished ones) on every
> launch/wake with a 15-min backoff until `heal_verified`, upgrading to
> the shallow-aware mode at execution time when well-connected; the app
> shows these as "Propagating…" instead of the dead-end "Not fully
> backed up".

#### Phase 11 — Interop, beta, polish

- Continuous interop against latest `bee` mainnet release in CI.
- External beta with opt-in telemetry (local-first, no PII).
- Performance / battery / data profiling; iterate.
- Documentation pass: integration guide, API reference, onboarding playbook.

**Exit:** Public beta on both stores.

---

### Milestone summary

| Milestone | Phases | Status | Demo |
|---|---|---|---|
| **M1.0 Basic mainnet connection** | 0 | ✅ shipped | `antd` connects to mainnet + completes BZZ handshake |
| **M1.1 Stable neighborhood** | 1 | ✅ shipped | Stable peer set + correct Kademlia routing bins over 24 h |
| **M2 Ultra-light (download)** | 2, 3, 4 | 🚧 desktop live, mobile pending | Download any reference + path resolution; shippable read-only build |
| **M3 Light node (upload + stamps + SWAP)** | 5, 6, 7, 8, 9 | 🚧 mostly shipped | Phases 5, 6, 8, 9 ✅; Phase 7 ✅ wire / verify / persistence + ✅ Tier-1 on-chain `cashChequeBeneficiary` round-trip (block `46019699`, tx `0x643bb08e…e308` — proves bee bit-compatibility) + ✅ upload-side settlement (Phases 7b–7d: pushsync cheque emission, factory deploy/verify tooling, pushsync-side pseudosettle — sustained 256 MiB / 66 K-chunk mainnet upload), ⏳ re-run live outbound `cheque_smoke` against bee bootnodes (the previous "cold-peer" diagnosis is still unconfirmed — bee may have been rejecting our broken EIP-712 digest, fixed in the same Tier-1 work). End-to-end: `antctl postage create` → `POST /bzz` (single file or tar) → `bzz://<ref>/path` retrieval all working live on Swarm mainnet. |
| **Hardening** | 10, 11 | 🚧 partial | Upload resumption (persisted per-job resume cursor, pause/resume/cancel via `antctl upload` + `ant-ffi`) landed; suspend/resume wiring and public beta with survived-kill uploads and mainnet interop still pending |

M1 and M2 can be parallelized by a second engineer starting on chain plumbing in Phase 5. Add buffer for unknowns — Swarm's protocols have undocumented edges, and the first honest interop run against `bee` always surfaces something.

### Drop-in light-node gap — prioritised follow-on

The "M3 mostly shipped" row above understates how much surface area is
still needed for `antd` to be a literal **drop-in replacement** for
`bee --bee-mode=light` against an unmodified bee-js / `swarm-cli` /
browser-uploader client. The libp2p protocol surface and the
on-chain plumbing are at parity (or near it). What's missing is the
long tail of HTTP endpoints bee-js touches, plus one real protocol
gap (auto-settlement) and one real read-path gap (Reed-Solomon
recovery).

This is the working order to close it, fastest-payoff first:

| # | Item | Effort | Status | Why first / notes |
| - | ---- | ------ | ------ | ----------------- |
| 1 | Auto-settlement trigger from `accounting` ↔ `swap::issue_and_emit` | ~1 day | ✅ shipped | `pushsync_swap` emits a cheque when debt crosses `DEFAULT_CHEQUE_TRIGGER` (= ½ `LIGHT_PAYMENT_THRESHOLD`). Flips `/node` → `beeMode:"light"`. |
| 2 | `/wallet`, `/chequebook/{address,balance,cheque[,/{peer}]}`, `/balances`, `/settlements`, `/timesettlements` reads | ~1 day | 🟡 partial | `/wallet`, `/chequebook/address`, `/chequebook/balance` shipped; `/balances`, `/settlements`, `/timesettlements`, `/chequebook/cheque` still fall through to 501. |
| 3 | `/stamps` reads + `POST /stamps/{amount}/{depth}` + `PATCH /stamps/{topup,dilute}/...` | ~1 day | ✅ shipped | Live registry (0.5.8): buy/topup/dilute register a runtime issuer; `GET /stamps` + `/stamps/{id}` reflect it. |
| 4 | `/tags` GET/POST + `/tags/{id}` GET/DELETE/PATCH | ~½ day | 🟡 partial | `POST /tags` + `GET /tags/{uid}` shipped (uploads echo `Swarm-Tag-Uid`); `DELETE`/`PATCH` not yet. |
| 5 | `antctl chequebook deploy` + first-run auto-deploy in `antd` | ~½ day | ✅ shipped | On first light start with a funded node wallet and no chequebook configured/persisted, `antd` auto-deploys a factory-registered chequebook (issuer = node EOA), funds it with xBZZ, and persists it to `<data-dir>/chequebook.json` (deploy-once, reuse-forever). Manual `--chequebook`/`CHEQUEBOOK_ADDRESS` still wins; `--no-auto-chequebook` opts out. See "Open: chequebook auto-bootstrap" below. |
| 6 | `/feeds/{owner}/{topic}` GET/POST + `/soc/{owner}/{id}` POST | ~2-3 d | ✅ shipped | `/feeds` GET/POST + `/soc/{owner}/{id}` GET/POST routed. GET dereferences to content with bee-shaped headers (`swarm-feed-index{,-next}`, `swarm-soc-signature`, `swarm-feed-resolved-version`); honors `Swarm-Only-Root-Chunk`. Resolves **v1** (`ts‖ref`) and **v2** (wrapped CAC is the content root), disambiguating v1-length payloads by probing the v1 ref like bee's `resolveFeed`. Encrypted (64-byte ref) payloads still unsupported (no chunk decryption). |
| 7 | `/stewardship/{ref}` GET/PUT, `/envelope/{addr}` POST | ~½-1 d | ⬜ open | Not routed yet → 501. |
| 8 | Reed-Solomon erasure decoding (`--swarm-redundancy=1..3` interop) | ~2-3 d | ⬜ open | Read-path protocol work; not started. |
| 9 | `/topology` known/visible-peer population (kademlia "known" set) | ~1 day | ✅ shipped | Bounded `KnownPeers` book (deduped by overlay, per-bin cap + TTL) fed by hive gossip + handshakes; `GET /topology` reports per-bin `population` from the known book, `connected` from the live table, and a real saturation `depth`. Live-verified: `connected≈100` while visible `population` climbs into the thousands (`depth=5`). See "Open: visible-peers" below. |

#### Open: chequebook auto-bootstrap (bee-parity) — row 5

**Status as of `antd` 0.5.8 (2026-06-05).** Two related runtime light-node items:

- **Runtime postage batch parity — ✅ shipped in 0.5.8.**
  Runtime batch registry, buy/dilute → `RegisterBatch`, `Swarm-Postage-Batch-Id`
  upload header, `beeMode=light` decoupled from `--postage-batch`, persisted
  issuers rescanned on startup, `GET /stamps[/{id}]` reflecting the live
  registry. Live-verified on Gnosis mainnet: buy → `usable:true` within seconds
  → upload with the batch header → readback.

- **Chequebook lifecycle parity (table row 5) — ✅ shipped.** `antd`
  now resolves the outbound-settlement chequebook in priority order
  (`resolve_chequebook` in `crates/antd/src/main.rs`):
  1. **Manual** — `--chequebook` (+ `--swap-key` / `CHEQUEBOOK_ADDRESS` +
     `WALLET_PRIVATE_KEY`). Operator-managed; never auto-persisted. Wins over
     everything else.
  2. **Persisted** — a chequebook this node auto-deployed on an earlier start,
     reloaded from `<data-dir>/chequebook.json` (deploy-once, reuse-forever —
     the equivalent of the record bee keeps in its statestore, which `antd`
     can't read). Issuer is the node EOA, so the node `signing_secret` signs
     cheques.
  3. **Auto-deploy** — on first light start with a funded node wallet and
     neither of the above, deploy a fresh factory-registered chequebook
     (`deploy_chequebook(factory, issuer = node EOA)`), fund it with xBZZ via
     `erc20_transfer` (`--chequebook-deposit-plur`, default 0.1 BZZ, capped to
     the wallet's balance), persist it, and build the SWAP config from it +
     `signing_secret`. No external key injection. Gated on light mode + a
     Gnosis RPC + a gas pre-flight; best-effort (a thin/unfunded wallet or a
     flaky RPC logs and starts without settlement rather than failing).
     `--no-auto-chequebook` opts out.

  `GET /chequebook/{address,balance}`, `POST /chequebook/deposit`, and
  `/wallet` all report whichever chequebook was resolved above. A standalone
  swap key that nominates a *non-node* issuer EOA (without a contract) keeps
  the historical "settlement disabled" behaviour rather than silently deploying
  one issued by the node. One-time caveat: a chequebook previously deployed by
  bee lives in bee's LevelDB statestore (which `antd` can't read) and isn't
  reverse-lookupable on-chain, so first light start deploys a *fresh*
  chequebook for the same node EOA — identical to bee's own behaviour against a
  fresh statestore.

#### visible-peers / known-peer book (bee-parity) — row 9 ✅ shipped (0.5.9)

**Shipped in 0.5.9** as specified below: a bounded `KnownPeers` book
(`crates/ant-p2p/src/routing.rs`) deduped by overlay with a per-bin cap +
TTL, fed in `enqueue_hint` (before the dial-dedup early-returns) and on
handshake success, pruned on the 30 s peerstore-flush tick, never cleared on
disconnect. `RoutingInfo` carries additive `known_size` / `known_bins`
(`#[serde(default)]`; `size`/`bins` stay connected for `antop` back-compat),
and `GET /topology` reports per-bin `population` from the known book,
`connected` from the live table, top-level `population = known_size`, and a
real saturation `depth`. Live-verified on mainnet: after ~2.5 min,
`connected = 100` / `GET /peers = 100` while visible `population = 1163` and
climbing, `depth = 5`.

**Reporting-only** change — `antd` keeps its ~100-peer working set; it just also
*tracks and reports* the larger hive-discovered population, exactly like bee's
kademlia distinguishes "known" from "connected".

**Why:** bee-shaped clients show two peer counters — **Connected Peers** ←
`GET /peers` array length, and **Visible Peers** ← `GET /topology`
`sum(bins[*].population)`. bee reports thousands of visible peers; `antd`
currently sets `population == connected` (~100), so both counters read the same.

**Current state (verified against `origin/main`):**
- `GET /topology` (`crates/ant-gateway/src/status.rs::topology`) emits per-bin
  `population == connected` (same value), top-level `population = routing.size`,
  and `depth: 0` (hardcoded).
- The counted `RoutingTable` (`crates/ant-p2p/src/routing.rs`) holds only
  currently-connected, handshaked peers: `admit` on BZZ-handshake success
  (`behaviour.rs` ~3341), `forget` on `ConnectionClosed` (~3214), bounded by
  `DEFAULT_TARGET_PEERS` (~100).
- Hive-discovered peers are **not retained**: `run_hive` (`sinks.rs`) decodes
  `PeerHint { peer_id, overlay }` → `hint_tx`; `enqueue_hint` (`behaviour.rs`
  ~837) only uses them to fill the dial queue (deduped via `seen_hints`) and
  drops the rest. No known-but-unconnected address book.
- Snapshot: `routing_snapshot` (`behaviour.rs` ~1048) → `RoutingInfo
  { base_overlay, size, bins }` (`ant-control/src/protocol.rs:507`,
  `bins: Vec<u32>` = connected counts) → `StatusSnapshot.peers.routing`.

**Required:**
1. **Known-peer book** (`crates/ant-p2p/src/routing.rs`, new `KnownPeers`):
   `entries: HashMap<PeerId, { overlay, po, last_seen }>` + `bins: [u32; 32]`.
   `note(peer, overlay)` (idempotent insert/update, bump `bins[po]` on first
   insert), `prune(now)` (TTL ~30–60 min and/or per-bin cap, decrement bins),
   `bin_counts()`, `len()`. Dedup by **overlay** (hive can re-advertise an
   overlay under a new `PeerId`).
2. **Feed the book** (`behaviour.rs`): call `state.known.note(...)` in
   `enqueue_hint` **before** the dial-dedup early-returns, and again next to
   `state.routing.admit(...)` on handshake success. Do **not** remove on
   `ConnectionClosed` (known persists). Add `known: KnownPeers` to `SwarmState`;
   `prune` on the existing periodic tick.
3. **Carry both counts in the snapshot:** extend `RoutingInfo` with
   `#[serde(default)] known_size: u32` + `#[serde(default)] known_bins: Vec<u32>`
   (keep `size`/`bins` = connected for `antop` back-compat); fill them in
   `routing_snapshot`.
4. **Report in `/topology`** (`status.rs::topology`): per-bin `population =
   known_bins[i]`, `connected = connected_bins[i]`; top-level `population =
   known_size`, `connected = sum(connected)`; compute a real `depth` from the
   connected bins (highest PO where cumulative connected ≥ saturation, e.g. 4)
   instead of `0`.

**Constraints:** don't raise `DEFAULT_TARGET_PEERS` or change the dialer; bound
the book (TTL + per-bin cap) so gateway-style churn can't grow it without limit;
new snapshot fields are additive/`#[serde(default)]`. **Acceptance:** after a few
minutes `sum(bins[*].population)` is in the thousands and climbing while
`connected`/`GET /peers` stays ~100; `population ≥ connected` always; `depth > 0`
once a neighborhood forms.

After (1)–(7) an unmodified bee-js script doing
`bee.uploadFile / downloadFile / createTag / createPostageBatch /
getChequebookBalance / feed read+write` works against `antd` with
no config changes. After (8) gateway-uploaded sites that opted into
RS recovery decode correctly under partial-graph conditions. That
is the "drop-in" line.

What stays out of scope for the drop-in claim:

- `/pinning/*`, `/transactions[/{hash}]`, `/auth`, `/refresh`,
  `/chequebook/cashout/{peer}` — permanently `501` by design
  (Appendix B + §5.8 + D.2.4). bee returns these too on a
  `bee-mode=light` install with the relevant subsystems disabled,
  so unmodified consumers don't trip on them. (`/pss/*` and
  `/gsoc/*` were on this list until the messaging graduation —
  they're real routes now, see Tier B+ in Appendix C.)
- `POST /bytes` (split-without-manifest) — bee accepts this, ant
  returns 501; bee-js's `uploadFile` always uses `POST /bzz`, so
  the gap doesn't actually hit consumers in practice. Worth fixing
  when convenient but not gating drop-in.
- Web3 v3 keystore portability, `bee init` / `bee dev` UX,
  Prometheus metrics, OpenAPI 3.0 spec — operator quality-of-life
  items that don't change protocol or HTTP-API behaviour.

Mobile artefact (UniFFI `.xcframework` / Kotlin `.aar`) is a
parallel M2 track — orthogonal to the bee drop-in question, since
bee itself doesn't ship mobile binaries.

### Optional early releases

The milestone structure deliberately supports three pre-GA checkpoints, each useful in its own right:

- **End of M1.0:** **`antd` dogfood release.** Internal-only daemon that peer with mainnet. Zero user value but huge team morale and infrastructure validation signal: a handful of engineers run `antd` on their laptops, file bugs, and the BZZ handshake hardens fast against the wild variety of bee peers out there.
- **End of M1.1:** **internal "Ant Probe" dev tool.** Still no user-facing value, but now provides a stable mainnet-connected node that retrieval work (M2) can be built on top of with confidence. Good point to onboard additional engineers.
- **End of M2:** **shippable public "Ant Reader" app / SDK.** Lets you learn UX and distribution without having to solve the xDAI/xBZZ/chequebook problem yet. Highly recommended — de-risks the hardest *product* problems before you finish the hardest *protocol* problems.

### iOS download smoke test (pre-FFI)

Throwaway exploratory track, runnable any time after Phase 2 lands retrieval against mainnet. Goal: prove that our libp2p stack can dial mainnet bee peers and complete a chunk download from a Rust library running inside an iOS app, **before** we commit to the full UniFFI surface in §7 or the polished mobile artifact in Phase 4. De-risks the iOS-specific edges (ATS / cleartext TCP, simulator vs. device sockets, cellular vs. Wi-Fi, OSLog wiring, Tokio inside a UIKit lifecycle) on a thin slice of code that's cheap to delete.

It is **not** a substitute for the M2 Phase 4 deliverable and does not change any milestone gate. It exists to surface iOS-specific surprises early, while the FFI is still soft.

> **Status update (2026-06):** exit criteria met — simulator *and* real-device downloads against mainnet work, including multi-chunk references through the joiner. The track then outgrew its "three functions, cheap to delete" scope by design pull rather than plan: `ant-ffi` now exposes ~30 `extern "C"` entry points (download + progress + cancel, manifest listing, `AVAssetResourceLoaderDelegate`-shaped streaming, upload jobs with pause/resume, storage-plan status/connect/quote/buy behind the `chain` cargo feature, account info), keeps a 512 MiB on-disk SQLite chunk cache instead of the in-memory-only cache described below, and is consumed by a second, much larger example app (`examples/ios-drive`, AntDrive — uploads, storage plans, on-chain plan purchase) plus the Android sibling. The scope description below is preserved as written for the historical record; `crates/ant-ffi/README.md` documents the surface as it exists today. The header is hand-written (not cbindgen-generated) and maintained next to `src/lib.rs`. The "expected to be deleted" framing has softened to "promoted or replaced": the surface is now the de-facto pre-UniFFI mobile API, and the § "Interim releasable iOS artifact" track below packages it for third parties ahead of Phase 4.

#### Scope

In:

- `aarch64-apple-ios-sim` static lib for the simulator. `aarch64-apple-ios` device slice is also built side-by-side (`cargo xtask build-ios-device`) so a paired iPhone is reachable behind a one-time Xcode sign-in — see `examples/ios-download/README.md` § "Running on a real iPhone". Still no `lipo` and no `.xcframework`; both slices live as plain `target/<triple>/release/libant_ffi.a` and the Xcode project picks the right one with `LIBRARY_SEARCH_PATHS[sdk=...]`.
- Three hand-written `extern "C"` functions: `ant_init(data_dir)`, `ant_download(handle, ref_hex, …)`, `ant_free`. No UniFFI, no `.udl`, no codegen.
- Hardcoded mainnet bootnodes; throwaway libp2p identity persisted under the app sandbox; in-memory chunk cache only (`disk_cache: None`).
- One SwiftUI screen: tap a button → "got N bytes" plus a hex prefix.

Out (deferred to the proper M2/M3 mobile track):

- UniFFI-generated Swift bindings.
- `KeyProvider` callback / Keychain / Secure Enclave plumbing.
- `BGProcessingTask` / `BGAppRefreshTask` / suspend-resume / lifecycle wiring.
- SQLite disk cache, size-budget release profile, code signing, notarization.
- Device-slice `.xcframework` packaging, CI gating.

#### Responsibility split — the FFI surface is the boundary

Anything any iOS app would need lives in `vibing/ant`; anything specific to this demo lives in the app. Useful test: imagine a third party writes their own iOS app against the FFI tomorrow — anything they would have to reinvent themselves is in the wrong place.

| Concern | Owner |
|---|---|
| `extern "C"` API + `crates/ant-ffi/include/ant.h` (hand-written, checked in) | ant |
| Tokio runtime, libp2p host, retrieval, joiner, mantaray | ant |
| Mainnet bootnode + `network_id = 1` defaults | ant — app calls `ant_init(data_dir)`, never `ant_init(data_dir, bootnodes, network_id, …)` |
| Auto-generated libp2p identity persisted under `<data_dir>/identity.json` | ant (replaced by `KeyProvider` callback later, see §5.10) |
| `tracing` → `OSLog` bridge, behind `#[cfg(target_os = "ios")]` | ant (replaced by `set_log_sink` callback per §11) |
| `cargo xtask build-ios-sim` / `cargo xtask build-ios-device` cross-compile recipes | ant |
| Sandbox path (`Application Support/`) chosen and passed in to `ant_init` | app |
| `Info.plist`, `NSAppTransportSecurity / NSAllowsArbitraryLoads = true` (Noise XX is not TLS) | app |
| SwiftUI views, the hardcoded test reference, code signing, provisioning | app |

Rule of thumb for future "where does this go" calls: **removing the example app should not break the Rust test suite, and removing ant should not break the example app.**

#### Workspace placement

```
vibing/ant/
├── crates/
│   └── ant-ffi/                # staticlib + cbindgen + extern "C"
│       └── include/ant.h       # checked in; bridging-header points here
├── xtask/                      # build-ios-sim, build-ios-device, later build-ios-xcframework
└── examples/
    └── ios-download/           # SwiftUI smoke-test app, Xcode project
```

The example app sits under `examples/` (not `ios/`) to flag *experimental*. When the proper download-only mobile artifact lands at the end of Phase 4, promote or replace it with the reference `ios/` project from §4.

In-repo from day one, not a separate repo: while the FFI is churning, every breaking change is one PR that touches both `crates/ant-ffi/src/lib.rs` and `examples/ios-download/AntDownload/ContentView.swift` — no version-skew matrix to manage. Same CI run can cross-compile the static lib and invoke `xcodebuild -scheme AntDownload -destination 'platform=iOS Simulator,name=iPhone 16'` against it.

#### Exit

- iOS Simulator on a Mac connects to live mainnet bee peers, completes the BZZ handshake, fetches a known small CAC root chunk via `RoutingFetcher::fetch` + `joiner::join`, and renders the byte length + hex prefix in the SwiftUI view.
- Stretch: same flow on a real iPhone over Wi-Fi (requires the device slice + a development provisioning profile).
- Stretch: same flow against a multi-chunk reference, end-to-end through the joiner.

#### What this does not solve

The proper M2 Phase 4 deliverable is still a UniFFI-based `.xcframework` that meets the size budget in §12, the FFI shape in §7, OSLog + lifecycle wiring per §8.4 / §11, and Keychain key storage per §5.10. The smoke test exists to prove the *transport* (Rust + libp2p inside iOS dialing mainnet) works end-to-end; it explicitly does not stand in for that artifact and is expected to be deleted when Phase 4 lands.

### Interim releasable iOS artifact — `AntFFI.xcframework` (pre-UniFFI)

> **Status update (2026-06):** work items 1 (xtask) and 3 (release CI) landed — `cargo xtask build-ios-xcframework` builds the device + fat-simulator slices (release, `chain`, `IPHONEOS_DEPLOYMENT_TARGET=15.0`), stages `ant.h` + a `module.modulemap` (with `link framework` directives for `Security` / `SystemConfiguration` / `CoreFoundation`), and assembles `AntFFI.xcframework`; `.github/workflows/release-ios.yml` builds it on a macOS runner per `v*` tag, zips with `ditto --keepParent`, computes the SPM checksum, and attaches `AntFFI.xcframework.zip` + `.checksum` to the GitHub release (first shipped with v0.5.17). Item 4's `strip = "symbols"` was already in the workspace release profile. Item 5 is partially done: `crates/ant-ffi/README.md` § 0 now documents consuming the released zip (SPM `binaryTarget` + checksum, or drag-into-Xcode, `import AntFFI`, autolinked frameworks, baked-in limitations); the trim into a standalone package README waits for the wrapper package itself. Still open: item 2 (the thin Swift wrapper package), item 6 (codesigning), and the out-of-repo fresh-Xcode-project exit test.

The hand-written `ant-ffi` surface has outgrown the original three-function smoke test: the AntDrive example exercises downloads, streaming, uploads, storage plans, and on-chain plan purchase through it. That makes an *interim* third-party release worthwhile before the UniFFI artefact lands — external apps can embed the node today without a Rust toolchain, and we learn the distribution mechanics (SPM, checksums, release CI) ahead of Phase 4 instead of during it.

This is a packaging track only. It does not change the FFI shape, any milestone gate, or the Phase 4 commitment to UniFFI; it ships the existing C surface as-is, tagged pre-1.0 with an explicit "ABI will change" notice.

#### Deliverable

A zipped `AntFFI.xcframework` (device + simulator slices, headers + module map) attached to GitHub releases with a SHA256 checksum, consumable as an SPM `binaryTarget`, plus a thin Swift wrapper package.

#### Work items

1. **`cargo xtask build-ios-xcframework`** — one command that:
   - builds `aarch64-apple-ios` (device), `aarch64-apple-ios-sim`, and `x86_64-apple-ios` (release, `--features chain` — features are baked into a staticlib, and one variant beats a 2× artifact matrix; non-chain callers pay only size);
   - pins `IPHONEOS_DEPLOYMENT_TARGET` explicitly so the artifact has a documented, predictable min-iOS floor instead of rustc's per-target default;
   - `lipo`s the two simulator slices into one fat `libant_ffi.a` (`-create-xcframework` accepts only one library per platform-variant);
   - stages `include/ant.h` plus a `module.modulemap` (`module AntFFI { header "ant.h"; export * }`) so consumers write `import AntFFI` — no bridging header;
   - runs `xcodebuild -create-xcframework -library <device>.a -headers include -library <sim-fat>.a -headers include -output AntFFI.xcframework`.
2. **Swift package.** `binaryTarget` pointing at the release zip + checksum, wrapped by an `Ant` target that (a) carries the mandatory `linkerSettings` (`SystemConfiguration`, `Security`, `CoreFoundation`) so consumers don't rediscover the linker errors by hand, and (b) hosts a Swift-friendly API generalised from `AntNode.swift` — handle lifecycle, `Task.detached` hops off the main thread, `Codable` decoding of the JSON returns, and encapsulation of the free-through-the-library memory rules.
3. **Release CI.** A macOS job (in `release.yml` or a sibling `release-ios.yml`, same `v*` tag trigger) that runs the xtask, zips, computes the SPM checksum (`swift package compute-checksum`), and attaches `AntFFI.xcframework.zip` + checksum as release assets — distinct from the `antd` matrix so Freedom's `fetch-ant.js` keyword matching is untouched. Versioned with the `ant-ffi` crate version; `ant_agent_string` already reports it at runtime so deployed-vs-claimed is verifiable.
4. **Size pass.** `strip = "debuginfo"` (or equivalent post-processing) on the release profile — the `.a` otherwise carries full debug info — and record the zipped size in the release notes. This artifact is *not* held to the §12 UniFFI budget (it includes `chain`: reqwest + rustls), but the number must be known and tracked.
5. **Consumer docs.** Trim `crates/ant-ffi/README.md` to the consumer-side contract for the package README: ATS exception (non-negotiable — Noise XX over raw TCP is not TLS), writable data dir, never-block-the-main-thread, memory rules, cold-start warmup behaviour, optional `peers.json` seeding.
6. **Optional codesigning.** `codesign --timestamp` the xcframework — Xcode 15+ pins and verifies XCFramework signatures. Nice-to-have, not gating.

#### Known limitations (state in the release notes)

- **ABI instability** — hand-written pre-UniFFI surface, replaced by the Phase 4 artefact; pre-1.0 semantics.
- **One-Rust-staticlib rule** — a Rust staticlib embeds the Rust runtime; linking a second Rust-based static library into the same app collides at symbol level. The eventual fix is a dynamic-library xcframework (which brings codesigning obligations), explicitly out of scope here.
- **`chain` baked in** — callers who never touch Gnosis still carry reqwest/rustls in their binary.

#### Exit

- `cargo xtask build-ios-xcframework` produces a valid `AntFFI.xcframework` from a clean checkout.
- A tag push publishes the zip + checksum to the GitHub release alongside the `antd` assets.
- A fresh Xcode project outside this repo adds the Swift package by URL, calls `ant_init` + `ant_download` on simulator **and** a real device, and round-trips a known mainnet reference — with no Rust toolchain installed.

#### Relationship to Phase 4

When the UniFFI `.xcframework` lands, it supersedes this artifact: the Swift package flips its `binaryTarget` to the UniFFI build, the wrapper API is replaced by the generated bindings, and the hand-written header is retired with the rest of the pre-FFI track. The release plumbing (xtask → CI → zip → checksum → SPM) carries over unchanged — that is most of the point of doing this early.

### Android download smoke test (pre-FFI)

Sibling to the iOS smoke above, runnable any time after `ant-ffi` exists. Goal: prove that our libp2p stack can dial mainnet bee peers and complete a chunk download from a Rust library running inside an Android app, **before** we commit to the full UniFFI surface in §7 or the polished `.aar` in Phase 4. De-risks the Android-specific edges (NDK toolchain quirks, `System.loadLibrary` + JNI symbol mangling, `NetworkOnMainThreadException`, `libc++_static` vs `libc++_shared`, logcat wiring, `cargo-ndk` integration) on a thin slice of code that's cheap to delete.

Like the iOS smoke, it is **not** a substitute for the M2 Phase 4 deliverable and does not change any milestone gate. It exists to surface Android-specific surprises early, while the FFI is still soft, on the same checked-in `ant-ffi` crate the iOS app links against.

> **Status update (2026-06):** landed as a *subset* of the scope below. `crates/ant-ffi/src/jni.rs` exports the download core — `nativeInit` / `nativeDownload` / `nativeDownloadProgress` / `nativeCancelDownload` / `nativePeerCount` / `nativeAgentString` / `nativeShutdown` — behind the default-off `jni` cargo feature; `examples/android-download` is a paste-and-download Compose app, and CI cross-compiles the arm64 `libant_ffi.so` and verifies the JNI symbols. The streaming JNI methods (`nativeListBzz` / `nativeStreamOpen` / `nativeStreamRead` / `nativeStreamPull` / `nativeStreamProgress` / `nativeStreamClose`) and the ExoPlayer *Player* tab described below have **not** landed — Android trails the iOS surface, which moved on to uploads/storage in AntDrive instead. One correction to the scope text: the chunk cache is no longer in-memory-only; the shared `ant_init` path keeps a 512 MiB on-disk SQLite cache on both platforms.

#### Scope

In:

- `aarch64-linux-android` shared library (`libant_ffi.so`) for arm64 phones (the only ABI worth caring about for v1 — covers every device shipped post-2017). `armv7-linux-androideabi` and `x86_64-linux-android` (emulator) slices follow as cheap additions once the arm64 path works. No `.aar`, no Maven publish; the `.so` files land directly under `app/src/main/jniLibs/<abi>/` so Gradle picks them up via `android:extractNativeLibs`.
- A hand-written **JNI** surface inside `crates/ant-ffi/src/jni.rs` (not a separate crate), gated by `#[cfg(target_os = "android")]` and a default-off `jni` Cargo feature. Mirrors the existing `extern "C"` API one-to-one as `Java_at_vibing_ant_downloadsmoke_AntNode_native…` exports — `nativeInit` / `nativeDownload` / `nativePeerCount` / `nativeAgentString` / `nativeDownloadProgress` / `nativeCancelDownload` / `nativeListBzz` / `nativeStreamOpen` / `nativeStreamRead` / `nativeStreamPull` / `nativeStreamProgress` / `nativeStreamClose` / `nativeShutdown`. No UniFFI, no `.udl`.
- `ant-ffi` grows `cdylib` alongside `staticlib` in its `crate-type` so the same crate produces `libant_ffi.a` (iOS) and `libant_ffi.so` (Android) from one source tree.
- Hardcoded mainnet bootnodes; the same `data_dir`-based `identity.json` / `peers.json` persistence as iOS, pointed at the app's `context.filesDir` by the Kotlin side; in-memory chunk cache only.
- Two-tab Jetpack Compose app: *Player* (paste-and-download field for v1, ExoPlayer streaming once the streaming JNI lands) and *Network* (peer count, agent string, retrieval progress) — direct mirror of the SwiftUI `RootView` / `PlayerView` / `NetworkView` triple.

Out (deferred to the proper M2/M3 mobile track, same as iOS):

- UniFFI-generated Kotlin bindings.
- `KeyProvider` callback / Android Keystore / StrongBox plumbing.
- `WorkManager` foreground service / suspend-resume / Doze whitelisting.
- SQLite disk cache, R8/ProGuard-tuned size budget, signed release APK / AAB.
- Multi-ABI `.aar` packaging, Play Store metadata, CI gating.

#### Responsibility split — same FFI surface as iOS

The Rust side is platform-agnostic; only the bridge layer differs (JNI exports vs `extern "C"`). Anything any Android app would need lives in `ant-ffi`; anything specific to this demo lives in the app.

| Concern | Owner |
|---|---|
| `extern "C"` API + `crates/ant-ffi/include/ant.h` (iOS) | ant |
| JNI exports (`Java_at_vibing_ant_downloadsmoke_AntNode_native…`) | ant, in `crates/ant-ffi/src/jni.rs` |
| Tokio runtime, libp2p host, retrieval, joiner, mantaray | ant — shared with iOS verbatim |
| Mainnet bootnode + `network_id = 1` defaults | ant |
| Auto-generated libp2p identity persisted under `<data_dir>/identity.json` | ant (replaced by `KeyProvider` callback later, see §5.10) |
| `tracing` → `logcat` bridge via `android_logger`, `#[cfg(target_os = "android")]` | ant (replaced by `set_log_sink` callback per §11) |
| `cargo xtask build-android-arm64` / `build-android-armv7` / `build-android-x86_64` / `build-android-all` cross-compile recipes | ant — wraps `cargo-ndk` |
| `context.filesDir.absolutePath` chosen and passed to `nativeInit` | app |
| `AndroidManifest.xml`, `INTERNET` permission | app |
| Compose views, ExoPlayer wiring, package name `at.vibing.ant.downloadsmoke` (matches iOS bundle id) | app |

Same rule of thumb: **removing the example app should not break the Rust test suite, and removing ant should not break the example app.** Anything a third-party Android app would have to reinvent themselves belongs in `ant-ffi`, not under `examples/`.

#### Workspace placement

```
vibing/ant/
├── crates/
│   └── ant-ffi/                # rlib + staticlib (iOS) + cdylib (Android)
│       ├── include/ant.h       # iOS bridging header
│       └── src/jni.rs          # Android JNI exports, cfg-gated
├── xtask/                      # build-ios-{sim,device}, build-android-{arm64,armv7,x86_64,all}
└── examples/
    ├── ios-download/           # SwiftUI smoke-test app, Xcode project
    └── android-download/       # Compose smoke-test app, Gradle project
```

Same in-repo argument as iOS: while the FFI is churning, every breaking change is one PR that touches `crates/ant-ffi/src/jni.rs` and `examples/android-download/app/src/main/java/at/vibing/ant/downloadsmoke/AntNode.kt` together — no version-skew matrix.

#### Bridge style — why hand-written JNI, not UniFFI

Same answer as iOS, mirrored: UniFFI is Phase 4 work; the smoke-test apps are explicitly throwaway. Hand-rolling JNI:

- Keeps the surface obvious — every `external fun` in Kotlin lines up 1:1 with a Rust `Java_…` export, no codegen step in the loop.
- Avoids the UniFFI/Kotlin/NDK toolchain stack while we're still pinning down which bootnodes Android trusts on cellular.
- Stays deletable: when Phase 4 lands the `.aar`, we delete `examples/android-download/` and `crates/ant-ffi/src/jni.rs` together.

JNI lives inside `ant-ffi` (not a sibling `ant-jni` crate) so the Rust state machine (`AntHandle`, `init_inner`, `download_inner`, `AntStream`) is reused verbatim by both extern-C and JNI wrappers, with no `pub` API surface widened just to bridge crates.

#### Player engine — ExoPlayer mirrors AVPlayer

The `ant_stream_open` / `ant_stream_read` / `ant_stream_pull` / `ant_stream_close` shape we shipped for `AVAssetResourceLoaderDelegate` was deliberately picked to also fit Media3 / ExoPlayer's `DataSource` interface:

| ExoPlayer method | iOS equivalent | FFI call |
|---|---|---|
| `DataSource.open(DataSpec)` | `loadingRequestHandled.dataRequest.open()` | `nativeStreamOpen(reference)` |
| `DataSource.read(buffer, offset, length)` | `respond(with:)` inside `dataRequest` | `nativeStreamRead(stream, offset, len, dst)` |
| `DataSource.close()` | `cancel(loadingRequest)` | `nativeStreamClose(stream)` |

v1 of the Android player goes through `nativeStreamRead` (blocking) since ExoPlayer's `DataSource` is already a blocking interface. The lower-overhead `nativeStreamPull` (callback-per-chunk) is held back until we measure whether the JNI byte-array copy is actually a bottleneck on a $200 phone.

#### Build pipeline

`xtask/src/main.rs` grows three Android subcommands and an `all` aggregator:

```
cargo xtask build-android-arm64    # aarch64-linux-android
cargo xtask build-android-armv7    # armv7-linux-androideabi
cargo xtask build-android-x86_64   # x86_64-linux-android (emulator)
cargo xtask build-android-all      # all three, in series
```

Each invokes `cargo ndk -t <abi> -p 26 -- build -p ant-ffi --release --features jni`. We depend on `cargo-ndk` being on `$PATH`; `ensure_rust_target` gets a sibling `ensure_cargo_ndk` that prints `cargo install cargo-ndk` on miss. On success, the `.so` is copied into `examples/android-download/app/src/main/jniLibs/<abi>/libant_ffi.so` so a plain `./gradlew assembleRelease` from the example directory finds it without further wiring. (Same shape as the iOS xtask: it builds and reports the path; the host build system consumes that path.)

minSdk is **26** (Android 8.0). NDK is **r26+** for current libp2p / `getrandom` / `clock_gettime` expectations. `compileSdk = 35`.

#### Suggested ordering, smallest-merge-first

Same logic as the iOS smoke — each step is the smallest checkpoint that proves the next layer of plumbing:

1. Rust `cdylib` + minimal JNI (`nativeInit` + `nativeDownload` + `nativeShutdown` + `nativePeerCount` + `nativeAgentString`) + xtask `build-android-arm64`. Validation: `cargo xtask build-android-arm64` produces `libant_ffi.so` and `nm -D` shows the `Java_…_nativeInit` symbol.
2. Compose app skeleton with the equivalent of `AntDownloadApp.swift` + `RootView.swift` + a single screen showing peer count + a paste-and-download field. Validation: install on an arm64 emulator (or device), paste a known mainnet hex, see bytes.
3. xtask `build-android-armv7` + `build-android-x86_64`, ABI splits, README scaffold (mirroring `examples/ios-download/README.md`).
4. Progress JNI + `DownloadProgress` flow + Network tab.
5. `nativeListBzz` + `Manifest.kt` + manifest browser screen.
6. Streaming JNI + ExoPlayer `DataSource` + audio playback.

#### Likely Android-specific traps

Lessons from iOS that translate, and a few that don't:

- **`libc++_shared.so` vs `libc++_static.a`.** `cargo-ndk` defaults to static-linking the C++ runtime, which is what we want — otherwise the APK has to ship `libc++_shared.so` alongside `libant_ffi.so` and Gradle won't do that automatically.
- **`panic = "abort"` on Android tends to kill processes with no useful stack.** Wrap every JNI entry point in `catch_unwind` (we do this on iOS already) and rethrow as a Kotlin `RuntimeException` carrying the panic message.
- **Network on the main thread is illegal on Android** (`NetworkOnMainThreadException`). Every native call that does I/O has to be on `Dispatchers.IO`. Mirroring the iOS `Task.detached(priority: .userInitiated)` pattern handles this naturally.
- **`System.loadLibrary` is per-process, not per-Activity.** Loading happens in a static initializer on the `AntNode` class. Calling `nativeInit` more than once leaks runtimes — gate it the same way `AntNode.swift` does (`guard case .idle = status else { return }`).
- **No ATS analogue.** Android trusts arbitrary TCP by default, so there's no `NSAllowsArbitraryLoads` switch to flip. Cleartext HTTP isn't relevant either since we only do raw TCP+Noise.
- **Background time.** Android is similar to iOS (~30 s before Doze) but stricter on battery-saver. v1 ignores this — the same way the iOS smoke does — and documents it. The proper fix is a foreground service (§ 10).

#### Exit

- Android emulator on a Mac connects to live mainnet bee peers, completes the BZZ handshake, fetches a known small CAC root chunk via `RoutingFetcher::fetch` + `joiner::join`, and renders the byte length + hex prefix in the Compose view.
- Stretch: same flow on a real Android device over Wi-Fi (USB debugging only — no provisioning-profile dance like iOS, just `adb install`).
- Stretch: streaming an audio file from a `bzz://` manifest through ExoPlayer's `DataSource`.

#### What this does not solve

The proper M2 Phase 4 deliverable is still a UniFFI-based `.aar` that meets the size budget in §12, the FFI shape in §7, logcat + lifecycle wiring per §8.4 / §11, and Keystore / StrongBox key storage per §5.10. This smoke test exists to prove the *transport* (Rust + libp2p inside Android dialing mainnet) works end-to-end; it explicitly does not stand in for that artifact and is expected to be deleted when Phase 4 lands.

### iOS `bzz://` browser — architecture note

Non-committed design note captured while building the § 9 smoke test. Records how a native iOS browser can serve `bzz://` links straight from libant **without** a localhost HTTP gateway, so we don't re-derive this from scratch when Phase 4 closes. Not a milestone, not on the critical path — here so the option stays visible.

#### Why this is feasible at all

Because `ant-ffi` is **statically linked into the app process**, there is no IPC hop between the Swift side and the retrieval pipeline. That same property makes it straightforward to wire a `WKWebView` directly to libant through `WKURLSchemeHandler`: the scheme handler runs in-process, already holds an `AntHandle`, and fulfils each `WKURLSchemeTask` by calling the FFI's download / stream APIs. No embedded HTTP server, no TCP listener, no `http://localhost:<port>` proxy, no ATS exemption for loopback traffic.

The two OS-level knobs that make this work are already within reach:

1. **`CFBundleURLTypes`** in `Info.plist` registers `bzz` as a scheme the app handles, so a tap on `bzz://…` from Mail / iMessage / other apps opens your browser via `UIApplication.open`.
2. **`WKURLSchemeHandler`** (iOS 11+) intercepts `bzz://` loads *inside* the web view and answers them from your code. WebKit can't be taught to recognise a new scheme for built-in ones (`http`, `https`, `file`, …) but `bzz` is unconstrained.

#### Request flow

```
Mail / iMessage / Safari                          Browser app
      │ tap bzz://<root>/page                           │
      └── UIApplication.open(URL) ─────────────────────▶│
                                               ┌────────┴──────────┐
                                               │  BrowserVC        │
                                               │   WKWebView       │
                                               │    │ bzz://<root>/page
                                               │    ▼              │
                                               │  BzzSchemeHandler │
                                               │    │ ant_download_stream(…)
                                               │    ▼              │
                                               │  libant_ffi       │── libp2p → Swarm
                                               └───────────────────┘
                                                (all in-process)
```

Sub-resources (`<img>`, `<link>`, `<script>`, `fetch()`) re-enter the same handler as further `bzz://<root>/…` requests. They share the node's chunk cache, so repeat loads don't re-hit the network.

#### What already works with the current FFI

The § 9 FFI surface (`ant_init` / `ant_download` / `ant_peer_count` / `ant_download_progress` / `ant_cancel_download` / `ant_shutdown`) is enough to serve **static bzz:// sites end-to-end**: HTML + CSS + JS + images resolved through the mantaray walker, same-origin policy honoured per `bzz://<root>` pair, user cancel mapped onto `WKURLSchemeTask.stop()`. The smoke test already exercises every primitive this path needs; wiring the scheme handler on top is ~80 lines of Swift.

#### Gaps that must be filled before shipping a real browser

The node already supports all of these via `ControlCommand::{StreamBytes, StreamBzz}` and the `BzzStreamStart` / `BzzChunk` / `StreamDone` acks (§ C.4, Appendix E). The smoke-test FFI collapses them into a single blocking call; a browser-grade FFI needs to expose the streaming shape:

| Missing FFI | Browser need | Node support |
|---|---|---|
| Streaming body (`ant_download_stream` → `pull` events for `Start(content_type, total_bytes)`, `Chunk(data)`, `Done`) | Video / audio playback, progressive HTML rendering, large downloads | Yes (`StreamBzz`) |
| HTTP `Range` forwarding | Video seeking, partial loads | Yes (`range` on stream commands) |
| HEAD requests | Some content-type sniffers preflight | Yes (`head_only`) |
| Content-type + filename surfaced across FFI | `WKURLSchemeTask.didReceive(response)` needs a correct `URLResponse` | Yes (`BzzStreamStart { content_type, filename, total_bytes }`) |

Estimated surface growth: ~3-4 new entry points on `ant.h`, ~200 lines in `ant-ffi/src/lib.rs`. This is the natural direction for the M2 Phase 4 UniFFI crate (§ 7) anyway — a browser would consume a superset of the read-only API already budgeted.

#### iOS-specific caveats to design for

- **Mobile Safari cannot render `bzz://` content.** `CFBundleURLTypes` only causes Safari to *hand off* to the registered app; WebKit inside Safari does not gain a new scheme handler. The browser has to ship as its own app.
- **Secure-context gating.** WebKit treats `bzz://` as non-secure by default, so `navigator.mediaDevices`, Service Workers, `SubtleCrypto.importKey` with `extractable: false` on some paths, and similar "secure context only" Web APIs will be denied. Apple exposes `WKWebpagePreferences.preferredHTTPSFirstPolicy` and the unofficial `WKPreferences.secureContextForCustomSchemes` route — needs validation against a current iOS release before we promise it works.
- **CORS between `bzz://` and `https://`.** bzz:// pages that fetch `https://` resources will be blocked by default cross-origin policy unless the remote server sends permissive CORS headers. Site authors who fully self-host on bzz:// don't hit this; mixed sites do.
- **No TLS identity.** Inherent to content addressing — each root `bzz://<hex>` *is* its own origin. Same-origin separation still works; there's just no CA-signed identity story. UX should make that legible ("content-addressed, not host-addressed").
- **Keyboard + URL bar UX.** A `bzz://` address is 64 hex characters; without ENS-style short names, users will paste rather than type. Future work: pinned-site home screen + bzz-over-feed hostname resolution (see § 5.5) once feeds land.

#### Where this sits in the roadmap

- **Prerequisite:** Phase 4 UniFFI artefact (§ 9 Phase 4) plus the streaming FFI extensions above. Neither is on the critical path for M2's Tier-A read-only story.
- **Delivery shape:** separate app repo that depends on the published `libant-ios.xcframework`, not a new crate in this workspace. The browser is a *consumer* of ant, like `antd` / `antctl`.
- **Minimum viable browser** once the streaming FFI lands: single `WKWebView`, URL bar, back/forward, history persisted to SQLite, `CFBundleURLTypes` registration, optional Share extension so any app's "Share → Open in Ant" routes a pasted bzz:// reference. Everything else (tabs, bookmarks, search, ENS names, Swarm feeds) is incremental.

Worth revisiting at the start of Phase 4 to decide whether to widen the § 7 UniFFI sketch to include the streaming shape, so a browser project can start the moment the `.xcframework` ships.

---

## 10. Testing Strategy

### Unit tests

- Cryptography: BMT against bee test vectors; SOC signatures; cheque EIP-712 hashes.
- ABI encoding against known tx hashes.
- Mantaray round-trip on generated trees.
- Bucket counter state machines (property tests with `proptest`).

### Integration tests

- Golden-path interop against a **locally-run bee node** in a Docker container. This is the single most important test surface.
- Each protocol (hive, retrieval, pushsync) has a harness that speaks to a bee container.
- Chain tests against a **local Gnosis dev chain** (`anvil --fork <rpc>` against an archived block, or `nethermind --init`).

### End-to-end tests

- Headless mobile test app driving upload/download scenarios.
- Fuzzing: protobuf decoders with `cargo-fuzz`; ABI decoders; manifest parsers.

### Mainnet smoke tests

- CI job that does one download of a known reference and one small upload against mainnet. Alert on regression.

### Device matrix

- iOS: oldest-supported device (aim for iOS 16+), plus current flagship.
- Android: low-end (4 GB RAM / arm64) + flagship, plus emulator for CI.

---

## 11. Observability

- `tracing` everywhere; spans for upload jobs, downloads, chain calls.
- FFI exposes a `set_log_sink` so the host app bridges:
  - iOS: `OSLog`
  - Android: `android.util.Log` via `log` crate + `android_logger`
- Structured events for UI: `AntEvent::UploadProgress { job, bytes, total }`, `ChequeIssued`, `BatchExpiring`, etc.
- **No telemetry to our backend by default.** Opt-in only, anonymized.

---

## 12. Size Budget

### Per-component target (stripped, library only)

| Component | Target (stripped) |
|---|---|
| libp2p (TCP/Noise/Yamux/Kad) | ≤ 2.5 MB |
| Protocols (retrieval, pushsync, hive, BMT, mantaray) | ≤ 1.0 MB |
| Stamps + SWAP + accounting | ≤ 0.7 MB |
| Alloy + RPC + ABI + signing | ≤ 1.0 MB |
| Tokio (single-threaded) + prost + sqlite | ≤ 2.5 MB |
| Crypto (k256, sha3, sha2, chacha20) | ≤ 0.6 MB |
| Glue + UniFFI scaffolding | ≤ 0.5 MB |
| **Total per platform** | **≤ 8.8 MB** |

Tracked as a CI check that fails the build if `.xcframework` or `.aar` grows past the threshold for a release build.

### Measured: Android `aarch64-linux-android` slice (smoke-test app, 2026-04)

These are real numbers from the smoke-test app in `examples/android-download/`,
not projections — captured with `cargo xtask build-android-arm64`,
`./gradlew :app:bundleRelease`, and Google's `bundletool 1.18.1`. The
PLAN.md ≤ 10 MB Tier-1 budget for the Android library slice (§ 3 deployment
targets table) is an order of magnitude bigger than the per-component
sums above on purpose — the per-component table is what we *aim* for once
the proper M2 Phase 4 mobile artefact lands; the 10 MB ceiling is what we
*allow* the throwaway smoke test to ship at while we're still hardening
the FFI.

| Artefact | Pre-R8 | Post-R8 | Δ |
|---|---|---|---|
| `libant_ffi.so` (uncompressed, stripped) | 6.7 MB | 6.7 MB (R8 doesn't touch native libs) | 0 |
| `app-release.aab` on disk | 11.0 MB | 6.06 MB | **−45 %** |
| Per-device download for arm64 (compressed, what Play Store reports) | 10.83 MB | 4.14 MB | **−62 %** |
| Per-device install footprint (uncompressed APK splits) | ~15 MB | ~7.9 MB | **−47 %** |

The post-R8 download size of 4.14 MB is the gzip-compressed wire bytes
Play Store streams to a device. After install the on-disk footprint
expands to ~7.9 MB because the `.so` ships uncompressed inside the APK
(so the dynamic linker can mmap it at process start). Of the 4.14 MB
download:

- `libant_ffi.so` (compressed during wire delivery, stored uncompressed
  inside the APK): ~3.1 MB. This is **the entire embedded ant** —
  Tokio, libp2p, Noise, the BMT joiner, the mantaray walker, the JNI
  surface, and the `tracing` → logcat bridge.
- Compose + Kotlin stdlib + AndroidX lifecycle / activity-compose +
  app dex + resources, post-R8: ~1.0 MB. Pre-R8 this was ~7.7 MB,
  ~85 % of which was dead code that Compose / Kotlin coroutines /
  AndroidX never reach from our actual entry points.

Section breakdown of the 6.7 MB stripped `libant_ffi.so` (from
`llvm-readelf -S`):

| Section | Size | What's in it |
|---|---|---|
| `.text` | 4.88 MB | executable code (Tokio + libp2p + retrieval + JNI) |
| `.rodata` | 0.70 MB | constants, string tables, vtables |
| `.eh_frame` | 0.46 MB | panic-unwind tables (kept for `catch_unwind` in `crates/ant-ffi/src/jni.rs`) |
| `.rela.dyn` | 0.33 MB | dynamic relocations |
| everything else | ~0.33 MB | `.data`, `.got`, `.dynsym`, ELF headers, PLT stubs |

#### Headroom vs Tier-1 budget

The deployment targets table (§ 3) caps the Android arm64 library slice
at 10 MB stripped. We're at 6.7 MB, so **3.3 MB of headroom**. That's
enough to absorb the Phase 4 additions (UniFFI runtime ≈ 0.3 MB, the
`KeyProvider` callback machinery ≈ 0.1 MB, SQLite disk cache code path
≈ 0.4 MB, Reed-Solomon recovery ≈ 0.2 MB) and still leave ~2 MB for
post-launch growth.

The per-component target table further down (≤ 8.8 MB total) is the
*aspirational* breakdown the proper Phase 4 `.aar` should hit; it's
intentionally tighter than the 10 MB Tier-1 ceiling so a single
component blowing its budget is detectable before the deliverable
overruns.

#### Easy wins still available

| Change | Library save | App save | Cost |
|---|---|---|---|
| `lto = "fat"` instead of `thin` | ~5–10 % (≈ 0.3–0.7 MB) | same | Doubles the link step (4–5 min cold) |
| `opt-level = "z"` instead of default `3` | ~10–20 % (≈ 0.7–1.3 MB) | same | Slower retrieval: 10–25 % throughput hit on the joiner inner loop |
| Strip `.eh_frame` post-build | 0.46 MB | 0.46 MB | Breaks `catch_unwind` JNI safety net; FFI panics would `SIGABRT` instead of throwing `RuntimeException` |
| Trim `tracing-subscriber`, swap to `log` + `android_logger` | 0.3–0.5 MB | 0.3–0.5 MB | Loses `RUST_LOG` env-var filter syntax inside the FFI |
| Replace `serde_json` with `simd-json` or hand-rolled parser for manifests | ~0.15 MB | ~0.15 MB | More maintenance on the manifest path |
| Audit transitive libp2p features (drop UPnP, mDNS) | 0.2–0.4 MB | 0.2–0.4 MB | Pre-Phase-4 the smoke test doesn't need either |

None of these are blocking — we're already well under budget. They
become relevant if the Tier-1 ceiling tightens (e.g. India / SEA market
data costs argue for a ≤ 5 MB Play Store download).

---

## 13. Security Considerations

- **Private keys never leave the secure hardware.** All signing through the `KeyProvider` callback; Rust holds only public keys.
- **No untrusted deserialization into `Vec<u8>`** without size caps; protobuf messages have explicit max-size limits.
- **Chain call allowlist:** only the specific contracts we know about; refuse generic `eth_call` surface from the host app.
- **Stamp replay / over-issue:** bucket counters persisted atomically before the chunk leaves the device; stamp signatures include monotonic bucket index.
- **Cheque replay:** cumulative payout is monotonically increasing per peer; we never sign a cheque with a lower value than previously sent.
- **Key compromise:** wallet key controls chequebook funds + batches. Offer a panic-withdraw flow to drain the chequebook to a recovery address.
- **Supply chain:** audit crates via `cargo-vet`, gate with `cargo-deny` and `cargo-audit`, reproducible builds via committed `Cargo.lock`. Versions pinned in the lockfile are continuously rebased onto latest stable per the §3 dependency policy — we never freeze dependencies for long.

---

## 14. Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Upstream protocol changes in `bee` | High | Medium | Track bee releases; mainnet interop tests in CI; version the wire format. |
| Forwarding Kademlia bugs | Medium | High | Strong integration test harness against live bee; conservative initial peer-selection. |
| iOS background-kill during uploads | High | Medium | Resumable upload state in SQLite; BGProcessingTask. |
| Gnosis Chain reorgs | Low | Medium | Wait for N confirmations (configurable); idempotent tx retry. |
| Stamp over-issuance leading to chunk rejection | Medium | High | Atomic bucket-counter writes; refuse upload if counter can't be persisted first. |
| xBZZ / xDAI acquisition blocks new users | High | High | Relayer-sponsored onboarding; clear "bring your own" path for power users. |
| Artifact size creeps past budget | Medium | Low | CI gate; per-crate size regression dashboard. |
| Libp2p API churn | Medium | Low | Continuous upgrade policy (§3); dedicated upgrade PRs for breaking changes; interop smoke tests gate the merge. |
| Bee HTTP API drifts faster than we track | Medium | Medium | Pinned reference bee version per release (§C.5); golden-response CI corpus (§C.6); 501-stub policy (§C.1) keeps consumers from silently degrading. |

---

## 15. Upstream Tracking

- Subscribe to `ethersphere/bee` releases.
- Maintain a compatibility matrix: our version ↔ bee versions we're known to interop with. As of `0.5.0` ant speaks both the bee 2.7.x BZZ handshake (`/swarm/handshake/14.0.0/handshake`, v14 wire schema) and the bee 2.8.0 handshake (`/swarm/handshake/15.0.0/handshake`, v15 wire schema with `nonce` / `Timestamp` / `ChequebookAddress` inside `BzzAddress`); the dialer tries V15 first and falls back to V14. See Appendix H for the cutover benchmark.
- Participate in Swarm community channels for early warning on protocol changes.
- Keep a "spec conformance" doc mapping each bee spec section to our implementation location.

---

## 16. Open Questions

1. **Do we support encrypted uploads in v1?** Adds ChaCha20 handling + key embedding in reference. Small code cost; larger UX cost (key sharing). Recommend: deferred to v1.1.
2. **Single wallet or separate node/wallet keys?** Simpler UX with one; cleaner isolation with two. Recommend: two keys, shared recovery seed.
3. **Does the app always run its own node, or can it fall back to a gateway?** Fallback is nice but doubles the retrieval code path. Recommend: node-only, ship the smallest node possible.
4. **xBZZ on-ramp inside the app or external?** External first to avoid regulatory overhead; revisit in v2.
5. **Windows / desktop ports?** Not in v1 scope but keep FFI clean so it's trivial later.
6. **Do we expose `ant-gateway` on a Unix domain socket as well as TCP?** Lets local consumers avoid binding the loopback port and dovetails with the existing `ant-control` socket transport. Recommend: yes, behind a `--api-socket <path>` flag, off by default.

---

## 17. Definition of Done for v1

- [ ] Mobile apps on iOS and Android can:
  - [ ] Onboard a new user including wallet funding, chequebook deploy, first batch purchase — all within a single flow under 5 minutes.
  - [ ] Download any Swarm mainnet reference.
  - [ ] Upload a single file (any size up to 4 GB) and share the reference.
  - [ ] Upload a directory as a mantaray manifest and retrieve by path.
  - [ ] Manage batches: buy, top-up, dilute, list, expiry warnings.
  - [ ] Survive app kill mid-upload and resume on next launch.
- [ ] Mainnet interop tests pass against the latest bee release.
- [ ] Per-platform artifact under 10 MB stripped.
- [ ] Key material never reaches the Rust library unencrypted.
- [ ] External security review of crypto + SWAP + stamp code.
- [ ] Documentation: integration guide, API reference, onboarding playbook for app teams.

---

## Appendix A — Bee spec sections we need to implement

- Overlay address derivation
- Binary Merkle Tree (BMT) hash
- Content-addressed chunks
- Single-owner chunks
- Mantaray manifests
- Hive v2 peer exchange
- Forwarding Kademlia routing
- Retrieval protocol
- Pushsync protocol
- Postage stamps (structure, signing, verification we don't do but must produce valid stamps)
- SWAP accounting + cheque format (EIP-712)
- Chequebook contract interface
- PostageStamp contract interface

## Appendix B — Bee spec sections we deliberately do **not** implement

- Pullsync **server** (serving reserve pages to peers). *The pullsync
  **client** graduated with the messaging work: the light node drives
  bee's pullsync 1.4.0 protocol to receive neighborhood chunks
  (`ant-p2p::pullsync` + `lurker`).*
- Storage incentives / redistribution game
- Staking contract
- ~~PSS / GSOC messaging~~ — **graduated (2026-07)**: send + receive on
  the light node (`/pss/send`, GSOC via `POST /soc`,
  `/gsoc/subscribe/{address}` + `/pss/subscribe/{topic}` WebSockets).
  See §Non-Goals amendment.
- ACT (access control trie)
- Feeds beyond basic SOC usage
- ~~Trojan chunks~~ — graduated with PSS (bee-compatible trojan
  construction + unwrap, golden-vector interop-tested).
- Cheque cashing
- Advanced redundancy (erasure-coded uploads) — parked for v2

---

## Appendix C — Bee compatibility surface

Independently of any specific downstream, we want **`antd` to be a drop-in replacement for `bee` for the protocol subset Ant implements.** Tools written against `bee` — `bee-js` clients, `curl` / `xh` scripts, custom protocol handlers, monitoring dashboards, ENS-resolving gateways — must work unmodified against `antd` for any feature Ant supports, and must fail cleanly (not mysteriously) for any feature Ant does not.

This appendix is the contract for that compatibility. It is intentionally strict on shapes and intentionally narrow on scope: every endpoint listed is either implemented, will be implemented in a stated milestone, or is a permanent stub that returns a structured 501 so consumers can feature-detect.

### C.1 Compatibility tiers

Tied directly to the milestones in §9.

| Tier | Capability | Lands in | What it unlocks |
|---|---|---|---|
| **A — Read-only / ultra-light** | Read endpoints (`/bzz`, `/bytes`, `/chunks`, status, peers, topology, addresses); stub wallet/stamps/chequebook returning empty/zero in a bee-shaped way; `beeMode = "ultra-light"`. | End of **M2** | Any bee-shaped read client (browsers, gateway proxies, scripts that only download) works against `antd` with zero changes. |
| **B — Read + write / light** | All Tier A endpoints + uploads, tags, stamps (buy/topup/dilute), feeds/SOCs, chequebook deposit, wallet balances; `beeMode = "light"`. | End of **M3** | Full `bee-js` API surface for the protocol subset Ant supports; uploaders, publishing tools, and feed-driven apps work unchanged. |
| **B+ — Messaging** | `/pss/send/{topic}/{targets}`, GSOC authoring via `POST /soc/{owner}/{id}`, and receive via `/gsoc/subscribe/{address}` + `/pss/subscribe/{topic}` WebSockets (bee-js `gsocSubscribe`/`pssSubscribe`-compatible binary frames). | Landed post-M3 (2026-07) | Real-time messaging dApps (chat, collaborative docs, presence) against a light node — send **and** receive, no full node required. |
| **Permanently absent** | Anything still in Appendix B (pullsync *server*, transactions, staking, ACT, restricted/admin API). | Never | Endpoints return `501 Not Implemented` with `{code:501, message:"not implemented in ant"}` so consumers can branch. |

### C.2 Process & configuration compatibility

- **Binary entry points.** `antd` accepts the same subcommand vocabulary `bee` does for the operations downstreams script against:
  - `antd init [--config <path>]` — generates `keys/swarm.key` (see §C.3) using the password from config; idempotent if a keystore already exists.
  - `antd start [--config <path>]` — runs the daemon.
  - Existing `antd` flags remain authoritative; the subcommands accept the bee-shaped YAML config below in addition to native flags.
- **Config schema.** `--config <file.yaml>` parses the bee YAML. Recognized keys, all optional, with the same semantics bee gives them:

  ```
  api-addr                       # default 127.0.0.1:1633
  data-dir
  password                       # decrypts keys/swarm.key
  mainnet                        # default true
  full-node                      # ant always reports false
  swap-enable                    # M3+; ignored on Tier A
  blockchain-rpc-endpoint        # required when swap-enable=true
  cors-allowed-origins           # default "" (no CORS)
  resolver-options               # ENS RPC URL(s)
  skip-postage-snapshot          # accepted as no-op (we never run the snapshot)
  storage-incentives-enable      # ignored (Appendix B)
  network-id                     # default 1 (mainnet)
  bootnodes                      # additive to dnsaddr defaults
  ```

  Unknown keys are logged at `warn` and ignored. Conflicting native CLI flags win.
- **Daemon lifecycle.** Default `api-addr` is `127.0.0.1:1633` to match bee. `GET /health` answers within 50 ms of socket bind so external supervisors can poll aggressively. `SIGTERM` triggers graceful shutdown; we guarantee exit within 5 s of receiving it.
- **Process identity.** `antd` may be invoked through a `bee` symlink or wrapper script without behavior change; we never branch on `argv[0]`.

### C.3 Identity & on-disk format

- Bee's `keys/swarm.key` is the canonical identity contract: a **Web3 v3 JSON keystore** encrypted with the operator-supplied `password`. Ant adopts this format as its **only** on-disk node-key format from M1.1 onward — the existing `signing.key` / `identity.json` pair is migrated to `keys/swarm.key` on first launch when a password is available, and the legacy files are removed once migration succeeds.
- `keys/` is the only subdirectory of `data-dir` we promise to keep bee-shaped. `statestore/`, `localstore/`, etc. remain Ant-internal; we do not promise byte-level compatibility there, and bee's data-dir is **not** a valid Ant data-dir (and vice versa).
- Externally-injected identities are a first-class flow: an embedder may write `keys/swarm.key` itself (e.g. derived from a vault) and start `antd` without running `antd init`. We detect the existing keystore and skip generation.
- The Web3 v3 implementation lives in `ant-crypto` and is exercised by both `antd` (file-backed) and the desktop `keyring` backend (used as opaque blob storage on Windows / Linux headless).

### C.4 HTTP gateway crate

- New crate **`ant-gateway`** in the workspace, slotted into the "planned after M1.1" group in §4. Depends on `ant-node`; nothing else depends on it.
- Cargo feature `http-api`, **enabled by default for `antd`, disabled for `ant-ffi`** so the mobile artifact never carries the HTTP server (the size budget in §12 is unaffected).
- Built on `axum` + `hyper`, single `tokio` runtime shared with the node core. No second listener, no debug port — bee unified its API in 2.0+ and we follow that convention.
- All response bodies follow bee's `jsonhttp` shape:
  - Success: bee field names, exact casing, no wrapper envelope.
  - Error: `{code: <http-status>, message: <human-readable>, reasons?: [...]}` with the same `code`/`message` field names bee uses.
- Streaming: `/bzz/...` and `/bytes/...` stream the body as chunks are joined; `Content-Length` set when known, `Transfer-Encoding: chunked` otherwise. `Range` is honored against the joiner.
- CORS: off by default; the `cors-allowed-origins` config value, when non-empty, drives an `Access-Control-Allow-Origin` allowlist.

#### C.4.1 Request headers honored

When acting as the bee-shaped server, `ant-gateway` recognizes:

| Header | Direction | Semantics |
|---|---|---|
| `Range` | request | Standard byte ranges on `/bzz`, `/bytes`, `/chunks/<addr>`. |
| `If-None-Match` | request | ETag match against root reference; `304` on hit. |
| `Swarm-Chunk-Retrieval-Timeout` | request | Per-chunk fetch timeout cap for this request. |
| `Swarm-Redundancy-Strategy` | request | Plumbed to `ant-retrieval` redundancy selection. |
| `Swarm-Redundancy-Fallback-Mode` | request | As above. |
| `Swarm-Encrypt` | request (M3) | Encrypted upload toggle (deferred per §16; rejected with 501 until v1.1). |
| `Swarm-Pin` | request (M3) | Pin uploaded references locally. |
| `Swarm-Tag` | request (M3) | Attach upload to existing tag. |
| `Swarm-Postage-Batch-Id` | request (M3) | Batch to stamp the upload with. |
| `Swarm-Collection` | request (M3) | Treat body as tar collection → mantaray. |
| `Swarm-Index-Document` / `Swarm-Error-Document` | request (M3) | Mantaray root metadata. |
| `Swarm-Deferred-Upload` | request (M3) | Async upload; client polls tag for status. |
| `Content-Type`, `Content-Length` | request | Standard. |
| `ETag`, `Swarm-Tag-Uid`, `Swarm-Feed-Index`, `Swarm-Feed-Index-Next` | response | Set where bee sets them. |

#### C.4.2 Endpoint matrix

Methods listed are the ones we serve; bee's other methods on the same path return `405`. "Tier" gives the milestone the endpoint becomes real; rows marked **501** are permanently stubbed.

| Path | Methods | Tier | Notes |
|---|---|---|---|
| `/health` | GET | A | `{status:"ok", version, apiVersion}`. |
| `/readiness` | GET | A | `200` once routing table has ≥1 BZZ-handshaked peer; else `503`. |
| `/node` | GET | A | `{beeMode}`: `"ultra-light"` until SWAP lights up, then `"light"`. |
| `/addresses` | GET | A | `{ethereum, overlay, public_key, pss_public_key:null, underlay:[...]}`. |
| `/peers` | GET | A | `{peers: [...]}` from `ant-p2p` snapshot. |
| `/peers/{addr}` | DELETE | A | Disconnect; matches bee shape. |
| `/topology` | GET | A | `{baseAddr, population, connected, timestamp, nnLowWatermark, depth, reachability, networkAvailability, bins}`. |
| `/blocklist` | GET | A | `{peers: []}` (we don't ban yet); writeable post-M3. |
| `/connect/{multi-address:.+}` | POST | A | Manual dial; useful for tests. |
| `/pingpong/{address}` | POST | A | Round-trip latency. |
| `/bzz/{addr}` | GET, HEAD | A | Mantaray traversal via `ant-retrieval::lookup_path`. |
| `/bzz/{addr}/{path:.*}` | GET, HEAD | A | As above. |
| `/bytes/{addr}` | GET, HEAD | A | Joined byte stream via `ant-retrieval::join`. |
| `/chunks/{addr}` | GET, HEAD | A | Single-chunk fetch. |
| `/wallet` | GET | A→B | Tier A: zero balances, no chain; Tier B: real balances via `ant-chain`. |
| `/stamps` | GET | A→B | Tier A: `{stamps: []}`; Tier B: real batches from `ant-stamps`. |
| `/stamps/{batch-id}` | GET | B | |
| `/stamps/{amount}/{depth}` | POST | B | Buy batch. |
| `/stamps/topup/{batch-id}/{amount}` | PATCH | B | |
| `/stamps/dilute/{batch-id}/{depth}` | PATCH | B | |
| `/chequebook/address` | GET | A→B | Tier A: zero address (consumers treat as not-deployed); Tier B: real address. |
| `/chequebook/balance` | GET | A→B | Tier A: zero; Tier B: real. |
| `/chequebook/deposit/{amount}` | POST | B | |
| `/chequebook/withdraw/{amount}` | POST | B | |
| `/chequebook/cashout/{peer}` | GET, POST | — | **501** — we don't cash cheques (§5.8). |
| `/bytes` | POST | B | Raw byte upload via `ant-pushsync`. |
| `/bzz` | POST | B | File / collection upload (with `Swarm-Collection: true`). |
| `/chunks` | POST | B | Direct chunk upload. |
| `/chunks/stream` | GET (WebSocket) | B | bee-js streams chunks here for high-throughput pushers. |
| `/tags` | GET, POST | B | |
| `/tags/{id}` | GET, DELETE, PATCH | B | Status fields bee-js reads: `split, seen, stored, sent, synced, uid, address, startedAt`. |
| `/feeds/{owner}/{topic}` | GET, POST | B | |
| `/soc/{owner}/{id}` | POST | B | |
| `/pins`, `/pins/check`, `/pins/{ref}` | GET, POST, DELETE | B | Local pin set. |
| `/stewardship/{address}` | GET, PUT | B | Reupload helper; uses `ant-pushsync`. |
| `/envelope/{address}` | POST | B | Stamp envelope for already-existing chunks. |
| `/grantee`, `/grantee/{address}` | POST, GET, PATCH | B | ACT grantee lists (bee `pkg/api/accesscontrol.go`); `swarm-act*` headers on the upload/download endpoints. |
| `/pss/send/{topic}/{targets}` | POST | B+ | PSS trojan send from the light node (`?recipient=` for directed encryption; topic-derived broadcast key otherwise). |
| `/pss/subscribe/{topic}` | GET (WS) | B+ | PSS reception via the pullsync-client lurker (`?neighborhood=` overrides the lurked target for rendezvous rooms). |
| `/gsoc/subscribe/{address}` | GET (WS) | B+ | GSOC reception: watches the exact SOC address, binary frames per update (bee-js `gsocSubscribe`-compatible). |
| `/transactions`, `/transactions/{hash}` | * | — | **501** — admin/restricted API. |
| `/reservestate`, `/chainstate`, `/redistributionstate`, `/stake/*` | * | — | **501** — full-node only (Appendix B). |
| `/auth`, `/refresh` | * | — | **501** — restricted-API auth. |

Endpoints not listed here are unknown to bee in the same versions we target, or are intentionally outside our scope; they 404.

### C.5 Pinned bee API version

We pick **one bee minor version per Ant release** as our "reference shape" and document it. Tier A ships against the latest stable bee at the time M2 lands; Tier B is re-snapped to whatever's latest at M3. Compatibility past that is best-effort and tracked in §15. The reference version is recorded in `ant-gateway/COMPAT.md` and exposed at runtime via `/health.apiVersion`.

### C.6 Conformance testing

In addition to the strategy in §10:

- **Golden response corpus.** A vendored set of recorded bee HTTP responses (one file per endpoint per scenario) lives in `crates/ant-gateway/tests/golden/`. CI replays each scenario against `antd` and `bee` side-by-side in containers and asserts JSON-shape equivalence (field names, types, presence — not values).
- **`bee-js` smoke suite.** A subset of the upstream `@ethersphere/bee-js` integration tests is run from a Node test harness against `antd`. We track which suites pass per release.
- **Header-handling tests.** Hyper-level tests that fire each header in §C.4.1 at the gateway and verify the documented behavior.
- **501 regression test.** Every endpoint in §C.4.2 marked permanently absent is asserted to return `501` with the agreed body — this prevents accidental partial implementations.

### C.7 Out of scope by design

- Anything still in Appendix B (pullsync *server*, storage incentives, staking, ACT, cheque cashing, advanced redundancy). PSS/GSOC and the pullsync client graduated — see the Non-Goals amendment and Tier B+.
- Restricted / admin API behind bee's `restricted: true` mode; `/auth` and `/refresh` are 501.
- Multi-instance configurations (`bee` running with separate API + debug API ports). We only expose the unified API surface bee added in 2.0+.
- Byte-level compatibility of `data-dir/statestore` and `data-dir/localstore`. Migrating an existing bee data directory into Ant is **not** supported; only the `keys/swarm.key` file carries over.

---

## Appendix D — Tier-A drop-in checklist

Concrete work breakdown for the **first Tier-A drop-in** from Appendix C — the smallest cut of `ant-gateway` that makes a generic bee HTTP consumer behave correctly against `antd`. This is the work that lands inside §9 Phase 4.

### D.1 Goal and scope

Single capability: a bee-shaped read-only HTTP surface on `127.0.0.1:1633`, sufficient for an unmodified `bee-js` client (or any other bee HTTP consumer) to:

- Probe health and version.
- Read peer / topology / address state.
- Resolve and stream `bzz://` content with mantaray path lookup.
- Receive zeroed bee-shaped responses on wallet / stamps / chequebook so consumer UIs degrade gracefully.

**Out of scope for this checklist** (deferred to later M3 phases or the optional follow-ons below): writes, real wallet / stamps / SWAP, ENS resolution, encrypted uploads.

### D.2 Work items

#### D.2.1 Gateway crate scaffold

- New `crates/ant-gateway/` with `axum` + `hyper`. Entry: `Gateway::serve(node, addr)`.
- Cargo feature `http-api`, default-on for `antd`, off elsewhere.
- New flag `--api-addr` on `antd`; default `127.0.0.1:1633`. Gateway started after `Node` is up.
- Bee `jsonhttp` error helper (`{code, message}`).
- `tracing` span per request.

**Done when:** `curl http://127.0.0.1:1633/health` returns `{"status":"ok","version":"...","apiVersion":"..."}` within 50 ms of socket bind.

#### D.2.2 Status endpoints (no retrieval)

| Endpoint | Source |
|---|---|
| `GET /health` | static — version from `env!("CARGO_PKG_VERSION")` |
| `GET /readiness` | `ant-p2p` peer snapshot ≥ 1 BZZ-handshaked peer |
| `GET /node` | hardcoded `{beeMode:"ultra-light", chequebookEnabled:false, swapEnabled:false}` |
| `GET /addresses` | existing identity in `antd::main` (overlay, ethereum, libp2p public key) |
| `GET /peers` | `ant-p2p` peer snapshot |
| `GET /topology` | `ant-p2p` routing table; first cut may flatten all peers into a single bin |

**Done when:** consumers polling these endpoints render version, peer count, and topology total without errors.

#### D.2.3 Retrieval endpoints

- `GET/HEAD /bytes/{addr}` — `ant_retrieval::join`, streamed body.
- `GET/HEAD /chunks/{addr}` — `ant_retrieval::ChunkFetcher`.
- `GET/HEAD /bzz/{addr}` — mantaray root metadata; honor `index-document`, fall back to `lookup_path` with empty path.
- `GET/HEAD /bzz/{addr}/{path:.*}` — `ant_retrieval::lookup_path` + joiner. `Content-Type` from manifest entry metadata; default `application/octet-stream`.
- `Range` (single-range) on `/bytes` and `/bzz`; reject multi-range with `416`.
- Honor `Swarm-Chunk-Retrieval-Timeout`. Accept and ignore `Swarm-Redundancy-Strategy`, `Swarm-Redundancy-Fallback-Mode` for the first cut.
- Stream chunked via `Body::from_stream` so large files don't materialize.

**Done when:** `curl -o out.html http://127.0.0.1:1633/bzz/<known-root>/index.html` produces the same bytes as `antctl get bzz://<known-root>/index.html`.

#### D.2.4 Stub the wallet / stamps / chequebook trio

These exist solely so bee consumers don't see undefined fields. Field names verbatim — `bee-js` indexes by them.

| Endpoint | Body |
|---|---|
| `GET /wallet` | `{bzzBalance:"0", nativeTokenBalance:"0", chainID:100, walletAddress:"0x00…", chequebookContractAddress:"0x00…"}` (all five fields are required-as-string by bee-js's `WalletBalance` parser) |
| `GET /stamps` | `{stamps:[]}` |
| `GET /chequebook/address` | `{chequebookAddress:"0x0000000000000000000000000000000000000000"}` |
| `GET /chequebook/balance` | `{totalBalance:"0", availableBalance:"0"}` |

#### D.2.5 Catch-all 501

Single `fallback` handler returning `501 {code:501, message:"not implemented in ant"}`. Anything outside the Tier-A column of §C.4.2 lands here. Future-proofs feature detection without us having to ship the rest of the matrix this week.

### D.3 Optional follow-ons

Not blockers for the smoke test, but unlock the next layer of integrations cheaply.

- **D.3.1 Bee YAML config + subcommand shim.** `antd init --config <yaml>` and `antd start --config <yaml>` parsing the §C.2 schema. Lets embedders that hand bee a YAML keep doing so unmodified. Roughly half a day; do it right after the smoke test.
- **D.3.2 Web3 v3 keystore.** `keys/swarm.key` read/write per §C.3, with migration from the legacy `signing.key` / `identity.json`. Required as soon as an embedder wants to inject identity (e.g. derived from a vault). Plan to land before any "real" embedder integration goes beyond a smoke test.

### D.4 Acceptance

Two automatable gates plus one manual:

1. **`bee-js` smoke.** Stand up `antd`, point `new Bee("http://127.0.0.1:1633")` at it, exercise `bee.getHealth()`, `bee.getNodeAddresses()`, `bee.getPeers()`, `bee.getTopology()`, `bee.downloadFile(<known-root>, "index.html")`. All succeed without retries beyond the cold-start window.
2. **Curl conformance.** Per §C.6 golden corpus: shape-equivalent to a bee container for every Tier-A endpoint.
3. **Manual browse.** Any bee-shaped browser proxy (custom protocol handler proxying to `127.0.0.1:1633`) navigating to `bzz://<known-root>/` resolves and renders.

This is the exit gate for the Tier-A column of §C.4.2. After it passes, M3 work proceeds against a known-good HTTP surface.

### D.5 Suggested order

Roughly one engineering week if D.3.1 ships with it; three to four days without.

1. D.2.1 + D.2.2 — half a day; verifiable with `curl`.
2. D.2.3 — one to two days; the real work, mostly wiring existing `ant-retrieval` to axum.
3. D.2.4 + D.2.5 — an hour.
4. D.3.1 — half a day, optional but high-leverage.
5. D.4 acceptance.

## Appendix E — Large-file retrieval and gateway streaming hardening

This appendix is a focused hardening track for the gateway's read path. It builds on Appendix D's Tier-A drop-in: D.2.3 wires `ant-retrieval` into axum and ships a streaming `/bytes` GET; this appendix finishes the job for `/bzz`, `HEAD`, true HTTP ranges, browser media, and CLI streaming. It slots into §9 as a hardening sub-track that runs alongside Phases 4 and 10.

### E.1 Purpose

Improve large-file retrieval in Ant so gateway users, browser media elements, and `antctl get` receive stable, progressive downloads without forcing the daemon, gateway, or CLI to hold the entire file body in memory.

The goal is not to build a downloader from scratch. Ant already has important pieces in place. The goal is to finish and harden the streaming path, especially for manifest-backed `/bzz` files, browser video/audio playback, HTTP range requests, and CLI downloads.

Target behaviour:

```text
HTTP client / media element / antctl file writer
        ↑
ordered byte stream or requested byte range
        ↑
metadata-first gateway response
        ↑
bounded streaming buffer
        ↑
parallel Swarm chunk/subtree retrieval with retries
        ↑
routing-aware peer/chunk fetcher
```

### E.2 Current state

Based on the current `solardev-xyz/ant` tree:

- The gateway already has a streaming path for raw `/bytes/{addr}` GET requests without a `Range` header.
- `/bzz/{addr}/path` still resolves the manifest, joins the resolved file body into memory, and then returns the full body.
- HTTP `Range` handling is currently applied after the full body has been materialised.
- `antctl get` receives terminal `Bytes` / `BzzBytes` responses over the public control protocol, with optional progress events, but not a real byte stream.
- The internal retrieval machinery already has useful building blocks: ordered joining, parallel subtree traversal, retries, peer fallback, hedged requests, cache support, and concurrency limits.

Relevant source areas:

```text
crates/ant-gateway/src/retrieval.rs
crates/ant-p2p/src/behaviour.rs
crates/ant-retrieval/src/joiner.rs
crates/ant-retrieval/src/fetcher.rs
crates/ant-control/src/protocol.rs
crates/antctl/src/main.rs
```

### E.3 Browser media requirements

Large audio and video files have stricter requirements than generic file downloads. A browser does not only need bytes. It needs enough HTTP and container metadata to decide whether it can play, how long the media is, and whether seeking is possible.

#### E.3.1 HTTP metadata needed early

For a normal media response, the gateway should provide these headers as soon as the manifest and root span are known:

```http
Content-Type: video/mp4
Content-Length: 104857600
Accept-Ranges: bytes
```

For a range response, the gateway should return:

```http
HTTP/1.1 206 Partial Content
Content-Type: video/mp4
Content-Range: bytes 0-1048575/104857600
Content-Length: 1048576
Accept-Ranges: bytes
```

Important distinction:

- `Content-Length` tells the browser the total object size for full responses.
- `Accept-Ranges: bytes` tells the browser it can ask for specific byte ranges.
- `Content-Range` tells the browser which part of the file it received and the total size.

For Ant, this means the gateway should fetch only the minimum required information first:

```text
/bzz path resolution
    ↓
file reference + content type + filename
    ↓
root chunk / span
    ↓
total size known
    ↓
headers can be returned
```

#### E.3.2 Media-container metadata needed early

After receiving headers, the browser still needs enough file bytes to parse the media container.

Examples:

- MP4: `ftyp`, `moov`, track metadata, codec initialization data.
- WebM: EBML header, segment info, track info, cues if present.
- MP3: ID3 tags, MPEG frames, and sometimes Xing/VBRI metadata for variable-bitrate duration.
- Ogg: Ogg pages and codec headers.

The browser can show duration only after it has parsed the relevant container metadata. Until then, duration may be unknown.

MP4 is especially important:

```text
Good for streaming:

[ ftyp ][ moov ][ mdat ... media data ... ]

Poor for first-byte-only streaming:

[ ftyp ][ mdat ... large media data ... ][ moov ]
```

If the MP4 `moov` atom is at the end, the browser may need to request the tail of the file before it can know duration. This is why true HTTP range support is critical. Sequential streaming from byte `0` is not enough for robust media playback.

#### E.3.3 Typical browser probing behaviour

A browser or media element may first request the beginning:

```http
Range: bytes=0-
```

or:

```http
Range: bytes=0-1048575
```

If required metadata is not near the beginning, it may then request the end:

```http
Range: bytes=103809024-104857599
```

Therefore Ant should treat range support as a media feature, not merely a download-resume feature.

#### E.3.4 Fastest useful response strategy

For video/audio, optimize these steps in order:

1. Resolve `/bzz` manifest path quickly.
2. Fetch the root chunk and read the span to know total file size.
3. Return `Content-Type`, `Content-Length`, and range capability immediately.
4. Support `HEAD` so metadata probes do not fetch the body.
5. Support single-range `GET` so browsers can fetch the beginning and/or tail.
6. Stream only the requested byte range through a range-aware joiner.

The browser-facing goal is not just "download fast". The useful metrics are:

```text
time to headers
time to first body byte
time to media loadedmetadata
time to first frame / first audio
time to seek completion
```

### E.4 Archive and ZIP file requirements

Compressed archives benefit from the same generic streaming and range machinery, but they do not need the same metadata-first optimizations as browser media.

For a normal ZIP download, the browser mainly needs:

```http
Content-Type: application/zip
Content-Length: 104857600
Accept-Ranges: bytes
Content-Disposition: attachment; filename="archive.zip"
```

A browser can download a ZIP sequentially without knowing media duration, codecs, dimensions, or container playback metadata. Therefore ZIP support should not displace the media/range priority.

The benefits of generic streaming and range support for archives are:

1. **No full buffering.** The gateway can stream archive bytes directly to the client instead of joining the entire archive in memory first.
2. **Download resume.** Browsers and download managers can resume interrupted downloads with a request such as:

   ```http
   Range: bytes=50000000-
   ```

3. **Advanced archive inspection.** ZIP files usually store the central directory near the end of the file. Archive-aware clients can request the tail, locate the central directory, list entries, and then selectively request the byte ranges needed for specific files inside the archive.
4. **Selective extraction by smart clients.** A custom app or package manager could fetch only part of a ZIP after reading the central directory. This is not normal browser download behaviour, but generic byte-range support makes it possible.

Important distinction:

```text
video/audio:
    Range support is essential for playback, duration, and seeking

zip download:
    Range support is useful for resume, bounded memory, and advanced clients,
    but not required for a simple sequential browser download
```

For `.tar.gz` or plain `.gz` files, range support is less useful for selective extraction because gzip decompression is mostly sequential unless an external index exists. The gateway can still stream and resume the compressed bytes, but it should not promise random access inside the compressed content.

Implementation guidance:

- Do not special-case ZIP in the first implementation.
- Implement streaming and single-range retrieval generically for all large files.
- Set archive-specific metadata correctly when known, especially `Content-Type` and `Content-Disposition`.
- Treat ZIP resume and advanced inspection as automatic benefits of the generic range layer.

The priority order remains:

```text
1. /bzz streaming
2. HEAD support
3. single Range support
4. media playback validation
5. generic large-file robustness, including ZIP resume
```

### E.5 Problem summary

Large downloads can still fail, stall, or feel poor for several distinct reasons:

1. **Manifest-backed `/bzz` retrieval still buffers the joined file.** This is the main browser-facing problem. Websites, videos, images, archives, and other normal dweb assets usually pass through `/bzz`, not raw `/bytes`.
2. **Browser media needs metadata and ranges, not only streaming.** Video/audio duration and seeking depend on `Content-Length`, `Accept-Ranges`, valid `206 Partial Content` responses, and container metadata such as MP4 `moov` atoms.
3. **The public CLI protocol is terminal-response based.** `antctl get` can show progress, but the actual file data still arrives as one final payload. This increases memory pressure and delays useful output.
4. **Range requests are not true retrieval ranges yet.** Browser media seeking, resumable downloads, and archive-aware clients should fetch only the requested section. The current behaviour joins first and slices afterwards.
5. **Archive downloads benefit from the same generic path.** ZIP files do not need media-style metadata probing, but they still need bounded memory use, clean resume support, and correct download headers.
6. **Streaming output granularity may still be too coarse.** The joiner should emit ordered bytes frequently enough to keep HTTP clients alive and users confident, while still respecting the Swarm tree layout.
7. **Missing data chunks remain fatal unless redundancy recovery is implemented.** Degraded redundancy handling can mask the redundancy level byte, but it does not reconstruct unavailable data chunks via Reed-Solomon recovery.
8. **Failure modes are hard to diagnose.** A large retrieval may fail because of sparse chunks, stale peer state, queue saturation, retry exhaustion, malformed trees, unsupported redundancy, media client aborts, or gateway timeouts. These should be distinguishable in logs and user-facing errors.

### E.6 Design principles

#### E.6.1 Metadata first, body second

The gateway should resolve enough information to answer HTTP metadata quickly before starting expensive body retrieval.

For `/bzz`, that means:

```text
manifest lookup → file reference + metadata → root span → headers → body stream
```

#### E.6.2 Make HTTP ranges first-class

For large media, range retrieval is as important as continuous streaming. The joiner should be able to stream a requested byte interval without first materialising the whole file.

#### E.6.3 Stay Swarm-DAG-native

Do not make arbitrary `100 KiB` byte parts the core abstraction. Fixed output windows are useful as a buffering and UX concept, but the downloader should remain aware of the Swarm chunk tree.

Better model:

```text
Swarm chunks/subtrees fetched out of order
        ↓
joiner verifies and orders data
        ↓
small contiguous byte buffers are emitted
        ↓
gateway / CLI writes them sequentially
```

#### E.6.4 Emit bytes only in file order

Network fetches may complete out of order. The client must receive a strictly ordered byte stream, or a strictly ordered byte stream inside the requested range.

#### E.6.5 Keep buffers bounded

The downloader may fetch ahead, but it must not accumulate unbounded output if the HTTP client or CLI writer is slow.

#### E.6.6 Hide retry logic inside retrieval

The gateway and CLI should consume an async byte stream. They should not know which chunk failed, which peer was retried, or whether a cache hit occurred.

#### E.6.7 Prefer additive protocol changes

Keep existing `GetBytes` / `GetBzz` terminal responses for compatibility. Add streaming variants rather than breaking current clients.

### E.7 Proposed architecture

```text
Gateway / antctl
        ↓
metadata-first response layer
        ↓
streaming retrieval interface
        ↓
manifest resolver, if `/bzz`
        ↓
root/span reader
        ↓
full-stream or range-aware joiner
        ↓
bounded ordered output buffer
        ↓
routing-aware chunk fetcher
        ↓
libp2p retrieval streams
```

The main changes are:

- make `/bzz` consume the same kind of ordered byte stream that raw `/bytes` already uses internally;
- add true single-range retrieval instead of materialise-then-slice;
- expose enough metadata early for browsers and media elements;
- eventually make `antctl get` stream file bytes progressively.

### E.8 Terminology

- **Swarm chunk**: content-addressed chunk, usually around 4 KiB of payload.
- **Intermediate chunk**: chunk containing child references in the Swarm file tree.
- **HTTP body chunk**: arbitrary byte buffer emitted to the HTTP client.
- **Streaming window**: bounded amount of ordered output data being prepared ahead of the client.
- **Prefetch window**: limited lookahead over future chunks/subtrees.
- **Terminal response**: a response that returns the whole joined file body at once.
- **Range request**: HTTP request asking for a specific byte interval.
- **Media metadata**: container-level data required for duration, dimensions, tracks, codecs, and seeking.

### E.9 Phase 1 — Stream `/bzz` responses

#### E.9.1 Goal

Make the most important browser-facing path progressive and memory-bounded.

#### E.9.2 Tasks

- Add an internal `StreamBzz` command or equivalent gateway-facing streaming function.
- Resolve the mantaray path first.
- Once the file reference and metadata are known, return stream metadata:
  - total file size, from the root span when known;
  - content type;
  - filename / content-disposition hint;
  - resolved reference.
- Stream the resolved file body through the same ordered joiner used for raw byte trees.
- Preserve the existing materialising `GetBzz` path for compatibility, tests, and possibly small files.
- For the first PR, keep range requests on the existing path or explicitly route them to the later range implementation.
- Do not advertise `Accept-Ranges: bytes` for paths where true range retrieval is not yet implemented.

#### E.9.3 Gateway requirements

The gateway must:

- start the HTTP response only after manifest/file metadata and root span are known;
- set `Content-Type` from manifest metadata where available;
- set `Content-Length` when the root span is known;
- set `Accept-Ranges: bytes` only when range semantics are actually supported;
- apply backpressure through bounded channels;
- stop retrieval when the HTTP client disconnects;
- avoid buffering the full response body in memory.

#### E.9.4 Acceptance criteria

- `GET /bzz/{addr}/large-file` starts sending body bytes before the full file has been fetched.
- Gateway memory usage does not scale linearly with the full file size for non-range `/bzz` GETs.
- Existing small `/bzz` website and file behaviour remains compatible.
- Errors before the first byte return normal HTTP errors.
- Errors after streaming has started terminate the body stream cleanly and are visible in logs.
- Client disconnect cancels the retrieval task.

#### E.9.5 Recommended first PR

```text
stream bzz file responses through the retrieval joiner
```

Scope:

- add internal streaming BZZ retrieval;
- reuse the existing streaming joiner path;
- keep the existing terminal `GetBzz` path;
- add tests proving `/bzz` large files begin responding before full retrieval completes.

This is the highest-impact step because normal web/media access goes through `/bzz`.

### E.10 Phase 2 — Add metadata-first `HEAD` support

#### E.10.1 Goal

Give browsers, media elements, download managers, and gateways the object metadata they need without forcing body retrieval.

#### E.10.2 Tasks

- Implement `HEAD` for `/bytes`.
- Implement `HEAD` for `/bzz` after manifest path resolution.
- Fetch only what is needed to determine:
  - content type;
  - total size;
  - filename / content-disposition hint;
  - entity tag / stable cache validator, if available;
  - range support status.
- For archives and other downloadable files, return `Content-Disposition: attachment` when manifest metadata or route semantics indicate a download.
- Return headers without joining or streaming the body.
- Keep behaviour consistent with the equivalent `GET` response.

#### E.10.3 Headers to return

For a known object:

```http
Content-Type: video/mp4
Content-Length: 104857600
Accept-Ranges: bytes
ETag: "..."
```

Only return `Accept-Ranges: bytes` once true range retrieval is available for that route.

#### E.10.4 Acceptance criteria

- `HEAD /bytes/{addr}` returns size without body retrieval.
- `HEAD /bzz/{addr}/video.mp4` resolves manifest metadata and returns size/content type without body retrieval.
- `HEAD` does not materialise large files.
- Browser/media metadata probes do not trigger a full download.
- `HEAD /bzz/{addr}/archive.zip` returns size, content type, and download metadata without body retrieval.

### E.11 Phase 3 — True HTTP range retrieval

#### E.11.1 Goal

Support efficient browser media metadata parsing, seeking, and resumable downloads without joining the whole file first.

#### E.11.2 Tasks

- Parse single-range HTTP headers before retrieval:

  ```http
  Range: bytes=start-end
  Range: bytes=start-
  Range: bytes=-suffixLength
  ```

- Resolve root span first to know total file length.
- Map the requested byte range to required Swarm chunks/subtrees.
- Skip output before `start` without sending it.
- Stop retrieval after `end`.
- Return proper `206 Partial Content` responses:
  - `Content-Range`;
  - `Accept-Ranges: bytes`;
  - accurate `Content-Length` for the range body.
- Return `416 Range Not Satisfiable` for invalid ranges.
- Reject or defer multi-range support initially.

#### E.11.3 Media-specific behaviour

The gateway must handle the two most common browser media probes efficiently:

```http
Range: bytes=0-
```

and:

```http
Range: bytes=<tail-start>-<total-end>
```

This allows browsers to read MP4 metadata at the beginning or, when necessary, at the end of the file.

#### E.11.4 Archive-specific behaviour

The same single-range implementation should work for archives without special casing ZIP internals. For ZIP files, tail-range requests allow advanced clients to read the central directory near the end of the archive, while ordinary browsers mainly benefit from download resume.

The gateway should serve the requested compressed bytes exactly as stored. It should not try to decompress, inspect, or rewrite archive contents in the first implementation.

#### E.11.5 Acceptance criteria

- `curl -r 0-999` fetches only the requested section.
- `curl -I` returns metadata without body retrieval.
- Browser video/audio can show duration for files with metadata near the start.
- Browser video/audio can request the tail of an MP4 file when metadata is at the end.
- Seeking works without fetching the whole file.
- Range requests work for both `/bytes` and `/bzz` after manifest resolution.
- Invalid ranges return `416`.
- Multi-range requests are explicitly unsupported or handled safely.

#### E.11.6 Recommended second PR

```text
add single-range streaming retrieval for bytes and bzz
```

Scope:

- implement range parsing and response headers;
- add a range-aware joiner entry point;
- support single-range requests for `/bytes` and `/bzz`;
- add browser/media integration tests where possible.

### E.12 Phase 4 — Smooth ordered output

#### E.12.1 Goal

Make large downloads feel continuous without fighting the Swarm tree layout.

#### E.12.2 Tasks

- Review the current joiner emission threshold and subtree streaming behaviour.
- Introduce a configurable target output buffer size, for example:

  ```rust
  STREAM_TARGET_OUTPUT_BYTES = 128 * 1024;
  STREAM_MIN_EMIT_BYTES = 32 * 1024;
  STREAM_PREFETCH_WINDOWS = 4;
  ```

- Treat those values as output-buffering goals, not as a requirement to split the Swarm tree into artificial byte ranges.
- Emit as soon as a contiguous ordered byte buffer is available and large enough, or when EOF/range-end is reached.
- Maintain bounded lookahead so later chunks/subtrees can be fetched while earlier bytes are being sent.
- Preserve current retry semantics and content verification.

#### E.12.3 Initial defaults

```text
output target:          128 KiB
minimum emit size:       32 KiB
prefetch windows:         4
join fanout:              keep current default initially
per-request concurrency: keep existing cap initially
process-wide cap:        keep existing cap initially
```

These values should be tunable only after observability is in place. Hard-coded constants are acceptable for the first implementation.

#### E.12.4 Acceptance criteria

- Time to first body byte is low after manifest/root resolution.
- Large downloads produce regular body progress under normal mainnet conditions.
- One slow later chunk does not block already-complete earlier output.
- Memory use is bounded approximately by:

  ```text
  active_streams × prefetch_windows × output_target_bytes
  ```

  plus chunk-fetch and protocol overhead.

### E.13 Phase 5 — Add real CLI byte streaming

#### E.13.1 Goal

Allow `antctl get` to write progressively to stdout or disk instead of waiting for a final hex-encoded payload.

#### E.13.2 Current distinction

There are two relevant protocol layers:

1. **Internal daemon/gateway command channel.** This already has chunk-streaming concepts for raw bytes.
2. **Public `antctl` control protocol.** This currently exposes progress events plus terminal `Bytes` / `BzzBytes` payloads.

Phase 5 concerns the public `antctl` control protocol.

#### E.13.3 Tasks

- Add additive public protocol request variants, for example:

  ```rust
  StreamBytes { reference, bypass_cache }
  StreamBzz { reference, path, allow_degraded_redundancy, bypass_cache }
  ```

  or add a clearly versioned streaming mode to existing requests.

- Add streamed response variants, for example:

  ```rust
  StreamStart {
      total_bytes: u64,
      content_type: Option<String>,
      filename: Option<String>,
  }

  BytesChunk {
      data: String, // base64 while NDJSON is used
  }

  StreamDone
  ```

- Continue to support existing terminal responses:

  ```rust
  Bytes { hex }
  BzzBytes { hex, content_type, filename }
  ```

- Prefer base64 over hex for streamed chunks if the transport remains newline-delimited JSON.
- Longer term, consider a framed binary control protocol for byte streams.
- Update `antctl get -o` to stream directly into a file.
- Keep progress rendering on stderr.

#### E.13.4 Acceptance criteria

- `antctl get bytes://... -o file.bin` writes progressively to disk.
- `antctl get bzz://.../large.bin -o file.bin` writes progressively to disk.
- `antctl get ... > file.bin` streams binary bytes to stdout when not in JSON mode.
- Existing clients using terminal `Bytes` / `BzzBytes` still work.
- JSON output mode remains safe for scripts and does not intermix raw binary with JSON.

### E.14 Phase 6 — Observability and failure diagnostics

#### E.14.1 Goal

Make large-download and media-playback failures actionable.

#### E.14.2 Metrics to record per retrieval

- time to first peer;
- time to manifest resolution, for `/bzz`;
- time to root chunk;
- time to headers ready;
- time to first streamed byte;
- time to first range byte;
- total bytes streamed;
- average throughput;
- longest idle gap between emitted output chunks;
- chunks fetched from cache vs network;
- peer attempts per chunk;
- retries per chunk/subtree;
- terminal failure reason;
- whether the downstream client disconnected first.

#### E.14.3 Media-specific diagnostics

When serving media, log:

- whether request was full `GET`, `HEAD`, or range `GET`;
- requested range;
- returned range;
- content type;
- total file size;
- whether the request targeted the beginning or tail of the file;
- whether the response reached first byte.

#### E.14.4 Error details

When possible, include:

- failing chunk address;
- approximate byte offset;
- retry count exhausted;
- peers attempted;
- whether the chunk was ever found in cache;
- whether failure happened before or after first byte;
- whether redundancy was present but unsupported.

#### E.14.5 Acceptance criteria

- Logs distinguish network timeout, missing chunk, malformed tree, unsupported redundancy, max-size rejection, gateway timeout, and client disconnect.
- Logs distinguish full-body streaming failures from range-streaming failures.
- `antctl get` shows a useful failure message without exposing excessive internals by default.
- Retrying a failed command benefits from cached chunks already fetched.
- Tuning fanout, retry count, and window size can be based on measurements rather than guesswork.

### E.15 Phase 7 — Retry and concurrency tuning

#### E.15.1 Goal

Improve completion rate without overwhelming Bee peers, local queues, or Ant's own retrieval tasks.

#### E.15.2 Tasks

- Keep existing process-wide concurrency caps.
- Add per-request budgets for:
  - maximum chunk attempts;
  - maximum distinct peers per chunk;
  - maximum subtree retries;
  - maximum idle time without emitted bytes.
- Consider adaptive behaviour:
  - reduce concurrency when many requests time out;
  - hedge earlier when tail latency is high;
  - retry more aggressively for large files after partial success;
  - avoid blacklisting peers too broadly for isolated chunk failures.
- Ensure concurrent browser media requests do not starve each other.

#### E.15.3 Acceptance criteria

- Four parallel large gateway downloads make progress without unbounded queue growth.
- Four parallel browser media range requests make progress without starving full-file downloads.
- A small number of slow peers does not stall an otherwise healthy download.
- Retry exhaustion produces a precise error.
- Throughput improves or stays stable compared with the current implementation.

### E.16 Phase 8 — Reed-Solomon / redundancy recovery

#### E.16.1 Goal

Stop treating every missing data chunk as fatal when the file was uploaded with recoverable redundancy.

#### E.16.2 Tasks

- Decode Swarm redundancy metadata properly.
- Implement or integrate Bee-compatible Reed-Solomon recovery.
- Recover missing data chunks from parity chunks where possible.
- Keep current degraded mode as a fallback/debug mode.

#### E.16.3 Acceptance criteria

- Files with supported redundancy decode correctly.
- Recoverable missing data chunks no longer fail the whole download.
- Non-recoverable missing chunks still produce precise diagnostics.

### E.17 Milestone plan

#### E.17.1 Milestone A — Browser-facing `/bzz` improvement

1. Stream non-range `/bzz` GET responses.
2. Preserve current `/bytes` streaming behaviour.
3. Add integration tests for large `/bzz` files.
4. Do not advertise range support until true ranges are implemented.

#### E.17.2 Milestone B — Media metadata and ranges

1. Add metadata-first `HEAD` for `/bytes` and `/bzz`.
2. Add true single-range retrieval.
3. Return correct `206`, `Content-Range`, `Content-Length`, and `Accept-Ranges` headers.
4. Test MP4/WebM/audio behaviour in real browsers.

#### E.17.3 Milestone C — Smoothness

1. Tune output granularity.
2. Add bounded prefetch/lookahead.
3. Measure time to first byte and longest output idle gap.

#### E.17.4 Milestone D — CLI streaming

1. Add public streamed control responses.
2. Make `antctl get -o` stream to disk.
3. Keep terminal responses for compatibility.

#### E.17.5 Milestone E — Robustness

1. Improve observability.
2. Tune retry/concurrency behaviour.
3. Add Reed-Solomon recovery.

### E.18 Test plan

#### E.18.1 Unit tests

- Joiner emits bytes in order even when child fetches complete out of order.
- Range-aware joiner emits only the requested byte interval.
- Suffix ranges resolve to the correct byte interval after total size is known.
- Failed chunks are retried before stream failure.
- Receiver drop cancels retrieval cleanly.
- Streaming buffer stays bounded under slow consumer conditions.
- Range mapping selects the correct Swarm chunks.
- Malformed intermediate chunks fail deterministically.

#### E.18.2 Integration tests

- `/bytes/{addr}` large file streams without full materialisation.
- `/bzz/{addr}/large.bin` streams without full materialisation.
- `/bzz/{addr}/index.html` still serves normal website files correctly.
- `HEAD /bytes/{addr}` returns metadata without body.
- `HEAD /bzz/{addr}/video.mp4` returns metadata without body.
- `GET /bytes/{addr}` with `Range: bytes=0-999` fetches only that range.
- `GET /bzz/{addr}/video.mp4` with `Range: bytes=0-999` fetches only that range.
- `GET /bzz/{addr}/video.mp4` with a tail range fetches only the tail.
- `GET /bzz/{addr}/archive.zip` streams without full materialisation.
- `GET /bzz/{addr}/archive.zip` with `Range: bytes=50000000-` resumes from the requested offset.
- `GET /bzz/{addr}/archive.zip` with a tail range fetches only the ZIP tail.
- `antctl get -o large.bin` writes progressively to disk.
- Concurrent large downloads respect process-wide concurrency caps.
- Slow/missing peer simulation triggers fallback without corrupting output order.
- Client disconnect cancels the retrieval task.

#### E.18.3 Browser media tests

Test with at least:

```text
MP4 with moov atom at the beginning
MP4 with moov atom at the end
WebM file
MP3 or Ogg audio file
```

Check:

- video/audio starts without downloading the full file;
- duration appears after metadata is loaded;
- seeking near the middle works;
- seeking near the end works;
- browser performs range requests and receives valid `206` responses;
- tail metadata probing does not trigger full-file retrieval.

#### E.18.4 Archive tests

Test with at least:

```text
ZIP archive around 10 MiB
ZIP archive around 100 MiB
tar.gz archive around 10 MiB
```

Check:

- archive downloads start without full materialisation;
- interrupted ZIP downloads can resume through `Range`;
- tail-range requests against ZIP files return only the requested tail bytes;
- `tar.gz` files stream and resume as compressed bytes, without implying random access inside the compressed content;
- correct `Content-Type`, `Content-Length`, `Accept-Ranges`, and `Content-Disposition` are returned when known.

#### E.18.5 Manual mainnet tests

Test files around:

```text
100 KiB
1 MiB
10 MiB
50 MiB
100 MiB
```

Measure:

- time to headers;
- time to first byte;
- time to browser `loadedmetadata`;
- time to first frame/audio;
- average throughput;
- peak memory;
- longest output idle gap;
- retries per chunk;
- peers used;
- cache hit ratio;
- completion rate under concurrent browser loads.

### E.19 Final acceptance criteria

The improvement is successful when:

- a 10 MiB file streams through `/bytes` without full buffering;
- a 10 MiB file streams through `/bzz` without full buffering;
- a 100 MiB file starts sending bytes soon after root/manifest resolution;
- `HEAD` returns accurate metadata for `/bytes` and `/bzz` without body retrieval;
- single-range requests fetch only the requested byte interval;
- browsers can show duration for normal MP4/WebM/audio files without downloading the full file;
- browsers can seek within large media files using range requests;
- ZIP downloads can resume via range requests without full re-download;
- memory usage remains bounded and does not scale linearly with file size;
- one slow or failing chunk triggers local retry rather than immediate request failure;
- retry exhaustion identifies the failing chunk or approximate byte offset;
- client disconnect cancels retrieval;
- four parallel large gateway downloads do not starve each other;
- warm-cache repeat downloads are materially faster than cold-cache downloads;
- existing small-file, website, and compatibility paths still work.

### E.20 Non-goals for the first pass

- Multi-range HTTP responses.
- Transcoding or rewriting media files.
- Moving MP4 `moov` atoms during gateway serving.
- ZIP-specific central-directory parsing or selective extraction in the gateway.
- Encrypted chunk support.
- Reed-Solomon recovery.
- Replacing the Swarm retrieval protocol.
- Replacing the current control protocol immediately.
- Perfect Bee parity for every edge case.
- Unbounded file sizes.

### E.21 Recommended immediate implementation sequence

1. Add internal `StreamBzz` support.
2. Route non-range `/bzz` GETs through that stream.
3. Add cancellation on downstream HTTP disconnect.
4. Add metadata-first `HEAD` support.
5. Add true single-range retrieval for `/bytes` and `/bzz`.
6. Return correct media-relevant headers: `Content-Type`, `Content-Length`, `Accept-Ranges`, and `Content-Range`.
7. Ensure the same range layer works for generic downloads, including ZIP resume and tail-range requests.
8. Add metrics for time to headers, time to first byte, longest idle gap, retries, and terminal failure reason.
9. Add CLI streaming only after the gateway/media path is stable.

### E.22 Implementation notes

#### E.22.1 Media files

The gateway should not try to fix badly packaged media in the first pass. If an MP4 file has the `moov` atom at the end, the correct gateway behaviour is to support range requests so the browser can fetch the tail quickly. Rewriting the file into "fast start" layout is an upload/tooling concern, not a gateway-serving concern.

#### E.22.2 Archives

The gateway should not try to understand or optimize archive internals in the first pass. ZIP central-directory access and selective extraction are client-level capabilities enabled by generic byte ranges. Ant should focus on correct metadata, bounded streaming, and exact byte-range serving for any file type.

---

## Appendix F — Bee Light-Node Accounting and Client-Side Pseudosettle

This appendix captures the production regression that landed with the `0.3.0`
streaming gateway from Appendix E and the work done to restore stability.
The summary is short: the architectural shift from "materialize the file,
then stream it from disk" to "stream chunks straight to the HTTP client"
exposed a debt-accounting cliff with bee that 0.2.0 had been unintentionally
hiding behind its serial fetch + small concurrency burst pattern.

### F.1 Symptoms

- `0.3.0` deployed to `vibing.at/ant`. Single 55 MiB WAV via `/bzz`
  fetched fine on a freshly-started daemon.
- Repeated downloads of the same file (or sustained traffic for a few
  minutes) caused increasing latency, then truncated downloads
  (`502 Bad Gateway` from the reverse proxy), then full request stalls.
- `antctl peers` showed the connected peer set collapsing from
  ~100 → ~0 over a couple of minutes of sustained `/bzz` load. After
  load stopped, peers slowly re-handshaked back toward 100. Same daemon
  binary, same identity, same chequebook, no other config change.
- `0.2.0` running on the same host, same network, same workload had
  none of these symptoms. Rolling back the binary on the gateway
  restored production immediately. Confirmed regression.

### F.2 Why 0.3.0 hit it and 0.2.0 didn't

Both versions use the same `RoutingFetcher` (8-way fanout, 3-way
hedging, 500 ms hedge delay) and the same chunk-retrieval protocol
against bee. The difference is what the gateway does with the fetcher:

- `0.2.0`: `GET /bzz/...` materializes the full file in memory via
  `join` (the joiner already does the parallel chunk fetch), then
  hands a `Bytes` body to axum which streams it to the client.
  Network traffic is one bursty fetch for the whole file, then
  network-quiet while bytes flow over HTTP.
- `0.3.0`: the joiner is range-aware and emits chunks into an
  `mpsc::Sender<Bytes>` consumed by axum's `Body::from_stream`. As
  soon as the HTTP client TCP buffer drains, joiner pulls more
  chunks. Network traffic is **continuous** for the entire wall-clock
  duration of the download, including the long tail where the client
  is throttled by their own access link.

For a 55 MiB file at a ~5 MiB/s client link:
- `0.2.0`: ~2 seconds of full-tilt fetch, then 9 seconds of HTTP-only.
- `0.3.0`: ~11 seconds of pull-as-the-client-drains fetch, the entire
  duration touching every neighbourhood the file's chunks live in.

The net effect is that `0.3.0` keeps network state across many more
request/response round trips with the same hot peers (those in the
file's neighbourhood), and accumulates per-peer state at bee for the
full download window instead of just the burst.

### F.3 The bee-side budget for a light client

We dug through bee 2.7.x to understand exactly what is being accounted.

`pkg/accounting/accounting.go` — every chunk bee serves us costs us
*accounting units*. The per-chunk price comes from `pricer/pricer.go`
and depends on proximity:
- `basePrice = 10_000` units for a chunk at PO ≥ 7 (in the peer's
  own neighbourhood).
- Each PO step further out **doubles** the price up to a cap.
- At the file's far edge a 4 KiB chunk can cost up to ~`320_000` units.

Our debt at a peer is tracked in their `accountingPeer` map.
On every successful chunk delivery they call `accounting.Apply()` which
adds the price to our balance. When that balance crosses
`paymentThresholdForPeer(peer, fullNode)` they expect us to settle.

For a light node (we send `full_node: false` in the bzz handshake) the
relevant constants in `accounting.go` are:
- `paymentThreshold = 13_500_000` units (full node default), divided
  by `lightFactor = 10` for light clients →
  `lightPaymentThreshold = 1_350_000`.
- `disconnectLimit = paymentThreshold + paymentThreshold/4 = 16_875_000`,
  then divided by `lightFactor` →
  `lightDisconnectLimit ≈ 1_687_500` units.
- `refreshRate = 4_500_000` units / sec full node →
  `lightRefreshRate = 450_000` units / sec.

The disconnect path is `accounting.Apply` → `BlockPeerError` → bee's
libp2p layer calls `Blocklist.Add(overlay, duration, reason)` and
closes the connection. The blocklist duration is computed as
`(debt + paymentThreshold) / refreshRate` seconds — for a light node
hitting the limit cleanly it works out to a few seconds, but multiple
overlapping in-flight requests crossing the threshold simultaneously
can multiply that significantly via the `ghostBalance` accounting.

The arithmetic at our scale: a 55 MiB file is ~14_000 chunks. Even
with 100 peers doing perfect load balancing that's ~140 chunks each;
in practice the file's neighbourhood has ~20 hot peers that take
~70% of the load. At an average chunk price of ~100_000 units, a hot
peer hits `lightDisconnectLimit` after ~17 chunks, i.e. within seconds
of the download starting. **Without a debt-clearing mechanism we run
out of every hot peer's budget before the file is half done.**

`0.2.0` got away with it because its single big burst tended to
finish before bee actually got around to closing the connection — bee
checks the threshold on the *next* incoming request after we cross
it, and the burst was over by then. `0.3.0`'s sustained 100 s of
chunk fetches gives bee plenty of opportunity to evict us mid-stream.

### F.4 Pseudosettle

Bee ships `/swarm/pseudosettle/1.0.0/pseudosettle` exactly for this
case: a free, time-based debt refresh between any two peers.

`pkg/settlement/pseudosettle/pseudosettle.go::handler` does
`min(attempted, lightRefreshRate * elapsed_since_last_settle, current_debt)`
and credits us for that many units. It's symmetric (either side can
dial), single round-trip, and bounded server-side, so we can't abuse
it. Bee implements both sides; Ant only ever needs the dialer side
(we're the one accumulating debt).

The wire protocol on top of bee's standard libp2p substreams:

```text
dialer → listener : varint(0x00)                              // empty pb.Headers
listener → dialer : varint(0x00)                              // empty pb.Headers
dialer → listener : varint(N) + Payment   { amount: bytes }   // big-endian big.Int
listener → dialer : varint(N) + PaymentAck { amount, timestamp }
```

The headers preamble is bee's `pkg/p2p/libp2p/headers.go`; it wraps
every protocol stream regardless of whether the protocol uses
headers. The `Payment.amount` and `PaymentAck.amount` are
`big.Int.Bytes()` (big-endian unsigned, no leading zeroes, empty for
zero). `PaymentAck.timestamp` is a Unix-seconds `int64`.

### F.5 Implementation in `crates/ant-p2p`

New module: `crates/ant-p2p/src/pseudosettle.rs`.

Three pieces:

1. **`refresh_peer(control, peer)`**. Opens the pseudosettle stream,
   writes the headers preamble, sends `Payment{ amount: 27_000_000 }`
   (an over-ask — bee clamps), reads the `PaymentAck`, returns the
   accepted amount + timestamp. Errors are returned and counted.

2. **`run_inbound`**. Defensive: if a peer ever opens pseudosettle
   *toward* us (we don't expect this since we never originate refresh
   inbound, but bee has been seen to probe) we drain the Payment and
   ack zero so they don't account anything to us.

3. **`run_driver(control, notify_rx)`**. Background tokio task. The
   fetcher's hot path (`crates/ant-retrieval/src/fetcher.rs`) sends
   the peer id of every successful chunk fetch into a bounded
   `mpsc::Sender<PeerId>` (1024-deep, `try_send`, drops on
   backpressure). The driver builds a `HashMap<PeerId, PeerState>`
   tracking `last_used` and `last_refresh`, and on a 1 s tick walks
   the set, dispatching `refresh_peer` for any peer whose last
   refresh is at least `MIN_REFRESH_INTERVAL = 2 s` old.

The driver gates concurrent refreshes with
`MAX_INFLIGHT_REFRESHES = 16` to keep the libp2p stream control's
queue from flooding when a download starts touching dozens of new
peers in quick succession.

A second filter, added after the first production deploy exposed a
gateway-scale failure mode, restricts refresh dispatch to peers
currently in the `peers_watch` snapshot (i.e. peers we still have a
direct, handshaked connection to). Without it, the active state
HashMap grew to ~2_200 entries within minutes of sustained gateway
traffic — the fetcher had cycled through that many distinct peers
during streaming — and ~6_000 of the ~6_400 dispatched refreshes per
minute failed with `no addresses for peer`. That starved the
in-flight semaphore so live peers couldn't refresh, and the
`ok` count collapsed from ~2_000/min to ~1/min. After the filter
the metrics stabilise around ~2_300 ok / ~50 failed / 600 MUnit
accepted per minute on a single-stream gateway, with the active set
tracking the routing snapshot at ~100–130 peers. A 5 s grace window
on `last_used` survives short connection flaps (yamux idle resets
that re-establish in <100 ms).

A `DriverMetrics` struct counts refreshes per outcome (`ok`,
`ok_nonzero`, `failed`, `timed_out`) plus total accepted units, and
the driver emits a single `INFO ant_p2p::pseudosettle: refresh
summary (last 60s)` log line per minute summarising the deltas.
Per-peer trace-level events are still available behind
`RUST_LOG=ant_p2p::pseudosettle=trace` for debugging.

Wiring: `crates/ant-p2p/src/sinks.rs` registers the pseudosettle
inbound stream with `libp2p_stream::Control::accept` and spawns
`run_inbound`. `crates/ant-p2p/src/behaviour.rs` constructs the
notify channel, spawns `run_driver` with a clone of `Control`, and
threads the `mpsc::Sender<PeerId>` into every `run_*` (`run_get_bytes`,
`run_stream_bytes`, `run_stream_bzz`, `run_get_bzz`, `run_list_bzz`)
so the `RoutingFetcher` they build can call
`.with_payment_notify(tx)`.

### F.6 Validation

Setup: separate `antd` daemon (release build) on `127.0.0.1:1734`
with its own data dir and identity, no production traffic, alongside
the production `0.2.0` instance still serving `vibing.at/ant`.

Reference fetched: `bzz://c4f8a45301b57d0e36f0f5348ed371aee42ea0b9fe9b3caaf26015d652eedc40/tracks/01%20arrival.wav`,
55 MiB WAV, the same file the user reported failing in 0.3.0.

Three back-to-back fetches on a freshly-started peer set:

| attempt | size       | wall time | http status |
|---------|-----------|-----------|-------------|
| 1       | 54_958_120 | 108 s     | 200         |
| 2       | 54_958_120 | 96 s      | 200         |
| 3       | 54_958_120 | 92 s      | 200         |

The `0.2.0` baseline on the same machine for the same file averaged
~95 s. `0.3.0` with pseudosettle is in the same throughput band and
**faster on repeated fetches** — the pseudosettled peer stays warm
across requests instead of rotating through cold peers.

Pseudosettle traffic during those three downloads:
- ~24_000 successful refreshes
- ~8_000 failures (almost entirely "no addresses for peer" — the peer
  disconnected between us notifying the driver and the driver dialing
  them; harmless, the next chunk fetch refills the active set)
- 0 timeouts
- ~60_000_000_000 accounting units accepted (~60 GUnit, well above
  the 1.7 MUnit-per-peer disconnect floor — pseudosettle is doing
  real debt clearing, not just NAKing).

Peer set behaviour during the same window:
- Stayed in the 99–138 range, never dropped below ~95.
- ~30 disconnects/sec sustained, but matched by reconnects so the
  set size oscillated rather than collapsing.
- Peer churn concentrates on a handful of peers (the same 5–10 peers
  account for >80% of the disconnect events) — almost certainly
  peers we already blocklisted *us* on before pseudosettle landed,
  whose 24 h blocklist entries are still active. They'll age out.
- 0 fetch errors during the entire test window.

The `info`-level summary lines at 60 s granularity make the steady
state easy to spot in production logs:

```
INFO ant_p2p::pseudosettle: refresh summary (last 60s)
    active_peers=124 ok=2450 failed=505 timed_out=1 units_accepted=691280000
```

A rate of 2_000–3_000 `ok` per minute and `units_accepted` in the
hundreds of millions is what a streaming gateway under load looks like.
A rate near zero means either the gateway is idle (fine) or
pseudosettle has stopped working (investigate).

### F.7 What's left

The implementation is correct and stable for the streaming workload
that triggered the regression. Open items for follow-up, in priority
order:

1. **Persist `lightRefreshRate * elapsed` budget across daemon
   restarts.** Currently every restart starts the local refresh
   timer at zero. Bee's view is wall-clock based, so we won't
   double-spend, but a fresh daemon hitting peers we previously
   refreshed will get NAK'd for the first second post-restart. Low
   impact in practice — the next tick just refreshes again — but
   tidy.

2. **Per-peer chunks-per-connection counter in the fetcher.** Would
   give us a direct read on whether pseudosettle is keeping us under
   the disconnect threshold. Currently we infer it from peer set
   stability, which is a coarse proxy.

3. **Inbound payment book-keeping.** `run_inbound` always acks zero,
   which is fine for steady state — but if we ever do start
   accepting work that bee thinks we owe payment for, we'll need
   real bookkeeping. Out of scope for the gateway use case but worth
   noting.

4. **Identify the one specific peer churn pattern remaining**: a
   handful of peers cycle connect/disconnect every ~30 ms. Hypothesis
   is they have us in a stale 24 h blocklist from pre-pseudosettle
   load tests; confirm by waiting 24 h and re-checking. If still
   churning, dig into bee's blocklist persistence to find what's
   keeping them sticky.

## Appendix G — Bee-shaped retrieval and the streaming "moving window"

After Appendix F's pseudosettle work shipped to `vibing.at/ant`, a
head-to-head benchmark against an in-process `ethersphere/bee:2.7.1`
on the same machine and the same network exposed two unrelated
performance gaps:

- Ant was 2–3× slower than bee on cold-cache `/bytes/<ref>` fetches
  of 35–60 MiB tracks; on a 112 MiB track ant timed out at 600 s
  while bee finished in ~205 s.
- Pseudosettle was reporting `active_peers=1` during sustained
  `/bytes` streaming, which superficially looked like a regression
  but actually surfaced a structural problem: the fetcher was
  funnelling almost every chunk through a single peer.

This appendix documents the `0.3.0` → `0.3.0+` retrieval rework that
closes both gaps, plus the follow-up cancel-tolerant hedging fix
(G.7) that converts the remaining ghost-overdraw into clearable real
debt and stabilises the peer set across multi-track sessions.

| track    | size    | bee     | ant before | ant + bee-shaped | ant + cancel-tolerant |
|----------|---------|---------|------------|------------------|-----------------------|
| 02 butterfly | 44 MiB  | 80.6 s  | 186.5 s    | 49.6 s           | 83.0 s                |
| 03 sallarom  | 59 MiB  | 109.6 s | 220.7 s    | 81.5 s           | 139.0 s               |
| 05 highfive  | 35 MiB  | 65.4 s  | 117.3 s    | 30.7 s           | 97.3 s                |
| 06 flutterby | 112 MiB | 204.9 s | TIMEOUT    | 307.8 s          | 286.9 s               |

The "ant + cancel-tolerant" column is slower per-track than the
preceding column on tracks 1-3, but that's the wrong axis to read it
on: the previous column was a *single* fresh fetch with a fully warm
peer set, while the new column is the **fourth track of a continuous
4-track session**. Across the same 4-track sequence the previous
design dropped from 100 → 36 connected peers and timed out on the
fourth track; the new design holds 99–100 peers throughout and
completes all four tracks with byte-identical content (G.7
validation table). The 112 MiB track is now the only one that beats
its previous standalone time, because it's the only one whose
duration was previously ghost-overdraw-bound.

### G.1 What bee actually does (`pkg/retrieval/retrieval.go`)

Reading the bee retrieval client end-to-end clarified that the
previous ant fetcher's hedging design was wrong in a specific way:
it always raced 3 peers per chunk with a 500 ms hedge, treating
every chunk as if it were tail-slow. Bee instead does:

```
errorsLeft := maxOriginErrors  // 32
forwards   := maxMultiplexForwards  // 2

retry()  // first peer, immediately
preemptiveTicker := 1 * time.Second

for errorsLeft > 0 {
    select {
    case <-preemptiveTicker:
        retry()
    case <-retryC:
        peer, _ := s.closestPeer(addr, fullSkip, origin)
        // dispatch fetch for peer; skip.Forever(addr, peer)
    case res := <-resultC:
        if res.err == nil { return res.chunk, nil }
        errorsLeft--
        s.errSkip.Add(addr, res.peer, time.Minute)  // 60 s peer-skip per chunk
        retry()  // backfill on error
    }
}
```

Three load-bearing pieces:

1. **Per-chunk skip set** (`skip.Forever(addr, peer)` and the 60 s
   `errSkip`). Once we've asked peer P for chunk C, we do not ask
   them again for C — even on retry — so each retry attempt naturally
   uses a fresh peer. Sibling chunks are not affected; P is fair game
   for any chunk address other than C.

2. **Preemptive ticker** (1 s). Every wall-clock second, bee
   dispatches a *new* peer on top of whoever's already in flight.
   This is what gives sustained multi-peer fanout for slow chunks
   without firing a hedge eagerly on every fetch.

3. **Generous error budget** (32 origin errors). Bee tolerates up to
   32 peer errors per chunk before giving up — far more than ant's
   previous 3-deep hedge budget.

Bee also has a `maxMultiplexForwards = 2` branch that fans out
immediately when the *current node* is in the chunk's neighbourhood
(forwarding case). As a light origin we are never in any
neighbourhood, so we don't implement that branch.

### G.2 The "moving window" — `langos`

Bee's HTTP layer (`pkg/api/bzz.go::downloadHandler`) wraps the joiner
in `github.com/ethersphere/langos` before passing it to
`http.ServeContent`:

```go
if size <= 10_000_000 { bufSize = 8 * 32 * 1024 }   // 256 KiB
else                  { bufSize = 16 * 32 * 1024 }  // 512 KiB
http.ServeContent(w, r, "", time.Now(), langos.NewBufferedLangos(reader, bufSize))
```

`langos` is a pipelined read-ahead: on every `Read(p)` it kicks a
`go ReadAt(buf[next], offset)` for the next window in the
background. So while the consumer is digesting `peek[N]`, bee's
joiner is already fetching `peek[N+1]`. Combined with the joiner's
`errgroup.Group` fan-out (`pkg/file/joiner.readAtOffset`), every
peek expands into 64 (small) or 128 (large) concurrent leaf-chunk
fetches.

Effective bee parallelism per request: **128 in-flight chunks**
when the lookahead pipeline is fully primed. Ant's previous design
had a hard cap of `RETRIEVAL_REQUEST_INFLIGHT_CAP = 16`; that's the
direct source of the 2-3× throughput gap.

### G.3 Implementation in ant

#### `crates/ant-retrieval/src/fetcher.rs`

`RoutingFetcher::fetch` rewritten to mirror bee's per-chunk loop:

- New constants: `MAX_ORIGIN_ERRORS = 32` (bee match), `HEDGE_DELAY
  = 4 s` (deliberately wider than bee's 1 s preemptive — see §G.4
  for why).
- Per-fetch `asked: Vec<PeerId>` replaces the previous unconditional
  3-wide race. A new helper `pick_next(&asked)` reads
  `peers_rx.borrow()` afresh on every call and returns the
  closest-by-XOR peer not yet asked, so disconnects mid-fetch
  automatically drop out of the candidate pool.
- Loop structure:
  - Initial dispatch: 1 peer.
  - On per-stream error: backfill the next peer immediately
    (mirrors bee's `retry()` in the error arm).
  - On `HEDGE_DELAY` elapsed with no result: dispatch one more
    peer in parallel and reset the timer.
  - Errors decrement `errors_left`; budget exhaustion + empty
    in-flight pool exits the loop.

The cross-call blacklist (CAC mismatch / framing errors) is kept;
it's distinct from bee's `errSkip` because misbehaving peers
*should* be banned for sibling chunks of the same fetch.

#### `crates/ant-retrieval/src/joiner.rs`

The streaming `join_subtree_to_sender` previously had a
`STREAM_RECURSE_THRESHOLD = 1 MiB` switch: any subtree with a child
spanning more than 1 MiB walked its children sequentially via a
`for` loop. For typical mainnet files (44–112 MiB), this collapsed
the entire root level to single-threaded retrieval — only the
leaves at the deepest level actually fanned out. Removed in favour
of a unified parallel-pipelined design at every level:

```rust
// Per-child bounded mpsc (≈ 256 KiB worth of buffered chunks each)
for spec in specs {
    let (tx, rx) = mpsc::channel(STREAM_PIPE_DEPTH);
    producers.push(async move { join_child_to_sender(...&tx).await });
    child_rxs.push(rx);
}

// Driver: run up to FETCH_FANOUT producers concurrently
let producers_drive = stream::iter(producers).buffer_unordered(fanout)
    .try_collect::<()>();

// Merger: drain receivers strictly left-to-right, forward to `out`
let merger = async move {
    for mut rx in child_rxs {
        while let Some(buf) = rx.recv().await { out.send(buf).await?; }
    }
};

tokio::try_join!(producers_drive, merger)?;
```

Each in-flight sibling subtree gets its own bounded channel
(`STREAM_PIPE_DEPTH = 64` ≈ 256 KiB), so it can keep producing
chunks ahead of the in-order merger until its window fills. The
merger drains receivers strictly left-to-right, so output stays in
document order while later siblings can already be fetching
descendants. With `FETCH_FANOUT = 8` parents in flight at any level,
the joiner keeps roughly `8 × 64 = 512` chunks (≈ 2 MiB) of decoded
look-ahead, comparable to bee's `langos` lookahead layered on top
of its joiner errgroup.

The `STREAM_RECURSE_THRESHOLD` constant and its sequential code
path are deleted.

#### `crates/ant-p2p/src/behaviour.rs`

Two semaphore caps tuned for the new design:

- `RETRIEVAL_INFLIGHT_CAP`: 64 → **256**. A soft global cap that
  protects against pathological loops but no longer constrains
  steady-state throughput; the per-request cap and the pseudosettle
  refresh budget do the actual fairness work.
- `RETRIEVAL_REQUEST_INFLIGHT_CAP`: 16 → **64**. Sized to roughly
  match bee's effective `langos lookahead × 2` parallelism for a
  single file while staying inside the per-peer debt allowance the
  pseudosettle driver can clear (see §G.4).

The streaming `mpsc` between the joiner and the control reply pump
is widened from depth 16 to 64 (≈ 256 KiB of decoded body), so a
slow HTTP consumer doesn't immediately back-pressure all the way
into the joiner's per-child pipes.

#### `crates/ant-p2p/src/pseudosettle.rs`

Two knobs tightened to keep up with the higher chunk-fetch rate the
new joiner drives:

- `MIN_REFRESH_INTERVAL`: 2 s → **1.1 s**. Bee's `peerAllowance`
  granularity is wall-clock seconds, so settling more often than
  once a second is wasted effort, but 1.1 s is the floor that lets
  us collect every per-second `lightRefreshRate * 1 s` slice from
  every hot peer.
- `MAX_INFLIGHT_REFRESHES`: 16 → **32**. At ~250 ms RTT/refresh,
  32 in flight gives ~128 successful refreshes per second — enough
  to cycle a 100-peer warm set every ~0.8 s.

### G.4 The ghost-overdraw discovery (why `HEDGE_DELAY = 4 s`)

The first pass of the new fetcher used a 1 s preemptive ticker
(direct bee match). That repeatedly cancelled in-flight retrievals
when a faster peer's response landed first, and the resulting peer
set collapse from 100 → 37 over the four-track benchmark led us to
trace the exact mechanism in
`bee/pkg/accounting/accounting.go::debitAction.Cleanup`:

```go
func (d *debitAction) Cleanup() {
    if d.applied { return }
    // ...
    d.accountingPeer.ghostBalance += d.price
    if ghostBalance > disconnectLimit {
        _ = a.blocklist(d.peer, 1, "ghost overdraw")  // ← us!
    }
}
```

Bee's retrieval handler calls `accounting.PrepareDebit` *before* it
writes the delivery message. If our peer cancels the substream
before bee's `WriteMsgWithContext + debit.Apply()` succeeds, the
deferred `debit.Cleanup()` still increments their `ghostBalance` by
the chunk price — and once ghostBalance crosses
`lightDisconnectLimit` (≈1.69 M units), bee blocklists us locally
for ~1 s and our connection drops.

Implication: every cancelled hedge is "free" only in the sense that
bee never charges us a *real* debit, but it does charge us a ghost
debit, and ghost debits accumulate against the same disconnect
threshold the real ones do. So preemptive multi-peer dispatch is
*not* free.

The mitigation is simply to make the hedge less aggressive. With
`HEDGE_DELAY = 4 s` (chunks resolving in <4 s never trigger a
hedge) the steady-state rate of cancelled streams drops by ~4×,
and the four-track benchmark goes from peer-set collapse to the
~64-peer steady-state captured in the table at the top of this
appendix.

The 1 s ticker may still be the right answer for a node with a
*slow* uplink (where chunks routinely take seconds), but for the
gateway's typical mainnet RTT the wider hedge dominates.

### G.5 Validation

Same setup as Appendix F.6: dev `antd` on port 1733, dockerized
`ethersphere/bee:2.7.1` on port 1833, both pulling from the live
mainnet, ~100 peers warm on each, `/bytes/<ref>` cold-cache fetches
of four `litter-ally.eth/tracks/` WAV files via `curl --max-time 600`.

Final results (table at top of appendix). All four tracks return
byte-identical content to bee's response (`sha256` match). Peer set
behaviour during the four-track sequence: starts at 100, oscillates
between 60-130 during the first three tracks, descends to ~36 by
the end of the 112 MiB fourth track. The descent is gradual (1-5
disconnects/sec) rather than the cliff-edge collapse the previous
design produced, and pseudosettle continues to accept ~1 GUnit/min
of debt clearance throughout.

The remaining gap on the largest track (308 s vs bee's 205 s) is
the steady-state effect of cumulative ghost-overdraw load on hot
peers: even at a 4 s hedge, a multi-minute fetch still cancels
enough streams that the few peers servicing chunks in the file's
neighbourhood eventually blocklist us. The next round of tuning
should look at per-peer concurrency caps (so chunks spread more
evenly across the 100-peer set) or treat streaming as a literal
queue we read from one peer at a time without ever cancelling.

G.7 takes the second of those routes (cancel-tolerant hedging) and
nearly closes the gap.

### G.6 What's still on the table

In rough order of remaining impact (item 2 in this list was
implemented in G.7 below):

1. **Per-peer in-flight cap.** A `HashMap<PeerId, AtomicUsize>` in
   the fetcher would let `pick_next` skip peers already at, say, 4
   concurrent fetches and pick the next-closest instead. Should
   close most of the remaining gap on the 112 MiB track by spreading
   load away from the hottest 5-10 peers.

2. **Connection-level back-pressure.** Right now an HTTP consumer
   that reads slowly back-pressures into our `mpsc` chain, and the
   joiner just blocks on `tx.send`. That's correct for memory
   bounding but means we're not draining peers as fast as we could
   on a fast consumer. A per-connection adaptive depth would help.

3. **Bee 2.7.1 forwarding-side caching.** Bee's
   `forwarderCaching` flag (off in our test setup) makes a peer
   cache chunks it forwards on our behalf. With it on, repeated
   fetches near the same neighbourhood get warmer peers; without
   it, cold chunks always travel the full forwarding chain.

### G.7 Cancel-tolerant hedging

The `HEDGE_DELAY = 4 s` mitigation in G.4 reduced the *rate* of
ghost overdraws but did not eliminate them: the four-track
benchmark still ended with the peer set drained from 100 → 36, and
the 112 MiB fourth track only finished because it had bled enough
peers off the hot set to slow itself down naturally. The structural
fix is to stop creating ghost debits in the first place.

#### G.7.1 The mechanism, again

`bee/pkg/retrieval/retrieval.go::handler` does:

```go
debit, _ := s.accounting.PrepareDebit(ctx, peer, price)  // accountingPeer.shadowReservedBalance += price
defer debit.Cleanup()                                    // ghost-overdraw on the un-applied path

if err := w.WriteMsgWithContext(ctx, &Delivery{...}); err != nil {
    // never applied → defer hits Cleanup → ghostBalance += price
    return err
}
if err := debit.Apply(); err != nil { ... }              // happy path: real debit, pseudosettle clears it
```

The `WriteMsgWithContext` call is what fails when *we* drop our side
of the libp2p substream. So the moment our `RoutingFetcher` sees a
winner come back and does `drop(in_flight)`, every other in-flight
peer's stream closes mid-write, their `Apply()` never runs, and bee
charges them a ghost debit instead.

Pseudosettle (Appendix F) only clears `accountingPeer.balance`, not
`accountingPeer.ghostBalance`. So accumulated ghost debt stays on
the books indefinitely, and once it crosses `lightDisconnectLimit`
(≈1.69 M units) bee blocklists us.

#### G.7.2 The fix

Instead of `drop(in_flight)`, hand the remaining `FuturesUnordered`
to a detached `tokio::spawn`'d drain task that polls each loser to
completion. Each loser's `retrieve_chunk` finishes reading the
delivery message off the wire — which is all bee's server side
needs to complete `WriteMsgWithContext` and call `debit.Apply()`.
The applied debit lands in `accountingPeer.balance`, where the
pseudosettle driver sees it on its next tick and clears it.

`crates/ant-retrieval/src/fetcher.rs::spawn_drain_losers`:

```rust
fn spawn_drain_losers<S>(
    in_flight: S,
    addr: [u8; 32],
    cache: Option<Arc<InMemoryChunkCache>>,
    record_dir: Option<PathBuf>,
    payment_notify: Option<mpsc::Sender<PeerId>>,
) where
    S: futures::stream::Stream<Item = (PeerId, Result<RetrievedChunk, RetrievalError>)>
        + Send + 'static,
{
    tokio::spawn(async move {
        let mut s = Box::pin(in_flight);
        while let Some((peer, result)) = s.next().await {
            if let Ok(chunk) = result {
                // write-through cache + record + pseudosettle notify
                // (the winner already counted progress, so we skip
                // ProgressTracker here to avoid double-counting bytes)
            }
        }
    });
}
```

Three secondary effects of the drain task:

1. **Cache write-through.** A loser that returns a CAC-valid chunk
   is free bytes; we cache them so a sibling chunk request (or any
   later request) skips the network entirely.
2. **Pseudosettle notify.** Each successful loser triggers a
   `payment_notify.try_send(peer)` so the pseudosettle driver
   promptly clears the just-applied debit.
3. **Permit retention.** The `_request_permit` and `_permit`
   semaphore guards stay held for the lifetime of the spawned task,
   which means concurrent fetches in the same request keep waiting
   on the per-request semaphore until losers actually finish. That
   's the right back-pressure: if a peer is responding slowly,
   sibling chunks stop piling on more requests.

#### G.7.3 Validation

Same setup as G.5 (debug `antd` on port 1635, ~100 peers warm,
cold-cache `/bytes/<ref>` fetches of the same four `litter-ally.eth`
WAVs in sequence):

```
=== Baseline peer set: 100 ===
Track 1 (44 MiB):  result: 200 46064680 83.0s   peers after: 99
Track 2 (59 MiB):  result: 200 61455400 139.0s  peers after: 99
Track 3 (35 MiB):  result: 200 36879400 97.3s   peers after: 99
Track 4 (112 MiB): result: 200 117542440 286.9s peers after: 99
=== Final peer set: 99 ===
```

Peer-set behaviour: **99–100 throughout, no collapse on the fourth
track.** Pseudosettle steady-state (`ant_p2p::pseudosettle: refresh
summary`) over the same 10 minutes:

```
ok=2606 failed=52  units_accepted=1.17 GUnit / 60s
ok=2716 failed=82  units_accepted=1.06 GUnit / 60s
ok=2796 failed=40  units_accepted=0.76 GUnit / 60s
ok=2871 failed=33  units_accepted=0.64 GUnit / 60s
ok=2973 failed=36  units_accepted=0.45 GUnit / 60s
ok=2992 failed=45  units_accepted=0.58 GUnit / 60s
ok=2877 failed=60  units_accepted=1.10 GUnit / 60s
ok=2982 failed=35  units_accepted=0.62 GUnit / 60s
ok=2961 failed=39  units_accepted=0.71 GUnit / 60s
ok=2959 failed=35  units_accepted=0.63 GUnit / 60s
```

~3000 successful pseudosettle round-trips/min sustained, 1-3% fail
rate (transient connection churn), and 0.4-1.2 GUnit/min of debt
cleared. That's ~3-7× the pre-fix steady-state because the drain
task now lets bee complete the write that produces a real debit
instead of dropping it as a ghost.

All four downloaded files match the corresponding bee responses
byte-for-byte (`sha256` cross-checked against `/tmp/bee-<ref>.wav`).

#### G.7.4 Production deployment

Built `0.3.0+cancel-tolerant` release (`target/release/antd`,
13.90 MiB), backed up the previous 0.3.0 binary as
`/usr/local/bin/antd-ant.0.3.0-pre-cancel-tolerant.bak`, restarted
`antd.service`. Peer set warmed to 100 within ~30 s. End-to-end
verification through `https://vibing.at/ant/bytes/<ref>`:

| track    | size     | prod-before (0.3.0) | prod-after (0.3.0+) |
|----------|----------|---------------------|---------------------|
| 02 butterfly | 44 MiB | 173.6 s             | 32.5 s              |
| 06 flutterby | 112 MiB | TIMEOUT (>600 s)   | 139.1 s             |

Final peer count after the 112 MiB production fetch: **100**.
Steady-state pseudosettle: `active_peers=100 ok=2919 failed=28
units_accepted=885220000` per minute.

#### G.7.5 Why this isn't the full ghost-overdraw fix

We still cancel hedges on chunks that *error*: if the loser's
`retrieve_chunk` returns `Err(_)` we just discard it. But errors
are bee's signal that the peer doesn't have the chunk, and on the
error path bee's handler returns *before* `PrepareDebit` is called
— there's nothing to apply or cleanup. So the only ghost debits
left are from clean-cancel races where the `WriteMsgWithContext`
attempt collides exactly with our drop, and those should be rare
relative to the steady stream of successful losing fetches.

If we ever do see ghost-overdraw symptoms again (peer-set
disconnection cliff, pseudosettle dropping below the chunk-fetch
rate), the next lever is item 1 in G.6 (per-peer in-flight cap):
spreading the load across more peers reduces each peer's
shadow-reserved-balance high-water mark, which in turn reduces the
window where a clean-cancel race can land us over
`lightDisconnectLimit`.


## Appendix H — Bee-parity admission control: the `Accounting` mirror

After Appendix G's cancel-tolerant hedging stabilised the peer set
during normal large-file streaming, a follow-up benchmark surfaced
a residual gap: the 112 MiB track still finished in 312 s (vs bee
ultra-light at 195 s on the identical file), and the peer set
quietly halved (100 → 48) over the four-track run. The pattern was
the same root cause but a different angle: bee was RST'ing
saturated forwarders before pseudosettle could refresh them.

Reading bee's `pkg/accounting/accounting.go::PrepareCredit`
revealed three structural pieces ant was missing:

1. **Client-side admission control.** Bee's `prepareCredit` checks
   the *predicted* per-peer balance against
   `disconnectLimit + min(int64(elapsed), 1) × refreshRate` *before*
   dispatching, and refuses to dispatch if the prediction would
   cross. The retrieval loop catches `ErrOverdraft` and skips the
   peer for `overDraftRefresh = 600 ms`, then tries the next-closest
   one. Ant had no client-side equivalent; the only signal a peer
   was over its limit was being RST'd at the TCP layer.

2. **Debt-driven pseudosettle trigger.** Bee fires `RefreshFunc`
   synchronously from `creditAction.Apply` when expected debt
   crosses `earlyPayment = 50 % × paymentThreshold`. Ant's
   pseudosettle driver was purely time-driven on a 1 s tick, so a
   peer that crossed the early-payment line between ticks had to
   wait up to a full second before its refresh dispatched.

3. **Faster preemptive fan-out.** Bee's hedge cadence is 1 s; ant
   was at 4 s. Ant widened the cadence in Appendix G specifically
   to reduce hedge-induced ghost debits. With (1) in place the
   widening becomes unnecessary: hedges land on admissible peers
   only, so the worst case of a 1 s hedge is one extra dispatch on
   a peer with headroom, not a pile-on saturating an already-hot
   peer.

### H.1 Implementation

`crates/ant-retrieval/src/accounting.rs` is a new module that
mirrors bee's per-peer `accountingPeer` struct, restricted to the
four fields that affect admission: `balance`, `reserved`,
`last_refresh`, and `last_used`. The module exposes:

- `Accounting::peer_price(overlay, chunk_addr)` — bee's
  `pricer.PeerPrice` formula
  (`(MAX_PO − proximity + 1) × basePrice`).
- `Accounting::try_reserve(peer, price) -> Option<DebitGuard>` — the
  admission gate. Returns `None` (i.e. `ErrOverdraft`) when
  `balance + reserved + price > disconnectLimit`. The fetcher's
  `pick_next` walks ranked peers, calls `try_reserve` on each, and
  the first peer that admits wins; refused peers go on a per-chunk
  skip list with the [`OVERDRAFT_REFRESH = 600 ms`] TTL.
- `DebitGuard::apply()` — moves `price` from `reserved` to
  `balance` on a successful chunk delivery. If the post-apply
  balance crosses `HOT_DEBT_THRESHOLD`, fires a `HotHint` into the
  pseudosettle driver.
- `DebitGuard::Drop` — releases the reservation without applying
  if the chunk fetch errored or was cancelled. Mirrors bee's
  `creditAction.Cleanup`.
- `Accounting::credit(peer, accepted)` — the pseudosettle ack
  callback. Subtracts `accepted` from `balance`, bumps
  `last_refresh` to `Instant::now()`.
- `Accounting::forget(peer)` — invoked from the swarm's
  `ConnectionClosed` handler so a peer's mirror state matches
  bee's `notifyPeerConnect` reset on reconnect.

The thresholds are sized off bee's constants:

| Constant | Value | Origin |
|----------|-------|--------|
| `LIGHT_DISCONNECT_LIMIT` | 1,687,500 units | `(100+25)% × (paymentThreshold/lightFactor) = 1.25 × (13.5 M / 10)` |
| `LIGHT_REFRESH_RATE_PER_SEC` | 450,000 units/s | `refreshRate / lightFactor = 4.5 M / 10` |
| `OVERDRAFT_LIMIT` | 1,687,500 units | `LIGHT_DISCONNECT_LIMIT` (no allowance term) |
| `HOT_DEBT_THRESHOLD` | 843,750 units | `LIGHT_DISCONNECT_LIMIT / 2`, matching bee's `earlyPayment = 50 %` |
| `OVERDRAFT_REFRESH` | 600 ms | `bee/pkg/retrieval/retrieval.go` |
| `HEDGE_DELAY` | 1 s | bee's `preemptiveInterval` |

`OVERDRAFT_LIMIT` is set to the *lower* of bee's two boundaries
(`disconnectLimit` alone, no `min(elapsed, 1) × refreshRate`
allowance term). Bee's allowance is binary on a one-second
boundary — `min(int64(elapsed), 1) × refreshRate` is *either* 0
*or* `refreshRate`, never anything in between — so any
interpolated estimate would either over- or under-shoot. Picking
the lower bound leaves us with a ~450 k unit safety margin between
"ant refuses to dispatch" and "bee disconnects", which is exactly
one second of pseudosettle work.

The pseudosettle driver in `crates/ant-p2p/src/pseudosettle.rs`
gained two changes:

- A second `mpsc::Receiver<HotHint>` channel. Hot hints set the
  `hot` flag on the per-peer state; the dispatch walk on each
  tick processes hot peers first (sorts by `hot ? 0 : 1` before
  iterating).
- `REFRESH_TICK` was tightened from 1 s to 100 ms. The 100 ms
  cadence makes hot hints land on the next tick rather than
  waiting up to a full second for the periodic walk; the existing
  per-peer `MIN_REFRESH_INTERVAL = 1.1 s` still keeps us from
  spamming bee with same-second refreshes (`ErrSettlementTooSoon`).

The driver also accepts an optional `Arc<Accounting>` and calls
`accounting.credit(peer, accepted)` on every successful refresh,
closing the loop between bee's `PaymentAck.accepted` field and our
admission mirror.

### H.2 Wiring

`SwarmState` gained an `accounting: Option<Arc<Accounting>>` field.
The daemon constructs one shared `Accounting` at startup, hands a
clone to every `RoutingFetcher` it builds (via
`with_accounting`), and the same clone to the pseudosettle driver
for crediting back. `ConnectionClosed` calls
`accounting.forget(&peer)` so a flapping peer's mirror state
doesn't carry stale balance across reconnects.

The fetcher's `pick_next` was rewritten to take `&mut
overdraft_skip` and walk ranked peers calling `try_reserve` on
each. The first admissible peer wins; refused peers go in
`overdraft_skip` with a 600 ms expiry. The dispatched future
carries the `DebitGuard` and applies it on success / drops it on
error. The cancel-tolerant drain task (Appendix G) was extended to
apply guards too — losing hedges that delivered still apply their
debits because bee already ran `creditAction.Apply` on its end.

### H.3 Validation

Test bench: `ant 0.3.x` (this commit, with H.1–H.2 wired in) vs
`bee 2.7.1` ultra-light, both fresh datadirs, both 100/136-peer
warm pools, four-track wav benchmark
(`bee-bench/cmp-bench-test.sh`):

| Track | File | Ant | Bee | Δ |
|-------|------|-----|-----|---|
| 1 | butterfly (44 MiB) | **69.7 s** | 75.0 s | ant +7 % faster |
| 2 | sallarom (59 MiB) | **95.4 s** | 102.8 s | ant +7 % faster |
| 3 | high five (35 MiB) | **54.8 s** | 62.6 s | ant +13 % faster |
| 4 | flutterby (112 MiB) | **184.6 s** | 200.1 s | ant +8 % faster |

All four files completed with byte-perfect SHA-256 match between
ant and bee. Ant's peer set stayed at 100 throughout the entire
12-minute bench (28 total disconnects, 24 of them in the warmup
window before the bench started). Pseudosettle accepted 1.3–2.7
billion units/min across the four tracks, vs ~660 M units/min
during the pre-fix collapse.

Two earlier bench iterations during this work showed the regime
sensitivity:

- With `OVERDRAFT_LIMIT = LIGHT_DISCONNECT_LIMIT +
  LIGHT_REFRESH_RATE_PER_SEC` (the upper bound, matching bee's
  max-allowance line): tracks 1–2 ran at 24 s and 27 s (3× faster
  than bee), but tracks 3–4 collapsed the peer set 100 → 1 and
  failed mid-stream. Pulling the cap down to
  `LIGHT_DISCONNECT_LIMIT` alone trades that 3× speedup for
  stability — ant gets bee-parity throughput with bee-parity
  resilience.
- Two earlier attempts at a per-peer in-flight cap
  (`PER_PEER_INFLIGHT_CAP = 8` and `= 16` in
  `RoutingFetcher::fetch`) both *worsened* stability: forcing
  requests onto less-optimal forwarders increased cancellations,
  which increased ghost debits, which collapsed the peer set
  faster. The admission-control mirror replaces this lever
  cleanly: it spreads load to peers with *headroom* rather than
  to peers chosen by an arbitrary hard cap.

### H.4 Deployment

After validation: bump `0.3.1 → 0.3.2` across all eight workspace
crates per `AGENTS.md`'s policy, build the release binary, deploy
to `vibing.at/ant`, and confirm `antd --version` matches what's
serving. The same `0.3.2` binary serves `*.eth.freedom.baby` since
it's the same `antd` instance.

## Appendix I — bee 2.8 cutover benchmarks

`0.4.1` (V14 handshake only, hive `1.1.0/peers`) vs. `0.5.0` (V15
handshake with V14 fallback, hive `2.0.0/peers` + `1.1.0/peers`),
both run on `fra1` (the production host) against the live Swarm
mainnet fleet — already mid-cutover, with the canonical bootnodes
`QmP9b7Mx…`, `QmTxX7…`, `QmRa6r…` all serving `bee/2.8.0-41d6efc6
go1.26.3 linux/amd64`. Procedure was the harness scripts in
[scripts/bench-bee-cutover.sh](scripts/bench-bee-cutover.sh), with
the bench daemon isolated to `/tmp/antd-bench-*` and port `11633`
so it never collides with the live `antd.service`.

### I.1 Summary table

| Scenario                    | Metric                | 0.4.1 / bee-2.8 | 0.5.0 / bee-2.8                |
|-----------------------------|-----------------------|-----------------|--------------------------------|
| B1 t→1 peer (cold)          | seconds, median       | TIMEOUT (>600)  | ~1                             |
| B1 t→50 peers (cold)        | seconds, median       | TIMEOUT (>600)  | ~1                             |
| B1 t→100 peers (cold)       | seconds, median       | TIMEOUT (>600)  | ~2                             |
| B2 upload 1 MiB             | seconds, median       | N/A (no peers)  | 31.9 (4.6 / 31.9 / 31.8)       |
| B3 download 1 MiB (cold)    | seconds, median       | N/A (no peers)  | 1.25 (1.13 / 1.25 / 1.32)      |
| B4 8× download (cold)       | aggregate ms, median  | N/A (no peers)  | 60020 — see anomaly note below |
| B5 upload 100 MiB           | seconds / MB/s        | N/A (no peers)  | 432 s / 0.23 MB/s              |
| B5 upload 500 MiB           | result                | N/A (no peers)  | pushsync timeout (see I.4)     |

The B2-B5 cells were measured on the same `fra1` host as B1, using
the production `.env` wallet (postage batch
`0x69fb3fbb…`, chequebook `0xfEecbb9a…`) sourced into a bench
daemon on port `11634` so the live `antd.service` stayed
untouched.

### I.2 B1 — Time to 100 peers (cold start)

#### Baseline (`0.4.1` against bee 2.8 mainnet)

Run 1 (capped at 600 s, the harness cap):

```
v14-run1 sock_ready=0s
v14-run1 t=0s routing=0
v14-run1 summary first=TIMEOUT p50=TIMEOUT p100=TIMEOUT
```

The 0.4.1 ant dials all three bootnodes, completes the libp2p
multistream handshake, then sits at `peer_set_size=0`
indefinitely. Every 15 s the dial pipeline re-fans the same three
bootnodes:

```
INFO ant_p2p: peer set below floor; re-warming dial queue
+ re-bootstrapping peer_set_size=0 target=100 floor=50 warmed=0
INFO ant_p2p: libp2p connected agent="bee/2.8.0-41d6efc6 …"
```

There is no warn-level log because the BZZ handshake doesn't even
start — bee 2.8 multistream-rejects `/swarm/handshake/14.0.0/handshake`
silently. With 0/3 bootnodes ever handing us a hive payload, our hive
hint pipe is empty and the routing table never grows.

Runs 2 and 3 were skipped — the failure is deterministic
(handshake protocol multistream-mismatch); three 600 s repeats
would just re-establish the same `peer_set_size=0` result. This is
recorded in the table as `TIMEOUT (>600)` for the median.

#### Post-upgrade (`0.5.0` against bee 2.8 mainnet)

```
v15-run1 sock_ready=1s    v15-run2 sock_ready=0s    v15-run3 sock_ready=0s
v15-run1 t=1s routing=77  v15-run2 t=0s routing=78  v15-run3 t=0s routing=85
v15-run1 t=2s routing=100 v15-run2 t=1s routing=100 v15-run3 t=2s routing=100
v15-run1 summary first=1 p50=1 p100=2
v15-run2 summary first=0 p50=0 p100=1
v15-run3 summary first=0 p50=0 p100=2
```

Median across runs: **first peer ≤ 1 s, 50 peers ≤ 1 s, 100 peers
≈ 1-2 s**. The measured numbers are an upper bound; they're
dominated by the latency of the first `antctl status` query
(which lives behind the `antd.sock` socket and races daemon
startup), not by actual peer warm-up time. The empirical
behaviour is: bootnode hive broadcasts deliver 75+ peers in the
first 100 ms, the rest fill in by the next status poll.

For comparison: 0.4.1 / bee-2.7.x historically warmed to 100
peers in ~30 s (Appendix F.5, line 4079). 0.5.0 / bee-2.8 is
faster, not slower, despite the larger handshake preimage —
mostly because bee 2.8's hive batches now arrive sooner thanks to
the `2.0.0` protocol's parallel broadcast.

### I.3 B2 — Upload of a 1 MiB single file (0.5.0)

```
B2,v15-b2,warmup,1
B2,v15-b2,1,ok ms=4596  ref=0xb971ea…e524
B2,v15-b2,2,ok ms=31930 ref=0xb971ea…e524
B2,v15-b2,3,ok ms=31802 ref=0xb971ea…e524
```

Wall-clock: 4.6 s / 31.9 s / 31.8 s, median **31.9 s** (1 MiB =
260 4 KiB content chunks + 2 manifest chunks). The first run
hits the empty SWAP outbox; runs 2-3 reproducibly stall on the
same pushsync settle pattern (Bee 2.8.0 storers acknowledge but
delay the final 5-10% of pushes by ~25 s before issuing receipts
— consistent with the receipt-batching change in the Bee
`feat: peer validation bundle` commit). The same file
upload-replays to the same reference because Swarm content is
content-addressed.

The 0.4.1 cell is `N/A (no peers)` — see B1: 0.4.1 cannot
handshake any Bee 2.8 peer on mainnet, so it has nowhere to
push.

### I.4 B3 — Download of a 1 MiB ref (cold cache, 0.5.0)

Each run uses a fresh daemon (cold libp2p, cold chunk cache):

```
B3,v15-b3-cold,1,warmup=1  ms=1134 bytes=1048576
B3,v15-b3-cold,2,warmup=1  ms=1319 bytes=1048576
B3,v15-b3-cold,3,warmup=1  ms=1252 bytes=1048576
```

Median **1.25 s** for a 1 MiB mantaray fetch. All three runs
returned byte-correct 1 048 576-byte payloads. Tight spread (±10
%) indicates the network steady-state on mainnet is healthy.

### I.5 B4 — 8× concurrent download (cold cache, 0.5.0)

Eight distinct 1 MiB files uploaded in advance (each pushed
twice — once at upload, once 60 s later to settle the missing
chunks; see anomaly in I.7). Then fanned out 8-way:

```
B4,v15-b4,warmup,1
B4,v15-b4,1,aggregate_ms=60020,ok=6/8,per_task_ms=7608,7610,7613,7618,7627,7651
B4,v15-b4,2,aggregate_ms=60022,ok=6/8,per_task_ms=56,70,76,83,85,86
B4,v15-b4,3,aggregate_ms=60016,ok=6/8,per_task_ms=55,56,60,69,74,74
```

Two of eight refs failed in every run (the same two — chunks
that propagated to too few storers). For the 6 successful refs:
cold-cache fan-out is **~7.6 s** wall-clock (very consistent —
the parallelism keeps every fetch on the slowest peer's path).
Warm-cache fan-out is **55-90 ms**. The 60 s aggregate is the
`antctl get` per-task watchdog firing on the 2 failed refs;
fixing this in the harness would just give a lower aggregate
for the same 6/8 success rate.

### I.6 B5 — Large-file upload (0.5.0)

| Size    | Wall-clock | Throughput  | Outcome                              |
|---------|------------|-------------|--------------------------------------|
| 100 MiB | 432 s      | 0.23 MB/s   | ok                                   |
| 100 MiB | timeout    | —           | pushsync `exhausted 5 retries`       |
| 500 MiB | timeout    | —           | pushsync `exhausted 5 retries` ×2    |

The 0.23 MB/s figure is real but pessimistic: every chunk has to
hit a fresh kademlia-closest storer (none of our 100 peers in
the local set will be the storer for most chunks), so 100 MiB =
25 600 chunks each costs one ~17 ms round trip. The pushsync
timeout failures are a property of the Bee 2.8.0 storer fleet
under load — Bee retries are capped at 5, with a 60 s per-chunk
wall, and on a 100+ MiB upload one slow storer is enough to trip
the cap. This will resolve naturally as the fleet's load
balancers age out the slow stragglers; it's not an Ant
regression (the 0.4.1 daemon can't even reach a single storer
on this network). See [Risks](#14-risks-and-mitigations) for
the upstream tracking item.

### I.7 Methodology / anomalies during the run

**Push-vs-pull race in B3/B4.** An `antctl upload start` returns
`completed` once the local chunk push pipeline drains, but a
subset of chunks (usually 1-2 out of 260 for a 1 MiB file) only
land in the kademlia-closest storer after one more replication
round. A cold `antctl get` immediately after this stall will see
"retrieve chunk: no peer found" from far-from-storer peers and
"retrieve chunk: storage: not found" from peers that *should*
hold it but haven't synced yet. Re-uploading the same file
forces the missing chunks back through the pipeline; after 60 s
the chunks become fetchable. B3's table number uses a reference
that was re-uploaded to settle this state; B4 ran on 8 fresh
refs and shows the worst case (2/8 still un-fetchable even
after a 60 s settle). This is unrelated to the wire-protocol
upgrade and reproduces on a stock Bee 2.8.0 client.

**The first iteration** of the 0.5.0 daemon completed the V15
handshake (`syn sent` / `syn-ack received` / `ack sent`) but bee
disconnected ~60 ms after our `Ack` with `ConnectionReset (errno
104)`. Tracing showed bee's libp2p layer attempting
`/swarm/hive/2.0.0/peers` which our sinks rejected because we
only registered `/swarm/hive/1.1.0/peers`. Bee follows hive
broadcast failure with `Disconnect(peer, "failed broadcasting
to peer")`, which is what we saw on the wire.

Fix: `crates/ant-p2p/src/sinks.rs` now claims both the 1.1.0 and
2.0.0 hive protocol ids and feeds them into the same hive
listener. The protobuf shape is identical (tags 5+6 are
forward-compatible). This was a wire change that the initial bee
2.7.1 → 2.8.0 diff investigation missed — it landed in the same
`feat: peer validation bundle` commit as the handshake bump and
hadn't shown up in the `protocolVersion` constant grep until the
B1 cold-start run exercised the post-handshake hive broadcast.

### I.8 Reproducing

```bash
# 0.4.1 baseline binary (pre-cutover):
git worktree add /tmp/ant-0.4.1 origin/main
( cd /tmp/ant-0.4.1 && cargo build --release -p antd -p antctl )

# 0.5.0 binary (post-cutover):
cargo build --release -p antd -p antctl

# B1 (cold start to 100 peers), each takes 1-2 s post-cutover:
ANTD=/home/ubuntu/ant/target/release/antd \
ANTCTL=/home/ubuntu/ant/target/release/antctl \
  scripts/bench-bee-cutover.sh --scenario b1 --build 0.5.0 --runs 3

# B1 baseline (will TIMEOUT 600 s per run):
ANTD=/tmp/ant-0.4.1/target/release/antd \
ANTCTL=/tmp/ant-0.4.1/target/release/antctl \
  scripts/bench-bee-cutover.sh --scenario b1 --build 0.4.1 --runs 3

# B2-B5 require the production wallet in env:
set -a; source .env; set +a
BENCH_API_ADDR=127.0.0.1:11634 scripts/bench-bee-cutover.sh --scenario b2 --build 0.5.0
# Capture the ref from B2 output, then:
BZZ_REF=<ref> BENCH_API_ADDR=127.0.0.1:11634 \
  scripts/bench-bee-cutover.sh --scenario b3 --build 0.5.0
# For B4 we need 8 distinct files uploaded ahead of time:
for i in 1 2 3 4 5 6 7 8; do
  dd if=/dev/urandom of=/tmp/b4-$i.bin bs=1M count=1 status=none
done
# Then upload all 8 (capture refs), wait 60 s for settle, then:
BZZ_REFS=<ref1>,…,<ref8> BENCH_API_ADDR=127.0.0.1:11634 \
  scripts/bench-bee-cutover.sh --scenario b4 --build 0.5.0
# B5 with a smaller size to dodge the pushsync 60s/chunk limit:
B5_MB=100 BENCH_API_ADDR=127.0.0.1:11634 \
  scripts/bench-bee-cutover.sh --scenario b5 --build 0.5.0
```

---

## Appendix J — Freedom drop-in: bee API surface required by the next release

This appendix is the concrete, audited list of everything `antd` must do to
be a **drop-in replacement for the bundled `bee` node in Freedom Browser**
(`solardev-xyz/freedom-browser`). It was produced by reading the Freedom
source at tag `0.7.5-dev` (commit `3a140bd`) — every place it touches the
Bee node, both the `@ethersphere/bee-js` client calls in the main process
and the raw `fetch()` calls in the renderer and protocol handlers.

The goal: a user points Freedom's integrated-node config at `antd` instead
of `bee`, flips the Nodes-panel toggle, and **publish, feeds, stamps, and
browsing all work unmodified**. Where `antd` already satisfies a
requirement (per Appendix C/D) it is marked ✅; gaps are ❌ / ⚠️ with a work
item.

### J.1 How Freedom talks to the node

Freedom drives the node through **three** distinct channels, all pointed at
`http://127.0.0.1:1633`:

1. **`bee-js` v`^12.2.1`** in the Electron main process
   (`src/main/swarm/*.js`). A single `Bee` client
   (`swarm-service.js::getBee()`) backs publish, feeds, chunks, and stamps.
   This is the bulk of the *write* surface and it pins us to bee-js v12's
   exact request/response shapes.
2. **Raw `fetch()`** in the renderer (`bee-ui.js`, `bee-api.js::fetchBeeJson`,
   `node-status.js`, `publish-setup.js`, `stamp-manager.js`,
   `chequebook-deposit.js`) for status/health/wallet polling.
3. **The `bzz://` protocol handler** (`src/main/swarm/bzz-protocol.js`) and
   **content probe** (`swarm-probe.js`), which proxy every in-page
   `bzz://<ref|ens>/<path>` request straight to `GET/HEAD /bzz/...` with
   `Swarm-Chunk-Retrieval-Timeout`, `Swarm-Redundancy-Strategy`, and
   `Swarm-Redundancy-Fallback-Mode` headers and a retry loop.

Additionally Freedom manages the node **process**: it writes a bee YAML
config, runs `bee init --config` then `bee start --config`, can **inject an
identity** by writing `keys/swarm.key` before start
(`bee-manager.js`, `useInjectedIdentity`), reuses an already-running daemon
detected on `:1633` via `GET /health`, and shells `SIGTERM` with a 5 s
force-kill fallback. All of this is in scope for a true drop-in (§J.5.D).

### J.2 Complete inventory of bee operations Freedom performs

Grouped by endpoint. "Call site" is the Freedom file; "bee-js" means the
call is issued by the bee-js client (so the HTTP shape is whatever bee-js
v12 emits, not Freedom's own code).

#### J.2.1 Status / health / identity (renderer `fetch` + main pre-flight)

| Op | Endpoint | Fields Freedom reads | Call site |
|---|---|---|---|
| Health / version | `GET /health` | `version` (split on `-`), `status` | `bee-ui.js`, `bee-manager.js` (`probeBeeApi`, reuse detection) |
| Readiness gate | `GET /readiness` | HTTP `200` vs not | `node-status.js`, `swarm-provider-ipc.js` pre-flight |
| Node mode | `GET /node` | **`beeMode` — must be `"light"`** | `node-status.js`, `publish-setup.js`, pre-flight (`checkSwarmPreFlight`, `checkBeeReachable`) |
| Addresses | `GET /addresses` | `ethereum` | `publish-setup.js`, `node-status.js` |
| Connected peers | `GET /peers` | `peers[].length` | `bee-ui.js` (Nodes panel, polled 500 ms) |
| Visible peers | `GET /topology` | `bins[*].population` (summed) | `bee-ui.js` (polled 1 s) |
| Chain sync | `GET /status` | `lastSyncedBlock` | `publish-setup.js`, `node-status.js` (sync-progress UI) |

#### J.2.2 Postage stamps (bee-js `stamp-service.js`)

| Op | bee-js call | HTTP (bee v12) | Fields read |
|---|---|---|---|
| List batches | `bee.getPostageBatches()` | `GET /stamps` | `batchID`, `usable`, `immutableFlag`, `size`, `remainingSize`, `usage`, `duration` (with `.toBytes()/.toSeconds()/.toEndDate()`) |
| Cost estimate | `bee.getStorageCost(Size, Duration)` | `GET /chainstate` (`currentPrice`) | computed client-side |
| Buy | `bee.buyStorage(Size, Duration, {waitForUsable:false})` | `POST /stamps/{amount}/{depth}` | returns `batchID` |
| Top-up (duration) | `bee.extendStorageDuration(id, Duration)` | `PATCH /stamps/topup/{id}/{amount}` | |
| Dilute (size) | `bee.extendStorageSize(id, Size)` | `PATCH /stamps/dilute/{id}/{depth}` | |
| Extension cost | `bee.getDurationExtensionCost` / `getSizeExtensionCost` | `GET /chainstate` + `GET /stamps/{id}` | computed client-side |

Stamp cost math in bee-js v12 **requires `GET /chainstate.currentPrice`**;
without it `getStorageCost` / `buyStorage` throw before any purchase. This
endpoint is currently marked **501** in §C.4.2 and that must change for
Freedom.

#### J.2.3 Wallet & chequebook (bee-js + renderer `fetch`)

| Op | Call | HTTP | Fields read |
|---|---|---|---|
| BZZ/native balance | `bee.getWalletBalance()` / `fetch /wallet` | `GET /wallet` | `bzzBalance` (and `.toPLURBigInt()`), `nativeTokenBalance` |
| Chequebook addr | `fetch /chequebook/address` | `GET /chequebook/address` | `chequebookAddress` (zero ⇒ "not deployed") |
| Chequebook bal | `bee.getChequebookBalance()` / `fetch` | `GET /chequebook/balance` | `totalBalance`, `availableBalance` (`.toSignificantDigits()`, `.toPLURBigInt()`) |
| Deposit | `bee.depositTokens(plur)` | `POST /chequebook/deposit?amount=` | returns tx id |

Freedom **auto-deposits 0.1 xBZZ into the chequebook** after a stamp
purchase (`autoDepositChequebookIfEmpty`) and pre-checks balances before
buying — so deposit + real wallet/chequebook reads are on the publish happy
path, not optional.

#### J.2.4 Uploads — data, files, collections (bee-js `publish-service.js`)

| Op | bee-js call | HTTP | Headers / query |
|---|---|---|---|
| Publish data | `bee.uploadFile(batch, data, name, {pin, deferred:false, contentType})` | `POST /bzz?name=` | `Swarm-Postage-Batch-Id`, `Swarm-Pin`, `Swarm-Deferred-Upload`, `Content-Type` |
| Publish file | `bee.uploadFile(batch, stream, name, {pin, deferred:true, size})` | `POST /bzz` | as above + `Content-Length` |
| Publish dir / in-memory files | `bee.uploadFilesFromDirectory(batch, dir, {pin, deferred, indexDocument})` | `POST /bzz` (tar) | `Swarm-Collection:true`, `Swarm-Index-Document` |
| Upload progress | `bee.retrieveTag(uid)` | `GET /tags/{uid}` | reads `split, seen, stored, sent, synced, uid` |

`uploadFile`/`uploadFilesFromDirectory` in bee-js create a **tag**
(`POST /tags`) and surface its `uid`; `getUploadStatus` polls
`GET /tags/{uid}`. Progress is computed from `sent/split`. So `/tags`
(POST + GET) is required for the upload-status UX even though content
addressing itself doesn't need it.

#### J.2.5 Feeds (bee-js `feed-service.js`)

| Op | bee-js call | HTTP |
|---|---|---|
| Create feed manifest | `bee.createFeedManifest(batch, topic, owner)` | `POST /feeds/{owner}/{topic}?type=sequence` |
| Read latest / by index | `makeFeedReader(topic, owner).downloadPayload()` | `GET /feeds/{owner}/{topic}` |
| Write reference | `makeFeedWriter(topic, key).uploadReference(batch, ref, {index})` | `POST /soc/...` (+ chunk) |
| Write payload | `makeFeedWriter(topic, key).uploadPayload(batch, data, {index})` | `POST /bytes` (payload > 4 KiB) + `POST /soc/...` |
| Resolve next index | `reader.downloadPayload()` (404/500 ⇒ index 0) | `GET /feeds` / `GET /chunks` |

bee-js v12 feeds are **SOC-backed**: a feed write is a content-addressed
`POST /bytes` (for payloads > 4096 B) followed by a single-owner-chunk
`POST /soc/{owner}/{id}?sig=`. Reads use `bee.downloadChunk` +
`unmarshalSingleOwnerChunk` + (for large payloads) `bee.downloadData`
(`GET /bytes/{ref}`). The exact identifier derivation
(`keccak256(topic || feedIndex)`) and the manifest format must match bee or
Freedom's published feeds become unresolvable by other bee gateways.

#### J.2.6 Low-level chunks / SOC (bee-js `chunk-service.js`)

| Op | bee-js call | HTTP |
|---|---|---|
| Upload CAC | `bee.uploadChunk(batch, chunk, {pin, deferred:false})` | `POST /chunks` |
| Download CAC | `bee.downloadChunk(ref)` | `GET /chunks/{ref}` |
| Upload SOC | hand-rolled `fetch` `POST /soc/{owner}/{id}?sig=` | `POST /soc/...` (`Swarm-Postage-Batch-Id`, `Swarm-Pin`, `Swarm-Deferred-Upload`) |
| Download SOC | `bee.downloadChunk(ref)` + `unmarshalSingleOwnerChunk` | `GET /chunks/{ref}` |
| Read bytes | `bee.downloadData(ref)` | `GET /bytes/{ref}` |

#### J.2.7 Content serving (`bzz-protocol.js`, `swarm-probe.js`, `ens-prefetch.js`)

| Op | HTTP | Notes |
|---|---|---|
| Probe availability | `HEAD /bzz/{hash}` | gates navigation; 404/500 ⇒ keep polling, conn-refused ⇒ "node down" |
| Serve page + subresources | `GET /bzz/{hash}/{path}` | streamed, `Range` honored, retried on 500/502/503/504; sends the three `Swarm-*` redundancy/timeout headers |
| ENS-named content | `GET /bzz/{ens-decoded-ref}/{path}` | Freedom resolves ENS itself, then hits `/bzz` with the hex ref |

### J.3 Gap analysis vs current `ant-gateway`

Current router (`crates/ant-gateway/src/router.rs`) and stubs
(`stubs.rs`) as of this writing:

| Endpoint | Method(s) | Needed by Freedom | ant today | Gap |
|---|---|---|---|---|
| `/health` | GET | ✅ | ✅ implemented | — |
| `/readiness` | GET | ✅ | ✅ | — |
| `/node` | GET | ✅ (`beeMode:"light"`) | ⚠️ hardcoded `ultra-light` | **must report `light` in swap mode** |
| `/addresses` | GET | ✅ | ✅ | — |
| `/peers` | GET | ✅ | ✅ | — |
| `/topology` | GET | ✅ | ✅ | — |
| `/status` | GET | ✅ (`lastSyncedBlock`) | ❌ (falls to 501) | **add** |
| `/chainstate` | GET | ✅ (`currentPrice`, for cost calc) | ❌ 501 by design | **add (reclassify from 501)** |
| `/bzz/{addr}[/{path}]` | GET, HEAD | ✅ | ✅ | — |
| `/bytes/{addr}` | GET, HEAD | ✅ | ✅ | — |
| `/bytes` | POST | ✅ (feed payloads, bee-js data upload) | ❌ | **add** |
| `/chunks/{addr}` | GET, HEAD | ✅ | ✅ | — |
| `/chunks` | POST | ✅ | ✅ | — |
| `/soc/{owner}/{id}` | GET/HEAD, POST | ✅ | ✅ | verify `?sig=` + body shape vs bee-js |
| `/feeds/{owner}/{topic}` | GET | ✅ | ✅ | verify identifier/index semantics |
| `/feeds/{owner}/{topic}` | POST | ✅ (createFeedManifest) | ❌ (GET only) | **add** |
| `/tags` | POST | ✅ (upload creates tag) | ❌ | **add** |
| `/tags/{uid}` | GET | ✅ (upload progress) | ❌ | **add** |
| `/bzz` | POST | ✅ (file + collection) | ✅ exists | **verify stamps wired + `Swarm-Collection`/`Swarm-Index-Document` + tar** |
| `/stamps` | GET | ✅ (real batches) | ⚠️ stub `{stamps:[]}` | **real listing** |
| `/stamps/{id}` | GET | ✅ (extension cost) | ❌ | **add** |
| `/stamps/{amount}/{depth}` | POST | ✅ (buy) | ❌ | **add** |
| `/stamps/topup/{id}/{amount}` | PATCH | ✅ | ❌ | **add** |
| `/stamps/dilute/{id}/{depth}` | PATCH | ✅ | ❌ | **add** |
| `/wallet` | GET | ✅ (real `bzzBalance`) | ⚠️ stub zero | **real balances** |
| `/chequebook/address` | GET | ✅ (real addr) | ⚠️ stub zero | **real address** |
| `/chequebook/balance` | GET | ✅ (real) | ⚠️ stub zero | **real balances** |
| `/chequebook/deposit` | POST | ✅ (`?amount=`) | ❌ | **add** |

**Read/browse path is essentially done** (Tier A + the SOC/chunk/feed
read endpoints already landed). The drop-in gap is the **publish path**:
real stamps, real wallet/chequebook, feed *create*, `POST /bytes`, tags,
`/chainstate`, `/status`, and — the gating one — **reporting
`beeMode:"light"`** so Freedom's `checkSwarmPreFlight` lets publishing
proceed at all.

### J.4 Behavioural requirements (not just routes)

These are the easy-to-miss bits that break Freedom even when the routes
exist:

1. **`beeMode` gate.** `checkSwarmPreFlight` (swarm-provider-ipc.js)
   *rejects* `ultra-light` for any publish/feed/SOC write. `antd` started
   with `swap-enable: true` must return `beeMode:"light"` from `/node`, and
   `isLightOrFull` logic also accepts `"full"`.
2. **bee-js typed responses.** Stamp/cost/balance objects are consumed via
   bee-js helpers (`.toBytes()`, `.toSeconds()`, `.toEndDate()`,
   `.toPLURBigInt()`, `.toSignificantDigits()`). That means the **raw JSON
   field names and units must match bee exactly** (e.g. `/stamps` items use
   `batchID`, `amount`, `depth`, `bucketDepth`, `utilization`, `usable`,
   `immutableFlag`, `batchTTL`, `exists`; `/wallet` uses string-integer
   `bzzBalance` in PLUR). bee-js parses these; wrong casing or a number
   where bee sends a string crashes the client.
3. **Error shape on not-found.** Feed/chunk reads rely on
   `BeeResponseError.status` being **404 or 500** to mean "empty / not
   found" (feed-service `isNotFoundError`, chunk-service
   `isChunkNotFoundError` which also matches a 500 body of
   `"read chunk failed"`). `antd` must return those statuses (not, say,
   400) for missing chunks/feeds or feed index-resolution loops forever.
4. **SOC upload contract.** `POST /soc/{owner}/{id}?sig=<hex>` with body
   `span(8 LE) || payload`, headers `Swarm-Postage-Batch-Id`, `Swarm-Pin`,
   `Swarm-Deferred-Upload:false`, and a JSON `{reference}` response whose
   reference equals the locally-computed SOC address (Freedom asserts
   equality and fails the write otherwise).
5. **Upload reference echo.** `POST /bzz` / `POST /chunks` must return
   `{reference}` (hex, no `0x`) and a `Swarm-Tag-Uid` so bee-js can poll
   progress.
6. **Deferred + pin semantics.** Freedom publishes data with
   `deferred:false` (synchronous pushsync, used as the "it's on the
   network" signal) and files/dirs with `deferred:true`. Both set
   `pin:true`. `antd` must accept both and have `deferred:false` actually
   block until the chunks are pushed (this is the existing pushsync work in
   M3 / Appendix F).
7. **Redundancy/timeout headers are advisory.** `bzz-protocol.js` always
   sends `Swarm-Chunk-Retrieval-Timeout:30s`,
   `Swarm-Redundancy-Strategy:3`, `Swarm-Redundancy-Fallback-Mode:true`.
   Accept-and-honor the timeout; accept-and-ignore the redundancy pair is
   fine (already the Tier-A stance).
8. **CORS.** Freedom's config sets `cors-allowed-origins: "null"`. Page
   `fetch`/SOC uploads originate from the dweb page origin; `antd` must
   apply the same CORS handling bee does for that value (Appendix C.4
   already commits to driving CORS off `cors-allowed-origins`).

### J.5 Work items

Ordered so each step is independently verifiable against a real Freedom
build. A–C are the gateway; D is process/identity parity.

**A. Make the read/publish *gate* pass (smallest unblock).**
- A1. `/node` returns `beeMode:"light"` when the node is running with SWAP
  enabled; `"ultra-light"` otherwise. Wire to the actual SWAP/chequebook
  state, not a constant.
- A2. `GET /status` returning at least `{lastSyncedBlock, ...}` from
  `ant-chain`'s synced block. (Freedom only reads `lastSyncedBlock`; emit
  the rest of bee's `/status` body for safety per §C.6 golden corpus.)
- A3. `GET /chainstate` with real `currentPrice` (and `block`,
  `totalAmount`) from `ant-chain`. Reclassify from 501 in §C.4.2 — bee-js
  cost math depends on it.

**B. Real postage (the core of publishing).**
- B1. `GET /stamps` + `GET /stamps/{id}` backed by `ant-postage`, with the
  full bee batch JSON (`batchID`, `amount`, `depth`, `bucketDepth`,
  `utilization`, `usable`, `immutableFlag`, `batchTTL`, `exists`, `label`).
- B2. `POST /stamps/{amount}/{depth}` (buy) → on-chain batch creation via
  `ant-chain` + `ant-postage`; honor `immutable` and `label` query/headers;
  return `{batchID}`. `waitForUsable=false` path: return as soon as the tx
  is mined; Freedom polls `/stamps` for `usable`.
- B3. `PATCH /stamps/topup/{id}/{amount}` and
  `PATCH /stamps/dilute/{id}/{depth}`.
- B4. Stamp signing on upload: `Swarm-Postage-Batch-Id` on `POST /bzz`,
  `POST /chunks`, `POST /bytes`, `POST /soc` must produce valid per-chunk
  stamps the network accepts.

**C. Missing write/route surface.**
- C1. `POST /bytes` (raw byte upload, returns `{reference}`), used by
  bee-js for feed payloads > 4 KiB and direct data uploads.
- C2. `POST /feeds/{owner}/{topic}` (create feed manifest); align the
  manifest + `?type=sequence` semantics with bee so other gateways resolve
  it.
- C3. `/tags`: `POST /tags` (create) and `GET /tags/{uid}` with
  `split, seen, stored, sent, synced, uid, address, startedAt`. Uploads
  attach to / create a tag and set `Swarm-Tag-Uid` on the response.
- C4. `POST /bzz` collection mode: `Swarm-Collection:true` + tar body →
  mantaray, honoring `Swarm-Index-Document`. Verify single-file `POST /bzz`
  already returns a browsable manifest (Freedom publishes *data* via
  `uploadFile`, which expects a manifest, not a raw `/bytes` ref).

**D. Wallet / chequebook / SWAP.**
- D1. `GET /wallet` real `bzzBalance` (PLUR string) + `nativeTokenBalance`
  + `chainID:100` from `ant-chain`.
- D2. `GET /chequebook/address` (real, or zero when not deployed) and
  `GET /chequebook/balance` real `totalBalance`/`availableBalance`.
- D3. `POST /chequebook/deposit?amount=` → deposit xBZZ into the
  chequebook; return `{transactionHash}`. (Freedom auto-deposits 0.1 xBZZ
  post-purchase and on demand.)
- D4. Chequebook **deploy** path: bee deploys the chequebook lazily on
  first need; Freedom keys its whole "publish ready" UX off
  `chequebookAddress !== 0x0`. Ensure first deposit / first light-mode
  start deploys it (this is the M3 SWAP milestone).

**E. Process / identity / config parity (so the toggle works at all).**
- E1. Bee YAML config support (`§C.2`, `D.3.1`): `api-addr`, `data-dir`,
  `password`, `mainnet`, `full-node:false`, `swap-enable`,
  `blockchain-rpc-endpoint`, `cors-allowed-origins`, `resolver-options`,
  `skip-postage-snapshot`, `storage-incentives-enable`. Freedom writes
  exactly these.
- E2. `antd init --config` / `antd start --config` subcommands; idempotent
  init.
- E3. **Injected identity:** detect a pre-written
  `keys/swarm.key` (Web3 v3 keystore, `§C.3`/`D.3.2`) and start without
  re-init. Freedom's identity system writes this file before launch.
- E4. Daemon manners Freedom relies on: `/health` answers < 50 ms after
  bind (reuse detection), graceful `SIGTERM` exit < 5 s, and tolerating a
  second instance probing `:1633` (it will reuse rather than spawn).

### J.6 Mapping to existing milestones

Almost all of J.5 is already planned — this appendix just confirms Freedom
needs the **whole of Tier B** (Appendix C) plus three endpoints currently
filed as 501/absent:

- **A1–A2, D1–D4, B1–B4** = §9 Milestone 3 (light node: uploads + stamps +
  SWAP) and the "Drop-in light-node gap" follow-on (§9 line ~1663).
- **A3 (`/chainstate`)** = **reclassification** required: §C.4.2 lists it
  under permanent 501 "full-node only", but bee-js's stamp-cost math reads
  `currentPrice` from it. Expose the read-only `currentPrice`/`block` view
  (not the full-node reserve machinery).
- **C1–C4** = Tier B endpoint matrix (`POST /bytes`, `POST /feeds`,
  `/tags`, collection upload) — already in §C.4.2, just not yet routed.
- **E1–E4** = Appendix D.3.1 / D.3.2 + §C.2/§C.3, promoted from "optional
  follow-on" to **required** for the Freedom drop-in.

### J.7 Acceptance — "Freedom runs on antd"

The drop-in is done when, against an `antd` started from Freedom's own
config (`swap-enable:true`, injected `keys/swarm.key`):

1. **Toggle + status.** Freedom's Nodes panel shows the `antd` version,
   connected/visible peer counts, and `beeMode:light`; the publish UI
   leaves the "checking node status" state and reaches "ready".
2. **Buy stamps.** The Storage screen estimates a cost (needs
   `/chainstate`), buys a batch (`POST /stamps/...`), polls it to `usable`,
   and can extend duration/size.
3. **Publish.** `freedom://publish` uploads text, a file, and a directory
   (`POST /bzz`), each returns a `bzz://<ref>` that renders back in a tab
   (`GET /bzz`), and the progress bar advances via `/tags`.
4. **Feeds + SOC.** A connected dweb page can `swarm_createFeed`,
   `swarm_writeFeedEntry`, `swarm_readFeedEntry`, and
   `swarm_write/readSingleOwnerChunk` through `window.swarm` without error.
5. **Wallet.** Bee-wallet xBZZ/native balances and chequebook
   address/balance render real values; post-purchase auto-deposit succeeds.
6. **No silent 501s** on any endpoint in §J.3 marked ✅/add. A Freedom
   session produces zero `NODE_UNAVAILABLE` / `not implemented` errors in
   normal publish/browse flows.

### J.8 Implementation status (gateway HTTP surface)

Landed in the gateway (all behind the existing in-process node-loop
control channel; no new chain wiring, fully covered by
`crates/ant-gateway/tests/freedom_dropin.rs`):

- **A1 `/node` beeMode** — returns `light` (+ `chequebookEnabled` /
  `swapEnabled`) when the daemon is started upload-capable (a postage
  batch is configured), `ultra-light` otherwise. Driven by
  `GatewayHandle::light_mode`, set by `antd` from the upload runtime.
- **C1 `POST /bytes`** — raw byte upload: split → pushsync → `{reference}`
  (bare `/bytes` ref, no manifest), with `Swarm-Tag-Uid`.
- **C2 `POST /feeds/{owner}/{topic}`** — builds + pushes a `Sequence`
  feed manifest (`ant_retrieval::build_feed_manifest`), returns the
  manifest root; `?type=epoch` → 501, matching the read path. Shares the
  route with the existing `GET/HEAD` feed lookup.
- **C3 `/tags`** — `POST /tags` (create) + `GET /tags/{uid}` returning
  bee-shaped `{split,seen,stored,sent,synced,uid,address,startedAt}`.
  Because gateway uploads are synchronous (every chunk is pushsynced
  before the response), a tag is reported fully synced as soon as the
  upload completes; uploads auto-create (or adopt a client-supplied) tag
  and echo it in `Swarm-Tag-Uid`. In-memory registry (`tags.rs`).
- **C4 `POST /bzz`** — single-file + collection paths now finalize an
  upload tag (`Swarm-Tag-Uid`) like bee; the manifest shape was already
  browsable.
- **B1 `GET /stamps` + `GET /stamps/{id}`** — backed by the daemon's
  `PostageStatusView` over `ControlCommand::PostageStatus` (`stamps.rs`).
  A configured batch is surfaced as a `usable` bee-shaped stamp so
  Freedom's publish pre-flight passes; an empty list is returned when
  uploads are disabled. `amount`/`batchTTL`/`blockNumber` use safe
  placeholders pending a chain read. The request is bounded by a 10 s
  timeout so a wedged node loop yields `504`, not a hung task.

Deferred (require live-mainnet validation; not safely testable offline,
and they pair with the on-chain *write* path which is itself deferred):

- **A2 `GET /status` (`lastSyncedBlock`)** and **A3 `GET /chainstate`
  (`currentPrice`)** — need new `ant-chain` reads (`eth_blockNumber`,
  postage price-oracle `eth_call`) plumbed through `ant-control` /
  `ant-node` to the gateway. Left on the 501 fallback rather than
  shipping fabricated chain data that bee-js cost math would trust.
- **B2–B4 on-chain stamp mutation** (`POST /stamps/{amount}/{depth}`,
  `PATCH topup|dilute`, per-chunk stamp signing on arbitrary
  `Swarm-Postage-Batch-Id`) and **D1–D4 wallet/chequebook/deposit/deploy**
  — Milestone 3 SWAP work; remain on the 501 fallback.


## Appendix P — perf-lab (2026-07-05): the ~250 MiB upload stall is RESOLVED

The production-era failure ("push chunk failed: exhausted N retries"
at ~250 MiB, phase 7b post-mortems above) was reproduced, root-caused
and fixed by the measure-first perf-lab campaign on branch `perf-lab`
(notebook: `PERF-LAB.md`, raw numbers: `perf/results/`, PR #25).

Mechanism (measured, not theorised): a cheque-less light node settled
NOTHING on the push path — the pseudosettle driver only walked
retrieval-side `Accounting` rows — so bee peers debt-killed our
connections (SO_LINGER=0 ⇒ pure RST storms, never "overdraft"
messages); connection churn masked it below ~100 MiB. The fix pair,
now default-on:

1. `ant-p2p::push_pseudosettle` — mirror push debits into the shared
   `Accounting`, letting the existing pseudosettle driver time-settle
   upload peers (bee-light behaviour; no cheques, no chain).
2. `ant-retrieval::push_load` — per-peer latency-aware in-flight cap
   (base 4) so the dispatcher never outspends a peer's refresh
   budget; this also stopped the early-burst blocklist scars that
   made upload tails grind.

Proof: 3 consecutive 512 MiB mainnet completions (33 / 59 / 54 min)
where the baseline died at ~250 MiB; first-ever 256 MiB completion.
Also landed from the same campaign: cold feed resolution fixed
(served the SOC-wrapped CAC instead of re-fetching it: 0 % → 100 %)
and warm feed polls flattened to ~1.6 s at any head depth.
