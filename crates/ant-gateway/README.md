# ant-gateway

Bee-shaped HTTP gateway in front of `antd`. Lets unmodified bee
consumers — `bee-js`, `curl`, browser proxies — drive read-only
browsing of Swarm mainnet content against `antd` on
`127.0.0.1:1633`.

This crate is the smallest cut of bee's HTTP API that gets the
ultra-light read path working end-to-end (PLAN.md "Tier A"). Anything
write-shaped, SWAP-shaped, or stamp-shaped lives behind the catch-all
`501 {code:501,message:"not implemented in ant"}` until a future
milestone wires it up. The compatibility contract — endpoint matrix,
JSON shapes, error envelope — is normative in [PLAN.md Appendix C][appx-c],
and the work breakdown is in [PLAN.md Appendix D][appx-d].

[appx-c]: ../../PLAN.md
[appx-d]: ../../PLAN.md

## Supported endpoints

| Path                         | Method     | Tier | Notes                                                                |
| ---------------------------- | ---------- | ---- | -------------------------------------------------------------------- |
| `/health`                    | GET        | A    | `{status, version, apiVersion}`. Constant after bind.                |
| `/readiness`                 | GET        | A    | 200 once a non-bootnode BZZ peer is in the routing table, 503 otherwise (#78). |
| `/node`                      | GET        | A    | `beeMode: "ultra-light"`, swap/chequebook flagged off.               |
| `/addresses`                 | GET        | A    | Static identity — overlay (bare hex), ethereum (`0x…`), publicKey.   |
| `/peers`                     | GET        | A    | BZZ-handshaked peers by overlay; pre-handshake peers omitted.        |
| `/topology`                  | GET        | A    | Bee `kademlia.Snapshot`-shaped: 32-bin map plus summary fields.      |
| `/wallet`                    | GET        | A    | Stubbed zero balances on Gnosis (chain ID 100); upgrades to live values once `antd` learns to read its own wallet (M3 Phase 7 polish). |
| `/stamps`                    | GET        | A    | Stubbed empty list — postage is managed via `antctl postage` for now. |
| `/chequebook/address`        | GET        | A    | All-zero address — bee's "feature absent" sentinel; flips to the configured chequebook once Phase 7 auto-settlement lands. |
| `/chequebook/balance`        | GET        | A    | Stubbed zero `totalBalance` / `availableBalance`.                    |
| `/chunks`                    | POST       | B    | Stamps + pushsyncs a single 32-byte content-addressed chunk.         |
| `/chunks/{addr}`             | GET / HEAD | A    | Returns wire bytes (`span ‖ payload`), matching `chunkstore.Get`.    |
| `/bytes/{addr}`              | GET / HEAD | A    | Joins the chunk tree; honors a single-range `Range` header.          |
| `/bzz`                       | POST       | B    | Single-file (`?name=…`) or tar-collection upload → Mantaray ref.     |
| `/bzz/{addr}`                | GET / HEAD | A    | Manifest root — resolves `website-index-document` if present.        |
| `/bzz/{addr}/{*path}`        | GET / HEAD | A    | Walks the manifest, joins data; `Content-Type` from manifest meta.   |
| `/v0/manifest/{addr}`        | GET        | A+   | Ant extension: JSON listing of mantaray paths + metadata (no body).  |
| _everything else_            | _any_      | —    | `501 {code:501, message:"not implemented in ant"}`.                  |

### Streaming, HEAD, and ranges

`/bytes`, `/bzz`, and `/chunks` all go through the streaming dispatch
path — bodies are never fully materialised in the gateway process.
`Body::from_stream` pulls ordered `BytesChunk`s from the joiner as they
land, and an HTTP client disconnect cancels the upstream retrieval task.

- **GET, no Range** — full streaming join. Headers go out as soon as the
  root span and (for `/bzz`) manifest metadata are known; the body
  follows as the joiner emits ordered subtrees.
- **HEAD** — streaming dispatch with `head_only = true`, so the daemon
  resolves manifest + root span but never fetches body chunks. Returns
  `Content-Type`, `Content-Length`, and `Accept-Ranges: bytes`.
- **GET, single Range** — streaming dispatch with a clamped interval
  mapped to the minimum set of Swarm subtrees required. Returns `206
  Partial Content` with an accurate `Content-Range`. Multi-range
  (`Range: bytes=a-b,c-d`) is rejected with `416`.

`GATEWAY_MAX_FILE_BYTES` (1 GiB) caps the per-request total size so a
malicious manifest claiming a multi-terabyte span is refused before any
allocation. See PLAN.md Appendix E for the full design.

### Upload endpoints (Tier B)

`POST /chunks` and `POST /bzz` are live and bee-shaped — `bee-js`'s
upload helpers and the official `swarm-cli` work against them
unchanged. Both endpoints require the `swarm-postage-batch-id`
header (32-byte hex; create one with `antctl postage create`) and
both demand that `antd` was started with a postage batch and the
batch owner's signing key:

```sh
antd \
  --gnosis-rpc-url      https://gnosis-mainnet.g.alchemy.com/v2/<key> \
  --postage-batch       0x<32-byte batch id> \
  --postage-owner-key   0x<32-byte secret>
```

`POST /chunks` accepts the wire form (`span ‖ payload`, ≤ 4104
bytes), stamps it locally with the configured `StampIssuer`, and
pushsyncs to the closest BZZ peer.

`POST /bzz` accepts either:

- A single file body — controlled by `?name=<filename>` and the
  `swarm-collection: false` header (the default). The gateway
  splits, stamps, and pushes every leaf + intermediate, then builds
  a one-entry Mantaray manifest pointing at the file root.
- A tar archive body — `swarm-collection: true` plus
  `content-type: application/x-tar`. The gateway iterates the tar
  members, splits + pushes every file, then builds a recursive
  Mantaray manifest. `swarm-index-document: <path>` and
  `swarm-error-document: <path>` set the `website-index-document` /
  `website-error-document` metadata on the root.

Response in both cases is `201 Created` with
`{"reference":"<32-byte hex>"}`. SWAP cheques are not yet emitted
when forwarders accept our pushes (see Phase 7 polish in PLAN.md
§9.0); peers tolerate this for the small volumes our upload tests
generate, but a sustained 1 GB+ upload would eventually trip
their disconnect threshold.

### Bee-API drop-in gap

The HTTP surface above gets bee-js's read path working unchanged.
The remaining bee endpoints fall into three buckets — the prioritised
order to close the **drop-in** gap is in [PLAN.md §9.0 "Drop-in
light-node gap — prioritised follow-on"][drop-in].

[drop-in]: ../../PLAN.md

| Path                               | Method     | Status today        | Gap class    |
| ---------------------------------- | ---------- | ------------------- | ------------ |
| `/wallet`                          | GET        | stub zeros          | drop-in #2   |
| `/balances`, `/balances/{addr}`    | GET        | 501                 | drop-in #2   |
| `/settlements`, `/settlements/{addr}` | GET     | 501                 | drop-in #2   |
| `/timesettlements`                 | GET        | 501                 | drop-in #2   |
| `/chequebook/address`              | GET        | stub `0x000…`       | drop-in #2   |
| `/chequebook/balance`              | GET        | stub zeros          | drop-in #2   |
| `/chequebook/cheque[/{peer}]`      | GET        | 501                 | drop-in #2   |
| `/chequebook/deposit/{amount}`     | POST       | 501                 | drop-in #2   |
| `/chequebook/withdraw/{amount}`    | POST       | 501                 | drop-in #2   |
| `/stamps`                          | GET        | stub `{stamps:[]}`  | drop-in #3   |
| `/stamps/{batch-id}`               | GET        | 501                 | drop-in #3   |
| `/stamps/{amount}/{depth}`         | POST       | 501 (`antctl postage create`) | drop-in #3 |
| `/stamps/topup/{batch-id}/{amount}` | PATCH     | 501 (`antctl postage top-up`) | drop-in #3 |
| `/stamps/dilute/{batch-id}/{depth}` | PATCH     | 501 (`antctl postage dilute`) | drop-in #3 |
| `/tags`, `/tags/{id}`              | GET/POST/DELETE/PATCH | 501       | drop-in #4   |
| `/feeds/{owner}/{topic}`           | GET/POST   | 501                 | drop-in #6   |
| `/soc/{owner}/{id}`                | POST       | 501                 | drop-in #6   |
| `/stewardship/{ref}`               | GET/PUT    | 501                 | drop-in #7   |
| `/envelope/{addr}`                 | POST       | 501                 | drop-in #7   |
| `POST /bytes` (split, no manifest) | POST       | 501                 | nice-to-have (bee-js never calls it) |
| `/pss/*`, `/gsoc/*`                | any        | 501 (permanent)     | by design — Appendix B |
| `/transactions[/{hash}]`           | any        | 501 (permanent)     | by design — admin/restricted API |
| `/auth`, `/refresh`                | any        | 501 (permanent)     | by design — restricted-API auth |
| `/chequebook/cashout/{peer}`       | GET/POST   | 501 (permanent)     | by design — light nodes don't cash, §5.8 |
| `/pinning/*`                       | any        | 501 (permanent)     | by design — light nodes don't pin |

**One real protocol gap** holds the rest of the surface in stub
mode: auto-settlement (drop-in #1 in the PLAN). Until SWAP cheques
are auto-emitted on per-peer debt threshold crossings, `GET /node`
correctly reports `beeMode: "ultra-light"`,
`chequebookEnabled: false`, `swapEnabled: false`. Once that lands
the stub responses for `/wallet`, `/chequebook/{address,balance}`,
and `/stamps` need to flip to real values *and* `/node` flips to
`beeMode: "light"` — that's a single coordinated change.

**One real read-path gap:** Reed-Solomon erasure decoding (drop-in
#8). Files uploaded with `--swarm-redundancy={1,2,3}` need RS
decoding when individual data chunks are unreachable. Today every
data chunk must be reachable; clients can opt in via the daemon's
`--allow-degraded-redundancy` flag.

### Other intentional non-features

- Web3 v3 keystore (`keys/swarm.key`) — PLAN.md D.3.2. Operators
  with an existing bee keystore have to convert to `signing.key`
  (32 raw hex bytes) by hand for now.
- ENS resolution (`/bzz/<ens>/...`) — currently treated as a literal
  reference; bee-js's reachability checks accept the resulting `400`.
- bee-style YAML config, `antd init`, `antd start` subcommand
  shim — `antd`'s existing CLI is unchanged.
- Prometheus metrics endpoint on `:1635` and OpenAPI 3.0 spec —
  bee ships both, ant ships neither. Operator quality-of-life
  items, not part of the drop-in claim.

## How to run

The gateway is wired into `antd` and starts after the node is up.
Defaults match bee's own (`127.0.0.1:1633`), so unmodified bee-js
clients work without configuration:

```sh
# build
cargo build --release -p antd

# run with the gateway on the default address (read-only)
./target/release/antd --data-dir /tmp/ant-smoke

# or pick a different port — pass through to bee-js as $BEE_API_URL
./target/release/antd --data-dir /tmp/ant-smoke --api-addr 127.0.0.1:11633

# headless deployments (no HTTP, only the antctl UDS)
./target/release/antd --data-dir /tmp/ant-smoke --no-http-api

# enable upload endpoints (POST /chunks, POST /bzz)
./target/release/antd --data-dir /tmp/ant-smoke \
  --gnosis-rpc-url    https://gnosis-mainnet.g.alchemy.com/v2/<key> \
  --postage-batch     0x<32-byte batch id> \
  --postage-owner-key 0x<32-byte secret>
```

Smoke tests once peers are connected:

```sh
curl -fsS  http://127.0.0.1:1633/health
curl -fsS  http://127.0.0.1:1633/peers | jq '.peers | length'
curl -fsSI http://127.0.0.1:1633/bzz/<root>/<path>
curl -i    http://127.0.0.1:1633/transactions   # 501, not implemented

# upload (requires --postage-batch + --postage-owner-key)
curl -fsS -X POST -H 'content-type: text/plain' \
     -H "swarm-postage-batch-id: $BATCH" \
     --data-binary 'uploaded via ant' \
     'http://127.0.0.1:1633/bzz?name=hello.txt'
```

## Embedding

Embedders that want the bee API surface depend on `ant-gateway` directly:

```rust,no_run
use ant_gateway::{Gateway, GatewayHandle};

# async fn run(handle: GatewayHandle) -> anyhow::Result<()> {
let addr = "127.0.0.1:1633".parse()?;
Gateway::serve(handle, addr).await?;
# Ok(()) }
```

Embedders that don't want the HTTP surface (most importantly the
`ant-ffi` mobile artefact) compile this crate with default features
disabled — the `http-api` feature is the only thing that pulls in
`axum` / `hyper` / `tower`, so the entire HTTP closure drops out.

## Tests

```sh
cargo test -p ant-gateway
```

Status / stub / fallback tests build the production router via the
`testkit` module and drive it through `tower::ServiceExt::oneshot` —
no socket. Retrieval tests reuse the on-disk chunk fixture in
`crates/ant-retrieval/tests/fixtures/bzz-ab7720-13-4358-2645/` (the
same one `bzz_fixture.rs` locks the joiner / mantaray walker against),
spinning up an in-process fake node loop that resolves the
`ControlCommand`s the gateway dispatches.
