//! Axum router assembly.
//!
//! The router is split out of `lib.rs` so the integration tests in
//! `tests/` can build the same `Router<()>` production uses and drive
//! it via `tower::ServiceExt::oneshot` — no socket, no port binding,
//! no flaky timing.

use std::time::Instant;

use axum::extract::{DefaultBodyLimit, Request};
use axum::http::{HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::Response;
use axum::routing::{get, on, patch, post, put, MethodFilter, MethodRouter};
use axum::Router;

use tracing::Instrument;

use crate::fallback::not_implemented;
use crate::handle::GatewayHandle;
use crate::retrieval::GATEWAY_MAX_UPLOAD_BYTES;
use crate::{
    act, chain, chunk_stream, pins, retrieval, settlements, stamps, status, stewardship, subscribe,
    tags,
};

const SERVER_HEADER: &str = concat!("ant-gateway/", env!("CARGO_PKG_VERSION"));

/// Build the production [`axum::Router`] from a [`GatewayHandle`].
///
/// Public so integration tests can mount it without standing up a
/// listener; the binary code in [`crate::Gateway::serve`] uses the same
/// builder.
pub fn build(handle: GatewayHandle) -> Router {
    let spend_guard =
        middleware::from_fn_with_state(handle.clone(), crate::cors::wallet_spend_guard);
    Router::new()
        .route("/health", get(status::health))
        .route("/readiness", get(status::readiness))
        .route("/node", get(status::node))
        .route("/addresses", get(status::addresses))
        .route("/peers", get(status::peers))
        .route("/topology", get(status::topology))
        // Chain-backed wallet / chequebook / status / chainstate
        // (PLAN.md J.5 A2/A3/D1/D2). Real Gnosis balances + block /
        // postage price when an RPC endpoint is configured; bee's
        // zero-stub (wallet/chequebook) or 501 (status/chainstate)
        // otherwise.
        .route("/wallet", get(chain::wallet))
        .route("/status", get(chain::status))
        .route("/chainstate", get(chain::chainstate))
        // Real postage listing backed by the daemon's configured batch
        // (PLAN.md J.5.B); on-chain buy/topup/dilute remain on the 501
        // fallback pending live-mainnet validation.
        .route("/stamps", get(stamps::stamps))
        .route("/stamps/{id}", get(stamps::stamp))
        // Per-bucket collision counters of a registered batch (bee
        // `postageGetStampBucketsHandler`).
        .route("/stamps/{id}/buckets", get(stamps::stamp_buckets))
        // Network batch listing / lookup (bee `postageGetAllBatchesHandler`
        // / `postageGetBatchHandler`). Ant has no postage event sync, so
        // the global list is honestly empty and the single-batch lookup
        // reads the PostageStamp contract views over the configured RPC.
        .route("/batches", get(stamps::batches))
        .route("/batches/{id}", get(stamps::batch))
        // Local pinning ("pin-light", bee `pkg/api/pin.go` shapes): pins
        // live in the persistent disk chunk cache and are exempt from
        // eviction, giving mobile an offline-first durable store.
        .route("/pins", get(pins::pin_list))
        .route("/pins/check", get(pins::pin_check))
        .route(
            "/pins/{reference}",
            get(pins::pin_get)
                .post(pins::pin_post)
                .delete(pins::pin_delete),
        )
        // WebSocket chunk-upload stream (bee `chunk_stream.go`).
        .route("/chunks/stream", get(chunk_stream::chunk_stream))
        // On-chain postage mutation (PLAN.md J.5 B2/B3): buy / topup /
        // dilute. `501` when no funded wallet key is configured.
        .route("/stamps/{amount}/{depth}", post(chain::buy_stamp))
        .route("/stamps/topup/{id}/{amount}", patch(chain::topup_stamp))
        .route("/stamps/dilute/{id}/{depth}", patch(chain::dilute_stamp))
        .route("/chequebook/address", get(chain::chequebook_address))
        .route("/chequebook/balance", get(chain::chequebook_balance))
        // Read-only settlement/balance mirror (bee `balances.go`,
        // `settlements.go`, `chequebook.go` last-cheque handlers). All
        // served from one node-side `AccountingSnapshot` command.
        .route("/balances", get(settlements::balances))
        .route("/balances/{peer}", get(settlements::peer_balance))
        .route("/consumed", get(settlements::consumed))
        .route("/consumed/{peer}", get(settlements::peer_consumed))
        .route("/settlements", get(settlements::settlements))
        .route("/settlements/{peer}", get(settlements::peer_settlements))
        .route("/timesettlements", get(settlements::timesettlements))
        .route("/chequebook/cheque", get(settlements::chequebook_cheque))
        .route(
            "/chequebook/cheque/{peer}",
            get(settlements::chequebook_cheque_peer),
        )
        // Chequebook funding (PLAN.md J.5 D3): transfer xBZZ into the
        // chequebook. `501` without a funded wallet key.
        .route("/chequebook/deposit", post(chain::chequebook_deposit))
        // Ant-specific xDAI storage funding: price and pay for storage
        // from the node wallet's plain xDAI (the node swaps for xBZZ
        // itself). Namespaced under `/v0/` like `/v0/manifest`.
        // The writes swap the wallet's xDAI, so they refuse requests
        // from web pages (`cors::wallet_spend_guard`): being
        // query-only, they'd otherwise be CORS-simple and reachable
        // from any page without a preflight.
        .route("/v0/storage/quote", get(chain::storage_quote))
        .route(
            "/v0/storage/buy",
            post(chain::storage_buy).route_layer(spend_guard.clone()),
        )
        .route(
            "/v0/storage/extend",
            post(chain::storage_extend).route_layer(spend_guard.clone()),
        )
        // Ant extension: the node's `swap-enable` switch, read and set
        // at runtime (Freedom runs `antd --no-control-socket`). The write
        // refuses web pages like the spending routes above.
        .route(
            "/v0/settlement/swap",
            get(status::swap_get).merge(put(status::swap_put).route_layer(
                middleware::from_fn_with_state(
                    handle.clone(),
                    crate::cors::settlement_switch_guard,
                ),
            )),
        )
        .route(
            "/v0/settlement/deposit",
            get(chain::settlement_deposit)
                .merge(post(chain::settlement_fund_deposit).route_layer(spend_guard.clone())),
        )
        // Upload-progress tags (PLAN.md J.2.4 / C3). `POST /tags`
        // creates a tag; `GET /tags` lists them; `GET /tags/{uid}`
        // polls one; `DELETE` drops it; `PATCH` marks the "done split"
        // (bee's `doneSplitHandler`). Uploads also auto-create a tag
        // and echo its uid in `Swarm-Tag` (+ legacy `Swarm-Tag-Uid`).
        .route("/tags", get(tags::list_tags).post(tags::create_tag))
        .route(
            "/tags/{uid}",
            get(tags::get_tag)
                .delete(tags::delete_tag)
                .patch(tags::patch_tag),
        )
        .route("/chunks", post(retrieval::upload_chunk))
        // Pre-signed postage stamp issuance (bee `envelope.go`) and
        // content stewardship (bee `stewardship.go` / `pkg/steward`):
        // retrievability check + re-upload of a whole content tree.
        .route("/envelope/{address}", post(stewardship::post_envelope))
        .route(
            "/stewardship/{address}",
            get(stewardship::stewardship_get).put(stewardship::stewardship_put),
        )
        .route("/soc/{owner}/{id}", post(retrieval::upload_soc))
        // PSS: wrap+mine+stamp+push a trojan chunk to a target
        // neighborhood (bee `pss.go`). Light-node send.
        .route("/pss/send/{topic}/{targets}", post(retrieval::upload_pss))
        // GSOC/PSS receive: pull-based lurker subscriptions streamed over
        // WebSocket (bee `gsoc.go` / `pss.go` subscribe). Light-node
        // receive without a reserve.
        .route("/gsoc/subscribe/{address}", get(subscribe::gsoc_subscribe))
        .route("/pss/subscribe/{topic}", get(subscribe::pss_subscribe))
        // Raw byte upload (PLAN.md J.2.5 / C1): bee-js feed payloads and
        // direct data uploads. Returns a `/bytes/<ref>` reference.
        .route("/bytes", post(retrieval::upload_bytes))
        .route("/chunks/{addr}", get_or_head(retrieval::chunk))
        // Single-owner-chunk read: the address is derived as
        // `keccak256(id || owner)`, so callers that already know the
        // `(owner, id)` pair can avoid computing it client-side.
        .route("/soc/{owner}/{id}", get_or_head(retrieval::download_soc))
        // Sequence-feed lookup (GET/HEAD) + feed-manifest creation
        // (POST, PLAN.md J.2.5 / C2) share the one path. GET resolves
        // the latest update and streams the content it points at,
        // byte-for-byte like bee (with `Swarm-Only-Root-Chunk`, Range,
        // and the `swarm-feed-index{,-next}` / `swarm-soc-signature`
        // headers); POST builds and pushes the feed manifest.
        .route(
            "/feeds/{owner}/{topic}",
            get_or_head(retrieval::download_feed).post(retrieval::create_feed),
        )
        .route("/bzz", post(retrieval::upload_bzz))
        // ACT (access control) grantee management, bee
        // `pkg/api/accesscontrol.go`: create / list / patch grantee
        // lists keyed by the publisher (this node's key).
        .route("/grantee", post(act::grantee_create))
        .route(
            "/grantee/{address}",
            get(act::grantee_get).patch(act::grantee_patch),
        )
        .route("/bytes/{addr}", get_or_head(retrieval::bytes))
        // Ant-specific extension (not part of bee Tier-A): list paths in
        // a mantaray manifest. Namespaced under `/v0/` so the bee compat
        // surface stays clean and the demo UI at `vibing.at/ant/` can
        // still introspect manifests.
        .route("/v0/manifest/{addr}", get(retrieval::manifest))
        .route("/bzz/{addr}", get_or_head(retrieval::bzz_root))
        // Trailing-slash root: bee treats `/bzz/<addr>/` as equivalent to
        // `/bzz/<addr>` (both resolve to the manifest's
        // `website-index-document`). Axum's `{*path}` wildcard requires at
        // least one character, so without this explicit route the
        // trailing-slash form falls through to the 501 fallback —
        // breaking, for example, freedom-browser's `bzz://` protocol
        // handler which canonicalises every URL with `new URL(...)` and
        // therefore appends a `/` whenever the source URL had no path.
        .route("/bzz/{addr}/", get_or_head(retrieval::bzz_root))
        .route("/bzz/{addr}/{*path}", get_or_head(retrieval::bzz_with_path))
        .fallback(not_implemented)
        .layer(DefaultBodyLimit::max(GATEWAY_MAX_UPLOAD_BYTES as usize))
        .layer(middleware::from_fn(request_span))
        // CORS is applied outermost so a preflight `OPTIONS` is answered
        // before the body-limit / span layers and never reaches a route
        // (PLAN.md J.4.8). `from_fn_with_state` hands it the same handle
        // the routes use, so the policy reads `handle.cors`.
        .layer(middleware::from_fn_with_state(
            handle.clone(),
            crate::cors::cors_middleware,
        ))
        .with_state(handle)
}

/// Wrap every request in a tracing span carrying the method and target
/// path, and tag the response with the elapsed time once the handler
/// returns. The span keeps each request's logs from interleaving
/// across concurrent connections; the elapsed log gives ops a one-line
/// "did this work?" trace without enabling debug-level on the joiner.
async fn request_span(req: Request, next: Next) -> Response {
    let method = req.method().clone();
    let path = req.uri().path().to_string();
    let span = tracing::info_span!(
        target: "ant_gateway",
        "http",
        %method,
        %path,
    );
    let started = Instant::now();
    let mut resp = next.run(req).instrument(span.clone()).await;
    let elapsed_ms = started.elapsed().as_millis() as u64;
    let status = resp.status().as_u16();
    let _entered = span.enter();
    resp.headers_mut().insert(
        axum::http::header::SERVER,
        HeaderValue::from_static(SERVER_HEADER),
    );
    // Bee's `jsonhttp.Respond` stamps `application/json; charset=utf-8`
    // on every JSON body; axum's `Json` writes the bare media type.
    // Normalize here so every handler (and any future one) matches bee
    // without each call site needing to remember the charset. Skip
    // responses whose content type is served verbatim from manifest
    // metadata (`VerbatimContentType`): bee's `serveManifestEntry`
    // copies the stored value byte-for-byte, so a `.json` file's
    // `application/json` must NOT grow a charset suffix.
    if resp
        .extensions()
        .get::<crate::retrieval::VerbatimContentType>()
        .is_none()
        && resp
            .headers()
            .get(axum::http::header::CONTENT_TYPE)
            .is_some_and(|ct| ct.as_bytes() == b"application/json")
    {
        resp.headers_mut().insert(
            axum::http::header::CONTENT_TYPE,
            crate::error::JSON_CONTENT_TYPE,
        );
    }
    if resp.status() == StatusCode::INTERNAL_SERVER_ERROR
        || resp.status() == StatusCode::SERVICE_UNAVAILABLE
        || resp.status() == StatusCode::GATEWAY_TIMEOUT
    {
        tracing::warn!(target: "ant_gateway", %status, %method, %path, elapsed_ms, "request");
    } else {
        tracing::debug!(target: "ant_gateway", %status, %method, %path, elapsed_ms, "request");
    }
    resp
}

/// Same handler for both `GET` and `HEAD`. Each retrieval handler
/// inspects the `Method` extractor itself and emits an empty body for
/// `HEAD` (axum doesn't strip it for us).
fn get_or_head<H, T, S>(handler: H) -> MethodRouter<S>
where
    H: axum::handler::Handler<T, S> + Clone,
    T: 'static,
    S: Clone + Send + Sync + 'static,
{
    on(MethodFilter::GET.or(MethodFilter::HEAD), handler)
}
