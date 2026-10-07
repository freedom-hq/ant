//! `/v0/cache` (Ant extension): see, clear and resize the chunk cache
//! of a running node — ant-ffi's `ant_cache_status` / `ant_cache_clear`
//! / `ant_cache_set_capacity` over HTTP, for hosts that run `antd` and
//! only talk to its API (Freedom desktop, #149). Same work
//! ([`ant_retrieval::ChunkCaches`]) and the same JSON shapes.
//!
//! Bee has no equivalent (its cache figures are `GET /debugstore`, its
//! only clear the offline `bee db nuke`), hence the `/v0/` namespace.
//!
//! The writes are destructive, so they take [`local_host_guard`]: the
//! caller must be on loopback and must not be a web page.

use std::net::SocketAddr;

use axum::extract::connect_info::ConnectInfo;
use axum::extract::rejection::ExtensionRejection;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;

use ant_retrieval::SetCapacityError;

use crate::error::json_error;
use crate::handle::GatewayHandle;

/// `PUT /v0/cache/capacity` body.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CapacityRequest {
    /// The new disk cache cap in bytes, clamped to 64 MiB ..= 16 GiB.
    pub bytes: u64,
}

/// `501` for an embedder that didn't hand its caches over.
fn no_cache_controls() -> Response {
    json_error(
        StatusCode::NOT_IMPLEMENTED,
        "chunk cache controls are not available on this node",
    )
}

/// `GET /v0/cache`: the cache figures (see
/// [`ant_retrieval::ChunkCaches::status_json`]). Counters only, cheap to poll.
pub async fn status(State(handle): State<GatewayHandle>) -> Response {
    match &handle.cache {
        Some(c) => Json(c.status_json()).into_response(),
        None => no_cache_controls(),
    }
}

/// `POST /v0/cache/clear`: evict every unpinned chunk from the disk and
/// in-memory caches and give the space back to the OS; pinned and
/// published content stays. Answers what was freed plus the status
/// afterwards (see [`ant_retrieval::ChunkCaches::clear`]). On a node without a disk
/// cache only the in-memory cache is emptied (`status.disk_enabled` is
/// `false`).
pub async fn clear(State(handle): State<GatewayHandle>) -> Response {
    let Some(c) = &handle.cache else {
        return no_cache_controls();
    };
    match c.clear().await {
        Ok(body) => Json(body).into_response(),
        Err(e) => json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("clear disk cache: {e}"),
        ),
    }
}

/// `PUT /v0/cache/capacity` with `{"bytes": N}`: set the disk cache cap
/// live, clamped to 64 MiB ..= 16 GiB, evicting down to it before
/// answering. Answers the status afterwards; its `capacity_bytes` is the
/// cap applied. Not persisted: the host saves it (`cache-capacity` in
/// the config file) for the next start.
///
/// `503` when the node has no disk cache (`--no-disk-cache`, or it failed
/// to open at start). `500` when the eviction failed: the new cap is
/// still applied and the next cache write evicts down to it again, so a
/// host needn't revert its saved setting.
pub async fn set_capacity(
    State(handle): State<GatewayHandle>,
    Json(req): Json<CapacityRequest>,
) -> Response {
    let Some(c) = &handle.cache else {
        return no_cache_controls();
    };
    match c.set_capacity(req.bytes).await {
        Ok(_) => Json(c.status_json()).into_response(),
        Err(e @ SetCapacityError::Unavailable) => json_error(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("{e} (disabled with --no-disk-cache, or it failed to open at start)"),
        ),
        Err(e @ SetCapacityError::Evict(_)) => {
            json_error(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
        }
    }
}

/// Guard for the `/v0/cache` writes: `403` unless the TCP peer is a
/// loopback address and the request isn't a web page's (the
/// [`crate::cors::wallet_spend_guard`] rule: no `Origin` and no
/// cross-site `Sec-Fetch-Site`, or an origin listed exactly in
/// `cors-allowed-origins`). `POST /v0/cache/clear` has no body, so
/// without the second check any page could send it as a CORS-simple
/// request; the first keeps it off the network when `api-addr` binds a
/// non-loopback address. A request whose peer address is unknown (the
/// router served without connect info) is refused.
pub async fn local_host_guard(
    State(handle): State<GatewayHandle>,
    peer: Result<ConnectInfo<SocketAddr>, ExtensionRejection>,
    req: Request,
    next: Next,
) -> Response {
    let loopback = peer.is_ok_and(|ConnectInfo(peer)| peer.ip().to_canonical().is_loopback());
    if !loopback {
        return json_error(
            StatusCode::FORBIDDEN,
            "this route changes the node's chunk cache and only accepts requests from the local host",
        );
    }
    if !crate::cors::browser_request_allowed(&handle.cors, req.headers()) {
        return json_error(
            StatusCode::FORBIDDEN,
            "this route changes the node's chunk cache and does not accept requests from web \
             pages; list the page's origin explicitly in cors-allowed-origins to allow it",
        );
    }
    next.run(req).await
}
