//! Bee-shaped HTTP gateway in front of `antd`.
//!
//! `ant-gateway` is the smallest cut of a bee-compatible HTTP surface
//! that lets unmodified bee consumers (`bee-js`, `curl`, browser proxies)
//! drive read-only browsing of Swarm mainnet content against `antd`.
//! See [`PLAN.md`] Appendix C (compatibility contract) and Appendix D
//! (Tier-A checklist) for the full scope.
//!
//! # Surface
//!
//! - Status: `/health`, `/readiness`, `/node`, `/addresses`, `/peers`,
//!   `/topology`.
//! - Retrieval: `/bytes/{addr}`, `/chunks/{addr}`, `/bzz/{addr}`,
//!   `/bzz/{addr}/{path}`.
//! - Stubs: `/wallet`, `/stamps`, `/chequebook/address`,
//!   `/chequebook/balance` — bee-shaped zero-balances so `bee-js` UIs
//!   degrade cleanly.
//! - Catch-all `501 {code:501, message:"not implemented in ant"}` for
//!   anything else.
//!
//! All response bodies follow bee's `jsonhttp` shape: success bodies
//! use the bee field names verbatim, errors are `{code, message}`. The
//! catch-all is implemented as an axum `fallback` so adding a new
//! handler later automatically removes it from the 501 surface.
//!
//! [`PLAN.md`]: ../../../PLAN.md

#![cfg_attr(not(feature = "http-api"), allow(dead_code))]
// Handler helpers early-return an already-built axum `Response` as the
// `Err` variant (`Result<T, Response>` + `?`) — axum's idiomatic "stop
// here and send this reply". `clippy::result_large_err` flags that
// ~128-byte `Response`; boxing it would cost a `*` unwrap at every `?`
// site for nothing on a path about to do network I/O. So each such
// helper carries a targeted `#[allow(clippy::result_large_err)]`
// rather than a crate-wide one, keeping the lint live for any other
// genuinely large error type added here.

#[cfg(feature = "http-api")]
mod act;
#[cfg(feature = "http-api")]
mod chain;
#[cfg(feature = "chain")]
pub mod chainreader;
#[cfg(feature = "http-api")]
mod chunk_stream;
#[cfg(feature = "http-api")]
mod cors;
#[cfg(feature = "http-api")]
mod error;
#[cfg(feature = "http-api")]
mod fallback;
#[cfg(feature = "http-api")]
mod handle;
#[cfg(feature = "http-api")]
mod pins;
#[cfg(feature = "http-api")]
mod retrieval;
#[cfg(feature = "http-api")]
mod router;
#[cfg(feature = "http-api")]
mod settlements;
#[cfg(feature = "http-api")]
mod stamps;
#[cfg(feature = "http-api")]
mod status;
#[cfg(feature = "http-api")]
mod stewardship;
#[cfg(feature = "http-api")]
mod subscribe;
#[cfg(feature = "http-api")]
mod tags;

#[cfg(feature = "http-api")]
pub use chain::{BatchMetaView, ChainContext, ChainReader, ChainWriter};
#[cfg(feature = "http-api")]
pub use cors::CorsConfig;
#[cfg(feature = "http-api")]
pub use handle::{GatewayChainState, GatewayHandle, GatewayIdentity};
#[cfg(feature = "http-api")]
pub use tags::TagRegistry;

#[cfg(feature = "http-api")]
use std::net::SocketAddr;
#[cfg(feature = "http-api")]
use thiserror::Error;

/// Errors raised by [`Gateway::serve`].
///
/// `Bind` and `Serve` are kept distinct so a startup failure (port already
/// in use) shows up differently from a runtime failure (hyper graceful
/// shutdown error after a long uptime).
#[cfg(feature = "http-api")]
#[derive(Debug, Error)]
pub enum GatewayError {
    #[error("bind {addr}: {source}")]
    Bind {
        addr: SocketAddr,
        #[source]
        source: std::io::Error,
    },
    #[error("serve: {0}")]
    Serve(#[source] std::io::Error),
}

/// HTTP gateway entry point. Holds no state of its own; everything is
/// driven by the [`GatewayHandle`] passed to [`Gateway::serve`], which
/// owns the live status receiver and the command sender into the node
/// loop.
#[cfg(feature = "http-api")]
pub struct Gateway;

#[cfg(feature = "http-api")]
impl Gateway {
    /// Bind to `addr` and serve the bee-shaped HTTP API until the
    /// underlying listener errors.
    ///
    /// `handle` is the live view into the running node: status snapshot
    /// receiver, control-command sender, and the static identity bits
    /// (overlay / ethereum address / libp2p public key) that `antd`
    /// resolved at startup.
    ///
    /// Runs forever (`Ok(())` is unreachable on a healthy listener);
    /// the caller is expected to race this future against the node
    /// loop with `tokio::select!` so a node-side fatal still tears the
    /// process down.
    pub async fn serve(handle: GatewayHandle, addr: SocketAddr) -> Result<(), GatewayError> {
        let app = router::build(handle);
        let listener = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| GatewayError::Bind { addr, source: e })?;
        let bound = listener
            .local_addr()
            .map_or_else(|_| addr.to_string(), |a| a.to_string());
        tracing::info!(target: "ant_gateway", "HTTP API listening on {bound}");
        axum::serve(listener, app)
            .await
            .map_err(GatewayError::Serve)
    }
}

#[cfg(feature = "http-api")]
#[doc(hidden)]
pub mod testkit {
    //! In-process helpers for crate-internal and integration tests.
    //!
    //! Exposed because the integration tests in `tests/` need to build
    //! the same router production uses without standing up a real
    //! node loop. Hidden from public docs because the API surface here
    //! is unstable by design.
    pub use crate::handle::GatewayHandle;
    pub use crate::router::build as build_router;
}
