//! D.2.2 status endpoints.
//!
//! Every handler reads the live `StatusSnapshot` exposed via the
//! [`GatewayHandle`] and reshapes it into bee's wire format. Field names
//! are bee's verbatim — bee-js and the `bee` Go client both index by
//! these strings, so any drift will look like data corruption to a
//! consumer.
//!
//! The plan (PLAN.md C.4.2) writes some shapes in `snake_case` as informal
//! notation, but the reference bee implementation marshals every JSON
//! field in camelCase. We follow bee on the wire: that's what bee-js
//! decodes against.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;
use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::handle::GatewayHandle;

#[derive(Debug, Serialize)]
struct HealthBody {
    status: &'static str,
    version: String,
    #[serde(rename = "apiVersion")]
    api_version: String,
    /// `false` while the daemon's startup chain init (postage /
    /// chequebook Gnosis reads) is still resolving. The listener now
    /// binds before that init (issue #38), so supervisors get their
    /// `200` from t≈0 and can read the init progress here instead of
    /// inferring it from connection-refused. Additive to bee's body —
    /// bee-js ignores unknown fields.
    #[serde(rename = "chainReady")]
    chain_ready: bool,
}

/// `GET /health`. Bound before chain init; PLAN.md D.2.1 requires this
/// to answer within 50 ms of socket bind so external supervisors can
/// poll aggressively, hence no I/O at all here.
pub async fn health(State(handle): State<GatewayHandle>) -> Response {
    let chain_ready = handle.chain_state().is_some();
    Json(HealthBody {
        status: "ok",
        version: handle.agent.as_str().to_string(),
        api_version: handle.api_version.as_str().to_string(),
        chain_ready,
    })
    .into_response()
}

/// `GET /readiness`. `200` once we have at least one BZZ-handshaked
/// peer; `503` until then. Bee marshals a JSON status body in BOTH cases
/// (`{"status":"ready",…}` / `{"status":"unready",…}`, same shape as
/// `/health`) — bee-js and our Swift `BeeReadiness` both index `status`,
/// so a bodyless `200` reads as not-ready. We mirror `/health`'s body.
/// Peers whose connection stopped answering pings (`stale`, issue #83)
/// don't make the node ready.
pub async fn readiness(State(handle): State<GatewayHandle>) -> Response {
    let snap = handle.status.borrow();
    let ready = snap.peers.connected > 0
        || snap
            .peers
            .connected_peers
            .iter()
            .any(|p| p.bzz_overlay.is_some() && !p.stale);
    let body = Json(HealthBody {
        status: if ready { "ready" } else { "unready" },
        version: handle.agent.as_str().to_string(),
        api_version: handle.api_version.as_str().to_string(),
        chain_ready: handle.chain_state().is_some(),
    });
    if ready {
        body.into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, body).into_response()
    }
}

#[derive(Debug, Serialize)]
struct NodeBody {
    #[serde(rename = "beeMode")]
    bee_mode: &'static str,
    #[serde(rename = "gatewayMode")]
    gateway_mode: bool,
    #[serde(rename = "chequebookEnabled")]
    chequebook_enabled: bool,
    #[serde(rename = "swapEnabled")]
    swap_enabled: bool,
    /// Ant extension: SWAP settlement capability and state, so a host
    /// can tell this node pays peers and has the runtime switch without
    /// parsing `antd --help` (Freedom's wallet UI).
    settlement: SettlementBody,
}

/// `GET /node`'s `settlement` object and the body of
/// `GET`/`PUT /v0/settlement/swap`.
#[derive(Debug, Serialize)]
pub(crate) struct SettlementBody {
    /// This node can pay peers with SWAP cheques once it has a funded
    /// chequebook (light mode). `false` on an ultra-light node.
    pub(crate) supported: bool,
    /// The `swap-enable` switch can be read and set at runtime on
    /// `/v0/settlement/swap`.
    #[serde(rename = "swapSwitch")]
    pub(crate) swap_switch: bool,
    /// Bee's `swap-enable`, live: cheques may pay peers, for downloads
    /// and uploads alike.
    #[serde(rename = "swapEnabled")]
    pub(crate) swap_enabled: bool,
    /// Cheques are paid right now: the switch is on and the chequebook
    /// has funds left as of the node's last chain read (every 60 s).
    /// `false`: the free pseudosettle tier.
    pub(crate) paying: bool,
    /// The chequebook settlement runs on (`0x…`), or `null`.
    pub(crate) chequebook: Option<String>,
}

impl SettlementBody {
    pub(crate) fn of(handle: &GatewayHandle, light: bool) -> Self {
        let s = handle.status.borrow().settlement.clone();
        Self {
            supported: light,
            swap_switch: true,
            swap_enabled: s.swap_enabled,
            paying: light && s.paying,
            chequebook: s.chequebook,
        }
    }
}

/// `GET /node`. Reports `light` once `antd` has uploads + SWAP
/// settlement configured, `ultra-light` otherwise. Freedom's
/// `checkSwarmPreFlight` refuses to publish against an `ultra-light`
/// node, so this flag is the gate that lets the whole
/// publish/feed/SOC-write surface work (PLAN.md J.4.1). While chain
/// init is still resolving we answer a retryable `503` rather than a
/// premature `ultra-light` that would wrongly disable Freedom's
/// publish surface. `chequebookEnabled` tracks `beeMode` because, in
/// `antd`, light mode *is* the SWAP-settlement mode; `swapEnabled` is
/// bee's `swap-enable` switch as the node runs it now (always `false`
/// on an ultra-light node, as in bee). The Ant-only `settlement` object
/// adds the capability (`supported`, `swapSwitch`) and whether cheques
/// are paid right now; see `/v0/settlement/swap`.
pub async fn node(State(handle): State<GatewayHandle>) -> Response {
    let Some(state) = handle.chain_state() else {
        return crate::error::chain_initializing();
    };
    let light = state.light_mode;
    let settlement = SettlementBody::of(&handle, light);
    Json(NodeBody {
        bee_mode: if light { "light" } else { "ultra-light" },
        gateway_mode: false,
        chequebook_enabled: light,
        swap_enabled: light && settlement.swap_enabled,
        settlement,
    })
    .into_response()
}

/// `GET`/`PUT /v0/settlement/swap` body: the node's settlement state,
/// and whether a change made here survives a restart.
#[derive(Debug, Serialize)]
struct SwapBody {
    #[serde(flatten)]
    settlement: SettlementBody,
    /// Always `false`: the runtime switch isn't persisted. `antd` reads
    /// `swap-enable` from its `--config` file or `--swap-enable` at the
    /// next start (Freedom writes it into the `config.yaml` it
    /// generates); ant-ffi hosts call `ant_set_swap_enabled` after every
    /// `ant_init`.
    persisted: bool,
}

/// `PUT /v0/settlement/swap` request: `{"swapEnabled": bool}`.
#[derive(Debug, serde::Deserialize)]
pub struct SwapRequest {
    #[serde(rename = "swapEnabled")]
    swap_enabled: bool,
}

/// How long `PUT /v0/settlement/swap` waits for the node loop.
const SWAP_SWITCH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// `GET /v0/settlement/swap` (Ant extension): bee's `swap-enable` switch
/// as the node runs it now, whether cheques are being paid, and the
/// chequebook — the same object as `GET /node`'s `settlement`, plus
/// `persisted`. `503` until chain init resolves.
pub async fn swap_get(State(handle): State<GatewayHandle>) -> Response {
    let Some(state) = handle.chain_state() else {
        return crate::error::chain_initializing();
    };
    Json(SwapBody {
        settlement: SettlementBody::of(&handle, state.light_mode),
        persisted: false,
    })
    .into_response()
}

/// `PUT /v0/settlement/swap` with `{"swapEnabled": bool}` (Ant
/// extension): switch SWAP settlement on or off for the running node,
/// downloads and uploads alike — the same command as `antctl`'s,
/// `antd --swap-enable` at runtime, and ant-ffi's
/// `ant_set_swap_enabled`, so a host running `antd
/// --no-control-socket` (Freedom) needn't restart the node to flip it.
/// Answers the state afterwards. Not persisted (see [`SwapBody`]).
///
/// Turning it on lets the node spend its chequebook, so the route takes
/// the same guard as the wallet-spending routes (#105): a request from a
/// web page is refused unless its origin is listed exactly in
/// `cors-allowed-origins` (`cors::settlement_switch_guard`).
pub async fn swap_put(
    State(handle): State<GatewayHandle>,
    Json(req): Json<SwapRequest>,
) -> Response {
    let Some(state) = handle.chain_state() else {
        return crate::error::chain_initializing();
    };
    let (ack, rx) = tokio::sync::oneshot::channel();
    let cmd = ant_control::ControlCommand::SetSwapEnabled {
        enabled: req.swap_enabled,
        ack,
    };
    if handle.commands.send(cmd).await.is_err() {
        return crate::retrieval::node_unavailable();
    }
    match tokio::time::timeout(SWAP_SWITCH_TIMEOUT, rx).await {
        Ok(Ok(ant_control::ControlAck::Ok { .. })) => {}
        Ok(Ok(ant_control::ControlAck::Error { message })) => {
            return crate::error::json_error(StatusCode::INTERNAL_SERVER_ERROR, message);
        }
        Ok(Ok(_) | Err(_)) => return crate::retrieval::node_unavailable(),
        Err(_) => {
            return crate::error::json_error(
                StatusCode::GATEWAY_TIMEOUT,
                "the node didn't answer the swap switch in time",
            )
        }
    }
    // The node loop publishes the new state before it acks.
    Json(SwapBody {
        settlement: SettlementBody::of(&handle, state.light_mode),
        persisted: false,
    })
    .into_response()
}

#[derive(Debug, Serialize)]
struct AddressesBody {
    overlay: String,
    underlay: Vec<String>,
    ethereum: String,
    #[serde(rename = "publicKey")]
    public_key: String,
    #[serde(rename = "pssPublicKey")]
    pss_public_key: String,
}

/// `GET /addresses`. `overlay` and `publicKey` are bare hex (bee's
/// `swarm.Address.MarshalJSON` and `crypto.SerializeSecpkPublicKey` both
/// drop the `0x` prefix); `ethereum` keeps its `0x` prefix. We don't
/// have a dedicated PSS key (see PLAN.md Appendix B — PSS is a 501
/// surface), but bee-js panics on a missing `pssPublicKey` field, so we
/// echo `publicKey` rather than emit `null`.
pub async fn addresses(State(handle): State<GatewayHandle>) -> Response {
    let snap = handle.status.borrow();
    let underlay: Vec<String> = snap.listeners.clone();
    let identity = &handle.identity;
    Json(AddressesBody {
        overlay: identity.overlay_hex.clone(),
        underlay,
        ethereum: identity.ethereum_hex.clone(),
        public_key: identity.public_key_hex.clone(),
        pss_public_key: identity.public_key_hex.clone(),
    })
    .into_response()
}

#[derive(Debug, Serialize)]
struct PeerEntry {
    address: String,
    #[serde(rename = "fullNode", skip_serializing_if = "Option::is_none")]
    full_node: Option<bool>,
}

#[derive(Debug, Serialize)]
struct PeersBody {
    peers: Vec<PeerEntry>,
}

/// `GET /peers`. Bee's `Peer.address` is the **swarm overlay** (not the
/// libp2p peer-id), so we surface `bzz_overlay` from each connected
/// peer; pre-handshake peers (no `bzz_overlay` yet) are skipped because
/// bee never reports a peer it hasn't handshaked.
pub async fn peers(State(handle): State<GatewayHandle>) -> Response {
    let snap = handle.status.borrow();
    let peers: Vec<PeerEntry> = snap
        .peers
        .connected_peers
        .iter()
        .filter_map(|p| {
            let overlay = p.bzz_overlay.as_deref()?;
            Some(PeerEntry {
                address: strip_0x(overlay).to_string(),
                full_node: p.full_node,
            })
        })
        .collect();
    Json(PeersBody { peers }).into_response()
}

#[derive(Debug, Serialize)]
struct BinInfo {
    population: u32,
    connected: u32,
    #[serde(rename = "disconnectedPeers", skip_serializing_if = "Vec::is_empty")]
    disconnected_peers: Vec<PeerEntry>,
    #[serde(rename = "connectedPeers", skip_serializing_if = "Vec::is_empty")]
    connected_peers: Vec<PeerEntry>,
}

#[derive(Debug, Serialize)]
struct TopologyBody {
    #[serde(rename = "baseAddr")]
    base_addr: String,
    population: u32,
    connected: u32,
    timestamp: String,
    #[serde(rename = "nnLowWatermark")]
    nn_low_watermark: u32,
    depth: u32,
    reachability: &'static str,
    #[serde(rename = "networkAvailability")]
    network_availability: &'static str,
    bins: BTreeMap<String, BinInfo>,
}

/// `GET /topology`. Mirrors bee `pkg/topology/kademlia.kademlia.Snapshot`:
/// 32 named bins (`bin_0` … `bin_31`) carrying population / connected
/// counts. We don't yet enumerate per-peer rows inside each bin (the
/// bee-js dashboard renders fine without them), so the bin entries omit
/// `connectedPeers` / `disconnectedPeers` via `skip_serializing_if`.
pub async fn topology(State(handle): State<GatewayHandle>) -> Response {
    let snap = handle.status.borrow();
    let routing = &snap.peers.routing;

    let base_addr = if routing.base_overlay.is_empty() {
        handle.identity.overlay_hex.clone()
    } else {
        strip_0x(&routing.base_overlay).to_string()
    };

    // `connected_bins` = peers we have live BZZ sessions with;
    // `known_bins` = those plus hive-discovered-but-unconnected peers
    // (deduped by overlay). bee-shaped dashboards show two counters:
    // Connected Peers ← `GET /peers` length ≈ sum(connected), and
    // Visible Peers ← `sum(bins[*].population)`. We mirror that by
    // reporting per-bin population from the known book and connected
    // from the live table. Older daemons send no `known_bins`, so fall
    // back to connected == population (the pre-row-9 behaviour).
    let connected_bins = pad32(&routing.bins);
    let known_bins = if routing.known_bins.is_empty() {
        connected_bins.clone()
    } else {
        pad32(&routing.known_bins)
    };

    let mut bins = BTreeMap::new();
    for i in 0..32 {
        bins.insert(
            format!("bin_{i}"),
            BinInfo {
                population: known_bins[i],
                connected: connected_bins[i],
                disconnected_peers: Vec::new(),
                connected_peers: Vec::new(),
            },
        );
    }

    let connected: u32 = connected_bins.iter().sum();
    let population = if routing.known_size > 0 {
        routing.known_size
    } else {
        routing.size
    };
    let depth = neighborhood_depth(&connected_bins);
    let now_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());

    Json(TopologyBody {
        base_addr,
        population,
        connected,
        timestamp: format_rfc3339(now_unix),
        nn_low_watermark: 0,
        depth,
        reachability: "Unknown",
        network_availability: "Unknown",
        bins,
    })
    .into_response()
}

/// Right-pad (or truncate) a per-bin count vector to exactly 32 entries.
fn pad32(bins: &[u32]) -> Vec<u32> {
    let mut out = bins.to_vec();
    out.resize(32, 0);
    out
}

/// Saturation depth from connected per-bin counts: the lowest PO bin
/// that is *not* saturated (fewer than `SATURATION` connected peers).
/// bee derives storage/neighborhood depth the same way — every bin
/// below the depth has a healthy fan-out, so the depth is where our
/// connectivity thins out. Returns 0 when bin 0 is already unsaturated
/// (or we have no peers), matching bee's "no neighborhood yet" state.
fn neighborhood_depth(connected_bins: &[u32]) -> u32 {
    /// Per-bin connected count bee treats as a healthy, saturated bin.
    const SATURATION: u32 = 4;
    let mut depth = 0u32;
    for (i, &c) in connected_bins.iter().enumerate() {
        if c >= SATURATION {
            depth = (i as u32) + 1;
        } else {
            break;
        }
    }
    depth
}

/// RFC3339 timestamp for the current wall-clock second. Shared with
/// the tag registry (`startedAt`) so both surfaces emit the identical
/// bee-parseable form.
pub(crate) fn format_rfc3339_now() -> String {
    let now_unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    format_rfc3339(now_unix)
}

/// Best-effort RFC3339 (`1970-01-01T00:00:00Z` form) without pulling in
/// `chrono`. Bee's `timestamp` field uses `time.Time.MarshalJSON` which
/// emits RFC3339; `bee-js` parses it via `new Date(...)` which accepts
/// both `Z` and `+00:00` suffixes, so the canonical form is enough.
pub(crate) fn format_rfc3339(unix: u64) -> String {
    // Days since 1970-01-01 → calendar date via Howard Hinnant's
    // civil_from_days (public domain). Cheaper than pulling in `time`
    // for one field.
    let secs = unix as i64;
    let days = secs.div_euclid(86_400);
    let secs_of_day = secs.rem_euclid(86_400) as u32;
    let (year, month, day) = civil_from_days(days);
    let hour = secs_of_day / 3_600;
    let minute = (secs_of_day / 60) % 60;
    let second = secs_of_day % 60;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z")
}

const fn civil_from_days(days: i64) -> (i32, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m, d)
}

fn strip_0x(s: &str) -> &str {
    s.strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s)
}

#[cfg(test)]
mod tests {
    use super::{neighborhood_depth, pad32};

    #[test]
    fn pad32_resizes_to_32() {
        assert_eq!(pad32(&[]).len(), 32);
        assert_eq!(pad32(&[1, 2, 3]).len(), 32);
        // Over-long input is truncated to 32.
        assert_eq!(pad32(&[7u32; 40]).len(), 32);
        assert_eq!(pad32(&[1, 2, 3])[1], 2);
    }

    #[test]
    fn depth_zero_when_no_neighborhood() {
        assert_eq!(neighborhood_depth(&[0u32; 32]), 0);
        // Bin 0 below saturation → depth 0 even if higher bins are full.
        let mut bins = vec![0u32; 32];
        bins[3] = 10;
        assert_eq!(neighborhood_depth(&bins), 0);
    }

    #[test]
    fn depth_counts_contiguous_saturated_bins() {
        let mut bins = vec![0u32; 32];
        // Bins 0,1,2 saturated (>=4), bin 3 not → depth 3.
        bins[0] = 4;
        bins[1] = 8;
        bins[2] = 5;
        bins[3] = 1;
        bins[4] = 9; // gap after the break is ignored
        assert_eq!(neighborhood_depth(&bins), 3);
    }
}
