//! Swarm ACT (access control) HTTP surface.
//!
//! Bee-shaped access control, end to end (`bee/pkg/api/accesscontrol.go`
//! + `bee/pkg/accesscontrol`):
//!
//! - **Upload** (`swarm-act: true` on `POST /bytes`, `/bzz`, `/chunks`,
//!   `/soc`, `/feeds`): the *content is uploaded unchanged* — ACT
//!   encrypts only the returned **reference** under a per-history
//!   access key. Without a `swarm-act-history-address` header a fresh
//!   history is created (a mantaray manifest with one epoch pointing
//!   at a new ACT key-value store that grants only the publisher);
//!   with one, the epoch valid *now* is reused and nothing new is
//!   stored (bee's `UploadHandler` only writes when `historyRef` is
//!   zero). The response carries the encrypted reference in the JSON
//!   body and the history address in `swarm-act-history-address`.
//! - **Download** (`swarm-act-publisher` + `swarm-act-history-address`
//!   [+ `swarm-act-timestamp`] on `GET|HEAD /bytes`, `/chunks`,
//!   `/bzz`): resolve the epoch for the timestamp (default: now),
//!   look up this node's grantee row by its ECDH lookup key, unwrap
//!   the access key, decrypt the reference, and serve the content it
//!   points at. Bee applies this as middleware
//!   (`actDecryptionHandler`); ant hooks the same resolution into the
//!   front of each download handler.
//! - **Grantees** (`POST /grantee`, `GET|PATCH /grantee/{address}`):
//!   bee's `UpdateHandler` semantics verbatim, including the
//!   new-access-key-on-revoke rule, the swap-remove ordering of the
//!   grantee list, and the same-second-update failure (a duplicate
//!   history epoch key → 500 `"failed to create or update grantee
//!   list"`).
//!
//! The publisher identity is the node's own key
//! ([`GatewayHandle::act_secret`]), exactly like bee wires
//! `accesscontrol.NewDefaultSession(swarmPrivateKey)` in `start.go`.
//! Storage rides the normal chunk machinery: reads go through
//! `ControlCommand::GetChunkRaw` (via [`CommandFetcher`]), writes are
//! split/manifest chunks pushed with [`crate::retrieval::push_chunks`].

use std::collections::BTreeMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ant_control::{ControlAck, ControlCommand, GatewayRequestKind};
use ant_crypto::act as actc;
use ant_retrieval::act as acts;
use ant_retrieval::act::{ActError, ActKvs};
use ant_retrieval::{split_bytes, split_bytes_encrypted, ChunkFetcher, SplitChunk};
use async_trait::async_trait;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::response::Response;
use k256::PublicKey;
use serde::Deserialize;
use tokio::sync::{mpsc, oneshot};
use tracing::debug;

use crate::error::{json_error, params_error, ParamKind, Reason};
use crate::handle::GatewayHandle;
use crate::retrieval::{
    go_parse_bool, json_response, parse_postage_batch_header, push_chunks, request_timeout,
    strconv_error_text, DEFAULT_REQUEST_TIMEOUT,
};

pub(crate) const SWARM_ACT: HeaderName = HeaderName::from_static("swarm-act");
pub(crate) const SWARM_ACT_PUBLISHER: HeaderName = HeaderName::from_static("swarm-act-publisher");
pub(crate) const SWARM_ACT_HISTORY_ADDRESS: HeaderName =
    HeaderName::from_static("swarm-act-history-address");
pub(crate) const SWARM_ACT_TIMESTAMP: HeaderName = HeaderName::from_static("swarm-act-timestamp");
const ACCESS_CONTROL_EXPOSE_HEADERS: HeaderName =
    HeaderName::from_static("access-control-expose-headers");

/// Bee's `errActDownload` / `errActUpload` / `errActGranteeList`
/// fallback messages, verbatim.
const ERR_ACT_DOWNLOAD: &str = "act download failed";
const ERR_ACT_UPLOAD: &str = "act upload failed";
const ERR_ACT_GRANTEE_LIST: &str = "failed to create or update grantee list";
/// Bee's `accesscontrol.ErrNotFound` HTTP body.
const ERR_ACT_NOT_FOUND: &str = "act or history entry not found";

// --- command-channel chunk fetcher -------------------------------------------

/// [`ChunkFetcher`] over the node loop's `GetChunkRaw`: the gateway's
/// window into the chunk store for walking ACT histories, key-value
/// stores, and grantee lists. Each artifact is a handful of chunks, so
/// per-chunk command round-trips are fine.
pub(crate) struct CommandFetcher {
    commands: mpsc::Sender<ControlCommand>,
    timeout: Duration,
}

impl CommandFetcher {
    pub(crate) fn new(handle: &GatewayHandle, timeout: Duration) -> Self {
        Self {
            commands: handle.commands.clone(),
            timeout,
        }
    }
}

#[async_trait]
impl ChunkFetcher for CommandFetcher {
    async fn fetch(
        &self,
        addr: [u8; 32],
    ) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        let (ack_tx, ack_rx) = oneshot::channel::<ControlAck>();
        // ACT histories, key-value stores and grantee lists are expected
        // to exist: no fast-miss fan-out, which would mostly buy duplicate,
        // separately-charged deliveries (issue #146).
        let cmd = ControlCommand::GetChunkRaw {
            reference: addr,
            fast_miss: false,
            ack: ack_tx,
        };
        self.commands
            .send(cmd)
            .await
            .map_err(|_| "node loop unavailable")?;
        match tokio::time::timeout(self.timeout, ack_rx).await {
            Ok(Ok(ControlAck::Bytes { data })) => Ok(data),
            Ok(Ok(
                ControlAck::Error { message }
                | ControlAck::NotFound { message }
                | ControlAck::NotReady { message },
            )) => Err(message.into()),
            Ok(Ok(other)) => Err(format!("unexpected ack from GetChunkRaw: {other:?}").into()),
            Ok(Err(_)) => Err("node loop dropped the ack".into()),
            Err(_) => Err("chunk fetch timed out".into()),
        }
    }
}

// --- header parsing -----------------------------------------------------------

/// Unix seconds, as bee's `time.Now().Unix()`.
fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}

/// Parse a hex value the way bee's `mapStructure` does for
/// `swarm.Address` / `[]byte` fields: Go `encoding/hex` semantics, any
/// length, no `0x` prefix. Pushes a bee-shaped [`Reason`] on failure.
fn parse_go_hex(field: &str, value: &str, reasons: &mut Vec<Reason>) -> Option<Vec<u8>> {
    if let Some(bad) = value.chars().find(|c| !c.is_ascii_hexdigit()) {
        reasons.push(Reason::new(field, crate::error::go_invalid_hex_byte(bad)));
        return None;
    }
    if !value.len().is_multiple_of(2) {
        reasons.push(Reason::new(field, "odd length hex string"));
        return None;
    }
    hex::decode(value).ok().or_else(|| {
        reasons.push(Reason::new(field, "odd length hex string"));
        None
    })
}

/// Parse the `swarm-act-publisher` value (hex-encoded SEC1 public key)
/// with bee's error strings: Go hex errors first, then
/// `btcec.ParsePubKey`'s (`dcrd/secp256k1`) messages for wrong length /
/// unsupported format / off-curve points.
fn parse_publisher(field: &str, value: &str, reasons: &mut Vec<Reason>) -> Option<PublicKey> {
    let bytes = parse_go_hex(field, value, reasons)?;
    match bytes.len() {
        33 => {
            if bytes[0] != 0x02 && bytes[0] != 0x03 {
                reasons.push(Reason::new(
                    field,
                    format!("invalid public key: unsupported format: {:x}", bytes[0]),
                ));
                return None;
            }
        }
        65 => {
            if bytes[0] != 0x04 && bytes[0] != 0x06 && bytes[0] != 0x07 {
                reasons.push(Reason::new(
                    field,
                    format!("invalid public key: unsupported format: {:x}", bytes[0]),
                ));
                return None;
            }
        }
        n => {
            reasons.push(Reason::new(
                field,
                format!("malformed public key: invalid length: {n}"),
            ));
            return None;
        }
    }
    if let Ok(pk) = actc::parse_public_key(&bytes) {
        Some(pk)
    } else {
        // Format byte and length already validated, so the point is
        // not on the curve (or a coordinate overflows the field) —
        // bee surfaces dcrd's off-curve message. `%v` of a FieldVal
        // is minimal lowercase hex.
        let x_full = hex::encode(&bytes[1..33]);
        let x_trimmed = x_full.trim_start_matches('0');
        let x_hex = if x_trimmed.is_empty() { "0" } else { x_trimmed };
        reasons.push(Reason::new(
            field,
            format!("invalid public key: x coordinate {x_hex} is not on the secp256k1 curve"),
        ));
        None
    }
}

/// A non-empty, non-all-zero history address (bee treats
/// `swarm.ZeroAddress` — and an absent header — as "no history").
fn nonzero_history(bytes: Vec<u8>) -> Option<Vec<u8>> {
    if bytes.is_empty() || (bytes.len() == 32 && bytes.iter().all(|&b| b == 0)) {
        None
    } else {
        Some(bytes)
    }
}

/// Parsed ACT upload headers: `None` when `swarm-act` is absent/false.
pub(crate) struct ActUpload {
    /// `swarm-act-history-address`, when present and non-zero. Bee
    /// accepts any hex length here; a value that isn't a real history
    /// root fails later with the 500 upload shape, exactly like bee's
    /// loadsave failing on a bogus reference.
    pub history: Option<Vec<u8>>,
}

/// Parse the upload-side ACT headers (`swarm-act` +
/// `swarm-act-history-address`), bee `mapStructure` shapes on failure.
#[allow(clippy::result_large_err)]
pub(crate) fn parse_act_upload(headers: &HeaderMap) -> Result<Option<ActUpload>, Response> {
    let mut reasons = Vec::new();

    let mut act = false;
    if let Some(v) = headers.get(&SWARM_ACT) {
        let raw = v.to_str().unwrap_or_default();
        match go_parse_bool(raw) {
            Some(b) => act = b,
            None => reasons.push(Reason::new("Swarm-Act", "invalid syntax")),
        }
    }

    let mut history: Option<Vec<u8>> = None;
    if let Some(v) = headers.get(&SWARM_ACT_HISTORY_ADDRESS) {
        let raw = v.to_str().unwrap_or_default();
        if let Some(bytes) = parse_go_hex("Swarm-Act-History-Address", raw, &mut reasons) {
            history = nonzero_history(bytes);
        }
    }

    if !reasons.is_empty() {
        return Err(params_error(ParamKind::Header, reasons));
    }
    Ok(act.then_some(ActUpload { history }))
}

/// Parsed ACT download headers, engaged only when both the publisher
/// and the history address are present (bee's `actDecryptionHandler`
/// passes the request through otherwise).
pub(crate) struct ActDownload {
    publisher: PublicKey,
    history: Vec<u8>,
    timestamp: i64,
}

/// Parse the download-side ACT headers. Reasons collect in bee's
/// struct-field order (timestamp, publisher, history address).
#[allow(clippy::result_large_err)]
pub(crate) fn parse_act_download(headers: &HeaderMap) -> Result<Option<ActDownload>, Response> {
    let mut reasons = Vec::new();

    let mut timestamp: Option<i64> = None;
    if let Some(v) = headers.get(&SWARM_ACT_TIMESTAMP) {
        let raw = v.to_str().unwrap_or_default();
        match raw.parse::<i64>() {
            Ok(t) => timestamp = Some(t),
            Err(e) => reasons.push(Reason::new("Swarm-Act-Timestamp", strconv_error_text(&e))),
        }
    }

    let mut publisher: Option<PublicKey> = None;
    if let Some(v) = headers.get(&SWARM_ACT_PUBLISHER) {
        let raw = v.to_str().unwrap_or_default();
        publisher = parse_publisher("Swarm-Act-Publisher", raw, &mut reasons);
    }

    let mut history: Option<Vec<u8>> = None;
    if let Some(v) = headers.get(&SWARM_ACT_HISTORY_ADDRESS) {
        let raw = v.to_str().unwrap_or_default();
        if let Some(bytes) = parse_go_hex("Swarm-Act-History-Address", raw, &mut reasons) {
            // Bee's middleware checks presence of the header (a parsed
            // *swarm.Address pointer), not zero-ness — an explicit
            // all-zero history engages the middleware and fails the
            // history load downstream.
            history = Some(bytes);
        }
    }

    if !reasons.is_empty() {
        return Err(params_error(ParamKind::Header, reasons));
    }
    let (Some(publisher), Some(history)) = (publisher, history) else {
        return Ok(None);
    };
    Ok(Some(ActDownload {
        publisher,
        history,
        timestamp: timestamp.unwrap_or_else(unix_now),
    }))
}

// --- core resolution ----------------------------------------------------------

/// Load a history root reference as a 32-byte address; anything else
/// fails like bee's loadsave on a malformed reference (join error).
fn history_root(history: &[u8]) -> Result<[u8; 32], ActError> {
    history.try_into().map_err(|_| {
        ActError::MalformedKvs(format!("history reference is {} bytes", history.len()))
    })
}

/// Resolve the ACT epoch valid at `timestamp` and unwrap the access key
/// for `grantee` (identified by our session with `publisher_secret` —
/// symmetric, so this serves both the publisher-side upload reuse and
/// the grantee-side download).
async fn access_key_at(
    fetcher: &dyn ChunkFetcher,
    node_secret: &[u8; 32],
    counterparty: &PublicKey,
    history: &[u8],
    timestamp: i64,
) -> Result<[u8; 32], ActError> {
    let root = history_root(history)?;
    let entries = acts::history_entries(fetcher, root).await?;
    let entry = acts::history_lookup(&entries, timestamp)?;
    let kvs = ActKvs::load(fetcher, entry.reference).await?;
    unwrap_access_key(&kvs, node_secret, counterparty)
}

/// Bee's `getAccessKey`: look up the counterparty's sealed access key
/// in the kvs and unwrap it with the session's AK-decryption key.
fn unwrap_access_key(
    kvs: &ActKvs,
    node_secret: &[u8; 32],
    counterparty: &PublicKey,
) -> Result<[u8; 32], ActError> {
    let (lookup, akdk) = actc::lookup_and_ak_decryption_keys(node_secret, counterparty)
        .map_err(|e| ActError::MalformedKvs(e.to_string()))?;
    let sealed = kvs.get(&lookup).ok_or(ActError::NotFound)?;
    let sealed: [u8; 32] = sealed.as_slice().try_into().map_err(|_| {
        ActError::MalformedKvs(format!("sealed access key is {} bytes", sealed.len()))
    })?;
    Ok(actc::seal_access_key(&sealed, &akdk))
}

/// Bee's `AddGrantee`: seal the ACT access key for one grantee. When
/// the grantee IS the publisher, a fresh random access key is drawn
/// (bee draws a new one in that branch — mirrored verbatim, quirks and
/// all); otherwise the current key is read back from the store.
fn add_grantee(
    kvs: &mut ActKvs,
    node_secret: &[u8; 32],
    publisher: &PublicKey,
    grantee: &PublicKey,
) -> Result<(), ActError> {
    let access_key = if grantee == publisher {
        actc::random_access_key()
    } else {
        unwrap_access_key(kvs, node_secret, publisher)?
    };
    let (lookup, akdk) = actc::lookup_and_ak_decryption_keys(node_secret, grantee)
        .map_err(|e| ActError::MalformedKvs(e.to_string()))?;
    kvs.put(&lookup, &actc::seal_access_key(&access_key, &akdk));
    Ok(())
}

/// Resolve a download address through ACT when the request carries the
/// ACT headers; pass it through untouched otherwise. This is bee's
/// `actDecryptionHandler` middleware, applied by the `/bytes`,
/// `/chunks` and `/bzz` GET/HEAD handlers before dispatching.
#[allow(clippy::result_large_err)] // axum Response-as-Err, see lib.rs
pub(crate) async fn act_maybe_resolve(
    handle: &GatewayHandle,
    headers: &HeaderMap,
    address: Vec<u8>,
) -> Result<Vec<u8>, Response> {
    let Some(params) = parse_act_download(headers)? else {
        return Ok(address);
    };
    let timeout = request_timeout(headers, DEFAULT_REQUEST_TIMEOUT);
    let fetcher = CommandFetcher::new(handle, timeout);
    let access_key = access_key_at(
        &fetcher,
        &handle.act_secret,
        &params.publisher,
        &params.history,
        params.timestamp,
    )
    .await
    .map_err(|e| act_download_error(&e))?;
    Ok(actc::transform_reference(&address, &access_key))
}

fn act_download_error(e: &ActError) -> Response {
    debug!(target: "ant_gateway", error = %e, "access control download failed");
    match e {
        ActError::NotFound => json_error(StatusCode::NOT_FOUND, ERR_ACT_NOT_FOUND),
        ActError::InvalidTimestamp => json_error(StatusCode::BAD_REQUEST, "invalid timestamp"),
        _ => json_error(StatusCode::INTERNAL_SERVER_ERROR, ERR_ACT_DOWNLOAD),
    }
}

fn act_upload_error(e: &ActError) -> Response {
    debug!(target: "ant_gateway", error = %e, "access control upload failed");
    match e {
        ActError::NotFound => json_error(StatusCode::NOT_FOUND, ERR_ACT_NOT_FOUND),
        _ => json_error(StatusCode::INTERNAL_SERVER_ERROR, ERR_ACT_UPLOAD),
    }
}

fn act_grantee_error(e: &ActError) -> Response {
    debug!(target: "ant_gateway", error = %e, "grantee list update failed");
    match e {
        ActError::NotFound => json_error(StatusCode::NOT_FOUND, ERR_ACT_NOT_FOUND),
        _ => json_error(StatusCode::INTERNAL_SERVER_ERROR, ERR_ACT_GRANTEE_LIST),
    }
}

/// Outcome of the upload-side ACT wrap.
pub(crate) struct ActUploadOutcome {
    /// The (reference-)encrypted reference to return in the JSON body.
    pub reference: Vec<u8>,
    /// The history address for the `swarm-act-history-address`
    /// response header.
    pub history_hex: String,
}

/// Bee's `actEncryptionHandler`: wrap an uploaded content reference for
/// access control. Creates + stores a fresh history/kvs when the
/// request had no (or a zero) history address; reuses the epoch valid
/// now otherwise, storing nothing new.
#[allow(clippy::result_large_err)] // axum Response-as-Err, see lib.rs
pub(crate) async fn act_encrypt_upload(
    handle: &GatewayHandle,
    act: &ActUpload,
    reference: &[u8],
    batch_id: [u8; 32],
    timeout: Duration,
) -> Result<ActUploadOutcome, Response> {
    let publisher = publisher_key(handle)?;
    let fetcher = CommandFetcher::new(handle, timeout);

    if let Some(history) = &act.history {
        // Existing history: reuse the epoch valid now.
        let access_key = access_key_at(
            &fetcher,
            &handle.act_secret,
            &publisher,
            history,
            unix_now(),
        )
        .await
        .map_err(|e| act_upload_error(&e))?;
        return Ok(ActUploadOutcome {
            reference: actc::transform_reference(reference, &access_key),
            history_hex: hex::encode(history),
        });
    }

    // Fresh history: new access key, new kvs granting the publisher,
    // one epoch at "now". Both artifacts are stored (bee's
    // `putter.Done` on the kvs and history references).
    let mut kvs = ActKvs::new();
    add_grantee(&mut kvs, &handle.act_secret, &publisher, &publisher)
        .map_err(|e| act_upload_error(&e))?;
    let access_key = unwrap_access_key(&kvs, &handle.act_secret, &publisher)
        .map_err(|e| act_upload_error(&e))?;

    let act_split = split_bytes(&kvs.to_manifest_bytes());
    let history = acts::history_with_epoch(
        &[],
        acts::history_key(unix_now()),
        act_split.root,
        BTreeMap::new(),
    )
    .map_err(|e| act_upload_error(&e))?;
    let mut chunks = act_split.chunks;
    chunks.extend(history.chunks.clone());
    push_act_chunks(handle, &chunks, batch_id, timeout).await?;

    Ok(ActUploadOutcome {
        reference: actc::transform_reference(reference, &access_key),
        history_hex: hex::encode(history.root),
    })
}

/// Stamp the ACT response headers on an upload response: the history
/// address plus bee's `Access-Control-Expose-Headers` addition.
pub(crate) fn set_act_history_header(resp: &mut Response, history_hex: &str) {
    if let Ok(v) = HeaderValue::from_str(history_hex) {
        resp.headers_mut().insert(SWARM_ACT_HISTORY_ADDRESS, v);
        resp.headers_mut().append(
            ACCESS_CONTROL_EXPOSE_HEADERS,
            HeaderValue::from_static("Swarm-Act-History-Address"),
        );
    }
}

/// The node's public key — the ACT publisher.
#[allow(clippy::result_large_err)]
fn publisher_key(handle: &GatewayHandle) -> Result<PublicKey, Response> {
    actc::public_key_of(&handle.act_secret).map_err(|_| {
        json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "node key is not a valid secp256k1 secret",
        )
    })
}

/// Push ACT bookkeeping chunks (kvs manifests, history nodes, grantee
/// lists) through the normal pushsync path.
#[allow(clippy::result_large_err)] // axum Response-as-Err, see lib.rs
async fn push_act_chunks(
    handle: &GatewayHandle,
    chunks: &[SplitChunk],
    batch_id: [u8; 32],
    timeout: Duration,
) -> Result<(), Response> {
    let guard = handle
        .activity
        .begin(GatewayRequestKind::Bytes, "act-store".to_string());
    push_chunks(handle, chunks, batch_id, timeout, &guard).await
}

// --- grantee list handlers ------------------------------------------------------

#[derive(Deserialize, Default)]
struct GranteesPostRequest {
    #[serde(default)]
    grantees: Vec<String>,
}

#[derive(Deserialize, Default)]
struct GranteesPatchRequest {
    #[serde(default)]
    add: Vec<String>,
    #[serde(default)]
    revoke: Vec<String>,
}

/// Bee's `parseKeys`: hex-decode + `btcec.ParsePubKey` each entry.
fn parse_keys(list: &[String]) -> Result<Vec<PublicKey>, ()> {
    let mut out = Vec::with_capacity(list.len());
    for g in list {
        let bytes = hex::decode(g).map_err(|_| ())?;
        out.push(actc::parse_public_key(&bytes).map_err(|_| ())?);
    }
    Ok(out)
}

/// Bee's `GranteeList.Add`: append, filtering duplicates against both
/// the existing list and the batch itself.
fn grantee_list_add(grantees: &mut Vec<PublicKey>, add: &[PublicKey]) {
    for key in add {
        if !grantees.contains(key) {
            grantees.push(*key);
        }
    }
}

/// Bee's `GranteeList.Remove`: swap-remove with bee's exact index walk
/// (which does not re-check the swapped-in element — bug-compatible).
fn grantee_list_remove(grantees: &mut Vec<PublicKey>, remove: &[PublicKey]) {
    for r in remove {
        let mut i = 0;
        while i < grantees.len() {
            if grantees[i] == *r {
                let last = grantees.len() - 1;
                grantees.swap(i, last);
                grantees.truncate(last);
            }
            i += 1;
        }
    }
}

/// The publisher-only wrap key for grantee-list references: the
/// session key of the node with itself under nonce `[0x01]`
/// (bee `encryptRefForPublisher` / `decryptRefForPublisher`).
fn publisher_glref_key(
    handle: &GatewayHandle,
    publisher: &PublicKey,
) -> Result<[u8; 32], ActError> {
    let keys = actc::session_keys(&handle.act_secret, publisher, &[actc::NONCE_AK_DECRYPT])
        .map_err(|e| ActError::MalformedKvs(e.to_string()))?;
    Ok(keys[0])
}

/// `POST /grantee` — create a grantee list (and, without a
/// `swarm-act-history-address`, a fresh history). Bee's
/// `actCreateGranteesHandler`.
pub async fn grantee_create(
    State(handle): State<GatewayHandle>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    if body.is_empty() {
        // Bee: `r.Body == http.NoBody` → 400 `errInvalidRequest`.
        return json_error(StatusCode::BAD_REQUEST, "could not validate request");
    }
    let batch_id = match parse_postage_batch_header(&headers) {
        Ok(b) => b,
        Err(resp) => return resp,
    };
    let history = match parse_act_history_header(&headers, false) {
        Ok(h) => h,
        Err(resp) => return resp,
    };

    let req: GranteesPostRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        // Bee: json.Unmarshal failure → 500 "error unmarshaling request body".
        Err(_) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "error unmarshaling request body",
            )
        }
    };
    let Ok(add) = parse_keys(&req.grantees) else {
        return json_error(StatusCode::BAD_REQUEST, "invalid grantee list");
    };

    let timeout = request_timeout(&headers, DEFAULT_REQUEST_TIMEOUT);
    match update_grantees(&handle, batch_id, timeout, None, history, add, Vec::new()).await {
        Ok((egranteeref, historyref)) => json_response(
            StatusCode::CREATED,
            &serde_json::json!({
                "ref": hex::encode(egranteeref),
                "historyref": hex::encode(historyref),
            }),
        ),
        Err(resp) => resp,
    }
}

/// `PATCH /grantee/{address}` — add/revoke grantees. Bee's
/// `actGrantRevokeHandler`.
pub async fn grantee_patch(
    State(handle): State<GatewayHandle>,
    Path(address): Path<String>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    if body.is_empty() {
        return json_error(StatusCode::BAD_REQUEST, "could not validate request");
    }
    let glref = match parse_grantee_address(&address) {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    let batch_id = match parse_postage_batch_header(&headers) {
        Ok(b) => b,
        Err(resp) => return resp,
    };
    let history = match parse_act_history_header(&headers, true) {
        Ok(h) => h,
        Err(resp) => return resp,
    };

    let req: GranteesPatchRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(_) => {
            return json_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "error unmarshaling request body",
            )
        }
    };
    let Ok(add) = parse_keys(&req.add) else {
        return json_error(StatusCode::BAD_REQUEST, "invalid add list");
    };
    let Ok(revoke) = parse_keys(&req.revoke) else {
        return json_error(StatusCode::BAD_REQUEST, "invalid revoke list");
    };

    let timeout = request_timeout(&headers, DEFAULT_REQUEST_TIMEOUT);
    match update_grantees(
        &handle,
        batch_id,
        timeout,
        Some(glref),
        history,
        add,
        revoke,
    )
    .await
    {
        Ok((egranteeref, historyref)) => json_response(
            StatusCode::OK,
            &serde_json::json!({
                "ref": hex::encode(egranteeref),
                "historyref": hex::encode(historyref),
            }),
        ),
        Err(resp) => resp,
    }
}

/// `GET /grantee/{address}` — list grantees; publisher only. Bee's
/// `actListGranteesHandler`, whose every failure is a 404
/// `"granteelist not found"`.
pub async fn grantee_get(
    State(handle): State<GatewayHandle>,
    Path(address): Path<String>,
    headers: HeaderMap,
) -> Response {
    let glref = match parse_grantee_address(&address) {
        Ok(a) => a,
        Err(resp) => return resp,
    };
    let timeout = request_timeout(&headers, DEFAULT_REQUEST_TIMEOUT);
    let fetcher = CommandFetcher::new(&handle, timeout);
    let publisher = match publisher_key(&handle) {
        Ok(p) => p,
        Err(resp) => return resp,
    };

    let grantees = async {
        let key = publisher_glref_key(&handle, &publisher)?;
        let grantee_ref = actc::transform_reference(&glref, &key);
        let data = load_grantee_list(&fetcher, &grantee_ref).await?;
        Ok::<_, ActError>(acts::deserialize_grantees(&data))
    }
    .await;

    match grantees {
        Ok(list) => {
            let hex_list: Vec<String> = list
                .iter()
                .map(|pk| hex::encode(actc::compress_public_key(pk)))
                .collect();
            json_response(StatusCode::OK, &hex_list)
        }
        Err(e) => {
            debug!(target: "ant_gateway", error = %e, "could not get grantees");
            json_error(StatusCode::NOT_FOUND, "granteelist not found")
        }
    }
}

/// Load a grantee list by its decrypted reference: 64-byte references
/// join through the decrypting joiner (grantee lists are stored
/// encrypted), 32-byte ones plainly — mirroring bee's loadsave, which
/// keys the behavior off the reference width.
async fn load_grantee_list(
    fetcher: &dyn ChunkFetcher,
    reference: &[u8],
) -> Result<Vec<u8>, ActError> {
    match reference.len() {
        64 => {
            let r: [u8; 64] = reference.try_into().expect("length checked");
            acts::load_encrypted(fetcher, r).await
        }
        32 => {
            let r: [u8; 32] = reference.try_into().expect("length checked");
            acts::load_plain(fetcher, r).await
        }
        n => Err(ActError::MalformedKvs(format!(
            "grantee list reference is {n} bytes"
        ))),
    }
}

/// Parse the `/grantee/{address}` path segment: bee's `mapStructure`
/// path parse (`swarm.Address`, any-length hex).
#[allow(clippy::result_large_err)]
fn parse_grantee_address(address: &str) -> Result<Vec<u8>, Response> {
    let mut reasons = Vec::new();
    match parse_go_hex("address", address, &mut reasons) {
        Some(bytes) if !bytes.is_empty() => Ok(bytes),
        Some(_) => Err(params_error(
            ParamKind::Path,
            vec![Reason::required("address")],
        )),
        None => Err(params_error(ParamKind::Path, reasons)),
    }
}

/// Parse `swarm-act-history-address` for the grantee endpoints. On
/// PATCH bee marks it `validate:"required"` (validation errors use the
/// lowercased field name).
#[allow(clippy::result_large_err)]
fn parse_act_history_header(
    headers: &HeaderMap,
    required: bool,
) -> Result<Option<Vec<u8>>, Response> {
    let raw = headers
        .get(&SWARM_ACT_HISTORY_ADDRESS)
        .map(|v| v.to_str().unwrap_or_default());
    let Some(raw) = raw else {
        if required {
            return Err(params_error(
                ParamKind::Header,
                vec![Reason::required("swarm-act-history-address")],
            ));
        }
        return Ok(None);
    };
    let mut reasons = Vec::new();
    match parse_go_hex("Swarm-Act-History-Address", raw, &mut reasons) {
        Some(bytes) => Ok(nonzero_history(bytes)),
        None => Err(params_error(ParamKind::Header, reasons)),
    }
}

/// Bee's `UpdateHandler`, verbatim semantics — see the module docs for
/// the epoch/access-key rules. Returns `(egranteeref, historyref)`.
#[allow(clippy::result_large_err)] // axum Response-as-Err, see lib.rs
async fn update_grantees(
    handle: &GatewayHandle,
    batch_id: [u8; 32],
    timeout: Duration,
    glref: Option<Vec<u8>>,
    history: Option<Vec<u8>>,
    add: Vec<PublicKey>,
    revoke: Vec<PublicKey>,
) -> Result<(Vec<u8>, [u8; 32]), Response> {
    let publisher = publisher_key(handle)?;
    let fetcher = CommandFetcher::new(handle, timeout);
    let now = unix_now();

    // 1. History + ACT store (fresh when no history was supplied).
    let (history_entries, mut kvs, history_existed) = match &history {
        None => {
            let mut kvs = ActKvs::new();
            add_grantee(&mut kvs, &handle.act_secret, &publisher, &publisher)
                .map_err(|e| act_grantee_error(&e))?;
            (Vec::new(), kvs, false)
        }
        Some(history) => {
            let root = history_root(history).map_err(|e| act_grantee_error(&e))?;
            let entries = acts::history_entries(&fetcher, root)
                .await
                .map_err(|e| act_grantee_error(&e))?;
            let entry = acts::history_lookup(&entries, now).map_err(|e| act_grantee_error(&e))?;
            let kvs = ActKvs::load(&fetcher, entry.reference)
                .await
                .map_err(|e| act_grantee_error(&e))?;
            (entries, kvs, true)
        }
    };

    // 2. Grantee list (empty when creating).
    let mut grantees: Vec<PublicKey> = match &glref {
        None => Vec::new(),
        Some(glref) => {
            let key = publisher_glref_key(handle, &publisher).map_err(|e| act_grantee_error(&e))?;
            let grantee_ref = actc::transform_reference(glref, &key);
            let data = load_grantee_list(&fetcher, &grantee_ref)
                .await
                .map_err(|e| act_grantee_error(&e))?;
            acts::deserialize_grantees(&data)
        }
    };

    // 3. Adds first, then revokes; a revoke on an existing history
    //    rotates the access key (fresh kvs re-granting everyone).
    if !add.is_empty() {
        grantee_list_add(&mut grantees, &add);
    }
    let mut grantees_to_add = add;
    if !revoke.is_empty() {
        if grantees.is_empty() {
            // Bee: `ErrNoGranteeFound` → 400 "remove from empty grantee list".
            return Err(json_error(
                StatusCode::BAD_REQUEST,
                "remove from empty grantee list",
            ));
        }
        grantee_list_remove(&mut grantees, &revoke);
        if history_existed {
            let mut fresh = ActKvs::new();
            add_grantee(&mut fresh, &handle.act_secret, &publisher, &publisher)
                .map_err(|e| act_grantee_error(&e))?;
            kvs = fresh;
        }
        grantees_to_add = grantees.clone();
    }

    for grantee in &grantees_to_add {
        add_grantee(&mut kvs, &handle.act_secret, &publisher, grantee)
            .map_err(|e| act_grantee_error(&e))?;
    }

    // 4. Store the grantee list (encrypted pipeline) and wrap its
    //    reference for the publisher.
    let gl_split = split_bytes_encrypted(&acts::serialize_grantees(&grantees), 0);
    let key = publisher_glref_key(handle, &publisher).map_err(|e| act_grantee_error(&e))?;
    let egranteeref = actc::transform_reference(&gl_split.root_ref, &key);

    // 5. Store the ACT kvs. Bee's `kvs.Save` fails with
    //    `ErrNothingToSave` when nothing was put (e.g. an empty PATCH
    //    against an existing history) → 500 grantee-list shape.
    if kvs.nothing_to_save() {
        return Err(json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            ERR_ACT_GRANTEE_LIST,
        ));
    }
    let act_split = split_bytes(&kvs.to_manifest_bytes());

    // 6. Append the history epoch carrying the encrypted grantee-list
    //    reference. A same-second duplicate fails exactly like bee.
    let mut metadata = BTreeMap::new();
    metadata.insert(
        acts::HISTORY_GRANTEE_LIST_KEY.to_string(),
        hex::encode(&egranteeref),
    );
    let history_manifest = acts::history_with_epoch(
        &history_entries,
        acts::history_key(now),
        act_split.root,
        metadata,
    )
    .map_err(|e| act_grantee_error(&e))?;

    let mut chunks = gl_split.chunks;
    chunks.extend(act_split.chunks);
    chunks.extend(history_manifest.chunks.clone());
    push_act_chunks(handle, &chunks, batch_id, timeout).await?;

    Ok((egranteeref, history_manifest.root))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// ACT histories, key-value stores and grantee lists are expected to
    /// exist, so their reads must not ask for the fast-miss fan-out
    /// (issue #146), which would mostly buy duplicate, separately-charged
    /// deliveries.
    #[tokio::test]
    async fn act_reads_do_not_ask_for_fast_miss() {
        let (tx, mut rx) = mpsc::channel(1);
        let fetcher = CommandFetcher {
            commands: tx,
            timeout: Duration::from_secs(5),
        };
        let node = tokio::spawn(async move {
            match rx.recv().await {
                Some(ControlCommand::GetChunkRaw {
                    reference,
                    fast_miss,
                    ack,
                }) => {
                    assert_eq!(reference, [7u8; 32]);
                    assert!(!fast_miss, "ACT reads must not use fast-miss mode");
                    let _ = ack.send(ControlAck::Bytes { data: vec![1, 2] });
                }
                other => panic!("expected GetChunkRaw, got {other:?}"),
            }
        });
        let data = fetcher.fetch([7u8; 32]).await.expect("served");
        assert_eq!(data, vec![1, 2]);
        node.await.expect("dispatcher ran");
    }
}
