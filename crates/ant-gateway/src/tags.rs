//! Upload-tag registry — bee-shaped `/tags` endpoints.
//!
//! Bee assigns every upload a *tag* (an upload "session"): a small
//! counter object tracking how many chunks of the upload have been
//! split, seen, stored, sent, and synced. bee-js surfaces it as
//! `UploadResult.tagUid` (read from the `Swarm-Tag` response header)
//! and polls `GET /tags/{uid}` to drive an upload-progress UI
//! (PLAN.md J.2.4). Freedom's `getUploadStatus` reads `split / seen /
//! stored / sent / synced / uid` and renders the progress bar from
//! `sent / split`.
//!
//! `antd`'s upload path is **synchronous**: `POST /bzz` / `POST /bytes`
//! pushsync every chunk to the network before sending the HTTP
//! response. By the time bee-js polls the tag, the upload is already
//! complete. So a tag is created in the "all done" state the moment the
//! upload finishes — `split == seen == stored == sent == synced`. A
//! freshly created tag (via `POST /tags`, before any upload is
//! attached) starts at zero, matching bee.
//!
//! Endpoint shapes mirror `bee/pkg/api/tag.go`:
//! `POST /tags` → 201 tag, `GET /tags` → `{"tags":[...]}`,
//! `GET /tags/{id}` → 200 tag or 404 `"tag not present"`,
//! `DELETE /tags/{id}` → 204, `PATCH /tags/{id}` (done-split) → 200
//! `{"code":200,"message":"ok"}` or 404 `"tag not present"`.

use std::collections::BTreeMap;
use std::sync::Mutex;

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::error::{json_error, params_error, parse_hex_param, ParamKind, Reason};
use crate::handle::GatewayHandle;
use crate::status::format_rfc3339_now;

/// One upload tag. Counters are cumulative chunk counts, matching bee's
/// `storer.SessionInfo`. `address` is the upload's root reference once
/// known.
#[derive(Debug, Clone, Default)]
struct Tag {
    uid: u32,
    split: u64,
    seen: u64,
    stored: u64,
    sent: u64,
    synced: u64,
    address: Option<[u8; 32]>,
    started_at: String,
}

/// bee-shaped tag JSON body. Field names, casing, and the field *set*
/// match bee's `api.tagResponse` exactly (no extra fields — bee has no
/// `total`) so bee-js decodes it without a shim.
#[derive(Debug, Serialize)]
pub struct TagView {
    pub uid: u32,
    pub split: u64,
    pub seen: u64,
    pub stored: u64,
    pub sent: u64,
    pub synced: u64,
    /// Root reference as bare lowercase hex, or empty before it's
    /// known. Bee emits an empty string for a not-yet-uploaded tag.
    pub address: String,
    #[serde(rename = "startedAt")]
    pub started_at: String,
}

impl Tag {
    fn view(&self) -> TagView {
        TagView {
            uid: self.uid,
            split: self.split,
            seen: self.seen,
            stored: self.stored,
            sent: self.sent,
            synced: self.synced,
            address: self.address.map(hex::encode).unwrap_or_default(),
            started_at: self.started_at.clone(),
        }
    }
}

/// Thread-safe in-process tag store. Cheap: a `Mutex<BTreeMap>` only
/// touched on upload start/finish and tag polls, neither of which is
/// hot. Uids are monotonic per process start (bee restarts also reset
/// uids, so no client depends on cross-restart stability); the ordered
/// map gives `GET /tags` a stable creation-order listing.
#[derive(Debug, Default)]
pub struct TagRegistry {
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    next_uid: u32,
    tags: BTreeMap<u32, Tag>,
}

impl TagRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                // Bee uids start at 1; 0 is "no tag".
                next_uid: 1,
                tags: BTreeMap::new(),
            }),
        }
    }

    /// Create an empty tag and return its uid. Mirrors `POST /tags`.
    pub fn create(&self) -> u32 {
        let mut inner = self.inner.lock().expect("tag registry poisoned");
        let uid = inner.next_uid.max(1);
        inner.next_uid = uid.wrapping_add(1).max(1);
        inner.tags.insert(
            uid,
            Tag {
                uid,
                started_at: format_rfc3339_now(),
                ..Tag::default()
            },
        );
        uid
    }

    /// Create a tag already marked fully synced for a synchronous
    /// upload of `total` chunks rooted at `address`. Returns the new
    /// uid for the `Swarm-Tag` response header.
    pub fn create_completed(&self, total: u64, address: [u8; 32]) -> u32 {
        let uid = self.create();
        self.complete(uid, total, address);
        uid
    }

    /// Mark an existing tag as fully synced. No-op if the uid is
    /// unknown (the client may have uploaded without first creating a
    /// tag, in which case `create_completed` is used instead).
    pub fn complete(&self, uid: u32, total: u64, address: [u8; 32]) {
        let mut inner = self.inner.lock().expect("tag registry poisoned");
        if let Some(tag) = inner.tags.get_mut(&uid) {
            tag.split = total;
            tag.seen = total;
            tag.stored = total;
            tag.sent = total;
            tag.synced = total;
            tag.address = Some(address);
        }
    }

    /// Add `n` fully-processed chunks to an existing tag's cumulative
    /// counters without touching its address. Used by the
    /// `/chunks/stream` WebSocket upload, where chunks arrive one at a
    /// time and each is pushsynced synchronously before the ack, so
    /// split/stored/sent/synced advance in lock-step (bee's deferred
    /// pipeline splits these; ant's synchronous path doesn't). No-op
    /// for an unknown uid.
    pub fn add_synced(&self, uid: u32, n: u64) {
        let mut inner = self.inner.lock().expect("tag registry poisoned");
        if let Some(tag) = inner.tags.get_mut(&uid) {
            tag.split += n;
            tag.stored += n;
            tag.sent += n;
            tag.synced += n;
        }
    }

    /// Snapshot a tag for `GET /tags/{uid}`.
    pub fn get(&self, uid: u32) -> Option<TagView> {
        let inner = self.inner.lock().expect("tag registry poisoned");
        inner.tags.get(&uid).map(Tag::view)
    }

    /// Remove a tag; `true` if it existed. Mirrors `DELETE /tags/{id}`.
    pub fn delete(&self, uid: u32) -> bool {
        let mut inner = self.inner.lock().expect("tag registry poisoned");
        inner.tags.remove(&uid).is_some()
    }

    /// Record the "done split" address on an existing tag (bee's
    /// `PATCH /tags/{id}`); `false` when the uid is unknown.
    pub fn set_address(&self, uid: u32, address: Option<[u8; 32]>) -> bool {
        let mut inner = self.inner.lock().expect("tag registry poisoned");
        match inner.tags.get_mut(&uid) {
            Some(tag) => {
                if address.is_some() {
                    tag.address = address;
                }
                true
            }
            None => false,
        }
    }

    /// Snapshot a page of tags in creation order for `GET /tags`.
    pub fn list(&self, offset: usize, limit: usize) -> Vec<TagView> {
        let inner = self.inner.lock().expect("tag registry poisoned");
        inner
            .tags
            .values()
            .skip(offset)
            .take(limit)
            .map(Tag::view)
            .collect()
    }
}

/// Bee's tag responses carry a private no-cache directive.
fn tag_cache_control(resp: &mut Response) {
    let _ = resp.headers_mut().insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-cache, private, max-age=0"),
    );
}

/// Parse the `{uid}` path segment bee-shaped: a non-numeric value is a
/// `400 invalid path params` with a reason on field `id` (bee's map tag
/// for the tag id).
#[allow(clippy::result_large_err)]
fn parse_uid(raw: &str) -> Result<u32, Response> {
    raw.parse::<u32>().map_err(|e| {
        params_error(
            ParamKind::Path,
            vec![Reason::new("id", format!("invalid syntax: {e}"))],
        )
    })
}

/// `POST /tags`. Creates a fresh (empty) tag and returns it bee-shaped
/// with status 201. bee-js calls this before a tracked upload; the
/// upload then references the returned uid via the `Swarm-Tag` header.
pub async fn create_tag(State(handle): State<GatewayHandle>) -> Response {
    let uid = handle.tags.create();
    match handle.tags.get(uid) {
        Some(view) => {
            let mut resp = (StatusCode::CREATED, Json(view)).into_response();
            tag_cache_control(&mut resp);
            resp
        }
        // Unreachable: we just created it. Defensive only.
        None => json_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "tag vanished after create",
        ),
    }
}

/// `GET /tags/{uid}`. Returns the tag's counters bee-shaped, or bee's
/// `404 "tag not present"` for an unknown uid.
pub async fn get_tag(State(handle): State<GatewayHandle>, Path(uid): Path<String>) -> Response {
    let uid = match parse_uid(&uid) {
        Ok(uid) => uid,
        Err(resp) => return resp,
    };
    match handle.tags.get(uid) {
        Some(view) => {
            let mut resp = Json(view).into_response();
            tag_cache_control(&mut resp);
            resp
        }
        None => json_error(StatusCode::NOT_FOUND, "tag not present"),
    }
}

/// `?offset=` / `?limit=` for `GET /tags`. Bee defaults the limit to
/// 100 (`listTagsHandler` in `bee/pkg/api/tag.go`).
#[derive(Debug, Default, Deserialize)]
pub struct ListTagsQuery {
    #[serde(default)]
    offset: Option<usize>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Serialize)]
struct ListTagsBody {
    tags: Vec<TagView>,
}

/// `GET /tags` — list tags bee-shaped: `{"tags":[...]}`.
pub async fn list_tags(
    State(handle): State<GatewayHandle>,
    Query(query): Query<ListTagsQuery>,
) -> Response {
    let tags = handle
        .tags
        .list(query.offset.unwrap_or(0), query.limit.unwrap_or(100));
    Json(ListTagsBody { tags }).into_response()
}

/// `DELETE /tags/{uid}` — drop a tag: bee answers `204 No Content`
/// (with its JSON content type — `jsonhttp.NoContent` sets the header
/// before suppressing the body) or `404 "tag not present"`.
pub async fn delete_tag(State(handle): State<GatewayHandle>, Path(uid): Path<String>) -> Response {
    let uid = match parse_uid(&uid) {
        Ok(uid) => uid,
        Err(resp) => return resp,
    };
    if !handle.tags.delete(uid) {
        return json_error(StatusCode::NOT_FOUND, "tag not present");
    }
    let mut resp = Response::new(Body::empty());
    *resp.status_mut() = StatusCode::NO_CONTENT;
    let _ = resp
        .headers_mut()
        .insert(header::CONTENT_TYPE, crate::error::JSON_CONTENT_TYPE);
    resp
}

/// Optional `{"address": "<hex>"}` body on `PATCH /tags/{uid}` (bee's
/// `tagRequest`).
#[derive(Debug, Default, Deserialize)]
pub struct PatchTagBody {
    #[serde(default)]
    address: Option<String>,
}

/// `PATCH /tags/{uid}` — bee's "done split" handler: records the
/// upload's root address on the session and answers
/// `200 {"code":200,"message":"ok"}`, or `404 "tag not present"` for an
/// unknown uid. antd's uploads are synchronous so there is no deferred
/// split to finalize; recording the address keeps `GET /tags/{uid}`
/// consistent for clients that drive the bee protocol to the letter.
pub async fn patch_tag(
    State(handle): State<GatewayHandle>,
    Path(uid): Path<String>,
    body: axum::body::Bytes,
) -> Response {
    let uid = match parse_uid(&uid) {
        Ok(uid) => uid,
        Err(resp) => return resp,
    };
    let parsed: PatchTagBody = if body.is_empty() {
        PatchTagBody::default()
    } else {
        match serde_json::from_slice(&body) {
            Ok(p) => p,
            // Bee's doneSplitHandler answers 500 "error unmarshaling
            // metadata" on a malformed body.
            Err(_) => {
                return json_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "error unmarshaling metadata",
                );
            }
        }
    };
    let address = match parsed.address.as_deref().filter(|s| !s.is_empty()) {
        Some(raw) => {
            let mut reasons = Vec::new();
            match parse_hex_param::<32>("address", raw, &mut reasons) {
                Some(addr) => Some(addr),
                None => {
                    return json_error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "error unmarshaling metadata",
                    );
                }
            }
        }
        None => None,
    };
    if !handle.tags.set_address(uid, address) {
        return json_error(StatusCode::NOT_FOUND, "tag not present");
    }
    // Bee: `jsonhttp.OK(w, "ok")` → `{"code":200,"message":"ok"}`.
    json_error(StatusCode::OK, "ok")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_then_get_roundtrips() {
        let reg = TagRegistry::new();
        let uid = reg.create();
        assert_eq!(uid, 1);
        let view = reg.get(uid).expect("tag exists");
        assert_eq!(view.uid, 1);
        assert_eq!(view.split, 0);
        assert_ne!(view.started_at, "");
    }

    #[test]
    fn completed_tag_reports_full_progress() {
        let reg = TagRegistry::new();
        let addr = [7u8; 32];
        let uid = reg.create_completed(42, addr);
        let view = reg.get(uid).expect("tag exists");
        assert_eq!(view.split, 42);
        assert_eq!(view.synced, 42);
        assert_eq!(view.sent, 42);
        assert_eq!(view.address, hex::encode(addr));
    }

    #[test]
    fn unknown_uid_is_none() {
        let reg = TagRegistry::new();
        assert!(reg.get(999).is_none());
    }

    #[test]
    fn view_json_matches_bee_field_set() {
        let reg = TagRegistry::new();
        let uid = reg.create();
        let view = reg.get(uid).unwrap();
        let json = serde_json::to_value(&view).unwrap();
        let keys: Vec<&str> = json
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        let mut expected = vec![
            "uid",
            "split",
            "seen",
            "stored",
            "sent",
            "synced",
            "address",
            "startedAt",
        ];
        let mut got = keys.clone();
        got.sort_unstable();
        expected.sort_unstable();
        assert_eq!(
            got, expected,
            "tag JSON must match bee's field set (no `total`)"
        );
    }

    #[test]
    fn list_pages_in_creation_order() {
        let reg = TagRegistry::new();
        for _ in 0..5 {
            reg.create();
        }
        let all = reg.list(0, 100);
        assert_eq!(all.len(), 5);
        assert!(all.windows(2).all(|w| w[0].uid < w[1].uid));
        let page = reg.list(1, 2);
        assert_eq!(page.len(), 2);
        assert_eq!(page[0].uid, 2);
    }

    #[test]
    fn delete_then_set_address_reports_missing() {
        let reg = TagRegistry::new();
        let uid = reg.create();
        assert!(reg.delete(uid));
        assert!(!reg.delete(uid));
        assert!(!reg.set_address(uid, Some([1u8; 32])));
    }
}
