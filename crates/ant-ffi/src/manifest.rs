//! Synchronous mantaray manifest listing for the iOS player.
//!
//! Wraps [`ControlCommand::ListBzz`] and serialises the resulting
//! `Vec<ManifestEntryInfo>` into a JSON string the Swift side can
//! decode directly. Returning JSON (rather than a Swift-shaped
//! `[ManifestEntry]` POD struct) keeps the FFI surface flat: we'd
//! otherwise need to ship a length-prefixed array of variable-size
//! structs and hand-roll memory ownership for every field.

use ant_control::{ControlAck, ControlCommand, ManifestEntryInfo};
use serde::Serialize;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::mpsc;

/// Bound on how long we'll wait for a manifest listing. The mantaray
/// walk fans out aggressively across the chunk cache so even a deep
/// directory tree resolves in well under 30 s once peers are warm.
const LIST_TIMEOUT: Duration = Duration::from_mins(1);
const NO_PEERS_RETRY_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Debug, thiserror::Error)]
pub(crate) enum ListError {
    #[error("{0}")]
    Stream(String),
}

#[derive(Serialize)]
struct ManifestEntryWire<'a> {
    path: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    reference: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    content_type: Option<&'a str>,
}

#[derive(Serialize)]
struct ManifestWire<'a> {
    entries: Vec<ManifestEntryWire<'a>>,
}

pub(crate) fn list_to_json(
    runtime: &Handle,
    cmd_tx: &mpsc::Sender<ControlCommand>,
    reference: [u8; 32],
) -> Result<String, ListError> {
    let cmd_tx = cmd_tx.clone();
    let entries: Vec<ManifestEntryInfo> = runtime.block_on(async move {
        let deadline = tokio::time::Instant::now() + LIST_TIMEOUT;
        loop {
            let (ack_tx, mut ack_rx) = mpsc::channel::<ControlAck>(8);
            let cmd = ControlCommand::ListBzz {
                reference,
                bypass_cache: false,
                // The iOS player's listing: whole manifest with sizes.
                page: None,
                ack: ack_tx,
            };
            cmd_tx
                .send(cmd)
                .await
                .map_err(|_| ListError::Stream("node loop is not accepting commands".into()))?;
            match tokio::time::timeout_at(deadline, ack_rx.recv()).await {
                Ok(Some(ControlAck::Manifest { entries })) => return Ok(entries),
                Ok(Some(ControlAck::Error { message })) => {
                    if message.contains("no peers available") {
                        let now = tokio::time::Instant::now();
                        if now >= deadline {
                            return Err(ListError::Stream(format!("list timed out: {message}")));
                        }
                        let slice =
                            NO_PEERS_RETRY_INTERVAL.min(deadline.saturating_duration_since(now));
                        tokio::time::sleep(slice).await;
                        continue;
                    }
                    return Err(ListError::Stream(message));
                }
                Ok(Some(_)) => {
                    return Err(ListError::Stream(
                        "unexpected ack while listing manifest".into(),
                    ))
                }
                Ok(None) => {
                    return Err(ListError::Stream(
                        "node dropped the ack channel without a manifest".into(),
                    ))
                }
                Err(_) => {
                    return Err(ListError::Stream(format!(
                        "list timed out after {} s",
                        LIST_TIMEOUT.as_secs()
                    )))
                }
            }
        }
    })?;

    let wire = ManifestWire {
        entries: entries
            .iter()
            .map(|e| ManifestEntryWire {
                path: &e.path,
                reference: e.reference.as_deref(),
                size: e.size,
                content_type: e.metadata.get("Content-Type").map(String::as_str),
            })
            .collect(),
    };
    serde_json::to_string(&wire).map_err(|e| ListError::Stream(format!("encode manifest: {e}")))
}
