//! Random-access streaming session for the iOS player.
//!
//! Backs the Swift `AVAssetResourceLoaderDelegate` path: the player asks
//! for arbitrary byte ranges out of a `bzz://<root>/<path>` file and
//! this module satisfies each request by issuing a fresh
//! [`ControlCommand::StreamBzz`] with the requested range. The first
//! call (`open`) is `head_only=true` so we learn `total_bytes` and
//! `Content-Type` without paying for any body chunks; later calls
//! (`read`) request a contiguous body slice.
//!
//! Every range request rebuilds the full `RoutingFetcher` inside the
//! node loop, but the manifest walk and the data-root chunk land in
//! the in-memory + on-disk chunk cache after the first hit, so back-to-back
//! reads from the same track see only the new chunks they need. That
//! plays well with `AVPlayer`'s prefetch / seek pattern: it issues a
//! tiny header sniff first (few KB), then a series of contiguous
//! ~256 KiB reads driven by the audio buffer's high-water mark.
//!
//! Progress accounting lives on the [`AntStream`] so the Network tab
//! can render a per-track progress bar. The node-wide
//! [`crate::AntProgress`] (peers used, in-flight chunks, cache hits)
//! is updated by the same `Progress` acks that drive the global
//! `ant_download_progress` slot — the stream just keeps a separate
//! cumulative `bytes_downloaded` counter on top of those.

use ant_control::{ControlAck, ControlCommand, StreamRange};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use tokio::sync::mpsc;

/// Per-`ant_stream_read` deadline. The body of one range request is
/// usually a handful of chunks (≤ 1 MiB), which lands in 1–3 s on a
/// warm peer set, but the first read on a cold cache also pays the
/// manifest walk. 1 minute is well above the worst case we've observed
/// from the existing smoke test and well below `AVPlayer`'s ~3 minute
/// buffering timeout, so the player retries before iOS gives up.
const READ_TIMEOUT: Duration = Duration::from_mins(1);

/// Shared with `lib.rs::download_inner` — same retry envelope, same
/// "wait for BZZ handshakes to complete on cold start" behaviour. We
/// could refactor this constant out to a single home but the duplication
/// keeps `stream.rs` self-contained.
const NO_PEERS_RETRY_INTERVAL: Duration = Duration::from_secs(2);

/// Same per-request joiner ceiling as the bulk download path uses. The
/// streaming path doesn't actually need it (range reads bound the body
/// implicitly), but `StreamBzz` carries it through so the node can
/// short-circuit pathological span declarations.
const MAX_STREAM_BYTES: u64 = 1024 * 1024 * 1024;

/// Opaque to the FFI: the Swift side carries a `*mut AntStream` around
/// and never inspects its fields.
pub struct AntStream {
    runtime: Handle,
    cmd_tx: mpsc::Sender<ControlCommand>,
    reference: [u8; 32],
    path: String,
    /// File body size, in bytes. Learned from the `head_only` open
    /// request and trusted thereafter; the node clamps each range read
    /// against this anyway.
    total_bytes: u64,
    /// MIME type the manifest reported, or `None` if the manifest had
    /// no `Content-Type` metadata. Surfaced to Swift so `AVPlayer`
    /// can pick a reader without sniffing the body.
    content_type: Option<String>,
    /// Filename derived from manifest metadata (or the path tail). Not
    /// surfaced through the FFI yet but kept here so future tracks-list
    /// UI can reuse it without a second manifest walk.
    #[allow(dead_code)]
    filename: Option<String>,
    progress: Mutex<StreamProgress>,
    bytes_downloaded: AtomicU64,
    started_at: Instant,
}

/// Latest snapshot of the in-flight `StreamBzz`'s `Progress` ack.
/// All fields are "the most recent value the node reported"; the
/// implicit `last_` prefix used to be spelled out on every field
/// but tripped clippy's `struct_field_names` lint and didn't add
/// any signal beyond what the struct's name already conveys.
#[derive(Clone, Copy, Default)]
pub(crate) struct StreamProgress {
    chunks_done: u64,
    total_chunks: u64,
    peers_used: u32,
    in_flight: u32,
    cache_hits: u64,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum StreamError {
    #[error("{0}")]
    Stream(String),
}

impl AntStream {
    /// Issue `StreamBzz { head_only: true }` and capture the resulting
    /// `BzzStreamStart` to learn `total_bytes` + `content_type`. The
    /// returned `AntStream` borrows the handle's mpsc sender + runtime
    /// handle but does *not* keep the handle alive; closing the handle
    /// while a stream is open will fail subsequent reads with an
    /// "ack channel closed" error rather than crash.
    pub(crate) fn open(
        runtime: Handle,
        cmd_tx: mpsc::Sender<ControlCommand>,
        reference: [u8; 32],
        path: String,
    ) -> Result<Self, StreamError> {
        let cmd_tx_for_head = cmd_tx.clone();
        let path_for_head = path.clone();
        let (total_bytes, content_type, filename) = runtime.block_on(async move {
            let deadline = tokio::time::Instant::now() + READ_TIMEOUT;
            head_request(&cmd_tx_for_head, reference, path_for_head, deadline).await
        })?;
        Ok(Self {
            runtime,
            cmd_tx,
            reference,
            path,
            total_bytes,
            content_type,
            filename,
            progress: Mutex::new(StreamProgress::default()),
            bytes_downloaded: AtomicU64::new(0),
            started_at: Instant::now(),
        })
    }

    pub(crate) const fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    pub(crate) fn content_type(&self) -> Option<&str> {
        self.content_type.as_deref()
    }

    pub(crate) fn bytes_downloaded(&self) -> u64 {
        self.bytes_downloaded.load(Ordering::Relaxed)
    }

    pub(crate) fn elapsed_ms(&self) -> u64 {
        u64::try_from(self.started_at.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    pub(crate) fn last_progress(&self) -> StreamProgress {
        self.progress
            .lock()
            .map_or_else(|poisoned| *poisoned.into_inner(), |guard| *guard)
    }

    /// Fetch `len` bytes starting at `offset`, blocking the calling
    /// thread until the request completes. The Swift caller drives this
    /// from a dedicated `DispatchQueue` on the resource loader path so
    /// blocking is fine.
    ///
    /// Returns the number of bytes copied into `dst` (which is `≤ len`
    /// only when `offset + len > total_bytes`, in which case the
    /// trailing slice is empty and we return whatever bytes the joiner
    /// produced before reaching EOF).
    pub(crate) fn read(
        &self,
        offset: u64,
        len: usize,
        dst: &mut [u8],
    ) -> Result<usize, StreamError> {
        if len == 0 || offset >= self.total_bytes {
            return Ok(0);
        }
        let bound_len = u64::try_from(len)
            .ok()
            .and_then(|len| len.checked_add(offset))
            .map(|end| end.min(self.total_bytes) - offset)
            .map_or(len, |n| usize::try_from(n).unwrap_or(len));
        let dst = &mut dst[..bound_len];
        let end_inclusive = offset.saturating_add(bound_len as u64).saturating_sub(1);
        let cmd_tx = self.cmd_tx.clone();
        let reference = self.reference;
        let path = self.path.clone();
        let progress_slot = &self.progress;
        let bytes_downloaded = &self.bytes_downloaded;
        self.runtime.block_on(async move {
            let deadline = tokio::time::Instant::now() + READ_TIMEOUT;
            range_request(
                &cmd_tx,
                reference,
                path,
                StreamRange {
                    start: offset,
                    end_inclusive,
                },
                dst,
                deadline,
                progress_slot,
                bytes_downloaded,
            )
            .await
        })
    }

    /// Pull `len` bytes starting at `offset` and deliver each
    /// joiner-produced chunk to `on_chunk` as soon as it arrives. One
    /// long-running `StreamBzz` services the entire range — the
    /// manifest walk + data-root fetch + joiner setup happen exactly
    /// once, and `BytesChunk` acks flow straight to the consumer
    /// without staging in a Swift-side buffer first.
    ///
    /// This is the path `AVPlayer`'s `AVAssetResourceLoaderDelegate`
    /// uses: each `dataRequest` opens one pull, and the consumer
    /// calls `dataRequest.respond(with:)` inside the callback. With
    /// the prior `read`-fills-buffer model, `AVPlayer`'s ~10 s
    /// per-`dataRequest` patience window expired before the first
    /// 256 KiB landed (each slice paid the full `StreamBzz` setup
    /// tax, so a 55 MiB toEnd request never delivered enough body
    /// for playback before `AVPlayer` canceled it).
    ///
    /// `on_chunk(ctx, ptr, len) -> i32`: returning a non-zero value
    /// requests early termination (e.g. the consumer was cancelled);
    /// the call returns `Ok(bytes_delivered_so_far)` in that case.
    /// The callback runs on a runtime worker thread; it must be
    /// non-blocking and Send-safe.
    ///
    /// Returns the total number of bytes delivered to the callback.
    pub(crate) fn pull<F>(&self, offset: u64, len: u64, mut on_chunk: F) -> Result<u64, StreamError>
    where
        F: FnMut(&[u8]) -> bool,
    {
        if len == 0 || offset >= self.total_bytes {
            return Ok(0);
        }
        let bound_len = len
            .checked_add(offset)
            .map_or(len, |end| end.min(self.total_bytes).saturating_sub(offset));
        if bound_len == 0 {
            return Ok(0);
        }
        let end_inclusive = offset + bound_len - 1;
        let cmd_tx = self.cmd_tx.clone();
        let reference = self.reference;
        let path = self.path.clone();
        let progress_slot = &self.progress;
        let bytes_downloaded = &self.bytes_downloaded;
        self.runtime.block_on(async move {
            let deadline = tokio::time::Instant::now() + READ_TIMEOUT;
            range_pull(
                &cmd_tx,
                reference,
                path,
                StreamRange {
                    start: offset,
                    end_inclusive,
                },
                bound_len,
                deadline,
                progress_slot,
                bytes_downloaded,
                &mut on_chunk,
            )
            .await
        })
    }
}

async fn head_request(
    cmd_tx: &mpsc::Sender<ControlCommand>,
    reference: [u8; 32],
    path: String,
    deadline: tokio::time::Instant,
) -> Result<(u64, Option<String>, Option<String>), StreamError> {
    loop {
        let (ack_tx, mut ack_rx) = mpsc::channel::<ControlAck>(8);
        let cmd = ControlCommand::StreamBzz {
            reference: reference.to_vec(),
            path: path.clone(),
            allow_degraded_redundancy: true,
            bypass_cache: false,
            max_bytes: Some(MAX_STREAM_BYTES),
            range: None,
            head_only: true,
            ack: ack_tx,
        };
        cmd_tx
            .send(cmd)
            .await
            .map_err(|_| StreamError::Stream("node loop is not accepting commands".into()))?;
        loop {
            let next = tokio::time::timeout_at(deadline, ack_rx.recv()).await;
            match next {
                Ok(Some(ControlAck::BzzStreamStart {
                    total_bytes,
                    content_type,
                    filename,
                    ..
                })) => {
                    drain_until_done(&mut ack_rx).await;
                    return Ok((total_bytes, content_type, filename));
                }
                Ok(Some(
                    ControlAck::Error { message }
                    | ControlAck::NotFound { message }
                    | ControlAck::NotReady { message },
                )) => {
                    if is_no_peers(&message) {
                        if !sleep_until_or_deadline(NO_PEERS_RETRY_INTERVAL, deadline).await {
                            return Err(StreamError::Stream(format!(
                                "head request timed out: {message}",
                            )));
                        }
                        break;
                    }
                    return Err(StreamError::Stream(message));
                }
                Ok(Some(_)) => {}
                Ok(None) => {
                    return Err(StreamError::Stream(
                        "node dropped the ack channel without a head response".into(),
                    ))
                }
                Err(_) => {
                    return Err(StreamError::Stream(format!(
                        "head request timed out after {} s",
                        READ_TIMEOUT.as_secs()
                    )))
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn range_request(
    cmd_tx: &mpsc::Sender<ControlCommand>,
    reference: [u8; 32],
    path: String,
    range: StreamRange,
    dst: &mut [u8],
    deadline: tokio::time::Instant,
    progress_slot: &Mutex<StreamProgress>,
    bytes_downloaded: &AtomicU64,
) -> Result<usize, StreamError> {
    loop {
        let (ack_tx, mut ack_rx) = mpsc::channel::<ControlAck>(64);
        let cmd = ControlCommand::StreamBzz {
            reference: reference.to_vec(),
            path: path.clone(),
            allow_degraded_redundancy: true,
            bypass_cache: false,
            max_bytes: Some(MAX_STREAM_BYTES),
            range: Some(range),
            head_only: false,
            ack: ack_tx,
        };
        cmd_tx
            .send(cmd)
            .await
            .map_err(|_| StreamError::Stream("node loop is not accepting commands".into()))?;
        let mut written: usize = 0;
        let mut got_start = false;
        loop {
            let next = tokio::time::timeout_at(deadline, ack_rx.recv()).await;
            match next {
                Ok(Some(ControlAck::BzzStreamStart { .. })) => {
                    got_start = true;
                }
                Ok(Some(ControlAck::BytesChunk { data })) => {
                    let take = data.len().min(dst.len().saturating_sub(written));
                    if take > 0 {
                        dst[written..written + take].copy_from_slice(&data[..take]);
                        written += take;
                        bytes_downloaded.fetch_add(take as u64, Ordering::Relaxed);
                    }
                }
                Ok(Some(ControlAck::StreamDone)) => {
                    return Ok(written);
                }
                Ok(Some(ControlAck::Progress(p))) => {
                    if let Ok(mut slot) = progress_slot.lock() {
                        slot.chunks_done = p.chunks_done;
                        slot.total_chunks = p.total_chunks_estimate;
                        slot.peers_used = p.peers_used;
                        slot.in_flight = p.in_flight;
                        slot.cache_hits = p.cache_hits;
                    }
                }
                Ok(Some(
                    ControlAck::Error { message }
                    | ControlAck::NotFound { message }
                    | ControlAck::NotReady { message },
                )) => {
                    // A no-peers error before `BzzStreamStart` is the
                    // cold-start case — back off and retry the whole
                    // range request. Once the stream has started we
                    // treat any error as terminal because the joiner
                    // already retries chunk fetches internally.
                    if !got_start && is_no_peers(&message) {
                        if !sleep_until_or_deadline(NO_PEERS_RETRY_INTERVAL, deadline).await {
                            return Err(StreamError::Stream(format!(
                                "range read timed out: {message}",
                            )));
                        }
                        break;
                    }
                    return Err(StreamError::Stream(message));
                }
                Ok(Some(_)) => {}
                Ok(None) => {
                    return Err(StreamError::Stream(
                        "node dropped the ack channel without StreamDone".into(),
                    ))
                }
                Err(_) => {
                    return Err(StreamError::Stream(format!(
                        "range read timed out after {} s",
                        READ_TIMEOUT.as_secs()
                    )))
                }
            }
        }
    }
}

/// Like [`range_request`] but delivers chunks to a callback as they
/// arrive instead of filling a destination buffer. Returns the total
/// number of bytes pushed to the callback (≤ `cap` and ≤ remaining
/// body bytes). The callback's return value of `true` means "stop
/// pulling now" — used by the Swift loader to honour `AVPlayer`'s
/// cancellation before the joiner runs out of input.
#[allow(clippy::too_many_arguments)]
async fn range_pull(
    cmd_tx: &mpsc::Sender<ControlCommand>,
    reference: [u8; 32],
    path: String,
    range: StreamRange,
    cap: u64,
    deadline: tokio::time::Instant,
    progress_slot: &Mutex<StreamProgress>,
    bytes_downloaded: &AtomicU64,
    on_chunk: &mut dyn FnMut(&[u8]) -> bool,
) -> Result<u64, StreamError> {
    loop {
        let (ack_tx, mut ack_rx) = mpsc::channel::<ControlAck>(64);
        let cmd = ControlCommand::StreamBzz {
            reference: reference.to_vec(),
            path: path.clone(),
            allow_degraded_redundancy: true,
            bypass_cache: false,
            max_bytes: Some(MAX_STREAM_BYTES),
            range: Some(range),
            head_only: false,
            ack: ack_tx,
        };
        cmd_tx
            .send(cmd)
            .await
            .map_err(|_| StreamError::Stream("node loop is not accepting commands".into()))?;
        let mut delivered: u64 = 0;
        let mut got_start = false;
        let mut canceled = false;
        loop {
            let next = tokio::time::timeout_at(deadline, ack_rx.recv()).await;
            match next {
                Ok(Some(ControlAck::BzzStreamStart { .. })) => {
                    got_start = true;
                }
                Ok(Some(ControlAck::BytesChunk { data })) => {
                    let remaining = cap.saturating_sub(delivered);
                    if remaining == 0 {
                        canceled = true;
                        continue;
                    }
                    let take = (data.len() as u64).min(remaining) as usize;
                    if take > 0 {
                        bytes_downloaded.fetch_add(take as u64, Ordering::Relaxed);
                        delivered += take as u64;
                        if on_chunk(&data[..take]) {
                            // Consumer asked us to stop; drain remaining
                            // acks until StreamDone so the node loop's
                            // sender finishes cleanly rather than seeing
                            // an abrupt receiver drop.
                            canceled = true;
                        }
                    }
                }
                Ok(Some(ControlAck::StreamDone)) => {
                    return Ok(delivered);
                }
                Ok(Some(ControlAck::Progress(p))) => {
                    if let Ok(mut slot) = progress_slot.lock() {
                        slot.chunks_done = p.chunks_done;
                        slot.total_chunks = p.total_chunks_estimate;
                        slot.peers_used = p.peers_used;
                        slot.in_flight = p.in_flight;
                        slot.cache_hits = p.cache_hits;
                    }
                }
                Ok(Some(
                    ControlAck::Error { message }
                    | ControlAck::NotFound { message }
                    | ControlAck::NotReady { message },
                )) => {
                    if !got_start && is_no_peers(&message) {
                        if !sleep_until_or_deadline(NO_PEERS_RETRY_INTERVAL, deadline).await {
                            return Err(StreamError::Stream(format!(
                                "range pull timed out: {message}",
                            )));
                        }
                        break;
                    }
                    if canceled {
                        return Ok(delivered);
                    }
                    return Err(StreamError::Stream(message));
                }
                Ok(Some(_)) => {}
                Ok(None) => {
                    if canceled {
                        return Ok(delivered);
                    }
                    return Err(StreamError::Stream(
                        "node dropped the ack channel without StreamDone".into(),
                    ));
                }
                Err(_) => {
                    return Err(StreamError::Stream(format!(
                        "range pull timed out after {} s",
                        READ_TIMEOUT.as_secs()
                    )))
                }
            }
        }
    }
}

async fn drain_until_done(rx: &mut mpsc::Receiver<ControlAck>) {
    while let Ok(Some(ack)) = tokio::time::timeout(Duration::from_millis(250), rx.recv()).await {
        if matches!(
            ack,
            ControlAck::StreamDone
                | ControlAck::Error { .. }
                | ControlAck::NotFound { .. }
                | ControlAck::NotReady { .. }
        ) {
            return;
        }
    }
}

fn is_no_peers(msg: &str) -> bool {
    msg.contains("no peers available")
}

/// Sleep up to `dur`, but never past `deadline`. Returns `true` if we
/// slept the full duration (caller may retry), `false` if `deadline`
/// passed first (caller should bail).
async fn sleep_until_or_deadline(dur: Duration, deadline: tokio::time::Instant) -> bool {
    let now = tokio::time::Instant::now();
    if now >= deadline {
        return false;
    }
    let slice = dur.min(deadline.saturating_duration_since(now));
    tokio::time::sleep(slice).await;
    true
}

/// Mirrors the `StreamProgress` fields so callers can read a snapshot
/// through the FFI without holding the mutex. Order matches the
/// `AntProgress` C struct in `crates/ant-ffi/include/ant.h`:
/// `(bytes_done, total_bytes, chunks_done, total_chunks, elapsed_ms,
///   peers_used, in_flight, cache_hits)`.
pub(crate) fn snapshot_progress(stream: &AntStream) -> (u64, u64, u64, u64, u64, u32, u32, u64) {
    let p = stream.last_progress();
    (
        stream.bytes_downloaded(),
        stream.total_bytes(),
        p.chunks_done,
        p.total_chunks,
        stream.elapsed_ms(),
        p.peers_used,
        p.in_flight,
        p.cache_hits,
    )
}
