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
    /// A stream over `cmd_tx` whose head is already known, for tests
    /// that script the node side of the command channel.
    #[cfg(test)]
    pub(crate) fn for_test(
        runtime: Handle,
        cmd_tx: mpsc::Sender<ControlCommand>,
        total_bytes: u64,
    ) -> Self {
        Self {
            runtime,
            cmd_tx,
            reference: [0u8; 32],
            path: String::new(),
            total_bytes,
            content_type: None,
            filename: None,
            progress: Mutex::new(StreamProgress::default()),
            bytes_downloaded: AtomicU64::new(0),
            started_at: Instant::now(),
        }
    }

    /// Issue `StreamBzz { head_only: true }` and capture the resulting
    /// `BzzStreamStart` to learn `total_bytes` + `content_type`. The
    /// returned `AntStream` borrows the handle's mpsc sender + runtime
    /// handle but does *not* keep the handle alive. Using the stream
    /// after `ant_shutdown` is undefined behaviour, exactly as `ant.h`
    /// says: once the runtime is shut down, `block_on` with the read
    /// deadline's timer panics, and with the release profile's
    /// `panic = "abort"` that aborts the process. Always close the
    /// stream before shutting the handle down.
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
        let bound_len = bounded_len(
            u64::try_from(len).unwrap_or(u64::MAX),
            offset,
            self.total_bytes,
        );
        // `bound_len <= len`, so it fits a `usize`.
        let dst = &mut dst[..usize::try_from(bound_len).unwrap_or(len)];
        let end_inclusive = offset + bound_len - 1;
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
    /// the call returns `Ok(bytes_delivered_so_far)` at once, without
    /// invoking the callback again. Dropping the ack receiver makes
    /// the node's `StreamBzz` task stop fetching (its next ack send
    /// fails and `unless_client_gone` sees the closed channel).
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
        let bound_len = bounded_len(len, offset, self.total_bytes);
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
                            // Consumer asked us to stop: return now and
                            // drop `ack_rx`. The node's `StreamBzz` task
                            // treats a closed ack channel as "client
                            // gone" and stops fetching, so there is
                            // nothing to drain, and the callback must not
                            // run again after it asked to stop.
                            return Ok(delivered);
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

/// How many bytes of `len` starting at `offset` exist in a body of
/// `total` bytes: `min(len, total - offset)`, with no overflow for any
/// input. Callers have already returned early for `len == 0` and
/// `offset >= total`, so the result is at least 1 for them.
fn bounded_len(len: u64, offset: u64, total: u64) -> u64 {
    len.min(total.saturating_sub(offset))
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::oneshot;

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("build runtime")
    }

    fn start_ack(total_bytes: u64) -> ControlAck {
        ControlAck::BzzStreamStart {
            total_bytes,
            reference: vec![0u8; 32],
            content_type: None,
            filename: None,
            feed: None,
        }
    }

    #[test]
    fn bounded_len_never_overflows() {
        assert_eq!(bounded_len(u64::MAX, 10, 100), 90);
        assert_eq!(bounded_len(u64::MAX, u64::MAX - 1, u64::MAX), 1);
        assert_eq!(bounded_len(5, 10, 100), 5);
        assert_eq!(bounded_len(90, 10, 100), 90);
        assert_eq!(bounded_len(1, 100, 100), 0);
    }

    /// A consumer that asks to stop on its first chunk is called once,
    /// gets `Ok(first_len)` right away (not at `StreamDone` or the 60 s
    /// deadline), and the node sees the ack channel close so it stops
    /// fetching (audit FFI-02, issue #289).
    #[test]
    fn pull_stops_at_once_when_the_callback_asks_to() {
        const CHUNK: usize = 4096;
        const TOTAL: u64 = 16 * CHUNK as u64;
        let rt = runtime();
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<ControlCommand>(4);
        let (closed_tx, closed_rx) = oneshot::channel::<bool>();
        rt.spawn(async move {
            let Some(ControlCommand::StreamBzz { ack, .. }) = cmd_rx.recv().await else {
                panic!("expected StreamBzz");
            };
            ack.send(start_ack(TOTAL)).await.expect("start");
            // More chunks than the 64-deep channel holds would block,
            // so stay well under it; then never send StreamDone.
            for _ in 0..8 {
                if ack
                    .send(ControlAck::BytesChunk {
                        data: vec![7u8; CHUNK],
                    })
                    .await
                    .is_err()
                {
                    break;
                }
            }
            let closed = tokio::time::timeout(Duration::from_secs(5), ack.closed())
                .await
                .is_ok();
            let _ = closed_tx.send(closed);
        });
        let stream = AntStream::for_test(rt.handle().clone(), cmd_tx, TOTAL);
        let mut calls = 0usize;
        let started = Instant::now();
        let out = stream.pull(0, TOTAL, |chunk| {
            calls += 1;
            assert_eq!(chunk.len(), CHUNK);
            true
        });
        let elapsed = started.elapsed();
        assert_eq!(out.expect("pull"), CHUNK as u64);
        assert_eq!(
            calls, 1,
            "callback must not run again after it asked to stop"
        );
        assert!(elapsed < Duration::from_secs(5), "pull took {elapsed:?}");
        assert!(
            rt.block_on(closed_rx).expect("node task"),
            "the node must see the ack channel close"
        );
    }

    /// `pull(10, u64::MAX)` asks the node for `10..=total-1`, not a
    /// wrapped (inverted) range that would serve the body from byte 0
    /// (audit FFI-04, issue #289).
    #[test]
    fn pull_with_huge_len_requests_offset_to_end() {
        const TOTAL: u64 = 100;
        let rt = runtime();
        let (cmd_tx, mut cmd_rx) = mpsc::channel::<ControlCommand>(4);
        let (range_tx, range_rx) = oneshot::channel::<Option<StreamRange>>();
        rt.spawn(async move {
            let Some(ControlCommand::StreamBzz { ack, range, .. }) = cmd_rx.recv().await else {
                panic!("expected StreamBzz");
            };
            let _ = range_tx.send(range);
            let _ = ack.send(start_ack(TOTAL)).await;
            let _ = ack
                .send(ControlAck::BytesChunk {
                    data: vec![1u8; 90],
                })
                .await;
            let _ = ack.send(ControlAck::StreamDone).await;
        });
        let stream = AntStream::for_test(rt.handle().clone(), cmd_tx, TOTAL);
        let mut got = 0u64;
        let out = stream.pull(10, u64::MAX, |chunk| {
            got += chunk.len() as u64;
            false
        });
        assert_eq!(out.expect("pull"), 90);
        assert_eq!(got, 90);
        let range = rt
            .block_on(range_rx)
            .expect("node task")
            .expect("a ranged request");
        assert_eq!(range.start, 10);
        assert_eq!(range.end_inclusive, TOTAL - 1);
    }
}
