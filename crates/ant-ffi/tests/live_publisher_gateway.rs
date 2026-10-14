//! The stage-2 live publisher against the **real** `ant-gateway`.
//!
//! The unit tests in `src/publisher.rs` drive a stub that speaks bee's
//! wire contract; this one drives the router the iOS app actually runs
//! (`ant_start_gateway` → `ant_gateway`), in `light_mode`, with a stub
//! node loop standing in for the swarm. That covers the half a stub
//! gateway cannot: whether `POST /bytes`, `POST /feeds` and `POST /soc`
//! *as this publisher writes them* are accepted by ant's own handlers —
//! header names, the `?sig=` query, the inner-CAC body shape, and the
//! SOC signature ant validates with `soc_valid` before it will dispatch
//! anything — and whether what they wrote reads back the way a Solar
//! Punk viewer reads it: the playlist text from `GET /soc/{owner}/{id}`
//! and from `GET /feeds/{owner}/{topic}`.

use std::collections::HashMap;
use std::error::Error as StdError;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ant_control::{ControlAck, ControlCommand, StatusSnapshot};
use ant_ffi::publisher::{self, PublisherConfig, SegmentKind, StreamFormat};
use ant_gateway::{CorsConfig, GatewayChainState, GatewayHandle, GatewayIdentity, TagRegistry};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{mpsc, watch};

const SECRET: [u8; 32] = [0x5e; 32];

/// What the stub node loop was asked to push, and everything it stores.
#[derive(Default)]
struct NodeLog {
    chunks: usize,
    socs: Vec<([u8; 32], Vec<u8>)>,
    /// SOC wires that failed ant's own validator. Must stay empty: the
    /// gateway rejects those with 401 before they reach the node, so an
    /// entry here would mean the gateway let a bad SOC through.
    invalid_socs: usize,
    /// Every chunk and SOC pushed, by address: the "network" the
    /// gateway's reads are answered from.
    store: HashMap<[u8; 32], Vec<u8>>,
    /// Chunk reads served from `store`: what one viewer request costs.
    reads: usize,
    /// Hold every command until cleared: an uplink that stalls, so the
    /// publisher's backlog overflows and sheds segments.
    paused: bool,
    /// Refuse a chunk whose payload is this byte throughout: an upload
    /// that fails.
    reject_fill: Option<u8>,
}

/// A local chunk store over [`NodeLog::store`], so the stub node can run
/// ant's real feed finder and joiner instead of re-implementing them.
struct MemFetcher(Arc<Mutex<NodeLog>>);

#[async_trait::async_trait]
impl ant_retrieval::ChunkFetcher for MemFetcher {
    async fn fetch(&self, addr: [u8; 32]) -> Result<Vec<u8>, Box<dyn StdError + Send + Sync>> {
        let mut log = self.0.lock().unwrap();
        log.reads += 1;
        log.store
            .get(&addr)
            .cloned()
            .ok_or_else(|| format!("chunk {} not found", hex::encode(addr)).into())
    }
}

/// Answer the commands the gateway dispatches, the way a healthy node
/// would: ack each chunk/SOC push with its own address as the reference,
/// and serve reads (`GET /soc`, `GET /feeds`) from what was pushed.
async fn stub_node(mut rx: mpsc::Receiver<ControlCommand>, log: Arc<Mutex<NodeLog>>) {
    let fetcher = MemFetcher(Arc::clone(&log));
    while let Some(cmd) = rx.recv().await {
        while log.lock().unwrap().paused {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        match cmd {
            ControlCommand::PushChunk { wire, ack, .. } => {
                let reject = log.lock().unwrap().reject_fill;
                if reject.is_some_and(|fill| wire.len() > 8 && wire[8..].iter().all(|b| *b == fill))
                {
                    let _ = ack.send(ControlAck::Error {
                        message: "refused".into(),
                    });
                    continue;
                }
                let Some(address) = bmt_address(&wire) else {
                    let _ = ack.send(ControlAck::Error {
                        message: "unsplittable chunk".into(),
                    });
                    continue;
                };
                {
                    let mut log = log.lock().unwrap();
                    log.chunks += 1;
                    log.store.insert(address, wire);
                }
                let _ = ack.send(ControlAck::ChunkUploaded {
                    reference: hex::encode(address),
                });
            }
            ControlCommand::PushSoc {
                address, wire, ack, ..
            } => {
                {
                    let mut log = log.lock().unwrap();
                    if !ant_crypto::soc_valid(&address, &wire) {
                        log.invalid_socs += 1;
                    }
                    log.socs.push((address, wire.clone()));
                    log.store.insert(address, wire);
                }
                let _ = ack.send(ControlAck::ChunkUploaded {
                    reference: hex::encode(address),
                });
            }
            ControlCommand::GetChunkRaw { reference, ack, .. } => {
                use ant_retrieval::ChunkFetcher as _;
                let reply = match fetcher.fetch(reference).await {
                    Ok(data) => ControlAck::Bytes { data },
                    Err(e) => ControlAck::NotFound {
                        message: e.to_string(),
                    },
                };
                let _ = ack.send(reply);
            }
            ControlCommand::GetFeed {
                owner,
                topic,
                after,
                ack,
            } => {
                let feed = ant_retrieval::Feed {
                    owner,
                    topic,
                    kind: ant_retrieval::FeedType::Sequence,
                };
                let reply = match ant_retrieval::resolve_sequence_feed_after(&fetcher, &feed, after)
                    .await
                {
                    Ok(r) => ControlAck::FeedResolved {
                        reference: r.reference,
                        index: r.index,
                        signature: r.signature,
                        v2: r.v2,
                        decrypt_key: r.decrypt_key,
                    },
                    Err(ant_retrieval::FeedError::NoUpdates { .. }) => ControlAck::FeedNotFound,
                    Err(e) => ControlAck::Error {
                        message: e.to_string(),
                    },
                };
                let _ = ack.send(reply);
            }
            ControlCommand::StreamBytes {
                reference,
                head_only,
                ack,
                ..
            } => {
                use ant_retrieval::ChunkFetcher as _;
                let root = match fetcher.fetch(reference).await {
                    Ok(root) => root,
                    Err(e) => {
                        let _ = ack
                            .send(ControlAck::NotFound {
                                message: e.to_string(),
                            })
                            .await;
                        continue;
                    }
                };
                let mut span = [0u8; 8];
                span.copy_from_slice(&root[..8]);
                span[7] = 0;
                let total_bytes = u64::from_le_bytes(span);
                let _ = ack.send(ControlAck::BytesStreamStart { total_bytes }).await;
                if head_only {
                    continue;
                }
                match ant_retrieval::join(&fetcher, &root, ant_retrieval::DEFAULT_MAX_FILE_BYTES)
                    .await
                {
                    Ok(data) => {
                        let _ = ack.send(ControlAck::BytesChunk { data }).await;
                        let _ = ack.send(ControlAck::StreamDone).await;
                    }
                    Err(e) => {
                        let _ = ack
                            .send(ControlAck::Error {
                                message: e.to_string(),
                            })
                            .await;
                    }
                }
            }
            ControlCommand::StreamBzz {
                reference,
                path,
                head_only,
                ack,
                ..
            } => {
                use ant_retrieval::ChunkFetcher as _;
                let found = match ant_retrieval::lookup_path(&fetcher, &reference, &path).await {
                    Ok(found) => found,
                    Err(e) => {
                        let _ = ack
                            .send(ControlAck::NotFound {
                                message: e.to_string(),
                            })
                            .await;
                        continue;
                    }
                };
                let mut data_ref = [0u8; 32];
                data_ref.copy_from_slice(&found.data_ref[..32]);
                let Ok(root) = fetcher.fetch(data_ref).await else {
                    let _ = ack
                        .send(ControlAck::NotFound {
                            message: "file root missing".into(),
                        })
                        .await;
                    continue;
                };
                let mut span = [0u8; 8];
                span.copy_from_slice(&root[..8]);
                span[7] = 0;
                let _ = ack
                    .send(ControlAck::BzzStreamStart {
                        total_bytes: u64::from_le_bytes(span),
                        reference: found.data_ref.clone(),
                        content_type: found.content_type.clone(),
                        filename: None,
                        feed: None,
                    })
                    .await;
                if head_only {
                    continue;
                }
                if let Ok(data) =
                    ant_retrieval::join(&fetcher, &root, ant_retrieval::DEFAULT_MAX_FILE_BYTES)
                        .await
                {
                    let _ = ack.send(ControlAck::BytesChunk { data }).await;
                    let _ = ack.send(ControlAck::StreamDone).await;
                }
            }
            other => drop(other),
        }
    }
}

/// Wall clock, Unix ms.
fn now_ms() -> u64 {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

/// `2026-09-01T12:00:00.000Z` → Unix ms. Only the exact shape the
/// publisher writes (JavaScript's `toISOString`) parses.
fn parse_program_date_time(text: &str) -> u64 {
    let b = text.as_bytes();
    assert!(
        b.len() == 24 && b[4] == b'-' && b[10] == b'T' && b[19] == b'.' && b[23] == b'Z',
        "not ISO 8601 UTC to the ms: {text}"
    );
    let num = |r: std::ops::Range<usize>| text[r].parse::<u64>().unwrap();
    let (year, month, day) = (num(0..4), num(5..7), num(8..10));
    // Days from civil (Howard Hinnant), the inverse of the publisher's.
    let y = if month <= 2 { year - 1 } else { year };
    let era = y / 400;
    let yoe = y - era * 400;
    let mp = (month + 9) % 12;
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    ((days * 24 + num(11..13)) * 60 + num(14..16)) * 60_000 + num(17..19) * 1000 + num(20..23)
}

/// Issue #170: every listed segment carries `#EXT-X-PROGRAM-DATE-TIME`
/// right in front of its `#EXTINF`, dated from one anchor taken from the
/// wall clock at go-live and stepped by each segment's media (here 2 s,
/// although the test pushes them 50 ms apart: the step is the media,
/// never the arrival). Returns the dates.
fn assert_dated(playlist: &str, pushed_from_ms: u64, pushed_until_ms: u64) -> Vec<u64> {
    let lines: Vec<&str> = playlist.lines().collect();
    let mut dates = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        if line.starts_with("#EXTINF:") {
            let date = lines[i - 1]
                .strip_prefix("#EXT-X-PROGRAM-DATE-TIME:")
                .unwrap_or_else(|| panic!("segment without a date:\n{playlist}"));
            dates.push(parse_program_date_time(date));
        }
    }
    assert!(!dates.is_empty(), "{playlist}");
    // The first segment is dated at its first frame: when it was pushed,
    // less the 2 s it holds.
    assert!(
        dates[0] + 2000 >= pushed_from_ms && dates[0] + 2000 <= pushed_until_ms,
        "first date {} outside the push window {pushed_from_ms}..{pushed_until_ms}",
        dates[0],
    );
    for pair in dates.windows(2) {
        assert_eq!(pair[1] - pair[0], 2000, "{playlist}");
    }
    dates
}

/// One plain `GET` against the gateway: status, lower-cased head and
/// body. HTTP/1.0, so the body arrives unchunked and ends at EOF.
async fn http_get(addr: &str, path: &str) -> (u16, String, Vec<u8>) {
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    stream
        .write_all(format!("GET {path} HTTP/1.0\r\nhost: {addr}\r\n\r\n").as_bytes())
        .await
        .unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.unwrap();
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("response head");
    let head = String::from_utf8_lossy(&raw[..split]).to_ascii_lowercase();
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .expect("status code");
    (status, head, raw[split + 4..].to_vec())
}

fn bmt_address(wire: &[u8]) -> Option<[u8; 32]> {
    let span: [u8; 8] = wire.get(..8)?.try_into().ok()?;
    ant_crypto::bmt::bmt_hash_with_span(&span, &wire[8..])
}

/// Bring up the production router on a loopback port, backed by
/// [`stub_node`]. Mirrors how `ant_start_gateway` builds its handle,
/// including `light_mode` (the mode `AntStream` publishes in).
async fn serve_gateway(log: Arc<Mutex<NodeLog>>) -> String {
    let (cmd_tx, cmd_rx) = mpsc::channel::<ControlCommand>(64);
    let (_status_tx, status_rx) = watch::channel(StatusSnapshot::default());
    tokio::spawn(stub_node(cmd_rx, log));

    let handle = GatewayHandle {
        agent: Arc::new("ant-ffi/test".to_string()),
        api_version: Arc::new("7.2.0".to_string()),
        identity: Arc::new(GatewayIdentity {
            overlay_hex: String::new(),
            ethereum_hex: String::new(),
            public_key_hex: String::new(),
            peer_id: String::new(),
        }),
        status: status_rx,
        commands: cmd_tx,
        activity: ant_control::GatewayActivity::new(),
        tags: Arc::new(TagRegistry::new()),
        cors: Arc::new(CorsConfig::new(["null"])),
        chain_state: GatewayChainState {
            light_mode: true,
            chain: None,
        }
        .preset(),
        act_secret: Arc::new(SECRET),
        on_batch_bought: None,
        on_chequebook_refused: None,
        cache: None,
    };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let app = ant_gateway::testkit::build_router(handle);
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });
    addr
}

fn owner() -> [u8; 20] {
    let sk = k256::ecdsa::SigningKey::from_bytes(&SECRET.into()).unwrap();
    ant_crypto::ethereum_address_from_public_key(sk.verifying_key())
}

const TOPIC_TEXT: &str = "antstream-integration";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broadcast_is_accepted_end_to_end_by_the_real_gateway() {
    let log = Arc::new(Mutex::new(NodeLog::default()));
    let addr = serve_gateway(Arc::clone(&log)).await;

    let config = PublisherConfig {
        channel: "integration".into(),
        // A text topic, the way a Solar Punk catalog entry names one.
        topic: String::new(),
        topic_text: TOPIC_TEXT.into(),
        format: StreamFormat::SolarPunk,
        gateway: format!("http://{addr}"),
        batch_id: "ab".repeat(32),
        segment_ms: 2000,
        bitrate_kbps: 900,
        max_in_flight: 4,
        max_backlog: 4,
        playlist_window: 6,
        notes: String::new(),
        start_index: 0,
        reap_ms: 0,
    };
    let run = publisher::start(
        &tokio::runtime::Handle::current(),
        config,
        SECRET,
        owner(),
        None,
    )
    .unwrap();

    let pushed_from = now_ms();
    let _ = run.push(SegmentKind::Init, vec![0x11; 900], 0, false);
    for i in 0..3u8 {
        let _ = run.push(SegmentKind::Media, vec![i; 40_000], 2000, false);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let pushed_until = now_ms();
    run.cancel();
    for _ in 0..400 {
        if run.is_finished() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(run.is_finished(), "publisher never finished");

    let report = run.report();
    assert_eq!(report.segments_failed, 0, "{:?}", report.errors);
    assert_eq!(report.segments_published, 3, "{report:?}");
    assert!(report.playlists_published > 0, "{report:?}");
    assert!(report.feed_updates > 0, "{report:?}");
    assert!(
        !report.channel_reference.is_empty(),
        "POST /feeds must yield a shareable channel reference: {report:?}",
    );
    assert!(report.kept_up, "{report:?}");

    let topic = publisher::topic_from_string(TOPIC_TEXT);
    assert_eq!(report.topic, hex::encode(topic));
    assert_eq!(report.topic_text, TOPIC_TEXT);
    {
        let log = log.lock().unwrap();
        assert_eq!(log.invalid_socs, 0, "gateway dispatched an invalid SOC");
        // Every feed update reached the node as a single-owner chunk at the
        // bee sequence address for its index. (The node also sees the
        // dispersed-replica SOCs bee's default redundancy level mints for
        // every upload, so match by address rather than by count.)
        let feed = ant_retrieval::Feed {
            owner: owner(),
            topic,
            kind: ant_retrieval::FeedType::Sequence,
        };
        for index in 0..report.feed_updates {
            let expected = ant_retrieval::sequence_update_address(&feed, index);
            assert!(
                log.socs.iter().any(|(address, _)| *address == expected),
                "feed update {index} never reached the node at its sequence address",
            );
        }
        // …and nothing was written past the last index the publisher
        // reported, which is what a viewer's feed walk relies on.
        let past_end = ant_retrieval::sequence_update_address(&feed, report.feed_updates);
        assert!(
            !log.socs.iter().any(|(address, _)| *address == past_end),
            "a feed update landed past the reported head index",
        );
        // Segments, playlists and the feed manifest all went out as chunks.
        assert!(log.chunks > 0);
    }

    // Read the channel back the two ways a Solar Punk viewer does, from
    // nothing but the owner and the topic.
    let owner_hex = hex::encode(owner());
    let (status, head, feeds_body) =
        http_get(&addr, &format!("/feeds/{owner_hex}/{}", hex::encode(topic))).await;
    assert_eq!(
        status,
        200,
        "GET /feeds: {}",
        String::from_utf8_lossy(&feeds_body)
    );
    assert!(head.contains("swarm-feed-resolved-version: v2"), "{head}");
    let head_index = format!(
        "swarm-feed-index: {}",
        hex::encode((report.feed_updates - 1).to_be_bytes())
    );
    assert!(
        head.contains(&head_index),
        "the head is the last update: {head}"
    );
    let playlist = String::from_utf8(feeds_body).expect("GET /feeds returns the playlist text");
    assert!(playlist.starts_with("#EXTM3U\n"), "{playlist}");
    assert!(
        playlist.contains("#EXT-X-ENDLIST") && playlist.contains("#EXT-X-PLAYLIST-TYPE:VOD"),
        "the head is the recording:\n{playlist}"
    );
    assert_eq!(report.recording_reference, report.playlist_reference);
    assert_eq!(playlist.matches("#EXTINF").count(), 3, "{playlist}");
    assert_eq!(assert_dated(&playlist, pushed_from, pushed_until).len(), 3);

    // The head's own slot, by address: the same text, not a pointer.
    let head_id = ant_retrieval::sequence_update_id(&topic, report.feed_updates - 1);
    let (status, _, soc_body) =
        http_get(&addr, &format!("/soc/{owner_hex}/{}", hex::encode(head_id))).await;
    assert_eq!(
        status,
        200,
        "GET /soc: {}",
        String::from_utf8_lossy(&soc_body)
    );
    assert_eq!(
        String::from_utf8(soc_body).unwrap(),
        playlist,
        "GET /soc and GET /feeds agree"
    );
    // The first slot is a playlist too: every update is one.
    let first_id = ant_retrieval::sequence_update_id(&topic, 0);
    let (status, _, first) = http_get(
        &addr,
        &format!("/soc/{owner_hex}/{}", hex::encode(first_id)),
    )
    .await;
    assert_eq!(status, 200);
    assert!(
        first.starts_with(b"#EXTM3U\n"),
        "{}",
        String::from_utf8_lossy(&first)
    );

    // The channel reference names the feed: its manifest node carries
    // the owner and topic as plain JSON metadata, which is how the
    // AntStream viewers get from a shared channel reference to the
    // `GET /feeds/{owner}/{topic}` above (the manifest's own `/bzz` path
    // can't reach a v2 update's content).
    let (status, _, manifest) =
        http_get(&addr, &format!("/bytes/{}", report.channel_reference)).await;
    assert_eq!(status, 200);
    let manifest = String::from_utf8_lossy(&manifest);
    assert!(
        manifest.contains(&format!("\"swarm-feed-owner\":\"{owner_hex}\"")),
        "{manifest:?}"
    );
    assert!(
        manifest.contains(&format!("\"swarm-feed-topic\":\"{}\"", hex::encode(topic))),
        "{manifest:?}"
    );

    // Segments and the map are bare references, and each one fetched
    // from this gateway's `/bytes/` — what a Solar Punk viewer builds
    // from them — is the segment that was pushed.
    let map = playlist
        .lines()
        .find_map(|l| l.strip_prefix("#EXT-X-MAP:URI=\"")?.strip_suffix('"'))
        .expect("map line");
    let (status, _, init) = http_get(&addr, &format!("/bytes/{map}")).await;
    assert_eq!((status, init), (200, vec![0x11; 900]));
    let uris: Vec<&str> = playlist.lines().filter(|l| !l.starts_with('#')).collect();
    assert_eq!(uris.len(), 3, "{playlist}");
    for (i, uri) in uris.iter().enumerate() {
        assert!(
            uri.len() == 64 && hex::decode(uri).is_ok(),
            "bare reference: {uri}"
        );
        let (status, _, segment) = http_get(&addr, &format!("/bytes/{uri}")).await;
        assert_eq!(status, 200, "segment {i}");
        assert_eq!(segment, vec![i as u8; 40_000], "segment {i}");
    }
}

/// Issue #171: the live window is sized to one chunk, not a segment
/// count. A broadcast far longer than fits keeps every live feed update a
/// single chunk (the SOC payload *is* the whole playlist), still lists
/// as many of the newest segments as fit, and a viewer's poll of a live
/// slot is one chunk read.
///
/// Issue #168: at stop, after the closing window, the whole broadcast
/// goes up as the recording and is the final update: a v2 update
/// wrapping the root of a chunk tree, which `GET /feeds` and
/// `GET /soc` both answer with the full playlist, every segment from
/// the first to the last as VOD.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn live_updates_are_single_chunks_and_the_head_is_the_whole_recording() {
    const SEGMENTS: u8 = 80;
    let log = Arc::new(Mutex::new(NodeLog::default()));
    let addr = serve_gateway(Arc::clone(&log)).await;
    // The JSON a host sends, `playlist_window` left at its default: the
    // byte budget, not a segment count, is what sizes the window.
    let config: PublisherConfig = serde_json::from_value(serde_json::json!({
        "channel": "integration",
        "topic_text": "antstream-integration-long",
        "format": "solar-punk",
        "gateway": format!("http://{addr}"),
        "batch_id": "ab".repeat(32),
        // Init + every segment, so none is shed.
        "max_backlog": usize::from(SEGMENTS) + 1,
    }))
    .unwrap();
    assert_eq!(config.playlist_window, publisher::DEFAULT_PLAYLIST_WINDOW);
    let run = publisher::start(
        &tokio::runtime::Handle::current(),
        config,
        SECRET,
        owner(),
        None,
    )
    .unwrap();
    let pushed_from = now_ms();
    let _ = run.push(SegmentKind::Init, vec![0x11; 900], 0, false);
    for i in 0..SEGMENTS {
        let _ = run.push(SegmentKind::Media, vec![i; 300], 2000, false);
    }
    let pushed_until = now_ms();
    run.cancel();
    for _ in 0..800 {
        if run.is_finished() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(run.is_finished(), "publisher never finished");
    let report = run.report();
    assert_eq!(report.segments_failed, 0, "{:?}", report.errors);
    assert_eq!(report.error_count, 0, "{:?}", report.errors);
    assert_eq!(report.segments_listed, u64::from(SEGMENTS), "{report:?}");
    // At least one live update, the closing window and the recording.
    assert!(report.feed_updates >= 3, "{report:?}");
    let head = report.feed_updates - 1;

    let topic = publisher::topic_from_string("antstream-integration-long");
    let feed = ant_retrieval::Feed {
        owner: owner(),
        topic,
        kind: ant_retrieval::FeedType::Sequence,
    };
    // id(32) ‖ signature(65) ‖ span(8) ‖ payload. The span's top byte
    // carries the redundancy level on an intermediate chunk; the rest is
    // the length it covers.
    let update = |log: &NodeLog, index: u64| -> (u64, Vec<u8>) {
        let address = ant_retrieval::sequence_update_address(&feed, index);
        let (_, wire) = log
            .socs
            .iter()
            .find(|(a, _)| *a == address)
            .unwrap_or_else(|| panic!("feed update {index} never reached the node"));
        let mut span = [0u8; 8];
        span.copy_from_slice(&wire[97..105]);
        span[7] = 0;
        (u64::from_le_bytes(span), wire[105..].to_vec())
    };
    {
        let log = log.lock().unwrap();
        assert_eq!(log.invalid_socs, 0);
        // Every live update, the closing one included: the SOC wraps a
        // leaf chunk (span = payload length, at most 4096) whose payload
        // is a whole playlist.
        for index in 0..head {
            let (span, payload) = update(&log, index);
            assert!(
                span <= publisher::LIVE_WINDOW_MAX_BYTES as u64,
                "feed update {index} wraps a {span}-byte tree, not one chunk",
            );
            assert_eq!(payload.len() as u64, span, "feed update {index} is a leaf");
            assert!(payload.starts_with(b"#EXTM3U\n"), "feed update {index}");
        }
        // The recording: far past one chunk, so the update wraps the
        // root of its tree (a payload of child references, not text).
        let (span, payload) = update(&log, head);
        assert!(
            span > publisher::LIVE_WINDOW_MAX_BYTES as u64,
            "the recording is {span} bytes"
        );
        assert!(
            !payload.starts_with(b"#EXTM3U"),
            "the head wraps a tree root"
        );
    }

    // The closing window, read the way a live viewer polls it: one HTTP
    // round trip answered from one chunk.
    let owner_hex = hex::encode(owner());
    let closing_id = ant_retrieval::sequence_update_id(&topic, head - 1);
    log.lock().unwrap().reads = 0;
    let (status, _, soc_body) = http_get(
        &addr,
        &format!("/soc/{owner_hex}/{}", hex::encode(closing_id)),
    )
    .await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&soc_body));
    assert_eq!(log.lock().unwrap().reads, 1, "GET /soc is one chunk read");
    let closing = String::from_utf8(soc_body).unwrap();
    assert!(
        closing.len() <= publisher::LIVE_WINDOW_MAX_BYTES,
        "{} bytes",
        closing.len()
    );
    assert!(closing.starts_with("#EXTM3U\n"));
    assert!(closing.ends_with("#EXT-X-ENDLIST\n"));
    assert!(!closing.contains("#EXT-X-PLAYLIST-TYPE"), "{closing}");
    // Far more than the old 6-segment window, fewer than the broadcast:
    // the newest segments that fit, each with its date-time (#170).
    let listed = closing.matches("#EXTINF").count();
    assert_eq!(
        closing.matches("#EXT-X-PROGRAM-DATE-TIME:").count(),
        listed,
        "{closing}"
    );
    assert!(
        (29..usize::from(SEGMENTS)).contains(&listed),
        "{listed} segments listed:\n{closing}"
    );
    assert!(
        closing.contains(&format!(
            "#EXT-X-MEDIA-SEQUENCE:{}\n",
            usize::from(SEGMENTS) - listed
        )),
        "{closing}"
    );

    // The head, the way a Solar Punk viewer finds it from the owner and
    // topic alone: the whole broadcast, joined from the tree.
    let (status, head_hdr, feeds_body) =
        http_get(&addr, &format!("/feeds/{owner_hex}/{}", hex::encode(topic))).await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&feeds_body));
    assert!(
        head_hdr.contains("swarm-feed-resolved-version: v2"),
        "{head_hdr}"
    );
    assert!(
        head_hdr.contains(&format!(
            "swarm-feed-index: {}",
            hex::encode(head.to_be_bytes())
        )),
        "{head_hdr}"
    );
    let recording = String::from_utf8(feeds_body).unwrap();
    assert!(recording.len() > publisher::LIVE_WINDOW_MAX_BYTES);
    assert!(recording.starts_with("#EXTM3U\n"), "{recording}");
    assert!(
        recording.contains("\n#EXT-X-PLAYLIST-TYPE:VOD\n"),
        "{recording}"
    );
    assert!(
        recording.contains("\n#EXT-X-MEDIA-SEQUENCE:0\n"),
        "{recording}"
    );
    assert!(recording.ends_with("#EXT-X-ENDLIST\n"), "{recording}");
    assert_eq!(recording.matches("#EXT-X-MAP:").count(), 1, "{recording}");
    // Every segment, first to last, dated straight through, and each one
    // the segment that was pushed at that place.
    assert_eq!(
        assert_dated(&recording, pushed_from, pushed_until).len(),
        usize::from(SEGMENTS)
    );
    let uris: Vec<&str> = recording.lines().filter(|l| !l.starts_with('#')).collect();
    assert_eq!(uris.len(), usize::from(SEGMENTS), "{recording}");
    for (i, uri) in uris.iter().enumerate() {
        let (status, _, segment) = http_get(&addr, &format!("/bytes/{uri}")).await;
        assert_eq!((status, segment), (200, vec![i as u8; 300]), "segment {i}");
    }
    // The closing window's segments are the recording's tail, at the
    // same numbers.
    let closing_uris: Vec<&str> = closing.lines().filter(|l| !l.starts_with('#')).collect();
    assert_eq!(closing_uris, uris[uris.len() - listed..]);

    // `GET /soc` of the head slot is the same text, and so is the
    // reference the stop report hands the host, read with no feed at all.
    let head_id = ant_retrieval::sequence_update_id(&topic, head);
    let (status, _, soc_body) =
        http_get(&addr, &format!("/soc/{owner_hex}/{}", hex::encode(head_id))).await;
    assert_eq!(status, 200);
    assert_eq!(String::from_utf8(soc_body).unwrap(), recording);
    assert_eq!(report.playlist_reference, report.recording_reference);
    let (status, _, by_ref) =
        http_get(&addr, &format!("/bytes/{}", report.recording_reference)).await;
    assert_eq!(status, 200);
    assert_eq!(String::from_utf8(by_ref).unwrap(), recording);
}

/// The default format, `"bzz"`: what the Android Watch tab and the web
/// player (the target of both apps' Share links) read. A host that sets
/// no `format` must keep producing a channel those viewers play through
/// `/bzz/<channel>/stream.m3u8`, with segments at gateway-root
/// `/bzz/<ref>/<name>` paths.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_default_format_plays_through_the_channel_bzz_path() {
    let log = Arc::new(Mutex::new(NodeLog::default()));
    let addr = serve_gateway(Arc::clone(&log)).await;
    // The JSON a host that predates `format` sends (the Android app).
    let config: PublisherConfig = serde_json::from_value(serde_json::json!({
        "channel": "integration",
        "topic": "cd".repeat(32),
        "gateway": format!("http://{addr}"),
        "batch_id": "ab".repeat(32),
    }))
    .unwrap();
    assert_eq!(config.format, StreamFormat::Bzz);
    let run = publisher::start(
        &tokio::runtime::Handle::current(),
        config,
        SECRET,
        owner(),
        None,
    )
    .unwrap();
    let pushed_from = now_ms();
    let _ = run.push(SegmentKind::Init, vec![0x11; 900], 0, false);
    for i in 0..3u8 {
        let _ = run.push(SegmentKind::Media, vec![i; 40_000], 2000, false);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let pushed_until = now_ms();
    run.cancel();
    for _ in 0..400 {
        if run.is_finished() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(run.is_finished(), "publisher never finished");
    let report = run.report();
    assert_eq!(report.segments_failed, 0, "{:?}", report.errors);
    assert_eq!(report.format, StreamFormat::Bzz);
    assert!(report.feed_updates > 0, "{report:?}");
    assert!(!report.channel_reference.is_empty(), "{report:?}");
    assert_eq!(log.lock().unwrap().invalid_socs, 0);

    let (status, _, body) = http_get(
        &addr,
        &format!("/bzz/{}/stream.m3u8", report.channel_reference),
    )
    .await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let playlist = String::from_utf8(body).unwrap();
    assert!(playlist.starts_with("#EXTM3U\n"), "{playlist}");
    assert!(playlist.contains("#EXT-X-ENDLIST"), "{playlist}");
    assert_eq!(assert_dated(&playlist, pushed_from, pushed_until).len(), 3);
    let map = playlist
        .lines()
        .find_map(|l| l.strip_prefix("#EXT-X-MAP:URI=\"")?.strip_suffix('"'))
        .expect("map line");
    assert!(
        map.starts_with("/bzz/") && map.ends_with("/init-0.mp4"),
        "{map}"
    );
    let (status, _, init) = http_get(&addr, map).await;
    assert_eq!((status, init), (200, vec![0x11; 900]));
    let uris: Vec<&str> = playlist.lines().filter(|l| !l.starts_with('#')).collect();
    assert_eq!(uris.len(), 3, "{playlist}");
    for (i, uri) in uris.iter().enumerate() {
        assert!(uri.starts_with("/bzz/") && uri.contains("/seg-"), "{uri}");
        let (status, _, segment) = http_get(&addr, uri).await;
        assert_eq!(status, 200, "segment {i}");
        assert_eq!(segment, vec![i as u8; 40_000], "segment {i}");
    }
    // The channel's head is the recording (issue #168), and the stop
    // report's reference plays it without the feed.
    assert!(
        playlist.contains("\n#EXT-X-PLAYLIST-TYPE:VOD\n"),
        "{playlist}"
    );
    let (status, _, by_ref) = http_get(
        &addr,
        &format!("/bzz/{}/stream.m3u8", report.recording_reference),
    )
    .await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&by_ref));
    assert_eq!(String::from_utf8(by_ref).unwrap(), playlist);
}

/// Issue #168 in the default format, what the web player and the
/// Android Watch tab read: a broadcast far longer than the live window
/// ends with `/bzz/<channel>/stream.m3u8` resolving to the recording,
/// every segment from the first to the last (a playlist file of
/// several chunks behind its manifest).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_default_format_channel_ends_on_the_whole_recording() {
    const SEGMENTS: u8 = 80;
    let log = Arc::new(Mutex::new(NodeLog::default()));
    let addr = serve_gateway(Arc::clone(&log)).await;
    let config: PublisherConfig = serde_json::from_value(serde_json::json!({
        "channel": "integration",
        "topic": "ce".repeat(32),
        "gateway": format!("http://{addr}"),
        "batch_id": "ab".repeat(32),
        "max_backlog": usize::from(SEGMENTS) + 1,
    }))
    .unwrap();
    let run = publisher::start(
        &tokio::runtime::Handle::current(),
        config,
        SECRET,
        owner(),
        None,
    )
    .unwrap();
    let pushed_from = now_ms();
    let _ = run.push(SegmentKind::Init, vec![0x11; 900], 0, false);
    for i in 0..SEGMENTS {
        let _ = run.push(SegmentKind::Media, vec![i; 300], 2000, false);
    }
    let pushed_until = now_ms();
    run.cancel();
    for _ in 0..800 {
        if run.is_finished() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(run.is_finished(), "publisher never finished");
    let report = run.report();
    assert_eq!(report.error_count, 0, "{:?}", report.errors);
    assert_eq!(report.segments_listed, u64::from(SEGMENTS), "{report:?}");
    assert!(!report.channel_reference.is_empty(), "{report:?}");
    assert!(!report.recording_reference.is_empty(), "{report:?}");

    let (status, _, body) = http_get(
        &addr,
        &format!("/bzz/{}/stream.m3u8", report.channel_reference),
    )
    .await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let recording = String::from_utf8(body).unwrap();
    assert!(recording.len() > publisher::LIVE_WINDOW_MAX_BYTES);
    assert!(
        recording.contains("\n#EXT-X-PLAYLIST-TYPE:VOD\n"),
        "{recording}"
    );
    assert!(
        recording.contains("\n#EXT-X-MEDIA-SEQUENCE:0\n"),
        "{recording}"
    );
    assert!(recording.ends_with("#EXT-X-ENDLIST\n"), "{recording}");
    assert_eq!(
        assert_dated(&recording, pushed_from, pushed_until).len(),
        usize::from(SEGMENTS)
    );
    let uris: Vec<&str> = recording.lines().filter(|l| !l.starts_with('#')).collect();
    for (i, uri) in uris.iter().enumerate() {
        let (status, _, segment) = http_get(&addr, uri).await;
        assert_eq!((status, segment), (200, vec![i as u8; 300]), "segment {i}");
    }
    let (status, _, by_ref) = http_get(
        &addr,
        &format!("/bzz/{}/stream.m3u8", report.recording_reference),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(String::from_utf8(by_ref).unwrap(), recording);
}

/// Wait until `done` holds for the run's snapshot.
async fn wait_for(run: &publisher::LiveRun, done: impl Fn(&publisher::PublisherSnapshot) -> bool) {
    for _ in 0..400 {
        if done(&run.snapshot()) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out: {:?}", run.snapshot());
}

/// Issue #169: a lost segment, whether the backlog dropped it or its
/// upload failed, is listed as an `#EXT-X-GAP` entry at its own media
/// sequence number, and arms no discontinuity. Read back through the
/// real gateway, every feed update a viewer could have polled agrees on
/// every segment's number while a 3-segment window slides past the
/// holes, no update starts or ends on a gap, and the dates run on
/// without a reset across them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lost_segment_is_a_gap_entry_and_numbers_never_shift() {
    const FAIL: u8 = 0xf0;
    let log = Arc::new(Mutex::new(NodeLog {
        reject_fill: Some(FAIL),
        ..NodeLog::default()
    }));
    let addr = serve_gateway(Arc::clone(&log)).await;
    let config = PublisherConfig {
        channel: "integration".into(),
        topic: String::new(),
        topic_text: "antstream-integration-gaps".into(),
        format: StreamFormat::SolarPunk,
        gateway: format!("http://{addr}"),
        batch_id: "ab".repeat(32),
        segment_ms: 2000,
        bitrate_kbps: 900,
        max_in_flight: 1,
        max_backlog: 2,
        playlist_window: 3,
        notes: String::new(),
        start_index: 0,
        reap_ms: 0,
    };
    let run = publisher::start(
        &tokio::runtime::Handle::current(),
        config,
        SECRET,
        owner(),
        None,
    )
    .unwrap();
    // Media segment `i` is filled with byte `i` (one chunk each), so a
    // listed reference reads back as its own number.
    let segment = |fill: u8| vec![fill; 4000];
    let _ = run.push(SegmentKind::Init, vec![0x11; 900], 0, false);
    let _ = run.push(SegmentKind::Media, segment(0), 2000, false);
    wait_for(&run, |s| s.feed_index >= 1).await;

    // The uplink stalls while 1-6 are captured: the backlog of 2 sheds.
    log.lock().unwrap().paused = true;
    let mut dropped_any = false;
    for i in 1..=6u8 {
        dropped_any |= run.push(SegmentKind::Media, segment(i), 2000, false)
            == publisher::PushOutcome::QueuedDroppingOldest;
    }
    assert!(dropped_any, "the burst must overflow the backlog");
    log.lock().unwrap().paused = false;
    wait_for(&run, |s| s.segments_published + s.segments_dropped >= 7).await;
    // 7's upload fails; 8-11 land one by one, so the window slides past
    // both holes an update at a time.
    let _ = run.push(SegmentKind::Media, vec![FAIL; 4000], 2000, false);
    wait_for(&run, |s| s.segments_failed == 1).await;
    for i in 8..=11u8 {
        let before = run.snapshot().feed_index;
        let _ = run.push(SegmentKind::Media, segment(i), 2000, false);
        wait_for(&run, |s| s.feed_index > before).await;
    }
    run.cancel();
    for _ in 0..400 {
        if run.is_finished() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(run.is_finished(), "publisher never finished");
    let report = run.report();
    assert_eq!(report.segments_failed, 1, "{report:?}");
    assert!(report.segments_dropped > 0, "{report:?}");
    let lost = report.segments_dropped + report.segments_failed;
    assert_eq!(report.segments_listed + lost, 12, "{report:?}");

    // Every update, read the way a viewer polls it.
    let topic = publisher::topic_from_string("antstream-integration-gaps");
    let owner_hex = hex::encode(owner());
    let mut numbers: HashMap<String, u64> = HashMap::new();
    let mut gaps_seen = std::collections::BTreeSet::new();
    for index in 0..report.feed_updates {
        let id = ant_retrieval::sequence_update_id(&topic, index);
        let (status, _, body) =
            http_get(&addr, &format!("/soc/{owner_hex}/{}", hex::encode(id))).await;
        assert_eq!(status, 200, "update {index}");
        let playlist = String::from_utf8(body).unwrap();
        assert!(
            !playlist.contains("#EXT-X-DISCONTINUITY"),
            "a hole is not a break:\n{playlist}"
        );
        let first: u64 = playlist
            .lines()
            .find_map(|l| l.strip_prefix("#EXT-X-MEDIA-SEQUENCE:"))
            .unwrap()
            .parse()
            .unwrap();
        let lines: Vec<&str> = playlist.lines().collect();
        let uris: Vec<&str> = lines
            .iter()
            .copied()
            .filter(|l| !l.starts_with('#'))
            .collect();
        assert!(!uris[0].starts_with("gap-"), "starts on a gap:\n{playlist}");
        assert!(
            !uris.last().unwrap().starts_with("gap-"),
            "ends on a gap:\n{playlist}"
        );
        for (i, uri) in uris.iter().enumerate() {
            let number = first + i as u64;
            let at = lines.iter().position(|l| l == uri).unwrap();
            if let Some(gap) = uri.strip_prefix("gap-") {
                // Solar Punk's shape: the tag, the date, the duration, a
                // placeholder named after its own sequence.
                assert_eq!(gap.parse::<u64>().unwrap(), number, "{playlist}");
                assert_eq!(lines[at - 3], "#EXT-X-GAP", "{playlist}");
                assert!(lines[at - 2].starts_with("#EXT-X-PROGRAM-DATE-TIME:"));
                assert_eq!(lines[at - 1], "#EXTINF:2.000,", "{playlist}");
                gaps_seen.insert(number);
            } else {
                let (status, _, data) = http_get(&addr, &format!("/bytes/{uri}")).await;
                assert_eq!(status, 200);
                assert_eq!(
                    u64::from(data[0]),
                    number,
                    "segment listed at the wrong number:\n{playlist}"
                );
            }
            let seen = *numbers.entry((*uri).to_string()).or_insert(number);
            assert_eq!(seen, number, "{uri} renumbered:\n{playlist}");
        }
        // The dates run straight through the gaps: one 2 s step per
        // entry, no reset.
        let dates: Vec<u64> = lines
            .iter()
            .filter_map(|l| l.strip_prefix("#EXT-X-PROGRAM-DATE-TIME:"))
            .map(parse_program_date_time)
            .collect();
        assert_eq!(dates.len(), uris.len(), "{playlist}");
        for pair in dates.windows(2) {
            assert_eq!(pair[1] - pair[0], 2000, "{playlist}");
        }
    }
    // Every lost segment was listed as a gap somewhere, the failed one
    // (number 7) among them.
    assert!(gaps_seen.contains(&7), "{gaps_seen:?}");
    assert_eq!(gaps_seen.len() as u64, lost, "{gaps_seen:?}");

    // Issue #168: the head is the recording, every entry of the
    // broadcast in one playlist although the window was 3 — segment 0 to
    // 11, each at the number the live updates gave it (checked above),
    // the holes as gap entries where they were.
    let head_id = ant_retrieval::sequence_update_id(&topic, report.feed_updates - 1);
    let (status, _, body) =
        http_get(&addr, &format!("/soc/{owner_hex}/{}", hex::encode(head_id))).await;
    assert_eq!(status, 200);
    let recording = String::from_utf8(body).unwrap();
    assert!(
        recording.contains("\n#EXT-X-PLAYLIST-TYPE:VOD\n"),
        "{recording}"
    );
    assert!(
        recording.contains("\n#EXT-X-MEDIA-SEQUENCE:0\n"),
        "{recording}"
    );
    assert!(recording.ends_with("#EXT-X-ENDLIST\n"), "{recording}");
    let entries: Vec<&str> = recording.lines().filter(|l| !l.starts_with('#')).collect();
    assert_eq!(entries.len(), 12, "{recording}");
    for (number, entry) in entries.iter().enumerate() {
        assert_eq!(numbers[*entry], number as u64, "{recording}");
    }
    assert_eq!(
        recording.matches("#EXT-X-GAP\n").count() as u64,
        lost,
        "{recording}"
    );
}

/// Read every feed update of `topic` the way a viewer polls it.
async fn feed_updates(addr: &str, topic: &[u8; 32], count: u64) -> Vec<String> {
    let owner_hex = hex::encode(owner());
    let mut updates = Vec::new();
    for index in 0..count {
        let id = ant_retrieval::sequence_update_id(topic, index);
        let (status, _, body) =
            http_get(addr, &format!("/soc/{owner_hex}/{}", hex::encode(id))).await;
        assert_eq!(status, 200, "update {index}");
        updates.push(String::from_utf8(body).unwrap());
    }
    updates
}

/// Issue #172, the acceptance case through the real gateway: the
/// network goes mid-broadcast while the camera keeps running, and comes
/// back. That is one feed (every update at the next index of the one
/// topic, none closed before the stop), and one recording at its head
/// that plays straight through: every segment that was sent, in order,
/// with a single `#EXT-X-DISCONTINUITY` at the seam, no gap entries for
/// the silence, and the dating re-anchored on the clock it came back at.
/// The outage here is 1.5 s of 100 ms segments rather than 30 s of 2 s
/// ones; the publisher treats both the same.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_outage_is_one_feed_and_one_recording_with_a_single_seam() {
    let log = Arc::new(Mutex::new(NodeLog::default()));
    let addr = serve_gateway(Arc::clone(&log)).await;
    let config: PublisherConfig = serde_json::from_value(serde_json::json!({
        "channel": "integration",
        "topic_text": "antstream-integration-outage",
        "format": "solar-punk",
        "gateway": format!("http://{addr}"),
        "batch_id": "ab".repeat(32),
        "segment_ms": 100,
        "reap_ms": 60_000,
    }))
    .unwrap();
    let run = publisher::start(
        &tokio::runtime::Handle::current(),
        config,
        SECRET,
        owner(),
        None,
    )
    .unwrap();
    let media = |fill: u8| vec![fill; 4000];
    let _ = run.push(SegmentKind::Init, vec![0x11; 900], 0, false);
    for i in 0..3u8 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = run.push(SegmentKind::Media, media(i), 100, false);
    }
    wait_for(&run, |s| s.segments_listed == 3).await;
    let before = run.snapshot();

    // The network goes; the node is told, and the stub node stops
    // answering as well, as a dead uplink would.
    run.pause();
    log.lock().unwrap().paused = true;
    let went = now_ms();
    for i in 3..18u8 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            run.push(SegmentKind::Media, media(i), 100, false),
            publisher::PushOutcome::Withheld,
        );
    }
    let during = run.snapshot();
    assert!(
        during.paused && during.running && !during.reaped,
        "{during:?}"
    );
    assert_eq!(during.feed_index, before.feed_index, "silence: no update");
    assert_eq!(during.segments_withheld, 15);

    // Back.
    log.lock().unwrap().paused = false;
    run.resume();
    let back = now_ms();
    for i in 18..21u8 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = run.push(SegmentKind::Media, media(i), 100, false);
    }
    wait_for(&run, |s| s.segments_listed == 6).await;
    run.cancel();
    for _ in 0..400 {
        if run.is_finished() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(run.is_finished(), "publisher never finished");
    let report = run.report();
    assert_eq!(report.segments_failed, 0, "{:?}", report.errors);
    assert_eq!(report.segments_listed, 6, "{report:?}");
    assert_eq!(report.segments_withheld, 15, "{report:?}");
    assert!(!report.reaped, "{report:?}");

    // One feed, closed only at the stop.
    let topic = publisher::topic_from_string("antstream-integration-outage");
    let updates = feed_updates(&addr, &topic, report.feed_updates).await;
    let (live, finals) = updates.split_at(updates.len() - 2);
    for playlist in live {
        assert!(!playlist.contains("#EXT-X-ENDLIST"), "{playlist}");
    }
    assert!(finals[0].ends_with("#EXT-X-ENDLIST\n"), "{}", finals[0]);
    let broadcast = |p: &str| {
        p.lines()
            .find(|l| l.starts_with("#ANTSTREAM-BROADCAST:"))
            .map(str::to_string)
    };
    for playlist in &updates {
        assert_eq!(broadcast(playlist), broadcast(&updates[0]), "one broadcast");
    }

    // One recording at the head, read the way a viewer reads it.
    let (status, _, head) = http_get(
        &addr,
        &format!("/feeds/{}/{}", hex::encode(owner()), hex::encode(topic)),
    )
    .await;
    assert_eq!(status, 200);
    let recording = String::from_utf8(head).unwrap();
    assert_eq!(recording, *updates.last().unwrap());
    assert!(
        recording.contains("#EXT-X-PLAYLIST-TYPE:VOD\n"),
        "{recording}"
    );
    assert_eq!(
        recording.matches("#EXT-X-DISCONTINUITY\n").count(),
        1,
        "{recording}"
    );
    assert!(!recording.contains("#EXT-X-GAP"), "{recording}");
    assert_eq!(recording.matches("#EXT-X-MAP:").count(), 1, "{recording}");
    let lines: Vec<&str> = recording.lines().collect();
    let uris: Vec<&str> = lines
        .iter()
        .copied()
        .filter(|l| !l.starts_with('#'))
        .collect();
    assert_eq!(uris.len(), 6, "{recording}");
    for (uri, fill) in uris.iter().zip([0u8, 1, 2, 18, 19, 20]) {
        let (status, _, data) = http_get(&addr, &format!("/bytes/{uri}")).await;
        assert_eq!((status, data), (200, media(fill)), "{recording}");
    }
    // The seam sits right in front of the first segment back, numbered
    // straight on from the last one before the silence.
    let seam = lines
        .iter()
        .position(|l| *l == "#EXT-X-DISCONTINUITY")
        .unwrap();
    assert_eq!(lines[seam + 3], uris[3], "{recording}");
    // Dated straight through, re-anchored at the return: the first
    // segment back starts after the silence, not 100 ms after the last
    // segment before it.
    let dates: Vec<u64> = lines
        .iter()
        .filter_map(|l| l.strip_prefix("#EXT-X-PROGRAM-DATE-TIME:"))
        .map(parse_program_date_time)
        .collect();
    assert_eq!(dates.len(), 6);
    for pair in dates.windows(2) {
        assert!(pair[1] >= pair[0], "never backwards: {dates:?}");
    }
    assert_eq!(dates[1] - dates[0], 100);
    assert_eq!(dates[5] - dates[4], 100);
    assert!(
        dates[3] >= dates[2] + (back - went) && dates[3] <= back + 400,
        "the seam is dated at the return: {dates:?}, went {went}, back {back}"
    );
}

/// Issue #172: with a reap window set, a broadcast whose host went quiet
/// (paused and never back) ends by itself one window after its last
/// segment, without the stop call, exactly as a stop ends it: the
/// closing window, then the recording at the feed's head.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broadcast_silent_past_its_reap_window_ends_itself() {
    let log = Arc::new(Mutex::new(NodeLog::default()));
    let addr = serve_gateway(Arc::clone(&log)).await;
    let config: PublisherConfig = serde_json::from_value(serde_json::json!({
        "channel": "integration",
        "topic_text": "antstream-integration-reap",
        "format": "solar-punk",
        "gateway": format!("http://{addr}"),
        "batch_id": "ab".repeat(32),
        "reap_ms": 500,
    }))
    .unwrap();
    let run = publisher::start(
        &tokio::runtime::Handle::current(),
        config,
        SECRET,
        owner(),
        None,
    )
    .unwrap();
    let _ = run.push(SegmentKind::Init, vec![0x11; 900], 0, false);
    let _ = run.push(SegmentKind::Media, vec![1; 4000], 2000, false);
    let _ = run.push(SegmentKind::Media, vec![2; 4000], 2000, false);
    wait_for(&run, |s| s.segments_listed == 2).await;
    run.pause();
    // No stop call: the reaper ends it.
    for _ in 0..200 {
        if run.is_finished() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(run.is_finished(), "never reaped: {:?}", run.snapshot());
    let report = run.report();
    assert!(report.reaped, "{report:?}");
    assert!(!report.recording_reference.is_empty(), "{report:?}");
    let topic = publisher::topic_from_string("antstream-integration-reap");
    let updates = feed_updates(&addr, &topic, report.feed_updates).await;
    let head = updates.last().unwrap();
    assert!(head.contains("#EXT-X-PLAYLIST-TYPE:VOD\n"), "{head}");
    assert_eq!(head.matches("#EXTINF").count(), 2, "{head}");
    assert!(updates[updates.len() - 2].ends_with("#EXT-X-ENDLIST\n"));
}
