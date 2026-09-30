//! The stage-2 live publisher against the **real** `ant-gateway`.
//!
//! The unit tests in `src/publisher.rs` drive a stub that speaks bee's
//! wire contract; this one drives the router the iOS app actually runs
//! (`ant_start_gateway` → `ant_gateway`), in `light_mode`, with a stub
//! node loop standing in for the swarm. That covers the half a stub
//! gateway cannot: whether `POST /bzz`, `POST /feeds` and `POST /soc`
//! *as this publisher writes them* are accepted by ant's own handlers —
//! header names, the `?sig=` query, the inner-CAC body shape, and the
//! SOC signature ant validates with `soc_valid` before it will dispatch
//! anything.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use ant_control::{ControlAck, ControlCommand, StatusSnapshot};
use ant_ffi::publisher::{self, PublisherConfig, SegmentKind};
use ant_gateway::{CorsConfig, GatewayChainState, GatewayHandle, GatewayIdentity, TagRegistry};
use tokio::sync::{mpsc, watch};

const SECRET: [u8; 32] = [0x5e; 32];

/// What the stub node loop was asked to push.
#[derive(Default)]
struct NodeLog {
    chunks: usize,
    socs: Vec<([u8; 32], Vec<u8>)>,
    /// SOC wires that failed ant's own validator. Must stay empty: the
    /// gateway rejects those with 401 before they reach the node, so an
    /// entry here would mean the gateway let a bad SOC through.
    invalid_socs: usize,
}

/// Answer the upload commands the gateway dispatches, the way a healthy
/// node would: ack each chunk/SOC with its own address as the reference.
async fn stub_node(mut rx: mpsc::Receiver<ControlCommand>, log: Arc<Mutex<NodeLog>>) {
    while let Some(cmd) = rx.recv().await {
        match cmd {
            ControlCommand::PushChunk { wire, ack, .. } => {
                let Some(address) = bmt_address(&wire) else {
                    let _ = ack.send(ControlAck::Error {
                        message: "unsplittable chunk".into(),
                    });
                    continue;
                };
                log.lock().unwrap().chunks += 1;
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
                    log.socs.push((address, wire));
                }
                let _ = ack.send(ControlAck::ChunkUploaded {
                    reference: hex::encode(address),
                });
            }
            other => drop(other),
        }
    }
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_broadcast_is_accepted_end_to_end_by_the_real_gateway() {
    let log = Arc::new(Mutex::new(NodeLog::default()));
    let addr = serve_gateway(Arc::clone(&log)).await;

    let config = PublisherConfig {
        channel: "integration".into(),
        topic: "33".repeat(32),
        gateway: format!("http://{addr}"),
        batch_id: "ab".repeat(32),
        segment_ms: 2000,
        bitrate_kbps: 900,
        max_in_flight: 4,
        max_backlog: 4,
        playlist_window: 6,
        notes: String::new(),
    };
    let run = publisher::start(
        &tokio::runtime::Handle::current(),
        config,
        SECRET,
        owner(),
        None,
    )
    .unwrap();

    let _ = run.push(SegmentKind::Init, vec![0x11; 900], 0, false);
    for i in 0..3u8 {
        let _ = run.push(SegmentKind::Media, vec![i; 40_000], 2000, false);
        tokio::time::sleep(Duration::from_millis(50)).await;
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
    assert_eq!(report.segments_failed, 0, "{:?}", report.errors);
    assert_eq!(report.segments_published, 3, "{report:?}");
    assert!(report.playlists_published > 0, "{report:?}");
    assert!(report.feed_updates > 0, "{report:?}");
    assert!(
        !report.channel_reference.is_empty(),
        "POST /feeds must yield a shareable channel reference: {report:?}",
    );
    assert!(report.kept_up, "{report:?}");

    let log = log.lock().unwrap();
    assert_eq!(log.invalid_socs, 0, "gateway dispatched an invalid SOC");
    // Every feed update reached the node as a single-owner chunk at the
    // bee sequence address for its index. (The node also sees the
    // dispersed-replica SOCs bee's default redundancy level mints for
    // every upload, so match by address rather than by count.)
    let feed = ant_retrieval::Feed {
        owner: owner(),
        topic: [0x33; 32],
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
