//! Native driver for the **live publisher** (issue #212): the whole
//! broadcast loop of [`ant_ffi::publisher`] — segment uploads, playlist
//! uploads, feed updates, and whatever else the config turns on (ladder
//! markers, the recovery entry) — fed by a generator instead of a camera,
//! against a running gateway (`antd`, or bee).
//!
//! `publish_bench` (issue #67 stage 1) measures one `POST /bzz` per
//! segment and nothing else; this one measures what a viewer sees: how
//! often the feed moves on, how far behind live it is when it does, and
//! how many segments the publisher dropped to stay there.
//!
//! ```text
//! ANT_LIVE_GATEWAY=http://127.0.0.1:1833 ANT_LIVE_BATCH=0x… \
//! ANT_LIVE_BITRATE_KBPS=600 ANT_LIVE_DURATION_S=1800 \
//! ANT_LIVE_CONFIG='{"ladder":true}' ANT_LIVE_EVENTS=feed.csv \
//!   cargo run --release -p ant-ffi --example live_bench
//! ```
//!
//! Every run signs its feed with a fresh random key (a fresh channel, so
//! no run reads another's head) and uploads fresh random content.
//! `ANT_LIVE_CONFIG` is merged into the publisher config as given, so
//! any [`ant_ffi::publisher::PublisherConfig`] field can be set.
//! `ANT_LIVE_EVENTS` names a CSV the driver writes one line per landed
//! feed update to (`elapsed_ms,feed_index,segments_published,
//! segments_dropped,live_edge_lag_ms`).
//!
//! It reads the snapshot and the report as JSON rather than through
//! their Rust fields, so the same file builds against older publishers
//! (the paired v0.5.68 / v0.5.72 benchmark in issue #212).

use std::io::Write as _;
use std::time::{Duration, Instant};

use ant_ffi::publisher::{self, PublisherConfig, SegmentKind};
use serde_json::{json, Value};

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(default)
}

fn env_string(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn die(msg: &str) -> ! {
    eprintln!("live_bench: {msg}");
    std::process::exit(1);
}

/// xorshift64*: fresh, incompressible-enough segment bytes without a
/// syscall per byte.
struct Fill(u64);

impl Fill {
    fn bytes(&mut self, len: usize) -> Vec<u8> {
        let mut out = Vec::with_capacity(len + 8);
        while out.len() < len {
            self.0 ^= self.0 >> 12;
            self.0 ^= self.0 << 25;
            self.0 ^= self.0 >> 27;
            out.extend_from_slice(&self.0.wrapping_mul(0x2545_F491_4F6C_DD1D).to_le_bytes());
        }
        out.truncate(len);
        out
    }
}

fn percentile(sorted: &[u64], pct: usize) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = (sorted.len() * pct).div_ceil(100).max(1);
    sorted[rank.min(sorted.len()) - 1]
}

/// A snapshot/report field that is a number in one publisher version and
/// an optional number in another (`feed_index`).
fn num(value: &Value, key: &str) -> Option<u64> {
    value.get(key).and_then(Value::as_u64)
}

fn main() {
    let gateway =
        env_string("ANT_LIVE_GATEWAY").unwrap_or_else(|| "http://127.0.0.1:1833".to_string());
    let batch = env_string("ANT_LIVE_BATCH").unwrap_or_else(|| die("ANT_LIVE_BATCH is required"));
    let bitrate_kbps: u32 = env_or("ANT_LIVE_BITRATE_KBPS", 600);
    let segment_ms: u32 = env_or("ANT_LIVE_SEGMENT_MS", 2000);
    let duration_s: u64 = env_or("ANT_LIVE_DURATION_S", 1800);
    let label = env_string("ANT_LIVE_LABEL").unwrap_or_else(|| "live bench".to_string());

    let secret = ant_crypto::random_secp256k1_secret();
    let signing = k256::ecdsa::SigningKey::from_bytes((&secret).into())
        .unwrap_or_else(|e| die(&format!("signing key: {e}")));
    let owner = ant_crypto::ethereum_address_from_public_key(signing.verifying_key());
    let seed = u64::from_le_bytes(secret[..8].try_into().expect("8 bytes")) | 1;
    let mut fill = Fill(seed);

    let channel = format!("live-bench-{}", hex::encode(&owner[..4]));
    let mut config = json!({
        "channel": channel,
        "gateway": gateway,
        "batch_id": batch.trim_start_matches("0x"),
        "bitrate_kbps": bitrate_kbps,
        "segment_ms": segment_ms,
        "notes": label,
    });
    if let Some(extra) = env_string("ANT_LIVE_CONFIG") {
        let extra: Value =
            serde_json::from_str(&extra).unwrap_or_else(|e| die(&format!("ANT_LIVE_CONFIG: {e}")));
        let (Some(into), Some(from)) = (config.as_object_mut(), extra.as_object()) else {
            die("ANT_LIVE_CONFIG must be a JSON object");
        };
        for (key, value) in from {
            into.insert(key.clone(), value.clone());
        }
    }
    let config: PublisherConfig = serde_json::from_value(config.clone())
        .unwrap_or_else(|e| die(&format!("publisher config: {e}")));

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap_or_else(|e| die(&format!("runtime: {e}")));
    let run = publisher::start(runtime.handle(), config, secret, owner, None)
        .unwrap_or_else(|e| die(&format!("start: {e}")));
    eprintln!(
        "live_bench: {label}: owner 0x{}, {bitrate_kbps} kbit/s in {segment_ms} ms segments, \
         {duration_s} s",
        hex::encode(owner)
    );

    let mut events = env_string("ANT_LIVE_EVENTS")
        .map(|path| std::fs::File::create(&path).unwrap_or_else(|e| die(&format!("{path}: {e}"))));
    if let Some(file) = events.as_mut() {
        let _ = writeln!(
            file,
            "elapsed_ms,feed_index,segments_published,segments_dropped,live_edge_lag_ms"
        );
    }

    let segment_bytes =
        (u64::from(bitrate_kbps) * 1000 / 8 * u64::from(segment_ms) / 1000) as usize;
    let started = Instant::now();
    let _ = run.push(SegmentKind::Init, fill.bytes(1024), 0, false);
    let mut next_segment = 1u64;
    let mut last_index: Option<u64> = None;
    let mut update_at: Vec<u64> = Vec::new();
    let mut edge_lag: Vec<u64> = Vec::new();
    let mut last_print = Instant::now();
    let mut last_edge_sample = Instant::now();
    let end = started + Duration::from_secs(duration_s);
    while Instant::now() < end {
        // A segment is handed over when it is finished, on the wall clock.
        let due = started + Duration::from_millis(next_segment * u64::from(segment_ms));
        if Instant::now() >= due {
            let _ = run.push(
                SegmentKind::Media,
                fill.bytes(segment_bytes),
                segment_ms,
                false,
            );
            next_segment += 1;
        }
        let snapshot = serde_json::to_value(run.snapshot()).unwrap_or(Value::Null);
        let index = num(&snapshot, "feed_index");
        let elapsed = started.elapsed().as_millis() as u64;
        if index.is_some() && index != last_index {
            if last_index.is_some() {
                update_at.push(elapsed);
            }
            last_index = index;
            if let Some(file) = events.as_mut() {
                let _ = writeln!(
                    file,
                    "{elapsed},{},{},{},{}",
                    index.unwrap_or(0),
                    num(&snapshot, "segments_published").unwrap_or(0),
                    num(&snapshot, "segments_dropped").unwrap_or(0),
                    num(&snapshot, "lag_ms").unwrap_or(0),
                );
            }
        }
        if last_edge_sample.elapsed() >= Duration::from_secs(1) {
            last_edge_sample = Instant::now();
            if !update_at.is_empty() {
                edge_lag.push(num(&snapshot, "lag_ms").unwrap_or(0));
            }
        }
        if last_print.elapsed() >= Duration::from_secs(60) {
            last_print = Instant::now();
            eprintln!(
                "live_bench: {:>5}s published {} dropped {} failed {} feed {:?} lag {} ms errors {} last {}",
                elapsed / 1000,
                num(&snapshot, "segments_published").unwrap_or(0),
                num(&snapshot, "segments_dropped").unwrap_or(0),
                num(&snapshot, "segments_failed").unwrap_or(0),
                index,
                num(&snapshot, "lag_ms").unwrap_or(0),
                num(&snapshot, "error_count").unwrap_or(0),
                snapshot.get("last_error").and_then(Value::as_str).unwrap_or(""),
            );
            // Publishers that time their stages (issue #212) say where
            // the time went so far.
            if let Some(timings) = snapshot.get("timings").and_then(Value::as_object) {
                let stages: Vec<String> = timings
                    .iter()
                    .map(|(stage, t)| {
                        format!(
                            "{stage} {}/{}/{}",
                            num(t, "p50_ms").unwrap_or(0),
                            num(t, "p95_ms").unwrap_or(0),
                            num(t, "max_ms").unwrap_or(0),
                        )
                    })
                    .collect();
                eprintln!("live_bench:        p50/p95/max ms: {}", stages.join(", "));
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }

    run.cancel();
    let deadline = Instant::now() + Duration::from_secs(130);
    while !run.is_finished() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(100));
    }
    let report = serde_json::to_value(run.report()).unwrap_or(Value::Null);

    // The feed's pace while live: the gaps between consecutive landed
    // updates, first update excluded (it waits on the head read).
    let mut gaps: Vec<u64> = update_at.windows(2).map(|w| w[1] - w[0]).collect();
    gaps.sort_unstable();
    edge_lag.sort_unstable();
    let over = |limit: u64| gaps.iter().filter(|&&g| g > limit).count();
    let summary = json!({
        "label": label,
        "bitrate_kbps": bitrate_kbps,
        "segment_ms": segment_ms,
        "duration_s": duration_s,
        "feed_updates_live": update_at.len(),
        "feed_gap_ms_p50": percentile(&gaps, 50),
        "feed_gap_ms_p95": percentile(&gaps, 95),
        "feed_gap_ms_p99": percentile(&gaps, 99),
        "feed_gap_ms_max": gaps.last().copied().unwrap_or(0),
        "feed_gaps_over_4s": over(4000),
        "feed_gaps_over_10s": over(10_000),
        "live_edge_lag_ms_p50": percentile(&edge_lag, 50),
        "live_edge_lag_ms_p95": percentile(&edge_lag, 95),
        "live_edge_lag_ms_max": edge_lag.last().copied().unwrap_or(0),
        "finished": run.is_finished(),
        "report": report,
    });
    let r = &summary["report"];
    println!(
        "| {label} | {}/{} | {} | {} | {} / {} / {} | {} / {} / {} | {} / {} / {} |",
        num(r, "segments_published").unwrap_or(0),
        num(r, "segments_pushed").unwrap_or(0),
        num(r, "segments_dropped").unwrap_or(0),
        num(r, "segments_failed").unwrap_or(0),
        num(r, "lag_ms_p50").unwrap_or(0),
        num(r, "lag_ms_p95").unwrap_or(0),
        num(r, "lag_ms_max").unwrap_or(0),
        summary["feed_gap_ms_p50"],
        summary["feed_gap_ms_p95"],
        summary["feed_gap_ms_max"],
        num(r, "publish_ms_p50").unwrap_or(0),
        num(r, "publish_ms_p95").unwrap_or(0),
        num(r, "publish_ms_max").unwrap_or(0),
    );
    println!("JSON: {summary}");
    runtime.shutdown_timeout(Duration::from_secs(5));
}
