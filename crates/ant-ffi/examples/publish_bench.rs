//! Native driver for the `AntStream` publisher throughput benchmark
//! (issue #67 stage 1) — the same [`ant_ffi::bench`] core the iOS app
//! runs, wired to a terminal instead of a `SwiftUI` sheet.
//!
//! Two ways to point it at a gateway:
//!
//! * **Embedded node** (what the phone does): give it a data dir and it
//!   runs `ant_init` + `ant_start_gateway` itself, then publishes into
//!   its own in-process gateway.
//! * **External gateway**: set `ANT_BENCH_GATEWAY` to a running `antd`
//!   (or bee) and it skips the embedded node entirely — useful for a
//!   desktop reference number to compare the phone against.
//!
//! ```text
//! # Pre-network pipeline ceiling: no node, no batch, no network.
//! ANT_BENCH_LABEL="Linux x86_64 / no network" ANT_BENCH_DURATION_S=120 \
//!   cargo run --release -p ant-ffi --example publish_bench
//!
//! # Real publish against a running antd that has a usable batch.
//! ANT_BENCH_LABEL="Linux x86_64 / wired" \
//! ANT_BENCH_GATEWAY=http://127.0.0.1:1633 ANT_BENCH_BATCH=0x… \
//! ANT_BENCH_DURATION_S=1800 \
//!   cargo run --release -p ant-ffi --example publish_bench
//!
//! # Real publish through an embedded node (closest to the iOS shape).
//! ANT_BENCH_LABEL="Linux x86_64 / embedded" \
//! ANT_FFI_DATA_DIR=/path/with/funded/identity.json ANT_BENCH_BATCH=0x… \
//!   cargo run --release -p ant-ffi --example publish_bench
//! ```
//!
//! It prints the Markdown results row on stdout (paste-ready for
//! `crates/ant-ffi/ANTSTREAM_BENCH.md`) and the full JSON report after
//! it, so a CI job can archive the raw numbers.

use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;
use std::time::{Duration, Instant};

use ant_ffi::bench::{markdown_header, BenchConfig, BenchReport, BenchSnapshot};
use ant_ffi::{
    ant_bench_progress, ant_bench_start, ant_bench_stop, ant_free_string, ant_init, ant_peer_count,
    ant_shutdown, ant_start_gateway, AntHandle,
};

/// Loopback port the embedded-node mode binds its gateway on. Not 1633:
/// a developer box running this bench usually has `antd` already on the
/// bee default port, and a clash there would fail the run for a reason
/// that has nothing to do with throughput.
const EMBEDDED_GATEWAY: &str = "127.0.0.1:1733";

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_string(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| default.to_string())
}

fn die(msg: &str) -> ! {
    eprintln!("publish_bench: {msg}");
    std::process::exit(1);
}

/// Copy an FFI-owned C string into an owned `String` and free it.
fn take_string(ptr: *mut c_char) -> Option<String> {
    if ptr.is_null() {
        return None;
    }
    // SAFETY: `ptr` is a NUL-terminated string the FFI allocated and
    // handed to us; we own it and free it exactly once here.
    let owned = unsafe { CStr::from_ptr(ptr).to_string_lossy().into_owned() };
    unsafe { ant_free_string(ptr) };
    Some(owned)
}

fn cstring(s: &str) -> CString {
    CString::new(s).unwrap_or_else(|_| die("argument contains an interior NUL byte"))
}

fn main() {
    let batch = env_string("ANT_BENCH_BATCH", "");
    let external_gateway = env_string("ANT_BENCH_GATEWAY", "");
    let data_dir = env_string("ANT_FFI_DATA_DIR", "");

    // An embedded node is only worth starting when this run actually
    // publishes: the pipeline mode never opens a socket, and a real
    // publish needs *some* gateway.
    let embedded = !batch.is_empty() && external_gateway.is_empty();
    if embedded && data_dir.is_empty() {
        die(
            "publishing needs a gateway: set ANT_BENCH_GATEWAY=http://host:port, \
             or ANT_FFI_DATA_DIR=<dir> to run an embedded node",
        );
    }

    let config = BenchConfig {
        label: env_string("ANT_BENCH_LABEL", ""),
        bitrate_kbps: env_or("ANT_BENCH_BITRATE_KBPS", 3400),
        segment_ms: env_or("ANT_BENCH_SEGMENT_MS", 2000),
        duration_s: env_or("ANT_BENCH_DURATION_S", 1800),
        warmup_s: env_or("ANT_BENCH_WARMUP_S", 30),
        max_in_flight: env_or("ANT_BENCH_MAX_IN_FLIGHT", 4),
        gateway: if embedded {
            format!("http://{EMBEDDED_GATEWAY}")
        } else {
            env_string("ANT_BENCH_GATEWAY", "http://127.0.0.1:1633")
        },
        batch_id: batch,
        seed: env_or("ANT_BENCH_SEED", 0x414e_5453_5452_4d31_u64),
        notes: env_string("ANT_BENCH_NOTES", ""),
    };
    if config.label.is_empty() {
        die(
            "set ANT_BENCH_LABEL to the environment this run describes, \
             e.g. ANT_BENCH_LABEL='Linux x86_64 / wired' — it is the table's row key",
        );
    }

    // The bench core needs a node handle for its runtime and (in
    // publish mode) its peer-count samples. Even the pipeline mode runs
    // on one, so both modes share exactly one code path.
    let dir = if data_dir.is_empty() {
        let tmp = std::env::temp_dir().join(format!("ant-publish-bench-{}", std::process::id()));
        std::fs::create_dir_all(&tmp)
            .unwrap_or_else(|e| die(&format!("create {}: {e}", tmp.display())));
        tmp.to_string_lossy().into_owned()
    } else {
        data_dir
    };
    let c_dir = cstring(&dir);
    let mut err: *mut c_char = ptr::null_mut();
    // SAFETY: `c_dir` is a valid NUL-terminated string; `err` is a
    // writable slot. The handle is freed via `ant_shutdown` below.
    let handle: *mut AntHandle = unsafe { ant_init(c_dir.as_ptr(), &raw mut err) };
    if handle.is_null() {
        die(&take_string(err).unwrap_or_else(|| "ant_init returned null".into()));
    }
    println!("node up (data dir {dir})");

    let code = drive(handle, &config, embedded);

    // SAFETY: `handle` came from `ant_init` and has not been freed yet.
    unsafe { ant_shutdown(handle) };
    std::process::exit(code);
}

fn drive(handle: *mut AntHandle, config: &BenchConfig, embedded: bool) -> i32 {
    if embedded {
        let peer_threshold: i32 = env_or("ANT_PEER_THRESHOLD", 50);
        let peer_timeout = Duration::from_secs(env_or("ANT_PEER_TIMEOUT", 300));
        if !wait_for_peers(handle, peer_threshold, peer_timeout) {
            return 1;
        }
        let addr = cstring(EMBEDDED_GATEWAY);
        let rpc = cstring(&env_string("GNOSIS_RPC_URL", ""));
        let mut err: *mut c_char = ptr::null_mut();
        // SAFETY: valid handle + NUL-terminated strings + writable slot.
        let ok =
            unsafe { ant_start_gateway(handle, addr.as_ptr(), true, rpc.as_ptr(), &raw mut err) };
        if !ok {
            eprintln!(
                "ant_start_gateway failed: {}",
                take_string(err).unwrap_or_else(|| "unknown".into())
            );
            return 1;
        }
        println!("gateway up on http://{EMBEDDED_GATEWAY}");
    }

    let config_json = match serde_json::to_string(config) {
        Ok(j) => j,
        Err(e) => {
            eprintln!("serialize config: {e}");
            return 1;
        }
    };
    let c_config = cstring(&config_json);
    let mut err: *mut c_char = ptr::null_mut();
    // SAFETY: valid handle, NUL-terminated config, writable slot.
    let started = unsafe { ant_bench_start(handle, c_config.as_ptr(), &raw mut err) };
    if !started {
        eprintln!(
            "ant_bench_start failed: {}",
            take_string(err).unwrap_or_else(|| "unknown".into())
        );
        return 1;
    }
    println!(
        "bench running: {} mode, {} kbit/s in {} ms segments ({} B each), {} s",
        config.mode().as_str(),
        config.bitrate_kbps,
        config.segment_ms,
        config.segment_bytes(),
        config.duration_s,
    );

    // Follow the run so a 30-minute soak isn't a silent terminal.
    let deadline = Instant::now() + Duration::from_secs(config.duration_s + 120);
    loop {
        std::thread::sleep(Duration::from_secs(10));
        let mut err: *mut c_char = ptr::null_mut();
        // SAFETY: valid handle + writable slot.
        let raw = unsafe { ant_bench_progress(handle, &raw mut err) };
        let Some(json) = take_string(raw) else {
            eprintln!(
                "ant_bench_progress failed: {}",
                take_string(err).unwrap_or_else(|| "unknown".into())
            );
            break;
        };
        match serde_json::from_str::<BenchSnapshot>(&json) {
            Ok(s) => {
                println!(
                    "  t={:>6.0}s segments={} ok={} failed={} {:.2} Mbit/s {:.1} chunks/s lag={} ms peers={}",
                    s.elapsed_s,
                    s.segments_total,
                    s.segments_ok,
                    s.segments_failed,
                    s.sustained_mbit_s,
                    s.sustained_chunks_s,
                    s.lag_ms_last,
                    s.peers,
                );
                if !s.running {
                    break;
                }
            }
            Err(e) => eprintln!("  (unreadable progress: {e})"),
        }
        if Instant::now() > deadline {
            eprintln!("bench overran its own deadline; stopping");
            break;
        }
    }

    let mut err: *mut c_char = ptr::null_mut();
    // SAFETY: valid handle + writable slot. Blocking by contract.
    let raw = unsafe { ant_bench_stop(handle, &raw mut err) };
    let Some(json) = take_string(raw) else {
        eprintln!(
            "ant_bench_stop failed: {}",
            take_string(err).unwrap_or_else(|| "unknown".into())
        );
        return 1;
    };
    let Ok(report) = serde_json::from_str::<BenchReport>(&json) else {
        eprintln!("unreadable report: {json}");
        return 1;
    };

    println!("\n{}", markdown_header());
    println!("{}", report.markdown_row());
    println!("\nJSON: {json}");
    i32::from(!report.sustained)
}

fn wait_for_peers(handle: *mut AntHandle, threshold: i32, timeout: Duration) -> bool {
    let start = Instant::now();
    loop {
        // SAFETY: `handle` is a live handle from `ant_init`.
        let peers = unsafe { ant_peer_count(handle) };
        if peers >= threshold {
            println!("peer set ready: {peers} peers");
            return true;
        }
        if start.elapsed() > timeout {
            eprintln!("only {peers} peers after timeout (needed {threshold})");
            return false;
        }
        std::thread::sleep(Duration::from_secs(5));
    }
}
