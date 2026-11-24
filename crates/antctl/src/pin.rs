//! `antctl pin` — populate the daemon's local disk chunk cache so this
//! node's HTTP gateway resolves a previously-uploaded reference (or a
//! freshly-built collection manifest) immediately, without waiting for
//! bee-side neighbourhood replication.
//!
//! Two entry points:
//!
//! - [`run`] backs `antctl pin <reference> <file>`: re-splits a single
//!   source file, derives the matching mantaray manifest with the same
//!   writer the upload driver uses, and pins every chunk after asserting
//!   the derived root matches the user-supplied `<reference>`.
//! - [`run_collection`] backs `antctl pin-collection`: takes one or
//!   more `<path-in-manifest>=<local-file>` pairs, computes each
//!   file's data-tree root with the streaming splitter (no chunks
//!   dispatched — those are assumed already pinned), builds a
//!   *collection* mantaray pointing at all of them, and pins the
//!   collection's manifest chunks (typically ≤ 5). Returns the new
//!   bzz reference, which exposes every entry under its `path`.
//!
//! Re-uses [`ant_retrieval::StreamingSplitter`] (constant-memory
//! chunk tree) and [`ant_retrieval::build_single_file_manifest`] /
//! [`ant_retrieval::build_collection_manifest`] (the same manifest
//! writers the daemon uses), so derived addresses match `antctl
//! upload` byte-for-byte. Chunks travel to the daemon over the
//! control socket as [`Request::PutChunkLocal`]; the daemon
//! BMT-validates each wire and writes it straight to
//! [`ant_retrieval::DiskChunkCache`]. No postage stamp, no pushsync —
//! pinning is purely a local mirror.
//!
//! The CLI verifies the derived manifest root against the
//! user-supplied `<reference>` *after* every chunk has been
//! dispatched (single-file pin only; collections are freshly minted
//! so there's nothing to compare against by default — pass
//! `--expect <ref>` to assert). A mismatch surfaces as a hard error
//! at the end (the data chunks are still in the cache; LRU will
//! reclaim them eventually if the operator never pins a matching
//! file).
//!
//! Concurrency: 8 worker threads, each holding their own `UnixStream`.
//! The splitter is much faster than `SQLite` at this scale, so workers
//! are the bottleneck — but 8x parallelism is enough to pin a 6.7 GiB
//! file (≈ 1.76M chunks) in 9-10 minutes against the local disk-cache
//! writer. Collection pinning is single-threaded since the chunk
//! count is tiny (≤ 5).

use crate::{PinArgs, PinCollectionArgs};
use ant_control::{request_sync, Request, Response};
use ant_crypto::CHUNK_SIZE;
use ant_retrieval::{
    build_collection_manifest, build_single_file_manifest, IndexAnchor, ManifestFile, SplitChunk,
    StreamingSplitter,
};
use anyhow::{anyhow, bail, Context, Result};
use std::io::{BufReader, Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

const WORKER_COUNT: usize = 8;

/// Single-file pin. Splits the source, derives the matching
/// manifest, dispatches every chunk to the daemon's disk cache, and
/// verifies that the derived root matches the user-supplied
/// reference.
pub fn run(socket: &Path, args: PinArgs, json: bool) -> Result<()> {
    let target_addr = parse_reference(&args.reference)?;
    let abs_path = args
        .path
        .canonicalize()
        .with_context(|| format!("source path {}", args.path.display()))?;
    let metadata = std::fs::metadata(&abs_path)
        .with_context(|| format!("stat source {}", abs_path.display()))?;
    if !metadata.is_file() {
        bail!("not a regular file: {}", abs_path.display());
    }
    let file_size = metadata.len();

    let display_name = args.name.clone().unwrap_or_else(|| {
        abs_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("file")
            .to_string()
    });
    let content_type = args
        .content_type
        .clone()
        .unwrap_or_else(|| "application/octet-stream".to_string());

    let (tx, rx) = mpsc::sync_channel::<SplitChunk>(WORKER_COUNT * 8);
    let rx = Arc::new(Mutex::new(rx));
    let socket_path = socket.to_path_buf();
    let mut workers: Vec<JoinHandle<Result<u64>>> = Vec::with_capacity(WORKER_COUNT);
    for _ in 0..WORKER_COUNT {
        let rx = rx.clone();
        let sock = socket_path.clone();
        workers.push(std::thread::spawn(move || worker(rx, sock)));
    }

    let started = Instant::now();
    let data_root = match split_file(&abs_path, file_size, Some(&tx), Some(started)) {
        Ok(r) => r,
        Err(e) => {
            drop(tx);
            for w in workers {
                if let Ok(Err(werr)) = w.join() {
                    return Err(werr);
                }
            }
            return Err(e);
        }
    };

    let manifest_root = if args.raw {
        data_root
    } else {
        let result = build_single_file_manifest(&display_name, Some(&content_type), data_root)
            .map_err(|e| anyhow!("build manifest: {e}"))?;
        for chunk in result.chunks {
            tx.send(chunk).map_err(|_| anyhow!("pin worker died"))?;
        }
        result.root
    };

    drop(tx);
    let total_pinned = join_workers(workers)?;
    eprintln!();

    if manifest_root != target_addr {
        bail!(
            "manifest root mismatch: derived 0x{} but reference is 0x{}; check --name / --content-type / --raw match the upload",
            hex::encode(manifest_root),
            hex::encode(target_addr),
        );
    }

    report_single(
        json,
        manifest_root,
        total_pinned,
        file_size,
        started.elapsed().as_secs_f64(),
    );
    Ok(())
}

/// Build a *collection* mantaray that exposes every input file under
/// its in-manifest path, pin its chunks, and return the resulting
/// bzz reference. Each input is `<path-in-manifest>=<local-file>`;
/// the local file is *not* re-pinned (assume the operator already ran
/// `antctl pin` for it earlier — collections only add the manifest
/// node on top of pre-existing data trees).
///
/// `--index` (optional) is the manifest path that should resolve when
/// the user fetches `bzz://<root>/` with no trailing path
/// (`website-index-document` metadata).
pub fn run_collection(socket: &Path, args: PinCollectionArgs, json: bool) -> Result<()> {
    if args.entries.is_empty() {
        bail!("at least one --entry / positional <path>=<file> is required");
    }
    let content_type = args
        .content_type
        .clone()
        .unwrap_or_else(|| "application/octet-stream".to_string());

    let started = Instant::now();
    let mut manifest_files: Vec<ManifestFile> = Vec::with_capacity(args.entries.len());
    for entry in &args.entries {
        let (manifest_path, local_path) = parse_entry(entry)?;
        let abs = local_path
            .canonicalize()
            .with_context(|| format!("source path {}", local_path.display()))?;
        let metadata =
            std::fs::metadata(&abs).with_context(|| format!("stat source {}", abs.display()))?;
        if !metadata.is_file() {
            bail!("not a regular file: {}", abs.display());
        }
        eprintln!(
            "computing data root for {} ({} bytes)",
            manifest_path,
            metadata.len()
        );
        let data_ref = split_file(&abs, metadata.len(), None, None)?;
        eprintln!("  data root: 0x{}", hex::encode(data_ref));
        manifest_files.push(ManifestFile {
            path: manifest_path,
            content_type: Some(content_type.clone()),
            data_ref: data_ref.to_vec(),
        });
    }

    let result = build_collection_manifest(
        &manifest_files,
        args.index.as_deref(),
        IndexAnchor::ZeroEntry,
    )
    .map_err(|e| anyhow!("build collection manifest: {e}"))?;

    if let Some(expect_ref) = args.expect.as_ref() {
        let expected = parse_reference(expect_ref)?;
        if result.root != expected {
            bail!(
                "manifest root mismatch: derived 0x{} but --expect was 0x{}; check entry order / paths / --index / --content-type",
                hex::encode(result.root),
                hex::encode(expected),
            );
        }
    }

    // Collections produce a tiny number of chunks (≤ 5 for typical
    // single-level collections), so a sequential dispatch is fine —
    // no need for the 8-worker pool the file pin uses.
    let mut chunks_pinned = 0u64;
    for chunk in &result.chunks {
        let req = Request::PutChunkLocal {
            wire_hex: format!("0x{}", hex::encode(&chunk.wire)),
        };
        let resp = request_sync(socket, &req)
            .map_err(|e| anyhow!("pin chunk 0x{}: {e}", hex::encode(chunk.address)))?;
        match resp {
            Response::Ok { .. } => chunks_pinned += 1,
            Response::Error { message } => bail!("antd: {message}"),
            other => bail!("unexpected response: {other:?}"),
        }
    }

    let elapsed = started.elapsed().as_secs_f64();
    if json {
        let entries: Vec<_> = manifest_files
            .iter()
            .map(|f| {
                serde_json::json!({
                    "path": f.path,
                    "data_ref": format!("0x{}", hex::encode(&f.data_ref)),
                    "content_type": f.content_type,
                })
            })
            .collect();
        println!(
            "{}",
            serde_json::json!({
                "ok": true,
                "reference": format!("0x{}", hex::encode(result.root)),
                "chunks_pinned": chunks_pinned,
                "entries": entries,
                "index_document": args.index,
                "elapsed_seconds": elapsed,
            })
        );
    } else {
        println!(
            "collection manifest pinned: {chunks_pinned} chunks, {} entries, {elapsed:.1}s",
            manifest_files.len()
        );
        println!("  reference: 0x{}", hex::encode(result.root));
        if let Some(idx) = args.index.as_ref() {
            println!("  index:     {idx}");
        }
        println!("  entries:");
        for f in &manifest_files {
            println!("    /{}  ->  0x{}", f.path, hex::encode(&f.data_ref));
        }
    }
    Ok(())
}

/// Worker thread loop: pull `SplitChunk` from `rx`, send each as a
/// `PutChunkLocal` over a dedicated socket, count successes.
fn worker(rx: Arc<Mutex<Receiver<SplitChunk>>>, sock: PathBuf) -> Result<u64> {
    let mut count = 0u64;
    loop {
        let chunk = {
            let g = rx.lock().expect("pin rx mutex");
            match g.recv() {
                Ok(c) => c,
                Err(_) => break,
            }
        };
        let req = Request::PutChunkLocal {
            wire_hex: format!("0x{}", hex::encode(&chunk.wire)),
        };
        let resp = request_sync(&sock, &req)
            .map_err(|e| anyhow!("pin chunk 0x{}: {e}", hex::encode(chunk.address)))?;
        match resp {
            Response::Ok { .. } => count += 1,
            Response::Error { message } => bail!("antd: {message}"),
            other => bail!("unexpected response: {other:?}"),
        }
    }
    Ok(count)
}

/// Walk a file with the streaming splitter, optionally dispatching
/// every produced chunk to `tx`. When `tx` is `None` the splitter
/// runs in "compute-root only" mode (used by collection pinning,
/// where the chunks are assumed pre-pinned and we only need the
/// data-tree root). Constant memory regardless of file size.
fn split_file(
    abs_path: &Path,
    file_size: u64,
    tx: Option<&SyncSender<SplitChunk>>,
    started: Option<Instant>,
) -> Result<[u8; 32]> {
    let file =
        std::fs::File::open(abs_path).with_context(|| format!("open {}", abs_path.display()))?;
    // Buffered reads, not mmap: a file that shrinks under a mapping
    // faults the process (SIGBUS) instead of returning an error. Here
    // a file that ends before `file_size` is a short read and fails
    // the pin. If it changes in place, the derived chunk addresses
    // won't match the expected reference and we abort at the end.
    let mut reader = BufReader::with_capacity(64 * CHUNK_SIZE, file);
    let mut buf = vec![0u8; CHUNK_SIZE];
    let mut splitter = StreamingSplitter::new();
    let mut last_report = Instant::now();
    let mut cursor: usize = 0;
    let total = usize::try_from(file_size).context("file too large for this platform")?;
    while cursor < total {
        let end = (cursor + CHUNK_SIZE).min(total);
        let leaf = &mut buf[..end - cursor];
        if let Err(e) = reader.read_exact(leaf) {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                bail!(
                    "{} changed during the pin: expected {file_size} bytes, it ended before byte {end}",
                    abs_path.display(),
                );
            }
            return Err(e).with_context(|| format!("read {}", abs_path.display()));
        }
        for chunk in splitter.push_leaf(leaf) {
            if let Some(s) = tx {
                s.send(chunk).map_err(|_| anyhow!("pin worker died"))?;
            }
        }
        cursor = end;
        if last_report.elapsed() >= Duration::from_millis(500) {
            let pct = if total > 0 {
                (cursor as f64 / total as f64) * 100.0
            } else {
                100.0
            };
            let label = if tx.is_some() { "pin" } else { "scan" };
            let mibps = if let Some(t0) = started {
                cursor as f64 / t0.elapsed().as_secs_f64() / (1024.0 * 1024.0)
            } else {
                cursor as f64 / last_report.elapsed().as_secs_f64() / (1024.0 * 1024.0)
            };
            eprint!("\r{label}: {pct:6.2}%  {mibps:6.1} MiB/s  ");
            std::io::stderr().flush().ok();
            last_report = Instant::now();
        }
    }
    let (data_root, _bytes, tail) = splitter.finish();
    if let Some(s) = tx {
        for chunk in tail {
            s.send(chunk).map_err(|_| anyhow!("pin worker died"))?;
        }
    }
    if started.is_some() {
        eprintln!();
    }
    Ok(data_root)
}

fn join_workers(workers: Vec<JoinHandle<Result<u64>>>) -> Result<u64> {
    let mut total = 0u64;
    for w in workers {
        total += w.join().map_err(|_| anyhow!("worker panicked"))??;
    }
    Ok(total)
}

fn report_single(
    json: bool,
    manifest_root: [u8; 32],
    total_pinned: u64,
    file_size: u64,
    elapsed: f64,
) {
    if json {
        println!(
            "{}",
            serde_json::json!({
                "ok": true,
                "reference": format!("0x{}", hex::encode(manifest_root)),
                "chunks_pinned": total_pinned,
                "bytes": file_size,
                "elapsed_seconds": elapsed,
            })
        );
    } else {
        let mibps = file_size as f64 / elapsed / (1024.0 * 1024.0);
        println!(
            "pinned {total_pinned} chunks ({file_size} bytes) in {elapsed:.1}s ({mibps:.1} MiB/s)"
        );
        println!("  reference: 0x{}", hex::encode(manifest_root));
    }
}

/// Parse a `<path-in-manifest>=<local-file>` entry. The path may not
/// be empty and may not start with `/` (mantaray paths are
/// relative). The local file part is anything after the *first* `=`
/// — paths containing `=` are fine on the right-hand side.
fn parse_entry(s: &str) -> Result<(String, PathBuf)> {
    let (path, file) = s
        .split_once('=')
        .ok_or_else(|| anyhow!("entry must be <path>=<file>: {s}"))?;
    if path.is_empty() {
        bail!("entry path is empty: {s}");
    }
    if path.starts_with('/') {
        bail!("entry path must be relative (no leading '/'): {s}");
    }
    if file.is_empty() {
        bail!("entry file is empty: {s}");
    }
    Ok((path.to_string(), PathBuf::from(file)))
}

fn parse_reference(s: &str) -> Result<[u8; 32]> {
    let s = s
        .strip_prefix("bzz://")
        .or_else(|| s.strip_prefix("bytes://"))
        .unwrap_or(s);
    let s = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    if s.len() != 64 {
        bail!("reference must be 64 hex chars; got {}", s.len());
    }
    let mut out = [0u8; 32];
    hex::decode_to_slice(s, &mut out).map_err(|e| anyhow!("invalid hex: {e}"))?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A source that is shorter than the size taken from its metadata
    /// (it shrank between the check and the read) fails the pin with an
    /// error instead of slicing past the end of a mapping.
    #[test]
    fn split_file_errors_when_source_shrank() {
        let path = std::env::temp_dir().join(format!(
            "antctl-pin-shrink-{}-{:?}.bin",
            std::process::id(),
            std::thread::current().id(),
        ));
        let full = 3 * CHUNK_SIZE + 5;
        std::fs::write(&path, vec![7u8; full]).expect("write");
        let ok = split_file(&path, full as u64, None, None);
        assert!(ok.is_ok(), "intact file splits: {ok:?}");

        std::fs::File::options()
            .write(true)
            .open(&path)
            .expect("open")
            .set_len(CHUNK_SIZE as u64 + 10)
            .expect("truncate");
        let err = split_file(&path, full as u64, None, None).expect_err("shrunk source");
        assert!(
            err.to_string().contains("changed during the pin"),
            "{err:#}"
        );
        let _ = std::fs::remove_file(&path);
    }
}
