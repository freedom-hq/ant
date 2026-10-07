//! Chunk cache controls for hosts: see it, clear it, size it.
//!
//! The node keeps two chunk caches: the persistent `SQLite` one at
//! `<data_dir>/chunks.sqlite` ([`DiskChunkCache`], byte-capped) and an
//! in-memory LRU ([`ant_retrieval::InMemoryChunkCache`], slot-capped). Pinned chunks
//! (`Swarm-Pin: true` uploads, bee `/pins`) live in the same database
//! but outside the byte cap, so a host can't just delete the file; these
//! calls clear and resize the cache while leaving pins alone.
//!
//! The work is [`ant_retrieval::ChunkCaches`], shared with the gateway's
//! `/v0/cache` routes (`antd`'s HTTP API). Bee has no cache-clear
//! endpoint (only the offline `bee db nuke`); bee's cache figures are on
//! the gateway's `GET /debugstore`.

use std::ffi::c_char;
use std::path::Path;
use std::sync::Arc;

use ant_retrieval::clamp_capacity;
use ant_retrieval::{ChunkCaches, DiskChunkCache};
use serde::Deserialize;

use crate::{
    clear_out_err, null_handle, run_string_call, write_out_err, AntHandle, DISK_CACHE_MAX_BYTES,
};

/// Open `<data_dir>/chunks.sqlite` with the host's saved cap (clamped),
/// or [`DISK_CACHE_MAX_BYTES`] when it has none. A failed open is
/// non-fatal: the node still serves uploads and live retrievals, it just
/// doesn't amortise them.
pub fn open_disk_cache(data_dir: &Path, capacity: Option<u64>) -> Option<Arc<DiskChunkCache>> {
    let path = data_dir.join("chunks.sqlite");
    let max_bytes = capacity.map_or(DISK_CACHE_MAX_BYTES, clamp_capacity);
    match DiskChunkCache::open(&path, max_bytes) {
        Ok(c) => {
            tracing::info!(
                target: "ant-ffi",
                path = %path.display(),
                max_bytes,
                used_bytes = c.used_bytes(),
                "opened persistent chunk cache",
            );
            Some(Arc::new(c))
        }
        Err(e) => {
            tracing::warn!(
                target: "ant-ffi",
                path = %path.display(),
                "failed to open persistent chunk cache: {e}; falling back to in-memory only",
            );
            None
        }
    }
}

fn caches(h: &AntHandle) -> ChunkCaches {
    ChunkCaches {
        disk: h.disk_cache.clone(),
        memory: h.memory_cache.clone(),
    }
}

fn status_string(h: &AntHandle) -> Result<String, String> {
    serde_json::to_string(&caches(h).status_json())
        .map_err(|e| format!("serialize cache status: {e}"))
}

/// Chunk cache figures, as JSON:
///
/// ```json
/// {"disk_enabled":true,"used_bytes":0,"capacity_bytes":0,"pinned_bytes":0,
///  "chunks":0,"pinned_chunks":0,"file_bytes":0,
///  "memory_chunks":0,"memory_capacity_chunks":8192}
/// ```
///
/// * `used_bytes` / `chunks`: unpinned cached chunks, the ones counted
///   against `capacity_bytes` and evicted oldest-first past it.
/// * `pinned_bytes` / `pinned_chunks`: pinned chunks, outside the cap.
/// * `file_bytes`: `chunks.sqlite` plus its `-wal` / `-shm` on disk.
/// * `memory_chunks` / `memory_capacity_chunks`: the in-memory cache.
/// * `disk_enabled`: `false` when `chunks.sqlite` couldn't be opened at
///   init; every disk figure is then `0`.
///
/// Reads counters, never the database: cheap enough to poll every few
/// seconds. Right after `ant_init` on a large cache the disk counters
/// read `0` until a background count finishes (seconds).
///
/// # Safety
///
/// * `handle` must come from [`crate::ant_init`] and must not have been
///   passed to [`crate::ant_shutdown`].
/// * `out_err` must point at a writable `*mut c_char` slot, or be null.
#[no_mangle]
pub unsafe extern "C" fn ant_cache_status(
    handle: *const AntHandle,
    out_err: *mut *mut c_char,
) -> *mut c_char {
    unsafe {
        run_string_call(out_err, "ant_cache_status", || {
            let h = handle.as_ref().ok_or_else(null_handle)?;
            status_string(h)
        })
    }
}

/// Remove every unpinned chunk from the disk cache, empty the in-memory
/// cache, and give the space back to the OS. Never touches pinned
/// chunks or pin bookkeeping. Blocks until done (typically well under
/// a second; longer for a multi-GB cache) — call it off the UI thread.
///
/// Safe while downloads and uploads run: a read racing the clear gets
/// the chunk or a cache miss (and then fetches it from the network);
/// cache writes queue behind the clear and land after it. Uploaded
/// chunks that aren't pinned are cache entries like any other, so an
/// upload's later self-heal re-reads its source file for them.
///
/// Returns JSON:
///
/// ```json
/// {"freed_bytes":0,"removed_chunks":0,"file_bytes_before":0,
///  "file_bytes_after":0,"memory_chunks_removed":0,"status":{...}}
/// ```
///
/// `freed_bytes` is the chunk bytes removed (what `used_bytes` dropped
/// by); `file_bytes_before - file_bytes_after` is what the files on disk
/// shrank by. Giving space back is best-effort: a database from an older
/// build is only rebuilt (once) when the pinned data left is small and
/// the disk has room, so the shrink can be 0 on a successful clear (see
/// `DiskChunkCache::clear_unpinned`). `status` is `ant_cache_status` after the clear.
///
/// # Safety
///
/// * `handle` must come from [`crate::ant_init`] and must not have been
///   passed to [`crate::ant_shutdown`].
/// * `out_err` must point at a writable `*mut c_char` slot, or be null.
#[no_mangle]
pub unsafe extern "C" fn ant_cache_clear(
    handle: *const AntHandle,
    out_err: *mut *mut c_char,
) -> *mut c_char {
    unsafe {
        run_string_call(out_err, "ant_cache_clear", || {
            let h = handle.as_ref().ok_or_else(null_handle)?;
            let cleared = h
                .runtime
                .block_on(caches(h).clear())
                .map_err(|e| format!("clear disk cache: {e}"))?;
            serde_json::to_string(&cleared).map_err(|e| format!("serialize cache clear: {e}"))
        })
    }
}

/// Set the disk cache cap at runtime. `bytes` is clamped to
/// [`ant_retrieval::CACHE_CAPACITY_MIN_BYTES`] (64 MiB) ..=
/// [`ant_retrieval::CACHE_CAPACITY_MAX_BYTES`]
/// (16 GiB); `ant_cache_status`'s `capacity_bytes` shows the value
/// applied. Lowering it below `used_bytes` evicts oldest-first down to
/// ~95% of the new cap before returning and gives the space back to the
/// OS; pinned chunks are never evicted and don't count. Blocks until
/// done — call it off the UI thread.
///
/// Not persisted: pass the saved choice to [`crate::ant_init_with_config`]
/// (`cache_capacity_bytes`) at the next start.
///
/// Returns `0` on success, `-1` on a null handle, `-2` if the disk cache
/// isn't available (failed to open at init) or the eviction failed; an
/// allocated error string is written into `*out_err` (free with
/// [`crate::ant_free_string`]). On an eviction failure the new cap is
/// still applied (`capacity_bytes` shows it), part of the eviction may
/// have committed, and the next cache write evicts down to it again, so
/// a host shouldn't revert its saved setting on `-2`. Giving file space
/// back is best-effort and never makes this fail.
///
/// # Safety
///
/// * `handle` must come from [`crate::ant_init`] and must not have been
///   passed to [`crate::ant_shutdown`].
/// * `out_err` must point at a writable `*mut c_char` slot, or be null.
#[no_mangle]
pub unsafe extern "C" fn ant_cache_set_capacity(
    handle: *const AntHandle,
    bytes: u64,
    out_err: *mut *mut c_char,
) -> i32 {
    unsafe {
        clear_out_err(out_err);
        let Some(h) = handle.as_ref() else {
            write_out_err(out_err, "ant_cache_set_capacity: null handle");
            return -1;
        };
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            h.runtime
                .block_on(caches(h).set_capacity(bytes))
                .map(drop)
                .map_err(|e| e.to_string())
        }));
        match result {
            Ok(Ok(())) => 0,
            Ok(Err(e)) => {
                write_out_err(out_err, &e);
                -2
            }
            Err(_) => {
                write_out_err(out_err, "panic in ant_cache_set_capacity");
                -2
            }
        }
    }
}

/// `ant_init_with_config`'s options document. Every key is optional;
/// unknown keys are rejected so a typo doesn't silently fall back to a
/// default.
#[derive(Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct InitConfig {
    /// `ant_init_with_options`'s `source_root`.
    pub source_root: Option<String>,
    /// `ant_init_with_identity`'s `identity_json`, as a string. Absent
    /// or null: the data dir's `identity.json`, as `ant_init` does.
    pub identity_json: Option<String>,
    /// Disk chunk cache cap, clamped like `ant_cache_set_capacity`.
    /// Absent or null: the 512 MiB default.
    pub cache_capacity_bytes: Option<u64>,
}

impl InitConfig {
    /// Parse the options JSON; a null / empty / whitespace string is the
    /// all-defaults config.
    pub fn parse(json: Option<&str>) -> Result<Self, String> {
        match json.map(str::trim) {
            None | Some("") => Ok(Self::default()),
            Some(s) => serde_json::from_str(s).map_err(|e| format!("invalid init config: {e}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ant_free_string, tests::cache_test_handle};
    use ant_retrieval::{CACHE_CAPACITY_MAX_BYTES, CACHE_CAPACITY_MIN_BYTES};
    use std::ffi::CStr;

    fn take_string(raw: *mut c_char) -> serde_json::Value {
        assert!(!raw.is_null());
        let s = unsafe { CStr::from_ptr(raw) }.to_str().unwrap().to_string();
        unsafe { ant_free_string(raw) };
        serde_json::from_str(&s).unwrap()
    }

    fn status(h: *const AntHandle) -> serde_json::Value {
        let mut err = std::ptr::null_mut();
        take_string(unsafe { ant_cache_status(h, &raw mut err) })
    }

    /// `n` distinct 4104-byte (span + 4 KiB) rows. The disk cache
    /// trusts stored bytes (bee's chunkstore contract), so these needn't
    /// be valid CACs — and skipping the BMT hash keeps the 80 MiB test
    /// below fast in a debug build.
    fn chunks(tag: u8, n: u32) -> Vec<([u8; 32], Vec<u8>)> {
        (0..n)
            .map(|i| {
                let mut addr = [tag; 32];
                addr[..4].copy_from_slice(&i.to_le_bytes());
                let mut wire = vec![tag; 4104];
                wire[..4].copy_from_slice(&i.to_le_bytes());
                (addr, wire)
            })
            .collect()
    }

    /// Status, clear and set-capacity through the real C entry points
    /// on a handle with a real disk + memory cache.
    #[test]
    fn cache_status_clear_and_capacity_through_the_c_api() {
        let (handle, dir) = cache_test_handle("cache-api", Some(256 * 1024 * 1024));
        let h = unsafe { &*handle };
        let disk = h.disk_cache.clone().unwrap();
        let unpinned = chunks(1, 2000);
        let pinned = chunks(2, 6);
        h.runtime.block_on(async {
            for b in unpinned.chunks(250) {
                disk.put_batch(b.to_vec()).await.unwrap();
            }
            disk.pin_collection(pinned[0].0.to_vec(), pinned.clone())
                .await
                .unwrap();
        });
        for (a, w) in unpinned.iter().take(100) {
            h.memory_cache.put(*a, w.clone());
        }

        let before = status(handle);
        assert_eq!(before["disk_enabled"], true);
        assert_eq!(before["capacity_bytes"], 256 * 1024 * 1024);
        assert_eq!(before["chunks"], 2000);
        assert_eq!(before["pinned_chunks"], 6);
        assert_eq!(before["pinned_bytes"], 6 * 4104);
        assert_eq!(before["used_bytes"], 2000 * 4104);
        assert_eq!(before["memory_chunks"], 100);
        assert_eq!(before["memory_capacity_chunks"], 8192);
        assert!(before["file_bytes"].as_u64().unwrap() > 2000 * 4104);

        let mut err = std::ptr::null_mut();
        let cleared = take_string(unsafe { ant_cache_clear(handle, &raw mut err) });
        assert_eq!(cleared["freed_bytes"], before["used_bytes"]);
        assert_eq!(cleared["removed_chunks"], 2000);
        assert_eq!(cleared["memory_chunks_removed"], 100);
        assert!(
            cleared["file_bytes_after"].as_u64().unwrap()
                < cleared["file_bytes_before"].as_u64().unwrap() / 10
        );
        let after = &cleared["status"];
        assert_eq!(after["chunks"], 0);
        assert_eq!(after["used_bytes"], 0);
        assert_eq!(after["pinned_chunks"], 6);
        assert_eq!(after["pinned_bytes"], before["pinned_bytes"]);
        assert_eq!(after["memory_chunks"], 0);
        assert_eq!(after["file_bytes"], cleared["file_bytes_after"]);
        assert_eq!(&status(handle), after, "status agrees with the clear's");
        h.runtime.block_on(async {
            for (a, w) in &pinned {
                assert_eq!(disk.get(*a).await.unwrap().as_deref(), Some(w.as_slice()));
            }
            assert!(disk.get(unpinned[0].0).await.unwrap().is_none());
            assert_eq!(disk.list_pins().await.unwrap(), vec![pinned[0].0.to_vec()]);
        });

        // Capacity: clamped both ways, applied at once.
        assert_eq!(
            unsafe { ant_cache_set_capacity(handle, 1, &raw mut err) },
            0
        );
        assert_eq!(status(handle)["capacity_bytes"], CACHE_CAPACITY_MIN_BYTES);
        assert_eq!(
            unsafe { ant_cache_set_capacity(handle, u64::MAX, &raw mut err) },
            0
        );
        assert_eq!(status(handle)["capacity_bytes"], CACHE_CAPACITY_MAX_BYTES);
        assert_eq!(
            unsafe { ant_cache_set_capacity(std::ptr::null(), 1, &raw mut err) },
            -1
        );
        unsafe { ant_free_string(err) };

        unsafe { crate::ant_shutdown(handle) };
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Lowering the cap through the C API evicts down to it before the
    /// call returns.
    #[test]
    fn set_capacity_evicts_down_through_the_c_api() {
        let (handle, dir) = cache_test_handle("cache-cap", None);
        let h = unsafe { &*handle };
        assert_eq!(status(handle)["capacity_bytes"], DISK_CACHE_MAX_BYTES);
        let disk = h.disk_cache.clone().unwrap();
        // ~80 MiB of unpinned chunks under the 512 MiB default.
        let many = chunks(3, 20_000);
        h.runtime.block_on(async {
            for b in many.chunks(1000) {
                disk.put_batch(b.to_vec()).await.unwrap();
            }
        });
        assert!(status(handle)["used_bytes"].as_u64().unwrap() > CACHE_CAPACITY_MIN_BYTES);
        let mut err = std::ptr::null_mut();
        assert_eq!(
            unsafe { ant_cache_set_capacity(handle, CACHE_CAPACITY_MIN_BYTES, &raw mut err) },
            0
        );
        let s = status(handle);
        assert_eq!(s["capacity_bytes"], CACHE_CAPACITY_MIN_BYTES);
        assert!(s["used_bytes"].as_u64().unwrap() <= CACHE_CAPACITY_MIN_BYTES);
        assert_eq!(
            s["chunks"].as_u64().unwrap() * 4104,
            s["used_bytes"].as_u64().unwrap(),
            "row and byte counters agree"
        );
        unsafe { crate::ant_shutdown(handle) };
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn init_config_parses_and_clamps() {
        assert_eq!(InitConfig::parse(None).unwrap(), InitConfig::default());
        assert_eq!(
            InitConfig::parse(Some("  ")).unwrap(),
            InitConfig::default()
        );
        let c = InitConfig::parse(Some(
            r#"{"source_root":"/x","identity_json":"{}","cache_capacity_bytes":1073741824}"#,
        ))
        .unwrap();
        assert_eq!(c.source_root.as_deref(), Some("/x"));
        assert_eq!(c.identity_json.as_deref(), Some("{}"));
        assert_eq!(c.cache_capacity_bytes, Some(1 << 30));
        assert!(
            InitConfig::parse(Some(r#"{"cache_capacity":1}"#)).is_err(),
            "typo rejected"
        );
        assert!(InitConfig::parse(Some("{")).is_err());
        assert_eq!(clamp_capacity(0), CACHE_CAPACITY_MIN_BYTES);
        assert_eq!(clamp_capacity(1 << 30), 1 << 30);
        assert_eq!(clamp_capacity(u64::MAX), CACHE_CAPACITY_MAX_BYTES);

        // The saved cap applies at open, clamped.
        let dir = std::env::temp_dir().join(format!("ant-ffi-open-cap-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let c = open_disk_cache(&dir, Some(1)).unwrap();
        assert_eq!(c.capacity_bytes(), CACHE_CAPACITY_MIN_BYTES);
        drop(c);
        let c = open_disk_cache(&dir, Some(2 << 30)).unwrap();
        assert_eq!(c.capacity_bytes(), 2 << 30);
        drop(c);
        let c = open_disk_cache(&dir, None).unwrap();
        assert_eq!(c.capacity_bytes(), DISK_CACHE_MAX_BYTES);
        drop(c);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
