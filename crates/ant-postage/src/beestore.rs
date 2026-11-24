//! Recover per-bucket stamp-issuer counters from a bee `stamperstore`
//! (PLAN.md "node-owned on-chain state recovery", Feature 1, option a).
//!
//! When a node is started on a data dir carried over from bee, the
//! on-chain batch is rediscoverable over RPC (see
//! `ant_chain::discover`), but the *local* stamp-issuer state — the
//! per-bucket counters across the `2^bucketDepth` collision buckets —
//! is never on-chain. Re-registering a batch with all-zero counters
//! would re-stamp `(batchId, bucket, index)` slots the network has
//! already seen, producing invalid/overwriting stamps. To avoid that we
//! read the counters bee persisted.
//!
//! Bee keeps them in `<data-dir>/stamperstore`, a `LevelDB`, in two forms
//! (see bee `pkg/postage/stampissuer.go`):
//!
//! - `StampIssuerItem`: key `"StampIssuerItem/" ++ raw32(batchID)`,
//!   value = msgpack of `stampIssuerData` (includes `buckets`). Flushed
//!   periodically and on clean shutdown.
//! - `StampItem`: key `"stampItem/" ++ raw32(batchID) ++ "/" ++
//!   hex(chunkAddr)`, value = `batchID(32) ++ chunkAddr(32) ++
//!   batchIndex(8) ++ timestamp(8) ++ 0`. One per issued stamp; bee
//!   rebuilds bucket counts from these after an unclean shutdown.
//!
//! [`recover_bee_buckets`] reads the `StampIssuerItem` for a batch and
//! then folds in the `StampItem` records (taking, per bucket, the
//! max of the stored count and the highest issued index + 1), matching
//! bee's own `recoverBuckets` so the result is correct even if bee
//! exited uncleanly — including before it ever wrote a
//! `StampIssuerItem`. It reads a private copy of the store, never bee's
//! files in place, and clamps every counter at the batch's capacity.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use rusty_leveldb::{LdbIterator, Options, DB};
use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BeeRecoverError {
    #[error("open bee stamperstore: {0}")]
    Open(String),
    #[error("read bee stamperstore: {0}")]
    Read(String),
    #[error("decode bee stamp issuer: {0}")]
    Decode(String),
}

/// The bucket state recovered for one batch from a bee `stamperstore`.
#[derive(Debug, Clone)]
pub struct RecoveredIssuer {
    /// Bee's recorded batch depth, or the caller's when bee left no
    /// `StampIssuerItem` for the batch.
    pub batch_depth: u8,
    /// Bee's recorded bucket depth, or the caller's when bee left no
    /// `StampIssuerItem` for the batch. Callers must compare it with the
    /// batch's on-chain bucket depth before using `buckets`.
    pub bucket_depth: u8,
    /// Per-bucket issued counters, length `2^bucket_depth`, each at most
    /// the bucket capacity `2^(batch_depth - bucket_depth)` for the
    /// caller's `batch_depth`.
    pub buckets: Vec<u32>,
}

/// Subset of bee's `stampIssuerData` we need. Bee marshals the struct
/// with `vmihailenco/msgpack` and field tags, so it lands as a msgpack
/// map keyed by these names; serde ignores the other keys (`label`,
/// `keyID`, `batchID`, `batchAmount`, `maxBucketCount`, `blockNumber`,
/// `immutableFlag`).
#[derive(Debug, Deserialize)]
struct BeeStampIssuerData {
    #[serde(rename = "batchDepth")]
    batch_depth: u8,
    #[serde(rename = "bucketDepth")]
    bucket_depth: u8,
    #[serde(rename = "buckets")]
    buckets: Vec<u32>,
}

const STAMP_ISSUER_NS: &[u8] = b"StampIssuerItem/";
const STAMP_ITEM_NS: &[u8] = b"stampItem/";

/// Recover the bucket counters bee persisted for `batch_id` at
/// `stamperstore_path`.
///
/// `batch_depth` and `bucket_depth` are the batch's on-chain depths.
/// They shape the result when bee left `stampItem/` records but no
/// `StampIssuerItem` for the batch (an unclean exit before the first
/// snapshot), and every counter is clamped at the bucket capacity they
/// imply, so a corrupt or foreign store can't seed a counter past what
/// the batch can issue. Returns `Ok(None)` when bee left neither record
/// for the batch, so the caller falls back to a fresh (all-zero) issuer.
///
/// Bee's store is never opened in place: `LevelDB` writes `LOCK`/`LOG`
/// files and may compact or rewrite the manifest on open, even when only
/// reading. The store's files are copied to a private temporary directory
/// (removed on return) and the copy is opened instead.
pub fn recover_bee_buckets(
    stamperstore_path: &Path,
    batch_id: &[u8; 32],
    batch_depth: u8,
    bucket_depth: u8,
) -> Result<Option<RecoveredIssuer>, BeeRecoverError> {
    if !stamperstore_path.exists() {
        return Ok(None);
    }
    let copy = TempCopy::of(stamperstore_path)?;
    // Never create structure for a missing store: a bee data-dir without
    // a stamperstore just means "nothing to recover".
    let opt = Options {
        create_if_missing: false,
        ..Options::default()
    };
    let mut db = DB::open(&copy.0, opt).map_err(|e| BeeRecoverError::Open(e.to_string()))?;

    let mut issuer_key = Vec::with_capacity(STAMP_ISSUER_NS.len() + 32);
    issuer_key.extend_from_slice(STAMP_ISSUER_NS);
    issuer_key.extend_from_slice(batch_id);

    let issuer_item = db.get(&issuer_key);
    let found_issuer = issuer_item.is_some();
    let mut recovered = match issuer_item {
        Some(raw) => {
            let data: BeeStampIssuerData =
                rmp_serde::from_slice(&raw).map_err(|e| BeeRecoverError::Decode(e.to_string()))?;
            RecoveredIssuer {
                batch_depth: data.batch_depth,
                bucket_depth: data.bucket_depth,
                buckets: data.buckets,
            }
        }
        None => RecoveredIssuer {
            batch_depth,
            bucket_depth,
            buckets: Vec::new(),
        },
    };
    if recovered.bucket_depth == 0 || recovered.bucket_depth > crate::MAX_BUCKET_DEPTH {
        return Err(BeeRecoverError::Decode(format!(
            "bucket depth {} out of range 1..={}",
            recovered.bucket_depth,
            crate::MAX_BUCKET_DEPTH
        )));
    }
    let n = 1usize << u32::from(recovered.bucket_depth);
    if !found_issuer {
        recovered.buckets = vec![0; n];
    } else if recovered.buckets.len() != n {
        return Err(BeeRecoverError::Decode(format!(
            "{} bucket counters for bucket depth {}",
            recovered.buckets.len(),
            recovered.bucket_depth
        )));
    }
    let capacity = crate::bucket_capacity(batch_depth, recovered.bucket_depth)
        .filter(|_| recovered.bucket_depth < batch_depth)
        .ok_or_else(|| {
            BeeRecoverError::Decode(format!(
                "bucket depth {} does not fit batch depth {batch_depth}",
                recovered.bucket_depth
            ))
        })?;

    let saw_items = fold_in_stamp_items(&mut db, batch_id, &mut recovered.buckets)?;
    if !found_issuer && !saw_items {
        return Ok(None);
    }
    for c in &mut recovered.buckets {
        *c = (*c).min(capacity);
    }
    Ok(Some(recovered))
}

/// A private copy of a `LevelDB` directory, removed on drop.
struct TempCopy(PathBuf);

impl TempCopy {
    fn of(src: &Path) -> Result<Self, BeeRecoverError> {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos());
        let dir = std::env::temp_dir().join(format!(
            "ant-bee-stamperstore-{}-{nanos}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir)
            .map_err(|e| BeeRecoverError::Open(format!("{}: {e}", dir.display())))?;
        let copy = Self(dir);
        let entries = std::fs::read_dir(src)
            .map_err(|e| BeeRecoverError::Open(format!("{}: {e}", src.display())))?;
        for entry in entries {
            let entry = entry.map_err(|e| BeeRecoverError::Open(e.to_string()))?;
            let name = entry.file_name();
            // The lock and info log are the live store's own; the copy
            // gets fresh ones.
            if name == "LOCK" || name == "LOG" || name == "LOG.old" {
                continue;
            }
            let ty = entry
                .file_type()
                .map_err(|e| BeeRecoverError::Open(e.to_string()))?;
            if !ty.is_file() {
                continue;
            }
            std::fs::copy(entry.path(), copy.0.join(&name)).map_err(|e| {
                BeeRecoverError::Open(format!("copy {}: {e}", entry.path().display()))
            })?;
        }
        Ok(copy)
    }
}

impl Drop for TempCopy {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Walk the per-stamp `StampItem` records for this batch and raise each
/// bucket counter to at least `highest_issued_index + 1`, mirroring
/// bee's crash-recovery (`StampIssuer.recover`). This makes the result
/// correct even when the `StampIssuerItem` snapshot predates the last
/// stamps bee issued (unclean shutdown), or is missing altogether.
/// Returns whether any record for the batch was found.
fn fold_in_stamp_items(
    db: &mut DB,
    batch_id: &[u8; 32],
    buckets: &mut [u32],
) -> Result<bool, BeeRecoverError> {
    let mut prefix = Vec::with_capacity(STAMP_ITEM_NS.len() + 33);
    prefix.extend_from_slice(STAMP_ITEM_NS);
    prefix.extend_from_slice(batch_id);
    prefix.push(b'/');

    let mut it = db
        .new_iter()
        .map_err(|e| BeeRecoverError::Read(e.to_string()))?;
    it.seek(&prefix);
    let mut saw = false;
    while it.valid() {
        let Some((key, val)) = it.current() else {
            break;
        };
        if !key.starts_with(&prefix) {
            break;
        }
        saw = true;
        // value = batchID(32) || chunkAddr(32) || batchIndex(8) || ts(8) [|| 0]
        if val.len() >= 72 {
            let bucket = u32::from_be_bytes(val[64..68].try_into().unwrap());
            let within = u32::from_be_bytes(val[68..72].try_into().unwrap());
            if let Some(slot) = buckets.get_mut(bucket as usize) {
                let need = within.saturating_add(1);
                if *slot < need {
                    *slot = need;
                }
            }
        }
        if !it.advance() {
            break;
        }
    }
    Ok(saw)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    /// Encode a msgpack map with string keys, mimicking how bee's
    /// `vmihailenco/msgpack` marshals a tagged struct, then decode it
    /// through the same path `recover_bee_buckets` uses.
    #[test]
    fn decodes_bee_stamp_issuer_map() {
        use rmp_serde::Serializer;
        use serde::Serialize;

        // Build a map: bee includes extra keys we must ignore.
        #[derive(Serialize)]
        struct Wire {
            label: String,
            #[serde(rename = "batchDepth")]
            batch_depth: u8,
            #[serde(rename = "bucketDepth")]
            bucket_depth: u8,
            buckets: Vec<u32>,
            #[serde(rename = "maxBucketCount")]
            max_bucket_count: u32,
            #[serde(rename = "immutableFlag")]
            immutable: bool,
        }
        let wire = Wire {
            label: "test".into(),
            batch_depth: 22,
            bucket_depth: 4,
            buckets: vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16],
            max_bucket_count: 16,
            immutable: false,
        };
        // Force map (string-keyed) encoding, like bee.
        let mut buf = Vec::new();
        wire.serialize(&mut Serializer::new(&mut buf).with_struct_map())
            .unwrap();

        let data: BeeStampIssuerData = rmp_serde::from_slice(&buf).unwrap();
        assert_eq!(data.batch_depth, 22);
        assert_eq!(data.bucket_depth, 4);
        assert_eq!(data.buckets.len(), 16);
        assert_eq!(data.buckets[15], 16);
    }

    /// The `StampItem` fold raises a bucket to the highest issued index
    /// + 1 and leaves already-higher counts untouched.
    #[test]
    fn stamp_item_fold_logic() {
        // Simulate two stamp items in bucket 3 (within 0 and 1) and one
        // in bucket 7 (within 4). Starting counters under-count bucket 7.
        let mut buckets = [0u32; 16];
        buckets[3] = 2; // already current
        buckets[7] = 2; // stale: a stamp at within=4 exists

        let mut items: BTreeMap<u32, u32> = BTreeMap::new();
        items.insert(3, 1); // highest within in bucket 3
        items.insert(7, 4); // highest within in bucket 7
        for (&bucket, &within) in &items {
            let need = within + 1;
            if buckets[bucket as usize] < need {
                buckets[bucket as usize] = need;
            }
        }
        assert_eq!(buckets[3], 2, "already-current bucket untouched");
        assert_eq!(buckets[7], 5, "stale bucket raised to within+1");
    }

    fn tmpdir(tag: &str) -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "ant-beestore-test-{tag}-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn stamp_item(batch: &[u8; 32], chunk: u8, bucket: u32, within: u32) -> (Vec<u8>, Vec<u8>) {
        let mut key = STAMP_ITEM_NS.to_vec();
        key.extend_from_slice(batch);
        key.push(b'/');
        key.extend_from_slice(hex::encode([chunk; 32]).as_bytes());
        let mut val = batch.to_vec();
        val.extend_from_slice(&[chunk; 32]);
        val.extend_from_slice(&bucket.to_be_bytes());
        val.extend_from_slice(&within.to_be_bytes());
        val.extend_from_slice(&[0u8; 8]);
        val.push(0);
        (key, val)
    }

    /// Write a bee-like stamperstore and close it. Records are left in
    /// the write-ahead log, which a read-write open would compact into a
    /// table file — exactly the mutation the copy has to avoid.
    fn write_store(dir: &Path, records: &[(Vec<u8>, Vec<u8>)]) {
        let opt = Options {
            create_if_missing: true,
            ..Options::default()
        };
        let mut db = DB::open(dir, opt).unwrap();
        for (k, v) in records {
            db.put(k, v).unwrap();
        }
        db.flush().unwrap();
        drop(db);
    }

    fn issuer_record(
        batch: &[u8; 32],
        batch_depth: u8,
        bucket_depth: u8,
        buckets: Vec<u32>,
    ) -> (Vec<u8>, Vec<u8>) {
        #[derive(serde::Serialize)]
        struct Wire {
            #[serde(rename = "batchDepth")]
            batch_depth: u8,
            #[serde(rename = "bucketDepth")]
            bucket_depth: u8,
            buckets: Vec<u32>,
            #[serde(rename = "immutableFlag")]
            immutable: bool,
        }
        let mut buf = Vec::new();
        serde::Serialize::serialize(
            &Wire {
                batch_depth,
                bucket_depth,
                buckets,
                immutable: true,
            },
            &mut rmp_serde::Serializer::new(&mut buf).with_struct_map(),
        )
        .unwrap();
        let mut key = STAMP_ISSUER_NS.to_vec();
        key.extend_from_slice(batch);
        (key, buf)
    }

    fn snapshot(dir: &Path) -> Vec<(std::ffi::OsString, u64, std::time::SystemTime)> {
        let mut out: Vec<_> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| {
                let e = e.unwrap();
                let m = e.metadata().unwrap();
                (e.file_name(), m.len(), m.modified().unwrap())
            })
            .collect();
        out.sort();
        out
    }

    #[test]
    fn bee_store_opened_without_mutation() {
        let root = tmpdir("ro");
        let store = root.join("stamperstore");
        let batch = [0x41u8; 32];
        let mut recs = vec![issuer_record(&batch, 6, 2, vec![1, 0, 0, 0])];
        recs.push(stamp_item(&batch, 1, 2, 3));
        write_store(&store, &recs);
        let before = snapshot(&store);
        // Let a rewrite show up in the mtimes even on coarse clocks.
        std::thread::sleep(std::time::Duration::from_millis(20));

        let rec = recover_bee_buckets(&store, &batch, 6, 2).unwrap().unwrap();
        assert_eq!(rec.buckets, vec![1, 0, 4, 0]);
        assert_eq!(snapshot(&store), before, "bee's store was modified");
        // Twice, to cover a second open of the same store too.
        recover_bee_buckets(&store, &batch, 6, 2).unwrap().unwrap();
        assert_eq!(snapshot(&store), before, "bee's store was modified");
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn bee_items_without_issuer_item_seed_counters() {
        let root = tmpdir("items");
        let store = root.join("stamperstore");
        let batch = [0x42u8; 32];
        let other = [0x43u8; 32];
        // depth 6 / bucket depth 2: capacity 16 per bucket.
        write_store(
            &store,
            &[
                stamp_item(&batch, 1, 0, 2),
                stamp_item(&batch, 2, 0, 0),
                stamp_item(&batch, 3, 3, 7),
                // Past capacity: clamped, not carried over as-is.
                stamp_item(&batch, 4, 1, 1000),
                // Another batch's items stay out.
                stamp_item(&other, 5, 2, 9),
            ],
        );
        let rec = recover_bee_buckets(&store, &batch, 6, 2).unwrap().unwrap();
        assert_eq!((rec.batch_depth, rec.bucket_depth), (6, 2));
        assert_eq!(rec.buckets, vec![3, 16, 0, 8]);
        // A batch with neither record has nothing to recover.
        assert!(recover_bee_buckets(&store, &[0x44; 32], 6, 2)
            .unwrap()
            .is_none());
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn bee_issuer_counters_are_clamped_and_depths_bounded() {
        let root = tmpdir("clamp");
        let store = root.join("stamperstore");
        let batch = [0x45u8; 32];
        let deep = [0x46u8; 32];
        write_store(
            &store,
            &[
                issuer_record(&batch, 6, 2, vec![u32::MAX, 16, 17, 3]),
                issuer_record(&deep, 40, 30, Vec::new()),
            ],
        );
        let rec = recover_bee_buckets(&store, &batch, 6, 2).unwrap().unwrap();
        assert_eq!(rec.buckets, vec![16, 16, 16, 3]);
        assert!(matches!(
            recover_bee_buckets(&store, &deep, 40, 30),
            Err(BeeRecoverError::Decode(_))
        ));
        std::fs::remove_dir_all(&root).ok();
    }
}
