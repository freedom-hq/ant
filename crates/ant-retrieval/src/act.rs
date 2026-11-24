//! Swarm ACT (access control) storage structures.
//!
//! Bee's access control (`pkg/accesscontrol`) persists three artifacts
//! on Swarm, all of which this module reads and writes byte-compatibly:
//!
//! - **ACT key-value store** (`kvs/kvs.go`): a *simple* manifest — a
//!   JSON document `{"entries":{"<hex lookup key>":{"reference":"<hex
//!   sealed access key>"}}}` (bee `pkg/manifest/simple`, keys sorted by
//!   Go's `json.Marshal`) — stored through the plain pipeline at
//!   redundancy `NONE`. Each row maps a grantee's lookup key to their
//!   XOR-sealed copy of the 32-byte access key.
//! - **History** (`history.go`): a mantaray manifest (zero obfuscation
//!   key) whose keys are `strconv.FormatInt(math.MaxInt64 - unixTime)`
//!   — the *latest* epoch gets the lexicographically *smallest* key —
//!   and whose entries reference ACT kvs roots. Grantee updates add
//!   `{"encryptedglref": <hex>}` metadata. `Lookup(t)` walks value
//!   nodes in ascending key order (newest first) and picks the first
//!   epoch whose timestamp is `<= t`, falling back to the *oldest*
//!   epoch when `t` predates them all.
//! - **Grantee list** (`grantee.go`): the concatenation of 65-byte
//!   uncompressed SEC1 public keys, stored through the *encrypted*
//!   pipeline (64-byte reference), which is then wrapped once more for
//!   the publisher.
//!
//! Reads go through a [`ChunkFetcher`]; writes return [`SplitChunk`]s
//! for the caller to pushsync — mirroring how the manifest writer and
//! splitter integrate with the gateway upload path.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::joiner::{join, join_encrypted, JoinError, ENCRYPTED_REF_SIZE};
use crate::manifest_writer::{
    build_entries_manifest, ManifestWriteError, ManifestWriteResult, RawManifestEntry,
};
use crate::mantaray::{load_node_ref, ManifestError};
use crate::{ChunkAddr, ChunkFetcher};

/// History metadata key carrying the publisher-encrypted grantee-list
/// reference (bee `controller.go::UpdateHandler`).
pub const HISTORY_GRANTEE_LIST_KEY: &str = "encryptedglref";

/// Upper bound for joined ACT artifacts (kvs manifests, histories,
/// grantee lists). These are small (tens of rows); 4 MiB is generous
/// headroom while bounding a malicious reference.
const ACT_MAX_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Error)]
pub enum ActError {
    /// The requested entry does not exist (bee `accesscontrol.ErrNotFound`).
    #[error("access control: not found")]
    NotFound,
    /// `Lookup` was called with a non-positive timestamp
    /// (bee `accesscontrol.ErrInvalidTimestamp`).
    #[error("invalid timestamp")]
    InvalidTimestamp,
    /// A history epoch with this key already exists. Bee's mantaray
    /// `Add` of an existing key on a lazily-loaded trie leaves the leaf
    /// with `forks == nil`, so `Store` fails with `ErrInvalidInput`
    /// ("input invalid") — the documented same-second-update failure
    /// (`controller.go` comment on `UpdateHandler`).
    #[error("input invalid")]
    DuplicateEpoch,
    #[error("fetch/join failed: {0}")]
    Join(#[from] JoinError),
    #[error("manifest read failed: {0}")]
    Manifest(#[from] ManifestError),
    #[error("manifest write failed: {0}")]
    Write(#[from] ManifestWriteError),
    #[error("malformed ACT key-value store: {0}")]
    MalformedKvs(String),
}

// --- ACT key-value store (bee pkg/accesscontrol/kvs over a simple manifest) ---

#[derive(Clone, Serialize, Deserialize)]
struct SimpleEntry {
    reference: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    metadata: BTreeMap<String, String>,
}

#[derive(Serialize, Deserialize)]
struct SimpleManifest {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    entries: BTreeMap<String, SimpleEntry>,
}

/// In-memory ACT key-value store: hex lookup key → sealed access key.
///
/// Serialization matches bee's simple manifest JSON exactly: object
/// keys sorted (Go's `json.Marshal` map ordering), `metadata` omitted
/// when empty, no whitespace.
#[derive(Default)]
pub struct ActKvs {
    entries: BTreeMap<String, SimpleEntry>,
    /// Number of `put`s since load/creation — bee's `putCnt`, whose
    /// zero value makes `Save` fail with `ErrNothingToSave`.
    puts: usize,
}

impl ActKvs {
    /// Empty store (bee `kvs.New`).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Load an existing store by joining its 32-byte reference
    /// (bee `kvs.NewReference`).
    pub async fn load(fetcher: &dyn ChunkFetcher, reference: ChunkAddr) -> Result<Self, ActError> {
        let bytes = load_plain(fetcher, reference).await?;
        let manifest: SimpleManifest = serde_json::from_slice(&bytes)
            .map_err(|e| ActError::MalformedKvs(format!("parse simple manifest: {e}")))?;
        Ok(Self {
            entries: manifest.entries,
            puts: 0,
        })
    }

    /// Fetch the value stored under `key` (bee `kvs.Get`, which reads
    /// the entry's "reference" hex back into bytes).
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let entry = self.entries.get(&hex::encode(key))?;
        hex::decode(&entry.reference).ok()
    }

    /// Store `value` under `key` (bee `kvs.Put`).
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.entries.insert(
            hex::encode(key),
            SimpleEntry {
                reference: hex::encode(value),
                metadata: BTreeMap::new(),
            },
        );
        self.puts += 1;
    }

    /// Whether `save` would fail bee-style with `ErrNothingToSave`
    /// (`kvs.Save` on a store that saw no `Put` since load).
    #[must_use]
    pub fn nothing_to_save(&self) -> bool {
        self.puts == 0
    }

    /// Serialize to the exact JSON bytes bee's simple manifest
    /// marshals; the caller stores them through the plain splitter.
    #[must_use]
    pub fn to_manifest_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(&SimpleManifest {
            entries: self.entries.clone(),
        })
        .expect("kvs manifest serializes")
    }
}

// --- history (bee pkg/accesscontrol/history.go) ---

/// One history epoch: a reversed-timestamp key, the ACT kvs root it
/// references, and its fork metadata.
#[derive(Debug, Clone)]
pub struct HistoryEntry {
    /// Manifest key: `(i64::MAX - unix_seconds)` in decimal.
    pub key: String,
    /// 32-byte ACT kvs root reference.
    pub reference: ChunkAddr,
    pub metadata: BTreeMap<String, String>,
}

impl HistoryEntry {
    /// The epoch's original unix timestamp, when the key parses.
    #[must_use]
    pub fn timestamp(&self) -> Option<i64> {
        self.key.parse::<i64>().ok().map(|rev| i64::MAX - rev)
    }
}

/// Bee's history key for a unix timestamp:
/// `strconv.FormatInt(math.MaxInt64 - unixTime, 10)`.
#[must_use]
pub fn history_key(unix_time: i64) -> String {
    (i64::MAX - unix_time).to_string()
}

/// Most epochs [`history_entries`] reads from one history. A history
/// gains one epoch per grantee update or first upload, each in its own
/// second, so real histories stay far below this; it bounds the
/// `HistoryEntry` list a crafted manifest can make the walk build
/// (RET-05, issue #287). [`history_with_epoch`] refuses to grow a
/// history past it, so ant never writes a history its own reader
/// rejects.
pub const MAX_HISTORY_ENTRIES: usize = 4_096;

/// Most node visits (each fork followed, plus the root) one history
/// walk makes. History keys all have the same length (19 decimal
/// digits), so every epoch is a leaf of the prefix-compressed trie,
/// every other node has at least two forks (the root at least one),
/// and a well-formed history of [`MAX_HISTORY_ENTRIES`] epochs has at
/// most `2 * MAX_HISTORY_ENTRIES` nodes. Anything past that is a
/// manifest whose forks share children to multiply the walk.
const MAX_HISTORY_NODE_VISITS: usize = 2 * MAX_HISTORY_ENTRIES + 1;

/// Deepest level below the root the walk descends to. A history key is
/// at most 19 decimal digits (`i64::MAX - unix_time`) and every fork
/// consumes at least one of them, so a real history is never deeper
/// than 19; one more level of slack.
const MAX_HISTORY_DEPTH: usize = 20;

/// Most fork-metadata bytes (keys plus values) the returned entries may
/// carry in total. A real epoch carries at most one `encryptedglref`
/// (about 150 bytes), so a full history needs well under 1 MiB; a fork's
/// metadata can be up to 64 KiB, and without this cap
/// [`MAX_HISTORY_ENTRIES`] crafted forks could retain 256 MiB.
const MAX_HISTORY_METADATA_BYTES: usize = ACT_MAX_BYTES;

/// Load every value entry of a history manifest, in bee's `WalkNode`
/// order: ascending fork-byte order at every level, i.e. ascending
/// lexicographic key order — the *newest* epoch first (keys are
/// reversed timestamps).
///
/// The walk is bounded (RET-05, issue #287): a manifest whose forks
/// all point at one shared child would otherwise be walked once per
/// path to it, `256^depth` node loads and entries from a handful of
/// chunks. Each distinct node is fetched and parsed once per walk (a
/// shared child is still walked, and its epochs listed, once per path,
/// as bee does), and a manifest that needs more than
/// [`MAX_HISTORY_NODE_VISITS`] visits, is deeper than
/// [`MAX_HISTORY_DEPTH`], has more than [`MAX_HISTORY_ENTRIES`] epochs
/// or more than [`MAX_HISTORY_METADATA_BYTES`] of epoch metadata is
/// [`ActError::MalformedKvs`].
pub async fn history_entries(
    fetcher: &dyn ChunkFetcher,
    root: ChunkAddr,
) -> Result<Vec<HistoryEntry>, ActError> {
    let mut walk = HistoryWalk {
        fetcher,
        nodes: HashMap::new(),
        visits: 0,
        metadata_bytes: 0,
        out: Vec::new(),
    };
    let node = walk.load(&root).await?;
    walk_history(&mut walk, node, String::new(), 0).await?;
    Ok(walk.out)
}

/// State of one [`history_entries`] walk: the nodes loaded so far, by
/// reference, and the running totals its caps are checked against.
struct HistoryWalk<'a> {
    fetcher: &'a dyn ChunkFetcher,
    nodes: HashMap<Vec<u8>, Arc<crate::mantaray::Node>>,
    visits: usize,
    metadata_bytes: usize,
    out: Vec<HistoryEntry>,
}

impl HistoryWalk<'_> {
    /// Count one node visit against [`MAX_HISTORY_NODE_VISITS`] and
    /// return the node, fetching and parsing it only on its first visit.
    async fn load(&mut self, node_ref: &[u8]) -> Result<Arc<crate::mantaray::Node>, ActError> {
        self.visits += 1;
        if self.visits > MAX_HISTORY_NODE_VISITS {
            return Err(ActError::MalformedKvs(format!(
                "history walk needs more than {MAX_HISTORY_NODE_VISITS} node visits"
            )));
        }
        if let Some(node) = self.nodes.get(node_ref) {
            return Ok(Arc::clone(node));
        }
        let node = Arc::new(load_node_ref(self.fetcher, node_ref).await?);
        self.nodes.insert(node_ref.to_vec(), Arc::clone(&node));
        Ok(node)
    }
}

fn walk_history<'a, 'w>(
    walk: &'a mut HistoryWalk<'w>,
    node: Arc<crate::mantaray::Node>,
    path: String,
    depth: usize,
) -> futures::future::BoxFuture<'a, Result<(), ActError>>
where
    'w: 'a,
{
    Box::pin(async move {
        if depth >= MAX_HISTORY_DEPTH && !node.forks.is_empty() {
            return Err(ActError::MalformedKvs(format!(
                "history manifest is deeper than {MAX_HISTORY_DEPTH} levels"
            )));
        }
        let mut fork_keys: Vec<u8> = node.forks.keys().copied().collect();
        fork_keys.sort_unstable();
        for k in fork_keys {
            let fork = &node.forks[&k];
            let fork_path = format!("{path}{}", String::from_utf8_lossy(&fork.prefix));
            let child = walk.load(&fork.child_ref).await?;
            // Bee's walker records nodes with `IsValueType() &&
            // len(Entry()) > 0`; ant's parser keeps a value node's
            // entry bytes verbatim (all-zero = no entry).
            if child.entry.iter().any(|&b| b != 0) {
                let reference: ChunkAddr = child.entry.as_slice().try_into().map_err(|_| {
                    ActError::Manifest(ManifestError::InvalidRefLength(child.entry.len() as u8))
                })?;
                if walk.out.len() >= MAX_HISTORY_ENTRIES {
                    return Err(ActError::MalformedKvs(format!(
                        "history has more than {MAX_HISTORY_ENTRIES} epochs"
                    )));
                }
                walk.metadata_bytes += fork
                    .metadata
                    .iter()
                    .map(|(k, v)| k.len() + v.len())
                    .sum::<usize>();
                if walk.metadata_bytes > MAX_HISTORY_METADATA_BYTES {
                    return Err(ActError::MalformedKvs(format!(
                        "history epochs carry more than {MAX_HISTORY_METADATA_BYTES} metadata bytes"
                    )));
                }
                walk.out.push(HistoryEntry {
                    key: fork_path.clone(),
                    reference,
                    metadata: fork
                        .metadata
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect(),
                });
            }
            walk_history(walk, child, fork_path, depth + 1).await?;
        }
        Ok(())
    })
}

/// Bee's `History.Lookup`: pick the epoch for `timestamp` from entries
/// in walk order (newest first). The first epoch whose timestamp is
/// `<= timestamp` wins; a timestamp older than every epoch falls back
/// to the *oldest* epoch; an empty history is `NotFound`; a
/// non-positive timestamp is `InvalidTimestamp`.
pub fn history_lookup(entries: &[HistoryEntry], timestamp: i64) -> Result<&HistoryEntry, ActError> {
    if timestamp <= 0 {
        return Err(ActError::InvalidTimestamp);
    }
    let searched_reversed = i64::MAX - timestamp;
    let mut after: Option<&HistoryEntry> = None;
    for entry in entries {
        after = Some(entry);
        // bee `isBeforeMatch`: unparseable keys abort the walk; a key
        // of exactly "0" never matches.
        let target: i64 = entry
            .key
            .parse()
            .map_err(|_| ActError::MalformedKvs(format!("bad history key {}", entry.key)))?;
        if target != 0 && searched_reversed <= target {
            return Ok(entry);
        }
    }
    after.ok_or(ActError::NotFound)
}

/// Append a new epoch and rebuild the history manifest. Bee's mantaray
/// `Add` of an already-present key on a freshly loaded trie makes the
/// subsequent `Store` fail (`ErrInvalidInput`) — the same-second update
/// limitation — so a duplicate key is [`ActError::DuplicateEpoch`].
///
/// The rebuild produces the same canonical prefix-compressed trie bee's
/// incremental adds maintain, hence the same root reference (pinned by
/// the deterministic `act.json` history vectors).
pub fn history_with_epoch(
    entries: &[HistoryEntry],
    key: String,
    reference: ChunkAddr,
    metadata: BTreeMap<String, String>,
) -> Result<ManifestWriteResult, ActError> {
    if entries.iter().any(|e| e.key == key) {
        return Err(ActError::DuplicateEpoch);
    }
    if entries.len() >= MAX_HISTORY_ENTRIES {
        return Err(ActError::MalformedKvs(format!(
            "history already has {} epochs, the most it can hold is {MAX_HISTORY_ENTRIES}",
            entries.len()
        )));
    }
    let mut raw: Vec<RawManifestEntry> = entries
        .iter()
        .map(|e| RawManifestEntry {
            path: e.key.clone(),
            reference: e.reference,
            metadata: e.metadata.clone(),
        })
        .collect();
    raw.push(RawManifestEntry {
        path: key,
        reference,
        metadata,
    });
    Ok(build_entries_manifest(&raw)?)
}

// --- grantee list (bee pkg/accesscontrol/grantee.go) ---

/// Serialize grantee public keys as concatenated 65-byte uncompressed
/// SEC1 points (bee `grantee.go::serialize`).
#[must_use]
pub fn serialize_grantees(grantees: &[k256::PublicKey]) -> Vec<u8> {
    let mut out = Vec::with_capacity(grantees.len() * 65);
    for g in grantees {
        out.extend_from_slice(&ant_crypto::act::uncompress_public_key(g));
    }
    out
}

/// Deserialize a grantee list. Bee returns an *empty list* — not an
/// error — for malformed input (`grantee.go::deserialize` bails to
/// `[]` when any key fails to parse); mirror that.
#[must_use]
pub fn deserialize_grantees(data: &[u8]) -> Vec<k256::PublicKey> {
    if data.is_empty() || !data.len().is_multiple_of(65) {
        return Vec::new();
    }
    let mut out = Vec::with_capacity(data.len() / 65);
    for chunk in data.chunks(65) {
        match ant_crypto::act::parse_public_key(chunk) {
            Ok(pk) => out.push(pk),
            Err(_) => return Vec::new(),
        }
    }
    out
}

// --- shared loaders ---

/// Join a plain 32-byte reference into its full byte payload (the
/// loadsave `Load` path bee uses for kvs manifests and histories).
pub async fn load_plain(
    fetcher: &dyn ChunkFetcher,
    reference: ChunkAddr,
) -> Result<Vec<u8>, ActError> {
    let root_chunk = fetcher.fetch(reference).await.map_err(|e| {
        ActError::Join(JoinError::FetchChunk {
            addr: hex::encode(reference),
            source: e,
        })
    })?;
    Ok(join(fetcher, &root_chunk, ACT_MAX_BYTES).await?)
}

/// Join an encrypted 64-byte reference (the grantee-list load path).
pub async fn load_encrypted(
    fetcher: &dyn ChunkFetcher,
    reference: [u8; ENCRYPTED_REF_SIZE],
) -> Result<Vec<u8>, ActError> {
    Ok(join_encrypted(fetcher, reference, ACT_MAX_BYTES).await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kvs_serializes_like_go_simple_manifest() {
        let mut kvs = ActKvs::new();
        kvs.put(&[0xbb; 4], &[0x02; 3]);
        kvs.put(&[0xaa; 4], &[0x01; 3]);
        assert_eq!(
            String::from_utf8(kvs.to_manifest_bytes()).unwrap(),
            r#"{"entries":{"aaaaaaaa":{"reference":"010101"},"bbbbbbbb":{"reference":"020202"}}}"#
        );
        assert_eq!(kvs.get(&[0xaa; 4]).unwrap(), vec![0x01; 3]);
        assert!(kvs.get(&[0xcc; 4]).is_none());
        assert!(!kvs.nothing_to_save());
        assert!(ActKvs::new().nothing_to_save());
    }

    #[test]
    fn history_key_reverses_timestamp() {
        assert_eq!(history_key(1_700_000_000), "9223372035154775807");
    }

    fn entry(ts: i64) -> HistoryEntry {
        HistoryEntry {
            key: history_key(ts),
            reference: [ts as u8; 32],
            metadata: BTreeMap::new(),
        }
    }

    #[test]
    fn history_lookup_matches_bee_walk_semantics() {
        // Walk order = ascending key = descending timestamp.
        let mut entries = vec![entry(100), entry(50), entry(10)];
        entries.sort_by(|a, b| a.key.cmp(&b.key));

        // Exact and in-between hits pick the closest epoch <= t.
        assert_eq!(
            history_lookup(&entries, 100).unwrap().timestamp(),
            Some(100)
        );
        assert_eq!(history_lookup(&entries, 70).unwrap().timestamp(), Some(50));
        assert_eq!(history_lookup(&entries, 10).unwrap().timestamp(), Some(10));
        // Future timestamps get the newest epoch.
        assert_eq!(
            history_lookup(&entries, 1 << 40).unwrap().timestamp(),
            Some(100)
        );
        // Before-all falls back to the OLDEST epoch (bee's afterNode).
        assert_eq!(history_lookup(&entries, 5).unwrap().timestamp(), Some(10));
        // Non-positive timestamps are invalid.
        assert!(matches!(
            history_lookup(&entries, 0),
            Err(ActError::InvalidTimestamp)
        ));
        // Empty history is NotFound.
        assert!(matches!(history_lookup(&[], 5), Err(ActError::NotFound)));
    }

    #[test]
    fn duplicate_epoch_is_rejected() {
        let entries = vec![entry(100)];
        let err =
            history_with_epoch(&entries, history_key(100), [1u8; 32], BTreeMap::new()).unwrap_err();
        assert!(matches!(err, ActError::DuplicateEpoch));
    }

    #[test]
    fn grantee_list_round_trips_and_tolerates_garbage() {
        let a = ant_crypto::act::public_key_of(&[0x11; 32]).unwrap();
        let b = ant_crypto::act::public_key_of(&[0x22; 32]).unwrap();
        let data = serialize_grantees(&[a, b]);
        assert_eq!(data.len(), 130);
        assert_eq!(deserialize_grantees(&data), vec![a, b]);
        assert_eq!(
            deserialize_grantees(&[0u8; 65]),
            [] as [k256::elliptic_curve::PublicKey<k256::Secp256k1>; 0]
        );
        assert_eq!(
            deserialize_grantees(&data[..64]),
            [] as [k256::elliptic_curve::PublicKey<k256::Secp256k1>; 0]
        );
        assert_eq!(
            deserialize_grantees(&[]),
            [] as [k256::elliptic_curve::PublicKey<k256::Secp256k1>; 0]
        );
    }

    // --- bounded history walk (RET-05, issue #287) ---

    use std::error::Error;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;

    use crate::manifest_writer::VERSION_HASH_31;
    use crate::splitter::split_bytes;

    /// In-memory chunk store that counts every fetch.
    #[derive(Default)]
    struct MapFetcher {
        chunks: HashMap<[u8; 32], Vec<u8>>,
        fetches: AtomicUsize,
    }

    impl MapFetcher {
        /// Store `payload` as a plain Swarm file; returns its root.
        fn put_file(&mut self, payload: &[u8]) -> ChunkAddr {
            let split = split_bytes(payload);
            for c in split.chunks {
                self.chunks.insert(c.address, c.wire);
            }
            split.root
        }
        fn fetches(&self) -> usize {
            self.fetches.load(Ordering::Relaxed)
        }
    }

    #[async_trait]
    impl ChunkFetcher for MapFetcher {
        async fn fetch(&self, addr: [u8; 32]) -> Result<Vec<u8>, Box<dyn Error + Send + Sync>> {
            self.fetches.fetch_add(1, Ordering::Relaxed);
            self.chunks
                .get(&addr)
                .cloned()
                .ok_or_else(|| format!("missing {}", hex::encode(addr)).into())
        }
    }

    /// Raw plain mantaray:0.2 node bytes: an optional entry, then one
    /// fork per `(prefix, child, metadata JSON)`, keyed by the prefix's
    /// first byte. Unlike the manifest writer, nothing stops several
    /// forks pointing at one child, which is the shape under test.
    fn raw_node(
        entry: Option<ChunkAddr>,
        forks: &[(Vec<u8>, ChunkAddr, Option<String>)],
    ) -> Vec<u8> {
        let mut p = vec![0u8; 32];
        p.extend_from_slice(&VERSION_HASH_31);
        p.push(32);
        p.extend_from_slice(&entry.unwrap_or([0; 32]));
        let mut bitmap = [0u8; 32];
        for (prefix, _, _) in forks {
            bitmap[usize::from(prefix[0]) / 8] |= 1 << (prefix[0] % 8);
        }
        p.extend_from_slice(&bitmap);
        let mut sorted: Vec<_> = forks.iter().collect();
        sorted.sort_by_key(|(prefix, _, _)| prefix[0]);
        for (prefix, child, meta) in sorted {
            // value | edge, plus with-metadata when there is some.
            p.push(if meta.is_some() { 2 | 4 | 16 } else { 2 | 4 });
            p.push(prefix.len() as u8);
            p.extend_from_slice(prefix);
            p.extend(std::iter::repeat_n(0u8, 30 - prefix.len()));
            p.extend_from_slice(child);
            if let Some(meta) = meta {
                let mut m = meta.clone().into_bytes();
                let pad = (32 - (m.len() + 2) % 32) % 32;
                m.extend(std::iter::repeat_n(b'\n', pad));
                p.extend_from_slice(&(m.len() as u16).to_be_bytes());
                p.extend_from_slice(&m);
            }
        }
        p
    }

    /// A node with all 256 fork bytes, every one pointing at `child`.
    fn fan_node(child: ChunkAddr, meta: Option<&str>) -> Vec<u8> {
        let forks: Vec<_> = (0..=255u8)
            .map(|b| (vec![b], child, meta.map(str::to_string)))
            .collect();
        raw_node(None, &forks)
    }

    fn assert_malformed(res: Result<Vec<HistoryEntry>, ActError>, what: &str) {
        match res {
            Err(ActError::MalformedKvs(msg)) => assert!(msg.contains(what), "{msg}"),
            other => panic!("expected MalformedKvs({what}), got {other:?}"),
        }
    }

    /// The audit's shape: a 256-fan node feeding one shared child, over
    /// three levels, would be 256^3 = 16.7M visits and as many entries.
    /// It now stops at the first cap it reaches (here the entry cap,
    /// after 4,096 entries), fetching each distinct node's chunks once.
    #[tokio::test]
    async fn shared_child_fan_is_bounded() {
        let mut f = MapFetcher::default();
        let leaf = f.put_file(&raw_node(Some([7; 32]), &[]));
        let l2 = f.put_file(&fan_node(leaf, None));
        let l1 = f.put_file(&fan_node(l2, None));
        let root = f.put_file(&fan_node(l1, None));
        let stored = f.chunks.len();

        assert_malformed(history_entries(&f, root).await, "");
        // Each fan node is a multi-chunk file; every chunk is fetched
        // at most once (twice for a root chunk: the manifest sniff and
        // the join), not once per path to it.
        assert!(
            f.fetches() <= 2 * stored,
            "{} fetches for {stored} chunks",
            f.fetches()
        );
    }

    /// Two levels of 256 forks without entries is 65,793 visits for no
    /// output at all: the visit budget binds on its own.
    #[tokio::test]
    async fn visit_budget_binds_without_entries() {
        let mut f = MapFetcher::default();
        let leaf = f.put_file(&raw_node(None, &[]));
        let l1 = f.put_file(&fan_node(leaf, None));
        let root = f.put_file(&fan_node(l1, None));
        assert_malformed(history_entries(&f, root).await, "node visits");
    }

    /// 16 fan nodes of 256 value forks each is 4,096 entries in only
    /// 4,113 visits, under the visit budget: one more fan of 256 and
    /// the entry cap binds first.
    #[tokio::test]
    async fn entry_cap_binds() {
        let mut f = MapFetcher::default();
        let leaf = f.put_file(&raw_node(Some([7; 32]), &[]));
        let fan = f.put_file(&fan_node(leaf, None));
        let mut root_forks: Vec<_> = (0..16u8).map(|b| (vec![b], fan, None)).collect();
        let full = f.put_file(&raw_node(None, &root_forks));
        let entries = history_entries(&f, full).await.expect("at the cap");
        assert_eq!(entries.len(), MAX_HISTORY_ENTRIES);

        root_forks.push((vec![16], fan, None));
        let over = f.put_file(&raw_node(None, &root_forks));
        assert_malformed(history_entries(&f, over).await, "epochs");
    }

    /// A fork's metadata can be up to 64 KiB, so far fewer entries than
    /// the entry cap can already retain more than the metadata budget.
    /// A small node of 4 heavy forks, shared by 18 paths, is 72 entries
    /// carrying 4.3 MB.
    #[tokio::test]
    async fn metadata_budget_binds() {
        let mut f = MapFetcher::default();
        let leaf = f.put_file(&raw_node(Some([7; 32]), &[]));
        let big = Some(format!(r#"{{"encryptedglref":"{}"}}"#, "a".repeat(60_000)));
        let heavy: Vec<_> = (0..4u8).map(|b| (vec![b], leaf, big.clone())).collect();
        let heavy = f.put_file(&raw_node(None, &heavy));
        let fan: Vec<_> = (0..18u8).map(|b| (vec![b], heavy, None)).collect();
        let root = f.put_file(&raw_node(None, &fan));
        assert_malformed(history_entries(&f, root).await, "metadata bytes");
    }

    /// A chain of single-fork nodes deeper than any 19-digit key can
    /// make is rejected, however few nodes it has.
    #[tokio::test]
    async fn depth_cap_binds() {
        let mut f = MapFetcher::default();
        let chain = |levels: usize, f: &mut MapFetcher| {
            let mut node = f.put_file(&raw_node(Some([7; 32]), &[]));
            for _ in 0..levels {
                node = f.put_file(&raw_node(None, &[(b"1".to_vec(), node, None)]));
            }
            node
        };
        let ok = chain(MAX_HISTORY_DEPTH, &mut f);
        assert_eq!(history_entries(&f, ok).await.expect("at the cap").len(), 1);
        let deep = chain(MAX_HISTORY_DEPTH + 1, &mut f);
        assert_malformed(history_entries(&f, deep).await, "deeper");
    }

    /// Legit histories still read back whole: a trie branching at every
    /// one of the 19 key digits (the deepest a real history gets), two
    /// epochs sharing one kvs reference (so the same leaf node is reached
    /// by two paths and both epochs must be listed), and metadata.
    #[tokio::test]
    async fn history_round_trips_through_bounded_walk() {
        let base = history_key(1_700_000_000);
        assert_eq!(base.len(), 19);
        let mut keys = vec![base.clone()];
        for i in 0..base.len() {
            let mut k = base.clone().into_bytes();
            k[i] = if k[i] == b'9' { b'0' } else { k[i] + 1 };
            keys.push(String::from_utf8(k).unwrap());
        }
        let mut f = MapFetcher::default();
        let mut entries: Vec<HistoryEntry> = Vec::new();
        for (n, key) in keys.iter().enumerate() {
            // The first two keys share one kvs reference.
            let reference = [n.saturating_sub(1) as u8 + 1; 32];
            let mut metadata = BTreeMap::new();
            if n % 2 == 0 {
                metadata.insert(HISTORY_GRANTEE_LIST_KEY.to_string(), "ab".repeat(64));
            }
            let built = history_with_epoch(&entries, key.clone(), reference, metadata.clone())
                .expect("append");
            for c in &built.chunks {
                f.chunks.insert(c.address, c.wire.clone());
            }
            let read = history_entries(&f, built.root).await.expect("read back");
            entries.push(HistoryEntry {
                key: key.clone(),
                reference,
                metadata,
            });
            let mut want: Vec<_> = entries
                .iter()
                .map(|e| (e.key.clone(), e.reference, e.metadata.clone()))
                .collect();
            want.sort();
            let got: Vec<_> = read
                .into_iter()
                .map(|e| (e.key, e.reference, e.metadata))
                .collect();
            assert_eq!(got, want, "after {} epochs", n + 1);
        }
    }

    #[test]
    fn full_history_refuses_another_epoch() {
        let entries: Vec<_> = (1..=MAX_HISTORY_ENTRIES as i64).map(entry).collect();
        let err =
            history_with_epoch(&entries, history_key(0), [1; 32], BTreeMap::new()).unwrap_err();
        assert!(matches!(err, ActError::MalformedKvs(_)), "{err:?}");
    }
}
