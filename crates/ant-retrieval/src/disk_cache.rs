//! Persistent (SQLite-backed) chunk cache — tier 2 of the cache stack
//! described in `PLAN.md` § 6.1.
//!
//! Network retrieval already validates every chunk on the wire (see
//! [`crate::retrieve_chunk`]) before any cache write. **Bee’s
//! `chunkstore.Get` trusts on-disk bytes the same way** — it reads the
//! retrieval index and sharky blob and returns `swarm.NewChunk` in Go
//! without re-proving the BMT against the address. This implementation matches
//! that contract: disk hits return stored wire bytes without
//! [`ant_crypto::cac_valid`] / [`ant_crypto::soc_valid`]. Bitrot or manual
//! edits can still poison a chunk until the next network refresh replaces
//! the row; that is the same durability model as bee’s localstore cache
//! path.
//!
//! Concurrency (bee-ish throughput goals):
//!
//! - **Dedicated read threads** — `get` is a prepared `SELECT` on a read-only
//!   WAL connection; jobs are routed round-robin over crossbeam channels (no
//!   `spawn_blocking` on the hot path). Pool size scales with
//!   [`std::thread::available_parallelism`] (8–32 workers).
//! - **Dedicated writer thread** — all mutating work (batched `put`s,
//!   `last_access` touches past the freshness window, eviction) runs on
//!   one thread with **batched transactions** so bursts amortise fsync.
//! - **Pragmas** — large `mmap_size` / `cache_size` on every connection.
//!
//! Async rule: `get` / `row_count` ship work to dedicated **read threads**
//! (each with a prepared `SELECT`) over a bounded channel — no
//! `spawn_blocking` hot path — so Tokio workers stay lightweight.

use crossbeam_channel::{
    bounded as cb_bounded, Receiver as CbReceiver, Sender as CbSender, TryRecvError,
};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension, TransactionBehavior};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{sync_channel, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tokio::sync::oneshot;
use tracing::{debug, trace, warn};

/// Default eviction "slack" — how far below the cap we drain on every
/// trigger. 95% means an over-cap insert deletes ~5% of the cap in
/// `last_access` order rather than one row at a time, keeping the
/// per-write eviction cost amortised. Mirrors the guidance in
/// `PLAN.md` § 6.1.
const EVICTION_SLACK_RATIO: f64 = 0.95;

/// How "fresh" `last_access` must already be for [`DiskChunkCache::get`]
/// to skip a touch write entirely. 60 s keeps hot reads as plain
/// `SELECT`s on the reader connections; rare `UPDATE`s go through the
/// writer queue.
const LAST_ACCESS_REFRESH_MS: i64 = 60_000;

/// Number of dedicated read threads (each owns one read-only connection +
/// cached prepared statements). Bounded to keep mobile FD use sane.
fn read_worker_count() -> usize {
    std::thread::available_parallelism()
        .map_or(8, std::num::NonZero::get)
        .clamp(8, 32)
}

/// Max `put` + `touch` rows coalesced into one writer transaction before commit.
/// Larger batches amortise WAL fsync against bursty write-through.
const WRITE_BATCH_MAX: usize = 256;

/// Target mmap size per `SQLite` connection (256 MiB).
const MMAP_BYTES: i64 = 512 * 1024 * 1024;

/// Negative `cache_size` pragma value = KiB of page cache per connection
/// (~128 MiB here).
const CACHE_KIB: i64 = -256 * 1024;

/// Default size cap for the persistent chunk cache. 10 GB matches
/// `PLAN.md` § 6.1's desktop / Raspberry Pi default. Operators on
/// constrained boxes (mobile / embedded) should set a smaller cap;
/// power users should raise it.
pub const DEFAULT_DISK_CACHE_BYTES: u64 = 10 * 1024 * 1024 * 1024;

#[derive(Debug, Error, Clone)]
pub enum DiskCacheError {
    #[error("sqlite: {0}")]
    Sqlite(String),
    #[error("io: {0}")]
    Io(String),
    #[error("background worker panicked")]
    Panicked,
    #[error("disk cache writer stopped")]
    WriterStopped,
}

impl From<rusqlite::Error> for DiskCacheError {
    fn from(e: rusqlite::Error) -> Self {
        Self::Sqlite(e.to_string())
    }
}

impl From<std::io::Error> for DiskCacheError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e.to_string())
    }
}

/// Pin membership rows: `(pinned root reference, member chunk
/// addresses)`.
pub type PinMembership = Vec<(Vec<u8>, Vec<[u8; 32]>)>;

/// SQLite-backed persistent chunk cache.
pub struct DiskChunkCache {
    inner: Arc<Inner>,
}

enum WriteMsg {
    Put {
        addr: [u8; 32],
        data: Vec<u8>,
        ack: oneshot::Sender<Result<(), DiskCacheError>>,
    },
    PutBatch {
        items: Vec<([u8; 32], Vec<u8>)>,
        ack: oneshot::Sender<Result<(), DiskCacheError>>,
    },
    Touch {
        addr: [u8; 32],
        last_access: i64,
    },
    /// Pin bookkeeping (see [`PinOp`]). Routed through the writer
    /// thread even for the read-shaped queries: pin traffic is rare
    /// (operator / bee-`/pins` actions, not the chunk hot path), and a
    /// single serialisation point keeps the pin tables and the
    /// `pin_count` budget accounting trivially consistent.
    Pin(PinOp),
    Shutdown,
}

/// Pin mutations and queries handled by the writer thread.
///
/// **Eviction invariant** (documented once, here): a chunk row with
/// `pin_count > 0` is *outside the cache budget* — its bytes are not
/// part of `total_bytes` (which tracks evictable bytes only) and the
/// eviction sweep never visits it. Pinning a chunk (0→1) subtracts its
/// size from the budget; dropping the last pin (1→0) adds it back and
/// re-checks the cap. `pin_count` is a reference count because two
/// pinned roots may share chunks (bee's pinstore refcounts the same
/// way); a chunk becomes evictable again only when *every* pin
/// covering it is removed.
enum PinOp {
    /// Record a pin collection: the root `reference` (32-byte plain or
    /// 64-byte encrypted reference), plus every member chunk of its
    /// tree with the wire bytes (inserted if not already cached).
    /// Replies `Ok(false)` when the reference was already pinned
    /// (idempotent, nothing written), `Ok(true)` when freshly pinned.
    Collection {
        reference: Vec<u8>,
        members: Vec<([u8; 32], Vec<u8>)>,
        ack: oneshot::Sender<Result<bool, DiskCacheError>>,
    },
    /// Drop a pin: decrement `pin_count` on every recorded member and
    /// delete the membership rows. Replies `Ok(false)` when the
    /// reference wasn't pinned.
    Unpin {
        reference: Vec<u8>,
        ack: oneshot::Sender<Result<bool, DiskCacheError>>,
    },
    Has {
        reference: Vec<u8>,
        ack: oneshot::Sender<Result<bool, DiskCacheError>>,
    },
    /// Every pinned reference, in pin-creation order.
    List {
        ack: oneshot::Sender<Result<Vec<Vec<u8>>, DiskCacheError>>,
    },
    /// Recorded member addresses per pin (all pins, or just
    /// `reference`). Backs the `/pins/check` integrity walk.
    Members {
        reference: Option<Vec<u8>>,
        ack: oneshot::Sender<Result<PinMembership, DiskCacheError>>,
    },
}

enum ReadJob {
    Get {
        addr: [u8; 32],
        reply: oneshot::Sender<Result<Option<Vec<u8>>, DiskCacheError>>,
    },
    Count {
        reply: oneshot::Sender<Result<u64, DiskCacheError>>,
    },
    Shutdown,
}

struct Inner {
    path: PathBuf,
    read_tx: Vec<CbSender<ReadJob>>,
    read_rr: AtomicUsize,
    read_joins: Mutex<Vec<JoinHandle<()>>>,
    write_tx: CbSender<WriteMsg>,
    writer_thread: Mutex<Option<JoinHandle<()>>>,
    max_bytes: u64,
    total_bytes: Arc<AtomicU64>,
    /// Live row count mirrored from `COUNT(*) FROM chunks`. Maintained
    /// by the writer thread alongside `total_bytes` so `antop` can show
    /// the chunk count without round-tripping a SQL query on every
    /// status tick. Reads `0` between `open()` returning and the
    /// initial `SELECT COUNT(*)` finishing (cosmetic; same trade-off as
    /// `total_bytes`).
    total_rows: Arc<AtomicU64>,
    /// Read-worker pool size, captured once at open time so the status
    /// snapshot can surface it without re-querying
    /// `available_parallelism`.
    read_workers: usize,
}

impl Drop for Inner {
    fn drop(&mut self) {
        for tx in &self.read_tx {
            let _ = tx.send(ReadJob::Shutdown);
        }
        if let Ok(mut joins) = self.read_joins.lock() {
            for h in joins.drain(..) {
                let _ = h.join();
            }
        }
        let _ = self.write_tx.send(WriteMsg::Shutdown);
        if let Ok(mut g) = self.writer_thread.lock() {
            if let Some(h) = g.take() {
                let _ = h.join();
            }
        }
    }
}

impl DiskChunkCache {
    /// Open (or create) a chunk cache database at `path` with a
    /// `max_bytes` byte cap.
    pub fn open(path: impl AsRef<Path>, max_bytes: u64) -> Result<Self, DiskCacheError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let total_bytes = Arc::new(AtomicU64::new(0));
        let total_rows = Arc::new(AtomicU64::new(0));
        let tb = total_bytes.clone();
        let tr = total_rows.clone();
        let slack_bytes = ((max_bytes as f64) * EVICTION_SLACK_RATIO) as u64;

        let (ready_tx, ready_rx) = sync_channel::<()>(0);
        let (write_tx, write_rx) = cb_bounded::<WriteMsg>(65_536);
        let path_thread = path.clone();
        let write_tx_main = write_tx.clone();

        let join = std::thread::Builder::new()
            .name("ant-disk-cache-writer".into())
            .spawn(move || {
                if let Err(e) = writer_main(
                    path_thread,
                    write_rx,
                    ready_tx,
                    tb,
                    tr,
                    max_bytes,
                    slack_bytes,
                ) {
                    warn!(
                        target: "ant_retrieval::disk_cache",
                        "writer thread exited with error: {e}",
                    );
                }
            })
            .map_err(|e| DiskCacheError::Io(e.to_string()))?;

        ready_rx.recv().map_err(|_| DiskCacheError::WriterStopped)?;

        let n_read = read_worker_count();
        let mut read_tx = Vec::with_capacity(n_read);
        let mut read_joins = Vec::with_capacity(n_read);
        for i in 0..n_read {
            let (job_tx, job_rx) = cb_bounded::<ReadJob>(4096);
            let path_r = path.clone();
            let wt = write_tx.clone();
            let j = std::thread::Builder::new()
                .name(format!("ant-disk-cache-read-{i}"))
                .spawn(move || read_worker_loop(path_r, job_rx, wt))
                .map_err(|e| DiskCacheError::Io(e.to_string()))?;
            read_tx.push(job_tx);
            read_joins.push(j);
        }

        debug!(
            target: "ant_retrieval::disk_cache",
            path = %path.display(),
            initial_total = total_bytes.load(Ordering::Relaxed),
            max_bytes,
            slack_bytes,
            read_workers = n_read,
            "opened persistent chunk cache",
        );

        Ok(Self {
            inner: Arc::new(Inner {
                path,
                read_tx,
                read_rr: AtomicUsize::new(0),
                read_joins: Mutex::new(read_joins),
                write_tx: write_tx_main,
                writer_thread: Mutex::new(Some(join)),
                max_bytes,
                total_bytes,
                total_rows,
                read_workers: n_read,
            }),
        })
    }

    /// Live row count (mirrored from `COUNT(*)` and maintained in
    /// transactions on every write / eviction). Reads `0` until the
    /// initial backfill scan completes (~30 s on a cold 7 GB cache);
    /// see the rationale in `writer_main`.
    #[must_use]
    pub fn used_rows(&self) -> u64 {
        self.inner.total_rows.load(Ordering::Relaxed)
    }

    /// Number of dedicated read worker threads servicing `get`
    /// requests. Set at `open()` time and held constant for the
    /// lifetime of the cache.
    #[must_use]
    pub fn read_workers(&self) -> usize {
        self.inner.read_workers
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.inner.path
    }

    #[must_use]
    pub fn capacity_bytes(&self) -> u64 {
        self.inner.max_bytes
    }

    #[must_use]
    pub fn used_bytes(&self) -> u64 {
        self.inner.total_bytes.load(Ordering::Relaxed)
    }

    /// Look up a chunk by address. Trusts stored bytes (bee `chunkstore`
    /// semantics). A stale `last_access` is refreshed via the writer
    /// queue (best-effort). Serviced by a dedicated read thread + prepared
    /// statement (no `spawn_blocking` on the hot path).
    pub async fn get(&self, addr: [u8; 32]) -> Result<Option<Vec<u8>>, DiskCacheError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        let i = self.inner.read_rr.fetch_add(1, Ordering::Relaxed) % self.inner.read_tx.len();
        self.inner.read_tx[i]
            .send(ReadJob::Get {
                addr,
                reply: reply_tx,
            })
            .map_err(|_| DiskCacheError::WriterStopped)?;
        reply_rx.await.map_err(|_| DiskCacheError::Panicked)?
    }

    /// Batched insert / upsert in a **single** writer transaction (amortises
    /// WAL fsync). Prefer this when filling the cache with many freshly
    /// validated chunks at once.
    pub async fn put_batch(&self, items: Vec<([u8; 32], Vec<u8>)>) -> Result<(), DiskCacheError> {
        if items.is_empty() {
            return Ok(());
        }
        let (ack_tx, ack_rx) = oneshot::channel();
        self.inner
            .write_tx
            .send(WriteMsg::PutBatch { items, ack: ack_tx })
            .map_err(|_| DiskCacheError::WriterStopped)?;
        ack_rx.await.map_err(|_| DiskCacheError::Panicked)?
    }

    pub async fn put(&self, addr: [u8; 32], data: Vec<u8>) -> Result<(), DiskCacheError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.inner
            .write_tx
            .send(WriteMsg::Put {
                addr,
                data,
                ack: ack_tx,
            })
            .map_err(|_| DiskCacheError::WriterStopped)?;
        ack_rx.await.map_err(|_| DiskCacheError::Panicked)?
    }

    pub async fn row_count(&self) -> Result<u64, DiskCacheError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.inner.read_tx[0]
            .send(ReadJob::Count { reply: reply_tx })
            .map_err(|_| DiskCacheError::WriterStopped)?;
        reply_rx.await.map_err(|_| DiskCacheError::Panicked)?
    }

    async fn pin_op<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<Result<T, DiskCacheError>>) -> PinOp,
    ) -> Result<T, DiskCacheError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.inner
            .write_tx
            .send(WriteMsg::Pin(build(ack_tx)))
            .map_err(|_| DiskCacheError::WriterStopped)?;
        ack_rx.await.map_err(|_| DiskCacheError::Panicked)?
    }

    /// Pin the collection rooted at `reference`: store every member
    /// chunk (upserting rows that aren't cached yet) and mark them all
    /// pin-protected. Idempotent — returns `Ok(false)` (writing
    /// nothing) when `reference` is already pinned, `Ok(true)` on a
    /// fresh pin. See [`PinOp`] for the eviction/budget invariant.
    pub async fn pin_collection(
        &self,
        reference: Vec<u8>,
        members: Vec<([u8; 32], Vec<u8>)>,
    ) -> Result<bool, DiskCacheError> {
        self.pin_op(|ack| PinOp::Collection {
            reference,
            members,
            ack,
        })
        .await
    }

    /// Drop the pin at `reference`; member chunks whose last pin is
    /// removed become evictable again (their `last_access` is
    /// refreshed so they age out like freshly-read rows rather than
    /// being first in line). Returns `Ok(false)` when `reference`
    /// wasn't pinned.
    pub async fn unpin(&self, reference: Vec<u8>) -> Result<bool, DiskCacheError> {
        self.pin_op(|ack| PinOp::Unpin { reference, ack }).await
    }

    /// Whether `reference` is a pinned root.
    pub async fn has_pin(&self, reference: Vec<u8>) -> Result<bool, DiskCacheError> {
        self.pin_op(|ack| PinOp::Has { reference, ack }).await
    }

    /// Every pinned root reference, in pin-creation order.
    pub async fn list_pins(&self) -> Result<Vec<Vec<u8>>, DiskCacheError> {
        self.pin_op(|ack| PinOp::List { ack }).await
    }

    /// Recorded member chunk addresses per pin — all pins when
    /// `reference` is `None`, else just that pin (empty result when it
    /// isn't pinned). Backs the `/pins/check` integrity walk.
    pub async fn pin_members(
        &self,
        reference: Option<Vec<u8>>,
    ) -> Result<PinMembership, DiskCacheError> {
        self.pin_op(|ack| PinOp::Members { reference, ack }).await
    }
}

fn apply_shared_pragmas(conn: &Connection, is_write: bool) -> Result<(), rusqlite::Error> {
    conn.pragma_update(None, "mmap_size", MMAP_BYTES)?;
    conn.pragma_update(None, "cache_size", CACHE_KIB)?;
    if is_write {
        conn.pragma_update(None, "page_size", 8192)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "synchronous", "NORMAL")?;
        conn.pragma_update(None, "busy_timeout", 5000)?;
    }
    Ok(())
}

fn open_write_connection(path: &Path) -> Result<Connection, rusqlite::Error> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    apply_shared_pragmas(&conn, true)?;
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS chunks (
                address     BLOB PRIMARY KEY,
                data        BLOB NOT NULL,
                size        INTEGER NOT NULL,
                last_access INTEGER NOT NULL,
                inserted_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS chunks_last_access_idx
                ON chunks(last_access);
            CREATE TABLE IF NOT EXISTS pins (
                reference  BLOB PRIMARY KEY,
                created_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS pin_members (
                reference BLOB NOT NULL,
                address   BLOB NOT NULL,
                PRIMARY KEY (reference, address)
            );",
    )?;
    // Additive migration for pre-pin databases: the pin refcount rides
    // on the chunks table so the eviction sweep can filter on it
    // without a join. `DEFAULT 0` keeps every pre-existing row
    // evictable, exactly as it was.
    let has_pin_count: bool = conn
        .query_row(
            "SELECT COUNT(*) FROM pragma_table_info('chunks') WHERE name = 'pin_count'",
            [],
            |row| row.get::<_, i64>(0),
        )
        .is_ok_and(|n| n > 0);
    if !has_pin_count {
        conn.execute_batch("ALTER TABLE chunks ADD COLUMN pin_count INTEGER NOT NULL DEFAULT 0;")?;
    }
    Ok(conn)
}

fn open_read_connection(path: &Path) -> Result<Connection, rusqlite::Error> {
    let conn = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    apply_shared_pragmas(&conn, false)?;
    Ok(conn)
}

fn read_worker_loop(path: PathBuf, rx: CbReceiver<ReadJob>, write_tx: CbSender<WriteMsg>) {
    let Ok(conn) = open_read_connection(&path) else {
        return;
    };
    let Ok(mut sel_stmt) =
        conn.prepare_cached("SELECT data, last_access FROM chunks WHERE address = ?1")
    else {
        return;
    };
    let Ok(mut count_stmt) = conn.prepare_cached("SELECT COUNT(*) FROM chunks") else {
        return;
    };

    while let Ok(job) = rx.recv() {
        match job {
            ReadJob::Shutdown => break,
            ReadJob::Get { addr, reply } => {
                let res = (|| -> Result<Option<Vec<u8>>, DiskCacheError> {
                    let row: Option<(Vec<u8>, i64)> = sel_stmt
                        .query_row(params![&addr[..]], |row| {
                            Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)?))
                        })
                        .optional()
                        .map_err(DiskCacheError::from)?;
                    let Some((data, last_access)) = row else {
                        return Ok(None);
                    };
                    let now = unix_now() as i64;
                    if now.saturating_sub(last_access) > LAST_ACCESS_REFRESH_MS {
                        let _ = write_tx.send(WriteMsg::Touch {
                            addr,
                            last_access: now,
                        });
                    }
                    Ok(Some(data))
                })();
                let _ = reply.send(res);
            }
            ReadJob::Count { reply } => {
                let res = (|| -> Result<u64, DiskCacheError> {
                    let n: i64 = count_stmt
                        .query_row([], |row| row.get(0))
                        .map_err(DiskCacheError::from)?;
                    Ok(n as u64)
                })();
                let _ = reply.send(res);
            }
        }
    }
}

fn process_put_batch_transaction(
    conn: &mut Connection,
    items: &[([u8; 32], Vec<u8>)],
    total_bytes: &Arc<AtomicU64>,
    total_rows: &Arc<AtomicU64>,
    max_bytes: u64,
    slack_bytes: u64,
) -> Result<(), DiskCacheError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    for (addr, data) in items {
        put_upsert_tx(&tx, *addr, data.as_slice(), total_bytes, total_rows)?;
    }
    tx.commit()?;
    let current = total_bytes.load(Ordering::Relaxed);
    if current > max_bytes {
        evict_to_slack(conn, total_bytes, total_rows, slack_bytes)?;
    }
    Ok(())
}

/// Pending put: chunk address, wire bytes, and the ack channel back
/// to the caller. Sized as a separate type alias to keep
/// `consume_put_touch_batch` readable.
type PutOp = (
    [u8; 32],
    Vec<u8>,
    oneshot::Sender<Result<(), DiskCacheError>>,
);

fn consume_put_touch_batch(
    conn: &mut Connection,
    batch: Vec<WriteMsg>,
    total_bytes: &Arc<AtomicU64>,
    total_rows: &Arc<AtomicU64>,
    max_bytes: u64,
    slack_bytes: u64,
) -> Result<bool, DiskCacheError> {
    let mut shutdown = false;
    let mut put_ops: Vec<PutOp> = Vec::new();
    let mut touches: Vec<([u8; 32], i64)> = Vec::new();

    for m in batch {
        match m {
            WriteMsg::Put { addr, data, ack } => put_ops.push((addr, data, ack)),
            WriteMsg::Touch { addr, last_access } => touches.push((addr, last_access)),
            WriteMsg::Shutdown => shutdown = true,
            // PutBatch / Pin never enter the coalescing buffer — the
            // writer loop flushes and handles them out-of-band.
            WriteMsg::PutBatch { .. } | WriteMsg::Pin(_) => {}
        }
    }

    if put_ops.is_empty() && touches.is_empty() {
        return Ok(shutdown);
    }

    let batch_res: Result<(), DiskCacheError> = (|| {
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
        for (addr, data, _) in &put_ops {
            put_upsert_tx(&tx, *addr, data, total_bytes, total_rows)?;
        }
        for (addr, last) in &touches {
            if let Err(e) = tx.execute(
                "UPDATE chunks SET last_access = ?1 WHERE address = ?2",
                params![last, &addr[..]],
            ) {
                trace!(
                    target: "ant_retrieval::disk_cache",
                    "last_access touch failed (non-fatal): {e}",
                );
            }
        }
        tx.commit()?;
        Ok(())
    })();

    match batch_res {
        Ok(()) => {
            let current = total_bytes.load(Ordering::Relaxed);
            if current > max_bytes {
                if let Err(e) = evict_to_slack(conn, total_bytes, total_rows, slack_bytes) {
                    for (_, _, ack) in put_ops {
                        let _ = ack.send(Err(e.clone()));
                    }
                    return Err(e);
                }
            }
            for (_, _, ack) in put_ops {
                let _ = ack.send(Ok(()));
            }
            Ok(shutdown)
        }
        Err(e) => {
            for (_, _, ack) in put_ops {
                let _ = ack.send(Err(e.clone()));
            }
            Err(e)
        }
    }
}

fn writer_main(
    path: PathBuf,
    rx: CbReceiver<WriteMsg>,
    ready_tx: SyncSender<()>,
    total_bytes: Arc<AtomicU64>,
    total_rows: Arc<AtomicU64>,
    max_bytes: u64,
    slack_bytes: u64,
) -> Result<(), DiskCacheError> {
    let mut conn = open_write_connection(&path)?;

    // Signal `open()` ready as soon as the schema is in place. The
    // initial backfill scan below (SUM(size) + COUNT(*)) touches every
    // row in the chunks table (sequential disk read on a cold page
    // cache: ~28 s on a 7 GB DB), and `antd` must be free to start its
    // libp2p listener + bootstrap dial before we finish that —
    // `time_to_first_peer_s` is otherwise dominated by this scan on
    // every cold start. Until backfill finishes, `total_bytes` and
    // `total_rows` stay at zero; that just means the eviction trigger
    // won't fire (no harm, we re-check inside every write batch via
    // `process_put_batch_transaction`), and `used_bytes()` /
    // `used_rows()` under-report for the first few seconds. Both are
    // acceptable; a frozen daemon is not.
    ready_tx
        .send(())
        .map_err(|_| DiskCacheError::WriterStopped)?;

    // Single combined backfill query: SQLite serves SUM + COUNT from
    // the same sequential scan, so we pay the cold-cache scan cost
    // once. Pinned rows are outside the budget (see [`PinOp`]), so the
    // byte total only sums evictable rows; the row count covers
    // everything.
    let (initial_total, initial_rows): (u64, u64) = conn
        .query_row(
            "SELECT COALESCE(SUM(CASE WHEN pin_count = 0 THEN size ELSE 0 END), 0), COUNT(*) \
             FROM chunks",
            [],
            |row| Ok((row.get::<_, i64>(0)? as u64, row.get::<_, i64>(1)? as u64)),
        )
        .unwrap_or((0, 0));
    total_bytes.store(initial_total, Ordering::Relaxed);
    total_rows.store(initial_rows, Ordering::Relaxed);

    'outer: loop {
        match rx.recv() {
            Err(_) | Ok(WriteMsg::Shutdown) => break,
            Ok(WriteMsg::PutBatch { items, ack }) => {
                let r = process_put_batch_transaction(
                    &mut conn,
                    &items,
                    &total_bytes,
                    &total_rows,
                    max_bytes,
                    slack_bytes,
                );
                let _ = ack.send(r);
            }
            Ok(WriteMsg::Pin(op)) => {
                process_pin_op(
                    &mut conn,
                    op,
                    &total_bytes,
                    &total_rows,
                    max_bytes,
                    slack_bytes,
                );
            }
            Ok(first) => {
                let mut batch = vec![first];
                while batch.len() < WRITE_BATCH_MAX {
                    match rx.try_recv() {
                        Ok(WriteMsg::Shutdown) => {
                            batch.push(WriteMsg::Shutdown);
                            break;
                        }
                        Ok(WriteMsg::Pin(op)) => {
                            let shutdown_after = match consume_put_touch_batch(
                                &mut conn,
                                std::mem::take(&mut batch),
                                &total_bytes,
                                &total_rows,
                                max_bytes,
                                slack_bytes,
                            ) {
                                Ok(s) => s,
                                Err(e) => {
                                    warn!(
                                        target: "ant_retrieval::disk_cache",
                                        "write batch ahead of pin op failed: {e}",
                                    );
                                    false
                                }
                            };
                            process_pin_op(
                                &mut conn,
                                op,
                                &total_bytes,
                                &total_rows,
                                max_bytes,
                                slack_bytes,
                            );
                            if shutdown_after {
                                break 'outer;
                            }
                            continue 'outer;
                        }
                        Ok(WriteMsg::PutBatch { items, ack }) => {
                            let shutdown_after = match consume_put_touch_batch(
                                &mut conn,
                                batch,
                                &total_bytes,
                                &total_rows,
                                max_bytes,
                                slack_bytes,
                            ) {
                                Ok(s) => s,
                                Err(e) => {
                                    let _ = ack.send(Err(e));
                                    continue 'outer;
                                }
                            };
                            let r = process_put_batch_transaction(
                                &mut conn,
                                &items,
                                &total_bytes,
                                &total_rows,
                                max_bytes,
                                slack_bytes,
                            );
                            let _ = ack.send(r);
                            if shutdown_after {
                                break 'outer;
                            }
                            continue 'outer;
                        }
                        Ok(m) => batch.push(m),
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => break 'outer,
                    }
                }

                let shutdown_after = consume_put_touch_batch(
                    &mut conn,
                    batch,
                    &total_bytes,
                    &total_rows,
                    max_bytes,
                    slack_bytes,
                )?;
                if shutdown_after {
                    break 'outer;
                }
            }
        }
    }

    Ok(())
}

fn put_upsert_tx(
    tx: &rusqlite::Transaction<'_>,
    addr: [u8; 32],
    data: &[u8],
    total_bytes: &AtomicU64,
    total_rows: &AtomicU64,
) -> Result<(), DiskCacheError> {
    let size = data.len() as u64;
    let now = unix_now();
    let existing: Option<(u64, i64)> = tx
        .query_row(
            "SELECT size, pin_count FROM chunks WHERE address = ?1",
            params![&addr[..]],
            |row| Ok((row.get::<_, i64>(0)? as u64, row.get::<_, i64>(1)?)),
        )
        .optional()?;

    tx.execute(
        "INSERT INTO chunks (address, data, size, last_access, inserted_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)
                 ON CONFLICT(address) DO UPDATE SET
                   data = excluded.data,
                   size = excluded.size,
                   last_access = excluded.last_access",
        params![&addr[..], data, size as i64, now as i64, now as i64],
    )?;

    // Pinned rows live outside the byte budget (see [`PinOp`]): an
    // upsert over one must not (re-)count its bytes.
    if let Some((old, pin_count)) = existing {
        if pin_count == 0 {
            update_total(total_bytes, size as i64 - old as i64);
        }
    } else {
        update_total(total_bytes, size as i64);
        total_rows.fetch_add(1, Ordering::Relaxed);
    }
    Ok(())
}

fn update_total(total: &AtomicU64, delta: i64) -> u64 {
    if delta >= 0 {
        total.fetch_add(delta as u64, Ordering::Relaxed) + delta as u64
    } else {
        let abs = (-delta) as u64;
        match total.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
            Some(v.saturating_sub(abs))
        }) {
            Ok(prev) => prev.saturating_sub(abs),
            Err(_) => total.load(Ordering::Relaxed),
        }
    }
}

fn evict_to_slack(
    conn: &mut Connection,
    total_bytes: &AtomicU64,
    total_rows: &AtomicU64,
    slack_bytes: u64,
) -> Result<(), DiskCacheError> {
    let current = total_bytes.load(Ordering::Relaxed);
    if current <= slack_bytes {
        return Ok(());
    }
    let need_to_free = current.saturating_sub(slack_bytes);

    // Pinned rows are exempt: the sweep only ever sees `pin_count = 0`
    // rows, and their bytes are the only ones in `total_bytes`, so the
    // slack target is reachable without touching pins (see [`PinOp`]).
    let mut stmt = conn
        .prepare(
            "SELECT address, size FROM chunks WHERE pin_count = 0 \
             ORDER BY last_access ASC, address ASC",
        )
        .map_err(DiskCacheError::from)?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, Vec<u8>>(0)?, row.get::<_, i64>(1)? as u64))
    })?;

    let mut victim_addrs: Vec<Vec<u8>> = Vec::new();
    let mut freed: u64 = 0;
    for row in rows {
        let (addr, size) = row.map_err(DiskCacheError::from)?;
        victim_addrs.push(addr);
        freed += size;
        if freed >= need_to_free {
            break;
        }
    }
    drop(stmt);

    if victim_addrs.is_empty() {
        return Ok(());
    }

    let tx = conn.unchecked_transaction().map_err(DiskCacheError::from)?;
    {
        let mut del = tx
            .prepare("DELETE FROM chunks WHERE address = ?1")
            .map_err(DiskCacheError::from)?;
        for addr in &victim_addrs {
            del.execute(params![addr]).map_err(DiskCacheError::from)?;
        }
    }
    tx.commit().map_err(DiskCacheError::from)?;

    update_total(total_bytes, -(freed as i64));
    // Decrement row count by the eviction batch size. Saturating
    // because total_rows is observably zero between open() returning
    // and the initial COUNT(*) backfill finishing — a write+eviction
    // racing the backfill must not underflow.
    let evicted = victim_addrs.len() as u64;
    let _ = total_rows.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
        Some(v.saturating_sub(evicted))
    });
    debug!(
        target: "ant_retrieval::disk_cache",
        evicted_rows = evicted,
        freed_bytes = freed,
        new_total = total_bytes.load(Ordering::Relaxed),
        new_rows = total_rows.load(Ordering::Relaxed),
        slack = slack_bytes,
        "disk cache eviction sweep complete",
    );
    Ok(())
}

/// Dispatch one [`PinOp`] on the writer connection. Failures are
/// reported to the caller through the op's ack channel; the writer
/// thread itself never dies over a pin error.
fn process_pin_op(
    conn: &mut Connection,
    op: PinOp,
    total_bytes: &Arc<AtomicU64>,
    total_rows: &Arc<AtomicU64>,
    max_bytes: u64,
    slack_bytes: u64,
) {
    match op {
        PinOp::Collection {
            reference,
            members,
            ack,
        } => {
            let r = pin_collection_tx(conn, &reference, &members, total_bytes, total_rows);
            let _ = ack.send(r);
        }
        PinOp::Unpin { reference, ack } => {
            let r = unpin_tx(conn, &reference, total_bytes);
            // Bytes returned to the budget may push it over the cap.
            if matches!(r, Ok(true)) && total_bytes.load(Ordering::Relaxed) > max_bytes {
                if let Err(e) = evict_to_slack(conn, total_bytes, total_rows, slack_bytes) {
                    warn!(
                        target: "ant_retrieval::disk_cache",
                        "post-unpin eviction sweep failed: {e}",
                    );
                }
            }
            let _ = ack.send(r);
        }
        PinOp::Has { reference, ack } => {
            let r = conn
                .query_row(
                    "SELECT COUNT(*) FROM pins WHERE reference = ?1",
                    params![&reference[..]],
                    |row| row.get::<_, i64>(0),
                )
                .map(|n| n > 0)
                .map_err(DiskCacheError::from);
            let _ = ack.send(r);
        }
        PinOp::List { ack } => {
            let r = (|| -> Result<Vec<Vec<u8>>, DiskCacheError> {
                let mut stmt =
                    conn.prepare("SELECT reference FROM pins ORDER BY created_at ASC, rowid ASC")?;
                let rows = stmt.query_map([], |row| row.get::<_, Vec<u8>>(0))?;
                let mut out = Vec::new();
                for row in rows {
                    out.push(row?);
                }
                Ok(out)
            })();
            let _ = ack.send(r);
        }
        PinOp::Members { reference, ack } => {
            let r = (|| -> Result<PinMembership, DiskCacheError> {
                let refs: Vec<Vec<u8>> = if let Some(r) = reference {
                    let pinned: i64 = conn.query_row(
                        "SELECT COUNT(*) FROM pins WHERE reference = ?1",
                        params![&r[..]],
                        |row| row.get(0),
                    )?;
                    if pinned > 0 {
                        vec![r]
                    } else {
                        Vec::new()
                    }
                } else {
                    let mut stmt = conn
                        .prepare("SELECT reference FROM pins ORDER BY created_at ASC, rowid ASC")?;
                    let rows = stmt.query_map([], |row| row.get::<_, Vec<u8>>(0))?;
                    rows.collect::<Result<_, _>>()?
                };
                let mut out = Vec::with_capacity(refs.len());
                let mut stmt = conn.prepare(
                    "SELECT address FROM pin_members WHERE reference = ?1 ORDER BY rowid ASC",
                )?;
                for r in refs {
                    let rows = stmt.query_map(params![&r[..]], |row| row.get::<_, Vec<u8>>(0))?;
                    let mut members = Vec::new();
                    for row in rows {
                        let raw = row?;
                        if let Ok(addr) = <[u8; 32]>::try_from(raw.as_slice()) {
                            members.push(addr);
                        }
                    }
                    out.push((r, members));
                }
                Ok(out)
            })();
            let _ = ack.send(r);
        }
    }
}

/// Record a pin collection in one transaction. Returns `Ok(false)`
/// without writing anything when `reference` is already pinned.
fn pin_collection_tx(
    conn: &mut Connection,
    reference: &[u8],
    members: &[([u8; 32], Vec<u8>)],
    total_bytes: &Arc<AtomicU64>,
    total_rows: &Arc<AtomicU64>,
) -> Result<bool, DiskCacheError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let already: i64 = tx.query_row(
        "SELECT COUNT(*) FROM pins WHERE reference = ?1",
        params![reference],
        |row| row.get(0),
    )?;
    if already > 0 {
        return Ok(false);
    }
    tx.execute(
        "INSERT INTO pins (reference, created_at) VALUES (?1, ?2)",
        params![reference, unix_now() as i64],
    )?;

    let mut seen = std::collections::HashSet::with_capacity(members.len());
    let mut new_rows = 0u64;
    let mut bytes_leaving_budget = 0u64;
    let now = unix_now() as i64;
    for (addr, wire) in members {
        if !seen.insert(*addr) {
            continue;
        }
        tx.execute(
            "INSERT INTO pin_members (reference, address) VALUES (?1, ?2)",
            params![reference, &addr[..]],
        )?;
        let existing: Option<(u64, i64)> = tx
            .query_row(
                "SELECT size, pin_count FROM chunks WHERE address = ?1",
                params![&addr[..]],
                |row| Ok((row.get::<_, i64>(0)? as u64, row.get::<_, i64>(1)?)),
            )
            .optional()?;
        match existing {
            None => {
                // Fresh row, pinned from birth: never enters the budget.
                tx.execute(
                    "INSERT INTO chunks (address, data, size, last_access, inserted_at, pin_count)
                     VALUES (?1, ?2, ?3, ?4, ?5, 1)",
                    params![&addr[..], wire, wire.len() as i64, now, now],
                )?;
                new_rows += 1;
            }
            Some((size, 0)) => {
                // First pin over a cached row: bytes leave the budget.
                tx.execute(
                    "UPDATE chunks SET pin_count = 1 WHERE address = ?1",
                    params![&addr[..]],
                )?;
                bytes_leaving_budget += size;
            }
            Some((_, n)) => {
                tx.execute(
                    "UPDATE chunks SET pin_count = ?1 WHERE address = ?2",
                    params![n + 1, &addr[..]],
                )?;
            }
        }
    }
    tx.commit()?;
    total_rows.fetch_add(new_rows, Ordering::Relaxed);
    update_total(total_bytes, -(bytes_leaving_budget as i64));
    Ok(true)
}

/// Remove a pin in one transaction. Returns `Ok(false)` when
/// `reference` wasn't pinned. Chunks whose last pin drops re-enter the
/// byte budget (the caller re-checks the cap afterwards).
fn unpin_tx(
    conn: &mut Connection,
    reference: &[u8],
    total_bytes: &Arc<AtomicU64>,
) -> Result<bool, DiskCacheError> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let pinned: i64 = tx.query_row(
        "SELECT COUNT(*) FROM pins WHERE reference = ?1",
        params![reference],
        |row| row.get(0),
    )?;
    if pinned == 0 {
        return Ok(false);
    }
    let members: Vec<Vec<u8>> = {
        let mut stmt = tx.prepare("SELECT address FROM pin_members WHERE reference = ?1")?;
        let rows = stmt.query_map(params![reference], |row| row.get::<_, Vec<u8>>(0))?;
        rows.collect::<Result<_, _>>()?
    };
    let mut bytes_returning = 0u64;
    for addr in &members {
        let existing: Option<(u64, i64)> = tx
            .query_row(
                "SELECT size, pin_count FROM chunks WHERE address = ?1",
                params![&addr[..]],
                |row| Ok((row.get::<_, i64>(0)? as u64, row.get::<_, i64>(1)?)),
            )
            .optional()?;
        match existing {
            Some((size, n)) if n <= 1 => {
                tx.execute(
                    "UPDATE chunks SET pin_count = 0, last_access = ?1 WHERE address = ?2",
                    params![unix_now() as i64, &addr[..]],
                )?;
                if n == 1 {
                    bytes_returning += size;
                }
            }
            Some((_, n)) => {
                tx.execute(
                    "UPDATE chunks SET pin_count = ?1 WHERE address = ?2",
                    params![n - 1, &addr[..]],
                )?;
            }
            None => {}
        }
    }
    tx.execute(
        "DELETE FROM pin_members WHERE reference = ?1",
        params![reference],
    )?;
    tx.execute("DELETE FROM pins WHERE reference = ?1", params![reference])?;
    tx.commit()?;
    update_total(total_bytes, bytes_returning as i64);
    Ok(true)
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ant_crypto::cac_new;
    use rusqlite::params as rparams;
    use tempfile::tempdir;

    fn make_chunk(payload: &[u8]) -> ([u8; 32], Vec<u8>) {
        cac_new(payload).expect("cac_new")
    }

    #[tokio::test]
    async fn put_batch_round_trips() {
        let dir = tempdir().unwrap();
        let cache = DiskChunkCache::open(dir.path().join("batch.sqlite"), 1 << 20).unwrap();
        let a = make_chunk(b"a");
        let b = make_chunk(b"b");
        cache
            .put_batch(vec![(a.0, a.1.clone()), (b.0, b.1.clone())])
            .await
            .unwrap();
        assert_eq!(cache.get(a.0).await.unwrap(), Some(a.1));
        assert_eq!(cache.get(b.0).await.unwrap(), Some(b.1));
        assert_eq!(cache.row_count().await.unwrap(), 2);
    }

    #[tokio::test]
    async fn put_then_get_round_trips() {
        let dir = tempdir().unwrap();
        let cache = DiskChunkCache::open(dir.path().join("c.sqlite"), 1 << 20).unwrap();
        let (addr, wire) = make_chunk(b"hello disk");

        cache.put(addr, wire.clone()).await.unwrap();
        let got = cache.get(addr).await.unwrap();
        assert_eq!(got, Some(wire.clone()));
        assert_eq!(cache.used_bytes(), wire.len() as u64);
        assert_eq!(cache.row_count().await.unwrap(), 1);
    }

    #[tokio::test]
    async fn rows_persist_across_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("persist.sqlite");
        let (addr, wire) = make_chunk(b"persistence test");

        {
            let cache = DiskChunkCache::open(&path, 1 << 20).unwrap();
            cache.put(addr, wire.clone()).await.unwrap();
            assert_eq!(cache.used_bytes(), wire.len() as u64);
        }

        let cache = DiskChunkCache::open(&path, 1 << 20).unwrap();
        // `open()` returns before the writer's backfill scan fills
        // `total_bytes`; any writer round-trip is queued behind that scan,
        // so awaiting one makes the `used_bytes()` read below race-free.
        cache.list_pins().await.unwrap();
        assert_eq!(cache.used_bytes(), wire.len() as u64);
        let got = cache.get(addr).await.unwrap();
        assert_eq!(got, Some(wire));
    }

    #[tokio::test]
    async fn evicts_oldest_by_last_access() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("evict.sqlite");
        let cache = DiskChunkCache::open(&path, 12 * 1024).unwrap();

        let chunks: Vec<([u8; 32], Vec<u8>)> =
            (0..3u8).map(|i| make_chunk(&vec![i; 4096])).collect();

        cache.put(chunks[0].0, chunks[0].1.clone()).await.unwrap();
        cache.put(chunks[1].0, chunks[1].1.clone()).await.unwrap();

        let now = unix_now() as i64;
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute(
                "UPDATE chunks SET last_access = ?1 WHERE address = ?2",
                rparams![now - 20 * 60_000, &chunks[0].0[..]],
            )
            .unwrap();
            conn.execute(
                "UPDATE chunks SET last_access = ?1 WHERE address = ?2",
                rparams![now - 10 * 60_000, &chunks[1].0[..]],
            )
            .unwrap();
        }

        assert!(cache.get(chunks[0].0).await.unwrap().is_some());

        cache.put(chunks[2].0, chunks[2].1.clone()).await.unwrap();

        assert!(
            cache.used_bytes() <= 12 * 1024,
            "post-eviction total {} bytes must fit the cap (12 KiB)",
            cache.used_bytes(),
        );
        assert!(cache.get(chunks[0].0).await.unwrap().is_some());
        assert!(cache.get(chunks[1].0).await.unwrap().is_none());
        assert!(cache.get(chunks[2].0).await.unwrap().is_some());
        // The fast-path row counter must stay in lock-step with the
        // SQL COUNT(*). One eviction victim → counter goes 3 → 2.
        assert_eq!(cache.used_rows(), 2);
        assert_eq!(cache.row_count().await.unwrap(), 2);
    }

    /// Bee-style trust: a hand-corrupted row is still returned on read
    /// (same as bee chunkstore returning sharky bytes for a retrieval key).
    #[tokio::test]
    async fn corrupt_disk_row_is_trusted() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("corrupt.sqlite");
        let cache = DiskChunkCache::open(&path, 1 << 20).unwrap();
        let (addr, wire) = make_chunk(b"trust me");
        drop(cache);

        let mut bad = wire.clone();
        bad[8] ^= 0x01;
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute(
                "INSERT INTO chunks (address, data, size, last_access, inserted_at)
                     VALUES (?1, ?2, ?3, ?4, ?5)",
                rparams![&addr[..], &bad, bad.len() as i64, 0i64, 0i64],
            )
            .unwrap();
        }
        let cache = DiskChunkCache::open(&path, 1 << 20).unwrap();
        assert_eq!(cache.row_count().await.unwrap(), 1);

        let got = cache.get(addr).await.unwrap();
        assert_eq!(got, Some(bad));
    }

    #[tokio::test]
    async fn upsert_does_not_double_count() {
        let dir = tempdir().unwrap();
        let cache = DiskChunkCache::open(dir.path().join("upsert.sqlite"), 1 << 20).unwrap();
        let (addr, wire) = make_chunk(b"upsert");

        cache.put(addr, wire.clone()).await.unwrap();
        cache.put(addr, wire.clone()).await.unwrap();
        cache.put(addr, wire.clone()).await.unwrap();
        assert_eq!(cache.row_count().await.unwrap(), 1);
        // Fast-path row counter must agree with the SQL truth.
        assert_eq!(cache.used_rows(), 1);
        assert_eq!(cache.used_bytes(), wire.len() as u64);
    }

    async fn read_last_access(cache: &DiskChunkCache, addr: [u8; 32]) -> i64 {
        let path = cache.path().to_path_buf();
        tokio::task::spawn_blocking(move || {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.query_row(
                "SELECT last_access FROM chunks WHERE address = ?1",
                rparams![&addr[..]],
                |row| row.get::<_, i64>(0),
            )
            .unwrap()
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn fresh_last_access_is_not_rewritten() {
        let dir = tempdir().unwrap();
        let cache = DiskChunkCache::open(dir.path().join("touch-skip.sqlite"), 1 << 20).unwrap();
        let (addr, wire) = make_chunk(b"hot read");

        cache.put(addr, wire.clone()).await.unwrap();
        let after_put = read_last_access(&cache, addr).await;

        for _ in 0..10 {
            assert_eq!(cache.get(addr).await.unwrap().as_deref(), Some(&wire[..]));
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }

        let after_reads = read_last_access(&cache, addr).await;
        assert_eq!(
            after_put, after_reads,
            "ten consecutive hot reads inside the refresh window must leave \
             last_access untouched (was {after_put}, now {after_reads})",
        );
    }

    /// The core pin invariant: pinned rows are exempt from the
    /// eviction sweep (and outside the byte budget), and unpinning
    /// restores evictability.
    #[tokio::test]
    async fn eviction_skips_pinned_rows_and_unpin_restores_evictability() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("pin-evict.sqlite");
        // Cap fits ~2 chunks of unpinned bytes.
        let cache = DiskChunkCache::open(&path, 9 * 1024).unwrap();

        let pinned = make_chunk(&vec![0xaau8; 4096]);
        cache.put(pinned.0, pinned.1.clone()).await.unwrap();
        let newly = cache
            .pin_collection(pinned.0.to_vec(), vec![(pinned.0, pinned.1.clone())])
            .await
            .unwrap();
        assert!(newly, "first pin is fresh");
        assert!(
            !cache
                .pin_collection(pinned.0.to_vec(), vec![(pinned.0, pinned.1.clone())])
                .await
                .unwrap(),
            "second pin of the same root is a no-op"
        );
        // Pinned bytes left the budget entirely.
        assert_eq!(cache.used_bytes(), 0, "pinned bytes are outside the budget");

        // Backdate the pinned row so LRU order would evict it first if
        // the sweep could see it.
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute(
                "UPDATE chunks SET last_access = 0 WHERE address = ?1",
                rparams![&pinned.0[..]],
            )
            .unwrap();
        }

        // Fill past the cap with unpinned chunks → eviction fires.
        let fillers: Vec<([u8; 32], Vec<u8>)> =
            (1..=3u8).map(|i| make_chunk(&vec![i; 4096])).collect();
        for (addr, wire) in &fillers {
            cache.put(*addr, wire.clone()).await.unwrap();
        }
        assert!(
            cache.get(pinned.0).await.unwrap().is_some(),
            "pinned chunk survives an eviction sweep even as the LRU-oldest row"
        );
        assert!(
            cache.used_bytes() <= 9 * 1024,
            "unpinned bytes still respect the cap"
        );

        // Pin bookkeeping is queryable.
        assert!(cache.has_pin(pinned.0.to_vec()).await.unwrap());
        assert_eq!(cache.list_pins().await.unwrap(), vec![pinned.0.to_vec()]);
        let members = cache.pin_members(None).await.unwrap();
        assert_eq!(members.len(), 1);
        assert_eq!(members[0].1, vec![pinned.0]);

        // Unpin → evictable again: keep it LRU-oldest and overflow.
        assert!(cache.unpin(pinned.0.to_vec()).await.unwrap());
        assert!(!cache.unpin(pinned.0.to_vec()).await.unwrap(), "idempotent");
        assert!(!cache.has_pin(pinned.0.to_vec()).await.unwrap());
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute(
                "UPDATE chunks SET last_access = 0 WHERE address = ?1",
                rparams![&pinned.0[..]],
            )
            .unwrap();
        }
        let more: Vec<([u8; 32], Vec<u8>)> =
            (10..=12u8).map(|i| make_chunk(&vec![i; 4096])).collect();
        for (addr, wire) in &more {
            cache.put(*addr, wire.clone()).await.unwrap();
        }
        assert!(
            cache.get(pinned.0).await.unwrap().is_none(),
            "after unpin the (backdated) chunk is evicted like any other row"
        );
    }

    /// Pins are `SQLite` rows: they survive a close + reopen, and the
    /// budget backfill keeps treating pinned bytes as out-of-budget.
    #[tokio::test]
    async fn pins_survive_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("pin-persist.sqlite");
        let (addr, wire) = make_chunk(b"pinned across restarts");
        {
            let cache = DiskChunkCache::open(&path, 1 << 20).unwrap();
            cache
                .pin_collection(addr.to_vec(), vec![(addr, wire.clone())])
                .await
                .unwrap();
        }
        let cache = DiskChunkCache::open(&path, 1 << 20).unwrap();
        assert!(cache.has_pin(addr.to_vec()).await.unwrap());
        assert_eq!(cache.list_pins().await.unwrap(), vec![addr.to_vec()]);
        assert_eq!(cache.get(addr).await.unwrap(), Some(wire));
        assert_eq!(
            cache.used_bytes(),
            0,
            "backfill keeps pinned bytes out of the budget"
        );
    }

    /// Two pins sharing a chunk: the shared chunk stays pin-protected
    /// until the *last* covering pin is removed (refcount semantics,
    /// like bee's pinstore).
    #[tokio::test]
    async fn shared_chunk_stays_pinned_until_last_pin_drops() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("pin-shared.sqlite");
        let cache = DiskChunkCache::open(&path, 1 << 20).unwrap();
        let shared = make_chunk(b"shared leaf");
        let root_a = make_chunk(b"root a");
        let root_b = make_chunk(b"root b");
        cache
            .pin_collection(
                root_a.0.to_vec(),
                vec![(root_a.0, root_a.1.clone()), (shared.0, shared.1.clone())],
            )
            .await
            .unwrap();
        cache
            .pin_collection(
                root_b.0.to_vec(),
                vec![(root_b.0, root_b.1.clone()), (shared.0, shared.1.clone())],
            )
            .await
            .unwrap();
        cache.unpin(root_a.0.to_vec()).await.unwrap();

        let pin_count: i64 = {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.query_row(
                "SELECT pin_count FROM chunks WHERE address = ?1",
                rparams![&shared.0[..]],
                |row| row.get(0),
            )
            .unwrap()
        };
        assert_eq!(pin_count, 1, "one covering pin left");
        cache.unpin(root_b.0.to_vec()).await.unwrap();
        let pin_count: i64 = {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.query_row(
                "SELECT pin_count FROM chunks WHERE address = ?1",
                rparams![&shared.0[..]],
                |row| row.get(0),
            )
            .unwrap()
        };
        assert_eq!(pin_count, 0, "evictable again after the last unpin");
    }

    #[tokio::test]
    async fn stale_last_access_is_refreshed() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("touch-stale.sqlite");
        let cache = DiskChunkCache::open(&path, 1 << 20).unwrap();
        let (addr, wire) = make_chunk(b"cool read");
        cache.put(addr, wire.clone()).await.unwrap();

        let backdated = unix_now() as i64 - 10 * 60_000;
        {
            let conn = rusqlite::Connection::open(&path).unwrap();
            conn.execute(
                "UPDATE chunks SET last_access = ?1 WHERE address = ?2",
                rparams![backdated, &addr[..]],
            )
            .unwrap();
        }

        assert!(cache.get(addr).await.unwrap().is_some());
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let after = read_last_access(&cache, addr).await;
        assert!(
            after > backdated,
            "stale last_access must be refreshed on read \
             (was {backdated}, now {after})",
        );
    }
}
