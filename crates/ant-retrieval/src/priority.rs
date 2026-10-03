//! Head-of-window retrieval priority for the streaming joiner (issue #46).
//!
//! The streaming joiner ([`crate::join_to_sender_range`]) hands bytes to
//! its consumer strictly in document order, but fetches up to
//! [`FETCH_FANOUT`](crate::joiner) sibling subtrees at once, and a
//! sibling that has finished frees its slot for the next one whether or
//! not the merger has reached it yet. So while the chunk the consumer is
//! waiting for is slow (a starved credit wait, a retry, a slow peer), the
//! other slots keep pulling in the rest of the file. On a cold node every
//! fetch competes for the same peer credit and the same per-request
//! permits, the chunk at the head gets no more of either than one a
//! megabyte ahead of it, and a download spends most of its time stalled
//! near byte 0 and then delivers almost everything at once.
//!
//! This module tells the fetcher where a chunk sits relative to the
//! consumer, so [`crate::RoutingFetcher`] can serve the head first:
//!
//! - A [`ReadHead`] is the consumer's position in one streaming request:
//!   the file offset of the next byte the joiner has not yet handed to
//!   the request's output channel. The joiner advances it as it sends.
//! - Every data-chunk fetch of the streaming joiner runs with its chunk's
//!   file offset in scope ([`at_offset`]). The chunk is at the **head**
//!   when it starts less than [`HEAD_WINDOW`] past the read head;
//!   otherwise it is **look-ahead** ([`Priority`]).
//!
//! Both live in task-locals, set by the joiner around the whole join and
//! around each data-chunk fetch, so every fetch made for that chunk
//! (the direct fetch, its hedges, a Reed-Solomon recovery sweep it
//! triggers) is ranked the same way without the [`crate::ChunkFetcher`]
//! trait or the decoder having to carry it. A fetch made outside a
//! streaming join (buffered joins, manifest walks, feeds, roots, the
//! chunk API) has no position: [`Priority::Unranked`], and the fetcher
//! treats it exactly as before.
//!
//! The rank is read afresh at every decision the fetcher makes, so a
//! look-ahead fetch that is still waiting when the consumer catches up
//! with it is served as the head from its next decision on (its next
//! credit check, at most [`crate::OVERDRAFT_REFRESH`] away, or its next
//! [`HEAD_RECHECK`] at the per-request permit queue).

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// How far past the read head a chunk may start and still count as the
/// head of the window: 16 chunks, one [`FETCH_FANOUT`](crate::joiner)'s
/// worth. More than the single chunk the consumer is blocked on, so the
/// next few chunks are already being served first by the time the head
/// reaches them (a waiting fetch notices its new rank only at its next
/// credit check, up to [`crate::OVERDRAFT_REFRESH`] later); small enough
/// that, with several downloads running, the heads stay a small share
/// of everything in flight.
pub const HEAD_WINDOW: u64 = 16 * 4096;

/// How often a look-ahead fetch queued for its request's permit
/// re-checks whether it has become the head (and may skip the queue).
pub const HEAD_RECHECK: Duration = Duration::from_millis(100);

/// The consumer's read position in one streaming join: the file offset
/// of the next byte not yet handed to the output channel. Cheap to
/// clone; clones share the position.
#[derive(Debug, Clone)]
pub struct ReadHead(Arc<AtomicU64>);

impl ReadHead {
    /// A read head at file offset `start` (`0`, or a range's first byte).
    #[must_use]
    pub fn new(start: u64) -> Self {
        Self(Arc::new(AtomicU64::new(start)))
    }

    /// The consumer has been handed `bytes` more.
    pub fn advance(&self, bytes: u64) {
        self.0.fetch_add(bytes, Ordering::Relaxed);
    }

    /// The file offset of the next byte the consumer needs.
    #[must_use]
    pub fn position(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    /// Rank of a chunk that starts at file offset `offset`.
    #[must_use]
    pub fn rank(&self, offset: u64) -> Priority {
        if offset < self.position().saturating_add(HEAD_WINDOW) {
            Priority::Head
        } else {
            Priority::LookAhead
        }
    }
}

/// Where the fetch running on the current task sits relative to its
/// consumer; see the module docs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Priority {
    /// Not part of a streaming join: no consumer position to rank
    /// against. Served exactly as before issue #46.
    Unranked,
    /// Within [`HEAD_WINDOW`] of the read head: what the consumer needs
    /// next. May use every peer's full overdraft limit, and skips the
    /// per-request permit queue.
    Head,
    /// Further ahead. Leaves each peer
    /// [`HEAD_CREDIT_RESERVE`](crate::accounting::HEAD_CREDIT_RESERVE) of
    /// credit for head fetches, and waits its turn for a per-request
    /// permit.
    LookAhead,
}

tokio::task_local! {
    static READ_HEAD: ReadHead;
    static CHUNK_OFFSET: u64;
}

/// Run `fut` (a whole streaming join) with `head` as its read head.
pub(crate) async fn with_read_head<F: Future>(head: ReadHead, fut: F) -> F::Output {
    READ_HEAD.scope(head, fut).await
}

/// Run `fut` (every fetch for the data chunk at file offset `offset`)
/// with that offset in scope.
pub(crate) async fn at_offset<F: Future>(offset: u64, fut: F) -> F::Output {
    CHUNK_OFFSET.scope(offset, fut).await
}

/// Rank of the fetch running on the current task right now.
#[must_use]
pub fn current() -> Priority {
    let Ok(offset) = CHUNK_OFFSET.try_with(|o| *o) else {
        return Priority::Unranked;
    };
    READ_HEAD
        .try_with(|head| head.rank(offset))
        .unwrap_or(Priority::Unranked)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn rank_follows_the_read_head() {
        assert_eq!(current(), Priority::Unranked);
        let head = ReadHead::new(0);
        let h = head.clone();
        with_read_head(head.clone(), async move {
            // An offset without a read head, or a read head without an
            // offset, is unranked.
            assert_eq!(current(), Priority::Unranked);
            at_offset(0, async {
                assert_eq!(current(), Priority::Head);
            })
            .await;
            at_offset(HEAD_WINDOW - 1, async {
                assert_eq!(current(), Priority::Head);
            })
            .await;
            at_offset(HEAD_WINDOW, async {
                assert_eq!(current(), Priority::LookAhead);
                // The consumer catches up: the same fetch is now the head.
                h.advance(4096);
                assert_eq!(current(), Priority::Head);
            })
            .await;
        })
        .await;
        at_offset(0, async {
            assert_eq!(current(), Priority::Unranked);
        })
        .await;
        assert_eq!(head.position(), 4096);
    }
}
