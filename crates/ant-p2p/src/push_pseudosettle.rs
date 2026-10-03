//! Cheque-less pushsync settlement: mirror push debt into the shared
//! [`Accounting`] so the pseudosettle driver refreshes upload peers.
//!
//! Perf-lab **Experiment 1** (the 512 MiB stall). Without a chequebook
//! the daemon has NO settlement on the push path at all: the only
//! [`PushsyncSettlement`] implementation was the SWAP service, and the
//! pseudosettle driver's scan covers retrieval debt (the `Accounting`
//! mirror) only. Bee meanwhile debits us for every pushed chunk in the
//! same per-peer ledger it uses for retrieval; once our unsettled debt
//! crosses its payment threshold + tolerance, it answers with
//! overdrafts and RSTs. Measured baseline signature (2026-07-05):
//! within one 256 MiB upload, throughput decayed 370 → ~145 KiB/s as
//! debt accumulated — extrapolating to the documented "uploads run for
//! ~20 K chunks then stall" failure at 512 MiB. Connection churn
//! partially masks it (bee resets per-peer accounting on reconnect),
//! which is why smaller uploads look healthy.
//!
//! Bee's own light nodes settle upload debt **time-based** via
//! `/swarm/pseudosettle` — no cheques, no chain, no BZZ. Ant already
//! runs the full pseudosettle driver for retrieval; this adapter just
//! records push debits in the same `Accounting` mirror, which
//! automatically fires the driver's `HotHint` when a peer's mirrored
//! debt crosses `HOT_DEBT_THRESHOLD`.
//!
//! DEFAULT ON since the perf-lab verdict (collapse-to-zero became a
//! stable plateau; see PERF-LAB.md exp 1). `ANT_PUSH_PSEUDOSETTLE=0`
//! disables it on a node without a chequebook (the A/B control arm).
//!
//! With a chequebook it is how uploads pay, too (issue #127): the
//! mirror is bee's one balance per peer, so the payer installed in it
//! (`PushsyncSwap`, while `swap-enable` is on and the chequebook has
//! funds) settles push debt exactly as it settles retrieval debt — the
//! refresh first, then a cheque priced `units × exchange + deduction`
//! once the debt reaches the early-payment threshold. Recording the
//! debit is what triggers that cheque (`Accounting::debit`), and it is
//! recorded even past the overdraft limit, since the push already
//! happened. See `behaviour::push_settlement`.

use ant_retrieval::accounting::Accounting;
use ant_retrieval::PushsyncSettlement;
use async_trait::async_trait;
use libp2p::PeerId;
use std::sync::Arc;

/// Adapter: push debits → the shared retrieval `Accounting` mirror.
pub struct PushPseudosettle(Arc<Accounting>);

impl PushPseudosettle {
    #[must_use]
    pub fn new(accounting: Arc<Accounting>) -> Self {
        Self(accounting)
    }

    /// Default ON; `ANT_PUSH_PSEUDOSETTLE=0`/`false` opts out (the
    /// A/B control arm).
    #[must_use]
    pub fn enabled_by_env() -> bool {
        match std::env::var("ANT_PUSH_PSEUDOSETTLE") {
            Err(_) => true,
            Ok(v) => {
                let v = v.trim();
                !v.is_empty() && v != "0" && !v.eq_ignore_ascii_case("false")
            }
        }
    }
}

#[async_trait]
impl PushsyncSettlement for PushPseudosettle {
    async fn note_pushsync(&self, peer: PeerId, price: u64) {
        if price == 0 {
            // Pre-flight call from `push_stamped_chunk` (price unknown
            // yet). Nothing to record; the post-receipt call carries
            // the real price.
            return;
        }
        // Mirror the debit, past the overdraft limit too: the chunk was
        // pushed and bee has debited it, so dropping it would leave the
        // mirror (the only record of upload debt the payer settles) short
        // of bee's view, and cheques would underpay. `debit` fires the
        // hot hint and starts a cheque when one is due (PR #126 R1-M2).
        self.0.debit(peer, price);
    }

    fn forget(&self, peer: &PeerId) {
        self.0.forget(peer);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ant_retrieval::accounting::OVERDRAFT_LIMIT;

    /// Every receipted push lands in the mirror, past the overdraft limit
    /// too: bee debited it, and the payer settles only what the mirror
    /// holds (PR #126 R1-M2).
    #[tokio::test]
    async fn push_debits_past_the_overdraft_limit_are_kept() {
        let acc = Arc::new(Accounting::new());
        let settle = PushPseudosettle::new(acc.clone());
        let peer = PeerId::random();
        settle.note_pushsync(peer, 0).await;
        assert_eq!(acc.debug_snapshot(&peer), None, "pre-flight: nothing");
        let n = OVERDRAFT_LIMIT / 100_000 + 5;
        for _ in 0..n {
            settle.note_pushsync(peer, 100_000).await;
        }
        assert_eq!(acc.debug_snapshot(&peer), Some((n * 100_000, 0)));
    }
}
