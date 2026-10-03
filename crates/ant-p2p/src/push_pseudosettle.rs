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
//! once the debt reaches the early-payment threshold. Reserving the
//! debit is what triggers that cheque (`Accounting::try_reserve`), even
//! when the reservation itself is refused. See
//! `behaviour::push_settlement`.

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
        // Mirror the debit. `try_reserve` refusing means our mirror is
        // already at the reserve ceiling for this peer — the hot hint
        // has fired and the driver is refreshing; bee's view is ahead
        // of ours in that state, so dropping the record (rather than
        // blocking the push path) is the honest cheap option.
        if let Some(guard) = self.0.try_reserve(peer, price) {
            guard.apply();
        }
    }

    fn forget(&self, peer: &PeerId) {
        self.0.forget(peer);
    }
}
