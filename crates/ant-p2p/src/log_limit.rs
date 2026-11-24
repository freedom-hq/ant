//! Per-key rate limit for `warn!` lines a remote peer can trigger.
//!
//! A misbehaving peer can repeat the same protocol error on every round
//! (an over-acked pseudosettle refresh, a bad pullsync `topmost` on each
//! puller respawn), and an unthrottled `warn!` per occurrence lets it
//! flood the log. [`WarnLimiter`] lets the first occurrence per key
//! through at `warn` and reports repeats within [`WarnLimiter::interval`]
//! as suppressed, so the call site logs those at `debug` instead.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Mutex, PoisonError};
use std::time::{Duration, Instant};

/// Above this many tracked keys, expired entries are swept on insert so
/// the map doesn't grow with every peer that ever misbehaved once.
const SWEEP_ABOVE: usize = 256;

pub(crate) struct WarnLimiter<K> {
    interval: Duration,
    last: Mutex<Option<HashMap<K, Instant>>>,
}

impl<K: Eq + Hash> WarnLimiter<K> {
    pub(crate) const fn new(interval: Duration) -> Self {
        Self {
            interval,
            last: Mutex::new(None),
        }
    }

    /// `true` if `key` hasn't been let through within the interval
    /// before `now` (and records `now` as its last warn); `false` for a
    /// repeat inside the interval, which the caller should log at
    /// `debug`.
    pub(crate) fn allow(&self, key: K, now: Instant) -> bool {
        let mut guard = self.last.lock().unwrap_or_else(PoisonError::into_inner);
        let map = guard.get_or_insert_with(HashMap::new);
        if let Some(t) = map.get(&key) {
            if now.saturating_duration_since(*t) < self.interval {
                return false;
            }
        }
        if map.len() >= SWEEP_ABOVE {
            let interval = self.interval;
            map.retain(|_, t| now.saturating_duration_since(*t) < interval);
        }
        map.insert(key, now);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_warn_passes_repeats_are_suppressed_until_interval() {
        let lim = WarnLimiter::new(Duration::from_secs(600));
        let t0 = Instant::now();
        assert!(lim.allow(1u32, t0));
        assert!(!lim.allow(1, t0 + Duration::from_secs(1)));
        assert!(!lim.allow(1, t0 + Duration::from_secs(599)));
        // A different key is independent.
        assert!(lim.allow(2, t0 + Duration::from_secs(1)));
        // After the interval the same key warns again.
        assert!(lim.allow(1, t0 + Duration::from_secs(600)));
        assert!(!lim.allow(1, t0 + Duration::from_secs(601)));
    }

    #[test]
    fn expired_keys_are_swept_once_the_map_is_large() {
        let lim = WarnLimiter::new(Duration::from_secs(10));
        let t0 = Instant::now();
        for k in 0..SWEEP_ABOVE as u32 {
            assert!(lim.allow(k, t0));
        }
        assert!(lim.allow(u32::MAX, t0 + Duration::from_secs(20)));
        let len = lim.last.lock().unwrap().as_ref().unwrap().len();
        assert_eq!(len, 1, "expired entries swept, only the fresh one left");
    }
}
