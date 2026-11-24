//! Test-only global allocator that tracks this thread's live heap bytes
//! and their peak, so a test can assert how much a single-threaded call
//! allocates at its worst (issue #286: a malformed chunk tree must not
//! reserve memory sized by its declared span).
//!
//! Counters are thread-local, so tests running in parallel on other
//! threads don't disturb a measurement. `#[tokio::test]` runs on a
//! current-thread runtime, so everything a non-spawning future allocates
//! lands on the test thread.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

struct PeakAlloc;

#[global_allocator]
static GLOBAL: PeakAlloc = PeakAlloc;

thread_local! {
    static LIVE: Cell<isize> = const { Cell::new(0) };
    static PEAK: Cell<isize> = const { Cell::new(0) };
}

fn track(delta: isize) {
    // `try_with`: allocations can happen while a thread's locals are
    // being torn down; skip those rather than panic in the allocator.
    let _ = LIVE.try_with(|live| {
        let now = live.get() + delta;
        live.set(now);
        let _ = PEAK.try_with(|peak| {
            if now > peak.get() {
                peak.set(now);
            }
        });
    });
}

// SAFETY: every call forwards to `System` with the caller's own
// arguments; the bookkeeping only touches const-initialised thread-local
// `Cell`s, which never allocate.
unsafe impl GlobalAlloc for PeakAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc(layout) };
        if !p.is_null() {
            track(layout.size() as isize);
        }
        p
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { System.alloc_zeroed(layout) };
        if !p.is_null() {
            track(layout.size() as isize);
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        track(-(layout.size() as isize));
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = unsafe { System.realloc(ptr, layout, new_size) };
        if !p.is_null() {
            track(new_size as isize - layout.size() as isize);
        }
        p
    }
}

/// Run `f` and return its result with the most heap bytes this thread
/// held at once during the call, above what it held when the call began.
pub(crate) fn peak_during<T>(f: impl FnOnce() -> T) -> (T, usize) {
    let start = LIVE.with(Cell::get);
    PEAK.with(|p| p.set(start));
    let out = f();
    let peak = PEAK.with(Cell::get);
    (out, (peak - start).max(0) as usize)
}
