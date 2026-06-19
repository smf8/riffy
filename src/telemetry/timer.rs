//! A drop-guard timer shared by the per-module metrics. It guarantees exactly
//! one timing observation: callers record the terminal outcome via `finish`,
//! and if the guard is dropped first — which is how async cancellation works,
//! since the in-flight future is dropped and code after an `.await` never runs
//! — it records the elapsed time with the [`CANCELLED`] outcome instead.
//!
//! Each module owns its own metric definition (name + labels) and supplies a
//! `record` closure mapping `(outcome, elapsed)` onto it; this primitive only
//! handles the timing and the exactly-once / cancelled-on-drop bookkeeping.

use std::time::{Duration, Instant};

/// Outcome recorded when a timer is dropped before `finish`: the awaiting
/// future was cancelled (client disconnect, server shutdown, or panic unwind).
pub const CANCELLED: &str = "cancelled";

pub struct GuardedTimer<F: Fn(&str, Duration)> {
    record: F,
    started: Instant,
    finished: bool,
}

impl<F: Fn(&str, Duration)> GuardedTimer<F> {
    pub fn start(record: F) -> Self {
        Self {
            record,
            started: Instant::now(),
            finished: false,
        }
    }

    /// Record the elapsed time with the terminal `outcome`, consuming the timer
    /// so the `Drop` path cannot also fire.
    pub fn finish(mut self, outcome: &str) {
        self.finished = true;
        (self.record)(outcome, self.started.elapsed());
    }
}

impl<F: Fn(&str, Duration)> Drop for GuardedTimer<F> {
    fn drop(&mut self) {
        if !self.finished {
            (self.record)(CANCELLED, self.started.elapsed());
        }
    }
}
