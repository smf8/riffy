use std::time::{Duration, Instant};

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

    /// Consume the timer so the `Drop` path cannot also fire.
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
