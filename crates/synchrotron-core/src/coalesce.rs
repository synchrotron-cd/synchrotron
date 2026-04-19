//! Per-key throttling/coalescing of rapid-fire events.
//!
//! The classic motivating case: a forge retries a webhook delivery,
//! or a CI system pushes 50 tags back-to-back, and we don't want to
//! kick off 50 identical fetches. [`Coalescer`] admits the first
//! event for a given key, then suppresses subsequent events for the
//! same key until `window` has elapsed since the admitted event.
//!
//! Leading-edge semantics (first event passes, subsequent events
//! within window are dropped) rather than debounce (delay until quiet
//! period) because we want the *earliest* possible trigger — waiting
//! 2s into a push storm is wasted latency on a change we already
//! have the signal for.
//!
//! Not a broadcast/bus itself — callers use this as a filter sitting
//! in front of whatever downstream they care about (RepoTriggers,
//! EventBus, etc.).

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub struct Coalescer<K: Eq + Hash + Clone> {
    window: Duration,
    last: Mutex<HashMap<K, Instant>>,
}

impl<K: Eq + Hash + Clone> Coalescer<K> {
    pub fn new(window: Duration) -> Self {
        Self {
            window,
            last: Mutex::new(HashMap::new()),
        }
    }

    /// Returns `true` if the caller should forward this event, or
    /// `false` if it's been suppressed (a prior event for the same
    /// key was admitted within the current window).
    pub fn admit(&self, key: K) -> bool {
        self.admit_at(key, Instant::now())
    }

    /// Test-only variant with an injected `now`. Production code
    /// should call [`Self::admit`].
    pub fn admit_at(&self, key: K, now: Instant) -> bool {
        let mut map = self.last.lock().expect("coalescer mutex poisoned");
        match map.get(&key) {
            Some(prev) if now.duration_since(*prev) < self.window => false,
            _ => {
                map.insert(key, now);
                true
            }
        }
    }

    /// Forget all keys whose window has elapsed. Optional — the map
    /// only grows to `O(distinct active keys)` anyway, but long-
    /// running processes with churning keys can call this to reclaim
    /// memory.
    pub fn sweep(&self, now: Instant) -> usize {
        let mut map = self.last.lock().expect("coalescer mutex poisoned");
        let before = map.len();
        map.retain(|_, t| now.duration_since(*t) < self.window);
        before - map.len()
    }

    pub fn tracked_keys(&self) -> usize {
        self.last.lock().expect("coalescer mutex poisoned").len()
    }
}
