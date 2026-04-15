//! Bounded + TTL cache used by the provider-resolver handlers.
//!
//! Two safeguards over a plain `HashMap`:
//!
//! 1. **TTL on read** — entries older than `ttl` are ignored.
//! 2. **Size cap on insert** — if the map is at `max_entries`, evict the
//!    oldest entry before inserting.
//!
//! Replaces the unbounded module-level `LazyLock<Mutex<HashMap>>` caches
//! (filemoon, sktorrent) that had grown-forever semantics before #443.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

type Entry<V> = (V, Instant);

/// Bounded TTL cache. Cheap to `clone` — the inner map is behind an `Arc`.
pub struct BoundedTtlCache<K, V> {
    inner: Arc<Mutex<HashMap<K, Entry<V>>>>,
    ttl: Duration,
    max_entries: usize,
}

impl<K, V> Clone for BoundedTtlCache<K, V> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            ttl: self.ttl,
            max_entries: self.max_entries,
        }
    }
}

impl<K, V> BoundedTtlCache<K, V>
where
    K: std::hash::Hash + Eq + Clone,
    V: Clone,
{
    pub fn new(max_entries: usize, ttl: Duration) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::with_capacity(max_entries.min(512)))),
            ttl,
            max_entries,
        }
    }

    /// Fetch a still-fresh entry, or `None` on miss / expiry.
    pub async fn get(&self, key: &K) -> Option<V> {
        let guard = self.inner.lock().await;
        guard
            .get(key)
            .filter(|(_, at)| at.elapsed() < self.ttl)
            .map(|(v, _)| v.clone())
    }

    /// Insert a fresh value. Evicts the oldest entry first when at
    /// capacity so memory usage is bounded by `max_entries`.
    pub async fn insert(&self, key: K, value: V) {
        let mut guard = self.inner.lock().await;
        if guard.len() >= self.max_entries
            && !guard.contains_key(&key)
            && let Some(oldest_key) = guard
                .iter()
                .min_by_key(|(_, (_, at))| *at)
                .map(|(k, _)| k.clone())
        {
            guard.remove(&oldest_key);
        }
        guard.insert(key, (value, Instant::now()));
    }
}
