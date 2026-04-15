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
    /// Build a new cache. `max_entries == 0` would be a subtle footgun
    /// (the eviction branch wouldn't trigger because `0 >= 0` is true
    /// but the key-presence guard always falls through on an empty map),
    /// so we clamp up to `1`. Callers that want the cache disabled
    /// should wrap this in an `Option` rather than passing zero.
    pub fn new(max_entries: usize, ttl: Duration) -> Self {
        let max_entries = max_entries.max(1);
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

#[cfg(test)]
mod tests {
    use super::BoundedTtlCache;
    use std::time::Duration;
    use tokio::time::sleep;

    #[tokio::test]
    async fn expired_entries_return_none() {
        let cache = BoundedTtlCache::new(2, Duration::from_millis(5));
        cache.insert("key", "value").await;
        sleep(Duration::from_millis(12)).await;
        assert_eq!(cache.get(&"key").await, None);
    }

    #[tokio::test]
    async fn inserting_past_max_entries_evicts_oldest() {
        let cache = BoundedTtlCache::new(2, Duration::from_secs(60));
        cache.insert("a", 1).await;
        sleep(Duration::from_millis(2)).await;
        cache.insert("b", 2).await;
        sleep(Duration::from_millis(2)).await;
        cache.insert("c", 3).await;
        assert_eq!(cache.get(&"a").await, None);
        assert_eq!(cache.get(&"b").await, Some(2));
        assert_eq!(cache.get(&"c").await, Some(3));
    }

    #[tokio::test]
    async fn overwriting_existing_key_does_not_grow_map() {
        let cache = BoundedTtlCache::new(2, Duration::from_secs(60));
        cache.insert("k", 1).await;
        cache.insert("k", 2).await;
        assert_eq!(cache.get(&"k").await, Some(2));
        assert_eq!(cache.inner.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn zero_max_entries_is_clamped_to_one() {
        // The constructor must not leave us with an unbounded cache when
        // a caller passes 0 — we clamp to 1 so at least the eviction
        // branch runs.
        let cache = BoundedTtlCache::new(0, Duration::from_secs(60));
        cache.insert("a", 1).await;
        cache.insert("b", 2).await;
        // Only the most-recent entry survives because the cap is 1.
        assert_eq!(cache.inner.lock().await.len(), 1);
        assert_eq!(cache.get(&"a").await, None);
        assert_eq!(cache.get(&"b").await, Some(2));
    }
}
