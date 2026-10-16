//! Keys known to be over their limit, so requests for them skip the backend until the entry
//! expires (`LOCAL_CACHE_SIZE_IN_BYTES`; upstream uses a `freecache.Cache`).

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;

/// freecache's entry header, stored with every key.
const ENTRY_HEADER_BYTES: usize = 24;
/// freecache's smallest cache.
const MIN_BYTES: usize = 512 * 1024;
/// freecache splits its memory into this many segments.
const SEGMENTS: usize = 256;
/// freecache refuses longer keys.
const MAX_KEY_BYTES: usize = 65535;

pub struct LocalCache {
    entries: Mutex<Entries>,
    capacity_bytes: usize,
    /// Longest key freecache accepts: a quarter of a segment, minus the entry header.
    max_key_bytes: usize,
}

#[derive(Default)]
struct Entries {
    /// Key → Unix second at which the entry expires.
    expiry: HashMap<String, i64>,
    /// Insertion order, for evicting the oldest entries when the cache is full.
    order: VecDeque<String>,
    bytes: usize,
}

impl LocalCache {
    pub fn new(capacity_bytes: usize) -> Self {
        let capacity_bytes = capacity_bytes.max(MIN_BYTES);
        let max_key_bytes = (capacity_bytes / SEGMENTS / 4 - ENTRY_HEADER_BYTES).min(MAX_KEY_BYTES);
        Self {
            entries: Mutex::default(),
            capacity_bytes,
            max_key_bytes,
        }
    }

    /// Whether `key` is cached and not expired at `now` (Unix seconds).
    pub fn contains(&self, key: &str, now: i64) -> bool {
        let entries = self.entries.lock().unwrap();
        entries
            .expiry
            .get(key)
            .is_some_and(|expires| now < *expires)
    }

    /// Caches `key` for `ttl_seconds` from `now`. Keys too long for freecache are not cached,
    /// as upstream (which ignores the error).
    pub fn insert(&self, key: &str, ttl_seconds: i64, now: i64) {
        if key.len() > self.max_key_bytes {
            return;
        }
        let mut entries = self.entries.lock().unwrap();
        if entries
            .expiry
            .insert(key.to_string(), now + ttl_seconds)
            .is_none()
        {
            entries.order.push_back(key.to_string());
            entries.bytes += key.len() + ENTRY_HEADER_BYTES;
        }
        while entries.bytes > self.capacity_bytes {
            let Some(oldest) = entries.order.pop_front() else {
                break;
            };
            entries.expiry.remove(&oldest);
            entries.bytes -= oldest.len() + ENTRY_HEADER_BYTES;
        }
    }
}
