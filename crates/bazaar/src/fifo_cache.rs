//! First-in-first-out caches.
//!
//! Mirrors `bzrformats.lru_cache.FIFOCache` / `FIFOSizeCache`: entries queue up
//! in insertion order and eviction takes from the front.
//!
//! Keys are looked up through a caller-supplied [`KeyIndex`] rather than by
//! hashing them here, because the pyo3 wrapper's keys are arbitrary Python
//! objects whose `hash`/`eq` live in the interpreter and may raise. A plain
//! Rust caller can use [`HashIndex`], which hashes keys the usual way.

use std::collections::{HashMap, VecDeque};
use std::hash::Hash;

/// Where an entry sits in the cache.
pub type Slot = usize;

/// Maps keys to slots on the cache's behalf.
///
/// The cache owns the queue, the sizes and the values; this owns only the
/// key-to-slot lookup, so a caller whose keys cannot implement [`Hash`] (such
/// as Python objects) can delegate that to its own machinery.
pub trait KeyIndex {
    /// The key type being indexed.
    type Key;
    /// Whatever the implementation can fail with; `Infallible` for pure Rust.
    type Error;

    fn get(&self, key: &Self::Key) -> Result<Option<Slot>, Self::Error>;
    fn insert(&mut self, key: &Self::Key, slot: Slot) -> Result<(), Self::Error>;
    fn remove(&mut self, key: &Self::Key) -> Result<(), Self::Error>;
}

/// A [`KeyIndex`] for keys that are ordinary hashable Rust values.
#[derive(Debug)]
pub struct HashIndex<K> {
    map: HashMap<K, Slot>,
}

impl<K> Default for HashIndex<K> {
    fn default() -> Self {
        Self {
            map: HashMap::new(),
        }
    }
}

impl<K: Clone + Eq + Hash> KeyIndex for HashIndex<K> {
    type Key = K;
    type Error = std::convert::Infallible;

    fn get(&self, key: &K) -> Result<Option<Slot>, Self::Error> {
        Ok(self.map.get(key).copied())
    }

    fn insert(&mut self, key: &K, slot: Slot) -> Result<(), Self::Error> {
        self.map.insert(key.clone(), slot);
        Ok(())
    }

    fn remove(&mut self, key: &K) -> Result<(), Self::Error> {
        self.map.remove(key);
        Ok(())
    }
}

/// One live entry.
struct Entry<K, V> {
    key: K,
    value: V,
    /// Size contribution, as computed by the caller.
    size: usize,
}

/// A FIFO cache holding its own keys and values.
///
/// Eviction is driven by `total_size`, which is the entry count when every
/// entry is inserted with size 1 (what `FIFOCache` does) or the summed value
/// sizes (`FIFOSizeCache`).
pub struct FifoCache<K, V, I: KeyIndex<Key = K> = HashIndex<K>> {
    entries: HashMap<Slot, Entry<K, V>>,
    /// Live slots in insertion order, front = oldest.
    queue: VecDeque<Slot>,
    index: I,
    next_slot: Slot,
    total_size: usize,
}

impl<K, V> FifoCache<K, V, HashIndex<K>>
where
    K: Clone + Eq + Hash,
{
    /// Create a cache that hashes its own keys.
    pub fn new() -> Self {
        Self::with_index(HashIndex::default())
    }
}

impl<K, V> Default for FifoCache<K, V, HashIndex<K>>
where
    K: Clone + Eq + Hash,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V, I: KeyIndex<Key = K>> FifoCache<K, V, I> {
    /// Create a cache that looks keys up through `index`.
    pub fn with_index(index: I) -> Self {
        Self {
            entries: HashMap::new(),
            queue: VecDeque::new(),
            index,
            next_slot: 0,
            total_size: 0,
        }
    }

    /// Number of live entries.
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// Sum of the sizes the entries were inserted with.
    pub fn total_size(&self) -> usize {
        self.total_size
    }

    /// The borrowed index, for callers that keep state in it.
    pub fn index(&self) -> &I {
        &self.index
    }

    pub fn contains(&self, key: &K) -> Result<bool, I::Error> {
        Ok(self.index.get(key)?.is_some())
    }

    pub fn get(&self, key: &K) -> Result<Option<&V>, I::Error> {
        Ok(self
            .index
            .get(key)?
            .and_then(|slot| self.entries.get(&slot))
            .map(|e| &e.value))
    }

    /// Keys in insertion order, oldest first.
    pub fn keys_oldest_first(&self) -> impl Iterator<Item = &K> {
        self.queue
            .iter()
            .filter_map(move |slot| self.entries.get(slot).map(|e| &e.key))
    }

    /// Insert an entry weighing `size`, replacing and re-queueing any entry
    /// already under `key`. Returns the displaced entry, if there was one.
    pub fn insert(&mut self, key: K, value: V, size: usize) -> Result<Option<(K, V)>, I::Error> {
        let displaced = self.remove(&key)?;
        let slot = self.next_slot;
        self.next_slot += 1;
        self.index.insert(&key, slot)?;
        self.entries.insert(slot, Entry { key, value, size });
        self.queue.push_back(slot);
        self.total_size += size;
        Ok(displaced)
    }

    /// Drop `key` wherever it sits, returning its entry.
    pub fn remove(&mut self, key: &K) -> Result<Option<(K, V)>, I::Error> {
        let slot = match self.index.get(key)? {
            Some(slot) => slot,
            None => return Ok(None),
        };
        self.index.remove(key)?;
        if let Some(pos) = self.queue.iter().position(|&s| s == slot) {
            self.queue.remove(pos);
        }
        Ok(self.entries.remove(&slot).map(|e| {
            self.total_size -= e.size.min(self.total_size);
            (e.key, e.value)
        }))
    }

    /// Evict oldest entries until `total_size <= after_cleanup`, returning
    /// them in eviction order.
    pub fn evict_until(&mut self, after_cleanup: usize) -> Result<Vec<(K, V)>, I::Error> {
        let mut evicted = Vec::new();
        while self.total_size > after_cleanup {
            match self.pop_oldest()? {
                Some(entry) => evicted.push(entry),
                None => break,
            }
        }
        Ok(evicted)
    }

    /// Remove every entry, oldest first.
    pub fn drain_oldest(&mut self) -> Result<Vec<(K, V)>, I::Error> {
        let mut out = Vec::new();
        while let Some(entry) = self.pop_oldest()? {
            out.push(entry);
        }
        Ok(out)
    }

    /// Remove and return the oldest entry.
    pub fn pop_oldest(&mut self) -> Result<Option<(K, V)>, I::Error> {
        let slot = match self.queue.pop_front() {
            Some(slot) => slot,
            None => return Ok(None),
        };
        let entry = match self.entries.remove(&slot) {
            Some(entry) => entry,
            None => return Ok(None),
        };
        self.total_size -= entry.size.min(self.total_size);
        self.index.remove(&entry.key)?;
        Ok(Some((entry.key, entry.value)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn counted(n: u32) -> FifoCache<u32, u32> {
        let mut c = FifoCache::new();
        for i in 0..n {
            c.insert(i, i, 1).unwrap();
        }
        c
    }

    fn keys(c: &FifoCache<u32, u32>) -> Vec<u32> {
        c.keys_oldest_first().copied().collect()
    }

    #[test]
    fn insertion_order_is_preserved() {
        let c = counted(3);
        assert_eq!(vec![0, 1, 2], keys(&c));
        assert_eq!(3, c.total_size());
        assert_eq!(Some(&1), c.get(&1).unwrap());
    }

    #[test]
    fn reinserting_moves_to_the_back() {
        let mut c = counted(3);
        assert_eq!(Some((0, 0)), c.insert(0, 9, 1).unwrap());
        assert_eq!(vec![1, 2, 0], keys(&c));
        assert_eq!(3, c.len());
        assert_eq!(Some(&9), c.get(&0).unwrap());
    }

    #[test]
    fn remove_takes_from_the_middle() {
        let mut c = counted(3);
        assert_eq!(Some((1, 1)), c.remove(&1).unwrap());
        assert_eq!(vec![0, 2], keys(&c));
        assert_eq!(2, c.total_size());
        assert_eq!(None, c.remove(&1).unwrap());
    }

    #[test]
    fn evict_until_drops_oldest_first() {
        let mut c = counted(5);
        let evicted: Vec<u32> = c
            .evict_until(2)
            .unwrap()
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(vec![0, 1, 2], evicted);
        assert_eq!(vec![3, 4], keys(&c));
    }

    #[test]
    fn evict_until_accounts_for_sizes() {
        let mut c = FifoCache::new();
        c.insert(1, "a", 2).unwrap();
        c.insert(2, "b", 3).unwrap();
        c.insert(3, "c", 4).unwrap();
        assert_eq!(9, c.total_size());
        // Stops as soon as the total fits, rather than clearing everything.
        let evicted: Vec<u32> = c
            .evict_until(5)
            .unwrap()
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(vec![1, 2], evicted);
        assert_eq!(4, c.total_size());
    }

    #[test]
    fn evict_until_stops_on_empty() {
        let mut c = counted(2);
        assert_eq!(2, c.evict_until(0).unwrap().len());
        assert!(c.is_empty());
        assert_eq!(0, c.total_size());
    }

    #[test]
    fn drain_returns_oldest_first() {
        let mut c = counted(3);
        let drained: Vec<u32> = c
            .drain_oldest()
            .unwrap()
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        assert_eq!(vec![0, 1, 2], drained);
        assert!(c.is_empty());
        assert_eq!(0, c.total_size());
    }
}
