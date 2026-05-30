//! Least-recently-used cache ordering engine.
//!
//! Mirrors the LRU bookkeeping of `bzrformats.lru_cache.LRUCache` /
//! `LRUSizeCache`: a doubly-linked list threads the entries from most- to
//! least-recently-used, and eviction walks from the LRU end. This core is
//! deliberately Python-agnostic — entries are identified by an opaque
//! [`NodeId`] and carry only an integer size, so the pyo3 wrapper can hold
//! the actual Python keys/values and compute sizes via a Python callable
//! while this module owns the ordering and eviction policy.

use std::collections::HashMap;

/// Opaque handle for a cache entry. The caller assigns ids (typically a
/// monotonically increasing counter) and maps them to its own keys/values.
pub type NodeId = u64;

struct Node {
    prev: Option<NodeId>,
    next: Option<NodeId>,
    /// Size contribution of this entry, as computed by the caller.
    size: usize,
}

/// LRU ordering engine with size-based eviction.
///
/// The `LRUCache` count-based variant in Python is the special case where
/// every entry has size 1 and `max_size`/`after_cleanup_size` are the entry
/// counts; the pyo3 layer uses this type for both.
#[derive(Default)]
pub struct LruOrder {
    nodes: HashMap<NodeId, Node>,
    /// Head of the list — the most recently used entry.
    most_recently_used: Option<NodeId>,
    /// Tail of the list — the least recently used entry.
    least_recently_used: Option<NodeId>,
    /// Sum of all entry sizes currently held.
    total_size: usize,
}

impl LruOrder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of entries currently tracked.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Total size of all entries (sum of per-entry sizes).
    pub fn total_size(&self) -> usize {
        self.total_size
    }

    pub fn contains(&self, id: NodeId) -> bool {
        self.nodes.contains_key(&id)
    }

    /// The current least-recently-used entry, if any.
    pub fn lru(&self) -> Option<NodeId> {
        self.least_recently_used
    }

    /// Insert a brand-new entry at the most-recently-used position.
    ///
    /// The caller must ensure `id` is not already present (use
    /// [`LruOrder::touch`] / [`LruOrder::update_size`] for existing ids).
    pub fn insert(&mut self, id: NodeId, size: usize) {
        debug_assert!(!self.nodes.contains_key(&id));
        self.nodes.insert(
            id,
            Node {
                prev: None,
                next: None,
                size,
            },
        );
        self.total_size += size;
        self.move_to_front(id);
    }

    /// Update the recorded size of an existing entry, adjusting the total.
    pub fn update_size(&mut self, id: NodeId, size: usize) {
        if let Some(node) = self.nodes.get_mut(&id) {
            self.total_size -= node.size;
            node.size = size;
            self.total_size += size;
        }
    }

    /// Mark an existing entry as most-recently-used. No-op for unknown ids.
    pub fn touch(&mut self, id: NodeId) {
        if self.nodes.contains_key(&id) {
            self.move_to_front(id);
        }
    }

    /// Remove an entry, returning its recorded size. No-op (returns `None`)
    /// for unknown ids.
    pub fn remove(&mut self, id: NodeId) -> Option<usize> {
        let node = self.nodes.remove(&id)?;
        let (prev, next, size) = (node.prev, node.next, node.size);
        match prev {
            Some(p) => {
                if let Some(pn) = self.nodes.get_mut(&p) {
                    pn.next = next;
                }
            }
            None => self.most_recently_used = next,
        }
        match next {
            Some(n) => {
                if let Some(nn) = self.nodes.get_mut(&n) {
                    nn.prev = prev;
                }
            }
            None => self.least_recently_used = prev,
        }
        self.total_size -= size;
        Some(size)
    }

    /// Evict least-recently-used entries until `total_size <=
    /// after_cleanup`, returning the evicted ids in eviction (LRU-first)
    /// order. Mirrors `LRUCache.cleanup` / `LRUSizeCache.cleanup`.
    pub fn evict_until(&mut self, after_cleanup: usize) -> Vec<NodeId> {
        let mut evicted = Vec::new();
        while self.total_size > after_cleanup {
            match self.least_recently_used {
                Some(id) => {
                    self.remove(id);
                    evicted.push(id);
                }
                None => break,
            }
        }
        evicted
    }

    /// Remove every entry, returning the ids in LRU-first order (the order
    /// `LRUCache.clear` removes them in).
    pub fn drain_lru(&mut self) -> Vec<NodeId> {
        let mut out = Vec::new();
        while let Some(id) = self.least_recently_used {
            self.remove(id);
            out.push(id);
        }
        out
    }

    /// Unlink `id` from its current position and splice it in at the head.
    fn move_to_front(&mut self, id: NodeId) {
        if self.most_recently_used == Some(id) {
            return;
        }
        // Unlink from current position (if it is currently linked).
        let (prev, next) = {
            let node = &self.nodes[&id];
            (node.prev, node.next)
        };
        if let Some(p) = prev {
            if let Some(pn) = self.nodes.get_mut(&p) {
                pn.next = next;
            }
        }
        if let Some(n) = next {
            if let Some(nn) = self.nodes.get_mut(&n) {
                nn.prev = prev;
            }
        }
        if self.least_recently_used == Some(id) {
            self.least_recently_used = prev;
        }
        // Splice in at the head.
        let old_head = self.most_recently_used;
        {
            let node = self.nodes.get_mut(&id).unwrap();
            node.prev = None;
            node.next = old_head;
        }
        if let Some(h) = old_head {
            if let Some(hn) = self.nodes.get_mut(&h) {
                hn.prev = Some(id);
            }
        }
        self.most_recently_used = Some(id);
        if self.least_recently_used.is_none() {
            self.least_recently_used = Some(id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_order() {
        let mut o = LruOrder::new();
        o.insert(1, 1);
        o.insert(2, 1);
        o.insert(3, 1);
        assert_eq!(o.len(), 3);
        // LRU is the first inserted.
        assert_eq!(o.lru(), Some(1));
        // Touch 1 -> it is no longer LRU.
        o.touch(1);
        assert_eq!(o.lru(), Some(2));
    }

    #[test]
    fn evict_until_size() {
        let mut o = LruOrder::new();
        o.insert(1, 5);
        o.insert(2, 6);
        o.insert(3, 7);
        assert_eq!(o.total_size(), 18);
        o.touch(2); // make 2 newer than 1 and 3? no: order is 3(mru),2,1(lru) after touch
                    // order now: 2, 3, 1 (lru)
        let evicted = o.evict_until(10);
        // remove LRU-first until <= 10: remove 1 (size5 ->13), remove 3 (->? )
        assert!(o.total_size() <= 10);
        assert!(!evicted.is_empty());
    }

    #[test]
    fn remove_adjusts_size_and_links() {
        let mut o = LruOrder::new();
        o.insert(10, 13);
        assert_eq!(o.total_size(), 13);
        assert_eq!(o.remove(10), Some(13));
        assert_eq!(o.total_size(), 0);
        assert!(o.is_empty());
        assert_eq!(o.lru(), None);
    }

    #[test]
    fn update_size_tracks_total() {
        let mut o = LruOrder::new();
        o.insert(1, 3);
        o.update_size(1, 8);
        assert_eq!(o.total_size(), 8);
    }

    #[test]
    fn drain_lru_order() {
        let mut o = LruOrder::new();
        o.insert(1, 1);
        o.insert(2, 1);
        o.insert(3, 1);
        // LRU-first: 1, 2, 3
        assert_eq!(o.drain_lru(), vec![1, 2, 3]);
        assert!(o.is_empty());
    }
}
