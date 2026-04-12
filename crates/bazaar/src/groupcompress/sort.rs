//! Group-compress-optimal key ordering.

use std::collections::BTreeMap;
use vcs_graph::tsort::TopoSorter;

/// A key in a parent map — a tuple of byte segments, e.g. `(file_id, revision_id)`.
pub type Key = Vec<Vec<u8>>;

/// Sort and group the keys in `parent_map` into groupcompress order.
///
/// Groupcompress order is reverse-topological, grouped by the key prefix
/// (the first segment of a multi-element key, or an empty prefix for
/// single-element keys).
pub fn sort_gc_optimal(parent_map: Vec<(Key, Vec<Key>)>) -> Vec<Key> {
    // Group by prefix.
    let mut per_prefix: BTreeMap<Vec<u8>, Vec<(Key, Vec<Key>)>> = BTreeMap::new();
    for (key, parents) in parent_map {
        let prefix = if key.len() <= 1 {
            Vec::new()
        } else {
            key[0].clone()
        };
        per_prefix.entry(prefix).or_default().push((key, parents));
    }

    // Topo-sort each bucket and append in reverse.
    let mut out = Vec::new();
    for (_prefix, bucket) in per_prefix {
        let mut sorter = TopoSorter::new(bucket.into_iter());
        let sorted = sorter
            .sorted()
            .expect("groupcompress parent_map should not contain cycles");
        out.extend(sorted.into_iter().rev());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(parts: &[&[u8]]) -> Key {
        parts.iter().map(|p| p.to_vec()).collect()
    }

    #[test]
    fn empty() {
        assert!(sort_gc_optimal(vec![]).is_empty());
    }

    #[test]
    fn single_prefix_reverse_topo() {
        // A chain: a -> b -> c (a is root, c is leaf)
        let a = key(&[b"f1", b"a"]);
        let b = key(&[b"f1", b"b"]);
        let c = key(&[b"f1", b"c"]);
        let parent_map = vec![
            (a.clone(), vec![]),
            (b.clone(), vec![a.clone()]),
            (c.clone(), vec![b.clone()]),
        ];
        let out = sort_gc_optimal(parent_map);
        // Topological is [a, b, c]; reversed is [c, b, a].
        assert_eq!(out, vec![c, b, a]);
    }

    #[test]
    fn multi_prefix_grouped_by_first_segment() {
        let f1a = key(&[b"f1", b"a"]);
        let f2a = key(&[b"f2", b"a"]);
        let parent_map = vec![(f2a.clone(), vec![]), (f1a.clone(), vec![])];
        let out = sort_gc_optimal(parent_map);
        // Prefixes are sorted (f1 before f2).
        assert_eq!(out, vec![f1a, f2a]);
    }

    #[test]
    fn single_element_keys_share_empty_prefix() {
        let a = key(&[b"a"]);
        let b = key(&[b"b"]);
        let parent_map = vec![(a.clone(), vec![]), (b.clone(), vec![a.clone()])];
        let out = sort_gc_optimal(parent_map);
        // Topological [a, b] reversed => [b, a].
        assert_eq!(out, vec![b, a]);
    }
}
