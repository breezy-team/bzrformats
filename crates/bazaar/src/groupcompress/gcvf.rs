//! Pure-logic core of `GroupCompressVersionedFiles`.
//!
//! This module ports the orchestration that the Python
//! `bzrformats.groupcompress.GroupCompressVersionedFiles` class performs on
//! top of the already-Rust groupcompress block/compressor/manager code. The
//! pyo3 layer (`crates/bazaar-py/src/groupcompress.rs`) wraps a Python index
//! and access object and drives these helpers.

use crate::groupcompress::block::GroupCompressBlock;
use crate::knit::FileRef;

/// A versioned-file key: a tuple of byte segments, the last being the
/// version id. Groupcompress shares the keyspace type with knit.
pub type GcKey = crate::versionedfile::Key;

/// Number of bytes a fetch batch accumulates before it is flushed.
///
/// Mirrors `bzrformats.groupcompress.BATCH_SIZE`.
pub const BATCH_SIZE: u64 = 1 << 16;

/// Default cap on the bytes a `GroupCompressor` indexes for delta matching.
///
/// Mirrors `GroupCompressVersionedFiles._DEFAULT_MAX_BYTES_TO_INDEX`.
pub const DEFAULT_MAX_BYTES_TO_INDEX: usize = 1024 * 1024;

/// Identifies a single groupcompress block within a store, plus the byte
/// range it occupies.
///
/// This is the cache key for the block cache and the unit `_get_blocks`
/// fetches. Mirrors the Python `read_memo = index_memo[0:3]` triple
/// `(index, start, stop)`. `index` is abstracted via [`FileRef`] so the
/// pure crate does not depend on the Python graph-index object.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReadMemo<F: FileRef = String> {
    /// Identifies which backing index/shard the block lives in.
    pub index: F,
    /// Byte offset of the block's start in the backing file.
    pub start: u64,
    /// Byte offset one past the block's end.
    pub stop: u64,
}

impl<F: FileRef> ReadMemo<F> {
    pub fn new(index: F, start: u64, stop: u64) -> Self {
        ReadMemo { index, start, stop }
    }

    /// The on-disk byte length of the block.
    pub fn byte_length(&self) -> u64 {
        self.stop.saturating_sub(self.start)
    }
}

/// Locates a single record: which block it lives in (`read_memo`) and the
/// `[entry_start, entry_end)` slice of the decompressed block that holds it.
///
/// Mirrors the Python `index_memo = (index, start, stop, basis_end,
/// delta_end)` 5-tuple; the trailing pair becomes [`Self::entry_start`] /
/// [`Self::entry_end`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexMemo<F: FileRef = String> {
    /// The block this record lives in.
    pub read_memo: ReadMemo<F>,
    /// Offset of the record inside the decompressed block.
    pub entry_start: u64,
    /// Offset one past the record inside the decompressed block.
    pub entry_end: u64,
}

impl<F: FileRef> IndexMemo<F> {
    pub fn new(read_memo: ReadMemo<F>, entry_start: u64, entry_end: u64) -> Self {
        IndexMemo {
            read_memo,
            entry_start,
            entry_end,
        }
    }
}

/// A fetched groupcompress block paired with the memo it was fetched for.
///
/// `_get_blocks` yields these in the order the read-memos were requested.
pub struct FetchedBlock<F: FileRef = String> {
    pub read_memo: ReadMemo<F>,
    pub block: GroupCompressBlock,
}

/// Given the read-memos a batch wants and which of them are already
/// cached, return the de-duplicated, order-preserving list of memos that
/// still need to be fetched.
///
/// Mirrors the partitioning loop in `GroupCompressVersionedFiles._get_blocks`:
/// a memo that is cached is skipped, and a memo already queued for fetch is
/// not queued twice. The first-seen request order is preserved so the
/// fetched raw records line up with the consume order. The actual cache
/// lookup, raw-record fetch, and block decode stay in the pyo3 layer
/// because the block cache is a Python `LRUSizeCache`.
pub fn memos_to_fetch<F: FileRef>(
    read_memos: &[ReadMemo<F>],
    is_cached: impl Fn(&ReadMemo<F>) -> bool,
) -> Vec<ReadMemo<F>> {
    let mut out: Vec<ReadMemo<F>> = Vec::new();
    let mut seen: std::collections::HashSet<ReadMemo<F>> = std::collections::HashSet::new();
    for memo in read_memos {
        if is_cached(memo) {
            continue;
        }
        if seen.insert(memo.clone()) {
            out.push(memo.clone());
        }
    }
    out
}

/// Which store a record-stream key is served from.
///
/// The Python code carries the `GroupCompressVersionedFiles` object itself
/// (`self`) or a fallback VF object. The pure crate cannot hold Python
/// objects, so a fallback is identified by its index into the ordered
/// fallback list.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// This versioned-files store (the Python `self`).
    Local,
    /// The fallback at this index in the immediate-fallback list.
    Fallback(usize),
}

/// Group an ordered key sequence into `(source, [keys])` runs.
///
/// `source_of` maps each key to its [`Source`]; consecutive keys from the
/// same source are collected into one run. Mirrors the "Now group by
/// source" loops shared by the three Python ordering helpers.
fn group_by_source(
    keys: impl IntoIterator<Item = GcKey>,
    source_of: impl Fn(&GcKey) -> Source,
) -> Vec<(Source, Vec<GcKey>)> {
    let mut runs: Vec<(Source, Vec<GcKey>)> = Vec::new();
    for key in keys {
        let source = source_of(&key);
        match runs.last_mut() {
            Some((s, run)) if *s == source => run.push(key),
            _ => runs.push((source, vec![key])),
        }
    }
    runs
}

/// Order keys topologically (or in groupcompress order) and group by source.
///
/// Mirrors `GroupCompressVersionedFiles._get_ordered_source_keys`. `ordering`
/// is `"topological"` or `"groupcompress"`; any key absent from
/// `key_to_source` is served locally.
pub fn ordered_source_keys(
    ordering: &str,
    parent_map: &[(GcKey, Vec<GcKey>)],
    key_to_source: &std::collections::HashMap<GcKey, Source>,
) -> Vec<(Source, Vec<GcKey>)> {
    let raw: Vec<(Vec<Vec<u8>>, Vec<Vec<Vec<u8>>>)> = parent_map
        .iter()
        .map(|(k, ps)| {
            (
                k.segments().to_vec(),
                ps.iter().map(|p| p.segments().to_vec()).collect(),
            )
        })
        .collect();
    let present: Vec<Vec<Vec<u8>>> = if ordering == "topological" {
        let mut sorter = vcs_graph::tsort::TopoSorter::new(raw.into_iter());
        sorter
            .sorted()
            .expect("groupcompress parent_map should not contain cycles")
    } else {
        crate::groupcompress::sort::sort_gc_optimal(raw)
    };
    let keys = present.into_iter().map(GcKey::fixed);
    group_by_source(keys, |k| {
        key_to_source.get(k).copied().unwrap_or(Source::Local)
    })
}

/// Keep the caller's requested order, grouping by source and dropping keys
/// that are absent from every store.
///
/// Mirrors `GroupCompressVersionedFiles._get_as_requested_source_keys`. A
/// key present in `locations` or `unadded` is local; otherwise its
/// `key_to_source` entry is used; a key in none of them is skipped.
pub fn as_requested_source_keys(
    orig_keys: &[GcKey],
    locations: &std::collections::HashSet<GcKey>,
    unadded: &std::collections::HashSet<GcKey>,
    key_to_source: &std::collections::HashMap<GcKey, Source>,
) -> Vec<(Source, Vec<GcKey>)> {
    let present: Vec<GcKey> = orig_keys
        .iter()
        .filter(|k| {
            locations.contains(*k) || unadded.contains(*k) || key_to_source.contains_key(*k)
        })
        .cloned()
        .collect();
    group_by_source(present, |k| {
        if locations.contains(k) || unadded.contains(k) {
            Source::Local
        } else {
            key_to_source.get(k).copied().unwrap_or(Source::Local)
        }
    })
}

/// Accumulates keys into a fetch batch, tracking the read-memos that batch
/// touches and a running byte estimate.
///
/// Ports the state that `_BatchingBlockFetcher.add_key` maintains. The
/// block cache is a Python `LRUSizeCache`, so the cache lookup is passed in
/// as a predicate; the actual fetch happens later in the pyo3 layer using
/// [`Self::memos_to_get`].
#[derive(Debug, Default)]
pub struct BatchAccumulator<F: FileRef = String> {
    keys: Vec<GcKey>,
    /// Read-memos seen in this batch, in first-seen order.
    batch_memos: Vec<ReadMemo<F>>,
    /// Read-memos in this batch that were not cached and must be fetched.
    memos_to_get: Vec<ReadMemo<F>>,
    total_bytes: u64,
}

impl<F: FileRef> BatchAccumulator<F> {
    pub fn new() -> Self {
        BatchAccumulator {
            keys: Vec::new(),
            batch_memos: Vec::new(),
            memos_to_get: Vec::new(),
            total_bytes: 0,
        }
    }

    /// Add a key to the current batch and return the running byte estimate.
    ///
    /// `read_memo` is the key's block memo (`index_memo[0:3]`). `is_cached`
    /// reports whether that block is already in the Python block cache.
    /// Mirrors `_BatchingBlockFetcher.add_key`: a memo already in the batch
    /// is not re-counted; a new uncached memo is queued for fetch and its
    /// `stop` offset is added to the estimate (Python adds `read_memo[2]`,
    /// the absolute stop, not the byte length — preserved here so the
    /// `BATCH_SIZE` threshold behaves identically).
    pub fn add_key(
        &mut self,
        key: GcKey,
        read_memo: ReadMemo<F>,
        is_cached: impl Fn(&ReadMemo<F>) -> bool,
    ) -> u64 {
        self.keys.push(key);
        if self.batch_memos.contains(&read_memo) {
            return self.total_bytes;
        }
        if !is_cached(&read_memo) {
            self.total_bytes += read_memo.stop;
            self.memos_to_get.push(read_memo.clone());
        }
        self.batch_memos.push(read_memo);
        self.total_bytes
    }

    /// Keys added to this batch, in insertion order.
    pub fn keys(&self) -> &[GcKey] {
        &self.keys
    }

    /// Uncached read-memos this batch must fetch, in first-seen order.
    pub fn memos_to_get(&self) -> &[ReadMemo<F>] {
        &self.memos_to_get
    }

    /// Running byte estimate for the batch.
    pub fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// Clear all batch state, ready for the next batch.
    pub fn reset(&mut self) {
        self.keys.clear();
        self.batch_memos.clear();
        self.memos_to_get.clear();
        self.total_bytes = 0;
    }
}

/// Order keys for I/O efficiency: in-memory (unadded) keys first, then
/// located keys grouped by the block they live in, then fallback runs.
///
/// Mirrors `GroupCompressVersionedFiles._get_io_ordered_source_keys`.
/// `located_keys` is the located keys in the caller's order; each must have
/// an entry in `locations`. They are stably sorted by their block index so
/// keys in one group stay together while keeping their relative order, as
/// Python's `sorted(locations, key=get_group)` does over an insertion-
/// ordered dict. `fallback_runs` is the already-grouped `(source, keys)`
/// list for keys served by fallbacks.
pub fn io_ordered_source_keys<F: FileRef>(
    located_keys: &[GcKey],
    locations: &std::collections::HashMap<GcKey, IndexMemo<F>>,
    unadded: &[GcKey],
    fallback_runs: Vec<(Source, Vec<GcKey>)>,
) -> Vec<(Source, Vec<GcKey>)> {
    let mut local: Vec<GcKey> = unadded.to_vec();
    let mut located: Vec<GcKey> = located_keys.to_vec();
    // Python sorts located keys by the group object alone (index_memo[0]);
    // the sort is stable, so keys within one group keep their relative order.
    located.sort_by(|a, b| {
        locations[a]
            .read_memo
            .index
            .cmp(&locations[b].read_memo.index)
    });
    local.extend(located);
    let mut runs = vec![(Source::Local, local)];
    runs.extend(fallback_runs);
    runs
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_memo_byte_length_is_stop_minus_start() {
        let memo = ReadMemo::new("idx".to_string(), 100, 350);
        assert_eq!(memo.byte_length(), 250);
    }

    #[test]
    fn read_memo_byte_length_saturates_when_stop_below_start() {
        // A corrupt memo with stop < start yields 0 rather than underflowing.
        let memo = ReadMemo::new("idx".to_string(), 500, 100);
        assert_eq!(memo.byte_length(), 0);
    }

    #[test]
    fn read_memos_compare_by_all_three_fields() {
        let base = ReadMemo::new("a".to_string(), 0, 10);
        assert_eq!(base, ReadMemo::new("a".to_string(), 0, 10));
        assert_ne!(base, ReadMemo::new("b".to_string(), 0, 10));
        assert_ne!(base, ReadMemo::new("a".to_string(), 1, 10));
        assert_ne!(base, ReadMemo::new("a".to_string(), 0, 11));
    }

    #[test]
    fn index_memo_carries_read_memo_and_entry_range() {
        let rm = ReadMemo::new("idx".to_string(), 0, 1000);
        let im = IndexMemo::new(rm.clone(), 40, 120);
        assert_eq!(im.read_memo, rm);
        assert_eq!(im.entry_start, 40);
        assert_eq!(im.entry_end, 120);
    }

    fn memo(idx: &str, start: u64) -> ReadMemo<String> {
        ReadMemo::new(idx.to_string(), start, start + 10)
    }

    #[test]
    fn memos_to_fetch_skips_cached_and_preserves_order() {
        let req = vec![memo("a", 0), memo("b", 0), memo("c", 0)];
        let cached = [memo("b", 0)];
        let out = memos_to_fetch(&req, |m| cached.contains(m));
        assert_eq!(out, vec![memo("a", 0), memo("c", 0)]);
    }

    #[test]
    fn memos_to_fetch_dedups_repeated_memos() {
        // The same block requested twice is fetched once, in first-seen order.
        let req = vec![memo("a", 0), memo("b", 0), memo("a", 0), memo("c", 0)];
        let out = memos_to_fetch(&req, |_| false);
        assert_eq!(out, vec![memo("a", 0), memo("b", 0), memo("c", 0)]);
    }

    #[test]
    fn memos_to_fetch_empty_when_all_cached() {
        let req = vec![memo("a", 0), memo("b", 0)];
        let out = memos_to_fetch(&req, |_| true);
        assert!(out.is_empty());
    }

    fn gckey(id: &[u8]) -> GcKey {
        GcKey::fixed(vec![id.to_vec()])
    }

    #[test]
    fn ordered_source_keys_topological_groups_by_source() {
        // Chain a -> b -> c; b is served by fallback 0, the rest locally.
        let a = gckey(b"a");
        let b = gckey(b"b");
        let c = gckey(b"c");
        let parent_map = vec![
            (a.clone(), vec![]),
            (b.clone(), vec![a.clone()]),
            (c.clone(), vec![b.clone()]),
        ];
        let mut k2s = std::collections::HashMap::new();
        k2s.insert(b.clone(), Source::Fallback(0));
        let runs = ordered_source_keys("topological", &parent_map, &k2s);
        assert_eq!(
            runs,
            vec![
                (Source::Local, vec![a]),
                (Source::Fallback(0), vec![b]),
                (Source::Local, vec![c]),
            ]
        );
    }

    #[test]
    fn as_requested_source_keys_keeps_order_and_drops_absent() {
        let a = gckey(b"a");
        let b = gckey(b"b");
        let absent = gckey(b"absent");
        let f = gckey(b"f");
        let locations: std::collections::HashSet<GcKey> = vec![a.clone()].into_iter().collect();
        let unadded: std::collections::HashSet<GcKey> = vec![b.clone()].into_iter().collect();
        let mut k2s = std::collections::HashMap::new();
        k2s.insert(f.clone(), Source::Fallback(1));
        let runs = as_requested_source_keys(
            &[a.clone(), absent, b.clone(), f.clone()],
            &locations,
            &unadded,
            &k2s,
        );
        // `absent` is dropped; a and b are both local and merge into one run.
        assert_eq!(
            runs,
            vec![(Source::Local, vec![a, b]), (Source::Fallback(1), vec![f])]
        );
    }

    #[test]
    fn io_ordered_source_keys_unadded_first_then_grouped_then_fallbacks() {
        let u = gckey(b"u");
        let x = gckey(b"x");
        let y = gckey(b"y");
        let f = gckey(b"f");
        let mut locations = std::collections::HashMap::new();
        // x in block "g2", y in block "g1" — sort pulls y ahead of x.
        locations.insert(
            x.clone(),
            IndexMemo::new(ReadMemo::new("g2".to_string(), 0, 10), 0, 5),
        );
        locations.insert(
            y.clone(),
            IndexMemo::new(ReadMemo::new("g1".to_string(), 0, 10), 0, 5),
        );
        let runs = io_ordered_source_keys(
            &[x.clone(), y.clone()],
            &locations,
            &[u.clone()],
            vec![(Source::Fallback(0), vec![f.clone()])],
        );
        assert_eq!(
            runs,
            vec![
                (Source::Local, vec![u, y, x]),
                (Source::Fallback(0), vec![f]),
            ]
        );
    }

    #[test]
    fn batch_accumulator_queues_uncached_memos_and_counts_stop() {
        let mut acc: BatchAccumulator<String> = BatchAccumulator::new();
        // Two keys in distinct uncached blocks.
        let t1 = acc.add_key(gckey(b"k1"), ReadMemo::new("g1".into(), 0, 30), |_| false);
        assert_eq!(t1, 30); // running estimate adds `stop`
        let t2 = acc.add_key(gckey(b"k2"), ReadMemo::new("g2".into(), 0, 50), |_| false);
        assert_eq!(t2, 80);
        assert_eq!(acc.keys(), &[gckey(b"k1"), gckey(b"k2")]);
        assert_eq!(
            acc.memos_to_get(),
            &[
                ReadMemo::new("g1".into(), 0, 30),
                ReadMemo::new("g2".into(), 0, 50),
            ]
        );
    }

    #[test]
    fn batch_accumulator_does_not_recount_repeated_memo() {
        let mut acc: BatchAccumulator<String> = BatchAccumulator::new();
        let block = ReadMemo::new("g1".to_string(), 0, 30);
        acc.add_key(gckey(b"k1"), block.clone(), |_| false);
        // A second key in the same block adds the key but not the bytes.
        let total = acc.add_key(gckey(b"k2"), block.clone(), |_| false);
        assert_eq!(total, 30);
        assert_eq!(acc.keys().len(), 2);
        assert_eq!(acc.memos_to_get(), &[block]);
    }

    #[test]
    fn batch_accumulator_skips_fetch_for_cached_memo() {
        let mut acc: BatchAccumulator<String> = BatchAccumulator::new();
        let block = ReadMemo::new("g1".to_string(), 0, 30);
        // Cached blocks are not queued and not counted.
        let total = acc.add_key(gckey(b"k1"), block, |_| true);
        assert_eq!(total, 0);
        assert!(acc.memos_to_get().is_empty());
        assert_eq!(acc.keys().len(), 1);
    }

    #[test]
    fn batch_accumulator_reset_clears_state() {
        let mut acc: BatchAccumulator<String> = BatchAccumulator::new();
        acc.add_key(gckey(b"k1"), ReadMemo::new("g1".into(), 0, 30), |_| false);
        acc.reset();
        assert!(acc.keys().is_empty());
        assert!(acc.memos_to_get().is_empty());
        assert_eq!(acc.total_bytes(), 0);
    }
}
