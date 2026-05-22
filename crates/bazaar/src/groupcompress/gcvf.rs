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
}
