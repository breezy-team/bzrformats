//! A standalone reader for B+Tree graph index files.
//!
//! [`btree_index`](crate::btree_index) provides stateless parsing helpers;
//! the stateful "open a file and look keys up" reader otherwise only
//! existed in the pyo3 layer over a Python transport. This is the
//! pure-Rust equivalent over a [`Transport`], needed to read the
//! `pack-names` and per-pack `.rix`/`.iix`/`.tix`/`.cix` indices of a 2a
//! repository.
//!
//! The reader loads the whole index into memory and decodes every leaf
//! into a sorted map. That is adequate for the index sizes a single
//! repository produces and keeps lookups simple; a lazy page-at-a-time
//! reader (using [`btree_index::plan_page_reads`](crate::btree_index::plan_page_reads))
//! can replace it later if huge indices become a concern.

use std::collections::BTreeMap;

use crate::btree_index::{
    decompress_page, parse_btree_header, BTreeIndexError, LeafKey, LeafNode, LeafRefLists,
    INTERNAL_FLAG, LEAF_FLAG, PAGE_SIZE,
};
use crate::transport::{Transport, TransportError};

/// Errors from reading a B+Tree graph index file.
#[derive(Debug)]
pub enum IndexError {
    /// The index file could not be parsed.
    Parse(BTreeIndexError),
    /// An underlying transport error.
    Transport(TransportError),
}

impl std::fmt::Display for IndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexError::Parse(e) => write!(f, "index parse error: {e}"),
            IndexError::Transport(e) => write!(f, "transport error: {e}"),
        }
    }
}

impl std::error::Error for IndexError {}

impl From<BTreeIndexError> for IndexError {
    fn from(e: BTreeIndexError) -> Self {
        IndexError::Parse(e)
    }
}

impl From<TransportError> for IndexError {
    fn from(e: TransportError) -> Self {
        IndexError::Transport(e)
    }
}

/// An in-memory B+Tree graph index.
///
/// All entries are decoded up front into a sorted map keyed by the
/// (multi-element) index key, mapping to the raw value bytes and the
/// reference lists (graph parents).
pub struct BTreeGraphIndex {
    key_length: usize,
    node_ref_lists: usize,
    entries: BTreeMap<LeafKey, (Vec<u8>, LeafRefLists)>,
}

impl BTreeGraphIndex {
    /// Open and decode the index `name` via `transport`.
    pub fn open(transport: &dyn Transport, name: &str) -> Result<Self, IndexError> {
        let data = transport.get_bytes(name)?;
        Self::from_bytes(&data)
    }

    /// Decode an index already read into memory.
    pub fn from_bytes(data: &[u8]) -> Result<Self, IndexError> {
        let header = parse_btree_header(data)?;
        let mut entries: BTreeMap<LeafKey, (Vec<u8>, LeafRefLists)> = BTreeMap::new();

        if header.key_count > 0 {
            let total_pages = data.len().div_ceil(PAGE_SIZE);
            for page in 0..total_pages {
                let start = page * PAGE_SIZE;
                let end = ((page + 1) * PAGE_SIZE).min(data.len());
                if start >= end {
                    break;
                }
                // Page 0 carries the plaintext header before the compressed
                // root node; every other page is entirely the compressed node.
                let payload = if page == 0 {
                    // A header that does not fit before the end of the first
                    // page is malformed; without this guard the slice below
                    // would panic on header_end > end.
                    if header.header_end > end {
                        return Err(IndexError::Parse(BTreeIndexError::BadOptions));
                    }
                    &data[header.header_end..end]
                } else {
                    &data[start..end]
                };
                if payload.is_empty() {
                    continue;
                }
                let decompressed = decompress_page(payload)?;
                if decompressed.starts_with(LEAF_FLAG) {
                    let leaf =
                        LeafNode::parse(&decompressed, header.key_length, header.node_ref_lists)?;
                    for (k, (v, r)) in leaf.entries {
                        entries.insert(k, (v, r));
                    }
                } else if decompressed.starts_with(INTERNAL_FLAG) {
                    // Internal nodes only steer navigation; all entries live
                    // in leaves, which we visit directly.
                    continue;
                } else {
                    return Err(IndexError::Parse(BTreeIndexError::BadInternalNode));
                }
            }
        }

        Ok(BTreeGraphIndex {
            key_length: header.key_length,
            node_ref_lists: header.node_ref_lists,
            entries,
        })
    }

    /// Number of keys in the index.
    pub fn key_count(&self) -> usize {
        self.entries.len()
    }

    /// Number of elements in each key tuple.
    pub fn key_length(&self) -> usize {
        self.key_length
    }

    /// Number of reference lists stored per entry.
    pub fn node_ref_lists(&self) -> usize {
        self.node_ref_lists
    }

    /// Look up a key, returning its `(value, refs)` if present.
    pub fn get(&self, key: &[Vec<u8>]) -> Option<&(Vec<u8>, LeafRefLists)> {
        self.entries.get(key)
    }

    /// Iterate all `(key, value, refs)` entries in sorted key order.
    pub fn iter_all_entries(&self) -> impl Iterator<Item = (&LeafKey, &Vec<u8>, &LeafRefLists)> {
        self.entries.iter().map(|(k, (v, r))| (k, v, r))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // A tiny index built with a single root leaf, captured from a real
    // pack-names file. We build one synthetically here via the writer in a
    // higher layer once that exists; for now coverage comes from the
    // repository integration tests which read real `brz`-produced indices.

    #[test]
    fn empty_bytes_is_not_an_index() {
        match BTreeGraphIndex::from_bytes(b"not an index") {
            Err(IndexError::Parse(BTreeIndexError::BadSignature)) => {}
            other => panic!("expected BadSignature, got {other:?}"),
        }
    }

    /// A header whose options span more than one page (e.g. an absurdly long
    /// `row_lengths` line) must be rejected as a parse error, not panic when
    /// the page-0 payload slice is taken.
    #[test]
    fn header_spanning_more_than_one_page_is_a_parse_error() {
        let mut data = Vec::new();
        data.extend_from_slice(crate::btree_index::BTREE_SIGNATURE);
        data.extend_from_slice(b"node_ref_lists=0\n");
        data.extend_from_slice(b"key_elements=1\n");
        data.extend_from_slice(b"len=1\n");
        // A valid but very long row_lengths line (many small comma-separated
        // numbers) pushes header_end past one page (4096 bytes).
        data.extend_from_slice(b"row_lengths=1");
        for _ in 0..3000 {
            data.extend_from_slice(b",1");
        }
        data.push(b'\n');
        // Pad so the total exceeds one page and a page-0 payload is attempted.
        data.resize(data.len() + 4096, 0);

        match BTreeGraphIndex::from_bytes(&data) {
            Err(IndexError::Parse(_)) => {}
            other => panic!("expected a parse error, got {other:?}"),
        }
    }
}
