//! Opening a pack index that may be in either on-disk index format.
//!
//! Pack repositories store their `pack-names` and per-pack `.rix`/`.iix`/
//! `.tix`/`.six`/`.cix` indices as graph indices. The older pack formats
//! (0.92, 1.6) use the format-1 [`GraphIndex`](crate::index); 1.9 and later
//! (and 2a) use the [`BTreeGraphIndex`](crate::btree_graph_index). Both
//! encode the same logical `(key, value, reference-lists)` entries, so this
//! module sniffs the file signature and exposes a single uniform view.

use crate::btree_graph_index::BTreeGraphIndex;
use crate::index::{self, IndexError};
use crate::transport::Transport;

/// One index entry, normalised across both index formats:
/// `(key, value, reference-lists)`.
pub type Entry = (Vec<Vec<u8>>, Vec<u8>, Vec<Vec<Vec<Vec<u8>>>>);

/// A pack index opened in whichever on-disk format it uses.
pub struct PackIndex {
    entries: Vec<Entry>,
    node_ref_lists: usize,
}

impl PackIndex {
    /// Open the index named `name` (e.g. `"pack-names"` or
    /// `"indices/<pack>.rix"`) under `transport`, detecting the format from
    /// its signature.
    pub fn open(transport: &dyn Transport, name: &str) -> Result<Self, IndexError> {
        let bytes = transport
            .get_bytes(name)
            .map_err(|e| IndexError::Other(format!("reading {name}: {e}")))?;
        Self::from_bytes(&bytes)
    }

    /// Parse index bytes, detecting btree vs format-1 from the signature.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, IndexError> {
        if bytes.starts_with(crate::btree_index::BTREE_SIGNATURE) {
            let btree = BTreeGraphIndex::from_bytes(bytes)
                .map_err(|e| IndexError::Other(format!("btree index: {e:?}")))?;
            let node_ref_lists = btree.node_ref_lists();
            let entries = btree
                .iter_all_entries()
                .map(|(k, v, r)| (k.clone(), v.clone(), r.clone()))
                .collect();
            Ok(PackIndex {
                entries,
                node_ref_lists,
            })
        } else {
            // Format-1 GraphIndex: parse the whole file in one pass.
            // parse_full already drops absent nodes.
            let (header, body) = index::parse_full(bytes)?;
            let entries = body
                .into_iter()
                .map(|(key, (value, references))| (key, value, references))
                .collect();
            Ok(PackIndex {
                entries,
                node_ref_lists: header.node_ref_lists,
            })
        }
    }

    /// Every entry, as `(key, value, reference-lists)`.
    pub fn iter_all_entries(&self) -> impl Iterator<Item = &Entry> {
        self.entries.iter()
    }

    /// The number of reference lists each entry carries (the graph arity).
    pub fn node_ref_lists(&self) -> usize {
        self.node_ref_lists
    }
}

/// A writer for a pack index in either on-disk format. `add_node`/`finish`
/// mirror both [`BTreeBuilder`](crate::btree_builder::BTreeBuilder) and
/// [`GraphIndexBuilder`](crate::index::GraphIndexBuilder); the format is
/// chosen by [`new`](IndexBuilder::new) from the repository format's
/// `uses_btree_index` flag.
pub enum IndexBuilder {
    BTree(crate::btree_builder::BTreeBuilder),
    Graph(crate::index::GraphIndexBuilder),
}

impl IndexBuilder {
    /// A builder of the requested format, with `ref_lists` reference lists
    /// and `key_elements`-element keys.
    pub fn new(uses_btree: bool, ref_lists: usize, key_elements: usize) -> Self {
        if uses_btree {
            IndexBuilder::BTree(crate::btree_builder::BTreeBuilder::new(
                ref_lists,
                key_elements,
            ))
        } else {
            IndexBuilder::Graph(crate::index::GraphIndexBuilder::new(
                ref_lists,
                key_elements,
            ))
        }
    }

    /// Add a `(key, value, reference-lists)` node.
    pub fn add_node(
        &mut self,
        key: Vec<Vec<u8>>,
        value: Vec<u8>,
        references: Vec<Vec<Vec<Vec<u8>>>>,
    ) -> Result<(), IndexError> {
        match self {
            IndexBuilder::BTree(b) => b
                .add_node(key, value, references)
                .map_err(|e| IndexError::Other(format!("btree node: {e:?}"))),
            IndexBuilder::Graph(b) => b.add_node(key, value, references),
        }
    }

    /// Serialise the index to bytes.
    pub fn finish(&self) -> Result<Vec<u8>, IndexError> {
        match self {
            IndexBuilder::BTree(b) => b
                .finish()
                .map_err(|e| IndexError::Other(format!("btree finish: {e:?}"))),
            IndexBuilder::Graph(b) => b.finish(),
        }
    }
}
