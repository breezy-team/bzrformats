//! B+Tree graph index format helpers.
//!
//! Pure-Rust port of `bzrformats/btree_index.py`. The reader
//! ([`BTreeGraphIndex`]) is generic over the [`crate::index::IndexTransport`]
//! trait so a pure-Rust caller can drive it without ever touching Python.
//! The Python side wraps the same type via PyO3, and the in-process
//! `BTreeBuilder` orchestration (with tempfile spill semantics) stays in
//! Python.

use crate::index::{IndexError, IndexTransport};
use std::collections::{BTreeMap, HashSet};

/// Magic signature written at the start of every B+Tree graph index.
pub const BTREE_SIGNATURE: &[u8] = b"B+Tree Graph Index 2\n";
pub const LEAF_FLAG: &[u8] = b"type=leaf\n";
pub const INTERNAL_FLAG: &[u8] = b"type=internal\n";
pub const OPTION_NODE_REFS: &[u8] = b"node_ref_lists=";
pub const OPTION_KEY_ELEMENTS: &[u8] = b"key_elements=";
pub const OPTION_LEN: &[u8] = b"len=";
pub const OPTION_ROW_LENGTHS: &[u8] = b"row_lengths=";

/// Page size used by the on-disk B+Tree format. Every node (except the
/// header-bearing root page) is exactly this many bytes after zlib
/// compression.
pub const PAGE_SIZE: usize = 4096;
/// Bytes reserved at the start of the file for the header.
pub const RESERVED_HEADER_BYTES: usize = 120;
/// Default LRU cache capacity for leaf nodes.
pub const NODE_CACHE_SIZE: usize = 1000;

/// Errors from parsing a B+Tree index header or internal node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BTreeIndexError {
    /// The file didn't start with the magic `B+Tree Graph Index 2\n` line.
    BadSignature,
    /// An option line was missing, in the wrong order, or had a non-decimal
    /// value.
    BadOptions,
    /// An internal node's body was too short — missing the type line, the
    /// offset line, or an integer that couldn't be parsed.
    BadInternalNode,
}

impl std::fmt::Display for BTreeIndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BTreeIndexError::BadSignature => write!(f, "bad btree index format signature"),
            BTreeIndexError::BadOptions => write!(f, "bad btree index options"),
            BTreeIndexError::BadInternalNode => write!(f, "bad btree internal node"),
        }
    }
}

impl std::error::Error for BTreeIndexError {}

/// Parsed B+Tree index header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BTreeHeader {
    pub node_ref_lists: usize,
    pub key_length: usize,
    pub key_count: usize,
    /// Number of nodes in each level of the tree, leaves first.
    pub row_lengths: Vec<usize>,
    /// Byte offset of the first byte after the header.
    pub header_end: usize,
}

/// Parse the B+Tree index file header from the start of `data`. Mirrors
/// `BTreeGraphIndex._parse_header_from_bytes`.
pub fn parse_btree_header(data: &[u8]) -> Result<BTreeHeader, BTreeIndexError> {
    if !data.starts_with(BTREE_SIGNATURE) {
        return Err(BTreeIndexError::BadSignature);
    }
    let after_sig = &data[BTREE_SIGNATURE.len()..];

    let mut option_lines: [&[u8]; 4] = [b"", b"", b"", b""];
    let mut offset = 0usize;
    for slot in option_lines.iter_mut() {
        let nl = after_sig[offset..]
            .iter()
            .position(|&b| b == b'\n')
            .ok_or(BTreeIndexError::BadOptions)?;
        *slot = &after_sig[offset..offset + nl];
        offset += nl + 1;
    }

    let node_ref_lists = parse_usize_option(option_lines[0], OPTION_NODE_REFS)?;
    let key_length = parse_usize_option(option_lines[1], OPTION_KEY_ELEMENTS)?;
    let key_count = parse_usize_option(option_lines[2], OPTION_LEN)?;
    let row_lengths = parse_row_lengths(option_lines[3])?;

    let header_end = BTREE_SIGNATURE.len()
        + option_lines[0].len()
        + option_lines[1].len()
        + option_lines[2].len()
        + option_lines[3].len()
        + 4;

    Ok(BTreeHeader {
        node_ref_lists,
        key_length,
        key_count,
        row_lengths,
        header_end,
    })
}

/// Parsed contents of an internal-node page body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalNode {
    /// The page-index offset at which the child leaves for this node begin.
    pub offset: usize,
    /// Key tuples acting as split points between children.
    pub keys: Vec<Vec<Vec<u8>>>,
}

/// Parse the body bytes of an internal B+Tree node. Mirrors
/// `_InternalNode.__init__`/`_parse_lines`: first line is a type marker,
/// second line is `offset=<int>`, subsequent non-empty lines are key
/// tuples joined by `\x00`, terminated by the first empty line.
pub fn parse_internal_node(body: &[u8]) -> Result<InternalNode, BTreeIndexError> {
    let mut lines = body.split(|&b| b == b'\n');
    let _type_line = lines.next().ok_or(BTreeIndexError::BadInternalNode)?;
    let offset_line = lines.next().ok_or(BTreeIndexError::BadInternalNode)?;
    // Python hardcodes `lines[1][7:]` — the `offset=` prefix is 7 bytes.
    // Preserve that quirk (no explicit prefix check) so we round-trip any
    // input the Python parser would accept, with the same ValueError
    // semantics if the rest isn't a decimal integer.
    if offset_line.len() < 7 {
        return Err(BTreeIndexError::BadInternalNode);
    }
    let offset = std::str::from_utf8(&offset_line[7..])
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .ok_or(BTreeIndexError::BadInternalNode)?;

    let mut keys: Vec<Vec<Vec<u8>>> = Vec::new();
    for line in lines {
        if line.is_empty() {
            break;
        }
        let parts: Vec<Vec<u8>> = line.split(|&b| b == b'\x00').map(|p| p.to_vec()).collect();
        keys.push(parts);
    }

    Ok(InternalNode { offset, keys })
}

/// One key in a B+Tree index — a tuple of byte segments.
pub type LeafKey = Vec<Vec<u8>>;

/// One reference list (a list of keys).
pub type LeafRefList = Vec<LeafKey>;

/// All reference lists for a single leaf node entry.
pub type LeafRefLists = Vec<LeafRefList>;

/// One leaf entry: `(key, value, reference_lists)`.
pub type LeafEntry = (LeafKey, Vec<u8>, LeafRefLists);

/// Parse the body bytes of a B+Tree leaf node into `(key, value, refs)`
/// entries. Mirrors `_btree_serializer._parse_leaf_lines`: the body must
/// start with `type=leaf\n`, each line is `seg\0...\0seg\0refs\0value`,
/// and refs is a tab-separated list of `\r`-separated reference keys
/// (each itself `\0`-joined).
pub fn parse_leaf_lines(
    body: &[u8],
    key_length: usize,
    ref_list_length: usize,
) -> Result<Vec<LeafEntry>, BTreeIndexError> {
    let mut header_found = false;
    let mut out = Vec::new();
    for line in body.split(|&b| b == b'\n') {
        if line.is_empty() {
            continue;
        }
        if !header_found {
            if line == b"type=leaf" {
                header_found = true;
                continue;
            }
            return Err(BTreeIndexError::BadInternalNode);
        }
        out.push(parse_leaf_line(line, key_length, ref_list_length)?);
    }
    if !header_found {
        return Err(BTreeIndexError::BadInternalNode);
    }
    Ok(out)
}

fn parse_leaf_line(
    line: &[u8],
    key_length: usize,
    ref_list_length: usize,
) -> Result<LeafEntry, BTreeIndexError> {
    let mut pos = 0;
    let mut key: LeafKey = Vec::with_capacity(key_length);
    for i in 0..key_length {
        if let Some(nul) = line[pos..].iter().position(|&b| b == 0) {
            key.push(line[pos..pos + nul].to_vec());
            pos += nul + 1;
        } else if i + 1 == key_length {
            // Last segment: capture to end (matches Python).
            key.push(line[pos..].to_vec());
            pos = line.len();
        } else {
            return Err(BTreeIndexError::BadInternalNode);
        }
    }
    let rest = &line[pos..];
    let last_nul = rest
        .iter()
        .rposition(|&b| b == 0)
        .ok_or(BTreeIndexError::BadInternalNode)?;
    let value = rest[last_nul + 1..].to_vec();
    let refs_area = &rest[..last_nul];

    let mut refs: LeafRefLists = Vec::with_capacity(ref_list_length);
    if ref_list_length > 0 {
        let sections: Vec<&[u8]> = refs_area.split(|&b| b == b'\t').collect();
        for section in sections.iter().take(ref_list_length) {
            let mut list: LeafRefList = Vec::new();
            if !section.is_empty() {
                for ref_bytes in section.split(|&b| b == b'\r') {
                    if ref_bytes.is_empty() {
                        continue;
                    }
                    let parts: LeafKey = ref_bytes.split(|&b| b == 0).map(|s| s.to_vec()).collect();
                    list.push(parts);
                }
            }
            refs.push(list);
        }
    } else if !refs_area.is_empty() {
        return Err(BTreeIndexError::BadInternalNode);
    }
    Ok((key, value, refs))
}

/// Decoded leaf-node payload: a sorted map from key to `(value, refs)`,
/// plus min/max bookkeeping used by the lookup path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeafNode {
    /// Map from key to `(value, ref_lists)`. Sorted iteration matches
    /// Python's `_LeafNode.all_items` (which sorts).
    pub entries: BTreeMap<LeafKey, (Vec<u8>, LeafRefLists)>,
    pub min_key: Option<LeafKey>,
    pub max_key: Option<LeafKey>,
}

impl LeafNode {
    pub fn parse(
        body: &[u8],
        key_length: usize,
        ref_list_length: usize,
    ) -> Result<Self, BTreeIndexError> {
        let entries_vec = parse_leaf_lines(body, key_length, ref_list_length)?;
        let (min_key, max_key) = if entries_vec.is_empty() {
            (None, None)
        } else {
            (
                Some(entries_vec[0].0.clone()),
                Some(entries_vec[entries_vec.len() - 1].0.clone()),
            )
        };
        let mut entries: BTreeMap<LeafKey, (Vec<u8>, LeafRefLists)> = BTreeMap::new();
        for (k, v, r) in entries_vec {
            entries.insert(k, (v, r));
        }
        Ok(Self {
            entries,
            min_key,
            max_key,
        })
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn contains_key(&self, key: &LeafKey) -> bool {
        self.entries.contains_key(key)
    }

    pub fn get(&self, key: &LeafKey) -> Option<&(Vec<u8>, LeafRefLists)> {
        self.entries.get(key)
    }

    /// Sorted (key, (value, refs)) iterator — matches `_LeafNode.all_items`.
    pub fn all_items(&self) -> impl Iterator<Item = (&LeafKey, &(Vec<u8>, LeafRefLists))> {
        self.entries.iter()
    }
}

/// One node read from the file: either a leaf or an internal node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeKind {
    Leaf(LeafNode),
    Internal(InternalNode),
}

/// One emitted entry: `(key, value, references)`.
pub type BTreeEntry = (LeafKey, Vec<u8>, LeafRefLists);

fn round_up_div(numerator: u64, denominator: u64) -> u64 {
    numerator.div_ceil(denominator)
}

/// Compute the cumulative `_row_offsets` from `_row_lengths`.
///
/// The result has `row_lengths.len() + 1` elements: each entry is the
/// page index at which the corresponding row starts, with the final
/// entry pointing one past the last leaf — i.e. the total page count.
pub fn compute_row_offsets(row_lengths: &[usize]) -> Vec<usize> {
    let mut out = Vec::with_capacity(row_lengths.len() + 1);
    let mut acc = 0usize;
    for &len in row_lengths {
        out.push(acc);
        acc += len;
    }
    out.push(acc);
    out
}

/// Find the [first, end) page range belonging to the same row as `offset`.
/// Mirrors `_find_layer_first_and_end`.
pub fn find_layer_first_and_end(row_offsets: &[usize], offset: usize) -> (usize, usize) {
    let mut first = 0usize;
    let mut end = 0usize;
    for &roffset in row_offsets {
        first = end;
        end = roffset;
        if offset < roffset {
            break;
        }
    }
    (first, end)
}

/// Pure port of `_multi_bisect_right`: for each key in `in_keys` (sorted),
/// find its bisect-right position in `fixed_keys` (sorted) and return
/// `(position, [keys at that position])` pairs, in input order.
pub fn multi_bisect_right(
    in_keys: &[LeafKey],
    fixed_keys: &[LeafKey],
) -> Vec<(usize, Vec<LeafKey>)> {
    if in_keys.is_empty() {
        return Vec::new();
    }
    if fixed_keys.is_empty() {
        return vec![(0, in_keys.to_vec())];
    }
    if in_keys.len() == 1 {
        let pos = fixed_keys.partition_point(|k| k <= &in_keys[0]);
        return vec![(pos, vec![in_keys[0].clone()])];
    }

    // Two-pointer walk matching the Python reference implementation.
    let mut output: Vec<(usize, Vec<LeafKey>)> = Vec::new();
    let mut in_iter = in_keys.iter();
    let mut fixed_iter = fixed_keys.iter().enumerate();
    let mut cur_in = match in_iter.next() {
        Some(k) => k,
        None => return output,
    };
    let (mut cur_fixed_offset, mut cur_fixed_key) = match fixed_iter.next() {
        Some(p) => p,
        None => {
            let mut tail = vec![cur_in.clone()];
            tail.extend(in_iter.cloned());
            return vec![(0, tail)];
        }
    };

    enum Done {
        Input,
        Fixed,
    }

    let result: Result<(), Done> = (|| -> Result<(), Done> {
        loop {
            if cur_in < cur_fixed_key {
                let mut bucket: Vec<LeafKey> = Vec::new();
                let pos = cur_fixed_offset;
                while cur_in < cur_fixed_key {
                    bucket.push(cur_in.clone());
                    cur_in = match in_iter.next() {
                        Some(k) => k,
                        None => {
                            output.push((pos, bucket));
                            return Err(Done::Input);
                        }
                    };
                }
                output.push((pos, bucket));
                // cur_in now >= cur_fixed_key.
            }
            // Step fixed forward until cur_in < cur_fixed_key, or fixed runs out.
            while cur_in >= cur_fixed_key {
                match fixed_iter.next() {
                    Some((o, k)) => {
                        cur_fixed_offset = o;
                        cur_fixed_key = k;
                    }
                    None => return Err(Done::Fixed),
                }
            }
        }
    })();

    match result {
        Err(Done::Input) => {}
        Err(Done::Fixed) => {
            let mut bucket = vec![cur_in.clone()];
            bucket.extend(in_iter.cloned());
            output.push((fixed_keys.len(), bucket));
        }
        Ok(()) => {}
    }
    output
}

/// Decompress a single page worth of bytes (zlib) and parse it into a
/// node. The first byte must indicate a leaf or internal node.
pub fn decode_node(
    data: &[u8],
    key_length: usize,
    ref_list_length: usize,
) -> Result<NodeKind, BTreeIndexError> {
    use std::io::Read;
    let mut z = flate2::read::ZlibDecoder::new(data);
    let mut decompressed = Vec::with_capacity(PAGE_SIZE);
    z.read_to_end(&mut decompressed)
        .map_err(|_| BTreeIndexError::BadInternalNode)?;
    if decompressed.starts_with(LEAF_FLAG) {
        Ok(NodeKind::Leaf(LeafNode::parse(
            &decompressed,
            key_length,
            ref_list_length,
        )?))
    } else if decompressed.starts_with(INTERNAL_FLAG) {
        Ok(NodeKind::Internal(parse_internal_node(&decompressed)?))
    } else {
        Err(BTreeIndexError::BadInternalNode)
    }
}

/// Stateful B+Tree graph index reader. Generic over an
/// [`crate::index::IndexTransport`] so pure-Rust callers can use it
/// without the Python layer.
///
/// The reader caches:
/// * the parsed root node and header metadata,
/// * an LRU of leaf nodes (default capacity [`NODE_CACHE_SIZE`]),
/// * a small LRU of internal nodes (capacity 100, matching the Python FIFO).
pub struct BTreeGraphIndex<T: IndexTransport> {
    transport: T,
    name: String,
    /// Total size of the backing region in bytes (excluding `base_offset`).
    size: Option<u64>,
    base_offset: u64,

    /// Parsed header data — `node_ref_lists`, `key_length`, `key_count`,
    /// `row_lengths`, `row_offsets` — populated on the first read.
    node_ref_lists: Option<usize>,
    key_length: Option<usize>,
    key_count: Option<usize>,
    row_lengths: Option<Vec<usize>>,
    row_offsets: Option<Vec<usize>>,

    root_node: Option<NodeKind>,
    leaf_cache: lru::LruCache<usize, LeafNode>,
    internal_cache: lru::LruCache<usize, InternalNode>,
    recommended_pages: usize,
}

impl<T: IndexTransport> BTreeGraphIndex<T> {
    /// Open an index. Pass `Some(size)` if you know the file size; this
    /// enables size-aware partial reads.
    pub fn new(transport: T, name: impl Into<String>, size: Option<u64>, base_offset: u64) -> Self {
        let recommended_pages =
            round_up_div(transport.recommended_page_size(), PAGE_SIZE as u64) as usize;
        Self {
            transport,
            name: name.into(),
            size,
            base_offset,
            node_ref_lists: None,
            key_length: None,
            key_count: None,
            row_lengths: None,
            row_offsets: None,
            root_node: None,
            leaf_cache: lru::LruCache::new(std::num::NonZeroUsize::new(NODE_CACHE_SIZE).unwrap()),
            internal_cache: lru::LruCache::new(std::num::NonZeroUsize::new(100).unwrap()),
            recommended_pages,
        }
    }

    /// Like [`new`], but with unbounded caches (matches the Python
    /// `unlimited_cache=True` constructor flag).
    pub fn new_unlimited_cache(
        transport: T,
        name: impl Into<String>,
        size: Option<u64>,
        base_offset: u64,
    ) -> Self {
        let mut s = Self::new(transport, name, size, base_offset);
        s.leaf_cache = lru::LruCache::unbounded();
        s.internal_cache = lru::LruCache::unbounded();
        s
    }

    /// Path on the transport.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Total size in bytes (after `base_offset`), if known.
    pub fn size(&self) -> Option<u64> {
        self.size
    }

    pub fn node_ref_lists(&self) -> Option<usize> {
        self.node_ref_lists
    }

    pub fn key_length(&self) -> Option<usize> {
        self.key_length
    }

    pub fn row_lengths(&self) -> Option<&[usize]> {
        self.row_lengths.as_deref()
    }

    pub fn row_offsets(&self) -> Option<&[usize]> {
        self.row_offsets.as_deref()
    }

    /// Drop the leaf-node cache. Mirrors `clear_cache`. The root and
    /// internal-node cache are intentionally retained.
    pub fn clear_cache(&mut self) {
        self.leaf_cache.clear();
    }

    /// Number of keys in the index. May trigger a transport read to
    /// load the header on first call.
    pub fn key_count(&mut self) -> Result<usize, IndexError> {
        if self.key_count.is_none() {
            self.get_root_node()?;
        }
        Ok(self.key_count.expect("populated by get_root_node"))
    }

    /// Read every leaf node in order and yield `(key, value, refs)`.
    /// Mirrors `iter_all_entries`.
    pub fn iter_all_entries(&mut self) -> Result<Vec<BTreeEntry>, IndexError> {
        if self.key_count()? == 0 {
            return Ok(Vec::new());
        }
        let row_offsets = self.row_offsets.as_ref().expect("row_offsets populated");
        let mut out = Vec::new();
        if row_offsets[row_offsets.len() - 1] == 1 {
            // Only the root node, already read by key_count().
            let root = self.root_node.as_ref().expect("root populated").clone();
            if let NodeKind::Leaf(leaf) = root {
                for (k, (v, r)) in leaf.all_items() {
                    out.push((k.clone(), v.clone(), r.clone()));
                }
            }
            return Ok(out);
        }
        let start_of_leaves = row_offsets[row_offsets.len() - 2];
        let end_of_leaves = row_offsets[row_offsets.len() - 1];
        let needed: Vec<usize> = (start_of_leaves..end_of_leaves).collect();
        let nodes = self.read_leaf_nodes_ordered(&needed)?;
        for (_, leaf) in nodes {
            for (k, (v, r)) in leaf.all_items() {
                out.push((k.clone(), v.clone(), r.clone()));
            }
        }
        Ok(out)
    }

    /// Look up `keys` in the index. Returns `(key, value, refs)` for
    /// each key that's present. Order is unspecified.
    pub fn iter_entries(&mut self, keys: &[LeafKey]) -> Result<Vec<BTreeEntry>, IndexError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        if self.key_count()? == 0 {
            return Ok(Vec::new());
        }
        // Deduplicate. Python uses a frozenset.
        let mut seen: HashSet<LeafKey> = HashSet::new();
        let mut needed: Vec<LeafKey> = Vec::new();
        for k in keys {
            if seen.insert(k.clone()) {
                needed.push(k.clone());
            }
        }

        let (nodes, nodes_and_keys) = self.walk_through_internal_nodes(&needed)?;
        let mut out = Vec::new();
        for (node_index, sub_keys) in nodes_and_keys {
            if sub_keys.is_empty() {
                continue;
            }
            let leaf = nodes
                .get(&node_index)
                .ok_or_else(|| IndexError::Other(format!("missing leaf node {}", node_index)))?;
            for sk in &sub_keys {
                if let Some((v, r)) = leaf.get(sk) {
                    out.push((sk.clone(), v.clone(), r.clone()));
                }
            }
        }
        Ok(out)
    }

    /// Look up entries by prefix tuple. `None` segments match anything;
    /// the first segment must be a concrete value.
    ///
    /// **Note**: like the Python implementation, this triggers a full
    /// index parse — there's no partial index walk for prefix queries.
    pub fn iter_entries_prefix(
        &mut self,
        prefixes: &[Vec<Option<Vec<u8>>>],
    ) -> Result<Vec<BTreeEntry>, IndexError> {
        if prefixes.is_empty() {
            return Ok(Vec::new());
        }
        let key_length = self.key_length_unwrap()?;
        for p in prefixes {
            if p.len() != key_length {
                return Err(IndexError::Other(format!(
                    "BadIndexKey: prefix length {} != key length {}",
                    p.len(),
                    key_length
                )));
            }
            if !matches!(p.first(), Some(Some(_))) {
                return Err(IndexError::Other(
                    "BadIndexKey: first prefix element may not be None".to_string(),
                ));
            }
        }
        let entries = self.iter_all_entries()?;
        // Fast path for length-1 keys: prefixes are exact lookups.
        if key_length == 1 {
            let mut wanted: HashSet<LeafKey> = HashSet::new();
            for p in prefixes {
                wanted.insert(vec![p[0].clone().expect("validated above")]);
            }
            return Ok(entries
                .into_iter()
                .filter(|(k, _, _)| wanted.contains(k))
                .collect());
        }
        let mut out: Vec<BTreeEntry> = Vec::new();
        let mut emitted: HashSet<LeafKey> = HashSet::new();
        for prefix in prefixes {
            for (k, v, r) in &entries {
                if k.len() != key_length {
                    continue;
                }
                let matches = prefix
                    .iter()
                    .zip(k.iter())
                    .all(|(p_elem, k_elem)| match p_elem {
                        Some(p) => p == k_elem,
                        None => true,
                    });
                if matches && emitted.insert(k.clone()) {
                    out.push((k.clone(), v.clone(), r.clone()));
                }
            }
        }
        Ok(out)
    }

    /// Convenience: walk ancestry. Repeatedly calls `find_ancestors`
    /// until no more parent keys need checking. Returns
    /// `(parent_map, missing_keys)`.
    pub fn find_ancestry(
        &mut self,
        keys: &[LeafKey],
        ref_list_num: usize,
    ) -> Result<
        (
            std::collections::HashMap<LeafKey, LeafRefList>,
            HashSet<LeafKey>,
        ),
        IndexError,
    > {
        let mut parent_map: std::collections::HashMap<LeafKey, LeafRefList> =
            std::collections::HashMap::new();
        let mut missing_keys: HashSet<LeafKey> = HashSet::new();
        let mut pending: Vec<LeafKey> = keys.to_vec();
        while !pending.is_empty() {
            let next =
                self.find_ancestors(&pending, ref_list_num, &mut parent_map, &mut missing_keys)?;
            pending = next.into_iter().collect();
        }
        Ok((parent_map, missing_keys))
    }

    fn key_length_unwrap(&mut self) -> Result<usize, IndexError> {
        if self.key_length.is_none() {
            self.get_root_node()?;
        }
        self.key_length
            .ok_or_else(|| IndexError::Other("header not parsed".to_string()))
    }

    /// Validate that every node in the index can be read and parsed.
    pub fn validate(&mut self) -> Result<(), IndexError> {
        self.get_root_node()?;
        let row_lengths = self.row_lengths.as_ref().expect("populated").clone();
        let row_offsets = self.row_offsets.as_ref().expect("populated").clone();
        let start_node = if row_lengths.len() > 1 {
            row_offsets[1]
        } else {
            1
        };
        let node_end = *row_offsets.last().unwrap();
        if start_node >= node_end {
            return Ok(());
        }
        // Just read every page.
        let pages: Vec<usize> = (start_node..node_end).collect();
        let _ = self.get_and_cache_nodes(&pages)?;
        Ok(())
    }

    /// Compute the set of references made by entries in this index that
    /// are not themselves keys in the index. Mirrors `external_references`.
    pub fn external_references(
        &mut self,
        ref_list_num: usize,
    ) -> Result<HashSet<LeafKey>, IndexError> {
        self.get_root_node()?;
        let nrl = self
            .node_ref_lists
            .ok_or_else(|| IndexError::Other("header not parsed".to_string()))?;
        if ref_list_num + 1 > nrl {
            return Err(IndexError::Other(format!(
                "No ref list {}, index has {} ref lists",
                ref_list_num, nrl
            )));
        }
        let entries = self.iter_all_entries()?;
        let mut keys: HashSet<LeafKey> = HashSet::new();
        let mut refs: HashSet<LeafKey> = HashSet::new();
        for (k, _v, ref_lists) in entries {
            keys.insert(k);
            if let Some(list) = ref_lists.get(ref_list_num) {
                for r in list {
                    refs.insert(r.clone());
                }
            }
        }
        Ok(refs.difference(&keys).cloned().collect())
    }

    /// Walk ancestry: see Python `_find_ancestors`. Populates
    /// `parent_map` and `missing_keys`; returns the set of parent keys
    /// not yet in `parent_map` that need a follow-up search.
    pub fn find_ancestors(
        &mut self,
        keys: &[LeafKey],
        ref_list_num: usize,
        parent_map: &mut std::collections::HashMap<LeafKey, LeafRefList>,
        missing_keys: &mut HashSet<LeafKey>,
    ) -> Result<HashSet<LeafKey>, IndexError> {
        if self.key_count()? == 0 {
            for k in keys {
                missing_keys.insert(k.clone());
            }
            return Ok(HashSet::new());
        }
        let nrl = self.node_ref_lists.unwrap_or(0);
        if ref_list_num >= nrl {
            return Err(IndexError::Other(format!(
                "No ref list {}, index has {} ref lists",
                ref_list_num, nrl
            )));
        }

        let key_vec = keys.to_vec();
        let (nodes, nodes_and_keys) = self.walk_through_internal_nodes(&key_vec)?;
        let mut parents_not_on_page: HashSet<LeafKey> = HashSet::new();

        for (node_index, sub_keys) in nodes_and_keys {
            if sub_keys.is_empty() {
                continue;
            }
            let leaf = nodes
                .get(&node_index)
                .ok_or_else(|| IndexError::Other(format!("missing leaf {}", node_index)))?;
            let mut parents_to_check: HashSet<LeafKey> = HashSet::new();
            for sk in &sub_keys {
                match leaf.get(sk) {
                    None => {
                        missing_keys.insert(sk.clone());
                    }
                    Some((_v, refs)) => {
                        let parent_keys = refs.get(ref_list_num).cloned().unwrap_or_default();
                        parent_map.insert(sk.clone(), parent_keys.clone());
                        for p in parent_keys {
                            parents_to_check.insert(p);
                        }
                    }
                }
            }
            // Don't look for things we've already found.
            parents_to_check.retain(|p| !parent_map.contains_key(p));
            while !parents_to_check.is_empty() {
                let mut next_check: HashSet<LeafKey> = HashSet::new();
                for key in &parents_to_check {
                    if let Some((_v, refs)) = leaf.get(key) {
                        let parent_keys = refs.get(ref_list_num).cloned().unwrap_or_default();
                        parent_map.insert(key.clone(), parent_keys.clone());
                        for p in parent_keys {
                            next_check.insert(p);
                        }
                    } else {
                        // Out of leaf range vs maybe missing.
                        let earlier = leaf.min_key.as_ref().is_some_and(|min| key < min);
                        let later = leaf.max_key.as_ref().is_some_and(|max| key > max);
                        if earlier || later {
                            parents_not_on_page.insert(key.clone());
                        } else {
                            missing_keys.insert(key.clone());
                        }
                    }
                }
                parents_to_check = next_check
                    .into_iter()
                    .filter(|p| !parent_map.contains_key(p))
                    .collect();
            }
        }
        // Cull parents we've already accounted for.
        let already_known: HashSet<LeafKey> = parent_map.keys().cloned().collect();
        let mut search: HashSet<LeafKey> = parents_not_on_page
            .difference(&already_known)
            .cloned()
            .collect();
        search.retain(|k| !missing_keys.contains(k));
        Ok(search)
    }

    // ---------------- internal helpers ----------------

    fn get_root_node(&mut self) -> Result<&NodeKind, IndexError> {
        if self.root_node.is_none() {
            self.get_internal_nodes(&[0])?;
        }
        Ok(self.root_node.as_ref().expect("root populated"))
    }

    fn compute_total_pages_in_index(&self) -> Result<u64, IndexError> {
        if let Some(row_offsets) = &self.row_offsets {
            return Ok(*row_offsets.last().unwrap_or(&0) as u64);
        }
        let size = self
            .size
            .ok_or_else(|| IndexError::Other("size unknown".to_string()))?;
        Ok(round_up_div(size, PAGE_SIZE as u64))
    }

    fn get_offsets_to_cached_pages(&self) -> HashSet<usize> {
        let mut set: HashSet<usize> = HashSet::new();
        for (k, _) in self.internal_cache.iter() {
            set.insert(*k);
        }
        for (k, _) in self.leaf_cache.iter() {
            set.insert(*k);
        }
        if self.root_node.is_some() {
            set.insert(0);
        }
        set
    }

    fn expand_offsets(&self, offsets: &[usize]) -> Result<Vec<usize>, IndexError> {
        // Mirrors the Python prefetch heuristic.
        if offsets.len() >= self.recommended_pages {
            return Ok(offsets.to_vec());
        }
        if self.size.is_none() {
            return Ok(offsets.to_vec());
        }
        let total_pages = self.compute_total_pages_in_index()? as usize;
        let cached = self.get_offsets_to_cached_pages();
        if total_pages.saturating_sub(cached.len()) <= self.recommended_pages {
            // Read whatever is left.
            let mut expanded: Vec<usize> =
                (0..total_pages).filter(|p| !cached.contains(p)).collect();
            expanded.sort();
            return Ok(expanded);
        }
        // First-read of root: don't pre-fetch yet.
        if self.root_node.is_none() {
            return Ok(offsets.to_vec());
        }
        let row_lengths = self.row_lengths.as_ref().expect("populated");
        let tree_depth = row_lengths.len();
        if cached.len() < tree_depth && offsets.len() == 1 {
            return Ok(offsets.to_vec());
        }
        let row_offsets = self.row_offsets.as_ref().expect("populated");
        let mut final_offsets =
            self.expand_to_neighbors(offsets, &cached, total_pages, row_offsets);
        final_offsets.sort();
        Ok(final_offsets)
    }

    fn expand_to_neighbors(
        &self,
        offsets: &[usize],
        cached: &HashSet<usize>,
        total_pages: usize,
        row_offsets: &[usize],
    ) -> Vec<usize> {
        let mut final_offsets: HashSet<usize> = offsets.iter().copied().collect();
        let mut new_tips = final_offsets.clone();
        let mut layer: Option<(usize, usize)> = None;
        while final_offsets.len() < self.recommended_pages && !new_tips.is_empty() {
            let mut next_tips: HashSet<usize> = HashSet::new();
            for &pos in &new_tips {
                if layer.is_none() {
                    layer = Some(find_layer_first_and_end(row_offsets, pos));
                }
                let (first, end) = layer.unwrap();
                if pos > 0 {
                    let prev = pos - 1;
                    if prev >= first && !cached.contains(&prev) && !final_offsets.contains(&prev) {
                        next_tips.insert(prev);
                    }
                }
                let after = pos + 1;
                if after < total_pages
                    && after < end
                    && !cached.contains(&after)
                    && !final_offsets.contains(&after)
                {
                    next_tips.insert(after);
                }
            }
            for n in &next_tips {
                final_offsets.insert(*n);
            }
            new_tips = next_tips;
        }
        final_offsets.into_iter().collect()
    }

    fn parse_header(&mut self, data: &[u8]) -> Result<(usize, Vec<u8>), IndexError> {
        let header = parse_btree_header(data).map_err(|e| match e {
            BTreeIndexError::BadSignature => IndexError::BadSignature,
            BTreeIndexError::BadOptions => IndexError::BadOptions,
            BTreeIndexError::BadInternalNode => IndexError::Other("bad btree node".to_string()),
        })?;
        self.node_ref_lists = Some(header.node_ref_lists);
        self.key_length = Some(header.key_length);
        self.key_count = Some(header.key_count);
        self.row_offsets = Some(compute_row_offsets(&header.row_lengths));
        self.row_lengths = Some(header.row_lengths);
        Ok((header.header_end, data[header.header_end..].to_vec()))
    }

    fn read_pages(&mut self, pages: &[usize]) -> Result<Vec<(usize, NodeKind)>, IndexError> {
        // Mirrors `_read_nodes`.
        let mut bytes_buf: Option<Vec<u8>> = None;
        let mut ranges: Vec<(u64, u64)> = Vec::new();
        let base_offset = self.base_offset;

        for &index in pages {
            let offset = (index as u64) * PAGE_SIZE as u64;
            let mut size = PAGE_SIZE as u64;
            if index == 0 {
                if let Some(file_size) = self.size {
                    size = (PAGE_SIZE as u64).min(file_size);
                } else {
                    // Don't know the size: read the whole file.
                    let data = self.transport.get_bytes(&self.name)?;
                    let total = data.len() as u64;
                    self.size = Some(total - base_offset);
                    let mut chunked: Vec<(u64, u64)> = Vec::new();
                    let mut start = base_offset;
                    while start < total {
                        let take = (PAGE_SIZE as u64).min(total - start);
                        chunked.push((start, take));
                        start += PAGE_SIZE as u64;
                    }
                    bytes_buf = Some(data);
                    ranges = chunked;
                    break;
                }
            } else {
                let file_size = self.size.unwrap_or(0);
                if offset > file_size {
                    return Err(IndexError::Other(format!(
                        "tried to read past the end of the file {} > {}",
                        offset, file_size
                    )));
                }
                size = size.min(file_size - offset);
            }
            ranges.push((base_offset + offset, size));
        }

        if ranges.is_empty() {
            return Ok(Vec::new());
        }

        let data_ranges: Vec<(u64, Vec<u8>)> = if let Some(buf) = bytes_buf {
            ranges
                .iter()
                .map(|(start, size)| {
                    let s = *start as usize;
                    let e = s + *size as usize;
                    (*start, buf[s..e].to_vec())
                })
                .collect()
        } else {
            self.transport.readv(
                &self.name,
                &ranges,
                true,
                base_offset + self.size.unwrap_or(0),
            )?
        };

        let mut out = Vec::with_capacity(data_ranges.len());
        for (offset, mut data) in data_ranges {
            let local_offset = offset - base_offset;
            let mut payload: Vec<u8> = if local_offset == 0 {
                let (_header_end, rest) = self.parse_header(&data)?;
                if rest.is_empty() {
                    continue;
                }
                rest
            } else {
                std::mem::take(&mut data)
            };
            // Decompress and parse.
            let key_length = self
                .key_length
                .ok_or_else(|| IndexError::Other("header not parsed".to_string()))?;
            let nrl = self
                .node_ref_lists
                .ok_or_else(|| IndexError::Other("header not parsed".to_string()))?;
            let node = decode_node(&payload, key_length, nrl)
                .map_err(|e| IndexError::Other(format!("bad btree node: {}", e)))?;
            payload.clear();
            let page_index = local_offset as usize / PAGE_SIZE;
            out.push((page_index, node));
        }
        Ok(out)
    }

    fn get_and_cache_nodes(
        &mut self,
        pages: &[usize],
    ) -> Result<std::collections::HashMap<usize, NodeKind>, IndexError> {
        let mut found: std::collections::HashMap<usize, NodeKind> =
            std::collections::HashMap::new();
        let mut sorted_pages = pages.to_vec();
        sorted_pages.sort();
        for (page_index, node) in self.read_pages(&sorted_pages)? {
            if page_index == 0 {
                self.root_node = Some(node.clone());
            } else {
                let row_offsets = self.row_offsets.as_ref().expect("header parsed");
                let start_of_leaves = row_offsets[row_offsets.len() - 2];
                if page_index < start_of_leaves {
                    if let NodeKind::Internal(ref n) = node {
                        self.internal_cache.put(page_index, n.clone());
                    }
                } else if let NodeKind::Leaf(ref n) = node {
                    self.leaf_cache.put(page_index, n.clone());
                }
            }
            found.insert(page_index, node);
        }
        Ok(found)
    }

    /// Get internal nodes — root + non-leaf pages — pulling from cache
    /// when possible, otherwise reading from the transport.
    fn get_internal_nodes(
        &mut self,
        pages: &[usize],
    ) -> Result<std::collections::HashMap<usize, InternalNode>, IndexError> {
        let mut out: std::collections::HashMap<usize, InternalNode> =
            std::collections::HashMap::new();
        let mut needed: Vec<usize> = Vec::new();
        for &p in pages {
            if p == 0 {
                if let Some(NodeKind::Internal(n)) = &self.root_node {
                    out.insert(0, n.clone());
                    continue;
                }
                needed.push(0);
                continue;
            }
            if let Some(n) = self.internal_cache.get(&p) {
                out.insert(p, n.clone());
            } else {
                needed.push(p);
            }
        }
        if needed.is_empty() {
            return Ok(out);
        }
        let needed = self.expand_offsets(&needed)?;
        let fetched = self.get_and_cache_nodes(&needed)?;
        for (idx, node) in fetched {
            if let NodeKind::Internal(n) = node {
                out.insert(idx, n);
            } else if idx == 0 {
                // The root may also be a leaf in tiny indices.
            }
        }
        Ok(out)
    }

    fn get_leaf_nodes(
        &mut self,
        pages: &[usize],
    ) -> Result<std::collections::HashMap<usize, LeafNode>, IndexError> {
        let mut out: std::collections::HashMap<usize, LeafNode> = std::collections::HashMap::new();
        let mut needed: Vec<usize> = Vec::new();
        for &p in pages {
            if p == 0 {
                if let Some(NodeKind::Leaf(n)) = &self.root_node {
                    out.insert(0, n.clone());
                    continue;
                }
                needed.push(0);
                continue;
            }
            if let Some(n) = self.leaf_cache.get(&p) {
                out.insert(p, n.clone());
            } else {
                needed.push(p);
            }
        }
        if needed.is_empty() {
            return Ok(out);
        }
        let needed = self.expand_offsets(&needed)?;
        let fetched = self.get_and_cache_nodes(&needed)?;
        for (idx, node) in fetched {
            if let NodeKind::Leaf(n) = node {
                out.insert(idx, n);
            }
        }
        Ok(out)
    }

    fn read_leaf_nodes_ordered(
        &mut self,
        pages: &[usize],
    ) -> Result<Vec<(usize, LeafNode)>, IndexError> {
        let map = self.get_leaf_nodes(pages)?;
        let mut out: Vec<(usize, LeafNode)> = pages
            .iter()
            .filter_map(|p| map.get(p).map(|n| (*p, n.clone())))
            .collect();
        out.sort_by_key(|(p, _)| *p);
        Ok(out)
    }

    /// Walk internal nodes to find the leaf nodes covering each requested
    /// key. Returns `(leaf_nodes, [(leaf_index, [keys for that leaf])])`.
    fn walk_through_internal_nodes(
        &mut self,
        keys: &[LeafKey],
    ) -> Result<
        (
            std::collections::HashMap<usize, LeafNode>,
            Vec<(usize, Vec<LeafKey>)>,
        ),
        IndexError,
    > {
        let mut sorted_keys: Vec<LeafKey> = keys.to_vec();
        sorted_keys.sort();
        let mut keys_at_index: Vec<(usize, Vec<LeafKey>)> = vec![(0, sorted_keys)];

        let row_offsets = self.row_offsets.as_ref().expect("header populated").clone();
        // Iterate row_offsets[1..len-1]: the non-leaf rows below the root.
        let mid_rows: Vec<usize> = row_offsets[1..row_offsets.len() - 1].to_vec();
        for next_row_start in mid_rows {
            let node_indexes: Vec<usize> = keys_at_index.iter().map(|(i, _)| *i).collect();
            let nodes = self.get_internal_nodes(&node_indexes)?;
            let mut next: Vec<(usize, Vec<LeafKey>)> = Vec::new();
            for (node_index, sub_keys) in keys_at_index.into_iter() {
                let node = nodes
                    .get(&node_index)
                    .ok_or_else(|| {
                        IndexError::Other(format!("missing internal node {}", node_index))
                    })?
                    .clone();
                let positions = multi_bisect_right(&sub_keys, &node.keys);
                let node_offset = next_row_start + node.offset;
                for (pos, sk) in positions {
                    next.push((node_offset + pos, sk));
                }
            }
            keys_at_index = next;
        }
        let leaf_indexes: Vec<usize> = keys_at_index.iter().map(|(i, _)| *i).collect();
        let nodes = self.get_leaf_nodes(&leaf_indexes)?;
        Ok((nodes, keys_at_index))
    }
}

fn parse_usize_option(line: &[u8], prefix: &[u8]) -> Result<usize, BTreeIndexError> {
    if !line.starts_with(prefix) {
        return Err(BTreeIndexError::BadOptions);
    }
    std::str::from_utf8(&line[prefix.len()..])
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .ok_or(BTreeIndexError::BadOptions)
}

fn parse_row_lengths(line: &[u8]) -> Result<Vec<usize>, BTreeIndexError> {
    if !line.starts_with(OPTION_ROW_LENGTHS) {
        return Err(BTreeIndexError::BadOptions);
    }
    let payload = &line[OPTION_ROW_LENGTHS.len()..];
    let mut out = Vec::new();
    for part in payload.split(|&b| b == b',') {
        // Empty parts (trailing comma, or empty payload entirely) are
        // skipped, matching Python's `if length` filter.
        if part.is_empty() {
            continue;
        }
        let n = std::str::from_utf8(part)
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .ok_or(BTreeIndexError::BadOptions)?;
        out.push(n);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_header(
        node_ref_lists: usize,
        key_length: usize,
        key_count: usize,
        row_lengths: &str,
    ) -> Vec<u8> {
        let mut data = BTREE_SIGNATURE.to_vec();
        data.extend_from_slice(format!("node_ref_lists={}\n", node_ref_lists).as_bytes());
        data.extend_from_slice(format!("key_elements={}\n", key_length).as_bytes());
        data.extend_from_slice(format!("len={}\n", key_count).as_bytes());
        data.extend_from_slice(format!("row_lengths={}\n", row_lengths).as_bytes());
        data
    }

    #[test]
    fn parse_header_minimal() {
        let data = build_header(0, 1, 0, "");
        let h = parse_btree_header(&data).unwrap();
        assert_eq!(h.node_ref_lists, 0);
        assert_eq!(h.key_length, 1);
        assert_eq!(h.key_count, 0);
        assert!(h.row_lengths.is_empty());
        assert_eq!(h.header_end, data.len());
    }

    #[test]
    fn parse_header_multi_row() {
        let data = build_header(2, 3, 100, "1,4,20");
        let h = parse_btree_header(&data).unwrap();
        assert_eq!(h.node_ref_lists, 2);
        assert_eq!(h.key_length, 3);
        assert_eq!(h.key_count, 100);
        assert_eq!(h.row_lengths, vec![1, 4, 20]);
    }

    #[test]
    fn parse_header_trailing_comma_in_row_lengths() {
        // Python's `if length` filter drops empty parts from the split —
        // tolerate the same.
        let data = build_header(1, 1, 10, "5,");
        let h = parse_btree_header(&data).unwrap();
        assert_eq!(h.row_lengths, vec![5]);
    }

    #[test]
    fn parse_header_rejects_bad_signature() {
        let data = b"Not a btree index\nnode_ref_lists=0\nkey_elements=1\nlen=0\nrow_lengths=\n";
        assert_eq!(parse_btree_header(data), Err(BTreeIndexError::BadSignature));
    }

    #[test]
    fn parse_header_rejects_missing_option() {
        let mut data = BTREE_SIGNATURE.to_vec();
        data.extend_from_slice(b"wrong=0\nkey_elements=1\nlen=0\nrow_lengths=\n");
        assert_eq!(parse_btree_header(&data), Err(BTreeIndexError::BadOptions));
    }

    #[test]
    fn parse_header_rejects_non_decimal_option() {
        let mut data = BTREE_SIGNATURE.to_vec();
        data.extend_from_slice(b"node_ref_lists=abc\nkey_elements=1\nlen=0\nrow_lengths=\n");
        assert_eq!(parse_btree_header(&data), Err(BTreeIndexError::BadOptions));
    }

    #[test]
    fn parse_header_rejects_non_decimal_row_length() {
        let mut data = BTREE_SIGNATURE.to_vec();
        data.extend_from_slice(b"node_ref_lists=0\nkey_elements=1\nlen=0\nrow_lengths=1,xyz\n");
        assert_eq!(parse_btree_header(&data), Err(BTreeIndexError::BadOptions));
    }

    #[test]
    fn parse_header_rejects_truncated() {
        // Only three option lines — missing row_lengths.
        let mut data = BTREE_SIGNATURE.to_vec();
        data.extend_from_slice(b"node_ref_lists=0\nkey_elements=1\nlen=0\n");
        assert_eq!(parse_btree_header(&data), Err(BTreeIndexError::BadOptions));
    }

    #[test]
    fn parse_header_end_offset_matches_byte_count() {
        let data = build_header(1, 2, 5, "1,2,3");
        let h = parse_btree_header(&data).unwrap();
        // The computed `header_end` should equal the total data length
        // (there's no trailing data after the row_lengths newline here).
        assert_eq!(h.header_end, data.len());
    }

    fn key(parts: &[&[u8]]) -> Vec<Vec<u8>> {
        parts.iter().map(|p| p.to_vec()).collect()
    }

    #[test]
    fn parse_internal_node_basic() {
        // Mirrors the cross-checked Python output for the same body.
        let body = b"type=internal\noffset=42\nkey1\none\x00two\nkey3\n";
        let n = parse_internal_node(body).unwrap();
        assert_eq!(n.offset, 42);
        assert_eq!(
            n.keys,
            vec![key(&[b"key1"]), key(&[b"one", b"two"]), key(&[b"key3"])]
        );
    }

    #[test]
    fn parse_internal_node_stops_at_first_empty_line() {
        // Content after the first empty line (explicit terminator) is
        // silently dropped, matching the Python `break` behavior.
        let body = b"type=internal\noffset=0\nalpha\n\nGARBAGE\nmore\n";
        let n = parse_internal_node(body).unwrap();
        assert_eq!(n.offset, 0);
        assert_eq!(n.keys, vec![key(&[b"alpha"])]);
    }

    #[test]
    fn parse_internal_node_no_keys() {
        let body = b"type=internal\noffset=7\n";
        let n = parse_internal_node(body).unwrap();
        assert_eq!(n.offset, 7);
        assert!(n.keys.is_empty());
    }

    #[test]
    fn parse_internal_node_rejects_missing_offset_line() {
        let body = b"type=internal\n";
        assert_eq!(
            parse_internal_node(body),
            Err(BTreeIndexError::BadInternalNode)
        );
    }

    #[test]
    fn parse_internal_node_rejects_short_offset_line() {
        // `offset=` is 7 bytes; anything shorter can't even be the prefix.
        let body = b"type=internal\nabc\n";
        assert_eq!(
            parse_internal_node(body),
            Err(BTreeIndexError::BadInternalNode)
        );
    }

    #[test]
    fn parse_internal_node_rejects_non_decimal_offset() {
        let body = b"type=internal\noffset=nope\n";
        assert_eq!(
            parse_internal_node(body),
            Err(BTreeIndexError::BadInternalNode)
        );
    }

    #[test]
    fn parse_leaf_lines_basic() {
        // Single key, no refs, value "v".
        let body = b"type=leaf\nkey1\0\0v\n";
        let entries = parse_leaf_lines(body, 1, 0).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, vec![b"key1".to_vec()]);
        assert_eq!(entries[0].1, b"v");
        assert!(entries[0].2.is_empty());
    }

    #[test]
    fn parse_leaf_lines_two_part_key_with_refs() {
        // key=("k1","k2"), 2 ref lists, value "val".
        // refs section: <list1>\t<list2>; each list is \r-separated keys.
        // list1 = [(b"r1a",b"r1b"), (b"r2a",b"r2b")]; list2 = []
        let body = b"type=leaf\nk1\0k2\0r1a\0r1b\rr2a\0r2b\t\0val\n";
        let entries = parse_leaf_lines(body, 2, 2).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, vec![b"k1".to_vec(), b"k2".to_vec()]);
        assert_eq!(entries[0].1, b"val");
        assert_eq!(entries[0].2.len(), 2);
        assert_eq!(
            entries[0].2[0],
            vec![
                vec![b"r1a".to_vec(), b"r1b".to_vec()],
                vec![b"r2a".to_vec(), b"r2b".to_vec()],
            ]
        );
        assert!(entries[0].2[1].is_empty());
    }

    #[test]
    fn parse_leaf_lines_rejects_missing_header() {
        let body = b"k\0\0v\n";
        assert!(matches!(
            parse_leaf_lines(body, 1, 0),
            Err(BTreeIndexError::BadInternalNode)
        ));
    }

    #[test]
    fn parse_leaf_lines_rejects_refs_when_no_ref_list_expected() {
        let body = b"type=leaf\nkey\0refstuff\0v\n";
        assert!(matches!(
            parse_leaf_lines(body, 1, 0),
            Err(BTreeIndexError::BadInternalNode)
        ));
    }

    #[test]
    fn leaf_node_tracks_min_max_keys() {
        let body = b"type=leaf\na\0\0v1\nb\0\0v2\nc\0\0v3\n";
        let leaf = LeafNode::parse(body, 1, 0).unwrap();
        assert_eq!(leaf.len(), 3);
        assert_eq!(leaf.min_key, Some(vec![b"a".to_vec()]));
        assert_eq!(leaf.max_key, Some(vec![b"c".to_vec()]));
        assert!(leaf.contains_key(&vec![b"b".to_vec()]));
    }

    #[test]
    fn leaf_node_empty() {
        let body = b"type=leaf\n";
        let leaf = LeafNode::parse(body, 1, 0).unwrap();
        assert!(leaf.is_empty());
        assert_eq!(leaf.min_key, None);
        assert_eq!(leaf.max_key, None);
    }

    #[test]
    fn leaf_node_all_items_sorted() {
        // Even when written out of order, all_items returns sorted by key.
        let body = b"type=leaf\nb\0\0v2\na\0\0v1\nc\0\0v3\n";
        let leaf = LeafNode::parse(body, 1, 0).unwrap();
        let keys: Vec<_> = leaf.all_items().map(|(k, _)| k.clone()).collect();
        assert_eq!(
            keys,
            vec![
                vec![b"a".to_vec()],
                vec![b"b".to_vec()],
                vec![b"c".to_vec()]
            ]
        );
    }

    #[test]
    fn compute_row_offsets_basic() {
        assert_eq!(compute_row_offsets(&[]), vec![0]);
        assert_eq!(compute_row_offsets(&[1]), vec![0, 1]);
        assert_eq!(compute_row_offsets(&[1, 4, 20]), vec![0, 1, 5, 25]);
    }

    #[test]
    fn find_layer_first_and_end_basic() {
        // Three rows: row 0 covers [0,1), row 1 covers [1,5), row 2 covers [5,25).
        let row_offsets = vec![0, 1, 5, 25];
        assert_eq!(find_layer_first_and_end(&row_offsets, 0), (0, 1));
        assert_eq!(find_layer_first_and_end(&row_offsets, 1), (1, 5));
        assert_eq!(find_layer_first_and_end(&row_offsets, 4), (1, 5));
        assert_eq!(find_layer_first_and_end(&row_offsets, 5), (5, 25));
    }

    #[test]
    fn multi_bisect_right_empty_inputs() {
        assert!(multi_bisect_right(&[], &[]).is_empty());
        // Empty fixed: everything falls left.
        let in_keys = vec![vec![b"a".to_vec()], vec![b"b".to_vec()]];
        assert_eq!(
            multi_bisect_right(&in_keys, &[]),
            vec![(0, in_keys.clone())]
        );
    }

    #[test]
    fn multi_bisect_right_single_in_key() {
        // Single in_key uses bisect_right.
        let fixed = vec![vec![b"b".to_vec()], vec![b"d".to_vec()]];
        // "a" -> 0, "b" -> 1, "c" -> 1, "d" -> 2, "e" -> 2.
        for (k, expected_pos) in &[
            (b"a".to_vec(), 0),
            (b"b".to_vec(), 1),
            (b"c".to_vec(), 1),
            (b"d".to_vec(), 2),
            (b"e".to_vec(), 2),
        ] {
            let in_keys = vec![vec![k.clone()]];
            let res = multi_bisect_right(&in_keys, &fixed);
            assert_eq!(res, vec![(*expected_pos, in_keys)]);
        }
    }

    #[test]
    fn multi_bisect_right_multi_in_key() {
        let fixed = vec![vec![b"b".to_vec()], vec![b"d".to_vec()]];
        // ["a","c","e"] split into [(0,["a"]), (1,["c"]), (2,["e"])].
        let in_keys = vec![
            vec![b"a".to_vec()],
            vec![b"c".to_vec()],
            vec![b"e".to_vec()],
        ];
        let result = multi_bisect_right(&in_keys, &fixed);
        assert_eq!(result.len(), 3);
        assert_eq!(result[0], (0, vec![vec![b"a".to_vec()]]));
        assert_eq!(result[1], (1, vec![vec![b"c".to_vec()]]));
        assert_eq!(result[2], (2, vec![vec![b"e".to_vec()]]));
    }

    use crate::index::IndexTransport;

    /// Tiny in-memory IndexTransport used for end-to-end tests.
    struct MemTransport {
        files: std::collections::HashMap<String, Vec<u8>>,
    }
    impl MemTransport {
        fn new(name: &str, data: Vec<u8>) -> Self {
            let mut files = std::collections::HashMap::new();
            files.insert(name.to_string(), data);
            Self { files }
        }
    }
    impl IndexTransport for MemTransport {
        fn get_bytes(&self, path: &str) -> Result<Vec<u8>, IndexError> {
            self.files
                .get(path)
                .cloned()
                .ok_or_else(|| IndexError::Other(format!("no such file {}", path)))
        }
        fn recommended_page_size(&self) -> u64 {
            64 * 1024
        }
    }

    fn build_index(
        nodes: &[(LeafKey, Vec<u8>, Vec<Vec<LeafKey>>)],
        reference_lists: usize,
        key_length: usize,
    ) -> Vec<u8> {
        use crate::btree_builder;
        let pairs: Vec<(btree_builder::Key, btree_builder::Node)> = nodes
            .iter()
            .map(|(k, v, refs)| {
                (
                    k.clone(),
                    btree_builder::Node {
                        references: refs.clone(),
                        value: v.clone(),
                    },
                )
            })
            .collect();
        btree_builder::write_nodes(
            &pairs,
            reference_lists,
            key_length,
            false,
            btree_builder::Layout::default(),
        )
        .expect("serialize")
    }

    #[test]
    fn graph_index_iter_all_entries_round_trip() {
        // Values must not contain NUL — that's the same constraint the
        // Python format docstring spells out as "no-newline-no-null-bytes".
        let nodes: Vec<(LeafKey, Vec<u8>, Vec<Vec<LeafKey>>)> = (0..50)
            .map(|i| {
                (
                    vec![format!("key{:04}", i).into_bytes()],
                    format!("v{}", i).into_bytes(),
                    vec![],
                )
            })
            .collect();
        let data = build_index(&nodes, 0, 1);
        let size = data.len() as u64;
        let transport = MemTransport::new("idx", data);
        let mut idx = BTreeGraphIndex::new(transport, "idx", Some(size), 0);
        let entries = idx.iter_all_entries().expect("iter_all");
        assert_eq!(entries.len(), 50);
        // Sorted by key (BTreeMap iteration order).
        for (i, (k, v, _)) in entries.iter().enumerate() {
            assert_eq!(k[0], format!("key{:04}", i).into_bytes());
            assert_eq!(*v, format!("v{}", i).into_bytes());
        }
    }

    #[test]
    fn graph_index_iter_entries_specific_keys() {
        let nodes: Vec<(LeafKey, Vec<u8>, Vec<Vec<LeafKey>>)> = (0..200)
            .map(|i| {
                (
                    vec![format!("k{:04}", i).into_bytes()],
                    format!("v{}", i).into_bytes(),
                    vec![],
                )
            })
            .collect();
        let data = build_index(&nodes, 0, 1);
        let size = data.len() as u64;
        let transport = MemTransport::new("idx", data);
        let mut idx = BTreeGraphIndex::new(transport, "idx", Some(size), 0);

        let wanted = vec![
            vec![b"k0001".to_vec()],
            vec![b"k0050".to_vec()],
            vec![b"k0199".to_vec()],
            vec![b"missing".to_vec()],
        ];
        let mut got = idx.iter_entries(&wanted).expect("iter_entries");
        got.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].0, vec![b"k0001".to_vec()]);
        assert_eq!(got[0].1, b"v1");
        assert_eq!(got[1].0, vec![b"k0050".to_vec()]);
        assert_eq!(got[2].0, vec![b"k0199".to_vec()]);
    }

    #[test]
    fn graph_index_validate_walks_every_page() {
        let nodes: Vec<(LeafKey, Vec<u8>, Vec<Vec<LeafKey>>)> = (0..1000)
            .map(|i| (vec![format!("k{:05}", i).into_bytes()], vec![1], vec![]))
            .collect();
        let data = build_index(&nodes, 0, 1);
        let size = data.len() as u64;
        let transport = MemTransport::new("idx", data);
        let mut idx = BTreeGraphIndex::new(transport, "idx", Some(size), 0);
        idx.validate().expect("validate");
    }

    #[test]
    fn graph_index_key_count_matches_header() {
        let nodes: Vec<(LeafKey, Vec<u8>, Vec<Vec<LeafKey>>)> = (0..123)
            .map(|i| (vec![format!("k{:04}", i).into_bytes()], vec![1], vec![]))
            .collect();
        let data = build_index(&nodes, 0, 1);
        let size = data.len() as u64;
        let transport = MemTransport::new("idx", data);
        let mut idx = BTreeGraphIndex::new(transport, "idx", Some(size), 0);
        assert_eq!(idx.key_count().unwrap(), 123);
    }

    #[test]
    fn graph_index_iter_entries_prefix_two_part_key() {
        let nodes: Vec<(LeafKey, Vec<u8>, Vec<Vec<LeafKey>>)> = vec![
            (vec![b"a".to_vec(), b"1".to_vec()], b"av1".to_vec(), vec![]),
            (vec![b"a".to_vec(), b"2".to_vec()], b"av2".to_vec(), vec![]),
            (vec![b"b".to_vec(), b"1".to_vec()], b"bv1".to_vec(), vec![]),
        ];
        let data = build_index(&nodes, 0, 2);
        let size = data.len() as u64;
        let transport = MemTransport::new("idx", data);
        let mut idx = BTreeGraphIndex::new(transport, "idx", Some(size), 0);
        let prefixes = vec![vec![Some(b"a".to_vec()), None]];
        let mut got = idx.iter_entries_prefix(&prefixes).unwrap();
        got.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].0, vec![b"a".to_vec(), b"1".to_vec()]);
        assert_eq!(got[1].0, vec![b"a".to_vec(), b"2".to_vec()]);
    }

    #[test]
    fn graph_index_external_references() {
        // 3 keys; key "c" references "a" (in-index) and "missing"
        // (out-of-index). external_references(0) should report only "missing".
        let nodes: Vec<(LeafKey, Vec<u8>, Vec<Vec<LeafKey>>)> = vec![
            (vec![b"a".to_vec()], b"".to_vec(), vec![vec![]]),
            (vec![b"b".to_vec()], b"".to_vec(), vec![vec![]]),
            (
                vec![b"c".to_vec()],
                b"".to_vec(),
                vec![vec![vec![b"a".to_vec()], vec![b"missing".to_vec()]]],
            ),
        ];
        let data = build_index(&nodes, 1, 1);
        let size = data.len() as u64;
        let transport = MemTransport::new("idx", data);
        let mut idx = BTreeGraphIndex::new(transport, "idx", Some(size), 0);
        let refs = idx.external_references(0).unwrap();
        let mut refs_v: Vec<_> = refs.into_iter().collect();
        refs_v.sort();
        assert_eq!(refs_v, vec![vec![b"missing".to_vec()]]);
    }

    #[test]
    fn graph_index_find_ancestry_basic() {
        // a -> b -> c, plus orphan d. find_ancestry([a]) should map
        // {a:[b], b:[c], c:[]} and missing should be empty.
        let nodes: Vec<(LeafKey, Vec<u8>, Vec<Vec<LeafKey>>)> = vec![
            (
                vec![b"a".to_vec()],
                b"".to_vec(),
                vec![vec![vec![b"b".to_vec()]]],
            ),
            (
                vec![b"b".to_vec()],
                b"".to_vec(),
                vec![vec![vec![b"c".to_vec()]]],
            ),
            (vec![b"c".to_vec()], b"".to_vec(), vec![vec![]]),
            (vec![b"d".to_vec()], b"".to_vec(), vec![vec![]]),
        ];
        let data = build_index(&nodes, 1, 1);
        let size = data.len() as u64;
        let transport = MemTransport::new("idx", data);
        let mut idx = BTreeGraphIndex::new(transport, "idx", Some(size), 0);
        let (parent_map, missing) = idx.find_ancestry(&[vec![b"a".to_vec()]], 0).unwrap();
        assert!(missing.is_empty());
        assert_eq!(parent_map.len(), 3);
        assert_eq!(parent_map[&vec![b"a".to_vec()]], vec![vec![b"b".to_vec()]]);
        assert_eq!(parent_map[&vec![b"b".to_vec()]], vec![vec![b"c".to_vec()]]);
        assert!(parent_map[&vec![b"c".to_vec()]].is_empty());
    }
}
