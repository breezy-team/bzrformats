//! B+Tree graph index format helpers.
//!
//! Pure-Rust port of the parsing and prefetch logic from
//! `bzrformats/btree_index.py`. These are stateless helpers: header/node
//! parsing, the `multi_bisect_right` search, and the read-prefetch
//! heuristic. The stateful reader (`BTreeGraphIndex`) and the
//! `BTreeBuilder` orchestration live in the `bazaar-py` pyo3 layer, which
//! drives IO and caching over Python objects and calls back into these
//! functions for the pure computation.

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

/// Parse a single body line of a B+Tree leaf node (after the
/// `type=leaf` header has already been consumed). Used by the
/// streaming pyo3 parser that processes lines one at a time.
pub fn parse_leaf_line(
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

/// Round `recommended_page_size` bytes up to a count of [`PAGE_SIZE`] pages.
/// Mirrors `_compute_recommended_pages`.
pub fn recommended_pages_for_read(recommended_read: u64) -> usize {
    recommended_read.div_ceil(PAGE_SIZE as u64) as usize
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

/// Decompress a single page worth of zlib-compressed bytes. Mirrors the
/// `zlib.decompress` call in `_read_nodes`; node-type dispatch and parsing
/// happen in the caller (the pyo3 layer, which may hand leaf bytes to a
/// pluggable Python `_leaf_factory`).
pub fn decompress_page(data: &[u8]) -> Result<Vec<u8>, BTreeIndexError> {
    use std::io::Read;
    let mut z = flate2::read::ZlibDecoder::new(data);
    let mut decompressed = Vec::with_capacity(PAGE_SIZE);
    z.read_to_end(&mut decompressed)
        .map_err(|_| BTreeIndexError::BadInternalNode)?;
    Ok(decompressed)
}

/// How many pages are in the index. Mirrors `_compute_total_pages_in_index`.
///
/// When the header has been parsed (`row_offsets_last` is the final
/// `_row_offsets` entry and a root node is present) that count is
/// authoritative; otherwise it is derived from the file `size`. Returns
/// `None` when neither is available — the Python code asserts in that case.
pub fn compute_total_pages_in_index(
    size: Option<u64>,
    root_present: bool,
    row_offsets_last: Option<usize>,
) -> Option<usize> {
    if root_present {
        if let Some(last) = row_offsets_last {
            return Some(last);
        }
    }
    size.map(|s| s.div_ceil(PAGE_SIZE as u64) as usize)
}

/// Decide which extra pages to prefetch alongside `offsets`. Pure port of
/// `_expand_offsets`: no IO and no cache access — the caller supplies
/// `cached` (which the Python suite obtains through the monkeypatchable
/// `_get_offsets_to_cached_pages`), so policy and the neighbor walk stay
/// testable in isolation.
///
/// The early-return branches deliberately echo `offsets` unchanged (and
/// unsorted): the Python implementation returns the original list object,
/// and tests assert on that exact ordering.
#[allow(clippy::too_many_arguments)]
pub fn expand_offsets(
    offsets: &[usize],
    recommended_pages: usize,
    size: Option<u64>,
    total_pages: usize,
    cached: &HashSet<usize>,
    root_present: bool,
    row_lengths_len: usize,
    row_offsets: &[usize],
) -> Vec<usize> {
    if offsets.len() >= recommended_pages {
        return offsets.to_vec();
    }
    if size.is_none() {
        return offsets.to_vec();
    }
    if total_pages.saturating_sub(cached.len()) <= recommended_pages {
        // Read whatever is left.
        let mut expanded: Vec<usize> = (0..total_pages).filter(|p| !cached.contains(p)).collect();
        expanded.sort_unstable();
        return expanded;
    }
    if !root_present {
        // First read of the root: don't pre-fetch yet.
        return offsets.to_vec();
    }
    let tree_depth = row_lengths_len;
    if cached.len() < tree_depth && offsets.len() == 1 {
        return offsets.to_vec();
    }
    let mut final_offsets: Vec<usize> =
        expand_to_neighbors(offsets, cached, total_pages, recommended_pages, row_offsets)
            .into_iter()
            .collect();
    final_offsets.sort_unstable();
    final_offsets
}

/// Grow `offsets` to neighboring pages within the same tree layer until at
/// least `recommended_pages` are requested. Pure port of
/// `_expand_to_neighbors`; the returned set is unsorted (the caller sorts).
pub fn expand_to_neighbors(
    offsets: &[usize],
    cached: &HashSet<usize>,
    total_pages: usize,
    recommended_pages: usize,
    row_offsets: &[usize],
) -> HashSet<usize> {
    let mut final_offsets: HashSet<usize> = offsets.iter().copied().collect();
    let mut new_tips = final_offsets.clone();
    // Python caches (first, end) for the first tip and reuses it for the
    // whole walk — every offset is assumed to be in the same layer.
    let mut layer: Option<(usize, usize)> = None;
    while final_offsets.len() < recommended_pages && !new_tips.is_empty() {
        let mut next_tips: HashSet<usize> = HashSet::new();
        for &pos in &new_tips {
            if layer.is_none() {
                layer = Some(find_layer_first_and_end(row_offsets, pos));
            }
            let (first, end) = layer.unwrap();
            // Note the strict `previous > 0`: page 0 (the root) is never
            // pulled in as a neighbor.
            if pos > 0 {
                let previous = pos - 1;
                if previous > 0
                    && !cached.contains(&previous)
                    && !final_offsets.contains(&previous)
                    && previous >= first
                {
                    next_tips.insert(previous);
                }
            }
            let after = pos + 1;
            if after < total_pages
                && !cached.contains(&after)
                && !final_offsets.contains(&after)
                && after < end
            {
                next_tips.insert(after);
            }
        }
        for n in &next_tips {
            final_offsets.insert(*n);
        }
        new_tips = next_tips;
    }
    final_offsets
}

/// One byte range to read from the backing file: `offset` bytes in, for
/// `length` bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageRange {
    pub offset: u64,
    pub length: u64,
}

/// Plan produced by [`plan_page_reads`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadPlan {
    /// Read these specific byte ranges from the file.
    Ranges(Vec<PageRange>),
    /// The file size is unknown and page 0 was requested: read the whole
    /// file in one `get_bytes` and slice it into pages afterwards.
    WholeFile,
}

/// Compute the byte ranges to read for the requested `pages`. Pure port of
/// the range-building loop in `_read_nodes` (the IO, zlib and node
/// construction stay in the caller).
///
/// Returns [`ReadPlan::WholeFile`] when page 0 is requested but `size` is
/// unknown. Errors (with a message mirroring the Python `AssertionError`)
/// when a non-root page lies past the end of the file. `page_size` is a
/// parameter because the test suite monkeypatches `_PAGE_SIZE`.
pub fn plan_page_reads(
    pages: &[usize],
    size: Option<u64>,
    base_offset: u64,
    page_size: usize,
) -> Result<ReadPlan, String> {
    let mut ranges: Vec<PageRange> = Vec::new();
    for &index in pages {
        let offset = (index as u64) * page_size as u64;
        let mut length = page_size as u64;
        if index == 0 {
            match size {
                Some(file_size) => {
                    length = (page_size as u64).min(file_size);
                }
                None => {
                    // Unknown size: signal a whole-file read.
                    return Ok(ReadPlan::WholeFile);
                }
            }
        } else {
            let file_size = size.unwrap_or(0);
            if offset > file_size {
                return Err(format!(
                    "tried to read past the end of the file {offset} > {file_size}"
                ));
            }
            length = length.min(file_size - offset);
        }
        ranges.push(PageRange {
            offset: base_offset + offset,
            length,
        });
    }
    Ok(ReadPlan::Ranges(ranges))
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

    // ---- Ports of TestMultiBisectRight from
    // bzrformats/tests/test_btree_index.py. The Python tests use bare
    // strings; we wrap them as one-segment LeafKeys for parity.

    fn k(s: &str) -> LeafKey {
        vec![s.as_bytes().to_vec()]
    }

    fn ks(slice: &[&str]) -> Vec<LeafKey> {
        slice.iter().map(|s| k(s)).collect()
    }

    fn assert_multi_bisect_right(expected: &[(usize, &[&str])], search: &[&str], fixed: &[&str]) {
        let got = multi_bisect_right(&ks(search), &ks(fixed));
        let want: Vec<(usize, Vec<LeafKey>)> = expected
            .iter()
            .map(|(pos, keys)| (*pos, ks(keys)))
            .collect();
        assert_eq!(got, want);
    }

    #[test]
    fn multi_bisect_right_after() {
        assert_multi_bisect_right(&[(1, &["b"])], &["b"], &["a"]);
        assert_multi_bisect_right(&[(3, &["e", "f", "g"])], &["e", "f", "g"], &["a", "b", "c"]);
    }

    #[test]
    fn multi_bisect_right_before() {
        assert_multi_bisect_right(&[(0, &["a"])], &["a"], &["b"]);
        assert_multi_bisect_right(
            &[(0, &["a", "b", "c", "d"])],
            &["a", "b", "c", "d"],
            &["e", "f", "g"],
        );
    }

    #[test]
    fn multi_bisect_right_exact() {
        assert_multi_bisect_right(&[(1, &["a"])], &["a"], &["a"]);
        assert_multi_bisect_right(&[(1, &["a"]), (2, &["b"])], &["a", "b"], &["a", "b"]);
        assert_multi_bisect_right(&[(1, &["a"]), (3, &["c"])], &["a", "c"], &["a", "b", "c"]);
    }

    #[test]
    fn multi_bisect_right_inbetween() {
        assert_multi_bisect_right(&[(1, &["b"])], &["b"], &["a", "c"]);
        assert_multi_bisect_right(
            &[(1, &["b", "c", "d"]), (2, &["f", "g"])],
            &["b", "c", "d", "f", "g"],
            &["a", "e", "h"],
        );
    }

    #[test]
    fn multi_bisect_right_mixed() {
        assert_multi_bisect_right(
            &[(0, &["a", "b"]), (2, &["d", "e"]), (4, &["g", "h"])],
            &["a", "b", "d", "e", "g", "h"],
            &["c", "d", "f", "g"],
        );
    }

    // ---- Ports of TestBTreeNodes (LeafNode_1_0, LeafNode_2_2,
    // InternalNode_1) from bzrformats/tests/test_btree_index.py.

    #[test]
    fn leaf_node_1_0_canned_bytes() {
        let body: &[u8] = b"type=leaf\n\
            0000000000000000000000000000000000000000\x00\x00value:0\n\
            1111111111111111111111111111111111111111\x00\x00value:1\n\
            2222222222222222222222222222222222222222\x00\x00value:2\n\
            3333333333333333333333333333333333333333\x00\x00value:3\n\
            4444444444444444444444444444444444444444\x00\x00value:4\n";
        let node = LeafNode::parse(body, 1, 0).unwrap();
        assert_eq!(node.len(), 5);
        for i in 0..5 {
            let key = vec![format!("{}", i).repeat(40).into_bytes()];
            let (value, refs) = node.get(&key).expect("key present");
            assert_eq!(value, &format!("value:{}", i).into_bytes());
            assert!(refs.is_empty());
        }
    }

    #[test]
    fn leaf_node_2_2_canned_bytes_with_refs() {
        let body: &[u8] = b"type=leaf\n\
            00\x0000\x00\t00\x00ref00\x00value:0\n\
            00\x0011\x0000\x00ref00\t00\x00ref00\r01\x00ref01\x00value:1\n\
            11\x0033\x0011\x00ref22\t11\x00ref22\r11\x00ref22\x00value:3\n\
            11\x0044\x00\t11\x00ref00\x00value:4\n";
        let node = LeafNode::parse(body, 2, 2).unwrap();
        assert_eq!(node.len(), 4);

        // (00, 00) -> value:0, refs=([], [(00, ref00)])
        let k = vec![b"00".to_vec(), b"00".to_vec()];
        let (v, refs) = node.get(&k).unwrap();
        assert_eq!(v, b"value:0");
        assert_eq!(refs.len(), 2);
        assert!(refs[0].is_empty());
        assert_eq!(refs[1], vec![vec![b"00".to_vec(), b"ref00".to_vec()]]);

        // (00, 11) -> value:1, refs=([(00, ref00)], [(00, ref00), (01, ref01)])
        let k = vec![b"00".to_vec(), b"11".to_vec()];
        let (v, refs) = node.get(&k).unwrap();
        assert_eq!(v, b"value:1");
        assert_eq!(refs[0], vec![vec![b"00".to_vec(), b"ref00".to_vec()]]);
        assert_eq!(
            refs[1],
            vec![
                vec![b"00".to_vec(), b"ref00".to_vec()],
                vec![b"01".to_vec(), b"ref01".to_vec()],
            ]
        );

        // (11, 33) -> value:3, refs=([(11, ref22)], [(11, ref22), (11, ref22)])
        let k = vec![b"11".to_vec(), b"33".to_vec()];
        let (v, refs) = node.get(&k).unwrap();
        assert_eq!(v, b"value:3");
        assert_eq!(refs[0], vec![vec![b"11".to_vec(), b"ref22".to_vec()]]);
        assert_eq!(
            refs[1],
            vec![
                vec![b"11".to_vec(), b"ref22".to_vec()],
                vec![b"11".to_vec(), b"ref22".to_vec()],
            ]
        );

        // (11, 44) -> value:4, refs=([], [(11, ref00)])
        let k = vec![b"11".to_vec(), b"44".to_vec()];
        let (v, refs) = node.get(&k).unwrap();
        assert_eq!(v, b"value:4");
        assert!(refs[0].is_empty());
        assert_eq!(refs[1], vec![vec![b"11".to_vec(), b"ref00".to_vec()]]);
    }

    #[test]
    fn internal_node_1_canned_bytes() {
        let body: &[u8] = b"type=internal\n\
            offset=1\n\
            0000000000000000000000000000000000000000\n\
            1111111111111111111111111111111111111111\n\
            2222222222222222222222222222222222222222\n\
            3333333333333333333333333333333333333333\n\
            4444444444444444444444444444444444444444\n";
        let node = parse_internal_node(body).unwrap();
        assert_eq!(node.offset, 1);
        let expected: Vec<LeafKey> = (0..5)
            .map(|i| vec![format!("{}", i).repeat(40).into_bytes()])
            .collect();
        assert_eq!(node.keys, expected);
    }

    // Prefetch heuristic ports of the Python `TestExpandOffsets` fixture.
    // The Python tests poke private attributes on a `BTreeGraphIndex`
    // (`_size`, `_recommended_pages`, `_row_lengths`, and a monkeypatched
    // `_get_offsets_to_cached_pages`) to drive the expansion logic without
    // touching disk. Here we call the pure free functions directly with the
    // same synthetic state.

    fn cached(set: &[usize]) -> HashSet<usize> {
        set.iter().copied().collect()
    }

    /// Synthetic index parameters mirroring `TestExpandOffsets.prepare_index`.
    struct SynthIndex {
        size: Option<u64>,
        recommended_pages: usize,
        row_lengths: Vec<usize>,
        row_offsets: Vec<usize>,
        with_root: bool,
    }

    impl SynthIndex {
        fn new(
            size: Option<u64>,
            recommended_pages: usize,
            row_lengths: Vec<usize>,
            with_root: bool,
        ) -> Self {
            let row_offsets = compute_row_offsets(&row_lengths);
            Self {
                size,
                recommended_pages,
                row_lengths,
                row_offsets,
                with_root,
            }
        }

        fn total_pages(&self) -> usize {
            compute_total_pages_in_index(
                self.size,
                self.with_root,
                self.row_offsets.last().copied(),
            )
            .expect("size or header present")
        }

        fn expand(&self, offsets: &[usize], cached_set: &[usize]) -> Vec<usize> {
            expand_offsets(
                offsets,
                self.recommended_pages,
                self.size,
                self.total_pages(),
                &cached(cached_set),
                self.with_root,
                self.row_lengths.len(),
                &self.row_offsets,
            )
        }
    }

    fn make_100_node_index() -> SynthIndex {
        SynthIndex::new(Some(4096 * 100), 6, vec![1, 99], true)
    }

    fn make_1000_node_index() -> SynthIndex {
        SynthIndex::new(Some(4096 * 1000), 6, vec![1, 9, 990], true)
    }

    #[test]
    fn recommended_pages_basic() {
        // The local transport recommends 4096 bytes => 1 page.
        assert_eq!(recommended_pages_for_read(4096), 1);
        assert_eq!(recommended_pages_for_read(64 * 1024), 16);
    }

    #[test]
    fn compute_total_pages_no_header() {
        // No header parsed yet: count is round_up(size / PAGE_SIZE).
        for (size, expected) in [
            (1024u64, 1usize),
            (4095, 1),
            (4096, 1),
            (4097, 2),
            (8192, 2),
            (4096 * 75 + 10, 76),
        ] {
            assert_eq!(
                compute_total_pages_in_index(Some(size), false, None),
                Some(expected)
            );
        }
    }

    #[test]
    fn compute_total_pages_unknown_returns_none() {
        assert_eq!(compute_total_pages_in_index(None, false, None), None);
    }

    #[test]
    fn find_layer_first_and_end_three_layers() {
        // row_lengths [1, 9, 990] => row_offsets [0, 1, 10, 1000].
        let idx = make_1000_node_index();
        let ro = &idx.row_offsets;
        assert_eq!(find_layer_first_and_end(ro, 0), (0, 1));
        assert_eq!(find_layer_first_and_end(ro, 1), (1, 10));
        assert_eq!(find_layer_first_and_end(ro, 9), (1, 10));
        assert_eq!(find_layer_first_and_end(ro, 10), (10, 1000));
        assert_eq!(find_layer_first_and_end(ro, 99), (10, 1000));
        assert_eq!(find_layer_first_and_end(ro, 999), (10, 1000));
    }

    #[test]
    fn expand_unknown_size() {
        // No size: never expand (offsets echoed unchanged). The size-None
        // branch returns before total_pages matters, so pass a dummy 0.
        let row_offsets = compute_row_offsets(&[1]);
        assert_eq!(
            expand_offsets(&[0], 10, None, 0, &cached(&[]), false, 1, &row_offsets),
            vec![0]
        );
        assert_eq!(
            expand_offsets(
                &[1, 4, 9],
                10,
                None,
                0,
                &cached(&[]),
                false,
                1,
                &row_offsets
            ),
            vec![1, 4, 9]
        );
    }

    #[test]
    fn expand_more_than_recommended() {
        // Already requesting >= recommended pages: echo unchanged.
        let idx = SynthIndex::new(Some(4096 * 100), 2, vec![1, 99], true);
        assert_eq!(idx.expand(&[1, 10], &[]), vec![1, 10]);
        assert_eq!(idx.expand(&[1, 10, 20], &[]), vec![1, 10, 20]);
    }

    #[test]
    fn expand_read_all_from_root() {
        // recommended=20 covers all 10 pages, so [0] expands to 0..10.
        let idx = SynthIndex::new(Some(4096 * 10), 20, vec![1, 9], false);
        assert_eq!(idx.expand(&[0], &[]), (0..10).collect::<Vec<_>>());
    }

    #[test]
    fn expand_read_all_when_cached() {
        // Cached [0,1,2,5,6]; 5 uncached, recommended=5 => read all the rest.
        let idx = SynthIndex::new(Some(4096 * 10), 5, vec![1, 9], true);
        let cset: &[usize] = &[0, 1, 2, 5, 6];
        assert_eq!(idx.expand(&[3], cset), vec![3, 4, 7, 8, 9]);
        assert_eq!(idx.expand(&[8], cset), vec![3, 4, 7, 8, 9]);
        assert_eq!(idx.expand(&[9], cset), vec![3, 4, 7, 8, 9]);
    }

    #[test]
    fn expand_no_root_node() {
        // First read, no root yet: don't expand.
        let idx = SynthIndex::new(Some(4096 * 10), 5, vec![1, 9], false);
        assert_eq!(idx.expand(&[0], &[]), vec![0]);
    }

    #[test]
    fn expand_include_neighbors() {
        let idx = make_100_node_index();
        let cset: &[usize] = &[0, 50];
        assert_eq!(idx.expand(&[12], cset), vec![9, 10, 11, 12, 13, 14, 15]);
        assert_eq!(idx.expand(&[91], cset), vec![88, 89, 90, 91, 92, 93, 94]);
        // Hitting the layer's edge: keep going the other direction. Page 0
        // is never pulled in (strict `previous > 0`).
        assert_eq!(idx.expand(&[2], cset), vec![1, 2, 3, 4, 5, 6]);
        assert_eq!(idx.expand(&[98], cset), vec![94, 95, 96, 97, 98, 99]);
        // Multiple offsets expand around each.
        assert_eq!(idx.expand(&[2, 81], cset), vec![1, 2, 3, 80, 81, 82]);
        assert_eq!(
            idx.expand(&[2, 10, 81], cset),
            vec![1, 2, 3, 9, 10, 11, 80, 81, 82]
        );
    }

    #[test]
    fn expand_stop_at_cached() {
        let idx = make_100_node_index();
        let cset: &[usize] = &[0, 10, 19];
        assert_eq!(idx.expand(&[11], cset), vec![11, 12, 13, 14, 15, 16]);
        assert_eq!(idx.expand(&[12], cset), vec![11, 12, 13, 14, 15, 16]);
        assert_eq!(idx.expand(&[15], cset), vec![12, 13, 14, 15, 16, 17, 18]);
        assert_eq!(idx.expand(&[16], cset), vec![13, 14, 15, 16, 17, 18]);
        assert_eq!(idx.expand(&[17], cset), vec![13, 14, 15, 16, 17, 18]);
        assert_eq!(idx.expand(&[18], cset), vec![13, 14, 15, 16, 17, 18]);
    }

    #[test]
    fn expand_cannot_fully_expand() {
        // Bound by cached neighbours on both sides: no infinite loop.
        let idx = make_100_node_index();
        assert_eq!(idx.expand(&[11], &[0, 10, 12]), vec![11]);
    }

    #[test]
    fn expand_overlap() {
        let idx = make_100_node_index();
        let cset: &[usize] = &[0, 50];
        assert_eq!(idx.expand(&[12, 13], cset), vec![10, 11, 12, 13, 14, 15]);
        assert_eq!(idx.expand(&[11, 14], cset), vec![10, 11, 12, 13, 14, 15]);
    }

    #[test]
    fn expand_stay_within_layer() {
        let idx = make_1000_node_index();
        let cset: &[usize] = &[0, 5, 500];
        assert_eq!(idx.expand(&[2], cset), vec![1, 2, 3, 4]);
        assert_eq!(idx.expand(&[6], cset), vec![6, 7, 8, 9]);
        assert_eq!(idx.expand(&[9], cset), vec![6, 7, 8, 9]);
        assert_eq!(idx.expand(&[10], cset), vec![10, 11, 12, 13, 14, 15]);
        assert_eq!(idx.expand(&[13], cset), vec![10, 11, 12, 13, 14, 15, 16]);

        let cset2: &[usize] = &[0, 4, 12];
        assert_eq!(idx.expand(&[7], cset2), vec![5, 6, 7, 8, 9]);
        assert_eq!(idx.expand(&[11], cset2), vec![10, 11]);
    }

    #[test]
    fn expand_small_requests_unexpanded() {
        // Single-page requests in a deep tree don't expand on the first pass.
        let idx = make_100_node_index();
        let cset: &[usize] = &[0];
        assert_eq!(idx.expand(&[1], cset), vec![1]);
        assert_eq!(idx.expand(&[50], cset), vec![50]);
        // But >1 offset expands around each.
        assert_eq!(idx.expand(&[50, 60], cset), vec![49, 50, 51, 59, 60, 61]);

        let idx = make_1000_node_index();
        assert_eq!(idx.expand(&[1], &[0]), vec![1]);
        assert_eq!(idx.expand(&[100], &[0, 1]), vec![100]);
        assert_eq!(idx.expand(&[2], &[0, 1, 100]), vec![2, 3, 4, 5, 6, 7]);
        assert_eq!(idx.expand(&[4], &[0, 1, 100]), vec![2, 3, 4, 5, 6, 7]);
        assert_eq!(
            idx.expand(&[105], &[0, 1, 2, 3, 4, 5, 6, 7, 100]),
            vec![102, 103, 104, 105, 106, 107, 108]
        );
    }

    #[test]
    fn plan_page_reads_unknown_size_root() {
        // Page 0 with no size => read the whole file.
        assert_eq!(
            plan_page_reads(&[0], None, 0, PAGE_SIZE),
            Ok(ReadPlan::WholeFile)
        );
    }

    #[test]
    fn plan_page_reads_known_size() {
        // Root page clamps to the file size; later pages are full PAGE_SIZE.
        let plan = plan_page_reads(&[0, 1], Some(PAGE_SIZE as u64 + 10), 0, PAGE_SIZE).unwrap();
        assert_eq!(
            plan,
            ReadPlan::Ranges(vec![
                PageRange {
                    offset: 0,
                    length: PAGE_SIZE as u64,
                },
                PageRange {
                    offset: PAGE_SIZE as u64,
                    length: 10,
                },
            ])
        );
    }

    #[test]
    fn plan_page_reads_applies_base_offset() {
        let plan = plan_page_reads(&[0], Some(100), 1234, PAGE_SIZE).unwrap();
        assert_eq!(
            plan,
            ReadPlan::Ranges(vec![PageRange {
                offset: 1234,
                length: 100,
            }])
        );
    }

    #[test]
    fn plan_page_reads_past_end_errors() {
        let err = plan_page_reads(&[5], Some(PAGE_SIZE as u64), 0, PAGE_SIZE).unwrap_err();
        assert!(err.contains("past the end"));
    }
}
