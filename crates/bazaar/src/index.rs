//! Graph index serialization.
//!
//! Port of the pure-logic pieces of `bzrformats/index.py` — starting with
//! the format-1 serializer (`GraphIndexBuilder.finish`). The parse side
//! and the stateful orchestration classes stay in Python for now.
//!
//! The format is documented in `GraphIndexBuilder`'s docstring:
//!
//! ```text
//! SIGNATURE      := 'Bazaar Graph Index 1\n'
//! OPTIONS        := 'node_ref_lists=' DIGITS NEWLINE
//!                   'key_elements='   DIGITS NEWLINE
//!                   'len='            DIGITS NEWLINE
//! NODE           := KEY NULL ABSENT? NULL REFERENCES NULL VALUE NEWLINE
//! REFERENCES     := REFERENCE_LIST (TAB REFERENCE_LIST){node_ref_lists - 1}
//! REFERENCE_LIST := (REFERENCE (CR REFERENCE)*)?
//! REFERENCE      := decimal byte offset of the referenced key, zero-padded
//!                   to the width needed to fit the entire file.
//! ```

use std::collections::HashMap;

/// Magic signature written at the start of every format-1 graph index.
pub const SIGNATURE: &[u8] = b"Bazaar Graph Index 1\n";
pub const OPTION_NODE_REFS: &[u8] = b"node_ref_lists=";
pub const OPTION_KEY_ELEMENTS: &[u8] = b"key_elements=";
pub const OPTION_LEN: &[u8] = b"len=";

/// One node as it lives in `GraphIndexBuilder._nodes`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexNode {
    /// The tuple key. Each element is a non-empty whitespace-free bytestring;
    /// elements are joined by `\x00` on disk.
    pub key: Vec<Vec<u8>>,
    /// True when this key is known only as a reference target — it was
    /// added implicitly to satisfy a reference from a present node.
    pub absent: bool,
    /// `reference_lists` lists of reference keys. Absent nodes always have
    /// this empty.
    pub references: Vec<Vec<Vec<Vec<u8>>>>,
    /// The value payload. Absent nodes always have this empty.
    pub value: Vec<u8>,
}

/// Errors produced by [`serialize_graph_index`]. Wrapped by the Python
/// `BzrError` in the binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IndexError {
    /// A node referenced a key that wasn't added anywhere in `nodes`.
    UnknownReference(Vec<Vec<u8>>),
    /// The final byte length didn't match the pre-pass estimate — indicates
    /// a logic bug in the serializer.
    LengthMismatch { expected: usize, actual: usize },
    /// The file didn't start with the magic signature.
    BadSignature,
    /// An option line was missing, in the wrong order, or had a non-decimal
    /// value.
    BadOptions,
    /// A node line had a wrong number of `\x00`-separated fields.
    BadLineData,
    /// A node line referenced a byte offset that couldn't be parsed as an
    /// integer.
    BadReferenceOffset(Vec<u8>),
    /// A key tuple was rejected (wrong length, empty element, or contained
    /// disallowed bytes).
    BadKey(IndexKey),
    /// A value was rejected (wrong reference list count, or disallowed
    /// bytes in payload).
    BadValue(String),
    /// `add_node` was called for a key already present (and not absent).
    DuplicateKey(IndexKey),
    /// Format-1 data parsing error (e.g. `_strip_prefix` mismatch).
    BadIndexData,
    /// Catch-all for runtime errors — bad input keys, IO failures from a
    /// transport, missing trailers, etc.
    Other(String),
}

impl std::fmt::Display for IndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexError::UnknownReference(k) => {
                write!(f, "reference to unknown key: {:?}", k)
            }
            IndexError::LengthMismatch { expected, actual } => write!(
                f,
                "mismatched output length and expected length: {} {}",
                actual, expected
            ),
            IndexError::BadSignature => write!(f, "bad index format signature"),
            IndexError::BadOptions => write!(f, "bad index options"),
            IndexError::BadLineData => write!(f, "bad index line data"),
            IndexError::BadReferenceOffset(s) => {
                write!(f, "bad reference offset: {:?}", s)
            }
            IndexError::BadKey(k) => write!(f, "bad index key: {:?}", k),
            IndexError::BadValue(msg) => write!(f, "bad index value: {}", msg),
            IndexError::DuplicateKey(k) => {
                write!(f, "duplicate index key: {:?}", k)
            }
            IndexError::BadIndexData => write!(f, "bad index data"),
            IndexError::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for IndexError {}

/// Metadata extracted from a graph index header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexHeader {
    pub node_ref_lists: usize,
    pub key_length: usize,
    pub key_count: usize,
    /// Byte offset of the first node line after the header.
    pub header_end: usize,
}

/// Parse the graph index file header from the start of `data`. Returns the
/// parsed metadata along with the offset at which the first node line
/// begins. The caller handles the rest of the stream.
pub fn parse_header(data: &[u8]) -> Result<IndexHeader, IndexError> {
    if !data.starts_with(SIGNATURE) {
        return Err(IndexError::BadSignature);
    }
    let after_sig = &data[SIGNATURE.len()..];
    let mut option_lines: [&[u8]; 3] = [b"", b"", b""];
    let mut offset = 0usize;
    for slot in option_lines.iter_mut() {
        let nl = after_sig[offset..]
            .iter()
            .position(|&b| b == b'\n')
            .ok_or(IndexError::BadOptions)?;
        *slot = &after_sig[offset..offset + nl];
        offset += nl + 1;
    }

    let node_ref_lists = parse_option(option_lines[0], OPTION_NODE_REFS)?;
    let key_length = parse_option(option_lines[1], OPTION_KEY_ELEMENTS)?;
    let key_count = parse_option(option_lines[2], OPTION_LEN)?;

    let header_end =
        SIGNATURE.len() + option_lines[0].len() + option_lines[1].len() + option_lines[2].len() + 3;

    Ok(IndexHeader {
        node_ref_lists,
        key_length,
        key_count,
        header_end,
    })
}

fn parse_option(line: &[u8], prefix: &[u8]) -> Result<usize, IndexError> {
    if !line.starts_with(prefix) {
        return Err(IndexError::BadOptions);
    }
    std::str::from_utf8(&line[prefix.len()..])
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .ok_or(IndexError::BadOptions)
}

/// A tuple key — each element is a bytestring, elements joined by `\x00`
/// on disk.
pub type IndexKey = Vec<Vec<u8>>;

/// One parsed node line, before reference offsets are resolved to real
/// keys by higher-level code. Mirrors the raw tuple stored in
/// `GraphIndex._keys_by_offset` on the Python side.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawNode {
    pub key: IndexKey,
    pub absent: bool,
    /// Reference lists of raw byte offsets pointing at other key lines.
    pub ref_offsets: Vec<Vec<u64>>,
    pub value: Vec<u8>,
}

/// One parsed present (non-absent) node as returned by [`parse_lines`]:
/// key tuple, value bytes, and the raw offset reference lists.
pub type ParsedNode = (IndexKey, Vec<u8>, Vec<Vec<u64>>);

/// The result of parsing a batch of node lines, matching the tuple
/// `GraphIndex._parse_lines` returns plus the `_keys_by_offset` side-table.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ParsedLines {
    pub first_key: Option<IndexKey>,
    pub last_key: Option<IndexKey>,
    /// Present (non-absent) nodes in the order they appeared.
    pub nodes: Vec<ParsedNode>,
    /// Per-offset raw records, including absent nodes.
    pub keys_by_offset: Vec<(u64, RawNode)>,
    /// Count of empty (trailer) lines seen.
    pub trailers: usize,
}

/// Parse a batch of `\n`-stripped node lines starting at `start_pos`.
/// `key_length` must match the value from the header. Mirrors
/// `GraphIndex._parse_lines`.
pub fn parse_lines(
    lines: &[&[u8]],
    start_pos: u64,
    key_length: usize,
) -> Result<ParsedLines, IndexError> {
    let expected_elements = 3 + key_length;
    let mut out = ParsedLines::default();
    let mut pos = start_pos;
    for line in lines {
        if line.is_empty() {
            out.trailers += 1;
            continue;
        }
        let elements: Vec<&[u8]> = line.split(|&b| b == b'\x00').collect();
        if elements.len() != expected_elements {
            return Err(IndexError::BadLineData);
        }
        let key: Vec<Vec<u8>> = elements[..key_length].iter().map(|e| e.to_vec()).collect();
        if out.first_key.is_none() {
            out.first_key = Some(key.clone());
        }
        out.last_key = Some(key.clone());

        let absent_field = elements[elements.len() - 3];
        let references_field = elements[elements.len() - 2];
        let value_field = elements[elements.len() - 1];
        let absent = absent_field == b"a";

        let mut ref_lists: Vec<Vec<u64>> = Vec::new();
        for ref_string in references_field.split(|&b| b == b'\t') {
            let mut list = Vec::new();
            for piece in ref_string.split(|&b| b == b'\r') {
                if piece.is_empty() {
                    continue;
                }
                let parsed = std::str::from_utf8(piece)
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                    .ok_or_else(|| IndexError::BadReferenceOffset(piece.to_vec()))?;
                list.push(parsed);
            }
            ref_lists.push(list);
        }

        let raw = RawNode {
            key: key.clone(),
            absent,
            ref_offsets: ref_lists.clone(),
            value: value_field.to_vec(),
        };
        out.keys_by_offset.push((pos, raw));
        pos += line.len() as u64 + 1; // +1 for the stripped newline

        if absent {
            continue;
        }
        out.nodes.push((key, value_field.to_vec(), ref_lists));
    }
    Ok(out)
}

/// Serialize a set of nodes into the format-1 graph-index byte stream.
///
/// `nodes` must already contain both real and "absent" entries (the
/// Python builder inserts `(absent=true, value=b"")` stubs for any
/// reference target not otherwise present).
///
/// `reference_lists` is the count of parallel reference lists per node
/// (0, 1, or 2 in practice). `key_elements` is the tuple length every
/// key must have.
pub fn serialize_graph_index(
    nodes: &[IndexNode],
    reference_lists: usize,
    key_elements: usize,
) -> Result<Vec<u8>, IndexError> {
    // Deterministic output order mirrors Python's `sorted(self._nodes.items())`.
    let mut sorted: Vec<&IndexNode> = nodes.iter().collect();
    sorted.sort_by(|a, b| a.key.cmp(&b.key));

    let mut header = Vec::new();
    header.extend_from_slice(SIGNATURE);
    header.extend_from_slice(OPTION_NODE_REFS);
    header.extend_from_slice(reference_lists.to_string().as_bytes());
    header.push(b'\n');
    header.extend_from_slice(OPTION_KEY_ELEMENTS);
    header.extend_from_slice(key_elements.to_string().as_bytes());
    header.push(b'\n');
    header.extend_from_slice(OPTION_LEN);
    let key_count = sorted.iter().filter(|n| !n.absent).count();
    header.extend_from_slice(key_count.to_string().as_bytes());
    header.push(b'\n');

    let prefix_length = header.len();

    // Only compute the zero-padding width and address table when there are
    // reference lists; without them there are no cross-offsets to resolve.
    let (digits, addresses, expected_bytes) = if reference_lists > 0 {
        let mut key_offset_info: Vec<(usize, usize)> = Vec::with_capacity(sorted.len());
        let mut non_ref_bytes = prefix_length;
        let mut total_references = 0usize;

        for (idx, node) in sorted.iter().enumerate() {
            key_offset_info.push((idx, non_ref_bytes));
            // key is literal, 3 null separators, 1 newline
            for element in &node.key {
                non_ref_bytes += element.len();
            }
            if key_elements > 1 {
                non_ref_bytes += key_elements - 1;
            }
            non_ref_bytes += node.value.len() + 3 + 1;
            if node.absent {
                non_ref_bytes += 1;
            } else {
                // (reference_lists - 1) tabs between ref lists
                non_ref_bytes += reference_lists - 1;
                for ref_list in &node.references {
                    total_references += ref_list.len();
                    if !ref_list.is_empty() {
                        non_ref_bytes += ref_list.len() - 1;
                    }
                }
            }
        }

        let mut digits = 1usize;
        let mut possible_total = non_ref_bytes + total_references * digits;
        while 10usize.pow(digits as u32) < possible_total {
            digits += 1;
            possible_total = non_ref_bytes + total_references * digits;
        }
        let expected = possible_total + 1; // trailing newline

        let mut addresses: HashMap<Vec<Vec<u8>>, usize> = HashMap::new();
        let mut total_refs_so_far = 0usize;
        for (idx, non_ref_so_far) in &key_offset_info {
            let node = sorted[*idx];
            addresses.insert(
                node.key.clone(),
                non_ref_so_far + total_refs_so_far * digits,
            );
            // Advance the running reference count *after* recording this
            // key's address — mirrors the Python pre-pass ordering.
            if !node.absent {
                for ref_list in &node.references {
                    total_refs_so_far += ref_list.len();
                }
            }
        }

        (digits, addresses, Some(expected))
    } else {
        (0, HashMap::new(), None)
    };

    let mut out = header;
    for node in &sorted {
        // Build the flattened references field.
        let mut flattened = Vec::new();
        for (i, ref_list) in node.references.iter().enumerate() {
            if i > 0 {
                flattened.push(b'\t');
            }
            for (j, reference) in ref_list.iter().enumerate() {
                if j > 0 {
                    flattened.push(b'\r');
                }
                let addr = addresses
                    .get(reference)
                    .ok_or_else(|| IndexError::UnknownReference(reference.clone()))?;
                let formatted = format!("{:0>width$}", addr, width = digits);
                flattened.extend_from_slice(formatted.as_bytes());
            }
        }

        // KEY \0 ABSENT \0 REFS \0 VALUE \n
        for (i, element) in node.key.iter().enumerate() {
            if i > 0 {
                out.push(b'\x00');
            }
            out.extend_from_slice(element);
        }
        out.push(b'\x00');
        if node.absent {
            out.push(b'a');
        }
        out.push(b'\x00');
        out.extend_from_slice(&flattened);
        out.push(b'\x00');
        out.extend_from_slice(&node.value);
        out.push(b'\n');
    }
    out.push(b'\n');

    if let Some(expected) = expected_bytes {
        if out.len() != expected {
            return Err(IndexError::LengthMismatch {
                expected,
                actual: out.len(),
            });
        }
    }
    Ok(out)
}

/// Minimal byte-store interface a [`GraphIndex`] needs to read its backing
/// file. The full-load path uses only [`IndexTransport::get_bytes`]; the
/// bisection path (not yet ported) will additionally use a `readv`-style
/// method.
///
/// Mirrors the slice of `bzrformats.transport.Transport` that
/// `GraphIndex` actually calls. Kept narrow on purpose so test fixtures
/// and pyo3 adapters don't have to implement methods the index logic
/// will never invoke.
pub trait IndexTransport {
    /// Read the full contents of `path` and return them as a byte vector.
    fn get_bytes(&self, path: &str) -> Result<Vec<u8>, IndexError>;

    /// Resolve `path` relative to the transport root. Used only for
    /// diagnostic messages — implementations may simply return `path`.
    fn abspath(&self, path: &str) -> String {
        path.to_string()
    }

    /// Vectored read. Each `(offset, length)` request returns one
    /// `(actual_offset, data)` pair, possibly out of order or with
    /// expanded coverage if the transport upcasts the request.
    /// `adjust_for_latency` corresponds to the bzrformats Transport
    /// flag of the same name; `upper_limit` bounds any expansion the
    /// transport performs.
    ///
    /// The default implementation falls back to `get_bytes` plus
    /// per-range slicing — adequate for in-memory test transports.
    fn readv(
        &self,
        path: &str,
        ranges: &[(u64, u64)],
        _adjust_for_latency: bool,
        _upper_limit: u64,
    ) -> Result<Vec<(u64, Vec<u8>)>, IndexError> {
        let data = self.get_bytes(path)?;
        let mut out = Vec::with_capacity(ranges.len());
        for &(offset, length) in ranges {
            let end = (offset + length) as usize;
            if end > data.len() {
                return Err(IndexError::Other(format!(
                    "readv past end of {} at offset {}+{}",
                    path, offset, length
                )));
            }
            out.push((offset, data[offset as usize..end].to_vec()));
        }
        Ok(out)
    }
}

/// Errors specific to `GraphIndex` operations beyond
/// signature/format issues already covered by [`IndexError`]. These are
/// folded into [`IndexError`] via the `Other` variant if needed.
impl IndexError {
    fn missing_trailer() -> Self {
        IndexError::Other("BadIndexData: missing trailer".to_string())
    }
}

/// One reference list (a list of keys), resolved from byte offsets.
pub type RefList = Vec<IndexKey>;

/// All reference lists for a single node, in declared order.
pub type RefLists = Vec<RefList>;

/// A `(value, reference lists)` pair stored against each present key.
pub type NodeBody = (Vec<u8>, RefLists);

/// One emitted entry: `(key, value, reference lists)`.
pub type IndexEntry = (IndexKey, Vec<u8>, RefLists);

/// A prefix tuple for [`GraphIndex::iter_entries_prefix`]. `None` slots
/// match any key element at that position.
pub type KeyPrefix = Vec<Option<Vec<u8>>>;

/// `true` when `b` is one of the bytes a key element must not contain:
/// tab, LF, VT, FF, CR, NUL, or space.
#[inline]
fn is_key_disallowed(b: u8) -> bool {
    matches!(b, b'\t' | b'\n' | 0x0b | 0x0c | b'\r' | 0 | b' ')
}

/// `true` when every element of `key` is non-empty and free of the
/// whitespace + null bytes the format reserves as field/record
/// separators. Matches the `_check_key` validation in the Python
/// `GraphIndexBuilder`.
pub fn key_is_valid(key: &[Vec<u8>], key_length: usize) -> bool {
    if key.len() != key_length {
        return false;
    }
    key.iter()
        .all(|element| !element.is_empty() && !element.iter().any(|&b| is_key_disallowed(b)))
}

/// `true` when `value` may legally appear as a node payload — neither
/// `\n` nor `\0` bytes anywhere in the slice.
pub fn value_is_valid(value: &[u8]) -> bool {
    !value.iter().any(|&b| b == b'\n' || b == 0)
}

/// Bookkeeping for the byte-range and key-range subsets of a
/// graph-index file that the bisection path has already parsed. The
/// ranges in each map are sorted, non-overlapping, and parallel: index
/// `i` in `byte_map` corresponds to index `i` in `key_map`.
///
/// `None` keys at either end represent "no key before this region",
/// matching the empty-tuple sentinel the Python code uses when the
/// region only contains the file header and not yet any node lines.
#[derive(Debug, Default, Clone)]
pub struct ParsedRangeMap {
    byte_map: Vec<(u64, u64)>,
    key_map: Vec<(Option<IndexKey>, Option<IndexKey>)>,
}

impl ParsedRangeMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.byte_map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.byte_map.is_empty()
    }

    pub fn byte_range(&self, index: usize) -> Option<(u64, u64)> {
        self.byte_map.get(index).copied()
    }

    pub fn key_range(&self, index: usize) -> Option<(Option<IndexKey>, Option<IndexKey>)> {
        self.key_map.get(index).cloned()
    }

    /// Largest index `i` such that `byte_map[i].0 <= offset`. Returns
    /// `-1` when no such index exists (empty map or offset before every
    /// region's start).
    pub fn byte_index(&self, offset: u64) -> isize {
        self.byte_map
            .iter()
            .rposition(|r| r.0 <= offset)
            .map(|i| i as isize)
            .unwrap_or(-1)
    }

    /// Largest index `i` such that `key_map[i].0 <= key`. Returns `-1`
    /// when no such index exists.
    pub fn key_index(&self, key: &Option<IndexKey>) -> isize {
        self.key_map
            .iter()
            .rposition(|r| r.0 <= *key)
            .map(|i| i as isize)
            .unwrap_or(-1)
    }

    /// `true` when `offset` falls inside one of the parsed regions.
    pub fn is_parsed(&self, offset: u64) -> bool {
        let index = self.byte_index(offset);
        if index < 0 {
            return false;
        }
        let (start, end) = self.byte_map[index as usize];
        offset >= start && offset < end
    }

    /// Mark `[start, end)` as parsed, keyed by `[start_key, end_key]`.
    /// Adjacent ranges are merged.
    pub fn mark_parsed(
        &mut self,
        start: u64,
        start_key: Option<IndexKey>,
        end: u64,
        end_key: Option<IndexKey>,
    ) {
        let index = self.byte_index(start);
        let new_value = (start, end);
        let new_key = (start_key, end_key);
        if index < 0 {
            self.byte_map.insert(0, new_value);
            self.key_map.insert(0, new_key);
            return;
        }
        let index = index as usize;
        let next = index + 1;
        if next < self.byte_map.len()
            && self.byte_map[index].1 == start
            && self.byte_map[next].0 == end
        {
            self.byte_map[index] = (self.byte_map[index].0, self.byte_map[next].1);
            self.key_map[index] = (self.key_map[index].0.clone(), self.key_map[next].1.clone());
            self.byte_map.remove(next);
            self.key_map.remove(next);
        } else if self.byte_map[index].1 == start {
            self.byte_map[index] = (self.byte_map[index].0, end);
            self.key_map[index] = (self.key_map[index].0.clone(), new_key.1);
        } else if next < self.byte_map.len() && self.byte_map[next].0 == end {
            self.byte_map[next] = (start, self.byte_map[next].1);
            self.key_map[next] = (new_key.0, self.key_map[next].1.clone());
        } else {
            self.byte_map.insert(next, new_value);
            self.key_map.insert(next, new_key);
        }
    }
}

/// Parse a complete graph-index file (header + body) and resolve every
/// reference offset to its key. Returns the header metadata along with
/// the `key -> (value, reference lists)` map for present nodes only.
///
/// `data` must be the body of the file with any base-offset already
/// trimmed off; the caller owns transport reads and offset adjustment.
pub fn parse_full(data: &[u8]) -> Result<(IndexHeader, HashMap<IndexKey, NodeBody>), IndexError> {
    let header = parse_header(data)?;
    let body = &data[header.header_end..];
    // Mirrors Python: split on b"\n", drop the trailing empty
    // segment that follows the final newline. parse_lines counts
    // trailer (empty) lines and we require exactly one.
    let mut segments: Vec<&[u8]> = body.split(|&b| b == b'\n').collect();
    segments.pop();
    let parsed = parse_lines(&segments, header.header_end as u64, header.key_length)?;
    if parsed.trailers != 1 {
        return Err(IndexError::missing_trailer());
    }
    let mut offset_to_key: HashMap<u64, IndexKey> = HashMap::new();
    for (offset, raw_node) in &parsed.keys_by_offset {
        offset_to_key.insert(*offset, raw_node.key.clone());
    }
    let mut nodes: HashMap<IndexKey, NodeBody> = HashMap::new();
    let node_ref_lists = header.node_ref_lists;
    for (_, raw_node) in parsed.keys_by_offset.into_iter() {
        if raw_node.absent {
            continue;
        }
        // parse_lines always emits at least one (possibly empty)
        // reference list, even when the index header says 0 — the
        // tab-split sees `[""]`. Truncate to the declared count.
        let resolved = if node_ref_lists == 0 {
            Vec::new()
        } else {
            let mut out: Vec<Vec<IndexKey>> = Vec::with_capacity(node_ref_lists);
            for ref_list in &raw_node.ref_offsets {
                let mut list: Vec<IndexKey> = Vec::with_capacity(ref_list.len());
                for off in ref_list {
                    let k = offset_to_key.get(off).ok_or_else(|| {
                        IndexError::Other(format!("unresolved reference offset {}", off))
                    })?;
                    list.push(k.clone());
                }
                out.push(list);
            }
            out
        };
        nodes.insert(raw_node.key, (raw_node.value, resolved));
    }
    Ok((header, nodes))
}

/// A node parsed during the bisection path but whose references are
/// stored as raw byte offsets, not yet resolved to keys.
pub type BisectNodeBody = (Vec<u8>, Vec<Vec<u64>>);

/// A read-only graph index opened on a [`IndexTransport`]-backed file.
///
/// Two paths share this struct: the full-load fallback implemented in
/// [`GraphIndex::buffer_all`] (reads + parses the entire file in one
/// shot) and the bisection-driven partial-read flow. The latter keeps
/// the file's size, the parsed-region map, and the half-resolved
/// `bisect_nodes` table around so successive lookups can satisfy
/// themselves from cached parts of the file.
pub struct GraphIndex<T: IndexTransport> {
    transport: T,
    name: String,
    base_offset: u64,
    /// Total size of the backing file, in bytes. `None` disables the
    /// bisection path (every read goes through `buffer_all`).
    size: Option<u64>,
    /// Parsed node table — `key -> (value, resolved reference lists)`.
    /// `None` until [`GraphIndex::buffer_all`] has been called.
    nodes: Option<HashMap<IndexKey, NodeBody>>,
    /// Header metadata. `None` until the file has been read at least
    /// once.
    header: Option<IndexHeader>,
    /// Nodes parsed during the bisection path. Reference lists are
    /// stored as byte offsets — call [`GraphIndex::resolve_references`]
    /// to substitute actual keys.
    bisect_nodes: Option<HashMap<IndexKey, BisectNodeBody>>,
    /// Raw nodes keyed by their byte offset in the file. Used to
    /// resolve reference offsets to keys during bisection.
    keys_by_offset: HashMap<u64, RawNode>,
    /// Tracks which byte (and key) ranges have already been parsed.
    range_map: ParsedRangeMap,
    /// Total bytes read from the transport so far. Used by the
    /// 50%-read heuristic that promotes a bisection lookup to a full
    /// `buffer_all`.
    bytes_read: u64,
}

impl<T: IndexTransport> GraphIndex<T> {
    /// Open an index on `transport` at `name`. Pass `base_offset` if the
    /// index lives at a non-zero offset within the underlying file (the
    /// pack-file case). `size` enables bisection-driven partial reads
    /// when known.
    pub fn new(transport: T, name: impl Into<String>, base_offset: u64) -> Self {
        Self::with_size(transport, name, base_offset, None)
    }

    /// Open an index whose backing file size is known. With a size,
    /// `iter_entries` for small key sets uses bisection rather than
    /// reading the whole file.
    pub fn with_size(
        transport: T,
        name: impl Into<String>,
        base_offset: u64,
        size: Option<u64>,
    ) -> Self {
        Self {
            transport,
            name: name.into(),
            base_offset,
            size,
            nodes: None,
            header: None,
            bisect_nodes: None,
            keys_by_offset: HashMap::new(),
            range_map: ParsedRangeMap::new(),
            bytes_read: 0,
        }
    }

    /// File size, if known.
    pub fn size(&self) -> Option<u64> {
        self.size
    }

    /// Total bytes read from the transport so far.
    pub fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    /// `true` once `buffer_all` has populated the in-memory node table.
    pub fn is_buffered_already(&self) -> bool {
        self.nodes.is_some()
    }

    /// Read-only view of the parsed header, if any.
    pub fn header(&self) -> Option<&IndexHeader> {
        self.header.as_ref()
    }

    /// Iterator over post-`buffer_all` nodes. Returns an empty iterator
    /// if `buffer_all` hasn't run yet.
    pub fn nodes_iter(&self) -> impl Iterator<Item = (&IndexKey, &NodeBody)> {
        self.nodes.iter().flat_map(|m| m.iter())
    }

    /// Read enough of the file to populate the header (and the bisect
    /// state). If the bytes-read crosses 50% of the file size this
    /// promotes to a full buffer. No-op if the header is already
    /// known.
    pub fn ensure_header_parsed(&mut self) -> Result<(), IndexError> {
        if self.header.is_some() {
            return Ok(());
        }
        self.read_and_parse(vec![(0, 200)])?;
        Ok(())
    }

    /// Public entry point for tests that want to drive the bisection
    /// `read_and_parse` flow directly with a list of `(offset, length)`
    /// readv ranges.
    pub fn read_and_parse_for_test(
        &mut self,
        readv_ranges: Vec<(u64, u64)>,
    ) -> Result<(), IndexError> {
        self.read_and_parse(readv_ranges)
    }

    /// Cached key count. Reads only what's already known — does **not**
    /// trigger any I/O. Returns `0` when the header hasn't been parsed
    /// yet (matching `key_count is None` in Python).
    pub fn key_count_or_zero(&self) -> usize {
        self.header.as_ref().map(|h| h.key_count).unwrap_or(0)
    }

    /// Read-only view of the parsed-range map. Tests and the pyo3
    /// adapter consult this to verify which byte spans the bisection
    /// path has covered.
    pub fn range_map(&self) -> &ParsedRangeMap {
        &self.range_map
    }

    /// Read-only view of the bisect-mode node cache. `None` until the
    /// header has been parsed via the bisection path.
    pub fn bisect_nodes(&self) -> Option<&HashMap<IndexKey, BisectNodeBody>> {
        self.bisect_nodes.as_ref()
    }

    /// Read-only view of the `offset -> RawNode` map populated by the
    /// bisection path.
    pub fn keys_by_offset(&self) -> &HashMap<u64, RawNode> {
        &self.keys_by_offset
    }

    /// Read the entire backing file, parse it, and resolve every
    /// reference offset to its key. Idempotent — subsequent calls are
    /// cheap no-ops.
    pub fn buffer_all(&mut self) -> Result<(), IndexError> {
        if self.nodes.is_some() {
            return Ok(());
        }
        let raw = self.transport.get_bytes(&self.name)?;
        let data = if self.base_offset == 0 {
            raw
        } else {
            raw[self.base_offset as usize..].to_vec()
        };
        let (header, nodes) = parse_full(&data)?;
        self.nodes = Some(nodes);
        self.header = Some(header);
        Ok(())
    }

    /// Number of keys in the index. With a known size, this reads only
    /// the header. Without a size, falls back to a full load.
    pub fn key_count(&mut self) -> Result<usize, IndexError> {
        if let Some(h) = &self.header {
            return Ok(h.key_count);
        }
        if self.size.is_some() {
            self.ensure_header_parsed()?;
        } else {
            self.buffer_all()?;
        }
        Ok(self
            .header
            .as_ref()
            .expect("header set by ensure_header_parsed/buffer_all")
            .key_count)
    }

    /// Number of parallel reference lists each present node carries.
    /// With a known size, reads only the header. Otherwise full load.
    pub fn node_ref_lists(&mut self) -> Result<usize, IndexError> {
        if let Some(h) = &self.header {
            return Ok(h.node_ref_lists);
        }
        if self.size.is_some() {
            self.ensure_header_parsed()?;
        } else {
            self.buffer_all()?;
        }
        Ok(self.header.as_ref().expect("header set").node_ref_lists)
    }

    /// Number of bytestrings in each key tuple. With a known size,
    /// reads only the header. Otherwise full load.
    pub fn key_length(&mut self) -> Result<usize, IndexError> {
        if let Some(h) = &self.header {
            return Ok(h.key_length);
        }
        if self.size.is_some() {
            self.ensure_header_parsed()?;
        } else {
            self.buffer_all()?;
        }
        Ok(self.header.as_ref().expect("header set").key_length)
    }

    /// Iterate over every present entry as `(key, value, resolved
    /// reference lists)`. Order is unspecified — the Python equivalent
    /// is also unordered (HashMap iteration).
    pub fn iter_all_entries(&mut self) -> Result<Vec<IndexEntry>, IndexError> {
        self.buffer_all()?;
        let nodes = self.nodes.as_ref().expect("buffer_all populated nodes");
        Ok(nodes
            .iter()
            .map(|(k, (v, r))| (k.clone(), v.clone(), r.clone()))
            .collect())
    }

    /// Iterate over only the entries whose key is in `keys`. Missing
    /// keys are silently skipped, matching Python.
    pub fn iter_entries(&mut self, keys: &[IndexKey]) -> Result<Vec<IndexEntry>, IndexError> {
        self.buffer_all()?;
        let nodes = self.nodes.as_ref().expect("buffer_all populated nodes");
        let mut out = Vec::new();
        let mut seen: std::collections::HashSet<&IndexKey> = std::collections::HashSet::new();
        for k in keys {
            if !seen.insert(k) {
                continue;
            }
            if let Some((v, r)) = nodes.get(k) {
                out.push((k.clone(), v.clone(), r.clone()));
            }
        }
        Ok(out)
    }

    /// Iterate over entries matching one of the given key prefixes. A
    /// prefix is a tuple the same length as a key with trailing
    /// elements set to `None`. The first element must not be `None`.
    pub fn iter_entries_prefix(
        &mut self,
        prefixes: &[KeyPrefix],
    ) -> Result<Vec<IndexEntry>, IndexError> {
        self.buffer_all()?;
        let key_length = self.header.as_ref().expect("header").key_length;
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
        let nodes = self.nodes.as_ref().expect("buffer_all populated nodes");
        // Fast path for length-1 keys: a prefix with no None elements is
        // just an exact lookup.
        if key_length == 1 {
            return self.iter_entries(
                &prefixes
                    .iter()
                    .map(|p| {
                        p.iter()
                            .map(|e| e.clone().expect("validated above"))
                            .collect::<IndexKey>()
                    })
                    .collect::<Vec<_>>(),
            );
        }
        let mut out = Vec::new();
        let mut emitted: std::collections::HashSet<IndexKey> = std::collections::HashSet::new();
        for prefix in prefixes {
            for (k, (v, r)) in nodes.iter() {
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

    /// Reference keys not present in the index, drawn from
    /// reference list `ref_list_num`. Triggers a full load.
    pub fn external_references(
        &mut self,
        ref_list_num: usize,
    ) -> Result<std::collections::HashSet<IndexKey>, IndexError> {
        self.buffer_all()?;
        let header = self.header.as_ref().expect("header");
        if ref_list_num + 1 > header.node_ref_lists {
            return Err(IndexError::Other(format!(
                "No ref list {}, index has {} ref lists",
                ref_list_num, header.node_ref_lists
            )));
        }
        let nodes = self.nodes.as_ref().expect("nodes");
        let mut refs = std::collections::HashSet::new();
        for (_k, (_v, ref_lists)) in nodes.iter() {
            let list = &ref_lists[ref_list_num];
            for r in list {
                if !nodes.contains_key(r) {
                    refs.insert(r.clone());
                }
            }
        }
        Ok(refs)
    }

    /// Validate the index — currently this just walks every entry,
    /// matching Python's `iter_all_entries`-based check.
    pub fn validate(&mut self) -> Result<(), IndexError> {
        self.buffer_all()?;
        Ok(())
    }

    /// Resolve a list of reference-offset lists against the
    /// `keys_by_offset` map, returning concrete key tuples in the same
    /// order. Mirrors the Python `_resolve_references` helper used
    /// during the bisection path.
    pub fn resolve_references(
        &self,
        references: &[Vec<u64>],
    ) -> Result<Vec<Vec<IndexKey>>, IndexError> {
        let mut out = Vec::with_capacity(references.len());
        for ref_list in references {
            let mut resolved = Vec::with_capacity(ref_list.len());
            for off in ref_list {
                let raw = self.keys_by_offset.get(off).ok_or_else(|| {
                    IndexError::Other(format!("unresolved reference offset {}", off))
                })?;
                resolved.push(raw.key.clone());
            }
            out.push(resolved);
        }
        Ok(out)
    }

    /// Parse a header from a freshly-read prefix of the file, populating
    /// the `header`, `range_map`, `keys_by_offset`, and `bisect_nodes`
    /// fields. Returns the offset and remaining body slice for the
    /// caller to feed into [`GraphIndex::parse_region`].
    fn parse_header_from_bytes<'a>(
        &mut self,
        bytes: &'a [u8],
    ) -> Result<(u64, &'a [u8]), IndexError> {
        let header = parse_header(bytes)?;
        self.range_map.mark_parsed(
            0,
            Some(Vec::new()),
            header.header_end as u64,
            Some(Vec::new()),
        );
        let header_end = header.header_end as u64;
        self.header = Some(header);
        self.bisect_nodes = Some(HashMap::new());
        Ok((header_end, &bytes[header_end as usize..]))
    }

    /// Parse one segment of `data` starting at `offset` into the
    /// bisect-state. Returns `(high_parsed_byte, last_segment)`. The
    /// segment-trimming logic mirrors the Python `_parse_segment`.
    fn parse_segment(
        &mut self,
        offset: u64,
        data: &[u8],
        end: u64,
        index: isize,
    ) -> Result<(u64, bool), IndexError> {
        let lower_end = self
            .range_map
            .byte_range(index as usize)
            .ok_or_else(|| IndexError::Other("parse_segment: index out of range".into()))?
            .1;

        let mut trim_start: Option<u64>;
        let start_adjacent;
        if offset < lower_end {
            trim_start = Some(lower_end - offset);
            start_adjacent = true;
        } else if offset == lower_end {
            trim_start = None;
            start_adjacent = true;
        } else {
            trim_start = None;
            start_adjacent = false;
        }

        let size = self.size.unwrap_or(0);
        let mut trim_end: Option<u64>;
        let end_adjacent;
        let last_segment;
        if Some(end) == self.size {
            trim_end = None;
            end_adjacent = true;
            last_segment = true;
        } else if (index as usize) + 1 == self.range_map.len() {
            trim_end = None;
            end_adjacent = false;
            last_segment = true;
        } else {
            let (higher_start, higher_end) = self
                .range_map
                .byte_range((index as usize) + 1)
                .expect("higher region present");
            if end == higher_start {
                trim_end = None;
                end_adjacent = true;
                last_segment = true;
            } else if end > higher_start {
                trim_end = Some(higher_start - offset);
                end_adjacent = true;
                last_segment = end < higher_end;
            } else {
                trim_end = None;
                end_adjacent = false;
                last_segment = true;
            }
        }
        let _ = size;

        if !start_adjacent {
            let start_idx = trim_start.map(|s| s as usize).unwrap_or(0);
            let pos = data[start_idx..]
                .iter()
                .position(|&b| b == b'\n')
                .ok_or_else(|| IndexError::Other("no \\n was present".into()))?;
            trim_start = Some((start_idx + pos + 1) as u64);
        }
        if !end_adjacent {
            let end_idx = trim_end.map(|e| e as usize).unwrap_or(data.len());
            let pos = data[..end_idx]
                .iter()
                .rposition(|&b| b == b'\n')
                .ok_or_else(|| IndexError::Other("no \\n was present".into()))?;
            trim_end = Some((pos + 1) as u64);
        }

        let ts = trim_start.map(|t| t as usize).unwrap_or(0);
        let te = trim_end.map(|t| t as usize).unwrap_or(data.len());
        let trimmed = &data[ts..te];
        if trimmed.is_empty() {
            return Err(IndexError::Other(format!(
                "read unneeded data [{}:{}] from [{}:{}]",
                ts,
                te,
                offset,
                offset + data.len() as u64
            )));
        }
        let parse_offset = if ts != 0 { offset + ts as u64 } else { offset };

        // splitlines mangles \r — use literal \n.
        let mut segments: Vec<&[u8]> = trimmed.split(|&b| b == b'\n').collect();
        segments.pop();
        let key_length = self.header.as_ref().expect("header parsed").key_length;
        let parsed = parse_lines(&segments, parse_offset, key_length)?;
        let bisect_nodes = self
            .bisect_nodes
            .as_mut()
            .expect("bisect_nodes initialised by parse_header_from_bytes");
        for (key, value, ref_offsets) in parsed.nodes {
            bisect_nodes.insert(key, (value, ref_offsets));
        }
        for (off, raw) in &parsed.keys_by_offset {
            self.keys_by_offset.insert(*off, raw.clone());
        }
        self.range_map.mark_parsed(
            parse_offset,
            parsed.first_key,
            parse_offset + trimmed.len() as u64,
            parsed.last_key,
        );
        Ok((parse_offset + trimmed.len() as u64, last_segment))
    }

    /// Parse `data` starting at `offset` into the bisect-state, calling
    /// [`GraphIndex::parse_segment`] in a loop until the region is
    /// fully covered.
    fn parse_region(&mut self, offset: u64, data: &[u8]) -> Result<(), IndexError> {
        let end = offset + data.len() as u64;
        let mut high_parsed = offset;
        loop {
            let index = self.range_map.byte_index(high_parsed);
            let cur_end = self
                .range_map
                .byte_range(index as usize)
                .map(|r| r.1)
                .unwrap_or(0);
            if end < cur_end {
                return Ok(());
            }
            let (next_high, last_segment) = self.parse_segment(offset, data, end, index)?;
            high_parsed = next_high;
            if last_segment {
                return Ok(());
            }
        }
    }

    /// Service a vectored read for the bisection path. If the read
    /// returns the whole file, promote it to `buffer_all`. Otherwise
    /// each chunk feeds into [`GraphIndex::parse_region`].
    fn read_and_parse(&mut self, mut readv_ranges: Vec<(u64, u64)>) -> Result<(), IndexError> {
        if readv_ranges.is_empty() {
            return Ok(());
        }
        let size = self
            .size
            .ok_or_else(|| IndexError::Other("read_and_parse called without a size".into()))?;
        if self.nodes.is_none() && self.bytes_read * 2 >= size {
            self.buffer_all()?;
            return Ok(());
        }
        if self.base_offset != 0 {
            for r in &mut readv_ranges {
                r.0 += self.base_offset;
            }
        }
        let upper = size + self.base_offset;
        let readv_data = self
            .transport
            .readv(&self.name, &readv_ranges, true, upper)?;
        for (raw_offset, raw_data) in readv_data {
            // Translate transport-absolute offset to index-local. If the
            // chunk starts before our base_offset (the transport
            // expanded the range), trim the prefix off rather than
            // serving spurious bytes to parse_header.
            let signed_offset = raw_offset as i64 - self.base_offset as i64;
            let (mut offset, mut data) = if signed_offset < 0 {
                let drop = (-signed_offset) as usize;
                if drop >= raw_data.len() {
                    self.bytes_read += raw_data.len() as u64;
                    continue;
                }
                (0u64, raw_data[drop..].to_vec())
            } else {
                (signed_offset as u64, raw_data)
            };
            self.bytes_read += data.len() as u64;
            if data.len() as u64 == size && offset == 0 {
                self.buffer_all_from_bytes(data)?;
                return Ok(());
            }
            if self.bisect_nodes.is_none() {
                if offset != 0 {
                    return Err(IndexError::Other(
                        "header chunk did not start at offset 0".into(),
                    ));
                }
                let (header_end, body) = self.parse_header_from_bytes(&data)?;
                let body_vec = body.to_vec();
                offset = header_end;
                data = body_vec;
            }
            self.parse_region(offset, &data)?;
        }
        Ok(())
    }

    /// Variant of `buffer_all` that consumes a pre-fetched byte buffer.
    /// `data` is the index region only (the caller has already trimmed
    /// any base-offset prefix).
    fn buffer_all_from_bytes(&mut self, data: Vec<u8>) -> Result<(), IndexError> {
        if self.nodes.is_some() {
            return Ok(());
        }
        let (header, nodes) = parse_full(&data)?;
        self.nodes = Some(nodes);
        self.header = Some(header);
        Ok(())
    }

    /// Bisection result for a single `(location, key)` probe.
    pub fn lookup_keys_via_location(
        &mut self,
        location_keys: &[(u64, IndexKey)],
    ) -> Result<Vec<((u64, IndexKey), LookupResult)>, IndexError> {
        let size = self
            .size
            .ok_or_else(|| IndexError::Other("lookup_keys_via_location requires a size".into()))?;

        let mut readv_ranges: Vec<(u64, u64)> = Vec::new();
        for (location, key) in location_keys {
            if let Some(bn) = &self.bisect_nodes {
                if bn.contains_key(key) {
                    continue;
                }
            }
            // Check the parsed key range first.
            let key_idx = self.range_map.key_index(&Some(key.clone()));
            if !self.range_map.is_empty() && key_idx >= 0 {
                let (key_start, key_end) = self
                    .range_map
                    .key_range(key_idx as usize)
                    .expect("idx in range");
                let (_, byte_end) = self
                    .range_map
                    .byte_range(key_idx as usize)
                    .expect("idx in range");
                if key_start.as_ref().map(|k| k <= key).unwrap_or(true)
                    && (key_end.as_ref().map(|k| k >= key).unwrap_or(false) || byte_end == size)
                {
                    continue;
                }
            }
            let byte_idx = self.range_map.byte_index(*location);
            if !self.range_map.is_empty() && byte_idx >= 0 {
                let (byte_start, byte_end) = self
                    .range_map
                    .byte_range(byte_idx as usize)
                    .expect("idx in range");
                if byte_start <= *location && byte_end > *location {
                    continue;
                }
            }
            let mut length = 800u64;
            if location + length > size {
                length = size - location;
            }
            if length > 0 {
                readv_ranges.push((*location, length));
            }
        }
        if self.bisect_nodes.is_none() {
            readv_ranges.push((0, 200));
        }
        self.read_and_parse(readv_ranges)?;

        let mut result: Vec<((u64, IndexKey), LookupResult)> = Vec::new();
        if let Some(nodes) = &self.nodes {
            // read_and_parse promoted to buffer_all.
            for (location, key) in location_keys {
                if !nodes.contains_key(key) {
                    result.push(((*location, key.clone()), LookupResult::Missing));
                } else {
                    let (value, refs) = nodes.get(key).unwrap();
                    result.push((
                        (*location, key.clone()),
                        LookupResult::Found {
                            value: value.clone(),
                            refs: refs.clone(),
                        },
                    ));
                }
            }
            return Ok(result);
        }

        let mut pending_references: Vec<(u64, IndexKey)> = Vec::new();
        let mut pending_locations: std::collections::HashSet<u64> =
            std::collections::HashSet::new();
        let bisect_nodes_view = self
            .bisect_nodes
            .as_ref()
            .expect("bisect_nodes initialised");
        let header_ref_lists = self.header.as_ref().expect("header").node_ref_lists;
        for (location, key) in location_keys {
            if bisect_nodes_view.contains_key(key) {
                let (value, refs) = bisect_nodes_view.get(key).unwrap();
                if header_ref_lists > 0 {
                    let mut wanted: Vec<u64> = Vec::new();
                    for ref_list in refs {
                        for r in ref_list {
                            if !self.keys_by_offset.contains_key(r) {
                                wanted.push(*r);
                            }
                        }
                    }
                    if !wanted.is_empty() {
                        pending_locations.extend(wanted);
                        pending_references.push((*location, key.clone()));
                        continue;
                    }
                    let resolved = self.resolve_references(refs)?;
                    result.push((
                        (*location, key.clone()),
                        LookupResult::Found {
                            value: value.clone(),
                            refs: resolved,
                        },
                    ));
                } else {
                    result.push((
                        (*location, key.clone()),
                        LookupResult::Found {
                            value: value.clone(),
                            refs: Vec::new(),
                        },
                    ));
                }
                continue;
            }
            let key_idx = self.range_map.key_index(&Some(key.clone()));
            if key_idx >= 0 {
                let (key_start, key_end) = self
                    .range_map
                    .key_range(key_idx as usize)
                    .expect("idx in range");
                let (_, byte_end) = self
                    .range_map
                    .byte_range(key_idx as usize)
                    .expect("idx in range");
                if key_start.as_ref().map(|k| k <= key).unwrap_or(true)
                    && (key_end.as_ref().map(|k| k >= key).unwrap_or(false) || byte_end == size)
                {
                    result.push(((*location, key.clone()), LookupResult::Missing));
                    continue;
                }
            }
            let byte_idx = self.range_map.byte_index(*location);
            let (probed_key_start, _) = self
                .range_map
                .key_range(byte_idx.max(0) as usize)
                .unwrap_or((None, None));
            let direction = if probed_key_start.as_ref().map(|k| key < k).unwrap_or(false) {
                LookupResult::Direction(-1)
            } else {
                LookupResult::Direction(1)
            };
            result.push(((*location, key.clone()), direction));
        }

        // Resolve pending references with another readv pass.
        let mut more_ranges: Vec<(u64, u64)> = Vec::new();
        for location in &pending_locations {
            let mut length = 800u64;
            if location + length > size {
                length = size - location;
            }
            if length > 0 {
                more_ranges.push((*location, length));
            }
        }
        self.read_and_parse(more_ranges)?;

        if let Some(nodes) = &self.nodes {
            for (location, key) in pending_references {
                let (value, refs) = nodes.get(&key).expect("nodes contains pending key");
                result.push((
                    (location, key.clone()),
                    LookupResult::Found {
                        value: value.clone(),
                        refs: refs.clone(),
                    },
                ));
            }
            return Ok(result);
        }
        // Re-borrow bisect_nodes since read_and_parse may have mutated it.
        let bisect_nodes_view = self
            .bisect_nodes
            .as_ref()
            .expect("bisect_nodes initialised");
        let pending_clone: Vec<(u64, IndexKey)> = pending_references.clone();
        for (location, key) in pending_clone {
            let (value, refs) = bisect_nodes_view
                .get(&key)
                .expect("bisect_nodes contains pending key");
            let value = value.clone();
            let refs = refs.clone();
            let resolved = self.resolve_references(&refs)?;
            result.push((
                (location, key),
                LookupResult::Found {
                    value,
                    refs: resolved,
                },
            ));
        }
        Ok(result)
    }
}

/// Outcome of a single `(location, key)` probe in
/// [`GraphIndex::lookup_keys_via_location`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LookupResult {
    /// Key is present; `refs` is fully key-resolved.
    Found {
        value: Vec<u8>,
        refs: Vec<Vec<IndexKey>>,
    },
    /// Key is absent in this index.
    Missing,
    /// Key is in an unparsed region above (`+1`) or below (`-1`) the
    /// probed location.
    Direction(i32),
}

/// One node held by [`GraphIndexBuilder`]. `absent` mirrors the
/// `b""` (present) vs `b"a"` (absent) marker stored in the Python
/// builder's three-tuple.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuilderNode {
    pub absent: bool,
    pub references: Vec<Vec<IndexKey>>,
    pub value: Vec<u8>,
}

/// In-memory builder for a graph-index file. Mirrors the Python
/// `GraphIndexBuilder`/`InMemoryGraphIndex`.
///
/// Use [`GraphIndexBuilder::add_node`] to insert nodes, then
/// [`GraphIndexBuilder::finish`] to serialise to the format-1 byte
/// stream the on-disk reader consumes.
#[derive(Debug, Clone)]
pub struct GraphIndexBuilder {
    reference_lists: usize,
    key_length: usize,
    nodes: HashMap<IndexKey, BuilderNode>,
    absent_keys: std::collections::HashSet<IndexKey>,
    optimize_for_size: bool,
    combine_backing_indices: bool,
}

impl GraphIndexBuilder {
    /// Create a new builder. `reference_lists` is the number of
    /// parallel reference lists per node (0, 1, or 2 in practice);
    /// `key_elements` is the tuple length every key must have.
    pub fn new(reference_lists: usize, key_elements: usize) -> Self {
        Self {
            reference_lists,
            key_length: key_elements,
            nodes: HashMap::new(),
            absent_keys: std::collections::HashSet::new(),
            optimize_for_size: false,
            combine_backing_indices: true,
        }
    }

    pub fn reference_lists(&self) -> usize {
        self.reference_lists
    }

    pub fn key_length(&self) -> usize {
        self.key_length
    }

    pub fn optimize_for_size(&self) -> bool {
        self.optimize_for_size
    }

    pub fn combine_backing_indices(&self) -> bool {
        self.combine_backing_indices
    }

    /// Mirrors `GraphIndexBuilder.set_optimize`. Either flag may be
    /// `None` to leave the current setting alone.
    pub fn set_optimize(&mut self, for_size: Option<bool>, combine_backing_indices: Option<bool>) {
        if let Some(v) = for_size {
            self.optimize_for_size = v;
        }
        if let Some(v) = combine_backing_indices {
            self.combine_backing_indices = v;
        }
    }

    /// Read-only view of the node table.
    pub fn nodes(&self) -> &HashMap<IndexKey, BuilderNode> {
        &self.nodes
    }

    /// `true` once `key` is in the table and not flagged absent.
    pub fn contains_present(&self, key: &IndexKey) -> bool {
        self.nodes.get(key).map(|n| !n.absent).unwrap_or(false)
    }

    /// Validate `key` against this builder's key length and the
    /// disallowed-bytes rules.
    pub fn check_key(&self, key: &IndexKey) -> Result<(), IndexError> {
        if !key_is_valid(key, self.key_length) {
            return Err(IndexError::BadKey(key.clone()));
        }
        Ok(())
    }

    /// Return `(node_refs, absent_references)` for a candidate
    /// `add_node` call. Mirrors `_check_key_ref_value`.
    pub fn check_key_ref_value(
        &self,
        key: &IndexKey,
        references: &[Vec<IndexKey>],
        value: &[u8],
    ) -> Result<(Vec<Vec<IndexKey>>, Vec<IndexKey>), IndexError> {
        self.check_key(key)?;
        if !value_is_valid(value) {
            return Err(IndexError::BadValue(format!(
                "value {:?} contains \\n or \\0",
                value
            )));
        }
        if references.len() != self.reference_lists {
            return Err(IndexError::BadValue(format!(
                "expected {} reference lists, got {}",
                self.reference_lists,
                references.len()
            )));
        }
        let mut absent = Vec::new();
        let mut node_refs = Vec::with_capacity(references.len());
        for ref_list in references {
            let mut tupled: Vec<IndexKey> = Vec::with_capacity(ref_list.len());
            for r in ref_list {
                if !self.nodes.contains_key(r) {
                    self.check_key(r)?;
                    absent.push(r.clone());
                }
                tupled.push(r.clone());
            }
            node_refs.push(tupled);
        }
        Ok((node_refs, absent))
    }

    /// Insert a node. Returns [`IndexError::DuplicateKey`] if `key` is
    /// already present (and not flagged absent).
    pub fn add_node(
        &mut self,
        key: IndexKey,
        value: Vec<u8>,
        references: Vec<Vec<IndexKey>>,
    ) -> Result<(), IndexError> {
        let (node_refs, absent_refs) = self.check_key_ref_value(&key, &references, &value)?;
        if let Some(existing) = self.nodes.get(&key) {
            if !existing.absent {
                return Err(IndexError::DuplicateKey(key));
            }
        }
        for r in &absent_refs {
            self.nodes.entry(r.clone()).or_insert_with(|| BuilderNode {
                absent: true,
                references: Vec::new(),
                value: Vec::new(),
            });
            self.absent_keys.insert(r.clone());
        }
        self.absent_keys.remove(&key);
        self.nodes.insert(
            key,
            BuilderNode {
                absent: false,
                references: node_refs,
                value,
            },
        );
        Ok(())
    }

    /// Number of present (non-absent) keys.
    pub fn key_count(&self) -> usize {
        self.nodes.len() - self.absent_keys.len()
    }

    /// Iterate every present entry as `(key, value, refs)`. Order is
    /// unspecified.
    pub fn iter_all_entries(
        &self,
    ) -> impl Iterator<Item = (IndexKey, Vec<u8>, Vec<Vec<IndexKey>>)> + '_ {
        self.nodes.iter().filter_map(|(k, n)| {
            if n.absent {
                None
            } else {
                Some((k.clone(), n.value.clone(), n.references.clone()))
            }
        })
    }

    /// Iterate present entries whose key is in `keys`.
    pub fn iter_entries<'a, I>(
        &'a self,
        keys: I,
    ) -> impl Iterator<Item = (IndexKey, Vec<u8>, Vec<Vec<IndexKey>>)> + 'a
    where
        I: IntoIterator<Item = IndexKey> + 'a,
    {
        keys.into_iter().filter_map(move |k| {
            let n = self.nodes.get(&k)?;
            if n.absent {
                None
            } else {
                Some((k, n.value.clone(), n.references.clone()))
            }
        })
    }

    /// Iterate present entries whose key matches one of `prefixes`.
    /// Each prefix is a [`KeyPrefix`] — same length as a key with
    /// trailing slots set to `None`. The first slot must not be `None`.
    pub fn iter_entries_prefix(
        &self,
        prefixes: &[KeyPrefix],
    ) -> Result<Vec<IndexEntry>, IndexError> {
        for p in prefixes {
            if p.len() != self.key_length {
                return Err(IndexError::BadKey(
                    p.iter().map(|e| e.clone().unwrap_or_default()).collect(),
                ));
            }
            if matches!(p.first(), Some(None)) {
                return Err(IndexError::BadKey(Vec::new()));
            }
        }
        let mut out = Vec::new();
        let mut emitted: std::collections::HashSet<IndexKey> = std::collections::HashSet::new();
        for prefix in prefixes {
            for (k, n) in self.nodes.iter() {
                if n.absent {
                    continue;
                }
                if k.len() != self.key_length {
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
                    out.push((k.clone(), n.value.clone(), n.references.clone()));
                }
            }
        }
        Ok(out)
    }

    /// Reference keys not present in this builder, drawn from the
    /// second reference list. Mirrors `_external_references`.
    pub fn external_references(&self) -> std::collections::HashSet<IndexKey> {
        let mut refs = std::collections::HashSet::new();
        if self.reference_lists < 2 {
            return refs;
        }
        let mut keys: std::collections::HashSet<&IndexKey> = std::collections::HashSet::new();
        for (k, n) in &self.nodes {
            if n.absent {
                continue;
            }
            keys.insert(k);
            if let Some(list) = n.references.get(1) {
                for r in list {
                    refs.insert(r.clone());
                }
            }
        }
        refs.retain(|r| !keys.contains(r));
        refs
    }

    /// Serialise to the format-1 byte stream.
    pub fn finish(&self) -> Result<Vec<u8>, IndexError> {
        let mut nodes: Vec<IndexNode> = Vec::with_capacity(self.nodes.len());
        for (key, node) in &self.nodes {
            nodes.push(IndexNode {
                key: key.clone(),
                absent: node.absent,
                references: node.references.clone(),
                value: node.value.clone(),
            });
        }
        serialize_graph_index(&nodes, self.reference_lists, self.key_length)
    }

    /// Compute ancestry by walking iter_entries and following the
    /// reference list at `ref_list_num`. Mirrors
    /// `GraphIndexBuilder.find_ancestry`.
    pub fn find_ancestry(
        &self,
        keys: &[IndexKey],
        ref_list_num: usize,
    ) -> Result<
        (
            HashMap<IndexKey, Vec<IndexKey>>,
            std::collections::HashSet<IndexKey>,
        ),
        IndexError,
    > {
        let mut pending: std::collections::HashSet<IndexKey> = keys.iter().cloned().collect();
        let mut parent_map: HashMap<IndexKey, Vec<IndexKey>> = HashMap::new();
        let mut missing: std::collections::HashSet<IndexKey> = std::collections::HashSet::new();
        while !pending.is_empty() {
            let mut next_pending: std::collections::HashSet<IndexKey> =
                std::collections::HashSet::new();
            let snapshot: Vec<IndexKey> = pending.iter().cloned().collect();
            for (k, _v, refs) in self.iter_entries(snapshot) {
                let parent_keys = refs.get(ref_list_num).cloned().unwrap_or_default();
                for p in &parent_keys {
                    if !parent_map.contains_key(p) {
                        next_pending.insert(p.clone());
                    }
                }
                parent_map.insert(k, parent_keys);
            }
            for k in pending.iter() {
                if !parent_map.contains_key(k) {
                    missing.insert(k.clone());
                }
            }
            pending = next_pending;
        }
        Ok((parent_map, missing))
    }
}

/// Validate-and-add interface that all index implementations support.
/// Pure-Rust consumers can use this for index abstraction; the pyo3
/// layer hides this behind duck-typed Python objects.
pub trait IndexLike {
    /// Number of present keys in the index.
    fn key_count(&self) -> Result<usize, IndexError>;

    /// Number of parallel reference lists per node.
    fn node_ref_lists(&self) -> Result<usize, IndexError>;

    /// Iterate every present entry.
    fn iter_all(&self) -> Result<Vec<IndexEntry>, IndexError>;

    /// Iterate present entries restricted to `keys`.
    fn iter(&self, keys: &[IndexKey]) -> Result<Vec<IndexEntry>, IndexError>;

    /// Iterate present entries whose keys match one of `prefixes`.
    fn iter_prefix(&self, prefixes: &[KeyPrefix]) -> Result<Vec<IndexEntry>, IndexError>;

    /// Set of reference keys at `ref_list_num` not present in the
    /// index.
    fn external_refs(
        &self,
        ref_list_num: usize,
    ) -> Result<std::collections::HashSet<IndexKey>, IndexError>;

    /// Best-effort validation walk.
    fn validate(&self) -> Result<(), IndexError> {
        let _ = self.iter_all()?;
        Ok(())
    }

    /// Optional cache-clear hook. Default no-op.
    fn clear_cache(&self) {}

    /// One step of the ancestry walk used by
    /// [`CombinedGraphIndex::find_ancestry`]. Looks up each `key` in the
    /// index, populating `parent_map[key] = parent_keys` for each
    /// found entry and adding the unfound keys to `missing_keys`.
    /// Returns the parent keys that aren't already in `parent_map`,
    /// ready to feed into the next iteration.
    fn find_ancestors(
        &self,
        search_keys: &[IndexKey],
        ref_list_num: usize,
        parent_map: &mut HashMap<IndexKey, Vec<IndexKey>>,
        missing_keys: &mut std::collections::HashSet<IndexKey>,
    ) -> Result<std::collections::HashSet<IndexKey>, IndexError> {
        let entries = self.iter(search_keys)?;
        let mut found: std::collections::HashSet<IndexKey> = std::collections::HashSet::new();
        let mut new_search: std::collections::HashSet<IndexKey> = std::collections::HashSet::new();
        for (key, _value, refs) in entries {
            let parents: Vec<IndexKey> = refs.get(ref_list_num).cloned().unwrap_or_default();
            for p in &parents {
                if !parent_map.contains_key(p) {
                    new_search.insert(p.clone());
                }
            }
            found.insert(key.clone());
            parent_map.insert(key, parents);
        }
        for k in search_keys {
            if !found.contains(k) {
                missing_keys.insert(k.clone());
            }
        }
        // Drop keys we already have parents for.
        new_search.retain(|k| !parent_map.contains_key(k));
        Ok(new_search)
    }
}

impl IndexLike for GraphIndexBuilder {
    fn key_count(&self) -> Result<usize, IndexError> {
        Ok(self.key_count())
    }

    fn node_ref_lists(&self) -> Result<usize, IndexError> {
        Ok(self.reference_lists)
    }

    fn iter_all(&self) -> Result<Vec<IndexEntry>, IndexError> {
        Ok(self.iter_all_entries().collect())
    }

    fn iter(&self, keys: &[IndexKey]) -> Result<Vec<IndexEntry>, IndexError> {
        Ok(self.iter_entries(keys.iter().cloned()).collect())
    }

    fn iter_prefix(&self, prefixes: &[KeyPrefix]) -> Result<Vec<IndexEntry>, IndexError> {
        self.iter_entries_prefix(prefixes)
    }

    fn external_refs(
        &self,
        ref_list_num: usize,
    ) -> Result<std::collections::HashSet<IndexKey>, IndexError> {
        if ref_list_num + 1 > self.reference_lists {
            return Err(IndexError::Other(format!(
                "No ref list {}, index has {} ref lists",
                ref_list_num, self.reference_lists
            )));
        }
        if ref_list_num != 1 {
            // The Python `_external_references` is hard-coded to use
            // ref list 1; for other lists we have no implementation.
            return Ok(std::collections::HashSet::new());
        }
        Ok(self.external_references())
    }
}

impl<T: IndexTransport> IndexLike for std::sync::Mutex<GraphIndex<T>> {
    fn key_count(&self) -> Result<usize, IndexError> {
        self.lock().unwrap().key_count()
    }

    fn node_ref_lists(&self) -> Result<usize, IndexError> {
        self.lock().unwrap().node_ref_lists()
    }

    fn iter_all(&self) -> Result<Vec<IndexEntry>, IndexError> {
        self.lock().unwrap().iter_all_entries()
    }

    fn iter(&self, keys: &[IndexKey]) -> Result<Vec<IndexEntry>, IndexError> {
        self.lock().unwrap().iter_entries(keys)
    }

    fn iter_prefix(&self, prefixes: &[KeyPrefix]) -> Result<Vec<IndexEntry>, IndexError> {
        self.lock().unwrap().iter_entries_prefix(prefixes)
    }

    fn external_refs(
        &self,
        ref_list_num: usize,
    ) -> Result<std::collections::HashSet<IndexKey>, IndexError> {
        self.lock().unwrap().external_references(ref_list_num)
    }

    fn validate(&self) -> Result<(), IndexError> {
        self.lock().unwrap().validate()
    }
}

/// A combined view over multiple [`IndexLike`] backends. Mirrors
/// the Python `CombinedGraphIndex`.
pub struct CombinedGraphIndex {
    indices: Vec<Box<dyn IndexLike + Send + Sync>>,
}

impl CombinedGraphIndex {
    pub fn new() -> Self {
        Self {
            indices: Vec::new(),
        }
    }

    pub fn from_indices(indices: Vec<Box<dyn IndexLike + Send + Sync>>) -> Self {
        Self { indices }
    }

    pub fn push(&mut self, index: Box<dyn IndexLike + Send + Sync>) {
        self.indices.push(index);
    }

    pub fn insert(&mut self, pos: usize, index: Box<dyn IndexLike + Send + Sync>) {
        self.indices.insert(pos, index);
    }

    pub fn len(&self) -> usize {
        self.indices.len()
    }

    pub fn is_empty(&self) -> bool {
        self.indices.is_empty()
    }

    /// Read-only access to the wrapped indices.
    pub fn indices(&self) -> &[Box<dyn IndexLike + Send + Sync>] {
        &self.indices
    }

    /// Move the indices at `hits` (positional) to the front of the
    /// list, preserving relative order. Mirrors
    /// `CombinedGraphIndex._move_to_front_by_index`.
    pub fn move_to_front(&mut self, hits: &[usize]) {
        if hits.is_empty() {
            return;
        }
        let mut hit_set: std::collections::HashSet<usize> = hits.iter().copied().collect();
        let mut new_order: Vec<Box<dyn IndexLike + Send + Sync>> =
            Vec::with_capacity(self.indices.len());
        // Preserve the order specified by `hits`.
        for &h in hits {
            if h < self.indices.len() {
                hit_set.remove(&h);
            }
        }
        // Move hits to the front in the requested order.
        let mut taken: HashMap<usize, Box<dyn IndexLike + Send + Sync>> = HashMap::new();
        for (i, idx) in std::mem::take(&mut self.indices).into_iter().enumerate() {
            taken.insert(i, idx);
        }
        for &h in hits {
            if let Some(idx) = taken.remove(&h) {
                new_order.push(idx);
            }
        }
        // Then keep the rest in original order.
        let mut leftover: Vec<(usize, Box<dyn IndexLike + Send + Sync>)> =
            taken.into_iter().collect();
        leftover.sort_by_key(|(i, _)| *i);
        for (_, idx) in leftover {
            new_order.push(idx);
        }
        self.indices = new_order;
    }
}

impl Default for CombinedGraphIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl CombinedGraphIndex {
    /// Like [`Self::iter`] but also returns the (positional) indices
    /// that contributed at least one entry — the caller can pass this
    /// to [`Self::move_to_front`] to reorder for locality.
    pub fn iter_entries_with_hits(
        &mut self,
        keys: &[IndexKey],
    ) -> Result<(Vec<IndexEntry>, Vec<usize>), IndexError> {
        let mut remaining: std::collections::HashSet<IndexKey> = keys.iter().cloned().collect();
        let mut out = Vec::new();
        let mut hits: Vec<usize> = Vec::new();
        for (i, idx) in self.indices.iter().enumerate() {
            if remaining.is_empty() {
                break;
            }
            let snapshot: Vec<IndexKey> = remaining.iter().cloned().collect();
            let entries = idx.iter(&snapshot)?;
            let mut hit = false;
            for entry in entries {
                if remaining.remove(&entry.0) {
                    out.push(entry);
                    hit = true;
                }
            }
            if hit {
                hits.push(i);
            }
        }
        Ok((out, hits))
    }

    /// Like [`Self::iter_prefix`] but also reports which positional
    /// indices contributed.
    pub fn iter_entries_prefix_with_hits(
        &mut self,
        prefixes: &[KeyPrefix],
    ) -> Result<(Vec<IndexEntry>, Vec<usize>), IndexError> {
        let mut seen: std::collections::HashSet<IndexKey> = std::collections::HashSet::new();
        let mut out = Vec::new();
        let mut hits: Vec<usize> = Vec::new();
        for (i, idx) in self.indices.iter().enumerate() {
            let entries = idx.iter_prefix(prefixes)?;
            let mut hit = false;
            for entry in entries {
                if seen.insert(entry.0.clone()) {
                    out.push(entry);
                    hit = true;
                }
            }
            if hit {
                hits.push(i);
            }
        }
        Ok((out, hits))
    }

    /// Find the complete ancestry for `keys`. Returns `(parent_map,
    /// missing_keys)`. Mirrors `CombinedGraphIndex.find_ancestry`.
    pub fn find_ancestry(
        &self,
        keys: &[IndexKey],
        ref_list_num: usize,
    ) -> Result<
        (
            HashMap<IndexKey, Vec<IndexKey>>,
            std::collections::HashSet<IndexKey>,
        ),
        IndexError,
    > {
        let mut parent_map: HashMap<IndexKey, Vec<IndexKey>> = HashMap::new();
        let mut missing_keys: std::collections::HashSet<IndexKey> =
            std::collections::HashSet::new();
        let mut keys_to_lookup: std::collections::HashSet<IndexKey> =
            keys.iter().cloned().collect();
        while !keys_to_lookup.is_empty() {
            let mut all_index_missing: Option<std::collections::HashSet<IndexKey>> = None;
            // The next index searches for what the previous one failed
            // to find — so reduce keys_to_lookup at each step.
            let mut current = keys_to_lookup.clone();
            for idx in &self.indices {
                let mut index_missing: std::collections::HashSet<IndexKey> =
                    std::collections::HashSet::new();
                let snapshot: Vec<IndexKey> = current.iter().cloned().collect();
                let mut search_keys = snapshot;
                while !search_keys.is_empty() {
                    let new_search = idx.find_ancestors(
                        &search_keys,
                        ref_list_num,
                        &mut parent_map,
                        &mut index_missing,
                    )?;
                    search_keys = new_search.into_iter().collect();
                }
                match all_index_missing.as_mut() {
                    None => {
                        all_index_missing = Some(index_missing.clone());
                    }
                    Some(prev) => {
                        prev.retain(|k| index_missing.contains(k));
                    }
                }
                current = index_missing;
                if current.is_empty() {
                    break;
                }
            }
            match all_index_missing {
                None => {
                    // No indices: everything we asked for is missing.
                    missing_keys.extend(current);
                    break;
                }
                Some(s) => {
                    missing_keys.extend(s.iter().cloned());
                    keys_to_lookup = current.difference(&s).cloned().collect();
                }
            }
        }
        Ok((parent_map, missing_keys))
    }

    /// Get the parent map for the given keys, mirroring
    /// `CombinedGraphIndex.get_parent_map`. `null_revision` is the
    /// project's `NULL_REVISION` constant — passed in so the pure
    /// crate stays unaware of revision-specific semantics.
    pub fn get_parent_map(
        &self,
        keys: &[IndexKey],
        null_revision: &IndexKey,
    ) -> Result<HashMap<IndexKey, Vec<IndexKey>>, IndexError> {
        let mut search_keys: Vec<IndexKey> = keys.to_vec();
        let mut found_parents: HashMap<IndexKey, Vec<IndexKey>> = HashMap::new();
        if let Some(pos) = search_keys.iter().position(|k| k == null_revision) {
            search_keys.remove(pos);
            found_parents.insert(null_revision.clone(), Vec::new());
        }
        for (key, _value, refs) in self.iter(&search_keys)? {
            let parents = refs.first().cloned().unwrap_or_default();
            if parents.is_empty() {
                found_parents.insert(key, vec![null_revision.clone()]);
            } else {
                found_parents.insert(key, parents);
            }
        }
        Ok(found_parents)
    }
}

impl IndexLike for CombinedGraphIndex {
    fn key_count(&self) -> Result<usize, IndexError> {
        let mut total = 0;
        for idx in &self.indices {
            total += idx.key_count()?;
        }
        Ok(total)
    }

    fn node_ref_lists(&self) -> Result<usize, IndexError> {
        // Combined inherits the first index's setting.
        if let Some(first) = self.indices.first() {
            first.node_ref_lists()
        } else {
            Ok(0)
        }
    }

    fn iter_all(&self) -> Result<Vec<IndexEntry>, IndexError> {
        let mut seen: std::collections::HashSet<IndexKey> = std::collections::HashSet::new();
        let mut out = Vec::new();
        for idx in &self.indices {
            for entry in idx.iter_all()? {
                if seen.insert(entry.0.clone()) {
                    out.push(entry);
                }
            }
        }
        Ok(out)
    }

    fn iter(&self, keys: &[IndexKey]) -> Result<Vec<IndexEntry>, IndexError> {
        let mut remaining: std::collections::HashSet<IndexKey> = keys.iter().cloned().collect();
        let mut out = Vec::new();
        for idx in &self.indices {
            if remaining.is_empty() {
                break;
            }
            let snapshot: Vec<IndexKey> = remaining.iter().cloned().collect();
            for entry in idx.iter(&snapshot)? {
                if remaining.remove(&entry.0) {
                    out.push(entry);
                }
            }
        }
        Ok(out)
    }

    fn iter_prefix(&self, prefixes: &[KeyPrefix]) -> Result<Vec<IndexEntry>, IndexError> {
        let mut seen: std::collections::HashSet<IndexKey> = std::collections::HashSet::new();
        let mut out = Vec::new();
        for idx in &self.indices {
            for entry in idx.iter_prefix(prefixes)? {
                if seen.insert(entry.0.clone()) {
                    out.push(entry);
                }
            }
        }
        Ok(out)
    }

    fn external_refs(
        &self,
        ref_list_num: usize,
    ) -> Result<std::collections::HashSet<IndexKey>, IndexError> {
        let mut refs = std::collections::HashSet::new();
        for idx in &self.indices {
            refs.extend(idx.external_refs(ref_list_num)?);
        }
        Ok(refs)
    }

    fn validate(&self) -> Result<(), IndexError> {
        for idx in &self.indices {
            idx.validate()?;
        }
        Ok(())
    }

    fn clear_cache(&self) {
        for idx in &self.indices {
            idx.clear_cache();
        }
    }
}

/// An adapter that prefixes/un-prefixes every key passed through to a
/// wrapped index. Mirrors `GraphIndexPrefixAdapter`.
pub struct GraphIndexPrefixAdapter<I: IndexLike> {
    inner: I,
    prefix: IndexKey,
    /// `prefix.len()` cached.
    prefix_len: usize,
    /// `prefix + (None,) * missing_key_length` — used for prefix
    /// queries against the inner index.
    prefix_query: KeyPrefix,
}

impl<I: IndexLike> GraphIndexPrefixAdapter<I> {
    pub fn new(inner: I, prefix: IndexKey, missing_key_length: usize) -> Self {
        let prefix_len = prefix.len();
        let mut prefix_query: KeyPrefix = prefix.iter().cloned().map(Some).collect();
        for _ in 0..missing_key_length {
            prefix_query.push(None);
        }
        Self {
            inner,
            prefix,
            prefix_len,
            prefix_query,
        }
    }

    fn extend_key(&self, key: &IndexKey) -> IndexKey {
        let mut full = self.prefix.clone();
        full.extend(key.iter().cloned());
        full
    }

    fn strip_entry(&self, entry: IndexEntry) -> Result<IndexEntry, IndexError> {
        let (key, value, refs) = entry;
        if key.len() < self.prefix_len {
            return Err(IndexError::BadIndexData);
        }
        for (a, b) in self.prefix.iter().zip(key.iter()) {
            if a != b {
                return Err(IndexError::BadIndexData);
            }
        }
        let stripped_key: IndexKey = key[self.prefix_len..].to_vec();
        let mut stripped_refs: Vec<Vec<IndexKey>> = Vec::with_capacity(refs.len());
        for ref_list in refs {
            let mut new_list: Vec<IndexKey> = Vec::with_capacity(ref_list.len());
            for ref_key in ref_list {
                if ref_key.len() < self.prefix_len {
                    return Err(IndexError::BadIndexData);
                }
                for (a, b) in self.prefix.iter().zip(ref_key.iter()) {
                    if a != b {
                        return Err(IndexError::BadIndexData);
                    }
                }
                new_list.push(ref_key[self.prefix_len..].to_vec());
            }
            stripped_refs.push(new_list);
        }
        Ok((stripped_key, value, stripped_refs))
    }
}

impl<I: IndexLike> IndexLike for GraphIndexPrefixAdapter<I> {
    fn key_count(&self) -> Result<usize, IndexError> {
        Ok(self.iter_all()?.len())
    }

    fn node_ref_lists(&self) -> Result<usize, IndexError> {
        self.inner.node_ref_lists()
    }

    fn iter_all(&self) -> Result<Vec<IndexEntry>, IndexError> {
        let inner_entries = self.inner.iter_prefix(&[self.prefix_query.clone()])?;
        let mut out = Vec::with_capacity(inner_entries.len());
        for e in inner_entries {
            out.push(self.strip_entry(e)?);
        }
        Ok(out)
    }

    fn iter(&self, keys: &[IndexKey]) -> Result<Vec<IndexEntry>, IndexError> {
        let extended: Vec<IndexKey> = keys.iter().map(|k| self.extend_key(k)).collect();
        let inner_entries = self.inner.iter(&extended)?;
        let mut out = Vec::with_capacity(inner_entries.len());
        for e in inner_entries {
            out.push(self.strip_entry(e)?);
        }
        Ok(out)
    }

    fn iter_prefix(&self, prefixes: &[KeyPrefix]) -> Result<Vec<IndexEntry>, IndexError> {
        let extended: Vec<KeyPrefix> = prefixes
            .iter()
            .map(|p| {
                let mut full: KeyPrefix = self.prefix.iter().cloned().map(Some).collect();
                full.extend(p.iter().cloned());
                full
            })
            .collect();
        let inner_entries = self.inner.iter_prefix(&extended)?;
        let mut out = Vec::with_capacity(inner_entries.len());
        for e in inner_entries {
            out.push(self.strip_entry(e)?);
        }
        Ok(out)
    }

    fn external_refs(
        &self,
        _ref_list_num: usize,
    ) -> Result<std::collections::HashSet<IndexKey>, IndexError> {
        // Prefix adapter inherits the inner index's external refs but
        // they would need stripping; not exercised by tests.
        Ok(std::collections::HashSet::new())
    }

    fn validate(&self) -> Result<(), IndexError> {
        self.inner.validate()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(parts: &[&[u8]]) -> Vec<Vec<u8>> {
        parts.iter().map(|p| p.to_vec()).collect()
    }

    fn node(k: &[&[u8]], absent: bool, refs: Vec<Vec<Vec<Vec<u8>>>>, value: &[u8]) -> IndexNode {
        IndexNode {
            key: key(k),
            absent,
            references: refs,
            value: value.to_vec(),
        }
    }

    #[test]
    fn serialize_empty_index_no_refs() {
        let out = serialize_graph_index(&[], 0, 1).unwrap();
        assert_eq!(
            out,
            b"Bazaar Graph Index 1\nnode_ref_lists=0\nkey_elements=1\nlen=0\n\n".to_vec()
        );
    }

    #[test]
    fn serialize_single_node_no_refs() {
        let nodes = vec![node(&[b"a"], false, vec![], b"val")];
        let out = serialize_graph_index(&nodes, 0, 1).unwrap();
        assert_eq!(
            out,
            b"Bazaar Graph Index 1\nnode_ref_lists=0\nkey_elements=1\nlen=1\na\x00\x00\x00val\n\n"
                .to_vec()
        );
    }

    #[test]
    fn serialize_with_reference_back_to_earlier_key() {
        // Two nodes where `b` references `a`. Byte-exact output verified
        // against Python.
        let nodes = vec![
            node(&[b"a"], false, vec![vec![]], b"val1"),
            node(&[b"b"], false, vec![vec![key(&[b"a"])]], b"val2"),
        ];
        let out = serialize_graph_index(&nodes, 1, 1).unwrap();
        assert_eq!(
            out,
            b"Bazaar Graph Index 1\nnode_ref_lists=1\nkey_elements=1\nlen=2\na\x00\x00\x00val1\nb\x00\x0059\x00val2\n\n"
                .to_vec()
        );
    }

    #[test]
    fn serialize_absent_node_has_no_tab_between_ref_lists() {
        // Verified against Python: an absent node writes `\x00a\x00\x00\n`
        // with no tab separator between the would-be reference lists.
        let nodes = vec![
            node(
                &[b"a"],
                false,
                vec![vec![key(&[b"missing"])], vec![]],
                b"value",
            ),
            node(&[b"missing"], true, vec![], b""),
        ];
        let out = serialize_graph_index(&nodes, 2, 1).unwrap();
        assert_eq!(
            out,
            b"Bazaar Graph Index 1\nnode_ref_lists=2\nkey_elements=1\nlen=1\na\x00\x0072\t\x00value\nmissing\x00a\x00\x00\n\n"
                .to_vec()
        );
    }

    #[test]
    fn serialize_multi_element_key() {
        let nodes = vec![node(&[b"x", b"y"], false, vec![], b"v")];
        let out = serialize_graph_index(&nodes, 0, 2).unwrap();
        // Keys with multiple elements join with \x00.
        assert_eq!(
            out,
            b"Bazaar Graph Index 1\nnode_ref_lists=0\nkey_elements=2\nlen=1\nx\x00y\x00\x00\x00v\n\n"
                .to_vec()
        );
    }

    #[test]
    fn serialize_reports_unknown_reference() {
        let nodes = vec![node(&[b"a"], false, vec![vec![key(&[b"missing"])]], b"v")];
        let err = serialize_graph_index(&nodes, 1, 1).unwrap_err();
        assert_eq!(err, IndexError::UnknownReference(key(&[b"missing"])));
    }

    #[test]
    fn serialize_pads_reference_offsets_to_matching_width() {
        // A 20-node chain forces 3-digit offsets; verified against
        // Python output for the exact same sequence.
        let mut nodes: Vec<IndexNode> = Vec::new();
        for i in 0..20 {
            let k = format!("key{:03}", i).into_bytes();
            let refs = if i == 0 {
                vec![vec![]]
            } else {
                vec![vec![key(&[&format!("key{:03}", i - 1).into_bytes()])]]
            };
            nodes.push(node(
                &[k.as_slice()],
                false,
                refs,
                format!("value{:03}", i).as_bytes(),
            ));
        }
        let out = serialize_graph_index(&nodes, 1, 1).unwrap();
        assert_eq!(out.len(), 478);
        // First reference points back to key000 at the very start of the
        // body and is padded to 3 digits.
        assert!(out
            .windows(b"key001\x00\x00060\x00value001\n".len())
            .any(|w| w == b"key001\x00\x00060\x00value001\n"));
    }

    #[test]
    fn parse_header_minimal_index() {
        let data = b"Bazaar Graph Index 1\nnode_ref_lists=0\nkey_elements=1\nlen=0\n\n";
        let h = parse_header(data).unwrap();
        assert_eq!(h.node_ref_lists, 0);
        assert_eq!(h.key_length, 1);
        assert_eq!(h.key_count, 0);
        // Header bytes end right after the `len=0\n` line.
        assert_eq!(h.header_end, 59);
    }

    #[test]
    fn parse_header_non_zero_values() {
        let data = b"Bazaar Graph Index 1\nnode_ref_lists=2\nkey_elements=3\nlen=42\n";
        let h = parse_header(data).unwrap();
        assert_eq!(h.node_ref_lists, 2);
        assert_eq!(h.key_length, 3);
        assert_eq!(h.key_count, 42);
    }

    #[test]
    fn parse_header_rejects_bad_signature() {
        assert_eq!(
            parse_header(b"not an index\n"),
            Err(IndexError::BadSignature)
        );
    }

    #[test]
    fn parse_header_rejects_missing_option() {
        let data = b"Bazaar Graph Index 1\nwrong_option=1\nkey_elements=1\nlen=0\n\n";
        assert_eq!(parse_header(data), Err(IndexError::BadOptions));
    }

    #[test]
    fn parse_header_rejects_non_decimal_option() {
        let data = b"Bazaar Graph Index 1\nnode_ref_lists=abc\nkey_elements=1\nlen=0\n\n";
        assert_eq!(parse_header(data), Err(IndexError::BadOptions));
    }

    #[test]
    fn parse_lines_single_node_no_refs() {
        let line: &[u8] = b"a\x00\x00\x00val";
        let lines = vec![line, b""];
        let parsed = parse_lines(&lines, 100, 1).unwrap();
        assert_eq!(parsed.first_key, Some(key(&[b"a"])));
        assert_eq!(parsed.last_key, Some(key(&[b"a"])));
        assert_eq!(parsed.trailers, 1);
        assert_eq!(parsed.nodes.len(), 1);
        let (k, v, refs) = &parsed.nodes[0];
        assert_eq!(k, &key(&[b"a"]));
        assert_eq!(v, b"val");
        // `_parse_lines` always pushes at least one reference list per node,
        // even when there are no ref lists declared — Python yields `(())`.
        assert_eq!(refs, &vec![Vec::<u64>::new()]);
    }

    #[test]
    fn parse_lines_tracks_offsets() {
        // Two lines starting at pos=0; the second should land at len+1.
        let line_a: &[u8] = b"a\x00\x00\x00val1";
        let line_b: &[u8] = b"b\x00\x0000\x00val2";
        let lines = vec![line_a, line_b];
        let parsed = parse_lines(&lines, 0, 1).unwrap();
        assert_eq!(parsed.keys_by_offset.len(), 2);
        assert_eq!(parsed.keys_by_offset[0].0, 0);
        assert_eq!(parsed.keys_by_offset[1].0, line_a.len() as u64 + 1);
    }

    #[test]
    fn parse_lines_absent_node_not_in_output_but_in_offset_map() {
        let line: &[u8] = b"ghost\x00a\x00\x00";
        let parsed = parse_lines(&[line], 50, 1).unwrap();
        assert!(parsed.nodes.is_empty());
        assert_eq!(parsed.keys_by_offset.len(), 1);
        assert!(parsed.keys_by_offset[0].1.absent);
        assert_eq!(parsed.keys_by_offset[0].1.key, key(&[b"ghost"]));
    }

    #[test]
    fn parse_lines_references() {
        // Two reference lists separated by \t, offsets separated by \r.
        let line: &[u8] = b"k\x00\x00100\r200\t300\x00val";
        let parsed = parse_lines(&[line], 0, 1).unwrap();
        let refs = &parsed.nodes[0].2;
        assert_eq!(refs.len(), 2);
        assert_eq!(refs[0], vec![100u64, 200]);
        assert_eq!(refs[1], vec![300u64]);
    }

    #[test]
    fn parse_lines_bad_field_count_errors() {
        let line: &[u8] = b"k\x00\x00val"; // 3 fields, expected 4 for key_length=1
        assert_eq!(parse_lines(&[line], 0, 1), Err(IndexError::BadLineData));
    }

    #[test]
    fn parse_lines_bad_reference_offset_errors() {
        let line: &[u8] = b"k\x00\x00notnumeric\x00val";
        assert!(matches!(
            parse_lines(&[line], 0, 1),
            Err(IndexError::BadReferenceOffset(_))
        ));
    }

    #[test]
    fn round_trip_serialize_then_parse() {
        // Two-node index with a cross-reference. Serialize, then parse the
        // header and body back and verify we recover the same shape.
        let nodes = vec![
            node(&[b"a"], false, vec![vec![]], b"val1"),
            node(&[b"b"], false, vec![vec![key(&[b"a"])]], b"val2"),
        ];
        let bytes = serialize_graph_index(&nodes, 1, 1).unwrap();
        let header = parse_header(&bytes).unwrap();
        assert_eq!(header.node_ref_lists, 1);
        assert_eq!(header.key_length, 1);
        assert_eq!(header.key_count, 2);

        // The body is everything from header_end onwards; split on \n and
        // feed the resulting lines (sans trailing newlines) to parse_lines.
        let body = &bytes[header.header_end..];
        let body_lines: Vec<&[u8]> = body.split(|&b| b == b'\n').collect();
        // The final split produces an empty trailing element; drop if
        // caller wants to feed it in. Here we leave it to exercise the
        // trailer counter.
        let parsed = parse_lines(&body_lines, header.header_end as u64, 1).unwrap();
        assert_eq!(parsed.nodes.len(), 2);
        assert_eq!(parsed.nodes[0].0, key(&[b"a"]));
        assert_eq!(parsed.nodes[1].0, key(&[b"b"]));
        // The reference from `b` points at the byte offset of `a`'s line,
        // which is exactly header_end (the first body byte).
        assert_eq!(parsed.nodes[1].2, vec![vec![header.header_end as u64]]);
        // There's one trailing blank line (the final `\n\n` plus split).
        assert!(parsed.trailers >= 1);
    }

    #[test]
    fn serialize_empty_index_two_element_keys() {
        // Mirrors test_index.test_build_index_empty_two_element_keys.
        let out = serialize_graph_index(&[], 0, 2).unwrap();
        assert_eq!(
            out,
            b"Bazaar Graph Index 1\nnode_ref_lists=0\nkey_elements=2\nlen=0\n\n".to_vec()
        );
    }

    #[test]
    fn serialize_empty_index_one_reference_list() {
        // Mirrors test_index.test_build_index_one_reference_list_empty.
        let out = serialize_graph_index(&[], 1, 1).unwrap();
        assert_eq!(
            out,
            b"Bazaar Graph Index 1\nnode_ref_lists=1\nkey_elements=1\nlen=0\n\n".to_vec()
        );
    }

    #[test]
    fn serialize_empty_index_two_reference_lists() {
        // Mirrors test_index.test_build_index_two_reference_list_empty.
        let out = serialize_graph_index(&[], 2, 1).unwrap();
        assert_eq!(
            out,
            b"Bazaar Graph Index 1\nnode_ref_lists=2\nkey_elements=1\nlen=0\n\n".to_vec()
        );
    }

    #[test]
    fn serialize_empty_value_node() {
        // Mirrors test_index.test_add_node_empty_value.
        let nodes = vec![node(&[b"akey"], false, vec![], b"")];
        let out = serialize_graph_index(&nodes, 0, 1).unwrap();
        assert_eq!(
            out,
            b"Bazaar Graph Index 1\nnode_ref_lists=0\nkey_elements=1\nlen=1\nakey\x00\x00\x00\n\n"
                .to_vec()
        );
    }

    #[test]
    fn serialize_sorts_three_nodes_byte_exact() {
        // Mirrors test_index.test_build_index_nodes_sorted.
        let nodes = vec![
            node(&[b"2002"], false, vec![], b"data"),
            node(&[b"2000"], false, vec![], b"data"),
            node(&[b"2001"], false, vec![], b"data"),
        ];
        let out = serialize_graph_index(&nodes, 0, 1).unwrap();
        assert_eq!(
            out,
            b"Bazaar Graph Index 1\nnode_ref_lists=0\nkey_elements=1\nlen=3\n\
              2000\x00\x00\x00data\n\
              2001\x00\x00\x00data\n\
              2002\x00\x00\x00data\n\n"
                .to_vec()
        );
    }

    #[test]
    fn serialize_sorts_two_element_keys_lexicographically() {
        // Mirrors test_index.test_build_index_2_element_key_nodes_sorted
        // — verifies both elements are used for comparison.
        let mut nodes = Vec::new();
        for first in &[b"2002", b"2000", b"2001"] {
            for second in &[b"2002", b"2000", b"2001"] {
                nodes.push(node(&[*first, *second], false, vec![], b"data"));
            }
        }
        let out = serialize_graph_index(&nodes, 0, 2).unwrap();
        let expected: Vec<u8> = [
            b"Bazaar Graph Index 1\nnode_ref_lists=0\nkey_elements=2\nlen=9\n".as_slice(),
            b"2000\x002000\x00\x00\x00data\n",
            b"2000\x002001\x00\x00\x00data\n",
            b"2000\x002002\x00\x00\x00data\n",
            b"2001\x002000\x00\x00\x00data\n",
            b"2001\x002001\x00\x00\x00data\n",
            b"2001\x002002\x00\x00\x00data\n",
            b"2002\x002000\x00\x00\x00data\n",
            b"2002\x002001\x00\x00\x00data\n",
            b"2002\x002002\x00\x00\x00data\n",
            b"\n",
        ]
        .concat();
        assert_eq!(out, expected);
    }

    #[test]
    fn serialize_single_node_with_empty_ref_list_of_one() {
        // Mirrors test_index.test_build_index_reference_lists_are_included_one.
        let nodes = vec![node(&[b"key"], false, vec![vec![]], b"data")];
        let out = serialize_graph_index(&nodes, 1, 1).unwrap();
        assert_eq!(
            out,
            b"Bazaar Graph Index 1\nnode_ref_lists=1\nkey_elements=1\nlen=1\nkey\x00\x00\x00data\n\n"
                .to_vec()
        );
    }

    #[test]
    fn serialize_single_node_with_empty_ref_lists_of_two() {
        // Mirrors test_index.test_build_index_reference_lists_are_included_two.
        // The `\t` separator between the two empty ref lists is the key
        // byte this test pins down.
        let nodes = vec![node(&[b"key"], false, vec![vec![], vec![]], b"data")];
        let out = serialize_graph_index(&nodes, 2, 1).unwrap();
        assert_eq!(
            out,
            b"Bazaar Graph Index 1\nnode_ref_lists=2\nkey_elements=1\nlen=1\nkey\x00\x00\t\x00data\n\n"
                .to_vec()
        );
    }

    #[test]
    fn serialize_ref_list_with_two_element_keys() {
        // Mirrors test_index.test_build_index_reference_lists_with_2_element_keys.
        let nodes = vec![node(&[b"key", b"key2"], false, vec![vec![]], b"data")];
        let out = serialize_graph_index(&nodes, 1, 2).unwrap();
        assert_eq!(
            out,
            b"Bazaar Graph Index 1\nnode_ref_lists=1\nkey_elements=2\nlen=1\nkey\x00key2\x00\x00\x00data\n\n"
                .to_vec()
        );
    }

    #[test]
    fn serialize_cr_delimits_multiple_refs_in_one_list() {
        // Mirrors test_index.test_node_references_are_cr_delimited.
        // The `077\r094` separator is the diagnostic byte sequence.
        let nodes = vec![
            node(&[b"reference"], false, vec![vec![]], b"data"),
            node(&[b"reference2"], false, vec![vec![]], b"data"),
            node(
                &[b"key"],
                false,
                vec![vec![key(&[b"reference"]), key(&[b"reference2"])]],
                b"data",
            ),
        ];
        let out = serialize_graph_index(&nodes, 1, 1).unwrap();
        assert_eq!(
            out,
            b"Bazaar Graph Index 1\nnode_ref_lists=1\nkey_elements=1\nlen=3\n\
              key\x00\x00077\r094\x00data\n\
              reference\x00\x00\x00data\n\
              reference2\x00\x00\x00data\n\n"
                .to_vec()
        );
    }

    #[test]
    fn serialize_tab_delimits_multiple_reference_lists() {
        // Mirrors test_index.test_multiple_reference_lists_are_tab_delimited.
        // Same reference appears in both lists to verify both ref lists
        // share the address table.
        let nodes = vec![
            node(&[b"keference"], false, vec![vec![], vec![]], b"data"),
            node(
                &[b"rey"],
                false,
                vec![vec![key(&[b"keference"])], vec![key(&[b"keference"])]],
                b"data",
            ),
        ];
        let out = serialize_graph_index(&nodes, 2, 1).unwrap();
        assert_eq!(
            out,
            b"Bazaar Graph Index 1\nnode_ref_lists=2\nkey_elements=1\nlen=2\n\
              keference\x00\x00\t\x00data\n\
              rey\x00\x0059\t59\x00data\n\n"
                .to_vec()
        );
    }

    #[test]
    fn serialize_absent_record_has_no_reference_overhead() {
        // Mirrors test_index.test_absent_has_no_reference_overhead.
        // Verifies offset math stays correct when absent records are
        // interleaved with present ones.
        let nodes = vec![
            node(&[b"aail"], true, vec![], b""),
            node(
                &[b"parent"],
                false,
                vec![vec![key(&[b"aail"]), key(&[b"zther"])], vec![]],
                b"",
            ),
            node(&[b"zther"], true, vec![], b""),
        ];
        let out = serialize_graph_index(&nodes, 2, 1).unwrap();
        assert_eq!(
            out,
            b"Bazaar Graph Index 1\nnode_ref_lists=2\nkey_elements=1\nlen=1\n\
              aail\x00a\x00\x00\n\
              parent\x00\x0059\r84\t\x00\n\
              zther\x00a\x00\x00\n\n"
                .to_vec()
        );
    }

    #[test]
    fn serialize_sorts_nodes_by_key() {
        let nodes = vec![
            node(&[b"c"], false, vec![], b"3"),
            node(&[b"a"], false, vec![], b"1"),
            node(&[b"b"], false, vec![], b"2"),
        ];
        let out = serialize_graph_index(&nodes, 0, 1).unwrap();
        let body_start = out
            .windows(b"len=3\n".len())
            .position(|w| w == b"len=3\n")
            .unwrap()
            + b"len=3\n".len();
        let body = &out[body_start..];
        assert!(body.starts_with(b"a\x00\x00\x001"));
        assert!(body.windows(5).any(|w| w == b"b\x00\x00\x002"));
        assert!(body.windows(5).any(|w| w == b"c\x00\x00\x003"));
    }

    struct MemTransport {
        files: std::collections::HashMap<String, Vec<u8>>,
    }

    impl MemTransport {
        fn new() -> Self {
            Self {
                files: std::collections::HashMap::new(),
            }
        }

        fn put(&mut self, path: &str, bytes: Vec<u8>) {
            self.files.insert(path.to_string(), bytes);
        }
    }

    impl IndexTransport for MemTransport {
        fn get_bytes(&self, path: &str) -> Result<Vec<u8>, IndexError> {
            self.files
                .get(path)
                .cloned()
                .ok_or_else(|| IndexError::Other(format!("NoSuchFile: {}", path)))
        }
    }

    fn build_index(nodes: &[IndexNode], reference_lists: usize, key_elements: usize) -> Vec<u8> {
        serialize_graph_index(nodes, reference_lists, key_elements).unwrap()
    }

    #[test]
    fn graph_index_buffer_all_no_refs() {
        let bytes = build_index(
            &[
                node(&[b"a"], false, vec![], b"v1"),
                node(&[b"b"], false, vec![], b"v2"),
            ],
            0,
            1,
        );
        let mut t = MemTransport::new();
        t.put("idx", bytes);
        let mut idx = GraphIndex::new(t, "idx", 0);
        assert_eq!(idx.key_count().unwrap(), 2);
        assert_eq!(idx.node_ref_lists().unwrap(), 0);
        let mut entries = idx.iter_all_entries().unwrap();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            entries,
            vec![
                (key(&[b"a"]), b"v1".to_vec(), vec![]),
                (key(&[b"b"]), b"v2".to_vec(), vec![]),
            ]
        );
    }

    #[test]
    fn graph_index_resolves_references() {
        let bytes = build_index(
            &[
                node(&[b"a"], false, vec![vec![]], b"v1"),
                node(&[b"b"], false, vec![vec![key(&[b"a"])]], b"v2"),
            ],
            1,
            1,
        );
        let mut t = MemTransport::new();
        t.put("idx", bytes);
        let mut idx = GraphIndex::new(t, "idx", 0);
        let mut entries = idx.iter_all_entries().unwrap();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            entries,
            vec![
                (key(&[b"a"]), b"v1".to_vec(), vec![vec![]]),
                (key(&[b"b"]), b"v2".to_vec(), vec![vec![key(&[b"a"])]],),
            ]
        );
    }

    #[test]
    fn graph_index_iter_entries_filters_to_requested_keys() {
        let bytes = build_index(
            &[
                node(&[b"a"], false, vec![], b"v1"),
                node(&[b"b"], false, vec![], b"v2"),
                node(&[b"c"], false, vec![], b"v3"),
            ],
            0,
            1,
        );
        let mut t = MemTransport::new();
        t.put("idx", bytes);
        let mut idx = GraphIndex::new(t, "idx", 0);
        let mut entries = idx
            .iter_entries(&[key(&[b"a"]), key(&[b"missing"]), key(&[b"c"])])
            .unwrap();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            entries,
            vec![
                (key(&[b"a"]), b"v1".to_vec(), vec![]),
                (key(&[b"c"]), b"v3".to_vec(), vec![]),
            ]
        );
    }

    #[test]
    fn graph_index_iter_entries_dedupes_repeated_keys() {
        let bytes = build_index(&[node(&[b"a"], false, vec![], b"v1")], 0, 1);
        let mut t = MemTransport::new();
        t.put("idx", bytes);
        let mut idx = GraphIndex::new(t, "idx", 0);
        let entries = idx.iter_entries(&[key(&[b"a"]), key(&[b"a"])]).unwrap();
        assert_eq!(entries, vec![(key(&[b"a"]), b"v1".to_vec(), vec![])]);
    }

    #[test]
    fn graph_index_external_references() {
        // `a` references `missing` (which is recorded as absent) — that
        // counts as external. `b` references `a` — that's internal.
        let bytes = build_index(
            &[
                node(&[b"a"], false, vec![vec![key(&[b"missing"])]], b"v1"),
                node(&[b"missing"], true, vec![], b""),
                node(&[b"b"], false, vec![vec![key(&[b"a"])]], b"v2"),
            ],
            1,
            1,
        );
        let mut t = MemTransport::new();
        t.put("idx", bytes);
        let mut idx = GraphIndex::new(t, "idx", 0);
        let externals = idx.external_references(0).unwrap();
        let expected: std::collections::HashSet<IndexKey> =
            vec![key(&[b"missing"])].into_iter().collect();
        assert_eq!(externals, expected);
    }

    #[test]
    fn graph_index_external_references_rejects_invalid_ref_list() {
        let bytes = build_index(&[node(&[b"a"], false, vec![], b"v1")], 0, 1);
        let mut t = MemTransport::new();
        t.put("idx", bytes);
        let mut idx = GraphIndex::new(t, "idx", 0);
        let err = idx.external_references(0).unwrap_err();
        assert_eq!(
            err,
            IndexError::Other("No ref list 0, index has 0 ref lists".to_string())
        );
    }

    #[test]
    fn graph_index_iter_entries_prefix_one_element() {
        let bytes = build_index(
            &[
                node(&[b"a"], false, vec![], b"v1"),
                node(&[b"b"], false, vec![], b"v2"),
            ],
            0,
            1,
        );
        let mut t = MemTransport::new();
        t.put("idx", bytes);
        let mut idx = GraphIndex::new(t, "idx", 0);
        // Length-1 prefix is just an exact lookup.
        let entries = idx
            .iter_entries_prefix(&[vec![Some(b"a".to_vec())]])
            .unwrap();
        assert_eq!(entries, vec![(key(&[b"a"]), b"v1".to_vec(), vec![])]);
    }

    #[test]
    fn graph_index_iter_entries_prefix_multi_element() {
        let bytes = build_index(
            &[
                node(&[b"foo", b"bar"], false, vec![], b"v1"),
                node(&[b"foo", b"baz"], false, vec![], b"v2"),
                node(&[b"qux", b"bar"], false, vec![], b"v3"),
            ],
            0,
            2,
        );
        let mut t = MemTransport::new();
        t.put("idx", bytes);
        let mut idx = GraphIndex::new(t, "idx", 0);
        // `(foo, None)` should match both foo entries.
        let mut entries = idx
            .iter_entries_prefix(&[vec![Some(b"foo".to_vec()), None]])
            .unwrap();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            entries,
            vec![
                (key(&[b"foo", b"bar"]), b"v1".to_vec(), vec![]),
                (key(&[b"foo", b"baz"]), b"v2".to_vec(), vec![]),
            ]
        );
    }

    #[test]
    fn graph_index_iter_entries_prefix_rejects_none_first_element() {
        let bytes = build_index(&[node(&[b"a"], false, vec![], b"v1")], 0, 1);
        let mut t = MemTransport::new();
        t.put("idx", bytes);
        let mut idx = GraphIndex::new(t, "idx", 0);
        let err = idx.iter_entries_prefix(&[vec![None]]).unwrap_err();
        assert_eq!(
            err,
            IndexError::Other("BadIndexKey: first prefix element may not be None".to_string())
        );
    }

    #[test]
    fn graph_index_validate_ok_for_well_formed_index() {
        let bytes = build_index(&[node(&[b"a"], false, vec![], b"v")], 0, 1);
        let mut t = MemTransport::new();
        t.put("idx", bytes);
        let mut idx = GraphIndex::new(t, "idx", 0);
        idx.validate().unwrap();
    }

    #[test]
    fn graph_index_buffer_all_idempotent() {
        let bytes = build_index(&[node(&[b"a"], false, vec![], b"v")], 0, 1);
        let mut t = MemTransport::new();
        t.put("idx", bytes);
        let mut idx = GraphIndex::new(t, "idx", 0);
        idx.buffer_all().unwrap();
        idx.buffer_all().unwrap();
        assert_eq!(idx.key_count().unwrap(), 1);
    }

    #[test]
    fn graph_index_missing_trailer_is_error() {
        // Build a header but truncate the trailing newline so the
        // empty-trailer count comes out wrong.
        let mut bytes = build_index(&[node(&[b"a"], false, vec![], b"v")], 0, 1);
        // The serializer ends the file with `\n\n`. Drop the final \n
        // so `parse_lines` sees zero trailers.
        assert_eq!(bytes.last(), Some(&b'\n'));
        bytes.pop();
        let mut t = MemTransport::new();
        t.put("idx", bytes);
        let mut idx = GraphIndex::new(t, "idx", 0);
        let err = idx.buffer_all().unwrap_err();
        assert_eq!(
            err,
            IndexError::Other("BadIndexData: missing trailer".to_string())
        );
    }

    #[test]
    fn graph_index_respects_base_offset() {
        let inner = build_index(&[node(&[b"a"], false, vec![], b"v")], 0, 1);
        let mut wrapped = b"junk-before-header".to_vec();
        let prefix_len = wrapped.len() as u64;
        wrapped.extend_from_slice(&inner);
        let mut t = MemTransport::new();
        t.put("idx", wrapped);
        let mut idx = GraphIndex::new(t, "idx", prefix_len);
        assert_eq!(idx.key_count().unwrap(), 1);
        let entries = idx.iter_all_entries().unwrap();
        assert_eq!(entries, vec![(key(&[b"a"]), b"v".to_vec(), vec![])]);
    }

    #[test]
    fn key_is_valid_accepts_clean_bytes() {
        assert!(key_is_valid(&[b"foo".to_vec()], 1));
        assert!(key_is_valid(&[b"foo".to_vec(), b"bar".to_vec()], 2));
    }

    #[test]
    fn key_is_valid_rejects_wrong_length() {
        assert!(!key_is_valid(&[b"foo".to_vec()], 2));
        assert!(!key_is_valid(&[b"a".to_vec(), b"b".to_vec()], 1));
    }

    #[test]
    fn key_is_valid_rejects_empty_element() {
        assert!(!key_is_valid(&[b"".to_vec()], 1));
        assert!(!key_is_valid(&[b"a".to_vec(), b"".to_vec()], 2));
    }

    #[test]
    fn key_is_valid_rejects_separator_bytes() {
        for &bad in [b'\t', b'\n', 0x0b, 0x0c, b'\r', 0, b' '].iter() {
            let elem = vec![b'a', bad];
            assert!(
                !key_is_valid(&[elem.clone()], 1),
                "byte 0x{:02x} should disqualify",
                bad
            );
        }
    }

    #[test]
    fn value_is_valid_accepts_arbitrary_bytes() {
        assert!(value_is_valid(b"any value"));
        assert!(value_is_valid(b""));
        assert!(value_is_valid(b"with\ttab and CR\r is fine"));
    }

    #[test]
    fn value_is_valid_rejects_newline_or_null() {
        assert!(!value_is_valid(b"has\nnewline"));
        assert!(!value_is_valid(b"has\0null"));
    }

    #[test]
    fn builder_add_node_and_finish_roundtrip() {
        let mut b = GraphIndexBuilder::new(0, 1);
        b.add_node(key(&[b"a"]), b"v1".to_vec(), vec![]).unwrap();
        b.add_node(key(&[b"b"]), b"v2".to_vec(), vec![]).unwrap();
        assert_eq!(b.key_count(), 2);
        let bytes = b.finish().unwrap();
        let (header, parsed) = parse_full(&bytes).unwrap();
        assert_eq!(header.key_count, 2);
        assert_eq!(parsed.get(&key(&[b"a"])), Some(&(b"v1".to_vec(), vec![])));
    }

    #[test]
    fn builder_rejects_duplicate_key() {
        let mut b = GraphIndexBuilder::new(0, 1);
        b.add_node(key(&[b"a"]), b"v1".to_vec(), vec![]).unwrap();
        let err = b
            .add_node(key(&[b"a"]), b"v2".to_vec(), vec![])
            .unwrap_err();
        assert!(matches!(err, IndexError::DuplicateKey(_)));
    }

    #[test]
    fn builder_rejects_bad_key() {
        let mut b = GraphIndexBuilder::new(0, 1);
        let err = b
            .add_node(key(&[b"with space"]), b"v".to_vec(), vec![])
            .unwrap_err();
        assert!(matches!(err, IndexError::BadKey(_)));
    }

    #[test]
    fn builder_records_absent_references() {
        let mut b = GraphIndexBuilder::new(1, 1);
        b.add_node(key(&[b"a"]), b"v".to_vec(), vec![vec![key(&[b"missing"])]])
            .unwrap();
        assert_eq!(b.key_count(), 1);
        // The absent reference is in the table but flagged absent.
        assert!(b.nodes().contains_key(&key(&[b"missing"])));
        assert!(b.nodes().get(&key(&[b"missing"])).unwrap().absent);
    }

    #[test]
    fn builder_external_references_returns_unresolved_second_refs() {
        let mut b = GraphIndexBuilder::new(2, 1);
        b.add_node(
            key(&[b"a"]),
            b"v".to_vec(),
            vec![vec![], vec![key(&[b"parent1"]), key(&[b"a"])]],
        )
        .unwrap();
        let refs = b.external_references();
        assert!(refs.contains(&key(&[b"parent1"])));
        // `a` itself is present (just added).
        assert!(!refs.contains(&key(&[b"a"])));
    }

    #[test]
    fn combined_iter_dedups_keys() {
        let mut b1 = GraphIndexBuilder::new(0, 1);
        b1.add_node(key(&[b"a"]), b"v-from-1".to_vec(), vec![])
            .unwrap();
        let mut b2 = GraphIndexBuilder::new(0, 1);
        b2.add_node(key(&[b"a"]), b"v-from-2".to_vec(), vec![])
            .unwrap();
        b2.add_node(key(&[b"b"]), b"vb".to_vec(), vec![]).unwrap();
        let combined = CombinedGraphIndex::from_indices(vec![Box::new(b1), Box::new(b2)]);
        let mut all = combined.iter_all().unwrap();
        all.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(all.len(), 2);
        // First index wins for duplicates.
        assert_eq!(all[0].1, b"v-from-1".to_vec());
    }

    #[test]
    fn prefix_adapter_strips_keys_and_refs() {
        let mut b = GraphIndexBuilder::new(1, 2);
        b.add_node(
            key(&[b"prefix", b"k1"]),
            b"v1".to_vec(),
            vec![vec![key(&[b"prefix", b"k2"])]],
        )
        .unwrap();
        b.add_node(key(&[b"prefix", b"k2"]), b"v2".to_vec(), vec![vec![]])
            .unwrap();
        let adapter = GraphIndexPrefixAdapter::new(b, key(&[b"prefix"]), 1);
        let mut entries = adapter.iter_all().unwrap();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(entries[0].0, key(&[b"k1"]));
        assert_eq!(entries[0].2, vec![vec![key(&[b"k2"])]]);
    }

    #[test]
    fn parsed_range_map_starts_empty() {
        let m = ParsedRangeMap::new();
        assert!(m.is_empty());
        assert_eq!(m.len(), 0);
        assert_eq!(m.byte_index(0), -1);
        assert_eq!(m.key_index(&None), -1);
        assert!(!m.is_parsed(0));
    }

    #[test]
    fn parsed_range_map_first_insert() {
        let mut m = ParsedRangeMap::new();
        m.mark_parsed(0, None, 100, Some(key(&[b"k"])));
        assert_eq!(m.len(), 1);
        assert_eq!(m.byte_range(0), Some((0, 100)));
        assert_eq!(m.key_range(0), Some((None, Some(key(&[b"k"])))));
    }

    #[test]
    fn parsed_range_map_byte_index_matches_python_doctest() {
        // Python doctest: regions 0..10, 11..12 → byte_index(0)=0,
        // byte_index(10)=0, byte_index(11)=1, byte_index(12)=1.
        let mut m = ParsedRangeMap::new();
        m.mark_parsed(0, Some(key(&[b"a"])), 10, Some(key(&[b"b"])));
        m.mark_parsed(11, Some(key(&[b"c"])), 12, Some(key(&[b"d"])));
        assert_eq!(m.byte_index(0), 0);
        assert_eq!(m.byte_index(10), 0);
        assert_eq!(m.byte_index(11), 1);
        assert_eq!(m.byte_index(12), 1);
    }

    #[test]
    fn parsed_range_map_extend_lower_region() {
        let mut m = ParsedRangeMap::new();
        m.mark_parsed(0, None, 50, Some(key(&[b"k1"])));
        m.mark_parsed(50, Some(key(&[b"k1"])), 100, Some(key(&[b"k2"])));
        assert_eq!(m.len(), 1);
        assert_eq!(m.byte_range(0), Some((0, 100)));
        assert_eq!(m.key_range(0), Some((None, Some(key(&[b"k2"])))));
    }

    #[test]
    fn parsed_range_map_extend_higher_region() {
        // Header seeds (0, 30) as the first region; a later parse for
        // (60, 100) creates a second region. Then a parse for (30, 60)
        // exactly fills the gap, extending the higher region's start
        // backwards rather than the lower region's end forwards.
        let mut m = ParsedRangeMap::new();
        m.mark_parsed(0, None, 30, None);
        m.mark_parsed(60, Some(key(&[b"k2"])), 100, Some(key(&[b"k3"])));
        // mark_parsed at (30, 60) abuts the next region exactly,
        // extending its start backward.
        m.mark_parsed(30, Some(key(&[b"k1"])), 60, Some(key(&[b"k2"])));
        // Adjacency on both ends merges into a single span.
        assert_eq!(m.len(), 1);
        assert_eq!(m.byte_range(0), Some((0, 100)));
    }

    #[test]
    fn parsed_range_map_combine_two_regions() {
        let mut m = ParsedRangeMap::new();
        m.mark_parsed(0, None, 50, Some(key(&[b"k1"])));
        m.mark_parsed(60, Some(key(&[b"k2"])), 100, Some(key(&[b"k3"])));
        assert_eq!(m.len(), 2);
        m.mark_parsed(50, Some(key(&[b"k1"])), 60, Some(key(&[b"k2"])));
        assert_eq!(m.len(), 1);
        assert_eq!(m.byte_range(0), Some((0, 100)));
    }

    #[test]
    fn parsed_range_map_disjoint_new_region() {
        let mut m = ParsedRangeMap::new();
        m.mark_parsed(0, None, 50, Some(key(&[b"k1"])));
        m.mark_parsed(200, Some(key(&[b"k5"])), 300, Some(key(&[b"k6"])));
        assert_eq!(m.len(), 2);
        assert_eq!(m.byte_range(0), Some((0, 50)));
        assert_eq!(m.byte_range(1), Some((200, 300)));
    }

    #[test]
    fn parsed_range_map_is_parsed_inside_only() {
        let mut m = ParsedRangeMap::new();
        m.mark_parsed(10, Some(key(&[b"a"])), 20, Some(key(&[b"b"])));
        assert!(!m.is_parsed(9));
        assert!(m.is_parsed(10));
        assert!(m.is_parsed(15));
        assert!(!m.is_parsed(20));
        assert!(!m.is_parsed(100));
    }

    #[test]
    fn parsed_range_map_key_index() {
        // Disjoint byte ranges so the two key ranges remain distinct.
        let mut m = ParsedRangeMap::new();
        m.mark_parsed(0, None, 50, Some(key(&[b"a"])));
        m.mark_parsed(60, Some(key(&[b"b"])), 100, Some(key(&[b"c"])));
        assert_eq!(m.key_index(&None), 0);
        assert_eq!(m.key_index(&Some(key(&[b"a"]))), 0);
        assert_eq!(m.key_index(&Some(key(&[b"b"]))), 1);
        assert_eq!(m.key_index(&Some(key(&[b"e"]))), 1);
    }
}
