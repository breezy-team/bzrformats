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

/// A read-only graph index opened on a [`IndexTransport`]-backed file.
///
/// This is the full-load implementation: the first method that needs
/// data calls [`GraphIndex::buffer_all`], which reads the entire file
/// into memory and resolves every node's reference offsets to keys. The
/// bisection-driven partial-read path stays in Python for now.
pub struct GraphIndex<T: IndexTransport> {
    transport: T,
    name: String,
    base_offset: u64,
    /// Parsed node table — `key -> (value, resolved reference lists)`.
    /// `None` until [`GraphIndex::buffer_all`] has been called.
    nodes: Option<HashMap<IndexKey, NodeBody>>,
    /// Header metadata. `None` until the file has been read at least
    /// once.
    header: Option<IndexHeader>,
}

impl<T: IndexTransport> GraphIndex<T> {
    /// Open an index on `transport` at `name`. Pass `base_offset` if the
    /// index lives at a non-zero offset within the underlying file (the
    /// pack-file case).
    pub fn new(transport: T, name: impl Into<String>, base_offset: u64) -> Self {
        Self {
            transport,
            name: name.into(),
            base_offset,
            nodes: None,
            header: None,
        }
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

    /// Number of keys in the index. Triggers a full load on first call.
    pub fn key_count(&mut self) -> Result<usize, IndexError> {
        if let Some(h) = &self.header {
            return Ok(h.key_count);
        }
        self.buffer_all()?;
        Ok(self
            .header
            .as_ref()
            .expect("header set by buffer_all")
            .key_count)
    }

    /// Number of parallel reference lists each present node carries.
    /// Triggers a full load on first call.
    pub fn node_ref_lists(&mut self) -> Result<usize, IndexError> {
        if let Some(h) = &self.header {
            return Ok(h.node_ref_lists);
        }
        self.buffer_all()?;
        Ok(self
            .header
            .as_ref()
            .expect("header set by buffer_all")
            .node_ref_lists)
    }

    /// Number of bytestrings in each key tuple. Triggers a full load on
    /// first call.
    pub fn key_length(&mut self) -> Result<usize, IndexError> {
        if let Some(h) = &self.header {
            return Ok(h.key_length);
        }
        self.buffer_all()?;
        Ok(self
            .header
            .as_ref()
            .expect("header set by buffer_all")
            .key_length)
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
}
