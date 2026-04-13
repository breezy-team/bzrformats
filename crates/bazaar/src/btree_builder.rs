//! B+Tree graph index builder.
//!
//! Port of `bzrformats.btree_index.BTreeBuilder`. See that module's docstring
//! for the wire format. This implementation supports building indexes that
//! fit in memory (no spill-to-disk) and produces byte-identical output to the
//! Python original for the common cases: empty indexes and single-leaf-row
//! trees. Multi-row trees are also supported via the propagation logic in
//! `add_key`.
//!
//! The caller can feed `(key, value, references)` tuples via [`BTreeBuilder::add_node`]
//! and then call [`BTreeBuilder::finish`] to get the serialised bytes.

use crate::chunk_writer::ChunkWriter;
use std::collections::BTreeMap;

/// Key type: an ordered sequence of byte segments.
pub type Key = Vec<Vec<u8>>;

/// One in-memory node: `(references, value)`. References have one list per
/// configured reference list.
#[derive(Debug, Clone)]
pub struct Node {
    pub references: Vec<Vec<Key>>,
    pub value: Vec<u8>,
}

#[derive(Debug)]
pub enum Error {
    DuplicateKey(Key),
    BadKey(Key, String),
    BadValue(Vec<u8>),
    BadReference(Key),
    KeyTooBig(Key),
    InconsistentKeyLength,
    InconsistentRefListCount,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::DuplicateKey(k) => write!(f, "duplicate key: {:?}", k),
            Error::BadKey(k, reason) => write!(f, "bad key {:?}: {}", k, reason),
            Error::BadValue(v) => write!(f, "bad value: {:?}", v),
            Error::BadReference(k) => write!(f, "bad reference: {:?}", k),
            Error::KeyTooBig(k) => write!(f, "key does not fit in one node: {:?}", k),
            Error::InconsistentKeyLength => write!(f, "inconsistent key length"),
            Error::InconsistentRefListCount => {
                write!(f, "inconsistent reference_lists count")
            }
        }
    }
}

impl std::error::Error for Error {}

const BT_SIGNATURE: &[u8] = b"B+Tree Graph Index 2\n";
const RESERVED_HEADER_BYTES: usize = 120;
const PAGE_SIZE: usize = 4096;
const LEAF_FLAG: &[u8] = b"type=leaf\n";
const INTERNAL_FLAG: &[u8] = b"type=internal\n";
const INTERNAL_OFFSET: &[u8] = b"offset=";

pub struct BTreeBuilder {
    reference_lists: usize,
    key_length: usize,
    optimize_for_size: bool,
    nodes: BTreeMap<Key, Node>,
}

impl BTreeBuilder {
    pub fn new(reference_lists: usize, key_elements: usize) -> Self {
        assert!(key_elements >= 1, "key_elements must be >= 1");
        Self {
            reference_lists,
            key_length: key_elements,
            optimize_for_size: false,
            nodes: BTreeMap::new(),
        }
    }

    pub fn set_optimize_for_size(&mut self, v: bool) {
        self.optimize_for_size = v;
    }

    pub fn key_count(&self) -> usize {
        self.nodes.len()
    }

    /// Add a single node.
    pub fn add_node(
        &mut self,
        key: Key,
        value: Vec<u8>,
        references: Vec<Vec<Key>>,
    ) -> Result<(), Error> {
        self.check_key_ref_value(&key, &references, &value)?;
        if self.nodes.contains_key(&key) {
            return Err(Error::DuplicateKey(key));
        }
        self.nodes.insert(key, Node { references, value });
        Ok(())
    }

    fn check_key_ref_value(
        &self,
        key: &Key,
        references: &[Vec<Key>],
        value: &[u8],
    ) -> Result<(), Error> {
        if key.len() != self.key_length {
            return Err(Error::InconsistentKeyLength);
        }
        if references.len() != self.reference_lists {
            return Err(Error::InconsistentRefListCount);
        }
        for segment in key {
            if segment.iter().any(|b| *b == b'\x00' || *b == b'\n') {
                return Err(Error::BadKey(
                    key.clone(),
                    "key segments must not contain \\x00 or \\n".to_string(),
                ));
            }
        }
        for b in value {
            if *b == 0 || *b == b'\n' {
                return Err(Error::BadValue(value.to_vec()));
            }
        }
        for ref_list in references {
            for reference in ref_list {
                if reference.len() != self.key_length {
                    return Err(Error::BadReference(reference.clone()));
                }
            }
        }
        Ok(())
    }

    /// Produce the serialised B+Tree bytes.
    pub fn finish(&self) -> Result<Vec<u8>, Error> {
        let node_iter: Vec<(Key, Node)> = self
            .nodes
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        write_nodes(
            &node_iter,
            self.reference_lists,
            self.key_length,
            self.optimize_for_size,
        )
    }
}

/// Flatten one `(key, value, references)` node into `(string_key, line_bytes)`.
///
/// Matches `_btree_serializer._flatten_node`:
///
/// * `string_key` is the key segments joined with `\0`.
/// * `line_bytes` is `string_key \0 refs_bytes \0 value \n`, where refs are
///   tab-separated lists of `\r`-separated references whose segments are
///   `\0`-joined.
pub fn flatten_node(
    key: &Key,
    value: &[u8],
    references: &[Vec<Key>],
    reference_lists: bool,
) -> (Vec<u8>, Vec<u8>) {
    let mut string_key = Vec::new();
    for (i, seg) in key.iter().enumerate() {
        if i > 0 {
            string_key.push(0);
        }
        string_key.extend_from_slice(seg);
    }

    let mut refs_bytes = Vec::new();
    if reference_lists {
        for (rl_idx, ref_list) in references.iter().enumerate() {
            if rl_idx > 0 {
                refs_bytes.push(b'\t');
            }
            for (ref_idx, reference) in ref_list.iter().enumerate() {
                if ref_idx > 0 {
                    refs_bytes.push(b'\r');
                }
                for (k_idx, seg) in reference.iter().enumerate() {
                    if k_idx > 0 {
                        refs_bytes.push(0);
                    }
                    refs_bytes.extend_from_slice(seg);
                }
            }
        }
    }

    let mut line = Vec::with_capacity(string_key.len() + refs_bytes.len() + value.len() + 3);
    line.extend_from_slice(&string_key);
    line.push(0);
    line.extend_from_slice(&refs_bytes);
    line.push(0);
    line.extend_from_slice(value);
    line.push(b'\n');
    (string_key, line)
}

struct BuilderRow {
    /// Pages already finished for this row, each exactly `PAGE_SIZE` bytes.
    /// The first PAGE_SIZE bytes of this buffer have the reserved header
    /// bytes padded at the front so the caller can patch them later.
    spool: Vec<u8>,
    /// Current open ChunkWriter for the in-progress node.
    writer: Option<ChunkWriter>,
    /// Number of nodes finished so far.
    nodes: usize,
    /// True for internal (non-leaf) rows; they must always pad.
    is_internal: bool,
}

impl BuilderRow {
    fn new(is_internal: bool) -> Self {
        Self {
            spool: Vec::new(),
            writer: None,
            nodes: 0,
            is_internal,
        }
    }

    fn finish_node(&mut self, pad: bool) {
        if self.is_internal {
            assert!(pad, "internal rows must be padded");
        }
        let writer = self
            .writer
            .take()
            .expect("finish_node called with no open writer");
        let finished = writer.finish();
        if self.nodes == 0 {
            // Reserve the header bytes at the very start of the first page.
            self.spool.extend_from_slice(&[0u8; RESERVED_HEADER_BYTES]);
        }
        let mut byte_lines = finished.bytes_list;
        let mut skipped_bytes = 0usize;
        if !pad && finished.nulls_needed > 0 {
            byte_lines.pop();
            skipped_bytes = finished.nulls_needed;
        }
        for b in &byte_lines {
            self.spool.extend_from_slice(b);
        }
        let remainder = (self.spool.len() + skipped_bytes) % PAGE_SIZE;
        assert_eq!(
            remainder,
            0,
            "incorrect node length: {}, {}",
            self.spool.len(),
            remainder
        );
        self.nodes += 1;
    }
}

fn write_nodes(
    node_iter: &[(Key, Node)],
    reference_lists: usize,
    key_length: usize,
    optimize_for_size: bool,
) -> Result<Vec<u8>, Error> {
    let mut rows: Vec<BuilderRow> = Vec::new();
    let mut key_count = 0usize;
    for (key, node) in node_iter {
        if key_count == 0 {
            rows.push(BuilderRow::new(false));
        }
        key_count += 1;
        let (string_key, line) =
            flatten_node(key, &node.value, &node.references, reference_lists > 0);
        add_key(
            &string_key,
            &line,
            &mut rows,
            optimize_for_size,
            /*allow_optimize=*/ true,
        )?;
    }
    // Finish every row that still has an open writer, in reverse so the leaf
    // finishes before its internal rows.
    let rows_len = rows.len();
    for (idx, row) in rows.iter_mut().enumerate().rev() {
        let pad = idx < rows_len - 1 || row.is_internal;
        if row.writer.is_some() {
            row.finish_node(pad);
        }
    }

    // Header lines.
    let mut header = Vec::new();
    header.extend_from_slice(BT_SIGNATURE);
    header.extend_from_slice(format!("node_ref_lists={}\n", reference_lists).as_bytes());
    header.extend_from_slice(format!("key_elements={}\n", key_length).as_bytes());
    header.extend_from_slice(format!("len={}\n", key_count).as_bytes());
    let row_lengths: Vec<usize> = rows.iter().map(|r| r.nodes).collect();
    let row_lengths_str = row_lengths
        .iter()
        .map(|n| n.to_string())
        .collect::<Vec<_>>()
        .join(",");
    header.extend_from_slice(b"row_lengths=");
    header.extend_from_slice(row_lengths_str.as_bytes());
    header.push(b'\n');
    assert!(
        header.len() <= RESERVED_HEADER_BYTES,
        "Could not fit the header in the reserved space: {} > {}",
        header.len(),
        RESERVED_HEADER_BYTES
    );

    let mut result = header;
    let header_len = result.len();
    // Now write each row. The first page of the *first* row has its header
    // bytes replaced with the header we just wrote (plus `reserved - position`
    // bytes of padding zeros to fill the reserved region out to
    // RESERVED_HEADER_BYTES). For single-page rows the tail of the first
    // (and only) page may be unpadded for leaves.
    let mut first_row = true;
    let mut position = header_len;
    for row in &rows {
        if row.spool.is_empty() {
            continue;
        }
        // The first page of this row: copy `spool[RESERVED_HEADER_BYTES..min(PAGE_SIZE, spool.len())]`
        // (skipping the reserved header placeholder).
        let first_page_end = std::cmp::min(PAGE_SIZE, row.spool.len());
        result.extend_from_slice(&row.spool[RESERVED_HEADER_BYTES..first_page_end]);
        if first_row && row.spool.len() >= PAGE_SIZE {
            // Pad the tail of the first page out so `position..RESERVED_HEADER_BYTES`
            // is all zero.
            assert!(position <= RESERVED_HEADER_BYTES);
            let pad = RESERVED_HEADER_BYTES - position;
            result.extend_from_slice(&vec![0u8; pad]);
        }
        // Remaining pages of this row, each exactly PAGE_SIZE.
        if row.spool.len() > PAGE_SIZE {
            result.extend_from_slice(&row.spool[PAGE_SIZE..]);
        }
        position = 0;
        first_row = false;
    }
    Ok(result)
}

fn add_key(
    string_key: &[u8],
    line: &[u8],
    rows: &mut Vec<BuilderRow>,
    optimize_for_size: bool,
    allow_optimize: bool,
) -> Result<(), Error> {
    let mut new_leaf = false;
    // Ensure the leaf (and any internal rows above with no writer) have an
    // open writer.
    if rows.last().unwrap().writer.is_none() {
        new_leaf = true;
        let rows_len = rows.len();
        for pos in 0..rows_len - 1 {
            if rows[pos].writer.is_none() {
                let length = if rows[pos].nodes == 0 {
                    PAGE_SIZE - RESERVED_HEADER_BYTES
                } else {
                    PAGE_SIZE
                };
                let opt = if allow_optimize {
                    optimize_for_size
                } else {
                    false
                };
                let mut writer = ChunkWriter::new(length, 0, opt);
                let _ = writer.write(INTERNAL_FLAG, false);
                let offset_line = format!("offset={}\n", rows[pos + 1].nodes);
                let _ = writer.write(offset_line.as_bytes(), false);
                rows[pos].writer = Some(writer);
            }
        }
        let leaf_idx = rows_len - 1;
        let length = if rows[leaf_idx].nodes == 0 {
            PAGE_SIZE - RESERVED_HEADER_BYTES
        } else {
            PAGE_SIZE
        };
        let mut writer = ChunkWriter::new(length, 0, optimize_for_size);
        let _ = writer.write(LEAF_FLAG, false);
        rows[leaf_idx].writer = Some(writer);
    }
    let overflow = rows
        .last_mut()
        .unwrap()
        .writer
        .as_mut()
        .unwrap()
        .write(line, false);
    if overflow {
        if new_leaf {
            return Err(Error::KeyTooBig(
                string_key.split(|b| *b == 0).map(|s| s.to_vec()).collect(),
            ));
        }
        // The leaf is full; finish it and propagate the divider key upwards.
        let leaf_last = rows.len() - 1;
        rows[leaf_last].finish_node(false);
        let mut key_line = string_key.to_vec();
        key_line.push(b'\n');
        let mut new_row_needed = true;
        for pos in (0..rows.len() - 1).rev() {
            let writer = rows[pos].writer.as_mut().unwrap();
            let overflow = writer.write(&key_line, false);
            if overflow {
                rows[pos].finish_node(true);
            } else {
                new_row_needed = false;
                break;
            }
        }
        if new_row_needed {
            // Insert a new root.
            let mut new_row = BuilderRow::new(true);
            let mut writer =
                ChunkWriter::new(PAGE_SIZE - RESERVED_HEADER_BYTES, 0, optimize_for_size);
            let _ = writer.write(INTERNAL_FLAG, false);
            let offset_line = format!("offset={}\n", rows[0].nodes - 1);
            let _ = writer.write(offset_line.as_bytes(), false);
            let _ = writer.write(&key_line, false);
            new_row.writer = Some(writer);
            rows.insert(0, new_row);
        }
        return add_key(string_key, line, rows, optimize_for_size, allow_optimize);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::read::ZlibDecoder;
    use std::io::Read;

    #[test]
    fn empty_1_0_matches_python_header() {
        let builder = BTreeBuilder::new(0, 1);
        let content = builder.finish().unwrap();
        assert_eq!(
            content,
            b"B+Tree Graph Index 2\nnode_ref_lists=0\nkey_elements=1\nlen=0\nrow_lengths=\n"
                .to_vec()
        );
    }

    #[test]
    fn empty_2_1_matches_python_header() {
        let builder = BTreeBuilder::new(1, 2);
        let content = builder.finish().unwrap();
        assert_eq!(
            content,
            b"B+Tree Graph Index 2\nnode_ref_lists=1\nkey_elements=2\nlen=0\nrow_lengths=\n"
                .to_vec()
        );
    }

    fn pos_to_key(pos: usize, lead: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(lead);
        let digit = format!("{}", pos).into_bytes();
        for _ in 0..40 {
            out.extend_from_slice(&digit);
        }
        out
    }

    #[test]
    fn root_leaf_1_0_round_trips_five_keys() {
        // Mirrors test_btree_index.test_root_leaf_1_0 except we check the
        // serialised node content (not the Python tempfile plumbing).
        let mut builder = BTreeBuilder::new(0, 1);
        for i in 0..5 {
            let key = vec![pos_to_key(i, b"")];
            let value = format!("value:{}", i).into_bytes();
            builder.add_node(key, value, vec![]).unwrap();
        }
        let content = builder.finish().unwrap();
        let header =
            b"B+Tree Graph Index 2\nnode_ref_lists=0\nkey_elements=1\nlen=5\nrow_lengths=1\n";
        assert_eq!(&content[..header.len()], header);
        // The compressed leaf follows the header.
        let node_content = &content[header.len()..];
        let mut decoder = ZlibDecoder::new(node_content);
        let mut node_bytes = Vec::new();
        decoder.read_to_end(&mut node_bytes).unwrap();
        // Decompressed payload should have the leaf flag and five entries.
        assert!(node_bytes.starts_with(b"type=leaf\n"));
        for i in 0..5 {
            let digit = format!("{}", i);
            let mut line = Vec::new();
            for _ in 0..40 {
                line.extend_from_slice(digit.as_bytes());
            }
            line.extend_from_slice(format!("\x00\x00value:{}\n", i).as_bytes());
            assert!(
                node_bytes.windows(line.len()).any(|w| w == line.as_slice()),
                "missing line for index {}",
                i
            );
        }
    }

    #[test]
    fn root_leaf_1_0_decompresses_byte_exact() {
        // The expected decompressed leaf contents should match Python's
        // exactly since the input nodes are deterministic.
        let mut builder = BTreeBuilder::new(0, 1);
        for i in 0..5 {
            let key = vec![pos_to_key(i, b"")];
            let value = format!("value:{}", i).into_bytes();
            builder.add_node(key, value, vec![]).unwrap();
        }
        let content = builder.finish().unwrap();
        let header =
            b"B+Tree Graph Index 2\nnode_ref_lists=0\nkey_elements=1\nlen=5\nrow_lengths=1\n";
        let node_content = &content[header.len()..];
        let mut decoder = ZlibDecoder::new(node_content);
        let mut node_bytes = Vec::new();
        decoder.read_to_end(&mut node_bytes).unwrap();
        let expected = concat_expected(&[
            b"type=leaf\n",
            b"0000000000000000000000000000000000000000\x00\x00value:0\n",
            b"1111111111111111111111111111111111111111\x00\x00value:1\n",
            b"2222222222222222222222222222222222222222\x00\x00value:2\n",
            b"3333333333333333333333333333333333333333\x00\x00value:3\n",
            b"4444444444444444444444444444444444444444\x00\x00value:4\n",
        ]);
        assert_eq!(node_bytes, expected);
    }

    fn concat_expected(parts: &[&[u8]]) -> Vec<u8> {
        let mut out = Vec::new();
        for p in parts {
            out.extend_from_slice(p);
        }
        out
    }

    #[test]
    fn flatten_node_without_references() {
        let key = vec![b"file-id".to_vec()];
        let value = b"val";
        let (string_key, line) = flatten_node(&key, value, &[], false);
        assert_eq!(string_key, b"file-id");
        assert_eq!(line, b"file-id\x00\x00val\n");
    }

    #[test]
    fn flatten_node_with_references() {
        let key = vec![b"f".to_vec(), b"r".to_vec()];
        let value = b"value:0";
        let references = vec![vec![
            vec![b"f".to_vec(), b"p1".to_vec()],
            vec![b"f".to_vec(), b"p2".to_vec()],
        ]];
        let (string_key, line) = flatten_node(&key, value, &references, true);
        assert_eq!(string_key, b"f\x00r");
        assert_eq!(line, b"f\x00r\x00f\x00p1\rf\x00p2\x00value:0\n");
    }
}
