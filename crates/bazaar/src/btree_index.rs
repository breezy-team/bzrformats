//! B+Tree graph index format helpers.
//!
//! Port of the pure-logic header/internal-node parsers from
//! `bzrformats/btree_index.py`. The stateful builder, reader, and leaf
//! parser (`_btree_serializer`) live elsewhere; this module only covers
//! the small byte-level helpers that don't need to touch transport or
//! page cache state.

/// Magic signature written at the start of every B+Tree graph index.
pub const BTREE_SIGNATURE: &[u8] = b"B+Tree Graph Index 2\n";
pub const OPTION_NODE_REFS: &[u8] = b"node_ref_lists=";
pub const OPTION_KEY_ELEMENTS: &[u8] = b"key_elements=";
pub const OPTION_LEN: &[u8] = b"len=";
pub const OPTION_ROW_LENGTHS: &[u8] = b"row_lengths=";

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
}
