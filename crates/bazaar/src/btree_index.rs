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
}

impl std::fmt::Display for BTreeIndexError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BTreeIndexError::BadSignature => write!(f, "bad btree index format signature"),
            BTreeIndexError::BadOptions => write!(f, "bad btree index options"),
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
}
