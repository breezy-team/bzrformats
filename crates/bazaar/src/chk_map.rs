//! Persistent maps from tuple_of_strings->string using CHK stores.
//!
//! Overview and current status:
//!
//! The CHKMap class implements a dict from tuple_of_strings->string by using a trie
//! with internal nodes of 8-bit fan out; The key tuples are mapped to strings by
//! joining them by \x00, and \x00 padding shorter keys out to the length of the
//! longest key. Leaf nodes are packed as densely as possible, and internal nodes
//! are all an additional 8-bits wide leading to a sparse upper tree.
//!
//! Updates to a CHKMap are done preferentially via the apply_delta method, to
//! allow optimisation of the update operation; but individual map/unmap calls are
//! possible and supported. Individual changes via map/unmap are buffered in memory
//! until the _save method is called to force serialisation of the tree.
//! apply_delta records its changes immediately by performing an implicit _save.
//!
//! # Todo
//!
//! Densely packed upper nodes.

use crc32fast::Hasher;

use std::fmt::Write;
use std::hash::Hash;
use std::iter::zip;

fn crc32(bit: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(bit);
    hasher.finalize()
}

pub type SerialisedKey = Vec<u8>;

pub type SearchKeyFn = fn(&Key) -> SerializedKey;

/// Map the key tuple into a search string that just uses the key bytes.
pub fn search_key_plain(key: &Key) -> SerializedKey {
    key.0.join(&b'\x00')
}

pub fn search_key_16(key: &Key) -> SerializedKey {
    let mut result = String::new();
    for bit in key.iter() {
        write!(&mut result, "{:08X}\x00", crc32(bit)).unwrap();
    }
    result.pop();
    result.as_bytes().to_vec()
}

pub fn search_key_255(key: &Key) -> SerializedKey {
    let mut result = vec![];
    for bit in key.iter() {
        let crc = crc32(bit);
        let crc_bytes = crc.to_be_bytes();
        result.extend(crc_bytes);
        result.push(0x00);
    }
    result.pop();
    result
        .iter()
        .map(|b| if *b == 0x0A { b'_' } else { *b })
        .collect()
}

pub fn bytes_to_text_key(data: &[u8]) -> Result<(&[u8], &[u8]), String> {
    let sections: Vec<&[u8]> = data.split(|&byte| byte == b'\n').collect();

    let delimiter_position = sections[0].windows(2).position(|window| window == b": ");

    if delimiter_position.is_none() {
        return Err("Invalid key file".to_string());
    }

    let (_kind, file_id) = sections[0].split_at(delimiter_position.unwrap() + 2);

    Ok((file_id, sections[3]))
}

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub struct Key(Vec<Vec<u8>>);

impl From<Vec<Vec<u8>>> for Key {
    fn from(v: Vec<Vec<u8>>) -> Self {
        Key(v)
    }
}

impl Key {
    pub fn serialize(&self) -> SerializedKey {
        let mut result = vec![];
        for bit in self.0.iter() {
            result.extend(bit);
            result.push(0x00);
        }
        result.pop();
        result
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &[u8]> {
        self.0.iter().map(|v| v.as_slice())
    }
}

impl std::ops::Index<usize> for Key {
    type Output = Vec<u8>;

    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl std::fmt::Display for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut first = true;
        for bit in &self.0 {
            if !first {
                write!(f, "/")?;
            }
            first = false;
            write!(f, "{}", String::from_utf8_lossy(bit))?;
        }
        Ok(())
    }
}

pub type SerializedKey = Vec<u8>;

pub type Value = Vec<u8>;

#[derive(Debug)]
pub enum Error {
    InconsistentDeltaDelta(Vec<(Option<Key>, Option<Key>, Value)>, String),
    DeserializeError(String),
}

impl From<std::num::ParseIntError> for Error {
    fn from(e: std::num::ParseIntError) -> Self {
        Error::DeserializeError(format!("Failed to parse int: {}", e))
    }
}

/// Given 2 strings, return the longest prefix common to both.
///
/// # Arguments
/// * `prefix` - This has been the common prefix for other keys, so it is more likely to be the common prefix in this case as well.
/// * `key` - Another string to compare to.
pub fn common_prefix_pair<'b>(prefix: &[u8], key: &'b [u8]) -> &'b [u8] {
    if key.starts_with(prefix) {
        return &key[..prefix.len()];
    }
    let mut p = 0;
    // Is there a better way to do this?
    for (left, right) in zip(prefix, key) {
        if left != right {
            break;
        }
        p += 1;
    }

    let p = p as usize;
    &key[..p]
}

#[test]
fn test_common_prefix_pair() {
    assert_eq!(common_prefix_pair(b"abc", b"abc"), b"abc");
    assert_eq!(common_prefix_pair(b"abc", b"abcd"), b"abc");
    assert_eq!(common_prefix_pair(b"abc", b"ab"), b"ab");
    assert_eq!(common_prefix_pair(b"abc", b"bbd"), b"");
    assert_eq!(common_prefix_pair(b"", b"bbc"), b"");
    assert_eq!(common_prefix_pair(b"abc", b""), b"");
}

/// Given a list of keys, find their common prefix.
///
/// # Arguments
/// * `keys`: An iterable of strings.
///
/// # Returns
/// The longest common prefix of all keys.
pub fn common_prefix_many<'a>(mut keys: impl Iterator<Item = &'a [u8]> + 'a) -> Option<&'a [u8]> {
    let mut cp = keys.next()?;
    for key in keys {
        cp = common_prefix_pair(cp, key);
        if cp.is_empty() {
            // if common_prefix is the empty string, then we know it won't
            // change further
            break;
        }
    }
    Some(cp)
}

/// Parsed contents of a serialised CHK leaf node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedLeafNode {
    pub maximum_size: usize,
    pub key_width: usize,
    pub length: usize,
    /// Common serialised prefix applied to every key line before splitting.
    /// Empty means there was no prefix line (or it was genuinely empty).
    pub common_serialised_prefix: Vec<u8>,
    /// (key_tuple, value) pairs in the order they appear in the serialised
    /// form — the caller is responsible for placing them in a dict.
    pub items: Vec<(Vec<Vec<u8>>, Vec<u8>)>,
    /// Matches `LeafNode._raw_size` as computed by the Python parser:
    /// `sum(len(l) for l in lines[5:]) + length*len(prefix) + (len(lines)-5)`.
    pub raw_size: usize,
}

/// Deserialise the serialised form of a CHK leaf node.
pub fn deserialise_leaf_node(data: &[u8]) -> Result<ParsedLeafNode, Error> {
    // Python does `data.split(b"\n")` which yields an empty trailing element
    // for a final newline; the parser insists on exactly that.
    let mut lines: Vec<&[u8]> = data.split(|&b| b == b'\n').collect();
    let trailing = lines
        .pop()
        .ok_or_else(|| Error::DeserializeError("empty leaf node body".into()))?;
    if !trailing.is_empty() {
        return Err(Error::DeserializeError(
            "leaf node did not end with final newline".into(),
        ));
    }
    if lines.len() < 5 {
        return Err(Error::DeserializeError(
            "leaf node truncated before item lines".into(),
        ));
    }
    if lines[0] != b"chkleaf:" {
        return Err(Error::DeserializeError("not a serialised leaf node".into()));
    }
    let maximum_size = parse_decimal(lines[1], "maximum_size")?;
    let width = parse_decimal(lines[2], "key_width")?;
    let length = parse_decimal(lines[3], "length")?;
    let prefix = lines[4];

    let mut items: Vec<(Vec<Vec<u8>>, Vec<u8>)> = Vec::with_capacity(length);
    let mut pos = 5usize;
    while pos < lines.len() {
        // Reconstitute the full key line by prepending the common prefix,
        // then split on NUL to recover the key elements + final count.
        let mut full = Vec::with_capacity(prefix.len() + lines[pos].len());
        full.extend_from_slice(prefix);
        full.extend_from_slice(lines[pos]);
        pos += 1;

        let mut elements: Vec<Vec<u8>> = full.split(|&b| b == 0).map(|s| s.to_vec()).collect();
        if elements.len() != width + 1 {
            return Err(Error::DeserializeError(format!(
                "incorrect number of elements ({} vs {}) for leaf line",
                elements.len(),
                width + 1
            )));
        }
        let count_bytes = elements.pop().expect("just checked non-empty");
        let num_value_lines = parse_decimal(&count_bytes, "value line count")?;
        if pos + num_value_lines > lines.len() {
            return Err(Error::DeserializeError(
                "leaf node value line runs past end of body".into(),
            ));
        }
        let value_lines = &lines[pos..pos + num_value_lines];
        pos += num_value_lines;
        // Join the value lines with literal '\n' to reconstruct the value.
        let value_len =
            value_lines.iter().map(|l| l.len()).sum::<usize>() + num_value_lines.saturating_sub(1);
        let mut value = Vec::with_capacity(value_len);
        for (i, line) in value_lines.iter().enumerate() {
            if i > 0 {
                value.push(b'\n');
            }
            value.extend_from_slice(line);
        }
        items.push((elements, value));
    }
    if items.len() != length {
        return Err(Error::DeserializeError(format!(
            "item count ({}) mismatch: found {}",
            length,
            items.len()
        )));
    }

    // Reproduce LeafNode._raw_size exactly (see the Python implementation).
    let suffix_bytes: usize = lines[5..].iter().map(|l| l.len()).sum();
    let raw_size = suffix_bytes + length * prefix.len() + (lines.len() - 5);

    Ok(ParsedLeafNode {
        maximum_size,
        key_width: width,
        length,
        common_serialised_prefix: prefix.to_vec(),
        items,
        raw_size,
    })
}

/// Parsed contents of a serialised CHK internal node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedInternalNode {
    pub maximum_size: usize,
    pub key_width: usize,
    pub length: usize,
    pub search_prefix: Vec<u8>,
    /// (reconstructed_prefix, child_sha1_key) pairs in file order.
    pub items: Vec<(Vec<u8>, Vec<u8>)>,
    /// Length of the last parsed prefix — matches how Python's loop variable
    /// leaks out into `InternalNode._node_width`.
    pub node_width: usize,
}

/// Deserialise the serialised form of a CHK internal node.
pub fn deserialise_internal_node(data: &[u8]) -> Result<ParsedInternalNode, Error> {
    let mut lines: Vec<&[u8]> = data.split(|&b| b == b'\n').collect();
    let trailing = lines
        .pop()
        .ok_or_else(|| Error::DeserializeError("empty internal node body".into()))?;
    if !trailing.is_empty() {
        return Err(Error::DeserializeError("last line must be ''".into()));
    }
    if lines.len() < 5 {
        return Err(Error::DeserializeError(
            "internal node truncated before item lines".into(),
        ));
    }
    if lines[0] != b"chknode:" {
        return Err(Error::DeserializeError(
            "not a serialised internal node".into(),
        ));
    }
    let maximum_size = parse_decimal(lines[1], "maximum_size")?;
    let width = parse_decimal(lines[2], "key_width")?;
    let length = parse_decimal(lines[3], "length")?;
    let common_prefix = lines[4];

    let mut items: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut last_prefix_len = 0usize;
    for suffix in &lines[5..] {
        let mut full = Vec::with_capacity(common_prefix.len() + suffix.len());
        full.extend_from_slice(common_prefix);
        full.extend_from_slice(suffix);
        let split_at = full
            .iter()
            .rposition(|&b| b == 0)
            .ok_or_else(|| Error::DeserializeError("internal node line missing NUL".into()))?;
        let prefix = full[..split_at].to_vec();
        let flat_key = full[split_at + 1..].to_vec();
        last_prefix_len = prefix.len();
        items.push((prefix, flat_key));
    }
    if items.is_empty() {
        return Err(Error::DeserializeError(
            "internal node contained no items".into(),
        ));
    }

    Ok(ParsedInternalNode {
        maximum_size,
        key_width: width,
        length,
        search_prefix: common_prefix.to_vec(),
        items,
        node_width: last_prefix_len,
    })
}

fn parse_decimal(bytes: &[u8], what: &str) -> Result<usize, Error> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .ok_or_else(|| Error::DeserializeError(format!("invalid {}: {:?}", what, bytes)))
}

#[test]
fn test_common_prefix_many() {
    assert_eq!(
        common_prefix_many(vec![&b"abc"[..], &b"abc"[..]].into_iter()),
        Some(&b"abc"[..])
    );
    assert_eq!(
        common_prefix_many(vec![&b"abc"[..], &b"abcd"[..]].into_iter()),
        Some(&b"abc"[..])
    );
    assert_eq!(
        common_prefix_many(vec![&b"abc"[..], &b"ab"[..]].into_iter()),
        Some(&b"ab"[..])
    );
    assert_eq!(
        common_prefix_many(vec![&b"abc"[..], &b"bbd"[..]].into_iter()),
        Some(&b""[..])
    );
    assert_eq!(
        common_prefix_many(vec![&b"abcd"[..], &b"abc"[..], &b"abc"[..]].into_iter()),
        Some(&b"abc"[..])
    );
    assert_eq!(common_prefix_many(vec![].into_iter()), None);
}

#[cfg(test)]
mod deserialise_tests {
    use super::*;

    // Fixture generated from the real Python serialiser: a leaf with
    // _maximum_size=100, key_width=1, and two items whose keys share
    // the common prefix "alph". Cross-checked in the session probe.
    const LEAF_FIXTURE: &[u8] = b"chkleaf:\n100\n1\n2\nalph\n2\x002\nv2\nv2line2\na\x001\nv1\n";

    #[test]
    fn deserialise_leaf_fixture_items_match() {
        let p = deserialise_leaf_node(LEAF_FIXTURE).unwrap();
        assert_eq!(p.maximum_size, 100);
        assert_eq!(p.key_width, 1);
        assert_eq!(p.length, 2);
        assert_eq!(p.common_serialised_prefix, b"alph");
        assert_eq!(p.items.len(), 2);
        // Order matches file order, not sorted order.
        assert_eq!(p.items[0].0, vec![b"alph2".to_vec()]);
        assert_eq!(p.items[0].1, b"v2\nv2line2");
        assert_eq!(p.items[1].0, vec![b"alpha".to_vec()]);
        assert_eq!(p.items[1].1, b"v1");
    }

    #[test]
    fn deserialise_leaf_raw_size_matches_python_formula() {
        // Cross-checked against LeafNode._raw_size: 30.
        let p = deserialise_leaf_node(LEAF_FIXTURE).unwrap();
        assert_eq!(p.raw_size, 30);
    }

    #[test]
    fn deserialise_leaf_empty_items() {
        // length=0, no item lines. Prefix line is empty.
        let data = b"chkleaf:\n100\n1\n0\n\n";
        let p = deserialise_leaf_node(data).unwrap();
        assert_eq!(p.length, 0);
        assert!(p.items.is_empty());
        assert_eq!(p.common_serialised_prefix, b"");
        // raw_size = 0 + 0*0 + 0 = 0 for this case.
        assert_eq!(p.raw_size, 0);
    }

    #[test]
    fn deserialise_leaf_multi_element_key() {
        // key_width=2 means each item line has 3 NUL-separated fields:
        // the two key elements plus the value-line count.
        let data = b"chkleaf:\n200\n2\n1\n\nkey1\x00sub\x001\nhello\n";
        let p = deserialise_leaf_node(data).unwrap();
        assert_eq!(p.key_width, 2);
        assert_eq!(p.items.len(), 1);
        assert_eq!(p.items[0].0, vec![b"key1".to_vec(), b"sub".to_vec()]);
        assert_eq!(p.items[0].1, b"hello");
    }

    #[test]
    fn deserialise_leaf_rejects_missing_trailing_newline() {
        let data = b"chkleaf:\n100\n1\n0\n";
        assert!(matches!(
            deserialise_leaf_node(data),
            Err(Error::DeserializeError(_))
        ));
    }

    #[test]
    fn deserialise_leaf_rejects_bad_magic() {
        let data = b"notaleaf:\n100\n1\n0\n\n";
        assert!(matches!(
            deserialise_leaf_node(data),
            Err(Error::DeserializeError(_))
        ));
    }

    #[test]
    fn deserialise_leaf_rejects_wrong_element_count() {
        // width=1 but the key line has three NUL-separated pieces.
        let data = b"chkleaf:\n100\n1\n1\n\nfoo\x00bar\x001\nval\n";
        assert!(matches!(
            deserialise_leaf_node(data),
            Err(Error::DeserializeError(_))
        ));
    }

    #[test]
    fn deserialise_leaf_rejects_length_mismatch() {
        // Claims length=2 but only one item is present.
        let data = b"chkleaf:\n100\n1\n2\n\nfoo\x001\nval\n";
        assert!(matches!(
            deserialise_leaf_node(data),
            Err(Error::DeserializeError(_))
        ));
    }

    // Fixture generated from the real Python serialiser for an internal
    // node: _maximum_size=200, key_width=1, _search_prefix=b"pre", two
    // children. Cross-checked in the session probe.
    const INTERNAL_FIXTURE: &[u8] = b"chknode:\n200\n1\n2\npre\nbar\x00sha1:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb\nfoo\x00sha1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n";

    #[test]
    fn deserialise_internal_fixture_items_match() {
        let p = deserialise_internal_node(INTERNAL_FIXTURE).unwrap();
        assert_eq!(p.maximum_size, 200);
        assert_eq!(p.key_width, 1);
        assert_eq!(p.length, 2);
        assert_eq!(p.search_prefix, b"pre");
        assert_eq!(p.items.len(), 2);
        assert_eq!(p.items[0].0, b"prebar");
        assert_eq!(
            p.items[0].1,
            b"sha1:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        );
        assert_eq!(p.items[1].0, b"prefoo");
        assert_eq!(
            p.items[1].1,
            b"sha1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        );
        // _node_width mirrors Python's loop-variable leak: the length
        // of the final parsed prefix.
        assert_eq!(p.node_width, b"prefoo".len());
    }

    #[test]
    fn deserialise_internal_rejects_empty_items() {
        let data = b"chknode:\n100\n1\n0\n\n";
        assert!(matches!(
            deserialise_internal_node(data),
            Err(Error::DeserializeError(_))
        ));
    }

    #[test]
    fn deserialise_internal_rejects_bad_magic() {
        let data = b"notchk:\n100\n1\n1\n\nfoo\x00sha1:aaaa\n";
        assert!(matches!(
            deserialise_internal_node(data),
            Err(Error::DeserializeError(_))
        ));
    }

    #[test]
    fn deserialise_internal_rejects_missing_trailing_newline() {
        let data = b"chknode:\n100\n1\n1\n\nfoo\x00sha1:aaaa";
        assert!(matches!(
            deserialise_internal_node(data),
            Err(Error::DeserializeError(_))
        ));
    }

    #[test]
    fn deserialise_internal_rejects_line_without_nul() {
        let data = b"chknode:\n100\n1\n1\n\nfoobar\n";
        assert!(matches!(
            deserialise_internal_node(data),
            Err(Error::DeserializeError(_))
        ));
    }
}
