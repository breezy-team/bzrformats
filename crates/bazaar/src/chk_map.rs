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

/// The set of search-key functions a CHKMap may be configured with.
///
/// `Plain` / `Hash16Way` / `Hash255Way` mirror the entries registered in
/// Python's `search_key_registry` for production use. `Custom` carries a
/// boxed closure for callers (most often test code) that register their
/// own — the pyo3 layer adapts a Python callable into a `Custom` variant.
#[derive(Clone)]
pub enum SearchKeyFunc {
    /// `b"plain"` — `b"\x00".join(key)`.
    Plain,
    /// `b"hash-16-way"` — 8-char uppercase hex of `crc32(part)` joined by NUL.
    Hash16Way,
    /// `b"hash-255-way"` — big-endian `crc32(part)` bytes joined by NUL,
    /// with any `\n` byte rewritten to `_`.
    Hash255Way,
    /// Any other callable, identified by a free-form name. The bytes
    /// returned by `name()` for a `Custom` variant come straight from
    /// the caller — they may not appear in `from_name`'s lookup table.
    Custom {
        name: Vec<u8>,
        func: std::sync::Arc<dyn Fn(&Key) -> SerializedKey + Send + Sync>,
    },
}

impl std::fmt::Debug for SearchKeyFunc {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SearchKeyFunc::Plain => f.write_str("Plain"),
            SearchKeyFunc::Hash16Way => f.write_str("Hash16Way"),
            SearchKeyFunc::Hash255Way => f.write_str("Hash255Way"),
            SearchKeyFunc::Custom { name, .. } => write!(f, "Custom({:?})", name),
        }
    }
}

impl PartialEq for SearchKeyFunc {
    /// Built-in variants compare equal by tag; `Custom` variants are
    /// only equal when their `Arc`s point at the same closure.
    fn eq(&self, other: &Self) -> bool {
        use SearchKeyFunc::*;
        match (self, other) {
            (Plain, Plain) | (Hash16Way, Hash16Way) | (Hash255Way, Hash255Way) => true,
            (Custom { func: a, .. }, Custom { func: b, .. }) => std::sync::Arc::ptr_eq(a, b),
            _ => false,
        }
    }
}

impl Eq for SearchKeyFunc {}

impl SearchKeyFunc {
    /// Resolve a registry name to a built-in variant. Unknown names
    /// return the raw bytes back; the caller may wrap them in a
    /// `Custom` variant with a closure of its own.
    pub fn from_name(name: &[u8]) -> Result<Self, Vec<u8>> {
        match name {
            b"plain" => Ok(SearchKeyFunc::Plain),
            b"hash-16-way" => Ok(SearchKeyFunc::Hash16Way),
            b"hash-255-way" => Ok(SearchKeyFunc::Hash255Way),
            other => Err(other.to_vec()),
        }
    }

    /// Wire name as it appears in serialised inventories.
    pub fn name(&self) -> &[u8] {
        match self {
            SearchKeyFunc::Plain => b"plain",
            SearchKeyFunc::Hash16Way => b"hash-16-way",
            SearchKeyFunc::Hash255Way => b"hash-255-way",
            SearchKeyFunc::Custom { name, .. } => name.as_slice(),
        }
    }

    /// Apply this variant's search-key transform to `key`.
    pub fn apply(&self, key: &Key) -> SerializedKey {
        match self {
            SearchKeyFunc::Plain => search_key_plain(key),
            SearchKeyFunc::Hash16Way => search_key_16(key),
            SearchKeyFunc::Hash255Way => search_key_255(key),
            SearchKeyFunc::Custom { func, .. } => func(key),
        }
    }
}

impl Default for SearchKeyFunc {
    fn default() -> Self {
        SearchKeyFunc::Plain
    }
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
    /// A precondition or invariant was violated. Mirrors Python's
    /// `AssertionError` paths in chk_map.py — usually a bug in the
    /// caller, not a corrupt input.
    AssertionFailed(String),
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

/// Build the byte chunks that [`LeafNode.serialise`] would emit before
/// adding them to a store. `items` must be presented in already-sorted
/// order (Python sorts `self._items.items()` before walking).
///
/// `common_prefix` is the longest common serialised prefix among all
/// items — pass `None` for an empty node. The output is split into one
/// `Vec<u8>` per line so the caller can hand it straight to
/// `store.add_lines(...)`.
///
/// Mirrors `LeafNode.serialise` minus the I/O side: the caller is still
/// responsible for `store.add_lines` and for updating `self._key` /
/// the in-memory cache from the resulting bytes.
pub fn serialise_leaf_node(
    maximum_size: usize,
    key_width: usize,
    items: &[(Vec<Vec<u8>>, Vec<u8>)],
    common_prefix: Option<&[u8]>,
) -> Result<Vec<Vec<u8>>, Error> {
    let mut out: Vec<Vec<u8>> = Vec::with_capacity(5 + items.len() * 2);
    out.push(b"chkleaf:\n".to_vec());
    out.push(format!("{}\n", maximum_size).into_bytes());
    out.push(format!("{}\n", key_width).into_bytes());
    out.push(format!("{}\n", items.len()).into_bytes());
    let prefix_bytes = match common_prefix {
        None => {
            if !items.is_empty() {
                return Err(Error::DeserializeError(
                    "common prefix is None but items is non-empty".into(),
                ));
            }
            out.push(b"\n".to_vec());
            return Ok(out);
        }
        Some(p) => {
            let mut line = p.to_vec();
            line.push(b'\n');
            out.push(line);
            p
        }
    };
    let prefix_len = prefix_bytes.len();
    for (key, value) in items {
        // Python's `osutils.chunks_to_lines([value + b"\n"])` resplits the
        // value bytes on newlines, ensuring every value line ends in '\n'
        // except possibly the last (and the trailing b"\n" we appended
        // guarantees the last one ends in '\n' too).
        let mut padded = value.to_vec();
        padded.push(b'\n');
        let value_lines: Vec<Vec<u8>> = split_lines_inclusive(&padded);

        let serialised_key = key.join(&b'\x00');
        let mut header = serialised_key.clone();
        header.push(b'\x00');
        header.extend_from_slice(format!("{}\n", value_lines.len()).as_bytes());
        if !header.starts_with(prefix_bytes) {
            return Err(Error::DeserializeError(format!(
                "serialised key {:?} does not start with common prefix {:?}",
                header, prefix_bytes
            )));
        }
        out.push(header[prefix_len..].to_vec());
        out.extend(value_lines);
    }
    Ok(out)
}

/// One entry on an `InternalNode`'s serialised body: a prefix and the
/// flat sha1 key of the child it points at. The Python serialiser sorts
/// these by prefix before writing.
#[derive(Debug, Clone)]
pub struct InternalNodeChild {
    pub prefix: Vec<u8>,
    pub flat_key: Vec<u8>,
}

/// Build the byte chunks that [`InternalNode.serialise`] would emit
/// before adding them to a store. `items` must be sorted by `prefix`
/// (the Python loop does `sorted(self._items.items())`).
///
/// `length` is the InternalNode's `_len` — the total number of leaf
/// entries reachable through this node, **not** `items.len()` (which is
/// the direct fan-out count).
///
/// Mirrors `InternalNode.serialise` minus the I/O side and the
/// recursive walk that flushes child nodes first.
pub fn serialise_internal_node(
    maximum_size: usize,
    key_width: usize,
    length: usize,
    search_prefix: &[u8],
    items: &[InternalNodeChild],
) -> Result<Vec<Vec<u8>>, Error> {
    let mut out: Vec<Vec<u8>> = Vec::with_capacity(5 + items.len());
    out.push(b"chknode:\n".to_vec());
    out.push(format!("{}\n", maximum_size).into_bytes());
    out.push(format!("{}\n", key_width).into_bytes());
    out.push(format!("{}\n", length).into_bytes());
    let mut prefix_line = search_prefix.to_vec();
    prefix_line.push(b'\n');
    out.push(prefix_line);
    let prefix_len = search_prefix.len();
    for child in items {
        let mut serialised = child.prefix.clone();
        serialised.push(b'\x00');
        serialised.extend_from_slice(&child.flat_key);
        serialised.push(b'\n');
        if !serialised.starts_with(search_prefix) {
            return Err(Error::DeserializeError(format!(
                "internal node entry {:?} does not start with prefix {:?}",
                serialised, search_prefix
            )));
        }
        out.push(serialised[prefix_len..].to_vec());
    }
    Ok(out)
}

/// Serialised byte cost of one `(key, value)` pair inside a leaf node.
///
/// Mirrors `LeafNode._key_value_len` exactly: the key tuple's NUL-joined
/// bytes, the count of newlines in the value as a decimal string, the
/// value bytes themselves, and three separator bytes. The hot path for
/// every map/unmap is calling this to track `_raw_size`.
pub fn leaf_node_key_value_len(key: &[Vec<u8>], value: &[u8]) -> usize {
    let key_len: usize = if key.is_empty() {
        0
    } else {
        key.iter().map(Vec::len).sum::<usize>() + (key.len() - 1)
    };
    let newline_count = value.iter().filter(|&&b| b == b'\n').count();
    let newline_count_digits = if newline_count == 0 {
        1
    } else {
        let mut n = newline_count;
        let mut digits = 0;
        while n > 0 {
            n /= 10;
            digits += 1;
        }
        digits
    };
    key_len + 1 + newline_count_digits + 1 + value.len() + 1
}

/// Serialised byte cost of a leaf node, including its header.
///
/// Mirrors `LeafNode._current_size`. `bytes_for_items` is the sum of
/// per-entry serialised costs (i.e. the running `_raw_size`), with the
/// common-prefix-collapse applied: subtract `prefix_len * length` so
/// each entry doesn't pay for the prefix once stored once at the top
/// of the leaf.
pub fn leaf_node_current_size(
    maximum_size: usize,
    key_width: usize,
    length: usize,
    raw_size: usize,
    common_serialised_prefix: Option<&[u8]>,
) -> usize {
    let (bytes_for_items, prefix_len) = match common_serialised_prefix {
        None => (0, 0),
        Some(prefix) => {
            let prefix_len = prefix.len();
            (raw_size - prefix_len * length, prefix_len)
        }
    };
    // 9 = b"chkleaf:\n".len()
    9 + decimal_digits(maximum_size)
        + 1
        + decimal_digits(key_width)
        + 1
        + decimal_digits(length)
        + 1
        + prefix_len
        + 1
        + bytes_for_items
}

/// Serialised byte cost of an internal node header.
///
/// Mirrors `InternalNode._current_size`. The body bytes are tracked
/// separately in `_raw_size`; the header adds the four decimal-encoded
/// integers (maximum_size, key_width, length).
pub fn internal_node_current_size(
    maximum_size: usize,
    key_width: usize,
    length: usize,
    raw_size: usize,
) -> usize {
    raw_size + decimal_digits(length) + decimal_digits(key_width) + decimal_digits(maximum_size)
}

#[inline]
fn decimal_digits(n: usize) -> usize {
    if n == 0 {
        return 1;
    }
    let mut n = n;
    let mut digits = 0;
    while n > 0 {
        n /= 10;
        digits += 1;
    }
    digits
}

/// State of a `LeafNode`'s `_search_prefix` field.
///
/// Python uses an `_unknown` sentinel object to mark "items were
/// rebuilt without recomputing the prefix"; map / unmap then
/// demand-compute it. `None` means the node is empty, so there is no
/// prefix.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchPrefix {
    /// The node was rebuilt and the prefix has not been recomputed.
    Unknown,
    /// The prefix is known to be `Some(p)` for a non-empty leaf, or
    /// `None` for an empty leaf.
    Computed(Option<Vec<u8>>),
}

impl SearchPrefix {
    /// Return the prefix bytes if computed, panicking on `Unknown`.
    /// Mirrors call sites where Python would have already demanded a
    /// recompute.
    pub fn expect_computed(&self) -> Option<&[u8]> {
        match self {
            SearchPrefix::Unknown => panic!("search prefix is Unknown"),
            SearchPrefix::Computed(p) => p.as_deref(),
        }
    }

    pub fn is_unknown(&self) -> bool {
        matches!(self, SearchPrefix::Unknown)
    }
}

/// In-memory state of a CHK leaf node.
///
/// Mirrors Python's `LeafNode` exactly, minus the store-touching
/// methods (`serialise`, `map`/`_split` recursion). Held by the
/// upcoming pyo3 `LeafNode` pyclass and used directly by pure-Rust
/// callers.
///
/// Iteration order of `items` follows insertion order so it matches
/// Python's `dict` semantics — tests and the `_split` algorithm both
/// observe items in that order when they don't sort first.
#[derive(Debug, Clone)]
pub struct LeafNode {
    /// `(b"sha1:...",)` once the node has been serialised to a store;
    /// `None` while it is still mutable.
    pub key: Option<Vec<u8>>,
    pub maximum_size: usize,
    pub key_width: usize,
    pub raw_size: usize,
    pub items: indexmap::IndexMap<Vec<Vec<u8>>, Vec<u8>>,
    pub search_prefix: SearchPrefix,
    pub common_serialised_prefix: Option<Vec<u8>>,
    pub search_key_func: SearchKeyFunc,
}

impl LeafNode {
    /// Empty node with the given search-key transform.
    pub fn new(search_key_func: SearchKeyFunc) -> Self {
        Self {
            key: None,
            maximum_size: 0,
            key_width: 1,
            raw_size: 0,
            items: indexmap::IndexMap::new(),
            search_prefix: SearchPrefix::Computed(None),
            common_serialised_prefix: None,
            search_key_func,
        }
    }

    /// Build a populated leaf from the output of [`deserialise_leaf_node`].
    ///
    /// Mirrors Python's `_deserialise_leaf_node` post-processing: the
    /// items become a dict, `search_prefix` is `Unknown` (Python uses
    /// `_unknown`) for non-empty nodes so the next mutator recomputes
    /// it on demand, and `common_serialised_prefix` is taken straight
    /// from the parsed prefix.
    pub fn from_parsed(parsed: ParsedLeafNode, search_key_func: SearchKeyFunc) -> Self {
        let items_empty = parsed.items.is_empty();
        let mut items = indexmap::IndexMap::with_capacity(parsed.items.len());
        for (k, v) in parsed.items {
            items.insert(k, v);
        }
        let (search_prefix, common_serialised_prefix) = if items_empty {
            (SearchPrefix::Computed(None), None)
        } else {
            (SearchPrefix::Unknown, Some(parsed.common_serialised_prefix))
        };
        Self {
            key: None,
            maximum_size: parsed.maximum_size,
            key_width: parsed.key_width,
            raw_size: parsed.raw_size,
            items,
            search_prefix,
            common_serialised_prefix,
            search_key_func,
        }
    }

    /// Number of entries in this leaf.
    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Wrapper around [`leaf_node_current_size`] using `self`'s state.
    pub fn current_size(&self) -> usize {
        leaf_node_current_size(
            self.maximum_size,
            self.key_width,
            self.len(),
            self.raw_size,
            self.common_serialised_prefix.as_deref(),
        )
    }

    /// Recompute `search_prefix` from scratch by hashing every key
    /// and reducing with [`common_prefix_many`]. Mirrors
    /// `LeafNode._compute_search_prefix`.
    pub fn compute_search_prefix(&mut self) -> Option<&[u8]> {
        let keys: Vec<SerializedKey> = self
            .items
            .keys()
            .map(|k| self.search_key_func.apply(&Key::from(k.clone())))
            .collect();
        let prefix = common_prefix_many(keys.iter().map(|k| k.as_slice())).map(|s| s.to_vec());
        self.search_prefix = SearchPrefix::Computed(prefix);
        match &self.search_prefix {
            SearchPrefix::Computed(p) => p.as_deref(),
            SearchPrefix::Unknown => unreachable!(),
        }
    }

    /// Recompute `common_serialised_prefix` from scratch. Mirrors
    /// `LeafNode._compute_serialised_prefix`.
    pub fn compute_serialised_prefix(&mut self) -> Option<&[u8]> {
        let keys: Vec<SerializedKey> = self
            .items
            .keys()
            .map(|k| Key::from(k.clone()).serialize())
            .collect();
        let prefix = common_prefix_many(keys.iter().map(|k| k.as_slice())).map(|s| s.to_vec());
        self.common_serialised_prefix = prefix;
        self.common_serialised_prefix.as_deref()
    }

    /// `LeafNode._are_search_keys_identical`: all entries hash to the
    /// same search key. Empty leaves return `true`.
    pub fn are_search_keys_identical(&self) -> bool {
        let keys = self
            .items
            .keys()
            .map(|k| self.search_key_func.apply(&Key::from(k.clone())));
        are_search_keys_identical(keys)
    }

    /// Insert `(key, value)` and update `raw_size`, `search_prefix`,
    /// `common_serialised_prefix`. Returns `true` if the leaf has
    /// overflowed `maximum_size` and the caller must split it.
    ///
    /// Mirrors `LeafNode._map_no_split`: the caller must have already
    /// removed any prior entry for `key` from `raw_size` / `len`. If
    /// `search_prefix` was `Unknown`, it is recomputed before applying
    /// the new key.
    pub fn map_no_split(&mut self, key: Vec<Vec<u8>>, value: Vec<u8>) -> bool {
        self.raw_size += leaf_node_key_value_len(&key, &value);
        let serialised_key = Key::from(key.clone()).serialize();
        let search_key = self.search_key_func.apply(&Key::from(key.clone()));
        self.items.insert(key, value);

        self.common_serialised_prefix = Some(match self.common_serialised_prefix.take() {
            None => serialised_key,
            Some(prefix) => common_prefix_pair(&prefix, &serialised_key).to_vec(),
        });

        if self.search_prefix.is_unknown() {
            self.compute_search_prefix();
        }
        let new_prefix = match &self.search_prefix {
            SearchPrefix::Computed(None) => search_key.clone(),
            SearchPrefix::Computed(Some(p)) => common_prefix_pair(p, &search_key).to_vec(),
            SearchPrefix::Unknown => unreachable!("just recomputed above"),
        };
        self.search_prefix = SearchPrefix::Computed(Some(new_prefix.clone()));

        if self.items.len() > 1
            && self.maximum_size > 0
            && self.current_size() > self.maximum_size
            && (search_key != new_prefix || !self.are_search_keys_identical())
        {
            return true;
        }
        false
    }

    /// Remove `key`, recomputing `search_prefix` and
    /// `common_serialised_prefix` from scratch. Returns the removed
    /// value, or `None` if `key` was not present (Python raises
    /// `KeyError`; here the caller decides how to react).
    ///
    /// Mirrors `LeafNode.unmap` minus the store argument (the store is
    /// unused on the leaf path).
    pub fn unmap(&mut self, key: &[Vec<u8>]) -> Option<Vec<u8>> {
        let removed = self.items.shift_remove(key)?;
        self.raw_size -= leaf_node_key_value_len(key, &removed);
        self.key = None;
        self.compute_search_prefix();
        self.compute_serialised_prefix();
        Some(removed)
    }

    /// Yield `(key, value)` pairs matching `key_filter`. When `None`,
    /// yields every entry in insertion order. Otherwise:
    ///
    /// * Filter keys of length `key_width` are looked up directly and
    ///   yielded in filter order (matching Python's left-to-right
    ///   iteration before falling through to the prefix-match pass).
    /// * Shorter filter keys act as prefix filters across the items.
    ///   Items are checked in insertion order; the first matching
    ///   filter wins (matches Python's `break` after a yield).
    ///
    /// Mirrors `LeafNode.iteritems` exactly; the store argument is
    /// unused on the leaf path and is omitted from this pure-Rust
    /// signature.
    pub fn iteritems(
        &self,
        key_filter: Option<&[Vec<Vec<u8>>]>,
    ) -> Vec<(Vec<Vec<u8>>, Vec<u8>)> {
        let mut out: Vec<(Vec<Vec<u8>>, Vec<u8>)> = Vec::new();
        let filter = match key_filter {
            None => {
                for (k, v) in self.items.iter() {
                    out.push((k.clone(), v.clone()));
                }
                return out;
            }
            Some(f) => f,
        };
        // Group short filters by length; iterate exact-width matches in
        // filter order.
        let mut short_filters: std::collections::HashMap<usize, Vec<&[Vec<u8>]>> =
            std::collections::HashMap::new();
        for key in filter.iter() {
            if key.len() == self.key_width {
                if let Some(v) = self.items.get(key) {
                    out.push((key.clone(), v.clone()));
                }
            } else {
                short_filters.entry(key.len()).or_default().push(key);
            }
        }
        if !short_filters.is_empty() {
            for (k, v) in self.items.iter() {
                for (length, candidates) in short_filters.iter() {
                    if k.len() >= *length && candidates.iter().any(|c| *c == &k[..*length]) {
                        out.push((k.clone(), v.clone()));
                        break;
                    }
                }
            }
        }
        out
    }
}

/// In-memory CHK node — either a leaf with key/value entries or an
/// internal node referencing other nodes. Mirrors the Python
/// `Node` base class hierarchy (LeafNode | InternalNode).
#[derive(Debug, Clone)]
pub enum Node {
    Leaf(Box<LeafNode>),
    Internal(Box<InternalNode>),
}

impl Node {
    /// `(b"sha1:...",)` key once the node has been serialised, else
    /// `None`. Mirrors Python's `Node.key()`.
    pub fn key(&self) -> Option<&[u8]> {
        match self {
            Node::Leaf(l) => l.key.as_deref(),
            Node::Internal(n) => n.key.as_deref(),
        }
    }

    /// Total number of leaf entries reachable through this node.
    /// Mirrors Python's `Node.__len__`.
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        match self {
            Node::Leaf(l) => l.len(),
            Node::Internal(n) => n.len,
        }
    }

    /// Maximum byte size allowed for this node when serialised, or
    /// `0` for unlimited.
    pub fn maximum_size(&self) -> usize {
        match self {
            Node::Leaf(l) => l.maximum_size,
            Node::Internal(n) => n.maximum_size,
        }
    }
}

/// Reference to a child of an `InternalNode`. The dict either holds a
/// `(b"sha1:...",)` reference to an unloaded child page or, after
/// `_iter_nodes` populates it from the store, the loaded child Node
/// itself.
#[derive(Debug, Clone)]
pub enum NodeRef {
    /// Unloaded reference — the `b"sha1:..."` key handed to the store
    /// to fetch the child's bytes.
    Unloaded(Vec<u8>),
    /// In-memory child node.
    Loaded(Node),
}

impl NodeRef {
    /// Length contribution of this child for `InternalNode._len`.
    pub fn len(&self) -> usize {
        match self {
            NodeRef::Unloaded(_) => {
                // Mirrors Python's behaviour: a tuple has no `len(node)`
                // semantic in `_len`, but Python only writes Unloaded
                // references after the parent's `_len` was already set
                // from the parsed body, so the per-ref length isn't
                // queried for unloaded children.
                0
            }
            NodeRef::Loaded(n) => n.len(),
        }
    }
}

/// In-memory state of a CHK internal node — a fan-out of byte-prefix
/// to child (loaded Node or unloaded sha1 reference).
///
/// Iteration order over `items` follows insertion order to match
/// Python's `dict` semantics; serialisation paths sort first.
#[derive(Debug, Clone)]
pub struct InternalNode {
    pub key: Option<Vec<u8>>,
    pub maximum_size: usize,
    pub key_width: usize,
    /// Total leaf entries reachable through this subtree.
    pub len: usize,
    /// Width in bytes of each prefix in `items`. `0` for an
    /// empty internal node (Python's `_node_width` default).
    pub node_width: usize,
    /// Reserved for future prefix-compression accounting; the Python
    /// `InternalNode` sets `_raw_size = None` since internal nodes
    /// don't carry per-entry payload. We track an explicit `0` here
    /// and recompute byte costs from the items list at serialise
    /// time.
    pub raw_size: usize,
    pub items: indexmap::IndexMap<Vec<u8>, NodeRef>,
    /// Search-key prefix common to every child of this node. Always
    /// `Some` once initialised; `None` only on a default-constructed
    /// empty node before `from_parsed` populates it.
    pub search_prefix: Option<Vec<u8>>,
    pub search_key_func: SearchKeyFunc,
}

impl InternalNode {
    /// Empty internal node with the given search prefix and key
    /// function. Mirrors Python's `InternalNode(prefix, search_key_func)`.
    pub fn new(prefix: Vec<u8>, search_key_func: SearchKeyFunc) -> Self {
        Self {
            key: None,
            maximum_size: 0,
            key_width: 1,
            len: 0,
            node_width: 0,
            raw_size: 0,
            items: indexmap::IndexMap::new(),
            search_prefix: Some(prefix),
            search_key_func,
        }
    }

    /// Build a populated internal node from the output of
    /// [`deserialise_internal_node`]. All children start as
    /// `NodeRef::Unloaded`; the caller demand-loads them via the
    /// store as `_iter_nodes` runs.
    pub fn from_parsed(parsed: ParsedInternalNode, search_key_func: SearchKeyFunc) -> Self {
        let mut items: indexmap::IndexMap<Vec<u8>, NodeRef> =
            indexmap::IndexMap::with_capacity(parsed.items.len());
        for (prefix, flat_key) in parsed.items {
            items.insert(prefix, NodeRef::Unloaded(flat_key));
        }
        Self {
            key: None,
            maximum_size: parsed.maximum_size,
            key_width: parsed.key_width,
            len: parsed.length,
            node_width: parsed.node_width,
            raw_size: 0,
            items,
            search_prefix: Some(parsed.search_prefix),
            search_key_func,
        }
    }

    /// Serialised byte cost of this node (header + body). Wraps
    /// [`internal_node_current_size`].
    pub fn current_size(&self) -> usize {
        internal_node_current_size(self.maximum_size, self.key_width, self.len, self.raw_size)
    }

    /// Add a loaded child node under `prefix`. Mirrors Python's
    /// `InternalNode.add_node`: validates that `prefix` extends
    /// `search_prefix` by exactly one byte, updates `len` and
    /// `node_width`, clears `key` (this node is now dirty).
    pub fn add_node(&mut self, prefix: Vec<u8>, node: Node) -> Result<(), Error> {
        let search_prefix = self.search_prefix.as_deref().ok_or_else(|| {
            Error::AssertionFailed("InternalNode.add_node: search_prefix is None".into())
        })?;
        if !prefix.starts_with(search_prefix) {
            return Err(Error::AssertionFailed(format!(
                "prefixes mismatch: {:?} must start with {:?}",
                prefix, search_prefix
            )));
        }
        if prefix.len() != search_prefix.len() + 1 {
            return Err(Error::AssertionFailed(format!(
                "prefix wrong length: len({:?}) is not {}",
                prefix,
                search_prefix.len() + 1
            )));
        }
        self.len += node.len();
        if self.items.is_empty() {
            self.node_width = prefix.len();
        }
        if self.node_width != search_prefix.len() + 1 {
            return Err(Error::AssertionFailed(format!(
                "node width mismatch: {} is not {}",
                self.node_width,
                search_prefix.len() + 1
            )));
        }
        self.items.insert(prefix, NodeRef::Loaded(node));
        self.key = None;
        Ok(())
    }

    /// Pad or truncate `key`'s search-key bytes to fit `node_width`,
    /// padding with NUL. Mirrors `InternalNode._search_key`.
    pub fn search_key(&self, key: &Key) -> SerializedKey {
        let base = self.search_key_func.apply(key);
        if base.len() >= self.node_width {
            base[..self.node_width].to_vec()
        } else {
            let mut padded = Vec::with_capacity(self.node_width);
            padded.extend_from_slice(&base);
            padded.resize(self.node_width, 0);
            padded
        }
    }

    /// Truncate `key`'s search-key bytes to `node_width` without
    /// padding (used as a prefix when filtering iteritems). Mirrors
    /// `InternalNode._search_prefix_filter`.
    pub fn search_prefix_filter(&self, key: &Key) -> SerializedKey {
        let base = self.search_key_func.apply(key);
        if base.len() >= self.node_width {
            base[..self.node_width].to_vec()
        } else {
            base
        }
    }

    /// Recompute `search_prefix` as the longest common prefix of all
    /// child prefixes. Mirrors `InternalNode._compute_search_prefix`.
    pub fn compute_search_prefix(&mut self) -> Option<&[u8]> {
        let prefix =
            common_prefix_many(self.items.keys().map(|k| k.as_slice())).map(|s| s.to_vec());
        self.search_prefix = prefix;
        self.search_prefix.as_deref()
    }

    /// Read-only references to the children. Mirrors `InternalNode.refs`:
    /// returns the sha1 key of each unloaded child plus the
    /// `.key()` of each loaded child. Returns an `AssertionFailed`
    /// error when `self.key` is unset — the Python equivalent
    /// raises an `AssertionError`.
    pub fn refs(&self) -> Result<Vec<Vec<u8>>, Error> {
        if self.key.is_none() {
            return Err(Error::AssertionFailed(
                "unserialised nodes have no refs".into(),
            ));
        }
        let mut out: Vec<Vec<u8>> = Vec::with_capacity(self.items.len());
        for value in self.items.values() {
            match value {
                NodeRef::Unloaded(k) => out.push(k.clone()),
                NodeRef::Loaded(n) => {
                    let k = n.key().ok_or_else(|| {
                        Error::AssertionFailed(
                            "InternalNode.refs: loaded child has no key".into(),
                        )
                    })?;
                    out.push(k.to_vec());
                }
            }
        }
        Ok(out)
    }
}

/// Check whether every search key in `search_keys` is identical.
///
/// Mirrors `LeafNode._are_search_keys_identical`: this is the safety check
/// that lets a LeafNode grow past `_maximum_size` when its keys all hash to
/// the same search key (a collision under hash-based search funcs). An
/// empty iterator returns `true` (no two keys disagree).
pub fn are_search_keys_identical<I, S>(search_keys: I) -> bool
where
    I: IntoIterator<Item = S>,
    S: AsRef<[u8]>,
{
    let mut iter = search_keys.into_iter();
    let first = match iter.next() {
        Some(k) => k,
        None => return true,
    };
    let first = first.as_ref();
    iter.all(|k| k.as_ref() == first)
}

/// Split `data` into lines, keeping the trailing `\n` on each non-final
/// line. Mirrors `osutils.chunks_to_lines([b"".join(chunks)])` for a
/// single chunk input: every `\n` ends a line, and any unterminated
/// tail becomes its own final line.
fn split_lines_inclusive(data: &[u8]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut start = 0;
    for (i, &b) in data.iter().enumerate() {
        if b == b'\n' {
            out.push(data[start..=i].to_vec());
            start = i + 1;
        }
    }
    if start < data.len() {
        out.push(data[start..].to_vec());
    }
    out
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
mod tests {
    use super::*;

    fn key(parts: &[&[u8]]) -> Key {
        Key::from(parts.iter().map(|p| p.to_vec()).collect::<Vec<_>>())
    }

    #[test]
    fn search_key_plain_joins_with_nul() {
        assert_eq!(
            search_key_plain(&key(&[b"foo", b"bar"])),
            b"foo\x00bar".to_vec()
        );
        assert_eq!(search_key_plain(&key(&[b"only"])), b"only".to_vec());
    }

    #[test]
    fn search_key_16_is_uppercase_hex() {
        // For a single-element key, the result is `{crc32:08X}`.
        let out = search_key_16(&key(&[b"hello"]));
        let s = std::str::from_utf8(&out).unwrap();
        assert_eq!(s.len(), 8);
        assert!(s
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_lowercase()));
    }

    #[test]
    fn search_key_16_joins_multi_element_keys_with_nul() {
        let out = search_key_16(&key(&[b"a", b"b"]));
        // Two 8-char hex blocks separated by a single \x00.
        assert_eq!(out.len(), 17);
        assert_eq!(out[8], b'\x00');
    }

    #[test]
    fn search_key_255_uses_raw_be_bytes_with_lf_replaced() {
        let out = search_key_255(&key(&[b"hello"]));
        assert_eq!(out.len(), 4);
        assert!(!out.contains(&b'\n'));
    }

    #[test]
    fn search_key_255_replaces_lf_with_underscore() {
        // The implementation post-processes the raw CRC bytes to replace any
        // \n byte with `_` so the result can be safely embedded in a node
        // dump.
        for input in [b"abc".as_slice(), b"x", b"y"] {
            let out = search_key_255(&key(&[input]));
            assert!(!out.contains(&b'\n'));
        }
    }

    #[test]
    fn search_key_255_multi_element_keys_use_nul_separator() {
        let out = search_key_255(&key(&[b"a", b"b"]));
        assert_eq!(out.len(), 9);
        assert_eq!(out[4], b'\x00');
    }

    #[test]
    fn bytes_to_text_key_parses_file_record() {
        let bytes = b"file: file-id\nparent-id\nname\nrevision-id\n\
da39a3ee5e6b4b0d3255bfef95601890afd80709\n100\nN";
        let (file_id, revision_id) = bytes_to_text_key(bytes).unwrap();
        assert_eq!(file_id, b"file-id");
        assert_eq!(revision_id, b"revision-id");
    }

    #[test]
    fn bytes_to_text_key_rejects_missing_separator() {
        // No `: ` between kind and file-id.
        let bytes = b"file:file-id\nparent-id\nname\nrevision-id\n\
da39a3ee5e6b4b0d3255bfef95601890afd80709\n100\nN";
        assert!(bytes_to_text_key(bytes).is_err());
    }

    #[test]
    fn key_serialize_joins_parts_with_nul() {
        assert_eq!(key(&[b"foo", b"bar"]).serialize(), b"foo\x00bar".to_vec());
        assert_eq!(key(&[b"alone"]).serialize(), b"alone".to_vec());
    }

    #[test]
    fn key_len_returns_part_count() {
        assert_eq!(key(&[b"foo", b"bar"]).len(), 2);
        assert_eq!(key(&[b"a"]).len(), 1);
    }

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

    #[test]
    fn serialise_leaf_node_roundtrips_through_deserialise() {
        let items: Vec<(Vec<Vec<u8>>, Vec<u8>)> = vec![
            (vec![b"a".to_vec()], b"v1".to_vec()),
            (vec![b"alpha".to_vec()], b"v2\nv2line2".to_vec()),
        ];
        // Common prefix of the serialised key lines `a\x001\n` and
        // `alpha\x002\n` is `a` (the first byte of "a" and "alpha").
        let out = serialise_leaf_node(100, 1, &items, Some(b"a")).unwrap();
        let blob: Vec<u8> = out.iter().flatten().copied().collect();
        let parsed = deserialise_leaf_node(&blob).unwrap();
        assert_eq!(parsed.maximum_size, 100);
        assert_eq!(parsed.key_width, 1);
        assert_eq!(parsed.length, 2);
        assert_eq!(parsed.common_serialised_prefix, b"a");
        assert_eq!(parsed.items.len(), 2);
        assert_eq!(parsed.items[0].0, vec![b"a".to_vec()]);
        assert_eq!(parsed.items[0].1, b"v1".to_vec());
        assert_eq!(parsed.items[1].0, vec![b"alpha".to_vec()]);
        assert_eq!(parsed.items[1].1, b"v2\nv2line2".to_vec());
    }

    #[test]
    fn serialise_empty_leaf_node_has_blank_prefix() {
        let out = serialise_leaf_node(100, 1, &[], None).unwrap();
        let blob: Vec<u8> = out.iter().flatten().copied().collect();
        assert_eq!(blob, b"chkleaf:\n100\n1\n0\n\n");
    }

    #[test]
    fn leaf_node_key_value_len_matches_python_layout() {
        // Single-segment key, value with one newline.
        let key = vec![b"foo".to_vec()];
        let value = b"hello\nworld";
        // Layout: key\0count\nvalue\n
        //         "foo" + "\0" + "1" + "\n" + "hello\nworld" + "\n"
        //          3      1     1     1       11               1   = 18
        // Note the function returns raw size (no trailing newline yet
        // collapsed into the value); count `\n` chars in value = 1
        let got = leaf_node_key_value_len(&key, value);
        // 3 + 1 (NUL after key) + 1 (digit "1") + 1 (NUL) + 11 (value) + 1 (NUL)
        assert_eq!(got, 3 + 1 + 1 + 1 + 11 + 1);
    }

    #[test]
    fn leaf_node_key_value_len_multi_segment_key_joins_with_nul() {
        let key = vec![b"a".to_vec(), b"bc".to_vec()];
        let value = b"x";
        // key joined: "a\0bc" = 4 bytes; value has 0 newlines so count="0" (1 digit).
        let got = leaf_node_key_value_len(&key, value);
        // 4 + 1 + 1 + 1 + 1 + 1
        assert_eq!(got, 9);
    }

    #[test]
    fn leaf_node_current_size_includes_header_and_drops_prefix() {
        // length=2 items, common prefix "ab" (2 bytes), raw_size=20.
        // bytes_for_items = 20 - 2*2 = 16.
        // Header: "chkleaf:\n100\n1\n2\nab\n" = 9 + 3+1 + 1+1 + 1+1 + 2+1 = 20
        // current = 20 + 16 = 36
        let got = leaf_node_current_size(100, 1, 2, 20, Some(b"ab"));
        assert_eq!(got, 36);
    }

    #[test]
    fn leaf_node_current_size_empty_prefix() {
        let got = leaf_node_current_size(15, 1, 0, 0, None);
        // "chkleaf:\n15\n1\n0\n\n" = 9 + 2+1 + 1+1 + 1+1 + 0+1 = 17
        assert_eq!(got, 17);
    }

    #[test]
    fn internal_node_current_size_adds_header_digits() {
        // raw_size=50, length=3 (1 digit), key_width=1 (1 digit),
        // maximum_size=100 (3 digits) → 50 + 1 + 1 + 3 = 55
        let got = internal_node_current_size(100, 1, 3, 50);
        assert_eq!(got, 55);
    }

    fn key_vec(parts: &[&[u8]]) -> Vec<Vec<u8>> {
        parts.iter().map(|p| p.to_vec()).collect()
    }

    #[test]
    fn leaf_node_new_starts_empty() {
        let n = LeafNode::new(SearchKeyFunc::Plain);
        assert_eq!(n.len(), 0);
        assert!(n.is_empty());
        assert_eq!(n.maximum_size, 0);
        assert_eq!(n.key_width, 1);
        assert_eq!(n.raw_size, 0);
        assert!(matches!(n.search_prefix, SearchPrefix::Computed(None)));
        assert!(n.common_serialised_prefix.is_none());
        assert!(n.key.is_none());
    }

    #[test]
    fn leaf_node_map_no_split_first_insert_sets_prefixes() {
        let mut n = LeafNode::new(SearchKeyFunc::Plain);
        let split = n.map_no_split(key_vec(&[b"foo"]), b"bar".to_vec());
        assert!(!split);
        assert_eq!(n.len(), 1);
        // With one entry, both prefixes equal the key bytes themselves.
        assert_eq!(n.common_serialised_prefix.as_deref(), Some(&b"foo"[..]));
        assert_eq!(
            n.search_prefix,
            SearchPrefix::Computed(Some(b"foo".to_vec()))
        );
        assert_eq!(
            n.raw_size,
            leaf_node_key_value_len(&[b"foo".to_vec()], b"bar")
        );
    }

    #[test]
    fn leaf_node_map_no_split_shrinks_prefix_on_divergent_key() {
        let mut n = LeafNode::new(SearchKeyFunc::Plain);
        n.map_no_split(key_vec(&[b"alpha"]), b"v1".to_vec());
        n.map_no_split(key_vec(&[b"alphabet"]), b"v2".to_vec());
        // Common prefix of "alpha" and "alphabet" is "alpha".
        assert_eq!(n.common_serialised_prefix.as_deref(), Some(&b"alpha"[..]));
        assert_eq!(
            n.search_prefix,
            SearchPrefix::Computed(Some(b"alpha".to_vec()))
        );
    }

    #[test]
    fn leaf_node_map_no_split_signals_split_when_oversize() {
        let mut n = LeafNode::new(SearchKeyFunc::Plain);
        n.maximum_size = 10;
        let _ = n.map_no_split(key_vec(&[b"foo"]), b"v1".to_vec());
        // Second entry pushes us over and the keys disagree, so we must split.
        let split = n.map_no_split(key_vec(&[b"bar"]), b"v2".to_vec());
        assert!(split);
    }

    #[test]
    fn leaf_node_unmap_removes_entry_and_recomputes_prefixes() {
        let mut n = LeafNode::new(SearchKeyFunc::Plain);
        n.map_no_split(key_vec(&[b"foo"]), b"v1".to_vec());
        n.map_no_split(key_vec(&[b"foobar"]), b"v2".to_vec());
        assert_eq!(n.common_serialised_prefix.as_deref(), Some(&b"foo"[..]));
        let removed = n.unmap(&key_vec(&[b"foobar"]));
        assert_eq!(removed, Some(b"v2".to_vec()));
        // After removing "foobar", the only key left is "foo" and the prefix
        // becomes its full bytes.
        assert_eq!(n.common_serialised_prefix.as_deref(), Some(&b"foo"[..]));
        assert_eq!(n.len(), 1);
        assert!(n.key.is_none());
    }

    #[test]
    fn leaf_node_unmap_returns_none_for_missing_key() {
        let mut n = LeafNode::new(SearchKeyFunc::Plain);
        n.map_no_split(key_vec(&[b"foo"]), b"v1".to_vec());
        assert_eq!(n.unmap(&key_vec(&[b"missing"])), None);
        // Unchanged otherwise.
        assert_eq!(n.len(), 1);
    }

    #[test]
    fn leaf_node_compute_search_prefix_resolves_unknown_state() {
        let blob: &[u8] = b"chkleaf:\n100\n1\n2\nalph\n2\x002\nv2\nv2line2\na\x001\nv1\n";
        let parsed = deserialise_leaf_node(blob).unwrap();
        let mut n = LeafNode::from_parsed(parsed, SearchKeyFunc::Plain);
        assert!(n.search_prefix.is_unknown());
        let prefix = n.compute_search_prefix().map(|s| s.to_vec());
        // Under Plain, the search keys equal the serialised keys; for
        // "alph2" and "alpha" the common prefix is "alph".
        assert_eq!(prefix, Some(b"alph".to_vec()));
        assert!(!n.search_prefix.is_unknown());
    }

    #[test]
    fn leaf_node_from_parsed_marks_search_prefix_unknown() {
        let blob: &[u8] = b"chkleaf:\n100\n1\n2\nalph\n2\x002\nv2\nv2line2\na\x001\nv1\n";
        let parsed = deserialise_leaf_node(blob).unwrap();
        let n = LeafNode::from_parsed(parsed, SearchKeyFunc::Plain);
        assert_eq!(n.len(), 2);
        assert!(n.search_prefix.is_unknown());
        assert_eq!(n.common_serialised_prefix.as_deref(), Some(&b"alph"[..]));
        // current_size reads off the raw_size; reproducing it just sanity
        // checks the wiring against leaf_node_current_size.
        assert!(n.current_size() > 0);
    }

    #[test]
    fn leaf_node_from_parsed_empty_keeps_prefix_none() {
        let blob: &[u8] = b"chkleaf:\n100\n1\n0\n\n";
        let parsed = deserialise_leaf_node(blob).unwrap();
        let n = LeafNode::from_parsed(parsed, SearchKeyFunc::Plain);
        assert_eq!(n.len(), 0);
        assert!(matches!(n.search_prefix, SearchPrefix::Computed(None)));
        assert!(n.common_serialised_prefix.is_none());
    }

    #[test]
    fn internal_node_new_starts_empty_with_prefix() {
        let n = InternalNode::new(b"pre".to_vec(), SearchKeyFunc::Plain);
        assert_eq!(n.len, 0);
        assert_eq!(n.node_width, 0);
        assert_eq!(n.maximum_size, 0);
        assert_eq!(n.search_prefix.as_deref(), Some(&b"pre"[..]));
        assert!(n.items.is_empty());
        assert!(n.key.is_none());
    }

    #[test]
    fn internal_node_from_parsed_marks_children_unloaded() {
        let blob: &[u8] = b"chknode:\n200\n1\n2\npre\nbar\x00sha1:bbbb\nfoo\x00sha1:aaaa\n";
        let parsed = deserialise_internal_node(blob).unwrap();
        let n = InternalNode::from_parsed(parsed, SearchKeyFunc::Plain);
        assert_eq!(n.len, 2);
        assert_eq!(n.search_prefix.as_deref(), Some(&b"pre"[..]));
        assert_eq!(n.maximum_size, 200);
        assert_eq!(n.key_width, 1);
        assert_eq!(n.items.len(), 2);
        // Verify both are Unloaded with the right sha1 keys.
        match n.items.get(&b"prebar".to_vec()) {
            Some(NodeRef::Unloaded(k)) => assert_eq!(k, b"sha1:bbbb"),
            other => panic!("expected Unloaded for prebar, got {:?}", other),
        }
        match n.items.get(&b"prefoo".to_vec()) {
            Some(NodeRef::Unloaded(k)) => assert_eq!(k, b"sha1:aaaa"),
            other => panic!("expected Unloaded for prefoo, got {:?}", other),
        }
    }

    #[test]
    fn internal_node_add_node_validates_prefix_length() {
        let mut n = InternalNode::new(b"pre".to_vec(), SearchKeyFunc::Plain);
        let leaf = Node::Leaf(Box::new(LeafNode::new(SearchKeyFunc::Plain)));
        // Prefix too long.
        let too_long = n.add_node(b"preXY".to_vec(), leaf.clone());
        assert!(matches!(too_long, Err(Error::AssertionFailed(_))));
        // Prefix not starting with search_prefix.
        let mismatch = n.add_node(b"qrZ".to_vec(), leaf.clone());
        assert!(matches!(mismatch, Err(Error::AssertionFailed(_))));
        // Correct: search_prefix + 1 byte.
        n.add_node(b"preX".to_vec(), leaf).unwrap();
        assert_eq!(n.items.len(), 1);
        assert_eq!(n.node_width, 4);
    }

    #[test]
    fn internal_node_add_node_clears_key_and_grows_len() {
        let mut n = InternalNode::new(b"".to_vec(), SearchKeyFunc::Plain);
        n.key = Some(b"sha1:old".to_vec());
        let mut leaf = LeafNode::new(SearchKeyFunc::Plain);
        leaf.map_no_split(key_vec(&[b"k1"]), b"v1".to_vec());
        leaf.map_no_split(key_vec(&[b"k2"]), b"v2".to_vec());
        n.add_node(b"a".to_vec(), Node::Leaf(Box::new(leaf))).unwrap();
        assert_eq!(n.len, 2);
        assert!(n.key.is_none());
    }

    #[test]
    fn internal_node_search_key_pads_with_nul() {
        let mut n = InternalNode::new(b"".to_vec(), SearchKeyFunc::Plain);
        n.node_width = 5;
        // Plain search key of `("k",)` is `b"k"`, length 1; pad to 5.
        assert_eq!(n.search_key(&Key::from(vec![b"k".to_vec()])), b"k\0\0\0\0");
    }

    #[test]
    fn internal_node_search_key_truncates_when_longer() {
        let mut n = InternalNode::new(b"".to_vec(), SearchKeyFunc::Plain);
        n.node_width = 3;
        assert_eq!(
            n.search_key(&Key::from(vec![b"longkey".to_vec()])),
            b"lon"
        );
    }

    #[test]
    fn internal_node_search_prefix_filter_does_not_pad() {
        let mut n = InternalNode::new(b"".to_vec(), SearchKeyFunc::Plain);
        n.node_width = 5;
        // Shorter key returns as-is (no padding).
        assert_eq!(
            n.search_prefix_filter(&Key::from(vec![b"k".to_vec()])),
            b"k"
        );
        // Longer key gets truncated.
        n.node_width = 3;
        assert_eq!(
            n.search_prefix_filter(&Key::from(vec![b"longkey".to_vec()])),
            b"lon"
        );
    }

    #[test]
    fn internal_node_compute_search_prefix_from_children() {
        let mut n = InternalNode::new(b"".to_vec(), SearchKeyFunc::Plain);
        n.items
            .insert(b"abc".to_vec(), NodeRef::Unloaded(b"sha1:x".to_vec()));
        n.items
            .insert(b"abd".to_vec(), NodeRef::Unloaded(b"sha1:y".to_vec()));
        n.items
            .insert(b"abe".to_vec(), NodeRef::Unloaded(b"sha1:z".to_vec()));
        let prefix = n.compute_search_prefix().map(<[u8]>::to_vec);
        assert_eq!(prefix, Some(b"ab".to_vec()));
        assert_eq!(n.search_prefix.as_deref(), Some(&b"ab"[..]));
    }

    #[test]
    fn internal_node_refs_returns_child_keys() {
        let mut n = InternalNode::new(b"p".to_vec(), SearchKeyFunc::Plain);
        n.node_width = 2;
        n.items
            .insert(b"pa".to_vec(), NodeRef::Unloaded(b"sha1:a".to_vec()));
        let mut leaf = LeafNode::new(SearchKeyFunc::Plain);
        leaf.key = Some(b"sha1:b".to_vec());
        n.items
            .insert(b"pb".to_vec(), NodeRef::Loaded(Node::Leaf(Box::new(leaf))));
        n.key = Some(b"sha1:self".to_vec());
        let refs = n.refs().unwrap();
        assert_eq!(refs.len(), 2);
        assert!(refs.contains(&b"sha1:a".to_vec()));
        assert!(refs.contains(&b"sha1:b".to_vec()));
    }

    #[test]
    fn internal_node_refs_errors_when_unserialised() {
        let n = InternalNode::new(b"".to_vec(), SearchKeyFunc::Plain);
        assert!(matches!(n.refs(), Err(Error::AssertionFailed(_))));
    }

    #[test]
    fn node_wraps_leaf_and_internal() {
        let leaf = Node::Leaf(Box::new(LeafNode::new(SearchKeyFunc::Plain)));
        let internal = Node::Internal(Box::new(InternalNode::new(
            b"".to_vec(),
            SearchKeyFunc::Plain,
        )));
        assert_eq!(leaf.len(), 0);
        assert_eq!(internal.len(), 0);
        assert_eq!(leaf.maximum_size(), 0);
        assert_eq!(internal.maximum_size(), 0);
        assert_eq!(leaf.key(), None);
        assert_eq!(internal.key(), None);
    }

    #[test]
    fn node_len_reflects_inner_state() {
        let mut leaf = LeafNode::new(SearchKeyFunc::Plain);
        leaf.map_no_split(key_vec(&[b"foo"]), b"bar".to_vec());
        leaf.map_no_split(key_vec(&[b"baz"]), b"qux".to_vec());
        let node = Node::Leaf(Box::new(leaf));
        assert_eq!(node.len(), 2);
    }

    #[test]
    fn leaf_node_iteritems_returns_all_with_no_filter() {
        let mut n = LeafNode::new(SearchKeyFunc::Plain);
        n.map_no_split(key_vec(&[b"foo"]), b"bar".to_vec());
        n.map_no_split(key_vec(&[b"baz"]), b"qux".to_vec());
        let items = n.iteritems(None);
        assert_eq!(items.len(), 2);
        let keys: Vec<&[Vec<u8>]> = items.iter().map(|(k, _)| k.as_slice()).collect();
        assert!(keys.contains(&key_vec(&[b"foo"]).as_slice()));
        assert!(keys.contains(&key_vec(&[b"baz"]).as_slice()));
    }

    #[test]
    fn leaf_node_iteritems_exact_match_filter() {
        let mut n = LeafNode::new(SearchKeyFunc::Plain);
        n.map_no_split(key_vec(&[b"foo"]), b"bar".to_vec());
        n.map_no_split(key_vec(&[b"baz"]), b"qux".to_vec());
        let items = n.iteritems(Some(&[key_vec(&[b"foo"])]));
        assert_eq!(items, vec![(key_vec(&[b"foo"]), b"bar".to_vec())]);
    }

    #[test]
    fn leaf_node_iteritems_exact_miss_yields_nothing() {
        let mut n = LeafNode::new(SearchKeyFunc::Plain);
        n.map_no_split(key_vec(&[b"foo"]), b"bar".to_vec());
        let items = n.iteritems(Some(&[key_vec(&[b"absent"])]));
        assert!(items.is_empty());
    }

    #[test]
    fn leaf_node_iteritems_short_prefix_filter_matches_items() {
        // key_width=2, filter is a 1-element prefix that matches the
        // first element of one stored key.
        let mut n = LeafNode::new(SearchKeyFunc::Plain);
        n.key_width = 2;
        n.map_no_split(key_vec(&[b"foo", b"sub1"]), b"v1".to_vec());
        n.map_no_split(key_vec(&[b"bar", b"sub2"]), b"v2".to_vec());
        let items = n.iteritems(Some(&[key_vec(&[b"foo"])]));
        assert_eq!(items, vec![(key_vec(&[b"foo", b"sub1"]), b"v1".to_vec())]);
    }

    #[test]
    fn leaf_node_iteritems_mixed_filter_lengths() {
        // key_width=2, mix of exact (length 2) and prefix (length 1).
        let mut n = LeafNode::new(SearchKeyFunc::Plain);
        n.key_width = 2;
        n.map_no_split(key_vec(&[b"foo", b"sub"]), b"v1".to_vec());
        n.map_no_split(key_vec(&[b"bar", b"sub"]), b"v2".to_vec());
        n.map_no_split(key_vec(&[b"baz", b"sub"]), b"v3".to_vec());
        let items = n.iteritems(Some(&[
            key_vec(&[b"foo", b"sub"]), // exact, yielded first
            key_vec(&[b"bar"]),         // prefix
        ]));
        // Exact match first, then prefix match in items order.
        assert_eq!(items.len(), 2);
        assert_eq!(items[0], (key_vec(&[b"foo", b"sub"]), b"v1".to_vec()));
        assert_eq!(items[1], (key_vec(&[b"bar", b"sub"]), b"v2".to_vec()));
    }

    #[test]
    fn search_key_func_from_name_resolves_known_variants() {
        assert_eq!(
            SearchKeyFunc::from_name(b"plain").unwrap(),
            SearchKeyFunc::Plain
        );
        assert_eq!(
            SearchKeyFunc::from_name(b"hash-16-way").unwrap(),
            SearchKeyFunc::Hash16Way
        );
        assert_eq!(
            SearchKeyFunc::from_name(b"hash-255-way").unwrap(),
            SearchKeyFunc::Hash255Way
        );
    }

    #[test]
    fn search_key_func_from_name_returns_unknown_bytes() {
        let err = SearchKeyFunc::from_name(b"unrecognised").unwrap_err();
        assert_eq!(err, b"unrecognised".to_vec());
    }

    #[test]
    fn search_key_func_name_roundtrips() {
        for variant in [
            SearchKeyFunc::Plain,
            SearchKeyFunc::Hash16Way,
            SearchKeyFunc::Hash255Way,
        ] {
            let parsed = SearchKeyFunc::from_name(variant.name()).unwrap();
            assert_eq!(parsed, variant);
        }
    }

    #[test]
    fn search_key_func_apply_matches_free_functions() {
        let k = key(&[b"foo", b"bar"]);
        assert_eq!(SearchKeyFunc::Plain.apply(&k), search_key_plain(&k));
        assert_eq!(SearchKeyFunc::Hash16Way.apply(&k), search_key_16(&k));
        assert_eq!(SearchKeyFunc::Hash255Way.apply(&k), search_key_255(&k));
    }

    #[test]
    fn search_key_func_default_is_plain() {
        assert_eq!(SearchKeyFunc::default(), SearchKeyFunc::Plain);
    }

    #[test]
    fn are_search_keys_identical_returns_true_for_empty() {
        let empty: [&[u8]; 0] = [];
        assert!(are_search_keys_identical(empty));
    }

    #[test]
    fn are_search_keys_identical_returns_true_for_single() {
        assert!(are_search_keys_identical([b"hash".as_slice()]));
    }

    #[test]
    fn are_search_keys_identical_returns_true_when_all_match() {
        assert!(are_search_keys_identical([
            b"same".as_slice(),
            b"same",
            b"same"
        ]));
    }

    #[test]
    fn are_search_keys_identical_returns_false_when_one_differs() {
        assert!(!are_search_keys_identical([
            b"same".as_slice(),
            b"same",
            b"other"
        ]));
        assert!(!are_search_keys_identical([b"first".as_slice(), b"second"]));
    }

    #[test]
    fn serialise_internal_node_roundtrips_through_deserialise() {
        let items = vec![
            InternalNodeChild {
                prefix: b"prebar".to_vec(),
                flat_key: b"sha1:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_vec(),
            },
            InternalNodeChild {
                prefix: b"prefoo".to_vec(),
                flat_key: b"sha1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_vec(),
            },
        ];
        // The fixture's `length=2` happens to match items.len() because
        // those two children are themselves leaves; for deeper trees
        // length is the total leaf count, which the caller passes in.
        let out = serialise_internal_node(200, 1, 2, b"pre", &items).unwrap();
        let blob: Vec<u8> = out.iter().flatten().copied().collect();
        assert_eq!(blob, INTERNAL_FIXTURE);
    }
}
