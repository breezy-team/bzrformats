//! Key-to-partition mappers used by versioned-file storage layouts.
//!
//! These map a key tuple's first element (a `file-id` style byte string) to a
//! partition identifier (a `String`) used as a relative storage path, and back.
//! The Python originals live in `bzrformats.versionedfile`.

use adler::adler32_slice;
use percent_encoding::{percent_decode_str, percent_encode, AsciiSet, CONTROLS};

/// Characters Python's `urllib.parse.quote(s, safe='/')` percent-encodes:
/// everything except the unreserved set `A-Za-z0-9_.-~` and the `/` separator.
const QUOTE_SET: &AsciiSet = &CONTROLS
    .add(b' ')
    .add(b'!')
    .add(b'"')
    .add(b'#')
    .add(b'$')
    .add(b'%')
    .add(b'&')
    .add(b'\'')
    .add(b'(')
    .add(b')')
    .add(b'*')
    .add(b'+')
    .add(b',')
    .add(b':')
    .add(b';')
    .add(b'<')
    .add(b'=')
    .add(b'>')
    .add(b'?')
    .add(b'@')
    .add(b'[')
    .add(b'\\')
    .add(b']')
    .add(b'^')
    .add(b'`')
    .add(b'{')
    .add(b'|')
    .add(b'}')
    .add(0x7f);

/// Translate between key tuples and storage paths.
///
/// Implementations mirror the Python `KeyMapper` hierarchy:
/// [`ConstantMapper`], `PrefixMapper`, `HashPrefixMapper`, etc.
/// The pyo3 layer provides a `PyMapper` adapter so pure-Rust code
/// accepts any Python mapper object.
pub trait Mapper: Send + Sync {
    /// Map a key (sequence of byte segments) to a relative storage path.
    fn map(&self, key: &[&[u8]]) -> String;
    /// Invert `map`, recovering the prefix bytes from a storage path.
    fn unmap(&self, path: &str) -> Vec<Vec<u8>>;
    /// Return true if every key maps to the same path (i.e. this is a
    /// `ConstantMapper`). Used by `KndxIndex::keys` to skip the file-scan
    /// path and by `load_prefix_inner` to decide whether to create the index
    /// file when it is missing.
    fn is_constant(&self) -> bool {
        false
    }
}

/// A `Mapper` that always returns the same path regardless of the key.
///
/// Mirrors `bzrformats.versionedfile.ConstantMapper`.
#[derive(Clone)]
pub struct ConstantMapper {
    pub result: String,
}

impl Mapper for ConstantMapper {
    fn map(&self, _key: &[&[u8]]) -> String {
        self.result.clone()
    }

    fn unmap(&self, _path: &str) -> Vec<Vec<u8>> {
        vec![]
    }

    fn is_constant(&self) -> bool {
        true
    }
}

/// A `Mapper` that uses the first key element as the storage path (url-quoted).
///
/// Mirrors `bzrformats.versionedfile.PrefixMapper`.
pub struct PrefixMapper;

impl Mapper for PrefixMapper {
    fn map(&self, key: &[&[u8]]) -> String {
        prefix_map(key[0])
    }

    fn unmap(&self, path: &str) -> Vec<Vec<u8>> {
        vec![prefix_unmap(path)]
    }
}

/// A `Mapper` that prefixes the path with a two-hex adler32 bucket.
///
/// Mirrors `bzrformats.versionedfile.HashPrefixMapper`.
#[derive(Clone)]
pub struct HashPrefixMapper;

impl Mapper for HashPrefixMapper {
    fn map(&self, key: &[&[u8]]) -> String {
        hash_prefix_map(key[0])
    }

    fn unmap(&self, path: &str) -> Vec<Vec<u8>> {
        vec![hash_prefix_unmap(path)]
    }
}

/// A `Mapper` that escapes non-filesystem-safe bytes before bucketing.
///
/// Mirrors `bzrformats.versionedfile.HashEscapedPrefixMapper`.
pub struct HashEscapedPrefixMapper;

impl Mapper for HashEscapedPrefixMapper {
    fn map(&self, key: &[&[u8]]) -> String {
        hash_escaped_prefix_map(key[0])
    }

    fn unmap(&self, path: &str) -> Vec<Vec<u8>> {
        vec![hash_escaped_prefix_unmap(path)]
    }
}

/// Percent-encode `s` matching Python's `urllib.parse.quote(s, safe='/')`.
///
/// Safe characters are ASCII letters, digits, `_.-~` and `/`.
pub(crate) fn url_quote(s: &str) -> String {
    percent_encode(s.as_bytes(), QUOTE_SET).to_string()
}

/// Percent-decode `s` matching Python's `urllib.parse.unquote(s)`.
///
/// `%xx` sequences are decoded as raw bytes; the resulting byte sequence is
/// interpreted as UTF-8. A malformed `%xx` sequence is left as-is, like Python,
/// and invalid UTF-8 is replaced with U+FFFD.
pub(crate) fn url_unquote(s: &str) -> String {
    percent_decode_str(s).decode_utf8_lossy().into_owned()
}

fn basename(path: &str) -> &str {
    match path.rfind('/') {
        Some(i) => &path[i + 1..],
        None => path,
    }
}

/// `PrefixMapper.map`: take the first element of the key as UTF-8 and quote it.
pub fn prefix_map(prefix: &[u8]) -> String {
    let s = std::str::from_utf8(prefix).expect("prefix must be valid UTF-8");
    url_quote(s)
}

/// `PrefixMapper.unmap`: undo `prefix_map`, returning the raw bytes.
pub fn prefix_unmap(partition_id: &str) -> Vec<u8> {
    url_unquote(partition_id).into_bytes()
}

/// `HashPrefixMapper.map`: prepend an adler32-derived two-hex-char bucket.
pub fn hash_prefix_map(prefix: &[u8]) -> String {
    let bucket = (adler32_slice(prefix) & 0xff) as u8;
    let s = std::str::from_utf8(prefix).expect("prefix must be valid UTF-8");
    url_quote(&format!("{:02x}/{}", bucket, s))
}

/// `HashPrefixMapper.unmap`: drop the bucket and return the raw bytes.
pub fn hash_prefix_unmap(partition_id: &str) -> Vec<u8> {
    let unquoted = url_unquote(partition_id);
    basename(&unquoted).as_bytes().to_vec()
}

/// Filesystem-safe characters used by `HashEscapedPrefixMapper._escape`.
fn is_escaped_safe(b: u8) -> bool {
    matches!(b,
        b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'@' | b',' | b'.')
}

fn escape_prefix(prefix: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(prefix.len());
    for &b in prefix {
        if is_escaped_safe(b) {
            out.push(b);
        } else {
            out.extend_from_slice(format!("%{:02x}", b).as_bytes());
        }
    }
    out
}

/// `HashEscapedPrefixMapper.map`: escape the prefix into a filesystem-safe
/// ASCII form, then apply `hash_prefix_map`-style bucketing and url-quoting.
pub fn hash_escaped_prefix_map(prefix: &[u8]) -> String {
    let escaped = escape_prefix(prefix);
    let bucket = (adler32_slice(&escaped) & 0xff) as u8;
    let escaped_str = std::str::from_utf8(&escaped).expect("escaped prefix is ASCII");
    url_quote(&format!("{:02x}/{}", bucket, escaped_str))
}

/// `HashEscapedPrefixMapper.unmap`: undo url-quoting, drop the bucket, then
/// undo the inner percent-escape to recover the original raw bytes.
pub fn hash_escaped_prefix_unmap(partition_id: &str) -> Vec<u8> {
    let unquoted = url_unquote(partition_id);
    let base = basename(&unquoted);
    url_unquote(base).into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_mapper_roundtrips() {
        assert_eq!(prefix_map(b"file-id"), "file-id");
        assert_eq!(prefix_map(b"new-id"), "new-id");
        assert_eq!(prefix_unmap("file-id"), b"file-id");
        assert_eq!(prefix_unmap("new-id"), b"new-id");
    }

    #[test]
    fn hash_prefix_mapper_matches_python() {
        assert_eq!(hash_prefix_map(b"file-id"), "9b/file-id");
        assert_eq!(hash_prefix_map(b"new-id"), "45/new-id");
        assert_eq!(hash_prefix_unmap("9b/file-id"), b"file-id");
        assert_eq!(hash_prefix_unmap("45/new-id"), b"new-id");
    }

    #[test]
    fn hash_escaped_prefix_mapper_matches_python() {
        assert_eq!(hash_escaped_prefix_map(b" "), "88/%2520");
        assert_eq!(hash_escaped_prefix_map(b"filE-Id"), "ed/fil%2545-%2549d");
        assert_eq!(hash_escaped_prefix_map(b"neW-Id"), "88/ne%2557-%2549d");
        assert_eq!(hash_escaped_prefix_unmap("ed/fil%2545-%2549d"), b"filE-Id");
        assert_eq!(hash_escaped_prefix_unmap("88/ne%2557-%2549d"), b"neW-Id");
    }

    #[test]
    fn url_quote_handles_special_chars() {
        assert_eq!(url_quote("a b"), "a%20b");
        assert_eq!(url_quote("a/b"), "a/b");
        assert_eq!(url_quote("a%b"), "a%25b");
    }

    #[test]
    fn url_unquote_handles_special_chars() {
        assert_eq!(url_unquote("a%20b"), "a b");
        assert_eq!(url_unquote("a%25b"), "a%b");
        assert_eq!(url_unquote("a%2zb"), "a%2zb");
    }

    #[test]
    fn url_quote_emits_uppercase_hex() {
        // Python's quote() emits uppercase hex digits.
        assert_eq!(url_quote("\x7f"), "%7F");
        assert_eq!(url_quote("\u{e9}"), "%C3%A9");
    }

    #[test]
    fn url_unquote_matches_python_edge_cases() {
        // Trailing %xx still decodes; a bare or truncated % is left as-is.
        assert_eq!(url_unquote("a%20"), "a ");
        assert_eq!(url_unquote("%20"), " ");
        assert_eq!(url_unquote("%"), "%");
        assert_eq!(url_unquote("a%2"), "a%2");
    }
}
