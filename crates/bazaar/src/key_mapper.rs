//! Key-to-partition mappers used by versioned-file storage layouts.
//!
//! These map a key tuple's first element (a `file-id` style byte string) to a
//! partition identifier (a `String`) used as a relative storage path, and back.
//! The Python originals live in `bzrformats.versionedfile`.

use adler::adler32_slice;

/// Percent-encode `s` matching Python's `urllib.parse.quote(s, safe='/')`.
///
/// Safe characters are ASCII letters, digits, `_.-~` and `/`.
fn url_quote(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.as_bytes() {
        if is_url_safe(*b) {
            out.push(*b as char);
        } else {
            out.push('%');
            out.push_str(&format!("{:02X}", b));
        }
    }
    out
}

fn is_url_safe(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-' | b'~' | b'/')
}

/// Percent-decode `s` matching Python's `urllib.parse.unquote(s)`.
///
/// `%xx` sequences are decoded as raw bytes; the resulting byte sequence is
/// interpreted as UTF-8. A malformed `%xx` sequence is left as-is, like Python.
fn url_unquote(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(h), Some(l)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                out.push((h << 4) | l);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    // Python's unquote replaces invalid UTF-8 with U+FFFD by default.
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
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
}
