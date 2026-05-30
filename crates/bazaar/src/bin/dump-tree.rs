//! Dump the contents of a B+Tree graph index file.
//!
//! Ported from breezy's `brz dump-btree` command. By default the parsed
//! `(key, value, references)` tuples are printed, one per line, in the
//! same form as Python's `repr`. With `--raw` the pages are decompressed
//! and their raw bytes written out instead.

use std::fs;
use std::process::ExitCode;

use bazaar::btree_index::{
    decompress_page, parse_btree_header, parse_leaf_lines, BTreeHeader, LeafEntry, LeafKey,
    LeafRefList, PAGE_SIZE,
};

const USAGE: &str = "Usage: dump-tree [--raw] PATH

Dump the contents of a btree index file to stdout.

PATH is a btree index file, such as .bzr/repository/pack-names or one of
the .bzr/repository/indices/*.iix files.

By default the tuples stored in the index file are displayed. With --raw
the pages are uncompressed and their raw bytes are written instead.";

fn main() -> ExitCode {
    let mut raw = false;
    let mut path: Option<String> = None;
    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "--raw" => raw = true,
            "-h" | "--help" => {
                println!("{USAGE}");
                return ExitCode::SUCCESS;
            }
            other if other.starts_with('-') => {
                eprintln!("dump-tree: unknown option {other}\n\n{USAGE}");
                return ExitCode::FAILURE;
            }
            other => {
                if path.is_some() {
                    eprintln!("dump-tree: too many arguments\n\n{USAGE}");
                    return ExitCode::FAILURE;
                }
                path = Some(other.to_string());
            }
        }
    }
    let Some(path) = path else {
        eprintln!("dump-tree: missing PATH argument\n\n{USAGE}");
        return ExitCode::FAILURE;
    };

    match run(&path, raw) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("dump-tree: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run(path: &str, raw: bool) -> Result<(), String> {
    let bytes = fs::read(path).map_err(|e| format!("cannot read {path}: {e}"))?;
    let header = parse_btree_header(&bytes).map_err(|e| format!("not a btree index: {e}"))?;
    let mut out = std::io::stdout().lock();
    if raw {
        dump_raw_bytes(&mut out, &bytes, &header)
    } else {
        dump_entries(&mut out, &bytes, &header)
    }
    .map_err(|e| format!("write error: {e}"))
}

/// The decompressed body of a single page.
///
/// Page 0 carries the file header in its first `header_end` bytes; every
/// other page is a full compressed payload. Returns `None` for a page
/// whose payload is empty (an empty index has no leaf data on page 0).
fn page_body(
    bytes: &[u8],
    page_idx: usize,
    header: &BTreeHeader,
) -> Result<Option<Vec<u8>>, String> {
    let page_start = page_idx * PAGE_SIZE;
    let page_end = std::cmp::min(page_start + PAGE_SIZE, bytes.len());
    let mut payload = &bytes[page_start..page_end];
    if page_idx == 0 {
        payload = &payload[header.header_end..];
    }
    if payload.is_empty() {
        return Ok(None);
    }
    decompress_page(payload)
        .map(Some)
        .map_err(|e| format!("bad btree node on page {page_idx}: {e}"))
}

fn dump_raw_bytes<W: std::io::Write>(
    out: &mut W,
    bytes: &[u8],
    header: &BTreeHeader,
) -> std::io::Result<()> {
    // The header bytes are written verbatim from page 0; the rest of each
    // page is the decompressed body. Mirrors `_dump_raw_bytes`.
    let page_count = bytes.len().div_ceil(PAGE_SIZE).max(1);
    for page_idx in 0..page_count {
        let page_start = page_idx * PAGE_SIZE;
        let page_end = std::cmp::min(page_start + PAGE_SIZE, bytes.len());
        let mut payload = &bytes[page_start..page_end];
        if page_idx == 0 {
            out.write_all(b"Root node:\n")?;
            out.write_all(&bytes[..header.header_end])?;
            payload = &payload[header.header_end..];
        }
        write!(out, "\nPage {page_idx}\n")?;
        if payload.is_empty() {
            out.write_all(b"(empty)\n")?;
            continue;
        }
        let decompressed = decompress_page(payload).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("bad btree node on page {page_idx}: {e}"),
            )
        })?;
        out.write_all(&decompressed)?;
        out.write_all(b"\n")?;
    }
    Ok(())
}

fn dump_entries<W: std::io::Write>(
    out: &mut W,
    bytes: &[u8],
    header: &BTreeHeader,
) -> std::io::Result<()> {
    // Leaf pages are the last row of the tree. row_lengths is top-first,
    // so the leaves occupy the final `row_lengths.last()` pages; when there
    // is a single row the only leaf page is page 0.
    let leaf_count = header.row_lengths.last().copied().unwrap_or(0);
    let total_pages: usize = header.row_lengths.iter().sum();
    let leaf_start = total_pages - leaf_count;
    let has_refs = header.node_ref_lists > 0;
    for page_idx in leaf_start..total_pages {
        let body = match page_body(bytes, page_idx, header)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?
        {
            Some(body) => body,
            None => continue,
        };
        let entries =
            parse_leaf_lines(&body, header.key_length, header.node_ref_lists).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("bad leaf node on page {page_idx}: {e}"),
                )
            })?;
        // _LeafNode.all_items yields keys in sorted order.
        let mut sorted: Vec<&LeafEntry> = entries.iter().collect();
        sorted.sort_by(|a, b| a.0.cmp(&b.0));
        for (key, value, refs) in sorted {
            out.write_all(format_entry(key, value, refs, has_refs).as_bytes())?;
            out.write_all(b"\n")?;
        }
    }
    Ok(())
}

/// Render one entry the way breezy's `_dump_entries` does: a Python tuple
/// `repr` of `(key, value, references)`, where `references` is `None` when
/// the index has no reference lists.
fn format_entry(key: &LeafKey, value: &[u8], refs: &[LeafRefList], has_refs: bool) -> String {
    let key_part = key_repr(key);
    let value_repr = bytes_str_repr(value);
    let refs_repr = if has_refs {
        tuple_repr(
            refs.iter()
                .map(|ref_list| tuple_repr(ref_list.iter().map(key_repr))),
        )
    } else {
        "None".to_string()
    };
    format!("({key_part}, {value_repr}, {refs_repr})")
}

/// Render a key tuple: each `\0`-joined segment as a Python `str` repr.
fn key_repr(key: &LeafKey) -> String {
    tuple_repr(key.iter().map(|seg| bytes_str_repr(seg)))
}

/// Render an iterator of already-formatted items as a Python tuple literal,
/// including the trailing comma for a one-element tuple.
fn tuple_repr<I: ExactSizeIterator<Item = String>>(items: I) -> String {
    let len = items.len();
    let mut s = String::from("(");
    for (i, item) in items.enumerate() {
        if i > 0 {
            s.push_str(", ");
        }
        s.push_str(&item);
    }
    if len == 1 {
        s.push(',');
    }
    s.push(')');
    s
}

/// Decode `bytes` as UTF-8 (lossily, matching the command's `.decode`) and
/// render it as Python's `str` repr would.
fn bytes_str_repr(bytes: &[u8]) -> String {
    let s = String::from_utf8_lossy(bytes);
    py_str_repr(&s)
}

/// Reproduce CPython's `repr()` of a `str`: choose `'` quotes unless the
/// string contains `'` but not `"`, and escape backslashes, the quote
/// character, and non-printable code points.
fn py_str_repr(s: &str) -> String {
    let quote = if s.contains('\'') && !s.contains('"') {
        '"'
    } else {
        '\''
    };
    let mut out = String::with_capacity(s.len() + 2);
    out.push(quote);
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c == quote => {
                out.push('\\');
                out.push(c);
            }
            c if is_py_printable(c) => out.push(c),
            c => {
                let cp = c as u32;
                if cp <= 0xff {
                    out.push_str(&format!("\\x{cp:02x}"));
                } else if cp <= 0xffff {
                    out.push_str(&format!("\\u{cp:04x}"));
                } else {
                    out.push_str(&format!("\\U{cp:08x}"));
                }
            }
        }
    }
    out.push(quote);
    out
}

/// Approximate `str.isprintable()` for the characters that appear in index
/// keys/values: ASCII printables stay literal, ASCII control characters are
/// escaped. Non-ASCII is kept literal (Python keeps printable Unicode as-is,
/// and index data is overwhelmingly ASCII).
fn is_py_printable(c: char) -> bool {
    if c.is_ascii() {
        !c.is_ascii_control()
    } else {
        !c.is_control()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bazaar::btree_builder::BTreeBuilder;

    fn seg(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    fn key(parts: &[&str]) -> LeafKey {
        parts.iter().map(|p| seg(p)).collect()
    }

    #[test]
    fn str_repr_picks_quotes_and_escapes() {
        assert_eq!(py_str_repr("value"), "'value'");
        // A single quote in the text switches to double quotes.
        assert_eq!(py_str_repr("it's"), "\"it's\"");
        // With both quote kinds present, single quotes are used and escaped.
        assert_eq!(py_str_repr("a'b\"c"), "'a\\'b\"c'");
        assert_eq!(py_str_repr("a\tb\nc"), "'a\\tb\\nc'");
        assert_eq!(py_str_repr("\x01"), "'\\x01'");
    }

    #[test]
    fn one_element_tuple_keeps_trailing_comma() {
        assert_eq!(
            tuple_repr(vec!["'a'".to_string()].into_iter()),
            "('a',)".to_string()
        );
        assert_eq!(
            tuple_repr(vec!["'a'".to_string(), "'b'".to_string()].into_iter()),
            "('a', 'b')".to_string()
        );
    }

    #[test]
    fn entry_with_refs_matches_python_repr() {
        let refs = vec![vec![key(&["ref", "entry"])]];
        let got = format_entry(&key(&["test", "key1"]), b"value", &refs, true);
        assert_eq!(got, "(('test', 'key1'), 'value', ((('ref', 'entry'),),))");
    }

    #[test]
    fn entry_without_refs_renders_none() {
        let got = format_entry(&key(&["test", "key1"]), b"value", &[], false);
        assert_eq!(got, "(('test', 'key1'), 'value', None)");
    }

    fn sample_index() -> Vec<u8> {
        let mut builder = BTreeBuilder::new(1, 2);
        builder
            .add_node(
                key(&["test", "key1"]),
                seg("value"),
                vec![vec![key(&["ref", "entry"])]],
            )
            .unwrap();
        builder
            .add_node(
                key(&["test", "key2"]),
                seg("value2"),
                vec![vec![key(&["ref", "entry2"])]],
            )
            .unwrap();
        builder
            .add_node(
                key(&["test2", "key3"]),
                seg("value3"),
                vec![vec![key(&["ref", "entry3"])]],
            )
            .unwrap();
        builder.finish().unwrap()
    }

    fn dump(bytes: &[u8], raw: bool) -> String {
        let header = parse_btree_header(bytes).unwrap();
        let mut out = Vec::new();
        if raw {
            dump_raw_bytes(&mut out, bytes, &header).unwrap();
        } else {
            dump_entries(&mut out, bytes, &header).unwrap();
        }
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn dump_entries_matches_dump_btree_command() {
        assert_eq!(
            dump(&sample_index(), false),
            "(('test', 'key1'), 'value', ((('ref', 'entry'),),))\n\
             (('test', 'key2'), 'value2', ((('ref', 'entry2'),),))\n\
             (('test2', 'key3'), 'value3', ((('ref', 'entry3'),),))\n"
        );
    }

    #[test]
    fn dump_raw_matches_dump_btree_raw_command() {
        assert_eq!(
            dump(&sample_index(), true),
            "Root node:\n\
             B+Tree Graph Index 2\n\
             node_ref_lists=1\n\
             key_elements=2\n\
             len=3\n\
             row_lengths=1\n\
             \n\
             Page 0\n\
             type=leaf\n\
             test\0key1\0ref\0entry\0value\n\
             test\0key2\0ref\0entry2\0value2\n\
             test2\0key3\0ref\0entry3\0value3\n\
             \n"
        );
    }

    #[test]
    fn empty_index_dumps_nothing_and_empty_page() {
        let empty = BTreeBuilder::new(1, 2).finish().unwrap();
        assert_eq!(dump(&empty, false), "");
        assert_eq!(
            dump(&empty, true),
            "Root node:\n\
             B+Tree Graph Index 2\n\
             node_ref_lists=1\n\
             key_elements=2\n\
             len=0\n\
             row_lengths=\n\
             \n\
             Page 0\n\
             (empty)\n"
        );
    }

    #[test]
    fn no_ref_lists_render_none() {
        let mut builder = BTreeBuilder::new(0, 2);
        builder
            .add_node(key(&["test", "key1"]), seg("value"), vec![])
            .unwrap();
        let bytes = builder.finish().unwrap();
        assert_eq!(dump(&bytes, false), "(('test', 'key1'), 'value', None)\n");
    }
}
