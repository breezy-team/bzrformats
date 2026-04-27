//! Dirstate header parsing and serialisation.
//!
//! Python's `DirState._read_header` / `_read_prelude` read five
//! newline-delimited lines off the top of the state file:
//!
//! 1. The format banner (`"#bazaar dirstate flat format 3\n"`)
//! 2. `crc32: <decimal>`
//! 3. `num_entries: <decimal>`
//! 4. parents list (NUL-separated)
//! 5. ghosts list (NUL-separated)
//!
//! This module owns those parsers, the inverse ghost/parents
//! serialisation, and the full-file `get_output_lines` wrapper.

pub const HEADER_FORMAT_2: &[u8] = b"#bazaar dirstate flat format 2\n";
pub const HEADER_FORMAT_3: &[u8] = b"#bazaar dirstate flat format 3\n";

/// Default bisect page size used when scanning the dirstate file on disk.
/// Mirrors `DirState.BISECT_PAGE_SIZE` (4096) in `bzrformats/dirstate.py`.
pub const BISECT_PAGE_SIZE: usize = 4096;

/// How many null-separated fields should be in each entry row.
///
/// Each line now has an extra `'\n'` field which is not used so we
/// just skip over it — so the per-entry count is 3 (for the key) + 5
/// (per tree_data) × tree_count + 1 (the newline field).
pub fn fields_per_entry(num_present_parents: usize) -> usize {
    let tree_count = 1 + num_present_parents;
    3 + 5 * tree_count + 1
}

/// Serialise the ghost-ids list to a single newline-free record.
pub fn get_ghosts_line(ghost_ids: &[&[u8]]) -> Vec<u8> {
    let mut entries = Vec::new();
    let l = format!("{}", ghost_ids.len());
    entries.push(l.as_bytes());
    entries.extend_from_slice(ghost_ids);
    entries.join(&b"\0"[..])
}

/// Serialise the parents list to a single newline-free record.
pub fn get_parents_line(parent_ids: &[&[u8]]) -> Vec<u8> {
    let mut entries = Vec::new();
    let l = format!("{}", parent_ids.len());
    entries.push(l.as_bytes());
    entries.extend_from_slice(parent_ids);
    entries.join(&b"\0"[..])
}

fn _crc32(bit: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(bit);
    hasher.finalize()
}

/// Format lines for final output.
///
/// Args:
///   lines: A sequence of lines containing the parents list and the path lines.
pub fn get_output_lines(mut lines: Vec<&[u8]>) -> Vec<Vec<u8>> {
    let mut output_lines = vec![HEADER_FORMAT_3];
    lines.push(b"");

    let inventory_text = lines.join(&b"\0\n\0"[..]).to_vec();

    let crc32 = _crc32(inventory_text.as_slice());
    let crc32_line = format!("crc32: {}\n", crc32).into_bytes();
    output_lines.push(crc32_line.as_slice());

    let num_entries = lines.len() - 3;
    let num_entries_line = format!("num_entries: {}\n", num_entries).into_bytes();
    output_lines.push(num_entries_line.as_slice());
    output_lines.push(inventory_text.as_slice());

    output_lines.into_iter().map(|l| l.to_vec()).collect()
}

/// Error returned while parsing the dirstate header.
#[derive(Debug, PartialEq, Eq)]
pub enum HeaderError {
    /// The first line is not `#bazaar dirstate flat format 3\n`.
    BadFormatLine(Vec<u8>),
    /// The crc32 line does not start with `crc32: `.
    MissingCrcLine(Vec<u8>),
    /// The crc32 value is not a valid decimal integer.
    BadCrc(Vec<u8>),
    /// The num_entries line does not start with `num_entries: `.
    MissingNumEntriesLine(Vec<u8>),
    /// The num_entries value is not a valid decimal integer.
    BadNumEntries(Vec<u8>),
    /// The parents line or ghosts line was missing or malformed.
    BadParentsLine,
    /// The ghosts line was missing or malformed.
    BadGhostsLine,
    /// The input ended before a complete header could be read.
    UnexpectedEof,
}

impl std::fmt::Display for HeaderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HeaderError::BadFormatLine(line) => write!(f, "invalid header line: {:?}", line),
            HeaderError::MissingCrcLine(line) => write!(f, "missing crc32 checksum: {:?}", line),
            HeaderError::BadCrc(bytes) => write!(f, "invalid crc32 value: {:?}", bytes),
            HeaderError::MissingNumEntriesLine(line) => {
                write!(f, "missing num_entries line: {:?}", line)
            }
            HeaderError::BadNumEntries(bytes) => {
                write!(f, "invalid num_entries value: {:?}", bytes)
            }
            HeaderError::BadParentsLine => write!(f, "malformed parents line"),
            HeaderError::BadGhostsLine => write!(f, "malformed ghosts line"),
            HeaderError::UnexpectedEof => write!(f, "unexpected end of header"),
        }
    }
}

impl std::error::Error for HeaderError {}

/// Parsed dirstate header fields.
#[derive(Debug, PartialEq, Eq)]
pub struct Header {
    /// The `crc32:` value from the header line.
    pub crc_expected: u32,
    /// The `num_entries:` value from the header line.
    pub num_entries: usize,
    /// Parent revision ids.
    pub parents: Vec<Vec<u8>>,
    /// Ghost parent revision ids.
    pub ghosts: Vec<Vec<u8>>,
    /// Byte offset in the input where the header ends and the
    /// per-entry dirblock data begins. Mirrors Python's
    /// `_end_of_header` (the position of `_state_file.tell()` right
    /// after `_read_header` returns).
    pub end_of_header: usize,
}

/// Read one `\n`-terminated line from `data` starting at `pos`. Returns the
/// line *including* the trailing newline (mirroring Python's
/// `file.readline()` semantics) and the new cursor position. If there is no
/// newline, returns the remainder as the final line — matching `readline`'s
/// behaviour on an unterminated final line.
fn read_line(data: &[u8], pos: usize) -> Option<(&[u8], usize)> {
    if pos >= data.len() {
        return None;
    }
    let remaining = &data[pos..];
    match remaining.iter().position(|&b| b == b'\n') {
        Some(end) => Some((&remaining[..=end], pos + end + 1)),
        None => Some((remaining, data.len())),
    }
}

/// Parse the dirstate header from `data`.
///
/// This is the pure-Rust counterpart of `DirState._read_header` plus
/// `_read_prelude` in `bzrformats/dirstate.py`. Given the full (or at least
/// header-containing) dirstate file contents it returns the parsed header
/// plus the byte offset where the per-entry block begins.
///
/// Only format 3 is accepted; earlier formats raise `BadFormatLine` just as
/// the Python code raises `BzrFormatsError`.
pub fn read_header(data: &[u8]) -> Result<Header, HeaderError> {
    let mut pos = 0;

    let (format_line, next) = read_line(data, pos).ok_or(HeaderError::UnexpectedEof)?;
    if format_line != HEADER_FORMAT_3 {
        return Err(HeaderError::BadFormatLine(format_line.to_vec()));
    }
    pos = next;

    let (crc_line, next) = read_line(data, pos).ok_or(HeaderError::UnexpectedEof)?;
    let crc_prefix: &[u8] = b"crc32: ";
    if !crc_line.starts_with(crc_prefix) {
        return Err(HeaderError::MissingCrcLine(crc_line.to_vec()));
    }
    let crc_body = crc_line[crc_prefix.len()..]
        .strip_suffix(b"\n")
        .unwrap_or(&crc_line[crc_prefix.len()..]);
    let crc_str =
        std::str::from_utf8(crc_body).map_err(|_| HeaderError::BadCrc(crc_body.to_vec()))?;
    let crc_expected: u32 = crc_str
        .parse()
        .map_err(|_| HeaderError::BadCrc(crc_body.to_vec()))?;
    pos = next;

    let (num_entries_line, next) = read_line(data, pos).ok_or(HeaderError::UnexpectedEof)?;
    let num_entries_prefix: &[u8] = b"num_entries: ";
    if !num_entries_line.starts_with(num_entries_prefix) {
        return Err(HeaderError::MissingNumEntriesLine(
            num_entries_line.to_vec(),
        ));
    }
    let num_entries_body = num_entries_line[num_entries_prefix.len()..]
        .strip_suffix(b"\n")
        .unwrap_or(&num_entries_line[num_entries_prefix.len()..]);
    let num_entries_str = std::str::from_utf8(num_entries_body)
        .map_err(|_| HeaderError::BadNumEntries(num_entries_body.to_vec()))?;
    let num_entries: usize = num_entries_str
        .parse()
        .map_err(|_| HeaderError::BadNumEntries(num_entries_body.to_vec()))?;
    pos = next;

    // Parents line: `COUNT\0p1\0p2\0...\0pN\n`. Matches Python's
    //     info = parent_line.split(b"\0"); int(info[0]); self._parents = info[1:-1]
    // (the `\n` lives inside the last split component, which gets discarded
    // by the `[1:-1]` slice).
    let (parents_line, next) = read_line(data, pos).ok_or(HeaderError::UnexpectedEof)?;
    let parents = parse_parents_field(parents_line).ok_or(HeaderError::BadParentsLine)?;
    pos = next;

    // Ghosts line: `\0COUNT\0g1\0...\0gN\n`. Matches Python's
    //     info = ghost_line.split(b"\0"); int(info[1]); self._ghosts = info[2:-1]
    // The leading NUL comes from the `\0\n\0` separator written between
    // lines by `get_output_lines`.
    let (ghosts_line, next) = read_line(data, pos).ok_or(HeaderError::UnexpectedEof)?;
    let ghosts = parse_ghosts_field(ghosts_line).ok_or(HeaderError::BadGhostsLine)?;
    pos = next;

    Ok(Header {
        crc_expected,
        num_entries,
        parents,
        ghosts,
        end_of_header: pos,
    })
}

fn parse_parents_field(line: &[u8]) -> Option<Vec<Vec<u8>>> {
    let parts: Vec<&[u8]> = line.split(|&b| b == 0).collect();
    if parts.len() < 2 {
        return None;
    }
    // info[0] must be a valid integer count (we validate but discard it,
    // mirroring the bare `int(info[0])` in Python).
    std::str::from_utf8(parts[0]).ok()?.parse::<usize>().ok()?;
    Some(
        parts[1..parts.len() - 1]
            .iter()
            .map(|s| s.to_vec())
            .collect(),
    )
}

fn parse_ghosts_field(line: &[u8]) -> Option<Vec<Vec<u8>>> {
    let parts: Vec<&[u8]> = line.split(|&b| b == 0).collect();
    if parts.len() < 3 {
        return None;
    }
    // Skip parts[0] (the empty leading segment) and validate parts[1] as
    // the integer count.
    std::str::from_utf8(parts[1]).ok()?.parse::<usize>().ok()?;
    Some(
        parts[2..parts.len() - 1]
            .iter()
            .map(|s| s.to_vec())
            .collect(),
    )
}
