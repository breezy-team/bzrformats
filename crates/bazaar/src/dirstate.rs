use crate::inventory::Entry as InventoryEntry;
use crate::FileId;
use base64::engine::Engine;
use osutils::sha::{sha_file, sha_file_by_name};
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::fs::Metadata;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

pub trait SHA1Provider: Send + Sync {
    fn sha1(&self, path: &Path) -> std::io::Result<String>;

    fn stat_and_sha1(&self, path: &Path) -> std::io::Result<(Metadata, String)>;
}

/// A SHA1Provider that reads directly from the filesystem."""
pub struct DefaultSHA1Provider;

impl DefaultSHA1Provider {
    pub fn new() -> DefaultSHA1Provider {
        DefaultSHA1Provider {}
    }
}

impl Default for DefaultSHA1Provider {
    fn default() -> Self {
        Self::new()
    }
}

impl SHA1Provider for DefaultSHA1Provider {
    /// Return the sha1 of a file given its absolute path.
    fn sha1(&self, path: &Path) -> std::io::Result<String> {
        sha_file_by_name(path)
    }

    /// Return the stat and sha1 of a file given its absolute path.
    fn stat_and_sha1(&self, path: &Path) -> std::io::Result<(Metadata, String)> {
        let mut f = File::open(path)?;
        let stat = f.metadata()?;
        let sha1 = sha_file(&mut f)?;
        Ok((stat, sha1))
    }
}

pub fn lt_by_dirs(path1: &Path, path2: &Path) -> bool {
    let path1_parts = path1.components();
    let path2_parts = path2.components();
    let mut path1_parts_iter = path1_parts;
    let mut path2_parts_iter = path2_parts;

    loop {
        match (path1_parts_iter.next(), path2_parts_iter.next()) {
            (None, None) => return false,
            (None, Some(_)) => return true,
            (Some(_), None) => return false,
            (Some(part1), Some(part2)) => match part1.cmp(&part2) {
                Ordering::Equal => continue,
                Ordering::Less => return true,
                Ordering::Greater => return false,
            },
        }
    }
}

pub fn lt_path_by_dirblock(path1: &Path, path2: &Path) -> bool {
    let key1 = (path1.parent(), path1.file_name());
    let key2 = (path2.parent(), path2.file_name());

    key1 < key2
}

pub fn bisect_path_left(paths: &[&Path], path: &Path) -> usize {
    let mut hi = paths.len();
    let mut lo = 0;
    while lo < hi {
        let mid = (lo + hi) / 2;
        // Grab the dirname for the current dirblock
        let cur = paths[mid];
        if lt_path_by_dirblock(cur, path) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

pub fn bisect_path_right(paths: &[&Path], path: &Path) -> usize {
    let mut hi = paths.len();
    let mut lo = 0;
    while lo < hi {
        let mid = (lo + hi) / 2;
        // Grab the dirname for the current dirblock
        let cur = paths[mid];
        if lt_path_by_dirblock(path, cur) {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    lo
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(s: &str) -> &Path {
        Path::new(s)
    }

    /// Python's assertCmpByDirs(expected, a, b) with expected in {-1, 0, 1}.
    fn assert_cmp(expected: i32, a: &str, b: &str) {
        let (pa, pb) = (p(a), p(b));
        match expected {
            0 => {
                assert_eq!(a, b);
                assert!(!lt_by_dirs(pa, pb));
                assert!(!lt_by_dirs(pb, pa));
            }
            v if v > 0 => {
                assert!(!lt_by_dirs(pa, pb));
                assert!(lt_by_dirs(pb, pa));
            }
            _ => {
                assert!(lt_by_dirs(pa, pb));
                assert!(!lt_by_dirs(pb, pa));
            }
        }
    }

    #[test]
    fn lt_by_dirs_cmp_empty() {
        assert_cmp(0, "", "");
        assert_cmp(1, "a", "");
        assert_cmp(1, "abcdef", "");
        assert_cmp(1, "test/ing/a/path/", "");
    }

    #[test]
    fn lt_by_dirs_cmp_same_str() {
        for s in ["a", "ab", "abc", "a/b", "a/b/c/d/e"] {
            assert_cmp(0, s, s);
        }
    }

    #[test]
    fn lt_by_dirs_simple_paths() {
        assert_cmp(-1, "a", "b");
        assert_cmp(-1, "aa", "ab");
        assert_cmp(-1, "ab", "bb");
        assert_cmp(-1, "a/a", "a/b");
        assert_cmp(-1, "a/b", "b/b");
        assert_cmp(-1, "a/a/a", "a/a/b");
    }

    #[test]
    fn lt_by_dirs_tricky_paths() {
        assert_cmp(1, "ab/cd/ef", "ab/cc/ef");
        assert_cmp(1, "ab/cd/ef", "ab/c/ef");
        assert_cmp(-1, "ab/cd/ef", "ab/cd-ef");
        assert_cmp(-1, "ab/cd", "ab/cd-");
        assert_cmp(-1, "ab/cd", "ab-cd");
    }

    #[test]
    fn lt_by_dirs_non_ascii() {
        // \u{b5} < \u{e5}
        assert_cmp(-1, "\u{b5}", "\u{e5}");
        assert_cmp(-1, "a", "\u{e5}");
        assert_cmp(-1, "b", "\u{b5}");
        assert_cmp(-1, "a/b", "a/\u{e5}");
        assert_cmp(-1, "b/a", "b/\u{b5}");
    }

    #[test]
    fn lt_path_by_dirblock_simple_sorted_list() {
        // Sorted by dirblock: all paths in a directory before subdirectories.
        let paths: Vec<&Path> = vec![p(""), p("a"), p("ab"), p("abc"), p("a/b/c"), p("b/d/e")];
        for (i, a) in paths.iter().enumerate() {
            for (j, b) in paths.iter().enumerate() {
                assert_eq!(
                    lt_path_by_dirblock(a, b),
                    i < j,
                    "lt_path_by_dirblock({:?}, {:?}) mismatched i={} j={}",
                    a,
                    b,
                    i,
                    j,
                );
            }
        }
    }

    #[test]
    fn bisect_path_left_simple_list() {
        let paths: Vec<&Path> = vec![p(""), p("a"), p("b"), p("c"), p("d")];
        for (i, path) in paths.iter().enumerate() {
            assert_eq!(bisect_path_left(&paths, path), i);
        }
        // Insertion positions for missing elements.
        assert_eq!(bisect_path_left(&paths, p("_")), 1);
        assert_eq!(bisect_path_left(&paths, p("aa")), 2);
        assert_eq!(bisect_path_left(&paths, p("bb")), 3);
        assert_eq!(bisect_path_left(&paths, p("dd")), 5);
    }

    #[test]
    fn bisect_path_right_after_equal_entry() {
        let paths: Vec<&Path> = vec![p(""), p("a"), p("b"), p("c"), p("d")];
        for (i, path) in paths.iter().enumerate() {
            // bisect_right on an existing entry returns the slot after it.
            assert_eq!(bisect_path_right(&paths, path), i + 1);
        }
    }
}

#[cfg(unix)]
pub fn pack_stat_metadata(metadata: &Metadata) -> String {
    pack_stat(
        metadata.len(),
        metadata
            .modified()
            .unwrap()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        metadata
            .created()
            .unwrap()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        metadata.dev(),
        metadata.ino(),
        metadata.mode(),
    )
}

#[cfg(windows)]
pub fn pack_stat_metadata(metadata: &Metadata) -> String {
    pack_stat(
        metadata.len(),
        metadata
            .modified()
            .unwrap()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        metadata
            .created()
            .unwrap()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        0,
        0,
        0,
    )
}

pub fn pack_stat(size: u64, mtime: u64, ctime: u64, dev: u64, ino: u64, mode: u32) -> String {
    let size = size & 0xFFFFFFFF;
    let mtime = mtime & 0xFFFFFFFF;
    let ctime = ctime & 0xFFFFFFFF;
    let dev = dev & 0xFFFFFFFF;
    let ino = ino & 0xFFFFFFFF;

    let packed_data = [
        (size >> 24) as u8,
        (size >> 16) as u8,
        (size >> 8) as u8,
        size as u8,
        (mtime >> 24) as u8,
        (mtime >> 16) as u8,
        (mtime >> 8) as u8,
        mtime as u8,
        (ctime >> 24) as u8,
        (ctime >> 16) as u8,
        (ctime >> 8) as u8,
        ctime as u8,
        (dev >> 24) as u8,
        (dev >> 16) as u8,
        (dev >> 8) as u8,
        dev as u8,
        (ino >> 24) as u8,
        (ino >> 16) as u8,
        (ino >> 8) as u8,
        ino as u8,
        (mode >> 24) as u8,
        (mode >> 16) as u8,
        (mode >> 8) as u8,
        mode as u8,
    ];

    base64::engine::general_purpose::STANDARD_NO_PAD.encode(packed_data)
}

pub fn stat_to_minikind(metadata: &Metadata) -> char {
    let file_type = metadata.file_type();
    if file_type.is_dir() {
        'd'
    } else if file_type.is_file() {
        'f'
    } else if file_type.is_symlink() {
        'l'
    } else {
        panic!("Unsupported file type");
    }
}

pub const HEADER_FORMAT_2: &[u8] = b"#bazaar dirstate flat format 2\n";
pub const HEADER_FORMAT_3: &[u8] = b"#bazaar dirstate flat format 3\n";

#[derive(PartialEq, Eq, Debug)]
pub enum Kind {
    Absent,
    File,
    Directory,
    Relocated,
    Symlink,
    TreeReference,
}

impl Kind {
    pub fn to_char(&self) -> char {
        match self {
            Kind::Absent => 'a',
            Kind::File => 'f',
            Kind::Directory => 'd',
            Kind::Relocated => 'r',
            Kind::Symlink => 'l',
            Kind::TreeReference => 't',
        }
    }

    pub fn to_byte(&self) -> u8 {
        self.to_char() as u8
    }

    pub fn to_str(&self) -> &str {
        match self {
            Kind::Absent => "absent",
            Kind::File => "file",
            Kind::Directory => "directory",
            Kind::Relocated => "relocated",
            Kind::Symlink => "symlink",
            Kind::TreeReference => "tree-reference",
        }
    }
}

impl From<osutils::Kind> for Kind {
    fn from(k: osutils::Kind) -> Self {
        match k {
            osutils::Kind::File => Kind::File,
            osutils::Kind::Directory => Kind::Directory,
            osutils::Kind::Symlink => Kind::Symlink,
            osutils::Kind::TreeReference => Kind::TreeReference,
        }
    }
}

impl ToString for Kind {
    fn to_string(&self) -> String {
        self.to_str().to_string()
    }
}

impl From<String> for Kind {
    fn from(s: String) -> Self {
        match s.as_str() {
            "absent" => Kind::Absent,
            "file" => Kind::File,
            "directory" => Kind::Directory,
            "relocated" => Kind::Relocated,
            "symlink" => Kind::Symlink,
            "tree-reference" => Kind::TreeReference,
            _ => panic!("Unknown kind: {}", s),
        }
    }
}

impl From<char> for Kind {
    fn from(c: char) -> Self {
        match c {
            'a' => Kind::Absent,
            'f' => Kind::File,
            'd' => Kind::Directory,
            'r' => Kind::Relocated,
            'l' => Kind::Symlink,
            't' => Kind::TreeReference,
            _ => panic!("Unknown kind: {}", c),
        }
    }
}

pub enum YesNo {
    Yes,
    No,
}

/// _header_state and _dirblock_state represent the current state
/// of the dirstate metadata and the per-row data respectiely.
/// In future we will add more granularity, for instance _dirblock_state
/// will probably support partially-in-memory as a separate variable,
/// allowing for partially-in-memory unmodified and partially-in-memory
/// modified states.
#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub enum MemoryState {
    /// indicates that no data is in memory
    NotInMemory,

    /// indicates that what we have in memory is the same as is on disk
    InMemoryUnmodified,

    /// indicates that we have a modified version of what is on disk.
    InMemoryModified,
    InMemoryHashModified,
}

pub fn fields_per_entry(num_present_parents: usize) -> usize {
    // How many null separated fields should be in each entry row.
    //
    // Each line now has an extra '\n' field which is not used
    // so we just skip over it
    //
    // entry size:
    //     3 fields for the key
    //     + number of fields per tree_data (5) * tree count
    //     + newline
    let tree_count = 1 + num_present_parents;
    3 + 5 * tree_count + 1
}

pub fn get_ghosts_line(ghost_ids: &[&[u8]]) -> Vec<u8> {
    // Create a line for the state file for ghost information.
    let mut entries = Vec::new();
    let l = format!("{}", ghost_ids.len());
    entries.push(l.as_bytes());
    entries.extend_from_slice(ghost_ids);
    entries.join(&b"\0"[..])
}

pub fn get_parents_line(parent_ids: &[&[u8]]) -> Vec<u8> {
    // Create a line for the state file for parents information.
    let mut entries = Vec::new();
    let l = format!("{}", parent_ids.len());
    entries.push(l.as_bytes());
    entries.extend_from_slice(parent_ids);
    entries.join(&b"\0"[..])
}

pub struct IdIndex {
    id_index: HashMap<FileId, Vec<(Vec<u8>, Vec<u8>, FileId)>>,
}

impl Default for IdIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl IdIndex {
    pub fn new() -> Self {
        IdIndex {
            id_index: HashMap::new(),
        }
    }

    pub fn add(&mut self, entry_key: (&[u8], &[u8], &FileId)) {
        // Add this entry to the _id_index mapping.
        //
        // This code used to use a set for every entry in the id_index. However,
        // it is *rare* to have more than one entry. So a set is a large
        // overkill. And even when we do, we won't ever have more than the
        // number of parent trees. Which is still a small number (rarely >2). As
        // such, we use a simple vector, and do our own uniqueness checks. While
        // the 'contains' check is O(N), since N is nicely bounded it shouldn't ever
        // cause quadratic failure.
        let file_id = entry_key.2;
        let entry_keys = self.id_index.entry(file_id.clone()).or_default();
        entry_keys.push((entry_key.0.to_vec(), entry_key.1.to_vec(), file_id.clone()));
    }

    pub fn remove(&mut self, entry_key: (&[u8], &[u8], &FileId)) {
        // Remove this entry from the _id_index mapping.
        //
        // It is a programming error to call this when the entry_key is not
        // already present.
        let file_id = entry_key.2;
        let entry_keys = self.id_index.get_mut(file_id).unwrap();
        entry_keys.retain(|key| (key.0.as_slice(), key.1.as_slice(), &key.2) != entry_key);
    }

    pub fn get(&self, file_id: &FileId) -> Vec<(Vec<u8>, Vec<u8>, FileId)> {
        self.id_index
            .get(file_id)
            .map_or_else(Vec::new, |v| v.clone())
    }

    pub fn iter_all(&self) -> impl Iterator<Item = &(Vec<u8>, Vec<u8>, FileId)> {
        self.id_index.values().flatten()
    }

    pub fn file_ids(&self) -> impl Iterator<Item = &FileId> {
        self.id_index.keys()
    }

    pub fn clear(&mut self) {
        self.id_index.clear();
    }
}

/// Convert an inventory entry (from a revision tree) to state details.
///
/// Args:
///   inv_entry: An inventory entry whose sha1 and link targets can be
///     relied upon, and which has a revision set.
/// Returns: A details tuple - the details for a single tree at a path id.
pub fn inv_entry_to_details(e: &InventoryEntry) -> (u8, Vec<u8>, u64, bool, Vec<u8>) {
    let minikind = Kind::from(e.kind()).to_byte();
    let tree_data = e
        .revision()
        .map_or_else(Vec::new, |r| r.as_bytes().to_vec());
    let (fingerprint, size, executable) = match e {
        InventoryEntry::Directory { .. } | InventoryEntry::Root { .. } => (Vec::new(), 0, false),
        InventoryEntry::File {
            text_sha1,
            text_size,
            executable,
            ..
        } => (
            text_sha1.as_ref().map_or_else(Vec::new, |f| f.to_vec()),
            text_size.unwrap_or(0),
            *executable,
        ),
        InventoryEntry::Link { symlink_target, .. } => (
            symlink_target
                .as_ref()
                .map_or_else(Vec::new, |f| f.as_bytes().to_vec()),
            0,
            false,
        ),
        InventoryEntry::TreeReference {
            reference_revision, ..
        } => (
            reference_revision
                .as_ref()
                .map_or_else(Vec::new, |f| f.as_bytes().to_vec()),
            0,
            false,
        ),
    };

    (minikind, fingerprint, size, executable, tree_data)
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
    // Format lines for final output.
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

/// Default bisect page size used when scanning the dirstate file on disk.
/// Mirrors `DirState.BISECT_PAGE_SIZE` (4096) in `bzrformats/dirstate.py`.
pub const BISECT_PAGE_SIZE: usize = 4096;

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
            HeaderError::BadFormatLine(line) => {
                write!(f, "invalid header line: {:?}", line)
            }
            HeaderError::MissingCrcLine(line) => {
                write!(f, "missing crc32 checksum: {:?}", line)
            }
            HeaderError::BadCrc(bytes) => {
                write!(f, "invalid crc32 value: {:?}", bytes)
            }
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
    // Strip the trailing newline (if any) before parsing.
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

/// Per-tree record attached to an entry: `(minikind, fingerprint, size, executable, packed_stat)`.
///
/// Mirrors the 5-tuple stored at `entry[1][tree_index]` in the Python
/// `DirState`. `minikind` is a single byte such as `b'f'`, `b'd'`, `b'l'`,
/// `b'a'`, `b'r'`, or `b't'`; `fingerprint` is the sha1 for files, the link
/// target for symlinks, or the parent revision for tree references; `size` is
/// the file size in bytes (0 for non-files); `packed_stat` is the base64
/// `pack_stat` string, or `DirState.NULLSTAT` when no stat is cached.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TreeData {
    pub minikind: u8,
    pub fingerprint: Vec<u8>,
    pub size: u64,
    pub executable: bool,
    pub packed_stat: Vec<u8>,
}

/// The `(dirname, basename, file_id)` triple that keys a dirstate entry.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct EntryKey {
    pub dirname: Vec<u8>,
    pub basename: Vec<u8>,
    pub file_id: Vec<u8>,
}

/// A single dirstate entry: a key plus one `TreeData` per tracked tree
/// (current tree followed by present parent trees).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    pub key: EntryKey,
    pub trees: Vec<TreeData>,
}

/// A directory block: all entries whose `dirname` equals `dirname`, in sort
/// order. Mirrors the `(dirname, [entry, ...])` tuple Python stores in
/// `DirState._dirblocks`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Dirblock {
    pub dirname: Vec<u8>,
    pub entries: Vec<Entry>,
}

/// Whether a dirstate is currently locked for read or write, matching the
/// `_lock_state` string Python stores (`"r"`, `"w"`, or `None`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LockState {
    Read,
    Write,
}

/// Filesystem snapshot for one path, as handed to
/// [`DirState::process_entry`].  Mirrors the 5-tuple Python's
/// `ProcessEntryPython` threads around internally:
/// `(top_relpath, basename, kind, stat, abspath)`.
#[derive(Debug, Clone)]
pub struct ProcessPathInfo {
    /// Absolute path of the file on disk (utf8 bytes).
    pub abspath: Vec<u8>,
    /// Filesystem kind ("file", "directory", "symlink",
    /// "tree-reference"), or `None` when the path is missing.
    pub kind: Option<String>,
    /// Stat info for the path.
    pub stat: StatInfo,
}

/// Mutable per-`iter_changes` state shared across
/// [`DirState::process_entry`] calls.  Ports the instance fields
/// Python's `ProcessEntryPython` carries: search / searched sets,
/// parent-id caches, dirname-to-file-id maps.
#[derive(Debug, Default)]
pub struct ProcessEntryState {
    /// `source_index` in the tree-data array; `None` means "compare
    /// against a synthetic empty source" (new-tree mode).
    pub source_index: Option<usize>,
    /// `target_index` in the tree-data array; always concrete.
    pub target_index: usize,
    /// Whether unchanged entries should still yield a change tuple.
    pub include_unchanged: bool,
    /// Paths whose children have already been walked.
    pub searched_specific_files: std::collections::HashSet<Vec<u8>>,
    /// Paths whose children still need walking (driven by the
    /// outer `iter_changes` loop).
    pub search_specific_files: std::collections::HashSet<Vec<u8>>,
    /// Cache: dirname → file_id for the *target* tree.
    pub new_dirname_to_file_id: std::collections::HashMap<Vec<u8>, Vec<u8>>,
    /// Cache: dirname → file_id for the *source* tree.
    pub old_dirname_to_file_id: std::collections::HashMap<Vec<u8>, Vec<u8>>,
    /// One-slot cache: (dirname, parent_file_id) for the source tree.
    pub last_source_parent: Option<(Vec<u8>, Option<Vec<u8>>)>,
    /// One-slot cache: (dirname, parent_file_id) for the target tree.
    pub last_target_parent: Option<(Vec<u8>, Option<Vec<u8>>)>,
}

/// One row returned by [`DirState::process_entry`], mirroring Python's
/// `DirstateInventoryChange` minus the utf8-decoding (Rust returns
/// raw bytes; the pyo3 layer decodes with surrogateescape).
#[derive(Debug, Clone)]
pub struct DirstateChange {
    pub file_id: Vec<u8>,
    pub old_path: Option<Vec<u8>>,
    pub new_path: Option<Vec<u8>>,
    pub content_change: bool,
    pub old_versioned: bool,
    pub new_versioned: bool,
    pub source_parent_id: Option<Vec<u8>>,
    pub target_parent_id: Option<Vec<u8>>,
    pub old_basename: Option<Vec<u8>>,
    pub new_basename: Option<Vec<u8>>,
    pub source_kind: Option<String>,
    pub target_kind: Option<String>,
    pub source_exec: Option<bool>,
    pub target_exec: Option<bool>,
}

/// Error returned by [`DirState::process_entry`].
#[derive(Debug)]
pub enum ProcessEntryError {
    DirstateCorrupt(String),
    BadFileKind { path: Vec<u8>, kind: String },
    Internal(String),
}

impl std::fmt::Display for ProcessEntryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProcessEntryError::DirstateCorrupt(s) => write!(f, "dirstate corrupt: {}", s),
            ProcessEntryError::BadFileKind { path, kind } => {
                write!(f, "bad file kind {:?} for path {:?}", kind, path)
            }
            ProcessEntryError::Internal(s) => write!(f, "process_entry: {}", s),
        }
    }
}

impl std::error::Error for ProcessEntryError {}

fn null_parent_details() -> TreeData {
    TreeData {
        minikind: b'a',
        fingerprint: Vec::new(),
        size: 0,
        executable: false,
        packed_stat: Vec::new(),
    }
}

fn join_path(dirname: &[u8], basename: &[u8]) -> Vec<u8> {
    if dirname.is_empty() {
        basename.to_vec()
    } else {
        let mut p = dirname.to_vec();
        p.push(b'/');
        p.extend_from_slice(basename);
        p
    }
}

/// Is `candidate` inside `parent` (or equal to it)?  Mirrors
/// `osutils.is_inside`: `parent` is the prefix directory, `candidate`
/// is the potentially-nested path.
fn is_inside(parent: &[u8], candidate: &[u8]) -> bool {
    if parent == candidate {
        return true;
    }
    if parent.is_empty() {
        return true;
    }
    candidate.len() > parent.len()
        && candidate.starts_with(parent)
        && candidate[parent.len()] == b'/'
}

fn kind_for_minikind(mk: u8) -> Option<String> {
    match mk {
        b'f' => Some("file".to_string()),
        b'd' => Some("directory".to_string()),
        b'l' => Some("symlink".to_string()),
        b't' => Some("tree-reference".to_string()),
        _ => None,
    }
}

#[allow(clippy::too_many_arguments)]
fn resolve_parent_id(
    dirblocks: &[Dirblock],
    old_dirname: &[u8],
    old_basename: &[u8],
    entry_file_id: &[u8],
    source_index: usize,
    old_dirname_to_file_id: &std::collections::HashMap<Vec<u8>, Vec<u8>>,
    last_source_parent: &mut Option<(Vec<u8>, Option<Vec<u8>>)>,
) -> Option<Vec<u8>> {
    if !old_basename.is_empty()
        && last_source_parent
            .as_ref()
            .map(|(d, _)| d.as_slice() == old_dirname)
            .unwrap_or(false)
    {
        return last_source_parent.as_ref().and_then(|(_, id)| id.clone());
    }
    let cached = old_dirname_to_file_id.get(old_dirname).cloned();
    let pid_raw = match cached {
        Some(v) => Some(v),
        None => {
            let bei = get_block_entry_index(dirblocks, &[], old_dirname, source_index);
            if bei.path_present {
                Some(
                    dirblocks[bei.block_index].entries[bei.entry_index]
                        .key
                        .file_id
                        .clone(),
                )
            } else {
                None
            }
        }
    };
    let pid = match pid_raw {
        Some(v) if v == entry_file_id => None,
        Some(v) => Some(v),
        None => None,
    };
    *last_source_parent = Some((old_dirname.to_vec(), pid.clone()));
    pid
}

#[allow(clippy::too_many_arguments)]
fn resolve_target_parent_id(
    dirblocks: &[Dirblock],
    new_dirname: &[u8],
    new_basename: &[u8],
    entry_file_id: &[u8],
    target_index: usize,
    new_dirname_to_file_id: &std::collections::HashMap<Vec<u8>, Vec<u8>>,
    last_target_parent: &mut Option<(Vec<u8>, Option<Vec<u8>>)>,
) -> Result<Option<Vec<u8>>, ProcessEntryError> {
    if !new_basename.is_empty()
        && last_target_parent
            .as_ref()
            .map(|(d, _)| d.as_slice() == new_dirname)
            .unwrap_or(false)
    {
        return Ok(last_target_parent.as_ref().and_then(|(_, id)| id.clone()));
    }
    let cached = new_dirname_to_file_id.get(new_dirname).cloned();
    let pid_raw = match cached {
        Some(v) => Some(v),
        None => {
            let bei = get_block_entry_index(dirblocks, &[], new_dirname, target_index);
            if bei.path_present {
                Some(
                    dirblocks[bei.block_index].entries[bei.entry_index]
                        .key
                        .file_id
                        .clone(),
                )
            } else {
                return Err(ProcessEntryError::Internal(format!(
                    "Could not find target parent in wt: {:?}",
                    new_dirname
                )));
            }
        }
    };
    let pid = match pid_raw {
        Some(v) if v == entry_file_id => None,
        Some(v) => Some(v),
        None => None,
    };
    *last_target_parent = Some((new_dirname.to_vec(), pid.clone()));
    Ok(pid)
}

/// Errors returned by [`Transport`] operations.
///
/// Variants are coarse on purpose: callers generally either propagate
/// the error or match on `NotFound` / `LockContention`. I/O errors are
/// normalised into `(ErrorKind, String)` so the enum stays
/// `Clone + PartialEq + Eq` and tests can compare values directly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportError {
    /// The backing file does not exist. Returned by `read_all` /
    /// `exists` / lock acquisition when there is nothing to open.
    NotFound(String),
    /// A lock was requested but another process already holds it, or
    /// the transport is already locked in an incompatible mode.
    LockContention(String),
    /// The caller tried to operate on an unlocked transport (read,
    /// write, or unlock without a prior `lock_read` / `lock_write`).
    NotLocked,
    /// The caller tried to acquire a second lock while one was still
    /// held. Dirstate's model is that you unlock before relocking;
    /// explicit rather than RAII.
    AlreadyLocked,
    /// Catch-all for I/O errors from the underlying store. The
    /// `(ErrorKind, message)` pair is preserved so callers can branch
    /// on kind without losing the original diagnostic.
    Io {
        kind: std::io::ErrorKind,
        message: String,
    },
    /// Catch-all for backend-specific failures that don't map to any
    /// of the above (typically wrapped Python exceptions on the pyo3
    /// adapter side).
    Other(String),
}

impl From<std::io::Error> for TransportError {
    fn from(e: std::io::Error) -> Self {
        if e.kind() == std::io::ErrorKind::NotFound {
            TransportError::NotFound(e.to_string())
        } else {
            TransportError::Io {
                kind: e.kind(),
                message: e.to_string(),
            }
        }
    }
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransportError::NotFound(p) => write!(f, "No such file: {}", p),
            TransportError::LockContention(p) => write!(f, "Lock contention: {}", p),
            TransportError::NotLocked => write!(f, "Transport is not locked"),
            TransportError::AlreadyLocked => write!(f, "Transport is already locked"),
            TransportError::Io { kind, message } => {
                write!(f, "I/O error ({:?}): {}", kind, message)
            }
            TransportError::Other(s) => write!(f, "Transport error: {}", s),
        }
    }
}

impl std::error::Error for TransportError {}

/// Single-file backing store for a [`DirState`].
///
/// Unlike `bazaar::transport::Transport` (the knit-side path-keyed byte
/// store), a dirstate transport represents exactly one file held open
/// across a lock. Operations:
///
/// * [`Transport::exists`] — whether the backing file exists. Used by
///   `on_file` to decide whether to create a fresh dirstate.
/// * [`Transport::lock_read`] / [`Transport::lock_write`] — acquire
///   a lock on the backing file. Explicit rather than RAII; the
///   caller must pair each lock with an `unlock`. Re-locking while
///   already locked returns `AlreadyLocked`.
/// * [`Transport::unlock`] — release the current lock. No-op on the
///   lock side if the underlying store doesn't need lock objects, but
///   the trait still expects the state transition.
/// * [`Transport::lock_state`] — observe the current lock state.
/// * [`Transport::read_all`] — return the full file contents. Requires
///   a read or write lock. The returned bytes are owned; callers parse
///   in memory (no streaming `readline` — the pure-Rust `read_header`
///   operates on a byte slice).
/// * [`Transport::write_all`] — replace the full file contents,
///   truncating any trailing bytes from the previous version. Requires
///   a write lock. Implementations are expected to flush before
///   returning, but are not required to fdatasync — call
///   [`Transport::fdatasync`] for that.
/// * [`Transport::fdatasync`] — force the current contents to durable
///   storage. Optional no-op for stores where fsync has no meaning
///   (e.g. in-memory tests); the trait method exists so `DirState.save`
///   can call it unconditionally.
///
/// The `&mut self` receivers are deliberate: every operation either
/// mutates the lock state, the file contents, or both. Callers that
/// need shared access should wrap an implementation in their own
/// synchronisation primitive.
/// Stat result returned by [`Transport::lstat`].  Mirrors the subset of
/// `os.stat_result` fields that dirstate logic actually inspects:
/// mode (for kind + executable), size, mtime/ctime (for the cutoff
/// check), dev/ino (fed into `pack_stat`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatInfo {
    pub mode: u32,
    pub size: u64,
    pub mtime: i64,
    pub ctime: i64,
    pub dev: u64,
    pub ino: u64,
}

impl StatInfo {
    /// Whether `mode` indicates a regular file (S_IFREG).
    pub fn is_file(&self) -> bool {
        self.mode & 0o170000 == 0o100000
    }
    /// Whether `mode` indicates a directory (S_IFDIR).
    pub fn is_dir(&self) -> bool {
        self.mode & 0o170000 == 0o040000
    }
    /// Whether `mode` indicates a symlink (S_IFLNK).
    pub fn is_symlink(&self) -> bool {
        self.mode & 0o170000 == 0o120000
    }
}

pub trait Transport {
    /// Whether the backing file exists. Does not require a lock.
    fn exists(&self) -> Result<bool, TransportError>;

    /// Acquire a read lock on the backing file. Returns
    /// `AlreadyLocked` if any lock is already held.
    fn lock_read(&mut self) -> Result<(), TransportError>;

    /// Acquire a write lock on the backing file. Returns
    /// `AlreadyLocked` if any lock is already held.
    fn lock_write(&mut self) -> Result<(), TransportError>;

    /// Release the current lock. Returns `NotLocked` if no lock was
    /// held.
    fn unlock(&mut self) -> Result<(), TransportError>;

    /// Current lock state, or `None` if no lock is held.
    fn lock_state(&self) -> Option<LockState>;

    /// Read the full contents of the backing file. Requires a read
    /// or write lock; returns `NotLocked` otherwise.
    fn read_all(&mut self) -> Result<Vec<u8>, TransportError>;

    /// Replace the full contents of the backing file, truncating any
    /// trailing bytes from the previous version. Requires a write
    /// lock; returns `NotLocked` if no lock is held, and a generic
    /// error if only a read lock is held.
    fn write_all(&mut self, bytes: &[u8]) -> Result<(), TransportError>;

    /// Force the current contents to durable storage. Implementations
    /// that have no meaningful fsync (in-memory tests, mocked
    /// backends) are free to make this a no-op; real filesystem
    /// implementations should call `fdatasync(2)` or the platform
    /// equivalent.
    fn fdatasync(&mut self) -> Result<(), TransportError>;

    /// Return the stat info for an absolute path in the working-tree
    /// filesystem that the dirstate is tracking (not the dirstate
    /// file itself).  `NoSuchFile` when the path is gone from disk.
    /// Required by `DirState::update_entry` / `process_entry`, which
    /// otherwise would couple the pure crate to `std::fs`.
    fn lstat(&self, abspath: &[u8]) -> Result<StatInfo, TransportError>;

    /// Return the target of the symlink at `abspath`.  `NoSuchFile`
    /// when the path is gone; a generic error when the path is not a
    /// symlink.
    fn read_link(&self, abspath: &[u8]) -> Result<Vec<u8>, TransportError>;
}

/// Error returned while parsing the on-disk dirblock body of a dirstate
/// file. Corresponds to the `DirstateCorrupt` errors raised by the Python
/// `_read_dirblocks` implementation.
#[derive(Debug, PartialEq, Eq)]
pub enum DirblocksError {
    /// A NUL-delimited field was requested past the end of the input.
    UnexpectedEof,
    /// A NUL-delimited field was read but no terminating NUL was found
    /// before the end of the input.
    MissingNul { trailing: Vec<u8> },
    /// The first post-header field was expected to be empty (the leading
    /// NUL from the `\0\n\0` line joiner) but contained data.
    LeadingFieldNotEmpty(Vec<u8>),
    /// A size field could not be parsed as a decimal integer.
    BadSize(Vec<u8>),
    /// The trailing `\n` after a row was missing or the wrong length.
    BadRowTerminator(Vec<u8>),
    /// The number of parsed entries did not match the count declared by the
    /// header.
    WrongEntryCount { expected: usize, actual: usize },
}

impl std::fmt::Display for DirblocksError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DirblocksError::UnexpectedEof => {
                write!(f, "get_next() called when there are no chars left")
            }
            DirblocksError::MissingNul { trailing } => {
                let end = std::cmp::min(trailing.len(), 20);
                write!(
                    f,
                    "failed to find trailing NULL (\\0). Trailing garbage: {:?}",
                    &trailing[..end]
                )
            }
            DirblocksError::LeadingFieldNotEmpty(field) => {
                write!(f, "First field should be empty, not: {:?}", field)
            }
            DirblocksError::BadSize(bytes) => {
                write!(f, "invalid size field: {:?}", bytes)
            }
            DirblocksError::BadRowTerminator(bytes) => {
                write!(
                    f,
                    "Bad parse, we expected to end on \\n, not: {} {:?}",
                    bytes.len(),
                    bytes
                )
            }
            DirblocksError::WrongEntryCount { expected, actual } => {
                write!(
                    f,
                    "We read the wrong number of entries. We expected to read {}, but read {}",
                    expected, actual
                )
            }
        }
    }
}

impl std::error::Error for DirblocksError {}

/// Read one NUL-terminated field from `data` starting at `pos`, returning
/// the field bytes and the new cursor position. Mirrors the inline
/// `get_next_field` helper from the pyo3 shim / Python implementation.
fn get_next_field(data: &[u8], pos: usize) -> Result<(&[u8], usize), DirblocksError> {
    if pos >= data.len() {
        return Err(DirblocksError::UnexpectedEof);
    }
    let remaining = &data[pos..];
    match remaining.iter().position(|&b| b == 0) {
        Some(offset) => Ok((&data[pos..pos + offset], pos + offset + 1)),
        None => Err(DirblocksError::MissingNul {
            trailing: remaining.to_vec(),
        }),
    }
}

/// Parse the on-disk dirblock body of a dirstate file into a flat list of
/// [`Dirblock`]s.
///
/// `text` is everything after `end_of_header`; `num_trees` is
/// `1 + num_present_parents`; `num_entries` is the value from the header
/// used only to validate that the parse saw the expected row count.
///
/// The returned sequence always begins with two sentinel blocks both
/// carrying an empty `dirname`: the first holds all root entries seen
/// during the parse, and the second is an empty placeholder. This matches
/// Python's `_read_dirblocks`, which relies on a follow-up
/// `_split_root_dirblock_into_contents` call (a separate commit) to
/// reshape those two blocks.
pub fn parse_dirblocks(
    text: &[u8],
    num_trees: usize,
    num_entries: usize,
) -> Result<Vec<Dirblock>, DirblocksError> {
    // Empty body: nothing to parse. The caller is expected to install the
    // usual pair of empty sentinel blocks itself if appropriate.
    if text.is_empty() {
        return Ok(Vec::new());
    }

    // The first NUL-delimited field is expected to be empty: it's the
    // leading NUL of the `\0\n\0` separator written between the ghosts
    // line and the first entry row.
    let (first_field, mut pos) = get_next_field(text, 0)?;
    if !first_field.is_empty() {
        return Err(DirblocksError::LeadingFieldNotEmpty(first_field.to_vec()));
    }

    // Seed with two sentinel empty-dirname blocks, matching Python's
    // `_read_dirblocks` initialisation.
    let mut dirblocks: Vec<Dirblock> = vec![
        Dirblock {
            dirname: Vec::new(),
            entries: Vec::new(),
        },
        Dirblock {
            dirname: Vec::new(),
            entries: Vec::new(),
        },
    ];

    let mut current_dirname: Vec<u8> = Vec::new();
    // Index of the "current" block within `dirblocks`; starts at the first
    // sentinel, which collects all root-level entries until
    // `_split_root_dirblock_into_contents` reshapes them later.
    let mut current_block_idx: usize = 0;
    let mut entry_count: usize = 0;

    while pos < text.len() {
        let (dirname_bytes, new_pos) = get_next_field(text, pos)?;
        pos = new_pos;

        if dirname_bytes != current_dirname.as_slice() {
            current_dirname = dirname_bytes.to_vec();
            dirblocks.push(Dirblock {
                dirname: current_dirname.clone(),
                entries: Vec::new(),
            });
            current_block_idx = dirblocks.len() - 1;
        }

        let (name_bytes, new_pos) = get_next_field(text, pos)?;
        pos = new_pos;
        let (file_id_bytes, new_pos) = get_next_field(text, pos)?;
        pos = new_pos;

        let key = EntryKey {
            dirname: current_dirname.clone(),
            basename: name_bytes.to_vec(),
            file_id: file_id_bytes.to_vec(),
        };

        let mut trees: Vec<TreeData> = Vec::with_capacity(num_trees);
        for _ in 0..num_trees {
            let (minikind_bytes, new_pos) = get_next_field(text, pos)?;
            pos = new_pos;
            let (fingerprint_bytes, new_pos) = get_next_field(text, pos)?;
            pos = new_pos;
            let (size_bytes, new_pos) = get_next_field(text, pos)?;
            pos = new_pos;
            let (exec_bytes, new_pos) = get_next_field(text, pos)?;
            pos = new_pos;
            let (info_bytes, new_pos) = get_next_field(text, pos)?;
            pos = new_pos;

            let size_str = std::str::from_utf8(size_bytes)
                .map_err(|_| DirblocksError::BadSize(size_bytes.to_vec()))?;
            let size: u64 = size_str
                .parse()
                .map_err(|_| DirblocksError::BadSize(size_bytes.to_vec()))?;

            // Matches Python `exec_bytes[0] == b'y'` with defensive
            // handling of the empty-field case (mirrors the pyo3 shim).
            let executable = !exec_bytes.is_empty() && exec_bytes[0] == b'y';

            trees.push(TreeData {
                // Python stores the minikind as a 1-byte `bytes` object
                // but otherwise treats it opaquely; we store only the
                // first byte and preserve the rest as part of the raw
                // field should future code need it.
                minikind: minikind_bytes.first().copied().unwrap_or(0),
                fingerprint: fingerprint_bytes.to_vec(),
                size,
                executable,
                packed_stat: info_bytes.to_vec(),
            });
        }

        // Each row ends with a trailing `\n` stored as its own NUL-delimited
        // field, i.e. the raw bytes `"\n\0"`.
        let (trailing, new_pos) = get_next_field(text, pos)?;
        pos = new_pos;
        if trailing.len() != 1 || trailing[0] != b'\n' {
            return Err(DirblocksError::BadRowTerminator(trailing.to_vec()));
        }

        dirblocks[current_block_idx]
            .entries
            .push(Entry { key, trees });
        entry_count += 1;
    }

    if entry_count != num_entries {
        return Err(DirblocksError::WrongEntryCount {
            expected: num_entries,
            actual: entry_count,
        });
    }

    Ok(dirblocks)
}

/// Serialise a single [`Entry`] to the NUL-delimited byte form Python
/// writes via `DirState._entry_to_line`.
///
/// The output is `dirname\0basename\0file_id\0` followed by, for each
/// tree, `minikind\0fingerprint\0size\0{y,n}\0packed_stat`. No trailing
/// NUL — the outer `get_output_lines` step adds the `\0\n\0` separator
/// between rows when it joins them into the full inventory text.
pub fn entry_to_line(entry: &Entry) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&entry.key.dirname);
    out.push(0);
    out.extend_from_slice(&entry.key.basename);
    out.push(0);
    out.extend_from_slice(&entry.key.file_id);
    for tree in &entry.trees {
        out.push(0);
        out.push(tree.minikind);
        out.push(0);
        out.extend_from_slice(&tree.fingerprint);
        out.push(0);
        out.extend_from_slice(format!("{}", tree.size).as_bytes());
        out.push(0);
        out.push(if tree.executable { b'y' } else { b'n' });
        out.push(0);
        out.extend_from_slice(&tree.packed_stat);
    }
    out
}

/// Flatten every entry in `dirblocks` into an iterator-style Vec of rows.
/// Each row is produced by [`entry_to_line`]; the returned vector is
/// ready to be chained with the parents/ghosts lines and handed to
/// [`get_output_lines`].
///
/// Mirrors Python's `_iter_entries` + `map(_entry_to_line, ...)` chain
/// inside `DirState.get_lines`.
pub fn dirblocks_to_entry_lines(dirblocks: &[Dirblock]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    for block in dirblocks {
        for entry in &block.entries {
            out.push(entry_to_line(entry));
        }
    }
    out
}

/// In-memory `DirState`, the Rust counterpart to `bzrformats.dirstate.DirState`.
///
/// This commit introduces the struct and a constructor mirroring Python's
/// `__init__`. Behaviour (reading, writing, entry lookup, change processing)
/// is added in follow-up commits; for now the struct is a passive container
/// so later ports have a stable place to hang methods.
pub struct DirState {
    /// Path to the dirstate file on disk (Python's `_filename`).
    pub filename: PathBuf,
    /// Provider used to compute sha1s and stat+sha1 tuples for working-tree
    /// files. Boxed so callers can swap in an alternate implementation for
    /// testing, matching Python's `_sha1_provider` attribute.
    pub sha1_provider: Box<dyn SHA1Provider + Send + Sync>,
    /// State of the header (`NotInMemory` until `_read_header` runs).
    pub header_state: MemoryState,
    /// State of the per-row dirblock data.
    pub dirblock_state: MemoryState,
    /// If an error was detected while updating the dirstate we refuse to
    /// write it back. Mirrors Python's `_changes_aborted` flag.
    pub changes_aborted: bool,
    /// The in-memory dirblocks, sorted by dirname. Python stores this as
    /// `[(dirname, [entry, ...])]` in `_dirblocks`.
    pub dirblocks: Vec<Dirblock>,
    /// Ghost parent revision ids: parents that are referenced but not
    /// present locally.
    pub ghosts: Vec<Vec<u8>>,
    /// Parent revision ids for the current tree, in order. The first entry
    /// is the current parent; subsequent entries are merged parents.
    pub parents: Vec<Vec<u8>>,
    /// Offset in `filename` where the header ends and the dirblock text
    /// begins, populated after the header has been parsed.
    pub end_of_header: Option<u64>,
    /// Cutoff mtime/ctime for trusting cached sha1s. `None` until
    /// `_sha_cutoff_time` has been computed for the current `now`.
    pub cutoff_time: Option<i64>,
    /// Declared entry count from the header, or `None` before the header is
    /// read. Used to validate the dirblock parse.
    pub num_entries: usize,
    /// Current read/write lock state.
    pub lock_state: Option<LockState>,
    /// Set of keys whose hash is known to have changed since load. Used by
    /// `_mark_modified` to decide whether a save is worthwhile.
    pub known_hash_changes: HashSet<EntryKey>,
    /// Below this many hash-only changes a save is skipped.
    /// `-1` means *never* save hash changes; `0` means always save them.
    pub worth_saving_limit: i64,
    /// Call `fdatasync` after writing the state file if true.
    pub fdatasync: bool,
    /// Trust the filesystem's executable bit when building tree data.
    pub use_filesystem_for_exec: bool,
    /// Bisect chunk size when reading the state file in pages; mirrors
    /// `_bisect_page_size`.
    pub bisect_page_size: usize,
    /// Lazily-populated index of `file_id → [(dirname, basename, file_id)]`.
    /// `None` until [`DirState::get_or_build_id_index`] is called, at
    /// which point it is rebuilt from the current `dirblocks`.
    /// Invalidate by setting to `None` whenever dirblocks change.
    pub id_index: Option<IdIndex>,
    /// Lazily-populated index of `packed_stat → sha1` for every file
    /// entry in tree 0. `None` until [`DirState::get_or_build_packed_stat_index`]
    /// is called, mirroring Python's `_packed_stat_index` attribute.
    /// Invalidate by setting to `None` whenever tree-0 entries change.
    pub packed_stat_index: Option<HashMap<Vec<u8>, Vec<u8>>>,
}

impl DirState {
    /// Create a new, empty `DirState` object.
    ///
    /// The returned state has no data loaded from disk — `header_state` and
    /// `dirblock_state` are both `NotInMemory`. Call a future `load` method
    /// to populate it. This mirrors the Python constructor at
    /// `bzrformats/dirstate.py` `DirState.__init__`.
    pub fn new<P: Into<PathBuf>>(
        path: P,
        sha1_provider: Box<dyn SHA1Provider + Send + Sync>,
        worth_saving_limit: i64,
        use_filesystem_for_exec: bool,
        fdatasync: bool,
    ) -> Self {
        DirState {
            filename: path.into(),
            sha1_provider,
            header_state: MemoryState::NotInMemory,
            dirblock_state: MemoryState::NotInMemory,
            changes_aborted: false,
            dirblocks: Vec::new(),
            ghosts: Vec::new(),
            parents: Vec::new(),
            end_of_header: None,
            cutoff_time: None,
            num_entries: 0,
            lock_state: None,
            known_hash_changes: HashSet::new(),
            worth_saving_limit,
            fdatasync,
            use_filesystem_for_exec,
            bisect_page_size: BISECT_PAGE_SIZE,
            id_index: None,
            packed_stat_index: None,
        }
    }

    /// Yield a reference to every entry across every dirblock, in
    /// dirblock order. Mirrors Python's `_iter_entries` in the simple
    /// case (without the implicit `_read_dirblocks_if_needed` —
    /// callers are expected to have populated `dirblocks` already).
    pub fn iter_entries(&self) -> impl Iterator<Item = &Entry> {
        self.dirblocks.iter().flat_map(|b| b.entries.iter())
    }

    /// Build an [`IdIndex`] from the current dirblocks. Pure — no
    /// cache interaction; callers that want Python's cached behaviour
    /// should use [`DirState::get_or_build_id_index`] instead.
    pub fn build_id_index(&self) -> IdIndex {
        let mut idx = IdIndex::new();
        for entry in self.iter_entries() {
            let file_id = FileId::from(&entry.key.file_id);
            idx.add((
                entry.key.dirname.as_slice(),
                entry.key.basename.as_slice(),
                &file_id,
            ));
        }
        idx
    }

    /// Return a reference to the cached [`IdIndex`], rebuilding it
    /// from `self.dirblocks` on first call after the cache was last
    /// invalidated. Mirrors Python's `DirState._get_id_index`.
    ///
    /// The cache lives in `self.id_index`; any code that mutates
    /// `self.dirblocks` must set `self.id_index = None` afterwards to
    /// force a rebuild on the next access.
    pub fn get_or_build_id_index(&mut self) -> &IdIndex {
        if self.id_index.is_none() {
            self.id_index = Some(self.build_id_index());
        }
        self.id_index.as_ref().unwrap()
    }

    /// Rebuild the `packed_stat → sha1` map from every tree-0 file
    /// entry. Pure — no cache interaction.
    pub fn build_packed_stat_index(&self) -> HashMap<Vec<u8>, Vec<u8>> {
        let mut index: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        for entry in self.iter_entries() {
            let tree0 = match entry.trees.first() {
                Some(t) => t,
                None => continue,
            };
            if tree0.minikind == b'f' {
                // Python stores the mapping keyed by the packed_stat
                // and with the fingerprint (the sha1) as the value.
                index.insert(tree0.packed_stat.clone(), tree0.fingerprint.clone());
            }
        }
        index
    }

    /// Return a reference to the cached `packed_stat → sha1` map,
    /// rebuilding it on first call after the cache was last
    /// invalidated. Mirrors Python's `DirState._get_packed_stat_index`.
    ///
    /// The cache lives in `self.packed_stat_index`; any code that
    /// mutates tree-0 file entries must set `self.packed_stat_index =
    /// None` afterwards to force a rebuild on the next access.
    pub fn get_or_build_packed_stat_index(&mut self) -> &HashMap<Vec<u8>, Vec<u8>> {
        if self.packed_stat_index.is_none() {
            self.packed_stat_index = Some(self.build_packed_stat_index());
        }
        self.packed_stat_index.as_ref().unwrap()
    }

    /// Parse the header of the dirstate file from `data` and populate the
    /// in-memory fields that Python's `_read_header` would populate.
    ///
    /// `data` must contain the full dirstate file contents (or at minimum
    /// enough bytes to cover the header); this mirrors Python's
    /// `state_file.readline()` loop operating on a buffered file. On
    /// success the `parents`, `ghosts`, `num_entries`, and `end_of_header`
    /// fields are set and `header_state` transitions to
    /// `InMemoryUnmodified`.
    pub fn read_header(&mut self, data: &[u8]) -> Result<(), HeaderError> {
        let header = read_header(data)?;
        self.parents = header.parents;
        self.ghosts = header.ghosts;
        self.num_entries = header.num_entries;
        self.end_of_header = Some(header.end_of_header as u64);
        self.header_state = MemoryState::InMemoryUnmodified;
        Ok(())
    }

    /// Split `self.dirblocks[0]` — which the parser fills with *both* root
    /// entries and contents-of-root entries — into the two sentinel
    /// blocks Python's `_read_dirblocks` / `_split_root_dirblock_into_contents`
    /// produces: block 0 holds entries whose basename is empty (the root
    /// itself and any parent-tree variants), and block 1 holds the rest.
    ///
    /// Returns an error if the layout does not match the expected
    /// post-parse shape (fewer than two blocks, or block 1 is not the
    /// empty sentinel).
    pub fn split_root_dirblock_into_contents(&mut self) -> Result<(), SplitRootError> {
        split_root_dirblock_into_contents(&mut self.dirblocks)
    }

    /// Locate the block for a given key. Mirrors
    /// `DirState._find_block_index_from_key`, without the
    /// `_last_block_index` / `_split_path_cache` memoisation layers
    /// (those live on the Python object and are a follow-up port).
    pub fn find_block_index_from_key(&self, key: &EntryKey) -> (usize, bool) {
        find_block_index_from_key(&self.dirblocks, key)
    }

    /// Locate the entry index for a key within a block. Mirrors
    /// `DirState._find_entry_index`, in the simpler uncached form.
    pub fn find_entry_index(&self, key: &EntryKey, block: &[Entry]) -> (usize, bool) {
        find_entry_index(key, block)
    }

    /// Look up a `(dirname, basename)` path in the given tree. Mirrors
    /// `DirState._get_block_entry_index`.
    pub fn get_block_entry_index(
        &self,
        dirname: &[u8],
        basename: &[u8],
        tree_index: usize,
    ) -> BlockEntryIndex {
        get_block_entry_index(&self.dirblocks, dirname, basename, tree_index)
    }

    /// Serialise the in-memory state to the byte chunks that make up the
    /// on-disk file. Mirrors Python's `DirState.get_lines` for the
    /// common "we have in-memory data to write" branch; it does not
    /// handle the fast-path shortcut that re-reads an unmodified file
    /// from disk (that shortcut belongs on the soon-to-be-ported
    /// `save` method).
    pub fn get_lines(&self) -> Vec<Vec<u8>> {
        let parents_refs: Vec<&[u8]> = self.parents.iter().map(|p| p.as_slice()).collect();
        let ghosts_refs: Vec<&[u8]> = self.ghosts.iter().map(|g| g.as_slice()).collect();
        let parents_line = get_parents_line(&parents_refs);
        let ghosts_line = get_ghosts_line(&ghosts_refs);

        let entry_lines = dirblocks_to_entry_lines(&self.dirblocks);

        // Build the owned-backing-store buffer, then borrow slices into
        // it when calling `get_output_lines`.
        let mut owned: Vec<Vec<u8>> = Vec::with_capacity(2 + entry_lines.len());
        owned.push(parents_line);
        owned.push(ghosts_line);
        owned.extend(entry_lines);
        let borrowed: Vec<&[u8]> = owned.iter().map(|l| l.as_slice()).collect();
        get_output_lines(borrowed)
    }

    /// Mark the dirstate as modified. Mirrors Python's
    /// `DirState._mark_modified`.
    ///
    /// If `hash_changed_entries` is non-empty, only the hash cache is
    /// affected: the provided entry keys are added to
    /// `known_hash_changes` and the `dirblock_state` transitions from
    /// `NotInMemory`/`InMemoryUnmodified` into `InMemoryHashModified`
    /// (a full `InMemoryModified` state takes precedence and is not
    /// downgraded).
    ///
    /// If `hash_changed_entries` is empty the whole dirblock state is
    /// considered dirty: `dirblock_state` becomes `InMemoryModified`
    /// regardless of its previous value. `header_modified` is an
    /// orthogonal flag that promotes `header_state` to
    /// `InMemoryModified` as well.
    pub fn mark_modified(&mut self, hash_changed_entries: &[EntryKey], header_modified: bool) {
        if !hash_changed_entries.is_empty() {
            for key in hash_changed_entries {
                self.known_hash_changes.insert(key.clone());
            }
            if matches!(
                self.dirblock_state,
                MemoryState::NotInMemory | MemoryState::InMemoryUnmodified
            ) {
                self.dirblock_state = MemoryState::InMemoryHashModified;
            }
        } else {
            self.dirblock_state = MemoryState::InMemoryModified;
        }
        if header_modified {
            self.header_state = MemoryState::InMemoryModified;
        }
    }

    /// Mark the dirstate as unmodified — both header and dirblock state
    /// return to `InMemoryUnmodified` and the hash-change set is
    /// cleared. Mirrors Python's `DirState._mark_unmodified`.
    pub fn mark_unmodified(&mut self) {
        self.header_state = MemoryState::InMemoryUnmodified;
        self.dirblock_state = MemoryState::InMemoryUnmodified;
        self.known_hash_changes.clear();
    }

    /// Replace the entire in-memory state with `parent_ids` and
    /// `dirblocks`, marking both the header and the dirblock data
    /// fully modified. Mirrors Python's `DirState._set_data`: the
    /// caller owns any sort/shape invariants on `dirblocks`; this
    /// method does not validate them.
    ///
    /// Any cached `id_index` is invalidated. Python's
    /// `_packed_stat_index` has no equivalent on the Rust struct yet
    /// and is therefore not touched here.
    pub fn set_data(&mut self, parent_ids: Vec<Vec<u8>>, dirblocks: Vec<Dirblock>) {
        self.dirblocks = dirblocks;
        self.mark_modified(&[], true);
        self.parents = parent_ids;
        self.id_index = None;
        self.packed_stat_index = None;
    }

    /// Overwrite the tree-0 slot of the entry at `key` with the given
    /// details. Returns an error if `key` is not present; otherwise
    /// does no other bookkeeping — no id_index changes, no cross-ref
    /// rewrites, no state bump. This is the narrow primitive the
    /// `py_update_entry` hash-refresh path needs: callers that want
    /// structural changes should use [`DirState::update_minimal`] or
    /// [`DirState::add`].
    pub fn set_tree0(&mut self, key: &EntryKey, details: TreeData) -> Result<(), MakeAbsentError> {
        let (block_index, block_present) = find_block_index_from_key(&self.dirblocks, key);
        if !block_present {
            return Err(MakeAbsentError::BlockNotFound { key: key.clone() });
        }
        let (entry_index, entry_present) =
            find_entry_index(key, &self.dirblocks[block_index].entries);
        if !entry_present {
            return Err(MakeAbsentError::EntryNotFound { key: key.clone() });
        }
        self.dirblocks[block_index].entries[entry_index].trees[0] = details;
        self.packed_stat_index = None;
        Ok(())
    }

    /// Return the live tree-0 minikind for `key`, or `None` when no
    /// entry with that key is present. Used by callers that need to
    /// refresh a stale snapshot against current dirblock contents
    /// (notably `set_state_from_inventory`'s zipper-merge loop, which
    /// used to rely on Python-side tuple aliasing to observe mid-loop
    /// rewrites).
    pub fn tree0_minikind(&self, key: &EntryKey) -> Option<u8> {
        let (block_index, block_present) = find_block_index_from_key(&self.dirblocks, key);
        if !block_present {
            return None;
        }
        let (entry_index, entry_present) =
            find_entry_index(key, &self.dirblocks[block_index].entries);
        if !entry_present {
            return None;
        }
        self.dirblocks[block_index].entries[entry_index]
            .trees
            .first()
            .map(|t| t.minikind)
    }

    /// Record an observed sha1 for `key`'s tree-0 row when the file's
    /// stat falls in the cacheable window.  Mirrors Python's
    /// `DirState._observed_sha1`: silently ignores non-file kinds and
    /// files whose mtime/ctime land after the cutoff.
    ///
    /// Takes the stat fields unpacked so callers can feed in whichever
    /// shape they already have (Python's `os.stat_result`, Rust's
    /// [`Metadata`], synthetic fixture data).
    #[allow(clippy::too_many_arguments)]
    pub fn observed_sha1(
        &mut self,
        key: &EntryKey,
        sha1: &[u8],
        st_mode: u32,
        st_size: u64,
        st_mtime: i64,
        st_ctime: i64,
        st_dev: u64,
        st_ino: u64,
    ) -> Result<(), UpdateEntryError> {
        use std::time::{SystemTime, UNIX_EPOCH};

        // S_IFREG (0o100000) after masking with S_IFMT.
        if (st_mode & 0o170000) != 0o100000 {
            return Ok(());
        }

        let now_secs: i64 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let cutoff: i64 = self.cutoff_time.unwrap_or_else(|| {
            let c = now_secs - 3;
            self.cutoff_time = Some(c);
            c
        });

        if st_mtime >= cutoff || st_ctime >= cutoff {
            return Ok(());
        }

        let (block_index, block_present) = find_block_index_from_key(&self.dirblocks, key);
        if !block_present {
            return Err(UpdateEntryError::EntryNotFound);
        }
        let (entry_index, entry_present) =
            find_entry_index(key, &self.dirblocks[block_index].entries);
        if !entry_present {
            return Err(UpdateEntryError::EntryNotFound);
        }
        let executable = self.dirblocks[block_index].entries[entry_index].trees[0].executable;
        let packed_stat = pack_stat(
            st_size,
            st_mtime as u64,
            st_ctime as u64,
            st_dev,
            st_ino,
            st_mode,
        )
        .into_bytes();
        self.dirblocks[block_index].entries[entry_index].trees[0] = TreeData {
            minikind: b'f',
            fingerprint: sha1.to_vec(),
            size: st_size,
            executable,
            packed_stat,
        };
        self.packed_stat_index = None;
        self.mark_modified(&[key.clone()], false);
        Ok(())
    }

    /// Refresh the tree-0 slot of `key` from the filesystem.  Mirrors
    /// Python's `py_update_entry`: if the stat hasn't changed since
    /// the last time we saved, re-use the cached link-or-sha1;
    /// otherwise read the file (or symlink) and rewrite the tree-0
    /// slot.  Returns the sha1 hex or symlink target, or `None` when
    /// the on-disk kind is not supported (e.g. block/char devices),
    /// when the row is a directory and the cached stat matches
    /// (nothing to report), or when we skip the sha because the
    /// Compare one dirstate entry against what's on disk (or nothing,
    /// if the path is absent in the target) and yield a
    /// [`DirstateChange`] describing any differences.  Ports Python's
    /// `ProcessEntryPython._process_entry`.
    ///
    /// Returns `(None, None)` when the entry is uninteresting (no row
    /// in either side of the comparison), `(None, Some(false))` when
    /// both sides match and `pstate.include_unchanged` is off,
    /// `(Some(change), Some(true))` for a real change, and
    /// `(Some(change), Some(false))` for an unchanged-but-included
    /// report.
    pub fn process_entry(
        &mut self,
        pstate: &mut ProcessEntryState,
        entry_key: &EntryKey,
        entry_trees: &[TreeData],
        path_info: Option<&ProcessPathInfo>,
        transport: &dyn Transport,
    ) -> Result<(Option<DirstateChange>, Option<bool>), ProcessEntryError> {
        let source_details: TreeData = if let Some(idx) = pstate.source_index {
            entry_trees
                .get(idx)
                .cloned()
                .unwrap_or_else(null_parent_details)
        } else {
            null_parent_details()
        };
        let target_idx = pstate.target_index;
        let mut target_details: TreeData = entry_trees
            .get(target_idx)
            .cloned()
            .unwrap_or_else(null_parent_details);
        let mut target_minikind = target_details.minikind;

        let fdlt = |k: u8| matches!(k, b'f' | b'd' | b'l' | b't');
        let fdltr = |k: u8| matches!(k, b'f' | b'd' | b'l' | b't' | b'r');

        // Step 1: if on disk and versioned in the target, refresh
        // via update_entry (which may flip minikind e.g. d → t).
        let mut link_or_sha1: Option<Vec<u8>> = None;
        if let Some(info) = path_info {
            if fdlt(target_minikind) {
                if target_idx != 0 {
                    return Err(ProcessEntryError::Internal(
                        "update_entry requires target_index == 0".into(),
                    ));
                }
                link_or_sha1 = self
                    .update_entry(entry_key, &info.abspath, &info.stat, transport)
                    .map_err(|e| ProcessEntryError::Internal(format!("update_entry: {}", e)))?;
                let (bi, _) = find_block_index_from_key(&self.dirblocks, entry_key);
                let (ei, _) = find_entry_index(entry_key, &self.dirblocks[bi].entries);
                target_details = self.dirblocks[bi].entries[ei].trees[target_idx].clone();
                target_minikind = target_details.minikind;
            }
        }

        let file_id = entry_key.file_id.clone();
        let mut source_minikind = source_details.minikind;
        let mut source_details_mut = source_details.clone();

        if fdltr(source_minikind) && fdlt(target_minikind) {
            let mut old_dirname: Vec<u8>;
            let mut old_basename: Vec<u8>;
            let mut old_path: Option<Vec<u8>>;
            let mut path: Option<Vec<u8>>;

            if source_minikind == b'r' {
                let src_path = source_details_mut.fingerprint.clone();
                let already_inside = pstate
                    .searched_specific_files
                    .iter()
                    .any(|p| is_inside(p.as_slice(), &src_path));
                if !already_inside {
                    pstate.search_specific_files.insert(src_path.clone());
                }
                old_path = Some(src_path.clone());
                let (od, ob) = split_path_utf8(&src_path);
                old_dirname = od.to_vec();
                old_basename = ob.to_vec();
                path = Some(join_path(&entry_key.dirname, &entry_key.basename));

                let src_idx = pstate.source_index.ok_or_else(|| {
                    ProcessEntryError::Internal("relocation with no source_index".into())
                })?;
                let bei =
                    get_block_entry_index(&self.dirblocks, &old_dirname, &old_basename, src_idx);
                let src = if bei.path_present {
                    self.dirblocks[bei.block_index].entries[bei.entry_index]
                        .trees
                        .get(src_idx)
                        .cloned()
                } else {
                    None
                };
                let src = src.ok_or_else(|| {
                    ProcessEntryError::DirstateCorrupt(format!(
                        "entry '{}/{}' is considered renamed from {:?} but source does not exist",
                        String::from_utf8_lossy(&entry_key.dirname),
                        String::from_utf8_lossy(&entry_key.basename),
                        src_path,
                    ))
                })?;
                source_details_mut = src;
                source_minikind = source_details_mut.minikind;
            } else {
                old_dirname = entry_key.dirname.clone();
                old_basename = entry_key.basename.clone();
                old_path = None;
                path = None;
            }

            let (content_change, target_kind, target_exec) = if let Some(info) = path_info {
                let target_kind_str: &str = info.kind.as_deref().unwrap_or("file");
                match target_kind_str {
                    "directory" => {
                        if path.is_none() {
                            let p = join_path(&old_dirname, &old_basename);
                            path = Some(p.clone());
                            old_path = Some(p);
                        }
                        if let Some(p) = path.as_ref() {
                            pstate
                                .new_dirname_to_file_id
                                .insert(p.clone(), file_id.clone());
                        }
                        (
                            source_minikind != b'd',
                            Some("directory".to_string()),
                            false,
                        )
                    }
                    "file" => {
                        let cc = if source_minikind != b'f' {
                            true
                        } else {
                            if link_or_sha1.is_none() {
                                let path_buf = bytes_to_path(&info.abspath);
                                let sha = self.sha1_provider.sha1(&path_buf).map_err(|e| {
                                    ProcessEntryError::Internal(format!("sha1: {}", e))
                                })?;
                                let sha_bytes = sha.as_bytes().to_vec();
                                let _ = self.observed_sha1(
                                    entry_key,
                                    &sha_bytes,
                                    info.stat.mode,
                                    info.stat.size,
                                    info.stat.mtime,
                                    info.stat.ctime,
                                    info.stat.dev,
                                    info.stat.ino,
                                );
                                link_or_sha1 = Some(sha_bytes);
                            }
                            link_or_sha1.as_deref()
                                != Some(source_details_mut.fingerprint.as_slice())
                        };
                        let te = if self.use_filesystem_for_exec {
                            (info.stat.mode & 0o100) != 0
                        } else {
                            target_details.executable
                        };
                        (cc, Some("file".to_string()), te)
                    }
                    "symlink" => {
                        let cc = if source_minikind != b'l' {
                            true
                        } else {
                            link_or_sha1.as_deref()
                                != Some(source_details_mut.fingerprint.as_slice())
                        };
                        (cc, Some("symlink".to_string()), false)
                    }
                    "tree-reference" => (
                        source_minikind != b't',
                        Some("tree-reference".to_string()),
                        false,
                    ),
                    other => {
                        if path.is_none() {
                            path = Some(join_path(&old_dirname, &old_basename));
                        }
                        return Err(ProcessEntryError::BadFileKind {
                            path: path.unwrap(),
                            kind: other.to_string(),
                        });
                    }
                }
            } else {
                (true, None, false)
            };

            if source_minikind == b'd' {
                if path.is_none() {
                    let p = join_path(&old_dirname, &old_basename);
                    path = Some(p.clone());
                    old_path = Some(p);
                }
                if let Some(op) = old_path.as_ref() {
                    pstate
                        .old_dirname_to_file_id
                        .insert(op.clone(), file_id.clone());
                }
            }

            let source_parent_id = resolve_parent_id(
                &self.dirblocks,
                &old_dirname,
                &old_basename,
                &entry_key.file_id,
                pstate.source_index.unwrap_or(0),
                &pstate.old_dirname_to_file_id,
                &mut pstate.last_source_parent,
            );
            let target_parent_id = resolve_target_parent_id(
                &self.dirblocks,
                &entry_key.dirname,
                &entry_key.basename,
                &entry_key.file_id,
                target_idx,
                &pstate.new_dirname_to_file_id,
                &mut pstate.last_target_parent,
            )?;

            let source_exec = source_details_mut.executable;
            let changed = content_change
                || source_parent_id != target_parent_id
                || old_basename != entry_key.basename
                || source_exec != target_exec;

            if !changed && !pstate.include_unchanged {
                return Ok((None, Some(false)));
            }

            let (old_path_out, path_out) = match old_path {
                Some(ref op) => (op.clone(), path.clone().unwrap_or_else(|| op.clone())),
                None => {
                    let p = join_path(&old_dirname, &old_basename);
                    (p.clone(), p)
                }
            };

            return Ok((
                Some(DirstateChange {
                    file_id: entry_key.file_id.clone(),
                    old_path: Some(old_path_out),
                    new_path: Some(path_out),
                    content_change,
                    old_versioned: true,
                    new_versioned: true,
                    source_parent_id,
                    target_parent_id,
                    old_basename: Some(old_basename),
                    new_basename: Some(entry_key.basename.clone()),
                    source_kind: kind_for_minikind(source_minikind),
                    target_kind,
                    source_exec: Some(source_exec),
                    target_exec: Some(target_exec),
                }),
                Some(changed),
            ));
        }

        if source_minikind == b'a' && fdlt(target_minikind) {
            let path = join_path(&entry_key.dirname, &entry_key.basename);
            let parent_bei =
                get_block_entry_index(&self.dirblocks, &Vec::new(), &entry_key.dirname, target_idx);
            let parent_id: Option<Vec<u8>> = if parent_bei.path_present {
                let pid = self.dirblocks[parent_bei.block_index].entries[parent_bei.entry_index]
                    .key
                    .file_id
                    .clone();
                (pid != entry_key.file_id).then_some(pid)
            } else {
                None
            };
            if let Some(info) = path_info {
                let te = if self.use_filesystem_for_exec {
                    (info.stat.mode & 0o170000 == 0o100000) && (info.stat.mode & 0o100) != 0
                } else {
                    target_details.executable
                };
                return Ok((
                    Some(DirstateChange {
                        file_id: entry_key.file_id.clone(),
                        old_path: None,
                        new_path: Some(path),
                        content_change: true,
                        old_versioned: false,
                        new_versioned: true,
                        source_parent_id: None,
                        target_parent_id: parent_id,
                        old_basename: None,
                        new_basename: Some(entry_key.basename.clone()),
                        source_kind: None,
                        target_kind: info.kind.clone(),
                        source_exec: None,
                        target_exec: Some(te),
                    }),
                    Some(true),
                ));
            } else {
                return Ok((
                    Some(DirstateChange {
                        file_id: entry_key.file_id.clone(),
                        old_path: None,
                        new_path: Some(path),
                        content_change: false,
                        old_versioned: false,
                        new_versioned: true,
                        source_parent_id: None,
                        target_parent_id: parent_id,
                        old_basename: None,
                        new_basename: Some(entry_key.basename.clone()),
                        source_kind: None,
                        target_kind: None,
                        source_exec: None,
                        target_exec: Some(false),
                    }),
                    Some(true),
                ));
            }
        }

        if fdlt(source_minikind) && target_minikind == b'a' {
            let old_path = join_path(&entry_key.dirname, &entry_key.basename);
            let src_idx = pstate.source_index.unwrap_or(0);
            let parent_bei =
                get_block_entry_index(&self.dirblocks, &Vec::new(), &entry_key.dirname, src_idx);
            let parent_id: Option<Vec<u8>> = if parent_bei.path_present {
                let pid = self.dirblocks[parent_bei.block_index].entries[parent_bei.entry_index]
                    .key
                    .file_id
                    .clone();
                (pid != entry_key.file_id).then_some(pid)
            } else {
                None
            };
            return Ok((
                Some(DirstateChange {
                    file_id: entry_key.file_id.clone(),
                    old_path: Some(old_path),
                    new_path: None,
                    content_change: true,
                    old_versioned: true,
                    new_versioned: false,
                    source_parent_id: parent_id,
                    target_parent_id: None,
                    old_basename: Some(entry_key.basename.clone()),
                    new_basename: None,
                    source_kind: kind_for_minikind(source_minikind),
                    target_kind: None,
                    source_exec: Some(source_details_mut.executable),
                    target_exec: None,
                }),
                Some(true),
            ));
        }

        if fdlt(source_minikind) && target_minikind == b'r' {
            let tpath = target_details.fingerprint.clone();
            let already_inside = pstate
                .searched_specific_files
                .iter()
                .any(|p| is_inside(p.as_slice(), &tpath));
            if !already_inside {
                pstate.search_specific_files.insert(tpath);
            }
            return Ok((None, None));
        }

        let ra = |k: u8| matches!(k, b'r' | b'a');
        if ra(source_minikind) && ra(target_minikind) {
            return Ok((None, None));
        }

        Err(ProcessEntryError::Internal(format!(
            "don't know how to compare source_minikind={:?}, target_minikind={:?}",
            source_minikind, target_minikind
        )))
    }

    /// Refresh the tree-0 slot of `key` from the filesystem.  Mirrors
    /// Python's `py_update_entry`:
    ///
    /// Arguments are (key, abspath, stat, transport) — see the doc
    /// comment on [`StatInfo`] for the stat fields, and the
    /// [`Transport`] trait for read_link semantics.
    pub fn update_entry(
        &mut self,
        key: &EntryKey,
        abspath: &[u8],
        stat: &StatInfo,
        transport: &dyn Transport,
    ) -> Result<Option<Vec<u8>>, UpdateEntryError> {
        use std::time::{SystemTime, UNIX_EPOCH};

        // 1. Derive minikind from st_mode.  Non-file/dir/symlink kinds
        //    are silently skipped (Python returns None via the
        //    KeyError branch).
        let mut minikind: u8 = if stat.is_file() {
            b'f'
        } else if stat.is_dir() {
            b'd'
        } else if stat.is_symlink() {
            b'l'
        } else {
            return Ok(None);
        };

        let packed_stat = pack_stat(
            stat.size,
            stat.mtime as u64,
            stat.ctime as u64,
            stat.dev,
            stat.ino,
            stat.mode,
        )
        .into_bytes();

        // 2. Fetch the saved tree-0 row (need a clone, we'll mutate it).
        let (block_index, block_present) = find_block_index_from_key(&self.dirblocks, key);
        if !block_present {
            return Err(UpdateEntryError::EntryNotFound);
        }
        let (entry_index, entry_present) =
            find_entry_index(key, &self.dirblocks[block_index].entries);
        if !entry_present {
            return Err(UpdateEntryError::EntryNotFound);
        }
        let entry_len = self.dirblocks[block_index].entries[entry_index].trees.len();
        let tree1_minikind: u8 = self.dirblocks[block_index].entries[entry_index]
            .trees
            .get(1)
            .map(|t| t.minikind)
            .unwrap_or(0);
        let saved = self.dirblocks[block_index].entries[entry_index].trees[0].clone();

        // 3. A directory row that used to be a tree-reference keeps
        //    its 't' minikind even when the filesystem kind is plain
        //    directory (matches Python's special case).
        if minikind == b'd' && saved.minikind == b't' {
            minikind = b't';
        }

        // 4. Cache-hit path: same kind + same stat + same size → return
        //    saved link/sha1 without further I/O.
        if minikind == saved.minikind && packed_stat == saved.packed_stat {
            if minikind == b'd' {
                return Ok(None);
            }
            if saved.size == stat.size {
                return Ok(Some(saved.fingerprint.clone()));
            }
        }

        // 5. Cache miss — rewrite the row.
        let now_secs: i64 = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let cutoff: i64 = self.cutoff_time.unwrap_or_else(|| {
            let c = now_secs - 3;
            self.cutoff_time = Some(c);
            c
        });

        let stat_is_cacheable = stat.mtime < cutoff && stat.ctime < cutoff;

        let mut result: Option<Vec<u8>> = None;
        let mut worth_saving = true;
        let mut became_directory = false;

        // Tree-references don't get a tree-0 rewrite: the Python
        // implementation's if/elif chain has no arm for b't', so the
        // saved row is left intact and only mark_modified runs.
        if minikind == b't' {
            self.mark_modified(&[key.clone()], false);
            return Ok(None);
        }

        let new_tree0 = match minikind {
            b'f' => {
                let executable = if self.use_filesystem_for_exec {
                    (stat.mode & 0o100) != 0
                } else {
                    saved.executable
                };
                if stat_is_cacheable && entry_len > 1 && tree1_minikind != b'a' {
                    // SHA1Provider remains a pluggable indirection for
                    // content hashing (content filters).  Callers can
                    // install a provider that reads through their own
                    // layer; DefaultSHA1Provider is a thin wrapper
                    // over `sha_file_by_name`.
                    let path_buf = bytes_to_path(abspath);
                    let sha1 = self
                        .sha1_provider
                        .sha1(&path_buf)
                        .map_err(UpdateEntryError::Io)?;
                    result = Some(sha1.as_bytes().to_vec());
                    TreeData {
                        minikind: b'f',
                        fingerprint: sha1.into_bytes(),
                        size: stat.size,
                        executable,
                        packed_stat,
                    }
                } else {
                    worth_saving = false;
                    TreeData {
                        minikind: b'f',
                        fingerprint: Vec::new(),
                        size: stat.size,
                        executable,
                        packed_stat: b"x".repeat(32),
                    }
                }
            }
            b'd' => {
                if saved.minikind != b'd' {
                    became_directory = true;
                } else {
                    worth_saving = false;
                }
                TreeData {
                    minikind: b'd',
                    fingerprint: Vec::new(),
                    size: 0,
                    executable: false,
                    packed_stat,
                }
            }
            b'l' => {
                if saved.minikind == b'l' {
                    worth_saving = false;
                }
                let target_bytes = transport.read_link(abspath).map_err(|e| match e {
                    TransportError::Io { kind, message } => {
                        UpdateEntryError::Io(std::io::Error::new(kind, message))
                    }
                    TransportError::NotFound(p) => {
                        UpdateEntryError::Io(std::io::Error::new(std::io::ErrorKind::NotFound, p))
                    }
                    other => UpdateEntryError::Other(other.to_string()),
                })?;
                result = Some(target_bytes.clone());
                if stat_is_cacheable {
                    TreeData {
                        minikind: b'l',
                        fingerprint: target_bytes,
                        size: stat.size,
                        executable: false,
                        packed_stat,
                    }
                } else {
                    TreeData {
                        minikind: b'l',
                        fingerprint: Vec::new(),
                        size: stat.size,
                        executable: false,
                        packed_stat: b"x".repeat(32),
                    }
                }
            }
            _ => {
                // Already handled via the fall-through in step 1; any
                // other minikind here is an internal error.
                return Err(UpdateEntryError::UnexpectedKind(minikind));
            }
        };

        self.dirblocks[block_index].entries[entry_index].trees[0] = new_tree0;
        self.packed_stat_index = None;

        if became_directory {
            // A former file/symlink is now a directory; ensure the
            // child dirblock exists.
            let (dirname_parent, basename_parent) = (key.dirname.clone(), key.basename.clone());
            let parent_bei =
                get_block_entry_index(&self.dirblocks, &dirname_parent, &basename_parent, 0);
            if parent_bei.path_present {
                let mut subdir = dirname_parent.clone();
                if !subdir.is_empty() {
                    subdir.push(b'/');
                }
                subdir.extend_from_slice(&basename_parent);
                self.ensure_block(
                    parent_bei.block_index as isize,
                    parent_bei.entry_index as isize,
                    &subdir,
                )
                .map_err(|e| UpdateEntryError::Other(format!("ensure_block: {:?}", e)))?;
            }
        }

        if worth_saving {
            self.mark_modified(&[key.clone()], false);
        }

        Ok(result)
    }

    /// Append a `NULL_PARENT_DETAILS` row to every entry's tree slot
    /// list. Mirrors Python's inline loop in `update_basis_by_delta`:
    /// when the current dirstate has no parents and a new parent is
    /// being introduced, each row needs space for the new parent's
    /// tree-1 slot before `update_basis_by_delta` can fill it in.
    pub fn bootstrap_new_parent_slot(&mut self) {
        for block in self.dirblocks.iter_mut() {
            for entry in block.entries.iter_mut() {
                entry.trees.push(TreeData {
                    minikind: b'a',
                    fingerprint: Vec::new(),
                    size: 0,
                    executable: false,
                    packed_stat: Vec::new(),
                });
            }
        }
    }

    /// Forget all in-memory state, returning the object to the same
    /// shape a freshly constructed [`DirState`] has before any load.
    /// Mirrors Python's `DirState._wipe_state`.
    ///
    /// Python additionally clears `_split_path_cache`; that field has
    /// no equivalent on the Rust struct yet (the still un-ported
    /// memoisation layer on `_find_block_index_from_key`), so this
    /// function resets what it can and leaves a note for the future
    /// port to extend.
    pub fn wipe_state(&mut self) {
        self.header_state = MemoryState::NotInMemory;
        self.dirblock_state = MemoryState::NotInMemory;
        self.changes_aborted = false;
        self.parents.clear();
        self.ghosts.clear();
        self.dirblocks.clear();
        self.id_index = None;
        self.packed_stat_index = None;
        self.end_of_header = None;
        self.cutoff_time = None;
    }

    /// Whether the current in-memory state is worth persisting. Mirrors
    /// `DirState._worth_saving`: full-dirblock or header modifications
    /// always save; hash-only changes save only once they exceed
    /// `worth_saving_limit`, and `-1` disables hash-only saves entirely.
    pub fn worth_saving(&self) -> bool {
        if matches!(self.header_state, MemoryState::InMemoryModified)
            || matches!(self.dirblock_state, MemoryState::InMemoryModified)
        {
            return true;
        }
        if matches!(self.dirblock_state, MemoryState::InMemoryHashModified) {
            if self.worth_saving_limit == -1 {
                return false;
            }
            if self.known_hash_changes.len() as i64 >= self.worth_saving_limit {
                return true;
            }
        }
        false
    }

    /// Persist the in-memory state through `transport`, assuming a
    /// write lock is already held. This is the post-lock-upgrade core
    /// of Python's `DirState.save`: honours `changes_aborted` and
    /// `worth_saving` as early-return gates, serialises `get_lines()`
    /// via `write_all`, optionally `fdatasync`s, and finishes with
    /// `mark_unmodified`.
    ///
    /// The caller owns the read→write lock-upgrade dance that Python's
    /// `save` performs via `temporary_write_lock` — the `Transport`
    /// trait deliberately does not model it, because lock-upgrade
    /// semantics belong to the Python `LockToken` plumbing rather than
    /// to dirstate. A caller that wants the full Python behaviour
    /// performs the upgrade, calls `save_to`, then restores the read
    /// lock.
    ///
    /// Returns `Ok(true)` if the state was actually written, `Ok(false)`
    /// if an early-return gate prevented the write, and `Err` if the
    /// transport is not write-locked or any `write_all`/`fdatasync`
    /// call failed.
    pub fn save_to<T: Transport + ?Sized>(
        &mut self,
        transport: &mut T,
    ) -> Result<bool, TransportError> {
        if self.changes_aborted {
            return Ok(false);
        }
        if !self.worth_saving() {
            return Ok(false);
        }
        if transport.lock_state() != Some(LockState::Write) {
            return Err(TransportError::Other(
                "save_to requires a write lock".to_string(),
            ));
        }
        let mut buf: Vec<u8> = Vec::new();
        for line in self.get_lines() {
            buf.extend_from_slice(&line);
        }
        transport.write_all(&buf)?;
        if self.fdatasync {
            transport.fdatasync()?;
        }
        self.mark_unmodified();
        Ok(true)
    }

    /// Number of parent entries present in each dirstate record row.
    /// Mirrors Python's `DirState._num_present_parents` — total
    /// parents minus ghost parents.
    pub fn num_present_parents(&self) -> usize {
        self.parents.len().saturating_sub(self.ghosts.len())
    }

    /// Replace the entire tree-0 state with the rows produced by
    /// walking `new_inv.iter_entries_by_dir()`. Mirrors Python's
    /// `DirState.set_state_from_inventory`: zips the existing dirstate
    /// entries (in iteration order) against the incoming inventory
    /// entries, calling [`DirState::update_minimal`] and
    /// [`DirState::make_absent`] to drive the dirstate into the new
    /// shape.
    ///
    /// Each element of `new_entries` is a pre-sorted tuple
    /// `(path_utf8, file_id, minikind, fingerprint, executable)`. The
    /// caller is expected to have built it from
    /// `iter_entries_by_dir`, which yields paths in the order the
    /// dirstate needs. `fingerprint` is normally empty for non
    /// tree-reference entries; the tree-reference case carries the
    /// `reference_revision` bytes.
    pub fn set_state_from_inventory(
        &mut self,
        new_entries: Vec<(Vec<u8>, Vec<u8>, u8, Vec<u8>, bool)>,
    ) -> Result<(), BasisApplyError> {
        fn cmp_by_dirs(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
            let mut ai = a.split(|&c| c == b'/');
            let mut bi = b.split(|&c| c == b'/');
            loop {
                match (ai.next(), bi.next()) {
                    (None, None) => return std::cmp::Ordering::Equal,
                    (None, Some(_)) => return std::cmp::Ordering::Less,
                    (Some(_), None) => return std::cmp::Ordering::Greater,
                    (Some(x), Some(y)) => match x.cmp(y) {
                        std::cmp::Ordering::Equal => continue,
                        other => return other,
                    },
                }
            }
        }

        // Snapshot the current tree-0 entries in dirstate iteration order,
        // mirroring Python's `list(self._iter_entries())` call.
        let old_entries: Vec<Entry> = self
            .dirblocks
            .iter()
            .flat_map(|block| block.entries.iter().cloned())
            .collect();

        let mut old_iter = old_entries.into_iter();
        let mut new_iter = new_entries.into_iter();
        let mut current_old: Option<Entry> = old_iter.next();
        let mut current_new: Option<(Vec<u8>, Vec<u8>, u8, Vec<u8>, bool)> = new_iter.next();

        while current_new.is_some() || current_old.is_some() {
            // Skip dead old rows: the live tree-0 minikind may differ
            // from the snapshot because prior update_minimal calls in
            // this loop could have rewritten it.
            if let Some(ref old) = current_old {
                let live = self.tree0_minikind(&old.key);
                if matches!(live, None | Some(b'a') | Some(b'r')) {
                    current_old = old_iter.next();
                    continue;
                }
            }

            // Materialise the new-entry split.
            let new_split = current_new.as_ref().map(|(path, file_id, mk, fp, ex)| {
                let (dn, bn) = split_path_utf8(path);
                let new_key = EntryKey {
                    dirname: dn.to_vec(),
                    basename: bn.to_vec(),
                    file_id: file_id.clone(),
                };
                (path.clone(), new_key, *mk, fp.clone(), *ex)
            });

            match (current_old.as_ref(), new_split.as_ref()) {
                (None, Some((path, key, mk, fp, ex))) => {
                    // Old is finished; insert the new entry.
                    let tree0 = TreeData {
                        minikind: *mk,
                        fingerprint: fp.clone(),
                        size: 0,
                        executable: *ex,
                        packed_stat: b"x".repeat(32),
                    };
                    self.update_minimal(key.clone(), tree0, Some(path), true)?;
                    current_new = new_iter.next();
                }
                (Some(old), None) => {
                    // New is finished; make the old entry absent.
                    let key = old.key.clone();
                    // Swallow EntryNotFound — a prior update_minimal
                    // may have pruned the row already.
                    if self.tree0_minikind(&key).is_some() {
                        self.make_absent(&key)
                            .map_err(|e| BasisApplyError::Internal {
                                reason: format!("make_absent: {}", e),
                            })?;
                    }
                    current_old = old_iter.next();
                }
                (Some(old), Some((path, key, mk, fp, ex))) => {
                    if *key == old.key {
                        // Same key; update in place if exec/minikind changed.
                        let old_t0 = &old.trees[0];
                        if old_t0.executable != *ex || old_t0.minikind != *mk {
                            let tree0 = TreeData {
                                minikind: *mk,
                                fingerprint: fp.clone(),
                                size: 0,
                                executable: *ex,
                                packed_stat: b"x".repeat(32),
                            };
                            self.update_minimal(key.clone(), tree0, Some(path), true)?;
                        }
                        current_old = old_iter.next();
                        current_new = new_iter.next();
                    } else {
                        let new_before_old = match cmp_by_dirs(&key.dirname, &old.key.dirname) {
                            std::cmp::Ordering::Less => true,
                            std::cmp::Ordering::Greater => false,
                            std::cmp::Ordering::Equal => {
                                (key.basename.as_slice(), key.file_id.as_slice())
                                    < (old.key.basename.as_slice(), old.key.file_id.as_slice())
                            }
                        };
                        if new_before_old {
                            let tree0 = TreeData {
                                minikind: *mk,
                                fingerprint: fp.clone(),
                                size: 0,
                                executable: *ex,
                                packed_stat: b"x".repeat(32),
                            };
                            self.update_minimal(key.clone(), tree0, Some(path), true)?;
                            current_new = new_iter.next();
                        } else {
                            let okey = old.key.clone();
                            if self.tree0_minikind(&okey).is_some() {
                                self.make_absent(&okey)
                                    .map_err(|e| BasisApplyError::Internal {
                                        reason: format!("make_absent: {}", e),
                                    })?;
                            }
                            current_old = old_iter.next();
                        }
                    }
                }
                (None, None) => unreachable!(),
            }
        }
        self.mark_modified(&[], false);
        self.id_index = None;
        Ok(())
    }

    /// Replace the parent trees. Mirrors Python's
    /// `DirState.set_parent_trees`.
    ///
    /// `trees` gives the revision-id of every parent (including
    /// ghosts) in order. `ghosts` is the list of revision-ids that
    /// are ghosts — must be a subset of `trees`. `parent_tree_entries`
    /// is one list per *non-ghost* parent tree, in the same order as
    /// non-ghost parents appear in `trees`; each list is the result of
    /// walking that tree via `iter_entries_by_dir` and mapping each
    /// entry to `(path_utf8, file_id, minikind, fingerprint, size,
    /// executable, tree_data)` (i.e. path/file_id plus the 5-tuple
    /// returned by [`inv_entry_to_details`]).
    ///
    /// The method rebuilds the full dirblocks layout from: (a) the
    /// current tree-0 rows already in `self.dirblocks` (non-absent,
    /// non-relocated), and (b) the per-parent-tree entry lists.
    /// Cross-tree relocation pointers are emitted in both the
    /// vertical and horizontal axes, matching the legacy matrix
    /// construction. Ghost parents occupy a tree slot but contribute
    /// no entries — their slot is always `NULL_PARENT_DETAILS`.
    pub fn set_parent_trees(
        &mut self,
        trees: Vec<Vec<u8>>,
        ghosts: Vec<Vec<u8>>,
        parent_tree_entries: Vec<Vec<(Vec<u8>, Vec<u8>, TreeData)>>,
    ) -> Result<(), EntriesToStateError> {
        let non_ghost_count = parent_tree_entries.len();
        // All parent slots, including ghosts: each entry has
        // `1 + non_ghost_count` tree slots.
        let parent_count = non_ghost_count;

        let mut by_path: std::collections::HashMap<EntryKey, Vec<TreeData>> =
            std::collections::HashMap::new();
        let mut id_index = IdIndex::new();

        // Step 1: seed with existing tree-0 entries.
        for block in self.dirblocks.iter() {
            for entry in block.entries.iter() {
                let mk = entry.trees.first().map(|t| t.minikind).unwrap_or(0);
                if mk == b'a' || mk == b'r' {
                    continue;
                }
                let mut row = Vec::with_capacity(1 + parent_count);
                row.push(entry.trees[0].clone());
                for _ in 0..parent_count {
                    row.push(TreeData {
                        minikind: b'a',
                        fingerprint: Vec::new(),
                        size: 0,
                        executable: false,
                        packed_stat: Vec::new(),
                    });
                }
                id_index.add((
                    entry.key.dirname.as_slice(),
                    entry.key.basename.as_slice(),
                    &FileId::from(&entry.key.file_id),
                ));
                by_path.insert(entry.key.clone(), row);
            }
        }

        // Step 2: fold each non-ghost parent tree into the matrix.
        for (index, tree_entries) in parent_tree_entries.into_iter().enumerate() {
            let tree_index = index + 1;
            let new_location_suffix_len = parent_count - tree_index;
            for (path_utf8, file_id, details) in tree_entries {
                let (dirname, basename) = split_path_utf8(&path_utf8);
                let new_entry_key = EntryKey {
                    dirname: dirname.to_vec(),
                    basename: basename.to_vec(),
                    file_id: file_id.clone(),
                };

                let fid = FileId::from(&file_id);
                let entry_keys: Vec<(Vec<u8>, Vec<u8>, FileId)> = id_index.get(&fid);

                // Vertical axis: every other path for this file_id in
                // this tree gets a relocation pointer back to path_utf8.
                for (e_dir, e_base, _e_fid) in &entry_keys {
                    let ek = EntryKey {
                        dirname: e_dir.clone(),
                        basename: e_base.clone(),
                        file_id: file_id.clone(),
                    };
                    if ek == new_entry_key {
                        continue;
                    }
                    if let Some(row) = by_path.get_mut(&ek) {
                        row[tree_index] = TreeData {
                            minikind: b'r',
                            fingerprint: path_utf8.clone(),
                            size: 0,
                            executable: false,
                            packed_stat: Vec::new(),
                        };
                    }
                }

                // By-path consistency: insert into existing row or
                // create a new one with relocation pointers for the
                // earlier tree indexes.
                let has_key = entry_keys.iter().any(|(d, b, _)| {
                    d.as_slice() == new_entry_key.dirname.as_slice()
                        && b.as_slice() == new_entry_key.basename.as_slice()
                });
                if has_key {
                    by_path.get_mut(&new_entry_key).unwrap()[tree_index] = details;
                } else {
                    let mut new_details: Vec<TreeData> = Vec::with_capacity(1 + parent_count);
                    for lookup_index in 0..tree_index {
                        if entry_keys.is_empty() {
                            new_details.push(TreeData {
                                minikind: b'a',
                                fingerprint: Vec::new(),
                                size: 0,
                                executable: false,
                                packed_stat: Vec::new(),
                            });
                        } else {
                            let a_key = &entry_keys[0];
                            let ak = EntryKey {
                                dirname: a_key.0.clone(),
                                basename: a_key.1.clone(),
                                file_id: file_id.clone(),
                            };
                            let look = &by_path[&ak][lookup_index];
                            if look.minikind == b'r' || look.minikind == b'a' {
                                new_details.push(look.clone());
                            } else {
                                let mut real_path = a_key.0.clone();
                                if !real_path.is_empty() {
                                    real_path.push(b'/');
                                }
                                real_path.extend_from_slice(&a_key.1);
                                new_details.push(TreeData {
                                    minikind: b'r',
                                    fingerprint: real_path,
                                    size: 0,
                                    executable: false,
                                    packed_stat: Vec::new(),
                                });
                            }
                        }
                    }
                    new_details.push(details);
                    for _ in 0..new_location_suffix_len {
                        new_details.push(TreeData {
                            minikind: b'a',
                            fingerprint: Vec::new(),
                            size: 0,
                            executable: false,
                            packed_stat: Vec::new(),
                        });
                    }
                    by_path.insert(new_entry_key.clone(), new_details);
                    id_index.add((
                        new_entry_key.dirname.as_slice(),
                        new_entry_key.basename.as_slice(),
                        &fid,
                    ));
                }
            }
        }

        // Step 3: materialise the sorted entry list.
        let mut new_entries: Vec<Entry> = by_path
            .into_iter()
            .map(|(key, trees)| Entry { key, trees })
            .collect();
        Self::sort_entries(&mut new_entries);
        self.entries_to_current_state(new_entries)?;
        self.parents = trees;
        self.ghosts = ghosts;
        self.mark_modified(&[], true);
        self.id_index = Some(id_index);
        self.packed_stat_index = None;
        Ok(())
    }

    /// Rebuild `self.dirblocks` from a pre-sorted, flat list of
    /// entries. Mirrors Python's `DirState._entries_to_current_state`.
    ///
    /// `new_entries` must start with the root row (dirname and
    /// basename both empty); otherwise
    /// [`EntriesToStateError::MissingRootRow`] is returned. The
    /// resulting layout contains the two sentinel empty-dirname blocks
    /// followed by one block per distinct subdirectory, then fed
    /// through [`DirState::split_root_dirblock_into_contents`] to
    /// separate the root row from the root-contents rows.
    ///
    /// This function does not re-sort entries — callers that hand in a
    /// sorted list skip the cost, and Python's comment calls this out
    /// explicitly.
    pub fn entries_to_current_state(
        &mut self,
        new_entries: Vec<Entry>,
    ) -> Result<(), EntriesToStateError> {
        let first = new_entries.first().ok_or(EntriesToStateError::Empty)?;
        if !first.key.dirname.is_empty() || !first.key.basename.is_empty() {
            return Err(EntriesToStateError::MissingRootRow {
                key: first.key.clone(),
            });
        }

        let mut dirblocks: Vec<Dirblock> = vec![
            Dirblock {
                dirname: Vec::new(),
                entries: Vec::new(),
            },
            Dirblock {
                dirname: Vec::new(),
                entries: Vec::new(),
            },
        ];
        // Root-group index: all entries with dirname == b"" are
        // appended to dirblocks[0]; `split_root_dirblock_into_contents`
        // later splits them into the true root and the contents-of-root.
        let mut current_idx: usize = 0;
        let mut current_dirname: Vec<u8> = Vec::new();
        for entry in new_entries {
            if entry.key.dirname != current_dirname {
                current_dirname = entry.key.dirname.clone();
                dirblocks.push(Dirblock {
                    dirname: current_dirname.clone(),
                    entries: Vec::new(),
                });
                current_idx = dirblocks.len() - 1;
            }
            dirblocks[current_idx].entries.push(entry);
        }
        self.dirblocks = dirblocks;
        self.id_index = None;
        self.packed_stat_index = None;
        split_root_dirblock_into_contents(&mut self.dirblocks)
            .map_err(EntriesToStateError::SplitFailed)?;
        Ok(())
    }

    /// Ensure a block for `dirname` exists in `self.dirblocks`, creating
    /// it if necessary. Mirrors Python's `DirState._ensure_block`.
    ///
    /// `parent_block_index` and `parent_row_index` identify the entry
    /// whose directory is being ensured. The root row is special-cased:
    /// `(parent_block_index=0, parent_row_index=0, dirname=b"")`
    /// shortcuts to block index 1 — the sentinel contents-of-root
    /// block produced by `split_root_dirblock_into_contents`.
    ///
    /// On success returns the index of the block for `dirname`. On
    /// failure — the dirname does not end with the basename stored at
    /// the given parent coordinates — returns
    /// [`EnsureBlockError::BadDirname`] to match Python's
    /// `AssertionError("bad dirname ...")`.
    pub fn ensure_block(
        &mut self,
        parent_block_index: isize,
        parent_row_index: isize,
        dirname: &[u8],
    ) -> Result<usize, EnsureBlockError> {
        // Root shortcut: block 0 row 0 with an empty dirname is always
        // followed by the empty sentinel at block 1.
        if dirname.is_empty() && parent_row_index == 0 && parent_block_index == 0 {
            return Ok(1);
        }
        // Python's assertion: dirname must end with the parent entry's
        // basename. The Python source guards the lookup with a
        // `(parent_block_index == -1 and parent_block_index == -1 and
        //   dirname == b"")` short-circuit — the second `parent_block_index`
        // is almost certainly meant to be `parent_row_index`, but we
        // preserve the (tautological) behaviour so this port is strictly
        // observation-preserving.
        let sentinel_shortcut =
            parent_block_index == -1 && parent_block_index == -1 && dirname.is_empty();
        if !sentinel_shortcut {
            let parent_basename = self
                .dirblocks
                .get(parent_block_index as usize)
                .and_then(|b| b.entries.get(parent_row_index as usize))
                .map(|e| e.key.basename.as_slice())
                .ok_or_else(|| EnsureBlockError::BadDirname(dirname.to_vec()))?;
            if !dirname.ends_with(parent_basename) {
                return Err(EnsureBlockError::BadDirname(dirname.to_vec()));
            }
        }
        let lookup_key = EntryKey {
            dirname: dirname.to_vec(),
            basename: Vec::new(),
            file_id: Vec::new(),
        };
        let (block_index, present) = find_block_index_from_key(&self.dirblocks, &lookup_key);
        if !present {
            self.dirblocks.insert(
                block_index,
                Dirblock {
                    dirname: dirname.to_vec(),
                    entries: Vec::new(),
                },
            );
        }
        Ok(block_index)
    }

    /// Discard any parent trees beyond the first. Mirrors Python's
    /// `DirState._discard_merge_parents`.
    ///
    /// After this function returns the dirstate contains either 1 or
    /// 2 trees per row: current + first parent, or just current if
    /// the first parent was a ghost (Python keeps the parent slot but
    /// replaces its tree data with a `NULL_PARENT_DETAILS` placeholder
    /// so every row still has two tree slots). Entries whose tree-0
    /// and tree-1 minikinds both fall into the "dead pattern" set
    /// `{(a,r), (a,a), (r,r), (r,a)}` — i.e. absent or relocated in
    /// both the current tree and the first parent — are removed from
    /// their dirblock entirely.
    ///
    /// The header is marked modified so the change survives a save.
    /// This invalidates the cached `id_index`; callers must not hold
    /// a reference to the old one across this call.
    pub fn discard_merge_parents(&mut self) {
        if self.parents.is_empty() {
            return;
        }

        let first_parent_is_ghost = self.ghosts.contains(&self.parents[0]);

        let dead_patterns: &[(u8, u8)] = &[(b'a', b'r'), (b'a', b'a'), (b'r', b'r'), (b'r', b'a')];

        for block in self.dirblocks.iter_mut() {
            let mut surviving: Vec<Entry> = Vec::with_capacity(block.entries.len());
            for entry in block.entries.drain(..) {
                let tree0_kind = entry.trees.first().map(|t| t.minikind).unwrap_or(0);
                let tree1_kind = entry.trees.get(1).map(|t| t.minikind).unwrap_or(0);
                let is_dead = dead_patterns
                    .iter()
                    .any(|(a, b)| *a == tree0_kind && *b == tree1_kind);
                if is_dead {
                    continue;
                }
                let mut new_entry = entry;
                if first_parent_is_ghost {
                    // Replace trees beyond index 0 with a single
                    // NULL_PARENT_DETAILS row so every entry still
                    // has exactly two tree slots after the discard.
                    new_entry.trees.truncate(1);
                    new_entry.trees.push(TreeData {
                        minikind: b'a',
                        fingerprint: Vec::new(),
                        size: 0,
                        executable: false,
                        packed_stat: Vec::new(),
                    });
                } else {
                    // Keep only trees 0 and 1.
                    new_entry.trees.truncate(2);
                }
                surviving.push(new_entry);
            }
            block.entries = surviving;
        }

        self.ghosts.clear();
        let first_parent = self.parents[0].clone();
        self.parents = vec![first_parent];
        self.id_index = None;
        self.packed_stat_index = None;
        self.mark_modified(&[], true);
    }

    /// Mark `key` as absent for tree 0, following Python's
    /// `DirState._make_absent`.
    ///
    /// Behaviour:
    /// 1. Scan trees 1.. of the entry at `key`. For each non-absent,
    ///    non-relocated row, remember `key` as still-referenced; for
    ///    each relocated row, remember the relocation target's key
    ///    (same file_id, new dirname/basename).
    /// 2. If `key` is not still-referenced by any remaining tree,
    ///    remove its entry row from the block and drop `key` from the
    ///    id index.
    /// 3. For every remaining-key, set its tree-0 slot to
    ///    `NULL_PARENT_DETAILS`. Assert that the slot isn't already
    ///    absent (mirroring Python's `bad row` assertion).
    /// 4. Mark the dirstate modified.
    ///
    /// Returns `true` when the entry row was removed in step (2),
    /// matching Python's `last_reference` return.
    pub fn make_absent(&mut self, key: &EntryKey) -> Result<bool, MakeAbsentError> {
        // Locate the entry we're making absent.
        let (block_index, block_present) = find_block_index_from_key(&self.dirblocks, key);
        if !block_present {
            return Err(MakeAbsentError::BlockNotFound { key: key.clone() });
        }
        let (entry_index, entry_present) =
            find_entry_index(key, &self.dirblocks[block_index].entries);
        if !entry_present {
            return Err(MakeAbsentError::EntryNotFound { key: key.clone() });
        }

        // Collect remaining references across trees 1..N. Python scans
        // `current_old[1][1:]`, i.e. every tree slot except tree 0.
        let mut remaining_keys: Vec<EntryKey> = Vec::new();
        {
            let entry = &self.dirblocks[block_index].entries[entry_index];
            for tree in entry.trees.iter().skip(1) {
                match tree.minikind {
                    // Python's branches treat 'a' as "not present at any
                    // path" and everything else except 'r' as "still at
                    // the original key". The `elif details[0] == b'r'`
                    // clause inside the first branch is effectively
                    // unreachable in Python because the outer
                    // `if details[0] not in (b'a', b'r')` already excludes
                    // 'r' — we mirror the intended split here.
                    b'a' => {}
                    b'r' => {
                        // Relocated row: fingerprint holds the target
                        // path, file_id stays the same.
                        let (dirname, basename) = split_path_utf8(&tree.fingerprint);
                        remaining_keys.push(EntryKey {
                            dirname: dirname.to_vec(),
                            basename: basename.to_vec(),
                            file_id: key.file_id.clone(),
                        });
                    }
                    _ => {
                        remaining_keys.push(key.clone());
                    }
                }
            }
        }

        // The same `key` can be pushed multiple times when an entry
        // has several parent-tree slots that all happen to be 'f' (or
        // 'd' / 'l' / 't'). Each such slot maps to "still at the
        // original key", so the tree-0 update only needs to happen
        // once per distinct key — Python achieves this implicitly by
        // working through a dict.
        remaining_keys.sort_by(|a, b| {
            a.dirname
                .cmp(&b.dirname)
                .then_with(|| a.basename.cmp(&b.basename))
                .then_with(|| a.file_id.cmp(&b.file_id))
        });
        remaining_keys.dedup();

        let last_reference = !remaining_keys.iter().any(|k| k == key);

        if last_reference {
            // Remove the entry row entirely.
            self.dirblocks[block_index].entries.remove(entry_index);
            if let Some(id_index) = self.id_index.as_mut() {
                let fid = FileId::from(&key.file_id);
                id_index.remove((key.dirname.as_slice(), key.basename.as_slice(), &fid));
            }
        }

        // Update every remaining-key's tree 0 slot to NULL_PARENT_DETAILS.
        for update_key in &remaining_keys {
            let (ub, ub_present) = find_block_index_from_key(&self.dirblocks, update_key);
            if !ub_present {
                return Err(MakeAbsentError::UpdateBlockNotFound {
                    key: update_key.clone(),
                });
            }
            let (ue, ue_present) = find_entry_index(update_key, &self.dirblocks[ub].entries);
            if !ue_present {
                return Err(MakeAbsentError::UpdateEntryNotFound {
                    key: update_key.clone(),
                });
            }
            let tree0 = self.dirblocks[ub].entries[ue]
                .trees
                .first_mut()
                .ok_or_else(|| MakeAbsentError::BadRow {
                    key: update_key.clone(),
                })?;
            if tree0.minikind == b'a' {
                return Err(MakeAbsentError::BadRow {
                    key: update_key.clone(),
                });
            }
            *tree0 = TreeData {
                minikind: b'a',
                fingerprint: Vec::new(),
                size: 0,
                executable: false,
                packed_stat: Vec::new(),
            };
        }

        // Tree-0 mutations invalidate the packed_stat_index.
        self.packed_stat_index = None;
        self.mark_modified(&[], false);
        Ok(last_reference)
    }

    /// Apply a sequence of "adds" to tree 1, mirroring Python's
    /// `DirState._update_basis_apply_adds`. `adds` is a flat list of
    /// per-entry records produced by `update_basis_by_delta`: each
    /// describes a new entry to insert (or, when `real_add` is false,
    /// the add half of a split rename). The caller is responsible for
    /// collecting and translating Python inventory entries into
    /// [`BasisAdd`] records — this function only touches dirblocks.
    ///
    /// Sorts `adds` in-place by `new_path` to match Python's
    /// `adds.sort(key=lambda x: x[1])`. The resulting lexicographic
    /// order ensures every parent dirblock is visited before its
    /// children.
    ///
    /// Invariants that produce an `InconsistentDelta` error — mirroring
    /// Python's `_raise_invalid` — are carried as
    /// [`BasisApplyError::Invalid`] values so the pyo3 layer can wrap
    /// them in the Python `InconsistentDelta` exception. Assertions
    /// about internal state that should never happen (such as
    /// `_find_entry_index` missing a key the linear scan locates) are
    /// reported as [`BasisApplyError::Internal`].
    ///
    /// Side effects:
    /// - may call [`DirState::ensure_block`] to materialise a dirblock
    ///   for a missing parent directory;
    /// - mutates tree-1 slots of existing entries;
    /// - inserts new entries with `[NULL_PARENT_DETAILS, new_details]`;
    /// - converts cross-directory renames to tree-0 relocation rows
    ///   when the new tree-1 entry's tree-0 slot is absent but the
    ///   file_id exists at a different path in tree 0;
    /// - ensures a child dirblock exists for directory-kind adds;
    /// - invalidates `id_index` and `packed_stat_index` caches.
    pub fn update_basis_apply_adds(
        &mut self,
        adds: &mut Vec<BasisAdd>,
    ) -> Result<(), BasisApplyError> {
        // Sort lexographically by new_path so parents are processed
        // before children.
        adds.sort_by(|a, b| a.new_path.cmp(&b.new_path));

        for add in adds.iter() {
            let (dirname_raw, basename_raw) = split_path_utf8(&add.new_path);
            let dirname = dirname_raw.to_vec();
            let basename = basename_raw.to_vec();
            let entry_key = EntryKey {
                dirname: dirname.clone(),
                basename: basename.clone(),
                file_id: add.file_id.clone(),
            };

            let (mut block_index, mut present) =
                find_block_index_from_key(&self.dirblocks, &entry_key);
            if !present {
                // The target dirblock is missing; look up the parent
                // in tree 1 and ensure a child block for `dirname`.
                let (parent_dir_raw, parent_base_raw) = split_path_utf8(&dirname);
                let bei =
                    get_block_entry_index(&self.dirblocks, parent_dir_raw, parent_base_raw, 1);
                if !bei.path_present {
                    return Err(BasisApplyError::Invalid {
                        path: add.new_path.clone(),
                        file_id: add.file_id.clone(),
                        reason: "Unable to find block for this record. Was the parent added?"
                            .to_string(),
                    });
                }
                self.ensure_block(bei.block_index as isize, bei.entry_index as isize, &dirname)
                    .map_err(|e| BasisApplyError::Invalid {
                        path: add.new_path.clone(),
                        file_id: add.file_id.clone(),
                        reason: format!("{:?}", e),
                    })?;
                // ensure_block may have inserted a new block at or
                // before the original `block_index`, shifting us.
                let (new_block_index, new_present) =
                    find_block_index_from_key(&self.dirblocks, &entry_key);
                block_index = new_block_index;
                present = new_present;
                // ensure_block must have created the dirblock for
                // `dirname`; `present` here refers to the dirblock,
                // not the entry inside it.
                debug_assert!(present);
            }
            let _ = present;

            let (entry_index, entry_present) =
                find_entry_index(&entry_key, &self.dirblocks[block_index].entries);

            if add.real_add && add.old_path.is_some() {
                return Err(BasisApplyError::Invalid {
                    path: add.new_path.clone(),
                    file_id: add.file_id.clone(),
                    reason: format!(
                        "considered a real add but still had old_path at {:?}",
                        add.old_path.as_ref().unwrap()
                    ),
                });
            }

            if entry_present {
                // Update the existing entry's tree 1 slot.
                let entry = &mut self.dirblocks[block_index].entries[entry_index];
                let basis_kind = entry.trees.get(1).map(|t| t.minikind).unwrap_or(0);
                match basis_kind {
                    b'a' => {
                        if entry.trees.len() >= 2 {
                            entry.trees[1] = add.new_details.clone();
                        } else {
                            entry.trees.push(add.new_details.clone());
                        }
                    }
                    b'r' => {
                        return Err(BasisApplyError::NotImplemented {
                            reason: "basis entry is a relocation".to_string(),
                        });
                    }
                    _ => {
                        return Err(BasisApplyError::Invalid {
                            path: add.new_path.clone(),
                            file_id: add.file_id.clone(),
                            reason:
                                "An entry was marked as a new add but the basis target already existed"
                                    .to_string(),
                        });
                    }
                }
            } else {
                // The exact key is not present; scan the two
                // neighbouring positions for same-path-different-id
                // conflicts (Python only checks `entry_index - 1`
                // and `entry_index`).
                let block_len = self.dirblocks[block_index].entries.len();
                let start = entry_index.saturating_sub(1);
                let end = entry_index + 1;
                for maybe_index in start..end {
                    if maybe_index >= block_len {
                        continue;
                    }
                    let maybe = &self.dirblocks[block_index].entries[maybe_index];
                    if maybe.key.dirname != dirname || maybe.key.basename != basename {
                        continue;
                    }
                    if maybe.key.file_id == add.file_id {
                        return Err(BasisApplyError::Internal {
                            reason: format!(
                                "find_entry_index did not find a key match but walking the data did, for ({:?}, {:?}, {:?})",
                                dirname, basename, add.file_id
                            ),
                        });
                    }
                    let basis_kind = maybe.trees.get(1).map(|t| t.minikind).unwrap_or(0);
                    if basis_kind != b'a' && basis_kind != b'r' {
                        return Err(BasisApplyError::Invalid {
                            path: add.new_path.clone(),
                            file_id: add.file_id.clone(),
                            reason: format!(
                                "we have an add record for path, but the path is already present with another file_id {:?}",
                                maybe.key.file_id
                            ),
                        });
                    }
                }

                // Insert the new entry with NULL_PARENT_DETAILS for
                // tree 0 and `new_details` for tree 1.
                let new_entry = Entry {
                    key: entry_key.clone(),
                    trees: vec![
                        TreeData {
                            minikind: b'a',
                            fingerprint: Vec::new(),
                            size: 0,
                            executable: false,
                            packed_stat: Vec::new(),
                        },
                        add.new_details.clone(),
                    ],
                };
                self.dirblocks[block_index]
                    .entries
                    .insert(entry_index, new_entry);
            }

            // Cross-tree check: if the (possibly just-inserted) entry's
            // tree 0 slot is absent, look up the file_id in tree 0
            // elsewhere and, if found, rewrite both sides into
            // relocation rows.
            let active_kind = self.dirblocks[block_index].entries[entry_index]
                .trees
                .first()
                .map(|t| t.minikind)
                .unwrap_or(0);

            if active_kind == b'a' {
                // Look up file_id via id_index; collect candidate
                // (block, entry) coordinates before mutating, to
                // keep the borrow checker happy.
                let fid = FileId::from(&add.file_id);
                let candidate_keys = self.get_or_build_id_index().get(&fid);

                let mut relocation: Option<(usize, usize, Vec<u8>)> = None;
                for key_tuple in candidate_keys {
                    let (k_dirname, k_basename, _k_file_id) = key_tuple;
                    let bei = get_block_entry_index(&self.dirblocks, &k_dirname, &k_basename, 0);
                    if !bei.path_present {
                        continue;
                    }
                    let candidate = &self.dirblocks[bei.block_index].entries[bei.entry_index];
                    if candidate.key.file_id != add.file_id {
                        continue;
                    }
                    let real_kind = candidate.trees.first().map(|t| t.minikind).unwrap_or(0);
                    if real_kind == b'a' || real_kind == b'r' {
                        return Err(BasisApplyError::Invalid {
                            path: add.new_path.clone(),
                            file_id: add.file_id.clone(),
                            reason: "We found a tree0 entry that doesnt make sense".to_string(),
                        });
                    }
                    let active_dir = candidate.key.dirname.clone();
                    let active_name = candidate.key.basename.clone();
                    let active_path = if active_dir.is_empty() {
                        active_name.clone()
                    } else {
                        let mut p = active_dir.clone();
                        p.push(b'/');
                        p.extend_from_slice(&active_name);
                        p
                    };
                    relocation = Some((bei.block_index, bei.entry_index, active_path));
                    break;
                }

                if let Some((other_block, other_entry, active_path)) = relocation {
                    // Update the other entry's tree 1 slot to point
                    // at the new path.
                    {
                        let other = &mut self.dirblocks[other_block].entries[other_entry];
                        let new_tree1 = TreeData {
                            minikind: b'r',
                            fingerprint: add.new_path.clone(),
                            size: 0,
                            executable: false,
                            packed_stat: Vec::new(),
                        };
                        if other.trees.len() >= 2 {
                            other.trees[1] = new_tree1;
                        } else {
                            other.trees.push(new_tree1);
                        }
                    }
                    // Update the new entry's tree 0 slot to point at
                    // the other path.
                    {
                        let e = &mut self.dirblocks[block_index].entries[entry_index];
                        e.trees[0] = TreeData {
                            minikind: b'r',
                            fingerprint: active_path,
                            size: 0,
                            executable: false,
                            packed_stat: Vec::new(),
                        };
                    }
                }
            } else if active_kind == b'r' {
                return Err(BasisApplyError::NotImplemented {
                    reason: "active entry is a relocation".to_string(),
                });
            }

            // If the new entry is a directory, ensure a child dirblock
            // for its path exists.
            if add.new_details.minikind == b'd' {
                // Use the (possibly-shifted) block_index + entry_index
                // as the parent coordinates for the child dirblock.
                self.ensure_block(block_index as isize, entry_index as isize, &add.new_path)
                    .map_err(|e| BasisApplyError::Invalid {
                        path: add.new_path.clone(),
                        file_id: add.file_id.clone(),
                        reason: format!("{:?}", e),
                    })?;
            }
        }

        self.id_index = None;
        self.packed_stat_index = None;
        Ok(())
    }

    /// Check that every `(dirname_utf8, file_id)` pair in `parents`
    /// exists in `tree_index` at the given path with the given id
    /// *and* is a directory. Mirrors Python's
    /// `DirState._after_delta_check_parents`.
    ///
    /// Returns [`BasisApplyError::Invalid`] on the first parent that
    /// is missing (`"This parent is not present."`) or not a
    /// directory (`"This parent is not a directory."`).
    pub fn after_delta_check_parents(
        &mut self,
        parents: &[(Vec<u8>, Vec<u8>)],
        tree_index: usize,
    ) -> Result<(), BasisApplyError> {
        for (dirname_utf8, file_id) in parents {
            let (d, b) = split_path_utf8(dirname_utf8);
            let bei = get_block_entry_index(&self.dirblocks, d, b, tree_index);
            if !bei.path_present {
                return Err(BasisApplyError::Invalid {
                    path: dirname_utf8.clone(),
                    file_id: file_id.clone(),
                    reason: "This parent is not present.".to_string(),
                });
            }
            let entry = &self.dirblocks[bei.block_index].entries[bei.entry_index];
            if entry.key.file_id != *file_id {
                return Err(BasisApplyError::Invalid {
                    path: dirname_utf8.clone(),
                    file_id: file_id.clone(),
                    reason: "This parent is not present.".to_string(),
                });
            }
            let kind = entry.trees.get(tree_index).map(|t| t.minikind).unwrap_or(0);
            if kind != b'd' {
                return Err(BasisApplyError::Invalid {
                    path: dirname_utf8.clone(),
                    file_id: file_id.clone(),
                    reason: "This parent is not a directory.".to_string(),
                });
            }
        }
        Ok(())
    }

    /// Verify that none of `new_ids` is already present at a live
    /// entry in `tree_index`. Mirrors Python's
    /// `DirState._check_delta_ids_absent` — used by both
    /// `update_by_delta` and `update_basis_by_delta` to guard against
    /// a delta that resurrects an already-present file id.
    ///
    /// On a conflict, returns [`BasisApplyError::Invalid`] carrying
    /// the first offending path / file id.
    pub fn check_delta_ids_absent(
        &mut self,
        new_ids: &[Vec<u8>],
        tree_index: usize,
    ) -> Result<(), BasisApplyError> {
        if new_ids.is_empty() {
            return Ok(());
        }
        let _ = self.get_or_build_id_index();
        for file_id in new_ids {
            let fid = FileId::from(file_id);
            let candidates = self.id_index.as_ref().unwrap().get(&fid);
            for (dn, bn, _) in candidates {
                let bei = get_block_entry_index(&self.dirblocks, &dn, &bn, tree_index);
                if !bei.path_present {
                    continue;
                }
                let entry = &self.dirblocks[bei.block_index].entries[bei.entry_index];
                if entry.key.file_id != *file_id {
                    continue;
                }
                let mut path = dn.clone();
                if !path.is_empty() {
                    path.push(b'/');
                }
                path.extend_from_slice(&bn);
                return Err(BasisApplyError::Invalid {
                    path,
                    file_id: file_id.clone(),
                    reason: "This file_id is new in the delta but already present in the target"
                        .to_string(),
                });
            }
        }
        Ok(())
    }

    /// Update a single entry in tree 0 — either insert a new row or
    /// replace its tree-0 details. Mirrors Python's
    /// `DirState.update_minimal`.
    ///
    /// # Parameters
    /// - `key`: `(dirname, basename, file_id)` identifying the entry.
    /// - `tree0_details`: replacement data for the tree-0 slot
    ///   (the `new_details` tuple Python builds from minikind,
    ///   fingerprint, size, executable, packed_stat).
    /// - `path_utf8`: `dirname + "/" + basename` without the leading
    ///   slash, or `b""` for the root; used when building relocation
    ///   pointers. Required whenever the method takes the
    ///   cross-reference branch.
    /// - `fullscan`: when true, skip the conflicting-entry check
    ///   that `set_state_from_inventory` disables for bulk loads.
    ///
    /// Returns `Ok(())` on success, or
    /// [`BasisApplyError::Invalid`] / [`BasisApplyError::Internal`]
    /// for user-visible delta conflicts and internal invariant
    /// violations (matching Python's `_raise_invalid` /
    /// `AssertionError` / "no path").
    pub fn update_minimal(
        &mut self,
        key: EntryKey,
        tree0_details: TreeData,
        path_utf8: Option<&[u8]>,
        fullscan: bool,
    ) -> Result<(), BasisApplyError> {
        // Ensure the block for `key.dirname` exists. Python's
        // `_find_block` performs a `_find_block_index_from_key`
        // lookup then — when the block is missing and the caller
        // does not pass `add_if_missing=True` — verifies the parent
        // directory is versioned in tree 0, raising
        // `NotVersionedError` otherwise.
        let (_block_index, block_present) = find_block_index_from_key(&self.dirblocks, &key);
        if !block_present {
            // Python's parent-check: osutils.split(key.dirname) and
            // require the result to be a present path in tree 0.
            let (parent_dir, parent_base) = split_path_utf8(&key.dirname);
            let parent_bei = get_block_entry_index(&self.dirblocks, parent_dir, parent_base, 0);
            if !parent_bei.path_present {
                let mut path = key.dirname.clone();
                if !path.is_empty() {
                    path.push(b'/');
                }
                path.extend_from_slice(&key.basename);
                return Err(BasisApplyError::NotVersioned { path });
            }
            self.ensure_block(
                parent_bei.block_index as isize,
                parent_bei.entry_index as isize,
                &key.dirname,
            )
            .map_err(|e| BasisApplyError::Internal {
                reason: format!("ensure_block failed: {:?}", e),
            })?;
        }
        let (block_index, _) = find_block_index_from_key(&self.dirblocks, &key);

        // Find the insertion point within the block.
        let (mut entry_index, present) =
            find_entry_index(&key, &self.dirblocks[block_index].entries);

        // Pre-populate the id_index cache once.
        let _ = self.get_or_build_id_index();

        if !present {
            // Non-fullscan conflict check: walk forward from the
            // basename-only match position and ensure no existing
            // entry occupies the same (dirname, basename) with a
            // live tree-0 row.
            if !fullscan {
                let prefix_key = EntryKey {
                    dirname: key.dirname.clone(),
                    basename: key.basename.clone(),
                    file_id: Vec::new(),
                };
                let (mut low_index, _) =
                    find_entry_index(&prefix_key, &self.dirblocks[block_index].entries);
                while low_index < self.dirblocks[block_index].entries.len() {
                    let candidate = &self.dirblocks[block_index].entries[low_index];
                    if candidate.key.dirname == key.dirname
                        && candidate.key.basename == key.basename
                    {
                        let t0 = candidate.trees.first().map(|t| t.minikind).unwrap_or(0);
                        if t0 != b'a' && t0 != b'r' {
                            let mut path = key.dirname.clone();
                            if !path.is_empty() {
                                path.push(b'/');
                            }
                            path.extend_from_slice(&key.basename);
                            return Err(BasisApplyError::Invalid {
                                path,
                                file_id: key.file_id.clone(),
                                reason: format!(
                                    "Attempt to add item at path already occupied by id {:?}",
                                    candidate.key.file_id
                                ),
                            });
                        }
                        low_index += 1;
                    } else {
                        break;
                    }
                }
            }

            // Existing keys for this file_id across the id_index.
            let fid = FileId::from(&key.file_id);
            let existing_keys: Vec<(Vec<u8>, Vec<u8>, FileId)> =
                self.id_index.as_ref().unwrap().get(&fid);

            let new_trees: Vec<TreeData> = if existing_keys.is_empty() {
                // Simple case: a new file id, no parents to link.
                let mut trees = vec![tree0_details.clone()];
                for _ in 0..self.num_present_parents() {
                    trees.push(TreeData {
                        minikind: b'a',
                        fingerprint: Vec::new(),
                        size: 0,
                        executable: false,
                        packed_stat: Vec::new(),
                    });
                }
                trees
            } else {
                // Cross-reference case: rewrite other rows to point
                // at this new entry, then assemble parent details
                // by cloning from existing rows or synthesising
                // relocation pointers.
                let path_bytes = path_utf8.ok_or_else(|| BasisApplyError::Internal {
                    reason: "update_minimal: no path".to_string(),
                })?;

                // Convert each existing key's tree-0 slot to a
                // relocation pointer to `path_utf8`. Python also
                // drops entries that become entirely dead
                // afterwards via `_maybe_remove_row`.
                let mut removed_before_target = 0usize;
                let keys_snapshot: Vec<(Vec<u8>, Vec<u8>, FileId)> = existing_keys.clone();
                for other_tuple in &keys_snapshot {
                    let (odirname, obasename, _ofid) = other_tuple;
                    let other_key = EntryKey {
                        dirname: odirname.clone(),
                        basename: obasename.clone(),
                        file_id: key.file_id.clone(),
                    };
                    let (ob_idx, ob_present) =
                        find_block_index_from_key(&self.dirblocks, &other_key);
                    if !ob_present {
                        return Err(BasisApplyError::Internal {
                            reason: format!("could not find block for {:?}", other_key),
                        });
                    }
                    let (oe_idx, oe_present) =
                        find_entry_index(&other_key, &self.dirblocks[ob_idx].entries);
                    if !oe_present {
                        return Err(BasisApplyError::Internal {
                            reason: format!(
                                "update_minimal: could not find other entry for {:?}",
                                other_key
                            ),
                        });
                    }

                    self.dirblocks[ob_idx].entries[oe_idx].trees[0] = TreeData {
                        minikind: b'r',
                        fingerprint: path_bytes.to_vec(),
                        size: 0,
                        executable: false,
                        packed_stat: Vec::new(),
                    };

                    let all_dead = self.dirblocks[ob_idx].entries[oe_idx]
                        .trees
                        .iter()
                        .all(|t| t.minikind == b'a' || t.minikind == b'r');
                    if all_dead {
                        let removed_key = self.dirblocks[ob_idx].entries[oe_idx].key.clone();
                        self.dirblocks[ob_idx].entries.remove(oe_idx);
                        if let Some(idx) = self.id_index.as_mut() {
                            let rfid = FileId::from(&removed_key.file_id);
                            idx.remove((
                                removed_key.dirname.as_slice(),
                                removed_key.basename.as_slice(),
                                &rfid,
                            ));
                        }
                        if ob_idx == block_index && oe_idx < entry_index {
                            removed_before_target += 1;
                        }
                    }
                }
                entry_index = entry_index.saturating_sub(removed_before_target);

                let mut trees = vec![tree0_details.clone()];
                let num_parents = self.num_present_parents();
                if num_parents > 0 {
                    // Python grabs `list(existing_keys)[0]` before
                    // the removals, so the first key in the
                    // snapshot is the authoritative source for
                    // parent-tree details.
                    let (odirname, obasename, _ofid) = keys_snapshot[0].clone();
                    let other_key = EntryKey {
                        dirname: odirname.clone(),
                        basename: obasename.clone(),
                        file_id: key.file_id.clone(),
                    };
                    let (ub_idx, ub_present) =
                        find_block_index_from_key(&self.dirblocks, &other_key);
                    if !ub_present {
                        return Err(BasisApplyError::Internal {
                            reason: format!("could not find block for {:?}", other_key),
                        });
                    }
                    let (ue_idx, ue_present) =
                        find_entry_index(&other_key, &self.dirblocks[ub_idx].entries);
                    if !ue_present {
                        return Err(BasisApplyError::Internal {
                            reason: format!(
                                "update_minimal: could not find entry for {:?}",
                                other_key
                            ),
                        });
                    }
                    for lookup_index in 1..=num_parents {
                        let source_tree = self.dirblocks[ub_idx].entries[ue_idx]
                            .trees
                            .get(lookup_index)
                            .cloned();
                        match source_tree {
                            Some(ref t) if t.minikind == b'a' || t.minikind == b'r' => {
                                trees.push(t.clone());
                            }
                            Some(_) => {
                                let mut ptr = odirname.clone();
                                if !ptr.is_empty() {
                                    ptr.push(b'/');
                                }
                                ptr.extend_from_slice(&obasename);
                                trees.push(TreeData {
                                    minikind: b'r',
                                    fingerprint: ptr,
                                    size: 0,
                                    executable: false,
                                    packed_stat: Vec::new(),
                                });
                            }
                            None => {
                                trees.push(TreeData {
                                    minikind: b'a',
                                    fingerprint: Vec::new(),
                                    size: 0,
                                    executable: false,
                                    packed_stat: Vec::new(),
                                });
                            }
                        }
                    }
                }
                trees
            };

            // Insert the new entry at `entry_index`, then extend
            // the id_index.
            let new_entry = Entry {
                key: key.clone(),
                trees: new_trees,
            };
            self.dirblocks[block_index]
                .entries
                .insert(entry_index, new_entry);
            if let Some(idx) = self.id_index.as_mut() {
                idx.add((
                    key.dirname.as_slice(),
                    key.basename.as_slice(),
                    &FileId::from(&key.file_id),
                ));
            }
        } else {
            // Update the tree-0 slot of the existing entry in place.
            self.dirblocks[block_index].entries[entry_index].trees[0] = tree0_details.clone();

            let path_bytes = path_utf8.ok_or_else(|| BasisApplyError::Internal {
                reason: "update_minimal: no path".to_string(),
            })?;

            // Cross-reference maintenance: every other entry that
            // shares this file_id (as recorded in the id_index)
            // must be turned into a relocation pointer to
            // `path_utf8`.
            let fid = FileId::from(&key.file_id);
            let existing_keys: Vec<(Vec<u8>, Vec<u8>, FileId)> =
                self.id_index.as_ref().unwrap().get(&fid);
            if !existing_keys
                .iter()
                .any(|(d, b, _)| d == &key.dirname && b == &key.basename)
            {
                return Err(BasisApplyError::Internal {
                    reason: format!(
                        "We found the entry in the blocks, but the key is not in the id_index. key: {:?}, existing_keys: {:?}",
                        key, existing_keys
                    ),
                });
            }

            for (odirname, obasename, _ofid) in &existing_keys {
                if odirname == &key.dirname && obasename == &key.basename {
                    continue;
                }
                let other_key = EntryKey {
                    dirname: odirname.clone(),
                    basename: obasename.clone(),
                    file_id: key.file_id.clone(),
                };
                let (ob_idx, ob_present) = find_block_index_from_key(&self.dirblocks, &other_key);
                if !ob_present {
                    return Err(BasisApplyError::Internal {
                        reason: format!("not present: {:?}", other_key),
                    });
                }
                let (oe_idx, oe_present) =
                    find_entry_index(&other_key, &self.dirblocks[ob_idx].entries);
                if !oe_present {
                    return Err(BasisApplyError::Internal {
                        reason: format!("not present: {:?}", other_key),
                    });
                }
                self.dirblocks[ob_idx].entries[oe_idx].trees[0] = TreeData {
                    minikind: b'r',
                    fingerprint: path_bytes.to_vec(),
                    size: 0,
                    executable: false,
                    packed_stat: Vec::new(),
                };
            }
        }

        // If the new entry is a directory, ensure a child block
        // exists for its path.
        if tree0_details.minikind == b'd' {
            let mut subdir_name = key.dirname.clone();
            if !subdir_name.is_empty() {
                subdir_name.push(b'/');
            }
            subdir_name.extend_from_slice(&key.basename);
            let subdir_key = EntryKey {
                dirname: subdir_name,
                basename: Vec::new(),
                file_id: Vec::new(),
            };
            let (sb_idx, sb_present) = find_block_index_from_key(&self.dirblocks, &subdir_key);
            if !sb_present {
                self.dirblocks.insert(
                    sb_idx,
                    Dirblock {
                        dirname: subdir_key.dirname.clone(),
                        entries: Vec::new(),
                    },
                );
            }
        }

        self.mark_modified(&[], false);
        self.packed_stat_index = None;
        Ok(())
    }

    /// Add a new tracked entry. Mirrors Python's `DirState.add` after
    /// path normalisation: the caller is responsible for handing in
    /// `utf8path` with its `dirname`/`basename` split already done, and
    /// for supplying the packed_stat bytes (use `pack_stat` on the
    /// `os.lstat` result, or `None` to substitute `NULLSTAT`).
    ///
    /// `kind` is one of `"file"`, `"directory"`, `"symlink"`, or
    /// `"tree-reference"` — anything else yields `AddError::UnknownKind`.
    ///
    /// The method performs the same duplicate-id detection Python does:
    /// if `file_id` is already tracked at a live (non-absent) path it
    /// returns `AddError::DuplicateFileId`. If the file_id existed
    /// previously at a different path marked absent, that old row is
    /// rewritten as a relocation pointer to the new path via
    /// [`DirState::update_minimal`], matching Python's `rename_from`
    /// fix-up. In that case the resulting entry's parent-tree slot 0
    /// stores a relocation row pointing back at the old path, so
    /// history-aware tooling can still resolve the id.
    ///
    /// The target dirblock is created (`ensure_block`) if missing, and a
    /// child block is ensured when the new entry is a directory — both
    /// matching Python's post-insert `_ensure_block` call.
    #[allow(clippy::too_many_arguments)]
    pub fn add(
        &mut self,
        utf8path: &[u8],
        dirname: &[u8],
        basename: &[u8],
        file_id: &[u8],
        kind: &str,
        size: u64,
        packed_stat: &[u8],
        fingerprint: &[u8],
    ) -> Result<(), AddError> {
        // Pre-flight: does this file_id already live somewhere?
        // Python calls `_get_entry(0, fileid_utf8=file_id,
        // include_deleted=True)` and branches on the result.
        self.get_or_build_id_index();
        let fid = FileId::from(&file_id.to_vec());
        let candidates = self.id_index.as_ref().unwrap().get(&fid);

        let mut rename_from: Option<(Vec<u8>, Vec<u8>)> = None;
        for (cand_dir, cand_base, _cfid) in candidates {
            let cand_key = EntryKey {
                dirname: cand_dir.clone(),
                basename: cand_base.clone(),
                file_id: file_id.to_vec(),
            };
            let (cb_idx, cb_present) = find_block_index_from_key(&self.dirblocks, &cand_key);
            if !cb_present {
                continue;
            }
            let (ce_idx, ce_present) = find_entry_index(&cand_key, &self.dirblocks[cb_idx].entries);
            if !ce_present {
                continue;
            }
            let entry = &self.dirblocks[cb_idx].entries[ce_idx];
            let tree0_kind = entry.trees.first().map(|t| t.minikind).unwrap_or(0);
            match tree0_kind {
                b'a' => {
                    if cand_dir.as_slice() != dirname || cand_base.as_slice() != basename {
                        rename_from = Some((cand_dir.clone(), cand_base.clone()));
                    }
                    break;
                }
                b'r' => {
                    // The candidate row is a relocation pointer; keep
                    // searching — the real home is elsewhere.
                    continue;
                }
                other => {
                    let kind_char = other as char;
                    let path = if cand_dir.is_empty() {
                        cand_base.clone()
                    } else {
                        let mut p = cand_dir.clone();
                        p.push(b'/');
                        p.extend_from_slice(&cand_base);
                        p
                    };
                    let path_str = String::from_utf8_lossy(&path);
                    let kind_str = match other {
                        b'f' => "file",
                        b'd' => "directory",
                        b'l' => "symlink",
                        b't' => "tree-reference",
                        _ => {
                            return Err(AddError::Internal {
                                reason: format!(
                                    "unexpected minikind {:?} in id_index row",
                                    kind_char
                                ),
                            })
                        }
                    };
                    return Err(AddError::DuplicateFileId {
                        file_id: file_id.to_vec(),
                        info: format!("{}:{}", kind_str, path_str),
                    });
                }
            }
        }

        // Rename fix-up: the id used to live at rename_from but was
        // marked absent. Python calls update_minimal to turn the old
        // row into a relocation pointer to the new path.
        if let Some((old_dir, old_base)) = rename_from.as_ref() {
            let old_key = EntryKey {
                dirname: old_dir.clone(),
                basename: old_base.clone(),
                file_id: file_id.to_vec(),
            };
            let reloc_details = TreeData {
                minikind: b'r',
                fingerprint: utf8path.to_vec(),
                size: 0,
                executable: false,
                packed_stat: Vec::new(),
            };
            self.update_minimal(old_key, reloc_details, Some(b""), false)
                .map_err(|e| AddError::Internal {
                    reason: format!("rename-from update_minimal: {}", e),
                })?;
        }

        // Find the block that should receive the new entry.
        let first_key = EntryKey {
            dirname: dirname.to_vec(),
            basename: basename.to_vec(),
            file_id: Vec::new(),
        };
        let (mut block_index, block_present) =
            find_block_index_from_key(&self.dirblocks, &first_key);
        if block_present {
            // A block exists; walk entries at this basename and ensure
            // none is live in tree 0.
            let (mut entry_index, _) =
                find_entry_index(&first_key, &self.dirblocks[block_index].entries);
            let block = &self.dirblocks[block_index].entries;
            while entry_index < block.len()
                && block[entry_index].key.dirname == dirname
                && block[entry_index].key.basename == basename
            {
                let t0 = block[entry_index]
                    .trees
                    .first()
                    .map(|t| t.minikind)
                    .unwrap_or(0);
                if t0 != b'a' && t0 != b'r' {
                    let mut path = dirname.to_vec();
                    if !path.is_empty() {
                        path.push(b'/');
                    }
                    path.extend_from_slice(basename);
                    return Err(AddError::AlreadyAdded { path });
                }
                entry_index += 1;
            }
        } else {
            // Python: look up the parent directory; if absent, raise
            // NotVersionedError. Otherwise ensure_block.
            let (parent_dir, parent_base) = split_path_utf8(dirname);
            let pbei = get_block_entry_index(&self.dirblocks, parent_dir, parent_base, 0);
            if !pbei.path_present {
                let mut path = dirname.to_vec();
                if !path.is_empty() {
                    path.push(b'/');
                }
                path.extend_from_slice(basename);
                return Err(AddError::NotVersioned { path });
            }
            self.ensure_block(
                pbei.block_index as isize,
                pbei.entry_index as isize,
                dirname,
            )
            .map_err(|e| AddError::Internal {
                reason: format!("ensure_block failed: {:?}", e),
            })?;
            let (new_block_index, _) = find_block_index_from_key(&self.dirblocks, &first_key);
            block_index = new_block_index;
        }

        // Build the tree-0 details. Python treats directories specially:
        // their fingerprint and size are always empty / zero, even if
        // the caller passes a value.
        let minikind_byte = match kind {
            "file" => b'f',
            "directory" => b'd',
            "symlink" => b'l',
            "tree-reference" => b't',
            other => {
                return Err(AddError::UnknownKind {
                    kind: other.to_string(),
                })
            }
        };
        let tree0 = match kind {
            "directory" => TreeData {
                minikind: minikind_byte,
                fingerprint: Vec::new(),
                size: 0,
                executable: false,
                packed_stat: packed_stat.to_vec(),
            },
            "tree-reference" => TreeData {
                minikind: minikind_byte,
                fingerprint: fingerprint.to_vec(),
                size: 0,
                executable: false,
                packed_stat: packed_stat.to_vec(),
            },
            _ => TreeData {
                minikind: minikind_byte,
                fingerprint: fingerprint.to_vec(),
                size,
                executable: false,
                packed_stat: packed_stat.to_vec(),
            },
        };

        // Empty parent info: NULL_PARENT_DETAILS per present parent.
        let num_present = self.num_present_parents();
        let mut parent_info: Vec<TreeData> = (0..num_present)
            .map(|_| TreeData {
                minikind: b'a',
                fingerprint: Vec::new(),
                size: 0,
                executable: false,
                packed_stat: Vec::new(),
            })
            .collect();
        if let Some((old_dir, old_base)) = rename_from {
            // Replace parent_info[0] with a relocation pointer to the
            // old path. Matches Python's
            // `parent_info[0] = (b"r", old_path_utf8, 0, False, b"")`.
            let old_path_utf8 = if old_dir.is_empty() {
                old_base
            } else {
                let mut p = old_dir.clone();
                p.push(b'/');
                p.extend_from_slice(&old_base);
                p
            };
            if let Some(p0) = parent_info.get_mut(0) {
                *p0 = TreeData {
                    minikind: b'r',
                    fingerprint: old_path_utf8,
                    size: 0,
                    executable: false,
                    packed_stat: Vec::new(),
                };
            }
        }

        let mut trees = vec![tree0];
        trees.extend(parent_info);

        let entry_key = EntryKey {
            dirname: dirname.to_vec(),
            basename: basename.to_vec(),
            file_id: file_id.to_vec(),
        };
        let (entry_index, present) =
            find_entry_index(&entry_key, &self.dirblocks[block_index].entries);
        if !present {
            self.dirblocks[block_index].entries.insert(
                entry_index,
                Entry {
                    key: entry_key.clone(),
                    trees,
                },
            );
            if let Some(idx) = self.id_index.as_mut() {
                idx.add((dirname, basename, &FileId::from(&file_id.to_vec())));
            }
        } else {
            let existing = &mut self.dirblocks[block_index].entries[entry_index];
            let current_t0 = existing.trees.first().map(|t| t.minikind).unwrap_or(0);
            if current_t0 != b'a' {
                return Err(AddError::AlreadyAddedAssertion {
                    basename: basename.to_vec(),
                    file_id: file_id.to_vec(),
                });
            }
            // Overwrite tree-0 only; leave parent slots alone.
            existing.trees[0] = trees.into_iter().next().unwrap();
        }

        if kind == "directory" {
            // Python: _ensure_block(block_index, entry_index, utf8path).
            // We need to pass coordinates of the entry we just inserted
            // / overwrote. Re-find it since insertion may have shifted.
            let (eb, _) = find_block_index_from_key(&self.dirblocks, &entry_key);
            let (ei, _) = find_entry_index(&entry_key, &self.dirblocks[eb].entries);
            self.ensure_block(eb as isize, ei as isize, utf8path)
                .map_err(|e| AddError::Internal {
                    reason: format!("child ensure_block failed: {:?}", e),
                })?;
        }

        self.mark_modified(&[], false);
        Ok(())
    }

    /// Change the file id of the root path. Mirrors Python's
    /// `DirState.set_path_id`, which only supports `path=b""`.
    ///
    /// Python's original implementation called `_make_absent` on the
    /// old root entry (which mutated the shared tree-0 slot to
    /// NULL_PARENT_DETAILS when parent trees kept the entry alive)
    /// and then called `update_minimal` with
    /// `packed_stat=entry[1][0][4]`. The packed_stat observed by
    /// `update_minimal` therefore depended on whether the mutation
    /// had reset it: empty bytes when parents held the entry alive,
    /// the original stat otherwise. This port reproduces that rule
    /// explicitly.
    pub fn set_path_id(&mut self, path: &[u8], new_id: &[u8]) -> Result<(), SetPathIdError> {
        if !path.is_empty() {
            return Err(SetPathIdError::NonRootPath);
        }

        // Locate the current root entry in tree 0. Python's
        // `_get_entry(0, path_utf8=b"")` lookup.
        let bei = get_block_entry_index(&self.dirblocks, b"", b"", 0);
        if !bei.path_present {
            // Root entry must exist; if it does not, the dirstate is
            // malformed — report it rather than silently no-op.
            return Err(SetPathIdError::Internal {
                reason: "root entry missing".to_string(),
            });
        }
        let entry = &self.dirblocks[bei.block_index].entries[bei.entry_index];
        if entry.key.file_id == new_id {
            return Ok(());
        }

        // Capture the data we need before make_absent mutates state.
        let old_key = entry.key.clone();
        let original_packed_stat = entry
            .trees
            .first()
            .map(|t| t.packed_stat.clone())
            .unwrap_or_default();
        // If any parent tree kept the entry alive (minikind not in
        // {a, r}), the legacy code's make_absent-in-place mutation
        // reset packed_stat to empty bytes; update_minimal then stored
        // NULLSTAT in the new row. Preserve that observable behaviour.
        let parents_keep_entry = entry
            .trees
            .iter()
            .skip(1)
            .any(|t| t.minikind != b'a' && t.minikind != b'r');
        let packed_stat = if parents_keep_entry {
            Vec::new()
        } else {
            original_packed_stat
        };

        self.make_absent(&old_key)
            .map_err(|e| SetPathIdError::Internal {
                reason: format!("make_absent: {}", e),
            })?;

        let new_key = EntryKey {
            dirname: Vec::new(),
            basename: Vec::new(),
            file_id: new_id.to_vec(),
        };
        let tree0 = TreeData {
            minikind: b'd',
            fingerprint: Vec::new(),
            size: 0,
            executable: false,
            packed_stat,
        };
        self.update_minimal(new_key, tree0, Some(b""), false)
            .map_err(|e| SetPathIdError::Internal {
                reason: format!("update_minimal: {}", e),
            })?;

        self.mark_modified(&[], false);
        Ok(())
    }

    /// Apply a sequence of "removals" to tree 0, mirroring Python's
    /// `DirState._apply_removals`. Each record is a
    /// `(file_id, path)` tuple; the method sorts them in reverse
    /// path order (so deeper paths are removed first), locates the
    /// entry in tree 0, asserts it is present with the expected
    /// file_id, and calls [`DirState::make_absent`].
    ///
    /// After each removal the directory block that used to hold the
    /// removed entry's children is scanned for live tree-0 rows —
    /// any surviving row flags an inconsistent delta, matching
    /// Python's "file id was deleted but its children were not
    /// deleted" guard.
    pub fn apply_removals(
        &mut self,
        removals: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<(), BasisApplyError> {
        // Sort by path in reverse so nested children come out before
        // their parents — matches Python's
        // `sorted(removals, reverse=True, key=operator.itemgetter(1))`.
        let mut sorted: Vec<&(Vec<u8>, Vec<u8>)> = removals.iter().collect();
        sorted.sort_by(|a, b| b.1.cmp(&a.1));

        for (file_id, path) in sorted {
            let (dirname, basename) = split_path_utf8(path);
            let bei = get_block_entry_index(&self.dirblocks, dirname, basename, 0);
            if !bei.path_present {
                return Err(BasisApplyError::Invalid {
                    path: path.clone(),
                    file_id: file_id.clone(),
                    reason: "Wrong path for old path.".to_string(),
                });
            }
            let entry_file_id = self.dirblocks[bei.block_index].entries[bei.entry_index]
                .key
                .file_id
                .clone();
            if entry_file_id != *file_id {
                return Err(BasisApplyError::Invalid {
                    path: path.clone(),
                    file_id: file_id.clone(),
                    reason: format!(
                        "Attempt to remove path has wrong id - found {:?}.",
                        entry_file_id
                    ),
                });
            }
            let target_key = self.dirblocks[bei.block_index].entries[bei.entry_index]
                .key
                .clone();
            self.make_absent(&target_key)
                .map_err(|e| BasisApplyError::Invalid {
                    path: path.clone(),
                    file_id: file_id.clone(),
                    reason: format!("{:?}", e),
                })?;

            // After-removal integrity check: if a dirblock for
            // `path` still exists in tree 0, none of its rows may
            // be live.
            let child_bei = get_block_entry_index(&self.dirblocks, path, b"", 0);
            if child_bei.dir_present {
                let block = &self.dirblocks[child_bei.block_index];
                for child in &block.entries {
                    let t0 = child.trees.first().map(|t| t.minikind).unwrap_or(0);
                    if t0 != b'a' && t0 != b'r' {
                        return Err(BasisApplyError::Invalid {
                            path: path.clone(),
                            file_id: file_id.clone(),
                            reason: "The file id was deleted but its children were not deleted."
                                .to_string(),
                        });
                    }
                }
            }
        }
        Ok(())
    }

    /// Mirrors Python's `DirState._validate`. Walks the dirblocks
    /// and cross-references tree state invariants: root-block
    /// sentinel, dirblock ordering, per-block entry ordering,
    /// per-tree id→path consistency (absent / relocation /
    /// file-or-dir rules), parent-entry presence, and id_index
    /// back-references when the cache is populated.
    ///
    /// Returns `Ok(())` when all invariants hold, or a
    /// [`ValidateError`] describing the first violation — which the
    /// pyo3 layer turns into `AssertionError` to match Python.
    pub fn validate(&self) -> Result<(), ValidateError> {
        if !self.dirblocks.is_empty() && !self.dirblocks[0].dirname.is_empty() {
            return Err(ValidateError(
                "dirblocks don't start with root block".into(),
            ));
        }
        if self.dirblocks.len() > 1 && !self.dirblocks[1].dirname.is_empty() {
            return Err(ValidateError("dirblocks missing root directory".into()));
        }
        // dirblock names after the root pair must be in sorted
        // component order. Python does
        // `[d[0].split(b"/") for d in self._dirblocks[1:]]`.
        let dir_names: Vec<Vec<&[u8]>> = self
            .dirblocks
            .iter()
            .skip(1)
            .map(|d| d.dirname.split(|&b| b == b'/').collect())
            .collect();
        let mut sorted_dir_names = dir_names.clone();
        sorted_dir_names.sort();
        if dir_names != sorted_dir_names {
            return Err(ValidateError("dir names are not in sorted order".into()));
        }
        for dirblock in &self.dirblocks {
            for entry in &dirblock.entries {
                if dirblock.dirname != entry.key.dirname {
                    return Err(ValidateError(format!(
                        "entry key dirname {} doesn't match block directory name {}",
                        String::from_utf8_lossy(&entry.key.dirname),
                        String::from_utf8_lossy(&dirblock.dirname)
                    )));
                }
            }
            let key_tuple =
                |k: &EntryKey| (k.dirname.clone(), k.basename.clone(), k.file_id.clone());
            if !dirblock
                .entries
                .windows(2)
                .all(|w| key_tuple(&w[0].key) <= key_tuple(&w[1].key))
            {
                return Err(ValidateError(format!(
                    "dirblock for {:?} is not sorted",
                    dirblock.dirname
                )));
            }
        }

        // Per-tree id→path map. Each slot is
        // Option<(previous_path, previous_loc)> matching Python's
        // tuple: previous_path == None means "seen as absent",
        // otherwise it's the canonical path (for a live row) or the
        // relocation target (for a relocation row).
        type IdMap = std::collections::HashMap<Vec<u8>, (Option<Vec<u8>>, Vec<u8>)>;
        let tree_count = 1 + self.num_present_parents();
        let mut id_path_maps: Vec<IdMap> = (0..tree_count).map(|_| IdMap::new()).collect();
        for entry in self.iter_entries() {
            let file_id = &entry.key.file_id;
            let mut this_path = entry.key.dirname.clone();
            if !this_path.is_empty() {
                this_path.push(b'/');
            }
            this_path.extend_from_slice(&entry.key.basename);
            if entry.trees.len() != tree_count {
                return Err(ValidateError(format!(
                    "wrong number of entry details for {:?}, expected {}",
                    entry.key, tree_count
                )));
            }
            let mut absent_positions = 0usize;
            for (tree_index, tree_state) in entry.trees.iter().enumerate() {
                let minikind = tree_state.minikind;
                if minikind == b'a' || minikind == b'r' {
                    absent_positions += 1;
                }
                if let Some((previous_path, previous_loc)) =
                    id_path_maps[tree_index].get(file_id.as_slice()).cloned()
                {
                    if minikind == b'a' {
                        if previous_path.is_some() {
                            return Err(ValidateError(format!(
                                "file {} absent but previously present",
                                String::from_utf8_lossy(file_id)
                            )));
                        }
                    } else if minikind == b'r' {
                        let target = tree_state.fingerprint.clone();
                        if previous_path.as_deref() != Some(target.as_slice()) {
                            return Err(ValidateError(format!(
                                "relocation {} inconsistent with previous {:?}",
                                String::from_utf8_lossy(file_id),
                                previous_path.as_deref().map(String::from_utf8_lossy)
                            )));
                        }
                    } else {
                        if previous_path.as_deref() != Some(this_path.as_slice()) {
                            return Err(ValidateError(format!(
                                "entry {:?} inconsistent with previous path {:?} at {:?}",
                                entry.key, previous_path, previous_loc
                            )));
                        }
                        self.check_valid_parent(tree_index, &entry.key, &this_path)?;
                    }
                } else {
                    match minikind {
                        b'a' => {
                            id_path_maps[tree_index]
                                .insert(file_id.to_vec(), (None, this_path.clone()));
                        }
                        b'r' => {
                            id_path_maps[tree_index].insert(
                                file_id.to_vec(),
                                (Some(tree_state.fingerprint.clone()), this_path.clone()),
                            );
                        }
                        _ => {
                            id_path_maps[tree_index].insert(
                                file_id.to_vec(),
                                (Some(this_path.clone()), this_path.clone()),
                            );
                            self.check_valid_parent(tree_index, &entry.key, &this_path)?;
                        }
                    }
                }
            }
            if absent_positions == tree_count {
                return Err(ValidateError(format!(
                    "entry {:?} has no data for any tree",
                    entry.key
                )));
            }
        }

        // id_index back-reference check, if the cache is built.
        if let Some(id_index) = &self.id_index {
            for (dirname, basename, file_id) in id_index.iter_all() {
                let lookup_key = EntryKey {
                    dirname: dirname.clone(),
                    basename: basename.clone(),
                    file_id: file_id.as_bytes().to_vec(),
                };
                let (block_index, present) =
                    find_block_index_from_key(&self.dirblocks, &lookup_key);
                if !present {
                    return Err(ValidateError(format!(
                        "missing block for entry key: {:?}",
                        lookup_key
                    )));
                }
                let (_, entry_present) =
                    find_entry_index(&lookup_key, &self.dirblocks[block_index].entries);
                if !entry_present {
                    return Err(ValidateError(format!(
                        "missing entry for key: {:?}",
                        lookup_key
                    )));
                }
            }
        }
        Ok(())
    }

    /// Helper for [`DirState::validate`] — mirrors Python's nested
    /// `check_valid_parent`. Verifies the containing directory
    /// entry exists and is marked as a directory in `tree_index`.
    /// The root row (empty dirname + empty basename) has no parent.
    fn check_valid_parent(
        &self,
        tree_index: usize,
        key: &EntryKey,
        this_path: &[u8],
    ) -> Result<(), ValidateError> {
        if key.dirname.is_empty() && key.basename.is_empty() {
            return Ok(());
        }
        let parent = self
            .get_entry_by_path(tree_index, &key.dirname)
            .ok_or_else(|| {
                ValidateError(format!(
                    "no parent entry for {:?} in tree {}",
                    this_path, tree_index
                ))
            })?;
        let parent_minikind = parent
            .trees
            .get(tree_index)
            .map(|t| t.minikind)
            .unwrap_or(0);
        if parent_minikind != b'd' {
            return Err(ValidateError(format!(
                "parent entry for {:?} is not a directory",
                this_path
            )));
        }
        Ok(())
    }

    /// Rebase the basis tree onto `new_revid`. Mirrors Python's
    /// `DirState.update_basis_by_delta` — the sibling of
    /// [`DirState::update_by_delta`] that rebases the basis tree.
    ///
    /// This encapsulates the full Python entrypoint:
    ///   1. `discard_merge_parents()` to drop all parents past the first.
    ///   2. Ghost-check: returns [`BasisApplyError::NotImplemented`]
    ///      when any ghost parent remains, matching Python's
    ///      `NotImplementedError`.
    ///   3. When the dirstate has no parents, extend every entry's
    ///      tree list with a `NULL_PARENT_DETAILS` row and append
    ///      `new_revid` to `parents`.
    ///   4. Replace `parents[0]` with `new_revid`.
    ///   5. Apply the pre-flattened, pre-sorted delta.
    ///   6. Mark modified and clear id_index.
    pub fn update_basis_by_delta(
        &mut self,
        entries: Vec<FlatBasisDeltaEntry>,
        new_revid: Vec<u8>,
    ) -> Result<(), BasisApplyError> {
        self.discard_merge_parents();
        if !self.ghosts.is_empty() {
            return Err(BasisApplyError::NotImplemented {
                reason: "update_basis_by_delta with ghost parents".to_string(),
            });
        }
        if self.parents.is_empty() {
            self.bootstrap_new_parent_slot();
            self.parents.push(new_revid.clone());
        }
        self.parents[0] = new_revid;
        let result = self.update_basis_by_delta_inner(entries);
        if result.is_ok() {
            self.mark_modified(&[], true);
            self.id_index = None;
        }
        result
    }

    fn update_basis_by_delta_inner(
        &mut self,
        entries: Vec<FlatBasisDeltaEntry>,
    ) -> Result<(), BasisApplyError> {
        use std::collections::BTreeSet;

        let mut adds: Vec<BasisAdd> = Vec::new();
        let mut changes: Vec<(Vec<u8>, Vec<u8>, Vec<u8>, TreeData)> = Vec::new();
        let mut deletes: Vec<(Vec<u8>, Option<Vec<u8>>, Vec<u8>, bool)> = Vec::new();
        let mut parents_set: BTreeSet<(Vec<u8>, Vec<u8>)> = BTreeSet::new();
        let mut new_ids: Vec<Vec<u8>> = Vec::new();

        let details_to_tree_data = |d: &(u8, Vec<u8>, u64, bool, Vec<u8>)| TreeData {
            minikind: d.0,
            fingerprint: d.1.clone(),
            size: d.2,
            executable: d.3,
            packed_stat: d.4.clone(),
        };

        for entry in entries {
            let FlatBasisDeltaEntry {
                old_path,
                new_path,
                file_id,
                parent_id,
                details,
            } = entry;
            if let Some(ref np) = new_path {
                let (dirname_utf8, basename_utf8) = split_path_utf8(np);
                if !basename_utf8.is_empty() {
                    let pid = parent_id.clone().unwrap_or_default();
                    parents_set.insert((dirname_utf8.to_vec(), pid));
                }
            }
            match (old_path.clone(), new_path.clone()) {
                (None, Some(np)) => {
                    let details = details.as_ref().expect("add must have details");
                    adds.push(BasisAdd {
                        old_path: None,
                        new_path: np,
                        file_id: file_id.clone(),
                        new_details: details_to_tree_data(details),
                        real_add: true,
                    });
                    new_ids.push(file_id);
                }
                (Some(op), None) => {
                    deletes.push((op, None, file_id, true));
                }
                (Some(op), Some(np)) if op.is_empty() && np.is_empty() => {
                    let details = details.as_ref().expect("change must have details");
                    changes.push((op, np, file_id, details_to_tree_data(details)));
                }
                (Some(op), Some(np)) => {
                    // Drain pending deletes before walking tree-1
                    // children of old_path — otherwise we'd see
                    // stale rows.
                    self.update_basis_apply_deletes(&deletes)?;
                    deletes.clear();
                    let details = details.as_ref().expect("rename must have details");
                    adds.push(BasisAdd {
                        old_path: Some(op.clone()),
                        new_path: np.clone(),
                        file_id: file_id.clone(),
                        new_details: details_to_tree_data(details),
                        real_add: false,
                    });
                    // Walk children of old_path in tree 1 in
                    // reverse (Python does `reversed(list(...))`)
                    // so deeper paths come out first.
                    let mut children = self.iter_child_entries(1, &op);
                    children.reverse();
                    for child in children {
                        let child_dirname = child.key.dirname.clone();
                        let child_basename = child.key.basename.clone();
                        let child_fid = child.key.file_id.clone();
                        let mut source_path = child_dirname.clone();
                        if !source_path.is_empty() {
                            source_path.push(b'/');
                        }
                        source_path.extend_from_slice(&child_basename);
                        let target_path = if !np.is_empty() {
                            let suffix = &source_path[op.len()..];
                            let mut t = np.clone();
                            t.extend_from_slice(suffix);
                            t
                        } else {
                            if op.is_empty() {
                                return Err(BasisApplyError::Internal {
                                    reason: "cannot rename directory to itself".to_string(),
                                });
                            }
                            source_path[op.len() + 1..].to_vec()
                        };
                        let child_tree1 = child.trees.get(1).cloned().unwrap_or(TreeData {
                            minikind: 0,
                            fingerprint: Vec::new(),
                            size: 0,
                            executable: false,
                            packed_stat: Vec::new(),
                        });
                        adds.push(BasisAdd {
                            old_path: None,
                            new_path: target_path.clone(),
                            file_id: child_fid.clone(),
                            new_details: child_tree1,
                            real_add: false,
                        });
                        deletes.push((source_path, Some(target_path), child_fid, false));
                    }
                    deletes.push((op, Some(np), file_id, false));
                }
                (None, None) => {
                    return Err(BasisApplyError::Internal {
                        reason: "delta row with neither old_path nor new_path".to_string(),
                    });
                }
            }
        }

        self.check_delta_ids_absent(&new_ids, 1)?;
        self.update_basis_apply_deletes(&deletes)?;
        self.update_basis_apply_adds(&mut adds)?;
        self.update_basis_apply_changes(&changes)?;
        let parents_vec: Vec<(Vec<u8>, Vec<u8>)> = parents_set.into_iter().collect();
        self.after_delta_check_parents(&parents_vec, 1)?;
        Ok(())
    }

    /// Apply a pre-flattened inventory delta to tree 0. Mirrors
    /// Python's `DirState.update_by_delta` — the workhorse for
    /// `apply_inventory_delta` in dirstate-based trees.
    ///
    /// Each `entries` element is the Python-side extraction of one
    /// delta row: `(old_path, new_path, file_id, parent_id,
    /// minikind, executable, fingerprint)`. The Python caller is
    /// responsible for delta `.check()`/`.sort()` and for looking up
    /// `inv_entry.parent_id` / kind → minikind / `reference_revision`
    /// before calling this method.
    ///
    /// This function:
    /// 1. validates no repeated file_id,
    /// 2. accumulates `removals`, `insertions`, `new_ids`, `parents`,
    /// 3. expands each rename into delete+add pairs for all
    ///    descendant entries by walking [`DirState::iter_child_entries`],
    /// 4. calls `check_delta_ids_absent`, `apply_removals`,
    ///    `apply_insertions`, and `after_delta_check_parents` in
    ///    order — matching Python's try/except block exactly.
    pub fn update_by_delta(&mut self, entries: Vec<FlatDeltaEntry>) -> Result<(), BasisApplyError> {
        use std::collections::{BTreeSet, HashMap};

        let mut insertions: HashMap<Vec<u8>, (EntryKey, u8, bool, Vec<u8>, Vec<u8>)> =
            HashMap::new();
        let mut removals: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();
        let mut parents_set: BTreeSet<(Vec<u8>, Vec<u8>)> = BTreeSet::new();
        let mut new_ids: Vec<Vec<u8>> = Vec::new();

        for entry in entries {
            let FlatDeltaEntry {
                old_path,
                new_path,
                file_id,
                parent_id,
                minikind,
                executable,
                fingerprint,
            } = entry;
            if insertions.contains_key(&file_id) || removals.contains_key(&file_id) {
                let path = old_path
                    .clone()
                    .or_else(|| new_path.clone())
                    .unwrap_or_default();
                return Err(BasisApplyError::Invalid {
                    path,
                    file_id,
                    reason: "repeated file_id".to_string(),
                });
            }
            if let Some(ref op) = old_path {
                removals.insert(file_id.clone(), op.clone());
            } else {
                new_ids.push(file_id.clone());
            }
            if let Some(ref np) = new_path {
                let (dirname_utf8, basename) = split_path_utf8(np);
                if !basename.is_empty() {
                    let pid = parent_id.clone().unwrap_or_default();
                    parents_set.insert((dirname_utf8.to_vec(), pid));
                }
                let key = EntryKey {
                    dirname: dirname_utf8.to_vec(),
                    basename: basename.to_vec(),
                    file_id: file_id.clone(),
                };
                insertions.insert(
                    file_id.clone(),
                    (key, minikind, executable, fingerprint.clone(), np.clone()),
                );
            }
            // Transform renames into delete+add pairs for all children.
            if let (Some(ref op), Some(ref np)) = (&old_path, &new_path) {
                let children = self.iter_child_entries(0, op);
                for child in children {
                    let child_id = child.key.file_id.clone();
                    if insertions.contains_key(&child_id) || removals.contains_key(&child_id) {
                        continue;
                    }
                    let child_dirname = child.key.dirname.clone();
                    let child_basename = child.key.basename.clone();
                    let child_tree0 = child.trees.first();
                    let child_minikind = child_tree0.map(|t| t.minikind).unwrap_or(0);
                    let child_fingerprint = child_tree0
                        .map(|t| t.fingerprint.clone())
                        .unwrap_or_default();
                    let child_executable = child_tree0.map(|t| t.executable).unwrap_or(false);
                    let mut old_child_path = child_dirname.clone();
                    if !old_child_path.is_empty() {
                        old_child_path.push(b'/');
                    }
                    old_child_path.extend_from_slice(&child_basename);
                    removals.insert(child_id.clone(), old_child_path);
                    // new_child_dirname = new_path + child_dirname[len(old_path):]
                    let suffix = &child_dirname[op.len()..];
                    let mut new_child_dirname = np.clone();
                    new_child_dirname.extend_from_slice(suffix);
                    let mut new_child_path = new_child_dirname.clone();
                    if !new_child_path.is_empty() {
                        new_child_path.push(b'/');
                    }
                    new_child_path.extend_from_slice(&child_basename);
                    let key = EntryKey {
                        dirname: new_child_dirname,
                        basename: child_basename,
                        file_id: child_id.clone(),
                    };
                    insertions.insert(
                        child_id,
                        (
                            key,
                            child_minikind,
                            child_executable,
                            child_fingerprint,
                            new_child_path,
                        ),
                    );
                }
            }
        }

        self.check_delta_ids_absent(&new_ids, 0)?;
        let removals_vec: Vec<(Vec<u8>, Vec<u8>)> = removals
            .into_iter()
            .map(|(fid, path)| (fid, path))
            .collect();
        self.apply_removals(&removals_vec)?;
        let insertions_vec: Vec<(EntryKey, u8, bool, Vec<u8>, Vec<u8>)> =
            insertions.into_values().collect();
        self.apply_insertions(insertions_vec)?;
        let parents_vec: Vec<(Vec<u8>, Vec<u8>)> = parents_set.into_iter().collect();
        self.after_delta_check_parents(&parents_vec, 0)?;
        Ok(())
    }

    /// Apply a sequence of "insertions" to tree 0. Mirrors Python's
    /// `DirState._apply_insertions`: sort the adds and, for each,
    /// call [`DirState::update_minimal`]. A `NotVersioned` error
    /// from `update_minimal` is reshaped into `Invalid` with reason
    /// `"Missing parent"`, matching Python's
    /// `except NotVersionedError: self._raise_invalid(..., "Missing parent")`.
    pub fn apply_insertions(
        &mut self,
        adds: Vec<(EntryKey, u8, bool, Vec<u8>, Vec<u8>)>,
    ) -> Result<(), BasisApplyError> {
        let mut sorted = adds;
        sorted.sort_by(|a, b| {
            a.0.dirname
                .cmp(&b.0.dirname)
                .then_with(|| a.0.basename.cmp(&b.0.basename))
                .then_with(|| a.0.file_id.cmp(&b.0.file_id))
        });
        for (key, minikind, executable, fingerprint, path_utf8) in sorted {
            let file_id = key.file_id.clone();
            let tree0_details = TreeData {
                minikind,
                fingerprint,
                size: 0,
                executable,
                packed_stat: b"x".repeat(32),
            };
            match self.update_minimal(key, tree0_details, Some(&path_utf8), false) {
                Ok(()) => {}
                Err(BasisApplyError::NotVersioned { .. }) => {
                    return Err(BasisApplyError::Invalid {
                        path: path_utf8,
                        file_id,
                        reason: "Missing parent".to_string(),
                    });
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Apply a sequence of "changes" to tree 1. Mirrors Python's
    /// `DirState._update_basis_apply_changes`. Each change updates
    /// the tree-1 slot of an existing entry whose file_id matches
    /// at the new path. The entry must already exist and be live
    /// (tree-1 minikind not absent/relocated); otherwise the caller
    /// sees `BasisApplyError::Invalid`.
    ///
    /// Invalidates id_index and packed_stat_index caches.
    pub fn update_basis_apply_changes(
        &mut self,
        changes: &[(Vec<u8>, Vec<u8>, Vec<u8>, TreeData)],
    ) -> Result<(), BasisApplyError> {
        for (_old_path, new_path, file_id, new_details) in changes {
            let (dirname, basename) = split_path_utf8(new_path);
            let bei = get_block_entry_index(&self.dirblocks, dirname, basename, 1);
            if !bei.path_present {
                return Err(BasisApplyError::Invalid {
                    path: new_path.clone(),
                    file_id: file_id.clone(),
                    reason: "changed entry considered not present".to_string(),
                });
            }
            let entry = &mut self.dirblocks[bei.block_index].entries[bei.entry_index];
            if entry.key.file_id != *file_id {
                return Err(BasisApplyError::Invalid {
                    path: new_path.clone(),
                    file_id: file_id.clone(),
                    reason: "changed entry considered not present".to_string(),
                });
            }
            let tree1_kind = entry.trees.get(1).map(|t| t.minikind).unwrap_or(0);
            if tree1_kind == b'a' || tree1_kind == b'r' {
                return Err(BasisApplyError::Invalid {
                    path: new_path.clone(),
                    file_id: file_id.clone(),
                    reason: "changed entry considered not present".to_string(),
                });
            }
            if entry.trees.len() >= 2 {
                entry.trees[1] = new_details.clone();
            } else {
                entry.trees.push(new_details.clone());
            }
        }
        self.id_index = None;
        self.packed_stat_index = None;
        Ok(())
    }

    /// Apply a sequence of "deletes" to tree 1. Mirrors Python's
    /// `DirState._update_basis_apply_deletes`. Each delete either
    /// removes an entry row entirely (when the active tree is also
    /// absent/relocated) or sets its tree-1 slot to NULL_PARENT_DETAILS
    /// so the file id survives in the active tree. The post-delete
    /// dirblock integrity check walks child blocks to ensure no live
    /// rows were left behind; that check follows Python exactly.
    ///
    /// Each tuple is `(old_path, Option<new_path>, file_id, real_delete)`
    /// where `real_delete` must equal `new_path.is_none()` — otherwise
    /// the caller sees `BasisApplyError::Invalid("bad delete delta")`.
    ///
    /// Invalidates id_index and packed_stat_index caches.
    pub fn update_basis_apply_deletes(
        &mut self,
        deletes: &[(Vec<u8>, Option<Vec<u8>>, Vec<u8>, bool)],
    ) -> Result<(), BasisApplyError> {
        for (old_path, new_path, file_id, real_delete) in deletes {
            if *real_delete != new_path.is_none() {
                return Err(BasisApplyError::Invalid {
                    path: old_path.clone(),
                    file_id: file_id.clone(),
                    reason: "bad delete delta".to_string(),
                });
            }

            let (dirname, basename) = split_path_utf8(old_path);
            let bei = get_block_entry_index(&self.dirblocks, dirname, basename, 1);
            if !bei.path_present {
                return Err(BasisApplyError::Invalid {
                    path: old_path.clone(),
                    file_id: file_id.clone(),
                    reason: "basis tree does not contain removed entry".to_string(),
                });
            }
            let (active_kind, old_kind, entry_file_id) = {
                let entry = &self.dirblocks[bei.block_index].entries[bei.entry_index];
                (
                    entry.trees.first().map(|t| t.minikind).unwrap_or(0),
                    entry.trees.get(1).map(|t| t.minikind).unwrap_or(0),
                    entry.key.file_id.clone(),
                )
            };
            if entry_file_id != *file_id {
                return Err(BasisApplyError::Invalid {
                    path: old_path.clone(),
                    file_id: file_id.clone(),
                    reason: "mismatched file_id in tree 1".to_string(),
                });
            }

            // The dirblock whose children are then scanned for
            // live-row leaks. `None` when no follow-up check is
            // needed.
            let mut dir_block_index: Option<usize> = None;

            if active_kind == b'a' || active_kind == b'r' {
                if active_kind == b'r' {
                    // Follow the tree-0 relocation pointer and
                    // clear the target's tree-1 slot.
                    let active_path = self.dirblocks[bei.block_index].entries[bei.entry_index]
                        .trees[0]
                        .fingerprint
                        .clone();
                    let (adirname, abasename) = split_path_utf8(&active_path);
                    let abei = get_block_entry_index(&self.dirblocks, adirname, abasename, 0);
                    if !abei.path_present {
                        return Err(BasisApplyError::Invalid {
                            path: old_path.clone(),
                            file_id: file_id.clone(),
                            reason: "Dirstate did not have matching rename entries".to_string(),
                        });
                    }
                    let (a_t0, a_t1) = {
                        let ae = &self.dirblocks[abei.block_index].entries[abei.entry_index];
                        (
                            ae.trees.first().map(|t| t.minikind).unwrap_or(0),
                            ae.trees.get(1).map(|t| t.minikind).unwrap_or(0),
                        )
                    };
                    if a_t1 != b'r' {
                        return Err(BasisApplyError::Invalid {
                            path: old_path.clone(),
                            file_id: file_id.clone(),
                            reason: "Dirstate did not have matching rename entries".to_string(),
                        });
                    }
                    if a_t0 == b'a' || a_t0 == b'r' {
                        return Err(BasisApplyError::Invalid {
                            path: old_path.clone(),
                            file_id: file_id.clone(),
                            reason: "Dirstate had a rename pointing at an inactive tree0"
                                .to_string(),
                        });
                    }
                    let ae = &mut self.dirblocks[abei.block_index].entries[abei.entry_index];
                    let null = TreeData {
                        minikind: b'a',
                        fingerprint: Vec::new(),
                        size: 0,
                        executable: false,
                        packed_stat: Vec::new(),
                    };
                    if ae.trees.len() >= 2 {
                        ae.trees[1] = null;
                    } else {
                        ae.trees.push(null);
                    }
                }

                self.dirblocks[bei.block_index]
                    .entries
                    .remove(bei.entry_index);

                if old_kind == b'd' {
                    let dirblock_key = EntryKey {
                        dirname: old_path.clone(),
                        basename: Vec::new(),
                        file_id: Vec::new(),
                    };
                    let (db_index, db_present) =
                        find_block_index_from_key(&self.dirblocks, &dirblock_key);
                    if db_present {
                        if self.dirblocks[db_index].entries.is_empty() {
                            self.dirblocks.remove(db_index);
                        } else {
                            dir_block_index = Some(db_index);
                        }
                    }
                }
            } else {
                let entry = &mut self.dirblocks[bei.block_index].entries[bei.entry_index];
                let null = TreeData {
                    minikind: b'a',
                    fingerprint: Vec::new(),
                    size: 0,
                    executable: false,
                    packed_stat: Vec::new(),
                };
                if entry.trees.len() >= 2 {
                    entry.trees[1] = null;
                } else {
                    entry.trees.push(null);
                }

                let child_bei = get_block_entry_index(&self.dirblocks, old_path, b"", 1);
                if child_bei.dir_present {
                    dir_block_index = Some(child_bei.block_index);
                }
            }

            if let Some(db_index) = dir_block_index {
                let block = &self.dirblocks[db_index];
                for child in &block.entries {
                    let child_tree1 = child.trees.get(1).map(|t| t.minikind).unwrap_or(0);
                    if child_tree1 != b'a' && child_tree1 != b'r' {
                        return Err(BasisApplyError::Invalid {
                            path: old_path.clone(),
                            file_id: file_id.clone(),
                            reason: "The file id was deleted but its children were not deleted."
                                .to_string(),
                        });
                    }
                }
            }
        }

        self.id_index = None;
        self.packed_stat_index = None;
        Ok(())
    }

    /// Look up the dirstate entry for `file_id` in `tree_index`,
    /// following any relocation chain the entries describe. Mirrors
    /// the `fileid_utf8` branch of Python's `DirState._get_entry`.
    ///
    /// If `include_deleted` is true, an entry whose tree data is
    /// absent (`b'a'`) is returned rather than hidden. Returns
    /// [`GetEntryResult::NotFound`] if no key for `file_id` exists in
    /// the id index, [`GetEntryResult::Entry`] with the located entry
    /// key on success, or [`GetEntryResult::InvalidMinikind`] if a
    /// tree-data row has a minikind that is neither live nor
    /// absent/relocated (mirroring the `AssertionError` Python raises).
    ///
    /// The result is returned as an owned [`EntryKey`] rather than a
    /// borrow because the caller may need to keep `self` borrowable
    /// for other lookups; callers that need the full entry can
    /// re-fetch it via [`DirState::find_block_index_from_key`] and
    /// [`DirState::find_entry_index`].
    pub fn get_entry_by_file_id(
        &mut self,
        tree_index: usize,
        file_id: &[u8],
        include_deleted: bool,
    ) -> GetEntryResult {
        // Copy out the candidate keys so we can drop the borrow on
        // `self.id_index` and mutate other state during the scan.
        let candidates = {
            let idx = self.get_or_build_id_index();
            idx.get(&FileId::from(&file_id.to_vec()))
        };
        if candidates.is_empty() {
            return GetEntryResult::NotFound;
        }

        // Follow relocation chains until we hit a live entry, an
        // absent entry, or run out of candidate keys. Bounded by the
        // number of relocation hops the dirstate actually contains;
        // the `visited` set guards against pathological cycles.
        let mut current: Vec<EntryKey> = candidates
            .into_iter()
            .map(|(d, b, f)| EntryKey {
                dirname: d,
                basename: b,
                file_id: f.as_bytes().to_vec(),
            })
            .collect();
        let mut visited: HashSet<EntryKey> = HashSet::new();

        loop {
            let mut relocation_target: Option<Vec<u8>> = None;
            for key in &current {
                if !visited.insert(key.clone()) {
                    continue;
                }
                let (block_index, present) = find_block_index_from_key(&self.dirblocks, key);
                // "strange, probably indicates an out of date id index" —
                // Python's comment: silently skip stale entries.
                if !present {
                    continue;
                }
                let block = &self.dirblocks[block_index].entries;
                let (entry_index, entry_present) = find_entry_index(key, block);
                if !entry_present {
                    continue;
                }
                let entry = &block[entry_index];
                let Some(tree) = entry.trees.get(tree_index) else {
                    continue;
                };
                match tree.minikind {
                    b'f' | b'd' | b'l' | b't' => {
                        return GetEntryResult::Entry(entry.key.clone());
                    }
                    b'a' => {
                        if include_deleted {
                            return GetEntryResult::Entry(entry.key.clone());
                        }
                        return GetEntryResult::NotFound;
                    }
                    b'r' => {
                        // Follow the relocation by recursing via the
                        // `real_path` fingerprint.
                        relocation_target = Some(tree.fingerprint.clone());
                        break;
                    }
                    other => {
                        return GetEntryResult::InvalidMinikind {
                            key: entry.key.clone(),
                            tree_index,
                            minikind: other,
                        };
                    }
                }
            }
            match relocation_target {
                Some(real_path) => {
                    // The relocation target is a path — Python just
                    // recurses with the same fileid_utf8 and the new
                    // path, walking the id index again. We mirror that
                    // by filtering the candidate set down to keys that
                    // match the (dirname, basename) split of the real
                    // path, leaving the file_id constraint in place.
                    let (dirname, basename) = split_path_utf8(&real_path);
                    let all = self
                        .get_or_build_id_index()
                        .get(&FileId::from(&file_id.to_vec()));
                    current = all
                        .into_iter()
                        .filter(|(d, b, _)| d == dirname && b == basename)
                        .map(|(d, b, f)| EntryKey {
                            dirname: d,
                            basename: b,
                            file_id: f.as_bytes().to_vec(),
                        })
                        .collect();
                    if current.is_empty() {
                        return GetEntryResult::NotFound;
                    }
                }
                None => return GetEntryResult::NotFound,
            }
        }
    }

    /// Remove `entries[index]` from `entries` (and drop it from
    /// `id_index`) if none of its trees hold a live record — i.e.
    /// every tree column is `b'a'` (absent) or `b'r'` (relocation).
    /// Mirrors Python's `DirState._maybe_remove_row`.
    ///
    /// Returns `true` if the row was removed, `false` otherwise.
    pub fn maybe_remove_row(
        entries: &mut Vec<Entry>,
        index: usize,
        id_index: &mut IdIndex,
    ) -> bool {
        let entry = &entries[index];
        let present_in_row = entry
            .trees
            .iter()
            .any(|t| t.minikind != b'a' && t.minikind != b'r');
        if present_in_row {
            return false;
        }
        let file_id = FileId::from(&entry.key.file_id);
        id_index.remove((
            entry.key.dirname.as_slice(),
            entry.key.basename.as_slice(),
            &file_id,
        ));
        entries.remove(index);
        true
    }

    /// Sort `entries` into canonical dirblock order. Mirrors Python's
    /// `DirState._sort_entries`: the sort key is
    /// `(dirname.split(b"/"), basename, file_id)`, which matches the
    /// order `_entries_to_current_state` expects before writing.
    ///
    /// The Python version caches `dirname → split` because real-world
    /// calls re-sort ~10× more entries than distinct directories;
    /// Rust's `sort_by_cached_key` gets the same amortisation
    /// automatically.
    pub fn sort_entries(entries: &mut [Entry]) {
        entries.sort_by_cached_key(|e| {
            (
                e.key
                    .dirname
                    .split(|&b| b == b'/')
                    .map(|s| s.to_vec())
                    .collect::<Vec<Vec<u8>>>(),
                e.key.basename.clone(),
                e.key.file_id.clone(),
            )
        });
    }

    /// Return references to every dirstate entry whose key `(dirname,
    /// basename)` matches `path_utf8`, across all file ids. Mirrors
    /// Python's `DirState._entries_for_path`: a path can be represented
    /// by multiple rows when the same location held different file ids
    /// in different parent trees, so the lookup walks the block
    /// starting at the first matching entry and stops at the first
    /// non-match. Returns an empty list when no block exists for the
    /// parent directory.
    pub fn entries_for_path(&self, path_utf8: &[u8]) -> Vec<&Entry> {
        let (dirname, basename) = split_path_utf8(path_utf8);
        let key = EntryKey {
            dirname: dirname.to_vec(),
            basename: basename.to_vec(),
            file_id: Vec::new(),
        };
        let (block_index, present) = self.find_block_index_from_key(&key);
        if !present {
            return Vec::new();
        }
        let block = &self.dirblocks[block_index].entries;
        let (mut entry_index, _) = self.find_entry_index(&key, block);
        let mut result = Vec::new();
        while entry_index < block.len() {
            let candidate = &block[entry_index];
            if candidate.key.dirname != key.dirname || candidate.key.basename != key.basename {
                break;
            }
            result.push(candidate);
            entry_index += 1;
        }
        result
    }

    /// Look up the dirstate entry at `path_utf8` in `tree_index` and
    /// return a reference to it, or `None` if the path is not present
    /// in that tree. Mirrors the `path_utf8` branch of Python's
    /// `DirState._get_entry` (the file-id fallback is a follow-up port
    /// once `_get_id_index` exists in Rust).
    ///
    /// `path_utf8` is split on the last `/` into a `(dirname, basename)`
    /// pair matching `osutils.split`, then fed through
    /// [`DirState::get_block_entry_index`]. The result points at a
    /// live (non-absent, non-relocated) entry only when `path_present`
    /// is true; otherwise `None` is returned.
    pub fn get_entry_by_path(&self, tree_index: usize, path_utf8: &[u8]) -> Option<&Entry> {
        let (dirname, basename) = split_path_utf8(path_utf8);
        let bei = self.get_block_entry_index(dirname, basename, tree_index);
        if !bei.path_present {
            return None;
        }
        self.dirblocks
            .get(bei.block_index)
            .and_then(|b| b.entries.get(bei.entry_index))
    }

    /// Walk the subtree rooted at `path_utf8` and return every live
    /// entry (kind not in `b'a'`/`b'r'`) in `tree_index`, in the order
    /// Python's `DirState._iter_child_entries` yields them.
    ///
    /// The walk is breadth-first: all immediate children of `path_utf8`
    /// first, then all children of those (grouped by whichever parent
    /// they were enqueued from). Directory entries whose tree data says
    /// they're directories (`b'd'`) are recursed into; absent and
    /// relocated entries are filtered out of the output but do not
    /// suppress the recursion into other entries.
    ///
    /// An empty `path_utf8` walks the top of the tree. Asking for the
    /// children of a non-directory returns an empty vector.
    pub fn iter_child_entries(&self, tree_index: usize, path_utf8: &[u8]) -> Vec<Entry> {
        let mut out: Vec<Entry> = Vec::new();
        let mut next_pending: Vec<Vec<u8>> = vec![path_utf8.to_vec()];
        while !next_pending.is_empty() {
            let pending = std::mem::take(&mut next_pending);
            for path in pending {
                let lookup_key = EntryKey {
                    dirname: path.clone(),
                    basename: Vec::new(),
                    file_id: Vec::new(),
                };
                let (mut block_index, present) =
                    find_block_index_from_key(&self.dirblocks, &lookup_key);
                // Python treats block_index 0 as a special case: the
                // caller asked for the root, and the first real block
                // with root entries lives at index 1. If there are no
                // other blocks we're done.
                if block_index == 0 {
                    block_index = 1;
                    if self.dirblocks.len() == 1 {
                        return out;
                    }
                } else if !present {
                    // children of a non-directory asked for.
                    continue;
                }
                if block_index >= self.dirblocks.len() {
                    continue;
                }
                let block = &self.dirblocks[block_index];
                for entry in &block.entries {
                    let kind = entry
                        .trees
                        .get(tree_index)
                        .map(|t| t.minikind)
                        .unwrap_or(b'a');
                    if kind != b'a' && kind != b'r' {
                        out.push(entry.clone());
                    }
                    if kind == b'd' {
                        // Build `dirname/basename` for the recursion.
                        let next_path = if entry.key.dirname.is_empty() {
                            entry.key.basename.clone()
                        } else {
                            let mut p = entry.key.dirname.clone();
                            p.push(b'/');
                            p.extend_from_slice(&entry.key.basename);
                            p
                        };
                        next_pending.push(next_path);
                    }
                }
            }
        }
        out
    }

    /// Bisect the on-disk dirstate for rows at the given paths.
    /// Mirrors Python's `DirState._bisect`.
    ///
    /// `read_range(offset, len)` must return the bytes at `[offset,
    /// offset+len)` from the dirstate file. `file_size` is the full
    /// file length (used to bound the initial bisect window). The
    /// caller must have already loaded the header (so
    /// `end_of_header` and `num_present_parents()` are populated)
    /// and must hold a read or write lock on the file.
    ///
    /// Returns a map from `path_utf8` → list of entries at that path
    /// (an entry is the usual `(key, [tree_data, ...])` shape).
    /// Missing paths do not appear in the map.
    pub fn bisect<F>(
        &self,
        paths: Vec<Vec<u8>>,
        file_size: u64,
        mut read_range: F,
    ) -> Result<std::collections::HashMap<Vec<u8>, Vec<Entry>>, BisectError>
    where
        F: FnMut(u64, usize) -> Result<Vec<u8>, BisectError>,
    {
        bisect_bytes(
            self.end_of_header.unwrap_or(0) as u64,
            file_size,
            self.num_present_parents(),
            paths,
            BisectMode::Paths,
            &mut read_range,
        )
    }

    /// Bisect the on-disk dirstate for every entry whose dirname is
    /// in `dir_list`. Mirrors Python's `DirState._bisect_dirblocks`.
    pub fn bisect_dirblocks<F>(
        &self,
        dir_list: Vec<Vec<u8>>,
        file_size: u64,
        mut read_range: F,
    ) -> Result<std::collections::HashMap<Vec<u8>, Vec<Entry>>, BisectError>
    where
        F: FnMut(u64, usize) -> Result<Vec<u8>, BisectError>,
    {
        bisect_bytes(
            self.end_of_header.unwrap_or(0) as u64,
            file_size,
            self.num_present_parents(),
            dir_list,
            BisectMode::Dirnames,
            &mut read_range,
        )
    }

    /// Recursive variant of `bisect`: for every path in `paths` find
    /// the row and, if it is a directory, recursively bisect for its
    /// children. Renames are followed via the fingerprint pointer.
    /// Mirrors `DirState._bisect_recursive`.
    ///
    /// Returns a map from `(dirname, basename, file_id)` → list of
    /// tree-data rows.
    #[allow(clippy::type_complexity)]
    pub fn bisect_recursive<F>(
        &self,
        paths: Vec<Vec<u8>>,
        file_size: u64,
        mut read_range: F,
    ) -> Result<std::collections::HashMap<(Vec<u8>, Vec<u8>, Vec<u8>), Vec<TreeData>>, BisectError>
    where
        F: FnMut(u64, usize) -> Result<Vec<u8>, BisectError>,
    {
        use std::collections::{HashMap, HashSet};
        let mut found: HashMap<(Vec<u8>, Vec<u8>, Vec<u8>), Vec<TreeData>> = HashMap::new();
        let mut found_dir_names: HashSet<(Vec<u8>, Vec<u8>)> = HashSet::new();
        let mut processed_dirs: HashSet<Vec<u8>> = HashSet::new();

        // Seed: run bisect() on the initial path list.
        let mut newly_found = bisect_bytes(
            self.end_of_header.unwrap_or(0) as u64,
            file_size,
            self.num_present_parents(),
            paths,
            BisectMode::Paths,
            &mut read_range,
        )?;

        while !newly_found.is_empty() {
            let mut pending_dirs: Vec<Vec<u8>> = Vec::new();
            let mut paths_to_search: Vec<Vec<u8>> = Vec::new();
            for entries in newly_found.values() {
                for entry in entries {
                    let key = (
                        entry.key.dirname.clone(),
                        entry.key.basename.clone(),
                        entry.key.file_id.clone(),
                    );
                    found.insert(key.clone(), entry.trees.clone());
                    found_dir_names.insert((entry.key.dirname.clone(), entry.key.basename.clone()));
                    let mut is_dir = false;
                    for tree_info in &entry.trees {
                        match tree_info.minikind {
                            b'd' => {
                                if is_dir {
                                    continue;
                                }
                                is_dir = true;
                                let mut path = entry.key.dirname.clone();
                                if !path.is_empty() {
                                    path.push(b'/');
                                }
                                path.extend_from_slice(&entry.key.basename);
                                if !processed_dirs.contains(&path) {
                                    pending_dirs.push(path);
                                }
                            }
                            b'r' => {
                                let (dn, _bn) = split_path_utf8(&tree_info.fingerprint);
                                if pending_dirs.iter().any(|p| p == dn) {
                                    continue;
                                }
                                let dn_vec = dn.to_vec();
                                let (rdn, rbn) = split_path_utf8(&tree_info.fingerprint);
                                if !found_dir_names.contains(&(rdn.to_vec(), rbn.to_vec())) {
                                    paths_to_search.push(tree_info.fingerprint.clone());
                                    let _ = dn_vec; // silence warning
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
            paths_to_search.sort();
            paths_to_search.dedup();
            pending_dirs.sort();
            pending_dirs.dedup();

            newly_found = bisect_bytes(
                self.end_of_header.unwrap_or(0) as u64,
                file_size,
                self.num_present_parents(),
                paths_to_search,
                BisectMode::Paths,
                &mut read_range,
            )?;
            let dir_results = bisect_bytes(
                self.end_of_header.unwrap_or(0) as u64,
                file_size,
                self.num_present_parents(),
                pending_dirs.clone(),
                BisectMode::Dirnames,
                &mut read_range,
            )?;
            for (k, v) in dir_results {
                newly_found.insert(k, v);
            }
            for d in pending_dirs {
                processed_dirs.insert(d);
            }
        }

        Ok(found)
    }
}

/// Shared bisect mode: match by full path (dirname/basename) or by
/// dirname only.
#[derive(Copy, Clone, PartialEq, Eq)]
enum BisectMode {
    /// Input keys are `dirname/basename` strings; match against the
    /// concatenation `fields[1]/fields[2]` (or `fields[2]` if
    /// `fields[1]` is empty).  Used by `bisect`.
    Paths,
    /// Input keys are dirnames; match against `fields[1]` directly.
    /// Used by `bisect_dirblocks`.
    Dirnames,
}

/// Error returned by the bisect primitives.
#[derive(Debug, PartialEq, Eq)]
pub enum BisectError {
    /// The caller's `read_range` closure reported a failure.
    ReadError(String),
    /// The bisect loop exceeded its safety counter.  Mirrors Python's
    /// `BzrFormatsError("Too many seeks, most likely a bug.")`.
    TooManySeeks,
    /// An entry row's size field could not be parsed as an integer.
    BadSize(String),
}

impl std::fmt::Display for BisectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BisectError::ReadError(s) => write!(f, "read error: {}", s),
            BisectError::TooManySeeks => write!(f, "too many seeks"),
            BisectError::BadSize(s) => write!(f, "bad size field: {}", s),
        }
    }
}

impl std::error::Error for BisectError {}

fn bisect_bytes<F>(
    end_of_header: u64,
    file_size: u64,
    num_present_parents: usize,
    keys: Vec<Vec<u8>>,
    mode: BisectMode,
    read_range: &mut F,
) -> Result<std::collections::HashMap<Vec<u8>, Vec<Entry>>, BisectError>
where
    F: FnMut(u64, usize) -> Result<Vec<u8>, BisectError>,
{
    let mut found: std::collections::HashMap<Vec<u8>, Vec<Entry>> =
        std::collections::HashMap::new();
    if keys.is_empty() || file_size == 0 {
        return Ok(found);
    }

    // Each entry has one extra trailing empty field because of the
    // terminating newline-NUL split: fields_per_entry accounts for the
    // trailing `\n` slot already, and we need one more for the empty
    // leading field produced by the record separator. The Python code
    // keeps them in the same count constant.
    let entry_field_count = fields_per_entry(num_present_parents) + 1;

    // Sort keys so the bisect_left/right calls below can rely on
    // ordered input.  (Python callers sort beforehand; we defensively
    // sort too.)
    let mut sorted_keys: Vec<Vec<u8>> = keys;
    sorted_keys.sort();
    sorted_keys.dedup();

    let max_count = 30 * sorted_keys.len();
    let mut count = 0usize;

    let low0 = end_of_header;
    let high0 = file_size.saturating_sub(1);
    let mut pending: Vec<(u64, u64, Vec<Vec<u8>>)> = vec![(low0, high0, sorted_keys)];
    let mut page_size: usize = BISECT_PAGE_SIZE;

    while let Some((low, high, cur_keys)) = pending.pop() {
        if cur_keys.is_empty() || low >= high {
            continue;
        }

        count += 1;
        if count > max_count {
            return Err(BisectError::TooManySeeks);
        }

        // `mid` biases toward reading from the *start* of a page-sized
        // window, matching Python's `(low + high - page_size) // 2`
        // calculation.
        let mid_i = ((low + high) as i64 - page_size as i64) / 2;
        let mid = if mid_i < low as i64 {
            low
        } else {
            mid_i as u64
        };
        let read_size = std::cmp::min(page_size as u64, (high - mid) + 1) as usize;
        let block = read_range(mid, read_size)?;

        let entries: Vec<&[u8]> = block.split(|&b| b == b'\n').collect();

        if entries.len() < 2 {
            page_size *= 2;
            pending.push((low, high, cur_keys));
            continue;
        }

        let mut start = mid;
        let mut first_entry_num: usize = 0;
        let mut first_fields: Vec<&[u8]> = entries[0].split(|&b| b == 0u8).collect();
        if first_fields.len() < entry_field_count {
            start += entries[0].len() as u64 + 1;
            first_entry_num = 1;
            first_fields = entries[1].split(|&b| b == 0u8).collect();
        }

        let first_threshold = match mode {
            BisectMode::Paths => 2,
            BisectMode::Dirnames => 1,
        };
        if first_fields.len() <= first_threshold {
            page_size *= 2;
            pending.push((low, high, cur_keys));
            continue;
        }

        let first_key: Vec<u8> = match mode {
            BisectMode::Paths => {
                if !first_fields[1].is_empty() {
                    let mut p = first_fields[1].to_vec();
                    p.push(b'/');
                    p.extend_from_slice(first_fields[2]);
                    p
                } else {
                    first_fields[2].to_vec()
                }
            }
            BisectMode::Dirnames => first_fields[1].to_vec(),
        };

        let first_loc = match mode {
            BisectMode::Paths => bisect_path_left_bytes(&cur_keys, &first_key),
            BisectMode::Dirnames => bisect_bytes_left(&cur_keys, &first_key),
        };
        let pre: Vec<Vec<u8>> = cur_keys[..first_loc].to_vec();
        let mut post: Vec<Vec<u8>> = cur_keys[first_loc..].to_vec();
        let mut after = start;

        let mut pre_out = pre;
        let mut post_out = post;
        if !post_out.is_empty() && first_fields.len() >= entry_field_count {
            let mut last_entry_num = entries.len() - 1;
            let mut last_fields: Vec<&[u8]> =
                entries[last_entry_num].split(|&b| b == 0u8).collect();
            if last_fields.len() < entry_field_count {
                after = mid + (block.len() as u64) - (entries[entries.len() - 1].len() as u64);
                last_entry_num -= 1;
                last_fields = entries[last_entry_num].split(|&b| b == 0u8).collect();
            } else {
                after = mid + block.len() as u64;
            }

            let last_key: Vec<u8> = match mode {
                BisectMode::Paths => {
                    if !last_fields[1].is_empty() {
                        let mut p = last_fields[1].to_vec();
                        p.push(b'/');
                        p.extend_from_slice(last_fields[2]);
                        p
                    } else {
                        last_fields[2].to_vec()
                    }
                }
                BisectMode::Dirnames => last_fields[1].to_vec(),
            };

            let last_loc = match mode {
                BisectMode::Paths => bisect_path_right_bytes(&post_out, &last_key),
                BisectMode::Dirnames => bisect_bytes_right(&post_out, &last_key),
            };
            let middle: Vec<Vec<u8>> = post_out[..last_loc].to_vec();
            post_out = post_out[last_loc..].to_vec();

            if !middle.is_empty() {
                if middle.first() == Some(&first_key) {
                    pre_out.push(first_key.clone());
                }
                if middle.last() == Some(&last_key) {
                    post_out.insert(0, last_key.clone());
                }

                // Map keys in this page to their parsed field rows.
                let mut page_paths: std::collections::HashMap<Vec<u8>, Vec<Vec<Vec<u8>>>> =
                    std::collections::HashMap::new();
                page_paths
                    .entry(first_key.clone())
                    .or_default()
                    .push(first_fields.iter().map(|s| s.to_vec()).collect());
                if last_entry_num != first_entry_num {
                    page_paths
                        .entry(last_key.clone())
                        .or_default()
                        .push(last_fields.iter().map(|s| s.to_vec()).collect());
                }
                for num in (first_entry_num + 1)..last_entry_num {
                    let fields: Vec<&[u8]> = entries[num].split(|&b| b == 0u8).collect();
                    let key: Vec<u8> = match mode {
                        BisectMode::Paths => {
                            if !fields[1].is_empty() {
                                let mut p = fields[1].to_vec();
                                p.push(b'/');
                                p.extend_from_slice(fields[2]);
                                p
                            } else {
                                fields[2].to_vec()
                            }
                        }
                        BisectMode::Dirnames => fields[1].to_vec(),
                    };
                    page_paths
                        .entry(key)
                        .or_default()
                        .push(fields.iter().map(|s| s.to_vec()).collect());
                }

                for key in &middle {
                    if let Some(rows) = page_paths.get(key) {
                        for row in rows {
                            let entry = fields_to_entry(&row[1..], num_present_parents)?;
                            found.entry(key.clone()).or_default().push(entry);
                        }
                    }
                }
            }
        }

        if !post_out.is_empty() {
            pending.push((after, high, post_out));
        }
        if !pre_out.is_empty() {
            pending.push((low, start.saturating_sub(1), pre_out));
        }
    }

    Ok(found)
}

fn fields_to_entry(fields: &[Vec<u8>], num_present_parents: usize) -> Result<Entry, BisectError> {
    let key = EntryKey {
        dirname: fields[0].clone(),
        basename: fields[1].clone(),
        file_id: fields[2].clone(),
    };
    let tree_count = 1 + num_present_parents;
    let mut trees = Vec::with_capacity(tree_count);
    for t in 0..tree_count {
        let base = 3 + 5 * t;
        let minikind = fields[base].first().copied().unwrap_or(0);
        let fingerprint = fields[base + 1].clone();
        let size_str = std::str::from_utf8(&fields[base + 2])
            .map_err(|e| BisectError::BadSize(e.to_string()))?;
        let size: u64 = size_str
            .parse()
            .map_err(|e: std::num::ParseIntError| BisectError::BadSize(e.to_string()))?;
        let executable = fields[base + 3].first() == Some(&b'y');
        let packed_stat = fields[base + 4].clone();
        trees.push(TreeData {
            minikind,
            fingerprint,
            size,
            executable,
            packed_stat,
        });
    }
    Ok(Entry { key, trees })
}

fn bisect_bytes_left(keys: &[Vec<u8>], needle: &[u8]) -> usize {
    let mut lo = 0;
    let mut hi = keys.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        if keys[mid].as_slice() < needle {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

fn bisect_bytes_right(keys: &[Vec<u8>], needle: &[u8]) -> usize {
    let mut lo = 0;
    let mut hi = keys.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        if needle < keys[mid].as_slice() {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    lo
}

/// Byte-slice variants of `bisect_path_left` / `bisect_path_right`
/// that compare by dirblock (component-wise split on `/`), used by
/// the bisect parser.
fn bisect_path_left_bytes(keys: &[Vec<u8>], needle: &[u8]) -> usize {
    let mut lo = 0;
    let mut hi = keys.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        if cmp_path_by_dirblock(&keys[mid], needle).is_lt() {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

fn bisect_path_right_bytes(keys: &[Vec<u8>], needle: &[u8]) -> usize {
    let mut lo = 0;
    let mut hi = keys.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        if cmp_path_by_dirblock(needle, &keys[mid]).is_lt() {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    lo
}

fn cmp_path_by_dirblock(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    let (a_dir, a_base) = split_path_utf8(a);
    let (b_dir, b_base) = split_path_utf8(b);
    let dir_ord = cmp_by_dirs_bytes(a_dir, b_dir);
    if dir_ord != std::cmp::Ordering::Equal {
        return dir_ord;
    }
    a_base.cmp(b_base)
}

fn cmp_by_dirs_bytes(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    let mut ai = a.split(|&c| c == b'/');
    let mut bi = b.split(|&c| c == b'/');
    loop {
        match (ai.next(), bi.next()) {
            (None, None) => return std::cmp::Ordering::Equal,
            (None, Some(_)) => return std::cmp::Ordering::Less,
            (Some(_), None) => return std::cmp::Ordering::Greater,
            (Some(x), Some(y)) => match x.cmp(y) {
                std::cmp::Ordering::Equal => continue,
                other => return other,
            },
        }
    }
}

/// Error returned by [`DirState::ensure_block`] when the requested
/// dirname does not end with the parent entry's basename. Mirrors the
/// `AssertionError("bad dirname ...")` Python raises.
#[derive(Debug, PartialEq, Eq)]
pub enum EnsureBlockError {
    BadDirname(Vec<u8>),
}

impl std::fmt::Display for EnsureBlockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EnsureBlockError::BadDirname(dirname) => write!(f, "bad dirname {:?}", dirname),
        }
    }
}

impl std::error::Error for EnsureBlockError {}

/// Error returned by [`DirState::entries_to_current_state`] when the
/// input entry list violates the layout invariants Python asserts in
/// `_entries_to_current_state`.
#[derive(Debug, PartialEq, Eq)]
pub enum EntriesToStateError {
    /// The input entry list was empty — Python's implementation
    /// unconditionally indexes `new_entries[0]`, so an empty list is
    /// an implicit invariant violation that we surface explicitly.
    Empty,
    /// The first entry was not the root row (dirname and basename
    /// both empty). Mirrors Python's
    /// `AssertionError("Missing root row ...")`.
    MissingRootRow { key: EntryKey },
    /// The follow-up `split_root_dirblock_into_contents` step failed.
    /// Should only happen if the new entry list contains trailing
    /// blocks that pollute the second sentinel.
    SplitFailed(SplitRootError),
}

impl std::fmt::Display for EntriesToStateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EntriesToStateError::Empty => write!(f, "new_entries is empty"),
            EntriesToStateError::MissingRootRow { key } => {
                write!(
                    f,
                    "Missing root row ({:?}, {:?}, {:?})",
                    key.dirname, key.basename, key.file_id
                )
            }
            EntriesToStateError::SplitFailed(err) => {
                write!(f, "split_root_dirblock_into_contents: {}", err)
            }
        }
    }
}

impl std::error::Error for EntriesToStateError {}

/// One record in the `adds` list consumed by
/// [`DirState::update_basis_apply_adds`]. Mirrors the per-entry
/// tuple Python's `_update_basis_apply_adds` iterates over:
/// `(old_path, new_path_utf8, file_id, (entry_details), real_add)`.
#[derive(Debug, Clone)]
pub struct BasisAdd {
    /// Previous path when this add is the second half of a split
    /// rename. `None` for a genuine add.
    pub old_path: Option<Vec<u8>>,
    /// UTF-8 path of the entry to insert/update.
    pub new_path: Vec<u8>,
    /// File id of the entry.
    pub file_id: Vec<u8>,
    /// Tree details for the new entry's tree-1 slot.
    pub new_details: TreeData,
    /// True for a real add, false when this record is the add half
    /// of a split rename.
    pub real_add: bool,
}

/// Error returned by [`DirState::update_basis_apply_adds`] and the
/// sibling apply-changes / apply-deletes methods. Mirrors Python's
/// `_raise_invalid` and `AssertionError` / `NotImplementedError` paths.
#[derive(Debug, PartialEq, Eq)]
pub enum BasisApplyError {
    /// The caller-supplied add/change/delete conflicts with existing
    /// dirstate content — mirrors Python's `InconsistentDelta(path,
    /// file_id, reason)` exception.
    Invalid {
        path: Vec<u8>,
        file_id: Vec<u8>,
        reason: String,
    },
    /// The Python implementation raises `NotImplementedError` in this
    /// branch; carry the same signal so the caller can reproduce it.
    NotImplemented { reason: String },
    /// An invariant that should never be reachable was violated.
    /// Mirrors Python's `AssertionError` inside the apply helpers.
    Internal { reason: String },
    /// The (dirname, basename) path is not versioned — the parent
    /// directory has no entry in tree 0. Mirrors Python's
    /// `NotVersionedError` raised from `_find_block` when called
    /// without `add_if_missing`.
    NotVersioned { path: Vec<u8> },
}

/// A pre-flattened inventory-delta row passed to
/// [`DirState::update_by_delta`]. Mirrors the Python-side tuple the
/// caller builds by unpacking a delta entry and its
/// `InventoryEntry`. `minikind` is the single-byte code from
/// `DirState._kind_to_minikind`; `fingerprint` is empty for
/// non-tree-reference entries.
#[derive(Debug, Clone)]
pub struct FlatDeltaEntry {
    pub old_path: Option<Vec<u8>>,
    pub new_path: Option<Vec<u8>>,
    pub file_id: Vec<u8>,
    pub parent_id: Option<Vec<u8>>,
    pub minikind: u8,
    pub executable: bool,
    pub fingerprint: Vec<u8>,
}

/// A pre-flattened row passed to [`DirState::update_basis_by_delta`].
/// `details` is the 5-tuple returned by
/// [`inv_entry_to_details`]: `(minikind, fingerprint, size,
/// executable, tree_data)` — Python runs `inv_entry_to_details` per
/// row before dispatching. `details` may be `None` for deletions.
#[derive(Debug, Clone)]
pub struct FlatBasisDeltaEntry {
    pub old_path: Option<Vec<u8>>,
    pub new_path: Option<Vec<u8>>,
    pub file_id: Vec<u8>,
    pub parent_id: Option<Vec<u8>>,
    pub details: Option<(u8, Vec<u8>, u64, bool, Vec<u8>)>,
}

/// Error returned by [`DirState::validate`]. A single descriptive
/// string is enough — the pyo3 layer wraps it in `AssertionError`
/// exactly like Python's `_validate` raises.
#[derive(Debug, Clone)]
pub struct ValidateError(pub String);

impl std::fmt::Display for ValidateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ValidateError {}

impl std::fmt::Display for BasisApplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BasisApplyError::Invalid {
                path,
                file_id,
                reason,
            } => write!(
                f,
                "inconsistent delta at {:?} ({:?}): {}",
                path, file_id, reason
            ),
            BasisApplyError::NotImplemented { reason } => {
                write!(f, "not implemented: {}", reason)
            }
            BasisApplyError::Internal { reason } => write!(f, "internal error: {}", reason),
            BasisApplyError::NotVersioned { path } => {
                write!(f, "not versioned: {:?}", path)
            }
        }
    }
}

impl std::error::Error for BasisApplyError {}

/// Error returned by [`DirState::make_absent`] when the dirstate is
/// not in the shape Python's `_make_absent` expects. Each variant
/// mirrors one of Python's `AssertionError`s, carrying the offending
/// key for diagnostic messages.
#[derive(Debug, PartialEq, Eq)]
pub enum MakeAbsentError {
    /// No dirblock exists for `key.dirname`.
    BlockNotFound { key: EntryKey },
    /// The dirblock exists but `key` is not in it.
    EntryNotFound { key: EntryKey },
    /// While updating a remaining-reference key, its dirblock was not
    /// found — equivalent to Python's "could not find block for ..."
    /// assertion.
    UpdateBlockNotFound { key: EntryKey },
    /// While updating a remaining-reference key, its entry row was
    /// not found — equivalent to Python's "could not find entry
    /// for ..." assertion.
    UpdateEntryNotFound { key: EntryKey },
    /// A remaining-reference key's tree 0 slot was missing or already
    /// marked absent. Mirrors Python's `bad row {update_tree_details}`
    /// assertion.
    BadRow { key: EntryKey },
}

impl std::fmt::Display for MakeAbsentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MakeAbsentError::BlockNotFound { key } => {
                write!(f, "could not find block for {:?}", key)
            }
            MakeAbsentError::EntryNotFound { key } => {
                write!(f, "could not find entry for {:?}", key)
            }
            MakeAbsentError::UpdateBlockNotFound { key } => {
                write!(f, "could not find block for {:?}", key)
            }
            MakeAbsentError::UpdateEntryNotFound { key } => {
                write!(f, "could not find entry for {:?}", key)
            }
            MakeAbsentError::BadRow { key } => write!(f, "bad row for {:?}", key),
        }
    }
}

impl std::error::Error for MakeAbsentError {}

/// Error returned by [`split_root_dirblock_into_contents`] when the
/// pre-split dirblock layout is malformed.
#[derive(Debug, PartialEq, Eq)]
pub enum SplitRootError {
    /// Fewer than the two sentinel blocks produced by `parse_dirblocks`.
    MissingSentinels,
    /// The second sentinel block is not `(b"", [])` as expected.
    BadSecondSentinel {
        dirname: Vec<u8>,
        entry_count: usize,
    },
}

impl std::fmt::Display for SplitRootError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SplitRootError::MissingSentinels => {
                write!(f, "dirblocks missing the expected sentinel entries")
            }
            SplitRootError::BadSecondSentinel {
                dirname,
                entry_count,
            } => {
                write!(
                    f,
                    "bad dirblock start ({:?}, {} entries)",
                    dirname, entry_count
                )
            }
        }
    }
}

impl std::error::Error for SplitRootError {}

/// Error returned by [`DirState::update_entry`].
#[derive(Debug)]
pub enum UpdateEntryError {
    /// No dirstate entry matches the given key.
    EntryNotFound,
    /// The key's entry has a minikind we do not know how to refresh.
    UnexpectedKind(u8),
    /// Filesystem I/O error while reading the file contents for a
    /// sha1, reading a symlink target, or similar.
    Io(std::io::Error),
    /// Catch-all for other unexpected failures (e.g. an internal
    /// invariant violated during the post-update `ensure_block`).
    Other(String),
}

impl std::fmt::Display for UpdateEntryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            UpdateEntryError::EntryNotFound => f.write_str("update_entry: entry not found"),
            UpdateEntryError::UnexpectedKind(k) => {
                write!(f, "update_entry: unexpected minikind {:?}", k)
            }
            UpdateEntryError::Io(e) => write!(f, "update_entry: i/o error: {}", e),
            UpdateEntryError::Other(s) => write!(f, "update_entry: {}", s),
        }
    }
}

impl std::error::Error for UpdateEntryError {}

/// Seconds-since-epoch from a [`Metadata::modified`] reading.  Returns
/// 0 when the platform does not carry the information.
/// Convert a byte-encoded filesystem path into a `PathBuf`.  On unix
/// this is a zero-copy `OsString::from_vec`; on other platforms we
/// fall back to utf8 decoding.  Callers that hold a `&[u8]` from the
/// Transport contract use this to talk to `SHA1Provider::sha1` which
/// still takes a `&Path`.
fn bytes_to_path(bytes: &[u8]) -> PathBuf {
    #[cfg(unix)]
    {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;
        PathBuf::from(OsString::from_vec(bytes.to_vec()))
    }
    #[cfg(not(unix))]
    {
        PathBuf::from(String::from_utf8_lossy(bytes).into_owned())
    }
}

fn metadata_mtime_secs(m: &Metadata) -> i64 {
    m.modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// Seconds-since-epoch from the filesystem's "changed" timestamp.  On
/// Unix we read `st_ctime` directly; on other platforms we fall back
/// to `created()` which is the closest analogue.
fn metadata_ctime_secs(m: &Metadata) -> i64 {
    #[cfg(unix)]
    {
        m.ctime()
    }
    #[cfg(not(unix))]
    {
        m.created()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
    }
}

/// Error returned by [`DirState::set_path_id`]. Mirrors the exceptions
/// Python's `DirState.set_path_id` raises.
#[derive(Debug, PartialEq, Eq)]
pub enum SetPathIdError {
    /// Only `set_path_id("", new_id)` is supported — Python raises
    /// `NotImplementedError` for any non-root path.
    NonRootPath,
    /// Internal invariant violation surfaced by a helper call. Includes
    /// the MakeAbsentError / BasisApplyError description, mapped to
    /// Python's `AssertionError`.
    Internal { reason: String },
}

impl std::fmt::Display for SetPathIdError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SetPathIdError::NonRootPath => write!(f, "set_path_id only supports the root path"),
            SetPathIdError::Internal { reason } => write!(f, "internal error: {}", reason),
        }
    }
}

impl std::error::Error for SetPathIdError {}

/// Error returned by [`DirState::add`] when the requested add cannot be
/// performed. Each variant mirrors one of the exceptions Python's
/// `DirState.add` raises: the pyo3 layer translates them back.
#[derive(Debug, PartialEq, Eq)]
pub enum AddError {
    /// The file_id is already tracked at a live path. Mirrors Python's
    /// `inventory.DuplicateFileId(file_id, info)`.
    DuplicateFileId { file_id: Vec<u8>, info: String },
    /// Adding at this `(dirname, basename)` would collide with a live
    /// tree-0 row under a different file_id. Mirrors Python's
    /// `Exception("adding already added path!")`.
    AlreadyAdded { path: Vec<u8> },
    /// The parent directory is not versioned. Mirrors Python's
    /// `NotVersionedError(path, self)`.
    NotVersioned { path: Vec<u8> },
    /// An unknown kind string was supplied. Mirrors Python's
    /// `BzrFormatsError(f"unknown kind {kind!r}")`.
    UnknownKind { kind: String },
    /// The rename-from branch tried to re-add a file_id that was
    /// previously 'a' but the in-place insertion found an existing row
    /// with a non-absent tree-0 (should be unreachable post-normalisation).
    AlreadyAddedAssertion { basename: Vec<u8>, file_id: Vec<u8> },
    /// An internal invariant violation surfaced from a helper call such
    /// as [`DirState::update_minimal`] during the rename-from step.
    Internal { reason: String },
}

impl std::fmt::Display for AddError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AddError::DuplicateFileId { file_id, info } => {
                write!(f, "duplicate file_id {:?}: {}", file_id, info)
            }
            AddError::AlreadyAdded { path } => {
                write!(f, "adding already added path {:?}", path)
            }
            AddError::NotVersioned { path } => write!(f, "not versioned: {:?}", path),
            AddError::UnknownKind { kind } => write!(f, "unknown kind {:?}", kind),
            AddError::AlreadyAddedAssertion { basename, file_id } => {
                write!(f, "{:?}({:?}) already added", basename, file_id)
            }
            AddError::Internal { reason } => write!(f, "internal error: {}", reason),
        }
    }
}

impl std::error::Error for AddError {}

/// Pure-function version of [`DirState::split_root_dirblock_into_contents`].
/// Exposed so callers that are still building a `Vec<Dirblock>` outside of
/// a full `DirState` (e.g. the pyo3 shim) can reuse the same logic.
/// Split a NUL-free dirstate `dirname` on `/` into its path components.
/// Mirrors the `split_object` helper inside the Python and pyo3
/// implementations of `bisect_dirblock`; the comparison is then
/// lexicographic-by-component rather than lexicographic-by-byte, which is
/// the ordering dirblocks use on disk.
fn split_dirname(dirname: &[u8]) -> Vec<&[u8]> {
    dirname.split(|&b| b == b'/').collect()
}

/// Split `path` on the last `/` into a `(dirname, basename)` pair,
/// matching `bzrformats.osutils.split`. Paths with no `/` map to
/// `(b"", path)`; `b""` itself maps to `(b"", b"")`.
fn split_path_utf8(path: &[u8]) -> (&[u8], &[u8]) {
    match path.iter().rposition(|&b| b == b'/') {
        Some(i) => (&path[..i], &path[i + 1..]),
        None => (b"".as_slice(), path),
    }
}

/// Find the insertion position for a directory name within `dirblocks`,
/// using component-wise comparison on the dirname. Mirrors the pyo3
/// `bisect_dirblock` function in `crates/bazaar-py/src/dirstate.rs` but
/// operates on a plain `&[Dirblock]` slice rather than Python objects.
///
/// `lo` defaults to 0 (Python's default is 1, which callers pass
/// explicitly to skip the sentinel root block); we require the caller to
/// be explicit to avoid hiding the sentinel-skipping convention.
pub fn bisect_dirblock(dirblocks: &[Dirblock], dirname: &[u8], lo: usize, hi: usize) -> usize {
    let target = split_dirname(dirname);
    let mut lo = lo;
    let mut hi = hi;
    while lo < hi {
        let mid = (lo + hi) / 2;
        let cur = split_dirname(&dirblocks[mid].dirname);
        if cur < target {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

/// Find the block index containing the key's `(dirname, basename)` —
/// pure-Rust counterpart of `DirState._find_block_index_from_key`. The
/// second tuple element is `true` when the returned index actually points
/// at a block whose dirname equals `key.dirname` (i.e. the block exists),
/// and `false` when the index is the position at which a block for that
/// dirname *would* be inserted.
///
/// This function does not consult or update the `last_block_index` cache
/// Python maintains; callers that want the cache should use
/// [`DirState::find_block_index_from_key`] instead.
pub fn find_block_index_from_key(dirblocks: &[Dirblock], key: &EntryKey) -> (usize, bool) {
    // Python's fast path: `(b"", b"")` always lives in block 0.
    if key.dirname.is_empty() && key.basename.is_empty() {
        return (0, true);
    }
    // Skip the first sentinel block (index 0); `_right`-style bisect
    // over the rest matches Python's `bisect_dirblock(..., 1, ...)` call.
    let block_index = bisect_dirblock(dirblocks, &key.dirname, 1, dirblocks.len());
    let present = block_index < dirblocks.len() && dirblocks[block_index].dirname == key.dirname;
    (block_index, present)
}

/// Compare `(dirname, basename, file_id)` keys in the tuple order Python
/// uses when Python's `bisect.bisect_left(block, (key, []))` walks
/// entries. The `file_id` is the third tuple element so the ordering here
/// matches Python's native tuple comparison.
fn entry_key_cmp(a: &EntryKey, b: &EntryKey) -> Ordering {
    match a.dirname.cmp(&b.dirname) {
        Ordering::Equal => match a.basename.cmp(&b.basename) {
            Ordering::Equal => a.file_id.cmp(&b.file_id),
            other => other,
        },
        other => other,
    }
}

/// Find the entry index for `key` within `block`. Returns the insertion
/// index and whether an exact match was found. Mirrors
/// `DirState._find_entry_index` in the simpler "no cache" form —
/// Python's version also consults `self._last_entry_index` as a
/// one-slot cache, but the caching layer is additive and lives on the
/// `DirState` method wrapper.
pub fn find_entry_index(key: &EntryKey, block: &[Entry]) -> (usize, bool) {
    // bisect_left over entry keys.
    let mut lo = 0;
    let mut hi = block.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        match entry_key_cmp(&block[mid].key, key) {
            Ordering::Less => lo = mid + 1,
            _ => hi = mid,
        }
    }
    let present = lo < block.len() && block[lo].key == *key;
    (lo, present)
}

/// Result of [`DirState::get_entry_by_file_id`]. Mirrors the
/// `(entry, None)` / `None` return pattern Python uses for
/// `DirState._get_entry`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum GetEntryResult {
    /// No entry for the requested file_id exists in the given tree.
    NotFound,
    /// The located entry's key. The full entry can be re-fetched via
    /// [`DirState::find_block_index_from_key`] +
    /// [`DirState::find_entry_index`] if the caller needs the trees.
    Entry(EntryKey),
    /// An entry's tree data had a minikind that is neither live
    /// (`b'f'`/`b'd'`/`b'l'`/`b't'`) nor absent/relocated
    /// (`b'a'`/`b'r'`). Mirrors Python's `AssertionError` for the
    /// "invalid minikind" case.
    InvalidMinikind {
        key: EntryKey,
        tree_index: usize,
        minikind: u8,
    },
}

/// Result of [`get_block_entry_index`]: the four-tuple Python returns,
/// giving coordinates of where a `(dirname, basename)` pair lives — or
/// should be inserted — in the dirblocks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockEntryIndex {
    /// Block index within `dirblocks`.
    pub block_index: usize,
    /// Entry index within the block at `block_index`.
    pub entry_index: usize,
    /// `true` when the directory (i.e. a block with the target dirname)
    /// exists anywhere in the dirstate.
    pub dir_present: bool,
    /// `true` when the specific `(dirname, basename)` exists in
    /// `tree_index` with a non-absent / non-relocated entry.
    pub path_present: bool,
}

/// Pure-Rust counterpart to `DirState._get_block_entry_index`.
///
/// Walks the block for `(dirname, basename)` to find the first entry in
/// `tree_index` whose minikind is neither `b'a'` (absent) nor `b'r'`
/// (relocated). Callers use this both for membership tests and for
/// computing the insertion point when adding new entries.
pub fn get_block_entry_index(
    dirblocks: &[Dirblock],
    dirname: &[u8],
    basename: &[u8],
    tree_index: usize,
) -> BlockEntryIndex {
    let key = EntryKey {
        dirname: dirname.to_vec(),
        basename: basename.to_vec(),
        file_id: Vec::new(),
    };
    let (block_index, dir_present) = find_block_index_from_key(dirblocks, &key);
    if !dir_present {
        return BlockEntryIndex {
            block_index,
            entry_index: 0,
            dir_present: false,
            path_present: false,
        };
    }
    let block = &dirblocks[block_index].entries;
    let (mut entry_index, _) = find_entry_index(&key, block);
    // Linear scan over the contiguous run of entries sharing the same
    // (dirname, basename), skipping absent/relocated variants for the
    // requested tree. Mirrors the Python loop at dirstate.py:2254.
    while entry_index < block.len()
        && block[entry_index].key.dirname == key.dirname
        && block[entry_index].key.basename == key.basename
    {
        if let Some(tree) = block[entry_index].trees.get(tree_index) {
            if tree.minikind != b'a' && tree.minikind != b'r' {
                return BlockEntryIndex {
                    block_index,
                    entry_index,
                    dir_present: true,
                    path_present: true,
                };
            }
        }
        entry_index += 1;
    }
    BlockEntryIndex {
        block_index,
        entry_index,
        dir_present: true,
        path_present: false,
    }
}

pub fn split_root_dirblock_into_contents(dirblocks: &mut [Dirblock]) -> Result<(), SplitRootError> {
    if dirblocks.len() < 2 {
        return Err(SplitRootError::MissingSentinels);
    }
    // Python: `if self._dirblocks[1] != (b"", []): raise ValueError(...)`.
    // The second sentinel is always empty after parse_dirblocks; anything
    // else means the caller already mutated the layout.
    if !dirblocks[1].dirname.is_empty() || !dirblocks[1].entries.is_empty() {
        return Err(SplitRootError::BadSecondSentinel {
            dirname: dirblocks[1].dirname.clone(),
            entry_count: dirblocks[1].entries.len(),
        });
    }

    let block_zero = std::mem::take(&mut dirblocks[0].entries);
    let (root_entries, contents_of_root): (Vec<Entry>, Vec<Entry>) = block_zero
        .into_iter()
        .partition(|entry| entry.key.basename.is_empty());
    dirblocks[0].entries = root_entries;
    dirblocks[1].entries = contents_of_root;
    Ok(())
}

#[cfg(test)]
mod dirstate_struct_tests {
    use super::*;

    #[test]
    fn new_matches_python_defaults() {
        let state = DirState::new(
            "/tmp/.bzr/checkout/dirstate",
            Box::new(DefaultSHA1Provider::new()),
            0,
            true,
            false,
        );
        assert_eq!(state.filename, PathBuf::from("/tmp/.bzr/checkout/dirstate"));
        assert_eq!(state.header_state, MemoryState::NotInMemory);
        assert_eq!(state.dirblock_state, MemoryState::NotInMemory);
        assert!(!state.changes_aborted);
        assert!(state.dirblocks.is_empty());
        assert!(state.ghosts.is_empty());
        assert!(state.parents.is_empty());
        assert_eq!(state.end_of_header, None);
        assert_eq!(state.cutoff_time, None);
        assert_eq!(state.num_entries, 0);
        assert_eq!(state.lock_state, None);
        assert!(state.known_hash_changes.is_empty());
        assert_eq!(state.worth_saving_limit, 0);
        assert!(!state.fdatasync);
        assert!(state.use_filesystem_for_exec);
        assert_eq!(state.bisect_page_size, BISECT_PAGE_SIZE);
        assert!(state.id_index.is_none());
    }

    #[test]
    fn new_honours_overrides() {
        let state = DirState::new(
            "dirstate",
            Box::new(DefaultSHA1Provider::new()),
            -1,
            false,
            true,
        );
        assert_eq!(state.worth_saving_limit, -1);
        assert!(!state.use_filesystem_for_exec);
        assert!(state.fdatasync);
    }

    /// Build a minimal dirstate file containing just a header (no entries)
    /// by running the same `get_output_lines` / `get_parents_line` /
    /// `get_ghosts_line` helpers Python uses when writing.
    fn make_header_bytes(parents: &[&[u8]], ghosts: &[&[u8]]) -> Vec<u8> {
        let parents_line = get_parents_line(parents);
        let ghosts_line = get_ghosts_line(ghosts);
        // Matches `get_lines` with no entries: lines[0]=parents, lines[1]=ghosts.
        let lines: Vec<&[u8]> = vec![parents_line.as_slice(), ghosts_line.as_slice()];
        let chunks = get_output_lines(lines);
        chunks.into_iter().flatten().collect()
    }

    #[test]
    fn read_header_no_parents_no_ghosts() {
        let bytes = make_header_bytes(&[], &[]);
        let header = read_header(&bytes).expect("parse header");
        assert_eq!(header.num_entries, 0);
        assert!(header.parents.is_empty());
        assert!(header.ghosts.is_empty());
    }

    #[test]
    fn read_header_with_parents_and_ghosts() {
        let bytes = make_header_bytes(&[b"rev-a", b"rev-b"], &[b"ghost-1"]);
        let header = read_header(&bytes).expect("parse header");
        assert_eq!(header.parents, vec![b"rev-a".to_vec(), b"rev-b".to_vec()]);
        assert_eq!(header.ghosts, vec![b"ghost-1".to_vec()]);
    }

    /// Cross-check the reader against bytes produced by the Python side
    /// calling `get_output_lines` + `get_parents_line` + `get_ghosts_line`.
    /// Pinning the exact byte sequence guards against any drift between
    /// the reader and the (already-Rust-backed) writer.
    #[test]
    fn read_header_matches_python_generated_bytes() {
        let bytes: &[u8] = b"#bazaar dirstate flat format 3\n\
                             crc32: 2265437010\n\
                             num_entries: 0\n\
                             2\x00rev-a\x00rev-b\x00\n\
                             \x001\x00ghost-1\x00\n\x00";
        let header = read_header(bytes).expect("parse header");
        assert_eq!(header.crc_expected, 2265437010);
        assert_eq!(header.num_entries, 0);
        assert_eq!(header.parents, vec![b"rev-a".to_vec(), b"rev-b".to_vec()]);
        assert_eq!(header.ghosts, vec![b"ghost-1".to_vec()]);
    }

    #[test]
    fn read_header_populates_struct_fields() {
        let bytes = make_header_bytes(&[b"rev-a"], &[]);
        let mut state = DirState::new(
            "dirstate",
            Box::new(DefaultSHA1Provider::new()),
            0,
            true,
            false,
        );
        state.read_header(&bytes).expect("parse header");
        assert_eq!(state.header_state, MemoryState::InMemoryUnmodified);
        assert_eq!(state.parents, vec![b"rev-a".to_vec()]);
        assert!(state.ghosts.is_empty());
        assert_eq!(state.num_entries, 0);
        assert!(state.end_of_header.is_some());
    }

    #[test]
    fn read_header_rejects_wrong_format_line() {
        let bytes = b"#bazaar dirstate flat format 2\ncrc32: 0\nnum_entries: 0\n0\n\x000\n";
        match read_header(bytes) {
            Err(HeaderError::BadFormatLine(line)) => {
                assert_eq!(line, HEADER_FORMAT_2.to_vec());
            }
            other => panic!("expected BadFormatLine, got {:?}", other),
        }
    }

    #[test]
    fn read_header_rejects_missing_crc() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(HEADER_FORMAT_3);
        bytes.extend_from_slice(b"not-a-crc-line\n");
        assert!(matches!(
            read_header(&bytes),
            Err(HeaderError::MissingCrcLine(_))
        ));
    }

    #[test]
    fn read_header_rejects_bad_num_entries() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(HEADER_FORMAT_3);
        bytes.extend_from_slice(b"crc32: 0\n");
        bytes.extend_from_slice(b"num_entries: abc\n");
        bytes.extend_from_slice(b"0\n\x000\n");
        assert!(matches!(
            read_header(&bytes),
            Err(HeaderError::BadNumEntries(_))
        ));
    }

    /// Hand-built line for a single entry with one tree. Mirrors
    /// `DirState._entry_to_line` in `bzrformats/dirstate.py`: the 3 key
    /// fields followed by 5 fields per tree, all joined by NUL.
    fn entry_line(
        dirname: &[u8],
        basename: &[u8],
        file_id: &[u8],
        trees: &[(&[u8], &[u8], u64, bool, &[u8])],
    ) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(dirname);
        out.push(0);
        out.extend_from_slice(basename);
        out.push(0);
        out.extend_from_slice(file_id);
        for (minikind, fingerprint, size, executable, info) in trees {
            out.push(0);
            out.extend_from_slice(minikind);
            out.push(0);
            out.extend_from_slice(fingerprint);
            out.push(0);
            out.extend_from_slice(format!("{}", size).as_bytes());
            out.push(0);
            out.push(if *executable { b'y' } else { b'n' });
            out.push(0);
            out.extend_from_slice(info);
        }
        out
    }

    /// Build the body text (post-header) by running `get_output_lines` on a
    /// [parents, ghosts, entry_lines...] sequence and then trimming the
    /// bytes preceding the first NUL that begins the entry block.
    fn make_body_bytes(parents: &[&[u8]], ghosts: &[&[u8]], entries: &[Vec<u8>]) -> Vec<u8> {
        let parents_line = get_parents_line(parents);
        let ghosts_line = get_ghosts_line(ghosts);
        let mut lines: Vec<&[u8]> = vec![parents_line.as_slice(), ghosts_line.as_slice()];
        for e in entries {
            lines.push(e.as_slice());
        }
        let chunks = get_output_lines(lines);
        let data: Vec<u8> = chunks.into_iter().flatten().collect();
        // Locate `end_of_header` by parsing the header in the same way
        // `DirState::read_header` does, then return the remainder.
        let header = read_header(&data).expect("header parses");
        data[header.end_of_header..].to_vec()
    }

    #[test]
    fn parse_dirblocks_empty_body() {
        let blocks = parse_dirblocks(&[], 1, 0).expect("empty body parses");
        assert!(blocks.is_empty());
    }

    #[test]
    fn parse_dirblocks_single_root_entry_one_tree() {
        let nullstat = b"x".repeat(32);
        let entry = entry_line(
            b"",
            b"",
            b"TREE_ROOT",
            &[(b"d", b"", 0, false, nullstat.as_slice())],
        );
        let body = make_body_bytes(&[], &[], &[entry]);
        let blocks = parse_dirblocks(&body, 1, 1).expect("parse dirblocks");
        assert_eq!(blocks.len(), 2, "expected two sentinel blocks");
        assert_eq!(blocks[0].dirname, b"".to_vec());
        assert_eq!(blocks[1].dirname, b"".to_vec());
        assert_eq!(blocks[0].entries.len(), 1);
        let entry = &blocks[0].entries[0];
        assert_eq!(entry.key.dirname, b"".to_vec());
        assert_eq!(entry.key.basename, b"".to_vec());
        assert_eq!(entry.key.file_id, b"TREE_ROOT".to_vec());
        assert_eq!(entry.trees.len(), 1);
        let tree = &entry.trees[0];
        assert_eq!(tree.minikind, b'd');
        assert_eq!(tree.fingerprint, Vec::<u8>::new());
        assert_eq!(tree.size, 0);
        assert!(!tree.executable);
        assert_eq!(tree.packed_stat, nullstat);
    }

    #[test]
    fn parse_dirblocks_multiple_dirs_group_by_dirname() {
        let nullstat = b"x".repeat(32);
        // Three entries: root, a/file-a, b/file-b. Must be sorted by
        // `(dirname, basename)` to match what the writer produces.
        let entries = vec![
            entry_line(
                b"",
                b"",
                b"TREE_ROOT",
                &[(b"d", b"", 0, false, nullstat.as_slice())],
            ),
            entry_line(
                b"a",
                b"file-a",
                b"fid-a",
                &[(b"f", b"sha-a", 5, true, nullstat.as_slice())],
            ),
            entry_line(
                b"b",
                b"file-b",
                b"fid-b",
                &[(b"f", b"sha-b", 7, false, nullstat.as_slice())],
            ),
        ];
        let body = make_body_bytes(&[], &[], &entries);
        let blocks = parse_dirblocks(&body, 1, 3).expect("parse dirblocks");
        // Two sentinels plus two real dir blocks.
        assert_eq!(blocks.len(), 4);
        assert_eq!(blocks[0].dirname, b"".to_vec());
        assert_eq!(blocks[0].entries.len(), 1);
        assert_eq!(blocks[1].dirname, b"".to_vec());
        assert_eq!(blocks[1].entries.len(), 0);
        assert_eq!(blocks[2].dirname, b"a".to_vec());
        assert_eq!(blocks[2].entries.len(), 1);
        assert_eq!(blocks[2].entries[0].key.basename, b"file-a".to_vec());
        assert!(blocks[2].entries[0].trees[0].executable);
        assert_eq!(blocks[2].entries[0].trees[0].size, 5);
        assert_eq!(blocks[3].dirname, b"b".to_vec());
        assert_eq!(blocks[3].entries.len(), 1);
        assert_eq!(blocks[3].entries[0].trees[0].size, 7);
        assert!(!blocks[3].entries[0].trees[0].executable);
    }

    #[test]
    fn parse_dirblocks_rejects_wrong_entry_count() {
        let nullstat = b"x".repeat(32);
        let entry = entry_line(
            b"",
            b"",
            b"TREE_ROOT",
            &[(b"d", b"", 0, false, nullstat.as_slice())],
        );
        let body = make_body_bytes(&[], &[], &[entry]);
        // Header claimed 2 entries but body only has 1.
        match parse_dirblocks(&body, 1, 2) {
            Err(DirblocksError::WrongEntryCount {
                expected: 2,
                actual: 1,
            }) => {}
            other => panic!("expected WrongEntryCount, got {:?}", other),
        }
    }

    #[test]
    fn parse_dirblocks_multi_tree() {
        let nullstat = b"x".repeat(32);
        // Two trees per entry: current + one parent.
        let entry = entry_line(
            b"",
            b"README",
            b"file-id-1",
            &[
                (b"f", b"sha-current", 10, true, nullstat.as_slice()),
                (b"f", b"sha-parent", 8, false, nullstat.as_slice()),
            ],
        );
        let body = make_body_bytes(&[b"rev-a"], &[], &[entry]);
        let blocks = parse_dirblocks(&body, 2, 1).expect("parse");
        assert_eq!(blocks.len(), 2);
        let e = &blocks[0].entries[0];
        assert_eq!(e.trees.len(), 2);
        assert_eq!(e.trees[0].fingerprint, b"sha-current".to_vec());
        assert_eq!(e.trees[0].size, 10);
        assert!(e.trees[0].executable);
        assert_eq!(e.trees[1].fingerprint, b"sha-parent".to_vec());
        assert_eq!(e.trees[1].size, 8);
        assert!(!e.trees[1].executable);
    }

    /// Cross-check against bytes produced by a full
    /// `DirState.initialize(...); _set_data(...); save()` cycle. Pinning
    /// the exact on-disk representation guards against any future drift
    /// between the writer and the new Rust reader.
    #[test]
    fn parse_dirblocks_matches_python_saved_file() {
        let bytes: &[u8] = b"#bazaar dirstate flat format 3\n\
                             crc32: 2823629280\n\
                             num_entries: 1\n\
                             0\x00\n\
                             \x000\x00\n\
                             \x00\x00\x00TREE_ROOT\x00d\x00\x000\x00n\x00xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\x00\n\x00";
        let header = read_header(bytes).expect("parse header");
        assert_eq!(header.num_entries, 1);
        assert!(header.parents.is_empty());
        assert!(header.ghosts.is_empty());
        let body = &bytes[header.end_of_header..];
        let blocks = parse_dirblocks(body, 1, header.num_entries).expect("parse body");
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].entries.len(), 1);
        let entry = &blocks[0].entries[0];
        assert_eq!(entry.key.file_id, b"TREE_ROOT".to_vec());
        assert_eq!(entry.trees[0].minikind, b'd');
        assert_eq!(entry.trees[0].packed_stat, b"x".repeat(32));
    }

    fn make_entry(dirname: &[u8], basename: &[u8], file_id: &[u8]) -> Entry {
        Entry {
            key: EntryKey {
                dirname: dirname.to_vec(),
                basename: basename.to_vec(),
                file_id: file_id.to_vec(),
            },
            trees: vec![TreeData {
                minikind: b'f',
                fingerprint: Vec::new(),
                size: 0,
                executable: false,
                packed_stat: b"x".repeat(32),
            }],
        }
    }

    #[test]
    fn split_root_dirblock_separates_root_from_contents() {
        let mut dirblocks = vec![
            Dirblock {
                dirname: Vec::new(),
                entries: vec![
                    make_entry(b"", b"", b"TREE_ROOT"),
                    make_entry(b"", b"README", b"fid-readme"),
                    make_entry(b"", b"CONTRIBUTING", b"fid-contrib"),
                ],
            },
            Dirblock {
                dirname: Vec::new(),
                entries: Vec::new(),
            },
        ];
        split_root_dirblock_into_contents(&mut dirblocks).expect("split");
        assert_eq!(dirblocks.len(), 2);
        assert_eq!(dirblocks[0].entries.len(), 1);
        assert_eq!(dirblocks[0].entries[0].key.file_id, b"TREE_ROOT".to_vec());
        assert_eq!(dirblocks[1].entries.len(), 2);
        assert_eq!(dirblocks[1].entries[0].key.basename, b"README".to_vec());
        assert_eq!(
            dirblocks[1].entries[1].key.basename,
            b"CONTRIBUTING".to_vec()
        );
    }

    #[test]
    fn split_root_dirblock_preserves_order_within_partitions() {
        // The partition step must keep the original relative order in both
        // halves — Python's implementation walks `_dirblocks[0][1]` in
        // order and appends to two separate lists.
        let mut dirblocks = vec![
            Dirblock {
                dirname: Vec::new(),
                entries: vec![
                    make_entry(b"", b"a", b"fid-a"),
                    make_entry(b"", b"", b"TREE_ROOT"),
                    make_entry(b"", b"b", b"fid-b"),
                ],
            },
            Dirblock {
                dirname: Vec::new(),
                entries: Vec::new(),
            },
        ];
        split_root_dirblock_into_contents(&mut dirblocks).expect("split");
        assert_eq!(dirblocks[0].entries.len(), 1);
        assert_eq!(dirblocks[0].entries[0].key.file_id, b"TREE_ROOT".to_vec());
        assert_eq!(dirblocks[1].entries.len(), 2);
        assert_eq!(dirblocks[1].entries[0].key.basename, b"a".to_vec());
        assert_eq!(dirblocks[1].entries[1].key.basename, b"b".to_vec());
    }

    #[test]
    fn split_root_dirblock_leaves_later_blocks_alone() {
        let mut dirblocks = vec![
            Dirblock {
                dirname: Vec::new(),
                entries: vec![make_entry(b"", b"", b"TREE_ROOT")],
            },
            Dirblock {
                dirname: Vec::new(),
                entries: Vec::new(),
            },
            Dirblock {
                dirname: b"subdir".to_vec(),
                entries: vec![make_entry(b"subdir", b"file", b"fid-s")],
            },
        ];
        split_root_dirblock_into_contents(&mut dirblocks).expect("split");
        assert_eq!(dirblocks.len(), 3);
        assert_eq!(dirblocks[2].dirname, b"subdir".to_vec());
        assert_eq!(dirblocks[2].entries.len(), 1);
    }

    #[test]
    fn split_root_dirblock_rejects_missing_sentinel() {
        let mut dirblocks = vec![Dirblock {
            dirname: Vec::new(),
            entries: Vec::new(),
        }];
        assert_eq!(
            split_root_dirblock_into_contents(&mut dirblocks),
            Err(SplitRootError::MissingSentinels)
        );
    }

    #[test]
    fn split_root_dirblock_rejects_polluted_sentinel() {
        let mut dirblocks = vec![
            Dirblock {
                dirname: Vec::new(),
                entries: vec![make_entry(b"", b"", b"TREE_ROOT")],
            },
            Dirblock {
                dirname: Vec::new(),
                entries: vec![make_entry(b"", b"x", b"fid-x")],
            },
        ];
        match split_root_dirblock_into_contents(&mut dirblocks) {
            Err(SplitRootError::BadSecondSentinel {
                dirname,
                entry_count,
            }) => {
                assert!(dirname.is_empty());
                assert_eq!(entry_count, 1);
            }
            other => panic!("expected BadSecondSentinel, got {:?}", other),
        }
    }

    fn dirblock_with_entries(dirname: &[u8], entries: Vec<Entry>) -> Dirblock {
        Dirblock {
            dirname: dirname.to_vec(),
            entries,
        }
    }

    /// Build the canonical two-sentinel-plus-real-blocks layout used by
    /// the lookup tests. `subdirs` is a list of `(dirname, entries)`
    /// pairs that become real blocks after the sentinels.
    fn make_dirblocks(subdirs: Vec<(&[u8], Vec<Entry>)>) -> Vec<Dirblock> {
        let mut blocks = vec![
            dirblock_with_entries(b"", Vec::new()),
            dirblock_with_entries(b"", Vec::new()),
        ];
        for (dirname, entries) in subdirs {
            blocks.push(dirblock_with_entries(dirname, entries));
        }
        blocks
    }

    fn tree(minikind: u8) -> TreeData {
        TreeData {
            minikind,
            fingerprint: Vec::new(),
            size: 0,
            executable: false,
            packed_stat: b"x".repeat(32),
        }
    }

    fn entry_with_trees(
        dirname: &[u8],
        basename: &[u8],
        file_id: &[u8],
        trees: Vec<TreeData>,
    ) -> Entry {
        Entry {
            key: EntryKey {
                dirname: dirname.to_vec(),
                basename: basename.to_vec(),
                file_id: file_id.to_vec(),
            },
            trees,
        }
    }

    #[test]
    fn bisect_dirblock_component_order_not_byte_order() {
        // Component-wise ordering: `a/b` splits to ["a", "b"] which is
        // less than ["a-b"] because the first-element comparison of "a"
        // and "a-b" treats "a" as a prefix of "a-b". A pure byte sort
        // would place "a-b" before "a/b" (0x2d < 0x2f), so this test
        // pins the path-component-aware behaviour.
        // Sorted input: ["a", "a/b", "a-b", "b"].
        let blocks = make_dirblocks(vec![
            (b"a", vec![]),
            (b"a/b", vec![]),
            (b"a-b", vec![]),
            (b"b", vec![]),
        ]);
        // 2 sentinels + 4 real. lo=1 skips the first sentinel (matching
        // Python's bisect_dirblock(..., 1, hi) idiom), hi=len.
        assert_eq!(bisect_dirblock(&blocks, b"a", 1, blocks.len()), 2);
        assert_eq!(bisect_dirblock(&blocks, b"a/b", 1, blocks.len()), 3);
        assert_eq!(bisect_dirblock(&blocks, b"a-b", 1, blocks.len()), 4);
        assert_eq!(bisect_dirblock(&blocks, b"b", 1, blocks.len()), 5);
        // Insertion for a missing dirname: "aa" > "a-b" byte-wise in
        // single-component form, so it lands after "a-b" (index 4) at
        // index 5, which is also the slot for "b".
        assert_eq!(bisect_dirblock(&blocks, b"aa", 1, blocks.len()), 5);
    }

    /// Build dirblocks from a list of sorted paths and, for each path,
    /// assert that `bisect_dirblock` agrees with a manual `bisect_left`
    /// over the split-by-`/` form. Mirrors `assertBisect` from the
    /// Python `TestBisectDirblock` test class.
    fn assert_bisect_matches_bisect_left(paths: &[&[u8]]) {
        // Verify the caller's list is actually sorted component-wise
        // (matches Python's `assertEqual(sorted(split_dirblocks), split_dirblocks)`).
        let split: Vec<Vec<&[u8]>> = paths.iter().map(|p| split_dirname(p)).collect();
        let mut sorted = split.clone();
        sorted.sort();
        assert_eq!(split, sorted, "test input paths are not sorted");

        let blocks: Vec<Dirblock> = paths
            .iter()
            .map(|p| Dirblock {
                dirname: p.to_vec(),
                entries: Vec::new(),
            })
            .collect();

        for probe in paths {
            let got = bisect_dirblock(&blocks, probe, 0, blocks.len());
            let probe_split = split_dirname(probe);
            let expected = split.partition_point(|s| *s < probe_split);
            assert_eq!(
                got, expected,
                "bisect_dirblock disagreed for {:?}: got {}, expected {}",
                probe, got, expected,
            );
        }
    }

    /// Rust counterpart of Python `TestBisectDirblock.test_simple`.
    #[test]
    fn bisect_dirblock_simple() {
        let paths: Vec<&[u8]> = vec![b"", b"a", b"b", b"c", b"d"];
        assert_bisect_matches_bisect_left(&paths);
    }

    /// Rust counterpart of Python `TestBisectDirblock.test_involved`.
    /// The pure-Rust `bisect_dirblock` does not have a `cache` parameter
    /// (Python's `_split_path_cache` only speeds up repeated lookups and
    /// does not affect results), so Python's `test_involved` and
    /// `test_involved_cached` collapse into a single Rust test over the
    /// same input.
    #[test]
    fn bisect_dirblock_involved() {
        let paths: Vec<&[u8]> = vec![
            b"", b"a", b"a/a", b"a/a/a", b"a/a/z", b"a/a-a", b"a/a-z", b"a/z", b"a/z/a", b"a/z/z",
            b"a/z-a", b"a/z-z", b"a-a", b"a-z", b"z", b"z/a/a", b"z/a/z", b"z/a-a", b"z/a-z",
            b"z/z", b"z/z/a", b"z/z/z", b"z/z-a", b"z/z-z", b"z-a", b"z-z",
        ];
        assert_bisect_matches_bisect_left(&paths);
    }

    #[test]
    fn find_block_index_from_key_root_fast_path() {
        let blocks = make_dirblocks(vec![(b"sub", vec![])]);
        let key = EntryKey {
            dirname: b"".to_vec(),
            basename: b"".to_vec(),
            file_id: b"TREE_ROOT".to_vec(),
        };
        assert_eq!(find_block_index_from_key(&blocks, &key), (0, true));
    }

    #[test]
    fn find_block_index_from_key_hit_and_miss() {
        let blocks = make_dirblocks(vec![(b"a", vec![]), (b"c", vec![])]);
        let hit = EntryKey {
            dirname: b"a".to_vec(),
            basename: b"foo".to_vec(),
            file_id: b"".to_vec(),
        };
        assert_eq!(find_block_index_from_key(&blocks, &hit), (2, true));
        let miss = EntryKey {
            dirname: b"b".to_vec(),
            basename: b"foo".to_vec(),
            file_id: b"".to_vec(),
        };
        // "b" would be inserted between "a" (index 2) and "c" (index 3).
        assert_eq!(find_block_index_from_key(&blocks, &miss), (3, false));
    }

    #[test]
    fn find_entry_index_exact_and_insertion() {
        let block = vec![
            entry_with_trees(b"dir", b"a", b"fid-a", vec![tree(b'f')]),
            entry_with_trees(b"dir", b"b", b"fid-b", vec![tree(b'f')]),
            entry_with_trees(b"dir", b"c", b"fid-c", vec![tree(b'f')]),
        ];
        let hit = EntryKey {
            dirname: b"dir".to_vec(),
            basename: b"b".to_vec(),
            file_id: b"fid-b".to_vec(),
        };
        assert_eq!(find_entry_index(&hit, &block), (1, true));
        let miss_before = EntryKey {
            dirname: b"dir".to_vec(),
            basename: b"ab".to_vec(),
            file_id: b"".to_vec(),
        };
        assert_eq!(find_entry_index(&miss_before, &block), (1, false));
        let miss_end = EntryKey {
            dirname: b"dir".to_vec(),
            basename: b"z".to_vec(),
            file_id: b"".to_vec(),
        };
        assert_eq!(find_entry_index(&miss_end, &block), (3, false));
    }

    #[test]
    fn get_block_entry_index_finds_live_entry() {
        let blocks = make_dirblocks(vec![(
            b"dir",
            vec![entry_with_trees(b"dir", b"a", b"fid-a", vec![tree(b'f')])],
        )]);
        let bei = get_block_entry_index(&blocks, b"dir", b"a", 0);
        assert_eq!(bei.block_index, 2);
        assert_eq!(bei.entry_index, 0);
        assert!(bei.dir_present);
        assert!(bei.path_present);
    }

    #[test]
    fn get_block_entry_index_absent_dir() {
        let blocks = make_dirblocks(vec![(b"a", vec![])]);
        let bei = get_block_entry_index(&blocks, b"missing", b"file", 0);
        assert!(!bei.dir_present);
        assert!(!bei.path_present);
    }

    #[test]
    fn get_block_entry_index_skips_absent_and_relocated() {
        // Two entries at (dir, a): the first is absent in tree 0, the
        // second is live. Python walks the contiguous run so the live
        // one should be returned.
        let blocks = make_dirblocks(vec![(
            b"dir",
            vec![
                entry_with_trees(b"dir", b"a", b"fid-absent", vec![tree(b'a')]),
                entry_with_trees(b"dir", b"a", b"fid-live", vec![tree(b'f')]),
            ],
        )]);
        let bei = get_block_entry_index(&blocks, b"dir", b"a", 0);
        assert!(bei.path_present);
        assert_eq!(bei.entry_index, 1);
        assert_eq!(
            blocks[bei.block_index].entries[bei.entry_index].key.file_id,
            b"fid-live".to_vec()
        );
    }

    #[test]
    fn get_block_entry_index_all_absent_returns_not_present() {
        let blocks = make_dirblocks(vec![(
            b"dir",
            vec![
                entry_with_trees(b"dir", b"a", b"fid-1", vec![tree(b'a')]),
                entry_with_trees(b"dir", b"a", b"fid-2", vec![tree(b'r')]),
            ],
        )]);
        let bei = get_block_entry_index(&blocks, b"dir", b"a", 0);
        assert!(bei.dir_present);
        assert!(!bei.path_present);
        assert_eq!(bei.entry_index, 2);
    }

    /// Packed_stat constant matching Python's test fixtures.
    const PACKED_STAT: &[u8] = b"AAAAREUHaIpFB2iKAAADAQAtkqUAAIGk";
    /// Null-sha matching Python's test fixtures.
    const NULL_SHA: &[u8] = b"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";

    fn stat_tree(minikind: u8) -> TreeData {
        TreeData {
            minikind,
            fingerprint: Vec::new(),
            size: 0,
            executable: false,
            packed_stat: PACKED_STAT.to_vec(),
        }
    }

    fn file_tree(size: u64) -> TreeData {
        TreeData {
            minikind: b'f',
            fingerprint: NULL_SHA.to_vec(),
            size,
            executable: false,
            packed_stat: PACKED_STAT.to_vec(),
        }
    }

    /// Rust mirror of Python's `create_dirstate_with_root_and_subdir`:
    /// a root entry plus a single `subdir` entry in the contents-of-root
    /// block. Used by `TestGetBlockRowIndex.test_simple_structure`.
    fn create_dirstate_with_root_and_subdir() -> DirState {
        let mut state = fresh_state();
        state.dirblocks = vec![
            Dirblock {
                dirname: Vec::new(),
                entries: vec![entry_with_trees(
                    b"",
                    b"",
                    b"a-root-value",
                    vec![stat_tree(b'd')],
                )],
            },
            Dirblock {
                dirname: Vec::new(),
                entries: vec![entry_with_trees(
                    b"",
                    b"subdir",
                    b"subdir-id",
                    vec![stat_tree(b'd')],
                )],
            },
        ];
        state
    }

    /// Rust mirror of Python's `create_complex_dirstate`. Matches the
    /// docstring in test_dirstate.py: root + directories a/ and b/, files
    /// c and d, a/e (empty dir), a/f, b/g, b/h\xc3\xa5.
    fn create_complex_dirstate() -> DirState {
        let mut state = fresh_state();
        state.dirblocks = vec![
            // Block 0: root entry.
            Dirblock {
                dirname: Vec::new(),
                entries: vec![entry_with_trees(
                    b"",
                    b"",
                    b"a-root-value",
                    vec![stat_tree(b'd')],
                )],
            },
            // Block 1: contents of root — a, b (both dirs), c, d (files).
            Dirblock {
                dirname: Vec::new(),
                entries: vec![
                    entry_with_trees(b"", b"a", b"a-dir", vec![stat_tree(b'd')]),
                    entry_with_trees(b"", b"b", b"b-dir", vec![stat_tree(b'd')]),
                    entry_with_trees(b"", b"c", b"c-file", vec![file_tree(10)]),
                    entry_with_trees(b"", b"d", b"d-file", vec![file_tree(20)]),
                ],
            },
            // Block 2: inside a/ — e (dir), f (file).
            Dirblock {
                dirname: b"a".to_vec(),
                entries: vec![
                    entry_with_trees(b"a", b"e", b"e-dir", vec![stat_tree(b'd')]),
                    entry_with_trees(b"a", b"f", b"f-file", vec![file_tree(30)]),
                ],
            },
            // Block 3: inside b/ — g, h\xc3\xa5 (file with non-ASCII name).
            Dirblock {
                dirname: b"b".to_vec(),
                entries: vec![
                    entry_with_trees(b"b", b"g", b"g-file", vec![file_tree(30)]),
                    entry_with_trees(b"b", b"h\xc3\xa5", b"h-\xc3\xa5-file", vec![file_tree(40)]),
                ],
            },
        ];
        state
    }

    /// Rust counterpart of Python
    /// `TestGetBlockRowIndex.test_simple_structure`.
    /// Rust mirror of Python's `create_dirstate_with_two_trees` fixture
    /// used by `TestIterChildEntries`. Two trees per row; the tree at
    /// index 1 is a pretend parent revision with a few differences from
    /// the working tree (b/g absent, b/h with a different file id,
    /// b/i new, c renamed to b/j).
    fn create_dirstate_with_two_trees() -> DirState {
        let mut state = fresh_state();
        state.parents = vec![b"parent".to_vec()];
        let stat_current = TreeData {
            minikind: b'd',
            fingerprint: Vec::new(),
            size: 0,
            executable: false,
            packed_stat: PACKED_STAT.to_vec(),
        };
        let stat_parent_dir = TreeData {
            minikind: b'd',
            fingerprint: Vec::new(),
            size: 0,
            executable: false,
            packed_stat: b"parent-revid".to_vec(),
        };
        let null_parent = TreeData {
            minikind: b'a',
            fingerprint: Vec::new(),
            size: 0,
            executable: false,
            packed_stat: Vec::new(),
        };
        let file_cur = |size: u64| TreeData {
            minikind: b'f',
            fingerprint: NULL_SHA.to_vec(),
            size,
            executable: false,
            packed_stat: PACKED_STAT.to_vec(),
        };
        let file_parent = |fingerprint: &[u8], size: u64| TreeData {
            minikind: b'f',
            fingerprint: fingerprint.to_vec(),
            size,
            executable: false,
            packed_stat: b"parent-revid".to_vec(),
        };
        let relocated = |to: &[u8]| TreeData {
            minikind: b'r',
            fingerprint: to.to_vec(),
            size: 0,
            executable: false,
            packed_stat: Vec::new(),
        };

        state.dirblocks = vec![
            Dirblock {
                dirname: Vec::new(),
                entries: vec![Entry {
                    key: EntryKey {
                        dirname: b"".to_vec(),
                        basename: b"".to_vec(),
                        file_id: b"a-root-value".to_vec(),
                    },
                    trees: vec![stat_current.clone(), stat_parent_dir.clone()],
                }],
            },
            Dirblock {
                dirname: Vec::new(),
                entries: vec![
                    Entry {
                        key: EntryKey {
                            dirname: b"".to_vec(),
                            basename: b"a".to_vec(),
                            file_id: b"a-dir".to_vec(),
                        },
                        trees: vec![stat_current.clone(), stat_parent_dir.clone()],
                    },
                    Entry {
                        key: EntryKey {
                            dirname: b"".to_vec(),
                            basename: b"b".to_vec(),
                            file_id: b"b-dir".to_vec(),
                        },
                        trees: vec![stat_current.clone(), stat_parent_dir.clone()],
                    },
                    Entry {
                        key: EntryKey {
                            dirname: b"".to_vec(),
                            basename: b"c".to_vec(),
                            file_id: b"c-file".to_vec(),
                        },
                        trees: vec![file_cur(10), relocated(b"b/j")],
                    },
                    Entry {
                        key: EntryKey {
                            dirname: b"".to_vec(),
                            basename: b"d".to_vec(),
                            file_id: b"d-file".to_vec(),
                        },
                        trees: vec![file_cur(20), file_parent(b"d", 20)],
                    },
                ],
            },
            Dirblock {
                dirname: b"a".to_vec(),
                entries: vec![
                    Entry {
                        key: EntryKey {
                            dirname: b"a".to_vec(),
                            basename: b"e".to_vec(),
                            file_id: b"e-dir".to_vec(),
                        },
                        trees: vec![stat_current.clone(), stat_parent_dir.clone()],
                    },
                    Entry {
                        key: EntryKey {
                            dirname: b"a".to_vec(),
                            basename: b"f".to_vec(),
                            file_id: b"f-file".to_vec(),
                        },
                        trees: vec![file_cur(30), file_parent(b"f", 20)],
                    },
                ],
            },
            Dirblock {
                dirname: b"b".to_vec(),
                entries: vec![
                    Entry {
                        key: EntryKey {
                            dirname: b"b".to_vec(),
                            basename: b"g".to_vec(),
                            file_id: b"g-file".to_vec(),
                        },
                        trees: vec![file_cur(30), null_parent.clone()],
                    },
                    Entry {
                        key: EntryKey {
                            dirname: b"b".to_vec(),
                            basename: b"h\xc3\xa5".to_vec(),
                            file_id: b"h-\xc3\xa5-file1".to_vec(),
                        },
                        trees: vec![file_cur(40), null_parent.clone()],
                    },
                    Entry {
                        key: EntryKey {
                            dirname: b"b".to_vec(),
                            basename: b"h\xc3\xa5".to_vec(),
                            file_id: b"h-\xc3\xa5-file2".to_vec(),
                        },
                        trees: vec![null_parent.clone(), file_parent(b"h", 20)],
                    },
                    Entry {
                        key: EntryKey {
                            dirname: b"b".to_vec(),
                            basename: b"i".to_vec(),
                            file_id: b"i-file".to_vec(),
                        },
                        trees: vec![null_parent.clone(), file_parent(b"h", 20)],
                    },
                    Entry {
                        key: EntryKey {
                            dirname: b"b".to_vec(),
                            basename: b"j".to_vec(),
                            file_id: b"c-file".to_vec(),
                        },
                        trees: vec![relocated(b"c"), file_parent(b"j", 20)],
                    },
                ],
            },
        ];
        state
    }

    /// Rust counterpart of Python
    /// `TestIterChildEntries.test_iter_children_b`. Walks the b/
    /// subtree in tree_index=1 (the parent revision) and expects to
    /// see the live entries h2, i, and j (in that order).
    #[test]
    fn iter_child_entries_children_b_tree_one() {
        let state = create_dirstate_with_two_trees();
        let children = state.iter_child_entries(1, b"b");
        let basenames: Vec<&[u8]> = children.iter().map(|e| e.key.basename.as_slice()).collect();
        let file_ids: Vec<&[u8]> = children.iter().map(|e| e.key.file_id.as_slice()).collect();
        // h2 and i share the basename "h\xc3\xa5" and "i"; distinguish
        // by file id so the test pins the exact row.
        assert_eq!(basenames, vec![&b"h\xc3\xa5"[..], b"i", b"j"]);
        assert_eq!(
            file_ids,
            vec![&b"h-\xc3\xa5-file2"[..], b"i-file", b"c-file"]
        );
    }

    /// Rust counterpart of Python
    /// `TestIterChildEntries.test_iter_child_root`. Walks the whole
    /// tree in tree_index=1 and expects: a, b, d (c is relocated so
    /// absent from this tree), then e, f from a/, then h2, i, j from
    /// b/.
    #[test]
    fn iter_child_entries_root_tree_one() {
        let state = create_dirstate_with_two_trees();
        let children = state.iter_child_entries(1, b"");
        let basenames: Vec<&[u8]> = children.iter().map(|e| e.key.basename.as_slice()).collect();
        let expected: Vec<&[u8]> = vec![b"a", b"b", b"d", b"e", b"f", b"h\xc3\xa5", b"i", b"j"];
        assert_eq!(basenames, expected);
    }

    #[test]
    fn iter_child_entries_non_directory_returns_empty() {
        let state = create_complex_dirstate();
        // "c" is a file, not a directory — iter_child_entries of a
        // non-directory path yields nothing.
        let children = state.iter_child_entries(0, b"c");
        assert!(children.is_empty());
    }

    #[test]
    fn split_path_utf8_matches_osutils_split() {
        assert_eq!(split_path_utf8(b"a/b/c"), (&b"a/b"[..], &b"c"[..]));
        assert_eq!(split_path_utf8(b"a"), (&b""[..], &b"a"[..]));
        assert_eq!(split_path_utf8(b""), (&b""[..], &b""[..]));
        assert_eq!(split_path_utf8(b"a/"), (&b"a"[..], &b""[..]));
    }

    /// Rust counterpart of Python
    /// `TestGetEntry.test_simple_structure`. Probe a small dirstate by
    /// path and verify the expected (dirname, basename, file_id) key
    /// comes back — or `None` for paths that don't exist or live under
    /// a non-existent directory.
    #[test]
    fn maybe_remove_row_keeps_row_with_any_live_tree() {
        let mut entries = vec![
            entry_with_trees(b"", b"a", b"fid-a", vec![tree(b'f'), tree(b'a')]),
            entry_with_trees(b"", b"b", b"fid-b", vec![tree(b'a'), tree(b'a')]),
        ];
        let mut id_index = IdIndex::new();
        let fid_a = FileId::from(&b"fid-a".to_vec());
        id_index.add((b"", b"a", &fid_a));

        let removed = DirState::maybe_remove_row(&mut entries, 0, &mut id_index);
        assert!(!removed);
        assert_eq!(entries.len(), 2);
        assert_eq!(id_index.get(&fid_a).len(), 1);
    }

    #[test]
    fn maybe_remove_row_drops_row_when_all_trees_dead() {
        let mut entries = vec![
            entry_with_trees(b"", b"a", b"fid-a", vec![tree(b'f')]),
            entry_with_trees(b"", b"b", b"fid-b", vec![tree(b'a'), tree(b'r')]),
        ];
        let mut id_index = IdIndex::new();
        let fid_a = FileId::from(&b"fid-a".to_vec());
        let fid_b = FileId::from(&b"fid-b".to_vec());
        id_index.add((b"", b"a", &fid_a));
        id_index.add((b"", b"b", &fid_b));

        let removed = DirState::maybe_remove_row(&mut entries, 1, &mut id_index);
        assert!(removed);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].key.file_id, b"fid-a".to_vec());
        // fid_b was dropped from the index.
        assert!(id_index.get(&fid_b).is_empty());
        // fid_a is still indexed.
        assert_eq!(id_index.get(&fid_a).len(), 1);
    }

    #[test]
    fn sort_entries_orders_by_dirname_basename_file_id() {
        // Shuffled input covering root, shallow file, nested file, and
        // deeper nested file; we expect canonical dirblock order on the
        // way out.
        let mut entries = vec![
            make_entry(b"a", b"e", b"fid-e"),
            make_entry(b"", b"", b"TREE_ROOT"),
            make_entry(b"b", b"g", b"fid-g"),
            make_entry(b"", b"a", b"fid-a"),
            make_entry(b"a", b"f", b"fid-f"),
        ];
        DirState::sort_entries(&mut entries);
        let keys: Vec<(&[u8], &[u8], &[u8])> = entries
            .iter()
            .map(|e| {
                (
                    e.key.dirname.as_slice(),
                    e.key.basename.as_slice(),
                    e.key.file_id.as_slice(),
                )
            })
            .collect();
        assert_eq!(
            keys,
            vec![
                (b"".as_slice(), b"".as_slice(), b"TREE_ROOT".as_slice()),
                (b"".as_slice(), b"a".as_slice(), b"fid-a".as_slice()),
                (b"a".as_slice(), b"e".as_slice(), b"fid-e".as_slice()),
                (b"a".as_slice(), b"f".as_slice(), b"fid-f".as_slice()),
                (b"b".as_slice(), b"g".as_slice(), b"fid-g".as_slice()),
            ]
        );
    }

    #[test]
    fn sort_entries_breaks_basename_ties_on_file_id() {
        // Same (dirname, basename) with different file ids.
        let mut entries = vec![
            make_entry(b"a", b"e", b"fid-z"),
            make_entry(b"a", b"e", b"fid-a"),
        ];
        DirState::sort_entries(&mut entries);
        assert_eq!(entries[0].key.file_id, b"fid-a".to_vec());
        assert_eq!(entries[1].key.file_id, b"fid-z".to_vec());
    }

    #[test]
    fn sort_entries_split_ordering_differs_from_raw_bytes() {
        // Python's `_sort_entries` splits the dirname on '/' before
        // comparing, which is the whole point of the port: a purely
        // byte-wise sort would put `"a-b"` before `"a/..."` because
        // `'-' < '/'`, while the split-based sort puts them *after*
        // every entry under `"a"`.
        let mut entries = vec![
            make_entry(b"a-b", b"x", b"fid-x"),
            make_entry(b"a/c", b"y", b"fid-y"),
            make_entry(b"a", b"z", b"fid-z"),
        ];
        DirState::sort_entries(&mut entries);
        let dirnames: Vec<&[u8]> = entries.iter().map(|e| e.key.dirname.as_slice()).collect();
        assert_eq!(
            dirnames,
            vec![b"a".as_slice(), b"a/c".as_slice(), b"a-b".as_slice()]
        );
    }

    #[test]
    fn entries_for_path_returns_all_rows_at_path() {
        let state = create_complex_dirstate();
        let rows = state.entries_for_path(b"a/e");
        assert!(!rows.is_empty());
        for row in &rows {
            assert_eq!(row.key.dirname, b"a".to_vec());
            assert_eq!(row.key.basename, b"e".to_vec());
        }
        // At least the direct entry should be present.
        assert!(rows.iter().any(|r| r.key.file_id == b"e-dir".to_vec()));
    }

    #[test]
    fn entries_for_path_root_returns_root_row() {
        let state = create_complex_dirstate();
        let rows = state.entries_for_path(b"");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].key.dirname, b"".to_vec());
        assert_eq!(rows[0].key.basename, b"".to_vec());
        assert_eq!(rows[0].key.file_id, b"a-root-value".to_vec());
    }

    #[test]
    fn entries_for_path_missing_directory_returns_empty() {
        let state = create_complex_dirstate();
        assert!(state.entries_for_path(b"nosuchdir/nope").is_empty());
    }

    #[test]
    fn entries_for_path_missing_basename_returns_empty() {
        let state = create_complex_dirstate();
        assert!(state.entries_for_path(b"a/nope").is_empty());
    }

    #[test]
    fn get_entry_by_path_simple_structure() {
        let state = create_dirstate_with_root_and_subdir();
        let root = state.get_entry_by_path(0, b"").expect("root");
        assert_eq!(root.key.file_id, b"a-root-value".to_vec());
        let subdir = state.get_entry_by_path(0, b"subdir").expect("subdir");
        assert_eq!(subdir.key.basename, b"subdir".to_vec());
        assert_eq!(subdir.key.file_id, b"subdir-id".to_vec());
        assert!(state.get_entry_by_path(0, b"missing").is_none());
        assert!(state.get_entry_by_path(0, b"missing/foo").is_none());
        assert!(state.get_entry_by_path(0, b"subdir/foo").is_none());
    }

    /// Rust counterpart of Python
    /// `TestGetEntry.test_complex_structure_exists`.
    #[test]
    fn get_entry_by_path_complex_structure_exists() {
        let state = create_complex_dirstate();
        let cases: &[(&[u8], &[u8], &[u8], &[u8])] = &[
            (b"", b"", b"", b"a-root-value"),
            (b"a", b"", b"a", b"a-dir"),
            (b"b", b"", b"b", b"b-dir"),
            (b"c", b"", b"c", b"c-file"),
            (b"d", b"", b"d", b"d-file"),
            (b"a/e", b"a", b"e", b"e-dir"),
            (b"a/f", b"a", b"f", b"f-file"),
            (b"b/g", b"b", b"g", b"g-file"),
            (b"b/h\xc3\xa5", b"b", b"h\xc3\xa5", b"h-\xc3\xa5-file"),
        ];
        for (path, dirname, basename, file_id) in cases {
            let entry = state
                .get_entry_by_path(0, path)
                .unwrap_or_else(|| panic!("expected entry at {:?}", path));
            assert_eq!(
                entry.key.dirname,
                dirname.to_vec(),
                "dirname for {:?}",
                path
            );
            assert_eq!(
                entry.key.basename,
                basename.to_vec(),
                "basename for {:?}",
                path
            );
            assert_eq!(
                entry.key.file_id,
                file_id.to_vec(),
                "file_id for {:?}",
                path
            );
        }
    }

    #[test]
    fn iter_entries_yields_every_entry_across_blocks() {
        let state = create_complex_dirstate();
        let entries: Vec<&[u8]> = state
            .iter_entries()
            .map(|e| e.key.file_id.as_slice())
            .collect();
        // Expected order: root, then root contents (a, b, c, d), then
        // the a/ block (e, f), then the b/ block (g, h\xc3\xa5).
        let expected: Vec<&[u8]> = vec![
            b"a-root-value",
            b"a-dir",
            b"b-dir",
            b"c-file",
            b"d-file",
            b"e-dir",
            b"f-file",
            b"g-file",
            b"h-\xc3\xa5-file",
        ];
        assert_eq!(entries, expected);
    }

    #[test]
    fn build_id_index_maps_every_file_id_to_its_key() {
        let state = create_complex_dirstate();
        let idx = state.build_id_index();
        // Every file_id in the complex dirstate should round-trip
        // through the index to the same (dirname, basename) triple.
        let expected: &[(&[u8], &[u8], &[u8])] = &[
            (b"a-root-value", b"", b""),
            (b"a-dir", b"", b"a"),
            (b"b-dir", b"", b"b"),
            (b"c-file", b"", b"c"),
            (b"d-file", b"", b"d"),
            (b"e-dir", b"a", b"e"),
            (b"f-file", b"a", b"f"),
            (b"g-file", b"b", b"g"),
            (b"h-\xc3\xa5-file", b"b", b"h\xc3\xa5"),
        ];
        for (file_id, dirname, basename) in expected {
            let got = idx.get(&FileId::from(&file_id.to_vec()));
            assert_eq!(got.len(), 1, "expected one entry for {:?}", file_id);
            assert_eq!(got[0].0, dirname.to_vec());
            assert_eq!(got[0].1, basename.to_vec());
            assert_eq!(got[0].2.as_bytes(), *file_id);
        }
    }

    #[test]
    fn build_id_index_collapses_duplicate_file_ids_across_trees() {
        // The two-tree fixture has two rows with the same file_id in
        // different trees (c-file appears as the c/ entry in tree 0
        // and the b/j relocation in tree 1). Both rows share the same
        // file_id and should appear under the same id_index bucket.
        let state = create_dirstate_with_two_trees();
        let idx = state.build_id_index();
        let c_file_entries = idx.get(&FileId::from(&b"c-file".to_vec()));
        // Two rows share the file_id across the dirstate (c and b/j).
        assert_eq!(c_file_entries.len(), 2);
        let basenames: Vec<&[u8]> = c_file_entries
            .iter()
            .map(|(_, b, _)| b.as_slice())
            .collect();
        assert!(basenames.contains(&&b"c"[..]));
        assert!(basenames.contains(&&b"j"[..]));
    }

    #[test]
    fn get_entry_by_file_id_direct_hit_in_tree_zero() {
        let mut state = create_complex_dirstate();
        let result = state.get_entry_by_file_id(0, b"c-file", false);
        match result {
            GetEntryResult::Entry(key) => {
                assert_eq!(key.dirname, b"".to_vec());
                assert_eq!(key.basename, b"c".to_vec());
                assert_eq!(key.file_id, b"c-file".to_vec());
            }
            other => panic!("expected Entry, got {:?}", other),
        }
    }

    #[test]
    fn get_entry_by_file_id_follows_relocation_chain() {
        // In create_dirstate_with_two_trees, c-file's row at (b"", b"c")
        // is relocated in tree 1 → b/j. The id_index should find the
        // (b"b", b"j") variant on the second pass and return it.
        let mut state = create_dirstate_with_two_trees();
        let result = state.get_entry_by_file_id(1, b"c-file", false);
        match result {
            GetEntryResult::Entry(key) => {
                assert_eq!(key.dirname, b"b".to_vec());
                assert_eq!(key.basename, b"j".to_vec());
                assert_eq!(key.file_id, b"c-file".to_vec());
            }
            other => panic!("expected Entry, got {:?}", other),
        }
    }

    #[test]
    fn get_entry_by_file_id_not_found_for_unknown_id() {
        let mut state = create_complex_dirstate();
        assert_eq!(
            state.get_entry_by_file_id(0, b"nonexistent", false),
            GetEntryResult::NotFound
        );
    }

    #[test]
    fn get_entry_by_file_id_absent_without_include_deleted() {
        // g-file is absent in tree 1 (null_parent). Without
        // include_deleted the lookup returns NotFound.
        let mut state = create_dirstate_with_two_trees();
        assert_eq!(
            state.get_entry_by_file_id(1, b"g-file", false),
            GetEntryResult::NotFound
        );
    }

    #[test]
    fn get_entry_by_file_id_absent_with_include_deleted() {
        // Same g-file/tree 1 lookup, but with include_deleted we get
        // the absent entry back.
        let mut state = create_dirstate_with_two_trees();
        match state.get_entry_by_file_id(1, b"g-file", true) {
            GetEntryResult::Entry(key) => {
                assert_eq!(key.basename, b"g".to_vec());
                assert_eq!(key.file_id, b"g-file".to_vec());
            }
            other => panic!("expected Entry, got {:?}", other),
        }
    }

    /// Python-style `present_dir` tuple: `(b"d", b"", 0, False, NULLSTAT)`.
    fn dmp_present_dir() -> TreeData {
        TreeData {
            minikind: b'd',
            fingerprint: Vec::new(),
            size: 0,
            executable: false,
            packed_stat: PACKED_STAT.to_vec(),
        }
    }

    /// Python-style `present_file` tuple: `(b"f", b"", 0, False, NULLSTAT)`.
    fn dmp_present_file() -> TreeData {
        TreeData {
            minikind: b'f',
            fingerprint: Vec::new(),
            size: 0,
            executable: false,
            packed_stat: PACKED_STAT.to_vec(),
        }
    }

    /// Python-style `NULL_PARENT_DETAILS`: `(b"a", b"", 0, False, b"")`.
    fn dmp_absent() -> TreeData {
        TreeData {
            minikind: b'a',
            fingerprint: Vec::new(),
            size: 0,
            executable: false,
            packed_stat: Vec::new(),
        }
    }

    /// Python-style relocation target tree data:
    /// `(b"r", real_path, 0, False, b"")`.
    fn dmp_relocated(real_path: &[u8]) -> TreeData {
        TreeData {
            minikind: b'r',
            fingerprint: real_path.to_vec(),
            size: 0,
            executable: false,
            packed_stat: Vec::new(),
        }
    }

    fn mk_entry(dirname: &[u8], basename: &[u8], file_id: &[u8], trees: Vec<TreeData>) -> Entry {
        Entry {
            key: EntryKey {
                dirname: dirname.to_vec(),
                basename: basename.to_vec(),
                file_id: file_id.to_vec(),
            },
            trees,
        }
    }

    /// Rust counterpart of Python
    /// `TestDiscardMergeParents.test_discard_no_parents`: no-op on an
    /// empty dirstate.
    #[test]
    fn discard_merge_parents_no_parents_is_noop() {
        let mut state = fresh_state();
        state.dirblocks = vec![
            Dirblock {
                dirname: Vec::new(),
                entries: Vec::new(),
            },
            Dirblock {
                dirname: Vec::new(),
                entries: Vec::new(),
            },
        ];
        state.discard_merge_parents();
        assert!(state.parents.is_empty());
        assert!(state.ghosts.is_empty());
        assert_eq!(state.dirblocks.len(), 2);
    }

    /// Rust counterpart of Python
    /// `TestDiscardMergeParents.test_discard_one_parent`: with exactly
    /// one parent there is nothing beyond tree 1 to discard, but the
    /// method still runs and leaves dirblocks unchanged.
    #[test]
    fn discard_merge_parents_one_parent_is_noop_on_dirblocks() {
        let mut state = fresh_state();
        state.parents = vec![b"parent-id".to_vec()];
        let original_dirblocks = vec![
            Dirblock {
                dirname: Vec::new(),
                entries: vec![mk_entry(
                    b"",
                    b"",
                    b"a-root-value",
                    vec![dmp_present_dir(), dmp_present_dir()],
                )],
            },
            Dirblock {
                dirname: Vec::new(),
                entries: Vec::new(),
            },
        ];
        state.dirblocks = original_dirblocks.clone();
        state.discard_merge_parents();
        assert_eq!(state.parents, vec![b"parent-id".to_vec()]);
        assert_eq!(state.dirblocks, original_dirblocks);
    }

    /// Rust counterpart of Python
    /// `TestDiscardMergeParents.test_discard_simple`: three trees per
    /// row collapse to two, dropping the merged parent column.
    #[test]
    fn discard_merge_parents_strips_merge_column() {
        let mut state = fresh_state();
        state.parents = vec![b"parent-id".to_vec(), b"merged-id".to_vec()];
        state.dirblocks = vec![
            Dirblock {
                dirname: Vec::new(),
                entries: vec![mk_entry(
                    b"",
                    b"",
                    b"a-root-value",
                    vec![dmp_present_dir(), dmp_present_dir(), dmp_present_dir()],
                )],
            },
            Dirblock {
                dirname: Vec::new(),
                entries: Vec::new(),
            },
        ];
        state.discard_merge_parents();
        assert_eq!(state.parents, vec![b"parent-id".to_vec()]);
        assert_eq!(state.dirblocks[0].entries.len(), 1);
        assert_eq!(state.dirblocks[0].entries[0].trees.len(), 2);
        assert_eq!(state.dirblocks[1].entries.len(), 0);
    }

    /// Rust counterpart of Python
    /// `TestDiscardMergeParents.test_discard_absent`: a row that only
    /// exists in the merge parent (absent in tree 0 and 1) is removed
    /// entirely.
    #[test]
    fn discard_merge_parents_removes_absent_only_rows() {
        let mut state = fresh_state();
        state.parents = vec![b"parent-id".to_vec(), b"merged-id".to_vec()];
        state.dirblocks = vec![
            Dirblock {
                dirname: Vec::new(),
                entries: vec![mk_entry(
                    b"",
                    b"",
                    b"a-root-value",
                    vec![dmp_present_dir(), dmp_present_dir(), dmp_present_dir()],
                )],
            },
            Dirblock {
                dirname: Vec::new(),
                entries: vec![
                    mk_entry(
                        b"",
                        b"file-in-merged",
                        b"b-file-id",
                        vec![dmp_absent(), dmp_absent(), dmp_present_file()],
                    ),
                    mk_entry(
                        b"",
                        b"file-in-root",
                        b"a-file-id",
                        vec![dmp_present_file(), dmp_present_file(), dmp_present_file()],
                    ),
                ],
            },
        ];
        state.discard_merge_parents();
        // file-in-merged was only in the merge tree — dropped.
        assert_eq!(state.dirblocks[1].entries.len(), 1);
        assert_eq!(
            state.dirblocks[1].entries[0].key.basename,
            b"file-in-root".to_vec()
        );
        assert_eq!(state.dirblocks[1].entries[0].trees.len(), 2);
    }

    /// Rust counterpart of Python
    /// `TestDiscardMergeParents.test_discard_renamed`: rows whose
    /// tree 0 / tree 1 kinds are `(a, r)`, `(r, a)`, or `(r, r)` are
    /// removed; rows that still have a live kind in one of the first
    /// two trees survive with their columns truncated.
    #[test]
    fn discard_merge_parents_removes_dead_relocation_rows() {
        let mut state = fresh_state();
        state.parents = vec![b"parent-id".to_vec(), b"merged-id".to_vec()];
        state.dirblocks = vec![
            Dirblock {
                dirname: Vec::new(),
                entries: vec![mk_entry(
                    b"",
                    b"",
                    b"a-root-value",
                    vec![dmp_present_dir(), dmp_present_dir(), dmp_present_dir()],
                )],
            },
            Dirblock {
                dirname: Vec::new(),
                entries: vec![
                    // (absent, present, r) — tree0/tree1 = (a, f). Lives on.
                    mk_entry(
                        b"",
                        b"file-in-1",
                        b"c-file-id",
                        vec![
                            dmp_absent(),
                            dmp_present_file(),
                            dmp_relocated(b"file-in-2"),
                        ],
                    ),
                    // (absent, r, present) — tree0/tree1 = (a, r). Dead.
                    mk_entry(
                        b"",
                        b"file-in-2",
                        b"c-file-id",
                        vec![
                            dmp_absent(),
                            dmp_relocated(b"file-in-1"),
                            dmp_present_file(),
                        ],
                    ),
                    // normal file — tree0/tree1 = (f, f). Lives on.
                    mk_entry(
                        b"",
                        b"file-in-root",
                        b"a-file-id",
                        vec![dmp_present_file(), dmp_present_file(), dmp_present_file()],
                    ),
                    // (r, absent, present) — tree0/tree1 = (r, a). Dead.
                    mk_entry(
                        b"",
                        b"file-s",
                        b"b-file-id",
                        vec![dmp_relocated(b"file-t"), dmp_absent(), dmp_present_file()],
                    ),
                    // (present, absent, r) — tree0/tree1 = (f, a). Lives on.
                    mk_entry(
                        b"",
                        b"file-t",
                        b"b-file-id",
                        vec![dmp_present_file(), dmp_absent(), dmp_relocated(b"file-s")],
                    ),
                ],
            },
        ];
        state.discard_merge_parents();

        let surviving: Vec<&[u8]> = state.dirblocks[1]
            .entries
            .iter()
            .map(|e| e.key.basename.as_slice())
            .collect();
        assert_eq!(
            surviving,
            vec![&b"file-in-1"[..], b"file-in-root", b"file-t"]
        );
        for entry in &state.dirblocks[1].entries {
            assert_eq!(entry.trees.len(), 2, "{:?}", entry.key.basename);
        }
    }

    /// Rust counterpart of Python
    /// `TestDiscardMergeParents.test_discard_all_subdir`: a whole
    /// block of merge-only children is emptied (but the block itself
    /// remains).
    #[test]
    fn discard_merge_parents_empties_block_of_merge_only_children() {
        let mut state = fresh_state();
        state.parents = vec![b"parent-id".to_vec(), b"merged-id".to_vec()];
        state.dirblocks = vec![
            Dirblock {
                dirname: Vec::new(),
                entries: vec![mk_entry(
                    b"",
                    b"",
                    b"a-root-value",
                    vec![dmp_present_dir(), dmp_present_dir(), dmp_present_dir()],
                )],
            },
            Dirblock {
                dirname: Vec::new(),
                entries: vec![mk_entry(
                    b"",
                    b"sub",
                    b"dir-id",
                    vec![dmp_present_dir(), dmp_present_dir(), dmp_present_dir()],
                )],
            },
            Dirblock {
                dirname: b"sub".to_vec(),
                entries: vec![
                    mk_entry(
                        b"sub",
                        b"child1",
                        b"child1-id",
                        vec![dmp_absent(), dmp_absent(), dmp_present_file()],
                    ),
                    mk_entry(
                        b"sub",
                        b"child2",
                        b"child2-id",
                        vec![dmp_absent(), dmp_absent(), dmp_present_file()],
                    ),
                    mk_entry(
                        b"sub",
                        b"child3",
                        b"child3-id",
                        vec![dmp_absent(), dmp_absent(), dmp_present_file()],
                    ),
                ],
            },
        ];
        state.discard_merge_parents();
        assert_eq!(state.dirblocks.len(), 3);
        assert_eq!(state.dirblocks[0].entries.len(), 1);
        assert_eq!(state.dirblocks[1].entries.len(), 1);
        assert_eq!(state.dirblocks[2].dirname, b"sub".to_vec());
        assert!(
            state.dirblocks[2].entries.is_empty(),
            "all children were merge-only and should have been removed"
        );
        // Each surviving entry has exactly two trees.
        for block in &state.dirblocks {
            for entry in &block.entries {
                assert_eq!(entry.trees.len(), 2);
            }
        }
    }

    /// Ghost parent path: the first parent is in the ghosts list, so
    /// every surviving row gets `NULL_PARENT_DETAILS` in slot 1
    /// rather than slot 1's real data. Python documents this
    /// behaviour via the `entry[1][1:] = empty_parent` branch.
    #[test]
    fn discard_merge_parents_ghost_first_parent_replaces_with_null_parent() {
        let mut state = fresh_state();
        state.parents = vec![b"parent-id".to_vec(), b"merged-id".to_vec()];
        state.ghosts = vec![b"parent-id".to_vec()];
        state.dirblocks = vec![
            Dirblock {
                dirname: Vec::new(),
                entries: vec![mk_entry(
                    b"",
                    b"",
                    b"a-root-value",
                    vec![dmp_present_dir(), dmp_present_dir(), dmp_present_dir()],
                )],
            },
            Dirblock {
                dirname: Vec::new(),
                entries: Vec::new(),
            },
        ];
        state.discard_merge_parents();
        // First parent is kept but the tree-1 slot is replaced with
        // a NULL_PARENT_DETAILS (minikind b'a').
        assert_eq!(state.parents, vec![b"parent-id".to_vec()]);
        assert!(state.ghosts.is_empty());
        let root = &state.dirblocks[0].entries[0];
        assert_eq!(root.trees.len(), 2);
        assert_eq!(root.trees[0].minikind, b'd');
        assert_eq!(root.trees[1].minikind, b'a');
        assert!(root.trees[1].fingerprint.is_empty());
        assert!(root.trees[1].packed_stat.is_empty());
    }

    /// A tree-row builder that lets tests set a non-default fingerprint
    /// on a single row — needed to exercise the relocation branch of
    /// `make_absent`, which uses the fingerprint as the target path.
    fn tree_with_fingerprint(minikind: u8, fingerprint: &[u8]) -> TreeData {
        TreeData {
            minikind,
            fingerprint: fingerprint.to_vec(),
            size: 0,
            executable: false,
            packed_stat: b"x".repeat(32),
        }
    }

    /// Build a minimal dirblocks layout containing the two empty
    /// sentinel blocks plus a single dirblock named `dir` with the
    /// supplied entries. The `make_absent` tests only need this
    /// shape; richer structure goes through `create_complex_dirstate`.
    fn absent_fixture(entries: Vec<Entry>) -> DirState {
        let mut state = fresh_state();
        state.parents = vec![b"parent-id".to_vec(), b"merged-id".to_vec()];
        state.dirblocks = vec![
            Dirblock {
                dirname: Vec::new(),
                entries: Vec::new(),
            },
            Dirblock {
                dirname: Vec::new(),
                entries: Vec::new(),
            },
            Dirblock {
                dirname: b"dir".to_vec(),
                entries,
            },
        ];
        state
    }

    /// The entry has no trees beyond tree 0, so marking it absent is
    /// the last reference — the row is removed from its block and
    /// dropped from the id_index.
    #[test]
    fn make_absent_removes_last_reference_and_updates_id_index() {
        let mut state = absent_fixture(vec![entry_with_trees(
            b"dir",
            b"a",
            b"fid-a",
            vec![tree(b'f')],
        )]);
        // Prime the id_index cache.
        let fid = FileId::from(&b"fid-a".to_vec());
        state.get_or_build_id_index();
        assert_eq!(state.id_index.as_ref().unwrap().get(&fid).len(), 1);

        let last_reference = state
            .make_absent(&EntryKey {
                dirname: b"dir".to_vec(),
                basename: b"a".to_vec(),
                file_id: b"fid-a".to_vec(),
            })
            .expect("make_absent");
        assert!(last_reference);
        assert!(state.dirblocks[2].entries.is_empty());
        assert!(state.id_index.as_ref().unwrap().get(&fid).is_empty());
        assert_eq!(state.dirblock_state, MemoryState::InMemoryModified);
    }

    /// A merge-parent row keeps the entry alive at its current key,
    /// so `make_absent` does not remove it — instead it sets tree 0
    /// to NULL_PARENT_DETAILS and leaves the block populated.
    #[test]
    fn make_absent_sets_tree0_absent_when_other_trees_keep_entry() {
        let mut state = absent_fixture(vec![entry_with_trees(
            b"dir",
            b"a",
            b"fid-a",
            vec![tree(b'f'), tree(b'd')],
        )]);

        let last_reference = state
            .make_absent(&EntryKey {
                dirname: b"dir".to_vec(),
                basename: b"a".to_vec(),
                file_id: b"fid-a".to_vec(),
            })
            .expect("make_absent");
        assert!(!last_reference);
        assert_eq!(state.dirblocks[2].entries.len(), 1);
        let entry = &state.dirblocks[2].entries[0];
        assert_eq!(entry.trees[0].minikind, b'a');
        assert!(entry.trees[0].fingerprint.is_empty());
        assert!(entry.trees[0].packed_stat.is_empty());
        assert_eq!(entry.trees[1].minikind, b'd');
    }

    /// A relocated parent row promotes the relocation target to a
    /// remaining-key, whose tree 0 slot must get set to absent too.
    /// The original entry is removed (last_reference=true).
    #[test]
    fn make_absent_follows_relocation_and_updates_target() {
        // Two entries:
        //   - dir/a: tree 0 present, tree 1 relocation → dir/b
        //   - dir/b: tree 0 present, tree 1 present (so make_absent
        //     against dir/a wipes dir/a and sets dir/b's tree 0 to a).
        let mut state = absent_fixture(vec![
            entry_with_trees(
                b"dir",
                b"a",
                b"fid-a",
                vec![tree(b'f'), tree_with_fingerprint(b'r', b"dir/b")],
            ),
            entry_with_trees(b"dir", b"b", b"fid-a", vec![tree(b'f'), tree(b'f')]),
        ]);
        // Prime the id_index; make_absent should remove dir/a from it.
        let fid = FileId::from(&b"fid-a".to_vec());
        state.get_or_build_id_index();
        assert_eq!(state.id_index.as_ref().unwrap().get(&fid).len(), 2);

        let last_reference = state
            .make_absent(&EntryKey {
                dirname: b"dir".to_vec(),
                basename: b"a".to_vec(),
                file_id: b"fid-a".to_vec(),
            })
            .expect("make_absent");
        assert!(last_reference);
        // dir/a is gone.
        assert_eq!(state.dirblocks[2].entries.len(), 1);
        assert_eq!(state.dirblocks[2].entries[0].key.basename, b"b".to_vec());
        // dir/b's tree 0 flipped to absent; tree 1 stayed intact.
        let survivor = &state.dirblocks[2].entries[0];
        assert_eq!(survivor.trees[0].minikind, b'a');
        assert_eq!(survivor.trees[1].minikind, b'f');
        assert_eq!(state.id_index.as_ref().unwrap().get(&fid).len(), 1);
    }

    /// An absent-parent row contributes nothing to remaining-keys —
    /// still a last_reference, still removes the entry, but makes no
    /// tree-0 updates elsewhere.
    #[test]
    fn make_absent_absent_parent_row_is_ignored() {
        let mut state = absent_fixture(vec![entry_with_trees(
            b"dir",
            b"a",
            b"fid-a",
            vec![tree(b'f'), tree(b'a')],
        )]);
        let last_reference = state
            .make_absent(&EntryKey {
                dirname: b"dir".to_vec(),
                basename: b"a".to_vec(),
                file_id: b"fid-a".to_vec(),
            })
            .expect("make_absent");
        assert!(last_reference);
        assert!(state.dirblocks[2].entries.is_empty());
    }

    #[test]
    fn packed_stat_index_only_contains_file_entries() {
        // Mix of file, directory, symlink, absent, relocated: only
        // `f` entries should make it into the index, keyed by their
        // packed_stat and valued by their fingerprint (sha1).
        let mut state = fresh_state();
        let f1 = TreeData {
            minikind: b'f',
            fingerprint: b"sha-f1".to_vec(),
            size: 10,
            executable: false,
            packed_stat: b"stat-f1".to_vec(),
        };
        let f2 = TreeData {
            minikind: b'f',
            fingerprint: b"sha-f2".to_vec(),
            size: 20,
            executable: true,
            packed_stat: b"stat-f2".to_vec(),
        };
        let dir = tree(b'd');
        let symlink = TreeData {
            minikind: b'l',
            fingerprint: b"target".to_vec(),
            size: 0,
            executable: false,
            packed_stat: b"stat-l".to_vec(),
        };
        state.dirblocks = vec![
            Dirblock {
                dirname: Vec::new(),
                entries: vec![entry_with_trees(b"", b"", b"TREE_ROOT", vec![dir.clone()])],
            },
            Dirblock {
                dirname: Vec::new(),
                entries: vec![
                    entry_with_trees(b"", b"a", b"fid-a", vec![f1.clone()]),
                    entry_with_trees(b"", b"b", b"fid-b", vec![f2.clone()]),
                    entry_with_trees(b"", b"c", b"fid-c", vec![symlink]),
                    entry_with_trees(b"", b"d", b"fid-d", vec![tree(b'a')]),
                ],
            },
        ];
        let idx = state.build_packed_stat_index();
        assert_eq!(idx.len(), 2);
        assert_eq!(idx.get(&b"stat-f1".to_vec()).unwrap(), &b"sha-f1".to_vec());
        assert_eq!(idx.get(&b"stat-f2".to_vec()).unwrap(), &b"sha-f2".to_vec());
    }

    #[test]
    fn get_or_build_packed_stat_index_caches_result() {
        let mut state = fresh_state();
        state.dirblocks = vec![
            Dirblock {
                dirname: Vec::new(),
                entries: vec![entry_with_trees(b"", b"", b"TREE_ROOT", vec![tree(b'd')])],
            },
            Dirblock {
                dirname: Vec::new(),
                entries: vec![entry_with_trees(
                    b"",
                    b"a",
                    b"fid-a",
                    vec![TreeData {
                        minikind: b'f',
                        fingerprint: b"sha-a".to_vec(),
                        size: 1,
                        executable: false,
                        packed_stat: b"stat-a".to_vec(),
                    }],
                )],
            },
        ];
        assert!(state.packed_stat_index.is_none());
        let idx = state.get_or_build_packed_stat_index();
        assert_eq!(idx.len(), 1);
        // Second call returns the cached map — structural equality
        // since we can't easily compare references without breaking
        // the borrow.
        assert!(state.packed_stat_index.is_some());
        let idx_again = state.get_or_build_packed_stat_index();
        assert_eq!(idx_again.len(), 1);
    }

    #[test]
    fn packed_stat_index_invalidated_by_set_data() {
        let mut state = fresh_state();
        state.packed_stat_index = Some(HashMap::new());
        state.set_data(Vec::new(), Vec::new());
        assert!(state.packed_stat_index.is_none());
    }

    #[test]
    fn packed_stat_index_invalidated_by_wipe_state() {
        let mut state = fresh_state();
        state.packed_stat_index = Some(HashMap::new());
        state.wipe_state();
        assert!(state.packed_stat_index.is_none());
    }

    /// Missing entry raises EntryNotFound — Python's corresponding
    /// AssertionError.
    #[test]
    fn make_absent_missing_entry_returns_error() {
        let mut state = absent_fixture(vec![entry_with_trees(
            b"dir",
            b"a",
            b"fid-a",
            vec![tree(b'f')],
        )]);
        let err = state
            .make_absent(&EntryKey {
                dirname: b"dir".to_vec(),
                basename: b"ghost".to_vec(),
                file_id: b"fid-ghost".to_vec(),
            })
            .unwrap_err();
        assert!(matches!(err, MakeAbsentError::EntryNotFound { .. }));
    }

    /// A minimal root-only dirblock layout, used to test `add` paths
    /// that insert new entries directly at the root.
    fn add_fixture() -> DirState {
        let mut state = fresh_state();
        state.parents = vec![];
        state.dirblocks = vec![
            Dirblock {
                dirname: Vec::new(),
                entries: vec![Entry {
                    key: EntryKey {
                        dirname: Vec::new(),
                        basename: Vec::new(),
                        file_id: b"TREE_ROOT".to_vec(),
                    },
                    trees: vec![TreeData {
                        minikind: b'd',
                        fingerprint: Vec::new(),
                        size: 0,
                        executable: false,
                        packed_stat: b"x".repeat(32),
                    }],
                }],
            },
            Dirblock {
                dirname: Vec::new(),
                entries: Vec::new(),
            },
        ];
        state
    }

    #[test]
    fn add_inserts_new_file_at_root() {
        let mut state = add_fixture();
        let stat = b"x".repeat(32);
        state
            .add(b"a", b"", b"a", b"fid-a", "file", 7, &stat, b"sha1")
            .expect("add");
        // Root-contents block (index 1) now has one entry.
        assert_eq!(state.dirblocks[1].entries.len(), 1);
        let entry = &state.dirblocks[1].entries[0];
        assert_eq!(entry.key.basename, b"a");
        assert_eq!(entry.key.file_id, b"fid-a");
        assert_eq!(entry.trees[0].minikind, b'f');
        assert_eq!(entry.trees[0].size, 7);
        assert_eq!(entry.trees[0].fingerprint, b"sha1");
    }

    #[test]
    fn add_directory_creates_child_block() {
        let mut state = add_fixture();
        let stat = b"x".repeat(32);
        state
            .add(b"sub", b"", b"sub", b"fid-sub", "directory", 0, &stat, b"")
            .expect("add");
        // A new block for the directory 'sub' should now exist.
        let block_names: Vec<&[u8]> = state
            .dirblocks
            .iter()
            .map(|b| b.dirname.as_slice())
            .collect();
        assert!(block_names.contains(&b"sub".as_slice()));
    }

    #[test]
    fn add_duplicate_file_id_errors() {
        let mut state = add_fixture();
        let stat = b"x".repeat(32);
        state
            .add(b"a", b"", b"a", b"fid-a", "file", 1, &stat, b"")
            .expect("first add");
        let err = state
            .add(b"b", b"", b"b", b"fid-a", "file", 1, &stat, b"")
            .unwrap_err();
        assert!(matches!(err, AddError::DuplicateFileId { .. }));
    }

    #[test]
    fn add_second_path_same_basename_errors() {
        let mut state = add_fixture();
        let stat = b"x".repeat(32);
        state
            .add(b"a", b"", b"a", b"fid-a", "file", 1, &stat, b"")
            .expect("first add");
        let err = state
            .add(b"a", b"", b"a", b"fid-other", "file", 1, &stat, b"")
            .unwrap_err();
        assert!(matches!(err, AddError::AlreadyAdded { .. }));
    }

    #[test]
    fn add_unknown_kind_errors() {
        let mut state = add_fixture();
        let stat = b"x".repeat(32);
        let err = state
            .add(b"a", b"", b"a", b"fid-a", "pipe", 0, &stat, b"")
            .unwrap_err();
        assert!(matches!(err, AddError::UnknownKind { .. }));
    }

    #[test]
    fn add_parent_missing_errors_not_versioned() {
        let mut state = add_fixture();
        let stat = b"x".repeat(32);
        // There is no block for 'missing', and its parent ('') has no
        // entry named 'missing' at tree 0.
        let err = state
            .add(
                b"missing/child",
                b"missing",
                b"child",
                b"fid-c",
                "file",
                0,
                &stat,
                b"",
            )
            .unwrap_err();
        assert!(matches!(err, AddError::NotVersioned { .. }));
    }

    #[test]
    fn set_path_id_rejects_non_root_path() {
        let mut state = add_fixture();
        let err = state.set_path_id(b"foo", b"new-id").unwrap_err();
        assert!(matches!(err, SetPathIdError::NonRootPath));
    }

    #[test]
    fn set_path_id_unchanged_id_is_noop() {
        let mut state = add_fixture();
        let before = state.dirblocks.clone();
        state.set_path_id(b"", b"TREE_ROOT").expect("same-id noop");
        assert_eq!(state.dirblocks, before);
    }

    #[test]
    fn set_path_id_rewrites_root_and_preserves_packed_stat() {
        let mut state = add_fixture();
        // The root row's packed_stat is `b"x".repeat(32)` per
        // add_fixture; no parent trees keep the entry alive, so the
        // new row should carry the same packed_stat.
        let original_packed_stat = state.dirblocks[0].entries[0].trees[0].packed_stat.clone();
        state.set_path_id(b"", b"new-id").expect("set_path_id");
        assert_eq!(state.dirblocks[0].entries.len(), 1);
        let new_root = &state.dirblocks[0].entries[0];
        assert_eq!(new_root.key.file_id, b"new-id");
        assert_eq!(new_root.trees[0].minikind, b'd');
        assert_eq!(new_root.trees[0].packed_stat, original_packed_stat);
    }

    #[test]
    fn set_state_from_inventory_rename_same_id_bug_395556() {
        // Regression for the bug395556 scenario: start with root + 'b'
        // (file-id b-id); then rename b -> a in the inventory.  After
        // the second set_state_from_inventory the dirstate should hold
        // root + 'a' (file-id b-id) with no stale 'b' row.
        let mut state = add_fixture();
        let stat = b"x".repeat(32);
        state
            .add(b"b", b"", b"b", b"b-id", "file", 0, &stat, b"")
            .expect("add");

        let inv_after_rename: Vec<(Vec<u8>, Vec<u8>, u8, Vec<u8>, bool)> = vec![
            (Vec::new(), b"TREE_ROOT".to_vec(), b'd', Vec::new(), false),
            (b"a".to_vec(), b"b-id".to_vec(), b'f', Vec::new(), false),
        ];
        state
            .set_state_from_inventory(inv_after_rename)
            .expect("set_state_from_inventory");

        // Expect: root row, then 'a' with file_id b-id in the root
        // contents block.  No live 'b' row.
        let mut live_entries = Vec::new();
        for block in &state.dirblocks {
            for entry in &block.entries {
                let t0 = entry.trees.first().map(|t| t.minikind).unwrap_or(0);
                if t0 != b'a' && t0 != b'r' {
                    live_entries.push((
                        entry.key.dirname.clone(),
                        entry.key.basename.clone(),
                        entry.key.file_id.clone(),
                        t0,
                    ));
                }
            }
        }
        assert_eq!(
            live_entries,
            vec![
                (Vec::new(), Vec::new(), b"TREE_ROOT".to_vec(), b'd'),
                (Vec::new(), b"a".to_vec(), b"b-id".to_vec(), b'f'),
            ]
        );
    }

    #[test]
    fn update_entry_refreshes_sha_after_content_change() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let fpath = dir.path().join("a-file");
        {
            let mut f = std::fs::File::create(&fpath).unwrap();
            f.write_all(b"first content\n").unwrap();
        }

        let mut state = add_fixture();
        // Give the dirstate a committed parent so update_entry's
        // "stat-cacheable and tree-1 is live" branch runs and the sha
        // actually gets written.  Without a parent the row is still
        // in "initial add" mode and we skip the sha computation.
        state.parents = vec![b"parent-rev".to_vec()];
        state.dirblocks[0].entries[0].trees.push(TreeData {
            minikind: b'd',
            fingerprint: Vec::new(),
            size: 0,
            executable: false,
            packed_stat: Vec::new(),
        });
        let stat = b"x".repeat(32);
        state
            .add(b"a-file", b"", b"a-file", b"file-id", "file", 0, &stat, b"")
            .expect("add");
        // The newly-added entry still has tree-1 = absent; make it
        // live so update_entry writes the sha.
        let bei = state.get_block_entry_index(b"", b"a-file", 0);
        state.dirblocks[bei.block_index].entries[bei.entry_index].trees[1] = TreeData {
            minikind: b'f',
            fingerprint: b"parent-sha".to_vec(),
            size: 0,
            executable: false,
            packed_stat: Vec::new(),
        };
        // Set cutoff_time so the on-disk stat is considered cacheable.
        state.cutoff_time = Some(i64::MAX);

        let meta = std::fs::symlink_metadata(&fpath).unwrap();
        let stat_info = StatInfo {
            mode: {
                #[cfg(unix)]
                {
                    meta.mode()
                }
                #[cfg(not(unix))]
                {
                    0o100644
                }
            },
            size: meta.len(),
            mtime: metadata_mtime_secs(&meta),
            ctime: metadata_ctime_secs(&meta),
            dev: {
                #[cfg(unix)]
                {
                    meta.dev()
                }
                #[cfg(not(unix))]
                {
                    0
                }
            },
            ino: {
                #[cfg(unix)]
                {
                    meta.ino()
                }
                #[cfg(not(unix))]
                {
                    0
                }
            },
        };
        let key = EntryKey {
            dirname: Vec::new(),
            basename: b"a-file".to_vec(),
            file_id: b"file-id".to_vec(),
        };
        let abspath_bytes = fpath.as_os_str().as_encoded_bytes().to_vec();
        let transport = MemoryTransport::new();
        let result = state
            .update_entry(&key, &abspath_bytes, &stat_info, &transport)
            .expect("update_entry");
        let sha = result.expect("file should yield a sha");
        // Sha of "first content\n".
        assert_eq!(
            std::str::from_utf8(&sha).unwrap(),
            "c0a245ade45b97366321074bb27a39a6ae1dc4fc"
        );
        // Tree-0 row should now carry that same sha.
        let bei = state.get_block_entry_index(b"", b"a-file", 0);
        assert!(bei.path_present);
        let entry = &state.dirblocks[bei.block_index].entries[bei.entry_index];
        assert_eq!(entry.trees[0].fingerprint, sha);
    }

    #[test]
    fn bisect_roundtrips_via_get_lines() {
        // Populate a dirstate, serialise it via get_lines, then bisect
        // the serialised byte stream for a known path.  Exercises the
        // full read pipeline (header, entry row parsing, bisect).
        let mut state = add_fixture();
        let stat = b"x".repeat(32);
        state
            .add(
                b"alpha", b"", b"alpha", b"a-id", "file", 11, &stat, b"sha-a",
            )
            .expect("add alpha");
        state
            .add(
                b"bravo", b"", b"bravo", b"b-id", "file", 22, &stat, b"sha-b",
            )
            .expect("add bravo");

        let lines = state.get_lines();
        let buf: Vec<u8> = lines.into_iter().flatten().collect();

        // Extract end_of_header just like the Python header reader:
        // it is the byte offset of the NUL right after the fifth
        // newline.  read_header handles that for us.
        let mut reader = DirState::new(
            "/tmp/fake",
            Box::new(DefaultSHA1Provider::new()),
            0,
            true,
            false,
        );
        reader.read_header(&buf).expect("read_header");
        reader.dirblock_state = MemoryState::NotInMemory;

        // Build a read_range closure over the buffer.
        let buf_clone = buf.clone();
        let read_range = move |offset: u64, len: usize| -> Result<Vec<u8>, BisectError> {
            let start = offset as usize;
            let end = std::cmp::min(start + len, buf_clone.len());
            if start > buf_clone.len() {
                return Ok(Vec::new());
            }
            Ok(buf_clone[start..end].to_vec())
        };

        let file_size = buf.len() as u64;
        let found = reader
            .bisect(vec![b"bravo".to_vec()], file_size, read_range)
            .expect("bisect");
        let bravo = found.get(b"bravo".as_slice()).expect("bravo present");
        assert_eq!(bravo.len(), 1);
        assert_eq!(bravo[0].key.basename, b"bravo");
        assert_eq!(bravo[0].key.file_id, b"b-id");
        assert_eq!(bravo[0].trees[0].size, 22);
        assert_eq!(bravo[0].trees[0].fingerprint, b"sha-b");
    }

    #[test]
    fn set_parent_trees_simple_case() {
        // Start from a tree with root + 'a-file' in tree-0.
        let mut state = add_fixture();
        let stat = b"x".repeat(32);
        state
            .add(b"a-file", b"", b"a-file", b"file-id", "file", 0, &stat, b"")
            .expect("add");

        // One non-ghost parent tree that contains the same entries but with
        // different details (simulating a committed revision).
        let details_root = TreeData {
            minikind: b'd',
            fingerprint: Vec::new(),
            size: 0,
            executable: false,
            packed_stat: b"rev1".to_vec(),
        };
        let details_file = TreeData {
            minikind: b'f',
            fingerprint: b"sha1-parent".to_vec(),
            size: 42,
            executable: false,
            packed_stat: b"rev1".to_vec(),
        };
        let parent_entries = vec![
            (Vec::new(), b"TREE_ROOT".to_vec(), details_root.clone()),
            (
                b"a-file".to_vec(),
                b"file-id".to_vec(),
                details_file.clone(),
            ),
        ];

        state
            .set_parent_trees(vec![b"rev1".to_vec()], vec![], vec![parent_entries])
            .expect("set_parent_trees");

        // Root row should have tree-0 (directory) and tree-1 = details_root.
        let bei = get_block_entry_index(&state.dirblocks, b"", b"", 0);
        assert!(bei.path_present);
        let root = &state.dirblocks[bei.block_index].entries[bei.entry_index];
        assert_eq!(root.trees.len(), 2);
        assert_eq!(root.trees[1], details_root);

        // a-file row should have tree-0 (file) and tree-1 = details_file.
        let bei = get_block_entry_index(&state.dirblocks, b"", b"a-file", 0);
        assert!(bei.path_present);
        let file_entry = &state.dirblocks[bei.block_index].entries[bei.entry_index];
        assert_eq!(file_entry.trees.len(), 2);
        assert_eq!(file_entry.trees[1], details_file);

        assert_eq!(state.parents, vec![b"rev1".to_vec()]);
        assert!(state.ghosts.is_empty());
    }

    #[test]
    fn set_parent_trees_ghost_parent_has_no_entries() {
        // Ghost parents occupy a tree slot but contribute no entries.
        let mut state = add_fixture();
        let stat = b"x".repeat(32);
        state
            .add(b"x", b"", b"x", b"x-id", "file", 0, &stat, b"")
            .expect("add");

        state
            .set_parent_trees(
                vec![b"ghost-rev".to_vec()],
                vec![b"ghost-rev".to_vec()],
                vec![], // no non-ghost parent trees
            )
            .expect("set_parent_trees");

        // Only one tree slot (tree-0) per entry since there are no
        // non-ghost parents.
        for block in &state.dirblocks {
            for entry in &block.entries {
                assert_eq!(entry.trees.len(), 1);
            }
        }
        assert_eq!(state.parents, vec![b"ghost-rev".to_vec()]);
        assert_eq!(state.ghosts, vec![b"ghost-rev".to_vec()]);
    }

    #[test]
    fn set_parent_trees_cross_path_relocation() {
        // Parent tree has file-id at a different path than tree-0.
        // Expect a relocation pointer in the new row.
        let mut state = add_fixture();
        let stat = b"x".repeat(32);
        state
            .add(b"new-path", b"", b"new-path", b"fid", "file", 0, &stat, b"")
            .expect("add");

        let root_details = TreeData {
            minikind: b'd',
            fingerprint: Vec::new(),
            size: 0,
            executable: false,
            packed_stat: b"rev".to_vec(),
        };
        let file_details = TreeData {
            minikind: b'f',
            fingerprint: b"old-sha".to_vec(),
            size: 7,
            executable: false,
            packed_stat: b"rev".to_vec(),
        };
        let parent_entries = vec![
            (Vec::new(), b"TREE_ROOT".to_vec(), root_details),
            (b"old-path".to_vec(), b"fid".to_vec(), file_details.clone()),
        ];

        state
            .set_parent_trees(vec![b"rev".to_vec()], vec![], vec![parent_entries])
            .expect("set_parent_trees");

        // New path still has tree-0 (file) and tree-1 now holds a
        // relocation pointer to old-path.
        let bei = get_block_entry_index(&state.dirblocks, b"", b"new-path", 0);
        assert!(bei.path_present);
        let new_entry = &state.dirblocks[bei.block_index].entries[bei.entry_index];
        assert_eq!(new_entry.trees[1].minikind, b'r');
        assert_eq!(new_entry.trees[1].fingerprint, b"old-path");

        // old-path exists as a row with tree-0 = relocation to new-path
        // and tree-1 = the actual parent-tree details.
        let bei = get_block_entry_index(&state.dirblocks, b"", b"old-path", 1);
        assert!(bei.path_present);
        let old_entry = &state.dirblocks[bei.block_index].entries[bei.entry_index];
        assert_eq!(old_entry.trees[0].minikind, b'r');
        assert_eq!(old_entry.trees[0].fingerprint, b"new-path");
        assert_eq!(old_entry.trees[1], file_details);
    }

    #[test]
    fn set_path_id_zeroes_packed_stat_when_parents_retain_entry() {
        let mut state = add_fixture();
        // Add a parent tree that still references the root row.
        state.parents = vec![b"parent-rev".to_vec()];
        state.dirblocks[0].entries[0].trees.push(TreeData {
            minikind: b'd',
            fingerprint: Vec::new(),
            size: 0,
            executable: false,
            packed_stat: Vec::new(),
        });
        state.set_path_id(b"", b"new-id").expect("set_path_id");
        let new_root = state
            .dirblocks
            .iter()
            .flat_map(|b| b.entries.iter())
            .find(|e| e.key.file_id == b"new-id")
            .expect("new root entry");
        // With parents holding the old row alive, Python's in-place
        // mutation produced an empty packed_stat on the replacement.
        assert_eq!(new_root.trees[0].packed_stat, b"");
    }

    /// Build a TreeData that looks like Python's
    /// `(minikind, fingerprint, size, executable, packed_stat)`
    /// tuple — more convenient than the raw `tree()` helper when a
    /// test needs to set specific size/fingerprint fields.
    fn basis_details(minikind: u8, fingerprint: &[u8], size: u64, executable: bool) -> TreeData {
        TreeData {
            minikind,
            fingerprint: fingerprint.to_vec(),
            size,
            executable,
            packed_stat: b"x".repeat(32),
        }
    }

    fn null_parent_details() -> TreeData {
        TreeData {
            minikind: b'a',
            fingerprint: Vec::new(),
            size: 0,
            executable: false,
            packed_stat: Vec::new(),
        }
    }

    /// Build a minimal two-tree dirstate populated with a single
    /// file at `b""/README` in tree 0 and NULL_PARENT_DETAILS in
    /// tree 1. Suitable for exercising the "insert new add" path of
    /// `update_basis_apply_adds`.
    fn basis_adds_fixture_one_file() -> DirState {
        let mut state = fresh_state();
        state.parents = vec![b"parent-id".to_vec()];
        let tree0 = basis_details(b'f', b"sha-r", 10, false);
        let tree1 = null_parent_details();
        state.dirblocks = vec![
            Dirblock {
                dirname: Vec::new(),
                entries: vec![entry_with_trees(
                    b"",
                    b"",
                    b"TREE_ROOT",
                    vec![tree(b'd'), tree(b'd')],
                )],
            },
            Dirblock {
                dirname: Vec::new(),
                entries: vec![entry_with_trees(
                    b"",
                    b"README",
                    b"fid-readme",
                    vec![tree0, tree1],
                )],
            },
        ];
        state
    }

    #[test]
    fn update_basis_apply_adds_inserts_new_entry() {
        // Add a brand new file at b"" / b"a.txt". The block for the
        // root contents already exists; the entry does not. ASCII
        // ordering places b"README" before b"a.txt" (0x52 < 0x61), so
        // the new entry lands after README.
        let mut state = basis_adds_fixture_one_file();
        let mut adds = vec![BasisAdd {
            old_path: None,
            new_path: b"a.txt".to_vec(),
            file_id: b"fid-a".to_vec(),
            new_details: basis_details(b'f', b"sha-a", 7, false),
            real_add: true,
        }];
        state.update_basis_apply_adds(&mut adds).expect("apply");

        let block = &state.dirblocks[1];
        assert_eq!(block.entries.len(), 2);
        assert_eq!(block.entries[0].key.basename, b"README".to_vec());
        assert_eq!(block.entries[1].key.basename, b"a.txt".to_vec());
        assert_eq!(block.entries[1].trees[0].minikind, b'a');
        assert_eq!(block.entries[1].trees[1].minikind, b'f');
        assert_eq!(block.entries[1].trees[1].fingerprint, b"sha-a".to_vec());
    }

    #[test]
    fn update_basis_apply_adds_updates_absent_tree1_slot_in_place() {
        // README already exists with tree1=absent; adding the same
        // entry fills in tree 1 instead of inserting a new row.
        let mut state = basis_adds_fixture_one_file();
        let mut adds = vec![BasisAdd {
            old_path: None,
            new_path: b"README".to_vec(),
            file_id: b"fid-readme".to_vec(),
            new_details: basis_details(b'f', b"sha-updated", 42, true),
            real_add: true,
        }];
        state.update_basis_apply_adds(&mut adds).expect("apply");

        let block = &state.dirblocks[1];
        assert_eq!(block.entries.len(), 1);
        assert_eq!(
            block.entries[0].trees[1].fingerprint,
            b"sha-updated".to_vec()
        );
        assert_eq!(block.entries[0].trees[1].size, 42);
        assert!(block.entries[0].trees[1].executable);
    }

    #[test]
    fn update_basis_apply_adds_conflicting_existing_basis_is_invalid() {
        // README already has tree1 populated with a live file entry;
        // trying to add a new entry at the same path flags it as
        // InconsistentDelta rather than silently overwriting.
        let mut state = basis_adds_fixture_one_file();
        state.dirblocks[1].entries[0].trees[1] = basis_details(b'f', b"sha-existing", 11, false);

        let mut adds = vec![BasisAdd {
            old_path: None,
            new_path: b"README".to_vec(),
            file_id: b"fid-readme".to_vec(),
            new_details: basis_details(b'f', b"sha-new", 22, false),
            real_add: true,
        }];
        let err = state.update_basis_apply_adds(&mut adds).unwrap_err();
        match err {
            BasisApplyError::Invalid { path, reason, .. } => {
                assert_eq!(path, b"README".to_vec());
                assert!(
                    reason.contains("basis target already existed"),
                    "{}",
                    reason
                );
            }
            other => panic!("expected Invalid, got {:?}", other),
        }
    }

    #[test]
    fn update_basis_apply_adds_real_add_with_old_path_is_invalid() {
        let mut state = basis_adds_fixture_one_file();
        let mut adds = vec![BasisAdd {
            old_path: Some(b"some/old".to_vec()),
            new_path: b"new.txt".to_vec(),
            file_id: b"fid-new".to_vec(),
            new_details: basis_details(b'f', b"sha", 0, false),
            real_add: true,
        }];
        let err = state.update_basis_apply_adds(&mut adds).unwrap_err();
        assert!(matches!(err, BasisApplyError::Invalid { .. }));
    }

    #[test]
    fn update_basis_apply_changes_updates_existing_tree1_slot() {
        // README exists in both trees 0 and 1. The change records a
        // new tree-1 value; tree 0 is left alone.
        let mut state = fresh_state();
        state.parents = vec![b"parent-id".to_vec()];
        let tree0 = basis_details(b'f', b"sha-r", 10, false);
        let tree1 = basis_details(b'f', b"sha-old", 10, false);
        state.dirblocks = vec![
            Dirblock {
                dirname: Vec::new(),
                entries: vec![entry_with_trees(
                    b"",
                    b"",
                    b"TREE_ROOT",
                    vec![tree(b'd'), tree(b'd')],
                )],
            },
            Dirblock {
                dirname: Vec::new(),
                entries: vec![entry_with_trees(
                    b"",
                    b"README",
                    b"fid-readme",
                    vec![tree0.clone(), tree1],
                )],
            },
        ];

        let new_details = basis_details(b'f', b"sha-updated", 99, true);
        let changes = vec![(
            b"README".to_vec(),
            b"README".to_vec(),
            b"fid-readme".to_vec(),
            new_details.clone(),
        )];
        state
            .update_basis_apply_changes(&changes)
            .expect("apply_changes");

        let entry = &state.dirblocks[1].entries[0];
        assert_eq!(entry.trees[0].fingerprint, b"sha-r".to_vec());
        assert_eq!(entry.trees[1].fingerprint, b"sha-updated".to_vec());
        assert_eq!(entry.trees[1].size, 99);
        assert!(entry.trees[1].executable);
    }

    #[test]
    fn update_basis_apply_changes_absent_entry_is_invalid() {
        let mut state = basis_adds_fixture_one_file();
        // README's tree-1 is absent in the fixture; a change targeting it
        // is inconsistent.
        let changes = vec![(
            b"README".to_vec(),
            b"README".to_vec(),
            b"fid-readme".to_vec(),
            basis_details(b'f', b"sha", 1, false),
        )];
        let err = state.update_basis_apply_changes(&changes).unwrap_err();
        assert!(matches!(err, BasisApplyError::Invalid { .. }));
    }

    #[test]
    fn update_basis_apply_deletes_removes_row_when_active_also_absent() {
        // README has tree 0 absent and tree 1 live; a real_delete
        // should drop the row entirely.
        let mut state = fresh_state();
        state.parents = vec![b"parent-id".to_vec()];
        let t0 = null_parent_details();
        let t1 = basis_details(b'f', b"sha", 1, false);
        state.dirblocks = vec![
            Dirblock {
                dirname: Vec::new(),
                entries: vec![entry_with_trees(
                    b"",
                    b"",
                    b"TREE_ROOT",
                    vec![tree(b'd'), tree(b'd')],
                )],
            },
            Dirblock {
                dirname: Vec::new(),
                entries: vec![entry_with_trees(
                    b"",
                    b"README",
                    b"fid-readme",
                    vec![t0, t1],
                )],
            },
        ];

        let deletes = vec![(
            b"README".to_vec(),
            None::<Vec<u8>>,
            b"fid-readme".to_vec(),
            true,
        )];
        state
            .update_basis_apply_deletes(&deletes)
            .expect("apply_deletes");
        assert!(state.dirblocks[1].entries.is_empty());
    }

    #[test]
    fn update_basis_apply_deletes_keeps_row_when_active_still_present() {
        // README has tree 0 live and tree 1 live; a non-real-delete
        // (split rename) should nullify tree 1 but keep the row.
        let mut state = basis_adds_fixture_one_file();
        state.dirblocks[1].entries[0].trees[1] = basis_details(b'f', b"sha-old", 10, false);

        let deletes = vec![(
            b"README".to_vec(),
            Some(b"README.new".to_vec()),
            b"fid-readme".to_vec(),
            false,
        )];
        state
            .update_basis_apply_deletes(&deletes)
            .expect("apply_deletes");
        let entry = &state.dirblocks[1].entries[0];
        assert_eq!(entry.trees[1].minikind, b'a');
        assert!(entry.trees[1].fingerprint.is_empty());
    }

    #[test]
    fn update_basis_apply_deletes_bad_delta_is_invalid() {
        let mut state = basis_adds_fixture_one_file();
        // real_delete=true but new_path=Some — inconsistent.
        let deletes = vec![(
            b"README".to_vec(),
            Some(b"README.new".to_vec()),
            b"fid-readme".to_vec(),
            true,
        )];
        let err = state.update_basis_apply_deletes(&deletes).unwrap_err();
        match err {
            BasisApplyError::Invalid { reason, .. } => {
                assert!(reason.contains("bad delete delta"), "{}", reason);
            }
            other => panic!("expected Invalid, got {:?}", other),
        }
    }

    #[test]
    fn update_basis_apply_deletes_missing_entry_is_invalid() {
        let mut state = basis_adds_fixture_one_file();
        let deletes = vec![(
            b"ghost".to_vec(),
            None::<Vec<u8>>,
            b"fid-ghost".to_vec(),
            true,
        )];
        let err = state.update_basis_apply_deletes(&deletes).unwrap_err();
        match err {
            BasisApplyError::Invalid { reason, .. } => {
                assert!(reason.contains("basis tree does not contain"), "{}", reason);
            }
            other => panic!("expected Invalid, got {:?}", other),
        }
    }

    #[test]
    fn update_basis_apply_adds_sorts_input_so_parents_come_first() {
        // Feed the adds out of order and confirm the function still
        // processes them correctly. We add `dir/` (a directory) and
        // then `dir/child`; the sort must place `dir/` first so the
        // directory block exists by the time the child is processed.
        let mut state = fresh_state();
        state.parents = vec![b"parent-id".to_vec()];
        state.dirblocks = vec![
            Dirblock {
                dirname: Vec::new(),
                entries: vec![entry_with_trees(
                    b"",
                    b"",
                    b"TREE_ROOT",
                    vec![tree(b'd'), tree(b'd')],
                )],
            },
            Dirblock {
                dirname: Vec::new(),
                entries: Vec::new(),
            },
        ];

        let mut adds = vec![
            BasisAdd {
                old_path: None,
                new_path: b"dir/child".to_vec(),
                file_id: b"fid-child".to_vec(),
                new_details: basis_details(b'f', b"sha-c", 1, false),
                real_add: true,
            },
            BasisAdd {
                old_path: None,
                new_path: b"dir".to_vec(),
                file_id: b"fid-dir".to_vec(),
                new_details: basis_details(b'd', b"", 0, false),
                real_add: true,
            },
        ];
        state.update_basis_apply_adds(&mut adds).expect("apply");

        // After the adds: a dirblock for b"dir" exists, the contents-of-root
        // block holds dir, and a dedicated dir block holds dir/child.
        let dirblock_names: Vec<&[u8]> = state
            .dirblocks
            .iter()
            .map(|b| b.dirname.as_slice())
            .collect();
        assert!(dirblock_names.iter().any(|&n| n == b"dir"));
        let dir_block = state
            .dirblocks
            .iter()
            .find(|b| b.dirname == b"dir")
            .unwrap();
        assert_eq!(dir_block.entries.len(), 1);
        assert_eq!(dir_block.entries[0].key.basename, b"child".to_vec());
    }

    /// Build a fresh single-tree dirstate with a live root entry and
    /// an empty contents-of-root block. Suitable for exercising
    /// `update_by_delta` adds/removes.
    fn one_tree_root_state() -> DirState {
        let mut state = fresh_state();
        state.dirblocks = vec![
            Dirblock {
                dirname: Vec::new(),
                entries: vec![entry_with_trees(b"", b"", b"TREE_ROOT", vec![tree(b'd')])],
            },
            Dirblock {
                dirname: Vec::new(),
                entries: Vec::new(),
            },
        ];
        state
    }

    #[test]
    fn update_by_delta_add_file_at_root() {
        // Minimal add: one row inserts README under the root.
        let mut state = one_tree_root_state();
        let entries = vec![FlatDeltaEntry {
            old_path: None,
            new_path: Some(b"README".to_vec()),
            file_id: b"fid-r".to_vec(),
            parent_id: Some(b"TREE_ROOT".to_vec()),
            minikind: b'f',
            executable: false,
            fingerprint: Vec::new(),
        }];
        state.update_by_delta(entries).expect("update_by_delta");

        let bei = get_block_entry_index(&state.dirblocks, b"", b"README", 0);
        assert!(bei.path_present);
        let entry = &state.dirblocks[bei.block_index].entries[bei.entry_index];
        assert_eq!(entry.trees[0].minikind, b'f');
        assert_eq!(entry.key.file_id, b"fid-r".to_vec());
    }

    #[test]
    fn update_by_delta_delete_then_reinsert_different_id_is_rejected() {
        // Adding a file id already present (not part of the
        // simultaneous delete) must fail via check_delta_ids_absent.
        let mut state = one_tree_root_state();
        state.dirblocks[1].entries.push(entry_with_trees(
            b"",
            b"README",
            b"fid-existing",
            vec![tree(b'f')],
        ));

        let entries = vec![FlatDeltaEntry {
            old_path: None,
            new_path: Some(b"OTHER".to_vec()),
            file_id: b"fid-existing".to_vec(),
            parent_id: Some(b"TREE_ROOT".to_vec()),
            minikind: b'f',
            executable: false,
            fingerprint: Vec::new(),
        }];
        let err = state.update_by_delta(entries).unwrap_err();
        match err {
            BasisApplyError::Invalid { file_id, .. } => {
                assert_eq!(file_id, b"fid-existing".to_vec());
            }
            other => panic!("expected Invalid, got {:?}", other),
        }
    }

    #[test]
    fn update_by_delta_repeated_file_id_is_rejected() {
        // Two delta rows touching the same file_id must fail — this
        // matches Python's "repeated file_id" _raise_invalid branch.
        let mut state = one_tree_root_state();
        let entries = vec![
            FlatDeltaEntry {
                old_path: None,
                new_path: Some(b"a".to_vec()),
                file_id: b"fid-dup".to_vec(),
                parent_id: Some(b"TREE_ROOT".to_vec()),
                minikind: b'f',
                executable: false,
                fingerprint: Vec::new(),
            },
            FlatDeltaEntry {
                old_path: None,
                new_path: Some(b"b".to_vec()),
                file_id: b"fid-dup".to_vec(),
                parent_id: Some(b"TREE_ROOT".to_vec()),
                minikind: b'f',
                executable: false,
                fingerprint: Vec::new(),
            },
        ];
        let err = state.update_by_delta(entries).unwrap_err();
        match err {
            BasisApplyError::Invalid { reason, .. } => {
                assert_eq!(reason, "repeated file_id");
            }
            other => panic!("expected Invalid, got {:?}", other),
        }
    }

    #[test]
    fn update_by_delta_rename_expands_children() {
        // Rename a/ -> z/ when a/ has a child a/f. After the delta:
        // a/ and a/f should be gone from tree 0, and z/ + z/f should
        // be present.
        let mut state = fresh_state();
        state.dirblocks = vec![
            Dirblock {
                dirname: Vec::new(),
                entries: vec![entry_with_trees(b"", b"", b"TREE_ROOT", vec![tree(b'd')])],
            },
            Dirblock {
                dirname: Vec::new(),
                entries: vec![entry_with_trees(b"", b"a", b"a-dir", vec![tree(b'd')])],
            },
            Dirblock {
                dirname: b"a".to_vec(),
                entries: vec![entry_with_trees(b"a", b"f", b"f-file", vec![tree(b'f')])],
            },
        ];

        let entries = vec![FlatDeltaEntry {
            old_path: Some(b"a".to_vec()),
            new_path: Some(b"z".to_vec()),
            file_id: b"a-dir".to_vec(),
            parent_id: Some(b"TREE_ROOT".to_vec()),
            minikind: b'd',
            executable: false,
            fingerprint: Vec::new(),
        }];
        state.update_by_delta(entries).expect("rename");

        // Old paths are now absent/relocated (make_absent), new paths
        // are present.
        let old_a = get_block_entry_index(&state.dirblocks, b"", b"a", 0);
        assert!(!old_a.path_present, "a should be gone from tree 0");
        let old_af = get_block_entry_index(&state.dirblocks, b"a", b"f", 0);
        assert!(!old_af.path_present, "a/f should be gone from tree 0");

        let new_z = get_block_entry_index(&state.dirblocks, b"", b"z", 0);
        assert!(new_z.path_present, "z should be present in tree 0");
        let new_zf = get_block_entry_index(&state.dirblocks, b"z", b"f", 0);
        assert!(new_zf.path_present, "z/f should be present in tree 0");
        assert_eq!(
            state.dirblocks[new_zf.block_index].entries[new_zf.entry_index]
                .key
                .file_id,
            b"f-file".to_vec()
        );
    }

    /// Build a two-tree dirstate suitable for `update_basis_by_delta`
    /// tests: tree 0 and tree 1 both contain the root and a single
    /// README file with different fingerprints.
    fn two_tree_basis_state() -> DirState {
        let mut state = fresh_state();
        state.parents = vec![b"old-revid".to_vec()];
        state.dirblocks = vec![
            Dirblock {
                dirname: Vec::new(),
                entries: vec![entry_with_trees(
                    b"",
                    b"",
                    b"TREE_ROOT",
                    vec![tree(b'd'), tree(b'd')],
                )],
            },
            Dirblock {
                dirname: Vec::new(),
                entries: vec![entry_with_trees(
                    b"",
                    b"README",
                    b"fid-readme",
                    vec![
                        basis_details(b'f', b"sha-cur", 10, false),
                        basis_details(b'f', b"sha-old", 10, false),
                    ],
                )],
            },
        ];
        state
    }

    #[test]
    fn update_basis_by_delta_in_place_change() {
        // In-place change of README: keep tree 0 untouched, update
        // tree 1 fingerprint.
        let mut state = two_tree_basis_state();
        let entries = vec![FlatBasisDeltaEntry {
            old_path: Some(b"README".to_vec()),
            new_path: Some(b"README".to_vec()),
            file_id: b"fid-readme".to_vec(),
            parent_id: Some(b"TREE_ROOT".to_vec()),
            details: Some((b'f', b"sha-new".to_vec(), 20, false, b"new-revid".to_vec())),
        }];
        state
            .update_basis_by_delta(entries, b"new-revid".to_vec())
            .expect("update_basis_by_delta");

        let bei = get_block_entry_index(&state.dirblocks, b"", b"README", 1);
        assert!(bei.path_present);
        let entry = &state.dirblocks[bei.block_index].entries[bei.entry_index];
        assert_eq!(entry.trees[1].fingerprint, b"sha-new".to_vec());
        assert_eq!(entry.trees[1].size, 20);
        // Tree 0 is untouched.
        assert_eq!(entry.trees[0].fingerprint, b"sha-cur".to_vec());
    }

    #[test]
    fn update_basis_by_delta_add_new_file() {
        // Add NEWFILE to tree 1 only.
        let mut state = two_tree_basis_state();
        let entries = vec![FlatBasisDeltaEntry {
            old_path: None,
            new_path: Some(b"NEWFILE".to_vec()),
            file_id: b"fid-new".to_vec(),
            parent_id: Some(b"TREE_ROOT".to_vec()),
            details: Some((b'f', b"sha-new".to_vec(), 5, false, b"new-revid".to_vec())),
        }];
        state
            .update_basis_by_delta(entries, b"new-revid".to_vec())
            .expect("update_basis_by_delta");

        let bei = get_block_entry_index(&state.dirblocks, b"", b"NEWFILE", 1);
        assert!(bei.path_present);
        let entry = &state.dirblocks[bei.block_index].entries[bei.entry_index];
        assert_eq!(entry.trees[1].minikind, b'f');
        assert_eq!(entry.key.file_id, b"fid-new".to_vec());
    }

    #[test]
    fn update_basis_by_delta_rename_directory_with_child() {
        // Rename a/ -> z/ when a/ has a child a/f in tree 1. After
        // the delta the rename child-expansion must emit add+delete
        // pairs for a/f so that tree 1 ends up with z/ + z/f live
        // and a/ + a/f gone. This exercises the mid-loop
        // apply_deletes drain + iter_child_entries(1, ...) walk.
        let mut state = fresh_state();
        state.parents = vec![b"old-revid".to_vec()];
        state.dirblocks = vec![
            Dirblock {
                dirname: Vec::new(),
                entries: vec![entry_with_trees(
                    b"",
                    b"",
                    b"TREE_ROOT",
                    vec![tree(b'd'), tree(b'd')],
                )],
            },
            Dirblock {
                dirname: Vec::new(),
                entries: vec![entry_with_trees(
                    b"",
                    b"a",
                    b"a-dir",
                    vec![tree(b'd'), tree(b'd')],
                )],
            },
            Dirblock {
                dirname: b"a".to_vec(),
                entries: vec![entry_with_trees(
                    b"a",
                    b"f",
                    b"f-file",
                    vec![
                        basis_details(b'f', b"sha-cur-f", 3, false),
                        basis_details(b'f', b"sha-old-f", 3, false),
                    ],
                )],
            },
        ];

        let entries = vec![FlatBasisDeltaEntry {
            old_path: Some(b"a".to_vec()),
            new_path: Some(b"z".to_vec()),
            file_id: b"a-dir".to_vec(),
            parent_id: Some(b"TREE_ROOT".to_vec()),
            details: Some((b'd', Vec::new(), 0, false, b"new-revid".to_vec())),
        }];
        state
            .update_basis_by_delta(entries, b"new-revid".to_vec())
            .expect("update_basis_by_delta");

        // Tree 1: a and a/f should no longer be live.
        let old_a = get_block_entry_index(&state.dirblocks, b"", b"a", 1);
        assert!(!old_a.path_present, "a should be gone from tree 1");
        let old_af = get_block_entry_index(&state.dirblocks, b"a", b"f", 1);
        assert!(!old_af.path_present, "a/f should be gone from tree 1");

        // Tree 1: z and z/f should now be live.
        let new_z = get_block_entry_index(&state.dirblocks, b"", b"z", 1);
        assert!(new_z.path_present, "z should be present in tree 1");
        let new_zf = get_block_entry_index(&state.dirblocks, b"z", b"f", 1);
        assert!(new_zf.path_present, "z/f should be present in tree 1");
        assert_eq!(
            state.dirblocks[new_zf.block_index].entries[new_zf.entry_index]
                .key
                .file_id,
            b"f-file".to_vec()
        );
    }

    #[test]
    fn update_basis_by_delta_delete_file() {
        // Delete README from tree 1 (old_path set, new_path None).
        let mut state = two_tree_basis_state();
        let entries = vec![FlatBasisDeltaEntry {
            old_path: Some(b"README".to_vec()),
            new_path: None,
            file_id: b"fid-readme".to_vec(),
            parent_id: None,
            details: None,
        }];
        state
            .update_basis_by_delta(entries, b"new-revid".to_vec())
            .expect("update_basis_by_delta");

        // After delete: tree 1 for README is absent.
        let bei = get_block_entry_index(&state.dirblocks, b"", b"README", 1);
        assert!(!bei.path_present);
    }

    #[test]
    fn get_or_build_id_index_caches_result() {
        let mut state = create_complex_dirstate();
        assert!(state.id_index.is_none());
        state.get_or_build_id_index();
        assert!(state.id_index.is_some());
        // Second call does not rebuild — we can't observe that
        // directly, but we can verify the cache survives by mutating
        // `dirblocks` and re-calling: the cached index should still
        // point to the pre-mutation data. (Invalidation is the
        // caller's responsibility, as Python documents.)
        state.dirblocks.clear();
        let idx_after = state.get_or_build_id_index();
        assert!(
            idx_after
                .get(&FileId::from(&b"a-root-value".to_vec()))
                .iter()
                .any(|(_, _, f)| f.as_bytes() == b"a-root-value"),
            "cache should survive dirblock mutation"
        );
    }

    /// Rust counterpart of Python
    /// `TestGetEntry.test_complex_structure_missing`.
    #[test]
    fn get_entry_by_path_complex_structure_missing() {
        let state = create_complex_dirstate();
        for path in [&b"_"[..], b"_\xc3\xa5", b"a/b", b"c/d"] {
            assert!(
                state.get_entry_by_path(0, path).is_none(),
                "expected None for {:?}",
                path
            );
        }
    }

    #[test]
    fn get_block_entry_index_simple_structure() {
        let state = create_dirstate_with_root_and_subdir();
        // subdir is present at (1, 0) in the contents-of-root block.
        let bei = state.get_block_entry_index(b"", b"subdir", 0);
        assert_eq!(bei.block_index, 1);
        assert_eq!(bei.entry_index, 0);
        assert!(bei.dir_present);
        assert!(bei.path_present);
        // bdir would sort before subdir — insertion point is still 0,
        // dir_present = true, path_present = false.
        let bei = state.get_block_entry_index(b"", b"bdir", 0);
        assert_eq!(bei.block_index, 1);
        assert_eq!(bei.entry_index, 0);
        assert!(bei.dir_present);
        assert!(!bei.path_present);
        // zdir would sort after subdir — insertion point is 1.
        let bei = state.get_block_entry_index(b"", b"zdir", 0);
        assert_eq!(bei.block_index, 1);
        assert_eq!(bei.entry_index, 1);
        assert!(bei.dir_present);
        assert!(!bei.path_present);
        // Non-existent parent directories — dir_present = false and the
        // block index is where they would be inserted (past the end).
        let bei = state.get_block_entry_index(b"a", b"foo", 0);
        assert_eq!(bei.block_index, 2);
        assert_eq!(bei.entry_index, 0);
        assert!(!bei.dir_present);
        assert!(!bei.path_present);
        let bei = state.get_block_entry_index(b"subdir", b"foo", 0);
        assert_eq!(bei.block_index, 2);
        assert!(!bei.dir_present);
        assert!(!bei.path_present);
    }

    /// Rust counterpart of Python
    /// `TestGetBlockRowIndex.test_complex_structure_exists`.
    #[test]
    fn get_block_entry_index_complex_structure_exists() {
        let state = create_complex_dirstate();
        // Root: (0, 0, true, true).
        let bei = state.get_block_entry_index(b"", b"", 0);
        assert_eq!(
            (
                bei.block_index,
                bei.entry_index,
                bei.dir_present,
                bei.path_present
            ),
            (0, 0, true, true)
        );
        // Root contents in block 1, each at their own index.
        for (i, basename) in [&b"a"[..], b"b", b"c", b"d"].iter().enumerate() {
            let bei = state.get_block_entry_index(b"", basename, 0);
            assert_eq!(
                (
                    bei.block_index,
                    bei.entry_index,
                    bei.dir_present,
                    bei.path_present
                ),
                (1, i, true, true),
                "root/{:?}",
                basename
            );
        }
        // a/e and a/f live in block 2.
        let bei = state.get_block_entry_index(b"a", b"e", 0);
        assert_eq!(
            (
                bei.block_index,
                bei.entry_index,
                bei.dir_present,
                bei.path_present
            ),
            (2, 0, true, true)
        );
        let bei = state.get_block_entry_index(b"a", b"f", 0);
        assert_eq!(
            (
                bei.block_index,
                bei.entry_index,
                bei.dir_present,
                bei.path_present
            ),
            (2, 1, true, true)
        );
        // b/g and b/h\xc3\xa5 live in block 3.
        let bei = state.get_block_entry_index(b"b", b"g", 0);
        assert_eq!(
            (
                bei.block_index,
                bei.entry_index,
                bei.dir_present,
                bei.path_present
            ),
            (3, 0, true, true)
        );
        let bei = state.get_block_entry_index(b"b", b"h\xc3\xa5", 0);
        assert_eq!(
            (
                bei.block_index,
                bei.entry_index,
                bei.dir_present,
                bei.path_present
            ),
            (3, 1, true, true)
        );
    }

    /// Rust counterpart of Python
    /// `TestGetBlockRowIndex.test_complex_structure_missing`. Checks
    /// that insertion points match Python's expectations for paths
    /// that don't yet exist in the complex dirstate.
    #[test]
    fn get_block_entry_index_complex_structure_missing() {
        let state = create_complex_dirstate();
        // Root row still present.
        let bei = state.get_block_entry_index(b"", b"", 0);
        assert_eq!(
            (
                bei.block_index,
                bei.entry_index,
                bei.dir_present,
                bei.path_present
            ),
            (0, 0, true, true)
        );
        // "_" sorts before "a" in the contents-of-root block.
        let bei = state.get_block_entry_index(b"", b"_", 0);
        assert_eq!(
            (
                bei.block_index,
                bei.entry_index,
                bei.dir_present,
                bei.path_present
            ),
            (1, 0, true, false)
        );
        // "aa" sorts between "a" (index 0) and "b" (index 1).
        let bei = state.get_block_entry_index(b"", b"aa", 0);
        assert_eq!(
            (
                bei.block_index,
                bei.entry_index,
                bei.dir_present,
                bei.path_present
            ),
            (1, 1, true, false)
        );
        // "h\xc3\xa5" sorts after "d" — insertion point is 4 (end of block).
        let bei = state.get_block_entry_index(b"", b"h\xc3\xa5", 0);
        assert_eq!(
            (
                bei.block_index,
                bei.entry_index,
                bei.dir_present,
                bei.path_present
            ),
            (1, 4, true, false)
        );
        // Directories that don't exist: _, aa, bb.
        let bei = state.get_block_entry_index(b"_", b"a", 0);
        assert_eq!(
            (
                bei.block_index,
                bei.entry_index,
                bei.dir_present,
                bei.path_present
            ),
            (2, 0, false, false)
        );
        let bei = state.get_block_entry_index(b"aa", b"a", 0);
        assert_eq!(
            (
                bei.block_index,
                bei.entry_index,
                bei.dir_present,
                bei.path_present
            ),
            (3, 0, false, false)
        );
        let bei = state.get_block_entry_index(b"bb", b"a", 0);
        assert_eq!(
            (
                bei.block_index,
                bei.entry_index,
                bei.dir_present,
                bei.path_present
            ),
            (4, 0, false, false)
        );
        // "a/e" as a dirname sorts component-wise between "a" (2) and "b" (3).
        let bei = state.get_block_entry_index(b"a/e", b"a", 0);
        assert_eq!(
            (
                bei.block_index,
                bei.entry_index,
                bei.dir_present,
                bei.path_present
            ),
            (3, 0, false, false)
        );
        // "e" comes after "b" — insertion point is 4 (past end).
        let bei = state.get_block_entry_index(b"e", b"a", 0);
        assert_eq!(
            (
                bei.block_index,
                bei.entry_index,
                bei.dir_present,
                bei.path_present
            ),
            (4, 0, false, false)
        );
    }

    #[test]
    fn dirstate_method_wrappers_delegate_to_free_functions() {
        let mut state = DirState::new(
            "dirstate",
            Box::new(DefaultSHA1Provider::new()),
            0,
            true,
            false,
        );
        state.dirblocks = make_dirblocks(vec![(
            b"dir",
            vec![entry_with_trees(b"dir", b"a", b"fid-a", vec![tree(b'f')])],
        )]);
        let key = EntryKey {
            dirname: b"dir".to_vec(),
            basename: b"a".to_vec(),
            file_id: b"fid-a".to_vec(),
        };
        assert_eq!(state.find_block_index_from_key(&key), (2, true));
        let block = &state.dirblocks[2].entries.clone();
        assert_eq!(state.find_entry_index(&key, block), (0, true));
        let bei = state.get_block_entry_index(b"dir", b"a", 0);
        assert_eq!(bei.block_index, 2);
        assert!(bei.path_present);
    }

    #[test]
    fn entry_to_line_single_tree_matches_expected_layout() {
        let nullstat = b"x".repeat(32);
        let entry = Entry {
            key: EntryKey {
                dirname: b"".to_vec(),
                basename: b"README".to_vec(),
                file_id: b"fid-readme".to_vec(),
            },
            trees: vec![TreeData {
                minikind: b'f',
                fingerprint: b"sha1value".to_vec(),
                size: 42,
                executable: true,
                packed_stat: nullstat.clone(),
            }],
        };
        let line = entry_to_line(&entry);
        let mut expected = Vec::new();
        expected.extend_from_slice(b"\x00README\x00fid-readme\x00f\x00sha1value\x0042\x00y\x00");
        expected.extend_from_slice(&nullstat);
        assert_eq!(line, expected);
    }

    #[test]
    fn entry_to_line_multi_tree() {
        let nullstat = b"x".repeat(32);
        let entry = Entry {
            key: EntryKey {
                dirname: b"sub".to_vec(),
                basename: b"f".to_vec(),
                file_id: b"fid".to_vec(),
            },
            trees: vec![
                TreeData {
                    minikind: b'f',
                    fingerprint: b"cur".to_vec(),
                    size: 7,
                    executable: false,
                    packed_stat: nullstat.clone(),
                },
                TreeData {
                    minikind: b'a',
                    fingerprint: b"".to_vec(),
                    size: 0,
                    executable: false,
                    packed_stat: nullstat.clone(),
                },
            ],
        };
        let line = entry_to_line(&entry);
        let mut expected = Vec::new();
        expected.extend_from_slice(b"sub\x00f\x00fid\x00f\x00cur\x007\x00n\x00");
        expected.extend_from_slice(&nullstat);
        expected.extend_from_slice(b"\x00a\x00\x000\x00n\x00");
        expected.extend_from_slice(&nullstat);
        assert_eq!(line, expected);
    }

    /// Rust counterpart of Python
    /// `TestGetLines.test_entry_to_line_with_parent`. Root entry with
    /// current tree details plus one parent whose "tree data" is the
    /// absent-pointer form `(b"a", <relocated-path>, 0, False, b"")`.
    #[test]
    fn entry_to_line_with_parent_matches_python_bytes() {
        let entry = Entry {
            key: EntryKey {
                dirname: b"".to_vec(),
                basename: b"".to_vec(),
                file_id: b"a-root-value".to_vec(),
            },
            trees: vec![
                TreeData {
                    minikind: b'd',
                    fingerprint: Vec::new(),
                    size: 0,
                    executable: false,
                    packed_stat: PACKED_STAT.to_vec(),
                },
                TreeData {
                    minikind: b'a',
                    fingerprint: b"dirname/basename".to_vec(),
                    size: 0,
                    executable: false,
                    packed_stat: Vec::new(),
                },
            ],
        };
        let expected: &[u8] = b"\x00\x00a-root-value\x00\
                                d\x00\x000\x00n\x00AAAAREUHaIpFB2iKAAADAQAtkqUAAIGk\x00\
                                a\x00dirname/basename\x000\x00n\x00";
        assert_eq!(entry_to_line(&entry), expected);
    }

    /// Rust counterpart of Python
    /// `TestGetLines.test_entry_to_line_with_two_parents_at_different_paths`.
    /// Root entry with current tree details, one parent at the same
    /// path, and a second parent whose data is the absent-pointer form
    /// pointing at `dirname/basename`.
    #[test]
    fn entry_to_line_with_two_parents_at_different_paths_matches_python_bytes() {
        let entry = Entry {
            key: EntryKey {
                dirname: b"".to_vec(),
                basename: b"".to_vec(),
                file_id: b"a-root-value".to_vec(),
            },
            trees: vec![
                TreeData {
                    minikind: b'd',
                    fingerprint: Vec::new(),
                    size: 0,
                    executable: false,
                    packed_stat: PACKED_STAT.to_vec(),
                },
                TreeData {
                    minikind: b'd',
                    fingerprint: Vec::new(),
                    size: 0,
                    executable: false,
                    packed_stat: b"rev_id".to_vec(),
                },
                TreeData {
                    minikind: b'a',
                    fingerprint: b"dirname/basename".to_vec(),
                    size: 0,
                    executable: false,
                    packed_stat: Vec::new(),
                },
            ],
        };
        let expected: &[u8] = b"\x00\x00a-root-value\x00\
                                d\x00\x000\x00n\x00AAAAREUHaIpFB2iKAAADAQAtkqUAAIGk\x00\
                                d\x00\x000\x00n\x00rev_id\x00\
                                a\x00dirname/basename\x000\x00n\x00";
        assert_eq!(entry_to_line(&entry), expected);
    }

    #[test]
    fn entry_to_line_round_trip_through_parse_dirblocks() {
        // Build a DirState, serialise it via get_lines, then feed the
        // body back through parse_dirblocks + split_root_dirblock_into_contents
        // and check the dirblocks survive the round-trip.
        let nullstat = b"x".repeat(32);
        let original = vec![
            Dirblock {
                dirname: Vec::new(),
                entries: vec![Entry {
                    key: EntryKey {
                        dirname: b"".to_vec(),
                        basename: b"".to_vec(),
                        file_id: b"TREE_ROOT".to_vec(),
                    },
                    trees: vec![TreeData {
                        minikind: b'd',
                        fingerprint: Vec::new(),
                        size: 0,
                        executable: false,
                        packed_stat: nullstat.clone(),
                    }],
                }],
            },
            Dirblock {
                dirname: Vec::new(),
                entries: vec![Entry {
                    key: EntryKey {
                        dirname: b"".to_vec(),
                        basename: b"README".to_vec(),
                        file_id: b"fid-readme".to_vec(),
                    },
                    trees: vec![TreeData {
                        minikind: b'f',
                        fingerprint: b"sha1".to_vec(),
                        size: 10,
                        executable: true,
                        packed_stat: nullstat.clone(),
                    }],
                }],
            },
        ];

        let mut state = DirState::new(
            "dirstate",
            Box::new(DefaultSHA1Provider::new()),
            0,
            true,
            false,
        );
        state.dirblocks = original.clone();

        let chunks = state.get_lines();
        let data: Vec<u8> = chunks.into_iter().flatten().collect();

        // Re-parse: two entries → get_output_lines writes num_entries=2.
        let header = read_header(&data).expect("parse header");
        assert_eq!(header.num_entries, 2);
        let body = &data[header.end_of_header..];
        let mut parsed = parse_dirblocks(body, 1, header.num_entries).expect("parse body");
        split_root_dirblock_into_contents(&mut parsed).expect("split");

        assert_eq!(parsed.len(), 2);
        // Block 0: just the root entry.
        assert_eq!(parsed[0].entries.len(), 1);
        assert_eq!(parsed[0].entries[0].key.file_id, b"TREE_ROOT".to_vec());
        // Block 1: the contents-of-root entry.
        assert_eq!(parsed[1].entries.len(), 1);
        assert_eq!(parsed[1].entries[0].key.basename, b"README".to_vec());
        assert_eq!(parsed[1].entries[0].trees[0].size, 10);
        assert!(parsed[1].entries[0].trees[0].executable);
    }

    #[test]
    fn dirstate_get_lines_matches_python_saved_bytes() {
        // The same single-entry layout we pinned earlier for
        // parse_dirblocks, but now produced by the Rust writer and
        // compared byte-for-byte.
        let nullstat = b"x".repeat(32);
        let mut state = DirState::new(
            "dirstate",
            Box::new(DefaultSHA1Provider::new()),
            0,
            true,
            false,
        );
        state.dirblocks = vec![
            Dirblock {
                dirname: Vec::new(),
                entries: vec![Entry {
                    key: EntryKey {
                        dirname: b"".to_vec(),
                        basename: b"".to_vec(),
                        file_id: b"TREE_ROOT".to_vec(),
                    },
                    trees: vec![TreeData {
                        minikind: b'd',
                        fingerprint: Vec::new(),
                        size: 0,
                        executable: false,
                        packed_stat: nullstat,
                    }],
                }],
            },
            Dirblock {
                dirname: Vec::new(),
                entries: Vec::new(),
            },
        ];
        let chunks = state.get_lines();
        let actual: Vec<u8> = chunks.into_iter().flatten().collect();
        let expected: &[u8] = b"#bazaar dirstate flat format 3\n\
                                crc32: 2823629280\n\
                                num_entries: 1\n\
                                0\x00\n\
                                \x000\x00\n\
                                \x00\x00\x00TREE_ROOT\x00d\x00\x000\x00n\x00xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\x00\n\x00";
        assert_eq!(actual, expected);
    }

    #[test]
    fn dirstate_get_lines_multi_tree_with_parent_matches_python() {
        // Cross-check against bytes produced by a real
        // `DirState.initialize(...); _set_data([b"rev-a"], [...]); save()`
        // cycle with one parent tree and a README file entry.
        let nullstat = b"x".repeat(32);
        let mut state = DirState::new(
            "dirstate",
            Box::new(DefaultSHA1Provider::new()),
            0,
            true,
            false,
        );
        state.parents = vec![b"rev-a".to_vec()];
        state.dirblocks = vec![
            Dirblock {
                dirname: Vec::new(),
                entries: vec![Entry {
                    key: EntryKey {
                        dirname: b"".to_vec(),
                        basename: b"".to_vec(),
                        file_id: b"TREE_ROOT".to_vec(),
                    },
                    trees: vec![
                        TreeData {
                            minikind: b'd',
                            fingerprint: Vec::new(),
                            size: 0,
                            executable: false,
                            packed_stat: nullstat.clone(),
                        },
                        TreeData {
                            minikind: b'd',
                            fingerprint: Vec::new(),
                            size: 0,
                            executable: false,
                            packed_stat: nullstat.clone(),
                        },
                    ],
                }],
            },
            Dirblock {
                dirname: Vec::new(),
                entries: vec![Entry {
                    key: EntryKey {
                        dirname: b"".to_vec(),
                        basename: b"README".to_vec(),
                        file_id: b"fid-readme".to_vec(),
                    },
                    trees: vec![
                        TreeData {
                            minikind: b'f',
                            fingerprint: b"sha-cur".to_vec(),
                            size: 10,
                            executable: true,
                            packed_stat: nullstat.clone(),
                        },
                        TreeData {
                            minikind: b'f',
                            fingerprint: b"sha-par".to_vec(),
                            size: 8,
                            executable: false,
                            packed_stat: nullstat,
                        },
                    ],
                }],
            },
        ];
        let chunks = state.get_lines();
        let actual: Vec<u8> = chunks.into_iter().flatten().collect();
        let expected: &[u8] = b"#bazaar dirstate flat format 3\n\
                                crc32: 2831533605\n\
                                num_entries: 2\n\
                                1\x00rev-a\x00\n\
                                \x000\x00\n\
                                \x00\x00\x00TREE_ROOT\x00d\x00\x000\x00n\x00xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\x00d\x00\x000\x00n\x00xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\x00\n\
                                \x00\x00README\x00fid-readme\x00f\x00sha-cur\x0010\x00y\x00xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\x00f\x00sha-par\x008\x00n\x00xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\x00\n\x00";
        assert_eq!(actual, expected);
    }

    fn fresh_state() -> DirState {
        DirState::new(
            "dirstate",
            Box::new(DefaultSHA1Provider::new()),
            0,
            true,
            false,
        )
    }

    fn entry_key(dirname: &[u8], basename: &[u8], file_id: &[u8]) -> EntryKey {
        EntryKey {
            dirname: dirname.to_vec(),
            basename: basename.to_vec(),
            file_id: file_id.to_vec(),
        }
    }

    #[test]
    fn mark_modified_no_hash_changes_marks_full_dirblock_state() {
        let mut state = fresh_state();
        state.dirblock_state = MemoryState::InMemoryUnmodified;
        state.mark_modified(&[], false);
        assert_eq!(state.dirblock_state, MemoryState::InMemoryModified);
        assert_eq!(state.header_state, MemoryState::NotInMemory);
        assert!(state.known_hash_changes.is_empty());
    }

    #[test]
    fn mark_modified_hash_only_promotes_unmodified_to_hash_modified() {
        let mut state = fresh_state();
        state.dirblock_state = MemoryState::InMemoryUnmodified;
        let key = entry_key(b"", b"README", b"fid-readme");
        state.mark_modified(&[key.clone()], false);
        assert_eq!(state.dirblock_state, MemoryState::InMemoryHashModified);
        assert!(state.known_hash_changes.contains(&key));
    }

    #[test]
    fn mark_modified_hash_only_promotes_not_in_memory_to_hash_modified() {
        let mut state = fresh_state();
        assert_eq!(state.dirblock_state, MemoryState::NotInMemory);
        state.mark_modified(&[entry_key(b"", b"a", b"fid-a")], false);
        assert_eq!(state.dirblock_state, MemoryState::InMemoryHashModified);
    }

    #[test]
    fn mark_modified_hash_only_leaves_in_memory_modified_alone() {
        // If the dirstate is already fully modified, a hash-only change
        // must not downgrade it back to InMemoryHashModified — Python's
        // comment explicitly flags the precedence rule.
        let mut state = fresh_state();
        state.dirblock_state = MemoryState::InMemoryModified;
        state.mark_modified(&[entry_key(b"", b"a", b"fid-a")], false);
        assert_eq!(state.dirblock_state, MemoryState::InMemoryModified);
    }

    #[test]
    fn mark_modified_header_flag_promotes_header_state() {
        let mut state = fresh_state();
        state.header_state = MemoryState::InMemoryUnmodified;
        state.mark_modified(&[], true);
        assert_eq!(state.header_state, MemoryState::InMemoryModified);
        assert_eq!(state.dirblock_state, MemoryState::InMemoryModified);
    }

    #[test]
    fn num_present_parents_subtracts_ghosts() {
        let mut state = fresh_state();
        state.parents = vec![b"rev-a".to_vec(), b"rev-b".to_vec(), b"rev-c".to_vec()];
        state.ghosts = vec![b"rev-b".to_vec()];
        assert_eq!(state.num_present_parents(), 2);
    }

    #[test]
    fn num_present_parents_no_parents() {
        let state = fresh_state();
        assert_eq!(state.num_present_parents(), 0);
    }

    #[test]
    fn num_present_parents_saturates_when_ghosts_exceed_parents() {
        // Defensive: if somehow ghosts > parents we return 0 rather than
        // underflow. Python would raise a ValueError from `-` on ints,
        // but saturating is safer and less surprising.
        let mut state = fresh_state();
        state.parents = vec![b"rev-a".to_vec()];
        state.ghosts = vec![b"g1".to_vec(), b"g2".to_vec()];
        assert_eq!(state.num_present_parents(), 0);
    }

    #[test]
    fn entries_to_current_state_builds_expected_dirblock_layout() {
        let mut state = fresh_state();
        let nullstat = b"x".repeat(32);
        let mk_entry = |dirname: &[u8], basename: &[u8], file_id: &[u8]| Entry {
            key: EntryKey {
                dirname: dirname.to_vec(),
                basename: basename.to_vec(),
                file_id: file_id.to_vec(),
            },
            trees: vec![TreeData {
                minikind: b'd',
                fingerprint: Vec::new(),
                size: 0,
                executable: false,
                packed_stat: nullstat.clone(),
            }],
        };
        let new_entries = vec![
            mk_entry(b"", b"", b"TREE_ROOT"),
            mk_entry(b"", b"README", b"fid-readme"),
            mk_entry(b"", b"sub", b"fid-sub"),
            mk_entry(b"sub", b"inner", b"fid-inner"),
        ];
        state
            .entries_to_current_state(new_entries)
            .expect("entries_to_current_state");

        // Two sentinels + one real block for "sub".
        assert_eq!(state.dirblocks.len(), 3);
        // Block 0 holds just the root entry.
        assert_eq!(state.dirblocks[0].entries.len(), 1);
        assert_eq!(
            state.dirblocks[0].entries[0].key.file_id,
            b"TREE_ROOT".to_vec()
        );
        // Block 1 holds README and sub (the root's contents, post-split).
        assert_eq!(state.dirblocks[1].entries.len(), 2);
        assert_eq!(
            state.dirblocks[1].entries[0].key.basename,
            b"README".to_vec()
        );
        assert_eq!(state.dirblocks[1].entries[1].key.basename, b"sub".to_vec());
        // Block 2 is the real "sub" block holding inner.
        assert_eq!(state.dirblocks[2].dirname, b"sub".to_vec());
        assert_eq!(state.dirblocks[2].entries.len(), 1);
        assert_eq!(
            state.dirblocks[2].entries[0].key.basename,
            b"inner".to_vec()
        );
    }

    #[test]
    fn entries_to_current_state_rejects_missing_root_row() {
        let mut state = fresh_state();
        let entry = Entry {
            key: EntryKey {
                dirname: b"".to_vec(),
                basename: b"README".to_vec(),
                file_id: b"fid".to_vec(),
            },
            trees: vec![tree(b'f')],
        };
        match state.entries_to_current_state(vec![entry]) {
            Err(EntriesToStateError::MissingRootRow { key }) => {
                assert_eq!(key.basename, b"README".to_vec());
            }
            other => panic!("expected MissingRootRow, got {:?}", other),
        }
    }

    #[test]
    fn entries_to_current_state_rejects_empty_list() {
        let mut state = fresh_state();
        assert_eq!(
            state.entries_to_current_state(Vec::new()),
            Err(EntriesToStateError::Empty)
        );
    }

    #[test]
    fn ensure_block_root_shortcut_returns_one() {
        let mut state = fresh_state();
        state.dirblocks = make_dirblocks(vec![]);
        // Root row coordinates: block 0, row 0, dirname=b"".
        assert_eq!(state.ensure_block(0, 0, b""), Ok(1));
        // No new block was created — we still have just the two
        // sentinel blocks.
        assert_eq!(state.dirblocks.len(), 2);
    }

    #[test]
    fn ensure_block_creates_missing_block() {
        let mut state = fresh_state();
        // Root entry lives in the first sentinel block's row 0.
        state.dirblocks = vec![
            Dirblock {
                dirname: Vec::new(),
                entries: vec![entry_with_trees(b"", b"", b"TREE_ROOT", vec![tree(b'd')])],
            },
            Dirblock {
                dirname: Vec::new(),
                entries: vec![entry_with_trees(b"", b"sub", b"fid-sub", vec![tree(b'd')])],
            },
        ];
        // Parent entry at block 1, row 0 has basename "sub"; "sub"
        // ends with "sub", so the assertion passes.
        let idx = state.ensure_block(1, 0, b"sub").expect("ensure");
        // A new block for dirname=b"sub" should have been inserted.
        assert_eq!(idx, 2);
        assert_eq!(state.dirblocks.len(), 3);
        assert_eq!(state.dirblocks[2].dirname, b"sub".to_vec());
        assert!(state.dirblocks[2].entries.is_empty());
    }

    #[test]
    fn ensure_block_idempotent_for_existing_block() {
        let mut state = fresh_state();
        state.dirblocks = vec![
            Dirblock {
                dirname: Vec::new(),
                entries: vec![entry_with_trees(b"", b"", b"TREE_ROOT", vec![tree(b'd')])],
            },
            Dirblock {
                dirname: Vec::new(),
                entries: vec![entry_with_trees(b"", b"sub", b"fid-sub", vec![tree(b'd')])],
            },
            Dirblock {
                dirname: b"sub".to_vec(),
                entries: vec![],
            },
        ];
        let idx = state.ensure_block(1, 0, b"sub").expect("ensure");
        assert_eq!(idx, 2);
        assert_eq!(state.dirblocks.len(), 3);
    }

    #[test]
    fn ensure_block_rejects_bad_dirname() {
        let mut state = fresh_state();
        state.dirblocks = vec![
            Dirblock {
                dirname: Vec::new(),
                entries: vec![entry_with_trees(b"", b"", b"TREE_ROOT", vec![tree(b'd')])],
            },
            Dirblock {
                dirname: Vec::new(),
                entries: vec![entry_with_trees(b"", b"sub", b"fid-sub", vec![tree(b'd')])],
            },
        ];
        // dirname "other" does not end with parent basename "sub".
        let err = state.ensure_block(1, 0, b"other").expect_err("bad dirname");
        assert_eq!(err, EnsureBlockError::BadDirname(b"other".to_vec()));
        // No block was inserted.
        assert_eq!(state.dirblocks.len(), 2);
    }

    #[test]
    fn mark_unmodified_resets_everything() {
        let mut state = fresh_state();
        state.header_state = MemoryState::InMemoryModified;
        state.dirblock_state = MemoryState::InMemoryHashModified;
        state
            .known_hash_changes
            .insert(entry_key(b"", b"x", b"fid"));
        state.mark_unmodified();
        assert_eq!(state.header_state, MemoryState::InMemoryUnmodified);
        assert_eq!(state.dirblock_state, MemoryState::InMemoryUnmodified);
        assert!(state.known_hash_changes.is_empty());
    }

    #[test]
    fn dirstate_split_root_dirblock_method_wires_through() {
        // Verify the `DirState::split_root_dirblock_into_contents` method
        // calls the free function on its own `dirblocks` field.
        let mut state = DirState::new(
            "dirstate",
            Box::new(DefaultSHA1Provider::new()),
            0,
            true,
            false,
        );
        state.dirblocks = vec![
            Dirblock {
                dirname: Vec::new(),
                entries: vec![
                    make_entry(b"", b"", b"TREE_ROOT"),
                    make_entry(b"", b"README", b"fid-readme"),
                ],
            },
            Dirblock {
                dirname: Vec::new(),
                entries: Vec::new(),
            },
        ];
        state.split_root_dirblock_into_contents().expect("split");
        assert_eq!(state.dirblocks[0].entries.len(), 1);
        assert_eq!(state.dirblocks[1].entries.len(), 1);
        assert_eq!(
            state.dirblocks[1].entries[0].key.basename,
            b"README".to_vec()
        );
    }

    #[test]
    fn parse_dirblocks_rejects_bad_size() {
        // Build a body with an invalid size field. Hand-craft to bypass the
        // `entry_line` helper which only takes u64 sizes.
        let nullstat = b"x".repeat(32);
        let mut entry = Vec::new();
        entry.extend_from_slice(b"");
        entry.push(0);
        entry.extend_from_slice(b"");
        entry.push(0);
        entry.extend_from_slice(b"TREE_ROOT");
        entry.push(0);
        entry.extend_from_slice(b"d");
        entry.push(0);
        entry.extend_from_slice(b"");
        entry.push(0);
        entry.extend_from_slice(b"not-a-number");
        entry.push(0);
        entry.push(b'n');
        entry.push(0);
        entry.extend_from_slice(nullstat.as_slice());

        let body = make_body_bytes(&[], &[], &[entry]);
        match parse_dirblocks(&body, 1, 1) {
            Err(DirblocksError::BadSize(bytes)) => {
                assert_eq!(bytes, b"not-a-number".to_vec());
            }
            other => panic!("expected BadSize, got {:?}", other),
        }
    }

    /// In-memory [`Transport`] for tests and non-persistent use. Holds
    /// the file contents in a `Vec<u8>`; lock state is tracked
    /// explicitly so the tests can verify the state transitions.
    /// Additionally maintains a simple `path -> (StatInfo, Option<symlink_target>)`
    /// map so tests that exercise `lstat`/`read_link` can pre-seed
    /// working-tree file metadata.
    struct MemoryTransport {
        contents: Option<Vec<u8>>,
        lock: Option<LockState>,
        fs: std::collections::HashMap<Vec<u8>, (StatInfo, Option<Vec<u8>>)>,
    }

    impl MemoryTransport {
        fn new() -> Self {
            Self {
                contents: None,
                lock: None,
                fs: std::collections::HashMap::new(),
            }
        }

        fn with_contents(bytes: &[u8]) -> Self {
            Self {
                contents: Some(bytes.to_vec()),
                lock: None,
                fs: std::collections::HashMap::new(),
            }
        }

        #[allow(dead_code)]
        fn set_fs(&mut self, path: &[u8], info: StatInfo, symlink_target: Option<Vec<u8>>) {
            self.fs.insert(path.to_vec(), (info, symlink_target));
        }
    }

    impl Transport for MemoryTransport {
        fn exists(&self) -> Result<bool, TransportError> {
            Ok(self.contents.is_some())
        }

        fn lock_read(&mut self) -> Result<(), TransportError> {
            if self.lock.is_some() {
                return Err(TransportError::AlreadyLocked);
            }
            self.lock = Some(LockState::Read);
            Ok(())
        }

        fn lock_write(&mut self) -> Result<(), TransportError> {
            if self.lock.is_some() {
                return Err(TransportError::AlreadyLocked);
            }
            self.lock = Some(LockState::Write);
            // A write lock creates the file if it does not yet exist,
            // matching the semantics of `lock.WriteLock` in Python.
            if self.contents.is_none() {
                self.contents = Some(Vec::new());
            }
            Ok(())
        }

        fn unlock(&mut self) -> Result<(), TransportError> {
            if self.lock.is_none() {
                return Err(TransportError::NotLocked);
            }
            self.lock = None;
            Ok(())
        }

        fn lock_state(&self) -> Option<LockState> {
            self.lock
        }

        fn read_all(&mut self) -> Result<Vec<u8>, TransportError> {
            if self.lock.is_none() {
                return Err(TransportError::NotLocked);
            }
            self.contents
                .clone()
                .ok_or_else(|| TransportError::NotFound("memory".to_string()))
        }

        fn write_all(&mut self, bytes: &[u8]) -> Result<(), TransportError> {
            match self.lock {
                Some(LockState::Write) => {}
                Some(LockState::Read) => {
                    return Err(TransportError::Other(
                        "write_all requires a write lock".to_string(),
                    ));
                }
                None => return Err(TransportError::NotLocked),
            }
            self.contents = Some(bytes.to_vec());
            Ok(())
        }

        fn fdatasync(&mut self) -> Result<(), TransportError> {
            // No-op for in-memory transport; the call is still valid
            // so `DirState.save` can call it unconditionally.
            Ok(())
        }

        fn lstat(&self, abspath: &[u8]) -> Result<StatInfo, TransportError> {
            self.fs.get(abspath).map(|(info, _)| *info).ok_or_else(|| {
                TransportError::NotFound(String::from_utf8_lossy(abspath).into_owned())
            })
        }

        fn read_link(&self, abspath: &[u8]) -> Result<Vec<u8>, TransportError> {
            self.fs
                .get(abspath)
                .and_then(|(_, link)| link.clone())
                .ok_or_else(|| {
                    TransportError::NotFound(String::from_utf8_lossy(abspath).into_owned())
                })
        }
    }

    #[test]
    fn transport_exists_reports_contents_presence() {
        let empty = MemoryTransport::new();
        assert!(!empty.exists().unwrap());
        let populated = MemoryTransport::with_contents(b"hi");
        assert!(populated.exists().unwrap());
    }

    #[test]
    fn transport_read_all_requires_lock() {
        let mut t = MemoryTransport::with_contents(b"hi");
        assert_eq!(t.read_all().unwrap_err(), TransportError::NotLocked);
        t.lock_read().unwrap();
        assert_eq!(t.read_all().unwrap(), b"hi".to_vec());
    }

    #[test]
    fn transport_write_all_requires_write_lock() {
        let mut t = MemoryTransport::with_contents(b"hi");
        // No lock at all.
        assert_eq!(t.write_all(b"new").unwrap_err(), TransportError::NotLocked);
        // Read lock is not enough.
        t.lock_read().unwrap();
        assert!(matches!(
            t.write_all(b"new").unwrap_err(),
            TransportError::Other(_)
        ));
        t.unlock().unwrap();
        // Write lock works.
        t.lock_write().unwrap();
        t.write_all(b"new").unwrap();
        assert_eq!(t.read_all().unwrap(), b"new".to_vec());
    }

    #[test]
    fn transport_write_all_truncates_trailing_bytes() {
        let mut t = MemoryTransport::with_contents(b"previous long contents");
        t.lock_write().unwrap();
        t.write_all(b"short").unwrap();
        assert_eq!(t.read_all().unwrap(), b"short".to_vec());
    }

    #[test]
    fn transport_lock_write_creates_missing_file() {
        let mut t = MemoryTransport::new();
        assert!(!t.exists().unwrap());
        t.lock_write().unwrap();
        // After lock_write the file exists (empty), matching Python's
        // `lock.WriteLock` behaviour.
        assert!(t.exists().unwrap());
        assert_eq!(t.read_all().unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn transport_double_lock_is_error() {
        let mut t = MemoryTransport::with_contents(b"");
        t.lock_read().unwrap();
        assert_eq!(t.lock_read().unwrap_err(), TransportError::AlreadyLocked);
        assert_eq!(t.lock_write().unwrap_err(), TransportError::AlreadyLocked);
    }

    #[test]
    fn transport_unlock_without_lock_is_error() {
        let mut t = MemoryTransport::with_contents(b"");
        assert_eq!(t.unlock().unwrap_err(), TransportError::NotLocked);
    }

    #[test]
    fn transport_lock_state_tracks_current_lock() {
        let mut t = MemoryTransport::with_contents(b"");
        assert_eq!(t.lock_state(), None);
        t.lock_read().unwrap();
        assert_eq!(t.lock_state(), Some(LockState::Read));
        t.unlock().unwrap();
        assert_eq!(t.lock_state(), None);
        t.lock_write().unwrap();
        assert_eq!(t.lock_state(), Some(LockState::Write));
    }

    #[test]
    fn transport_fdatasync_is_noop_on_memory_transport() {
        let mut t = MemoryTransport::with_contents(b"");
        // fdatasync without a lock is also fine — it just flushes
        // whatever is already committed.
        t.fdatasync().unwrap();
        t.lock_write().unwrap();
        t.fdatasync().unwrap();
    }

    #[test]
    fn transport_round_trip_through_get_lines_and_read_header() {
        // End-to-end sanity: write a serialised DirState to the
        // transport via write_all, read it back via read_all, then
        // parse the header out of the returned bytes.
        let nullstat = b"x".repeat(32);
        let mut state = fresh_state();
        state.dirblocks = vec![
            Dirblock {
                dirname: Vec::new(),
                entries: vec![Entry {
                    key: EntryKey {
                        dirname: b"".to_vec(),
                        basename: b"".to_vec(),
                        file_id: b"TREE_ROOT".to_vec(),
                    },
                    trees: vec![TreeData {
                        minikind: b'd',
                        fingerprint: Vec::new(),
                        size: 0,
                        executable: false,
                        packed_stat: nullstat,
                    }],
                }],
            },
            Dirblock {
                dirname: Vec::new(),
                entries: Vec::new(),
            },
        ];
        let chunks = state.get_lines();
        let bytes: Vec<u8> = chunks.into_iter().flatten().collect();

        let mut t = MemoryTransport::new();
        t.lock_write().unwrap();
        t.write_all(&bytes).unwrap();
        t.unlock().unwrap();

        t.lock_read().unwrap();
        let read_back = t.read_all().unwrap();
        assert_eq!(read_back, bytes);
        let header = read_header(&read_back).expect("header parses");
        assert_eq!(header.num_entries, 1);
    }

    /// Build a minimal in-memory DirState whose dirblocks are the two
    /// empty-dirname sentinel blocks plus a single TREE_ROOT entry —
    /// the smallest shape `get_lines` accepts without panicking.
    fn minimal_populated_state() -> DirState {
        let nullstat = b"x".repeat(32);
        let mut state = fresh_state();
        state.dirblocks = vec![
            Dirblock {
                dirname: Vec::new(),
                entries: vec![Entry {
                    key: EntryKey {
                        dirname: b"".to_vec(),
                        basename: b"".to_vec(),
                        file_id: b"TREE_ROOT".to_vec(),
                    },
                    trees: vec![TreeData {
                        minikind: b'd',
                        fingerprint: Vec::new(),
                        size: 0,
                        executable: false,
                        packed_stat: nullstat,
                    }],
                }],
            },
            Dirblock {
                dirname: Vec::new(),
                entries: Vec::new(),
            },
        ];
        state
    }

    #[test]
    fn worth_saving_full_dirblock_modification_always_saves() {
        let mut state = fresh_state();
        state.dirblock_state = MemoryState::InMemoryModified;
        assert!(state.worth_saving());
    }

    #[test]
    fn worth_saving_header_modification_always_saves() {
        let mut state = fresh_state();
        state.header_state = MemoryState::InMemoryModified;
        assert!(state.worth_saving());
    }

    #[test]
    fn worth_saving_unmodified_state_is_not_worth_saving() {
        let mut state = fresh_state();
        state.header_state = MemoryState::InMemoryUnmodified;
        state.dirblock_state = MemoryState::InMemoryUnmodified;
        assert!(!state.worth_saving());
    }

    #[test]
    fn worth_saving_hash_only_under_limit_is_not_worth_saving() {
        let mut state = fresh_state();
        state.worth_saving_limit = 5;
        state.dirblock_state = MemoryState::InMemoryHashModified;
        state
            .known_hash_changes
            .insert(entry_key(b"", b"a", b"fid-a"));
        assert!(!state.worth_saving());
    }

    #[test]
    fn worth_saving_hash_only_at_or_above_limit_saves() {
        let mut state = fresh_state();
        state.worth_saving_limit = 2;
        state.dirblock_state = MemoryState::InMemoryHashModified;
        state
            .known_hash_changes
            .insert(entry_key(b"", b"a", b"fid-a"));
        state
            .known_hash_changes
            .insert(entry_key(b"", b"b", b"fid-b"));
        assert!(state.worth_saving());
    }

    #[test]
    fn worth_saving_hash_only_with_negative_limit_never_saves() {
        let mut state = fresh_state();
        state.worth_saving_limit = -1;
        state.dirblock_state = MemoryState::InMemoryHashModified;
        for i in 0..10 {
            state
                .known_hash_changes
                .insert(entry_key(b"", &[b'a' + i], b"fid"));
        }
        assert!(!state.worth_saving());
    }

    #[test]
    fn save_to_writes_get_lines_and_marks_unmodified() {
        let mut state = minimal_populated_state();
        state.dirblock_state = MemoryState::InMemoryModified;
        let expected: Vec<u8> = state.get_lines().into_iter().flatten().collect();

        let mut t = MemoryTransport::new();
        t.lock_write().unwrap();
        let wrote = state.save_to(&mut t).expect("save_to");
        assert!(wrote);
        assert_eq!(t.read_all().unwrap(), expected);
        // After a successful save the state flips back to unmodified.
        assert_eq!(state.dirblock_state, MemoryState::InMemoryUnmodified);
        assert_eq!(state.header_state, MemoryState::InMemoryUnmodified);
    }

    #[test]
    fn save_to_honours_changes_aborted() {
        let mut state = minimal_populated_state();
        state.dirblock_state = MemoryState::InMemoryModified;
        state.changes_aborted = true;
        let mut t = MemoryTransport::new();
        t.lock_write().unwrap();
        let wrote = state.save_to(&mut t).expect("save_to");
        assert!(!wrote);
        // Nothing was written.
        assert_eq!(t.read_all().unwrap(), Vec::<u8>::new());
        // State flags are left alone.
        assert_eq!(state.dirblock_state, MemoryState::InMemoryModified);
    }

    #[test]
    fn save_to_skips_when_not_worth_saving() {
        let mut state = minimal_populated_state();
        // Fresh + unmodified → worth_saving is false.
        state.header_state = MemoryState::InMemoryUnmodified;
        state.dirblock_state = MemoryState::InMemoryUnmodified;
        let mut t = MemoryTransport::new();
        t.lock_write().unwrap();
        let wrote = state.save_to(&mut t).expect("save_to");
        assert!(!wrote);
        assert_eq!(t.read_all().unwrap(), Vec::<u8>::new());
    }

    #[test]
    fn set_data_replaces_parents_and_dirblocks_and_marks_modified() {
        let mut state = fresh_state();
        // Start in a clean, unmodified state and make sure set_data
        // flips both dirblock and header to InMemoryModified.
        state.header_state = MemoryState::InMemoryUnmodified;
        state.dirblock_state = MemoryState::InMemoryUnmodified;
        // Pre-populate id_index so we can verify it is invalidated.
        state.id_index = Some(IdIndex::new());

        let new_parents = vec![b"rev-x".to_vec()];
        let new_dirblocks = vec![Dirblock {
            dirname: b"sub".to_vec(),
            entries: Vec::new(),
        }];

        state.set_data(new_parents.clone(), new_dirblocks.clone());

        assert_eq!(state.parents, new_parents);
        assert_eq!(state.dirblocks.len(), 1);
        assert_eq!(state.dirblocks[0].dirname, b"sub".to_vec());
        assert_eq!(state.dirblock_state, MemoryState::InMemoryModified);
        assert_eq!(state.header_state, MemoryState::InMemoryModified);
        assert!(state.id_index.is_none());
    }

    #[test]
    fn wipe_state_resets_all_fields() {
        let mut state = minimal_populated_state();
        state.parents = vec![b"rev-a".to_vec(), b"rev-b".to_vec()];
        state.ghosts = vec![b"rev-b".to_vec()];
        state.header_state = MemoryState::InMemoryModified;
        state.dirblock_state = MemoryState::InMemoryModified;
        state.changes_aborted = true;
        state.end_of_header = Some(42);
        state.cutoff_time = Some(123);
        let _ = state.get_or_build_id_index();
        assert!(state.id_index.is_some());

        state.wipe_state();

        assert_eq!(state.header_state, MemoryState::NotInMemory);
        assert_eq!(state.dirblock_state, MemoryState::NotInMemory);
        assert!(!state.changes_aborted);
        assert!(state.parents.is_empty());
        assert!(state.ghosts.is_empty());
        assert!(state.dirblocks.is_empty());
        assert!(state.id_index.is_none());
        assert!(state.end_of_header.is_none());
        assert!(state.cutoff_time.is_none());
    }

    #[test]
    fn save_to_requires_write_lock() {
        let mut state = minimal_populated_state();
        state.dirblock_state = MemoryState::InMemoryModified;
        // No lock at all.
        let mut t = MemoryTransport::new();
        assert!(matches!(
            state.save_to(&mut t).unwrap_err(),
            TransportError::Other(_)
        ));
        // Read lock is still not enough.
        t.lock_read().unwrap();
        assert!(matches!(
            state.save_to(&mut t).unwrap_err(),
            TransportError::Other(_)
        ));
    }
}
