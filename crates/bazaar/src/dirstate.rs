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
#[derive(PartialEq, Eq, Debug)]
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
    pub sha1_provider: Box<dyn SHA1Provider>,
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
        sha1_provider: Box<dyn SHA1Provider>,
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
        }
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
}
