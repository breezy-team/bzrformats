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
    /// Lazily-populated index of `file_id → [(dirname, basename, file_id)]`.
    /// `None` until [`DirState::get_or_build_id_index`] is called, at
    /// which point it is rebuilt from the current `dirblocks`.
    /// Invalidate by setting to `None` whenever dirblocks change.
    pub id_index: Option<IdIndex>,
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
            id_index: None,
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
    }

    /// Forget all in-memory state, returning the object to the same
    /// shape a freshly constructed [`DirState`] has before any load.
    /// Mirrors Python's `DirState._wipe_state`.
    ///
    /// Python additionally clears `_packed_stat_index` and
    /// `_split_path_cache`; neither field exists on the Rust struct
    /// yet (the equivalents are the `id_index` cache plus the still
    /// un-ported memoisation layers on `_find_block_index_from_key`),
    /// so this function resets what it can and leaves a note for the
    /// future port to extend.
    pub fn wipe_state(&mut self) {
        self.header_state = MemoryState::NotInMemory;
        self.dirblock_state = MemoryState::NotInMemory;
        self.changes_aborted = false;
        self.parents.clear();
        self.ghosts.clear();
        self.dirblocks.clear();
        self.id_index = None;
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
        self.mark_modified(&[], true);
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
    struct MemoryTransport {
        contents: Option<Vec<u8>>,
        lock: Option<LockState>,
    }

    impl MemoryTransport {
        fn new() -> Self {
            Self {
                contents: None,
                lock: None,
            }
        }

        fn with_contents(bytes: &[u8]) -> Self {
            Self {
                contents: Some(bytes.to_vec()),
                lock: None,
            }
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
