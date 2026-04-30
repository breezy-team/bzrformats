//! Binary serde for the dirstate file body — the per-entry portion
//! that follows the header.
//!
//! The format is a long sequence of NUL-delimited fields grouped
//! into fixed-shape rows.  [`parse_dirblocks`] is the inverse of
//! [`entry_to_line`]/[`dirblocks_to_entry_lines`].

use super::{Dirblock, Entry, EntryKey, Kind, TreeData};

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
    /// The minikind byte wasn't one of the six valid codes.
    InvalidMinikind(u8),
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
            DirblocksError::InvalidMinikind(byte) => {
                write!(f, "invalid minikind byte {:?}", byte)
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

            let minikind_byte = minikind_bytes.first().copied().unwrap_or(0);
            let minikind =
                Kind::from_minikind(minikind_byte).map_err(DirblocksError::InvalidMinikind)?;
            trees.push(TreeData {
                minikind,
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
        out.push(tree.minikind.to_minikind());
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
/// [`super::get_output_lines`].
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
