//! Core dirstate record types: the `TreeData` / `EntryKey` / `Entry`
//! / `Dirblock` quartet plus the tag enums (`YesNo`, `MemoryState`,
//! `LockState`).

use super::Kind;

pub enum YesNo {
    Yes,
    No,
}

/// `_header_state` and `_dirblock_state` represent the current state
/// of the dirstate metadata and the per-row data respectively.
///
/// In future we will add more granularity — for instance
/// `_dirblock_state` will probably support partially-in-memory as a
/// separate variable, allowing for partially-in-memory unmodified
/// and partially-in-memory modified states.
#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub enum MemoryState {
    /// No data is in memory.
    NotInMemory,
    /// What we have in memory is the same as what is on disk.
    InMemoryUnmodified,
    /// We have a modified version of what is on disk.
    InMemoryModified,
    InMemoryHashModified,
}

/// Per-tree record attached to an entry: `(minikind, fingerprint, size, executable, packed_stat)`.
///
/// Mirrors the 5-tuple stored at `entry[1][tree_index]` in the Python
/// `DirState`. `fingerprint` is the sha1 for files, the link target
/// for symlinks, or the parent revision for tree references; `size`
/// is the file size in bytes (0 for non-files); `packed_stat` is the
/// base64 `pack_stat` string, or `DirState.NULLSTAT` when no stat is
/// cached.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TreeData {
    pub minikind: Kind,
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

impl Entry {
    /// Minikind of the slot at `tree_index`, or `None` when the
    /// entry has fewer tree slots than that index.
    #[inline]
    pub fn tree_kind(&self, tree_index: usize) -> Option<Kind> {
        self.trees.get(tree_index).map(|t| t.minikind)
    }

    /// Minikind of the current (tree-0) slot.  Shorthand for
    /// ``entry.tree_kind(0)``.
    #[inline]
    pub fn tree0_kind(&self) -> Option<Kind> {
        self.tree_kind(0)
    }
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
