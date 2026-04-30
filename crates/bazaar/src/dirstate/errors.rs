//! Error types raised by [`super::DirState`] operations.
//!
//! Each variant maps onto a specific Python exception that the pyo3
//! adapter re-raises (`DuplicateFileId`, `NotVersionedError`,
//! `InconsistentDelta`, etc.) — the structured form makes the
//! translation mechanical and keeps the pure crate panic-free.

use super::{EntryKey, Kind, TreeData};

/// Error returned by [`super::DirState::ensure_block`] when the
/// requested dirname does not end with the parent entry's basename.
/// Mirrors the `AssertionError("bad dirname ...")` Python raises.
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

/// Error returned by [`super::DirState::entries_to_current_state`] when
/// the input entry list violates the layout invariants Python asserts
/// in `_entries_to_current_state`.
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
/// [`super::DirState::update_basis_apply_adds`]. Mirrors the per-entry
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

/// Error returned by [`super::DirState::update_basis_apply_adds`] and
/// the sibling apply-changes / apply-deletes methods. Mirrors
/// Python's `_raise_invalid` and `AssertionError` /
/// `NotImplementedError` paths.
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
    /// An `InventoryDeltaEntry` supplied a `new_entry` whose
    /// `file_id` disagrees with the delta row's own `file_id`.
    /// Python raises this as `InconsistentDelta(new_path, file_id,
    /// "mismatched entry file_id …")`.
    MismatchedEntryFileId {
        new_path: Vec<u8>,
        file_id: Vec<u8>,
        entry_debug: String,
    },
    /// The delta row has `new_path` but no accompanying `new_entry`.
    /// Python raises this as `InconsistentDelta(new_path, file_id,
    /// "new_path with no entry")`.
    NewPathWithoutEntry { new_path: Vec<u8>, file_id: Vec<u8> },
}

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
            BasisApplyError::MismatchedEntryFileId {
                new_path,
                file_id,
                entry_debug,
            } => write!(
                f,
                "mismatched entry file_id at {:?} ({:?}): {}",
                new_path, file_id, entry_debug
            ),
            BasisApplyError::NewPathWithoutEntry { new_path, file_id } => {
                write!(
                    f,
                    "new_path with no entry at {:?} ({:?})",
                    new_path, file_id
                )
            }
        }
    }
}

impl std::error::Error for BasisApplyError {}

/// A pre-flattened inventory-delta row passed to
/// [`super::DirState::update_by_delta`]. Mirrors the Python-side
/// tuple the caller builds by unpacking a delta entry and its
/// `InventoryEntry`. `minikind` is the single-byte code from
/// `DirState._kind_to_minikind`; `fingerprint` is empty for
/// non-tree-reference entries.
#[derive(Debug, Clone)]
pub struct FlatDeltaEntry {
    pub old_path: Option<Vec<u8>>,
    pub new_path: Option<Vec<u8>>,
    pub file_id: Vec<u8>,
    pub parent_id: Option<Vec<u8>>,
    pub minikind: Kind,
    pub executable: bool,
    pub fingerprint: Vec<u8>,
}

/// A pre-flattened row passed to [`super::DirState::update_basis_by_delta`].
/// `details` is the 5-tuple returned by
/// [`super::inv_entry_to_details`]: `(minikind, fingerprint, size,
/// executable, tree_data)` — Python runs `inv_entry_to_details` per
/// row before dispatching. `details` may be `None` for deletions.
#[derive(Debug, Clone)]
pub struct FlatBasisDeltaEntry {
    pub old_path: Option<Vec<u8>>,
    pub new_path: Option<Vec<u8>>,
    pub file_id: Vec<u8>,
    pub parent_id: Option<Vec<u8>>,
    pub details: Option<(Kind, Vec<u8>, u64, bool, Vec<u8>)>,
}

/// Error returned by [`super::DirState::validate`]. A single
/// descriptive string is enough — the pyo3 layer wraps it in
/// `AssertionError` exactly like Python's `_validate` raises.
#[derive(Debug, Clone)]
pub struct ValidateError(pub String);

impl std::fmt::Display for ValidateError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ValidateError {}

/// Error returned by [`super::DirState::make_absent`] when the
/// dirstate is not in the shape Python's `_make_absent` expects.
/// Each variant mirrors one of Python's `AssertionError`s, carrying
/// the offending key for diagnostic messages.
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

/// Error returned by
/// [`super::split_root_dirblock_into_contents`] when the pre-split
/// dirblock layout is malformed.
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

/// Error returned by [`super::DirState::update_entry`].
#[derive(Debug)]
pub enum UpdateEntryError {
    /// No dirstate entry matches the given key.
    EntryNotFound,
    /// The key's entry has a minikind we do not know how to refresh.
    UnexpectedKind(Kind),
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

/// Error returned by [`super::DirState::set_path_id`]. Mirrors the
/// exceptions Python's `DirState.set_path_id` raises.
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

/// Error returned by [`super::DirState::add`] when the requested add
/// cannot be performed. Each variant mirrors one of the exceptions
/// Python's `DirState.add` raises: the pyo3 layer translates them
/// back.
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
    /// The rename-from branch tried to re-add a file_id that was
    /// previously 'a' but the in-place insertion found an existing row
    /// with a non-absent tree-0 (should be unreachable post-normalisation).
    AlreadyAddedAssertion { basename: Vec<u8>, file_id: Vec<u8> },
    /// An internal invariant violation surfaced from a helper call such
    /// as [`super::DirState::update_minimal`] during the rename-from step.
    Internal { reason: String },
    /// The basename is not unicode-normalized and the normalized form
    /// would point at an inaccessible path.  Mirrors Python's
    /// `InvalidNormalization(path)`.
    InvalidNormalization { path: String },
    /// The basename is `.` or `..`.  Mirrors Python's
    /// `inventory.InvalidEntryName(path)`.
    InvalidEntryName { name: String },
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
            AddError::AlreadyAddedAssertion { basename, file_id } => {
                write!(f, "{:?}({:?}) already added", basename, file_id)
            }
            AddError::Internal { reason } => write!(f, "internal error: {}", reason),
            AddError::InvalidNormalization { path } => {
                write!(f, "path not unicode-normalized: {:?}", path)
            }
            AddError::InvalidEntryName { name } => write!(f, "invalid entry name: {:?}", name),
        }
    }
}

impl std::error::Error for AddError {}
