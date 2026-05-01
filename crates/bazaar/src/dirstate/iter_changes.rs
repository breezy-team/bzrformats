//! State carried by the lazy `iter_changes` iterator and the
//! per-row result type yielded by [`super::DirState::process_entry`].
//!
//! The actual state-machine driver lives on `DirState`
//! (`iter_changes_next`, `iter_changes_step_walk`,
//! `iter_changes_step_parents`, `process_entry`); this module just
//! owns the value types.

use super::{DirEntryInfo, StatInfo, WalkDirsUtf8};

/// Filesystem snapshot for one path, as handed to
/// [`super::DirState::process_entry`].  Mirrors the 5-tuple Python's
/// `ProcessEntryPython` threads around internally:
/// `(top_relpath, basename, kind, stat, abspath)`.
#[derive(Debug, Clone)]
pub struct ProcessPathInfo {
    /// Absolute path of the file on disk (utf8 bytes).
    pub abspath: Vec<u8>,
    /// Filesystem kind, or `None` when the path is missing or is of
    /// a kind dirstate doesn't track (block / char / socket / fifo).
    pub kind: Option<osutils::Kind>,
    /// Stat info for the path.
    pub stat: StatInfo,
}

/// Mutable per-`iter_changes` state shared across
/// [`super::DirState::process_entry`] calls.  Ports the instance
/// fields Python's `ProcessEntryPython` carries: search / searched
/// sets, parent-id caches, dirname-to-file-id maps.
#[derive(Debug, Default)]
pub struct ProcessEntryState {
    /// `source_index` in the tree-data array; `None` means "compare
    /// against a synthetic empty source" (new-tree mode).
    pub source_index: Option<usize>,
    /// `target_index` in the tree-data array; always concrete.
    pub target_index: usize,
    /// Whether unchanged entries should still yield a change tuple.
    pub include_unchanged: bool,
    /// Whether the iter_changes caller wants reports for paths on
    /// disk that aren't in either source or target dirstate trees.
    pub want_unversioned: bool,
    /// Partial iter_changes: true when the caller supplied a
    /// narrower set of paths than `{b""}`.  Used by
    /// `_gather_result_for_consistency` to decide whether to queue
    /// parent-directory bookkeeping.
    pub partial: bool,
    /// Whether the current working-tree format supports tree
    /// references.  When false, `is_tree_reference_dir` is never
    /// called during the walk.
    pub supports_tree_reference: bool,
    /// Absolute path of the working-tree root on disk.  Used to
    /// join `root + relpath` into an absolute path that `Transport`
    /// methods can accept.  Filled at `iter_changes` call time.
    pub root_abspath: Vec<u8>,
    /// Paths whose children have already been walked.
    pub searched_specific_files: std::collections::HashSet<Vec<u8>>,
    /// Paths whose children still need walking (driven by the
    /// outer `iter_changes` loop).
    pub search_specific_files: std::collections::HashSet<Vec<u8>>,
    /// Parent directories we need to re-visit after the main walk
    /// — populated by `_gather_result_for_consistency` when a
    /// partial iter_changes produces a relocated entry.
    pub search_specific_file_parents: std::collections::HashSet<Vec<u8>>,
    /// Paths we've examined via `_iter_specific_file_parents`.
    pub searched_exact_paths: std::collections::HashSet<Vec<u8>>,
    /// File ids we've already yielded during the main walk.
    pub seen_ids: std::collections::HashSet<Vec<u8>>,
    /// Cache: dirname → file_id for the *target* tree.
    pub new_dirname_to_file_id: std::collections::HashMap<Vec<u8>, Vec<u8>>,
    /// Cache: dirname → file_id for the *source* tree.
    pub old_dirname_to_file_id: std::collections::HashMap<Vec<u8>, Vec<u8>>,
    /// One-slot cache: (dirname, parent_file_id) for the source tree.
    pub last_source_parent: Option<(Vec<u8>, Option<Vec<u8>>)>,
    /// One-slot cache: (dirname, parent_file_id) for the target tree.
    pub last_target_parent: Option<(Vec<u8>, Option<Vec<u8>>)>,
}

/// One row returned by [`super::DirState::process_entry`], mirroring
/// Python's `DirstateInventoryChange` minus the utf8-decoding (Rust
/// returns raw bytes; the pyo3 layer decodes with surrogateescape).
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
    pub source_kind: Option<osutils::Kind>,
    pub target_kind: Option<osutils::Kind>,
    pub source_exec: Option<bool>,
    pub target_exec: Option<bool>,
}

/// Error returned by [`super::DirState::process_entry`].
#[derive(Debug)]
pub enum ProcessEntryError {
    DirstateCorrupt(String),
    /// On-disk path exists but isn't a kind dirstate can represent
    /// (FIFO, socket, block / char device, etc.). Carries the path
    /// and the raw st_mode so callers can format
    /// `BadFileKindError` for the user.
    BadFileKind {
        path: Vec<u8>,
        mode: u32,
    },
    Internal(String),
}

impl std::fmt::Display for ProcessEntryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProcessEntryError::DirstateCorrupt(s) => write!(f, "dirstate corrupt: {}", s),
            ProcessEntryError::BadFileKind { path, mode } => write!(
                f,
                "bad file kind for {}: mode {:o}",
                String::from_utf8_lossy(path),
                mode
            ),
            ProcessEntryError::Internal(s) => write!(f, "process_entry: {}", s),
        }
    }
}

impl std::error::Error for ProcessEntryError {}

/// Lazy iterator state for [`super::DirState::iter_changes_next`].
/// Holds enough information to resume the depth-first walk across
/// calls, so callers (pyo3 included) can consume one change at a
/// time without materialising the full change set upfront.
///
/// All fields are owned — no borrow on `DirState` or `Transport` —
/// so the state can live inside a pyclass that re-borrows `DirState`
/// on every `__next__` call.
#[derive(Debug)]
pub struct IterChangesIter {
    pub(super) phase: IterPhase,
    /// When the state machine is walking a specific subtree, the
    /// root currently being processed plus its absolute path on disk.
    pub(super) current_root: Option<(Vec<u8>, Vec<u8>)>,
    /// Have we processed the dirstate entries + want_unversioned
    /// emission for the root itself?
    pub(super) root_processed: bool,
    /// Filesystem walker for the current root's subtree.
    pub(super) walker: Option<WalkDirsUtf8>,
    /// Dirblock cursor under the current root — the block index in
    /// `DirState.dirblocks`.
    pub(super) block_index: usize,
    /// Staged walker yield that hasn't yet been consumed by the
    /// merge loop.  Lazily filled on demand.
    pub(super) staged_walker_block: Option<(Vec<u8>, Vec<u8>, Vec<DirEntryInfo>)>,
    /// Per-block merge cursors.  Reset every time we advance to a
    /// new block/walker pair.
    pub(super) merge_entry_index: usize,
    pub(super) merge_path_index: usize,
    pub(super) merge_path_handled: bool,
    pub(super) merge_advance_entry: bool,
    pub(super) merge_advance_path: bool,
    /// Changes buffered for emission.  One state-machine step can
    /// produce several changes (e.g. a root walk that handles both
    /// existing entries and a want_unversioned record) — we queue
    /// them and drain one per `next_change` call.
    pub(super) pending: std::collections::VecDeque<DirstateChange>,
    /// Set once iter_specific_file_parents drain has begun, so we
    /// don't restart it.
    pub(super) parents_drain_started: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum IterPhase {
    /// Pull the next root from `search_specific_files`.
    PickRoot,
    /// Process the root path itself (entries + want_unversioned).
    ProcessRoot,
    /// Walk the root's subtree, merging walker output against
    /// dirblocks.
    WalkSubtree,
    /// Drain `search_specific_file_parents`.
    DrainParents,
    /// Finished — `next_change` returns `Ok(None)`.
    Done,
}

impl Default for IterChangesIter {
    fn default() -> Self {
        Self::new()
    }
}

impl IterChangesIter {
    pub fn new() -> Self {
        Self {
            phase: IterPhase::PickRoot,
            current_root: None,
            root_processed: false,
            walker: None,
            block_index: 0,
            staged_walker_block: None,
            merge_entry_index: 0,
            merge_path_index: 0,
            merge_path_handled: false,
            merge_advance_entry: true,
            merge_advance_path: true,
            pending: std::collections::VecDeque::new(),
            parents_drain_started: false,
        }
    }
}
