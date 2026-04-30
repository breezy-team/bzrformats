//! Depth-first directory walker modelled on Python's
//! `_walkdirs_utf8`.  Yields one directory at a time; the caller
//! may mutate the yielded entries list to prune subdirectories
//! from the descent before the next `next_dir` call.

use super::{DirEntryInfo, Transport, TransportError};

/// One directory block yielded by the walker.  Mirrors the shape
/// Python's `_walkdirs_utf8` yields: `((relroot, abspath),
/// [DirEntryInfo, ...])`.  The entries are sorted by basename; the
/// caller may mutate the list (remove entries to skip recursion)
/// before the walker proceeds.
#[derive(Debug, Clone)]
pub struct WalkedDir {
    /// Utf8 relative path of this directory, relative to the walk's
    /// `prefix`.  Empty for the top of the walk.
    pub relpath: Vec<u8>,
    /// Absolute path of this directory on disk.
    pub abspath: Vec<u8>,
    /// Per-child entries (sorted by basename).  Each entry's
    /// `basename` field is the child's basename (not its relpath
    /// relative to `walk.prefix`).  The walker reads each child's
    /// full relpath as `relpath + '/' + basename` when it recurses.
    pub entries: Vec<DirEntryInfo>,
}

/// Iterator-like helper for depth-first directory walks modeled on
/// Python's `_walkdirs_utf8`.  Call [`WalkDirsUtf8::next_dir`]
/// repeatedly; it yields `Some(WalkedDir)` per directory in
/// depth-first order and `None` when the walk completes.  The caller
/// mutates the returned `entries` list before the next call to
/// prune directories from the descent (matching how the Python
/// walker mutates the yielded dirblock list in place).
#[derive(Debug)]
pub struct WalkDirsUtf8 {
    /// Stack of (relpath, abspath) pairs still to visit.  Most
    /// recently discovered directories are on top so the walk is
    /// depth-first matching Python's behaviour.
    pub pending: Vec<(Vec<u8>, Vec<u8>)>,
    /// Last-yielded directory's children, filtered down to surviving
    /// subdirectories that still need recursion.  Reset on every
    /// call to `next_dir`.
    pub pending_subdirs: Vec<(Vec<u8>, Vec<u8>)>,
}

impl WalkDirsUtf8 {
    /// Start a walk rooted at `root_abspath`.  `prefix` is the utf8
    /// relpath that should precede every yielded child's relpath
    /// (matching Python's `prefix` argument to `_walkdirs_utf8`).
    pub fn new(root_abspath: &[u8], prefix: &[u8]) -> Self {
        Self {
            pending: vec![(prefix.to_vec(), root_abspath.to_vec())],
            pending_subdirs: Vec::new(),
        }
    }

    /// Yield the next directory block.  Returns `Ok(false)` when the
    /// walk is done.  `callback` is invoked with the yielded block
    /// and a mutable slice of its entries so the caller can prune
    /// subdirectories before recursion.  The caller's mutation
    /// semantics mirror the Python walker: an entry removed from the
    /// slice will not be recursed into.
    ///
    /// Takes the [`Transport`] per call rather than storing a
    /// borrow so callers can embed the walker in longer-lived
    /// iterator state.
    pub fn next_dir<F>(
        &mut self,
        transport: &dyn Transport,
        mut callback: F,
    ) -> Result<bool, TransportError>
    where
        F: FnMut(&[u8], &[u8], &mut Vec<DirEntryInfo>),
    {
        // Promote subdirectories discovered on the previous iteration
        // into the pending stack.  `pending_subdirs` is in forward
        // (byte-sorted) order; drain it in reverse so the
        // smallest-named child lands on top of the stack and
        // `pop()` below yields it first — depth-first, alphabetical.
        for entry in self.pending_subdirs.drain(..).rev() {
            self.pending.push(entry);
        }
        let (relpath, abspath) = match self.pending.pop() {
            Some(v) => v,
            None => return Ok(false),
        };

        let mut entries = transport.list_dir(&abspath)?;
        entries.sort_by(|a, b| a.basename.cmp(&b.basename));
        callback(&relpath, &abspath, &mut entries);

        // Collect surviving directory entries into `pending_subdirs`
        // in forward (byte-sorted) order.  The promotion loop above
        // runs in that same order on the next call, which puts the
        // last-named child on top of the stack — so `pop` yields
        // the first-named child first.
        self.pending_subdirs = entries
            .iter()
            .filter(|e| e.kind == Some(osutils::Kind::Directory))
            .map(|e| {
                let mut child_relpath = relpath.clone();
                if !child_relpath.is_empty() {
                    child_relpath.push(b'/');
                }
                child_relpath.extend_from_slice(&e.basename);
                (child_relpath, e.abspath.clone())
            })
            .collect();
        Ok(true)
    }
}
