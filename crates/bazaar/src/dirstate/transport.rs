//! The `Transport` trait and its companion types.
//!
//! `DirState` does all of its filesystem I/O through a
//! [`Transport`] implementation: read the dirstate file, write it
//! back, acquire a lock, stat a tracked file, read a symlink, list
//! a directory.  The pure crate never touches `std::fs` directly
//! (CLAUDE.md "Filesystem goes through Transport, not std::fs") so
//! the pyo3 adapter can supply a Python-file-backed transport, and
//! tests can use a `MemoryTransport` that models the filesystem in
//! a `HashMap`.

use super::LockState;

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

/// One entry yielded by [`Transport::list_dir`] — mirrors the
/// per-child tuple Python's `DirReader.read_dir` returns.
#[derive(Debug, Clone)]
pub struct DirEntryInfo {
    /// The child's utf8 basename (no trailing slash).
    pub basename: Vec<u8>,
    /// Filesystem kind, or `None` for kinds dirstate doesn't track
    /// (block / char / socket / fifo).
    pub kind: Option<osutils::Kind>,
    /// Stat info from `lstat` on the child.
    pub stat: StatInfo,
    /// Absolute path of the child on disk (utf8 bytes).
    pub abspath: Vec<u8>,
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
/// The dirstate reads and writes one on-disk file; callers hand in a
/// `Transport` that knows where that file lives and how to lock it.
/// The real-filesystem backend is a thin wrapper over `std::fs`;
/// tests use a `MemoryTransport`, and the pyo3 layer uses a
/// `PyFileTransport` that delegates to a Python file-like object.
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

    /// Whether the directory at `abspath` is a nested tree reference
    /// (i.e. contains a `.bzr/` control directory).  Mirrors the
    /// per-format `_directory_is_tree_reference` hook on breezy's
    /// `WorkingTree`: the file format decides whether tree references
    /// can exist at all, and a concrete directory qualifies iff it
    /// carries its own `.bzr/`.  Consumers use this during
    /// `iter_changes` to flip the on-disk `directory` kind to
    /// `tree-reference` before handing the entry to
    /// [`DirState::process_entry`].
    ///
    /// Formats that don't support tree references should implement
    /// this as an unconditional `Ok(false)`.
    fn is_tree_reference_dir(&self, abspath: &[u8]) -> Result<bool, TransportError>;

    /// List the immediate children of directory `abspath`.  Used by
    /// the pure-crate `iter_changes` walker.  Returns a vector of
    /// per-child entries; the implementation does not guarantee any
    /// particular order — the walker sorts.
    ///
    /// Each entry carries the child's utf8 basename, its kind
    /// (`"file"`, `"directory"`, `"symlink"`, or `"tree-reference"`),
    /// its [`StatInfo`] (from an `lstat`), and the absolute path of
    /// the child on disk.  The walker re-uses the stat to avoid a
    /// second syscall inside `process_entry`.
    ///
    /// `NoSuchFile` when `abspath` does not exist or is not a
    /// directory.
    fn list_dir(&self, abspath: &[u8]) -> Result<Vec<DirEntryInfo>, TransportError>;
}
