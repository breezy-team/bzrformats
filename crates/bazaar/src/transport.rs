//! Storage transport abstraction.
//!
//! [`Transport`] is the path-keyed byte store that knit (and eventually
//! groupcompress, pack_repo, etc.) reads and writes through. It mirrors
//! the duck-typed Python `bzrformats.transport.Transport` interface but
//! exposes only the methods the format-handling crates actually call —
//! not the dozens of housekeeping operations the full Python interface
//! carries.
//!
//! Pure-Rust callers implement this trait directly (local FS, S3,
//! in-memory test fixtures). The pyo3 layer provides a `PyTransport`
//! adapter that wraps any Python object satisfying the equivalent
//! Python interface, so a `KnitVersionedFiles` instance built on
//! pure-Rust traits can still run on top of the existing Python
//! transport stack.
//!
//! ## Error handling
//!
//! All operations return `Result<_, TransportError>`. The variants are
//! deliberately coarse — most callers either propagate the error or
//! match on `NoSuchFile` for the not-found path. Detailed I/O errors
//! are normalised into `(ErrorKind, String)` so the enum stays
//! `Clone + PartialEq + Eq` and tests can compare error values.

/// Errors returned by [`Transport`] operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportError {
    /// The requested path does not exist.
    NoSuchFile(String),
    /// The transport refused a write because it is read-only.
    ReadOnly(String),
    /// An underlying I/O error. The `(ErrorKind, message)` pair is
    /// preserved so callers can branch on kind without losing the
    /// original diagnostic.
    Io {
        kind: std::io::ErrorKind,
        message: String,
    },
    /// Catch-all for transport-specific failures that don't map to
    /// any of the above (typically wrapped Python exceptions on the
    /// pyo3 adapter side).
    Other(String),
}

impl From<std::io::Error> for TransportError {
    fn from(e: std::io::Error) -> Self {
        if e.kind() == std::io::ErrorKind::NotFound {
            TransportError::NoSuchFile(e.to_string())
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
            TransportError::NoSuchFile(p) => write!(f, "No such file: {}", p),
            TransportError::ReadOnly(p) => write!(f, "Read-only transport: {}", p),
            TransportError::Io { kind, message } => {
                write!(f, "I/O error ({:?}): {}", kind, message)
            }
            TransportError::Other(s) => write!(f, "Transport error: {}", s),
        }
    }
}

impl std::error::Error for TransportError {}

/// One range request handed to [`Transport::readv`]: byte offset plus
/// length to read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadRange {
    pub offset: u64,
    pub length: usize,
}

/// One byte range returned from [`Transport::readv`]. The `offset` /
/// `length` echo the request the bytes correspond to so callers can
/// match each result against its request without tracking order
/// themselves (the implementation is allowed to coalesce adjacent
/// requests and yield the merged bytes in any order).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadResult {
    pub offset: u64,
    pub length: usize,
    pub bytes: Vec<u8>,
}

/// Path-keyed byte store. The minimal method set needed by the knit
/// reader and writer — additional operations can be added as more
/// modules port to this trait.
///
/// `path` is always interpreted as relative to the transport's root.
/// Implementations are responsible for whatever path normalisation
/// their backing store requires.
pub trait Transport {
    /// Read the entire contents of `path`.
    fn get_bytes(&self, path: &str) -> Result<Vec<u8>, TransportError>;

    /// Write `bytes` to `path`, creating parent directories if
    /// `create_parent_dir` is true. Replaces any existing content.
    fn put_file_non_atomic(
        &self,
        path: &str,
        bytes: &[u8],
        create_parent_dir: bool,
    ) -> Result<(), TransportError>;

    /// Atomically write `bytes` to `path`, replacing any existing content.
    /// `mode` is an optional Unix permission bits value for the new file.
    ///
    /// The default implementation defers to [`Transport::put_file_non_atomic`]
    /// (ignoring `mode`); backends with a native atomic put should override it.
    fn put_bytes(&self, path: &str, bytes: &[u8], mode: Option<u32>) -> Result<(), TransportError> {
        let _ = mode;
        self.put_file_non_atomic(path, bytes, false)
    }

    /// Append `bytes` to the end of `path`, creating it if missing.
    /// Returns the byte offset where the appended data starts.
    fn append_bytes(&self, path: &str, bytes: &[u8]) -> Result<u64, TransportError>;

    /// Create directory `path`. It is not an error if the directory already
    /// exists; implementations should silently succeed in that case.
    fn mkdir(&self, path: &str) -> Result<(), TransportError>;

    /// Test whether `path` exists.
    fn has(&self, path: &str) -> Result<bool, TransportError>;

    /// Read multiple byte ranges from `path` in a single call.
    /// Implementations are encouraged (but not required) to coalesce
    /// adjacent ranges and issue a single underlying read; the order
    /// of returned [`ReadResult`]s is not specified, but each result
    /// carries its `offset`/`length` so callers can match it back to
    /// the request.
    ///
    /// The default implementation falls back to a `get_bytes` of the
    /// whole file followed by per-range slicing — efficient enough
    /// for in-memory and small-file backends, but real network
    /// transports should override this with a true range read.
    fn readv(&self, path: &str, ranges: &[ReadRange]) -> Result<Vec<ReadResult>, TransportError> {
        let data = self.get_bytes(path)?;
        let mut out = Vec::with_capacity(ranges.len());
        for r in ranges {
            let start = r.offset as usize;
            let end = start.checked_add(r.length).ok_or_else(|| {
                TransportError::Other(format!(
                    "readv range overflow: offset={} length={}",
                    r.offset, r.length
                ))
            })?;
            if end > data.len() {
                return Err(TransportError::Other(format!(
                    "readv range past end: offset={} length={} data_len={}",
                    r.offset,
                    r.length,
                    data.len()
                )));
            }
            out.push(ReadResult {
                offset: r.offset,
                length: r.length,
                bytes: data[start..end].to_vec(),
            });
        }
        Ok(out)
    }

    /// List all files under the transport root recursively, returning
    /// relative paths. Used by [`crate::knit::KndxIndex::keys`] to
    /// enumerate prefixes when the mapper is not constant.
    fn iter_files_recursive(&self) -> Result<Vec<String>, TransportError>;

    /// Resolve `path` relative to the transport root into an absolute
    /// identifier (typically a filesystem path or URL). Used for error
    /// messages and reload-tracking; implementations are free to
    /// return any stable string.
    fn abspath(&self, path: &str) -> Result<String, TransportError>;

    /// Atomically write `bytes` to `path`, replacing any existing
    /// content. The default delegates to [`Transport::put_file_non_atomic`],
    /// which is *not* atomic; backends that can offer atomicity (e.g. via
    /// write-to-temp-then-rename) should override this.
    fn put_bytes(&self, path: &str, bytes: &[u8]) -> Result<(), TransportError> {
        self.put_file_non_atomic(path, bytes, false)
    }

    /// Rename `from` to `to`. For the lockdir protocol this must fail
    /// (rather than overwrite) when `to` already exists, so that the
    /// atomic "claim the lock by renaming into place" step is reliable.
    ///
    /// The default returns [`TransportError::Other`]; backends that
    /// support renaming must override it.
    fn rename(&self, from: &str, to: &str) -> Result<(), TransportError> {
        let _ = (from, to);
        Err(TransportError::Other(
            "rename not supported by this transport".to_string(),
        ))
    }

    /// Delete the file at `path`.
    fn delete(&self, path: &str) -> Result<(), TransportError> {
        let _ = path;
        Err(TransportError::Other(
            "delete not supported by this transport".to_string(),
        ))
    }

    /// Remove the (empty) directory at `path`.
    fn rmdir(&self, path: &str) -> Result<(), TransportError> {
        let _ = path;
        Err(TransportError::Other(
            "rmdir not supported by this transport".to_string(),
        ))
    }

    /// List the immediate entries of directory `path`, returning their
    /// names (not full paths) in unspecified order.
    fn list_dir(&self, path: &str) -> Result<Vec<String>, TransportError> {
        let _ = path;
        Err(TransportError::Other(
            "list_dir not supported by this transport".to_string(),
        ))
    }

    /// Return metadata about `path`.
    fn stat(&self, path: &str) -> Result<Stat, TransportError> {
        let _ = path;
        Err(TransportError::Other(
            "stat not supported by this transport".to_string(),
        ))
    }

    /// Return a new transport rooted at `path` relative to this one.
    ///
    /// Used to descend from a `.bzr` directory into its `repository`,
    /// `branch` and `checkout` components. The default returns
    /// [`TransportError::Other`]; backends that can be re-rooted (e.g.
    /// [`LocalTransport`]) override it.
    fn subtransport(&self, path: &str) -> Result<SharedTransport, TransportError> {
        let _ = path;
        Err(TransportError::Other(
            "subtransport not supported by this transport".to_string(),
        ))
    }
}

/// A transport shared across the opener objects (`BzrDir`, `Branch`,
/// `Repository`, `WorkingTree`).
///
/// They own their transport via this `Arc` rather than borrowing it, so a
/// `BzrDir` can hand out sub-objects that outlive it, and the 2a
/// repository's CHK store (which needs `Arc<S>` with `S: Send + Sync`) is
/// satisfiable. `Send + Sync` is required because the groupcompress stores
/// implement the `Send + Sync` `VersionedFiles` trait.
pub type SharedTransport = std::sync::Arc<dyn Transport + Send + Sync>;

/// Minimal file metadata returned by [`Transport::stat`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stat {
    /// Size in bytes (0 for directories).
    pub size: u64,
    /// Whether the entry is a directory.
    pub is_dir: bool,
}

/// A [`Transport`] rooted at a local filesystem directory.
///
/// All `path` arguments are interpreted relative to [`root`](LocalTransport::root)
/// and joined onto it; the transport does not guard against `..`
/// escaping the root, matching the trust model of the rest of the
/// format code (callers pass paths they constructed themselves).
pub struct LocalTransport {
    root: std::path::PathBuf,
}

impl LocalTransport {
    /// Create a transport rooted at `root`.
    pub fn new<P: Into<std::path::PathBuf>>(root: P) -> Self {
        LocalTransport { root: root.into() }
    }

    /// The directory this transport is rooted at.
    pub fn root(&self) -> &std::path::Path {
        &self.root
    }

    fn resolve(&self, path: &str) -> std::path::PathBuf {
        self.root.join(path)
    }
}

impl Transport for LocalTransport {
    fn get_bytes(&self, path: &str) -> Result<Vec<u8>, TransportError> {
        Ok(std::fs::read(self.resolve(path))?)
    }

    fn put_file_non_atomic(
        &self,
        path: &str,
        bytes: &[u8],
        create_parent_dir: bool,
    ) -> Result<(), TransportError> {
        let full = self.resolve(path);
        if create_parent_dir {
            if let Some(parent) = full.parent() {
                std::fs::create_dir_all(parent)?;
            }
        }
        std::fs::write(full, bytes)?;
        Ok(())
    }

    fn put_bytes(&self, path: &str, bytes: &[u8]) -> Result<(), TransportError> {
        // Atomic via write-to-temp-then-rename within the same directory.
        let full = self.resolve(path);
        let parent = full.parent().ok_or_else(|| {
            TransportError::Other(format!("path has no parent directory: {path}"))
        })?;
        let tmp = parent.join(format!(".{}.tmp", crate::osutils::rand_chars(16)));
        std::fs::write(&tmp, bytes)?;
        if let Err(e) = std::fs::rename(&tmp, &full) {
            let _ = std::fs::remove_file(&tmp);
            return Err(e.into());
        }
        Ok(())
    }

    fn append_bytes(&self, path: &str, bytes: &[u8]) -> Result<u64, TransportError> {
        use std::io::{Seek, Write};
        let mut f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.resolve(path))?;
        let offset = f.seek(std::io::SeekFrom::End(0))?;
        f.write_all(bytes)?;
        Ok(offset)
    }

    fn mkdir(&self, path: &str) -> Result<(), TransportError> {
        match std::fs::create_dir(self.resolve(path)) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    fn has(&self, path: &str) -> Result<bool, TransportError> {
        Ok(self.resolve(path).exists())
    }

    fn iter_files_recursive(&self) -> Result<Vec<String>, TransportError> {
        let mut out = Vec::new();
        let mut stack = vec![self.root.clone()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir)? {
                let entry = entry?;
                let p = entry.path();
                if p.is_dir() {
                    stack.push(p);
                } else if let Ok(rel) = p.strip_prefix(&self.root) {
                    out.push(rel.to_string_lossy().replace('\\', "/"));
                }
            }
        }
        Ok(out)
    }

    fn abspath(&self, path: &str) -> Result<String, TransportError> {
        Ok(self.resolve(path).to_string_lossy().into_owned())
    }

    fn rename(&self, from: &str, to: &str) -> Result<(), TransportError> {
        let to_path = self.resolve(to);
        // bzr's lockdir relies on rename failing when the target exists
        // (so two contenders can't both "win" the lock). std::fs::rename
        // would silently overwrite an empty target dir on some platforms,
        // so reject an existing target explicitly.
        if to_path.exists() {
            return Err(TransportError::Io {
                kind: std::io::ErrorKind::AlreadyExists,
                message: format!("rename target already exists: {to}"),
            });
        }
        std::fs::rename(self.resolve(from), to_path)?;
        Ok(())
    }

    fn delete(&self, path: &str) -> Result<(), TransportError> {
        std::fs::remove_file(self.resolve(path))?;
        Ok(())
    }

    fn rmdir(&self, path: &str) -> Result<(), TransportError> {
        std::fs::remove_dir(self.resolve(path))?;
        Ok(())
    }

    fn list_dir(&self, path: &str) -> Result<Vec<String>, TransportError> {
        let mut out = Vec::new();
        for entry in std::fs::read_dir(self.resolve(path))? {
            out.push(entry?.file_name().to_string_lossy().into_owned());
        }
        Ok(out)
    }

    fn stat(&self, path: &str) -> Result<Stat, TransportError> {
        let meta = std::fs::metadata(self.resolve(path))?;
        Ok(Stat {
            size: meta.len(),
            is_dir: meta.is_dir(),
        })
    }

    fn subtransport(&self, path: &str) -> Result<SharedTransport, TransportError> {
        Ok(std::sync::Arc::new(LocalTransport::new(self.resolve(path))))
    }
}

#[cfg(test)]
pub(crate) mod testing {
    //! In-memory `Transport` implementation, available to tests in
    //! other modules of this crate.
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    #[derive(Default)]
    pub struct MemoryTransport {
        files: Mutex<HashMap<String, Vec<u8>>>,
        root: String,
    }

    impl MemoryTransport {
        pub fn new() -> Self {
            Self {
                files: Mutex::new(HashMap::new()),
                root: "memory:///".to_string(),
            }
        }
    }

    impl Transport for MemoryTransport {
        fn get_bytes(&self, path: &str) -> Result<Vec<u8>, TransportError> {
            let files = self.files.lock().unwrap();
            files
                .get(path)
                .cloned()
                .ok_or_else(|| TransportError::NoSuchFile(path.to_string()))
        }

        fn put_file_non_atomic(
            &self,
            path: &str,
            bytes: &[u8],
            _create_parent_dir: bool,
        ) -> Result<(), TransportError> {
            let mut files = self.files.lock().unwrap();
            files.insert(path.to_string(), bytes.to_vec());
            Ok(())
        }

        fn mkdir(&self, _path: &str) -> Result<(), TransportError> {
            Ok(())
        }

        fn append_bytes(&self, path: &str, bytes: &[u8]) -> Result<u64, TransportError> {
            let mut files = self.files.lock().unwrap();
            let entry = files.entry(path.to_string()).or_default();
            let offset = entry.len() as u64;
            entry.extend_from_slice(bytes);
            Ok(offset)
        }

        fn has(&self, path: &str) -> Result<bool, TransportError> {
            let files = self.files.lock().unwrap();
            Ok(files.contains_key(path))
        }

        fn iter_files_recursive(&self) -> Result<Vec<String>, TransportError> {
            let files = self.files.lock().unwrap();
            Ok(files.keys().cloned().collect())
        }

        fn abspath(&self, path: &str) -> Result<String, TransportError> {
            Ok(format!("{}{}", self.root, path))
        }
    }

    #[test]
    fn memory_transport_basic_round_trip() {
        let t = MemoryTransport::new();
        assert!(!t.has("foo").unwrap());
        t.put_file_non_atomic("foo", b"hello", false).unwrap();
        assert!(t.has("foo").unwrap());
        assert_eq!(t.get_bytes("foo").unwrap(), b"hello".to_vec());
    }

    #[test]
    fn memory_transport_append_returns_offset() {
        let t = MemoryTransport::new();
        assert_eq!(t.append_bytes("log", b"first ").unwrap(), 0);
        assert_eq!(t.append_bytes("log", b"second").unwrap(), 6);
        assert_eq!(t.get_bytes("log").unwrap(), b"first second".to_vec());
    }

    #[test]
    fn memory_transport_get_bytes_missing_is_error() {
        let t = MemoryTransport::new();
        assert_eq!(
            t.get_bytes("nope").unwrap_err(),
            TransportError::NoSuchFile("nope".to_string())
        );
    }

    #[test]
    fn default_readv_slices_via_get_bytes() {
        let t = MemoryTransport::new();
        t.put_file_non_atomic("data", b"0123456789", false).unwrap();
        let ranges = vec![
            ReadRange {
                offset: 0,
                length: 3,
            },
            ReadRange {
                offset: 5,
                length: 2,
            },
        ];
        let results = t.readv("data", &ranges).unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].bytes, b"012".to_vec());
        assert_eq!(results[1].bytes, b"56".to_vec());
    }

    #[test]
    fn default_readv_rejects_past_end() {
        let t = MemoryTransport::new();
        t.put_file_non_atomic("data", b"hi", false).unwrap();
        let err = t
            .readv(
                "data",
                &[ReadRange {
                    offset: 0,
                    length: 100,
                }],
            )
            .unwrap_err();
        assert!(matches!(err, TransportError::Other(_)));
    }
}
