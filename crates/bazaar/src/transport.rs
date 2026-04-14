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

    /// Atomically replace the contents of `path` with `bytes`. If the
    /// transport is read-only, returns `TransportError::ReadOnly`.
    fn put_bytes(&self, path: &str, bytes: &[u8]) -> Result<(), TransportError>;

    /// Append `bytes` to the end of `path`, creating it if missing.
    /// Returns the byte offset where the appended data starts.
    fn append_bytes(&self, path: &str, bytes: &[u8]) -> Result<u64, TransportError>;

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

    /// Resolve `path` relative to the transport root into an absolute
    /// identifier (typically a filesystem path or URL). Used for error
    /// messages and reload-tracking; implementations are free to
    /// return any stable string.
    fn abspath(&self, path: &str) -> Result<String, TransportError>;
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

        fn put_bytes(&self, path: &str, bytes: &[u8]) -> Result<(), TransportError> {
            let mut files = self.files.lock().unwrap();
            files.insert(path.to_string(), bytes.to_vec());
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

        fn abspath(&self, path: &str) -> Result<String, TransportError> {
            Ok(format!("{}{}", self.root, path))
        }
    }

    #[test]
    fn memory_transport_basic_round_trip() {
        let t = MemoryTransport::new();
        assert!(!t.has("foo").unwrap());
        t.put_bytes("foo", b"hello").unwrap();
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
        t.put_bytes("data", b"0123456789").unwrap();
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
        t.put_bytes("data", b"hi").unwrap();
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
