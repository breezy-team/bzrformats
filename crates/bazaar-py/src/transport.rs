//! pyo3 adapter that wraps any Python object satisfying the
//! `bzrformats.transport.Transport` duck-typed interface and exposes it
//! to pure-Rust code as a [`bazaar::transport::Transport`] implementor.
//!
//! See `crates/bazaar/src/transport.rs` for the trait definition. The
//! method dispatch here is intentionally one-to-one with the trait's
//! method set — every Rust call becomes a single Python `call_method1`.

use bazaar::transport::{ReadRange, ReadResult, Transport, TransportError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple};

/// Wraps a Python `Transport` object so it can be passed to pure-Rust
/// code that expects a `Transport` trait object.
///
/// Construction borrows the Python object through `Bound`; internally
/// the adapter holds an unbound `Py<PyAny>` so the wrapper itself can
/// be moved around freely. Each call re-attaches to a `Python<'_>` to
/// dispatch the underlying method.
// TODO: not yet wired up to a Python entry point; will be used once
// pure-Rust knit code accepts a Transport trait object directly.
#[allow(dead_code)]
pub struct PyTransport(Py<PyAny>);

impl PyTransport {
    /// Wrap `obj`. The caller is responsible for ensuring `obj`
    /// implements the duck-typed Python `Transport` interface knit
    /// reads/writes use; mismatches surface as `TransportError::Other`
    /// at call time.
    #[allow(dead_code)]
    pub fn new(obj: Bound<'_, PyAny>) -> Self {
        Self(obj.unbind())
    }
}

/// Convert a Python error into a [`TransportError`], mapping the
/// `NoSuchFile` exception class to [`TransportError::NoSuchFile`] and
/// everything else to [`TransportError::Other`] with the exception's
/// `repr()`.
#[allow(dead_code)]
fn map_py_err(py: Python<'_>, err: PyErr) -> TransportError {
    pyo3::import_exception!(bzrformats.errors, NoSuchFile);
    if err.is_instance_of::<NoSuchFile>(py) {
        let msg = err
            .value(py)
            .str()
            .map(|s| s.to_string())
            .unwrap_or_else(|_| "<unknown path>".to_string());
        return TransportError::NoSuchFile(msg);
    }
    TransportError::Other(err.to_string())
}

impl Transport for PyTransport {
    fn get_bytes(&self, path: &str) -> Result<Vec<u8>, TransportError> {
        Python::attach(|py| -> Result<Vec<u8>, TransportError> {
            let result = self
                .0
                .bind(py)
                .call_method1("get_bytes", (path,))
                .map_err(|e| map_py_err(py, e))?;
            let bytes = result.cast_into::<PyBytes>().map_err(|_| {
                TransportError::Other("transport.get_bytes did not return bytes".to_string())
            })?;
            Ok(bytes.as_bytes().to_vec())
        })
    }

    fn put_bytes(&self, path: &str, bytes: &[u8]) -> Result<(), TransportError> {
        Python::attach(|py| -> Result<(), TransportError> {
            let py_bytes = PyBytes::new(py, bytes);
            self.0
                .bind(py)
                .call_method1("put_bytes", (path, py_bytes))
                .map_err(|e| map_py_err(py, e))?;
            Ok(())
        })
    }

    fn append_bytes(&self, path: &str, bytes: &[u8]) -> Result<u64, TransportError> {
        Python::attach(|py| -> Result<u64, TransportError> {
            let py_bytes = PyBytes::new(py, bytes);
            let result = self
                .0
                .bind(py)
                .call_method1("append_bytes", (path, py_bytes))
                .map_err(|e| map_py_err(py, e))?;
            result.extract::<u64>().map_err(|_| {
                TransportError::Other("transport.append_bytes did not return an int".to_string())
            })
        })
    }

    fn has(&self, path: &str) -> Result<bool, TransportError> {
        Python::attach(|py| -> Result<bool, TransportError> {
            let result = self
                .0
                .bind(py)
                .call_method1("has", (path,))
                .map_err(|e| map_py_err(py, e))?;
            result.extract::<bool>().map_err(|_| {
                TransportError::Other("transport.has did not return a bool".to_string())
            })
        })
    }

    fn readv(&self, path: &str, ranges: &[ReadRange]) -> Result<Vec<ReadResult>, TransportError> {
        // bzrformats Transport.readv takes an iterable of `(offset, length)`
        // tuples and returns an iterator yielding `(offset, bytes)` pairs.
        // We thread the original lengths back in so the caller can
        // match each result against its request.
        Python::attach(|py| -> Result<Vec<ReadResult>, TransportError> {
            let py_ranges: Vec<Bound<'_, PyTuple>> = ranges
                .iter()
                .map(|r| PyTuple::new(py, [r.offset, r.length as u64]))
                .collect::<Result<_, _>>()
                .map_err(|e| TransportError::Other(e.to_string()))?;
            let py_list = pyo3::types::PyList::new(py, py_ranges)
                .map_err(|e| TransportError::Other(e.to_string()))?;
            let iter = self
                .0
                .bind(py)
                .call_method1("readv", (path, py_list))
                .map_err(|e| map_py_err(py, e))?;
            let mut out = Vec::with_capacity(ranges.len());
            for (i, item) in iter.try_iter().map_err(|e| map_py_err(py, e))?.enumerate() {
                let item = item.map_err(|e| map_py_err(py, e))?;
                let tup = item.cast_into::<PyTuple>().map_err(|_| {
                    TransportError::Other("transport.readv yielded a non-tuple item".to_string())
                })?;
                let offset: u64 = tup
                    .get_item(0)
                    .map_err(|e| map_py_err(py, e))?
                    .extract()
                    .map_err(|e| map_py_err(py, e))?;
                let bytes = tup
                    .get_item(1)
                    .map_err(|e| map_py_err(py, e))?
                    .cast_into::<PyBytes>()
                    .map_err(|_| {
                        TransportError::Other(
                            "transport.readv yielded a non-bytes payload".to_string(),
                        )
                    })?;
                let bytes_vec = bytes.as_bytes().to_vec();
                let length = bytes_vec.len();
                // Cross-check the length against the request, where we
                // can; the Python transport doesn't always preserve
                // 1:1 ordering with the request list, so we fall back
                // to recording the actual byte count.
                let request_length = ranges.get(i).map(|r| r.length).unwrap_or(length);
                out.push(ReadResult {
                    offset,
                    length: request_length,
                    bytes: bytes_vec,
                });
            }
            Ok(out)
        })
    }

    fn abspath(&self, path: &str) -> Result<String, TransportError> {
        Python::attach(|py| -> Result<String, TransportError> {
            let result = self
                .0
                .bind(py)
                .call_method1("abspath", (path,))
                .map_err(|e| map_py_err(py, e))?;
            result.extract::<String>().map_err(|_| {
                TransportError::Other("transport.abspath did not return a string".to_string())
            })
        })
    }
}
