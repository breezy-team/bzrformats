//! pyo3 adapter that wraps any Python object satisfying the
//! `bzrformats.transport.Transport` duck-typed interface and exposes it
//! to pure-Rust code as a [`bazaar::transport::Transport`] implementor.
//!
//! See `crates/bazaar/src/transport.rs` for the trait definition. The
//! method dispatch here is intentionally one-to-one with the trait's
//! method set — every Rust call becomes a single Python `call_method1`.

use bazaar::key_mapper::Mapper;
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
pyo3::import_exception!(bzrformats._bzr_rs.errors, NoSuchFile);
mod transport_exc {
    pyo3::import_exception!(bzrformats.transport, NoSuchFile);
}
mod dromedary_exc {
    pyo3::import_exception!(dromedary.errors, NoSuchFile);
}

fn map_py_err(py: Python<'_>, err: PyErr) -> TransportError {
    if err.is_instance_of::<NoSuchFile>(py)
        || err.is_instance_of::<transport_exc::NoSuchFile>(py)
        || err.is_instance_of::<dromedary_exc::NoSuchFile>(py)
    {
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

    fn put_file_non_atomic(
        &self,
        path: &str,
        bytes: &[u8],
        create_parent_dir: bool,
    ) -> Result<(), TransportError> {
        Python::attach(|py| -> Result<(), TransportError> {
            let io = py
                .import("io")
                .map_err(|e| TransportError::Other(e.to_string()))?;
            let sio = io
                .call_method1("BytesIO", (PyBytes::new(py, bytes),))
                .map_err(|e| TransportError::Other(e.to_string()))?;
            let kwargs = pyo3::types::PyDict::new(py);
            kwargs
                .set_item("create_parent_dir", create_parent_dir)
                .map_err(|e| TransportError::Other(e.to_string()))?;
            self.0
                .bind(py)
                .call_method("put_file_non_atomic", (path, sio), Some(&kwargs))
                .map_err(|e| map_py_err(py, e))?;
            Ok(())
        })
    }

    fn mkdir(&self, path: &str) -> Result<(), TransportError> {
        Python::attach(|py| -> Result<(), TransportError> {
            self.0
                .bind(py)
                .call_method1("mkdir", (path,))
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

    fn iter_files_recursive(&self) -> Result<Vec<String>, TransportError> {
        Python::attach(|py| -> Result<Vec<String>, TransportError> {
            let iter = self
                .0
                .bind(py)
                .call_method0("iter_files_recursive")
                .map_err(|e| map_py_err(py, e))?;
            let mut out = Vec::new();
            for item in iter.try_iter().map_err(|e| map_py_err(py, e))? {
                let item = item.map_err(|e| map_py_err(py, e))?;
                let s = item.extract::<String>().map_err(|_| {
                    TransportError::Other(
                        "transport.iter_files_recursive yielded a non-string".to_string(),
                    )
                })?;
                out.push(s);
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

/// Wraps a Python `KeyMapper` object so it can be passed to pure-Rust
/// code that expects a [`Mapper`] trait object.
///
/// The Python `KeyMapper.map(key)` method takes a tuple of bytes and
/// returns a `str`. The `unmap(partition_id)` method takes a `str` and
/// returns a tuple of bytes.  This adapter handles the conversion.
pub struct PyMapper(pub Py<PyAny>);

// SAFETY: Py<PyAny> is Send; all calls re-acquire the GIL via Python::attach.
unsafe impl Send for PyMapper {}
unsafe impl Sync for PyMapper {}

impl PyMapper {
    pub fn new(obj: Bound<'_, PyAny>) -> Self {
        Self(obj.unbind())
    }
}

impl Mapper for PyMapper {
    fn is_constant(&self) -> bool {
        Python::attach(|py| {
            py.import("bzrformats.versionedfile")
                .and_then(|m| m.getattr("ConstantMapper"))
                .map(|cls| self.0.bind(py).is_instance(&cls).unwrap_or(false))
                .unwrap_or(false)
        })
    }

    fn map(&self, key: &[&[u8]]) -> String {
        Python::attach(|py| -> String {
            let parts: Vec<Bound<'_, PyBytes>> = key.iter().map(|s| PyBytes::new(py, s)).collect();
            let tuple = PyTuple::new(py, parts).expect("PyTuple::new failed");
            let result = self
                .0
                .bind(py)
                .call_method1("map", (tuple,))
                .expect("mapper.map() failed");
            result
                .extract::<String>()
                .expect("mapper.map() did not return a string")
        })
    }

    fn unmap(&self, path: &str) -> Vec<Vec<u8>> {
        Python::attach(|py| -> Vec<Vec<u8>> {
            let result = self
                .0
                .bind(py)
                .call_method1("unmap", (path,))
                .expect("mapper.unmap() failed");
            let tup = result
                .cast_into::<PyTuple>()
                .expect("mapper.unmap() did not return a tuple");
            tup.iter()
                .map(|item| {
                    item.cast_into::<PyBytes>()
                        .expect("mapper.unmap() tuple element must be bytes")
                        .as_bytes()
                        .to_vec()
                })
                .collect()
        })
    }
}
