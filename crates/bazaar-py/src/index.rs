use bazaar::index::{
    parse_full, parse_header, parse_lines, serialize_graph_index, GraphIndex as RsGraphIndex,
    IndexEntry, IndexError, IndexHeader, IndexKey, IndexNode, IndexTransport, KeyPrefix,
    ParsedLines, RawNode,
};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::import_exception;
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyBytes, PyDict, PyList, PyTuple};

import_exception!(bzrformats.index, BadIndexFormatSignature);
import_exception!(bzrformats.index, BadIndexOptions);
import_exception!(bzrformats.index, BadIndexData);
import_exception!(bzrformats.errors, BzrFormatsError);

fn index_err_to_py(err: IndexError) -> PyErr {
    match err {
        IndexError::BadSignature => BadIndexFormatSignature::new_err(("", "GraphIndex")),
        IndexError::BadOptions => BadIndexOptions::new_err(("",)),
        IndexError::BadLineData => BadIndexData::new_err(("",)),
        IndexError::Other(msg) if msg.starts_with("BadIndexData") => BadIndexData::new_err((msg,)),
        other => BzrFormatsError::new_err(other.to_string()),
    }
}

/// Extract a tuple key (`IndexKey`) from a Python `tuple` of `bytes`.
fn extract_key(obj: &Bound<PyAny>) -> PyResult<IndexKey> {
    let mut parts = Vec::new();
    for item in obj.try_iter()? {
        let b = item?
            .cast_into::<PyBytes>()
            .map_err(|_| PyTypeError::new_err("key element must be bytes"))?;
        parts.push(b.as_bytes().to_vec());
    }
    Ok(parts)
}

/// Convert a Rust `IndexKey` back into a Python tuple of bytes.
fn key_to_py<'py>(py: Python<'py>, key: &IndexKey) -> PyResult<Bound<'py, PyTuple>> {
    let parts: Vec<Bound<PyBytes>> = key.iter().map(|e| PyBytes::new(py, e)).collect();
    PyTuple::new(py, parts)
}

/// Serialize a Python `GraphIndexBuilder._nodes` dict into format-1 bytes.
///
/// `nodes_dict` has the shape `{key_tuple: (absent_marker_bytes,
/// reference_lists_tuple, value_bytes)}` where `absent_marker_bytes` is
/// either `b""` (present) or `b"a"` (absent).
#[pyfunction]
#[pyo3(name = "serialize_graph_index")]
fn py_serialize_graph_index<'py>(
    py: Python<'py>,
    nodes_dict: Bound<'py, PyDict>,
    reference_lists: usize,
    key_elements: usize,
) -> PyResult<Bound<'py, PyBytes>> {
    let mut nodes: Vec<IndexNode> = Vec::with_capacity(nodes_dict.len());
    for (key_obj, value_obj) in nodes_dict.iter() {
        let key = extract_key(&key_obj)?;
        let tuple = value_obj
            .cast::<PyTuple>()
            .map_err(|_| PyTypeError::new_err("node value must be a 3-tuple"))?;
        if tuple.len() != 3 {
            return Err(PyTypeError::new_err("node value must be a 3-tuple"));
        }
        let absent_marker = tuple
            .get_item(0)?
            .cast_into::<PyBytes>()
            .map_err(|_| PyTypeError::new_err("absent marker must be bytes"))?;
        let absent = absent_marker.as_bytes() == b"a";

        let refs_obj = tuple.get_item(1)?;
        let mut refs: Vec<Vec<IndexKey>> = Vec::new();
        for ref_list_obj in refs_obj.try_iter()? {
            let ref_list_obj = ref_list_obj?;
            let mut ref_list: Vec<IndexKey> = Vec::new();
            for ref_key_obj in ref_list_obj.try_iter()? {
                ref_list.push(extract_key(&ref_key_obj?)?);
            }
            refs.push(ref_list);
        }

        let value = tuple
            .get_item(2)?
            .cast_into::<PyBytes>()
            .map_err(|_| PyTypeError::new_err("node value must be bytes"))?;

        nodes.push(IndexNode {
            key,
            absent,
            references: refs,
            value: value.as_bytes().to_vec(),
        });
    }

    let out =
        serialize_graph_index(&nodes, reference_lists, key_elements).map_err(index_err_to_py)?;
    Ok(PyBytes::new(py, &out))
}

/// Parse the graph-index file header. Returns
/// `(node_ref_lists, key_length, key_count, header_end)`.
#[pyfunction]
#[pyo3(name = "parse_header")]
fn py_parse_header(data: &[u8]) -> PyResult<(usize, usize, usize, usize)> {
    let IndexHeader {
        node_ref_lists,
        key_length,
        key_count,
        header_end,
    } = parse_header(data).map_err(index_err_to_py)?;
    Ok((node_ref_lists, key_length, key_count, header_end))
}

/// Convert a `RawNode` into the tuple shape stored in
/// `GraphIndex._keys_by_offset`: `(key_tuple, absent_bytes,
/// tuple_of_ref_tuples, value_bytes)`.
fn raw_node_to_py<'py>(py: Python<'py>, raw: &RawNode) -> PyResult<Bound<'py, PyTuple>> {
    let key_tuple = key_to_py(py, &raw.key)?;
    let absent_bytes = PyBytes::new(py, if raw.absent { b"a" } else { b"" });
    let ref_tuples: Vec<Bound<PyTuple>> = raw
        .ref_offsets
        .iter()
        .map(|inner| {
            let items: Vec<Bound<PyAny>> = inner
                .iter()
                .map(|o| -> PyResult<Bound<PyAny>> { Ok(o.into_pyobject(py)?.into_any()) })
                .collect::<PyResult<_>>()?;
            PyTuple::new(py, items)
        })
        .collect::<PyResult<_>>()?;
    let refs_tuple = PyTuple::new(py, ref_tuples)?;
    let value_bytes = PyBytes::new(py, &raw.value);
    PyTuple::new(
        py,
        [
            key_tuple.into_any(),
            absent_bytes.into_any(),
            refs_tuple.into_any(),
            value_bytes.into_any(),
        ],
    )
}

/// Parse a batch of node lines. Returns
/// `(first_key_or_none, last_key_or_none, nodes_list, trailers,
/// keys_by_offset_dict)`.
///
/// When `node_ref_lists == 0`, each entry in `nodes_list` is
/// `(key_tuple, value_bytes)`. Otherwise it is
/// `(key_tuple, (value_bytes, ref_lists_tuple))` where ref lists are tuples
/// of integer byte offsets.
#[pyfunction]
#[pyo3(name = "parse_lines")]
fn py_parse_lines<'py>(
    py: Python<'py>,
    lines: Bound<'py, PyList>,
    start_pos: u64,
    key_length: usize,
    node_ref_lists: usize,
) -> PyResult<ParseLinesResult<'py>> {
    let owned: Vec<Vec<u8>> = lines
        .iter()
        .map(|item| -> PyResult<Vec<u8>> {
            Ok(item
                .cast_into::<PyBytes>()
                .map_err(|_| PyTypeError::new_err("line must be bytes"))?
                .as_bytes()
                .to_vec())
        })
        .collect::<PyResult<_>>()?;
    let slices: Vec<&[u8]> = owned.iter().map(|l| l.as_slice()).collect();

    let ParsedLines {
        first_key,
        last_key,
        nodes,
        keys_by_offset,
        trailers,
    } = parse_lines(&slices, start_pos, key_length).map_err(index_err_to_py)?;

    let first_py: Bound<PyAny> = match first_key {
        Some(k) => key_to_py(py, &k)?.into_any(),
        None => py.None().into_bound(py),
    };
    let last_py: Bound<PyAny> = match last_key {
        Some(k) => key_to_py(py, &k)?.into_any(),
        None => py.None().into_bound(py),
    };

    // Node list shape depends on node_ref_lists — mirrors the Python logic
    // in `_parse_lines`.
    let nodes_list = PyList::empty(py);
    for (key, value, refs) in &nodes {
        let key_tuple = key_to_py(py, key)?;
        let value_bytes = PyBytes::new(py, value);
        if node_ref_lists == 0 {
            nodes_list.append(PyTuple::new(
                py,
                [key_tuple.into_any(), value_bytes.into_any()],
            )?)?;
        } else {
            let ref_tuples: Vec<Bound<PyTuple>> = refs
                .iter()
                .map(|inner| {
                    let items: Vec<Bound<PyAny>> = inner
                        .iter()
                        .map(|o| -> PyResult<Bound<PyAny>> { Ok(o.into_pyobject(py)?.into_any()) })
                        .collect::<PyResult<_>>()?;
                    PyTuple::new(py, items)
                })
                .collect::<PyResult<_>>()?;
            let refs_tuple = PyTuple::new(py, ref_tuples)?;
            let node_value = PyTuple::new(py, [value_bytes.into_any(), refs_tuple.into_any()])?;
            nodes_list.append(PyTuple::new(
                py,
                [key_tuple.into_any(), node_value.into_any()],
            )?)?;
        }
    }

    let offset_dict = PyDict::new(py);
    for (pos, raw) in &keys_by_offset {
        offset_dict.set_item(*pos, raw_node_to_py(py, raw)?)?;
    }

    Ok((first_py, last_py, nodes_list, trailers, offset_dict))
}

/// Tuple returned by [`py_parse_lines`]. Named so the complex-type clippy
/// lint doesn't fire.
type ParseLinesResult<'py> = (
    Bound<'py, PyAny>,
    Bound<'py, PyAny>,
    Bound<'py, PyList>,
    usize,
    Bound<'py, PyDict>,
);

/// Adapter that lets a Python `Transport` object stand in for a Rust
/// [`IndexTransport`]. Calls `transport.get_bytes(name)` on the wrapped
/// object — the index never needs anything else from the Python
/// transport for the full-load path.
struct PyIndexTransport {
    obj: Py<PyAny>,
}

impl IndexTransport for PyIndexTransport {
    fn get_bytes(&self, path: &str) -> Result<Vec<u8>, IndexError> {
        Python::attach(|py| {
            let result = self
                .obj
                .bind(py)
                .call_method1("get_bytes", (path,))
                .map_err(|e| IndexError::Other(e.to_string()))?;
            let bytes = result
                .cast_into::<PyBytes>()
                .map_err(|_| IndexError::Other("get_bytes did not return bytes".to_string()))?;
            Ok(bytes.as_bytes().to_vec())
        })
    }

    fn abspath(&self, path: &str) -> String {
        Python::attach(|py| {
            self.obj
                .bind(py)
                .call_method1("abspath", (path,))
                .ok()
                .and_then(|r| r.extract::<String>().ok())
                .unwrap_or_else(|| path.to_string())
        })
    }
}

/// A pyo3-exposed graph-index reader backed by the pure-Rust
/// [`bazaar::index::GraphIndex`].
///
/// The Python wrapper around it should pass any `Transport`-shaped
/// object that implements `get_bytes` (the standard
/// `bzrformats.transport.Transport` does). Bisection-based partial
/// reads remain in Python; this binding is for callers that want the
/// full-load path.
#[pyclass(name = "GraphIndex")]
struct PyGraphIndex {
    inner: std::sync::Mutex<RsGraphIndex<PyIndexTransport>>,
}

fn extract_prefix(obj: &Bound<PyAny>) -> PyResult<KeyPrefix> {
    let mut out = Vec::new();
    for item in obj.try_iter()? {
        let elem = item?;
        if elem.is_none() {
            out.push(None);
        } else {
            let b = elem
                .cast_into::<PyBytes>()
                .map_err(|_| PyTypeError::new_err("prefix element must be bytes or None"))?;
            out.push(Some(b.as_bytes().to_vec()));
        }
    }
    Ok(out)
}

#[pymethods]
impl PyGraphIndex {
    #[new]
    #[pyo3(signature = (transport, name, _size = None, _unlimited_cache = false, offset = 0))]
    fn new(
        transport: Py<PyAny>,
        name: String,
        _size: Option<u64>,
        _unlimited_cache: bool,
        offset: u64,
    ) -> PyResult<Self> {
        let t = PyIndexTransport { obj: transport };
        Ok(Self {
            inner: std::sync::Mutex::new(RsGraphIndex::new(t, name, offset)),
        })
    }

    fn key_count(&self) -> PyResult<usize> {
        self.inner
            .lock()
            .unwrap()
            .key_count()
            .map_err(index_err_to_py)
    }

    #[getter]
    fn node_ref_lists(&self) -> PyResult<usize> {
        self.inner
            .lock()
            .unwrap()
            .node_ref_lists()
            .map_err(index_err_to_py)
    }

    #[getter]
    fn _key_length(&self) -> PyResult<usize> {
        self.inner
            .lock()
            .unwrap()
            .key_length()
            .map_err(index_err_to_py)
    }

    fn validate(&self) -> PyResult<()> {
        self.inner
            .lock()
            .unwrap()
            .validate()
            .map_err(index_err_to_py)
    }

    fn _buffer_all(&self) -> PyResult<()> {
        self.inner
            .lock()
            .unwrap()
            .buffer_all()
            .map_err(index_err_to_py)
    }

    /// Yield `(self, key, value)` or `(self, key, value, refs)` tuples
    /// matching the Python `GraphIndex.iter_all_entries` shape.
    fn iter_all_entries<'py>(
        slf: Bound<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyList>> {
        let (entries, node_ref_lists) = {
            let r = slf.borrow();
            let mut g = r.inner.lock().unwrap();
            let entries = g.iter_all_entries().map_err(index_err_to_py)?;
            let nrl = g.node_ref_lists().map_err(index_err_to_py)?;
            (entries, nrl)
        };
        emit_entries(py, &slf, &entries, node_ref_lists)
    }

    /// Same as `iter_all_entries` but restricted to `keys`.
    fn iter_entries<'py>(
        slf: Bound<'py, Self>,
        py: Python<'py>,
        keys: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        let mut requested: Vec<IndexKey> = Vec::new();
        for key_obj in keys.try_iter()? {
            requested.push(extract_key(&key_obj?)?);
        }
        if requested.is_empty() {
            return Ok(PyList::empty(py));
        }
        let (entries, node_ref_lists) = {
            let r = slf.borrow();
            let mut g = r.inner.lock().unwrap();
            let entries = g.iter_entries(&requested).map_err(index_err_to_py)?;
            let nrl = g.node_ref_lists().map_err(index_err_to_py)?;
            (entries, nrl)
        };
        emit_entries(py, &slf, &entries, node_ref_lists)
    }

    /// Same shape as `iter_entries`, but matches by prefix.
    fn iter_entries_prefix<'py>(
        slf: Bound<'py, Self>,
        py: Python<'py>,
        keys: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        let mut prefixes: Vec<KeyPrefix> = Vec::new();
        for prefix_obj in keys.try_iter()? {
            prefixes.push(extract_prefix(&prefix_obj?)?);
        }
        if prefixes.is_empty() {
            return Ok(PyList::empty(py));
        }
        let (entries, node_ref_lists) = {
            let r = slf.borrow();
            let mut g = r.inner.lock().unwrap();
            let entries = g.iter_entries_prefix(&prefixes).map_err(|e| match e {
                IndexError::Other(msg) if msg.starts_with("BadIndexKey") => {
                    PyValueError::new_err(msg)
                }
                other => index_err_to_py(other),
            })?;
            let nrl = g.node_ref_lists().map_err(index_err_to_py)?;
            (entries, nrl)
        };
        emit_entries(py, &slf, &entries, node_ref_lists)
    }

    /// Set of keys referenced by `ref_list_num` that aren't present in
    /// the index.
    fn external_references<'py>(
        &self,
        py: Python<'py>,
        ref_list_num: usize,
    ) -> PyResult<Bound<'py, pyo3::types::PySet>> {
        let refs = self
            .inner
            .lock()
            .unwrap()
            .external_references(ref_list_num)
            .map_err(|e| match e {
                IndexError::Other(msg) if msg.starts_with("No ref list") => {
                    PyValueError::new_err(msg)
                }
                other => index_err_to_py(other),
            })?;
        let set = pyo3::types::PySet::empty(py)?;
        for r in refs {
            set.add(key_to_py(py, &r)?)?;
        }
        Ok(set)
    }

    fn __repr__(&self) -> PyResult<String> {
        Ok("GraphIndex(<rust>)".to_string())
    }
}

/// Build the per-entry tuple matching Python's `iter_all_entries`
/// shape: `(self, key, value)` for zero-ref-list indexes, or
/// `(self, key, value, refs)` otherwise.
fn emit_entries<'py>(
    py: Python<'py>,
    slf: &Bound<'py, PyGraphIndex>,
    entries: &[IndexEntry],
    node_ref_lists: usize,
) -> PyResult<Bound<'py, PyList>> {
    let out = PyList::empty(py);
    let self_any: Bound<PyAny> = slf.clone().into_any();
    for (key, value, refs) in entries {
        let key_t = key_to_py(py, key)?;
        let value_b = PyBytes::new(py, value);
        if node_ref_lists == 0 {
            out.append(PyTuple::new(
                py,
                [self_any.clone(), key_t.into_any(), value_b.into_any()],
            )?)?;
        } else {
            let mut ref_tuples: Vec<Bound<PyTuple>> = Vec::with_capacity(refs.len());
            for inner in refs {
                let key_tuples: Vec<Bound<PyTuple>> = inner
                    .iter()
                    .map(|k| key_to_py(py, k))
                    .collect::<PyResult<_>>()?;
                ref_tuples.push(PyTuple::new(py, key_tuples)?);
            }
            let refs_tuple = PyTuple::new(py, ref_tuples)?;
            out.append(PyTuple::new(
                py,
                [
                    self_any.clone(),
                    key_t.into_any(),
                    value_b.into_any(),
                    refs_tuple.into_any(),
                ],
            )?)?;
        }
    }
    Ok(out)
}

/// Parse a full index file given its raw bytes (with any base-offset
/// already trimmed off by the caller). Returns
/// `(node_ref_lists, key_length, key_count, nodes_dict)` where
/// `nodes_dict` is keyed by the node's tuple-of-bytes key.
///
/// For 0-ref-list indexes the dict values are `value_bytes`; otherwise
/// they are `(value_bytes, refs_tuple)` matching the layout
/// `GraphIndex._buffer_all` produces.
#[pyfunction]
#[pyo3(name = "parse_full")]
fn py_parse_full<'py>(
    py: Python<'py>,
    data: &[u8],
) -> PyResult<(usize, usize, usize, Bound<'py, PyDict>)> {
    let (header, nodes) = parse_full(data).map_err(index_err_to_py)?;
    let nodes_dict = PyDict::new(py);
    for (key, (value, refs)) in &nodes {
        let key_t = key_to_py(py, key)?;
        let value_b = PyBytes::new(py, value);
        if header.node_ref_lists == 0 {
            nodes_dict.set_item(key_t, value_b)?;
        } else {
            let mut ref_tuples: Vec<Bound<PyTuple>> = Vec::with_capacity(refs.len());
            for inner in refs {
                let key_tuples: Vec<Bound<PyTuple>> = inner
                    .iter()
                    .map(|k| key_to_py(py, k))
                    .collect::<PyResult<_>>()?;
                ref_tuples.push(PyTuple::new(py, key_tuples)?);
            }
            let refs_tuple = PyTuple::new(py, ref_tuples)?;
            let value_tuple = PyTuple::new(py, [value_b.into_any(), refs_tuple.into_any()])?;
            nodes_dict.set_item(key_t, value_tuple)?;
        }
    }
    Ok((
        header.node_ref_lists,
        header.key_length,
        header.key_count,
        nodes_dict,
    ))
}

pub fn _index_rs(py: Python) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "index")?;
    m.add_function(wrap_pyfunction!(py_serialize_graph_index, &m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_header, &m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_lines, &m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_full, &m)?)?;
    m.add_class::<PyGraphIndex>()?;
    Ok(m)
}
