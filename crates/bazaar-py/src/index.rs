use bazaar::index::{
    parse_full, parse_header, parse_lines, serialize_graph_index, GraphIndex as RsGraphIndex,
    IndexEntry, IndexError, IndexHeader, IndexKey, IndexNode, IndexTransport, KeyPrefix,
    ParsedLines, ParsedRangeMap as RsParsedRangeMap, RawNode,
};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::import_exception;
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyBytes, PyDict, PyList, PyTuple};

import_exception!(bzrformats.index, BadIndexFormatSignature);
import_exception!(bzrformats.index, BadIndexOptions);
import_exception!(bzrformats.index, BadIndexData);
import_exception!(bzrformats.index, BadIndexKey);
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

/// Tracks which byte spans of a graph-index file have already been
/// parsed by the bisection path, along with the corresponding key
/// ranges. Replaces the parallel `_parsed_byte_map` /
/// `_parsed_key_map` lists in the Python `GraphIndex`.
#[pyclass(name = "ParsedRangeMap")]
struct PyParsedRangeMap {
    inner: std::sync::Mutex<RsParsedRangeMap>,
}

fn key_or_none_from_py(obj: &Bound<PyAny>) -> PyResult<Option<IndexKey>> {
    if obj.is_none() {
        return Ok(None);
    }
    let key = extract_key(obj)?;
    Ok(Some(key))
}

fn key_or_none_to_py<'py>(py: Python<'py>, k: &Option<IndexKey>) -> PyResult<Bound<'py, PyAny>> {
    match k {
        Some(key) => Ok(key_to_py(py, key)?.into_any()),
        None => Ok(py.None().into_bound(py)),
    }
}

#[pymethods]
impl PyParsedRangeMap {
    #[new]
    fn new() -> Self {
        Self {
            inner: std::sync::Mutex::new(RsParsedRangeMap::new()),
        }
    }

    fn __len__(&self) -> usize {
        self.inner.lock().unwrap().len()
    }

    fn byte_range<'py>(&self, py: Python<'py>, index: usize) -> PyResult<Bound<'py, PyTuple>> {
        let m = self.inner.lock().unwrap();
        let (start, end) = m
            .byte_range(index)
            .ok_or_else(|| pyo3::exceptions::PyIndexError::new_err(index))?;
        PyTuple::new(
            py,
            [
                start.into_pyobject(py)?.into_any(),
                end.into_pyobject(py)?.into_any(),
            ],
        )
    }

    fn key_range<'py>(&self, py: Python<'py>, index: usize) -> PyResult<Bound<'py, PyTuple>> {
        let m = self.inner.lock().unwrap();
        let (start, end) = m
            .key_range(index)
            .ok_or_else(|| pyo3::exceptions::PyIndexError::new_err(index))?;
        let start_py = key_or_none_to_py(py, &start)?;
        let end_py = key_or_none_to_py(py, &end)?;
        PyTuple::new(py, [start_py, end_py])
    }

    fn byte_index(&self, offset: u64) -> isize {
        self.inner.lock().unwrap().byte_index(offset)
    }

    fn key_index(&self, key: Bound<'_, PyAny>) -> PyResult<isize> {
        // The Python caller passes a key tuple — never None — but be
        // defensive for the empty-tuple sentinel that means "before any
        // real key".
        let probe = key_or_none_from_py(&key)?;
        Ok(self.inner.lock().unwrap().key_index(&probe))
    }

    fn is_parsed(&self, offset: u64) -> bool {
        self.inner.lock().unwrap().is_parsed(offset)
    }

    fn mark_parsed<'py>(
        &self,
        start: u64,
        start_key: Bound<'py, PyAny>,
        end: u64,
        end_key: Bound<'py, PyAny>,
    ) -> PyResult<()> {
        let sk = key_or_none_from_py(&start_key)?;
        let ek = key_or_none_from_py(&end_key)?;
        self.inner.lock().unwrap().mark_parsed(start, sk, end, ek);
        Ok(())
    }

    /// Materialise the byte-range list as `[(start, end), ...]`.
    fn byte_ranges<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let m = self.inner.lock().unwrap();
        let out = PyList::empty(py);
        for i in 0..m.len() {
            let (s, e) = m.byte_range(i).expect("in range");
            out.append(PyTuple::new(
                py,
                [
                    s.into_pyobject(py)?.into_any(),
                    e.into_pyobject(py)?.into_any(),
                ],
            )?)?;
        }
        Ok(out)
    }

    /// Materialise the key-range list as `[(start_key, end_key), ...]`.
    fn key_ranges<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let m = self.inner.lock().unwrap();
        let out = PyList::empty(py);
        for i in 0..m.len() {
            let (s, e) = m.key_range(i).expect("in range");
            let sp = key_or_none_to_py(py, &s)?;
            let ep = key_or_none_to_py(py, &e)?;
            out.append(PyTuple::new(py, [sp, ep])?)?;
        }
        Ok(out)
    }
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

/// Linear-scan prefix lookup over a `_nodes`-shaped dict. Each prefix
/// is a tuple the same length as a key with `None` permitted in any
/// position except the first.
///
/// `mode` selects the dict-value shape:
///  - `"reader-norefs"`: values are `bytes`; entries are `(key, value)`.
///  - `"reader-refs"`:   values are `(bytes, refs)`; entries are
///    `(key, value, refs)`.
///  - `"builder-norefs"`: values are `(absent, refs, value)`; entries are
///    `(key, value)`. Absent nodes are skipped.
///  - `"builder-refs"`:   values are `(absent, refs, value)`; entries are
///    `(key, value, refs)`. Absent nodes are skipped.
///  - `"btree-builder-norefs"`: values are `(refs, value)`; entries are
///    `(key, value)`.
///  - `"btree-builder-refs"`:   values are `(refs, value)`; entries are
///    `(key, value, refs)`.
///
/// Returns a list of result tuples; the caller prepends `self`.
#[pyfunction]
#[pyo3(name = "iter_entries_prefix")]
fn py_iter_entries_prefix<'py>(
    py: Python<'py>,
    nodes: Bound<'py, PyDict>,
    prefixes: Bound<'py, PyAny>,
    key_length: usize,
    mode: &str,
) -> PyResult<Bound<'py, PyList>> {
    enum NodeShape {
        ReaderNoRefs,
        ReaderRefs,
        BuilderNoRefs,
        BuilderRefs,
        BTreeBuilderNoRefs,
        BTreeBuilderRefs,
    }
    let shape = match mode {
        "reader-norefs" => NodeShape::ReaderNoRefs,
        "reader-refs" => NodeShape::ReaderRefs,
        "builder-norefs" => NodeShape::BuilderNoRefs,
        "builder-refs" => NodeShape::BuilderRefs,
        "btree-builder-norefs" => NodeShape::BTreeBuilderNoRefs,
        "btree-builder-refs" => NodeShape::BTreeBuilderRefs,
        other => {
            return Err(PyValueError::new_err(format!(
                "unknown iter_entries_prefix mode: {other}"
            )))
        }
    };
    let mut parsed: Vec<(Bound<'py, PyTuple>, KeyPrefix)> = Vec::new();
    let mut seen_prefixes: std::collections::HashSet<Vec<Option<Vec<u8>>>> =
        std::collections::HashSet::new();
    for prefix_obj in prefixes.try_iter()? {
        let prefix_obj = prefix_obj?;
        let prefix_tuple = prefix_obj
            .cast::<PyTuple>()
            .map_err(|_| BadIndexKey::new_err((prefix_obj.clone().unbind(),)))?
            .clone();
        let prefix = extract_prefix(prefix_tuple.as_any())
            .map_err(|_| BadIndexKey::new_err((prefix_tuple.clone().unbind(),)))?;
        if prefix.len() != key_length || prefix.first().is_none_or(|e| e.is_none()) {
            return Err(BadIndexKey::new_err((prefix_tuple.unbind(),)));
        }
        if seen_prefixes.insert(prefix.clone()) {
            parsed.push((prefix_tuple, prefix));
        }
    }

    let out = PyList::empty(py);
    if parsed.is_empty() {
        return Ok(out);
    }

    let mut emitted: std::collections::HashSet<Vec<Vec<u8>>> = std::collections::HashSet::new();
    for (key_obj, value_obj) in nodes.iter() {
        let key_tuple = match key_obj.cast::<PyTuple>() {
            Ok(t) => t.clone(),
            Err(_) => continue,
        };
        let key_rs = match extract_key(key_tuple.as_any()) {
            Ok(k) => k,
            Err(_) => continue,
        };
        if key_rs.len() != key_length {
            continue;
        }
        let any_match = parsed.iter().any(|(_, prefix)| {
            prefix
                .iter()
                .zip(key_rs.iter())
                .all(|(p_elem, k_elem)| match p_elem {
                    Some(p) => p == k_elem,
                    None => true,
                })
        });
        if !any_match {
            continue;
        }
        if !emitted.insert(key_rs) {
            continue;
        }
        match shape {
            NodeShape::ReaderNoRefs => {
                out.append(PyTuple::new(py, [key_tuple.into_any(), value_obj])?)?;
            }
            NodeShape::ReaderRefs => {
                let value_tuple = value_obj
                    .cast_into::<PyTuple>()
                    .map_err(|_| PyTypeError::new_err("node value must be a 2-tuple"))?;
                let value_b = value_tuple.get_item(0)?;
                let refs_t = value_tuple.get_item(1)?;
                out.append(PyTuple::new(
                    py,
                    [key_tuple.into_any(), value_b.into_any(), refs_t.into_any()],
                )?)?;
            }
            NodeShape::BuilderNoRefs | NodeShape::BuilderRefs => {
                let value_tuple = value_obj
                    .cast_into::<PyTuple>()
                    .map_err(|_| PyTypeError::new_err("builder node must be a 3-tuple"))?;
                let absent_obj = value_tuple.get_item(0)?;
                let absent_bytes = absent_obj
                    .cast::<PyBytes>()
                    .map_err(|_| PyTypeError::new_err("absent marker must be bytes"))?;
                if absent_bytes.as_bytes() == b"a" {
                    continue;
                }
                let refs_t = value_tuple.get_item(1)?;
                let value_b = value_tuple.get_item(2)?;
                if matches!(shape, NodeShape::BuilderRefs) {
                    out.append(PyTuple::new(
                        py,
                        [key_tuple.into_any(), value_b.into_any(), refs_t.into_any()],
                    )?)?;
                } else {
                    out.append(PyTuple::new(
                        py,
                        [key_tuple.into_any(), value_b.into_any()],
                    )?)?;
                }
            }
            NodeShape::BTreeBuilderNoRefs | NodeShape::BTreeBuilderRefs => {
                let value_tuple = value_obj
                    .cast_into::<PyTuple>()
                    .map_err(|_| PyTypeError::new_err("btree builder node must be a 2-tuple"))?;
                let refs_t = value_tuple.get_item(0)?;
                let value_b = value_tuple.get_item(1)?;
                if matches!(shape, NodeShape::BTreeBuilderRefs) {
                    out.append(PyTuple::new(
                        py,
                        [key_tuple.into_any(), value_b.into_any(), refs_t.into_any()],
                    )?)?;
                } else {
                    out.append(PyTuple::new(
                        py,
                        [key_tuple.into_any(), value_b.into_any()],
                    )?)?;
                }
            }
        }
    }
    Ok(out)
}

/// External references for a `GraphIndexBuilder`-shaped `_nodes` dict.
///
/// Returns the set of keys referenced from the second reference list
/// of any present node that aren't themselves present (or are absent)
/// in the index. Mirrors `GraphIndexBuilder._external_references`.
///
/// `nodes` is `{key: (absent_marker_bytes, refs_tuple, value_bytes)}`.
/// `reference_lists` is the configured number of parallel reference
/// lists; the function returns an empty set unless this is `>= 2`.
#[pyfunction]
#[pyo3(name = "external_references_from_builder_nodes")]
fn py_external_references_from_builder_nodes<'py>(
    py: Python<'py>,
    nodes: Bound<'py, PyDict>,
    reference_lists: usize,
) -> PyResult<Bound<'py, pyo3::types::PySet>> {
    let out = pyo3::types::PySet::empty(py)?;
    if reference_lists < 2 {
        return Ok(out);
    }
    let mut present: std::collections::HashSet<Vec<Vec<u8>>> = std::collections::HashSet::new();
    let mut refs: Vec<Bound<'py, PyAny>> = Vec::new();
    for (key_obj, value_obj) in nodes.iter() {
        let value_tuple = value_obj
            .cast_into::<PyTuple>()
            .map_err(|_| PyTypeError::new_err("builder node must be a 3-tuple"))?;
        let absent_obj = value_tuple.get_item(0)?;
        let absent_bytes = absent_obj
            .cast::<PyBytes>()
            .map_err(|_| PyTypeError::new_err("absent marker must be bytes"))?;
        if absent_bytes.as_bytes() == b"a" {
            continue;
        }
        let key_tuple = key_obj
            .cast::<PyTuple>()
            .map_err(|_| PyTypeError::new_err("key must be a tuple"))?;
        let key_rs = extract_key(key_tuple.as_any())?;
        present.insert(key_rs);
        let refs_tuple_obj = value_tuple.get_item(1)?;
        let refs_tuple = refs_tuple_obj
            .cast::<PyTuple>()
            .map_err(|_| PyTypeError::new_err("refs must be a tuple"))?;
        if refs_tuple.len() < 2 {
            continue;
        }
        let second_refs_obj = refs_tuple.get_item(1)?;
        for ref_obj in second_refs_obj.try_iter()? {
            refs.push(ref_obj?);
        }
    }
    for ref_obj in refs {
        let ref_tuple = ref_obj
            .cast::<PyTuple>()
            .map_err(|_| PyTypeError::new_err("ref must be a tuple"))?;
        let ref_rs = extract_key(ref_tuple.as_any())?;
        if !present.contains(&ref_rs) {
            out.add(ref_tuple)?;
        }
    }
    Ok(out)
}

pub fn _index_rs(py: Python) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "index")?;
    m.add_function(wrap_pyfunction!(py_serialize_graph_index, &m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_header, &m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_lines, &m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_full, &m)?)?;
    m.add_function(wrap_pyfunction!(py_iter_entries_prefix, &m)?)?;
    m.add_function(wrap_pyfunction!(
        py_external_references_from_builder_nodes,
        &m
    )?)?;
    m.add_class::<PyGraphIndex>()?;
    m.add_class::<PyParsedRangeMap>()?;
    Ok(m)
}
