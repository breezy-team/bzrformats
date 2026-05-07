use bazaar::index::{
    key_is_valid, parse_full, parse_header, parse_lines, serialize_graph_index, value_is_valid,
    GraphIndex as RsGraphIndex, IndexEntry, IndexError, IndexHeader, IndexKey, IndexNode,
    IndexTransport, KeyPrefix, ParsedLines, ParsedRangeMap as RsParsedRangeMap, RawNode,
};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::import_exception;
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyBytes, PyDict, PyList, PyTuple};

import_exception!(bzrformats.index, BadIndexFormatSignature);
import_exception!(bzrformats.index, BadIndexOptions);
import_exception!(bzrformats.index, BadIndexData);
import_exception!(bzrformats.index, BadIndexKey);
import_exception!(bzrformats.index, BadIndexValue);
import_exception!(bzrformats.index, BadIndexDuplicateKey);
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
/// [`IndexTransport`]. Holds an unbound `Py<PyAny>` and re-attaches to a
/// `Python<'_>` for each call.
struct PyIndexTransport {
    obj: Py<PyAny>,
}

impl Clone for PyIndexTransport {
    fn clone(&self) -> Self {
        Python::attach(|py| Self {
            obj: self.obj.clone_ref(py),
        })
    }
}

thread_local! {
    /// The most recent Python exception raised by a `PyIndexTransport`
    /// call; the pyo3 method dispatcher consults this so the original
    /// exception class (e.g. `TransportNoSuchFile`) is preserved
    /// across the Rust boundary.
    static PENDING_PY_ERR: std::cell::RefCell<Option<PyErr>> = const {
        std::cell::RefCell::new(None)
    };
}

fn stash_py_err(err: PyErr) -> IndexError {
    let msg = err.to_string();
    PENDING_PY_ERR.with(|c| *c.borrow_mut() = Some(err));
    IndexError::Other(format!("__pyerr__: {msg}"))
}

fn reraise_pending_pyerr_or(err: IndexError) -> PyErr {
    if let Some(stashed) = PENDING_PY_ERR.with(|c| c.borrow_mut().take()) {
        return stashed;
    }
    index_err_to_py(err)
}

impl IndexTransport for PyIndexTransport {
    fn get_bytes(&self, path: &str) -> Result<Vec<u8>, IndexError> {
        Python::attach(|py| {
            let result = self
                .obj
                .bind(py)
                .call_method1("get_bytes", (path,))
                .map_err(stash_py_err)?;
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

    fn readv(
        &self,
        path: &str,
        ranges: &[(u64, u64)],
        adjust_for_latency: bool,
        upper_limit: u64,
    ) -> Result<Vec<(u64, Vec<u8>)>, IndexError> {
        Python::attach(|py| -> Result<_, IndexError> {
            let py_ranges: Vec<Bound<'_, PyTuple>> = ranges
                .iter()
                .map(|(o, l)| PyTuple::new(py, [*o, *l]))
                .collect::<PyResult<_>>()
                .map_err(|e| IndexError::Other(e.to_string()))?;
            let py_list = pyo3::types::PyList::new(py, py_ranges)
                .map_err(|e| IndexError::Other(e.to_string()))?;
            let kwargs = pyo3::types::PyDict::new(py);
            kwargs
                .set_item("adjust_for_latency", adjust_for_latency)
                .map_err(|e| IndexError::Other(e.to_string()))?;
            kwargs
                .set_item("upper_limit", upper_limit)
                .map_err(|e| IndexError::Other(e.to_string()))?;
            let iter = self
                .obj
                .bind(py)
                .call_method("readv", (path, py_list), Some(&kwargs))
                .map_err(stash_py_err)?;
            let mut out = Vec::with_capacity(ranges.len());
            for item in iter.try_iter().map_err(stash_py_err)? {
                let item = item.map_err(stash_py_err)?;
                let tup = item
                    .cast_into::<PyTuple>()
                    .map_err(|_| IndexError::Other("readv yielded non-tuple item".to_string()))?;
                let offset_obj = tup.get_item(0).map_err(stash_py_err)?;
                let offset: u64 = offset_obj.extract().map_err(stash_py_err)?;
                let bytes = tup
                    .get_item(1)
                    .map_err(stash_py_err)?
                    .cast_into::<PyBytes>()
                    .map_err(|_| {
                        IndexError::Other("readv yielded non-bytes payload".to_string())
                    })?;
                out.push((offset, bytes.as_bytes().to_vec()));
            }
            Ok(out)
        })
    }
}

/// pyo3-exposed graph-index reader. Owns both the Rust-side
/// [`bazaar::index::GraphIndex`] state and the original Python
/// transport reference — the latter is exposed as `_transport` so that
/// Python tests, hashing, and equality keep working.
#[pyclass(name = "GraphIndex", subclass)]
struct PyGraphIndex {
    /// Rust-side index state. Wrapped in a `Mutex` because pyo3 method
    /// calls take `&self`.
    inner: std::sync::Mutex<RsGraphIndex<PyIndexTransport>>,
    /// The Python transport object passed to `__init__`. Tests and
    /// `__hash__` consult it directly.
    transport_py: Py<PyAny>,
    /// Filename within the transport.
    name: String,
    /// Backing-file size. `None` disables bisection.
    size: Option<u64>,
    /// Base offset into the backing file (used by pack-files).
    base_offset: u64,
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
    #[pyo3(signature = (transport, name, size = None, unlimited_cache = false, offset = 0))]
    fn new(
        py: Python<'_>,
        transport: Py<PyAny>,
        name: String,
        size: Option<u64>,
        unlimited_cache: bool,
        offset: u64,
    ) -> PyResult<Self> {
        let _ = unlimited_cache;
        let t = PyIndexTransport {
            obj: transport.clone_ref(py),
        };
        Ok(Self {
            inner: std::sync::Mutex::new(RsGraphIndex::with_size(t, name.clone(), offset, size)),
            transport_py: transport,
            name,
            size,
            base_offset: offset,
        })
    }

    #[getter]
    fn _transport<'py>(&self, py: Python<'py>) -> Bound<'py, PyAny> {
        self.transport_py.bind(py).clone()
    }

    #[getter]
    fn _name(&self) -> &str {
        &self.name
    }

    #[getter]
    fn _size(&self) -> Option<u64> {
        self.size
    }

    #[getter]
    fn _base_offset(&self) -> u64 {
        self.base_offset
    }

    #[getter]
    fn _bytes_read(&self) -> u64 {
        self.inner.lock().unwrap().bytes_read()
    }

    fn key_count(&self) -> PyResult<usize> {
        self.inner
            .lock()
            .unwrap()
            .key_count()
            .map_err(reraise_pending_pyerr_or)
    }

    #[getter]
    fn node_ref_lists(&self) -> PyResult<usize> {
        self.inner
            .lock()
            .unwrap()
            .node_ref_lists()
            .map_err(reraise_pending_pyerr_or)
    }

    #[getter]
    fn _key_length(&self) -> PyResult<usize> {
        self.inner
            .lock()
            .unwrap()
            .key_length()
            .map_err(reraise_pending_pyerr_or)
    }

    fn validate(&self) -> PyResult<()> {
        self.inner
            .lock()
            .unwrap()
            .validate()
            .map_err(reraise_pending_pyerr_or)
    }

    fn _buffer_all(&self) -> PyResult<()> {
        self.inner
            .lock()
            .unwrap()
            .buffer_all()
            .map_err(reraise_pending_pyerr_or)
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
            let entries = g.iter_all_entries().map_err(reraise_pending_pyerr_or)?;
            let nrl = g.node_ref_lists().map_err(reraise_pending_pyerr_or)?;
            (entries, nrl)
        };
        emit_entries(py, &slf, &entries, node_ref_lists)
    }

    /// Same as `iter_all_entries` but restricted to `keys`. When the
    /// index size is known and the key set is small relative to the
    /// total key count, this dispatches through bisection. Otherwise it
    /// promotes to `buffer_all`.
    fn iter_entries<'py>(
        slf: Bound<'py, Self>,
        py: Python<'py>,
        keys: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        // Materialise the input keys but defer bytes-extraction until
        // after we've decided whether to buffer the whole file. The
        // Python contract is: file-not-found errors should surface
        // before key-type errors.
        let key_objs: Vec<Bound<'py, PyAny>> = keys.try_iter()?.collect::<PyResult<Vec<_>>>()?;
        if key_objs.is_empty() {
            return Ok(PyList::empty(py));
        }
        // Decide whether to buffer the whole file or use bisection.
        let need_buffer_all = {
            let r = slf.borrow();
            let mut g = r.inner.lock().unwrap();
            match g.size() {
                None => true,
                Some(_) => {
                    if g.is_buffered_already() {
                        true
                    } else {
                        // Read just the header so we know the key count.
                        if g.key_count_or_zero() == 0 {
                            g.ensure_header_parsed().map_err(reraise_pending_pyerr_or)?;
                        }
                        // After buffer_all may have been triggered by the
                        // 50%-bytes heuristic.
                        if g.is_buffered_already() {
                            true
                        } else {
                            key_objs.len() * 20 > g.key_count_or_zero()
                        }
                    }
                }
            }
        };
        // I/O succeeded; now extract keys. Non-bytes elements are
        // silently dropped — they cannot match any actual key in the
        // index, which matches the duck-typed lookup the Python
        // version did via plain dict containment.
        let mut requested: Vec<IndexKey> = Vec::new();
        let mut seen: std::collections::HashSet<IndexKey> = std::collections::HashSet::new();
        for key_obj in &key_objs {
            let Ok(k) = extract_key(key_obj) else {
                continue;
            };
            if seen.insert(k.clone()) {
                requested.push(k);
            }
        }
        if requested.is_empty() {
            return Ok(PyList::empty(py));
        }
        if need_buffer_all {
            let (entries, node_ref_lists) = {
                let r = slf.borrow();
                let mut g = r.inner.lock().unwrap();
                let entries = g
                    .iter_entries(&requested)
                    .map_err(reraise_pending_pyerr_or)?;
                let nrl = g.node_ref_lists().map_err(reraise_pending_pyerr_or)?;
                (entries, nrl)
            };
            return emit_entries(py, &slf, &entries, node_ref_lists);
        }
        // Bisection path: use bisect_multi via Python.
        let bisect_multi = py.import("bzrformats.bisect_multi")?;
        let bisect_fn = bisect_multi.getattr("bisect_multi_bytes")?;
        let probe = slf.getattr("_lookup_keys_via_location")?;
        let size_obj = slf.borrow().size.unwrap_or(0).into_pyobject(py)?;
        let keys_set = pyo3::types::PySet::new(
            py,
            requested
                .iter()
                .map(|k| key_to_py(py, k))
                .collect::<PyResult<Vec<_>>>()?,
        )?;
        let bisect_result = bisect_fn.call1((probe, size_obj, keys_set))?;
        let out = PyList::empty(py);
        for item in bisect_result.try_iter()? {
            let item = item?;
            let tup = item
                .cast_into::<PyTuple>()
                .map_err(|_| PyTypeError::new_err("bisect_multi yielded non-tuple item"))?;
            let inner_result = tup.get_item(1)?;
            if inner_result.is_truthy()? {
                out.append(inner_result)?;
            }
        }
        Ok(out)
    }

    /// Same shape as `iter_entries`, but matches by prefix. Always
    /// triggers a full load (`buffer_all`); the pure-Rust prefix
    /// matcher only operates on the post-`buffer_all` node table.
    fn iter_entries_prefix<'py>(
        slf: Bound<'py, Self>,
        py: Python<'py>,
        keys: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        // Materialise the keys list once; we may iterate twice.
        let keys_list = pyo3::types::PyList::empty(py);
        for k in keys.try_iter()? {
            keys_list.append(k?)?;
        }
        if keys_list.is_empty() {
            return Ok(PyList::empty(py));
        }
        let (key_length, has_refs) = {
            let r = slf.borrow();
            let mut g = r.inner.lock().unwrap();
            g.buffer_all().map_err(reraise_pending_pyerr_or)?;
            let kl = g.key_length().map_err(reraise_pending_pyerr_or)?;
            let nrl = g.node_ref_lists().map_err(reraise_pending_pyerr_or)?;
            (kl, nrl > 0)
        };
        let nodes_dict = slf.getattr("_nodes")?;
        let nodes = nodes_dict
            .cast_into::<pyo3::types::PyDict>()
            .map_err(|_| PyTypeError::new_err("_nodes is not a dict"))?;
        let mode = if has_refs {
            "reader-refs"
        } else {
            "reader-norefs"
        };
        let entries = py_iter_entries_prefix(py, nodes, keys_list.into_any(), key_length, mode)?;
        // Prepend (self,) to each entry tuple.
        let out = PyList::empty(py);
        let self_any: Bound<PyAny> = slf.clone().into_any();
        for entry in entries.iter() {
            let tup = entry
                .cast_into::<PyTuple>()
                .map_err(|_| PyTypeError::new_err("entry must be a tuple"))?;
            let mut items: Vec<Bound<PyAny>> = vec![self_any.clone()];
            for it in tup.iter() {
                items.push(it.into_any());
            }
            out.append(PyTuple::new(py, items)?)?;
        }
        Ok(out)
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
                other => reraise_pending_pyerr_or(other),
            })?;
        let set = pyo3::types::PySet::empty(py)?;
        for r in refs {
            set.add(key_to_py(py, &r)?)?;
        }
        Ok(set)
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let abspath: String = self
            .transport_py
            .bind(py)
            .call_method1("abspath", (self.name.as_str(),))
            .ok()
            .and_then(|r| r.extract().ok())
            .unwrap_or_else(|| self.name.clone());
        Ok(format!("GraphIndex({:?})", abspath))
    }

    fn __richcmp__(
        &self,
        py: Python<'_>,
        other: Bound<'_, PyAny>,
        op: pyo3::pyclass::CompareOp,
    ) -> PyResult<Py<PyAny>> {
        if let Ok(rhs) = other.cast::<PyGraphIndex>() {
            let rhs_ref = rhs.borrow();
            let lhs_t = self.transport_py.bind(py);
            let rhs_t = rhs_ref.transport_py.bind(py);
            let transports_equal = lhs_t.eq(rhs_t).unwrap_or(false);
            let equal = transports_equal && self.name == rhs_ref.name && self.size == rhs_ref.size;
            return match op {
                pyo3::pyclass::CompareOp::Eq => {
                    Ok(equal.into_pyobject(py)?.to_owned().into_any().unbind())
                }
                pyo3::pyclass::CompareOp::Ne => {
                    Ok((!equal).into_pyobject(py)?.to_owned().into_any().unbind())
                }
                pyo3::pyclass::CompareOp::Lt => {
                    let lh = self.__hash__(py)?;
                    let rh = rhs_ref.__hash__(py)?;
                    Ok((lh < rh).into_pyobject(py)?.to_owned().into_any().unbind())
                }
                _ => Ok(py.NotImplemented()),
            };
        }
        match op {
            pyo3::pyclass::CompareOp::Eq => {
                Ok(false.into_pyobject(py)?.to_owned().into_any().unbind())
            }
            pyo3::pyclass::CompareOp::Ne => {
                Ok(true.into_pyobject(py)?.to_owned().into_any().unbind())
            }
            pyo3::pyclass::CompareOp::Lt => Err(PyTypeError::new_err(other.unbind())),
            _ => Ok(py.NotImplemented()),
        }
    }

    fn __hash__(&self, py: Python<'_>) -> PyResult<isize> {
        // Mirrors Python: hash((type(self), self._transport, self._name, self._size))
        let class_obj = py.get_type::<PyGraphIndex>();
        let tup = PyTuple::new(
            py,
            [
                class_obj.into_any(),
                self.transport_py.bind(py).clone(),
                pyo3::types::PyString::new(py, &self.name).into_any(),
                match self.size {
                    Some(s) => s.into_pyobject(py)?.into_any(),
                    None => py.None().into_bound(py),
                },
            ],
        )?;
        tup.hash()
    }

    /// Materialised dict of post-`buffer_all` nodes, or `None` if
    /// `buffer_all` hasn't run yet. Mirrors the Python `_nodes`
    /// attribute. Tests inspect this to confirm caching behaviour.
    #[getter]
    fn _nodes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let g = self.inner.lock().unwrap();
        if !g.is_buffered_already() {
            return Ok(py.None().into_bound(py));
        }
        let node_ref_lists = g.key_count_or_zero(); // unused; just to silence
        let _ = node_ref_lists;
        let nrl = g.header().map(|h| h.node_ref_lists).unwrap_or(0);
        let dict = pyo3::types::PyDict::new(py);
        for (key, (value, refs)) in g.nodes_iter() {
            let key_t = key_to_py(py, key)?;
            let value_b = PyBytes::new(py, value);
            if nrl == 0 {
                dict.set_item(key_t, value_b)?;
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
                let pair = PyTuple::new(py, [value_b.into_any(), refs_tuple.into_any()])?;
                dict.set_item(key_t, pair)?;
            }
        }
        Ok(dict.into_any())
    }

    /// Materialise the bisect-state node table. Tests inspect this to
    /// verify which keys the bisection path has already cached.
    #[getter]
    fn _bisect_nodes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let g = self.inner.lock().unwrap();
        match g.bisect_nodes() {
            None => Ok(py.None().into_bound(py)),
            Some(map) => {
                let dict = pyo3::types::PyDict::new(py);
                for (k, (value, refs)) in map.iter() {
                    let key_t = key_to_py(py, k)?;
                    let value_b = PyBytes::new(py, value);
                    if refs.is_empty() {
                        dict.set_item(key_t, value_b)?;
                    } else {
                        let mut ref_tuples: Vec<Bound<PyTuple>> = Vec::with_capacity(refs.len());
                        for inner in refs {
                            let items: Vec<Bound<PyAny>> = inner
                                .iter()
                                .map(|o| -> PyResult<Bound<PyAny>> {
                                    Ok(o.into_pyobject(py)?.into_any())
                                })
                                .collect::<PyResult<_>>()?;
                            ref_tuples.push(PyTuple::new(py, items)?);
                        }
                        let refs_tuple = PyTuple::new(py, ref_tuples)?;
                        let pair = PyTuple::new(py, [value_b.into_any(), refs_tuple.into_any()])?;
                        dict.set_item(key_t, pair)?;
                    }
                }
                Ok(dict.into_any())
            }
        }
    }

    #[getter]
    fn _keys_by_offset<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::types::PyDict>> {
        let g = self.inner.lock().unwrap();
        let dict = pyo3::types::PyDict::new(py);
        for (offset, raw) in g.keys_by_offset().iter() {
            dict.set_item(*offset, raw_node_to_py(py, raw)?)?;
        }
        Ok(dict)
    }

    /// Read-only view of the parsed-range map. Returns a fresh
    /// `ParsedRangeMap` snapshot; mutations on the returned object do
    /// not affect the index.
    #[getter]
    fn _range_map(&self) -> PyParsedRangeMap {
        let g = self.inner.lock().unwrap();
        PyParsedRangeMap {
            inner: std::sync::Mutex::new(g.range_map().clone()),
        }
    }

    /// Backward-compatible view of the parsed byte spans as
    /// `[(start, end), ...]`. Mirrors the pre-Rust-port attribute that
    /// older callers and tests still read.
    #[getter]
    fn _parsed_byte_map<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let g = self.inner.lock().unwrap();
        let m = g.range_map();
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

    /// Backward-compatible view of the parsed key spans as
    /// `[(start_key, end_key), ...]`. Mirrors the pre-Rust-port
    /// attribute that older callers and tests still read.
    #[getter]
    fn _parsed_key_map<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let g = self.inner.lock().unwrap();
        let m = g.range_map();
        let out = PyList::empty(py);
        for i in 0..m.len() {
            let (s, e) = m.key_range(i).expect("in range");
            let sp = key_or_none_to_py(py, &s)?;
            let ep = key_or_none_to_py(py, &e)?;
            out.append(PyTuple::new(py, [sp, ep])?)?;
        }
        Ok(out)
    }

    /// `_find_ancestors` from the Python class. Walks
    /// `iter_entries(keys)`, populating `parent_map` and adding any
    /// missing keys to `missing_keys`. Returns the set of newly-seen
    /// parent keys not yet in `parent_map`.
    fn _find_ancestors<'py>(
        slf: Bound<'py, Self>,
        py: Python<'py>,
        keys: Bound<'py, PyAny>,
        ref_list_num: usize,
        parent_map: Bound<'py, pyo3::types::PyDict>,
        missing_keys: Bound<'py, pyo3::types::PySet>,
    ) -> PyResult<Bound<'py, pyo3::types::PySet>> {
        let key_list = pyo3::types::PyList::empty(py);
        for k in keys.try_iter()? {
            key_list.append(k?)?;
        }
        let entries = slf.call_method1("iter_entries", (key_list.clone(),))?;
        let found = pyo3::types::PySet::empty(py)?;
        let new_search = pyo3::types::PySet::empty(py)?;
        for entry_obj in entries.try_iter()? {
            let entry = entry_obj?;
            let entry_t = entry
                .cast_into::<PyTuple>()
                .map_err(|_| PyTypeError::new_err("entry must be a tuple"))?;
            let key = entry_t.get_item(1)?;
            let refs = entry_t.get_item(3)?;
            let parent_keys = refs.get_item(ref_list_num)?;
            found.add(key.clone())?;
            parent_map.set_item(key, parent_keys.clone())?;
            for p in parent_keys.try_iter()? {
                new_search.add(p?)?;
            }
        }
        // Find missing keys: original_keys - found.
        for k in key_list.iter() {
            if !found.contains(k.clone())? {
                missing_keys.add(k)?;
            }
        }
        // Return new_search - parent_map keys.
        let result = pyo3::types::PySet::empty(py)?;
        for k in new_search.iter() {
            if !parent_map.contains(k.clone())? {
                result.add(k)?;
            }
        }
        Ok(result)
    }

    fn clear_cache(&self) {}

    /// Service a vectored read against the bisection state. Tests
    /// call this directly to exercise the parsed-region bookkeeping;
    /// mirrors the Python `_read_and_parse`.
    fn _read_and_parse(&self, readv_ranges: Bound<'_, PyAny>) -> PyResult<()> {
        let mut ranges: Vec<(u64, u64)> = Vec::new();
        for item in readv_ranges.try_iter()? {
            let item = item?;
            let tup = item
                .cast_into::<PyTuple>()
                .map_err(|_| PyTypeError::new_err("readv_ranges items must be tuples"))?;
            let start: u64 = tup.get_item(0)?.extract()?;
            let length: u64 = tup.get_item(1)?.extract()?;
            ranges.push((start, length));
        }
        self.inner
            .lock()
            .unwrap()
            .read_and_parse_for_test(ranges)
            .map_err(reraise_pending_pyerr_or)
    }

    /// Bisection probe used by `bisect_multi.bisect_multi_bytes`.
    /// `location_keys` is a list of `(byte_offset, key_tuple)` pairs;
    /// returns a list of `(input_pair, result)` matching the Python
    /// `_lookup_keys_via_location` contract (result is `False` for
    /// missing, `-1`/`+1` for direction, or
    /// `(self, key, value[, refs])` for found).
    fn _lookup_keys_via_location<'py>(
        slf: Bound<'py, Self>,
        py: Python<'py>,
        location_keys: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        let mut requested: Vec<(u64, IndexKey)> = Vec::new();
        for item_obj in location_keys.try_iter()? {
            let item = item_obj?;
            let tup = item
                .cast_into::<PyTuple>()
                .map_err(|_| PyTypeError::new_err("location_keys items must be tuples"))?;
            let location: u64 = tup.get_item(0)?.extract()?;
            let key = extract_key(&tup.get_item(1)?)?;
            requested.push((location, key));
        }
        let results = {
            let r = slf.borrow();
            let mut g = r.inner.lock().unwrap();
            g.lookup_keys_via_location(&requested)
                .map_err(|e| match e {
                    IndexError::Other(msg) => BzrFormatsError::new_err(msg),
                    other => reraise_pending_pyerr_or(other),
                })?
        };
        let node_ref_lists = {
            let r = slf.borrow();
            let mut g = r.inner.lock().unwrap();
            g.node_ref_lists().map_err(reraise_pending_pyerr_or)?
        };
        let out = PyList::empty(py);
        let self_any: Bound<PyAny> = slf.clone().into_any();
        for ((location, key), res) in results {
            let key_t = key_to_py(py, &key)?;
            let in_pair = PyTuple::new(
                py,
                [
                    location.into_pyobject(py)?.into_any(),
                    key_t.clone().into_any(),
                ],
            )?;
            let result_obj: Bound<'py, PyAny> = match res {
                bazaar::index::LookupResult::Missing => {
                    false.into_pyobject(py)?.to_owned().into_any()
                }
                bazaar::index::LookupResult::Direction(d) => {
                    (d as i32).into_pyobject(py)?.into_any().into_any()
                }
                bazaar::index::LookupResult::Found { value, refs } => {
                    let value_b = PyBytes::new(py, &value);
                    if node_ref_lists == 0 {
                        PyTuple::new(
                            py,
                            [
                                self_any.clone(),
                                key_t.clone().into_any(),
                                value_b.into_any(),
                            ],
                        )?
                        .into_any()
                    } else {
                        let mut ref_tuples: Vec<Bound<PyTuple>> = Vec::with_capacity(refs.len());
                        for inner in &refs {
                            let key_tuples: Vec<Bound<PyTuple>> = inner
                                .iter()
                                .map(|k| key_to_py(py, k))
                                .collect::<PyResult<_>>()?;
                            ref_tuples.push(PyTuple::new(py, key_tuples)?);
                        }
                        let refs_tuple = PyTuple::new(py, ref_tuples)?;
                        PyTuple::new(
                            py,
                            [
                                self_any.clone(),
                                key_t.clone().into_any(),
                                value_b.into_any(),
                                refs_tuple.into_any(),
                            ],
                        )?
                        .into_any()
                    }
                }
            };
            out.append(PyTuple::new(py, [in_pair.into_any(), result_obj])?)?;
        }
        Ok(out)
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
    let (header, nodes) = parse_full(data).map_err(reraise_pending_pyerr_or)?;
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

/// External references from a `GraphIndex._nodes` dict at a specific
/// reference-list index. Returns the set of keys reachable through
/// `ref_list_num` that aren't themselves present in the index.
///
/// `nodes` is `{key: (bytes, refs_tuple)}`. Raises `ValueError` if
/// `ref_list_num` is out of range for `node_ref_lists`.
#[pyfunction]
#[pyo3(name = "external_references_from_reader_nodes")]
fn py_external_references_from_reader_nodes<'py>(
    py: Python<'py>,
    nodes: Bound<'py, PyDict>,
    ref_list_num: usize,
    node_ref_lists: usize,
) -> PyResult<Bound<'py, pyo3::types::PySet>> {
    if ref_list_num + 1 > node_ref_lists {
        return Err(PyValueError::new_err(format!(
            "No ref list {}, index has {} ref lists",
            ref_list_num, node_ref_lists
        )));
    }
    let out = pyo3::types::PySet::empty(py)?;
    let mut present: std::collections::HashSet<Vec<Vec<u8>>> = std::collections::HashSet::new();
    let mut candidate_refs: Vec<Bound<'py, PyAny>> = Vec::new();
    for (key_obj, value_obj) in nodes.iter() {
        let key_tuple = key_obj
            .cast::<PyTuple>()
            .map_err(|_| PyTypeError::new_err("key must be a tuple"))?;
        present.insert(extract_key(key_tuple.as_any())?);
        let value_tuple = value_obj
            .cast_into::<PyTuple>()
            .map_err(|_| PyTypeError::new_err("node value must be a 2-tuple"))?;
        let refs_obj = value_tuple.get_item(1)?;
        let refs_tuple = refs_obj
            .cast::<PyTuple>()
            .map_err(|_| PyTypeError::new_err("refs must be a tuple"))?;
        let ref_list_obj = refs_tuple.get_item(ref_list_num)?;
        for r in ref_list_obj.try_iter()? {
            candidate_refs.push(r?);
        }
    }
    for ref_obj in candidate_refs {
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

/// Prepend `prefix` to each node's key (and to every reference key
/// when the node carries refs). Mirrors the inner loop of
/// `GraphIndexPrefixAdapter.add_nodes`. Returns the translated list
/// in the same shape as the input — `(key, value)` or
/// `(key, value, refs)`.
#[pyfunction]
#[pyo3(name = "prepend_prefix_nodes")]
fn py_prepend_prefix_nodes<'py>(
    py: Python<'py>,
    nodes: Bound<'py, PyAny>,
    prefix: Bound<'py, PyTuple>,
) -> PyResult<Bound<'py, PyList>> {
    let prefix_parts: Vec<Bound<'py, PyAny>> = (0..prefix.len())
        .map(|i| prefix.get_item(i))
        .collect::<PyResult<_>>()?;
    let out = PyList::empty(py);
    for node_obj in nodes.try_iter()? {
        let node = node_obj?;
        let node_tuple = node
            .cast::<PyTuple>()
            .map_err(|_| PyTypeError::new_err("node must be a tuple"))?
            .clone();
        if node_tuple.len() != 2 && node_tuple.len() != 3 {
            return Err(PyValueError::new_err("node must be a 2- or 3-tuple"));
        }
        let key = node_tuple
            .get_item(0)?
            .cast_into::<PyTuple>()
            .map_err(|_| PyTypeError::new_err("node key must be a tuple"))?;
        let value = node_tuple.get_item(1)?;
        let mut new_key_parts = prefix_parts.clone();
        for i in 0..key.len() {
            new_key_parts.push(key.get_item(i)?);
        }
        let new_key = PyTuple::new(py, new_key_parts)?;
        if node_tuple.len() == 3 {
            let refs_tuple = node_tuple
                .get_item(2)?
                .cast_into::<PyTuple>()
                .map_err(|_| PyTypeError::new_err("refs must be a tuple"))?;
            let mut new_lists: Vec<Bound<'py, PyTuple>> = Vec::with_capacity(refs_tuple.len());
            for list_idx in 0..refs_tuple.len() {
                let ref_list = refs_tuple
                    .get_item(list_idx)?
                    .cast_into::<PyTuple>()
                    .map_err(|_| PyTypeError::new_err("ref list must be a tuple"))?;
                let mut new_refs: Vec<Bound<'py, PyTuple>> = Vec::with_capacity(ref_list.len());
                for ref_idx in 0..ref_list.len() {
                    let ref_key = ref_list
                        .get_item(ref_idx)?
                        .cast_into::<PyTuple>()
                        .map_err(|_| PyTypeError::new_err("ref key must be a tuple"))?;
                    let mut new_ref_parts = prefix_parts.clone();
                    for i in 0..ref_key.len() {
                        new_ref_parts.push(ref_key.get_item(i)?);
                    }
                    new_refs.push(PyTuple::new(py, new_ref_parts)?);
                }
                new_lists.push(PyTuple::new(py, new_refs)?);
            }
            let new_refs_tuple = PyTuple::new(py, new_lists)?;
            out.append(PyTuple::new(
                py,
                [new_key.into_any(), value, new_refs_tuple.into_any()],
            )?)?;
        } else {
            out.append(PyTuple::new(py, [new_key.into_any(), value])?)?;
        }
    }
    Ok(out)
}

/// Strip a fixed key prefix from each node yielded by `nodes_iter`,
/// validating that every key (and every reference key) starts with
/// `prefix`. Yielded tuples preserve `node[0]` (the inner index),
/// strip the prefix from `node[1]` and from each ref-key, and pass
/// `node[2]` through unchanged. Raises `BadIndexData(adapter)` on
/// mismatch.
#[pyfunction]
#[pyo3(name = "strip_prefix_entries")]
fn py_strip_prefix_entries<'py>(
    py: Python<'py>,
    nodes_iter: Bound<'py, PyAny>,
    prefix: Bound<'py, PyTuple>,
    adapter: Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyList>> {
    let prefix_len = prefix.len();
    let prefix_parts: Vec<Bound<'py, PyAny>> = (0..prefix_len)
        .map(|i| prefix.get_item(i))
        .collect::<PyResult<_>>()?;
    let out = PyList::empty(py);
    for node_obj in nodes_iter.try_iter()? {
        let node = node_obj?;
        let node_tuple = node
            .cast::<PyTuple>()
            .map_err(|_| BadIndexData::new_err((adapter.clone().unbind(),)))?
            .clone();
        let inner_index = node_tuple.get_item(0)?;
        let key = node_tuple
            .get_item(1)?
            .cast_into::<PyTuple>()
            .map_err(|_| BadIndexData::new_err((adapter.clone().unbind(),)))?;
        if key.len() < prefix_len {
            return Err(BadIndexData::new_err((adapter.clone().unbind(),)));
        }
        for (i, p) in prefix_parts.iter().enumerate() {
            let key_part = key.get_item(i)?;
            if !key_part.eq(p)? {
                return Err(BadIndexData::new_err((adapter.clone().unbind(),)));
            }
        }
        let stripped_key_parts: Vec<Bound<'py, PyAny>> = (prefix_len..key.len())
            .map(|i| key.get_item(i))
            .collect::<PyResult<_>>()?;
        let stripped_key = PyTuple::new(py, stripped_key_parts)?;
        let value = node_tuple.get_item(2)?;
        let stripped_refs = if node_tuple.len() >= 4 {
            let refs_tuple = node_tuple
                .get_item(3)?
                .cast_into::<PyTuple>()
                .map_err(|_| BadIndexData::new_err((adapter.clone().unbind(),)))?;
            let mut new_lists: Vec<Bound<'py, PyTuple>> = Vec::with_capacity(refs_tuple.len());
            for ref_list_idx in 0..refs_tuple.len() {
                let ref_list = refs_tuple
                    .get_item(ref_list_idx)?
                    .cast_into::<PyTuple>()
                    .map_err(|_| BadIndexData::new_err((adapter.clone().unbind(),)))?;
                let mut new_refs: Vec<Bound<'py, PyTuple>> = Vec::with_capacity(ref_list.len());
                for ref_idx in 0..ref_list.len() {
                    let ref_key = ref_list
                        .get_item(ref_idx)?
                        .cast_into::<PyTuple>()
                        .map_err(|_| BadIndexData::new_err((adapter.clone().unbind(),)))?;
                    if ref_key.len() < prefix_len {
                        return Err(BadIndexData::new_err((adapter.clone().unbind(),)));
                    }
                    for (i, p) in prefix_parts.iter().enumerate() {
                        let part = ref_key.get_item(i)?;
                        if !part.eq(p)? {
                            return Err(BadIndexData::new_err((adapter.clone().unbind(),)));
                        }
                    }
                    let stripped_ref_parts: Vec<Bound<'py, PyAny>> = (prefix_len..ref_key.len())
                        .map(|i| ref_key.get_item(i))
                        .collect::<PyResult<_>>()?;
                    new_refs.push(PyTuple::new(py, stripped_ref_parts)?);
                }
                new_lists.push(PyTuple::new(py, new_refs)?);
            }
            Some(PyTuple::new(py, new_lists)?)
        } else {
            None
        };
        if let Some(refs) = stripped_refs {
            out.append(PyTuple::new(
                py,
                [inner_index, stripped_key.into_any(), value, refs.into_any()],
            )?)?;
        } else {
            out.append(PyTuple::new(
                py,
                [inner_index, stripped_key.into_any(), value],
            )?)?;
        }
    }
    Ok(out)
}

/// Sort and emit a `BTreeBuilder`-shaped `_nodes` dict
/// (`{key: (refs, value)}`). Returns a list of `(key, value)` or
/// `(key, value, refs)` tuples sorted by key — the caller prepends
/// `self`.
#[pyfunction]
#[pyo3(name = "iter_btree_builder_nodes_sorted")]
fn py_iter_btree_builder_nodes_sorted<'py>(
    py: Python<'py>,
    nodes: Bound<'py, PyDict>,
    has_refs: bool,
) -> PyResult<Bound<'py, PyList>> {
    let mut sortable: Vec<(IndexKey, Bound<'py, PyAny>, Bound<'py, PyAny>)> =
        Vec::with_capacity(nodes.len());
    for (key_obj, value_obj) in nodes.iter() {
        let key_tuple = key_obj
            .cast::<PyTuple>()
            .map_err(|_| PyTypeError::new_err("key must be a tuple"))?;
        let key_rs = extract_key(key_tuple.as_any())?;
        let value_tuple = value_obj
            .cast_into::<PyTuple>()
            .map_err(|_| PyTypeError::new_err("btree node must be a 2-tuple"))?;
        let refs_obj = value_tuple.get_item(0)?;
        let value_b = value_tuple.get_item(1)?;
        sortable.push((key_rs, refs_obj, value_b));
    }
    sortable.sort_by(|a, b| a.0.cmp(&b.0));
    let out = PyList::empty(py);
    for (key_rs, refs_obj, value_b) in sortable {
        let key_t = key_to_py(py, &key_rs)?;
        if has_refs {
            out.append(PyTuple::new(py, [key_t.into_any(), value_b, refs_obj])?)?;
        } else {
            out.append(PyTuple::new(py, [key_t.into_any(), value_b])?)?;
        }
    }
    Ok(out)
}

/// Iterate all present entries in a `GraphIndexBuilder`-shaped
/// `_nodes` dict (`{key: (absent, refs, value)}`). Skips absent
/// entries. Returns a list of `(key, value)` or `(key, value, refs)`
/// tuples; the caller prepends `self`.
#[pyfunction]
#[pyo3(name = "iter_builder_nodes")]
fn py_iter_builder_nodes<'py>(
    py: Python<'py>,
    nodes: Bound<'py, PyDict>,
    has_refs: bool,
) -> PyResult<Bound<'py, PyList>> {
    let out = PyList::empty(py);
    for (key_obj, value_obj) in nodes.iter() {
        let value_tuple = value_obj
            .cast_into::<PyTuple>()
            .map_err(|_| PyTypeError::new_err("builder node must be a 3-tuple"))?;
        let absent_bytes = value_tuple
            .get_item(0)?
            .cast_into::<PyBytes>()
            .map_err(|_| PyTypeError::new_err("absent marker must be bytes"))?;
        if absent_bytes.as_bytes() == b"a" {
            continue;
        }
        let refs_obj = value_tuple.get_item(1)?;
        let value_b = value_tuple.get_item(2)?;
        if has_refs {
            out.append(PyTuple::new(py, [key_obj.clone(), value_b, refs_obj])?)?;
        } else {
            out.append(PyTuple::new(py, [key_obj.clone(), value_b])?)?;
        }
    }
    Ok(out)
}

/// Iterate present entries in a builder-shaped `_nodes` dict that
/// match one of the requested `keys`. Same return shape as
/// `iter_builder_nodes`.
#[pyfunction]
#[pyo3(name = "iter_builder_nodes_for_keys")]
fn py_iter_builder_nodes_for_keys<'py>(
    py: Python<'py>,
    nodes: Bound<'py, PyDict>,
    keys: Bound<'py, PyAny>,
    has_refs: bool,
) -> PyResult<Bound<'py, PyList>> {
    let out = PyList::empty(py);
    for key_obj in keys.try_iter()? {
        let key_obj = key_obj?;
        let Some(value_obj) = nodes.get_item(key_obj.clone())? else {
            continue;
        };
        let value_tuple = value_obj
            .cast_into::<PyTuple>()
            .map_err(|_| PyTypeError::new_err("builder node must be a 3-tuple"))?;
        let absent_bytes = value_tuple
            .get_item(0)?
            .cast_into::<PyBytes>()
            .map_err(|_| PyTypeError::new_err("absent marker must be bytes"))?;
        if absent_bytes.as_bytes() == b"a" {
            continue;
        }
        let refs_obj = value_tuple.get_item(1)?;
        let value_b = value_tuple.get_item(2)?;
        if has_refs {
            out.append(PyTuple::new(py, [key_obj, value_b, refs_obj])?)?;
        } else {
            out.append(PyTuple::new(py, [key_obj, value_b])?)?;
        }
    }
    Ok(out)
}

/// Insert a node into a `BTreeBuilder`-shaped `_nodes` dict
/// (`{key: (refs, value)}`). Performs the per-add validation +
/// duplicate-key check + dict insertion in a single Rust call.
///
/// Raises `BadIndexDuplicateKey(key, builder)` if `key` is already
/// present.
#[pyfunction]
#[pyo3(name = "add_node_to_btree_builder")]
fn py_add_node_to_btree_builder<'py>(
    py: Python<'py>,
    builder: Bound<'py, PyAny>,
    key: Bound<'py, PyAny>,
    value: Bound<'py, PyBytes>,
    references: Bound<'py, PyAny>,
    nodes: Bound<'py, PyDict>,
    reference_lists_count: usize,
    key_length: usize,
) -> PyResult<Bound<'py, PyTuple>> {
    let (node_refs, _absent) = py_check_key_ref_value(
        py,
        key.clone(),
        references,
        value.clone(),
        nodes.clone(),
        reference_lists_count,
        key_length,
    )?;
    if nodes.contains(key.clone())? {
        return Err(BadIndexDuplicateKey::new_err((
            key.unbind(),
            builder.unbind(),
        )));
    }
    let pair = PyTuple::new(py, [node_refs.clone().into_any(), value.into_any()])?;
    nodes.set_item(key, pair)?;
    Ok(node_refs)
}

/// Insert a node into a `GraphIndexBuilder`-shaped state. Folds the
/// per-node check_key_ref_value + duplicate check + dict updates from
/// `add_node` into a single Rust call.
///
/// `nodes` is the builder's `_nodes` dict (mutated in place).
/// `absent_keys` is the `_absent_keys` set (mutated in place).
/// `builder` is the Python builder instance, only used so that
/// `BadIndexDuplicateKey(key, builder)` carries the right context.
#[pyfunction]
#[pyo3(name = "add_node_to_builder")]
fn py_add_node_to_builder<'py>(
    py: Python<'py>,
    builder: Bound<'py, PyAny>,
    key: Bound<'py, PyAny>,
    value: Bound<'py, PyBytes>,
    references: Bound<'py, PyAny>,
    nodes: Bound<'py, PyDict>,
    absent_keys: Bound<'py, pyo3::types::PySet>,
    reference_lists_count: usize,
    key_length: usize,
) -> PyResult<()> {
    let (node_refs, absent_references) = py_check_key_ref_value(
        py,
        key.clone(),
        references,
        value.clone(),
        nodes.clone(),
        reference_lists_count,
        key_length,
    )?;
    if let Some(existing) = nodes.get_item(key.clone())? {
        let existing_tuple = existing
            .cast_into::<PyTuple>()
            .map_err(|_| PyTypeError::new_err("nodes value must be a tuple"))?;
        let absent_marker = existing_tuple
            .get_item(0)?
            .cast_into::<PyBytes>()
            .map_err(|_| PyTypeError::new_err("absent marker must be bytes"))?;
        if absent_marker.as_bytes() != b"a" {
            return Err(BadIndexDuplicateKey::new_err((
                key.unbind(),
                builder.unbind(),
            )));
        }
    }
    let empty_tuple = PyTuple::empty(py);
    let absent_value = PyTuple::new(
        py,
        [
            PyBytes::new(py, b"a").into_any(),
            empty_tuple.clone().into_any(),
            PyBytes::new(py, b"").into_any(),
        ],
    )?;
    for ref_obj in absent_references.iter() {
        nodes.set_item(ref_obj.clone(), absent_value.clone())?;
        absent_keys.add(ref_obj)?;
    }
    absent_keys.discard(key.clone())?;
    let present_value = PyTuple::new(
        py,
        [
            PyBytes::new(py, b"").into_any(),
            node_refs.into_any(),
            value.into_any(),
        ],
    )?;
    nodes.set_item(key, present_value)?;
    Ok(())
}

/// Validate `key`, `references`, and `value` for a builder
/// `add_node` call. Returns `(node_refs_tuple, absent_references_list)`
/// where `node_refs_tuple` is a tuple of tuples of tuples (each inner
/// tuple is a key) and `absent_references_list` is a list of keys that
/// aren't already present in `nodes`.
///
/// Raises `BadIndexKey` for bad keys, `BadIndexValue` for bad values
/// or wrong reference list count.
#[pyfunction]
#[pyo3(name = "check_key_ref_value")]
fn py_check_key_ref_value<'py>(
    py: Python<'py>,
    key: Bound<'py, PyAny>,
    references: Bound<'py, PyAny>,
    value: Bound<'py, PyBytes>,
    nodes: Bound<'py, PyDict>,
    reference_lists_count: usize,
    key_length: usize,
) -> PyResult<(Bound<'py, PyTuple>, Bound<'py, PyList>)> {
    py_check_key(key.clone(), key_length)?;
    py_check_value(value.clone())?;
    let ref_lists: Vec<Bound<'py, PyAny>> = references.try_iter()?.collect::<PyResult<Vec<_>>>()?;
    if ref_lists.len() != reference_lists_count {
        return Err(BadIndexValue::new_err((references.unbind(),)));
    }
    let absent_list = PyList::empty(py);
    let mut node_ref_tuples: Vec<Bound<'py, PyTuple>> = Vec::with_capacity(ref_lists.len());
    for ref_list_obj in ref_lists {
        let mut tupled_refs: Vec<Bound<'py, PyTuple>> = Vec::new();
        for ref_obj in ref_list_obj.try_iter()? {
            let ref_obj = ref_obj?;
            let ref_tuple = if let Ok(t) = ref_obj.cast::<PyTuple>() {
                t.clone()
            } else {
                let parts: Vec<Bound<'py, PyAny>> =
                    ref_obj.try_iter()?.collect::<PyResult<Vec<_>>>()?;
                PyTuple::new(py, parts)?
            };
            if !nodes.contains(ref_tuple.clone())? {
                py_check_key(ref_tuple.clone().into_any(), key_length)?;
                absent_list.append(ref_tuple.clone())?;
            }
            tupled_refs.push(ref_tuple);
        }
        node_ref_tuples.push(PyTuple::new(py, tupled_refs)?);
    }
    let result_tuple = PyTuple::new(py, node_ref_tuples)?;
    Ok((result_tuple, absent_list))
}

/// Validate that `key` conforms to the `GraphIndexBuilder` key
/// interface: a tuple of `key_length` non-empty `bytes` elements with
/// no whitespace or null characters anywhere. Raises `BadIndexKey` on
/// failure.
#[pyfunction]
#[pyo3(name = "check_key")]
fn py_check_key(key: Bound<'_, PyAny>, key_length: usize) -> PyResult<()> {
    let key_tuple = key
        .cast::<PyTuple>()
        .map_err(|_| BadIndexKey::new_err((key.clone().unbind(),)))?
        .clone();
    if key_tuple.len() != key_length {
        return Err(BadIndexKey::new_err((key_tuple.unbind(),)));
    }
    let mut parts: Vec<Vec<u8>> = Vec::with_capacity(key_length);
    for item in key_tuple.iter() {
        let b = item
            .cast_into::<PyBytes>()
            .map_err(|_| BadIndexKey::new_err((key_tuple.clone().unbind(),)))?;
        parts.push(b.as_bytes().to_vec());
    }
    if !key_is_valid(&parts, key_length) {
        return Err(BadIndexKey::new_err((key_tuple.unbind(),)));
    }
    Ok(())
}

/// Validate that `value` may legally appear as a node payload: no `\n`
/// or `\0` bytes. Raises `BadIndexValue` on failure.
#[pyfunction]
#[pyo3(name = "check_value")]
fn py_check_value(value: Bound<'_, PyBytes>) -> PyResult<()> {
    if !value_is_valid(value.as_bytes()) {
        return Err(BadIndexValue::new_err((value.unbind(),)));
    }
    Ok(())
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
    m.add_function(wrap_pyfunction!(py_check_key, &m)?)?;
    m.add_function(wrap_pyfunction!(py_check_value, &m)?)?;
    m.add_function(wrap_pyfunction!(py_check_key_ref_value, &m)?)?;
    m.add_function(wrap_pyfunction!(py_add_node_to_builder, &m)?)?;
    m.add_function(wrap_pyfunction!(py_add_node_to_btree_builder, &m)?)?;
    m.add_function(wrap_pyfunction!(py_iter_builder_nodes, &m)?)?;
    m.add_function(wrap_pyfunction!(py_iter_btree_builder_nodes_sorted, &m)?)?;
    m.add_function(wrap_pyfunction!(py_iter_builder_nodes_for_keys, &m)?)?;
    m.add_function(wrap_pyfunction!(py_strip_prefix_entries, &m)?)?;
    m.add_function(wrap_pyfunction!(py_prepend_prefix_nodes, &m)?)?;
    m.add_function(wrap_pyfunction!(
        py_external_references_from_reader_nodes,
        &m
    )?)?;
    m.add_class::<PyGraphIndex>()?;
    m.add_class::<PyParsedRangeMap>()?;
    Ok(m)
}
