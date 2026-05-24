use bazaar::btree_builder::spill_landing_slot;
use bazaar::btree_index::{
    parse_btree_header, parse_internal_node, BTreeGraphIndex as RsBTreeGraphIndex, BTreeHeader,
    BTreeIndexError, InternalNode, LeafKey,
};
use bazaar::index::{IndexError, IndexTransport};
use pyo3::class::basic::CompareOp;
use pyo3::exceptions::{PyNotImplementedError, PyStopIteration, PyValueError};
use pyo3::import_exception;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};
use std::sync::Mutex;

use crate::index::PyGraphIndexBuilder;

import_exception!(bzrformats.index, BadIndexFormatSignature);
import_exception!(bzrformats.index, BadIndexOptions);
import_exception!(bzrformats.index, BadIndexDuplicateKey);

fn header_err_to_py(err: BTreeIndexError) -> PyErr {
    match err {
        BTreeIndexError::BadSignature => BadIndexFormatSignature::new_err(("", "BTreeGraphIndex")),
        BTreeIndexError::BadOptions => BadIndexOptions::new_err(("",)),
        BTreeIndexError::BadInternalNode => {
            pyo3::exceptions::PyValueError::new_err(err.to_string())
        }
    }
}

/// Parse a B+Tree graph index header. Returns
/// `(node_ref_lists, key_length, key_count, row_lengths, header_end)`.
#[pyfunction]
#[pyo3(name = "parse_btree_header")]
fn py_parse_btree_header<'py>(
    py: Python<'py>,
    data: &[u8],
) -> PyResult<(usize, usize, usize, Bound<'py, PyList>, usize)> {
    let BTreeHeader {
        node_ref_lists,
        key_length,
        key_count,
        row_lengths,
        header_end,
    } = parse_btree_header(data).map_err(header_err_to_py)?;
    let rl = PyList::empty(py);
    for n in &row_lengths {
        rl.append(*n)?;
    }
    Ok((node_ref_lists, key_length, key_count, rl, header_end))
}

/// Parse an internal-node body into `(offset, keys)` where `keys` is a list
/// of tuples of bytes matching what `_InternalNode.keys` stores.
#[pyfunction]
#[pyo3(name = "parse_internal_node")]
fn py_parse_internal_node<'py>(
    py: Python<'py>,
    body: &[u8],
) -> PyResult<(usize, Bound<'py, PyList>)> {
    let InternalNode { offset, keys } = parse_internal_node(body).map_err(header_err_to_py)?;
    let py_keys = PyList::empty(py);
    for key in &keys {
        let parts: Vec<Bound<PyBytes>> = key.iter().map(|e| PyBytes::new(py, e)).collect();
        py_keys.append(PyTuple::new(py, parts)?)?;
    }
    Ok((offset, py_keys))
}

// ---------------------------------------------------------------------------
// PyIndexTransport adapter — same shape as the index.rs adapter, copied
// here to avoid a cross-module pub leak.
// ---------------------------------------------------------------------------

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
    static PENDING_PY_ERR: std::cell::RefCell<Option<PyErr>> =
        const { std::cell::RefCell::new(None) };
}

fn stash_py_err(err: PyErr) -> IndexError {
    let msg = err.to_string();
    PENDING_PY_ERR.with(|c| *c.borrow_mut() = Some(err));
    IndexError::Other(format!("__pyerr__: {msg}"))
}

fn reraise_pending_or(err: IndexError) -> PyErr {
    if let Some(stashed) = PENDING_PY_ERR.with(|c| c.borrow_mut().take()) {
        return stashed;
    }
    PyValueError::new_err(format!("{:?}", err))
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

    fn recommended_page_size(&self) -> u64 {
        Python::attach(|py| {
            self.obj
                .bind(py)
                .call_method0("recommended_page_size")
                .ok()
                .and_then(|r| r.extract::<u64>().ok())
                .unwrap_or(64 * 1024)
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
            let py_list =
                PyList::new(py, py_ranges).map_err(|e| IndexError::Other(e.to_string()))?;
            let kwargs = PyDict::new(py);
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
                    .map_err(|_| IndexError::Other("readv did not yield tuples".to_string()))?;
                if tup.len() != 2 {
                    return Err(IndexError::Other(
                        "readv yielded tuple of wrong arity".to_string(),
                    ));
                }
                let offset: u64 = tup
                    .get_item(0)
                    .map_err(stash_py_err)?
                    .extract()
                    .map_err(stash_py_err)?;
                let data: Vec<u8> = tup
                    .get_item(1)
                    .map_err(stash_py_err)?
                    .cast_into::<PyBytes>()
                    .map_err(|_| IndexError::Other("readv data not bytes".to_string()))?
                    .as_bytes()
                    .to_vec();
                out.push((offset, data));
            }
            Ok(out)
        })
    }
}

// ---------------------------------------------------------------------------
// PyO3 BTreeGraphIndex
// ---------------------------------------------------------------------------

fn extract_keys(keys: &Bound<PyAny>) -> PyResult<Vec<LeafKey>> {
    let mut out = Vec::new();
    for key in keys.try_iter()? {
        let key = key?;
        let tuple = key.cast_into::<PyTuple>()?;
        let mut parts = Vec::new();
        for item in tuple.iter() {
            let bytes = item.cast_into::<PyBytes>()?;
            parts.push(bytes.as_bytes().to_vec());
        }
        out.push(parts);
    }
    Ok(out)
}

fn extract_prefixes(keys: &Bound<PyAny>) -> PyResult<Vec<Vec<Option<Vec<u8>>>>> {
    let mut out = Vec::new();
    for prefix in keys.try_iter()? {
        let prefix = prefix?;
        let tuple = prefix.cast_into::<PyTuple>()?;
        let mut parts = Vec::new();
        for item in tuple.iter() {
            if item.is_none() {
                parts.push(None);
            } else {
                let bytes = item.cast_into::<PyBytes>()?;
                parts.push(Some(bytes.as_bytes().to_vec()));
            }
        }
        out.push(parts);
    }
    Ok(out)
}

fn key_to_py<'py>(py: Python<'py>, key: &LeafKey) -> PyResult<Bound<'py, PyTuple>> {
    let parts: Vec<Bound<PyBytes>> = key.iter().map(|p| PyBytes::new(py, p)).collect();
    PyTuple::new(py, parts)
}

fn refs_to_py<'py>(py: Python<'py>, refs: &[Vec<LeafKey>]) -> PyResult<Bound<'py, PyTuple>> {
    let mut lists: Vec<Bound<PyTuple>> = Vec::with_capacity(refs.len());
    for ref_list in refs {
        let mut keys: Vec<Bound<PyTuple>> = Vec::with_capacity(ref_list.len());
        for k in ref_list {
            keys.push(key_to_py(py, k)?);
        }
        lists.push(PyTuple::new(py, keys)?);
    }
    PyTuple::new(py, lists)
}

/// Wraps `bazaar::btree_index::BTreeGraphIndex` for Python callers.
///
/// All blocking transport reads happen behind `Python::detach` so the GIL
/// is released during IO. The class is `Send + Sync` via the Mutex.
#[pyclass(module = "bzrformats._bzr_rs.btree_index")]
struct BTreeGraphIndex {
    inner: Mutex<RsBTreeGraphIndex<PyIndexTransport>>,
    transport: Py<PyAny>,
    name: String,
    base_offset: u64,
}

#[pymethods]
impl BTreeGraphIndex {
    #[new]
    #[pyo3(signature = (transport, name, size, unlimited_cache = false, offset = 0))]
    fn new(
        py: Python<'_>,
        transport: Py<PyAny>,
        name: String,
        size: Option<u64>,
        unlimited_cache: bool,
        offset: u64,
    ) -> Self {
        let pt = PyIndexTransport {
            obj: transport.clone_ref(py),
        };
        let inner = if unlimited_cache {
            RsBTreeGraphIndex::new_unlimited_cache(pt, name.clone(), size, offset)
        } else {
            RsBTreeGraphIndex::new(pt, name.clone(), size, offset)
        };
        Self {
            inner: Mutex::new(inner),
            transport,
            name,
            base_offset: offset,
        }
    }

    // -------- attribute getters --------

    #[getter]
    fn _name(&self) -> &str {
        &self.name
    }

    #[getter]
    fn _transport(&self, py: Python<'_>) -> Py<PyAny> {
        self.transport.clone_ref(py)
    }

    #[getter]
    fn _base_offset(&self) -> u64 {
        self.base_offset
    }

    #[getter]
    fn _size(&self) -> Option<u64> {
        self.inner.lock().unwrap().size()
    }

    #[setter(_size)]
    fn set_size(&self, value: Option<u64>) {
        // Tests use `index._size = None` to throw away the size info and
        // force a full read.
        let mut guard = self.inner.lock().unwrap();
        // Replace inner with a fresh index that has the same transport
        // but a new size. We rebuild because the field isn't pub.
        let transport = PyIndexTransport {
            obj: Python::attach(|py| self.transport.clone_ref(py)),
        };
        let mut new = RsBTreeGraphIndex::new(transport, self.name.clone(), value, self.base_offset);
        // Preserve the parsed header / root if present so we don't lose
        // state on a `_size = None` mutation right after a previous read.
        std::mem::swap(&mut *guard, &mut new);
        // Drop `new` (the old state). The new index above starts empty,
        // matching what tests expect when they nuke the size.
        let _ = new;
    }

    #[getter]
    fn node_ref_lists(&self) -> PyResult<usize> {
        self.inner
            .lock()
            .unwrap()
            .node_ref_lists()
            .ok_or_else(|| PyValueError::new_err("index header not yet parsed"))
    }

    #[getter]
    fn _key_length(&self) -> Option<usize> {
        self.inner.lock().unwrap().key_length()
    }

    #[getter]
    fn _key_count(&self, py: Python<'_>) -> PyResult<Option<usize>> {
        // Lazy: reading key_count here would trigger transport IO; tests
        // that probe `_key_count` after constructing the index expect
        // None until something has been read.
        let _ = py;
        Ok(self.inner.lock().unwrap().node_ref_lists().and_then(|_| {
            // If the header has been parsed, key_count is set. Use a
            // private accessor exposed via key_length presence.
            self.inner.lock().unwrap().key_length().map(|_| {
                // We need a separate accessor. For now route through
                // key_count() which returns Result but won't trigger IO
                // if the value's already cached.
                self.inner
                    .lock()
                    .unwrap()
                    .key_count()
                    .ok()
                    .unwrap_or_default()
            })
        }))
    }

    #[getter]
    fn _row_lengths<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyList>>> {
        let guard = self.inner.lock().unwrap();
        if let Some(rl) = guard.row_lengths() {
            let l = PyList::empty(py);
            for n in rl {
                l.append(*n)?;
            }
            Ok(Some(l))
        } else {
            Ok(None)
        }
    }

    #[getter]
    fn _row_offsets<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyList>>> {
        let guard = self.inner.lock().unwrap();
        if let Some(ro) = guard.row_offsets() {
            let l = PyList::empty(py);
            for n in ro {
                l.append(*n)?;
            }
            Ok(Some(l))
        } else {
            Ok(None)
        }
    }

    // -------- equality / sort --------

    fn __hash__(slf: PyRef<'_, Self>) -> usize {
        // Match the Python implementation: hash is identity-based.
        slf.as_ptr() as usize
    }

    fn __richcmp__(
        &self,
        other: &Bound<'_, PyAny>,
        op: CompareOp,
        py: Python<'_>,
    ) -> PyResult<Py<PyAny>> {
        match op {
            CompareOp::Eq | CompareOp::Ne => {
                let same = if let Ok(other) = other.extract::<PyRef<BTreeGraphIndex>>() {
                    let same_transport = self.transport.bind(py).eq(other.transport.bind(py))?;
                    same_transport && self.name == other.name && self._size() == other._size()
                } else {
                    false
                };
                let result = if matches!(op, CompareOp::Eq) {
                    same
                } else {
                    !same
                };
                Ok(result.into_pyobject(py)?.to_owned().into_any().unbind())
            }
            CompareOp::Lt => {
                if let Ok(other) = other.extract::<PyRef<BTreeGraphIndex>>() {
                    let lt =
                        (self.name.clone(), self._size()) < (other.name.clone(), other._size());
                    Ok(lt.into_pyobject(py)?.to_owned().into_any().unbind())
                } else {
                    Err(PyTypeError::new_err("cannot compare"))
                }
            }
            _ => Err(PyNotImplementedError::new_err("comparison not supported")),
        }
    }

    // -------- public API --------

    fn clear_cache(&self) {
        self.inner.lock().unwrap().clear_cache();
    }

    fn key_count(&self, py: Python<'_>) -> PyResult<usize> {
        py.detach(|| self.inner.lock().unwrap().key_count())
            .map_err(reraise_pending_or)
    }

    fn validate(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.inner.lock().unwrap().validate())
            .map_err(reraise_pending_or)
    }

    fn external_references<'py>(
        &self,
        py: Python<'py>,
        ref_list_num: usize,
    ) -> PyResult<Vec<Bound<'py, PyTuple>>> {
        let refs = py
            .detach(|| self.inner.lock().unwrap().external_references(ref_list_num))
            .map_err(reraise_pending_or)?;
        refs.iter()
            .map(|k| key_to_py(py, k))
            .collect::<PyResult<Vec<_>>>()
    }

    fn iter_all_entries<'py>(
        slf: Py<Self>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, EntryIterator>> {
        let entries = {
            let bound = slf.bind(py);
            let s = bound.borrow();
            let inner = &s.inner;
            py.detach(|| inner.lock().unwrap().iter_all_entries())
                .map_err(reraise_pending_or)?
        };
        let nrl = slf
            .bind(py)
            .borrow()
            .inner
            .lock()
            .unwrap()
            .node_ref_lists()
            .unwrap_or(0);
        Bound::new(
            py,
            EntryIterator {
                index: slf.clone_ref(py),
                entries,
                pos: 0,
                node_ref_lists: nrl,
            },
        )
    }

    fn iter_entries<'py>(
        slf: Py<Self>,
        py: Python<'py>,
        keys: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, EntryIterator>> {
        let key_vec = extract_keys(&keys)?;
        let entries = {
            let bound = slf.bind(py);
            let s = bound.borrow();
            let inner = &s.inner;
            py.detach(|| inner.lock().unwrap().iter_entries(&key_vec))
                .map_err(reraise_pending_or)?
        };
        let nrl = slf
            .bind(py)
            .borrow()
            .inner
            .lock()
            .unwrap()
            .node_ref_lists()
            .unwrap_or(0);
        Bound::new(
            py,
            EntryIterator {
                index: slf.clone_ref(py),
                entries,
                pos: 0,
                node_ref_lists: nrl,
            },
        )
    }

    fn iter_entries_prefix<'py>(
        slf: Py<Self>,
        py: Python<'py>,
        keys: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, EntryIterator>> {
        let prefix_vec = extract_prefixes(&keys)?;
        let entries = {
            let bound = slf.bind(py);
            let s = bound.borrow();
            let inner = &s.inner;
            py.detach(|| inner.lock().unwrap().iter_entries_prefix(&prefix_vec))
                .map_err(reraise_pending_or)?
        };
        let nrl = slf
            .bind(py)
            .borrow()
            .inner
            .lock()
            .unwrap()
            .node_ref_lists()
            .unwrap_or(0);
        Bound::new(
            py,
            EntryIterator {
                index: slf.clone_ref(py),
                entries,
                pos: 0,
                node_ref_lists: nrl,
            },
        )
    }

    fn _find_ancestors<'py>(
        &self,
        py: Python<'py>,
        keys: Bound<'py, PyAny>,
        ref_list_num: usize,
        parent_map: Bound<'py, PyDict>,
        missing_keys: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, pyo3::types::PySet>> {
        let key_vec = extract_keys(&keys)?;
        // Pull existing parent_map / missing_keys into Rust.
        let mut rs_parent_map: std::collections::HashMap<LeafKey, Vec<LeafKey>> =
            std::collections::HashMap::new();
        for (k, v) in parent_map.iter() {
            let k_vec = extract_keys(&PyList::new(py, [k])?.into_any())?
                .pop()
                .unwrap_or_default();
            let v_list = v.cast_into::<PyAny>()?;
            let parent_keys = extract_keys(&v_list)?;
            rs_parent_map.insert(k_vec, parent_keys);
        }
        let mut rs_missing_keys: std::collections::HashSet<LeafKey> =
            std::collections::HashSet::new();
        for item in missing_keys.try_iter()? {
            let item = item?;
            let k_vec = extract_keys(&PyList::new(py, [item])?.into_any())?
                .pop()
                .unwrap_or_default();
            rs_missing_keys.insert(k_vec);
        }
        let search = py
            .detach(|| {
                self.inner.lock().unwrap().find_ancestors(
                    &key_vec,
                    ref_list_num,
                    &mut rs_parent_map,
                    &mut rs_missing_keys,
                )
            })
            .map_err(reraise_pending_or)?;
        // Push back into Python.
        for (k, v) in &rs_parent_map {
            let kt = key_to_py(py, k)?;
            let vt: Vec<Bound<PyTuple>> = v
                .iter()
                .map(|x| key_to_py(py, x))
                .collect::<PyResult<_>>()?;
            parent_map.set_item(kt, PyTuple::new(py, vt)?)?;
        }
        for k in &rs_missing_keys {
            let kt = key_to_py(py, k)?;
            missing_keys.call_method1("add", (kt,))?;
        }
        let result = pyo3::types::PySet::empty(py)?;
        for k in &search {
            let kt = key_to_py(py, k)?;
            result.add(kt)?;
        }
        Ok(result)
    }
}

use pyo3::exceptions::PyTypeError;

/// Iterator that yields `(self, key, value)` or `(self, key, value, refs)`
/// tuples — matches the Python BTreeGraphIndex iterators' shape.
#[pyclass(module = "bzrformats._bzr_rs.btree_index")]
struct EntryIterator {
    index: Py<BTreeGraphIndex>,
    entries: Vec<(LeafKey, Vec<u8>, Vec<Vec<LeafKey>>)>,
    pos: usize,
    node_ref_lists: usize,
}

#[pymethods]
impl EntryIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(
        mut slf: PyRefMut<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        if slf.pos >= slf.entries.len() {
            return Err(PyStopIteration::new_err(()));
        }
        let (k, v, r) = slf.entries[slf.pos].clone();
        slf.pos += 1;
        let key = key_to_py(py, &k)?;
        let value = PyBytes::new(py, &v);
        if slf.node_ref_lists > 0 {
            let refs = refs_to_py(py, &r)?;
            PyTuple::new(
                py,
                [
                    slf.index.bind(py).clone().into_any(),
                    key.into_any(),
                    value.into_any(),
                    refs.into_any(),
                ],
            )
        } else {
            PyTuple::new(
                py,
                [
                    slf.index.bind(py).clone().into_any(),
                    key.into_any(),
                    value.into_any(),
                ],
            )
        }
    }
}

/// B+Tree builder. Extends [`PyGraphIndexBuilder`] and adds
/// spill-to-disk semantics: once the in-memory node dict crosses
/// `spill_at` entries, the held nodes are serialised into a temporary
/// file and tracked as a "backing index". On every subsequent spill,
/// the power-of-2 merge strategy from
/// [`bazaar::btree_builder::spill_landing_slot`] decides which slot
/// the new merged blob lands in.
///
/// Why bindings code rather than pure crate: the spill output is a
/// Python `tempfile.NamedTemporaryFile` (or `BytesIO`), the backing
/// index objects are pyo3 [`BTreeGraphIndex`] instances reading
/// directly from those Python file handles, and querying a backing
/// index goes through Python attribute/method calls. The orchestration
/// is fundamentally over Python objects.
#[pyclass(
    module = "bzrformats._bzr_rs.btree_index",
    name = "BTreeBuilder",
    extends = PyGraphIndexBuilder,
    subclass,
    dict
)]
struct BTreeBuilder {
    spill_at: Mutex<usize>,
    /// `_backing_indices`. Each slot is either a `BTreeGraphIndex` (or
    /// `Py<PyAny>` to match the Python contract that other index types
    /// can be stored) or `None`.
    backing_indices: Mutex<Vec<Option<Py<PyAny>>>>,
    /// `_nodes`: `{key_tuple: (refs_tuple, value_bytes)}`. Held as a
    /// Python dict because the helper `add_node_to_btree_builder` and
    /// `iter_btree_builder_nodes_sorted` expect that exact shape.
    nodes: Mutex<Py<PyDict>>,
    /// `_nodes_by_key`: lazy `{first_segment: {second_segment: ...
    /// {last_segment: (key, value[, refs])}}}` trie. `None` until
    /// `_get_nodes_by_key` materialises it.
    nodes_by_key: Mutex<Option<Py<PyDict>>>,
}

#[pymethods]
impl BTreeBuilder {
    #[new]
    #[pyo3(signature = (reference_lists = 0, key_elements = 1, spill_at = 100000))]
    fn new(
        py: Python<'_>,
        reference_lists: usize,
        key_elements: usize,
        spill_at: usize,
    ) -> (Self, PyGraphIndexBuilder) {
        use bazaar::index::GraphIndexBuilder as RsGraphIndexBuilder;
        let parent = PyGraphIndexBuilder {
            inner: Mutex::new(RsGraphIndexBuilder::new(reference_lists, key_elements)),
            optimize_for_size_py: Mutex::new(None),
            combine_backing_indices_py: Mutex::new(None),
        };
        let me = BTreeBuilder {
            spill_at: Mutex::new(spill_at),
            backing_indices: Mutex::new(Vec::new()),
            nodes: Mutex::new(PyDict::new(py).unbind()),
            nodes_by_key: Mutex::new(None),
        };
        (me, parent)
    }

    #[getter]
    fn _spill_at(&self) -> usize {
        *self.spill_at.lock().unwrap()
    }

    #[setter(_spill_at)]
    fn set_spill_at(&self, value: usize) {
        *self.spill_at.lock().unwrap() = value;
    }

    #[getter]
    fn _backing_indices<'py>(&self, py: Python<'py>) -> Bound<'py, PyList> {
        let guard = self.backing_indices.lock().unwrap();
        let out = PyList::empty(py);
        for entry in guard.iter() {
            match entry {
                Some(b) => out.append(b.bind(py).clone()).unwrap(),
                None => out.append(py.None()).unwrap(),
            }
        }
        out
    }

    #[getter]
    fn _nodes<'py>(&self, py: Python<'py>) -> Bound<'py, PyDict> {
        self.nodes.lock().unwrap().bind(py).clone()
    }

    #[getter]
    fn _nodes_by_key<'py>(&self, py: Python<'py>) -> Bound<'py, PyAny> {
        match self.nodes_by_key.lock().unwrap().as_ref() {
            Some(d) => d.bind(py).clone().into_any(),
            None => py.None().into_bound(py),
        }
    }

    /// Add a node to the in-memory dict. Once `_nodes` reaches
    /// `spill_at`, the held nodes are merged into a backing index on
    /// disk via [`Self::spill_mem_keys_to_disk`].
    #[pyo3(signature = (key, value, references = None))]
    fn add_node(
        slf: Bound<'_, Self>,
        py: Python<'_>,
        key: Bound<'_, PyAny>,
        value: Bound<'_, PyBytes>,
        references: Option<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let key_tuple = ensure_key_tuple(py, &key)?;
        let parent = slf.borrow().into_super();
        let reference_lists = parent.inner.lock().unwrap().reference_lists();
        let key_length = parent.inner.lock().unwrap().key_length();
        drop(parent);
        let me = slf.borrow();
        let nodes_guard = me.nodes.lock().unwrap();
        let nodes = nodes_guard.bind(py).clone();
        drop(nodes_guard);
        let refs_arg = match references {
            Some(r) => r,
            None => PyTuple::empty(py).into_any(),
        };
        // Delegate to the existing helper that does validation +
        // duplicate-key check + dict insertion in one shot.
        let node_refs = crate::index::py_add_node_to_btree_builder(
            py,
            slf.clone().into_any(),
            key_tuple.clone().into_any(),
            value,
            refs_arg,
            nodes.clone(),
            reference_lists,
            key_length,
        )?;
        let node_refs: Bound<'_, PyAny> = node_refs.into_any();
        if me.nodes_by_key.lock().unwrap().is_some() && key_length > 1 {
            let val = nodes.get_item(key_tuple.clone())?.unwrap();
            let val_tuple = val.cast_into::<PyTuple>().map_err(|_| {
                PyTypeError::new_err("btree node value must be a (refs, value) tuple")
            })?;
            let value_b = val_tuple.get_item(1)?;
            Self::update_nodes_by_key_inner(
                py,
                &me,
                reference_lists > 0,
                key_tuple,
                value_b,
                &node_refs,
            )?;
        }
        if nodes.len() < *me.spill_at.lock().unwrap() {
            return Ok(());
        }
        drop(me);
        Self::spill_mem_keys_to_disk(slf, py)
    }

    /// Bulk-add nodes accepting either `(key, value, refs)` or
    /// `(key, value)` tuples depending on whether this builder has
    /// reference lists configured.
    fn add_nodes(slf: Bound<'_, Self>, py: Python<'_>, nodes: Bound<'_, PyAny>) -> PyResult<()> {
        let has_refs = slf
            .borrow()
            .into_super()
            .inner
            .lock()
            .unwrap()
            .reference_lists()
            > 0;
        for node in nodes.try_iter()? {
            let node = node?;
            let tup = node
                .cast::<PyTuple>()
                .map_err(|_| PyTypeError::new_err("node must be a tuple"))?;
            if has_refs {
                if tup.len() != 3 {
                    return Err(PyTypeError::new_err(
                        "node must be a 3-tuple when reference_lists > 0",
                    ));
                }
                let key = tup.get_item(0)?;
                let value = tup.get_item(1)?;
                let refs = tup.get_item(2)?;
                let value_b = value
                    .cast_into::<PyBytes>()
                    .map_err(|_| PyTypeError::new_err("value must be bytes"))?;
                Self::add_node(slf.clone(), py, key, value_b, Some(refs))?;
            } else {
                if tup.len() != 2 {
                    return Err(PyTypeError::new_err(
                        "node must be a 2-tuple when reference_lists == 0",
                    ));
                }
                let key = tup.get_item(0)?;
                let value = tup.get_item(1)?;
                let value_b = value
                    .cast_into::<PyBytes>()
                    .map_err(|_| PyTypeError::new_err("value must be bytes"))?;
                Self::add_node(slf.clone(), py, key, value_b, None)?;
            }
        }
        Ok(())
    }

    /// Return an estimate of the number of keys (exact, since this is
    /// an in-memory builder).
    fn key_count(&self, py: Python<'_>) -> PyResult<usize> {
        let mem = self.nodes.lock().unwrap().bind(py).len();
        let mut total = mem;
        let guard = self.backing_indices.lock().unwrap();
        for entry in guard.iter().flatten() {
            let n: usize = entry.bind(py).call_method0("key_count")?.extract()?;
            total += n;
        }
        Ok(total)
    }

    /// In-memory indices have no on-disk state, so validation is a
    /// no-op. Matches the historical Python implementation.
    fn validate(&self) {}

    fn __lt__(slf: Bound<'_, Self>, py: Python<'_>, other: Bound<'_, PyAny>) -> PyResult<bool> {
        if other.is_instance_of::<BTreeBuilder>() {
            // Compare on the underlying `_nodes` dict, matching the
            // Python original's `self._nodes < other._nodes`.
            let a = slf.borrow().nodes.lock().unwrap().clone_ref(py);
            let b_borrow = other.downcast::<BTreeBuilder>().unwrap().borrow();
            let b = b_borrow.nodes.lock().unwrap().clone_ref(py);
            return a.bind(py).lt(b.bind(py));
        }
        if other.is_instance_of::<BTreeGraphIndex>() {
            // Existing on-disk indices sort before still-being-built ones.
            return Ok(false);
        }
        Err(PyTypeError::new_err(other.unbind()))
    }

    fn __hash__(slf: Bound<'_, Self>) -> isize {
        slf.as_ptr() as isize
    }

    /// `_iter_mem_nodes`: sorted iterator over the in-memory dict,
    /// each entry prefixed with `self`.
    fn _iter_mem_nodes<'py>(
        slf: Bound<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyList>> {
        let me = slf.borrow();
        let nodes = me.nodes.lock().unwrap().bind(py).clone();
        let has_refs = slf
            .borrow()
            .into_super()
            .inner
            .lock()
            .unwrap()
            .reference_lists()
            > 0;
        let sorted: Bound<'py, PyList> =
            crate::index::py_iter_btree_builder_nodes_sorted(py, nodes, has_refs)?;
        let out = PyList::empty(py);
        let self_any: Bound<'py, PyAny> = slf.into_any();
        for entry in sorted.iter() {
            let tup = entry
                .cast_into::<PyTuple>()
                .map_err(|_| PyTypeError::new_err("entry must be a tuple"))?;
            let mut items: Vec<Bound<'py, PyAny>> = vec![self_any.clone()];
            for it in tup.iter() {
                items.push(it);
            }
            out.append(PyTuple::new(py, items)?)?;
        }
        Ok(out)
    }

    /// `iter_all_entries`: merge-sorted iteration over in-memory +
    /// backing indices.
    fn iter_all_entries<'py>(
        slf: Bound<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mem = Self::_iter_mem_nodes(slf.clone(), py)?;
        let mem_iter = mem.try_iter()?;
        let mut iterators: Vec<Bound<'py, PyAny>> = vec![mem_iter.into_any()];
        let backings: Vec<Py<PyAny>> = {
            let me = slf.borrow();
            let guard = me.backing_indices.lock().unwrap();
            guard
                .iter()
                .filter_map(|e| e.as_ref().map(|p| p.clone_ref(py)))
                .collect()
        };
        for backing in backings {
            let entries = backing.bind(py).call_method0("iter_all_entries")?;
            iterators.push(entries.try_iter()?.into_any());
        }
        if iterators.len() == 1 {
            return Ok(iterators.into_iter().next().unwrap());
        }
        Self::iter_smallest(slf, py, iterators)
    }

    /// `iter_entries(keys)`: yields entries for the requested keys in
    /// (no-defined) order. Looks in the in-memory dict first; any keys
    /// not found there are searched through the backing indices.
    fn iter_entries<'py>(
        slf: Bound<'py, Self>,
        py: Python<'py>,
        keys: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let key_set = pyo3::types::PySet::empty(py)?;
        for k in keys.try_iter()? {
            key_set.add(k?)?;
        }
        let nodes = slf.borrow().nodes.lock().unwrap().bind(py).clone();
        let has_refs = slf
            .borrow()
            .into_super()
            .inner
            .lock()
            .unwrap()
            .reference_lists()
            > 0;
        let (entries, local_keys) = crate::index::py_iter_btree_builder_nodes_for_keys(
            py,
            nodes,
            key_set.clone().into_any(),
            has_refs,
        )?;

        let out = PyList::empty(py);
        let self_any: Bound<'py, PyAny> = slf.clone().into_any();
        for entry in entries.iter() {
            let tup = entry
                .cast_into::<PyTuple>()
                .map_err(|_| PyTypeError::new_err("entry must be a tuple"))?;
            let mut items: Vec<Bound<'py, PyAny>> = vec![self_any.clone()];
            for it in tup.iter() {
                items.push(it);
            }
            out.append(PyTuple::new(py, items)?)?;
        }
        for k in local_keys.iter() {
            key_set.discard(k)?;
        }
        let backings: Vec<Py<PyAny>> = {
            let me = slf.borrow();
            let guard = me.backing_indices.lock().unwrap();
            guard
                .iter()
                .filter_map(|e| e.as_ref().map(|p| p.clone_ref(py)))
                .collect()
        };
        for backing in backings {
            if key_set.is_empty() {
                break;
            }
            let entries = backing
                .bind(py)
                .call_method1("iter_entries", (key_set.clone(),))?;
            for entry in entries.try_iter()? {
                let entry = entry?;
                let tup = entry
                    .clone()
                    .cast_into::<PyTuple>()
                    .map_err(|_| PyTypeError::new_err("entry must be a tuple"))?;
                let key = tup.get_item(1)?;
                key_set.discard(key)?;
                let mut items: Vec<Bound<'py, PyAny>> = vec![self_any.clone()];
                for i in 1..tup.len() {
                    items.push(tup.get_item(i)?);
                }
                out.append(PyTuple::new(py, items)?)?;
            }
        }
        Ok(out.try_iter()?.into_any())
    }

    /// `iter_entries_prefix(keys)`: prefix-keyed lookup. Walks backing
    /// indices first then the in-memory dict (matching the Python
    /// original).
    fn iter_entries_prefix<'py>(
        slf: Bound<'py, Self>,
        py: Python<'py>,
        keys: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let keys_list = PyList::empty(py);
        for k in keys.try_iter()? {
            keys_list.append(k?)?;
        }
        let out = PyList::empty(py);
        if keys_list.is_empty() {
            return Ok(out.try_iter()?.into_any());
        }
        let self_any: Bound<'py, PyAny> = slf.clone().into_any();
        let backings: Vec<Py<PyAny>> = {
            let me = slf.borrow();
            let guard = me.backing_indices.lock().unwrap();
            guard
                .iter()
                .filter_map(|e| e.as_ref().map(|p| p.clone_ref(py)))
                .collect()
        };
        for backing in backings {
            let entries = backing
                .bind(py)
                .call_method1("iter_entries_prefix", (keys_list.clone(),))?;
            for entry in entries.try_iter()? {
                let entry = entry?;
                let tup = entry
                    .cast_into::<PyTuple>()
                    .map_err(|_| PyTypeError::new_err("entry must be a tuple"))?;
                let mut items: Vec<Bound<'py, PyAny>> = vec![self_any.clone()];
                for i in 1..tup.len() {
                    items.push(tup.get_item(i)?);
                }
                out.append(PyTuple::new(py, items)?)?;
            }
        }
        let parent = slf.borrow().into_super();
        let has_refs = parent.inner.lock().unwrap().reference_lists() > 0;
        let key_length = parent.inner.lock().unwrap().key_length();
        drop(parent);
        let nodes = slf.borrow().nodes.lock().unwrap().bind(py).clone();
        let mode = if has_refs {
            "btree-builder-refs"
        } else {
            "btree-builder-norefs"
        };
        let local_entries: Bound<'py, PyList> = crate::index::py_iter_entries_prefix(
            py,
            nodes,
            keys_list.into_any(),
            key_length,
            mode,
        )?;
        for entry in local_entries.iter() {
            let tup = entry
                .cast_into::<PyTuple>()
                .map_err(|_| PyTypeError::new_err("entry must be a tuple"))?;
            let mut items: Vec<Bound<'py, PyAny>> = vec![self_any.clone()];
            for it in tup.iter() {
                items.push(it);
            }
            out.append(PyTuple::new(py, items)?)?;
        }
        Ok(out.try_iter()?.into_any())
    }

    /// `_get_nodes_by_key`: lazy trie. First call builds it from
    /// `_nodes`; subsequent calls return the cached dict.
    fn _get_nodes_by_key<'py>(
        slf: Bound<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyDict>> {
        {
            let me = slf.borrow();
            let guard = me.nodes_by_key.lock().unwrap();
            if let Some(d) = guard.as_ref() {
                return Ok(d.bind(py).clone());
            }
        }
        let parent = slf.borrow().into_super();
        let has_refs = parent.inner.lock().unwrap().reference_lists() > 0;
        drop(parent);
        let nodes_by_key = PyDict::new(py);
        let nodes = slf.borrow().nodes.lock().unwrap().bind(py).clone();
        for (key_obj, value_obj) in nodes.iter() {
            let key_tuple = key_obj
                .cast::<PyTuple>()
                .map_err(|_| PyTypeError::new_err("key must be a tuple"))?;
            let value_tuple = value_obj
                .cast_into::<PyTuple>()
                .map_err(|_| PyTypeError::new_err("btree node must be a 2-tuple"))?;
            let refs_obj = value_tuple.get_item(0)?;
            let value_b = value_tuple.get_item(1)?;
            let leaf_value: Bound<'py, PyAny> = if has_refs {
                PyTuple::new(
                    py,
                    [
                        key_tuple.clone().into_any(),
                        value_b.clone(),
                        refs_obj.clone(),
                    ],
                )?
                .into_any()
            } else {
                PyTuple::new(py, [key_tuple.clone().into_any(), value_b.clone()])?.into_any()
            };
            let mut key_dict = nodes_by_key.clone();
            for i in 0..(key_tuple.len() - 1) {
                let subkey = key_tuple.get_item(i)?;
                let entry = key_dict.get_item(subkey.clone())?;
                match entry {
                    Some(d) => {
                        key_dict = d.cast_into()?;
                    }
                    None => {
                        let new_dict = PyDict::new(py);
                        key_dict.set_item(subkey, new_dict.clone())?;
                        key_dict = new_dict;
                    }
                }
            }
            let last = key_tuple.get_item(key_tuple.len() - 1)?;
            key_dict.set_item(last, leaf_value)?;
        }
        *slf.borrow().nodes_by_key.lock().unwrap() = Some(nodes_by_key.clone().unbind());
        Ok(nodes_by_key)
    }

    /// `find_ancestry`: classic graph walk over `iter_entries`. Each
    /// iteration looks up the current `pending` keys, records their
    /// parents, and feeds newly-discovered parents into the next round.
    fn find_ancestry<'py>(
        slf: Bound<'py, Self>,
        py: Python<'py>,
        keys: Bound<'py, PyAny>,
        ref_list_num: usize,
    ) -> PyResult<(Bound<'py, PyDict>, Bound<'py, pyo3::types::PySet>)> {
        let parent_map = PyDict::new(py);
        let missing = pyo3::types::PySet::empty(py)?;
        let mut pending = pyo3::types::PySet::empty(py)?;
        for k in keys.try_iter()? {
            pending.add(k?)?;
        }
        while !pending.is_empty() {
            let next_pending = pyo3::types::PySet::empty(py)?;
            let entries = Self::iter_entries(slf.clone(), py, pending.clone().into_any())?;
            for entry in entries.try_iter()? {
                let entry = entry?;
                let tup = entry
                    .cast_into::<PyTuple>()
                    .map_err(|_| PyTypeError::new_err("entry must be a tuple"))?;
                let key = tup.get_item(1)?;
                let refs: Bound<'py, PyTuple> = tup.get_item(3)?.cast_into()?;
                let parent_keys: Bound<'py, PyAny> = refs.get_item(ref_list_num)?;
                let parent_keys_tuple: Bound<'py, PyTuple> = parent_keys.clone().cast_into()?;
                parent_map.set_item(key, parent_keys.clone())?;
                for p in parent_keys_tuple.iter() {
                    if !parent_map.contains(p.clone())? {
                        next_pending.add(p)?;
                    }
                }
            }
            // Anything in pending that didn't end up in parent_map this
            // round is genuinely missing.
            for k in pending.iter() {
                if !parent_map.contains(k.clone())? {
                    missing.add(k)?;
                }
            }
            pending = next_pending;
        }
        Ok((parent_map, missing))
    }

    /// `_find_ancestors`: one-step ancestry walk. Populates
    /// `parent_map` with each search-key's parents and adds unfound
    /// keys to `missing_keys`. Returns the set of newly-discovered
    /// parents not already in `parent_map`.
    fn _find_ancestors<'py>(
        slf: Bound<'py, Self>,
        py: Python<'py>,
        search_keys: Bound<'py, PyAny>,
        ref_list_num: usize,
        parent_map: Bound<'py, PyDict>,
        missing_keys: Bound<'py, pyo3::types::PySet>,
    ) -> PyResult<Bound<'py, pyo3::types::PySet>> {
        let found = pyo3::types::PySet::empty(py)?;
        let new_search = pyo3::types::PySet::empty(py)?;
        let search_set = pyo3::types::PySet::empty(py)?;
        for k in search_keys.try_iter()? {
            search_set.add(k?)?;
        }
        let entries = Self::iter_entries(slf, py, search_set.clone().into_any())?;
        for entry in entries.try_iter()? {
            let entry = entry?;
            let tup = entry
                .cast_into::<PyTuple>()
                .map_err(|_| PyTypeError::new_err("entry must be a tuple"))?;
            let key = tup.get_item(1)?;
            let refs: Bound<'py, PyTuple> = tup.get_item(3)?.cast_into()?;
            let parent_keys: Bound<'py, PyTuple> = refs.get_item(ref_list_num)?.cast_into()?;
            parent_map.set_item(key.clone(), parent_keys.clone())?;
            for p in parent_keys.iter() {
                if !parent_map.contains(p.clone())? {
                    new_search.add(p)?;
                }
            }
            found.add(key)?;
        }
        // search_keys - found = newly-known-missing
        for k in search_set.iter() {
            if !found.contains(k.clone())? {
                missing_keys.add(k)?;
            }
        }
        Ok(new_search)
    }

    /// `finish`: serialise all entries to a temporary file and return
    /// its handle.
    fn finish<'py>(slf: Bound<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let iter = Self::iter_all_entries(slf.clone(), py)?;
        let (file, _size) = Self::write_nodes(slf, py, iter, true)?;
        Ok(file)
    }
}

/// Coerce a Python value into a tuple (the historic Python wrapper
/// did `key = tuple(key)`), preserving the contract that builders
/// accept any iterable that materialises into a key tuple.
fn ensure_key_tuple<'py>(
    py: Python<'py>,
    obj: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyTuple>> {
    if let Ok(t) = obj.downcast::<PyTuple>() {
        return Ok(t.clone());
    }
    let builtins = py.import("builtins")?;
    let tuple_class = builtins.getattr("tuple")?;
    let coerced = tuple_class.call1((obj.clone(),))?;
    Ok(coerced.cast_into()?)
}

/// Non-pyo3 helpers for [`BTreeBuilder`]. These are called from the
/// `#[pymethods]` block above but are not themselves exposed to
/// Python — they handle spill/merge/serialise orchestration over the
/// Python tempfile + `BTreeGraphIndex` objects.
impl BTreeBuilder {
    /// Update the lazy `_nodes_by_key` trie with a single new key.
    /// Mirrors Python's `_update_nodes_by_key`. The caller passes
    /// `has_refs` so this helper doesn't have to re-borrow the parent
    /// PyGraphIndexBuilder.
    fn update_nodes_by_key_inner<'py>(
        py: Python<'py>,
        me: &PyRef<'_, Self>,
        has_refs: bool,
        key_tuple: Bound<'py, PyTuple>,
        value_b: Bound<'py, PyAny>,
        node_refs: &Bound<'py, PyAny>,
    ) -> PyResult<()> {
        let nbk_guard = me.nodes_by_key.lock().unwrap();
        let Some(nbk_py) = nbk_guard.as_ref() else {
            return Ok(());
        };
        let leaf_value: Bound<'py, PyAny> = if has_refs {
            PyTuple::new(
                py,
                [
                    key_tuple.clone().into_any(),
                    value_b,
                    node_refs.clone().into_any(),
                ],
            )?
            .into_any()
        } else {
            PyTuple::new(py, [key_tuple.clone().into_any(), value_b])?.into_any()
        };
        let mut key_dict = nbk_py.bind(py).clone();
        for i in 0..(key_tuple.len() - 1) {
            let subkey = key_tuple.get_item(i)?;
            let entry = key_dict.get_item(subkey.clone())?;
            match entry {
                Some(d) => {
                    key_dict = d.cast_into()?;
                }
                None => {
                    let new_dict = PyDict::new(py);
                    key_dict.set_item(subkey, new_dict.clone())?;
                    key_dict = new_dict;
                }
            }
        }
        let last = key_tuple.get_item(key_tuple.len() - 1)?;
        key_dict.set_item(last, leaf_value)?;
        Ok(())
    }

    /// `_spill_mem_keys_to_disk`: flush the in-memory `_nodes` dict
    /// into a backing index on disk. If `_combine_backing_indices` is
    /// true, merge with leading filled slots per the power-of-2 strategy.
    fn spill_mem_keys_to_disk(slf: Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let combine: bool = {
            let parent = slf.borrow().into_super();
            let stored = parent
                .combine_backing_indices_py
                .lock()
                .unwrap()
                .as_ref()
                .and_then(|v| v.extract::<bool>(py).ok());
            stored.unwrap_or_else(|| parent.inner.lock().unwrap().combine_backing_indices())
        };
        let (file, size, slot) = if combine {
            let occupancy: Vec<bool> = {
                let me = slf.borrow();
                let guard = me.backing_indices.lock().unwrap();
                guard.iter().map(|e| e.is_some()).collect()
            };
            let slot = spill_landing_slot(&occupancy);
            // Combine mem with every leading non-None backing (slots 0..slot).
            let mem_entries = Self::_iter_mem_nodes(slf.clone(), py)?;
            let mut iterators: Vec<Bound<'_, PyAny>> = vec![mem_entries.try_iter()?.into_any()];
            let leading: Vec<Py<PyAny>> = {
                let me = slf.borrow();
                let guard = me.backing_indices.lock().unwrap();
                guard[..slot]
                    .iter()
                    .filter_map(|e| e.as_ref().map(|p| p.clone_ref(py)))
                    .collect()
            };
            for backing in leading {
                let entries = backing.bind(py).call_method0("iter_all_entries")?;
                iterators.push(entries.try_iter()?.into_any());
            }
            let merged = Self::iter_smallest(slf.clone(), py, iterators)?;
            let (file, size) = Self::write_nodes(slf.clone(), py, merged, false)?;
            (file, size, slot)
        } else {
            // Plain spill: just write the mem nodes; new backing goes
            // at the end of the list.
            let slot = slf.borrow().backing_indices.lock().unwrap().len();
            let mem_entries = Self::_iter_mem_nodes(slf.clone(), py)?;
            let (file, size) = Self::write_nodes(slf.clone(), py, mem_entries.into_any(), false)?;
            (file, size, slot)
        };

        // Build a Python BTreeGraphIndex over a dummy transport that
        // returns a fixed recommended_page_size. The transport itself
        // is never used for I/O because we overwrite `_file` to point
        // at the just-written tempfile.
        let btree_index_py = py.import("bzrformats.btree_index")?;
        let dummy_transport = btree_index_py.getattr("_DummyTransport")?.call0()?;
        let new_backing_cls = btree_index_py.getattr("BTreeGraphIndex")?;
        let new_backing = new_backing_cls.call1((dummy_transport, "<temp>", size))?;
        new_backing.setattr("_file", file)?;

        {
            let me = slf.borrow();
            let mut guard = me.backing_indices.lock().unwrap();
            if combine {
                if guard.len() == slot {
                    guard.push(None);
                }
                guard[slot] = Some(new_backing.unbind());
                for prev in &mut guard[..slot] {
                    *prev = None;
                }
            } else {
                guard.push(Some(new_backing.unbind()));
            }
            // Clear mem.
            *me.nodes.lock().unwrap() = PyDict::new(py).unbind();
            *me.nodes_by_key.lock().unwrap() = None;
        }
        Ok(())
    }

    /// `_iter_smallest`: k-way merge across pre-sorted iterators, each
    /// yielding `(self, key, ...)` tuples. Raises `BadIndexDuplicateKey`
    /// when the same key appears in two iterators back-to-back.
    fn iter_smallest<'py>(
        slf: Bound<'py, Self>,
        py: Python<'py>,
        iterators: Vec<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if iterators.len() == 1 {
            return Ok(iterators.into_iter().next().unwrap());
        }
        let mut current: Vec<Option<Bound<'py, PyTuple>>> = Vec::with_capacity(iterators.len());
        for it in &iterators {
            current.push(advance_iter(it)?);
        }
        let out = PyList::empty(py);
        let mut last_key: Option<Bound<'py, PyAny>> = None;
        let self_any: Bound<'py, PyAny> = slf.clone().into_any();
        let iterators_vec = iterators;
        loop {
            // Find the index of the smallest-key current entry.
            let mut best: Option<(usize, Bound<'py, PyAny>)> = None;
            for (i, entry) in current.iter().enumerate() {
                let Some(e) = entry.as_ref() else { continue };
                let key = e.get_item(1)?;
                let smaller = match &best {
                    Some((_, cur_best_key)) => key.lt(cur_best_key)?,
                    None => true,
                };
                if smaller {
                    best = Some((i, key));
                }
            }
            let Some((idx, key)) = best else {
                break;
            };
            // Duplicate detection — last selected key must not equal this one.
            if let Some(prev) = &last_key {
                if prev.eq(key.clone())? {
                    return Err(BadIndexDuplicateKey::new_err((
                        prev.clone().unbind(),
                        slf.clone().into_any().unbind(),
                    )));
                }
            }
            // Yield: replace the (other-self, ...) prefix with our self.
            let original = current[idx].clone().unwrap();
            let mut items: Vec<Bound<'py, PyAny>> = vec![self_any.clone()];
            for i in 1..original.len() {
                items.push(original.get_item(i)?);
            }
            out.append(PyTuple::new(py, items)?)?;
            last_key = Some(key);
            current[idx] = advance_iter(&iterators_vec[idx])?;
        }
        Ok(out.try_iter()?.into_any())
    }

    /// `_write_nodes`: serialise a sorted iterator of nodes into a
    /// `tempfile.NamedTemporaryFile` (or `BytesIO` for small outputs)
    /// and return `(file_handle, size)`. The handle is rewound to the
    /// start so the caller can read it directly.
    fn write_nodes<'py>(
        slf: Bound<'py, Self>,
        py: Python<'py>,
        node_iterator: Bound<'py, PyAny>,
        allow_optimize: bool,
    ) -> PyResult<(Bound<'py, PyAny>, usize)> {
        let parent = slf.borrow().into_super();
        let reference_lists = parent.inner.lock().unwrap().reference_lists();
        let key_length = parent.inner.lock().unwrap().key_length();
        let optimize_for_size = if allow_optimize {
            parent
                .optimize_for_size_py
                .lock()
                .unwrap()
                .as_ref()
                .and_then(|v| v.extract::<bool>(py).ok())
                .unwrap_or_else(|| parent.inner.lock().unwrap().optimize_for_size())
        } else {
            false
        };
        drop(parent);
        // Read _PAGE_SIZE and _RESERVED_HEADER_BYTES from the Python
        // btree_index module so the historical test pattern of
        // monkey-patching those module-level constants (via
        // overrideAttr) keeps working.
        let btree_index_mod = py.import("bzrformats.btree_index")?;
        let page_size: usize = btree_index_mod.getattr("_PAGE_SIZE")?.extract()?;
        let reserved_header_bytes: usize = btree_index_mod
            .getattr("_RESERVED_HEADER_BYTES")?
            .extract()?;
        let blob = crate::btree_serializer::serialize_btree_index(
            py,
            &node_iterator,
            reference_lists,
            key_length,
            optimize_for_size,
            Some(page_size),
            Some(reserved_header_bytes),
        )?;
        let blob_bytes = blob.as_bytes();
        let size = blob_bytes.len();
        let file: Bound<'py, PyAny> = if size > page_size {
            let tempfile_mod = py.import("tempfile")?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("prefix", "bzr-index-")?;
            tempfile_mod
                .getattr("NamedTemporaryFile")?
                .call((), Some(&kwargs))?
        } else {
            let io = py.import("io")?;
            io.getattr("BytesIO")?.call0()?
        };
        file.call_method1("write", (blob.clone(),))?;
        file.call_method0("flush")?;
        file.call_method1("seek", (0,))?;
        Ok((file, size))
    }
}

/// Pull the next item from a Python iterator, returning `None` on
/// `StopIteration`. The iterator must yield tuples for `iter_smallest`.
fn advance_iter<'py>(iter: &Bound<'py, PyAny>) -> PyResult<Option<Bound<'py, PyTuple>>> {
    match iter.call_method0("__next__") {
        Ok(item) => Ok(Some(item.cast_into()?)),
        Err(e) if e.is_instance_of::<PyStopIteration>(iter.py()) => Ok(None),
        Err(e) => Err(e),
    }
}

pub fn _btree_index_rs(py: Python) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "btree_index")?;
    m.add_function(wrap_pyfunction!(py_parse_btree_header, &m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_internal_node, &m)?)?;
    m.add_class::<BTreeGraphIndex>()?;
    m.add_class::<EntryIterator>()?;
    m.add_class::<BTreeBuilder>()?;
    Ok(m)
}
