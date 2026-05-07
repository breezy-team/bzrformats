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

import_exception!(bzrformats.index, BadIndexFormatSignature);
import_exception!(bzrformats.index, BadIndexOptions);

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

    #[setter]
    fn set__size(&self, value: Option<u64>) {
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

pub fn _btree_index_rs(py: Python) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "btree_index")?;
    m.add_function(wrap_pyfunction!(py_parse_btree_header, &m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_internal_node, &m)?)?;
    m.add_class::<BTreeGraphIndex>()?;
    m.add_class::<EntryIterator>()?;
    Ok(m)
}
