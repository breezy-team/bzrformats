use bazaar::btree_builder::spill_landing_slot;
use bazaar::btree_index::{
    compute_row_offsets, compute_total_pages_in_index, decompress_page, expand_offsets,
    find_layer_first_and_end, parse_btree_header, parse_internal_node, parse_leaf_lines,
    BTreeHeader, BTreeIndexError, InternalNode, LeafKey, PageRange, ReadPlan, INTERNAL_FLAG,
    LEAF_FLAG,
};
use pyo3::class::basic::CompareOp;
use pyo3::exceptions::{
    PyKeyError, PyNotImplementedError, PyStopIteration, PyTypeError, PyValueError,
};
use pyo3::import_exception;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PySet, PyTuple};
use std::collections::HashSet;
use std::sync::Mutex;

use crate::index::PyGraphIndexBuilder;

import_exception!(bzrformats.index, BadIndexFormatSignature);
import_exception!(bzrformats.index, BadIndexOptions);
import_exception!(bzrformats.index, BadIndexDuplicateKey);

/// The on-disk B+Tree page size.
fn page_size() -> usize {
    bazaar::btree_index::PAGE_SIZE
}

/// Stand-in transport for spilled backing indices.
///
/// A spilled backing is read directly from the open tempfile handle, so
/// its `BTreeGraphIndex`'s transport is never used for I/O. This only has
/// to satisfy the `recommended_page_size` protocol. Mirrors the Python
/// `bzrformats.btree_index._DummyTransport`.
#[pyclass]
struct DummyTransport;

#[pymethods]
impl DummyTransport {
    fn recommended_page_size(&self) -> usize {
        page_size()
    }
}

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

fn key_to_py<'py>(py: Python<'py>, key: &LeafKey) -> PyResult<Bound<'py, PyTuple>> {
    let parts: Vec<Bound<PyBytes>> = key.iter().map(|p| PyBytes::new(py, p)).collect();
    PyTuple::new(py, parts)
}

/// `[(node_index, sub_keys_list)]` — the per-leaf key groupings produced
/// while walking the internal nodes.
type KeysAtIndex = Vec<(usize, Py<PyList>)>;

/// A leaf node of a serialised B+Tree index. Mirrors the historic
/// `_LeafNode(dict)`: a sorted key -> `(value, refs)` map with min/max
/// bookkeeping. The reader builds these via the (pluggable) `_leaf_factory`;
/// tests also construct them directly.
#[pyclass(module = "bzrformats._bzr_rs.btree_index", name = "_LeafNode")]
struct LeafNodePy {
    /// `(key_tuple, (value_bytes, refs_tuple))` pairs, sorted by key.
    entries: Vec<(Py<PyTuple>, Py<PyTuple>)>,
    /// Map from key tuple (as raw segments) to its index in `entries`.
    by_key: std::collections::HashMap<LeafKey, usize>,
    min_key: Option<Py<PyTuple>>,
    max_key: Option<Py<PyTuple>>,
}

#[pymethods]
impl LeafNodePy {
    #[new]
    fn new(
        py: Python<'_>,
        bytes: &[u8],
        key_length: usize,
        ref_list_length: usize,
    ) -> PyResult<Self> {
        let parsed = parse_leaf_lines(bytes, key_length, ref_list_length)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        // parse_leaf_lines preserves on-disk order; the historic _LeafNode
        // sorts on access. Sort once here so all_items()/all_keys() and the
        // min/max keys match.
        let mut sorted = parsed;
        sorted.sort_by(|a, b| a.0.cmp(&b.0));
        let mut entries: Vec<(Py<PyTuple>, Py<PyTuple>)> = Vec::with_capacity(sorted.len());
        let mut by_key: std::collections::HashMap<LeafKey, usize> =
            std::collections::HashMap::with_capacity(sorted.len());
        for (key, value, refs) in &sorted {
            let key_py = key_to_py(py, key)?;
            let value_py = PyBytes::new(py, value);
            let refs_py = refs_to_py(py, refs)?;
            let pair = PyTuple::new(py, [value_py.into_any(), refs_py.into_any()])?;
            by_key.insert(key.clone(), entries.len());
            entries.push((key_py.unbind(), pair.unbind()));
        }
        let min_key = entries.first().map(|(k, _)| k.clone_ref(py));
        let max_key = entries.last().map(|(k, _)| k.clone_ref(py));
        Ok(Self {
            entries,
            by_key,
            min_key,
            max_key,
        })
    }

    fn __len__(&self) -> usize {
        self.entries.len()
    }

    fn __contains__(&self, key: &Bound<PyAny>) -> PyResult<bool> {
        Ok(self.by_key.contains_key(&py_key_segments(key)?))
    }

    fn __getitem__<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        match self.by_key.get(&py_key_segments(key)?) {
            Some(&idx) => Ok(self.entries[idx].1.bind(py).clone()),
            None => Err(PyKeyError::new_err(key.clone().unbind())),
        }
    }

    /// Sorted `(key, (value, refs))` items. Matches `_LeafNode.all_items`.
    fn all_items<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let out = PyList::empty(py);
        for (k, v) in &self.entries {
            out.append(PyTuple::new(
                py,
                [k.bind(py).clone().into_any(), v.bind(py).clone().into_any()],
            )?)?;
        }
        Ok(out)
    }

    /// Sorted keys. Matches `_LeafNode.all_keys`.
    fn all_keys<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let out = PyList::empty(py);
        for (k, _) in &self.entries {
            out.append(k.bind(py).clone())?;
        }
        Ok(out)
    }

    #[getter]
    fn min_key<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyTuple>> {
        self.min_key.as_ref().map(|k| k.bind(py).clone())
    }

    #[getter]
    fn max_key<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyTuple>> {
        self.max_key.as_ref().map(|k| k.bind(py).clone())
    }
}

/// An internal node of a serialised B+Tree index. Mirrors `_InternalNode`:
/// a child page `offset` plus the key tuples used as bisect split points.
#[pyclass(module = "bzrformats._bzr_rs.btree_index", name = "_InternalNode")]
struct InternalNodePy {
    #[pyo3(get)]
    offset: usize,
    keys: Vec<Py<PyTuple>>,
}

#[pymethods]
impl InternalNodePy {
    #[new]
    fn new(py: Python<'_>, bytes: &[u8]) -> PyResult<Self> {
        let InternalNode { offset, keys } =
            parse_internal_node(bytes).map_err(|e| PyValueError::new_err(e.to_string()))?;
        let keys_py: Vec<Py<PyTuple>> = keys
            .iter()
            .map(|k| key_to_py(py, k).map(|t| t.unbind()))
            .collect::<PyResult<_>>()?;
        Ok(Self {
            offset,
            keys: keys_py,
        })
    }

    #[getter]
    fn keys<'py>(&self, py: Python<'py>) -> Bound<'py, PyList> {
        let out = PyList::empty(py);
        for k in &self.keys {
            out.append(k.bind(py).clone()).unwrap();
        }
        out
    }
}

/// Extract the raw `Vec<Vec<u8>>` segments from a Python key tuple, for
/// hashing/lookup inside `_LeafNode`.
fn py_key_segments(key: &Bound<PyAny>) -> PyResult<LeafKey> {
    let tuple = key.cast::<PyTuple>()?;
    let mut parts = Vec::with_capacity(tuple.len());
    for item in tuple.iter() {
        parts.push(item.cast_into::<PyBytes>()?.as_bytes().to_vec());
    }
    Ok(parts)
}

/// Convert reference lists to the nested tuple-of-tuples Python shape.
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

/// B+Tree graph index reader. Thin wrapper holding the index's mutable
/// state (header fields, node caches, root node, pluggable leaf factory)
/// as Python objects, and delegating the pure parsing and prefetch math to
/// `bazaar::btree_index`. Orchestration (transport IO, zlib, caching) lives
/// in these `#[pymethods]` so the white-box tests can drive and monkeypatch
/// the private surface exactly as they did the historic Python class.
#[pyclass(module = "bzrformats._bzr_rs.btree_index", subclass, dict)]
struct BTreeGraphIndex {
    transport: Py<PyAny>,
    name: String,
    base_offset: u64,
    file: Mutex<Option<Py<PyAny>>>,

    size: Mutex<Option<u64>>,
    node_ref_lists: Mutex<Option<usize>>,
    key_length: Mutex<Option<usize>>,
    key_count: Mutex<Option<usize>>,
    row_lengths: Mutex<Option<Vec<usize>>>,
    row_offsets: Mutex<Option<Vec<usize>>>,
    recommended_pages: Mutex<usize>,

    root_node: Mutex<Option<Py<PyAny>>>,
    leaf_node_cache: Mutex<Py<PyAny>>,
    internal_node_cache: Mutex<Py<PyAny>>,
    leaf_factory: Mutex<Py<PyAny>>,
    leaf_value_cache: Mutex<Option<Py<PyAny>>>,
}

impl BTreeGraphIndex {
    fn lock_size(&self) -> Option<u64> {
        *self.size.lock().unwrap()
    }
    fn lock_node_ref_lists(&self) -> Option<usize> {
        *self.node_ref_lists.lock().unwrap()
    }
    fn lock_row_offsets(&self) -> Option<Vec<usize>> {
        self.row_offsets.lock().unwrap().clone()
    }
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
    ) -> PyResult<Self> {
        let ps = page_size();
        let recommended_read: u64 = transport
            .bind(py)
            .call_method0("recommended_page_size")?
            .extract()?;
        let recommended_pages = recommended_read.div_ceil(ps as u64) as usize;

        let lru_mod = py.import("bzrformats.lru_cache")?;
        let (leaf_cache, internal_cache): (Py<PyAny>, Py<PyAny>) = if unlimited_cache {
            (
                PyDict::new(py).into_any().unbind(),
                PyDict::new(py).into_any().unbind(),
            )
        } else {
            let node_cache_size = bazaar::btree_index::NODE_CACHE_SIZE;
            let leaf = lru_mod.getattr("LRUCache")?.call1((node_cache_size,))?;
            let internal = lru_mod.getattr("FIFOCache")?.call1((100,))?;
            (leaf.unbind(), internal.unbind())
        };
        let leaf_factory = py.get_type::<LeafNodePy>();

        Ok(Self {
            transport,
            name,
            base_offset: offset,
            file: Mutex::new(None),
            size: Mutex::new(size),
            node_ref_lists: Mutex::new(None),
            key_length: Mutex::new(None),
            key_count: Mutex::new(None),
            row_lengths: Mutex::new(None),
            row_offsets: Mutex::new(None),
            recommended_pages: Mutex::new(recommended_pages),
            root_node: Mutex::new(None),
            leaf_node_cache: Mutex::new(leaf_cache),
            internal_node_cache: Mutex::new(internal_cache),
            leaf_factory: Mutex::new(leaf_factory.into_any().unbind()),
            leaf_value_cache: Mutex::new(None),
        })
    }

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
    fn _file(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.file.lock().unwrap().as_ref().map(|f| f.clone_ref(py))
    }

    #[setter(_file)]
    fn set_file(&self, value: Option<Py<PyAny>>) {
        *self.file.lock().unwrap() = value;
    }

    #[getter]
    fn _size(&self) -> Option<u64> {
        self.lock_size()
    }

    #[setter(_size)]
    fn set_size(&self, value: Option<u64>) {
        *self.size.lock().unwrap() = value;
    }

    #[getter]
    fn node_ref_lists(&self) -> PyResult<usize> {
        self.lock_node_ref_lists()
            .ok_or_else(|| PyValueError::new_err("index header not yet parsed"))
    }

    #[setter(node_ref_lists)]
    fn set_node_ref_lists(&self, value: usize) {
        *self.node_ref_lists.lock().unwrap() = Some(value);
    }

    #[getter]
    fn _key_length(&self) -> Option<usize> {
        *self.key_length.lock().unwrap()
    }

    #[setter(_key_length)]
    fn set_key_length(&self, value: usize) {
        *self.key_length.lock().unwrap() = Some(value);
    }

    #[getter]
    fn _key_count(&self) -> Option<usize> {
        *self.key_count.lock().unwrap()
    }

    #[setter(_key_count)]
    fn set_key_count(&self, value: usize) {
        *self.key_count.lock().unwrap() = Some(value);
    }

    #[getter]
    fn _row_lengths<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyList>> {
        self.row_lengths.lock().unwrap().as_ref().map(|rl| {
            let l = PyList::empty(py);
            for n in rl {
                l.append(*n).unwrap();
            }
            l
        })
    }

    #[setter(_row_lengths)]
    fn set_row_lengths(&self, value: Vec<usize>) {
        *self.row_lengths.lock().unwrap() = Some(value);
    }

    #[getter]
    fn _row_offsets<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyList>> {
        self.row_offsets.lock().unwrap().as_ref().map(|ro| {
            let l = PyList::empty(py);
            for n in ro {
                l.append(*n).unwrap();
            }
            l
        })
    }

    #[setter(_row_offsets)]
    fn set_row_offsets(&self, value: Vec<usize>) {
        *self.row_offsets.lock().unwrap() = Some(value);
    }

    #[getter]
    fn _recommended_pages(&self) -> usize {
        *self.recommended_pages.lock().unwrap()
    }

    #[setter(_recommended_pages)]
    fn set_recommended_pages(&self, value: usize) {
        *self.recommended_pages.lock().unwrap() = value;
    }

    #[getter]
    fn _root_node(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.root_node
            .lock()
            .unwrap()
            .as_ref()
            .map(|n| n.clone_ref(py))
    }

    #[setter(_root_node)]
    fn set_root_node(&self, value: Option<Py<PyAny>>) {
        *self.root_node.lock().unwrap() = value;
    }

    #[getter]
    fn _leaf_node_cache(&self, py: Python<'_>) -> Py<PyAny> {
        self.leaf_node_cache.lock().unwrap().clone_ref(py)
    }

    #[setter(_leaf_node_cache)]
    fn set_leaf_node_cache(&self, value: Py<PyAny>) {
        *self.leaf_node_cache.lock().unwrap() = value;
    }

    #[getter]
    fn _internal_node_cache(&self, py: Python<'_>) -> Py<PyAny> {
        self.internal_node_cache.lock().unwrap().clone_ref(py)
    }

    #[setter(_internal_node_cache)]
    fn set_internal_node_cache(&self, value: Py<PyAny>) {
        *self.internal_node_cache.lock().unwrap() = value;
    }

    #[getter]
    fn _leaf_factory(&self, py: Python<'_>) -> Py<PyAny> {
        self.leaf_factory.lock().unwrap().clone_ref(py)
    }

    #[setter(_leaf_factory)]
    fn set_leaf_factory(&self, value: Py<PyAny>) {
        *self.leaf_factory.lock().unwrap() = value;
    }

    #[getter]
    fn _leaf_value_cache(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.leaf_value_cache
            .lock()
            .unwrap()
            .as_ref()
            .map(|c| c.clone_ref(py))
    }

    #[setter(_leaf_value_cache)]
    fn set_leaf_value_cache(&self, value: Option<Py<PyAny>>) {
        *self.leaf_value_cache.lock().unwrap() = value;
    }

    fn __hash__(slf: PyRef<'_, Self>) -> usize {
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
                    same_transport
                        && self.name == other.name
                        && self.lock_size() == other.lock_size()
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
                    let lt = (self.name.clone(), self.lock_size())
                        < (other.name.clone(), other.lock_size());
                    Ok(lt.into_pyobject(py)?.to_owned().into_any().unbind())
                } else if other.is_instance_of::<BTreeBuilder>() {
                    // Existing indexes sort before still-being-built ones.
                    Ok(true.into_pyobject(py)?.to_owned().into_any().unbind())
                } else {
                    Err(PyTypeError::new_err("cannot compare"))
                }
            }
            _ => Err(PyNotImplementedError::new_err("comparison not supported")),
        }
    }

    fn clear_cache(&self, py: Python<'_>) -> PyResult<()> {
        // Only the leaf cache is dropped; the root and internal-node cache
        // are intentionally retained (they are small and save round trips).
        let cache = self.leaf_node_cache.lock().unwrap().clone_ref(py);
        cache.bind(py).call_method0("clear")?;
        Ok(())
    }

    /// Compute `_row_offsets` from `_row_lengths`.
    fn _compute_row_offsets(&self) -> PyResult<()> {
        let row_lengths = self
            .row_lengths
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| PyValueError::new_err("_row_lengths not set"))?;
        *self.row_offsets.lock().unwrap() = Some(compute_row_offsets(&row_lengths));
        Ok(())
    }

    /// How many pages the index spans. Mirrors `_compute_total_pages_in_index`.
    fn _compute_total_pages_in_index(&self) -> PyResult<usize> {
        let size = self.lock_size();
        let root_present = self.root_node.lock().unwrap().is_some();
        let row_offsets_last = self.lock_row_offsets().and_then(|ro| ro.last().copied());
        if size.is_none() && !(root_present && row_offsets_last.is_some()) {
            return Err(pyo3::exceptions::PyAssertionError::new_err(
                "_compute_total_pages_in_index should not be called when self._size is None",
            ));
        }
        compute_total_pages_in_index(size, root_present, row_offsets_last, page_size()).ok_or_else(
            || pyo3::exceptions::PyAssertionError::new_err("cannot compute total pages"),
        )
    }

    /// Start/end page of the layer containing `offset`.
    fn _find_layer_first_and_end(&self, offset: usize) -> PyResult<(usize, usize)> {
        let row_offsets = self
            .lock_row_offsets()
            .ok_or_else(|| PyValueError::new_err("_row_offsets not set"))?;
        Ok(find_layer_first_and_end(&row_offsets, offset))
    }

    /// Page indexes we currently have cached. Defined as a normal method so
    /// tests can shadow it with an instance attribute (monkeypatch).
    fn _get_offsets_to_cached_pages<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PySet>> {
        let internal = self.internal_node_cache.lock().unwrap().clone_ref(py);
        let leaf = self.leaf_node_cache.lock().unwrap().clone_ref(py);
        let result = PySet::empty(py)?;
        for k in internal.bind(py).try_iter()? {
            result.add(k?)?;
        }
        for k in leaf.bind(py).call_method0("keys")?.try_iter()? {
            result.add(k?)?;
        }
        if self.root_node.lock().unwrap().is_some() {
            result.add(0usize)?;
        }
        Ok(result)
    }

    /// Decide which pages to prefetch. Reaches `_get_offsets_to_cached_pages`
    /// via Python dispatch so a monkeypatched version is honored.
    fn _expand_offsets<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        offsets: Vec<usize>,
    ) -> PyResult<Bound<'py, PyList>> {
        let me = slf.borrow();
        let recommended = *me.recommended_pages.lock().unwrap();
        let size = me.lock_size();
        // Early returns mirror the Python ones, echoing offsets unchanged.
        if offsets.len() >= recommended || size.is_none() {
            return PyList::new(py, &offsets);
        }
        let root_present = me.root_node.lock().unwrap().is_some();
        let row_lengths = me.row_lengths.lock().unwrap().clone().unwrap_or_default();
        let row_offsets = me.lock_row_offsets().unwrap_or_default();
        let total_pages = me._compute_total_pages_in_index()?;
        drop(me);

        let cached_set = slf.call_method0("_get_offsets_to_cached_pages")?;
        let mut cached: HashSet<usize> = HashSet::new();
        for item in cached_set.try_iter()? {
            cached.insert(item?.extract()?);
        }
        let expanded = expand_offsets(
            &offsets,
            recommended,
            size,
            total_pages,
            &cached,
            root_present,
            row_lengths.len(),
            &row_offsets,
        );
        PyList::new(py, &expanded)
    }

    /// Estimate of the number of keys (exact; stored in the header).
    fn key_count(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<usize> {
        if slf.borrow().key_count.lock().unwrap().is_none() {
            Self::_get_root_node(slf, py)?;
        }
        slf.borrow()
            .key_count
            .lock()
            .unwrap()
            .ok_or_else(|| PyValueError::new_err("key_count unavailable"))
    }

    fn external_references<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        ref_list_num: usize,
    ) -> PyResult<Bound<'py, PySet>> {
        if slf.borrow().root_node.lock().unwrap().is_none() {
            Self::_get_root_node(slf, py)?;
        }
        let nrl = slf.borrow().lock_node_ref_lists().unwrap_or(0);
        if ref_list_num + 1 > nrl {
            return Err(PyValueError::new_err(format!(
                "No ref list {ref_list_num}, index has {nrl} ref lists"
            )));
        }
        let keys = PySet::empty(py)?;
        let refs = PySet::empty(py)?;
        for entry in Self::iter_all_entries(slf, py)?.bind(py).try_iter()? {
            let tup = entry?.cast_into::<PyTuple>()?;
            keys.add(tup.get_item(1)?)?;
            let ref_lists = tup.get_item(3)?.cast_into::<PyTuple>()?;
            let this_list = ref_lists.get_item(ref_list_num)?.cast_into::<PyTuple>()?;
            for r in this_list.iter() {
                refs.add(r)?;
            }
        }
        refs.call_method1("difference_update", (keys,))?;
        Ok(refs)
    }

    fn validate(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        Self::_get_root_node(slf, py)?;
        let (start_node, node_end) = {
            let me = slf.borrow();
            let row_lengths = me.row_lengths.lock().unwrap().clone().unwrap_or_default();
            let row_offsets = me.lock_row_offsets().unwrap_or_default();
            let start = if row_lengths.len() > 1 {
                row_offsets.get(1).copied().unwrap_or(1)
            } else {
                1
            };
            let end = row_offsets.last().copied().unwrap_or(0);
            (start, end)
        };
        if start_node < node_end {
            let pages: Vec<usize> = (start_node..node_end).collect();
            // Just read and parse every node.
            for _ in Self::_read_nodes(slf, py, pages)?.try_iter()? {}
        }
        Ok(())
    }

    fn iter_all_entries<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Py<EntryIterator>> {
        let out = PyList::empty(py);
        if Self::key_count(slf, py)? == 0 {
            return Self::make_iter(slf, py, out);
        }
        let row_offsets = slf.borrow().lock_row_offsets().unwrap_or_default();
        let nrl = slf.borrow().lock_node_ref_lists().unwrap_or(0);
        let self_any = slf.clone().into_any();
        if *row_offsets.last().unwrap_or(&0) == 1 {
            // Only the root node, already read by key_count().
            let root = slf
                .borrow()
                ._root_node(py)
                .ok_or_else(|| PyValueError::new_err("root not loaded"))?;
            append_node_entries(py, &out, &self_any, &root.bind(py).clone(), nrl)?;
            return Self::make_iter(slf, py, out);
        }
        let start = row_offsets[row_offsets.len() - 2];
        let end = row_offsets[row_offsets.len() - 1];
        let needed: Vec<usize> = (start..end).collect();
        let nodes = Self::_read_nodes(slf, py, needed)?;
        for pair in nodes.try_iter()? {
            let tup = pair?.cast_into::<PyTuple>()?;
            let node = tup.get_item(1)?;
            append_node_entries(py, &out, &self_any, &node, nrl)?;
        }
        Self::make_iter(slf, py, out)
    }

    fn iter_entries<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        keys: Bound<'py, PyAny>,
    ) -> PyResult<Py<EntryIterator>> {
        let out = PyList::empty(py);
        // Deduplicate (the Python original uses a frozenset).
        let key_set = PySet::empty(py)?;
        for k in keys.try_iter()? {
            key_set.add(k?)?;
        }
        if key_set.is_empty() || Self::key_count(slf, py)? == 0 {
            return Self::make_iter(slf, py, out);
        }
        let nrl = slf.borrow().lock_node_ref_lists().unwrap_or(0);
        let (nodes, nodes_and_keys) = Self::walk_through_internal_nodes(slf, py, &key_set)?;
        let self_any = slf.clone().into_any();
        for (node_index, sub_keys) in nodes_and_keys {
            let sub_keys = sub_keys.bind(py);
            if sub_keys.is_empty() {
                continue;
            }
            let node = nodes
                .get_item(node_index)?
                .ok_or_else(|| PyValueError::new_err(format!("missing leaf {node_index}")))?;
            for sk in sub_keys.try_iter()? {
                let sk = sk?;
                if node.contains(&sk)? {
                    let value_refs = node.get_item(&sk)?.cast_into::<PyTuple>()?;
                    append_entry(py, &out, &self_any, &sk, &value_refs, nrl)?;
                }
            }
        }
        Self::make_iter(slf, py, out)
    }

    /// Iterate entries matching the given key prefixes. Returns a lazy
    /// iterator: prefix validation (which can raise `BadIndexKey`) and the
    /// full index scan are deferred to first iteration, matching the
    /// generator semantics of the historic Python implementation (tests do
    /// `assertRaises(BadIndexKey, list, index.iter_entries_prefix(...))`).
    fn iter_entries_prefix(
        slf: Py<Self>,
        py: Python<'_>,
        keys: Bound<'_, PyAny>,
    ) -> PyResult<Py<PrefixIterator>> {
        // Materialise the prefixes up front (the argument may be a one-shot
        // iterable) but do no validation yet.
        let prefixes = PyList::empty(py);
        for k in keys.try_iter()? {
            prefixes.append(k?)?;
        }
        Py::new(
            py,
            PrefixIterator {
                index: slf,
                prefixes: prefixes.unbind(),
                computed: Mutex::new(None),
                pos: Mutex::new(0),
            },
        )
    }

    /// Eager body of `iter_entries_prefix`, invoked lazily by
    /// [`PrefixIterator`]. Returns the fully-built result tuples.
    fn iter_entries_prefix_impl<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        keys: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        // Sorted, de-duplicated prefixes.
        let prefix_set = PySet::empty(py)?;
        for k in keys.try_iter()? {
            prefix_set.add(k?)?;
        }
        let out = PyList::empty(py);
        if prefix_set.is_empty() {
            return Ok(out);
        }
        // Load the header (for key length) if needed.
        if slf.borrow().key_count.lock().unwrap().is_none() {
            Self::_get_root_node(slf, py)?;
        }
        let key_length = slf.borrow()._key_length().unwrap_or(1);
        let nrl = slf.borrow().lock_node_ref_lists().unwrap_or(0);

        // Full index scan into a {key: value[, refs]} dict, then delegate the
        // prefix matching to the shared index helper (matches the Python path).
        let nodes = PyDict::new(py);
        for entry in Self::iter_all_entries(slf, py)?.bind(py).try_iter()? {
            let tup = entry?.cast_into::<PyTuple>()?;
            let key = tup.get_item(1)?;
            let value = tup.get_item(2)?;
            if nrl > 0 {
                let refs = tup.get_item(3)?;
                nodes.set_item(key, PyTuple::new(py, [value, refs])?)?;
            } else {
                nodes.set_item(key, value)?;
            }
        }
        let mode = if nrl > 0 {
            "reader-refs"
        } else {
            "reader-norefs"
        };
        let keys_list = PyList::empty(py);
        for p in prefix_set.iter() {
            keys_list.append(p)?;
        }
        keys_list.call_method1("sort", ())?;
        let entries = crate::index::py_iter_entries_prefix(
            py,
            nodes.clone(),
            keys_list.into_any(),
            key_length,
            mode,
        )?;
        let self_any = slf.clone().into_any();
        for entry in entries.iter() {
            let tup = entry.cast_into::<PyTuple>()?;
            let mut items: Vec<Bound<'py, PyAny>> = vec![self_any.clone()];
            for it in tup.iter() {
                items.push(it);
            }
            out.append(PyTuple::new(py, items)?)?;
        }
        Ok(out)
    }

    /// Find the ancestry of `keys`. Populates `parent_map`/`missing_keys`
    /// and returns parent keys still needing a follow-up search.
    fn _find_ancestors<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        keys: Bound<'py, PyAny>,
        ref_list_num: usize,
        parent_map: Bound<'py, PyDict>,
        missing_keys: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PySet>> {
        if Self::key_count(slf, py)? == 0 {
            for k in keys.try_iter()? {
                missing_keys.call_method1("add", (k?,))?;
            }
            return PySet::empty(py);
        }
        let nrl = slf.borrow().lock_node_ref_lists().unwrap_or(0);
        if ref_list_num >= nrl {
            return Err(PyValueError::new_err(format!(
                "No ref list {ref_list_num}, index has {nrl} ref lists"
            )));
        }
        let key_set = PySet::empty(py)?;
        for k in keys.try_iter()? {
            key_set.add(k?)?;
        }
        let (nodes, nodes_and_keys) = Self::walk_through_internal_nodes(slf, py, &key_set)?;
        let parents_not_on_page = PySet::empty(py)?;
        for (node_index, sub_keys) in nodes_and_keys {
            let sub_keys = sub_keys.bind(py);
            if sub_keys.is_empty() {
                continue;
            }
            let node = nodes
                .get_item(node_index)?
                .ok_or_else(|| PyValueError::new_err(format!("missing leaf {node_index}")))?;
            let parents_to_check = PySet::empty(py)?;
            for sk in sub_keys.try_iter()? {
                let sk = sk?;
                if !node.contains(&sk)? {
                    missing_keys.call_method1("add", (sk,))?;
                } else {
                    let value_refs = node.get_item(&sk)?.cast_into::<PyTuple>()?;
                    let parent_keys = value_refs.get_item(1)?.cast_into::<PyTuple>()?;
                    let parent_keys = parent_keys.get_item(ref_list_num)?;
                    parent_map.set_item(&sk, &parent_keys)?;
                    parents_to_check.call_method1("update", (parent_keys,))?;
                }
            }
            // Don't look for things we've already found.
            let mut to_check = parents_to_check.call_method1("difference", (&parent_map,))?;
            while to_check.is_truthy()? {
                let next = PySet::empty(py)?;
                for key in to_check.try_iter()? {
                    let key = key?;
                    if node.contains(&key)? {
                        let value_refs = node.get_item(&key)?.cast_into::<PyTuple>()?;
                        let parent_keys = value_refs.get_item(1)?.cast_into::<PyTuple>()?;
                        let parent_keys = parent_keys.get_item(ref_list_num)?;
                        parent_map.set_item(&key, &parent_keys)?;
                        next.call_method1("update", (parent_keys,))?;
                    } else {
                        let min_key = node.getattr("min_key")?;
                        let max_key = node.getattr("max_key")?;
                        if key.lt(&min_key)? || key.gt(&max_key)? {
                            parents_not_on_page.add(&key)?;
                        } else {
                            missing_keys.call_method1("add", (key,))?;
                        }
                    }
                }
                to_check = next.call_method1("difference", (&parent_map,))?;
            }
        }
        // Cull parents we've already accounted for.
        let search = parents_not_on_page.call_method1("difference", (&parent_map,))?;
        let search = search.call_method1("difference", (&missing_keys,))?;
        search.cast_into::<PySet>().map_err(Into::into)
    }

    #[staticmethod]
    fn _multi_bisect_right<'py>(
        py: Python<'py>,
        in_keys: Bound<'py, PyAny>,
        fixed_keys: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        // Operates on arbitrary orderable Python keys (tests use str), so the
        // comparison/bisection is done over Python objects rather than the
        // byte-key crate helper.
        let in_vec: Vec<Bound<'py, PyAny>> = in_keys.try_iter()?.collect::<PyResult<_>>()?;
        let fixed_vec: Vec<Bound<'py, PyAny>> = fixed_keys.try_iter()?.collect::<PyResult<_>>()?;
        let out = PyList::empty(py);
        if in_vec.is_empty() {
            return Ok(out);
        }
        if fixed_vec.is_empty() {
            out.append(PyTuple::new(
                py,
                [0usize.into_pyobject(py)?.into_any(), {
                    let l = PyList::empty(py);
                    for k in &in_vec {
                        l.append(k)?;
                    }
                    l.into_any()
                }],
            )?)?;
            return Ok(out);
        }
        if in_vec.len() == 1 {
            // bisect_right: first position where fixed[pos] > in_key.
            let mut pos = fixed_vec.len();
            for (i, fk) in fixed_vec.iter().enumerate() {
                if fk.gt(&in_vec[0])? {
                    pos = i;
                    break;
                }
            }
            let l = PyList::empty(py);
            l.append(&in_vec[0])?;
            out.append(PyTuple::new(
                py,
                [pos.into_pyobject(py)?.into_any(), l.into_any()],
            )?)?;
            return Ok(out);
        }
        // Two-pointer walk over Python keys, mirroring the reference.
        let mut in_iter = in_vec.iter();
        let mut fixed_iter = fixed_vec.iter().enumerate();
        let mut cur_in = in_iter.next().unwrap().clone();
        let (mut cur_fixed_offset, mut cur_fixed_key) = {
            let (o, k) = fixed_iter.next().unwrap();
            (o, k.clone())
        };
        #[derive(PartialEq)]
        enum Done {
            Input,
            Fixed,
        }
        let done: Done = 'outer: loop {
            if cur_in.lt(&cur_fixed_key)? {
                let bucket = PyList::empty(py);
                let pos = cur_fixed_offset;
                while cur_in.lt(&cur_fixed_key)? {
                    bucket.append(&cur_in)?;
                    match in_iter.next() {
                        Some(k) => cur_in = k.clone(),
                        None => {
                            out.append(PyTuple::new(
                                py,
                                [pos.into_pyobject(py)?.into_any(), bucket.into_any()],
                            )?)?;
                            break 'outer Done::Input;
                        }
                    }
                }
                out.append(PyTuple::new(
                    py,
                    [pos.into_pyobject(py)?.into_any(), bucket.into_any()],
                )?)?;
            }
            while cur_in.ge(&cur_fixed_key)? {
                match fixed_iter.next() {
                    Some((o, k)) => {
                        cur_fixed_offset = o;
                        cur_fixed_key = k.clone();
                    }
                    None => break 'outer Done::Fixed,
                }
            }
        };
        if done == Done::Fixed {
            let bucket = PyList::empty(py);
            bucket.append(&cur_in)?;
            for k in in_iter {
                bucket.append(k)?;
            }
            out.append(PyTuple::new(
                py,
                [
                    fixed_vec.len().into_pyobject(py)?.into_any(),
                    bucket.into_any(),
                ],
            )?)?;
        }
        Ok(out)
    }

    /// Ensure the header (and the root node, when one exists) has been read.
    /// Empty indices have no root page; in that case the header is parsed
    /// and `root_node` stays `None`. Returns the root node if present.
    fn _get_root_node(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        if slf.borrow().root_node.lock().unwrap().is_none() {
            Self::_get_internal_nodes(slf, py, vec![0])?;
        }
        Ok(slf.borrow()._root_node(py))
    }

    fn _get_internal_nodes<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        node_indexes: Vec<usize>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let cache = slf
            .borrow()
            .internal_node_cache
            .lock()
            .unwrap()
            .clone_ref(py);
        Self::get_nodes(slf, py, &cache.bind(py).clone(), node_indexes)
    }

    fn _get_leaf_nodes<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        node_indexes: Vec<usize>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let cache = slf.borrow().leaf_node_cache.lock().unwrap().clone_ref(py);
        Self::get_nodes(slf, py, &cache.bind(py).clone(), node_indexes)
    }

    fn _read_nodes<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        nodes: Vec<usize>,
    ) -> PyResult<Bound<'py, PyList>> {
        Self::read_nodes_impl(slf, py, nodes)
    }

    fn _parse_header_from_bytes<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        data: &[u8],
    ) -> PyResult<(usize, Bound<'py, PyBytes>)> {
        let header = parse_btree_header(data).map_err(|e| match e {
            BTreeIndexError::BadSignature => {
                BadIndexFormatSignature::new_err(("", "BTreeGraphIndex"))
            }
            BTreeIndexError::BadOptions => BadIndexOptions::new_err(("",)),
            other => PyValueError::new_err(other.to_string()),
        })?;
        let me = slf.borrow();
        *me.node_ref_lists.lock().unwrap() = Some(header.node_ref_lists);
        *me.key_length.lock().unwrap() = Some(header.key_length);
        *me.key_count.lock().unwrap() = Some(header.key_count);
        *me.row_offsets.lock().unwrap() = Some(compute_row_offsets(&header.row_lengths));
        *me.row_lengths.lock().unwrap() = Some(header.row_lengths);
        let rest = PyBytes::new(py, &data[header.header_end..]);
        Ok((header.header_end, rest))
    }
}

/// Pull nodes from `cache`, reading any missing ones (expanded for prefetch)
/// from the transport. Mirrors `_get_nodes` + `_get_and_cache_nodes`.
impl BTreeGraphIndex {
    fn get_nodes<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        cache: &Bound<'py, PyAny>,
        node_indexes: Vec<usize>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let found = PyDict::new(py);
        let mut needed: Vec<usize> = Vec::new();
        let root = slf.borrow()._root_node(py);
        for idx in node_indexes {
            if idx == 0 {
                if let Some(r) = &root {
                    found.set_item(0usize, r.bind(py).clone())?;
                    continue;
                }
            }
            match cache.call_method1("__getitem__", (idx,)) {
                Ok(node) => {
                    found.set_item(idx, node)?;
                }
                Err(e) if e.is_instance_of::<PyKeyError>(py) => needed.push(idx),
                Err(e) => return Err(e),
            }
        }
        if needed.is_empty() {
            return Ok(found);
        }
        let expanded = Self::_expand_offsets(slf, py, needed)?;
        let expanded_vec: Vec<usize> = expanded.extract()?;
        let fetched = Self::get_and_cache_nodes(slf, py, expanded_vec)?;
        found.call_method1("update", (fetched,))?;
        Ok(found)
    }

    fn get_and_cache_nodes<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        nodes: Vec<usize>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let found = PyDict::new(py);
        let mut sorted = nodes;
        sorted.sort_unstable();
        let read = Self::read_nodes_impl(slf, py, sorted)?;
        let leaf_cache = slf.borrow().leaf_node_cache.lock().unwrap().clone_ref(py);
        let internal_cache = slf
            .borrow()
            .internal_node_cache
            .lock()
            .unwrap()
            .clone_ref(py);
        let mut start_of_leaves: Option<usize> = None;
        for pair in read.try_iter()? {
            let tup = pair?.cast_into::<PyTuple>()?;
            let node_pos: usize = tup.get_item(0)?.extract()?;
            let node = tup.get_item(1)?;
            if node_pos == 0 {
                slf.borrow().set_root_node(Some(node.clone().unbind()));
            } else {
                if start_of_leaves.is_none() {
                    let ro = slf.borrow().lock_row_offsets().unwrap_or_default();
                    start_of_leaves = Some(ro[ro.len() - 2]);
                }
                if node_pos < start_of_leaves.unwrap() {
                    internal_cache
                        .bind(py)
                        .call_method1("__setitem__", (node_pos, &node))?;
                } else {
                    leaf_cache
                        .bind(py)
                        .call_method1("__setitem__", (node_pos, &node))?;
                }
            }
            found.set_item(node_pos, node)?;
        }
        Ok(found)
    }

    /// Walk internal nodes to map each requested key to the leaf covering it.
    /// Returns `(leaf_nodes_dict, [(leaf_index, sub_keys_list)])`.
    fn walk_through_internal_nodes<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        keys: &Bound<'py, PySet>,
    ) -> PyResult<(Bound<'py, PyDict>, KeysAtIndex)> {
        let sorted = PyList::empty(py);
        for k in keys.iter() {
            sorted.append(k)?;
        }
        sorted.call_method1("sort", ())?;
        let mut keys_at_index: Vec<(usize, Py<PyList>)> = vec![(0, sorted.unbind())];

        let row_offsets = slf.borrow().lock_row_offsets().unwrap_or_default();
        let mid_rows: Vec<usize> = if row_offsets.len() >= 2 {
            row_offsets[1..row_offsets.len() - 1].to_vec()
        } else {
            Vec::new()
        };
        for next_row_start in mid_rows {
            let node_indexes: Vec<usize> = keys_at_index.iter().map(|(i, _)| *i).collect();
            let nodes = Self::_get_internal_nodes(slf, py, node_indexes)?;
            let mut next: Vec<(usize, Py<PyList>)> = Vec::new();
            for (node_index, sub_keys) in keys_at_index.into_iter() {
                let node = nodes.get_item(node_index)?.ok_or_else(|| {
                    PyValueError::new_err(format!("missing internal node {node_index}"))
                })?;
                let node_offset: usize =
                    next_row_start + node.getattr("offset")?.extract::<usize>()?;
                let node_keys = node.getattr("keys")?;
                let positions =
                    Self::_multi_bisect_right(py, sub_keys.bind(py).clone().into_any(), node_keys)?;
                for entry in positions.iter() {
                    let tup = entry.cast_into::<PyTuple>()?;
                    let pos: usize = tup.get_item(0)?.extract()?;
                    let s_keys = tup.get_item(1)?.cast_into::<PyList>()?;
                    next.push((node_offset + pos, s_keys.unbind()));
                }
            }
            keys_at_index = next;
        }
        let leaf_indexes: Vec<usize> = keys_at_index.iter().map(|(i, _)| *i).collect();
        let nodes = Self::_get_leaf_nodes(slf, py, leaf_indexes)?;
        Ok((nodes, keys_at_index))
    }

    fn read_nodes_impl<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        pages: Vec<usize>,
    ) -> PyResult<Bound<'py, PyList>> {
        let ps = page_size();
        let base_offset = slf.borrow().base_offset;
        let size = slf.borrow().lock_size();
        let plan = bazaar::btree_index::plan_page_reads(&pages, size, base_offset, ps)
            .map_err(pyo3::exceptions::PyAssertionError::new_err)?;

        let out = PyList::empty(py);
        // (offset, data) pairs to decode.
        let data_ranges = PyList::empty(py);
        match plan {
            ReadPlan::WholeFile => {
                let transport = slf.borrow().transport.clone_ref(py);
                let data: Bound<'py, PyBytes> = transport
                    .bind(py)
                    .call_method1("get_bytes", (&slf.borrow().name,))?
                    .cast_into()?;
                let bytes = data.as_bytes();
                let num_bytes = bytes.len() as u64;
                slf.borrow().set_size(Some(num_bytes - base_offset));
                let mut start = base_offset;
                while start < num_bytes {
                    let take = (ps as u64).min(num_bytes - start);
                    let chunk = PyBytes::new(py, &bytes[start as usize..(start + take) as usize]);
                    data_ranges.append(PyTuple::new(
                        py,
                        [start.into_pyobject(py)?.into_any(), chunk.into_any()],
                    )?)?;
                    start += ps as u64;
                }
            }
            ReadPlan::Ranges(ranges) => {
                if ranges.is_empty() {
                    return Ok(out);
                }
                let file = slf.borrow()._file(py);
                if let Some(file) = file {
                    // Spilled-backing path: read directly from the open file.
                    for PageRange { offset, length } in &ranges {
                        file.bind(py).call_method1("seek", (*offset,))?;
                        let chunk = file.bind(py).call_method1("read", (*length,))?;
                        data_ranges.append(PyTuple::new(
                            py,
                            [offset.into_pyobject(py)?.into_any(), chunk],
                        )?)?;
                    }
                } else {
                    // Normal path: readv with the two positional args the
                    // tracing tests assert on (no extra kwargs).
                    let py_ranges = PyList::empty(py);
                    for PageRange { offset, length } in &ranges {
                        py_ranges.append(PyTuple::new(py, [*offset, *length])?)?;
                    }
                    let transport = slf.borrow().transport.clone_ref(py);
                    let read = transport
                        .bind(py)
                        .call_method1("readv", (&slf.borrow().name, py_ranges))?;
                    for item in read.try_iter()? {
                        data_ranges.append(item?)?;
                    }
                }
            }
        }

        let leaf_factory = slf.borrow().leaf_factory.lock().unwrap().clone_ref(py);
        for item in data_ranges.iter() {
            let tup = item.cast_into::<PyTuple>()?;
            let mut offset: u64 = tup.get_item(0)?.extract()?;
            let data: Bound<'py, PyBytes> = tup.get_item(1)?.cast_into()?;
            offset -= base_offset;
            let payload: Vec<u8> = if offset == 0 {
                let (_he, rest) = Self::_parse_header_from_bytes(slf, py, data.as_bytes())?;
                let rest_bytes = rest.as_bytes().to_vec();
                if rest_bytes.is_empty() {
                    continue;
                }
                rest_bytes
            } else {
                data.as_bytes().to_vec()
            };
            let decompressed = decompress_page(&payload)
                .map_err(|e| PyValueError::new_err(format!("bad btree node: {e}")))?;
            let key_length = slf.borrow()._key_length().unwrap_or(1);
            let nrl = slf.borrow().lock_node_ref_lists().unwrap_or(0);
            let node: Bound<'py, PyAny> = if decompressed.starts_with(LEAF_FLAG) {
                let bytes = PyBytes::new(py, &decompressed);
                leaf_factory.bind(py).call1((bytes, key_length, nrl))?
            } else if decompressed.starts_with(INTERNAL_FLAG) {
                let bytes = PyBytes::new(py, &decompressed);
                Bound::new(py, InternalNodePy::new(py, bytes.as_bytes())?)?.into_any()
            } else {
                return Err(pyo3::exceptions::PyAssertionError::new_err(format!(
                    "Unknown node type for {decompressed:?}"
                )));
            };
            let page_index = offset as usize / ps;
            out.append(PyTuple::new(
                py,
                [page_index.into_pyobject(py)?.into_any(), node],
            )?)?;
        }
        Ok(out)
    }

    fn make_iter(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        entries: Bound<'_, PyList>,
    ) -> PyResult<Py<EntryIterator>> {
        let _ = slf;
        Py::new(
            py,
            EntryIterator {
                entries: entries.unbind(),
                pos: Mutex::new(0),
            },
        )
    }
}

/// Append all entries from a leaf node object (built via `_leaf_factory`)
/// to `out`, each prefixed with the owning index.
fn append_node_entries<'py>(
    py: Python<'py>,
    out: &Bound<'py, PyList>,
    index: &Bound<'py, PyAny>,
    node: &Bound<'py, PyAny>,
    node_ref_lists: usize,
) -> PyResult<()> {
    for item in node.call_method0("all_items")?.try_iter()? {
        let tup = item?.cast_into::<PyTuple>()?;
        let key = tup.get_item(0)?;
        let value_refs = tup.get_item(1)?.cast_into::<PyTuple>()?;
        append_entry(py, out, index, &key, &value_refs, node_ref_lists)?;
    }
    Ok(())
}

/// Append one `(index, key, value[, refs])` tuple to `out`.
fn append_entry<'py>(
    py: Python<'py>,
    out: &Bound<'py, PyList>,
    index: &Bound<'py, PyAny>,
    key: &Bound<'py, PyAny>,
    value_refs: &Bound<'py, PyTuple>,
    node_ref_lists: usize,
) -> PyResult<()> {
    let value = value_refs.get_item(0)?;
    let tuple = if node_ref_lists > 0 {
        let refs = value_refs.get_item(1)?;
        PyTuple::new(py, [index.clone(), key.clone(), value, refs])?
    } else {
        PyTuple::new(py, [index.clone(), key.clone(), value])?
    };
    out.append(tuple)
}

/// Iterator over pre-built `(index, key, value[, refs])` tuples.
#[pyclass(module = "bzrformats._bzr_rs.btree_index")]
struct EntryIterator {
    entries: Py<PyList>,
    pos: Mutex<usize>,
}

#[pymethods]
impl EntryIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let mut pos = self.pos.lock().unwrap();
        let entries = self.entries.bind(py);
        if *pos >= entries.len() {
            return Err(PyStopIteration::new_err(()));
        }
        let item = entries.get_item(*pos)?;
        *pos += 1;
        Ok(item)
    }
}

/// Lazy iterator returned by `iter_entries_prefix`. The prefix validation
/// and full index scan run on first `__next__` so the historic generator
/// semantics hold (the work, including a possible `BadIndexKey`, happens
/// during iteration rather than at call time).
#[pyclass(module = "bzrformats._bzr_rs.btree_index")]
struct PrefixIterator {
    index: Py<BTreeGraphIndex>,
    prefixes: Py<PyList>,
    computed: Mutex<Option<Py<PyList>>>,
    pos: Mutex<usize>,
}

#[pymethods]
impl PrefixIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let mut computed = self.computed.lock().unwrap();
        if computed.is_none() {
            let entries = BTreeGraphIndex::iter_entries_prefix_impl(
                self.index.bind(py),
                py,
                self.prefixes.bind(py).clone().into_any(),
            )?;
            *computed = Some(entries.unbind());
        }
        let entries = computed.as_ref().unwrap().bind(py);
        let mut pos = self.pos.lock().unwrap();
        if *pos >= entries.len() {
            return Err(PyStopIteration::new_err(()));
        }
        let item = entries.get_item(*pos)?;
        *pos += 1;
        Ok(item)
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
        // Existing on-disk indices sort before still-being-built ones.
        // `bzrformats.btree_index.BTreeGraphIndex` is an alias for this Rust
        // pyclass, so this instance check also covers spilled backings
        // constructed via the Python name.
        if other.is_instance_of::<BTreeGraphIndex>() {
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
    // Equivalent to Python's `tuple(obj)`: materialise the iterable. A
    // non-iterable raises the same `TypeError` via `try_iter`.
    let items = obj
        .try_iter()?
        .collect::<PyResult<Vec<Bound<'py, PyAny>>>>()?;
    PyTuple::new(py, items)
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

        // Build a BTreeGraphIndex over a dummy transport that returns a
        // fixed recommended_page_size. The transport itself is never used
        // for I/O because we overwrite `_file` to point at the
        // just-written tempfile.
        let dummy_transport = Py::new(py, DummyTransport)?;
        let new_backing =
            py.get_type::<BTreeGraphIndex>()
                .call1((dummy_transport, "<temp>", size))?;
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
        let page_size = bazaar::btree_index::PAGE_SIZE;
        let blob = crate::btree_serializer::serialize_btree_index(
            py,
            &node_iterator,
            reference_lists,
            key_length,
            optimize_for_size,
            Some(page_size),
            Some(bazaar::btree_index::RESERVED_HEADER_BYTES),
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
    m.add_class::<PrefixIterator>()?;
    m.add_class::<BTreeBuilder>()?;
    m.add_class::<LeafNodePy>()?;
    m.add_class::<InternalNodePy>()?;
    Ok(m)
}
