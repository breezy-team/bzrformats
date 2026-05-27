use bazaar::chk_map::{
    are_search_keys_identical, deserialise_internal_node, deserialise_leaf_node,
    internal_node_current_size, leaf_node_current_size, leaf_node_key_value_len,
    serialise_internal_node, serialise_leaf_node, Error as ChkError, InternalNodeChild, Key,
    LeafNode as RsLeafNode, SearchKeyFunc, SearchPrefix,
};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};
use pyo3::wrap_pyfunction;

fn chk_err_to_py(err: ChkError) -> PyErr {
    match err {
        ChkError::DeserializeError(msg) => pyo3::exceptions::PyValueError::new_err(msg),
        ChkError::InconsistentDeltaDelta(_, msg) => pyo3::exceptions::PyValueError::new_err(msg),
        ChkError::AssertionFailed(msg) => pyo3::exceptions::PyAssertionError::new_err(msg),
    }
}

#[pyfunction]
fn _search_key_plain(py: Python, key: Vec<Vec<u8>>) -> Bound<PyBytes> {
    let key: Key = key.into();
    let ret = bazaar::chk_map::search_key_plain(&key);
    PyBytes::new(py, &ret)
}

#[pyfunction]
fn _search_key_16(py: Python, key: Vec<Vec<u8>>) -> Bound<PyBytes> {
    let key: Key = key.into();
    let ret = bazaar::chk_map::search_key_16(&key);
    PyBytes::new(py, &ret)
}

#[pyfunction]
fn _search_key_255(py: Python, key: Vec<Vec<u8>>) -> Bound<PyBytes> {
    let key: Key = key.into();
    let ret = bazaar::chk_map::search_key_255(&key);
    PyBytes::new(py, &ret)
}

#[pyfunction]
fn _bytes_to_text_key(py: Python, key: Vec<u8>) -> PyResult<(Bound<PyBytes>, Bound<PyBytes>)> {
    let ret = bazaar::chk_map::bytes_to_text_key(key.as_slice());
    if ret.is_err() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Invalid key",
        ));
    }
    let ret = ret.unwrap();
    Ok((PyBytes::new(py, ret.0), PyBytes::new(py, ret.1)))
}

#[pyfunction]
fn common_prefix_pair<'a>(py: Python<'a>, key: &'a [u8], key2: &'a [u8]) -> Bound<'a, PyBytes> {
    PyBytes::new(py, bazaar::chk_map::common_prefix_pair(key, key2))
}

#[pyfunction]
fn common_prefix_many(py: Python, keys: Vec<Vec<u8>>) -> Option<Bound<PyBytes>> {
    let keys = keys.iter().map(|v| v.as_slice()).collect::<Vec<&[u8]>>();
    bazaar::chk_map::common_prefix_many(keys.into_iter())
        .as_ref()
        .map(|v| PyBytes::new(py, v))
}

/// Deserialise a CHK leaf node body. Returns
/// `(maximum_size, key_width, length, common_serialised_prefix, items, raw_size)`
/// where `items` is a list of `(key_tuple, value)` pairs in file order.
#[pyfunction]
#[pyo3(name = "_deserialise_leaf_node")]
#[allow(clippy::type_complexity)]
fn py_deserialise_leaf_node<'py>(
    py: Python<'py>,
    data: &[u8],
) -> PyResult<(
    usize,
    usize,
    usize,
    Bound<'py, PyBytes>,
    Bound<'py, PyList>,
    usize,
)> {
    let p = deserialise_leaf_node(data).map_err(chk_err_to_py)?;
    let items = PyList::empty(py);
    for (key_elements, value) in &p.items {
        let key_parts: Vec<Bound<PyBytes>> =
            key_elements.iter().map(|e| PyBytes::new(py, e)).collect();
        let key_tuple = PyTuple::new(py, key_parts)?;
        let pair = PyTuple::new(
            py,
            [key_tuple.into_any(), PyBytes::new(py, value).into_any()],
        )?;
        items.append(pair)?;
    }
    Ok((
        p.maximum_size,
        p.key_width,
        p.length,
        PyBytes::new(py, &p.common_serialised_prefix),
        items,
        p.raw_size,
    ))
}

/// Deserialise a CHK internal node body. Returns
/// `(maximum_size, key_width, length, search_prefix, items, node_width)`
/// where `items` is a list of `(prefix_bytes, flat_key_bytes)` pairs.
#[pyfunction]
#[pyo3(name = "_deserialise_internal_node")]
#[allow(clippy::type_complexity)]
fn py_deserialise_internal_node<'py>(
    py: Python<'py>,
    data: &[u8],
) -> PyResult<(
    usize,
    usize,
    usize,
    Bound<'py, PyBytes>,
    Bound<'py, PyList>,
    usize,
)> {
    let p = deserialise_internal_node(data).map_err(chk_err_to_py)?;
    let items = PyList::empty(py);
    for (prefix, flat_key) in &p.items {
        let pair = PyTuple::new(
            py,
            [
                PyBytes::new(py, prefix).into_any(),
                PyBytes::new(py, flat_key).into_any(),
            ],
        )?;
        items.append(pair)?;
    }
    Ok((
        p.maximum_size,
        p.key_width,
        p.length,
        PyBytes::new(py, &p.search_prefix),
        items,
        p.node_width,
    ))
}

/// Build the line list that `LeafNode.serialise` would hand to
/// `store.add_lines(...)`. `items` is a list of `(key_tuple, value)`
/// pairs in already-sorted order; `common_prefix` is `None` only for the
/// empty-node case.
#[pyfunction]
#[pyo3(name = "_serialise_leaf_node", signature = (maximum_size, key_width, items, common_prefix))]
fn py_serialise_leaf_node<'py>(
    py: Python<'py>,
    maximum_size: usize,
    key_width: usize,
    items: Bound<'py, PyAny>,
    common_prefix: Option<&[u8]>,
) -> PyResult<Bound<'py, PyList>> {
    let mut rust_items: Vec<(Vec<Vec<u8>>, Vec<u8>)> = Vec::new();
    for pair in items.try_iter()? {
        let pair = pair?.cast_into::<PyTuple>()?;
        let key_tuple = pair.get_item(0)?.cast_into::<PyTuple>()?;
        let mut key_parts: Vec<Vec<u8>> = Vec::with_capacity(key_tuple.len());
        for part in key_tuple.iter() {
            key_parts.push(part.cast_into::<PyBytes>()?.as_bytes().to_vec());
        }
        let value = pair
            .get_item(1)?
            .cast_into::<PyBytes>()?
            .as_bytes()
            .to_vec();
        rust_items.push((key_parts, value));
    }
    let out = serialise_leaf_node(maximum_size, key_width, &rust_items, common_prefix)
        .map_err(chk_err_to_py)?;
    let lines = PyList::empty(py);
    for line in out {
        lines.append(PyBytes::new(py, &line))?;
    }
    Ok(lines)
}

/// Build the line list that `InternalNode.serialise` would hand to
/// `store.add_lines(...)`. `items` is a list of `(prefix, flat_key)`
/// pairs in already-sorted order. `length` is the InternalNode's
/// total leaf count (`self._len`), not the direct fan-out.
#[pyfunction]
#[pyo3(name = "_serialise_internal_node")]
fn py_serialise_internal_node<'py>(
    py: Python<'py>,
    maximum_size: usize,
    key_width: usize,
    length: usize,
    search_prefix: &[u8],
    items: Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyList>> {
    let mut rust_items: Vec<InternalNodeChild> = Vec::new();
    for pair in items.try_iter()? {
        let pair = pair?.cast_into::<PyTuple>()?;
        let prefix = pair
            .get_item(0)?
            .cast_into::<PyBytes>()?
            .as_bytes()
            .to_vec();
        let flat_key = pair
            .get_item(1)?
            .cast_into::<PyBytes>()?
            .as_bytes()
            .to_vec();
        rust_items.push(InternalNodeChild { prefix, flat_key });
    }
    let out = serialise_internal_node(maximum_size, key_width, length, search_prefix, &rust_items)
        .map_err(chk_err_to_py)?;
    let lines = PyList::empty(py);
    for line in out {
        lines.append(PyBytes::new(py, &line))?;
    }
    Ok(lines)
}

/// Serialised byte cost of one `(key, value)` pair inside a leaf node.
/// Mirrors `LeafNode._key_value_len`.
#[pyfunction]
#[pyo3(name = "_leaf_node_key_value_len")]
fn py_leaf_node_key_value_len(key: &Bound<'_, PyTuple>, value: &[u8]) -> PyResult<usize> {
    let mut parts: Vec<Vec<u8>> = Vec::with_capacity(key.len());
    for i in 0..key.len() {
        parts.push(key.get_item(i)?.cast_into::<PyBytes>()?.as_bytes().to_vec());
    }
    Ok(leaf_node_key_value_len(&parts, value))
}

/// Serialised byte cost of a leaf node (header + items, with prefix
/// collapse). Mirrors `LeafNode._current_size`.
#[pyfunction]
#[pyo3(name = "_leaf_node_current_size", signature = (maximum_size, key_width, length, raw_size, common_serialised_prefix))]
fn py_leaf_node_current_size(
    maximum_size: usize,
    key_width: usize,
    length: usize,
    raw_size: usize,
    common_serialised_prefix: Option<&[u8]>,
) -> usize {
    leaf_node_current_size(
        maximum_size,
        key_width,
        length,
        raw_size,
        common_serialised_prefix,
    )
}

/// Serialised byte cost of an internal node header + body.
/// Mirrors `InternalNode._current_size`.
#[pyfunction]
#[pyo3(name = "_internal_node_current_size")]
fn py_internal_node_current_size(
    maximum_size: usize,
    key_width: usize,
    length: usize,
    raw_size: usize,
) -> usize {
    internal_node_current_size(maximum_size, key_width, length, raw_size)
}

/// Module-level `_unknown` sentinel. Python's `chk_map._unknown` is a
/// plain `object()` used for identity comparison; we expose the same
/// object via this module so the Rust mutators can return it when the
/// search prefix is in the "needs recompute" state.
static UNKNOWN_SENTINEL: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

fn unknown_sentinel(py: Python<'_>) -> &Py<PyAny> {
    UNKNOWN_SENTINEL.get_or_init(py, || {
        // `object()` — identity-only, no other behaviour required.
        py.import("builtins")
            .unwrap()
            .getattr("object")
            .unwrap()
            .call0()
            .unwrap()
            .unbind()
    })
}

#[pyfunction]
#[pyo3(name = "_unknown_sentinel")]
fn py_unknown_sentinel(py: Python<'_>) -> Py<PyAny> {
    unknown_sentinel(py).clone_ref(py)
}

/// Default `search_key_func` callable for LeafNode/InternalNode/CHKMap
/// pyclasses. Filled in at module-init time with the
/// `_search_key_plain` pyfunction so the pyclass `#[new]` can stash
/// it on instances constructed with `search_key_func=None`.
static DEFAULT_SEARCH_KEY_PLAIN: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

fn default_search_key_plain(py: Python<'_>) -> &Py<PyAny> {
    DEFAULT_SEARCH_KEY_PLAIN
        .get(py)
        .expect("DEFAULT_SEARCH_KEY_PLAIN not initialised; call _chk_map_rs(py) first")
}

/// Lazily-initialised pyfunction callables for the three registered
/// search-key variants. Populated by `_chk_map_rs(py)` at module load.
static SEARCH_KEY_16_CALLABLE: PyOnceLock<Py<PyAny>> = PyOnceLock::new();
static SEARCH_KEY_255_CALLABLE: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

/// Resolve a `search_key_name` (b"plain" / b"hash-16-way" /
/// b"hash-255-way") to the matching Python callable. Returns
/// `None` for unknown names.
///
/// Available cross-module (e.g. from `inventory.rs`) so siblings
/// don't need to `py.import("bzrformats._bzr_rs.chk_map")` to find
/// the registered callable for a given search-key variant.
pub(crate) fn search_key_callable_for_name<'py>(py: Python<'py>, name: &[u8]) -> Option<Py<PyAny>> {
    match name {
        b"plain" => Some(default_search_key_plain(py).clone_ref(py)),
        b"hash-16-way" => SEARCH_KEY_16_CALLABLE.get(py).map(|c| c.clone_ref(py)),
        b"hash-255-way" => SEARCH_KEY_255_CALLABLE.get(py).map(|c| c.clone_ref(py)),
        _ => None,
    }
}

/// Resolve a Python `_search_key_func` callable to a `SearchKeyFunc`.
///
/// Identifies built-in variants by their output on a one-element
/// Process-wide CHK page cache. Python originally used a per-thread
/// LRU keyed on the sha1 tuple; with the GIL there's at most one
/// active CHK reader at a time, so a single shared cache is
/// equivalent under the GIL and simpler to reason about.
static PAGE_CACHE: std::sync::OnceLock<bazaar::chk_map::InMemoryPageCache> =
    std::sync::OnceLock::new();

fn page_cache() -> &'static bazaar::chk_map::InMemoryPageCache {
    PAGE_CACHE.get_or_init(bazaar::chk_map::InMemoryPageCache::new)
}

/// Clear the process-wide CHK page cache. Mirrors Python's
/// `chk_map.clear_cache`.
#[pyfunction]
fn clear_cache() {
    use bazaar::chk_map::PageCache as _;
    page_cache().clear();
}

/// Look up `key` (a sha1 tuple) in the page cache. Returns `None`
/// on miss. Exposed so the few remaining Python orchestration
/// methods (`_internal_iter_nodes`, `_leaf_serialise`) can keep
/// hitting the same cache without going through a CHKMap instance.
#[pyfunction]
fn _page_cache_get<'py>(
    py: Python<'py>,
    key: Bound<'py, PyTuple>,
) -> PyResult<Option<Bound<'py, PyBytes>>> {
    use bazaar::chk_map::PageCache as _;
    let sha1: Vec<u8> = key
        .get_item(0)?
        .cast_into::<PyBytes>()?
        .as_bytes()
        .to_vec();
    Ok(page_cache().get(&sha1).map(|b| PyBytes::new(py, &b)))
}

/// Insert `value` into the page cache under `key`. Companion to
/// `_page_cache_get`.
#[pyfunction]
fn _page_cache_set(key: Bound<'_, PyTuple>, value: &[u8]) -> PyResult<()> {
    use bazaar::chk_map::PageCache as _;
    let sha1: Vec<u8> = key
        .get_item(0)?
        .cast_into::<PyBytes>()?
        .as_bytes()
        .to_vec();
    page_cache().insert(sha1, value.to_vec());
    Ok(())
}

/// fingerprint key whose `plain` / `hash-16` / `hash-255` outputs are
/// all distinct. Anything else becomes a [`SearchKeyFunc::Custom`]
/// wrapping a closure that calls back into Python — tests register
/// their own search-key functions, so the variant set isn't truly
/// closed.
fn resolve_search_key_func_by_callable(
    py: Python<'_>,
    callable: &Bound<'_, PyAny>,
) -> PyResult<SearchKeyFunc> {
    let fingerprint_key = PyTuple::new(py, [PyBytes::new(py, b"x")])?;
    let observed = callable.call1((fingerprint_key,))?;
    let observed_bytes = observed.cast_into::<PyBytes>()?;
    let observed = observed_bytes.as_bytes();
    let key = Key::from(vec![b"x".to_vec()]);
    for variant in [
        SearchKeyFunc::Plain,
        SearchKeyFunc::Hash16Way,
        SearchKeyFunc::Hash255Way,
    ] {
        if variant.apply(&key) == observed {
            return Ok(variant);
        }
    }
    // Custom callable — wrap it in a closure. The unbound `Py<PyAny>`
    // is `Send`/`Sync`; the closure reacquires the GIL on each call.
    let unbound: Py<PyAny> = callable.clone().unbind();
    let name: Vec<u8> = match callable.getattr("__name__") {
        Ok(n) => n
            .extract::<String>()
            .map(|s| s.into_bytes())
            .unwrap_or_else(|_| b"custom".to_vec()),
        Err(_) => b"custom".to_vec(),
    };
    Ok(SearchKeyFunc::Custom {
        name,
        func: std::sync::Arc::new(move |key: &Key| -> Vec<u8> {
            Python::attach(|py| {
                let parts: Vec<Bound<PyBytes>> = key.iter().map(|p| PyBytes::new(py, p)).collect();
                let key_tuple = PyTuple::new(py, parts).unwrap();
                let result = unbound.bind(py).call1((key_tuple,)).unwrap_or_else(|e| {
                    panic!(
                        "_search_key_func callback raised {}: {}",
                        e.get_type(py).qualname().unwrap(),
                        e
                    )
                });
                let bytes_obj = result
                    .cast_into::<PyBytes>()
                    .unwrap_or_else(|e| panic!("_search_key_func did not return bytes: {}", e));
                bytes_obj.as_bytes().to_vec()
            })
        }),
    })
}

/// Apply the named search-key transform to `key`. `name` selects one of
/// the registered variants — `b"plain"`, `b"hash-16-way"`, or
/// `b"hash-255-way"`. Returns a `KeyError` for unknown names to match
/// the behaviour of Python's `search_key_registry.get`.
#[pyfunction]
#[pyo3(name = "_search_key_by_name")]
fn py_search_key_by_name<'py>(
    py: Python<'py>,
    name: &[u8],
    key: Vec<Vec<u8>>,
) -> PyResult<Bound<'py, PyBytes>> {
    let func = SearchKeyFunc::from_name(name).map_err(|raw| {
        pyo3::exceptions::PyKeyError::new_err(format!("Unknown search key: {:?}", raw))
    })?;
    Ok(PyBytes::new(py, &func.apply(&Key::from(key))))
}

/// `LeafNode._are_search_keys_identical` — given the precomputed search
/// keys for every entry in the node, return True iff they are all equal.
/// An empty iterable returns True.
#[pyfunction]
#[pyo3(name = "_are_search_keys_identical")]
fn py_are_search_keys_identical(search_keys: Bound<'_, PyAny>) -> PyResult<bool> {
    let mut keys: Vec<Vec<u8>> = Vec::new();
    for key in search_keys.try_iter()? {
        keys.push(key?.cast_into::<PyBytes>()?.as_bytes().to_vec());
    }
    Ok(are_search_keys_identical(keys.iter()))
}

/// CHK leaf node — actual key/value storage.
///
/// Owns its state in Rust via `bazaar::chk_map::LeafNode`. The Python
/// `_search_key_func` attribute round-trips through whatever the caller
/// passed at construction (or `None`, which resolves to the plain
/// variant); internal algorithms always run against the resolved
/// `SearchKeyFunc` enum.
#[pyclass(module = "bzrformats._bzr_rs.chk_map", name = "LeafNode")]
pub struct LeafNode {
    inner: RsLeafNode,
    /// Original Python callable as passed in; preserved so the
    /// `_search_key_func` getter returns the same object the caller
    /// sees. `None` means the caller asked for the default
    /// (plain) variant — the getter then synthesises a callable.
    search_key_callable: Option<Py<PyAny>>,
}

#[pymethods]
impl LeafNode {
    #[new]
    #[pyo3(signature = (search_key_func = None))]
    fn new(py: Python<'_>, search_key_func: Option<Bound<'_, PyAny>>) -> PyResult<Self> {
        let (func, callable) = match search_key_func {
            None => (
                SearchKeyFunc::Plain,
                Some(default_search_key_plain(py).clone_ref(py)),
            ),
            Some(cb) => {
                let func = resolve_search_key_func_by_callable(py, &cb)?;
                (func, Some(cb.unbind()))
            }
        };
        Ok(Self {
            inner: RsLeafNode::new(func),
            search_key_callable: callable,
        })
    }

    fn __len__(&self) -> usize {
        self.inner.len()
    }

    fn __repr__(&self) -> String {
        // Mirror Python's LeafNode.__repr__: include key, len, size,
        // max, prefix, key_width, and a truncated items debug.
        let items_dbg = format!("{:?}", self.inner.items.keys().collect::<Vec<_>>());
        let items_short = if items_dbg.len() > 20 {
            format!("{}...]", &items_dbg[..16])
        } else {
            items_dbg
        };
        let key_dbg = match &self.inner.key {
            Some(k) => format!("({:?},)", String::from_utf8_lossy(k)),
            None => "None".to_string(),
        };
        let prefix_dbg = match &self.inner.search_prefix {
            SearchPrefix::Unknown => "<unknown>".to_string(),
            SearchPrefix::Computed(None) => "None".to_string(),
            SearchPrefix::Computed(Some(p)) => format!("{:?}", p),
        };
        format!(
            "LeafNode(key:{} len:{} size:{} max:{} prefix:{} keywidth:{} items:{})",
            key_dbg,
            self.inner.len(),
            self.inner.raw_size,
            self.inner.maximum_size,
            prefix_dbg,
            self.inner.key_width,
            items_short
        )
    }

    /// `(sha1_key,)` tuple once serialised, `None` while mutable.
    fn key<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyTuple>>> {
        match &self.inner.key {
            None => Ok(None),
            Some(k) => Ok(Some(PyTuple::new(py, [PyBytes::new(py, k)])?)),
        }
    }

    fn set_maximum_size(&mut self, new_size: usize) {
        self.inner.maximum_size = new_size;
    }

    #[getter]
    fn maximum_size(&self) -> usize {
        self.inner.maximum_size
    }

    /// Leaf nodes never reference other CHK pages — always `[]`.
    fn refs<'py>(&self, py: Python<'py>) -> Bound<'py, PyList> {
        PyList::empty(py)
    }

    // ----- whitebox state accessors -----

    #[getter]
    fn _key<'py>(&self, py: Python<'py>) -> PyResult<Py<PyAny>> {
        match &self.inner.key {
            None => Ok(py.None()),
            Some(k) => Ok(PyTuple::new(py, [PyBytes::new(py, k)])?.into_any().unbind()),
        }
    }

    #[setter]
    fn set__key(&mut self, py: Python<'_>, value: Bound<'_, PyAny>) -> PyResult<()> {
        if value.is_none() {
            self.inner.key = None;
        } else {
            let tup = value.cast_into::<PyTuple>()?;
            let first = tup.get_item(0)?;
            self.inner.key = Some(first.cast_into::<PyBytes>()?.as_bytes().to_vec());
            let _ = py;
        }
        Ok(())
    }

    #[getter]
    fn _len(&self) -> usize {
        self.inner.len()
    }

    #[setter]
    fn set__len(&mut self, value: usize) -> PyResult<()> {
        // `_len` mirrors `len(_items)`; the underlying IndexMap is
        // already authoritative. Writes are only accepted when they
        // match what the items dict actually contains, to catch
        // callers that drift out of sync.
        if value != self.inner.len() {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "LeafNode._len must match len(_items): tried to set {} but items has {}",
                value,
                self.inner.len()
            )));
        }
        Ok(())
    }

    #[getter]
    fn _maximum_size(&self) -> usize {
        self.inner.maximum_size
    }

    #[setter]
    fn set__maximum_size(&mut self, value: usize) {
        self.inner.maximum_size = value;
    }

    #[getter]
    fn _key_width(&self) -> usize {
        self.inner.key_width
    }

    #[setter]
    fn set__key_width(&mut self, value: usize) {
        self.inner.key_width = value;
    }

    #[getter]
    fn _raw_size(&self) -> usize {
        self.inner.raw_size
    }

    #[setter]
    fn set__raw_size(&mut self, value: usize) {
        self.inner.raw_size = value;
    }

    /// Materialise `_items` as a fresh `dict[tuple[bytes, ...], bytes]`
    /// each access. Mutations to the returned dict do *not* propagate
    /// back — callers that need to replace the contents assign a new
    /// dict to `_items`.
    #[getter]
    fn _items<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        for (k, v) in self.inner.items.iter() {
            let parts: Vec<Bound<PyBytes>> = k.iter().map(|p| PyBytes::new(py, p)).collect();
            let key_tuple = PyTuple::new(py, parts)?;
            dict.set_item(key_tuple, PyBytes::new(py, v))?;
        }
        Ok(dict)
    }

    /// Bulk-replace `_items`. Used by `CHKMap._create_directly`.
    #[setter]
    fn set__items(&mut self, value: Bound<'_, PyDict>) -> PyResult<()> {
        let mut items: indexmap::IndexMap<Vec<Vec<u8>>, Vec<u8>> =
            indexmap::IndexMap::with_capacity(value.len());
        for (k, v) in value.iter() {
            let key_tuple = k.cast_into::<PyTuple>()?;
            let mut parts: Vec<Vec<u8>> = Vec::with_capacity(key_tuple.len());
            for part in key_tuple.iter() {
                parts.push(part.cast_into::<PyBytes>()?.as_bytes().to_vec());
            }
            let value_bytes = v.cast_into::<PyBytes>()?.as_bytes().to_vec();
            items.insert(parts, value_bytes);
        }
        self.inner.items = items;
        Ok(())
    }

    /// Returns one of: `chk_map._unknown` (sentinel), `None` (empty
    /// node), or `bytes` (computed prefix).
    #[getter]
    fn _search_prefix<'py>(&self, py: Python<'py>) -> Py<PyAny> {
        match &self.inner.search_prefix {
            SearchPrefix::Unknown => unknown_sentinel(py).clone_ref(py),
            SearchPrefix::Computed(None) => py.None(),
            SearchPrefix::Computed(Some(p)) => PyBytes::new(py, p).into_any().unbind(),
        }
    }

    #[setter]
    fn set__search_prefix(&mut self, py: Python<'_>, value: Bound<'_, PyAny>) -> PyResult<()> {
        if value.is(unknown_sentinel(py)) {
            self.inner.search_prefix = SearchPrefix::Unknown;
        } else if value.is_none() {
            self.inner.search_prefix = SearchPrefix::Computed(None);
        } else {
            self.inner.search_prefix =
                SearchPrefix::Computed(Some(value.cast_into::<PyBytes>()?.as_bytes().to_vec()));
        }
        Ok(())
    }

    #[getter]
    fn _common_serialised_prefix<'py>(&self, py: Python<'py>) -> Py<PyAny> {
        match &self.inner.common_serialised_prefix {
            None => py.None(),
            Some(p) => PyBytes::new(py, p).into_any().unbind(),
        }
    }

    #[setter]
    fn set__common_serialised_prefix(&mut self, value: Bound<'_, PyAny>) -> PyResult<()> {
        if value.is_none() {
            self.inner.common_serialised_prefix = None;
        } else {
            self.inner.common_serialised_prefix =
                Some(value.cast_into::<PyBytes>()?.as_bytes().to_vec());
        }
        Ok(())
    }

    /// Returns the original callable passed at construction, or a
    /// synthesised wrapper around the resolved variant. Identity is
    /// Returns whatever callable was passed at construction, or
    /// `None` if the default plain variant was used. Python wrappers
    /// substitute their own default callable (the `_search_key_plain`
    /// function in bzrformats.chk_map) before reading this when a
    /// real callable is required.
    #[getter]
    fn _search_key_func<'py>(&self, py: Python<'py>) -> Py<PyAny> {
        match &self.search_key_callable {
            Some(cb) => cb.clone_ref(py),
            None => py.None(),
        }
    }

    #[setter]
    fn set__search_key_func(&mut self, py: Python<'_>, value: Bound<'_, PyAny>) -> PyResult<()> {
        if value.is_none() {
            self.inner.search_key_func = SearchKeyFunc::Plain;
            self.search_key_callable = None;
        } else {
            self.inner.search_key_func = resolve_search_key_func_by_callable(py, &value)?;
            self.search_key_callable = Some(value.unbind());
        }
        Ok(())
    }

    // ----- pure methods -----

    fn _current_size(&self) -> usize {
        self.inner.current_size()
    }

    fn _key_value_len(&self, key: &Bound<'_, PyTuple>, value: &[u8]) -> PyResult<usize> {
        let mut parts: Vec<Vec<u8>> = Vec::with_capacity(key.len());
        for part in key.iter() {
            parts.push(part.cast_into::<PyBytes>()?.as_bytes().to_vec());
        }
        Ok(leaf_node_key_value_len(&parts, value))
    }

    fn _search_key<'py>(
        &self,
        py: Python<'py>,
        key: Bound<'_, PyTuple>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let mut parts: Vec<Vec<u8>> = Vec::with_capacity(key.len());
        for part in key.iter() {
            parts.push(part.cast_into::<PyBytes>()?.as_bytes().to_vec());
        }
        let bytes = self.inner.search_key_func.apply(&Key::from(parts));
        Ok(PyBytes::new(py, &bytes))
    }

    /// Static helper mirroring `LeafNode._serialise_key`. The Python
    /// classmethod has no `self`, so this is exposed as a regular
    /// pyo3 staticmethod.
    #[staticmethod]
    fn _serialise_key<'py>(
        py: Python<'py>,
        key: Bound<'_, PyTuple>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let mut parts: Vec<Vec<u8>> = Vec::with_capacity(key.len());
        for part in key.iter() {
            parts.push(part.cast_into::<PyBytes>()?.as_bytes().to_vec());
        }
        Ok(PyBytes::new(py, &Key::from(parts).serialize()))
    }

    fn _compute_search_prefix<'py>(&mut self, py: Python<'py>) -> Py<PyAny> {
        let prefix = self.inner.compute_search_prefix().map(|s| s.to_vec());
        match prefix {
            None => py.None(),
            Some(p) => PyBytes::new(py, &p).into_any().unbind(),
        }
    }

    fn _compute_serialised_prefix<'py>(&mut self, py: Python<'py>) -> Py<PyAny> {
        let prefix = self.inner.compute_serialised_prefix().map(|s| s.to_vec());
        match prefix {
            None => py.None(),
            Some(p) => PyBytes::new(py, &p).into_any().unbind(),
        }
    }

    fn _are_search_keys_identical(&self) -> bool {
        self.inner.are_search_keys_identical()
    }

    /// Insert `(key, value)` and return whether the node has now
    /// overflowed `maximum_size`. Mirrors `LeafNode._map_no_split`.
    fn _map_no_split(&mut self, key: Bound<'_, PyTuple>, value: &[u8]) -> PyResult<bool> {
        let mut parts: Vec<Vec<u8>> = Vec::with_capacity(key.len());
        for part in key.iter() {
            parts.push(part.cast_into::<PyBytes>()?.as_bytes().to_vec());
        }
        Ok(self.inner.map_no_split(parts, value.to_vec()))
    }

    /// Remove `key`, recomputing both prefixes from scratch. Mirrors
    /// `LeafNode.unmap`; `_store` is unused on the leaf path. Raises
    /// `KeyError` when the key is not present.
    #[pyo3(signature = (_store, key))]
    fn unmap(
        &mut self,
        py: Python<'_>,
        _store: Bound<'_, PyAny>,
        key: Bound<'_, PyTuple>,
    ) -> PyResult<Py<PyAny>> {
        let mut parts: Vec<Vec<u8>> = Vec::with_capacity(key.len());
        for part in key.iter() {
            parts.push(part.cast_into::<PyBytes>()?.as_bytes().to_vec());
        }
        if self.inner.unmap(&parts).is_none() {
            return Err(pyo3::exceptions::PyKeyError::new_err(format!(
                "{:?}",
                parts
            )));
        }
        // Python's unmap returns `self` to chain. Returning None from a
        // pyo3 method would replace `self`; instead, return Python's
        // None and let the Python wrapper hand back the LeafNode
        // instance. (Adjusted in the final replacement commit.)
        Ok(py.None())
    }

    /// `LeafNode.iteritems` — return matching `(key, value)` pairs.
    /// `_store` is unused on the leaf path.
    #[pyo3(signature = (_store, key_filter = None))]
    fn iteritems<'py>(
        &self,
        py: Python<'py>,
        _store: Bound<'_, PyAny>,
        key_filter: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyList>> {
        let filter_vec: Option<Vec<Vec<Vec<u8>>>> = match key_filter {
            None => None,
            Some(filter) => {
                let mut out: Vec<Vec<Vec<u8>>> = Vec::new();
                for key in filter.try_iter()? {
                    let key = key?;
                    let key_tuple = key.cast_into::<PyTuple>()?;
                    let mut parts: Vec<Vec<u8>> = Vec::with_capacity(key_tuple.len());
                    for part in key_tuple.iter() {
                        parts.push(part.cast_into::<PyBytes>()?.as_bytes().to_vec());
                    }
                    out.push(parts);
                }
                Some(out)
            }
        };
        let pairs = self.inner.iteritems(filter_vec.as_deref());
        let list = PyList::empty(py);
        for (k, v) in pairs {
            let parts: Vec<Bound<PyBytes>> = k.iter().map(|p| PyBytes::new(py, p)).collect();
            let key_tuple = PyTuple::new(py, parts)?;
            let pair = PyTuple::new(py, [key_tuple.into_any(), PyBytes::new(py, &v).into_any()])?;
            list.append(pair)?;
        }
        Ok(list)
    }

    /// Deserialise the bytes of a serialised LeafNode. `search_key_func`
    /// is optional and defaults to plain.
    #[classmethod]
    #[pyo3(signature = (data, key, search_key_func = None))]
    fn deserialise(
        _cls: &Bound<'_, pyo3::types::PyType>,
        py: Python<'_>,
        data: &[u8],
        key: Bound<'_, PyTuple>,
        search_key_func: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        // Parse the data first so bad-data errors surface before any
        // key-shape complaints.
        let parsed = deserialise_leaf_node(data).map_err(chk_err_to_py)?;
        let (func, callable) = match search_key_func {
            None => (SearchKeyFunc::Plain, None),
            Some(cb) => {
                let resolved = resolve_search_key_func_by_callable(py, &cb)?;
                (resolved, Some(cb.unbind()))
            }
        };
        let mut leaf = RsLeafNode::from_parsed(parsed, func);
        let first = key.get_item(0)?;
        leaf.key = Some(first.cast_into::<PyBytes>()?.as_bytes().to_vec());
        // Sanity check matching the Python deserialise wrapper.
        if data.len() != leaf.current_size() {
            return Err(pyo3::exceptions::PyAssertionError::new_err(
                "_current_size computed incorrectly",
            ));
        }
        Ok(Self {
            inner: leaf,
            search_key_callable: callable,
        })
    }
}

/// CHK internal node — fan-out to child nodes (LeafNode or
/// InternalNode instances) or unloaded sha1 references.
///
/// Holds its scalar state directly; `_items` is a Python dict whose
/// values are either `(b"sha1:...",)` tuples (unloaded) or
/// LeafNode/InternalNode pyclass instances (loaded). The
/// orchestration methods (`map`, `unmap`, `serialise`, `iteritems`)
/// stay in Python — they need to construct sibling pyclass instances
/// and walk the heterogeneous items dict.
#[pyclass(module = "bzrformats._bzr_rs.chk_map", name = "InternalNode")]
pub struct InternalNode {
    key: Option<Vec<u8>>,
    maximum_size: usize,
    key_width: usize,
    len: usize,
    node_width: usize,
    raw_size: usize,
    search_prefix: Option<Vec<u8>>,
    search_key_func: SearchKeyFunc,
    /// Original Python callable as passed in. `None` means use the
    /// default plain variant; the `_search_key_func` getter
    /// synthesises a callable from `search_key_func.name()` in that
    /// case.
    search_key_callable: Option<Py<PyAny>>,
    /// `prefix_bytes -> (b"sha1:...",) tuple OR LeafNode/InternalNode pyclass`.
    /// Heterogeneous, mirroring Python's `InternalNode._items`.
    items: Py<PyDict>,
}

#[pymethods]
impl InternalNode {
    #[new]
    #[pyo3(signature = (prefix = None, search_key_func = None))]
    fn new(
        py: Python<'_>,
        prefix: Option<&[u8]>,
        search_key_func: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let (func, callable) = match search_key_func {
            None => (
                SearchKeyFunc::Plain,
                Some(default_search_key_plain(py).clone_ref(py)),
            ),
            Some(cb) => {
                let resolved = resolve_search_key_func_by_callable(py, &cb)?;
                (resolved, Some(cb.unbind()))
            }
        };
        Ok(Self {
            key: None,
            maximum_size: 0,
            key_width: 1,
            len: 0,
            node_width: 0,
            raw_size: 0,
            search_prefix: Some(prefix.unwrap_or(b"").to_vec()),
            search_key_func: func,
            search_key_callable: callable,
            items: PyDict::new(py).unbind(),
        })
    }

    fn __len__(&self) -> usize {
        self.len
    }

    fn __repr__(&self, py: Python<'_>) -> String {
        let items_dbg = {
            let dict = self.items.bind(py);
            let mut keys: Vec<Vec<u8>> = Vec::new();
            for (k, _v) in dict.iter() {
                if let Ok(b) = k.cast_into::<PyBytes>() {
                    keys.push(b.as_bytes().to_vec());
                }
            }
            keys.sort();
            format!("{:?}", keys)
        };
        let items_short = if items_dbg.len() > 20 {
            format!("{}...]", &items_dbg[..16])
        } else {
            items_dbg
        };
        let key_dbg = match &self.key {
            Some(k) => format!("({:?},)", String::from_utf8_lossy(k)),
            None => "None".to_string(),
        };
        let prefix_dbg = match &self.search_prefix {
            None => "None".to_string(),
            Some(p) => format!("{:?}", p),
        };
        format!(
            "InternalNode(key:{} len:{} size:{} max:{} prefix:{} items:{})",
            key_dbg, self.len, self.raw_size, self.maximum_size, prefix_dbg, items_short,
        )
    }

    fn key<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyTuple>>> {
        match &self.key {
            None => Ok(None),
            Some(k) => Ok(Some(PyTuple::new(py, [PyBytes::new(py, k)])?)),
        }
    }

    fn set_maximum_size(&mut self, new_size: usize) {
        self.maximum_size = new_size;
    }

    #[getter]
    fn maximum_size(&self) -> usize {
        self.maximum_size
    }

    /// Add a child under `prefix`. Mirrors Python's `add_node`:
    /// validates that `prefix` extends `_search_prefix` by exactly
    /// one byte, updates `_len` and `_node_width`, clears `_key`.
    fn add_node(&mut self, py: Python<'_>, prefix: &[u8], node: Bound<'_, PyAny>) -> PyResult<()> {
        let sp = self.search_prefix.as_deref().ok_or_else(|| {
            pyo3::exceptions::PyAssertionError::new_err("_search_prefix should not be None")
        })?;
        if !prefix.starts_with(sp) {
            return Err(pyo3::exceptions::PyAssertionError::new_err(format!(
                "prefixes mismatch: {:?} must start with {:?}",
                prefix, sp
            )));
        }
        if prefix.len() != sp.len() + 1 {
            return Err(pyo3::exceptions::PyAssertionError::new_err(format!(
                "prefix wrong length: len({:?}) is not {}",
                prefix,
                sp.len() + 1
            )));
        }
        let child_len: usize = node.len()?;
        self.len += child_len;
        let dict = self.items.bind(py);
        if dict.is_empty() {
            self.node_width = prefix.len();
        }
        if self.node_width != sp.len() + 1 {
            return Err(pyo3::exceptions::PyAssertionError::new_err(format!(
                "node width mismatch: {} is not {}",
                self.node_width,
                sp.len() + 1
            )));
        }
        dict.set_item(PyBytes::new(py, prefix), node)?;
        self.key = None;
        Ok(())
    }

    fn _current_size(&self) -> usize {
        internal_node_current_size(self.maximum_size, self.key_width, self.len, self.raw_size)
    }

    fn _search_key<'py>(
        &self,
        py: Python<'py>,
        key: Bound<'_, PyTuple>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let mut parts: Vec<Vec<u8>> = Vec::with_capacity(key.len());
        for part in key.iter() {
            parts.push(part.cast_into::<PyBytes>()?.as_bytes().to_vec());
        }
        let base = self.search_key_func.apply(&Key::from(parts));
        let bytes: Vec<u8> = if base.len() >= self.node_width {
            base[..self.node_width].to_vec()
        } else {
            let mut padded = base;
            padded.resize(self.node_width, 0);
            padded
        };
        Ok(PyBytes::new(py, &bytes))
    }

    fn _search_prefix_filter<'py>(
        &self,
        py: Python<'py>,
        key: Bound<'_, PyTuple>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let mut parts: Vec<Vec<u8>> = Vec::with_capacity(key.len());
        for part in key.iter() {
            parts.push(part.cast_into::<PyBytes>()?.as_bytes().to_vec());
        }
        let base = self.search_key_func.apply(&Key::from(parts));
        let bytes: Vec<u8> = if base.len() >= self.node_width {
            base[..self.node_width].to_vec()
        } else {
            base
        };
        Ok(PyBytes::new(py, &bytes))
    }

    fn _compute_search_prefix<'py>(&mut self, py: Python<'py>) -> PyResult<Py<PyAny>> {
        // common_prefix_many over the keys of self.items.
        let dict = self.items.bind(py);
        let mut keys: Vec<Vec<u8>> = Vec::new();
        for (k, _v) in dict.iter() {
            keys.push(k.cast_into::<PyBytes>()?.as_bytes().to_vec());
        }
        let prefix = bazaar::chk_map::common_prefix_many(keys.iter().map(|k| k.as_slice()))
            .map(|s| s.to_vec());
        self.search_prefix = prefix.clone();
        match prefix {
            None => Ok(py.None()),
            Some(p) => Ok(PyBytes::new(py, &p).into_any().unbind()),
        }
    }

    fn refs<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        if self.key.is_none() {
            return Err(pyo3::exceptions::PyAssertionError::new_err(
                "unserialised nodes have no refs",
            ));
        }
        let out = PyList::empty(py);
        let dict = self.items.bind(py);
        for (_k, v) in dict.iter() {
            // Tuple → use directly; Node → call .key()
            if let Ok(t) = v.clone().cast_into::<PyTuple>() {
                out.append(t)?;
            } else {
                let k = v.call_method0("key")?;
                out.append(k)?;
            }
        }
        Ok(out)
    }

    // ----- whitebox state accessors -----

    #[getter]
    fn _key<'py>(&self, py: Python<'py>) -> PyResult<Py<PyAny>> {
        match &self.key {
            None => Ok(py.None()),
            Some(k) => Ok(PyTuple::new(py, [PyBytes::new(py, k)])?.into_any().unbind()),
        }
    }

    #[setter]
    fn set__key(&mut self, value: Bound<'_, PyAny>) -> PyResult<()> {
        if value.is_none() {
            self.key = None;
        } else {
            let tup = value.cast_into::<PyTuple>()?;
            let first = tup.get_item(0)?;
            self.key = Some(first.cast_into::<PyBytes>()?.as_bytes().to_vec());
        }
        Ok(())
    }

    #[getter]
    fn _len(&self) -> usize {
        self.len
    }

    #[setter]
    fn set__len(&mut self, value: usize) {
        self.len = value;
    }

    #[getter]
    fn _maximum_size(&self) -> usize {
        self.maximum_size
    }

    #[setter]
    fn set__maximum_size(&mut self, value: usize) {
        self.maximum_size = value;
    }

    #[getter]
    fn _key_width(&self) -> usize {
        self.key_width
    }

    #[setter]
    fn set__key_width(&mut self, value: usize) {
        self.key_width = value;
    }

    #[getter]
    fn _raw_size(&self) -> usize {
        self.raw_size
    }

    #[setter]
    fn set__raw_size(&mut self, value: usize) {
        self.raw_size = value;
    }

    #[getter]
    fn _node_width(&self) -> usize {
        self.node_width
    }

    #[setter]
    fn set__node_width(&mut self, value: usize) {
        self.node_width = value;
    }

    /// Live reference to the `_items` dict — mutations from Python
    /// propagate. Mirrors Python's `dict` semantics directly.
    #[getter]
    fn _items<'py>(&self, py: Python<'py>) -> Bound<'py, PyDict> {
        self.items.bind(py).clone()
    }

    /// Replace `_items` with a fresh dict. Mirrors
    /// `node._items = {...}`.
    #[setter]
    fn set__items(&mut self, py: Python<'_>, value: Bound<'_, PyDict>) -> PyResult<()> {
        let new_dict = PyDict::new(py);
        for (k, v) in value.iter() {
            new_dict.set_item(k, v)?;
        }
        self.items = new_dict.unbind();
        Ok(())
    }

    #[getter]
    fn _search_prefix<'py>(&self, py: Python<'py>) -> Py<PyAny> {
        match &self.search_prefix {
            None => py.None(),
            Some(p) => PyBytes::new(py, p).into_any().unbind(),
        }
    }

    #[setter]
    fn set__search_prefix(&mut self, value: Bound<'_, PyAny>) -> PyResult<()> {
        if value.is_none() {
            self.search_prefix = None;
        } else {
            self.search_prefix = Some(value.cast_into::<PyBytes>()?.as_bytes().to_vec());
        }
        Ok(())
    }

    /// Returns the callable passed at construction, or `None` if the
    /// default plain variant was used. Same convention as
    /// `LeafNode._search_key_func` — Python wrappers substitute their
    /// own default callable when None is returned.
    #[getter]
    fn _search_key_func<'py>(&self, py: Python<'py>) -> Py<PyAny> {
        match &self.search_key_callable {
            Some(cb) => cb.clone_ref(py),
            None => py.None(),
        }
    }

    #[setter]
    fn set__search_key_func(&mut self, py: Python<'_>, value: Bound<'_, PyAny>) -> PyResult<()> {
        if value.is_none() {
            self.search_key_func = SearchKeyFunc::Plain;
            self.search_key_callable = None;
        } else {
            self.search_key_func = resolve_search_key_func_by_callable(py, &value)?;
            self.search_key_callable = Some(value.unbind());
        }
        Ok(())
    }

    /// `InternalNode.deserialise`: build an internal node from a
    /// serialised page, with every child starting as an unloaded
    /// `(b"sha1:...",)` reference.
    #[classmethod]
    #[pyo3(signature = (data, key, search_key_func = None))]
    fn deserialise(
        _cls: &Bound<'_, pyo3::types::PyType>,
        py: Python<'_>,
        data: &[u8],
        key: Bound<'_, PyAny>,
        search_key_func: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let parsed = deserialise_internal_node(data).map_err(chk_err_to_py)?;
        let (func, callable) = match search_key_func {
            None => (SearchKeyFunc::Plain, None),
            Some(cb) => {
                let resolved = resolve_search_key_func_by_callable(py, &cb)?;
                (resolved, Some(cb.unbind()))
            }
        };
        let key_bytes = if let Ok(t) = key.clone().cast_into::<PyTuple>() {
            t.get_item(0)?.cast_into::<PyBytes>()?.as_bytes().to_vec()
        } else if let Ok(b) = key.cast_into::<PyBytes>() {
            b.as_bytes().to_vec()
        } else {
            return Err(pyo3::exceptions::PyTypeError::new_err(
                "key must be a tuple or bytes",
            ));
        };
        let items = PyDict::new(py);
        for (prefix, flat_key) in parsed.items {
            // Unloaded child: (b"sha1:...",) tuple.
            let tuple = PyTuple::new(py, [PyBytes::new(py, &flat_key)])?;
            items.set_item(PyBytes::new(py, &prefix), tuple)?;
        }
        Ok(Self {
            key: Some(key_bytes),
            maximum_size: parsed.maximum_size,
            key_width: parsed.key_width,
            len: parsed.length,
            node_width: parsed.node_width,
            raw_size: 0,
            search_prefix: Some(parsed.search_prefix),
            search_key_func: func,
            search_key_callable: callable,
            items: items.unbind(),
        })
    }
}

/// CHK persistent map — a string→string dict backed by a CHK store.
///
/// State holder over a Python VersionedFiles store, a root node
/// (either a `(b"sha1:...",)` tuple or a LeafNode/InternalNode
/// pyclass instance), and a search-key function. The full
/// orchestration (map, unmap, iteritems, apply_delta, iter_changes,
/// _save) is monkey-patched on from `bzrformats/chk_map.py` so it
/// can drive the heterogeneous root via duck typing.
#[pyclass(module = "bzrformats._bzr_rs.chk_map", name = "CHKMap")]
pub struct CHKMap {
    pub(crate) store: Py<PyAny>,
    pub(crate) root_node: Py<PyAny>,
    pub(crate) search_key_func: SearchKeyFunc,
    pub(crate) search_key_callable: Option<Py<PyAny>>,
}

#[pymethods]
impl CHKMap {
    #[new]
    #[pyo3(signature = (store, root_key, search_key_func = None))]
    fn new(
        py: Python<'_>,
        store: Bound<'_, PyAny>,
        root_key: Bound<'_, PyAny>,
        search_key_func: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let (func, callable) = match search_key_func {
            None => (
                SearchKeyFunc::Plain,
                Some(default_search_key_plain(py).clone_ref(py)),
            ),
            Some(cb) => {
                let resolved = resolve_search_key_func_by_callable(py, &cb)?;
                (resolved, Some(cb.unbind()))
            }
        };
        // root_key=None → start with an empty LeafNode (constructed
        // directly via Py::new — no py.import needed since the
        // pyclass is local).
        // Otherwise, normalise: if we were handed an existing Node
        // instance (LeafNode/InternalNode), extract its `.key()`
        // (mirrors Python's `_node_key`). A tuple is stored as-is.
        let root_node: Py<PyAny> = if root_key.is_none() {
            let leaf = LeafNode {
                inner: RsLeafNode::new(func.clone()),
                search_key_callable: callable.as_ref().map(|cb| cb.clone_ref(py)),
            };
            Py::new(py, leaf)?.into_any()
        } else if let Ok(_) = root_key.clone().cast_into::<PyTuple>() {
            root_key.unbind()
        } else {
            // Node-like: pull off `.key()`.
            let k = root_key.call_method0("key")?;
            if k.is_none() {
                root_key.unbind()
            } else {
                k.unbind()
            }
        };
        Ok(Self {
            store: store.unbind(),
            root_node,
            search_key_func: func,
            search_key_callable: callable,
        })
    }

    #[getter]
    fn _store<'py>(&self, py: Python<'py>) -> Py<PyAny> {
        self.store.clone_ref(py)
    }

    #[setter]
    fn set__store(&mut self, value: Bound<'_, PyAny>) {
        self.store = value.unbind();
    }

    #[getter]
    fn _root_node<'py>(&self, py: Python<'py>) -> Py<PyAny> {
        self.root_node.clone_ref(py)
    }

    #[setter]
    fn set__root_node(&mut self, value: Bound<'_, PyAny>) {
        self.root_node = value.unbind();
    }

    #[getter]
    fn _search_key_func<'py>(&self, py: Python<'py>) -> Py<PyAny> {
        match &self.search_key_callable {
            Some(cb) => cb.clone_ref(py),
            None => py.None(),
        }
    }

    #[setter]
    fn set__search_key_func(&mut self, py: Python<'_>, value: Bound<'_, PyAny>) -> PyResult<()> {
        if value.is_none() {
            self.search_key_func = SearchKeyFunc::Plain;
            self.search_key_callable = None;
        } else {
            self.search_key_func = resolve_search_key_func_by_callable(py, &value)?;
            self.search_key_callable = Some(value.unbind());
        }
        Ok(())
    }

    /// Return this map's root key tuple. Mirrors Python's
    /// `_chkmap_key`: if the root is a tuple (unloaded), return it;
    /// otherwise pull `.key()` off the loaded node.
    fn key<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let root = self.root_node.bind(py);
        if let Ok(t) = root.clone().cast_into::<PyTuple>() {
            return Ok(t.into_any());
        }
        root.call_method0("key")
    }

    /// Number of items in the CHK map. Mirrors Python's
    /// `_chkmap_len`: ensure_root, then `len(self._root_node)`.
    fn __len__(slf: Bound<'_, Self>, py: Python<'_>) -> PyResult<usize> {
        Self::_ensure_root(slf.clone(), py)?;
        let root = slf.borrow().root_node.bind(py).clone();
        root.len()
    }

    /// Force the root to be a loaded Node, not a tuple key.
    /// Mirrors Python's `_chkmap_ensure_root`.
    fn _ensure_root(slf: Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let needs_load = slf
            .borrow()
            .root_node
            .bind(py)
            .clone()
            .cast_into::<PyTuple>()
            .is_ok();
        if !needs_load {
            return Ok(());
        }
        let key_tuple = slf
            .borrow()
            .root_node
            .bind(py)
            .clone()
            .cast_into::<PyTuple>()?;
        let node = Self::_get_node_inner(&slf, py, key_tuple.into_any())?;
        slf.borrow_mut().root_node = node.unbind();
        Ok(())
    }

    /// Resolve a node argument: tuple keys are fetched from the
    /// store and deserialised; loaded nodes pass through.
    /// Mirrors Python's `_chkmap_get_node`.
    fn _get_node<'py>(
        slf: Bound<'py, Self>,
        py: Python<'py>,
        node: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        Self::_get_node_inner(&slf, py, node)
    }

    /// Get the key for a node-or-tuple. Mirrors Python's
    /// `_chkmap_node_key`.
    fn _node_key<'py>(
        &self,
        py: Python<'py>,
        node: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let _ = py;
        if node.clone().cast_into::<PyTuple>().is_ok() {
            return Ok(node);
        }
        node.call_method0("key")
    }

    /// Fetch the raw bytes for a CHK key. Consults the
    /// process-wide page cache before going to the store.
    /// Mirrors Python's `_chkmap_read_bytes`.
    fn _read_bytes<'py>(
        &self,
        py: Python<'py>,
        key: Bound<'py, PyTuple>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        use bazaar::chk_map::PageCache as _;
        // Cache lookup uses the flat sha1 bytes (the first tuple element).
        let sha1: Vec<u8> = key
            .get_item(0)?
            .cast_into::<PyBytes>()?
            .as_bytes()
            .to_vec();
        if let Some(cached) = page_cache().get(&sha1) {
            return Ok(PyBytes::new(py, &cached));
        }
        let keys = PyList::new(py, [key.clone()])?;
        let stream = self.store.bind(py).call_method1(
            "get_record_stream",
            (keys, "unordered", true),
        )?;
        let iter = stream.try_iter()?;
        let record = iter
            .into_iter()
            .next()
            .ok_or_else(|| {
                pyo3::exceptions::PyKeyError::new_err(format!(
                    "no record returned for key {:?}",
                    key
                ))
            })??;
        let bytes_obj = record.call_method1("get_bytes_as", ("fulltext",))?;
        let bytes_py = bytes_obj.cast_into::<PyBytes>()?;
        let bytes_vec = bytes_py.as_bytes().to_vec();
        page_cache().insert(sha1, bytes_vec);
        Ok(bytes_py)
    }
}

impl CHKMap {
    /// Shared body of `_get_node` / `_ensure_root`: tuple keys load
    /// the page bytes and dispatch to LeafNode or InternalNode
    /// `deserialise`; anything else passes through.
    fn _get_node_inner<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        node: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let Ok(key_tuple) = node.clone().cast_into::<PyTuple>() else {
            return Ok(node);
        };
        let bytes = slf
            .borrow()
            ._read_bytes(py, key_tuple.clone())?;
        let data = bytes.as_bytes();
        let search_key_callable = slf
            .borrow()
            .search_key_callable
            .as_ref()
            .map(|c| c.bind(py).clone());
        if data.starts_with(b"chkleaf:\n") {
            let cls = py.get_type::<LeafNode>();
            cls.call_method(
                "deserialise",
                (bytes, key_tuple, search_key_callable),
                None,
            )
        } else if data.starts_with(b"chknode:\n") {
            let cls = py.get_type::<InternalNode>();
            cls.call_method(
                "deserialise",
                (bytes, key_tuple, search_key_callable),
                None,
            )
        } else {
            Err(pyo3::exceptions::PyAssertionError::new_err(
                "Unknown node type.",
            ))
        }
    }
}

pub(crate) fn _chk_map_rs(py: Python) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "chk_map")?;
    m.add_wrapped(wrap_pyfunction!(_search_key_plain))?;
    m.add_wrapped(wrap_pyfunction!(_search_key_16))?;
    m.add_wrapped(wrap_pyfunction!(_search_key_255))?;
    m.add_wrapped(wrap_pyfunction!(_bytes_to_text_key))?;
    m.add_wrapped(wrap_pyfunction!(common_prefix_pair))?;
    m.add_wrapped(wrap_pyfunction!(common_prefix_many))?;
    m.add_wrapped(wrap_pyfunction!(py_deserialise_leaf_node))?;
    m.add_wrapped(wrap_pyfunction!(py_deserialise_internal_node))?;
    m.add_wrapped(wrap_pyfunction!(py_serialise_leaf_node))?;
    m.add_wrapped(wrap_pyfunction!(py_serialise_internal_node))?;
    m.add_wrapped(wrap_pyfunction!(py_leaf_node_key_value_len))?;
    m.add_wrapped(wrap_pyfunction!(py_leaf_node_current_size))?;
    m.add_wrapped(wrap_pyfunction!(py_internal_node_current_size))?;
    m.add_wrapped(wrap_pyfunction!(py_are_search_keys_identical))?;
    m.add_wrapped(wrap_pyfunction!(py_search_key_by_name))?;
    m.add_wrapped(wrap_pyfunction!(clear_cache))?;
    m.add_wrapped(wrap_pyfunction!(_page_cache_get))?;
    m.add_wrapped(wrap_pyfunction!(_page_cache_set))?;
    // Stash the per-variant pyfunctions so pyclass `#[new]` and
    // cross-module helpers (`search_key_callable_for_name`) can hand
    // them back without going through Python's search-key registry.
    // Use `m.getattr` after add so we share the same Python object
    // that callers see via the module — preserves callable identity.
    DEFAULT_SEARCH_KEY_PLAIN
        .set(py, m.getattr("_search_key_plain")?.unbind())
        .ok();
    SEARCH_KEY_16_CALLABLE
        .set(py, m.getattr("_search_key_16")?.unbind())
        .ok();
    SEARCH_KEY_255_CALLABLE
        .set(py, m.getattr("_search_key_255")?.unbind())
        .ok();
    // Expose the `_unknown` sentinel itself so Python can re-export it
    // as `chk_map._unknown` and identity comparisons line up with what
    // the Rust mutators write back.
    m.add("_unknown", py_unknown_sentinel(py))?;
    m.add_class::<LeafNode>()?;
    m.add_class::<InternalNode>()?;
    m.add_class::<CHKMap>()?;
    Ok(m)
}
