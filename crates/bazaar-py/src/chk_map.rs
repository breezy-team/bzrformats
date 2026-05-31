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

pyo3::import_exception!(bzrformats._bzr_rs.errors, InconsistentDeltaDelta);
pyo3::import_exception!(bzrformats._bzr_rs.errors, NoSuchRevision);

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

/// Convert serialised node bytes into a `LeafNode` or `InternalNode`,
/// dispatching on the body prefix. Mirrors the Python
/// `chk_map._deserialise` helper used by repositorydetails code.
#[pyfunction]
#[pyo3(name = "_deserialise")]
#[pyo3(signature = (data, key, search_key_func = None))]
fn py_deserialise<'py>(
    py: Python<'py>,
    data: &[u8],
    key: Bound<'py, PyTuple>,
    search_key_func: Option<Bound<'py, PyAny>>,
) -> PyResult<Bound<'py, PyAny>> {
    if data.starts_with(b"chkleaf:\n") {
        py.get_type::<LeafNode>()
            .call_method("deserialise", (data, key, search_key_func), None)
    } else if data.starts_with(b"chknode:\n") {
        py.get_type::<InternalNode>()
            .call_method("deserialise", (data, key, search_key_func), None)
    } else {
        Err(pyo3::exceptions::PyAssertionError::new_err(
            "Unknown node type.",
        ))
    }
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
        // `PyAny` is the `object` base type (`PyBaseObject_Type`).
        py.get_type::<PyAny>().call0().unwrap().unbind()
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

/// Zero-sized `PageCache` that forwards to the process-wide
/// [`page_cache`]. Lets pure-crate code that wants an owned
/// `Arc<dyn PageCache>` share the same cache the binding uses, so
/// lazy-loading behaviour (and tests that assert pages are not
/// re-fetched) stay consistent.
struct GlobalPageCache;

impl bazaar::chk_map::PageCache for GlobalPageCache {
    fn get(&self, sha1_key: &[u8]) -> Option<Vec<u8>> {
        page_cache().get(sha1_key)
    }
    fn insert(&self, sha1_key: Vec<u8>, bytes: Vec<u8>) {
        page_cache().insert(sha1_key, bytes);
    }
    fn clear(&self) {
        page_cache().clear();
    }
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
    let sha1: Vec<u8> = key.get_item(0)?.cast_into::<PyBytes>()?.as_bytes().to_vec();
    Ok(page_cache().get(&sha1).map(|b| PyBytes::new(py, &b)))
}

/// Insert `value` into the page cache under `key`. Companion to
/// `_page_cache_get`.
#[pyfunction]
fn _page_cache_set(key: Bound<'_, PyTuple>, value: &[u8]) -> PyResult<()> {
    use bazaar::chk_map::PageCache as _;
    let sha1: Vec<u8> = key.get_item(0)?.cast_into::<PyBytes>()?.as_bytes().to_vec();
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
    /// `KeyError` when the key is not present. Returns `self` so callers
    /// can chain, matching the Python API.
    #[pyo3(signature = (_store, key))]
    fn unmap<'py>(
        slf: Bound<'py, Self>,
        _store: Bound<'_, PyAny>,
        key: Bound<'_, PyTuple>,
    ) -> PyResult<Bound<'py, Self>> {
        let mut parts: Vec<Vec<u8>> = Vec::with_capacity(key.len());
        for part in key.iter() {
            parts.push(part.cast_into::<PyBytes>()?.as_bytes().to_vec());
        }
        if slf.borrow_mut().inner.unmap(&parts).is_none() {
            return Err(pyo3::exceptions::PyKeyError::new_err(format!(
                "{:?}",
                parts
            )));
        }
        Ok(slf)
    }

    /// Serialise this leaf into `store`, returning `[(b"sha1:...",)]`.
    /// Mirrors `LeafNode.serialise` (the former Python `_leaf_serialise`).
    fn serialise<'py>(
        &mut self,
        py: Python<'py>,
        store: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        use bazaar::chk_map::PageCache as _;
        let mut sorted_items: Vec<(&Vec<Vec<u8>>, &Vec<u8>)> = self.inner.items.iter().collect();
        sorted_items.sort_by(|a, b| a.0.cmp(b.0));
        let rust_items: Vec<(Vec<Vec<u8>>, Vec<u8>)> = sorted_items
            .into_iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        let out = serialise_leaf_node(
            self.inner.maximum_size,
            self.inner.key_width,
            &rust_items,
            self.inner.common_serialised_prefix.as_deref(),
        )
        .map_err(chk_err_to_py)?;
        let lines = PyList::empty(py);
        for line in &out {
            lines.append(PyBytes::new(py, line))?;
        }
        let result = store.call_method1("add_lines", ((py.None(),), PyList::empty(py), &lines))?;
        let sha1: Vec<u8> = result
            .cast_into::<PyTuple>()?
            .get_item(0)?
            .cast_into::<PyBytes>()?
            .as_bytes()
            .to_vec();
        let mut key = b"sha1:".to_vec();
        key.extend_from_slice(&sha1);
        self.inner.key = Some(key.clone());
        let data: Vec<u8> = out.iter().flatten().copied().collect();
        if data.len() != self.inner.current_size() {
            return Err(pyo3::exceptions::PyAssertionError::new_err(
                "Invalid _current_size",
            ));
        }
        page_cache().insert(key.clone(), data);
        let key_tuple = PyTuple::new(py, [PyBytes::new(py, &key)])?;
        PyList::new(py, [key_tuple])
    }

    /// Map `key`->`value`, returning `(prefix, [(node_prefix, node)])`.
    /// If the node overflows it splits. Mirrors `LeafNode.map`
    /// (the former Python `_leaf_map`).
    fn map<'py>(
        slf: Bound<'py, Self>,
        py: Python<'py>,
        store: Bound<'py, PyAny>,
        key: Bound<'py, PyTuple>,
        value: Vec<u8>,
    ) -> PyResult<(Py<PyAny>, Bound<'py, PyList>)> {
        let overflowed = {
            let mut me = slf.borrow_mut();
            let mut parts: Vec<Vec<u8>> = Vec::with_capacity(key.len());
            for part in key.iter() {
                parts.push(part.cast_into::<PyBytes>()?.as_bytes().to_vec());
            }
            if let Some(existing) = me.inner.items.get(&parts) {
                me.inner.raw_size -= leaf_node_key_value_len(&parts, existing);
            }
            me.inner.key = None;
            me.inner.map_no_split(parts, value)
        };
        if overflowed {
            return LeafNode::_split(slf, py, store);
        }
        let prefix = match &slf.borrow().inner.search_prefix {
            SearchPrefix::Unknown => {
                return Err(pyo3::exceptions::PyAssertionError::new_err(
                    "search prefix must be known",
                ));
            }
            SearchPrefix::Computed(None) => py.None(),
            SearchPrefix::Computed(Some(p)) => PyBytes::new(py, p).into_any().unbind(),
        };
        let details = PyList::new(
            py,
            [PyTuple::new(
                py,
                [PyBytes::new(py, b"").into_any(), slf.into_any()],
            )?],
        )?;
        Ok((prefix, details))
    }

    /// Split an overflowed leaf into multiple leaves, returning
    /// `(common_prefix, [(node_prefix, node)])`. Mirrors `LeafNode._split`
    /// (the former Python `_leaf_split`).
    fn _split<'py>(
        slf: Bound<'py, Self>,
        py: Python<'py>,
        store: Bound<'py, PyAny>,
    ) -> PyResult<(Py<PyAny>, Bound<'py, PyList>)> {
        let (common_prefix, maximum_size, key_width, items, callable) = {
            let me = slf.borrow();
            let common_prefix = match &me.inner.search_prefix {
                SearchPrefix::Unknown => {
                    return Err(pyo3::exceptions::PyAssertionError::new_err(
                        "Search prefix must be known",
                    ));
                }
                SearchPrefix::Computed(None) => Vec::new(),
                SearchPrefix::Computed(Some(p)) => p.clone(),
            };
            let items: Vec<(Vec<Vec<u8>>, Vec<u8>)> = me
                .inner
                .items
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            let callable = me.search_key_callable.as_ref().map(|c| c.bind(py).clone());
            (
                common_prefix,
                me.inner.maximum_size,
                me.inner.key_width,
                items,
                callable,
            )
        };
        let split_at = common_prefix.len() + 1;
        // `result` is an insertion-ordered dict, exactly like Python's;
        // the final `(prefix, node)` list is just its items.
        let result = PyDict::new(py);
        for (key_parts, value) in items {
            let search_key = slf
                .borrow()
                .inner
                .search_key_func
                .apply(&Key::from(key_parts.clone()));
            let mut prefix: Vec<u8> = search_key.iter().take(split_at).copied().collect();
            if prefix.len() < split_at {
                prefix.resize(split_at, 0);
            }
            let prefix_py = PyBytes::new(py, &prefix);
            let node: Bound<'py, PyAny> = if let Some(existing) = result.get_item(&prefix_py)? {
                existing
            } else {
                let leaf = Bound::new(py, LeafNode::new(py, callable.clone())?)?;
                leaf.borrow_mut().inner.maximum_size = maximum_size;
                leaf.borrow_mut().inner.key_width = key_width;
                result.set_item(&prefix_py, &leaf)?;
                leaf.into_any()
            };
            let key_tuple = PyTuple::new(py, key_parts.iter().map(|p| PyBytes::new(py, p)))?;
            // `node` may already be an InternalNode if an earlier item with
            // this prefix overflowed and was promoted; dispatch via the
            // Python `map` so the right implementation runs.
            let mapped = node
                .call_method1("map", (store.clone(), key_tuple, PyBytes::new(py, &value)))?
                .cast_into::<PyTuple>()?;
            let sub_prefix = mapped.get_item(0)?;
            let node_details = mapped.get_item(1)?.cast_into::<PyList>()?;
            if node_details.len() > 1 {
                if !sub_prefix.eq(&prefix_py)? {
                    // Re-pathed under a different prefix; drop the old slot.
                    result.del_item(&prefix_py)?;
                }
                let sub_prefix_slice = sub_prefix.cast_into::<PyBytes>()?.as_bytes().to_vec();
                let internal = Bound::new(
                    py,
                    InternalNode::new(py, Some(&sub_prefix_slice), callable.clone())?,
                )?;
                internal.borrow_mut().maximum_size = maximum_size;
                internal.borrow_mut().key_width = key_width;
                for detail in node_details.iter() {
                    let detail = detail.cast_into::<PyTuple>()?;
                    let split = detail.get_item(0)?.cast_into::<PyBytes>()?;
                    let sub_node = detail.get_item(1)?;
                    InternalNode::add_node(
                        &mut internal.borrow_mut(),
                        py,
                        split.as_bytes(),
                        sub_node,
                    )?;
                }
                result.set_item(&prefix_py, &internal)?;
            }
        }
        let details = PyList::empty(py);
        for (prefix, node) in result.iter() {
            details.append(PyTuple::new(py, [prefix, node])?)?;
        }
        let common = PyBytes::new(py, &common_prefix).into_any().unbind();
        Ok((common, details))
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

    /// Iterate over child nodes matching `key_filter`, demand-loading
    /// unloaded children from the page cache and store. Returns a lazy
    /// iterator of `(node, node_key_filter)` pairs; loading replaces the
    /// `(b"sha1:...",)` tuple in `_items` with the deserialised node.
    /// Laziness matters: `_check_remap` stops early and must not page in
    /// children it never reaches. Mirrors `_internal_iter_nodes`.
    #[pyo3(signature = (store, key_filter = None, batch_size = None))]
    fn _iter_nodes<'py>(
        slf: Bound<'py, Self>,
        py: Python<'py>,
        store: Bound<'py, PyAny>,
        key_filter: Option<Bound<'py, PyAny>>,
        batch_size: Option<usize>,
    ) -> PyResult<Bound<'py, InternalNodeIterator>> {
        // Eager filtering pass: classify each child as already-resolved
        // (into `result`) or pending load (into `to_load`/`key_order`).
        // Loading is deferred to the iterator so early consumers (e.g.
        // `_check_remap`) don't page in children they never reach.
        let result = PyList::empty(py);
        let to_load = PyDict::new(py);
        let mut key_order: Vec<Py<PyAny>> = Vec::new();
        {
            let me = slf.borrow();
            let items = me.items.bind(py);
            let filter_len = match &key_filter {
                None => None,
                Some(kf) => Some(kf.len()?),
            };
            let mut shortcut = false;
            if key_filter.is_none() {
                shortcut = true;
                for (prefix, node) in items.iter() {
                    if node.clone().cast::<PyTuple>().is_ok() {
                        record_to_load(
                            &to_load,
                            &mut key_order,
                            &node,
                            &prefix,
                            py.None().bind(py),
                        )?;
                    } else if is_node(&node) {
                        result.append(PyTuple::new(py, [node, py.None().into_bound(py)])?)?;
                    } else {
                        return Err(invalid_node_type(&node));
                    }
                }
            } else if filter_len == Some(1) {
                let kf = key_filter.as_ref().unwrap();
                let key = kf.try_iter()?.next().unwrap()?;
                let key_tuple = key.clone().cast_into::<PyTuple>()?;
                let search_prefix = me._search_prefix_filter(py, key_tuple)?;
                if search_prefix.as_bytes().len() == me.node_width {
                    shortcut = true;
                    if let Some(node) = items.get_item(&search_prefix)? {
                        let filter_list = PyList::new(py, [key.clone()])?;
                        if node.clone().cast::<PyTuple>().is_ok() {
                            record_to_load(
                                &to_load,
                                &mut key_order,
                                &node,
                                &search_prefix,
                                filter_list.as_any(),
                            )?;
                        } else if is_node(&node) {
                            result.append(PyTuple::new(py, [node, filter_list.into_any()])?)?;
                        } else {
                            return Err(invalid_node_type(&node));
                        }
                    }
                }
            }

            if !shortcut {
                let kf = key_filter.as_ref().ok_or_else(|| {
                    pyo3::exceptions::PyAssertionError::new_err("key_filter must not be None")
                })?;
                let prefix_to_keys = PyDict::new(py);
                let mut length_filters: std::collections::HashMap<
                    usize,
                    std::collections::HashSet<Vec<u8>>,
                > = std::collections::HashMap::new();
                for key in kf.try_iter()? {
                    let key = key?;
                    let key_tuple = key.clone().cast_into::<PyTuple>()?;
                    let search_prefix = me._search_prefix_filter(py, key_tuple)?;
                    let sp_bytes = search_prefix.as_bytes().to_vec();
                    length_filters
                        .entry(sp_bytes.len())
                        .or_default()
                        .insert(sp_bytes.clone());
                    match prefix_to_keys.get_item(&search_prefix)? {
                        Some(lst) => lst.cast_into::<PyList>()?.append(key)?,
                        None => {
                            prefix_to_keys.set_item(&search_prefix, PyList::new(py, [key])?)?;
                        }
                    }
                }

                if length_filters.contains_key(&me.node_width) && length_filters.len() == 1 {
                    let search_prefixes = &length_filters[&me.node_width];
                    for sp in search_prefixes {
                        let sp_py = PyBytes::new(py, sp);
                        let Some(node) = items.get_item(&sp_py)? else {
                            continue;
                        };
                        let node_key_filter = prefix_to_keys
                            .get_item(&sp_py)?
                            .unwrap()
                            .cast_into::<PyList>()?;
                        if node.clone().cast::<PyTuple>().is_ok() {
                            record_to_load(
                                &to_load,
                                &mut key_order,
                                &node,
                                &sp_py,
                                node_key_filter.as_any(),
                            )?;
                        } else if is_node(&node) {
                            result.append(PyTuple::new(py, [node, node_key_filter.into_any()])?)?;
                        } else {
                            return Err(invalid_node_type(&node));
                        }
                    }
                } else {
                    for (prefix, node) in items.iter() {
                        let prefix_bytes =
                            prefix.clone().cast_into::<PyBytes>()?.as_bytes().to_vec();
                        let node_key_filter = PyList::empty(py);
                        for (length, length_filter) in &length_filters {
                            if prefix_bytes.len() >= *length {
                                let sub_prefix = &prefix_bytes[..*length];
                                if length_filter.contains(sub_prefix) {
                                    let sub_py = PyBytes::new(py, sub_prefix);
                                    let keys = prefix_to_keys
                                        .get_item(&sub_py)?
                                        .unwrap()
                                        .cast_into::<PyList>()?;
                                    for k in keys.iter() {
                                        node_key_filter.append(k)?;
                                    }
                                }
                            }
                        }
                        if !node_key_filter.is_empty() {
                            if node.clone().cast::<PyTuple>().is_ok() {
                                record_to_load(
                                    &to_load,
                                    &mut key_order,
                                    &node,
                                    &prefix,
                                    node_key_filter.as_any(),
                                )?;
                            } else if is_node(&node) {
                                result.append(PyTuple::new(
                                    py,
                                    [node, node_key_filter.into_any()],
                                )?)?;
                            } else {
                                return Err(invalid_node_type(&node));
                            }
                        }
                    }
                }
            }
        }
        InternalNodeIterator::new_from(py, &slf, store, result, to_load, key_order, batch_size)
    }

    /// Iterate over `(key, value)` items in this node and its children,
    /// demand-loading as needed. Mirrors `_internal_iteritems`.
    #[pyo3(signature = (store, key_filter = None))]
    fn iteritems<'py>(
        slf: Bound<'py, Self>,
        py: Python<'py>,
        store: Bound<'py, PyAny>,
        key_filter: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyList>> {
        let out = PyList::empty(py);
        let nodes = InternalNode::_iter_nodes(slf, py, store.clone(), key_filter, None)?;
        for pair in nodes.try_iter()? {
            let pair = pair?.cast_into::<PyTuple>()?;
            let node = pair.get_item(0)?;
            let node_filter = pair.get_item(1)?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("key_filter", node_filter)?;
            let items = node.call_method("iteritems", (store.clone(),), Some(&kwargs))?;
            for item in items.try_iter()? {
                out.append(item?)?;
            }
        }
        Ok(out)
    }

    /// Serialise this node and any dirty children to `store`, returning
    /// the list of sha1 keys written. Mirrors `_internal_serialise`.
    fn serialise<'py>(
        slf: Bound<'py, Self>,
        py: Python<'py>,
        store: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        use bazaar::chk_map::PageCache as _;
        let yielded = PyList::empty(py);
        // Serialise dirty children first.
        let children: Vec<Bound<'py, PyAny>> = {
            let me = slf.borrow();
            let dict = me.items.bind(py);
            dict.values().iter().collect()
        };
        for node in children {
            if node.clone().cast::<PyTuple>().is_ok() {
                continue;
            }
            if !is_node(&node) {
                return Err(pyo3::exceptions::PyAssertionError::new_err(
                    "InternalNode._items should only contain tuples or Nodes",
                ));
            }
            // Already-serialised children (with a key) are skipped.
            if !node.getattr("_key")?.is_none() {
                continue;
            }
            for key in node
                .call_method1("serialise", (store.clone(),))?
                .try_iter()?
            {
                yielded.append(key?)?;
            }
        }
        let (maximum_size, key_width, len, search_prefix, sorted_items) = {
            let me = slf.borrow();
            let search_prefix = me.search_prefix.clone().ok_or_else(|| {
                pyo3::exceptions::PyAssertionError::new_err("_search_prefix should not be None")
            })?;
            let dict = me.items.bind(py);
            let mut entries: Vec<(Vec<u8>, Bound<'py, PyAny>)> = Vec::new();
            for (k, v) in dict.iter() {
                entries.push((k.cast_into::<PyBytes>()?.as_bytes().to_vec(), v));
            }
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            let sorted_items = PyList::empty(py);
            for (prefix, node) in entries {
                let flat_key: Bound<'py, PyBytes> = if let Ok(t) = node.clone().cast::<PyTuple>() {
                    t.get_item(0)?.cast_into::<PyBytes>()?
                } else {
                    node.getattr("_key")?
                        .cast_into::<PyTuple>()?
                        .get_item(0)?
                        .cast_into::<PyBytes>()?
                };
                sorted_items.append(PyTuple::new(
                    py,
                    [PyBytes::new(py, &prefix).into_any(), flat_key.into_any()],
                )?)?;
            }
            (
                me.maximum_size,
                me.key_width,
                me.len,
                search_prefix,
                sorted_items,
            )
        };
        let lines = py_serialise_internal_node(
            py,
            maximum_size,
            key_width,
            len,
            &search_prefix,
            sorted_items.into_any(),
        )?;
        let result = store.call_method1("add_lines", ((py.None(),), PyList::empty(py), &lines))?;
        let sha1: Vec<u8> = result
            .cast_into::<PyTuple>()?
            .get_item(0)?
            .cast_into::<PyBytes>()?
            .as_bytes()
            .to_vec();
        let mut key = b"sha1:".to_vec();
        key.extend_from_slice(&sha1);
        slf.borrow_mut().key = Some(key.clone());
        let mut data: Vec<u8> = Vec::new();
        for line in lines.iter() {
            data.extend_from_slice(line.cast_into::<PyBytes>()?.as_bytes());
        }
        page_cache().insert(key.clone(), data);
        let key_tuple = PyTuple::new(py, [PyBytes::new(py, &key)])?;
        yielded.append(key_tuple)?;
        Ok(yielded)
    }

    /// Split into smaller nodes starting at `offset`; only meaningful
    /// when `offset >= node_width`. Mirrors `_internal_split`.
    fn _split<'py>(&self, py: Python<'py>, offset: usize) -> PyResult<Bound<'py, PyList>> {
        let out = PyList::empty(py);
        if offset >= self.node_width {
            let dict = self.items.bind(py);
            for node in dict.values().iter() {
                for item in node.call_method1("_split", (offset,))?.try_iter()? {
                    out.append(item?)?;
                }
            }
        }
        Ok(out)
    }

    /// Check whether the whole subtree now fits in a single LeafNode;
    /// if so return that new leaf, else return `self`. Mirrors
    /// `_internal_check_remap`.
    fn _check_remap<'py>(
        slf: Bound<'py, Self>,
        py: Python<'py>,
        store: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (callable, maximum_size, key_width) = {
            let me = slf.borrow();
            (
                me.search_key_callable.as_ref().map(|c| c.bind(py).clone()),
                me.maximum_size,
                me.key_width,
            )
        };
        let new_leaf = Bound::new(py, LeafNode::new(py, callable)?)?;
        new_leaf.borrow_mut().inner.maximum_size = maximum_size;
        new_leaf.borrow_mut().inner.key_width = key_width;
        let nodes = InternalNode::_iter_nodes(slf.clone(), py, store, None, Some(16))?;
        for pair in nodes.try_iter()? {
            let pair = pair?.cast_into::<PyTuple>()?;
            let node = pair.get_item(0)?;
            if node.clone().cast::<InternalNode>().is_ok() {
                return Ok(slf.into_any());
            }
            let leaf = node.cast_into::<LeafNode>()?;
            let items: Vec<(Vec<Vec<u8>>, Vec<u8>)> = leaf
                .borrow()
                .inner
                .items
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            for (k, v) in items {
                if new_leaf.borrow_mut().inner.map_no_split(k, v) {
                    return Ok(slf.into_any());
                }
            }
        }
        Ok(new_leaf.into_any())
    }

    /// Map `key`->`value` into the subtree, returning
    /// `(prefix, [(node_prefix, node)])`. Mirrors `_internal_map`.
    fn map<'py>(
        slf: Bound<'py, Self>,
        py: Python<'py>,
        store: Bound<'py, PyAny>,
        key: Bound<'py, PyTuple>,
        value: Vec<u8>,
    ) -> PyResult<(Py<PyAny>, Bound<'py, PyList>)> {
        {
            let me = slf.borrow();
            if me.items.bind(py).is_empty() {
                return Err(pyo3::exceptions::PyAssertionError::new_err(
                    "can't map in an empty InternalNode.",
                ));
            }
        }
        let search_key = slf.borrow()._search_key(py, key.clone())?;
        let search_prefix = slf.borrow().search_prefix.clone().unwrap_or_default();
        let node_width = slf.borrow().node_width;
        if node_width != search_prefix.len() + 1 {
            return Err(pyo3::exceptions::PyAssertionError::new_err(format!(
                "node width mismatch: {} is not {}",
                node_width,
                search_prefix.len() + 1
            )));
        }
        if !search_key.as_bytes().starts_with(&search_prefix) {
            // The key falls outside this node; build a new common parent.
            let new_prefix =
                bazaar::chk_map::common_prefix_pair(&search_prefix, search_key.as_bytes()).to_vec();
            let callable = slf
                .borrow()
                .search_key_callable
                .as_ref()
                .map(|c| c.bind(py).clone());
            let new_parent = Bound::new(py, InternalNode::new(py, Some(&new_prefix), callable)?)?;
            new_parent.borrow_mut().maximum_size = slf.borrow().maximum_size;
            new_parent.borrow_mut().key_width = slf.borrow().key_width;
            let self_prefix = search_prefix[..new_prefix.len() + 1].to_vec();
            InternalNode::add_node(
                &mut new_parent.borrow_mut(),
                py,
                &self_prefix,
                slf.clone().into_any(),
            )?;
            return InternalNode::map(new_parent, py, store, key, value);
        }
        // Find or create the child for this search key.
        let filter = PyList::new(py, [key.clone()])?;
        let nodes = InternalNode::_iter_nodes(
            slf.clone(),
            py,
            store.clone(),
            Some(filter.into_any()),
            None,
        )?;
        let first = nodes.try_iter()?.next();
        let child: Bound<'py, PyAny> = match first {
            Some(pair) => pair?.cast_into::<PyTuple>()?.get_item(0)?,
            None => internal_new_child(&slf, py, search_key.as_bytes(), false)?,
        };
        let old_len: usize = child.len()?;
        let old_size: Option<usize> = match child.cast::<LeafNode>() {
            Ok(leaf) => Some(leaf.borrow()._current_size()),
            Err(_) => None,
        };
        let mapped = child
            .call_method1(
                "map",
                (store.clone(), key.clone(), PyBytes::new(py, &value)),
            )?
            .cast_into::<PyTuple>()?;
        let prefix = mapped.get_item(0)?;
        let node_details = mapped.get_item(1)?.cast_into::<PyList>()?;
        if node_details.len() == 1 {
            let child = node_details
                .get_item(0)?
                .cast_into::<PyTuple>()?
                .get_item(1)?;
            let new_child_len: usize = child.len()?;
            {
                let mut me = slf.borrow_mut();
                me.len = me.len - old_len + new_child_len;
                me.items.bind(py).set_item(&search_key, &child)?;
                me.key = None;
            }
            let mut new_node: Bound<'py, PyAny> = slf.clone().into_any();
            if let Ok(leaf) = child.cast::<LeafNode>() {
                let do_remap = match old_size {
                    None => true,
                    Some(old) => {
                        let new_size = leaf.borrow()._current_size();
                        let shrinkage = old as isize - new_size as isize;
                        (shrinkage > 0 && new_size < bazaar::chk_map::INTERESTING_NEW_SIZE)
                            || shrinkage > bazaar::chk_map::INTERESTING_SHRINKAGE_LIMIT as isize
                    }
                };
                if do_remap {
                    new_node = InternalNode::_check_remap(slf.clone(), py, store.clone())?;
                }
            }
            let new_prefix = node_search_prefix(&new_node, py)?;
            if new_prefix.is_none() {
                return Err(pyo3::exceptions::PyAssertionError::new_err(
                    "_search_prefix should not be None",
                ));
            }
            let details = PyList::new(
                py,
                [PyTuple::new(
                    py,
                    [PyBytes::new(py, b"").into_any(), new_node],
                )?],
            )?;
            return Ok((new_prefix.unwrap().into_any().unbind(), details));
        }
        // Child split: wrap the pieces in a fresh InternalNode child.
        let child = internal_new_child(&slf, py, search_key.as_bytes(), true)?;
        let child = child.cast_into::<InternalNode>()?;
        child.setattr("_search_prefix", &prefix)?;
        for detail in node_details.iter() {
            let detail = detail.cast_into::<PyTuple>()?;
            let split = detail.get_item(0)?.cast_into::<PyBytes>()?;
            let node = detail.get_item(1)?;
            InternalNode::add_node(&mut child.borrow_mut(), py, split.as_bytes(), node)?;
        }
        let new_child_len: usize = child.borrow().len;
        {
            let mut me = slf.borrow_mut();
            me.len = me.len - old_len + new_child_len;
            me.key = None;
        }
        let self_prefix = slf.borrow().search_prefix.clone().unwrap_or_default();
        let details = PyList::new(
            py,
            [PyTuple::new(
                py,
                [PyBytes::new(py, b"").into_any(), slf.into_any()],
            )?],
        )?;
        Ok((PyBytes::new(py, &self_prefix).into_any().unbind(), details))
    }

    /// Remove `key` from the subtree, returning the (possibly collapsed)
    /// replacement node. Mirrors `_internal_unmap`.
    #[pyo3(signature = (store, key, check_remap = true))]
    fn unmap<'py>(
        slf: Bound<'py, Self>,
        py: Python<'py>,
        store: Bound<'py, PyAny>,
        key: Bound<'py, PyTuple>,
        check_remap: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        if slf.borrow().items.bind(py).is_empty() {
            return Err(pyo3::exceptions::PyAssertionError::new_err(
                "can't unmap in an empty InternalNode.",
            ));
        }
        let filter = PyList::new(py, [key.clone()])?;
        let nodes = InternalNode::_iter_nodes(
            slf.clone(),
            py,
            store.clone(),
            Some(filter.into_any()),
            None,
        )?;
        let first = nodes.try_iter()?.next();
        let child = match first {
            Some(pair) => pair?.cast_into::<PyTuple>()?.get_item(0)?,
            None => return Err(pyo3::exceptions::PyKeyError::new_err(format!("{:?}", key))),
        };
        slf.borrow_mut().len -= 1;
        let unmapped = child.call_method1("unmap", (store.clone(), key.clone()))?;
        slf.borrow_mut().key = None;
        let search_key = slf.borrow()._search_key(py, key)?;
        let unmapped_len: usize = unmapped.len()?;
        let mut unmapped_is_none = false;
        if unmapped_len == 0 {
            slf.borrow().items.bind(py).del_item(&search_key)?;
            unmapped_is_none = true;
        } else {
            slf.borrow()
                .items
                .bind(py)
                .set_item(&search_key, &unmapped)?;
        }
        if slf.borrow().items.bind(py).len() == 1 {
            let only = slf.borrow().items.bind(py).values().get_item(0)?;
            return Ok(only);
        }
        if !unmapped_is_none && unmapped.cast::<InternalNode>().is_ok() {
            return Ok(slf.into_any());
        }
        if check_remap {
            InternalNode::_check_remap(slf, py, store)
        } else {
            Ok(slf.into_any())
        }
    }
}

/// Create a new child node of `klass` under `search_key`, inheriting
/// max-size/key-width/search-key-func. `internal` picks InternalNode,
/// else LeafNode. Mirrors `_internal_new_child`.
fn internal_new_child<'py>(
    parent: &Bound<'py, InternalNode>,
    py: Python<'py>,
    search_key: &[u8],
    internal: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let (callable, maximum_size, key_width) = {
        let me = parent.borrow();
        (
            me.search_key_callable.as_ref().map(|c| c.bind(py).clone()),
            me.maximum_size,
            me.key_width,
        )
    };
    let child: Bound<'py, PyAny> = if internal {
        let node = Bound::new(py, InternalNode::new(py, None, callable)?)?;
        node.borrow_mut().maximum_size = maximum_size;
        node.borrow_mut().key_width = key_width;
        node.into_any()
    } else {
        let node = Bound::new(py, LeafNode::new(py, callable)?)?;
        node.borrow_mut().inner.maximum_size = maximum_size;
        node.borrow_mut().inner.key_width = key_width;
        node.into_any()
    };
    parent
        .borrow()
        .items
        .bind(py)
        .set_item(PyBytes::new(py, search_key), &child)?;
    Ok(child)
}

/// Read a node's `_search_prefix`, returning `None` for the Python
/// `None` sentinel (an empty internal node) and `Some(bytes)` otherwise.
fn node_search_prefix<'py>(
    node: &Bound<'py, PyAny>,
    py: Python<'py>,
) -> PyResult<Option<Bound<'py, PyBytes>>> {
    let sp = node.getattr("_search_prefix")?;
    if sp.is_none() {
        Ok(None)
    } else if sp.is(unknown_sentinel(py)) {
        // A leaf with an unknown prefix: compute it.
        let computed = node.call_method0("_compute_search_prefix")?;
        if computed.is_none() {
            Ok(None)
        } else {
            Ok(Some(computed.cast_into::<PyBytes>()?))
        }
    } else {
        Ok(Some(sp.cast_into::<PyBytes>()?))
    }
}

/// `bytes.decode(encoding)` via Python, returning the resulting `str`
/// object (so the caller can take its `repr()` with Python's quoting).
fn decode_bytes<'py>(py: Python<'py>, data: &[u8], encoding: &str) -> PyResult<Bound<'py, PyAny>> {
    PyBytes::new(py, data).call_method1("decode", (encoding,))
}

/// Python `repr(obj)` as a Rust `String`.
fn py_repr(obj: &Bound<'_, PyAny>) -> PyResult<String> {
    obj.repr()?.extract()
}

/// Recursively render `node` and its descendants into `lines`. Mirrors
/// the former Python `_chkmap_dump_tree_node`: internal nodes are
/// demand-loaded via `_iter_nodes` and their children walked in sorted
/// prefix order; leaf items are listed in sorted key order.
#[allow(clippy::too_many_arguments)]
fn dump_tree_node<'py>(
    py: Python<'py>,
    store: &Bound<'py, PyAny>,
    node: &Bound<'py, PyAny>,
    prefix: &[u8],
    indent: &str,
    encoding: &str,
    include_keys: bool,
    lines: &mut Vec<String>,
) -> PyResult<()> {
    let key_str = if include_keys {
        let node_key = node.call_method0("key")?;
        if node_key.is_none() {
            " None".to_string()
        } else {
            let first = node_key.cast_into::<PyTuple>()?.get_item(0)?;
            let decoded = decode_bytes(py, first.cast_into::<PyBytes>()?.as_bytes(), encoding)?;
            format!(" {}", decoded.extract::<String>()?)
        }
    } else {
        String::new()
    };
    let class_name = node.get_type().name()?;
    let prefix_repr = py_repr(&decode_bytes(py, prefix, encoding)?)?;
    lines.push(format!("{indent}{prefix_repr} {class_name}{key_str}"));

    if node.cast::<InternalNode>().is_ok() {
        // Demand-load all children, then walk them in sorted prefix order.
        let _ = InternalNode::_iter_nodes(
            node.clone().cast_into::<InternalNode>()?,
            py,
            store.clone(),
            None,
            None,
        )?
        .try_iter()?
        .collect::<PyResult<Vec<_>>>()?;
        let items = node.getattr("_items")?.cast_into::<PyDict>()?;
        let mut entries: Vec<(Vec<u8>, Bound<'py, PyAny>)> = Vec::new();
        for (k, v) in items.iter() {
            entries.push((k.cast_into::<PyBytes>()?.as_bytes().to_vec(), v));
        }
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        let child_indent = format!("{indent}  ");
        for (sub_prefix, sub) in entries {
            dump_tree_node(
                py,
                store,
                &sub,
                &sub_prefix,
                &child_indent,
                encoding,
                include_keys,
                lines,
            )?;
        }
    } else {
        let items = node.getattr("_items")?.cast_into::<PyDict>()?;
        let mut entries: Vec<(Vec<Vec<u8>>, Bound<'py, PyAny>)> = Vec::new();
        for (k, v) in items.iter() {
            let key_tuple = k.cast_into::<PyTuple>()?;
            let mut parts: Vec<Vec<u8>> = Vec::with_capacity(key_tuple.len());
            for p in key_tuple.iter() {
                parts.push(p.cast_into::<PyBytes>()?.as_bytes().to_vec());
            }
            entries.push((parts, v));
        }
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        for (key_parts, value) in entries {
            // Decode each key element, build a tuple, and take its repr.
            let decoded_key = PyTuple::new(
                py,
                key_parts
                    .iter()
                    .map(|p| decode_bytes(py, p, encoding))
                    .collect::<PyResult<Vec<_>>>()?,
            )?;
            let value_repr = py_repr(&decode_bytes(
                py,
                value.cast_into::<PyBytes>()?.as_bytes(),
                encoding,
            )?)?;
            lines.push(format!("      {} {}", py_repr(&decoded_key)?, value_repr));
        }
    }
    Ok(())
}

/// Is `obj` a loaded CHK node (LeafNode or InternalNode pyclass)?
fn is_node(obj: &Bound<'_, PyAny>) -> bool {
    obj.cast::<LeafNode>().is_ok() || obj.cast::<InternalNode>().is_ok()
}

/// Build the "invalid node type" assertion error matching Python.
fn invalid_node_type(obj: &Bound<'_, PyAny>) -> PyErr {
    pyo3::exceptions::PyAssertionError::new_err(format!(
        "Invalid node type: {}",
        obj.get_type()
            .name()
            .map(|n| n.to_string())
            .unwrap_or_default()
    ))
}

/// Queue an unloaded child for loading: `to_load[ref_tuple] = (prefix, filter)`,
/// preserving first-seen order in `key_order`.
fn record_to_load<'py>(
    to_load: &Bound<'py, PyDict>,
    key_order: &mut Vec<Py<PyAny>>,
    ref_tuple: &Bound<'py, PyAny>,
    prefix: &Bound<'py, PyAny>,
    filter: &Bound<'py, PyAny>,
) -> PyResult<()> {
    if !to_load.contains(ref_tuple)? {
        key_order.push(ref_tuple.clone().unbind());
    }
    let entry = PyTuple::new(ref_tuple.py(), [prefix.clone(), filter.clone()])?;
    to_load.set_item(ref_tuple, entry)?;
    Ok(())
}

/// Lazy iterator over `InternalNode._iter_nodes`. Yields already-resolved
/// `(node, filter)` pairs first, then demand-loads pending children from
/// the page cache and finally the store, replacing the `(sha1,)` tuple in
/// the parent's `_items` as each child loads.
#[pyclass(module = "bzrformats._bzr_rs.chk_map")]
pub struct InternalNodeIterator {
    parent: Py<InternalNode>,
    /// `(node, filter)` pairs already resolved during filtering.
    resolved: std::collections::VecDeque<Py<PyAny>>,
    /// `ref_tuple -> (prefix, filter)` for pending loads.
    to_load: Py<PyDict>,
    /// Pending ref tuples in first-seen order, still to try the cache.
    cache_queue: std::collections::VecDeque<Py<PyAny>>,
    /// Refs that missed the page cache, awaiting a store read.
    store_queue: Vec<Py<PyAny>>,
    /// Records buffered from the current store batch.
    store_buffer: std::collections::VecDeque<Py<PyAny>>,
    /// The store to demand-load pages from.
    store_handle: Py<PyAny>,
    batch_size: Option<usize>,
    callable: Option<Py<PyAny>>,
}

impl InternalNodeIterator {
    #[allow(clippy::too_many_arguments)]
    fn new_from<'py>(
        py: Python<'py>,
        parent: &Bound<'py, InternalNode>,
        store: Bound<'py, PyAny>,
        resolved: Bound<'py, PyList>,
        to_load: Bound<'py, PyDict>,
        key_order: Vec<Py<PyAny>>,
        batch_size: Option<usize>,
    ) -> PyResult<Bound<'py, InternalNodeIterator>> {
        let callable = parent
            .borrow()
            .search_key_callable
            .as_ref()
            .map(|c| c.clone_ref(py));
        let resolved_q: std::collections::VecDeque<Py<PyAny>> =
            resolved.iter().map(|p| p.unbind()).collect();
        Bound::new(
            py,
            InternalNodeIterator {
                parent: parent.clone().unbind(),
                resolved: resolved_q,
                to_load: to_load.unbind(),
                cache_queue: key_order.into_iter().collect(),
                store_queue: Vec::new(),
                store_buffer: std::collections::VecDeque::new(),
                store_handle: store.unbind(),
                batch_size,
                callable,
            },
        )
    }

    /// Resolve a loaded child: look up its `(prefix, filter)`, store the
    /// node back into the parent's `_items`, and return `(node, filter)`.
    fn resolve_loaded<'py>(
        &self,
        py: Python<'py>,
        ref_key: &Bound<'py, PyAny>,
        node: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        if !is_node(&node) {
            return Err(invalid_node_type(&node));
        }
        let entry = self
            .to_load
            .bind(py)
            .get_item(ref_key)?
            .unwrap()
            .cast_into::<PyTuple>()?;
        let prefix = entry.get_item(0)?;
        let node_key_filter = entry.get_item(1)?;
        self.parent
            .bind(py)
            .borrow()
            .items
            .bind(py)
            .set_item(&prefix, &node)?;
        PyTuple::new(py, [node, node_key_filter])
    }
}

#[pymethods]
impl InternalNodeIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyTuple>>> {
        use bazaar::chk_map::PageCache as _;
        if let Some(pair) = self.resolved.pop_front() {
            return Ok(Some(pair.into_bound(py).cast_into::<PyTuple>()?));
        }
        // Drain the page-cache queue; misses move to the store queue.
        while let Some(ref_key) = self.cache_queue.pop_front() {
            let ref_b = ref_key.bind(py);
            let sha1: Vec<u8> = ref_b
                .clone()
                .cast_into::<PyTuple>()?
                .get_item(0)?
                .cast_into::<PyBytes>()?
                .as_bytes()
                .to_vec();
            match page_cache().get(&sha1) {
                Some(bytes) => {
                    let ref_tuple = ref_b.clone().cast_into::<PyTuple>()?;
                    let node = py_deserialise(py, &bytes, ref_tuple, self.callable_bound(py))?;
                    return Ok(Some(self.resolve_loaded(py, ref_b, node)?));
                }
                None => self.store_queue.push(ref_key.clone_ref(py)),
            }
        }
        // Serve already-resolved pairs from the current store batch.
        if let Some(pair) = self.store_buffer.pop_front() {
            return Ok(Some(pair.into_bound(py).cast_into::<PyTuple>()?));
        }
        // Fetch the next store batch. Like Python, set `_items` for every
        // record in the batch up front (even if the consumer stops early),
        // then buffer the resolved pairs for yielding.
        if !self.store_queue.is_empty() {
            let batch_size = self.batch_size.unwrap_or(self.store_queue.len());
            let take = batch_size.min(self.store_queue.len());
            let this_batch: Vec<Py<PyAny>> = self.store_queue.drain(..take).collect();
            let batch = PyList::empty(py);
            for k in &this_batch {
                batch.append(k.bind(py))?;
            }
            let store_obj = self.store_handle.bind(py).clone();
            let stream = store_obj.call_method1("get_record_stream", (batch, "unordered", true))?;
            for record in stream.try_iter()? {
                let pair = self.consume_record(py, record?)?;
                self.store_buffer.push_back(pair.unbind().into_any());
            }
            if let Some(pair) = self.store_buffer.pop_front() {
                return Ok(Some(pair.into_bound(py).cast_into::<PyTuple>()?));
            }
        }
        Ok(None)
    }
}

impl InternalNodeIterator {
    fn callable_bound<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyAny>> {
        self.callable.as_ref().map(|c| c.bind(py).clone())
    }

    /// Deserialise a store record, cache its bytes, and resolve it into
    /// the `(node, filter)` pair.
    fn consume_record<'py>(
        &self,
        py: Python<'py>,
        record: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        use bazaar::chk_map::PageCache as _;
        let bytes_obj = record.call_method1("get_bytes_as", ("fulltext",))?;
        let bytes_py = bytes_obj.cast_into::<PyBytes>()?;
        let rec_key = record.getattr("key")?;
        let rec_key_tuple = rec_key.clone().cast_into::<PyTuple>()?;
        let sha1: Vec<u8> = rec_key_tuple
            .get_item(0)?
            .cast_into::<PyBytes>()?
            .as_bytes()
            .to_vec();
        let node = py_deserialise(
            py,
            bytes_py.as_bytes(),
            rec_key_tuple,
            self.callable_bound(py),
        )?;
        page_cache().insert(sha1, bytes_py.as_bytes().to_vec());
        self.resolve_loaded(py, &rec_key, node)
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

    /// Iterate over the entire CHKMap's contents, optionally
    /// filtered by `key_filter`. Mirrors Python's `_chkmap_iteritems`.
    #[pyo3(signature = (key_filter=None))]
    fn iteritems<'py>(
        slf: Bound<'py, Self>,
        py: Python<'py>,
        key_filter: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        Self::_ensure_root(slf.clone(), py)?;
        let root = slf.borrow().root_node.clone_ref(py);
        let root_bound = root.bind(py);
        if root_bound.clone().cast_into::<PyTuple>().is_ok() {
            return Err(pyo3::exceptions::PyAssertionError::new_err(
                "Cannot iterate over a map with a tuple root node",
            ));
        }
        // Normalise key_filter: each entry must be a tuple.
        let normalised_filter = match key_filter {
            None => None,
            Some(kf) => {
                let lst = PyList::empty(py);
                for k in kf.try_iter()? {
                    let k = k?;
                    if k.clone().cast_into::<PyTuple>().is_ok() {
                        lst.append(k)?;
                    } else {
                        // tuple(k)
                        let t = pyo3::types::PyTuple::new(
                            py,
                            k.try_iter()?.collect::<PyResult<Vec<_>>>()?,
                        )?;
                        lst.append(t)?;
                    }
                }
                Some(lst.into_any())
            }
        };
        let store = slf.borrow().store.clone_ref(py);
        let kwargs = PyDict::new(py);
        if let Some(kf) = &normalised_filter {
            kwargs.set_item("key_filter", kf)?;
        }
        let iter = root_bound.call_method("iteritems", (store,), Some(&kwargs))?;
        Ok(iter.call_method0("__iter__")?)
    }

    /// Drop `key` from the map, possibly collapsing internal nodes.
    /// Mirrors Python's `_chkmap_unmap`.
    #[pyo3(signature = (key, check_remap=true))]
    fn unmap<'py>(
        slf: Bound<'py, Self>,
        py: Python<'py>,
        key: Bound<'py, PyAny>,
        check_remap: bool,
    ) -> PyResult<()> {
        Self::_ensure_root(slf.clone(), py)?;
        let root = slf.borrow().root_node.clone_ref(py);
        let store = slf.borrow().store.clone_ref(py);
        let is_internal = root.bind(py).is_instance_of::<InternalNode>();
        let unmapped = if is_internal {
            let kwargs = PyDict::new(py);
            kwargs.set_item("check_remap", check_remap)?;
            root.bind(py)
                .call_method("unmap", (store, key), Some(&kwargs))?
        } else {
            root.bind(py).call_method1("unmap", (store, key))?
        };
        slf.borrow_mut().root_node = unmapped.unbind();
        Ok(())
    }

    /// Force an internal-node remap check. Mirrors Python's
    /// `_chkmap_check_remap`.
    fn _check_remap(slf: Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        Self::_ensure_root(slf.clone(), py)?;
        let root = slf.borrow().root_node.clone_ref(py);
        if root.bind(py).is_instance_of::<InternalNode>() {
            let store = slf.borrow().store.clone_ref(py);
            let new_root = root.bind(py).call_method1("_check_remap", (store,))?;
            slf.borrow_mut().root_node = new_root.unbind();
        }
        Ok(())
    }

    /// Save the map completely; return the root key. Mirrors Python's
    /// `_chkmap_save`.
    fn _save<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let root = self.root_node.bind(py);
        if root.clone().cast_into::<PyTuple>().is_ok() {
            return Ok(root.clone());
        }
        let store = self.store.bind(py);
        let keys: Vec<Bound<'py, PyAny>> = root
            .call_method1("serialise", (store,))?
            .try_iter()?
            .collect::<PyResult<Vec<_>>>()?;
        keys.into_iter().next_back().ok_or_else(|| {
            pyo3::exceptions::PyAssertionError::new_err("serialise returned no keys")
        })
    }

    /// Map `key` to `value`. May replace the root with a fresh
    /// InternalNode if the map split. Mirrors Python's `_chkmap_map`.
    fn map<'py>(
        slf: Bound<'py, Self>,
        py: Python<'py>,
        key: Bound<'py, PyAny>,
        value: Bound<'py, PyAny>,
    ) -> PyResult<()> {
        // Coerce key to tuple.
        let key_tuple = if key.clone().cast_into::<PyTuple>().is_ok() {
            key
        } else {
            PyTuple::new(py, key.try_iter()?.collect::<PyResult<Vec<_>>>()?)?.into_any()
        };
        Self::_ensure_root(slf.clone(), py)?;
        let root = slf.borrow().root_node.clone_ref(py);
        if root.bind(py).clone().cast_into::<PyTuple>().is_ok() {
            return Err(pyo3::exceptions::PyAssertionError::new_err(
                "Cannot map a key to a tuple root node",
            ));
        }
        let store = slf.borrow().store.clone_ref(py);
        let result = root
            .bind(py)
            .call_method1("map", (store, key_tuple, value))?;
        let result_tup = result.cast_into::<PyTuple>()?;
        let prefix = result_tup.get_item(0)?;
        let node_details = result_tup.get_item(1)?;
        let node_details: Vec<Bound<'py, PyAny>> =
            node_details.try_iter()?.collect::<PyResult<Vec<_>>>()?;
        if node_details.len() == 1 {
            let pair = node_details[0].clone().cast_into::<PyTuple>()?;
            slf.borrow_mut().root_node = pair.get_item(1)?.unbind();
        } else {
            // Build a new InternalNode covering all splits.
            let internal_cls = py.get_type::<InternalNode>();
            let search_key_callable = slf
                .borrow()
                .search_key_callable
                .as_ref()
                .map(|c| c.clone_ref(py));
            let kwargs = PyDict::new(py);
            if let Some(cb) = &search_key_callable {
                kwargs.set_item("search_key_func", cb)?;
            }
            let new_root = internal_cls.call((prefix,), Some(&kwargs))?;
            let first = node_details[0].clone().cast_into::<PyTuple>()?;
            let first_node = first.get_item(1)?;
            let first_max: usize = first_node.getattr("maximum_size")?.extract()?;
            new_root.call_method1("set_maximum_size", (first_max,))?;
            let first_kw: usize = first_node.getattr("_key_width")?.extract()?;
            new_root.setattr("_key_width", first_kw)?;
            for d in &node_details {
                let pair = d.clone().cast_into::<PyTuple>()?;
                let split = pair.get_item(0)?;
                let node = pair.get_item(1)?;
                new_root.call_method1("add_node", (split, node))?;
            }
            slf.borrow_mut().root_node = new_root.unbind();
        }
        Ok(())
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
        let sha1: Vec<u8> = key.get_item(0)?.cast_into::<PyBytes>()?.as_bytes().to_vec();
        if let Some(cached) = page_cache().get(&sha1) {
            return Ok(PyBytes::new(py, &cached));
        }
        let keys = PyList::new(py, [key.clone()])?;
        let stream = self
            .store
            .bind(py)
            .call_method1("get_record_stream", (keys, "unordered", true))?;
        let iter = stream.try_iter()?;
        let record = iter.into_iter().next().ok_or_else(|| {
            pyo3::exceptions::PyKeyError::new_err(format!("no record returned for key {:?}", key))
        })??;
        let bytes_obj = record.call_method1("get_bytes_as", ("fulltext",))?;
        let bytes_py = bytes_obj.cast_into::<PyBytes>()?;
        let bytes_vec = bytes_py.as_bytes().to_vec();
        page_cache().insert(sha1, bytes_vec);
        Ok(bytes_py)
    }

    /// Apply a `(old_key, new_key, value)` delta and save, returning the
    /// new root key. Mirrors the former Python `_chkmap_apply_delta`.
    fn apply_delta<'py>(
        slf: Bound<'py, Self>,
        py: Python<'py>,
        delta: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Collect the delta once; it may be a one-shot iterable.
        let entries: Vec<(Bound<'py, PyAny>, Bound<'py, PyAny>, Bound<'py, PyAny>)> = {
            let mut out = Vec::new();
            for entry in delta.try_iter()? {
                let tup = entry?.cast_into::<PyTuple>()?;
                out.push((tup.get_item(0)?, tup.get_item(1)?, tup.get_item(2)?));
            }
            out
        };
        // New keys (added, not moved) must not already exist.
        let new_items = pyo3::types::PySet::empty(py)?;
        for (old, new, _value) in &entries {
            if !new.is_none() && old.is_none() {
                let key_tuple = if new.clone().cast_into::<PyTuple>().is_ok() {
                    new.clone()
                } else {
                    PyTuple::new(py, new.try_iter()?.collect::<PyResult<Vec<_>>>()?)?.into_any()
                };
                new_items.add(key_tuple)?;
            }
        }
        let existing_new: Vec<Bound<'py, PyAny>> =
            Self::iteritems(slf.clone(), py, Some(new_items.into_any()))?
                .try_iter()?
                .collect::<PyResult<Vec<_>>>()?;
        if !existing_new.is_empty() {
            let msg = format!(
                "New items are already in the map {}",
                PyList::new(py, &existing_new)?.repr()?
            );
            return Err(InconsistentDeltaDelta::new_err((
                delta.clone().unbind(),
                msg,
            )));
        }
        let mut has_deletes = false;
        for (old, new, _value) in &entries {
            if !old.is_none() && !old.eq(new)? {
                Self::unmap(slf.clone(), py, old.clone(), false)?;
                has_deletes = true;
            }
        }
        for (_old, new, value) in &entries {
            if !new.is_none() {
                Self::map(slf.clone(), py, new.clone(), value.clone())?;
            }
        }
        if has_deletes {
            Self::_check_remap(slf.clone(), py)?;
        }
        slf.borrow()._save(py)
    }

    /// Yield `(key, old_value, new_value)` for every difference between
    /// this map and `basis`. Delegates to the pure-crate diff algorithm,
    /// which demand-loads pages through each side's store and skips
    /// identical subtrees. Mirrors the former Python `_chkmap_iter_changes`.
    fn iter_changes<'py>(
        slf: Bound<'py, Self>,
        py: Python<'py>,
        basis: Bound<'py, Self>,
    ) -> PyResult<Bound<'py, PyList>> {
        let out = PyList::empty(py);
        let mut self_map = slf.borrow().build_pure_map(py)?;
        let mut basis_map = basis.borrow().build_pure_map(py)?;
        let changes = self_map
            .iter_changes(&mut basis_map)
            .map_err(chk_err_to_py)?;
        for (key, old, new) in changes {
            let key_tuple = PyTuple::new(py, key.iter().map(|p| PyBytes::new(py, p.as_slice())))?;
            let old_obj = match old {
                Some(v) => PyBytes::new(py, &v).into_any(),
                None => py.None().into_bound(py),
            };
            let new_obj = match new {
                Some(v) => PyBytes::new(py, &v).into_any(),
                None => py.None().into_bound(py),
            };
            out.append(PyTuple::new(py, [key_tuple.into_any(), old_obj, new_obj])?)?;
        }
        Ok(out)
    }

    /// Render the tree as an indented, human-readable string for
    /// debugging. Mirrors the former Python `_chkmap_dump_tree`.
    #[pyo3(signature = (include_keys = false, encoding = "utf-8"))]
    fn _dump_tree(
        slf: Bound<'_, Self>,
        py: Python<'_>,
        include_keys: bool,
        encoding: &str,
    ) -> PyResult<String> {
        Self::_ensure_root(slf.clone(), py)?;
        let root = slf.borrow().root_node.clone_ref(py);
        let store = slf.borrow().store.clone_ref(py);
        let mut lines: Vec<String> = Vec::new();
        dump_tree_node(
            py,
            store.bind(py),
            root.bind(py),
            b"",
            "",
            encoding,
            include_keys,
            &mut lines,
        )?;
        lines.push(String::new());
        Ok(lines.join("\n"))
    }

    /// Create a CHKMap in `store` from `initial_value`, returning the
    /// root key. Mirrors the former Python `_chkmap_from_dict`.
    #[classmethod]
    #[pyo3(signature = (store, initial_value, maximum_size = 0, key_width = 1, search_key_func = None))]
    fn from_dict<'py>(
        cls: &Bound<'py, pyo3::types::PyType>,
        py: Python<'py>,
        store: Bound<'py, PyAny>,
        initial_value: Bound<'py, PyAny>,
        maximum_size: usize,
        key_width: usize,
        search_key_func: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let root_key = Self::_create_directly(
            cls,
            py,
            store,
            initial_value,
            maximum_size,
            key_width,
            search_key_func,
        )?;
        if root_key.clone().cast_into::<PyTuple>().is_err() {
            return Err(pyo3::exceptions::PyAssertionError::new_err(format!(
                "we got a {} instead of a tuple",
                root_key.get_type().name()?
            )));
        }
        Ok(root_key)
    }

    /// Build a CHKMap by applying every item as a delta. Slower than
    /// `_create_directly` but exercises the map/split path; used by
    /// tests. Mirrors the former Python `_chkmap_create_via_map`.
    #[classmethod]
    #[pyo3(signature = (store, initial_value, maximum_size = 0, key_width = 1, search_key_func = None))]
    fn _create_via_map<'py>(
        cls: &Bound<'py, pyo3::types::PyType>,
        py: Python<'py>,
        store: Bound<'py, PyAny>,
        initial_value: Bound<'py, PyAny>,
        maximum_size: usize,
        key_width: usize,
        search_key_func: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let kwargs = PyDict::new(py);
        if let Some(skf) = &search_key_func {
            kwargs.set_item("search_key_func", skf)?;
        }
        let result = cls.call((store, py.None()), Some(&kwargs))?;
        let root = result.getattr("_root_node")?;
        if !is_node(&root) {
            return Err(pyo3::exceptions::PyAssertionError::new_err(
                "expected root node to be Node",
            ));
        }
        root.call_method1("set_maximum_size", (maximum_size,))?;
        root.setattr("_key_width", key_width)?;
        let delta = PyList::empty(py);
        for item in initial_value.call_method0("items")?.try_iter()? {
            let pair = item?.cast_into::<PyTuple>()?;
            let key = pair.get_item(0)?;
            let value = pair.get_item(1)?;
            delta.append(PyTuple::new(py, [py.None().into_bound(py), key, value])?)?;
        }
        result.call_method1("apply_delta", (delta,))
    }

    /// Build a CHKMap directly: pack everything into a leaf, split into an
    /// InternalNode if it overflows, then serialise. Mirrors the former
    /// Python `_chkmap_create_directly`.
    #[classmethod]
    #[pyo3(signature = (store, initial_value, maximum_size = 0, key_width = 1, search_key_func = None))]
    fn _create_directly<'py>(
        _cls: &Bound<'py, pyo3::types::PyType>,
        py: Python<'py>,
        store: Bound<'py, PyAny>,
        initial_value: Bound<'py, PyAny>,
        maximum_size: usize,
        key_width: usize,
        search_key_func: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let leaf = Bound::new(py, LeafNode::new(py, search_key_func.clone())?)?;
        {
            let mut l = leaf.borrow_mut();
            l.inner.maximum_size = maximum_size;
            l.inner.key_width = key_width;
            let mut items: indexmap::IndexMap<Vec<Vec<u8>>, Vec<u8>> = indexmap::IndexMap::new();
            let mut raw_size = 0usize;
            for item in initial_value.call_method0("items")?.try_iter()? {
                let pair = item?.cast_into::<PyTuple>()?;
                let key_tuple = pair.get_item(0)?.cast_into::<PyTuple>()?;
                let mut parts: Vec<Vec<u8>> = Vec::with_capacity(key_tuple.len());
                for p in key_tuple.iter() {
                    parts.push(p.cast_into::<PyBytes>()?.as_bytes().to_vec());
                }
                let value = pair
                    .get_item(1)?
                    .cast_into::<PyBytes>()?
                    .as_bytes()
                    .to_vec();
                raw_size += leaf_node_key_value_len(&parts, &value);
                items.insert(parts, value);
            }
            l.inner.items = items;
            l.inner.raw_size = raw_size;
        }
        leaf.borrow_mut().inner.compute_search_prefix();
        leaf.borrow_mut().inner.compute_serialised_prefix();
        let (len, current_size) = {
            let l = leaf.borrow();
            (l.inner.len(), l.inner.current_size())
        };
        let node: Bound<'py, PyAny> = if len > 1 && maximum_size != 0 && current_size > maximum_size
        {
            let mapped = LeafNode::_split(leaf.clone(), py, store.clone())?;
            let (prefix_obj, node_details) = mapped;
            let node_details = node_details;
            if node_details.len() == 1 {
                return Err(pyo3::exceptions::PyAssertionError::new_err(
                    "Failed to split using node._split",
                ));
            }
            let prefix = prefix_obj.bind(py).cast::<PyBytes>()?.as_bytes().to_vec();
            let internal = Bound::new(
                py,
                InternalNode::new(py, Some(&prefix), search_key_func.clone())?,
            )?;
            internal.borrow_mut().maximum_size = maximum_size;
            internal.borrow_mut().key_width = key_width;
            for d in node_details.iter() {
                let pair = d.cast_into::<PyTuple>()?;
                let split = pair.get_item(0)?.cast_into::<PyBytes>()?;
                let subnode = pair.get_item(1)?;
                InternalNode::add_node(&mut internal.borrow_mut(), py, split.as_bytes(), subnode)?;
            }
            internal.into_any()
        } else {
            leaf.into_any()
        };
        let keys: Vec<Bound<'py, PyAny>> = node
            .call_method1("serialise", (store,))?
            .try_iter()?
            .collect::<PyResult<Vec<_>>>()?;
        keys.into_iter().next_back().ok_or_else(|| {
            pyo3::exceptions::PyAssertionError::new_err("serialise returned no keys")
        })
    }
}

impl CHKMap {
    /// Build a pure-crate `CHKMap` over this map's Python store, seeded
    /// from the current root key. Used to delegate `iter_changes` to the
    /// pure diff algorithm. Reads the root key without forcing a load, so
    /// the pyo3 root node is left untouched.
    fn build_pure_map(
        &self,
        py: Python<'_>,
    ) -> PyResult<bazaar::chk_map::CHKMap<crate::versionedfile::PyVersionedFiles>> {
        let root = self.root_node.bind(py);
        let root_key: Option<Vec<u8>> = if let Ok(t) = root.clone().cast_into::<PyTuple>() {
            Some(t.get_item(0)?.cast_into::<PyBytes>()?.as_bytes().to_vec())
        } else {
            let k = root.call_method0("key")?;
            if k.is_none() {
                None
            } else {
                Some(
                    k.cast_into::<PyTuple>()?
                        .get_item(0)?
                        .cast_into::<PyBytes>()?
                        .as_bytes()
                        .to_vec(),
                )
            }
        };
        let store = std::sync::Arc::new(crate::versionedfile::PyVersionedFiles::new(
            self.store.clone_ref(py),
        ));
        let cache: std::sync::Arc<dyn bazaar::chk_map::PageCache> =
            std::sync::Arc::new(GlobalPageCache);
        Ok(bazaar::chk_map::CHKMap::new(
            store,
            cache,
            root_key,
            self.search_key_func.clone(),
        ))
    }

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
        let bytes = slf.borrow()._read_bytes(py, key_tuple.clone())?;
        let data = bytes.as_bytes();
        let search_key_callable = slf
            .borrow()
            .search_key_callable
            .as_ref()
            .map(|c| c.bind(py).clone());
        if data.starts_with(b"chkleaf:\n") {
            let cls = py.get_type::<LeafNode>();
            cls.call_method("deserialise", (bytes, key_tuple, search_key_callable), None)
        } else if data.starts_with(b"chknode:\n") {
            let cls = py.get_type::<InternalNode>();
            cls.call_method("deserialise", (bytes, key_tuple, search_key_callable), None)
        } else {
            Err(pyo3::exceptions::PyAssertionError::new_err(
                "Unknown node type.",
            ))
        }
    }
}

/// Build a `bzrformats.errors.NoSuchRevision(store, key)` error, matching
/// what the Python difference algorithm raises for an absent record.
fn no_such_revision(store: &Bound<'_, PyAny>, key: &Bound<'_, PyAny>) -> PyErr {
    NoSuchRevision::new_err((store.clone().unbind(), key.clone().unbind()))
}

/// A CHK page reference: the flat sha1 bytes that form the single
/// element of a `(b"sha1:...",)` key tuple.
type ChkRef = Vec<u8>;
/// A flat (key, value) item from a leaf node: the key is the list of
/// tuple elements, the value the stored bytes.
type ChkItem = (Vec<Vec<u8>>, Vec<u8>);
/// One `process()` result: an optional record (None for the items-only
/// flush) paired with the items in/under that page.
type DiffResult = (Option<Py<PyAny>>, Vec<ChkItem>);

/// The parsed contents of one stored CHK page, as the difference
/// algorithm needs them.
struct ReadNode {
    record: Py<PyAny>,
    /// `(prefix, ref)` pairs for an internal node; empty for a leaf.
    prefix_refs: Vec<(Vec<u8>, ChkRef)>,
    /// `(key, value)` pairs for a leaf node; empty for an internal node.
    items: Vec<ChkItem>,
}

/// Convert a flat sha1 ref into the `(ref,)` key tuple the store and
/// records use.
fn ref_to_key_tuple<'py>(py: Python<'py>, r: &[u8]) -> PyResult<Bound<'py, PyTuple>> {
    PyTuple::new(py, [PyBytes::new(py, r)])
}

/// Iterate the stored pages and (key, value) pairs that are in any of
/// the new maps and not in any of the old maps. Rust port of the
/// Python `chk_map.CHKMapDifference`.
#[pyclass(module = "bzrformats._bzr_rs.chk_map", name = "CHKMapDifference")]
pub struct CHKMapDifference {
    store: Py<PyAny>,
    new_root_keys: Vec<ChkRef>,
    old_root_keys: Vec<ChkRef>,
    pb: Option<Py<PyAny>>,
    search_key_func: Py<PyAny>,
    all_old_chks: std::collections::HashSet<ChkRef>,
    all_old_items: std::collections::HashSet<ChkItem>,
    processed_new_refs: std::collections::HashSet<ChkRef>,
    old_queue: Vec<ChkRef>,
    new_queue: Vec<ChkRef>,
    new_item_queue: Vec<ChkItem>,
}

impl CHKMapDifference {
    /// Read the given keys from the store and parse each page into the
    /// prefix-refs / items the algorithm consumes. Mirrors
    /// `_read_nodes_from_store`.
    fn read_nodes_from_store(&self, py: Python<'_>, keys: &[ChkRef]) -> PyResult<Vec<ReadNode>> {
        let key_tuples = PyList::empty(py);
        for k in keys {
            key_tuples.append(ref_to_key_tuple(py, k)?)?;
        }
        let stream = self
            .store
            .bind(py)
            .call_method1("get_record_stream", (key_tuples, "unordered", true))?;
        let mut out = Vec::new();
        for record in stream.try_iter()? {
            let record = record?;
            if let Some(pb) = &self.pb {
                pb.bind(py).call_method0("tick")?;
            }
            let storage_kind: String = record.getattr("storage_kind")?.extract()?;
            if storage_kind == "absent" {
                let key = record.getattr("key")?;
                return Err(no_such_revision(self.store.bind(py), &key));
            }
            let bytes_obj = record.call_method1("get_bytes_as", ("fulltext",))?;
            let bytes_py = bytes_obj.cast_into::<PyBytes>()?;
            let data = bytes_py.as_bytes();
            let (prefix_refs, items) = if data.starts_with(b"chknode:\n") {
                let parsed = deserialise_internal_node(data).map_err(chk_err_to_py)?;
                (parsed.items, Vec::new())
            } else if data.starts_with(b"chkleaf:\n") {
                let parsed = deserialise_leaf_node(data).map_err(chk_err_to_py)?;
                (Vec::new(), parsed.items)
            } else {
                return Err(pyo3::exceptions::PyAssertionError::new_err(
                    "Unknown node type.",
                ));
            };
            out.push(ReadNode {
                record: record.unbind(),
                prefix_refs,
                items,
            });
        }
        Ok(out)
    }

    /// Compute the search key for a leaf item's key by calling the
    /// Python `search_key_func`. Mirrors `self._search_key_func(item[0])`.
    fn search_key_for_item(&self, py: Python<'_>, key: &[Vec<u8>]) -> PyResult<Vec<u8>> {
        let key_tuple = PyTuple::new(py, key.iter().map(|p| PyBytes::new(py, p)))?;
        let result = self.search_key_func.bind(py).call1((key_tuple,))?;
        Ok(result.cast_into::<PyBytes>()?.as_bytes().to_vec())
    }

    /// `_read_old_roots`: walk the old roots, recording their items and
    /// chk refs, and return the `(prefix, ref)` pairs still to enqueue.
    fn read_old_roots(&mut self, py: Python<'_>) -> PyResult<Vec<(Vec<u8>, ChkRef)>> {
        let mut old_chks_to_enqueue = Vec::new();
        let nodes = self.read_nodes_from_store(py, &self.old_root_keys.clone())?;
        for node in nodes {
            let prefix_refs: Vec<(Vec<u8>, ChkRef)> = node
                .prefix_refs
                .into_iter()
                .filter(|(_, r)| !self.all_old_chks.contains(r))
                .collect();
            for (_, r) in &prefix_refs {
                self.all_old_chks.insert(r.clone());
            }
            for item in node.items {
                self.all_old_items.insert(item);
            }
            old_chks_to_enqueue.extend(prefix_refs);
        }
        Ok(old_chks_to_enqueue)
    }

    /// `_enqueue_old`: queue old refs whose prefix is still in the
    /// remaining interesting prefix set.
    fn enqueue_old(
        &mut self,
        new_prefixes: &std::collections::HashSet<Vec<u8>>,
        old_chks_to_enqueue: Vec<(Vec<u8>, ChkRef)>,
    ) {
        for (prefix, r) in old_chks_to_enqueue {
            let mut interesting = false;
            for i in (1..=prefix.len()).rev() {
                if new_prefixes.contains(&prefix[..i]) {
                    interesting = true;
                    break;
                }
            }
            if interesting {
                self.old_queue.push(r);
            }
        }
    }

    /// `_read_all_roots`: bootstrap phase. Returns the new-root records
    /// to be yielded (each paired with an empty item list by `process`).
    fn read_all_roots(&mut self, py: Python<'_>) -> PyResult<Vec<Py<PyAny>>> {
        if self.old_root_keys.is_empty() {
            self.new_queue = self.new_root_keys.clone();
            return Ok(Vec::new());
        }
        let old_chks_to_enqueue = self.read_old_roots(py)?;
        let new_keys: Vec<ChkRef> = self
            .new_root_keys
            .iter()
            .filter(|k| !self.all_old_chks.contains(*k))
            .cloned()
            .collect();
        let mut new_prefixes: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
        for k in &new_keys {
            self.processed_new_refs.insert(k.clone());
        }
        let mut records = Vec::new();
        let nodes = self.read_nodes_from_store(py, &new_keys)?;
        for node in nodes {
            let prefix_refs: Vec<(Vec<u8>, ChkRef)> = node
                .prefix_refs
                .into_iter()
                .filter(|(_, r)| {
                    !self.all_old_chks.contains(r) && !self.processed_new_refs.contains(r)
                })
                .collect();
            let refs: Vec<ChkRef> = prefix_refs.iter().map(|(_, r)| r.clone()).collect();
            for (p, _) in &prefix_refs {
                new_prefixes.insert(p.clone());
            }
            self.new_queue.extend(refs.iter().cloned());
            let new_items: Vec<ChkItem> = node
                .items
                .into_iter()
                .filter(|item| !self.all_old_items.contains(item))
                .collect();
            for item in &new_items {
                new_prefixes.insert(self.search_key_for_item(py, &item.0)?);
            }
            self.new_item_queue.extend(new_items);
            for r in &refs {
                self.processed_new_refs.insert(r.clone());
            }
            records.push(node.record);
        }
        // Expand new_prefixes to include all shorter prefixes.
        let full: Vec<Vec<u8>> = new_prefixes.iter().cloned().collect();
        for prefix in full {
            for i in 1..prefix.len() {
                new_prefixes.insert(prefix[..i].to_vec());
            }
        }
        self.enqueue_old(&new_prefixes, old_chks_to_enqueue);
        Ok(records)
    }

    /// `_process_next_old`: drain the old queue one pass, recording
    /// items and discovering further old refs.
    fn process_next_old(&mut self, py: Python<'_>) -> PyResult<()> {
        let refs = std::mem::take(&mut self.old_queue);
        let nodes = self.read_nodes_from_store(py, &refs)?;
        for node in nodes {
            for item in node.items {
                self.all_old_items.insert(item);
            }
            let new_refs: Vec<ChkRef> = node
                .prefix_refs
                .into_iter()
                .map(|(_, r)| r)
                .filter(|r| !self.all_old_chks.contains(r))
                .collect();
            for r in &new_refs {
                self.all_old_chks.insert(r.clone());
            }
            self.old_queue.extend(new_refs);
        }
        Ok(())
    }
}

#[pymethods]
impl CHKMapDifference {
    #[new]
    #[pyo3(signature = (store, new_root_keys, old_root_keys, search_key_func, pb = None))]
    fn new(
        store: Bound<'_, PyAny>,
        new_root_keys: Bound<'_, PyAny>,
        old_root_keys: Bound<'_, PyAny>,
        search_key_func: Bound<'_, PyAny>,
        pb: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let new_root_keys = extract_chk_refs(&new_root_keys)?;
        let old_root_keys = extract_chk_refs(&old_root_keys)?;
        let all_old_chks: std::collections::HashSet<ChkRef> =
            old_root_keys.iter().cloned().collect();
        Ok(Self {
            store: store.unbind(),
            new_root_keys,
            old_root_keys,
            pb: pb.map(|p| p.unbind()),
            search_key_func: search_key_func.unbind(),
            all_old_chks,
            all_old_items: std::collections::HashSet::new(),
            processed_new_refs: std::collections::HashSet::new(),
            old_queue: Vec::new(),
            new_queue: Vec::new(),
            new_item_queue: Vec::new(),
        })
    }

    /// Yield `(record, items)` tuples for pages and key-value pairs that
    /// are in the new maps but not the old maps.
    fn process(slf: Bound<'_, Self>, py: Python<'_>) -> PyResult<CHKDifferenceIterator> {
        // Bootstrap: read roots, capturing the records to yield first.
        let root_records = slf.borrow_mut().read_all_roots(py)?;
        Ok(CHKDifferenceIterator {
            diff: slf.unbind(),
            root_records: root_records.into(),
            phase: DiffPhase::Roots,
            flush_refs: Vec::new(),
            pending: std::collections::VecDeque::new(),
        })
    }

    // ----- whitebox state accessors (mirror the Python attributes) -----

    #[getter]
    fn _all_old_chks<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::types::PySet>> {
        let set = pyo3::types::PySet::empty(py)?;
        for r in &self.all_old_chks {
            set.add(ref_to_key_tuple(py, r)?)?;
        }
        Ok(set)
    }

    #[getter]
    fn _old_queue<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        refs_to_key_list(py, &self.old_queue)
    }

    #[getter]
    fn _new_queue<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        refs_to_key_list(py, &self.new_queue)
    }

    #[getter]
    fn _new_item_queue<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let list = PyList::empty(py);
        for (key, value) in &self.new_item_queue {
            let key_tuple = PyTuple::new(py, key.iter().map(|p| PyBytes::new(py, p)))?;
            let pair = PyTuple::new(
                py,
                [key_tuple.into_any(), PyBytes::new(py, value).into_any()],
            )?;
            list.append(pair)?;
        }
        Ok(list)
    }

    /// Read the root pages, populating the queues, and return the new-root
    /// records. Mirrors the Python generator `_read_all_roots`.
    fn _read_all_roots<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let records = self.read_all_roots(py)?;
        let list = PyList::empty(py);
        for r in records {
            list.append(r.into_bound(py))?;
        }
        Ok(list)
    }

    /// Process one pass of the old queue. Mirrors `_process_next_old`.
    fn _process_next_old(&mut self, py: Python<'_>) -> PyResult<()> {
        self.process_next_old(py)
    }
}

/// Render a list of refs as a Python list of `(ref,)` key tuples.
fn refs_to_key_list<'py>(py: Python<'py>, refs: &[ChkRef]) -> PyResult<Bound<'py, PyList>> {
    let list = PyList::empty(py);
    for r in refs {
        list.append(ref_to_key_tuple(py, r)?)?;
    }
    Ok(list)
}

/// Extract a sequence of `(b"sha1:...",)` key tuples into flat refs.
fn extract_chk_refs(obj: &Bound<'_, PyAny>) -> PyResult<Vec<ChkRef>> {
    let mut out = Vec::new();
    for item in obj.try_iter()? {
        let item = item?;
        let tuple = item.cast_into::<PyTuple>()?;
        let first = tuple.get_item(0)?;
        out.push(first.cast_into::<PyBytes>()?.as_bytes().to_vec());
    }
    Ok(out)
}

#[derive(PartialEq)]
enum DiffPhase {
    /// Yielding new-root records as `(record, [])`.
    Roots,
    /// Draining the old queue, then emitting the buffered new items.
    DrainOld,
    /// Walking the new queue breadth-first, yielding `(record, items)`.
    FlushNew,
    Done,
}

/// Lazy iterator over `CHKMapDifference.process()` results.
#[pyclass(module = "bzrformats._bzr_rs.chk_map")]
pub struct CHKDifferenceIterator {
    diff: Py<CHKMapDifference>,
    root_records: std::collections::VecDeque<Py<PyAny>>,
    phase: DiffPhase,
    /// The frontier of refs for the current `_flush_new_queue` pass.
    flush_refs: Vec<ChkRef>,
    /// Results produced but not yet handed out, each `(record_or_none, items)`.
    pending: std::collections::VecDeque<DiffResult>,
}

impl CHKDifferenceIterator {
    /// Materialise a stored result into the Python `(record, items)`
    /// tuple the caller expects: items become `[(key_tuple, value)]`.
    fn build_result<'py>(
        py: Python<'py>,
        record: Option<Py<PyAny>>,
        items: &[ChkItem],
    ) -> PyResult<Bound<'py, PyTuple>> {
        let py_items = PyList::empty(py);
        for (key, value) in items {
            let key_tuple = PyTuple::new(py, key.iter().map(|p| PyBytes::new(py, p)))?;
            let pair = PyTuple::new(
                py,
                [key_tuple.into_any(), PyBytes::new(py, value).into_any()],
            )?;
            py_items.append(pair)?;
        }
        let rec = match record {
            Some(r) => r.into_bound(py).into_any(),
            None => py.None().into_bound(py),
        };
        PyTuple::new(py, [rec, py_items.into_any()])
    }

    /// Advance the state machine until a result is available or the
    /// iteration is exhausted. Returns the next `(record_or_none, items)`.
    fn advance(&mut self, py: Python<'_>) -> PyResult<Option<DiffResult>> {
        loop {
            if let Some(result) = self.pending.pop_front() {
                return Ok(Some(result));
            }
            match self.phase {
                DiffPhase::Roots => {
                    if let Some(record) = self.root_records.pop_front() {
                        return Ok(Some((Some(record), Vec::new())));
                    }
                    // Roots done: drain the old queue, then set up flush.
                    let mut diff = self.diff.bind(py).borrow_mut();
                    while !diff.old_queue.is_empty() {
                        diff.process_next_old(py)?;
                    }
                    self.phase = DiffPhase::DrainOld;
                }
                DiffPhase::DrainOld => {
                    // `_flush_new_queue` setup: emit buffered new items,
                    // then seed the breadth-first frontier.
                    let mut diff = self.diff.bind(py).borrow_mut();
                    let new_queue = std::mem::take(&mut diff.new_queue);
                    let new_items: Vec<ChkItem> = std::mem::take(&mut diff.new_item_queue)
                        .into_iter()
                        .filter(|item| !diff.all_old_items.contains(item))
                        .collect();
                    let mut refs: std::collections::HashSet<ChkRef> =
                        new_queue.into_iter().collect();
                    for r in &diff.all_old_chks {
                        refs.remove(r);
                    }
                    for r in &refs {
                        diff.processed_new_refs.insert(r.clone());
                    }
                    self.flush_refs = refs.into_iter().collect();
                    self.phase = DiffPhase::FlushNew;
                    if !new_items.is_empty() {
                        return Ok(Some((None, new_items)));
                    }
                }
                DiffPhase::FlushNew => {
                    if self.flush_refs.is_empty() {
                        self.phase = DiffPhase::Done;
                        continue;
                    }
                    let refs = std::mem::take(&mut self.flush_refs);
                    let mut diff = self.diff.bind(py).borrow_mut();
                    let nodes = diff.read_nodes_from_store(py, &refs)?;
                    let mut next_refs: std::collections::HashSet<ChkRef> =
                        std::collections::HashSet::new();
                    let all_old_items_empty = diff.all_old_items.is_empty();
                    for node in nodes {
                        let items: Vec<ChkItem> = if all_old_items_empty {
                            node.items
                        } else {
                            node.items
                                .into_iter()
                                .filter(|item| !diff.all_old_items.contains(item))
                                .collect()
                        };
                        for (_, r) in &node.prefix_refs {
                            next_refs.insert(r.clone());
                        }
                        self.pending.push_back((Some(node.record), items));
                    }
                    for r in &diff.all_old_chks {
                        next_refs.remove(r);
                    }
                    next_refs.retain(|r| !diff.processed_new_refs.contains(r));
                    for r in &next_refs {
                        diff.processed_new_refs.insert(r.clone());
                    }
                    self.flush_refs = next_refs.into_iter().collect();
                }
                DiffPhase::Done => return Ok(None),
            }
        }
    }
}

#[pymethods]
impl CHKDifferenceIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyTuple>>> {
        match self.advance(py)? {
            Some((record, items)) => Ok(Some(Self::build_result(py, record, &items)?)),
            None => Ok(None),
        }
    }
}

/// Given root keys, find interesting nodes — those referenced by the
/// interesting roots but not by the uninteresting roots. Returns an
/// iterator of `(record, items)`. Rust port of the Python
/// `chk_map.iter_interesting_nodes`.
#[pyfunction]
#[pyo3(signature = (store, interesting_root_keys, uninteresting_root_keys, pb = None))]
fn iter_interesting_nodes(
    py: Python<'_>,
    store: Bound<'_, PyAny>,
    interesting_root_keys: Bound<'_, PyAny>,
    uninteresting_root_keys: Bound<'_, PyAny>,
    pb: Option<Bound<'_, PyAny>>,
) -> PyResult<CHKDifferenceIterator> {
    let search_key_func = store.getattr("_search_key_func")?;
    let diff = Bound::new(
        py,
        CHKMapDifference::new(
            store.clone(),
            interesting_root_keys,
            uninteresting_root_keys,
            search_key_func,
            pb,
        )?,
    )?;
    CHKMapDifference::process(diff, py)
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
    m.add_wrapped(wrap_pyfunction!(py_deserialise))?;
    m.add_wrapped(wrap_pyfunction!(iter_interesting_nodes))?;
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
    m.add_class::<CHKMapDifference>()?;
    m.add_class::<CHKDifferenceIterator>()?;
    m.add_class::<InternalNodeIterator>()?;
    Ok(m)
}
