use bazaar::chk_map::{
    are_search_keys_identical, deserialise_internal_node, deserialise_leaf_node,
    internal_node_current_size, leaf_node_current_size, leaf_node_key_value_len,
    serialise_internal_node, serialise_leaf_node, Error as ChkError, InternalNodeChild, Key,
    LeafNode, SearchKeyFunc, SearchPrefix,
};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};
use pyo3::wrap_pyfunction;

fn chk_err_to_py(err: ChkError) -> PyErr {
    match err {
        ChkError::DeserializeError(msg) => pyo3::exceptions::PyValueError::new_err(msg),
        ChkError::InconsistentDeltaDelta(_, msg) => pyo3::exceptions::PyValueError::new_err(msg),
    }
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

/// Read a `LeafNode`'s Python attributes into a pure-Rust `LeafNode`.
///
/// `search_key_func` is identified by *name* — the caller hands in the
/// resolved bytes name (the upcoming pyclass will store this directly;
/// for now, callers pass it explicitly because the Python LeafNode
/// stores a callable rather than a name).
fn extract_leaf_node(
    py: Python<'_>,
    node: &Bound<'_, PyAny>,
    search_key_func: SearchKeyFunc,
) -> PyResult<LeafNode> {
    let items_attr = node.getattr("_items")?;
    let items_dict = items_attr.cast::<PyDict>()?;
    let mut items: indexmap::IndexMap<Vec<Vec<u8>>, Vec<u8>> =
        indexmap::IndexMap::with_capacity(items_dict.len());
    for (k, v) in items_dict.iter() {
        let key_tuple = k.cast_into::<PyTuple>()?;
        let mut parts: Vec<Vec<u8>> = Vec::with_capacity(key_tuple.len());
        for part in key_tuple.iter() {
            parts.push(part.cast_into::<PyBytes>()?.as_bytes().to_vec());
        }
        let value = v.cast_into::<PyBytes>()?.as_bytes().to_vec();
        items.insert(parts, value);
    }

    let raw_size: usize = node.getattr("_raw_size")?.extract()?;
    let maximum_size: usize = node.getattr("_maximum_size")?.extract()?;
    let key_width: usize = node.getattr("_key_width")?.extract()?;

    let search_prefix_attr = node.getattr("_search_prefix")?;
    let search_prefix = if search_prefix_attr.is(unknown_sentinel(py)) {
        SearchPrefix::Unknown
    } else if search_prefix_attr.is_none() {
        SearchPrefix::Computed(None)
    } else {
        SearchPrefix::Computed(Some(
            search_prefix_attr
                .cast_into::<PyBytes>()?
                .as_bytes()
                .to_vec(),
        ))
    };

    let csp_attr = node.getattr("_common_serialised_prefix")?;
    let common_serialised_prefix = if csp_attr.is_none() {
        None
    } else {
        Some(csp_attr.cast_into::<PyBytes>()?.as_bytes().to_vec())
    };

    let key_attr = node.getattr("_key")?;
    let key = if key_attr.is_none() {
        None
    } else {
        // _key is a 1-tuple like (b"sha1:...",); flatten it.
        let key_tuple = key_attr.cast_into::<PyTuple>()?;
        let first = key_tuple.get_item(0)?;
        Some(first.cast_into::<PyBytes>()?.as_bytes().to_vec())
    };

    Ok(LeafNode {
        key,
        maximum_size,
        key_width,
        raw_size,
        items,
        search_prefix,
        common_serialised_prefix,
        search_key_func,
    })
}

/// Write a pure-Rust `LeafNode`'s state back onto a Python LeafNode
/// instance. Sets `_items`, `_len`, `_raw_size`, `_search_prefix` and
/// `_common_serialised_prefix`; does not touch `_key` (the caller
/// decides when to invalidate it).
fn store_leaf_node(py: Python<'_>, node: &Bound<'_, PyAny>, leaf: &LeafNode) -> PyResult<()> {
    // Build a fresh dict to replace _items.
    let new_items = PyDict::new(py);
    for (k, v) in leaf.items.iter() {
        let parts: Vec<Bound<PyBytes>> = k.iter().map(|p| PyBytes::new(py, p)).collect();
        let key_tuple = PyTuple::new(py, parts)?;
        let value_bytes = PyBytes::new(py, v);
        new_items.set_item(key_tuple, value_bytes)?;
    }
    node.setattr("_items", new_items)?;
    node.setattr("_len", leaf.items.len())?;
    node.setattr("_raw_size", leaf.raw_size)?;
    match &leaf.search_prefix {
        SearchPrefix::Unknown => {
            node.setattr("_search_prefix", unknown_sentinel(py))?;
        }
        SearchPrefix::Computed(None) => {
            node.setattr("_search_prefix", py.None())?;
        }
        SearchPrefix::Computed(Some(p)) => {
            node.setattr("_search_prefix", PyBytes::new(py, p))?;
        }
    }
    match &leaf.common_serialised_prefix {
        None => node.setattr("_common_serialised_prefix", py.None())?,
        Some(p) => node.setattr("_common_serialised_prefix", PyBytes::new(py, p))?,
    }
    Ok(())
}

/// Resolve a Python `_search_key_func` callable to a `SearchKeyFunc`.
///
/// Identifies built-in variants by their output on a one-element
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

/// `LeafNode._map_no_split` — read state from `node`, insert
/// `(key, value)`, recompute prefixes, and write state back. Returns
/// True if the node has overflowed and the caller should split.
#[pyfunction]
#[pyo3(name = "_leaf_node_map_no_split")]
fn py_leaf_node_map_no_split(
    py: Python<'_>,
    node: Bound<'_, PyAny>,
    key: Bound<'_, PyTuple>,
    value: &[u8],
) -> PyResult<bool> {
    let func_obj = node.getattr("_search_key_func")?;
    let func = resolve_search_key_func_by_callable(py, &func_obj)?;
    let mut leaf = extract_leaf_node(py, &node, func)?;
    let mut key_parts: Vec<Vec<u8>> = Vec::with_capacity(key.len());
    for part in key.iter() {
        key_parts.push(part.cast_into::<PyBytes>()?.as_bytes().to_vec());
    }
    let split = leaf.map_no_split(key_parts, value.to_vec());
    store_leaf_node(py, &node, &leaf)?;
    Ok(split)
}

/// `LeafNode.unmap` minus the store argument (unused on the leaf path).
/// Removes `key` from the node and recomputes prefixes from scratch.
/// Raises KeyError if `key` is not present, matching Python's
/// `del self._items[key]` behaviour.
#[pyfunction]
#[pyo3(name = "_leaf_node_unmap")]
fn py_leaf_node_unmap(
    py: Python<'_>,
    node: Bound<'_, PyAny>,
    key: Bound<'_, PyTuple>,
) -> PyResult<()> {
    let func_obj = node.getattr("_search_key_func")?;
    let func = resolve_search_key_func_by_callable(py, &func_obj)?;
    let mut leaf = extract_leaf_node(py, &node, func)?;
    let mut key_parts: Vec<Vec<u8>> = Vec::with_capacity(key.len());
    for part in key.iter() {
        key_parts.push(part.cast_into::<PyBytes>()?.as_bytes().to_vec());
    }
    if leaf.unmap(&key_parts).is_none() {
        return Err(pyo3::exceptions::PyKeyError::new_err(format!(
            "{:?}",
            key_parts
        )));
    }
    store_leaf_node(py, &node, &leaf)?;
    // Python's unmap also clears _key.
    node.setattr("_key", py.None())?;
    Ok(())
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

pub(crate) fn _chk_map_rs(py: Python) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "chk_map")?;
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
    m.add_wrapped(wrap_pyfunction!(py_leaf_node_map_no_split))?;
    m.add_wrapped(wrap_pyfunction!(py_leaf_node_unmap))?;
    // Expose the `_unknown` sentinel itself so Python can re-export it
    // as `chk_map._unknown` and identity comparisons line up with what
    // the Rust mutators write back.
    m.add("_unknown", py_unknown_sentinel(py))?;
    Ok(m)
}
