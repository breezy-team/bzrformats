use bazaar::chk_map::{
    deserialise_internal_node, deserialise_leaf_node, internal_node_current_size,
    leaf_node_current_size, leaf_node_key_value_len, serialise_internal_node, serialise_leaf_node,
    Error as ChkError, InternalNodeChild, Key,
};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyTuple};
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
    Ok(m)
}
