use bazaar::chk_map::{deserialise_internal_node, deserialise_leaf_node, Error as ChkError, Key};
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

pub(crate) fn _chk_map_rs(py: Python) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "chk_map")?;
    m.add_wrapped(wrap_pyfunction!(_search_key_16))?;
    m.add_wrapped(wrap_pyfunction!(_search_key_255))?;
    m.add_wrapped(wrap_pyfunction!(_bytes_to_text_key))?;
    m.add_wrapped(wrap_pyfunction!(common_prefix_pair))?;
    m.add_wrapped(wrap_pyfunction!(common_prefix_many))?;
    m.add_wrapped(wrap_pyfunction!(py_deserialise_leaf_node))?;
    m.add_wrapped(wrap_pyfunction!(py_deserialise_internal_node))?;
    Ok(m)
}
