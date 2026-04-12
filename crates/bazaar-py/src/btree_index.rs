use bazaar::btree_index::{
    parse_btree_header, parse_internal_node, BTreeHeader, BTreeIndexError, InternalNode,
};
use pyo3::import_exception;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyTuple};

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

pub fn _btree_index_rs(py: Python) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "btree_index")?;
    m.add_function(wrap_pyfunction!(py_parse_btree_header, &m)?)?;
    m.add_function(wrap_pyfunction!(py_parse_internal_node, &m)?)?;
    Ok(m)
}
