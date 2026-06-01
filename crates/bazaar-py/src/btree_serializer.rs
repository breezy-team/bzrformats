// Copyright (C) 2008, 2009, 2010 Canonical Ltd
// Copyright (C) 2024 Jelmer Vernooij
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

//! Rust/PyO3 implementation of the btree serializer extension.

use bazaar::btree_serializer::{
    hexlify_sha1, sha1_bin_to_bytes, sha1_bytes_to_bin, unhexlify_sha1, ChkLeafNode, ChkSha1Record,
};
use pyo3::exceptions::{PyAssertionError, PyKeyError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyTuple};
use std::convert::TryInto;

/// Convert a key tuple of the form (b'sha1:xxxx...',) to 20-byte binary sha1.
/// Returns None if the key is not a valid sha1 key.
fn key_to_sha1(key: &Bound<PyAny>) -> Option<[u8; 20]> {
    let tuple: &Bound<PyTuple> = key.cast().ok()?;
    if tuple.len() != 1 {
        return None;
    }
    let item = tuple.get_item(0).ok()?;
    let bytes_obj: &Bound<PyBytes> = item.cast().ok()?;
    sha1_bytes_to_bin(bytes_obj.as_bytes())
}

/// Convert 20-byte binary sha1 into a key tuple (b'sha1:xxxx...',).
fn sha1_to_key<'py>(py: Python<'py>, sha1: &[u8; 20]) -> PyResult<Bound<'py, PyTuple>> {
    let py_bytes = PyBytes::new(py, &sha1_bin_to_bytes(sha1));
    PyTuple::new(py, &[py_bytes.as_any()])
}

// ---------------------------------------------------------------------------
// BTreeLeafParser
// ---------------------------------------------------------------------------

/// Parse the leaf nodes of a BTree index.
#[pyclass]
struct BTreeLeafParser {
    data: Py<PyBytes>,
    key_length: usize,
    ref_list_length: usize,
    keys: Py<PyList>,
}

impl BTreeLeafParser {
    /// Process a single line of leaf node data. Returns true if there is more to process.
    fn process_line<'py>(
        &self,
        py: Python<'py>,
        line: &[u8],
        header_found: &mut bool,
    ) -> PyResult<()> {
        if line.is_empty() {
            return Ok(());
        }

        if !*header_found {
            if line == b"type=leaf" {
                *header_found = true;
                return Ok(());
            } else {
                return Err(PyAssertionError::new_err(format!(
                    "Node did not start with \"type=leaf\": {:?}",
                    line
                )));
            }
        }

        // Delegate the per-line splitting (key segments / refs / value)
        // to the pure crate; this wrapper only marshals the resulting
        // bytes into the Python tuple shape the parser exposes.
        let (key_segments, value_bytes, ref_lists) =
            bazaar::btree_index::parse_leaf_line(line, self.key_length, self.ref_list_length)
                .map_err(|_| PyAssertionError::new_err("Failed to parse leaf line"))?;

        let key_parts: Vec<Bound<PyBytes>> =
            key_segments.iter().map(|s| PyBytes::new(py, s)).collect();
        let key = PyTuple::new(py, key_parts.iter().map(|b| b.as_any()))?;
        let value = PyBytes::new(py, &value_bytes);

        let node_value: Bound<PyTuple> = if self.ref_list_length > 0 {
            let mut ref_list_tuples: Vec<Bound<PyTuple>> = Vec::with_capacity(ref_lists.len());
            for ref_list in &ref_lists {
                let mut refs: Vec<Bound<PyTuple>> = Vec::with_capacity(ref_list.len());
                for ref_key in ref_list {
                    let parts: Vec<Bound<PyBytes>> =
                        ref_key.iter().map(|s| PyBytes::new(py, s)).collect();
                    refs.push(PyTuple::new(py, parts.iter().map(|b| b.as_any()))?);
                }
                ref_list_tuples.push(PyTuple::new(py, refs.iter().map(|t| t.as_any()))?);
            }
            let ref_lists_tuple = PyTuple::new(py, ref_list_tuples.iter().map(|t| t.as_any()))?;
            PyTuple::new(py, &[value.as_any(), ref_lists_tuple.as_any()])?
        } else {
            let empty = PyTuple::empty(py);
            PyTuple::new(py, &[value.as_any(), empty.as_any()])?
        };

        let entry = PyTuple::new(py, &[key.as_any(), node_value.as_any()])?;
        self.keys.bind(py).append(entry)?;
        Ok(())
    }
}

#[pymethods]
impl BTreeLeafParser {
    #[new]
    fn new(py: Python, data: Py<PyBytes>, key_length: usize, ref_list_length: usize) -> Self {
        BTreeLeafParser {
            data,
            key_length,
            ref_list_length,
            keys: PyList::empty(py).unbind(),
        }
    }

    fn parse(&self, py: Python) -> PyResult<Py<PyList>> {
        let data_ref = self.data.bind(py);
        let bytes = data_ref.as_bytes();
        let mut header_found = false;

        for line in bytes.split(|&b| b == b'\n') {
            self.process_line(py, line, &mut header_found)?;
        }

        Ok(self.keys.clone_ref(py))
    }
}

/// Parse leaf lines using BTreeLeafParser.
#[pyfunction]
fn _parse_leaf_lines(
    py: Python,
    data: Py<PyBytes>,
    key_length: usize,
    ref_list_length: usize,
) -> PyResult<Py<PyList>> {
    let parser = BTreeLeafParser::new(py, data, key_length, ref_list_length);
    parser.parse(py)
}

// ---------------------------------------------------------------------------
// GCCHKSHA1LeafNode
// ---------------------------------------------------------------------------

/// Track all the entries for a given leaf node.
///
/// Thin wrapper over [`bazaar::btree_serializer::ChkLeafNode`], which owns the
/// performance-critical parse + offset-table + binary-search logic. This layer
/// only marshals sha1 key tuples and `(value, refs)` shapes, plus the
/// `__contains__`/`__getitem__` last-record cache.
#[pyclass]
struct GCCHKSHA1LeafNode {
    inner: ChkLeafNode,
    last_key: Option<Py<PyAny>>,
    last_record_idx: Option<usize>,
}

impl GCCHKSHA1LeafNode {
    fn record_to_value_and_refs<'py>(
        &self,
        py: Python<'py>,
        record: &ChkSha1Record,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let value = PyBytes::new(py, &record.format_value());
        let empty = PyTuple::empty(py);
        PyTuple::new(py, &[value.as_any(), empty.as_any()])
    }

    fn record_to_item<'py>(
        &self,
        py: Python<'py>,
        record: &ChkSha1Record,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let key = sha1_to_key(py, &record.sha1)?;
        let value_and_refs = self.record_to_value_and_refs(py, record)?;
        PyTuple::new(py, &[key.as_any(), value_and_refs.as_any()])
    }
}

#[pymethods]
impl GCCHKSHA1LeafNode {
    #[new]
    fn new(data: &Bound<PyBytes>) -> PyResult<Self> {
        let inner = ChkLeafNode::parse(data.as_bytes())
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        Ok(GCCHKSHA1LeafNode {
            inner,
            last_key: None,
            last_record_idx: None,
        })
    }

    #[getter]
    fn common_shift(&self) -> u8 {
        self.inner.common_shift()
    }

    fn __sizeof__(&self) -> usize {
        std::mem::size_of::<GCCHKSHA1LeafNode>()
            + self.inner.len() * std::mem::size_of::<ChkSha1Record>()
    }

    fn __contains__(&mut self, key: &Bound<PyAny>) -> bool {
        if let Some(sha1) = key_to_sha1(key) {
            if let Some(idx) = self.inner.lookup_record(&sha1) {
                self.last_key = Some(key.clone().unbind());
                self.last_record_idx = Some(idx);
                return true;
            }
        }
        false
    }

    fn __getitem__<'py>(
        &mut self,
        py: Python<'py>,
        key: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        // Check cached last_record first
        if let Some(ref last_key) = self.last_key {
            if key.is(last_key.bind(py)) {
                if let Some(idx) = self.last_record_idx {
                    let record = self.inner.records()[idx].clone();
                    return self.record_to_value_and_refs(py, &record);
                }
            }
        }

        if let Some(sha1) = key_to_sha1(key) {
            if let Some(idx) = self.inner.lookup_record(&sha1) {
                let record = self.inner.records()[idx].clone();
                return self.record_to_value_and_refs(py, &record);
            }
        }

        Err(PyKeyError::new_err(format!("key {:?} is not present", key)))
    }

    fn __len__(&self) -> usize {
        self.inner.len()
    }

    #[getter]
    fn min_key<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyTuple>>> {
        match self.inner.min_record() {
            None => Ok(None),
            Some(r) => Ok(Some(sha1_to_key(py, &r.sha1)?)),
        }
    }

    #[getter]
    fn max_key<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyTuple>>> {
        match self.inner.max_record() {
            None => Ok(None),
            Some(r) => Ok(Some(sha1_to_key(py, &r.sha1)?)),
        }
    }

    fn all_keys<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let result = PyList::empty(py);
        for record in self.inner.records() {
            result.append(sha1_to_key(py, &record.sha1)?)?;
        }
        Ok(result)
    }

    fn all_items<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let result = PyList::empty(py);
        for record in self.inner.records() {
            result.append(self.record_to_item(py, record)?)?;
        }
        Ok(result)
    }

    fn _get_offsets<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let result = PyList::empty(py);
        for &offset in self.inner.offsets().iter() {
            result.append(offset)?;
        }
        Ok(result)
    }

    fn _get_offset_for_sha1(&self, sha1: &Bound<PyBytes>) -> usize {
        let bytes = sha1.as_bytes();
        let mut arr = [0u8; 20];
        arr.copy_from_slice(&bytes[..std::cmp::min(20, bytes.len())]);
        self.inner.offset_for_sha1(&arr)
    }
}

// ---------------------------------------------------------------------------
// Module-level functions
// ---------------------------------------------------------------------------

/// Parse into a format optimized for chk records.
#[pyfunction]
fn _parse_into_chk(
    data: &Bound<PyAny>,
    key_length: usize,
    ref_list_length: usize,
) -> PyResult<GCCHKSHA1LeafNode> {
    if key_length != 1 {
        return Err(PyAssertionError::new_err(
            "key_length must be 1 for chk parsing",
        ));
    }
    if ref_list_length != 0 {
        return Err(PyAssertionError::new_err(
            "ref_list_length must be 0 for chk parsing",
        ));
    }
    let bytes_obj: &Bound<PyBytes> = data
        .cast()
        .map_err(|_| PyTypeError::new_err("We only support parsing byte strings."))?;
    GCCHKSHA1LeafNode::new(bytes_obj)
}

/// Convert a node into the serialized form.
///
/// :param node: A tuple representing a node (index, key_tuple, value, references)
/// :param reference_lists: Does this index have reference lists?
/// :return: (string_key, flattened)
#[pyfunction]
fn _flatten_node<'py>(
    py: Python<'py>,
    node: &Bound<'py, PyTuple>,
    reference_lists: isize,
) -> PyResult<(Bound<'py, PyBytes>, Bound<'py, PyBytes>)> {
    let node_len = node.len();
    let reference_lists = reference_lists != 0;

    if reference_lists {
        if node_len != 4 {
            return Err(PyValueError::new_err(format!(
                "With ref_lists, we expected 4 entries not: {}",
                node_len
            )));
        }
    } else if node_len < 3 {
        return Err(PyValueError::new_err(format!(
            "Without ref_lists, we need at least 3 entries not: {}",
            node_len
        )));
    }

    let key_tuple = node.get_item(1)?;
    let key_tuple: &Bound<PyTuple> = key_tuple
        .cast()
        .map_err(|_| PyTypeError::new_err("Expected a tuple for key"))?;
    let mut key: bazaar::btree_builder::Key = Vec::with_capacity(key_tuple.len());
    for i in 0..key_tuple.len() {
        let item = key_tuple.get_item(i)?;
        let b: &Bound<PyBytes> = item
            .cast()
            .map_err(|_| PyTypeError::new_err("Expected bytes for key part"))?;
        key.push(b.as_bytes().to_vec());
    }

    let val_obj = node.get_item(2)?;
    let val_bytes: &Bound<PyBytes> = val_obj.cast().map_err(|_| {
        PyTypeError::new_err(format!(
            "Expected bytes for value not: {:?}",
            val_obj.get_type()
        ))
    })?;
    let value = val_bytes.as_bytes().to_vec();

    let mut references: Vec<Vec<bazaar::btree_builder::Key>> = Vec::new();
    if reference_lists {
        let ref_lists_obj = node.get_item(3)?;
        for ref_list_obj in ref_lists_obj.try_iter()? {
            let ref_list_obj = ref_list_obj?;
            let mut rl: Vec<bazaar::btree_builder::Key> = Vec::new();
            for reference_obj in ref_list_obj.try_iter()? {
                let reference_obj = reference_obj?;
                let reference: &Bound<'py, PyTuple> = reference_obj.cast().map_err(|_| {
                    PyTypeError::new_err(format!(
                        "We expect references to be tuples not: {:?}",
                        reference_obj.get_type()
                    ))
                })?;
                let mut r: bazaar::btree_builder::Key = Vec::with_capacity(reference.len());
                for k in 0..reference.len() {
                    let ref_bit = reference.get_item(k)?;
                    let ref_bit_bytes: &Bound<'py, PyBytes> = ref_bit.cast().map_err(|_| {
                        PyTypeError::new_err(format!(
                            "We expect reference bits to be bytes not: {:?}",
                            ref_bit.get_type()
                        ))
                    })?;
                    r.push(ref_bit_bytes.as_bytes().to_vec());
                }
                rl.push(r);
            }
            references.push(rl);
        }
    }

    let (string_key_bytes, line) =
        bazaar::btree_builder::flatten_node(&key, &value, &references, reference_lists);
    let string_key = PyBytes::new(py, &string_key_bytes);
    let line_bytes = PyBytes::new(py, &line);
    Ok((string_key, line_bytes))
}

/// For test infrastructure: hexlify a 20-byte binary digest.
#[pyfunction]
fn _py_hexlify<'py>(py: Python<'py>, as_bin: &Bound<PyBytes>) -> PyResult<Bound<'py, PyBytes>> {
    let data = as_bin.as_bytes();
    if data.len() != 20 {
        return Err(PyValueError::new_err("not a 20-byte binary digest"));
    }
    let arr: &[u8; 20] = data.try_into().unwrap();
    let hex = hexlify_sha1(arr);
    Ok(PyBytes::new(py, &hex))
}

/// For test infrastructure: unhexlify a 40-byte hex digest.
#[pyfunction]
fn _py_unhexlify<'py>(
    py: Python<'py>,
    as_hex: &Bound<PyAny>,
) -> PyResult<Option<Bound<'py, PyBytes>>> {
    let bytes_obj: &Bound<PyBytes> = as_hex
        .cast()
        .map_err(|_| PyValueError::new_err("not a 40-byte hex digest"))?;
    let data = bytes_obj.as_bytes();
    if data.len() != 40 {
        return Err(PyValueError::new_err("not a 40-byte hex digest"));
    }
    let mut bin = [0u8; 20];
    if unhexlify_sha1(data, &mut bin) {
        Ok(Some(PyBytes::new(py, &bin)))
    } else {
        Ok(None)
    }
}

/// Map a key to a simple sha1 string. Testing thunk.
#[pyfunction]
fn _py_key_to_sha1<'py>(
    py: Python<'py>,
    key: &Bound<'py, PyAny>,
) -> PyResult<Option<Bound<'py, PyBytes>>> {
    match key_to_sha1(key) {
        Some(sha1) => Ok(Some(PyBytes::new(py, &sha1))),
        None => Ok(None),
    }
}

/// Test thunk to check the sha1-to-key mapping.
#[pyfunction]
fn _py_sha1_to_key<'py>(
    py: Python<'py>,
    sha1_bin: &Bound<PyBytes>,
) -> PyResult<Bound<'py, PyTuple>> {
    let data = sha1_bin.as_bytes();
    if data.len() != 20 {
        return Err(PyValueError::new_err(
            "sha1_bin must be a str of exactly 20 bytes",
        ));
    }
    let arr: &[u8; 20] = data.try_into().unwrap();
    sha1_to_key(py, arr)
}

/// Serialize an iterable of `(index, key, value, refs?)` nodes into a B+Tree
/// graph index. Mirrors `BTreeBuilder._write_nodes` on the Python side.
#[pyfunction]
#[pyo3(signature = (nodes, reference_lists, key_elements, optimize_for_size=false, page_size=None, reserved_header_bytes=None))]
pub(crate) fn serialize_btree_index<'py>(
    py: Python<'py>,
    nodes: &Bound<'py, PyAny>,
    reference_lists: usize,
    key_elements: usize,
    optimize_for_size: bool,
    page_size: Option<usize>,
    reserved_header_bytes: Option<usize>,
) -> PyResult<Bound<'py, PyBytes>> {
    use bazaar::btree_builder::{Layout, Node};
    let layout = Layout {
        page_size: page_size.unwrap_or(bazaar::btree_builder::DEFAULT_PAGE_SIZE),
        reserved_header_bytes: reserved_header_bytes
            .unwrap_or(bazaar::btree_builder::DEFAULT_RESERVED_HEADER_BYTES),
    };

    // Collect the iterable into a sorted list of (key, Node).
    let mut collected: Vec<(Vec<Vec<u8>>, Node)> = Vec::new();
    for item in nodes.try_iter()? {
        let item = item?;
        let tuple = item.cast::<PyTuple>()?;
        // node layout: (index, key_tuple, value[, reference_lists]).
        let key_any = tuple.get_item(1)?;
        let key_tuple = key_any.cast::<PyTuple>()?;
        let key: Vec<Vec<u8>> = key_tuple
            .iter()
            .map(|seg| {
                seg.cast::<PyBytes>()
                    .map(|b| b.as_bytes().to_vec())
                    .map_err(|_| PyTypeError::new_err("key segments must be bytes"))
            })
            .collect::<PyResult<_>>()?;
        let value_any = tuple.get_item(2)?;
        let value_bytes = value_any.cast::<PyBytes>()?.as_bytes().to_vec();
        let references: Vec<Vec<Vec<Vec<u8>>>> = if reference_lists > 0 {
            let refs_any = tuple.get_item(3)?;
            let refs_tuple = refs_any.cast::<PyTuple>()?;
            let mut rls: Vec<Vec<Vec<Vec<u8>>>> = Vec::with_capacity(refs_tuple.len());
            for rl in refs_tuple.iter() {
                let rl_seq = rl.cast::<PyTuple>()?;
                let mut rl_out: Vec<Vec<Vec<u8>>> = Vec::with_capacity(rl_seq.len());
                for r in rl_seq.iter() {
                    let r_tup = r.cast::<PyTuple>()?;
                    let r_out: Vec<Vec<u8>> = r_tup
                        .iter()
                        .map(|seg| {
                            seg.cast::<PyBytes>()
                                .map(|b| b.as_bytes().to_vec())
                                .map_err(|_| PyTypeError::new_err("ref segments must be bytes"))
                        })
                        .collect::<PyResult<_>>()?;
                    rl_out.push(r_out);
                }
                rls.push(rl_out);
            }
            rls
        } else {
            Vec::new()
        };
        let node = Node {
            references,
            value: value_bytes,
        };
        collected.push((key, node));
    }
    // The Python caller already feeds us in sorted order via iter_all_entries
    // but sort defensively just in case.
    collected.sort_by(|a, b| a.0.cmp(&b.0));

    pyo3::import_exception!(bzrformats.index, BadIndexKey);
    let bytes = bazaar::btree_builder::write_nodes(
        &collected,
        reference_lists,
        key_elements,
        optimize_for_size,
        layout,
    )
    .map_err(|e| match e {
        bazaar::btree_builder::Error::KeyTooBig(key) => {
            let key_tuple = PyTuple::new(py, key.iter().map(|seg| PyBytes::new(py, seg))).unwrap();
            BadIndexKey::new_err((key_tuple.unbind(),))
        }
        other => PyValueError::new_err(other.to_string()),
    })?;
    Ok(PyBytes::new(py, &bytes))
}

/// Register the btree serializer module.
pub(crate) fn _btree_serializer_rs(py: Python) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "btree_serializer")?;
    m.add_class::<BTreeLeafParser>()?;
    m.add_class::<GCCHKSHA1LeafNode>()?;
    m.add_function(wrap_pyfunction!(_parse_leaf_lines, &m)?)?;
    m.add_function(wrap_pyfunction!(_parse_into_chk, &m)?)?;
    m.add_function(wrap_pyfunction!(_flatten_node, &m)?)?;
    m.add_function(wrap_pyfunction!(_py_hexlify, &m)?)?;
    m.add_function(wrap_pyfunction!(_py_unhexlify, &m)?)?;
    m.add_function(wrap_pyfunction!(_py_key_to_sha1, &m)?)?;
    m.add_function(wrap_pyfunction!(_py_sha1_to_key, &m)?)?;
    m.add_function(wrap_pyfunction!(serialize_btree_index, &m)?)?;
    Ok(m)
}
