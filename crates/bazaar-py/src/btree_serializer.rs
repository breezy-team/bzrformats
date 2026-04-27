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

use pyo3::exceptions::{PyAssertionError, PyKeyError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyTuple};
use std::convert::TryInto;

/// A record for a gc-chk-sha1 leaf node entry.
#[derive(Clone)]
struct GcChkSha1Record {
    block_offset: u64,
    block_length: u32,
    record_start: u32,
    record_end: u32,
    sha1: [u8; 20],
}

/// Lookup table for unhexlifying: maps ASCII byte value to 0..15, or -1 for invalid.
fn build_unhex_table() -> [i8; 256] {
    let mut table = [-1i8; 256];
    for i in 0u8..10 {
        table[(b'0' + i) as usize] = i as i8;
    }
    for i in 0u8..6 {
        table[(b'a' + i) as usize] = (10 + i) as i8;
        table[(b'A' + i) as usize] = (10 + i) as i8;
    }
    table
}

static HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

/// Convert 40 hex bytes into 20 binary bytes. Returns false on invalid input.
fn unhexlify_sha1(hex: &[u8], bin: &mut [u8; 20]) -> bool {
    let table = build_unhex_table();
    if hex.len() != 40 {
        return false;
    }
    for i in 0..20 {
        let top = table[hex[i * 2] as usize];
        let bot = table[hex[i * 2 + 1] as usize];
        if top < 0 || bot < 0 {
            return false;
        }
        bin[i] = ((top << 4) | bot) as u8;
    }
    true
}

/// Convert 20 binary bytes into 40 hex bytes.
fn hexlify_sha1(bin: &[u8; 20]) -> [u8; 40] {
    let mut hex = [0u8; 40];
    for i in 0..20 {
        hex[i * 2] = HEX_CHARS[((bin[i] >> 4) & 0xf) as usize];
        hex[i * 2 + 1] = HEX_CHARS[(bin[i] & 0xf) as usize];
    }
    hex
}

/// Convert a key tuple of the form (b'sha1:xxxx...',) to 20-byte binary sha1.
/// Returns None if the key is not a valid sha1 key.
fn key_to_sha1(key: &Bound<PyAny>) -> Option<[u8; 20]> {
    let tuple: &Bound<PyTuple> = key.downcast().ok()?;
    if tuple.len() != 1 {
        return None;
    }
    let item = tuple.get_item(0).ok()?;
    let bytes_obj: &Bound<PyBytes> = item.downcast().ok()?;
    let data = bytes_obj.as_bytes();
    if data.len() != 45 || !data.starts_with(b"sha1:") {
        return None;
    }
    let mut sha1 = [0u8; 20];
    if unhexlify_sha1(&data[5..], &mut sha1) {
        Some(sha1)
    } else {
        None
    }
}

/// Convert 20-byte binary sha1 into a key tuple (b'sha1:xxxx...',).
fn sha1_to_key<'py>(py: Python<'py>, sha1: &[u8; 20]) -> PyResult<Bound<'py, PyTuple>> {
    let hex = hexlify_sha1(sha1);
    let mut buf = Vec::with_capacity(45);
    buf.extend_from_slice(b"sha1:");
    buf.extend_from_slice(&hex);
    let py_bytes = PyBytes::new(py, &buf);
    PyTuple::new(py, &[py_bytes.as_any()])
}

/// Interpret the first 4 bytes of a sha1 as a big-endian u32.
fn sha1_to_uint(sha1: &[u8; 20]) -> u32 {
    u32::from_be_bytes(sha1[..4].try_into().unwrap())
}

/// Format a record value as bytes like "block_offset block_length record_start record_end".
fn format_record(record: &GcChkSha1Record) -> Vec<u8> {
    format!(
        "{} {} {} {}",
        record.block_offset, record.block_length, record.record_start, record.record_end
    )
    .into_bytes()
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
    /// Extract a key of `key_length` segments starting from `pos` within `line`.
    /// Returns the key tuple and the new position.
    fn extract_key<'py>(
        &self,
        py: Python<'py>,
        line: &[u8],
        mut pos: usize,
    ) -> PyResult<(Bound<'py, PyTuple>, usize)> {
        let mut parts: Vec<Bound<'py, PyBytes>> = Vec::with_capacity(self.key_length);
        for i in 0..self.key_length {
            if let Some(nul_offset) = line[pos..].iter().position(|&b| b == 0) {
                parts.push(PyBytes::new(py, &line[pos..pos + nul_offset]));
                pos = pos + nul_offset + 1;
            } else if i + 1 == self.key_length {
                // Last segment: capture to end
                parts.push(PyBytes::new(py, &line[pos..]));
                pos = line.len();
            } else {
                return Err(PyAssertionError::new_err(format!(
                    "invalid key, wanted segment from {:?}",
                    &line[pos..]
                )));
            }
        }
        let key = PyTuple::new(py, parts.iter().map(|b| b.as_any()))?;
        Ok((key, pos))
    }

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

        let (key, pos) = self.extract_key(py, line, 0)?;

        // Find the last \0 to separate references from value
        let rest = &line[pos..];
        let last_nul = rest.iter().rposition(|&b| b == 0);
        let last_nul = match last_nul {
            Some(idx) => idx,
            None => {
                return Err(PyAssertionError::new_err("Failed to find the value area"));
            }
        };
        let value = PyBytes::new(py, &rest[last_nul + 1..]);
        let refs_area = &rest[..last_nul];

        let node_value: Bound<PyTuple>;
        if self.ref_list_length > 0 {
            let mut ref_lists: Vec<Bound<PyTuple>> = Vec::with_capacity(self.ref_list_length);
            let ref_sections: Vec<&[u8]> = refs_area.split(|&b| b == b'\t').collect();
            for ref_section in ref_sections.iter().take(self.ref_list_length) {
                let mut ref_list: Vec<Bound<PyTuple>> = Vec::new();
                if !ref_section.is_empty() {
                    for ref_bytes in ref_section.split(|&b| b == b'\r') {
                        if ref_bytes.is_empty() {
                            continue;
                        }
                        // Parse a reference key: segments separated by \0
                        let ref_parts: Vec<Bound<PyBytes>> = ref_bytes
                            .split(|&b| b == 0)
                            .map(|s| PyBytes::new(py, s))
                            .collect();
                        let ref_key = PyTuple::new(py, ref_parts.iter().map(|b| b.as_any()))?;
                        ref_list.push(ref_key);
                    }
                }
                ref_lists.push(PyTuple::new(py, ref_list.iter().map(|t| t.as_any()))?);
            }
            let ref_lists_tuple = PyTuple::new(py, ref_lists.iter().map(|t| t.as_any()))?;
            node_value = PyTuple::new(py, &[value.as_any(), ref_lists_tuple.as_any()])?;
        } else {
            if !refs_area.is_empty() {
                return Err(PyAssertionError::new_err(
                    "unexpected reference data present",
                ));
            }
            let empty = PyTuple::empty(py);
            node_value = PyTuple::new(py, &[value.as_any(), empty.as_any()])?;
        }

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
/// This is a performance-critical class that uses binary search with a
/// precomputed offset table for fast lookups of sha1-keyed records.
#[pyclass]
struct GCCHKSHA1LeafNode {
    records: Vec<GcChkSha1Record>,
    last_key: Option<Py<PyAny>>,
    last_record_idx: Option<usize>,
    /// Number of bits to shift to get to the interesting byte.
    /// 24 means the very first byte changes across all keys.
    #[pyo3(get)]
    common_shift: u8,
    /// Maps an interesting byte to the first record that matches.
    offsets: [u8; 257],
}

impl GCCHKSHA1LeafNode {
    fn parse_bytes(&mut self, data: &[u8]) -> PyResult<()> {
        if !data.starts_with(b"type=leaf\n") {
            return Err(PyValueError::new_err(format!(
                "bytes did not start with 'type=leaf\\n': {:?}",
                &data[..std::cmp::min(10, data.len())]
            )));
        }

        let content = &data[10..];
        // Count records (number of newlines)
        let num_records = content.iter().filter(|&&b| b == b'\n').count();
        self.records.reserve(num_records);

        let mut cur = content;
        while !cur.is_empty() {
            // Find next newline
            let nl_pos = match cur.iter().position(|&b| b == b'\n') {
                Some(p) => p,
                None => break,
            };
            let line = &cur[..nl_pos];
            cur = &cur[nl_pos + 1..];

            if line.is_empty() {
                continue;
            }

            let record = self.parse_one_entry(line)?;
            self.records.push(record);
        }

        self.compute_common();
        Ok(())
    }

    fn parse_one_entry(&self, line: &[u8]) -> PyResult<GcChkSha1Record> {
        if !line.starts_with(b"sha1:") {
            return Err(PyValueError::new_err(format!(
                "line did not start with sha1: {:?}",
                &line[..std::cmp::min(10, line.len())]
            )));
        }
        let after_prefix = &line[5..];

        // Find the first \0 after the 40-byte hex sha1
        let nul_pos = after_prefix
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| PyValueError::new_err("Line did not contain expected null byte"))?;
        if nul_pos != 40 {
            return Err(PyValueError::new_err("Line did not contain 40 hex bytes"));
        }

        let mut sha1 = [0u8; 20];
        if !unhexlify_sha1(&after_prefix[..40], &mut sha1) {
            return Err(PyValueError::new_err("We failed to unhexlify"));
        }

        // After the 40 hex chars + \0, expect another \0
        let rest = &after_prefix[41..];
        if rest.is_empty() || rest[0] != 0 {
            return Err(PyValueError::new_err("only 1 null, not 2 as expected"));
        }
        let value_str = &rest[1..];

        // Parse "block_offset block_length record_start record_end"
        let parts: Vec<&[u8]> = value_str.split(|&b| b == b' ').collect();
        if parts.len() != 4 {
            return Err(PyValueError::new_err(
                "Expected 4 space-separated values in record",
            ));
        }

        let block_offset: u64 = std::str::from_utf8(parts[0])
            .map_err(|_| PyValueError::new_err("Failed to parse block offset"))?
            .parse()
            .map_err(|_| PyValueError::new_err("Failed to parse block offset"))?;
        let block_length: u32 = std::str::from_utf8(parts[1])
            .map_err(|_| PyValueError::new_err("Failed to parse block length"))?
            .parse()
            .map_err(|_| PyValueError::new_err("Failed to parse block length"))?;
        let record_start: u32 = std::str::from_utf8(parts[2])
            .map_err(|_| PyValueError::new_err("Failed to parse record start"))?
            .parse()
            .map_err(|_| PyValueError::new_err("Failed to parse record start"))?;
        let record_end: u32 = std::str::from_utf8(parts[3])
            .map_err(|_| PyValueError::new_err("Failed to parse record end"))?
            .parse()
            .map_err(|_| PyValueError::new_err("Failed to parse record end"))?;

        Ok(GcChkSha1Record {
            block_offset,
            block_length,
            record_start,
            record_end,
            sha1,
        })
    }

    fn offset_for_sha1(&self, sha1: &[u8; 20]) -> usize {
        let as_uint = sha1_to_uint(sha1);
        ((as_uint >> self.common_shift) & 0xFF) as usize
    }

    fn compute_common(&mut self) {
        if self.records.len() < 2 {
            self.common_shift = 24;
        } else {
            let mut common_mask: u32 = 0xFFFFFFFF;
            let first = sha1_to_uint(&self.records[0].sha1);
            for record in &self.records[1..] {
                let this = sha1_to_uint(&record.sha1);
                common_mask &= !(first ^ this);
            }
            let mut shift: u8 = 24;
            while common_mask & 0x80000000 != 0 && shift > 0 {
                common_mask <<= 1;
                shift -= 1;
            }
            self.common_shift = shift;
        }

        let max_offset = std::cmp::min(self.records.len(), 255);
        let mut offset: usize = 0;
        for i in 0..max_offset {
            let this_offset = self.offset_for_sha1(&self.records[i].sha1);
            while offset <= this_offset {
                self.offsets[offset] = i as u8;
                offset += 1;
            }
        }
        while offset < 257 {
            self.offsets[offset] = max_offset as u8;
            offset += 1;
        }
    }

    fn lookup_record(&self, sha1: &[u8; 20]) -> Option<usize> {
        let offset = self.offset_for_sha1(sha1);
        let lo_val = self.offsets[offset] as usize;
        let hi_val = self.offsets[offset + 1];
        let mut hi = if hi_val == 255 {
            self.records.len()
        } else {
            hi_val as usize
        };
        let mut lo = lo_val;

        while lo < hi {
            let mid = (lo + hi) / 2;
            match self.records[mid].sha1.cmp(sha1) {
                std::cmp::Ordering::Equal => return Some(mid),
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        None
    }

    fn record_to_value_and_refs<'py>(
        &self,
        py: Python<'py>,
        record: &GcChkSha1Record,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let value = PyBytes::new(py, &format_record(record));
        let empty = PyTuple::empty(py);
        PyTuple::new(py, &[value.as_any(), empty.as_any()])
    }

    fn record_to_item<'py>(
        &self,
        py: Python<'py>,
        record: &GcChkSha1Record,
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
        let bytes = data.as_bytes();
        let mut node = GCCHKSHA1LeafNode {
            records: Vec::new(),
            last_key: None,
            last_record_idx: None,
            common_shift: 0,
            offsets: [0u8; 257],
        };
        node.parse_bytes(bytes)?;
        Ok(node)
    }

    fn __sizeof__(&self) -> usize {
        // Approximate: base struct size + per-record allocation
        std::mem::size_of::<GCCHKSHA1LeafNode>()
            + self.records.len() * std::mem::size_of::<GcChkSha1Record>()
    }

    fn __contains__(&mut self, key: &Bound<PyAny>) -> bool {
        if let Some(sha1) = key_to_sha1(key) {
            if let Some(idx) = self.lookup_record(&sha1) {
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
                    return self.record_to_value_and_refs(py, &self.records[idx].clone());
                }
            }
        }

        if let Some(sha1) = key_to_sha1(key) {
            if let Some(idx) = self.lookup_record(&sha1) {
                return self.record_to_value_and_refs(py, &self.records[idx].clone());
            }
        }

        Err(PyKeyError::new_err(format!("key {:?} is not present", key)))
    }

    fn __len__(&self) -> usize {
        self.records.len()
    }

    #[getter]
    fn min_key<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyTuple>>> {
        if self.records.is_empty() {
            Ok(None)
        } else {
            Ok(Some(sha1_to_key(py, &self.records[0].sha1)?))
        }
    }

    #[getter]
    fn max_key<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyTuple>>> {
        if self.records.is_empty() {
            Ok(None)
        } else {
            let last = &self.records[self.records.len() - 1];
            Ok(Some(sha1_to_key(py, &last.sha1)?))
        }
    }

    fn all_keys<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let result = PyList::empty(py);
        for record in &self.records {
            result.append(sha1_to_key(py, &record.sha1)?)?;
        }
        Ok(result)
    }

    fn all_items<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let result = PyList::empty(py);
        for record in &self.records {
            result.append(self.record_to_item(py, record)?)?;
        }
        Ok(result)
    }

    fn _get_offsets<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let result = PyList::empty(py);
        for &offset in self.offsets.iter() {
            result.append(offset)?;
        }
        Ok(result)
    }

    fn _get_offset_for_sha1(&self, sha1: &Bound<PyBytes>) -> usize {
        let bytes = sha1.as_bytes();
        let mut arr = [0u8; 20];
        arr.copy_from_slice(&bytes[..std::cmp::min(20, bytes.len())]);
        self.offset_for_sha1(&arr)
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
        .downcast()
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

    // Build string_key from node[1] (key tuple joined by \0)
    let key_tuple = node.get_item(1)?;
    let key_tuple: &Bound<PyTuple> = key_tuple
        .downcast()
        .map_err(|_| PyTypeError::new_err("Expected a tuple for key"))?;
    let mut string_key_bytes: Vec<u8> = Vec::new();
    for i in 0..key_tuple.len() {
        if i > 0 {
            string_key_bytes.push(0);
        }
        let item = key_tuple.get_item(i)?;
        let b: &Bound<PyBytes> = item
            .downcast()
            .map_err(|_| PyTypeError::new_err("Expected bytes for key part"))?;
        string_key_bytes.extend_from_slice(b.as_bytes());
    }

    // Get value from node[2]
    let val_obj = node.get_item(2)?;
    let val_bytes: &Bound<PyBytes> = val_obj.downcast().map_err(|_| {
        PyTypeError::new_err(format!(
            "Expected bytes for value not: {:?}",
            val_obj.get_type()
        ))
    })?;
    let value = val_bytes.as_bytes();

    // Compute refs bytes
    let mut refs_bytes: Vec<u8> = Vec::new();
    if reference_lists {
        let ref_lists_obj = node.get_item(3)?;
        let ref_lists_seq: Vec<Bound<'py, PyAny>> =
            ref_lists_obj.try_iter()?.collect::<PyResult<Vec<_>>>()?;
        for (rl_idx, ref_list_obj) in ref_lists_seq.iter().enumerate() {
            if rl_idx > 0 {
                refs_bytes.push(b'\t');
            }
            let ref_list: Vec<Bound<'py, PyAny>> =
                ref_list_obj.try_iter()?.collect::<PyResult<Vec<_>>>()?;
            for (ref_idx, reference_obj) in ref_list.iter().enumerate() {
                if ref_idx > 0 {
                    refs_bytes.push(b'\r');
                }
                let reference: &Bound<'py, PyTuple> = reference_obj.downcast().map_err(|_| {
                    PyTypeError::new_err(format!(
                        "We expect references to be tuples not: {:?}",
                        reference_obj.get_type()
                    ))
                })?;
                for k in 0..reference.len() {
                    if k > 0 {
                        refs_bytes.push(0);
                    }
                    let ref_bit = reference.get_item(k)?;
                    let ref_bit_bytes: &Bound<'py, PyBytes> = ref_bit.downcast().map_err(|_| {
                        PyTypeError::new_err(format!(
                            "We expect reference bits to be bytes not: {:?}",
                            ref_bit.get_type()
                        ))
                    })?;
                    refs_bytes.extend_from_slice(ref_bit_bytes.as_bytes());
                }
            }
        }
    }

    // Build final line: string_key \0 refs \0 value \n
    let mut line =
        Vec::with_capacity(string_key_bytes.len() + 1 + refs_bytes.len() + 1 + value.len() + 1);
    line.extend_from_slice(&string_key_bytes);
    line.push(0);
    line.extend_from_slice(&refs_bytes);
    line.push(0);
    line.extend_from_slice(value);
    line.push(b'\n');

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
        .downcast()
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
fn serialize_btree_index<'py>(
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
