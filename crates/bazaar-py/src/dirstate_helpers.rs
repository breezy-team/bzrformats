//! Rust implementations of dirstate helper functions.
//!
//! This module provides `_read_dirblocks`, `update_entry`, and `ProcessEntryC`
//! as Rust/PyO3 replacements for the former Cython `_dirstate_helpers_pyx` module.
//!
//! `_read_dirblocks` is fully implemented in Rust for performance.
//! `update_entry` and `ProcessEntryC` delegate to their Python implementations
//! since they interact deeply with the Python DirState object.

use pyo3::exceptions::{PyAssertionError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyTuple};

/// Parse a single null-terminated field from `data` starting at `pos`.
/// Returns (field_bytes, next_pos). Raises DirstateCorrupt if no null found.
fn get_next_field<'a>(
    data: &'a [u8],
    pos: usize,
    state: &Bound<PyAny>,
) -> PyResult<(&'a [u8], usize)> {
    if pos >= data.len() {
        let dirstate_corrupt = state
            .py()
            .import("bzrformats.dirstate")?
            .getattr("DirstateCorrupt")?;
        return Err(PyErr::from_value(dirstate_corrupt.call1((
            state,
            "get_next() called when there are no chars left",
        ))?));
    }
    let remaining = &data[pos..];
    match remaining.iter().position(|&b| b == 0) {
        Some(offset) => Ok((&data[pos..pos + offset], pos + offset + 1)),
        None => {
            let dirstate_corrupt = state
                .py()
                .import("bzrformats.dirstate")?
                .getattr("DirstateCorrupt")?;
            let end = std::cmp::min(remaining.len(), 20);
            Err(PyErr::from_value(dirstate_corrupt.call1((
                state,
                format!(
                    "failed to find trailing NULL (\\0). Trailing garbage: {:?}",
                    &remaining[..end]
                ),
            ))?))
        }
    }
}

/// Read in the dirblocks for the given DirState object.
///
/// This is tightly bound to the DirState internal representation. It should be
/// thought of as a member function, which is only separated out so that we can
/// re-write it in Rust for performance.
///
/// :param state: A DirState object.
/// :return: None
/// :postcondition: The dirblocks will be loaded into the appropriate fields
///     in the DirState object.
#[pyfunction]
pub fn _read_dirblocks(py: Python, state: &Bound<PyAny>) -> PyResult<()> {
    let dirstate_mod = py.import("bzrformats.dirstate")?;
    let dirstate_cls = dirstate_mod.getattr("DirState")?;

    // Seek to end of header and read the rest
    let state_file = state.getattr("_state_file")?;
    let end_of_header = state.getattr("_end_of_header")?;
    state_file.call_method1("seek", (end_of_header,))?;
    let text_obj = state_file.call_method0("read")?;
    let text: &[u8] = text_obj.extract()?;

    if text.is_empty() {
        // No data to parse
        state.setattr(
            "_dirblock_state",
            dirstate_cls.getattr("IN_MEMORY_UNMODIFIED")?,
        )?;
        return Ok(());
    }

    let num_present_parents: usize = state.call_method0("_num_present_parents")?.extract()?;
    let num_trees = num_present_parents + 1;
    let num_entries: usize = state.getattr("_num_entries")?.extract()?;

    // Skip the first null byte (trailing from header)
    let mut pos: usize = 0;

    // The first field should be an empty string left over from the Header
    let (first_field, new_pos) = get_next_field(text, pos, state)?;
    if !first_field.is_empty() {
        return Err(PyAssertionError::new_err(format!(
            "First field should be empty, not: {:?}",
            first_field
        )));
    }
    pos = new_pos;

    // Build dirblocks
    let empty_bytes = PyBytes::new(py, b"");
    let root_block = PyList::empty(py);
    let dirblocks = PyList::new(
        py,
        [
            PyTuple::new(py, [empty_bytes.as_any(), root_block.as_any()])?,
            PyTuple::new(py, [empty_bytes.as_any(), PyList::empty(py).as_any()])?,
        ],
    )?;

    let mut current_dirname: Vec<u8> = Vec::new();
    let mut current_block = root_block;
    let mut entry_count: usize = 0;

    while pos < text.len() {
        // Read key: dirname, name, file_id
        let (dirname_bytes, new_pos) = get_next_field(text, pos, state)?;
        pos = new_pos;

        // Check if we have a new directory block
        let new_block = dirname_bytes != current_dirname.as_slice();
        if new_block {
            current_dirname = dirname_bytes.to_vec();
            current_block = PyList::empty(py);
            let dirname_py = PyBytes::new(py, dirname_bytes);
            dirblocks.append(PyTuple::new(
                py,
                [dirname_py.as_any(), current_block.as_any()],
            )?)?;
        }

        let dirname_py = PyBytes::new(py, &current_dirname);
        let (name_bytes, new_pos) = get_next_field(text, pos, state)?;
        pos = new_pos;
        let name_py = PyBytes::new(py, name_bytes);

        let (file_id_bytes, new_pos) = get_next_field(text, pos, state)?;
        pos = new_pos;
        let file_id_py = PyBytes::new(py, file_id_bytes);

        let key = PyTuple::new(
            py,
            [dirname_py.as_any(), name_py.as_any(), file_id_py.as_any()],
        )?;

        // Parse per-tree data
        let trees = PyList::empty(py);
        for _i in 0..num_trees {
            let (minikind_bytes, new_pos) = get_next_field(text, pos, state)?;
            pos = new_pos;
            let minikind = PyBytes::new(py, minikind_bytes);

            let (fingerprint_bytes, new_pos) = get_next_field(text, pos, state)?;
            pos = new_pos;
            let fingerprint = PyBytes::new(py, fingerprint_bytes);

            let (size_bytes, new_pos) = get_next_field(text, pos, state)?;
            pos = new_pos;
            let size_str = std::str::from_utf8(size_bytes).map_err(|e| {
                PyValueError::new_err(format!("Invalid UTF-8 in size field: {}", e))
            })?;
            let size: u64 = size_str.parse().map_err(|e| {
                PyValueError::new_err(format!("Invalid size field '{}': {}", size_str, e))
            })?;

            let (exec_bytes, new_pos) = get_next_field(text, pos, state)?;
            pos = new_pos;
            let is_executable = !exec_bytes.is_empty() && exec_bytes[0] == b'y';

            let (info_bytes, new_pos) = get_next_field(text, pos, state)?;
            pos = new_pos;
            let info = PyBytes::new(py, info_bytes);

            let tree_data = PyTuple::new(
                py,
                [
                    minikind.into_any(),
                    fingerprint.into_any(),
                    size.into_pyobject(py)?.into_any(),
                    is_executable.into_pyobject(py)?.to_owned().into_any(),
                    info.into_any(),
                ],
            )?;
            trees.append(tree_data)?;
        }

        let entry = PyTuple::new(py, [key.as_any(), trees.as_any()])?;

        // Read and check trailing newline
        let (trailing, new_pos) = get_next_field(text, pos, state)?;
        pos = new_pos;
        if trailing.len() != 1 || trailing[0] != b'\n' {
            let dirstate_corrupt = py
                .import("bzrformats.dirstate")?
                .getattr("DirstateCorrupt")?;
            return Err(PyErr::from_value(dirstate_corrupt.call1((
                state,
                format!(
                    "Bad parse, we expected to end on \\n, not: {} {:?}: {:?}",
                    trailing.len(),
                    trailing,
                    entry
                ),
            ))?));
        }

        current_block.append(entry)?;
        entry_count += 1;
    }

    if entry_count != num_entries {
        let dirstate_corrupt = py
            .import("bzrformats.dirstate")?
            .getattr("DirstateCorrupt")?;
        return Err(PyErr::from_value(dirstate_corrupt.call1((
            state,
            format!(
                "We read the wrong number of entries. We expected to read {}, but read {}",
                num_entries, entry_count
            ),
        ))?));
    }

    state.setattr("_dirblocks", dirblocks)?;
    state.call_method0("_split_root_dirblock_into_contents")?;
    state.setattr(
        "_dirblock_state",
        dirstate_cls.getattr("IN_MEMORY_UNMODIFIED")?,
    )?;

    Ok(())
}

/// Update the entry based on what is actually on disk.
///
/// This delegates to the Python `py_update_entry` implementation in dirstate.py
/// since it interacts deeply with the DirState object's internal state.
#[pyfunction]
pub fn update_entry(
    py: Python,
    state: &Bound<PyAny>,
    entry: &Bound<PyAny>,
    abspath: &Bound<PyAny>,
    stat_value: &Bound<PyAny>,
) -> PyResult<Py<PyAny>> {
    let dirstate_mod = py.import("bzrformats.dirstate")?;
    let py_update_entry = dirstate_mod.getattr("py_update_entry")?;
    let result = py_update_entry.call1((state, entry, abspath, stat_value))?;
    Ok(result.into())
}

/// Process entries for tree comparison.
///
/// This is a thin wrapper that delegates to the Python ProcessEntryPython
/// class, since the implementation interacts deeply with the Python DirState
/// object and the full tree comparison machinery.
#[pyclass]
pub struct ProcessEntryC {
    inner: Py<PyAny>,
}

#[pymethods]
impl ProcessEntryC {
    #[new]
    #[pyo3(signature = (include_unchanged, use_filesystem_for_exec, search_specific_files, state, source_index, target_index, want_unversioned, tree))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        py: Python,
        include_unchanged: &Bound<PyAny>,
        use_filesystem_for_exec: &Bound<PyAny>,
        search_specific_files: &Bound<PyAny>,
        state: &Bound<PyAny>,
        source_index: &Bound<PyAny>,
        target_index: &Bound<PyAny>,
        want_unversioned: &Bound<PyAny>,
        tree: &Bound<PyAny>,
    ) -> PyResult<Self> {
        let dirstate_mod = py.import("bzrformats.dirstate")?;
        let process_entry_cls = dirstate_mod.getattr("ProcessEntryPython")?;
        let inner = process_entry_cls.call1((
            include_unchanged,
            use_filesystem_for_exec,
            search_specific_files,
            state,
            source_index,
            target_index,
            want_unversioned,
            tree,
        ))?;
        Ok(ProcessEntryC {
            inner: inner.into(),
        })
    }

    fn __iter__(&self, py: Python) -> PyResult<Py<PyAny>> {
        // Delegate to the inner Python object's iter_changes() generator so
        // that `for x in process_entry_c` works.
        let inner = self.inner.bind(py);
        Ok(inner.call_method0("iter_changes")?.into())
    }

    fn iter_changes(&self, py: Python) -> PyResult<Py<PyAny>> {
        let inner = self.inner.bind(py);
        Ok(inner.call_method0("iter_changes")?.into())
    }

    #[getter]
    fn searched_specific_files(&self, py: Python) -> PyResult<Py<PyAny>> {
        let inner = self.inner.bind(py);
        Ok(inner.getattr("searched_specific_files")?.into())
    }

    #[getter]
    fn searched_exact_paths(&self, py: Python) -> PyResult<Py<PyAny>> {
        let inner = self.inner.bind(py);
        Ok(inner.getattr("searched_exact_paths")?.into())
    }

    #[getter]
    fn search_specific_files(&self, py: Python) -> PyResult<Py<PyAny>> {
        let inner = self.inner.bind(py);
        Ok(inner.getattr("search_specific_files")?.into())
    }
}

/// Register the dirstate helper functions into the given module.
pub fn register(m: &Bound<pyo3::types::PyModule>) -> PyResult<()> {
    m.add_function(pyo3::wrap_pyfunction!(_read_dirblocks, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(update_entry, m)?)?;
    m.add_class::<ProcessEntryC>()?;
    Ok(())
}
