//! PyO3 glue for the dirstate helper functions.
//!
//! `_read_dirblocks` delegates the NUL-delimited parse of the on-disk body
//! to `bazaar::dirstate::parse_dirblocks` in the pure crate; this module
//! only marshals the resulting `Vec<Dirblock>` into the list-of-tuples
//! shape Python stores in `DirState._dirblocks`, and handles the
//! surrounding file I/O and state-object mutation.

use bazaar::dirstate::{
    entry_to_line as pure_entry_to_line, parse_dirblocks, Dirblock, DirblocksError, Entry,
    EntryKey, TreeData,
};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyTuple};

/// Convert a `DirblocksError` from the pure crate into the Python-level
/// `DirstateCorrupt` exception that Python callers expect.
fn dirblocks_err_to_py(state: &Bound<PyAny>, err: DirblocksError) -> PyErr {
    match state
        .py()
        .import("bzrformats.dirstate")
        .and_then(|m| m.getattr("DirstateCorrupt"))
    {
        Ok(cls) => match cls.call1((state, err.to_string())) {
            Ok(instance) => PyErr::from_value(instance),
            Err(e) => e,
        },
        Err(e) => e,
    }
}

/// Marshal a `Vec<Dirblock>` from the pure crate into the
/// `[(dirname_bytes, [entry_tuple, ...])]` layout Python uses for
/// `DirState._dirblocks`. Each entry tuple is
/// `((dirname, basename, file_id), [(minikind, fingerprint, size, exec, packed_stat), ...])`
/// with `minikind` being a one-byte `bytes` object — matching what the
/// previous inline parser produced.
pub(crate) fn dirblocks_to_py<'py>(
    py: Python<'py>,
    dirblocks: &[Dirblock],
) -> PyResult<Bound<'py, PyList>> {
    let out = PyList::empty(py);
    for block in dirblocks {
        let dirname_py = PyBytes::new(py, &block.dirname);
        let entries_py = PyList::empty(py);
        for entry in &block.entries {
            let key = PyTuple::new(
                py,
                [
                    PyBytes::new(py, &entry.key.dirname).into_any(),
                    PyBytes::new(py, &entry.key.basename).into_any(),
                    PyBytes::new(py, &entry.key.file_id).into_any(),
                ],
            )?;
            let trees = PyList::empty(py);
            for tree in &entry.trees {
                let tree_tuple = PyTuple::new(
                    py,
                    [
                        PyBytes::new(py, &[tree.minikind.to_minikind()]).into_any(),
                        PyBytes::new(py, &tree.fingerprint).into_any(),
                        tree.size.into_pyobject(py)?.into_any(),
                        tree.executable.into_pyobject(py)?.to_owned().into_any(),
                        PyBytes::new(py, &tree.packed_stat).into_any(),
                    ],
                )?;
                trees.append(tree_tuple)?;
            }
            entries_py.append(PyTuple::new(py, [key.as_any(), trees.as_any()])?)?;
        }
        out.append(PyTuple::new(
            py,
            [dirname_py.as_any(), entries_py.as_any()],
        )?)?;
    }
    Ok(out)
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
    let num_entries: usize = state.getattr("_rs")?.getattr("num_entries")?.extract()?;

    let dirblocks =
        parse_dirblocks(text, num_trees, num_entries).map_err(|e| dirblocks_err_to_py(state, e))?;

    let py_dirblocks = dirblocks_to_py(py, &dirblocks)?;
    state.setattr("_dirblocks", py_dirblocks)?;
    // `_split_root_dirblock_into_contents` still lives in Python; it is a
    // pure reshuffle of `_dirblocks[0]` / `_dirblocks[1]` and is cheap to
    // call across the FFI boundary. A later commit can delegate the split
    // to `bazaar::dirstate::split_root_dirblock_into_contents` once the
    // pure-Rust version is wired into the marshalling path.
    state.call_method0("_split_root_dirblock_into_contents")?;
    state.setattr(
        "_dirblock_state",
        dirstate_cls.getattr("IN_MEMORY_UNMODIFIED")?,
    )?;

    Ok(())
}

/// Extract a single Python entry tuple into a pure-Rust [`Entry`]. The
/// Python shape is
/// `((dirname, basename, file_id), [(minikind, fingerprint, size, executable, packed_stat), ...])`
/// with `minikind` as a one-byte `bytes` object, `size` as an int, and
/// `executable` as a bool — this is the same layout `_read_dirblocks`
/// produces.
pub(crate) fn entry_from_py(py_entry: &Bound<PyAny>) -> PyResult<Entry> {
    let tuple = py_entry.cast::<PyTuple>()?;
    let key_tuple = tuple.get_item(0)?.cast_into::<PyTuple>()?;
    let key = EntryKey {
        dirname: key_tuple.get_item(0)?.extract::<Vec<u8>>()?,
        basename: key_tuple.get_item(1)?.extract::<Vec<u8>>()?,
        file_id: key_tuple.get_item(2)?.extract::<Vec<u8>>()?,
    };
    let trees_obj = tuple.get_item(1)?;
    let mut trees: Vec<TreeData> = Vec::new();
    for tree in trees_obj.try_iter()? {
        let tree = tree?;
        let tree_tuple = tree.cast::<PyTuple>()?;
        let minikind_bytes: Vec<u8> = tree_tuple.get_item(0)?.extract()?;
        let minikind_byte = minikind_bytes
            .first()
            .copied()
            .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("empty minikind"))?;
        let minikind = bazaar::dirstate::Kind::from_minikind(minikind_byte).map_err(|b| {
            pyo3::exceptions::PyValueError::new_err(format!("invalid minikind byte {:?}", b))
        })?;
        let fingerprint: Vec<u8> = tree_tuple.get_item(1)?.extract()?;
        // Legacy Python dirblocks tolerated 3-tuple relocation rows
        // `(b"r", target_path, target_file_id)` alongside the normal
        // 5-tuple shape, since nothing in the Python code path accessed
        // slots 2/3/4 on `b'r'` / `b'a'` entries.  Production writers
        // always emit 5-tuples; only accept the shorter shape for the
        // two minikinds where it is actually meaningful, so a malformed
        // 3-tuple for a normal entry still rejects.
        let (size, executable, packed_stat) = if tree_tuple.len() >= 5 {
            (
                tree_tuple.get_item(2)?.extract::<u64>()?,
                tree_tuple.get_item(3)?.extract::<bool>()?,
                tree_tuple.get_item(4)?.extract::<Vec<u8>>()?,
            )
        } else if matches!(
            minikind,
            bazaar::dirstate::Kind::Relocated | bazaar::dirstate::Kind::Absent
        ) {
            (0u64, false, Vec::new())
        } else {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "entry tuple too short for minikind {:?}: got {} items, expected 5",
                minikind,
                tree_tuple.len(),
            )));
        };
        trees.push(TreeData {
            minikind,
            fingerprint,
            size,
            executable,
            packed_stat,
        });
    }
    Ok(Entry { key, trees })
}

/// Convert a Python dirblocks list into the pure-Rust
/// `Vec<Dirblock>` layout. Input shape matches Python's `_dirblocks`:
/// `[(dirname_bytes, [entry_tuple, ...]), ...]` where each
/// `entry_tuple` is `((dirname, basename, file_id), [tree_tuple, ...])`
/// and each `tree_tuple` is
/// `(minikind, fingerprint, size, executable, packed_stat_or_revid)`.
///
/// Used as the sync boundary between Python's `_dirblocks` attribute
/// and the pure-Rust `DirState.dirblocks` field while dirblock
/// ownership is being migrated method-by-method.
pub(crate) fn dirblocks_from_py(dirblocks: &Bound<PyAny>) -> PyResult<Vec<Dirblock>> {
    let mut out = Vec::new();
    for block in dirblocks.try_iter()? {
        let block = block?;
        let block_tuple = block.cast::<PyTuple>()?;
        let dirname: Vec<u8> = block_tuple.get_item(0)?.extract()?;
        let entries_obj = block_tuple.get_item(1)?;
        let mut entries: Vec<Entry> = Vec::new();
        for entry in entries_obj.try_iter()? {
            let entry = entry?;
            entries.push(entry_from_py(&entry)?);
        }
        out.push(Dirblock { dirname, entries });
    }
    Ok(out)
}

/// Serialise a single dirstate entry to the NUL-delimited line format
/// the writer uses. Replaces Python's `DirState._entry_to_line`; the
/// pure-Rust implementation lives in `bazaar::dirstate::entry_to_line`.
#[pyfunction]
#[pyo3(name = "entry_to_line")]
fn py_entry_to_line<'py>(
    py: Python<'py>,
    entry: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyBytes>> {
    let rust_entry = entry_from_py(entry)?;
    let bytes = pure_entry_to_line(&rust_entry);
    Ok(PyBytes::new(py, &bytes))
}

/// Register the dirstate helper functions into the given module.
pub fn register(m: &Bound<pyo3::types::PyModule>) -> PyResult<()> {
    m.add_function(pyo3::wrap_pyfunction!(_read_dirblocks, m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(py_entry_to_line, m)?)?;
    Ok(())
}
