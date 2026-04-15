#![allow(non_snake_case)]

use bazaar::FileId;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString, PyTuple};
use pyo3::wrap_pyfunction;
use std::ffi::OsString;
#[cfg(unix)]
use std::os::unix::ffi::OsStringExt;
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

// TODO(jelmer): Shared pyo3 utils?
fn extract_path(object: &Bound<PyAny>) -> PyResult<PathBuf> {
    if let Ok(path) = object.extract::<Vec<u8>>() {
        #[cfg(unix)]
        {
            Ok(PathBuf::from(OsString::from_vec(path)))
        }
        #[cfg(not(unix))]
        {
            Ok(PathBuf::from(
                String::from_utf8(path).map_err(|e| PyTypeError::new_err(e.to_string()))?,
            ))
        }
    } else if let Ok(path) = object.extract::<PathBuf>() {
        Ok(path)
    } else {
        Err(PyTypeError::new_err("path must be a string or bytes"))
    }
}

/// Compare two paths directory by directory.
///
///  This is equivalent to doing::
///
///     operator.lt(path1.split('/'), path2.split('/'))
///
///  The idea is that you should compare path components separately. This
///  differs from plain ``path1 < path2`` for paths like ``'a-b'`` and ``a/b``.
///  "a-b" comes after "a" but would come before "a/b" lexically.
///
/// Args:
///  path1: first path
///  path2: second path
/// Returns: True if path1 comes first, otherwise False
#[pyfunction]
fn lt_by_dirs(path1: &Bound<PyAny>, path2: &Bound<PyAny>) -> PyResult<bool> {
    let path1 = extract_path(path1)?;
    let path2 = extract_path(path2)?;
    Ok(bazaar::dirstate::lt_by_dirs(&path1, &path2))
}

/// Return the index where to insert path into paths.
///
/// This uses the dirblock sorting. So all children in a directory come before
/// the children of children. For example::
///
///     a/
///       b/
///         c
///       d/
///         e
///       b-c
///       d-e
///     a-a
///     a=c
///
/// Will be sorted as::
///
///     a
///     a-a
///     a=c
///     a/b
///     a/b-c
///     a/d
///     a/d-e
///     a/b/c
///     a/d/e
///
/// Args:
///   paths: A list of paths to search through
///   path: A single path to insert
/// Returns: An offset where 'path' can be inserted.
/// See also: bisect.bisect_left

#[pyfunction]
fn bisect_path_left(paths: Vec<Bound<PyAny>>, path: &Bound<PyAny>) -> PyResult<usize> {
    let path = extract_path(path)?;
    let paths = paths
        .iter()
        .map(|x| extract_path(x).unwrap())
        .collect::<Vec<PathBuf>>();
    let offset = bazaar::dirstate::bisect_path_left(
        paths
            .iter()
            .map(|x| x.as_path())
            .collect::<Vec<&Path>>()
            .as_slice(),
        &path,
    );
    Ok(offset)
}

/// Return the index where to insert path into paths.
///
/// This uses a path-wise comparison so we get::
///     a
///     a-b
///     a=b
///     a/b
/// Rather than::
///     a
///     a-b
///     a/b
///     a=b
///
/// Args:
///   paths: A list of paths to search through
///   path: A single path to insert
/// Returns: An offset where 'path' can be inserted.
/// See also: bisect.bisect_right
#[pyfunction]
fn bisect_path_right(paths: Vec<Bound<PyAny>>, path: &Bound<PyAny>) -> PyResult<usize> {
    let path = extract_path(path)?;
    let paths = paths
        .iter()
        .map(|x| extract_path(x).unwrap())
        .collect::<Vec<PathBuf>>();
    let offset = bazaar::dirstate::bisect_path_right(
        paths
            .iter()
            .map(|x| x.as_path())
            .collect::<Vec<&Path>>()
            .as_slice(),
        &path,
    );
    Ok(offset)
}

#[pyfunction]
fn lt_path_by_dirblock(path1: &Bound<PyAny>, path2: &Bound<PyAny>) -> PyResult<bool> {
    let path1 = extract_path(path1)?;
    let path2 = extract_path(path2)?;
    Ok(bazaar::dirstate::lt_path_by_dirblock(&path1, &path2))
}

#[pyfunction]
#[pyo3(signature = (dirblocks, dirname, lo=None, hi=None, cache=None))]
fn bisect_dirblock(
    py: Python,
    dirblocks: &Bound<PyList>,
    dirname: &Bound<PyAny>,
    lo: Option<usize>,
    hi: Option<usize>,
    cache: Option<Bound<PyDict>>,
) -> PyResult<usize> {
    fn split_object(obj: &Bound<PyAny>) -> PyResult<Vec<PathBuf>> {
        if let Ok(py_str) = obj.extract::<Bound<PyString>>() {
            Ok(py_str
                .to_string()
                .split('/')
                .map(PathBuf::from)
                .collect::<Vec<_>>())
        } else if let Ok(py_bytes) = obj.extract::<Bound<PyBytes>>() {
            Ok(py_bytes
                .as_bytes()
                .split(|&byte| byte == b'/')
                .map(|s| PathBuf::from(String::from_utf8_lossy(s).to_string()))
                .collect::<Vec<_>>())
        } else {
            Err(PyTypeError::new_err("Not a PyBytes or PyString"))
        }
    }

    let hi = hi.unwrap_or(dirblocks.len());
    let cache = cache.unwrap_or_else(|| PyDict::new(py));

    let dirname_split = match cache.get_item(dirname)? {
        Some(item) => item.extract::<Vec<PathBuf>>()?,
        None => {
            let split = split_object(dirname)?;
            cache.set_item(dirname.clone(), split.clone())?;
            split
        }
    };

    let mut lo = lo.unwrap_or(0);
    let mut hi = hi;

    while lo < hi {
        let mid = (lo + hi) / 2;
        let dirblock = dirblocks.get_item(mid)?.downcast_into::<PyTuple>()?;
        let cur = dirblock.get_item(0)?;

        let cur_split = match cache.get_item(&cur)? {
            Some(item) => item.extract::<Vec<PathBuf>>()?,
            None => {
                let split = split_object(&cur)?;
                cache.set_item(cur, split.clone())?;
                split
            }
        };

        if cur_split < dirname_split {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    Ok(lo)
}

// TODO(jelmer): Move this into a more central place?
#[pyclass]
struct StatResult {
    metadata: std::fs::Metadata,
}

#[pymethods]
impl StatResult {
    #[getter]
    fn st_size(&self) -> PyResult<u64> {
        Ok(self.metadata.len())
    }

    #[getter]
    fn st_mtime(&self) -> PyResult<u64> {
        let modified = self
            .metadata
            .modified()
            .map_err(PyErr::new::<pyo3::exceptions::PyOSError, _>)?;
        let since_epoch = modified
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(e.to_string()))?;
        Ok(since_epoch.as_secs())
    }

    #[getter]
    fn st_ctime(&self) -> PyResult<u64> {
        let created = self
            .metadata
            .created()
            .map_err(PyErr::new::<pyo3::exceptions::PyOSError, _>)?;
        let since_epoch = created
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyOSError, _>(e.to_string()))?;
        Ok(since_epoch.as_secs())
    }

    #[cfg(unix)]
    #[getter]
    fn st_mode(&self) -> PyResult<u32> {
        Ok(self.metadata.permissions().mode())
    }

    #[cfg(not(unix))]
    #[getter]
    fn st_mode(&self) -> PyResult<u32> {
        Ok(0)
    }

    #[cfg(unix)]
    #[getter]
    fn st_dev(&self) -> PyResult<u64> {
        Ok(self.metadata.dev())
    }

    #[cfg(unix)]
    #[getter]
    fn st_ino(&self) -> PyResult<u64> {
        Ok(self.metadata.ino())
    }
}

#[pyclass]
struct SHA1Provider {
    provider: Box<dyn bazaar::dirstate::SHA1Provider>,
}

#[pymethods]
impl SHA1Provider {
    fn sha1<'a>(&mut self, py: Python<'a>, path: &Bound<PyAny>) -> PyResult<Bound<'a, PyBytes>> {
        let path = extract_path(path)?;
        let sha1 = self
            .provider
            .sha1(&path)
            .map_err(PyErr::new::<pyo3::exceptions::PyOSError, _>)?;
        Ok(PyBytes::new(py, sha1.as_bytes()))
    }

    fn stat_and_sha1<'a>(
        &mut self,
        py: Python<'a>,
        path: &Bound<PyAny>,
    ) -> PyResult<(Py<PyAny>, Bound<'a, PyBytes>)> {
        let path = extract_path(path)?;
        let (md, sha1) = self.provider.stat_and_sha1(&path)?;
        let pmd = StatResult { metadata: md };
        Ok((
            pmd.into_pyobject(py)?.unbind().into(),
            PyBytes::new(py, sha1.as_bytes()),
        ))
    }
}

#[pyfunction]
fn DefaultSHA1Provider() -> PyResult<SHA1Provider> {
    Ok(SHA1Provider {
        provider: Box::new(bazaar::dirstate::DefaultSHA1Provider::new()),
    })
}

/// Python constants that [`DirStateRs`] uses in its scalar-state
/// getters/setters to match `bzrformats.dirstate.DirState`'s
/// `NOT_IN_MEMORY` / `IN_MEMORY_UNMODIFIED` / `IN_MEMORY_MODIFIED` /
/// `IN_MEMORY_HASH_MODIFIED` class attributes.
const PY_NOT_IN_MEMORY: i64 = 0;
const PY_IN_MEMORY_UNMODIFIED: i64 = 1;
const PY_IN_MEMORY_MODIFIED: i64 = 2;
const PY_IN_MEMORY_HASH_MODIFIED: i64 = 3;

/// Build the Python tuple representation of a single dirstate entry,
/// matching the shape `((dirname, basename, file_id),
/// [(minikind, fingerprint, size, executable, packed_stat), ...])`
/// that `DirStateRs.dirblocks` and the rest of the legacy Python
/// `_dirblocks` consumers use.
fn entry_to_py_tuple<'py>(
    py: Python<'py>,
    entry: &bazaar::dirstate::Entry,
) -> PyResult<Bound<'py, PyTuple>> {
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
                PyBytes::new(py, &[tree.minikind]).into_any(),
                PyBytes::new(py, &tree.fingerprint).into_any(),
                tree.size.into_pyobject(py)?.into_any(),
                tree.executable.into_pyobject(py)?.to_owned().into_any(),
                PyBytes::new(py, &tree.packed_stat).into_any(),
            ],
        )?;
        trees.append(tree_tuple)?;
    }
    PyTuple::new(py, [key.as_any(), trees.as_any()])
}

/// Collect any Python iterable of `bytes` into a `Vec<Vec<u8>>`. Used
/// by the parents / ghosts setters on [`PyDirState`] to accept plain
/// Python lists as well as tuples or generators.
fn collect_bytes_vec(obj: &Bound<PyAny>) -> PyResult<Vec<Vec<u8>>> {
    let mut out = Vec::new();
    for item in obj.try_iter()? {
        out.push(item?.extract::<Vec<u8>>()?);
    }
    Ok(out)
}

fn memory_state_to_py(state: bazaar::dirstate::MemoryState) -> i64 {
    use bazaar::dirstate::MemoryState;
    match state {
        MemoryState::NotInMemory => PY_NOT_IN_MEMORY,
        MemoryState::InMemoryUnmodified => PY_IN_MEMORY_UNMODIFIED,
        MemoryState::InMemoryModified => PY_IN_MEMORY_MODIFIED,
        MemoryState::InMemoryHashModified => PY_IN_MEMORY_HASH_MODIFIED,
    }
}

fn memory_state_from_py(value: i64) -> PyResult<bazaar::dirstate::MemoryState> {
    use bazaar::dirstate::MemoryState;
    match value {
        PY_NOT_IN_MEMORY => Ok(MemoryState::NotInMemory),
        PY_IN_MEMORY_UNMODIFIED => Ok(MemoryState::InMemoryUnmodified),
        PY_IN_MEMORY_MODIFIED => Ok(MemoryState::InMemoryModified),
        PY_IN_MEMORY_HASH_MODIFIED => Ok(MemoryState::InMemoryHashModified),
        other => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "invalid memory state: {}",
            other
        ))),
    }
}

/// Python-facing owner of a pure-Rust [`bazaar::dirstate::DirState`].
///
/// This is the beginning of the gradual replacement of
/// `bzrformats.dirstate.DirState` with the Rust port: each commit
/// exposes a few more attributes or methods, Python's `DirState`
/// gradually delegates to them, and once the whole surface is here
/// the Python class collapses into a thin shim.
///
/// Commit 1 (this one) only exposes the scalar state flags and the
/// methods from the pure crate that do not touch dirblocks/parents
/// (`worth_saving`, `wipe_state`, `mark_modified`, `mark_unmodified`,
/// `num_present_parents`). Dirblocks, parents, ghosts, id_index, the
/// save path, and the various get_entry/iter variants come in later
/// commits.
#[pyclass(name = "DirStateRs", unsendable)]
struct PyDirState {
    inner: bazaar::dirstate::DirState,
}

#[pymethods]
impl PyDirState {
    /// Construct an empty dirstate at `path`. Mirrors Python's
    /// `DirState.__init__` for the pure-state fields only — lock and
    /// file-object plumbing stays on the Python side until its
    /// counterpart exists in Rust.
    #[new]
    #[pyo3(signature = (
        path,
        sha1_provider = None,
        worth_saving_limit = 0,
        use_filesystem_for_exec = true,
        fdatasync = false,
    ))]
    fn new(
        path: &Bound<PyAny>,
        sha1_provider: Option<&Bound<PyAny>>,
        worth_saving_limit: i64,
        use_filesystem_for_exec: bool,
        fdatasync: bool,
    ) -> PyResult<Self> {
        let path = extract_path(path)?;
        // Commit 1 only supports the default sha1 provider. Custom
        // providers — whether the pyo3 SHA1Provider wrapper or an
        // arbitrary Python callable — need a dedicated adapter, which
        // is a follow-up commit.
        if sha1_provider.is_some() {
            return Err(pyo3::exceptions::PyNotImplementedError::new_err(
                "custom sha1_provider is not yet wired through DirStateRs",
            ));
        }
        let provider: Box<dyn bazaar::dirstate::SHA1Provider> =
            Box::new(bazaar::dirstate::DefaultSHA1Provider::new());
        Ok(Self {
            inner: bazaar::dirstate::DirState::new(
                path,
                provider,
                worth_saving_limit,
                use_filesystem_for_exec,
                fdatasync,
            ),
        })
    }

    /// On-disk filename the dirstate points at. Read-only; matches
    /// Python's `DirState._filename` attribute.
    #[getter]
    fn filename<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        // Python stores `_filename` as bytes on POSIX and as str on
        // Windows; we always return bytes for now, matching the
        // POSIX-only branch that dirstate tests exercise.
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            Ok(PyBytes::new(py, self.inner.filename.as_os_str().as_bytes()))
        }
        #[cfg(not(unix))]
        {
            let s = self
                .inner
                .filename
                .to_str()
                .ok_or_else(|| PyTypeError::new_err("dirstate filename is not valid utf-8"))?;
            Ok(PyBytes::new(py, s.as_bytes()))
        }
    }

    /// Header state flag matching Python's `_header_state` attribute.
    #[getter]
    fn header_state(&self) -> i64 {
        memory_state_to_py(self.inner.header_state)
    }

    #[setter]
    fn set_header_state(&mut self, value: i64) -> PyResult<()> {
        self.inner.header_state = memory_state_from_py(value)?;
        Ok(())
    }

    /// Dirblock state flag matching Python's `_dirblock_state`.
    #[getter]
    fn dirblock_state(&self) -> i64 {
        memory_state_to_py(self.inner.dirblock_state)
    }

    #[setter]
    fn set_dirblock_state(&mut self, value: i64) -> PyResult<()> {
        self.inner.dirblock_state = memory_state_from_py(value)?;
        Ok(())
    }

    #[getter]
    fn changes_aborted(&self) -> bool {
        self.inner.changes_aborted
    }

    #[setter]
    fn set_changes_aborted(&mut self, value: bool) {
        self.inner.changes_aborted = value;
    }

    /// Offset in the backing file where the header ends and the
    /// dirblock body begins. `None` before the header has been read.
    /// Matches Python's `_end_of_header` attribute.
    #[getter]
    fn end_of_header(&self) -> Option<u64> {
        self.inner.end_of_header
    }

    #[setter]
    fn set_end_of_header(&mut self, value: Option<u64>) {
        self.inner.end_of_header = value;
    }

    /// Cutoff mtime/ctime used when deciding whether cached sha1s are
    /// trustworthy. `None` before `_sha_cutoff_time` runs. Matches
    /// Python's `_cutoff_time` attribute.
    #[getter]
    fn cutoff_time(&self) -> Option<i64> {
        self.inner.cutoff_time
    }

    #[setter]
    fn set_cutoff_time(&mut self, value: Option<i64>) {
        self.inner.cutoff_time = value;
    }

    /// Declared entry count from the header. Matches Python's
    /// `_num_entries`; Python stores `None` before the header is read,
    /// but the Rust struct always has a count, so we expose the
    /// numeric value unconditionally.
    #[getter]
    fn num_entries(&self) -> usize {
        self.inner.num_entries
    }

    #[setter]
    fn set_num_entries(&mut self, value: usize) {
        self.inner.num_entries = value;
    }

    #[getter]
    fn worth_saving_limit(&self) -> i64 {
        self.inner.worth_saving_limit
    }

    #[setter]
    fn set_worth_saving_limit(&mut self, value: i64) {
        self.inner.worth_saving_limit = value;
    }

    #[getter]
    fn fdatasync(&self) -> bool {
        self.inner.fdatasync
    }

    #[setter]
    fn set_fdatasync(&mut self, value: bool) {
        self.inner.fdatasync = value;
    }

    #[getter]
    fn use_filesystem_for_exec(&self) -> bool {
        self.inner.use_filesystem_for_exec
    }

    #[setter]
    fn set_use_filesystem_for_exec(&mut self, value: bool) {
        self.inner.use_filesystem_for_exec = value;
    }

    #[getter]
    fn bisect_page_size(&self) -> usize {
        self.inner.bisect_page_size
    }

    #[setter]
    fn set_bisect_page_size(&mut self, value: usize) {
        self.inner.bisect_page_size = value;
    }

    /// Number of parent entries present in each record row. Mirrors
    /// Python's `DirState._num_present_parents`.
    fn num_present_parents(&self) -> usize {
        self.inner.num_present_parents()
    }

    /// Parent revision ids for the current tree, in order. First
    /// entry is the current parent; subsequent entries are merged
    /// parents. Matches Python's `DirState._parents` attribute.
    ///
    /// Returns a fresh Python list on each access — mutating that
    /// list does NOT write back to the dirstate. Use
    /// [`Self::append_parent`] or [`Self::set_parent_at`] for in-place
    /// mutation, or assign the attribute to replace the list
    /// wholesale.
    #[getter]
    fn parents<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let items: Vec<Bound<PyBytes>> = self
            .inner
            .parents
            .iter()
            .map(|p| PyBytes::new(py, p))
            .collect();
        PyList::new(py, items)
    }

    #[setter]
    fn set_parents(&mut self, value: &Bound<PyAny>) -> PyResult<()> {
        self.inner.parents = collect_bytes_vec(value)?;
        Ok(())
    }

    /// Ghost parent revision ids: parents referenced by the tree but
    /// not present locally. Same aliasing semantics as
    /// [`Self::parents`] — the getter returns a copy.
    #[getter]
    fn ghosts<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let items: Vec<Bound<PyBytes>> = self
            .inner
            .ghosts
            .iter()
            .map(|g| PyBytes::new(py, g))
            .collect();
        PyList::new(py, items)
    }

    #[setter]
    fn set_ghosts(&mut self, value: &Bound<PyAny>) -> PyResult<()> {
        self.inner.ghosts = collect_bytes_vec(value)?;
        Ok(())
    }

    /// Append a revision id to the parents list in place. Replaces
    /// the Python pattern `self._parents.append(revid)`.
    fn append_parent(&mut self, revid: Vec<u8>) {
        self.inner.parents.push(revid);
    }

    /// In-memory dirblocks, in the same list-of-tuples shape Python's
    /// `DirState._dirblocks` uses. Each block is `(dirname_bytes,
    /// [entry_tuple, ...])`; each entry is
    /// `((dirname, basename, file_id), [tree_tuple, ...])`; each tree
    /// tuple is `(minikind, fingerprint, size, executable,
    /// packed_stat_or_revid)`.
    ///
    /// Both the getter and the setter convert the full dirblock tree
    /// on every call. They exist as a temporary sync boundary while
    /// dirblock ownership migrates from Python's `_dirblocks`
    /// attribute to the pure-Rust `DirState.dirblocks` field. Once
    /// every reader and writer on the Python side has migrated, these
    /// conversions go away along with Python's `_dirblocks`.
    ///
    /// Writing through the setter clears the cached id_index, since
    /// the previous index is no longer consistent with the new data.
    #[getter]
    fn dirblocks<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        crate::dirstate_helpers::dirblocks_to_py(py, &self.inner.dirblocks)
    }

    #[setter]
    fn set_dirblocks(&mut self, value: &Bound<PyAny>) -> PyResult<()> {
        let new_blocks = crate::dirstate_helpers::dirblocks_from_py(value)?;
        self.inner.dirblocks = new_blocks;
        self.inner.id_index = None;
        self.inner.packed_stat_index = None;
        Ok(())
    }

    /// Replace the parent at `index`. Replaces the Python pattern
    /// `self._parents[index] = revid`. Raises `IndexError` if `index`
    /// is out of range.
    fn set_parent_at(&mut self, index: usize, revid: Vec<u8>) -> PyResult<()> {
        if index >= self.inner.parents.len() {
            return Err(pyo3::exceptions::PyIndexError::new_err(
                "parent index out of range",
            ));
        }
        self.inner.parents[index] = revid;
        Ok(())
    }

    /// Whether the current in-memory state is worth persisting. Mirrors
    /// `DirState._worth_saving`.
    fn worth_saving(&self) -> bool {
        self.inner.worth_saving()
    }

    /// Forget all in-memory state. Mirrors `DirState._wipe_state`.
    fn wipe_state(&mut self) {
        self.inner.wipe_state();
    }

    /// Discard any parent trees beyond the first, including any
    /// entries that are dead in both tree 0 and tree 1 after the
    /// discard. Mirrors Python's `DirState._discard_merge_parents`.
    fn discard_merge_parents(&mut self) {
        self.inner.discard_merge_parents();
    }

    /// Split the root dirblock into two sentinel blocks: block 0 with
    /// the root row, block 1 with the contents-of-root rows. Mirrors
    /// Python's `DirState._split_root_dirblock_into_contents`. Raises
    /// `ValueError` when the pre-split layout is not the expected
    /// "everything in block 0, block 1 empty" shape.
    fn split_root_dirblock_into_contents(&mut self) -> PyResult<()> {
        self.inner
            .split_root_dirblock_into_contents()
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{:?}", e)))
    }

    /// Find the dirblock index whose dirname matches `key[0]`.
    /// Mirrors Python's `DirState._find_block_index_from_key` and
    /// returns `(block_index, present)`. Python's one-slot
    /// `_last_block_index` cache is dropped by this port — bisect in
    /// Rust is cheap enough that the extra branch isn't worth it.
    fn find_block_index_from_key(&self, key: &Bound<PyTuple>) -> PyResult<(usize, bool)> {
        let entry_key = bazaar::dirstate::EntryKey {
            dirname: key.get_item(0)?.extract()?,
            basename: key.get_item(1)?.extract()?,
            file_id: key.get_item(2)?.extract()?,
        };
        Ok(self.inner.find_block_index_from_key(&entry_key))
    }

    /// Find the entry index within `block` for `key`. Mirrors Python's
    /// `DirState._find_entry_index`. `block` is the
    /// `self._dirblocks[block_index][1]` list.
    fn find_entry_index(
        &self,
        key: &Bound<PyTuple>,
        block: &Bound<PyAny>,
    ) -> PyResult<(usize, bool)> {
        let entry_key = bazaar::dirstate::EntryKey {
            dirname: key.get_item(0)?.extract()?,
            basename: key.get_item(1)?.extract()?,
            file_id: key.get_item(2)?.extract()?,
        };
        // The caller's `block` is Python's view of
        // self._dirblocks[i][1]; we need the Rust view, so convert
        // the block entries on the fly. This is wasteful — once the
        // dirblock aliasing migrates fully, callers will pass
        // block_index and we can read from self.inner.dirblocks
        // directly.
        let mut entries: Vec<bazaar::dirstate::Entry> = Vec::new();
        for item in block.try_iter()? {
            entries.push(crate::dirstate_helpers::entry_from_py(&item?)?);
        }
        Ok(self.inner.find_entry_index(&entry_key, &entries))
    }

    /// Look up `(dirname, basename)` in `tree_index` and return the
    /// four-field result Python's `DirState._get_block_entry_index`
    /// produces: `(block_index, entry_index, dir_present,
    /// path_present)`.
    fn get_block_entry_index(
        &self,
        dirname: &[u8],
        basename: &[u8],
        tree_index: usize,
    ) -> (usize, usize, bool, bool) {
        let bei = self
            .inner
            .get_block_entry_index(dirname, basename, tree_index);
        (
            bei.block_index,
            bei.entry_index,
            bei.dir_present,
            bei.path_present,
        )
    }

    /// Ensure a dirblock for `dirname` exists. Mirrors Python's
    /// `DirState._ensure_block`: takes the (block_index, row_index)
    /// coordinates of the parent entry (used for the basename
    /// assertion) and returns the index of the block for `dirname`,
    /// creating an empty block if necessary. Raises `AssertionError`
    /// when the supplied dirname does not end with the parent entry's
    /// basename.
    fn ensure_block(
        &mut self,
        parent_block_index: isize,
        parent_row_index: isize,
        dirname: &[u8],
    ) -> PyResult<usize> {
        self.inner
            .ensure_block(parent_block_index, parent_row_index, dirname)
            .map_err(|e| pyo3::exceptions::PyAssertionError::new_err(format!("{:?}", e)))
    }

    /// Return the sha1 of the file whose packed_stat matches
    /// `packed_stat`, or `None` if no such file is present. Mirrors
    /// Python's `DirState.sha1_from_stat` slow path
    /// (`_get_packed_stat_index().get(pack_stat(stat))`). The caller
    /// provides the already-packed stat bytes since pack_stat is
    /// already a pure-Rust pyo3 function on the module.
    fn sha1_from_packed_stat<'py>(
        &mut self,
        py: Python<'py>,
        packed_stat: &[u8],
    ) -> Option<Bound<'py, PyBytes>> {
        self.inner
            .get_or_build_packed_stat_index()
            .get(packed_stat)
            .map(|sha1| PyBytes::new(py, sha1))
    }

    /// Mark the entry at `key` as absent for tree 0, returning True
    /// when the entry row was removed entirely (the "last reference"
    /// case). Mirrors Python's `DirState._make_absent`.
    fn make_absent(&mut self, key: &Bound<PyTuple>) -> PyResult<bool> {
        let entry_key = bazaar::dirstate::EntryKey {
            dirname: key.get_item(0)?.extract()?,
            basename: key.get_item(1)?.extract()?,
            file_id: key.get_item(2)?.extract()?,
        };
        self.inner
            .make_absent(&entry_key)
            .map_err(|e| pyo3::exceptions::PyAssertionError::new_err(e.to_string()))
    }

    /// Apply a sequence of "adds" to tree 1. Mirrors Python's
    /// `DirState._update_basis_apply_adds`. The input is a Python
    /// iterable of `(old_path, new_path, file_id, new_details,
    /// real_add)` 5-tuples where `new_details` itself is a
    /// `(minikind, fingerprint, size, executable, packed_stat)`
    /// 5-tuple, matching the shape Python's `update_basis_by_delta`
    /// passes through today.
    ///
    /// Raises `InconsistentDelta(path, file_id, reason)` for
    /// caller-visible delta problems (setting `changes_aborted` on
    /// the inner state first, mirroring Python's `_raise_invalid`),
    /// `NotImplementedError` for the basis-relocation branch, and
    /// `AssertionError` for internal invariant violations.
    fn update_basis_apply_adds(&mut self, adds: &Bound<PyAny>) -> PyResult<()> {
        let mut rust_adds: Vec<bazaar::dirstate::BasisAdd> = Vec::new();
        for item in adds.try_iter()? {
            let tup = item?.cast_into::<PyTuple>()?;
            if tup.len() != 5 {
                return Err(PyTypeError::new_err(
                    "update_basis_apply_adds entries must be 5-tuples",
                ));
            }
            let old_path: Option<Vec<u8>> = {
                let obj = tup.get_item(0)?;
                if obj.is_none() {
                    None
                } else {
                    Some(obj.extract()?)
                }
            };
            let new_path: Vec<u8> = tup.get_item(1)?.extract()?;
            let file_id: Vec<u8> = tup.get_item(2)?.extract()?;
            let details_tup = tup.get_item(3)?.cast_into::<PyTuple>()?;
            if details_tup.len() != 5 {
                return Err(PyTypeError::new_err(
                    "entry details tuple must have 5 fields",
                ));
            }
            let minikind_bytes: Vec<u8> = details_tup.get_item(0)?.extract()?;
            let new_details = bazaar::dirstate::TreeData {
                minikind: minikind_bytes.first().copied().unwrap_or(0),
                fingerprint: details_tup.get_item(1)?.extract()?,
                size: details_tup.get_item(2)?.extract()?,
                executable: details_tup.get_item(3)?.extract()?,
                packed_stat: details_tup.get_item(4)?.extract()?,
            };
            let real_add: bool = tup.get_item(4)?.extract()?;
            rust_adds.push(bazaar::dirstate::BasisAdd {
                old_path,
                new_path,
                file_id,
                new_details,
                real_add,
            });
        }

        match self.inner.update_basis_apply_adds(&mut rust_adds) {
            Ok(()) => Ok(()),
            Err(e) => Err(self.raise_basis_apply_error(adds.py(), e)),
        }
    }

    /// Apply a sequence of "removals" to tree 0. Mirrors Python's
    /// `DirState._apply_removals`. Input is a Python iterable of
    /// `(file_id, path)` 2-tuples, matching the caller pattern
    /// `update_by_delta` uses: `removals.items()`.
    fn apply_removals(&mut self, removals: &Bound<PyAny>) -> PyResult<()> {
        let mut rust_removals: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for item in removals.try_iter()? {
            let tup = item?.cast_into::<PyTuple>()?;
            if tup.len() != 2 {
                return Err(PyTypeError::new_err(
                    "apply_removals entries must be 2-tuples",
                ));
            }
            let file_id: Vec<u8> = tup.get_item(0)?.extract()?;
            let path: Vec<u8> = tup.get_item(1)?.extract()?;
            rust_removals.push((file_id, path));
        }
        match self.inner.apply_removals(&rust_removals) {
            Ok(()) => Ok(()),
            Err(e) => Err(self.raise_basis_apply_error(removals.py(), e)),
        }
    }

    /// Apply a sequence of "changes" to tree 1. Mirrors Python's
    /// `DirState._update_basis_apply_changes`. Input is a Python
    /// iterable of `(old_path, new_path, file_id, new_details)`
    /// 4-tuples; `new_details` is the same 5-tuple layout used by
    /// `update_basis_apply_adds`. Raises `InconsistentDelta` on a
    /// stale entry.
    fn update_basis_apply_changes(&mut self, changes: &Bound<PyAny>) -> PyResult<()> {
        let mut rust_changes: Vec<(Vec<u8>, Vec<u8>, Vec<u8>, bazaar::dirstate::TreeData)> =
            Vec::new();
        for item in changes.try_iter()? {
            let tup = item?.cast_into::<PyTuple>()?;
            if tup.len() != 4 {
                return Err(PyTypeError::new_err(
                    "update_basis_apply_changes entries must be 4-tuples",
                ));
            }
            let old_path: Vec<u8> = tup.get_item(0)?.extract()?;
            let new_path: Vec<u8> = tup.get_item(1)?.extract()?;
            let file_id: Vec<u8> = tup.get_item(2)?.extract()?;
            let details_tup = tup.get_item(3)?.cast_into::<PyTuple>()?;
            let minikind_bytes: Vec<u8> = details_tup.get_item(0)?.extract()?;
            let new_details = bazaar::dirstate::TreeData {
                minikind: minikind_bytes.first().copied().unwrap_or(0),
                fingerprint: details_tup.get_item(1)?.extract()?,
                size: details_tup.get_item(2)?.extract()?,
                executable: details_tup.get_item(3)?.extract()?,
                packed_stat: details_tup.get_item(4)?.extract()?,
            };
            rust_changes.push((old_path, new_path, file_id, new_details));
        }
        match self.inner.update_basis_apply_changes(&rust_changes) {
            Ok(()) => Ok(()),
            Err(e) => Err(self.raise_basis_apply_error(changes.py(), e)),
        }
    }

    /// Apply a sequence of "deletes" to tree 1. Mirrors Python's
    /// `DirState._update_basis_apply_deletes`. Input is a Python
    /// iterable of `(old_path, new_path_or_None, file_id, _ignored,
    /// real_delete)` 5-tuples — the 4th element is unused by the
    /// Python implementation (it carries `None` in the current
    /// caller) but we accept it to preserve the existing wire shape.
    fn update_basis_apply_deletes(&mut self, deletes: &Bound<PyAny>) -> PyResult<()> {
        let mut rust_deletes: Vec<(Vec<u8>, Option<Vec<u8>>, Vec<u8>, bool)> = Vec::new();
        for item in deletes.try_iter()? {
            let tup = item?.cast_into::<PyTuple>()?;
            if tup.len() != 5 {
                return Err(PyTypeError::new_err(
                    "update_basis_apply_deletes entries must be 5-tuples",
                ));
            }
            let old_path: Vec<u8> = tup.get_item(0)?.extract()?;
            let new_path: Option<Vec<u8>> = {
                let obj = tup.get_item(1)?;
                if obj.is_none() {
                    None
                } else {
                    Some(obj.extract()?)
                }
            };
            let file_id: Vec<u8> = tup.get_item(2)?.extract()?;
            // tup.get_item(3) ignored — matches Python's `_` binding.
            let real_delete: bool = tup.get_item(4)?.extract()?;
            rust_deletes.push((old_path, new_path, file_id, real_delete));
        }
        match self.inner.update_basis_apply_deletes(&rust_deletes) {
            Ok(()) => Ok(()),
            Err(e) => Err(self.raise_basis_apply_error(deletes.py(), e)),
        }
    }

    /// Replace the entire in-memory state with `parent_ids` and
    /// `dirblocks` (both in the Python tuple shape), marking both the
    /// header and the dirblock data fully modified. Mirrors Python's
    /// `DirState._set_data`. Invalidates the cached id_index.
    fn set_data(&mut self, parent_ids: &Bound<PyAny>, dirblocks: &Bound<PyAny>) -> PyResult<()> {
        let parents = collect_bytes_vec(parent_ids)?;
        let blocks = crate::dirstate_helpers::dirblocks_from_py(dirblocks)?;
        self.inner.set_data(parents, blocks);
        Ok(())
    }

    /// Rebuild dirblocks from a flat, sorted list of entries.
    /// Mirrors Python's `DirState._entries_to_current_state`:
    /// assembles per-directory dirblocks from the sorted entry
    /// stream and runs `split_root_dirblock_into_contents` at the
    /// end so the two empty-dirname sentinel blocks are present.
    ///
    /// The input is a Python iterable of entry tuples in the same
    /// shape as `DirStateRs.dirblocks` entries. Raises `ValueError`
    /// if the entry list is empty or does not start with the root
    /// row.
    fn entries_to_current_state(&mut self, new_entries: &Bound<PyAny>) -> PyResult<()> {
        let mut entries: Vec<bazaar::dirstate::Entry> = Vec::new();
        for item in new_entries.try_iter()? {
            let item = item?;
            entries.push(crate::dirstate_helpers::entry_from_py(&item)?);
        }
        self.inner
            .entries_to_current_state(entries)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{:?}", e)))
    }

    /// Mark the dirstate as modified. `hash_changed_keys` is an
    /// optional iterable of `(dirname, basename, file_id)` tuples
    /// indicating hash-only changes; pass `None` for a full
    /// modification. Mirrors `DirState._mark_modified`.
    #[pyo3(signature = (hash_changed_keys = None, header_modified = false))]
    fn mark_modified(
        &mut self,
        hash_changed_keys: Option<&Bound<PyAny>>,
        header_modified: bool,
    ) -> PyResult<()> {
        let mut keys: Vec<bazaar::dirstate::EntryKey> = Vec::new();
        if let Some(iter) = hash_changed_keys {
            for item in iter.try_iter()? {
                let tup = item?.cast_into::<PyTuple>()?;
                if tup.len() != 3 {
                    return Err(PyTypeError::new_err(
                        "hash_changed_keys entries must be 3-tuples",
                    ));
                }
                let dirname: Vec<u8> = tup.get_item(0)?.extract()?;
                let basename: Vec<u8> = tup.get_item(1)?.extract()?;
                let file_id: Vec<u8> = tup.get_item(2)?.extract()?;
                keys.push(bazaar::dirstate::EntryKey {
                    dirname,
                    basename,
                    file_id,
                });
            }
        }
        self.inner.mark_modified(&keys, header_modified);
        Ok(())
    }

    /// Mark the dirstate as unmodified. Mirrors
    /// `DirState._mark_unmodified`.
    fn mark_unmodified(&mut self) {
        self.inner.mark_unmodified();
    }

    /// Serialise the in-memory state to the byte chunks that make up
    /// the on-disk dirstate file. Mirrors Python's
    /// `DirState.get_lines` slow path (the fast path that re-reads
    /// unchanged bytes from disk belongs to the caller, since it
    /// requires the Python `_state_file` handle).
    fn get_lines<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let lines = self.inner.get_lines();
        let items: Vec<Bound<PyBytes>> = lines.iter().map(|l| PyBytes::new(py, l)).collect();
        PyList::new(py, items)
    }

    /// Return all dirstate entries whose key `(dirname, basename)`
    /// matches `path_utf8`, across every file id. Mirrors Python's
    /// `DirState._entries_for_path`. Returns a snapshot list of
    /// entries in the `DirStateRs.dirblocks` tuple shape.
    fn entries_for_path<'py>(
        &self,
        py: Python<'py>,
        path_utf8: &[u8],
    ) -> PyResult<Bound<'py, PyList>> {
        let entries = self.inner.entries_for_path(path_utf8);
        let out = PyList::empty(py);
        for entry in entries {
            out.append(entry_to_py_tuple(py, entry)?)?;
        }
        Ok(out)
    }

    /// Walk the subtree rooted at `path_utf8` and return every live
    /// entry in `tree_index`. Mirrors Python's
    /// `DirState._iter_child_entries`. Returns a list of Python
    /// entries in the same tuple shape as `DirStateRs.dirblocks`.
    ///
    /// The result is a snapshot: mutating returned entry tuples does
    /// NOT write back to the Rust-owned dirblocks. Callers that need
    /// in-place mutation must go through the (not-yet-exposed) Rust
    /// mutation methods.
    fn iter_child_entries<'py>(
        &mut self,
        py: Python<'py>,
        tree_index: usize,
        path_utf8: &[u8],
    ) -> PyResult<Bound<'py, PyList>> {
        let entries = self.inner.iter_child_entries(tree_index, path_utf8);
        let out = PyList::empty(py);
        for entry in &entries {
            out.append(entry_to_py_tuple(py, entry)?)?;
        }
        Ok(out)
    }
}

impl PyDirState {
    /// Shared error conversion for the three update_basis_apply_*
    /// methods: `Invalid` becomes `bzrformats.errors.InconsistentDelta`
    /// and also sets `changes_aborted` on the inner state (mirroring
    /// Python's `_raise_invalid`); `NotImplemented` and `Internal`
    /// become `NotImplementedError` and `AssertionError`.
    ///
    /// Defined in a plain `impl` block rather than `#[pymethods]`
    /// because `BasisApplyError` is not FFI-exposable.
    fn raise_basis_apply_error(
        &mut self,
        py: Python<'_>,
        err: bazaar::dirstate::BasisApplyError,
    ) -> PyErr {
        match err {
            bazaar::dirstate::BasisApplyError::Invalid {
                path,
                file_id,
                reason,
            } => {
                self.inner.changes_aborted = true;
                let errors_mod = match py.import("bzrformats.errors") {
                    Ok(m) => m,
                    Err(e) => return e,
                };
                let cls = match errors_mod.getattr("InconsistentDelta") {
                    Ok(c) => c,
                    Err(e) => return e,
                };
                let path_bytes = PyBytes::new(py, &path);
                let file_id_bytes = PyBytes::new(py, &file_id);
                match cls.call1((path_bytes, file_id_bytes, reason)) {
                    Ok(instance) => PyErr::from_value(instance),
                    Err(e) => e,
                }
            }
            bazaar::dirstate::BasisApplyError::NotImplemented { reason } => {
                pyo3::exceptions::PyNotImplementedError::new_err(reason)
            }
            bazaar::dirstate::BasisApplyError::Internal { reason } => {
                pyo3::exceptions::PyAssertionError::new_err(reason)
            }
        }
    }
}

fn extract_fs_time(obj: &Bound<PyAny>) -> PyResult<u64> {
    if let Ok(u) = obj.extract::<u64>() {
        Ok(u)
    } else if let Ok(u) = obj.extract::<f64>() {
        Ok(u as u64)
    } else {
        Err(PyTypeError::new_err("Not a float or int"))
    }
}

#[pyfunction]
fn pack_stat<'a>(stat_result: &'a Bound<'a, PyAny>) -> PyResult<Bound<'a, PyBytes>> {
    let size = stat_result.getattr("st_size")?.extract::<u64>()?;
    let mtime = extract_fs_time(&stat_result.getattr("st_mtime")?)?;
    let ctime = extract_fs_time(&stat_result.getattr("st_ctime")?)?;
    let dev = stat_result.getattr("st_dev")?.extract::<u64>()?;
    let ino = stat_result.getattr("st_ino")?.extract::<u64>()?;
    let mode = stat_result.getattr("st_mode")?.extract::<u32>()?;
    let s = bazaar::dirstate::pack_stat(size, mtime, ctime, dev, ino, mode);
    Ok(PyBytes::new(stat_result.py(), s.as_bytes()))
}

#[pyfunction]
fn fields_per_entry(num_present_parents: usize) -> usize {
    bazaar::dirstate::fields_per_entry(num_present_parents)
}

#[pyfunction]
fn get_ghosts_line(py: Python, ghost_ids: Vec<Vec<u8>>) -> PyResult<Bound<PyBytes>> {
    let ghost_ids = ghost_ids
        .iter()
        .map(|x| x.as_slice())
        .collect::<Vec<&[u8]>>();
    let bs = bazaar::dirstate::get_ghosts_line(ghost_ids.as_slice());
    Ok(PyBytes::new(py, bs.as_slice()))
}

#[pyfunction]
fn get_parents_line(py: Python, parent_ids: Vec<Vec<u8>>) -> PyResult<Bound<PyBytes>> {
    let parent_ids = parent_ids
        .iter()
        .map(|x| x.as_slice())
        .collect::<Vec<&[u8]>>();
    let bs = bazaar::dirstate::get_parents_line(parent_ids.as_slice());
    Ok(PyBytes::new(py, bs.as_slice()))
}

#[pyclass]
struct IdIndex(bazaar::dirstate::IdIndex);

#[pymethods]
impl IdIndex {
    #[new]
    fn new() -> Self {
        IdIndex(bazaar::dirstate::IdIndex::new())
    }

    fn add(&mut self, entry: (Vec<u8>, Vec<u8>, FileId)) -> PyResult<()> {
        self.0.add((&entry.0, &entry.1, &entry.2));
        Ok(())
    }

    fn remove(&mut self, entry: (Vec<u8>, Vec<u8>, FileId)) -> PyResult<()> {
        self.0.remove((&entry.0, &entry.1, &entry.2));
        Ok(())
    }

    fn get<'a>(
        &self,
        py: Python<'a>,
        file_id: FileId,
    ) -> PyResult<Vec<(Bound<'a, PyBytes>, Bound<'a, PyBytes>, Bound<'a, PyBytes>)>> {
        let ret = self.0.get(&file_id);
        ret.iter()
            .map(|(a, b, c)| {
                Ok((
                    PyBytes::new(py, a),
                    PyBytes::new(py, b),
                    c.into_pyobject(py)?,
                ))
            })
            .collect::<PyResult<Vec<_>>>()
    }

    fn iter_all<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<
        Vec<(
            Bound<'py, PyBytes>,
            Bound<'py, PyBytes>,
            Bound<'py, PyBytes>,
        )>,
    > {
        let ret = self.0.iter_all();
        ret.map(|(a, b, c)| {
            Ok((
                PyBytes::new(py, a),
                PyBytes::new(py, b),
                c.into_pyobject(py)?,
            ))
        })
        .collect::<PyResult<Vec<_>>>()
    }

    fn file_ids<'a>(&self, py: Python<'a>) -> PyResult<Vec<Bound<'a, PyBytes>>> {
        self.0.file_ids().map(|x| x.into_pyobject(py)).collect()
    }
}

#[pyfunction]
fn inv_entry_to_details<'a>(
    py: Python<'a>,
    e: &'a crate::inventory::InventoryEntry,
) -> (
    Bound<'a, PyBytes>,
    Bound<'a, PyBytes>,
    u64,
    bool,
    Bound<'a, PyBytes>,
) {
    let ret = bazaar::dirstate::inv_entry_to_details(&e.0);

    (
        PyBytes::new(py, &[ret.0]),
        PyBytes::new(py, ret.1.as_slice()),
        ret.2,
        ret.3,
        PyBytes::new(py, ret.4.as_slice()),
    )
}

#[pyfunction]
fn get_output_lines(py: Python<'_>, lines: Vec<Vec<u8>>) -> Vec<Bound<'_, PyBytes>> {
    let lines = lines.iter().map(|x| x.as_slice()).collect::<Vec<&[u8]>>();
    bazaar::dirstate::get_output_lines(lines)
        .into_iter()
        .map(|x| PyBytes::new(py, x.as_slice()))
        .collect()
}

/// Helpers for the dirstate module.
pub fn _dirstate_rs(py: Python) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "dirstate")?;
    m.add_wrapped(wrap_pyfunction!(lt_by_dirs))?;
    m.add_wrapped(wrap_pyfunction!(bisect_path_left))?;
    m.add_wrapped(wrap_pyfunction!(bisect_path_right))?;
    m.add_wrapped(wrap_pyfunction!(lt_path_by_dirblock))?;
    m.add_wrapped(wrap_pyfunction!(bisect_dirblock))?;
    m.add_wrapped(wrap_pyfunction!(DefaultSHA1Provider))?;
    m.add_wrapped(wrap_pyfunction!(pack_stat))?;
    m.add_wrapped(wrap_pyfunction!(fields_per_entry))?;
    m.add_wrapped(wrap_pyfunction!(get_ghosts_line))?;
    m.add_wrapped(wrap_pyfunction!(get_parents_line))?;
    m.add_class::<IdIndex>()?;
    m.add_class::<PyDirState>()?;
    m.add_wrapped(wrap_pyfunction!(inv_entry_to_details))?;
    m.add_wrapped(wrap_pyfunction!(get_output_lines))?;

    // Register dirstate helper functions (_read_dirblocks, update_entry, ProcessEntryC)
    crate::dirstate_helpers::register(&m)?;

    Ok(m)
}
