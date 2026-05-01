#![allow(non_snake_case)]

use bazaar::FileId;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyString, PyTuple};
use pyo3::wrap_pyfunction;
use std::ffi::OsString;
#[cfg(unix)]
use std::os::unix::ffi::OsStringExt;
use std::path::{Path, PathBuf};

pyo3::import_exception!(bzrformats.errors, NotVersionedError);
pyo3::import_exception!(bzrformats.errors, BzrFormatsError);
pyo3::import_exception!(bzrformats.errors, InvalidNormalization);
pyo3::import_exception!(bzrformats.errors, BadFileKindError);
pyo3::import_exception!(bzrformats.inventory, DuplicateFileId);
pyo3::import_exception!(bzrformats.inventory, InvalidEntryName);
pyo3::import_exception!(bzrformats.dirstate, DirstateCorrupt);

/// `bazaar::dirstate::Transport` adapter backed by a Python file-like
/// object.  Used by `DirStateRs.save_to_file` so the pure-Rust
/// `DirState::save_to` flow can handle the write+fdatasync+state
/// bookkeeping while Python retains ownership of the file descriptor
/// and the OS-level lock (both managed by `bzrformats.lock`).
///
/// The adapter is *told* its lock state at construction time — it
/// does not acquire or release locks itself.  Callers should hold a
/// write lock through `bzrformats.lock.WriteLock` (or the temporary
/// upgrade dance inside `ReadLock.temporary_write_lock`) before
/// creating one with `LockState::Write`.
struct PyFileTransport {
    file: Py<PyAny>,
    lock_state: Option<bazaar::dirstate::LockState>,
}

impl PyFileTransport {
    fn new(file: Py<PyAny>, lock_state: bazaar::dirstate::LockState) -> Self {
        Self {
            file,
            lock_state: Some(lock_state),
        }
    }

    fn map_err(py: Python<'_>, err: PyErr) -> bazaar::dirstate::TransportError {
        bazaar::dirstate::TransportError::Other(err.value(py).to_string())
    }
}

impl bazaar::dirstate::Transport for PyFileTransport {
    fn exists(&self) -> Result<bool, bazaar::dirstate::TransportError> {
        // The caller already has an open fd; the file exists by
        // construction.
        Ok(true)
    }

    fn lock_read(&mut self) -> Result<(), bazaar::dirstate::TransportError> {
        if self.lock_state.is_some() {
            return Err(bazaar::dirstate::TransportError::AlreadyLocked);
        }
        self.lock_state = Some(bazaar::dirstate::LockState::Read);
        Ok(())
    }

    fn lock_write(&mut self) -> Result<(), bazaar::dirstate::TransportError> {
        if self.lock_state.is_some() {
            return Err(bazaar::dirstate::TransportError::AlreadyLocked);
        }
        self.lock_state = Some(bazaar::dirstate::LockState::Write);
        Ok(())
    }

    fn unlock(&mut self) -> Result<(), bazaar::dirstate::TransportError> {
        if self.lock_state.is_none() {
            return Err(bazaar::dirstate::TransportError::NotLocked);
        }
        self.lock_state = None;
        Ok(())
    }

    fn lock_state(&self) -> Option<bazaar::dirstate::LockState> {
        self.lock_state
    }

    fn read_all(&mut self) -> Result<Vec<u8>, bazaar::dirstate::TransportError> {
        if self.lock_state.is_none() {
            return Err(bazaar::dirstate::TransportError::NotLocked);
        }
        Python::attach(|py| -> Result<Vec<u8>, bazaar::dirstate::TransportError> {
            let f = self.file.bind(py);
            f.call_method1("seek", (0,))
                .map_err(|e| Self::map_err(py, e))?;
            let data = f.call_method0("read").map_err(|e| Self::map_err(py, e))?;
            let bytes = data.cast_into::<PyBytes>().map_err(|_| {
                bazaar::dirstate::TransportError::Other(
                    "file.read() did not return bytes".to_string(),
                )
            })?;
            Ok(bytes.as_bytes().to_vec())
        })
    }

    fn write_all(&mut self, bytes: &[u8]) -> Result<(), bazaar::dirstate::TransportError> {
        if self.lock_state != Some(bazaar::dirstate::LockState::Write) {
            return Err(bazaar::dirstate::TransportError::Other(
                "write_all requires a write lock".to_string(),
            ));
        }
        Python::attach(|py| -> Result<(), bazaar::dirstate::TransportError> {
            let f = self.file.bind(py);
            f.call_method1("seek", (0,))
                .map_err(|e| Self::map_err(py, e))?;
            let py_bytes = PyBytes::new(py, bytes);
            f.call_method1("write", (py_bytes,))
                .map_err(|e| Self::map_err(py, e))?;
            f.call_method0("truncate")
                .map_err(|e| Self::map_err(py, e))?;
            f.call_method0("flush").map_err(|e| Self::map_err(py, e))?;
            Ok(())
        })
    }

    fn fdatasync(&mut self) -> Result<(), bazaar::dirstate::TransportError> {
        Python::attach(|py| -> Result<(), bazaar::dirstate::TransportError> {
            let osutils_mod = py
                .import("bzrformats.osutils")
                .map_err(|e| Self::map_err(py, e))?;
            let fdatasync_fn = osutils_mod
                .getattr("fdatasync")
                .map_err(|e| Self::map_err(py, e))?;
            let fileno = self
                .file
                .bind(py)
                .call_method0("fileno")
                .map_err(|e| Self::map_err(py, e))?;
            fdatasync_fn
                .call1((fileno,))
                .map_err(|e| Self::map_err(py, e))?;
            Ok(())
        })
    }

    fn lstat(
        &self,
        abspath: &[u8],
    ) -> Result<bazaar::dirstate::StatInfo, bazaar::dirstate::TransportError> {
        Python::attach(
            |py| -> Result<bazaar::dirstate::StatInfo, bazaar::dirstate::TransportError> {
                let os_mod = py.import("os").map_err(|e| Self::map_err(py, e))?;
                let lstat_fn = os_mod.getattr("lstat").map_err(|e| Self::map_err(py, e))?;
                let py_bytes = PyBytes::new(py, abspath);
                let st = match lstat_fn.call1((py_bytes,)) {
                    Ok(r) => r,
                    Err(e) => {
                        if e.is_instance_of::<pyo3::exceptions::PyFileNotFoundError>(py) {
                            return Err(bazaar::dirstate::TransportError::NotFound(
                                String::from_utf8_lossy(abspath).into_owned(),
                            ));
                        }
                        return Err(Self::map_err(py, e));
                    }
                };
                let mode: u32 = st
                    .getattr("st_mode")
                    .and_then(|v| v.extract())
                    .map_err(|e| Self::map_err(py, e))?;
                let size: u64 = st
                    .getattr("st_size")
                    .and_then(|v| v.extract())
                    .map_err(|e| Self::map_err(py, e))?;
                let mtime_f: f64 = st
                    .getattr("st_mtime")
                    .and_then(|v| v.extract())
                    .map_err(|e| Self::map_err(py, e))?;
                let ctime_f: f64 = st
                    .getattr("st_ctime")
                    .and_then(|v| v.extract())
                    .map_err(|e| Self::map_err(py, e))?;
                let dev: u64 = st
                    .getattr("st_dev")
                    .and_then(|v| v.extract())
                    .map_err(|e| Self::map_err(py, e))?;
                let ino: u64 = st
                    .getattr("st_ino")
                    .and_then(|v| v.extract())
                    .map_err(|e| Self::map_err(py, e))?;
                Ok(bazaar::dirstate::StatInfo {
                    mode,
                    size,
                    mtime: mtime_f as i64,
                    ctime: ctime_f as i64,
                    dev,
                    ino,
                })
            },
        )
    }

    fn read_link(&self, abspath: &[u8]) -> Result<Vec<u8>, bazaar::dirstate::TransportError> {
        Python::attach(|py| -> Result<Vec<u8>, bazaar::dirstate::TransportError> {
            let os_mod = py.import("os").map_err(|e| Self::map_err(py, e))?;
            let readlink_fn = os_mod
                .getattr("readlink")
                .map_err(|e| Self::map_err(py, e))?;
            let py_bytes = PyBytes::new(py, abspath);
            let target = match readlink_fn.call1((py_bytes,)) {
                Ok(t) => t,
                Err(e) => {
                    if e.is_instance_of::<pyo3::exceptions::PyFileNotFoundError>(py) {
                        return Err(bazaar::dirstate::TransportError::NotFound(
                            String::from_utf8_lossy(abspath).into_owned(),
                        ));
                    }
                    return Err(Self::map_err(py, e));
                }
            };
            // os.readlink(bytes) returns bytes per the Python docs; if it
            // ever returns str, that's the caller's bug to fix.
            let bytes: Vec<u8> = target.extract().map_err(|e| Self::map_err(py, e))?;
            Ok(bytes)
        })
    }

    fn list_dir(
        &self,
        abspath: &[u8],
    ) -> Result<Vec<bazaar::dirstate::DirEntryInfo>, bazaar::dirstate::TransportError> {
        Python::attach(
            |py| -> Result<Vec<bazaar::dirstate::DirEntryInfo>, bazaar::dirstate::TransportError> {
                let os_mod = py.import("os").map_err(|e| Self::map_err(py, e))?;
                let scandir_fn = os_mod
                    .getattr("scandir")
                    .map_err(|e| Self::map_err(py, e))?;
                let abs_bytes = PyBytes::new(py, abspath);
                let iter = match scandir_fn.call1((abs_bytes,)) {
                    Ok(it) => it,
                    Err(e) => {
                        if e.is_instance_of::<pyo3::exceptions::PyFileNotFoundError>(py) {
                            return Err(bazaar::dirstate::TransportError::NotFound(
                                String::from_utf8_lossy(abspath).into_owned(),
                            ));
                        }
                        return Err(Self::map_err(py, e));
                    }
                };
                let mut out: Vec<bazaar::dirstate::DirEntryInfo> = Vec::new();
                for item in iter.try_iter().map_err(|e| Self::map_err(py, e))? {
                    let entry = item.map_err(|e| Self::map_err(py, e))?;
                    // os.scandir(bytes) yields DirEntry whose name/path are
                    // bytes; we always pass bytes above, so trust that.
                    let name_bytes: Vec<u8> = entry
                        .getattr("name")
                        .and_then(|n| n.extract())
                        .map_err(|e| Self::map_err(py, e))?;
                    let abspath_child: Vec<u8> = entry
                        .getattr("path")
                        .and_then(|p| p.extract())
                        .map_err(|e| Self::map_err(py, e))?;
                    let kwargs = pyo3::types::PyDict::new(py);
                    kwargs
                        .set_item("follow_symlinks", false)
                        .map_err(|e| Self::map_err(py, e))?;
                    let stat_obj = entry
                        .call_method("stat", (), Some(&kwargs))
                        .map_err(|e| Self::map_err(py, e))?;
                    let mode: u32 = stat_obj
                        .getattr("st_mode")
                        .and_then(|v| v.extract())
                        .map_err(|e| Self::map_err(py, e))?;
                    let size: u64 = stat_obj
                        .getattr("st_size")
                        .and_then(|v| v.extract())
                        .map_err(|e| Self::map_err(py, e))?;
                    let mtime_f: f64 = stat_obj
                        .getattr("st_mtime")
                        .and_then(|v| v.extract())
                        .map_err(|e| Self::map_err(py, e))?;
                    let ctime_f: f64 = stat_obj
                        .getattr("st_ctime")
                        .and_then(|v| v.extract())
                        .map_err(|e| Self::map_err(py, e))?;
                    let dev: u64 = stat_obj
                        .getattr("st_dev")
                        .and_then(|v| v.extract())
                        .map_err(|e| Self::map_err(py, e))?;
                    let ino: u64 = stat_obj
                        .getattr("st_ino")
                        .and_then(|v| v.extract())
                        .map_err(|e| Self::map_err(py, e))?;
                    let kind = kind_from_mode(mode);
                    out.push(bazaar::dirstate::DirEntryInfo {
                        basename: name_bytes,
                        kind,
                        stat: bazaar::dirstate::StatInfo {
                            mode,
                            size,
                            mtime: mtime_f as i64,
                            ctime: ctime_f as i64,
                            dev,
                            ino,
                        },
                        abspath: abspath_child,
                    });
                }
                Ok(out)
            },
        )
    }

    fn is_tree_reference_dir(
        &self,
        abspath: &[u8],
    ) -> Result<bool, bazaar::dirstate::TransportError> {
        // The "does this directory contain a nested tree" question is
        // a breezy-side concept: only tree-reference-supporting
        // formats answer True, and even then only when the directory
        // carries its own `.bzr/`.  The pyo3 adapter dispatches via
        // the bzrformats.osutils.isdir helper + a `.bzr` suffix —
        // good enough to match breezy's
        // `_directory_may_be_tree_reference` without pulling in a
        // separate Python callback per call.
        Python::attach(|py| -> Result<bool, bazaar::dirstate::TransportError> {
            if abspath.is_empty() {
                // Mirrors breezy's guard: the tree root is not a
                // reference even when the repository supports
                // tree references.
                return Ok(false);
            }
            let osutils_mod = py
                .import("bzrformats.osutils")
                .map_err(|e| Self::map_err(py, e))?;
            let isdir_fn = osutils_mod
                .getattr("isdir")
                .map_err(|e| Self::map_err(py, e))?;
            let mut probe = abspath.to_vec();
            probe.extend_from_slice(b"/.bzr");
            let probe_bytes = PyBytes::new(py, &probe);
            let result = isdir_fn
                .call1((probe_bytes,))
                .map_err(|e| Self::map_err(py, e))?;
            result.extract::<bool>().map_err(|e| Self::map_err(py, e))
        })
    }
}

/// Decode a minikind from the first byte of a Python-supplied
/// `bytes` object, raising `ValueError` on an empty slice or unknown
/// byte.  Used by every pyo3 entry that accepts a minikind slice from
/// Python.
fn decode_minikind(bytes: &[u8]) -> PyResult<bazaar::dirstate::Kind> {
    let byte = bytes
        .first()
        .copied()
        .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("empty minikind"))?;
    bazaar::dirstate::Kind::from_minikind(byte).map_err(|b| {
        pyo3::exceptions::PyValueError::new_err(format!("invalid minikind byte {:?}", b))
    })
}

/// Map a POSIX `st_mode` to the dirstate kind.  Matches
/// `bzrformats.osutils.file_kind_from_stat_mode`; block / char /
/// socket / fifo kinds are reported as `None` (the walker ignores
/// those rows).  Never returns `TreeReference` — that distinction
/// comes from `is_tree_reference_dir`, not the stat mode.
fn kind_from_mode(mode: u32) -> Option<osutils::Kind> {
    match mode & 0o170000 {
        0o100000 => Some(osutils::Kind::File),
        0o040000 => Some(osutils::Kind::Directory),
        0o120000 => Some(osutils::Kind::Symlink),
        _ => None,
    }
}

/// Spell out the kind name for an `st_mode`, matching breezy's
/// `_readdir_py._formats` mapping.  Used to format
/// `BadFileKindError` payloads for kinds the dirstate can't track.
fn kind_name_from_mode(mode: u32) -> &'static str {
    match mode & 0o170000 {
        0o010000 => "fifo",
        0o020000 => "chardev",
        0o040000 => "directory",
        0o060000 => "block",
        0o100000 => "file",
        0o120000 => "symlink",
        0o140000 => "socket",
        _ => "unknown",
    }
}

/// Build a `bzrformats.errors.BadFileKindError` for `path` (utf-8
/// bytes) and the raw stat mode.  Surfaces the kinds the walker
/// cannot represent (fifo, socket, …) without coupling the pure
/// crate to the Python error class.
fn bad_file_kind_error(_py: Python<'_>, path: &[u8], mode: u32) -> PyErr {
    let path_str = String::from_utf8_lossy(path).into_owned();
    let kind = kind_name_from_mode(mode);
    BadFileKindError::new_err((path_str, kind))
}

/// Translate a `BisectError` into the appropriate Python exception:
/// genuine I/O failures become `OSError`, anything else (bad minikind,
/// bad size field, too many seeks while parsing) is dirstate corruption
/// and is raised as `DirstateCorrupt(state, msg)` so callers can catch
/// a single class for "the dirstate is unreadable".
fn bisect_err_to_py(state: &Bound<PyAny>, err: bazaar::dirstate::BisectError) -> PyErr {
    match err {
        bazaar::dirstate::BisectError::ReadError(s) => pyo3::exceptions::PyOSError::new_err(s),
        other => DirstateCorrupt::new_err((state.clone().unbind(), other.to_string())),
    }
}

/// Build a `read_range(offset, len)` closure that seeks + reads on a
/// Python file-like object.  Used by the bisect entrypoints — the
/// pure-Rust bisect only needs random byte access, not the full
/// Transport contract.
fn make_read_range(
    state_file: &Bound<PyAny>,
) -> impl FnMut(u64, usize) -> Result<Vec<u8>, bazaar::dirstate::BisectError> {
    let file: Py<PyAny> = state_file.clone().unbind();
    move |offset: u64, len: usize| -> Result<Vec<u8>, bazaar::dirstate::BisectError> {
        Python::attach(|py| {
            let f = file.bind(py);
            f.call_method1("seek", (offset,))
                .map_err(|e| bazaar::dirstate::BisectError::ReadError(e.to_string()))?;
            let data = f
                .call_method1("read", (len,))
                .map_err(|e| bazaar::dirstate::BisectError::ReadError(e.to_string()))?;
            let bytes = data.cast_into::<PyBytes>().map_err(|_| {
                bazaar::dirstate::BisectError::ReadError(
                    "state_file.read() did not return bytes".to_string(),
                )
            })?;
            Ok(bytes.as_bytes().to_vec())
        })
    }
}

/// Convert the `bisect` / `bisect_dirblocks` result into a Python
/// dict: `{path_bytes: [entry_tuple, ...]}` where `entry_tuple` has
/// the same shape as `DirStateRs.dirblocks` entries.
fn bisect_result_to_pydict<'py>(
    py: Python<'py>,
    found: &std::collections::HashMap<Vec<u8>, Vec<bazaar::dirstate::Entry>>,
) -> PyResult<Bound<'py, PyDict>> {
    let out = PyDict::new(py);
    for (key, entries) in found {
        let mut py_entries: Vec<Bound<PyAny>> = Vec::with_capacity(entries.len());
        for entry in entries {
            let key_tuple = PyTuple::new(
                py,
                [
                    PyBytes::new(py, &entry.key.dirname).into_any(),
                    PyBytes::new(py, &entry.key.basename).into_any(),
                    PyBytes::new(py, &entry.key.file_id).into_any(),
                ],
            )?;
            let mut tree_list: Vec<Bound<PyAny>> = Vec::with_capacity(entry.trees.len());
            for t in &entry.trees {
                let tup = PyTuple::new(
                    py,
                    [
                        PyBytes::new(py, &[t.minikind.to_minikind()]).into_any(),
                        PyBytes::new(py, &t.fingerprint).into_any(),
                        t.size.into_pyobject(py)?.into_any(),
                        pyo3::types::PyBool::new(py, t.executable)
                            .to_owned()
                            .into_any(),
                        PyBytes::new(py, &t.packed_stat).into_any(),
                    ],
                )?;
                tree_list.push(tup.into_any());
            }
            let entry_tuple = PyTuple::new(
                py,
                [key_tuple.into_any(), PyList::new(py, tree_list)?.into_any()],
            )?;
            py_entries.push(entry_tuple.into_any());
        }
        out.set_item(PyBytes::new(py, key), PyList::new(py, py_entries)?)?;
    }
    Ok(out)
}

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
        .map(extract_path)
        .collect::<PyResult<Vec<PathBuf>>>()?;
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
        .map(extract_path)
        .collect::<PyResult<Vec<PathBuf>>>()?;
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
        let dirblock = dirblocks.get_item(mid)?.cast_into::<PyTuple>()?;
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

/// Lightweight `os.stat_result`-shaped pyclass exposing exactly the
/// six fields dirstate consumes.
#[pyclass]
struct StatResult {
    info: bazaar::dirstate::StatInfo,
}

#[pymethods]
impl StatResult {
    #[getter]
    fn st_size(&self) -> u64 {
        self.info.size
    }

    #[getter]
    fn st_mtime(&self) -> i64 {
        self.info.mtime
    }

    #[getter]
    fn st_ctime(&self) -> i64 {
        self.info.ctime
    }

    #[getter]
    fn st_mode(&self) -> u32 {
        self.info.mode
    }

    #[getter]
    fn st_dev(&self) -> u64 {
        self.info.dev
    }

    #[getter]
    fn st_ino(&self) -> u64 {
        self.info.ino
    }
}

#[pyclass]
struct SHA1Provider {
    provider: Box<dyn bazaar::dirstate::SHA1Provider + Send + Sync>,
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
        let (info, sha1) = self.provider.stat_and_sha1(&path)?;
        let pmd = StatResult { info };
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

/// Adapter that lets a Python `SHA1Provider`-shaped object (anything
/// with a `sha1(abspath)` method returning bytes) be plugged into the
/// pure-Rust `DirState`. The provider is held as a `Py<PyAny>` so we
/// can call back into Python; the GIL is acquired on each call.
struct PyCallbackSha1Provider {
    obj: Py<PyAny>,
}

impl bazaar::dirstate::SHA1Provider for PyCallbackSha1Provider {
    fn sha1(&self, path: &std::path::Path) -> std::io::Result<String> {
        Python::attach(|py| {
            let path_obj = path_to_py(py, path)
                .map_err(|e| std::io::Error::other(format!("path_to_py: {}", e)))?;
            let result = self
                .obj
                .bind(py)
                .call_method1("sha1", (path_obj,))
                .map_err(|e| std::io::Error::other(format!("sha1 callback: {}", e)))?;
            let bytes: &[u8] = result
                .extract()
                .map_err(|e| std::io::Error::other(format!("sha1 result: {}", e)))?;
            std::str::from_utf8(bytes)
                .map(|s| s.to_string())
                .map_err(|e| std::io::Error::other(format!("sha1 utf8: {}", e)))
        })
    }

    fn stat_and_sha1(
        &self,
        path: &std::path::Path,
    ) -> std::io::Result<(bazaar::dirstate::StatInfo, String)> {
        Python::attach(|py| {
            let path_obj = path_to_py(py, path)
                .map_err(|e| std::io::Error::other(format!("path_to_py: {}", e)))?;
            let result = self
                .obj
                .bind(py)
                .call_method1("stat_and_sha1", (path_obj,))
                .map_err(|e| std::io::Error::other(format!("stat_and_sha1 callback: {}", e)))?;
            let (stat_obj, sha_obj): (Bound<'_, PyAny>, Bound<'_, PyAny>) = result
                .extract()
                .map_err(|e| std::io::Error::other(format!("stat_and_sha1 result: {}", e)))?;
            let info = stat_result_to_info(&stat_obj)
                .map_err(|e| std::io::Error::other(format!("stat_and_sha1 stat_result: {}", e)))?;
            let sha_bytes: &[u8] = sha_obj
                .extract()
                .map_err(|e| std::io::Error::other(format!("stat_and_sha1 sha bytes: {}", e)))?;
            let sha = std::str::from_utf8(sha_bytes)
                .map_err(|e| std::io::Error::other(format!("stat_and_sha1 sha utf8: {}", e)))?
                .to_string();
            Ok((info, sha))
        })
    }
}

/// Read the `st_*` attributes off a Python `os.stat_result`-shaped
/// object and pack them into a [`StatInfo`].  Used by callback
/// adapters that bridge Python `SHA1Provider`s into the Rust trait.
fn stat_result_to_info(obj: &Bound<'_, PyAny>) -> PyResult<bazaar::dirstate::StatInfo> {
    Ok(bazaar::dirstate::StatInfo {
        mode: obj.getattr("st_mode")?.extract()?,
        size: obj.getattr("st_size")?.extract()?,
        mtime: obj.getattr("st_mtime")?.extract::<f64>()? as i64,
        ctime: obj.getattr("st_ctime")?.extract::<f64>()? as i64,
        dev: obj.getattr("st_dev")?.extract()?,
        ino: obj.getattr("st_ino")?.extract()?,
    })
}

/// Build the `Box<dyn SHA1Provider>` to hand to `DirState::new`.
/// Recognises the pyo3 `SHA1Provider` pyclass (uses its inner Rust
/// provider directly) and otherwise wraps the Python object in
/// `PyCallbackSha1Provider`.
fn sha1_provider_from_py(
    py: Python<'_>,
    obj: &Bound<PyAny>,
) -> Box<dyn bazaar::dirstate::SHA1Provider + Send + Sync> {
    let _ = py;
    Box::new(PyCallbackSha1Provider {
        obj: obj.clone().unbind(),
    })
}

/// Convert a `&Path` to a Python object suitable for passing to
/// `SHA1Provider.sha1`. On Unix, hand back raw bytes so non-utf8
/// paths survive; on other platforms fall back to the path string.
fn path_to_py<'py>(py: Python<'py>, path: &Path) -> PyResult<Bound<'py, PyAny>> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let bytes = path.as_os_str().as_bytes();
        Ok(PyBytes::new(py, bytes).into_any())
    }
    #[cfg(not(unix))]
    {
        let s = path.to_string_lossy();
        Ok(PyString::new(py, &s).into_any())
    }
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
                PyBytes::new(py, &[tree.minikind.to_minikind()]).into_any(),
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

/// Collect a Python `set[bytes]` (or any iterable of bytes) into a
/// `HashSet<Vec<u8>>`.
fn collect_bytes_set(obj: &Bound<PyAny>) -> PyResult<std::collections::HashSet<Vec<u8>>> {
    let mut out = std::collections::HashSet::new();
    for item in obj.try_iter()? {
        out.insert(item?.extract::<Vec<u8>>()?);
    }
    Ok(out)
}

/// Collect a Python `dict[bytes, bytes]` into a `HashMap<Vec<u8>, Vec<u8>>`.
fn collect_bytes_map(d: &Bound<PyDict>) -> PyResult<std::collections::HashMap<Vec<u8>, Vec<u8>>> {
    let mut out = std::collections::HashMap::new();
    for (k, v) in d.iter() {
        out.insert(k.extract::<Vec<u8>>()?, v.extract::<Vec<u8>>()?);
    }
    Ok(out)
}

/// Decode a Python `[dirname_or_none, file_id_or_none]` list into the
/// `Option<(Vec<u8>, Option<Vec<u8>>)>` shape
/// [`bazaar::dirstate::ProcessEntryState::last_source_parent`] uses.
fn decode_last_parent(lst: &Bound<PyList>) -> PyResult<Option<(Vec<u8>, Option<Vec<u8>>)>> {
    if lst.len() < 2 {
        return Ok(None);
    }
    let d = lst.get_item(0)?;
    if d.is_none() {
        return Ok(None);
    }
    let dirname: Vec<u8> = d.extract()?;
    let f = lst.get_item(1)?;
    let file_id: Option<Vec<u8>> = if f.is_none() {
        None
    } else {
        Some(f.extract()?)
    };
    Ok(Some((dirname, file_id)))
}

/// Replace the contents of `target` with `source` — used when the
/// pure-crate process_entry added new entries to its `search_specific_files`
/// set that Python's ProcessEntryPython.search_specific_files needs to
/// see.
fn write_back_bytes_set(
    target: &Bound<PyAny>,
    source: &std::collections::HashSet<Vec<u8>>,
) -> PyResult<()> {
    target.call_method0("clear")?;
    for item in source {
        target.call_method1("add", (PyBytes::new(target.py(), item),))?;
    }
    Ok(())
}

fn write_back_bytes_map(
    target: &Bound<PyDict>,
    source: &std::collections::HashMap<Vec<u8>, Vec<u8>>,
) -> PyResult<()> {
    target.clear();
    let py = target.py();
    for (k, v) in source {
        target.set_item(PyBytes::new(py, k), PyBytes::new(py, v))?;
    }
    Ok(())
}

fn write_back_last_parent(
    target: &Bound<PyList>,
    source: &Option<(Vec<u8>, Option<Vec<u8>>)>,
) -> PyResult<()> {
    let py = target.py();
    while target.len() < 2 {
        target.append(py.None())?;
    }
    match source {
        Some((dn, fid)) => {
            target.set_item(0, PyBytes::new(py, dn))?;
            target.set_item(
                1,
                match fid {
                    Some(b) => PyBytes::new(py, b).into_any(),
                    None => py.None().into_bound(py),
                },
            )?;
        }
        None => {
            target.set_item(0, py.None())?;
            target.set_item(1, py.None())?;
        }
    }
    Ok(())
}

/// Convert a Rust [`bazaar::dirstate::DirstateChange`] into the 9-tuple
/// Python's `DirstateInventoryChange` constructor accepts, with path
/// fields utf8-decoded using `surrogateescape`.
fn dirstate_change_to_pytuple<'py>(
    py: Python<'py>,
    change: &bazaar::dirstate::DirstateChange,
) -> PyResult<Bound<'py, PyTuple>> {
    fn decode_bytes<'py>(py: Python<'py>, b: &Option<Vec<u8>>) -> PyResult<Py<PyAny>> {
        match b {
            None => Ok(py.None()),
            Some(v) => {
                // utf8 decode with surrogateescape, matching
                // self.utf8_decode(..., "surrogateescape") in Python.
                let py_bytes = PyBytes::new(py, v);
                let s = py_bytes
                    .call_method1("decode", ("utf-8", "surrogateescape"))?
                    .unbind();
                Ok(s)
            }
        }
    }

    let path_tuple = PyTuple::new(
        py,
        [
            decode_bytes(py, &change.old_path)?,
            decode_bytes(py, &change.new_path)?,
        ],
    )?;
    let versioned_tuple = PyTuple::new(
        py,
        [
            pyo3::types::PyBool::new(py, change.old_versioned)
                .to_owned()
                .into_any(),
            pyo3::types::PyBool::new(py, change.new_versioned)
                .to_owned()
                .into_any(),
        ],
    )?;
    let parent_tuple = PyTuple::new(
        py,
        [
            match &change.source_parent_id {
                Some(v) => PyBytes::new(py, v).into_any().unbind(),
                None => py.None(),
            },
            match &change.target_parent_id {
                Some(v) => PyBytes::new(py, v).into_any().unbind(),
                None => py.None(),
            },
        ],
    )?;
    let name_tuple = PyTuple::new(
        py,
        [
            decode_bytes(py, &change.old_basename)?,
            decode_bytes(py, &change.new_basename)?,
        ],
    )?;
    let kind_tuple = PyTuple::new(
        py,
        [
            match change.source_kind {
                Some(k) => PyString::new(py, k.as_str()).into_any().unbind(),
                None => py.None(),
            },
            match change.target_kind {
                Some(k) => PyString::new(py, k.as_str()).into_any().unbind(),
                None => py.None(),
            },
        ],
    )?;
    let exec_tuple = PyTuple::new(
        py,
        [
            match change.source_exec {
                Some(b) => pyo3::types::PyBool::new(py, b)
                    .to_owned()
                    .into_any()
                    .unbind(),
                None => py.None(),
            },
            match change.target_exec {
                Some(b) => pyo3::types::PyBool::new(py, b)
                    .to_owned()
                    .into_any()
                    .unbind(),
                None => py.None(),
            },
        ],
    )?;
    // Python expects file_id=None for unversioned entries.  The Rust
    // DirstateChange currently stores file_id as Vec<u8>, with an
    // empty vec sentinel meaning "unversioned"; surface that as None
    // here so the resulting InventoryTreeChange compares equal to
    // what InterInventoryTree.iter_changes produces.
    let file_id_obj = if change.file_id.is_empty()
        && !change.old_versioned
        && !change.new_versioned
    {
        py.None()
    } else {
        PyBytes::new(py, &change.file_id).into_any().unbind()
    };
    PyTuple::new(
        py,
        [
            file_id_obj,
            path_tuple.into_any().unbind(),
            pyo3::types::PyBool::new(py, change.content_change)
                .to_owned()
                .into_any()
                .unbind(),
            versioned_tuple.into_any().unbind(),
            parent_tuple.into_any().unbind(),
            name_tuple.into_any().unbind(),
            kind_tuple.into_any().unbind(),
            exec_tuple.into_any().unbind(),
        ],
    )
}

/// Convert an [`AddError`] to the Python exception that
/// `DirState.add` would have raised.
fn add_error_to_py(py: Python<'_>, err: bazaar::dirstate::AddError) -> PyErr {
    use bazaar::dirstate::AddError;
    match err {
        AddError::DuplicateFileId { file_id, info } => {
            DuplicateFileId::new_err((PyBytes::new(py, &file_id).unbind(), info))
        }
        AddError::AlreadyAdded { path } => {
            pyo3::exceptions::PyException::new_err(format!("adding already added path! {:?}", path))
        }
        AddError::NotVersioned { path } => {
            NotVersionedError::new_err((PyBytes::new(py, &path).unbind(), ""))
        }
        AddError::AlreadyAddedAssertion { basename, file_id } => {
            pyo3::exceptions::PyAssertionError::new_err(format!(
                " {:?}({:?}) already added",
                basename, file_id
            ))
        }
        AddError::Internal { reason } => pyo3::exceptions::PyAssertionError::new_err(reason),
        AddError::InvalidNormalization { path } => InvalidNormalization::new_err((path,)),
        AddError::InvalidEntryName { name } => InvalidEntryName::new_err((name,)),
    }
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
#[pyclass(name = "DirStateRs")]
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
        py: Python<'_>,
        path: &Bound<PyAny>,
        sha1_provider: Option<&Bound<PyAny>>,
        worth_saving_limit: i64,
        use_filesystem_for_exec: bool,
        fdatasync: bool,
    ) -> PyResult<Self> {
        let path = extract_path(path)?;
        let provider: Box<dyn bazaar::dirstate::SHA1Provider + Send + Sync> =
            match sha1_provider {
                Some(obj) => sha1_provider_from_py(py, obj),
                None => Box::new(bazaar::dirstate::DefaultSHA1Provider::new()),
            };
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
    /// Python's `DirState._filename` attribute, which is the ``str``
    /// path returned by ``Transport.local_abspath`` (always str, on
    /// every platform).
    #[getter]
    fn filename<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::types::PyString>> {
        let s = self
            .inner
            .filename
            .to_str()
            .ok_or_else(|| PyTypeError::new_err("dirstate filename is not valid utf-8"))?;
        Ok(pyo3::types::PyString::new(py, s))
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

    /// Compute, cache, and return the SHA cutoff time (`now - 3`).
    /// Mirrors Python's `DirState._sha_cutoff_time`.
    fn compute_sha_cutoff_time(&mut self) -> i64 {
        self.inner.compute_sha_cutoff_time()
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

    /// Persist the in-memory state to the given Python file-like
    /// object, assuming the caller already holds a write lock.
    /// Mirrors the `try` block inside Python's `DirState.save`:
    /// serialises get_lines, seeks to 0, writes, truncates, flushes,
    /// optionally fdatasyncs, and marks the dirstate unmodified.
    /// Returns `True` if the state was actually written, `False` if
    /// an early-return gate (`changes_aborted` / not-worth-saving)
    /// prevented it.
    fn save_to_file(&mut self, state_file: &Bound<PyAny>) -> PyResult<bool> {
        let mut transport = PyFileTransport::new(
            state_file.clone().unbind(),
            bazaar::dirstate::LockState::Write,
        );
        self.inner.save_to(&mut transport).map_err(|e| {
            pyo3::exceptions::PyIOError::new_err(format!("dirstate save failed: {:?}", e))
        })
    }

    /// Record the observed sha1 for the entry at `key` and return the
    /// updated tree-0 5-tuple (or `None` if no update was recorded —
    /// non-regular file, or uncacheable mtime/ctime).  Mirrors
    /// Python's `DirState._observed_sha1` including the write-back
    /// the Python side used to do with a second `get_entry` call.
    #[pyo3(signature = (key, sha1, st_mode, st_size, st_mtime, st_ctime, st_dev, st_ino))]
    fn observed_sha1<'py>(
        &mut self,
        py: Python<'py>,
        key: &Bound<PyTuple>,
        sha1: &[u8],
        st_mode: u32,
        st_size: u64,
        st_mtime: f64,
        st_ctime: f64,
        st_dev: u64,
        st_ino: u64,
    ) -> PyResult<Option<Bound<'py, PyTuple>>> {
        let entry_key = bazaar::dirstate::EntryKey {
            dirname: key.get_item(0)?.extract()?,
            basename: key.get_item(1)?.extract()?,
            file_id: key.get_item(2)?.extract()?,
        };
        let updated = self
            .inner
            .observed_sha1(
                &entry_key,
                sha1,
                st_mode,
                st_size,
                st_mtime as i64,
                st_ctime as i64,
                st_dev,
                st_ino,
            )
            .map_err(|e| match e {
                bazaar::dirstate::UpdateEntryError::EntryNotFound => {
                    pyo3::exceptions::PyKeyError::new_err("observed_sha1: entry not found")
                }
                bazaar::dirstate::UpdateEntryError::Io(io) => {
                    pyo3::exceptions::PyOSError::new_err(io.to_string())
                }
                other => pyo3::exceptions::PyRuntimeError::new_err(other.to_string()),
            })?;
        match updated {
            None => Ok(None),
            Some(td) => Ok(Some(PyTuple::new(
                py,
                [
                    PyBytes::new(py, &[td.minikind.to_minikind()]).into_any(),
                    PyBytes::new(py, &td.fingerprint).into_any(),
                    td.size.into_pyobject(py)?.into_any(),
                    pyo3::types::PyBool::new(py, td.executable)
                        .to_owned()
                        .into_any(),
                    PyBytes::new(py, &td.packed_stat).into_any(),
                ],
            )?)),
        }
    }

    /// Refresh the tree-0 slot of the entry at `key` from the
    /// filesystem.  Mirrors Python's `py_update_entry`.
    /// `abspath` is the absolute path of the file on disk.
    /// `stat_value` is a Python `os.stat_result` (usually produced by
    /// `os.lstat`).  Returns the sha1 hex or symlink target as
    /// `bytes`, or `None` when the cache matches / the on-disk kind
    /// is unsupported / the stat falls in the uncacheable window.
    fn update_entry<'py>(
        &mut self,
        py: Python<'py>,
        key: &Bound<PyTuple>,
        abspath: &Bound<PyAny>,
        stat_value: &Bound<PyAny>,
    ) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let entry_key = bazaar::dirstate::EntryKey {
            dirname: key.get_item(0)?.extract()?,
            basename: key.get_item(1)?.extract()?,
            file_id: key.get_item(2)?.extract()?,
        };
        let abspath_bytes: Vec<u8> = abspath.extract()?;
        // Unpack stat_value into a StatInfo — Python already did the
        // lstat, no need to double-stat.
        let stat = bazaar::dirstate::StatInfo {
            mode: stat_value.getattr("st_mode")?.extract()?,
            size: stat_value.getattr("st_size")?.extract()?,
            mtime: stat_value.getattr("st_mtime")?.extract::<f64>()? as i64,
            ctime: stat_value.getattr("st_ctime")?.extract::<f64>()? as i64,
            dev: stat_value.getattr("st_dev")?.extract()?,
            ino: stat_value.getattr("st_ino")?.extract()?,
        };
        // Transport for read_link / lstat; `update_entry` only uses
        // read_link (symlink case), but we still need the trait object.
        // A fresh PyFileTransport over a placeholder file is wrong —
        // PyFileTransport's read_all/write_all/fdatasync are tied to
        // the dirstate file.  The pure-crate contract is that lstat
        // and read_link take arbitrary paths, which PyFileTransport
        // implements via os.lstat / os.readlink directly.
        let transport = PyFileTransport::new(
            pyo3::types::PyNone::get(py).to_owned().into_any().unbind(),
            bazaar::dirstate::LockState::Read,
        );
        let result = self
            .inner
            .update_entry(&entry_key, &abspath_bytes, &stat, &transport)
            .map_err(|e| match e {
                bazaar::dirstate::UpdateEntryError::EntryNotFound => {
                    pyo3::exceptions::PyKeyError::new_err("update_entry: entry not found")
                }
                bazaar::dirstate::UpdateEntryError::Io(io) => {
                    pyo3::exceptions::PyOSError::new_err(io.to_string())
                }
                other => pyo3::exceptions::PyRuntimeError::new_err(other.to_string()),
            })?;
        Ok(result.map(|v| PyBytes::new(py, &v)))
    }

    /// Compare one dirstate entry against what's on disk (or against
    /// "absent on disk" when `path_info` is None).  Mirrors Python's
    /// `ProcessEntryPython._process_entry`.
    ///
    /// Returns `(change_tuple | None, changed_or_None)`.  The change
    /// tuple is the 9-field record Python's `DirstateInventoryChange`
    /// constructor takes (with utf8 path fields already decoded on the
    /// Rust side using surrogateescape).
    ///
    /// Caller-owned state dict/sets (passed in and mutated in place):
    /// `searched_specific_files`, `search_specific_files`,
    /// `old_dirname_to_file_id`, `new_dirname_to_file_id`,
    /// `last_source_parent`, `last_target_parent`.
    #[pyo3(signature = (
        entry,
        path_info,
        source_index,
        target_index,
        include_unchanged,
        searched_specific_files,
        search_specific_files,
        old_dirname_to_file_id,
        new_dirname_to_file_id,
        last_source_parent,
        last_target_parent,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn process_entry<'py>(
        &mut self,
        py: Python<'py>,
        entry: &Bound<PyAny>,
        path_info: Option<&Bound<PyAny>>,
        source_index: Option<usize>,
        target_index: usize,
        include_unchanged: bool,
        searched_specific_files: &Bound<PyAny>,
        search_specific_files: &Bound<PyAny>,
        old_dirname_to_file_id: &Bound<PyDict>,
        new_dirname_to_file_id: &Bound<PyDict>,
        last_source_parent: &Bound<PyList>,
        last_target_parent: &Bound<PyList>,
    ) -> PyResult<(Option<Bound<'py, PyTuple>>, Option<bool>)> {
        // Decode the entry tuple: ((dirname, basename, file_id),
        // [tree_tuple, ...]).
        let entry_tup = entry.cast::<PyTuple>()?;
        let key_tup = entry_tup.get_item(0)?.cast_into::<PyTuple>()?;
        let entry_key = bazaar::dirstate::EntryKey {
            dirname: key_tup.get_item(0)?.extract()?,
            basename: key_tup.get_item(1)?.extract()?,
            file_id: key_tup.get_item(2)?.extract()?,
        };
        let trees_any = entry_tup.get_item(1)?;
        let mut entry_trees: Vec<bazaar::dirstate::TreeData> = Vec::new();
        for t in trees_any.try_iter()? {
            let tt = t?.cast_into::<PyTuple>()?;
            let mk_bytes: Vec<u8> = tt.get_item(0)?.extract()?;
            let minikind = decode_minikind(&mk_bytes)?;
            entry_trees.push(bazaar::dirstate::TreeData {
                minikind,
                fingerprint: tt.get_item(1)?.extract()?,
                size: tt.get_item(2)?.extract()?,
                executable: tt.get_item(3)?.extract()?,
                packed_stat: tt.get_item(4)?.extract()?,
            });
        }

        // Decode path_info. The 5-tuple shape Python uses is
        // (top_relpath, basename, kind, stat, abspath).
        let path_info_rs: Option<bazaar::dirstate::ProcessPathInfo> = if let Some(pi) = path_info {
            if pi.is_none() {
                None
            } else {
                let pt = pi.cast::<PyTuple>()?;
                let kind_obj = pt.get_item(2)?;
                let kind: Option<osutils::Kind> = if kind_obj.is_none() {
                    None
                } else {
                    Some(kind_obj.extract::<osutils::Kind>()?)
                };
                let stat_obj = pt.get_item(3)?;
                let abspath: Vec<u8> = pt.get_item(4)?.extract()?;
                let stat = bazaar::dirstate::StatInfo {
                    mode: stat_obj.getattr("st_mode")?.extract()?,
                    size: stat_obj.getattr("st_size")?.extract()?,
                    mtime: stat_obj.getattr("st_mtime")?.extract::<f64>()? as i64,
                    ctime: stat_obj.getattr("st_ctime")?.extract::<f64>()? as i64,
                    dev: stat_obj.getattr("st_dev")?.extract()?,
                    ino: stat_obj.getattr("st_ino")?.extract()?,
                };
                Some(bazaar::dirstate::ProcessPathInfo {
                    abspath,
                    kind,
                    stat,
                })
            }
        } else {
            None
        };

        // Build state from the Python-owned containers.  iter_changes
        // uses a richer ProcessEntryState; for process_entry (which
        // is called per-entry by Python's ProcessEntryPython loop)
        // the walk-only fields are unused.
        let mut pstate = bazaar::dirstate::ProcessEntryState {
            source_index,
            target_index,
            include_unchanged,
            want_unversioned: false,
            partial: false,
            supports_tree_reference: false,
            root_abspath: Vec::new(),
            searched_specific_files: collect_bytes_set(searched_specific_files)?,
            search_specific_files: collect_bytes_set(search_specific_files)?,
            search_specific_file_parents: std::collections::HashSet::new(),
            searched_exact_paths: std::collections::HashSet::new(),
            seen_ids: std::collections::HashSet::new(),
            new_dirname_to_file_id: collect_bytes_map(new_dirname_to_file_id)?,
            old_dirname_to_file_id: collect_bytes_map(old_dirname_to_file_id)?,
            last_source_parent: decode_last_parent(last_source_parent)?,
            last_target_parent: decode_last_parent(last_target_parent)?,
        };

        // Transport: PyFileTransport is the only implementor we have
        // on the pyo3 side, and process_entry only calls lstat /
        // read_link on it (both go through os.* directly rather than
        // the underlying file handle).  A dummy PyNone handle is
        // therefore safe here.
        let transport = PyFileTransport::new(
            pyo3::types::PyNone::get(py).to_owned().into_any().unbind(),
            bazaar::dirstate::LockState::Read,
        );

        let dirstate_path = self.inner.filename.to_string_lossy().into_owned();
        let (change, changed) = self
            .inner
            .process_entry(
                &mut pstate,
                &entry_key,
                &entry_trees,
                path_info_rs.as_ref(),
                &transport,
            )
            .map_err(|e| match e {
                bazaar::dirstate::ProcessEntryError::DirstateCorrupt(msg) => {
                    DirstateCorrupt::new_err((dirstate_path, msg))
                }
                bazaar::dirstate::ProcessEntryError::BadFileKind { path, mode } => {
                    bad_file_kind_error(py, &path, mode)
                }
                bazaar::dirstate::ProcessEntryError::Internal(msg) => {
                    pyo3::exceptions::PyAssertionError::new_err(msg)
                }
            })?;

        // Write back mutable state to the Python containers.
        write_back_bytes_set(search_specific_files, &pstate.search_specific_files)?;
        write_back_bytes_map(old_dirname_to_file_id, &pstate.old_dirname_to_file_id)?;
        write_back_bytes_map(new_dirname_to_file_id, &pstate.new_dirname_to_file_id)?;
        write_back_last_parent(last_source_parent, &pstate.last_source_parent)?;
        write_back_last_parent(last_target_parent, &pstate.last_target_parent)?;

        let change_tuple = change
            .map(|c| dirstate_change_to_pytuple(py, &c))
            .transpose()?;
        Ok((change_tuple, changed))
    }

    /// Read the dirstate header out of `state_file` and populate the
    /// header state (parents, ghosts, num_entries, end_of_header).
    /// Mirrors `DirState._read_header`: reads five newline-delimited
    /// lines from the current file position and leaves the file
    /// pointer immediately after the fifth newline — the position
    /// where the first dirblock record begins.  The caller must hold
    /// a read or write lock and must have positioned the file at the
    /// start of the header.  `state` is forwarded into the
    /// `DirstateCorrupt(state, msg)` exception so callers can inspect
    /// which dirstate failed to parse.
    fn read_header_from_file(
        &mut self,
        state: &Bound<PyAny>,
        state_file: &Bound<PyAny>,
    ) -> PyResult<()> {
        let mut data: Vec<u8> = Vec::new();
        for _ in 0..5 {
            let line = state_file.call_method0("readline")?;
            let bytes = line.cast_into::<PyBytes>()?;
            data.extend_from_slice(bytes.as_bytes());
        }
        self.inner
            .read_header(&data)
            .map_err(|e| DirstateCorrupt::new_err((state.clone().unbind(), e.to_string())))
    }

    /// Bisect for rows at the given paths. Mirrors Python's
    /// `DirState._bisect`. `paths` is an iterable of `bytes`; returns
    /// a `dict` mapping path → list of entries (same tuple shape as
    /// `DirStateRs.dirblocks` entries). Requires the header to have
    /// been read and the caller to hold a read (or write) lock on
    /// `state_file`.
    fn bisect<'py>(
        &self,
        py: Python<'py>,
        state: &Bound<PyAny>,
        state_file: &Bound<PyAny>,
        file_size: u64,
        paths: &Bound<PyAny>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let rust_paths = collect_bytes_vec(paths)?;
        let read_range = make_read_range(state_file);
        let found = self
            .inner
            .bisect(rust_paths, file_size, read_range)
            .map_err(|e| bisect_err_to_py(state, e))?;
        bisect_result_to_pydict(py, &found)
    }

    /// Bisect for all entries whose dirname is in `dir_list`.
    /// Mirrors Python's `DirState._bisect_dirblocks`.
    fn bisect_dirblocks<'py>(
        &self,
        py: Python<'py>,
        state: &Bound<PyAny>,
        state_file: &Bound<PyAny>,
        file_size: u64,
        dir_list: &Bound<PyAny>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let rust_dirs = collect_bytes_vec(dir_list)?;
        let read_range = make_read_range(state_file);
        let found = self
            .inner
            .bisect_dirblocks(rust_dirs, file_size, read_range)
            .map_err(|e| bisect_err_to_py(state, e))?;
        bisect_result_to_pydict(py, &found)
    }

    /// Recursive bisect. Mirrors Python's `DirState._bisect_recursive`.
    /// `paths` is an iterable of `bytes` paths; returns a `dict`
    /// mapping `(dirname, basename, file_id)` to a list of
    /// tree-data 5-tuples.
    fn bisect_recursive<'py>(
        &self,
        py: Python<'py>,
        state: &Bound<PyAny>,
        state_file: &Bound<PyAny>,
        file_size: u64,
        paths: &Bound<PyAny>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let rust_paths = collect_bytes_vec(paths)?;
        let read_range = make_read_range(state_file);
        let found = self
            .inner
            .bisect_recursive(rust_paths, file_size, read_range)
            .map_err(|e| bisect_err_to_py(state, e))?;
        let out = PyDict::new(py);
        for ((dn, bn, fid), trees) in &found {
            let key = PyTuple::new(
                py,
                [
                    PyBytes::new(py, dn).into_any(),
                    PyBytes::new(py, bn).into_any(),
                    PyBytes::new(py, fid).into_any(),
                ],
            )?;
            let tree_items: Vec<Bound<PyAny>> = trees
                .iter()
                .map(|t| {
                    PyTuple::new(
                        py,
                        [
                            PyBytes::new(py, &[t.minikind.to_minikind()]).into_any(),
                            PyBytes::new(py, &t.fingerprint).into_any(),
                            t.size.into_pyobject(py)?.into_any(),
                            pyo3::types::PyBool::new(py, t.executable)
                                .to_owned()
                                .into_any(),
                            PyBytes::new(py, &t.packed_stat).into_any(),
                        ],
                    )
                    .map(|tup| tup.into_any())
                })
                .collect::<PyResult<Vec<_>>>()?;
            out.set_item(key, PyList::new(py, tree_items)?)?;
        }
        Ok(out)
    }

    /// Forget all in-memory state. Mirrors `DirState._wipe_state`.
    fn wipe_state(&mut self) {
        self.inner.wipe_state();
    }

    /// Parse the 5-line dirstate header out of `data`. Mirrors
    /// Python's `DirState._read_header`: populates parents, ghosts,
    /// num_entries, end_of_header, and marks the header as in-memory
    /// unmodified. `data` must be exactly the bytes of the five
    /// header lines as returned by sequential `readline()` calls on
    /// the state file — the resulting `end_of_header` equals
    /// `len(data)` so it matches `state_file.tell()` on the caller
    /// side.
    fn read_header(&mut self, data: &[u8]) -> PyResult<()> {
        self.inner
            .read_header(data)
            .map_err(|e| BzrFormatsError::new_err(e.to_string()))
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

    /// Overwrite the tree-0 slot of `key`'s entry with the provided
    /// details, without touching id_index, cross-references, or
    /// dirblock_state. Mirrors Python's old in-place
    /// `entry[1][0] = (...)` mutation used by `update_entry`'s
    /// hash-refresh path.
    #[pyo3(signature = (key, minikind, fingerprint, size, executable, packed_stat))]
    fn set_tree0(
        &mut self,
        key: &Bound<PyTuple>,
        minikind: &[u8],
        fingerprint: &[u8],
        size: u64,
        executable: bool,
        packed_stat: &[u8],
    ) -> PyResult<()> {
        let entry_key = bazaar::dirstate::EntryKey {
            dirname: key.get_item(0)?.extract()?,
            basename: key.get_item(1)?.extract()?,
            file_id: key.get_item(2)?.extract()?,
        };
        let details = bazaar::dirstate::TreeData {
            minikind: decode_minikind(minikind)?,
            fingerprint: fingerprint.to_vec(),
            size,
            executable,
            packed_stat: packed_stat.to_vec(),
        };
        self.inner
            .set_tree0(&entry_key, details)
            .map_err(|e| pyo3::exceptions::PyAssertionError::new_err(e.to_string()))
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
                minikind: decode_minikind(&minikind_bytes)?,
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

    /// Look up a dirstate entry by path and/or file_id in
    /// `tree_index`. Mirrors Python's `DirState._get_entry` —
    /// including the `(None, None)` sentinel returned on a miss.
    /// On hit, the return is the same entry-tuple shape as
    /// `DirStateRs.dirblocks` entries.
    ///
    /// `include_deleted` controls whether the file_id branch
    /// returns absent entries (`b'a'`) as-is or hides them.
    ///
    /// Raises `BzrFormatsError` for the "unversioned entry?" and
    /// "mismatching tree_index/file_id/path" guards; the second one
    /// also sets `changes_aborted` to match Python's side effect.
    #[pyo3(signature = (
        tree_index,
        fileid_utf8 = None,
        path_utf8 = None,
        include_deleted = false,
    ))]
    fn get_entry<'py>(
        &mut self,
        py: Python<'py>,
        tree_index: usize,
        fileid_utf8: Option<&[u8]>,
        path_utf8: Option<&[u8]>,
        include_deleted: bool,
    ) -> PyResult<Py<PyAny>> {
        let none_pair = || -> PyResult<Py<PyAny>> {
            Ok(PyTuple::new(py, [py.None(), py.None()])?.unbind().into())
        };

        if let Some(path) = path_utf8 {
            // Path lookup branch.
            let (dirname_raw, basename_raw) = match path.iter().rposition(|&b| b == b'/') {
                Some(i) => (&path[..i], &path[i + 1..]),
                None => (b"".as_slice(), path),
            };
            let bei = self
                .inner
                .get_block_entry_index(dirname_raw, basename_raw, tree_index);
            if !bei.path_present {
                return none_pair();
            }
            let entry = &self.inner.dirblocks[bei.block_index].entries[bei.entry_index];
            let t_kind = entry.trees.get(tree_index).map(|t| t.minikind);
            if entry.key.file_id.is_empty()
                || matches!(
                    t_kind,
                    None | Some(bazaar::dirstate::Kind::Absent)
                        | Some(bazaar::dirstate::Kind::Relocated)
                )
            {
                let errors_mod = py.import("bzrformats.errors")?;
                let bzr_err_cls = errors_mod.getattr("BzrFormatsError")?;
                let exc = bzr_err_cls.call1(("unversioned entry?",))?;
                return Err(PyErr::from_value(exc));
            }
            if let Some(fid) = fileid_utf8 {
                if entry.key.file_id != fid {
                    self.inner.changes_aborted = true;
                    let errors_mod = py.import("bzrformats.errors")?;
                    let bzr_err_cls = errors_mod.getattr("BzrFormatsError")?;
                    let exc = bzr_err_cls
                        .call1(("integrity error ? : mismatching tree_index, file_id and path",))?;
                    return Err(PyErr::from_value(exc));
                }
            }
            return Ok(entry_to_py_tuple(py, entry)?.unbind().into());
        }

        // file_id lookup branch.
        let fid = match fileid_utf8 {
            Some(f) => f,
            None => return none_pair(),
        };

        let file_id = bazaar::FileId::from(&fid.to_vec());
        let candidates = self.inner.get_or_build_id_index().get(&file_id);

        let mut next_path: Option<Vec<u8>> = None;
        for (dn, bn, _) in candidates {
            let search_key = bazaar::dirstate::EntryKey {
                dirname: dn.clone(),
                basename: bn.clone(),
                file_id: fid.to_vec(),
            };
            let (b_idx, b_present) = self.inner.find_block_index_from_key(&search_key);
            if !b_present {
                continue;
            }
            let (e_idx, e_present) = self
                .inner
                .find_entry_index(&search_key, &self.inner.dirblocks[b_idx].entries);
            if !e_present {
                continue;
            }
            let entry = &self.inner.dirblocks[b_idx].entries[e_idx];
            let t_kind = entry.trees.get(tree_index).map(|t| t.minikind);
            match t_kind {
                Some(k) if k.is_fdlt() => {
                    return Ok(entry_to_py_tuple(py, entry)?.unbind().into());
                }
                Some(bazaar::dirstate::Kind::Absent) => {
                    if include_deleted {
                        return Ok(entry_to_py_tuple(py, entry)?.unbind().into());
                    }
                    return none_pair();
                }
                Some(bazaar::dirstate::Kind::Relocated) => {
                    let real_path = entry.trees[tree_index].fingerprint.clone();
                    next_path = Some(real_path);
                    break;
                }
                Some(_) | None => {
                    return Err(pyo3::exceptions::PyAssertionError::new_err(format!(
                        "entry has invalid minikind for tree {}",
                        tree_index
                    )));
                }
            }
        }

        if let Some(real_path) = next_path {
            return self.get_entry(py, tree_index, Some(fid), Some(&real_path), include_deleted);
        }

        none_pair()
    }

    /// Check that every `(dirname_utf8, file_id)` pair in `parents`
    /// exists in `tree_index`. Mirrors Python's
    /// `DirState._after_delta_check_parents`. Raises
    /// `InconsistentDelta` on the first bad parent.
    fn after_delta_check_parents(
        &mut self,
        py: Python<'_>,
        parents: &Bound<PyAny>,
        index: usize,
    ) -> PyResult<()> {
        let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for item in parents.try_iter()? {
            let tup = item?.cast_into::<PyTuple>()?;
            if tup.len() != 2 {
                return Err(PyTypeError::new_err(
                    "after_delta_check_parents entries must be 2-tuples",
                ));
            }
            let dirname: Vec<u8> = tup.get_item(0)?.extract()?;
            let file_id: Vec<u8> = tup.get_item(1)?.extract()?;
            pairs.push((dirname, file_id));
        }
        match self.inner.after_delta_check_parents(&pairs, index) {
            Ok(()) => Ok(()),
            Err(e) => Err(self.raise_basis_apply_error(py, e)),
        }
    }

    /// Verify that none of `new_ids` is already present at a live
    /// entry in `tree_index`. Mirrors Python's
    /// `DirState._check_delta_ids_absent`. Raises
    /// `InconsistentDelta` on conflict, via the shared
    /// `raise_basis_apply_error` helper.
    fn check_delta_ids_absent(
        &mut self,
        py: Python<'_>,
        new_ids: &Bound<PyAny>,
        tree_index: usize,
    ) -> PyResult<()> {
        let mut ids: Vec<Vec<u8>> = Vec::new();
        for item in new_ids.try_iter()? {
            ids.push(item?.extract()?);
        }
        match self.inner.check_delta_ids_absent(&ids, tree_index) {
            Ok(()) => Ok(()),
            Err(e) => Err(self.raise_basis_apply_error(py, e)),
        }
    }

    /// Update a single entry in tree 0. Mirrors Python's
    /// `DirState.update_minimal`. Inputs are passed as separate
    /// positional arguments rather than bundled into a tuple to
    /// match the Python signature byte-for-byte:
    /// - `key` — a `(dirname, basename, file_id)` 3-tuple;
    /// - `minikind` — a one-byte `bytes` object;
    /// - `executable` — bool;
    /// - `fingerprint` — bytes (defaults to `b""`);
    /// - `packed_stat` — bytes or None (None is treated as
    ///   NULLSTAT);
    /// - `size` — unsigned int;
    /// - `path_utf8` — bytes or None (required when the
    ///   cross-reference branch runs);
    /// - `fullscan` — bool.
    ///
    /// Raises `InconsistentDelta` / `NotImplementedError` /
    /// `AssertionError` via the shared `raise_basis_apply_error`
    /// helper.
    #[pyo3(signature = (
        key,
        minikind,
        executable = false,
        fingerprint = None,
        packed_stat = None,
        size = 0,
        path_utf8 = None,
        fullscan = false,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn update_minimal(
        &mut self,
        py: Python<'_>,
        key: &Bound<PyTuple>,
        minikind: &[u8],
        executable: bool,
        fingerprint: Option<&[u8]>,
        packed_stat: Option<&[u8]>,
        size: u64,
        path_utf8: Option<&[u8]>,
        fullscan: bool,
    ) -> PyResult<()> {
        let entry_key = bazaar::dirstate::EntryKey {
            dirname: key.get_item(0)?.extract()?,
            basename: key.get_item(1)?.extract()?,
            file_id: key.get_item(2)?.extract()?,
        };
        let packed_stat_bytes = match packed_stat {
            Some(s) => s.to_vec(),
            None => b"x".repeat(32), // DirState.NULLSTAT is 32 `x` bytes.
        };
        let tree0_details = bazaar::dirstate::TreeData {
            minikind: decode_minikind(minikind)?,
            fingerprint: fingerprint.unwrap_or(b"").to_vec(),
            size,
            executable,
            packed_stat: packed_stat_bytes,
        };
        match self
            .inner
            .update_minimal(entry_key, tree0_details, path_utf8, fullscan)
        {
            Ok(()) => Ok(()),
            Err(e) => Err(self.raise_basis_apply_error(py, e)),
        }
    }

    /// Add a new tracked entry. Mirrors Python's `DirState.add` after
    /// path normalisation: the caller hands in the already-normalised
    /// utf8 path, its `(dirname, basename)` split, the file id, kind
    /// string, size, packed_stat bytes, and fingerprint bytes.
    ///
    /// Raises `DuplicateFileId` when the file_id already lives at a
    /// live path, a bare `Exception("adding already added path!")`
    /// when a different file_id already occupies `(dirname,
    /// basename)`, `NotVersionedError` when the parent dir is missing,
    /// `BzrFormatsError` for unknown kinds, and `AssertionError` for
    /// internal invariant violations.
    #[allow(clippy::too_many_arguments)]
    fn add(
        &mut self,
        py: Python<'_>,
        utf8path: &[u8],
        dirname: &[u8],
        basename: &[u8],
        file_id: &[u8],
        kind: osutils::Kind,
        size: u64,
        packed_stat: &[u8],
        fingerprint: Option<&[u8]>,
    ) -> PyResult<()> {
        match self.inner.add(
            utf8path,
            dirname,
            basename,
            file_id,
            kind,
            size,
            packed_stat,
            fingerprint.unwrap_or(b""),
        ) {
            Ok(()) => Ok(()),
            Err(e) => Err(add_error_to_py(py, e)),
        }
    }

    /// Add a new tracked entry starting from an unsplit, possibly
    /// unnormalised path string.  Mirrors Python's full
    /// `DirState.add` body: splits the path, NFC-normalises the
    /// basename, rejects `.`/`..`, packs the stat tuple, and
    /// dispatches to the pure-crate `add`.
    ///
    /// `stat` is either `None` (substitutes NULLSTAT) or any object
    /// exposing `st_mode`/`st_size`/`st_mtime`/`st_ctime`/`st_dev`/
    /// `st_ino` — matching `os.stat_result`.
    #[pyo3(signature = (path, file_id, kind, stat, fingerprint))]
    fn add_path(
        &mut self,
        py: Python<'_>,
        path: &str,
        file_id: &[u8],
        kind: osutils::Kind,
        stat: Option<&Bound<PyAny>>,
        fingerprint: Option<&[u8]>,
    ) -> PyResult<()> {
        let stat_info: Option<bazaar::dirstate::StatInfo> = match stat {
            None => None,
            Some(s) if s.is_none() => None,
            Some(s) => Some(bazaar::dirstate::StatInfo {
                mode: s.getattr("st_mode")?.extract()?,
                size: s.getattr("st_size")?.extract()?,
                mtime: s.getattr("st_mtime")?.extract::<f64>()? as i64,
                ctime: s.getattr("st_ctime")?.extract::<f64>()? as i64,
                dev: s.getattr("st_dev")?.extract()?,
                ino: s.getattr("st_ino")?.extract()?,
            }),
        };
        match self
            .inner
            .add_path(path, file_id, kind, stat_info, fingerprint.unwrap_or(b""))
        {
            Ok(()) => Ok(()),
            Err(e) => Err(add_error_to_py(py, e)),
        }
    }

    /// Change the file id of the root path. Mirrors Python's
    /// `DirState.set_path_id` for `path=b""` — any other path raises
    /// `NotImplementedError`. Returns silently when `new_id` already
    /// matches the current root id.
    fn set_path_id(&mut self, path: &[u8], new_id: &[u8]) -> PyResult<()> {
        match self.inner.set_path_id(path, new_id) {
            Ok(()) => Ok(()),
            Err(bazaar::dirstate::SetPathIdError::NonRootPath) => Err(
                pyo3::exceptions::PyNotImplementedError::new_err("set_path_id non-root path"),
            ),
            Err(bazaar::dirstate::SetPathIdError::Internal { reason }) => {
                Err(pyo3::exceptions::PyAssertionError::new_err(reason))
            }
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

    /// Walk the dirblocks and verify `DirState._validate`'s
    /// invariants. On violation raises `AssertionError` with the
    /// same message Python would — mirroring
    /// `DirState._validate`.
    fn validate(&self) -> PyResult<()> {
        self.inner
            .validate()
            .map_err(|e| pyo3::exceptions::PyAssertionError::new_err(e.to_string()))
    }

    /// Apply an inventory delta to tree 1.  Mirrors Python's
    /// `DirState.update_basis_by_delta` end-to-end: takes the raw
    /// `InventoryDelta` pyclass directly (no Python-side flattening),
    /// validates each row's `file_id` against its `new_entry`, and
    /// dispatches to the Rust applier.
    fn update_basis_by_delta(
        &mut self,
        py: Python<'_>,
        delta: &crate::inventory::InventoryDelta,
        new_revid: Vec<u8>,
    ) -> PyResult<()> {
        match self
            .inner
            .update_basis_by_delta_from_inventory_delta(&delta.0, new_revid)
        {
            Ok(()) => Ok(()),
            Err(e) => {
                self.inner.changes_aborted = true;
                Err(self.raise_basis_apply_error(py, e))
            }
        }
    }

    /// Apply a pre-flattened inventory delta to tree 0. Mirrors
    /// Python's `DirState.update_by_delta`. Input is a Python
    /// Apply an inventory delta to tree 0.  Takes an `InventoryDelta`
    /// pyclass; Rust does the per-row flattening and dispatch.
    fn update_by_delta(
        &mut self,
        py: Python<'_>,
        delta: &crate::inventory::InventoryDelta,
    ) -> PyResult<()> {
        match self.inner.update_by_delta_from_inventory_delta(&delta.0) {
            Ok(()) => Ok(()),
            Err(e) => {
                self.inner.changes_aborted = true;
                Err(self.raise_basis_apply_error(py, e))
            }
        }
    }

    /// Apply a sequence of "insertions" to tree 0. Mirrors Python's
    /// `DirState._apply_insertions`. Input is a Python iterable of
    /// `(key, minikind, executable, fingerprint, path_utf8)` 5-tuples
    /// matching the shape assembled by `update_by_delta`.
    fn apply_insertions(&mut self, adds: &Bound<PyAny>) -> PyResult<()> {
        let mut rust_adds: Vec<(
            bazaar::dirstate::EntryKey,
            bazaar::dirstate::Kind,
            bool,
            Vec<u8>,
            Vec<u8>,
        )> = Vec::new();
        for item in adds.try_iter()? {
            let tup = item?.cast_into::<PyTuple>()?;
            if tup.len() != 5 {
                return Err(PyTypeError::new_err(
                    "apply_insertions entries must be 5-tuples",
                ));
            }
            let key_tup = tup.get_item(0)?.cast_into::<PyTuple>()?;
            let key = bazaar::dirstate::EntryKey {
                dirname: key_tup.get_item(0)?.extract()?,
                basename: key_tup.get_item(1)?.extract()?,
                file_id: key_tup.get_item(2)?.extract()?,
            };
            let minikind_bytes: Vec<u8> = tup.get_item(1)?.extract()?;
            let minikind = decode_minikind(&minikind_bytes)?;
            let executable: bool = tup.get_item(2)?.extract()?;
            let fingerprint: Vec<u8> = tup.get_item(3)?.extract()?;
            let path_utf8: Vec<u8> = tup.get_item(4)?.extract()?;
            rust_adds.push((key, minikind, executable, fingerprint, path_utf8));
        }
        match self.inner.apply_insertions(rust_adds) {
            Ok(()) => Ok(()),
            Err(e) => Err(self.raise_basis_apply_error(adds.py(), e)),
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
                minikind: decode_minikind(&minikind_bytes)?,
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

    /// Replace the current tree-0 state with entries from the given
    /// inventory rows. Mirrors Python's
    /// `DirState.set_state_from_inventory`. Input is an iterable of
    /// `(path_utf8, file_id, minikind, fingerprint, executable)`
    /// tuples in `iter_entries_by_dir` order.
    fn set_state_from_inventory(
        &mut self,
        py: Python<'_>,
        new_entries: &Bound<PyAny>,
    ) -> PyResult<()> {
        let mut rows: Vec<(Vec<u8>, Vec<u8>, bazaar::dirstate::Kind, Vec<u8>, bool)> = Vec::new();
        for item in new_entries.try_iter()? {
            let tup = item?.cast_into::<PyTuple>()?;
            if tup.len() != 5 {
                return Err(PyTypeError::new_err(
                    "set_state_from_inventory entries must be 5-tuples",
                ));
            }
            let path: Vec<u8> = tup.get_item(0)?.extract()?;
            let file_id: Vec<u8> = tup.get_item(1)?.extract()?;
            let minikind_bytes: Vec<u8> = tup.get_item(2)?.extract()?;
            let minikind = decode_minikind(&minikind_bytes)?;
            let fingerprint: Vec<u8> = tup.get_item(3)?.extract()?;
            let executable: bool = tup.get_item(4)?.extract()?;
            rows.push((path, file_id, minikind, fingerprint, executable));
        }
        match self.inner.set_state_from_inventory(rows) {
            Ok(()) => Ok(()),
            Err(e) => Err(self.raise_basis_apply_error(py, e)),
        }
    }

    /// Replace the parent trees. Mirrors Python's
    /// `DirState.set_parent_trees`. Input:
    ///
    /// - `trees`: iterable of `bytes` revision ids (one per parent,
    ///   including ghosts), in order.
    /// - `ghosts`: iterable of `bytes` revision ids that are ghosts.
    /// - `parent_tree_entries`: iterable of lists — one list per
    ///   non-ghost parent tree, in the same order as non-ghost parents
    ///   appear in `trees`. Each list is a sequence of
    ///   `(path_utf8, file_id, details)` tuples where `details` is the
    ///   5-tuple returned by `inv_entry_to_details`:
    ///   `(minikind_byte, fingerprint, size, executable, tree_data)`.
    ///
    /// Each inner list must be pre-sorted in `iter_entries_by_dir`
    /// order (i.e. root first, then children in lexicographic order).
    fn set_parent_trees(
        &mut self,
        trees: &Bound<PyAny>,
        ghosts: &Bound<PyAny>,
        parent_tree_entries: &Bound<PyAny>,
    ) -> PyResult<()> {
        let rust_trees = collect_bytes_vec(trees)?;
        let rust_ghosts = collect_bytes_vec(ghosts)?;
        let mut rust_entries: Vec<Vec<(Vec<u8>, Vec<u8>, bazaar::dirstate::TreeData)>> = Vec::new();
        for tree_iter in parent_tree_entries.try_iter()? {
            let tree = tree_iter?;
            let mut tree_rows: Vec<(Vec<u8>, Vec<u8>, bazaar::dirstate::TreeData)> = Vec::new();
            for row in tree.try_iter()? {
                let tup = row?.cast_into::<PyTuple>()?;
                if tup.len() != 3 {
                    return Err(PyTypeError::new_err(
                        "parent_tree_entries entries must be 3-tuples",
                    ));
                }
                let path_utf8: Vec<u8> = tup.get_item(0)?.extract()?;
                let file_id: Vec<u8> = tup.get_item(1)?.extract()?;
                let details_tup = tup.get_item(2)?.cast_into::<PyTuple>()?;
                if details_tup.len() != 5 {
                    return Err(PyTypeError::new_err("details must be a 5-tuple"));
                }
                let minikind_bytes: Vec<u8> = details_tup.get_item(0)?.extract()?;
                let minikind = decode_minikind(&minikind_bytes)?;
                let fingerprint: Vec<u8> = details_tup.get_item(1)?.extract()?;
                let size: u64 = details_tup.get_item(2)?.extract()?;
                let executable: bool = details_tup.get_item(3)?.extract()?;
                let packed_stat: Vec<u8> = details_tup.get_item(4)?.extract()?;
                tree_rows.push((
                    path_utf8,
                    file_id,
                    bazaar::dirstate::TreeData {
                        minikind,
                        fingerprint,
                        size,
                        executable,
                        packed_stat,
                    },
                ));
            }
            rust_entries.push(tree_rows);
        }
        self.inner
            .set_parent_trees(rust_trees, rust_ghosts, rust_entries)
            .map_err(|e| pyo3::exceptions::PyValueError::new_err(format!("{:?}", e)))
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

    /// Materialise every entry across every dirblock as a list of
    /// `((dirname, basename, file_id), [tree_tuple, ...])` tuples in
    /// dirblock order.  Mirrors Python's `_iter_entries`, but does the
    /// marshalling once instead of once per dirblock access — call
    /// once and iterate the returned list, do not re-call inside a
    /// loop.
    fn entries<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let out = PyList::empty(py);
        for entry in self.inner.iter_entries() {
            out.append(entry_to_py_tuple(py, entry)?)?;
        }
        Ok(out)
    }

    /// Build a lazy `IterChanges` iterator.  Wraps the pure-crate
    /// `IterChangesIter` state machine; call repeatedly via
    /// `__next__` to drain one `DirstateInventoryChange`-shaped tuple
    /// at a time.  Mirrors `ProcessEntryPython.iter_changes` but
    /// without materialising the change list up front.
    #[pyo3(signature = (
        source_index,
        target_index,
        include_unchanged,
        want_unversioned,
        search_specific_files,
        supports_tree_reference,
        root_abspath,
    ))]
    fn iter_changes(
        slf: Py<Self>,
        py: Python<'_>,
        source_index: Option<usize>,
        target_index: usize,
        include_unchanged: bool,
        want_unversioned: bool,
        search_specific_files: &Bound<PyAny>,
        supports_tree_reference: bool,
        root_abspath: &Bound<PyAny>,
    ) -> PyResult<IterChanges> {
        let search: std::collections::HashSet<Vec<u8>> = collect_bytes_set(search_specific_files)?;
        let partial = !(search.len() == 1 && search.contains(&Vec::<u8>::new()));
        let root_abspath_bytes: Vec<u8> = root_abspath.extract()?;
        let pstate = bazaar::dirstate::ProcessEntryState {
            source_index,
            target_index,
            include_unchanged,
            want_unversioned,
            partial,
            supports_tree_reference,
            root_abspath: root_abspath_bytes,
            searched_specific_files: std::collections::HashSet::new(),
            search_specific_files: search,
            search_specific_file_parents: std::collections::HashSet::new(),
            searched_exact_paths: std::collections::HashSet::new(),
            seen_ids: std::collections::HashSet::new(),
            new_dirname_to_file_id: std::collections::HashMap::new(),
            old_dirname_to_file_id: std::collections::HashMap::new(),
            last_source_parent: None,
            last_target_parent: None,
        };
        Ok(IterChanges {
            dirstate: slf.clone_ref(py),
            iter: bazaar::dirstate::IterChangesIter::new(),
            pstate,
        })
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
            bazaar::dirstate::BasisApplyError::NotVersioned { path } => {
                NotVersionedError::new_err((PyBytes::new(py, &path).unbind(), ""))
            }
            bazaar::dirstate::BasisApplyError::MismatchedEntryFileId {
                new_path,
                file_id,
                entry_debug,
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
                let path_bytes = PyBytes::new(py, &new_path);
                let file_id_bytes = PyBytes::new(py, &file_id);
                let reason = format!("mismatched entry file_id {}", entry_debug);
                match cls.call1((path_bytes, file_id_bytes, reason)) {
                    Ok(instance) => PyErr::from_value(instance),
                    Err(e) => e,
                }
            }
            bazaar::dirstate::BasisApplyError::NewPathWithoutEntry { new_path, file_id } => {
                self.inner.changes_aborted = true;
                let errors_mod = match py.import("bzrformats.errors") {
                    Ok(m) => m,
                    Err(e) => return e,
                };
                let cls = match errors_mod.getattr("InconsistentDelta") {
                    Ok(c) => c,
                    Err(e) => return e,
                };
                let path_bytes = PyBytes::new(py, &new_path);
                let file_id_bytes = PyBytes::new(py, &file_id);
                match cls.call1((path_bytes, file_id_bytes, "new_path with no entry")) {
                    Ok(instance) => PyErr::from_value(instance),
                    Err(e) => e,
                }
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

/// Lazy iterator over the output of the pure-crate iter_changes walk.
///
/// The pyclass owns the walker state (`IterChangesIter`) and the
/// per-iter `ProcessEntryState`; it borrows the underlying `DirState`
/// via a `Py<PyDirState>` handle and re-acquires a mutable reference
/// to it on every `__next__` call.  Filesystem calls dispatch through
/// `PyFileTransport` wrapping `PyNone` — the transport's lstat /
/// readlink / list_dir all go through `os.*` directly, so no file
/// handle is required.
#[pyclass]
struct IterChanges {
    dirstate: Py<PyDirState>,
    iter: bazaar::dirstate::IterChangesIter,
    pstate: bazaar::dirstate::ProcessEntryState,
}

#[pymethods]
impl IterChanges {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<Self>) -> PyResult<Option<Py<PyAny>>> {
        let py = slf.py();
        let transport = PyFileTransport::new(
            pyo3::types::PyNone::get(py).to_owned().into_any().unbind(),
            bazaar::dirstate::LockState::Read,
        );
        let dirstate = slf.dirstate.clone_ref(py);
        let IterChanges {
            iter: ref mut iter_state,
            ref mut pstate,
            ..
        } = *slf;
        let (result, dirstate_path) = {
            let mut state_ref = dirstate.borrow_mut(py);
            let path = state_ref.inner.filename.to_string_lossy().into_owned();
            let r = state_ref
                .inner
                .iter_changes_next(iter_state, pstate, &transport);
            (r, path)
        };
        match result {
            Ok(Some(change)) => {
                let tup = dirstate_change_to_pytuple(py, &change)?;
                let ds_mod = py.import("bzrformats.dirstate")?;
                let cls = ds_mod.getattr("DirstateInventoryChange")?;
                Ok(Some(cls.call1(tup)?.unbind()))
            }
            Ok(None) => Ok(None),
            Err(bazaar::dirstate::ProcessEntryError::DirstateCorrupt(msg)) => {
                Err(DirstateCorrupt::new_err((dirstate_path, msg)))
            }
            Err(bazaar::dirstate::ProcessEntryError::BadFileKind { path, mode }) => {
                Err(bad_file_kind_error(py, &path, mode))
            }
            Err(bazaar::dirstate::ProcessEntryError::Internal(msg)) => {
                Err(pyo3::exceptions::PyAssertionError::new_err(msg))
            }
        }
    }

    /// Read-only view of `search_specific_files` — the roots that
    /// still have to be walked.  Used by Python callers that want to
    /// peek at walker progress; mutation goes through the pure crate.
    #[getter]
    fn search_specific_files<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let out = pyo3::types::PySet::empty(py)?;
        for p in &self.pstate.search_specific_files {
            out.add(PyBytes::new(py, p))?;
        }
        Ok(out.into_any())
    }

    /// Read-only view of `searched_specific_files`.
    #[getter]
    fn searched_specific_files<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let out = pyo3::types::PySet::empty(py)?;
        for p in &self.pstate.searched_specific_files {
            out.add(PyBytes::new(py, p))?;
        }
        Ok(out.into_any())
    }

    /// Read-only view of `searched_exact_paths`.
    #[getter]
    fn searched_exact_paths<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let out = pyo3::types::PySet::empty(py)?;
        for p in &self.pstate.searched_exact_paths {
            out.add(PyBytes::new(py, p))?;
        }
        Ok(out.into_any())
    }

    /// Read-only view of `search_specific_file_parents`.
    #[getter]
    fn search_specific_file_parents<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let out = pyo3::types::PySet::empty(py)?;
        for p in &self.pstate.search_specific_file_parents {
            out.add(PyBytes::new(py, p))?;
        }
        Ok(out.into_any())
    }

    /// Read-only view of `seen_ids`.
    #[getter]
    fn seen_ids<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let out = pyo3::types::PySet::empty(py)?;
        for p in &self.pstate.seen_ids {
            out.add(PyBytes::new(py, p))?;
        }
        Ok(out.into_any())
    }
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

    fn clear(&mut self) {
        self.0.clear();
    }

    /// Replace the contents of this IdIndex with the id_index Rust
    /// already maintains for `state`.  Faster than walking dirblocks
    /// from Python because it avoids marshalling the whole tree.
    fn fill_from_state(&mut self, state: &mut PyDirState) {
        self.0.clear();
        let rust_index = state.inner.get_or_build_id_index();
        for (dn, bn, fid) in rust_index.iter_all() {
            self.0.add((dn, bn, fid));
        }
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
        PyBytes::new(py, &[ret.0.to_minikind()]),
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
    m.add_class::<IterChanges>()?;
    m.add_wrapped(wrap_pyfunction!(inv_entry_to_details))?;
    m.add_wrapped(wrap_pyfunction!(get_output_lines))?;

    // Register dirstate helper functions (_read_dirblocks, entry_to_line).
    crate::dirstate_helpers::register(&m)?;

    Ok(m)
}
