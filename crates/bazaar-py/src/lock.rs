//! pyo3 bindings for [`bazaar::lock`]. Exposes `ReadLock`, `WriteLock`,
//! and `LogicalLockResult` plus access to the in-process bookkeeping
//! that the existing `bzrformats.lock` Python tests inspect.

use bazaar::lock::{
    self as rs_lock, LockError, ReadLock as RsReadLock, TemporaryWriteLockResult,
    WriteLock as RsWriteLock,
};
use pyo3::exceptions::{PyOSError, PyValueError};
use pyo3::import_exception;
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyDict, PyTuple};
use std::path::PathBuf;

import_exception!(bzrformats.errors, LockContention);
import_exception!(bzrformats.errors, LockNotHeld);

fn lock_err_to_py(err: LockError) -> PyErr {
    match err {
        LockError::Contention(p) => LockContention::new_err(p.to_string_lossy().into_owned()),
        LockError::NotHeld(p) => LockNotHeld::new_err(p.to_string_lossy().into_owned()),
        LockError::Io(e) => match e.kind() {
            std::io::ErrorKind::NotFound => {
                pyo3::exceptions::PyFileNotFoundError::new_err(e.to_string())
            }
            _ => PyOSError::new_err(e.to_string()),
        },
    }
}

fn extract_path(obj: &Bound<'_, PyAny>) -> PyResult<PathBuf> {
    if let Ok(s) = obj.extract::<String>() {
        return Ok(PathBuf::from(s));
    }
    if let Ok(b) = obj.extract::<Vec<u8>>() {
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            return Ok(PathBuf::from(std::ffi::OsStr::from_bytes(&b)));
        }
        #[cfg(not(unix))]
        {
            return String::from_utf8(b)
                .map(PathBuf::from)
                .map_err(|e| PyValueError::new_err(e.to_string()));
        }
    }
    obj.str()
        .and_then(|s| s.extract::<String>())
        .map(PathBuf::from)
}

/// Wraps a `bazaar::lock::ReadLock`.
#[pyclass(name = "ReadLock", subclass)]
struct PyReadLock {
    /// `None` once the lock has been released or moved into a write
    /// lock via `temporary_write_lock`.
    inner: std::sync::Mutex<Option<RsReadLock>>,
    filename: PathBuf,
    /// Cached Python file object so successive accesses to `.f` see
    /// the same value.
    file_obj: std::sync::Mutex<Option<Py<PyAny>>>,
}

impl PyReadLock {
    fn build_file_obj(py: Python<'_>, lock: &RsReadLock) -> PyResult<Py<PyAny>> {
        use std::os::fd::AsRawFd;
        let file = lock.file().ok_or_else(|| LockNotHeld::new_err(()))?;
        let fd = file.as_raw_fd();
        // Wrap the underlying fd in a Python file object that does not
        // own (close) it on `__del__` — closeFd=False — because the
        // Rust ReadLock owns the std::fs::File and will close it on
        // unlock/drop.
        let fdopen = py.import("os")?.getattr("fdopen")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("closefd", false)?;
        let pyfile = fdopen.call((fd, "rb"), Some(&kwargs))?;
        Ok(pyfile.unbind())
    }
}

#[pymethods]
impl PyReadLock {
    #[new]
    fn new(py: Python<'_>, filename: Bound<'_, PyAny>) -> PyResult<Self> {
        let path = extract_path(&filename)?;
        let lock = RsReadLock::new(&path).map_err(lock_err_to_py)?;
        let file_obj = Self::build_file_obj(py, &lock)?;
        Ok(Self {
            inner: std::sync::Mutex::new(Some(lock)),
            filename: path,
            file_obj: std::sync::Mutex::new(Some(file_obj)),
        })
    }

    #[getter]
    fn filename(&self) -> String {
        self.filename.to_string_lossy().into_owned()
    }

    #[getter]
    fn f<'py>(&self, py: Python<'py>) -> Bound<'py, PyAny> {
        let guard = self.file_obj.lock().unwrap();
        match guard.as_ref() {
            Some(f) => f.bind(py).clone(),
            None => py.None().into_bound(py),
        }
    }

    #[setter]
    fn set_f(&self, value: Bound<'_, PyAny>) {
        let mut guard = self.file_obj.lock().unwrap();
        if value.is_none() {
            *guard = None;
        } else {
            *guard = Some(value.unbind());
        }
    }

    fn unlock(&self) -> PyResult<()> {
        // Drop our cached Python file object first so any pending
        // close happens before the Rust File goes away.
        {
            let mut file_obj = self.file_obj.lock().unwrap();
            *file_obj = None;
        }
        let mut guard = self.inner.lock().unwrap();
        let mut lock = guard
            .take()
            .ok_or_else(|| LockNotHeld::new_err(self.filename.to_string_lossy().into_owned()))?;
        lock.unlock().map_err(lock_err_to_py)?;
        Ok(())
    }

    /// Try to upgrade to a write lock. Returns `(True, write_lock)`
    /// on success or `(False, self)` on contention, matching Python.
    fn temporary_write_lock<'py>(
        slf: Bound<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let path = {
            let r = slf.borrow();
            r.filename.clone()
        };
        // Drop the cached Python file object — the underlying fd is
        // about to be closed by the Rust upgrade dance.
        {
            let r = slf.borrow();
            let mut file_obj = r.file_obj.lock().unwrap();
            *file_obj = None;
        }
        let lock_opt = {
            let r = slf.borrow();
            let mut guard = r.inner.lock().unwrap();
            guard.take()
        };
        let lock =
            lock_opt.ok_or_else(|| LockNotHeld::new_err(path.to_string_lossy().into_owned()))?;
        let result = lock.temporary_write_lock().map_err(lock_err_to_py)?;
        match result {
            TemporaryWriteLockResult::Succeeded(wl) => {
                let py_wl = PyWriteLock::from_inner(py, wl)?;
                let wl_bound = Bound::new(py, py_wl)?;
                Ok(PyTuple::new(
                    py,
                    [
                        true.into_pyobject(py)?.to_owned().into_any(),
                        wl_bound.into_any(),
                    ],
                )?)
            }
            TemporaryWriteLockResult::Failed(read_lock) => {
                // Re-stash the read lock and rebuild the Python file.
                let new_file = Self::build_file_obj(py, &read_lock)?;
                {
                    let r = slf.borrow();
                    let mut file_obj = r.file_obj.lock().unwrap();
                    *file_obj = Some(new_file);
                }
                {
                    let r = slf.borrow();
                    let mut guard = r.inner.lock().unwrap();
                    *guard = Some(read_lock);
                }
                Ok(PyTuple::new(
                    py,
                    [
                        false.into_pyobject(py)?.to_owned().into_any(),
                        slf.into_any(),
                    ],
                )?)
            }
        }
    }
}

/// Wraps a `bazaar::lock::WriteLock`.
#[pyclass(name = "WriteLock", subclass)]
struct PyWriteLock {
    inner: std::sync::Mutex<Option<RsWriteLock>>,
    filename: PathBuf,
    file_obj: std::sync::Mutex<Option<Py<PyAny>>>,
}

impl PyWriteLock {
    fn build_file_obj(py: Python<'_>, lock: &RsWriteLock) -> PyResult<Py<PyAny>> {
        use std::os::fd::AsRawFd;
        let file = lock.file().ok_or_else(|| LockNotHeld::new_err(()))?;
        let fd = file.as_raw_fd();
        let fdopen = py.import("os")?.getattr("fdopen")?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("closefd", false)?;
        let pyfile = fdopen.call((fd, "rb+"), Some(&kwargs))?;
        Ok(pyfile.unbind())
    }

    fn from_inner(py: Python<'_>, lock: RsWriteLock) -> PyResult<Self> {
        let filename = lock.path().to_path_buf();
        let file_obj = Self::build_file_obj(py, &lock)?;
        Ok(Self {
            inner: std::sync::Mutex::new(Some(lock)),
            filename,
            file_obj: std::sync::Mutex::new(Some(file_obj)),
        })
    }
}

#[pymethods]
impl PyWriteLock {
    #[new]
    fn new(py: Python<'_>, filename: Bound<'_, PyAny>) -> PyResult<Self> {
        let path = extract_path(&filename)?;
        let lock = RsWriteLock::new(&path).map_err(lock_err_to_py)?;
        Self::from_inner(py, lock)
    }

    #[getter]
    fn filename(&self) -> String {
        self.filename.to_string_lossy().into_owned()
    }

    #[getter]
    fn f<'py>(&self, py: Python<'py>) -> Bound<'py, PyAny> {
        let guard = self.file_obj.lock().unwrap();
        match guard.as_ref() {
            Some(f) => f.bind(py).clone(),
            None => py.None().into_bound(py),
        }
    }

    #[setter]
    fn set_f(&self, value: Bound<'_, PyAny>) {
        let mut guard = self.file_obj.lock().unwrap();
        if value.is_none() {
            *guard = None;
        } else {
            *guard = Some(value.unbind());
        }
    }

    fn unlock(&self) -> PyResult<()> {
        {
            let mut file_obj = self.file_obj.lock().unwrap();
            *file_obj = None;
        }
        let mut guard = self.inner.lock().unwrap();
        let mut lock = guard
            .take()
            .ok_or_else(|| LockNotHeld::new_err(self.filename.to_string_lossy().into_owned()))?;
        lock.unlock().map_err(lock_err_to_py)?;
        Ok(())
    }

    fn restore_read_lock(&self, py: Python<'_>) -> PyResult<PyReadLock> {
        {
            let mut file_obj = self.file_obj.lock().unwrap();
            *file_obj = None;
        }
        let mut guard = self.inner.lock().unwrap();
        let lock = guard
            .take()
            .ok_or_else(|| LockNotHeld::new_err(self.filename.to_string_lossy().into_owned()))?;
        let new_lock = lock.restore_read_lock().map_err(lock_err_to_py)?;
        let file_obj = PyReadLock::build_file_obj(py, &new_lock)?;
        let path = new_lock.path().to_path_buf();
        Ok(PyReadLock {
            inner: std::sync::Mutex::new(Some(new_lock)),
            filename: path,
            file_obj: std::sync::Mutex::new(Some(file_obj)),
        })
    }
}

/// `LogicalLockResult` matching Python's two-arg constructor.
#[pyclass(name = "LogicalLockResult", subclass)]
struct PyLogicalLockResult {
    unlock: Py<PyAny>,
    token: Option<Py<PyAny>>,
}

#[pymethods]
impl PyLogicalLockResult {
    #[new]
    #[pyo3(signature = (unlock, token = None))]
    fn new(unlock: Py<PyAny>, token: Option<Py<PyAny>>) -> Self {
        Self { unlock, token }
    }

    #[getter]
    fn unlock<'py>(&self, py: Python<'py>) -> Bound<'py, PyAny> {
        self.unlock.bind(py).clone()
    }

    #[getter]
    fn token<'py>(&self, py: Python<'py>) -> Bound<'py, PyAny> {
        match self.token.as_ref() {
            Some(t) => t.bind(py).clone(),
            None => py.None().into_bound(py),
        }
    }

    fn __repr__(&self, py: Python<'_>) -> String {
        let unlock_repr = self
            .unlock
            .bind(py)
            .repr()
            .map(|s| s.to_string())
            .unwrap_or_else(|_| "<unlock>".into());
        format!("LogicalLockResult({})", unlock_repr)
    }

    fn __enter__<'py>(slf: Bound<'py, Self>) -> Bound<'py, Self> {
        slf
    }

    fn __exit__(
        &self,
        py: Python<'_>,
        exc_type: Bound<'_, PyAny>,
        _exc_val: Bound<'_, PyAny>,
        _exc_tb: Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        // Mirror Python: call self.unlock(); if it raises and there's
        // already an exception in flight, swallow ours; otherwise
        // propagate.
        let result = self.unlock.bind(py).call0();
        if exc_type.is_none() && result.is_err() {
            return Err(result.err().unwrap());
        }
        Ok(false)
    }
}

/// Snapshot the in-process bookkeeping. Returns a dict with two keys:
/// `read_locks` (mapping path → count) and `write_locks` (set of paths).
/// Used by `bzrformats.lock` Python tests to verify invariants.
#[pyfunction]
fn _snapshot_state<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
    let (reads, writes) = rs_lock::snapshot();
    let out = PyDict::new(py);
    let read_locks = PyDict::new(py);
    for (p, c) in reads {
        read_locks.set_item(p.to_string_lossy().into_owned(), c)?;
    }
    out.set_item("read_locks", read_locks)?;
    let write_locks = pyo3::types::PySet::empty(py)?;
    for p in writes {
        write_locks.add(p.to_string_lossy().into_owned())?;
    }
    out.set_item("write_locks", write_locks)?;
    Ok(out)
}

/// Reset the bookkeeping. Used by tests' setUp.
#[pyfunction]
fn _reset_state() {
    rs_lock::reset_for_tests();
}

pub fn _lock_rs(py: Python<'_>) -> PyResult<Bound<'_, PyModule>> {
    let m = PyModule::new(py, "lock")?;
    m.add_class::<PyReadLock>()?;
    m.add_class::<PyWriteLock>()?;
    m.add_class::<PyLogicalLockResult>()?;
    m.add_function(wrap_pyfunction!(_snapshot_state, &m)?)?;
    m.add_function(wrap_pyfunction!(_reset_state, &m)?)?;
    Ok(m)
}
