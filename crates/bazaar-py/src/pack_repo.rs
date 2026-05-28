//! pyo3 bindings for `bzrformats.pack_repo`.
//!
//! These are thin wrappers: the pure algorithm (memo grouping, raw-record
//! slicing, reload decision, index name/offset table) lives in
//! [`bazaar::pack_repo`]; everything else here is orchestration over
//! Python objects (transports, graph indices, container writers, config
//! stacks) that has no Rust-native representation.
//!
//! `RetryWithNewPacks` stays a pure-Python exception in
//! `bzrformats.pack_repo` (its `__str__` relies on the `BzrFormatsError`
//! `_fmt`/`__dict__` machinery); the pyo3 side only imports it to raise
//! it from `_DirectPackAccess.get_raw_records`.

use bazaar::pack_repo::{
    self, group_retrieval_requests, index_name as rs_index_name, index_offset as rs_index_offset,
    reload_decision, split_raw_records, IndexKind, ReloadDecision,
};
use pyo3::exceptions::{PyAssertionError, PyKeyError, PyStopIteration, PyTypeError};
use pyo3::import_exception;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};
use pyo3::PyTypeInfo;

import_exception!(bzrformats.pack_repo, RetryWithNewPacks);
import_exception!(bzrformats.errors, BzrCheckError);
import_exception!(bzrformats.transport, NoSuchFile);

/// Resolve a Python index-type string to the typed [`IndexKind`],
/// raising `KeyError` for an unknown type to match the Python code's
/// `Pack.index_definitions[index_type]` dict lookup.
fn index_kind(index_type: &str) -> PyResult<IndexKind> {
    IndexKind::from_name(index_type).ok_or_else(|| PyKeyError::new_err(index_type.to_string()))
}

/// Test whether `err` is a not-found error from either the bzrformats or
/// the breezy transport stack. The Python code catches
/// `TransportNoSuchFile`, the tuple of `bzrformats.transport.NoSuchFile`
/// and (when breezy is installed) `breezy.transport.NoSuchFile`. That
/// tuple is precomputed by `bzrformats.transport`, so check against it
/// to handle the optional-breezy case exactly as Python does.
fn is_no_such_file(py: Python<'_>, err: &PyErr) -> bool {
    if err.is_instance_of::<NoSuchFile>(py) {
        return true;
    }
    let Ok(transport) = PyModule::import(py, "bzrformats.transport") else {
        return false;
    };
    let Ok(tuple) = transport.getattr("TransportNoSuchFile") else {
        return false;
    };
    err.matches(py, tuple).unwrap_or(false)
}

/// Build a `sys.exc_info()`-style 3-tuple `(type, value, traceback)` from
/// a `PyErr`, matching the `exc_info` argument the Python code passes to
/// `RetryWithNewPacks`.
fn exc_info_tuple<'py>(py: Python<'py>, err: &PyErr) -> Bound<'py, PyTuple> {
    let ty = err.get_type(py).into_any();
    let value = err.value(py).clone().into_any();
    let tb = match err.traceback(py) {
        Some(tb) => tb.into_any(),
        None => py.None().into_bound(py),
    };
    PyTuple::new(py, [ty, value, tb]).expect("3-element tuple")
}

/// `bzrformats.pack_repo._DirectPackAccess` — access to data in one or
/// more packs with less translation.
#[pyclass(module = "bzrformats._bzr_rs.pack_repo", name = "_DirectPackAccess")]
pub struct DirectPackAccess {
    /// Maps index object -> (transport, path) tuple. Keyed by Python
    /// object identity/equality, so kept as a Python dict.
    indices: Py<PyDict>,
    reload_func: Option<Py<PyAny>>,
    flush_func: Option<Py<PyAny>>,
    container_writer: Option<Py<PyAny>>,
    write_index: Option<Py<PyAny>>,
}

#[pymethods]
impl DirectPackAccess {
    #[new]
    #[pyo3(signature = (index_to_packs, reload_func=None, flush_func=None))]
    fn new(
        py: Python<'_>,
        index_to_packs: Bound<'_, PyAny>,
        reload_func: Option<Py<PyAny>>,
        flush_func: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        // The Python constructor stores the dict by reference. Accept any
        // mapping and adopt it as our backing dict.
        let indices: Py<PyDict> = index_to_packs.extract()?;
        Ok(Self {
            indices,
            reload_func: reload_func.filter(|f| !f.is_none(py)),
            flush_func: flush_func.filter(|f| !f.is_none(py)),
            container_writer: None,
            write_index: None,
        })
    }

    /// The live `index -> (transport, path)` mapping. Returned by
    /// reference so callers mutating it (e.g. breezy tests doing
    /// `access._indices.clear()`) affect this object's state, matching
    /// the Python `self._indices` attribute.
    #[getter]
    fn _indices(&self, py: Python<'_>) -> Py<PyDict> {
        self.indices.clone_ref(py)
    }

    #[setter]
    fn set__indices(&mut self, value: Py<PyDict>) {
        self.indices = value;
    }

    /// The reload function (or `None`). Readable and writable to match
    /// the Python `self._reload_func` attribute, which downstream tests
    /// reassign after construction.
    #[getter]
    fn _reload_func(&self, py: Python<'_>) -> Py<PyAny> {
        self.reload_func
            .as_ref()
            .map(|f| f.clone_ref(py))
            .unwrap_or_else(|| py.None())
    }

    #[setter]
    fn set__reload_func(&mut self, py: Python<'_>, value: Py<PyAny>) {
        self.reload_func = if value.is_none(py) { None } else { Some(value) };
    }

    /// The flush function (or `None`), matching `self._flush_func`.
    #[getter]
    fn _flush_func(&self, py: Python<'_>) -> Py<PyAny> {
        self.flush_func
            .as_ref()
            .map(|f| f.clone_ref(py))
            .unwrap_or_else(|| py.None())
    }

    #[setter]
    fn set__flush_func(&mut self, py: Python<'_>, value: Py<PyAny>) {
        self.flush_func = if value.is_none(py) { None } else { Some(value) };
    }

    /// The container writer (or `None`), matching `self._container_writer`.
    #[getter]
    fn _container_writer(&self, py: Python<'_>) -> Py<PyAny> {
        self.container_writer
            .as_ref()
            .map(|w| w.clone_ref(py))
            .unwrap_or_else(|| py.None())
    }

    #[setter]
    fn set__container_writer(&mut self, py: Python<'_>, value: Py<PyAny>) {
        self.container_writer = if value.is_none(py) { None } else { Some(value) };
    }

    /// The write index (or `None`), matching `self._write_index`.
    #[getter]
    fn _write_index(&self, py: Python<'_>) -> Py<PyAny> {
        self.write_index
            .as_ref()
            .map(|i| i.clone_ref(py))
            .unwrap_or_else(|| py.None())
    }

    #[setter]
    fn set__write_index(&mut self, value: Py<PyAny>) {
        self.write_index = Some(value);
    }

    /// Add raw bytes to the storage area, returning the `(index, pos,
    /// length)` memo to retrieve them later.
    fn add_raw_record(
        &self,
        py: Python<'_>,
        _key: Bound<'_, PyAny>,
        size: usize,
        raw_data: Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let writer = self
            .container_writer
            .as_ref()
            .ok_or_else(|| PyAssertionError::new_err("add_raw_record called before set_writer"))?;
        let result = writer
            .bind(py)
            .call_method1("add_bytes_record", (raw_data, size, PyList::empty(py)))?;
        let (p_offset, p_length): (Py<PyAny>, Py<PyAny>) = result.extract()?;
        let write_index = self
            .write_index
            .as_ref()
            .map(|i| i.clone_ref(py))
            .unwrap_or_else(|| py.None());
        Ok(PyTuple::new(py, [write_index, p_offset, p_length])?
            .into_any()
            .unbind())
    }

    /// Add many raw records from one concatenated buffer. Returns a memo
    /// list parallel to `key_sizes`.
    fn add_raw_records(
        &self,
        py: Python<'_>,
        key_sizes: Bound<'_, PyAny>,
        raw_data: Bound<'_, PyAny>,
    ) -> PyResult<Py<PyList>> {
        // raw_data is an iterable of byte chunks; join into one buffer.
        let joined = join_bytes(py, &raw_data)?;

        let mut sizes: Vec<usize> = Vec::new();
        let mut keys: Vec<Py<PyAny>> = Vec::new();
        for item in key_sizes.try_iter()? {
            let item = item?;
            let (key, size): (Py<PyAny>, usize) = item.extract()?;
            keys.push(key);
            sizes.push(size);
        }

        let slices = split_raw_records(&sizes, joined.len())
            .map_err(|e| PyAssertionError::new_err(e.to_string()))?;

        let result = PyList::empty(py);
        for (key, slice) in keys.into_iter().zip(slices) {
            let chunk = PyBytes::new(py, &joined[slice.start..slice.start + slice.length]);
            let chunk_list = PyList::new(py, [chunk])?;
            let memo =
                self.add_raw_record(py, key.into_bound(py), slice.length, chunk_list.into_any())?;
            result.append(memo)?;
        }
        Ok(result.unbind())
    }

    /// Flush pending writes by invoking the configured flush function.
    fn flush(&self, py: Python<'_>) -> PyResult<()> {
        if let Some(f) = &self.flush_func {
            f.bind(py).call0()?;
        }
        Ok(())
    }

    /// Return an iterator over the raw bytes for the given retrieval
    /// memos. Reads are grouped by index and served via
    /// `pack.make_readv_reader`. A missing index or a missing pack file
    /// raises `RetryWithNewPacks` (during iteration) when a reload
    /// function is configured.
    fn get_raw_records(
        &self,
        py: Python<'_>,
        memos_for_retrieval: Bound<'_, PyAny>,
    ) -> PyResult<Py<RawRecordsIter>> {
        // First pass: group into consecutive same-index requests. The
        // index objects are arbitrary Python objects, so wrap them in a
        // key that compares by Python equality.
        let mut memos: Vec<(PyIndexKey, u64, u64)> = Vec::new();
        for item in memos_for_retrieval.try_iter()? {
            let item = item?;
            let (index, offset, length): (Py<PyAny>, u64, u64) = item.extract()?;
            memos.push((PyIndexKey(index), offset, length));
        }
        let groups = group_retrieval_requests(memos);

        let request_lists = PyList::empty(py);
        for group in groups {
            let offsets = PyList::empty(py);
            for (offset, length) in group.reads {
                offsets.append(PyTuple::new(py, [offset, length])?)?;
            }
            request_lists.append(PyTuple::new(
                py,
                [group.index.0.into_bound(py).into_any(), offsets.into_any()],
            )?)?;
        }

        let iter = RawRecordsIter {
            request_lists: request_lists.unbind(),
            group_pos: 0,
            indices: self.indices.clone_ref(py),
            reload_func: self.reload_func.as_ref().map(|f| f.clone_ref(py)),
            current_reader: None,
            current_transport_path: None,
        };
        Py::new(py, iter)
    }

    /// Set the writer (container writer + write index) used for adding
    /// data. Registers `transport_packname` for the index if given.
    fn set_writer(
        &mut self,
        py: Python<'_>,
        writer: Py<PyAny>,
        index: Py<PyAny>,
        transport_packname: Py<PyAny>,
    ) -> PyResult<()> {
        if !index.is_none(py) {
            self.indices
                .bind(py)
                .set_item(index.bind(py), transport_packname.bind(py))?;
        }
        self.container_writer = Some(writer);
        self.write_index = Some(index);
        Ok(())
    }

    /// Try to reload, or re-raise the original exception. Returns to the
    /// caller (meaning "retry") on recovery, otherwise re-raises the
    /// error carried by `retry_exc`.
    fn reload_or_raise(&self, py: Python<'_>, retry_exc: Bound<'_, PyAny>) -> PyResult<()> {
        let reload_changed = match &self.reload_func {
            Some(f) => f.bind(py).call0()?.is_truthy()?,
            None => false,
        };
        let reload_occurred = retry_exc.getattr("reload_occurred")?.is_truthy()?;
        match reload_decision(self.reload_func.is_some(), reload_changed, reload_occurred) {
            ReloadDecision::Retry => Ok(()),
            ReloadDecision::Raise => {
                // raise retry_exc.exc_info[1]
                let exc_info = retry_exc.getattr("exc_info")?;
                let value = exc_info.get_item(1)?;
                Err(PyErr::from_value(value))
            }
        }
    }
}

/// Wrapper giving a Python index object `PartialEq` by Python equality,
/// so [`group_retrieval_requests`] can coalesce neighbours.
struct PyIndexKey(Py<PyAny>);

impl PartialEq for PyIndexKey {
    fn eq(&self, other: &Self) -> bool {
        Python::attach(|py| self.0.bind(py).eq(other.0.bind(py)).unwrap_or(false))
    }
}

/// Lazy iterator returned by `_DirectPackAccess.get_raw_records`,
/// reproducing the Python generator: for each grouped request it looks up
/// the `(transport, path)`, builds a readv reader, and yields each
/// record's bytes. Errors surface during iteration, matching the
/// generator so an enclosing retry loop can catch `RetryWithNewPacks`.
#[pyclass(module = "bzrformats._bzr_rs.pack_repo")]
struct RawRecordsIter {
    /// List of `(index, [(offset, length), ...])` request groups.
    request_lists: Py<PyList>,
    group_pos: usize,
    indices: Py<PyDict>,
    reload_func: Option<Py<PyAny>>,
    /// The `iter_records()` iterator of the current group's reader, if a
    /// group is in progress.
    current_reader: Option<Py<PyAny>>,
    /// The `(transport, path)` of the in-progress group, retained so a
    /// `NoSuchFile` raised while draining the reader can be wrapped in
    /// `RetryWithNewPacks` with the right `transport.abspath(path)`
    /// context (matching the Python `try/except TransportNoSuchFile`
    /// that spans both reader construction and iteration).
    current_transport_path: Option<(Py<PyAny>, Py<PyAny>)>,
}

#[pymethods]
impl RawRecordsIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        loop {
            // Drain the current reader, if any. A NoSuchFile here means
            // the pack went missing mid-read; wrap it like the Python
            // `try/except TransportNoSuchFile` around the iteration.
            if let Some(reader) = slf.current_reader.as_ref().map(|r| r.clone_ref(py)) {
                let step = (|| -> PyResult<Option<Py<PyAny>>> {
                    match reader.bind(py).call_method0("__next__") {
                        Ok(item) => {
                            // item is (names, read_func); call read_func(None).
                            let read_func = item.get_item(1)?;
                            let data = read_func.call1((py.None(),))?;
                            Ok(Some(data.unbind()))
                        }
                        Err(e) if e.is_instance_of::<PyStopIteration>(py) => Ok(None),
                        Err(e) => Err(e),
                    }
                })();
                match step {
                    Ok(Some(data)) => return Ok(data),
                    Ok(None) => {
                        slf.current_reader = None;
                        slf.current_transport_path = None;
                    }
                    Err(e) if is_no_such_file(py, &e) => {
                        return Err(slf.wrap_no_such_file(py, e));
                    }
                    Err(e) => return Err(e),
                }
            }

            // Advance to the next group.
            let request_lists = slf.request_lists.bind(py);
            if slf.group_pos >= request_lists.len() {
                return Err(PyStopIteration::new_err(()));
            }
            let group = request_lists.get_item(slf.group_pos)?;
            slf.group_pos += 1;
            let index = group.get_item(0)?;
            let offsets = group.get_item(1)?;

            // Look up (transport, path); a missing key means an index
            // reload dropped it -> RetryWithNewPacks(reload_occurred=True).
            let transport_packname = match slf.indices.bind(py).get_item(index.clone())? {
                Some(v) => v,
                None => {
                    let key_err = PyKeyError::new_err(());
                    if slf.reload_func.is_none() {
                        return Err(key_err);
                    }
                    return Err(make_retry(py, &index, true, &key_err));
                }
            };
            let (transport, path): (Bound<PyAny>, Bound<PyAny>) = transport_packname.extract()?;
            slf.current_transport_path = Some((transport.clone().unbind(), path.clone().unbind()));

            // Build the readv reader and start iterating. A NoSuchFile
            // raised here (the readv generator can fail lazily, and the
            // reader reads the format header on the first iter_records
            // step) means the pack went missing ->
            // RetryWithNewPacks(reload_occurred=False).
            let records_iter = (|| -> PyResult<Py<PyAny>> {
                let pack_mod = PyModule::import(py, "bzrformats.pack")?;
                let reader = pack_mod.call_method1(
                    "make_readv_reader",
                    (transport.clone(), path.clone(), offsets),
                )?;
                Ok(reader.call_method0("iter_records")?.unbind())
            })();
            let records_iter = match records_iter {
                Ok(it) => it,
                Err(e) if is_no_such_file(py, &e) => {
                    return Err(slf.wrap_no_such_file(py, e));
                }
                Err(e) => return Err(e),
            };
            slf.current_reader = Some(records_iter);
        }
    }
}

impl RawRecordsIter {
    /// Wrap a `NoSuchFile` from the in-progress group into a
    /// `RetryWithNewPacks(reload_occurred=False)` (with the original
    /// error preserved when no reload function is configured), using the
    /// current group's `transport.abspath(path)` as the context.
    fn wrap_no_such_file(&self, py: Python<'_>, err: PyErr) -> PyErr {
        if self.reload_func.is_none() {
            return err;
        }
        let Some((transport, path)) = &self.current_transport_path else {
            return err;
        };
        match transport.bind(py).call_method1("abspath", (path.bind(py),)) {
            Ok(abspath) => make_retry(py, &abspath, false, &err),
            Err(e) => e,
        }
    }
}

/// Build a `RetryWithNewPacks(context, reload_occurred=..., exc_info=...)`
/// exception value chained from `cause`.
fn make_retry(
    py: Python<'_>,
    context: &Bound<'_, PyAny>,
    reload_occurred: bool,
    cause: &PyErr,
) -> PyErr {
    let exc_info = exc_info_tuple(py, cause);
    let kwargs = PyDict::new(py);
    let _ = kwargs.set_item("reload_occurred", reload_occurred);
    let _ = kwargs.set_item("exc_info", exc_info);
    let cls = RetryWithNewPacks::type_object(py);
    match cls.call((context.clone(),), Some(&kwargs)) {
        Ok(exc) => {
            let err = PyErr::from_value(exc);
            err.set_cause(py, Some(cause.clone_ref(py)));
            err
        }
        Err(e) => e,
    }
}

/// Join an iterable of byte chunks into a single buffer, matching the
/// Python `b"".join(raw_data)`.
fn join_bytes(py: Python<'_>, raw_data: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    // Fast path: a single bytes object.
    if let Ok(b) = raw_data.cast::<PyBytes>() {
        return Ok(b.as_bytes().to_vec());
    }
    let mut out = Vec::new();
    for chunk in raw_data.try_iter()? {
        let chunk = chunk?;
        let b = chunk
            .cast_into::<PyBytes>()
            .map_err(|_| PyTypeError::new_err("raw_data chunks must be bytes"))?;
        out.extend_from_slice(b.as_bytes());
    }
    let _ = py;
    Ok(out)
}

/// `bzrformats.pack_repo.Pack` — in-memory proxy for a pack and its
/// indices. Base class for `ExistingPack`/`NewPack`. A `dict` pyclass:
/// every attribute the Python class set on `self` lives in the instance
/// `__dict__` so subclasses and Python callers can read/mutate them and
/// so `ExistingPack.__eq__` (a `__dict__` comparison) keeps working.
#[pyclass(subclass, dict, module = "bzrformats._bzr_rs.pack_repo", name = "Pack")]
pub struct Pack;

/// Store the five index objects in the instance `__dict__`, matching
/// `Pack.__init__`. Shared by `Pack` and every subclass `__init__`.
fn pack_init_indices(
    slf: &Bound<'_, PyAny>,
    revision_index: &Bound<'_, PyAny>,
    inventory_index: &Bound<'_, PyAny>,
    text_index: &Bound<'_, PyAny>,
    signature_index: &Bound<'_, PyAny>,
    chk_index: &Bound<'_, PyAny>,
) -> PyResult<()> {
    slf.setattr("revision_index", revision_index)?;
    slf.setattr("inventory_index", inventory_index)?;
    slf.setattr("text_index", text_index)?;
    slf.setattr("signature_index", signature_index)?;
    slf.setattr("chk_index", chk_index)?;
    Ok(())
}

#[pymethods]
impl Pack {
    /// `Pack.index_definitions` — class attribute mapping each index type
    /// name to its `(extension, index_sizes offset)` pair. Downstream
    /// code (e.g. breezy's `pack_repo`) iterates this for the type names,
    /// so it must stay a real mapping with the same contents as the
    /// original Python dict.
    #[classattr]
    fn index_definitions(py: Python<'_>) -> PyResult<Py<PyDict>> {
        let d = PyDict::new(py);
        for kind in [
            IndexKind::Chk,
            IndexKind::Revision,
            IndexKind::Inventory,
            IndexKind::Text,
            IndexKind::Signature,
        ] {
            let (ext, offset) = pack_repo::index_definition(kind);
            d.set_item(kind.as_name(), (ext, offset))?;
        }
        Ok(d.unbind())
    }

    #[new]
    #[pyo3(signature = (*_args, **_kwargs))]
    fn new(_args: Bound<'_, PyTuple>, _kwargs: Option<Bound<'_, PyDict>>) -> Self {
        Pack
    }

    #[pyo3(signature = (revision_index, inventory_index, text_index, signature_index, chk_index=None))]
    fn __init__(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        revision_index: Bound<'_, PyAny>,
        inventory_index: Bound<'_, PyAny>,
        text_index: Bound<'_, PyAny>,
        signature_index: Bound<'_, PyAny>,
        chk_index: Option<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let none = py.None().into_bound(py);
        let any = slf.as_any();
        pack_init_indices(
            any,
            &revision_index,
            &inventory_index,
            &text_index,
            &signature_index,
            chk_index.as_ref().unwrap_or(&none),
        )
    }

    /// Return a `(transport, name)` tuple for the pack content.
    fn access_tuple(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let transport = slf.getattr("pack_transport")?;
        let name = slf.call_method0("file_name")?;
        Ok(PyTuple::new(py, [transport, name])?.into_any().unbind())
    }

    /// The pack file name on disk: `name + ".pack"`.
    fn file_name(slf: &Bound<'_, Self>) -> PyResult<String> {
        let name: String = slf.getattr("name")?.extract()?;
        Ok(format!("{}.pack", name))
    }

    /// Number of revisions in the pack.
    fn get_revision_count(slf: &Bound<'_, Self>) -> PyResult<Py<PyAny>> {
        slf.getattr("revision_index")?
            .call_method0("key_count")
            .map(|v| v.unbind())
    }

    /// Disk name of `index_type`'s index for pack `name`.
    fn index_name(&self, index_type: &str, name: &str) -> PyResult<String> {
        Ok(rs_index_name(index_kind(index_type)?, name))
    }

    /// Position of `index_type` in the `index_sizes` array.
    fn index_offset(&self, index_type: &str) -> PyResult<usize> {
        Ok(rs_index_offset(index_kind(index_type)?))
    }

    fn inventory_index_name(slf: &Bound<'_, Self>, name: &str) -> PyResult<String> {
        slf.call_method1("index_name", ("inventory", name))?
            .extract()
    }

    fn revision_index_name(slf: &Bound<'_, Self>, name: &str) -> PyResult<String> {
        slf.call_method1("index_name", ("revision", name))?
            .extract()
    }

    fn signature_index_name(slf: &Bound<'_, Self>, name: &str) -> PyResult<String> {
        slf.call_method1("index_name", ("signature", name))?
            .extract()
    }

    fn text_index_name(slf: &Bound<'_, Self>, name: &str) -> PyResult<String> {
        slf.call_method1("index_name", ("text", name))?.extract()
    }

    /// Replace a writable index with a read-only on-disk one.
    fn _replace_index_with_readonly(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        index_type: &str,
    ) -> PyResult<()> {
        let kind = index_kind(index_type)?;
        let unlimited_cache = kind == IndexKind::Chk;
        let name: String = slf.getattr("name")?.extract()?;
        let index_name = rs_index_name(kind, &name);
        let index_transport = slf.getattr("index_transport")?;
        let index_sizes = slf.getattr("index_sizes")?;
        let size = index_sizes.get_item(rs_index_offset(kind))?;
        let index_class = slf.getattr("index_class")?;

        let kwargs = PyDict::new(py);
        kwargs.set_item("unlimited_cache", unlimited_cache)?;
        let index = index_class.call((index_transport, index_name, size), Some(&kwargs))?;
        if kind == IndexKind::Chk {
            let btree = PyModule::import(py, "bzrformats.btree_index")?;
            let factory = btree.getattr("_gcchk_factory")?;
            index.setattr("_leaf_factory", factory)?;
        }
        slf.setattr(format!("{}_index", index_type), index)?;
        Ok(())
    }

    /// Check that external delta references are present in the
    /// collection's combined text/inventory indices.
    fn _check_references(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let pack_collection = slf.getattr("_pack_collection")?;
        let missing_items = PyDict::new(py);
        let checks = [
            ("texts", slf.getattr("text_index")?, "text_index"),
            (
                "inventories",
                slf.getattr("inventory_index")?,
                "inventory_index",
            ),
        ];
        for (label, index, collection_attr) in checks {
            let external_refs = slf.call_method1("_get_external_refs", (index,))?;
            let combined = pack_collection
                .getattr(collection_attr)?
                .getattr("combined_index")?;
            // present = {k for (idx, k, v, r) in combined.iter_entries(external_refs)}
            let present = pyo3::types::PySet::empty(py)?;
            for entry in combined
                .call_method1("iter_entries", (&external_refs,))?
                .try_iter()?
            {
                let entry = entry?;
                present.add(entry.get_item(1)?)?;
            }
            let missing = external_refs.call_method1("difference", (present,))?;
            if missing.is_truthy()? {
                // sorted(missing)
                let sorted = PyList::new(py, missing.try_iter()?.collect::<PyResult<Vec<_>>>()?)?;
                sorted.sort()?;
                missing_items.set_item(label, sorted)?;
            }
        }
        if missing_items.is_truthy()? {
            let pformat = PyModule::import(py, "pprint")?.getattr("pformat")?;
            let formatted: String = pformat.call1((missing_items,))?.extract()?;
            let repr: String = slf.repr()?.extract()?;
            return Err(BzrCheckError::new_err(format!(
                "Newly created pack file {} has delta references to items not in its repository:\n{}",
                repr, formatted
            )));
        }
        Ok(())
    }

    fn __lt__(slf: &Bound<'_, Self>, other: Bound<'_, PyAny>) -> PyResult<bool> {
        pack_lt(slf.as_any(), &other)
    }

    fn __hash__(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<isize> {
        pack_hash(slf.as_any(), py)
    }
}

/// Order packs by object identity, matching `Pack.__lt__`. Only orderable
/// against another `Pack` (or subclass); anything else is a `TypeError`.
/// Shared by `Pack` and its subclasses because pyo3 compiles comparison
/// into a per-class slot that does not inherit from a Rust base.
fn pack_lt(slf: &Bound<'_, PyAny>, other: &Bound<'_, PyAny>) -> PyResult<bool> {
    if !other.is_instance_of::<Pack>() {
        return Err(PyTypeError::new_err(other.clone().unbind()));
    }
    Ok((slf.as_ptr() as usize) < (other.as_ptr() as usize))
}

/// Hash a pack by `(type, revision_index, inventory_index, text_index,
/// signature_index, chk_index)`, matching `Pack.__hash__`.
fn pack_hash(slf: &Bound<'_, PyAny>, py: Python<'_>) -> PyResult<isize> {
    let ty = slf.get_type().into_any();
    let tuple = PyTuple::new(
        py,
        [
            ty,
            slf.getattr("revision_index")?,
            slf.getattr("inventory_index")?,
            slf.getattr("text_index")?,
            slf.getattr("signature_index")?,
            slf.getattr("chk_index")?,
        ],
    )?;
    tuple.hash()
}

/// `bzrformats.pack_repo.ExistingPack` — proxy for an existing `.pack`
/// and its disk indices.
#[pyclass(extends = Pack, subclass, dict, module = "bzrformats._bzr_rs.pack_repo", name = "ExistingPack")]
pub struct ExistingPack;

#[pymethods]
impl ExistingPack {
    #[new]
    #[pyo3(signature = (*_args, **_kwargs))]
    fn new(_args: Bound<'_, PyTuple>, _kwargs: Option<Bound<'_, PyDict>>) -> (Self, Pack) {
        (ExistingPack, Pack)
    }

    #[pyo3(signature = (pack_transport, name, revision_index, inventory_index, text_index, signature_index, chk_index=None))]
    #[allow(clippy::too_many_arguments)]
    fn __init__(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        pack_transport: Bound<'_, PyAny>,
        name: Bound<'_, PyAny>,
        revision_index: Bound<'_, PyAny>,
        inventory_index: Bound<'_, PyAny>,
        text_index: Bound<'_, PyAny>,
        signature_index: Bound<'_, PyAny>,
        chk_index: Option<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let none = py.None().into_bound(py);
        let any = slf.as_any();
        pack_init_indices(
            any,
            &revision_index,
            &inventory_index,
            &text_index,
            &signature_index,
            chk_index.as_ref().unwrap_or(&none),
        )?;
        any.setattr("name", &name)?;
        any.setattr("pack_transport", &pack_transport)?;
        if revision_index.is_none()
            || inventory_index.is_none()
            || text_index.is_none()
            || signature_index.is_none()
            || name.is_none()
            || pack_transport.is_none()
        {
            return Err(PyAssertionError::new_err(()));
        }
        Ok(())
    }

    /// Equality compares the instance dicts, matching the Python
    /// `__eq__`.
    fn __eq__(slf: &Bound<'_, Self>, other: Bound<'_, PyAny>) -> PyResult<bool> {
        let self_dict = slf.getattr("__dict__")?;
        let other_dict = match other.getattr("__dict__") {
            Ok(d) => d,
            Err(_) => return Ok(false),
        };
        self_dict.eq(other_dict)
    }

    fn __ne__(slf: &Bound<'_, Self>, other: Bound<'_, PyAny>) -> PyResult<bool> {
        Ok(!Self::__eq__(slf, other)?)
    }

    // Re-declared because pyo3 compiles comparison into a per-class slot:
    // defining __eq__ here would otherwise shadow the inherited
    // Pack.__lt__ and make ExistingPack instances unorderable.
    fn __lt__(slf: &Bound<'_, Self>, other: Bound<'_, PyAny>) -> PyResult<bool> {
        pack_lt(slf.as_any(), &other)
    }

    fn __repr__(slf: &Bound<'_, Self>) -> PyResult<String> {
        let ty = slf.get_type();
        let module: String = ty.getattr("__module__")?.extract()?;
        let qualname: String = ty.getattr("__name__")?.extract()?;
        let id = slf.as_ptr() as usize;
        let pack_transport = slf.getattr("pack_transport")?.str()?;
        let name = slf.getattr("name")?.str()?;
        Ok(format!(
            "<{}.{} object at 0x{:x}, {}, {}",
            module, qualname, id, pack_transport, name
        ))
    }

    fn __hash__(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<isize> {
        let ty = slf.get_type().into_any();
        let name = slf.getattr("name")?;
        PyTuple::new(py, [ty, name])?.hash()
    }
}

/// `bzrformats.pack_repo.ResumedPack` — a pack being resumed from an
/// interrupted upload.
#[pyclass(extends = ExistingPack, subclass, dict, module = "bzrformats._bzr_rs.pack_repo", name = "ResumedPack")]
pub struct ResumedPack;

#[pymethods]
impl ResumedPack {
    #[new]
    #[pyo3(signature = (*_args, **_kwargs))]
    fn new(
        _args: Bound<'_, PyTuple>,
        _kwargs: Option<Bound<'_, PyDict>>,
    ) -> PyClassInitializer<Self> {
        PyClassInitializer::from(Pack)
            .add_subclass(ExistingPack)
            .add_subclass(ResumedPack)
    }

    #[pyo3(signature = (name, revision_index, inventory_index, text_index, signature_index, upload_transport, pack_transport, index_transport, pack_collection, chk_index=None))]
    #[allow(clippy::too_many_arguments)]
    fn __init__(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        name: Bound<'_, PyAny>,
        revision_index: Bound<'_, PyAny>,
        inventory_index: Bound<'_, PyAny>,
        text_index: Bound<'_, PyAny>,
        signature_index: Bound<'_, PyAny>,
        upload_transport: Bound<'_, PyAny>,
        pack_transport: Bound<'_, PyAny>,
        index_transport: Bound<'_, PyAny>,
        pack_collection: Bound<'_, PyAny>,
        chk_index: Option<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let any = slf.as_any();
        // ExistingPack.__init__(self, pack_transport, name, ...)
        ExistingPack::__init__(
            slf.as_super(),
            py,
            pack_transport.clone(),
            name,
            revision_index.clone(),
            inventory_index.clone(),
            text_index.clone(),
            signature_index.clone(),
            chk_index.clone(),
        )?;
        any.setattr("upload_transport", &upload_transport)?;
        any.setattr("index_transport", &index_transport)?;

        let index_sizes = PyList::new(py, [py.None(), py.None(), py.None(), py.None()])?;
        let mut indices: Vec<(&str, Bound<PyAny>)> = vec![
            ("revision", revision_index),
            ("inventory", inventory_index),
            ("text", text_index),
            ("signature", signature_index),
        ];
        if let Some(chk) = &chk_index {
            if !chk.is_none() {
                indices.push(("chk", chk.clone()));
                index_sizes.append(py.None())?;
            }
        }
        for (index_type, index) in indices {
            let offset = rs_index_offset(index_kind(index_type)?);
            index_sizes.set_item(offset, index.getattr("_size")?)?;
        }
        any.setattr("index_sizes", index_sizes)?;
        any.setattr("index_class", pack_collection.getattr("_index_class")?)?;
        any.setattr("_pack_collection", &pack_collection)?;
        any.setattr("_state", "resumed")?;
        Ok(())
    }

    /// Transport + file name for accessing the pack data, depending on
    /// resumed/finished state.
    fn access_tuple(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let state: String = slf.getattr("_state")?.extract()?;
        match state.as_str() {
            "finished" => {
                let transport = slf.getattr("pack_transport")?;
                let name = slf.call_method0("file_name")?;
                Ok(PyTuple::new(py, [transport, name])?.into_any().unbind())
            }
            "resumed" => {
                let transport = slf.getattr("upload_transport")?;
                let name = slf.call_method0("file_name")?;
                Ok(PyTuple::new(py, [transport, name])?.into_any().unbind())
            }
            other => Err(PyAssertionError::new_err(other.to_string())),
        }
    }

    /// Abort the resumed pack, deleting its files.
    fn abort(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        let upload_transport = slf.getattr("upload_transport")?;
        let file_name = slf.call_method0("file_name")?;
        upload_transport.call_method1("delete", (file_name,))?;
        let mut indices = vec![
            slf.getattr("revision_index")?,
            slf.getattr("inventory_index")?,
            slf.getattr("text_index")?,
            slf.getattr("signature_index")?,
        ];
        let chk_index = slf.getattr("chk_index")?;
        if !chk_index.is_none() {
            indices.push(chk_index);
        }
        for index in indices {
            let transport = index.getattr("_transport")?;
            let name = index.getattr("_name")?;
            transport.call_method1("delete", (name,))?;
        }
        let _ = py;
        Ok(())
    }

    /// Finish the resumed pack, moving files into place.
    fn finish(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<()> {
        slf.call_method0("_check_references")?;
        let mut index_types = vec!["revision", "inventory", "text", "signature"];
        if !slf.getattr("chk_index")?.is_none() {
            index_types.push("chk");
        }
        let name: String = slf.getattr("name")?.extract()?;
        let upload_transport = slf.getattr("upload_transport")?;
        for index_type in index_types {
            let old_name = rs_index_name(index_kind(index_type)?, &name);
            let new_name = format!("../indices/{}", old_name);
            upload_transport.call_method1("move", (&old_name, &new_name))?;
            slf.call_method1("_replace_index_with_readonly", (index_type,))?;
        }
        let file_name: String = slf.call_method0("file_name")?.extract()?;
        let new_name = format!("../packs/{}", file_name);
        upload_transport.call_method1("move", (&file_name, &new_name))?;
        slf.setattr("_state", "finished")?;
        let _ = py;
        Ok(())
    }

    /// Return compression parents referenced by this index but not in it.
    fn _get_external_refs(&self, index: Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        index
            .call_method1("external_references", (1,))
            .map(|v| v.unbind())
    }
}

/// `bzrformats.pack_repo.NewPack` — proxy for a pack being created.
#[pyclass(extends = Pack, subclass, dict, module = "bzrformats._bzr_rs.pack_repo", name = "NewPack")]
pub struct NewPack;

#[pymethods]
impl NewPack {
    #[new]
    #[pyo3(signature = (*_args, **_kwargs))]
    fn new(_args: Bound<'_, PyTuple>, _kwargs: Option<Bound<'_, PyDict>>) -> (Self, Pack) {
        (NewPack, Pack)
    }

    #[pyo3(signature = (pack_collection, upload_suffix="", file_mode=None))]
    fn __init__(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        pack_collection: Bound<'_, PyAny>,
        upload_suffix: &str,
        file_mode: Option<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let any = slf.as_any();
        let index_builder_class = pack_collection.getattr("_index_builder_class")?;

        let chk_index: Bound<PyAny> = if !pack_collection.getattr("chk_index")?.is_none() {
            let kwargs = PyDict::new(py);
            kwargs.set_item("reference_lists", 0)?;
            index_builder_class.call((), Some(&kwargs))?
        } else {
            py.None().into_bound(py)
        };

        let mk = |reference_lists: i32, key_elements: Option<i32>| -> PyResult<Bound<PyAny>> {
            let kwargs = PyDict::new(py);
            kwargs.set_item("reference_lists", reference_lists)?;
            if let Some(ke) = key_elements {
                kwargs.set_item("key_elements", ke)?;
            }
            index_builder_class.call((), Some(&kwargs))
        };
        let revision_index = mk(1, None)?;
        let inventory_index = mk(2, None)?;
        let text_index = mk(2, Some(2))?;
        let signature_index = mk(0, None)?;
        pack_init_indices(
            any,
            &revision_index,
            &inventory_index,
            &text_index,
            &signature_index,
            &chk_index,
        )?;

        any.setattr("_pack_collection", &pack_collection)?;
        any.setattr("index_class", pack_collection.getattr("_index_class")?)?;
        let upload_transport = pack_collection.getattr("_upload_transport")?;
        any.setattr("upload_transport", &upload_transport)?;
        any.setattr(
            "index_transport",
            pack_collection.getattr("_index_transport")?,
        )?;
        any.setattr(
            "pack_transport",
            pack_collection.getattr("_pack_transport")?,
        )?;
        let file_mode = file_mode.unwrap_or_else(|| py.None().into_bound(py));
        any.setattr("_file_mode", &file_mode)?;

        let hashlib = PyModule::import(py, "hashlib")?;
        let md5_kwargs = PyDict::new(py);
        md5_kwargs.set_item("usedforsecurity", false)?;
        let hash_obj = hashlib.call_method("md5", (), Some(&md5_kwargs))?;
        any.setattr("_hash", &hash_obj)?;

        any.setattr("index_sizes", py.None())?;
        any.setattr("_cache_limit", 0)?;

        let osutils = PyModule::import(py, "bzrformats.osutils")?;
        let rand: String = osutils.call_method1("rand_chars", (20,))?.extract()?;
        let random_name = format!("{}{}", rand, upload_suffix);
        any.setattr("random_name", &random_name)?;

        let time_mod = PyModule::import(py, "time")?;
        let start_time = time_mod.call_method0("time")?;
        any.setattr("start_time", &start_time)?;

        let stream_kwargs = PyDict::new(py);
        stream_kwargs.set_item("mode", &file_mode)?;
        let write_stream = upload_transport.call_method(
            "open_write_stream",
            (&random_name,),
            Some(&stream_kwargs),
        )?;
        any.setattr("write_stream", &write_stream)?;

        any.setattr(
            "_buffer",
            PyList::new(
                py,
                [
                    PyList::empty(py).into_any(),
                    0i64.into_pyobject(py)?.into_any(),
                ],
            )?,
        )?;

        // Build the _write_data closure as a bound method of the Python
        // object so it can mutate self._buffer / self._hash like the
        // original. We delegate to a Rust-defined helper method.
        let write_data = slf.getattr("_write_data_impl")?;
        any.setattr("_write_data", &write_data)?;

        let pack_mod = PyModule::import(py, "bzrformats.pack")?;
        let writer = pack_mod.getattr("ContainerWriter")?.call1((&write_data,))?;
        writer.call_method0("begin")?;
        any.setattr("_writer", &writer)?;
        any.setattr("_state", "open")?;
        any.setattr("name", py.None())?;
        Ok(())
    }

    /// Append `bytes` to the write buffer, flushing to the stream when
    /// the cache limit is exceeded or `flush` is set. This is the
    /// `_write_data` closure from the Python `NewPack.__init__`.
    #[pyo3(signature = (bytes, flush=false))]
    fn _write_data_impl(
        slf: &Bound<'_, Self>,
        bytes: Bound<'_, PyBytes>,
        flush: bool,
    ) -> PyResult<()> {
        let py = slf.py();
        let buffer = slf.getattr("_buffer")?;
        let chunks = buffer.get_item(0)?;
        chunks.call_method1("append", (&bytes,))?;
        let cur: i64 = buffer.get_item(1)?.extract()?;
        let new_len = cur + bytes.as_bytes().len() as i64;
        buffer.set_item(1, new_len)?;

        let cache_limit: i64 = slf.getattr("_cache_limit")?.extract()?;
        if new_len > cache_limit || flush {
            let joined = PyBytes::new(py, b"").call_method1("join", (&chunks,))?;
            slf.getattr("write_stream")?
                .call_method1("write", (&joined,))?;
            slf.getattr("_hash")?.call_method1("update", (&joined,))?;
            slf.setattr(
                "_buffer",
                PyList::new(
                    py,
                    [
                        PyList::empty(py).into_any(),
                        0i64.into_pyobject(py)?.into_any(),
                    ],
                )?,
            )?;
        }
        Ok(())
    }

    /// Cancel creating this pack and remove the temporary file.
    fn abort(slf: &Bound<'_, Self>) -> PyResult<()> {
        slf.setattr("_state", "aborted")?;
        slf.getattr("write_stream")?.call_method0("close")?;
        let random_name = slf.getattr("random_name")?;
        slf.getattr("upload_transport")?
            .call_method1("delete", (random_name,))?;
        Ok(())
    }

    /// Transport + file name for the pack content, depending on
    /// open/finished state.
    fn access_tuple(slf: &Bound<'_, Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let state: String = slf.getattr("_state")?.extract()?;
        match state.as_str() {
            "finished" => {
                let transport = slf.getattr("pack_transport")?;
                let name = slf.call_method0("file_name")?;
                Ok(PyTuple::new(py, [transport, name])?.into_any().unbind())
            }
            "open" => {
                let transport = slf.getattr("upload_transport")?;
                let name = slf.getattr("random_name")?;
                Ok(PyTuple::new(py, [transport, name])?.into_any().unbind())
            }
            other => Err(PyAssertionError::new_err(other.to_string())),
        }
    }

    /// True if any index has had data added.
    fn data_inserted(slf: &Bound<'_, Self>) -> PyResult<bool> {
        let count: usize = slf.call_method0("get_revision_count")?.extract()?;
        if count != 0 {
            return Ok(true);
        }
        for attr in ["inventory_index", "text_index", "signature_index"] {
            let c: usize = slf.getattr(attr)?.call_method0("key_count")?.extract()?;
            if c != 0 {
                return Ok(true);
            }
        }
        let chk_index = slf.getattr("chk_index")?;
        if !chk_index.is_none() {
            let c: usize = chk_index.call_method0("key_count")?.extract()?;
            if c != 0 {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Finalize the pack content and compute the md5 content name.
    fn finish_content(slf: &Bound<'_, Self>) -> PyResult<()> {
        if !slf.getattr("name")?.is_none() {
            return Ok(());
        }
        slf.getattr("_writer")?.call_method0("end")?;
        let buffer = slf.getattr("_buffer")?;
        let buffered: i64 = buffer.get_item(1)?.extract()?;
        if buffered != 0 {
            let py = slf.py();
            // self._write_data(b"", flush=True)
            let kwargs = PyDict::new(py);
            kwargs.set_item("flush", true)?;
            slf.call_method("_write_data", (PyBytes::new(py, b""),), Some(&kwargs))?;
        }
        let name = slf.getattr("_hash")?.call_method0("hexdigest")?;
        slf.setattr("name", name)?;
        Ok(())
    }

    /// Finish the new pack: finalize content, write indices, rename into
    /// place, and record the index sizes.
    #[pyo3(signature = (suspend=false))]
    fn finish(slf: &Bound<'_, Self>, py: Python<'_>, suspend: bool) -> PyResult<()> {
        slf.call_method0("finish_content")?;
        if !suspend {
            slf.call_method0("_check_references")?;
        }
        let index_sizes = PyList::new(py, [py.None(), py.None(), py.None(), py.None()])?;
        slf.setattr("index_sizes", &index_sizes)?;
        slf.call_method1(
            "_write_index",
            (
                "revision",
                slf.getattr("revision_index")?,
                "revision",
                suspend,
            ),
        )?;
        slf.call_method1(
            "_write_index",
            (
                "inventory",
                slf.getattr("inventory_index")?,
                "inventory",
                suspend,
            ),
        )?;
        slf.call_method1(
            "_write_index",
            ("text", slf.getattr("text_index")?, "file texts", suspend),
        )?;
        slf.call_method1(
            "_write_index",
            (
                "signature",
                slf.getattr("signature_index")?,
                "revision signatures",
                suspend,
            ),
        )?;
        let chk_index = slf.getattr("chk_index")?;
        if !chk_index.is_none() {
            index_sizes.append(py.None())?;
            slf.call_method1(
                "_write_index",
                ("chk", chk_index, "content hash bytes", suspend),
            )?;
        }
        let fdatasync = slf
            .getattr("_pack_collection")?
            .getattr("config_stack")?
            .call_method1("get", ("repository.fdatasync",))?;
        let close_kwargs = PyDict::new(py);
        close_kwargs.set_item("want_fdatasync", fdatasync)?;
        slf.getattr("write_stream")?
            .call_method("close", (), Some(&close_kwargs))?;

        let name: String = slf.getattr("name")?.extract()?;
        let mut new_name = format!("{}.pack", name);
        if !suspend {
            new_name = format!("../packs/{}", new_name);
        }
        let random_name = slf.getattr("random_name")?;
        slf.getattr("upload_transport")?
            .call_method1("move", (random_name, &new_name))?;
        slf.setattr("_state", "finished")?;
        Ok(())
    }

    /// Flush any buffered data to the write stream.
    fn flush(slf: &Bound<'_, Self>) -> PyResult<()> {
        let py = slf.py();
        let buffer = slf.getattr("_buffer")?;
        let buffered: i64 = buffer.get_item(1)?.extract()?;
        if buffered != 0 {
            let chunks = buffer.get_item(0)?;
            let joined = PyBytes::new(py, b"").call_method1("join", (chunks,))?;
            slf.getattr("write_stream")?
                .call_method1("write", (&joined,))?;
            slf.getattr("_hash")?.call_method1("update", (&joined,))?;
            slf.setattr(
                "_buffer",
                PyList::new(
                    py,
                    [
                        PyList::empty(py).into_any(),
                        0i64.into_pyobject(py)?.into_any(),
                    ],
                )?,
            )?;
        }
        Ok(())
    }

    fn _get_external_refs(&self, index: Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        index
            .call_method0("_external_references")
            .map(|v| v.unbind())
    }

    /// Set the in-memory write cache size in bytes.
    fn set_write_cache_size(slf: &Bound<'_, Self>, size: i64) -> PyResult<()> {
        slf.setattr("_cache_limit", size)
    }

    /// Serialize one index to disk and replace it with a read-only one.
    #[pyo3(signature = (index_type, index, _label, suspend=false))]
    fn _write_index(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        index_type: &str,
        index: Bound<'_, PyAny>,
        _label: &str,
        suspend: bool,
    ) -> PyResult<()> {
        let kind = index_kind(index_type)?;
        let name: String = slf.getattr("name")?.extract()?;
        let index_name = rs_index_name(kind, &name);
        let transport = if suspend {
            slf.getattr("upload_transport")?
        } else {
            slf.getattr("index_transport")?
        };
        let index_tempfile = index.call_method0("finish")?;
        let index_bytes = index_tempfile.call_method0("read")?;
        let index_bytes_len: usize = index_bytes.call_method0("__len__")?.extract()?;

        let file_mode = slf.getattr("_file_mode")?;
        let stream_kwargs = PyDict::new(py);
        stream_kwargs.set_item("mode", &file_mode)?;
        let write_stream =
            transport.call_method("open_write_stream", (&index_name,), Some(&stream_kwargs))?;
        write_stream.call_method1("write", (&index_bytes,))?;
        let fdatasync = slf
            .getattr("_pack_collection")?
            .getattr("config_stack")?
            .call_method1("get", ("repository.fdatasync",))?;
        let close_kwargs = PyDict::new(py);
        close_kwargs.set_item("want_fdatasync", fdatasync)?;
        write_stream.call_method("close", (), Some(&close_kwargs))?;

        let index_sizes = slf.getattr("index_sizes")?;
        index_sizes.set_item(rs_index_offset(kind), index_bytes_len)?;
        slf.call_method1("_replace_index_with_readonly", (index_type,))?;
        Ok(())
    }
}

pub fn _pack_repo_rs(py: Python) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "pack_repo")?;
    m.add_class::<DirectPackAccess>()?;
    m.add_class::<RawRecordsIter>()?;
    m.add_class::<Pack>()?;
    m.add_class::<ExistingPack>()?;
    m.add_class::<ResumedPack>()?;
    m.add_class::<NewPack>()?;
    let _ = pack_repo::index_extension; // referenced by tests; keep linked.
    Ok(m)
}
