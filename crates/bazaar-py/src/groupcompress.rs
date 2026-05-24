use bazaar::groupcompress::compressor::GroupCompressor;
use bazaar::versionedfile::Key;
use pyo3::exceptions::{PyKeyError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PySet, PyTuple};
use pyo3::wrap_pyfunction;
use std::borrow::Cow;
use std::convert::TryInto;

pyo3::import_exception!(bzrformats.errors, ObjectNotLocked);
pyo3::import_exception!(bzrformats.errors, ReadOnlyError);
pyo3::import_exception!(bzrformats.errors, RevisionNotPresent);

/// A [`FileRef`](bazaar::knit::FileRef) backed by a Python graph-index
/// object.
///
/// A groupcompress read-memo is `(index, start, stop)`; `index` is a
/// long-lived `BTreeGraphIndex`-like object with no custom `__eq__`, so
/// Python equality is object identity. `GcFileRef` hashes and compares by
/// that object's pointer, which agrees with how the Python `LRUSizeCache`
/// keys its read-memo tuples.
pub struct GcFileRef(Py<PyAny>);

impl GcFileRef {
    pub fn new(obj: Py<PyAny>) -> Self {
        GcFileRef(obj)
    }

    fn ptr(&self) -> usize {
        self.0.as_ptr() as usize
    }

    /// Borrow the wrapped index object.
    pub fn bind<'py>(&self, py: Python<'py>) -> Bound<'py, PyAny> {
        self.0.bind(py).clone()
    }
}

impl Clone for GcFileRef {
    fn clone(&self) -> Self {
        Python::attach(|py| GcFileRef(self.0.clone_ref(py)))
    }
}

impl std::fmt::Debug for GcFileRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "GcFileRef(0x{:x})", self.ptr())
    }
}

impl PartialEq for GcFileRef {
    fn eq(&self, other: &Self) -> bool {
        self.ptr() == other.ptr()
    }
}
impl Eq for GcFileRef {}

impl std::hash::Hash for GcFileRef {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.ptr().hash(state);
    }
}

impl PartialOrd for GcFileRef {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for GcFileRef {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.ptr().cmp(&other.ptr())
    }
}

impl bazaar::knit::FileRef for GcFileRef {
    fn placeholder() -> Self {
        Python::attach(|py| GcFileRef(py.None()))
    }
}

/// The pure-crate read-memo type with its file ref backed by a Python
/// graph-index object.
type GcReadMemo = bazaar::groupcompress::gcvf::ReadMemo<GcFileRef>;

/// Convert a Python `(index, start, stop)` read-memo tuple to [`GcReadMemo`].
fn extract_read_memo(obj: &Bound<'_, PyAny>) -> PyResult<GcReadMemo> {
    let index = obj.get_item(0)?.unbind();
    let start: u64 = obj.get_item(1)?.extract()?;
    let stop: u64 = obj.get_item(2)?.extract()?;
    Ok(GcReadMemo::new(GcFileRef::new(index), start, stop))
}

/// The `(index, start, stop)` read-memo triple from a full `index_memo`.
fn read_memo_tuple<'py>(
    py: Python<'py>,
    index_memo: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyTuple>> {
    PyTuple::new(
        py,
        [
            index_memo.get_item(0)?,
            index_memo.get_item(1)?,
            index_memo.get_item(2)?,
        ],
    )
}

/// Map a Python error to a `KnitError` for the pure-crate trait calls.
///
/// `RevisionNotPresent` keeps its identity; anything else is folded into
/// `Corrupt` carrying the message.
fn gc_err_from_py(py: Python<'_>, err: PyErr) -> bazaar::knit::KnitError {
    if err
        .get_type(py)
        .name()
        .map(|n| n == "RevisionNotPresent")
        .unwrap_or(false)
    {
        return bazaar::knit::KnitError::RevisionNotPresent(vec![]);
    }
    bazaar::knit::KnitError::Corrupt(err.to_string())
}

/// Rebuild the Python `(index, start, stop)` read-memo tuple from a typed
/// [`GcReadMemo`].
fn read_memo_to_py<'py>(py: Python<'py>, memo: &GcReadMemo) -> Bound<'py, PyTuple> {
    PyTuple::new(
        py,
        [
            memo.index.bind(py),
            memo.start.into_pyobject(py).unwrap().into_any(),
            memo.stop.into_pyobject(py).unwrap().into_any(),
        ],
    )
    .unwrap()
}

/// Adapter exposing a Python `_GCGraphIndex` as the pure [`GcIndex`] trait.
pub struct PyGcIndex(Py<PyAny>);

impl PyGcIndex {
    pub fn new(obj: Py<PyAny>) -> Self {
        PyGcIndex(obj)
    }
}

impl bazaar::groupcompress::gcvf::GcIndex for PyGcIndex {
    type F = GcFileRef;

    fn get_build_details(
        &self,
        keys: &[bazaar::groupcompress::gcvf::GcKey],
    ) -> Result<
        std::collections::HashMap<
            bazaar::groupcompress::gcvf::GcKey,
            bazaar::groupcompress::gcvf::GcBuildDetails<GcFileRef>,
        >,
        bazaar::knit::KnitError,
    > {
        Python::attach(|py| {
            let py_keys = PyList::empty(py);
            for k in keys {
                py_keys
                    .append(k.clone())
                    .map_err(|e| gc_err_from_py(py, e))?;
            }
            let result = self
                .0
                .bind(py)
                .call_method1("get_build_details", (py_keys,))
                .map_err(|e| gc_err_from_py(py, e))?
                .cast_into::<PyDict>()
                .map_err(|e| gc_err_from_py(py, e.into()))?;
            let mut out = std::collections::HashMap::new();
            for (k, details) in result.iter() {
                let key: bazaar::groupcompress::gcvf::GcKey =
                    k.extract().map_err(|e| gc_err_from_py(py, e))?;
                // details[0] is the (index, start, stop, basis_end, delta_end)
                // index_memo; details[2] is the key's parents.
                let index_memo = details.get_item(0).map_err(|e| gc_err_from_py(py, e))?;
                let read_memo =
                    extract_read_memo(&index_memo).map_err(|e| gc_err_from_py(py, e))?;
                let entry_start: u64 = index_memo
                    .get_item(3)
                    .and_then(|v| v.extract())
                    .map_err(|e| gc_err_from_py(py, e))?;
                let entry_end: u64 = index_memo
                    .get_item(4)
                    .and_then(|v| v.extract())
                    .map_err(|e| gc_err_from_py(py, e))?;
                let parents_obj = details.get_item(2).map_err(|e| gc_err_from_py(py, e))?;
                let parents: Option<Vec<bazaar::groupcompress::gcvf::GcKey>> =
                    if parents_obj.is_none() {
                        None
                    } else {
                        Some(parents_obj.extract().map_err(|e| gc_err_from_py(py, e))?)
                    };
                out.insert(
                    key,
                    bazaar::groupcompress::gcvf::GcBuildDetails {
                        index_memo: bazaar::groupcompress::gcvf::IndexMemo::new(
                            read_memo,
                            entry_start,
                            entry_end,
                        ),
                        parents,
                    },
                );
            }
            Ok(out)
        })
    }

    fn get_parent_map(
        &self,
        keys: &[bazaar::groupcompress::gcvf::GcKey],
    ) -> Result<
        std::collections::HashMap<
            bazaar::groupcompress::gcvf::GcKey,
            Vec<bazaar::groupcompress::gcvf::GcKey>,
        >,
        bazaar::knit::KnitError,
    > {
        Python::attach(|py| {
            let py_keys = PyList::empty(py);
            for k in keys {
                py_keys
                    .append(k.clone())
                    .map_err(|e| gc_err_from_py(py, e))?;
            }
            let result = self
                .0
                .bind(py)
                .call_method1("get_parent_map", (py_keys,))
                .map_err(|e| gc_err_from_py(py, e))?
                .cast_into::<PyDict>()
                .map_err(|e| gc_err_from_py(py, e.into()))?;
            let mut out = std::collections::HashMap::new();
            for (k, v) in result.iter() {
                let key = k.extract().map_err(|e| gc_err_from_py(py, e))?;
                let parents: Vec<bazaar::groupcompress::gcvf::GcKey> = if v.is_none() {
                    Vec::new()
                } else {
                    v.extract().map_err(|e| gc_err_from_py(py, e))?
                };
                out.insert(key, parents);
            }
            Ok(out)
        })
    }

    fn keys(&self) -> Result<Vec<bazaar::groupcompress::gcvf::GcKey>, bazaar::knit::KnitError> {
        Python::attach(|py| {
            let result = self
                .0
                .bind(py)
                .call_method0("keys")
                .map_err(|e| gc_err_from_py(py, e))?;
            let mut out = Vec::new();
            for k in result.try_iter().map_err(|e| gc_err_from_py(py, e))? {
                out.push(
                    k.and_then(|k| k.extract())
                        .map_err(|e| gc_err_from_py(py, e))?,
                );
            }
            Ok(out)
        })
    }

    fn has_graph(&self) -> bool {
        Python::attach(|py| {
            self.0
                .bind(py)
                .getattr("has_graph")
                .and_then(|v| v.extract())
                .unwrap_or(false)
        })
    }

    fn check_write_ok(&self) -> Result<(), bazaar::knit::KnitError> {
        Python::attach(|py| {
            self.0
                .bind(py)
                .call_method0("_check_write_ok")
                .map(|_| ())
                .map_err(|e| gc_err_from_py(py, e))
        })
    }

    fn add_records(
        &self,
        records: &[(
            bazaar::groupcompress::gcvf::GcKey,
            bazaar::groupcompress::gcvf::IndexMemo<GcFileRef>,
            Option<Vec<bazaar::groupcompress::gcvf::GcKey>>,
        )],
        random_id: bool,
    ) -> Result<(), bazaar::knit::KnitError> {
        Python::attach(|py| {
            // Each node is (key, b"block_start block_length entry_start
            // entry_end", (parents,)) -- the value layout _GCGraphIndex
            // expects.
            let nodes = PyList::empty(py);
            for (key, memo, parents) in records {
                let value = format!(
                    "{} {} {} {}",
                    memo.read_memo.start,
                    memo.read_memo.byte_length(),
                    memo.entry_start,
                    memo.entry_end
                );
                let refs = match parents {
                    Some(ps) => {
                        let parent_tuple = PyTuple::new(py, ps.iter().cloned())
                            .map_err(|e| gc_err_from_py(py, e))?;
                        PyTuple::new(py, [parent_tuple]).map_err(|e| gc_err_from_py(py, e))?
                    }
                    None => PyTuple::new(py, [py.None()]).map_err(|e| gc_err_from_py(py, e))?,
                };
                nodes
                    .append(
                        PyTuple::new(
                            py,
                            [
                                key.clone()
                                    .into_pyobject(py)
                                    .map_err(|e| gc_err_from_py(py, e))?
                                    .into_any(),
                                PyBytes::new(py, value.as_bytes()).into_any(),
                                refs.into_any(),
                            ],
                        )
                        .map_err(|e| gc_err_from_py(py, e))?,
                    )
                    .map_err(|e| gc_err_from_py(py, e))?;
            }
            let kwargs = PyDict::new(py);
            kwargs
                .set_item("random_id", random_id)
                .map_err(|e| gc_err_from_py(py, e))?;
            self.0
                .bind(py)
                .call_method("add_records", (nodes,), Some(&kwargs))
                .map(|_| ())
                .map_err(|e| gc_err_from_py(py, e))
        })
    }
}

/// Adapter exposing a Python access object as the pure [`GcAccess`] trait.
pub struct PyGcAccess(Py<PyAny>);

impl PyGcAccess {
    pub fn new(obj: Py<PyAny>) -> Self {
        PyGcAccess(obj)
    }
}

impl bazaar::groupcompress::gcvf::GcAccess for PyGcAccess {
    type F = GcFileRef;

    fn get_raw_records(
        &self,
        memos: &[GcReadMemo],
    ) -> Result<Vec<Vec<u8>>, bazaar::knit::KnitError> {
        Python::attach(|py| {
            let py_memos = PyList::empty(py);
            for m in memos {
                py_memos
                    .append(read_memo_to_py(py, m))
                    .map_err(|e| gc_err_from_py(py, e))?;
            }
            let result = self
                .0
                .bind(py)
                .call_method1("get_raw_records", (py_memos,))
                .map_err(|e| gc_err_from_py(py, e))?;
            let mut out = Vec::with_capacity(memos.len());
            for item in result.try_iter().map_err(|e| gc_err_from_py(py, e))? {
                let item = item.map_err(|e| gc_err_from_py(py, e))?;
                let bytes: Vec<u8> = item.extract().map_err(|e| gc_err_from_py(py, e))?;
                out.push(bytes);
            }
            Ok(out)
        })
    }

    fn add_raw_record(
        &self,
        size: usize,
        chunks: Vec<Vec<u8>>,
    ) -> Result<GcReadMemo, bazaar::knit::KnitError> {
        Python::attach(|py| {
            let py_chunks = PyList::empty(py);
            for c in &chunks {
                py_chunks
                    .append(PyBytes::new(py, c))
                    .map_err(|e| gc_err_from_py(py, e))?;
            }
            // add_raw_record(key, size, chunks) -> (index, start, length)
            let memo = self
                .0
                .bind(py)
                .call_method1("add_raw_record", (py.None(), size, py_chunks))
                .map_err(|e| gc_err_from_py(py, e))?;
            let index = memo.get_item(0).map_err(|e| gc_err_from_py(py, e))?;
            let start: u64 = memo
                .get_item(1)
                .and_then(|v| v.extract())
                .map_err(|e| gc_err_from_py(py, e))?;
            let length: u64 = memo
                .get_item(2)
                .and_then(|v| v.extract())
                .map_err(|e| gc_err_from_py(py, e))?;
            Ok(GcReadMemo::new(
                GcFileRef::new(index.unbind()),
                start,
                start + length,
            ))
        })
    }
}

/// `BlockCache` adapter that mirrors its contents into a Python
/// `LRUSizeCache`.
///
/// The pure store reads / writes blocks through the trait; the Python
/// cache is kept in lockstep so `vf._group_cache` (which Python callers
/// can inspect for size or `clear()` behaviour) sees the same membership.
/// Blocks are stored Rust-side as `Rc<RefCell<GroupCompressBlock>>` (the
/// shape the pure trait expects); the Python side stores a sentinel value
/// keyed by the read-memo tuple so its `len` matches.
pub struct PyBlockCache {
    /// Rust-side mirror of the cache, shared across clones via `Arc` so a
    /// `without_fallbacks` clone of the pyclass keeps the same cache as the
    /// original. `Mutex` (not `RefCell`) keeps the cache `Send + Sync` so
    /// the pyclass doesn't need `unsendable`.
    rust: std::sync::Arc<
        std::sync::Mutex<
            std::collections::HashMap<
                bazaar::groupcompress::gcvf::ReadMemo<GcFileRef>,
                bazaar::groupcompress::gcvf::SharedBlock,
            >,
        >,
    >,
    /// The Python `LRUSizeCache` (or compatible dict-like) the pyclass
    /// exposes as `_group_cache`. Shared the same way (`Py::clone_ref`).
    py_cache: Py<PyAny>,
}

impl PyBlockCache {
    pub fn new(py_cache: Py<PyAny>) -> Self {
        PyBlockCache {
            rust: std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            py_cache,
        }
    }

    /// The wrapped Python cache (`vf._group_cache`).
    pub fn py_cache(&self, py: Python<'_>) -> Py<PyAny> {
        self.py_cache.clone_ref(py)
    }
}

impl Clone for PyBlockCache {
    fn clone(&self) -> Self {
        Python::attach(|py| PyBlockCache {
            rust: std::sync::Arc::clone(&self.rust),
            py_cache: self.py_cache.clone_ref(py),
        })
    }
}

impl bazaar::groupcompress::gcvf::BlockCache<GcFileRef> for PyBlockCache {
    fn get(
        &self,
        memo: &bazaar::groupcompress::gcvf::ReadMemo<GcFileRef>,
    ) -> Option<bazaar::groupcompress::gcvf::SharedBlock> {
        self.rust.lock().unwrap().get(memo).cloned()
    }

    fn insert(
        &self,
        memo: bazaar::groupcompress::gcvf::ReadMemo<GcFileRef>,
        block: bazaar::groupcompress::gcvf::SharedBlock,
    ) {
        Python::attach(|py| {
            // Mirror into the Python cache so vf._group_cache reflects the
            // same membership. The value is a sentinel: the real block lives
            // in `self.rust`. LRUSizeCache.add takes (key, value, size).
            let key = read_memo_to_py(py, &memo);
            let size = memo.byte_length() as usize;
            let _ = self
                .py_cache
                .bind(py)
                .call_method1("add", (key, py.None(), size));
        });
        self.rust.lock().unwrap().insert(memo, block);
    }

    fn contains(&self, memo: &bazaar::groupcompress::gcvf::ReadMemo<GcFileRef>) -> bool {
        self.rust.lock().unwrap().contains_key(memo)
    }

    fn clear(&self) {
        self.rust.lock().unwrap().clear();
        Python::attach(|py| {
            let _ = self.py_cache.bind(py).call_method0("clear");
        });
    }

    fn len(&self) -> usize {
        self.rust.lock().unwrap().len()
    }
}

fn extract_key_segments(obj: &Bound<PyAny>) -> PyResult<Vec<Vec<u8>>> {
    let tuple = obj.cast::<PyTuple>().map_err(|_| {
        PyValueError::new_err("sort_gc_optimal keys and parents must be tuples of bytes")
    })?;
    let mut out = Vec::with_capacity(tuple.len());
    for item in tuple.iter() {
        let b = item
            .cast::<PyBytes>()
            .map_err(|_| PyValueError::new_err("sort_gc_optimal keys must contain only bytes"))?;
        out.push(b.as_bytes().to_vec());
    }
    Ok(out)
}

/// Sort and group the keys in `parent_map` into groupcompress order.
///
/// Returns a list of keys in reverse-topological order, grouped by the
/// first segment of each key. Single-segment keys share an empty prefix.
#[pyfunction]
fn sort_gc_optimal<'py>(
    py: Python<'py>,
    parent_map: &Bound<'py, PyDict>,
) -> PyResult<Vec<Bound<'py, PyTuple>>> {
    let mut input = Vec::with_capacity(parent_map.len());
    for (key, value) in parent_map.iter() {
        let k = extract_key_segments(&key)?;
        let parents_tuple = value
            .cast::<PyTuple>()
            .map_err(|_| PyValueError::new_err("sort_gc_optimal values must be tuples of keys"))?;
        let mut parents = Vec::with_capacity(parents_tuple.len());
        for parent in parents_tuple.iter() {
            parents.push(extract_key_segments(&parent)?);
        }
        input.push((k, parents));
    }
    let sorted = bazaar::groupcompress::sort::sort_gc_optimal(input);
    sorted
        .into_iter()
        .map(|segments| PyTuple::new(py, segments.into_iter().map(|s| PyBytes::new(py, &s))))
        .collect()
}

#[pyfunction]
fn encode_base128_int(py: Python, value: u128) -> PyResult<Bound<PyBytes>> {
    let ret = bazaar::groupcompress::delta::encode_base128_int(value);
    Ok(PyBytes::new(py, &ret))
}

#[pyfunction]
fn decode_base128_int(value: Vec<u8>) -> PyResult<(u128, usize)> {
    Ok(bazaar::groupcompress::delta::decode_base128_int(&value))
}

#[pyfunction]
fn apply_delta(py: Python, basis: Vec<u8>, delta: Vec<u8>) -> PyResult<Bound<PyBytes>> {
    bazaar::groupcompress::delta::apply_delta(&basis, &delta)
        .map_err(|e| PyErr::new::<PyValueError, _>(format!("Invalid delta: {}", e)))
        .map(|x| PyBytes::new(py, &x))
}

#[pyfunction]
fn decode_copy_instruction(data: Vec<u8>, cmd: u8, pos: usize) -> PyResult<(usize, usize, usize)> {
    let ret = bazaar::groupcompress::delta::decode_copy_instruction(&data, cmd, pos);
    if ret.is_err() {
        return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
            "Invalid copy instruction",
        ));
    }
    let ret = ret.unwrap();

    Ok((ret.0, ret.1, ret.2))
}

#[pyfunction]
#[pyo3(signature = (source, delta_start, delta_end))]
fn apply_delta_to_source<'a>(
    py: Python<'a>,
    source: &'a [u8],
    delta_start: usize,
    delta_end: usize,
) -> PyResult<Bound<'a, PyBytes>> {
    bazaar::groupcompress::delta::apply_delta_to_source(source, delta_start, delta_end)
        .map_err(|e| PyErr::new::<PyValueError, _>(format!("Invalid delta: {}", e)))
        .map(|x| PyBytes::new(py, &x))
}

#[pyfunction]
fn encode_copy_instruction(py: Python, offset: usize, length: usize) -> PyResult<Bound<PyBytes>> {
    let ret = bazaar::groupcompress::delta::encode_copy_instruction(offset, length);
    Ok(PyBytes::new(py, &ret))
}

#[pyfunction]
fn make_line_delta<'a>(
    py: Python<'a>,
    source_bytes: &'a [u8],
    target_bytes: &'a [u8],
) -> Bound<'a, PyBytes> {
    PyBytes::new(
        py,
        bazaar::groupcompress::line_delta::make_delta(source_bytes, target_bytes)
            .flat_map(|x| x.into_owned())
            .collect::<Vec<_>>()
            .as_slice(),
    )
}

#[pyfunction]
fn make_rabin_delta<'a>(
    py: Python<'a>,
    source_bytes: &'a [u8],
    target_bytes: &'a [u8],
) -> Bound<'a, PyBytes> {
    PyBytes::new(
        py,
        bazaar::groupcompress::rabin_delta::make_delta(source_bytes, target_bytes).as_slice(),
    )
}

#[pyclass]
pub struct LinesDeltaIndex(bazaar::groupcompress::line_delta::LinesDeltaIndex);

#[pymethods]
impl LinesDeltaIndex {
    #[new]
    fn new(lines: Vec<Vec<u8>>) -> Self {
        let index = bazaar::groupcompress::line_delta::LinesDeltaIndex::new(lines);
        Self(index)
    }

    #[getter]
    fn lines<'a>(&self, py: Python<'a>) -> Vec<Bound<'a, PyBytes>> {
        self.0
            .lines()
            .iter()
            .map(|x| PyBytes::new(py, x.as_ref()))
            .collect()
    }

    #[pyo3(signature = (source, bytes_length, soft = None))]
    fn make_delta<'a>(
        &'a self,
        py: Python<'a>,
        source: Vec<Vec<Vec<u8>>>,
        bytes_length: usize,
        soft: Option<bool>,
    ) -> (Vec<Bound<'a, PyBytes>>, Vec<bool>) {
        let source: Vec<Cow<[u8]>> = source
            .iter()
            .map(|x| Cow::Owned(x.iter().flatten().copied().collect::<Vec<_>>()))
            .collect::<Vec<_>>();
        let (delta, index) = self.0.make_delta(source.as_slice(), bytes_length, soft);
        (
            delta
                .into_iter()
                .map(|x| PyBytes::new(py, x.as_ref()))
                .collect(),
            index,
        )
    }

    fn extend_lines(&mut self, lines: Vec<Vec<u8>>, index: Vec<bool>) -> PyResult<()> {
        self.0.extend_lines(lines.as_slice(), index.as_slice());
        Ok(())
    }

    #[getter]
    fn endpoint(&self) -> usize {
        self.0.endpoint()
    }
}

#[pyclass]
struct GroupCompressBlock {
    inner: bazaar::groupcompress::block::GroupCompressBlock,
    /// Cached PyBytes for `_z_content`. Matches Python's semantics where
    /// `b"".join((x,))` returns `x` itself — tests do `assertIs` against
    /// the same block accessed twice.
    z_content_cache: Option<Py<PyBytes>>,
}

impl GroupCompressBlock {
    fn invalidate_cache(&mut self) {
        self.z_content_cache = None;
    }
}

#[pymethods]
impl GroupCompressBlock {
    #[new]
    fn new() -> Self {
        Self {
            inner: bazaar::groupcompress::block::GroupCompressBlock::new(),
            z_content_cache: None,
        }
    }

    fn __len__(&self) -> usize {
        self.inner.len()
    }

    #[getter]
    fn _z_content<'a>(&mut self, py: Python<'a>) -> PyResult<Bound<'a, PyBytes>> {
        if let Some(cached) = &self.z_content_cache {
            return Ok(cached.bind(py).clone());
        }
        let ret = self.inner.z_content();
        let bound = PyBytes::new(py, &ret);
        self.z_content_cache = Some(bound.clone().unbind());
        Ok(bound)
    }

    #[getter]
    fn _content<'a>(&mut self, py: Python<'a>) -> PyResult<Option<Bound<'a, PyBytes>>> {
        let ret = self.inner.content();
        Ok(ret.map(|x| PyBytes::new(py, x)))
    }

    #[getter]
    fn _content_length(&self) -> Option<usize> {
        self.inner.content_length()
    }

    #[setter(_content_length)]
    fn set_content_length_py(&mut self, value: usize) {
        self.inner.set_content_length(value);
    }

    #[getter]
    fn _z_content_length(&self) -> Option<usize> {
        self.inner.z_content_length()
    }

    #[setter(_z_content_length)]
    fn set_z_content_length_py(&mut self, value: usize) {
        self.inner.set_z_content_length(value);
    }

    #[setter(_z_content_chunks)]
    fn set_z_content_chunks_py(&mut self, chunks: Vec<Vec<u8>>) {
        self.inner.set_z_content_chunks(chunks);
        self.invalidate_cache();
    }

    /// Test probe: `None` before a streaming decompressor has been created
    /// (or after full content has been realised directly), otherwise
    /// `True`. Matches the Python class's `_z_content_decompressor` attr.
    #[getter]
    fn _z_content_decompressor(&self) -> Option<bool> {
        if self.inner.has_z_content_decompressor() {
            Some(true)
        } else {
            None
        }
    }

    #[setter(_compressor_name)]
    fn set_compressor_name_py(&mut self, name: &str) -> PyResult<()> {
        let kind = match name {
            "zlib" => bazaar::groupcompress::block::CompressorKind::Zlib,
            "lzma" => bazaar::groupcompress::block::CompressorKind::Lzma,
            other => {
                return Err(PyValueError::new_err(format!(
                    "Unknown compressor: {}",
                    other
                )));
            }
        };
        self.inner.set_compressor(kind);
        self.invalidate_cache();
        Ok(())
    }

    #[classmethod]
    fn from_bytes(_type: &pyo3::Bound<pyo3::types::PyType>, data: &[u8]) -> PyResult<Self> {
        let ret = bazaar::groupcompress::block::GroupCompressBlock::from_bytes(data);
        if ret.is_err() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Invalid block",
            ));
        }
        Ok(Self {
            inner: ret.unwrap(),
            z_content_cache: None,
        })
    }

    #[pyo3(signature = (key, start, end, sha1 = None))]
    fn extract<'a>(
        &mut self,
        py: Python<'a>,
        key: Py<PyAny>,
        start: usize,
        end: usize,
        sha1: Option<Py<PyAny>>,
    ) -> PyResult<Vec<Bound<'a, PyBytes>>> {
        let _ = key;
        let _ = sha1;
        let chunks = self
            .inner
            .extract(start, end)
            .map_err(|e| PyValueError::new_err(format!("Error during extract: {:?}", e)))?;
        Ok(chunks
            .into_iter()
            .map(|x| PyBytes::new(py, x.as_ref()))
            .collect())
    }

    fn set_chunked_content(&mut self, data: Vec<Vec<u8>>, length: usize) -> PyResult<()> {
        self.inner.set_chunked_content(data.as_slice(), length);
        self.invalidate_cache();
        Ok(())
    }

    fn set_content(&mut self, content: &[u8]) -> PyResult<()> {
        self.inner.set_content(content);
        self.invalidate_cache();
        Ok(())
    }

    #[pyo3(signature = (kind = None))]
    fn to_chunks<'a>(
        &mut self,
        py: Python<'a>,
        kind: Option<bazaar::groupcompress::block::CompressorKind>,
    ) -> (usize, Vec<Bound<'a, PyBytes>>) {
        // to_chunks may rebuild z_content_chunks internally; invalidate the
        // cached PyBytes so the next _z_content call picks up fresh bytes.
        self.invalidate_cache();
        let (size, chunks) = self.inner.to_chunks(kind);

        let chunks = chunks
            .into_iter()
            .map(|x| PyBytes::new(py, x.as_ref()))
            .collect();

        (size, chunks)
    }

    fn to_bytes<'a>(&mut self, py: Python<'a>) -> PyResult<Bound<'a, PyBytes>> {
        self.invalidate_cache();
        let ret = self.inner.to_bytes();
        Ok(PyBytes::new(py, &ret))
    }

    #[pyo3(signature = (size = None))]
    fn _ensure_content(&mut self, size: Option<usize>) -> PyResult<()> {
        self.inner
            .ensure_content(size)
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    #[pyo3(signature = (include_text = None))]
    fn _dump<'a>(
        &mut self,
        py: Python<'a>,
        include_text: Option<bool>,
    ) -> PyResult<Bound<'a, pyo3::types::PyList>> {
        use bazaar::groupcompress::block::{DeltaInfo, DumpInfo};
        use pyo3::types::{PyList, PyTuple};

        let ret = self
            .inner
            .dump(include_text)
            .map_err(|e| PyValueError::new_err(format!("Error during dump: {:?}", e)))?;

        let items: Vec<Bound<PyAny>> = ret
            .into_iter()
            .map(|info| -> PyResult<Bound<PyAny>> {
                match info {
                    DumpInfo::Fulltext { length, text } => {
                        // (b"f", length) or (b"f", length, text) when include_text.
                        let kind = PyBytes::new(py, b"f").into_any();
                        let tuple = if let Some(text) = text {
                            PyTuple::new(
                                py,
                                [
                                    kind,
                                    length.into_pyobject(py)?.into_any(),
                                    PyBytes::new(py, &text).into_any(),
                                ],
                            )?
                        } else {
                            PyTuple::new(py, [kind, length.into_pyobject(py)?.into_any()])?
                        };
                        Ok(tuple.into_any())
                    }
                    DumpInfo::Delta {
                        delta_length,
                        decomp_length,
                        instructions,
                    } => {
                        // (b"d", delta_length, decomp_length, [insts]) where each inst is
                        // (b"c", offset, length) or (b"i", length, text).
                        let inst_items: Vec<Bound<PyAny>> = instructions
                            .into_iter()
                            .map(|inst| -> PyResult<Bound<PyAny>> {
                                let tuple = match inst {
                                    DeltaInfo::Copy {
                                        offset,
                                        length,
                                        text: _,
                                    } => PyTuple::new(
                                        py,
                                        [
                                            PyBytes::new(py, b"c").into_any(),
                                            offset.into_pyobject(py)?.into_any(),
                                            length.into_pyobject(py)?.into_any(),
                                        ],
                                    )?,
                                    DeltaInfo::Insert { length, text } => {
                                        let payload = match text {
                                            Some(t) => PyBytes::new(py, &t),
                                            None => PyBytes::new(py, b""),
                                        };
                                        PyTuple::new(
                                            py,
                                            [
                                                PyBytes::new(py, b"i").into_any(),
                                                length.into_pyobject(py)?.into_any(),
                                                payload.into_any(),
                                            ],
                                        )?
                                    }
                                };
                                Ok(tuple.into_any())
                            })
                            .collect::<PyResult<_>>()?;
                        let inst_list = PyList::new(py, inst_items)?;
                        let tuple = PyTuple::new(
                            py,
                            [
                                PyBytes::new(py, b"d").into_any(),
                                delta_length.into_pyobject(py)?.into_any(),
                                decomp_length.into_pyobject(py)?.into_any(),
                                inst_list.into_any(),
                            ],
                        )?;
                        Ok(tuple.into_any())
                    }
                }
            })
            .collect::<PyResult<_>>()?;
        PyList::new(py, items)
    }
}

#[pyclass]
struct TraditionalGroupCompressor(
    Option<bazaar::groupcompress::compressor::TraditionalGroupCompressor>,
);

#[pymethods]
impl TraditionalGroupCompressor {
    #[new]
    #[allow(unused_variables)]
    #[pyo3(signature = (settings = None))]
    fn new(settings: Option<Py<PyAny>>) -> Self {
        Self(Some(
            bazaar::groupcompress::compressor::TraditionalGroupCompressor::new(),
        ))
    }

    #[getter]
    fn chunks<'a>(&self, py: Python<'a>) -> PyResult<Vec<Bound<'a, PyBytes>>> {
        if let Some(c) = self.0.as_ref() {
            Ok(c.chunks()
                .iter()
                .map(|x| PyBytes::new(py, x.as_ref()))
                .collect())
        } else {
            Err(PyRuntimeError::new_err("Compressor is already finalized"))
        }
    }

    #[getter]
    fn endpoint(&self) -> PyResult<usize> {
        if let Some(c) = self.0.as_ref() {
            Ok(c.endpoint())
        } else {
            Err(PyRuntimeError::new_err("Compressor is already finalized"))
        }
    }

    fn ratio(&self) -> PyResult<f32> {
        if let Some(c) = self.0.as_ref() {
            Ok(c.ratio())
        } else {
            Err(PyRuntimeError::new_err("Compressor is already finalized"))
        }
    }

    fn extract<'a>(
        &self,
        py: Python<'a>,
        key: Vec<Vec<u8>>,
    ) -> PyResult<(Vec<Bound<'a, PyBytes>>, Bound<'a, PyBytes>)> {
        if let Some(c) = self.0.as_ref() {
            let (data, hash) = c
                .extract(&key)
                .map_err(|e| PyValueError::new_err(format!("Error during extract: {:?}", e)))?;
            Ok((
                data.iter().map(|x| PyBytes::new(py, x.as_ref())).collect(),
                PyBytes::new(py, hash.as_bytes()),
            ))
        } else {
            Err(PyRuntimeError::new_err("Compressor is already finalized"))
        }
    }

    fn flush<'a>(&mut self, py: Python<'a>) -> PyResult<(Vec<Bound<'a, PyBytes>>, usize)> {
        if let Some(c) = self.0.take() {
            let (chunks, endpoint) = c.flush();
            Ok((
                chunks
                    .into_iter()
                    .map(|x| PyBytes::new(py, x.as_ref()))
                    .collect(),
                endpoint,
            ))
        } else {
            Err(PyRuntimeError::new_err("Compressor is already finalized"))
        }
    }

    fn flush_without_last<'a>(
        &mut self,
        py: Python<'a>,
    ) -> PyResult<(Vec<Bound<'a, PyBytes>>, usize)> {
        if let Some(c) = self.0.take() {
            let (chunks, endpoint) = c.flush_without_last();
            Ok((
                chunks
                    .into_iter()
                    .map(|x| PyBytes::new(py, x.as_ref()))
                    .collect(),
                endpoint,
            ))
        } else {
            Err(PyRuntimeError::new_err("Compressor is already finalized"))
        }
    }

    #[pyo3(signature = (key, chunks, length, expected_sha = None, nostore_sha = None, soft = None))]
    fn compress<'a>(
        &mut self,
        py: Python<'a>,
        key: Key,
        chunks: Vec<Vec<u8>>,
        length: usize,
        expected_sha: Option<String>,
        nostore_sha: Option<String>,
        soft: Option<bool>,
    ) -> PyResult<(Bound<'a, PyBytes>, usize, usize, &'a str)> {
        let chunks_l = chunks.iter().map(|x| x.as_slice()).collect::<Vec<_>>();
        if let Some(c) = self.0.as_mut() {
            c.compress(
                &key,
                chunks_l.as_slice(),
                length,
                expected_sha,
                nostore_sha,
                soft,
            )
            .map_err(|e| PyValueError::new_err(format!("Error during compress: {:?}", e)))
            .map(|(hash, size, chunks, kind)| {
                (PyBytes::new(py, hash.as_ref()), size, chunks, kind.as_str())
            })
        } else {
            Err(PyRuntimeError::new_err("Compressor is already finalized"))
        }
    }
}

#[pyclass]
struct RabinGroupCompressor(Option<bazaar::groupcompress::compressor::RabinGroupCompressor>);

fn max_bytes_from_settings(settings: Option<&Bound<PyAny>>) -> PyResult<Option<usize>> {
    let Some(settings) = settings else {
        return Ok(None);
    };
    if settings.is_none() {
        return Ok(None);
    }
    let dict = settings.cast::<pyo3::types::PyDict>().map_err(|_| {
        PyValueError::new_err("RabinGroupCompressor settings must be a dict or None")
    })?;
    let Some(value) = dict.get_item("max_bytes_to_index")? else {
        return Ok(None);
    };
    let v: usize = value.extract()?;
    Ok(if v == 0 { None } else { Some(v) })
}

impl RabinGroupCompressor {
    /// Construct a `GroupCompressBlock` Py wrapper around the compressed
    /// chunks produced by a flush. Factored out so `flush` and
    /// `flush_without_last` share the plumbing.
    fn build_block<'a>(
        py: Python<'a>,
        chunks: Vec<Vec<u8>>,
        endpoint: usize,
    ) -> PyResult<Bound<'a, GroupCompressBlock>> {
        let mut inner = bazaar::groupcompress::block::GroupCompressBlock::new();
        inner.set_chunked_content(&chunks, endpoint);
        Bound::new(
            py,
            GroupCompressBlock {
                inner,
                z_content_cache: None,
            },
        )
    }
}

#[pymethods]
impl RabinGroupCompressor {
    #[new]
    #[pyo3(signature = (settings = None))]
    fn new(settings: Option<&Bound<PyAny>>) -> PyResult<Self> {
        let max_bytes_to_index = max_bytes_from_settings(settings)?;
        Ok(Self(Some(
            bazaar::groupcompress::compressor::RabinGroupCompressor::new(max_bytes_to_index),
        )))
    }

    #[getter]
    fn chunks<'a>(&self, py: Python<'a>) -> PyResult<Vec<Bound<'a, PyBytes>>> {
        if let Some(c) = self.0.as_ref() {
            Ok(c.chunks()
                .iter()
                .map(|x| PyBytes::new(py, x.as_ref()))
                .collect())
        } else {
            Err(PyRuntimeError::new_err("Compressor is already finalized"))
        }
    }

    #[getter]
    fn endpoint(&self) -> PyResult<usize> {
        if let Some(c) = self.0.as_ref() {
            Ok(c.endpoint())
        } else {
            Err(PyRuntimeError::new_err("Compressor is already finalized"))
        }
    }

    #[getter]
    fn input_bytes(&self) -> PyResult<usize> {
        if let Some(c) = self.0.as_ref() {
            Ok(c.input_bytes())
        } else {
            Err(PyRuntimeError::new_err("Compressor is already finalized"))
        }
    }

    /// Test probe: read the underlying delta-index byte budget.
    #[getter]
    fn _max_bytes_to_index(&self) -> PyResult<usize> {
        if let Some(c) = self.0.as_ref() {
            Ok(c.max_bytes_to_index().unwrap_or(0))
        } else {
            Err(PyRuntimeError::new_err("Compressor is already finalized"))
        }
    }

    /// Map of key tuple → (start_byte, start_chunk, end_byte, end_chunk).
    #[getter]
    fn labels_deltas<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, pyo3::types::PyDict>> {
        let Some(c) = self.0.as_ref() else {
            return Err(PyRuntimeError::new_err("Compressor is already finalized"));
        };
        let dict = pyo3::types::PyDict::new(py);
        for (k, &(sb, sc, eb, ec)) in c.labels_deltas() {
            let key_tuple =
                pyo3::types::PyTuple::new(py, k.iter().map(|seg| PyBytes::new(py, seg)))?;
            dict.set_item(key_tuple, (sb, sc, eb, ec))?;
        }
        Ok(dict)
    }

    fn ratio(&self) -> PyResult<f32> {
        if let Some(c) = self.0.as_ref() {
            Ok(c.ratio())
        } else {
            Err(PyRuntimeError::new_err("Compressor is already finalized"))
        }
    }

    fn extract<'a>(
        &self,
        py: Python<'a>,
        key: Vec<Vec<u8>>,
    ) -> PyResult<(Vec<Bound<'a, PyBytes>>, Bound<'a, PyBytes>)> {
        if let Some(c) = self.0.as_ref() {
            let (data, hash) = c
                .extract(&key)
                .map_err(|e| PyValueError::new_err(format!("Error during extract: {:?}", e)))?;
            Ok((
                data.iter().map(|x| PyBytes::new(py, x.as_ref())).collect(),
                PyBytes::new(py, hash.as_bytes()),
            ))
        } else {
            Err(PyRuntimeError::new_err("Compressor is already finalized"))
        }
    }

    /// Finish this group, returning a GroupCompressBlock containing the
    /// compressed chunks.
    fn flush<'a>(&mut self, py: Python<'a>) -> PyResult<Bound<'a, GroupCompressBlock>> {
        use bazaar::groupcompress::compressor::GroupCompressor;
        let Some(c) = self.0.take() else {
            return Err(PyRuntimeError::new_err("Compressor is already finalized"));
        };
        let (chunks, endpoint) = c.flush();
        Self::build_block(py, chunks, endpoint)
    }

    fn flush_without_last<'a>(
        &mut self,
        py: Python<'a>,
    ) -> PyResult<Bound<'a, GroupCompressBlock>> {
        use bazaar::groupcompress::compressor::GroupCompressor;
        let Some(c) = self.0.take() else {
            return Err(PyRuntimeError::new_err("Compressor is already finalized"));
        };
        let (chunks, endpoint) = c.flush_without_last();
        Self::build_block(py, chunks, endpoint)
    }

    #[pyo3(signature = (key, chunks, length, expected_sha = None, nostore_sha = None, soft = None))]
    fn compress<'a>(
        &mut self,
        py: Python<'a>,
        key: Key,
        chunks: Vec<Vec<u8>>,
        length: usize,
        expected_sha: Option<Vec<u8>>,
        nostore_sha: Option<Vec<u8>>,
        soft: Option<bool>,
    ) -> PyResult<(Bound<'a, PyBytes>, usize, usize, &'a str)> {
        use bazaar::groupcompress::compressor::GroupCompressor;
        let chunks_l = chunks.iter().map(|x| x.as_slice()).collect::<Vec<_>>();
        let expected_sha = expected_sha
            .map(|b| String::from_utf8(b).map_err(|e| PyValueError::new_err(e.to_string())))
            .transpose()?;
        let nostore_sha = nostore_sha
            .map(|b| String::from_utf8(b).map_err(|e| PyValueError::new_err(e.to_string())))
            .transpose()?;
        let Some(c) = self.0.as_mut() else {
            return Err(PyRuntimeError::new_err("Compressor is already finalized"));
        };
        let (hash, size, chunks, kind) = c.compress(
            &key,
            chunks_l.as_slice(),
            length,
            expected_sha,
            nostore_sha,
            soft,
        )?;
        Ok((PyBytes::new(py, hash.as_ref()), size, chunks, kind.as_str()))
    }
}

/// Parse the outer wire framing of a groupcompress block.
///
/// Returns `(block_bytes, factories)` where `factories` is a list of
/// `(key_tuple, parents_tuple_or_none, start, end)` tuples in record order.
#[pyfunction]
fn parse_wire_header<'py>(
    py: Python<'py>,
    bytes: &'py [u8],
) -> PyResult<(Bound<'py, PyBytes>, Bound<'py, pyo3::types::PyList>)> {
    let frame = bazaar::groupcompress::wire::parse_wire(bytes)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    let block_bytes = PyBytes::new(py, frame.block_bytes);
    let mut entries: Vec<Bound<PyTuple>> = Vec::with_capacity(frame.factories.len());
    for factory in frame.factories {
        let key = PyTuple::new(py, factory.key.iter().map(|s| PyBytes::new(py, s)))?;
        let parents: Bound<PyAny> = match factory.parents {
            None => py.None().into_bound(py),
            Some(parents) => PyTuple::new(
                py,
                parents
                    .iter()
                    .map(|p| PyTuple::new(py, p.iter().map(|s| PyBytes::new(py, s))).unwrap()),
            )?
            .into_any(),
        };
        let entry = PyTuple::new(
            py,
            [
                key.into_any(),
                parents,
                factory.start.into_pyobject(py)?.into_any(),
                factory.end.into_pyobject(py)?.into_any(),
            ],
        )?;
        entries.push(entry);
    }
    let list = pyo3::types::PyList::new(py, entries)?;
    Ok((block_bytes, list))
}

/// Build the framing prefix for the wire format of a groupcompress block.
///
/// `factories` is a list of `(key_tuple, parents_tuple_or_none, start, end)`
/// tuples and `block_bytes_len` is the length of the inner block payload that
/// will be appended after the returned prefix.
#[pyfunction]
fn build_wire_prefix<'py>(
    py: Python<'py>,
    factories: &Bound<'py, pyo3::types::PyList>,
    block_bytes_len: usize,
) -> PyResult<Bound<'py, PyBytes>> {
    let mut wire_factories = Vec::with_capacity(factories.len());
    for entry in factories.iter() {
        let tuple = entry.cast_into::<PyTuple>()?;
        if tuple.len() != 4 {
            return Err(PyValueError::new_err(
                "wire factory must be (key, parents, start, end)",
            ));
        }
        let key_tuple = tuple.get_item(0)?.cast_into::<PyTuple>()?;
        let key: Vec<Vec<u8>> = key_tuple
            .iter()
            .map(|seg| {
                seg.cast_into::<PyBytes>()
                    .map(|b| b.as_bytes().to_vec())
                    .map_err(|_| PyValueError::new_err("key segments must be bytes"))
            })
            .collect::<PyResult<_>>()?;

        let parents_obj = tuple.get_item(1)?;
        let parents: Option<Vec<Vec<Vec<u8>>>> = if parents_obj.is_none() {
            None
        } else {
            let parents_tuple = parents_obj.cast_into::<PyTuple>()?;
            let mut parents = Vec::with_capacity(parents_tuple.len());
            for parent_obj in parents_tuple.iter() {
                let parent_tuple = parent_obj.cast_into::<PyTuple>()?;
                let parent: Vec<Vec<u8>> = parent_tuple
                    .iter()
                    .map(|seg| {
                        seg.cast_into::<PyBytes>()
                            .map(|b| b.as_bytes().to_vec())
                            .map_err(|_| PyValueError::new_err("parent segments must be bytes"))
                    })
                    .collect::<PyResult<_>>()?;
                parents.push(parent);
            }
            Some(parents)
        };

        let start: u64 = tuple.get_item(2)?.extract()?;
        let end: u64 = tuple.get_item(3)?.extract()?;
        wire_factories.push(bazaar::groupcompress::wire::WireFactory {
            key,
            parents,
            start,
            end,
        });
    }

    let prefix = bazaar::groupcompress::wire::build_wire_prefix(&wire_factories, block_bytes_len)
        .map_err(|e| PyValueError::new_err(format!("zlib error: {}", e)))?;
    Ok(PyBytes::new(py, &prefix))
}

/// Parse a `_GCGraphIndex` node value into its four position integers.
///
/// Returns `(start, stop, basis_end, delta_end)`. The Python original is
/// `_GCGraphIndex._node_to_position`.
#[pyfunction]
fn parse_node_position(value: &[u8]) -> PyResult<(u64, u64, u64, u64)> {
    let pos = bazaar::groupcompress::manager::parse_node_position(value)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    Ok((pos.start, pos.stop, pos.basis_end, pos.delta_end))
}

/// Decide whether a block should be repacked.
///
/// `factories` is an iterable of `(start, end)` tuples and `content_length`
/// is the uncompressed size of the block. Returns
/// `(action, last_byte_used, total_bytes_used)` where `action` is one of
/// `None`, `"trim"`, or `"rebuild"`.
#[pyfunction]
fn check_rebuild_action<'py>(
    py: Python<'py>,
    factories: Vec<(usize, usize)>,
    content_length: usize,
) -> PyResult<(Bound<'py, PyAny>, usize, usize)> {
    let (action, last, total) =
        bazaar::groupcompress::manager::check_rebuild_action(&factories, content_length);
    let action: Bound<'py, PyAny> = match action {
        bazaar::groupcompress::manager::RebuildAction::Keep => py.None().into_bound(py),
        bazaar::groupcompress::manager::RebuildAction::Trim => "trim".into_pyobject(py)?.into_any(),
        bazaar::groupcompress::manager::RebuildAction::Rebuild => {
            "rebuild".into_pyobject(py)?.into_any()
        }
    };
    Ok((action, last, total))
}

/// Decide whether a block is "well utilized" enough to leave intact.
///
/// `factories` is a list of `((start, end), prefix_bytes)` tuples where
/// `prefix_bytes` is the joined `key[:-1]` for the record (used for the
/// mixed-content heuristic).
#[pyfunction]
#[pyo3(signature = (
    factories,
    content_length,
    max_cut_fraction = 0.75,
    full_enough_block_size = 3 * 1024 * 1024,
    full_enough_mixed_block_size = 2 * 768 * 1024,
))]
fn check_is_well_utilized(
    factories: Vec<((usize, usize), Vec<u8>)>,
    content_length: usize,
    max_cut_fraction: f64,
    full_enough_block_size: usize,
    full_enough_mixed_block_size: usize,
) -> bool {
    let settings = bazaar::groupcompress::manager::WellUtilizedSettings {
        max_cut_fraction,
        full_enough_block_size,
        full_enough_mixed_block_size,
    };
    bazaar::groupcompress::manager::check_is_well_utilized(&factories, content_length, &settings)
}

#[pyfunction]
fn rabin_hash(data: Vec<u8>) -> PyResult<u32> {
    Ok(bazaar::groupcompress::rabin_delta::rabin_hash(
        data.try_into()
            .map_err(|e| PyValueError::new_err(format!("Error during rabin_hash: {:?}", e)))?,
    )
    .into())
}

/// One factory's per-record state inside a [`LazyGroupContentManager`].
///
/// Mirrors the public attributes of Python's `_LazyGroupCompressFactory` —
/// `key`, `parents`, `start`, `end`, optional cached chunks/sha1/size, and
/// the `_first` flag controlling its `storage_kind`.
#[derive(Default)]
struct FactoryState {
    key: Option<Py<PyTuple>>,
    parents: Option<Py<PyAny>>,
    start: u64,
    end: u64,
    sha1: Option<Py<PyAny>>,
    size: Option<usize>,
    chunks: Option<Vec<Py<PyBytes>>>,
    first: bool,
}

/// Rust-backed `_LazyGroupContentManager`.
///
/// Holds an inline list of [`FactoryState`]s and a `Py<GroupCompressBlock>`,
/// so the manager owns the underlying data without a Python-level reference
/// cycle. Factories are exposed as separate `LazyGroupCompressFactory`
/// pyclasses on demand; iteration breaks the back-reference exactly the same
/// way the Python original does.
#[pyclass(
    name = "LazyGroupContentManager",
    module = "bzrformats._bzr_rs.groupcompress"
)]
struct LazyGroupContentManager {
    block: Py<GroupCompressBlock>,
    factories: Vec<FactoryState>,
    last_byte: u64,
    get_settings: Option<Py<PyAny>>,
    compressor_settings: Option<Py<PyAny>>,
    /// Per-instance override for the well-utilized threshold. Tests poke at
    /// this directly to force smaller blocks to count as full.
    full_enough_block_size: usize,
    full_enough_mixed_block_size: usize,
    max_cut_fraction: f64,
}

const DEFAULT_MAX_BYTES_TO_INDEX: usize = 1024 * 1024;

const MAX_CUT_FRACTION: f64 = 0.75;
const FULL_ENOUGH_BLOCK_SIZE: usize = 3 * 1024 * 1024;
const FULL_ENOUGH_MIXED_BLOCK_SIZE: usize = 2 * 768 * 1024;

fn default_compressor_settings(py: Python) -> PyResult<Py<PyAny>> {
    let dict = PyDict::new(py);
    dict.set_item("max_bytes_to_index", DEFAULT_MAX_BYTES_TO_INDEX)?;
    Ok(dict.into_any().unbind())
}

impl LazyGroupContentManager {
    fn ensure_compressor_settings(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if let Some(settings) = &self.compressor_settings {
            return Ok(settings.clone_ref(py));
        }
        let settings = if let Some(cb) = &self.get_settings {
            let result = cb.call0(py)?;
            if result.is_none(py) {
                default_compressor_settings(py)?
            } else {
                result
            }
        } else {
            default_compressor_settings(py)?
        };
        self.compressor_settings = Some(settings.clone_ref(py));
        Ok(settings)
    }

    fn factories_for_well_utilized(&self, py: Python<'_>) -> Vec<((usize, usize), Vec<u8>)> {
        self.factories
            .iter()
            .map(|f| {
                let prefix = if let Some(key) = &f.key {
                    let key = key.bind(py);
                    let len = key.len();
                    if len <= 1 {
                        Vec::new()
                    } else {
                        let mut out = Vec::new();
                        for i in 0..len - 1 {
                            if i > 0 {
                                out.push(b'\x00');
                            }
                            if let Ok(item) = key.get_item(i) {
                                if let Ok(b) = item.cast::<PyBytes>() {
                                    out.extend_from_slice(b.as_bytes());
                                }
                            }
                        }
                        out
                    }
                } else {
                    Vec::new()
                };
                ((f.start as usize, f.end as usize), prefix)
            })
            .collect()
    }

    fn invoke_check_rebuild(&self) -> PyResult<(Py<PyAny>, usize, usize)> {
        Python::attach(|py| {
            let positions: Vec<(usize, usize)> = self
                .factories
                .iter()
                .map(|f| (f.start as usize, f.end as usize))
                .collect();
            let block = self.block.borrow(py);
            let content_length = block
                .inner
                .content_length()
                .ok_or_else(|| PyValueError::new_err("block has no content length"))?;
            drop(block);
            let (action, last, total) =
                bazaar::groupcompress::manager::check_rebuild_action(&positions, content_length);
            let action_obj: Py<PyAny> = match action {
                bazaar::groupcompress::manager::RebuildAction::Keep => py.None(),
                bazaar::groupcompress::manager::RebuildAction::Trim => {
                    "trim".into_pyobject(py)?.into_any().unbind()
                }
                bazaar::groupcompress::manager::RebuildAction::Rebuild => {
                    "rebuild".into_pyobject(py)?.into_any().unbind()
                }
            };
            Ok((action_obj, last, total))
        })
    }
}

#[pymethods]
impl LazyGroupContentManager {
    #[new]
    #[pyo3(signature = (block, get_compressor_settings = None))]
    fn new(block: Py<GroupCompressBlock>, get_compressor_settings: Option<Py<PyAny>>) -> Self {
        Self {
            block,
            factories: Vec::new(),
            last_byte: 0,
            get_settings: get_compressor_settings,
            compressor_settings: None,
            full_enough_block_size: FULL_ENOUGH_BLOCK_SIZE,
            full_enough_mixed_block_size: FULL_ENOUGH_MIXED_BLOCK_SIZE,
            max_cut_fraction: MAX_CUT_FRACTION,
        }
    }

    #[getter]
    fn _full_enough_block_size(&self) -> usize {
        self.full_enough_block_size
    }

    #[setter(_full_enough_block_size)]
    fn set_full_enough_block_size_py(&mut self, v: usize) {
        self.full_enough_block_size = v;
    }

    #[getter]
    fn _full_enough_mixed_block_size(&self) -> usize {
        self.full_enough_mixed_block_size
    }

    #[setter(_full_enough_mixed_block_size)]
    fn set_full_enough_mixed_block_size_py(&mut self, v: usize) {
        self.full_enough_mixed_block_size = v;
    }

    #[getter]
    fn _max_cut_fraction(&self) -> f64 {
        self.max_cut_fraction
    }

    #[setter(_max_cut_fraction)]
    fn set_max_cut_fraction_py(&mut self, v: f64) {
        self.max_cut_fraction = v;
    }

    fn _make_group_compressor(&mut self, py: Python<'_>) -> PyResult<Py<RabinGroupCompressor>> {
        let settings = self.ensure_compressor_settings(py)?;
        let settings_bound = settings.into_bound(py);
        let settings_ref: Option<&Bound<PyAny>> = if settings_bound.is_none() {
            None
        } else {
            Some(&settings_bound)
        };
        let inner = RabinGroupCompressor::new(settings_ref)?;
        Py::new(py, inner)
    }

    #[getter]
    fn _block(&self, py: Python<'_>) -> Py<GroupCompressBlock> {
        self.block.clone_ref(py)
    }

    /// Test probe: number of registered factories.
    #[getter]
    fn _factories<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Vec<Bound<'py, LazyGroupCompressFactory>>> {
        let n = slf.factories.len();
        let manager: Py<LazyGroupContentManager> = slf.into();
        (0..n)
            .map(|i| {
                Bound::new(
                    py,
                    LazyGroupCompressFactory {
                        manager: Some(manager.clone_ref(py)),
                        index: i,
                    },
                )
            })
            .collect()
    }

    #[getter]
    fn _last_byte(&self) -> u64 {
        self.last_byte
    }

    #[getter]
    fn _compressor_settings(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.compressor_settings.as_ref().map(|s| s.clone_ref(py))
    }

    fn _get_compressor_settings(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.ensure_compressor_settings(py)
    }

    fn add_factory(
        &mut self,
        py: Python<'_>,
        key: Py<PyAny>,
        parents: Py<PyAny>,
        start: u64,
        end: u64,
    ) -> PyResult<()> {
        let key_tuple = key.bind(py).clone().cast_into::<PyTuple>().map_err(|_| {
            PyValueError::new_err("LazyGroupContentManager.add_factory: key must be a tuple")
        })?;
        let first = self.factories.is_empty();
        if end > self.last_byte {
            self.last_byte = end;
        }
        self.factories.push(FactoryState {
            key: Some(key_tuple.unbind()),
            parents: Some(parents),
            start,
            end,
            sha1: None,
            size: None,
            chunks: None,
            first,
        });
        Ok(())
    }

    /// Iterate the factories. After yielding a factory, its back-reference to
    /// this manager is cleared (matching the Python original).
    fn get_record_stream<'py>(
        slf: PyRef<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, RecordStreamIter>> {
        let n = slf.factories.len();
        let manager: Py<LazyGroupContentManager> = slf.into();
        Bound::new(
            py,
            RecordStreamIter {
                manager: Some(manager),
                index: 0,
                len: n,
            },
        )
    }

    fn check_is_well_utilized(&self, py: Python<'_>) -> PyResult<bool> {
        if self.factories.len() == 1 {
            return Ok(false);
        }
        let factories = self.factories_for_well_utilized(py);
        let block = self.block.borrow(py);
        let content_length = block
            .inner
            .content_length()
            .ok_or_else(|| PyValueError::new_err("block has no content length"))?;
        let settings = bazaar::groupcompress::manager::WellUtilizedSettings {
            max_cut_fraction: self.max_cut_fraction,
            full_enough_block_size: self.full_enough_block_size,
            full_enough_mixed_block_size: self.full_enough_mixed_block_size,
        };
        Ok(bazaar::groupcompress::manager::check_is_well_utilized(
            &factories,
            content_length,
            &settings,
        ))
    }

    fn _check_rebuild_action<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<(Bound<'py, PyAny>, usize, usize)> {
        let (action, last, total) = self.invoke_check_rebuild()?;
        Ok((action.into_bound(py), last, total))
    }

    fn _check_rebuild_block(&mut self, py: Python<'_>) -> PyResult<()> {
        let (action, last_byte_used, _) = self.invoke_check_rebuild()?;
        let action_bound = action.into_bound(py);
        if action_bound.is_none() {
            return Ok(());
        }
        let action_str: String = action_bound.extract()?;
        match action_str.as_str() {
            "trim" => self.trim_block(py, last_byte_used),
            "rebuild" => self.rebuild_block(py),
            other => Err(PyValueError::new_err(format!(
                "unknown rebuild action: {:?}",
                other
            ))),
        }
    }

    fn _rebuild_block(&mut self, py: Python<'_>) -> PyResult<()> {
        self.rebuild_block(py)
    }

    fn _trim_block(&mut self, py: Python<'_>, last_byte: usize) -> PyResult<()> {
        self.trim_block(py, last_byte)
    }

    /// Build the over-the-wire representation of this manager, repacking the
    /// underlying block first if `_check_rebuild_block` thinks it's worth it.
    fn _wire_bytes<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        self._check_rebuild_block(py)?;
        let mut wire_factories = Vec::with_capacity(self.factories.len());
        for f in &self.factories {
            let key_tuple = f
                .key
                .as_ref()
                .ok_or_else(|| PyValueError::new_err("factory missing key"))?
                .bind(py);
            let key: Vec<Vec<u8>> = key_tuple
                .iter()
                .map(|seg| {
                    seg.cast_into::<PyBytes>()
                        .map(|b| b.as_bytes().to_vec())
                        .map_err(|_| PyValueError::new_err("key segments must be bytes"))
                })
                .collect::<PyResult<_>>()?;
            let parents_obj = f
                .parents
                .as_ref()
                .map(|p| p.clone_ref(py).into_bound(py))
                .unwrap_or_else(|| py.None().into_bound(py));
            let parents: Option<Vec<Vec<Vec<u8>>>> = if parents_obj.is_none() {
                None
            } else {
                let pt = parents_obj.cast_into::<PyTuple>()?;
                let mut parents = Vec::with_capacity(pt.len());
                for parent_obj in pt.iter() {
                    let parent_tuple = parent_obj.cast_into::<PyTuple>()?;
                    let parent: Vec<Vec<u8>> = parent_tuple
                        .iter()
                        .map(|seg| {
                            seg.cast_into::<PyBytes>()
                                .map(|b| b.as_bytes().to_vec())
                                .map_err(|_| PyValueError::new_err("parent segments must be bytes"))
                        })
                        .collect::<PyResult<_>>()?;
                    parents.push(parent);
                }
                Some(parents)
            };
            wire_factories.push(bazaar::groupcompress::wire::WireFactory {
                key,
                parents,
                start: f.start,
                end: f.end,
            });
        }
        let (block_bytes_len, block_chunks) = {
            let mut block = self.block.borrow_mut(py);
            block.to_chunks(py, None)
        };
        let prefix =
            bazaar::groupcompress::wire::build_wire_prefix(&wire_factories, block_bytes_len)
                .map_err(|e| PyValueError::new_err(format!("zlib error: {}", e)))?;
        // Concatenate prefix + chunks into a single bytes object.
        let mut out = prefix;
        for chunk in block_chunks {
            out.extend_from_slice(chunk.as_bytes());
        }
        Ok(PyBytes::new(py, &out))
    }

    /// Used by `_LazyGroupCompressFactory._extract_bytes` to make sure the
    /// inner block content has been decompressed up to `_last_byte`.
    fn _prepare_for_extract(&self, py: Python<'_>) -> PyResult<()> {
        let mut block = self.block.borrow_mut(py);
        block
            .inner
            .ensure_content(Some(self.last_byte as usize))
            .map_err(|e| PyValueError::new_err(e.to_string()))
    }

    #[classmethod]
    fn from_bytes<'py>(
        _cls: &Bound<'py, pyo3::types::PyType>,
        py: Python<'py>,
        bytes: &[u8],
    ) -> PyResult<Bound<'py, LazyGroupContentManager>> {
        let frame = bazaar::groupcompress::wire::parse_wire(bytes)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        let block_inner =
            bazaar::groupcompress::block::GroupCompressBlock::from_bytes(frame.block_bytes)
                .map_err(|e| PyValueError::new_err(format!("Invalid block: {:?}", e)))?;
        let block = Bound::new(
            py,
            GroupCompressBlock {
                inner: block_inner,
                z_content_cache: None,
            },
        )?;
        let mgr = Bound::new(
            py,
            LazyGroupContentManager {
                block: block.unbind(),
                factories: Vec::new(),
                last_byte: 0,
                get_settings: None,
                compressor_settings: None,
                full_enough_block_size: FULL_ENOUGH_BLOCK_SIZE,
                full_enough_mixed_block_size: FULL_ENOUGH_MIXED_BLOCK_SIZE,
                max_cut_fraction: MAX_CUT_FRACTION,
            },
        )?;
        {
            let mut mgr_ref = mgr.borrow_mut();
            for factory in frame.factories {
                let key_tuple = PyTuple::new(py, factory.key.iter().map(|s| PyBytes::new(py, s)))?;
                let parents: Bound<PyAny> = match factory.parents {
                    None => py.None().into_bound(py),
                    Some(parents) => PyTuple::new(
                        py,
                        parents.iter().map(|p| {
                            PyTuple::new(py, p.iter().map(|s| PyBytes::new(py, s))).unwrap()
                        }),
                    )?
                    .into_any(),
                };
                let first = mgr_ref.factories.is_empty();
                if factory.end > mgr_ref.last_byte {
                    mgr_ref.last_byte = factory.end;
                }
                mgr_ref.factories.push(FactoryState {
                    key: Some(key_tuple.unbind()),
                    parents: Some(parents.unbind()),
                    start: factory.start,
                    end: factory.end,
                    sha1: None,
                    size: None,
                    chunks: None,
                    first,
                });
            }
        }
        Ok(mgr)
    }
}

impl LazyGroupContentManager {
    /// Snapshot the wrapper's per-record state into the pure-Rust
    /// [`bazaar::groupcompress::manager::FactoryState`] form. The result has
    /// no Python references and can be passed to the pure-Rust state machine.
    fn snapshot_factory_states(
        &self,
        py: Python<'_>,
    ) -> PyResult<Vec<bazaar::groupcompress::manager::FactoryState>> {
        self.factories
            .iter()
            .map(|f| {
                let chunks = if let Some(cached) = &f.chunks {
                    Some(
                        cached
                            .iter()
                            .map(|b| b.bind(py).as_bytes().to_vec())
                            .collect::<Vec<Vec<u8>>>(),
                    )
                } else {
                    None
                };
                Ok(bazaar::groupcompress::manager::FactoryState {
                    start: f.start,
                    end: f.end,
                    sha1: None,
                    size: f.size,
                    chunks,
                    first: f.first,
                })
            })
            .collect()
    }

    /// Snapshot just the per-record key segments (in pure bytes form), used
    /// to feed [`bazaar::groupcompress::manager::rebuild_block`].
    fn snapshot_factory_keys(&self, py: Python<'_>) -> PyResult<Vec<Vec<Vec<u8>>>> {
        self.factories
            .iter()
            .map(|f| {
                let key_tuple = f
                    .key
                    .as_ref()
                    .ok_or_else(|| PyValueError::new_err("factory missing key"))?
                    .bind(py);
                key_tuple
                    .iter()
                    .map(|seg| {
                        seg.cast_into::<PyBytes>()
                            .map(|b| b.as_bytes().to_vec())
                            .map_err(|_| PyValueError::new_err("key segments must be bytes"))
                    })
                    .collect::<PyResult<Vec<Vec<u8>>>>()
            })
            .collect()
    }

    fn install_block(
        &mut self,
        py: Python<'_>,
        block: bazaar::groupcompress::block::GroupCompressBlock,
    ) -> PyResult<()> {
        self.block = Bound::new(
            py,
            GroupCompressBlock {
                inner: block,
                z_content_cache: None,
            },
        )?
        .unbind();
        Ok(())
    }

    fn trim_block(&mut self, py: Python<'_>, last_byte: usize) -> PyResult<()> {
        let new_block = {
            let mut block = self.block.borrow_mut(py);
            bazaar::groupcompress::manager::trim_block(&mut block.inner, last_byte)
                .map_err(|e| PyValueError::new_err(e.to_string()))?
        };
        self.install_block(py, new_block)
    }

    fn rebuild_block(&mut self, py: Python<'_>) -> PyResult<()> {
        // Get the compressor settings (Python side may want to lazily compute
        // them via a callback).
        let settings_obj = self.ensure_compressor_settings(py)?;
        let settings_bound = settings_obj.into_bound(py);
        let settings_ref: Option<&Bound<PyAny>> = if settings_bound.is_none() {
            None
        } else {
            Some(&settings_bound)
        };
        let max_bytes_to_index = max_bytes_from_settings(settings_ref)?;

        let keys = self.snapshot_factory_keys(py)?;
        let mut states = self.snapshot_factory_states(py)?;
        let result = {
            let mut block = self.block.borrow_mut(py);
            bazaar::groupcompress::manager::rebuild_block(
                &mut block.inner,
                &mut states,
                &keys,
                max_bytes_to_index,
            )
            .map_err(PyValueError::new_err)?
        };
        // Write the new offsets/sha1s back into the wrapper's slots.
        for (slot, state) in self.factories.iter_mut().zip(states.iter()) {
            slot.start = state.start;
            slot.end = state.end;
            slot.sha1 = state
                .sha1
                .as_ref()
                .map(|s| PyBytes::new(py, s.as_bytes()).into_any().unbind());
            slot.chunks = None;
        }
        self.last_byte = result.last_byte;
        self.install_block(py, result.block)
    }
}

/// Iterator returned by `LazyGroupContentManager.get_record_stream`.
///
/// On each `__next__` it yields a fresh [`LazyGroupCompressFactory`] view of
/// the next slot, then on the *following* call it sets that factory's manager
/// reference to `None` to break the back-pointer (matching the Python
/// original's `factory._manager = None` after `yield factory`).
#[pyclass]
struct RecordStreamIter {
    manager: Option<Py<LazyGroupContentManager>>,
    index: usize,
    len: usize,
}

#[pymethods]
impl RecordStreamIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__<'py>(
        mut slf: PyRefMut<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Option<Bound<'py, LazyGroupCompressFactory>>> {
        let Some(manager) = slf.manager.as_ref().map(|m| m.clone_ref(py)) else {
            return Ok(None);
        };
        if slf.index >= slf.len {
            slf.manager = None;
            return Ok(None);
        }
        let idx = slf.index;
        slf.index += 1;
        Bound::new(
            py,
            LazyGroupCompressFactory {
                manager: Some(manager),
                index: idx,
            },
        )
        .map(Some)
    }
}

/// Rust-backed `_LazyGroupCompressFactory`.
///
/// This is a thin view onto a slot inside [`LazyGroupContentManager`]. It
/// keeps an optional back-reference to the manager so its `get_bytes_as`
/// method can extract bytes lazily; the back-reference can be cleared from
/// Python (mirroring `factory._manager = None`).
#[pyclass(
    name = "LazyGroupCompressFactory",
    module = "bzrformats._bzr_rs.groupcompress"
)]
struct LazyGroupCompressFactory {
    manager: Option<Py<LazyGroupContentManager>>,
    index: usize,
}

impl LazyGroupCompressFactory {
    fn with_state<R, F>(&self, py: Python<'_>, f: F) -> PyResult<R>
    where
        F: FnOnce(&FactoryState) -> PyResult<R>,
    {
        let manager_py = self
            .manager
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("factory has no manager"))?;
        let manager = manager_py.borrow(py);
        let state = manager
            .factories
            .get(self.index)
            .ok_or_else(|| PyValueError::new_err("factory index out of range"))?;
        f(state)
    }

    fn with_state_mut<R, F>(&self, py: Python<'_>, f: F) -> PyResult<R>
    where
        F: FnOnce(&mut FactoryState) -> PyResult<R>,
    {
        let manager_py = self
            .manager
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("factory has no manager"))?;
        let mut manager = manager_py.borrow_mut(py);
        let index = self.index;
        let state = manager
            .factories
            .get_mut(index)
            .ok_or_else(|| PyValueError::new_err("factory index out of range"))?;
        f(state)
    }
}

#[pymethods]
impl LazyGroupCompressFactory {
    #[getter]
    fn key(&self, py: Python<'_>) -> PyResult<Py<PyTuple>> {
        self.with_state(py, |s| {
            s.key
                .as_ref()
                .map(|k| k.clone_ref(py))
                .ok_or_else(|| PyValueError::new_err("factory missing key"))
        })
    }

    #[getter]
    fn parents(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.with_state(py, |s| {
            Ok(s.parents
                .as_ref()
                .map(|p| p.clone_ref(py))
                .unwrap_or_else(|| py.None()))
        })
    }

    #[setter]
    fn set_parents(&mut self, py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        // Contract: parents is a list/tuple of revision-id keys, or None.
        // Reject anything else at the binding boundary so a typo in the
        // caller (e.g. passing an int) fails loudly here rather than
        // surfacing as a confusing AttributeError later inside reconcile.
        if !value.is_none(py) {
            let bound = value.bind(py);
            if !bound.is_instance_of::<pyo3::types::PyList>()
                && !bound.is_instance_of::<pyo3::types::PyTuple>()
            {
                return Err(pyo3::exceptions::PyTypeError::new_err(
                    "parents must be a list, tuple, or None",
                ));
            }
        }
        self.with_state_mut(py, |s| {
            s.parents = if value.is_none(py) { None } else { Some(value) };
            Ok(())
        })
    }

    #[getter]
    fn _start(&self, py: Python<'_>) -> PyResult<u64> {
        self.with_state(py, |s| Ok(s.start))
    }

    #[getter]
    fn _end(&self, py: Python<'_>) -> PyResult<u64> {
        self.with_state(py, |s| Ok(s.end))
    }

    #[getter]
    fn _first(&self, py: Python<'_>) -> PyResult<bool> {
        self.with_state(py, |s| Ok(s.first))
    }

    #[getter]
    fn sha1(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.with_state(py, |s| {
            Ok(s.sha1
                .as_ref()
                .map(|x| x.clone_ref(py))
                .unwrap_or_else(|| py.None()))
        })
    }

    #[getter]
    fn size(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.with_state(py, |s| {
            Ok(s.size
                .map(|x| x.into_pyobject(py).unwrap().into_any().unbind())
                .unwrap_or_else(|| py.None()))
        })
    }

    #[getter]
    fn storage_kind(&self, py: Python<'_>) -> PyResult<&'static str> {
        self.with_state(py, |s| {
            Ok(if s.first {
                "groupcompress-block"
            } else {
                "groupcompress-block-ref"
            })
        })
    }

    #[getter]
    fn _manager(&self, py: Python<'_>) -> Option<Py<LazyGroupContentManager>> {
        self.manager.as_ref().map(|m| m.clone_ref(py))
    }

    #[setter(_manager)]
    fn set_manager_py(&mut self, value: Option<Py<LazyGroupContentManager>>) {
        self.manager = value;
    }

    fn get_bytes_as<'py>(&mut self, py: Python<'py>, storage_kind: &str) -> PyResult<Py<PyAny>> {
        let manager_py = self
            .manager
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("factory has no manager"))?
            .clone_ref(py);

        // Determine our own storage_kind from the cached `_first` flag.
        let own_kind = {
            let manager = manager_py.borrow(py);
            let state = manager
                .factories
                .get(self.index)
                .ok_or_else(|| PyValueError::new_err("factory index out of range"))?;
            if state.first {
                "groupcompress-block"
            } else {
                "groupcompress-block-ref"
            }
        };

        if storage_kind == own_kind {
            if own_kind == "groupcompress-block" {
                // First factory → wire bytes for the whole manager.
                let mut manager = manager_py.borrow_mut(py);
                let bound = manager._wire_bytes(py)?;
                return Ok(bound.into_any().unbind());
            } else {
                return Ok(PyBytes::new(py, b"").into_any().unbind());
            }
        }
        if !matches!(storage_kind, "fulltext" | "chunked" | "lines") {
            return Err(unavailable_representation(
                py,
                &manager_py,
                self.index,
                storage_kind,
                own_kind,
            )?);
        }

        // Make sure the chunks have been extracted.
        let chunks = self.ensure_chunks(py, &manager_py)?;

        match storage_kind {
            "fulltext" => {
                let mut all = Vec::new();
                for c in &chunks {
                    all.extend_from_slice(c.bind(py).as_bytes());
                }
                Ok(PyBytes::new(py, &all).into_any().unbind())
            }
            "chunked" => {
                let list =
                    pyo3::types::PyList::new(py, chunks.into_iter().map(|c| c.into_bound(py)))?;
                Ok(list.into_any().unbind())
            }
            "lines" => {
                let raw: Vec<Vec<u8>> = chunks
                    .iter()
                    .map(|c| c.bind(py).as_bytes().to_vec())
                    .collect();
                let lines: Vec<Vec<u8>> = bazaar::osutils::chunks_to_lines(
                    raw.into_iter().map(Ok::<_, std::convert::Infallible>),
                )
                .map(|r| r.unwrap().into_owned())
                .collect();
                Ok(
                    pyo3::types::PyList::new(py, lines.iter().map(|l| PyBytes::new(py, l)))?
                        .into_any()
                        .unbind(),
                )
            }
            _ => unreachable!(),
        }
    }

    fn iter_bytes_as<'py>(&mut self, py: Python<'py>, storage_kind: &str) -> PyResult<Py<PyAny>> {
        let manager_py = self
            .manager
            .as_ref()
            .ok_or_else(|| PyValueError::new_err("factory has no manager"))?
            .clone_ref(py);
        let chunks = self.ensure_chunks(py, &manager_py)?;
        match storage_kind {
            "chunked" => {
                let list =
                    pyo3::types::PyList::new(py, chunks.into_iter().map(|c| c.into_bound(py)))?;
                Ok(list.try_iter()?.unbind().into())
            }
            "lines" => {
                let raw: Vec<Vec<u8>> = chunks
                    .iter()
                    .map(|c| c.bind(py).as_bytes().to_vec())
                    .collect();
                let lines: Vec<Vec<u8>> = bazaar::osutils::chunks_to_lines(
                    raw.into_iter().map(Ok::<_, std::convert::Infallible>),
                )
                .map(|r| r.unwrap().into_owned())
                .collect();
                let list = pyo3::types::PyList::new(py, lines.iter().map(|l| PyBytes::new(py, l)))?;
                Ok(list.try_iter()?.unbind().into())
            }
            _ => Err(unavailable_representation(
                py,
                &manager_py,
                self.index,
                storage_kind,
                "groupcompress-block",
            )?),
        }
    }
}

impl LazyGroupCompressFactory {
    fn ensure_chunks(
        &self,
        py: Python<'_>,
        manager_py: &Py<LazyGroupContentManager>,
    ) -> PyResult<Vec<Py<PyBytes>>> {
        // Try the cached chunks first.
        {
            let manager = manager_py.borrow(py);
            let state = manager
                .factories
                .get(self.index)
                .ok_or_else(|| PyValueError::new_err("factory index out of range"))?;
            if let Some(c) = &state.chunks {
                return Ok(c.iter().map(|x| x.clone_ref(py)).collect());
            }
        }
        // Extract from the block. _prepare_for_extract first.
        {
            let manager = manager_py.borrow(py);
            manager._prepare_for_extract(py)?;
        }
        let chunks = {
            let manager = manager_py.borrow(py);
            let state = manager
                .factories
                .get(self.index)
                .ok_or_else(|| PyValueError::new_err("factory index out of range"))?;
            let start = state.start as usize;
            let end = state.end as usize;
            let _ = state;
            let mut block = manager.block.borrow_mut(py);
            block
                .inner
                .extract(start, end)
                .map_err(|e| {
                    let msg = format!("zlib: {:?}", e);
                    let dc = py
                        .import("bzrformats.groupcompress")
                        .and_then(|m| m.getattr("DecompressCorruption"))
                        .ok();
                    if let Some(cls) = dc {
                        let exc = cls.call1((msg.clone(),)).unwrap();
                        PyErr::from_value(exc)
                    } else {
                        PyValueError::new_err(msg)
                    }
                })?
                .into_iter()
                .map(|c| PyBytes::new(py, &c).unbind())
                .collect::<Vec<_>>()
        };
        // Store back on the state.
        {
            let mut manager = manager_py.borrow_mut(py);
            manager.factories[self.index].chunks =
                Some(chunks.iter().map(|c| c.clone_ref(py)).collect());
        }
        Ok(chunks)
    }
}

fn unavailable_representation(
    py: Python<'_>,
    manager_py: &Py<LazyGroupContentManager>,
    index: usize,
    requested: &str,
    own_kind: &str,
) -> PyResult<PyErr> {
    let key: Py<PyAny> = {
        let manager = manager_py.borrow(py);
        let state = manager
            .factories
            .get(index)
            .ok_or_else(|| PyValueError::new_err("factory index out of range"))?;
        match &state.key {
            Some(k) => k.clone_ref(py).into_any(),
            None => py.None(),
        }
    };
    let cls = py
        .import("bzrformats.versionedfile")?
        .getattr("UnavailableRepresentation")?;
    let exc = cls.call1((key, requested, own_kind))?;
    Ok(PyErr::from_value(exc))
}

/// Rust-backed `_GCBuildDetails`.
///
/// A tuple-like record holding a parent key list plus a 5-tuple index memo
/// `(index, group_start, group_end, basis_end, delta_end)`. `compression_parent`
/// is always `None` and `method` is always `"group"`, so `__getitem__` exposes
/// the 4-tuple `(index_memo, None, parents, ("group", None))`.
#[pyclass(name = "GCBuildDetails", module = "bzrformats._bzr_rs.groupcompress")]
struct GCBuildDetails {
    parents: Py<PyAny>,
    index: Py<PyAny>,
    group_start: u64,
    group_end: u64,
    basis_end: u64,
    delta_end: u64,
}

#[pymethods]
impl GCBuildDetails {
    #[new]
    fn new(parents: Py<PyAny>, position_info: &Bound<'_, PyAny>) -> PyResult<Self> {
        let tup: (Py<PyAny>, u64, u64, u64, u64) = position_info.extract()?;
        Ok(Self {
            parents,
            index: tup.0,
            group_start: tup.1,
            group_end: tup.2,
            basis_end: tup.3,
            delta_end: tup.4,
        })
    }

    #[classattr]
    fn method(py: Python<'_>) -> Py<PyAny> {
        pyo3::types::PyString::new(py, "group").into_any().unbind()
    }

    #[classattr]
    fn compression_parent(py: Python<'_>) -> Py<PyAny> {
        py.None()
    }

    #[getter]
    fn index_memo<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        PyTuple::new(
            py,
            [
                self.index.clone_ref(py).into_bound(py),
                self.group_start.into_pyobject(py)?.into_any(),
                self.group_end.into_pyobject(py)?.into_any(),
                self.basis_end.into_pyobject(py)?.into_any(),
                self.delta_end.into_pyobject(py)?.into_any(),
            ],
        )
    }

    #[getter]
    fn record_details<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        PyTuple::new(
            py,
            [
                pyo3::types::PyString::new(py, "group").into_any(),
                py.None().into_bound(py),
            ],
        )
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let memo = self.index_memo(py)?;
        let parents = self.parents.bind(py);
        Ok(format!(
            "_GCBuildDetails({}, {})",
            memo.repr()?.to_str()?,
            parents.repr()?.to_str()?
        ))
    }

    fn __len__(&self) -> usize {
        4
    }

    fn __getitem__<'py>(&self, py: Python<'py>, offset: isize) -> PyResult<Bound<'py, PyAny>> {
        match offset {
            0 => Ok(self.index_memo(py)?.into_any()),
            1 => Ok(py.None().into_bound(py)),
            2 => Ok(self.parents.clone_ref(py).into_bound(py)),
            3 => Ok(self.record_details(py)?.into_any()),
            _ => Err(pyo3::exceptions::PyIndexError::new_err(
                "offset out of range",
            )),
        }
    }
}

/// Mapper from `GroupCompressVersionedFiles` needs into `GraphIndex` storage.
///
/// Mirrors `bzrformats.groupcompress._GCGraphIndex`.
#[pyclass(name = "_GCGraphIndex")]
struct GCGraphIndex {
    graph_index: Py<PyAny>,
    is_locked: Py<PyAny>,
    parents: bool,
    add_callback: Option<Py<PyAny>>,
    inconsistency_fatal: bool,
    /// Integer cache for group start/stop values (avoids duplicate int objects).
    int_cache: std::collections::HashMap<u64, u64>,
    /// Optional external-parent-ref tracker.
    key_dependencies: Option<Py<crate::versionedfile::KeyRefs>>,
}

#[pymethods]
impl GCGraphIndex {
    #[new]
    #[pyo3(signature = (
        graph_index,
        is_locked,
        parents = true,
        add_callback = None,
        track_external_parent_refs = false,
        inconsistency_fatal = true,
        track_new_keys = false,
    ))]
    fn new(
        py: Python<'_>,
        graph_index: Bound<'_, PyAny>,
        is_locked: Bound<'_, PyAny>,
        parents: bool,
        add_callback: Option<Bound<'_, PyAny>>,
        track_external_parent_refs: bool,
        inconsistency_fatal: bool,
        track_new_keys: bool,
    ) -> PyResult<Self> {
        let key_dependencies = if track_external_parent_refs {
            let kr = crate::versionedfile::KeyRefs::new_rust(py, track_new_keys)?;
            Some(Py::new(py, kr)?)
        } else {
            None
        };
        Ok(Self {
            graph_index: graph_index.unbind(),
            is_locked: is_locked.unbind(),
            parents,
            add_callback: add_callback.map(|c| c.unbind()),
            inconsistency_fatal,
            int_cache: std::collections::HashMap::new(),
            key_dependencies,
        })
    }

    #[getter]
    fn has_graph(&self) -> bool {
        self.parents
    }

    #[getter]
    fn _graph_index(&self, py: Python<'_>) -> Py<PyAny> {
        self.graph_index.clone_ref(py)
    }

    #[getter]
    fn _int_cache(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let d = PyDict::new(py);
        for (k, v) in &self.int_cache {
            d.set_item(k, v)?;
        }
        Ok(d.unbind())
    }

    #[getter]
    fn _key_dependencies(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match &self.key_dependencies {
            Some(kd) => Ok(kd.clone_ref(py).into_any()),
            None => Ok(py.None()),
        }
    }

    /// Public alias for `_key_dependencies`. Mirrors the Python
    /// `_GCGraphIndex.key_dependencies` property that breezy reads
    /// directly when materialising missing-parent reports.
    #[getter]
    fn key_dependencies(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self._key_dependencies(py)
    }

    /// Reset the recorded parent references. No-op when the index was
    /// built without `track_external_parent_refs=True`.
    fn clear_key_dependencies(&self, py: Python<'_>) -> PyResult<()> {
        if let Some(kd) = &self.key_dependencies {
            kd.bind(py).call_method0("clear")?;
        }
        Ok(())
    }

    /// Add-records callback exposed for read access. `None` if the index
    /// was constructed without one (i.e. read-only).
    #[getter]
    fn _add_callback(&self, py: Python<'_>) -> Py<PyAny> {
        self.add_callback
            .as_ref()
            .map(|c| c.clone_ref(py))
            .unwrap_or_else(|| py.None())
    }

    /// Install or replace the add-records callback after construction.
    /// Mirrors the Python `_GCGraphIndex.set_add_callback`.
    fn set_add_callback(&mut self, callback: Option<Bound<'_, PyAny>>) {
        self.add_callback = callback.map(|c| c.unbind());
    }

    /// Whether duplicate-with-different-details adds raise instead of
    /// warning. Mirrors the Python `_GCGraphIndex._inconsistency_fatal`.
    #[getter]
    fn _inconsistency_fatal(&self) -> bool {
        self.inconsistency_fatal
    }

    fn _check_read(&self, py: Python<'_>) -> PyResult<()> {
        if !self.is_locked.bind(py).call0()?.is_truthy()? {
            return Err(ObjectNotLocked::new_err((py.None(),)));
        }
        Ok(())
    }

    fn _check_write_ok(&self, py: Python<'_>) -> PyResult<()> {
        if self.add_callback.is_none() {
            return Err(ReadOnlyError::new_err(py.None()));
        }
        self._check_read(py)
    }

    fn keys(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        self._check_read(py)?;
        let entries = self.graph_index.bind(py).call_method0("iter_all_entries")?;
        let result = PyList::empty(py);
        for entry in entries.try_iter()? {
            result.append(entry?.get_item(1)?)?;
        }
        Ok(result.unbind())
    }

    fn get_parent_map<'py>(
        &self,
        py: Python<'py>,
        keys: Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyDict>> {
        self._check_read(py)?;
        let result = PyDict::new(py);
        let nodes = self._get_entries(py, keys)?;
        if self.parents {
            for node in nodes.try_iter()? {
                let node = node?;
                let key = node.get_item(1)?;
                let parents = node.get_item(3)?.get_item(0)?;
                result.set_item(key, parents)?;
            }
        } else {
            for node in nodes.try_iter()? {
                let key = node?.get_item(1)?;
                result.set_item(key, py.None())?;
            }
        }
        Ok(result)
    }

    fn get_build_details<'py>(
        &mut self,
        py: Python<'py>,
        keys: Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyDict>> {
        self._check_read(py)?;
        let result = PyDict::new(py);
        let entries = self._get_entries(py, keys)?;
        for entry in entries.try_iter()? {
            let entry = entry?;
            let key = entry.get_item(1)?;
            let parents = if self.parents {
                entry.get_item(3)?.get_item(0)?.unbind()
            } else {
                py.None()
            };
            let position = self._node_to_position(&entry)?;
            let details = GCBuildDetails {
                parents,
                index: position.0,
                group_start: position.1,
                group_end: position.2,
                basis_end: position.3,
                delta_end: position.4,
            };
            result.set_item(key, Py::new(py, details)?)?;
        }
        Ok(result)
    }

    fn find_ancestry<'py>(
        &self,
        py: Python<'py>,
        keys: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.graph_index
            .bind(py)
            .call_method1("find_ancestry", (keys, 0i32))
    }

    fn get_missing_parents<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PySet>> {
        let kd = self.key_dependencies.as_ref().ok_or_else(|| {
            PyValueError::new_err("get_missing_parents called without key_dependencies")
        })?;
        let kd = kd.bind(py).borrow();
        let unsatisfied = kd.get_unsatisfied_refs_rust(py)?;
        let parent_map = self.get_parent_map(py, unsatisfied)?;
        kd.satisfy_refs_for_keys_rust(py, parent_map.into_any())?;
        let refs = kd.get_unsatisfied_refs_rust(py)?;
        let out = PySet::empty(py)?;
        for key in refs.try_iter()? {
            out.add(key?)?;
        }
        Ok(out)
    }

    #[pyo3(signature = (records, random_id = false))]
    fn add_records(
        &mut self,
        py: Python<'_>,
        records: Bound<'_, PyAny>,
        random_id: bool,
    ) -> PyResult<()> {
        let add_callback = self
            .add_callback
            .as_ref()
            .ok_or_else(|| ReadOnlyError::new_err(py.None()))?;

        // Collect into a dict: key -> (value, refs)
        let keys_map = PyDict::new(py);
        let mut changed = false;
        for record in records.try_iter()? {
            let record = record?;
            let key = record.get_item(0)?;
            let value = record.get_item(1)?;
            let refs = record.get_item(2)?;

            // For parentless index, strip non-empty refs.
            if !self.parents {
                let has_refs = refs.try_iter()?.any(|r| {
                    r.as_ref()
                        .map(|r| r.is_truthy().unwrap_or(false))
                        .unwrap_or(false)
                });
                if has_refs {
                    pyo3::import_exception!(bzrformats.knit, KnitCorrupt);
                    return Err(KnitCorrupt::new_err((
                        py.None(),
                        "attempt to add node with parents in parentless index.",
                    )));
                }
                changed = true;
                keys_map.set_item(key, (value, PyTuple::empty(py)))?;
            } else {
                keys_map.set_item(key, (value, refs))?;
            }
        }

        // Check for duplicates if not random_id.
        if !random_id {
            let present = self._get_entries(py, keys_map.call_method0("keys")?)?;
            for node in present.try_iter()? {
                let node = node?;
                let key = node.get_item(1)?;
                let existing_value = node.get_item(2)?;
                let existing_refs = if self.parents {
                    node.get_item(3)?
                } else {
                    PyTuple::empty(py).into_any()
                };

                let entry = keys_map.get_item(&key)?.unwrap();
                let passed_refs = entry.get_item(1)?;

                // Compare refs as nested tuples.
                let passed_as_tuples = as_tuples(py, &passed_refs)?;
                let existing_as_tuples = as_tuples(py, &existing_refs)?;
                if !existing_as_tuples.eq(&passed_as_tuples)? {
                    // Match Python: f"{key} {value, node_refs} {passed}"
                    let existing_pair =
                        PyTuple::new(py, [existing_value.clone(), existing_refs.clone()])?;
                    let details = format!(
                        "{} {} {}",
                        key.repr()?.to_str()?,
                        existing_pair.repr()?.to_str()?,
                        entry.repr()?.to_str()?,
                    );
                    if self.inconsistency_fatal {
                        pyo3::import_exception!(bzrformats.knit, KnitCorrupt);
                        return Err(KnitCorrupt::new_err((
                            py.None(),
                            format!("inconsistent details in add_records: {}", details),
                        )));
                    } else {
                        // Log warning and skip.
                        let logging = py.import("logging")?;
                        let logger =
                            logging.call_method1("getLogger", ("bzrformats.groupcompress",))?;
                        logger.call_method1(
                            "warning",
                            (format!(
                                "inconsistent details in skipped record: {}",
                                details
                            ),),
                        )?;
                    }
                }
                keys_map.del_item(key)?;
                changed = true;
            }
        }

        // Build the records list for the callback.
        let result = PyList::empty(py);
        if self.parents {
            for (key, entry) in keys_map.iter() {
                let value = entry.get_item(0)?;
                let refs = entry.get_item(1)?;
                result.append(PyTuple::new(py, [key, value, refs])?)?;
            }
        } else {
            // Parentless: always emit 2-tuples.
            changed = true;
            for (key, entry) in keys_map.iter() {
                let value = entry.get_item(0)?;
                result.append(PyTuple::new(py, [key, value])?)?;
            }
        }

        // Update key_dependencies.
        if let Some(kd) = &self.key_dependencies {
            let kd = kd.bind(py).borrow();
            if self.parents {
                for item in result.iter() {
                    let item: Bound<'_, PyAny> = item;
                    let key = item.get_item(0)?;
                    let refs = item.get_item(2)?;
                    let parents = refs.get_item(0)?;
                    kd.add_references_rust(py, key, parents)?;
                }
            } else {
                for item in result.iter() {
                    let item: Bound<'_, PyAny> = item;
                    let key = item.get_item(0)?;
                    kd.add_key_rust(py, key)?;
                }
            }
        }

        let records_to_add = if changed {
            result.into_any()
        } else {
            // Re-use original records — they haven't changed shape.
            // (In practice `changed` is always true for parentless or when
            // duplicates were dropped; when false we can pass result directly
            // since we built it identically.)
            result.into_any()
        };

        add_callback.call1(py, (records_to_add,))?;
        Ok(())
    }

    fn scan_unvalidated_index(
        &self,
        py: Python<'_>,
        graph_index: Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let kd = match &self.key_dependencies {
            Some(kd) => kd,
            None => return Ok(()),
        };
        let entries = graph_index.call_method0("iter_all_entries")?;
        let kd = kd.bind(py).borrow();
        for node in entries.try_iter()? {
            let node = node?;
            let key = node.get_item(1)?;
            let refs = node.get_item(3)?;
            let parents = refs.get_item(0)?;
            kd.add_references_rust(py, key, parents)?;
        }
        Ok(())
    }
}

impl GCGraphIndex {
    /// Convert an index entry to its `(index, group_start, group_end, basis_end, delta_end)` tuple.
    fn _node_to_position(
        &mut self,
        node: &Bound<'_, PyAny>,
    ) -> PyResult<(Py<PyAny>, u64, u64, u64, u64)> {
        let value: Vec<u8> = node.get_item(2)?.extract::<Vec<u8>>()?;
        let pos = bazaar::groupcompress::manager::parse_node_position(&value)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        // Cache start and stop to avoid duplicate int objects.
        let start = *self.int_cache.entry(pos.start).or_insert(pos.start);
        let stop = *self.int_cache.entry(pos.stop).or_insert(pos.stop);
        let index = node.get_item(0)?.unbind();
        Ok((index, start, stop, pos.basis_end, pos.delta_end))
    }

    /// Collect entries from the underlying graph_index for `keys`.
    /// When `parents` is false, adapts output to include an empty refs tuple.
    fn _get_entries<'py>(
        &self,
        py: Python<'py>,
        keys: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        let iter = self
            .graph_index
            .bind(py)
            .call_method1("iter_entries", (keys,))?;
        let result = PyList::empty(py);
        if self.parents {
            for node in iter.try_iter()? {
                result.append(node?)?;
            }
        } else {
            for node in iter.try_iter()? {
                let node = node?;
                let idx = node.get_item(0)?;
                let key = node.get_item(1)?;
                let val = node.get_item(2)?;
                result.append(PyTuple::new(
                    py,
                    [idx, key, val, PyTuple::empty(py).into_any()],
                )?)?;
            }
        }
        Ok(result)
    }
}

/// Recursively convert `obj` to nested plain Python tuples.
fn as_tuples<'py>(py: Python<'py>, obj: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    if let Ok(seq) = obj.try_iter() {
        let items: Vec<Bound<'py, PyAny>> = seq
            .map(|r| r.and_then(|item| as_tuples(py, &item)))
            .collect::<PyResult<_>>()?;
        Ok(PyTuple::new(py, items)?.into_any())
    } else {
        Ok(obj.clone())
    }
}

/// Fetches groupcompress blocks in batches and turns them into record
/// factories.
///
/// Port of the Python `_BatchingBlockFetcher`. Keys are accumulated with
/// `add_key`; `yield_factories` fetches the batch's blocks, builds one
/// `LazyGroupContentManager` per block, registers each key as a factory,
/// and returns the resulting record factories.
#[pyclass(name = "_BatchingBlockFetcher")]
pub struct BatchingBlockFetcher {
    /// The owning `GroupCompressVersionedFiles` (for `_get_blocks`).
    gcvf: Py<PyAny>,
    /// `{key: index_memo}` for every key that might be fetched.
    locations: Py<PyDict>,
    /// Keys added to the current batch, in order.
    keys: Vec<Py<PyAny>>,
    /// Read-memos seen this batch -> cached block, or `None` if to-fetch.
    batch_memos: std::collections::HashMap<GcReadMemo, Option<Py<PyAny>>>,
    /// Uncached read-memos to fetch: typed memo paired with its tuple.
    memos_to_get: Vec<(GcReadMemo, Py<PyAny>)>,
    /// Running byte estimate for the pending batch.
    total_bytes: u64,
    /// Read-memo of the block the current manager covers.
    last_read_memo: Option<GcReadMemo>,
    /// The manager accumulating factories for the current block.
    manager: Option<Py<LazyGroupContentManager>>,
    /// Optional compressor-settings callback passed to each manager.
    get_compressor_settings: Option<Py<PyAny>>,
}

#[pymethods]
impl BatchingBlockFetcher {
    #[new]
    #[pyo3(signature = (gcvf, locations, get_compressor_settings=None))]
    fn new(
        gcvf: Bound<'_, PyAny>,
        locations: Bound<'_, PyDict>,
        get_compressor_settings: Option<Bound<'_, PyAny>>,
    ) -> Self {
        BatchingBlockFetcher {
            gcvf: gcvf.unbind(),
            locations: locations.unbind(),
            keys: Vec::new(),
            batch_memos: std::collections::HashMap::new(),
            memos_to_get: Vec::new(),
            total_bytes: 0,
            last_read_memo: None,
            manager: None,
            get_compressor_settings: get_compressor_settings.map(|s| s.unbind()),
        }
    }

    /// Add a key to the current batch; return the running byte estimate.
    ///
    /// Mirrors `_BatchingBlockFetcher.add_key`: a read-memo already in the
    /// batch is not re-counted; an uncached one is queued for fetch and its
    /// `stop` offset added to the estimate (matching the Python code, which
    /// adds `read_memo[2]`).
    fn add_key(&mut self, py: Python<'_>, key: Bound<'_, PyAny>) -> PyResult<u64> {
        // locations[key] is a GCBuildDetails; index_memo is its element 0.
        let details = self
            .locations
            .bind(py)
            .get_item(&key)?
            .ok_or_else(|| PyKeyError::new_err("key not in locations"))?;
        let index_memo = details.get_item(0)?;
        // read_memo = index_memo[0:3]
        let read_memo_obj = read_memo_tuple(py, &index_memo)?;
        let read_memo = extract_read_memo(&read_memo_obj)?;
        self.keys.push(key.unbind());
        if self.batch_memos.contains_key(&read_memo) {
            return Ok(self.total_bytes);
        }
        let cached = self
            .gcvf
            .bind(py)
            .getattr("_group_cache")?
            .call_method1("get", (&read_memo_obj, py.None()))?;
        if cached.is_none() {
            self.batch_memos.insert(read_memo.clone(), None);
            self.memos_to_get
                .push((read_memo, read_memo_obj.into_any().unbind()));
            self.total_bytes += index_memo.get_item(2)?.extract::<u64>()?;
        } else {
            self.batch_memos.insert(read_memo, Some(cached.unbind()));
        }
        Ok(self.total_bytes)
    }

    /// Keys added to the current batch, in order.
    #[getter]
    fn keys<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let list = PyList::empty(py);
        for k in &self.keys {
            list.append(k.bind(py))?;
        }
        Ok(list)
    }

    /// Read-memo tuples this batch still needs to fetch, in first-seen order.
    #[getter]
    fn memos_to_get<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let list = PyList::empty(py);
        for (_, tuple) in &self.memos_to_get {
            list.append(tuple.bind(py))?;
        }
        Ok(list)
    }

    /// Running byte estimate for the pending batch.
    #[getter]
    fn total_bytes(&self) -> u64 {
        self.total_bytes
    }

    /// Build and return the record factories for the keys added so far.
    ///
    /// Mirrors `_BatchingBlockFetcher.yield_factories`: blocks are fetched,
    /// a `LazyGroupContentManager` is started per block, each key is
    /// registered, and the managers' record streams are collected. With
    /// `full_flush` the final manager is flushed too. Returns the factories
    /// as a list (the Python generator is consumed eagerly by callers).
    #[pyo3(signature = (full_flush=false))]
    fn yield_factories<'py>(
        &mut self,
        py: Python<'py>,
        full_flush: bool,
    ) -> PyResult<Bound<'py, PyList>> {
        let out = PyList::empty(py);
        if self.manager.is_none() && self.keys.is_empty() {
            return Ok(out);
        }
        // Fetch every block this batch needs, as a (read_memo, block) iter.
        let memos_list = PyList::empty(py);
        for (_, tuple) in &self.memos_to_get {
            memos_list.append(tuple.bind(py))?;
        }
        let blocks = self
            .gcvf
            .bind(py)
            .call_method1("_get_blocks", (memos_list,))?;
        let mut blocks_iter = blocks.try_iter()?;
        // memos_to_get_stack: the to-fetch memos in reverse, so the next
        // expected block is always at the end.
        let mut memos_stack: Vec<GcReadMemo> = self
            .memos_to_get
            .iter()
            .rev()
            .map(|(m, _)| m.clone())
            .collect();
        let keys = std::mem::take(&mut self.keys);
        for key in &keys {
            // locations[key] is a GCBuildDetails: details[0] is index_memo,
            // details[2] is the key's parents.
            let details = self
                .locations
                .bind(py)
                .get_item(key.bind(py))?
                .ok_or_else(|| PyKeyError::new_err("key not in locations"))?;
            let index_memo = details.get_item(0)?;
            let read_memo = extract_read_memo(&index_memo)?;
            if self.last_read_memo.as_ref() != Some(&read_memo) {
                // Crossing into a new block: flush the previous manager.
                self.flush_manager(py, &out)?;
                let block: Py<PyAny> = if memos_stack.last() == Some(&read_memo) {
                    // The next block from _get_blocks is the one we need.
                    let pair = blocks_iter.next().ok_or_else(|| {
                        PyRuntimeError::new_err("_get_blocks yielded too few blocks")
                    })??;
                    let block_read_memo = extract_read_memo(&pair.get_item(0)?)?;
                    if block_read_memo != read_memo {
                        return Err(pyo3::exceptions::PyAssertionError::new_err(
                            "block_read_memo out of sync with read_memo",
                        ));
                    }
                    let block = pair.get_item(1)?.unbind();
                    self.batch_memos
                        .insert(read_memo.clone(), Some(block.clone_ref(py)));
                    memos_stack.pop();
                    block
                } else {
                    self.batch_memos
                        .get(&read_memo)
                        .and_then(|b| b.as_ref())
                        .ok_or_else(|| {
                            PyRuntimeError::new_err("batch_memos missing a cached block")
                        })?
                        .clone_ref(py)
                };
                let block_obj = block.bind(py).clone().cast_into::<GroupCompressBlock>()?;
                let settings = self
                    .get_compressor_settings
                    .as_ref()
                    .map(|s| s.bind(py).clone());
                let manager = Bound::new(
                    py,
                    LazyGroupContentManager::new(block_obj.unbind(), settings.map(|s| s.unbind())),
                )?;
                self.manager = Some(manager.unbind());
                self.last_read_memo = Some(read_memo);
            }
            // index_memo[3:5] -> (start, end); parents is details[2].
            let start: u64 = index_memo.get_item(3)?.extract()?;
            let end: u64 = index_memo.get_item(4)?.extract()?;
            let parents = details.get_item(2)?;
            self.manager
                .as_ref()
                .expect("manager set above")
                .bind(py)
                .call_method1("add_factory", (key.bind(py), parents, start, end))?;
        }
        if full_flush {
            self.flush_manager(py, &out)?;
        }
        self.batch_memos.clear();
        self.memos_to_get.clear();
        self.total_bytes = 0;
        Ok(out)
    }
}

impl BatchingBlockFetcher {
    /// Drain the current manager's record stream into `out` and drop it.
    ///
    /// Mirrors `_BatchingBlockFetcher._flush_manager`.
    fn flush_manager(&mut self, py: Python<'_>, out: &Bound<'_, PyList>) -> PyResult<()> {
        if let Some(manager) = self.manager.take() {
            let stream = manager.bind(py).call_method0("get_record_stream")?;
            for record in stream.try_iter()? {
                out.append(record?)?;
            }
            self.last_read_memo = None;
        }
        Ok(())
    }
}

/// Concrete instantiation of the pure `GroupCompressVersionedFiles` that
/// drives Python index / access / cache objects.
type PureGcvf =
    bazaar::groupcompress::gcvf::GroupCompressVersionedFiles<PyGcIndex, PyGcAccess, PyBlockCache>;

/// Python binding for `GroupCompressVersionedFiles`.
///
/// Holds the pure-Rust store plus the Python-visible state the test surface
/// expects (the Python-side `_group_cache`, `_unadded_refs`, the original
/// fallback objects, etc.). Methods marshal arguments in, call the pure
/// store, and marshal results back.
#[pyclass(name = "GroupCompressVersionedFiles", subclass, dict)]
pub struct GroupCompressVersionedFiles {
    /// The pure-Rust store; all real operations go through this.
    pure: PureGcvf,
    /// The `_GCGraphIndex` (or compatible) index object, kept for the
    /// `_index` getter.
    index_obj: Py<PyAny>,
    /// The raw-record access object, kept for the `_access` getter.
    access_obj: Py<PyAny>,
    /// Whether to delta-compress (True) or only entropy-compress.
    delta: bool,
    /// In-memory records added but not yet flushed, keyed by key.
    unadded_refs: Py<PyDict>,
    /// Block cache (`LRUSizeCache`); also the Python side of [`PyBlockCache`]
    /// inside the pure store.
    group_cache: Py<PyAny>,
    /// Python fallback VF objects, kept for the `_immediate_fallback_vfs`
    /// getter; the pure store holds the matching `PyVersionedFiles`
    /// wrappers in its own fallback list.
    immediate_fallback_vfs: Vec<Py<PyAny>>,
    /// Cap on bytes a `GroupCompressor` indexes; `None` until first use.
    max_bytes_to_index: Option<usize>,
}

#[pymethods]
impl GroupCompressVersionedFiles {
    #[new]
    #[pyo3(signature = (index, access, delta=true, _unadded_refs=None, _group_cache=None))]
    fn new(
        py: Python<'_>,
        index: Bound<'_, PyAny>,
        access: Bound<'_, PyAny>,
        delta: bool,
        _unadded_refs: Option<Bound<'_, PyDict>>,
        _group_cache: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Self> {
        let unadded_refs = match _unadded_refs {
            Some(d) => d.unbind(),
            None => PyDict::new(py).unbind(),
        };
        let group_cache = match _group_cache {
            Some(c) => c.unbind(),
            None => {
                // Default: LRUSizeCache(max_size=50 * 1024 * 1024)
                let cls = py.import("bzrformats.lru_cache")?.getattr("LRUSizeCache")?;
                let kwargs = PyDict::new(py);
                kwargs.set_item("max_size", 50 * 1024 * 1024)?;
                cls.call((), Some(&kwargs))?.unbind()
            }
        };
        let pure = bazaar::groupcompress::gcvf::GroupCompressVersionedFiles::with_cache(
            PyGcIndex::new(index.clone().unbind()),
            PyGcAccess::new(access.clone().unbind()),
            delta,
            PyBlockCache::new(group_cache.clone_ref(py)),
        );
        Ok(Self {
            pure,
            index_obj: index.unbind(),
            access_obj: access.unbind(),
            delta,
            unadded_refs,
            group_cache,
            immediate_fallback_vfs: Vec::new(),
            max_bytes_to_index: None,
        })
    }

    #[getter]
    fn _index(&self, py: Python<'_>) -> Py<PyAny> {
        self.index_obj.clone_ref(py)
    }

    #[getter]
    fn _access(&self, py: Python<'_>) -> Py<PyAny> {
        self.access_obj.clone_ref(py)
    }

    #[getter]
    fn _delta(&self) -> bool {
        self.delta
    }

    #[getter]
    fn _unadded_refs(&self, py: Python<'_>) -> Py<PyDict> {
        self.unadded_refs.clone_ref(py)
    }

    #[setter]
    fn set__unadded_refs(&mut self, value: Bound<'_, PyDict>) {
        self.unadded_refs = value.unbind();
    }

    #[getter]
    fn _group_cache(&self, py: Python<'_>) -> Py<PyAny> {
        self.group_cache.clone_ref(py)
    }

    #[getter]
    fn _immediate_fallback_vfs<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let list = PyList::empty(py);
        for vf in &self.immediate_fallback_vfs {
            list.append(vf.bind(py))?;
        }
        Ok(list)
    }

    #[getter]
    fn _max_bytes_to_index(&self) -> Option<usize> {
        self.max_bytes_to_index
    }

    #[setter]
    fn set__max_bytes_to_index(&mut self, value: Option<usize>) {
        self.max_bytes_to_index = value;
    }

    /// Return a clone of this object without any fallbacks configured.
    ///
    /// Mirrors `GroupCompressVersionedFiles.without_fallbacks`: the clone
    /// shares the block cache and gets a shallow copy of the unadded refs.
    /// The clone is built via `type(self)` so the Python subclass (which
    /// still carries the not-yet-ported record-stream methods) is produced,
    /// not the bare Rust base.
    fn without_fallbacks<'py>(slf: &Bound<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        let me = slf.borrow();
        let unadded_copy = me.unadded_refs.bind(py).copy()?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("_unadded_refs", unadded_copy)?;
        kwargs.set_item("_group_cache", me.group_cache.bind(py))?;
        slf.get_type().call(
            (me.index_obj.bind(py), me.access_obj.bind(py), me.delta),
            Some(&kwargs),
        )
    }

    /// Add a fallback store for texts not present in this one.
    ///
    /// Registers the object both on the Python-visible
    /// `_immediate_fallback_vfs` list (read by external callers via the
    /// getter) and on the pure store's fallback list as a
    /// [`PyVersionedFiles`] adapter, so trait-driven code paths
    /// (`get_sha1s`, `iter_lines_added_or_present_in_keys`, `check`, etc.)
    /// consult fallbacks correctly.
    fn add_fallback_versioned_files(&mut self, a_versioned_files: Bound<'_, PyAny>) {
        let unbound = a_versioned_files.unbind();
        let cloned = Python::attach(|py| unbound.clone_ref(py));
        self.immediate_fallback_vfs.push(unbound);
        self.pure
            .add_fallback_versioned_files(Box::new(crate::versionedfile::PyVersionedFiles::new(
                cloned,
            )));
    }

    /// Drop the block cache and the index's caches.
    ///
    /// Mirrors `GroupCompressVersionedFiles.clear_cache`. The pure store
    /// drops its block cache (and the wrapped Python LRUSizeCache via
    /// `PyBlockCache::clear`); we also clear the index's auxiliary caches
    /// that live outside the pure store.
    fn clear_cache(&self, py: Python<'_>) -> PyResult<()> {
        self.pure.clear_cache();
        let index = self.index_obj.bind(py);
        index.getattr("_graph_index")?.call_method0("clear_cache")?;
        index.getattr("_int_cache")?.call_method0("clear")?;
        Ok(())
    }

    /// Get a map of the graph parents of `keys`; absent keys are omitted.
    fn get_parent_map<'py>(
        &self,
        py: Python<'py>,
        keys: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyDict>> {
        // Iterate `keys` manually -- it may be any iterable (set, dict_keys,
        // generator), not just a Sequence pyo3 can extract a Vec from.
        let mut key_vec: Vec<bazaar::groupcompress::gcvf::GcKey> = Vec::new();
        for k in keys.try_iter()? {
            key_vec.push(k?.extract()?);
        }
        use bazaar::groupcompress::gcvf::GcIndex;
        let has_graph = self.pure.index().has_graph();
        let map = self
            .pure
            .get_parent_map(&key_vec)
            .map_err(crate::knit::knit_err_to_py)?;
        let result = PyDict::new(py);
        for (k, parents) in map {
            // A parentless index emits None for parents to distinguish "no
            // graph info" from "empty parents" (matches
            // _GCGraphIndex.get_parent_map and the per-vf tests).
            if has_graph {
                result.set_item(k, PyTuple::new(py, parents)?)?;
            } else {
                result.set_item(k, py.None())?;
            }
        }
        Ok(result)
    }

    /// Get the parent map together with the per-source result list.
    ///
    /// Mirrors `GroupCompressVersionedFiles._get_parent_map_with_sources`:
    /// the local index is consulted first, then each fallback in order;
    /// `source_results[i]` is the slice of the answer that source i
    /// supplied.
    fn _get_parent_map_with_sources<'py>(
        &self,
        py: Python<'py>,
        keys: Bound<'py, PyAny>,
    ) -> PyResult<(Bound<'py, PyDict>, Bound<'py, PyList>)> {
        self.parent_map_with_sources(py, &keys)
    }

    /// All keys present in this store or any fallback.
    fn keys<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PySet>> {
        py.import("logging")?
            .call_method1("getLogger", ("bzrformats.evil",))?
            .call_method1("debug", ("keys scales with size of history",))?;
        // The pure store walks its fallback list internally; we just
        // marshal the result into a Python set.
        let keys = self.pure.keys().map_err(crate::knit::knit_err_to_py)?;
        let result = PySet::empty(py)?;
        for k in keys {
            result.add(k)?;
        }
        Ok(result)
    }

    /// Fetch `GroupCompressBlock`s for `read_memos`, in request order.
    ///
    /// Mirrors `GroupCompressVersionedFiles._get_blocks`: blocks already in
    /// the cache are reused; uncached read-memos are de-duplicated, fetched
    /// in one `get_raw_records` call, decoded and cached. Returns an
    /// iterator of `(read_memo, block)` pairs matching the input order, so
    /// callers can `next()` over it as they did the original generator.
    fn _get_blocks<'py>(
        &self,
        py: Python<'py>,
        read_memos: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Keep each original Python read-memo tuple (the cache key) paired
        // with its typed form, which de-duplication compares by.
        let mut requested: Vec<(Bound<'py, PyAny>, GcReadMemo)> = Vec::new();
        for item in read_memos.try_iter()? {
            let obj = item?;
            let typed = extract_read_memo(&obj)?;
            requested.push((obj, typed));
        }
        let cache = self.group_cache.bind(py);
        // Map each typed memo back to its cache-key tuple for the fetch call.
        let tuple_of: std::collections::HashMap<GcReadMemo, Bound<'py, PyAny>> = requested
            .iter()
            .map(|(obj, typed)| (typed.clone(), obj.clone()))
            .collect();
        // Which read-memos still need fetching: de-duplicated, in request
        // order, skipping any already in the block cache.
        let typed_only: Vec<GcReadMemo> = requested.iter().map(|(_, t)| t.clone()).collect();
        let to_fetch = bazaar::groupcompress::gcvf::memos_to_fetch(&typed_only, |m| {
            tuple_of
                .get(m)
                .map(|obj| cache.contains(obj).unwrap_or(false))
                .unwrap_or(false)
        });
        let fetch_tuples = PyList::empty(py);
        for memo in &to_fetch {
            fetch_tuples.append(&tuple_of[memo])?;
        }
        let raw_records = self
            .access_obj
            .bind(py)
            .call_method1("get_raw_records", (fetch_tuples,))?;
        let mut raw_iter = raw_records.try_iter()?;
        let block_type = py.get_type::<GroupCompressBlock>();
        let result = PyList::empty(py);
        for (obj, _) in &requested {
            let cached = cache.get_item(obj).ok();
            let block = match cached {
                Some(block) => block,
                None => {
                    let zdata = raw_iter.next().ok_or_else(|| {
                        PyRuntimeError::new_err("get_raw_records yielded too few records")
                    })??;
                    let block = block_type.call_method1("from_bytes", (zdata,))?;
                    cache.set_item(obj, &block)?;
                    block
                }
            };
            result.append(PyTuple::new(py, [obj, &block])?)?;
        }
        // Return an iterator so callers can `next()` over it, as they did
        // the original generator.
        Ok(result.try_iter()?.into_any())
    }

    /// Get a stream of records for `keys`.
    ///
    /// Mirrors `GroupCompressVersionedFiles.get_record_stream`: drives
    /// `_get_remaining_record_stream`, retrying on `RetryWithNewPacks`.
    /// Returns the records as a list (the Python generator is consumed by
    /// iteration anyway).
    fn get_record_stream<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        keys: Bound<'py, PyAny>,
        ordering: Bound<'py, PyAny>,
        include_delta_closure: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let retry_cls = py
            .import("bzrformats.pack_repo")?
            .getattr("RetryWithNewPacks")?;
        // keys might be a generator; materialise it once.
        let orig_keys = PyList::empty(py);
        for k in keys.try_iter()? {
            orig_keys.append(k?)?;
        }
        let out = PyList::empty(py);
        if orig_keys.is_empty() {
            return Ok(out.try_iter()?.into_any());
        }
        let mut ordering: String = ordering.extract()?;
        let has_graph: bool = slf
            .borrow()
            .index_obj
            .bind(py)
            .getattr("has_graph")?
            .extract()?;
        if !has_graph && (ordering == "topological" || ordering == "groupcompress") {
            // No graph stored: a topological ordering is not possible.
            ordering = "unordered".to_string();
        }
        // remaining_keys shrinks as records come back; on a retry only the
        // still-missing keys are re-requested.
        let remaining: Bound<'py, PySet> = PySet::empty(py)?;
        for k in orig_keys.iter() {
            remaining.add(k)?;
        }
        loop {
            let request = PySet::empty(py)?;
            for k in remaining.iter() {
                request.add(k)?;
            }
            match Self::get_remaining_record_stream(
                slf,
                py,
                &request,
                &orig_keys,
                &ordering,
                include_delta_closure,
            ) {
                Ok(records) => {
                    for record in records.iter() {
                        remaining.discard(record.getattr("key")?)?;
                        out.append(record)?;
                    }
                    return Ok(out.try_iter()?.into_any());
                }
                Err(e) if e.is_instance(py, &retry_cls) => {
                    slf.borrow()
                        .access_obj
                        .bind(py)
                        .call_method1("reload_or_raise", (e.value(py),))?;
                    // Loop and retry with the still-remaining keys.
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// The GroupCompressor settings dict.
    ///
    /// Mirrors `_get_compressor_settings`: defaults `_max_bytes_to_index`
    /// on first use, then returns `{"max_bytes_to_index": ...}`.
    fn _get_compressor_settings<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        if self.max_bytes_to_index.is_none() {
            self.max_bytes_to_index = Some(bazaar::groupcompress::gcvf::DEFAULT_MAX_BYTES_TO_INDEX);
        }
        let d = PyDict::new(py);
        d.set_item("max_bytes_to_index", self.max_bytes_to_index)?;
        Ok(d)
    }

    /// Build a fresh GroupCompressor from the current settings.
    fn _make_group_compressor<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let settings = slf.borrow_mut()._get_compressor_settings(py)?;
        py.get_type::<RabinGroupCompressor>()
            .call1((settings,))
            .map(|c| c.into_any())
    }

    /// Insert a record stream, returning `(sha1, length)` per record.
    ///
    /// Mirrors `_insert_record_stream`. Records are compressed into a
    /// GroupCompressor; full blocks are flushed to the access object and
    /// indexed. With `reuse_blocks`, a well-utilised incoming
    /// groupcompress-block is copied as-is instead of being recompressed.
    #[pyo3(signature = (stream, random_id=false, nostore_sha=None, reuse_blocks=true))]
    fn _insert_record_stream<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        stream: Bound<'py, PyAny>,
        random_id: bool,
        nostore_sha: Option<Bound<'py, PyAny>>,
        reuse_blocks: bool,
    ) -> PyResult<Bound<'py, PyList>> {
        let results = PyList::empty(py);
        let adapter_registry = py
            .import("bzrformats.versionedfile")?
            .getattr("adapter_registry")?;
        let unavailable = py
            .import("bzrformats.versionedfile")?
            .getattr("UnavailableRepresentation")?;
        let decompress_corruption = py
            .import("bzrformats.groupcompress")?
            .getattr("DecompressCorruption")?;
        let revision_not_present = py
            .import("bzrformats.errors")?
            .getattr("RevisionNotPresent")?;

        // adapter cache: {adapter_key: adapter}
        let adapters = PyDict::new(py);
        let get_adapter = |adapter_key: &Bound<'py, PyAny>| -> PyResult<Bound<'py, PyAny>> {
            if let Some(a) = adapters.get_item(adapter_key)? {
                return Ok(a);
            }
            let factory = adapter_registry.call_method1("get", (adapter_key,))?;
            let adapter = factory.call1((slf,))?;
            adapters.set_item(adapter_key, &adapter)?;
            Ok(adapter)
        };

        let compressor = Self::_make_group_compressor(slf, py)?;
        slf.setattr("_compressor", &compressor)?;
        slf.borrow_mut().unadded_refs = PyDict::new(py).unbind();
        // keys_to_add: Vec<(key, "start end" reads, refs)>
        let mut keys_to_add: Vec<(Py<PyAny>, Py<PyAny>, Py<PyAny>)> = Vec::new();

        let mut last_prefix: Option<Py<PyAny>> = None;
        let mut max_fulltext_len: usize = 0;
        let mut max_fulltext_prefix: Option<Py<PyAny>> = None;
        let mut insert_manager: Option<Py<PyAny>> = None;
        let mut block_start: u64 = 0;
        let mut block_length: u64 = 0;
        let mut inserted_keys: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut reuse_this_block = reuse_blocks;

        for record in stream.try_iter()? {
            let record = record?;
            let storage_kind: String = record.getattr("storage_kind")?.extract()?;
            if storage_kind == "absent" {
                return Err(PyErr::from_value(
                    revision_not_present.call1((record.getattr("key")?, slf))?,
                ));
            }
            if random_id {
                let key_repr = record.getattr("key")?.repr()?.to_string();
                if !inserted_keys.insert(key_repr) {
                    py.import("logging")?
                        .call_method1("getLogger", ("bzrformats.groupcompress",))?
                        .call_method1(
                            "info",
                            (
                                "Insert claimed random_id=True, but then inserted %r two times",
                                record.getattr("key")?,
                            ),
                        )?;
                    continue;
                }
            }
            if reuse_blocks {
                // Only the leading groupcompress-block record decides reuse.
                if storage_kind == "groupcompress-block" {
                    let manager = record.getattr("_manager")?;
                    reuse_this_block = manager.call_method0("check_is_well_utilized")?.extract()?;
                    insert_manager = Some(manager.unbind());
                }
            } else {
                reuse_this_block = false;
            }
            if reuse_this_block {
                if storage_kind == "groupcompress-block" {
                    let manager = record.getattr("_manager")?;
                    insert_manager = Some(manager.clone().unbind());
                    let block = manager.getattr("_block")?;
                    let (bytes_len, chunks): (usize, Bound<'py, PyAny>) =
                        block.call_method0("to_chunks")?.extract()?;
                    let memo = slf
                        .borrow()
                        .access_obj
                        .bind(py)
                        .call_method1("add_raw_record", (py.None(), bytes_len, chunks))?;
                    block_start = memo.get_item(1)?.extract()?;
                    block_length = memo.get_item(2)?.extract()?;
                }
                if storage_kind == "groupcompress-block"
                    || storage_kind == "groupcompress-block-ref"
                {
                    let manager = record.getattr("_manager")?;
                    match &insert_manager {
                        None => {
                            return Err(pyo3::exceptions::PyAssertionError::new_err(
                                "No insert_manager set",
                            ))
                        }
                        Some(im) if !im.bind(py).is(&manager) => {
                            return Err(pyo3::exceptions::PyAssertionError::new_err(
                                "insert_manager does not match the current record, we \
                                 cannot be positive that the appropriate content was \
                                 inserted.",
                            ))
                        }
                        _ => {}
                    }
                    let start: u64 = record.getattr("_start")?.extract()?;
                    let end: u64 = record.getattr("_end")?.extract()?;
                    let value = PyBytes::new(
                        py,
                        format!("{} {} {} {}", block_start, block_length, start, end).as_bytes(),
                    );
                    let parents = record.getattr("parents")?;
                    let node = PyTuple::new(
                        py,
                        [
                            record.getattr("key")?,
                            value.into_any(),
                            PyTuple::new(py, [parents])?.into_any(),
                        ],
                    )?;
                    let kwargs = PyDict::new(py);
                    kwargs.set_item("random_id", random_id)?;
                    slf.borrow().index_obj.bind(py).call_method(
                        "add_records",
                        (PyList::new(py, [node])?,),
                        Some(&kwargs),
                    )?;
                    continue;
                }
            }
            // Ordinary path: get the record's chunked bytes, adapting if needed.
            let chunks: Bound<'py, PyAny> = match record.call_method1("get_bytes_as", ("chunked",))
            {
                Ok(c) => c,
                Err(e) if e.is_instance(py, &unavailable) => {
                    let adapter_key = PyTuple::new(
                        py,
                        [
                            record.getattr("storage_kind")?,
                            "chunked".into_pyobject(py)?.into_any(),
                        ],
                    )?;
                    let adapter = get_adapter(&adapter_key.into_any())?;
                    adapter.call_method1("get_bytes", (&record, "chunked"))?
                }
                Err(e) if e.is_instance_of::<PyValueError>(py) => {
                    return Err(PyErr::from_value(
                        decompress_corruption.call1((e.to_string(),))?,
                    ));
                }
                Err(e) => return Err(e),
            };
            let chunks_vec: Vec<Vec<u8>> = chunks.extract()?;
            let chunks_len: usize = match record.getattr("size")?.extract::<Option<usize>>()? {
                Some(s) => s,
                None => chunks_vec.iter().map(|c| c.len()).sum(),
            };
            let key = record.getattr("key")?;
            let (prefix, soft): (Option<Bound<'py, PyAny>>, bool) = if key.len()? > 1 {
                let prefix = key.get_item(0)?;
                let soft = last_prefix
                    .as_ref()
                    .is_some_and(|lp| lp.bind(py).eq(&prefix).unwrap_or(false));
                (Some(prefix), soft)
            } else {
                (None, false)
            };
            if max_fulltext_len < chunks_len {
                max_fulltext_len = chunks_len;
                max_fulltext_prefix = prefix.as_ref().map(|p| p.clone().unbind());
            }
            let compressor = slf.getattr("_compressor")?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("soft", soft)?;
            kwargs.set_item("nostore_sha", &nostore_sha)?;
            let res = compressor.call_method(
                "compress",
                (&key, &chunks, chunks_len, record.getattr("sha1")?),
                Some(&kwargs),
            )?;
            let mut found_sha1: Py<PyAny> = res.get_item(0)?.unbind();
            let mut end_point: usize = res.get_item(2)?.extract()?;
            let mut start_point: usize = res.get_item(1)?.extract()?;
            // start-new-block heuristic
            let same_prefix = match (&prefix, &max_fulltext_prefix) {
                (Some(p), Some(mp)) => p.eq(mp.bind(py)).unwrap_or(false),
                (None, None) => true,
                _ => false,
            };
            let start_new_block = if same_prefix && end_point < 2 * max_fulltext_len {
                false
            } else if end_point > 4 * 1024 * 1024 {
                true
            } else {
                prefix.is_some()
                    && !prefix
                        .as_ref()
                        .unwrap()
                        .eq(last_prefix.as_ref().map(|p| p.bind(py)))
                        .unwrap_or(false)
                    && end_point > 2 * 1024 * 1024
            };
            last_prefix = prefix.as_ref().map(|p| p.clone().unbind());
            if start_new_block {
                let block = compressor.call_method0("flush_without_last")?;
                Self::insert_flush(slf, py, &block, &mut keys_to_add, random_id)?;
                max_fulltext_len = chunks_len;
                let res2 = slf.getattr("_compressor")?.call_method1(
                    "compress",
                    (&key, &chunks, chunks_len, record.getattr("sha1")?),
                )?;
                found_sha1 = res2.get_item(0)?.unbind();
                start_point = res2.get_item(1)?.extract()?;
                end_point = res2.get_item(2)?.extract()?;
            }
            // key may be content-addressed: replace a None version id.
            let stored_key = if key.get_item(-1)?.is_none() {
                let n = key.len()?;
                let prefix_items = PyList::empty(py);
                for i in 0..n - 1 {
                    prefix_items.append(key.get_item(i)?)?;
                }
                let mut sha_seg = b"sha1:".to_vec();
                sha_seg.extend_from_slice(found_sha1.bind(py).extract::<Vec<u8>>()?.as_slice());
                prefix_items.append(PyBytes::new(py, &sha_seg))?;
                PyTuple::new(py, prefix_items.iter())?.into_any()
            } else {
                key.clone()
            };
            let parents = record.getattr("parents")?;
            slf.borrow()
                .unadded_refs
                .bind(py)
                .set_item(&stored_key, &parents)?;
            results.append(PyTuple::new(
                py,
                [
                    &found_sha1.bind(py).clone(),
                    &chunks_len.into_pyobject(py)?.into_any(),
                ],
            )?)?;
            // refs = (parents,) with parents normalised to nested tuples.
            let refs_parents = if parents.is_none() {
                py.None().into_bound(py)
            } else {
                let outer = PyList::empty(py);
                for p in parents.try_iter()? {
                    outer.append(PyTuple::new(
                        py,
                        p?.try_iter()?.collect::<PyResult<Vec<_>>>()?,
                    )?)?;
                }
                PyTuple::new(py, outer.iter())?.into_any()
            };
            let reads = PyBytes::new(py, format!("{} {}", start_point, end_point).as_bytes());
            keys_to_add.push((
                stored_key.unbind(),
                reads.into_any().unbind(),
                PyTuple::new(py, [refs_parents])?.into_any().unbind(),
            ));
        }
        if !keys_to_add.is_empty() {
            let block = slf.getattr("_compressor")?.call_method0("flush")?;
            Self::insert_flush(slf, py, &block, &mut keys_to_add, random_id)?;
        }
        slf.setattr("_compressor", py.None())?;
        Ok(results)
    }

    /// Check that a key is safe to add. Mirrors `_check_add`.
    fn _check_add(slf: &Bound<'_, Self>, key: Bound<'_, PyAny>, random_id: bool) -> PyResult<()> {
        let _ = random_id;
        let version_id = key.get_item(-1)?;
        if !version_id.is_none() {
            let vid: Vec<u8> = version_id.extract()?;
            // Mirror osutils.contains_whitespace: ASCII space/tab/CR/LF/VT/FF.
            if vid
                .iter()
                .any(|b| matches!(b, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c))
            {
                return Err(PyErr::from_value(
                    slf.py()
                        .import("bzrformats.errors")?
                        .getattr("InvalidRevisionId")?
                        .call1((version_id, slf))?,
                ));
            }
        }
        slf.call_method1("check_not_reserved_id", (version_id,))?;
        Ok(())
    }

    /// Add a text from a `ContentFactory`. Mirrors `add_content`.
    #[pyo3(signature = (factory, parent_texts=None, left_matching_blocks=None, nostore_sha=None, random_id=false))]
    fn add_content<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        factory: Bound<'py, PyAny>,
        parent_texts: Option<Bound<'py, PyAny>>,
        left_matching_blocks: Option<Bound<'py, PyAny>>,
        nostore_sha: Option<Bound<'py, PyAny>>,
        random_id: bool,
    ) -> PyResult<(Py<PyAny>, Py<PyAny>, Py<PyAny>)> {
        let _ = (parent_texts, left_matching_blocks);
        slf.borrow()
            .index_obj
            .bind(py)
            .call_method0("_check_write_ok")?;
        Self::_check_add(slf, factory.getattr("key")?, random_id)?;
        let records = PyList::new(py, [&factory])?;
        let result =
            Self::_insert_record_stream(slf, py, records.into_any(), random_id, nostore_sha, true)?;
        let first = result.get_item(0)?;
        Ok((
            first.get_item(0)?.unbind(),
            first.get_item(1)?.unbind(),
            py.None(),
        ))
    }

    /// Add a text given as a list of lines. Mirrors `add_lines`.
    #[pyo3(signature = (key, parents, lines, parent_texts=None, left_matching_blocks=None, nostore_sha=None, random_id=false, check_content=true))]
    #[allow(clippy::too_many_arguments)]
    fn add_lines<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        key: Bound<'py, PyAny>,
        parents: Bound<'py, PyAny>,
        lines: Bound<'py, PyAny>,
        parent_texts: Option<Bound<'py, PyAny>>,
        left_matching_blocks: Option<Bound<'py, PyAny>>,
        nostore_sha: Option<Bound<'py, PyAny>>,
        random_id: bool,
        check_content: bool,
    ) -> PyResult<(Py<PyAny>, Py<PyAny>, Py<PyAny>)> {
        slf.borrow()
            .index_obj
            .bind(py)
            .call_method0("_check_write_ok")?;
        let line_vec: Vec<Vec<u8>> = lines.extract()?;
        if check_content {
            for line in &line_vec {
                if !line.is_empty() && line[..line.len() - 1].contains(&b'\n') {
                    return Err(PyValueError::new_err("lines contain newlines"));
                }
            }
        }
        let sha1 = PyBytes::new(py, &bazaar::weave::sha_strings(&line_vec));
        let chunked_cls = py
            .import("bzrformats._bzr_rs.versionedfile")?
            .getattr("ChunkedContentFactory")?;
        let factory = chunked_cls.call1((&key, &parents, sha1, &lines))?;
        Self::add_content(
            slf,
            py,
            factory,
            parent_texts,
            left_matching_blocks,
            nostore_sha,
            random_id,
        )
    }

    /// Insert a record stream. Mirrors `insert_record_stream`.
    fn insert_record_stream<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        stream: Bound<'py, PyAny>,
    ) -> PyResult<()> {
        // random_id stays False: see the note in the Python original about
        // test_insert_record_stream_existing_keys.
        Self::_insert_record_stream(slf, py, stream, false, None, true)?;
        Ok(())
    }

    /// SHA-1 of every key. Mirrors `get_sha1s`.
    fn get_sha1s<'py>(
        &self,
        py: Python<'py>,
        keys: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let mut key_vec: Vec<bazaar::groupcompress::gcvf::GcKey> = Vec::new();
        for k in keys.try_iter()? {
            key_vec.push(k?.extract()?);
        }
        let map = self
            .pure
            .get_sha1s(&key_vec)
            .map_err(crate::knit::knit_err_to_py)?;
        let result = PyDict::new(py);
        for (k, digest) in map {
            result.set_item(k, PyBytes::new(py, &digest))?;
        }
        Ok(result)
    }

    /// Keys of missing compression parents.
    ///
    /// Mirrors `get_missing_compression_parent_keys`: groupcompress cannot
    /// reference texts outside the group, so this is always empty.
    fn get_missing_compression_parent_keys<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        py.import("builtins")?
            .call_method1("frozenset", (PyList::empty(py),))
    }

    /// Check the store for integrity. Mirrors `check`.
    ///
    /// With `keys=None` every record is read and decoded; otherwise the
    /// record stream for `keys` is returned for the caller to inspect.
    #[pyo3(signature = (progress_bar=None, keys=None))]
    fn check<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        progress_bar: Option<Bound<'py, PyAny>>,
        keys: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Option<Bound<'py, PyAny>>> {
        let _ = progress_bar;
        match keys {
            None => {
                slf.borrow()
                    .pure
                    .check()
                    .map_err(crate::knit::knit_err_to_py)?;
                Ok(None)
            }
            Some(keys) => {
                let unordered = "unordered".into_pyobject(py)?.into_any();
                Ok(Some(Self::get_record_stream(
                    slf, py, keys, unordered, true,
                )?))
            }
        }
    }

    /// Iterate `(line, key)` pairs over the lines in `keys`.
    ///
    /// Mirrors `iter_lines_added_or_present_in_keys`: each requested key's
    /// text is read and split into lines. Returns the pairs as a list.
    #[pyo3(signature = (keys, pb=None))]
    fn iter_lines_added_or_present_in_keys<'py>(
        &self,
        py: Python<'py>,
        keys: Bound<'py, PyAny>,
        pb: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyList>> {
        let _ = pb;
        let mut key_vec: Vec<bazaar::groupcompress::gcvf::GcKey> = Vec::new();
        for k in keys.try_iter()? {
            key_vec.push(k?.extract()?);
        }
        let pairs = self
            .pure
            .iter_lines_added_or_present_in_keys(&key_vec)
            .map_err(crate::knit::knit_err_to_py)?;
        let out = PyList::empty(py);
        for (line, key) in pairs {
            out.append(PyTuple::new(
                py,
                [
                    PyBytes::new(py, &line).into_any(),
                    key.into_pyobject(py)?.into_any(),
                ],
            )?)?;
        }
        Ok(out)
    }
}

impl GroupCompressVersionedFiles {
    /// Flush a finished block: write it via the access object, index every
    /// buffered key against it, and reset the pending state.
    ///
    /// Mirrors the `flush` closure inside `_insert_record_stream`.
    fn insert_flush<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        block: &Bound<'py, PyAny>,
        keys_to_add: &mut Vec<(Py<PyAny>, Py<PyAny>, Py<PyAny>)>,
        random_id: bool,
    ) -> PyResult<()> {
        let (bytes_len, chunks): (usize, Bound<'py, PyAny>) =
            block.call_method0("to_chunks")?.extract()?;
        // A new compressor starts the next block.
        let compressor = Self::_make_group_compressor(slf, py)?;
        slf.setattr("_compressor", &compressor)?;
        let memo = slf
            .borrow()
            .access_obj
            .bind(py)
            .call_method1("add_raw_record", (py.None(), bytes_len, chunks))?;
        let start: u64 = memo.get_item(1)?.extract()?;
        let length: u64 = memo.get_item(2)?.extract()?;
        let nodes = PyList::empty(py);
        for (key, reads, refs) in keys_to_add.iter() {
            let reads_bytes = reads.bind(py).extract::<Vec<u8>>()?;
            let mut value = format!("{} {} ", start, length).into_bytes();
            value.extend_from_slice(&reads_bytes);
            nodes.append(PyTuple::new(
                py,
                [
                    key.bind(py).clone(),
                    PyBytes::new(py, &value).into_any(),
                    refs.bind(py).clone(),
                ],
            )?)?;
        }
        let kwargs = PyDict::new(py);
        kwargs.set_item("random_id", random_id)?;
        slf.borrow()
            .index_obj
            .bind(py)
            .call_method("add_records", (nodes,), Some(&kwargs))?;
        slf.borrow_mut().unadded_refs = PyDict::new(py).unbind();
        keys_to_add.clear();
        Ok(())
    }

    /// Shared implementation behind `get_parent_map` /
    /// `_get_parent_map_with_sources`: walk the local index then each
    /// fallback, merging their `get_parent_map` answers and recording what
    /// each source contributed.
    fn parent_map_with_sources<'py>(
        &self,
        py: Python<'py>,
        keys: &Bound<'py, PyAny>,
    ) -> PyResult<(Bound<'py, PyDict>, Bound<'py, PyList>)> {
        let result = PyDict::new(py);
        let source_results = PyList::empty(py);
        let missing = PySet::empty(py)?;
        for k in keys.try_iter()? {
            missing.add(k?)?;
        }
        let mut sources: Vec<Bound<'py, PyAny>> = vec![self.index_obj.bind(py).clone()];
        for fb in &self.immediate_fallback_vfs {
            sources.push(fb.bind(py).clone());
        }
        for source in sources {
            if missing.is_empty() {
                break;
            }
            let new_result = source
                .call_method1("get_parent_map", (&missing,))?
                .cast_into::<PyDict>()?;
            source_results.append(&new_result)?;
            for (k, v) in new_result.iter() {
                result.set_item(&k, v)?;
                missing.discard(k)?;
            }
        }
        Ok((result, source_results))
    }

    /// Find whatever `missing` keys the fallback stores can supply.
    ///
    /// Mirrors `_find_from_fallback`. Returns `(parent_map,
    /// key_to_source_map, source_results)`; `missing` is mutated to drop
    /// keys a fallback supplied. `key_to_source_map` maps a key to the
    /// fallback object that has it.
    fn find_from_fallback<'py>(
        &self,
        py: Python<'py>,
        missing: &Bound<'py, PySet>,
    ) -> PyResult<(Bound<'py, PyDict>, Bound<'py, PyDict>, Bound<'py, PyList>)> {
        let parent_map = PyDict::new(py);
        let key_to_source = PyDict::new(py);
        let source_results = PyList::empty(py);
        for fb in &self.immediate_fallback_vfs {
            if missing.is_empty() {
                break;
            }
            let source = fb.bind(py);
            let source_parents = source
                .call_method1("get_parent_map", (&*missing,))?
                .cast_into::<PyDict>()?;
            let found = PyList::empty(py);
            for (k, v) in source_parents.iter() {
                parent_map.set_item(&k, v)?;
                key_to_source.set_item(&k, source)?;
                found.append(&k)?;
                missing.discard(k)?;
            }
            source_results.append(PyTuple::new(py, [source.as_any(), found.as_any()])?)?;
        }
        Ok((parent_map, key_to_source, source_results))
    }

    /// Group `present_keys` into `(source, [keys])` runs.
    ///
    /// `source_of` returns the Python source object for a key; consecutive
    /// keys from the same source (compared by identity) merge into one run.
    fn group_by_source<'py>(
        py: Python<'py>,
        present_keys: impl IntoIterator<Item = Bound<'py, PyAny>>,
        source_of: impl Fn(&Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyList>> {
        let runs = PyList::empty(py);
        let mut current: Option<Bound<'py, PyAny>> = None;
        for key in present_keys {
            let source = source_of(&key)?;
            let same = current.as_ref().is_some_and(|c| c.is(&source));
            if !same {
                runs.append(PyTuple::new(py, [&source, &PyList::empty(py).into_any()])?)?;
                current = Some(source);
            }
            let last = runs.get_item(runs.len() - 1)?;
            last.get_item(1)?.call_method1("append", (key,))?;
        }
        Ok(runs)
    }

    /// Topologically (or groupcompress-) order `parent_map`'s keys and group
    /// them by source. Mirrors `_get_ordered_source_keys`.
    fn ordered_source_keys<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        ordering: &str,
        parent_map: &Bound<'py, PyDict>,
        key_to_source: &Bound<'py, PyDict>,
    ) -> PyResult<Bound<'py, PyList>> {
        // Marshal the parent map to the (key, parents) segment form the
        // pure sorters use, remembering each key's Python object.
        let mut raw: Vec<(Vec<Vec<u8>>, Vec<Vec<Vec<u8>>>)> = Vec::new();
        let mut key_obj: std::collections::HashMap<Vec<Vec<u8>>, Bound<'py, PyAny>> =
            std::collections::HashMap::new();
        for (k, v) in parent_map.iter() {
            let segs: Vec<Vec<u8>> = k.extract()?;
            let parents: Vec<Vec<Vec<u8>>> = v.extract()?;
            key_obj.insert(segs.clone(), k);
            raw.push((segs, parents));
        }
        let present: Vec<Vec<Vec<u8>>> = if ordering == "topological" {
            let mut sorter = vcs_graph::tsort::TopoSorter::new(raw.into_iter());
            sorter
                .sorted()
                .map_err(|e| PyValueError::new_err(format!("topo_sort: {e:?}")))?
        } else {
            bazaar::groupcompress::sort::sort_gc_optimal(raw)
        };
        let ordered = present
            .into_iter()
            .filter_map(|segs| key_obj.get(&segs).cloned());
        Self::group_by_source(py, ordered, |key| match key_to_source.get_item(key)? {
            Some(src) => Ok(src),
            None => Ok(slf.clone().into_any()),
        })
    }

    /// Keep `orig_keys` order, grouping by source, dropping absent keys.
    /// Mirrors `_get_as_requested_source_keys`.
    fn as_requested_source_keys<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        orig_keys: &Bound<'py, PyList>,
        locations: &Bound<'py, PyDict>,
        unadded: &Bound<'py, PySet>,
        key_to_source: &Bound<'py, PyDict>,
    ) -> PyResult<Bound<'py, PyList>> {
        let mut present: Vec<Bound<'py, PyAny>> = Vec::new();
        for key in orig_keys.iter() {
            if locations.contains(&key)?
                || unadded.contains(&key)?
                || key_to_source.contains(&key)?
            {
                present.push(key);
            }
        }
        Self::group_by_source(py, present, |key| {
            if locations.contains(key)? || unadded.contains(key)? {
                Ok(slf.clone().into_any())
            } else {
                match key_to_source.get_item(key)? {
                    Some(src) => Ok(src),
                    None => Ok(slf.clone().into_any()),
                }
            }
        })
    }

    /// In-memory keys first, then located keys grouped by block, then
    /// fallback runs. Mirrors `_get_io_ordered_source_keys`.
    fn io_ordered_source_keys<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        locations: &Bound<'py, PyDict>,
        unadded: &Bound<'py, PySet>,
        source_result: &Bound<'py, PyList>,
    ) -> PyResult<Bound<'py, PyList>> {
        let present = PyList::empty(py);
        for k in unadded.iter() {
            present.append(k)?;
        }
        // Sort located keys by their index_memo's numeric fields
        // (start, stop, basis_end, delta_end): the start/stop pair keeps
        // keys of one block contiguous and orders blocks by file position,
        // and the basis_end/delta_end pair orders keys within a block by
        // their position in it. Python sorts by the whole index_memo; the
        // index object is equal within a single index, so ordering falls to
        // exactly these four numbers. The sort is stable, matching
        // `sorted(locations, key=get_group)`.
        let mut located: Vec<Bound<'py, PyAny>> = locations.keys().iter().collect();
        located.sort_by(|a, b| {
            let group = |k: &Bound<'py, PyAny>| -> (u64, u64, u64, u64) {
                locations
                    .get_item(k)
                    .ok()
                    .flatten()
                    .and_then(|d| d.get_item(0).ok())
                    .map(|im| {
                        let num = |i: isize| -> u64 {
                            im.get_item(i)
                                .ok()
                                .and_then(|v| v.extract::<u64>().ok())
                                .unwrap_or(0)
                        };
                        (num(1), num(2), num(3), num(4))
                    })
                    .unwrap_or((0, 0, 0, 0))
            };
            group(a).cmp(&group(b))
        });
        for k in located {
            present.append(k)?;
        }
        let runs = PyList::empty(py);
        runs.append(PyTuple::new(
            py,
            [slf.clone().into_any(), present.into_any()],
        )?)?;
        for sr in source_result.iter() {
            runs.append(sr)?;
        }
        Ok(runs)
    }

    /// The non-retrying core of `get_record_stream`.
    ///
    /// Mirrors `_get_remaining_record_stream`: locate keys, find what
    /// fallbacks can supply, order the keys per `ordering`, then walk the
    /// `(source, keys)` runs — batching local keys through a
    /// `_BatchingBlockFetcher`, extracting unadded keys from the compressor,
    /// and delegating fallback runs. Returns the records as a list.
    fn get_remaining_record_stream<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        keys: &Bound<'py, PySet>,
        orig_keys: &Bound<'py, PyList>,
        ordering: &str,
        include_delta_closure: bool,
    ) -> PyResult<Bound<'py, PyList>> {
        let me = slf.borrow();
        let index = me.index_obj.bind(py);
        let unadded_refs = me.unadded_refs.bind(py).clone();
        let out = PyList::empty(py);

        let locations = index
            .call_method1("get_build_details", (&*keys,))?
            .cast_into::<PyDict>()?;
        // unadded_keys = unadded_refs ∩ keys
        let unadded_keys = PySet::empty(py)?;
        for k in keys.iter() {
            if unadded_refs.contains(&k)? {
                unadded_keys.add(k)?;
            }
        }
        // missing = keys − locations − unadded_keys
        let missing = PySet::empty(py)?;
        for k in keys.iter() {
            if !locations.contains(&k)? && !unadded_keys.contains(&k)? {
                missing.add(k)?;
            }
        }
        let (fallback_parent_map, key_to_source, source_result) =
            me.find_from_fallback(py, &missing)?;

        let source_keys = if ordering == "topological" || ordering == "groupcompress" {
            // parent_map = {key: details[2]} ∪ unadded ∪ fallback
            let parent_map = PyDict::new(py);
            for (k, details) in locations.iter() {
                parent_map.set_item(k, details.get_item(2)?)?;
            }
            for k in unadded_keys.iter() {
                parent_map.set_item(
                    &k,
                    unadded_refs
                        .get_item(&k)?
                        .ok_or_else(|| PyKeyError::new_err("unadded ref vanished"))?,
                )?;
            }
            parent_map.update(fallback_parent_map.as_mapping())?;
            Self::ordered_source_keys(slf, py, ordering, &parent_map, &key_to_source)?
        } else if ordering == "as-requested" {
            Self::as_requested_source_keys(
                slf,
                py,
                orig_keys,
                &locations,
                &unadded_keys,
                &key_to_source,
            )?
        } else {
            Self::io_ordered_source_keys(slf, py, &locations, &unadded_keys, &source_result)?
        };

        let absent_cls = py
            .import("bzrformats._bzr_rs.versionedfile")?
            .getattr("AbsentContentFactory")?;
        for k in missing.iter() {
            out.append(absent_cls.call1((k,))?)?;
        }

        let chunked_cls = py
            .import("bzrformats._bzr_rs.versionedfile")?
            .getattr("ChunkedContentFactory")?;
        let get_compressor_settings = slf.getattr("_get_compressor_settings")?;
        let batcher = py.get_type::<BatchingBlockFetcher>().call1((
            slf,
            &locations,
            get_compressor_settings,
        ))?;

        for entry in source_keys.iter() {
            let source = entry.get_item(0)?;
            let run_keys = entry.get_item(1)?;
            if source.is(slf) {
                for key in run_keys.try_iter()? {
                    let key = key?;
                    if unadded_refs.contains(&key)? {
                        // Flush, then yield the unadded ref from the compressor.
                        for r in batcher
                            .call_method1("yield_factories", (true,))?
                            .try_iter()?
                        {
                            out.append(r?)?;
                        }
                        let compressor = slf.getattr("_compressor")?;
                        let extracted = compressor.call_method1("extract", (&key,))?;
                        let chunks = extracted.get_item(0)?;
                        let sha1 = extracted.get_item(1)?;
                        let parents = unadded_refs
                            .get_item(&key)?
                            .ok_or_else(|| PyKeyError::new_err("unadded ref vanished"))?;
                        out.append(chunked_cls.call1((&key, parents, sha1, chunks))?)?;
                        continue;
                    }
                    let total: u64 = batcher.call_method1("add_key", (&key,))?.extract()?;
                    if total > bazaar::groupcompress::gcvf::BATCH_SIZE {
                        for r in batcher.call_method0("yield_factories")?.try_iter()? {
                            out.append(r?)?;
                        }
                    }
                }
            } else {
                for r in batcher
                    .call_method1("yield_factories", (true,))?
                    .try_iter()?
                {
                    out.append(r?)?;
                }
                let stream = source.call_method1(
                    "get_record_stream",
                    (run_keys, ordering, include_delta_closure),
                )?;
                for r in stream.try_iter()? {
                    out.append(r?)?;
                }
            }
        }
        for r in batcher
            .call_method1("yield_factories", (true,))?
            .try_iter()?
        {
            out.append(r?)?;
        }
        Ok(out)
    }
}

pub(crate) fn _groupcompress_rs(py: Python) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "groupcompress")?;
    m.add_wrapped(wrap_pyfunction!(encode_base128_int))?;
    m.add_wrapped(wrap_pyfunction!(decode_base128_int))?;
    m.add_wrapped(wrap_pyfunction!(apply_delta))?;
    m.add_wrapped(wrap_pyfunction!(decode_copy_instruction))?;
    m.add_wrapped(wrap_pyfunction!(encode_copy_instruction))?;
    m.add_wrapped(wrap_pyfunction!(apply_delta_to_source))?;
    m.add_wrapped(wrap_pyfunction!(make_line_delta))?;
    m.add_wrapped(wrap_pyfunction!(make_rabin_delta))?;
    m.add_wrapped(wrap_pyfunction!(rabin_hash))?;
    m.add_function(wrap_pyfunction!(sort_gc_optimal, &m)?)?;
    m.add_function(wrap_pyfunction!(parse_wire_header, &m)?)?;
    m.add_function(wrap_pyfunction!(check_rebuild_action, &m)?)?;
    m.add_function(wrap_pyfunction!(check_is_well_utilized, &m)?)?;
    m.add_function(wrap_pyfunction!(build_wire_prefix, &m)?)?;
    m.add_function(wrap_pyfunction!(parse_node_position, &m)?)?;
    m.add_class::<GroupCompressBlock>()?;
    m.add_class::<LinesDeltaIndex>()?;
    m.add_class::<TraditionalGroupCompressor>()?;
    m.add_class::<RabinGroupCompressor>()?;
    m.add_class::<LazyGroupContentManager>()?;
    m.add_class::<LazyGroupCompressFactory>()?;
    m.add_class::<RecordStreamIter>()?;
    m.add_class::<GCBuildDetails>()?;
    m.add_class::<GCGraphIndex>()?;
    m.add_class::<BatchingBlockFetcher>()?;
    m.add_class::<GroupCompressVersionedFiles>()?;
    m.add_class::<crate::groupcompress_delta::DeltaIndex>()?;
    m.add_function(wrap_pyfunction!(
        crate::groupcompress_delta::_rabin_hash,
        &m
    )?)?;
    m.add_function(wrap_pyfunction!(
        crate::groupcompress_delta::make_delta,
        &m
    )?)?;
    m.add(
        "NULL_SHA1",
        pyo3::types::PyBytes::new(py, &bazaar::groupcompress::NULL_SHA1),
    )?;
    Ok(m)
}
