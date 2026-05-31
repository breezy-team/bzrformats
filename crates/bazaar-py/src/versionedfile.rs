use bazaar::versionedfile::{ContentFactory, Key};
use pyo3::exceptions::{PyKeyError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PySet, PyTuple};

#[pyclass(subclass)]
pub(crate) struct AbstractContentFactory(Box<dyn ContentFactory + Send + Sync>);

pyo3::import_exception!(bzrformats._bzr_rs.errors, UnavailableRepresentation);

#[pymethods]
impl AbstractContentFactory {
    #[getter]
    fn sha1(&self, py: Python) -> Option<Py<PyAny>> {
        self.0.sha1().map(|x| PyBytes::new(py, &x).into())
    }

    #[getter]
    fn key(&self) -> Key {
        self.0.key()
    }

    #[getter]
    fn parents(&self) -> Option<Vec<Key>> {
        self.0.parents()
    }

    #[getter]
    fn storage_kind(&self) -> String {
        self.0.storage_kind()
    }

    #[getter]
    fn size(&self) -> Option<usize> {
        self.0.size()
    }

    fn get_bytes_as(&self, py: Python, storage_kind: &str) -> PyResult<Py<PyAny>> {
        if self.0.storage_kind() == "absent" {
            return Err(UnavailableRepresentation::new_err(
                "Absent content has no bytes".to_string(),
            ));
        }
        match storage_kind {
            "fulltext" => Ok(PyBytes::new(py, self.0.to_fulltext().as_ref()).into()),
            "lines" => Ok(self
                .0
                .to_lines()
                .map(|b| PyBytes::new(py, b.as_ref()))
                .map(|b| b.unbind().into())
                .collect::<Vec<Py<PyAny>>>()
                .into_pyobject(py)?
                .unbind()),
            "chunked" => Ok(self
                .0
                .to_chunks()
                .map(|b| PyBytes::new(py, b.as_ref()))
                .map(|b| b.unbind().into())
                .collect::<Vec<Py<PyAny>>>()
                .into_pyobject(py)?
                .unbind()),
            _ => Err(UnavailableRepresentation::new_err(format!(
                "Unsupported storage kind: {}",
                storage_kind
            ))),
        }
    }

    fn iter_bytes_as(&self, py: Python, storage_kind: &str) -> PyResult<Py<PyAny>> {
        if self.0.storage_kind() == "absent" {
            return Err(UnavailableRepresentation::new_err(
                "Absent content has no bytes".to_string(),
            ));
        }
        match storage_kind {
            "lines" => Ok(self
                .0
                .to_lines()
                .map(|b| PyBytes::new(py, b.as_ref()))
                .map(|b| b.unbind().into())
                .collect::<Vec<Py<PyAny>>>()
                .into_pyobject(py)?
                .unbind()),
            "chunked" => Ok(self
                .0
                .to_chunks()
                .map(|b| PyBytes::new(py, b.as_ref()))
                .map(|b| b.unbind().into())
                .collect::<Vec<Py<PyAny>>>()
                .into_pyobject(py)?
                .unbind()),
            _ => Err(UnavailableRepresentation::new_err(format!(
                "Unsupported storage kind: {}",
                storage_kind
            ))),
        }
    }

    fn map_key(&mut self, py: Python, cb: Py<PyAny>) -> PyResult<()> {
        self.0
            .map_key(&|k| cb.call1(py, (k,)).unwrap().extract::<Key>(py).unwrap());
        Ok(())
    }
}

#[pyclass(extends=AbstractContentFactory)]
struct FulltextContentFactory;

#[pymethods]
impl FulltextContentFactory {
    #[new]
    #[pyo3(signature = (key, parents, sha1, text))]
    fn new(
        key: Key,
        parents: Option<Vec<Key>>,
        sha1: Option<Vec<u8>>,
        text: Vec<u8>,
    ) -> PyResult<(Self, AbstractContentFactory)> {
        let of = bazaar::versionedfile::FulltextContentFactory::new(sha1, key, parents, text);

        Ok((FulltextContentFactory, AbstractContentFactory(Box::new(of))))
    }
}

#[pyclass(extends=AbstractContentFactory)]
pub(crate) struct ChunkedContentFactory;

#[pymethods]
impl ChunkedContentFactory {
    #[new]
    #[pyo3(signature = (key, parents, sha1, chunks))]
    fn new(
        key: Key,
        parents: Option<Vec<Key>>,
        sha1: Option<Vec<u8>>,
        chunks: Vec<Vec<u8>>,
    ) -> PyResult<(Self, AbstractContentFactory)> {
        let of = bazaar::versionedfile::ChunkedContentFactory::new(sha1, key, parents, chunks);

        Ok((ChunkedContentFactory, AbstractContentFactory(Box::new(of))))
    }
}

/// Build a `ChunkedContentFactory` pyclass instance directly, without
/// importing `bzrformats._bzr_rs.versionedfile` back into the extension.
pub(crate) fn new_chunked_content_factory(
    py: Python<'_>,
    key: Key,
    parents: Option<Vec<Key>>,
    sha1: Option<Vec<u8>>,
    chunks: Vec<Vec<u8>>,
) -> PyResult<Bound<'_, ChunkedContentFactory>> {
    let of = bazaar::versionedfile::ChunkedContentFactory::new(sha1, key, parents, chunks);
    let init = PyClassInitializer::from(AbstractContentFactory(Box::new(of)))
        .add_subclass(ChunkedContentFactory);
    Bound::new(py, init)
}

/// `ContentFactory` backed by a Python file-like object.
///
/// Wraps `bzrformats.versionedfile.FileContentFactory`: the storage kind
/// is `"file"`, and bytes are pulled out of the Python file on first
/// access (cached in memory thereafter so repeat reads don't have to
/// `seek(0)`). The original Python implementation re-read the file from
/// the start on each call; caching matches that behaviour from the
/// caller's perspective without holding a Python lock across reads.
struct FileContentFactoryInner {
    key: Key,
    parents: Option<Vec<Key>>,
    sha1: Option<Vec<u8>>,
    size: Option<usize>,
    file: Py<PyAny>,
    cache: std::sync::Mutex<Option<Vec<u8>>>,
}

impl FileContentFactoryInner {
    fn fulltext(&self) -> Vec<u8> {
        // Read the full file once; subsequent calls hit the cache. Any
        // Python error during the read is panicked because the trait
        // signature is infallible; callers should only construct this
        // factory from a file they trust.
        let mut guard = self.cache.lock().unwrap();
        if let Some(cached) = guard.as_ref() {
            return cached.clone();
        }
        let bytes: Vec<u8> = Python::attach(|py| -> PyResult<Vec<u8>> {
            let f = self.file.bind(py);
            // The Python original only seeks on _subsequent_ calls; the cache
            // above turns subsequent reads into no-ops, so we never need to
            // seek. This matters for non-seekable file-likes (e.g. PyIterableFile).
            let buf: Vec<u8> = f.call_method0("read")?.extract()?;
            Ok(buf)
        })
        .expect("FileContentFactory.read failed");
        *guard = Some(bytes.clone());
        bytes
    }
}

impl bazaar::versionedfile::ContentFactory for FileContentFactoryInner {
    fn sha1(&self) -> Option<Vec<u8>> {
        self.sha1.clone()
    }

    fn size(&self) -> Option<usize> {
        self.size
    }

    fn key(&self) -> Key {
        self.key.clone()
    }

    fn parents(&self) -> Option<Vec<Key>> {
        self.parents.clone()
    }

    fn to_fulltext<'a, 'b>(&'a self) -> std::borrow::Cow<'b, [u8]>
    where
        'a: 'b,
    {
        std::borrow::Cow::Owned(self.fulltext())
    }

    fn to_chunks<'a, 'b>(&'a self) -> Box<dyn Iterator<Item = std::borrow::Cow<'b, [u8]>> + 'b>
    where
        'a: 'b,
    {
        let full = self.fulltext();
        // 64KB chunks, matching `osutils.file_iterator`'s default.
        const CHUNK: usize = 65536;
        let chunks: Vec<Vec<u8>> = full.chunks(CHUNK).map(|c| c.to_vec()).collect();
        Box::new(chunks.into_iter().map(std::borrow::Cow::Owned))
    }

    fn to_lines<'a, 'b>(&'a self) -> Box<dyn Iterator<Item = std::borrow::Cow<'b, [u8]>> + 'b>
    where
        'a: 'b,
    {
        let full = self.fulltext();
        Box::new(
            bazaar::osutils::chunks_to_lines(std::iter::once(Ok::<_, std::io::Error>(&full[..])))
                .map(|l| std::borrow::Cow::Owned(l.unwrap().into_owned()))
                .collect::<Vec<_>>()
                .into_iter(),
        )
    }

    fn into_fulltext(self) -> Vec<u8> {
        self.fulltext()
    }

    fn into_chunks(self) -> Box<dyn Iterator<Item = Vec<u8>>> {
        let full = self.fulltext();
        const CHUNK: usize = 65536;
        let chunks: Vec<Vec<u8>> = full.chunks(CHUNK).map(|c| c.to_vec()).collect();
        Box::new(chunks.into_iter())
    }

    fn storage_kind(&self) -> String {
        "file".into()
    }

    fn map_key(&mut self, f: &dyn Fn(Key) -> Key) {
        self.key = f(self.key.clone());
        self.parents = self.parents.take().map(|v| v.into_iter().map(f).collect());
    }
}

#[pyclass(name = "FileContentFactory", extends = AbstractContentFactory, module = "bzrformats._bzr_rs.versionedfile")]
struct PyFileContentFactory;

#[pymethods]
impl PyFileContentFactory {
    #[new]
    #[pyo3(signature = (key, parents, fileobj, sha1=None, size=None))]
    fn new(
        key: Key,
        parents: Option<Vec<Key>>,
        fileobj: Py<PyAny>,
        sha1: Option<Vec<u8>>,
        size: Option<usize>,
    ) -> PyResult<(Self, AbstractContentFactory)> {
        let inner = FileContentFactoryInner {
            key,
            parents,
            sha1,
            size,
            file: fileobj,
            cache: std::sync::Mutex::new(None),
        };
        Ok((
            PyFileContentFactory,
            AbstractContentFactory(Box::new(inner)),
        ))
    }
}

/// `ContentFactory` that overrides the underlying record's key (and
/// parents) while delegating everything else - bytes, size, sha1,
/// storage_kind - to a Python `ContentFactory` instance.
///
/// Mirrors `bzrformats.versionedfile.AdapterFactory`. The Python class
/// used `__getattr__` to forward attribute access; here we make the
/// forwarding explicit through the `ContentFactory` trait.
struct AdapterFactoryInner {
    key: Key,
    parents: Option<Vec<Key>>,
    adapted: Py<PyAny>,
}

impl AdapterFactoryInner {
    fn call_adapted_bytes(&self, method: &str, kind: &str) -> Vec<u8> {
        Python::attach(|py| -> PyResult<Vec<u8>> {
            self.adapted
                .bind(py)
                .call_method1(method, (kind,))?
                .extract()
        })
        .expect("AdapterFactory delegate call failed")
    }

    fn adapted_storage_kind(&self) -> String {
        Python::attach(|py| -> PyResult<String> {
            self.adapted.bind(py).getattr("storage_kind")?.extract()
        })
        .expect("AdapterFactory storage_kind read failed")
    }

    fn adapted_sha1(&self) -> Option<Vec<u8>> {
        Python::attach(|py| -> PyResult<Option<Vec<u8>>> {
            let val = self.adapted.bind(py).getattr("sha1")?;
            if val.is_none() {
                Ok(None)
            } else {
                Ok(Some(val.extract()?))
            }
        })
        .expect("AdapterFactory sha1 read failed")
    }

    fn adapted_size(&self) -> Option<usize> {
        Python::attach(|py| -> PyResult<Option<usize>> {
            let val = self.adapted.bind(py).getattr("size")?;
            if val.is_none() {
                Ok(None)
            } else {
                Ok(Some(val.extract()?))
            }
        })
        .expect("AdapterFactory size read failed")
    }
}

impl bazaar::versionedfile::ContentFactory for AdapterFactoryInner {
    fn sha1(&self) -> Option<Vec<u8>> {
        self.adapted_sha1()
    }

    fn size(&self) -> Option<usize> {
        self.adapted_size()
    }

    fn key(&self) -> Key {
        self.key.clone()
    }

    fn parents(&self) -> Option<Vec<Key>> {
        self.parents.clone()
    }

    fn to_fulltext<'a, 'b>(&'a self) -> std::borrow::Cow<'b, [u8]>
    where
        'a: 'b,
    {
        std::borrow::Cow::Owned(self.call_adapted_bytes("get_bytes_as", "fulltext"))
    }

    fn to_chunks<'a, 'b>(&'a self) -> Box<dyn Iterator<Item = std::borrow::Cow<'b, [u8]>> + 'b>
    where
        'a: 'b,
    {
        let chunks: Vec<Vec<u8>> = Python::attach(|py| -> PyResult<Vec<Vec<u8>>> {
            self.adapted
                .bind(py)
                .call_method1("get_bytes_as", ("chunked",))?
                .extract()
        })
        .expect("AdapterFactory get_bytes_as(chunked) failed");
        Box::new(chunks.into_iter().map(std::borrow::Cow::Owned))
    }

    fn to_lines<'a, 'b>(&'a self) -> Box<dyn Iterator<Item = std::borrow::Cow<'b, [u8]>> + 'b>
    where
        'a: 'b,
    {
        let lines: Vec<Vec<u8>> = Python::attach(|py| -> PyResult<Vec<Vec<u8>>> {
            self.adapted
                .bind(py)
                .call_method1("get_bytes_as", ("lines",))?
                .extract()
        })
        .expect("AdapterFactory get_bytes_as(lines) failed");
        Box::new(lines.into_iter().map(std::borrow::Cow::Owned))
    }

    fn into_fulltext(self) -> Vec<u8> {
        self.call_adapted_bytes("get_bytes_as", "fulltext")
    }

    fn into_chunks(self) -> Box<dyn Iterator<Item = Vec<u8>>> {
        let chunks: Vec<Vec<u8>> = Python::attach(|py| -> PyResult<Vec<Vec<u8>>> {
            self.adapted
                .bind(py)
                .call_method1("get_bytes_as", ("chunked",))?
                .extract()
        })
        .expect("AdapterFactory get_bytes_as(chunked) failed");
        Box::new(chunks.into_iter())
    }

    fn storage_kind(&self) -> String {
        self.adapted_storage_kind()
    }

    fn map_key(&mut self, f: &dyn Fn(Key) -> Key) {
        self.key = f(self.key.clone());
        self.parents = self.parents.take().map(|v| v.into_iter().map(f).collect());
    }
}

#[pyclass(name = "AdapterFactory", extends = AbstractContentFactory, module = "bzrformats._bzr_rs.versionedfile")]
struct PyAdapterFactory {
    // Duplicate the adapted reference here so the `__getattr__` forwarder
    // can reach it without downcasting through `AbstractContentFactory`'s
    // boxed trait object. Cheap clone of the same Python reference.
    adapted: Py<PyAny>,
}

#[pymethods]
impl PyAdapterFactory {
    #[new]
    fn new(
        py: Python<'_>,
        key: Key,
        parents: Option<Vec<Key>>,
        adapted: Py<PyAny>,
    ) -> PyResult<(Self, AbstractContentFactory)> {
        let adapted_for_forward = adapted.clone_ref(py);
        let inner = AdapterFactoryInner {
            key,
            parents,
            adapted,
        };
        Ok((
            PyAdapterFactory {
                adapted: adapted_for_forward,
            },
            AbstractContentFactory(Box::new(inner)),
        ))
    }

    /// Forward arbitrary attribute access to the adapted factory. Mirrors
    /// the Python `__getattr__` that the original `AdapterFactory`
    /// relied on (and that knit's adapter chain probes via `_raw_record`
    /// and friends).
    fn __getattr__<'py>(&self, py: Python<'py>, name: &str) -> PyResult<Bound<'py, PyAny>> {
        self.adapted.bind(py).getattr(name)
    }
}

#[pyfunction]
pub fn record_to_fulltext_bytes<'py>(
    py: Python<'py>,
    record: Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyBytes>> {
    // Pull every framing input out of the Python record with `?` so
    // attribute or extraction failures surface as proper Python errors.
    // Python's record contract:
    //   record.key                         -> tuple of bytes
    //   record.parents                     -> None, or sequence of tuples
    //   record.get_bytes_as("fulltext")    -> bytes
    let key: Key = record.getattr("key")?.extract()?;
    let parents_obj = record.getattr("parents")?;
    let parents: Option<Vec<Key>> = if parents_obj.is_none() {
        None
    } else {
        Some(parents_obj.extract()?)
    };
    let fulltext: Vec<u8> = record
        .call_method1("get_bytes_as", ("fulltext",))?
        .extract()?;

    let _ = py;
    let mut buf = Vec::new();
    bazaar::versionedfile::write_fulltext_record(&key, parents.as_deref(), &fulltext, &mut buf)?;
    Ok(PyBytes::new(record.py(), &buf))
}

#[pyclass(extends=AbstractContentFactory)]
pub(crate) struct AbsentContentFactory;

#[pymethods]
impl AbsentContentFactory {
    #[new]
    fn new(key: Key) -> PyResult<(Self, AbstractContentFactory)> {
        let of = bazaar::versionedfile::AbsentContentFactory::new(key);

        Ok((AbsentContentFactory, AbstractContentFactory(Box::new(of))))
    }
}

/// Build an `AbsentContentFactory` pyclass instance directly, without
/// importing `bzrformats._bzr_rs.versionedfile` back into the extension.
pub(crate) fn new_absent_content_factory(
    py: Python<'_>,
    key: Key,
) -> PyResult<Bound<'_, AbsentContentFactory>> {
    let of = bazaar::versionedfile::AbsentContentFactory::new(key);
    let init = PyClassInitializer::from(AbstractContentFactory(Box::new(of)))
        .add_subclass(AbsentContentFactory);
    Bound::new(py, init)
}

/// First-pass refcount/needed-key bookkeeping for `_MPDiffGenerator`.
///
/// Exposes the per-step intermediate state that breezy's whitebox tests
/// probe (`_find_needed_keys` + `gen.ghost_parents` / `gen.refcounts`).
/// The single-shot fast path lives in
/// [`bazaar::versionedfile::make_mpdiffs`]; this helper backs the
/// step-by-step Python flavour only.
#[pyfunction]
fn mpdiff_first_pass<'py>(
    py: Python<'py>,
    ordered_keys: &Bound<'py, PyAny>,
    parent_map: &Bound<'py, PyDict>,
) -> PyResult<(
    Bound<'py, PySet>,
    Bound<'py, PyDict>,
    Bound<'py, PySet>,
    Bound<'py, PySet>,
)> {
    let needed_keys = PySet::empty(py)?;
    for k in ordered_keys.try_iter()? {
        needed_keys.add(k?)?;
    }

    let missing_keys = PySet::empty(py)?;
    for k in needed_keys.iter() {
        if !parent_map.contains(&k)? {
            missing_keys.add(k)?;
        }
    }

    let refcounts = PyDict::new(py);
    let just_parents = PySet::empty(py)?;
    for (_child_key, parent_keys) in parent_map.iter() {
        if parent_keys.is_none() {
            continue;
        }
        if parent_keys.len().unwrap_or(0) == 0 {
            continue;
        }
        for p in parent_keys.try_iter()? {
            let p = p?;
            just_parents.add(&p)?;
            needed_keys.add(&p)?;
            let new_count = match refcounts.get_item(&p)? {
                Some(existing) => existing.extract::<i64>()? + 1,
                None => 1,
            };
            refcounts.set_item(&p, new_count)?;
        }
    }

    let to_remove: Vec<Py<PyAny>> = just_parents
        .iter()
        .filter_map(|p| match parent_map.contains(&p) {
            Ok(true) => Some(Ok(p.unbind())),
            Ok(false) => None,
            Err(e) => Some(Err(e)),
        })
        .collect::<PyResult<_>>()?;
    for p in to_remove {
        just_parents.discard(p.bind(py))?;
    }

    Ok((needed_keys, refcounts, just_parents, missing_keys))
}

/// Release satisfied parents for `_MPDiffGenerator._process_one_record`.
///
/// For each non-ghost parent key, decrement its refcount in `refcounts`. When
/// the refcount reaches zero, pop the cached value from `chunks` (last
/// child); otherwise fetch (not pop) the still-shared cached value. Mutates
/// `refcounts` and `chunks` in place.
#[pyfunction]
fn mpdiff_collect_parent_chunks<'py>(
    py: Python<'py>,
    parent_keys: &Bound<'py, PyAny>,
    ghost_parents: &Bound<'py, PySet>,
    refcounts: &Bound<'py, PyDict>,
    chunks: &Bound<'py, PyDict>,
) -> PyResult<Py<PyAny>> {
    let out = PyList::empty(py);
    for p in parent_keys.try_iter()? {
        let p = p?;
        if ghost_parents.contains(&p)? {
            continue;
        }
        let refcount: i64 = refcounts
            .get_item(&p)?
            .ok_or_else(|| {
                pyo3::exceptions::PyKeyError::new_err(format!("missing refcount for {:?}", p))
            })?
            .extract()?;
        let parent_value = if refcount == 1 {
            let value = chunks.get_item(&p)?.ok_or_else(|| {
                pyo3::exceptions::PyKeyError::new_err(format!("missing chunks for {:?}", p))
            })?;
            refcounts.del_item(&p)?;
            chunks.del_item(&p)?;
            value
        } else {
            refcounts.set_item(&p, refcount - 1)?;
            chunks.get_item(&p)?.ok_or_else(|| {
                pyo3::exceptions::PyKeyError::new_err(format!("missing chunks for {:?}", p))
            })?
        };
        out.append(parent_value)?;
    }
    Ok(out.into_any().unbind())
}

/// A `KeyMapper` that always returns the same path. Mirrors the Python
/// `bzrformats.versionedfile.ConstantMapper`.
#[pyclass(name = "ConstantMapper", module = "bzrformats._bzr_rs.versionedfile")]
#[derive(Clone)]
struct PyConstantMapper {
    result: String,
}

#[pymethods]
impl PyConstantMapper {
    #[new]
    fn new(result: String) -> Self {
        Self { result }
    }

    fn map(&self, _key: &Bound<'_, PyAny>) -> String {
        self.result.clone()
    }

    /// Property kept for parity with the previous Python attribute access.
    #[getter]
    fn _result(&self) -> &str {
        &self.result
    }
}

/// A `KeyMapper` that uses the first key element as the storage path.
/// Mirrors the Python `bzrformats.versionedfile.PrefixMapper`.
#[pyclass(name = "PrefixMapper", module = "bzrformats._bzr_rs.versionedfile")]
#[derive(Clone)]
struct PyPrefixMapper;

#[pymethods]
impl PyPrefixMapper {
    #[new]
    fn new() -> Self {
        Self
    }

    fn map(&self, key: &Bound<'_, PyAny>) -> PyResult<String> {
        let first = key.get_item(0)?.cast_into::<PyBytes>()?;
        Ok(bazaar::key_mapper::prefix_map(first.as_bytes()))
    }

    fn unmap<'py>(&self, py: Python<'py>, partition_id: &str) -> PyResult<Bound<'py, PyTuple>> {
        let bytes = bazaar::key_mapper::prefix_unmap(partition_id);
        PyTuple::new(py, [PyBytes::new(py, &bytes)])
    }
}

/// A `KeyMapper` that prefixes the path with a two-hex adler32 bucket.
/// Mirrors the Python `bzrformats.versionedfile.HashPrefixMapper`.
#[pyclass(name = "HashPrefixMapper", module = "bzrformats._bzr_rs.versionedfile")]
#[derive(Clone)]
struct PyHashPrefixMapper;

#[pymethods]
impl PyHashPrefixMapper {
    #[new]
    fn new() -> Self {
        Self
    }

    fn map(&self, key: &Bound<'_, PyAny>) -> PyResult<String> {
        let first = key.get_item(0)?.cast_into::<PyBytes>()?;
        Ok(bazaar::key_mapper::hash_prefix_map(first.as_bytes()))
    }

    fn unmap<'py>(&self, py: Python<'py>, partition_id: &str) -> PyResult<Bound<'py, PyTuple>> {
        let bytes = bazaar::key_mapper::hash_prefix_unmap(partition_id);
        PyTuple::new(py, [PyBytes::new(py, &bytes)])
    }
}

/// A `KeyMapper` that escapes non-filesystem-safe bytes before bucketing.
/// Mirrors the Python `bzrformats.versionedfile.HashEscapedPrefixMapper`.
#[pyclass(
    name = "HashEscapedPrefixMapper",
    module = "bzrformats._bzr_rs.versionedfile"
)]
#[derive(Clone)]
struct PyHashEscapedPrefixMapper;

#[pymethods]
impl PyHashEscapedPrefixMapper {
    #[new]
    fn new() -> Self {
        Self
    }

    fn map(&self, key: &Bound<'_, PyAny>) -> PyResult<String> {
        let first = key.get_item(0)?.cast_into::<PyBytes>()?;
        Ok(bazaar::key_mapper::hash_escaped_prefix_map(
            first.as_bytes(),
        ))
    }

    fn unmap<'py>(&self, py: Python<'py>, partition_id: &str) -> PyResult<Bound<'py, PyTuple>> {
        let bytes = bazaar::key_mapper::hash_escaped_prefix_unmap(partition_id);
        PyTuple::new(py, [PyBytes::new(py, &bytes)])
    }
}

/// `VersionedFiles` adapter that defers `get_parent_map` and `get_lines`
/// to two Python callables. Mirrors
/// `bzrformats.versionedfile.VirtualVersionedFiles`.
///
/// External callers see the tuple-keyed `VersionedFiles` API. Internally
/// the callbacks operate on bare bytes keys; this binding handles the
/// `(k,) <-> k` rewrapping at the boundary.
///
/// `add_lines`, `add_mpdiffs`, `insert_record_stream` and other write
/// paths raise `NotImplementedError`, matching the Python implementation.
#[pyclass(
    name = "VirtualVersionedFiles",
    extends = PyVersionedFilesBase,
    module = "bzrformats._bzr_rs.versionedfile"
)]
struct PyVirtualVersionedFiles {
    get_parent_map_cb: Py<PyAny>,
    get_lines_cb: Py<PyAny>,
}

impl bazaar::versionedfile::VersionedFiles for PyVirtualVersionedFiles {
    fn get_parent_map(
        &self,
        keys: &[Key],
    ) -> Result<std::collections::HashMap<Key, Vec<Key>>, bazaar::knit::KnitError> {
        Python::attach(|py| {
            // The Python callback expects an iterable of bare bytes keys
            // (the tuple wrapping is this adapter's concern, not the
            // caller's).
            let py_keys = PyList::empty(py);
            for k in keys {
                let bare = key_single_bytes(k)?;
                py_keys
                    .append(PyBytes::new(py, bare))
                    .map_err(|e| vf_err_from_py(py, e))?;
            }
            let raw = self
                .get_parent_map_cb
                .bind(py)
                .call1((py_keys,))
                .map_err(|e| vf_err_from_py(py, e))?;
            let dict = raw
                .cast_into::<PyDict>()
                .map_err(|e| vf_err_from_py(py, e.into()))?;
            let mut out = std::collections::HashMap::new();
            for (k, v) in dict.iter() {
                let k_bytes: Vec<u8> = k.extract().map_err(|e| vf_err_from_py(py, e))?;
                let parents = if v.is_none() {
                    Vec::new()
                } else {
                    let mut ps = Vec::new();
                    for p in v.try_iter().map_err(|e| vf_err_from_py(py, e))? {
                        let p = p.map_err(|e| vf_err_from_py(py, e))?;
                        let pb: Vec<u8> = p.extract().map_err(|e| vf_err_from_py(py, e))?;
                        ps.push(Key::Fixed(vec![pb]));
                    }
                    ps
                };
                out.insert(Key::Fixed(vec![k_bytes]), parents);
            }
            Ok(out)
        })
    }

    fn get_record_stream(
        &self,
        keys: &[Key],
        _ordering: &str,
        _include_delta_closure: bool,
    ) -> Result<
        Box<dyn Iterator<Item = Result<Box<dyn ContentFactory>, bazaar::knit::KnitError>>>,
        bazaar::knit::KnitError,
    > {
        let mut records: Vec<Result<Box<dyn ContentFactory>, bazaar::knit::KnitError>> = Vec::new();
        Python::attach(|py| -> Result<(), bazaar::knit::KnitError> {
            for key in keys {
                let bare = key_single_bytes(key)?;
                let result = self
                    .get_lines_cb
                    .bind(py)
                    .call1((PyBytes::new(py, bare),))
                    .map_err(|e| vf_err_from_py(py, e))?;
                if result.is_none() {
                    let factory = bazaar::versionedfile::AbsentContentFactory::new(key.clone());
                    records.push(Ok(Box::new(factory) as Box<dyn ContentFactory>));
                } else {
                    let mut lines = Vec::new();
                    for line in result.try_iter().map_err(|e| vf_err_from_py(py, e))? {
                        let line = line.map_err(|e| vf_err_from_py(py, e))?;
                        let bytes: Vec<u8> = line.extract().map_err(|e| vf_err_from_py(py, e))?;
                        lines.push(bytes);
                    }
                    let sha = bazaar::weave::sha_strings(&lines);
                    let factory = bazaar::versionedfile::ChunkedContentFactory::new(
                        Some(sha),
                        key.clone(),
                        None,
                        lines,
                    );
                    records.push(Ok(Box::new(factory) as Box<dyn ContentFactory>));
                }
            }
            Ok(())
        })?;
        Ok(Box::new(records.into_iter()))
    }

    fn get_sha1s(
        &self,
        keys: &[Key],
    ) -> Result<std::collections::HashMap<Key, Vec<u8>>, bazaar::knit::KnitError> {
        Python::attach(|py| {
            let mut out = std::collections::HashMap::new();
            for key in keys {
                let bare = key_single_bytes(key)?;
                let result = self
                    .get_lines_cb
                    .bind(py)
                    .call1((PyBytes::new(py, bare),))
                    .map_err(|e| vf_err_from_py(py, e))?;
                if !result.is_none() {
                    let mut lines: Vec<Vec<u8>> = Vec::new();
                    for line in result.try_iter().map_err(|e| vf_err_from_py(py, e))? {
                        let line = line.map_err(|e| vf_err_from_py(py, e))?;
                        let bytes: Vec<u8> = line.extract().map_err(|e| vf_err_from_py(py, e))?;
                        lines.push(bytes);
                    }
                    out.insert(key.clone(), bazaar::weave::sha_strings(&lines));
                }
            }
            Ok(out)
        })
    }

    fn keys(&self) -> Result<Vec<Key>, bazaar::knit::KnitError> {
        Err(bazaar::knit::KnitError::NotImplemented(
            "VirtualVersionedFiles.keys",
        ))
    }

    fn add_lines(
        &self,
        _key: &Key,
        _parents: Option<&[Key]>,
        _lines: &[Vec<u8>],
    ) -> Result<(Vec<u8>, usize), bazaar::knit::KnitError> {
        Err(bazaar::knit::KnitError::NotImplemented(
            "VirtualVersionedFiles.add_lines",
        ))
    }

    fn insert_record_stream(
        &self,
        _stream: Box<dyn Iterator<Item = Box<dyn ContentFactory>>>,
    ) -> Result<(), bazaar::knit::KnitError> {
        Err(bazaar::knit::KnitError::NotImplemented(
            "VirtualVersionedFiles.insert_record_stream",
        ))
    }

    fn iter_lines_added_or_present_in_keys(
        &self,
        keys: &[Key],
    ) -> Result<Vec<(Vec<u8>, Key)>, bazaar::knit::KnitError> {
        Python::attach(|py| {
            let mut out = Vec::new();
            for key in keys {
                let bare = key_single_bytes(key)?;
                let result = self
                    .get_lines_cb
                    .bind(py)
                    .call1((PyBytes::new(py, bare),))
                    .map_err(|e| vf_err_from_py(py, e))?;
                if !result.is_none() {
                    for line in result.try_iter().map_err(|e| vf_err_from_py(py, e))? {
                        let line = line.map_err(|e| vf_err_from_py(py, e))?;
                        let bytes: Vec<u8> = line.extract().map_err(|e| vf_err_from_py(py, e))?;
                        out.push((bytes, key.clone()));
                    }
                }
            }
            Ok(out)
        })
    }

    fn annotate(&self, _key: &Key) -> Result<Vec<(Key, Vec<u8>)>, bazaar::knit::KnitError> {
        Err(bazaar::knit::KnitError::NotImplemented(
            "VirtualVersionedFiles.annotate",
        ))
    }

    fn check(&self) -> Result<(), bazaar::knit::KnitError> {
        Ok(())
    }
}

fn key_single_bytes(key: &Key) -> Result<&[u8], bazaar::knit::KnitError> {
    let segs = match key {
        Key::Fixed(v) | Key::ContentAddressed(v) => v,
    };
    if segs.len() != 1 {
        return Err(bazaar::knit::KnitError::Corrupt(format!(
            "VirtualVersionedFiles expects single-segment keys, got {:?}",
            key
        )));
    }
    Ok(&segs[0])
}

#[pymethods]
impl PyVirtualVersionedFiles {
    #[new]
    fn new(get_parent_map: Py<PyAny>, get_lines: Py<PyAny>) -> PyClassInitializer<Self> {
        vf_initializer().add_subclass(Self {
            get_parent_map_cb: get_parent_map,
            get_lines_cb: get_lines,
        })
    }

    #[pyo3(signature = (progressbar=None))]
    fn check(&self, progressbar: Option<Py<PyAny>>) -> bool {
        let _ = progressbar;
        true
    }

    fn add_mpdiffs(&self, _records: Py<PyAny>) -> PyResult<()> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "VirtualVersionedFiles.add_mpdiffs",
        ))
    }

    #[pyo3(signature = (
        _key,
        _parents,
        _lines,
        parent_texts=None,
        left_matching_blocks=None,
        nostore_sha=None,
        random_id=false,
        check_content=true,
    ))]
    #[allow(clippy::too_many_arguments)]
    fn add_lines(
        &self,
        _key: Py<PyAny>,
        _parents: Py<PyAny>,
        _lines: Py<PyAny>,
        parent_texts: Option<Py<PyAny>>,
        left_matching_blocks: Option<Py<PyAny>>,
        nostore_sha: Option<Py<PyAny>>,
        random_id: bool,
        check_content: bool,
    ) -> PyResult<()> {
        let _ = (
            parent_texts,
            left_matching_blocks,
            nostore_sha,
            random_id,
            check_content,
        );
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "VirtualVersionedFiles.add_lines",
        ))
    }

    fn insert_record_stream(&self, _stream: Py<PyAny>) -> PyResult<()> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "VirtualVersionedFiles.insert_record_stream",
        ))
    }

    fn get_parent_map<'py>(
        &self,
        py: Python<'py>,
        keys: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyDict>> {
        use bazaar::versionedfile::VersionedFiles;
        let mut rust_keys: Vec<Key> = Vec::new();
        for k in keys.try_iter()? {
            rust_keys.push(k?.extract()?);
        }
        let raw = <Self as bazaar::versionedfile::VersionedFiles>::get_parent_map(self, &rust_keys)
            .map_err(crate::knit::knit_err_to_py)?;
        let result = PyDict::new(py);
        for (k, parents) in raw {
            let py_k = k.into_pyobject(py)?;
            let py_parents_vec: Vec<Bound<'py, PyTuple>> = parents
                .into_iter()
                .map(|p| p.into_pyobject(py))
                .collect::<Result<_, _>>()?;
            let py_parents = PyTuple::new(py, py_parents_vec)?;
            result.set_item(py_k, py_parents)?;
        }
        Ok(result)
    }

    fn get_sha1s<'py>(
        &self,
        py: Python<'py>,
        keys: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyDict>> {
        use bazaar::versionedfile::VersionedFiles;
        let mut rust_keys: Vec<Key> = Vec::new();
        for k in keys.try_iter()? {
            rust_keys.push(k?.extract()?);
        }
        let raw = <Self as bazaar::versionedfile::VersionedFiles>::get_sha1s(self, &rust_keys)
            .map_err(crate::knit::knit_err_to_py)?;
        let result = PyDict::new(py);
        for (k, sha) in raw {
            let py_k = k.into_pyobject(py)?;
            result.set_item(py_k, PyBytes::new(py, &sha))?;
        }
        Ok(result)
    }

    fn get_record_stream<'py>(
        &self,
        py: Python<'py>,
        keys: Bound<'py, PyAny>,
        _ordering: &str,
        _include_delta_closure: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let out = PyList::empty(py);
        for k in keys.try_iter()? {
            let key: Key = k?.extract()?;
            let bare = match &key {
                Key::Fixed(v) | Key::ContentAddressed(v) => {
                    if v.len() != 1 {
                        return Err(pyo3::exceptions::PyValueError::new_err(format!(
                            "VirtualVersionedFiles expects single-segment keys, got {:?}",
                            key
                        )));
                    }
                    v[0].clone()
                }
            };
            let result = self
                .get_lines_cb
                .bind(py)
                .call1((PyBytes::new(py, &bare),))?;
            let wrapped = if result.is_none() {
                let factory = bazaar::versionedfile::AbsentContentFactory::new(key);
                let init = PyClassInitializer::from(AbstractContentFactory(Box::new(factory)))
                    .add_subclass(AbsentContentFactory);
                Bound::new(py, init)?.into_any()
            } else {
                let mut lines: Vec<Vec<u8>> = Vec::new();
                for line in result.try_iter()? {
                    let line = line?;
                    let bytes: Vec<u8> = line.extract()?;
                    lines.push(bytes);
                }
                let sha = bazaar::weave::sha_strings(&lines);
                let factory =
                    bazaar::versionedfile::ChunkedContentFactory::new(Some(sha), key, None, lines);
                let init = PyClassInitializer::from(AbstractContentFactory(Box::new(factory)))
                    .add_subclass(ChunkedContentFactory);
                Bound::new(py, init)?.into_any()
            };
            out.append(wrapped)?;
        }
        Ok(out.into_any().call_method0("__iter__")?)
    }

    #[pyo3(signature = (keys, pb=None))]
    fn iter_lines_added_or_present_in_keys<'py>(
        &self,
        py: Python<'py>,
        keys: Bound<'py, PyAny>,
        pb: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        use bazaar::versionedfile::VersionedFiles;
        let _ = pb;
        let mut rust_keys: Vec<Key> = Vec::new();
        for k in keys.try_iter()? {
            rust_keys.push(k?.extract()?);
        }
        let pairs =
            <Self as bazaar::versionedfile::VersionedFiles>::iter_lines_added_or_present_in_keys(
                self, &rust_keys,
            )
            .map_err(crate::knit::knit_err_to_py)?;
        let out = PyList::empty(py);
        for (line, key) in pairs {
            // Match Python: yield (line, bare_bytes_key).
            let bare = key_single_bytes(&key).map_err(crate::knit::knit_err_to_py)?;
            let py_line = PyBytes::new(py, &line);
            let py_key = PyBytes::new(py, bare);
            out.append(PyTuple::new(py, [py_line.into_any(), py_key.into_any()])?)?;
        }
        Ok(out.into_any().call_method0("__iter__")?)
    }
}

#[pyfunction]
fn network_bytes_to_kind_and_offset(network_bytes: &[u8]) -> (String, usize) {
    bazaar::versionedfile::network_bytes_to_kind_and_offset(network_bytes)
}

#[pyfunction]
fn fulltext_network_to_record<'a>(
    py: Python<'a>,
    _kind: &'a str,
    bytes: &'a [u8],
    line_end: usize,
) -> Vec<Bound<'a, FulltextContentFactory>> {
    let record = bazaar::versionedfile::fulltext_network_to_record(bytes, line_end);

    let sub = PyClassInitializer::from(AbstractContentFactory(Box::new(record)))
        .add_subclass(FulltextContentFactory);

    vec![Bound::new(py, sub).unwrap()]
}

/// Raise `TypeError` if any line being added to a versioned file is not
/// `bytes`. Mirrors `VersionedFiles._check_lines_not_unicode`; the check is
/// inherently a Python type test, so it stays at the marshalling boundary
/// rather than in the pure crate.
#[pyfunction]
fn check_lines_not_unicode(lines: &Bound<'_, PyAny>) -> PyResult<()> {
    for line in lines.try_iter()? {
        if !line?.is_instance_of::<PyBytes>() {
            return Err(pyo3::exceptions::PyTypeError::new_err("lines"));
        }
    }
    Ok(())
}

/// Raise `ValueError` if any line carries an embedded newline (a newline
/// anywhere but its final byte). Mirrors `VersionedFiles._check_lines_are_lines`.
#[pyfunction]
fn check_lines_are_lines(lines: Vec<Vec<u8>>) -> PyResult<()> {
    if bazaar::versionedfile::check_lines_are_lines(&lines) {
        Ok(())
    } else {
        Err(pyo3::exceptions::PyValueError::new_err(
            "lines contain newlines",
        ))
    }
}

/// Reference-count bookkeeping for compression-parent satisfaction during
/// stream insertion. Python-facing counterpart of the pure-Rust
/// `bazaar::versionedfile::KeyRefs`; stores Python tuples directly via
/// `PyDict`/`PySet` so hashing delegates to the Python tuple hash.
///
/// Mirrors `bzrformats.versionedfile._KeyRefs` one-to-one. `refs` maps
/// each referenced parent key to the set of child keys that reference it,
/// and `new_keys` (when tracking is enabled) remembers every key added.
#[pyclass(name = "KeyRefs")]
pub(crate) struct KeyRefs {
    refs: Py<PyDict>,
    new_keys: Option<Py<PySet>>,
}

impl KeyRefs {
    pub(crate) fn empty(py: Python<'_>) -> PyResult<Self> {
        Ok(Self {
            refs: PyDict::new(py).unbind(),
            new_keys: None,
        })
    }

    pub(crate) fn new_rust(py: Python<'_>, track_new_keys: bool) -> PyResult<Self> {
        Ok(Self {
            refs: PyDict::new(py).unbind(),
            new_keys: if track_new_keys {
                Some(PySet::empty(py)?.unbind())
            } else {
                None
            },
        })
    }

    pub(crate) fn add_references_rust<'py>(
        &self,
        py: Python<'py>,
        key: Bound<'py, PyAny>,
        refs: Bound<'py, PyAny>,
    ) -> PyResult<()> {
        self.add_references(py, key, refs)
    }

    pub(crate) fn add_key_rust<'py>(
        &self,
        py: Python<'py>,
        key: Bound<'py, PyAny>,
    ) -> PyResult<()> {
        self.add_key(py, key)
    }

    pub(crate) fn get_unsatisfied_refs_rust<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.get_unsatisfied_refs(py)
    }

    pub(crate) fn satisfy_refs_for_keys_rust<'py>(
        &self,
        py: Python<'py>,
        keys: Bound<'py, PyAny>,
    ) -> PyResult<()> {
        self.satisfy_refs_for_keys(py, keys)
    }
}

#[pymethods]
impl KeyRefs {
    #[new]
    #[pyo3(signature = (track_new_keys = false))]
    fn new(py: Python<'_>, track_new_keys: bool) -> PyResult<Self> {
        Ok(Self {
            refs: PyDict::new(py).unbind(),
            new_keys: if track_new_keys {
                Some(PySet::empty(py)?.unbind())
            } else {
                None
            },
        })
    }

    /// `dict` from parent key to the set of children that reference it.
    /// Exposed as an attribute for parity with the Python implementation,
    /// which callers read directly.
    #[getter]
    fn refs<'py>(&self, py: Python<'py>) -> Bound<'py, PyDict> {
        self.refs.bind(py).clone()
    }

    /// Set of keys added since the last `clear()`, or `None` when this
    /// instance was not constructed with `track_new_keys=True`.
    /// Exposed as an attribute for parity with the Python implementation,
    /// which sets `self.new_keys` directly.
    #[getter(new_keys)]
    fn get_new_keys_attr<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PySet>> {
        self.new_keys.as_ref().map(|s| s.bind(py).clone())
    }

    fn clear(&self, py: Python<'_>) -> PyResult<()> {
        self.refs.bind(py).clear();
        if let Some(new_keys) = self.new_keys.as_ref() {
            new_keys.bind(py).clear();
        }
        Ok(())
    }

    fn add_references<'py>(
        &self,
        py: Python<'py>,
        key: Bound<'py, PyAny>,
        refs: Bound<'py, PyAny>,
    ) -> PyResult<()> {
        let refs_dict = self.refs.bind(py);
        for referenced in refs.try_iter()? {
            let referenced = referenced?;
            let set = match refs_dict.get_item(&referenced)? {
                Some(existing) => existing.cast_into::<PySet>()?,
                None => {
                    let fresh = PySet::empty(py)?;
                    refs_dict.set_item(&referenced, &fresh)?;
                    fresh
                }
            };
            set.add(&key)?;
        }
        self.add_key(py, key)
    }

    fn get_new_keys<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PySet>> {
        self.new_keys.as_ref().map(|s| s.bind(py).clone())
    }

    fn get_unsatisfied_refs<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.refs.bind(py).call_method0("keys")
    }

    fn add_key<'py>(&self, py: Python<'py>, key: Bound<'py, PyAny>) -> PyResult<()> {
        // Satisfy any outstanding references to `key`.
        let refs_dict = self.refs.bind(py);
        if refs_dict.contains(&key)? {
            refs_dict.del_item(&key)?;
        }
        if let Some(new_keys) = self.new_keys.as_ref() {
            new_keys.bind(py).add(&key)?;
        }
        Ok(())
    }

    fn satisfy_refs_for_keys<'py>(&self, py: Python<'py>, keys: Bound<'py, PyAny>) -> PyResult<()> {
        let refs_dict = self.refs.bind(py);
        for key in keys.try_iter()? {
            let key = key?;
            if refs_dict.contains(&key)? {
                refs_dict.del_item(&key)?;
            }
        }
        Ok(())
    }

    fn get_referrers<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PySet>> {
        let out = PySet::empty(py)?;
        for (_k, v) in self.refs.bind(py).iter() {
            let inner = v.cast_into::<PySet>()?;
            for item in inner.iter() {
                out.add(item)?;
            }
        }
        Ok(out)
    }
}

/// Rust `ContentFactory` adapter wrapping a Python `ContentFactory` object.
///
/// The Python factory's metadata (key, parents, sha1, size, storage_kind)
/// is extracted eagerly at construction. Chunks are materialised on first
/// access via `get_bytes_as("chunked")` and cached so the borrowing trait
/// methods can return `Cow::Borrowed` slices.
pub struct PyContentFactory {
    obj: Py<PyAny>,
    key: bazaar::versionedfile::Key,
    parents: Option<Vec<bazaar::versionedfile::Key>>,
    sha1: Option<Vec<u8>>,
    size: Option<usize>,
    storage_kind: String,
    chunks: std::sync::OnceLock<Vec<Vec<u8>>>,
}

impl PyContentFactory {
    /// Wrap a Python `ContentFactory` object, extracting metadata eagerly.
    pub fn from_py(obj: Bound<'_, PyAny>) -> PyResult<Self> {
        let key: bazaar::versionedfile::Key = obj.getattr("key")?.extract()?;
        let parents_obj = obj.getattr("parents")?;
        let parents: Option<Vec<bazaar::versionedfile::Key>> = if parents_obj.is_none() {
            None
        } else {
            Some(parents_obj.extract()?)
        };
        let sha1_obj = obj.getattr("sha1")?;
        let sha1: Option<Vec<u8>> = if sha1_obj.is_none() {
            None
        } else {
            Some(sha1_obj.extract()?)
        };
        let size_obj = obj.getattr("size")?;
        let size: Option<usize> = if size_obj.is_none() {
            None
        } else {
            Some(size_obj.extract()?)
        };
        let storage_kind: String = obj.getattr("storage_kind")?.extract()?;
        Ok(PyContentFactory {
            obj: obj.unbind(),
            key,
            parents,
            sha1,
            size,
            storage_kind,
            chunks: std::sync::OnceLock::new(),
        })
    }

    /// Materialise the record's chunked text, caching it for repeat reads.
    fn ensure_chunks(&self) -> &[Vec<u8>] {
        self.chunks.get_or_init(|| {
            Python::attach(|py| {
                // get_bytes_as("chunked") returns a list of bytes chunks.
                let obj = self.obj.bind(py);
                let mut out = Vec::new();
                if let Ok(result) = obj.call_method1("get_bytes_as", ("chunked",)) {
                    if let Ok(iter) = result.try_iter() {
                        for c in iter.flatten() {
                            if let Ok(bytes) = c.extract::<Vec<u8>>() {
                                out.push(bytes);
                            }
                        }
                    }
                }
                out
            })
        })
    }
}

impl bazaar::versionedfile::ContentFactory for PyContentFactory {
    fn sha1(&self) -> Option<Vec<u8>> {
        self.sha1.clone()
    }
    fn size(&self) -> Option<usize> {
        self.size
    }
    fn key(&self) -> bazaar::versionedfile::Key {
        self.key.clone()
    }
    fn parents(&self) -> Option<Vec<bazaar::versionedfile::Key>> {
        self.parents.clone()
    }
    fn to_fulltext<'a, 'b>(&'a self) -> std::borrow::Cow<'b, [u8]>
    where
        'a: 'b,
    {
        std::borrow::Cow::Owned(self.ensure_chunks().concat())
    }
    fn to_chunks<'a, 'b>(&'a self) -> Box<dyn Iterator<Item = std::borrow::Cow<'b, [u8]>> + 'b>
    where
        'a: 'b,
    {
        Box::new(self.ensure_chunks().iter().map(|c| c.as_slice().into()))
    }
    fn to_lines<'a, 'b>(&'a self) -> Box<dyn Iterator<Item = std::borrow::Cow<'b, [u8]>> + 'b>
    where
        'a: 'b,
    {
        Box::new(
            bazaar::osutils::chunks_to_lines(
                self.ensure_chunks().iter().map(Ok::<_, std::io::Error>),
            )
            .map(|l| l.unwrap()),
        )
    }
    fn into_fulltext(self) -> Vec<u8> {
        self.ensure_chunks().concat()
    }
    fn into_chunks(self) -> Box<dyn Iterator<Item = Vec<u8>>> {
        // Drain the cached chunks (or materialise if not yet cached).
        self.ensure_chunks();
        let chunks = self.chunks.into_inner().unwrap_or_default();
        Box::new(chunks.into_iter())
    }
    fn storage_kind(&self) -> String {
        self.storage_kind.clone()
    }
    fn map_key(&mut self, f: &dyn Fn(bazaar::versionedfile::Key) -> bazaar::versionedfile::Key) {
        self.key = f(self.key.clone());
        self.parents = self.parents.take().map(|v| v.into_iter().map(f).collect());
    }
}

/// Adapter that wraps a Python `VersionedFiles` object so pure-Rust code can
/// call it through the [`bazaar::versionedfile::VersionedFiles`] trait. All
/// methods re-enter the interpreter via [`Python::attach`] and marshal the
/// arguments/results in both directions.
///
/// Used by the groupcompress / knit pyclasses to register their Python
/// fallbacks on the pure store's fallback list, so trait-driven code paths
/// (e.g. `get_sha1s`, `iter_lines_added_or_present_in_keys`, `check`) consult
/// fallbacks correctly.
pub struct PyVersionedFiles {
    obj: Py<PyAny>,
}

impl PyVersionedFiles {
    pub fn new(obj: Py<PyAny>) -> Self {
        Self { obj }
    }
}

/// Map a Python exception raised by a `VersionedFiles` call back to a
/// `KnitError` variant the trait can carry. The trait error type is
/// `KnitError` for historical reasons (it predated a dedicated
/// VersionedFile error enum).
fn vf_err_from_py(py: Python<'_>, err: PyErr) -> bazaar::knit::KnitError {
    crate::knit::knit_err_from_py(py, err)
}

impl bazaar::versionedfile::VersionedFiles for PyVersionedFiles {
    fn get_parent_map(
        &self,
        keys: &[Key],
    ) -> Result<std::collections::HashMap<Key, Vec<Key>>, bazaar::knit::KnitError> {
        Python::attach(|py| {
            let py_keys = PySet::empty(py).map_err(|e| vf_err_from_py(py, e))?;
            for k in keys {
                let pk = k
                    .clone()
                    .into_pyobject(py)
                    .map_err(|e| vf_err_from_py(py, e))?;
                py_keys.add(pk).map_err(|e| vf_err_from_py(py, e))?;
            }
            let result = self
                .obj
                .bind(py)
                .call_method1("get_parent_map", (py_keys,))
                .map_err(|e| vf_err_from_py(py, e))?;
            let result = result
                .cast_into::<PyDict>()
                .map_err(|e| vf_err_from_py(py, e.into()))?;
            let mut out = std::collections::HashMap::new();
            for (k, v) in result.iter() {
                let key: Key = k.extract().map_err(|e| vf_err_from_py(py, e))?;
                // A parentless index emits None for parents; map that to an
                // empty Vec to satisfy the trait's signature.
                let parents: Vec<Key> = if v.is_none() {
                    Vec::new()
                } else {
                    let mut ps = Vec::new();
                    for p in v.try_iter().map_err(|e| vf_err_from_py(py, e))? {
                        let p = p.map_err(|e| vf_err_from_py(py, e))?;
                        ps.push(p.extract::<Key>().map_err(|e| vf_err_from_py(py, e))?);
                    }
                    ps
                };
                out.insert(key, parents);
            }
            Ok(out)
        })
    }

    fn get_record_stream(
        &self,
        keys: &[Key],
        ordering: &str,
        include_delta_closure: bool,
    ) -> Result<
        Box<dyn Iterator<Item = Result<Box<dyn ContentFactory>, bazaar::knit::KnitError>>>,
        bazaar::knit::KnitError,
    > {
        // Initiate the Python call eagerly, then return an iterator that
        // pulls one record at a time on demand. This preserves the lazy
        // semantics of Python's get_record_stream so we don't have to
        // materialise the whole closure up front.
        Python::attach(|py| {
            let py_keys = PyList::empty(py);
            for k in keys {
                let pk = k
                    .clone()
                    .into_pyobject(py)
                    .map_err(|e| vf_err_from_py(py, e))?;
                py_keys.append(pk).map_err(|e| vf_err_from_py(py, e))?;
            }
            let stream = self
                .obj
                .bind(py)
                .call_method1(
                    "get_record_stream",
                    (py_keys, ordering, include_delta_closure),
                )
                .map_err(|e| vf_err_from_py(py, e))?;
            Ok(Box::new(PyRecordStream {
                stream: stream.unbind(),
            })
                as Box<
                    dyn Iterator<Item = Result<Box<dyn ContentFactory>, bazaar::knit::KnitError>>,
                >)
        })
    }

    fn get_sha1s(
        &self,
        keys: &[Key],
    ) -> Result<std::collections::HashMap<Key, Vec<u8>>, bazaar::knit::KnitError> {
        Python::attach(|py| {
            let py_keys = PySet::empty(py).map_err(|e| vf_err_from_py(py, e))?;
            for k in keys {
                let pk = k
                    .clone()
                    .into_pyobject(py)
                    .map_err(|e| vf_err_from_py(py, e))?;
                py_keys.add(pk).map_err(|e| vf_err_from_py(py, e))?;
            }
            let result = self
                .obj
                .bind(py)
                .call_method1("get_sha1s", (py_keys,))
                .map_err(|e| vf_err_from_py(py, e))?;
            let result = result
                .cast_into::<PyDict>()
                .map_err(|e| vf_err_from_py(py, e.into()))?;
            let mut out = std::collections::HashMap::new();
            for (k, v) in result.iter() {
                let key: Key = k.extract().map_err(|e| vf_err_from_py(py, e))?;
                let sha1: Vec<u8> = v.extract().map_err(|e| vf_err_from_py(py, e))?;
                out.insert(key, sha1);
            }
            Ok(out)
        })
    }

    fn keys(&self) -> Result<Vec<Key>, bazaar::knit::KnitError> {
        Python::attach(|py| {
            let result = self
                .obj
                .bind(py)
                .call_method0("keys")
                .map_err(|e| vf_err_from_py(py, e))?;
            let mut out = Vec::new();
            for k in result.try_iter().map_err(|e| vf_err_from_py(py, e))? {
                let k = k.map_err(|e| vf_err_from_py(py, e))?;
                out.push(k.extract::<Key>().map_err(|e| vf_err_from_py(py, e))?);
            }
            Ok(out)
        })
    }

    fn add_lines(
        &self,
        key: &Key,
        parents: Option<&[Key]>,
        lines: &[Vec<u8>],
    ) -> Result<(Vec<u8>, usize), bazaar::knit::KnitError> {
        Python::attach(|py| {
            let py_key = key
                .clone()
                .into_pyobject(py)
                .map_err(|e| vf_err_from_py(py, e))?;
            let py_parents = match parents {
                None => py.None().into_bound(py),
                Some(ps) => {
                    let lst = PyList::empty(py);
                    for p in ps {
                        let pp = p
                            .clone()
                            .into_pyobject(py)
                            .map_err(|e| vf_err_from_py(py, e))?;
                        lst.append(pp).map_err(|e| vf_err_from_py(py, e))?;
                    }
                    lst.into_any()
                }
            };
            let py_lines = PyList::empty(py);
            for l in lines {
                py_lines
                    .append(PyBytes::new(py, l))
                    .map_err(|e| vf_err_from_py(py, e))?;
            }
            let result = self
                .obj
                .bind(py)
                .call_method1("add_lines", (py_key, py_parents, py_lines))
                .map_err(|e| vf_err_from_py(py, e))?;
            let result = result
                .cast_into::<PyTuple>()
                .map_err(|e| vf_err_from_py(py, e.into()))?;
            let digest: Vec<u8> = result
                .get_item(0)
                .map_err(|e| vf_err_from_py(py, e))?
                .extract()
                .map_err(|e| vf_err_from_py(py, e))?;
            let text_length: usize = result
                .get_item(1)
                .map_err(|e| vf_err_from_py(py, e))?
                .extract()
                .map_err(|e| vf_err_from_py(py, e))?;
            Ok((digest, text_length))
        })
    }

    fn insert_record_stream(
        &self,
        _stream: Box<dyn Iterator<Item = Box<dyn ContentFactory>>>,
    ) -> Result<(), bazaar::knit::KnitError> {
        // TODO: marshal a Rust ContentFactory stream back into Python and call
        // self.obj.insert_record_stream. No production caller needs this yet
        // (fallbacks are read-only in practice), so leave it unimplemented.
        Err(bazaar::knit::KnitError::NotImplemented(
            "PyVersionedFiles::insert_record_stream",
        ))
    }

    fn iter_lines_added_or_present_in_keys(
        &self,
        keys: &[Key],
    ) -> Result<Vec<(Vec<u8>, Key)>, bazaar::knit::KnitError> {
        Python::attach(|py| {
            let py_keys = PySet::empty(py).map_err(|e| vf_err_from_py(py, e))?;
            for k in keys {
                let pk = k
                    .clone()
                    .into_pyobject(py)
                    .map_err(|e| vf_err_from_py(py, e))?;
                py_keys.add(pk).map_err(|e| vf_err_from_py(py, e))?;
            }
            let result = self
                .obj
                .bind(py)
                .call_method1("iter_lines_added_or_present_in_keys", (py_keys,))
                .map_err(|e| vf_err_from_py(py, e))?;
            let mut out = Vec::new();
            for item in result.try_iter().map_err(|e| vf_err_from_py(py, e))? {
                let tup = item.map_err(|e| vf_err_from_py(py, e))?;
                let tup = tup
                    .cast_into::<PyTuple>()
                    .map_err(|e| vf_err_from_py(py, e.into()))?;
                let line: Vec<u8> = tup
                    .get_item(0)
                    .map_err(|e| vf_err_from_py(py, e))?
                    .extract()
                    .map_err(|e| vf_err_from_py(py, e))?;
                let key: Key = tup
                    .get_item(1)
                    .map_err(|e| vf_err_from_py(py, e))?
                    .extract()
                    .map_err(|e| vf_err_from_py(py, e))?;
                out.push((line, key));
            }
            Ok(out)
        })
    }

    fn annotate(&self, key: &Key) -> Result<Vec<(Key, Vec<u8>)>, bazaar::knit::KnitError> {
        Python::attach(|py| {
            let py_key = key
                .clone()
                .into_pyobject(py)
                .map_err(|e| vf_err_from_py(py, e))?;
            let result = self
                .obj
                .bind(py)
                .call_method1("annotate", (py_key,))
                .map_err(|e| vf_err_from_py(py, e))?;
            let mut out = Vec::new();
            for item in result.try_iter().map_err(|e| vf_err_from_py(py, e))? {
                let tup = item.map_err(|e| vf_err_from_py(py, e))?;
                let tup = tup
                    .cast_into::<PyTuple>()
                    .map_err(|e| vf_err_from_py(py, e.into()))?;
                let key: Key = tup
                    .get_item(0)
                    .map_err(|e| vf_err_from_py(py, e))?
                    .extract()
                    .map_err(|e| vf_err_from_py(py, e))?;
                let line: Vec<u8> = tup
                    .get_item(1)
                    .map_err(|e| vf_err_from_py(py, e))?
                    .extract()
                    .map_err(|e| vf_err_from_py(py, e))?;
                out.push((key, line));
            }
            Ok(out)
        })
    }

    fn clear_cache(&self) {
        Python::attach(|py| {
            let _ = self.obj.bind(py).call_method0("clear_cache");
        })
    }

    fn check(&self) -> Result<(), bazaar::knit::KnitError> {
        Python::attach(|py| {
            self.obj
                .bind(py)
                .call_method0("check")
                .map(|_| ())
                .map_err(|e| vf_err_from_py(py, e))
        })
    }
}

/// Lazy iterator over a Python `get_record_stream` result. Yields one
/// `ContentFactory` per `__next__` call until the Python iterator is
/// exhausted or raises.
struct PyRecordStream {
    stream: Py<PyAny>,
}

impl Iterator for PyRecordStream {
    type Item = Result<Box<dyn ContentFactory>, bazaar::knit::KnitError>;

    fn next(&mut self) -> Option<Self::Item> {
        Python::attach(|py| {
            let stream = self.stream.bind(py);
            let record = match stream.call_method0("__next__") {
                Ok(r) => r,
                Err(e) if e.is_instance_of::<pyo3::exceptions::PyStopIteration>(py) => return None,
                Err(e) => return Some(Err(vf_err_from_py(py, e))),
            };
            Some(record_to_content_factory(py, &record))
        })
    }
}

fn record_to_content_factory(
    py: Python<'_>,
    record: &Bound<'_, PyAny>,
) -> Result<Box<dyn ContentFactory>, bazaar::knit::KnitError> {
    let storage_kind: String = record
        .getattr("storage_kind")
        .map_err(|e| vf_err_from_py(py, e))?
        .extract()
        .map_err(|e| vf_err_from_py(py, e))?;
    let key: Key = record
        .getattr("key")
        .map_err(|e| vf_err_from_py(py, e))?
        .extract()
        .map_err(|e| vf_err_from_py(py, e))?;
    if storage_kind == "absent" {
        return Ok(Box::new(bazaar::versionedfile::AbsentContentFactory::new(
            key,
        )));
    }
    let parents_obj = record
        .getattr("parents")
        .map_err(|e| vf_err_from_py(py, e))?;
    let parents: Option<Vec<Key>> = if parents_obj.is_none() {
        None
    } else {
        let mut ps = Vec::new();
        for p in parents_obj.try_iter().map_err(|e| vf_err_from_py(py, e))? {
            let p = p.map_err(|e| vf_err_from_py(py, e))?;
            ps.push(p.extract::<Key>().map_err(|e| vf_err_from_py(py, e))?);
        }
        Some(ps)
    };
    let fulltext: Vec<u8> = record
        .call_method1("get_bytes_as", ("fulltext",))
        .map_err(|e| vf_err_from_py(py, e))?
        .extract()
        .map_err(|e| vf_err_from_py(py, e))?;
    let sha1_obj = record.getattr("sha1").map_err(|e| vf_err_from_py(py, e))?;
    let sha1: Option<Vec<u8>> = if sha1_obj.is_none() {
        None
    } else {
        Some(sha1_obj.extract().map_err(|e| vf_err_from_py(py, e))?)
    };
    Ok(Box::new(
        bazaar::versionedfile::FulltextContentFactory::new(sha1, key, parents, fulltext),
    ))
}

/// Resolve the full ancestry of `keys` against a Python `VersionedFiles`,
/// returning a `{key: parents}` dict.
///
/// Drives the `get_parent_map` walk in Rust; mirrors the loop in
/// `VersionedFiles.get_known_graph_ancestry`. The caller wraps the result
/// in a `KnownGraph`.
#[pyfunction]
fn known_graph_ancestry_map<'py>(
    py: Python<'py>,
    vf: Py<PyAny>,
    keys: Vec<Key>,
) -> PyResult<Bound<'py, PyDict>> {
    use bazaar::versionedfile::VersionedFiles;
    let wrapped = PyVersionedFiles::new(vf);
    let parent_map = wrapped
        .known_graph_ancestry_map(&keys)
        .map_err(crate::knit::knit_err_to_py)?;
    let out = PyDict::new(py);
    for (key, parents) in parent_map {
        out.set_item(key, parents)?;
    }
    Ok(out)
}

pyo3::import_exception!(bzrformats._bzr_rs.errors, VersionedFileInvalidChecksum);

/// Drive `VersionedFiles.add_mpdiffs(records)` in Rust.
///
/// The pure-crate helpers [`add_mpdiffs_build`] and [`add_mpdiffs_prepare`]
/// own the business logic (mpvf assembly, needed-parent discovery,
/// reconstruction, matching-blocks computation). This wrapper handles only
/// the Python ABI: extracting records, fetching missing parents via the
/// caller's Python `get_record_stream`, dispatching `vf.add_lines` with the
/// `parent_texts` / `left_matching_blocks` kwargs, and raising
/// `VersionedFileInvalidChecksum` on sha1 mismatch.
#[pyfunction]
fn add_mpdiffs(py: Python<'_>, vf: Py<PyAny>, records: Bound<'_, PyAny>) -> PyResult<()> {
    use bazaar::versionedfile::{add_mpdiffs_build, add_mpdiffs_prepare, MpdiffRecord};

    let mut rs: Vec<MpdiffRecord> = Vec::new();
    for item in records.try_iter()? {
        let tup = item?.cast_into::<PyTuple>()?;
        let key: Key = tup.get_item(0)?.extract()?;
        let parents_obj = tup.get_item(1)?;
        let mut parents: Vec<Key> = Vec::new();
        for p in parents_obj.try_iter()? {
            parents.push(p?.extract::<Key>()?);
        }
        let expected_sha1: Vec<u8> = tup.get_item(2)?.extract()?;
        let mp_obj = tup.get_item(3)?;
        let hunks = mp_obj.getattr("hunks")?.cast_into::<PyList>()?;
        let diff = crate::multiparent::py_hunks_to_rust(&hunks)?;
        rs.push(MpdiffRecord {
            key,
            parents,
            expected_sha1,
            diff,
        });
    }

    let (mut mpvf, needed) = add_mpdiffs_build(&rs);

    if !needed.is_empty() {
        use bazaar::versionedfile::VersionedFiles;
        let wrapped = PyVersionedFiles::new(vf.clone_ref(py));
        let stream = wrapped
            .get_record_stream(&needed, "unordered", true)
            .map_err(crate::knit::knit_err_to_py)?;
        for rec in stream {
            let rec = rec.map_err(crate::knit::knit_err_to_py)?;
            if rec.storage_kind() == "absent" {
                continue;
            }
            // `get_bytes_as("lines")` semantics: split the fulltext into
            // newline-terminated lines.
            let lines: Vec<Vec<u8>> = rec.to_lines().map(|l| l.into_owned()).collect();
            mpvf.add_version(lines, rec.key(), vec![], None, false)
                .map_err(crate::multiparent::reconstruct_err)?;
        }
    }

    let prepared =
        add_mpdiffs_prepare(&mut mpvf, &rs).map_err(crate::multiparent::reconstruct_err)?;

    // Dispatch each prepared row through Python. `vf_parents` threads the
    // opaque `parent_texts` token returned by add_lines back into
    // subsequent calls so the implementation can avoid re-fetching.
    let vf_bound = vf.bind(py);
    let vf_parents = PyDict::new(py);
    for row in &prepared {
        let left_matching_blocks_obj: Py<PyAny> = match &row.left_matching_blocks {
            Some(blocks) => PyList::new(
                py,
                blocks
                    .iter()
                    .map(|t| PyTuple::new(py, [t.0, t.1, t.2]).unwrap()),
            )?
            .into_any()
            .unbind(),
            None => py.None(),
        };

        let py_key = row.key.clone().into_pyobject(py)?;
        let py_parents = PyList::empty(py);
        for p in &row.parents {
            py_parents.append(p.clone().into_pyobject(py)?)?;
        }
        let py_lines = PyList::empty(py);
        for l in &row.lines {
            py_lines.append(PyBytes::new(py, l))?;
        }
        let kwargs = PyDict::new(py);
        kwargs.set_item("left_matching_blocks", left_matching_blocks_obj)?;
        let result = vf_bound
            .call_method(
                "add_lines",
                (py_key, py_parents, py_lines, vf_parents.clone()),
                Some(&kwargs),
            )?
            .cast_into::<PyTuple>()?;
        let version_sha1: Vec<u8> = result.get_item(0)?.extract()?;
        if version_sha1 != row.expected_sha1 {
            let version_repr = format!("{:?}", row.key);
            return Err(VersionedFileInvalidChecksum::new_err(version_repr));
        }
        let version_text = result.get_item(2)?;
        let key_py = row.key.clone().into_pyobject(py)?;
        vf_parents.set_item(key_py, version_text)?;
    }

    Ok(())
}

/// Drive `VersionedFile.add_mpdiffs(records)` (the singular flavour) in Rust.
///
/// Mirrors the legacy `VersionedFile.add_mpdiffs` body. Records carry bytes
/// `version_id`s rather than key tuples, the parent fetch goes through
/// `_get_lf_split_line_list` instead of `get_record_stream`, ghosts fall back
/// from `add_lines_with_ghosts` to `add_lines` on `NotImplementedError`, and
/// sha1 verification is post-hoc via `get_sha1s`.
///
/// The pure-crate helpers `add_mpdiffs_build` and `add_mpdiffs_prepare`
/// still do the mpvf assembly, needed-parent discovery, reconstruction, and
/// left-matching-blocks computation; we just wrap each `version_id` as a
/// single-element `Key` so the same algorithm applies.
#[pyfunction]
fn add_mpdiffs_singular(py: Python<'_>, vf: Py<PyAny>, records: Bound<'_, PyAny>) -> PyResult<()> {
    use bazaar::versionedfile::{add_mpdiffs_build, add_mpdiffs_prepare, MpdiffRecord};

    // Wrap a bytes version_id as a Key::Fixed([version_id]).
    fn wrap(version_id: Vec<u8>) -> Key {
        Key::Fixed(vec![version_id])
    }
    // Unwrap a single-element Key back into its bytes.
    fn unwrap(key: &Key) -> &[u8] {
        key.segments()
            .first()
            .map(Vec::as_slice)
            .unwrap_or_default()
    }

    let mut rs: Vec<MpdiffRecord> = Vec::new();
    let mut version_ids: Vec<Vec<u8>> = Vec::new();
    for item in records.try_iter()? {
        let tup = item?.cast_into::<PyTuple>()?;
        let version_id: Vec<u8> = tup.get_item(0)?.cast_into::<PyBytes>()?.as_bytes().to_vec();
        let parents_obj = tup.get_item(1)?;
        let mut parents: Vec<Key> = Vec::new();
        for p in parents_obj.try_iter()? {
            parents.push(wrap(p?.cast_into::<PyBytes>()?.as_bytes().to_vec()));
        }
        let expected_sha1: Vec<u8> = tup.get_item(2)?.extract()?;
        let mp_obj = tup.get_item(3)?;
        let hunks = mp_obj.getattr("hunks")?.cast_into::<PyList>()?;
        let diff = crate::multiparent::py_hunks_to_rust(&hunks)?;
        version_ids.push(version_id.clone());
        rs.push(MpdiffRecord {
            key: wrap(version_id),
            parents,
            expected_sha1,
            diff,
        });
    }

    let (mut mpvf, needed) = add_mpdiffs_build(&rs);

    if !needed.is_empty() {
        // Filter ghosts via vf.get_parent_map(needed), then fetch the
        // present parents' lines via _get_lf_split_line_list.
        let needed_bytes: Vec<&[u8]> = needed.iter().map(|k| unwrap(k)).collect();
        let needed_py = PyList::empty(py);
        for b in &needed_bytes {
            needed_py.append(PyBytes::new(py, b))?;
        }
        let parent_map = vf
            .bind(py)
            .call_method1("get_parent_map", (needed_py,))?
            .cast_into::<PyDict>()?;
        let mut present: Vec<Vec<u8>> = Vec::new();
        for k in parent_map.keys() {
            present.push(k.cast_into::<PyBytes>()?.as_bytes().to_vec());
        }
        if !present.is_empty() {
            let present_py = PyList::empty(py);
            for b in &present {
                present_py.append(PyBytes::new(py, b))?;
            }
            let lines_lists = vf
                .bind(py)
                .call_method1("_get_lf_split_line_list", (present_py,))?;
            let lines_vec: Vec<Vec<Vec<u8>>> = lines_lists.extract()?;
            for (vid, lines) in present.iter().zip(lines_vec.into_iter()) {
                mpvf.add_version(lines, wrap(vid.clone()), vec![], None, false)
                    .map_err(crate::multiparent::reconstruct_err)?;
            }
        }
    }

    let prepared =
        add_mpdiffs_prepare(&mut mpvf, &rs).map_err(crate::multiparent::reconstruct_err)?;

    // Dispatch each prepared row through Python. Try add_lines_with_ghosts
    // first, fall back to add_lines on NotImplementedError so non-ghost-aware
    // backends still work (and fail naturally if data actually has ghosts).
    let vf_bound = vf.bind(py);
    let vf_parents = PyDict::new(py);
    for row in &prepared {
        let left_matching_blocks_obj: Py<PyAny> = match &row.left_matching_blocks {
            Some(blocks) => PyList::new(
                py,
                blocks
                    .iter()
                    .map(|t| PyTuple::new(py, [t.0, t.1, t.2]).unwrap()),
            )?
            .into_any()
            .unbind(),
            None => py.None(),
        };

        let py_version_id = PyBytes::new(py, unwrap(&row.key));
        let py_parents = PyList::empty(py);
        for p in &row.parents {
            py_parents.append(PyBytes::new(py, unwrap(p)))?;
        }
        let py_lines = PyList::empty(py);
        for l in &row.lines {
            py_lines.append(PyBytes::new(py, l))?;
        }
        let kwargs = PyDict::new(py);
        kwargs.set_item("left_matching_blocks", left_matching_blocks_obj)?;
        let result = match vf_bound.call_method(
            "add_lines_with_ghosts",
            (
                py_version_id.clone(),
                py_parents.clone(),
                py_lines.clone(),
                vf_parents.clone(),
            ),
            Some(&kwargs),
        ) {
            Ok(r) => r,
            Err(e) if e.is_instance_of::<pyo3::exceptions::PyNotImplementedError>(py) => vf_bound
                .call_method(
                "add_lines",
                (
                    py_version_id.clone(),
                    py_parents,
                    py_lines,
                    vf_parents.clone(),
                ),
                Some(&kwargs),
            )?,
            Err(e) => return Err(e),
        };
        let result_tuple = result.cast_into::<PyTuple>()?;
        let version_text = result_tuple.get_item(2)?;
        vf_parents.set_item(py_version_id, version_text)?;
    }

    // Post-hoc sha1 check via vf.get_sha1s(versions).
    let versions_py = PyList::empty(py);
    for vid in &version_ids {
        versions_py.append(PyBytes::new(py, vid))?;
    }
    let sha1s = vf_bound
        .call_method1("get_sha1s", (versions_py,))?
        .cast_into::<PyDict>()?;
    for r in &rs {
        let vid_bytes = unwrap(&r.key);
        let actual = sha1s
            .get_item(PyBytes::new(py, vid_bytes))?
            .ok_or_else(|| {
                pyo3::exceptions::PyKeyError::new_err(format!("missing sha1 for {:?}", vid_bytes))
            })?
            .cast_into::<PyBytes>()?
            .as_bytes()
            .to_vec();
        if actual != r.expected_sha1 {
            let version_repr = format!("{:?}", vid_bytes);
            return Err(VersionedFileInvalidChecksum::new_err(version_repr));
        }
    }

    Ok(())
}

/// Drive `VersionedFile.make_mpdiffs(version_ids)` (the singular flavour) in Rust.
///
/// Mirrors the legacy `VersionedFile.make_mpdiffs` body. Records carry bytes
/// `version_id`s rather than key tuples. The Python callbacks invoked are
/// `vf.get_parent_map` (called twice — once for inputs+ghost-filter) and
/// `vf._get_lf_split_line_list` (once, in bulk). The per-record diff
/// computation runs in pure Rust through `MultiParent::from_lines`.
#[pyfunction]
fn make_mpdiffs_singular<'py>(
    py: Python<'py>,
    vf: Py<PyAny>,
    version_ids: Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyList>> {
    use bazaar::multiparent::MultiParent;

    let vf_bound = vf.bind(py);
    let mut requested: Vec<Vec<u8>> = Vec::new();
    for v in version_ids.try_iter()? {
        requested.push(v?.cast_into::<PyBytes>()?.as_bytes().to_vec());
    }

    // First pass: collect all keys we'll need (inputs + their parents).
    let initial_py = PyList::empty(py);
    for v in &requested {
        initial_py.append(PyBytes::new(py, v))?;
    }
    let parent_map_py = vf_bound
        .call_method1("get_parent_map", (initial_py,))?
        .cast_into::<PyDict>()?;

    // Build a Rust-side {version_id -> parent_ids} dict and detect missing inputs.
    let mut parent_map: std::collections::HashMap<Vec<u8>, Vec<Vec<u8>>> =
        std::collections::HashMap::new();
    for (k, vparents) in parent_map_py.iter() {
        let key = k.cast_into::<PyBytes>()?.as_bytes().to_vec();
        let parents: Vec<Vec<u8>> = if vparents.is_none() {
            vec![]
        } else {
            let mut out = Vec::new();
            for p in vparents.try_iter()? {
                out.push(p?.cast_into::<PyBytes>()?.as_bytes().to_vec());
            }
            out
        };
        parent_map.insert(key, parents);
    }
    for v in &requested {
        if !parent_map.contains_key(v) {
            let errors = PyModule::import(py, "bzrformats.errors")?;
            let exc = errors
                .getattr("RevisionNotPresent")?
                .call1((PyBytes::new(py, v), vf_bound.clone()))?;
            return Err(PyErr::from_value(exc));
        }
    }

    // Second pass: get_parent_map(all_keys_including_parents) so we can
    // distinguish present parents from ghosts.
    let mut all_keys: std::collections::HashSet<Vec<u8>> = requested.iter().cloned().collect();
    for parents in parent_map.values() {
        for p in parents {
            all_keys.insert(p.clone());
        }
    }
    let all_keys_py = PyList::empty(py);
    for v in &all_keys {
        all_keys_py.append(PyBytes::new(py, v))?;
    }
    let present_map = vf_bound
        .call_method1("get_parent_map", (all_keys_py,))?
        .cast_into::<PyDict>()?;
    let mut present: Vec<Vec<u8>> = Vec::new();
    for k in present_map.keys() {
        present.push(k.cast_into::<PyBytes>()?.as_bytes().to_vec());
    }
    let present_set: std::collections::HashSet<Vec<u8>> = present.iter().cloned().collect();

    // Bulk-fetch all present keys' lines.
    let present_py = PyList::empty(py);
    for v in &present {
        present_py.append(PyBytes::new(py, v))?;
    }
    let lines_lists = vf_bound.call_method1("_get_lf_split_line_list", (present_py,))?;
    let lines_vec: Vec<Vec<Vec<u8>>> = lines_lists.extract()?;
    let lines: std::collections::HashMap<Vec<u8>, Vec<Vec<u8>>> =
        present.iter().cloned().zip(lines_vec.into_iter()).collect();

    // Now build each diff in pure Rust.
    let module = PyModule::import(py, "bzrformats.multiparent")?;
    let mp_cls = module.getattr("MultiParent")?;
    let new_text_cls = module.getattr("NewText")?;
    let parent_text_cls = module.getattr("ParentText")?;
    let out = PyList::empty(py);
    for version_id in &requested {
        let target = lines.get(version_id).ok_or_else(|| {
            pyo3::exceptions::PyKeyError::new_err(format!("missing lines for {:?}", version_id))
        })?;
        let parent_lines: Vec<Vec<Vec<u8>>> = parent_map[version_id]
            .iter()
            .filter(|p| present_set.contains(*p))
            .map(|p| lines[p].clone())
            .collect();
        let parent_refs: Vec<&[Vec<u8>]> = parent_lines.iter().map(Vec::as_slice).collect();
        let diff = MultiParent::from_lines(target, &parent_refs, None);

        // Materialise into bzrformats.multiparent.MultiParent.
        let hunks = PyList::empty(py);
        for hunk in diff.hunks {
            match hunk {
                bazaar::multiparent::Hunk::NewText(lines) => {
                    let py_lines: Vec<Bound<PyBytes>> =
                        lines.iter().map(|l| PyBytes::new(py, l)).collect();
                    let lines_list = PyList::new(py, py_lines)?;
                    hunks.append(new_text_cls.call1((lines_list,))?)?;
                }
                bazaar::multiparent::Hunk::ParentText {
                    parent,
                    parent_pos,
                    child_pos,
                    num_lines,
                } => {
                    hunks.append(
                        parent_text_cls.call1((parent, parent_pos, child_pos, num_lines))?,
                    )?;
                }
            }
        }
        out.append(mp_cls.call1((hunks,))?)?;
    }
    Ok(out)
}

/// Drive `_MPDiffGenerator.compute_diffs(vf, keys)` in Rust.
///
/// The pure-crate helper [`bazaar::versionedfile::make_mpdiffs`] owns the
/// orchestration (parent-map walk, ghost detection, refcount-based cache
/// release, per-record diff computation). This wrapper marshals the
/// Python `vf` through [`PyVersionedFiles`] and converts the resulting
/// `MultiParent`s into `bzrformats.multiparent.MultiParent(hunks=[...])`
/// instances so the Python caller cannot tell the loop now lives in Rust.
#[pyfunction]
fn make_mpdiffs<'py>(
    py: Python<'py>,
    vf: Py<PyAny>,
    keys: Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyList>> {
    use bazaar::versionedfile::make_mpdiffs as pure_make_mpdiffs;

    let mut ordered_keys: Vec<Key> = Vec::new();
    for k in keys.try_iter()? {
        ordered_keys.push(k?.extract::<Key>()?);
    }

    let wrapped = PyVersionedFiles::new(vf);
    let diffs = pure_make_mpdiffs(&wrapped, &ordered_keys).map_err(crate::knit::knit_err_to_py)?;

    // Materialise into bzrformats.multiparent.MultiParent / NewText /
    // ParentText instances so the Python caller (and tests) see real
    // class instances.
    let module = PyModule::import(py, "bzrformats.multiparent")?;
    let mp_cls = module.getattr("MultiParent")?;
    let new_text_cls = module.getattr("NewText")?;
    let parent_text_cls = module.getattr("ParentText")?;

    let out = PyList::empty(py);
    for diff in diffs {
        let hunks = PyList::empty(py);
        for hunk in diff.hunks {
            match hunk {
                bazaar::multiparent::Hunk::NewText(lines) => {
                    let py_lines: Vec<Bound<PyBytes>> =
                        lines.iter().map(|l| PyBytes::new(py, l)).collect();
                    let lines_list = PyList::new(py, py_lines)?;
                    hunks.append(new_text_cls.call1((lines_list,))?)?;
                }
                bazaar::multiparent::Hunk::ParentText {
                    parent,
                    parent_pos,
                    child_pos,
                    num_lines,
                } => {
                    hunks.append(
                        parent_text_cls.call1((parent, parent_pos, child_pos, num_lines))?,
                    )?;
                }
            }
        }
        out.append(mp_cls.call1((hunks,))?)?;
    }
    Ok(out)
}

/// Sort and group the keys in `parent_map` into groupcompress order
/// (reverse-topological, grouped by key prefix). Mirrors
/// `bzrformats.versionedfile.sort_groupcompress`: bare-bytes keys (used by
/// Weave) are wrapped into single-element tuples for the Rust
/// `sort_gc_optimal`, then unwrapped on the way back.
#[pyfunction]
fn sort_groupcompress<'py>(
    py: Python<'py>,
    parent_map: Bound<'py, PyDict>,
) -> PyResult<Bound<'py, PyList>> {
    // bytes_keys = any(isinstance(k, bytes) for k in parent_map)
    let mut bytes_keys = false;
    for key in parent_map.keys() {
        if key.is_instance_of::<PyBytes>() {
            bytes_keys = true;
            break;
        }
    }
    let gc = py.import("bzrformats._bzr_rs.groupcompress")?;
    if bytes_keys {
        let wrapped = PyDict::new(py);
        for (k, v) in parent_map.iter() {
            let k_tup = PyTuple::new(py, [k])?;
            // Values must be a *tuple* of single-element key tuples, matching
            // the Python `tuple((p,) for p in v)`.
            let mut v_items: Vec<Bound<PyAny>> = Vec::new();
            for p in v.try_iter()? {
                v_items.push(PyTuple::new(py, [p?])?.into_any());
            }
            wrapped.set_item(k_tup, PyTuple::new(py, v_items)?)?;
        }
        let sorted = gc.call_method1("sort_gc_optimal", (wrapped,))?;
        let out = PyList::empty(py);
        for k in sorted.try_iter()? {
            out.append(k?.get_item(0)?)?;
        }
        Ok(out)
    } else {
        let sorted = gc.call_method1("sort_gc_optimal", (parent_map,))?;
        sorted.cast_into::<PyList>().map_err(Into::into)
    }
}

/// Decorator for a `VersionedFiles` that skips `add_lines` when the key is
/// already present. Mirrors
/// `bzrformats.versionedfile.NoDupeAddLinesDecorator`.
#[pyclass(
    name = "NoDupeAddLinesDecorator",
    module = "bzrformats._bzr_rs.versionedfile"
)]
struct NoDupeAddLinesDecorator {
    store: Py<PyAny>,
}

#[pymethods]
impl NoDupeAddLinesDecorator {
    #[new]
    fn new(store: Py<PyAny>) -> Self {
        Self { store }
    }

    #[pyo3(signature = (key, parents, lines, parent_texts=None, left_matching_blocks=None, nostore_sha=None, random_id=false, check_content=true))]
    #[allow(clippy::too_many_arguments)]
    fn add_lines<'py>(
        &self,
        py: Python<'py>,
        key: Bound<'py, PyAny>,
        parents: Bound<'py, PyAny>,
        lines: Bound<'py, PyAny>,
        parent_texts: Option<Bound<'py, PyAny>>,
        left_matching_blocks: Option<Bound<'py, PyAny>>,
        nostore_sha: Option<Bound<'py, PyAny>>,
        random_id: bool,
        check_content: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let store = self.store.bind(py);
        if let Some(ns) = &nostore_sha {
            if ns.is_truthy()? {
                return Err(pyo3::exceptions::PyNotImplementedError::new_err(
                    "NoDupeAddLinesDecorator.add_lines does not implement the nostore_sha behaviour.",
                ));
            }
        }
        let osutils = py.import("bzrformats.osutils")?;
        // key[-1] is None?
        let last = key.get_item(-1)?;
        let (key, sha1): (Bound<PyAny>, Option<Bound<PyAny>>) = if last.is_none() {
            let s = osutils.call_method1("sha_strings", (lines.clone(),))?;
            let new_key = PyTuple::new(
                py,
                [PyBytes::new(py, b"sha1:").call_method1("__add__", (&s,))?],
            )?;
            (new_key.into_any(), Some(s))
        } else {
            (key, None)
        };
        // if key in store.get_parent_map([key]):
        let pm = store.call_method1("get_parent_map", (PyList::new(py, [&key])?,))?;
        if pm.contains(&key)? {
            let sha1 = match sha1 {
                Some(s) => s,
                None => osutils.call_method1("sha_strings", (lines.clone(),))?,
            };
            let mut total = 0usize;
            for l in lines.try_iter()? {
                total += l?.len()?;
            }
            return PyTuple::new(
                py,
                [
                    sha1.into_any(),
                    total.into_pyobject(py)?.into_any(),
                    py.None().into_bound(py),
                ],
            )
            .map(|t| t.into_any());
        }
        let none = py.None().into_bound(py);
        let kwargs = PyDict::new(py);
        kwargs.set_item("parent_texts", parent_texts.unwrap_or_else(|| none.clone()))?;
        kwargs.set_item(
            "left_matching_blocks",
            left_matching_blocks.unwrap_or_else(|| none.clone()),
        )?;
        kwargs.set_item("nostore_sha", nostore_sha.unwrap_or_else(|| none.clone()))?;
        kwargs.set_item("random_id", random_id)?;
        kwargs.set_item("check_content", check_content)?;
        store.call_method("add_lines", (key, parents, lines), Some(&kwargs))
    }

    fn __getattr__<'py>(
        &self,
        py: Python<'py>,
        name: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.store
            .bind(py)
            .getattr(name.cast::<pyo3::types::PyString>()?)
    }
}

/// A record_stream which reconstitutes a serialised stream. Mirrors
/// `bzrformats.versionedfile.NetworkRecordStream`.
#[pyclass(
    name = "NetworkRecordStream",
    module = "bzrformats._bzr_rs.versionedfile"
)]
struct NetworkRecordStream {
    bytes_iterator: Py<PyAny>,
}

#[pymethods]
impl NetworkRecordStream {
    #[new]
    fn new(bytes_iterator: Py<PyAny>) -> Self {
        Self { bytes_iterator }
    }

    /// Read the stream, yielding records as per
    /// `VersionedFiles.get_record_stream`. The per-kind factory dispatch
    /// matches the Python `_kind_factory` table.
    fn read<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let vf = py.import("bzrformats.versionedfile")?;
        let groupcompress = py.import("bzrformats.groupcompress")?;
        let knit = py.import("bzrformats.knit")?;
        let kind_factory = PyDict::new(py);
        kind_factory.set_item("fulltext", vf.getattr("fulltext_network_to_record")?)?;
        kind_factory.set_item(
            "groupcompress-block",
            groupcompress.getattr("network_block_to_records")?,
        )?;
        let knit_net = knit.getattr("knit_network_to_record")?;
        for k in [
            "knit-ft-gz",
            "knit-delta-gz",
            "knit-annotated-ft-gz",
            "knit-annotated-delta-gz",
        ] {
            kind_factory.set_item(k, &knit_net)?;
        }
        kind_factory.set_item(
            "knit-delta-closure",
            knit.getattr("knit_delta_closure_to_records")?,
        )?;

        let kind_offset = vf.getattr("network_bytes_to_kind_and_offset")?;
        let out = PyList::empty(py);
        for bytes in self.bytes_iterator.bind(py).try_iter()? {
            let bytes = bytes?;
            let pair = kind_offset.call1((bytes.clone(),))?;
            let storage_kind = pair.get_item(0)?;
            let line_end = pair.get_item(1)?;
            let factory = kind_factory.get_item(&storage_kind)?.ok_or_else(|| {
                pyo3::exceptions::PyKeyError::new_err(storage_kind.clone().unbind())
            })?;
            let records = factory.call1((storage_kind, bytes, line_end))?;
            for record in records.try_iter()? {
                out.append(record?)?;
            }
        }
        out.call_method0("__iter__")
    }
}

/// Helper: `version_id is not None and revision.check_not_reserved_id(...)`.
fn check_not_reserved_id_impl(py: Python<'_>, version_id: &Bound<'_, PyAny>) -> PyResult<()> {
    if !version_id.is_none() {
        py.import("bzrformats.revision")?
            .getattr("check_not_reserved_id")?
            .call1((version_id,))?;
    }
    Ok(())
}

/// Abstract base for a single versioned text file. Mirrors
/// `bzrformats.versionedfile.VersionedFile`. The `Weave` pyclass extends
/// this; breezy subclasses it in Python. Abstract methods raise
/// `NotImplementedError`; concrete helpers are provided.
#[pyclass(
    subclass,
    name = "VersionedFile",
    module = "bzrformats._bzr_rs.versionedfile"
)]
pub struct PyVersionedFileBase;

/// Build the base initializer for a `VersionedFile` subclass implemented in
/// another module (weave).
pub fn versionedfile_initializer() -> PyClassInitializer<PyVersionedFileBase> {
    PyClassInitializer::from(PyVersionedFileBase)
}

#[pymethods]
impl PyVersionedFileBase {
    #[new]
    #[pyo3(signature = (*_args, **_kwargs))]
    fn new(_args: Bound<'_, PyTuple>, _kwargs: Option<Bound<'_, PyDict>>) -> Self {
        PyVersionedFileBase
    }

    #[staticmethod]
    fn check_not_reserved_id(py: Python<'_>, version_id: Bound<'_, PyAny>) -> PyResult<()> {
        check_not_reserved_id_impl(py, &version_id)
    }

    fn copy_to(
        slf: &Bound<'_, Self>,
        name: Bound<'_, PyAny>,
        transport: Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let _ = (name, transport);
        Err(not_implemented(slf, "copy_to"))
    }

    fn get_record_stream(
        slf: &Bound<'_, Self>,
        versions: Bound<'_, PyAny>,
        ordering: Bound<'_, PyAny>,
        include_delta_closure: Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let _ = (versions, ordering, include_delta_closure);
        Err(not_implemented(slf, "get_record_stream"))
    }

    fn has_version(slf: &Bound<'_, Self>, version_id: Bound<'_, PyAny>) -> PyResult<()> {
        let _ = version_id;
        Err(not_implemented(slf, "has_version"))
    }

    fn insert_record_stream(slf: &Bound<'_, Self>, stream: Bound<'_, PyAny>) -> PyResult<()> {
        let _ = stream;
        Err(pyo3::exceptions::PyNotImplementedError::new_err(()))
    }

    #[pyo3(signature = (version_id, parents, lines, parent_texts=None, left_matching_blocks=None, nostore_sha=None, random_id=false, check_content=true))]
    #[allow(clippy::too_many_arguments)]
    fn add_lines<'py>(
        slf: &Bound<'py, Self>,
        version_id: Bound<'py, PyAny>,
        parents: Bound<'py, PyAny>,
        lines: Bound<'py, PyAny>,
        parent_texts: Option<Bound<'py, PyAny>>,
        left_matching_blocks: Option<Bound<'py, PyAny>>,
        nostore_sha: Option<Bound<'py, PyAny>>,
        random_id: bool,
        check_content: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        slf.call_method0("_check_write_ok")?;
        let none = py.None().into_bound(py);
        slf.call_method1(
            "_add_lines",
            (
                version_id,
                parents,
                lines,
                parent_texts.unwrap_or_else(|| none.clone()),
                left_matching_blocks.unwrap_or_else(|| none.clone()),
                nostore_sha.unwrap_or_else(|| none.clone()),
                random_id,
                check_content,
            ),
        )
    }

    #[pyo3(signature = (version_id, parents, lines, parent_texts=None, left_matching_blocks=None, nostore_sha=None, random_id=false, check_content=true))]
    #[allow(clippy::too_many_arguments, unused_variables)]
    fn _add_lines(
        slf: &Bound<'_, Self>,
        version_id: Bound<'_, PyAny>,
        parents: Bound<'_, PyAny>,
        lines: Bound<'_, PyAny>,
        parent_texts: Option<Bound<'_, PyAny>>,
        left_matching_blocks: Option<Bound<'_, PyAny>>,
        nostore_sha: Option<Bound<'_, PyAny>>,
        random_id: bool,
        check_content: bool,
    ) -> PyResult<()> {
        Err(not_implemented(slf, "add_lines"))
    }

    #[pyo3(signature = (version_id, parents, lines, parent_texts=None, nostore_sha=None, random_id=false, check_content=true, left_matching_blocks=None))]
    #[allow(clippy::too_many_arguments)]
    fn add_lines_with_ghosts<'py>(
        slf: &Bound<'py, Self>,
        version_id: Bound<'py, PyAny>,
        parents: Bound<'py, PyAny>,
        lines: Bound<'py, PyAny>,
        parent_texts: Option<Bound<'py, PyAny>>,
        nostore_sha: Option<Bound<'py, PyAny>>,
        random_id: bool,
        check_content: bool,
        left_matching_blocks: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        slf.call_method0("_check_write_ok")?;
        let none = py.None().into_bound(py);
        slf.call_method1(
            "_add_lines_with_ghosts",
            (
                version_id,
                parents,
                lines,
                parent_texts.unwrap_or_else(|| none.clone()),
                nostore_sha.unwrap_or_else(|| none.clone()),
                random_id,
                check_content,
                left_matching_blocks.unwrap_or_else(|| none.clone()),
            ),
        )
    }

    #[pyo3(signature = (version_id, parents, lines, parent_texts=None, nostore_sha=None, random_id=false, check_content=true, left_matching_blocks=None))]
    #[allow(clippy::too_many_arguments, unused_variables)]
    fn _add_lines_with_ghosts(
        slf: &Bound<'_, Self>,
        version_id: Bound<'_, PyAny>,
        parents: Bound<'_, PyAny>,
        lines: Bound<'_, PyAny>,
        parent_texts: Option<Bound<'_, PyAny>>,
        nostore_sha: Option<Bound<'_, PyAny>>,
        random_id: bool,
        check_content: bool,
        left_matching_blocks: Option<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        Err(not_implemented(slf, "add_lines_with_ghosts"))
    }

    #[pyo3(signature = (progress_bar=None))]
    fn check(slf: &Bound<'_, Self>, progress_bar: Option<Bound<'_, PyAny>>) -> PyResult<()> {
        let _ = progress_bar;
        Err(not_implemented(slf, "check"))
    }

    fn _check_lines_not_unicode(&self, py: Python<'_>, lines: Bound<'_, PyAny>) -> PyResult<()> {
        py.import("bzrformats._bzr_rs.versionedfile")?
            .getattr("check_lines_not_unicode")?
            .call1((lines,))?;
        Ok(())
    }

    fn _check_lines_are_lines(&self, py: Python<'_>, lines: Bound<'_, PyAny>) -> PyResult<()> {
        py.import("bzrformats._bzr_rs.versionedfile")?
            .getattr("check_lines_are_lines")?
            .call1((lines,))?;
        Ok(())
    }

    fn get_format_signature(slf: &Bound<'_, Self>) -> PyResult<()> {
        Err(not_implemented(slf, "get_format_signature"))
    }

    /// make_mpdiffs(version_ids) — singular VersionedFile variant.
    fn make_mpdiffs<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        version_ids: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        let ids = PyList::new(py, version_ids.try_iter()?.collect::<PyResult<Vec<_>>>()?)?;
        let res = py
            .import("bzrformats._bzr_rs.versionedfile")?
            .getattr("make_mpdiffs_singular")?
            .call1((slf, ids))?;
        PyList::new(py, res.try_iter()?.collect::<PyResult<Vec<_>>>()?)
    }

    fn add_mpdiffs(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        records: Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let recs = PyList::new(py, records.try_iter()?.collect::<PyResult<Vec<_>>>()?)?;
        py.import("bzrformats._bzr_rs.versionedfile")?
            .getattr("add_mpdiffs_singular")?
            .call1((slf, recs))?;
        Ok(())
    }

    /// get_text = b"".join(get_lines(version_id)).
    fn get_text<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        version_id: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let lines = slf.call_method1("get_lines", (version_id,))?;
        join_bytes(py, &lines)
    }

    fn get_string<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        version_id: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        Self::get_text(slf, py, version_id)
    }

    fn get_texts<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        version_ids: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        let out = PyList::empty(py);
        for v in version_ids.try_iter()? {
            let lines = slf.call_method1("get_lines", (v?,))?;
            out.append(join_bytes(py, &lines)?)?;
        }
        Ok(out)
    }

    fn get_lines(slf: &Bound<'_, Self>, version_id: Bound<'_, PyAny>) -> PyResult<()> {
        let _ = version_id;
        Err(not_implemented(slf, "get_lines"))
    }

    /// [BytesIO(t).readlines() for t in get_texts(version_ids)]
    fn _get_lf_split_line_list<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        version_ids: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        let texts = slf.call_method1("get_texts", (version_ids,))?;
        let bio_cls = py.import("io")?.getattr("BytesIO")?;
        let out = PyList::empty(py);
        for t in texts.try_iter()? {
            out.append(bio_cls.call1((t?,))?.call_method0("readlines")?)?;
        }
        Ok(out)
    }

    fn get_ancestry(slf: &Bound<'_, Self>, version_ids: Bound<'_, PyAny>) -> PyResult<()> {
        let _ = version_ids;
        Err(not_implemented(slf, "get_ancestry"))
    }

    fn get_ancestry_with_ghosts(
        slf: &Bound<'_, Self>,
        version_ids: Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let _ = version_ids;
        Err(not_implemented(slf, "get_ancestry_with_ghosts"))
    }

    fn get_parent_map(slf: &Bound<'_, Self>, version_ids: Bound<'_, PyAny>) -> PyResult<()> {
        let _ = version_ids;
        Err(not_implemented(slf, "get_parent_map"))
    }

    fn get_parents_with_ghosts<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        version_id: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let pm = slf.call_method1("get_parent_map", (PyList::new(py, [&version_id])?,))?;
        match pm.get_item(&version_id) {
            Ok(parents) => Ok(pyo3::types::PyList::new(
                py,
                parents.try_iter()?.collect::<PyResult<Vec<_>>>()?,
            )?
            .into_any()),
            Err(_) => Err(PyErr::from_value(
                py.import("bzrformats.errors")?
                    .getattr("RevisionNotPresent")?
                    .call1((version_id, slf))?,
            )),
        }
    }

    fn annotate(slf: &Bound<'_, Self>, version_id: Bound<'_, PyAny>) -> PyResult<()> {
        let _ = version_id;
        Err(not_implemented(slf, "annotate"))
    }

    #[pyo3(signature = (version_ids=None, pb=None))]
    fn iter_lines_added_or_present_in_versions(
        slf: &Bound<'_, Self>,
        version_ids: Option<Bound<'_, PyAny>>,
        pb: Option<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let _ = (version_ids, pb);
        Err(not_implemented(
            slf,
            "iter_lines_added_or_present_in_versions",
        ))
    }

    #[pyo3(signature = (ver_a, ver_b, base=None))]
    fn plan_merge(
        slf: &Bound<'_, Self>,
        ver_a: Bound<'_, PyAny>,
        ver_b: Bound<'_, PyAny>,
        base: Option<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let _ = (ver_a, ver_b, base);
        // Mirrors `raise NotImplementedError(VersionedFile.plan_merge)` (the
        // unbound class method).
        let cls = slf.py().get_type::<PyVersionedFileBase>();
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            cls.getattr("plan_merge")?.unbind(),
        ))
    }

    #[pyo3(signature = (plan, a_marker=None, b_marker=None))]
    fn weave_merge<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        plan: Bound<'py, PyAny>,
        a_marker: Option<Bound<'py, PyAny>>,
        b_marker: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let _ = slf;
        let pwm_cls = py
            .import("bzrformats.versionedfile")?
            .getattr("PlanWeaveMerge")?;
        let a =
            a_marker.unwrap_or_else(|| PyBytes::new(py, bazaar::textmerge::A_MARKER).into_any());
        let b =
            b_marker.unwrap_or_else(|| PyBytes::new(py, bazaar::textmerge::B_MARKER).into_any());
        let pwm = pwm_cls.call1((plan, a, b))?;
        let res = pwm.call_method0("merge_lines")?;
        res.get_item(0)
    }
}

/// `b"".join(iterable_of_bytes)` for a Python iterable of bytes chunks.
fn join_bytes<'py>(py: Python<'py>, chunks: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyBytes>> {
    let mut out: Vec<u8> = Vec::new();
    for c in chunks.try_iter()? {
        out.extend_from_slice(c?.cast_into::<PyBytes>()?.as_bytes());
    }
    Ok(PyBytes::new(py, &out))
}

/// Abstract base for storage of many versioned files. Mirrors
/// `bzrformats.versionedfile.VersionedFiles`. Subclassable from Python; the
/// concrete backends (knit, groupcompress, weave) and breezy repos subclass
/// it. Most methods raise `NotImplementedError`; the concrete helpers
/// delegate to the Rust core.
#[pyclass(
    subclass,
    name = "VersionedFiles",
    module = "bzrformats._bzr_rs.versionedfile"
)]
pub struct PyVersionedFilesBase;

#[pymethods]
impl PyVersionedFilesBase {
    #[new]
    #[pyo3(signature = (*_args, **_kwargs))]
    fn new(_args: Bound<'_, PyTuple>, _kwargs: Option<Bound<'_, PyDict>>) -> Self {
        PyVersionedFilesBase
    }

    #[pyo3(signature = (key, parents, lines, parent_texts=None, left_matching_blocks=None, nostore_sha=None, random_id=false, check_content=true))]
    #[allow(clippy::too_many_arguments, unused_variables)]
    fn add_lines(
        slf: &Bound<'_, Self>,
        key: Bound<'_, PyAny>,
        parents: Bound<'_, PyAny>,
        lines: Bound<'_, PyAny>,
        parent_texts: Option<Bound<'_, PyAny>>,
        left_matching_blocks: Option<Bound<'_, PyAny>>,
        nostore_sha: Option<Bound<'_, PyAny>>,
        random_id: bool,
        check_content: bool,
    ) -> PyResult<()> {
        Err(not_implemented(slf, "add_lines"))
    }

    #[pyo3(signature = (factory, parent_texts=None, left_matching_blocks=None, nostore_sha=None, random_id=false, check_content=true))]
    #[allow(clippy::too_many_arguments, unused_variables)]
    fn add_content(
        slf: &Bound<'_, Self>,
        factory: Bound<'_, PyAny>,
        parent_texts: Option<Bound<'_, PyAny>>,
        left_matching_blocks: Option<Bound<'_, PyAny>>,
        nostore_sha: Option<Bound<'_, PyAny>>,
        random_id: bool,
        check_content: bool,
    ) -> PyResult<()> {
        Err(not_implemented(slf, "add_content"))
    }

    /// Add mpdiffs. Drives the Rust build/fetch/reconstruct/add_lines loop,
    /// calling back into `self.get_record_stream` and `self.add_lines`.
    fn add_mpdiffs(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        records: Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let vf = py.import("bzrformats._bzr_rs.versionedfile")?;
        vf.getattr("add_mpdiffs")?.call1((slf, records))?;
        Ok(())
    }

    fn annotate(slf: &Bound<'_, Self>, key: Bound<'_, PyAny>) -> PyResult<()> {
        let _ = key;
        Err(not_implemented(slf, "annotate"))
    }

    #[pyo3(signature = (progress_bar=None))]
    fn check(slf: &Bound<'_, Self>, progress_bar: Option<Bound<'_, PyAny>>) -> PyResult<()> {
        let _ = progress_bar;
        Err(not_implemented(slf, "check"))
    }

    #[staticmethod]
    fn check_not_reserved_id(py: Python<'_>, version_id: Bound<'_, PyAny>) -> PyResult<()> {
        check_not_reserved_id_impl(py, &version_id)
    }

    /// Clear whatever caches this VersionedFiles holds. Default no-op.
    fn clear_cache(&self) {}

    fn _check_lines_not_unicode(&self, py: Python<'_>, lines: Bound<'_, PyAny>) -> PyResult<()> {
        py.import("bzrformats._bzr_rs.versionedfile")?
            .getattr("check_lines_not_unicode")?
            .call1((lines,))?;
        Ok(())
    }

    fn _check_lines_are_lines(&self, py: Python<'_>, lines: Bound<'_, PyAny>) -> PyResult<()> {
        py.import("bzrformats._bzr_rs.versionedfile")?
            .getattr("check_lines_are_lines")?
            .call1((lines,))?;
        Ok(())
    }

    /// Get a KnownGraph instance with the ancestry of keys.
    fn get_known_graph_ancestry<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        keys: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let keys_list = PyList::new(py, keys.try_iter()?.collect::<PyResult<Vec<_>>>()?)?;
        let parent_map = py
            .import("bzrformats._bzr_rs.versionedfile")?
            .getattr("known_graph_ancestry_map")?
            .call1((slf, keys_list))?;
        py.import("vcsgraph.known_graph")?
            .getattr("KnownGraph")?
            .call1((parent_map,))
    }

    fn get_parent_map(slf: &Bound<'_, Self>, keys: Bound<'_, PyAny>) -> PyResult<()> {
        let _ = keys;
        Err(not_implemented(slf, "get_parent_map"))
    }

    fn get_record_stream(
        slf: &Bound<'_, Self>,
        keys: Bound<'_, PyAny>,
        ordering: Bound<'_, PyAny>,
        include_delta_closure: Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let _ = (keys, ordering, include_delta_closure);
        Err(not_implemented(slf, "get_record_stream"))
    }

    fn get_sha1s(slf: &Bound<'_, Self>, keys: Bound<'_, PyAny>) -> PyResult<()> {
        let _ = keys;
        Err(not_implemented(slf, "get_sha1s"))
    }

    /// `key in self` — mirrors `index._has_key_from_parent_map`.
    fn __contains__(slf: &Bound<'_, Self>, key: Bound<'_, PyAny>) -> PyResult<bool> {
        let pm = slf.call_method1("get_parent_map", (PyList::new(slf.py(), [&key])?,))?;
        pm.contains(key)
    }

    fn get_missing_compression_parent_keys(slf: &Bound<'_, Self>) -> PyResult<()> {
        Err(not_implemented(slf, "get_missing_compression_parent_keys"))
    }

    fn insert_record_stream(slf: &Bound<'_, Self>, stream: Bound<'_, PyAny>) -> PyResult<()> {
        let _ = stream;
        Err(pyo3::exceptions::PyNotImplementedError::new_err(()))
    }

    #[pyo3(signature = (keys, pb=None))]
    fn iter_lines_added_or_present_in_keys(
        slf: &Bound<'_, Self>,
        keys: Bound<'_, PyAny>,
        pb: Option<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let _ = (keys, pb);
        Err(not_implemented(slf, "iter_lines_added_or_present_in_keys"))
    }

    fn keys(slf: &Bound<'_, Self>) -> PyResult<()> {
        Err(not_implemented(slf, "keys"))
    }

    /// Create multiparent diffs for specified keys.
    fn make_mpdiffs<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        keys: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let gen = py
            .import("bzrformats.versionedfile")?
            .getattr("_MPDiffGenerator")?
            .call1((slf, keys))?;
        gen.call_method0("compute_diffs")
    }

    /// `missing_keys` — keys absent from get_parent_map. Mirrors
    /// `index._missing_keys_from_parent_map`.
    fn missing_keys<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        keys: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PySet>> {
        let keys_list = PyList::new(py, keys.try_iter()?.collect::<PyResult<Vec<_>>>()?)?;
        let pm = slf.call_method1("get_parent_map", (&keys_list,))?;
        let out = PySet::empty(py)?;
        for k in keys_list.iter() {
            if !pm.contains(&k)? {
                out.add(k)?;
            }
        }
        Ok(out)
    }

    /// Build a VersionedFileAnnotator over this versioned file.
    fn get_annotator<'py>(slf: &Bound<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        py.import("bzrformats.annotate")?
            .getattr("VersionedFileAnnotator")?
            .call1((slf,))
    }

    /// Return the whole stack of fallback versionedfiles.
    fn _transitive_fallbacks<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyList>> {
        let out = PyList::empty(py);
        let fallbacks = slf.getattr("_immediate_fallback_vfs")?;
        for a_vfs in fallbacks.try_iter()? {
            let a_vfs = a_vfs?;
            out.append(&a_vfs)?;
            let sub = a_vfs.call_method0("_transitive_fallbacks")?;
            for f in sub.try_iter()? {
                out.append(f?)?;
            }
        }
        Ok(out)
    }
}

/// `NotImplementedError(self.<method>)` — match the Python ABC, whose
/// `raise NotImplementedError(self.method)` carries the bound method.
fn not_implemented(slf: &Bound<'_, impl pyo3::PyClass>, method: &str) -> PyErr {
    match slf.as_any().getattr(method) {
        Ok(m) => pyo3::exceptions::PyNotImplementedError::new_err(m.unbind()),
        Err(e) => e,
    }
}

/// `NotImplementedError(self.<method>)` for a plain `Bound<PyAny>` receiver.
fn not_implemented_any(slf: &Bound<'_, PyAny>, method: &str) -> PyErr {
    match slf.getattr(method) {
        Ok(m) => pyo3::exceptions::PyNotImplementedError::new_err(m.unbind()),
        Err(e) => e,
    }
}

/// A `VersionedFiles` that supports fallback sources. Mirrors
/// `bzrformats.versionedfile.VersionedFilesWithFallbacks`. Extends the
/// `VersionedFiles` ABC; the knit and groupcompress backends extend this.
#[pyclass(
    extends = PyVersionedFilesBase,
    subclass,
    name = "VersionedFilesWithFallbacks",
    module = "bzrformats._bzr_rs.versionedfile"
)]
pub struct PyVersionedFilesWithFallbacks;

/// Build the base initializer chain for a `VersionedFilesWithFallbacks`
/// subclass implemented in another module (knit, groupcompress). Lets those
/// pyclasses do `vfwf_initializer().add_subclass(Self { .. })`.
pub fn vfwf_initializer() -> PyClassInitializer<PyVersionedFilesWithFallbacks> {
    PyClassInitializer::from(PyVersionedFilesBase).add_subclass(PyVersionedFilesWithFallbacks)
}

/// Build the base initializer for a plain `VersionedFiles` subclass.
pub fn vf_initializer() -> PyClassInitializer<PyVersionedFilesBase> {
    PyClassInitializer::from(PyVersionedFilesBase)
}

#[pymethods]
impl PyVersionedFilesWithFallbacks {
    #[new]
    #[pyo3(signature = (*_args, **_kwargs))]
    fn new(
        _args: Bound<'_, PyTuple>,
        _kwargs: Option<Bound<'_, PyDict>>,
    ) -> PyClassInitializer<Self> {
        PyClassInitializer::from(PyVersionedFilesBase).add_subclass(PyVersionedFilesWithFallbacks)
    }

    fn without_fallbacks(slf: &Bound<'_, Self>) -> PyResult<()> {
        Err(not_implemented_any(slf.as_any(), "without_fallbacks"))
    }

    fn add_fallback_versioned_files(
        slf: &Bound<'_, Self>,
        a_versioned_files: Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let _ = a_versioned_files;
        Err(not_implemented_any(
            slf.as_any(),
            "add_fallback_versioned_files",
        ))
    }

    /// Get a KnownGraph with the ancestry of keys, walking fallbacks via
    /// each store's `_index.find_ancestry`.
    fn get_known_graph_ancestry<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        keys: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let slf = slf.as_any();
        let index = slf.getattr("_index")?;
        let res = index.call_method1("find_ancestry", (keys,))?;
        let parent_map = res.get_item(0)?;
        let mut missing_keys = res.get_item(1)?;
        let fallbacks = slf.call_method0("_transitive_fallbacks")?;
        for fallback in fallbacks.try_iter()? {
            if !missing_keys.is_truthy()? {
                break;
            }
            let fallback = fallback?;
            let fres = fallback
                .getattr("_index")?
                .call_method1("find_ancestry", (&missing_keys,))?;
            let f_parent_map = fres.get_item(0)?;
            parent_map.call_method1("update", (f_parent_map,))?;
            missing_keys = fres.get_item(1)?;
        }
        py.import("vcsgraph.known_graph")?
            .getattr("KnownGraph")?
            .call1((parent_map,))
    }
}

/// A `VersionedFiles` for uncommitted and committed texts, used to plan merges
/// against working-tree texts. Ported from
/// `bzrformats.versionedfile._PlanMergeVersionedFile`.
///
/// Holds local `(key -> parents)` / `(key -> lines)` maps plus a list of
/// fallback `VersionedFiles`, and drives the Rust `_PlanMerge` / `_PlanLCAMerge`
/// (via `bzrformats.merge`). Instance state lives in `__dict__`.
#[pyclass(
    name = "_PlanMergeVersionedFile",
    extends = PyVersionedFilesBase,
    dict,
    module = "bzrformats._bzr_rs.versionedfile"
)]
pub struct PyPlanMergeVersionedFile;

#[pymethods]
impl PyPlanMergeVersionedFile {
    #[new]
    fn new(file_id: Py<PyAny>) -> PyClassInitializer<Self> {
        let _ = file_id;
        vf_initializer().add_subclass(PyPlanMergeVersionedFile)
    }

    fn __init__(slf: &Bound<'_, Self>, file_id: Bound<'_, PyAny>) -> PyResult<()> {
        let py = slf.py();
        slf.setattr("_file_id", file_id)?;
        slf.setattr("fallback_versionedfiles", PyList::empty(py))?;
        let parents = PyDict::new(py);
        slf.setattr("_parents", &parents)?;
        slf.setattr("_lines", PyDict::new(py))?;
        // _providers = [DictParentsProvider(self._parents)]
        let provider = py
            .import("vcsgraph.graph")?
            .getattr("DictParentsProvider")?
            .call1((&parents,))?;
        slf.setattr("_providers", PyList::new(py, [provider])?)?;
        Ok(())
    }

    #[pyo3(signature = (ver_a, ver_b, base=None))]
    fn plan_merge<'py>(
        slf: &Bound<'py, Self>,
        ver_a: Bound<'py, PyAny>,
        ver_b: Bound<'py, PyAny>,
        base: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        let plan_merge_cls = py.import("bzrformats.merge")?.getattr("_PlanMerge")?;
        let file_id = slf.getattr("_file_id")?;
        let prefix = PyTuple::new(py, [&file_id])?;
        match base {
            None => {
                let pm = plan_merge_cls.call1((&ver_a, &ver_b, slf, &prefix))?;
                pm.call_method0("plan_merge")
            }
            Some(base) => {
                let old = plan_merge_cls
                    .call1((&ver_a, &base, slf, &prefix))?
                    .call_method0("plan_merge")?;
                let old = py.import("builtins")?.getattr("list")?.call1((old,))?;
                let new = plan_merge_cls
                    .call1((&ver_a, &ver_b, slf, &prefix))?
                    .call_method0("plan_merge")?;
                let new = py.import("builtins")?.getattr("list")?.call1((new,))?;
                plan_merge_cls.getattr("_subtract_plans")?.call1((old, new))
            }
        }
    }

    #[pyo3(signature = (ver_a, ver_b, base=None))]
    fn plan_lca_merge<'py>(
        slf: &Bound<'py, Self>,
        ver_a: Bound<'py, PyAny>,
        ver_b: Bound<'py, PyAny>,
        base: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        let merge = py.import("bzrformats.merge")?;
        let lca_cls = merge.getattr("_PlanLCAMerge")?;
        let graph = py
            .import("vcsgraph.graph")?
            .getattr("Graph")?
            .call1((slf,))?;
        let file_id = slf.getattr("_file_id")?;
        let prefix = PyTuple::new(py, [&file_id])?;
        let list = py.import("builtins")?.getattr("list")?;
        let new = lca_cls
            .call1((&ver_a, &ver_b, slf, &prefix, &graph))?
            .call_method0("plan_merge")?;
        match base {
            None => Ok(new),
            Some(base) => {
                let old = lca_cls
                    .call1((&ver_a, &base, slf, &prefix, &graph))?
                    .call_method0("plan_merge")?;
                let old = list.call1((old,))?;
                let new = list.call1((new,))?;
                lca_cls.getattr("_subtract_plans")?.call1((old, new))
            }
        }
    }

    fn add_content<'py>(
        slf: &Bound<'py, Self>,
        factory: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let key = factory.getattr("key")?;
        let parents = factory.getattr("parents")?;
        let lines = factory.call_method1("get_bytes_as", ("lines",))?;
        Self::add_lines(slf, key, parents, lines)
    }

    fn add_lines<'py>(
        slf: &Bound<'py, Self>,
        key: Bound<'py, PyAny>,
        parents: Bound<'py, PyAny>,
        lines: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        if !key.is_instance_of::<PyTuple>() {
            return Err(PyTypeError::new_err(key.unbind()));
        }
        // Only reserved ids may be used.
        let last = key.get_item(key.len()? - 1)?;
        let is_reserved = py
            .import("bzrformats.revision")?
            .getattr("is_reserved_id")?
            .call1((&last,))?
            .is_truthy()?;
        if !is_reserved {
            return Err(PyValueError::new_err("Only reserved ids may be used"));
        }
        if parents.is_none() {
            return Err(PyValueError::new_err("Parents may not be None"));
        }
        if lines.is_none() {
            return Err(PyValueError::new_err("Lines may not be None"));
        }
        let parents_tuple = py
            .import("builtins")?
            .getattr("tuple")?
            .call1((&parents,))?;
        slf.getattr("_parents")?
            .downcast::<PyDict>()?
            .set_item(&key, parents_tuple)?;
        slf.getattr("_lines")?
            .downcast::<PyDict>()?
            .set_item(&key, lines)?;
        Ok(py.None().into_bound(py))
    }

    fn get_record_stream<'py>(
        slf: &Bound<'py, Self>,
        keys: Bound<'py, PyAny>,
        ordering: Bound<'py, PyAny>,
        include_delta_closure: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let _ = (ordering, include_delta_closure);
        let py = slf.py();
        let out = PyList::empty(py);
        let lines_map = slf.getattr("_lines")?;
        let lines_map = lines_map.downcast::<PyDict>()?;
        let parents_map = slf.getattr("_parents")?;
        let parents_map = parents_map.downcast::<PyDict>()?;
        // pending = set(keys); locally-held keys yield ChunkedContentFactory.
        let pending = PySet::empty(py)?;
        for k in keys.try_iter()? {
            pending.add(k?)?;
        }
        let keys_list: Vec<Bound<PyAny>> = pending.iter().collect();
        for key in keys_list {
            if let Some(lines) = lines_map.get_item(&key)? {
                let parents = parents_map
                    .get_item(&key)?
                    .ok_or_else(|| PyKeyError::new_err(key.clone().unbind()))?;
                pending.discard(&key)?;
                let cf = py.get_type::<ChunkedContentFactory>().call1((
                    &key,
                    parents,
                    py.None(),
                    lines,
                ))?;
                out.append(cf)?;
            }
        }
        // Then consult fallback versionedfiles.
        let fallbacks = slf.getattr("fallback_versionedfiles")?;
        for vf in fallbacks.try_iter()? {
            let vf = vf?;
            let stream = vf.call_method1("get_record_stream", (&pending, "unordered", true))?;
            for record in stream.try_iter()? {
                let record = record?;
                let kind: String = record.getattr("storage_kind")?.extract()?;
                if kind == "absent" {
                    continue;
                }
                pending.discard(record.getattr("key")?)?;
                out.append(record)?;
            }
            if pending.is_empty() {
                return out.into_any().try_iter().map(|i| i.into_any());
            }
        }
        // Report absent entries.
        for key in pending.iter() {
            let cf = py.get_type::<AbsentContentFactory>().call1((key,))?;
            out.append(cf)?;
        }
        out.into_any().try_iter().map(|i| i.into_any())
    }

    fn get_parent_map<'py>(
        slf: &Bound<'py, Self>,
        keys: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let py = slf.py();
        let revision = py.import("bzrformats.revision")?;
        let null_rev = revision.getattr("NULL_REVISION")?;
        let key_set = PySet::empty(py)?;
        for k in keys.try_iter()? {
            key_set.add(k?)?;
        }
        let result = PyDict::new(py);
        if key_set.contains(&null_rev)? {
            key_set.discard(&null_rev)?;
            result.set_item(&null_rev, PyTuple::empty(py))?;
        }
        // _providers = self._providers[:1] + fallback_versionedfiles
        let providers = slf.getattr("_providers")?;
        let first = providers.get_item(0)?;
        let combined = PyList::new(py, [first])?;
        for vf in slf.getattr("fallback_versionedfiles")?.try_iter()? {
            combined.append(vf?)?;
        }
        slf.setattr("_providers", &combined)?;
        let stacked = py
            .import("vcsgraph.graph")?
            .getattr("StackedParentsProvider")?
            .call1((&combined,))?;
        let looked_up = stacked.call_method1("get_parent_map", (&key_set,))?;
        result.call_method1("update", (looked_up,))?;
        // Replace empty parents with (NULL_REVISION,).
        let empty = PyTuple::empty(py);
        let items: Vec<(Bound<PyAny>, Bound<PyAny>)> = result
            .items()
            .iter()
            .map(|it| {
                let t = it.downcast::<PyTuple>().unwrap();
                (t.get_item(0).unwrap(), t.get_item(1).unwrap())
            })
            .collect();
        for (key, parents) in items {
            if parents.eq(&empty)? {
                result.set_item(&key, PyTuple::new(py, [&null_rev])?)?;
            }
        }
        Ok(result)
    }
}

pub(crate) fn _versionedfile_rs(py: Python) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "versionedfile")?;
    m.add_class::<PyPlanMergeVersionedFile>()?;
    m.add_class::<AbstractContentFactory>()?;
    m.add_class::<FulltextContentFactory>()?;
    m.add_class::<ChunkedContentFactory>()?;
    m.add_class::<PyFileContentFactory>()?;
    m.add_class::<PyAdapterFactory>()?;
    m.add_class::<AbsentContentFactory>()?;
    m.add_class::<KeyRefs>()?;
    m.add_class::<PyConstantMapper>()?;
    m.add_class::<PyPrefixMapper>()?;
    m.add_class::<PyHashPrefixMapper>()?;
    m.add_class::<PyHashEscapedPrefixMapper>()?;
    m.add_class::<PyVirtualVersionedFiles>()?;
    m.add_function(wrap_pyfunction!(record_to_fulltext_bytes, &m)?)?;
    m.add_function(wrap_pyfunction!(fulltext_network_to_record, &m)?)?;
    m.add_function(wrap_pyfunction!(network_bytes_to_kind_and_offset, &m)?)?;
    m.add_function(wrap_pyfunction!(check_lines_not_unicode, &m)?)?;
    m.add_function(wrap_pyfunction!(check_lines_are_lines, &m)?)?;
    m.add_function(wrap_pyfunction!(known_graph_ancestry_map, &m)?)?;
    m.add_function(wrap_pyfunction!(make_mpdiffs, &m)?)?;
    m.add_function(wrap_pyfunction!(mpdiff_first_pass, &m)?)?;
    m.add_function(wrap_pyfunction!(mpdiff_collect_parent_chunks, &m)?)?;
    m.add_function(wrap_pyfunction!(add_mpdiffs, &m)?)?;
    m.add_function(wrap_pyfunction!(add_mpdiffs_singular, &m)?)?;
    m.add_function(wrap_pyfunction!(make_mpdiffs_singular, &m)?)?;
    m.add_function(wrap_pyfunction!(sort_groupcompress, &m)?)?;
    m.add_class::<NoDupeAddLinesDecorator>()?;
    m.add_class::<NetworkRecordStream>()?;
    m.add_class::<PyVersionedFileBase>()?;
    m.add_class::<PyVersionedFilesBase>()?;
    m.add_class::<PyVersionedFilesWithFallbacks>()?;
    Ok(m)
}
