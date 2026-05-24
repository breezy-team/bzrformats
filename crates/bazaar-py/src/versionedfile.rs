use bazaar::versionedfile::{ContentFactory, Key};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PySet, PyTuple};

#[pyclass(subclass)]
struct AbstractContentFactory(Box<dyn ContentFactory + Send + Sync>);

pyo3::import_exception!(bzrformats.errors, UnavailableRepresentation);

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
struct ChunkedContentFactory;

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
struct AbsentContentFactory;

#[pymethods]
impl AbsentContentFactory {
    #[new]
    fn new(key: Key) -> PyResult<(Self, AbstractContentFactory)> {
        let of = bazaar::versionedfile::AbsentContentFactory::new(key);

        Ok((AbsentContentFactory, AbstractContentFactory(Box::new(of))))
    }
}

#[pyfunction]
fn prefix_map(prefix: &[u8]) -> String {
    bazaar::key_mapper::prefix_map(prefix)
}

#[pyfunction]
fn prefix_unmap<'py>(py: Python<'py>, partition_id: &str) -> Bound<'py, PyBytes> {
    PyBytes::new(py, &bazaar::key_mapper::prefix_unmap(partition_id))
}

#[pyfunction]
fn hash_prefix_map(prefix: &[u8]) -> String {
    bazaar::key_mapper::hash_prefix_map(prefix)
}

#[pyfunction]
fn hash_prefix_unmap<'py>(py: Python<'py>, partition_id: &str) -> Bound<'py, PyBytes> {
    PyBytes::new(py, &bazaar::key_mapper::hash_prefix_unmap(partition_id))
}

#[pyfunction]
fn hash_escaped_prefix_map(prefix: &[u8]) -> String {
    bazaar::key_mapper::hash_escaped_prefix_map(prefix)
}

#[pyfunction]
fn hash_escaped_prefix_unmap<'py>(py: Python<'py>, partition_id: &str) -> Bound<'py, PyBytes> {
    PyBytes::new(
        py,
        &bazaar::key_mapper::hash_escaped_prefix_unmap(partition_id),
    )
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

pyo3::import_exception!(bzrformats.errors, VersionedFileInvalidChecksum);

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
            mpvf.add_version(lines, rec.key(), vec![], None, false);
        }
    }

    let prepared = add_mpdiffs_prepare(&mut mpvf, &rs);

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
                mpvf.add_version(lines, wrap(vid.clone()), vec![], None, false);
            }
        }
    }

    let prepared = add_mpdiffs_prepare(&mut mpvf, &rs);

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

pub(crate) fn _versionedfile_rs(py: Python) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "versionedfile")?;
    m.add_class::<AbstractContentFactory>()?;
    m.add_class::<FulltextContentFactory>()?;
    m.add_class::<ChunkedContentFactory>()?;
    m.add_class::<AbsentContentFactory>()?;
    m.add_class::<KeyRefs>()?;
    m.add_function(wrap_pyfunction!(record_to_fulltext_bytes, &m)?)?;
    m.add_function(wrap_pyfunction!(fulltext_network_to_record, &m)?)?;
    m.add_function(wrap_pyfunction!(network_bytes_to_kind_and_offset, &m)?)?;
    m.add_function(wrap_pyfunction!(prefix_map, &m)?)?;
    m.add_function(wrap_pyfunction!(prefix_unmap, &m)?)?;
    m.add_function(wrap_pyfunction!(hash_prefix_map, &m)?)?;
    m.add_function(wrap_pyfunction!(hash_prefix_unmap, &m)?)?;
    m.add_function(wrap_pyfunction!(hash_escaped_prefix_map, &m)?)?;
    m.add_function(wrap_pyfunction!(hash_escaped_prefix_unmap, &m)?)?;
    m.add_function(wrap_pyfunction!(check_lines_not_unicode, &m)?)?;
    m.add_function(wrap_pyfunction!(check_lines_are_lines, &m)?)?;
    m.add_function(wrap_pyfunction!(known_graph_ancestry_map, &m)?)?;
    m.add_function(wrap_pyfunction!(make_mpdiffs, &m)?)?;
    m.add_function(wrap_pyfunction!(add_mpdiffs, &m)?)?;
    m.add_function(wrap_pyfunction!(add_mpdiffs_singular, &m)?)?;
    m.add_function(wrap_pyfunction!(make_mpdiffs_singular, &m)?)?;
    Ok(m)
}
