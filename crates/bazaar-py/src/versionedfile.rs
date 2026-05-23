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

/// First pass of `_MPDiffGenerator._find_needed_keys`: from `ordered_keys` plus
/// the parent map for those keys, derive:
///
/// * `needed_keys` – ordered_keys ∪ all parent keys (may include ghosts)
/// * `refcounts`   – {parent_key: child_count} over the same parents
/// * `just_parents` – parent_keys \ keys-present-in-parent_map (i.e. parents
///   that themselves still need to be looked up to distinguish ghosts)
/// * `missing_keys` – ordered_keys that are not present in parent_map; the
///   caller raises `RevisionNotPresent` with its own `vf` reference.
///
/// Mirrors the pure set/dict bookkeeping in `versionedfile._MPDiffGenerator`.
/// Does not touch the VersionedFile – the caller handles the two
/// `vf.get_parent_map` round trips and the ghost subtraction afterwards.
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

    // `needed_keys.difference(parent_map)` — returned to the caller so it can
    // raise `RevisionNotPresent(first, vf)` with its own vf reference.
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
        // `if not parent_keys` also covers the empty-tuple case.
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

    // just_parents.difference_update(parent_map): drop any parent that is
    // itself a key in parent_map (i.e. already known to be present).
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
/// the refcount reaches zero, pop the cached value from `chunks` (last child);
/// otherwise fetch (not pop) the still-shared cached value. Returns the list
/// of parent cached values in the original order, ready for the caller to run
/// `osutils.chunks_to_lines` and `_compute_diff`.
///
/// Mutates `refcounts` and `chunks` in place. The caller is responsible for
/// the per-record `this_chunks` cache write after diffing.
#[pyfunction]
fn mpdiff_collect_parent_chunks<'py>(
    py: Python<'py>,
    parent_keys: &Bound<'py, PyAny>,
    ghost_parents: &Bound<'py, PySet>,
    refcounts: &Bound<'py, PyDict>,
    chunks: &Bound<'py, PyDict>,
) -> PyResult<Py<PyAny>> {
    let out = pyo3::types::PyList::empty(py);
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

    pub(crate) fn add_key_rust<'py>(&self, py: Python<'py>, key: Bound<'py, PyAny>) -> PyResult<()> {
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
                let pk = k.clone().into_pyobject(py).map_err(|e| vf_err_from_py(py, e))?;
                py_keys.add(pk).map_err(|e| vf_err_from_py(py, e))?;
            }
            let result = self
                .obj
                .bind(py)
                .call_method1("get_parent_map", (py_keys,))
                .map_err(|e| vf_err_from_py(py, e))?;
            let result = result.cast_into::<PyDict>().map_err(|e| vf_err_from_py(py, e.into()))?;
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
                let pk = k.clone().into_pyobject(py).map_err(|e| vf_err_from_py(py, e))?;
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
            Ok(Box::new(PyRecordStream { stream: stream.unbind() })
                as Box<
                    dyn Iterator<
                        Item = Result<Box<dyn ContentFactory>, bazaar::knit::KnitError>,
                    >,
                >)
        })
    }

    fn get_sha1s(&self, keys: &[Key]) -> Result<std::collections::HashMap<Key, Vec<u8>>, bazaar::knit::KnitError> {
        Python::attach(|py| {
            let py_keys = PySet::empty(py).map_err(|e| vf_err_from_py(py, e))?;
            for k in keys {
                let pk = k.clone().into_pyobject(py).map_err(|e| vf_err_from_py(py, e))?;
                py_keys.add(pk).map_err(|e| vf_err_from_py(py, e))?;
            }
            let result = self
                .obj
                .bind(py)
                .call_method1("get_sha1s", (py_keys,))
                .map_err(|e| vf_err_from_py(py, e))?;
            let result = result.cast_into::<PyDict>().map_err(|e| vf_err_from_py(py, e.into()))?;
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
            let py_key = key.clone().into_pyobject(py).map_err(|e| vf_err_from_py(py, e))?;
            let py_parents = match parents {
                None => py.None().into_bound(py),
                Some(ps) => {
                    let lst = PyList::empty(py);
                    for p in ps {
                        let pp = p.clone().into_pyobject(py).map_err(|e| vf_err_from_py(py, e))?;
                        lst.append(pp).map_err(|e| vf_err_from_py(py, e))?;
                    }
                    lst.into_any()
                }
            };
            let py_lines = PyList::empty(py);
            for l in lines {
                py_lines.append(PyBytes::new(py, l)).map_err(|e| vf_err_from_py(py, e))?;
            }
            let result = self
                .obj
                .bind(py)
                .call_method1("add_lines", (py_key, py_parents, py_lines))
                .map_err(|e| vf_err_from_py(py, e))?;
            let result = result.cast_into::<PyTuple>().map_err(|e| vf_err_from_py(py, e.into()))?;
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
                let pk = k.clone().into_pyobject(py).map_err(|e| vf_err_from_py(py, e))?;
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
                let tup = tup.cast_into::<PyTuple>().map_err(|e| vf_err_from_py(py, e.into()))?;
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
            let py_key = key.clone().into_pyobject(py).map_err(|e| vf_err_from_py(py, e))?;
            let result = self
                .obj
                .bind(py)
                .call_method1("annotate", (py_key,))
                .map_err(|e| vf_err_from_py(py, e))?;
            let mut out = Vec::new();
            for item in result.try_iter().map_err(|e| vf_err_from_py(py, e))? {
                let tup = item.map_err(|e| vf_err_from_py(py, e))?;
                let tup = tup.cast_into::<PyTuple>().map_err(|e| vf_err_from_py(py, e.into()))?;
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
                Err(e) if e.is_instance_of::<pyo3::exceptions::PyStopIteration>(py) => {
                    return None
                }
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
    let parents_obj = record.getattr("parents").map_err(|e| vf_err_from_py(py, e))?;
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
/// `records` is an iterable of `(key, parents, expected_sha1, MultiParent)`
/// tuples. The orchestration:
///
/// 1. Build a [`MultiMemoryVersionedFile`] over the input diffs, indexed
///    by key.
/// 2. Pull the needed parent fulltexts via `vf.get_record_stream(needed,
///    "unordered", True)` and add them to the mpvf as snapshots.
/// 3. Reconstruct each input key's lines from the mpvf chain, compute
///    matching blocks for the single-parent case, then call
///    `vf.add_lines(key, parents, lines, parent_texts, left_matching_blocks)`.
/// 4. Verify the returned sha1 matches `expected_sha1`; raise
///    `VersionedFileInvalidChecksum` otherwise.
///
/// Mirrors `bzrformats.versionedfile.VersionedFiles.add_mpdiffs` exactly.
#[pyfunction]
fn add_mpdiffs(py: Python<'_>, vf: Py<PyAny>, records: Bound<'_, PyAny>) -> PyResult<()> {
    use bazaar::multiparent::MultiMemoryVersionedFile;

    // Materialise the records once: the original Python loop iterates
    // twice (build mpvf, find missing parents) so we cache here too.
    struct Record {
        key: Key,
        parents: Vec<Key>,
        expected_sha1: Vec<u8>,
        mp: bazaar::multiparent::MultiParent,
    }
    let mut rs: Vec<Record> = Vec::new();
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
        let mp = crate::multiparent::py_hunks_to_rust(&hunks)?;
        rs.push(Record {
            key,
            parents,
            expected_sha1,
            mp,
        });
    }

    let mut mpvf: MultiMemoryVersionedFile<Key> = MultiMemoryVersionedFile::default();
    for r in &rs {
        mpvf.add_diff(r.mp.clone(), r.key.clone(), r.parents.clone());
    }

    // Collect parents that aren't already in the mpvf — those are the
    // ones we have to fetch as fulltexts.
    let mut needed_parents: std::collections::HashSet<Key> = std::collections::HashSet::new();
    for r in &rs {
        for p in &r.parents {
            if !mpvf.has_version(p) {
                needed_parents.insert(p.clone());
            }
        }
    }

    if !needed_parents.is_empty() {
        // Fetch via PyVersionedFiles so we don't have to materialise the
        // whole iterator into a list. The trait already streams.
        use bazaar::versionedfile::VersionedFiles;
        let wrapped = PyVersionedFiles::new(vf.clone_ref(py));
        let needed_vec: Vec<Key> = needed_parents.into_iter().collect();
        let stream = wrapped
            .get_record_stream(&needed_vec, "unordered", true)
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

    // Reconstruct each key's lines from the mpvf chain and dispatch to
    // vf.add_lines. `vf_parents` is the opaque `parent_texts` map the
    // Python add_lines threads back so the implementation can avoid
    // re-fetching.
    let vf_bound = vf.bind(py);
    let vf_parents = PyDict::new(py);
    let keys: Vec<Key> = rs.iter().map(|r| r.key.clone()).collect();
    let reconstructed = mpvf.get_line_list(&keys);
    for (r, lines) in rs.iter().zip(reconstructed.into_iter()) {
        let left_matching_blocks_obj: Py<PyAny> = if r.parents.len() == 1 {
            let parent_len = mpvf
                .get_diff(&r.parents[0])
                .map(bazaar::multiparent::MultiParent::num_lines)
                .unwrap_or(0);
            let blocks = r.mp.get_matching_blocks(0, parent_len);
            PyList::new(py, blocks.iter().map(|t| PyTuple::new(py, [t.0, t.1, t.2]).unwrap()))?
                .into_any()
                .unbind()
        } else {
            py.None()
        };

        let py_key = r.key.clone().into_pyobject(py)?;
        let py_parents = PyList::empty(py);
        for p in &r.parents {
            py_parents.append(p.clone().into_pyobject(py)?)?;
        }
        let py_lines = PyList::empty(py);
        for l in &lines {
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
        if version_sha1 != r.expected_sha1 {
            // Python passes the *version* (key) here, mirroring the
            // historical message.
            let version_repr = format!("{:?}", r.key);
            return Err(VersionedFileInvalidChecksum::new_err(version_repr));
        }
        let version_text = result.get_item(2)?;
        let key_py = r.key.clone().into_pyobject(py)?;
        vf_parents.set_item(key_py, version_text)?;
    }

    Ok(())
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
    m.add_function(wrap_pyfunction!(mpdiff_first_pass, &m)?)?;
    m.add_function(wrap_pyfunction!(mpdiff_collect_parent_chunks, &m)?)?;
    m.add_function(wrap_pyfunction!(check_lines_not_unicode, &m)?)?;
    m.add_function(wrap_pyfunction!(check_lines_are_lines, &m)?)?;
    m.add_function(wrap_pyfunction!(known_graph_ancestry_map, &m)?)?;
    m.add_function(wrap_pyfunction!(add_mpdiffs, &m)?)?;
    Ok(m)
}
