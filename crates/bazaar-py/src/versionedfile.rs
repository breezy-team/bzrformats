use bazaar::versionedfile::{ContentFactory, Key};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PySet};

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
pub fn record_to_fulltext_bytes(py: Python, record: Py<PyAny>) -> PyResult<Py<PyAny>> {
    let record = record.extract::<bazaar::pyversionedfile::PyContentFactory>(py)?;

    let mut s = Vec::new();

    bazaar::versionedfile::record_to_fulltext_bytes(record, &mut s)?;

    Ok(PyBytes::new(py, &s).into())
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

/// Reference-count bookkeeping for compression-parent satisfaction during
/// stream insertion. Python-facing counterpart of the pure-Rust
/// `bazaar::versionedfile::KeyRefs`; stores Python tuples directly via
/// `PyDict`/`PySet` so hashing delegates to the Python tuple hash.
///
/// Mirrors `bzrformats.versionedfile._KeyRefs` one-to-one. `refs` maps
/// each referenced parent key to the set of child keys that reference it,
/// and `new_keys` (when tracking is enabled) remembers every key added.
#[pyclass(name = "KeyRefs")]
struct KeyRefs {
    refs: Py<PyDict>,
    new_keys: Option<Py<PySet>>,
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
    Ok(m)
}
