use bazaar::RevisionId;
use chrono::NaiveDateTime;
use pyo3::class::basic::CompareOp;
use pyo3::exceptions::{PyNotImplementedError, PyRuntimeError, PyTypeError, PyValueError};
use pyo3::import_exception;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyString};
use pyo3_filelike::PyBinaryFile;
use std::collections::HashMap;

mod bisect_multi;
mod btree_index;
mod btree_serializer;
mod chk_map;
mod chunk_writer;
mod dirstate;
mod dirstate_helpers;
mod groupcompress;
mod groupcompress_delta;
mod index;
mod inventory;
mod knit;
mod lock;
mod multiparent;
mod pack;
mod smart;
mod textmerge;
mod transport;
mod tuned_gzip;
mod versionedfile;
mod weave;

import_exception!(bzrformats.errors, ReservedId);

/// Create a new file id suffix that is reasonably unique.
///
/// On the first call we combine the current time with 64 bits of randomness to
/// give a highly probably globally unique number. Then each call in the same
/// process adds 1 to a serial number we append to that unique value.
#[pyfunction]
#[pyo3(signature = (suffix = None))]
fn _next_id_suffix<'py>(py: Python<'py>, suffix: Option<&str>) -> Bound<'py, PyBytes> {
    PyBytes::new(py, bazaar::gen_ids::next_id_suffix(suffix).as_slice())
}

/// Return new file id for the basename 'name'.
///
/// The uniqueness is supplied from _next_id_suffix.
#[pyfunction]
fn gen_file_id(name: &str) -> bazaar::FileId {
    bazaar::FileId::generate(name)
}

/// Return a new tree-root file id.
#[pyfunction]
fn gen_root_id() -> bazaar::FileId {
    bazaar::FileId::generate_root_id()
}

/// Return new revision-id.
///
/// Args:
///   username: The username of the committer, in the format returned by
///      config.username().  This is typically a real name, followed by an
///      email address. If found, we will use just the email address portion.
///      Otherwise we flatten the real name, and use that.
/// Returns: A new revision id.
#[pyfunction]
#[pyo3(signature = (username, timestamp = None))]
fn gen_revision_id(
    py: Python,
    username: &str,
    timestamp: Option<Py<PyAny>>,
) -> PyResult<bazaar::RevisionId> {
    let timestamp = match timestamp {
        Some(timestamp) => {
            if let Ok(timestamp) = timestamp.extract::<f64>(py) {
                Some(timestamp as u64)
            } else if let Ok(timestamp) = timestamp.extract::<u64>(py) {
                Some(timestamp)
            } else {
                return Err(PyTypeError::new_err(
                    "timestamp must be a float or an int".to_string(),
                ));
            }
        }
        None => None,
    };
    Ok(bazaar::RevisionId::generate(username, timestamp))
}

#[pyfunction]
fn normalize_pattern(pattern: &str) -> String {
    bazaar::globbing::normalize_pattern(pattern)
}

#[pyclass]
struct Replacer {
    replacer: bazaar::globbing::Replacer,
}

#[pymethods]
impl Replacer {
    #[new]
    #[pyo3(signature = (source = None))]
    fn new(source: Option<&Self>) -> Self {
        Self {
            replacer: bazaar::globbing::Replacer::new(source.map(|p| &p.replacer)),
        }
    }

    /// Add a pattern and replacement.
    ///
    /// The pattern must not contain capturing groups.
    /// The replacement might be either a string template in which \& will be
    /// replaced with the match, or a function that will get the matching text
    /// as argument. It does not get match object, because capturing is
    /// forbidden anyway.
    fn add(&mut self, py: Python, pattern: &str, func: Py<PyAny>) -> PyResult<()> {
        if let Ok(func) = func.extract::<String>(py) {
            self.replacer
                .add(pattern, bazaar::globbing::Replacement::String(func));
            Ok(())
        } else {
            let callable = Box::new(move |t: String| -> String {
                Python::attach(|py| match func.call1(py, (t,)) {
                    Ok(result) => result.extract::<String>(py).unwrap(),
                    Err(e) => {
                        e.restore(py);
                        String::new()
                    }
                })
            });
            self.replacer
                .add(pattern, bazaar::globbing::Replacement::Closure(callable));
            Ok(())
        }
    }

    /// Add all patterns from another replacer.
    ///
    /// All patterns and replacements from replacer are appended to the ones
    /// already defined.
    fn add_replacer(&mut self, replacer: &Self) {
        self.replacer.add_replacer(&replacer.replacer)
    }

    fn __call__(&mut self, py: Python, text: &str) -> PyResult<String> {
        let ret = self
            .replacer
            .replace(text)
            .map_err(|e| PyValueError::new_err(e.to_string()))?;
        if PyErr::occurred(py) {
            Err(PyErr::fetch(py))
        } else {
            Ok(ret)
        }
    }
}

#[pyclass(subclass)]
struct Revision(bazaar::revision::Revision);

/// Single revision on a branch.
///
/// Revisions may know their revision_hash, but only once they've been
/// written out.  This is not stored because you cannot write the hash
/// into the file it describes.
///
/// Attributes:
///   parent_ids: List of parent revision_ids
///
///   properties:
///     Dictionary of revision properties.  These are attached to the
///     revision as extra metadata.  The name must be a single
///     word; the value can be an arbitrary string.
#[pymethods]
impl Revision {
    #[new]
    #[pyo3(signature = (revision_id, parent_ids, committer, message, properties, inventory_sha1, timestamp, timezone))]
    fn new(
        py: Python,
        revision_id: RevisionId,
        parent_ids: Vec<RevisionId>,
        committer: Option<String>,
        message: String,
        properties: Option<HashMap<String, Py<PyAny>>>,
        inventory_sha1: Option<Vec<u8>>,
        timestamp: f64,
        timezone: Option<i32>,
    ) -> PyResult<Self> {
        let mut cproperties: HashMap<String, Vec<u8>> = HashMap::new();
        for (k, v) in properties.unwrap_or_default() {
            if let Ok(s) = v.extract::<Bound<PyBytes>>(py) {
                cproperties.insert(k, s.as_bytes().to_vec());
            } else if let Ok(s) = v.extract::<Bound<PyString>>(py) {
                let s = s
                    .call_method1("encode", ("utf-8", "surrogateescape"))?
                    .extract::<Bound<PyBytes>>()?;
                cproperties.insert(k, s.as_bytes().to_vec());
            } else {
                return Err(PyTypeError::new_err(
                    "properties must be a dictionary of strings",
                ));
            }
        }

        if !bazaar::revision::validate_properties(&cproperties) {
            return Err(PyValueError::new_err(
                "properties must be a dictionary of strings",
            ));
        }
        Ok(Self(bazaar::revision::Revision {
            revision_id,
            parent_ids,
            committer,
            message,
            properties: cproperties,
            inventory_sha1,
            timestamp,
            timezone,
        }))
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self.0 == other.0),
            CompareOp::Ne => Ok(self.0 != other.0),
            _ => Err(PyNotImplementedError::new_err(
                "only == and != are supported",
            )),
        }
    }

    fn __repr__(self_: PyRef<Self>) -> String {
        format!("<Revision id {:?}>", self_.0.revision_id)
    }

    #[getter]
    fn revision_id(&self) -> &bazaar::RevisionId {
        &self.0.revision_id
    }

    #[getter]
    fn parent_ids(&self) -> &Vec<RevisionId> {
        &self.0.parent_ids
    }

    #[getter]
    fn committer(&self) -> Option<String> {
        self.0.committer.clone()
    }

    #[getter]
    fn message(&self) -> String {
        self.0.message.clone()
    }

    #[getter]
    fn properties(&self) -> HashMap<String, String> {
        self.0
            .properties
            .iter()
            .map(|(k, v)| (k.clone(), String::from_utf8_lossy(v).into()))
            .collect()
    }

    #[getter]
    fn get_inventory_sha1<'py>(&self, py: Python<'py>) -> Bound<'py, PyAny> {
        if let Some(sha1) = &self.0.inventory_sha1 {
            PyBytes::new(py, sha1).into_any()
        } else {
            py.None().into_bound(py)
        }
    }

    #[setter]
    fn set_inventory_sha1(&mut self, py: Python, value: Py<PyAny>) -> PyResult<()> {
        if let Ok(value) = value.extract::<Bound<PyBytes>>(py) {
            self.0.inventory_sha1 = Some(value.as_bytes().to_vec());
            Ok(())
        } else if value.is_none(py) {
            self.0.inventory_sha1 = None;
            Ok(())
        } else {
            Err(PyTypeError::new_err("expected bytes or None"))
        }
    }

    #[getter]
    fn timestamp(&self) -> f64 {
        self.0.timestamp
    }

    #[getter]
    fn timezone(&self) -> Option<i32> {
        self.0.timezone
    }

    fn datetime(&self) -> NaiveDateTime {
        self.0.datetime()
    }

    fn check_properties(&self) -> PyResult<()> {
        if self.0.check_properties() {
            Ok(())
        } else {
            Err(PyValueError::new_err("invalid properties"))
        }
    }

    fn get_summary(&self) -> String {
        self.0.get_summary()
    }

    fn get_apparent_authors(&self) -> Vec<String> {
        self.0.get_apparent_authors()
    }

    fn bug_urls(&self) -> Vec<String> {
        self.0.bug_urls()
    }
}

fn serializer_err_to_py_err(e: bazaar::serializer::Error) -> PyErr {
    PyRuntimeError::new_err(format!("serializer error: {:?}", e))
}

#[pyclass(subclass)]
struct RevisionSerializer(Box<dyn bazaar::serializer::RevisionSerializer>);

#[pyclass(subclass,extends=RevisionSerializer)]
struct BEncodeRevisionSerializerv1;

#[pymethods]
impl BEncodeRevisionSerializerv1 {
    #[new]
    fn new() -> (Self, RevisionSerializer) {
        (
            Self {},
            RevisionSerializer(Box::new(
                bazaar::bencode_serializer::BEncodeRevisionSerializer1,
            )),
        )
    }
}

#[pyclass(subclass,extends=RevisionSerializer)]
struct XMLRevisionSerializer8;

#[pymethods]
impl XMLRevisionSerializer8 {
    #[new]
    fn new() -> (Self, RevisionSerializer) {
        (
            Self {},
            RevisionSerializer(Box::new(bazaar::xml_serializer::XMLRevisionSerializer8)),
        )
    }
}

/// v4 revision serializer (deserialization-only). Unlike v5/v8 it
/// also recovers `inventory_id` and `parent_sha1s` metadata, so it
/// doesn't fit the `RevisionSerializer` trait shape — it's exposed
/// directly with its own read methods that return a tuple.
#[pyclass]
struct XMLRevisionSerializer4;

#[pymethods]
impl XMLRevisionSerializer4 {
    #[new]
    fn new() -> Self {
        Self
    }

    #[getter]
    fn format_name(&self) -> &'static str {
        "4"
    }

    #[getter]
    fn squashes_xml_invalid_characters(&self) -> bool {
        true
    }

    fn read_revision_from_string<'py>(
        &self,
        py: Python<'py>,
        text: &[u8],
    ) -> PyResult<(
        Revision,
        Option<Bound<'py, PyBytes>>,
        Vec<Option<Bound<'py, PyBytes>>>,
    )> {
        let rv4 = py
            .detach(|| {
                bazaar::xml_serializer::XMLRevisionSerializer4.read_revision_from_string(text)
            })
            .map_err(serializer_err_to_py_err)?;
        Ok((
            Revision(rv4.revision),
            rv4.inventory_id.map(|v| PyBytes::new(py, &v)),
            rv4.parent_sha1s
                .into_iter()
                .map(|s| s.map(|v| PyBytes::new(py, &v)))
                .collect(),
        ))
    }

    fn read_revision<'py>(
        &self,
        py: Python<'py>,
        file: Py<PyAny>,
    ) -> PyResult<(
        Revision,
        Option<Bound<'py, PyBytes>>,
        Vec<Option<Bound<'py, PyBytes>>>,
    )> {
        let mut file = PyBinaryFile::from(file);
        let rv4 = py
            .detach(|| bazaar::xml_serializer::XMLRevisionSerializer4.read_revision(&mut file))
            .map_err(serializer_err_to_py_err)?;
        Ok((
            Revision(rv4.revision),
            rv4.inventory_id.map(|v| PyBytes::new(py, &v)),
            rv4.parent_sha1s
                .into_iter()
                .map(|s| s.map(|v| PyBytes::new(py, &v)))
                .collect(),
        ))
    }
}

#[pyclass(subclass,extends=RevisionSerializer)]
struct XMLRevisionSerializer5;

#[pymethods]
impl XMLRevisionSerializer5 {
    #[new]
    fn new() -> (Self, RevisionSerializer) {
        (
            Self {},
            RevisionSerializer(Box::new(bazaar::xml_serializer::XMLRevisionSerializer5)),
        )
    }
}

#[pymethods]
impl RevisionSerializer {
    #[getter]
    fn format_name(&self) -> String {
        self.0.format_name().to_string()
    }

    #[getter]
    fn squashes_xml_invalid_characters(&self) -> bool {
        self.0.squashes_xml_invalid_characters()
    }

    fn read_revision(&self, py: Python, file: Py<PyAny>) -> PyResult<Revision> {
        py.detach(|| {
            let mut file = PyBinaryFile::from(file);
            Ok(Revision(
                self.0
                    .read_revision(&mut file)
                    .map_err(serializer_err_to_py_err)?,
            ))
        })
    }

    fn write_revision_to_string<'py>(
        &self,
        py: Python<'py>,
        revision: &Revision,
    ) -> PyResult<Bound<'py, PyBytes>> {
        Ok(PyBytes::new(
            py,
            py.detach(|| self.0.write_revision_to_string(&revision.0))
                .map_err(serializer_err_to_py_err)?
                .as_slice(),
        ))
    }

    fn write_revision_to_lines<'a>(
        &self,
        py: Python<'a>,
        revision: &Revision,
    ) -> PyResult<Vec<Bound<'a, PyBytes>>> {
        self.0
            .write_revision_to_lines(&revision.0)
            .map(|s| -> PyResult<Bound<PyBytes>> {
                Ok(PyBytes::new(
                    py,
                    s.map_err(serializer_err_to_py_err)?.as_slice(),
                ))
            })
            .collect::<PyResult<Vec<Bound<PyBytes>>>>()
    }

    fn read_revision_from_string(&self, py: Python, string: &[u8]) -> PyResult<Revision> {
        Ok(Revision(
            py.detach(|| self.0.read_revision_from_string(string))
                .map_err(serializer_err_to_py_err)?,
        ))
    }
}

import_exception!(bzrformats.serializer, UnexpectedInventoryFormat);
import_exception!(bzrformats.serializer, UnsupportedInventoryKind);

fn inventory_serializer_err_to_py_err(e: bazaar::serializer::Error) -> PyErr {
    use bazaar::serializer::Error;
    match e {
        Error::UnexpectedInventoryFormat(msg) => UnexpectedInventoryFormat::new_err(msg),
        Error::UnsupportedInventoryKind(kind) => UnsupportedInventoryKind::new_err((kind,)),
        other => PyRuntimeError::new_err(format!("serializer error: {:?}", other)),
    }
}

#[pyclass(subclass)]
struct InventorySerializer(Box<dyn bazaar::serializer::InventorySerializer>);

#[pymethods]
impl InventorySerializer {
    #[getter]
    fn format_num<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, self.0.format_num())
    }

    #[pyo3(signature = (inv, f, working = false))]
    fn write_inventory<'py>(
        &self,
        py: Python<'py>,
        inv: &inventory::Inventory,
        f: Py<PyAny>,
        working: bool,
    ) -> PyResult<Vec<Bound<'py, PyBytes>>> {
        if working {
            return Err(PyRuntimeError::new_err(
                "working=True is not supported by Rust XML inventory serializer",
            ));
        }
        let lines = self
            .0
            .write_inventory_to_lines(&inv.0)
            .map_err(inventory_serializer_err_to_py_err)?;
        if !f.is_none(py) {
            for line in &lines {
                f.call_method1(py, "write", (PyBytes::new(py, line),))?;
            }
        }
        Ok(lines.into_iter().map(|l| PyBytes::new(py, &l)).collect())
    }

    fn write_inventory_to_lines<'py>(
        &self,
        py: Python<'py>,
        inv: &inventory::Inventory,
    ) -> PyResult<Vec<Bound<'py, PyBytes>>> {
        let lines = self
            .0
            .write_inventory_to_lines(&inv.0)
            .map_err(inventory_serializer_err_to_py_err)?;
        Ok(lines.into_iter().map(|l| PyBytes::new(py, &l)).collect())
    }

    fn write_inventory_to_chunks<'py>(
        &self,
        py: Python<'py>,
        inv: &inventory::Inventory,
    ) -> PyResult<Vec<Bound<'py, PyBytes>>> {
        self.write_inventory_to_lines(py, inv)
    }

    fn write_inventory_to_string<'py>(
        &self,
        py: Python<'py>,
        inv: &inventory::Inventory,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let buf = self
            .0
            .write_inventory_to_string(&inv.0)
            .map_err(inventory_serializer_err_to_py_err)?;
        Ok(PyBytes::new(py, &buf))
    }

    #[pyo3(signature = (lines, revision_id = None, entry_cache = None, return_from_cache = false))]
    fn read_inventory_from_lines(
        &self,
        py: Python,
        lines: Vec<Vec<u8>>,
        revision_id: Option<RevisionId>,
        entry_cache: Option<Py<PyAny>>,
        return_from_cache: bool,
    ) -> PyResult<inventory::Inventory> {
        let _ = (entry_cache, return_from_cache);
        let line_refs: Vec<&[u8]> = lines.iter().map(|v| v.as_slice()).collect();
        let inv = py
            .detach(|| self.0.read_inventory_from_lines(&line_refs, revision_id))
            .map_err(inventory_serializer_err_to_py_err)?;
        Ok(inventory::Inventory(inv))
    }

    #[pyo3(signature = (f, revision_id = None))]
    fn read_inventory(
        &self,
        py: Python,
        f: Py<PyAny>,
        revision_id: Option<RevisionId>,
    ) -> PyResult<inventory::Inventory> {
        let mut file = PyBinaryFile::from(f);
        let inv = py
            .detach(|| self.0.read_inventory(&mut file, revision_id))
            .map_err(inventory_serializer_err_to_py_err)?;
        Ok(inventory::Inventory(inv))
    }
}

#[pyclass(subclass, extends = InventorySerializer)]
struct XMLInventorySerializer4;

#[pymethods]
impl XMLInventorySerializer4 {
    #[new]
    fn new() -> (Self, InventorySerializer) {
        (
            Self,
            InventorySerializer(Box::new(bazaar::xml_serializer::XMLInventorySerializer4)),
        )
    }
}

#[pyclass(subclass, extends = InventorySerializer)]
struct XMLInventorySerializer5;

#[pymethods]
impl XMLInventorySerializer5 {
    #[new]
    fn new() -> (Self, InventorySerializer) {
        (
            Self,
            InventorySerializer(Box::new(bazaar::xml_serializer::XMLInventorySerializer5)),
        )
    }
}

#[pyclass(subclass, extends = InventorySerializer)]
struct XMLInventorySerializer6;

#[pymethods]
impl XMLInventorySerializer6 {
    #[new]
    fn new() -> (Self, InventorySerializer) {
        (
            Self,
            InventorySerializer(Box::new(bazaar::xml_serializer::XMLInventorySerializer6)),
        )
    }
}

#[pyclass(subclass, extends = InventorySerializer)]
struct XMLInventorySerializer7;

#[pymethods]
impl XMLInventorySerializer7 {
    #[new]
    fn new() -> (Self, InventorySerializer) {
        (
            Self,
            InventorySerializer(Box::new(bazaar::xml_serializer::XMLInventorySerializer7)),
        )
    }
}

#[pyclass(subclass, extends = InventorySerializer)]
struct XMLInventorySerializer8;

#[pymethods]
impl XMLInventorySerializer8 {
    #[new]
    fn new() -> (Self, InventorySerializer) {
        (
            Self,
            InventorySerializer(Box::new(bazaar::xml_serializer::XMLInventorySerializer8)),
        )
    }
}

#[pyfunction(name = "is_null")]
fn is_null_revision(revision_id: RevisionId) -> bool {
    revision_id.is_null()
}

#[pyfunction(name = "is_reserved_id")]
fn is_reserved_revision_id(revision_id: RevisionId) -> bool {
    revision_id.is_reserved()
}

#[pyfunction(name = "check_not_reserved_id")]
fn check_not_reserved_id(_py: Python, revision_id: Bound<PyBytes>) -> PyResult<()> {
    if revision_id.is_none() {
        return Ok(());
    }
    if let Ok(revision_id) = revision_id.extract::<RevisionId>() {
        if revision_id.is_reserved() {
            Err(ReservedId::new_err((revision_id,)))
        } else {
            Ok(())
        }
    } else {
        // For now, just ignore other types..
        Ok(())
    }
}

#[pyfunction]
#[pyo3(signature = (message = None))]
fn escape_invalid_chars(message: Option<&str>) -> (Option<String>, usize) {
    if let Some(message) = message {
        (
            Some(bazaar::xml_serializer::escape_invalid_chars(message)),
            message.len(),
        )
    } else {
        (None, 0)
    }
}

#[pyfunction]
fn encode_and_escape(py: Python, unicode_or_utf8_str: Py<PyAny>) -> PyResult<Bound<PyBytes>> {
    let ret = if let Ok(text) = unicode_or_utf8_str.extract::<String>(py) {
        bazaar::xml_serializer::encode_and_escape_string(&text)
    } else if let Ok(bytes) = unicode_or_utf8_str.extract::<Vec<u8>>(py) {
        bazaar::xml_serializer::encode_and_escape_bytes(&bytes)
    } else {
        return Err(PyTypeError::new_err("expected str or bytes"));
    };

    Ok(PyBytes::new(py, ret.as_bytes()))
}

mod hashcache;
mod rio;

#[pymodule]
fn _bzr_rs(py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_wrapped(wrap_pyfunction!(_next_id_suffix))?;
    m.add_wrapped(wrap_pyfunction!(gen_file_id))?;
    m.add_wrapped(wrap_pyfunction!(gen_root_id))?;
    m.add_wrapped(wrap_pyfunction!(gen_revision_id))?;
    let m_globbing = PyModule::new(py, "globbing")?;
    m_globbing.add_wrapped(wrap_pyfunction!(normalize_pattern))?;
    m_globbing.add_class::<Replacer>()?;
    m.add_submodule(&m_globbing)?;
    m.add_class::<Revision>()?;
    let inventorym = inventory::_inventory_rs(py)?;
    m.add_submodule(&inventorym)?;
    m.add_class::<RevisionSerializer>()?;
    m.add_class::<BEncodeRevisionSerializerv1>()?;
    m.add_class::<XMLRevisionSerializer4>()?;
    m.add_class::<XMLRevisionSerializer5>()?;
    m.add_class::<XMLRevisionSerializer8>()?;
    m.add_class::<InventorySerializer>()?;
    m.add_class::<XMLInventorySerializer4>()?;
    m.add_class::<XMLInventorySerializer5>()?;
    m.add_class::<XMLInventorySerializer6>()?;
    m.add_class::<XMLInventorySerializer7>()?;
    m.add_class::<XMLInventorySerializer8>()?;
    m.add(
        "revision_bencode_serializer",
        m.getattr("BEncodeRevisionSerializerv1")?.call0()?,
    )?;
    m.add(
        "revision_serializer_v8",
        m.getattr("XMLRevisionSerializer8")?.call0()?,
    )?;
    m.add(
        "revision_serializer_v5",
        m.getattr("XMLRevisionSerializer5")?.call0()?,
    )?;
    m.add(
        "_revision_serializer_v4_rs",
        m.getattr("XMLRevisionSerializer4")?.call0()?,
    )?;
    m.add(
        "inventory_serializer_v4",
        m.getattr("XMLInventorySerializer4")?.call0()?,
    )?;
    m.add(
        "inventory_serializer_v5",
        m.getattr("XMLInventorySerializer5")?.call0()?,
    )?;
    m.add(
        "inventory_serializer_v6",
        m.getattr("XMLInventorySerializer6")?.call0()?,
    )?;
    m.add(
        "inventory_serializer_v7",
        m.getattr("XMLInventorySerializer7")?.call0()?,
    )?;
    m.add(
        "inventory_serializer_v8",
        m.getattr("XMLInventorySerializer8")?.call0()?,
    )?;
    m.add("CURRENT_REVISION", bazaar::CURRENT_REVISION)?;
    m.add("NULL_REVISION", bazaar::NULL_REVISION)?;
    m.add("ROOT_ID", bazaar::inventory::ROOT_ID)?;
    m.add_wrapped(wrap_pyfunction!(is_null_revision))?;
    m.add_wrapped(wrap_pyfunction!(is_reserved_revision_id))?;
    m.add_wrapped(wrap_pyfunction!(check_not_reserved_id))?;
    m.add_wrapped(wrap_pyfunction!(escape_invalid_chars))?;
    m.add_wrapped(wrap_pyfunction!(encode_and_escape))?;

    let riom = PyModule::new(py, "rio")?;
    rio::rio(&riom)?;
    m.add_submodule(&riom)?;

    let hashcachem = PyModule::new(py, "hashcache")?;
    hashcache::hashcache(&hashcachem)?;
    m.add_submodule(&hashcachem)?;

    let dirstatem = dirstate::_dirstate_rs(py)?;
    m.add_submodule(&dirstatem)?;

    let lockm = lock::_lock_rs(py)?;
    m.add_submodule(&lockm)?;

    let groupcompressm = groupcompress::_groupcompress_rs(py)?;
    m.add_submodule(&groupcompressm)?;

    let chk_mapm = chk_map::_chk_map_rs(py)?;
    m.add_submodule(&chk_mapm)?;

    let knitm = knit::_knit_rs(py)?;
    m.add_submodule(&knitm)?;

    let smartm = smart::_smart_rs(py)?;
    m.add_submodule(&smartm)?;

    let textmergem = textmerge::_textmerge_rs(py)?;
    m.add_submodule(&textmergem)?;

    let multiparentm = multiparent::_multiparent_rs(py)?;
    m.add_submodule(&multiparentm)?;

    let weavem = weave::_weave_rs(py)?;
    m.add_submodule(&weavem)?;

    let packm = pack::_pack_rs(py)?;
    m.add_submodule(&packm)?;

    let indexm = index::_index_rs(py)?;
    m.add_submodule(&indexm)?;

    let btree_indexm = btree_index::_btree_index_rs(py)?;
    m.add_submodule(&btree_indexm)?;

    let versionedfilem = versionedfile::_versionedfile_rs(py)?;
    m.add_submodule(&versionedfilem)?;

    let btree_serializerm = btree_serializer::_btree_serializer_rs(py)?;
    m.add_submodule(&btree_serializerm)?;

    let bisect_multim = bisect_multi::_bisect_multi_rs(py)?;
    m.add_submodule(&bisect_multim)?;

    let chunk_writerm = chunk_writer::_chunk_writer_rs(py)?;
    m.add_submodule(&chunk_writerm)?;

    let tuned_gzipm = tuned_gzip::_tuned_gzip_rs(py)?;
    m.add_submodule(&tuned_gzipm)?;

    let chunk_writerm = chunk_writer::_chunk_writer_rs(py)?;
    m.add_submodule(&chunk_writerm)?;

    let bisect_multim = bisect_multi::_bisect_multi_rs(py)?;
    m.add_submodule(&bisect_multim)?;

    // PyO3 submodule hack for proper import support
    let sys = py.import("sys")?;
    let modules = sys.getattr("modules")?;
    let module_name = m.name()?;

    // Register submodules in sys.modules for dotted import support
    modules.set_item(format!("{}.globbing", module_name), &m_globbing)?;
    modules.set_item(format!("{}.inventory", module_name), &inventorym)?;
    modules.set_item(format!("{}.rio", module_name), &riom)?;
    modules.set_item(format!("{}.hashcache", module_name), &hashcachem)?;
    modules.set_item(format!("{}.dirstate", module_name), &dirstatem)?;
    modules.set_item(format!("{}.lock", module_name), &lockm)?;
    modules.set_item(format!("{}.groupcompress", module_name), &groupcompressm)?;
    modules.set_item(format!("{}.chk_map", module_name), &chk_mapm)?;
    modules.set_item(format!("{}.knit", module_name), &knitm)?;
    modules.set_item(format!("{}.smart", module_name), &smartm)?;
    modules.set_item(format!("{}.textmerge", module_name), &textmergem)?;
    modules.set_item(format!("{}.multiparent", module_name), &multiparentm)?;
    modules.set_item(format!("{}.weave", module_name), &weavem)?;
    modules.set_item(format!("{}.pack", module_name), &packm)?;
    modules.set_item(format!("{}.index", module_name), &indexm)?;
    modules.set_item(format!("{}.btree_index", module_name), &btree_indexm)?;
    modules.set_item(format!("{}.versionedfile", module_name), &versionedfilem)?;
    modules.set_item(
        format!("{}.btree_serializer", module_name),
        &btree_serializerm,
    )?;
    modules.set_item(format!("{}.bisect_multi", module_name), &bisect_multim)?;
    modules.set_item(format!("{}.chunk_writer", module_name), &chunk_writerm)?;
    modules.set_item(format!("{}.tuned_gzip", module_name), &tuned_gzipm)?;
    modules.set_item(format!("{}.chunk_writer", module_name), &chunk_writerm)?;
    modules.set_item(format!("{}.bisect_multi", module_name), &bisect_multim)?;

    Ok(())
}
