use bazaar::inventory::{describe_change, detect_changes, Entry, Error, Inventory as _};
use bazaar::inventory_delta::{
    InventoryDeltaEntry, InventoryDeltaInconsistency, InventoryDeltaParseError,
    InventoryDeltaSerializeError,
};
use bazaar::osutils::Kind;
use bazaar::{FileId, RevisionId};
use pyo3::class::basic::CompareOp;
use pyo3::exceptions::{
    PyIndexError, PyKeyError, PyNotImplementedError, PyTypeError, PyValueError,
};
use pyo3::prelude::*;
use pyo3::pyclass_init::PyClassInitializer;
use pyo3::types::{PyBytes, PyDict, PyList, PyString, PyTuple};
use pyo3::wrap_pyfunction;
use pyo3::{create_exception, import_exception};
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;

use std::iter::FromIterator;

import_exception!(bzrformats.inventory, InvalidEntryName);
import_exception!(bzrformats.inventory, DuplicateFileId);
import_exception!(bzrformats.inventory, NoSuchId);
import_exception!(bzrformats._bzr_rs.errors, BzrCheckError);
import_exception!(bzrformats._bzr_rs.errors, InvalidNormalization);
import_exception!(bzrformats._bzr_rs.errors, InconsistentDelta);
import_exception!(bzrformats._bzr_rs.errors, AlreadyVersionedError);
import_exception!(bzrformats._bzr_rs.errors, BzrFormatsError);
import_exception!(bzrformats.errors, NotADirectory);
import_exception!(bzrformats._bzr_rs.errors, NotVersionedError);
create_exception!(
    bzrformats.inventory_delta,
    IncompatibleInventoryDelta,
    BzrFormatsError
);
create_exception!(
    bzrformats.inventory_delta,
    InventoryDeltaError,
    BzrFormatsError
);

fn kind_from_str(kind: &str) -> Option<Kind> {
    match kind {
        "file" => Some(Kind::File),
        "directory" => Some(Kind::Directory),
        "tree-reference" => Some(Kind::TreeReference),
        "symlink" => Some(Kind::Symlink),
        _ => None,
    }
}

fn check_name(name: &str) -> PyResult<()> {
    if !is_valid_name(name) {
        Err(InvalidEntryName::new_err((name.to_string(),)))
    } else {
        Ok(())
    }
}

fn common_ie_check(
    slf: Py<PyAny>,
    ie: &Entry,
    py: Python,
    checker: &Py<PyAny>,
    rev_id: &RevisionId,
    inv: Py<PyAny>,
) -> PyResult<()> {
    if let Some(parent_id) = ie.parent_id() {
        let present = inv
            .call_method1(py, "has_id", (parent_id,))?
            .extract::<bool>(py)?;
        if !present {
            return Err(BzrCheckError::new_err(format!(
                "missing parent {{{}}} in inventory for revision {{{}}}",
                parent_id, rev_id
            )));
        }
    }

    checker.call_method1(py, "_add_entry_to_text_key_references", (inv, slf))?;

    Ok(())
}

#[pyclass(subclass)]
pub struct InventoryEntry(pub Entry);

#[pymethods]
impl InventoryEntry {
    fn has_text(&self) -> bool {
        matches!(&self.0, Entry::File { .. })
    }

    fn kind_character(&self) -> &'static str {
        self.0.kind().marker()
    }

    #[getter]
    fn kind(&self) -> &'static str {
        self.0.kind().as_str()
    }

    #[getter]
    fn get_name(&self) -> &str {
        match &self.0 {
            Entry::File { name, .. } => name,
            Entry::Directory { name, .. } => name,
            Entry::TreeReference { name, .. } => name,
            Entry::Link { name, .. } => name,
            Entry::Root { .. } => "",
        }
    }

    #[getter]
    fn get_file_id<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyBytes>> {
        let file_id = self.0.file_id();

        file_id.into_pyobject(py)
    }

    #[getter]
    fn get_parent_id<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyBytes>> {
        let parent_id = self.0.parent_id();

        parent_id.map(|parent_id| parent_id.into_pyobject(py).unwrap())
    }

    #[getter]
    fn get_revision<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyBytes>> {
        let revision = self.0.revision();

        revision
            .as_ref()
            .map(|revision| revision.into_pyobject(py).unwrap())
    }

    #[staticmethod]
    fn versionable_kind(kind: &str) -> bool {
        if let Some(kind) = kind_from_str(kind) {
            bazaar::inventory::versionable_kind(kind)
        } else {
            false
        }
    }

    #[getter]
    fn get_executable(&self) -> bool {
        match &self.0 {
            Entry::File { executable, .. } => *executable,
            _ => false,
        }
    }

    fn is_unmodified(&self, other: &InventoryEntry) -> bool {
        self.0.is_unmodified(&other.0)
    }

    fn detect_changes(&self, other: &InventoryEntry) -> (bool, bool) {
        detect_changes(&self.0, &other.0)
    }

    #[staticmethod]
    #[pyo3(signature = (slf=None, other=None))]
    fn describe_change(slf: Option<&InventoryEntry>, other: Option<&InventoryEntry>) -> String {
        describe_change(slf.map(|s| &s.0), other.map(|o| &o.0)).to_string()
    }

    fn __richcmp__(&self, other: &InventoryEntry, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self.0 == other.0),
            CompareOp::Ne => Ok(self.0 != other.0),
            _ => Err(PyNotImplementedError::new_err("")),
        }
    }

    fn _unchanged(&self, other: &InventoryEntry) -> bool {
        self.0.unchanged(&other.0)
    }

    #[pyo3(signature = (revision=None, name=None, parent_id=None))]
    fn derive(
        &self,
        revision: Option<RevisionId>,
        name: Option<String>,
        parent_id: Option<FileId>,
    ) -> InventoryEntry {
        let mut entry = self.0.clone();
        let revision = revision.or_else(|| entry.revision().cloned());
        let name = name.unwrap_or_else(|| entry.name().to_string());
        let parent_id = parent_id.or_else(|| entry.parent_id().cloned());
        match &mut entry {
            Entry::File {
                revision: r,
                name: n,
                parent_id: p,
                ..
            } => {
                *r = revision;
                *n = name;
                *p = parent_id.unwrap();
            }
            Entry::Directory {
                revision: r,
                name: n,
                parent_id: p,
                ..
            } => {
                *r = revision;
                *n = name;
                *p = parent_id.unwrap();
            }
            Entry::TreeReference {
                revision: r,
                name: n,
                parent_id: p,
                ..
            } => {
                *r = revision;
                *n = name;
                *p = parent_id.unwrap();
            }
            Entry::Link {
                revision: r,
                name: n,
                parent_id: p,
                ..
            } => {
                *r = revision;
                *n = name;
                *p = parent_id.unwrap();
            }
            Entry::Root { revision: r, .. } => {
                *r = revision;
            }
        }
        InventoryEntry(entry)
    }

    /// Find possible per-file graph parents.
    ///
    /// This is currently defined by:
    /// Select the last changed revision in the parent inventory.
    /// Do deal with a short lived bug in bzr 0.8's development two entries
    /// that have the same last changed but different 'x' bit settings are
    /// changed in-place.
    fn parent_candidates<'py>(
        &self,
        py: Python<'py>,
        previous_inventories: Vec<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyDict>> {
        // revision:ie mapping for each ie found in previous_inventories
        let mut candidates: HashMap<&RevisionId, Py<PyAny>> = HashMap::new();
        // identify candidate head revision ids
        for inv in previous_inventories {
            match inv.call_method1(py, "get_entry", (self.get_file_id(py)?,)) {
                Ok(py_entry) => {
                    if let Ok(mut entry) = py_entry.extract::<PyRefMut<InventoryEntry>>(py) {
                        if let Some(revision) = entry.0.revision() {
                            if let Some(candidate) = candidates.get_mut(revision) {
                                // same revision value in two different inventories:
                                // correct possible inconsistencies:
                                //  * there was a bug in revision updates with executable bit support
                                let mut candidate =
                                    candidate.extract::<PyRefMut<InventoryEntry>>(py)?;
                                if let (
                                    Entry::File {
                                        executable: candidate_executable,
                                        ..
                                    },
                                    Entry::File {
                                        executable: entry_executable,
                                        ..
                                    },
                                ) = (&mut candidate.0, &mut entry.0)
                                {
                                    if candidate_executable != entry_executable {
                                        *entry_executable = false;
                                        *candidate_executable = false;
                                    }
                                }
                            } else {
                                // add this revision as a candidate.
                                //candidates.insert(revision, py_entry);
                            }
                        }
                    }
                }
                Err(e) if e.is_instance_of::<NoSuchId>(py) => {}
                Err(e) => {
                    return Err(e);
                }
            }
        }
        let ret = PyDict::new(py);
        for (revision, entry) in candidates.into_iter() {
            ret.set_item(revision, entry)?;
        }
        Ok(ret)
    }
}

#[pyclass(subclass,extends=InventoryEntry)]
struct InventoryFile();

#[pymethods]
impl InventoryFile {
    #[new]
    #[pyo3(signature = (file_id, name, parent_id, revision=None, text_sha1=None, text_size=None, executable=None, text_id=None))]
    fn new(
        file_id: FileId,
        name: String,
        parent_id: FileId,
        revision: Option<RevisionId>,
        text_sha1: Option<Vec<u8>>,
        text_size: Option<u64>,
        executable: Option<bool>,
        text_id: Option<Vec<u8>>,
    ) -> PyResult<(Self, InventoryEntry)> {
        let executable = executable.unwrap_or(false);
        check_name(name.as_str())?;
        let entry = Entry::File {
            file_id,
            name,
            parent_id,
            revision,
            text_sha1,
            text_size,
            text_id,
            executable,
        };
        Ok((Self(), InventoryEntry(entry)))
    }

    #[getter]
    fn get_executable(slf: PyRef<Self>) -> bool {
        match slf.into_super().0 {
            Entry::File { executable, .. } => executable,
            _ => false,
        }
    }

    #[getter]
    fn get_text_sha1(slf: PyRef<Self>, py: Python) -> Option<Py<PyAny>> {
        let s = slf.into_super();
        match &s.0 {
            Entry::File { text_sha1, .. } => text_sha1
                .as_ref()
                .map(|text_sha1| PyBytes::new(py, text_sha1.as_ref()).into()),
            _ => panic!("Not a file"),
        }
    }

    #[getter]
    fn get_text_size(slf: PyRef<Self>) -> Option<u64> {
        let s = slf.into_super();
        match &s.0 {
            Entry::File { text_size, .. } => *text_size,
            _ => panic!("Not a file"),
        }
    }

    #[getter]
    fn get_text_id(slf: PyRef<Self>, py: Python) -> Option<Py<PyAny>> {
        let s = slf.into_super();
        match &s.0 {
            Entry::File { text_id, .. } => text_id
                .as_ref()
                .map(|text_id| PyBytes::new(py, text_id).into()),
            _ => panic!("Not a file"),
        }
    }

    #[getter]
    fn get_reference_revision(_slf: PyRef<Self>, py: Python) -> Py<PyAny> {
        py.None()
    }

    fn copy<'a>(slf: PyRef<'a, Self>, py: Python<'a>) -> PyResult<Bound<'a, InventoryFile>> {
        let s = slf.into_super();
        let init = PyClassInitializer::from(InventoryEntry(s.0.clone()));
        let init = init.add_subclass(Self());
        Bound::new(py, init)
    }

    fn __repr__(slf: PyRef<Self>, py: Python) -> PyResult<String> {
        let s = slf.into_super();
        Ok(match &s.0 {
            Entry::File {
                name,
                file_id,
                parent_id,
                text_sha1,
                text_size,
                revision,
                ..
            } => format!(
                "InventoryFile({}, {}, parent_id={}, sha1={}, len={}, revision={})",
                file_id.into_pyobject(py).unwrap().repr()?,
                name.into_pyobject(py).unwrap().repr()?,
                parent_id.into_pyobject(py).unwrap().repr()?,
                text_sha1
                    .as_ref()
                    .map(|s| PyBytes::new(py, s.as_slice()).repr())
                    .unwrap_or_else(|| Ok(PyString::new(py, "None")))?,
                text_size.into_pyobject(py).unwrap().repr()?,
                revision
                    .as_ref()
                    .map(|r| r.into_pyobject(py).unwrap())
                    .into_pyobject(py)
                    .unwrap()
                    .repr()?,
            ),
            _ => panic!("Not a file"),
        })
    }

    fn check(
        slf: &Bound<Self>,
        py: Python,
        checker: Py<PyAny>,
        rev_id: RevisionId,
        inv: Py<PyAny>,
    ) -> PyResult<()> {
        let spr = slf.borrow().into_super();
        common_ie_check(
            slf.clone().unbind().into(),
            &spr.0,
            py,
            &checker,
            &rev_id,
            inv,
        )?;

        let (file_id, revision, text_sha1, text_size) = match spr.0 {
            Entry::File {
                ref text_sha1,
                ref file_id,
                ref revision,
                text_size,
                ..
            } => (file_id, revision, text_sha1, text_size),
            _ => panic!("Not a file"),
        };

        checker.call_method1(
            py,
            "add_pending_item",
            (
                &rev_id,
                ("texts", &file_id, &revision),
                PyBytes::new(py, b"text"),
                PyBytes::new(py, text_sha1.as_ref().unwrap()),
            ),
        )?;

        if text_size.is_none() {
            checker.getattr(py, "_report_items")?.call_method1(
                py,
                "append",
                (format!(
                    "fileid {{{}}} in {{{}}} has None for text_size",
                    file_id, rev_id
                ),),
            )?;
        }

        Ok(())
    }
}

#[pyclass(subclass,extends=InventoryEntry)]
struct InventoryDirectory();

#[pymethods]
impl InventoryDirectory {
    #[new]
    #[pyo3(signature = (file_id, name, parent_id=None, revision=None))]
    fn new(
        file_id: FileId,
        name: String,
        parent_id: Option<FileId>,
        revision: Option<RevisionId>,
    ) -> PyResult<(Self, InventoryEntry)> {
        check_name(name.as_str())?;
        let entry = if let Some(parent_id) = parent_id {
            Entry::Directory {
                file_id,
                name,
                parent_id,
                revision,
            }
        } else {
            Entry::Root { file_id, revision }
        };
        Ok((Self(), InventoryEntry(entry)))
    }

    fn copy<'py>(slf: PyRef<Self>, py: Python<'py>) -> PyResult<Bound<'py, InventoryDirectory>> {
        let s = slf.into_super();
        let init = PyClassInitializer::from(InventoryEntry(s.0.clone()));
        let init = init.add_subclass(Self());
        Bound::new(py, init)
    }

    #[getter]
    fn get_text_size(&self, py: Python) -> Py<PyAny> {
        py.None()
    }

    #[getter]
    fn get_text_sha1(&self, py: Python) -> Py<PyAny> {
        py.None()
    }

    fn __repr__(slf: PyRef<Self>, py: Python) -> PyResult<String> {
        let s = slf.into_super();
        Ok(match &s.0 {
            Entry::Directory {
                name,
                file_id,
                parent_id,
                revision,
                ..
            } => format!(
                "InventoryDirectory({}, {}, parent_id={}, revision={})",
                file_id.into_pyobject(py).unwrap().repr()?,
                name.into_pyobject(py).unwrap().repr()?,
                parent_id.into_pyobject(py).unwrap().repr()?,
                revision.into_pyobject(py).unwrap().repr()?,
            ),
            Entry::Root {
                file_id, revision, ..
            } => format!(
                "InventoryDirectory({}, \"\", parent_id=None, revision={})",
                file_id.into_pyobject(py).unwrap().repr()?,
                revision.into_pyobject(py).unwrap().repr()?,
            ),
            _ => panic!("Not a directory"),
        })
    }

    fn check(
        slf: &Bound<Self>,
        py: Python,
        checker: Py<PyAny>,
        rev_id: RevisionId,
        inv: Py<PyAny>,
    ) -> PyResult<()> {
        let spr = slf.borrow().into_super();
        common_ie_check(
            slf.clone().unbind().into(),
            &spr.0,
            py,
            &checker,
            &rev_id,
            inv,
        )?;

        // In non rich root repositories we do not expect a file graph for the
        // root.
        if spr.0.name().is_empty() && !checker.getattr(py, "rich_roots")?.extract::<bool>(py)? {
            return Ok(());
        }
        // Directories are stored as an empty file, but the file should exist
        // to provide a per-fileid log. The hash of every directory content is
        // "da..." below (the sha1sum of '').
        checker.call_method1(
            py,
            "add_pending_item",
            (
                &rev_id,
                ("texts", spr.0.file_id(), spr.0.revision()),
                PyBytes::new(py, b"text"),
                PyBytes::new(py, b"da39a3ee5e6b4b0d3255bfef95601890afd80709"),
            ),
        )?;

        Ok(())
    }
}

#[pyclass(subclass,extends=InventoryEntry)]
struct TreeReference();

#[pymethods]
impl TreeReference {
    #[new]
    #[pyo3(signature = (file_id, name, parent_id, revision=None, reference_revision=None))]
    fn new(
        file_id: FileId,
        name: String,
        parent_id: FileId,
        revision: Option<RevisionId>,
        reference_revision: Option<RevisionId>,
    ) -> PyResult<(Self, InventoryEntry)> {
        check_name(name.as_str())?;
        let entry = Entry::TreeReference {
            file_id,
            name,
            parent_id,
            revision,
            reference_revision,
        };
        Ok((Self(), InventoryEntry(entry)))
    }

    #[getter]
    fn get_reference_revision<'a>(
        slf: PyRef<'a, Self>,
        py: Python<'a>,
    ) -> Option<Bound<'a, PyBytes>> {
        let s = slf.into_super();
        match &s.0 {
            Entry::TreeReference {
                reference_revision, ..
            } => reference_revision
                .as_ref()
                .map(|reference_revision| reference_revision.into_pyobject(py).unwrap()),
            _ => panic!("Not a tree reference"),
        }
    }

    fn copy<'py>(slf: PyRef<Self>, py: Python<'py>) -> PyResult<Bound<'py, TreeReference>> {
        let s = slf.into_super();
        let init = PyClassInitializer::from(InventoryEntry(s.0.clone()));
        let init = init.add_subclass(Self());
        Bound::new(py, init)
    }
}

#[pyclass(subclass,extends=InventoryEntry)]
struct InventoryLink();

#[pymethods]
impl InventoryLink {
    #[new]
    #[pyo3(signature = (file_id, name, parent_id, revision=None, symlink_target=None))]
    fn new(
        file_id: FileId,
        name: String,
        parent_id: FileId,
        revision: Option<RevisionId>,
        symlink_target: Option<String>,
    ) -> PyResult<(Self, InventoryEntry)> {
        check_name(name.as_str())?;
        let entry = Entry::Link {
            file_id,
            name,
            parent_id,
            symlink_target,
            revision,
        };
        Ok((Self(), InventoryEntry(entry)))
    }

    #[getter]
    fn get_symlink_target(slf: PyRef<Self>) -> Option<String> {
        let s = slf.into_super();
        match s.0 {
            Entry::Link {
                ref symlink_target, ..
            } => symlink_target.clone(),
            _ => panic!("Not a link"),
        }
    }

    fn copy<'py>(slf: PyRef<Self>, py: Python<'py>) -> PyResult<Bound<'py, InventoryLink>> {
        let s = slf.into_super();
        let init = PyClassInitializer::from(InventoryEntry(s.0.clone()));
        let init = init.add_subclass(Self());
        Bound::new(py, init)
    }

    #[getter]
    fn get_text_size(&self, py: Python) -> Py<PyAny> {
        py.None()
    }

    #[getter]
    fn get_text_sha1(&self, py: Python) -> Py<PyAny> {
        py.None()
    }

    fn check(
        slf: &Bound<Self>,
        py: Python,
        checker: Py<PyAny>,
        rev_id: RevisionId,
        inv: Py<PyAny>,
    ) -> PyResult<()> {
        let spr = slf.borrow().into_super();
        common_ie_check(
            slf.clone().unbind().into(),
            &spr.0,
            py,
            &checker,
            &rev_id,
            inv,
        )?;

        if spr.0.symlink_target().is_none() {
            let report_items = checker.getattr(py, "_report_items")?;
            report_items.call_method1(
                py,
                "append",
                (format!(
                    "symlink {} has no target in revision {}",
                    spr.0.file_id(),
                    spr.0
                        .revision()
                        .map_or_else(|| String::from("None"), |p| p.to_string())
                ),),
            )?;
        }

        // Symlinks are stored as ''
        checker.call_method1(
            py,
            "add_pending_item",
            (
                &rev_id,
                ("texts", spr.0.file_id(), spr.0.revision()),
                PyBytes::new(py, b"text"),
                PyBytes::new(py, b"da39a3ee5e6b4b0d3255bfef95601890afd80709"),
            ),
        )?;
        Ok(())
    }
}

fn entry_to_py(py: Python, e: Entry) -> PyResult<Bound<PyAny>> {
    let kind = e.kind();
    let init = PyClassInitializer::from(InventoryEntry(e));
    match kind {
        Kind::File => {
            let init = init.add_subclass(InventoryFile());
            Ok(Bound::new(py, init)?.into_any())
        }
        Kind::Directory => {
            let init = init.add_subclass(InventoryDirectory());
            Ok(Bound::new(py, init)?.into_any())
        }
        Kind::TreeReference => {
            let init = init.add_subclass(TreeReference());
            Ok(Bound::new(py, init)?.into_any())
        }
        Kind::Symlink => {
            let init = init.add_subclass(InventoryLink());
            Ok(Bound::new(py, init)?.into_any())
        }
    }
}

#[pyfunction]
#[allow(clippy::too_many_arguments)]
#[pyo3(signature = (kind, name, parent_id=None, revision=None, file_id=None, text_sha1=None, text_size=None, executable=None, text_id=None, symlink_target=None, reference_revision=None))]
fn make_entry<'a>(
    py: Python<'a>,
    kind: &'a str,
    name: &'a str,
    parent_id: Option<FileId>,
    revision: Option<RevisionId>,
    file_id: Option<FileId>,
    text_sha1: Option<Vec<u8>>,
    text_size: Option<u64>,
    executable: Option<bool>,
    text_id: Option<Vec<u8>>,
    symlink_target: Option<String>,
    reference_revision: Option<RevisionId>,
) -> PyResult<Bound<'a, PyAny>> {
    let kind = match kind {
        "file" => Kind::File,
        "directory" => Kind::Directory,
        "tree-reference" => Kind::TreeReference,
        "symlink" => Kind::Symlink,
        _ => panic!("Unknown kind"),
    };
    entry_to_py(
        py,
        bazaar::inventory::make_entry(
            kind,
            name.to_string(),
            file_id,
            parent_id,
            revision,
            text_sha1,
            text_size,
            executable,
            text_id,
            symlink_target,
            reference_revision,
        )
        .map_err(|e| inventory_err_to_py_err(e, py))?,
    )
}

#[pyfunction]
fn is_valid_name(name: &str) -> bool {
    bazaar::inventory::is_valid_name(name)
}

#[pyfunction]
fn ensure_normalized_name(name: std::path::PathBuf) -> PyResult<String> {
    let path = bazaar::inventory::ensure_normalized_name(name.as_path())
        .map_err(|_e| InvalidNormalization::new_err(name.clone()))?;

    path.to_str().map(|s| s.to_string()).ok_or_else(|| {
        PyValueError::new_err(format!(
            "Invalid normalization for path: {}",
            name.display()
        ))
    })
}

fn delta_err_to_py_err(py: Python, e: InventoryDeltaInconsistency) -> PyErr {
    match e {
        InventoryDeltaInconsistency::NoPath => {
            InconsistentDelta::new_err(("", "", "No path in entry"))
        }
        InventoryDeltaInconsistency::DuplicateFileId(ref path, ref fid) => {
            InconsistentDelta::new_err((path.clone(), fid.clone(), "repeated file_id"))
        }
        InventoryDeltaInconsistency::DuplicateOldPath(path, fid) => {
            InconsistentDelta::new_err((path, fid, "repeated path"))
        }
        InventoryDeltaInconsistency::DuplicateNewPath(path, fid) => {
            InconsistentDelta::new_err((path, fid, "repeated path"))
        }
        InventoryDeltaInconsistency::MismatchedId(path, fid1, fid2) => {
            InconsistentDelta::new_err((path, fid1, format!("mismatched id with entry {}", fid2)))
        }
        InventoryDeltaInconsistency::EntryWithoutPath(path, fid) => {
            InconsistentDelta::new_err((path, fid, "Entry with no new_path"))
        }
        InventoryDeltaInconsistency::PathWithoutEntry(path, fid) => {
            InconsistentDelta::new_err((path, fid, "new_path with no entry"))
        }
        InventoryDeltaInconsistency::OrphanedChild(fid) => {
            InconsistentDelta::new_err(("<deleted>", fid, "orphaned child"))
        }
        InventoryDeltaInconsistency::NoSuchId(fid) => NoSuchId::new_err((py.None(), fid)),
        InventoryDeltaInconsistency::PathMismatch(fid, path1, path2) => {
            InconsistentDelta::new_err((path1, fid, format!("path mismatch != {}", path2)))
        }
        InventoryDeltaInconsistency::ParentMissing(fid) => {
            InconsistentDelta::new_err(("", fid, "parent missing"))
        }
        InventoryDeltaInconsistency::InvalidEntryName(name) => InvalidEntryName::new_err((name,)),
        InventoryDeltaInconsistency::FileIdCycle(fid, path, parent_path) => {
            InconsistentDelta::new_err((path, fid, format!("file_id cycle with {}", parent_path)))
        }
        InventoryDeltaInconsistency::ParentNotDirectory(path, fid) => {
            InconsistentDelta::new_err((path, fid, "parent is not a directory"))
        }
        InventoryDeltaInconsistency::PathAlreadyVersioned(name, parent_path) => {
            InconsistentDelta::new_err((name, parent_path, "path already versioned"))
        }
    }
}

#[pyclass]
pub(crate) struct InventoryDelta(pub(crate) bazaar::inventory_delta::InventoryDelta);

#[pymethods]
impl InventoryDelta {
    #[new]
    #[allow(clippy::type_complexity)]
    #[pyo3(signature = (delta=None))]
    fn new(
        _py: Python,
        delta: Option<
            Vec<(
                Option<String>,
                Option<String>,
                FileId,
                Option<PyRef<InventoryEntry>>,
            )>,
        >,
    ) -> PyResult<Self> {
        let delta = delta.unwrap_or_default();
        let delta = delta
            .into_iter()
            .map(|(old_name, new_name, file_id, entry)| {
                let old_name = old_name.as_deref();
                let new_name = new_name.as_deref();
                let entry = entry.as_ref().map(|e| e.0.clone());
                InventoryDeltaEntry {
                    old_path: old_name.map(|s| s.to_string()),
                    new_path: new_name.map(|s| s.to_string()),
                    file_id,
                    new_entry: entry,
                }
            })
            .collect::<Vec<_>>();
        Ok(Self(bazaar::inventory_delta::InventoryDelta::from(delta)))
    }

    fn __nonzero__(slf: PyRef<Self>) -> bool {
        !slf.0.is_empty()
    }

    pub(crate) fn sort(&mut self) {
        self.0.sort();
    }

    fn __len__(&self) -> usize {
        self.0.len()
    }

    fn __richcmp__(&self, other: PyRef<InventoryDelta>, op: CompareOp) -> PyResult<Option<bool>> {
        match op {
            CompareOp::Eq => Ok(Some(self.0 == other.0)),
            CompareOp::Ne => Ok(Some(self.0 != other.0)),
            _ => Err(PyNotImplementedError::new_err(
                "Only == and != are supported",
            )),
        }
    }

    fn __getitem__<'a>(
        &self,
        py: Python<'a>,
        index: isize,
    ) -> PyResult<(Option<String>, Option<String>, FileId, Bound<'a, PyAny>)> {
        let index: usize = if index < 0 {
            (self.0.len() as isize + index) as usize
        } else {
            index as usize
        };
        let entry = self
            .0
            .get(index)
            .ok_or(PyIndexError::new_err("Index out of bounds"))?;
        Ok((
            entry.old_path.clone(),
            entry.new_path.clone(),
            entry.file_id.clone(),
            entry.new_entry.as_ref().map_or_else(
                || Ok(py.None().into_bound(py)),
                |e| entry_to_py(py, e.clone()),
            )?,
        ))
    }

    pub(crate) fn check(&self, py: Python) -> PyResult<()> {
        self.0.check().map_err(|e| match e {
            InventoryDeltaInconsistency::NoPath => {
                InconsistentDelta::new_err(("", "", "No path in entry"))
            }
            InventoryDeltaInconsistency::DuplicateFileId(ref path, ref fid) => {
                InconsistentDelta::new_err((path.clone(), fid.clone(), "repeated file_id"))
            }
            InventoryDeltaInconsistency::DuplicateOldPath(path, fid) => {
                InconsistentDelta::new_err((path, fid, "repeated path"))
            }
            InventoryDeltaInconsistency::DuplicateNewPath(path, fid) => {
                InconsistentDelta::new_err((path, fid, "repeated path"))
            }
            InventoryDeltaInconsistency::MismatchedId(path, fid1, fid2) => {
                InconsistentDelta::new_err((
                    path,
                    fid1,
                    format!("mismatched id with entry {}", fid2),
                ))
            }
            InventoryDeltaInconsistency::PathMismatch(fid, path1, path2) => {
                InconsistentDelta::new_err((
                    path1,
                    fid,
                    format!("mismatched path with entry {}", path2),
                ))
            }
            InventoryDeltaInconsistency::OrphanedChild(fid) => {
                InconsistentDelta::new_err(("", fid, "orphaned child"))
            }
            InventoryDeltaInconsistency::ParentNotDirectory(path, fid) => {
                InconsistentDelta::new_err((path, fid, "parent not directory"))
            }
            InventoryDeltaInconsistency::ParentMissing(fid) => {
                InconsistentDelta::new_err(("", fid, "parent missing"))
            }
            InventoryDeltaInconsistency::NoSuchId(fid) => NoSuchId::new_err((py.None(), fid)),
            InventoryDeltaInconsistency::InvalidEntryName(n) => InvalidEntryName::new_err((n,)),
            InventoryDeltaInconsistency::FileIdCycle(fid, path, parent_path) => {
                InconsistentDelta::new_err((
                    path,
                    fid,
                    format!("file_id cycle with {}", parent_path),
                ))
            }
            InventoryDeltaInconsistency::PathAlreadyVersioned(path, fid) => {
                InconsistentDelta::new_err((path, fid, "path already versioned"))
            }
            InventoryDeltaInconsistency::EntryWithoutPath(path, fid) => {
                InconsistentDelta::new_err((path, fid, "Entry with no new_path"))
            }
            InventoryDeltaInconsistency::PathWithoutEntry(path, fid) => {
                InconsistentDelta::new_err((path, fid, "new_path with no entry"))
            }
        })
    }

    fn __repr__(&self) -> String {
        format!("{:?}", self.0)
    }
}

fn inventory_err_to_py_err(e: Error, py: Python) -> PyErr {
    match e {
        Error::InvalidEntryName(name) => InvalidEntryName::new_err((name,)),
        Error::InvalidNormalization(n, _) => InvalidNormalization::new_err((n,)),
        Error::DuplicateFileId(fid, path) => DuplicateFileId::new_err((fid, path)),
        Error::NoSuchId(fid) => NoSuchId::new_err((py.None(), fid)),
        Error::ParentNotDirectory(path, fid) => {
            InconsistentDelta::new_err((path, fid, "parent not directory"))
        }
        Error::FileIdCycle(fid, path, parent_path) => {
            InconsistentDelta::new_err((path, fid, format!("file_id cycle with {}", parent_path)))
        }
        Error::ParentMissing(fid) => InconsistentDelta::new_err(("", fid, "parent missing")),
        Error::PathAlreadyVersioned(name, parent_path) => {
            AlreadyVersionedError::new_err(format!("{}/{}", parent_path, name))
        }
        Error::ParentNotVersioned(path) => {
            NotVersionedError::new_err(format!("parent not versioned: {}", path))
        }
        Error::Backend(msg) => BzrFormatsError::new_err(msg),
    }
}

/// Build a delta between two inventories of any shape by walking
/// `iter_all_ids()` on each side and comparing entries. Mirrors the
/// fallback branch of Python's `bzrformats.inventory._make_delta`.
///
/// Both `new` and `old` may be any object exposing `iter_all_ids()`,
/// `id2path(file_id)`, and `get_entry(file_id)` — i.e. an
/// `Inventory` or `CHKInventory` pyclass.
fn make_delta_via_attrs<'py>(
    new: &Bound<'py, PyAny>,
    old: &Bound<'py, PyAny>,
) -> PyResult<bazaar::inventory_delta::InventoryDelta> {
    let mut old_ids: std::collections::HashSet<FileId> = std::collections::HashSet::new();
    for fid in old.call_method0("iter_all_ids")?.try_iter()? {
        old_ids.insert(fid?.extract()?);
    }
    let mut new_ids: std::collections::HashSet<FileId> = std::collections::HashSet::new();
    for fid in new.call_method0("iter_all_ids")?.try_iter()? {
        new_ids.insert(fid?.extract()?);
    }
    let mut delta: Vec<InventoryDeltaEntry> = Vec::new();
    for file_id in old_ids.difference(&new_ids) {
        let old_path: String = old.call_method1("id2path", (file_id.clone(),))?.extract()?;
        delta.push(InventoryDeltaEntry {
            old_path: Some(old_path),
            new_path: None,
            file_id: file_id.clone(),
            new_entry: None,
        });
    }
    for file_id in new_ids.difference(&old_ids) {
        let new_path: String = new.call_method1("id2path", (file_id.clone(),))?.extract()?;
        let entry_obj = new.call_method1("get_entry", (file_id.clone(),))?;
        let entry = entry_obj.extract::<PyRef<InventoryEntry>>()?.0.clone();
        delta.push(InventoryDeltaEntry {
            old_path: None,
            new_path: Some(new_path),
            file_id: file_id.clone(),
            new_entry: Some(entry),
        });
    }
    for file_id in old_ids.intersection(&new_ids) {
        let old_entry_obj = old.call_method1("get_entry", (file_id.clone(),))?;
        let new_entry_obj = new.call_method1("get_entry", (file_id.clone(),))?;
        let old_entry = old_entry_obj.extract::<PyRef<InventoryEntry>>()?.0.clone();
        let new_entry = new_entry_obj.extract::<PyRef<InventoryEntry>>()?.0.clone();
        if old_entry != new_entry {
            let old_path: String = old.call_method1("id2path", (file_id.clone(),))?.extract()?;
            let new_path: String = new.call_method1("id2path", (file_id.clone(),))?.extract()?;
            delta.push(InventoryDeltaEntry {
                old_path: Some(old_path),
                new_path: Some(new_path),
                file_id: file_id.clone(),
                new_entry: Some(new_entry),
            });
        }
    }
    Ok(bazaar::inventory_delta::InventoryDelta::from(delta))
}

#[pyclass]
pub(crate) struct Inventory(pub(crate) bazaar::inventory::MutableInventory);

#[pymethods]
impl Inventory {
    #[new]
    #[pyo3(signature = (root_id=b"TREE_ROOT".to_vec(), revision_id=None, root_revision=None))]
    fn new(
        root_id: Option<Vec<u8>>,
        revision_id: Option<RevisionId>,
        root_revision: Option<RevisionId>,
    ) -> PyResult<Self> {
        let root_id = root_id.map(bazaar::FileId::from);
        let mut inv = Inventory(bazaar::inventory::MutableInventory::new());

        if let Some(root_id) = root_id {
            let root = bazaar::inventory::Entry::root(root_id, root_revision);
            inv.0.add(root).unwrap();
        } else if root_revision.is_some() {
            return Err(PyTypeError::new_err("root_revision requires root_id"));
        }
        inv.0.revision_id = revision_id;
        Ok(inv)
    }

    #[getter]
    fn root<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        if let Some(root) = self.0.root() {
            entry_to_py(py, root.clone())
        } else {
            Ok(py.None().into_bound(py))
        }
    }

    fn add(&mut self, py: Python, entry: &InventoryEntry) -> PyResult<()> {
        self.0
            .add(entry.0.clone())
            .map_err(|e| inventory_err_to_py_err(e, py))?;
        Ok(())
    }

    #[pyo3(signature = (relpath, kind, file_id=None, revision=None, text_sha1=None, text_size=None, executable=None, text_id=None, symlink_target=None, reference_revision=None))]
    fn add_path<'py>(
        &mut self,
        py: Python<'py>,
        relpath: &str,
        kind: bazaar::osutils::Kind,
        file_id: Option<FileId>,
        revision: Option<RevisionId>,
        text_sha1: Option<Vec<u8>>,
        text_size: Option<u64>,
        executable: Option<bool>,
        text_id: Option<Vec<u8>>,
        symlink_target: Option<String>,
        reference_revision: Option<RevisionId>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let file_id = self
            .0
            .add_path(
                relpath,
                kind,
                file_id,
                revision,
                text_sha1,
                text_size,
                executable,
                text_id,
                symlink_target,
                reference_revision,
            )
            .map_err(|e| inventory_err_to_py_err(e, py))?;
        self.get_entry(py, file_id)
    }

    #[getter]
    fn get_revision_id(&self) -> Option<RevisionId> {
        self.0.revision_id.as_ref().cloned()
    }

    #[setter]
    fn set_revision_id(&mut self, revision_id: Option<RevisionId>) {
        self.0.revision_id = revision_id;
    }

    fn id2path(&self, py: Python, file_id: FileId) -> PyResult<String> {
        self.0
            .id2path(&file_id)
            .map_err(|e| inventory_err_to_py_err(e, py))
    }

    fn path2id(&self, path: &str) -> Option<FileId> {
        self.0.path2id(path).cloned()
    }

    fn is_root(&self, file_id: FileId) -> PyResult<bool> {
        Ok(self.0.is_root(file_id))
    }

    fn has_filename(&self, py: Python, name: &str) -> PyResult<bool> {
        self.0
            .has_filename(name)
            .map_err(|e| inventory_err_to_py_err(e, py))
    }

    fn get_children<'py>(
        &self,
        py: Python<'py>,
        file_id: FileId,
    ) -> PyResult<HashMap<String, Bound<'py, PyAny>>> {
        let children = self.0.get_children(&file_id);
        if children.is_none() {
            return Err(NoSuchId::new_err((py.None(), file_id)));
        }
        let children = children.unwrap();
        let mut result = HashMap::with_capacity(children.len());
        for (name, child) in children {
            result.insert(name.to_string(), entry_to_py(py, child.clone())?);
        }
        Ok(result)
    }

    fn entries<'py>(&self, py: Python<'py>) -> PyResult<Vec<(String, Bound<'py, PyAny>)>> {
        let entries = self.0.entries();
        let mut result = Vec::with_capacity(entries.len());
        for (name, entry) in entries {
            result.push((name, entry_to_py(py, entry.clone())?));
        }
        Ok(result)
    }

    fn rename_id(&mut self, py: Python, old_file_id: FileId, new_file_id: FileId) -> PyResult<()> {
        self.0
            .rename_id(&old_file_id, &new_file_id)
            .map_err(|e| inventory_err_to_py_err(e, py))
    }

    fn path2id_segments(&self, names: Vec<String>) -> Option<FileId> {
        let names = names.iter().map(|s| s.as_str()).collect::<Vec<_>>();
        self.0.path2id_segments(names.as_slice()).cloned()
    }

    fn filter(&self, py: Python, specific_fileids: HashSet<FileId>) -> PyResult<Self> {
        let result = self
            .0
            .filter(&specific_fileids.iter().collect())
            .map_err(|e| inventory_err_to_py_err(e, py))?;
        Ok(Self(result))
    }

    fn get_entry_by_path_partial<'py>(
        &self,
        py: Python<'py>,
        relpath: Py<PyAny>,
    ) -> PyResult<(
        Option<Bound<'py, PyAny>>,
        Option<Vec<String>>,
        Option<Vec<String>>,
    )> {
        let ret = if let Ok(relpath) = relpath.extract::<String>(py) {
            self.0.get_entry_by_path_partial(&relpath)
        } else if let Ok(segments) = relpath.extract::<Vec<String>>(py) {
            let segments = segments.iter().map(|s| s.as_str()).collect::<Vec<_>>();
            self.0
                .get_entry_by_path_segments_partial(segments.as_slice())
        } else {
            return Err(PyTypeError::new_err("expected str or list of str"));
        };

        if let Some((e, segments, missing)) = ret {
            Ok((
                Some(entry_to_py(py, e.clone())?),
                Some(segments),
                Some(missing),
            ))
        } else {
            Ok((None, None, None))
        }
    }

    fn get_entry_by_path<'py>(
        &self,
        py: Python<'py>,
        relpath: Py<PyAny>,
    ) -> PyResult<Option<Bound<'py, PyAny>>> {
        if let Ok(relpath) = relpath.extract::<String>(py) {
            Ok(self
                .0
                .get_entry_by_path(&relpath)
                .map(|entry| entry_to_py(py, entry.clone()).unwrap()))
        } else if let Ok(segments) = relpath.extract::<Vec<String>>(py) {
            let segments = segments.iter().map(|s| s.as_str()).collect::<Vec<_>>();
            Ok(self
                .0
                .get_entry_by_path_segments(segments.as_slice())
                .map(|entry| entry_to_py(py, entry.clone()).unwrap()))
        } else {
            Err(PyTypeError::new_err("expected str or list of str"))
        }
    }

    #[pyo3(signature = (delta))]
    fn apply_delta(
        &mut self,
        py: Python,
        delta: Vec<(
            Option<String>,
            Option<String>,
            FileId,
            Option<PyRef<InventoryEntry>>,
        )>,
    ) -> PyResult<()> {
        let delta = bazaar::inventory_delta::InventoryDelta::from_iter(delta.into_iter().map(
            |(old_name, new_name, file_id, entry)| InventoryDeltaEntry {
                old_path: old_name,
                new_path: new_name,
                file_id,
                new_entry: entry.map(|entry| entry.0.clone()),
            },
        ));
        self.0
            .apply_delta(&delta)
            .map_err(|e| delta_err_to_py_err(py, e))
    }

    #[pyo3(signature = (delta, new_revision_id))]
    fn create_by_apply_delta(
        &self,
        py: Python,
        delta: Vec<(
            Option<String>,
            Option<String>,
            FileId,
            Option<PyRef<InventoryEntry>>,
        )>,
        new_revision_id: RevisionId,
    ) -> PyResult<Self> {
        let delta = bazaar::inventory_delta::InventoryDelta::from_iter(delta.into_iter().map(
            |(old_name, new_name, file_id, entry)| InventoryDeltaEntry {
                old_path: old_name,
                new_path: new_name,
                file_id,
                new_entry: entry.map(|entry| entry.0.clone()),
            },
        ));
        let result = self
            .0
            .create_by_apply_delta(&delta, new_revision_id)
            .map_err(|e| delta_err_to_py_err(py, e))?;
        Ok(Self(result))
    }

    fn __len__(&self) -> usize {
        self.0.len()
    }

    fn get_entry<'py>(&self, py: Python<'py>, file_id: FileId) -> PyResult<Bound<'py, PyAny>> {
        self.0
            .get_entry(&file_id)
            .map(|entry| entry_to_py(py, entry.clone()).unwrap())
            .ok_or_else(|| NoSuchId::new_err((py.None(), file_id)))
    }

    fn get_file_kind(&self, file_id: FileId) -> Option<&'static str> {
        self.0.get_file_kind(&file_id).map(|kind| kind.as_str())
    }

    fn has_id(&self, py: Python, file_id: FileId) -> PyResult<bool> {
        self.0
            .has_id(&file_id)
            .map_err(|e| inventory_err_to_py_err(e, py))
    }

    fn get_child<'py>(
        &self,
        py: Python<'py>,
        file_id: FileId,
        name: &str,
    ) -> Option<Bound<'py, PyAny>> {
        self.0
            .get_child(&file_id, name)
            .map(|entry| entry_to_py(py, entry.clone()).unwrap())
    }

    fn delete(&mut self, py: Python, file_id: FileId) -> PyResult<()> {
        self.0
            .delete(&file_id)
            .map_err(|e| inventory_err_to_py_err(e, py))
    }

    fn _make_delta<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        old: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, InventoryDelta>> {
        // Fast path: both inventories are the Rust-backed `Inventory`.
        if let Ok(old_inv) = old.extract::<PyRef<Inventory>>() {
            let this = slf.borrow();
            let inventory_delta = this.0.make_delta(&old_inv.0);
            return Bound::new(py, InventoryDelta(inventory_delta));
        }
        // Mixed Inventory<->CHKInventory: fall back to the generic
        // attribute-based diff.
        let delta = make_delta_via_attrs(slf.as_any(), old)?;
        Bound::new(py, InventoryDelta(delta))
    }

    fn remove_recursive_id<'a>(
        &mut self,
        py: Python<'a>,
        file_id: FileId,
    ) -> PyResult<Vec<Bound<'a, PyAny>>> {
        self.0
            .remove_recursive_id(&file_id)
            .into_iter()
            .map(|entry| entry_to_py(py, entry))
            .collect::<PyResult<Vec<_>>>()
    }

    fn rename(
        &mut self,
        py: Python,
        file_id: FileId,
        new_parent_id: FileId,
        new_name: &str,
    ) -> PyResult<()> {
        self.0
            .rename(&file_id, &new_parent_id, new_name)
            .map_err(|e| inventory_err_to_py_err(e, py))
    }

    fn iter_sorted_children(
        &self,
        py: Python<'_>,
        file_id: FileId,
    ) -> PyResult<Py<SortedChildrenIterator>> {
        let children = self.0.iter_sorted_children(&file_id);
        if children.is_none() {
            return Err(NoSuchId::new_err((py.None(), file_id)));
        }
        let entries = children.unwrap().map(|(_n, e)| e.clone()).collect();
        Py::new(py, SortedChildrenIterator { entries })
    }

    fn iter_all_ids(&self, py: Python<'_>) -> PyResult<Py<FileIdIterator>> {
        use bazaar::inventory::Inventory;
        let ids = self
            .0
            .all_file_ids()
            .map_err(|e| inventory_err_to_py_err(e, py))?;
        Py::new(py, FileIdIterator { ids: ids.into() })
    }

    #[pyo3(signature = (from_dir=None, recursive=true))]
    fn iter_entries(
        slf: Py<Inventory>,
        py: Python,
        from_dir: Option<FileId>,
        recursive: Option<bool>,
    ) -> PyResult<Bound<IterEntriesIterator>> {
        let recursive = recursive.unwrap_or(true);

        Bound::new(py, IterEntriesIterator::new(py, slf, from_dir, recursive)?)
    }

    #[pyo3(signature = (from_dir=None, specific_file_ids=None))]
    fn iter_entries_by_dir(
        slf: Py<Inventory>,
        py: Python,
        from_dir: Option<FileId>,
        specific_file_ids: Option<HashSet<FileId>>,
    ) -> PyResult<Bound<IterEntriesByDirIterator>> {
        Bound::new(
            py,
            IterEntriesByDirIterator::new(py, slf, from_dir, specific_file_ids)?,
        )
    }

    fn change_root_id(&mut self, new_root_id: FileId) -> PyResult<()> {
        self.0.change_root_id(new_root_id);
        Ok(())
    }

    fn copy(&self) -> Self {
        Self(self.0.clone())
    }

    #[pyo3(signature = (kind, name, parent_id=None, file_id=None, revision=None, text_sha1=None, text_size=None, text_id=None, executable=None, symlink_target=None, reference_revision=None))]
    #[allow(clippy::too_many_arguments)]
    fn make_entry<'a>(
        &self,
        py: Python<'a>,
        kind: &str,
        name: &str,
        parent_id: Option<FileId>,
        file_id: Option<FileId>,
        revision: Option<RevisionId>,
        text_sha1: Option<Vec<u8>>,
        text_size: Option<u64>,
        text_id: Option<Vec<u8>>,
        executable: Option<bool>,
        symlink_target: Option<String>,
        reference_revision: Option<RevisionId>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let kind = match kind {
            "directory" => Kind::Directory,
            "file" => Kind::File,
            "symlink" => Kind::Symlink,
            "tree-reference" => Kind::TreeReference,
            _ => return Err(PyValueError::new_err(format!("Unknown kind: {}", kind))),
        };
        let entry = bazaar::inventory::make_entry(
            kind,
            name.to_string(),
            parent_id,
            file_id,
            revision,
            text_sha1,
            text_size,
            executable,
            text_id,
            symlink_target,
            reference_revision,
        )
        .map_err(|e| inventory_err_to_py_err(e, py))?;
        entry_to_py(py, entry)
    }

    pub fn __richcmp__(&self, other: PyRef<Inventory>, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self.0 == other.0),
            CompareOp::Ne => Ok(self.0 != other.0),
            _ => Err(PyNotImplementedError::new_err(
                "Only == and != are implemented",
            )),
        }
    }
}

#[pyclass]
struct IterEntriesByDirIterator {
    inv: Py<Inventory>,
    parents: Option<HashSet<FileId>>,
    stack: Vec<(String, FileId)>,
    children: VecDeque<(String, Entry)>,
    specific_file_ids: Option<HashSet<FileId>>,
}

impl IterEntriesByDirIterator {
    fn new(
        py: Python,
        inv: Py<Inventory>,
        from_dir: Option<FileId>,
        specific_file_ids: Option<HashSet<FileId>>,
    ) -> PyResult<Self> {
        let parents = specific_file_ids.as_ref().map(|specific_file_ids| {
            bazaar::inventory::find_interesting_parents(
                &inv.borrow(py).0,
                &specific_file_ids.iter().collect(),
            )
            .into_iter()
            .cloned()
            .collect()
        });

        let mut stack: Vec<(String, FileId)> = vec![];
        let from_dir = if let Some(from_dir) = from_dir {
            let inv = &inv.borrow(py).0;
            let e = inv.get_entry(&from_dir);

            if e.is_none() {
                return Err(NoSuchId::new_err((py.None(), from_dir)));
            }

            let e = e.unwrap();

            if e.kind() != Kind::Directory {
                return Err(NotADirectory::new_err(from_dir));
            }
            Some(from_dir)
        } else {
            inv.borrow(py).0.root().map(|e| e.file_id().clone())
        };

        let mut children = VecDeque::new();

        if let Some(from_dir) = from_dir.as_ref() {
            assert!(
                inv.borrow(py).0.get_children(from_dir).is_some(),
                "from_dir {:?} must be a directory",
                from_dir
            );
            stack.push(("".to_string(), from_dir.clone()));
            if specific_file_ids.is_none() || specific_file_ids.as_ref().unwrap().contains(from_dir)
            {
                children.push_front((
                    "".to_string(),
                    inv.borrow(py).0.get_entry(from_dir).unwrap().clone(),
                ));
            }
        }

        Ok(Self {
            inv,
            parents,
            children,
            stack,
            specific_file_ids,
        })
    }
}

#[pymethods]
impl IterEntriesByDirIterator {
    fn __iter__(slf: PyRef<Self>) -> PyResult<Py<IterEntriesByDirIterator>> {
        Ok(slf.into())
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> PyResult<Option<(String, Bound<'py, PyAny>)>> {
        loop {
            if let Some((relpath, ie)) = self.children.pop_front() {
                return Ok(Some((relpath, entry_to_py(py, ie)?)));
            }
            if let Some((cur_relpath, cur_dir)) = self.stack.pop() {
                let mut child_dirs = Vec::new();
                let inv = &self.inv.borrow(py).0;
                for (child_name, child_ie) in inv
                    .iter_sorted_children(&cur_dir)
                    .expect("should be known directory")
                {
                    let child_relpath = cur_relpath.to_string() + child_name;

                    if self.specific_file_ids.is_none()
                        || self
                            .specific_file_ids
                            .as_ref()
                            .unwrap()
                            .contains(child_ie.file_id())
                    {
                        self.children
                            .push_back((child_relpath.clone(), child_ie.clone()));
                    }

                    if child_ie.kind() == Kind::Directory
                        && (self.parents.is_none()
                            || self.parents.as_ref().unwrap().contains(child_ie.file_id()))
                    {
                        assert!(self
                            .inv
                            .borrow(py)
                            .0
                            .get_children(child_ie.file_id())
                            .is_some());
                        child_dirs.push((child_relpath + "/", child_ie.file_id()))
                    }
                }
                self.stack
                    .extend(child_dirs.into_iter().rev().map(|(n, f)| (n, f.clone())));
            } else {
                return Ok(None);
            }
        }
    }
}

#[pyclass]
struct IterEntriesIterator {
    inv: Py<Inventory>,
    stack: VecDeque<(String, VecDeque<(String, Entry)>)>,
    recursive: bool,
    first_entry: Option<Entry>,
}

impl IterEntriesIterator {
    fn new(
        py: Python<'_>,
        inv: Py<Inventory>,
        mut from_dir: Option<FileId>,
        recursive: bool,
    ) -> PyResult<Self> {
        let mut stack = VecDeque::new();

        let first_entry = if from_dir.is_none() {
            from_dir = inv.borrow(py).0.root().map(|e| e.file_id().clone());
            inv.borrow(py).0.root().cloned()
        } else {
            None
        };

        if let Some(from_dir) = from_dir.as_ref() {
            let inv = &inv.borrow(py).0;
            let children = inv.iter_sorted_children(from_dir);
            if children.is_none() {
                return Err(NoSuchId::new_err((py.None(), from_dir.clone())));
            }
            stack.push_back((
                String::new(),
                children
                    .unwrap()
                    .map(|(p, ie)| (p.to_string(), ie.clone()))
                    .collect::<VecDeque<_>>(),
            ));
        }

        Ok(Self {
            inv,
            stack,
            recursive,
            first_entry,
        })
    }
}

#[pymethods]
impl IterEntriesIterator {
    fn __iter__(slf: PyRef<Self>) -> PyResult<Py<IterEntriesIterator>> {
        Ok(slf.into())
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> PyResult<Option<(String, Bound<'py, PyAny>)>> {
        if let Some(first_entry) = self.first_entry.take() {
            return Ok(Some((String::new(), entry_to_py(py, first_entry)?)));
        }
        loop {
            if let Some((base, children)) = self.stack.back_mut() {
                if let Some((name, ie)) = children.pop_front() {
                    let path = if base.is_empty() {
                        name
                    } else {
                        format!("{}/{}", base, name)
                    };
                    if ie.kind() == Kind::Directory && self.recursive {
                        let children = self
                            .inv
                            .borrow(py)
                            .0
                            .iter_sorted_children(ie.file_id())
                            .unwrap()
                            .map(|(p, ie)| (p.to_string(), ie.clone()))
                            .collect::<VecDeque<_>>();
                        self.stack.push_back((path.clone(), children));
                    }
                    return Ok(Some((path, entry_to_py(py, ie)?)));
                } else {
                    self.stack.pop_back();
                }
            } else {
                return Ok(None);
            }
        }
    }
}

/// Iterator returned by `Inventory.iter_sorted_children`. Holds the
/// sorted entries and constructs the Python `InventoryEntry` objects
/// one at a time.
#[pyclass]
struct SortedChildrenIterator {
    entries: VecDeque<Entry>,
}

#[pymethods]
impl SortedChildrenIterator {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        match self.entries.pop_front() {
            Some(e) => Ok(Some(entry_to_py(py, e)?)),
            None => Ok(None),
        }
    }
}

/// Iterator returned by `Inventory.iter_all_ids`, yielding file-ids.
#[pyclass]
struct FileIdIterator {
    ids: VecDeque<FileId>,
}

#[pymethods]
impl FileIdIterator {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        match self.ids.pop_front() {
            Some(id) => Ok(Some(id.into_pyobject(py)?.into_any().unbind())),
            None => Ok(None),
        }
    }
}

/// Iterator returned by `CHKInventory._iter_file_id_parents`. Walks
/// one entry up the parent chain per step, from `file_id` to the root.
#[pyclass]
struct FileIdParentsIter {
    inv: Py<CHKInventory>,
    cur: Option<Py<PyAny>>,
}

#[pymethods]
impl FileIdParentsIter {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        let Some(id) = self.cur.take() else {
            return Ok(None);
        };
        let id_bound = id.bind(py).clone();
        if id_bound.is_none() {
            return Ok(None);
        }
        let entry = self.inv.borrow(py).get_entry(py, id_bound)?;
        let parent = entry.getattr("parent_id")?;
        self.cur = if parent.is_none() {
            None
        } else {
            Some(parent.unbind())
        };
        Ok(Some(entry))
    }
}

/// Generic iterator over a pre-built Python list, yielding one element
/// per step. Used where the backing data is already materialised but
/// the public contract is an iterator.
#[pyclass]
struct ListIterator {
    list: Py<PyList>,
    index: usize,
}

impl ListIterator {
    fn new(list: Bound<'_, PyList>) -> Self {
        ListIterator {
            list: list.unbind(),
            index: 0,
        }
    }
}

#[pymethods]
impl ListIterator {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        let list = self.list.bind(py);
        if self.index >= list.len() {
            return Ok(None);
        }
        let item = list.get_item(self.index)?;
        self.index += 1;
        Ok(Some(item))
    }
}

/// Iterator returned by `UnversionedInventory.iter_all_ids`. Pulls one
/// `(key, value)` pair from the backing `id_to_entry.iteritems()` per
/// step and yields `key[-1]`.
#[pyclass]
struct AllIdsIterator {
    items: Py<PyAny>,
}

#[pymethods]
impl AllIdsIterator {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        let items = self.items.bind(py);
        let Some(pair) = items.try_iter()?.next() else {
            return Ok(None);
        };
        let tup = pair?.cast_into::<PyTuple>()?;
        let key_tup = tup.get_item(0)?.cast_into::<PyTuple>()?;
        Ok(Some(key_tup.get_item(key_tup.len() - 1)?))
    }
}

/// Iterator returned by `UnversionedInventory.iter_just_entries`. Pulls
/// one `(key, value)` pair from `id_to_entry.iteritems()` per step,
/// decoding the entry (and caching it) on demand.
#[pyclass]
struct JustEntriesIterator {
    items: Py<PyAny>,
    cache: Py<pyo3::types::PyDict>,
}

#[pymethods]
impl JustEntriesIterator {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyAny>>> {
        let items = self.items.bind(py);
        let Some(pair) = items.try_iter()?.next() else {
            return Ok(None);
        };
        let tup = pair?.cast_into::<PyTuple>()?;
        let key = tup.get_item(0)?;
        let value = tup.get_item(1)?;
        let file_id = key.cast_into::<PyTuple>()?.get_item(0)?;
        let cache = self.cache.bind(py);
        let entry = match cache.get_item(&file_id)? {
            Some(e) => e,
            None => {
                let bytes = value.cast_into::<PyBytes>()?;
                let e = chk_inventory_bytes_to_entry(py, bytes.as_bytes())?;
                cache.set_item(&file_id, &e)?;
                e
            }
        };
        Ok(Some(entry))
    }
}

#[pyfunction]
#[pyo3(signature = (lines, allow_versioned_root=None, allow_tree_references=None))]
fn parse_inventory_delta(
    py: Python,
    lines: Vec<Vec<u8>>,
    allow_versioned_root: Option<bool>,
    allow_tree_references: Option<bool>,
) -> PyResult<(
    Bound<PyBytes>,
    Bound<PyBytes>,
    bool,
    bool,
    Bound<InventoryDelta>,
)> {
    let (parent, version, versioned_root, tree_references, result) =
        bazaar::inventory_delta::parse_inventory_delta(
            lines
                .iter()
                .map(|x| x.as_slice())
                .collect::<Vec<_>>()
                .as_slice(),
            allow_versioned_root,
            allow_tree_references,
        )
        .map_err(|e| match e {
            InventoryDeltaParseError::Invalid(m) => InventoryDeltaError::new_err((m,)),
            InventoryDeltaParseError::Incompatible(m) => IncompatibleInventoryDelta::new_err((m,)),
        })?;

    let result = Bound::new(py, InventoryDelta(result))?;

    Ok((
        parent.into_pyobject(py)?,
        version.into_pyobject(py)?,
        versioned_root,
        tree_references,
        result,
    ))
}

#[pyfunction(signature = (file_id, name, parent_id, revision, lines))]
fn parse_inventory_entry(
    file_id: FileId,
    name: String,
    parent_id: Option<FileId>,
    revision: Option<RevisionId>,
    lines: &[u8],
) -> InventoryEntry {
    InventoryEntry(bazaar::inventory_delta::parse_inventory_entry(
        file_id, name, parent_id, revision, lines,
    ))
}

#[pyfunction]
fn serialize_inventory_entry<'a>(
    py: Python<'a>,
    entry: &'a InventoryEntry,
) -> PyResult<Bound<'a, PyBytes>> {
    Ok(PyBytes::new(
        py,
        bazaar::inventory_delta::serialize_inventory_entry(&entry.0)
            .map_err(|e| match e {
                InventoryDeltaSerializeError::Invalid(m) => InventoryDeltaError::new_err((m,)),
                InventoryDeltaSerializeError::UnsupportedKind(k) => PyKeyError::new_err((k,)),
            })?
            .as_slice(),
    ))
}

#[pyfunction]
fn serialize_inventory_delta<'a>(
    py: Python<'a>,
    old_name: RevisionId,
    new_name: RevisionId,
    delta_to_new: &'a InventoryDelta,
    versioned_root: bool,
    tree_references: bool,
) -> PyResult<Vec<Bound<'a, PyBytes>>> {
    Ok(bazaar::inventory_delta::serialize_inventory_delta(
        &old_name,
        &new_name,
        &delta_to_new.0,
        versioned_root,
        tree_references,
    )
    .map_err(|e| match e {
        InventoryDeltaSerializeError::Invalid(m) => InventoryDeltaError::new_err((m,)),
        InventoryDeltaSerializeError::UnsupportedKind(m) => PyKeyError::new_err((m,)),
    })?
    .into_iter()
    .map(|x| PyBytes::new(py, x.as_slice()))
    .collect())
}

/// Serialize inventory deltas. Ported from
/// `bzrformats.inventory_delta.InventoryDeltaSerializer`.
#[pyclass(
    name = "InventoryDeltaSerializer",
    module = "bzrformats._bzr_rs.inventory"
)]
struct PyInventoryDeltaSerializer {
    versioned_root: bool,
    tree_references: bool,
}

#[pymethods]
impl PyInventoryDeltaSerializer {
    #[new]
    fn new(versioned_root: bool, tree_references: bool) -> Self {
        Self {
            versioned_root,
            tree_references,
        }
    }

    /// Return a line sequence for `delta_to_new`.
    fn delta_to_lines<'a>(
        &self,
        py: Python<'a>,
        old_name: RevisionId,
        new_name: RevisionId,
        delta_to_new: &'a InventoryDelta,
    ) -> PyResult<Vec<Bound<'a, PyBytes>>> {
        serialize_inventory_delta(
            py,
            old_name,
            new_name,
            delta_to_new,
            self.versioned_root,
            self.tree_references,
        )
    }
}

/// Deserialize inventory deltas. Ported from
/// `bzrformats.inventory_delta.InventoryDeltaDeserializer`.
#[pyclass(
    name = "InventoryDeltaDeserializer",
    module = "bzrformats._bzr_rs.inventory"
)]
struct PyInventoryDeltaDeserializer {
    allow_versioned_root: bool,
    allow_tree_references: bool,
}

#[pymethods]
impl PyInventoryDeltaDeserializer {
    #[new]
    #[pyo3(signature = (allow_versioned_root=true, allow_tree_references=true))]
    fn new(allow_versioned_root: bool, allow_tree_references: bool) -> Self {
        Self {
            allow_versioned_root,
            allow_tree_references,
        }
    }

    /// Parse the text bytes of a serialized inventory delta, returning
    /// `(parent_id, new_id, versioned_root, tree_references, inventory_delta)`.
    fn parse_text_bytes<'a>(
        &self,
        py: Python<'a>,
        lines: Vec<Vec<u8>>,
    ) -> PyResult<(
        Bound<'a, PyBytes>,
        Bound<'a, PyBytes>,
        bool,
        bool,
        Bound<'a, InventoryDelta>,
    )> {
        parse_inventory_delta(
            py,
            lines,
            Some(self.allow_versioned_root),
            Some(self.allow_tree_references),
        )
    }
}

#[pyfunction]
fn chk_inventory_entry_to_bytes<'a>(
    py: Python<'a>,
    entry: &'a InventoryEntry,
) -> PyResult<Bound<'a, PyBytes>> {
    Ok(PyBytes::new(
        py,
        bazaar::chk_inventory::chk_inventory_entry_to_bytes(&entry.0).as_slice(),
    ))
}

#[pyfunction]
pub fn chk_inventory_bytes_to_entry<'py>(
    py: Python<'py>,
    data: &[u8],
) -> PyResult<Bound<'py, PyAny>> {
    entry_to_py(
        py,
        bazaar::chk_inventory::chk_inventory_bytes_to_entry(data),
    )
}

#[pyfunction]
fn chk_inventory_bytes_to_utf8name_key<'py>(
    py: Python<'py>,
    data: &[u8],
) -> PyResult<(Bound<'py, PyBytes>, FileId, RevisionId)> {
    let (name, file_id, revision_id) =
        bazaar::chk_inventory::chk_inventory_bytes_to_utf8_name_key(data);

    Ok((PyBytes::new(py, name), file_id, revision_id))
}

/// CHK-store-backed inventory.
///
/// State-only pyclass that mirrors Python's `CHKInventory` attributes:
/// the two CHKMaps (id_to_entry, parent_id_basename_to_file_id), the
/// configured search-key name, the revision and root ids, plus the
/// in-memory caches. Orchestration methods (get_entry, has_id, id2path,
/// path2id, get_children, get_child, iter_entries, etc.) are
/// monkey-patched on from `bzrformats/inventory.py`.
#[pyclass(
    module = "bzrformats._bzr_rs.inventory",
    name = "CHKInventory",
    subclass
)]
pub struct CHKInventory {
    search_key_name: Vec<u8>,
    root_id: Option<FileId>,
    revision_id: Option<RevisionId>,
    id_to_entry: Option<Py<PyAny>>,
    parent_id_basename_to_file_id: Option<Py<PyAny>>,
    fileid_to_entry_cache: Py<pyo3::types::PyDict>,
    fully_cached: bool,
    path_to_fileid_cache: Py<pyo3::types::PyDict>,
    children_cache: Py<pyo3::types::PyDict>,
}

#[pymethods]
impl CHKInventory {
    #[new]
    #[pyo3(signature = (search_key_name=None))]
    fn new(py: Python<'_>, search_key_name: Option<&[u8]>) -> PyResult<Self> {
        Ok(Self {
            // Default to b"plain" when called with None — matches the
            // Python CHKInventory(None) idiom used by test fixtures
            // that don't need a particular search-key variant.
            search_key_name: search_key_name.unwrap_or(b"plain").to_vec(),
            root_id: None,
            revision_id: None,
            id_to_entry: None,
            parent_id_basename_to_file_id: None,
            fileid_to_entry_cache: pyo3::types::PyDict::new(py).unbind(),
            fully_cached: false,
            path_to_fileid_cache: pyo3::types::PyDict::new(py).unbind(),
            children_cache: pyo3::types::PyDict::new(py).unbind(),
        })
    }

    #[getter]
    fn _search_key_name<'py>(&self, py: Python<'py>) -> Bound<'py, pyo3::types::PyBytes> {
        pyo3::types::PyBytes::new(py, &self.search_key_name)
    }

    #[setter(_search_key_name)]
    fn set_search_key_name(&mut self, value: &[u8]) {
        self.search_key_name = value.to_vec();
    }

    #[getter]
    fn root_id<'py>(&self, py: Python<'py>) -> Py<PyAny> {
        match &self.root_id {
            None => py.None(),
            Some(id) => pyo3::types::PyBytes::new(py, id.as_bytes())
                .into_any()
                .unbind(),
        }
    }

    #[setter]
    fn set_root_id(&mut self, value: Bound<'_, PyAny>) -> PyResult<()> {
        if value.is_none() {
            self.root_id = None;
        } else {
            let bytes = value.cast_into::<pyo3::types::PyBytes>()?;
            self.root_id = Some(FileId::from(bytes.as_bytes()));
        }
        Ok(())
    }

    #[getter]
    fn revision_id<'py>(&self, py: Python<'py>) -> Py<PyAny> {
        match &self.revision_id {
            None => py.None(),
            Some(id) => pyo3::types::PyBytes::new(py, id.as_bytes())
                .into_any()
                .unbind(),
        }
    }

    #[setter]
    fn set_revision_id(&mut self, value: Bound<'_, PyAny>) -> PyResult<()> {
        if value.is_none() {
            self.revision_id = None;
        } else {
            let bytes = value.cast_into::<pyo3::types::PyBytes>()?;
            self.revision_id = Some(RevisionId::from(bytes.as_bytes()));
        }
        Ok(())
    }

    #[getter]
    fn id_to_entry<'py>(&self, py: Python<'py>) -> Py<PyAny> {
        match &self.id_to_entry {
            None => py.None(),
            Some(m) => m.clone_ref(py),
        }
    }

    #[setter]
    fn set_id_to_entry(&mut self, value: Bound<'_, PyAny>) {
        if value.is_none() {
            self.id_to_entry = None;
        } else {
            self.id_to_entry = Some(value.unbind());
        }
    }

    #[getter]
    fn parent_id_basename_to_file_id<'py>(&self, py: Python<'py>) -> Py<PyAny> {
        match &self.parent_id_basename_to_file_id {
            None => py.None(),
            Some(m) => m.clone_ref(py),
        }
    }

    #[setter]
    fn set_parent_id_basename_to_file_id(&mut self, value: Bound<'_, PyAny>) {
        if value.is_none() {
            self.parent_id_basename_to_file_id = None;
        } else {
            self.parent_id_basename_to_file_id = Some(value.unbind());
        }
    }

    #[getter]
    fn _fileid_to_entry_cache<'py>(&self, py: Python<'py>) -> Bound<'py, pyo3::types::PyDict> {
        self.fileid_to_entry_cache.bind(py).clone()
    }

    #[setter(_fileid_to_entry_cache)]
    fn set_fileid_to_entry_cache(&mut self, value: Bound<'_, pyo3::types::PyDict>) {
        self.fileid_to_entry_cache = value.unbind();
    }

    #[getter]
    fn _fully_cached(&self) -> bool {
        self.fully_cached
    }

    #[setter(_fully_cached)]
    fn set_fully_cached(&mut self, value: bool) {
        self.fully_cached = value;
    }

    #[getter]
    fn _path_to_fileid_cache<'py>(&self, py: Python<'py>) -> Bound<'py, pyo3::types::PyDict> {
        self.path_to_fileid_cache.bind(py).clone()
    }

    #[setter(_path_to_fileid_cache)]
    fn set_path_to_fileid_cache(&mut self, value: Bound<'_, pyo3::types::PyDict>) {
        self.path_to_fileid_cache = value.unbind();
    }

    #[getter]
    fn _children_cache<'py>(&self, py: Python<'py>) -> Bound<'py, pyo3::types::PyDict> {
        self.children_cache.bind(py).clone()
    }

    #[setter(_children_cache)]
    fn set_children_cache(&mut self, value: Bound<'_, pyo3::types::PyDict>) {
        self.children_cache = value.unbind();
    }

    // ----- methods ported from bzrformats.inventory.CHKInventory -----

    /// Compare two CHKInventory instances by sha1 keys of their two
    /// underlying CHKMaps. Mirrors Python's `__eq__`.
    fn __eq__<'py>(&self, py: Python<'py>, other: Bound<'py, PyAny>) -> PyResult<bool> {
        // Only equal to another CHKInventory.
        let other_ref = match other.cast::<CHKInventory>() {
            Ok(o) => o,
            Err(_) => return Ok(false),
        };
        let other_borrow = other_ref.borrow();
        let (Some(self_id), Some(self_pid)) =
            (&self.id_to_entry, &self.parent_id_basename_to_file_id)
        else {
            return Ok(false);
        };
        let (Some(other_id), Some(other_pid)) = (
            &other_borrow.id_to_entry,
            &other_borrow.parent_id_basename_to_file_id,
        ) else {
            return Ok(false);
        };
        let this_key = self_id.bind(py).call_method0("key")?;
        let other_key = other_id.bind(py).call_method0("key")?;
        let this_pid_key = self_pid.bind(py).call_method0("key")?;
        let other_pid_key = other_pid.bind(py).call_method0("key")?;
        if this_key.is_none()
            || other_key.is_none()
            || this_pid_key.is_none()
            || other_pid_key.is_none()
        {
            return Ok(false);
        }
        Ok(this_key.eq(other_key)? && this_pid_key.eq(other_pid_key)?)
    }

    fn __len__(&self, py: Python<'_>) -> PyResult<usize> {
        let map = self
            .id_to_entry
            .as_ref()
            .ok_or_else(|| BzrFormatsError::new_err("id_to_entry not set"))?;
        map.bind(py).len()
    }

    /// True iff `file_id` matches the inventory's `root_id`. Mirrors
    /// Python's `is_root`. Accepts bytes or any equality-comparable
    /// object; non-matches return False.
    fn is_root(&self, py: Python<'_>, file_id: Bound<'_, PyAny>) -> PyResult<bool> {
        let root_id = match &self.root_id {
            None => return Ok(false),
            Some(id) => id,
        };
        if let Ok(b) = file_id.cast_into::<PyBytes>() {
            Ok(b.as_bytes() == root_id.as_bytes())
        } else {
            let _ = py;
            Ok(false)
        }
    }

    /// Check whether `file_id` exists in the inventory. Mirrors
    /// Python's `has_id`. Consults the cache first.
    fn has_id(&self, py: Python<'_>, file_id: Bound<'_, PyAny>) -> PyResult<bool> {
        if self.fileid_to_entry_cache.bind(py).contains(&file_id)? {
            return Ok(true);
        }
        // `file_id` must be bytes for the CHKMap lookup; non-bytes
        // returns False (matches the LeafNode filter behaviour).
        let Ok(bytes) = file_id.cast_into::<PyBytes>() else {
            return Ok(false);
        };
        let map = self
            .id_to_entry
            .as_ref()
            .ok_or_else(|| BzrFormatsError::new_err("id_to_entry not set"))?;
        let key_tuple = PyTuple::new(py, [bytes])?;
        let filter = PyList::new(py, [key_tuple])?;
        let items_iter = map.bind(py).call_method1("iteritems", (filter,))?;
        let items: Bound<'_, pyo3::types::PyList> = PyList::empty(py);
        for item in items_iter.try_iter()? {
            items.append(item?)?;
        }
        Ok(items.len() == 1)
    }

    /// True iff `filename` resolves to a file_id. Mirrors Python's
    /// `has_filename`. Dispatches through `path2id` (still Python-
    /// defined as of this commit; lifted in a later one).
    fn has_filename(slf: pyo3::Bound<'_, CHKInventory>, filename: &str) -> PyResult<bool> {
        let result = slf.call_method1("path2id", (filename,))?;
        Ok(!result.is_none())
    }

    /// Yield the parents of `file_id` up to the root. Mirrors
    /// Python's `_iter_file_id_parents` generator, walking one entry up
    /// the chain per step.
    fn _iter_file_id_parents(
        slf: Bound<'_, Self>,
        py: Python<'_>,
        file_id: Bound<'_, PyBytes>,
    ) -> PyResult<Py<FileIdParentsIter>> {
        Py::new(
            py,
            FileIdParentsIter {
                inv: slf.unbind(),
                cur: Some(file_id.into_any().unbind()),
            },
        )
    }

    /// Collect the parent chain of `file_id` up to the root as a list.
    /// Used by `id2path`, which needs random access and a length.
    fn file_id_parents_list<'py>(
        &self,
        py: Python<'py>,
        file_id: Bound<'_, PyBytes>,
    ) -> PyResult<Bound<'py, PyList>> {
        let out = PyList::empty(py);
        let mut cur: Option<Py<PyAny>> = Some(file_id.into_any().unbind());
        while let Some(id) = cur {
            let id_bound = id.bind(py).clone();
            if id_bound.is_none() {
                break;
            }
            let entry = self.get_entry(py, id_bound)?;
            cur = {
                let parent = entry.getattr("parent_id")?;
                if parent.is_none() {
                    None
                } else {
                    Some(parent.unbind())
                }
            };
            out.append(entry)?;
        }
        Ok(out)
    }

    /// Yield every file id stored in id_to_entry. Mirrors Python's
    /// `iter_all_ids` generator.
    fn iter_all_ids(&self, py: Python<'_>) -> PyResult<Py<AllIdsIterator>> {
        let map = self
            .id_to_entry
            .as_ref()
            .ok_or_else(|| BzrFormatsError::new_err("id_to_entry not set"))?;
        let items_iter = map.bind(py).call_method0("iteritems")?.try_iter()?;
        Py::new(
            py,
            AllIdsIterator {
                items: items_iter.into_any().unbind(),
            },
        )
    }

    /// Yield every entry in the inventory. Mirrors Python's
    /// `iter_just_entries`; populates the cache as it walks.
    fn iter_just_entries(&self, py: Python<'_>) -> PyResult<Py<JustEntriesIterator>> {
        let map = self
            .id_to_entry
            .as_ref()
            .ok_or_else(|| BzrFormatsError::new_err("id_to_entry not set"))?;
        let items_iter = map.bind(py).call_method0("iteritems")?.try_iter()?;
        Py::new(
            py,
            JustEntriesIterator {
                items: items_iter.into_any().unbind(),
                cache: self.fileid_to_entry_cache.clone_ref(py),
            },
        )
    }

    /// Look up an inventory entry by file id. Mirrors Python's
    /// `get_entry`. Raises NoSuchId for missing or non-bytes ids.
    fn get_entry<'py>(
        &self,
        py: Python<'py>,
        file_id: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if file_id.is_none() {
            return Err(NoSuchId::new_err((py.None(), py.None())));
        }
        let Ok(bytes) = file_id.clone().cast_into::<PyBytes>() else {
            return Err(NoSuchId::new_err((py.None(), file_id.unbind())));
        };
        if let Some(entry) = self.fileid_to_entry_cache.bind(py).get_item(&bytes)? {
            return Ok(entry);
        }
        let map = self
            .id_to_entry
            .as_ref()
            .ok_or_else(|| BzrFormatsError::new_err("id_to_entry not set"))?;
        let key_tuple = PyTuple::new(py, [&bytes])?;
        let filter = PyList::new(py, [key_tuple])?;
        let items_iter = map.bind(py).call_method1("iteritems", (filter,))?;
        let mut iter = items_iter.try_iter()?;
        let first = match iter.next() {
            None => return Err(NoSuchId::new_err((py.None(), bytes.unbind()))),
            Some(r) => r?,
        };
        let pair = first.cast_into::<PyTuple>()?;
        let value_bytes = pair.get_item(1)?.cast_into::<PyBytes>()?;
        self._bytes_to_entry(py, value_bytes)
    }

    /// Multi-id variant of get_entry. Mirrors Python's `_getitems`:
    /// silently omits missing ids; cache is filled for newly-loaded
    /// entries. Return order is undefined.
    fn _getitems<'py>(
        &self,
        py: Python<'py>,
        file_ids: Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        let out = PyList::empty(py);
        let mut remaining: Vec<Py<PyAny>> = Vec::new();
        let cache = self.fileid_to_entry_cache.bind(py);
        for fid in file_ids.try_iter()? {
            let fid = fid?;
            if let Some(entry) = cache.get_item(&fid)? {
                out.append(entry)?;
            } else {
                remaining.push(fid.unbind());
            }
        }
        if remaining.is_empty() {
            return Ok(out);
        }
        let file_keys = PyList::empty(py);
        for r in &remaining {
            let key_tuple = PyTuple::new(py, [r.bind(py).clone()])?;
            file_keys.append(key_tuple)?;
        }
        let map = self
            .id_to_entry
            .as_ref()
            .ok_or_else(|| BzrFormatsError::new_err("id_to_entry not set"))?;
        let items_iter = map.bind(py).call_method1("iteritems", (file_keys,))?;
        for pair in items_iter.try_iter()? {
            let pair = pair?;
            let tup = pair.cast_into::<PyTuple>()?;
            let value = tup.get_item(1)?.cast_into::<PyBytes>()?;
            let entry = self._bytes_to_entry(py, value)?;
            out.append(entry)?;
        }
        Ok(out)
    }

    /// Deserialise a serialised entry, caching it under its file_id.
    /// Mirrors Python's `_bytes_to_entry`.
    fn _bytes_to_entry<'py>(
        &self,
        py: Python<'py>,
        bytes: Bound<'_, PyBytes>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let entry = chk_inventory_bytes_to_entry(py, bytes.as_bytes())?;
        let file_id = entry.getattr("file_id")?;
        self.fileid_to_entry_cache
            .bind(py)
            .set_item(file_id, &entry)?;
        Ok(entry)
    }

    /// Produce an `InventoryDelta` from `old` to `self`. When `old` is
    /// another `CHKInventory`, the two `id_to_entry` CHKMaps are diffed
    /// via `iter_changes`; otherwise the generic attribute-based diff is
    /// used. Mirrors `bzrformats.inventory._make_delta`.
    fn _make_delta<'py>(
        slf: &Bound<'py, Self>,
        py: Python<'py>,
        old: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, InventoryDelta>> {
        if let Ok(old_chk) = old.cast::<CHKInventory>() {
            let self_id_map = slf
                .borrow()
                .id_to_entry
                .as_ref()
                .ok_or_else(|| BzrFormatsError::new_err("self.id_to_entry not set"))?
                .clone_ref(py);
            let basis_id_map = old_chk
                .borrow()
                .id_to_entry
                .as_ref()
                .ok_or_else(|| BzrFormatsError::new_err("old.id_to_entry not set"))?
                .clone_ref(py);
            let changes_iter = self_id_map
                .bind(py)
                .call_method1("iter_changes", (basis_id_map,))?;
            let mut delta: Vec<InventoryDeltaEntry> = Vec::new();
            let cache = slf.borrow().fileid_to_entry_cache.bind(py).clone();
            for change in changes_iter.try_iter()? {
                let change = change?;
                let tup = change.cast_into::<PyTuple>()?;
                let key = tup.get_item(0)?;
                let old_value = tup.get_item(1)?;
                let self_value = tup.get_item(2)?;
                let file_id_obj = key.cast_into::<PyTuple>()?.get_item(0)?;
                let file_id_bytes = file_id_obj.cast_into::<PyBytes>()?;
                let file_id = FileId::from(file_id_bytes.as_bytes());
                let old_path = if old_value.is_none() {
                    None
                } else {
                    Some(
                        old.call_method1("id2path", (file_id_bytes.clone(),))?
                            .extract::<String>()?,
                    )
                };
                let (new_path, new_entry) = if self_value.is_none() {
                    (None, None)
                } else {
                    let self_bytes = self_value.cast_into::<PyBytes>()?;
                    let entry =
                        bazaar::chk_inventory::chk_inventory_bytes_to_entry(self_bytes.as_bytes());
                    // Repopulate the cache the same way Python's
                    // `_bytes_to_entry` would have.
                    let py_entry = entry_to_py(py, entry.clone())?;
                    cache.set_item(file_id_bytes.clone(), py_entry)?;
                    let np = slf
                        .call_method1("id2path", (file_id_bytes,))?
                        .extract::<String>()?;
                    (Some(np), Some(entry))
                };
                delta.push(InventoryDeltaEntry {
                    old_path,
                    new_path,
                    file_id,
                    new_entry,
                });
            }
            return Bound::new(
                py,
                InventoryDelta(bazaar::inventory_delta::InventoryDelta::from(delta)),
            );
        }
        let delta = make_delta_via_attrs(slf.as_any(), old)?;
        Bound::new(py, InventoryDelta(delta))
    }

    /// Compute the `(parent_id, basename_utf8)` key used by the
    /// parent_id_basename_to_file_id index. Mirrors Python's
    /// `_parent_id_basename_key`.
    fn _parent_id_basename_key<'py>(
        &self,
        py: Python<'py>,
        entry: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let parent_id = entry.getattr("parent_id")?;
        let parent_bytes: Bound<'py, PyBytes> = if parent_id.is_none() {
            PyBytes::new(py, b"")
        } else {
            parent_id.cast_into::<PyBytes>()?
        };
        let name = entry.getattr("name")?;
        let name_str: String = name.extract()?;
        let name_bytes = PyBytes::new(py, name_str.as_bytes());
        PyTuple::new(py, [parent_bytes, name_bytes])
    }

    /// Always raises NotImplementedError. Mirrors Python's
    /// `get_idpath` placeholder.
    fn get_idpath(&self, _file_id: Bound<'_, PyAny>) -> PyResult<()> {
        Err(pyo3::exceptions::PyNotImplementedError::new_err(
            "get_idpath",
        ))
    }

    /// Get the root entry. Mirrors Python's `root` property.
    #[getter]
    fn root<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let root_id = self
            .root_id
            .as_ref()
            .ok_or_else(|| NoSuchId::new_err((py.None(), py.None())))?;
        let id_bytes = PyBytes::new(py, root_id.as_bytes());
        self.get_entry(py, id_bytes.into_any())
    }

    /// Return the slash-separated path to `file_id`. Mirrors
    /// Python's `id2path`. Raises NoSuchId if absent.
    fn id2path(&self, py: Python<'_>, file_id: Bound<'_, PyBytes>) -> PyResult<String> {
        let parents = self.file_id_parents_list(py, file_id)?;
        // Walk parents (child-to-root order), drop the root, reverse,
        // join with '/'.
        let mut segments: Vec<String> = Vec::with_capacity(parents.len());
        for p in parents.iter() {
            let name = p.getattr("name")?.extract::<String>()?;
            segments.push(name);
        }
        if !segments.is_empty() {
            segments.pop(); // drop the root's name ("")
        }
        segments.reverse();
        Ok(segments.join("/"))
    }

    /// Return the file_id at `relpath`, or `None` if not found.
    /// Mirrors Python's `path2id`. `relpath` can be a slash-separated
    /// string or a list of path components.
    fn path2id<'py>(&self, py: Python<'py>, relpath: Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        // Normalise `relpath` to (names: Vec<String>, joined: String).
        let (names, joined): (Vec<String>, String) =
            if let Ok(s) = relpath.clone().cast_into::<PyString>() {
                let s: String = s.extract()?;
                let names: Vec<String> = if s.is_empty() {
                    Vec::new()
                } else {
                    s.split('/').map(str::to_string).collect()
                };
                (names, s)
            } else {
                // list of basenames
                let mut names = Vec::new();
                for n in relpath.try_iter()? {
                    names.push(n?.extract::<String>()?);
                }
                let joined = if names.is_empty() {
                    String::new()
                } else {
                    names.join("/")
                };
                (names, joined)
            };
        // Cache lookup.
        let cache = self.path_to_fileid_cache.bind(py);
        let joined_bound = PyString::new(py, &joined);
        if let Some(cached) = cache.get_item(&joined_bound)? {
            return Ok(cached.unbind());
        }
        let mut current_id: Py<PyAny> = match &self.root_id {
            None => return Ok(py.None()),
            Some(id) => PyBytes::new(py, id.as_bytes()).into_any().unbind(),
        };
        let parent_id_index = self
            .parent_id_basename_to_file_id
            .as_ref()
            .ok_or_else(|| BzrFormatsError::new_err("parent_id_basename_to_file_id not set"))?;
        let mut cur_path: Option<String> = None;
        for basename in &names {
            cur_path = Some(match cur_path {
                None => basename.clone(),
                Some(p) => format!("{}/{}", p, basename),
            });
            let cp = cur_path.as_ref().unwrap();
            let cp_bound = PyString::new(py, cp);
            if let Some(cached) = cache.get_item(&cp_bound)? {
                current_id = cached.unbind();
                continue;
            }
            let basename_utf8 = PyBytes::new(py, basename.as_bytes());
            let key_tuple =
                PyTuple::new(py, [current_id.bind(py).clone(), basename_utf8.into_any()])?;
            let key_filter = PyList::new(py, [key_tuple])?;
            let items_iter = parent_id_index
                .bind(py)
                .call_method1("iteritems", (key_filter,))?;
            let mut file_id: Option<Py<PyAny>> = None;
            for pair in items_iter.try_iter()? {
                let pair = pair?;
                let tup = pair.cast_into::<PyTuple>()?;
                let key = tup.get_item(0)?.cast_into::<PyTuple>()?;
                let parent_id = key.get_item(0)?;
                let name_utf8 = key.get_item(1)?;
                if !parent_id.eq(current_id.bind(py))?
                    || !name_utf8.eq(PyBytes::new(py, basename.as_bytes()))?
                {
                    return Err(BzrFormatsError::new_err(format!(
                        "corrupt inventory lookup! {:?} {:?}",
                        parent_id, name_utf8,
                    )));
                }
                file_id = Some(tup.get_item(1)?.unbind());
            }
            let Some(fid) = file_id else {
                return Ok(py.None());
            };
            cache.set_item(&cp_bound, fid.bind(py).clone())?;
            current_id = fid;
        }
        Ok(current_id)
    }

    /// Children of `dir_id` as a `{name -> Entry}` Python dict.
    /// Mirrors Python's `get_children`. Caches the result.
    fn get_children<'py>(
        &self,
        py: Python<'py>,
        dir_id: Bound<'py, PyBytes>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let children_cache = self.children_cache.bind(py);
        if let Some(cached) = children_cache.get_item(&dir_id)? {
            return cached
                .cast_into::<PyDict>()
                .map_err(|e| pyo3::PyErr::from(e));
        }
        let parent_idx = self.parent_id_basename_to_file_id.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyAssertionError::new_err(
                "Inventories without parent_id_basename_to_file_id are no longer supported",
            )
        })?;
        let result = PyDict::new(py);
        // 1-element prefix filter yields just dir_id's children.
        let prefix_tuple = PyTuple::new(py, [&dir_id])?;
        let key_filter = PyList::new(py, [prefix_tuple])?;
        let items_iter = parent_idx
            .bind(py)
            .call_method1("iteritems", (key_filter,))?;
        let mut child_keys: Vec<Py<PyAny>> = Vec::new();
        for pair in items_iter.try_iter()? {
            let pair = pair?;
            let tup = pair.cast_into::<PyTuple>()?;
            let file_id = tup.get_item(1)?;
            child_keys.push(file_id.unbind());
        }
        let cache = self.fileid_to_entry_cache.bind(py);
        let mut remaining: Vec<Py<PyAny>> = Vec::new();
        for fid in &child_keys {
            if let Some(entry) = cache.get_item(fid.bind(py))? {
                let name = entry.getattr("name")?;
                result.set_item(name, entry)?;
            } else {
                remaining.push(fid.clone_ref(py));
            }
        }
        if !remaining.is_empty() {
            let id_to_entry = self
                .id_to_entry
                .as_ref()
                .ok_or_else(|| BzrFormatsError::new_err("id_to_entry not set"))?;
            let file_keys = PyList::empty(py);
            for fid in &remaining {
                let tup = PyTuple::new(py, [fid.bind(py).clone()])?;
                file_keys.append(tup)?;
            }
            let items_iter = id_to_entry
                .bind(py)
                .call_method1("iteritems", (file_keys,))?;
            for pair in items_iter.try_iter()? {
                let pair = pair?;
                let tup = pair.cast_into::<PyTuple>()?;
                let bytes_val = tup.get_item(1)?.cast_into::<PyBytes>()?;
                let entry = self._bytes_to_entry(py, bytes_val)?;
                let name = entry.getattr("name")?;
                result.set_item(name, entry)?;
            }
        }
        children_cache.set_item(&dir_id, &result)?;
        Ok(result)
    }

    /// Look up one child of `dir_id` by name. Mirrors Python's
    /// `get_child`. Returns None if not found.
    fn get_child<'py>(
        &self,
        py: Python<'py>,
        dir_id: Bound<'py, PyBytes>,
        name: Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let children = self.get_children(py, dir_id)?;
        match children.get_item(&name)? {
            Some(entry) => Ok(entry.unbind()),
            None => Ok(py.None()),
        }
    }

    /// Iterate children of `file_id` in lexicographic-name order.
    /// Mirrors Python's `iter_sorted_children` generator.
    fn iter_sorted_children<'py>(
        &self,
        py: Python<'py>,
        file_id: Bound<'py, PyBytes>,
    ) -> PyResult<Py<ListIterator>> {
        let list = self.sorted_children_list(py, file_id)?;
        Py::new(py, ListIterator::new(list))
    }

    /// Walk the inventory in lexicographic order. Mirrors Python's
    /// `iter_entries(from_dir, recursive)`. Returns an iterator
    /// yielding `(path, entry)` pairs.
    #[pyo3(signature = (from_dir=None, recursive=true))]
    fn iter_entries<'py>(
        slf: pyo3::Bound<'py, CHKInventory>,
        py: Python<'py>,
        from_dir: Option<Bound<'py, PyAny>>,
        recursive: bool,
    ) -> PyResult<Bound<'py, CHKIterEntriesIterator>> {
        let mut first: Option<(String, Py<PyAny>)> = None;
        let start_file_id: Py<PyAny> = match from_dir {
            None => {
                if slf.borrow().root_id.is_none() {
                    // Empty iterator.
                    return Bound::new(
                        py,
                        CHKIterEntriesIterator {
                            inv: slf.unbind(),
                            stack: Vec::new(),
                            recursive,
                            first: None,
                        },
                    );
                }
                let root = slf.getattr("root")?;
                let fid = root.getattr("file_id")?;
                first = Some((String::new(), root.unbind()));
                fid.unbind()
            }
            Some(fd) => {
                if let Ok(b) = fd.clone().cast_into::<PyBytes>() {
                    b.into_any().unbind()
                } else {
                    fd.getattr("file_id")?.unbind()
                }
            }
        };
        let start_bytes = start_file_id.bind(py).clone().cast_into::<PyBytes>()?;
        let direct = slf.borrow().sorted_children_list(py, start_bytes)?;
        let mut queue: std::collections::VecDeque<Py<PyAny>> = std::collections::VecDeque::new();
        for c in direct.iter() {
            queue.push_back(c.unbind());
        }
        let stack = vec![(String::new(), queue)];
        Bound::new(
            py,
            CHKIterEntriesIterator {
                inv: slf.unbind(),
                stack,
                recursive,
                first,
            },
        )
    }

    /// Return `[(path, entry)]` for every entry except the root.
    /// Mirrors Python's `entries`.
    fn entries<'py>(
        slf: pyo3::Bound<'py, CHKInventory>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyList>> {
        let accum = PyList::empty(py);
        if slf.borrow().root_id.is_none() {
            return Ok(accum);
        }
        let root = slf.getattr("root")?;
        // Iterative depth-first descent using osutils.pathjoin (which
        // is what Python's `entries` uses) — but for the CHKInventory
        // case paths are simple slash-joins, so just format here.
        let mut stack: Vec<(String, Py<PyAny>)> = vec![(String::new(), root.unbind())];
        while let Some((dir_path, dir_ie_py)) = stack.pop() {
            let dir_ie = dir_ie_py.bind(py);
            let fid = dir_ie.getattr("file_id")?.cast_into::<PyBytes>()?;
            let children = slf.borrow().sorted_children_list(py, fid)?;
            // Push child directories in reverse so they pop in order.
            let mut child_dirs: Vec<(String, Py<PyAny>)> = Vec::new();
            for ie in children.iter() {
                let name: String = ie.getattr("name")?.extract()?;
                let child_path = if dir_path.is_empty() {
                    name.clone()
                } else {
                    format!("{}/{}", dir_path, name)
                };
                accum.append(PyTuple::new(
                    py,
                    [PyString::new(py, &child_path).into_any(), ie.clone()],
                )?)?;
                let kind: String = ie.getattr("kind")?.extract()?;
                if kind == "directory" {
                    child_dirs.push((child_path, ie.unbind()));
                }
            }
            for cd in child_dirs.into_iter().rev() {
                stack.push(cd);
            }
        }
        Ok(accum)
    }

    /// Return the entry at `relpath` or None. Mirrors Python's
    /// `get_entry_by_path`.
    fn get_entry_by_path<'py>(
        slf: pyo3::Bound<'py, CHKInventory>,
        py: Python<'py>,
        relpath: Bound<'py, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let names = split_relpath(py, relpath)?;
        let parent = match slf.getattr("root") {
            Ok(r) => r,
            Err(e) if e.is_instance_of::<NoSuchId>(py) => return Ok(py.None()),
            Err(e) => return Err(e),
        };
        if parent.is_none() {
            return Ok(py.None());
        }
        let mut parent_py: Py<PyAny> = parent.unbind();
        for f in &names {
            let dir_id = parent_py
                .bind(py)
                .getattr("file_id")?
                .cast_into::<PyBytes>()?;
            let cie = slf
                .borrow()
                .get_child(py, dir_id, PyString::new(py, f).into_any())?;
            if cie.bind(py).is_none() {
                return Ok(py.None());
            }
            parent_py = cie;
        }
        Ok(parent_py)
    }

    /// Like `get_entry_by_path` but stops at the first tree
    /// reference. Returns `(entry, resolved, remaining)` or
    /// `(None, None, None)`. Mirrors Python's
    /// `get_entry_by_path_partial`.
    fn get_entry_by_path_partial<'py>(
        slf: pyo3::Bound<'py, CHKInventory>,
        py: Python<'py>,
        relpath: Bound<'py, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let names = split_relpath(py, relpath)?;
        let parent = match slf.getattr("root") {
            Ok(r) => r,
            Err(e) if e.is_instance_of::<NoSuchId>(py) => {
                let t = PyTuple::new(py, [py.None(), py.None(), py.None()])?;
                return Ok(t.into_any().unbind());
            }
            Err(e) => return Err(e),
        };
        if parent.is_none() {
            let t = PyTuple::new(py, [py.None(), py.None(), py.None()])?;
            return Ok(t.into_any().unbind());
        }
        let mut parent_py: Py<PyAny> = parent.unbind();
        for (i, f) in names.iter().enumerate() {
            let dir_id = parent_py
                .bind(py)
                .getattr("file_id")?
                .cast_into::<PyBytes>()?;
            let cie = slf
                .borrow()
                .get_child(py, dir_id, PyString::new(py, f).into_any())?;
            if cie.bind(py).is_none() {
                let t = PyTuple::new(py, [py.None(), py.None(), py.None()])?;
                return Ok(t.into_any().unbind());
            }
            let kind: String = cie.bind(py).getattr("kind")?.extract()?;
            if kind == "tree-reference" {
                let resolved: Vec<&str> = names[..=i].iter().map(String::as_str).collect();
                let remaining: Vec<&str> = names[i + 1..].iter().map(String::as_str).collect();
                let resolved_list = PyList::new(py, resolved)?;
                let remaining_list = PyList::new(py, remaining)?;
                let t = PyTuple::new(
                    py,
                    [
                        cie.bind(py).clone(),
                        resolved_list.into_any(),
                        remaining_list.into_any(),
                    ],
                )?;
                return Ok(t.into_any().unbind());
            }
            parent_py = cie;
        }
        let resolved_list = PyList::new(py, names.iter().map(String::as_str).collect::<Vec<_>>())?;
        let remaining_list = PyList::empty(py);
        let t = PyTuple::new(
            py,
            [
                parent_py.bind(py).clone(),
                resolved_list.into_any(),
                remaining_list.into_any(),
            ],
        )?;
        Ok(t.into_any().unbind())
    }

    /// Walk the inventory in directory-first order. Mirrors Python's
    /// `iter_entries_by_dir(from_dir, specific_file_ids)`. Returns an
    /// iterator yielding `(path, entry)` pairs.
    #[pyo3(signature = (from_dir=None, specific_file_ids=None))]
    fn iter_entries_by_dir<'py>(
        slf: pyo3::Bound<'py, CHKInventory>,
        py: Python<'py>,
        from_dir: Option<Bound<'py, PyAny>>,
        specific_file_ids: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, CHKIterEntriesByDirIterator>> {
        let specific_set: Option<std::collections::HashSet<Vec<u8>>> =
            if let Some(s) = specific_file_ids.as_ref() {
                let mut set = std::collections::HashSet::new();
                for fid in s.try_iter()? {
                    let fid = fid?;
                    if let Ok(b) = fid.cast_into::<PyBytes>() {
                        set.insert(b.as_bytes().to_vec());
                    }
                }
                Some(set)
            } else {
                None
            };
        if from_dir.is_none() && specific_file_ids.is_none() {
            slf.call_method0("_preload_cache")?;
        }
        let mut buffer: std::collections::VecDeque<(String, Py<PyAny>)> =
            std::collections::VecDeque::new();
        let mut stack: Vec<(String, Py<PyAny>)> = Vec::new();
        let from_entry: Option<Py<PyAny>> = if let Some(fd) = from_dir.clone() {
            let e = if let Ok(b) = fd.clone().cast_into::<PyBytes>() {
                slf.borrow().get_entry(py, b.into_any())?.unbind()
            } else {
                fd.unbind()
            };
            Some(e)
        } else {
            if let Some(set) = &specific_set {
                if set.len() == 1 {
                    let only = set.iter().next().unwrap().clone();
                    let bytes = PyBytes::new(py, &only);
                    match slf.call_method1("id2path", (&bytes,)) {
                        Ok(path) => {
                            if let Ok(entry) = slf.borrow().get_entry(py, bytes.into_any()) {
                                buffer.push_back((path.extract::<String>()?, entry.unbind()));
                            }
                        }
                        Err(e) if e.is_instance_of::<NoSuchId>(py) => {}
                        Err(e) => return Err(e),
                    }
                    return Bound::new(
                        py,
                        CHKIterEntriesByDirIterator {
                            inv: slf.unbind(),
                            buffer,
                            stack,
                            specific_set: None,
                            parents_filter: None,
                        },
                    );
                }
            }
            if slf.borrow().root_id.is_none() {
                return Bound::new(
                    py,
                    CHKIterEntriesByDirIterator {
                        inv: slf.unbind(),
                        buffer,
                        stack,
                        specific_set: None,
                        parents_filter: None,
                    },
                );
            }
            let root = slf.getattr("root")?;
            let root_fid: Vec<u8> = root
                .getattr("file_id")?
                .cast_into::<PyBytes>()?
                .as_bytes()
                .to_vec();
            if specific_set
                .as_ref()
                .map_or(true, |s| s.contains(&root_fid))
            {
                buffer.push_back((String::new(), root.clone().unbind()));
            }
            Some(root.unbind())
        };
        let parents_filter: Option<std::collections::HashSet<Vec<u8>>> = match &specific_set {
            None => None,
            Some(set) => {
                let mut ancestors: std::collections::HashSet<Vec<u8>> =
                    std::collections::HashSet::new();
                for fid in set {
                    let mut cur: Option<Vec<u8>> = Some(fid.clone());
                    while let Some(id) = cur {
                        let id_bytes = PyBytes::new(py, &id);
                        let has_id: bool = slf.borrow().has_id(py, id_bytes.clone().into_any())?;
                        if !has_id {
                            break;
                        }
                        let entry = slf.borrow().get_entry(py, id_bytes.into_any())?;
                        let parent_id = entry.getattr("parent_id")?;
                        let parent_bytes: Option<Vec<u8>> = if parent_id.is_none() {
                            None
                        } else {
                            Some(parent_id.cast_into::<PyBytes>()?.as_bytes().to_vec())
                        };
                        if let Some(pid) = &parent_bytes {
                            if ancestors.contains(pid) {
                                break;
                            }
                            ancestors.insert(pid.clone());
                        }
                        cur = parent_bytes;
                    }
                }
                Some(ancestors)
            }
        };
        if let Some(entry) = from_entry {
            stack.push((String::new(), entry));
        }
        Bound::new(
            py,
            CHKIterEntriesByDirIterator {
                inv: slf.unbind(),
                buffer,
                stack,
                specific_set,
                parents_filter,
            },
        )
    }

    /// Serialise the inventory header to lines. Mirrors Python's
    /// `to_lines`. The body (the two CHK maps) lives separately in
    /// the store and is referenced by sha1 key here.
    fn to_lines<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let lines = PyList::empty(py);
        lines.append(PyBytes::new(py, b"chkinventory:\n"))?;
        let root_id_bytes = self
            .root_id
            .as_ref()
            .ok_or_else(|| BzrFormatsError::new_err("root_id not set on CHKInventory"))?;
        let revision_id_bytes = self
            .revision_id
            .as_ref()
            .ok_or_else(|| BzrFormatsError::new_err("revision_id not set on CHKInventory"))?;
        let id_to_entry = self
            .id_to_entry
            .as_ref()
            .ok_or_else(|| BzrFormatsError::new_err("id_to_entry not set on CHKInventory"))?;
        // Extract sha1 keys from the CHKMap.key() tuples.
        let id_key_tuple = id_to_entry.bind(py).call_method0("key")?;
        let id_key_bytes = id_key_tuple
            .cast_into::<PyTuple>()?
            .get_item(0)?
            .cast_into::<PyBytes>()?
            .as_bytes()
            .to_vec();
        let pid_key_bytes: Option<Vec<u8>> = match &self.parent_id_basename_to_file_id {
            None => None,
            Some(pid) => {
                let t = pid.bind(py).call_method0("key")?;
                Some(
                    t.cast_into::<PyTuple>()?
                        .get_item(0)?
                        .cast_into::<PyBytes>()?
                        .as_bytes()
                        .to_vec(),
                )
            }
        };

        if self.search_key_name != b"plain" {
            // Custom ordering for non-plain serialisers.
            let mut buf = b"search_key_name: ".to_vec();
            buf.extend_from_slice(&self.search_key_name);
            buf.push(b'\n');
            lines.append(PyBytes::new(py, &buf))?;
            let mut buf = b"root_id: ".to_vec();
            buf.extend_from_slice(root_id_bytes.as_bytes());
            buf.push(b'\n');
            lines.append(PyBytes::new(py, &buf))?;
            // parent_id_basename_to_file_id is mandatory for non-plain.
            let pid = pid_key_bytes.as_deref().ok_or_else(|| {
                BzrFormatsError::new_err("parent_id_basename_to_file_id not set on CHKInventory")
            })?;
            let mut buf = b"parent_id_basename_to_file_id: ".to_vec();
            buf.extend_from_slice(pid);
            buf.push(b'\n');
            lines.append(PyBytes::new(py, &buf))?;
            let mut buf = b"revision_id: ".to_vec();
            buf.extend_from_slice(revision_id_bytes.as_bytes());
            buf.push(b'\n');
            lines.append(PyBytes::new(py, &buf))?;
            let mut buf = b"id_to_entry: ".to_vec();
            buf.extend_from_slice(&id_key_bytes);
            buf.push(b'\n');
            lines.append(PyBytes::new(py, &buf))?;
        } else {
            let mut buf = b"revision_id: ".to_vec();
            buf.extend_from_slice(revision_id_bytes.as_bytes());
            buf.push(b'\n');
            lines.append(PyBytes::new(py, &buf))?;
            let mut buf = b"root_id: ".to_vec();
            buf.extend_from_slice(root_id_bytes.as_bytes());
            buf.push(b'\n');
            lines.append(PyBytes::new(py, &buf))?;
            if let Some(pid) = &pid_key_bytes {
                let mut buf = b"parent_id_basename_to_file_id: ".to_vec();
                buf.extend_from_slice(pid);
                buf.push(b'\n');
                lines.append(PyBytes::new(py, &buf))?;
            }
            let mut buf = b"id_to_entry: ".to_vec();
            buf.extend_from_slice(&id_key_bytes);
            buf.push(b'\n');
            lines.append(PyBytes::new(py, &buf))?;
        }
        Ok(lines)
    }

    /// Deserialise inventory header bytes into a fresh CHKInventory.
    /// Mirrors Python's `CHKInventory.deserialise(chk_store, lines,
    /// expected_revision_id)`.
    #[classmethod]
    fn deserialise<'py>(
        cls: &Bound<'py, pyo3::types::PyType>,
        py: Python<'py>,
        chk_store: Bound<'_, PyAny>,
        lines: Bound<'_, PyAny>,
        expected_revision_id: Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Collect lines into a Vec<Vec<u8>>.
        let mut line_vec: Vec<Vec<u8>> = Vec::new();
        for line in lines.try_iter()? {
            let b = line?.cast_into::<PyBytes>()?;
            line_vec.push(b.as_bytes().to_vec());
        }
        if line_vec.is_empty() || !line_vec[line_vec.len() - 1].ends_with(b"\n") {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "last line should have trailing eol",
            ));
        }
        if line_vec[0] != b"chkinventory:\n" {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "not a serialised CHKInventory",
            ));
        }
        let allowed: &[&[u8]] = &[
            b"root_id",
            b"revision_id",
            b"parent_id_basename_to_file_id",
            b"search_key_name",
            b"id_to_entry",
        ];
        let mut info: std::collections::HashMap<Vec<u8>, Vec<u8>> =
            std::collections::HashMap::new();
        for line in &line_vec[1..] {
            let line = line.strip_suffix(b"\n").unwrap_or(line);
            let split_at = line.windows(2).position(|w| w == b": ").ok_or_else(|| {
                BzrFormatsError::new_err(format!("Inventory line missing ': ': {:?}", line))
            })?;
            let key = line[..split_at].to_vec();
            let value = line[split_at + 2..].to_vec();
            if !allowed.iter().any(|a| *a == &key[..]) {
                return Err(BzrFormatsError::new_err(format!(
                    "Unknown key in inventory: {:?}",
                    key
                )));
            }
            if info.contains_key(&key) {
                return Err(BzrFormatsError::new_err(format!(
                    "Duplicate key in inventory: {:?}",
                    key
                )));
            }
            info.insert(key, value);
        }
        let revision_id = info
            .remove(&b"revision_id"[..].to_vec())
            .ok_or_else(|| BzrFormatsError::new_err("missing revision_id"))?;
        let root_id = info
            .remove(&b"root_id"[..].to_vec())
            .ok_or_else(|| BzrFormatsError::new_err("missing root_id"))?;
        let search_key_name = info
            .remove(&b"search_key_name"[..].to_vec())
            .unwrap_or_else(|| b"plain".to_vec());
        let parent_id_basename_to_file_id =
            info.remove(&b"parent_id_basename_to_file_id"[..].to_vec());
        let id_to_entry = info
            .remove(&b"id_to_entry"[..].to_vec())
            .ok_or_else(|| BzrFormatsError::new_err("missing id_to_entry"))?;
        if let Some(pk) = &parent_id_basename_to_file_id {
            if !pk.starts_with(b"sha1:") {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "parent_id_basename_to_file_id should be a sha1 key not {:?}",
                    pk
                )));
            }
        }
        if !id_to_entry.starts_with(b"sha1:") {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "id_to_entry should be a sha1 key not {:?}",
                id_to_entry
            )));
        }
        // Verify the expected revision id matches.
        let expected_tup = expected_revision_id.cast_into::<PyTuple>()?;
        let expected_bytes = expected_tup
            .get_item(0)?
            .cast_into::<PyBytes>()?
            .as_bytes()
            .to_vec();
        if revision_id != expected_bytes {
            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Mismatched revision id and expected: {:?}, {:?}",
                revision_id, expected_bytes
            )));
        }
        let search_key_callable =
            crate::chk_map::search_key_callable_for_name(py, &search_key_name);
        // Build the result. The CHKInventory's id_to_entry/pid maps
        // are CHKMap pyclass instances wrapping the chk_store.
        let id_root_tuple = PyTuple::new(py, [PyBytes::new(py, &id_to_entry)])?;
        let id_chkmap = make_chkmap_pyinstance(
            py,
            &chk_store,
            id_root_tuple.into_any(),
            search_key_callable.as_ref().map(|c| c.bind(py).clone()),
        )?;
        let pid_chkmap = if let Some(pid_bytes) = parent_id_basename_to_file_id {
            let pid_tuple = PyTuple::new(py, [PyBytes::new(py, &pid_bytes)])?;
            Some(make_chkmap_pyinstance(
                py,
                &chk_store,
                pid_tuple.into_any(),
                search_key_callable.as_ref().map(|c| c.bind(py).clone()),
            )?)
        } else {
            None
        };
        // Construct via cls(search_key_name) so subclasses get a
        // subclass instance instead of a bare pyclass instance.
        let args = PyTuple::new(py, [PyBytes::new(py, &search_key_name)])?;
        let inv_obj = cls.call1(args)?;
        {
            let inv_cell = inv_obj.cast::<CHKInventory>()?;
            let mut inv = inv_cell.borrow_mut();
            inv.root_id = Some(FileId::from(root_id.as_slice()));
            inv.revision_id = Some(RevisionId::from(revision_id.as_slice()));
            inv.id_to_entry = Some(id_chkmap);
            inv.parent_id_basename_to_file_id = pid_chkmap;
        }
        Ok(inv_obj)
    }

    /// Bulk-create a CHKInventory from an existing inventory.
    /// Mirrors Python's `CHKInventory.from_inventory(chk_store,
    /// inventory, maximum_size=0, search_key_name=b"plain")`.
    #[classmethod]
    #[pyo3(signature = (chk_store, inventory, maximum_size=0, search_key_name=None))]
    fn from_inventory<'py>(
        cls: &Bound<'py, pyo3::types::PyType>,
        py: Python<'py>,
        chk_store: Bound<'py, PyAny>,
        inventory: Bound<'py, PyAny>,
        maximum_size: usize,
        search_key_name: Option<&[u8]>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let search_key_name = search_key_name.unwrap_or(b"plain");
        // Build the two seed dicts by walking inventory.iter_entries().
        let id_to_entry_dict = PyDict::new(py);
        let pid_dict = PyDict::new(py);
        let iter = inventory.call_method0("iter_entries")?;
        for pair in iter.try_iter()? {
            let pair = pair?;
            let tup = pair.cast_into::<PyTuple>()?;
            let entry = tup.get_item(1)?;
            let file_id = entry.getattr("file_id")?.cast_into::<PyBytes>()?;
            let key_tuple = PyTuple::new(py, [&file_id])?;
            // Serialise the entry to bytes.
            let entry_inner = entry.cast::<InventoryEntry>()?;
            let entry_borrow = entry_inner.borrow();
            let bytes_val = chk_inventory_entry_to_bytes(py, &entry_borrow)?;
            id_to_entry_dict.set_item(key_tuple, &bytes_val)?;
            // _parent_id_basename_key inline (we have static access).
            let parent_id = entry.getattr("parent_id")?;
            let parent_bytes: Bound<'py, PyBytes> = if parent_id.is_none() {
                PyBytes::new(py, b"")
            } else {
                parent_id.cast_into::<PyBytes>()?
            };
            let name: String = entry.getattr("name")?.extract()?;
            let name_bytes = PyBytes::new(py, name.as_bytes());
            let p_id_key = PyTuple::new(py, [parent_bytes, name_bytes])?;
            pid_dict.set_item(p_id_key, &file_id)?;
        }
        // Construct an empty inventory via cls(search_key_name) so
        // subclasses get a subclass instance.
        let root_id = inventory.getattr("root")?.getattr("file_id")?;
        let revision_id = inventory.getattr("revision_id")?;
        let args = PyTuple::new(py, [PyBytes::new(py, search_key_name)])?;
        let inv_obj = cls.call1(args)?;
        {
            let inv_cell = inv_obj.cast::<CHKInventory>()?;
            let mut inv = inv_cell.borrow_mut();
            inv.root_id = if root_id.is_none() {
                None
            } else {
                Some(FileId::from(root_id.cast_into::<PyBytes>()?.as_bytes()))
            };
            inv.revision_id = if revision_id.is_none() {
                None
            } else {
                Some(RevisionId::from(
                    revision_id.cast_into::<PyBytes>()?.as_bytes(),
                ))
            };
        }
        {
            let inv_cell = inv_obj.cast::<CHKInventory>()?;
            inv_cell.borrow_mut().populate_from_dicts(
                py,
                &chk_store,
                id_to_entry_dict.into_any(),
                pid_dict.into_any(),
                maximum_size,
            )?;
        }
        Ok(inv_obj)
    }

    /// Populate `id_to_entry` and `parent_id_basename_to_file_id`
    /// from two seed dicts via `CHKMap.from_dict`. Mirrors Python's
    /// `_populate_from_dicts`.
    #[pyo3(signature = (chk_store, id_to_entry_dict, parent_id_basename_dict, maximum_size))]
    fn _populate_from_dicts<'py>(
        &mut self,
        py: Python<'py>,
        chk_store: Bound<'_, PyAny>,
        id_to_entry_dict: Bound<'_, PyAny>,
        parent_id_basename_dict: Bound<'_, PyAny>,
        maximum_size: usize,
    ) -> PyResult<()> {
        self.populate_from_dicts(
            py,
            &chk_store,
            id_to_entry_dict,
            parent_id_basename_dict,
            maximum_size,
        )
    }

    /// Return an Inventory view filtered against `specific_fileids`.
    /// Children of directories and parents are included. Mirrors
    /// Python's `CHKInventory.filter`.
    fn filter<'py>(
        slf: Bound<'py, CHKInventory>,
        py: Python<'py>,
        specific_fileids: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, Inventory>> {
        let (interesting, parent_to_children) =
            CHKInventory::_expand_fileids_to_parents_and_children(
                slf.clone(),
                py,
                specific_fileids,
            )?;
        // Create the new (empty) Inventory; seed with the root.
        let inv_cls = py.get_type::<Inventory>();
        let inv_obj = inv_cls.call1((py.None(),))?;
        let other = inv_obj.cast::<Inventory>()?.clone();
        let root_attr = slf.getattr("root")?;
        let root_revision = root_attr.getattr("revision")?;
        let root_id_obj = match slf.borrow().root_id.as_ref() {
            Some(rid) => PyBytes::new(py, rid.as_bytes()).into_any(),
            None => py.None().into_bound(py),
        };
        let inv_dir_cls = py.get_type::<InventoryDirectory>();
        let root_dir = inv_dir_cls.call1((root_id_obj.clone(), "", py.None(), root_revision))?;
        other.call_method1("add", (root_dir,))?;
        let revision_id_obj = match slf.borrow().revision_id.as_ref() {
            Some(rev) => PyBytes::new(py, rev.as_bytes()).into_any(),
            None => py.None().into_bound(py),
        };
        other.setattr("revision_id", revision_id_obj)?;
        if interesting.is_empty() || parent_to_children.is_empty() {
            return Ok(other);
        }
        let cache = slf.borrow().fileid_to_entry_cache.clone_ref(py);
        // Seed deque with parent_to_children[root_id].
        let mut remaining: std::collections::VecDeque<Py<PyAny>> =
            std::collections::VecDeque::new();
        if let Some(root_children) = parent_to_children.get_item(&root_id_obj)? {
            for child in root_children.try_iter()? {
                remaining.push_back(child?.unbind());
            }
        }
        while let Some(file_id_obj) = remaining.pop_front() {
            let file_id = file_id_obj.bind(py);
            let ie = cache.bind(py).get_item(file_id)?.ok_or_else(|| {
                BzrFormatsError::new_err(format!("file_id {:?} not in fileid cache", file_id))
            })?;
            let kind: String = ie.getattr("kind")?.extract()?;
            let ie_to_add = if kind == "directory" {
                ie.call_method0("copy")?
            } else {
                ie
            };
            other.call_method1("add", (ie_to_add,))?;
            if let Some(children) = parent_to_children.get_item(file_id)? {
                for c in children.try_iter()? {
                    remaining.push_back(c?.unbind());
                }
            }
        }
        Ok(other)
    }

    /// Given a starting set of file_ids, return the set of all
    /// interesting file_ids plus a parent_id -> set-of-children
    /// dict. For directories in `file_ids`, all children (recursively)
    /// are included; ancestors of every input file_id are also
    /// included (but their other children are not). Mirrors Python's
    /// `_expand_fileids_to_parents_and_children`.
    fn _expand_fileids_to_parents_and_children<'py>(
        slf: Bound<'py, CHKInventory>,
        py: Python<'py>,
        file_ids: Bound<'py, PyAny>,
    ) -> PyResult<(Bound<'py, pyo3::types::PySet>, Bound<'py, PyDict>)> {
        // Collect file_ids into a Python set so we can do set
        // operations against it efficiently.
        let file_ids_set = pyo3::types::PySet::empty(py)?;
        for fid in file_ids.try_iter()? {
            file_ids_set.add(fid?)?;
        }
        let interesting = pyo3::types::PySet::empty(py)?;
        let mut directories_to_expand: Vec<Py<PyAny>> = Vec::new();
        let children_of_parent_id = PyDict::new(py);

        // First pass — _getitems(file_ids) gives entries (some may be
        // missing). Track directories to expand, and add each
        // entry's parent to `interesting`.
        let first_items = slf.call_method1("_getitems", (file_ids_set.clone(),))?;
        for entry in first_items.try_iter()? {
            let entry = entry?;
            let kind: String = entry.getattr("kind")?.extract()?;
            let file_id = entry.getattr("file_id")?;
            let parent_id = entry.getattr("parent_id")?;
            if kind == "directory" {
                directories_to_expand.push(file_id.clone().unbind());
            }
            interesting.add(parent_id.clone())?;
            match children_of_parent_id.get_item(&parent_id)? {
                Some(s) => {
                    s.cast_into::<pyo3::types::PySet>()?.add(file_id)?;
                }
                None => {
                    let new_set = pyo3::types::PySet::empty(py)?;
                    new_set.add(file_id)?;
                    children_of_parent_id.set_item(parent_id, new_set)?;
                }
            }
        }

        // Now climb parents until we reach the root. `None` is the
        // sentinel parent above the tree root — auto-filtered.
        let mut remaining_parents =
            interesting.call_method1("difference", (file_ids_set.clone(),))?;
        interesting.add(py.None())?;
        remaining_parents.call_method1("discard", (py.None(),))?;
        while remaining_parents.is_truthy()? {
            let next_parents = pyo3::types::PySet::empty(py)?;
            let items = slf.call_method1("_getitems", (remaining_parents.clone(),))?;
            for entry in items.try_iter()? {
                let entry = entry?;
                let file_id = entry.getattr("file_id")?;
                let parent_id = entry.getattr("parent_id")?;
                next_parents.add(parent_id.clone())?;
                match children_of_parent_id.get_item(&parent_id)? {
                    Some(s) => {
                        s.cast_into::<pyo3::types::PySet>()?.add(file_id)?;
                    }
                    None => {
                        let new_set = pyo3::types::PySet::empty(py)?;
                        new_set.add(file_id)?;
                        children_of_parent_id.set_item(parent_id, new_set)?;
                    }
                }
            }
            remaining_parents = next_parents.call_method1("difference", (interesting.clone(),))?;
            interesting.call_method1("update", (remaining_parents.clone(),))?;
        }
        interesting.call_method1("update", (file_ids_set.clone(),))?;
        interesting.call_method1("discard", (py.None(),))?;

        // Now expand any directories in `directories_to_expand` by
        // querying parent_id_basename_to_file_id.iteritems(keys).
        while !directories_to_expand.is_empty() {
            let keys = PyList::empty(py);
            for f in &directories_to_expand {
                keys.append(PyTuple::new(py, [f.bind(py)])?)?;
            }
            directories_to_expand.clear();
            let pid_map = slf
                .borrow()
                .parent_id_basename_to_file_id
                .as_ref()
                .ok_or_else(|| BzrFormatsError::new_err("parent_id_basename_to_file_id not set"))?
                .clone_ref(py);
            let items = pid_map.bind(py).call_method1("iteritems", (keys,))?;
            let next_file_ids = pyo3::types::PySet::empty(py)?;
            for item in items.try_iter()? {
                let item = item?;
                let tup = item.cast_into::<PyTuple>()?;
                let child_file_id = tup.get_item(1)?;
                next_file_ids.add(child_file_id)?;
            }
            let next_file_ids = next_file_ids.call_method1("difference", (interesting.clone(),))?;
            interesting.call_method1("update", (next_file_ids.clone(),))?;
            let items2 = slf.call_method1("_getitems", (next_file_ids,))?;
            for entry in items2.try_iter()? {
                let entry = entry?;
                let kind: String = entry.getattr("kind")?.extract()?;
                let file_id = entry.getattr("file_id")?;
                let parent_id = entry.getattr("parent_id")?;
                if kind == "directory" {
                    directories_to_expand.push(file_id.clone().unbind());
                }
                match children_of_parent_id.get_item(&parent_id)? {
                    Some(s) => {
                        s.cast_into::<pyo3::types::PySet>()?.add(file_id)?;
                    }
                    None => {
                        let new_set = pyo3::types::PySet::empty(py)?;
                        new_set.add(file_id)?;
                        children_of_parent_id.set_item(parent_id, new_set)?;
                    }
                }
            }
        }
        Ok((interesting, children_of_parent_id))
    }

    /// Populate the in-memory caches by walking the two CHKMaps.
    /// Mirrors Python's `_preload_cache`.
    ///
    /// After this returns, every entry is materialised in
    /// `_fileid_to_entry_cache`, and `_children_cache` is populated
    /// for every directory.
    fn _preload_cache<'py>(slf: Bound<'py, CHKInventory>, py: Python<'py>) -> PyResult<()> {
        if slf.borrow().fully_cached {
            return Ok(());
        }
        let id_map = slf
            .borrow()
            .id_to_entry
            .as_ref()
            .ok_or_else(|| BzrFormatsError::new_err("id_to_entry not set"))?
            .clone_ref(py);
        let pid_map_opt = slf
            .borrow()
            .parent_id_basename_to_file_id
            .as_ref()
            .map(|p| p.clone_ref(py));
        let fileid_cache: Py<PyDict> = slf.borrow().fileid_to_entry_cache.clone_ref(py);
        let children_cache: Py<PyDict> = slf.borrow().children_cache.clone_ref(py);
        let root_id_obj: Py<PyAny> = match slf.borrow().root_id.as_ref() {
            Some(rid) => PyBytes::new(py, rid.as_bytes()).into_any().unbind(),
            None => py.None(),
        };
        // Walk id_to_entry, populating fileid_cache.
        let id_iter = id_map.bind(py).call_method0("iteritems")?;
        for item in id_iter.try_iter()? {
            let item = item?;
            let pair = item.cast_into::<PyTuple>()?;
            let key = pair.get_item(0)?.cast_into::<PyTuple>()?;
            let file_id = key.get_item(0)?;
            let value = pair.get_item(1)?.cast_into::<PyBytes>()?;
            let cache = fileid_cache.bind(py);
            if !cache.contains(&file_id)? {
                let ie = slf.borrow()._bytes_to_entry(py, value)?;
                cache.set_item(file_id, ie)?;
            }
        }
        // Walk parent_id_basename_to_file_id, populating children_cache.
        if let Some(pid_map) = pid_map_opt {
            let mut last_parent_id: Option<Py<PyAny>> = None;
            let mut last_parent_ie: Option<Py<PyAny>> = None;
            let pid_iter = pid_map.bind(py).call_method0("iteritems")?;
            for item in pid_iter.try_iter()? {
                let item = item?;
                let pair = item.cast_into::<PyTuple>()?;
                let key = pair.get_item(0)?.cast_into::<PyTuple>()?;
                let child_file_id = pair.get_item(1)?;
                let parent_id = key.get_item(0)?;
                let basename_bytes = key.get_item(1)?.cast_into::<PyBytes>()?;
                let empty = PyBytes::new(py, b"");
                let parent_eq_empty = parent_id.as_any().eq(empty.as_any())?;
                let basename_eq_empty = basename_bytes.as_any().eq(empty.as_any())?;
                if parent_eq_empty && basename_eq_empty {
                    // Root entry — sanity-check matches root_id, skip.
                    if !child_file_id.eq(root_id_obj.bind(py))? {
                        return Err(pyo3::exceptions::PyValueError::new_err(format!(
                            "Data inconsistency detected. We expected data with key (\"\",\"\") to match the root id, but {:?} != {:?}",
                            child_file_id, root_id_obj
                        )));
                    }
                    continue;
                }
                let ie = fileid_cache
                    .bind(py)
                    .get_item(&child_file_id)?
                    .ok_or_else(|| {
                        BzrFormatsError::new_err(format!(
                            "child_file_id {:?} not in fileid cache",
                            child_file_id
                        ))
                    })?;
                let parent_ie = match &last_parent_id {
                    Some(lpid) if parent_id.eq(lpid.bind(py))? => last_parent_ie
                        .as_ref()
                        .ok_or_else(|| {
                            pyo3::exceptions::PyAssertionError::new_err(
                                "last_parent_ie should not be None",
                            )
                        })?
                        .clone_ref(py)
                        .into_bound(py),
                    _ => {
                        let pie = fileid_cache.bind(py).get_item(&parent_id)?.ok_or_else(|| {
                            BzrFormatsError::new_err(format!(
                                "parent_id {:?} not in fileid cache",
                                parent_id
                            ))
                        })?;
                        last_parent_id = Some(parent_id.clone().unbind());
                        last_parent_ie = Some(pie.clone().unbind());
                        pie
                    }
                };
                let parent_kind: String = parent_ie.getattr("kind")?.extract()?;
                if parent_kind != "directory" {
                    return Err(pyo3::exceptions::PyValueError::new_err(format!(
                        "Data inconsistency detected. An entry in the parent_id_basename_to_file_id map has parent_id {{{:?}}} but the kind of that object is {:?} not \"directory\"",
                        parent_id, parent_kind
                    )));
                }
                let parent_file_id = parent_ie.getattr("file_id")?;
                let siblings: Bound<'py, PyDict> =
                    match children_cache.bind(py).get_item(&parent_file_id)? {
                        Some(s) => s.cast_into::<PyDict>()?,
                        None => {
                            let d = PyDict::new(py);
                            children_cache.bind(py).set_item(&parent_file_id, &d)?;
                            d
                        }
                    };
                let basename: String = String::from_utf8(basename_bytes.as_bytes().to_vec())
                    .map_err(|e| {
                        pyo3::exceptions::PyValueError::new_err(format!(
                            "invalid utf8 basename: {}",
                            e
                        ))
                    })?;
                if siblings.contains(&basename)? {
                    if let Some(existing) = siblings.get_item(&basename)? {
                        if !existing.eq(&ie)? {
                            return Err(pyo3::exceptions::PyValueError::new_err(format!(
                                "Data inconsistency detected. Two entries with basename {:?} were found in the parent entry {{{:?}}}",
                                basename, parent_id
                            )));
                        }
                    }
                }
                let ie_name: String = ie.getattr("name")?.extract()?;
                if basename != ie_name {
                    return Err(pyo3::exceptions::PyValueError::new_err(format!(
                        "Data inconsistency detected. In the parent_id_basename_to_file_id map, file_id {{{:?}}} is listed as having basename {:?}, but in the id_to_entry map it is {:?}",
                        child_file_id, basename, ie_name
                    )));
                }
                siblings.set_item(basename, &ie)?;
            }
        }
        slf.borrow_mut().fully_cached = true;
        Ok(())
    }

    /// Generate a `Tree.iter_changes`-style change list between
    /// `self` and `basis`. Mirrors Python's `CHKInventory.iter_changes`.
    ///
    /// Returns a list of 8-tuples:
    ///   (file_id, (path_in_source, path_in_target),
    ///    changed_content, versioned, parent, name, kind, executable)
    fn iter_changes<'py>(
        slf: Bound<'py, CHKInventory>,
        py: Python<'py>,
        basis: Bound<'py, CHKInventory>,
    ) -> PyResult<Py<CHKIterChangesIterator>> {
        // Walk the CHKMap iter_changes generator on self.id_to_entry
        // vs basis.id_to_entry. We borrow both pyclass instances
        // immutably for attribute access.
        let self_id_map = slf
            .borrow()
            .id_to_entry
            .as_ref()
            .ok_or_else(|| BzrFormatsError::new_err("self.id_to_entry not set"))?
            .clone_ref(py);
        let basis_id_map = basis
            .borrow()
            .id_to_entry
            .as_ref()
            .ok_or_else(|| BzrFormatsError::new_err("basis.id_to_entry not set"))?
            .clone_ref(py);
        // CHKMap.iter_changes yields the raw changes (currently as a
        // list); take an iterator over it once so __next__ advances a
        // single cursor rather than restarting from the beginning.
        let changes_iter = self_id_map
            .bind(py)
            .call_method1("iter_changes", (basis_id_map,))?
            .try_iter()?;
        Py::new(
            py,
            CHKIterChangesIterator {
                slf: slf.unbind(),
                basis: basis.unbind(),
                changes: changes_iter.into_any().unbind(),
            },
        )
    }
    /// Apply `inventory_delta` to `self`, producing a new
    /// CHKInventory at `new_revision_id`. Mirrors Python's
    /// `CHKInventory.create_by_apply_delta`.
    #[pyo3(signature = (inventory_delta, new_revision_id, propagate_caches=false))]
    fn create_by_apply_delta<'py>(
        slf: Bound<'py, CHKInventory>,
        py: Python<'py>,
        inventory_delta: Bound<'py, PyAny>,
        new_revision_id: Bound<'py, PyAny>,
        propagate_caches: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Construct the result via cls(search_key_name) — same idea
        // as from_inventory: preserve subclass identity.
        let cls = slf.get_type();
        let search_key_name_bytes = PyBytes::new(py, &slf.borrow().search_key_name).into_any();
        let result_obj = cls.call1((search_key_name_bytes,))?;
        let result = result_obj.cast::<CHKInventory>()?.clone();
        if propagate_caches {
            let pf = slf
                .borrow()
                .path_to_fileid_cache
                .bind(py)
                .call_method0("copy")?
                .cast_into::<PyDict>()?;
            result.borrow_mut().path_to_fileid_cache = pf.unbind();
        }
        let search_key_callable =
            crate::chk_map::search_key_callable_for_name(py, &slf.borrow().search_key_name);
        // Snapshot id_to_entry: ensure root, capture maximum_size,
        // build a fresh CHKMap pointing at the same root key.
        let self_id_map = slf
            .borrow()
            .id_to_entry
            .as_ref()
            .ok_or_else(|| BzrFormatsError::new_err("id_to_entry not set"))?
            .clone_ref(py);
        self_id_map.bind(py).call_method0("_ensure_root")?;
        let maximum_size: usize = self_id_map
            .bind(py)
            .getattr("_root_node")?
            .getattr("maximum_size")?
            .extract()?;
        let chk_store = self_id_map.bind(py).getattr("_store")?;
        let self_id_key = self_id_map.bind(py).call_method0("key")?;
        let result_id_map = make_chkmap_pyinstance(
            py,
            &chk_store,
            self_id_key,
            search_key_callable.as_ref().map(|c| c.bind(py).clone()),
        )?;
        result_id_map.bind(py).call_method0("_ensure_root")?;
        result_id_map
            .bind(py)
            .getattr("_root_node")?
            .call_method1("set_maximum_size", (maximum_size,))?;
        result.borrow_mut().id_to_entry = Some(result_id_map);

        // parent_id_basename_to_file_id snapshot if present.
        let mut have_pid = false;
        if let Some(self_pid_map) = slf
            .borrow()
            .parent_id_basename_to_file_id
            .as_ref()
            .map(|p| p.clone_ref(py))
        {
            have_pid = true;
            self_pid_map.bind(py).call_method0("_ensure_root")?;
            let pid_store = self_pid_map.bind(py).getattr("_store")?;
            let pid_key = self_pid_map.bind(py).call_method0("key")?;
            let result_pid_map = make_chkmap_pyinstance(
                py,
                &pid_store,
                pid_key,
                search_key_callable.as_ref().map(|c| c.bind(py).clone()),
            )?;
            result_pid_map.bind(py).call_method0("_ensure_root")?;
            let self_root_node = self_pid_map.bind(py).getattr("_root_node")?;
            let result_root_node = result_pid_map.bind(py).getattr("_root_node")?;
            let max_pid_size: usize = self_root_node.getattr("maximum_size")?.extract()?;
            result_root_node.call_method1("set_maximum_size", (max_pid_size,))?;
            let key_width: usize = self_root_node.getattr("_key_width")?.extract()?;
            result_root_node.setattr("_key_width", key_width)?;
            result.borrow_mut().parent_id_basename_to_file_id = Some(result_pid_map);
        }

        // Set revision_id and root_id.
        result.setattr("revision_id", new_revision_id)?;
        let self_root_id_obj: Py<PyAny> = match slf.borrow().root_id.as_ref() {
            Some(rid) => PyBytes::new(py, rid.as_bytes()).into_any().unbind(),
            None => py.None(),
        };
        result.setattr("root_id", &self_root_id_obj)?;
        // Walk inventory_delta. Each item is (old_path, new_path,
        // file_id, entry). Track parent_id_basename_delta as a dict
        // (key -> [old_key, new_value]) so concurrent
        // moves-on-the-same-key collapse to a single record.
        inventory_delta.call_method0("check")?;
        let parents = pyo3::types::PySet::empty(py)?;
        let deletes = pyo3::types::PySet::empty(py)?;
        let altered = pyo3::types::PySet::empty(py)?;
        let parent_id_basename_delta = PyDict::new(py);
        let id_to_entry_delta = PyList::empty(py);
        // `osutils.split` is just os.path.split for str/bytes; inline as
        // "split at last '/'" to avoid round-tripping through Python.
        for change in inventory_delta.try_iter()? {
            let change = change?;
            let tup = change.cast_into::<PyTuple>()?;
            let old_path = tup.get_item(0)?;
            let new_path = tup.get_item(1)?;
            let file_id = tup.get_item(2)?;
            let entry = tup.get_item(3)?;
            // Detect new root.
            if !new_path.is_none() {
                let np: String = new_path.extract()?;
                if np.is_empty() {
                    result.setattr("root_id", &file_id)?;
                }
            }
            let (new_key, new_value): (Py<PyAny>, Py<PyAny>) = if new_path.is_none() {
                if propagate_caches {
                    let pf_cache = result.borrow().path_to_fileid_cache.clone_ref(py);
                    let _ = pf_cache.bind(py).del_item(&old_path);
                }
                deletes.add(&file_id)?;
                (py.None(), py.None())
            } else {
                let nk = PyTuple::new(py, [&file_id])?.into_any().unbind();
                let entry_inner = entry.cast::<InventoryEntry>()?.borrow();
                let nv = chk_inventory_entry_to_bytes(py, &entry_inner)?
                    .into_any()
                    .unbind();
                let pf_cache = result.borrow().path_to_fileid_cache.clone_ref(py);
                pf_cache.bind(py).set_item(&new_path, &file_id)?;
                let new_path_str: String = new_path.extract()?;
                let parent_part_str: &str = match new_path_str.rfind('/') {
                    Some(idx) => &new_path_str[..idx],
                    None => "",
                };
                let parent_part = parent_part_str.into_pyobject(py)?.into_any();
                let parent_id = entry.getattr("parent_id")?;
                parents.add(PyTuple::new(py, [parent_part, parent_id])?)?;
                (nk, nv)
            };
            let old_key: Py<PyAny> = if old_path.is_none() {
                py.None()
            } else {
                let ok = PyTuple::new(py, [&file_id])?.into_any().unbind();
                let id2path_self = slf.call_method1("id2path", (file_id.clone(),))?;
                if !id2path_self.eq(&old_path)? {
                    return Err(InconsistentDelta::new_err((
                        old_path.unbind(),
                        file_id.clone().unbind(),
                        format!("Entry was at wrong other path {:?}.", id2path_self),
                    )));
                }
                altered.add(&file_id)?;
                ok
            };
            id_to_entry_delta.append(PyTuple::new(
                py,
                [
                    old_key.bind(py).clone(),
                    new_key.bind(py).clone(),
                    new_value.bind(py).clone(),
                ],
            )?)?;
            if have_pid {
                // parent_id, basename changes
                let old_pid_key: Py<PyAny> = if old_path.is_none() {
                    py.None()
                } else {
                    let old_entry = slf.call_method1("get_entry", (file_id.clone(),))?;
                    slf.call_method1("_parent_id_basename_key", (old_entry,))?
                        .unbind()
                };
                let (new_pid_key, new_pid_value): (Py<PyAny>, Py<PyAny>) = if new_path.is_none() {
                    (py.None(), py.None())
                } else {
                    let nk = slf
                        .call_method1("_parent_id_basename_key", (entry.clone(),))?
                        .unbind();
                    (nk, file_id.clone().unbind())
                };
                if !old_pid_key.bind(py).eq(new_pid_key.bind(py))? {
                    if !old_pid_key.is_none(py) {
                        let entry_obj = parent_id_basename_delta
                            .get_item(old_pid_key.bind(py))?
                            .unwrap_or_else(|| {
                                PyList::new(py, [py.None(), py.None()]).unwrap().into_any()
                            });
                        entry_obj.set_item(0, old_pid_key.bind(py))?;
                        parent_id_basename_delta.set_item(old_pid_key.bind(py), entry_obj)?;
                    }
                    if !new_pid_key.is_none(py) {
                        let entry_obj = parent_id_basename_delta
                            .get_item(new_pid_key.bind(py))?
                            .unwrap_or_else(|| {
                                PyList::new(py, [py.None(), py.None()]).unwrap().into_any()
                            });
                        entry_obj.set_item(1, new_pid_value.bind(py))?;
                        parent_id_basename_delta.set_item(new_pid_key.bind(py), entry_obj)?;
                    }
                }
            }
        }
        // Validate that deletes are complete.
        for file_id in deletes.iter() {
            let entry = slf.call_method1("get_entry", (file_id.clone(),))?;
            let kind: String = entry.getattr("kind")?.extract()?;
            if kind != "directory" {
                continue;
            }
            let entry_file_id = entry.getattr("file_id")?;
            let children = slf.call_method1("iter_sorted_children", (entry_file_id,))?;
            for child in children.try_iter()? {
                let child = child?;
                let child_file_id = child.getattr("file_id")?;
                if !altered.contains(&child_file_id)? {
                    let child_path = slf.call_method1("id2path", (child_file_id.clone(),))?;
                    return Err(InconsistentDelta::new_err((
                        child_path.unbind(),
                        child_file_id.unbind(),
                        "Child not deleted or reparented when parent deleted.",
                    )));
                }
            }
        }
        // Apply id_to_entry delta.
        let result_id_map = result
            .borrow()
            .id_to_entry
            .as_ref()
            .ok_or_else(|| BzrFormatsError::new_err("result.id_to_entry not set"))?
            .clone_ref(py);
        result_id_map
            .bind(py)
            .call_method1("apply_delta", (id_to_entry_delta,))?;
        if !parent_id_basename_delta.is_empty() {
            let delta_list = PyList::empty(py);
            for (key, value_pair) in parent_id_basename_delta.iter() {
                let pair = value_pair.cast_into::<PyList>()?;
                let old_key = pair.get_item(0)?;
                let value = pair.get_item(1)?;
                if !value.is_none() {
                    delta_list.append(PyTuple::new(py, [old_key, key, value])?)?;
                } else {
                    delta_list.append(PyTuple::new(
                        py,
                        [old_key, py.None().into_bound(py), py.None().into_bound(py)],
                    )?)?;
                }
            }
            let result_pid_map = result
                .borrow()
                .parent_id_basename_to_file_id
                .as_ref()
                .ok_or_else(|| {
                    BzrFormatsError::new_err("result.parent_id_basename_to_file_id not set")
                })?
                .clone_ref(py);
            result_pid_map
                .bind(py)
                .call_method1("apply_delta", (delta_list,))?;
        }
        // Validate parent structure. Discard the synthetic
        // root tuple ("", None) which represents the root's parent.
        let empty_root_tup = PyTuple::new(
            py,
            ["".into_pyobject(py)?.into_any(), py.None().into_bound(py)],
        )?;
        parents.discard(&empty_root_tup)?;
        for pair in parents.iter() {
            let tup = pair.cast_into::<PyTuple>()?;
            let parent_path = tup.get_item(0)?;
            let parent = tup.get_item(1)?;
            match result.call_method1("get_entry", (parent.clone(),)) {
                Ok(entry) => {
                    let kind: String = entry.getattr("kind")?.extract()?;
                    if kind != "directory" {
                        let parent_inv_path = result.call_method1("id2path", (parent.clone(),))?;
                        return Err(InconsistentDelta::new_err((
                            parent_inv_path.unbind(),
                            parent.unbind(),
                            "Not a directory, but given children",
                        )));
                    }
                }
                Err(e) if e.is_instance_of::<NoSuchId>(py) => {
                    return Err(InconsistentDelta::new_err((
                        "<unknown>".to_string(),
                        parent.unbind(),
                        "Parent is not present in resulting inventory.",
                    )));
                }
                Err(e) => return Err(e),
            }
            let resolved = result.call_method1("path2id", (parent_path.clone(),))?;
            if !resolved.eq(&parent)? {
                return Err(InconsistentDelta::new_err((
                    parent_path.unbind(),
                    parent.unbind(),
                    format!("Parent has wrong path {:?}.", resolved),
                )));
            }
        }
        Ok(result_obj)
    }
}

/// Iterator returned by `CHKInventory.iter_changes`. Pulls one raw
/// change from the underlying CHKMap `iter_changes` per step, builds
/// the `tree.iter_changes`-shaped tuple, and skips entries that did
/// not actually change. Mirrors the Python generator.
#[pyclass]
struct CHKIterChangesIterator {
    slf: Py<CHKInventory>,
    basis: Py<CHKInventory>,
    changes: Py<PyAny>,
}

#[pymethods]
impl CHKIterChangesIterator {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyTuple>>> {
        let slf = self.slf.bind(py);
        let basis = self.basis.bind(py);
        let mut changes = self.changes.bind(py).try_iter()?;
        loop {
            let Some(change) = changes.next() else {
                return Ok(None);
            };
            let change = change?;
            let tup = change.cast_into::<PyTuple>()?;
            let key = tup.get_item(0)?;
            let basis_value = tup.get_item(1)?;
            let self_value = tup.get_item(2)?;
            let file_id = key.cast_into::<PyTuple>()?.get_item(0)?;
            let (basis_entry, path_in_source, basis_parent, basis_name, basis_executable) =
                if basis_value.is_none() {
                    (py.None(), py.None(), py.None(), py.None(), py.None())
                } else {
                    let bytes = basis_value.cast_into::<PyBytes>()?;
                    let entry = basis
                        .borrow()
                        ._bytes_to_entry(py, bytes)?
                        .into_pyobject(py)?;
                    let path: Py<PyAny> =
                        basis.call_method1("id2path", (file_id.clone(),))?.unbind();
                    let parent = entry.getattr("parent_id")?.unbind();
                    let name = entry.getattr("name")?.unbind();
                    let executable = entry.getattr("executable").ok().map_or(py.None(), |v| {
                        if v.is_none() {
                            py.None()
                        } else {
                            v.unbind()
                        }
                    });
                    (entry.unbind(), path, parent, name, executable)
                };
            let (self_entry, path_in_target, self_parent, self_name, self_executable) =
                if self_value.is_none() {
                    (py.None(), py.None(), py.None(), py.None(), py.None())
                } else {
                    let bytes = self_value.cast_into::<PyBytes>()?;
                    let entry = slf.borrow()._bytes_to_entry(py, bytes)?.into_pyobject(py)?;
                    let path: Py<PyAny> = slf.call_method1("id2path", (file_id.clone(),))?.unbind();
                    let parent = entry.getattr("parent_id")?.unbind();
                    let name = entry.getattr("name")?.unbind();
                    let executable = entry.getattr("executable").ok().map_or(py.None(), |v| {
                        if v.is_none() {
                            py.None()
                        } else {
                            v.unbind()
                        }
                    });
                    (entry.unbind(), path, parent, name, executable)
                };
            let (basis_kind, self_kind) = (
                if basis_entry.is_none(py) {
                    py.None()
                } else {
                    basis_entry.getattr(py, "kind")?
                },
                if self_entry.is_none(py) {
                    py.None()
                } else {
                    self_entry.getattr(py, "kind")?
                },
            );
            let versioned = (!basis_entry.is_none(py), !self_entry.is_none(py));
            let mut changed_content = !basis_kind.bind(py).eq(self_kind.bind(py))?;
            if !changed_content && !basis_entry.is_none(py) && !self_entry.is_none(py) {
                let kind_str: Option<String> = basis_kind.extract(py).ok();
                match kind_str.as_deref() {
                    Some("file") => {
                        let bs = basis_entry.getattr(py, "text_size")?;
                        let ss = self_entry.getattr(py, "text_size")?;
                        let bsha = basis_entry.getattr(py, "text_sha1")?;
                        let ssha = self_entry.getattr(py, "text_sha1")?;
                        if !bs.bind(py).eq(ss.bind(py))? || !bsha.bind(py).eq(ssha.bind(py))? {
                            changed_content = true;
                        }
                    }
                    Some("symlink") => {
                        let bt = basis_entry.getattr(py, "symlink_target")?;
                        let st = self_entry.getattr(py, "symlink_target")?;
                        if !bt.bind(py).eq(st.bind(py))? {
                            changed_content = true;
                        }
                    }
                    Some("tree-reference") => {
                        let br = basis_entry.getattr(py, "reference_revision")?;
                        let sr = self_entry.getattr(py, "reference_revision")?;
                        if !br.bind(py).eq(sr.bind(py))? {
                            changed_content = true;
                        }
                    }
                    _ => {}
                }
            }
            let parent_eq = basis_parent.bind(py).eq(self_parent.bind(py))?;
            let name_eq = basis_name.bind(py).eq(self_name.bind(py))?;
            let executable_eq = basis_executable.bind(py).eq(self_executable.bind(py))?;
            if !changed_content && parent_eq && name_eq && executable_eq {
                continue;
            }
            let paths_tup = PyTuple::new(py, [path_in_source, path_in_target])?;
            let versioned_tup = (versioned.0, versioned.1).into_pyobject(py)?;
            let parent_tup = PyTuple::new(py, [basis_parent, self_parent])?;
            let name_tup = PyTuple::new(py, [basis_name, self_name])?;
            let kind_tup = PyTuple::new(py, [basis_kind, self_kind])?;
            let executable_tup = PyTuple::new(py, [basis_executable, self_executable])?;
            let row = PyTuple::new(
                py,
                [
                    file_id.unbind(),
                    paths_tup.into_any().unbind(),
                    changed_content
                        .into_pyobject(py)?
                        .to_owned()
                        .into_any()
                        .unbind(),
                    versioned_tup.into_any().unbind(),
                    parent_tup.into_any().unbind(),
                    name_tup.into_any().unbind(),
                    kind_tup.into_any().unbind(),
                    executable_tup.into_any().unbind(),
                ],
            )?;
            return Ok(Some(row));
        }
    }
}

/// Construct a `_chk_map_rs.CHKMap` pyclass instance directly,
/// without going through the Python module attribute lookup.
fn make_chkmap_pyinstance<'py>(
    py: Python<'py>,
    chk_store: &Bound<'_, PyAny>,
    root_key: Bound<'_, PyAny>,
    search_key_callable: Option<Bound<'_, PyAny>>,
) -> PyResult<Py<PyAny>> {
    // The pyclass exposes a (#[new]) constructor we can call via
    // type lookup; constructing the struct directly here would mean
    // referencing private fields. Use the pyclass type bound to the
    // already-loaded module via py.get_type::<CHKMap>().
    let cls = py.get_type::<crate::chk_map::CHKMap>();
    let args = match search_key_callable {
        Some(cb) => PyTuple::new(py, [chk_store.clone(), root_key, cb])?,
        None => PyTuple::new(py, [chk_store.clone(), root_key])?,
    };
    Ok(cls.call1(args)?.unbind())
}

/// Iterator returned by `CHKInventory.iter_entries`. Yields
/// `(path, entry)` pairs in lexicographic order, descending into
/// directories when `recursive` is true. The synthetic root entry is
/// yielded first when iteration was started without a `from_dir`.
#[pyclass]
struct CHKIterEntriesIterator {
    inv: Py<CHKInventory>,
    stack: Vec<(String, std::collections::VecDeque<Py<PyAny>>)>,
    recursive: bool,
    first: Option<(String, Py<PyAny>)>,
}

#[pymethods]
impl CHKIterEntriesIterator {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> PyResult<Option<(String, Bound<'py, PyAny>)>> {
        if let Some((path, ie)) = self.first.take() {
            return Ok(Some((path, ie.into_bound(py))));
        }
        loop {
            let Some((path, children)) = self.stack.last_mut() else {
                return Ok(None);
            };
            let Some(ie_py) = children.pop_front() else {
                self.stack.pop();
                continue;
            };
            let ie = ie_py.into_bound(py);
            let name: String = ie.getattr("name")?.extract()?;
            let new_path = format!("{}/{}", path, name);
            let yield_path = new_path.trim_start_matches('/').to_string();
            let kind: String = ie.getattr("kind")?.extract()?;
            if self.recursive && kind == "directory" {
                let fid = ie.getattr("file_id")?.cast_into::<PyBytes>()?;
                let new_children = self.inv.bind(py).borrow().sorted_children_list(py, fid)?;
                let mut q: std::collections::VecDeque<Py<PyAny>> =
                    std::collections::VecDeque::new();
                for c in new_children.iter() {
                    q.push_back(c.unbind());
                }
                self.stack.push((new_path, q));
            }
            return Ok(Some((yield_path, ie)));
        }
    }
}

/// Iterator returned by `CHKInventory.iter_entries_by_dir`. Walks
/// the inventory directory-first, optionally restricted to
/// `specific_file_ids` (and their ancestors).
#[pyclass]
struct CHKIterEntriesByDirIterator {
    inv: Py<CHKInventory>,
    buffer: std::collections::VecDeque<(String, Py<PyAny>)>,
    stack: Vec<(String, Py<PyAny>)>,
    specific_set: Option<std::collections::HashSet<Vec<u8>>>,
    parents_filter: Option<std::collections::HashSet<Vec<u8>>>,
}

#[pymethods]
impl CHKIterEntriesByDirIterator {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> PyResult<Option<(String, Bound<'py, PyAny>)>> {
        loop {
            if let Some((path, ie)) = self.buffer.pop_front() {
                return Ok(Some((path, ie.into_bound(py))));
            }
            let Some((cur_relpath, cur_dir)) = self.stack.pop() else {
                return Ok(None);
            };
            let cur_dir_bound = cur_dir.into_bound(py);
            let cur_fid = cur_dir_bound.getattr("file_id")?.cast_into::<PyBytes>()?;
            let children = self
                .inv
                .bind(py)
                .borrow()
                .sorted_children_list(py, cur_fid)?;
            let mut child_dirs: Vec<(String, Py<PyAny>)> = Vec::new();
            for child in children.iter() {
                let child_name: String = child.getattr("name")?.extract()?;
                let child_relpath = format!("{}{}", cur_relpath, child_name);
                let child_fid: Vec<u8> = child
                    .getattr("file_id")?
                    .cast_into::<PyBytes>()?
                    .as_bytes()
                    .to_vec();
                if self
                    .specific_set
                    .as_ref()
                    .map_or(true, |s| s.contains(&child_fid))
                {
                    self.buffer
                        .push_back((child_relpath.clone(), child.clone().unbind()));
                }
                let kind: String = child.getattr("kind")?.extract()?;
                if kind == "directory" {
                    let recurse = match &self.parents_filter {
                        None => true,
                        Some(p) => p.contains(&child_fid),
                    };
                    if recurse {
                        child_dirs.push((format!("{}/", child_relpath), child.unbind()));
                    }
                }
            }
            for cd in child_dirs.into_iter().rev() {
                self.stack.push(cd);
            }
        }
    }
}

impl CHKInventory {
    /// Build the lexicographically-sorted list of a directory's
    /// children. Shared by the public `iter_sorted_children` iterator
    /// and the entry-walking iterators.
    fn sorted_children_list<'py>(
        &self,
        py: Python<'py>,
        file_id: Bound<'py, PyBytes>,
    ) -> PyResult<Bound<'py, PyList>> {
        let children = self.get_children(py, file_id)?;
        let mut pairs: Vec<(String, Py<PyAny>)> = Vec::new();
        for (k, v) in children.iter() {
            pairs.push((k.extract::<String>()?, v.unbind()));
        }
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        let out = PyList::empty(py);
        for (_, v) in pairs {
            out.append(v)?;
        }
        Ok(out)
    }

    /// Internal helper called by `from_inventory` and the public
    /// `_populate_from_dicts` method.
    fn populate_from_dicts<'py>(
        &mut self,
        py: Python<'py>,
        chk_store: &Bound<'_, PyAny>,
        id_to_entry_dict: Bound<'_, PyAny>,
        parent_id_basename_dict: Bound<'_, PyAny>,
        maximum_size: usize,
    ) -> PyResult<()> {
        let search_key_callable =
            crate::chk_map::search_key_callable_for_name(py, &self.search_key_name);
        // Get the CHKMap pyclass type and call its `from_dict`
        // classmethod for each of the two seed dicts.
        let chkmap_cls = py.get_type::<crate::chk_map::CHKMap>();
        let id_root_key = call_chkmap_from_dict(
            py,
            &chkmap_cls,
            chk_store,
            &id_to_entry_dict,
            maximum_size,
            1,
            search_key_callable.as_ref().map(|c| c.bind(py).clone()),
        )?;
        let id_chkmap = make_chkmap_pyinstance(
            py,
            chk_store,
            id_root_key,
            search_key_callable.as_ref().map(|c| c.bind(py).clone()),
        )?;
        self.id_to_entry = Some(id_chkmap);
        let pid_root_key = call_chkmap_from_dict(
            py,
            &chkmap_cls,
            chk_store,
            &parent_id_basename_dict,
            maximum_size,
            2,
            search_key_callable.as_ref().map(|c| c.bind(py).clone()),
        )?;
        let pid_chkmap = make_chkmap_pyinstance(
            py,
            chk_store,
            pid_root_key,
            search_key_callable.as_ref().map(|c| c.bind(py).clone()),
        )?;
        self.parent_id_basename_to_file_id = Some(pid_chkmap);
        Ok(())
    }
}

/// Call `CHKMap.from_dict(chk_store, items, maximum_size=N,
/// key_width=K, search_key_func=cb)` and return the resulting root
/// key tuple.
fn call_chkmap_from_dict<'py>(
    py: Python<'py>,
    chkmap_cls: &Bound<'py, pyo3::types::PyType>,
    chk_store: &Bound<'py, PyAny>,
    items: &Bound<'py, PyAny>,
    maximum_size: usize,
    key_width: usize,
    search_key_callable: Option<Bound<'py, PyAny>>,
) -> PyResult<Bound<'py, PyAny>> {
    let kwargs = PyDict::new(py);
    kwargs.set_item("maximum_size", maximum_size)?;
    kwargs.set_item("key_width", key_width)?;
    if let Some(cb) = search_key_callable {
        kwargs.set_item("search_key_func", cb)?;
    }
    let args = PyTuple::new(py, [chk_store.clone(), items.clone()])?;
    chkmap_cls.call_method("from_dict", args, Some(&kwargs))
}

/// Helper: split a relpath argument (string or list of components)
/// into a `Vec<String>`. Empty string yields an empty vec.
fn split_relpath(py: Python<'_>, relpath: Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    if let Ok(s) = relpath.clone().cast_into::<PyString>() {
        let s: String = s.extract()?;
        if s.is_empty() {
            Ok(Vec::new())
        } else {
            Ok(s.split('/').map(str::to_string).collect())
        }
    } else {
        let mut n = Vec::new();
        for x in relpath.try_iter()? {
            n.push(x?.extract::<String>()?);
        }
        let _ = py;
        Ok(n)
    }
}

pub fn _inventory_rs(py: Python) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "inventory")?;

    m.add_class::<InventoryEntry>()?;
    m.add_class::<InventoryFile>()?;
    m.add_class::<InventoryLink>()?;
    m.add_class::<InventoryDirectory>()?;
    m.add_class::<TreeReference>()?;
    m.add_wrapped(wrap_pyfunction!(make_entry))?;
    m.add_wrapped(wrap_pyfunction!(is_valid_name))?;
    m.add_wrapped(wrap_pyfunction!(ensure_normalized_name))?;
    m.add_class::<Inventory>()?;

    m.add_class::<InventoryDelta>()?;
    m.add_wrapped(wrap_pyfunction!(parse_inventory_delta))?;
    m.add_wrapped(wrap_pyfunction!(parse_inventory_entry))?;
    m.add_wrapped(wrap_pyfunction!(serialize_inventory_delta))?;
    m.add_wrapped(wrap_pyfunction!(serialize_inventory_entry))?;
    m.add_class::<PyInventoryDeltaSerializer>()?;
    m.add_class::<PyInventoryDeltaDeserializer>()?;
    m.add("InventoryDeltaError", py.get_type::<InventoryDeltaError>())?;
    m.add(
        "IncompatibleInventoryDelta",
        py.get_type::<IncompatibleInventoryDelta>(),
    )?;
    m.add_wrapped(wrap_pyfunction!(chk_inventory_entry_to_bytes))?;
    m.add_wrapped(wrap_pyfunction!(chk_inventory_bytes_to_entry))?;
    m.add_wrapped(wrap_pyfunction!(chk_inventory_bytes_to_utf8name_key))?;
    m.add_class::<CHKInventory>()?;
    m.add_class::<CHKIterEntriesIterator>()?;
    m.add_class::<CHKIterEntriesByDirIterator>()?;

    Ok(m)
}
