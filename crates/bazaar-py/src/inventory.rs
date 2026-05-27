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
import_exception!(bzrformats.errors, BzrCheckError);
import_exception!(bzrformats.errors, InvalidNormalization);
import_exception!(bzrformats.errors, InconsistentDelta);
import_exception!(bzrformats.errors, AlreadyVersionedError);
import_exception!(bzrformats.errors, BzrFormatsError);
import_exception!(bzrformats.errors, NotADirectory);
import_exception!(bzrformats.errors, NotVersionedError);
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
    }
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

    fn has_filename(&self, name: &str) -> PyResult<bool> {
        Ok(self.0.has_filename(name))
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

    fn has_id(&self, file_id: FileId) -> bool {
        self.0.has_id(&file_id)
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
    ) -> PyResult<Bound<'py, PyAny>> {
        // Fast path: both inventories are the Rust-backed `Inventory`.
        if let Ok(old_inv) = old.extract::<PyRef<Inventory>>() {
            let this = slf.borrow();
            let inventory_delta = this.0.make_delta(&old_inv.0);
            return Ok(Bound::new(py, InventoryDelta(inventory_delta))?.into_any());
        }
        // TODO: handle `CHKInventory` natively in Rust so we don't need the
        // Python round-trip. For now, fall back to the Python-side dispatcher
        // which knows how to produce a delta across mixed inventory types.
        py.import("bzrformats.inventory")?
            .getattr("_make_delta")?
            .call1((slf, old))
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

    fn iter_sorted_children<'a>(
        &self,
        py: Python<'a>,
        file_id: FileId,
    ) -> PyResult<Vec<Bound<'a, PyAny>>> {
        let children = self.0.iter_sorted_children(&file_id);
        if children.is_none() {
            return Err(NoSuchId::new_err((py.None(), file_id)));
        }
        children
            .unwrap()
            .map(|(_n, e)| Ok(entry_to_py(py, e.clone())?.into_any()))
            .collect::<PyResult<Vec<_>>>()
    }

    fn iter_all_ids<'a>(&self, py: Python<'a>) -> PyResult<Bound<'a, PyAny>> {
        let ids = self.0.iter_all_ids();
        ids.into_iter()
            .collect::<Vec<_>>()
            .into_pyobject(py)?
            .call_method0("__iter__")
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
#[pyclass(module = "bzrformats._bzr_rs.inventory", name = "CHKInventory", subclass)]
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

    #[setter]
    fn set__search_key_name(&mut self, value: &[u8]) {
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
    fn _fileid_to_entry_cache<'py>(
        &self,
        py: Python<'py>,
    ) -> Bound<'py, pyo3::types::PyDict> {
        self.fileid_to_entry_cache.bind(py).clone()
    }

    #[setter]
    fn set__fileid_to_entry_cache(
        &mut self,
        value: Bound<'_, pyo3::types::PyDict>,
    ) {
        self.fileid_to_entry_cache = value.unbind();
    }

    #[getter]
    fn _fully_cached(&self) -> bool {
        self.fully_cached
    }

    #[setter]
    fn set__fully_cached(&mut self, value: bool) {
        self.fully_cached = value;
    }

    #[getter]
    fn _path_to_fileid_cache<'py>(
        &self,
        py: Python<'py>,
    ) -> Bound<'py, pyo3::types::PyDict> {
        self.path_to_fileid_cache.bind(py).clone()
    }

    #[setter]
    fn set__path_to_fileid_cache(
        &mut self,
        value: Bound<'_, pyo3::types::PyDict>,
    ) {
        self.path_to_fileid_cache = value.unbind();
    }

    #[getter]
    fn _children_cache<'py>(
        &self,
        py: Python<'py>,
    ) -> Bound<'py, pyo3::types::PyDict> {
        self.children_cache.bind(py).clone()
    }

    #[setter]
    fn set__children_cache(&mut self, value: Bound<'_, pyo3::types::PyDict>) {
        self.children_cache = value.unbind();
    }

    // ----- methods ported from bzrformats.inventory.CHKInventory -----

    /// Compare two CHKInventory instances by sha1 keys of their two
    /// underlying CHKMaps. Mirrors Python's `__eq__`.
    fn __eq__<'py>(
        &self,
        py: Python<'py>,
        other: Bound<'py, PyAny>,
    ) -> PyResult<bool> {
        // Only equal to another CHKInventory.
        let other_ref = match other.downcast::<CHKInventory>() {
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
    fn has_filename(
        slf: pyo3::Bound<'_, CHKInventory>,
        filename: &str,
    ) -> PyResult<bool> {
        let result = slf.call_method1("path2id", (filename,))?;
        Ok(!result.is_none())
    }

    /// Yield the parents of `file_id` up to the root. Mirrors
    /// Python's `_iter_file_id_parents`. Returns a list rather than
    /// a generator (no streaming benefit for the typical short chain
    /// up to the root).
    fn _iter_file_id_parents<'py>(
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
    /// `iter_all_ids` (which is a generator); we return a list.
    fn iter_all_ids<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let out = PyList::empty(py);
        let map = self
            .id_to_entry
            .as_ref()
            .ok_or_else(|| BzrFormatsError::new_err("id_to_entry not set"))?;
        let items_iter = map.bind(py).call_method0("iteritems")?;
        for pair in items_iter.try_iter()? {
            let pair = pair?;
            let tup = pair.cast_into::<PyTuple>()?;
            let key = tup.get_item(0)?;
            // key[-1] in Python — the last element of the key tuple.
            let key_tup = key.cast_into::<PyTuple>()?;
            let last = key_tup.get_item(key_tup.len() - 1)?;
            out.append(last)?;
        }
        Ok(out)
    }

    /// Yield every entry in the inventory. Mirrors Python's
    /// `iter_just_entries`; populates the cache as it walks.
    fn iter_just_entries<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyList>> {
        let out = PyList::empty(py);
        let map = self
            .id_to_entry
            .as_ref()
            .ok_or_else(|| BzrFormatsError::new_err("id_to_entry not set"))?;
        let cache = self.fileid_to_entry_cache.bind(py);
        let items_iter = map.bind(py).call_method0("iteritems")?;
        for pair in items_iter.try_iter()? {
            let pair = pair?;
            let tup = pair.cast_into::<PyTuple>()?;
            let key = tup.get_item(0)?;
            let value = tup.get_item(1)?;
            let key_tup = key.cast_into::<PyTuple>()?;
            let file_id = key_tup.get_item(0)?;
            let entry = match cache.get_item(&file_id)? {
                Some(e) => e,
                None => {
                    let bytes = value.cast_into::<PyBytes>()?;
                    let e = chk_inventory_bytes_to_entry(py, bytes.as_bytes())?;
                    cache.set_item(&file_id, &e)?;
                    e
                }
            };
            out.append(entry)?;
        }
        Ok(out)
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
        let root_id = self.root_id.as_ref().ok_or_else(|| {
            NoSuchId::new_err((py.None(), py.None()))
        })?;
        let id_bytes = PyBytes::new(py, root_id.as_bytes());
        self.get_entry(py, id_bytes.into_any())
    }

    /// Return the slash-separated path to `file_id`. Mirrors
    /// Python's `id2path`. Raises NoSuchId if absent.
    fn id2path(&self, py: Python<'_>, file_id: Bound<'_, PyBytes>) -> PyResult<String> {
        let parents = self._iter_file_id_parents(py, file_id)?;
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
    fn path2id<'py>(
        &self,
        py: Python<'py>,
        relpath: Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
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
        let parent_id_index = self.parent_id_basename_to_file_id.as_ref().ok_or_else(|| {
            BzrFormatsError::new_err("parent_id_basename_to_file_id not set")
        })?;
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
            let key_tuple = PyTuple::new(
                py,
                [current_id.bind(py).clone(), basename_utf8.into_any()],
            )?;
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
            let id_to_entry = self.id_to_entry.as_ref().ok_or_else(|| {
                BzrFormatsError::new_err("id_to_entry not set")
            })?;
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
    /// Mirrors Python's `iter_sorted_children`. Returns a list rather
    /// than a generator.
    fn iter_sorted_children<'py>(
        &self,
        py: Python<'py>,
        file_id: Bound<'py, PyBytes>,
    ) -> PyResult<Bound<'py, PyList>> {
        let children = self.get_children(py, file_id)?;
        // Sort by key (name).
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

    /// Walk the inventory in lexicographic order. Mirrors Python's
    /// `iter_entries(from_dir, recursive)`. Returns a list of
    /// `(path, entry)` pairs.
    #[pyo3(signature = (from_dir=None, recursive=true))]
    fn iter_entries<'py>(
        slf: pyo3::Bound<'py, CHKInventory>,
        py: Python<'py>,
        from_dir: Option<Bound<'py, PyAny>>,
        recursive: bool,
    ) -> PyResult<Bound<'py, PyList>> {
        let out = PyList::empty(py);
        let (start_file_id, _) = match from_dir {
            None => {
                if slf.borrow().root_id.is_none() {
                    return Ok(out);
                }
                let root = slf.getattr("root")?;
                let fid = root.getattr("file_id")?;
                out.append(PyTuple::new(
                    py,
                    [PyString::new(py, "").into_any(), root.clone()],
                )?)?;
                (fid.unbind(), true)
            }
            Some(fd) => {
                if let Ok(b) = fd.clone().cast_into::<PyBytes>() {
                    (b.into_any().unbind(), false)
                } else {
                    let fid = fd.getattr("file_id")?;
                    (fid.unbind(), false)
                }
            }
        };
        let start_bytes = start_file_id.bind(py).clone().cast_into::<PyBytes>()?;
        let direct = slf.borrow().iter_sorted_children(py, start_bytes)?;
        if !recursive {
            for c in direct.iter() {
                let name = c.getattr("name")?;
                out.append(PyTuple::new(py, [name, c])?)?;
            }
            return Ok(out);
        }
        let mut stack: Vec<(String, std::collections::VecDeque<Py<PyAny>>)> = Vec::new();
        let mut queue: std::collections::VecDeque<Py<PyAny>> =
            std::collections::VecDeque::new();
        for c in direct.iter() {
            queue.push_back(c.unbind());
        }
        stack.push((String::new(), queue));
        while let Some((path, children)) = stack.last_mut() {
            if let Some(ie_py) = children.pop_front() {
                let ie = ie_py.bind(py);
                let name: String = ie.getattr("name")?.extract()?;
                let new_path = format!("{}/{}", path, name);
                let yield_path = new_path.trim_start_matches('/').to_string();
                let kind: String = ie.getattr("kind")?.extract()?;
                out.append(PyTuple::new(
                    py,
                    [PyString::new(py, &yield_path).into_any(), ie.clone()],
                )?)?;
                if kind == "directory" {
                    let fid = ie.getattr("file_id")?.cast_into::<PyBytes>()?;
                    let new_children = slf.borrow().iter_sorted_children(py, fid)?;
                    let mut q: std::collections::VecDeque<Py<PyAny>> =
                        std::collections::VecDeque::new();
                    for c in new_children.iter() {
                        q.push_back(c.unbind());
                    }
                    stack.push((new_path, q));
                }
            } else {
                stack.pop();
            }
        }
        Ok(out)
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
            let children = slf.borrow().iter_sorted_children(py, fid)?;
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
            let cie = slf.borrow().get_child(
                py,
                dir_id,
                PyString::new(py, f).into_any(),
            )?;
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
            let cie = slf.borrow().get_child(
                py,
                dir_id,
                PyString::new(py, f).into_any(),
            )?;
            if cie.bind(py).is_none() {
                let t = PyTuple::new(py, [py.None(), py.None(), py.None()])?;
                return Ok(t.into_any().unbind());
            }
            let kind: String = cie.bind(py).getattr("kind")?.extract()?;
            if kind == "tree-reference" {
                let resolved: Vec<&str> = names[..=i].iter().map(String::as_str).collect();
                let remaining: Vec<&str> =
                    names[i + 1..].iter().map(String::as_str).collect();
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
        let resolved_list = PyList::new(
            py,
            names.iter().map(String::as_str).collect::<Vec<_>>(),
        )?;
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
    /// `iter_entries_by_dir(from_dir, specific_file_ids)`.
    #[pyo3(signature = (from_dir=None, specific_file_ids=None))]
    fn iter_entries_by_dir<'py>(
        slf: pyo3::Bound<'py, CHKInventory>,
        py: Python<'py>,
        from_dir: Option<Bound<'py, PyAny>>,
        specific_file_ids: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyList>> {
        let out = PyList::empty(py);
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
        let from_entry: Py<PyAny> = if let Some(fd) = from_dir.clone() {
            if let Ok(b) = fd.clone().cast_into::<PyBytes>() {
                slf.borrow().get_entry(py, b.into_any())?.unbind()
            } else {
                fd.unbind()
            }
        } else {
            if let Some(set) = &specific_set {
                if set.len() == 1 {
                    let only = set.iter().next().unwrap().clone();
                    let bytes = PyBytes::new(py, &only);
                    match slf.call_method1("id2path", (&bytes,)) {
                        Ok(path) => {
                            match slf.borrow().get_entry(py, bytes.into_any()) {
                                Ok(entry) => {
                                    out.append(PyTuple::new(py, [path, entry])?)?;
                                }
                                Err(_) => {}
                            }
                        }
                        Err(e) if e.is_instance_of::<NoSuchId>(py) => {}
                        Err(e) => return Err(e),
                    }
                    return Ok(out);
                }
            }
            if slf.borrow().root_id.is_none() {
                return Ok(out);
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
                out.append(PyTuple::new(
                    py,
                    [PyString::new(py, "").into_any(), root.clone()],
                )?)?;
            }
            root.unbind()
        };
        let parents_filter: Option<std::collections::HashSet<Vec<u8>>> =
            match &specific_set {
                None => None,
                Some(set) => {
                    let mut ancestors: std::collections::HashSet<Vec<u8>> =
                        std::collections::HashSet::new();
                    for fid in set {
                        let mut cur: Option<Vec<u8>> = Some(fid.clone());
                        while let Some(id) = cur {
                            let id_bytes = PyBytes::new(py, &id);
                            let has_id: bool =
                                slf.borrow().has_id(py, id_bytes.clone().into_any())?;
                            if !has_id {
                                break;
                            }
                            let entry =
                                slf.borrow().get_entry(py, id_bytes.into_any())?;
                            let parent_id = entry.getattr("parent_id")?;
                            let parent_bytes: Option<Vec<u8>> = if parent_id.is_none() {
                                None
                            } else {
                                Some(
                                    parent_id
                                        .cast_into::<PyBytes>()?
                                        .as_bytes()
                                        .to_vec(),
                                )
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
        let mut stack: Vec<(String, Py<PyAny>)> = vec![(String::new(), from_entry)];
        while let Some((cur_relpath, cur_dir)) = stack.pop() {
            let mut child_dirs: Vec<(String, Py<PyAny>)> = Vec::new();
            let cur_fid = cur_dir
                .bind(py)
                .getattr("file_id")?
                .cast_into::<PyBytes>()?;
            let children = slf.borrow().iter_sorted_children(py, cur_fid)?;
            for child in children.iter() {
                let child_name: String = child.getattr("name")?.extract()?;
                let child_relpath = format!("{}{}", cur_relpath, child_name);
                let child_fid: Vec<u8> = child
                    .getattr("file_id")?
                    .cast_into::<PyBytes>()?
                    .as_bytes()
                    .to_vec();
                if specific_set
                    .as_ref()
                    .map_or(true, |s| s.contains(&child_fid))
                {
                    out.append(PyTuple::new(
                        py,
                        [
                            PyString::new(py, &child_relpath).into_any(),
                            child.clone(),
                        ],
                    )?)?;
                }
                let kind: String = child.getattr("kind")?.extract()?;
                if kind == "directory" {
                    let recurse = match &parents_filter {
                        None => true,
                        Some(p) => p.contains(&child_fid),
                    };
                    if recurse {
                        child_dirs
                            .push((format!("{}/", child_relpath), child.unbind()));
                    }
                }
            }
            for cd in child_dirs.into_iter().rev() {
                stack.push(cd);
            }
        }
        Ok(out)
    }
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
    m.add("InventoryDeltaError", py.get_type::<InventoryDeltaError>())?;
    m.add(
        "IncompatibleInventoryDelta",
        py.get_type::<IncompatibleInventoryDelta>(),
    )?;
    m.add_wrapped(wrap_pyfunction!(chk_inventory_entry_to_bytes))?;
    m.add_wrapped(wrap_pyfunction!(chk_inventory_bytes_to_entry))?;
    m.add_wrapped(wrap_pyfunction!(chk_inventory_bytes_to_utf8name_key))?;
    m.add_class::<CHKInventory>()?;

    Ok(m)
}
