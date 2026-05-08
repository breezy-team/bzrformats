use bazaar::weave::{
    extract, inclusions, order_record_stream, read_weave_v5, reweave, walk_internal,
    write_weave_v5, ExtractLine, Instruction, PlanMergeState, WalkLine, WeaveEntry, WeaveError,
    WeaveFile, WeaveFileError,
};
use pyo3::class::basic::CompareOp;
use pyo3::exceptions::{PyNotImplementedError, PyTypeError, PyValueError};
use pyo3::import_exception;
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyBytes, PyFrozenSet, PyList, PyTuple};

import_exception!(bzrformats.weave, WeaveFormatError);
import_exception!(bzrformats.weave, WeaveInvalidChecksum);
import_exception!(bzrformats.weave, WeaveParentMismatch);
import_exception!(bzrformats.weave, WeaveTextDiffers);
import_exception!(bzrformats.errors, RevisionAlreadyPresent);
import_exception!(bzrformats.errors, RevisionNotPresent);
import_exception!(bzrformats.versionedfile, ExistingContent);
import_exception!(bzrformats.versionedfile, UnavailableRepresentation);

fn py_weave_to_rust(weave: &Bound<PyList>) -> PyResult<Vec<WeaveEntry>> {
    let mut out = Vec::with_capacity(weave.len());
    for item in weave.iter() {
        if let Ok(bytes) = item.cast::<PyBytes>() {
            out.push(WeaveEntry::Line(bytes.as_bytes().to_vec()));
            continue;
        }
        let tup = item
            .cast::<PyTuple>()
            .map_err(|_| PyTypeError::new_err("weave entries must be bytes or 2-tuples"))?;
        if tup.len() != 2 {
            return Err(PyTypeError::new_err(
                "weave control tuples must have length 2",
            ));
        }
        let tag = tup
            .get_item(0)?
            .cast_into::<PyBytes>()
            .map_err(|_| PyTypeError::new_err("weave control tag must be bytes"))?;
        let op = match tag.as_bytes() {
            b"{" => Instruction::InsertOpen,
            b"}" => Instruction::InsertClose,
            b"[" => Instruction::DeleteOpen,
            b"]" => Instruction::DeleteClose,
            other => {
                return Err(PyValueError::new_err(format!(
                    "unknown weave instruction: {:?}",
                    other
                )));
            }
        };
        let version_obj = tup.get_item(1)?;
        // Python stores `(b"}", None)` for close-insertion — the version slot
        // is unused there, so accept None.
        let version = if version_obj.is_none() {
            0
        } else {
            version_obj.extract::<usize>()?
        };
        out.push(WeaveEntry::Control { op, version });
    }
    Ok(out)
}

fn weave_err_to_py(err: WeaveError) -> PyErr {
    // Map to whatever the Python caller expected; for now a plain ValueError
    // carrying the display string. Callers wrap this in WeaveFormatError.
    PyValueError::new_err(err.to_string())
}

/// Walk the weave and return the extracted `(origin_index, lineno, line)`
/// tuples for the given `included` set. `included` may be any iterable of
/// integer version indices; it should already be the transitive ancestor
/// closure.
#[pyfunction]
#[pyo3(name = "extract")]
fn py_extract<'py>(
    py: Python<'py>,
    weave: Bound<'py, PyList>,
    included: Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyList>> {
    let entries = py_weave_to_rust(&weave)?;
    let mut incl = std::collections::HashSet::new();
    for item in included.try_iter()? {
        incl.insert(item?.extract::<usize>()?);
    }
    let lines: Vec<ExtractLine<'_>> = extract(&entries, &incl).map_err(weave_err_to_py)?;
    let items: Vec<Bound<PyTuple>> = lines
        .into_iter()
        .map(|e| {
            PyTuple::new(
                py,
                [
                    e.origin.into_pyobject(py)?.into_any(),
                    e.lineno.into_pyobject(py)?.into_any(),
                    PyBytes::new(py, e.text).into_any(),
                ],
            )
        })
        .collect::<PyResult<_>>()?;
    PyList::new(py, items)
}

/// Compute the transitive ancestor set of `versions` given a list-of-lists
/// `parents` table indexed by version number. Returns a Python `set` of int.
#[pyfunction]
#[pyo3(name = "inclusions")]
fn py_inclusions<'py>(
    py: Python<'py>,
    parents: Bound<'py, PyList>,
    versions: Bound<'py, PyAny>,
) -> PyResult<Bound<'py, pyo3::types::PySet>> {
    let mut parents_rust: Vec<Vec<usize>> = Vec::with_capacity(parents.len());
    for row in parents.iter() {
        let mut ps = Vec::new();
        for p in row.try_iter()? {
            ps.push(p?.extract::<usize>()?);
        }
        parents_rust.push(ps);
    }
    let mut versions_rust: Vec<usize> = Vec::new();
    for v in versions.try_iter()? {
        versions_rust.push(v?.extract::<usize>()?);
    }
    let result = inclusions(&parents_rust, &versions_rust);
    pyo3::types::PySet::new(py, result.iter())
}

/// Walk the weave yielding `(lineno, insert_version, frozenset(deletes), line)`
/// tuples for every literal line. `insert_version` and the deletion-set
/// elements are integer indices; callers translate to names if desired.
#[pyfunction]
#[pyo3(name = "walk_internal")]
fn py_walk_internal<'py>(
    py: Python<'py>,
    weave: Bound<'py, PyList>,
) -> PyResult<Bound<'py, PyList>> {
    let entries = py_weave_to_rust(&weave)?;
    let walked: Vec<WalkLine<'_>> = walk_internal(&entries).map_err(weave_err_to_py)?;
    let items: Vec<Bound<PyTuple>> = walked
        .into_iter()
        .map(|w| {
            let deletes = PyFrozenSet::new(py, w.deletes.iter())?;
            PyTuple::new(
                py,
                [
                    w.lineno.into_pyobject(py)?.into_any(),
                    w.insert.into_pyobject(py)?.into_any(),
                    deletes.into_any(),
                    PyBytes::new(py, w.text).into_any(),
                ],
            )
        })
        .collect::<PyResult<_>>()?;
    PyList::new(py, items)
}

fn weave_file_err_to_py(err: WeaveFileError) -> PyErr {
    WeaveFormatError::new_err(err.to_string())
}

/// The four-list tuple returned by [`py_read_weave_v5`] — parents, sha1s,
/// names, weave body.
type WeaveFileFields<'py> = (
    Bound<'py, PyList>,
    Bound<'py, PyList>,
    Bound<'py, PyList>,
    Bound<'py, PyList>,
);

/// Assemble the Rust-side weave entry list into a Python list matching the
/// shape `bzrformats.weave.Weave._weave` uses: literal lines as bytes,
/// control tuples as `(op, version)` with `None` for close-insertion.
fn rust_weave_to_py<'py>(py: Python<'py>, entries: &[WeaveEntry]) -> PyResult<Bound<'py, PyList>> {
    let out = PyList::empty(py);
    for entry in entries {
        match entry {
            WeaveEntry::Line(line) => out.append(PyBytes::new(py, line))?,
            WeaveEntry::Control { op, version } => {
                let (tag, with_version): (&[u8], bool) = match op {
                    Instruction::InsertOpen => (b"{", true),
                    Instruction::InsertClose => (b"}", false),
                    Instruction::DeleteOpen => (b"[", true),
                    Instruction::DeleteClose => (b"]", true),
                };
                let tag_bytes = PyBytes::new(py, tag);
                let tuple = if with_version {
                    PyTuple::new(
                        py,
                        [tag_bytes.into_any(), version.into_pyobject(py)?.into_any()],
                    )?
                } else {
                    PyTuple::new(py, [tag_bytes.into_any(), py.None().into_bound(py)])?
                };
                out.append(tuple)?;
            }
        }
    }
    Ok(out)
}

/// Parse a v5 weave file. Returns `(parents, sha1s, names, weave)` — the
/// four lists the Python `Weave` instance needs.
#[pyfunction]
#[pyo3(name = "read_weave_v5")]
fn py_read_weave_v5<'py>(py: Python<'py>, data: &[u8]) -> PyResult<WeaveFileFields<'py>> {
    let wf = read_weave_v5(data).map_err(weave_file_err_to_py)?;

    let parents = PyList::empty(py);
    for ps in &wf.parents {
        let inner: Vec<Bound<PyAny>> = ps
            .iter()
            .map(|p| -> PyResult<Bound<PyAny>> { Ok(p.into_pyobject(py)?.into_any()) })
            .collect::<PyResult<_>>()?;
        parents.append(PyList::new(py, inner)?)?;
    }

    let sha1s = PyList::empty(py);
    for s in &wf.sha1s {
        sha1s.append(PyBytes::new(py, s))?;
    }

    let names = PyList::empty(py);
    for n in &wf.names {
        names.append(PyBytes::new(py, n))?;
    }

    let weave_list = rust_weave_to_py(py, &wf.weave)?;
    Ok((parents, sha1s, names, weave_list))
}

/// Serialize a weave to v5 bytes. Arguments are the same four lists the
/// Python `Weave` stores: `_parents`, `_sha1s`, `_names`, `_weave`.
#[pyfunction]
#[pyo3(name = "write_weave_v5")]
fn py_write_weave_v5<'py>(
    py: Python<'py>,
    parents: Bound<'py, PyList>,
    sha1s: Bound<'py, PyList>,
    names: Bound<'py, PyList>,
    weave: Bound<'py, PyList>,
) -> PyResult<Bound<'py, PyBytes>> {
    let mut parents_rust: Vec<Vec<usize>> = Vec::with_capacity(parents.len());
    for row in parents.iter() {
        let mut ps = Vec::new();
        for p in row.try_iter()? {
            ps.push(p?.extract::<usize>()?);
        }
        parents_rust.push(ps);
    }
    let sha1s_rust: Vec<Vec<u8>> = sha1s
        .iter()
        .map(|s| -> PyResult<Vec<u8>> {
            Ok(s.cast_into::<PyBytes>()
                .map_err(|_| PyTypeError::new_err("sha1 entries must be bytes"))?
                .as_bytes()
                .to_vec())
        })
        .collect::<PyResult<_>>()?;
    let names_rust: Vec<Vec<u8>> = names
        .iter()
        .map(|n| -> PyResult<Vec<u8>> {
            Ok(n.cast_into::<PyBytes>()
                .map_err(|_| PyTypeError::new_err("name entries must be bytes"))?
                .as_bytes()
                .to_vec())
        })
        .collect::<PyResult<_>>()?;
    let weave_rust = py_weave_to_rust(&weave)?;

    let wf = WeaveFile {
        parents: parents_rust,
        sha1s: sha1s_rust,
        names: names_rust,
        weave: weave_rust,
    };
    Ok(PyBytes::new(py, &write_weave_v5(&wf)))
}

/// Decode the four `Weave._parents/_sha1s/_names/_weave` lists into a
/// pure-Rust [`WeaveFile`]. Used by helpers that need to mutate a weave.
fn weave_lists_to_rust<'py>(
    parents: &Bound<'py, PyList>,
    sha1s: &Bound<'py, PyList>,
    names: &Bound<'py, PyList>,
    weave: &Bound<'py, PyList>,
) -> PyResult<WeaveFile> {
    let mut parents_rust: Vec<Vec<usize>> = Vec::with_capacity(parents.len());
    for row in parents.iter() {
        let mut ps = Vec::new();
        for p in row.try_iter()? {
            ps.push(p?.extract::<usize>()?);
        }
        parents_rust.push(ps);
    }
    let sha1s_rust: Vec<Vec<u8>> = sha1s
        .iter()
        .map(|s| -> PyResult<Vec<u8>> {
            Ok(s.cast_into::<PyBytes>()
                .map_err(|_| PyTypeError::new_err("sha1 entries must be bytes"))?
                .as_bytes()
                .to_vec())
        })
        .collect::<PyResult<_>>()?;
    let names_rust: Vec<Vec<u8>> = names
        .iter()
        .map(|n| -> PyResult<Vec<u8>> {
            Ok(n.cast_into::<PyBytes>()
                .map_err(|_| PyTypeError::new_err("name entries must be bytes"))?
                .as_bytes()
                .to_vec())
        })
        .collect::<PyResult<_>>()?;
    let weave_rust = py_weave_to_rust(weave)?;
    Ok(WeaveFile {
        parents: parents_rust,
        sha1s: sha1s_rust,
        names: names_rust,
        weave: weave_rust,
    })
}

/// Encode a [`WeaveFile`] back into the four-list shape Python expects.
fn weave_to_lists<'py>(py: Python<'py>, wf: &WeaveFile) -> PyResult<WeaveFileFields<'py>> {
    let parents = PyList::empty(py);
    for ps in &wf.parents {
        let inner: Vec<Bound<PyAny>> = ps
            .iter()
            .map(|p| -> PyResult<Bound<PyAny>> { Ok(p.into_pyobject(py)?.into_any()) })
            .collect::<PyResult<_>>()?;
        parents.append(PyList::new(py, inner)?)?;
    }
    let sha1s_out = PyList::empty(py);
    for s in &wf.sha1s {
        sha1s_out.append(PyBytes::new(py, s))?;
    }
    let names_out = PyList::empty(py);
    for n in &wf.names {
        names_out.append(PyBytes::new(py, n))?;
    }
    let weave_out = rust_weave_to_py(py, &wf.weave)?;
    Ok((parents, sha1s_out, names_out, weave_out))
}

fn weave_op_err_to_py(py: Python<'_>, err: WeaveError) -> PyErr {
    match err {
        WeaveError::RevisionAlreadyPresent(name) => {
            RevisionAlreadyPresent::new_err((PyBytes::new(py, &name).unbind(), py.None()))
        }
        WeaveError::RevisionNotPresent(idx) => RevisionNotPresent::new_err((idx, py.None())),
        WeaveError::RevisionNotPresentByName(name) => {
            RevisionNotPresent::new_err((PyBytes::new(py, &name).unbind(), py.None()))
        }
        WeaveError::ExistingContent => ExistingContent::new_err(()),
        WeaveError::InvalidChecksum { .. } => WeaveInvalidChecksum::new_err(err.to_string()),
        WeaveError::TextDiffers(name) => {
            // Python signature: WeaveTextDiffers(revision_id, weave_a, weave_b).
            // The Rust core doesn't carry the two weaves, so pass None for both.
            WeaveTextDiffers::new_err((PyBytes::new(py, &name).unbind(), py.None(), py.None()))
        }
        WeaveError::ParentMismatch { .. } => WeaveParentMismatch::new_err(err.to_string()),
        other => WeaveFormatError::new_err(other.to_string()),
    }
}

/// Add a single text on top of a weave. Mirrors `Weave._add` from
/// `bzrformats/weave.py`. Returns the four post-mutation lists plus the
/// new version index.
#[pyfunction]
#[pyo3(name = "weave_add", signature = (parents, sha1s, names, weave, version_id, lines, parent_ids, sha1=None, nostore_sha=None))]
#[allow(clippy::too_many_arguments)]
fn py_weave_add<'py>(
    py: Python<'py>,
    parents: Bound<'py, PyList>,
    sha1s: Bound<'py, PyList>,
    names: Bound<'py, PyList>,
    weave: Bound<'py, PyList>,
    version_id: Option<&[u8]>,
    lines: Bound<'py, PyAny>,
    parent_ids: Bound<'py, PyAny>,
    sha1: Option<&[u8]>,
    nostore_sha: Option<&[u8]>,
) -> PyResult<(
    Bound<'py, PyList>,
    Bound<'py, PyList>,
    Bound<'py, PyList>,
    Bound<'py, PyList>,
    usize,
)> {
    let mut wf = weave_lists_to_rust(&parents, &sha1s, &names, &weave)?;
    let lines_rust: Vec<Vec<u8>> = lines
        .try_iter()?
        .map(|l| -> PyResult<Vec<u8>> {
            Ok(l?
                .cast_into::<PyBytes>()
                .map_err(|_| PyTypeError::new_err("lines must be bytes"))?
                .as_bytes()
                .to_vec())
        })
        .collect::<PyResult<_>>()?;
    let parent_ids_rust: Vec<usize> = parent_ids
        .try_iter()?
        .map(|p| -> PyResult<usize> { p?.extract::<usize>() })
        .collect::<PyResult<_>>()?;
    let idx = wf
        .add(
            version_id,
            &lines_rust,
            &parent_ids_rust,
            sha1.map(|s| s.to_vec()),
            nostore_sha,
        )
        .map_err(|e| weave_op_err_to_py(py, e))?;
    let (p, s, n, w) = weave_to_lists(py, &wf)?;
    Ok((p, s, n, w, idx))
}

import_exception!(bzrformats.errors, ReservedId);

/// Reserved-id check, mirroring `Weave.check_not_reserved_id`. A reserved
/// id has a trailing `:`. Always allowed when `_allow_reserved` is True.
fn check_reserved(name: &[u8], allow_reserved: bool) -> PyResult<()> {
    if !allow_reserved && name.ends_with(b":") {
        Python::attach(|py| -> PyResult<()> {
            Err(ReservedId::new_err((PyBytes::new(py, name).unbind(),)))
        })
    } else {
        Ok(())
    }
}

/// Clone the opaque weave-name slot into a Python value (or None) for
/// use as the `file_id`/`weave` argument of a Python-side exception.
fn weave_name_for_err(py: Python<'_>, name: Option<&Py<PyAny>>) -> Py<PyAny> {
    match name {
        None => py.None(),
        Some(obj) => obj.clone_ref(py),
    }
}

/// Rust-backed `Weave` — holds the entire weave state and exposes the
/// same surface the previous Python class did. The Python `bzrformats.weave`
/// module subclasses this to add transport-backed `WeaveFile` (which
/// overrides `_add_lines` to save).
#[pyclass(subclass, name = "Weave", module = "bzrformats._bzr_rs.weave")]
pub struct PyWeave {
    inner: WeaveFile,
    /// Opaque "name" attached to this weave. Python keeps it as
    /// whatever the caller passed (bytes, str, an int from a tempfile
    /// fd, or None). Stored as a Py<PyAny> so we don't second-guess.
    weave_name: Option<Py<PyAny>>,
    access_mode: String,
    allow_reserved: bool,
    /// Optional scope callback (called as `get_scope()`) — used by
    /// `_check_write_ok`. None means "no scope checking".
    get_scope: Option<Py<PyAny>>,
    /// Cached value of `get_scope()` at construction time.
    scope: Option<Py<PyAny>>,
}

#[pymethods]
impl PyWeave {
    #[new]
    #[pyo3(signature = (weave_name=None, access_mode="w".to_string(), get_scope=None, allow_reserved=false))]
    fn new(
        py: Python<'_>,
        weave_name: Option<Py<PyAny>>,
        access_mode: String,
        get_scope: Option<Py<PyAny>>,
        allow_reserved: bool,
    ) -> PyResult<Self> {
        let weave_name = match weave_name {
            None => None,
            Some(obj) if obj.is_none(py) => None,
            Some(obj) => Some(obj),
        };
        let scope = match &get_scope {
            None => None,
            Some(cb) => Some(cb.call0(py)?),
        };
        Ok(Self {
            inner: WeaveFile::default(),
            weave_name,
            access_mode,
            allow_reserved,
            get_scope,
            scope,
        })
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        match &self.weave_name {
            None => Ok("Weave(None)".to_string()),
            Some(obj) => {
                let r = obj.bind(py).repr()?.extract::<String>()?;
                Ok(format!("Weave({})", r))
            }
        }
    }

    fn __richcmp__(&self, other: &Self, op: CompareOp) -> PyResult<bool> {
        match op {
            CompareOp::Eq => Ok(self.inner.parents == other.inner.parents
                && self.inner.weave == other.inner.weave
                && self.inner.sha1s == other.inner.sha1s),
            CompareOp::Ne => Ok(!(self.inner.parents == other.inner.parents
                && self.inner.weave == other.inner.weave
                && self.inner.sha1s == other.inner.sha1s)),
            _ => Err(PyNotImplementedError::new_err(
                "only == and != are supported",
            )),
        }
    }

    fn __contains__(&self, version_id: &Bound<'_, PyAny>) -> bool {
        match version_id.extract::<&[u8]>() {
            Ok(name) => self.inner.has_version(name),
            // Python's original accepted any object, returning False for
            // anything not in the name map. Mirror that — non-bytes is
            // simply not present.
            Err(_) => false,
        }
    }

    fn __len__(&self) -> usize {
        self.inner.num_versions()
    }

    fn has_version(&self, version_id: &Bound<'_, PyAny>) -> bool {
        match version_id.extract::<&[u8]>() {
            Ok(name) => self.inner.has_version(name),
            Err(_) => false,
        }
    }

    fn num_versions(&self) -> usize {
        self.inner.num_versions()
    }

    fn versions<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let items: Vec<Bound<PyBytes>> =
            self.inner.names.iter().map(|n| PyBytes::new(py, n)).collect();
        PyList::new(py, items)
    }

    /// Return a fresh deep copy. Mirrors `Weave.copy`.
    fn copy(&self, py: Python<'_>) -> PyResult<Py<Self>> {
        Py::new(
            py,
            Self {
                inner: self.inner.clone(),
                weave_name: self.weave_name.as_ref().map(|n| n.clone_ref(py)),
                access_mode: self.access_mode.clone(),
                allow_reserved: self.allow_reserved,
                get_scope: self.get_scope.as_ref().map(|c| c.clone_ref(py)),
                scope: self.scope.as_ref().map(|s| s.clone_ref(py)),
            },
        )
    }

    /// Copy from `other` into self in place. Mirrors `Weave._copy_weave_content`.
    fn _copy_weave_content(&mut self, py: Python<'_>, other: PyRef<Self>) {
        self.inner = other.inner.clone();
        // Match Python: copy every slot except `_weave_name`.
        self.access_mode = other.access_mode.clone();
        self.allow_reserved = other.allow_reserved;
        self.get_scope = other.get_scope.as_ref().map(|c| c.clone_ref(py));
        self.scope = other.scope.as_ref().map(|s| s.clone_ref(py));
    }

    // ---- read-only views over the underlying state ----

    /// Snapshot of the parent table as a list of lists of int.
    #[getter]
    fn _parents<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let outer = PyList::empty(py);
        for ps in &self.inner.parents {
            let inner: Vec<Bound<PyAny>> = ps
                .iter()
                .map(|p| -> PyResult<Bound<PyAny>> { Ok(p.into_pyobject(py)?.into_any()) })
                .collect::<PyResult<_>>()?;
            outer.append(PyList::new(py, inner)?)?;
        }
        Ok(outer)
    }
    #[setter]
    fn set__parents<'py>(&mut self, py: Python<'py>, new_parents: Bound<'py, PyAny>) -> PyResult<()> {
        let mut outer = Vec::new();
        for ps in new_parents.try_iter()? {
            let mut inner = Vec::new();
            for p in ps?.try_iter()? {
                inner.push(p?.extract::<usize>()?);
            }
            outer.push(inner);
        }
        self.inner.parents = outer;
        Ok(())
    }


    /// Snapshot of the per-version sha1 list.
    #[getter]
    fn _sha1s<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let out = PyList::empty(py);
        for s in &self.inner.sha1s {
            out.append(PyBytes::new(py, s))?;
        }
        Ok(out)
    }
    #[setter]
    fn set__names<'py>(&mut self, py: Python<'py>, new_names: Bound<'py, PyAny>) -> PyResult<()> {
        let mut names = Vec::new();
        for n in new_names.try_iter()? {
            let b = n?.cast_into::<PyBytes>()
                .map_err(|_| PyTypeError::new_err("_names must be bytes"))?;
            names.push(b.as_bytes().to_vec());
        }
        self.inner.names = names;
        Ok(())
    }


    /// Snapshot of the per-version name list.
    #[getter]
    fn _names<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let out = PyList::empty(py);
        for n in &self.inner.names {
            out.append(PyBytes::new(py, n))?;
        }
        Ok(out)
    }

    /// Snapshot of the weave entry stream in the `(b"{",v)`/literal-bytes shape
    /// that the Python `Weave._weave` attribute used.
    #[getter]
    fn _weave<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        rust_weave_to_py(py, &self.inner.weave)
    }

    /// Snapshot of the name -> index map.
    #[getter]
    fn _name_map<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::types::PyDict>> {
        let dict = pyo3::types::PyDict::new(py);
        for (i, name) in self.inner.names.iter().enumerate() {
            dict.set_item(PyBytes::new(py, name), i)?;
        }
        Ok(dict)
    }

    #[getter]
    fn _weave_name<'py>(&self, py: Python<'py>) -> Py<PyAny> {
        match &self.weave_name {
            None => py.None(),
            Some(obj) => obj.clone_ref(py),
        }
    }

    #[setter]
    fn set__weave_name(&mut self, py: Python<'_>, value: Py<PyAny>) -> PyResult<()> {
        self.weave_name = if value.is_none(py) { None } else { Some(value) };
        Ok(())
    }

    #[getter]
    fn _access_mode(&self) -> String {
        self.access_mode.clone()
    }

    #[getter]
    fn _allow_reserved(&self) -> bool {
        self.allow_reserved
    }

    fn _check_write_ok(slf: PyRef<'_, Self>, py: Python<'_>) -> PyResult<()> {
        if let Some(get_scope) = &slf.get_scope {
            let current = get_scope.call0(py)?;
            let stored = match &slf.scope {
                None => py.None(),
                Some(s) => s.clone_ref(py),
            };
            if !current.bind(py).eq(stored.bind(py))? {
                let exc = py.import("bzrformats.errors")?.getattr("OutSideTransaction")?;
                return Err(PyErr::from_value(exc.call0()?));
            }
        }
        if slf.access_mode != "w" {
            let exc = py
                .import("bzrformats.errors")?
                .getattr("ReadOnlyObjectDirtiedError")?;
            return Err(PyErr::from_value(exc.call1((slf.into_pyobject(py)?,))?));
        }
        Ok(())
    }

    /// Translate symbolic name to internal index. Errors with
    /// `RevisionNotPresent` if missing. Mirrors `Weave._lookup`.
    fn _lookup(slf: PyRef<'_, Self>, name: &[u8]) -> PyResult<usize> {
        if !slf.allow_reserved {
            check_reserved(name, slf.allow_reserved)?;
        }
        match slf.inner.lookup(name) {
            Some(i) => Ok(i),
            None => Python::attach(|py| {
                Err(RevisionNotPresent::new_err((
                    PyBytes::new(py, name).unbind(),
                    weave_name_for_err(py, slf.weave_name.as_ref()),
                )))
            }),
        }
    }

    /// Map an integer index to its symbolic version name. Mirrors
    /// `Weave._idx_to_name`.
    fn _idx_to_name<'py>(&self, py: Python<'py>, idx: usize) -> PyResult<Bound<'py, PyBytes>> {
        if idx >= self.inner.names.len() {
            return Err(PyValueError::new_err(format!(
                "index {} out of range",
                idx
            )));
        }
        Ok(PyBytes::new(py, &self.inner.names[idx]))
    }

    /// Convert either an integer index or a symbolic name to an integer
    /// index. Mirrors `Weave._maybe_lookup`.
    fn _maybe_lookup(slf: PyRef<'_, Self>, py: Python<'_>, name_or_index: Py<PyAny>) -> PyResult<usize> {
        if let Ok(i) = name_or_index.extract::<usize>(py) {
            return Ok(i);
        }
        let name = name_or_index.extract::<&[u8]>(py)?;
        Self::_lookup(slf, name)
    }

    /// Compute the transitive ancestor index set for the given indices.
    /// Mirrors `Weave._inclusions`.
    fn _inclusions<'py>(
        &self,
        py: Python<'py>,
        versions: Vec<usize>,
    ) -> PyResult<Bound<'py, pyo3::types::PySet>> {
        let result = inclusions(&self.inner.parents, &versions);
        pyo3::types::PySet::new(py, result.iter())
    }

    /// Static subset check used by `_join`. Mirrors `Weave._compatible_parents`.
    #[staticmethod]
    fn _compatible_parents(
        my_parents: Bound<'_, PyAny>,
        other_parents: Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        let my: std::collections::HashSet<i64> = my_parents
            .try_iter()?
            .map(|x| x?.extract::<i64>())
            .collect::<PyResult<_>>()?;
        let other: std::collections::HashSet<i64> = other_parents
            .try_iter()?
            .map(|x| x?.extract::<i64>())
            .collect::<PyResult<_>>()?;
        Ok(other.is_subset(&my))
    }

    /// Walk the weave, yielding `(lineno, insert_name, frozenset(delete_names),
    /// line)` tuples. Mirrors `Weave._walk_internal`.
    #[pyo3(signature = (_version_ids=None))]
    fn _walk_internal<'py>(
        &self,
        py: Python<'py>,
        _version_ids: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyList>> {
        let walked = walk_internal(&self.inner.weave).map_err(|e| weave_op_err_to_py(py, e))?;
        let names = &self.inner.names;
        let items: Vec<Bound<PyTuple>> = walked
            .into_iter()
            .map(|w| {
                let delete_names: Vec<Bound<PyBytes>> =
                    w.deletes.iter().map(|&d| PyBytes::new(py, &names[d])).collect();
                let deletes = PyFrozenSet::new(py, delete_names.iter())?;
                PyTuple::new(
                    py,
                    [
                        w.lineno.into_pyobject(py)?.into_any(),
                        PyBytes::new(py, &names[w.insert]).into_any(),
                        deletes.into_any(),
                        PyBytes::new(py, w.text).into_any(),
                    ],
                )
            })
            .collect::<PyResult<_>>()?;
        PyList::new(py, items)
    }

    /// Walk the weave for the given int indices and yield
    /// `(origin_index, lineno, line)` tuples. Mirrors `Weave._extract`.
    fn _extract<'py>(
        &self,
        py: Python<'py>,
        versions: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        let mut idxs = Vec::new();
        for v in versions.try_iter()? {
            let v = v?;
            let i = v.extract::<usize>().map_err(|_| {
                PyValueError::new_err("_extract requires integer version indices")
            })?;
            idxs.push(i);
        }
        let included = inclusions(&self.inner.parents, &idxs);
        let lines: Vec<ExtractLine<'_>> =
            extract(&self.inner.weave, &included).map_err(|e| weave_op_err_to_py(py, e))?;
        let items: Vec<Bound<PyTuple>> = lines
            .into_iter()
            .map(|e| {
                PyTuple::new(
                    py,
                    [
                        e.origin.into_pyobject(py)?.into_any(),
                        e.lineno.into_pyobject(py)?.into_any(),
                        PyBytes::new(py, e.text).into_any(),
                    ],
                )
            })
            .collect::<PyResult<_>>()?;
        PyList::new(py, items)
    }

    /// Get parent map for the given version names. Unknown names are
    /// silently dropped. NULL_REVISION maps to an empty parent tuple.
    /// Mirrors `Weave.get_parent_map`.
    fn get_parent_map<'py>(
        &self,
        py: Python<'py>,
        version_ids: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, pyo3::types::PyDict>> {
        let result = pyo3::types::PyDict::new(py);
        for v in version_ids.try_iter()? {
            let v = v?;
            let bytes = v.cast_into::<PyBytes>().map_err(|_| {
                PyTypeError::new_err("get_parent_map version_ids must be bytes")
            })?;
            let name = bytes.as_bytes();
            if name == bazaar::NULL_REVISION {
                let empty = PyTuple::empty(py);
                result.set_item(PyBytes::new(py, name), empty)?;
                continue;
            }
            if let Some(idx) = self.inner.lookup(name) {
                let parents = &self.inner.parents[idx];
                let parent_names: Vec<Bound<PyBytes>> = parents
                    .iter()
                    .map(|&p| PyBytes::new(py, &self.inner.names[p]))
                    .collect();
                let tup = PyTuple::new(py, parent_names.iter())?;
                result.set_item(PyBytes::new(py, name), tup)?;
            }
        }
        Ok(result)
    }

    fn get_parents_with_ghosts(&self, _version_id: &[u8]) -> PyResult<Py<PyAny>> {
        Err(PyNotImplementedError::new_err(
            "get_parents_with_ghosts not supported on Weave",
        ))
    }

    /// Map version names to their stored sha1 hex digests. Errors with
    /// `RevisionNotPresent` for unknown names. Mirrors `Weave.get_sha1s`.
    fn get_sha1s<'py>(
        &self,
        py: Python<'py>,
        version_ids: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, pyo3::types::PyDict>> {
        let result = pyo3::types::PyDict::new(py);
        for v in version_ids.try_iter()? {
            let v = v?;
            let bytes = v
                .cast_into::<PyBytes>()
                .map_err(|_| PyTypeError::new_err("get_sha1s version_ids must be bytes"))?;
            let name = bytes.as_bytes();
            let idx = self.inner.lookup(name).ok_or_else(|| {
                Python::attach(|py| {
                    RevisionNotPresent::new_err((
                        PyBytes::new(py, name).unbind(),
                        weave_name_for_err(py, self.weave_name.as_ref()),
                    ))
                })
            })?;
            result.set_item(
                PyBytes::new(py, name),
                PyBytes::new(py, &self.inner.sha1s[idx]),
            )?;
        }
        Ok(result)
    }

    /// Return the ancestor name set for the given starting names.
    /// `version_ids` may be a single bytes or an iterable. Mirrors
    /// `Weave.get_ancestry`.
    #[pyo3(signature = (version_ids, topo_sorted=true))]
    fn get_ancestry<'py>(
        &self,
        py: Python<'py>,
        version_ids: Bound<'py, PyAny>,
        topo_sorted: bool,
    ) -> PyResult<Bound<'py, pyo3::types::PySet>> {
        let _ = topo_sorted;
        let mut names: Vec<Vec<u8>> = Vec::new();
        if let Ok(b) = version_ids.cast::<PyBytes>() {
            names.push(b.as_bytes().to_vec());
        } else {
            for v in version_ids.try_iter()? {
                let v = v?;
                let bytes = v.cast_into::<PyBytes>().map_err(|_| {
                    PyTypeError::new_err("get_ancestry expects bytes or iterable of bytes")
                })?;
                names.push(bytes.as_bytes().to_vec());
            }
        }
        let mut idxs = Vec::with_capacity(names.len());
        for name in &names {
            let i = self.inner.lookup(name).ok_or_else(|| {
                Python::attach(|py| {
                    RevisionNotPresent::new_err((
                        PyBytes::new(py, name).unbind(),
                        weave_name_for_err(py, self.weave_name.as_ref()),
                    ))
                })
            })?;
            idxs.push(i);
        }
        let inc = inclusions(&self.inner.parents, &idxs);
        let names_out: Vec<Bound<PyBytes>> = inc
            .into_iter()
            .map(|i| PyBytes::new(py, &self.inner.names[i]))
            .collect();
        pyo3::types::PySet::new(py, names_out.iter())
    }

    /// Return `[(origin_name, line), ...]` for the given version. Mirrors
    /// `Weave.annotate`.
    fn annotate<'py>(
        &self,
        py: Python<'py>,
        version_id: &[u8],
    ) -> PyResult<Bound<'py, PyList>> {
        let idx = self.inner.lookup(version_id).ok_or_else(|| {
            RevisionNotPresent::new_err((
                PyBytes::new(py, version_id).unbind(),
                weave_name_for_err(py, self.weave_name.as_ref()),
            ))
        })?;
        let pairs = self
            .inner
            .annotate(idx)
            .map_err(|e| weave_op_err_to_py(py, e))?;
        let items: Vec<Bound<PyTuple>> = pairs
            .into_iter()
            .map(|(name, line)| {
                PyTuple::new(
                    py,
                    [
                        PyBytes::new(py, &name).into_any(),
                        PyBytes::new(py, &line).into_any(),
                    ],
                )
            })
            .collect::<PyResult<_>>()?;
        PyList::new(py, items)
    }

    /// Get the lines of a version, verifying its sha1. `version_id` may be
    /// a bytes name or an integer index. Mirrors `Weave.get_lines`.
    fn get_lines<'py>(
        &self,
        py: Python<'py>,
        version_id: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        let idx = if let Ok(i) = version_id.extract::<usize>() {
            i
        } else {
            let bytes = version_id.cast_into::<PyBytes>().map_err(|_| {
                PyTypeError::new_err("get_lines expects bytes name or integer index")
            })?;
            let name = bytes.as_bytes();
            if !self.allow_reserved && name.ends_with(b":") {
                return Err(ReservedId::new_err((PyBytes::new(py, name).unbind(),)));
            }
            self.inner.lookup(name).ok_or_else(|| {
                RevisionNotPresent::new_err((
                    PyBytes::new(py, name).unbind(),
                    weave_name_for_err(py, self.weave_name.as_ref()),
                ))
            })?
        };
        let lines = self
            .inner
            .get_lines(idx)
            .map_err(|e| weave_op_err_to_py(py, e))?;
        let items: Vec<Bound<PyBytes>> = lines.iter().map(|l| PyBytes::new(py, l)).collect();
        PyList::new(py, items)
    }

    /// Convenience: concatenate `get_lines`. Matches `VersionedFile.get_text`.
    fn get_text<'py>(
        &self,
        py: Python<'py>,
        version_id: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let lines = self.get_lines(py, version_id)?;
        let mut buf: Vec<u8> = Vec::new();
        for line in lines.iter() {
            let b = line
                .cast_into::<PyBytes>()
                .expect("get_lines returned non-bytes");
            buf.extend_from_slice(b.as_bytes());
        }
        Ok(PyBytes::new(py, &buf))
    }

    /// Iterator over `(line_with_eol, inserted_name)` pairs. Mirrors
    /// `Weave.iter_lines_added_or_present_in_versions`.
    #[pyo3(signature = (version_ids=None, pb=None))]
    fn iter_lines_added_or_present_in_versions<'py>(
        &self,
        py: Python<'py>,
        version_ids: Option<Bound<'py, PyAny>>,
        pb: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyList>> {
        let _ = pb;
        let names_owned: Option<Vec<Vec<u8>>> = match version_ids {
            None => None,
            Some(obj) => {
                let mut v = Vec::new();
                for item in obj.try_iter()? {
                    let it = item?;
                    let b = it.cast_into::<PyBytes>().map_err(|_| {
                        PyTypeError::new_err(
                            "iter_lines_added_or_present_in_versions: version_ids must be bytes",
                        )
                    })?;
                    v.push(b.as_bytes().to_vec());
                }
                Some(v)
            }
        };
        let pairs = match &names_owned {
            None => self
                .inner
                .iter_lines_added_or_present_in_versions::<std::iter::Empty<&[u8]>>(None)
                .map_err(|e| weave_op_err_to_py(py, e))?,
            Some(v) => {
                let refs: Vec<&[u8]> = v.iter().map(|x| x.as_slice()).collect();
                self.inner
                    .iter_lines_added_or_present_in_versions(Some(refs))
                    .map_err(|e| weave_op_err_to_py(py, e))?
            }
        };
        let items: Vec<Bound<PyTuple>> = pairs
            .into_iter()
            .map(|(line, name)| {
                PyTuple::new(
                    py,
                    [
                        PyBytes::new(py, &line).into_any(),
                        PyBytes::new(py, &name).into_any(),
                    ],
                )
            })
            .collect::<PyResult<_>>()?;
        PyList::new(py, items)
    }

    /// Three-way merge plan. Yields `(state_str, line_bytes)` tuples
    /// where `state_str` is one of "killed-base", "killed-both",
    /// "killed-a", "killed-b", "unchanged", "new-a", "new-b", "ghost-a",
    /// "ghost-b", or "irrelevant". Mirrors `Weave.plan_merge`.
    fn plan_merge<'py>(
        &self,
        py: Python<'py>,
        ver_a: &[u8],
        ver_b: &[u8],
    ) -> PyResult<Bound<'py, PyList>> {
        let plan = self
            .inner
            .plan_merge(ver_a, ver_b)
            .map_err(|e| weave_op_err_to_py(py, e))?;
        let items: Vec<Bound<PyTuple>> = plan
            .into_iter()
            .map(|(state, line): (PlanMergeState, Vec<u8>)| {
                let tag_str = std::str::from_utf8(state.tag())
                    .expect("PlanMergeState tags are ASCII");
                PyTuple::new(
                    py,
                    [
                        pyo3::types::PyString::new(py, tag_str).into_any(),
                        PyBytes::new(py, &line).into_any(),
                    ],
                )
            })
            .collect::<PyResult<_>>()?;
        PyList::new(py, items)
    }

    /// Internal consistency check. Raises WeaveFormatError or
    /// WeaveInvalidChecksum on detected corruption. Mirrors `Weave.check`.
    #[pyo3(signature = (progress_bar=None))]
    fn check(&self, py: Python<'_>, progress_bar: Option<Py<PyAny>>) -> PyResult<()> {
        let _ = progress_bar;
        self.inner.check().map_err(|e| weave_op_err_to_py(py, e))
    }

    /// Mirrors `Weave._add` — add a single text on top of the weave.
    /// Returns the new index. `parents` is a list of *integer* parent
    /// indices.
    #[pyo3(signature = (version_id, lines, parents, sha1=None, nostore_sha=None))]
    fn _add<'py>(
        &mut self,
        py: Python<'py>,
        version_id: &[u8],
        lines: Bound<'py, PyAny>,
        parents: Bound<'py, PyAny>,
        sha1: Option<&[u8]>,
        nostore_sha: Option<&[u8]>,
    ) -> PyResult<usize> {
        // Validate lines (mirror _check_lines_not_unicode and _check_lines_are_lines).
        let lines_rust: Vec<Vec<u8>> = lines
            .try_iter()?
            .map(|l| -> PyResult<Vec<u8>> {
                let l = l?;
                let b = l
                    .cast_into::<PyBytes>()
                    .map_err(|_| PyTypeError::new_err("lines"))?;
                let bytes = b.as_bytes();
                if bytes.len() > 1 && bytes[..bytes.len() - 1].contains(&b'\n') {
                    return Err(PyValueError::new_err("lines contain newlines"));
                }
                Ok(bytes.to_vec())
            })
            .collect::<PyResult<_>>()?;
        let parent_idxs: Vec<usize> = parents
            .try_iter()?
            .map(|p| -> PyResult<usize> { p?.extract::<usize>() })
            .collect::<PyResult<_>>()?;
        self.inner
            .add(
                Some(version_id),
                &lines_rust,
                &parent_idxs,
                sha1.map(|s| s.to_vec()),
                nostore_sha,
            )
            .map_err(|e| weave_op_err_to_py(py, e))
    }

    /// Mirrors `Weave._add_lines` — add a single text given parent *names*.
    /// Returns `(sha1_bytes, total_size, idx)`. `version_id` may be None;
    /// the Rust core then auto-allocates `b"sha1:" + sha1` as the name.
    #[pyo3(signature = (version_id, parents, lines, parent_texts=None, left_matching_blocks=None, nostore_sha=None, random_id=false, check_content=true))]
    #[allow(clippy::too_many_arguments)]
    fn _add_lines<'py>(
        &mut self,
        py: Python<'py>,
        version_id: Option<&[u8]>,
        parents: Bound<'py, PyAny>,
        lines: Bound<'py, PyAny>,
        parent_texts: Option<Bound<'py, PyAny>>,
        left_matching_blocks: Option<Bound<'py, PyAny>>,
        nostore_sha: Option<&[u8]>,
        random_id: bool,
        check_content: bool,
    ) -> PyResult<(Bound<'py, PyBytes>, usize, usize)> {
        let _ = (parent_texts, left_matching_blocks, random_id);
        let parent_names: Vec<Vec<u8>> = parents
            .try_iter()?
            .map(|p| -> PyResult<Vec<u8>> {
                let p = p?;
                let b = p
                    .cast_into::<PyBytes>()
                    .map_err(|_| PyTypeError::new_err("parents must be bytes"))?;
                Ok(b.as_bytes().to_vec())
            })
            .collect::<PyResult<_>>()?;
        // Bytes-only check is unconditional (we have to be able to copy
        // the lines somewhere); the inline-newline check honours
        // `check_content` because callers sometimes opt out for
        // performance on already-validated input. Mirrors the Python
        // `VersionedFile._check_lines_*` flow.
        let lines_rust: Vec<Vec<u8>> = lines
            .try_iter()?
            .map(|l| -> PyResult<Vec<u8>> {
                let l = l?;
                let b = l
                    .cast_into::<PyBytes>()
                    .map_err(|_| PyTypeError::new_err("lines"))?;
                let bytes = b.as_bytes();
                if check_content && bytes.len() > 1 && bytes[..bytes.len() - 1].contains(&b'\n') {
                    return Err(PyValueError::new_err("lines contain newlines"));
                }
                Ok(bytes.to_vec())
            })
            .collect::<PyResult<_>>()?;
        // Resolve parent names to indices up front so we can call the
        // index-taking `add()` path directly. Falling through `add_lines`
        // would require a bytes name; this _add_lines accepts None so the
        // SHA-based default naming kicks in.
        let mut parent_idxs = Vec::with_capacity(parent_names.len());
        for name in &parent_names {
            parent_idxs.push(self.inner.lookup(name).ok_or_else(|| {
                Python::attach(|py| {
                    RevisionNotPresent::new_err((
                        PyBytes::new(py, name).unbind(),
                        weave_name_for_err(py, self.weave_name.as_ref()),
                    ))
                })
            })?);
        }
        let total: usize = lines_rust.iter().map(|l| l.len()).sum();
        let idx = self
            .inner
            .add(version_id, &lines_rust, &parent_idxs, None, nostore_sha)
            .map_err(|e| weave_op_err_to_py(py, e))?;
        let sha = bazaar::weave::sha_strings(&lines_rust);
        Ok((PyBytes::new(py, &sha), total, idx))
    }

    /// Translate `other`'s parent indices to indices in `self`. Mirrors
    /// `Weave._imported_parents`.
    fn _imported_parents(
        &self,
        py: Python<'_>,
        other: PyRef<'_, Self>,
        other_idx: usize,
    ) -> PyResult<Vec<usize>> {
        self.inner
            .imported_parents(&other.inner, other_idx)
            .map_err(|e| weave_op_err_to_py(py, e))
    }

    /// Cross-check shared version consistency. Mirrors
    /// `Weave._check_version_consistent`.
    fn _check_version_consistent(
        &self,
        py: Python<'_>,
        other: PyRef<'_, Self>,
        other_idx: usize,
        name: &[u8],
    ) -> PyResult<bool> {
        self.inner
            .check_version_consistent(&other.inner, other_idx, name)
            .map_err(|e| weave_op_err_to_py(py, e))
    }

    /// In-place reweave with `other`. Mirrors `Weave._reweave`.
    #[pyo3(signature = (other, _pb=None, _msg=None))]
    fn _reweave(
        &mut self,
        py: Python<'_>,
        other: PyRef<'_, Self>,
        _pb: Option<Py<PyAny>>,
        _msg: Option<String>,
    ) -> PyResult<()> {
        let new = reweave(&self.inner, &other.inner).map_err(|e| weave_op_err_to_py(py, e))?;
        // Match Python `_copy_weave_content` semantics: copy every slot
        // except `_weave_name`.
        self.inner = new;
        Ok(())
    }

    /// Replace the binary contents from a v5 weave file. Used by
    /// `WeaveFile.__init__` after reading the on-disk bytes. The lengths
    /// of `parents`, `sha1s`, and `names` must agree.
    fn _load_from_v5_bytes(&mut self, py: Python<'_>, data: &[u8]) -> PyResult<()> {
        let wf = read_weave_v5(data).map_err(|e| WeaveFormatError::new_err(e.to_string()))?;
        let _ = py;
        self.inner = wf;
        Ok(())
    }

    /// Serialize this weave to v5 bytes. Mirrors `write_weave_v5(self, f)`
    /// but returns the bytes rather than writing.
    fn _to_v5_bytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        Ok(PyBytes::new(py, &write_weave_v5(&self.inner)))
    }

    // ---- test-only mutators: required by per_versionedfile corruption
    // tests. Not part of the public Weave API. Naming kept blunt so they
    // stand out in greps.

    /// Replace the text of an existing literal weave entry. Used only by
    /// tests that simulate on-disk corruption.
    fn _test_corrupt_line(&mut self, idx: usize, bytes: &[u8]) -> PyResult<()> {
        if idx >= self.inner.weave.len() {
            return Err(PyValueError::new_err("idx out of range"));
        }
        match &mut self.inner.weave[idx] {
            WeaveEntry::Line(slot) => {
                *slot = bytes.to_vec();
                Ok(())
            }
            WeaveEntry::Control { .. } => Err(PyValueError::new_err(
                "_test_corrupt_line target is a control instruction, not a literal line",
            )),
        }
    }

    /// Replace a stored sha1. Used only by tests that simulate header
    /// corruption.
    fn _test_corrupt_sha1(&mut self, version: usize, sha: &[u8]) -> PyResult<()> {
        if version >= self.inner.sha1s.len() {
            return Err(PyValueError::new_err("version out of range"));
        }
        self.inner.sha1s[version] = sha.to_vec();
        Ok(())
    }

    /// Yield content factories for `version_keys` in the requested order.
    /// Mirrors `Weave.get_record_stream` from bzrformats/weave.py:
    ///
    /// * each input is a 1-element tuple key `(name,)`
    /// * `ordering` is one of `"unordered"`, `"topological"`,
    ///   `"groupcompress"`
    /// * `include_delta_closure` is accepted for interface parity but
    ///   ignored; this storage doesn't carry deltas
    ///
    /// Versions known to this weave are returned as
    /// [`WeaveContentFactory`]; missing versions are returned as
    /// `bzrformats._bzr_rs.versionedfile.AbsentContentFactory` so the
    /// caller can short-circuit to its absent path.
    ///
    /// Returns an iterator object so callers can use `next()` directly,
    /// matching the original Python generator.
    #[pyo3(signature = (version_keys, ordering, include_delta_closure))]
    fn get_record_stream<'py>(
        slf: Py<Self>,
        py: Python<'py>,
        version_keys: Bound<'py, PyAny>,
        ordering: &str,
        include_delta_closure: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let _ = include_delta_closure;

        // `version_keys` is an iterable of 1-element tuples — extract the
        // last segment of each (matching the Python `version[-1]` idiom).
        let mut names: Vec<Vec<u8>> = Vec::new();
        for item in version_keys.try_iter()? {
            let tup = item?;
            // Accept either a tuple-of-bytes or a bare bytes object;
            // the Python code did `version[-1]` which works for both.
            if let Ok(b) = tup.extract::<&[u8]>() {
                names.push(b.to_vec());
                continue;
            }
            let last = tup.get_item(tup.len()? - 1)?;
            let bytes = last
                .cast_into::<PyBytes>()
                .map_err(|_| PyTypeError::new_err("version key tail must be bytes"))?;
            names.push(bytes.as_bytes().to_vec());
        }

        // Reorder names per `ordering`. Unknown names land at the end,
        // matching the `set(versions).difference(set(parents))` fallback
        // in the Python implementation.
        let weave_ref = slf.borrow(py);
        let ordered_names = order_record_stream(&weave_ref.inner, &names, ordering).ok_or_else(
            || PyValueError::new_err(format!("unknown ordering {:?}", ordering)),
        )?;
        drop(weave_ref);

        // Build the result list: one factory per name.
        let absent_cls = py
            .import("bzrformats._bzr_rs.versionedfile")?
            .getattr("AbsentContentFactory")?;

        let out = PyList::empty(py);
        for name in ordered_names {
            let weave_ref = slf.borrow(py);
            if weave_ref.inner.lookup(&name).is_some() {
                drop(weave_ref);
                // Construct via the public constructor so `key` and
                // `parents` get the same Py-tuple shape map_key expects.
                let factory = WeaveContentFactory::new(py, slf.clone_ref(py), name)?;
                out.append(Py::new(py, factory)?)?;
            } else {
                drop(weave_ref);
                let key = PyTuple::new(py, [PyBytes::new(py, &name)])?;
                let absent = absent_cls.call1((key,))?;
                out.append(absent)?;
            }
        }
        // Wrap the eager list in an iterator object so callers can
        // `next()` it just like the original Python generator.
        out.call_method0("__iter__")
    }
}

/// Streaming content factory wrapping a single version of a [`PyWeave`].
///
/// The Python `Weave.get_record_stream` previously yielded an
/// `WeaveContentFactory` defined in `bzrformats/weave.py` that called
/// back into the weave for every byte access. This Rust port is
/// behaviour-equivalent but holds a `Py<PyWeave>` directly so reads go
/// straight into the Rust core without bouncing through Python.
///
/// `key` and `parents` are mutable Python tuples so wrappers can call
/// `map_key()` to push a partition prefix in place — that's how
/// `ThunkedVersionedFiles` re-tags records as they flow up.
#[pyclass(name = "WeaveContentFactory", module = "bzrformats._bzr_rs.weave")]
pub struct WeaveContentFactory {
    weave: Py<PyWeave>,
    /// Internal version name. `key[-1]` always equals this; `key`
    /// itself may grow a prefix via `map_key`.
    name: Vec<u8>,
    /// Stored sha1 hex digest.
    sha1: Vec<u8>,
    /// Currently-published key. Initialised to `(name,)` and rewritten
    /// by `map_key`.
    key: Py<PyTuple>,
    /// Currently-published parent keys. Initialised to single-element
    /// tuples per parent name; rewritten by `map_key`.
    parents: Py<PyTuple>,
}

#[pymethods]
impl WeaveContentFactory {
    #[new]
    fn new(py: Python<'_>, weave: Py<PyWeave>, name: Vec<u8>) -> PyResult<Self> {
        let weave_ref = weave.borrow(py);
        let idx = weave_ref.inner.lookup(&name).ok_or_else(|| {
            RevisionNotPresent::new_err((PyBytes::new(py, &name).unbind(), py.None()))
        })?;
        let sha1 = weave_ref.inner.sha1s[idx].clone();
        let parent_names: Vec<Vec<u8>> = weave_ref.inner.parents[idx]
            .iter()
            .map(|&p| weave_ref.inner.names[p].clone())
            .collect();
        drop(weave_ref);
        let key = PyTuple::new(py, [PyBytes::new(py, &name)])?.unbind();
        let parent_tuples: Vec<Bound<PyTuple>> = parent_names
            .iter()
            .map(|p| PyTuple::new(py, [PyBytes::new(py, p)]))
            .collect::<PyResult<_>>()?;
        let parents = PyTuple::new(py, parent_tuples)?.unbind();
        Ok(Self {
            weave,
            name,
            sha1,
            key,
            parents,
        })
    }

    #[getter]
    fn sha1<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.sha1)
    }

    /// Size of the fulltext. The original Python class didn't populate
    /// this, returning None; mirror that. Not consulted by callers we
    /// know about, but kept for parity.
    #[getter]
    fn size(&self, py: Python<'_>) -> Py<PyAny> {
        py.None()
    }

    #[getter]
    fn key(&self, py: Python<'_>) -> Py<PyTuple> {
        self.key.clone_ref(py)
    }

    #[getter]
    fn parents(&self, py: Python<'_>) -> Py<PyTuple> {
        self.parents.clone_ref(py)
    }

    #[getter]
    fn storage_kind(&self) -> &'static str {
        "fulltext"
    }

    /// Apply `cb` to the key and to each parent key in place. Mirrors
    /// `ContentFactory.map_key`: used by `ThunkedVersionedFiles` to push
    /// a partition prefix onto the key.
    fn map_key(slf: Py<Self>, py: Python<'_>, cb: Py<PyAny>) -> PyResult<Py<Self>> {
        let mut me = slf.borrow_mut(py);
        let new_key = cb.call1(py, (me.key.bind(py).clone(),))?;
        let new_key = new_key
            .bind(py)
            .clone()
            .cast_into::<PyTuple>()
            .map_err(|_| PyTypeError::new_err("map_key callback must return a tuple"))?;
        me.key = new_key.unbind();

        let parents_bound = me.parents.bind(py).clone();
        let mut new_parents: Vec<Bound<PyTuple>> = Vec::with_capacity(parents_bound.len());
        for parent in parents_bound.iter() {
            let mapped = cb.call1(py, (parent,))?;
            let mapped = mapped
                .bind(py)
                .clone()
                .cast_into::<PyTuple>()
                .map_err(|_| PyTypeError::new_err("map_key callback must return a tuple"))?;
            new_parents.push(mapped);
        }
        me.parents = PyTuple::new(py, new_parents)?.unbind();
        drop(me);
        Ok(slf)
    }

    /// Return the content in the requested encoding. Mirrors
    /// `WeaveContentFactory.get_bytes_as`.
    fn get_bytes_as<'py>(
        &self,
        py: Python<'py>,
        storage_kind: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        match storage_kind {
            "fulltext" => {
                // Concatenate the lines into a single bytes blob.
                let weave_ref = self.weave.borrow(py);
                let idx = weave_ref.inner.lookup(&self.name).ok_or_else(|| {
                    RevisionNotPresent::new_err((
                        PyBytes::new(py, &self.name).unbind(),
                        py.None(),
                    ))
                })?;
                let lines = weave_ref
                    .inner
                    .get_lines(idx)
                    .map_err(|e| weave_op_err_to_py(py, e))?;
                let mut buf: Vec<u8> = Vec::new();
                for line in &lines {
                    buf.extend_from_slice(line);
                }
                Ok(PyBytes::new(py, &buf).into_any())
            }
            "chunked" | "lines" => self
                .get_lines_as_pylist(py)
                .map(|l| l.into_any()),
            other => Err(UnavailableRepresentation::new_err((
                self.key.clone_ref(py),
                other.to_string(),
                "fulltext",
            ))),
        }
    }

    /// Iterate the content lines. Mirrors
    /// `WeaveContentFactory.iter_bytes_as`.
    fn iter_bytes_as<'py>(
        &self,
        py: Python<'py>,
        storage_kind: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        match storage_kind {
            "chunked" | "lines" => {
                // Return an iterator over the lines list. Python's
                // `iter(list)` is fine and matches the original behavior.
                let lines = self.get_lines_as_pylist(py)?;
                Ok(lines.call_method0("__iter__")?)
            }
            other => Err(UnavailableRepresentation::new_err((
                self.key.clone_ref(py),
                other.to_string(),
                "fulltext",
            ))),
        }
    }
}

impl WeaveContentFactory {
    /// Shared helper for the `chunked`/`lines` paths.
    fn get_lines_as_pylist<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let weave_ref = self.weave.borrow(py);
        let idx = weave_ref.inner.lookup(&self.name).ok_or_else(|| {
            RevisionNotPresent::new_err((PyBytes::new(py, &self.name).unbind(), py.None()))
        })?;
        let lines = weave_ref
            .inner
            .get_lines(idx)
            .map_err(|e| weave_op_err_to_py(py, e))?;
        let items: Vec<Bound<PyBytes>> = lines.iter().map(|l| PyBytes::new(py, l)).collect();
        PyList::new(py, items)
    }
}

pub fn _weave_rs(py: Python) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "weave")?;
    m.add_function(wrap_pyfunction!(py_extract, &m)?)?;
    m.add_function(wrap_pyfunction!(py_inclusions, &m)?)?;
    m.add_function(wrap_pyfunction!(py_walk_internal, &m)?)?;
    m.add_function(wrap_pyfunction!(py_read_weave_v5, &m)?)?;
    m.add_function(wrap_pyfunction!(py_write_weave_v5, &m)?)?;
    m.add_function(wrap_pyfunction!(py_weave_add, &m)?)?;
    m.add_class::<PyWeave>()?;
    m.add_class::<WeaveContentFactory>()?;
    Ok(m)
}
