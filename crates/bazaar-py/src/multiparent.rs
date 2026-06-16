use bazaar::multiparent::{
    self, Hunk, MultiMemoryVersionedFile, MultiParent, ParseError, ReconstructError,
};
use pyo3::exceptions::{PyAssertionError, PyKeyError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyBytes, PyDict, PyList, PySet, PyTuple};
use std::collections::HashMap;

/// Convert the Python hunks list into Rust hunks, borrowing the bytes out of
/// `NewText.lines` and reading integer fields off `ParentText` instances.
pub(crate) fn py_hunks_to_rust(hunks: &Bound<PyList>) -> PyResult<MultiParent> {
    let mut out = Vec::with_capacity(hunks.len());
    for hunk in hunks.iter() {
        if let Ok(lines_attr) = hunk.getattr("lines") {
            let mut lines: Vec<Vec<u8>> = Vec::new();
            for line in lines_attr.try_iter()? {
                let line = line?;
                let bytes = line
                    .cast_into::<PyBytes>()
                    .map_err(|_| PyTypeError::new_err("NewText.lines must contain bytes"))?;
                lines.push(bytes.as_bytes().to_vec());
            }
            out.push(Hunk::NewText(lines));
        } else {
            let parent: usize = hunk.getattr("parent")?.extract()?;
            let parent_pos: usize = hunk.getattr("parent_pos")?.extract()?;
            let child_pos: usize = hunk.getattr("child_pos")?.extract()?;
            let num_lines: usize = hunk.getattr("num_lines")?.extract()?;
            out.push(Hunk::ParentText {
                parent,
                parent_pos,
                child_pos,
                num_lines,
            });
        }
    }
    Ok(MultiParent::with_hunks(out))
}

/// Serialize hunks to the multiparent patch wire format.
#[pyfunction]
fn to_patch<'py>(py: Python<'py>, hunks: Bound<'py, PyList>) -> PyResult<Bound<'py, PyList>> {
    let mp = py_hunks_to_rust(&hunks)?;
    let chunks = mp.to_patch();
    let items: Vec<Bound<PyBytes>> = chunks.iter().map(|c| PyBytes::new(py, c)).collect();
    PyList::new(py, items)
}

/// Number of lines in the reconstructed text.
#[pyfunction]
fn num_lines(hunks: Bound<PyList>) -> PyResult<usize> {
    Ok(py_hunks_to_rust(&hunks)?.num_lines())
}

/// True if the hunks represent a fulltext (single NewText hunk).
#[pyfunction]
fn is_snapshot(hunks: Bound<PyList>) -> PyResult<bool> {
    Ok(py_hunks_to_rust(&hunks)?.is_snapshot())
}

/// Convert a `ReconstructError` to a Python exception.
///
/// Both variants surface as `bzrformats.errors.RevisionNotPresent` because
/// both mean "the version we were asked to reconstruct cannot be built
/// from what's in this `MultiMemoryVersionedFile`". Callers in breezy
/// (notably `bundle/serializer/v4.RevisionInstaller.install_revisions`)
/// catch `RevisionNotPresent` to retry against a fallback branch; raising
/// `IndexError` / `KeyError` here bypasses that recovery path.
pub(crate) fn reconstruct_err(e: ReconstructError) -> PyErr {
    Python::attach(|py| {
        let errors = match PyModule::import(py, "bzrformats.errors") {
            Ok(m) => m,
            Err(import_err) => return import_err,
        };
        let cls = match errors.getattr("RevisionNotPresent") {
            Ok(c) => c,
            Err(attr_err) => return attr_err,
        };
        match cls.call1((e.to_string(), py.None())) {
            Ok(exc) => PyErr::from_value(exc),
            Err(call_err) => call_err,
        }
    })
}

fn parse_error_to_py(e: ParseError) -> PyErr {
    match e {
        ParseError::UnexpectedChar(c) => {
            // Match Python's `AssertionError(first_char)` (which received a
            // single-byte bytes object) so callers can't tell the difference.
            Python::attach(|py| PyAssertionError::new_err(PyBytes::new(py, &[c]).unbind()))
        }
        other => PyAssertionError::new_err(other.to_string()),
    }
}

/// Render a `MultiParent`'s hunks as the `(kind, payload)` tuple list shape
/// that the Python wrapper materialises into `NewText` / `ParentText`.
fn hunks_to_py<'py>(py: Python<'py>, mp: MultiParent) -> PyResult<Bound<'py, PyList>> {
    let mut out: Vec<Bound<PyTuple>> = Vec::with_capacity(mp.hunks.len());
    for hunk in mp.hunks {
        match hunk {
            Hunk::NewText(lines) => {
                let py_lines: Vec<Bound<PyBytes>> =
                    lines.iter().map(|l| PyBytes::new(py, l)).collect();
                let lines_list = PyList::new(py, py_lines)?;
                out.push(PyTuple::new(
                    py,
                    [PyBytes::new(py, b"n").into_any(), lines_list.into_any()],
                )?);
            }
            Hunk::ParentText {
                parent,
                parent_pos,
                child_pos,
                num_lines,
            } => {
                let payload = PyTuple::new(py, [parent, parent_pos, child_pos, num_lines])?;
                out.push(PyTuple::new(
                    py,
                    [PyBytes::new(py, b"p").into_any(), payload.into_any()],
                )?);
            }
        }
    }
    PyList::new(py, out)
}

/// Parse a patch into a list of (kind, payload) tuples. `kind` is `b"n"` for a
/// NewText hunk (payload: list of bytes lines) or `b"p"` for a ParentText hunk
/// (payload: (parent, parent_pos, child_pos, num_lines)). The Python caller
/// materializes these as `NewText` / `ParentText` instances.
#[pyfunction]
fn parse_patch<'py>(py: Python<'py>, data: &[u8]) -> PyResult<Bound<'py, PyList>> {
    let mp = MultiParent::from_patch(data).map_err(parse_error_to_py)?;
    hunks_to_py(py, mp)
}

/// Build multi-parent diff hunks from `text` and per-parent matching blocks.
///
/// `text` is the child text as a list of line bytes. `parent_blocks[p]` is
/// the list of `(i, j, n)` matches against parent `p` (typically produced by
/// `patiencediff.PatienceSequenceMatcher.get_matching_blocks`). Returns the
/// hunks in the same `(kind, payload)` shape as [`parse_patch`] so the caller
/// can materialise them into `NewText` / `ParentText` instances.
#[pyfunction]
fn from_lines_with_blocks<'py>(
    py: Python<'py>,
    text: Vec<Vec<u8>>,
    parent_blocks: Vec<Vec<(usize, usize, usize)>>,
) -> PyResult<Bound<'py, PyList>> {
    let mp = MultiParent::from_lines_with_blocks(&text, &parent_blocks);
    hunks_to_py(py, mp)
}

/// Build multi-parent diff hunks from `text` and its `parents`, running
/// patiencediff for each non-skipped parent. `left_blocks`, if supplied,
/// short-circuits the diff against `parents[0]`. Returns the same
/// `(kind, payload)` tuple shape as [`from_lines_with_blocks`].
#[pyfunction]
#[pyo3(signature = (text, parents, left_blocks=None))]
fn from_lines<'py>(
    py: Python<'py>,
    text: Vec<Vec<u8>>,
    parents: Vec<Vec<Vec<u8>>>,
    left_blocks: Option<Vec<(usize, usize, usize)>>,
) -> PyResult<Bound<'py, PyList>> {
    let parent_refs: Vec<&[Vec<u8>]> = parents.iter().map(|p| p.as_slice()).collect();
    let mp = MultiParent::from_lines(&text, &parent_refs, left_blocks);
    hunks_to_py(py, mp)
}

/// A hashable Python object whose `Hash` and `Eq` defer to Python. The
/// interpreter lock is assumed to be held whenever these methods run, since
/// they may execute arbitrary Python code.
struct PyHashable(Py<PyAny>);

impl PyHashable {
    fn new(obj: Bound<'_, PyAny>) -> PyResult<Self> {
        // Fail fast if the value isn't actually hashable.
        obj.hash()?;
        Ok(Self(obj.unbind()))
    }

    fn bind<'py>(&'py self, py: Python<'py>) -> Bound<'py, PyAny> {
        self.0.bind(py).clone()
    }
}

impl Clone for PyHashable {
    fn clone(&self) -> Self {
        Python::attach(|py| Self(self.0.clone_ref(py)))
    }
}

impl std::hash::Hash for PyHashable {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        Python::attach(|py| {
            // hash() was validated in `new`, so this cannot fail for a
            // properly constructed PyHashable — but if it somehow does
            // (e.g. a __hash__ method that started raising), fall back to
            // 0 and let the equality check reject false positives.
            let h = self.0.bind(py).hash().unwrap_or(0);
            h.hash(state);
        })
    }
}

impl PartialEq for PyHashable {
    fn eq(&self, other: &Self) -> bool {
        Python::attach(|py| self.0.bind(py).eq(other.0.bind(py)).unwrap_or(false))
    }
}

impl Eq for PyHashable {}

/// Topologically sort `versions` given a `parents` mapping. Delegates to the
/// generic [`multiparent::topo_iter`] in the pure-Rust crate. Keys may be any
/// hashable Python objects; `parents[v]` is either an iterable of parent
/// keys or `None` for a parentless sentinel.
#[pyfunction]
fn topo_iter<'py>(
    py: Python<'py>,
    parents: Bound<'py, PyDict>,
    versions: Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyList>> {
    let mut versions_rust: Vec<PyHashable> = Vec::new();
    for v in versions.try_iter()? {
        versions_rust.push(PyHashable::new(v?)?);
    }

    let mut parents_rust: HashMap<PyHashable, Option<Vec<PyHashable>>> = HashMap::new();
    for (key, value) in parents.iter() {
        let k = PyHashable::new(key)?;
        let v = if value.is_none() {
            None
        } else {
            let mut ps = Vec::new();
            for p in value.try_iter()? {
                ps.push(PyHashable::new(p?)?);
            }
            Some(ps)
        };
        parents_rust.insert(k, v);
    }

    let ordered = multiparent::topo_iter(&parents_rust, &versions_rust);
    let out = PyList::empty(py);
    for item in ordered {
        out.append(item.bind(py))?;
    }
    Ok(out)
}

/// Build a Python `bzrformats.multiparent.MultiParent(hunks=...)` from a Rust
/// [`MultiParent`]. The hunks list contains real `NewText` / `ParentText`
/// instances, so callers cannot tell this came from Rust.
fn rust_to_py_multiparent<'py>(py: Python<'py>, mp: &MultiParent) -> PyResult<Bound<'py, PyAny>> {
    let module = PyModule::import(py, "bzrformats.multiparent")?;
    let mp_cls = module.getattr("MultiParent")?;
    let new_text_cls = module.getattr("NewText")?;
    let parent_text_cls = module.getattr("ParentText")?;
    let hunks = PyList::empty(py);
    for hunk in &mp.hunks {
        match hunk {
            Hunk::NewText(lines) => {
                let py_lines: Vec<Bound<PyBytes>> =
                    lines.iter().map(|l| PyBytes::new(py, l)).collect();
                let lines_list = PyList::new(py, py_lines)?;
                hunks.append(new_text_cls.call1((lines_list,))?)?;
            }
            Hunk::ParentText {
                parent,
                parent_pos,
                child_pos,
                num_lines,
            } => {
                hunks.append(parent_text_cls.call1((
                    *parent,
                    *parent_pos,
                    *child_pos,
                    *num_lines,
                ))?)?;
            }
        }
    }
    mp_cls.call1((hunks,))
}

/// Pull the `hunks` attribute off a Python MultiParent and convert it into a
/// Rust [`MultiParent`].
fn py_multiparent_to_rust(diff: &Bound<'_, PyAny>) -> PyResult<MultiParent> {
    let hunks = diff.getattr("hunks")?;
    let hunks = hunks.cast::<PyList>()?;
    py_hunks_to_rust(hunks)
}

#[pyclass(
    extends = PyBaseVersionedFile,
    module = "bzrformats._multiparent_rs",
    name = "MultiMemoryVersionedFile"
)]
pub struct PyMultiMemoryVersionedFile {
    inner: MultiMemoryVersionedFile<PyHashable>,
}

#[pymethods]
impl PyMultiMemoryVersionedFile {
    #[new]
    #[pyo3(signature = (snapshot_interval=Some(25), max_snapshots=None))]
    fn new(
        snapshot_interval: Option<usize>,
        max_snapshots: Option<usize>,
    ) -> pyo3::PyClassInitializer<Self> {
        pyo3::PyClassInitializer::from(PyBaseVersionedFile).add_subclass(Self {
            inner: MultiMemoryVersionedFile::new(snapshot_interval, max_snapshots),
        })
    }

    #[getter]
    fn snapshot_interval(&self) -> Option<usize> {
        self.inner.snapshot_interval()
    }

    #[getter]
    fn max_snapshots(&self) -> Option<usize> {
        self.inner.max_snapshots()
    }

    /// Read-only snapshot of the line cache (mirrors the legacy
    /// `_lines` attribute on the Python class). Provided so external
    /// helpers like `_Reconstructor` can keep walking this VF without
    /// changes.
    #[getter]
    fn _lines<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        for (k, lines) in self.inner.lines_cache() {
            let inner = PyList::empty(py);
            for l in lines {
                inner.append(PyBytes::new(py, l))?;
            }
            dict.set_item(k.bind(py), inner)?;
        }
        Ok(dict)
    }

    /// Read-only snapshot of the parent map (mirrors the legacy
    /// `_parents` attribute on the Python class).
    #[getter]
    fn _parents<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        for (k, parents) in self.inner.parents_map() {
            let inner = PyList::empty(py);
            for p in parents {
                inner.append(p.bind(py))?;
            }
            dict.set_item(k.bind(py), inner)?;
        }
        Ok(dict)
    }

    fn versions<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let list = PyList::empty(py);
        for v in self.inner.versions() {
            list.append(v.bind(py))?;
        }
        list.try_iter().map(|i| i.into_any())
    }

    fn has_version(&self, version: Bound<'_, PyAny>) -> PyResult<bool> {
        let key = PyHashable::new(version)?;
        Ok(self.inner.has_version(&key))
    }

    fn add_diff(
        &mut self,
        diff: Bound<'_, PyAny>,
        version_id: Bound<'_, PyAny>,
        parent_ids: Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let mp = py_multiparent_to_rust(&diff)?;
        let key = PyHashable::new(version_id)?;
        let parents = py_iter_to_hashable(&parent_ids)?;
        self.inner.add_diff(mp, key, parents);
        Ok(())
    }

    fn get_diff<'py>(
        &self,
        py: Python<'py>,
        version_id: Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let key = PyHashable::new(version_id.clone())?;
        match self.inner.get_diff(&key) {
            Some(mp) => rust_to_py_multiparent(py, mp),
            None => {
                // Python raises errors.RevisionNotPresent here; mirror that.
                let errors = PyModule::import(py, "bzrformats.errors")?;
                let exc = errors.getattr("RevisionNotPresent")?;
                Err(PyErr::from_value(exc.call1((version_id, py.None()))?))
            }
        }
    }

    fn get_parents<'py>(
        &self,
        py: Python<'py>,
        version_id: Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        let key = PyHashable::new(version_id)?;
        match self.inner.get_parents(&key) {
            Some(parents) => {
                let list = PyList::empty(py);
                for p in parents {
                    list.append(p.bind(py))?;
                }
                Ok(list)
            }
            None => Err(PyKeyError::new_err("unknown version")),
        }
    }

    #[pyo3(signature = (lines, version_id, parent_ids, force_snapshot=None, single_parent=false))]
    fn add_version(
        &mut self,
        lines: Vec<Vec<u8>>,
        version_id: Bound<'_, PyAny>,
        parent_ids: Bound<'_, PyAny>,
        force_snapshot: Option<bool>,
        single_parent: bool,
    ) -> PyResult<()> {
        let key = PyHashable::new(version_id)?;
        let parents = py_iter_to_hashable(&parent_ids)?;
        self.inner
            .add_version(lines, key, parents, force_snapshot, single_parent)
            .map_err(reconstruct_err)
    }

    fn get_line_list<'py>(
        &mut self,
        py: Python<'py>,
        version_ids: Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        let keys = py_iter_to_hashable(&version_ids)?;
        let lines_list = self.inner.get_line_list(&keys).map_err(reconstruct_err)?;
        let outer = PyList::empty(py);
        for lines in lines_list {
            let inner: Vec<Bound<PyBytes>> = lines.iter().map(|l| PyBytes::new(py, l)).collect();
            outer.append(PyList::new(py, inner)?)?;
        }
        Ok(outer)
    }

    fn cache_version<'py>(
        &mut self,
        py: Python<'py>,
        version_id: Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        let key = PyHashable::new(version_id)?;
        let lines = self
            .inner
            .cache_version(&key)
            .map_err(reconstruct_err)?
            .to_vec();
        let inner: Vec<Bound<PyBytes>> = lines.iter().map(|l| PyBytes::new(py, l)).collect();
        PyList::new(py, inner)
    }

    fn do_snapshot(
        &self,
        version_id: Bound<'_, PyAny>,
        parent_ids: Bound<'_, PyAny>,
    ) -> PyResult<bool> {
        let key = PyHashable::new(version_id)?;
        let parents = py_iter_to_hashable(&parent_ids)?;
        Ok(self.inner.do_snapshot(&key, &parents))
    }

    fn clear_cache(&mut self) {
        self.inner.clear_cache();
    }

    fn make_snapshot(&mut self, version_id: Bound<'_, PyAny>) -> PyResult<()> {
        let key = PyHashable::new(version_id)?;
        self.inner.make_snapshot(key).map_err(reconstruct_err)
    }

    fn import_diffs(&mut self, other: &Self) {
        self.inner.import_diffs(&other.inner);
    }

    fn snapshots<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PySet>> {
        let s = PySet::empty(py)?;
        for v in self.inner.snapshots() {
            s.add(v.bind(py))?;
        }
        Ok(s)
    }

    fn select_snapshots<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PySet>> {
        let s = PySet::empty(py)?;
        for v in self.inner.select_snapshots() {
            s.add(v.bind(py))?;
        }
        Ok(s)
    }

    fn select_by_size<'py>(&mut self, py: Python<'py>, num: usize) -> PyResult<Bound<'py, PyList>> {
        let picks = self.inner.select_by_size(num).map_err(reconstruct_err)?;
        let list = PyList::empty(py);
        for v in &picks {
            list.append(v.bind(py))?;
        }
        Ok(list)
    }

    fn get_size_ranking<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let ranking = self.inner.get_size_ranking().map_err(reconstruct_err)?;
        let list = PyList::empty(py);
        for (score, v) in &ranking {
            let tup = PyTuple::new(
                py,
                [score.into_pyobject(py)?.into_any(), v.bind(py).into_any()],
            )?;
            list.append(tup)?;
        }
        Ok(list)
    }

    fn get_build_ranking<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let ranking = self.inner.get_build_ranking();
        let list = PyList::empty(py);
        for v in &ranking {
            list.append(v.bind(py))?;
        }
        Ok(list)
    }

    /// Clears all stored diffs (mirrors Python's `destroy`).
    fn destroy(&mut self) {
        self.inner = MultiMemoryVersionedFile::new(
            self.inner.snapshot_interval(),
            self.inner.max_snapshots(),
        );
    }
}

fn py_iter_to_hashable(obj: &Bound<'_, PyAny>) -> PyResult<Vec<PyHashable>> {
    let mut out = Vec::new();
    for item in obj.try_iter()? {
        out.push(PyHashable::new(item?)?);
    }
    Ok(out)
}

/// A run of new lines introduced by a text (no parent reference).
#[pyclass(name = "NewText", module = "bzrformats._bzr_rs.multiparent")]
pub struct NewText {
    #[pyo3(get, set)]
    lines: Py<PyAny>,
}

#[pymethods]
impl NewText {
    #[new]
    fn new(lines: Py<PyAny>) -> Self {
        NewText { lines }
    }

    fn __eq__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<bool> {
        let Ok(other) = other.cast::<NewText>() else {
            return Ok(false);
        };
        self.lines.bind(py).eq(&other.borrow().lines)
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!("NewText({})", self.lines.bind(py).repr()?))
    }

    /// Generate patch lines for this NewText.
    fn to_patch<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let lines = self.lines.bind(py).try_iter()?;
        let mut count = 0usize;
        let mut body: Vec<Py<PyAny>> = Vec::new();
        for line in lines {
            body.push(line?.unbind());
            count += 1;
        }
        let out = PyList::empty(py);
        out.append(PyBytes::new(py, format!("i {}\n", count).as_bytes()))?;
        for line in body {
            out.append(line)?;
        }
        out.append(PyBytes::new(py, b"\n"))?;
        Ok(out)
    }
}

/// A reference to a slice of lines present in a parent text.
#[pyclass(
    name = "ParentText",
    module = "bzrformats._bzr_rs.multiparent",
    skip_from_py_object
)]
#[derive(Clone)]
pub struct ParentText {
    #[pyo3(get, set)]
    parent: usize,
    #[pyo3(get, set)]
    parent_pos: usize,
    #[pyo3(get, set)]
    child_pos: usize,
    #[pyo3(get, set)]
    num_lines: usize,
}

#[pymethods]
impl ParentText {
    #[new]
    fn new(parent: usize, parent_pos: usize, child_pos: usize, num_lines: usize) -> Self {
        ParentText {
            parent,
            parent_pos,
            child_pos,
            num_lines,
        }
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> bool {
        match other.cast::<ParentText>() {
            Ok(o) => {
                let o = o.borrow();
                self.parent == o.parent
                    && self.parent_pos == o.parent_pos
                    && self.child_pos == o.child_pos
                    && self.num_lines == o.num_lines
            }
            Err(_) => false,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "ParentText({}, {}, {}, {})",
            self.parent, self.parent_pos, self.child_pos, self.num_lines
        )
    }

    /// Generate patch lines for this ParentText.
    fn to_patch<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let line = format!(
            "c {} {} {} {}\n",
            self.parent, self.parent_pos, self.child_pos, self.num_lines
        );
        PyList::new(py, [PyBytes::new(py, line.as_bytes())])
    }
}

/// A multi-parent diff: an ordered list of `NewText` / `ParentText` hunks.
#[pyclass(name = "MultiParent", module = "bzrformats._bzr_rs.multiparent")]
pub struct PyMultiParent {
    /// The hunks, kept as a live Python list so callers can mutate it
    /// (`diff.hunks.append(...)`).
    #[pyo3(get, set)]
    hunks: Py<PyList>,
}

impl PyMultiParent {
    fn hunks_list<'py>(&self, py: Python<'py>) -> Bound<'py, PyList> {
        self.hunks.bind(py).clone()
    }
}

#[pymethods]
impl PyMultiParent {
    #[new]
    #[pyo3(signature = (hunks=None))]
    fn new(py: Python<'_>, hunks: Option<Bound<'_, PyAny>>) -> PyResult<Self> {
        let hunks = match hunks {
            Some(h) if !h.is_none() => {
                if let Ok(list) = h.cast::<PyList>() {
                    list.clone().unbind()
                } else {
                    // Accept any iterable, materialising into a list.
                    let list = PyList::empty(py);
                    for item in h.try_iter()? {
                        list.append(item?)?;
                    }
                    list.unbind()
                }
            }
            _ => PyList::empty(py).unbind(),
        };
        Ok(PyMultiParent { hunks })
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        Ok(format!("MultiParent({})", self.hunks.bind(py).repr()?))
    }

    fn __eq__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<bool> {
        let Ok(other) = other.cast::<PyMultiParent>() else {
            return Ok(false);
        };
        self.hunks.bind(py).eq(&other.borrow().hunks)
    }

    /// Produce a MultiParent from a list of lines and parents.
    #[staticmethod]
    #[pyo3(signature = (text, parents=None, left_blocks=None))]
    fn from_lines(
        py: Python<'_>,
        text: Bound<'_, PyAny>,
        parents: Option<Bound<'_, PyAny>>,
        left_blocks: Option<Vec<(usize, usize, usize)>>,
    ) -> PyResult<Py<Self>> {
        let text: Vec<Vec<u8>> = extract_lines(&text)?;
        let parents: Vec<Vec<Vec<u8>>> = match parents {
            Some(ps) if !ps.is_none() => {
                let mut out = Vec::new();
                for p in ps.try_iter()? {
                    out.push(extract_lines(&p?)?);
                }
                out
            }
            _ => Vec::new(),
        };
        let parent_refs: Vec<&[Vec<u8>]> = parents.iter().map(|p| p.as_slice()).collect();
        let mp = MultiParent::from_lines(&text, &parent_refs, left_blocks);
        Self::from_rust(py, mp)
    }

    /// Produce a MultiParent from a text and list of parent texts.
    #[classmethod]
    #[pyo3(signature = (text, parents=None))]
    fn from_texts(
        cls: &Bound<'_, pyo3::types::PyType>,
        py: Python<'_>,
        text: &[u8],
        parents: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<Self>> {
        let _ = cls;
        let text_lines = split_readlines(text);
        let parent_lists: Vec<Vec<Vec<u8>>> = match parents {
            Some(ps) if !ps.is_none() => {
                let mut out = Vec::new();
                for p in ps.try_iter()? {
                    let p = p?;
                    let bytes = p.cast::<PyBytes>()?;
                    out.push(split_readlines(bytes.as_bytes()));
                }
                out
            }
            _ => Vec::new(),
        };
        let parent_refs: Vec<&[Vec<u8>]> = parent_lists.iter().map(|p| p.as_slice()).collect();
        let mp = MultiParent::from_lines(&text_lines, &parent_refs, None);
        Self::from_rust(py, mp)
    }

    /// Create a MultiParent from its serialised patch form.
    #[classmethod]
    fn from_patch(
        cls: &Bound<'_, pyo3::types::PyType>,
        py: Python<'_>,
        text: &[u8],
    ) -> PyResult<Py<Self>> {
        let _ = cls;
        let mp = MultiParent::from_patch(text).map_err(parse_error_to_py)?;
        Self::from_rust(py, mp)
    }

    /// Yield text lines for a patch.
    fn to_patch<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let mp = py_hunks_to_rust(&self.hunks_list(py))?;
        let chunks = mp.to_patch();
        let items: Vec<Bound<PyBytes>> = chunks.iter().map(|c| PyBytes::new(py, c)).collect();
        PyList::new(py, items)
    }

    /// The length of the patch in bytes.
    fn patch_len(&self, py: Python<'_>) -> PyResult<usize> {
        let mp = py_hunks_to_rust(&self.hunks_list(py))?;
        Ok(mp.to_patch().iter().map(|c| c.len()).sum())
    }

    /// The length of the gzipped patch in bytes.
    fn zipped_patch_len(&self, py: Python<'_>) -> PyResult<usize> {
        let mp = py_hunks_to_rust(&self.hunks_list(py))?;
        Ok(mp.zipped_patch_len())
    }

    /// The number of lines in the output text.
    fn num_lines(&self, py: Python<'_>) -> PyResult<usize> {
        Ok(py_hunks_to_rust(&self.hunks_list(py))?.num_lines())
    }

    /// True if these hunks represent a fulltext (single NewText hunk).
    fn is_snapshot(&self, py: Python<'_>) -> PyResult<bool> {
        Ok(py_hunks_to_rust(&self.hunks_list(py))?.is_snapshot())
    }

    /// Yield `(parent_pos, child_pos, num_lines)` matches for `parent`, ending
    /// with the `(parent_len, num_lines, 0)` sentinel.
    fn get_matching_blocks<'py>(
        &self,
        py: Python<'py>,
        parent: usize,
        parent_len: usize,
    ) -> PyResult<Bound<'py, PyList>> {
        let out = PyList::empty(py);
        for hunk in self.hunks_list(py).iter() {
            if let Ok(pt) = hunk.cast::<ParentText>() {
                let pt = pt.borrow();
                if pt.parent == parent {
                    out.append((pt.parent_pos, pt.child_pos, pt.num_lines))?;
                }
            }
        }
        out.append((parent_len, self.num_lines(py)?, 0usize))?;
        Ok(out)
    }

    /// Iterate the hunks with `(start, end, kind, data)` ranges. `kind` is
    /// `"new"` (data: list of lines) or `"parent"` (data:
    /// `(parent, parent_start, parent_end)`). Returns a true iterator (callers
    /// such as `_Reconstructor` call `next()` on it).
    fn range_iterator<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let out = PyList::empty(py);
        let mut start: usize = 0;
        for hunk in self.hunks_list(py).iter() {
            let (start_v, end, kind, data): (usize, usize, &str, Py<PyAny>) =
                if let Ok(nt) = hunk.cast::<NewText>() {
                    let lines = nt.borrow().lines.bind(py).clone();
                    let len = lines.len()?;
                    (start, start + len, "new", lines.unbind())
                } else {
                    let pt = hunk.cast::<ParentText>()?.borrow();
                    let s = pt.child_pos;
                    let data = (pt.parent, pt.parent_pos, pt.parent_pos + pt.num_lines)
                        .into_pyobject(py)?;
                    (s, s + pt.num_lines, "parent", data.into_any().unbind())
                };
            out.append((start_v, end, kind, data))?;
            start = end;
        }
        Ok(out.into_any().try_iter()?.into_any())
    }

    /// Reconstruct a fulltext from this diff and its parents.
    #[pyo3(signature = (parents=None))]
    fn to_lines<'py>(
        &self,
        py: Python<'py>,
        parents: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyList>> {
        let parent_lists: Vec<Vec<Vec<u8>>> = match parents {
            Some(ps) if !ps.is_none() => {
                let mut out = Vec::new();
                for p in ps.try_iter()? {
                    let p = p?;
                    let bytes = p.cast::<PyBytes>()?;
                    out.push(split_readlines(bytes.as_bytes()));
                }
                out
            }
            _ => Vec::new(),
        };
        let mp = py_hunks_to_rust(&self.hunks_list(py))?;
        // Mirror the Python `to_lines`: register each parent as its own
        // version (keyed by its index) in a scratch MultiMemoryVersionedFile,
        // add this diff referencing all parents, then reconstruct it. The diff
        // version is keyed -1 to stay distinct from the parent indices.
        let mut mpvf: MultiMemoryVersionedFile<i64> = MultiMemoryVersionedFile::new(Some(25), None);
        for (num, parent) in parent_lists.into_iter().enumerate() {
            mpvf.add_version(parent, num as i64, Vec::new(), None, false)
                .map_err(reconstruct_err)?;
        }
        let parent_ids: Vec<i64> = (0..mpvf.versions().count() as i64).collect();
        mpvf.add_diff(mp, -1, parent_ids);
        let lines = mpvf.get_line_list(&[-1]).map_err(reconstruct_err)?;
        let items: Vec<Bound<PyBytes>> = lines[0].iter().map(|l| PyBytes::new(py, l)).collect();
        PyList::new(py, items)
    }
}

impl PyMultiParent {
    /// Build a Python `MultiParent` (with real NewText/ParentText hunks) from a
    /// Rust [`MultiParent`].
    fn from_rust(py: Python<'_>, mp: MultiParent) -> PyResult<Py<Self>> {
        let hunks = PyList::empty(py);
        for hunk in mp.hunks {
            match hunk {
                Hunk::NewText(lines) => {
                    let py_lines: Vec<Bound<PyBytes>> =
                        lines.iter().map(|l| PyBytes::new(py, l)).collect();
                    let lines_list = PyList::new(py, py_lines)?;
                    hunks.append(Py::new(
                        py,
                        NewText {
                            lines: lines_list.into_any().unbind(),
                        },
                    )?)?;
                }
                Hunk::ParentText {
                    parent,
                    parent_pos,
                    child_pos,
                    num_lines,
                } => {
                    hunks.append(Py::new(
                        py,
                        ParentText {
                            parent,
                            parent_pos,
                            child_pos,
                            num_lines,
                        },
                    )?)?;
                }
            }
        }
        Py::new(
            py,
            PyMultiParent {
                hunks: hunks.unbind(),
            },
        )
    }
}

/// Extract a list of `bytes` lines from a Python iterable of bytes.
fn extract_lines(obj: &Bound<'_, PyAny>) -> PyResult<Vec<Vec<u8>>> {
    let mut out = Vec::new();
    for line in obj.try_iter()? {
        let line = line?;
        out.push(line.cast::<PyBytes>()?.as_bytes().to_vec());
    }
    Ok(out)
}

/// Split a byte string into lines the way `BytesIO(text).readlines()` does:
/// each line keeps its trailing newline; a final line without a newline is
/// kept as-is.
fn split_readlines(data: &[u8]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut start = 0;
    for (i, &b) in data.iter().enumerate() {
        if b == b'\n' {
            out.push(data[start..=i].to_vec());
            start = i + 1;
        }
    }
    if start < data.len() {
        out.push(data[start..].to_vec());
    }
    out
}

/// Disk-backed pseudo-versionedfile, ported from
/// `bzrformats.multiparent.MultiVersionedFile`.
#[pyclass(
    extends = PyBaseVersionedFile,
    name = "MultiVersionedFile",
    module = "bzrformats._bzr_rs.multiparent"
)]
pub struct PyMultiVersionedFile {
    inner: bazaar::multiparent::DiskMultiVersionedFile,
}

fn disk_err_to_py(e: bazaar::multiparent::DiskError) -> PyErr {
    match e {
        bazaar::multiparent::DiskError::Reconstruct(r) => reconstruct_err(r),
        bazaar::multiparent::DiskError::Io(io) => {
            pyo3::exceptions::PyOSError::new_err(io.to_string())
        }
    }
}

#[pymethods]
impl PyMultiVersionedFile {
    #[new]
    #[pyo3(signature = (filename, snapshot_interval=25, max_snapshots=None))]
    fn new(
        filename: String,
        snapshot_interval: Option<usize>,
        max_snapshots: Option<usize>,
    ) -> pyo3::PyClassInitializer<Self> {
        pyo3::PyClassInitializer::from(PyBaseVersionedFile).add_subclass(Self {
            inner: bazaar::multiparent::DiskMultiVersionedFile::new(
                filename,
                snapshot_interval,
                max_snapshots,
            ),
        })
    }

    #[pyo3(signature = (lines, version_id, parent_ids, force_snapshot=None, single_parent=false))]
    fn add_version(
        &mut self,
        lines: Vec<Vec<u8>>,
        version_id: Vec<u8>,
        parent_ids: Vec<Vec<u8>>,
        force_snapshot: Option<bool>,
        single_parent: bool,
    ) -> PyResult<()> {
        self.inner
            .add_version(lines, version_id, parent_ids, force_snapshot, single_parent)
            .map_err(disk_err_to_py)
    }

    fn get_line_list<'py>(
        &mut self,
        py: Python<'py>,
        version_ids: Vec<Vec<u8>>,
    ) -> PyResult<Bound<'py, PyList>> {
        let lists = self
            .inner
            .get_line_list(&version_ids)
            .map_err(reconstruct_err)?;
        let outer = PyList::empty(py);
        for lines in lists {
            let inner: Vec<Bound<PyBytes>> = lines.iter().map(|l| PyBytes::new(py, l)).collect();
            outer.append(PyList::new(py, inner)?)?;
        }
        Ok(outer)
    }

    fn get_diff<'py>(&self, py: Python<'py>, version_id: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        let mp = self
            .inner
            .read_diff_from_disk(&version_id)
            .map_err(|e| pyo3::exceptions::PyOSError::new_err(e.to_string()))?;
        rust_to_py_multiparent(py, &mp)
    }

    fn save(&self) -> PyResult<()> {
        self.inner
            .save()
            .map_err(|e| pyo3::exceptions::PyOSError::new_err(e.to_string()))
    }

    fn load(&mut self) -> PyResult<()> {
        self.inner
            .load()
            .map_err(|e| pyo3::exceptions::PyOSError::new_err(e.to_string()))
    }

    fn destroy(&self) -> PyResult<()> {
        self.inner
            .destroy()
            .map_err(|e| pyo3::exceptions::PyOSError::new_err(e.to_string()))
    }
}

/// Pseudo-VersionedFile base class for the MultiParent backends.
///
/// Port of the `bzrformats.multiparent.BaseVersionedFile` skeleton. The
/// concrete backends (`MultiMemoryVersionedFile`, `MultiVersionedFile`) each
/// keep their own Rust-backed state and reimplement the skeleton methods, so
/// this base carries no state of its own; it exists so the backends share a
/// common type and `isinstance` behaviour matches the original Python
/// hierarchy. breezy re-exports it for API compatibility.
#[pyclass(
    subclass,
    name = "BaseVersionedFile",
    module = "bzrformats._bzr_rs.multiparent"
)]
pub struct PyBaseVersionedFile;

#[pymethods]
impl PyBaseVersionedFile {
    #[new]
    #[pyo3(signature = (snapshot_interval=25, max_snapshots=None))]
    fn new(snapshot_interval: Option<usize>, max_snapshots: Option<usize>) -> Self {
        let _ = (snapshot_interval, max_snapshots);
        PyBaseVersionedFile
    }
}

/// Gzip-compress an iterable of `bytes` lines into a single gzip container.
#[pyfunction]
fn gzip_string<'py>(py: Python<'py>, lines: Bound<'py, PyAny>) -> PyResult<Bound<'py, PyBytes>> {
    let lines = extract_lines(&lines)?;
    let compressed = multiparent::gzip_string(lines.iter().map(Vec::as_slice));
    Ok(PyBytes::new(py, &compressed))
}

/// Build a text from the diffs, ancestry graph and cached lines.
///
/// Port of `bzrformats.multiparent._Reconstructor`. `diffs` is any object
/// with a `get_diff(version_id)` method returning a `MultiParent` (whose
/// `range_iterator()` yields `(start, end, kind, data)`); `lines` and
/// `parents` are the version->lines and version->parent-ids maps (typically a
/// backend's `_lines` / `_parents`).
#[pyclass(name = "_Reconstructor", module = "bzrformats._bzr_rs.multiparent")]
pub struct PyReconstructor {
    diffs: Py<PyAny>,
    lines: Py<PyAny>,
    parents: Py<PyAny>,
    /// Per-version cursor: version_id -> (start, end, kind, data, iterator).
    cursor: HashMap<PyHashable, Py<PyTuple>>,
}

#[pymethods]
impl PyReconstructor {
    #[new]
    fn new(diffs: Py<PyAny>, lines: Py<PyAny>, parents: Py<PyAny>) -> Self {
        Self {
            diffs,
            lines,
            parents,
            cursor: HashMap::new(),
        }
    }

    /// Append the lines referred to by a `ParentText` to `lines`.
    fn reconstruct(
        &mut self,
        py: Python<'_>,
        lines: &Bound<'_, PyAny>,
        parent_text: &Bound<'_, PyAny>,
        version_id: Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let parent_idx: usize = parent_text.getattr("parent")?.extract()?;
        let parent_pos: usize = parent_text.getattr("parent_pos")?.extract()?;
        let num_lines: usize = parent_text.getattr("num_lines")?.extract()?;
        let parent_id = self
            .parents
            .bind(py)
            .get_item(&version_id)?
            .get_item(parent_idx)?;
        let end = parent_pos + num_lines;
        self._reconstruct(py, lines, parent_id, parent_pos, end)
    }

    /// Append lines for the requested `version_id` range `[req_start, req_end)`.
    fn _reconstruct(
        &mut self,
        py: Python<'_>,
        lines: &Bound<'_, PyAny>,
        req_version_id: Bound<'_, PyAny>,
        req_start: usize,
        req_end: usize,
    ) -> PyResult<()> {
        if req_start == req_end {
            return Ok(());
        }
        let mut pending: Vec<(Py<PyAny>, usize, usize)> =
            vec![(req_version_id.unbind(), req_start, req_end)];
        let lines_map = self.lines.bind(py);
        let parents_map = self.parents.bind(py);
        let diffs = self.diffs.bind(py);
        while let Some((version_obj, req_start, mut req_end)) = pending.pop() {
            let version = version_obj.bind(py);
            // Already-cached fulltext: slice straight out of it.
            if lines_map.contains(version)? {
                let cached = lines_map.get_item(version)?;
                let slice = cached.get_item(pyo3::types::PySlice::new(
                    py,
                    req_start as isize,
                    req_end as isize,
                    1,
                ))?;
                lines.call_method1("extend", (slice,))?;
                continue;
            }
            let key = PyHashable::new(version.clone())?;
            let (mut start, mut end, mut kind, mut data, mut iterator) = match self.cursor.get(&key)
            {
                Some(t) => unpack_cursor(t.bind(py))?,
                None => {
                    let it = diffs
                        .call_method1("get_diff", (version,))?
                        .call_method0("range_iterator")?;
                    let (s, e, k, d) = next_range(&it)?;
                    (s, e, k, d, it)
                }
            };
            // A stored cursor may be ahead of this request; restart the
            // iterator from the top in that case.
            if start > req_start {
                iterator = diffs
                    .call_method1("get_diff", (version,))?
                    .call_method0("range_iterator")?;
                let (s, e, k, d) = next_range(&iterator)?;
                start = s;
                end = e;
                kind = k;
                data = d;
            }
            // Advance to the first hunk relevant to the request.
            while end <= req_start {
                let (s, e, k, d) = next_range(&iterator)?;
                start = s;
                end = e;
                kind = k;
                data = d;
            }
            self.cursor.insert(
                key.clone(),
                pack_cursor(py, start, end, &kind, &data, &iterator)?,
            );
            // Split the request if the current hunk can't satisfy it all.
            if req_end > end {
                pending.push((version.clone().unbind(), end, req_end));
                req_end = end;
            }
            if kind == "new" {
                let slice = data.bind(py).get_item(pyo3::types::PySlice::new(
                    py,
                    (req_start - start) as isize,
                    (req_end - start) as isize,
                    1,
                ))?;
                lines.call_method1("extend", (slice,))?;
            } else {
                // ParentText: rewrite as a range request against the parent.
                let tup = data.bind(py);
                let parent_idx: usize = tup.get_item(0)?.extract()?;
                let parent_start: usize = tup.get_item(1)?.extract()?;
                let parent_end: usize = tup.get_item(2)?.extract()?;
                let new_version = parents_map.get_item(version)?.get_item(parent_idx)?;
                let new_start = parent_start + req_start - start;
                let new_end = parent_end + req_end - end;
                pending.push((new_version.unbind(), new_start, new_end));
            }
        }
        Ok(())
    }

    fn reconstruct_version(
        &mut self,
        py: Python<'_>,
        lines: &Bound<'_, PyAny>,
        version_id: Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let length: usize = self
            .diffs
            .bind(py)
            .call_method1("get_diff", (&version_id,))?
            .call_method0("num_lines")?
            .extract()?;
        self._reconstruct(py, lines, version_id, 0, length)
    }
}

/// Pull the next `(start, end, kind, data)` range off a `range_iterator`.
fn next_range(iterator: &Bound<'_, PyAny>) -> PyResult<(usize, usize, String, Py<PyAny>)> {
    let item = iterator.call_method0("__next__")?;
    let start: usize = item.get_item(0)?.extract()?;
    let end: usize = item.get_item(1)?.extract()?;
    let kind: String = item.get_item(2)?.extract()?;
    let data = item.get_item(3)?.unbind();
    Ok((start, end, kind, data))
}

/// Pack a cursor tuple `(start, end, kind, data, iterator)`.
fn pack_cursor(
    py: Python<'_>,
    start: usize,
    end: usize,
    kind: &str,
    data: &Py<PyAny>,
    iterator: &Bound<'_, PyAny>,
) -> PyResult<Py<PyTuple>> {
    Ok(PyTuple::new(
        py,
        [
            start.into_pyobject(py)?.into_any(),
            end.into_pyobject(py)?.into_any(),
            kind.into_pyobject(py)?.into_any(),
            data.bind(py).clone(),
            iterator.clone(),
        ],
    )?
    .unbind())
}

/// Unpack a stored cursor tuple back into its parts.
fn unpack_cursor<'py>(
    t: &Bound<'py, PyAny>,
) -> PyResult<(usize, usize, String, Py<PyAny>, Bound<'py, PyAny>)> {
    let start: usize = t.get_item(0)?.extract()?;
    let end: usize = t.get_item(1)?.extract()?;
    let kind: String = t.get_item(2)?.extract()?;
    let data = t.get_item(3)?.unbind();
    let iterator = t.get_item(4)?;
    Ok((start, end, kind, data, iterator))
}

pub fn _multiparent_rs(py: Python) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "multiparent")?;
    m.add_class::<PyMultiVersionedFile>()?;
    m.add_function(wrap_pyfunction!(to_patch, &m)?)?;
    m.add_function(wrap_pyfunction!(num_lines, &m)?)?;
    m.add_function(wrap_pyfunction!(is_snapshot, &m)?)?;
    m.add_function(wrap_pyfunction!(parse_patch, &m)?)?;
    m.add_function(wrap_pyfunction!(topo_iter, &m)?)?;
    m.add_function(wrap_pyfunction!(from_lines_with_blocks, &m)?)?;
    m.add_function(wrap_pyfunction!(from_lines, &m)?)?;
    m.add_function(wrap_pyfunction!(gzip_string, &m)?)?;
    m.add_class::<PyBaseVersionedFile>()?;
    m.add_class::<PyReconstructor>()?;
    m.add_class::<PyMultiMemoryVersionedFile>()?;
    m.add_class::<PyMultiParent>()?;
    m.add_class::<NewText>()?;
    m.add_class::<ParentText>()?;
    Ok(m)
}
