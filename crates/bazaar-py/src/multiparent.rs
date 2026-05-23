use bazaar::multiparent::{self, Hunk, MultiMemoryVersionedFile, MultiParent, ParseError};
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
    let hunks = hunks.downcast::<PyList>()?;
    py_hunks_to_rust(hunks)
}

#[pyclass(
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
    fn new(snapshot_interval: Option<usize>, max_snapshots: Option<usize>) -> Self {
        Self {
            inner: MultiMemoryVersionedFile::new(snapshot_interval, max_snapshots),
        }
    }

    #[getter]
    fn snapshot_interval(&self) -> Option<usize> {
        self.inner.snapshot_interval()
    }

    #[getter]
    fn max_snapshots(&self) -> Option<usize> {
        self.inner.max_snapshots()
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
            .add_version(lines, key, parents, force_snapshot, single_parent);
        Ok(())
    }

    fn get_line_list<'py>(
        &mut self,
        py: Python<'py>,
        version_ids: Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        let keys = py_iter_to_hashable(&version_ids)?;
        let lines_list = self.inner.get_line_list(&keys);
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
        let lines = self.inner.cache_version(&key).clone();
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
        self.inner.make_snapshot(key);
        Ok(())
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
        let picks = self.inner.select_by_size(num);
        let list = PyList::empty(py);
        for v in &picks {
            list.append(v.bind(py))?;
        }
        Ok(list)
    }

    fn get_size_ranking<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let ranking = self.inner.get_size_ranking();
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

pub fn _multiparent_rs(py: Python) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "multiparent")?;
    m.add_function(wrap_pyfunction!(to_patch, &m)?)?;
    m.add_function(wrap_pyfunction!(num_lines, &m)?)?;
    m.add_function(wrap_pyfunction!(is_snapshot, &m)?)?;
    m.add_function(wrap_pyfunction!(parse_patch, &m)?)?;
    m.add_function(wrap_pyfunction!(topo_iter, &m)?)?;
    m.add_function(wrap_pyfunction!(from_lines_with_blocks, &m)?)?;
    m.add_function(wrap_pyfunction!(from_lines, &m)?)?;
    m.add_class::<PyMultiMemoryVersionedFile>()?;
    Ok(m)
}
