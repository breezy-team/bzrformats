use bazaar::multiparent::{self, Hunk, MultiParent, ParseError};
use pyo3::exceptions::{PyAssertionError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyBytes, PyDict, PyList, PyTuple};
use std::collections::HashMap;

/// Convert the Python hunks list into Rust hunks, borrowing the bytes out of
/// `NewText.lines` and reading integer fields off `ParentText` instances.
fn py_hunks_to_rust(hunks: &Bound<PyList>) -> PyResult<MultiParent> {
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

/// Parse a patch into a list of (kind, payload) tuples. `kind` is `b"n"` for a
/// NewText hunk (payload: list of bytes lines) or `b"p"` for a ParentText hunk
/// (payload: (parent, parent_pos, child_pos, num_lines)). The Python caller
/// materializes these as `NewText` / `ParentText` instances.
#[pyfunction]
fn parse_patch<'py>(py: Python<'py>, data: &[u8]) -> PyResult<Bound<'py, PyList>> {
    let mp = MultiParent::from_patch(data).map_err(parse_error_to_py)?;
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

pub fn _multiparent_rs(py: Python) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "multiparent")?;
    m.add_function(wrap_pyfunction!(to_patch, &m)?)?;
    m.add_function(wrap_pyfunction!(num_lines, &m)?)?;
    m.add_function(wrap_pyfunction!(is_snapshot, &m)?)?;
    m.add_function(wrap_pyfunction!(parse_patch, &m)?)?;
    m.add_function(wrap_pyfunction!(topo_iter, &m)?)?;
    Ok(m)
}
