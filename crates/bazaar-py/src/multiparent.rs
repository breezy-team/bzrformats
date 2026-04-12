use bazaar::multiparent::{Hunk, MultiParent, ParseError};
use pyo3::exceptions::{PyAssertionError, PyTypeError};
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyBytes, PyDict, PyList, PySet, PyTuple};

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

/// Topologically sort `versions` given a `parents` mapping.
///
/// `parents[v]` is either an iterable of parent keys or `None` for a
/// "parentless" sentinel (treated as having no parents). Keys may be any
/// hashable Python objects. Returns versions in an order where every version
/// appears after its parents that are present in the input set.
#[pyfunction]
fn topo_iter<'py>(
    py: Python<'py>,
    parents: Bound<'py, PyDict>,
    versions: Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyList>> {
    let versions_set = PySet::empty(py)?;
    let versions_order = PyList::empty(py);
    for v in versions.try_iter()? {
        let v = v?;
        if !versions_set.contains(&v)? {
            versions_set.add(&v)?;
            versions_order.append(&v)?;
        }
    }

    let seen = PySet::empty(py)?;
    let descendants = PyDict::new(py);

    let pending_count = |v: &Bound<'py, PyAny>| -> PyResult<usize> {
        let ps = parents.get_item(v)?.ok_or_else(|| {
            pyo3::exceptions::PyKeyError::new_err("version missing from parents map")
        })?;
        if ps.is_none() {
            return Ok(0);
        }
        let mut count = 0usize;
        for p in ps.try_iter()? {
            let p = p?;
            if versions_set.contains(&p)? && !seen.contains(&p)? {
                count += 1;
            }
        }
        Ok(count)
    };

    for v in versions_order.iter() {
        let ps = parents.get_item(&v)?.ok_or_else(|| {
            pyo3::exceptions::PyKeyError::new_err("version missing from parents map")
        })?;
        if ps.is_none() {
            continue;
        }
        for p in ps.try_iter()? {
            let p = p?;
            let existing = descendants.get_item(&p)?;
            match existing {
                Some(list) => {
                    list.cast::<PyList>()?.append(&v)?;
                }
                None => {
                    let list = PyList::empty(py);
                    list.append(&v)?;
                    descendants.set_item(&p, list)?;
                }
            }
        }
    }

    let mut cur: Vec<Bound<'py, PyAny>> = Vec::new();
    for v in versions_order.iter() {
        if pending_count(&v)? == 0 {
            cur.push(v);
        }
    }

    let out = PyList::empty(py);
    while !cur.is_empty() {
        let mut next: Vec<Bound<'py, PyAny>> = Vec::new();
        for v in &cur {
            if seen.contains(v)? {
                continue;
            }
            if pending_count(v)? != 0 {
                continue;
            }
            if let Some(ds) = descendants.get_item(v)? {
                for d in ds.cast::<PyList>()?.iter() {
                    next.push(d);
                }
            }
            out.append(v)?;
            seen.add(v)?;
        }
        cur = next;
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
