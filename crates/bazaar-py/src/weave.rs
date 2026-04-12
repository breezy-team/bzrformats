use bazaar::weave::{
    extract, inclusions, walk_internal, ExtractLine, Instruction, WalkLine, WeaveEntry, WeaveError,
};
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyBytes, PyFrozenSet, PyList, PyTuple};

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

pub fn _weave_rs(py: Python) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "weave")?;
    m.add_function(wrap_pyfunction!(py_extract, &m)?)?;
    m.add_function(wrap_pyfunction!(py_inclusions, &m)?)?;
    m.add_function(wrap_pyfunction!(py_walk_internal, &m)?)?;
    Ok(m)
}
