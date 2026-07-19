use pyo3::prelude::*;
use pyo3::types::PySet;
use pyo3_filelike::PyBinaryFile;
use std::io::Read;

/// Parse an ignore file.
///
/// Continue in the case of utf8 decoding errors, and emit a warning when such
/// an error is found. Optimise for the common case -- no decoding errors.
#[pyfunction]
fn parse_ignore_file<'py>(py: Python<'py>, f: Py<PyAny>) -> PyResult<Bound<'py, PySet>> {
    let mut reader = PyBinaryFile::from(f);
    let mut data = Vec::new();
    reader.read_to_end(&mut data)?;
    let ignored = bazaar::ignores::parse_ignore_file(&data);
    PySet::new(py, ignored.iter())
}

pub(crate) fn _ignores_rs(py: Python) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "ignores")?;
    m.add_wrapped(wrap_pyfunction!(parse_ignore_file))?;
    Ok(m)
}
