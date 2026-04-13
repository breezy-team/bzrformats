use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::wrap_pyfunction;

#[pyfunction]
fn chunks_to_gzip<'py>(py: Python<'py>, chunks: Vec<Vec<u8>>) -> Vec<Bound<'py, PyBytes>> {
    bazaar::tuned_gzip::chunks_to_gzip(chunks)
        .into_iter()
        .map(|c| PyBytes::new(py, &c))
        .collect()
}

pub(crate) fn _tuned_gzip_rs(py: Python) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "tuned_gzip")?;
    m.add_function(wrap_pyfunction!(chunks_to_gzip, &m)?)?;
    Ok(m)
}
