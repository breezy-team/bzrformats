use bazaar::pack;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::import_exception;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyTuple};

import_exception!(bzrformats.pack, InvalidRecordError);
import_exception!(bzrformats.pack, UnknownContainerFormatError);
import_exception!(bzrformats.pack, UnknownRecordTypeError);

fn pack_err_to_py(err: pack::PackError) -> PyErr {
    Python::attach(|py| match err {
        pack::PackError::InvalidName(n) => {
            let bytes = PyBytes::new(py, &n);
            InvalidRecordError::new_err((format!("{:?} is not a valid name.", bytes),))
        }
        pack::PackError::UnknownContainerFormat(line) => {
            UnknownContainerFormatError::new_err((PyBytes::new(py, &line).unbind(),))
        }
        pack::PackError::UnknownRecordType(b) => {
            UnknownRecordTypeError::new_err((PyBytes::new(py, &[b]).unbind(),))
        }
        pack::PackError::InvalidRecord(reason) => InvalidRecordError::new_err((reason,)),
    })
}

fn extract_names(names: &Bound<PyAny>) -> PyResult<Vec<Vec<Vec<u8>>>> {
    let mut out = Vec::new();
    for name_tuple in names.try_iter()? {
        let name_tuple = name_tuple?;
        let mut parts = Vec::new();
        for part in name_tuple.try_iter()? {
            let part = part?;
            let bytes = part
                .cast_into::<PyBytes>()
                .map_err(|_| PyTypeError::new_err("name parts must be bytes"))?;
            parts.push(bytes.as_bytes().to_vec());
        }
        out.push(parts);
    }
    Ok(out)
}

fn record_to_py<'py>(py: Python<'py>, record: pack::Record) -> PyResult<Bound<'py, PyTuple>> {
    let (names, body) = record;
    let name_tuples: Vec<Bound<PyTuple>> = names
        .into_iter()
        .map(|nt| {
            let parts: Vec<Bound<PyBytes>> = nt.into_iter().map(|p| PyBytes::new(py, &p)).collect();
            PyTuple::new(py, parts)
        })
        .collect::<PyResult<_>>()?;
    let names_list = PyList::new(py, name_tuples)?;
    PyTuple::new(
        py,
        [names_list.into_any(), PyBytes::new(py, &body).into_any()],
    )
}

/// Rust-backed port of `bzrformats.pack.ContainerSerialiser`. All methods
/// return bytes; the class is stateless aside from being a namespace.
#[pyclass(module = "bzrformats._bzr_rs.pack")]
struct ContainerSerialiser;

#[pymethods]
impl ContainerSerialiser {
    #[new]
    fn new() -> Self {
        ContainerSerialiser
    }

    fn begin<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &pack::begin())
    }

    fn end<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, pack::end())
    }

    fn bytes_header<'py>(
        &self,
        py: Python<'py>,
        length: usize,
        names: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let names = extract_names(&names)?;
        let out = pack::bytes_header(length, &names).map_err(pack_err_to_py)?;
        Ok(PyBytes::new(py, &out))
    }

    fn bytes_record<'py>(
        &self,
        py: Python<'py>,
        bytes: &[u8],
        names: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let names = extract_names(&names)?;
        let out = pack::bytes_record(bytes, &names).map_err(pack_err_to_py)?;
        Ok(PyBytes::new(py, &out))
    }
}

/// Rust-backed port of `bzrformats.pack.ContainerPushParser`.
#[pyclass(module = "bzrformats._bzr_rs.pack")]
struct ContainerPushParser {
    inner: pack::ContainerPushParser,
}

#[pymethods]
impl ContainerPushParser {
    #[new]
    fn new() -> Self {
        Self {
            inner: pack::ContainerPushParser::new(),
        }
    }

    #[getter]
    fn finished(&self) -> bool {
        self.inner.finished()
    }

    fn accept_bytes(&mut self, bytes: &[u8]) -> PyResult<()> {
        self.inner.accept_bytes(bytes).map_err(pack_err_to_py)
    }

    #[pyo3(signature = (max = None))]
    fn read_pending_records<'py>(
        &mut self,
        py: Python<'py>,
        max: Option<usize>,
    ) -> PyResult<Bound<'py, PyList>> {
        let records = self.inner.read_pending_records(max);
        let tuples: Vec<Bound<PyTuple>> = records
            .into_iter()
            .map(|r| record_to_py(py, r))
            .collect::<PyResult<_>>()?;
        PyList::new(py, tuples)
    }

    fn read_size_hint(&self) -> usize {
        self.inner.read_size_hint()
    }
}

/// Validate a name per `pack._check_name` — rejects whitespace bytes.
#[pyfunction]
#[pyo3(name = "_check_name")]
fn py_check_name(name: &[u8]) -> PyResult<()> {
    pack::check_name(name).map_err(|e| match e {
        pack::PackError::InvalidName(_) => Python::attach(|py| {
            InvalidRecordError::new_err((format!(
                "{:?} is not a valid name.",
                PyBytes::new(py, name)
            ),))
        }),
        _ => PyValueError::new_err(e.to_string()),
    })
}

pub fn _pack_rs(py: Python) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "pack")?;
    m.add_class::<ContainerSerialiser>()?;
    m.add_class::<ContainerPushParser>()?;
    m.add_function(wrap_pyfunction!(py_check_name, &m)?)?;
    m.add("FORMAT_ONE", PyBytes::new(py, pack::FORMAT_ONE))?;
    Ok(m)
}
