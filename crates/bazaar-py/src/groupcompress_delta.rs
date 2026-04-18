use bazaar::groupcompress::rabin_delta::{self, OwningDeltaIndex};
use pyo3::exceptions::{PyMemoryError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::convert::TryInto;

#[pyclass]
pub struct DeltaIndex {
    inner: OwningDeltaIndex,
}

#[pymethods]
impl DeltaIndex {
    #[new]
    #[pyo3(signature = (source=None, max_bytes_to_index=None))]
    fn new(source: Option<&[u8]>, max_bytes_to_index: Option<usize>) -> PyResult<Self> {
        let mbi = match max_bytes_to_index {
            Some(0) | None => None,
            Some(n) => Some(n),
        };
        let mut inner = OwningDeltaIndex::new(mbi);

        if let Some(source) = source {
            inner.add_source(source.to_vec(), 0);
        }

        Ok(Self { inner })
    }

    fn __repr__(&self) -> String {
        format!(
            "DeltaIndex({}, {})",
            self.inner.num_sources(),
            self.inner.source_offset()
        )
    }

    fn __sizeof__(&self) -> usize {
        let mut size = std::mem::size_of::<Self>();
        for source in self.inner.sources() {
            size += source.len();
        }
        // Rough estimate for the index overhead
        size += self.inner.num_sources() * std::mem::size_of::<Vec<u8>>();
        size
    }

    #[getter]
    fn _sources<'py>(&self, py: Python<'py>) -> PyResult<Vec<Bound<'py, PyBytes>>> {
        Ok(self
            .inner
            .sources()
            .iter()
            .map(|s| PyBytes::new(py, s))
            .collect())
    }

    #[getter]
    fn _source_offset(&self) -> usize {
        self.inner.source_offset()
    }

    #[setter]
    fn set_source_offset(&mut self, value: usize) {
        self.inner.set_source_offset(value);
    }

    #[getter]
    fn _max_num_sources(&self) -> usize {
        65000
    }

    #[getter]
    fn _max_bytes_to_index(&self) -> usize {
        self.inner.max_bytes_to_index().unwrap_or(0)
    }

    #[setter]
    fn set_max_bytes_to_index(&mut self, value: usize) {
        self.inner
            .set_max_bytes_to_index(if value == 0 { None } else { Some(value) });
    }

    fn _has_index(&self) -> bool {
        !self.inner.is_empty()
    }

    fn add_source(&mut self, source: &[u8], unadded_bytes: usize) -> PyResult<()> {
        if self.inner.num_sources() >= 65000 {
            return Err(PyMemoryError::new_err("too many sources for DeltaIndex"));
        }
        self.inner.add_source(source.to_vec(), unadded_bytes);
        Ok(())
    }

    fn add_delta_source(&mut self, delta: &[u8], unadded_bytes: usize) -> PyResult<()> {
        if self.inner.num_sources() >= 65000 {
            return Err(PyMemoryError::new_err("too many sources for DeltaIndex"));
        }
        self.inner
            .add_delta_source(delta.to_vec(), unadded_bytes)
            .map_err(PyValueError::new_err)
    }

    #[pyo3(signature = (target_bytes, max_delta_size=0.0))]
    fn make_delta<'py>(
        &mut self,
        py: Python<'py>,
        target_bytes: &[u8],
        max_delta_size: f64,
    ) -> PyResult<Option<Bound<'py, PyBytes>>> {
        self.inner
            .make_delta(target_bytes, max_delta_size as usize)
            .map(|opt| opt.map(|data| PyBytes::new(py, &data)))
            .map_err(PyValueError::new_err)
    }
}

#[pyfunction]
pub fn _rabin_hash(content: &[u8]) -> PyResult<u32> {
    if content.len() < 16 {
        return Err(PyValueError::new_err(
            "content must be at least 16 bytes long",
        ));
    }
    let data: [u8; 16] = content[..16]
        .try_into()
        .map_err(|_| PyValueError::new_err("content must be at least 16 bytes long"))?;
    Ok(rabin_delta::rabin_hash(data).into())
}

#[pyfunction]
pub fn make_delta<'py>(
    py: Python<'py>,
    source_bytes: &[u8],
    target_bytes: &[u8],
) -> PyResult<Option<Bound<'py, PyBytes>>> {
    let result = rabin_delta::make_delta(source_bytes, target_bytes);
    Ok(Some(PyBytes::new(py, &result)))
}
