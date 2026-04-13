// Copyright (C) 2008 Canonical Ltd
// Copyright (C) 2026 Jelmer Vernooij
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.

//! PyO3 wrapper around `bazaar::chunk_writer::ChunkWriter`.

use bazaar::chunk_writer::{
    ChunkWriter as RsChunkWriter, REPACK_OPTS_FOR_SIZE, REPACK_OPTS_FOR_SPEED,
};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyTuple};

#[pyclass(module = "bzrformats._bzr_rs.chunk_writer", name = "ChunkWriter")]
pub struct ChunkWriter {
    inner: Option<RsChunkWriter>,
}

#[pymethods]
impl ChunkWriter {
    #[new]
    #[pyo3(signature = (chunk_size, reserved=0, optimize_for_size=false))]
    fn new(chunk_size: usize, reserved: usize, optimize_for_size: bool) -> Self {
        Self {
            inner: Some(RsChunkWriter::new(chunk_size, reserved, optimize_for_size)),
        }
    }

    #[classattr]
    #[allow(non_snake_case)]
    fn _repack_opts_for_speed(py: Python<'_>) -> Py<PyTuple> {
        PyTuple::new(py, [REPACK_OPTS_FOR_SPEED.0, REPACK_OPTS_FOR_SPEED.1])
            .unwrap()
            .unbind()
    }

    #[classattr]
    #[allow(non_snake_case)]
    fn _repack_opts_for_size(py: Python<'_>) -> Py<PyTuple> {
        PyTuple::new(py, [REPACK_OPTS_FOR_SIZE.0, REPACK_OPTS_FOR_SIZE.1])
            .unwrap()
            .unbind()
    }

    #[getter]
    fn _max_repack(&self) -> PyResult<u32> {
        Ok(self.borrow()?.max_repack())
    }

    #[getter]
    fn _max_zsync(&self) -> PyResult<u32> {
        Ok(self.borrow()?.max_zsync())
    }

    #[pyo3(signature = (for_size=true))]
    fn set_optimize(&mut self, for_size: bool) -> PyResult<()> {
        self.borrow_mut()?.set_optimize(for_size);
        Ok(())
    }

    #[pyo3(signature = (bytes, reserved=false))]
    fn write(&mut self, bytes: &[u8], reserved: bool) -> PyResult<bool> {
        Ok(self.borrow_mut()?.write(bytes, reserved))
    }

    fn finish<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        let inner = self.inner.take().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("ChunkWriter already finished")
        })?;
        let finished = inner.finish();
        let bytes_list = PyList::empty(py);
        for chunk in &finished.bytes_list {
            bytes_list.append(PyBytes::new(py, chunk))?;
        }
        let unused = match finished.unused_bytes {
            Some(ref b) => PyBytes::new(py, b).into_any(),
            None => py.None().into_bound(py),
        };
        PyTuple::new(
            py,
            [
                bytes_list.into_any(),
                unused,
                finished.nulls_needed.into_pyobject(py)?.into_any(),
            ],
        )
    }
}

impl ChunkWriter {
    fn borrow(&self) -> PyResult<&RsChunkWriter> {
        self.inner.as_ref().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("ChunkWriter already finished")
        })
    }
    fn borrow_mut(&mut self) -> PyResult<&mut RsChunkWriter> {
        self.inner.as_mut().ok_or_else(|| {
            pyo3::exceptions::PyRuntimeError::new_err("ChunkWriter already finished")
        })
    }
}

pub(crate) fn _chunk_writer_rs(py: Python<'_>) -> PyResult<Bound<'_, PyModule>> {
    let m = PyModule::new(py, "chunk_writer")?;
    m.add_class::<ChunkWriter>()?;
    Ok(m)
}
