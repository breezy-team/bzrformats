// Copyright (C) 2005-2010 Canonical Ltd
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

//! Store and retrieve weaves in files.
//!
//! Ported from `bzrformats.weavefile`. These are thin adapters over the
//! Rust-backed `Weave` (which owns the v5 serialisation via
//! `_to_v5_bytes`/`_load_from_v5_bytes`); they move bytes between a Python
//! file-like object and a weave.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

/// The v5 weave file format marker.
const FORMAT_1: &[u8] = b"# bzr weave file v5\n";

/// Write a weave to a file, dispatching on the requested format.
#[pyfunction]
#[pyo3(signature = (weave, f, format=None))]
fn write_weave(
    py: Python<'_>,
    weave: Bound<'_, PyAny>,
    f: Bound<'_, PyAny>,
    format: Option<i64>,
) -> PyResult<()> {
    match format {
        None | Some(1) => write_weave_v5(py, weave, f),
        Some(other) => Err(PyValueError::new_err(format!(
            "unknown weave format {}",
            other
        ))),
    }
}

/// Write `weave` to file `f` in v5 format.
#[pyfunction]
fn write_weave_v5(_py: Python<'_>, weave: Bound<'_, PyAny>, f: Bound<'_, PyAny>) -> PyResult<()> {
    let data = weave.call_method0("_to_v5_bytes")?;
    f.call_method1("write", (data,))?;
    Ok(())
}

/// Read a weave from a file, returning a fresh `Weave`.
#[pyfunction]
fn read_weave<'py>(py: Python<'py>, f: Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    // FIXME: detect the weave type and dispatch (mirrors the Python TODO).
    let name = match f.getattr("name") {
        Ok(n) => n,
        Err(_) => py.None().into_bound(py),
    };
    let weave = py.get_type::<crate::weave::PyWeave>().call1((name,))?;
    _read_weave_v5(py, f, weave.clone())?;
    Ok(weave)
}

/// Read a v5 weave file into the weave `w`, closing `f` afterwards.
///
/// Only to be used by `read_weave` and `WeaveFile.__init__`.
#[pyfunction]
fn _read_weave_v5<'py>(
    _py: Python<'py>,
    f: Bound<'py, PyAny>,
    w: Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let read_result = f.call_method0("read");
    // Mirror the try/finally: always close, even if read fails.
    let close_result = f.call_method0("close");
    let data = read_result?;
    close_result?;
    let bytes: &[u8] = data.cast::<PyBytes>()?.as_bytes();
    w.call_method1("_load_from_v5_bytes", (bytes,))?;
    Ok(w)
}

pub(crate) fn _weavefile_rs(py: Python<'_>) -> PyResult<Bound<'_, PyModule>> {
    let m = PyModule::new(py, "weavefile")?;
    m.add("FORMAT_1", PyBytes::new(py, FORMAT_1))?;
    m.add_function(wrap_pyfunction!(write_weave, &m)?)?;
    m.add_function(wrap_pyfunction!(write_weave_v5, &m)?)?;
    m.add_function(wrap_pyfunction!(read_weave, &m)?)?;
    m.add_function(wrap_pyfunction!(_read_weave_v5, &m)?)?;
    Ok(m)
}
