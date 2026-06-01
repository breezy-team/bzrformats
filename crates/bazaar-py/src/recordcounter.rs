// Copyright (C) 2010 Canonical Ltd
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

//! Record counting support for showing progress of revision fetch.
//!
//! Thin pyo3 wrapper over [`bazaar::recordcounter::RecordCounter`].

use bazaar::recordcounter::RecordCounter as RsRecordCounter;
use pyo3::prelude::*;

/// Container that maintains estimates of the work required for a fetch.
#[pyclass(name = "RecordCounter", module = "bzrformats._bzr_rs.recordcounter")]
pub struct RecordCounter {
    inner: RsRecordCounter,
}

#[pymethods]
impl RecordCounter {
    #[new]
    fn new() -> Self {
        Self {
            inner: RsRecordCounter::new(),
        }
    }

    #[getter]
    fn initialized(&self) -> bool {
        self.inner.initialized
    }

    #[setter]
    fn set_initialized(&mut self, value: bool) {
        self.inner.initialized = value;
    }

    #[getter]
    fn current(&self) -> i64 {
        self.inner.current
    }

    #[setter]
    fn set_current(&mut self, value: i64) {
        self.inner.current = value;
    }

    #[getter]
    fn key_count(&self) -> i64 {
        self.inner.key_count
    }

    #[setter]
    fn set_key_count(&mut self, value: i64) {
        self.inner.key_count = value;
    }

    #[getter]
    fn max(&self) -> i64 {
        self.inner.max
    }

    #[setter]
    fn set_max(&mut self, value: i64) {
        self.inner.max = value;
    }

    #[getter(STEP)]
    fn step(&self) -> i64 {
        self.inner.step
    }

    #[setter(STEP)]
    fn set_step(&mut self, value: i64) {
        self.inner.step = value;
    }

    /// Whether `setup()` has been called.
    fn is_initialized(&self) -> bool {
        self.inner.is_initialized()
    }

    fn _estimate_max(&self, key_count: i64) -> i64 {
        self.inner.estimate_max(key_count)
    }

    #[pyo3(signature = (key_count, current=0))]
    fn setup(&mut self, key_count: i64, current: i64) {
        self.inner.setup(key_count, current);
    }

    fn increment(&mut self, count: i64) {
        self.inner.increment(count);
    }
}

pub(crate) fn _recordcounter_rs(py: Python<'_>) -> PyResult<Bound<'_, PyModule>> {
    let m = PyModule::new(py, "recordcounter")?;
    m.add_class::<RecordCounter>()?;
    Ok(m)
}
