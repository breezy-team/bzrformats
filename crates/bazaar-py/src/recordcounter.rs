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
//! Ported from `bzrformats.recordcounter`. `RecordCounter` keeps an estimate
//! of how much work a fetch (push, pull, branch, checkout) will involve so a
//! progress bar can show a fetched-vs-estimate ratio.

use pyo3::prelude::*;

/// Container that maintains estimates of the work required for a fetch.
#[pyclass(name = "RecordCounter", module = "bzrformats._bzr_rs.recordcounter")]
pub struct RecordCounter {
    #[pyo3(get, set)]
    initialized: bool,
    #[pyo3(get, set)]
    current: i64,
    #[pyo3(get, set)]
    key_count: i64,
    #[pyo3(get, set)]
    max: i64,
    /// Users update the progress bar every `STEP` records. An odd number keeps
    /// the last digit of the fetched-vs-estimate ratio changing periodically.
    #[pyo3(get, set, name = "STEP")]
    step: i64,
}

#[pymethods]
impl RecordCounter {
    #[new]
    fn new() -> Self {
        Self {
            initialized: false,
            current: 0,
            key_count: 0,
            max: 0,
            step: 7,
        }
    }

    /// Whether `setup()` has been called.
    fn is_initialized(&self) -> bool {
        self.initialized
    }

    /// Estimate the maximum amount of "inserting stream" work.
    ///
    /// The 10.3 multiplier comes from empirical data across three projects; it
    /// is chosen to under-promise/over-deliver and to render a realistic
    /// progress ratio. See the original Python for the derivation.
    fn _estimate_max(&self, key_count: i64) -> i64 {
        (key_count as f64 * 10.3) as i64
    }

    /// Set up `max`/`current` to reflect the amount of work pending.
    #[pyo3(signature = (key_count, current=0))]
    fn setup(&mut self, key_count: i64, current: i64) {
        self.current = current;
        self.key_count = key_count;
        self.max = self._estimate_max(key_count);
        self.initialized = true;
    }

    /// Increment `current` by `count`, growing `max` so it stays ahead.
    fn increment(&mut self, count: i64) {
        self.current += count;
        if self.current > self.max {
            self.max += self.key_count;
        }
    }
}

pub(crate) fn _recordcounter_rs(py: Python<'_>) -> PyResult<Bound<'_, PyModule>> {
    let m = PyModule::new(py, "recordcounter")?;
    m.add_class::<RecordCounter>()?;
    Ok(m)
}
