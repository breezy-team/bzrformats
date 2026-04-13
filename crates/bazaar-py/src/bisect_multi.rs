// Copyright (C) 2007 Canonical Ltd
// Copyright (C) 2026 Jelmer Vernooij
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.

//! PyO3 wrapper around `bazaar::bisect_multi::bisect_multi_bytes`.

use bazaar::bisect_multi::{bisect_multi_bytes, BisectStatus};
use pyo3::prelude::*;
use pyo3::types::{PyList, PyTuple};

/// Translate a Python return value for one probe into a [`BisectStatus`].
fn classify_status(status: &Bound<'_, PyAny>) -> PyResult<BisectStatus<Py<PyAny>>> {
    // `False` means absent. Match the Python `status is False` semantics by
    // checking for the `False` singleton before trying to extract an integer,
    // because `bool(False) == 0` would otherwise collide with a legitimate
    // integer status of 0.
    if status.is_instance_of::<pyo3::types::PyBool>() {
        let b: bool = status.extract()?;
        if !b {
            return Ok(BisectStatus::Absent);
        }
        // `True` is not a documented sentinel, treat as Found.
        return Ok(BisectStatus::Found(status.clone().unbind()));
    }
    if let Ok(n) = status.extract::<i64>() {
        if n == -1 {
            return Ok(BisectStatus::Earlier);
        }
        if n == 1 {
            return Ok(BisectStatus::Later);
        }
    }
    Ok(BisectStatus::Found(status.clone().unbind()))
}

#[pyfunction]
#[pyo3(name = "bisect_multi_bytes")]
fn py_bisect_multi_bytes<'py>(
    py: Python<'py>,
    content_lookup: Bound<'py, PyAny>,
    size: usize,
    keys: Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyList>> {
    let key_vec: Vec<Py<PyAny>> = keys
        .try_iter()?
        .map(|k| k.map(|obj| obj.unbind()))
        .collect::<PyResult<_>>()?;

    let mut lookup_err: Option<PyErr> = None;
    let results = bisect_multi_bytes(
        |probes| -> Vec<((usize, Py<PyAny>), BisectStatus<Py<PyAny>>)> {
            if lookup_err.is_some() {
                return Vec::new();
            }
            // Rebuild the probes list as Python tuples, consuming the probes
            // (the Python callback will hand the keys back via its response).
            let probe_count = probes.len();
            let py_probes = PyList::empty(py);
            for (loc, key) in probes {
                let tup = match PyTuple::new(
                    py,
                    [
                        loc.into_pyobject(py).unwrap().into_any(),
                        key.into_bound(py),
                    ],
                ) {
                    Ok(t) => t,
                    Err(e) => {
                        lookup_err = Some(e);
                        return Vec::new();
                    }
                };
                if let Err(e) = py_probes.append(tup) {
                    lookup_err = Some(e);
                    return Vec::new();
                }
            }
            let ret = match content_lookup.call1((py_probes,)) {
                Ok(r) => r,
                Err(e) => {
                    lookup_err = Some(e);
                    return Vec::new();
                }
            };
            // Expect an iterable of ((loc, key), status) pairs.
            let iter = match ret.try_iter() {
                Ok(i) => i,
                Err(e) => {
                    lookup_err = Some(e);
                    return Vec::new();
                }
            };
            let mut out = Vec::with_capacity(probe_count);
            for item in iter {
                let item = match item {
                    Ok(i) => i,
                    Err(e) => {
                        lookup_err = Some(e);
                        return out;
                    }
                };
                let parts = match item.extract::<(Bound<'_, PyAny>, Bound<'_, PyAny>)>() {
                    Ok(p) => p,
                    Err(e) => {
                        lookup_err = Some(e);
                        return out;
                    }
                };
                let (loc_key, status) = parts;
                let lk = match loc_key.extract::<(usize, Py<PyAny>)>() {
                    Ok(lk) => lk,
                    Err(e) => {
                        lookup_err = Some(e);
                        return out;
                    }
                };
                let st = match classify_status(&status) {
                    Ok(st) => st,
                    Err(e) => {
                        lookup_err = Some(e);
                        return out;
                    }
                };
                out.push((lk, st));
            }
            out
        },
        size,
        key_vec,
    );
    if let Some(e) = lookup_err {
        return Err(e);
    }
    let out_list = PyList::empty(py);
    for (key, value) in results {
        let tup = PyTuple::new(py, [key.into_bound(py), value.into_bound(py)])?;
        out_list.append(tup)?;
    }
    Ok(out_list)
}

pub(crate) fn _bisect_multi_rs(py: Python<'_>) -> PyResult<Bound<'_, PyModule>> {
    let m = PyModule::new(py, "bisect_multi")?;
    m.add_function(wrap_pyfunction!(py_bisect_multi_bytes, &m)?)?;
    Ok(m)
}
