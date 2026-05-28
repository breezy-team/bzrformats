use bazaar::plan_merge as pm;
use bazaar::versionedfile::Key;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyTuple};
use std::collections::HashSet;

use crate::versionedfile::PyVersionedFiles;

/// Walk the iterable of LCAs returned by `vcsgraph.graph.Graph.find_lca`,
/// stripping the `key_prefix` so each entry is a bare revision id (or the
/// literal byte string `null:`).
fn extract_lcas(
    py: Python<'_>,
    lcas: Bound<'_, PyAny>,
    key_prefix_len: usize,
) -> PyResult<HashSet<Vec<u8>>> {
    let mut out = HashSet::new();
    for item in lcas.try_iter()? {
        let item = item?;
        if let Ok(bytes) = item.extract::<Vec<u8>>() {
            // Python returns a plain NULL_REVISION (b"null:") rather than a
            // tuple when there's no common ancestor.
            out.insert(bytes);
        } else {
            // Key tuple — strip the prefix.
            let key: Key = item.extract()?;
            let segs = key.segments();
            if segs.len() <= key_prefix_len {
                return Err(pyo3::exceptions::PyValueError::new_err(format!(
                    "find_lca returned key {:?} shorter than expected prefix",
                    segs
                )));
            }
            out.insert(segs[key_prefix_len].clone());
        }
    }
    let _ = py;
    Ok(out)
}

/// Pyo3 binding for `bzrformats.merge._PlanLCAMerge`.
///
/// The Python constructor takes `(a_rev, b_rev, vf, key_prefix, graph)`.
/// We call `graph.find_lca(prefix + (a_rev,), prefix + (b_rev,))` on the
/// Python `graph` object (so vcs-graph's existing pyo3 binding handles
/// the actual LCA walk), then drive the pure-crate `PlanLCAMerge` for
/// the merge plan generation.
#[pyclass(name = "_PlanLCAMerge", module = "bzrformats._bzr_rs.plan_merge")]
struct PyPlanLCAMerge {
    plan: Vec<(pm::MergeTag, Vec<u8>)>,
    a_rev: Vec<u8>,
    b_rev: Vec<u8>,
    lcas: HashSet<Vec<u8>>,
}

#[pymethods]
impl PyPlanLCAMerge {
    #[new]
    #[pyo3(signature = (a_rev, b_rev, vf, key_prefix, graph))]
    fn new<'py>(
        py: Python<'py>,
        a_rev: Vec<u8>,
        b_rev: Vec<u8>,
        vf: Py<PyAny>,
        key_prefix: Bound<'py, PyAny>,
        graph: Bound<'py, PyAny>,
    ) -> PyResult<Self> {
        let prefix_vec: Vec<Vec<u8>> = key_prefix
            .try_iter()?
            .map(|item| item?.extract::<Vec<u8>>())
            .collect::<PyResult<_>>()?;
        let py_vf = PyVersionedFiles::new(vf);
        // Build the two tip keys and ask the (Python) graph for LCAs.
        let a_key = build_key(py, &prefix_vec, &a_rev)?;
        let b_key = build_key(py, &prefix_vec, &b_rev)?;
        let lcas_obj = graph.call_method1("find_lca", (a_key, b_key))?;
        let lcas = extract_lcas(py, lcas_obj, prefix_vec.len())?;
        let mut planner = pm::PlanLCAMerge::new(
            &py_vf,
            a_rev.clone(),
            b_rev.clone(),
            prefix_vec,
            lcas.clone(),
        )
        .map_err(crate::knit::knit_err_to_py)?;
        let plan = planner.plan_merge().map_err(crate::knit::knit_err_to_py)?;
        Ok(Self {
            plan,
            a_rev,
            b_rev,
            lcas,
        })
    }

    /// Yield the merge plan as `(tag_str, line_bytes)` tuples, matching
    /// the Python generator's output shape.
    fn plan_merge<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let out = PyList::empty(py);
        for (tag, line) in &self.plan {
            let tup = PyTuple::new(
                py,
                [
                    tag.as_str().into_pyobject(py)?.into_any(),
                    PyBytes::new(py, line).into_any(),
                ],
            )?;
            out.append(tup)?;
        }
        Ok(out.into_any().call_method0("__iter__")?)
    }

    #[getter]
    fn a_rev<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.a_rev)
    }

    #[getter]
    fn b_rev<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.b_rev)
    }

    /// `lcas` is a Python set of bare revision ids (matching the legacy
    /// Python attribute layout).
    #[getter]
    fn lcas<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, pyo3::types::PySet>> {
        let s = pyo3::types::PySet::empty(py)?;
        for lca in &self.lcas {
            s.add(PyBytes::new(py, lca))?;
        }
        Ok(s)
    }

    /// Classmethod mirror of `_PlanMergeBase._subtract_plans`. Drives the
    /// pure-crate helper so callers (notably
    /// `_PlanMergeVersionedFile.plan_lca_merge`) can do
    /// `_PlanLCAMerge._subtract_plans(old_list, new_list)` without
    /// reaching into the Python module.
    #[classmethod]
    fn _subtract_plans<'py>(
        _cls: &Bound<'_, pyo3::types::PyType>,
        py: Python<'py>,
        old_plan: Bound<'py, PyAny>,
        new_plan: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        subtract_plans_py(py, old_plan, new_plan)
    }
}

fn build_key<'py>(
    py: Python<'py>,
    prefix: &[Vec<u8>],
    suffix: &[u8],
) -> PyResult<Bound<'py, PyTuple>> {
    let mut parts: Vec<Bound<'py, PyBytes>> = prefix.iter().map(|p| PyBytes::new(py, p)).collect();
    parts.push(PyBytes::new(py, suffix));
    PyTuple::new(py, parts)
}

#[pyfunction]
#[pyo3(name = "subtract_plans")]
fn subtract_plans_py<'py>(
    py: Python<'py>,
    old_plan: Bound<'py, PyAny>,
    new_plan: Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyList>> {
    let old = extract_plan(&old_plan)?;
    let new = extract_plan(&new_plan)?;
    let out = pm::subtract_plans(&old, &new);
    let result = PyList::empty(py);
    for (tag, line) in out {
        let tup = PyTuple::new(
            py,
            [
                tag.as_str().into_pyobject(py)?.into_any(),
                PyBytes::new(py, &line).into_any(),
            ],
        )?;
        result.append(tup)?;
    }
    Ok(result)
}

fn extract_plan<'py>(plan: &Bound<'py, PyAny>) -> PyResult<Vec<(pm::MergeTag, Vec<u8>)>> {
    let mut out = Vec::new();
    for item in plan.try_iter()? {
        let item = item?;
        let tup = item.cast_into::<PyTuple>()?;
        let tag_str: String = tup.get_item(0)?.extract()?;
        let tag = pm::MergeTag::from_str(&tag_str).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!("unknown merge tag {:?}", tag_str))
        })?;
        let line: Vec<u8> = tup.get_item(1)?.extract()?;
        out.push((tag, line));
    }
    Ok(out)
}

pub fn _plan_merge_rs(py: Python<'_>) -> PyResult<Bound<'_, PyModule>> {
    let m = PyModule::new(py, "plan_merge")?;
    m.add_class::<PyPlanLCAMerge>()?;
    m.add_function(wrap_pyfunction!(subtract_plans_py, &m)?)?;
    Ok(m)
}
