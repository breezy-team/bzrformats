use bazaar::plan_merge as pm;
use bazaar::versionedfile::Key;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};
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

/// Pyo3 binding for `bzrformats.merge._PlanMerge`.
///
/// The Python constructor takes `(a_rev, b_rev, vf, key_prefix)`. We wrap
/// `vf` in [`PyVersionedFiles`] and drive the pure-crate
/// [`pm::PlanMerge`], which builds the in-memory weave and computes the
/// plan eagerly. `plan_merge()` then yields the cached `(tag, line)`
/// tuples. The query helpers `_unique_lines` / `_get_matching_blocks` and
/// the static graph helpers `_remove_external_references` / `_prune_tails`
/// (which the test-suite drives with plain Python keys) are exposed too.
#[pyclass(name = "_PlanMerge", module = "bzrformats._bzr_rs.plan_merge")]
struct PyPlanMerge {
    plan: Vec<(pm::MergeTag, Vec<u8>)>,
    a_rev: Vec<u8>,
    b_rev: Vec<u8>,
    key_prefix: Vec<Vec<u8>>,
    vf: Py<PyAny>,
}

#[pymethods]
impl PyPlanMerge {
    #[new]
    #[pyo3(signature = (a_rev, b_rev, vf, key_prefix))]
    fn new(
        a_rev: Vec<u8>,
        b_rev: Vec<u8>,
        vf: Py<PyAny>,
        key_prefix: Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let prefix_vec: Vec<Vec<u8>> = key_prefix
            .try_iter()?
            .map(|item| item?.extract::<Vec<u8>>())
            .collect::<PyResult<_>>()?;
        let py_vf = PyVersionedFiles::new(vf.clone_ref(key_prefix.py()));
        let mut planner =
            pm::PlanMerge::new(&py_vf, a_rev.clone(), b_rev.clone(), prefix_vec.clone())
                .map_err(crate::knit::knit_err_to_py)?;
        let plan = planner.plan_merge().map_err(crate::knit::knit_err_to_py)?;
        Ok(Self {
            plan,
            a_rev,
            b_rev,
            key_prefix: prefix_vec,
            vf,
        })
    }

    /// Yield the merge plan as `(tag_str, line_bytes)` tuples.
    fn plan_merge<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let out = plan_to_pylist(py, &self.plan)?;
        out.into_any().call_method0("__iter__")
    }

    /// Mirror of `_PlanMergeBase._unique_lines`: partition the line indices
    /// not covered by the matching blocks into `(unique_a, unique_b)`.
    fn _unique_lines<'py>(
        &self,
        py: Python<'py>,
        matching_blocks: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let blocks = extract_blocks(&matching_blocks)?;
        let (left, right) = pm::unique_lines(&blocks);
        let left_list = PyList::new(py, left)?;
        let right_list = PyList::new(py, right)?;
        PyTuple::new(py, [left_list.into_any(), right_list.into_any()])
    }

    /// Mirror of `_PlanMergeBase._get_matching_blocks`. `_PlanMerge` does no
    /// tip-line precaching, so this always computes fresh blocks.
    fn _get_matching_blocks<'py>(
        &self,
        py: Python<'py>,
        left_revision: Vec<u8>,
        right_revision: Vec<u8>,
    ) -> PyResult<Bound<'py, PyList>> {
        let py_vf = PyVersionedFiles::new(self.vf.clone_ref(py));
        let blocks =
            pm::matching_blocks_uncached(&py_vf, &self.key_prefix, &left_revision, &right_revision)
                .map_err(crate::knit::knit_err_to_py)?;
        blocks_to_pylist(py, &blocks)
    }

    #[getter]
    fn a_rev<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.a_rev)
    }

    #[getter]
    fn b_rev<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.b_rev)
    }

    /// Classmethod mirror of `_PlanMergeBase._subtract_plans`.
    #[classmethod]
    fn _subtract_plans<'py>(
        _cls: &Bound<'_, pyo3::types::PyType>,
        py: Python<'py>,
        old_plan: Bound<'py, PyAny>,
        new_plan: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        subtract_plans_py(py, old_plan, new_plan)
    }

    /// Staticmethod mirror of `_PlanMerge._remove_external_references`.
    ///
    /// Operates on arbitrary hashable Python keys (the test-suite drives it
    /// with plain integers), so this is implemented directly over the
    /// Python `dict` rather than the typed crate helper.
    #[staticmethod]
    fn _remove_external_references<'py>(
        py: Python<'py>,
        parent_map: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        remove_external_references_py(py, &parent_map)
    }

    /// Staticmethod mirror of `_PlanMerge._prune_tails`. Mutates
    /// `parent_map` and `child_map` in place (matching the Python contract)
    /// and consumes `tails_to_remove`.
    #[staticmethod]
    fn _prune_tails<'py>(
        py: Python<'py>,
        parent_map: Bound<'py, PyDict>,
        child_map: Bound<'py, PyDict>,
        tails_to_remove: Bound<'py, PyAny>,
    ) -> PyResult<()> {
        prune_tails_py(py, &parent_map, &child_map, &tails_to_remove)
    }
}

/// Build a `[(tag_str, line_bytes), ...]` list from a crate plan.
fn plan_to_pylist<'py>(
    py: Python<'py>,
    plan: &[(pm::MergeTag, Vec<u8>)],
) -> PyResult<Bound<'py, PyList>> {
    let out = PyList::empty(py);
    for (tag, line) in plan {
        let tup = PyTuple::new(
            py,
            [
                tag.as_str().into_pyobject(py)?.into_any(),
                PyBytes::new(py, line).into_any(),
            ],
        )?;
        out.append(tup)?;
    }
    Ok(out)
}

/// Render matching blocks as a list of `(i, j, n)` tuples.
fn blocks_to_pylist<'py>(
    py: Python<'py>,
    blocks: &[pm::MatchingBlock],
) -> PyResult<Bound<'py, PyList>> {
    let out = PyList::empty(py);
    for &(i, j, n) in blocks {
        out.append(PyTuple::new(py, [i, j, n])?)?;
    }
    Ok(out)
}

/// Extract `(i, j, n)` matching-block tuples from a Python iterable.
fn extract_blocks(blocks: &Bound<'_, PyAny>) -> PyResult<Vec<pm::MatchingBlock>> {
    let mut out = Vec::new();
    for item in blocks.try_iter()? {
        let tup = item?.cast_into::<PyTuple>()?;
        let i: usize = tup.get_item(0)?.extract()?;
        let j: usize = tup.get_item(1)?.extract()?;
        let n: usize = tup.get_item(2)?.extract()?;
        out.push((i, j, n));
    }
    Ok(out)
}

/// Python-key version of `remove_external_references`: returns
/// `(filtered_parent_map, child_map, tails)`. Preserves `parent_map`'s
/// iteration order so child lists match the Python reference implementation.
fn remove_external_references_py<'py>(
    py: Python<'py>,
    parent_map: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyTuple>> {
    let parent_map = parent_map.cast::<PyDict>()?;
    let filtered = PyDict::new(py);
    let child_map = PyDict::new(py);
    let tails = PyList::empty(py);
    for (key, parents) in parent_map.iter() {
        let mut culled: Vec<Bound<PyAny>> = Vec::new();
        for parent in parents.try_iter()? {
            let parent = parent?;
            if parent_map.contains(&parent)? {
                culled.push(parent);
            }
        }
        if culled.is_empty() {
            tails.append(&key)?;
        }
        for parent_key in &culled {
            match child_map.get_item(parent_key)? {
                Some(existing) => existing.cast_into::<PyList>()?.append(&key)?,
                None => {
                    let lst = PyList::empty(py);
                    lst.append(&key)?;
                    child_map.set_item(parent_key, lst)?;
                }
            }
        }
        if !child_map.contains(&key)? {
            child_map.set_item(&key, PyList::empty(py))?;
        }
        filtered.set_item(&key, PyList::new(py, &culled)?)?;
    }
    PyTuple::new(
        py,
        [filtered.into_any(), child_map.into_any(), tails.into_any()],
    )
}

/// Python-key version of `prune_tails`: mutates `parent_map` and
/// `child_map` in place, consuming `tails_to_remove`.
fn prune_tails_py<'py>(
    _py: Python<'py>,
    parent_map: &Bound<'py, PyDict>,
    child_map: &Bound<'py, PyDict>,
    tails_to_remove: &Bound<'py, PyAny>,
) -> PyResult<()> {
    let mut stack: Vec<Bound<PyAny>> = Vec::new();
    for item in tails_to_remove.try_iter()? {
        stack.push(item?);
    }
    while let Some(next) = stack.pop() {
        parent_map.del_item(&next)?;
        let children = child_map
            .get_item(&next)?
            .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err("child_map missing tail"))?;
        child_map.del_item(&next)?;
        for child in children.try_iter()? {
            let child = child?;
            let child_parents = parent_map
                .get_item(&child)?
                .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err("parent_map missing child"))?;
            let child_parents = child_parents.cast_into::<PyList>()?;
            // Remove `next` from the child's parents (first occurrence).
            for (idx, parent) in child_parents.iter().enumerate() {
                if parent.eq(&next)? {
                    child_parents.del_item(idx)?;
                    break;
                }
            }
            if child_parents.len() == 0 {
                stack.push(child);
            }
        }
    }
    Ok(())
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
    m.add_class::<PyPlanMerge>()?;
    m.add_function(wrap_pyfunction!(subtract_plans_py, &m)?)?;
    Ok(m)
}
