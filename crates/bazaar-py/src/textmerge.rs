use bazaar::textmerge;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyTuple};

type ExtractedLines = (Vec<Py<PyBytes>>, Vec<Vec<u8>>);

fn extract_byte_lines(seq: &Bound<PyAny>) -> PyResult<ExtractedLines> {
    let mut items = Vec::new();
    let mut keys = Vec::new();
    for item in seq.try_iter()? {
        let item = item?;
        let bytes = item
            .cast_into::<PyBytes>()
            .map_err(|_| pyo3::exceptions::PyTypeError::new_err("lines must be bytes"))?;
        keys.push(bytes.as_bytes().to_vec());
        items.push(bytes.unbind());
    }
    Ok((items, keys))
}

fn slice_pylist<'py>(
    py: Python<'py>,
    items: &[Py<PyBytes>],
    start: usize,
    end: usize,
) -> PyResult<Bound<'py, PyList>> {
    PyList::new(py, items[start..end].iter().map(|o| o.bind(py).clone()))
}

fn group_to_tuple<'py>(py: Python<'py>, group: &Group) -> PyResult<Bound<'py, PyTuple>> {
    match group {
        Group::Unchanged(lines) => PyTuple::new(py, [lines.bind(py).clone().into_any()]),
        Group::Conflict { a, b } => PyTuple::new(
            py,
            [a.bind(py).clone().into_any(), b.bind(py).clone().into_any()],
        ),
    }
}

/// A two-way merge group, with the inner line lists held as Python objects so
/// the original `bytes` instances round-trip back out unchanged.
enum Group {
    Unchanged(Py<PyList>),
    Conflict { a: Py<PyList>, b: Py<PyList> },
}

impl Group {
    fn is_useful(&self, py: Python) -> bool {
        match self {
            Group::Unchanged(lines) => lines.bind(py).len() > 0,
            Group::Conflict { a, b } => a.bind(py).len() > 0 || b.bind(py).len() > 0,
        }
    }
}

fn run_merge(
    py: Python,
    items_a: &[Py<PyBytes>],
    keys_a: &[Vec<u8>],
    items_b: &[Py<PyBytes>],
    keys_b: &[Vec<u8>],
) -> PyResult<Vec<Group>> {
    let raw = textmerge::merge2(keys_a, keys_b);
    let mut out = Vec::with_capacity(raw.len());
    let mut pa = 0usize;
    let mut pb = 0usize;
    for group in &raw {
        match group {
            textmerge::Group::Unchanged(lines) => {
                let len = lines.len();
                out.push(Group::Unchanged(
                    slice_pylist(py, items_a, pa, pa + len)?.unbind(),
                ));
                pa += len;
                pb += len;
            }
            textmerge::Group::Conflict { a, b } => {
                let la = a.len();
                let lb = b.len();
                out.push(Group::Conflict {
                    a: slice_pylist(py, items_a, pa, pa + la)?.unbind(),
                    b: slice_pylist(py, items_b, pb, pb + lb)?.unbind(),
                });
                pa += la;
                pb += lb;
            }
        }
    }
    Ok(out)
}

/// Two-way text merge.
///
/// Common regions are reported as one-element tuples; conflicts as two-element
/// tuples `(this_lines, other_lines)`.
#[pyclass(frozen, module = "bzrformats._bzr_rs.textmerge")]
struct Merge2 {
    items_a: Vec<Py<PyBytes>>,
    keys_a: Vec<Vec<u8>>,
    items_b: Vec<Py<PyBytes>>,
    keys_b: Vec<Vec<u8>>,
    a_marker: Py<PyBytes>,
    b_marker: Py<PyBytes>,
    split_marker: Py<PyBytes>,
}

#[pymethods]
impl Merge2 {
    #[classattr]
    #[allow(non_snake_case)]
    fn A_MARKER(py: Python) -> Bound<PyBytes> {
        PyBytes::new(py, textmerge::A_MARKER)
    }

    #[classattr]
    #[allow(non_snake_case)]
    fn B_MARKER(py: Python) -> Bound<PyBytes> {
        PyBytes::new(py, textmerge::B_MARKER)
    }

    #[classattr]
    #[allow(non_snake_case)]
    fn SPLIT_MARKER(py: Python) -> Bound<PyBytes> {
        PyBytes::new(py, textmerge::SPLIT_MARKER)
    }

    #[new]
    #[pyo3(signature = (lines_a, lines_b, a_marker = None, b_marker = None, split_marker = None))]
    fn new(
        py: Python,
        lines_a: &Bound<PyAny>,
        lines_b: &Bound<PyAny>,
        a_marker: Option<Py<PyBytes>>,
        b_marker: Option<Py<PyBytes>>,
        split_marker: Option<Py<PyBytes>>,
    ) -> PyResult<Self> {
        let (items_a, keys_a) = extract_byte_lines(lines_a)?;
        let (items_b, keys_b) = extract_byte_lines(lines_b)?;
        Ok(Self {
            items_a,
            keys_a,
            items_b,
            keys_b,
            a_marker: a_marker.unwrap_or_else(|| PyBytes::new(py, textmerge::A_MARKER).unbind()),
            b_marker: b_marker.unwrap_or_else(|| PyBytes::new(py, textmerge::B_MARKER).unbind()),
            split_marker: split_marker
                .unwrap_or_else(|| PyBytes::new(py, textmerge::SPLIT_MARKER).unbind()),
        })
    }

    #[getter]
    fn lines_a<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        slice_pylist(py, &self.items_a, 0, self.items_a.len())
    }

    #[getter]
    fn lines_b<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        slice_pylist(py, &self.items_b, 0, self.items_b.len())
    }

    #[getter]
    fn a_marker<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        self.a_marker.bind(py).clone()
    }

    #[getter]
    fn b_marker<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        self.b_marker.bind(py).clone()
    }

    #[getter]
    fn split_marker<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        self.split_marker.bind(py).clone()
    }

    /// Return raw structured merge info, without filtering empty groups.
    ///
    /// Each element is a tuple: length 1 for an unchanged region, length 2
    /// `(this, other)` for a conflict.
    fn _merge_struct<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let groups = run_merge(py, &self.items_a, &self.keys_a, &self.items_b, &self.keys_b)?;
        let tuples: Vec<Bound<PyTuple>> = groups
            .iter()
            .map(|g| group_to_tuple(py, g))
            .collect::<PyResult<_>>()?;
        PyList::new(py, tuples)
    }

    /// Return structured merge info, with empty groups filtered out and
    /// optionally with conflict regions reduced via `reprocess_struct`.
    #[pyo3(signature = (reprocess = false))]
    fn merge_struct<'py>(&self, py: Python<'py>, reprocess: bool) -> PyResult<Bound<'py, PyList>> {
        let groups = run_merge(py, &self.items_a, &self.keys_a, &self.items_b, &self.keys_b)?;
        let useful: Vec<Group> = groups.into_iter().filter(|g| g.is_useful(py)).collect();
        let final_groups = if reprocess {
            reprocess_groups(py, useful)?
        } else {
            useful
        };
        let tuples: Vec<Bound<PyTuple>> = final_groups
            .iter()
            .map(|g| group_to_tuple(py, g))
            .collect::<PyResult<_>>()?;
        PyList::new(py, tuples)
    }

    /// Return `(merged_lines, had_conflicts)` where `merged_lines` is a list of
    /// byte lines, with conflict markers inserted around conflict regions.
    #[pyo3(signature = (reprocess = false))]
    fn merge_lines<'py>(
        &self,
        py: Python<'py>,
        reprocess: bool,
    ) -> PyResult<(Bound<'py, PyList>, bool)> {
        let groups = run_merge(py, &self.items_a, &self.keys_a, &self.items_b, &self.keys_b)?;
        let useful: Vec<Group> = groups.into_iter().filter(|g| g.is_useful(py)).collect();
        let final_groups = if reprocess {
            reprocess_groups(py, useful)?
        } else {
            useful
        };
        let (lines, conflicts) = render_lines(
            py,
            &final_groups,
            self.a_marker.bind(py),
            self.b_marker.bind(py),
            self.split_marker.bind(py),
        )?;
        Ok((lines, conflicts))
    }

    /// Filter empty groups out of a structured merge iterator.
    fn iter_useful<'py>(
        &self,
        py: Python<'py>,
        struct_iter: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        let mut out: Vec<Bound<PyTuple>> = Vec::new();
        for item in struct_iter.try_iter()? {
            let group = item?.cast_into::<PyTuple>()?;
            let len = group.len();
            let first = group.get_item(0)?;
            if first.try_iter()?.next().is_some() {
                out.push(group);
                continue;
            }
            if len > 1 {
                let second = group.get_item(1)?;
                if second.try_iter()?.next().is_some() {
                    out.push(group);
                }
            }
        }
        PyList::new(py, out)
    }

    /// Render structured merge info to a flat line list using this instance's
    /// conflict markers.
    fn struct_to_lines<'py>(
        &self,
        py: Python<'py>,
        struct_iter: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        let groups = iter_to_groups(struct_iter)?;
        let (lines, _) = render_lines(
            py,
            &groups,
            self.a_marker.bind(py),
            self.b_marker.bind(py),
            self.split_marker.bind(py),
        )?;
        Ok(lines)
    }

    /// Re-run a two-way merge over each conflict region, shrinking conflicts to
    /// their minimal diverging core.
    #[staticmethod]
    fn reprocess_struct<'py>(
        py: Python<'py>,
        struct_iter: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        let groups = iter_to_groups(struct_iter)?;
        let reprocessed = reprocess_groups(py, groups)?;
        let tuples: Vec<Bound<PyTuple>> = reprocessed
            .iter()
            .map(|g| group_to_tuple(py, g))
            .collect::<PyResult<_>>()?;
        PyList::new(py, tuples)
    }
}

/// Convert an iterable of `(lines,)` / `(lines_a, lines_b)` tuples into our
/// internal Group representation, retaining the original Python list objects.
fn iter_to_groups(struct_iter: &Bound<PyAny>) -> PyResult<Vec<Group>> {
    let py = struct_iter.py();
    let mut out = Vec::new();
    for item in struct_iter.try_iter()? {
        let tuple = item?.cast_into::<PyTuple>()?;
        let len = tuple.len();
        if len == 1 {
            let lines = tuple.get_item(0)?;
            let pylist = ensure_pylist(py, &lines)?;
            out.push(Group::Unchanged(pylist.unbind()));
        } else if len == 2 {
            let a = ensure_pylist(py, &tuple.get_item(0)?)?;
            let b = ensure_pylist(py, &tuple.get_item(1)?)?;
            out.push(Group::Conflict {
                a: a.unbind(),
                b: b.unbind(),
            });
        } else {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "merge struct tuples must have length 1 or 2",
            ));
        }
    }
    Ok(out)
}

fn ensure_pylist<'py>(py: Python<'py>, obj: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyList>> {
    if let Ok(list) = obj.cast::<PyList>() {
        Ok(list.clone())
    } else {
        let mut items = Vec::new();
        for item in obj.try_iter()? {
            items.push(item?);
        }
        PyList::new(py, items)
    }
}

fn reprocess_groups(py: Python, groups: Vec<Group>) -> PyResult<Vec<Group>> {
    let mut out = Vec::new();
    for group in groups {
        match group {
            Group::Unchanged(_) => out.push(group),
            Group::Conflict { a, b } => {
                let a_bound = a.bind(py);
                let b_bound = b.bind(py);
                let (items_a, keys_a) = extract_byte_lines(a_bound.as_any())?;
                let (items_b, keys_b) = extract_byte_lines(b_bound.as_any())?;
                let sub = run_merge(py, &items_a, &keys_a, &items_b, &keys_b)?;
                for g in sub.into_iter().filter(|g| g.is_useful(py)) {
                    out.push(g);
                }
            }
        }
    }
    Ok(out)
}

fn render_lines<'py>(
    py: Python<'py>,
    groups: &[Group],
    a_marker: &Bound<'py, PyBytes>,
    b_marker: &Bound<'py, PyBytes>,
    split_marker: &Bound<'py, PyBytes>,
) -> PyResult<(Bound<'py, PyList>, bool)> {
    let mut lines: Vec<Bound<PyAny>> = Vec::new();
    let mut conflicts = false;
    for group in groups {
        match group {
            Group::Unchanged(g) => {
                for item in g.bind(py).iter() {
                    lines.push(item);
                }
            }
            Group::Conflict { a, b } => {
                conflicts = true;
                lines.push(a_marker.clone().into_any());
                for item in a.bind(py).iter() {
                    lines.push(item);
                }
                lines.push(split_marker.clone().into_any());
                for item in b.bind(py).iter() {
                    lines.push(item);
                }
                lines.push(b_marker.clone().into_any());
            }
        }
    }
    Ok((PyList::new(py, lines)?, conflicts))
}

/// Extract `(state_str, line_obj)` tuples from an iterable, returning the
/// states (parsed), the line objects (unbound for re-emission) and the raw
/// line bytes (used for content comparisons inside the merge state machine).
fn extract_plan(
    plan: &Bound<PyAny>,
) -> PyResult<(Vec<textmerge::PlanState>, Vec<Py<PyAny>>, Vec<Vec<u8>>)> {
    let mut states = Vec::new();
    let mut lines = Vec::new();
    let mut line_bytes = Vec::new();
    for item in plan.try_iter()? {
        let pair = item?.cast_into::<PyTuple>()?;
        if pair.len() != 2 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "plan items must be (state, line) pairs",
            ));
        }
        let state_str: String = pair.get_item(0)?.extract()?;
        let state = textmerge::PlanState::from_str(&state_str)
            .ok_or_else(|| pyo3::exceptions::PyAssertionError::new_err(state_str.clone()))?;
        states.push(state);
        let line_obj = pair.get_item(1)?;
        line_bytes.push(line_obj.extract::<Vec<u8>>().unwrap_or_default());
        lines.push(line_obj.unbind());
    }
    Ok((states, lines, line_bytes))
}

/// Translate a weave merge plan into structured merge groups (length-1 tuples
/// for resolved chunks, length-2 tuples for conflicts). Line objects from the
/// input plan are returned by reference, preserving identity.
#[pyfunction]
fn merge_struct_from_plan<'py>(
    py: Python<'py>,
    plan: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyList>> {
    let (states, lines, line_bytes) = extract_plan(plan)?;
    let groups = textmerge::merge_struct_from_plan(&states, &line_bytes);

    let mut tuples: Vec<Bound<PyTuple>> = Vec::with_capacity(groups.len());
    for group in groups {
        match group {
            textmerge::PlanGroup::Single(indices) => {
                let lst = PyList::new(py, indices.iter().map(|&i| lines[i].bind(py).clone()))?;
                tuples.push(PyTuple::new(py, [lst.into_any()])?);
            }
            textmerge::PlanGroup::Conflict { a, b } => {
                let la = PyList::new(py, a.iter().map(|&i| lines[i].bind(py).clone()))?;
                let lb = PyList::new(py, b.iter().map(|&i| lines[i].bind(py).clone()))?;
                tuples.push(PyTuple::new(py, [la.into_any(), lb.into_any()])?);
            }
        }
    }
    PyList::new(py, tuples)
}

/// Reconstruct a BASE text from a weave merge plan: emits the line objects for
/// `unchanged`, `killed-a`, `killed-b` and `killed-both` states.
#[pyfunction]
fn base_from_plan<'py>(py: Python<'py>, plan: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyList>> {
    let (states, lines, _line_bytes) = extract_plan(plan)?;
    let indices = textmerge::base_indices_from_plan(&states);
    PyList::new(py, indices.into_iter().map(|i| lines[i].bind(py).clone()))
}

pub fn _textmerge_rs(py: Python) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "textmerge")?;
    m.add_class::<Merge2>()?;
    m.add("A_MARKER", PyBytes::new(py, textmerge::A_MARKER))?;
    m.add("B_MARKER", PyBytes::new(py, textmerge::B_MARKER))?;
    m.add("SPLIT_MARKER", PyBytes::new(py, textmerge::SPLIT_MARKER))?;
    m.add_function(pyo3::wrap_pyfunction!(merge_struct_from_plan, &m)?)?;
    m.add_function(pyo3::wrap_pyfunction!(base_from_plan, &m)?)?;
    Ok(m)
}
