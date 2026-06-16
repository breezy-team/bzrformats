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

//! File annotation driven by a VersionedFiles, ported from
//! `bzrformats.annotate`.
//!
//! `VersionedFileAnnotator` is a `subclass`-able pyo3 class: `knit._KnitAnnotator`
//! is a Python subclass that overrides `_get_needed_texts` /
//! `_get_parent_annotations_and_matches` and reaches into the per-step
//! bookkeeping dicts. To preserve that, all mutable state lives in the instance
//! `__dict__` (the pyclass carries `dict`), and the overridable hooks are
//! invoked through `slf.call_method(...)` so Python overrides win, exactly as
//! Python method dispatch would.

use pyo3::exceptions::PyKeyError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PySet, PyTuple};

/// Drives annotation of texts stored in a VersionedFiles.
#[pyclass(
    name = "VersionedFileAnnotator",
    subclass,
    dict,
    module = "bzrformats._bzr_rs.annotate"
)]
pub struct VersionedFileAnnotator;

impl VersionedFileAnnotator {
    /// Read instance attribute `name` as a Bound value.
    fn attr<'py>(slf: &Bound<'py, Self>, name: &str) -> PyResult<Bound<'py, PyAny>> {
        slf.getattr(name)
    }
}

#[pymethods]
impl VersionedFileAnnotator {
    /// Create a new Annotator from a VersionedFile.
    #[new]
    #[pyo3(signature = (vf))]
    fn new(vf: Bound<'_, PyAny>) -> Self {
        let _ = vf;
        VersionedFileAnnotator
    }

    fn __init__(slf: &Bound<'_, Self>, vf: Bound<'_, PyAny>) -> PyResult<()> {
        let py = slf.py();
        slf.setattr("_vf", vf)?;
        slf.setattr("_parent_map", PyDict::new(py))?;
        slf.setattr("_text_cache", PyDict::new(py))?;
        // key => number of children still to be built from this key.
        slf.setattr("_num_needed_children", PyDict::new(py))?;
        slf.setattr("_annotations_cache", PyDict::new(py))?;
        slf.setattr("_heads_provider", py.None())?;
        slf.setattr("_ann_tuple_cache", PyDict::new(py))?;
        Ok(())
    }

    fn _update_needed_children(
        slf: &Bound<'_, Self>,
        key: Bound<'_, PyAny>,
        parent_keys: Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let _ = key;
        let needed = Self::attr(slf, "_num_needed_children")?;
        let needed = needed.cast::<PyDict>()?;
        for parent_key in parent_keys.try_iter()? {
            let parent_key = parent_key?;
            match needed.get_item(&parent_key)? {
                Some(v) => {
                    let n: i64 = v.extract()?;
                    needed.set_item(&parent_key, n + 1)?;
                }
                None => needed.set_item(&parent_key, 1)?,
            }
        }
        Ok(())
    }

    /// Determine the texts we need to fetch from the backing vf.
    ///
    /// Returns `(vf_keys_needed, ann_keys_needed)`.
    fn _get_needed_keys<'py>(
        slf: &Bound<'py, Self>,
        key: Bound<'py, PyAny>,
    ) -> PyResult<(Bound<'py, PySet>, Bound<'py, PySet>)> {
        let py = slf.py();
        let parent_map = Self::attr(slf, "_parent_map")?;
        let parent_map = parent_map.cast::<PyDict>()?;
        let text_cache = Self::attr(slf, "_text_cache")?;
        let text_cache = text_cache.cast::<PyDict>()?;
        let annotations_cache = Self::attr(slf, "_annotations_cache")?;
        let annotations_cache = annotations_cache.cast::<PyDict>()?;
        let vf = Self::attr(slf, "_vf")?;

        // One extra copy of the node we are looking at when we are done.
        let needed_children = Self::attr(slf, "_num_needed_children")?;
        needed_children.cast::<PyDict>()?.set_item(&key, 1)?;

        let vf_keys_needed = PySet::empty(py)?;
        let ann_keys_needed = PySet::empty(py)?;
        let mut needed_keys = PySet::empty(py)?;
        needed_keys.add(&key)?;

        while !needed_keys.is_empty() {
            let parent_lookup = PyList::empty(py);
            let next_parent_map = PyDict::new(py);
            for k in needed_keys.iter() {
                if parent_map.contains(&k)? {
                    if !text_cache.contains(&k)? {
                        vf_keys_needed.add(&k)?;
                    } else if !annotations_cache.contains(&k)? {
                        ann_keys_needed.add(&k)?;
                        next_parent_map.set_item(&k, parent_map.get_item(&k)?.unwrap())?;
                    }
                } else {
                    parent_lookup.append(&k)?;
                    vf_keys_needed.add(&k)?;
                }
            }
            let new_needed = PySet::empty(py)?;
            let looked_up = vf.call_method1("get_parent_map", (&parent_lookup,))?;
            next_parent_map.call_method1("update", (looked_up,))?;
            for item in next_parent_map.items().iter() {
                let item = item.cast::<PyTuple>()?;
                let k = item.get_item(0)?;
                let mut parent_keys = item.get_item(1)?;
                if parent_keys.is_none() {
                    // No-graph versionedfile.
                    parent_keys = PyTuple::empty(py).into_any();
                    next_parent_map.set_item(&k, &parent_keys)?;
                }
                Self::_update_needed_children(slf, k.clone(), parent_keys.clone())?;
                for p in parent_keys.try_iter()? {
                    let p = p?;
                    if !parent_map.contains(&p)? {
                        new_needed.add(p)?;
                    }
                }
            }
            parent_map.call_method1("update", (&next_parent_map,))?;
            // _heads_provider caches against _parent_map; invalidate it.
            slf.setattr("_heads_provider", py.None())?;
            needed_keys = new_needed;
        }
        Ok((vf_keys_needed, ann_keys_needed))
    }

    /// Get the texts needed to annotate `key`.
    ///
    /// Yields `(this_key, lines, num_lines)`.
    #[pyo3(signature = (key, pb=None))]
    fn _get_needed_texts<'py>(
        slf: &Bound<'py, Self>,
        key: Bound<'py, PyAny>,
        pb: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        let (keys, ann_keys) = Self::_get_needed_keys(slf, key)?;
        let vf = Self::attr(slf, "_vf")?;
        let text_cache = Self::attr(slf, "_text_cache")?;
        let text_cache = text_cache.cast::<PyDict>()?;
        let keys_len = keys.len();
        if let Some(pb) = &pb {
            pb.call_method1("update", ("getting stream", 0, keys_len))?;
        }
        let out = PyList::empty(py);
        let stream = vf.call_method1("get_record_stream", (&keys, "topological", true))?;
        for record in stream.try_iter()? {
            let record = record?;
            if let Some(pb) = &pb {
                pb.call_method1("update", ("extracting", 0, keys_len))?;
            }
            let storage_kind: String = record.getattr("storage_kind")?.extract()?;
            if storage_kind == "absent" {
                let rec_key = record.getattr("key")?;
                return Err(crate::annotate::revision_not_present(py, rec_key, &vf));
            }
            let this_key = record.getattr("key")?;
            let lines = record.call_method1("get_bytes_as", ("lines",))?;
            let num_lines = lines.len()?;
            text_cache.set_item(&this_key, &lines)?;
            out.append((this_key, lines, num_lines))?;
        }
        for key in ann_keys.iter() {
            let lines = text_cache
                .get_item(&key)?
                .ok_or_else(|| PyKeyError::new_err(key.clone().unbind()))?;
            let num_lines = lines.len()?;
            out.append((key, lines, num_lines))?;
        }
        Ok(out.into_any().try_iter()?.into_any())
    }

    /// Get `(parent_annotations, matching_blocks)` for `parent_key`.
    fn _get_parent_annotations_and_matches<'py>(
        slf: &Bound<'py, Self>,
        key: Bound<'py, PyAny>,
        text: Bound<'py, PyAny>,
        parent_key: Bound<'py, PyAny>,
    ) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)> {
        let _ = key;
        let py = slf.py();
        let text_cache = Self::attr(slf, "_text_cache")?;
        let parent_lines = text_cache
            .cast::<PyDict>()?
            .get_item(&parent_key)?
            .ok_or_else(|| PyKeyError::new_err(parent_key.clone().unbind()))?;
        let annotations_cache = Self::attr(slf, "_annotations_cache")?;
        let parent_annotations = annotations_cache
            .cast::<PyDict>()?
            .get_item(&parent_key)?
            .ok_or_else(|| PyKeyError::new_err(parent_key.clone().unbind()))?;
        let matcher = py
            .import("patiencediff")?
            .getattr("PatienceSequenceMatcher")?
            .call1((py.None(), parent_lines, text))?;
        let matching_blocks = matcher.call_method0("get_matching_blocks")?;
        Ok((parent_annotations, matching_blocks))
    }

    /// Reannotate `key`'s text relative to its first parent.
    fn _update_from_first_parent(
        slf: &Bound<'_, Self>,
        key: Bound<'_, PyAny>,
        annotations: Bound<'_, PyList>,
        lines: Bound<'_, PyAny>,
        parent_key: Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let res = slf.call_method1(
            "_get_parent_annotations_and_matches",
            (&key, &lines, &parent_key),
        )?;
        let res = res.cast::<PyTuple>()?;
        let parent_annotations = res.get_item(0)?;
        let matching_blocks = res.get_item(1)?;
        for block in matching_blocks.try_iter()? {
            let block = block?;
            let block = block.cast::<PyTuple>()?;
            let parent_idx: usize = block.get_item(0)?.extract()?;
            let lines_idx: usize = block.get_item(1)?.extract()?;
            let match_len: usize = block.get_item(2)?.extract()?;
            for i in 0..match_len {
                let val = parent_annotations.get_item(parent_idx + i)?;
                annotations.set_item(lines_idx + i, val)?;
            }
        }
        Ok(())
    }

    /// Reannotate `key`'s text relative to a second (or more) parent.
    fn _update_from_other_parents(
        slf: &Bound<'_, Self>,
        key: Bound<'_, PyAny>,
        annotations: Bound<'_, PyList>,
        lines: Bound<'_, PyAny>,
        this_annotation: Bound<'_, PyAny>,
        parent_key: Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let py = slf.py();
        let res = slf.call_method1(
            "_get_parent_annotations_and_matches",
            (&key, &lines, &parent_key),
        )?;
        let res = res.cast::<PyTuple>()?;
        let parent_annotations = res.get_item(0)?;
        let matching_blocks = res.get_item(1)?;

        let mut last_ann: Option<Bound<'_, PyAny>> = None;
        let mut last_parent: Option<Bound<'_, PyAny>> = None;
        let mut last_res: Option<Bound<'_, PyAny>> = None;
        for block in matching_blocks.try_iter()? {
            let block = block?;
            let block = block.cast::<PyTuple>()?;
            let parent_idx: usize = block.get_item(0)?.extract()?;
            let lines_idx: usize = block.get_item(1)?.extract()?;
            let match_len: usize = block.get_item(2)?.extract()?;
            // Fast path: identical sub-ranges need no work.
            let ann_sub = annotations.get_slice(lines_idx, lines_idx + match_len);
            let par_sub = parent_annotations.call_method1(
                "__getitem__",
                (pyo3::types::PySlice::new(
                    py,
                    parent_idx as isize,
                    (parent_idx + match_len) as isize,
                    1,
                ),),
            )?;
            if ann_sub.as_any().eq(&par_sub)? {
                continue;
            }
            for i in 0..match_len {
                let ann = annotations.get_item(lines_idx + i)?;
                let par_ann = parent_annotations.get_item(parent_idx + i)?;
                let ann_idx = lines_idx + i;
                if ann.eq(&par_ann)? {
                    continue;
                }
                if ann.eq(&this_annotation)? {
                    annotations.set_item(ann_idx, &par_ann)?;
                    continue;
                }
                let matches_last = match (&last_ann, &last_parent) {
                    (Some(la), Some(lp)) => ann.eq(la)? && par_ann.eq(lp)?,
                    _ => false,
                };
                if matches_last {
                    annotations.set_item(ann_idx, last_res.as_ref().unwrap())?;
                } else {
                    // new_ann = tuple(sorted(set(ann) | set(par_ann)))
                    let new_set = PySet::empty(py)?;
                    new_set.call_method1("update", (&ann,))?;
                    new_set.call_method1("update", (&par_ann,))?;
                    let sorted = py
                        .import("builtins")?
                        .getattr("sorted")?
                        .call1((&new_set,))?;
                    let new_ann =
                        PyTuple::new(py, sorted.try_iter()?.collect::<PyResult<Vec<_>>>()?)?;
                    annotations.set_item(ann_idx, &new_ann)?;
                    last_ann = Some(ann);
                    last_parent = Some(par_ann);
                    last_res = Some(new_ann.into_any());
                }
            }
        }
        Ok(())
    }

    fn _record_annotation(
        slf: &Bound<'_, Self>,
        key: Bound<'_, PyAny>,
        parent_keys: Bound<'_, PyAny>,
        annotations: Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let annotations_cache = Self::attr(slf, "_annotations_cache")?;
        let annotations_cache = annotations_cache.cast::<PyDict>()?;
        annotations_cache.set_item(&key, &annotations)?;
        let needed = Self::attr(slf, "_num_needed_children")?;
        let needed = needed.cast::<PyDict>()?;
        let text_cache = Self::attr(slf, "_text_cache")?;
        let text_cache = text_cache.cast::<PyDict>()?;
        for parent_key in parent_keys.try_iter()? {
            let parent_key = parent_key?;
            let num: i64 = needed
                .get_item(&parent_key)?
                .ok_or_else(|| PyKeyError::new_err(parent_key.clone().unbind()))?
                .extract()?;
            let num = num - 1;
            if num == 0 {
                text_cache.del_item(&parent_key)?;
                annotations_cache.del_item(&parent_key)?;
            }
            needed.set_item(&parent_key, num)?;
        }
        Ok(())
    }

    fn _annotate_one(
        slf: &Bound<'_, Self>,
        key: Bound<'_, PyAny>,
        text: Bound<'_, PyAny>,
        num_lines: usize,
    ) -> PyResult<()> {
        let py = slf.py();
        let this_annotation = PyTuple::new(py, [&key])?;
        // annotations is mutated in-place by the _update_from* helpers.
        let annotations = PyList::empty(py);
        for _ in 0..num_lines {
            annotations.append(&this_annotation)?;
        }
        let parent_map = Self::attr(slf, "_parent_map")?;
        let parent_keys = parent_map
            .cast::<PyDict>()?
            .get_item(&key)?
            .ok_or_else(|| PyKeyError::new_err(key.clone().unbind()))?;
        let parents: Vec<Bound<'_, PyAny>> =
            parent_keys.try_iter()?.collect::<PyResult<Vec<_>>>()?;
        if !parents.is_empty() {
            slf.call_method1(
                "_update_from_first_parent",
                (&key, &annotations, &text, &parents[0]),
            )?;
            for parent in &parents[1..] {
                slf.call_method1(
                    "_update_from_other_parents",
                    (&key, &annotations, &text, &this_annotation, parent),
                )?;
            }
        }
        slf.call_method1("_record_annotation", (&key, &parent_keys, &annotations))?;
        Ok(())
    }

    /// Add a text not otherwise present in the versioned file.
    fn add_special_text(
        slf: &Bound<'_, Self>,
        key: Bound<'_, PyAny>,
        parent_keys: Bound<'_, PyAny>,
        text: Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let py = slf.py();
        Self::attr(slf, "_parent_map")?
            .cast::<PyDict>()?
            .set_item(&key, parent_keys)?;
        let split = py
            .import("bzrformats.osutils")?
            .getattr("split_lines")?
            .call1((text,))?;
        Self::attr(slf, "_text_cache")?
            .cast::<PyDict>()?
            .set_item(&key, split)?;
        slf.setattr("_heads_provider", py.None())?;
        Ok(())
    }

    /// Return `(annotations, lines)` for `key`.
    #[pyo3(signature = (key, pb=None))]
    fn annotate<'py>(
        slf: &Bound<'py, Self>,
        key: Bound<'py, PyAny>,
        pb: Option<Bound<'py, PyAny>>,
    ) -> PyResult<(Bound<'py, PyAny>, Bound<'py, PyAny>)> {
        let py = slf.py();
        let needed = slf.call_method1("_get_needed_texts", (&key, pb))?;
        for item in needed.try_iter()? {
            let item = item?;
            let item = item.cast::<PyTuple>()?;
            let text_key = item.get_item(0)?;
            let text = item.get_item(1)?;
            let num_lines: usize = item.get_item(2)?.extract()?;
            slf.call_method1("_annotate_one", (text_key, text, num_lines))?;
        }
        let annotations_cache = Self::attr(slf, "_annotations_cache")?;
        let annotations = match annotations_cache.cast::<PyDict>()?.get_item(&key)? {
            Some(a) => a,
            None => {
                let vf = Self::attr(slf, "_vf")?;
                return Err(revision_not_present(py, key, &vf));
            }
        };
        let lines = Self::attr(slf, "_text_cache")?
            .cast::<PyDict>()?
            .get_item(&key)?
            .ok_or_else(|| PyKeyError::new_err(key.clone().unbind()))?;
        Ok((annotations, lines))
    }

    fn _get_heads_provider<'py>(slf: &Bound<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        let current = Self::attr(slf, "_heads_provider")?;
        if current.is_none() {
            let parent_map = Self::attr(slf, "_parent_map")?;
            let provider = py
                .import("vcsgraph.known_graph")?
                .getattr("KnownGraph")?
                .call1((parent_map,))?;
            slf.setattr("_heads_provider", &provider)?;
            return Ok(provider);
        }
        Ok(current)
    }

    fn _resolve_annotation_tie<'py>(
        slf: &Bound<'py, Self>,
        the_heads: Bound<'py, PyAny>,
        line: Bound<'py, PyAny>,
        tiebreaker: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let _ = slf;
        let py = slf.py();
        if tiebreaker.is_none() {
            // head = sorted(the_heads)[0]
            let sorted = py
                .import("builtins")?
                .getattr("sorted")?
                .call1((&the_heads,))?;
            return sorted.get_item(0);
        }
        // Backwards compatibility: break heads into pairs and resolve.
        let mut iter = the_heads.try_iter()?;
        let mut head = iter
            .next()
            .ok_or_else(|| PyKeyError::new_err("empty heads"))??;
        for possible_head in iter {
            let possible_head = possible_head?;
            let pair = PyTuple::new(
                py,
                [
                    PyTuple::new(py, [&head, &line])?,
                    PyTuple::new(py, [&possible_head, &line])?,
                ],
            )?;
            head = tiebreaker.call1((pair,))?.get_item(0)?;
        }
        Ok(head)
    }

    /// Determine the single best source revision for each line.
    fn annotate_flat<'py>(
        slf: &Bound<'py, Self>,
        key: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        let py = slf.py();
        // Module-level test hook; read dynamically so tests can monkeypatch it.
        let custom_tiebreaker = py
            .import("bzrformats.annotate")?
            .getattr("_break_annotation_tie")?;
        let result = slf.call_method1("annotate", (&key,))?;
        let result = result.cast::<PyTuple>()?;
        let annotations = result.get_item(0)?;
        let lines = result.get_item(1)?;
        let heads_provider = slf.call_method0("_get_heads_provider")?;
        let heads = heads_provider.getattr("heads")?;
        let out = PyList::empty(py);
        let zipped = py
            .import("builtins")?
            .getattr("zip")?
            .call1((&annotations, &lines))?;
        for pair in zipped.try_iter()? {
            let pair = pair?;
            let pair = pair.cast::<PyTuple>()?;
            let annotation = pair.get_item(0)?;
            let line = pair.get_item(1)?;
            let head = if annotation.len()? == 1 {
                annotation.get_item(0)?
            } else {
                let the_heads = heads.call1((&annotation,))?;
                if the_heads.len()? == 1 {
                    the_heads.try_iter()?.next().unwrap()?
                } else {
                    slf.call_method1(
                        "_resolve_annotation_tie",
                        (the_heads, &line, &custom_tiebreaker),
                    )?
                }
            };
            out.append((head, line))?;
        }
        Ok(out)
    }
}

/// Build a `bzrformats.errors.RevisionNotPresent(key, vf)`.
pub(crate) fn revision_not_present(
    py: Python<'_>,
    key: Bound<'_, PyAny>,
    vf: &Bound<'_, PyAny>,
) -> PyErr {
    match py
        .import("bzrformats.errors")
        .and_then(|m| m.getattr("RevisionNotPresent"))
        .and_then(|c| c.call1((key, vf)))
    {
        Ok(exc) => PyErr::from_value(exc),
        Err(e) => e,
    }
}

pub(crate) fn _annotate_rs(py: Python<'_>) -> PyResult<Bound<'_, PyModule>> {
    let m = PyModule::new(py, "annotate")?;
    m.add_class::<VersionedFileAnnotator>()?;
    Ok(m)
}
