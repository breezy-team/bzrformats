use bazaar::key_mapper::Mapper as _;
use bazaar::knit::{
    lower_fulltext, lower_line_delta_annotated, lower_line_delta_raw, parse_fulltext,
    parse_line_delta_annotated, parse_line_delta_plain, parse_line_delta_raw,
    parse_network_record_header, AnnotatedKnitContent, AnnotatedLine, DeltaHunk, KndxLoadError,
    KnitAccess as KnitAccessTrait, KnitAnnotateFactory, KnitAnnotator,
    KnitContent as KnitContentTrait, KnitError, KnitFactory as KnitFactoryTrait,
    KnitIndex as KnitIndexTrait, KnitIndexMemo, KnitKey, KnitMethod, KnitPlainFactory,
    KnitRecordDetails, PlainKnitContent,
};
use bazaar::transport::Transport as _;
use pyo3::exceptions::{PyNotImplementedError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};
use std::cell::RefCell;
use std::rc::Rc;

pyo3::import_exception!(bzrformats.errors, RevisionNotPresent);
pyo3::import_exception!(bzrformats.errors, NoSuchFile);
pyo3::import_exception!(bzrformats.versionedfile, UnavailableRepresentation);
pyo3::import_exception!(bzrformats.errors, ReadOnlyError);
pyo3::import_exception!(bzrformats.errors, ObjectNotLocked);
pyo3::import_exception!(bzrformats.knit, KnitCorrupt);
pyo3::import_exception!(bzrformats.knit, KnitHeaderError);
pyo3::import_exception!(bzrformats.knit, KnitIndexUnknownMethod);
pyo3::import_exception!(bzrformats.knit, SHA1KnitCorrupt);
pyo3::import_exception!(bzrformats.pack_repo, RetryWithNewPacks);

/// Run `op`, retrying it whenever it raises `RetryWithNewPacks`.
///
/// A `RetryWithNewPacks` means the pack listing changed underneath the
/// read. The access object's `reload_or_raise` decides what to do: it
/// reloads the pack listing and returns (so the operation is retried),
/// or re-raises the original error (so we give up). This mirrors the
/// `while True` / `except RetryWithNewPacks` loops that used to live in
/// Python's `KnitVersionedFiles`.
///
/// `op` is re-run from scratch — including re-fetching build details —
/// because a reload invalidates the `index_memo`s of the previous run.
fn retry_on_new_packs<T>(
    py: Python<'_>,
    access_obj: &Py<PyAny>,
    mut op: impl FnMut() -> PyResult<T>,
) -> PyResult<T> {
    loop {
        match op() {
            Ok(value) => return Ok(value),
            Err(err) if err.is_instance_of::<RetryWithNewPacks>(py) => {
                // reload_or_raise returns to signal "retry", or raises
                // the underlying error to signal "give up".
                access_obj
                    .bind(py)
                    .call_method1("reload_or_raise", (err.value(py),))?;
            }
            Err(err) => return Err(err),
        }
    }
}

/// Load the knit index file into memory.
///
/// Successor to the Cython `_load_data_c`; the `_c` suffix is dropped
/// because the Rust extension is no longer C-shaped. Delegates parsing
/// to the pure-crate `parse_kndx_body` and only marshals the resulting
/// cache + history into the Python `_KndxIndex` instance.
#[pyfunction]
pub fn _load_data(py: Python, kndx: &Bound<PyAny>, fp: &Bound<PyAny>) -> PyResult<()> {
    let cache = kndx.getattr("_cache")?.cast_into::<PyDict>()?;
    let history = kndx.getattr("_history")?.cast_into::<PyList>()?;

    kndx.call_method1("check_header", (fp,))?;

    let text = fp.call_method0("read")?;
    let body = text.cast_into::<PyBytes>()?;
    let parsed = bazaar::knit::parse_kndx_body(body.as_bytes()).map_err(|e| match e {
        bazaar::knit::KnitError::KndxCorrupt { line, detail } => {
            let filename = kndx
                .getattr("_filename")
                .map(|f| f.unbind())
                .unwrap_or_else(|_| py.None());
            let py_line = PyBytes::new(py, &line);
            KnitCorrupt::new_err((filename, format!("line {:?}: {}", py_line, detail)))
        }
        other => knit_err_to_py(other),
    })?;

    // Append the freshly-seen history entries (parse_kndx_body builds a
    // fresh history; merge so the Python list stays append-only across
    // multiple loads).
    let base = history.len();
    for v in &parsed.history {
        history.append(PyBytes::new(py, v))?;
    }
    for entry in parsed.cache.values() {
        let version_id = PyBytes::new(py, &entry.version_id);
        let options: Vec<Bound<PyBytes>> =
            entry.options.iter().map(|o| PyBytes::new(py, o)).collect();
        let options_list = PyList::new(py, &options)?;
        let parents: Vec<Bound<PyBytes>> =
            entry.parents.iter().map(|p| PyBytes::new(py, p)).collect();
        let parents_tuple = PyTuple::new(py, &parents)?;
        let index_obj = ((base + entry.index) as i64).into_pyobject(py)?;
        let pos_obj = (entry.pos as i64).into_pyobject(py)?;
        let size_obj = (entry.size as i64).into_pyobject(py)?;
        let tuple = PyTuple::new(
            py,
            &[
                version_id.as_any(),
                options_list.as_any(),
                pos_obj.as_any(),
                size_obj.as_any(),
                parents_tuple.as_any(),
                index_obj.as_any(),
            ],
        )?;
        cache.set_item(&version_id, &tuple)?;
    }

    Ok(())
}

pub(crate) fn knit_err_to_py(err: KnitError) -> PyErr {
    match err {
        KnitError::NotImplemented(name) => PyNotImplementedError::new_err(name),
        KnitError::ReadOnly => Python::attach(|py| ReadOnlyError::new_err((py.None(),))),
        KnitError::RevisionNotPresent(key) => {
            Python::attach(|py| match py_knit_key_to_py(py, &key) {
                Ok(py_key) => RevisionNotPresent::new_err((py_key.unbind(), py.None())),
                Err(e) => e,
            })
        }
        KnitError::MissingOrigin(_)
        | KnitError::BadDeltaHeader(_)
        | KnitError::TruncatedDelta
        | KnitError::Gzip(_)
        | KnitError::EmptyRecord
        | KnitError::HeaderFields(_)
        | KnitError::HeaderCount(_)
        | KnitError::LineCount { .. }
        | KnitError::BadEndMarker { .. }
        | KnitError::MissingTrailingNewline
        | KnitError::NetworkMissingKeyTerminator
        | KnitError::NetworkMissingParentsTerminator
        | KnitError::NetworkMissingNoEolByte
        | KnitError::BadIndexValue(_)
        | KnitError::TooManyCompressionParents(_)
        | KnitError::UnexpectedVersion { .. }
        | KnitError::BadKnitHeader { .. }
        | KnitError::KndxCorrupt { .. }
        | KnitError::Corrupt(_) => KnitCorrupt::new_err(("", err.to_string())),
        // Retry should be handled by the read pipeline's retry loop. An
        // Aborted error may carry a thread-local-stashed PyErr (set by
        // knit_err_from_py for unknown Python exception classes); restore
        // it so callers see ObjectNotLocked / ReadOnlyError / etc. as the
        // original Python exception rather than a generic Corrupt.
        KnitError::Retry(_) => PyRuntimeError::new_err(err.to_string()),
        KnitError::Aborted(_) => match take_stashed_py_err() {
            Some(stashed) => stashed,
            None => PyRuntimeError::new_err(err.to_string()),
        },
        KnitError::ExistingContent(_) | KnitError::BadSha1 { .. } => {
            KnitCorrupt::new_err(("", err.to_string()))
        }
    }
}

/// Convert a [`KnitError`] from a read driven through `access` into a
/// `PyErr`, restoring the stashed Python exception when the error is a
/// retry-related variant:
///
/// - [`KnitError::Retry`] re-raises the original `RetryWithNewPacks` so
///   an enclosing [`retry_on_new_packs`] loop can catch it.
/// - [`KnitError::Aborted`] re-raises the unrecoverable error verbatim
///   instead of remapping it to `KnitCorrupt`.
fn read_err_to_py(access: &PyKnitAccess, err: KnitError) -> PyErr {
    match err {
        KnitError::Retry(_) => {
            if let Some(retry_exc) = access.take_pending_retry() {
                return Python::attach(|py| PyErr::from_value(retry_exc.into_bound(py)));
            }
            knit_err_to_py(err)
        }
        KnitError::Aborted(_) => {
            if let Some(original) = access.take_final_error() {
                return original;
            }
            knit_err_to_py(err)
        }
        _ => knit_err_to_py(err),
    }
}

/// Extract a sequence of byte-lines from any Python iterable-of-bytes.
fn extract_byte_lines(seq: &Bound<PyAny>) -> PyResult<Vec<Vec<u8>>> {
    let mut out = Vec::new();
    for item in seq.try_iter()? {
        let item = item?;
        let bytes = item
            .cast_into::<PyBytes>()
            .map_err(|_| PyValueError::new_err("knit records must be bytes lines"))?;
        out.push(bytes.as_bytes().to_vec());
    }
    Ok(out)
}

fn as_slices(lines: &[Vec<u8>]) -> Vec<&[u8]> {
    lines.iter().map(|l| l.as_slice()).collect()
}

/// Parse an annotated fulltext body into a list of `(origin, text)` tuples.
#[pyfunction]
fn parse_fulltext_rs<'py>(
    py: Python<'py>,
    content: Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyList>> {
    let owned = extract_byte_lines(&content)?;
    let parsed = parse_fulltext(&as_slices(&owned)).map_err(knit_err_to_py)?;
    annotated_lines_to_py(py, &parsed)
}

/// Parse an annotated line delta into `[(start, end, count, contents), ...]`.
/// When `plain` is true, `contents` is a list of text bytes; otherwise it is
/// a list of `(origin, text)` tuples.
#[pyfunction]
#[pyo3(signature = (lines, plain = false))]
fn parse_line_delta_rs<'py>(
    py: Python<'py>,
    lines: Bound<'py, PyAny>,
    plain: bool,
) -> PyResult<Bound<'py, PyList>> {
    let owned = extract_byte_lines(&lines)?;
    let slices = as_slices(&owned);
    let items: Vec<Bound<PyTuple>> = if plain {
        let hunks = parse_line_delta_plain(&slices).map_err(knit_err_to_py)?;
        hunks
            .iter()
            .map(|h| {
                let content_list: Vec<Bound<PyBytes>> =
                    h.lines.iter().map(|t| PyBytes::new(py, t)).collect();
                PyTuple::new(
                    py,
                    [
                        h.start.into_pyobject(py)?.into_any(),
                        h.end.into_pyobject(py)?.into_any(),
                        h.count.into_pyobject(py)?.into_any(),
                        PyList::new(py, content_list)?.into_any(),
                    ],
                )
            })
            .collect::<PyResult<_>>()?
    } else {
        let hunks = parse_line_delta_annotated(&slices).map_err(knit_err_to_py)?;
        hunks
            .iter()
            .map(|h| {
                let content_tuples: Vec<Bound<PyTuple>> = h
                    .lines
                    .iter()
                    .map(|(o, t)| PyTuple::new(py, [PyBytes::new(py, o), PyBytes::new(py, t)]))
                    .collect::<PyResult<_>>()?;
                PyTuple::new(
                    py,
                    [
                        h.start.into_pyobject(py)?.into_any(),
                        h.end.into_pyobject(py)?.into_any(),
                        h.count.into_pyobject(py)?.into_any(),
                        PyList::new(py, content_tuples)?.into_any(),
                    ],
                )
            })
            .collect::<PyResult<_>>()?
    };
    PyList::new(py, items)
}

fn annotated_lines_to_py<'py>(
    py: Python<'py>,
    lines: &[AnnotatedLine],
) -> PyResult<Bound<'py, PyList>> {
    let tuples: Vec<Bound<PyTuple>> = lines
        .iter()
        .map(|(o, t)| PyTuple::new(py, [PyBytes::new(py, o), PyBytes::new(py, t)]))
        .collect::<PyResult<_>>()?;
    PyList::new(py, tuples)
}

/// Serialize an iterable of `(origin, text)` pairs back to knit fulltext
/// bytes — inverse of [`parse_fulltext_rs`].
#[pyfunction]
fn lower_fulltext_rs<'py>(
    py: Python<'py>,
    lines: Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyList>> {
    let pairs = extract_annotated_lines(&lines)?;
    let out = lower_fulltext(&pairs);
    let items: Vec<Bound<PyBytes>> = out.iter().map(|b| PyBytes::new(py, b)).collect();
    PyList::new(py, items)
}

/// Serialize an annotated line-delta back to knit bytes.
#[pyfunction]
fn lower_line_delta_rs<'py>(
    py: Python<'py>,
    delta: Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyList>> {
    let mut hunks: Vec<DeltaHunk<AnnotatedLine>> = Vec::new();
    for hunk in delta.try_iter()? {
        let tup = hunk?;
        let start: usize = tup.get_item(0)?.extract()?;
        let end: usize = tup.get_item(1)?.extract()?;
        let count: usize = tup.get_item(2)?.extract()?;
        let hunk_lines = extract_annotated_lines(&tup.get_item(3)?)?;
        hunks.push(DeltaHunk {
            start,
            end,
            count,
            lines: hunk_lines,
        });
    }
    let out = lower_line_delta_annotated(&hunks);
    let items: Vec<Bound<PyBytes>> = out.iter().map(|b| PyBytes::new(py, b)).collect();
    PyList::new(py, items)
}

/// Parse an unannotated line-delta into `[(start, end, count, [lines]), ...]`.
/// Mirrors `KnitPlainFactory.parse_line_delta`.
#[pyfunction]
fn parse_line_delta_raw_rs<'py>(
    py: Python<'py>,
    lines: Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyList>> {
    let owned = extract_byte_lines(&lines)?;
    let hunks = parse_line_delta_raw(&as_slices(&owned)).map_err(knit_err_to_py)?;
    let items: Vec<Bound<PyTuple>> = hunks
        .iter()
        .map(|h| {
            let content_list: Vec<Bound<PyBytes>> =
                h.lines.iter().map(|t| PyBytes::new(py, t)).collect();
            PyTuple::new(
                py,
                [
                    h.start.into_pyobject(py)?.into_any(),
                    h.end.into_pyobject(py)?.into_any(),
                    h.count.into_pyobject(py)?.into_any(),
                    PyList::new(py, content_list)?.into_any(),
                ],
            )
        })
        .collect::<PyResult<_>>()?;
    PyList::new(py, items)
}

/// Serialize an unannotated line-delta back to bytes. Mirrors
/// `KnitPlainFactory.lower_line_delta`.
#[pyfunction]
fn lower_line_delta_raw_rs<'py>(
    py: Python<'py>,
    delta: Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyList>> {
    let mut hunks: Vec<DeltaHunk<Vec<u8>>> = Vec::new();
    for hunk in delta.try_iter()? {
        let tup = hunk?;
        let start: usize = tup.get_item(0)?.extract()?;
        let end: usize = tup.get_item(1)?.extract()?;
        let count: usize = tup.get_item(2)?.extract()?;
        let hunk_lines = extract_byte_lines(&tup.get_item(3)?)?;
        hunks.push(DeltaHunk {
            start,
            end,
            count,
            lines: hunk_lines,
        });
    }
    let out = lower_line_delta_raw(&hunks);
    let items: Vec<Bound<PyBytes>> = out.iter().map(|b| PyBytes::new(py, b)).collect();
    PyList::new(py, items)
}

fn extract_annotated_lines(obj: &Bound<PyAny>) -> PyResult<Vec<AnnotatedLine>> {
    let mut out = Vec::new();
    for item in obj.try_iter()? {
        let pair = item?;
        let origin = pair
            .get_item(0)?
            .cast_into::<PyBytes>()
            .map_err(|_| PyValueError::new_err("origin must be bytes"))?
            .as_bytes()
            .to_vec();
        let text = pair
            .get_item(1)?
            .cast_into::<PyBytes>()
            .map_err(|_| PyValueError::new_err("text must be bytes"))?
            .as_bytes()
            .to_vec();
        out.push((origin, text));
    }
    Ok(out)
}

/// Extract matching blocks from a knit line-delta. Accepts the same
/// `(s_begin, s_end, t_len, _new_text)` hunk tuples as the Python
/// `KnitContent.get_line_delta_blocks` classmethod. Source and target are
/// any indexable sequences whose elements support `!=` — typically byte
/// lines, but the Python tests also pass string lines.
#[pyfunction]
fn get_line_delta_blocks_rs<'py>(
    py: Python<'py>,
    knit_delta: Bound<'py, PyAny>,
    source: Bound<'py, PyAny>,
    target: Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyList>> {
    let mut hunks: Vec<(usize, usize, usize)> = Vec::new();
    for item in knit_delta.try_iter()? {
        let tup = item?;
        let s_begin: usize = tup.get_item(0)?.extract()?;
        let s_end: usize = tup.get_item(1)?.extract()?;
        let t_len: usize = tup.get_item(2)?.extract()?;
        hunks.push((s_begin, s_end, t_len));
    }
    let target_len: usize = target.len()?;
    let not_equal = |a: &Bound<PyAny>, b: &Bound<PyAny>| -> PyResult<bool> { a.ne(b) };

    let mut blocks: Vec<(usize, usize, usize)> = Vec::new();
    let mut s_pos = 0usize;
    let mut t_pos = 0usize;
    for (s_begin, s_end, t_len) in hunks {
        let true_n = s_begin - s_pos;
        let mut n = true_n;
        if n > 0 {
            let sa = source.get_item(s_pos + n - 1)?;
            let tb = target.get_item(t_pos + n - 1)?;
            if not_equal(&sa, &tb)? {
                n -= 1;
            }
            if n > 0 {
                blocks.push((s_pos, t_pos, n));
            }
        }
        t_pos += t_len + true_n;
        s_pos = s_end;
    }
    let mut n = target_len - t_pos;
    if n > 0 {
        let sa = source.get_item(s_pos + n - 1)?;
        let tb = target.get_item(t_pos + n - 1)?;
        if not_equal(&sa, &tb)? {
            n -= 1;
        }
        if n > 0 {
            blocks.push((s_pos, t_pos, n));
        }
    }
    blocks.push((s_pos + (target_len - t_pos), target_len, 0));

    let items: Vec<Bound<PyTuple>> = blocks
        .iter()
        .map(|&(a, b, n)| {
            PyTuple::new(
                py,
                [
                    a.into_pyobject(py)?.into_any(),
                    b.into_pyobject(py)?.into_any(),
                    n.into_pyobject(py)?.into_any(),
                ],
            )
        })
        .collect::<PyResult<_>>()?;
    PyList::new(py, items)
}

/// Parse a knit network record header (everything between the storage-kind
/// line and the raw record body). Returns
/// `(key_tuple, parents_tuple_or_none, noeol, raw_record_offset)`.
#[pyfunction]
fn parse_network_record_header_rs<'py>(
    py: Python<'py>,
    bytes: &'py [u8],
    line_end: usize,
) -> PyResult<(Bound<'py, PyTuple>, Bound<'py, PyAny>, bool, usize)> {
    let header = parse_network_record_header(bytes, line_end)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    let key = PyTuple::new(py, header.key.iter().map(|s| PyBytes::new(py, s)))?;
    let parents: Bound<PyAny> = match header.parents {
        None => py.None().into_bound(py),
        Some(parents) => PyTuple::new(
            py,
            parents
                .iter()
                .map(|p| PyTuple::new(py, p.iter().map(|s| PyBytes::new(py, s))).unwrap()),
        )?
        .into_any(),
    };
    // Compute offset of raw record from the start of the input. This avoids
    // returning a fresh bytes copy so the Python caller can keep using a
    // memoryview / slice over the original buffer.
    let raw_offset = bytes.len() - header.raw_record.len();
    Ok((key, parents, header.noeol, raw_offset))
}

/// Decompress and split a knit record body, returning
/// `((method, version_id, count, digest), record_contents)`.
///
/// Mirrors `_KnitData._parse_record_unchecked`. On corruption raises
/// `ValueError` with a descriptive message; the Python caller rewraps it
/// as `KnitCorrupt(self, ...)`.
#[pyfunction]
fn parse_record_unchecked_rs<'py>(
    py: Python<'py>,
    data: &[u8],
) -> PyResult<(Bound<'py, PyTuple>, Bound<'py, pyo3::types::PyList>)> {
    let (rec, contents) = bazaar::knit::parse_record_unchecked(data)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    let header = PyTuple::new(
        py,
        [
            PyBytes::new(py, &rec.method).into_any(),
            PyBytes::new(py, &rec.version_id).into_any(),
            // Python historically returns the count field as bytes (it was
            // not converted). The caller does `int(rec[2])` itself.
            PyBytes::new(py, rec.count.to_string().as_bytes()).into_any(),
            PyBytes::new(py, &rec.digest).into_any(),
        ],
    )?;
    let list = pyo3::types::PyList::empty(py);
    for line in &contents {
        list.append(PyBytes::new(py, line))?;
    }
    Ok((header, list))
}

/// Parse a knit record and verify that its embedded version matches
/// `expected_version`, returning `(body_lines, digest)`. Mirrors
/// `_KnitData._parse_record`: combines gzip decode, header parse,
/// validation, and version check into a single FFI call so the hot
/// read path only crosses the boundary once per record.
#[pyfunction]
fn parse_record_rs<'py>(
    py: Python<'py>,
    expected_version: &[u8],
    data: &[u8],
) -> PyResult<(Bound<'py, pyo3::types::PyList>, Bound<'py, PyBytes>)> {
    let (body, digest) = bazaar::knit::parse_record(expected_version, data)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    let list = pyo3::types::PyList::empty(py);
    for line in &body {
        list.append(PyBytes::new(py, line))?;
    }
    Ok((list, PyBytes::new(py, &digest)))
}

/// Serialize a knit network record. Inverse of
/// `parse_network_record_header_rs`. Mirrors
/// `KnitContentFactory._create_network_bytes`.
#[pyfunction]
#[pyo3(signature = (storage_kind, key, parents, noeol, raw_record))]
fn build_network_record_rs<'py>(
    py: Python<'py>,
    storage_kind: &str,
    key: Vec<Vec<u8>>,
    parents: Option<Vec<Vec<Vec<u8>>>>,
    noeol: bool,
    raw_record: &[u8],
) -> Bound<'py, PyBytes> {
    let out = bazaar::knit::build_network_record(
        storage_kind.as_bytes(),
        &key,
        parents.as_deref(),
        noeol,
        raw_record,
    );
    PyBytes::new(py, &out)
}

/// Compute total raw byte count needed to materialise `keys` from a knit,
/// walking the compression-parent chain via `positions`.
///
/// Mirrors `bzrformats.knit._get_total_build_size`: each `positions` entry
/// is `(info, index_memo, compression_parent)`, and the third element of
/// `index_memo` is the compressed byte length to sum. Keys missing from
/// `positions` (the "stacked fallback" case) are skipped. Duplicate compression
/// parents are followed only once.
#[pyfunction]
fn get_total_build_size_rs(
    py: Python<'_>,
    keys: Bound<'_, pyo3::types::PyAny>,
    positions: Bound<'_, pyo3::types::PyDict>,
) -> PyResult<usize> {
    use pyo3::types::{PyAnyMethods, PyDict};

    // `seen` holds every key we've ever scheduled (to dedupe the frontier
    // across and within levels — multiple children can share a compression
    // parent). Values are the stored `index_memo` when the key actually
    // resolved in `positions`, or `None` for stacked-fallback keys that we
    // skip. We tally the total at the end from this single map.
    let seen: Bound<'_, PyDict> = PyDict::new(py);
    let mut frontier: Vec<Bound<'_, pyo3::types::PyAny>> = Vec::new();
    for key in keys.try_iter()? {
        let k = key?;
        if !seen.contains(&k)? {
            seen.set_item(&k, py.None())?;
            frontier.push(k);
        }
    }

    while !frontier.is_empty() {
        let mut next: Vec<Bound<'_, pyo3::types::PyAny>> = Vec::new();
        for key in frontier.drain(..) {
            let Some(entry) = positions.get_item(&key)? else {
                continue;
            };
            let tuple = entry.cast_into::<PyTuple>()?;
            let index_memo = tuple.get_item(1)?;
            let compression_parent = tuple.get_item(2)?;
            seen.set_item(&key, &index_memo)?;
            if !compression_parent.is_none() && !seen.contains(&compression_parent)? {
                seen.set_item(&compression_parent, py.None())?;
                next.push(compression_parent);
            }
        }
        frontier = next;
    }

    let mut total: usize = 0;
    for (_k, memo) in seen.iter() {
        if memo.is_none() {
            continue;
        }
        let memo_tuple = memo.cast_into::<PyTuple>()?;
        total += memo_tuple.get_item(2)?.extract::<usize>()?;
    }
    Ok(total)
}

/// Group `keys` by their first segment, preserving first-seen order.
/// Mirrors `KnitVersionedFiles._split_by_prefix`. Returns
/// `(split_by_prefix_dict, prefix_order_list)`. Single-segment keys land
/// under the empty-bytes prefix.
#[pyfunction]
fn split_keys_by_prefix_rs<'py>(
    py: Python<'py>,
    keys: Vec<Vec<Vec<u8>>>,
) -> PyResult<(
    Bound<'py, pyo3::types::PyDict>,
    Bound<'py, pyo3::types::PyList>,
)> {
    let (buckets, prefix_order) = bazaar::knit::split_keys_by_prefix(&keys);
    let out_dict = pyo3::types::PyDict::new(py);
    for (prefix, bucket_keys) in &buckets {
        let list = pyo3::types::PyList::empty(py);
        for key in bucket_keys {
            let tuple = PyTuple::new(py, key.iter().map(|seg| PyBytes::new(py, seg)))?;
            list.append(tuple)?;
        }
        out_dict.set_item(PyBytes::new(py, prefix), list)?;
    }
    let order_list = pyo3::types::PyList::empty(py);
    for prefix in &prefix_order {
        order_list.append(PyBytes::new(py, prefix))?;
    }
    Ok((out_dict, order_list))
}

/// Serialize a knit-delta-closure wire record. Mirrors
/// `_ContentMapGenerator._wire_bytes`.
///
/// `records` is a list of
/// `(key, parents_or_none, method, noeol, next_or_none, record_bytes)` tuples,
/// where `parents_or_none` is `None` for the literal `None:` line and
/// `key`/`next`/each parent key are tuples of bytes.
#[pyfunction]
#[pyo3(signature = (annotated, emit_keys, records))]
fn build_knit_delta_closure_wire_rs<'py>(
    py: Python<'py>,
    annotated: bool,
    emit_keys: Vec<Vec<Vec<u8>>>,
    records: Vec<(
        Vec<Vec<u8>>,
        Option<Vec<Vec<Vec<u8>>>>,
        String,
        bool,
        Option<Vec<Vec<u8>>>,
        Vec<u8>,
    )>,
) -> Bound<'py, PyBytes> {
    // With KnitDeltaClosureRecord now generic over Seg: AsRef<[u8]>, we can
    // use Vec<u8> directly as the segment type and only need one level of
    // slice shells (for each record's parent list, since the struct field
    // is `&[&[Seg]]`).
    let parent_slices: Vec<Option<Vec<&[Vec<u8>]>>> = records
        .iter()
        .map(|(_, parents, ..)| {
            parents
                .as_ref()
                .map(|ps| ps.iter().map(|p| p.as_slice()).collect())
        })
        .collect();

    let record_refs: Vec<bazaar::knit::KnitDeltaClosureRecord<'_, Vec<u8>>> = records
        .iter()
        .zip(parent_slices.iter())
        .map(|((key, _, method, noeol, next, record_bytes), parents)| {
            bazaar::knit::KnitDeltaClosureRecord {
                key: key.as_slice(),
                parents: parents.as_deref(),
                method: method.as_bytes(),
                noeol: *noeol,
                next: next.as_deref(),
                record_bytes: record_bytes.as_slice(),
            }
        })
        .collect();

    let out = bazaar::knit::build_knit_delta_closure_wire(annotated, &emit_keys, &record_refs);
    PyBytes::new(py, &out)
}

/// Parse a `_KnitGraphIndex` entry's value field. Thin wrapper around
/// [`bazaar::knit::parse_knit_index_value`]; returns `(noeol, pos, size)`.
#[pyfunction]
fn parse_knit_index_value_rs(value: &[u8]) -> PyResult<(bool, u64, u64)> {
    let parsed = bazaar::knit::parse_knit_index_value(value).map_err(knit_err_to_py)?;
    Ok((parsed.noeol, parsed.pos, parsed.size))
}

/// Newtype wrapping a Python object so it can be used as a HashMap key
/// in pure-Rust algorithms. Hash and equality delegate to Python's
/// `__hash__` / `__eq__` by attaching to a `Python<'_>` token at call
/// time. Used by `walk_components_positions_rs` to feed
/// `bazaar::knit::walk_compression_closure` opaque key tuples without
/// reimplementing the BFS in the pyo3 layer.
struct PyKey(Py<PyAny>);

impl PyKey {
    fn new(b: Bound<'_, PyAny>) -> Self {
        Self(b.unbind())
    }
}

impl Clone for PyKey {
    fn clone(&self) -> Self {
        Python::attach(|py| Self(self.0.clone_ref(py)))
    }
}

impl PartialEq for PyKey {
    fn eq(&self, other: &Self) -> bool {
        Python::attach(|py| {
            self.0
                .bind(py)
                .eq(other.0.bind(py))
                .expect("Python __eq__ must not raise on knit keys")
        })
    }
}
impl Eq for PyKey {}

impl std::hash::Hash for PyKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let h = Python::attach(|py| {
            self.0
                .bind(py)
                .hash()
                .expect("Python __hash__ must not raise on knit keys")
        });
        state.write_isize(h);
    }
}

/// Walk the transitive compression closure of `initial_keys`, batching
/// lookups via the Python callable `lookup_batch`.
///
/// `lookup_batch` takes a list of keys and returns the dict
/// `_KnitGraphIndex.get_build_details` produces — `{key: (index_memo,
/// compression_parent_or_None, parents, record_details), ...}`. Missing
/// keys are detected by absence from the returned dict; if
/// `allow_missing` is False the wrapper raises RevisionNotPresent for
/// the first missing key.
///
/// Returns the assembled `component_data` dict that
/// `KnitVersionedFiles._get_components_positions` would have built:
/// `{key: (record_details, index_memo, compression_parent), ...}`.
///
/// The BFS traversal lives in [`bazaar::knit::walk_compression_closure`];
/// this function is just marshalling — wrap each Python key in a
/// `PyKey`, call the pure-Rust algorithm, then translate the resulting
/// `HashMap<PyKey, payload>` back into a `PyDict`.
#[pyfunction]
fn walk_components_positions_rs<'py>(
    py: Python<'py>,
    initial_keys: Bound<'py, PyAny>,
    allow_missing: bool,
    lookup_batch: Bound<'py, PyAny>,
) -> PyResult<Bound<'py, pyo3::types::PyDict>> {
    use bazaar::knit::{walk_compression_closure, ClosureBatch};

    let mut initial: Vec<PyKey> = Vec::new();
    for k in initial_keys.try_iter()? {
        initial.push(PyKey::new(k?));
    }

    // Per-key payload carries the three opaque pieces the final result
    // dict needs: (record_details, index_memo, compression_parent). The
    // algorithm itself only inspects the compression parent (as the
    // separate `Option<K>` field of `ClosureBatch.present`) — the
    // payload is just data that gets handed back at the end.
    type Payload = (Py<PyAny>, Py<PyAny>, Py<PyAny>);
    let mut callback_err: Option<PyErr> = None;

    let walked = walk_compression_closure::<PyKey, Payload, _>(initial, allow_missing, |batch| {
        let inner = || -> PyResult<ClosureBatch<PyKey, Payload>> {
            let pending_list = pyo3::types::PyList::new(py, batch.iter().map(|k| k.0.bind(py)))?;
            let lookup = lookup_batch
                .call1((pending_list,))?
                .cast_into::<pyo3::types::PyDict>()?;
            let mut present: std::collections::HashMap<PyKey, (Option<PyKey>, Payload)> =
                std::collections::HashMap::new();
            let mut missing: std::collections::HashSet<PyKey> = std::collections::HashSet::new();
            for k in batch {
                if !lookup.contains(k.0.bind(py))? {
                    missing.insert(k.clone());
                }
            }
            for (key, details) in lookup.iter() {
                let details_tuple = details.cast_into::<PyTuple>()?;
                let index_memo = details_tuple.get_item(0)?;
                let compression_parent = details_tuple.get_item(1)?;
                let record_details = details_tuple.get_item(3)?;
                let cp = if compression_parent.is_none() {
                    None
                } else {
                    Some(PyKey::new(compression_parent.clone()))
                };
                present.insert(
                    PyKey::new(key),
                    (
                        cp,
                        (
                            record_details.unbind(),
                            index_memo.unbind(),
                            compression_parent.unbind(),
                        ),
                    ),
                );
            }
            Ok(ClosureBatch { present, missing })
        };
        match inner() {
            Ok(b) => b,
            Err(e) => {
                callback_err = Some(e);
                ClosureBatch {
                    present: std::collections::HashMap::new(),
                    missing: std::collections::HashSet::new(),
                }
            }
        }
    });

    if let Some(e) = callback_err {
        return Err(e);
    }

    let walked = match walked {
        Ok(map) => map,
        Err(missing) => {
            let key: Py<PyAny> = missing
                .into_iter()
                .next()
                .map(|k| k.0)
                .unwrap_or_else(|| py.None());
            return Err(RevisionNotPresent::new_err((key, py.None())));
        }
    };

    let component_data = pyo3::types::PyDict::new(py);
    for (key, (record_details, index_memo, compression_parent)) in walked {
        let py_key = key.0.bind(py);
        let entry = PyTuple::new(
            py,
            [
                record_details.into_bound(py),
                index_memo.into_bound(py),
                compression_parent.into_bound(py),
            ],
        )?;
        component_data.set_item(py_key, entry)?;
    }

    Ok(component_data)
}

/// Walk the compression chain starting at `initial_parent` to decide
/// whether a new record should be stored as a delta. `get_step` is a
/// Python callable that takes a parent key and returns either
/// `(size, compression_parent_or_None)` or `None` if the parent isn't
/// locally present.
///
/// Returns one of `"use-delta"`, `"fulltext-smaller"`, `"chain-too-long"`,
/// `"missing-parent"` — the four `DeltaDecision` variants. The Python
/// caller turns the first variant into `True` and the others into
/// `False` to match the historical `_check_should_delta` bool return.
#[pyfunction]
fn check_should_delta_rs<'py>(
    initial_parent: Bound<'py, PyAny>,
    max_chain: usize,
    get_step: Bound<'py, PyAny>,
) -> PyResult<&'static str> {
    use bazaar::knit::{should_use_delta, ChainStep, DeltaDecision};

    let mut callback_err: Option<PyErr> = None;
    let decision = should_use_delta(initial_parent, max_chain, |parent| {
        match get_step.call1((parent.clone(),)) {
            Err(e) => {
                callback_err = Some(e);
                None
            }
            Ok(result) => {
                if result.is_none() {
                    return None;
                }
                let tup = match result.cast_into::<PyTuple>() {
                    Ok(t) => t,
                    Err(e) => {
                        callback_err = Some(e.into());
                        return None;
                    }
                };
                let size: u64 = match tup.get_item(0).and_then(|o| o.extract::<u64>()) {
                    Ok(s) => s,
                    Err(e) => {
                        callback_err = Some(e);
                        return None;
                    }
                };
                let cp_obj = match tup.get_item(1) {
                    Ok(o) => o,
                    Err(e) => {
                        callback_err = Some(e);
                        return None;
                    }
                };
                let compression_parent = if cp_obj.is_none() { None } else { Some(cp_obj) };
                Some(ChainStep {
                    size,
                    compression_parent,
                })
            }
        }
    });
    if let Some(e) = callback_err {
        return Err(e);
    }
    Ok(match decision {
        DeltaDecision::UseDelta => "use-delta",
        DeltaDecision::FulltextSmaller => "fulltext-smaller",
        DeltaDecision::ChainTooLong => "chain-too-long",
        DeltaDecision::MissingParent => "missing-parent",
    })
}

/// Decide method + noeol for a `_KndxIndex` cache row's options list.
/// Returns `(method_str, noeol)`.
#[pyfunction]
fn decode_kndx_options_rs<'py>(
    py: Python<'py>,
    options: Vec<Vec<u8>>,
) -> PyResult<(Bound<'py, PyAny>, bool)> {
    let (method, noeol) = bazaar::knit::decode_kndx_options(&options).map_err(knit_err_to_py)?;
    Ok((knit_method_to_py(py, method), noeol))
}

/// Build the per-key result dict that `_KnitGraphIndex.get_build_details`
/// returns, given an iterable of GraphIndex entry tuples
/// `(graph_index, key, value, refs)`.
///
/// All the actual decoding work — value-string parsing, fulltext-vs-delta
/// dispatch, compression-parent-count validation — lives in
/// [`bazaar::knit::decode_knit_build_details`]. This wrapper only marshals
/// Python tuples in and out and threads through the opaque `graph_index`
/// pointer that ends up as the first element of the `index_memo` tuple.
#[pyfunction]
fn knit_entries_to_build_details_rs<'py>(
    py: Python<'py>,
    entries: Bound<'py, PyAny>,
    has_parents: bool,
    has_deltas: bool,
) -> PyResult<Bound<'py, pyo3::types::PyDict>> {
    let result = pyo3::types::PyDict::new(py);
    let empty_parents = PyTuple::empty(py);

    for entry in entries.try_iter()? {
        let entry_tuple = entry?.cast_into::<PyTuple>()?;
        let graph_index = entry_tuple.get_item(0)?;
        let key = entry_tuple.get_item(1)?;
        let value_pb = entry_tuple.get_item(2)?.cast_into::<PyBytes>()?;
        let refs = entry_tuple.get_item(3)?;

        let compression_parent_count = if has_deltas {
            refs.get_item(1)?.len()?
        } else {
            0
        };
        let details = bazaar::knit::decode_knit_build_details(
            value_pb.as_bytes(),
            has_deltas,
            compression_parent_count,
        )
        .map_err(knit_err_to_py)?;

        let parents = if has_parents {
            refs.get_item(0)?
        } else {
            empty_parents.clone().into_any()
        };

        let compression_parent_key: Bound<'py, PyAny> = match details.compression_parent {
            Some(idx) => refs.get_item(1)?.get_item(idx)?,
            None => py.None().into_bound(py),
        };

        let index_memo = PyTuple::new(
            py,
            [
                graph_index.into_any(),
                details.pos.into_pyobject(py)?.into_any(),
                details.size.into_pyobject(py)?.into_any(),
            ],
        )?;
        let record_details = PyTuple::new(
            py,
            [
                knit_method_to_py(py, details.method),
                details.noeol.into_pyobject(py)?.to_owned().into_any(),
            ],
        )?;

        let value_tuple = PyTuple::new(
            py,
            [
                index_memo.into_any(),
                compression_parent_key,
                parents,
                record_details.into_any(),
            ],
        )?;
        result.set_item(key, value_tuple)?;
    }
    Ok(result)
}

fn knit_method_to_py<'py>(py: Python<'py>, method: bazaar::knit::KnitMethod) -> Bound<'py, PyAny> {
    let s = match method {
        bazaar::knit::KnitMethod::Fulltext => pyo3::intern!(py, "fulltext"),
        bazaar::knit::KnitMethod::LineDelta => pyo3::intern!(py, "line-delta"),
        bazaar::knit::KnitMethod::NoEol => pyo3::intern!(py, "no-eol"),
    };
    s.clone().into_any()
}

/// Decompress only enough of a knit record to parse its header. Returns
/// `(method, version_id, count, digest)` without validating the line count
/// or end marker — `_KnitData._read_records_iter_raw` relies on this
/// leniency.
#[pyfunction]
fn parse_record_header_only_rs<'py>(py: Python<'py>, data: &[u8]) -> PyResult<Bound<'py, PyTuple>> {
    let rec = bazaar::knit::parse_record_header_only(data)
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    PyTuple::new(
        py,
        [
            PyBytes::new(py, &rec.method).into_any(),
            PyBytes::new(py, &rec.version_id).into_any(),
            PyBytes::new(py, rec.count.to_string().as_bytes()).into_any(),
            PyBytes::new(py, &rec.digest).into_any(),
        ],
    )
}

/// Serialize a knit record: build the header, assemble header + payload +
/// end-marker chunks, and gzip-compress them. Returns
/// `(compressed_len, compressed_chunks)`. Raises `ValueError` if
/// `has_trailing_newline` is false; the caller rewraps as needed.
#[pyfunction]
#[pyo3(signature = (version_id, digest, line_count, payload, has_trailing_newline))]
fn record_to_data_rs<'py>(
    py: Python<'py>,
    version_id: &[u8],
    digest: &[u8],
    line_count: usize,
    payload: Vec<Vec<u8>>,
    has_trailing_newline: bool,
) -> PyResult<(usize, Bound<'py, pyo3::types::PyList>)> {
    let (len, chunks) = bazaar::knit::record_to_data(
        version_id,
        digest,
        line_count,
        &payload,
        has_trailing_newline,
    )
    .map_err(|e| PyValueError::new_err(e.to_string()))?;
    let list = pyo3::types::PyList::empty(py);
    for c in &chunks {
        list.append(PyBytes::new(py, c))?;
    }
    Ok((len, list))
}

// These wrap a Python `_KnitGraphIndex` / `_KndxIndex` and a
// `_KnitKeyAccess` / `_DirectPackAccess` respectively, exposing them
// as pure-Rust `bazaar::knit::KnitIndex` / `KnitAccess` implementors so
// the pure-Rust `get_text` pipeline can drive a Python-side knit.
//
// Memo-shuttling note: the Python side's `index_memo` is an opaque
// `(graph_index_or_prefix, pos, size)` tuple where the first element
// identifies the file the bytes live in (a `GraphIndex` object for a
// pack, a prefix key tuple for a kndx). The pure-Rust
// `KnitIndexMemo { path, offset, length }` doesn't carry arbitrary
// Python objects, so the index adapter interns just that first element
// in a shared, deduplicated [`MemoTable`] and synthesises a
// `path = format!("py:{slot}")` for it. `offset`/`length` come straight
// from the memo tuple. The matching access adapter rebuilds the
// `(first_element, offset, length)` tuple and calls
// `py_access.get_raw_records([memo])` to recover the bytes.
//
// Interning the *first element* (rather than the whole memo) is what
// makes `path` identify the file: every record in the same pack shares
// a slot, so `sort_keys_by_io` can group by `(path, offset)` and read
// each pack in position order. Both adapters share the same
// `Arc<Mutex<...>>` so the round-trip works within one call.

use bazaar::knit::{
    get_content as rust_get_content, get_sha1s as rust_get_sha1s, get_text as rust_get_text,
};
use std::sync::{Arc, Mutex};

/// File-reference for Python-backed knit indices.
///
/// Wraps the first element of a Python `(file_id, offset, length)` index
/// memo tuple. Equality / hash / ordering use the Python object's
/// pointer address (a stable per-object id), which is enough to group
/// records by file in `sort_keys_by_io` without ever needing the GIL.
///
/// Replaces the old MemoTable / slot-path indirection — the file id is
/// carried inline in [`KnitIndexMemo<PyFileRef>`] rather than parked in
/// a side table keyed by a synthetic `"py:N"` string.
#[derive(Debug)]
pub struct PyFileRef(pub(crate) Py<PyAny>);

impl Clone for PyFileRef {
    fn clone(&self) -> Self {
        Python::attach(|py| PyFileRef(self.0.clone_ref(py)))
    }
}

impl PartialEq for PyFileRef {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_ptr() == other.0.as_ptr()
    }
}

impl Eq for PyFileRef {}

impl std::hash::Hash for PyFileRef {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        (self.0.as_ptr() as usize).hash(state);
    }
}

impl PartialOrd for PyFileRef {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PyFileRef {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        (self.0.as_ptr() as usize).cmp(&(other.0.as_ptr() as usize))
    }
}

impl bazaar::knit::FileRef for PyFileRef {
    fn placeholder() -> Self {
        // Use Py_None as the placeholder identity. Acquiring the GIL is
        // unavoidable here, but the only call sites are absent-record
        // construction, which already runs under the interpreter.
        Python::attach(|py| PyFileRef(py.None()))
    }
}

/// Rebuild the Python `(file_id, offset, length)` index_memo tuple from
/// a [`KnitIndexMemo<PyFileRef>`]. The file id is carried inline by
/// `PyFileRef`; just wrap it together with the byte range.
fn rebuild_py_memo(
    py: Python<'_>,
    memo: &KnitIndexMemo<PyFileRef>,
) -> Result<Py<PyAny>, KnitError> {
    let tuple = PyTuple::new(
        py,
        [
            memo.file_ref.0.clone_ref(py).into_bound(py),
            memo.offset
                .into_pyobject(py)
                .map_err(|e| knit_err_from_py(py, e.into()))?
                .into_any(),
            memo.length
                .into_pyobject(py)
                .map_err(|e| knit_err_from_py(py, e.into()))?
                .into_any(),
        ],
    )
    .map_err(|e| knit_err_from_py(py, e))?;
    Ok(tuple.into_any().unbind())
}

/// Adapter that exposes a Python `_KnitGraphIndex` / `_KndxIndex` as a
/// pure-Rust [`KnitIndexTrait`].
///
/// The Python `get_build_details(keys)` returns the dict shape
/// `{key: (index_memo, compression_parent, parents, (method, noeol))}`;
/// this adapter walks each entry and stores the opaque Python
/// `index_memo`'s file id directly as a [`PyFileRef`] inside the
/// `KnitRecordDetails`.
pub struct PyKnitIndex {
    py_index: Py<PyAny>,
}

impl PyKnitIndex {
    pub fn new(py_index: Bound<'_, PyAny>) -> Self {
        Self {
            py_index: py_index.unbind(),
        }
    }

    /// Assign a deterministic rank to each distinct file-identity
    /// referenced by `positions`.
    ///
    /// The file-identities (the first element of each Python `index_memo`)
    /// arrive from `get_build_details` in HashMap iteration order, which
    /// is not stable. Ranking them by their Python value gives a stable
    /// order. On a comparison failure (which a GraphIndex or key tuple
    /// never produces) we fall back to pointer-id order so the sort stays
    /// total and deterministic.
    fn rank_file_identities(
        &self,
        positions: &std::collections::HashMap<KnitKey, KnitRecordDetails<PyFileRef>>,
    ) -> std::collections::HashMap<PyFileRef, usize> {
        let mut idents: Vec<PyFileRef> = positions
            .values()
            .map(|det| det.index_memo.file_ref.clone())
            .collect();
        idents.sort();
        idents.dedup();
        Python::attach(|py| {
            idents.sort_by(|a, b| {
                a.0.bind(py)
                    .compare(b.0.bind(py))
                    .unwrap_or_else(|_| a.cmp(b))
            });
            idents
                .into_iter()
                .enumerate()
                .map(|(rank, fr)| (fr, rank))
                .collect()
        })
    }
}

/// Look up the file-identity rank of a memo (see
/// [`PyKnitIndex::rank_file_identities`]). An unranked identity sorts last.
fn file_identity_rank(
    ranks: &std::collections::HashMap<PyFileRef, usize>,
    memo: &KnitIndexMemo<PyFileRef>,
) -> usize {
    ranks
        .get(&memo.file_ref)
        .copied()
        .unwrap_or(usize::MAX)
}

thread_local! {
    /// Stash for a Python exception that crossed into Rust through
    /// [`knit_err_from_py`] but did not match a known [`KnitError`] variant.
    /// [`knit_err_to_py`] checks the stash so the original `PyErr` (e.g.
    /// `ObjectNotLocked`) is re-raised verbatim rather than being remapped
    /// to `KnitCorrupt`.
    static STASHED_PY_ERR: std::cell::RefCell<Option<PyErr>> = const { std::cell::RefCell::new(None) };
}

/// Pop the stashed Python error (if any). Called by [`knit_err_to_py`]
/// after it sees the [`KnitError::Aborted`] sentinel.
fn take_stashed_py_err() -> Option<PyErr> {
    STASHED_PY_ERR.with(|cell| cell.borrow_mut().take())
}

pub(crate) fn knit_err_from_py(py: Python<'_>, err: PyErr) -> KnitError {
    if err.is_instance_of::<PyNotImplementedError>(py) {
        return KnitError::NotImplemented("operation not implemented by Python index");
    }
    if err.is_instance_of::<RevisionNotPresent>(py) {
        // Extract the offending key from the exception when possible.
        if let Ok(args) = err
            .value(py)
            .getattr("args")
            .and_then(|a| a.extract::<Vec<Py<PyAny>>>())
        {
            if let Some(key_obj) = args.into_iter().next() {
                if let Ok(key) = extract_knit_key(key_obj.bind(py)) {
                    return KnitError::RevisionNotPresent(key);
                }
            }
        }
        return KnitError::RevisionNotPresent(vec![]);
    }
    // Preserve any other Python exception (ObjectNotLocked, ReadOnlyError,
    // ...) by stashing it on a thread-local and returning the Aborted
    // sentinel; knit_err_to_py will re-raise it verbatim.
    let summary = err.to_string();
    STASHED_PY_ERR.with(|cell| *cell.borrow_mut() = Some(err));
    KnitError::Aborted(summary)
}

fn extract_knit_key(obj: &Bound<'_, PyAny>) -> Result<KnitKey, KnitError> {
    let tup = obj
        .cast::<PyTuple>()
        .map_err(|_| KnitError::BadIndexValue(b"key is not a tuple".to_vec()))?;
    let mut out = Vec::with_capacity(tup.len());
    for i in 0..tup.len() {
        let item = tup
            .get_item(i)
            .map_err(|e| KnitError::BadIndexValue(e.to_string().into_bytes()))?;
        let bytes = item
            .cast_into::<PyBytes>()
            .map_err(|_| KnitError::BadIndexValue(b"key segment is not bytes".to_vec()))?;
        out.push(bytes.as_bytes().to_vec());
    }
    Ok(out)
}

fn knit_key_to_py<'py>(py: Python<'py>, key: &KnitKey) -> PyResult<Bound<'py, PyTuple>> {
    PyTuple::new(py, key.iter().map(|seg| PyBytes::new(py, seg)))
}

impl KnitIndexTrait for PyKnitIndex {
    type F = PyFileRef;

    fn get_build_details(
        &self,
        keys: &[KnitKey],
    ) -> Result<std::collections::HashMap<KnitKey, KnitRecordDetails<PyFileRef>>, KnitError> {
        Python::attach(
            |py| -> Result<
                std::collections::HashMap<KnitKey, KnitRecordDetails<PyFileRef>>,
                KnitError,
            > {
                let py_keys = pyo3::types::PyList::empty(py);
                for k in keys {
                    let tup = knit_key_to_py(py, k).map_err(|e| knit_err_from_py(py, e))?;
                    py_keys.append(tup).map_err(|e| knit_err_from_py(py, e))?;
                }
                let result = self
                    .py_index
                    .bind(py)
                    .call_method1("get_build_details", (py_keys,))
                    .map_err(|e| knit_err_from_py(py, e))?;
                let dict = result.cast_into::<PyDict>().map_err(|_| {
                    KnitError::BadIndexValue(b"get_build_details did not return a dict".to_vec())
                })?;
                let mut out = std::collections::HashMap::new();
                for (key_obj, value_obj) in dict.iter() {
                    let key = extract_knit_key(&key_obj)?;
                    let tup = value_obj.cast_into::<PyTuple>().map_err(|_| {
                        KnitError::BadIndexValue(b"build_details value is not a tuple".to_vec())
                    })?;
                    if tup.len() != 4 {
                        return Err(KnitError::BadIndexValue(
                            b"build_details tuple is not 4-element".to_vec(),
                        ));
                    }
                    let py_memo = tup.get_item(0).map_err(|e| knit_err_from_py(py, e))?;
                    let cp_obj = tup.get_item(1).map_err(|e| knit_err_from_py(py, e))?;
                    let parents_obj = tup.get_item(2).map_err(|e| knit_err_from_py(py, e))?;
                    let record_details_tup = tup
                        .get_item(3)
                        .map_err(|e| knit_err_from_py(py, e))?
                        .cast_into::<PyTuple>()
                        .map_err(|_| {
                            KnitError::BadIndexValue(b"record_details is not a tuple".to_vec())
                        })?;

                    let method_str: String = record_details_tup
                        .get_item(0)
                        .map_err(|e| knit_err_from_py(py, e))?
                        .extract()
                        .map_err(|e| knit_err_from_py(py, e))?;
                    let noeol: bool = record_details_tup
                        .get_item(1)
                        .map_err(|e| knit_err_from_py(py, e))?
                        .extract()
                        .map_err(|e| knit_err_from_py(py, e))?;
                    let method = match method_str.as_str() {
                        "fulltext" => KnitMethod::Fulltext,
                        "line-delta" => KnitMethod::LineDelta,
                        other => {
                            return Err(KnitError::BadIndexValue(other.as_bytes().to_vec()));
                        }
                    };

                    // Split the index_memo tuple into (file_id, pos,
                    // size). The first element identifies the file; we
                    // intern it in the deduplicated side table so every
                    // record from the same file shares a slot.
                    let memo_tup = py_memo.clone().cast_into::<PyTuple>().map_err(|_| {
                        KnitError::BadIndexValue(b"index_memo is not a tuple".to_vec())
                    })?;
                    let file_id = memo_tup.get_item(0).map_err(|e| knit_err_from_py(py, e))?;
                    let pos: u64 = memo_tup
                        .get_item(1)
                        .map_err(|e| knit_err_from_py(py, e))?
                        .extract()
                        .map_err(|e| knit_err_from_py(py, e))?;
                    let length: u64 = memo_tup
                        .get_item(2)
                        .map_err(|e| knit_err_from_py(py, e))?
                        .extract()
                        .map_err(|e| knit_err_from_py(py, e))?;
                    let index_memo = KnitIndexMemo {
                        file_ref: PyFileRef(file_id.unbind()),
                        offset: pos,
                        length: length as usize,
                    };

                    let compression_parent = if cp_obj.is_none() {
                        None
                    } else {
                        Some(extract_knit_key(&cp_obj)?)
                    };

                    let mut parents = Vec::new();
                    if !parents_obj.is_none() {
                        if let Ok(plist) = parents_obj.cast::<PyTuple>() {
                            for i in 0..plist.len() {
                                let p_obj =
                                    plist.get_item(i).map_err(|e| knit_err_from_py(py, e))?;
                                parents.push(extract_knit_key(&p_obj)?);
                            }
                        } else if let Ok(plist) = parents_obj.cast::<PyList>() {
                            for i in 0..plist.len() {
                                let p_obj =
                                    plist.get_item(i).map_err(|e| knit_err_from_py(py, e))?;
                                parents.push(extract_knit_key(&p_obj)?);
                            }
                        }
                    }

                    out.insert(
                        key,
                        KnitRecordDetails {
                            method,
                            noeol,
                            index_memo,
                            compression_parent,
                            parents,
                        },
                    );
                }
                Ok(out)
            },
        )
    }

    fn keys(&self) -> Result<Vec<KnitKey>, KnitError> {
        Python::attach(|py| -> Result<Vec<KnitKey>, KnitError> {
            let result = self
                .py_index
                .bind(py)
                .call_method0("keys")
                .map_err(|e| knit_err_from_py(py, e))?;
            let mut out = Vec::new();
            for item in result.try_iter().map_err(|e| knit_err_from_py(py, e))? {
                let item = item.map_err(|e| knit_err_from_py(py, e))?;
                out.push(extract_knit_key(&item)?);
            }
            Ok(out)
        })
    }

    fn get_parent_map(
        &self,
        keys: &[KnitKey],
    ) -> Result<std::collections::HashMap<KnitKey, Vec<KnitKey>>, KnitError> {
        Python::attach(
            |py| -> Result<std::collections::HashMap<KnitKey, Vec<KnitKey>>, KnitError> {
                let py_keys = pyo3::types::PyList::empty(py);
                for k in keys {
                    let tup = knit_key_to_py(py, k).map_err(|e| knit_err_from_py(py, e))?;
                    py_keys.append(tup).map_err(|e| knit_err_from_py(py, e))?;
                }
                let result = self
                    .py_index
                    .bind(py)
                    .call_method1("get_parent_map", (py_keys,))
                    .map_err(|e| knit_err_from_py(py, e))?;
                let dict = result.cast_into::<PyDict>().map_err(|_| {
                    KnitError::BadIndexValue(b"get_parent_map did not return a dict".to_vec())
                })?;
                let mut out = std::collections::HashMap::new();
                for (k, v) in dict.iter() {
                    let key = extract_knit_key(&k)?;
                    let mut parents = Vec::new();
                    if !v.is_none() {
                        for p in v.try_iter().map_err(|e| knit_err_from_py(py, e))? {
                            let p = p.map_err(|e| knit_err_from_py(py, e))?;
                            parents.push(extract_knit_key(&p)?);
                        }
                    }
                    out.insert(key, parents);
                }
                Ok(out)
            },
        )
    }

    fn get_method(&self, key: &KnitKey) -> Result<KnitMethod, KnitError> {
        Python::attach(|py| -> Result<KnitMethod, KnitError> {
            let py_key = knit_key_to_py(py, key).map_err(|e| knit_err_from_py(py, e))?;
            let result = self
                .py_index
                .bind(py)
                .call_method1("get_method", (py_key,))
                .map_err(|e| knit_err_from_py(py, e))?;
            let s: String = result.extract().map_err(|e| knit_err_from_py(py, e))?;
            match s.as_str() {
                "fulltext" => Ok(KnitMethod::Fulltext),
                "line-delta" => Ok(KnitMethod::LineDelta),
                other => Err(KnitError::BadIndexValue(other.as_bytes().to_vec())),
            }
        })
    }

    fn get_total_build_size(
        &self,
        keys: &[KnitKey],
        positions: &std::collections::HashMap<KnitKey, KnitRecordDetails<PyFileRef>>,
    ) -> usize {
        Python::attach(|py| -> usize {
            let py_keys = pyo3::types::PyList::empty(py);
            for k in keys {
                if let Ok(tup) = knit_key_to_py(py, k) {
                    let _ = py_keys.append(tup);
                }
            }
            // Build a Python dict of positions to pass to _get_total_build_size.
            // We reconstruct the Python (index_memo, cp, parents, record_details)
            // tuples from KnitRecordDetails — but the Python side only needs the
            // size, so we can pass a simplified mapping.
            // Simplest: just call `_get_total_build_size` if available, otherwise
            // compute it ourselves from positions.
            if let Ok(result) = self
                .py_index
                .bind(py)
                .call_method1("_get_total_build_size", (py_keys, py.None()))
            {
                if let Ok(n) = result.extract::<usize>() {
                    return n;
                }
            }
            // Fallback: sum sizes from the Rust positions map.
            let mut total = 0usize;
            let mut seen = std::collections::HashSet::new();
            let mut queue: std::collections::VecDeque<&KnitKey> = keys.iter().collect();
            while let Some(key) = queue.pop_front() {
                if !seen.insert(key) {
                    continue;
                }
                if let Some(det) = positions.get(key) {
                    total += det.index_memo.length;
                    if let Some(ref cp) = det.compression_parent {
                        if positions.contains_key(cp) {
                            queue.push_back(cp);
                        }
                    }
                }
            }
            total
        })
    }

    fn sort_keys_by_io(
        &self,
        keys: &mut [KnitKey],
        positions: &std::collections::HashMap<KnitKey, KnitRecordDetails<PyFileRef>>,
    ) {
        // Mirror Python's `_KnitGraphIndex._sort_keys_by_io`, which sorts
        // by the index_memo tuple: `(file_identity, pos, size)`. The
        // file_identity (a GraphIndex for a pack, a key tuple for a kndx)
        // groups records by file; `pos` orders within a file.
        //
        // The interned slot in `index_memo.file_ref` is not a usable sort key
        // on its own: slot numbers follow intern (i.e. dict-iteration)
        // order, so for a kndx -- where every record interns its own key
        // as the file_identity -- they would order non-deterministically.
        // So rank the distinct file_identities by their Python value and
        // sort by (rank, pos), giving a deterministic order that still
        // groups records by file.
        let ranks = self.rank_file_identities(positions);
        keys.sort_by(|a, b| {
            let a_key = positions.get(a).map(|d| {
                (
                    file_identity_rank(&ranks, &d.index_memo),
                    d.index_memo.offset,
                )
            });
            let b_key = positions.get(b).map(|d| {
                (
                    file_identity_rank(&ranks, &d.index_memo),
                    d.index_memo.offset,
                )
            });
            a_key.cmp(&b_key).then_with(|| a.cmp(b))
        });
    }

    fn has_graph(&self) -> bool {
        Python::attach(|py| {
            self.py_index
                .bind(py)
                .getattr("has_graph")
                .and_then(|v| v.extract::<bool>())
                .unwrap_or(true)
        })
    }

    fn contains(&self, key: &KnitKey) -> Result<bool, KnitError> {
        Python::attach(|py| -> Result<bool, KnitError> {
            let py_key = knit_key_to_py(py, key).map_err(|e| knit_err_from_py(py, e))?;
            let result = self
                .py_index
                .bind(py)
                .call_method1("__contains__", (py_key,))
                .map_err(|e| knit_err_from_py(py, e))?;
            result
                .extract::<bool>()
                .map_err(|e| knit_err_from_py(py, e))
        })
    }

    fn get_missing_compression_parents(&self) -> Result<Vec<KnitKey>, KnitError> {
        Python::attach(|py| -> Result<Vec<KnitKey>, KnitError> {
            let result = self
                .py_index
                .bind(py)
                .call_method0("get_missing_compression_parents")
                .map_err(|e| knit_err_from_py(py, e))?;
            let mut out = Vec::new();
            for item in result.try_iter().map_err(|e| knit_err_from_py(py, e))? {
                let item = item.map_err(|e| knit_err_from_py(py, e))?;
                out.push(extract_knit_key(&item)?);
            }
            Ok(out)
        })
    }

    fn check_write_ok(&self) -> Result<(), KnitError> {
        Python::attach(|py| -> Result<(), KnitError> {
            self.py_index
                .bind(py)
                .call_method0("_check_write_ok")
                .map_err(|e| knit_err_from_py(py, e))?;
            Ok(())
        })
    }

    fn add_records(
        &self,
        records: &[(
            KnitKey,
            Vec<KnitMethod>,
            KnitIndexMemo<PyFileRef>,
            Vec<KnitKey>,
        )],
        random_id: bool,
        missing_compression_parents: bool,
    ) -> Result<(), KnitError> {
        Python::attach(|py| -> Result<(), KnitError> {
            let py_records = pyo3::types::PyList::empty(py);
            // Rebuild the full (file_id, pos, length) memo tuple each
            // record's index entry needs.
            let py_memos: Vec<Py<PyAny>> = records
                .iter()
                .map(|(_, _, memo, _)| rebuild_py_memo(py, memo))
                .collect::<Result<_, _>>()?;
            for ((key, methods, _memo, parents), py_memo) in records.iter().zip(py_memos) {
                let py_key = knit_key_to_py(py, key).map_err(|e| knit_err_from_py(py, e))?;
                // Build a Python list of bytes from the method list, matching the
                // format that _KndxIndex.add_records expects: [b"fulltext"] or
                // [b"line-delta", b"no-eol"].
                let py_options = pyo3::types::PyList::empty(py);
                for m in methods {
                    py_options
                        .append(pyo3::types::PyBytes::new(py, m.as_str().as_bytes()))
                        .map_err(|e| knit_err_from_py(py, e))?;
                }
                let py_parents = pyo3::types::PyTuple::new(
                    py,
                    parents
                        .iter()
                        .map(|p| knit_key_to_py(py, p))
                        .collect::<PyResult<Vec<_>>>()
                        .map_err(|e| knit_err_from_py(py, e))?,
                )
                .map_err(|e| knit_err_from_py(py, e))?;
                let entry = pyo3::types::PyTuple::new(
                    py,
                    [
                        py_key.into_any(),
                        py_options.into_any(),
                        py_memo.into_bound(py).into_any(),
                        py_parents.into_any(),
                    ],
                )
                .map_err(|e| knit_err_from_py(py, e))?;
                py_records
                    .append(entry)
                    .map_err(|e| knit_err_from_py(py, e))?;
            }
            let kwargs = pyo3::types::PyDict::new(py);
            kwargs
                .set_item("random_id", random_id)
                .map_err(|e| knit_err_from_py(py, e))?;
            kwargs
                .set_item("missing_compression_parents", missing_compression_parents)
                .map_err(|e| knit_err_from_py(py, e))?;
            self.py_index
                .bind(py)
                .call_method("add_records", (py_records,), Some(&kwargs))
                .map_err(|e| knit_err_from_py(py, e))?;
            Ok(())
        })
    }
}

/// Adapter that exposes a Python `_KnitKeyAccess` / `_DirectPackAccess`
/// as a pure-Rust [`KnitAccessTrait`].
///
/// Rebuilds each `(file_id, offset, length)` memo tuple from the
/// [`PyFileRef`] in the memo (carried over directly from the Python
/// index), then calls `py_access.get_raw_records([memo])` and reads
/// the items from the returned iterator.
pub struct PyKnitAccess {
    py_access: Py<PyAny>,
    /// The most recent `RetryWithNewPacks` exception raised by the
    /// Python access layer. `KnitError` cannot carry a `Py<PyAny>`, so a
    /// raised `RetryWithNewPacks` is stashed here and surfaced as
    /// [`KnitError::Retry`]; [`PyKnitAccess::reload_or_raise`] then hands
    /// the stashed exception to the Python `reload_or_raise`.
    pending_retry: Mutex<Option<Py<PyAny>>>,
    /// The error raised by Python's `reload_or_raise` when a retry could
    /// not recover. Stashed here and surfaced as [`KnitError::Aborted`]
    /// so it can be re-raised verbatim at the language boundary instead
    /// of being remapped to `KnitCorrupt`.
    final_error: Mutex<Option<PyErr>>,
}

impl PyKnitAccess {
    pub fn new(py_access: Bound<'_, PyAny>) -> Self {
        Self {
            py_access: py_access.unbind(),
            pending_retry: Mutex::new(None),
            final_error: Mutex::new(None),
        }
    }

    /// Take a stashed unrecoverable error, if any. Used at the pyo3
    /// boundary to re-raise the original exception verbatim.
    pub fn take_final_error(&self) -> Option<PyErr> {
        self.final_error.lock().unwrap().take()
    }

    /// Take the stashed `RetryWithNewPacks` exception, if any. Used at
    /// the pyo3 boundary to re-raise it for an enclosing retry loop.
    pub fn take_pending_retry(&self) -> Option<Py<PyAny>> {
        self.pending_retry.lock().unwrap().take()
    }

    /// Convert a Python error into a [`KnitError`]. If it is a
    /// `RetryWithNewPacks`, stash the exception and return
    /// [`KnitError::Retry`] so the read pipeline retries the operation.
    fn access_err_from_py(&self, py: Python<'_>, err: PyErr) -> KnitError {
        if err.is_instance_of::<RetryWithNewPacks>(py) {
            let ctx = err.to_string();
            *self.pending_retry.lock().unwrap() = Some(err.value(py).clone().into_any().unbind());
            return KnitError::Retry(ctx);
        }
        knit_err_from_py(py, err)
    }
}

impl KnitAccessTrait for PyKnitAccess {
    type F = PyFileRef;

    fn get_raw_record(&self, memo: &KnitIndexMemo<PyFileRef>) -> Result<Vec<u8>, KnitError> {
        Python::attach(|py| -> Result<Vec<u8>, KnitError> {
            let py_memo = rebuild_py_memo(py, memo)?;

            let memos_list = pyo3::types::PyList::empty(py);
            memos_list
                .append(py_memo.bind(py))
                .map_err(|e| knit_err_from_py(py, e))?;
            // get_raw_records may return a generator; RetryWithNewPacks
            // can surface either from the call or while iterating, so
            // route both through access_err_from_py.
            let iter = self
                .py_access
                .bind(py)
                .call_method1("get_raw_records", (memos_list,))
                .map_err(|e| self.access_err_from_py(py, e))?;
            let mut iter = iter
                .try_iter()
                .map_err(|e| self.access_err_from_py(py, e))?;
            let first = iter
                .next()
                .ok_or_else(|| {
                    KnitError::BadIndexValue(b"get_raw_records returned no items".to_vec())
                })?
                .map_err(|e| self.access_err_from_py(py, e))?;
            let bytes = first.cast_into::<PyBytes>().map_err(|_| {
                KnitError::BadIndexValue(b"get_raw_records yielded non-bytes".to_vec())
            })?;
            Ok(bytes.as_bytes().to_vec())
        })
    }

    fn get_raw_records(
        &self,
        memos: &[KnitIndexMemo<PyFileRef>],
    ) -> Result<Vec<Vec<u8>>, KnitError> {
        Python::attach(|py| -> Result<Vec<Vec<u8>>, KnitError> {
            let py_memos = pyo3::types::PyList::empty(py);
            for memo in memos {
                let py_memo = rebuild_py_memo(py, memo)?;
                py_memos
                    .append(py_memo.bind(py))
                    .map_err(|e| knit_err_from_py(py, e))?;
            }
            // get_raw_records may return a generator; RetryWithNewPacks
            // can surface either from the call or while iterating, so
            // route both through access_err_from_py.
            let iter = self
                .py_access
                .bind(py)
                .call_method1("get_raw_records", (py_memos,))
                .map_err(|e| self.access_err_from_py(py, e))?;
            let mut out = Vec::with_capacity(memos.len());
            for item in iter
                .try_iter()
                .map_err(|e| self.access_err_from_py(py, e))?
            {
                let item = item.map_err(|e| self.access_err_from_py(py, e))?;
                let bytes = item.cast_into::<PyBytes>().map_err(|_| {
                    KnitError::BadIndexValue(b"get_raw_records yielded non-bytes".to_vec())
                })?;
                out.push(bytes.as_bytes().to_vec());
            }
            Ok(out)
        })
    }

    fn add_raw_record(
        &self,
        key: &KnitKey,
        size: usize,
        data: Vec<Vec<u8>>,
    ) -> Result<KnitIndexMemo<PyFileRef>, KnitError> {
        Python::attach(|py| -> Result<KnitIndexMemo<PyFileRef>, KnitError> {
            let py_key = knit_key_to_py(py, key).map_err(|e| knit_err_from_py(py, e))?;
            let flat: Vec<u8> = data.into_iter().flatten().collect();
            let py_data = pyo3::types::PyList::new(py, [PyBytes::new(py, &flat)])
                .map_err(|e| knit_err_from_py(py, e))?;
            let result = self
                .py_access
                .bind(py)
                .call_method1("add_raw_record", (py_key, size, py_data))
                .map_err(|e| knit_err_from_py(py, e))?;
            // The returned memo is a (file_id, pos, length) tuple. Intern
            // the file_id and carry pos/length on the KnitIndexMemo.
            let memo_tup = result.cast_into::<PyTuple>().map_err(|_| {
                KnitError::BadIndexValue(b"add_raw_record did not return a tuple".to_vec())
            })?;
            let file_id = memo_tup.get_item(0).map_err(|e| knit_err_from_py(py, e))?;
            let offset: u64 = memo_tup
                .get_item(1)
                .map_err(|e| knit_err_from_py(py, e))?
                .extract()
                .map_err(|e| knit_err_from_py(py, e))?;
            let length: u64 = memo_tup
                .get_item(2)
                .map_err(|e| knit_err_from_py(py, e))?
                .extract()
                .map_err(|e| knit_err_from_py(py, e))?;
            Ok(KnitIndexMemo {
                file_ref: PyFileRef(file_id.unbind()),
                offset,
                length: length as usize,
            })
        })
    }

    fn flush(&self) -> Result<(), KnitError> {
        Python::attach(|py| -> Result<(), KnitError> {
            self.py_access
                .bind(py)
                .call_method0("flush")
                .map_err(|e| knit_err_from_py(py, e))?;
            Ok(())
        })
    }

    fn reload_or_raise(&self, err: KnitError) -> Result<(), KnitError> {
        // The Python `reload_or_raise` needs the original RetryWithNewPacks
        // exception (it reads .reload_occurred and .exc_info). It was
        // stashed by access_err_from_py when KnitError::Retry was produced.
        let retry_exc = match self.pending_retry.lock().unwrap().take() {
            Some(exc) => exc,
            // No stashed exception means this wasn't a retry error.
            None => return Err(err),
        };
        Python::attach(|py| -> Result<(), KnitError> {
            // reload_or_raise either returns (reload succeeded, retry the
            // operation) or raises the underlying error (give up). When
            // it raises, stash that error so it can be re-raised verbatim
            // at the language boundary rather than remapped.
            match self
                .py_access
                .bind(py)
                .call_method1("reload_or_raise", (retry_exc.bind(py),))
            {
                Ok(_) => Ok(()),
                Err(e) => {
                    let ctx = e.to_string();
                    *self.final_error.lock().unwrap() = Some(e);
                    Err(KnitError::Aborted(ctx))
                }
            }
        })
    }
}

/// Reconstruct the text of `key` by driving the pure-Rust
/// `bazaar::knit::get_text` pipeline on top of a Python `_index` /
/// `_access` pair. `annotated` selects between [`KnitAnnotateFactory`]
/// and [`KnitPlainFactory`] for record parsing.
///
/// Mirrors the Python `KnitVersionedFiles.get_text` contract, except
/// it does not consult fallback versioned files — those still live
/// entirely on the Python side.
#[pyfunction]
fn get_text_via_traits_rs<'py>(
    py: Python<'py>,
    py_index: Bound<'py, PyAny>,
    py_access: Bound<'py, PyAny>,
    key: Bound<'py, PyAny>,
    annotated: bool,
) -> PyResult<Bound<'py, PyBytes>> {
    let index = PyKnitIndex::new(py_index);
    let access = PyKnitAccess::new(py_access);
    let knit_key = extract_knit_key(&key).map_err(knit_err_to_py)?;

    let bytes = if annotated {
        rust_get_text(&index, &access, &KnitAnnotateFactory, &knit_key).map_err(knit_err_to_py)?
    } else {
        rust_get_text(&index, &access, &KnitPlainFactory, &knit_key).map_err(knit_err_to_py)?
    };
    Ok(PyBytes::new(py, &bytes))
}

/// Reconstruct a single key's content via the pure-Rust pipeline and
/// return the *raw* per-line data the Python `AnnotatedKnitContent` /
/// `PlainKnitContent` constructors expect, plus the `should_strip_eol`
/// flag.
///
/// For the annotated factory the second tuple element is a list of
/// `(origin_bytes, text_bytes)` pairs; for the plain factory it's a
/// list of bare text bytes. The first tuple element is always the
/// content's owning version_id (used by `PlainKnitContent`; the
/// annotated wrapper just ignores it). The third element is the
/// `should_strip_eol` flag from the final record's noeol bit.
///
/// The Python `KnitVersionedFiles._get_content` wraps these into the
/// matching `KnitContent` subclass — the wrapping itself is a one-line
/// Python call, but the chain walk + delta apply happens entirely in
/// Rust.
#[pyfunction]
fn get_content_via_traits_rs<'py>(
    py: Python<'py>,
    py_index: Bound<'py, PyAny>,
    py_access: Bound<'py, PyAny>,
    key: Bound<'py, PyAny>,
    annotated: bool,
) -> PyResult<(Bound<'py, PyBytes>, Bound<'py, PyAny>, bool)> {
    let index = PyKnitIndex::new(py_index);
    let access = PyKnitAccess::new(py_access);
    let knit_key = extract_knit_key(&key).map_err(knit_err_to_py)?;
    let last_segment = knit_key.last().cloned().unwrap_or_default();

    if annotated {
        let content = rust_get_content(&index, &access, &KnitAnnotateFactory, &knit_key)
            .map_err(knit_err_to_py)?;
        let strip = content.should_strip_eol();
        let pairs_list = pyo3::types::PyList::empty(py);
        for (origin, text) in &content.lines {
            let tup = PyTuple::new(py, [PyBytes::new(py, origin), PyBytes::new(py, text)])?;
            pairs_list.append(tup)?;
        }
        Ok((
            PyBytes::new(py, &last_segment),
            pairs_list.into_any(),
            strip,
        ))
    } else {
        let content = rust_get_content(&index, &access, &KnitPlainFactory, &knit_key)
            .map_err(knit_err_to_py)?;
        let strip = content.should_strip_eol();
        let lines_list = pyo3::types::PyList::empty(py);
        for line in &content.lines {
            lines_list.append(PyBytes::new(py, line))?;
        }
        Ok((
            PyBytes::new(py, &content.version_id),
            lines_list.into_any(),
            strip,
        ))
    }
}

/// Batch digest-only lookup for `keys` via the pure-Rust pipeline.
/// Returns a `{key: digest_bytes}` dict; keys missing from the index
/// are simply absent, matching the Python `_get_record_map(allow_missing=True)`
/// semantics.
///
/// The pure-Rust implementation fetches each raw record and parses
/// just its header (via `parse_record_header_only`), never touching
/// the body bytes — the same cheap path the Python
/// `_read_records_iter_raw` takes for sha verification.
#[pyfunction]
fn get_sha1s_via_traits_rs<'py>(
    py: Python<'py>,
    py_index: Bound<'py, PyAny>,
    py_access: Bound<'py, PyAny>,
    keys: Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyDict>> {
    let index = PyKnitIndex::new(py_index);
    let access = PyKnitAccess::new(py_access);

    let mut rust_keys: Vec<KnitKey> = Vec::new();
    for item in keys.try_iter()? {
        let obj = item?;
        rust_keys.push(extract_knit_key(&obj).map_err(knit_err_to_py)?);
    }

    let result = rust_get_sha1s(&index, &access, &rust_keys).map_err(knit_err_to_py)?;

    let out = PyDict::new(py);
    for (key, digest) in result {
        let tup = knit_key_to_py(py, &key)?;
        out.set_item(tup, PyBytes::new(py, &digest))?;
    }
    Ok(out)
}

/// Dictionary-compress a list of suffixes against a per-prefix kndx cache.
///
/// Mirrors `_KndxIndex._dictionary_compress`: the caller hands in the list of
/// `key[-1]` suffixes (all from keys sharing the same prefix) and the raw
/// `_kndx_cache[prefix][0]` dict. Each suffix is emitted as either its decimal
/// history index (cache hit) or `.`+suffix (cache miss). The mismatched-prefix
/// check stays on the Python side to keep error reporting identical.
#[pyfunction]
fn dictionary_compress_rs<'py>(
    py: Python<'py>,
    suffixes: Vec<Vec<u8>>,
    cache: &Bound<'py, PyDict>,
) -> PyResult<Bound<'py, PyBytes>> {
    if suffixes.is_empty() {
        return Ok(PyBytes::new(py, b""));
    }
    let mut out = Vec::new();
    for (i, suffix) in suffixes.iter().enumerate() {
        if i > 0 {
            out.push(b' ');
        }
        let key = PyBytes::new(py, suffix);
        match cache.get_item(&key)? {
            Some(entry) => {
                let tup = entry.cast_into::<PyTuple>()?;
                let pos: i64 = tup.get_item(5)?.extract()?;
                use std::io::Write;
                write!(out, "{}", pos).unwrap();
            }
            None => {
                out.push(b'.');
                out.extend_from_slice(suffix);
            }
        }
    }
    Ok(PyBytes::new(py, &out))
}

/// Python-accessible wrapper around [`bazaar::knit::AnnotatedKnitContent`].
///
/// Exposes the same public interface as the Python `AnnotatedKnitContent`:
/// `annotate()`, `text()`, `copy()`, `apply_delta()`, `line_delta()`,
/// `line_delta_iter()`, `get_line_delta_blocks()`, plus the `_lines` and
/// `_should_strip_eol` attributes for compatibility with callers that access
/// the internal state directly.
#[pyclass(name = "AnnotatedKnitContent")]
pub struct PyAnnotatedKnitContent(AnnotatedKnitContent);

#[pymethods]
impl PyAnnotatedKnitContent {
    #[new]
    fn new(lines: &Bound<PyAny>) -> PyResult<Self> {
        let pairs = extract_annotated_lines(lines)?;
        Ok(Self(AnnotatedKnitContent::new(pairs)))
    }

    #[getter]
    fn _lines<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        annotated_lines_to_py(py, &self.0.lines)
    }

    #[setter]
    fn set__lines(&mut self, lines: &Bound<PyAny>) -> PyResult<()> {
        self.0.lines = extract_annotated_lines(lines)?;
        Ok(())
    }

    #[getter]
    fn _should_strip_eol(&self) -> bool {
        self.0.should_strip_eol()
    }

    #[setter]
    fn set__should_strip_eol(&mut self, val: bool) {
        self.0.set_should_strip_eol(val);
    }

    fn annotate<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        annotated_lines_to_py(py, &self.0.annotate())
    }

    fn text<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let lines = self.0.text();
        let items: Vec<Bound<PyBytes>> = lines.iter().map(|l| PyBytes::new(py, l)).collect();
        PyList::new(py, items)
    }

    fn copy(&self) -> Self {
        Self(self.0.clone())
    }

    fn apply_delta(&mut self, delta: &Bound<PyAny>, _new_version_id: &[u8]) -> PyResult<()> {
        let hunks = extract_annotated_delta_hunks(delta)?;
        self.0.apply_delta(&hunks, _new_version_id);
        Ok(())
    }

    fn line_delta<'py>(
        slf: PyRef<'_, Self>,
        py: Python<'py>,
        new_lines: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        let it = Self::line_delta_iter_impl(slf, py, new_lines)?;
        PyList::new(py, it)
    }

    fn line_delta_iter<'py>(
        slf: PyRef<'_, Self>,
        py: Python<'py>,
        new_lines: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let items = Self::line_delta_iter_impl(slf, py, new_lines)?;
        Ok(PyList::new(py, items)?.call_method0("__iter__")?)
    }

    #[staticmethod]
    fn get_line_delta_blocks<'py>(
        py: Python<'py>,
        knit_delta: Bound<'py, PyAny>,
        source: Bound<'py, PyAny>,
        target: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        get_line_delta_blocks_rs(py, knit_delta, source, target)
    }
}

/// Extract plain text lines from a `PyAnnotatedKnitContent` or `PyPlainKnitContent`.
fn extract_content_text(obj: &Bound<'_, PyAny>) -> PyResult<Vec<Vec<u8>>> {
    if let Ok(annotated) = obj.downcast::<PyAnnotatedKnitContent>() {
        return Ok(annotated.borrow().0.text());
    }
    if let Ok(plain) = obj.downcast::<PyPlainKnitContent>() {
        return Ok(plain.borrow().0.text());
    }
    // Fallback for other content objects: call .text() and extract bytes.
    let text_obj = obj.call_method0("text")?;
    text_obj
        .try_iter()?
        .map(|item| item?.extract::<Vec<u8>>())
        .collect()
}

fn line_delta_iter_impl<'py>(
    old_lines: Vec<Vec<u8>>,
    new_lines_obj: &Bound<'py, PyAny>,
    new_raw_lines: &Bound<'py, PyAny>,
    py: Python<'py>,
) -> PyResult<Vec<Bound<'py, PyTuple>>> {
    let new_lines = extract_content_text(new_lines_obj)?;
    let old_refs: Vec<&[u8]> = old_lines.iter().map(|l| l.as_slice()).collect();
    let new_refs: Vec<&[u8]> = new_lines.iter().map(|l| l.as_slice()).collect();
    let mut matcher = patiencediff::SequenceMatcher::new(&old_refs, &new_refs);
    let opcodes = matcher.get_opcodes().to_vec();
    let mut out = Vec::new();
    for op in opcodes {
        let (i1, i2, j1, j2) = match op {
            patiencediff::Opcode::Equal(_, _, _, _) => continue,
            patiencediff::Opcode::Replace(i1, i2, j1, j2) => (i1, i2, j1, j2),
            patiencediff::Opcode::Delete(i1, i2, j1, j2) => (i1, i2, j1, j2),
            patiencediff::Opcode::Insert(i1, i2, j1, j2) => (i1, i2, j1, j2),
        };
        let count = j2 - j1;
        let slice =
            new_raw_lines.get_item(pyo3::types::PySlice::new(py, j1 as isize, j2 as isize, 1))?;
        out.push(PyTuple::new(
            py,
            [
                i1.into_pyobject(py)?.into_any(),
                i2.into_pyobject(py)?.into_any(),
                count.into_pyobject(py)?.into_any(),
                slice,
            ],
        )?);
    }
    Ok(out)
}

impl PyAnnotatedKnitContent {
    fn line_delta_iter_impl<'py>(
        slf: PyRef<'_, Self>,
        py: Python<'py>,
        new_lines: &Bound<'py, PyAny>,
    ) -> PyResult<Vec<Bound<'py, PyTuple>>> {
        let old_lines = slf.0.text();
        let new_raw_lines = new_lines.getattr("_lines")?;
        line_delta_iter_impl(old_lines, new_lines, &new_raw_lines, py)
    }
}

/// Python-accessible wrapper around [`bazaar::knit::PlainKnitContent`].
///
/// Exposes the same public interface as the Python `PlainKnitContent`.
#[pyclass(name = "PlainKnitContent")]
pub struct PyPlainKnitContent(PlainKnitContent);

#[pymethods]
impl PyPlainKnitContent {
    #[new]
    fn new(lines: &Bound<PyAny>, version_id: &Bound<PyAny>) -> PyResult<Self> {
        let lines = extract_byte_lines(lines)?;
        let vid = extract_version_id(version_id)?;
        Ok(Self(PlainKnitContent::new(lines, vid)))
    }

    #[getter]
    fn _lines<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let items: Vec<Bound<PyBytes>> = self.0.lines.iter().map(|l| PyBytes::new(py, l)).collect();
        PyList::new(py, items)
    }

    #[setter]
    fn set__lines(&mut self, lines: &Bound<PyAny>) -> PyResult<()> {
        self.0.lines = extract_byte_lines(lines)?;
        Ok(())
    }

    #[getter]
    fn _should_strip_eol(&self) -> bool {
        self.0.should_strip_eol()
    }

    #[setter]
    fn set__should_strip_eol(&mut self, val: bool) {
        self.0.set_should_strip_eol(val);
    }

    #[getter]
    fn _version_id<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.0.version_id)
    }

    fn annotate<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let pairs = self.0.annotate();
        annotated_lines_to_py(py, &pairs)
    }

    fn text<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let lines = self.0.text();
        let items: Vec<Bound<PyBytes>> = lines.iter().map(|l| PyBytes::new(py, l)).collect();
        PyList::new(py, items)
    }

    fn copy(&self) -> Self {
        Self(self.0.clone())
    }

    fn apply_delta(&mut self, delta: &Bound<PyAny>, new_version_id: &Bound<PyAny>) -> PyResult<()> {
        let hunks = extract_plain_delta_hunks(delta)?;
        let vid = extract_version_id(new_version_id)?;
        self.0.apply_delta(&hunks, &vid);
        Ok(())
    }

    fn line_delta<'py>(
        slf: PyRef<'_, Self>,
        py: Python<'py>,
        new_lines: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        let it = Self::line_delta_iter_impl(slf, py, new_lines)?;
        PyList::new(py, it)
    }

    fn line_delta_iter<'py>(
        slf: PyRef<'_, Self>,
        py: Python<'py>,
        new_lines: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let items = Self::line_delta_iter_impl(slf, py, new_lines)?;
        Ok(PyList::new(py, items)?.call_method0("__iter__")?)
    }

    #[staticmethod]
    fn get_line_delta_blocks<'py>(
        py: Python<'py>,
        knit_delta: Bound<'py, PyAny>,
        source: Bound<'py, PyAny>,
        target: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        get_line_delta_blocks_rs(py, knit_delta, source, target)
    }
}

impl PyPlainKnitContent {
    fn line_delta_iter_impl<'py>(
        slf: PyRef<'_, Self>,
        py: Python<'py>,
        new_lines: &Bound<'py, PyAny>,
    ) -> PyResult<Vec<Bound<'py, PyTuple>>> {
        let old_lines = slf.0.text();
        let new_raw_lines = new_lines.getattr("_lines")?;
        line_delta_iter_impl(old_lines, new_lines, &new_raw_lines, py)
    }
}

/// Extract a version_id as bytes. Accepts either `bytes` directly, or a tuple
/// of bytes (key tuple), in which case the last element is taken — matching
/// the breezy convention that `key[-1]` is the bare revision id.
fn extract_version_id(obj: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(b) = obj.downcast::<PyBytes>() {
        return Ok(b.as_bytes().to_vec());
    }
    if let Ok(t) = obj.downcast::<PyTuple>() {
        let len = t.len();
        if len == 0 {
            return Err(PyValueError::new_err("version_id tuple must be non-empty"));
        }
        let last = t.get_item(len - 1)?;
        return last
            .downcast::<PyBytes>()
            .map(|b| b.as_bytes().to_vec())
            .map_err(|_| PyValueError::new_err("version_id tuple elements must be bytes"));
    }
    Err(PyValueError::new_err(
        "argument 'version_id': expected bytes or tuple of bytes",
    ))
}

fn extract_annotated_delta_hunks(delta: &Bound<PyAny>) -> PyResult<Vec<DeltaHunk<AnnotatedLine>>> {
    let mut hunks = Vec::new();
    for item in delta.try_iter()? {
        let tup = item?;
        let start: usize = tup.get_item(0)?.extract()?;
        let end: usize = tup.get_item(1)?.extract()?;
        let count: usize = tup.get_item(2)?.extract()?;
        let lines = extract_annotated_lines(&tup.get_item(3)?)?;
        hunks.push(DeltaHunk {
            start,
            end,
            count,
            lines,
        });
    }
    Ok(hunks)
}

fn extract_plain_delta_hunks(delta: &Bound<PyAny>) -> PyResult<Vec<DeltaHunk<Vec<u8>>>> {
    let mut hunks = Vec::new();
    for item in delta.try_iter()? {
        let tup = item?;
        let start: usize = tup.get_item(0)?.extract()?;
        let end: usize = tup.get_item(1)?.extract()?;
        let count: usize = tup.get_item(2)?.extract()?;
        let lines = extract_byte_lines(&tup.get_item(3)?)?;
        hunks.push(DeltaHunk {
            start,
            end,
            count,
            lines,
        });
    }
    Ok(hunks)
}

/// Python-accessible wrapper around [`KnitAnnotateFactory`].
#[pyclass(name = "KnitAnnotateFactory")]
pub struct PyKnitAnnotateFactory;

#[pymethods]
impl PyKnitAnnotateFactory {
    #[new]
    fn new() -> Self {
        Self
    }

    #[getter]
    fn annotated(&self) -> bool {
        true
    }

    fn make<'py>(
        &self,
        _py: Python<'py>,
        lines: Vec<Vec<u8>>,
        version_id: &[u8],
    ) -> PyResult<PyAnnotatedKnitContent> {
        let pairs: Vec<AnnotatedLine> = lines
            .into_iter()
            .map(|l| (version_id.to_vec(), l))
            .collect();
        Ok(PyAnnotatedKnitContent(AnnotatedKnitContent::new(pairs)))
    }

    fn parse_fulltext(
        &self,
        content: &Bound<'_, PyAny>,
        version_id: &[u8],
    ) -> PyResult<PyAnnotatedKnitContent> {
        let _ = version_id;
        let owned = extract_byte_lines(content)?;
        let parsed = parse_fulltext(&as_slices(&owned)).map_err(knit_err_to_py)?;
        Ok(PyAnnotatedKnitContent(AnnotatedKnitContent::new(parsed)))
    }

    #[pyo3(signature = (lines, version_id, plain = false))]
    fn parse_line_delta<'py>(
        &self,
        py: Python<'py>,
        lines: Bound<'py, PyAny>,
        version_id: &[u8],
        plain: bool,
    ) -> PyResult<Bound<'py, PyList>> {
        let _ = version_id;
        parse_line_delta_rs(py, lines, plain)
    }

    fn get_fulltext_content<'py>(
        &self,
        py: Python<'py>,
        lines: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        // yields line.split(b" ", 1)[1] for each line — return as a generator-like list
        let mut out = Vec::new();
        for item in lines.try_iter()? {
            let line = item?.cast_into::<PyBytes>()?;
            let bytes = line.as_bytes();
            let content = bytes
                .iter()
                .position(|&b| b == b' ')
                .map(|i| &bytes[i + 1..])
                .unwrap_or(bytes);
            out.push(PyBytes::new(py, content));
        }
        Ok(PyList::new(py, out)?.into_any())
    }

    fn get_linedelta_content<'py>(
        &self,
        py: Python<'py>,
        lines: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut out = Vec::new();
        let mut iter = lines.try_iter()?;
        while let Some(header_item) = iter.next() {
            let header = header_item?.cast_into::<PyBytes>()?;
            let parts: Vec<&[u8]> = header.as_bytes().split(|&b| b == b',').collect();
            if parts.len() < 3 {
                return Err(PyValueError::new_err("invalid delta header"));
            }
            let count: usize = std::str::from_utf8(parts[2])
                .map_err(|_| PyValueError::new_err("invalid count"))?
                .trim()
                .parse()
                .map_err(|_| PyValueError::new_err("invalid count"))?;
            for _ in 0..count {
                let line = iter
                    .next()
                    .ok_or_else(|| PyValueError::new_err("truncated delta"))??
                    .cast_into::<PyBytes>()?;
                let bytes = line.as_bytes();
                let text = bytes
                    .iter()
                    .position(|&b| b == b' ')
                    .map(|i| &bytes[i + 1..])
                    .unwrap_or(bytes);
                out.push(PyBytes::new(py, text));
            }
        }
        Ok(PyList::new(py, out)?.into_any())
    }

    /// Mirrors `_KnitFactory.parse_record(version_id, record, record_details,
    /// base_content, copy_base_content=True)`. `record_details` is `(method,
    /// noeol)`.
    #[pyo3(signature = (version_id, record, record_details, base_content, copy_base_content = true))]
    fn parse_record<'py>(
        &self,
        py: Python<'py>,
        version_id: &Bound<'py, PyAny>,
        record: Bound<'py, PyAny>,
        record_details: Bound<'py, PyAny>,
        base_content: Option<&PyAnnotatedKnitContent>,
        copy_base_content: bool,
    ) -> PyResult<(PyAnnotatedKnitContent, Bound<'py, PyAny>)> {
        let vid = extract_version_id(version_id)?;
        let method_obj = record_details.get_item(0)?;
        let method_str: &str = method_obj.extract()?;
        let noeol: bool = record_details.get_item(1)?.extract()?;
        let method = match method_str {
            "line-delta" => KnitMethod::LineDelta,
            "fulltext" => KnitMethod::Fulltext,
            other => {
                return Err(PyValueError::new_err(format!(
                    "unknown knit method: {:?}",
                    other
                )));
            }
        };
        let _ = copy_base_content; // Rust always clones; Python default is True
        let owned = extract_byte_lines(&record)?;
        let slices = as_slices(&owned);
        let base = base_content.map(|c| &c.0);
        let content = KnitAnnotateFactory
            .parse_record(&vid, &slices, method, noeol, base)
            .map_err(knit_err_to_py)?;
        let delta = if method == KnitMethod::LineDelta {
            // Return the parsed delta as Python list for callers that need it
            parse_line_delta_rs(py, record, false)?.into_any()
        } else {
            py.None().into_bound(py)
        };
        Ok((PyAnnotatedKnitContent(content), delta))
    }

    fn lower_fulltext<'py>(
        &self,
        py: Python<'py>,
        content: &PyAnnotatedKnitContent,
    ) -> PyResult<Bound<'py, PyList>> {
        lower_fulltext_rs(py, content._lines(py)?.into_any())
    }

    fn lower_line_delta<'py>(
        &self,
        py: Python<'py>,
        delta: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        lower_line_delta_rs(py, delta)
    }

    fn annotate<'py>(
        &self,
        py: Python<'py>,
        knit: Bound<'py, PyAny>,
        key: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let content = knit.call_method1("_get_content", (&key,))?;
        let prefix: Bound<PyAny> = if let Ok(tup) = key.cast::<PyTuple>() {
            let len = tup.len();
            if len > 1 {
                PyTuple::new(py, (0..len - 1).map(|i| tup.get_item(i).unwrap()))?.into_any()
            } else {
                PyTuple::empty(py).into_any()
            }
        } else {
            return content.call_method0("annotate");
        };
        let origins = content.call_method0("annotate")?;
        let result = PyList::empty(py);
        for pair in origins.try_iter()? {
            let pair = pair?;
            let origin = pair.get_item(0)?;
            let line = pair.get_item(1)?;
            let full_origin = prefix.call_method1("__add__", (PyTuple::new(py, [origin])?,))?;
            result.append(PyTuple::new(py, [full_origin, line])?)?;
        }
        Ok(result.into_any())
    }
}

/// Python-accessible wrapper around [`KnitPlainFactory`].
#[pyclass(name = "KnitPlainFactory")]
pub struct PyKnitPlainFactory;

#[pymethods]
impl PyKnitPlainFactory {
    #[new]
    fn new() -> Self {
        Self
    }

    #[getter]
    fn annotated(&self) -> bool {
        false
    }

    fn make(&self, lines: Vec<Vec<u8>>, version_id: &[u8]) -> PyPlainKnitContent {
        PyPlainKnitContent(PlainKnitContent::new(lines, version_id.to_vec()))
    }

    fn parse_fulltext(&self, content: Vec<Vec<u8>>, version_id: &[u8]) -> PyPlainKnitContent {
        PyPlainKnitContent(PlainKnitContent::new(content, version_id.to_vec()))
    }

    fn parse_line_delta_iter<'py>(
        &self,
        py: Python<'py>,
        lines: Bound<'py, PyAny>,
        _version_id: &[u8],
    ) -> PyResult<Bound<'py, PyAny>> {
        Ok(parse_line_delta_raw_rs(py, lines)?.into_any())
    }

    fn parse_line_delta<'py>(
        &self,
        py: Python<'py>,
        lines: Bound<'py, PyAny>,
        _version_id: &[u8],
    ) -> PyResult<Bound<'py, PyList>> {
        parse_line_delta_raw_rs(py, lines)
    }

    fn get_fulltext_content<'py>(&self, lines: Bound<'py, PyAny>) -> Bound<'py, PyAny> {
        // plain: lines are the content directly
        lines
    }

    fn get_linedelta_content<'py>(
        &self,
        py: Python<'py>,
        lines: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let mut out = Vec::new();
        let mut iter = lines.try_iter()?;
        while let Some(header_item) = iter.next() {
            let header = header_item?.cast_into::<PyBytes>()?;
            let parts: Vec<&[u8]> = header.as_bytes().split(|&b| b == b',').collect();
            if parts.len() < 3 {
                return Err(PyValueError::new_err("invalid delta header"));
            }
            let count: usize = std::str::from_utf8(parts[2])
                .map_err(|_| PyValueError::new_err("invalid count"))?
                .trim()
                .parse()
                .map_err(|_| PyValueError::new_err("invalid count"))?;
            for _ in 0..count {
                let line = iter
                    .next()
                    .ok_or_else(|| PyValueError::new_err("truncated delta"))??;
                out.push(line);
            }
        }
        Ok(PyList::new(py, out)?.into_any())
    }

    fn lower_fulltext<'py>(
        &self,
        py: Python<'py>,
        content: &PyPlainKnitContent,
    ) -> PyResult<Bound<'py, PyList>> {
        content.text(py)
    }

    fn lower_line_delta<'py>(
        &self,
        py: Python<'py>,
        delta: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        lower_line_delta_raw_rs(py, delta)
    }

    /// Mirrors `_KnitFactory.parse_record(version_id, record, record_details,
    /// base_content, copy_base_content=True)`. `record_details` is `(method,
    /// noeol)`.
    #[pyo3(signature = (version_id, record, record_details, base_content, copy_base_content = true))]
    fn parse_record<'py>(
        &self,
        py: Python<'py>,
        version_id: &Bound<'py, PyAny>,
        record: Bound<'py, PyAny>,
        record_details: Bound<'py, PyAny>,
        base_content: Option<&PyPlainKnitContent>,
        copy_base_content: bool,
    ) -> PyResult<(PyPlainKnitContent, Bound<'py, PyAny>)> {
        let vid = extract_version_id(version_id)?;
        let method_obj = record_details.get_item(0)?;
        let method_str: &str = method_obj.extract()?;
        let noeol: bool = record_details.get_item(1)?.extract()?;
        let method = match method_str {
            "line-delta" => KnitMethod::LineDelta,
            "fulltext" => KnitMethod::Fulltext,
            other => {
                return Err(PyValueError::new_err(format!(
                    "unknown knit method: {:?}",
                    other
                )));
            }
        };
        let _ = copy_base_content; // Rust always clones; Python default is True
        let owned = extract_byte_lines(&record)?;
        let slices = as_slices(&owned);
        let base = base_content.map(|c| &c.0);
        let content = KnitPlainFactory
            .parse_record(&vid, &slices, method, noeol, base)
            .map_err(knit_err_to_py)?;
        let delta = if method == KnitMethod::LineDelta {
            parse_line_delta_raw_rs(py, record)?.into_any()
        } else {
            py.None().into_bound(py)
        };
        Ok((PyPlainKnitContent(content), delta))
    }

    fn annotate<'py>(
        &self,
        py: Python<'py>,
        knit: Bound<'py, PyKnitVersionedFiles>,
        key: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        if !knit.borrow().immediate_fallback_vfs.is_empty() {
            return annotate_with_fallbacks(py, &knit, key).map(|l| l.into_any());
        }
        // A pack reload partway through invalidates the annotator's
        // cached build details, so retry with a fresh annotator.
        let access_obj = knit.borrow().access_obj.clone_ref(py);
        retry_on_new_packs(py, &access_obj, || {
            let mut annotator = PyKnitAnnotator::from_kvf(py, &knit.borrow())?;
            annotator
                .annotate_flat(py, key.clone())
                .map(|l| l.into_any().unbind())
        })
        .map(|obj| obj.into_bound(py))
    }
}

/// Enum so the pyclass doesn't need to be generic.
enum AnyKnitAnnotator {
    Annotated(KnitAnnotator<PyKnitIndex, PyKnitAccess, KnitAnnotateFactory>),
    Plain(KnitAnnotator<PyKnitIndex, PyKnitAccess, KnitPlainFactory>),
}

impl AnyKnitAnnotator {
    fn annotate_flat(&mut self, key: &KnitKey) -> Result<Vec<(KnitKey, Vec<u8>)>, KnitError> {
        match self {
            AnyKnitAnnotator::Annotated(a) => a.annotate_flat(key),
            AnyKnitAnnotator::Plain(a) => a.annotate_flat(key),
        }
    }

    fn annotate(
        &mut self,
        key: &KnitKey,
    ) -> Result<(Vec<bazaar::knit::LineAnnotation>, Vec<Vec<u8>>), KnitError> {
        match self {
            AnyKnitAnnotator::Annotated(a) => a.annotate(key),
            AnyKnitAnnotator::Plain(a) => a.annotate(key),
        }
    }

    fn seed_text(&mut self, key: KnitKey, parents: Vec<KnitKey>, lines: Vec<Vec<u8>>) {
        match self {
            AnyKnitAnnotator::Annotated(a) => a.seed_text(key, parents, lines),
            AnyKnitAnnotator::Plain(a) => a.seed_text(key, parents, lines),
        }
    }

    fn add_special_text(&mut self, key: KnitKey, parent_keys: Vec<KnitKey>, lines: Vec<Vec<u8>>) {
        match self {
            AnyKnitAnnotator::Annotated(a) => a.add_special_text(key, parent_keys, lines),
            AnyKnitAnnotator::Plain(a) => a.add_special_text(key, parent_keys, lines),
        }
    }

    fn annotate_flat_seeded(
        &mut self,
        key: &KnitKey,
        order: &[KnitKey],
    ) -> Result<Vec<(KnitKey, Vec<u8>)>, KnitError> {
        match self {
            AnyKnitAnnotator::Annotated(a) => a.annotate_flat_seeded(key, order),
            AnyKnitAnnotator::Plain(a) => a.annotate_flat_seeded(key, order),
        }
    }

    /// The underlying access adapter, for retry-error conversion.
    fn access(&self) -> &PyKnitAccess {
        match self {
            AnyKnitAnnotator::Annotated(a) => a.access(),
            AnyKnitAnnotator::Plain(a) => a.access(),
        }
    }
}

/// Walk `kvf`'s parent map (consulting fallbacks) starting from `key`, fetch
/// each needed text via `kvf.get_record_stream(_, "topological", True)`, and
/// run [`KnitAnnotator::annotate_flat_seeded`] over the resulting topological
/// order. Mirrors `VersionedFileAnnotator._get_needed_texts` /
/// `VersionedFileAnnotator.annotate_flat`.
fn annotate_with_fallbacks<'py>(
    py: Python<'py>,
    kvf: &Bound<'py, PyKnitVersionedFiles>,
    key: Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyList>> {
    let initial_key = extract_knit_key(&key).map_err(knit_err_to_py)?;
    let mut parent_map: std::collections::HashMap<KnitKey, Vec<KnitKey>> =
        std::collections::HashMap::new();
    let mut needed_keys: std::collections::HashSet<KnitKey> =
        std::iter::once(initial_key.clone()).collect();
    let mut vf_keys_needed: std::collections::HashSet<KnitKey> = std::collections::HashSet::new();
    while !needed_keys.is_empty() {
        let lookup = pyo3::types::PySet::empty(py)?;
        for k in &needed_keys {
            lookup.add(py_knit_key_to_py(py, k)?)?;
            vf_keys_needed.insert(k.clone());
        }
        let pmap_obj = kvf.call_method1("get_parent_map", (lookup,))?;
        let pmap = pmap_obj.cast_into::<PyDict>()?;
        let mut next_keys: std::collections::HashSet<KnitKey> = std::collections::HashSet::new();
        for (pk, pv) in pmap.iter() {
            let key_r = extract_knit_key(&pk).map_err(knit_err_to_py)?;
            let parents: Vec<KnitKey> = if pv.is_none() {
                Vec::new()
            } else {
                pv.try_iter()?
                    .map(|p| extract_knit_key(&p?).map_err(knit_err_to_py))
                    .collect::<PyResult<_>>()?
            };
            for p in &parents {
                if !parent_map.contains_key(p) {
                    next_keys.insert(p.clone());
                }
            }
            parent_map.insert(key_r, parents);
        }
        needed_keys = next_keys;
    }

    let stream_keys = PyList::empty(py);
    for k in &vf_keys_needed {
        stream_keys.append(py_knit_key_to_py(py, k)?)?;
    }
    let stream = kvf.call_method1("get_record_stream", (stream_keys, "topological", true))?;

    let mut annotator = PyKnitAnnotator::from_kvf(py, &kvf.borrow())?;
    let mut order: Vec<KnitKey> = Vec::new();
    for item in stream.try_iter()? {
        let record = item?;
        let storage_kind: String = record.getattr("storage_kind")?.extract()?;
        if storage_kind == "absent" {
            let rkey = record.getattr("key")?;
            return Err(RevisionNotPresent::new_err((rkey.unbind(), py.None())));
        }
        let rec_key = extract_knit_key(&record.getattr("key")?).map_err(knit_err_to_py)?;
        let lines_obj = record.call_method1("get_bytes_as", ("lines",))?;
        let lines: Vec<Vec<u8>> = lines_obj
            .try_iter()?
            .map(|item| {
                item?
                    .cast_into::<PyBytes>()
                    .map(|b| b.as_bytes().to_vec())
                    .map_err(|_| PyValueError::new_err("lines must be bytes"))
            })
            .collect::<PyResult<_>>()?;
        let parents = parent_map.get(&rec_key).cloned().unwrap_or_default();
        annotator.inner.seed_text(rec_key.clone(), parents, lines);
        order.push(rec_key);
    }

    let pairs = annotator
        .inner
        .annotate_flat_seeded(&initial_key, &order)
        .map_err(knit_err_to_py)?;
    let out = PyList::empty(py);
    for (ann_key, line) in pairs {
        let ak = py_knit_key_to_py(py, &ann_key)?;
        let lb = PyBytes::new(py, &line);
        out.append(PyTuple::new(py, [ak.into_any(), lb.into_any()])?)?;
    }
    Ok(out)
}

/// Convert one Python record (from a `get_record_stream` iterator) into the
/// `KnitStreamRecord` variant that `bazaar::knit::insert_record_stream` consumes.
///
/// `kvf` is the destination KnitVersionedFiles; for delta records whose basis
/// is not natively storable, we fetch the basis lines back from `kvf.get_record_stream`
/// (which sees both local and fallback storage, plus everything inserted so
/// far in this stream).
fn convert_stream_record<'py>(
    py: Python<'py>,
    record: &Bound<'py, PyAny>,
    native_types: &std::collections::HashSet<String>,
    convertible_types: &std::collections::HashSet<String>,
    has_fallbacks: bool,
    index_obj: &Bound<'py, PyAny>,
    kvf: &Bound<'py, PyKnitVersionedFiles>,
) -> PyResult<bazaar::knit::KnitStreamRecord> {
    let storage_kind: String = record.getattr("storage_kind")?.extract()?;
    let sk = storage_kind.as_str();

    let key_obj = record.getattr("key")?;
    let knit_key = extract_knit_key(&key_obj).map_err(knit_err_to_py)?;

    if sk == "absent" {
        return Err(RevisionNotPresent::new_err((
            py_knit_key_to_py(py, &knit_key)?.unbind(),
            py.None(),
        )));
    }

    let parents_obj = record.getattr("parents")?;
    let parents: Vec<KnitKey> = if parents_obj.is_none() {
        vec![]
    } else {
        parents_obj
            .try_iter()?
            .map(|p| extract_knit_key(&p?).map_err(knit_err_to_py))
            .collect::<PyResult<_>>()?
    };

    let is_native = native_types.contains(sk);
    let is_convertible = convertible_types.contains(sk);

    if is_native || is_convertible {
        let is_delta = sk.contains("-delta-");
        let compression_parent = if is_delta {
            parents.first().cloned()
        } else {
            None
        };

        let mut store_direct = compression_parent.is_none()
            || !has_fallbacks
            || compression_parent.as_ref().is_some_and(|cp| {
                let Ok(py_cp) = py_knit_key_to_py(py, cp) else {
                    return false;
                };
                let Ok(map_obj) =
                    index_obj.call_method1("get_parent_map", (PyList::new(py, [&py_cp]).unwrap(),))
                else {
                    return false;
                };
                let Ok(map) = map_obj.cast_into::<PyDict>() else {
                    return false;
                };
                map.contains(py_cp).unwrap_or(false)
            });
        // Mirror Python's `compression_parent not in self`: if cp isn't in
        // any fallback either, we still store the delta directly and rely on
        // the buffering layer to defer the index entry until the basis lands.
        if !store_direct {
            if let Some(cp) = compression_parent.as_ref() {
                if let Ok(py_cp) = py_knit_key_to_py(py, cp) {
                    let lst = PyList::new(py, [&py_cp]).ok();
                    if let Some(lst) = lst {
                        if let Ok(map_obj) = kvf.call_method1("get_parent_map", (lst,)) {
                            if let Ok(map) = map_obj.cast_into::<PyDict>() {
                                if !map.contains(py_cp).unwrap_or(true) {
                                    store_direct = true;
                                }
                            }
                        }
                    }
                }
            }
        }

        if store_direct {
            let raw_bytes: Vec<u8> = record.getattr("_raw_record")?.extract::<Vec<u8>>()?;
            let method = if is_delta {
                bazaar::knit::KnitMethod::LineDelta
            } else {
                bazaar::knit::KnitMethod::Fulltext
            };
            let build_tup = record.getattr("_build_details")?.cast_into::<PyTuple>()?;
            let noeol: bool = build_tup.get_item(1)?.extract()?;

            if is_native {
                return Ok(bazaar::knit::KnitStreamRecord::NativeKnit {
                    key: knit_key,
                    parents,
                    method,
                    noeol,
                    compression_parent,
                    raw_record: raw_bytes,
                });
            } else {
                return Ok(bazaar::knit::KnitStreamRecord::ConvertAnnotated {
                    key: knit_key,
                    parents,
                    method,
                    noeol,
                    compression_parent,
                    raw_record: raw_bytes,
                });
            }
        }
    }

    // Fall-through: convert the record to plain text lines.
    let lines_result = record.call_method1("get_bytes_as", ("lines",));
    let lines: Vec<Vec<u8>> = match lines_result {
        Ok(obj) => obj
            .try_iter()?
            .map(|l| l?.extract::<Vec<u8>>())
            .collect::<PyResult<_>>()?,
        Err(_) => {
            let is_delta = sk.contains("-delta-");
            if !is_delta {
                return Err(PyValueError::new_err(format!(
                    "UnavailableRepresentation: cannot reconstruct {sk} for {knit_key:?}"
                )));
            }
            let compression_parent = parents.first().cloned().ok_or_else(|| {
                PyValueError::new_err(format!(
                    "knit-delta record has no compression parent: {knit_key:?}"
                ))
            })?;
            let cp_list = PyList::empty(py);
            cp_list.append(py_knit_key_to_py(py, &compression_parent)?)?;
            // Reconstructing a delta means reading its basis back from
            // storage. Earlier records in this same stream may still be
            // buffered in the access layer's writer, so flush first.
            // (See vf_repository.insert_stream_without_locking: "a delta
            // record from the source that should be a fulltext may need
            // to be expanded by the target ... flush any buffered writes
            // first.")
            kvf.getattr("_access")?.call_method0("flush")?;
            let basis_stream =
                kvf.call_method1("get_record_stream", (cp_list, "unordered", true))?;
            let basis_entry = basis_stream.call_method0("__next__")?;
            let basis_storage: String = basis_entry.getattr("storage_kind")?.extract()?;
            if basis_storage == "absent" {
                return Err(RevisionNotPresent::new_err((
                    py_knit_key_to_py(py, &compression_parent)?.unbind(),
                    py.None(),
                )));
            }
            let basis_lines_obj = basis_entry.call_method1("get_bytes_as", ("lines",))?;
            let basis_lines: Vec<Vec<u8>> = basis_lines_obj
                .try_iter()?
                .map(|l| l?.extract::<Vec<u8>>())
                .collect::<PyResult<_>>()?;
            let raw_record: Vec<u8> = record.getattr("_raw_record")?.extract::<Vec<u8>>()?;
            let build_tup = record.getattr("_build_details")?.cast_into::<PyTuple>()?;
            let noeol: bool = build_tup.get_item(1)?.extract()?;
            let decompressed =
                bazaar::knit::decode_record_gz(&raw_record).map_err(knit_err_to_py)?;
            let (_, body) =
                bazaar::knit::parse_record_body_unchecked(&decompressed).map_err(knit_err_to_py)?;
            use bazaar::knit::{KnitContent as _, KnitFactory as _};
            let version_bytes = knit_key.last().map(|s| s.as_slice()).unwrap_or(&[]);
            let source_annotated = sk.contains("-annotated-");
            if source_annotated {
                // Annotated body: basis must also be annotated so the delta hunks line up.
                let factory = bazaar::knit::KnitAnnotateFactory;
                let basis_pairs: Vec<bazaar::knit::AnnotatedLine> = basis_lines
                    .into_iter()
                    .map(|l| (compression_parent.last().cloned().unwrap_or_default(), l))
                    .collect();
                let basis_content = AnnotatedKnitContent::new(basis_pairs);
                let content = factory
                    .parse_record(
                        version_bytes,
                        &body,
                        bazaar::knit::KnitMethod::LineDelta,
                        noeol,
                        Some(&basis_content),
                    )
                    .map_err(knit_err_to_py)?;
                content.text()
            } else {
                let factory = bazaar::knit::KnitPlainFactory;
                let basis_content = PlainKnitContent::new(
                    basis_lines,
                    compression_parent.last().cloned().unwrap_or_default(),
                );
                let content = factory
                    .parse_record(
                        version_bytes,
                        &body,
                        bazaar::knit::KnitMethod::LineDelta,
                        noeol,
                        Some(&basis_content),
                    )
                    .map_err(knit_err_to_py)?;
                content.text()
            }
        }
    };
    Ok(bazaar::knit::KnitStreamRecord::Lines {
        key: knit_key,
        parents,
        lines,
    })
}

/// Python-accessible wrapper around [`KnitAnnotator`].
#[pyclass(name = "_KnitAnnotator")]
pub struct PyKnitAnnotator {
    inner: AnyKnitAnnotator,
    /// The versioned file this annotator was constructed from.  Exposed as
    /// `_vf` to match the Python `_KnitAnnotator` / `VersionedFileAnnotator`
    /// interface (used by tests and callers that need to add special texts).
    vf: Py<PyAny>,
}

impl PyKnitAnnotator {
    fn from_kvf(py: Python<'_>, kvf: &PyKnitVersionedFiles) -> PyResult<Self> {
        let index = PyKnitIndex::new(kvf.index_obj.bind(py).clone());
        let access = PyKnitAccess::new(kvf.access_obj.bind(py).clone());
        let inner = if kvf.annotated {
            AnyKnitAnnotator::Annotated(KnitAnnotator::new(index, access, KnitAnnotateFactory))
        } else {
            AnyKnitAnnotator::Plain(KnitAnnotator::new(index, access, KnitPlainFactory))
        };
        Ok(PyKnitAnnotator { inner, vf: py.None() })
    }
}

fn knit_annotation_to_py<'py>(
    py: Python<'py>,
    annotation: Vec<KnitKey>,
) -> PyResult<Bound<'py, PyTuple>> {
    let items: Vec<Bound<'py, PyTuple>> = annotation
        .into_iter()
        .map(|k| knit_key_to_py(py, &k))
        .collect::<PyResult<_>>()?;
    PyTuple::new(py, items)
}

#[pymethods]
impl PyKnitAnnotator {
    #[new]
    fn new(py: Python<'_>, vf: Bound<'_, PyKnitVersionedFiles>) -> PyResult<Self> {
        let vf_obj = vf.clone().into_any().unbind();
        let mut this = Self::from_kvf(py, &vf.borrow())?;
        this.vf = vf_obj;
        Ok(this)
    }

    fn add_special_text(
        &mut self,
        key: Bound<'_, PyAny>,
        parent_keys: Bound<'_, PyAny>,
        text: &[u8],
    ) -> PyResult<()> {
        let rust_key = extract_py_knit_key(&key)?;
        let rust_parents: Vec<KnitKey> = parent_keys
            .try_iter()?
            .map(|item| extract_py_knit_key(&item?))
            .collect::<PyResult<_>>()?;
        let lines = bazaar::osutils::split_lines(text)
            .into_iter()
            .map(|l| l.to_vec())
            .collect();
        self.inner.add_special_text(rust_key, rust_parents, lines);
        Ok(())
    }

    #[getter]
    fn _vf<'py>(&self, py: Python<'py>) -> Bound<'py, PyAny> {
        self.vf.bind(py).clone()
    }

    fn annotate_flat<'py>(
        &mut self,
        py: Python<'py>,
        key: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyList>> {
        let rust_key =
            extract_knit_key(&key).map_err(|e| PyValueError::new_err(format!("{:?}", e)))?;
        let pairs = self
            .inner
            .annotate_flat(&rust_key)
            .map_err(|e| read_err_to_py(self.inner.access(), e))?;
        let out = PyList::empty(py);
        for (ann_key, line) in pairs {
            let ann_py = knit_key_to_py(py, &ann_key)?;
            let line_py = PyBytes::new(py, &line);
            let pair = PyTuple::new(py, [ann_py.into_any(), line_py.into_any()])?;
            out.append(pair)?;
        }
        Ok(out)
    }

    fn annotate<'py>(
        &mut self,
        py: Python<'py>,
        key: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let rust_key =
            extract_knit_key(&key).map_err(|e| PyValueError::new_err(format!("{:?}", e)))?;
        let (annotations, lines) = self
            .inner
            .annotate(&rust_key)
            .map_err(|e| read_err_to_py(self.inner.access(), e))?;
        let anns_py: Vec<Bound<'py, PyTuple>> = annotations
            .into_iter()
            .map(|ann| knit_annotation_to_py(py, ann))
            .collect::<PyResult<_>>()?;
        let anns_list = PyList::new(py, anns_py)?;
        let lines_list = PyList::new(py, lines.iter().map(|l| PyBytes::new(py, l)))?;
        PyTuple::new(py, [anns_list.into_any(), lines_list.into_any()])
    }
}

fn transport_err_to_py(e: bazaar::transport::TransportError) -> PyErr {
    PyValueError::new_err(e.to_string())
}

fn kndx_load_err_to_py(py: Python<'_>, e: KndxLoadError) -> PyErr {
    match e {
        KndxLoadError::Transport(te) => transport_err_to_py(te),
        KndxLoadError::Knit(ke) => match &ke {
            bazaar::knit::KnitError::BadKnitHeader { path } => {
                let badline = pyo3::types::PyBytes::new(py, b"");
                KnitHeaderError::new_err((badline.into_any().unbind(), path.as_str().to_owned()))
            }
            bazaar::knit::KnitError::KndxCorrupt { line, detail } => {
                let py_line = pyo3::types::PyBytes::new(py, line);
                KnitCorrupt::new_err((py_line.into_any().unbind(), detail.as_str().to_owned()))
            }
            _ => PyValueError::new_err(ke.to_string()),
        },
    }
}

type PyKndxIndexInner =
    bazaar::knit::KndxIndex<crate::transport::PyTransport, crate::transport::PyMapper>;

/// pyo3 wrapper around `bazaar::knit::KndxIndex`.
///
/// Exposes the same interface as the Python `_KndxIndex` class but
/// delegates all parsing and caching to the pure-Rust implementation.
/// The `transport` and `mapper` arguments accept any Python object
/// satisfying the respective duck-typed interfaces.
#[pyclass(name = "_KndxIndex")]
pub struct PyKndxIndex {
    inner: PyKndxIndexInner,
    // Keep hold of the Python transport and mapper so Python code can
    // still reach `._transport` / `._mapper` if needed.
    transport_obj: Py<PyAny>,
    mapper_obj: Py<PyAny>,
    get_scope: Py<PyAny>,
    allow_writes: Py<PyAny>,
    is_locked: Py<PyAny>,
    scope: Py<PyAny>,
    mode: String,
}

#[pymethods]
impl PyKndxIndex {
    #[new]
    fn new(
        py: Python<'_>,
        transport: Bound<'_, PyAny>,
        mapper: Bound<'_, PyAny>,
        get_scope: Bound<'_, PyAny>,
        allow_writes: Bound<'_, PyAny>,
        is_locked: Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        use crate::transport::{PyMapper, PyTransport};
        use bazaar::knit::KndxIndex;
        let py_transport = PyTransport::new(transport.clone());
        let py_mapper = PyMapper::new(mapper.clone());
        let inner = KndxIndex::new(py_transport, py_mapper);
        let scope = get_scope.call0()?;
        let mode_bool: bool = allow_writes.call0()?.extract()?;
        let mode = if mode_bool { "w" } else { "r" }.to_string();
        Ok(Self {
            inner,
            transport_obj: transport.unbind(),
            mapper_obj: mapper.unbind(),
            get_scope: get_scope.unbind(),
            allow_writes: allow_writes.unbind(),
            is_locked: is_locked.unbind(),
            scope: scope.unbind(),
            mode,
        })
    }

    #[getter]
    fn _transport(&self, py: Python<'_>) -> Py<PyAny> {
        self.transport_obj.clone_ref(py)
    }

    #[getter]
    fn _mapper(&self, py: Python<'_>) -> Py<PyAny> {
        self.mapper_obj.clone_ref(py)
    }

    #[classattr]
    fn HEADER(py: Python<'_>) -> Py<PyBytes> {
        PyBytes::new(py, bazaar::knit::KNDX_HEADER).unbind()
    }

    #[getter]
    fn has_graph(&self) -> bool {
        true
    }

    fn _check_read(&mut self, py: Python<'_>) -> PyResult<()> {
        let locked: bool = self.is_locked.bind(py).call0()?.extract()?;
        if !locked {
            return Err(ObjectNotLocked::new_err((py.None(),)));
        }
        let current_scope = self.get_scope.bind(py).call0()?;
        if !current_scope.eq(self.scope.bind(py))? {
            self._reset_cache(py)?;
        }
        Ok(())
    }

    fn _check_write_ok(&mut self, py: Python<'_>) -> PyResult<()> {
        self._check_read(py)?;
        if self.mode != "w" {
            return Err(PyValueError::new_err("read only object dirtied"));
        }
        Ok(())
    }

    fn _reset_cache(&mut self, py: Python<'_>) -> PyResult<()> {
        use crate::transport::{PyMapper, PyTransport};
        use bazaar::knit::KndxIndex;
        let py_transport = PyTransport::new(self.transport_obj.bind(py).clone());
        let py_mapper = PyMapper::new(self.mapper_obj.bind(py).clone());
        self.inner = KndxIndex::new(py_transport, py_mapper);
        let scope = self.get_scope.bind(py).call0()?;
        self.scope = scope.unbind();
        let mode_bool: bool = self.allow_writes.bind(py).call0()?.extract()?;
        self.mode = if mode_bool { "w" } else { "r" }.to_string();
        Ok(())
    }

    fn get_build_details<'py>(
        &mut self,
        py: Python<'py>,
        keys: Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyDict>> {
        self._check_read(py)?;
        let rust_keys = extract_py_knit_keys(&keys)?;
        use bazaar::knit::KnitIndex;
        let details = self
            .inner
            .get_build_details(&rust_keys)
            .map_err(knit_err_to_py)?;
        let result = PyDict::new(py);
        for (key, det) in &details {
            let py_key = py_knit_key_to_py(py, key)?;
            let index_memo = knit_index_memo_to_py(py, key, det)?;
            let compression_parent = match &det.compression_parent {
                Some(p) => py_knit_key_to_py(py, p)?.into_any(),
                None => py.None().into_bound(py),
            };
            let parents = PyTuple::new(
                py,
                det.parents
                    .iter()
                    .map(|p| py_knit_key_to_py(py, p))
                    .collect::<PyResult<Vec<_>>>()?,
            )?
            .into_any();
            let record_details = PyTuple::new(
                py,
                [
                    det.method.as_str().into_pyobject(py)?.into_any(),
                    det.noeol.into_pyobject(py)?.to_owned().into_any(),
                ],
            )?;
            let value = PyTuple::new(
                py,
                [
                    index_memo.into_any(),
                    compression_parent,
                    parents,
                    record_details.into_any(),
                ],
            )?;
            result.set_item(py_key, value)?;
        }
        Ok(result)
    }

    fn get_parent_map<'py>(
        &mut self,
        py: Python<'py>,
        keys: Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyDict>> {
        self._check_read(py)?;
        let rust_keys = extract_py_knit_keys(&keys)?;
        use bazaar::knit::KnitIndex;
        let details = self
            .inner
            .get_build_details(&rust_keys)
            .map_err(knit_err_to_py)?;
        let result = PyDict::new(py);
        for key in &rust_keys {
            if let Some(det) = details.get(key) {
                let py_key = py_knit_key_to_py(py, key)?;
                let py_parents = PyTuple::new(
                    py,
                    det.parents
                        .iter()
                        .map(|p| py_knit_key_to_py(py, p))
                        .collect::<PyResult<Vec<_>>>()?,
                )?;
                result.set_item(py_key, py_parents)?;
            }
        }
        Ok(result)
    }

    fn get_position(&mut self, py: Python<'_>, key: Bound<'_, PyAny>) -> PyResult<Py<PyTuple>> {
        self._check_read(py)?;
        let rust_key = extract_py_knit_key(&key)?;
        use bazaar::knit::KnitIndex;
        let details = self
            .inner
            .get_build_details(&[rust_key.clone()])
            .map_err(knit_err_to_py)?;
        let det = details
            .get(&rust_key)
            .ok_or_else(|| PyValueError::new_err("key not present"))?;
        let py_key = py_knit_key_to_py(py, &rust_key)?;
        Ok(PyTuple::new(
            py,
            [
                py_key.into_any(),
                det.index_memo.offset.into_pyobject(py)?.into_any(),
                det.index_memo.length.into_pyobject(py)?.into_any(),
            ],
        )?
        .unbind())
    }

    fn get_options(&mut self, py: Python<'_>, key: Bound<'_, PyAny>) -> PyResult<Py<PyList>> {
        self._check_read(py)?;
        let rust_key = extract_py_knit_key_or_bytes(&key)?;
        let prefix = PyKndxIndexInner::prefix_of(&rust_key);
        let suffix = PyKndxIndexInner::suffix_of(&rust_key);
        self.inner
            .load_prefix_shared(prefix.clone())
            .map_err(transport_err_to_py)?;
        let cache = self.inner.kndx_cache().lock().unwrap();
        let pc = cache
            .get(&prefix)
            .ok_or_else(|| PyValueError::new_err("prefix not in cache"))?;
        let entry = pc
            .cache
            .get(&suffix)
            .ok_or_else(|| PyValueError::new_err("key not present"))?;
        let list = PyList::empty(py);
        for opt in &entry.options {
            list.append(PyBytes::new(py, opt))?;
        }
        Ok(list.unbind())
    }

    fn get_method(&mut self, py: Python<'_>, key: Bound<'_, PyAny>) -> PyResult<String> {
        let options = self.get_options(py, key)?;
        let options_bound = options.bind(py);
        let opts: Vec<Vec<u8>> = options_bound
            .iter()
            .map(|o| Ok(o.extract::<Vec<u8>>()?))
            .collect::<PyResult<_>>()?;
        let refs: Vec<&[u8]> = opts.iter().map(|o| o.as_slice()).collect();
        let transport_obj = self.transport_obj.bind(py).clone().into_any().unbind();
        let (method, _noeol) = bazaar::knit::decode_kndx_options(&refs).map_err(|_| {
            KnitIndexUnknownMethod::new_err((transport_obj, options.bind(py).clone().unbind()))
        })?;
        Ok(method.as_str().to_string())
    }

    fn _dictionary_compress(
        &mut self,
        py: Python<'_>,
        keys: Bound<'_, PyAny>,
    ) -> PyResult<Py<PyBytes>> {
        let rust_keys = extract_py_knit_keys(&keys)?;
        if rust_keys.is_empty() {
            return Ok(PyBytes::new(py, b"").unbind());
        }
        let prefix = PyKndxIndexInner::prefix_of(&rust_keys[0]);
        let suffixes: Vec<Vec<u8>> = rust_keys
            .iter()
            .map(|k| PyKndxIndexInner::suffix_of(k))
            .collect();
        self.inner
            .load_prefix_shared(prefix.clone())
            .map_err(transport_err_to_py)?;
        let index_map: std::collections::HashMap<Vec<u8>, u64> = {
            let cache = self.inner.kndx_cache().lock().unwrap();
            cache
                .get(&prefix)
                .map(|pc| {
                    pc.cache
                        .iter()
                        .map(|(k, v)| (k.clone(), v.index as u64))
                        .collect()
                })
                .unwrap_or_default()
        };
        let lookup: std::collections::HashMap<&[u8], u64> =
            index_map.iter().map(|(k, v)| (k.as_slice(), *v)).collect();
        let refs: Vec<&[u8]> = suffixes.iter().map(|s| s.as_slice()).collect();
        let compressed = bazaar::knit::dictionary_compress_suffixes(&refs, &lookup);
        Ok(PyBytes::new(py, &compressed).unbind())
    }

    #[pyo3(signature = (records, random_id=None, missing_compression_parents=None))]
    fn add_records(
        &mut self,
        py: Python<'_>,
        records: Bound<'_, PyAny>,
        random_id: Option<bool>,
        missing_compression_parents: Option<bool>,
    ) -> PyResult<()> {
        let _ = random_id;
        if missing_compression_parents.unwrap_or(false) {
            let err_cls = py
                .import("bzrformats.errors")?
                .getattr("RevisionNotPresent")?;
            let exc = err_cls.call1((py.None(), py.None()))?;
            return Err(PyErr::from_value(exc.unbind().into_bound(py)));
        }
        // Collect all records first so we can group them
        let mut all_recs: Vec<(
            Vec<Vec<u8>>,      // key
            Vec<Vec<u8>>,      // options
            u64,               // pos
            usize,             // size
            Vec<Vec<Vec<u8>>>, // parent keys
        )> = Vec::new();
        for rec in records.try_iter()? {
            let rec = rec?;
            let key = extract_py_knit_key_or_bytes(&rec.get_item(0)?)?;
            let options: Vec<Vec<u8>> = rec
                .get_item(1)?
                .try_iter()?
                .map(|item| {
                    item?
                        .cast_into::<PyBytes>()
                        .map(|b| b.as_bytes().to_vec())
                        .map_err(|_| PyValueError::new_err("options must be bytes"))
                })
                .collect::<PyResult<_>>()?;
            let memo = rec.get_item(2)?;
            let pos: u64 = memo.get_item(1)?.extract()?;
            let size: u64 = memo.get_item(2)?.extract()?;
            let parents_obj = rec.get_item(3)?;
            let parents: Vec<Vec<Vec<u8>>> = if parents_obj.is_none() {
                vec![]
            } else {
                parents_obj
                    .try_iter()?
                    .map(|p| extract_py_knit_key_or_bytes(&p?))
                    .collect::<PyResult<_>>()?
            };
            all_recs.push((key, options, pos, size as usize, parents));
        }
        // Group by kndx path (sorted for determinism)
        let mut path_groups: std::collections::BTreeMap<String, (Vec<Vec<u8>>, Vec<usize>)> =
            std::collections::BTreeMap::new();
        for (i, (key, _, _, _, _)) in all_recs.iter().enumerate() {
            let prefix = PyKndxIndexInner::prefix_of(key);
            let path = self.inner.prefix_path(&prefix);
            let entry = path_groups
                .entry(path)
                .or_insert_with(|| (prefix, Vec::new()));
            entry.1.push(i);
        }
        for (path, (prefix, indices)) in path_groups {
            self.inner
                .load_prefix_shared(prefix.clone())
                .map_err(transport_err_to_py)?;
            // Snapshot whether history was non-empty before we add any records
            // for this prefix (mirrors Python's `orig_history` check).
            let had_history = self
                .inner
                .kndx_cache()
                .lock()
                .unwrap()
                .get(&prefix)
                .map(|p| !p.history.is_empty())
                .unwrap_or(false);
            let mut lines: Vec<Vec<u8>> = Vec::new();
            for idx in indices {
                let (key, options, pos, size, parents) = &all_recs[idx];
                let suffix = PyKndxIndexInner::suffix_of(key);
                let parent_suffixes: Vec<Vec<u8>> = parents
                    .iter()
                    .map(|p| PyKndxIndexInner::suffix_of(p))
                    .collect();
                let cache_lookup: std::collections::HashMap<Vec<u8>, u64> = {
                    let cache = self.inner.kndx_cache().lock().unwrap();
                    cache
                        .get(&prefix)
                        .map(|pc| {
                            pc.cache
                                .iter()
                                .map(|(k, v)| (k.clone(), v.index as u64))
                                .collect()
                        })
                        .unwrap_or_default()
                };
                let lookup_refs: std::collections::HashMap<&[u8], u64> = cache_lookup
                    .iter()
                    .map(|(k, v)| (k.as_slice(), *v))
                    .collect();
                let parent_refs = bazaar::knit::dictionary_compress_suffixes(
                    &parent_suffixes
                        .iter()
                        .map(|s| s.as_slice())
                        .collect::<Vec<_>>(),
                    &lookup_refs,
                );
                let mut line = b"\n".to_vec();
                line.extend_from_slice(&suffix);
                line.push(b' ');
                line.extend_from_slice(&options.join(b",".as_ref()));
                line.push(b' ');
                line.extend_from_slice(pos.to_string().as_bytes());
                line.push(b' ');
                line.extend_from_slice(size.to_string().as_bytes());
                line.push(b' ');
                line.extend_from_slice(&parent_refs);
                line.extend_from_slice(b" :");
                // Update the in-memory cache
                {
                    let mut cache = self.inner.kndx_cache().lock().unwrap();
                    let pc = cache.entry(prefix.clone()).or_default();
                    let index = if !pc.cache.contains_key(&suffix) {
                        let idx = pc.history.len();
                        pc.history.push(suffix.clone());
                        idx
                    } else {
                        pc.cache[&suffix].index
                    };
                    pc.cache.insert(
                        suffix.clone(),
                        bazaar::knit::KndxCacheEntry {
                            version_id: suffix,
                            options: options.clone(),
                            pos: *pos,
                            size: *size,
                            parents: parent_suffixes,
                            index,
                        },
                    );
                }
                lines.push(line);
            }
            let all_bytes: Vec<u8> = lines.into_iter().flatten().collect();
            if had_history {
                self.inner
                    .transport()
                    .append_bytes(&path, &all_bytes)
                    .map_err(transport_err_to_py)?;
            } else {
                let mut content = bazaar::knit::KNDX_HEADER.to_vec();
                content.extend_from_slice(&all_bytes);
                self.inner
                    .transport()
                    .put_file_non_atomic(&path, &content, true)
                    .map_err(transport_err_to_py)?;
            }
        }
        Ok(())
    }

    fn keys(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self._check_read(py)?;
        use bazaar::key_mapper::Mapper as _;
        // Collect the prefixes to load: for a ConstantMapper there is exactly
        // one (the empty prefix); for other mappers we enumerate the transport.
        let prefixes: Vec<Vec<Vec<u8>>> = if self.inner.mapper().is_constant() {
            vec![vec![]]
        } else {
            self.inner
                .transport()
                .iter_files_recursive()
                .map_err(transport_err_to_py)?
                .into_iter()
                .filter_map(|relpath| {
                    let path = std::path::Path::new(&relpath);
                    if path.extension().and_then(|e| e.to_str()) == Some("kndx") {
                        let stem = path.with_extension("").to_string_lossy().into_owned();
                        Some(self.inner.mapper().unmap(&stem))
                    } else {
                        None
                    }
                })
                .collect()
        };
        for prefix in &prefixes {
            self.inner
                .load_prefix_typed(prefix.clone())
                .map_err(|e| kndx_load_err_to_py(py, e))?;
        }
        let result = pyo3::types::PySet::empty(py)?;
        let cache = self.inner.kndx_cache().lock().unwrap();
        for prefix in &prefixes {
            if let Some(pc) = cache.get(prefix) {
                for suffix in &pc.history {
                    let mut key = prefix.clone();
                    key.push(suffix.clone());
                    result.add(py_knit_key_to_py(py, &key)?)?;
                }
            }
        }
        Ok(result.into_any().unbind())
    }

    fn scan_unvalidated_index(&self, _graph_index: Bound<'_, PyAny>) -> PyResult<()> {
        Err(PyNotImplementedError::new_err("scan_unvalidated_index"))
    }

    fn _get_total_build_size(
        &self,
        py: Python<'_>,
        keys: Bound<'_, PyAny>,
        positions: Bound<'_, PyDict>,
    ) -> PyResult<usize> {
        get_total_build_size_rs(py, keys, positions)
    }

    fn __contains__(&mut self, py: Python<'_>, key: Bound<'_, PyAny>) -> PyResult<bool> {
        let rust_key = extract_py_knit_key_or_bytes(&key)?;
        let prefix = PyKndxIndexInner::prefix_of(&rust_key);
        let suffix = PyKndxIndexInner::suffix_of(&rust_key);
        self.inner
            .load_prefix_shared(prefix.clone())
            .map_err(transport_err_to_py)?;
        let cache = self.inner.kndx_cache().lock().unwrap();
        Ok(cache
            .get(&prefix)
            .map(|pc| pc.cache.contains_key(&suffix))
            .unwrap_or(false))
    }

    fn get_missing_compression_parents<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let missing = self
            .inner
            .get_missing_compression_parents()
            .map_err(knit_err_to_py)?;
        let s = pyo3::types::PyFrozenSet::new(
            py,
            missing
                .iter()
                .map(|k| py_knit_key_to_py(py, k))
                .collect::<PyResult<Vec<_>>>()?,
        )?;
        Ok(s.into_any())
    }

    fn check_header(&self, py: Python<'_>, fp: Bound<'_, PyAny>) -> PyResult<()> {
        let line = fp.call_method0("readline")?;
        let line = line
            .cast_into::<PyBytes>()
            .map_err(|_| PyValueError::new_err("check_header: expected bytes from readline"))?;
        if line.as_bytes().is_empty() {
            return Err(NoSuchFile::new_err((py.None(),)));
        }
        if line.as_bytes() != bazaar::knit::KNDX_HEADER {
            return Err(KnitHeaderError::new_err((
                line.into_any().unbind(),
                py.None(),
            )));
        }
        Ok(())
    }

    fn find_ancestry(
        &mut self,
        py: Python<'_>,
        keys: Bound<'_, PyAny>,
    ) -> PyResult<(Py<PyDict>, Py<PyAny>)> {
        self._check_read(py)?;
        let rust_keys = extract_py_knit_keys(&keys)?;
        // Load all prefix files first
        let prefixes: std::collections::HashSet<Vec<Vec<u8>>> =
            rust_keys.iter().map(PyKndxIndexInner::prefix_of).collect();
        for prefix in &prefixes {
            self.inner
                .load_prefix_shared(prefix.clone())
                .map_err(transport_err_to_py)?;
        }
        let mut parent_map: std::collections::HashMap<Vec<Vec<u8>>, Vec<Vec<Vec<u8>>>> =
            std::collections::HashMap::new();
        let mut missing: std::collections::HashSet<Vec<Vec<u8>>> = std::collections::HashSet::new();
        let mut pending = rust_keys.clone();
        while let Some(key) = pending.pop() {
            if parent_map.contains_key(&key) {
                continue;
            }
            let prefix = PyKndxIndexInner::prefix_of(&key);
            let suffix = PyKndxIndexInner::suffix_of(&key);
            self.inner
                .load_prefix_shared(prefix.clone())
                .map_err(transport_err_to_py)?;
            let cache = self.inner.kndx_cache().lock().unwrap();
            if let Some(pc) = cache.get(&prefix) {
                if let Some(entry) = pc.cache.get(&suffix) {
                    let parent_keys: Vec<Vec<Vec<u8>>> = entry
                        .parents
                        .iter()
                        .map(|p| {
                            let mut pk = prefix.clone();
                            pk.push(p.clone());
                            pk
                        })
                        .collect();
                    for pk in &parent_keys {
                        if !parent_map.contains_key(pk) {
                            pending.push(pk.clone());
                        }
                    }
                    drop(cache);
                    parent_map.insert(key, parent_keys);
                } else {
                    missing.insert(key);
                }
            } else {
                missing.insert(key);
            }
        }
        let py_parent_map = PyDict::new(py);
        for (key, parents) in parent_map {
            let py_key = py_knit_key_to_py(py, &key)?;
            let py_parents = PyTuple::new(
                py,
                parents
                    .iter()
                    .map(|p| py_knit_key_to_py(py, p))
                    .collect::<PyResult<Vec<_>>>()?,
            )?;
            py_parent_map.set_item(py_key, py_parents)?;
        }
        let py_missing = pyo3::types::PySet::empty(py)?;
        for key in missing {
            py_missing.add(py_knit_key_to_py(py, &key)?)?;
        }
        Ok((py_parent_map.unbind(), py_missing.into_any().unbind()))
    }

    fn _sort_keys_by_io(
        &self,
        py: Python<'_>,
        keys: Bound<'_, pyo3::types::PyList>,
        positions: Bound<'_, PyDict>,
    ) -> PyResult<()> {
        // Sort keys in-place grouped by index file and ordered by byte position.
        // positions[key] = (record_details, index_memo, next, parents)
        // For _KndxIndex, index_memo = (key_tuple, pos, size).
        // Group by the .kndx path (derived from the key's prefix via the
        // mapper), then sort by byte offset within each file.
        let n = keys.len();
        let mut keyed: Vec<(String, u64, Bound<'_, PyAny>)> = Vec::with_capacity(n);
        for i in 0..n {
            let k = keys.get_item(i)?;
            let rust_key = extract_py_knit_key(&k)?;
            let prefix = PyKndxIndexInner::prefix_of(&rust_key);
            let path = self.inner.prefix_path(&prefix);
            let pos_entry = positions
                .get_item(&k)?
                .ok_or_else(|| PyValueError::new_err("_sort_keys_by_io: key not in positions"))?;
            let index_memo = pos_entry.get_item(1)?;
            let pos: u64 = index_memo.get_item(1)?.extract()?;
            keyed.push((path, pos, k));
        }
        keyed.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        for (i, (_, _, k)) in keyed.into_iter().enumerate() {
            keys.set_item(i, k)?;
        }
        Ok(())
    }
}

/// PyO3 wrapper around a Python callable used as the `add_callback` for
/// [`bazaar::knit::KnitGraphIndex`].
struct PyAddCallback(Py<PyAny>);

impl bazaar::knit::AddCallback for PyAddCallback {
    fn call(
        &mut self,
        entries: &[(
            bazaar::knit::KnitKey,
            Vec<u8>,
            Vec<Vec<bazaar::knit::KnitKey>>,
        )],
        has_parents: bool,
    ) -> Result<(), bazaar::knit::KnitError> {
        Python::attach(|py| {
            // Any Python error en route gets stashed via knit_err_from_py
            // so the boundary can re-raise the original exception verbatim
            // (e.g. ObjectNotLocked) rather than wrapping it as KnitCorrupt.
            let build = || -> PyResult<()> {
                let result = pyo3::types::PyList::empty(py);
                if has_parents {
                    for (key, value, node_refs) in entries {
                        let py_key = py_knit_key_to_py(py, key)?;
                        let py_value = PyBytes::new(py, value);
                        let py_refs = PyTuple::new(
                            py,
                            node_refs
                                .iter()
                                .map(|rl| {
                                    PyTuple::new(
                                        py,
                                        rl.iter()
                                            .map(|k| py_knit_key_to_py(py, k))
                                            .collect::<PyResult<Vec<_>>>()?,
                                    )
                                })
                                .collect::<PyResult<Vec<_>>>()?,
                        )?;
                        result.append(PyTuple::new(
                            py,
                            [py_key.into_any(), py_value.into_any(), py_refs.into_any()],
                        )?)?;
                    }
                } else {
                    for (key, value, _) in entries {
                        let py_key = py_knit_key_to_py(py, key)?;
                        let py_value = PyBytes::new(py, value);
                        result.append(PyTuple::new(
                            py,
                            [py_key.into_any(), py_value.into_any()],
                        )?)?;
                    }
                }
                self.0.bind(py).call1((result,))?;
                Ok(())
            };
            build().map_err(|e| knit_err_from_py(py, e))
        })
    }
}

/// pyo3 wrapper that exposes `_KnitGraphIndex` to Python.
///
/// Wraps a Python `CombinedGraphIndex` (or any compatible graph index) and
/// implements the same public interface as the Python `_KnitGraphIndex` class.
/// Graph-index I/O is delegated back to the wrapped Python object; all
/// knit-specific encoding/decoding and state management runs in Rust via
/// [`bazaar::knit::KnitGraphIndex`].
#[pyclass(name = "_KnitGraphIndex")]
pub struct PyKnitGraphIndex {
    graph_index: Py<PyAny>,
    is_locked: Py<PyAny>,
    inner: bazaar::knit::KnitGraphIndex<PyAddCallback>,
}

#[pymethods]
impl PyKnitGraphIndex {
    #[new]
    #[pyo3(signature = (graph_index, is_locked, deltas=false, parents=true, add_callback=None, track_external_parent_refs=false))]
    fn new(
        graph_index: Bound<'_, PyAny>,
        is_locked: Bound<'_, PyAny>,
        deltas: bool,
        parents: bool,
        add_callback: Option<Bound<'_, PyAny>>,
        track_external_parent_refs: bool,
    ) -> PyResult<Self> {
        if deltas && !parents {
            return Err(knit_err_to_py(bazaar::knit::KnitError::Corrupt(
                "Cannot do delta compression without parent tracking.".to_string(),
            )));
        }
        let mut inner = bazaar::knit::KnitGraphIndex::new(deltas, parents);
        if let Some(cb) = add_callback {
            inner.set_add_callback(PyAddCallback(cb.unbind()));
        }
        if track_external_parent_refs {
            inner.enable_key_dependencies(false);
        }
        Ok(Self {
            graph_index: graph_index.unbind(),
            is_locked: is_locked.unbind(),
            inner,
        })
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let gi_repr = self.graph_index.bind(py).repr()?;
        Ok(format!("_KnitGraphIndex({})", gi_repr))
    }

    #[getter]
    fn has_graph(&self) -> bool {
        self.inner.parents
    }

    #[getter]
    fn _graph_index(&self, py: Python<'_>) -> Py<PyAny> {
        self.graph_index.clone_ref(py)
    }

    /// Returns `self` when key_dependencies tracking is enabled, else `None`.
    /// Python callers get back the `_KnitGraphIndex` itself and call
    /// `get_referrers()` / `satisfy_refs_for_keys()` / `get_new_keys()` on it.
    #[getter]
    fn key_dependencies(slf: PyRef<'_, Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if slf.inner.key_dependencies.is_some() {
            Ok(Py::from(slf).into_any())
        } else {
            Ok(py.None())
        }
    }

    fn set_add_callback(&mut self, value: Option<Bound<'_, PyAny>>) {
        self.inner.add_callback = value.map(|v| PyAddCallback(v.unbind()));
    }

    fn clear_key_dependencies(&mut self) {
        self.inner.clear_key_dependencies();
    }

    fn get_referrers<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let refs = self.inner.referrers();
        let result = pyo3::types::PyList::empty(py);
        for key in refs {
            result.append(py_knit_key_to_py(py, &key)?)?;
        }
        Ok(result.into_any())
    }

    fn satisfy_refs_for_keys(&mut self, keys: Bound<'_, PyAny>) -> PyResult<()> {
        let rust_keys = extract_py_knit_keys(&keys)?;
        self.inner.satisfy_refs_for_keys(rust_keys);
        Ok(())
    }

    fn get_new_keys<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let Some(new_keys) = self.inner.new_keys() else {
            return Ok(pyo3::types::PyFrozenSet::empty(py)?.into_any());
        };
        let result = pyo3::types::PyFrozenSet::new(
            py,
            new_keys
                .iter()
                .map(|k| py_knit_key_to_py(py, k))
                .collect::<PyResult<Vec<_>>>()?,
        )?;
        Ok(result.into_any())
    }

    fn add_missing_compression_parent(&mut self, key: Bound<'_, PyAny>) -> PyResult<()> {
        let k = extract_py_knit_key(&key)?;
        self.inner.add_missing_compression_parent(k);
        Ok(())
    }

    fn _check_read(&self, py: Python<'_>) -> PyResult<()> {
        if !self.is_locked.bind(py).call0()?.is_truthy()? {
            return Err(ObjectNotLocked::new_err((py.None(),)));
        }
        Ok(())
    }

    fn _check_write_ok(&self, py: Python<'_>) -> PyResult<()> {
        self._check_read(py)
    }

    fn keys(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self._check_read(py)?;
        let entries = self.graph_index.bind(py).call_method0("iter_all_entries")?;
        let result = pyo3::types::PyList::empty(py);
        for entry in entries.try_iter()? {
            let entry = entry?;
            result.append(entry.get_item(1)?)?;
        }
        Ok(result.into_any().unbind())
    }

    fn get_parent_map<'py>(
        &self,
        py: Python<'py>,
        keys: Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyDict>> {
        self._check_read(py)?;
        let result = PyDict::new(py);
        let nodes = self._get_entries(py, &keys)?;
        let nodes = nodes.bind(py);
        if self.inner.parents {
            for entry in nodes.try_iter()? {
                let entry = entry?.cast_into::<PyTuple>()?;
                let key = entry.get_item(1)?;
                let refs = entry.get_item(3)?;
                let parents = refs.get_item(0)?;
                result.set_item(key, parents)?;
            }
        } else {
            for entry in nodes.try_iter()? {
                let entry = entry?.cast_into::<PyTuple>()?;
                let key = entry.get_item(1)?;
                result.set_item(key, py.None())?;
            }
        }
        Ok(result)
    }

    fn get_build_details<'py>(
        &self,
        py: Python<'py>,
        keys: Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyDict>> {
        self._check_read(py)?;
        let entries = self._get_entries(py, &keys)?;
        let entries = entries.into_bound(py);
        knit_entries_to_build_details_rs(py, entries, self.inner.parents, self.inner.deltas)
    }

    fn get_method(&self, py: Python<'_>, key: Bound<'_, PyAny>) -> PyResult<String> {
        let node = self._get_node(py, &key)?;
        self._get_method_from_node(&node.bind(py))
    }

    fn get_options(&self, py: Python<'_>, key: Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let node = self._get_node(py, &key)?;
        let node = node.bind(py);
        let method = self._get_method_from_node(node)?;
        let result = pyo3::types::PyList::empty(py);
        result.append(PyBytes::new(py, method.as_bytes()))?;
        let value = node.get_item(2)?.cast_into::<PyBytes>()?;
        if value.as_bytes().first() == Some(&b'N') {
            result.append(PyBytes::new(py, b"no-eol"))?;
        }
        Ok(result.into_any().unbind())
    }

    fn get_position<'py>(
        &self,
        py: Python<'py>,
        key: Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, PyTuple>> {
        let node = self._get_node(py, &key)?;
        let node = node.bind(py);
        let value = node.get_item(2)?.cast_into::<PyBytes>()?;
        let parsed =
            bazaar::knit::parse_knit_index_value(value.as_bytes()).map_err(knit_err_to_py)?;
        let graph_index = node.get_item(0)?;
        PyTuple::new(
            py,
            [
                graph_index,
                parsed.pos.into_pyobject(py)?.into_any(),
                parsed.size.into_pyobject(py)?.into_any(),
            ],
        )
    }

    fn __contains__(&self, py: Python<'_>, key: Bound<'_, PyAny>) -> PyResult<bool> {
        let key_list = pyo3::types::PyList::new(py, [key.clone()])?;
        let result = self.get_parent_map(py, key_list.into_any())?;
        Ok(result.contains(&key)?)
    }

    fn find_ancestry(&self, py: Python<'_>, keys: Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self._check_read(py)?;
        self.graph_index
            .bind(py)
            .call_method1("find_ancestry", (keys, 0usize))
            .map(|r| r.unbind())
    }

    fn _sort_keys_by_io(
        &self,
        py: Python<'_>,
        keys: Bound<'_, pyo3::types::PyList>,
        positions: Bound<'_, PyDict>,
    ) -> PyResult<()> {
        let n = keys.len();
        let mut keyed: Vec<(usize, u64, Bound<'_, PyAny>)> = Vec::with_capacity(n);
        for i in 0..n {
            let k = keys.get_item(i)?;
            let pos_entry = positions
                .get_item(&k)?
                .ok_or_else(|| PyValueError::new_err("_sort_keys_by_io: key not in positions"))?;
            let index_memo = pos_entry.get_item(1)?;
            let file_ref = index_memo.get_item(0)?;
            let file_id: usize = py
                .import("builtins")?
                .getattr("id")?
                .call1((&file_ref,))?
                .extract()?;
            let pos: u64 = index_memo.get_item(1)?.extract()?;
            keyed.push((file_id, pos, k));
        }
        keyed.sort_by_key(|(file_id, pos, _)| (*file_id, *pos));
        for (i, (_, _, k)) in keyed.into_iter().enumerate() {
            keys.set_item(i, k)?;
        }
        Ok(())
    }

    fn _get_total_build_size(
        &self,
        py: Python<'_>,
        keys: Bound<'_, PyAny>,
        positions: Bound<'_, PyDict>,
    ) -> PyResult<usize> {
        get_total_build_size_rs(py, keys, positions)
    }

    fn get_missing_compression_parents<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let s = pyo3::types::PyFrozenSet::new(
            py,
            self.inner
                .missing_compression_parents
                .iter()
                .map(|k| py_knit_key_to_py(py, k))
                .collect::<PyResult<Vec<_>>>()?,
        )?;
        Ok(s.into_any())
    }

    fn get_missing_parents<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let Some(kd) = &self.inner.key_dependencies else {
            return Ok(pyo3::types::PyFrozenSet::empty(py)?.into_any());
        };
        let unsatisfied_keys: Vec<bazaar::knit::KnitKey> = kd.unsatisfied_refs().cloned().collect();
        let py_keys = pyo3::types::PyList::new(
            py,
            unsatisfied_keys
                .iter()
                .map(|k| py_knit_key_to_py(py, k))
                .collect::<PyResult<Vec<_>>>()?,
        )?;
        let parent_map = self.get_parent_map(py, py_keys.into_any())?;
        let satisfied: std::collections::HashSet<bazaar::knit::KnitKey> = parent_map
            .keys()
            .try_iter()?
            .map(|k| extract_py_knit_key(&k?))
            .collect::<PyResult<_>>()?;
        let remaining: Vec<_> = unsatisfied_keys
            .iter()
            .filter(|k| !satisfied.contains(*k))
            .map(|k| py_knit_key_to_py(py, k))
            .collect::<PyResult<_>>()?;
        let s = pyo3::types::PyFrozenSet::new(py, remaining)?;
        Ok(s.into_any())
    }

    fn scan_unvalidated_index(
        &mut self,
        py: Python<'_>,
        graph_index: Bound<'_, PyAny>,
    ) -> PyResult<()> {
        if self.inner.deltas {
            let new_missing = graph_index.call_method1("external_references", (1usize,))?;
            let new_missing_keys = extract_py_knit_keys(&new_missing)?;
            let parent_map = self.get_parent_map(py, new_missing.clone())?;
            let present_keys: std::collections::HashSet<bazaar::knit::KnitKey> = parent_map
                .keys()
                .try_iter()?
                .map(|k| extract_py_knit_key(&k?))
                .collect::<PyResult<_>>()?;
            self.inner
                .update_missing_compression_parents(new_missing_keys, &present_keys);
        }
        if self.inner.key_dependencies.is_some() {
            for node in graph_index.call_method0("iter_all_entries")?.try_iter()? {
                let node = node?.cast_into::<PyTuple>()?;
                let key = extract_py_knit_key(&node.get_item(1)?)?;
                let refs = node.get_item(3)?;
                let parent_refs = refs.get_item(0)?;
                let parent_keys: Vec<bazaar::knit::KnitKey> = parent_refs
                    .try_iter()?
                    .map(|k| extract_py_knit_key(&k?))
                    .collect::<PyResult<_>>()?;
                self.inner.add_key_dependencies(key, parent_keys);
            }
        }
        Ok(())
    }

    #[pyo3(signature = (records, random_id=false, missing_compression_parents=false))]
    fn add_records(
        &mut self,
        py: Python<'_>,
        records: Bound<'_, PyAny>,
        random_id: bool,
        missing_compression_parents: bool,
    ) -> PyResult<()> {
        if self.inner.add_callback.is_none() {
            return Err(ReadOnlyError::new_err((py.None(),)));
        }

        type KnitKey = bazaar::knit::KnitKey;

        let mut inputs: Vec<bazaar::knit::AddRecordInput> = Vec::new();
        for rec in records.try_iter()? {
            let rec = rec?.cast_into::<PyTuple>()?;
            let key = extract_py_knit_key_or_bytes(&rec.get_item(0)?)?;
            let options_obj = rec.get_item(1)?;
            let options: Vec<u8> = if let Ok(b) = options_obj.clone().cast_into::<PyBytes>() {
                b.as_bytes().to_vec()
            } else {
                let mut buf = Vec::new();
                for (i, opt) in options_obj.try_iter()?.enumerate() {
                    if i > 0 {
                        buf.push(b',');
                    }
                    let ob = opt?
                        .cast_into::<PyBytes>()
                        .map_err(|_| PyValueError::new_err("options must be bytes"))?;
                    buf.extend_from_slice(ob.as_bytes());
                }
                buf
            };
            let memo = rec.get_item(2)?;
            let pos: u64 = memo.get_item(1)?.extract()?;
            let size: u64 = memo.get_item(2)?.extract()?;
            let parents_obj = rec.get_item(3)?;
            let parents: Vec<KnitKey> = if parents_obj.is_none() {
                Vec::new()
            } else {
                parents_obj
                    .try_iter()?
                    .map(|p| extract_py_knit_key_or_bytes(&p?))
                    .collect::<PyResult<_>>()?
            };
            inputs.push(bazaar::knit::AddRecordInput {
                key,
                options,
                pos,
                size,
                parents,
            });
        }

        let mut to_remove: std::collections::HashSet<KnitKey> = std::collections::HashSet::new();
        if !random_id {
            let prepared = bazaar::knit::prepare_dedup_records(
                &inputs,
                self.inner.parents,
                self.inner.deltas,
            )
            .map_err(knit_err_to_py)?;
            let py_keys = pyo3::types::PyList::new(
                py,
                prepared
                    .iter()
                    .map(|p| py_knit_key_to_py(py, &p.key))
                    .collect::<PyResult<Vec<_>>>()?,
            )?;
            let existing_iter = self._get_entries(py, py_keys.as_any())?;
            let existing_iter = existing_iter.bind(py);
            let mut existing: Vec<bazaar::knit::ExistingAddRecord> = Vec::new();
            for node in existing_iter.try_iter()? {
                let node = node?.cast_into::<PyTuple>()?;
                let key = extract_py_knit_key(&node.get_item(1)?)?;
                let value = node
                    .get_item(2)?
                    .cast_into::<PyBytes>()?
                    .as_bytes()
                    .to_vec();
                let parents: Vec<KnitKey> = node
                    .get_item(3)?
                    .get_item(0)
                    .ok()
                    .map(|rl| {
                        rl.try_iter()?
                            .map(|k| extract_py_knit_key(&k?))
                            .collect::<PyResult<Vec<_>>>()
                    })
                    .transpose()?
                    .unwrap_or_default();
                existing.push(bazaar::knit::ExistingAddRecord {
                    key,
                    value,
                    parents,
                });
            }
            to_remove = bazaar::knit::verify_dedup_records(&prepared, &existing)
                .map_err(knit_err_to_py)?;
        }

        let filtered = inputs.into_iter().filter_map(|i| {
            if to_remove.contains(&i.key) {
                None
            } else {
                Some((i.key, i.options, i.pos, i.size, i.parents))
            }
        });

        self.inner
            .encode_and_dispatch(filtered, missing_compression_parents)
            .map_err(knit_err_to_py)
    }
}

impl PyKnitGraphIndex {
    /// Call `graph_index.iter_entries(keys)`, adapting parentless indices by
    /// appending an empty refs tuple. Returns an unbound `Py<PyAny>` (a list)
    /// so callers can rebind it to any lifetime.
    fn _get_entries(&self, py: Python<'_>, keys: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let gi = self.graph_index.bind(py);
        if self.inner.parents {
            let result = gi.call_method1("iter_entries", (keys,))?;
            Ok(result.unbind())
        } else {
            let raw = gi.call_method1("iter_entries", (keys,))?;
            let adapted = pyo3::types::PyList::empty(py);
            for entry in raw.try_iter()? {
                let entry = entry?.cast_into::<PyTuple>()?;
                let with_empty_refs = PyTuple::new(
                    py,
                    [
                        entry.get_item(0)?,
                        entry.get_item(1)?,
                        entry.get_item(2)?,
                        PyTuple::empty(py).into_any(),
                    ],
                )?;
                adapted.append(with_empty_refs)?;
            }
            Ok(adapted.into_any().unbind())
        }
    }

    fn _get_node(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<Py<PyTuple>> {
        let key_list = pyo3::types::PyList::new(py, [key.clone()])?;
        let entries = self._get_entries(py, key_list.as_any())?;
        let entries = entries.bind(py);
        let mut iter = entries.try_iter()?;
        match iter.next() {
            Some(entry) => Ok(entry?.cast_into::<PyTuple>()?.unbind()),
            None => {
                let exc_cls = py
                    .import("bzrformats.errors")?
                    .getattr("RevisionNotPresent")?;
                Err(PyErr::from_value(
                    exc_cls.call1((key, py.None()))?.unbind().into_bound(py),
                ))
            }
        }
    }

    fn _get_method_from_node(&self, node: &Bound<'_, PyTuple>) -> PyResult<String> {
        if !self.inner.deltas {
            return Ok("fulltext".to_string());
        }
        let refs = node.get_item(3)?;
        let has_compression_parent = refs.len()? > 1 && refs.get_item(1)?.len()? > 0;
        if has_compression_parent {
            Ok("line-delta".to_string())
        } else {
            Ok("fulltext".to_string())
        }
    }
}

/// pyo3 wrapper around `bazaar::knit::KnitKeyAccess`.
///
/// Exposes the same interface as the Python `_KnitKeyAccess` class.
#[pyclass(name = "_KnitKeyAccess")]
pub struct PyKnitKeyAccess {
    inner: bazaar::knit::KnitKeyAccess<crate::transport::PyTransport, crate::transport::PyMapper>,
    transport_obj: Py<PyAny>,
    mapper_obj: Py<PyAny>,
}

#[pymethods]
impl PyKnitKeyAccess {
    #[new]
    fn new(transport: Bound<'_, PyAny>, mapper: Bound<'_, PyAny>) -> Self {
        use crate::transport::{PyMapper, PyTransport};
        use bazaar::knit::KnitKeyAccess;
        Self {
            inner: KnitKeyAccess::new(
                PyTransport::new(transport.clone()),
                PyMapper::new(mapper.clone()),
            ),
            transport_obj: transport.unbind(),
            mapper_obj: mapper.unbind(),
        }
    }

    #[getter]
    fn _transport(&self, py: Python<'_>) -> Py<PyAny> {
        self.transport_obj.clone_ref(py)
    }

    #[getter]
    fn _mapper(&self, py: Python<'_>) -> Py<PyAny> {
        self.mapper_obj.clone_ref(py)
    }

    fn add_raw_record(
        &self,
        py: Python<'_>,
        key: Bound<'_, PyAny>,
        size: usize,
        raw_data: Bound<'_, PyAny>,
    ) -> PyResult<Py<PyTuple>> {
        let rust_key = extract_py_knit_key_or_bytes(&key)?;
        let data: Vec<u8> = {
            let mut buf = Vec::new();
            for chunk in raw_data.try_iter()? {
                let b = chunk?
                    .cast_into::<PyBytes>()
                    .map_err(|_| PyValueError::new_err("raw_data must be iterable of bytes"))?;
                buf.extend_from_slice(b.as_bytes());
            }
            buf
        };
        let _ = size;
        let (ret_key, offset, ret_size) = self
            .inner
            .add_raw_record_bytes(rust_key, &data)
            .map_err(transport_err_to_py)?;
        let py_key = py_knit_key_to_py(py, &ret_key)?;
        Ok(PyTuple::new(
            py,
            [
                py_key.into_any(),
                offset.into_pyobject(py)?.into_any(),
                ret_size.into_pyobject(py)?.into_any(),
            ],
        )?
        .unbind())
    }

    fn add_raw_records(
        &self,
        py: Python<'_>,
        key_sizes: Bound<'_, PyAny>,
        raw_data: Bound<'_, PyAny>,
    ) -> PyResult<Py<PyList>> {
        let all_data: Vec<u8> = {
            let mut buf = Vec::new();
            for chunk in raw_data.try_iter()? {
                let b = chunk?
                    .cast_into::<PyBytes>()
                    .map_err(|_| PyValueError::new_err("raw_data must be iterable of bytes"))?;
                buf.extend_from_slice(b.as_bytes());
            }
            buf
        };
        let result = PyList::empty(py);
        let mut offset = 0usize;
        for item in key_sizes.try_iter()? {
            let item = item?;
            let key = extract_py_knit_key_or_bytes(&item.get_item(0)?)?;
            let size: usize = item.get_item(1)?.extract()?;
            let slice = &all_data[offset..offset + size];
            let (ret_key, ret_offset, ret_size) = self
                .inner
                .add_raw_record_bytes(key, slice)
                .map_err(transport_err_to_py)?;
            let py_key = py_knit_key_to_py(py, &ret_key)?;
            let memo = PyTuple::new(
                py,
                [
                    py_key.into_any(),
                    ret_offset.into_pyobject(py)?.into_any(),
                    ret_size.into_pyobject(py)?.into_any(),
                ],
            )?;
            result.append(memo)?;
            offset += size;
        }
        Ok(result.unbind())
    }

    fn flush(&self) {}

    fn get_raw_records<'py>(
        &self,
        py: Python<'py>,
        memos_for_retrieval: Bound<'py, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let list = PyList::empty(py);
        // Group by prefix path for efficient readv batching
        let mut request_lists: Vec<(String, Vec<(u64, usize)>)> = Vec::new();
        let mut current_path: Option<String> = None;
        for memo in memos_for_retrieval.try_iter()? {
            let memo = memo?;
            let key = extract_py_knit_key(&memo.get_item(0)?)?;
            let offset: u64 = memo.get_item(1)?.extract()?;
            let length: usize = memo.get_item(2)?.extract()?;
            // Derive path from key prefix
            let prefix = PyKndxIndexInner::prefix_of(&key);
            let path = {
                let refs: Vec<&[u8]> = prefix.iter().map(|s| s.as_slice()).collect();
                self.inner.mapper().map(&refs) + ".knit"
            };
            match current_path.as_deref() {
                Some(p) if p == path => {
                    request_lists.last_mut().unwrap().1.push((offset, length));
                }
                _ => {
                    current_path = Some(path.clone());
                    request_lists.push((path, vec![(offset, length)]));
                }
            }
        }
        for (path, read_vector) in request_lists {
            let ranges: Vec<bazaar::transport::ReadRange> = read_vector
                .iter()
                .map(|&(offset, length)| bazaar::transport::ReadRange { offset, length })
                .collect();
            let results = self
                .inner
                .transport()
                .readv(&path, &ranges)
                .map_err(transport_err_to_py)?;
            for r in results {
                list.append(PyBytes::new(py, &r.bytes))?;
            }
        }
        Ok(list.into_any().call_method0("__iter__")?.unbind())
    }
}

// ── helpers used by PyKndxIndex ────────────────────────────────────────────

/// Extract a knit key from a Python object that is either a tuple of bytes
/// (the normal case) or a plain bytes object (accepted by the legacy
/// `get_method` / `get_options` API that some tests rely on).
fn extract_py_knit_key_or_bytes(obj: &Bound<'_, PyAny>) -> PyResult<bazaar::knit::KnitKey> {
    if let Ok(b) = obj.clone().cast_into::<PyBytes>() {
        return Ok(vec![b.as_bytes().to_vec()]);
    }
    extract_py_knit_key(obj)
}

fn extract_py_knit_key(obj: &Bound<'_, PyAny>) -> PyResult<bazaar::knit::KnitKey> {
    let tup = obj
        .downcast::<PyTuple>()
        .map_err(|_| PyValueError::new_err("knit key must be a tuple of bytes"))?;
    let mut key = Vec::with_capacity(tup.len());
    for item in tup.iter() {
        let b = item
            .cast_into::<PyBytes>()
            .map_err(|_| PyValueError::new_err("knit key elements must be bytes"))?;
        key.push(b.as_bytes().to_vec());
    }
    Ok(key)
}

/// Like `extract_py_knit_key` but allows the last element to be `None`.
/// Returns `(key, true)` when the last element was `None` (auto-generate).
fn extract_py_knit_key_autogen(obj: &Bound<'_, PyAny>) -> PyResult<(bazaar::knit::KnitKey, bool)> {
    let tup = obj
        .downcast::<PyTuple>()
        .map_err(|_| PyValueError::new_err("knit key must be a tuple of bytes"))?;
    let mut key = Vec::with_capacity(tup.len());
    let mut autogen = false;
    let len = tup.len();
    for (i, item) in tup.iter().enumerate() {
        if i == len - 1 && item.is_none() {
            autogen = true;
            key.push(Vec::new()); // placeholder, filled in after digest
        } else {
            let b = item
                .cast_into::<PyBytes>()
                .map_err(|_| PyValueError::new_err("knit key elements must be bytes"))?;
            key.push(b.as_bytes().to_vec());
        }
    }
    Ok((key, autogen))
}

fn extract_py_knit_keys(obj: &Bound<'_, PyAny>) -> PyResult<Vec<bazaar::knit::KnitKey>> {
    // Silently skip malformed keys: callers (e.g. get_parent_map) treat
    // unknown keys as absent, so a key that can't be parsed is just absent.
    // This matches the Python _KnitGraphIndex behaviour of passing keys
    // through to the underlying graph index which returns no match.
    let mut keys = Vec::new();
    for item in obj.try_iter()? {
        if let Ok(key) = extract_py_knit_key(&item?) {
            keys.push(key);
        }
    }
    Ok(keys)
}

fn py_knit_key_to_py<'py>(
    py: Python<'py>,
    key: &bazaar::knit::KnitKey,
) -> PyResult<Bound<'py, PyTuple>> {
    let parts: Vec<Bound<'py, PyBytes>> = key.iter().map(|s| PyBytes::new(py, s)).collect();
    PyTuple::new(py, parts)
}

fn knit_index_memo_to_py<'py>(
    py: Python<'py>,
    key: &bazaar::knit::KnitKey,
    det: &bazaar::knit::KnitRecordDetails,
) -> PyResult<Bound<'py, PyTuple>> {
    let py_key = py_knit_key_to_py(py, key)?;
    PyTuple::new(
        py,
        [
            py_key.into_any(),
            det.index_memo.offset.into_pyobject(py)?.into_any(),
            det.index_memo.length.into_pyobject(py)?.into_any(),
        ],
    )
}

/// Shared state for a batch of delta-closure records produced by one
/// `get_record_stream(include_delta_closure=True)` call.
///
/// All `PyKnitDeltaClosureRecord` objects in the same batch share this via
/// `Arc` so the raw record map and global map are fetched only once.
struct DeltaClosureState {
    /// Pre-fetched raw bytes map: key → (raw_bytes, method, noeol, next).
    raw_map: bazaar::knit::DeltaClosureRawMap,
    /// Parent map for all keys (including nonlocal): key → Option<parents>.
    global_map: std::collections::HashMap<KnitKey, Option<Vec<KnitKey>>>,
    /// Serialised wire bytes for the first record in this batch.  Computed
    /// once and cached; subsequent records return `b""`.
    wire_bytes: std::sync::OnceLock<Vec<u8>>,
    /// `emit_keys`: the locally-present keys in this batch (not nonlocal).
    emit_keys: Vec<KnitKey>,
    annotated: bool,
}

impl DeltaClosureState {
    fn wire_bytes(&self) -> &[u8] {
        self.wire_bytes.get_or_init(|| {
            bazaar::knit::build_delta_closure_wire_bytes(
                self.annotated,
                &self.emit_keys,
                &self.raw_map,
                &self.global_map,
            )
        })
    }
}

/// One record emitted by `get_record_stream(include_delta_closure=True)`.
///
/// Mirrors Python's `LazyKnitContentFactory`:
/// - `storage_kind = "knit-delta-closure"` for the first record in a batch
/// - `storage_kind = "knit-delta-closure-ref"` for subsequent records
/// - `get_bytes_as("knit-delta-closure")` → wire bytes (first) or `b""`
/// - `get_bytes_as("fulltext" / "lines" / "chunked")` → reconstructed text
#[pyclass(name = "KnitDeltaClosureRecord")]
struct PyKnitDeltaClosureRecord {
    inner_key: KnitKey,
    inner_parents: Option<Vec<KnitKey>>,
    first: bool,
    state: Arc<DeltaClosureState>,
}

#[pymethods]
impl PyKnitDeltaClosureRecord {
    #[getter]
    fn key<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        py_knit_key_to_py(py, &self.inner_key)
    }

    #[getter]
    fn parents<'py>(&self, py: Python<'py>) -> PyResult<Py<PyAny>> {
        match &self.inner_parents {
            None => Ok(py.None()),
            Some(parents) => {
                let tup = PyTuple::new(
                    py,
                    parents
                        .iter()
                        .map(|p| py_knit_key_to_py(py, p))
                        .collect::<PyResult<Vec<_>>>()?,
                )?;
                Ok(tup.into_any().unbind())
            }
        }
    }

    #[getter]
    fn storage_kind(&self) -> &str {
        if self.first {
            "knit-delta-closure"
        } else {
            "knit-delta-closure-ref"
        }
    }

    #[getter]
    fn sha1(&self, py: Python<'_>) -> Py<PyAny> {
        py.None()
    }

    /// Size of the content fulltext, or `None` when not known.
    ///
    /// Mirrors Python's `LazyKnitContentFactory.size`, which is always
    /// `None`; callers such as `groupcompress.insert_record_stream` fall
    /// back to summing the chunk lengths.
    #[getter]
    fn size(&self, py: Python<'_>) -> Py<PyAny> {
        py.None()
    }

    fn get_bytes_as<'py>(&self, py: Python<'py>, storage_kind: &str) -> PyResult<Py<PyAny>> {
        match storage_kind {
            "knit-delta-closure" => {
                if self.first {
                    Ok(PyBytes::new(py, self.state.wire_bytes())
                        .into_any()
                        .unbind())
                } else {
                    Ok(PyBytes::new(py, b"").into_any().unbind())
                }
            }
            "knit-delta-closure-ref" => Ok(PyBytes::new(py, b"").into_any().unbind()),
            "fulltext" | "lines" | "chunked" => {
                let lines = self.reconstruct_lines(py)?;
                let line_list = PyList::empty(py);
                for l in &lines {
                    line_list.append(PyBytes::new(py, l))?;
                }
                if storage_kind == "fulltext" {
                    let joined: Vec<u8> = lines.into_iter().flatten().collect();
                    Ok(PyBytes::new(py, &joined).into_any().unbind())
                } else {
                    Ok(line_list.into_any().unbind())
                }
            }
            _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
                "UnavailableRepresentation: {storage_kind} not available"
            ))),
        }
    }

    fn iter_bytes_as<'py>(&self, py: Python<'py>, storage_kind: &str) -> PyResult<Py<PyAny>> {
        let bytes = self.get_bytes_as(py, storage_kind)?;
        Ok(bytes.into_bound(py).call_method0("__iter__")?.unbind())
    }
}

impl PyKnitDeltaClosureRecord {
    fn reconstruct_lines(&self, py: Python<'_>) -> PyResult<Vec<Vec<u8>>> {
        let (lines, digest) = if self.state.annotated {
            bazaar::knit::reconstruct_text_from_raw_map(
                &bazaar::knit::KnitAnnotateFactory,
                &self.state.raw_map,
                &self.inner_key,
            )
        } else {
            bazaar::knit::reconstruct_text_from_raw_map(
                &bazaar::knit::KnitPlainFactory,
                &self.state.raw_map,
                &self.inner_key,
            )
        }
        .map_err(knit_err_to_py)?;
        use sha1::{Digest, Sha1};
        let mut hasher = Sha1::new();
        for line in &lines {
            hasher.update(line);
        }
        let actual: String = hasher
            .finalize()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let actual_bytes = actual.as_bytes();
        if actual_bytes != digest.as_slice() {
            let key_tuple = py_knit_key_to_py(py, &self.inner_key)?.unbind();
            let lines_py = PyList::empty(py);
            for l in &lines {
                lines_py.append(PyBytes::new(py, l))?;
            }
            return Err(SHA1KnitCorrupt::new_err((
                "".to_string(),
                PyBytes::new(py, actual_bytes).unbind(),
                PyBytes::new(py, &digest).unbind(),
                key_tuple,
                lines_py.unbind(),
            )));
        }
        Ok(lines)
    }
}

/// Rust-backed equivalent of Python's `KnitContentFactory`.
///
/// Emitted by `get_record_stream(include_delta_closure=False)`.  Holds the raw
/// gzip-compressed bytes for one knit record.  `get_bytes_as` is implemented
/// entirely in Rust, removing the Python adapter-registry indirection.
#[pyclass(name = "KnitContentFactory")]
struct PyKnitContentFactory {
    inner_key: KnitKey,
    inner_parents: Option<Vec<KnitKey>>,
    /// `("line-delta" | "fulltext", noeol)` — mirrors `_build_details`.
    method: KnitMethod,
    noeol: bool,
    inner_sha1: Option<Vec<u8>>,
    raw_record: Vec<u8>,
    annotated: bool,
}

#[pymethods]
impl PyKnitContentFactory {
    #[getter]
    fn key<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        py_knit_key_to_py(py, &self.inner_key)
    }

    #[getter]
    fn parents<'py>(&self, py: Python<'py>) -> PyResult<Py<PyAny>> {
        match &self.inner_parents {
            None => Ok(py.None()),
            Some(parents) => Ok(PyTuple::new(
                py,
                parents
                    .iter()
                    .map(|p| py_knit_key_to_py(py, p))
                    .collect::<PyResult<Vec<_>>>()?,
            )?
            .into_any()
            .unbind()),
        }
    }

    #[getter]
    fn storage_kind(&self) -> String {
        bazaar::knit::format_storage_kind(self.method.clone(), self.annotated)
    }

    #[getter]
    fn sha1<'py>(&self, py: Python<'py>) -> Py<PyAny> {
        match &self.inner_sha1 {
            None => py.None(),
            Some(s) => PyBytes::new(py, s).into_any().unbind(),
        }
    }

    #[getter]
    fn _raw_record<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.raw_record)
    }

    #[getter]
    fn size<'py>(&self, py: Python<'py>) -> Py<PyAny> {
        py.None()
    }

    #[getter]
    fn _build_details<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyTuple>> {
        PyTuple::new(
            py,
            [
                pyo3::types::PyString::new(py, self.method.as_str()).as_any(),
                pyo3::types::PyBool::new(py, self.noeol).as_any(),
            ],
        )
    }

    fn get_bytes_as<'py>(&self, py: Python<'py>, storage_kind: &str) -> PyResult<Py<PyAny>> {
        let my_kind = self.storage_kind();
        if storage_kind == my_kind.as_str() {
            // Return network bytes.
            let network = build_network_record_bytes(py, self)?;
            return Ok(network.into_any().unbind());
        }
        // Fulltext/lines/chunked from a fulltext raw record.
        if self.method == KnitMethod::Fulltext
            && matches!(storage_kind, "fulltext" | "lines" | "chunked")
        {
            let lines = self.decompress_to_lines()?;
            if storage_kind == "fulltext" {
                let joined: Vec<u8> = lines.into_iter().flatten().collect();
                return Ok(PyBytes::new(py, &joined).into_any().unbind());
            } else {
                let lst = PyList::empty(py);
                for l in &lines {
                    lst.append(PyBytes::new(py, l))?;
                }
                return Ok(lst.into_any().unbind());
            }
        }
        let exc_cls = py
            .import("bzrformats.versionedfile")?
            .getattr("UnavailableRepresentation")?;
        Err(PyErr::from_value(exc_cls.call1((
            self.key(py)?,
            storage_kind,
            my_kind.as_str(),
        ))?))
    }

    fn iter_bytes_as<'py>(&self, py: Python<'py>, storage_kind: &str) -> PyResult<Py<PyAny>> {
        let bytes = self.get_bytes_as(py, storage_kind)?;
        Ok(bytes.into_bound(py).call_method0("__iter__")?.unbind())
    }
}

impl PyKnitContentFactory {
    fn decompress_to_lines(&self) -> PyResult<Vec<Vec<u8>>> {
        let version_id = self.inner_key.last().cloned().unwrap_or_default();
        let (body_lines, _digest) =
            bazaar::knit::parse_record(&version_id, &self.raw_record).map_err(knit_err_to_py)?;
        // Strip annotation prefix for annotated records — `lines`/`chunked`/
        // `fulltext` callers expect plain text.
        let mut lines = if self.annotated {
            use bazaar::knit::KnitFactory as _;
            bazaar::knit::KnitAnnotateFactory
                .fulltext_payload_lines(&body_lines)
                .map_err(knit_err_to_py)?
        } else {
            body_lines
        };
        // Apply the record's noeol flag: drop the trailing '\n' that lower_fulltext
        // adds to stored lines, restoring the original (noeol) text.
        if self.noeol {
            if let Some(last) = lines.last_mut() {
                if last.ends_with(b"\n") {
                    last.pop();
                }
            }
        }
        Ok(lines)
    }
}

/// `LazyKnitContentFactory` — the record yielded by a delta-closure
/// `get_record_stream` iteration.
///
/// Holds a back-reference to its `generator` (`_NetworkContentMapGenerator`
/// or `_VFContentMapGenerator`); `get_bytes_as` dispatches the actual
/// reconstruction to the generator's `_get_one_work`. The first record in
/// the stream serialises the whole closure as wire bytes; subsequent
/// records emit an empty `knit-delta-closure-ref` payload.
#[pyclass(name = "LazyKnitContentFactory")]
pub struct PyLazyKnitContentFactory {
    #[pyo3(get)]
    key: Py<PyAny>,
    #[pyo3(get)]
    parents: Py<PyAny>,
    #[pyo3(get)]
    sha1: Py<PyAny>,
    #[pyo3(get)]
    storage_kind: String,
    generator: Py<PyAny>,
    first: bool,
}

#[pymethods]
impl PyLazyKnitContentFactory {
    #[new]
    fn new(
        key: Bound<'_, PyAny>,
        parents: Bound<'_, PyAny>,
        generator: Bound<'_, PyAny>,
        first: bool,
    ) -> PyResult<Self> {
        let py = key.py();
        Ok(Self {
            key: key.unbind(),
            parents: parents.unbind(),
            sha1: py.None(),
            storage_kind: bazaar::knit::delta_closure_storage_kind(first).to_owned(),
            generator: generator.unbind(),
            first,
        })
    }

    fn get_bytes_as<'py>(
        &self,
        py: Python<'py>,
        storage_kind: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        if storage_kind == self.storage_kind {
            if self.first {
                return self.generator.bind(py).call_method0("_wire_bytes");
            }
            return Ok(PyBytes::new(py, b"").into_any());
        }
        if matches!(storage_kind, "chunked" | "fulltext" | "lines") {
            let work = self
                .generator
                .bind(py)
                .call_method1("_get_one_work", (self.key.bind(py),))?;
            let chunks = work.call_method0("text")?;
            let lines: Vec<Vec<u8>> = chunks
                .try_iter()?
                .map(|item| {
                    item?
                        .cast_into::<PyBytes>()
                        .map(|b| b.as_bytes().to_vec())
                        .map_err(|_| pyo3::exceptions::PyTypeError::new_err("expected bytes"))
                })
                .collect::<PyResult<_>>()?;
            if matches!(storage_kind, "chunked" | "lines") {
                let py_lines: Vec<Bound<'py, PyBytes>> =
                    lines.iter().map(|l| PyBytes::new(py, l)).collect();
                return Ok(pyo3::types::PyList::new(py, &py_lines)?.into_any());
            }
            return Ok(PyBytes::new(py, &lines.concat()).into_any());
        }
        Err(UnavailableRepresentation::new_err((
            self.key.clone_ref(py),
            storage_kind.to_owned(),
            self.storage_kind.clone(),
        )))
    }

    fn iter_bytes_as<'py>(
        &self,
        py: Python<'py>,
        storage_kind: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        if matches!(storage_kind, "chunked" | "lines") {
            let work = self
                .generator
                .bind(py)
                .call_method1("_get_one_work", (self.key.bind(py),))?;
            return work.call_method0("text");
        }
        Err(UnavailableRepresentation::new_err((
            self.key.clone_ref(py),
            storage_kind.to_owned(),
            self.storage_kind.clone(),
        )))
    }
}

fn build_network_record_bytes<'py>(
    py: Python<'py>,
    rec: &PyKnitContentFactory,
) -> PyResult<Bound<'py, PyBytes>> {
    let storage_kind = rec.storage_kind();
    let parents_list: Option<Vec<Vec<Vec<u8>>>> = rec.inner_parents.clone();
    let out = bazaar::knit::build_network_record(
        storage_kind.as_bytes(),
        &rec.inner_key,
        parents_list.as_deref(),
        rec.noeol,
        &rec.raw_record,
    );
    Ok(PyBytes::new(py, &out))
}

/// Lazy iterator over knit records produced by `_read_records_iter`.
///
/// Holds the pre-fetched raw bytes for each `(key, index_memo)` pair and
/// parses one record per `__next__` call. Parse errors surface to the caller
/// when iterating, mirroring the Python generator semantics.
#[pyclass]
struct KnitReadRecordsIter {
    items: std::vec::IntoIter<(Py<PyAny>, Py<PyBytes>)>,
}

#[pymethods]
impl KnitReadRecordsIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let Some((key, raw)) = self.items.next() else {
            return Ok(None);
        };
        let key_b = key.bind(py);
        let raw_b = raw.bind(py);
        let version_id = key_b
            .get_item(-1_isize)?
            .cast_into::<PyBytes>()
            .map_err(|_| PyValueError::new_err("key segments must be bytes"))?;
        let (body_lines, digest) =
            bazaar::knit::parse_record(version_id.as_bytes(), raw_b.as_bytes())
                .map_err(knit_err_to_py)?;
        let content = PyList::empty(py);
        for line in &body_lines {
            content.append(PyBytes::new(py, line))?;
        }
        let py_digest = PyBytes::new(py, &digest);
        Ok(Some(
            PyTuple::new(py, [key_b, &content.into_any(), py_digest.as_any()])?
                .into_any()
                .unbind(),
        ))
    }
}

/// Lazy iterator backing `_read_records_iter_raw`: parses each record's
/// header (sha1 digest) on demand and yields `(key, raw_bytes, digest)`.
#[pyclass]
struct KnitReadRecordsIterRaw {
    items: std::vec::IntoIter<(Py<PyAny>, Py<PyBytes>)>,
}

#[pymethods]
impl KnitReadRecordsIterRaw {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let Some((key, raw)) = self.items.next() else {
            return Ok(None);
        };
        let key_b = key.bind(py);
        let raw_b = raw.bind(py);
        let header =
            bazaar::knit::parse_record_header_only(raw_b.as_bytes()).map_err(knit_err_to_py)?;
        let expected = key_b
            .get_item(-1_isize)?
            .cast_into::<PyBytes>()
            .map_err(|_| PyValueError::new_err("key segments must be bytes"))?;
        if header.version_id != expected.as_bytes() {
            return Err(knit_err_to_py(bazaar::knit::KnitError::UnexpectedVersion {
                wanted: expected.as_bytes().to_vec(),
                got: header.version_id.clone(),
            }));
        }
        let digest = PyBytes::new(py, &header.digest);
        Ok(Some(
            PyTuple::new(py, [key_b, raw_b.as_any(), digest.as_any()])?
                .into_any()
                .unbind(),
        ))
    }
}

/// Lazy iterator backing `_read_records_iter_unchecked`: yields `(key, raw_bytes)`.
#[pyclass]
struct KnitReadRecordsIterUnchecked {
    items: std::vec::IntoIter<(Py<PyAny>, Py<PyAny>)>,
}

#[pymethods]
impl KnitReadRecordsIterUnchecked {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let Some((key, raw)) = self.items.next() else {
            return Ok(None);
        };
        Ok(Some(
            PyTuple::new(py, [key.bind(py), raw.bind(py)])?
                .into_any()
                .unbind(),
        ))
    }
}

/// One unit of work in a lazy `get_record_stream`.
enum StreamItem {
    /// An absent key — emit an `AbsentContentFactory`.
    Absent(KnitKey),
    /// A run of locally-present keys, in stream order, fetched from the
    /// local access on demand.
    Local(Vec<KnitKey>),
    /// A run of keys owned by `immediate_fallback_vfs[idx - 1]`,
    /// delegated to that fallback's `get_record_stream`.
    Fallback { src_idx: usize, keys: Vec<KnitKey> },
}

/// Lazy iterator backing `get_record_stream` for the non-delta-closure
/// path.
///
/// Records are fetched one at a time so a pack reload (`RetryWithNewPacks`)
/// only happens when the stream actually reaches the affected pack,
/// matching the streaming semantics callers rely on. On a reload the
/// remaining keys of the current local group are re-fetched with fresh
/// build details, since a reload invalidates the previous `index_memo`s.
#[pyclass]
struct KnitRecordStreamLazy {
    vf: Py<PyKnitVersionedFiles>,
    annotated: bool,
    /// Ordering passed through to fallback `get_record_stream` calls.
    effective_ordering: String,
    /// Stream order: absent keys first, then source-grouped present keys.
    items: std::collections::VecDeque<StreamItem>,
    /// Parents for every present key (`None` for parentless).
    global_map: std::collections::HashMap<KnitKey, Option<Vec<KnitKey>>>,
    /// State for the local group currently being drained, if any.
    local: Option<LocalGroupState>,
    /// Records buffered from a fallback's stream, drained before the
    /// next item is started.
    fallback_buffer: std::collections::VecDeque<Py<PyAny>>,
}

/// In-progress drain of one `StreamItem::Local` group.
struct LocalGroupState {
    /// Keys of this group not yet emitted, in order.
    remaining: Vec<KnitKey>,
    /// How many of `remaining` have been emitted.
    emitted: usize,
    /// Build details for the keys of this group.
    positions: std::collections::HashMap<KnitKey, KnitRecordDetails<PyFileRef>>,
    /// Live Python `get_raw_records` generator for `remaining[emitted..]`.
    raw_iter: Py<PyAny>,
}

#[pymethods]
impl KnitRecordStreamLazy {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        loop {
            // Drain any records buffered from a fallback stream first.
            if let Some(rec) = self.fallback_buffer.pop_front() {
                return Ok(Some(rec));
            }
            // Continue draining the current local group, if any.
            if self.local.is_some() {
                if let Some(rec) = self.next_local_record(py)? {
                    return Ok(Some(rec));
                }
                // Group exhausted.
                self.local = None;
            }
            // Start the next item.
            let Some(item) = self.items.pop_front() else {
                return Ok(None);
            };
            match item {
                StreamItem::Absent(key) => {
                    let absent_cls = py
                        .import("bzrformats._bzr_rs.versionedfile")?
                        .getattr("AbsentContentFactory")?;
                    return Ok(Some(
                        absent_cls.call1((py_knit_key_to_py(py, &key)?,))?.unbind(),
                    ));
                }
                StreamItem::Fallback { src_idx, keys } => {
                    let vf = self.vf.bind(py).borrow();
                    let fb = vf.immediate_fallback_vfs[src_idx - 1].bind(py);
                    let fb_keys = PyList::empty(py);
                    for k in &keys {
                        fb_keys.append(py_knit_key_to_py(py, k)?)?;
                    }
                    let fb_stream = fb.call_method1(
                        "get_record_stream",
                        (fb_keys, self.effective_ordering.as_str(), false),
                    )?;
                    for rec in fb_stream.try_iter()? {
                        self.fallback_buffer.push_back(rec?.unbind());
                    }
                }
                StreamItem::Local(keys) => {
                    self.local = Some(self.start_local_group(py, keys)?);
                }
            }
        }
    }
}

impl KnitRecordStreamLazy {
    /// Fetch build details for `keys` and open a lazy `get_raw_records`
    /// generator, retrying once across a pack reload if needed.
    fn start_local_group(&self, py: Python<'_>, keys: Vec<KnitKey>) -> PyResult<LocalGroupState> {
        let (positions, raw_iter) = self.fetch_local(py, &keys)?;
        Ok(LocalGroupState {
            remaining: keys,
            emitted: 0,
            positions,
            raw_iter,
        })
    }

    /// Build details + a fresh `get_raw_records` generator for `keys`.
    fn fetch_local(
        &self,
        py: Python<'_>,
        keys: &[KnitKey],
    ) -> PyResult<(
        std::collections::HashMap<KnitKey, KnitRecordDetails<PyFileRef>>,
        Py<PyAny>,
    )> {
        let vf = self.vf.bind(py).borrow();
        let index = PyKnitIndex::new(vf.index_obj.bind(py).clone());
        let positions = index.get_build_details(keys).map_err(knit_err_to_py)?;
        let memos = PyList::empty(py);
        for k in keys {
            let memo = &positions
                .get(k)
                .ok_or_else(|| {
                    knit_err_to_py(bazaar::knit::KnitError::RevisionNotPresent(k.clone()))
                })?
                .index_memo;
            memos.append(PyTuple::new(
                py,
                [
                    memo.file_ref.0.clone_ref(py).into_bound(py),
                    memo.offset.into_pyobject(py)?.into_any(),
                    memo.length.into_pyobject(py)?.into_any(),
                ],
            )?)?;
        }
        let raw_iter = vf
            .access_obj
            .bind(py)
            .call_method1("get_raw_records", (memos,))?
            .call_method0("__iter__")?
            .unbind();
        Ok((positions, raw_iter))
    }

    /// Emit the next record of the current local group, or `None` when
    /// the group is fully drained. Handles `RetryWithNewPacks` by
    /// reloading and re-fetching the still-undelivered keys.
    fn next_local_record(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        loop {
            let state = self.local.as_mut().unwrap();
            if state.emitted >= state.remaining.len() {
                return Ok(None);
            }
            let raw_next = state.raw_iter.bind(py).call_method0("__next__");
            match raw_next {
                Ok(raw_obj) => {
                    let key = state.remaining[state.emitted].clone();
                    let details = state.positions.get(&key).ok_or_else(|| {
                        knit_err_to_py(bazaar::knit::KnitError::RevisionNotPresent(key.clone()))
                    })?;
                    let raw_bytes: Vec<u8> = raw_obj
                        .cast_into::<PyBytes>()
                        .map_err(|_| PyValueError::new_err("get_raw_records yielded non-bytes"))?
                        .as_bytes()
                        .to_vec();
                    let factory = PyKnitContentFactory {
                        inner_key: key.clone(),
                        inner_parents: self.global_map.get(&key).cloned().flatten(),
                        method: details.method,
                        noeol: details.noeol,
                        inner_sha1: None,
                        raw_record: raw_bytes,
                        annotated: self.annotated,
                    };
                    state.emitted += 1;
                    return Ok(Some(factory.into_pyobject(py)?.into_any().unbind()));
                }
                Err(err) if err.is_instance_of::<pyo3::exceptions::PyStopIteration>(py) => {
                    return Ok(None);
                }
                Err(err) if err.is_instance_of::<RetryWithNewPacks>(py) => {
                    // Reload, then re-fetch the not-yet-emitted keys.
                    let vf = self.vf.bind(py).borrow();
                    vf.access_obj
                        .bind(py)
                        .call_method1("reload_or_raise", (err.value(py),))?;
                    drop(vf);
                    let pending: Vec<KnitKey> = state.remaining[state.emitted..].to_vec();
                    let (positions, raw_iter) = self.fetch_local(py, &pending)?;
                    let state = self.local.as_mut().unwrap();
                    state.remaining = {
                        let mut r = state.remaining[..state.emitted].to_vec();
                        r.extend(pending);
                        r
                    };
                    state.positions = positions;
                    state.raw_iter = raw_iter;
                }
                Err(err) => return Err(err),
            }
        }
    }
}

/// Rust-backed implementation of Python's `KnitVersionedFiles`.
///
/// Wraps [`bazaar::knit::KnitVersionedFiles`] with [`PyKnitIndex`] and
/// [`PyKnitAccess`] adapters so pure-Rust logic (add_lines, get_text, get_sha1s,
/// check_should_delta, …) drives the Python index and access objects.
#[pyclass(name = "KnitVersionedFiles", subclass, dict)]
pub struct PyKnitVersionedFiles {
    /// Held so Python callers can read `._index` / `._access`.
    index_obj: Py<PyAny>,
    access_obj: Py<PyAny>,
    /// Whether the factory is annotated (True) or plain (False).
    annotated: bool,
    max_delta_chain: usize,
    reload_func: Py<PyAny>,
    immediate_fallback_vfs: Vec<Py<PyAny>>,
}

#[pymethods]
impl PyKnitVersionedFiles {
    #[new]
    #[pyo3(signature = (index, data_access, max_delta_chain=200, annotated=false, reload_func=None))]
    fn new(
        py: Python<'_>,
        index: Bound<'_, PyAny>,
        data_access: Bound<'_, PyAny>,
        max_delta_chain: usize,
        annotated: bool,
        reload_func: Option<Bound<'_, PyAny>>,
    ) -> Self {
        Self {
            index_obj: index.unbind(),
            access_obj: data_access.unbind(),
            annotated,
            max_delta_chain,
            reload_func: reload_func.map(|f| f.unbind()).unwrap_or_else(|| py.None()),
            immediate_fallback_vfs: Vec::new(),
        }
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let index_repr = self.index_obj.bind(py).repr()?;
        let access_repr = self.access_obj.bind(py).repr()?;
        Ok(format!(
            "KnitVersionedFiles({}, {})",
            index_repr, access_repr
        ))
    }

    #[getter]
    fn _index(&self, py: Python<'_>) -> Py<PyAny> {
        self.index_obj.clone_ref(py)
    }

    #[getter]
    fn _access(&self, py: Python<'_>) -> Py<PyAny> {
        self.access_obj.clone_ref(py)
    }

    #[getter]
    fn _max_delta_chain(&self) -> usize {
        self.max_delta_chain
    }

    #[setter]
    fn set__max_delta_chain(&mut self, value: usize) {
        self.max_delta_chain = value;
    }

    #[getter]
    fn _immediate_fallback_vfs<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let list = PyList::empty(py);
        for vf in &self.immediate_fallback_vfs {
            list.append(vf.bind(py))?;
        }
        Ok(list)
    }

    fn without_fallbacks(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let result = PyKnitVersionedFiles {
            index_obj: self.index_obj.clone_ref(py),
            access_obj: self.access_obj.clone_ref(py),
            max_delta_chain: self.max_delta_chain,
            annotated: self.annotated,
            reload_func: self.reload_func.clone_ref(py),
            immediate_fallback_vfs: Vec::new(),
        };
        Py::new(py, result).map(|p| p.into_any())
    }

    fn add_fallback_versioned_files(&mut self, a_versioned_files: Bound<'_, PyAny>) {
        self.immediate_fallback_vfs.push(a_versioned_files.unbind());
    }

    #[pyo3(signature = (key, parents, lines, parent_texts=None, left_matching_blocks=None, nostore_sha=None, random_id=false, check_content=true))]
    fn add_lines(
        &self,
        py: Python<'_>,
        key: Bound<'_, PyAny>,
        parents: Option<Bound<'_, PyAny>>,
        lines: Bound<'_, PyAny>,
        parent_texts: Option<Bound<'_, PyAny>>,
        left_matching_blocks: Option<Bound<'_, PyAny>>,
        nostore_sha: Option<Bound<'_, PyAny>>,
        random_id: bool,
        check_content: bool,
    ) -> PyResult<Py<PyAny>> {
        // TODO: call check_write_ok through the Rust KnitIndex trait once
        // key parsing can happen after the lock check.
        self.index_obj.bind(py).call_method0("_check_write_ok")?;
        let (mut rust_key, autogen_key) = extract_py_knit_key_autogen(&key)?;
        let rust_lines: Vec<Vec<u8>> = lines
            .try_iter()?
            .map(|item| {
                item?
                    .cast_into::<PyBytes>()
                    .map(|b| b.as_bytes().to_vec())
                    .map_err(|_| PyValueError::new_err("lines must be an iterable of bytes"))
            })
            .collect::<PyResult<_>>()?;

        // Validate key and lines the same way the Python side does.
        if check_content {
            self.check_lines_not_unicode(py, &rust_lines)?;
            self.check_lines_are_lines(py, &rust_lines)?;
        }
        for (i, seg) in rust_key.iter().enumerate() {
            if autogen_key && i == rust_key.len() - 1 {
                continue; // placeholder, filled below
            }
            if seg.contains(&b' ') || seg.contains(&b'\t') || seg.contains(&b'\n') {
                return Err(PyValueError::new_err(format!(
                    "key element contains whitespace: {:?}",
                    seg
                )));
            }
        }

        let line_bytes: Vec<u8> = rust_lines.iter().flat_map(|l| l.iter().copied()).collect();
        let digest = bazaar::osutils::sha::sha_string(&line_bytes);
        let digest_bytes = digest.clone().into_bytes();

        if autogen_key {
            let last = rust_key.last_mut().unwrap();
            *last = [b"sha1:".as_ref(), digest_bytes.as_slice()].concat();
        }

        // Check nostore_sha.
        if let Some(ref ns) = nostore_sha {
            let ns_bytes: Vec<u8> = ns
                .cast::<PyBytes>()
                .map(|b| b.as_bytes().to_vec())
                .unwrap_or_default();
            if ns_bytes == digest_bytes {
                pyo3::import_exception!(bzrformats.versionedfile, ExistingContent);
                return Err(ExistingContent::new_err(()));
            }
        }

        let rust_parents: Vec<Vec<Vec<u8>>> = match parents {
            None => Vec::new(),
            Some(ref p) => {
                if p.is_none() {
                    Vec::new()
                } else {
                    p.try_iter()?
                        .map(|item| extract_py_knit_key(&item?))
                        .collect::<PyResult<_>>()?
                }
            }
        };

        let index = PyKnitIndex::new(self.index_obj.bind(py).clone());
        let access = PyKnitAccess::new(self.access_obj.bind(py).clone());

        let kvf = if self.annotated {
            bazaar::knit::KnitVersionedFiles::new(
                index,
                access,
                bazaar::knit::KnitAnnotateFactory,
                self.max_delta_chain,
            )
        } else {
            // Need the same type; use a newtype trick via trait objects or
            // just call the Rust KVF directly for the plain case.
            // Since KnitVersionedFiles is generic, we can't store both in one
            // field.  Call the common pipeline directly instead.
            return self.add_lines_plain(
                py,
                rust_key,
                rust_parents,
                rust_lines,
                digest_bytes,
                random_id,
            );
        };

        let (ret_digest, text_length) = kvf
            .add_lines(rust_key, rust_parents, rust_lines, random_id)
            .map_err(knit_err_to_py)?;

        let result = PyTuple::new(
            py,
            [
                PyBytes::new(py, &ret_digest).into_any(),
                text_length.into_pyobject(py)?.into_any(),
                py.None().into_bound(py),
            ],
        )?;
        Ok(result.into_any().unbind())
    }

    fn get_parent_map<'py>(
        &self,
        py: Python<'py>,
        keys: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let rust_keys = extract_py_knit_keys(&keys)?;
        let index = PyKnitIndex::new(self.index_obj.bind(py).clone());

        let has_graph = index.has_graph();
        let local_map = index.get_parent_map(&rust_keys).map_err(knit_err_to_py)?;
        let result = PyDict::new(py);
        for (key, parents) in &local_map {
            let py_key = py_knit_key_to_py(py, key)?;
            if has_graph {
                let py_parents = PyTuple::new(
                    py,
                    parents
                        .iter()
                        .map(|p| py_knit_key_to_py(py, p))
                        .collect::<PyResult<Vec<_>>>()?,
                )?;
                result.set_item(py_key, py_parents)?;
            } else {
                result.set_item(py_key, py.None())?;
            }
        }

        // Consult fallback VFs for any missing keys.
        let mut missing: std::collections::HashSet<Vec<Vec<u8>>> = rust_keys.into_iter().collect();
        for key in local_map.keys() {
            missing.remove(key);
        }
        for fallback in &self.immediate_fallback_vfs {
            if missing.is_empty() {
                break;
            }
            let fb_keys = pyo3::types::PySet::empty(py)?;
            for k in &missing {
                fb_keys.add(py_knit_key_to_py(py, k)?)?;
            }
            let fb_result = fallback
                .bind(py)
                .call_method1("get_parent_map", (fb_keys,))?;
            let fb_dict = fb_result.cast_into::<PyDict>()?;
            for (k, v) in fb_dict.iter() {
                let rust_key = extract_py_knit_key(&k)?;
                missing.remove(&rust_key);
                result.set_item(k, v)?;
            }
        }
        Ok(result)
    }

    fn get_sha1s<'py>(
        &self,
        py: Python<'py>,
        keys: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let rust_keys = extract_py_knit_keys(&keys)?;
        let index = PyKnitIndex::new(self.index_obj.bind(py).clone());
        let access = PyKnitAccess::new(self.access_obj.bind(py).clone());

        let local_result = rust_get_sha1s(&index, &access, &rust_keys).map_err(knit_err_to_py)?;
        let result = PyDict::new(py);
        for (key, digest) in &local_result {
            result.set_item(py_knit_key_to_py(py, key)?, PyBytes::new(py, digest))?;
        }

        let mut missing: std::collections::HashSet<Vec<Vec<u8>>> = rust_keys.into_iter().collect();
        for k in local_result.keys() {
            missing.remove(k);
        }
        for fallback in &self.immediate_fallback_vfs {
            if missing.is_empty() {
                break;
            }
            let fb_keys = pyo3::types::PySet::empty(py)?;
            for k in &missing {
                fb_keys.add(py_knit_key_to_py(py, k)?)?;
            }
            let fb_result = fallback.bind(py).call_method1("get_sha1s", (fb_keys,))?;
            let fb_dict = fb_result.cast_into::<PyDict>()?;
            for (k, v) in fb_dict.iter() {
                let rust_key = extract_py_knit_key(&k)?;
                missing.remove(&rust_key);
                result.set_item(k, v)?;
            }
        }
        Ok(result)
    }

    fn keys<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        // Call Python index.keys() directly so Python exceptions (KnitHeaderError
        // etc.) propagate unchanged rather than being wrapped in ValueError.
        let local_keys = self.index_obj.bind(py).call_method0("keys")?;
        let result = pyo3::types::PySet::empty(py)?;
        for k in local_keys.try_iter()? {
            result.add(k?)?;
        }
        for fallback in &self.immediate_fallback_vfs {
            let fb_keys = fallback.bind(py).call_method0("keys")?;
            for k in fb_keys.try_iter()? {
                result.add(k?)?;
            }
        }
        Ok(result.into_any())
    }

    #[pyo3(signature = (progress_bar=None, keys=None))]
    fn check(
        slf: Py<Self>,
        py: Python<'_>,
        progress_bar: Option<Bound<'_, PyAny>>,
        keys: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let _ = progress_bar;
        if let Some(k) = keys {
            // check(keys=...) is just get_record_stream(keys, "unordered", True)
            let ordering = pyo3::intern!(py, "unordered").clone().into_any();
            let idc = pyo3::types::PyBool::new(py, true).to_owned().into_any();
            return PyKnitVersionedFiles::get_record_stream(slf, py, k, ordering, idc);
        }
        let this_ref = slf.bind(py);
        let this = this_ref.borrow();
        // _logical_check: verify all delta keys have their compression parent present.
        let index = PyKnitIndex::new(this.index_obj.bind(py).clone());
        let all_keys = index.keys().map_err(knit_err_to_py)?;
        let py_keys = PyList::empty(py);
        for k in &all_keys {
            py_keys.append(py_knit_key_to_py(py, k)?)?;
        }
        let parent_map_raw = this
            .index_obj
            .bind(py)
            .call_method1("get_parent_map", (py_keys.clone(),))?
            .cast_into::<PyDict>()?;
        for k in &all_keys {
            let method = index.get_method(k).map_err(knit_err_to_py)?;
            if method != bazaar::knit::KnitMethod::Fulltext {
                let py_key = py_knit_key_to_py(py, k)?;
                let parents_obj = parent_map_raw.get_item(&py_key)?.ok_or_else(|| {
                    pyo3::exceptions::PyKeyError::new_err(format!("{k:?} not in parent_map"))
                })?;
                let parents_tup = parents_obj.cast_into::<PyTuple>()?;
                if parents_tup.is_empty() {
                    continue;
                }
                let compression_parent = parents_tup.get_item(0)?;
                if parent_map_raw.get_item(&compression_parent)?.is_none() {
                    return Err(KnitCorrupt::new_err((
                        py.None(),
                        format!(
                            "Missing basis parent {:?} for {:?}",
                            compression_parent, py_key
                        ),
                    )));
                }
            }
        }
        // Check fallback VFs.
        for fallback in &this.immediate_fallback_vfs {
            fallback.bind(py).call_method0("check")?;
        }
        Ok(py.None())
    }

    fn get_missing_compression_parent_keys(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let index = PyKnitIndex::new(self.index_obj.bind(py).clone());
        let missing = index
            .get_missing_compression_parents()
            .map_err(knit_err_to_py)?;
        let result = pyo3::types::PySet::empty(py)?;
        for k in &missing {
            result.add(py_knit_key_to_py(py, k)?)?;
        }
        Ok(result.into_any().unbind())
    }

    fn annotate<'py>(
        slf: Py<Self>,
        py: Python<'py>,
        key: Bound<'py, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let factory = slf.bind(py).borrow()._factory(py)?;
        factory
            .bind(py)
            .call_method1("annotate", (slf, key))
            .map(|b| b.unbind())
    }

    fn get_annotator(slf: Py<Self>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let vf_obj = slf.clone_ref(py).into_any();
        let mut annotator = PyKnitAnnotator::from_kvf(py, &slf.bind(py).borrow())?;
        annotator.vf = vf_obj;
        Py::new(py, annotator).map(|p| p.into_any())
    }

    fn insert_record_stream(
        slf: Py<Self>,
        py: Python<'_>,
        stream: Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let this_ref = slf.bind(py);
        let this = this_ref.borrow();

        let annotated = this.annotated;
        let has_delta = this.max_delta_chain > 0;
        let has_fallbacks = !this.immediate_fallback_vfs.is_empty();
        let max_delta_chain = this.max_delta_chain;

        // Build the type sets matching Python's insert_record_stream logic.
        let annotated_prefix = if annotated { "annotated-" } else { "" };
        let mut native_types: std::collections::HashSet<&str> = std::collections::HashSet::new();
        let ft_native = format!("knit-{annotated_prefix}ft-gz");
        let delta_native = format!("knit-{annotated_prefix}delta-gz");
        native_types.insert(&ft_native);
        if has_delta {
            native_types.insert(&delta_native);
        }
        let convertible_annotated_ft = "knit-annotated-ft-gz";
        let convertible_annotated_delta = "knit-annotated-delta-gz";
        let mut convertible_types: std::collections::HashSet<&str> =
            std::collections::HashSet::new();
        if !annotated {
            convertible_types.insert(convertible_annotated_ft);
            if has_delta {
                convertible_types.insert(convertible_annotated_delta);
            }
        }

        let index = PyKnitIndex::new(this.index_obj.bind(py).clone());
        let index_obj = this.index_obj.clone_ref(py);
        let access = PyKnitAccess::new(this.access_obj.bind(py).clone());
        drop(this);

        // Lazy iterator: pull one record at a time from the Python stream and
        // convert it into a `KnitStreamRecord`. Yielding lazily lets bazaar's
        // insert loop commit each record (or buffer it) before we materialise
        // the next — so a later delta record can fetch its basis from `slf`.
        let stream_py: Py<PyAny> = stream.try_iter()?.into_any().unbind();
        let slf_for_iter = slf.clone_ref(py);
        let native_types_owned: std::collections::HashSet<String> =
            native_types.iter().map(|s| s.to_string()).collect();
        let convertible_types_owned: std::collections::HashSet<String> =
            convertible_types.iter().map(|s| s.to_string()).collect();
        // Use a slot for the last PyErr — KnitError can't carry it directly,
        // so we stash it here, return a placeholder error, and re-raise after
        // the bazaar layer hands control back.
        let py_err_slot: Rc<RefCell<Option<PyErr>>> = Rc::new(RefCell::new(None));
        let py_err_slot_iter = py_err_slot.clone();
        let record_iter = std::iter::from_fn(move || {
            Python::attach(|py| {
                let stream_b = stream_py.bind(py);
                let next_item = stream_b.call_method0("__next__");
                let record = match next_item {
                    Ok(r) => r,
                    Err(e) if e.is_instance_of::<pyo3::exceptions::PyStopIteration>(py) => {
                        return None;
                    }
                    Err(e) => {
                        *py_err_slot_iter.borrow_mut() = Some(e);
                        return Some(Err(bazaar::knit::KnitError::Corrupt(
                            "py stream error".to_string(),
                        )));
                    }
                };
                Some(
                    convert_stream_record(
                        py,
                        &record,
                        &native_types_owned,
                        &convertible_types_owned,
                        has_fallbacks,
                        index_obj.bind(py),
                        slf_for_iter.bind(py),
                    )
                    .map_err(|e| {
                        *py_err_slot_iter.borrow_mut() = Some(e);
                        bazaar::knit::KnitError::Corrupt("py conversion error".to_string())
                    }),
                )
            })
        });

        let result = if annotated {
            let kvf = bazaar::knit::KnitVersionedFiles::new(
                index,
                access,
                bazaar::knit::KnitAnnotateFactory,
                max_delta_chain,
            );
            kvf.insert_record_stream(record_iter)
        } else {
            let kvf = bazaar::knit::KnitVersionedFiles::new(
                index,
                access,
                bazaar::knit::KnitPlainFactory,
                max_delta_chain,
            );
            kvf.insert_record_stream(record_iter)
        };

        if let Some(py_err) = py_err_slot.borrow_mut().take() {
            return Err(py_err);
        }
        result.map_err(knit_err_to_py)?;
        Ok(())
    }

    fn get_record_stream(
        slf: Py<Self>,
        py: Python<'_>,
        keys: Bound<'_, PyAny>,
        ordering: Bound<'_, PyAny>,
        include_delta_closure: Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let this_ref = slf.bind(py);
        let this = this_ref.borrow();
        let include_delta_closure: bool = include_delta_closure.extract()?;
        let ordering: String = ordering.extract()?;

        let key_set = pyo3::types::PySet::empty(py)?;
        for k in keys.try_iter()? {
            key_set.add(k?)?;
        }
        if key_set.is_empty() {
            return Ok(PyList::empty(py).try_iter()?.into_any().unbind());
        }

        let has_graph: bool = this.index_obj.bind(py).getattr("has_graph")?.extract()?;
        let effective_ordering = if !has_graph {
            "unordered".to_string()
        } else {
            ordering.clone()
        };

        if include_delta_closure {
            // Delta-closure path. Mirrors KnitVersionedFiles._get_remaining_record_stream:
            //   1. Walk local positions + global parent map.
            //   2. Order keys topologically (or remote-first for unordered) and
            //      group by their owning source.
            //   3. For each group: locally, drive the existing
            //      _group_keys_for_io / _get_record_map_unparsed pipeline; for
            //      a fallback, delegate to its get_record_stream.
            let positions = this
                ._get_components_positions(py, key_set.clone().into_any(), Some(true))?
                .into_bound(py)
                .cast_into::<PyDict>()?;

            let global_map_tup = this
                ._get_parent_map_with_sources(py, key_set.clone().into_any())?
                .into_bound(py)
                .cast_into::<PyTuple>()?;
            let global_map_py = global_map_tup.get_item(0)?.cast_into::<PyDict>()?;
            let parent_maps = global_map_tup.get_item(1)?.cast_into::<PyList>()?;

            let mut global_map_rust: std::collections::HashMap<KnitKey, Option<Vec<KnitKey>>> =
                std::collections::HashMap::new();
            for (k, v) in global_map_py.iter() {
                let key = extract_knit_key(&k).map_err(knit_err_to_py)?;
                let parents: Option<Vec<KnitKey>> = if v.is_none() {
                    None
                } else {
                    Some(
                        v.try_iter()?
                            .map(|p| extract_knit_key(&p?).map_err(knit_err_to_py))
                            .collect::<PyResult<_>>()?,
                    )
                };
                global_map_rust.insert(key, parents);
            }

            let mut source_of: std::collections::HashMap<KnitKey, usize> =
                std::collections::HashMap::new();
            for (idx, src_obj) in parent_maps.iter().enumerate() {
                let src = src_obj.cast_into::<PyDict>()?;
                for k_obj in src.keys().iter() {
                    let k = extract_knit_key(&k_obj).map_err(knit_err_to_py)?;
                    source_of.entry(k).or_insert(idx);
                }
            }

            let present_keys: Vec<KnitKey> = match effective_ordering.as_str() {
                "topological" => {
                    let tsort_iter = global_map_rust
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone().unwrap_or_default()));
                    let mut sorter = vcs_graph::tsort::TopoSorter::new(tsort_iter);
                    sorter.sorted().map_err(|e| {
                        knit_err_to_py(bazaar::knit::KnitError::Corrupt(format!(
                            "topo_sort: {e:?}"
                        )))
                    })?
                }
                "unordered" => {
                    let mut out: Vec<KnitKey> = Vec::new();
                    for src_obj in parent_maps.iter().rev() {
                        let src = src_obj.cast_into::<PyDict>()?;
                        for k_obj in src.keys().iter() {
                            out.push(extract_knit_key(&k_obj).map_err(knit_err_to_py)?);
                        }
                    }
                    out
                }
                "groupcompress" => bazaar::groupcompress::sort::sort_gc_optimal(
                    global_map_rust
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone().unwrap_or_default()))
                        .collect(),
                ),
                other => {
                    return Err(PyValueError::new_err(format!(
                        "valid values for ordering are: \"unordered\", \"groupcompress\" or \"topological\" not: {other:?}"
                    )));
                }
            };

            let mut source_groups: Vec<(usize, Vec<KnitKey>)> = Vec::new();
            for key in &present_keys {
                let src_idx = source_of[key];
                if source_groups.last().map(|(s, _)| *s) != Some(src_idx) {
                    source_groups.push((src_idx, Vec::new()));
                }
                source_groups.last_mut().unwrap().1.push(key.clone());
            }

            let absent_set: std::collections::HashSet<KnitKey> = key_set
                .clone()
                .try_iter()?
                .map(|k| extract_knit_key(&k?).map_err(knit_err_to_py))
                .collect::<PyResult<std::collections::HashSet<_>>>()?
                .into_iter()
                .filter(|k| !global_map_rust.contains_key(k))
                .collect();

            let global_map_arc = Arc::new(global_map_rust.clone());

            let absent_cls = py
                .import("bzrformats._bzr_rs.versionedfile")?
                .getattr("AbsentContentFactory")?;
            let result_list = PyList::empty(py);
            for key in &absent_set {
                result_list.append(absent_cls.call1((py_knit_key_to_py(py, key)?,))?)?;
            }

            let annotated = this.annotated;
            for (src_idx, group) in source_groups {
                if group.is_empty() {
                    continue;
                }
                if src_idx == 0 {
                    let group_py = PyList::empty(py);
                    for k in &group {
                        group_py.append(py_knit_key_to_py(py, k)?)?;
                    }
                    for sub_keys in this
                        ._group_keys_for_io(
                            py,
                            group_py.into_any(),
                            pyo3::types::PySet::empty(py)?.into_any(),
                            positions.clone().into_any(),
                            None,
                        )?
                        .into_bound(py)
                        .try_iter()?
                    {
                        let sub_tup = sub_keys?.cast_into::<PyTuple>()?;
                        let chunk_keys_obj = sub_tup.get_item(0)?;
                        let nonlocal_obj = sub_tup.get_item(1)?;

                        let nonlocal_set: std::collections::HashSet<KnitKey> = nonlocal_obj
                            .try_iter()?
                            .map(|k| extract_knit_key(&k?).map_err(knit_err_to_py))
                            .collect::<PyResult<_>>()?;
                        let emit_keys: Vec<KnitKey> = chunk_keys_obj
                            .try_iter()?
                            .map(|k| extract_knit_key(&k?).map_err(knit_err_to_py))
                            .collect::<PyResult<Vec<_>>>()?
                            .into_iter()
                            .filter(|k| !nonlocal_set.contains(k))
                            .collect();

                        let raw_map_py = this
                            ._get_record_map_unparsed(py, chunk_keys_obj.clone(), true)?
                            .into_bound(py)
                            .cast_into::<PyDict>()?;

                        let mut raw_map = bazaar::knit::DeltaClosureRawMap::new();
                        for (k, v) in raw_map_py.iter() {
                            let key = extract_knit_key(&k).map_err(knit_err_to_py)?;
                            let tup = v.cast_into::<PyTuple>()?;
                            let raw_bytes: Vec<u8> = tup.get_item(0)?.extract()?;
                            let record_details = tup.get_item(1)?.cast_into::<PyTuple>()?;
                            let method_str: String = record_details.get_item(0)?.extract()?;
                            let noeol: bool = record_details.get_item(1)?.extract()?;
                            let next_obj = tup.get_item(2)?;
                            let next = if next_obj.is_none() {
                                None
                            } else {
                                Some(extract_knit_key(&next_obj).map_err(knit_err_to_py)?)
                            };
                            let method = match method_str.as_str() {
                                "line-delta" => bazaar::knit::KnitMethod::LineDelta,
                                _ => bazaar::knit::KnitMethod::Fulltext,
                            };
                            raw_map.insert(
                                key,
                                bazaar::knit::DeltaClosureRawEntry {
                                    raw_bytes,
                                    method,
                                    noeol,
                                    next,
                                },
                            );
                        }

                        let state = Arc::new(DeltaClosureState {
                            raw_map,
                            global_map: (*global_map_arc).clone(),
                            wire_bytes: std::sync::OnceLock::new(),
                            emit_keys: emit_keys.clone(),
                            annotated,
                        });

                        let mut first = true;
                        for key in &emit_keys {
                            let parents = global_map_arc.get(key).cloned().flatten();
                            let record = PyKnitDeltaClosureRecord {
                                inner_key: key.clone(),
                                inner_parents: parents,
                                first,
                                state: Arc::clone(&state),
                            };
                            result_list.append(record.into_pyobject(py)?)?;
                            first = false;
                        }
                    }
                } else {
                    let fb = this.immediate_fallback_vfs[src_idx - 1].bind(py);
                    let fb_keys = PyList::empty(py);
                    for k in &group {
                        fb_keys.append(py_knit_key_to_py(py, k)?)?;
                    }
                    let fb_stream = fb.call_method1(
                        "get_record_stream",
                        (fb_keys, effective_ordering.as_str(), true),
                    )?;
                    for item in fb_stream.try_iter()? {
                        let item = item?;
                        let storage_kind: String = item.getattr("storage_kind")?.extract()?;
                        if storage_kind == "absent" {
                            continue;
                        }
                        result_list.append(item)?;
                    }
                }
            }
            return Ok(result_list.try_iter()?.into_any().unbind());
        }

        // Non-delta-closure path. Plan computation (index reads) is
        // retried on a pack reload here; the actual record fetches are
        // streamed lazily by KnitRecordStreamLazy, which handles its own
        // reloads as the stream advances.
        let access_obj = this.access_obj.clone_ref(py);
        drop(this);
        retry_on_new_packs(py, &access_obj, || {
            Self::get_record_stream_local_once(slf.clone_ref(py), py, &key_set, &effective_ordering)
        })
    }

    #[pyo3(signature = (keys, pb=None))]
    fn iter_lines_added_or_present_in_keys(
        &self,
        py: Python<'_>,
        keys: Bound<'_, PyAny>,
        pb: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let knit_keys: Vec<KnitKey> = keys
            .try_iter()?
            .map(|item| extract_knit_key(&item?).map_err(knit_err_to_py))
            .collect::<PyResult<_>>()?;
        let index = PyKnitIndex::new(self.index_obj.bind(py).clone());
        let access = PyKnitAccess::new(self.access_obj.bind(py).clone());
        let pairs = if self.annotated {
            let kvf = bazaar::knit::KnitVersionedFiles::new(
                index,
                access,
                bazaar::knit::KnitAnnotateFactory,
                self.max_delta_chain,
            );
            kvf.iter_lines_added_or_present_in_keys(&knit_keys)
                .map_err(|e| read_err_to_py(&kvf.access, e))?
        } else {
            let kvf = bazaar::knit::KnitVersionedFiles::new(
                index,
                access,
                bazaar::knit::KnitPlainFactory,
                self.max_delta_chain,
            );
            kvf.iter_lines_added_or_present_in_keys(&knit_keys)
                .map_err(|e| read_err_to_py(&kvf.access, e))?
        };
        // Emit local results first.
        let out = PyList::empty(py);
        let mut remaining_keys: std::collections::HashSet<KnitKey> =
            knit_keys.into_iter().collect();
        for (line, key) in pairs {
            remaining_keys.remove(&key);
            let py_key = py_knit_key_to_py(py, &key)?;
            out.append(PyTuple::new(
                py,
                [PyBytes::new(py, &line).into_any(), py_key.into_any()],
            )?)?;
        }
        // Consult fallback VFs for any keys that were not found locally.
        for source in &self.immediate_fallback_vfs {
            if remaining_keys.is_empty() {
                break;
            }
            let source_keys = pyo3::types::PySet::empty(py)?;
            for k in &remaining_keys {
                source_keys.add(py_knit_key_to_py(py, k)?)?;
            }
            let fallback_iter = source
                .bind(py)
                .call_method1("iter_lines_added_or_present_in_keys", (source_keys,))?;
            for item in fallback_iter.try_iter()? {
                let tup = item?.cast_into::<PyTuple>()?;
                let key = tup.get_item(1)?;
                let rust_key = extract_knit_key(&key).map_err(knit_err_to_py)?;
                remaining_keys.remove(&rust_key);
                out.append(tup)?;
            }
        }
        let _ = pb; // progress bar not needed for eager collection
        Ok(out.try_iter()?.into_any().unbind())
    }

    fn make_mpdiffs(slf: Py<Self>, py: Python<'_>, keys: Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        // VersionedFiles.make_mpdiffs uses _MPDiffGenerator(self, keys).
        // Pass this PyKnitVersionedFiles directly since it exposes get_record_stream.
        let vf_m = py.import("bzrformats.versionedfile")?;
        let generator = vf_m.call_method1("_MPDiffGenerator", (slf, keys))?;
        generator.call_method0("compute_diffs").map(|b| b.unbind())
    }

    fn add_mpdiffs(slf: Py<Self>, py: Python<'_>, records: Bound<'_, PyAny>) -> PyResult<()> {
        // VersionedFiles.add_mpdiffs: uses get_record_stream and add_lines,
        // both of which are exposed on PyKnitVersionedFiles.
        let vf_m = py.import("bzrformats.versionedfile")?;
        let base_cls = vf_m.getattr("VersionedFiles")?;
        base_cls.call_method1("add_mpdiffs", (slf, records))?;
        Ok(())
    }

    #[pyo3(signature = (content_factory, parent_texts=None, left_matching_blocks=None, nostore_sha=None, random_id=false))]
    fn add_content(
        &self,
        py: Python<'_>,
        content_factory: Bound<'_, PyAny>,
        parent_texts: Option<Bound<'_, PyAny>>,
        left_matching_blocks: Option<Bound<'_, PyAny>>,
        nostore_sha: Option<Bound<'_, PyAny>>,
        random_id: Option<bool>,
    ) -> PyResult<Py<PyAny>> {
        let key = content_factory.getattr("key")?;
        let parents_obj = content_factory.getattr("parents")?;
        let parents: Option<Bound<'_, PyAny>> = if parents_obj.is_none() {
            None
        } else {
            Some(parents_obj)
        };
        let lines = content_factory.call_method1("get_bytes_as", ("lines",))?;
        self.add_lines(
            py,
            key,
            parents,
            lines,
            parent_texts,
            left_matching_blocks,
            nostore_sha,
            random_id.unwrap_or(false),
            false,
        )
    }

    #[getter]
    fn _factory(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        if self.annotated {
            Py::new(py, PyKnitAnnotateFactory).map(|p| p.into_any())
        } else {
            Py::new(py, PyKnitPlainFactory).map(|p| p.into_any())
        }
    }

    fn _read_records_iter_unchecked(
        &self,
        py: Python<'_>,
        records: Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        // Fetch raw (gzip-compressed) bytes for each (key, index_memo) pair
        // in order, without any validation or parsing.
        let mut keys: Vec<Bound<'_, PyAny>> = Vec::new();
        let memos_list = PyList::empty(py);
        for item in records.try_iter()? {
            let tup = item?.cast_into::<PyTuple>()?;
            keys.push(tup.get_item(0)?);
            memos_list.append(tup.get_item(1)?)?;
        }
        let raw_iter = self
            .access_obj
            .bind(py)
            .call_method1("get_raw_records", (memos_list,))?;
        let mut items: Vec<(Py<PyAny>, Py<PyAny>)> = Vec::with_capacity(keys.len());
        for (key, raw_obj) in keys.into_iter().zip(raw_iter.try_iter()?) {
            items.push((key.unbind(), raw_obj?.unbind()));
        }
        Ok(Py::new(
            py,
            KnitReadRecordsIterUnchecked {
                items: items.into_iter(),
            },
        )?
        .into_any())
    }

    fn _read_records_iter_raw(
        &self,
        py: Python<'_>,
        records: Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        // Fetch raw bytes and parse each record header to extract the sha1
        // digest. Yields (key, raw_bytes, digest_bytes).
        let mut keys: Vec<Bound<'_, PyAny>> = Vec::new();
        let memos_list = PyList::empty(py);
        for item in records.try_iter()? {
            let tup = item?.cast_into::<PyTuple>()?;
            keys.push(tup.get_item(0)?);
            memos_list.append(tup.get_item(1)?)?;
        }
        let raw_iter = self
            .access_obj
            .bind(py)
            .call_method1("get_raw_records", (memos_list,))?;
        let mut items: Vec<(Py<PyAny>, Py<PyBytes>)> = Vec::with_capacity(keys.len());
        for (key, raw_obj) in keys.into_iter().zip(raw_iter.try_iter()?) {
            let raw_bytes = raw_obj?.cast_into::<PyBytes>()?;
            items.push((key.unbind(), raw_bytes.unbind()));
        }
        Ok(Py::new(
            py,
            KnitReadRecordsIterRaw {
                items: items.into_iter(),
            },
        )?
        .into_any())
    }

    fn _read_records_iter(&self, py: Python<'_>, records: Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let mut pairs: Vec<(Bound<'_, PyAny>, Bound<'_, PyAny>)> = Vec::new();
        for item in records.try_iter()? {
            let tup = item?.cast_into::<PyTuple>()?;
            pairs.push((tup.get_item(0)?, tup.get_item(1)?));
        }
        let mut seen_ids: std::collections::HashSet<usize> = std::collections::HashSet::new();
        let mut needed: Vec<(Bound<'_, PyAny>, Bound<'_, PyAny>)> = Vec::new();
        for (key, memo) in pairs {
            if seen_ids.insert(memo.as_ptr() as usize) {
                needed.push((key, memo));
            }
        }
        needed.sort_by(|(_, a), (_, b)| {
            let ar = a.repr().map(|s| s.to_string()).unwrap_or_default();
            let br = b.repr().map(|s| s.to_string()).unwrap_or_default();
            ar.cmp(&br)
        });
        let memos_list = PyList::empty(py);
        for (_, memo) in &needed {
            memos_list.append(memo)?;
        }
        let raw_iter = self
            .access_obj
            .bind(py)
            .call_method1("get_raw_records", (memos_list,))?;
        let mut items: Vec<(Py<PyAny>, Py<PyBytes>)> = Vec::with_capacity(needed.len());
        for ((key, _), raw_obj) in needed.into_iter().zip(raw_iter.try_iter()?) {
            let raw_bytes = raw_obj?.cast_into::<PyBytes>()?;
            items.push((key.unbind(), raw_bytes.unbind()));
        }
        Ok(Py::new(
            py,
            KnitReadRecordsIter {
                items: items.into_iter(),
            },
        )?
        .into_any())
    }

    fn _parse_record(
        &self,
        py: Python<'_>,
        version_id: Bound<'_, PyAny>,
        data: Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let vid = version_id
            .cast_into::<PyBytes>()
            .map_err(|_| PyValueError::new_err("version_id must be bytes"))?;
        let raw = data
            .cast_into::<PyBytes>()
            .map_err(|_| PyValueError::new_err("data must be bytes"))?;
        let (body, digest) =
            bazaar::knit::parse_record(vid.as_bytes(), raw.as_bytes()).map_err(knit_err_to_py)?;
        let list = pyo3::types::PyList::empty(py);
        for line in &body {
            list.append(PyBytes::new(py, line))?;
        }
        Ok(
            PyTuple::new(py, [list.as_any(), PyBytes::new(py, &digest).as_any()])?
                .into_any()
                .unbind(),
        )
    }

    fn _parse_record_header(
        &self,
        py: Python<'_>,
        key: Bound<'_, PyAny>,
        raw_data: Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let raw = raw_data
            .cast_into::<PyBytes>()
            .map_err(|_| PyValueError::new_err("raw_data must be bytes"))?;
        let rec = bazaar::knit::parse_record_header_only(raw.as_bytes()).map_err(|e| {
            PyValueError::new_err(format!("While reading {{{key}}} got error: {e}"))
        })?;
        // Validate version_id matches key[-1].
        let expected = key
            .get_item(-1_isize)?
            .cast_into::<PyBytes>()
            .map_err(|_| PyValueError::new_err("key segments must be bytes"))?;
        if rec.version_id != expected.as_bytes() {
            return Err(PyValueError::new_err(format!(
                "Mismatched version: expected {:?}, got {:?}",
                expected.as_bytes(),
                &rec.version_id,
            )));
        }
        Ok(PyTuple::new(
            py,
            [
                PyBytes::new(py, &rec.method).into_any(),
                PyBytes::new(py, &rec.version_id).into_any(),
                PyBytes::new(py, rec.count.to_string().as_bytes()).into_any(),
                PyBytes::new(py, &rec.digest).into_any(),
            ],
        )?
        .into_any()
        .unbind())
    }

    #[pyo3(signature = (key, parent_texts=None))]
    fn _get_content(
        &self,
        py: Python<'_>,
        key: Bound<'_, PyAny>,
        parent_texts: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        if let Some(ref pt) = parent_texts {
            if let Ok(cached) = pt.get_item(&key) {
                if !cached.is_none() {
                    return Ok(cached.unbind());
                }
            }
        }
        let index = PyKnitIndex::new(self.index_obj.bind(py).clone());
        let access = PyKnitAccess::new(self.access_obj.bind(py).clone());
        let knit_key = extract_knit_key(&key).map_err(knit_err_to_py)?;
        let local_result: Result<Py<PyAny>, bazaar::knit::KnitError> = if self.annotated {
            bazaar::knit::get_content(
                &index,
                &access,
                &bazaar::knit::KnitAnnotateFactory,
                &knit_key,
            )
            .and_then(|content| {
                let strip = content.should_strip_eol();
                let mut inner = PyAnnotatedKnitContent(content);
                inner.0.set_should_strip_eol(strip);
                Py::new(py, inner)
                    .map(|p| p.into_any())
                    .map_err(|e| bazaar::knit::KnitError::Corrupt(e.to_string()))
            })
        } else {
            bazaar::knit::get_content(&index, &access, &bazaar::knit::KnitPlainFactory, &knit_key)
                .and_then(|content| {
                    let strip = content.should_strip_eol();
                    let version_id = knit_key.last().cloned().unwrap_or_default();
                    let mut plain = PlainKnitContent::new(content.lines, version_id);
                    plain.set_should_strip_eol(strip);
                    Py::new(py, PyPlainKnitContent(plain))
                        .map(|p| p.into_any())
                        .map_err(|e| bazaar::knit::KnitError::Corrupt(e.to_string()))
                })
        };
        match local_result {
            Ok(obj) => Ok(obj),
            Err(e) => {
                for fallback in &self.immediate_fallback_vfs {
                    let fb = fallback.bind(py);
                    let present: usize = fb
                        .call_method1("get_parent_map", (PyList::new(py, [&key])?,))?
                        .call_method0("__len__")?
                        .extract()?;
                    if present == 0 {
                        continue;
                    }
                    let stream = fb.call_method1(
                        "get_record_stream",
                        (PyList::new(py, [&key])?, "unordered", true),
                    )?;
                    let record = stream
                        .call_method0("__next__")
                        .map_err(|_| knit_err_to_py(e.clone()))?;
                    let storage_kind: String = record.getattr("storage_kind")?.extract()?;
                    if storage_kind == "absent" {
                        continue;
                    }
                    let lines_obj = record.call_method1("get_bytes_as", ("lines",))?;
                    let body: Vec<Vec<u8>> = lines_obj
                        .try_iter()?
                        .map(|item| {
                            item?
                                .cast_into::<PyBytes>()
                                .map(|b| b.as_bytes().to_vec())
                                .map_err(|_| PyValueError::new_err("lines must be bytes"))
                        })
                        .collect::<PyResult<_>>()?;
                    if self.annotated {
                        let version_id = knit_key.last().cloned().unwrap_or_default();
                        let pairs: Vec<bazaar::knit::AnnotatedLine> =
                            body.into_iter().map(|l| (version_id.clone(), l)).collect();
                        let content = AnnotatedKnitContent::new(pairs);
                        let obj = Py::new(py, PyAnnotatedKnitContent(content))?;
                        return Ok(obj.into_any());
                    } else {
                        let version_id = knit_key.last().cloned().unwrap_or_default();
                        let plain = PlainKnitContent::new(body, version_id);
                        let obj = Py::new(py, PyPlainKnitContent(plain))?;
                        return Ok(obj.into_any());
                    }
                }
                Err(knit_err_to_py(e))
            }
        }
    }

    fn _check_should_delta(&self, py: Python<'_>, parent: Bound<'_, PyAny>) -> PyResult<bool> {
        let index = PyKnitIndex::new(self.index_obj.bind(py).clone());
        let access = PyKnitAccess::new(self.access_obj.bind(py).clone());
        let knit_key = extract_knit_key(&parent).map_err(knit_err_to_py)?;
        if self.annotated {
            let kvf = bazaar::knit::KnitVersionedFiles::new(
                index,
                access,
                bazaar::knit::KnitAnnotateFactory,
                self.max_delta_chain,
            );
            kvf.check_should_delta(&knit_key).map_err(knit_err_to_py)
        } else {
            let kvf = bazaar::knit::KnitVersionedFiles::new(
                index,
                access,
                bazaar::knit::KnitPlainFactory,
                self.max_delta_chain,
            );
            kvf.check_should_delta(&knit_key).map_err(knit_err_to_py)
        }
    }

    fn _get_components_positions(
        &self,
        py: Python<'_>,
        keys: Bound<'_, PyAny>,
        allow_missing: Option<bool>,
    ) -> PyResult<Py<PyAny>> {
        let allow_missing = allow_missing.unwrap_or(false);
        let key_list = PyList::empty(py);
        for k in keys.try_iter()? {
            key_list.append(k?)?;
        }
        let get_build_details = self.index_obj.bind(py).getattr("get_build_details")?;
        walk_components_positions_rs(py, key_list.into_any(), allow_missing, get_build_details)
            .map(|d| d.into_any().unbind())
    }

    fn _get_parent_map_with_sources(
        &self,
        py: Python<'_>,
        keys: Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let result = PyDict::new(py);
        let source_results = PyList::empty(py);
        let missing = pyo3::types::PySet::empty(py)?;
        for k in keys.try_iter()? {
            missing.add(k?)?;
        }
        // Local index first, then fallback VFs.
        let local_map = self
            .index_obj
            .bind(py)
            .call_method1("get_parent_map", (missing.clone(),))?
            .cast_into::<PyDict>()?;
        for (k, v) in local_map.iter() {
            result.set_item(&k, &v)?;
            missing.discard(k)?;
        }
        source_results.append(local_map)?;
        for source in &self.immediate_fallback_vfs {
            if missing.is_empty() {
                break;
            }
            let new_result = source
                .bind(py)
                .call_method1("get_parent_map", (missing.clone(),))?
                .cast_into::<PyDict>()?;
            for (k, v) in new_result.iter() {
                result.set_item(&k, &v)?;
                missing.discard(k)?;
            }
            source_results.append(new_result)?;
        }
        Ok(
            PyTuple::new(py, [result.as_any(), source_results.as_any()])?
                .into_any()
                .unbind(),
        )
    }

    #[staticmethod]
    fn _split_by_prefix(py: Python<'_>, keys: Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let keys_raw: Vec<Vec<Vec<u8>>> = keys
            .try_iter()?
            .map(|k| {
                k?.try_iter()?
                    .map(|seg| seg?.extract::<Vec<u8>>())
                    .collect::<PyResult<_>>()
            })
            .collect::<PyResult<_>>()?;
        let (buckets, prefix_order) = bazaar::knit::split_keys_by_prefix(&keys_raw);
        let out_dict = PyDict::new(py);
        for (prefix, bucket_keys) in &buckets {
            let list = PyList::empty(py);
            for key in bucket_keys {
                list.append(PyTuple::new(
                    py,
                    key.iter().map(|seg| PyBytes::new(py, seg)),
                )?)?;
            }
            out_dict.set_item(PyBytes::new(py, prefix), list)?;
        }
        let order_list = PyList::empty(py);
        for prefix in &prefix_order {
            order_list.append(PyBytes::new(py, prefix))?;
        }
        Ok(PyTuple::new(py, [out_dict.as_any(), order_list.as_any()])?
            .into_any()
            .unbind())
    }

    fn _record_to_data(
        &self,
        py: Python<'_>,
        key: Bound<'_, PyAny>,
        digest: Bound<'_, PyAny>,
        lines: Bound<'_, PyAny>,
        dense_lines: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let version_id = key
            .get_item(-1_isize)?
            .cast_into::<PyBytes>()
            .map_err(|_| PyValueError::new_err("key[-1] must be bytes"))?;
        let digest_bytes = digest
            .cast_into::<PyBytes>()
            .map_err(|_| PyValueError::new_err("digest must be bytes"))?;
        let lines_list: Vec<Bound<'_, PyAny>> = lines.try_iter()?.collect::<PyResult<_>>()?;
        let line_count = lines_list.len();
        let payload_src = dense_lines.as_ref().unwrap_or(&lines);
        let payload: Vec<Vec<u8>> = payload_src
            .try_iter()?
            .map(|item| {
                item?
                    .cast_into::<PyBytes>()
                    .map(|b| b.as_bytes().to_vec())
                    .map_err(|_| PyValueError::new_err("lines must be bytes"))
            })
            .collect::<PyResult<_>>()?;
        let has_trailing_newline = lines_list
            .last()
            .and_then(|l| l.cast::<PyBytes>().ok())
            .map(|b| b.as_bytes().ends_with(b"\n"))
            .unwrap_or(true);
        let (size, chunks) = bazaar::knit::record_to_data(
            version_id.as_bytes(),
            digest_bytes.as_bytes(),
            line_count,
            &payload,
            has_trailing_newline,
        )
        .map_err(knit_err_to_py)?;
        let chunk_list = PyList::empty(py);
        for c in &chunks {
            chunk_list.append(PyBytes::new(py, c))?;
        }
        Ok(PyTuple::new(
            py,
            [size.into_pyobject(py)?.into_any(), chunk_list.into_any()],
        )?
        .into_any()
        .unbind())
    }

    #[pyo3(signature = (keys, allow_missing=false))]
    fn _get_record_map_unparsed(
        &self,
        py: Python<'_>,
        keys: Bound<'_, PyAny>,
        allow_missing: bool,
    ) -> PyResult<Py<PyAny>> {
        // The whole request is retried if a pack reload happens partway
        // through; _get_components_positions grabs the entire build chain
        // anyway, so re-fetching it after a reload is cheap enough.
        retry_on_new_packs(py, &self.access_obj, || {
            self._get_record_map_unparsed_once(py, keys.clone(), allow_missing)
        })
    }

    fn _get_record_map_unparsed_once(
        &self,
        py: Python<'_>,
        keys: Bound<'_, PyAny>,
        allow_missing: bool,
    ) -> PyResult<Py<PyAny>> {
        // Walk the compression closure to build position_map, then fetch raw
        // bytes for all components and build {key: (raw_bytes, record_details, next)}.
        let position_map = self
            ._get_components_positions(py, keys, Some(allow_missing))?
            .into_bound(py)
            .cast_into::<PyDict>()?;
        let mut records: Vec<(Bound<'_, PyAny>, Bound<'_, PyAny>)> = Vec::new();
        for (key, value) in position_map.iter() {
            let tup = value.cast_into::<PyTuple>()?;
            let index_memo = tup.get_item(1)?;
            records.push((key, index_memo));
        }
        records.sort_by(|(_, a), (_, b)| {
            let ar = a.repr().map(|s| s.to_string()).unwrap_or_default();
            let br = b.repr().map(|s| s.to_string()).unwrap_or_default();
            ar.cmp(&br)
        });
        let records_list = PyList::empty(py);
        for (key, memo) in &records {
            records_list.append(PyTuple::new(py, [key, memo])?)?;
        }
        let raw_map = PyDict::new(py);
        for ((key, _), raw_obj) in records.iter().zip(
            self._read_records_iter_unchecked(py, records_list.into_any())?
                .bind(py)
                .try_iter()?,
        ) {
            let tup = raw_obj?.cast_into::<PyTuple>()?;
            let raw_data = tup.get_item(1)?;
            let pos_tup = position_map
                .get_item(key)?
                .ok_or_else(|| PyValueError::new_err("key missing from position_map"))?
                .cast_into::<PyTuple>()?;
            let record_details = pos_tup.get_item(0)?;
            let next = pos_tup.get_item(2)?;
            raw_map.set_item(key, PyTuple::new(py, [&raw_data, &record_details, &next])?)?;
        }
        Ok(raw_map.into_any().unbind())
    }

    fn _raw_map_to_record_map(
        &self,
        py: Python<'_>,
        raw_map: Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let raw_map = raw_map.cast_into::<PyDict>()?;
        let result = PyDict::new(py);
        for (key, value) in raw_map.iter() {
            let tup = value.cast_into::<PyTuple>()?;
            let raw_data = tup.get_item(0)?;
            let record_details = tup.get_item(1)?;
            let next = tup.get_item(2)?;
            let version_id = key
                .get_item(-1_isize)?
                .cast_into::<PyBytes>()
                .map_err(|_| PyValueError::new_err("key[-1] must be bytes"))?;
            let raw = raw_data
                .cast_into::<PyBytes>()
                .map_err(|_| PyValueError::new_err("raw_data must be bytes"))?;
            let (body, digest) = bazaar::knit::parse_record(version_id.as_bytes(), raw.as_bytes())
                .map_err(knit_err_to_py)?;
            let lines = pyo3::types::PyList::empty(py);
            for line in &body {
                lines.append(PyBytes::new(py, line))?;
            }
            let py_digest = PyBytes::new(py, &digest);
            result.set_item(
                &key,
                PyTuple::new(
                    py,
                    [
                        lines.as_any(),
                        record_details.as_any(),
                        py_digest.as_any(),
                        next.as_any(),
                    ],
                )?,
            )?;
        }
        Ok(result.into_any().unbind())
    }

    #[pyo3(signature = (keys, allow_missing=false))]
    fn _get_record_map(
        &self,
        py: Python<'_>,
        keys: Bound<'_, PyAny>,
        allow_missing: bool,
    ) -> PyResult<Py<PyAny>> {
        let raw_map = self
            ._get_record_map_unparsed(py, keys, allow_missing)?
            .into_bound(py);
        self._raw_map_to_record_map(py, raw_map)
    }

    fn _parse_record_unchecked(
        &self,
        py: Python<'_>,
        data: Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let raw = data
            .cast_into::<PyBytes>()
            .map_err(|_| PyValueError::new_err("data must be bytes"))?;
        let (header, body) =
            bazaar::knit::parse_record_unchecked(raw.as_bytes()).map_err(knit_err_to_py)?;
        let rec = PyTuple::new(
            py,
            [
                PyBytes::new(py, &header.method).into_any(),
                PyBytes::new(py, &header.version_id).into_any(),
                PyBytes::new(py, header.count.to_string().as_bytes()).into_any(),
                PyBytes::new(py, &header.digest).into_any(),
            ],
        )?;
        let list = pyo3::types::PyList::empty(py);
        for line in &body {
            list.append(PyBytes::new(py, line))?;
        }
        Ok(PyTuple::new(py, [rec.as_any(), list.as_any()])?
            .into_any()
            .unbind())
    }

    #[pyo3(signature = (keys, non_local_keys, positions, _min_buffer_size=None))]
    fn _group_keys_for_io(
        &self,
        py: Python<'_>,
        keys: Bound<'_, PyAny>,
        non_local_keys: Bound<'_, PyAny>,
        positions: Bound<'_, PyAny>,
        _min_buffer_size: Option<usize>,
    ) -> PyResult<Py<PyAny>> {
        const DEFAULT_MIN_BUFFER_SIZE: usize = 5 * 1024 * 1024;
        let min_buffer_size = _min_buffer_size.unwrap_or(DEFAULT_MIN_BUFFER_SIZE);
        let positions_dict = positions.cast_into::<PyDict>()?;
        // Collect all keys and non-local keys as Python lists for later use.
        let keys_list: Vec<Bound<'_, PyAny>> = keys.try_iter()?.collect::<PyResult<_>>()?;
        let non_local_list: Vec<Bound<'_, PyAny>> =
            non_local_keys.try_iter()?.collect::<PyResult<_>>()?;
        // Extract keys as Rust-native Vec<Vec<Vec<u8>>> for split_keys_by_prefix.
        let keys_raw: Vec<Vec<Vec<u8>>> = keys_list
            .iter()
            .map(|k| {
                k.try_iter()?
                    .map(|seg| seg?.extract::<Vec<u8>>())
                    .collect::<PyResult<_>>()
            })
            .collect::<PyResult<_>>()?;
        let non_local_raw: Vec<Vec<Vec<u8>>> = non_local_list
            .iter()
            .map(|k| {
                k.try_iter()?
                    .map(|seg| seg?.extract::<Vec<u8>>())
                    .collect::<PyResult<_>>()
            })
            .collect::<PyResult<_>>()?;

        let (prefix_split_keys_rs, prefix_order_rs) = bazaar::knit::split_keys_by_prefix(&keys_raw);
        let (prefix_split_nl_rs, _) = bazaar::knit::split_keys_by_prefix(&non_local_raw);

        // Build Python dicts/lists from the Rust results.
        let prefix_split_keys = PyDict::new(py);
        for (prefix, bucket) in &prefix_split_keys_rs {
            let list = PyList::empty(py);
            for key in bucket {
                list.append(PyTuple::new(
                    py,
                    key.iter().map(|seg| PyBytes::new(py, seg)),
                )?)?;
            }
            prefix_split_keys.set_item(PyBytes::new(py, prefix), list)?;
        }
        let prefix_order_list = pyo3::types::PyList::empty(py);
        for prefix in &prefix_order_rs {
            prefix_order_list.append(PyBytes::new(py, prefix))?;
        }
        let prefix_split_non_local = PyDict::new(py);
        for (prefix, bucket) in &prefix_split_nl_rs {
            let list = PyList::empty(py);
            for key in bucket {
                list.append(PyTuple::new(
                    py,
                    key.iter().map(|seg| PyBytes::new(py, seg)),
                )?)?;
            }
            prefix_split_non_local.set_item(PyBytes::new(py, prefix), list)?;
        }
        let result = PyList::empty(py);
        let mut cur_keys = PyList::empty(py);
        let mut cur_non_local = pyo3::types::PySet::empty(py)?;
        let mut cur_size: usize = 0;
        for prefix in prefix_order_list.iter() {
            let bucket_keys = prefix_split_keys
                .get_item(&prefix)?
                .unwrap_or_else(|| PyList::empty(py).into_any());
            let bucket_nl = prefix_split_non_local
                .get_item(&prefix)?
                .unwrap_or_else(|| PyList::empty(py).into_any());
            let this_size: usize = self
                .index_obj
                .bind(py)
                .call_method1(
                    "_get_total_build_size",
                    (bucket_keys.clone(), positions_dict.clone()),
                )?
                .extract()?;
            cur_size += this_size;
            for k in bucket_keys.try_iter()? {
                cur_keys.append(k?)?;
            }
            for k in bucket_nl.try_iter()? {
                cur_non_local.add(k?)?;
            }
            if cur_size > min_buffer_size {
                result.append(PyTuple::new(
                    py,
                    [cur_keys.as_any(), cur_non_local.as_any()],
                )?)?;
                cur_keys = PyList::empty(py);
                cur_non_local = pyo3::types::PySet::empty(py)?;
                cur_size = 0;
            }
        }
        if !cur_keys.is_empty() {
            result.append(PyTuple::new(
                py,
                [cur_keys.as_any(), cur_non_local.as_any()],
            )?)?;
        }
        Ok(result.into_any().unbind())
    }

    fn clear_cache(&self, _py: Python<'_>) -> PyResult<()> {
        // No in-memory cache to clear at this layer.
        Ok(())
    }

    fn get_known_graph_ancestry(
        &self,
        py: Python<'_>,
        keys: Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        // Mirrors VersionedFilesWithFallbacks.get_known_graph_ancestry:
        // call find_ancestry on the local index, then walk fallbacks for any
        // missing keys, and finally wrap in a KnownGraph.
        let key_list = PyList::empty(py);
        for k in keys.try_iter()? {
            key_list.append(k?)?;
        }
        let result_tup = self
            .index_obj
            .bind(py)
            .call_method1("find_ancestry", (key_list,))?
            .cast_into::<PyTuple>()?;
        let parent_map = result_tup.get_item(0)?.cast_into::<PyDict>()?;
        let mut missing_keys = result_tup.get_item(1)?.cast_into::<pyo3::types::PySet>()?;
        // Walk the full transitive fallback chain, not just the immediate
        // fallbacks: a revision may live several stacking levels deep.
        for fallback in self._transitive_fallbacks(py)?.iter() {
            if missing_keys.is_empty() {
                break;
            }
            let ftup = fallback
                .getattr("_index")?
                .call_method1("find_ancestry", (missing_keys.clone(),))?
                .cast_into::<PyTuple>()?;
            let f_parent_map = ftup.get_item(0)?.cast_into::<PyDict>()?;
            let f_missing = ftup.get_item(1)?.cast_into::<pyo3::types::PySet>()?;
            for (k, v) in f_parent_map.iter() {
                parent_map.set_item(k, v)?;
            }
            missing_keys = f_missing;
        }
        let m = py.import("vcsgraph.known_graph")?;
        m.call_method1("KnownGraph", (parent_map,))
            .map(|b| b.unbind())
    }

    fn _transitive_fallbacks<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let result = PyList::empty(py);
        for fallback in &self.immediate_fallback_vfs {
            result.append(fallback.bind(py))?;
            let nested = fallback.bind(py).call_method0("_transitive_fallbacks")?;
            for item in nested.try_iter()? {
                result.append(item?)?;
            }
        }
        Ok(result)
    }
}

impl PyKnitVersionedFiles {
    /// Plan a non-delta-closure `get_record_stream` and return a lazy
    /// [`KnitRecordStreamLazy`] iterator.
    ///
    /// Mirrors `KnitVersionedFiles._get_remaining_record_stream`:
    ///   1. Build local positions from the index.
    ///   2. Use `_get_parent_map_with_sources` to learn which keys live where.
    ///   3. Sort/group keys by source ("topological" tsorts the union;
    ///      "unordered" groups remote-first).
    ///   4. Hand the source groups to the lazy stream, which fetches
    ///      local records on demand and delegates fallback groups.
    ///
    /// Only index reads happen here; record data is fetched lazily as the
    /// returned iterator is consumed.
    fn get_record_stream_local_once(
        slf: Py<PyKnitVersionedFiles>,
        py: Python<'_>,
        key_set: &Bound<'_, pyo3::types::PySet>,
        effective_ordering: &str,
    ) -> PyResult<Py<PyAny>> {
        let vf = slf;
        let this = vf.bind(py).borrow();
        let knit_keys: Vec<KnitKey> = key_set
            .clone()
            .try_iter()?
            .map(|k| extract_knit_key(&k?).map_err(knit_err_to_py))
            .collect::<PyResult<_>>()?;

        let local_index = PyKnitIndex::new(this.index_obj.bind(py).clone());
        let positions = local_index
            .get_build_details(&knit_keys)
            .map_err(knit_err_to_py)?;

        let global_map_tup = this
            ._get_parent_map_with_sources(py, key_set.clone().into_any())?
            .into_bound(py)
            .cast_into::<PyTuple>()?;
        let global_map_py = global_map_tup.get_item(0)?.cast_into::<PyDict>()?;
        let parent_maps = global_map_tup.get_item(1)?.cast_into::<PyList>()?;

        let mut global_map: std::collections::HashMap<KnitKey, Option<Vec<KnitKey>>> =
            std::collections::HashMap::new();
        for (k, v) in global_map_py.iter() {
            let key = extract_knit_key(&k).map_err(knit_err_to_py)?;
            let parents: Option<Vec<KnitKey>> = if v.is_none() {
                None
            } else {
                Some(
                    v.try_iter()?
                        .map(|p| extract_knit_key(&p?).map_err(knit_err_to_py))
                        .collect::<PyResult<_>>()?,
                )
            };
            global_map.insert(key, parents);
        }

        let mut source_of: std::collections::HashMap<KnitKey, usize> =
            std::collections::HashMap::new();
        for (idx, src_obj) in parent_maps.iter().enumerate() {
            let src = src_obj.cast_into::<PyDict>()?;
            for k_obj in src.keys().iter() {
                let k = extract_knit_key(&k_obj).map_err(knit_err_to_py)?;
                source_of.entry(k).or_insert(idx);
            }
        }

        let present_keys: Vec<KnitKey> = match effective_ordering {
            "topological" => {
                let tsort_iter = global_map
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone().unwrap_or_default()));
                let mut sorter = vcs_graph::tsort::TopoSorter::new(tsort_iter);
                sorter.sorted().map_err(|e| {
                    knit_err_to_py(bazaar::knit::KnitError::Corrupt(format!(
                        "topo_sort: {e:?}"
                    )))
                })?
            }
            "unordered" => {
                let mut out: Vec<KnitKey> = Vec::new();
                for src_obj in parent_maps.iter().rev() {
                    let src = src_obj.cast_into::<PyDict>()?;
                    for k_obj in src.keys().iter() {
                        out.push(extract_knit_key(&k_obj).map_err(knit_err_to_py)?);
                    }
                }
                out
            }
            "groupcompress" => bazaar::groupcompress::sort::sort_gc_optimal(
                global_map
                    .iter()
                    .map(|(k, v)| (k.clone(), v.clone().unwrap_or_default()))
                    .collect(),
            ),
            other => {
                return Err(PyValueError::new_err(format!(
                    "valid values for ordering are: \"unordered\", \"groupcompress\" or \"topological\" not: {other:?}"
                )));
            }
        };

        // Group consecutive keys by their owning source.
        let mut source_groups: Vec<(usize, Vec<KnitKey>)> = Vec::new();
        for key in &present_keys {
            let src_idx = source_of[key];
            if source_groups.last().map(|(s, _)| *s) != Some(src_idx) {
                source_groups.push((src_idx, Vec::new()));
            }
            source_groups.last_mut().unwrap().1.push(key.clone());
        }

        // For unordered mode the local group needs an I/O-sorted pass.
        if effective_ordering == "unordered" {
            for (src_idx, group) in source_groups.iter_mut() {
                if *src_idx == 0 {
                    local_index.sort_keys_by_io(group, &positions);
                }
            }
        }

        let absent_set: std::collections::HashSet<KnitKey> = knit_keys
            .iter()
            .filter(|k| !global_map.contains_key(*k))
            .cloned()
            .collect();

        // Build the lazy stream: absent records first, then each source
        // group in topological order. The local groups are fetched on
        // demand by KnitRecordStreamLazy so a pack reload only happens
        // when the stream reaches the affected pack.
        let mut items: std::collections::VecDeque<StreamItem> = std::collections::VecDeque::new();
        for key in &absent_set {
            items.push_back(StreamItem::Absent(key.clone()));
        }
        for (src_idx, group) in source_groups {
            if group.is_empty() {
                continue;
            }
            if src_idx == 0 {
                items.push_back(StreamItem::Local(group));
            } else {
                items.push_back(StreamItem::Fallback {
                    src_idx,
                    keys: group,
                });
            }
        }

        let stream = KnitRecordStreamLazy {
            vf: vf.clone_ref(py),
            annotated: this.annotated,
            effective_ordering: effective_ordering.to_string(),
            items,
            global_map,
            local: None,
            fallback_buffer: std::collections::VecDeque::new(),
        };
        Ok(Py::new(py, stream)?.into_any())
    }

    /// Plain-factory add_lines (avoids monomorphising KnitVersionedFiles twice).
    fn add_lines_plain(
        &self,
        py: Python<'_>,
        key: bazaar::knit::KnitKey,
        parents: Vec<bazaar::knit::KnitKey>,
        lines: Vec<Vec<u8>>,
        _digest: Vec<u8>,
        random_id: bool,
    ) -> PyResult<Py<PyAny>> {
        let index = PyKnitIndex::new(self.index_obj.bind(py).clone());
        let access = PyKnitAccess::new(self.access_obj.bind(py).clone());
        let kvf = bazaar::knit::KnitVersionedFiles::new(
            index,
            access,
            bazaar::knit::KnitPlainFactory,
            self.max_delta_chain,
        );
        let (ret_digest, text_length) = kvf
            .add_lines(key, parents, lines, random_id)
            .map_err(knit_err_to_py)?;
        let result = PyTuple::new(
            py,
            [
                PyBytes::new(py, &ret_digest).into_any(),
                text_length.into_pyobject(py)?.into_any(),
                py.None().into_bound(py),
            ],
        )?;
        Ok(result.into_any().unbind())
    }

    fn check_lines_not_unicode(&self, _py: Python<'_>, lines: &[Vec<u8>]) -> PyResult<()> {
        for line in lines {
            // All lines should be bytes; if they are Vec<u8> already, this is
            // a no-op since we've already converted from PyBytes.
            let _ = line;
        }
        Ok(())
    }

    fn check_lines_are_lines(&self, _py: Python<'_>, lines: &[Vec<u8>]) -> PyResult<()> {
        for (i, line) in lines.iter().enumerate() {
            if line.is_empty() {
                return Err(PyValueError::new_err(format!(
                    "line {} is empty, all lines must end with \\n",
                    i
                )));
            }
            if i < lines.len() - 1 && !line.ends_with(b"\n") {
                return Err(PyValueError::new_err(format!(
                    "line {} does not end with \\n: {:?}",
                    i,
                    &line[..line.len().min(40)]
                )));
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// _NetworkContentMapGenerator — Rust port of the Python class
// ---------------------------------------------------------------------------

/// Rust-backed `_NetworkContentMapGenerator`.
///
/// Parses the knit-delta-closure wire bytes once in `__init__` and serves as
/// the `generator` argument to `LazyKnitContentFactory`. Implements
/// `_get_one_work`, `_wire_bytes`, and `get_record_stream`.
#[pyclass(name = "_NetworkContentMapGenerator")]
struct PyNetworkContentMapGenerator {
    bytes: Vec<u8>,
    annotated: bool,
    keys: Vec<KnitKey>,
    global_map: std::collections::HashMap<KnitKey, Option<Vec<KnitKey>>>,
    raw_map: bazaar::knit::DeltaClosureRawMap,
    /// Cached reconstructed content: key → list of lines.
    contents_map: std::collections::HashMap<KnitKey, Vec<Vec<u8>>>,
}

#[pymethods]
impl PyNetworkContentMapGenerator {
    #[new]
    fn new(bytes: &[u8], line_end: usize) -> PyResult<Self> {
        let parsed = bazaar::knit::parse_delta_closure_wire_bytes(bytes, line_end)
            .map_err(knit_err_to_py)?;
        Ok(Self {
            bytes: bytes.to_vec(),
            annotated: parsed.annotated,
            keys: parsed.keys,
            global_map: parsed.global_map,
            raw_map: parsed.raw_map,
            contents_map: std::collections::HashMap::new(),
        })
    }

    fn _wire_bytes<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.bytes)
    }

    fn _get_one_work<'py>(
        &mut self,
        py: Python<'py>,
        key: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let kkey = extract_knit_key(&key).map_err(knit_err_to_py)?;
        if !self.contents_map.contains_key(&kkey) {
            // Reconstruct every key in the closure (matches Python).
            for k in &self.keys {
                if self.contents_map.contains_key(k) {
                    continue;
                }
                let lines = if self.annotated {
                    bazaar::knit::reconstruct_text_from_raw_map(
                        &bazaar::knit::KnitAnnotateFactory,
                        &self.raw_map,
                        k,
                    )
                } else {
                    bazaar::knit::reconstruct_text_from_raw_map(
                        &bazaar::knit::KnitPlainFactory,
                        &self.raw_map,
                        k,
                    )
                }
                .map_err(knit_err_to_py)?;
                self.contents_map.insert(k.clone(), lines.0);
            }
        }
        let lines = self.contents_map.get(&kkey).ok_or_else(|| {
            pyo3::exceptions::PyKeyError::new_err(format!("key {kkey:?} not in generator"))
        })?;
        let version_id = kkey.last().cloned().unwrap_or_default();
        let plain = PyPlainKnitContent(PlainKnitContent::new(lines.clone(), version_id));
        Ok(plain.into_pyobject(py)?.into_any())
    }

    fn get_record_stream(
        slf: Py<Self>,
        py: Python<'_>,
    ) -> PyResult<Bound<'_, pyo3::types::PyList>> {
        let list = pyo3::types::PyList::empty(py);
        let keys: Vec<KnitKey> = slf.borrow(py).keys.clone();
        let global_map: std::collections::HashMap<KnitKey, Option<Vec<KnitKey>>> =
            slf.borrow(py).global_map.clone();
        let mut first = true;
        for key in &keys {
            let parents = global_map.get(key).cloned().flatten();
            let py_key = py_knit_key_to_py(py, key)?.into_any().unbind();
            let py_parents = match parents {
                None => py.None(),
                Some(ref ps) => PyTuple::new(
                    py,
                    ps.iter()
                        .map(|p| py_knit_key_to_py(py, p).map(|t| t.into_any()))
                        .collect::<PyResult<Vec<_>>>()?,
                )?
                .into_any()
                .unbind(),
            };
            let factory = PyLazyKnitContentFactory {
                key: py_key,
                parents: py_parents,
                sha1: py.None(),
                storage_kind: bazaar::knit::delta_closure_storage_kind(first).to_owned(),
                generator: slf.clone_ref(py).into_any(),
                first,
            };
            list.append(factory.into_pyobject(py)?)?;
            first = false;
        }
        Ok(list)
    }
}


// ---------------------------------------------------------------------------
// _VFContentMapGenerator — Rust port of the Python class
// ---------------------------------------------------------------------------

/// Convert the Python `_get_record_map_unparsed` dict into a
/// `DeltaClosureRawMap`. The dict has the shape
/// `{key: (raw_bytes, (method_str, noeol_bool), next)}`.
fn py_raw_map_to_delta_closure_map(
    raw_map_obj: &Bound<'_, PyAny>,
) -> PyResult<bazaar::knit::DeltaClosureRawMap> {
    let dict = raw_map_obj.clone().cast_into::<PyDict>()?;
    let mut result = bazaar::knit::DeltaClosureRawMap::new();
    for (key, value) in dict.iter() {
        let tup = value.cast_into::<PyTuple>()?;
        let raw_bytes = tup
            .get_item(0)?
            .cast_into::<PyBytes>()
            .map(|b| b.as_bytes().to_vec())
            .map_err(|_| pyo3::exceptions::PyTypeError::new_err("raw_bytes must be bytes"))?;
        let record_details = tup.get_item(1)?;
        let method_str: String = record_details.get_item(0)?.extract()?;
        let noeol: bool = record_details.get_item(1)?.extract()?;
        let method = bazaar::knit::KnitMethod::from_str(&method_str).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!("unknown method: {method_str}"))
        })?;
        let next_obj = tup.get_item(2)?;
        let next = if next_obj.is_none() {
            None
        } else {
            Some(extract_knit_key(&next_obj).map_err(knit_err_to_py)?)
        };
        result.insert(
            extract_knit_key(&key).map_err(knit_err_to_py)?,
            bazaar::knit::DeltaClosureRawEntry {
                raw_bytes,
                method,
                noeol,
                next,
            },
        );
    }
    Ok(result)
}

/// Rust-backed `_VFContentMapGenerator`.
///
/// Generates `LazyKnitContentFactory` records by pulling from a Python
/// `KnitVersionedFiles` object. The raw record map and global parent map
/// are populated lazily on first use (or supplied up front by the caller).
#[pyclass(name = "_VFContentMapGenerator")]
struct PyVFContentMapGenerator {
    vf: Py<PyAny>,
    keys: Vec<KnitKey>,
    nonlocal_keys: std::collections::HashSet<KnitKey>,
    global_map: Option<std::collections::HashMap<KnitKey, Option<Vec<KnitKey>>>>,
    annotated: bool,
    raw_map: Option<bazaar::knit::DeltaClosureRawMap>,
    contents_map: std::collections::HashMap<KnitKey, Vec<Vec<u8>>>,
}

#[pymethods]
impl PyVFContentMapGenerator {
    #[new]
    #[pyo3(signature = (versioned_files, keys, nonlocal_keys=None, global_map=None, raw_record_map=None, ordering="unordered"))]
    fn new(
        versioned_files: Bound<'_, PyAny>,
        keys: Bound<'_, PyAny>,
        nonlocal_keys: Option<Bound<'_, PyAny>>,
        global_map: Option<Bound<'_, PyAny>>,
        raw_record_map: Option<Bound<'_, PyAny>>,
        ordering: &str,
    ) -> PyResult<Self> {
        let _ = ordering; // accepted for API parity; no per-record ordering yet
        let annotated: bool = versioned_files
            .getattr("_factory")?
            .getattr("annotated")?
            .extract()?;
        let knit_keys: Vec<KnitKey> = keys
            .try_iter()?
            .map(|item| extract_knit_key(&item?).map_err(knit_err_to_py))
            .collect::<PyResult<_>>()?;
        let nonlocal: std::collections::HashSet<KnitKey> = match nonlocal_keys {
            None => std::collections::HashSet::new(),
            Some(ref obj) => obj
                .try_iter()?
                .map(|item| extract_knit_key(&item?).map_err(knit_err_to_py))
                .collect::<PyResult<_>>()?,
        };
        let parsed_global_map: Option<std::collections::HashMap<KnitKey, Option<Vec<KnitKey>>>> =
            match global_map {
                None => None,
                Some(ref gm) => {
                    let mut map = std::collections::HashMap::new();
                    let d = gm.clone().cast_into::<PyDict>()?;
                    for (key, val) in d.iter() {
                        let k = extract_knit_key(&key).map_err(knit_err_to_py)?;
                        let parents = if val.is_none() {
                            None
                        } else {
                            Some(
                                val.try_iter()?
                                    .map(|p| extract_knit_key(&p?).map_err(knit_err_to_py))
                                    .collect::<PyResult<_>>()?,
                            )
                        };
                        map.insert(k, parents);
                    }
                    Some(map)
                }
            };
        let preloaded_raw_map = match raw_record_map {
            None => None,
            Some(ref rm) => Some(py_raw_map_to_delta_closure_map(rm)?),
        };
        Ok(Self {
            vf: versioned_files.unbind(),
            keys: knit_keys,
            nonlocal_keys: nonlocal,
            global_map: parsed_global_map,
            annotated,
            raw_map: preloaded_raw_map,
            contents_map: std::collections::HashMap::new(),
        })
    }

    fn _get_one_work<'py>(
        &mut self,
        py: Python<'py>,
        key: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let kkey = extract_knit_key(&key).map_err(knit_err_to_py)?;
        if !self.contents_map.contains_key(&kkey) {
            if self.raw_map.is_none() {
                let local_keys: Vec<KnitKey> = self
                    .keys
                    .iter()
                    .filter(|k| !self.nonlocal_keys.contains(*k))
                    .cloned()
                    .collect();
                let py_keys = pyo3::types::PyList::empty(py);
                for k in &local_keys {
                    py_keys.append(py_knit_key_to_py(py, k)?)?;
                }
                let raw_map_obj = self
                    .vf
                    .bind(py)
                    .call_method1("_get_record_map_unparsed", (py_keys, Some(true)))?;
                self.raw_map = Some(py_raw_map_to_delta_closure_map(&raw_map_obj)?);
            }
            let raw_map = self.raw_map.as_ref().unwrap();
            for k in &self.keys {
                if self.nonlocal_keys.contains(k) || self.contents_map.contains_key(k) {
                    continue;
                }
                let lines = if self.annotated {
                    bazaar::knit::reconstruct_text_from_raw_map(
                        &bazaar::knit::KnitAnnotateFactory,
                        raw_map,
                        k,
                    )
                } else {
                    bazaar::knit::reconstruct_text_from_raw_map(
                        &bazaar::knit::KnitPlainFactory,
                        raw_map,
                        k,
                    )
                }
                .map_err(knit_err_to_py)?;
                self.contents_map.insert(k.clone(), lines.0);
            }
        }
        let lines = self.contents_map.get(&kkey).ok_or_else(|| {
            pyo3::exceptions::PyKeyError::new_err(format!("key {kkey:?} not in VF generator"))
        })?;
        let version_id = kkey.last().cloned().unwrap_or_default();
        let plain = PyPlainKnitContent(PlainKnitContent::new(lines.clone(), version_id));
        Ok(plain.into_pyobject(py)?.into_any())
    }

    fn _wire_bytes<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        if self.global_map.is_none() {
            let py_keys = pyo3::types::PyList::empty(py);
            for k in &self.keys {
                py_keys.append(py_knit_key_to_py(py, k)?)?;
            }
            let gm_obj = self
                .vf
                .bind(py)
                .call_method1("get_parent_map", (py_keys,))?;
            let mut map = std::collections::HashMap::new();
            let d = gm_obj.cast_into::<PyDict>()?;
            for (key, val) in d.iter() {
                let k = extract_knit_key(&key).map_err(knit_err_to_py)?;
                let parents = if val.is_none() {
                    None
                } else {
                    Some(
                        val.try_iter()?
                            .map(|p| extract_knit_key(&p?).map_err(knit_err_to_py))
                            .collect::<PyResult<_>>()?,
                    )
                };
                map.insert(k, parents);
            }
            self.global_map = Some(map);
        }
        if self.raw_map.is_none() {
            let local_keys: Vec<KnitKey> = self
                .keys
                .iter()
                .filter(|k| !self.nonlocal_keys.contains(*k))
                .cloned()
                .collect();
            let py_keys = pyo3::types::PyList::empty(py);
            for k in &local_keys {
                py_keys.append(py_knit_key_to_py(py, k)?)?;
            }
            let raw_map_obj = self
                .vf
                .bind(py)
                .call_method1("_get_record_map_unparsed", (py_keys, Some(true)))?;
            self.raw_map = Some(py_raw_map_to_delta_closure_map(&raw_map_obj)?);
        }
        let emit_keys: Vec<KnitKey> = self
            .keys
            .iter()
            .filter(|k| !self.nonlocal_keys.contains(*k))
            .cloned()
            .collect();
        let wire = bazaar::knit::build_delta_closure_wire_bytes(
            self.annotated,
            &emit_keys,
            self.raw_map.as_ref().unwrap(),
            self.global_map.as_ref().unwrap(),
        );
        Ok(PyBytes::new(py, &wire))
    }

    fn get_record_stream(
        slf: Py<Self>,
        py: Python<'_>,
    ) -> PyResult<Bound<'_, pyo3::types::PyList>> {
        let list = pyo3::types::PyList::empty(py);
        let keys: Vec<KnitKey> = slf.borrow(py).keys.clone();
        let nonlocal_keys: std::collections::HashSet<KnitKey> =
            slf.borrow(py).nonlocal_keys.clone();
        let global_map_snapshot: Option<
            std::collections::HashMap<KnitKey, Option<Vec<KnitKey>>>,
        > = slf.borrow(py).global_map.clone();

        let mut first = true;
        for key in &keys {
            if nonlocal_keys.contains(key) {
                continue;
            }
            let parents = global_map_snapshot
                .as_ref()
                .and_then(|m| m.get(key))
                .cloned()
                .flatten();
            let py_key = py_knit_key_to_py(py, key)?.into_any().unbind();
            let py_parents = match parents {
                None => py.None(),
                Some(ref ps) => PyTuple::new(
                    py,
                    ps.iter()
                        .map(|p| py_knit_key_to_py(py, p).map(|t| t.into_any()))
                        .collect::<PyResult<Vec<_>>>()?,
                )?
                .into_any()
                .unbind(),
            };
            let factory = PyLazyKnitContentFactory {
                key: py_key,
                parents: py_parents,
                sha1: py.None(),
                storage_kind: bazaar::knit::delta_closure_storage_kind(first).to_owned(),
                generator: slf.clone_ref(py).into_any(),
                first,
            };
            list.append(factory.into_pyobject(py)?)?;
            first = false;
        }
        Ok(list)
    }
}

// ---------------------------------------------------------------------------
// KnitAdapter pyclass + adapter_registry shim
// ---------------------------------------------------------------------------

/// Marshal a `KnitTextResult` (the pure-crate "fulltext bytes vs. list of
/// lines") into the matching Python object.
fn text_result_to_py<'py>(
    py: Python<'py>,
    result: bazaar::knit::KnitTextResult,
) -> PyResult<Bound<'py, PyAny>> {
    match result {
        bazaar::knit::KnitTextResult::Bytes(b) => Ok(PyBytes::new(py, &b).into_any()),
        bazaar::knit::KnitTextResult::Lines(lines) => {
            let py_lines: Vec<Bound<'py, PyBytes>> =
                lines.iter().map(|l| PyBytes::new(py, l)).collect();
            Ok(pyo3::types::PyList::new(py, &py_lines)?.into_any())
        }
    }
}

/// Fetch the fulltext lines for `compression_parent` from a Python
/// `versioned_files` object via `get_record_stream`.
fn get_basis_lines<'py>(
    py: Python<'py>,
    basis_vf: &Bound<'py, PyAny>,
    compression_parent: &Bound<'py, PyAny>,
) -> PyResult<Vec<Vec<u8>>> {
    let stream = basis_vf.call_method1(
        "get_record_stream",
        (
            vec![compression_parent.clone()],
            pyo3::intern!(py, "unordered"),
            true,
        ),
    )?;
    let record = stream
        .try_iter()?
        .next()
        .ok_or_else(|| {
            pyo3::exceptions::PyStopIteration::new_err("no records returned from basis_vf")
        })??;
    let kind: String = record.getattr("storage_kind")?.extract()?;
    if kind == "absent" {
        return Err(RevisionNotPresent::new_err((
            compression_parent.clone().unbind(),
            basis_vf.clone().unbind(),
        )));
    }
    let lines_obj = record.call_method1("get_bytes_as", (pyo3::intern!(py, "lines"),))?;
    lines_obj
        .try_iter()?
        .map(|item| {
            item?
                .cast_into::<PyBytes>()
                .map(|b| b.as_bytes().to_vec())
                .map_err(|_| {
                    pyo3::exceptions::PyTypeError::new_err("basis line must be bytes")
                })
        })
        .collect()
}

/// Bridge from a `&dyn KnitAdapter` to a Python `versioned_files` object.
///
/// PyErr from the Python callback can't cross the pure-crate
/// `AdapterError` boundary without losing its exception class
/// (`RevisionNotPresent`, etc.). Stash the PyErr in `last_pyerr` and
/// surface a sentinel `AdapterError::Knit` to the adapter; the caller
/// in `PyKnitAdapter::get_bytes` checks `last_pyerr` first and
/// re-raises the original on its way back to Python.
struct PyBasisVfBridge<'py> {
    py: Python<'py>,
    versioned_files: Bound<'py, PyAny>,
    last_pyerr: std::cell::Cell<Option<PyErr>>,
}

impl<'py> bazaar::knit::BasisVfBridge for PyBasisVfBridge<'py> {
    fn get_basis_lines(
        &self,
        compression_parent: &[Vec<u8>],
    ) -> Result<Vec<Vec<u8>>, bazaar::knit::AdapterError> {
        let py = self.py;
        let inner = || -> PyResult<Vec<Vec<u8>>> {
            let parent_tuple =
                PyTuple::new(py, compression_parent.iter().map(|seg| PyBytes::new(py, seg)))?;
            get_basis_lines(py, &self.versioned_files, &parent_tuple.into_any())
        };
        match inner() {
            Ok(lines) => Ok(lines),
            Err(e) => {
                let msg = e.to_string();
                self.last_pyerr.set(Some(e));
                Err(bazaar::knit::AdapterError::Knit(
                    bazaar::knit::KnitError::Corrupt(msg),
                ))
            }
        }
    }
}

/// Pull the borrowed key / raw_record / noeol / parents / storage_kind
/// off a Python `ContentFactory`. Returns owned data so the adapter can
/// keep them alive while it runs.
struct ExtractedFactory {
    key: Vec<Vec<u8>>,
    raw_record: Vec<u8>,
    noeol: bool,
    parents: Option<Vec<Vec<Vec<u8>>>>,
    storage_kind: String,
}

fn extract_factory(factory: &Bound<'_, PyAny>) -> PyResult<ExtractedFactory> {
    let key: Vec<Vec<u8>> = factory
        .getattr("key")?
        .try_iter()?
        .map(|seg| {
            seg?.cast_into::<PyBytes>()
                .map(|b| b.as_bytes().to_vec())
                .map_err(|_| pyo3::exceptions::PyTypeError::new_err("key segment must be bytes"))
        })
        .collect::<PyResult<_>>()?;
    let raw_record: Vec<u8> = factory
        .getattr("_raw_record")?
        .cast_into::<PyBytes>()
        .map(|b| b.as_bytes().to_vec())
        .map_err(|_| pyo3::exceptions::PyTypeError::new_err("_raw_record must be bytes"))?;
    let build_details = factory.getattr("_build_details")?;
    let noeol: bool = build_details.get_item(1)?.extract()?;
    let parents_obj = factory.getattr("parents")?;
    let parents: Option<Vec<Vec<Vec<u8>>>> = if parents_obj.is_none() {
        None
    } else {
        Some(
            parents_obj
                .try_iter()?
                .map(|p| {
                    p?.try_iter()?
                        .map(|seg| {
                            seg?.cast_into::<PyBytes>()
                                .map(|b| b.as_bytes().to_vec())
                                .map_err(|_| {
                                    pyo3::exceptions::PyTypeError::new_err(
                                        "parent segment must be bytes",
                                    )
                                })
                        })
                        .collect::<PyResult<_>>()
                })
                .collect::<PyResult<_>>()?,
        )
    };
    let storage_kind: String = factory.getattr("storage_kind")?.extract()?;
    Ok(ExtractedFactory {
        key,
        raw_record,
        noeol,
        parents,
        storage_kind,
    })
}

fn adapter_err_to_py(
    py: Python<'_>,
    e: bazaar::knit::AdapterError,
    factory_key: &[Vec<u8>],
) -> PyErr {
    use bazaar::knit::AdapterError;
    match e {
        AdapterError::Unavailable {
            source_storage_kind,
            target_storage_kind,
        } => {
            let py_key = PyTuple::new(py, factory_key.iter().map(|s| PyBytes::new(py, s)))
                .map(|t| t.into_any().unbind())
                .unwrap_or_else(|_| py.None());
            UnavailableRepresentation::new_err((py_key, target_storage_kind, source_storage_kind))
        }
        AdapterError::BasisNotPresent(key) => {
            let py_key = PyTuple::new(py, key.iter().map(|s| PyBytes::new(py, s)))
                .map(|t| t.into_any().unbind())
                .unwrap_or_else(|_| py.None());
            RevisionNotPresent::new_err((py_key, py.None()))
        }
        AdapterError::Knit(k) => knit_err_to_py(k),
    }
}

/// Python-facing wrapper around a `&'static dyn KnitAdapter`.
///
/// Behaves like the old `KnitAdapter` Python classes: callers do
/// `adapter = get_knit_adapter(src, tgt, basis_vf)` and then call
/// `adapter.get_bytes(factory, target_storage_kind)`.
#[pyclass(name = "KnitAdapter")]
struct PyKnitAdapter {
    inner: &'static dyn bazaar::knit::KnitAdapter,
    basis_vf: Option<Py<PyAny>>,
}

#[pymethods]
impl PyKnitAdapter {
    fn get_bytes<'py>(
        &self,
        py: Python<'py>,
        factory: Bound<'py, PyAny>,
        target_storage_kind: &str,
    ) -> PyResult<Bound<'py, PyAny>> {
        let extracted = extract_factory(&factory)?;
        let parents_slice: Option<&[Vec<Vec<u8>>]> = extracted.parents.as_deref();
        let input = bazaar::knit::KnitAdapterInput {
            key: &extracted.key,
            raw_record: &extracted.raw_record,
            noeol: extracted.noeol,
            parents: parents_slice,
            storage_kind: &extracted.storage_kind,
        };
        let basis_vf_bound = self.basis_vf.as_ref().map(|v| v.bind(py).clone());
        let bridge = basis_vf_bound.as_ref().map(|vf| PyBasisVfBridge {
            py,
            versioned_files: vf.clone(),
            last_pyerr: std::cell::Cell::new(None),
        });
        let basis: Option<&dyn bazaar::knit::BasisVfBridge> = match &bridge {
            Some(b) => Some(b),
            None => None,
        };
        let result = self.inner.get_bytes(&input, target_storage_kind, basis);
        // If the bridge caught a PyErr during the call, re-raise it so the
        // original exception class is preserved (e.g. RevisionNotPresent).
        if let Some(b) = &bridge {
            if let Some(err) = b.last_pyerr.take() {
                return Err(err);
            }
        }
        let out = result.map_err(|e| adapter_err_to_py(py, e, &extracted.key))?;
        match out {
            bazaar::knit::KnitAdapterOutput::RawBytes(b) => Ok(PyBytes::new(py, &b).into_any()),
            bazaar::knit::KnitAdapterOutput::Text(t) => text_result_to_py(py, t),
        }
    }
}

/// Look up a knit adapter for `(source_storage_kind, target_storage_kind)`,
/// optionally binding `basis_vf` for the delta-to-fulltext adapters.
/// Returns `None` if no adapter handles the requested transition.
#[pyfunction]
#[pyo3(signature = (source_storage_kind, target_storage_kind, basis_vf=None))]
fn get_knit_adapter(
    source_storage_kind: &str,
    target_storage_kind: &str,
    basis_vf: Option<Bound<'_, PyAny>>,
) -> Option<PyKnitAdapter> {
    bazaar::knit::lookup_adapter(source_storage_kind, target_storage_kind).map(|adapter| {
        PyKnitAdapter {
            inner: adapter,
            basis_vf: basis_vf.map(|v| v.unbind()),
        }
    })
}

pub(crate) fn _knit_rs(py: Python) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "knit")?;
    m.add_function(wrap_pyfunction!(_load_data, &m)?)?;
    m.add_function(wrap_pyfunction!(parse_fulltext_rs, &m)?)?;
    m.add_function(wrap_pyfunction!(parse_line_delta_rs, &m)?)?;
    m.add_function(wrap_pyfunction!(lower_fulltext_rs, &m)?)?;
    m.add_function(wrap_pyfunction!(lower_line_delta_rs, &m)?)?;
    m.add_function(wrap_pyfunction!(parse_line_delta_raw_rs, &m)?)?;
    m.add_function(wrap_pyfunction!(lower_line_delta_raw_rs, &m)?)?;
    m.add_function(wrap_pyfunction!(get_line_delta_blocks_rs, &m)?)?;
    m.add_function(wrap_pyfunction!(parse_network_record_header_rs, &m)?)?;
    m.add_function(wrap_pyfunction!(parse_record_unchecked_rs, &m)?)?;
    m.add_function(wrap_pyfunction!(parse_record_rs, &m)?)?;
    m.add_function(wrap_pyfunction!(record_to_data_rs, &m)?)?;
    m.add_function(wrap_pyfunction!(parse_record_header_only_rs, &m)?)?;
    m.add_function(wrap_pyfunction!(knit_entries_to_build_details_rs, &m)?)?;
    m.add_function(wrap_pyfunction!(parse_knit_index_value_rs, &m)?)?;
    m.add_function(wrap_pyfunction!(decode_kndx_options_rs, &m)?)?;
    m.add_function(wrap_pyfunction!(check_should_delta_rs, &m)?)?;
    m.add_function(wrap_pyfunction!(walk_components_positions_rs, &m)?)?;
    m.add_function(wrap_pyfunction!(get_text_via_traits_rs, &m)?)?;
    m.add_function(wrap_pyfunction!(get_content_via_traits_rs, &m)?)?;
    m.add_function(wrap_pyfunction!(get_sha1s_via_traits_rs, &m)?)?;
    m.add_function(wrap_pyfunction!(build_network_record_rs, &m)?)?;
    m.add_function(wrap_pyfunction!(build_knit_delta_closure_wire_rs, &m)?)?;
    m.add_function(wrap_pyfunction!(split_keys_by_prefix_rs, &m)?)?;
    m.add_function(wrap_pyfunction!(get_total_build_size_rs, &m)?)?;
    m.add_function(wrap_pyfunction!(dictionary_compress_rs, &m)?)?;
    m.add_class::<PyAnnotatedKnitContent>()?;
    m.add_class::<PyPlainKnitContent>()?;
    m.add_class::<PyKnitAnnotateFactory>()?;
    m.add_class::<PyKnitPlainFactory>()?;
    m.add_class::<PyKnitAnnotator>()?;
    m.add_class::<PyKndxIndex>()?;
    m.add_class::<PyKnitGraphIndex>()?;
    m.add_class::<PyKnitKeyAccess>()?;
    m.add_class::<PyKnitVersionedFiles>()?;
    m.add_class::<PyKnitDeltaClosureRecord>()?;
    m.add_class::<PyLazyKnitContentFactory>()?;
    m.add_class::<PyNetworkContentMapGenerator>()?;
    m.add_class::<PyVFContentMapGenerator>()?;
    m.add_class::<PyKnitAdapter>()?;
    m.add_function(wrap_pyfunction!(get_knit_adapter, &m)?)?;
    Ok(m)
}
