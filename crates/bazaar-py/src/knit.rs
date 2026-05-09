use bazaar::key_mapper::Mapper as _;
use bazaar::knit::{
    lower_fulltext, lower_line_delta_annotated, lower_line_delta_raw, parse_fulltext,
    parse_line_delta_annotated, parse_line_delta_plain, parse_line_delta_raw,
    parse_network_record_header, AnnotatedKnitContent, AnnotatedLine, DeltaHunk, KndxLoadError,
    KnitAccess as KnitAccessTrait, KnitAnnotateFactory, KnitContent as KnitContentTrait, KnitError,
    KnitFactory as KnitFactoryTrait, KnitIndex as KnitIndexTrait, KnitIndexMemo, KnitKey,
    KnitMethod, KnitPlainFactory, KnitRecordDetails, PlainKnitContent,
};
use bazaar::transport::Transport as _;
use pyo3::exceptions::{PyIndexError, PyNotImplementedError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};

pyo3::import_exception!(bzrformats.errors, RevisionNotPresent);

/// Parse a knit index record line into its components.
///
/// Each line has the format: `version_id options pos size parent1 parent2 ... :`
/// Returns None if the line is incomplete/corrupt.
fn process_one_record<'py>(
    py: Python<'py>,
    line: &[u8],
    history: &Bound<'py, PyList>,
    history_len: &mut i64,
    cache: &Bound<'py, PyDict>,
) -> PyResult<bool> {
    // Split the line by spaces
    let fields: Vec<&[u8]> = line.split(|&b| b == b' ').collect();

    // Need at least 5 fields: version_id options pos size ... :
    if fields.len() < 5 || fields[fields.len() - 1] != b":" {
        return Ok(false);
    }

    let version_id = PyBytes::new(py, fields[0]);
    let options: Vec<Bound<'py, PyBytes>> = fields[1]
        .split(|&b| b == b',')
        .map(|opt| PyBytes::new(py, opt))
        .collect();
    let options_list = PyList::new(py, &options)?;

    let pos_str = std::str::from_utf8(fields[2])
        .map_err(|_| PyValueError::new_err(format!("{:?} is not a valid integer", fields[2])))?;
    let pos: i64 = pos_str
        .parse()
        .map_err(|_| PyValueError::new_err(format!("{:?} is not a valid integer", pos_str)))?;

    let size_str = std::str::from_utf8(fields[3])
        .map_err(|_| PyValueError::new_err(format!("{:?} is not a valid integer", fields[3])))?;
    let size: i64 = size_str
        .parse()
        .map_err(|_| PyValueError::new_err(format!("{:?} is not a valid integer", size_str)))?;

    // Parse parents (fields[4..len-1], skipping the trailing ":")
    // Skip empty fields (from consecutive spaces)
    let mut parents: Vec<Bound<'py, PyBytes>> = Vec::new();
    for &parent_field in &fields[4..fields.len() - 1] {
        if parent_field.is_empty() {
            continue;
        }
        if parent_field.first() == Some(&b'.') {
            // Explicit revision id (skip the leading '.')
            parents.push(PyBytes::new(py, &parent_field[1..]));
        } else {
            let idx_str = std::str::from_utf8(parent_field).map_err(|_| {
                PyValueError::new_err(format!("{:?} is not a valid integer", parent_field))
            })?;
            let idx: i64 = idx_str.parse().map_err(|_| {
                PyValueError::new_err(format!("{:?} is not a valid integer", idx_str))
            })?;
            if idx >= *history_len {
                return Err(PyIndexError::new_err(format!(
                    "Parent index refers to a revision which does not exist yet. {} > {}",
                    idx, *history_len
                )));
            }
            let parent = history.get_item(idx as usize)?;
            parents.push(parent.cast_into::<PyBytes>()?);
        }
    }
    let parents_tuple = PyTuple::new(py, &parents)?;

    // Check if version_id is already in cache
    let index: i64;
    if let Some(existing) = cache.get_item(&version_id)? {
        let existing_tuple = existing.cast_into::<PyTuple>()?;
        index = existing_tuple.get_item(5)?.extract()?;
    } else {
        history.append(&version_id)?;
        index = *history_len;
        *history_len += 1;
    }

    let pos_obj = pos.into_pyobject(py)?;
    let size_obj = size.into_pyobject(py)?;
    let index_obj = index.into_pyobject(py)?;
    let entry = PyTuple::new(
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
    cache.set_item(&version_id, &entry)?;

    Ok(true)
}

/// Load the knit index file into memory.
///
/// Successor to the Cython `_load_data_c`; the `_c` suffix is dropped
/// because the Rust extension is no longer C-shaped.
#[pyfunction]
pub fn _load_data(py: Python, kndx: &Bound<PyAny>, fp: &Bound<PyAny>) -> PyResult<()> {
    let cache = kndx.getattr("_cache")?;
    let cache = cache.cast_into::<PyDict>()?;
    let history = kndx.getattr("_history")?;
    let history = history.cast_into::<PyList>()?;

    // Call kndx.check_header(fp)
    kndx.call_method1("check_header", (fp,))?;

    // Read the entire file content
    let text = fp.call_method0("read")?;
    let text_bytes = text.cast_into::<PyBytes>()?;
    let data = text_bytes.as_bytes();

    let mut history_len = history.len() as i64;

    let knit_corrupt = py.import("bzrformats.knit")?.getattr("KnitCorrupt")?;
    let filename = kndx.getattr("_filename")?;

    // Process line by line
    for line in data.split(|&b| b == b'\n') {
        if line.is_empty() {
            continue;
        }
        // Strip trailing \r if present
        let line = if line.last() == Some(&b'\r') {
            &line[..line.len() - 1]
        } else {
            line
        };
        if line.is_empty() {
            continue;
        }

        match process_one_record(py, line, &history, &mut history_len, &cache) {
            Ok(_) => {}
            Err(e) => {
                // Wrap ValueError/IndexError in KnitCorrupt
                if e.is_instance_of::<PyValueError>(py) || e.is_instance_of::<PyIndexError>(py) {
                    let py_line = PyBytes::new(py, line);
                    let how = format!("line {:?}: {}", py_line, e);
                    let exc = knit_corrupt.call1((&filename, how))?;
                    return Err(PyErr::from_value(exc.unbind().into_bound(py)));
                }
                return Err(e);
            }
        }
    }

    Ok(())
}

fn knit_err_to_py(err: KnitError) -> PyErr {
    Python::attach(|py| -> PyErr {
        if let KnitError::Corrupt(ref msg) = err {
            if let Ok(cls) = py
                .import("bzrformats.knit")
                .and_then(|m| m.getattr("KnitCorrupt"))
            {
                if let Ok(exc) = cls.call1(("", msg.as_str())) {
                    return PyErr::from_value(exc.unbind().into_bound(py));
                }
            }
        }
        PyValueError::new_err(err.to_string())
    })
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
    };
    s.clone().into_any()
}

/// Extract an annotated-fulltext knit record to its plain text lines.
/// Returns a list of bytes objects. Mirrors
/// `bzrformats.knit.FTAnnotatedToFullText.get_bytes` (without the
/// final `b"".join` step that callers do based on storage_kind).
#[pyfunction]
fn extract_annotated_fulltext_to_plain_lines_rs<'py>(
    py: Python<'py>,
    raw_record: &[u8],
    noeol: bool,
) -> PyResult<Bound<'py, PyList>> {
    let lines = bazaar::knit::extract_annotated_fulltext_to_plain_lines(raw_record, noeol)
        .map_err(knit_err_to_py)?;
    let items: Vec<Bound<PyBytes>> = lines.iter().map(|l| PyBytes::new(py, l)).collect();
    PyList::new(py, items)
}

/// Extract a plain (already-unannotated) fulltext knit record to its
/// text lines. Mirrors `bzrformats.knit.FTPlainToFullText.get_bytes`.
#[pyfunction]
fn extract_plain_fulltext_lines_rs<'py>(
    py: Python<'py>,
    raw_record: &[u8],
    noeol: bool,
) -> PyResult<Bound<'py, PyList>> {
    let lines =
        bazaar::knit::extract_plain_fulltext_lines(raw_record, noeol).map_err(knit_err_to_py)?;
    let items: Vec<Bound<PyBytes>> = lines.iter().map(|l| PyBytes::new(py, l)).collect();
    PyList::new(py, items)
}

/// End-to-end recompression of an annotated-fulltext knit record into
/// an unannotated one. Mirrors
/// `bzrformats.knit.FTAnnotatedToUnannotated.get_bytes`.
#[pyfunction]
fn recompress_annotated_to_unannotated_fulltext_rs<'py>(
    py: Python<'py>,
    raw_record: &[u8],
) -> PyResult<Bound<'py, PyBytes>> {
    let out = bazaar::knit::recompress_annotated_to_unannotated_fulltext(raw_record)
        .map_err(knit_err_to_py)?;
    Ok(PyBytes::new(py, &out))
}

/// End-to-end recompression of an annotated-delta knit record into
/// an unannotated one. Mirrors
/// `bzrformats.knit.DeltaAnnotatedToUnannotated.get_bytes`.
#[pyfunction]
fn recompress_annotated_to_unannotated_delta_rs<'py>(
    py: Python<'py>,
    raw_record: &[u8],
) -> PyResult<Bound<'py, PyBytes>> {
    let out = bazaar::knit::recompress_annotated_to_unannotated_delta(raw_record)
        .map_err(knit_err_to_py)?;
    Ok(PyBytes::new(py, &out))
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
// is a Python object the access layer needs to dereference. The pure-
// Rust `KnitIndexMemo { path, offset, length }` doesn't carry arbitrary
// Python objects, so the index adapter stuffs each Python memo into a
// shared `Vec<Py<PyAny>>` and synthesises a `path = format!("py:{idx}")`
// pointing at the slot. The matching access adapter looks the slot up
// and calls `py_access.get_raw_records([memo])` to recover the bytes.
// Both adapters share the same `Arc<Mutex<...>>` so the round-trip
// works within one `get_text_rs` call.

use bazaar::knit::{
    get_content as rust_get_content, get_sha1s as rust_get_sha1s, get_text as rust_get_text,
};
use std::sync::{Arc, Mutex};

#[derive(Default)]
pub(crate) struct MemoTable {
    /// Original Python memo tuples, indexed by their slot in this Vec.
    memos: Vec<Py<PyAny>>,
}

impl MemoTable {
    fn intern(&mut self, memo: Py<PyAny>) -> usize {
        let idx = self.memos.len();
        self.memos.push(memo);
        idx
    }

    fn get(&self, idx: usize) -> Option<&Py<PyAny>> {
        self.memos.get(idx)
    }
}

fn slot_path(idx: usize) -> String {
    format!("py:{}", idx)
}

fn parse_slot_path(path: &str) -> Option<usize> {
    path.strip_prefix("py:").and_then(|s| s.parse().ok())
}

/// Adapter that exposes a Python `_KnitGraphIndex` / `_KndxIndex` as a
/// pure-Rust [`KnitIndexTrait`].
///
/// The Python `get_build_details(keys)` returns the dict shape
/// `{key: (index_memo, compression_parent, parents, (method, noeol))}`;
/// this adapter walks each entry, parks the opaque Python `index_memo`
/// in the shared `MemoTable`, and builds a `KnitRecordDetails` with a
/// synthetic `KnitIndexMemo` whose path points back at the slot.
pub struct PyKnitIndex {
    py_index: Py<PyAny>,
    table: Arc<Mutex<MemoTable>>,
}

impl PyKnitIndex {
    pub fn new(py_index: Bound<'_, PyAny>, table: Arc<Mutex<MemoTable>>) -> Self {
        Self {
            py_index: py_index.unbind(),
            table,
        }
    }
}

fn knit_err_from_py(py: Python<'_>, err: PyErr) -> KnitError {
    pyo3::import_exception!(bzrformats.errors, RevisionNotPresent);
    if err.is_instance_of::<RevisionNotPresent>(py) {
        return KnitError::BadIndexValue(err.to_string().into_bytes());
    }
    KnitError::BadIndexValue(err.to_string().into_bytes())
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
    fn get_build_details(
        &self,
        keys: &[KnitKey],
    ) -> Result<std::collections::HashMap<KnitKey, KnitRecordDetails>, KnitError> {
        Python::attach(
            |py| -> Result<std::collections::HashMap<KnitKey, KnitRecordDetails>, KnitError> {
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

                    // Pull (pos, size) out of the index_memo tuple — the
                    // first element is the opaque GraphIndex/prefix, which
                    // we park in the side table.
                    let memo_tup = py_memo.clone().cast_into::<PyTuple>().map_err(|_| {
                        KnitError::BadIndexValue(b"index_memo is not a tuple".to_vec())
                    })?;
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
                    let slot = self.table.lock().unwrap().intern(py_memo.unbind());
                    let index_memo = KnitIndexMemo {
                        path: slot_path(slot),
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
}

/// Adapter that exposes a Python `_KnitKeyAccess` / `_DirectPackAccess`
/// as a pure-Rust [`KnitAccessTrait`].
///
/// Looks each `KnitIndexMemo` up in the shared [`MemoTable`] (where
/// the matching [`PyKnitIndex`] parked the original Python memo
/// tuple), then calls `py_access.get_raw_records([memo])` and reads
/// the first item from the returned iterator.
pub struct PyKnitAccess {
    py_access: Py<PyAny>,
    table: Arc<Mutex<MemoTable>>,
}

impl PyKnitAccess {
    pub fn new(py_access: Bound<'_, PyAny>, table: Arc<Mutex<MemoTable>>) -> Self {
        Self {
            py_access: py_access.unbind(),
            table,
        }
    }
}

impl KnitAccessTrait for PyKnitAccess {
    fn get_raw_record(&self, memo: &KnitIndexMemo) -> Result<Vec<u8>, KnitError> {
        Python::attach(|py| -> Result<Vec<u8>, KnitError> {
            let slot = parse_slot_path(&memo.path)
                .ok_or_else(|| KnitError::BadIndexValue(memo.path.as_bytes().to_vec()))?;
            let table = self.table.lock().unwrap();
            let py_memo = table
                .get(slot)
                .ok_or_else(|| KnitError::BadIndexValue(memo.path.as_bytes().to_vec()))?
                .clone_ref(py);
            drop(table);

            let memos_list = pyo3::types::PyList::empty(py);
            memos_list
                .append(py_memo.bind(py))
                .map_err(|e| knit_err_from_py(py, e))?;
            let iter = self
                .py_access
                .bind(py)
                .call_method1("get_raw_records", (memos_list,))
                .map_err(|e| knit_err_from_py(py, e))?;
            let mut iter = iter.try_iter().map_err(|e| knit_err_from_py(py, e))?;
            let first = iter
                .next()
                .ok_or_else(|| {
                    KnitError::BadIndexValue(b"get_raw_records returned no items".to_vec())
                })?
                .map_err(|e| knit_err_from_py(py, e))?;
            let bytes = first.cast_into::<PyBytes>().map_err(|_| {
                KnitError::BadIndexValue(b"get_raw_records yielded non-bytes".to_vec())
            })?;
            Ok(bytes.as_bytes().to_vec())
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
    let table = Arc::new(Mutex::new(MemoTable::default()));
    let index = PyKnitIndex::new(py_index, table.clone());
    let access = PyKnitAccess::new(py_access, table);
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
    let table = Arc::new(Mutex::new(MemoTable::default()));
    let index = PyKnitIndex::new(py_index, table.clone());
    let access = PyKnitAccess::new(py_access, table);
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
    let table = Arc::new(Mutex::new(MemoTable::default()));
    let index = PyKnitIndex::new(py_index, table.clone());
    let access = PyKnitAccess::new(py_access, table);

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

impl PyAnnotatedKnitContent {
    fn line_delta_iter_impl<'py>(
        slf: PyRef<'_, Self>,
        py: Python<'py>,
        new_lines: &Bound<'py, PyAny>,
    ) -> PyResult<Vec<Bound<'py, PyTuple>>> {
        // line_delta_iter uses patiencediff, a Python library — call back into Python.
        let patiencediff = py.import("patiencediff")?;
        let old_texts = {
            let lines = slf.0.text();
            let items: Vec<Bound<PyBytes>> = lines.iter().map(|l| PyBytes::new(py, l)).collect();
            PyList::new(py, items)?
        };
        // new_lines can be either a PyAnnotatedKnitContent or PyPlainKnitContent
        let new_texts = new_lines.call_method0("text")?;
        let new_lines_list = new_lines.getattr("_lines")?;

        let matcher = patiencediff.getattr("PatienceSequenceMatcher")?.call1((
            py.None(),
            old_texts,
            new_texts,
        ))?;
        let opcodes = matcher.call_method0("get_opcodes")?;

        let mut out = Vec::new();
        for opcode in opcodes.try_iter()? {
            let op = opcode?;
            let tag: String = op.get_item(0)?.extract()?;
            if tag == "equal" {
                continue;
            }
            let i1: usize = op.get_item(1)?.extract()?;
            let i2: usize = op.get_item(2)?.extract()?;
            let j1: usize = op.get_item(3)?.extract()?;
            let j2: usize = op.get_item(4)?.extract()?;
            let count = j2 - j1;
            let slice = new_lines_list.get_item(pyo3::types::PySlice::new(
                py,
                j1 as isize,
                j2 as isize,
                1,
            ))?;
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
        let patiencediff = py.import("patiencediff")?;
        let old_texts = {
            let lines = slf.0.text();
            let items: Vec<Bound<PyBytes>> = lines.iter().map(|l| PyBytes::new(py, l)).collect();
            PyList::new(py, items)?
        };
        let new_texts = new_lines.call_method0("text")?;
        let new_lines_list = new_lines.getattr("_lines")?;

        let matcher = patiencediff.getattr("PatienceSequenceMatcher")?.call1((
            py.None(),
            old_texts,
            new_texts,
        ))?;
        let opcodes = matcher.call_method0("get_opcodes")?;

        let mut out = Vec::new();
        for opcode in opcodes.try_iter()? {
            let op = opcode?;
            let tag: String = op.get_item(0)?.extract()?;
            if tag == "equal" {
                continue;
            }
            let i1: usize = op.get_item(1)?.extract()?;
            let i2: usize = op.get_item(2)?.extract()?;
            let j1: usize = op.get_item(3)?.extract()?;
            let j2: usize = op.get_item(4)?.extract()?;
            let count = j2 - j1;
            let slice = new_lines_list.get_item(pyo3::types::PySlice::new(
                py,
                j1 as isize,
                j2 as isize,
                1,
            ))?;
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
        knit: Bound<'py, PyAny>,
        key: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        // Plain factory delegates to _KnitAnnotator.annotate_flat
        let annotator_class = py.import("bzrformats.knit")?.getattr("_KnitAnnotator")?;
        let annotator = annotator_class.call1((knit,))?;
        annotator.call_method1("annotate_flat", (key,))
    }
}

fn transport_err_to_py(e: bazaar::transport::TransportError) -> PyErr {
    PyValueError::new_err(e.to_string())
}

fn kndx_load_err_to_py(py: Python<'_>, e: KndxLoadError) -> PyErr {
    match e {
        KndxLoadError::Transport(te) => transport_err_to_py(te),
        KndxLoadError::Knit(ke) => match &ke {
            bazaar::knit::KnitError::BadKnitHeader { path } => py
                .import("bzrformats.knit")
                .and_then(|m| m.getattr("KnitHeaderError"))
                .and_then(|cls| {
                    let badline = pyo3::types::PyBytes::new(py, b"");
                    cls.call1((badline, path.as_str()))
                })
                .map(|exc| PyErr::from_value(exc.unbind().into_bound(py)))
                .unwrap_or_else(|import_err| import_err),
            bazaar::knit::KnitError::KndxCorrupt { line, detail } => py
                .import("bzrformats.knit")
                .and_then(|m| m.getattr("KnitCorrupt"))
                .and_then(|cls| {
                    let py_line = pyo3::types::PyBytes::new(py, line);
                    cls.call1((py_line, detail.as_str()))
                })
                .map(|exc| PyErr::from_value(exc.unbind().into_bound(py)))
                .unwrap_or_else(|import_err| import_err),
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
            return Err(PyValueError::new_err("object not locked"));
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
        let (method, _noeol) = bazaar::knit::decode_kndx_options(&refs).map_err(|_| {
            py.import("bzrformats.knit")
                .and_then(|m| m.getattr("KnitIndexUnknownMethod"))
                .and_then(|cls| cls.call1((self.transport_obj.bind(py), options.bind(py))))
                .map(|exc| PyErr::from_value(exc.unbind().into_bound(py)))
                .unwrap_or_else(|e| e)
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

    fn get_missing_compression_parents(&self) -> PyResult<()> {
        Err(PyNotImplementedError::new_err(
            "get_missing_compression_parents",
        ))
    }

    fn check_header(&self, py: Python<'_>, fp: Bound<'_, PyAny>) -> PyResult<()> {
        let line = fp.call_method0("readline")?;
        let line = line
            .cast_into::<PyBytes>()
            .map_err(|_| PyValueError::new_err("check_header: expected bytes from readline"))?;
        if line.as_bytes().is_empty() {
            let err_cls = py.import("bzrformats.errors")?.getattr("NoSuchFile")?;
            let exc = err_cls.call1((py.None(),))?;
            return Err(PyErr::from_value(exc.unbind().into_bound(py)));
        }
        if line.as_bytes() != bazaar::knit::KNDX_HEADER {
            let err_cls = py.import("bzrformats.knit")?.getattr("KnitHeaderError")?;
            let exc = err_cls.call1((line, py.None()))?;
            return Err(PyErr::from_value(exc.unbind().into_bound(py)));
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

pyo3::import_exception!(bzrformats.errors, ReadOnlyError);
pyo3::import_exception!(bzrformats.errors, ObjectNotLocked);

/// pyo3 wrapper that exposes `_KnitGraphIndex` to Python.
///
/// Wraps a Python `CombinedGraphIndex` (or any compatible graph index) and
/// implements the same public interface as the Python `_KnitGraphIndex` class.
/// All graph-index operations are delegated back to the wrapped Python object;
/// only the knit-specific encoding/decoding logic runs in Rust.
#[pyclass(name = "_KnitGraphIndex")]
pub struct PyKnitGraphIndex {
    graph_index: Py<PyAny>,
    is_locked: Py<PyAny>,
    deltas: bool,
    parents: bool,
    add_callback: Option<Py<PyAny>>,
    /// Keys of compression parents that are referenced but not yet present
    /// in any scanned index.
    missing_compression_parents: std::collections::HashSet<bazaar::knit::KnitKey>,
    /// Optional external-parent-ref tracker (used when
    /// `track_external_parent_refs=True`).
    key_dependencies: Option<Py<PyAny>>,
}

#[pymethods]
impl PyKnitGraphIndex {
    #[new]
    #[pyo3(signature = (graph_index, is_locked, deltas=false, parents=true, add_callback=None, track_external_parent_refs=false))]
    fn new(
        py: Python<'_>,
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
        let key_dependencies = if track_external_parent_refs {
            let cls = py.import("bzrformats.versionedfile")?.getattr("_KeyRefs")?;
            Some(cls.call0()?.unbind())
        } else {
            None
        };
        Ok(Self {
            graph_index: graph_index.unbind(),
            is_locked: is_locked.unbind(),
            deltas,
            parents,
            add_callback: add_callback.map(|c| c.unbind()),
            missing_compression_parents: std::collections::HashSet::new(),
            key_dependencies,
        })
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let gi_repr = self.graph_index.bind(py).repr()?;
        Ok(format!("_KnitGraphIndex({})", gi_repr))
    }

    #[getter]
    fn has_graph(&self) -> bool {
        self.parents
    }

    #[getter]
    fn _graph_index(&self, py: Python<'_>) -> Py<PyAny> {
        self.graph_index.clone_ref(py)
    }

    fn _check_read(&self, py: Python<'_>) -> PyResult<()> {
        if !self.is_locked.bind(py).call0()?.is_truthy()? {
            let exc = ObjectNotLocked::new_err(py.None());
            return Err(exc);
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
        if self.parents {
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
        knit_entries_to_build_details_rs(py, entries, self.parents, self.deltas)
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
        let result = self.get_parent_map(py, key.clone().into_any())?;
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
        // positions[key] = (record_details, index_memo, next, parents)
        // index_memo = (graph_index_obj, pos, size)
        // Group by the graph_index_obj's identity (ptr), sort by offset.
        let n = keys.len();
        let mut keyed: Vec<(usize, u64, Bound<'_, PyAny>)> = Vec::with_capacity(n);
        for i in 0..n {
            let k = keys.get_item(i)?;
            let pos_entry = positions
                .get_item(&k)?
                .ok_or_else(|| PyValueError::new_err("_sort_keys_by_io: key not in positions"))?;
            let index_memo = pos_entry.get_item(1)?;
            let file_ref = index_memo.get_item(0)?;
            // Use the Python object's id() as grouping key — stable for the
            // lifetime of this call, and unique per GraphIndex shard.
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
            self.missing_compression_parents
                .iter()
                .map(|k| py_knit_key_to_py(py, k))
                .collect::<PyResult<Vec<_>>>()?,
        )?;
        Ok(s.into_any())
    }

    fn get_missing_parents<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let Some(kd) = &self.key_dependencies else {
            return Ok(pyo3::types::PyFrozenSet::empty(py)?.into_any());
        };
        let kd = kd.bind(py);
        let unsatisfied = kd.call_method0("get_unsatisfied_refs")?;
        let parent_map = self.get_parent_map(py, unsatisfied.clone())?;
        kd.call_method1("satisfy_refs_for_keys", (parent_map,))?;
        let remaining = kd.call_method0("get_unsatisfied_refs")?;
        let s = pyo3::types::PyFrozenSet::new(
            py,
            remaining.try_iter()?.collect::<PyResult<Vec<_>>>()?,
        )?;
        Ok(s.into_any())
    }

    fn scan_unvalidated_index(
        &mut self,
        py: Python<'_>,
        graph_index: Bound<'_, PyAny>,
    ) -> PyResult<()> {
        if self.deltas {
            let new_missing = graph_index.call_method1("external_references", (1usize,))?;
            let new_missing_keys = extract_py_knit_keys(&new_missing)?;
            let parent_map = self.get_parent_map(py, new_missing.clone())?;
            let present_keys: std::collections::HashSet<bazaar::knit::KnitKey> = parent_map
                .keys()
                .try_iter()?
                .map(|k| extract_py_knit_key(&k?))
                .collect::<PyResult<_>>()?;
            for key in new_missing_keys {
                if !present_keys.contains(&key) {
                    self.missing_compression_parents.insert(key);
                }
            }
        }
        if let Some(kd) = &self.key_dependencies {
            let kd = kd.bind(py);
            for node in graph_index.call_method0("iter_all_entries")?.try_iter()? {
                let node = node?.cast_into::<PyTuple>()?;
                let key = node.get_item(1)?;
                let refs = node.get_item(3)?;
                let parent_refs = refs.get_item(0)?;
                kd.call_method1("add_references", (key, parent_refs))?;
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
        let Some(add_callback) = &self.add_callback else {
            let exc_cls = py.import("bzrformats.errors")?.getattr("ReadOnlyError")?;
            return Err(PyErr::from_value(exc_cls.call1((py.None(),))?));
        };
        let add_callback = add_callback.clone_ref(py);

        type KnitKey = bazaar::knit::KnitKey;
        // Ordered vec preserving insertion order for the callback; dedup by key.
        let mut entries: Vec<(KnitKey, Vec<u8>, Vec<Vec<KnitKey>>)> = Vec::new();
        let mut new_compression_parents: std::collections::HashSet<KnitKey> =
            std::collections::HashSet::new();

        for rec in records.try_iter()? {
            let rec = rec?.cast_into::<PyTuple>()?;
            let key = extract_py_knit_key_or_bytes(&rec.get_item(0)?)?;
            let options_obj = rec.get_item(1)?;
            // options is a bytes string like b"fulltext,no-eol" or a list of bytes
            let options_bytes: Vec<u8> = if let Ok(b) = options_obj.clone().cast_into::<PyBytes>() {
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
            let noeol = options_bytes.windows(6).any(|w| w == b"no-eol");
            let method = if options_bytes.windows(10).any(|w| w == b"line-delta") {
                bazaar::knit::KnitMethod::LineDelta
            } else {
                bazaar::knit::KnitMethod::Fulltext
            };
            // access_memo = (index_obj, pos, size) — only pos and size matter here
            let memo = rec.get_item(2)?;
            let pos: u64 = memo.get_item(1)?.extract()?;
            let size: u64 = memo.get_item(2)?.extract()?;
            let parents_obj = rec.get_item(3)?;
            let parents: Vec<KnitKey> = parents_obj
                .try_iter()?
                .map(|p| extract_py_knit_key_or_bytes(&p?))
                .collect::<PyResult<_>>()?;

            let (value, node_refs) = bazaar::knit::encode_graph_index_record(
                noeol,
                pos,
                size,
                method,
                self.parents,
                self.deltas,
                &parents,
            )
            .map_err(knit_err_to_py)?;

            if let Some(kd) = &self.key_dependencies {
                let py_parents = PyTuple::new(
                    py,
                    parents
                        .iter()
                        .map(|p| py_knit_key_to_py(py, p))
                        .collect::<PyResult<Vec<_>>>()?,
                )?;
                kd.bind(py)
                    .call_method1("add_references", (py_knit_key_to_py(py, &key)?, py_parents))?;
            }

            if missing_compression_parents && method == bazaar::knit::KnitMethod::LineDelta {
                if let Some(cp) = parents.first() {
                    new_compression_parents.insert(cp.clone());
                }
            }

            // Last entry for a key wins (matches Python dict semantics).
            if let Some(existing) = entries.iter_mut().find(|(k, _, _)| k == &key) {
                *existing = (key, value, node_refs);
            } else {
                entries.push((key, value, node_refs));
            }
        }

        // Dedup check: look up entries that already exist in the index.
        if !random_id {
            let py_keys = pyo3::types::PyList::new(
                py,
                entries
                    .iter()
                    .map(|(k, _, _)| py_knit_key_to_py(py, k))
                    .collect::<PyResult<Vec<_>>>()?,
            )?;
            let existing = self._get_entries(py, py_keys.as_any())?;
            let existing = existing.bind(py);
            let mut to_remove: std::collections::HashSet<KnitKey> =
                std::collections::HashSet::new();
            for node in existing.try_iter()? {
                let node = node?.cast_into::<PyTuple>()?;
                let existing_key = extract_py_knit_key(&node.get_item(1)?)?;
                let existing_value = node.get_item(2)?.cast_into::<PyBytes>()?;
                let existing_refs = node.get_item(3)?;

                let Some((_, new_value, new_refs)) =
                    entries.iter().find(|(k, _, _)| k == &existing_key)
                else {
                    continue;
                };

                // Compare noeol flag byte only — pos/size differ per pack.
                let existing_flag = existing_value.as_bytes().first().copied().unwrap_or(b' ');
                let new_flag: u8 = new_value.first().copied().unwrap_or(b' ');
                // Compare first ref list (parents) for consistency.
                let existing_parents: Vec<KnitKey> = existing_refs
                    .get_item(0)
                    .ok()
                    .map(|rl| {
                        rl.try_iter()?
                            .map(|k| extract_py_knit_key(&k?))
                            .collect::<PyResult<Vec<_>>>()
                    })
                    .transpose()?
                    .unwrap_or_default();
                let new_parents: &[KnitKey] = new_refs.first().map(|v| v.as_slice()).unwrap_or(&[]);
                if existing_flag != new_flag || existing_parents.as_slice() != new_parents {
                    return Err(knit_err_to_py(bazaar::knit::KnitError::Corrupt(format!(
                        "inconsistent details in add_records: \
                         existing flag={:?} new flag={:?}",
                        existing_flag as char, new_flag as char,
                    ))));
                }
                to_remove.insert(existing_key);
            }
            entries.retain(|(k, _, _)| !to_remove.contains(k));
        }

        // Build the argument list for add_callback.
        let result = pyo3::types::PyList::empty(py);
        if self.parents {
            for (key, value, node_refs) in &entries {
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
            for (key, value, _) in &entries {
                let py_key = py_knit_key_to_py(py, key)?;
                let py_value = PyBytes::new(py, value);
                result.append(PyTuple::new(py, [py_key.into_any(), py_value.into_any()])?)?;
            }
        }
        add_callback.bind(py).call1((result,))?;

        // Update missing-compression-parent tracking.
        let added_keys: std::collections::HashSet<&KnitKey> =
            entries.iter().map(|(k, _, _)| k).collect();
        if missing_compression_parents {
            new_compression_parents.retain(|k| !added_keys.contains(k));
            self.missing_compression_parents
                .extend(new_compression_parents);
        }
        self.missing_compression_parents
            .retain(|k| !added_keys.contains(k));
        Ok(())
    }
}

impl PyKnitGraphIndex {
    /// Call `graph_index.iter_entries(keys)`, adapting parentless indices by
    /// appending an empty refs tuple. Returns an unbound `Py<PyAny>` (a list)
    /// so callers can rebind it to any lifetime.
    fn _get_entries(&self, py: Python<'_>, keys: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let gi = self.graph_index.bind(py);
        if self.parents {
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
        if !self.deltas {
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
            .add_raw_record(rust_key, &data)
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
                .add_raw_record(key, slice)
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
        use bazaar::knit::{KnitAccess, KnitIndexMemo};
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

fn extract_py_knit_keys(obj: &Bound<'_, PyAny>) -> PyResult<Vec<bazaar::knit::KnitKey>> {
    let mut keys = Vec::new();
    for item in obj.try_iter()? {
        keys.push(extract_py_knit_key(&item?)?);
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
    m.add_function(wrap_pyfunction!(
        recompress_annotated_to_unannotated_fulltext_rs,
        &m
    )?)?;
    m.add_function(wrap_pyfunction!(
        recompress_annotated_to_unannotated_delta_rs,
        &m
    )?)?;
    m.add_function(wrap_pyfunction!(
        extract_annotated_fulltext_to_plain_lines_rs,
        &m
    )?)?;
    m.add_function(wrap_pyfunction!(extract_plain_fulltext_lines_rs, &m)?)?;
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
    m.add_class::<PyKndxIndex>()?;
    m.add_class::<PyKnitGraphIndex>()?;
    m.add_class::<PyKnitKeyAccess>()?;
    Ok(m)
}
