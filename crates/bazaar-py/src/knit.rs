use bazaar::knit::{
    lower_fulltext, lower_line_delta_annotated, lower_line_delta_raw, parse_fulltext,
    parse_line_delta_annotated, parse_line_delta_plain, parse_line_delta_raw,
    parse_network_record_header, AnnotatedLine, DeltaHunk, KnitAccess as KnitAccessTrait,
    KnitAnnotateFactory, KnitError, KnitIndex as KnitIndexTrait, KnitIndexMemo, KnitKey,
    KnitMethod, KnitPlainFactory, KnitRecordDetails,
};
use pyo3::exceptions::{PyIndexError, PyValueError};
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
            parents.push(parent.downcast_into::<PyBytes>()?);
        }
    }
    let parents_tuple = PyTuple::new(py, &parents)?;

    // Check if version_id is already in cache
    let index: i64;
    if let Some(existing) = cache.get_item(&version_id)? {
        let existing_tuple = existing.downcast_into::<PyTuple>()?;
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
/// This is the Rust equivalent of _load_data_c from the Cython extension.
#[pyfunction]
pub fn _load_data_c(py: Python, kndx: &Bound<PyAny>, fp: &Bound<PyAny>) -> PyResult<()> {
    let cache = kndx.getattr("_cache")?;
    let cache = cache.downcast_into::<PyDict>()?;
    let history = kndx.getattr("_history")?;
    let history = history.downcast_into::<PyList>()?;

    // Call kndx.check_header(fp)
    kndx.call_method1("check_header", (fp,))?;

    // Read the entire file content
    let text = fp.call_method0("read")?;
    let text_bytes = text.downcast_into::<PyBytes>()?;
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
    PyValueError::new_err(err.to_string())
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

// ============================================================
// PyKnitIndex / PyKnitAccess adapters
// ============================================================
//
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
    KnitContent as KnitContentTrait,
};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct MemoTable {
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

pub(crate) fn _knit_rs(py: Python) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "knit")?;
    m.add_function(wrap_pyfunction!(_load_data_c, &m)?)?;
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
    Ok(m)
}
