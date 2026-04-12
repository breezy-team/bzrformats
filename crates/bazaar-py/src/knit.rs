use bazaar::knit::{
    lower_fulltext, lower_line_delta_annotated, lower_line_delta_raw, parse_fulltext,
    parse_line_delta_annotated, parse_line_delta_plain, parse_line_delta_raw, AnnotatedLine,
    DeltaHunk, KnitError,
};
use pyo3::exceptions::{PyIndexError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyTuple};

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
    Ok(m)
}
