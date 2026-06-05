use bazaar::textinv;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

/// URL-like escape so a value never contains a space.
#[pyfunction]
fn escape(s: &str) -> String {
    textinv::escape(s)
}

/// Inverse of `escape`. Raises ValueError if the input contains a space.
#[pyfunction]
fn unescape(s: &str) -> PyResult<String> {
    textinv::unescape(s)
        .ok_or_else(|| pyo3::exceptions::PyValueError::new_err("escaped value contains a space"))
}

/// Serialise inventory `entries` as a text inventory.
///
/// Each entry is `(file_id, name, kind, parent_id)` for non-files, or
/// `(file_id, name, "file", parent_id, text_id, text_sha1, text_size)` for
/// files. `file_id`, `parent_id`, `text_id` and `text_sha1` are bytes;
/// `name` and `kind` are str; `text_size` is an int.
#[pyfunction]
fn write_text_inventory<'py>(
    py: Python<'py>,
    entries: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyBytes>> {
    let mut parsed = Vec::new();
    for item in entries.try_iter()? {
        let t = item?;
        let file_id: Vec<u8> = t.get_item(0)?.extract()?;
        let name: String = t.get_item(1)?.extract()?;
        let kind: String = t.get_item(2)?.extract()?;
        let parent_id: Vec<u8> = t.get_item(3)?.extract()?;
        let file_details = if kind == "file" {
            Some(textinv::FileDetails {
                text_id: t.get_item(4)?.extract()?,
                text_sha1: t.get_item(5)?.extract()?,
                text_size: t.get_item(6)?.extract()?,
            })
        } else {
            None
        };
        parsed.push(textinv::TextInvEntry {
            file_id,
            name,
            kind,
            parent_id,
            file_details,
        });
    }
    Ok(PyBytes::new(py, &textinv::write_text_inventory(&parsed)))
}

pub(crate) fn _textinv_rs(py: Python) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "textinv")?;
    m.add("START_MARK", PyBytes::new(py, textinv::START_MARK))?;
    m.add("END_MARK", PyBytes::new(py, textinv::END_MARK))?;
    m.add_function(wrap_pyfunction!(escape, &m)?)?;
    m.add_function(wrap_pyfunction!(unescape, &m)?)?;
    m.add_function(wrap_pyfunction!(write_text_inventory, &m)?)?;
    Ok(m)
}
