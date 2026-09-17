//! pyo3 binding for the `views` working-tree control file.
//!
//! Exposes the [`bazaar::views`] codec as the pair of functions breezy's
//! `PathBasedViews` delegates its serialization to. The view manager itself
//! (locking, caching, the `NoSuchView`/`ViewsNotSupported` errors) stays in
//! Python, because it is working-tree plumbing rather than a file format.

use bazaar::views::{Keywords, ViewDict, ViewsError};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};

/// Map a parse failure onto the `ValueError` breezy's callers expect.
fn to_py_err(content: &[u8], e: ViewsError) -> PyErr {
    match e {
        // These two are raised before the content-level wrapper in breezy, so
        // their messages stand alone.
        ViewsError::MissingMarker | ViewsError::UnsupportedFormat(_) => {
            PyValueError::new_err(e.to_string())
        }
        _ => PyValueError::new_err(format!(
            "failed to deserialize views content {}: {e}",
            PyBytesRepr(content)
        )),
    }
}

/// Render bytes the way Python's `repr()` would, for error messages.
struct PyBytesRepr<'a>(&'a [u8]);

impl std::fmt::Display for PyBytesRepr<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "b'")?;
        for &b in self.0 {
            match b {
                b'\n' => write!(f, "\\n")?,
                b'\r' => write!(f, "\\r")?,
                b'\t' => write!(f, "\\t")?,
                b'\\' => write!(f, "\\\\")?,
                b'\'' => write!(f, "\\'")?,
                0x20..=0x7e => write!(f, "{}", b as char)?,
                _ => write!(f, "\\x{b:02x}")?,
            }
        }
        write!(f, "'")
    }
}

/// Convert view keywords and a view dictionary into views file content.
///
/// `keywords` is read in iteration order, so a dict read back from
/// [`deserialize_view_content`] re-serialises to the same bytes.
#[pyfunction]
fn serialize_view_content<'py>(
    py: Python<'py>,
    keywords: &Bound<'py, PyDict>,
    view_dict: ViewDict,
) -> PyResult<Bound<'py, PyBytes>> {
    let mut kw = Keywords::new();
    for (key, value) in keywords.iter() {
        kw.insert(key.extract()?, value.extract()?);
    }
    Ok(PyBytes::new(py, &bazaar::views::serialize(&kw, &view_dict)))
}

/// Convert views file content into view keywords and a dictionary of views.
#[pyfunction]
fn deserialize_view_content<'py>(
    py: Python<'py>,
    view_content: &[u8],
) -> PyResult<(Bound<'py, PyDict>, ViewDict)> {
    let (keywords, views) =
        bazaar::views::deserialize(view_content).map_err(|e| to_py_err(view_content, e))?;
    // A dict preserves insertion order, so the keyword file order survives.
    let kw = PyDict::new(py);
    for (key, value) in &keywords {
        kw.set_item(key, value)?;
    }
    Ok((kw, views))
}

/// Get the display string for a list of view files.
#[pyfunction]
fn view_display_str(view_files: Vec<String>) -> String {
    bazaar::views::view_display_str(&view_files)
}

pub(crate) fn _views_rs(py: Python) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "views")?;
    m.add_wrapped(wrap_pyfunction!(serialize_view_content))?;
    m.add_wrapped(wrap_pyfunction!(deserialize_view_content))?;
    m.add_wrapped(wrap_pyfunction!(view_display_str))?;
    Ok(m)
}
