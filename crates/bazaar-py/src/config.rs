//! pyo3 binding for `bzrformats.config.configobj`.
//!
//! Exposes the pure-Rust [`bazaar::config::ConfigObj`] parser/writer, its
//! read-only [`Section`] view, and the `quote_value`/`unquote_value` helpers.
//! This is the parsing layer breezy's `config.py` consumes; the `Store`/`Stack`
//! layers stay in the pure-Rust crate for now.

use bazaar::config::{ConfigObj as RsConfigObj, ConfigObjError, Section as RsSection};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyBytes;

fn parse_error_to_py(e: ConfigObjError) -> PyErr {
    PyValueError::new_err(e.to_string())
}

/// A read view of one config section.
///
/// Mirrors `bzrformats.config.configobj.Section`: an ordered `{name: value}`
/// map plus the section id (`None` for the top-level no-name section). Values
/// are raw (with `list_values=False`, surrounding quotes are kept).
#[pyclass(name = "Section", module = "bzrformats._bzr_rs.config", frozen)]
struct Section {
    inner: RsSection,
}

#[pymethods]
impl Section {
    /// The section id; `None` for the no-name (top-level) section.
    #[getter]
    fn id(&self) -> Option<&str> {
        self.inner.id()
    }

    /// The raw value for `name`, or `None` if absent.
    fn get(&self, name: &str) -> Option<&str> {
        self.inner.get(name)
    }

    /// Option names in file order.
    fn option_names(&self) -> Vec<String> {
        self.inner.iter_option_names().map(str::to_string).collect()
    }

    fn __contains__(&self, name: &str) -> bool {
        self.inner.get(name).is_some()
    }

    fn __repr__(&self) -> String {
        match self.inner.id() {
            Some(id) => format!("<Section {id:?}>"),
            None => "<Section (no name)>".to_string(),
        }
    }
}

/// A parsed ConfigObj file.
///
/// Mirrors `bzrformats.config.configobj.ConfigObj`. Holds the lines in order;
/// lookups and mutations work through the section/key model while the physical
/// line order is preserved for writing.
#[pyclass(name = "ConfigObj", module = "bzrformats._bzr_rs.config")]
struct ConfigObj {
    inner: RsConfigObj,
}

#[pymethods]
impl ConfigObj {
    /// An empty config (no lines).
    #[new]
    fn new() -> Self {
        ConfigObj {
            inner: RsConfigObj::empty(),
        }
    }

    /// Parse `data` (UTF-8) into a config.
    #[staticmethod]
    fn parse(data: &[u8]) -> PyResult<Self> {
        let inner = RsConfigObj::parse(data).map_err(parse_error_to_py)?;
        Ok(ConfigObj { inner })
    }

    /// All sections in file order: the no-name section first (if it has any
    /// scalars), then each named section.
    fn sections(&self) -> Vec<Section> {
        self.inner
            .sections()
            .into_iter()
            .map(|inner| Section { inner })
            .collect()
    }

    /// The section with the given id (`None` = no-name), or `None` if it has no
    /// entries.
    #[pyo3(signature = (id=None))]
    fn section(&self, id: Option<&str>) -> Option<Section> {
        self.inner.section(id).map(|inner| Section { inner })
    }

    /// Set `key` to `value` in section `id`, in place if it already exists,
    /// otherwise appended to that section (creating the header if needed).
    #[pyo3(signature = (id, key, value))]
    fn set_value(&mut self, id: Option<&str>, key: &str, value: &str) {
        self.inner.set_value(id, key, value);
    }

    /// Remove `key` from section `id` if present.
    #[pyo3(signature = (id, key))]
    fn remove_value(&mut self, id: Option<&str>, key: &str) {
        self.inner.remove_value(id, key);
    }

    /// Serialize back to bytes (UTF-8), newline-terminated.
    fn to_bytes<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.inner.to_bytes())
    }
}

/// Quote `value` for writing, matching breezy's list-aware `Store.quote`.
#[pyfunction]
fn quote_value(value: &str) -> String {
    bazaar::config::quote_value(value)
}

/// Strip a matched surrounding quote pair from a raw value, as `Store.unquote`.
#[pyfunction]
fn unquote_value(value: &str) -> String {
    bazaar::config::unquote_value(value)
}

pub(crate) fn _config_rs(py: Python) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "config")?;
    m.add_class::<ConfigObj>()?;
    m.add_class::<Section>()?;
    m.add_wrapped(wrap_pyfunction!(quote_value))?;
    m.add_wrapped(wrap_pyfunction!(unquote_value))?;
    Ok(m)
}
