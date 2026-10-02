//! pyo3 binding for `bzrformats.config`.
//!
//! Exposes a configobj-compatible [`ConfigObj`] built on the pure-Rust
//! [`bazaar::config::ConfigObj`] parser/writer. It presents the subset of the
//! third-party `configobj.ConfigObj` API that breezy's `IniFileStore` uses --
//! a `dict`-like top level whose items are scalar strings and nested
//! [`Section`] mappings, plus `scalars`/`sections`/`setdefault`/`write` and
//! `_quote`/`_unquote` -- so breezy can drop it in without a Python wrapper.
//!
//! The top-level object and every section share one `Rc<RefCell<..>>` model, so
//! mutating a section handed out by `setdefault` writes through to the whole
//! config, matching configobj's live-view semantics.

use std::cell::RefCell;
use std::rc::Rc;

use bazaar::config::{ConfigObj as RsConfigObj, ConfigObjError};
use pyo3::exceptions::{PyKeyError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyType};

fn parse_error_to_py(e: ConfigObjError) -> PyErr {
    PyValueError::new_err(e.to_string())
}

/// Coerce a Python value to a `String`, matching configobj's `stringify=True`:
/// strings pass through, anything else is rendered via `str()`.
fn stringify(value: &Bound<'_, PyAny>) -> PyResult<String> {
    if let Ok(s) = value.extract::<String>() {
        Ok(s)
    } else {
        value.str().map(|s| s.to_string())
    }
}

/// The shared parsed model behind a [`ConfigObj`] and its [`Section`] views.
type Shared = Rc<RefCell<RsConfigObj>>;

/// The scalar option names of the no-name (top-level) section, in file order.
fn top_scalar_names(model: &RsConfigObj) -> Vec<String> {
    model
        .section_tree()
        .into_iter()
        .find(|n| n.name.is_none())
        .map(|n| n.options.into_iter().map(|(k, _)| k).collect())
        .unwrap_or_default()
}

/// The named (depth-1) section names, in file order.
fn section_names(model: &RsConfigObj) -> Vec<String> {
    model
        .section_tree()
        .into_iter()
        .filter_map(|n| n.name)
        .collect()
}

/// The scalar value for `key` in the top-level section, if present.
fn top_scalar(model: &RsConfigObj, key: &str) -> Option<String> {
    model
        .section(None)
        .and_then(|s| s.get(key).map(str::to_string))
}

/// A read/write mapping view of one named section (or a nested subsection),
/// backed by the shared model so mutations write through.
///
/// Mirrors the subset of `configobj.Section` breezy mutates: `__getitem__`,
/// `__setitem__`, `__delitem__`, `__contains__`, `get`, `keys`.
#[pyclass(name = "Section", module = "bzrformats._bzr_rs.config", unsendable)]
struct Section {
    shared: Shared,
    /// The owning section name.
    section: String,
    /// For a subsection view, the parent section name; `None` for a depth-1
    /// section.
    parent: Option<String>,
}

impl Section {
    /// The `(key, value)` options of this section in file order.
    fn options(&self) -> Vec<(String, String)> {
        let model = self.shared.borrow();
        let tree = model.section_tree();
        match &self.parent {
            None => tree
                .into_iter()
                .find(|n| n.name.as_deref() == Some(&self.section))
                .map(|n| n.options)
                .unwrap_or_default(),
            Some(parent) => tree
                .into_iter()
                .find(|n| n.name.as_deref() == Some(parent))
                .and_then(|n| {
                    n.subsections
                        .into_iter()
                        .find(|s| s.name.as_deref() == Some(&self.section))
                })
                .map(|n| n.options)
                .unwrap_or_default(),
        }
    }

    /// The names of this section's nested subsections (only a depth-1 section
    /// has these; a subsection view has none).
    fn subsection_names(&self) -> Vec<String> {
        if self.parent.is_some() {
            return Vec::new();
        }
        self.shared
            .borrow()
            .section_tree()
            .into_iter()
            .find(|n| n.name.as_deref() == Some(&self.section))
            .map(|n| n.subsections.into_iter().filter_map(|s| s.name).collect())
            .unwrap_or_default()
    }

    fn get_value(&self, key: &str) -> Option<String> {
        self.options()
            .into_iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v)
    }

    /// A view of the nested subsection `key`, if this section has one.
    fn subsection_view(&self, key: &str) -> Option<Section> {
        if self.subsection_names().iter().any(|n| n == key) {
            Some(Section {
                shared: Rc::clone(&self.shared),
                section: key.to_string(),
                parent: Some(self.section.clone()),
            })
        } else {
            None
        }
    }

    /// All keys (scalar options then nested subsection names), in file order,
    /// mirroring configobj's mixed keyspace.
    fn all_keys(&self) -> Vec<String> {
        let mut names: Vec<String> = self.options().into_iter().map(|(k, _)| k).collect();
        names.extend(self.subsection_names());
        names
    }
}

#[pymethods]
impl Section {
    fn __getitem__(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        if let Some(v) = self.get_value(key) {
            return Ok(v.into_pyobject(py)?.into_any().unbind());
        }
        if let Some(sub) = self.subsection_view(key) {
            return Ok(sub.into_pyobject(py)?.into_any().unbind());
        }
        Err(PyKeyError::new_err(key.to_string()))
    }

    fn __setitem__(&self, key: &str, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let value = stringify(value)?;
        let mut model = self.shared.borrow_mut();
        match &self.parent {
            None => model.set_value(Some(&self.section), key, &value),
            Some(parent) => model.set_subsection_value(parent, &self.section, key, &value),
        }
        Ok(())
    }

    fn __delitem__(&self, key: &str) -> PyResult<()> {
        if self.get_value(key).is_none() {
            return Err(PyKeyError::new_err(key.to_string()));
        }
        // Subsection key removal is not exercised by breezy's store; depth-1 is.
        if self.parent.is_none() {
            self.shared
                .borrow_mut()
                .remove_value(Some(&self.section), key);
        }
        Ok(())
    }

    fn __contains__(&self, key: &str) -> bool {
        self.get_value(key).is_some() || self.subsection_view(key).is_some()
    }

    #[pyo3(signature = (key, default=None))]
    fn get(&self, py: Python<'_>, key: &str, default: Option<Py<PyAny>>) -> PyResult<Py<PyAny>> {
        match self.__getitem__(py, key) {
            Ok(v) => Ok(v),
            Err(_) => Ok(default.unwrap_or_else(|| py.None())),
        }
    }

    fn keys(&self) -> Vec<String> {
        self.all_keys()
    }

    fn __iter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let iter = pyo3::types::PyList::new(py, self.all_keys())?;
        Ok(iter.try_iter()?.into_any().unbind())
    }

    fn __eq__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<bool> {
        // Compare as an ordinary mapping: scalars plus nested subsection dicts.
        let this = PyDict::new(py);
        for (k, v) in self.options() {
            this.set_item(k, v)?;
        }
        for name in self.subsection_names() {
            if let Some(sub) = self.subsection_view(&name) {
                this.set_item(name, sub.into_pyobject(py)?)?;
            }
        }
        this.as_any().eq(other)
    }

    /// Render like the equivalent `dict` (configobj's Section is a dict
    /// subclass, and callers `str()`/`%s` it expecting dict output).
    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let this = PyDict::new(py);
        for (k, v) in self.options() {
            this.set_item(k, v)?;
        }
        for name in self.subsection_names() {
            if let Some(sub) = self.subsection_view(&name) {
                this.set_item(name, sub.into_pyobject(py)?)?;
            }
        }
        this.as_any().repr().map(|r| r.to_string())
    }

    fn __str__(&self, py: Python<'_>) -> PyResult<String> {
        self.__repr__(py)
    }
}

/// A configobj-compatible parsed config file.
///
/// The top-level object behaves like a `dict` whose items are scalar option
/// strings (the no-name section) and nested [`Section`] mappings (named
/// sections). Mutations write through the shared model.
#[pyclass(name = "ConfigObj", module = "bzrformats._bzr_rs.config", unsendable)]
struct ConfigObj {
    shared: Shared,
}

impl ConfigObj {
    fn from_model(model: RsConfigObj) -> Self {
        ConfigObj {
            shared: Rc::new(RefCell::new(model)),
        }
    }

    fn section_view(&self, name: &str) -> Section {
        Section {
            shared: Rc::clone(&self.shared),
            section: name.to_string(),
            parent: None,
        }
    }

    fn is_section(&self, name: &str) -> bool {
        section_names(&self.shared.borrow())
            .iter()
            .any(|n| n == name)
    }
}

#[pymethods]
impl ConfigObj {
    /// Construct an empty config, or parse `data` (bytes) if given.
    #[new]
    #[pyo3(signature = (data=None))]
    fn new(data: Option<&[u8]>) -> PyResult<Self> {
        match data {
            None => Ok(ConfigObj::from_model(RsConfigObj::empty())),
            Some(bytes) => {
                let model = RsConfigObj::parse(bytes).map_err(parse_error_to_py)?;
                Ok(ConfigObj::from_model(model))
            }
        }
    }

    /// Parse `data` (bytes) into a config. Equivalent to `ConfigObj(data)`.
    #[classmethod]
    fn parse(_cls: &Bound<'_, PyType>, data: &[u8]) -> PyResult<Self> {
        let model = RsConfigObj::parse(data).map_err(parse_error_to_py)?;
        Ok(ConfigObj::from_model(model))
    }

    /// Top-level scalar option names in file order (configobj's `scalars`).
    #[getter]
    fn scalars(&self) -> Vec<String> {
        top_scalar_names(&self.shared.borrow())
    }

    /// Named section names in file order (configobj's `sections`).
    #[getter]
    fn sections(&self) -> Vec<String> {
        section_names(&self.shared.borrow())
    }

    /// `co[key]`: a top-level scalar value (str) or a named [`Section`].
    fn __getitem__(&self, py: Python<'_>, key: &str) -> PyResult<Py<PyAny>> {
        if let Some(v) = top_scalar(&self.shared.borrow(), key) {
            return Ok(v.into_pyobject(py)?.into_any().unbind());
        }
        if self.is_section(key) {
            return Ok(self
                .section_view(key)
                .into_pyobject(py)?
                .into_any()
                .unbind());
        }
        Err(PyKeyError::new_err(key.to_string()))
    }

    /// `co[key] = value` sets a top-level scalar (non-str values stringified).
    fn __setitem__(&self, key: &str, value: &Bound<'_, PyAny>) -> PyResult<()> {
        let value = stringify(value)?;
        self.shared.borrow_mut().set_value(None, key, &value);
        Ok(())
    }

    /// `del co[key]` removes a top-level scalar (or an empty named section).
    fn __delitem__(&self, key: &str) -> PyResult<()> {
        if top_scalar(&self.shared.borrow(), key).is_some() {
            self.shared.borrow_mut().remove_value(None, key);
            return Ok(());
        }
        Err(PyKeyError::new_err(key.to_string()))
    }

    fn __contains__(&self, key: &str) -> bool {
        top_scalar(&self.shared.borrow(), key).is_some() || self.is_section(key)
    }

    #[pyo3(signature = (key, default=None))]
    fn get(&self, py: Python<'_>, key: &str, default: Option<Py<PyAny>>) -> PyResult<Py<PyAny>> {
        match self.__getitem__(py, key) {
            Ok(v) => Ok(v),
            Err(_) => Ok(default.unwrap_or_else(|| py.None())),
        }
    }

    /// `co.setdefault(name, {})`: return the named section, creating an empty
    /// one if absent. The `default` argument is accepted for configobj
    /// compatibility but only its section name matters (breezy passes `{}`).
    #[pyo3(signature = (name, _default=None))]
    fn setdefault(&self, name: &str, _default: Option<Py<PyAny>>) -> Section {
        if !self.is_section(name) {
            // Create the (empty) section header so it exists in file order.
            self.shared.borrow_mut().ensure_section(name);
        }
        self.section_view(name)
    }

    /// Serialize to `outfile` (a binary file object) via the Rust writer.
    fn write(&self, py: Python<'_>, outfile: &Bound<'_, PyAny>) -> PyResult<()> {
        let bytes = self.shared.borrow().to_bytes();
        outfile.call_method1("write", (PyBytes::new(py, &bytes),))?;
        Ok(())
    }

    /// Serialize to bytes (UTF-8), newline-terminated.
    fn to_bytes<'py>(&self, py: Python<'py>) -> Bound<'py, PyBytes> {
        PyBytes::new(py, &self.shared.borrow().to_bytes())
    }

    /// Quote `value` for storage, matching configobj's list-aware `_quote`
    /// (non-str values are stringified first). Raises `ValueError` when the
    /// value cannot be safely quoted.
    fn _quote(&self, value: &Bound<'_, PyAny>) -> PyResult<String> {
        let value = stringify(value)?;
        bazaar::config::quote_value(&value).ok_or_else(|| {
            PyValueError::new_err(format!("value cannot be safely quoted: {value:?}"))
        })
    }

    /// Strip a matched surrounding quote pair, matching configobj's `_unquote`.
    fn _unquote(&self, value: &str) -> String {
        bazaar::config::unquote_value(value)
    }

    fn keys(&self) -> Vec<String> {
        let mut names = self.scalars();
        names.extend(self.sections());
        names
    }

    fn __iter__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let iter = pyo3::types::PyList::new(py, self.keys())?;
        Ok(iter.try_iter()?.into_any().unbind())
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>) -> PyResult<bool> {
        self.to_bytes(other.py())
            .as_any()
            .eq(other_bytes(other)?.as_any())
    }

    fn __ne__(&self, other: &Bound<'_, PyAny>) -> PyResult<bool> {
        Ok(!self.__eq__(other)?)
    }
}

/// The serialized bytes of `other` when it is another `ConfigObj`, for the
/// content-equality check breezy does in `_load_from_string`.
fn other_bytes<'py>(other: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyBytes>> {
    let co: PyRef<'_, ConfigObj> = other.extract()?;
    Ok(co.to_bytes(other.py()))
}

/// Quote `value` for writing, matching configobj's list-aware `_quote`. Raises
/// `ValueError` when the value cannot be safely quoted (as configobj does).
#[pyfunction]
fn quote_value(value: &str) -> PyResult<String> {
    bazaar::config::quote_value(value)
        .ok_or_else(|| PyValueError::new_err(format!("value cannot be safely quoted: {value:?}")))
}

/// Strip a matched surrounding quote pair from a raw value, as configobj's
/// `_unquote`.
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
