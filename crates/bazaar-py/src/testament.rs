use bazaar::testament::{
    EntryKind, Testament as RsTestament, TestamentEntry, TestamentError, TestamentFormat,
};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use std::collections::BTreeMap;

fn format_from_str(name: &str) -> PyResult<TestamentFormat> {
    match name {
        "1" => Ok(TestamentFormat::V1),
        "strict" | "2.1" => Ok(TestamentFormat::Strict),
        "strict3" | "3" => Ok(TestamentFormat::Strict3),
        other => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "unknown testament format: {other:?}"
        ))),
    }
}

fn kind_from_str(name: &str) -> PyResult<EntryKind> {
    match name {
        "file" => Ok(EntryKind::File),
        "directory" => Ok(EntryKind::Directory),
        "symlink" => Ok(EntryKind::Symlink),
        "tree-reference" => Ok(EntryKind::TreeReference),
        other => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "unknown entry kind: {other:?}"
        ))),
    }
}

fn err_to_py(e: TestamentError) -> PyErr {
    pyo3::exceptions::PyValueError::new_err(e.to_string())
}

/// A signable summary of a revision.
///
/// Built from the revision's fields and its tree entries; the format
/// (`"1"`, `"strict"`, or `"strict3"`) selects which testament variant the
/// `as_*` methods produce.
#[pyclass]
struct Testament {
    inner: RsTestament,
}

#[pymethods]
impl Testament {
    /// Construct a testament.
    ///
    /// `entries` is a sequence of
    /// `(path, kind, file_id, content, revision, executable)` where `path`
    /// is str, `kind` is one of "file"/"directory"/"symlink"/
    /// "tree-reference", `file_id`/`content`/`revision` are bytes (content
    /// is the file text sha1 or symlink target), and `executable` is bool.
    #[new]
    #[pyo3(signature = (revision_id, committer, timestamp, timezone, message, parent_ids, revprops, entries))]
    #[allow(clippy::too_many_arguments)]
    fn new(
        revision_id: Vec<u8>,
        committer: String,
        timestamp: i64,
        timezone: i32,
        message: String,
        parent_ids: Vec<Vec<u8>>,
        revprops: &Bound<'_, PyDict>,
        entries: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let mut props: BTreeMap<String, String> = BTreeMap::new();
        for (k, v) in revprops.iter() {
            props.insert(k.extract()?, v.extract()?);
        }
        let mut parsed = Vec::new();
        for item in entries.try_iter()? {
            let t = item?;
            parsed.push(TestamentEntry {
                path: t.get_item(0)?.extract()?,
                kind: kind_from_str(&t.get_item(1)?.extract::<String>()?)?,
                file_id: t.get_item(2)?.extract()?,
                content: t.get_item(3)?.extract()?,
                revision: t.get_item(4)?.extract()?,
                executable: t.get_item(5)?.extract()?,
            });
        }
        Ok(Testament {
            inner: RsTestament {
                revision_id,
                committer,
                timestamp,
                timezone,
                message,
                parent_ids,
                revprops: props,
                entries: parsed,
            },
        })
    }

    /// The full testament text in the given format.
    fn as_text<'py>(&self, py: Python<'py>, format: &str) -> PyResult<Bound<'py, PyBytes>> {
        let bytes = self
            .inner
            .as_text(format_from_str(format)?)
            .map_err(err_to_py)?;
        Ok(PyBytes::new(py, &bytes))
    }

    /// The short, digest-based testament text.
    fn as_short_text<'py>(&self, py: Python<'py>, format: &str) -> PyResult<Bound<'py, PyBytes>> {
        let bytes = self
            .inner
            .as_short_text(format_from_str(format)?)
            .map_err(err_to_py)?;
        Ok(PyBytes::new(py, &bytes))
    }

    /// The hex sha1 of the full testament.
    fn as_sha1<'py>(&self, py: Python<'py>, format: &str) -> PyResult<Bound<'py, PyBytes>> {
        let bytes = self
            .inner
            .as_sha1(format_from_str(format)?)
            .map_err(err_to_py)?;
        Ok(PyBytes::new(py, &bytes))
    }
}

pub(crate) fn _testament_rs(py: Python) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "testament")?;
    m.add_class::<Testament>()?;
    Ok(m)
}
