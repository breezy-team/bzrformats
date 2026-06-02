//! Bindings for the standalone control-directory API: `BzrDir`, `Branch`,
//! `Repository` and `WorkingTree`.
//!
//! These wrap the pure-Rust opener types in `bazaar`. The Python entry
//! points are the module functions [`open`] and [`create`], which take a
//! filesystem path; `BzrDir.open_branch()` / `open_repository()` /
//! `open_workingtree()` then yield the component objects.

use std::collections::BTreeMap;
use std::sync::Arc;

use bazaar::branch::Branch as RsBranch;
use bazaar::bzrdir::BzrDir as RsBzrDir;
use bazaar::repository::Repository as RsRepository;
use bazaar::transport::{LocalTransport, SharedTransport};
use bazaar::workingtree::{EntryKind, WorkingTree as RsWorkingTree};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};

pyo3::import_exception!(bzrformats.errors, BzrFormatsError);

fn err<E: std::fmt::Display>(e: E) -> PyErr {
    BzrFormatsError::new_err(e.to_string())
}

fn kind_str(kind: EntryKind) -> &'static str {
    match kind {
        EntryKind::File => "file",
        EntryKind::Directory => "directory",
        EntryKind::Symlink => "symlink",
        EntryKind::TreeReference => "tree-reference",
    }
}

/// A `.bzr` control directory.
#[pyclass(name = "BzrDir")]
struct BzrDir {
    inner: RsBzrDir,
}

#[pymethods]
impl BzrDir {
    /// Whether this control directory contains a repository.
    fn has_repository(&self) -> bool {
        self.inner.has_repository()
    }

    /// Whether this control directory contains a branch.
    fn has_branch(&self) -> bool {
        self.inner.has_branch()
    }

    /// Whether this control directory contains a working tree.
    fn has_workingtree(&self) -> bool {
        self.inner.has_workingtree()
    }

    /// Open the repository in this control directory.
    fn open_repository(&self) -> PyResult<Repository> {
        Ok(Repository {
            inner: self.inner.open_repository().map_err(err)?,
        })
    }

    /// Open the branch in this control directory.
    fn open_branch(&self) -> PyResult<Branch> {
        Ok(Branch {
            inner: self.inner.open_branch().map_err(err)?,
        })
    }

    /// Open the working tree in this control directory.
    fn open_workingtree(&self) -> PyResult<WorkingTree> {
        Ok(WorkingTree {
            inner: self.inner.open_workingtree().map_err(err)?,
        })
    }
}

/// A bzr repository.
#[pyclass(name = "Repository")]
struct Repository {
    inner: Box<dyn RsRepository>,
}

#[pymethods]
impl Repository {
    /// All revision ids in this repository, sorted.
    fn all_revision_ids<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let ids = self.inner.all_revision_ids().map_err(err)?;
        PyList::new(py, ids.iter().map(|i| PyBytes::new(py, i)))
    }

    /// The committer, message and parents of a revision, as a dict.
    fn get_revision<'py>(
        &self,
        py: Python<'py>,
        revision_id: &[u8],
    ) -> PyResult<Bound<'py, PyDict>> {
        let rev = self.inner.get_revision(revision_id).map_err(err)?;
        let d = PyDict::new(py);
        d.set_item("revision_id", PyBytes::new(py, rev.revision_id.as_bytes()))?;
        d.set_item("committer", rev.committer.clone())?;
        d.set_item("message", rev.message.clone())?;
        d.set_item("timestamp", rev.timestamp)?;
        let parents = PyList::new(
            py,
            rev.parent_ids
                .iter()
                .map(|p| PyBytes::new(py, p.as_bytes())),
        )?;
        d.set_item("parent_ids", parents)?;
        Ok(d)
    }

    /// The full text of a versioned file at a revision.
    fn get_file_text<'py>(
        &self,
        py: Python<'py>,
        file_id: &[u8],
        revision: &[u8],
    ) -> PyResult<Bound<'py, PyBytes>> {
        let text = self.inner.get_file_text(file_id, revision).map_err(err)?;
        Ok(PyBytes::new(py, &text))
    }

    /// The inventory of a revision, as a list of `(path, kind, file_id)`.
    fn get_inventory<'py>(
        &self,
        py: Python<'py>,
        revision_id: &[u8],
    ) -> PyResult<Bound<'py, PyList>> {
        let inv = self.inner.get_inventory(revision_id).map_err(err)?;
        let entries = inv
            .entries()
            .map_err(|e| BzrFormatsError::new_err(format!("{e:?}")))?;
        let out = PyList::empty(py);
        for (path, entry) in entries {
            let kind = format!("{:?}", entry.kind()).to_lowercase();
            let tuple = (path, kind, PyBytes::new(py, entry.file_id().as_bytes()));
            out.append(tuple)?;
        }
        Ok(out)
    }
}

/// A bzr branch.
#[pyclass(name = "Branch")]
struct Branch {
    inner: RsBranch,
}

#[pymethods]
impl Branch {
    /// The tip as `(revno, revision_id)`.
    fn last_revision_info<'py>(&self, py: Python<'py>) -> PyResult<(u64, Bound<'py, PyBytes>)> {
        let (revno, revid) = self.inner.last_revision_info().map_err(err)?;
        Ok((revno, PyBytes::new(py, &revid)))
    }

    /// The tip revision id (`b"null:"` for an empty branch).
    fn last_revision<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        Ok(PyBytes::new(py, &self.inner.last_revision().map_err(err)?))
    }

    /// The branch tags as a `{name: revision_id}` dict.
    fn tags<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let tags = self.inner.tags().map_err(err)?;
        let d = PyDict::new(py);
        for (name, target) in tags {
            d.set_item(name, PyBytes::new(py, &target))?;
        }
        Ok(d)
    }

    /// Set the tip to `(revno, revision_id)`.
    fn set_last_revision_info(&self, revno: u64, revision_id: &[u8]) -> PyResult<()> {
        self.inner
            .set_last_revision_info(revno, revision_id)
            .map_err(err)
    }

    /// Replace the branch tags from a `{name: revision_id}` dict.
    fn set_tags(&self, tags: &Bound<'_, PyDict>) -> PyResult<()> {
        let mut map: BTreeMap<String, Vec<u8>> = BTreeMap::new();
        for (k, v) in tags.iter() {
            map.insert(k.extract()?, v.extract()?);
        }
        self.inner.set_tags(&map).map_err(err)
    }
}

/// A dirstate-based working tree.
#[pyclass(name = "WorkingTree")]
struct WorkingTree {
    inner: RsWorkingTree,
}

#[pymethods]
impl WorkingTree {
    /// The basis revision id, or None for a never-committed tree.
    fn basis_revision<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyBytes>> {
        self.inner.basis_revision().map(|r| PyBytes::new(py, &r))
    }

    /// The live tracked entries as a list of `(path, kind, file_id)`.
    fn list_files<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let out = PyList::empty(py);
        for e in self.inner.list_files() {
            out.append((e.path, kind_str(e.kind), PyBytes::new(py, &e.file_id)))?;
        }
        Ok(out)
    }

    /// The file id at `path`, or None if not versioned.
    fn path2id<'py>(&self, py: Python<'py>, path: &str) -> Option<Bound<'py, PyBytes>> {
        self.inner.path2id(path).map(|i| PyBytes::new(py, &i))
    }

    /// The content of a versioned file, read from disk.
    fn get_file_text<'py>(&self, py: Python<'py>, path: &str) -> PyResult<Bound<'py, PyBytes>> {
        Ok(PyBytes::new(
            py,
            &self.inner.get_file_text(path).map_err(err)?,
        ))
    }

    /// Commit the live tree state as a new revision and return its id.
    #[pyo3(signature = (repository, branch, committer, message, timestamp, timezone))]
    #[allow(clippy::too_many_arguments)]
    fn commit<'py>(
        &mut self,
        py: Python<'py>,
        repository: &Bound<'py, Repository>,
        branch: &Bound<'py, Branch>,
        committer: &str,
        message: &str,
        timestamp: u64,
        timezone: i32,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let mut repo = repository.borrow_mut();
        let branch = branch.borrow();
        let revid = self
            .inner
            .commit(
                repo.inner.as_mut(),
                &branch.inner,
                committer,
                message,
                timestamp,
                timezone,
            )
            .map_err(err)?;
        Ok(PyBytes::new(py, &revid))
    }
}

/// Build a local transport rooted at `path`.
fn local(path: &str) -> SharedTransport {
    Arc::new(LocalTransport::new(path))
}

/// Open the `.bzr` control directory at `path` (the directory containing
/// `.bzr`).
#[pyfunction]
fn open(path: &str) -> PyResult<BzrDir> {
    let root = local(path);
    let bzr = root.subtransport(".bzr").map_err(err)?;
    Ok(BzrDir {
        inner: RsBzrDir::open(bzr).map_err(err)?,
    })
}

/// Create a fresh 2a control directory at `path` and open it.
#[pyfunction]
fn create(path: &str) -> PyResult<BzrDir> {
    let parent = local(path);
    Ok(BzrDir {
        inner: RsBzrDir::create(&parent).map_err(err)?,
    })
}

pub(crate) fn _controldir_rs(py: Python) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "controldir")?;
    m.add_class::<BzrDir>()?;
    m.add_class::<Repository>()?;
    m.add_class::<Branch>()?;
    m.add_class::<WorkingTree>()?;
    m.add_function(wrap_pyfunction!(open, &m)?)?;
    m.add_function(wrap_pyfunction!(create, &m)?)?;
    Ok(m)
}
