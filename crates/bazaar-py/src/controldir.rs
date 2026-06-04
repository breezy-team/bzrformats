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
use bazaar::bzrdir::{
    find_control_dir_format, BzrDirAllInOne, BzrDirMeta, ControlDir as RsControlDir,
};
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

fn kind_from_str(kind: &str) -> PyResult<EntryKind> {
    match kind {
        "file" => Ok(EntryKind::File),
        "directory" => Ok(EntryKind::Directory),
        "symlink" => Ok(EntryKind::Symlink),
        "tree-reference" => Ok(EntryKind::TreeReference),
        other => Err(pyo3::exceptions::PyValueError::new_err(format!(
            "unknown kind: {other}"
        ))),
    }
}

/// A `.bzr` control directory.
#[pyclass(name = "BzrDir")]
struct BzrDir {
    inner: Box<dyn RsControlDir>,
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

    /// The stored parents of each of `revision_ids`, as a `{revid: [parent]}`
    /// dict. Revision ids not present in the repository are omitted.
    fn get_parent_map<'py>(
        &self,
        py: Python<'py>,
        revision_ids: Vec<Vec<u8>>,
    ) -> PyResult<Bound<'py, PyDict>> {
        let map = self.inner.get_parent_map(&revision_ids).map_err(err)?;
        let d = PyDict::new(py);
        for (revid, parents) in map {
            let plist = PyList::new(py, parents.iter().map(|p| PyBytes::new(py, p)))?;
            d.set_item(PyBytes::new(py, &revid), plist)?;
        }
        Ok(d)
    }

    /// Whether `revision_id` is present in this repository.
    fn has_revision(&self, revision_id: &[u8]) -> PyResult<bool> {
        self.inner.has_revision(revision_id).map_err(err)
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
        d.set_item("timezone", rev.timezone)?;
        let props = PyDict::new(py);
        for (k, v) in &rev.properties {
            props.set_item(k, PyBytes::new(py, v))?;
        }
        d.set_item("properties", props)?;
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

    /// The signature text stored for `revision_id`, or None if unsigned.
    fn get_signature_text<'py>(
        &self,
        py: Python<'py>,
        revision_id: &[u8],
    ) -> PyResult<Option<Bound<'py, PyBytes>>> {
        Ok(self
            .inner
            .get_signature_text(revision_id)
            .map_err(err)?
            .map(|s| PyBytes::new(py, &s)))
    }

    /// Open a write group: a batch of additions flushed by
    /// `commit_write_group`. Writing requires an open write group.
    fn start_write_group(&mut self) -> PyResult<()> {
        self.inner.start_write_group().map_err(err)
    }

    /// Flush the open write group, committing its additions.
    fn commit_write_group(&mut self) -> PyResult<()> {
        self.inner.commit_write_group().map_err(err)
    }

    /// Add a file text keyed by `(file_id, revision)` to the open write group.
    /// `parents` is a list of `(file_id, revision)` tuples.
    #[pyo3(signature = (file_id, revision, bytes, parents=None))]
    fn add_text(
        &mut self,
        file_id: &[u8],
        revision: &[u8],
        bytes: &[u8],
        parents: Option<Vec<(Vec<u8>, Vec<u8>)>>,
    ) -> PyResult<()> {
        self.inner
            .add_text(file_id, revision, &parents.unwrap_or_default(), bytes)
            .map_err(err)
    }

    /// Add a signature text for `revision_id` to the open write group.
    fn add_signature_text(&mut self, revision_id: &[u8], signature: &[u8]) -> PyResult<()> {
        self.inner
            .add_signature_text(revision_id, signature)
            .map_err(err)
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

    /// The mainline revision ids, oldest first. For a format-5 branch this is
    /// the full `revision-history`; for 6/7/8 it is the tip alone (or empty).
    fn revision_history<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let history = self.inner.revision_history().map_err(err)?;
        PyList::new(py, history.iter().map(|r| PyBytes::new(py, r)))
    }

    /// Replace the full mainline (format 5) from a list of revision ids.
    fn set_revision_history(&self, history: Vec<Vec<u8>>) -> PyResult<()> {
        self.inner.set_revision_history(&history).map_err(err)
    }

    /// The raw bytes of `branch.conf` (empty if the file is absent).
    fn get_config_bytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        Ok(PyBytes::new(
            py,
            &self.inner.get_config_bytes().map_err(err)?,
        ))
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

/// A working tree, backed by whichever on-disk format was opened.
#[pyclass(name = "WorkingTree")]
struct WorkingTree {
    inner: Box<dyn RsWorkingTree>,
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

    /// Version `path` with `kind` ("file"/"directory"/"symlink"/
    /// "tree-reference"), optionally with an explicit `file_id` (a fresh id
    /// is generated when omitted). Returns the file id. Already-versioned
    /// paths are left unchanged.
    #[pyo3(signature = (path, kind, file_id=None))]
    fn add<'py>(
        &mut self,
        py: Python<'py>,
        path: &str,
        kind: &str,
        file_id: Option<&[u8]>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let id = self
            .inner
            .add(path, kind_from_str(kind)?, file_id)
            .map_err(err)?;
        Ok(PyBytes::new(py, &id))
    }

    /// Stop versioning `path` (and its children, if a directory). The files
    /// are left on disk.
    fn remove(&mut self, path: &str) -> PyResult<()> {
        self.inner.remove(path).map_err(err)
    }

    /// Move a versioned entry from `from_path` to `to_path`, keeping its
    /// file id, and move the file on disk.
    fn rename(&mut self, from_path: &str, to_path: &str) -> PyResult<()> {
        self.inner.rename(from_path, to_path).map_err(err)
    }

    /// The tree-relative paths of on-disk files that are not versioned.
    fn unknowns(&self) -> PyResult<Vec<String>> {
        self.inner.unknowns().map_err(err)
    }

    /// The tree's parent revision ids (basis first, then pending merges).
    fn parent_ids<'py>(&self, py: Python<'py>) -> Vec<Bound<'py, PyBytes>> {
        self.inner
            .parent_ids()
            .iter()
            .map(|p| PyBytes::new(py, p))
            .collect()
    }

    /// Add `revision_id` as a pending-merge parent for the next commit.
    fn add_pending_merge(&mut self, revision_id: &[u8]) -> PyResult<()> {
        self.inner.add_pending_merge(revision_id).map_err(err)
    }

    /// The changes between this working tree and the basis `basis_revision_id`
    /// (resolved against `repository`), as a list of dicts with keys
    /// `file_id`, `old_path`, `new_path`, `content_change`, `kind`,
    /// `executable`. A `None` path means the entry is added (`old_path`) or
    /// removed (`new_path`).
    fn iter_changes<'py>(
        &self,
        py: Python<'py>,
        repository: &Bound<'py, Repository>,
        basis_revision_id: &[u8],
    ) -> PyResult<Bound<'py, PyList>> {
        let repo = repository.borrow();
        let basis = repo.inner.revision_tree(basis_revision_id).map_err(err)?;
        let changes = self.inner.iter_changes(&basis).map_err(err)?;
        let out = PyList::empty(py);
        for c in changes {
            let d = PyDict::new(py);
            d.set_item("file_id", PyBytes::new(py, &c.file_id))?;
            d.set_item("old_path", c.old_path)?;
            d.set_item("new_path", c.new_path)?;
            d.set_item("content_change", c.content_change)?;
            d.set_item("kind", c.new_kind.map(kind_str))?;
            d.set_item("executable", c.new_executable)?;
            out.append(d)?;
        }
        Ok(out)
    }

    /// Commit the live tree state as a new revision and return its id.
    ///
    /// `revprops` is an optional `{str: bytes}` dict of revision properties;
    /// `authors` an optional list of author strings; `revision_id` an
    /// optional explicit id (generated when omitted).
    #[pyo3(signature = (repository, branch, committer, message, timestamp, timezone,
        revprops=None, authors=None, revision_id=None, branch_nick=None,
        allow_pointless=false, strict=false, specific_files=None, exclude=None,
        signing_key=None))]
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
        revprops: Option<&Bound<'py, PyDict>>,
        authors: Option<Vec<String>>,
        revision_id: Option<&[u8]>,
        branch_nick: Option<String>,
        allow_pointless: bool,
        strict: bool,
        specific_files: Option<Vec<String>>,
        exclude: Option<Vec<String>>,
        signing_key: Option<&[u8]>,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let mut repo = repository.borrow_mut();
        let branch = branch.borrow();
        let mut options = bazaar::workingtree::CommitOptions::new(committer, message)
            .timestamp(timestamp)
            .timezone(timezone)
            .allow_pointless(allow_pointless)
            .strict(strict);
        if let Some(props) = revprops {
            let mut map: std::collections::HashMap<String, Vec<u8>> =
                std::collections::HashMap::new();
            for (k, v) in props.iter() {
                map.insert(k.extract()?, v.extract()?);
            }
            options = options.revprops(map);
        }
        if let Some(authors) = authors {
            options = options.authors(authors);
        }
        if let Some(id) = revision_id {
            options = options.revision_id(id.to_vec());
        }
        if let Some(nick) = branch_nick {
            options = options.branch_nick(nick);
        }
        if let Some(files) = specific_files {
            options = options.specific_files(files);
        }
        if let Some(exclude) = exclude {
            options = options.exclude(exclude);
        }
        if let Some(key) = signing_key {
            options = options.signing_key(key.to_vec());
        }
        let revid = self
            .inner
            .commit(repo.inner.as_mut(), &branch.inner, &options)
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
        inner: bazaar::bzrdir::open(bzr).map_err(err)?,
    })
}

/// Create a fresh control directory at `path` in `format` and open it.
///
/// `format` is a registry name as accepted by `brz init --format=` -- "2a"
/// (the default), the knit-pack variants ("pack-0.92", "1.9", "rich-root-pack",
/// ...), "knit", or "weave" (the all-in-one bzr 0.8 format). See
/// [`format_names`] for the full list.
#[pyfunction]
#[pyo3(signature = (path, format="2a"))]
fn create(path: &str, format: &str) -> PyResult<BzrDir> {
    let parent = local(path);
    let inner: Box<dyn RsControlDir> = if format == "weave" {
        Box::new(BzrDirAllInOne::create(&parent).map_err(err)?)
    } else {
        let fmt = find_control_dir_format(format).ok_or_else(|| {
            pyo3::exceptions::PyValueError::new_err(format!("unknown control dir format: {format}"))
        })?;
        Box::new(BzrDirMeta::create_with_format(&parent, fmt).map_err(err)?)
    };
    Ok(BzrDir { inner })
}

/// The control-directory format names accepted by [`create`].
#[pyfunction]
fn format_names() -> Vec<&'static str> {
    let mut names: Vec<&'static str> = bazaar::bzrdir::control_dir_formats()
        .iter()
        .map(|f| f.name)
        .collect();
    names.push("weave");
    names
}

pub(crate) fn _controldir_rs(py: Python) -> PyResult<Bound<PyModule>> {
    let m = PyModule::new(py, "controldir")?;
    m.add_class::<BzrDir>()?;
    m.add_class::<Repository>()?;
    m.add_class::<Branch>()?;
    m.add_class::<WorkingTree>()?;
    m.add_function(wrap_pyfunction!(open, &m)?)?;
    m.add_function(wrap_pyfunction!(create, &m)?)?;
    m.add_function(wrap_pyfunction!(format_names, &m)?)?;
    Ok(m)
}
