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
use bazaar::lockable_files::{LockMode, LockToken, Lockable, LockableFilesError, NoWait};
use bazaar::lockdir::{LockError, LockHeldInfo};
use bazaar::repository::Repository as RsRepository;
use bazaar::transport::{LocalTransport, SharedTransport};
use bazaar::workingtree::{EntryKind, WorkingTree as RsWorkingTree};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList};
use pyo3::{PyTraverseError, PyVisit};

use crate::lock::{PyBranchWriteLockResult, PyLogicalLockResult, PyRepositoryWriteLockResult};

pyo3::import_exception!(bzrformats.errors, BzrFormatsError);
pyo3::import_exception!(bzrformats.errors, NotStacked);
pyo3::import_exception!(bzrformats.errors, UnstackableBranchFormat);
pyo3::import_exception!(bzrformats.errors, UnsupportedOperation);
pyo3::import_exception!(bzrformats.errors, LockContention);
pyo3::import_exception!(bzrformats.errors, LockNotHeld);
pyo3::import_exception!(bzrformats.errors, ReadOnlyError);
pyo3::import_exception!(bzrformats.errors, LockActive);
pyo3::import_exception!(bzrformats.errors, LockBreakMismatch);

fn err<E: std::fmt::Display>(e: E) -> PyErr {
    BzrFormatsError::new_err(e.to_string())
}

/// Map a branch error onto the matching breezy-style exception, so downstream
/// `except NotStacked`/`UnstackableBranchFormat`/`UnsupportedOperation` clauses
/// catch the right thing. Other variants fall back to the generic error.
fn branch_err(e: bazaar::branch::BranchError) -> PyErr {
    use bazaar::branch::BranchError;
    match e {
        // The Rust BranchError variants do not carry the branch/format objects
        // the breezy exceptions name, so pass the bzrformats branch identity we
        // have. The exception type is what downstream except-clauses match on.
        BranchError::NotStacked => NotStacked::new_err(("bzrformats branch",)),
        BranchError::Unstackable => {
            UnstackableBranchFormat::new_err(("branch", "bzrformats branch"))
        }
        BranchError::Unsupported(op) => UnsupportedOperation::new_err((op, "bzrformats branch")),
        BranchError::Locking(e) => lock_err(e, "branch"),
        BranchError::Repository(e) => repository_err(e),
        other => BzrFormatsError::new_err(other.to_string()),
    }
}

/// Map a failure to lock `what` onto the lock exceptions in
/// `bzrformats.errors`.
fn lock_err(e: LockableFilesError, what: &str) -> PyErr {
    match e {
        LockableFilesError::Contention => LockContention::new_err((what.to_string(),)),
        LockableFilesError::NotHeld => LockNotHeld::new_err((what.to_string(),)),
        LockableFilesError::ReadOnly => ReadOnlyError::new_err((what.to_string(),)),
        LockableFilesError::Active | LockableFilesError::Lock(LockError::BreakOwnLock) => {
            LockActive::new_err((what.to_string(),))
        }
        LockableFilesError::Lock(LockError::BreakMismatch { held, target }) => {
            LockBreakMismatch::new_err((what.to_string(), held, target))
        }
        other => BzrFormatsError::new_err(other.to_string()),
    }
}

/// A lock holder as the dict passed to a `break_lock` confirmation
/// callback, with the fields of [`LockHeldInfo`].
fn holder_dict<'py>(py: Python<'py>, holder: &LockHeldInfo) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new(py);
    d.set_item("user", &holder.user)?;
    d.set_item("hostname", &holder.hostname)?;
    d.set_item("pid", holder.pid)?;
    d.set_item("nonce", &holder.nonce)?;
    let start_time = holder
        .start_time
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_secs_f64());
    d.set_item("start_time", start_time)?;
    for (key, value) in &holder.extra_holder_info {
        d.set_item(key, value)?;
    }
    Ok(d)
}

/// Run `f` with a callback asking the Python callable `confirm` whether to
/// break a lock: it is given the holder as a dict, or `None` when the lock's
/// info is corrupt. Without `confirm` nothing is broken. An exception the
/// callable raises declines that break and is raised once `f` returns.
fn with_confirm<R>(
    py: Python<'_>,
    confirm: Option<&Bound<'_, PyAny>>,
    f: impl FnOnce(&mut dyn FnMut(Option<&LockHeldInfo>) -> bool) -> PyResult<R>,
) -> PyResult<R> {
    let mut raised: Option<PyErr> = None;
    let mut ask = |holder: Option<&LockHeldInfo>| -> bool {
        let Some(confirm) = confirm else {
            return false;
        };
        if raised.is_some() {
            return false;
        }
        let answer = holder
            .map(|holder| holder_dict(py, holder))
            .transpose()
            .and_then(|info| confirm.call1((info,)))
            .and_then(|answer| answer.is_truthy());
        answer.unwrap_or_else(|e| {
            raised = Some(e);
            false
        })
    };
    let result = f(&mut ask);
    match raised {
        Some(e) => Err(e),
        None => result,
    }
}

/// Map a repository error, lock failures onto the lock exceptions.
fn repository_err(e: bazaar::repository::RepositoryError) -> PyErr {
    match e {
        bazaar::repository::RepositoryError::Locking(e) => lock_err(e, "repository"),
        other => err(other),
    }
}

/// Map a working tree error, lock failures onto the lock exceptions.
fn tree_err(e: bazaar::workingtree::WorkingTreeError) -> PyErr {
    use bazaar::workingtree::WorkingTreeError;
    match e {
        WorkingTreeError::Locking(e) => lock_err(e, "working tree"),
        WorkingTreeError::Branch(e) => branch_err(e),
        WorkingTreeError::Repository(e) => repository_err(e),
        other => err(other),
    }
}

/// The name of a lock mode, as `peek_lock_mode` returns it.
fn lock_mode_str(mode: Option<LockMode>) -> Option<&'static str> {
    mode.map(LockMode::as_str)
}

/// A `LogicalLockResult` releasing the lock through `unlock`.
fn logical_lock_result(unlock: Bound<'_, PyAny>) -> PyResult<Py<PyLogicalLockResult>> {
    Py::new(
        unlock.py(),
        PyLogicalLockResult {
            unlock: unlock.unbind(),
            token: None,
        },
    )
}

/// The token of a write lock as a Python string, or `None`.
fn token_object(py: Python<'_>, token: Option<LockToken>) -> Option<Py<PyAny>> {
    token.map(|t| {
        pyo3::types::PyString::new(py, t.as_str())
            .into_any()
            .unbind()
    })
}

/// Where a `Repository` object's repository lives: its own, or the one held
/// by the branch it was reached through as `branch.repository`.
enum RepositoryRef {
    Owned(Box<dyn RsRepository>),
    Branch(Py<Branch>),
}

/// Where a `Branch` object's branch lives: its own, or the one held by the
/// working tree it was reached through as `tree.branch`.
enum BranchRef {
    Owned(RsBranch),
    Tree(Py<WorkingTree>),
}

fn kind_str(kind: EntryKind) -> &'static str {
    match kind {
        EntryKind::File => "file",
        EntryKind::Directory => "directory",
        EntryKind::Symlink => "symlink",
        EntryKind::TreeReference => "tree-reference",
    }
}

/// Iterator returned by `WorkingTree.iter_changes`, yielding one
/// change dict per step. The tree-vs-basis diff is computed eagerly
/// (it is a whole-tree comparison); only the dict construction is lazy.
#[pyclass]
struct TreeChangesIter {
    changes: std::collections::VecDeque<bazaar::workingtree::WorkingTreeChange>,
}

#[pymethods]
impl TreeChangesIter {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__<'py>(&mut self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyDict>>> {
        let Some(c) = self.changes.pop_front() else {
            return Ok(None);
        };
        let d = PyDict::new(py);
        d.set_item("file_id", PyBytes::new(py, &c.file_id))?;
        d.set_item("old_path", c.old_path)?;
        d.set_item("new_path", c.new_path)?;
        d.set_item("content_change", c.content_change)?;
        d.set_item("kind", c.new_kind.map(kind_str))?;
        d.set_item("executable", c.new_executable)?;
        Ok(Some(d))
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
        Ok(Repository::owned(
            self.inner.open_repository().map_err(err)?,
        ))
    }

    /// Open the repository with any stacked-on fallback activated, so reads
    /// resolve objects held only in the base repository this branch is stacked
    /// on.
    fn open_repository_stacked(&self) -> PyResult<Repository> {
        Ok(Repository::owned(
            self.inner.open_repository_stacked().map_err(err)?,
        ))
    }

    /// Open the branch in this control directory.
    fn open_branch(&self) -> PyResult<Branch> {
        Ok(Branch {
            inner: BranchRef::Owned(self.inner.open_branch().map_err(err)?),
            repository: None,
        })
    }

    /// Open the working tree in this control directory.
    fn open_workingtree(&self) -> PyResult<WorkingTree> {
        Ok(WorkingTree {
            inner: self.inner.open_workingtree().map_err(err)?,
            branch: None,
        })
    }

    /// Whether this control directory's repository is shared.
    fn is_shared(&self) -> PyResult<bool> {
        self.inner.is_shared().map_err(err)
    }

    /// Whether this repository creates working trees for branches it serves.
    fn make_working_trees(&self) -> PyResult<bool> {
        self.inner.make_working_trees().map_err(err)
    }

    /// Set whether this repository creates working trees.
    fn set_make_working_trees(&self, value: bool) -> PyResult<()> {
        self.inner.set_make_working_trees(value).map_err(err)
    }

    /// Find the repository serving this control directory, walking up to an
    /// enclosing shared repository when this one has none of its own.
    fn find_repository(&self) -> PyResult<Repository> {
        Ok(Repository::owned(
            self.inner.find_repository().map_err(err)?,
        ))
    }
}

/// A bzr repository.
///
// TODO: expose Repository.add_fallback_repository directly. It takes ownership
// of the fallback (Box<dyn Repository>), which cannot be moved out of a live
// Python Repository object; for now stacked repositories are obtained through
// BzrDir.open_repository_stacked, which covers the branch-stacking use case.
#[pyclass(name = "Repository")]
struct Repository {
    inner: RepositoryRef,
}

impl Repository {
    fn owned(repository: Box<dyn RsRepository>) -> Self {
        Repository {
            inner: RepositoryRef::Owned(repository),
        }
    }

    /// Run `f` on the repository, wherever it lives.
    fn with<R>(
        &self,
        py: Python<'_>,
        f: impl FnOnce(&dyn RsRepository) -> PyResult<R>,
    ) -> PyResult<R> {
        match &self.inner {
            RepositoryRef::Owned(repository) => f(repository.as_ref()),
            RepositoryRef::Branch(branch) => branch
                .bind(py)
                .try_borrow()?
                .with(py, |branch| f(branch.repository())),
        }
    }

    /// Run `f` on the repository for writing, wherever it lives.
    fn with_mut<R>(
        &mut self,
        py: Python<'_>,
        f: impl FnOnce(&mut dyn RsRepository) -> PyResult<R>,
    ) -> PyResult<R> {
        match &mut self.inner {
            RepositoryRef::Owned(repository) => f(repository.as_mut()),
            RepositoryRef::Branch(branch) => branch
                .bind(py)
                .try_borrow_mut()?
                .with_mut(py, |branch| f(branch.repository_mut())),
        }
    }
}

#[pymethods]
impl Repository {
    /// This repository's format as `{format_string: bytes, description: str}`.
    fn format<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        self.with(py, |inner| {
            let fmt = inner.format();
            let d = PyDict::new(py);
            d.set_item("format_string", PyBytes::new(py, fmt.format_string()))?;
            d.set_item("description", fmt.get_format_description())?;
            Ok(d)
        })
    }

    /// All revision ids in this repository, sorted.
    fn all_revision_ids<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        self.with(py, |inner| {
            let ids = inner.all_revision_ids().map_err(err)?;
            PyList::new(py, ids.iter().map(|i| PyBytes::new(py, i)))
        })
    }

    /// The stored parents of each of `revision_ids`, as a `{revid: [parent]}`
    /// dict. Revision ids not present in the repository are omitted.
    fn get_parent_map<'py>(
        &self,
        py: Python<'py>,
        revision_ids: Vec<Vec<u8>>,
    ) -> PyResult<Bound<'py, PyDict>> {
        self.with(py, |inner| {
            let map = inner.get_parent_map(&revision_ids).map_err(err)?;
            let d = PyDict::new(py);
            for (revid, parents) in map {
                let plist = PyList::new(py, parents.iter().map(|p| PyBytes::new(py, p)))?;
                d.set_item(PyBytes::new(py, &revid), plist)?;
            }
            Ok(d)
        })
    }

    /// Whether `revision_id` is present in this repository.
    fn has_revision(&self, py: Python<'_>, revision_id: &[u8]) -> PyResult<bool> {
        self.with(py, |inner| inner.has_revision(revision_id).map_err(err))
    }

    /// The committer, message and parents of a revision, as a dict.
    fn get_revision<'py>(
        &self,
        py: Python<'py>,
        revision_id: &[u8],
    ) -> PyResult<Bound<'py, PyDict>> {
        self.with(py, |inner| {
            let rev = inner.get_revision(revision_id).map_err(err)?;
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
        })
    }

    /// The full text of a versioned file at a revision.
    fn get_file_text<'py>(
        &self,
        py: Python<'py>,
        file_id: &[u8],
        revision: &[u8],
    ) -> PyResult<Bound<'py, PyBytes>> {
        self.with(py, |inner| {
            let text = inner.get_file_text(file_id, revision).map_err(err)?;
            Ok(PyBytes::new(py, &text))
        })
    }

    /// The full text of the file at tree-relative `path` in `revision`.
    fn get_file_text_at_path<'py>(
        &self,
        py: Python<'py>,
        path: &str,
        revision: &[u8],
    ) -> PyResult<Bound<'py, PyBytes>> {
        let text = self.with(py, |repository| {
            repository
                .get_file_text_at_path(path, revision)
                .map_err(err)
        })?;
        Ok(PyBytes::new(py, &text))
    }

    /// The signature text stored for `revision_id`, or None if unsigned.
    fn get_signature_text<'py>(
        &self,
        py: Python<'py>,
        revision_id: &[u8],
    ) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let signature = self.with(py, |repository| {
            repository.get_signature_text(revision_id).map_err(err)
        })?;
        Ok(signature.map(|s| PyBytes::new(py, &s)))
    }

    /// Verify the stored GPG signature of `revision_id` against `keyring`
    /// (a list of public-key blobs, ASCII-armored or binary).
    ///
    /// Returns an integer status matching breezy's `gpg` constants:
    /// 0 valid, 1 key missing, 2 not valid, 3 not signed, 4 expired.
    #[cfg(feature = "gpg")]
    fn verify_revision_signature(
        &self,
        py: Python<'_>,
        revision_id: &[u8],
        keyring: Vec<Vec<u8>>,
    ) -> PyResult<u8> {
        let result = self.with(py, |repository| {
            repository
                .verify_revision_signature_bytes(revision_id, &keyring)
                .map_err(err)
        })?;
        Ok(result as u8)
    }

    /// Lock the repository for reading.
    /// Returns a `LogicalLockResult` whose `unlock` releases the lock.
    fn lock_read(slf: &Bound<'_, Self>) -> PyResult<Py<PyLogicalLockResult>> {
        slf.try_borrow_mut()?.with_mut(slf.py(), |repository| {
            repository.lock_read().map_err(repository_err)
        })?;
        logical_lock_result(slf.getattr("unlock")?)
    }

    /// Lock the repository for writing, failing if someone else holds it;
    /// with `token`, take over the lock held under it. Returns a
    /// `RepositoryWriteLockResult` carrying the lock's `repository_token`.
    #[pyo3(signature = (token=None))]
    fn lock_write(
        slf: &Bound<'_, Self>,
        token: Option<String>,
    ) -> PyResult<Py<PyRepositoryWriteLockResult>> {
        let py = slf.py();
        let token = token.map(LockToken::from);
        let locked = slf.try_borrow_mut()?.with_mut(py, |repository| {
            repository
                .lock_write_with_token(token.as_ref(), &mut NoWait)
                .map_err(repository_err)
        })?;
        Py::new(
            py,
            PyRepositoryWriteLockResult::new(
                slf.getattr("unlock")?.unbind(),
                token_object(py, locked.into_token()),
            ),
        )
    }

    /// Release one lock.
    fn unlock(&mut self, py: Python<'_>) -> PyResult<()> {
        self.with_mut(py, |repository| {
            repository.unlock().map(drop).map_err(repository_err)
        })
    }

    fn is_locked(&self, py: Python<'_>) -> PyResult<bool> {
        self.with(py, |repository| Ok(repository.lock().is_locked()))
    }

    fn is_write_locked(&self, py: Python<'_>) -> PyResult<bool> {
        self.with(py, |repository| Ok(repository.is_write_locked()))
    }

    /// Whether the repository's lock directory is held, by anyone.
    fn get_physical_lock_status(&self, py: Python<'_>) -> PyResult<bool> {
        self.with(py, |repository| {
            repository
                .lock()
                .get_physical_lock_status()
                .map_err(|e| lock_err(e, "repository"))
        })
    }

    /// Leave the lock directory held when this object is unlocked.
    fn leave_lock_in_place(&self, py: Python<'_>) -> PyResult<()> {
        self.with(py, |repository| {
            repository.lock().leave_in_place();
            Ok(())
        })
    }

    /// Release the lock directory when this object is unlocked, even if it
    /// did not take it.
    fn dont_leave_lock_in_place(&self, py: Python<'_>) -> PyResult<()> {
        self.with(py, |repository| {
            repository.lock().dont_leave_in_place();
            Ok(())
        })
    }

    /// Break the repository's lock if someone else holds it and
    /// `confirm(holder)` returns true; `holder` is a dict describing the
    /// holder, or `None` when the lock's info is corrupt. Without `confirm`
    /// nothing is broken.
    #[pyo3(signature = (confirm=None))]
    fn break_lock(&self, py: Python<'_>, confirm: Option<Bound<'_, PyAny>>) -> PyResult<()> {
        self.with(py, |repository| {
            with_confirm(py, confirm.as_ref(), |ask| {
                repository.break_lock(ask).map_err(repository_err)
            })
        })
    }

    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        if let RepositoryRef::Branch(branch) = &self.inner {
            visit.call(branch)?;
        }
        Ok(())
    }

    /// Open a write group: a batch of additions flushed by
    /// `commit_write_group`. Writing requires a write lock and an open write
    /// group.
    fn start_write_group(&mut self, py: Python<'_>) -> PyResult<()> {
        self.with_mut(py, |inner| inner.start_write_group().map_err(err))
    }

    /// Flush the open write group, committing its additions.
    fn commit_write_group(&mut self, py: Python<'_>) -> PyResult<()> {
        self.with_mut(py, |inner| inner.commit_write_group().map_err(err))
    }

    /// Combine the repository's packs into a single pack. A no-op for formats
    /// without packs, or a repository already holding one pack.
    fn pack(&mut self, py: Python<'_>) -> PyResult<()> {
        self.with_mut(py, |inner| inner.pack().map_err(err))
    }

    /// Repack the smallest packs if the repository has accumulated too many.
    /// Returns whether a repack happened.
    fn autopack(&mut self, py: Python<'_>) -> PyResult<bool> {
        self.with_mut(py, |inner| inner.autopack().map_err(err))
    }

    /// Check repository integrity. Returns a dict with `checked_revisions`,
    /// `checked_texts`, `ghosts` (a list of revision-id bytes), and `problems`
    /// (a list of description strings). An empty `problems` list means the
    /// repository is consistent.
    fn check<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        self.with(py, |inner| {
            let result = inner.check().map_err(err)?;
            let d = PyDict::new(py);
            d.set_item("checked_revisions", result.checked_revisions)?;
            d.set_item("checked_texts", result.checked_texts)?;
            d.set_item(
                "ghosts",
                PyList::new(py, result.ghosts.iter().map(|g| PyBytes::new(py, g)))?,
            )?;
            d.set_item("problems", result.problems)?;
            Ok(d)
        })
    }

    /// Reconcile (garbage-collect) the repository: regenerate its storage
    /// keeping only data reachable from its revisions and discarding garbage.
    /// Returns a dict with `garbage_inventories` (count of unreachable
    /// inventories dropped) and `repacked` (whether storage was regenerated).
    fn reconcile<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        self.with_mut(py, |inner| {
            let result = inner.reconcile().map_err(err)?;
            let d = PyDict::new(py);
            d.set_item("garbage_inventories", result.garbage_inventories)?;
            d.set_item("repacked", result.repacked)?;
            Ok(d)
        })
    }

    /// Copy revisions from `source` into this repository, returning the number
    /// copied. `revision_id` selects the revision (and its ancestry) to copy;
    /// `None` copies everything the source has. Works across formats.
    #[pyo3(signature = (source, revision_id=None))]
    fn fetch(
        &mut self,
        py: Python<'_>,
        source: &Bound<'_, Repository>,
        revision_id: Option<&[u8]>,
    ) -> PyResult<usize> {
        let source = source.try_borrow()?;
        source.with(py, |source| {
            self.with_mut(py, |target| {
                bazaar::repository::fetch(source, target, revision_id).map_err(err)
            })
        })
    }

    /// Add a file text keyed by `(file_id, revision)` to the open write group.
    /// `parents` is a list of `(file_id, revision)` tuples.
    #[pyo3(signature = (file_id, revision, bytes, parents=None))]
    fn add_text(
        &mut self,
        py: Python<'_>,
        file_id: &[u8],
        revision: &[u8],
        bytes: &[u8],
        parents: Option<Vec<(Vec<u8>, Vec<u8>)>>,
    ) -> PyResult<()> {
        self.with_mut(py, |inner| {
            inner
                .add_text(file_id, revision, &parents.unwrap_or_default(), bytes)
                .map_err(err)
        })
    }

    /// Add a signature text for `revision_id` to the open write group.
    fn add_signature_text(
        &mut self,
        py: Python<'_>,
        revision_id: &[u8],
        signature: &[u8],
    ) -> PyResult<()> {
        self.with_mut(py, |inner| {
            inner
                .add_signature_text(revision_id, signature)
                .map_err(err)
        })
    }

    /// Add a revision to the open write group.
    ///
    /// `revision_id`, `committer` (or None), `message`, `timestamp` (float) and
    /// `timezone` (int seconds east of UTC, or None) describe the revision;
    /// `parents` is its parent revision ids; `revprops` an optional
    /// `{str: bytes}` of revision properties; `inventory_sha1` the optional
    /// recorded inventory sha1. Mirrors the dict shape `get_revision` returns.
    #[pyo3(signature = (revision_id, message, committer, timestamp, timezone,
        parents=None, revprops=None, inventory_sha1=None))]
    #[allow(clippy::too_many_arguments)]
    fn add_revision(
        &mut self,
        py: Python<'_>,
        revision_id: &[u8],
        message: &str,
        committer: Option<String>,
        timestamp: f64,
        timezone: Option<i32>,
        parents: Option<Vec<Vec<u8>>>,
        revprops: Option<&Bound<'_, PyDict>>,
        inventory_sha1: Option<Vec<u8>>,
    ) -> PyResult<()> {
        self.with_mut(py, |inner| {
            let parents = parents.unwrap_or_default();
            let mut properties: std::collections::HashMap<String, Vec<u8>> =
                std::collections::HashMap::new();
            if let Some(props) = revprops {
                for (k, v) in props.iter() {
                    properties.insert(k.extract()?, v.extract()?);
                }
            }
            let revision = bazaar::revision::Revision::new(
                bazaar::RevisionId::from(revision_id),
                parents
                    .iter()
                    .map(|p| bazaar::RevisionId::from(p.as_slice()))
                    .collect(),
                committer,
                message.to_string(),
                properties,
                inventory_sha1,
                timestamp,
                timezone,
            );
            inner.add_revision(&revision, &parents).map_err(err)
        })
    }

    /// The inventory of a revision, as a list of `(path, kind, file_id)`.
    fn get_inventory<'py>(
        &self,
        py: Python<'py>,
        revision_id: &[u8],
    ) -> PyResult<Bound<'py, PyList>> {
        self.with(py, |inner| {
            let inv = inner.get_inventory(revision_id).map_err(err)?;
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
        })
    }
}

/// A bzr branch.
#[pyclass(name = "Branch")]
struct Branch {
    inner: BranchRef,
    /// The `repository` object handed out, so it is the same each time.
    repository: Option<Py<Repository>>,
}

impl Branch {
    /// Run `f` on the branch, wherever it lives.
    fn with<R>(&self, py: Python<'_>, f: impl FnOnce(&RsBranch) -> PyResult<R>) -> PyResult<R> {
        match &self.inner {
            BranchRef::Owned(branch) => f(branch),
            BranchRef::Tree(tree) => f(tree.bind(py).try_borrow()?.inner.branch()),
        }
    }

    /// Run `f` on the branch for writing, wherever it lives.
    fn with_mut<R>(
        &mut self,
        py: Python<'_>,
        f: impl FnOnce(&mut RsBranch) -> PyResult<R>,
    ) -> PyResult<R> {
        match &mut self.inner {
            BranchRef::Owned(branch) => f(branch),
            BranchRef::Tree(tree) => f(tree.bind(py).try_borrow_mut()?.inner.branch_mut()),
        }
    }
}

#[pymethods]
impl Branch {
    /// The repository holding the branch's revisions; the same object each
    /// time. Locking the branch locks it too.
    #[getter]
    fn repository(slf: &Bound<'_, Self>) -> PyResult<Py<Repository>> {
        let py = slf.py();
        if let Some(repository) = &slf.try_borrow()?.repository {
            return Ok(repository.clone_ref(py));
        }
        let repository = Py::new(
            py,
            Repository {
                inner: RepositoryRef::Branch(slf.clone().unbind()),
            },
        )?;
        slf.try_borrow_mut()?.repository = Some(repository.clone_ref(py));
        Ok(repository)
    }

    /// Lock the branch, and its repository with the first lock, for reading.
    /// Returns a `LogicalLockResult` whose `unlock` releases the lock.
    fn lock_read(slf: &Bound<'_, Self>) -> PyResult<Py<PyLogicalLockResult>> {
        slf.try_borrow_mut()?
            .with_mut(slf.py(), |branch| branch.lock_read().map_err(branch_err))?;
        logical_lock_result(slf.getattr("unlock")?)
    }

    /// Lock the branch, and its repository with the first lock, for writing,
    /// failing if someone else holds it; with `token`, take over the branch
    /// lock held under it. Returns a `BranchWriteLockResult` carrying the
    /// lock's `token`.
    #[pyo3(signature = (token=None))]
    fn lock_write(
        slf: &Bound<'_, Self>,
        token: Option<String>,
    ) -> PyResult<Py<PyBranchWriteLockResult>> {
        let py = slf.py();
        let token = token.map(LockToken::from);
        let locked = slf.try_borrow_mut()?.with_mut(py, |branch| {
            branch
                .lock_write_with_token(token.as_ref(), &mut NoWait)
                .map_err(branch_err)
        })?;
        Py::new(
            py,
            PyBranchWriteLockResult::new(
                slf.getattr("unlock")?.unbind(),
                token_object(py, locked.into_token()),
            ),
        )
    }

    /// Release one lock, and the repository's with the last.
    fn unlock(&mut self, py: Python<'_>) -> PyResult<()> {
        self.with_mut(py, |branch| branch.unlock().map(drop).map_err(branch_err))
    }

    fn is_locked(&self, py: Python<'_>) -> PyResult<bool> {
        self.with(py, |branch| Ok(branch.lock().is_locked()))
    }

    /// The mode the branch is locked in, `"r"` or `"w"`, or `None`.
    fn peek_lock_mode(&self, py: Python<'_>) -> PyResult<Option<&'static str>> {
        self.with(py, |branch| Ok(lock_mode_str(branch.lock().lock_mode())))
    }

    /// Whether the branch's lock directory is held, by anyone.
    fn get_physical_lock_status(&self, py: Python<'_>) -> PyResult<bool> {
        self.with(py, |branch| {
            branch
                .lock()
                .get_physical_lock_status()
                .map_err(|e| lock_err(e, "branch"))
        })
    }

    /// Leave the lock directory held when this object is unlocked.
    fn leave_lock_in_place(&self, py: Python<'_>) -> PyResult<()> {
        self.with(py, |branch| {
            branch.lock().leave_in_place();
            Ok(())
        })
    }

    /// Release the lock directory when this object is unlocked, even if it
    /// did not take it.
    fn dont_leave_lock_in_place(&self, py: Python<'_>) -> PyResult<()> {
        self.with(py, |branch| {
            branch.lock().dont_leave_in_place();
            Ok(())
        })
    }

    /// Break the branch's lock, then its repository's, if someone else holds
    /// them and `confirm(holder)` returns true, as for
    /// `Repository.break_lock`.
    #[pyo3(signature = (confirm=None))]
    fn break_lock(&self, py: Python<'_>, confirm: Option<Bound<'_, PyAny>>) -> PyResult<()> {
        self.with(py, |branch| {
            with_confirm(py, confirm.as_ref(), |ask| {
                branch.break_lock(ask).map_err(branch_err)
            })
        })
    }

    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        if let Some(repository) = &self.repository {
            visit.call(repository)?;
        }
        if let BranchRef::Tree(tree) = &self.inner {
            visit.call(tree)?;
        }
        Ok(())
    }

    fn __clear__(&mut self) {
        self.repository = None;
    }

    /// This branch's format as `{format_string: bytes, description: str}`.
    fn format<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        self.with(py, |inner| {
            let fmt = inner.format();
            let d = PyDict::new(py);
            d.set_item("format_string", PyBytes::new(py, fmt.format_string()))?;
            d.set_item("description", fmt.get_format_description())?;
            Ok(d)
        })
    }

    /// The tip as `(revno, revision_id)`.
    fn last_revision_info<'py>(&self, py: Python<'py>) -> PyResult<(u64, Bound<'py, PyBytes>)> {
        self.with(py, |inner| {
            let (revno, revid) = inner.last_revision_info().map_err(err)?;
            Ok((revno, PyBytes::new(py, &revid)))
        })
    }

    /// The tip revision id (`b"null:"` for an empty branch).
    fn last_revision<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        self.with(py, |inner| {
            Ok(PyBytes::new(py, &inner.last_revision().map_err(err)?))
        })
    }

    /// The branch tags as a `{name: revision_id}` dict.
    fn tags<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        self.with(py, |inner| {
            let tags = inner.tags().map_err(err)?;
            let d = PyDict::new(py);
            for (name, target) in tags {
                d.set_item(name, PyBytes::new(py, &target))?;
            }
            Ok(d)
        })
    }

    /// The mainline revision ids, oldest first. For a format-5 branch this is
    /// the full `revision-history`; for 6/7/8 it is the tip alone (or empty).
    fn revision_history<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        self.with(py, |inner| {
            let history = inner.revision_history().map_err(err)?;
            PyList::new(py, history.iter().map(|r| PyBytes::new(py, r)))
        })
    }

    /// Replace the full mainline (format 5) from a list of revision ids.
    fn set_revision_history(&mut self, py: Python<'_>, history: Vec<Vec<u8>>) -> PyResult<()> {
        self.with_mut(py, |inner| {
            inner.set_revision_history(&history).map_err(err)
        })
    }

    /// The raw bytes of `branch.conf` (empty if the file is absent).
    fn get_config_bytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        self.with(py, |inner| {
            Ok(PyBytes::new(py, &inner.get_config_bytes().map_err(err)?))
        })
    }

    /// Set the tip to `(revno, revision_id)`.
    fn set_last_revision_info(
        &mut self,
        py: Python<'_>,
        revno: u64,
        revision_id: &[u8],
    ) -> PyResult<()> {
        self.with_mut(py, |inner| {
            inner
                .set_last_revision_info(revno, revision_id)
                .map_err(err)
        })
    }

    /// Replace the branch tags from a `{name: revision_id}` dict.
    fn set_tags(&mut self, py: Python<'_>, tags: &Bound<'_, PyDict>) -> PyResult<()> {
        self.with_mut(py, |inner| {
            let mut map: BTreeMap<String, Vec<u8>> = BTreeMap::new();
            for (k, v) in tags.iter() {
                map.insert(k.extract()?, v.extract()?);
            }
            inner.set_tags(&map).map_err(err)
        })
    }

    /// The URL this branch is stacked on. Raises `NotStacked` when a stackable
    /// branch has no stacked-on location, and `UnstackableBranchFormat` for a
    /// format that does not support stacking.
    fn get_stacked_on_url(&self, py: Python<'_>) -> PyResult<String> {
        self.with(py, |inner| inner.get_stacked_on_url().map_err(branch_err))
    }

    /// Set (or clear, with `None`) the URL this branch is stacked on.
    #[pyo3(signature = (url=None))]
    fn set_stacked_on_url(&mut self, py: Python<'_>, url: Option<&str>) -> PyResult<()> {
        self.with_mut(py, |inner| {
            inner.set_stacked_on_url(url).map_err(branch_err)
        })
    }

    /// The master branch URL this branch is bound to, or `None` if unbound.
    fn get_bound_location(&self, py: Python<'_>) -> PyResult<Option<String>> {
        self.with(py, |inner| inner.get_bound_location().map_err(branch_err))
    }

    /// The previous master URL after an unbind, or `None`.
    fn get_old_bound_location(&self, py: Python<'_>) -> PyResult<Option<String>> {
        self.with(py, |inner| {
            inner.get_old_bound_location().map_err(branch_err)
        })
    }

    /// Bind this branch to `location` (its new master).
    fn bind(&mut self, py: Python<'_>, location: &str) -> PyResult<()> {
        self.with_mut(py, |inner| inner.bind(location).map_err(branch_err))
    }

    /// Unbind this branch.
    fn unbind(&mut self, py: Python<'_>) -> PyResult<()> {
        self.with_mut(py, |inner| inner.unbind().map_err(branch_err))
    }

    /// The `(branch_location, tree_path)` recorded for a tree-reference
    /// `file_id`, or `(None, None)` if none. Raises `UnsupportedOperation` on a
    /// format without reference locations.
    fn get_reference_info(
        &self,
        py: Python<'_>,
        file_id: &[u8],
    ) -> PyResult<(Option<String>, Option<String>)> {
        self.with(py, |inner| {
            inner.get_reference_info(file_id).map_err(branch_err)
        })
    }

    /// Record (or, with `branch_location=None`, delete) the reference location
    /// for a tree-reference `file_id`.
    #[pyo3(signature = (file_id, branch_location=None, tree_path=None))]
    fn set_reference_info(
        &mut self,
        py: Python<'_>,
        file_id: &[u8],
        branch_location: Option<&str>,
        tree_path: Option<&str>,
    ) -> PyResult<()> {
        self.with_mut(py, |inner| {
            inner
                .set_reference_info(file_id, branch_location, tree_path)
                .map_err(branch_err)
        })
    }

    /// The URL a branch-reference points at, or `None` if this is not a branch
    /// reference.
    fn get_reference(&self, py: Python<'_>) -> PyResult<Option<String>> {
        self.with(py, |inner| inner.get_reference().map_err(branch_err))
    }

    /// Point this branch reference at `to_url`.
    fn set_reference(&mut self, py: Python<'_>, to_url: &str) -> PyResult<()> {
        self.with_mut(py, |inner| inner.set_reference(to_url).map_err(branch_err))
    }
}

/// A working tree, backed by whichever on-disk format was opened.
#[pyclass(name = "WorkingTree")]
struct WorkingTree {
    inner: Box<dyn RsWorkingTree>,
    /// The `branch` object handed out, so it is the same each time.
    branch: Option<Py<Branch>>,
}

#[pymethods]
impl WorkingTree {
    /// The branch this tree is a checkout of; the same object each time.
    /// Locking the tree locks it too.
    #[getter]
    fn branch(slf: &Bound<'_, Self>) -> PyResult<Py<Branch>> {
        let py = slf.py();
        if let Some(branch) = &slf.try_borrow()?.branch {
            return Ok(branch.clone_ref(py));
        }
        let branch = Py::new(
            py,
            Branch {
                inner: BranchRef::Tree(slf.clone().unbind()),
                repository: None,
            },
        )?;
        slf.try_borrow_mut()?.branch = Some(branch.clone_ref(py));
        Ok(branch)
    }

    /// Lock the tree and its branch for reading. Returns a
    /// `LogicalLockResult` whose `unlock` releases the lock.
    fn lock_read(slf: &Bound<'_, Self>) -> PyResult<Py<PyLogicalLockResult>> {
        slf.try_borrow_mut()?.inner.lock_read().map_err(tree_err)?;
        logical_lock_result(slf.getattr("unlock")?)
    }

    /// Lock the tree for writing and its branch for reading, failing if
    /// someone else holds the tree. Returns a `LogicalLockResult`.
    fn lock_tree_write(slf: &Bound<'_, Self>) -> PyResult<Py<PyLogicalLockResult>> {
        slf.try_borrow_mut()?
            .inner
            .lock_tree_write(&mut NoWait)
            .map_err(tree_err)?;
        logical_lock_result(slf.getattr("unlock")?)
    }

    /// Lock the tree and its branch for writing, failing if someone else
    /// holds either. Returns a `LogicalLockResult`.
    fn lock_write(slf: &Bound<'_, Self>) -> PyResult<Py<PyLogicalLockResult>> {
        slf.try_borrow_mut()?
            .inner
            .lock_write(&mut NoWait)
            .map_err(tree_err)?;
        logical_lock_result(slf.getattr("unlock")?)
    }

    /// Release one lock on the tree and its branch, saving the tree's changes
    /// with the last write lock.
    fn unlock(&mut self) -> PyResult<()> {
        self.inner.unlock().map(drop).map_err(tree_err)
    }

    fn is_locked(&self) -> bool {
        self.inner.lock().files().is_locked()
    }

    /// Whether the tree's lock directory is held, by anyone.
    fn get_physical_lock_status(&self) -> PyResult<bool> {
        self.inner
            .lock()
            .files()
            .get_physical_lock_status()
            .map_err(|e| lock_err(e, "working tree"))
    }

    /// Break the tree's lock, then its branch's and repository's, if someone
    /// else holds them and `confirm(holder)` returns true, as for
    /// `Repository.break_lock`. A tree whose dirstate is in use raises
    /// `LockActive`.
    #[pyo3(signature = (confirm=None))]
    fn break_lock(&self, py: Python<'_>, confirm: Option<Bound<'_, PyAny>>) -> PyResult<()> {
        with_confirm(py, confirm.as_ref(), |ask| {
            self.inner.break_lock(ask).map_err(tree_err)
        })
    }

    fn __traverse__(&self, visit: PyVisit<'_>) -> Result<(), PyTraverseError> {
        if let Some(branch) = &self.branch {
            visit.call(branch)?;
        }
        Ok(())
    }

    fn __clear__(&mut self) {
        self.branch = None;
    }

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

    /// Whether this working-tree format stores views (format 6 only).
    fn supports_views(&self) -> bool {
        self.inner.supports_views()
    }

    /// The defined views as `(current_view, {name: [paths]})`. `current_view`
    /// is the enabled view's name or None.
    fn views<'py>(&self, py: Python<'py>) -> PyResult<(Option<String>, Bound<'py, PyDict>)> {
        let info = self.inner.views().map_err(err)?;
        let d = PyDict::new(py);
        for (name, paths) in &info.views {
            d.set_item(name, paths.clone())?;
        }
        Ok((info.current, d))
    }

    /// Set the defined views and current-view selection. `views` is a
    /// `{name: [paths]}` dict; `current` names an enabled view (or None).
    #[pyo3(signature = (views, current=None))]
    fn set_views(&self, views: &Bound<'_, PyDict>, current: Option<String>) -> PyResult<()> {
        let mut info = bazaar::workingtree::ViewInfo {
            current,
            views: std::collections::BTreeMap::new(),
        };
        for (k, v) in views.iter() {
            info.views.insert(k.extract()?, v.extract()?);
        }
        self.inner.set_views(&info).map_err(err)
    }

    /// The recorded conflicts, each a dict with `type`, `path`, and optional
    /// `file_id`, `action`, `conflict_path` and `conflict_file_id`.
    fn conflicts<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let conflicts = self.inner.conflicts().map_err(err)?;
        let items: Vec<Bound<'py, PyDict>> = conflicts
            .iter()
            .map(|c| {
                let d = PyDict::new(py);
                d.set_item("type", &c.typestring)?;
                d.set_item("path", &c.path)?;
                if let Some(fid) = &c.file_id {
                    d.set_item("file_id", PyBytes::new(py, fid))?;
                }
                if let Some(action) = &c.action {
                    d.set_item("action", action)?;
                }
                if let Some(conflict_path) = &c.conflict_path {
                    d.set_item("conflict_path", conflict_path)?;
                }
                if let Some(fid) = &c.conflict_file_id {
                    d.set_item("conflict_file_id", PyBytes::new(py, fid))?;
                }
                Ok::<_, PyErr>(d)
            })
            .collect::<Result<_, _>>()?;
        PyList::new(py, items)
    }

    /// Replace the recorded conflicts. `conflicts` is a list of dicts with
    /// `type`, `path`, and optional `file_id`, `action`, `conflict_path` and
    /// `conflict_file_id`.
    fn set_conflicts(&self, conflicts: Vec<Bound<'_, PyDict>>) -> PyResult<()> {
        let mut out = Vec::with_capacity(conflicts.len());
        for d in &conflicts {
            let typestring: String = d
                .get_item("type")?
                .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err("conflict missing 'type'"))?
                .extract()?;
            let path: String = d
                .get_item("path")?
                .ok_or_else(|| pyo3::exceptions::PyKeyError::new_err("conflict missing 'path'"))?
                .extract()?;
            let file_id: Option<Vec<u8>> = match d.get_item("file_id")? {
                Some(v) if !v.is_none() => Some(v.extract()?),
                _ => None,
            };
            let action: Option<String> = match d.get_item("action")? {
                Some(v) if !v.is_none() => Some(v.extract()?),
                _ => None,
            };
            let conflict_path: Option<String> = match d.get_item("conflict_path")? {
                Some(v) if !v.is_none() => Some(v.extract()?),
                _ => None,
            };
            let conflict_file_id: Option<Vec<u8>> = match d.get_item("conflict_file_id")? {
                Some(v) if !v.is_none() => Some(v.extract()?),
                _ => None,
            };
            out.push(bazaar::workingtree::Conflict {
                typestring,
                path,
                file_id,
                action,
                conflict_path,
                conflict_file_id,
            });
        }
        self.inner.set_conflicts(&out).map_err(err)
    }

    /// The changes between this working tree and the basis `basis_revision_id`
    /// (resolved against `repository`), as a list of dicts with keys
    /// `file_id`, `old_path`, `new_path`, `content_change`, `kind`,
    /// `executable`. A `None` path means the entry is added (`old_path`) or
    /// removed (`new_path`).
    fn iter_changes(
        &self,
        repository: &Bound<'_, Repository>,
        basis_revision_id: &[u8],
    ) -> PyResult<TreeChangesIter> {
        let basis = repository
            .try_borrow()?
            .with(repository.py(), |repository| {
                repository.revision_tree(basis_revision_id).map_err(err)
            })?;
        // The tree-vs-basis diff is a whole-tree comparison, so it runs
        // here; the per-change dicts are built on demand during iteration.
        let changes = self.inner.iter_changes(&basis).map_err(err)?;
        Ok(TreeChangesIter {
            changes: changes.into(),
        })
    }

    /// As `iter_changes`, but also considering the non-basis merge parents in
    /// `other_revision_ids` for per-file text parents.
    fn iter_changes_with_parents(
        &self,
        repository: &Bound<'_, Repository>,
        basis_revision_id: &[u8],
        other_revision_ids: Vec<Vec<u8>>,
    ) -> PyResult<TreeChangesIter> {
        let (basis, others) = repository
            .try_borrow()?
            .with(repository.py(), |repository| {
                let basis = repository.revision_tree(basis_revision_id).map_err(err)?;
                let others: Vec<_> = other_revision_ids
                    .iter()
                    .map(|r| repository.revision_tree(r).map_err(err))
                    .collect::<Result<_, _>>()?;
                Ok((basis, others))
            })?;
        let changes = self
            .inner
            .iter_changes_with_parents(&basis, &others)
            .map_err(err)?;
        Ok(TreeChangesIter {
            changes: changes.into(),
        })
    }

    /// Commit the live tree state as a new revision and return its id.
    ///
    /// `revprops` is an optional `{str: bytes}` dict of revision properties;
    /// `authors` an optional list of author strings; `revision_id` an
    /// optional explicit id (generated when omitted).
    #[pyo3(signature = (committer, message, timestamp, timezone,
        revprops=None, authors=None, revision_id=None, branch_nick=None,
        allow_pointless=false, strict=false, specific_files=None, exclude=None,
        signing_key=None))]
    #[allow(clippy::too_many_arguments)]
    fn commit<'py>(
        &mut self,
        py: Python<'py>,
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
        let revid = self.inner.commit(&options).map_err(tree_err)?;
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

/// Create a shared repository (no branch or working tree) at `path` in
/// `format` and open it. The repository serves branches in sibling control
/// directories that resolve to it via `find_repository`.
///
/// `format` is a metadir format name as accepted by [`create`] (the all-in-one
/// "weave" format cannot be a shared repository).
#[pyfunction]
#[pyo3(signature = (path, format="2a"))]
fn create_shared_repository(path: &str, format: &str) -> PyResult<BzrDir> {
    let parent = local(path);
    let fmt = find_control_dir_format(format).ok_or_else(|| {
        pyo3::exceptions::PyValueError::new_err(format!("unknown control dir format: {format}"))
    })?;
    Ok(BzrDir {
        inner: Box::new(
            BzrDirMeta::create_shared_repository_with_format(&parent, fmt).map_err(err)?,
        ),
    })
}

/// Upgrade the control directory at `path` to `format`.
///
/// Builds a fresh control directory in the target format, fetches every
/// revision, carries over the branch tip and tags, and moves the old `.bzr`
/// aside to `backup.bzr`. `format` is a metadir format name as accepted by
/// [`create`].
#[pyfunction]
fn upgrade(path: &str, format: &str) -> PyResult<()> {
    let parent = local(path);
    let fmt = find_control_dir_format(format).ok_or_else(|| {
        pyo3::exceptions::PyValueError::new_err(format!("unknown control dir format: {format}"))
    })?;
    bazaar::bzrdir::upgrade(&parent, fmt).map_err(err)
}

/// The control-directory format names accepted by [`create`].
#[pyfunction]
fn format_names() -> Vec<&'static str> {
    let mut names: Vec<&'static str> = bazaar::bzrdir::control_dir_formats()
        .iter()
        .filter_map(|f| f.name)
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
    m.add_function(wrap_pyfunction!(create_shared_repository, &m)?)?;
    m.add_function(wrap_pyfunction!(upgrade, &m)?)?;
    m.add_function(wrap_pyfunction!(format_names, &m)?)?;
    Ok(m)
}
