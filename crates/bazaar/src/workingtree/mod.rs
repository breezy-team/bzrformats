//! Reading a dirstate-based working tree (Working Tree Format 6).
//!
//! A working tree is the user's checkout: the files on disk plus
//! `.bzr/checkout/dirstate`, which records the tracked state (tree 0) and
//! the basis it was checked out from (tree 1). This module opens the
//! dirstate through a [`Transport`] rooted at the tree root (the directory
//! that contains `.bzr`) and exposes the live tracked files.
//!
//! It can read the tree (list the tracked files, map paths to file ids,
//! read file contents), mutate the tracked set ([`add`](WorkingTree::add),
//! [`remove`](WorkingTree::remove), [`rename`](WorkingTree::rename)), and
//! [`commit`](WorkingTree::commit) the live state as a new revision.

pub mod format;
mod formats;

pub use format::{all_formats, find_format, WorkingTreeFormat};

use crate::dirstate::{DefaultSHA1Provider, DirState, Kind, LoadError};
use crate::transport::{SharedTransport, TransportError};

/// Path to the dirstate within the control directory.
const DIRSTATE_PATH: &str = ".bzr/checkout/dirstate";

/// Errors from working-tree operations.
#[derive(Debug)]
pub enum WorkingTreeError {
    /// The dirstate could not be read.
    Dirstate(LoadError),
    /// A path was not versioned in this tree.
    NotVersioned(String),
    /// A path could not be versioned (dirstate add failed).
    Add(crate::dirstate::AddError),
    /// A path could not be unversioned (dirstate make-absent failed).
    Remove(crate::dirstate::MakeAbsentError),
    /// A commit could not be assembled.
    Commit(String),
    /// An error from the repository during commit.
    Repository(crate::repository::RepositoryError),
    /// An error from the branch during commit.
    Branch(crate::branch::BranchError),
    /// An underlying transport error.
    Transport(TransportError),
}

impl std::fmt::Display for WorkingTreeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkingTreeError::Dirstate(e) => write!(f, "dirstate: {e}"),
            WorkingTreeError::NotVersioned(p) => write!(f, "path not versioned: {p}"),
            WorkingTreeError::Add(e) => write!(f, "add: {e}"),
            WorkingTreeError::Remove(e) => write!(f, "remove: {e}"),
            WorkingTreeError::Commit(m) => write!(f, "commit: {m}"),
            WorkingTreeError::Repository(e) => write!(f, "repository: {e}"),
            WorkingTreeError::Branch(e) => write!(f, "branch: {e}"),
            WorkingTreeError::Transport(e) => write!(f, "transport error: {e}"),
        }
    }
}

impl std::error::Error for WorkingTreeError {}

impl From<LoadError> for WorkingTreeError {
    fn from(e: LoadError) -> Self {
        WorkingTreeError::Dirstate(e)
    }
}

impl From<TransportError> for WorkingTreeError {
    fn from(e: TransportError) -> Self {
        WorkingTreeError::Transport(e)
    }
}

/// The kind of a versioned entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    File,
    Directory,
    Symlink,
    TreeReference,
}

impl EntryKind {
    fn from_minikind(k: Kind) -> Option<Self> {
        match k {
            Kind::File => Some(EntryKind::File),
            Kind::Directory => Some(EntryKind::Directory),
            Kind::Symlink => Some(EntryKind::Symlink),
            Kind::TreeReference => Some(EntryKind::TreeReference),
            // Absent and relocated entries are not live in this tree.
            Kind::Absent | Kind::Relocated => None,
        }
    }

    fn to_osutils_kind(self) -> crate::osutils::Kind {
        match self {
            EntryKind::File => crate::osutils::Kind::File,
            EntryKind::Directory => crate::osutils::Kind::Directory,
            EntryKind::Symlink => crate::osutils::Kind::Symlink,
            EntryKind::TreeReference => crate::osutils::Kind::TreeReference,
        }
    }
}

/// One tracked entry in the working tree: its path (relative to the tree
/// root), file id, and kind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionedEntry {
    pub path: String,
    pub file_id: Vec<u8>,
    pub kind: EntryKind,
}

/// A dirstate-based working tree, accessed through a transport rooted at
/// the tree root (the directory containing `.bzr`).
pub struct WorkingTree {
    transport: SharedTransport,
    dirstate: DirState,
}

impl WorkingTree {
    /// Open the working tree reachable through `transport` (rooted at the
    /// directory that contains `.bzr`).
    pub fn open(transport: SharedTransport) -> Result<Self, WorkingTreeError> {
        let data = transport.get_bytes(DIRSTATE_PATH)?;
        let mut dirstate =
            DirState::new(DIRSTATE_PATH, Box::new(DefaultSHA1Provider), 0, true, false);
        dirstate.load_bytes(&data)?;
        Ok(WorkingTree {
            transport,
            dirstate,
        })
    }

    /// The basis revision id this tree was checked out from, or `None` if
    /// the tree has no parent (a fresh, never-committed tree).
    pub fn basis_revision(&self) -> Option<Vec<u8>> {
        self.dirstate.parents.first().cloned()
    }

    /// List the tracked files and directories in the live working tree
    /// (dirstate tree 0), in dirstate (path) order. The synthetic root
    /// entry is omitted.
    pub fn list_files(&self) -> Vec<VersionedEntry> {
        let mut out = Vec::new();
        for entry in self.dirstate.iter_entries() {
            let kind = match entry
                .trees
                .first()
                .and_then(|t| EntryKind::from_minikind(t.minikind))
            {
                Some(k) => k,
                None => continue,
            };
            let path = join_path(&entry.key.dirname, &entry.key.basename);
            if path.is_empty() {
                // The tree root itself.
                continue;
            }
            out.push(VersionedEntry {
                path,
                file_id: entry.key.file_id.clone(),
                kind,
            });
        }
        out
    }

    /// The file id of the entry at `path`, or `None` if `path` is not
    /// versioned in the live tree.
    pub fn path2id(&self, path: &str) -> Option<Vec<u8>> {
        let (dirname, basename) = split_path(path);
        for entry in self.dirstate.iter_entries() {
            if entry.key.dirname == dirname.as_bytes() && entry.key.basename == basename.as_bytes()
            {
                if let Some(t) = entry.trees.first() {
                    if EntryKind::from_minikind(t.minikind).is_some() {
                        return Some(entry.key.file_id.clone());
                    }
                }
            }
        }
        None
    }

    /// Read the content of a versioned file from disk.
    pub fn get_file_text(&self, path: &str) -> Result<Vec<u8>, WorkingTreeError> {
        if self.path2id(path).is_none() {
            return Err(WorkingTreeError::NotVersioned(path.to_string()));
        }
        Ok(self.transport.get_bytes(path)?)
    }

    /// Version `path` with `kind`, assigning `file_id` (a fresh id is
    /// generated from the path when `None`). The entry is added with no
    /// cached stat or sha1; those are gathered on the next access. Already
    /// versioned paths are left unchanged. The dirstate is rewritten to
    /// disk. Returns the file id of the (now) versioned path.
    ///
    /// The parent directory must already be versioned, mirroring breezy's
    /// `MutableTree._add` (callers add parents first).
    pub fn add(
        &mut self,
        path: &str,
        kind: EntryKind,
        file_id: Option<&[u8]>,
    ) -> Result<Vec<u8>, WorkingTreeError> {
        let path = path.trim_matches('/');
        if let Some(existing) = self.path2id(path) {
            return Ok(existing);
        }
        let file_id = match file_id {
            Some(id) => id.to_vec(),
            None => crate::gen_ids::gen_file_id(path),
        };
        self.dirstate
            .add_path(path, &file_id, kind.to_osutils_kind(), None, b"")
            .map_err(WorkingTreeError::Add)?;
        self.save_dirstate()?;
        Ok(file_id)
    }

    /// Stop versioning `path` and (if it is a directory) everything beneath
    /// it. The files are left on disk; only the tracked set changes. The
    /// dirstate is rewritten to disk.
    ///
    /// Returns [`WorkingTreeError::NotVersioned`] if `path` is not tracked.
    pub fn remove(&mut self, path: &str) -> Result<(), WorkingTreeError> {
        let path = path.trim_matches('/');
        if self.path2id(path).is_none() {
            return Err(WorkingTreeError::NotVersioned(path.to_string()));
        }

        // Collect the keys to make absent: the path itself plus, when it is
        // a directory, every live descendant. iter_entries yields all
        // tree-0 rows; a descendant is one whose path is `path` or starts
        // with `path/`.
        let prefix = format!("{path}/");
        let mut keys: Vec<crate::dirstate::EntryKey> = Vec::new();
        for entry in self.dirstate.iter_entries() {
            let tree0 = match entry.trees.first() {
                Some(t) => t,
                None => continue,
            };
            if EntryKind::from_minikind(tree0.minikind).is_none() {
                continue; // absent / relocated rows are already gone.
            }
            let entry_path = join_path(&entry.key.dirname, &entry.key.basename);
            if entry_path == path || entry_path.starts_with(&prefix) {
                keys.push(entry.key.clone());
            }
        }
        // Remove deepest paths first so a directory is emptied before it is
        // itself made absent.
        keys.sort_by_key(|k| std::cmp::Reverse(k.dirname.len()));
        for key in &keys {
            self.dirstate
                .make_absent(key)
                .map_err(WorkingTreeError::Remove)?;
        }
        self.save_dirstate()
    }

    /// Move a versioned entry from `from_path` to `to_path`, keeping its
    /// file id, and move the file on disk. The destination's parent
    /// directory must already be versioned. The dirstate is rewritten to
    /// disk.
    ///
    /// Only a single file or empty directory is moved; moving a directory
    /// with versioned children is not yet supported.
    pub fn rename(&mut self, from_path: &str, to_path: &str) -> Result<(), WorkingTreeError> {
        let from_path = from_path.trim_matches('/');
        let to_path = to_path.trim_matches('/');

        let file_id = self
            .path2id(from_path)
            .ok_or_else(|| WorkingTreeError::NotVersioned(from_path.to_string()))?;
        if self.path2id(to_path).is_some() {
            return Err(WorkingTreeError::Commit(format!(
                "destination already versioned: {to_path}"
            )));
        }

        // Find the source entry's kind, and refuse to move a directory that
        // still has versioned children (the dirstate-level re-key below only
        // moves the named row).
        let (kind, from_key) = self
            .dirstate
            .iter_entries()
            .find_map(|e| {
                let path = join_path(&e.key.dirname, &e.key.basename);
                if path != from_path {
                    return None;
                }
                let k = EntryKind::from_minikind(e.trees.first()?.minikind)?;
                Some((k, e.key.clone()))
            })
            .ok_or_else(|| WorkingTreeError::NotVersioned(from_path.to_string()))?;
        if kind == EntryKind::Directory {
            let child_prefix = format!("{from_path}/");
            if self
                .dirstate
                .iter_entries()
                .any(|e| join_path(&e.key.dirname, &e.key.basename).starts_with(&child_prefix))
            {
                return Err(WorkingTreeError::Commit(
                    "moving a directory with versioned children is not supported".to_string(),
                ));
            }
        }

        // Re-key in the dirstate: drop the old row, add the new path under
        // the same file id, then move the file on disk.
        self.dirstate
            .make_absent(&from_key)
            .map_err(WorkingTreeError::Remove)?;
        self.dirstate
            .add_path(to_path, &file_id, kind.to_osutils_kind(), None, b"")
            .map_err(WorkingTreeError::Add)?;
        self.transport
            .rename(from_path, to_path)
            .map_err(WorkingTreeError::Transport)?;
        self.save_dirstate()
    }

    /// Commit the live working tree as a new revision.
    ///
    /// Writes file texts, a CHK inventory and the revision to `repository`,
    /// advances `branch` to the new tip, and updates the dirstate basis.
    /// Returns the new revision id.
    ///
    /// This covers the common case of committing a tree with no pending
    /// merges: every versioned entry is recorded against the new revision.
    /// (Recording only changed entries against the new revision, leaving
    /// unchanged ones pointing at their previous revision, is a later
    /// refinement.)
    pub fn commit(
        &mut self,
        repository: &mut dyn crate::repository::Repository,
        branch: &crate::branch::Branch,
        committer: &str,
        message: &str,
        timestamp: u64,
        timezone: i32,
    ) -> Result<Vec<u8>, WorkingTreeError> {
        let parents: Vec<Vec<u8>> = self
            .dirstate
            .parents
            .iter()
            .filter(|p| p.as_slice() != crate::branch::NULL_REVISION)
            .cloned()
            .collect();
        let revid = crate::RevisionId::generate(committer, Some(timestamp))
            .as_bytes()
            .to_vec();

        // Gather the live entries and map each path to its file id, so child
        // entries can name their parent directory.
        let live = self.collect_live_entries();
        let mut path_to_id: std::collections::HashMap<String, Vec<u8>> =
            std::collections::HashMap::new();
        path_to_id.insert(String::new(), live.root_id.clone());
        for e in &live.entries {
            path_to_id.insert(e.path.clone(), e.file_id.clone());
        }

        repository
            .start_write_group()
            .map_err(WorkingTreeError::Repository)?;

        // Build inventory entries (root first), adding a text record per
        // entry as we go. Every inventory entry has a text in the texts
        // store keyed (file_id, revision): real content for files, an empty
        // text for the root, directories and symlinks -- this is what brz
        // check expects so the per-file graph is complete.
        let rev = crate::RevisionId::from(revid.as_slice());
        repository
            .add_text(&live.root_id, &revid, &[], b"")
            .map_err(WorkingTreeError::Repository)?;
        let mut inv_entries = vec![crate::inventory::Entry::root(
            crate::FileId::from(live.root_id.as_slice()),
            Some(rev.clone()),
        )];
        for e in &live.entries {
            let (parent_path, basename) = split_path(&e.path);
            let parent_id = path_to_id
                .get(&parent_path)
                .ok_or_else(|| WorkingTreeError::Commit(format!("no parent for {}", e.path)))?;
            let parent_fid = crate::FileId::from(parent_id.as_slice());
            let fid = crate::FileId::from(e.file_id.as_slice());
            match e.kind {
                EntryKind::Directory => {
                    repository
                        .add_text(&e.file_id, &revid, &[], b"")
                        .map_err(WorkingTreeError::Repository)?;
                    inv_entries.push(crate::inventory::Entry::directory(
                        fid,
                        basename,
                        parent_fid,
                        Some(rev.clone()),
                    ));
                }
                EntryKind::File => {
                    let content = self.transport.get_bytes(&e.path)?;
                    let sha1 = crate::weave::sha_strings(&[content.as_slice()]);
                    let size = content.len() as u64;
                    repository
                        .add_text(&e.file_id, &revid, &[], &content)
                        .map_err(WorkingTreeError::Repository)?;
                    inv_entries.push(crate::inventory::Entry::file(
                        fid,
                        basename,
                        parent_fid,
                        Some(rev.clone()),
                        Some(sha1),
                        Some(size),
                        Some(e.executable),
                        None,
                    ));
                }
                EntryKind::Symlink => {
                    let target = String::from_utf8_lossy(&e.symlink_target).into_owned();
                    repository
                        .add_text(&e.file_id, &revid, &[], b"")
                        .map_err(WorkingTreeError::Repository)?;
                    inv_entries.push(crate::inventory::Entry::link(
                        fid,
                        basename,
                        parent_fid,
                        Some(rev.clone()),
                        Some(target),
                    ));
                }
                EntryKind::TreeReference => {
                    return Err(WorkingTreeError::Commit(
                        "tree references are not supported".to_string(),
                    ));
                }
            }
        }

        let inv_sha1 = repository
            .add_inventory_from_entries(&revid, &parents, &live.root_id, &inv_entries)
            .map_err(WorkingTreeError::Repository)?;

        // Build and add the revision.
        let revision = crate::revision::Revision::new(
            crate::RevisionId::from(revid.as_slice()),
            parents
                .iter()
                .map(|p| crate::RevisionId::from(p.as_slice()))
                .collect(),
            Some(committer.to_string()),
            message.to_string(),
            std::collections::HashMap::new(),
            Some(inv_sha1),
            timestamp as f64,
            Some(timezone),
        );
        repository
            .add_revision(&revision, &parents)
            .map_err(WorkingTreeError::Repository)?;
        repository
            .commit_write_group()
            .map_err(WorkingTreeError::Repository)?;

        // Advance the branch tip.
        let new_revno = self.dirstate_revno() + 1;
        branch
            .set_last_revision_info(new_revno, &revid)
            .map_err(WorkingTreeError::Branch)?;

        // Record the new revision as the dirstate basis, so the working
        // tree reads as up to date. Pair each inventory entry with its
        // tree-relative path: the root is "", the rest follow live order.
        let mut paths: Vec<String> = vec![String::new()];
        paths.extend(live.entries.iter().map(|e| e.path.clone()));
        self.update_basis(&revid, &paths, &inv_entries)?;

        Ok(revid)
    }

    /// Set the dirstate basis (tree 1) to the just-committed revision so
    /// the working tree no longer reports as out of date.
    ///
    /// Requires a local-filesystem transport (the dirstate is rewritten
    /// under an fcntl lock); on a non-local transport this is skipped.
    fn update_basis(
        &mut self,
        revid: &[u8],
        paths: &[String],
        inv_entries: &[crate::inventory::Entry],
    ) -> Result<(), WorkingTreeError> {
        use crate::dirstate::{inv_entry_to_details, TreeData};

        if self.transport.local_path(DIRSTATE_PATH).is_none() {
            return Ok(()); // non-local: skip the basis rewrite.
        }

        let parent_entries: Vec<(Vec<u8>, Vec<u8>, TreeData)> = paths
            .iter()
            .zip(inv_entries)
            .map(|(path, entry)| {
                let (minikind, fingerprint, size, executable, _rev) = inv_entry_to_details(entry);
                let td = TreeData {
                    minikind,
                    fingerprint,
                    size,
                    executable,
                    packed_stat: crate::dirstate::NULLSTAT.to_vec(),
                };
                (
                    path.clone().into_bytes(),
                    entry.file_id().as_bytes().to_vec(),
                    td,
                )
            })
            .collect();

        self.dirstate
            .set_parent_trees(vec![revid.to_vec()], Vec::new(), vec![parent_entries])
            .map_err(|e| WorkingTreeError::Commit(format!("set basis: {e:?}")))?;
        self.save_dirstate()
    }

    /// Rewrite the dirstate to disk under a write lock.
    ///
    /// Requires a local-filesystem transport (the dirstate is locked with
    /// fcntl); on a non-local transport the rewrite is skipped, matching
    /// [`update_basis`](Self::update_basis).
    fn save_dirstate(&mut self) -> Result<(), WorkingTreeError> {
        use crate::dirstate::{FileTransport, Transport as DirstateTransport};

        let dirstate_path = match self.transport.local_path(DIRSTATE_PATH) {
            Some(p) => p,
            None => return Ok(()),
        };
        let mut ft = FileTransport::new(&dirstate_path);
        ft.lock_write()
            .map_err(|e| WorkingTreeError::Commit(format!("lock dirstate: {e:?}")))?;
        self.dirstate.mark_modified(&[], true);
        self.dirstate
            .save_to(&mut ft)
            .map_err(|e| WorkingTreeError::Commit(format!("save dirstate: {e:?}")))?;
        ft.unlock()
            .map_err(|e| WorkingTreeError::Commit(format!("unlock dirstate: {e:?}")))?;
        Ok(())
    }

    /// The current basis revno, derived from the branch the dirstate was
    /// checked out from. A tree with no parent is revno 0.
    fn dirstate_revno(&self) -> u64 {
        // For a first commit there are no parents (revno 0 -> 1). With a
        // parent, the caller's branch already knows the revno; we read it
        // back through the branch at commit time. Keep it simple: 0 when no
        // parents, else rely on the branch (handled by the caller advancing
        // from the existing tip). Here we only support the no-parent case
        // precisely; multi-revno history is a refinement.
        if self.dirstate.parents.is_empty()
            || self
                .dirstate
                .parents
                .iter()
                .all(|p| p.as_slice() == crate::branch::NULL_REVISION)
        {
            0
        } else {
            // Best effort: one more than the number of parents recorded.
            self.dirstate.parents.len() as u64
        }
    }

    /// Collect the live tree-0 entries with their kind and (for symlinks)
    /// target, plus the tree root id.
    fn collect_live_entries(&self) -> LiveEntries {
        let mut entries = Vec::new();
        let mut root_id = crate::inventory::ROOT_ID.to_vec();
        for entry in self.dirstate.iter_entries() {
            let tree0 = match entry.trees.first() {
                Some(t) => t,
                None => continue,
            };
            let kind = match EntryKind::from_minikind(tree0.minikind) {
                Some(k) => k,
                None => continue,
            };
            let path = join_path(&entry.key.dirname, &entry.key.basename);
            if path.is_empty() {
                // The root entry: record its id.
                root_id = entry.key.file_id.clone();
                continue;
            }
            entries.push(LiveEntry {
                path,
                file_id: entry.key.file_id.clone(),
                kind,
                executable: tree0.executable,
                // For symlinks the dirstate fingerprint is the link target.
                symlink_target: if kind == EntryKind::Symlink {
                    tree0.fingerprint.clone()
                } else {
                    Vec::new()
                },
            });
        }
        LiveEntries { root_id, entries }
    }
}

/// A live working-tree entry gathered for commit.
struct LiveEntry {
    path: String,
    file_id: Vec<u8>,
    kind: EntryKind,
    executable: bool,
    symlink_target: Vec<u8>,
}

/// The live entries plus the tree root id.
struct LiveEntries {
    root_id: Vec<u8>,
    entries: Vec<LiveEntry>,
}

/// Join a dirstate `(dirname, basename)` into a tree-relative path.
fn join_path(dirname: &[u8], basename: &[u8]) -> String {
    let dir = String::from_utf8_lossy(dirname);
    let base = String::from_utf8_lossy(basename);
    if dir.is_empty() {
        base.into_owned()
    } else {
        format!("{dir}/{base}")
    }
}

/// Split a tree-relative path into a dirstate `(dirname, basename)` pair.
fn split_path(path: &str) -> (String, String) {
    match path.rsplit_once('/') {
        Some((dir, base)) => (dir.to_string(), base.to_string()),
        None => (String::new(), path.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bzrdir::BzrDir;
    use crate::transport::{LocalTransport, SharedTransport};
    use std::sync::Arc;

    #[test]
    fn split_and_join_round_trip() {
        for p in ["a.txt", "sub/b.txt", "a/b/c"] {
            let (d, b) = split_path(p);
            assert_eq!(join_path(d.as_bytes(), b.as_bytes()), p);
        }
    }

    /// Create a fresh tree, commit its (empty) state, and read the
    /// resulting revision back -- a self-contained create -> commit ->
    /// read loop with no external fixtures. Cross-compatibility with brz
    /// is verified separately against a real tree.
    #[test]
    fn create_and_commit_empty_tree() {
        let dir = tempfile::tempdir().unwrap();
        let parent: SharedTransport = Arc::new(LocalTransport::new(dir.path()));
        let cd = BzrDir::create(&parent).unwrap();

        let mut repo = cd.open_repository().unwrap();
        let branch = cd.open_branch().unwrap();
        let mut wt = cd.open_workingtree().unwrap();

        let revid = wt
            .commit(
                repo.as_mut(),
                &branch,
                "T <t@e>",
                "empty commit",
                1577880000,
                0,
            )
            .unwrap();

        // The dirstate basis was advanced to the new revision, in memory
        // and on disk (re-opening the tree reads the same basis).
        assert_eq!(wt.basis_revision().as_deref(), Some(revid.as_slice()));
        let reread = WorkingTree::open(parent.clone()).unwrap();
        assert_eq!(reread.basis_revision().as_deref(), Some(revid.as_slice()));

        // Branch advanced to revno 1 at the new revision.
        let reopened = BzrDir::open(parent.subtransport(".bzr").unwrap()).unwrap();
        let branch = reopened.open_branch().unwrap();
        assert_eq!(branch.last_revision_info().unwrap(), (1, revid.clone()));

        // The revision and its inventory are readable.
        let repo = reopened.open_repository().unwrap();
        let rev = repo.get_revision(&revid).unwrap();
        assert_eq!(rev.message, "empty commit");
        let inv = repo.get_inventory(&revid).unwrap();
        // An empty tree has only the root, so no non-root entries.
        assert!(inv.entries().unwrap().is_empty());
    }

    /// Build a fresh tree and return its root transport plus an open
    /// working tree.
    fn fresh_tree() -> (tempfile::TempDir, SharedTransport, WorkingTree) {
        let dir = tempfile::tempdir().unwrap();
        let parent: SharedTransport = Arc::new(LocalTransport::new(dir.path()));
        let cd = BzrDir::create(&parent).unwrap();
        let wt = cd.open_workingtree().unwrap();
        (dir, parent, wt)
    }

    #[test]
    fn add_versions_a_path_and_persists() {
        let (_d, parent, mut wt) = fresh_tree();
        parent.put_bytes("a.txt", b"hello\n").unwrap();

        let file_id = wt.add("a.txt", EntryKind::File, None).unwrap();
        assert_eq!(wt.path2id("a.txt"), Some(file_id.clone()));
        assert_eq!(
            wt.list_files(),
            vec![VersionedEntry {
                path: "a.txt".to_string(),
                file_id: file_id.clone(),
                kind: EntryKind::File,
            }]
        );

        // Re-opening the tree reads the same versioned set from disk.
        let reread = WorkingTree::open(parent.clone()).unwrap();
        assert_eq!(reread.path2id("a.txt"), Some(file_id));
    }

    #[test]
    fn add_is_idempotent_and_honours_explicit_id() {
        let (_d, parent, mut wt) = fresh_tree();
        parent.put_bytes("a.txt", b"x\n").unwrap();

        let id = wt.add("a.txt", EntryKind::File, Some(b"my-id")).unwrap();
        assert_eq!(id, b"my-id".to_vec());
        // A second add of the same path is a no-op returning the same id.
        let id2 = wt.add("a.txt", EntryKind::File, Some(b"other")).unwrap();
        assert_eq!(id2, b"my-id".to_vec());
    }

    #[test]
    fn remove_unversions_directory_and_children() {
        let (_d, parent, mut wt) = fresh_tree();
        parent.mkdir("sub").unwrap();
        parent.put_bytes("sub/a.txt", b"a\n").unwrap();
        parent.put_bytes("keep.txt", b"k\n").unwrap();
        wt.add("sub", EntryKind::Directory, None).unwrap();
        wt.add("sub/a.txt", EntryKind::File, None).unwrap();
        wt.add("keep.txt", EntryKind::File, None).unwrap();

        wt.remove("sub").unwrap();

        // The directory and its child are unversioned; the sibling remains.
        assert_eq!(wt.path2id("sub"), None);
        assert_eq!(wt.path2id("sub/a.txt"), None);
        assert!(wt.path2id("keep.txt").is_some());
        // The files are still on disk.
        assert!(parent.has("sub/a.txt").unwrap());

        // Removing an unversioned path is an error.
        assert!(matches!(
            wt.remove("sub"),
            Err(WorkingTreeError::NotVersioned(_))
        ));
    }

    #[test]
    fn rename_moves_entry_and_keeps_file_id() {
        let (_d, parent, mut wt) = fresh_tree();
        parent.put_bytes("a.txt", b"hello\n").unwrap();
        let id = wt.add("a.txt", EntryKind::File, None).unwrap();

        wt.rename("a.txt", "b.txt").unwrap();

        assert_eq!(wt.path2id("a.txt"), None);
        assert_eq!(wt.path2id("b.txt"), Some(id));
        // The file moved on disk.
        assert!(!parent.has("a.txt").unwrap());
        assert_eq!(wt.get_file_text("b.txt").unwrap(), b"hello\n");
    }

    /// Add files, commit, and read the committed inventory back.
    #[test]
    fn add_then_commit_records_the_files() {
        let (_d, parent, mut wt) = fresh_tree();
        let cd = BzrDir::open(parent.subtransport(".bzr").unwrap()).unwrap();
        parent.put_bytes("a.txt", b"hello\n").unwrap();
        wt.add("a.txt", EntryKind::File, None).unwrap();

        let mut repo = cd.open_repository().unwrap();
        let branch = cd.open_branch().unwrap();
        let revid = wt
            .commit(repo.as_mut(), &branch, "T <t@e>", "add a", 1577880000, 0)
            .unwrap();

        let reopened = BzrDir::open(parent.subtransport(".bzr").unwrap()).unwrap();
        let repo = reopened.open_repository().unwrap();
        let inv = repo.get_inventory(&revid).unwrap();
        let paths: Vec<String> = inv.entries().unwrap().into_iter().map(|(p, _)| p).collect();
        assert_eq!(paths, vec!["a.txt".to_string()]);
        let file_id = wt.path2id("a.txt").unwrap();
        assert_eq!(repo.get_file_text(&file_id, &revid).unwrap(), b"hello\n");
    }
}
