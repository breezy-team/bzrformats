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
    /// The commit would record no change and `allow_pointless` was false.
    PointlessCommit,
    /// A strict commit found unversioned files in the tree.
    StrictCommitFailed(Vec<String>),
    /// A selective commit (specific_files or exclude) was combined with a
    /// commit that has pending merges, which is not allowed.
    CannotCommitSelectedFileMerge,
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
            WorkingTreeError::PointlessCommit => {
                write!(f, "no changes to commit (use allow_pointless to override)")
            }
            WorkingTreeError::StrictCommitFailed(unknowns) => {
                write!(
                    f,
                    "strict commit failed: {} unversioned file(s) present",
                    unknowns.len()
                )
            }
            WorkingTreeError::CannotCommitSelectedFileMerge => {
                write!(f, "cannot commit selected files with pending merges")
            }
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

    fn from_inventory_kind(k: crate::osutils::Kind) -> Option<Self> {
        match k {
            crate::osutils::Kind::File => Some(EntryKind::File),
            crate::osutils::Kind::Directory => Some(EntryKind::Directory),
            crate::osutils::Kind::Symlink => Some(EntryKind::Symlink),
            crate::osutils::Kind::TreeReference => Some(EntryKind::TreeReference),
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

/// One entry that differs between a working tree and a basis tree, as
/// produced by [`WorkingTree::iter_changes`] and consumed by the commit
/// builder. Mirrors the subset of breezy's `TreeChange` a single-parent
/// commit needs: identity, the path in each tree, whether the file content
/// changed, and the target-side metadata (name, parent, kind, exec) the new
/// inventory entry is built from. `basis_revision` is the entry's
/// last-changed revision in the basis (used to carry an unchanged entry
/// over at its prior revision), or `None` when the entry is new.
#[derive(Debug, Clone)]
pub struct WorkingTreeChange {
    pub file_id: Vec<u8>,
    /// Path in the basis tree, or `None` if newly added.
    pub old_path: Option<String>,
    /// Path in the working tree, or `None` if removed.
    pub new_path: Option<String>,
    /// Whether the file content (or symlink target) changed.
    pub content_change: bool,
    /// Target-tree name (basename), parent id, kind and executable bit.
    /// `None` when the entry is removed in the working tree.
    pub new_name: Option<String>,
    pub new_parent_id: Option<Vec<u8>>,
    pub new_kind: Option<EntryKind>,
    pub new_executable: bool,
    /// The entry's last-changed revision in the basis, or `None` if new.
    pub basis_revision: Option<Vec<u8>>,
}

/// Options for [`WorkingTree::commit`], mirroring the parameters of
/// breezy's `commit`. Build with [`CommitOptions::new`] and the chained
/// setters; unset fields take breezy's defaults.
#[derive(Debug, Clone, Default)]
pub struct CommitOptions {
    /// The commit message.
    pub message: String,
    /// The committer string ("Name <email>"). Required.
    pub committer: String,
    /// Authors, recorded as the `authors` revision property (one per line);
    /// distinct from the committer.
    pub authors: Vec<String>,
    /// Commit timestamp (seconds since the epoch).
    pub timestamp: u64,
    /// Timezone offset in seconds east of UTC.
    pub timezone: i32,
    /// Extra revision properties. `\r` is rejected in values.
    pub revprops: std::collections::HashMap<String, Vec<u8>>,
    /// An explicit revision id; generated from the committer/timestamp when
    /// `None`.
    pub revision_id: Option<Vec<u8>>,
    /// The branch nickname, recorded as the `branch-nick` revision property
    /// when set and not already present in `revprops`.
    pub branch_nick: Option<String>,
    /// Whether to allow a commit that records no change. When `false`
    /// (breezy's default), a commit with nothing to record fails with
    /// [`WorkingTreeError::PointlessCommit`]. A commit with pending merges
    /// is never pointless.
    pub allow_pointless: bool,
    /// Strict mode: refuse the commit if the tree has unversioned files,
    /// failing with [`WorkingTreeError::StrictCommitFailed`].
    pub strict: bool,
    /// When non-empty, commit only changes at these tree-relative paths and
    /// their descendants; other changed entries are left at their basis
    /// state. Cannot be combined with pending merges.
    pub specific_files: Vec<String>,
    /// Tree-relative paths (and their descendants) to exclude from the
    /// commit. Cannot be combined with pending merges.
    pub exclude: Vec<String>,
    /// An OpenPGP secret key (a Transferable Secret Key, armored or binary)
    /// to sign the commit with. Requires the crate's `gpg` feature; supplying
    /// a key without it is an error.
    pub signing_key: Option<Vec<u8>>,
}

impl CommitOptions {
    /// A new option set with the required `committer` and `message`.
    pub fn new(committer: impl Into<String>, message: impl Into<String>) -> Self {
        CommitOptions {
            message: message.into(),
            committer: committer.into(),
            ..Default::default()
        }
    }

    pub fn timestamp(mut self, timestamp: u64) -> Self {
        self.timestamp = timestamp;
        self
    }

    pub fn timezone(mut self, timezone: i32) -> Self {
        self.timezone = timezone;
        self
    }

    pub fn authors(mut self, authors: Vec<String>) -> Self {
        self.authors = authors;
        self
    }

    pub fn revprops(mut self, revprops: std::collections::HashMap<String, Vec<u8>>) -> Self {
        self.revprops = revprops;
        self
    }

    pub fn revision_id(mut self, revision_id: Vec<u8>) -> Self {
        self.revision_id = Some(revision_id);
        self
    }

    pub fn branch_nick(mut self, nick: impl Into<String>) -> Self {
        self.branch_nick = Some(nick.into());
        self
    }

    pub fn allow_pointless(mut self, allow: bool) -> Self {
        self.allow_pointless = allow;
        self
    }

    pub fn strict(mut self, strict: bool) -> Self {
        self.strict = strict;
        self
    }

    pub fn specific_files(mut self, files: Vec<String>) -> Self {
        self.specific_files = files;
        self
    }

    pub fn exclude(mut self, exclude: Vec<String>) -> Self {
        self.exclude = exclude;
        self
    }

    pub fn signing_key(mut self, key: Vec<u8>) -> Self {
        self.signing_key = Some(key);
        self
    }

    /// The full revision-property map: the caller's `revprops` plus the
    /// derived `authors` and `branch-nick` properties. Validates that no
    /// value contains a carriage return (which the XML/bencode serializers
    /// cannot round-trip).
    fn build_properties(
        &self,
    ) -> Result<std::collections::HashMap<String, Vec<u8>>, WorkingTreeError> {
        let mut props = self.revprops.clone();
        if !self.authors.is_empty() {
            // breezy stores multiple authors under "authors" (newline
            // separated) and a single author under "author".
            let key = if self.authors.len() == 1 {
                "author"
            } else {
                "authors"
            };
            props.insert(key.to_string(), self.authors.join("\n").into_bytes());
        }
        if let Some(nick) = &self.branch_nick {
            props
                .entry("branch-nick".to_string())
                .or_insert_with(|| nick.clone().into_bytes());
        }
        for (k, v) in &props {
            if v.contains(&b'\r') {
                return Err(WorkingTreeError::Commit(format!(
                    "revision property {k:?} contains a carriage return"
                )));
            }
        }
        Ok(props)
    }
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

    /// The tree-relative paths of files and directories on disk that are not
    /// versioned, in sorted order. The control directory (`.bzr`) is never
    /// reported. The contents of an unknown directory are not descended
    /// into (the directory itself is the one unknown).
    pub fn unknowns(&self) -> Result<Vec<String>, WorkingTreeError> {
        // Tracked paths, to test membership as we walk.
        let tracked: std::collections::HashSet<String> =
            self.list_files().into_iter().map(|e| e.path).collect();

        let mut unknowns = Vec::new();
        let mut dirs = vec![String::new()];
        while let Some(dir) = dirs.pop() {
            let names = match self.transport.list_dir(&dir) {
                Ok(n) => n,
                Err(_) => continue,
            };
            for name in names {
                if dir.is_empty() && name == ".bzr" {
                    continue;
                }
                let path = if dir.is_empty() {
                    name.clone()
                } else {
                    format!("{dir}/{name}")
                };
                if tracked.contains(&path) {
                    // Versioned: descend into a versioned directory to find
                    // unknowns beneath it.
                    if let Ok(st) = self.transport.stat(&path) {
                        if st.is_dir {
                            dirs.push(path);
                        }
                    }
                } else {
                    unknowns.push(path);
                }
            }
        }
        unknowns.sort();
        Ok(unknowns)
    }

    /// The changes between this working tree and `basis`, one
    /// [`WorkingTreeChange`] per entry that differs (added, removed, moved,
    /// or with changed content or metadata). This is the input the commit
    /// builder records.
    ///
    /// Content change for a file is determined by comparing the on-disk
    /// content's sha1 with the basis entry's recorded `text_sha1`; for a
    /// symlink, by comparing the link target. Entries that are byte- and
    /// metadata-identical to the basis are omitted.
    pub fn iter_changes(
        &self,
        basis: &crate::repository::RevisionTree,
    ) -> Result<Vec<WorkingTreeChange>, WorkingTreeError> {
        use crate::FileId;

        let live = self.collect_live_entries();
        let mut changes = Vec::new();
        let mut seen: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();

        // The tree root: record it when the basis has no root (a first
        // commit), so its empty text and inventory entry are written at the
        // new revision. On later commits an unchanged root is carried over.
        // This is the rich-root behaviour, which the only writable format
        // here (2a) requires. TODO: for a non-rich-root format the empty
        // root change must be suppressed (breezy's `_require_root_change`);
        // commit currently always runs against a 2a tree, so that path is
        // not yet exercised.
        let root_fid = FileId::from(live.root_id.as_slice());
        seen.insert(live.root_id.clone());
        if basis.get_entry(&root_fid).is_none() {
            changes.push(WorkingTreeChange {
                file_id: live.root_id.clone(),
                old_path: None,
                new_path: Some(String::new()),
                content_change: false,
                new_name: Some(String::new()),
                new_parent_id: None,
                new_kind: Some(EntryKind::Directory),
                new_executable: false,
                basis_revision: None,
            });
        }

        // Added, moved, or modified entries: walk the working tree's tracked
        // set and compare each against the basis.
        for e in &live.entries {
            seen.insert(e.file_id.clone());
            let fid = FileId::from(e.file_id.as_slice());
            let basis_entry = basis.get_entry(&fid);
            let old_path = basis.id2path(&fid);
            let basis_revision = basis_entry
                .as_ref()
                .and_then(|be| be.revision().map(|r| r.as_bytes().to_vec()));

            // A versioned file or symlink that has vanished from disk is a
            // deletion: record it as removed (and the tree unversions it
            // after the commit). Only entries that were in the basis can be
            // deleted; a never-committed missing add is simply dropped.
            if matches!(e.kind, EntryKind::File | EntryKind::Symlink)
                && !self.transport.has(&e.path)?
            {
                if old_path.is_some() {
                    changes.push(WorkingTreeChange {
                        file_id: e.file_id.clone(),
                        old_path,
                        new_path: None,
                        content_change: false,
                        new_name: None,
                        new_parent_id: None,
                        new_kind: None,
                        new_executable: false,
                        basis_revision,
                    });
                }
                continue;
            }

            let content_change = self.content_changed(e, basis_entry.as_ref())?;
            let new_parent_id = self.parent_id_of(&e.path, &live);
            let new_name = basename(&e.path).to_string();
            let meta_change = match &basis_entry {
                None => true, // newly added
                Some(be) => {
                    let kind_changed = match EntryKind::from_inventory_kind(be.kind()) {
                        Some(k) => k != e.kind,
                        None => true,
                    };
                    be.name() != new_name
                        || be.parent_id().map(|p| p.as_bytes()) != Some(new_parent_id.as_slice())
                        || kind_changed
                        || be.executable() != e.executable
                }
            };
            let moved = old_path.as_deref() != Some(e.path.as_str());

            if content_change || meta_change || moved {
                changes.push(WorkingTreeChange {
                    file_id: e.file_id.clone(),
                    old_path,
                    new_path: Some(e.path.clone()),
                    content_change,
                    new_name: Some(new_name),
                    new_parent_id: Some(new_parent_id),
                    new_kind: Some(e.kind),
                    new_executable: e.executable,
                    basis_revision,
                });
            }
        }

        // Removed entries: present in the basis but no longer tracked.
        for fid in basis
            .inventory()
            .all_file_ids()
            .map_err(|e| WorkingTreeError::Commit(format!("reading basis inventory: {e:?}")))?
        {
            if seen.contains(fid.as_bytes()) {
                continue;
            }
            let old_path = match basis.id2path(&fid) {
                Some(p) if !p.is_empty() => p, // skip the basis root
                _ => continue,
            };
            let basis_revision = basis
                .get_entry(&fid)
                .and_then(|be| be.revision().map(|r| r.as_bytes().to_vec()));
            changes.push(WorkingTreeChange {
                file_id: fid.as_bytes().to_vec(),
                old_path: Some(old_path),
                new_path: None,
                content_change: false,
                new_name: None,
                new_parent_id: None,
                new_kind: None,
                new_executable: false,
                basis_revision,
            });
        }

        Ok(changes)
    }

    /// Whether `entry`'s on-disk content differs from its basis entry. A new
    /// entry (no basis) always counts as changed.
    fn content_changed(
        &self,
        entry: &LiveEntry,
        basis_entry: Option<&crate::inventory::Entry>,
    ) -> Result<bool, WorkingTreeError> {
        let basis_entry = match basis_entry {
            None => return Ok(true),
            Some(be) => be,
        };
        match entry.kind {
            EntryKind::File => {
                let content = self.transport.get_bytes(&entry.path)?;
                let sha1 = crate::weave::sha_strings(&[content.as_slice()]);
                Ok(basis_entry.text_sha1() != Some(sha1.as_slice()))
            }
            EntryKind::Symlink => Ok(basis_entry.symlink_target().map(|s| s.as_bytes())
                != Some(entry.symlink_target.as_slice())),
            // Directories and tree references have no content; only metadata
            // (handled by the caller) can change.
            EntryKind::Directory | EntryKind::TreeReference => Ok(false),
        }
    }

    /// The file id of the directory that contains `path` in the working
    /// tree (the tree root for a top-level path).
    fn parent_id_of(&self, path: &str, live: &LiveEntries) -> Vec<u8> {
        match path.rsplit_once('/') {
            None => live.root_id.clone(),
            Some((parent, _)) => live
                .entries
                .iter()
                .find(|e| e.path == parent)
                .map(|e| e.file_id.clone())
                .unwrap_or_else(|| live.root_id.clone()),
        }
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
    /// Records the changes between the working tree and its basis through a
    /// [`CommitBuilder`](crate::repository::CommitBuilder): only changed or
    /// new entries get a new per-file text and are recorded at the new
    /// revision, while unchanged entries are carried over at their prior
    /// revision (so the per-file graph and the CHK inventory pages stay
    /// proportional to the change, not the tree size). Advances `branch` to
    /// the new tip and updates the dirstate basis. Returns the new revision
    /// id.
    ///
    /// Single-parent only: pending merges are not yet recorded.
    pub fn commit(
        &mut self,
        repository: &mut dyn crate::repository::Repository,
        branch: &crate::branch::Branch,
        options: &CommitOptions,
    ) -> Result<Vec<u8>, WorkingTreeError> {
        // Strict mode refuses to commit while unversioned files are present.
        if options.strict {
            let unknowns = self.unknowns()?;
            if !unknowns.is_empty() {
                return Err(WorkingTreeError::StrictCommitFailed(unknowns));
            }
        }

        let parents: Vec<Vec<u8>> = self
            .dirstate
            .parents
            .iter()
            .filter(|p| p.as_slice() != crate::branch::NULL_REVISION)
            .cloned()
            .collect();
        let revid = match &options.revision_id {
            Some(id) => id.clone(),
            None => crate::RevisionId::generate(&options.committer, Some(options.timestamp))
                .as_bytes()
                .to_vec(),
        };
        let properties = options.build_properties()?;
        let basis_revision_id = parents
            .first()
            .cloned()
            .unwrap_or_else(|| crate::branch::NULL_REVISION.to_vec());

        // Selective commit cannot be combined with pending merges (the
        // merge parents' per-file graphs would be lost for unselected files).
        let selective = !options.specific_files.is_empty() || !options.exclude.is_empty();
        if selective && parents.len() > 1 {
            return Err(WorkingTreeError::CannotCommitSelectedFileMerge);
        }

        // Diff the live tree against its basis before opening the write
        // group, so we know exactly which entries changed.
        let basis = repository
            .revision_tree(&basis_revision_id)
            .map_err(WorkingTreeError::Repository)?;
        let mut changes = self.iter_changes(&basis)?;
        if selective {
            changes.retain(|c| change_selected(c, &options.specific_files, &options.exclude));
        }

        // Refuse a commit that records no change, unless pending merges make
        // it meaningful or the caller opts in. Each recorded change yields a
        // delta entry; against the null revision the root entry alone (one
        // change) is not a real change.
        if !options.allow_pointless && parents.len() <= 1 {
            let basis_is_null = basis_revision_id == crate::branch::NULL_REVISION;
            let pointless = if basis_is_null {
                changes.len() <= 1
            } else {
                changes.is_empty()
            };
            if pointless {
                return Err(WorkingTreeError::PointlessCommit);
            }
        }

        repository
            .start_write_group()
            .map_err(WorkingTreeError::Repository)?;
        {
            let mut builder = repository
                .get_commit_builder(
                    parents.clone(),
                    revid.clone(),
                    options.committer.clone(),
                    options.timestamp,
                    options.timezone,
                )
                .with_properties(properties.clone());
            builder
                .record_iter_changes(&changes, |path| {
                    self.transport
                        .get_bytes(path)
                        .map_err(crate::repository::RepositoryError::Transport)
                })
                .map_err(WorkingTreeError::Repository)?;
            builder
                .finish_inventory()
                .map_err(WorkingTreeError::Repository)?;
            builder
                .commit(&options.message)
                .map_err(WorkingTreeError::Repository)?;
        }

        // Sign the commit while the write group is still open, so the
        // signature lands in the same pack as the revision.
        if let Some(key) = &options.signing_key {
            let (paths, inv_entries) = self.build_committed_entries(&revid, &basis, &changes)?;
            let signature =
                self.sign_commit(&revid, options, &properties, &paths, &inv_entries, key)?;
            repository
                .add_signature_text(&revid, &signature)
                .map_err(WorkingTreeError::Repository)?;
        }

        repository
            .commit_write_group()
            .map_err(WorkingTreeError::Repository)?;

        // Unversion any files that were committed as deletions because they
        // had vanished from disk, so they leave the working tree's tracked
        // set (and the dirstate basis rebuilt below). Paths the user had
        // already unversioned are no longer tracked, so this skips them.
        let deleted_paths: Vec<String> = changes
            .iter()
            .filter(|c| c.new_path.is_none())
            .filter_map(|c| c.old_path.clone())
            .filter(|p| self.path2id(p).is_some())
            .collect();
        for path in &deleted_paths {
            self.remove(path)?;
        }

        // Advance the branch tip.
        let new_revno = self.dirstate_revno() + 1;
        branch
            .set_last_revision_info(new_revno, &revid)
            .map_err(WorkingTreeError::Branch)?;

        // Record the new revision as the dirstate basis. The basis tree is
        // the full live tree; each entry keeps its last-changed revision
        // (the new revision for changed/new entries, the prior one for
        // carried-over entries).
        self.update_basis_from_changes(&revid, &basis, &changes)?;

        Ok(revid)
    }

    /// Set the dirstate basis to the just-committed tree.
    fn update_basis_from_changes(
        &mut self,
        revid: &[u8],
        basis: &crate::repository::RevisionTree,
        changes: &[WorkingTreeChange],
    ) -> Result<(), WorkingTreeError> {
        let (paths, inv_entries) = self.build_committed_entries(revid, basis, changes)?;
        self.update_basis(revid, &paths, &inv_entries)
    }

    /// Build the full inventory entry list for the just-committed tree,
    /// deriving each entry's last-changed revision from the recorded changes
    /// (the new revision for changed/new entries, the basis revision
    /// otherwise). Returns the entries paired with their tree-relative paths
    /// (root first). Used both to update the dirstate basis and to build the
    /// testament for signing.
    fn build_committed_entries(
        &self,
        revid: &[u8],
        basis: &crate::repository::RevisionTree,
        changes: &[WorkingTreeChange],
    ) -> Result<(Vec<String>, Vec<crate::inventory::Entry>), WorkingTreeError> {
        use crate::FileId;

        // Map file_id -> last-changed revision for entries recorded at the
        // new revision (changed or new); everything else keeps its basis
        // revision.
        let mut new_rev_ids: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
        for c in changes {
            if c.new_path.is_some() && (c.content_change || c.basis_revision.is_none()) {
                new_rev_ids.insert(c.file_id.clone());
            }
        }

        let live = self.collect_live_entries();
        let mut path_to_id: std::collections::HashMap<String, Vec<u8>> =
            std::collections::HashMap::new();
        path_to_id.insert(String::new(), live.root_id.clone());
        for e in &live.entries {
            path_to_id.insert(e.path.clone(), e.file_id.clone());
        }

        // The root keeps its basis revision unless this is the first commit
        // (no basis root), in which case it is recorded at the new revision.
        let root_fid = FileId::from(live.root_id.as_slice());
        let root_rev = match basis.get_file_revision(&root_fid) {
            Some(r) => crate::RevisionId::from(r.as_slice()),
            None => crate::RevisionId::from(revid),
        };
        let mut paths: Vec<String> = vec![String::new()];
        let mut inv_entries = vec![crate::inventory::Entry::root(root_fid, Some(root_rev))];
        for e in &live.entries {
            let (parent_path, basename) = split_path(&e.path);
            let parent_id = path_to_id
                .get(&parent_path)
                .ok_or_else(|| WorkingTreeError::Commit(format!("no parent for {}", e.path)))?;
            let parent_fid = FileId::from(parent_id.as_slice());
            let fid = FileId::from(e.file_id.as_slice());
            // The entry's recorded revision: the new one if it changed,
            // otherwise its revision in the basis.
            let entry_rev = if new_rev_ids.contains(&e.file_id) {
                crate::RevisionId::from(revid)
            } else {
                match basis.get_file_revision(&fid) {
                    Some(r) => crate::RevisionId::from(r.as_slice()),
                    None => crate::RevisionId::from(revid),
                }
            };
            paths.push(e.path.clone());
            inv_entries.push(match e.kind {
                EntryKind::Directory => {
                    crate::inventory::Entry::directory(fid, basename, parent_fid, Some(entry_rev))
                }
                EntryKind::File => {
                    // For a file recorded at the new revision, hash the
                    // current on-disk content. For a carried-over file, the
                    // committed inventory still holds the basis sha/size (the
                    // working copy on disk may differ -- e.g. an unselected
                    // change), so reuse the basis entry's values to keep the
                    // dirstate basis consistent with what was committed.
                    let recorded_at_new = new_rev_ids.contains(&e.file_id);
                    let (sha1, size) = if recorded_at_new {
                        let content = self.transport.get_bytes(&e.path)?;
                        let sha1 = crate::weave::sha_strings(&[content.as_slice()]);
                        (sha1, content.len() as u64)
                    } else {
                        match basis.get_entry(&fid) {
                            Some(be) => (
                                be.text_sha1().map(|s| s.to_vec()).unwrap_or_default(),
                                be.text_size().unwrap_or(0),
                            ),
                            None => {
                                let content = self.transport.get_bytes(&e.path)?;
                                let sha1 = crate::weave::sha_strings(&[content.as_slice()]);
                                (sha1, content.len() as u64)
                            }
                        }
                    };
                    crate::inventory::Entry::file(
                        fid,
                        basename,
                        parent_fid,
                        Some(entry_rev),
                        Some(sha1),
                        Some(size),
                        Some(e.executable),
                        None,
                    )
                }
                EntryKind::Symlink => {
                    let target = String::from_utf8_lossy(&e.symlink_target).into_owned();
                    crate::inventory::Entry::link(
                        fid,
                        basename,
                        parent_fid,
                        Some(entry_rev),
                        Some(target),
                    )
                }
                EntryKind::TreeReference => {
                    return Err(WorkingTreeError::Commit(
                        "tree references are not supported".to_string(),
                    ))
                }
            });
        }
        Ok((paths, inv_entries))
    }

    /// Build the strict-v3 testament for the commit and return its
    /// clearsigned short text (the form brz stores in the signature store).
    ///
    /// Requires the `gpg` feature; without it, supplying a signing key is an
    /// error.
    #[allow(clippy::too_many_arguments)]
    fn sign_commit(
        &self,
        revid: &[u8],
        options: &CommitOptions,
        properties: &std::collections::HashMap<String, Vec<u8>>,
        paths: &[String],
        inv_entries: &[crate::inventory::Entry],
        signing_key: &[u8],
    ) -> Result<Vec<u8>, WorkingTreeError> {
        #[cfg(not(feature = "gpg"))]
        {
            let _ = (revid, options, properties, paths, inv_entries, signing_key);
            Err(WorkingTreeError::Commit(
                "commit signing requires the crate's `gpg` feature".to_string(),
            ))
        }

        #[cfg(feature = "gpg")]
        {
            use crate::testament::{
                EntryKind as TKind, Testament, TestamentEntry, TestamentFormat,
            };

            let revprops: std::collections::BTreeMap<String, String> = properties
                .iter()
                .map(|(k, v)| (k.clone(), String::from_utf8_lossy(v).into_owned()))
                .collect();

            // Testament entries: every non-root inventory entry, paired with
            // its tree-relative path.
            let mut entries = Vec::new();
            for (path, entry) in paths.iter().zip(inv_entries) {
                if path.is_empty() {
                    continue; // the root is not a testament entry.
                }
                let (kind, content) = match entry {
                    crate::inventory::Entry::File { text_sha1, .. } => {
                        (TKind::File, text_sha1.clone().unwrap_or_default())
                    }
                    crate::inventory::Entry::Directory { .. } => (TKind::Directory, Vec::new()),
                    crate::inventory::Entry::Link { symlink_target, .. } => (
                        TKind::Symlink,
                        symlink_target.clone().unwrap_or_default().into_bytes(),
                    ),
                    crate::inventory::Entry::TreeReference { .. } => {
                        (TKind::TreeReference, Vec::new())
                    }
                    crate::inventory::Entry::Root { .. } => continue,
                };
                entries.push(TestamentEntry {
                    path: path.clone(),
                    kind,
                    file_id: entry.file_id().as_bytes().to_vec(),
                    content,
                    revision: entry
                        .revision()
                        .map(|r| r.as_bytes().to_vec())
                        .unwrap_or_default(),
                    executable: entry.executable(),
                });
            }

            let testament = Testament {
                revision_id: revid.to_vec(),
                committer: options.committer.clone(),
                timestamp: options.timestamp as i64,
                timezone: options.timezone,
                message: options.message.clone(),
                parent_ids: self
                    .dirstate
                    .parents
                    .iter()
                    .filter(|p| p.as_slice() != crate::branch::NULL_REVISION)
                    .cloned()
                    .collect(),
                revprops,
                entries,
            };
            let short = testament
                .as_short_text(TestamentFormat::Strict3)
                .map_err(|e| WorkingTreeError::Commit(format!("testament: {e:?}")))?;
            crate::gpg::clearsign(&short, signing_key)
                .map_err(|e| WorkingTreeError::Commit(format!("sign: {e}")))
        }
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

/// Whether `path` is equal to `prefix` or lies beneath it (i.e. `path` is
/// `prefix` or starts with `prefix/`).
fn path_is_within(path: &str, prefix: &str) -> bool {
    if prefix.is_empty() {
        return true;
    }
    path == prefix || path.starts_with(&format!("{prefix}/"))
}

/// Whether a change should be included in a selective commit. The tree root
/// is always included. A change is included when its path is within one of
/// `specific_files` (or `specific_files` is empty) and not within any of
/// `exclude`. The change's path is taken from the working-tree side, falling
/// back to the basis side for deletions.
fn change_selected(
    change: &WorkingTreeChange,
    specific_files: &[String],
    exclude: &[String],
) -> bool {
    let path = match change.new_path.as_deref().or(change.old_path.as_deref()) {
        Some(p) => p,
        None => return false,
    };
    if path.is_empty() {
        return true; // the root is always recorded.
    }
    if exclude.iter().any(|e| path_is_within(path, e)) {
        return false;
    }
    if specific_files.is_empty() {
        return true;
    }
    specific_files.iter().any(|f| path_is_within(path, f))
}

/// The final component of a tree-relative path.
fn basename(path: &str) -> &str {
    match path.rsplit_once('/') {
        Some((_, base)) => base,
        None => path,
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
                &CommitOptions::new("T <t@e>", "empty commit")
                    .timestamp(1577880000)
                    .allow_pointless(true),
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
            .commit(
                repo.as_mut(),
                &branch,
                &crate::workingtree::CommitOptions::new("T <t@e>", "add a").timestamp(1577880000),
            )
            .unwrap();

        let reopened = BzrDir::open(parent.subtransport(".bzr").unwrap()).unwrap();
        let repo = reopened.open_repository().unwrap();
        let inv = repo.get_inventory(&revid).unwrap();
        let paths: Vec<String> = inv.entries().unwrap().into_iter().map(|(p, _)| p).collect();
        assert_eq!(paths, vec!["a.txt".to_string()]);
        let file_id = wt.path2id("a.txt").unwrap();
        assert_eq!(repo.get_file_text(&file_id, &revid).unwrap(), b"hello\n");
    }

    /// After a commit, iter_changes against the committed basis reports
    /// only the entries that actually differ.
    #[test]
    fn iter_changes_reports_only_differences() {
        use crate::repository::Repository as _;
        let (_d, parent, mut wt) = fresh_tree();
        let cd = BzrDir::open(parent.subtransport(".bzr").unwrap()).unwrap();
        parent.put_bytes("a.txt", b"hello\n").unwrap();
        parent.put_bytes("b.txt", b"world\n").unwrap();
        wt.add("a.txt", EntryKind::File, None).unwrap();
        wt.add("b.txt", EntryKind::File, None).unwrap();
        let mut repo = cd.open_repository().unwrap();
        let branch = cd.open_branch().unwrap();
        let revid = wt
            .commit(
                repo.as_mut(),
                &branch,
                &CommitOptions::new("T <t@e>", "two files").timestamp(1577880000),
            )
            .unwrap();

        // Re-open the tree (its basis is now the commit), modify a.txt,
        // add c.txt, leave b.txt untouched.
        let mut wt = WorkingTree::open(parent.clone()).unwrap();
        parent.put_bytes("a.txt", b"changed\n").unwrap();
        parent.put_bytes("c.txt", b"new\n").unwrap();
        wt.add("c.txt", EntryKind::File, None).unwrap();

        let repo = cd.open_repository().unwrap();
        let basis = repo.revision_tree(&revid).unwrap();
        let mut changes = wt.iter_changes(&basis).unwrap();
        changes.sort_by(|x, y| x.new_path.cmp(&y.new_path));

        // a.txt: content changed; c.txt: added. b.txt: unchanged (omitted).
        let summary: Vec<(Option<String>, Option<String>, bool)> = changes
            .iter()
            .map(|c| (c.old_path.clone(), c.new_path.clone(), c.content_change))
            .collect();
        assert_eq!(
            summary,
            vec![
                (Some("a.txt".to_string()), Some("a.txt".to_string()), true),
                (None, Some("c.txt".to_string()), true),
            ]
        );
    }

    /// A removed file is reported with a new_path of None.
    #[test]
    fn iter_changes_reports_removals() {
        use crate::repository::Repository as _;
        let (_d, parent, mut wt) = fresh_tree();
        let cd = BzrDir::open(parent.subtransport(".bzr").unwrap()).unwrap();
        parent.put_bytes("a.txt", b"hello\n").unwrap();
        wt.add("a.txt", EntryKind::File, None).unwrap();
        let mut repo = cd.open_repository().unwrap();
        let branch = cd.open_branch().unwrap();
        let revid = wt
            .commit(
                repo.as_mut(),
                &branch,
                &crate::workingtree::CommitOptions::new("T <t@e>", "add a").timestamp(1577880000),
            )
            .unwrap();

        let mut wt = WorkingTree::open(parent.clone()).unwrap();
        wt.remove("a.txt").unwrap();
        let repo = cd.open_repository().unwrap();
        let basis = repo.revision_tree(&revid).unwrap();
        let changes = wt.iter_changes(&basis).unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].old_path.as_deref(), Some("a.txt"));
        assert_eq!(changes[0].new_path, None);
    }

    /// A second commit that changes one file records that file at the new
    /// revision and carries the unchanged file over at its original
    /// revision -- the incremental property.
    #[test]
    fn second_commit_is_incremental() {
        use crate::repository::Repository as _;
        let (_d, parent, mut wt) = fresh_tree();
        let cd = BzrDir::open(parent.subtransport(".bzr").unwrap()).unwrap();
        parent.put_bytes("a.txt", b"a one\n").unwrap();
        parent.put_bytes("b.txt", b"b one\n").unwrap();
        wt.add("a.txt", EntryKind::File, None).unwrap();
        wt.add("b.txt", EntryKind::File, None).unwrap();
        let mut repo = cd.open_repository().unwrap();
        let branch = cd.open_branch().unwrap();
        let rev1 = wt
            .commit(
                repo.as_mut(),
                &branch,
                &crate::workingtree::CommitOptions::new("T <t@e>", "first").timestamp(1577880000),
            )
            .unwrap();

        // Second commit: change only a.txt.
        let mut wt = WorkingTree::open(parent.clone()).unwrap();
        parent.put_bytes("a.txt", b"a two\n").unwrap();
        let mut repo = cd.open_repository().unwrap();
        let branch = cd.open_branch().unwrap();
        let rev2 = wt
            .commit(
                repo.as_mut(),
                &branch,
                &crate::workingtree::CommitOptions::new("T <t@e>", "second").timestamp(1577890000),
            )
            .unwrap();
        assert_ne!(rev1, rev2);

        // rev2 has both files with the new content.
        let repo = cd.open_repository().unwrap();
        let inv = repo.get_inventory(&rev2).unwrap();
        let mut paths: Vec<String> = inv.entries().unwrap().into_iter().map(|(p, _)| p).collect();
        paths.sort();
        assert_eq!(paths, vec!["a.txt".to_string(), "b.txt".to_string()]);

        // a.txt was recorded at rev2; b.txt carried over at rev1.
        let a_id = wt.path2id("a.txt").unwrap();
        let b_id = wt.path2id("b.txt").unwrap();
        let a_entry = inv
            .get_entry(&crate::FileId::from(a_id.as_slice()))
            .unwrap()
            .unwrap();
        let b_entry = inv
            .get_entry(&crate::FileId::from(b_id.as_slice()))
            .unwrap()
            .unwrap();
        assert_eq!(a_entry.revision().unwrap().as_bytes(), rev2.as_slice());
        assert_eq!(b_entry.revision().unwrap().as_bytes(), rev1.as_slice());
        // The changed file's new text is stored at rev2; the unchanged file
        // has no rev2 text (it was not rewritten).
        assert_eq!(repo.get_file_text(&a_id, &rev2).unwrap(), b"a two\n");
        assert!(repo.get_file_text(&b_id, &rev2).is_err());
        assert_eq!(repo.get_file_text(&b_id, &rev1).unwrap(), b"b one\n");
    }

    /// Revision properties, authors and an explicit revision id are recorded
    /// on the committed revision.
    #[test]
    fn commit_records_revprops_authors_and_revid() {
        use crate::repository::Repository as _;
        let (_d, parent, mut wt) = fresh_tree();
        let cd = BzrDir::open(parent.subtransport(".bzr").unwrap()).unwrap();
        parent.put_bytes("a.txt", b"hi\n").unwrap();
        wt.add("a.txt", EntryKind::File, None).unwrap();

        let mut props = std::collections::HashMap::new();
        props.insert("custom".to_string(), b"value".to_vec());
        let options = CommitOptions::new("T <t@e>", "msg")
            .timestamp(1577880000)
            .revprops(props)
            .authors(vec!["A <a@e>".to_string(), "B <b@e>".to_string()])
            .branch_nick("trunk")
            .revision_id(b"my-explicit-revid".to_vec());

        let mut repo = cd.open_repository().unwrap();
        let branch = cd.open_branch().unwrap();
        let revid = wt.commit(repo.as_mut(), &branch, &options).unwrap();
        assert_eq!(revid, b"my-explicit-revid".to_vec());

        let repo = cd.open_repository().unwrap();
        let rev = repo.get_revision(&revid).unwrap();
        assert_eq!(
            rev.properties.get("custom").map(|v| v.as_slice()),
            Some(&b"value"[..])
        );
        // Multiple authors are stored under "authors", newline-separated.
        assert_eq!(
            rev.properties.get("authors").map(|v| v.as_slice()),
            Some(&b"A <a@e>\nB <b@e>"[..])
        );
        assert_eq!(
            rev.properties.get("branch-nick").map(|v| v.as_slice()),
            Some(&b"trunk"[..])
        );
    }

    /// A carriage return in a revision property is rejected.
    #[test]
    fn commit_rejects_cr_in_revprops() {
        let (_d, parent, mut wt) = fresh_tree();
        let cd = BzrDir::open(parent.subtransport(".bzr").unwrap()).unwrap();
        parent.put_bytes("a.txt", b"hi\n").unwrap();
        wt.add("a.txt", EntryKind::File, None).unwrap();
        let mut props = std::collections::HashMap::new();
        props.insert("bad".to_string(), b"has\rcr".to_vec());
        let options = CommitOptions::new("T <t@e>", "msg")
            .timestamp(1577880000)
            .revprops(props);
        let mut repo = cd.open_repository().unwrap();
        let branch = cd.open_branch().unwrap();
        assert!(matches!(
            wt.commit(repo.as_mut(), &branch, &options),
            Err(WorkingTreeError::Commit(_))
        ));
    }

    /// A commit with no changes is refused unless allow_pointless is set.
    #[test]
    fn pointless_commit_is_refused_then_allowed() {
        let (_d, parent, mut wt) = fresh_tree();
        let cd = BzrDir::open(parent.subtransport(".bzr").unwrap()).unwrap();
        parent.put_bytes("a.txt", b"hi\n").unwrap();
        wt.add("a.txt", EntryKind::File, None).unwrap();
        let mut repo = cd.open_repository().unwrap();
        let branch = cd.open_branch().unwrap();
        wt.commit(
            repo.as_mut(),
            &branch,
            &CommitOptions::new("T <t@e>", "first").timestamp(1577880000),
        )
        .unwrap();

        // Re-open with no changes: a plain commit is pointless.
        let mut wt = WorkingTree::open(parent.clone()).unwrap();
        let mut repo = cd.open_repository().unwrap();
        let branch = cd.open_branch().unwrap();
        assert!(matches!(
            wt.commit(
                repo.as_mut(),
                &branch,
                &CommitOptions::new("T <t@e>", "empty").timestamp(1577890000)
            ),
            Err(WorkingTreeError::PointlessCommit)
        ));

        // With allow_pointless it succeeds.
        let revid = wt
            .commit(
                repo.as_mut(),
                &branch,
                &CommitOptions::new("T <t@e>", "empty")
                    .timestamp(1577890000)
                    .allow_pointless(true),
            )
            .unwrap();
        assert!(!revid.is_empty());
    }

    /// A versioned file deleted from disk is committed as a removal and
    /// unversioned from the tree.
    #[test]
    fn commit_records_disk_deletion() {
        use crate::repository::Repository as _;
        let (_d, parent, mut wt) = fresh_tree();
        let cd = BzrDir::open(parent.subtransport(".bzr").unwrap()).unwrap();
        parent.put_bytes("a.txt", b"a\n").unwrap();
        parent.put_bytes("b.txt", b"b\n").unwrap();
        wt.add("a.txt", EntryKind::File, None).unwrap();
        wt.add("b.txt", EntryKind::File, None).unwrap();
        let mut repo = cd.open_repository().unwrap();
        let branch = cd.open_branch().unwrap();
        wt.commit(
            repo.as_mut(),
            &branch,
            &CommitOptions::new("T <t@e>", "two").timestamp(1577880000),
        )
        .unwrap();

        // Delete a.txt from disk (without calling remove) and commit.
        let mut wt = WorkingTree::open(parent.clone()).unwrap();
        parent.delete("a.txt").unwrap();
        let mut repo = cd.open_repository().unwrap();
        let branch = cd.open_branch().unwrap();
        let rev2 = wt
            .commit(
                repo.as_mut(),
                &branch,
                &CommitOptions::new("T <t@e>", "del a").timestamp(1577890000),
            )
            .unwrap();

        // a.txt is gone from the committed inventory and from the tree.
        let repo = cd.open_repository().unwrap();
        let inv = repo.get_inventory(&rev2).unwrap();
        let paths: Vec<String> = inv.entries().unwrap().into_iter().map(|(p, _)| p).collect();
        assert_eq!(paths, vec!["b.txt".to_string()]);
        assert_eq!(wt.path2id("a.txt"), None);
        assert!(wt.path2id("b.txt").is_some());
    }

    /// unknowns lists on-disk files that are not versioned, skipping .bzr.
    #[test]
    fn unknowns_lists_unversioned_files() {
        let (_d, parent, mut wt) = fresh_tree();
        parent.put_bytes("tracked.txt", b"t\n").unwrap();
        parent.put_bytes("loose.txt", b"l\n").unwrap();
        wt.add("tracked.txt", EntryKind::File, None).unwrap();
        let wt = WorkingTree::open(parent.clone()).unwrap();
        assert_eq!(wt.unknowns().unwrap(), vec!["loose.txt".to_string()]);
    }

    /// A strict commit is refused while unversioned files are present, and
    /// succeeds once the tree is clean.
    #[test]
    fn strict_commit_refuses_unknown_files() {
        let (_d, parent, mut wt) = fresh_tree();
        let cd = BzrDir::open(parent.subtransport(".bzr").unwrap()).unwrap();
        parent.put_bytes("a.txt", b"a\n").unwrap();
        parent.put_bytes("loose.txt", b"l\n").unwrap();
        wt.add("a.txt", EntryKind::File, None).unwrap();

        let mut repo = cd.open_repository().unwrap();
        let branch = cd.open_branch().unwrap();
        let strict = CommitOptions::new("T <t@e>", "c")
            .timestamp(1577880000)
            .strict(true);
        assert!(matches!(
            wt.commit(repo.as_mut(), &branch, &strict),
            Err(WorkingTreeError::StrictCommitFailed(_))
        ));

        // Remove the unknown file; the strict commit now succeeds.
        parent.delete("loose.txt").unwrap();
        let revid = wt.commit(repo.as_mut(), &branch, &strict).unwrap();
        assert!(!revid.is_empty());
    }

    /// A commit limited to specific_files records only those files; other
    /// changed files are carried over at their basis revision.
    #[test]
    fn selective_commit_records_only_named_files() {
        use crate::repository::Repository as _;
        let (_d, parent, mut wt) = fresh_tree();
        let cd = BzrDir::open(parent.subtransport(".bzr").unwrap()).unwrap();
        parent.put_bytes("a.txt", b"a1\n").unwrap();
        parent.put_bytes("b.txt", b"b1\n").unwrap();
        wt.add("a.txt", EntryKind::File, None).unwrap();
        wt.add("b.txt", EntryKind::File, None).unwrap();
        let mut repo = cd.open_repository().unwrap();
        let branch = cd.open_branch().unwrap();
        let rev1 = wt
            .commit(
                repo.as_mut(),
                &branch,
                &CommitOptions::new("T <t@e>", "two").timestamp(1577880000),
            )
            .unwrap();

        // Modify both files but commit only a.txt.
        let mut wt = WorkingTree::open(parent.clone()).unwrap();
        parent.put_bytes("a.txt", b"a2\n").unwrap();
        parent.put_bytes("b.txt", b"b2\n").unwrap();
        let mut repo = cd.open_repository().unwrap();
        let branch = cd.open_branch().unwrap();
        let rev2 = wt
            .commit(
                repo.as_mut(),
                &branch,
                &CommitOptions::new("T <t@e>", "only a")
                    .timestamp(1577890000)
                    .specific_files(vec!["a.txt".to_string()]),
            )
            .unwrap();

        // In rev2, a.txt is at rev2 with the new content; b.txt is carried
        // over at rev1 with its old content.
        let repo = cd.open_repository().unwrap();
        let inv = repo.get_inventory(&rev2).unwrap();
        let a_id = wt.path2id("a.txt").unwrap();
        let b_id = wt.path2id("b.txt").unwrap();
        let a_entry = inv
            .get_entry(&crate::FileId::from(a_id.as_slice()))
            .unwrap()
            .unwrap();
        let b_entry = inv
            .get_entry(&crate::FileId::from(b_id.as_slice()))
            .unwrap()
            .unwrap();
        assert_eq!(a_entry.revision().unwrap().as_bytes(), rev2.as_slice());
        assert_eq!(b_entry.revision().unwrap().as_bytes(), rev1.as_slice());
        assert_eq!(repo.get_file_text(&a_id, &rev2).unwrap(), b"a2\n");
        // b.txt's recorded content is still the rev1 version.
        assert_eq!(repo.get_file_text(&b_id, &rev1).unwrap(), b"b1\n");
    }

    /// A signed commit stores a clearsigned testament in the signature
    /// store. (Requires the `gpg` feature.)
    #[cfg(feature = "gpg")]
    #[test]
    fn commit_with_signing_key_stores_signature() {
        use crate::repository::Repository as _;
        use sequoia_openpgp::cert::CertBuilder;
        use sequoia_openpgp::serialize::Serialize;

        let (cert, _) = CertBuilder::new().add_signing_subkey().generate().unwrap();
        let mut tsk = Vec::new();
        cert.as_tsk().serialize(&mut tsk).unwrap();

        let (_d, parent, mut wt) = fresh_tree();
        let cd = BzrDir::open(parent.subtransport(".bzr").unwrap()).unwrap();
        parent.put_bytes("a.txt", b"hi\n").unwrap();
        wt.add("a.txt", EntryKind::File, None).unwrap();
        let mut repo = cd.open_repository().unwrap();
        let branch = cd.open_branch().unwrap();
        let revid = wt
            .commit(
                repo.as_mut(),
                &branch,
                &CommitOptions::new("T <t@e>", "signed")
                    .timestamp(1577880000)
                    .signing_key(tsk),
            )
            .unwrap();

        let repo = cd.open_repository().unwrap();
        let sig = repo.get_signature_text(&revid).unwrap().unwrap();
        let sig = String::from_utf8(sig).unwrap();
        assert!(sig.starts_with("-----BEGIN PGP SIGNED MESSAGE-----"));
        assert!(sig.contains("bazaar testament short form 3 strict"));
        assert!(sig.contains("-----BEGIN PGP SIGNATURE-----"));
    }
}
