//! Reading a dirstate-based working tree (Working Tree Format 6).
//!
//! A working tree is the user's checkout: the files on disk plus
//! `.bzr/checkout/dirstate`, which records the tracked state (tree 0) and
//! the basis it was checked out from (tree 1). This module opens the
//! dirstate through a [`Transport`] rooted at the tree root (the directory
//! that contains `.bzr`) and exposes the live tracked files.
//!
//! Mutation (add/remove/rename) and commit land in a later step; for now
//! this is the read side: list the tracked files, map paths to file ids,
//! and read file contents from disk.

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
    /// An underlying transport error.
    Transport(TransportError),
}

impl std::fmt::Display for WorkingTreeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WorkingTreeError::Dirstate(e) => write!(f, "dirstate: {e}"),
            WorkingTreeError::NotVersioned(p) => write!(f, "path not versioned: {p}"),
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

    #[test]
    fn split_and_join_round_trip() {
        for p in ["a.txt", "sub/b.txt", "a/b/c"] {
            let (d, b) = split_path(p);
            assert_eq!(join_path(d.as_bytes(), b.as_bytes()), p);
        }
    }
}
