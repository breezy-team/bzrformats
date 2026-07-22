//! A read-only view of a tree at a committed revision.
//!
//! A [`RevisionTree`] pairs a revision id with that revision's inventory.
//! It is what [`Repository::revision_tree`](super::Repository::revision_tree)
//! returns and what a commit builds its inventory delta against: the basis
//! tree's inventory supplies each unchanged entry's last-changed revision,
//! path and metadata.

use crate::inventory::{Entry, Inventory};
use crate::FileId;

/// Map an inventory lookup error to the absent/present distinction the tree
/// methods expose: a genuinely missing id becomes `Ok(None)`, while a backend
/// failure (e.g. a CHK inventory failing to read its store) propagates.
fn absent_or_err(e: crate::inventory::Error) -> Result<(), crate::inventory::Error> {
    match e {
        crate::inventory::Error::NoSuchId(_) => Ok(()),
        other => Err(other),
    }
}

/// A tree as it stood at a particular revision, backed by that revision's
/// inventory. The inventory keeps its natural representation (a lazy CHK
/// inventory for 2a, an in-memory one for knit-pack) behind the box.
pub struct RevisionTree {
    revision_id: Vec<u8>,
    inventory: Box<dyn Inventory>,
}

impl RevisionTree {
    pub(super) fn new(revision_id: Vec<u8>, inventory: Box<dyn Inventory>) -> Self {
        RevisionTree {
            revision_id,
            inventory,
        }
    }

    /// The revision this tree represents.
    pub fn revision_id(&self) -> &[u8] {
        &self.revision_id
    }

    /// The tree's inventory.
    pub fn inventory(&self) -> &dyn Inventory {
        self.inventory.as_ref()
    }

    /// The tree-relative path of `file_id`, or `None` if it is not in this
    /// tree. A backend read failure propagates rather than reading as absent.
    pub fn id2path(&self, file_id: &FileId) -> Result<Option<String>, crate::inventory::Error> {
        match self.inventory.id2path(file_id) {
            Ok(p) => Ok(Some(p)),
            Err(e) => absent_or_err(e).map(|()| None),
        }
    }

    /// The file id at tree-relative `path`, or `None` if no entry is at that
    /// path. (The synthetic tree root has no path-keyed entry here.)
    pub fn path2id(&self, path: &str) -> Option<FileId> {
        let path = path.trim_matches('/');
        for (entry_path, entry) in self.inventory.entries().ok()? {
            if entry_path == path {
                return Some(entry.file_id().clone());
            }
        }
        None
    }

    /// The entries in this tree as `(path, entry)` pairs, in path order. The
    /// synthetic root is not included.
    pub fn iter_entries(&self) -> Vec<(String, Entry)> {
        self.inventory.entries().unwrap_or_default()
    }

    /// The inventory entry for `file_id`, or `None` if it is not in this
    /// tree. A backend read failure propagates rather than reading as absent.
    pub fn get_entry(&self, file_id: &FileId) -> Result<Option<Entry>, crate::inventory::Error> {
        self.inventory.get_entry(file_id)
    }

    /// The revision in which `file_id` last changed, or `None` if the entry
    /// is absent or carries no recorded revision. A backend read failure
    /// propagates rather than reading as absent.
    pub fn get_file_revision(
        &self,
        file_id: &FileId,
    ) -> Result<Option<Vec<u8>>, crate::inventory::Error> {
        Ok(self
            .get_entry(file_id)?
            .and_then(|e| e.revision().map(|r| r.as_bytes().to_vec())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inventory::MutableInventory;

    fn id(bytes: &[u8]) -> FileId {
        FileId::from(bytes.to_vec())
    }

    fn file(file_id: &[u8], name: &str, parent_id: &[u8]) -> Entry {
        Entry::file(
            id(file_id),
            name.to_string(),
            id(parent_id),
            None,
            Some(b"sha".to_vec()),
            Some(1),
            Some(false),
            None,
        )
    }

    /// A tree holding `a`, `sub/b` and `sub/c`.
    fn sample_tree() -> RevisionTree {
        let mut inv = MutableInventory::new();
        inv.add(Entry::root(id(b"TREE_ROOT"), None)).unwrap();
        inv.add(Entry::directory(
            id(b"sub-id"),
            "sub".to_string(),
            id(b"TREE_ROOT"),
            None,
        ))
        .unwrap();
        inv.add(file(b"c-id", "c", b"sub-id")).unwrap();
        inv.add(file(b"b-id", "b", b"sub-id")).unwrap();
        inv.add(file(b"a-id", "a", b"TREE_ROOT")).unwrap();
        RevisionTree::new(b"rev1".to_vec(), Box::new(inv))
    }

    #[test]
    fn can_be_sent_to_another_thread() {
        let tree = sample_tree();
        let found = std::thread::spawn(move || tree.path2id("sub/b"))
            .join()
            .unwrap();
        assert_eq!(found, Some(id(b"b-id")));
    }
}
