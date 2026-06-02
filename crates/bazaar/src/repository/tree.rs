//! A read-only view of a tree at a committed revision.
//!
//! A [`RevisionTree`] pairs a revision id with that revision's inventory.
//! It is what [`Repository::revision_tree`](super::Repository::revision_tree)
//! returns and what a commit builds its inventory delta against: the basis
//! tree's inventory supplies each unchanged entry's last-changed revision,
//! path and metadata.

use crate::inventory::{Entry, Inventory};
use crate::FileId;

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
    /// tree.
    pub fn id2path(&self, file_id: &FileId) -> Option<String> {
        self.inventory.id2path(file_id).ok()
    }

    /// The inventory entry for `file_id`, or `None` if it is not in this
    /// tree.
    pub fn get_entry(&self, file_id: &FileId) -> Option<Entry> {
        self.inventory.get_entry(file_id).ok().flatten()
    }

    /// The revision in which `file_id` last changed, or `None` if the entry
    /// is absent or carries no recorded revision.
    pub fn get_file_revision(&self, file_id: &FileId) -> Option<Vec<u8>> {
        self.get_entry(file_id)?
            .revision()
            .map(|r| r.as_bytes().to_vec())
    }
}
