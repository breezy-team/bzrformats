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
