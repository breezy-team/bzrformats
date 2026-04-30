//! `IdIndex`: a file-id → (dirname, basename) map that lets
//! `DirState` jump to every row referring to a given file_id without
//! a linear scan.  Mirrors Python's `DirState._id_index`.

use super::{InventoryEntry, Kind};
use crate::FileId;
use std::collections::HashMap;

pub struct IdIndex {
    id_index: HashMap<FileId, Vec<(Vec<u8>, Vec<u8>, FileId)>>,
}

impl Default for IdIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl IdIndex {
    pub fn new() -> Self {
        IdIndex {
            id_index: HashMap::new(),
        }
    }

    /// Add this entry to the _id_index mapping.
    ///
    /// This code used to use a set for every entry in the id_index.
    /// However, it is *rare* to have more than one entry, so a set
    /// is a large overkill.  And even when we do, we won't ever
    /// have more than the number of parent trees, which is still a
    /// small number (rarely >2).  As such, we use a simple vector
    /// and do our own uniqueness checks.  While the `contains`
    /// check is O(N), since N is nicely bounded it shouldn't ever
    /// cause quadratic failure.
    pub fn add(&mut self, entry_key: (&[u8], &[u8], &FileId)) {
        let file_id = entry_key.2;
        let entry_keys = self.id_index.entry(file_id.clone()).or_default();
        entry_keys.push((entry_key.0.to_vec(), entry_key.1.to_vec(), file_id.clone()));
    }

    /// Remove this entry from the _id_index mapping.
    ///
    /// It is a programming error to call this when the entry_key
    /// is not already present.
    pub fn remove(&mut self, entry_key: (&[u8], &[u8], &FileId)) {
        let file_id = entry_key.2;
        let entry_keys = self.id_index.get_mut(file_id).unwrap();
        entry_keys.retain(|key| (key.0.as_slice(), key.1.as_slice(), &key.2) != entry_key);
    }

    pub fn get(&self, file_id: &FileId) -> Vec<(Vec<u8>, Vec<u8>, FileId)> {
        self.id_index
            .get(file_id)
            .map_or_else(Vec::new, |v| v.clone())
    }

    pub fn iter_all(&self) -> impl Iterator<Item = &(Vec<u8>, Vec<u8>, FileId)> {
        self.id_index.values().flatten()
    }

    pub fn file_ids(&self) -> impl Iterator<Item = &FileId> {
        self.id_index.keys()
    }

    pub fn clear(&mut self) {
        self.id_index.clear();
    }
}

/// Convert an inventory entry (from a revision tree) to state details.
///
/// Args:
///   inv_entry: An inventory entry whose sha1 and link targets can be
///     relied upon, and which has a revision set.
/// Returns: A details tuple - the details for a single tree at a path id.
pub fn inv_entry_to_details(e: &InventoryEntry) -> (Kind, Vec<u8>, u64, bool, Vec<u8>) {
    let minikind = Kind::from(e.kind());
    let tree_data = e
        .revision()
        .map_or_else(Vec::new, |r| r.as_bytes().to_vec());
    let (fingerprint, size, executable) = match e {
        InventoryEntry::Directory { .. } | InventoryEntry::Root { .. } => (Vec::new(), 0, false),
        InventoryEntry::File {
            text_sha1,
            text_size,
            executable,
            ..
        } => (
            text_sha1.as_ref().map_or_else(Vec::new, |f| f.to_vec()),
            text_size.unwrap_or(0),
            *executable,
        ),
        InventoryEntry::Link { symlink_target, .. } => (
            symlink_target
                .as_ref()
                .map_or_else(Vec::new, |f| f.as_bytes().to_vec()),
            0,
            false,
        ),
        InventoryEntry::TreeReference {
            reference_revision, ..
        } => (
            reference_revision
                .as_ref()
                .map_or_else(Vec::new, |f| f.as_bytes().to_vec()),
            0,
            false,
        ),
    };

    (minikind, fingerprint, size, executable, tree_data)
}
