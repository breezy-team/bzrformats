//! A read-only view of a tree at a committed revision.
//!
//! A [`RevisionTree`] pairs a revision id with that revision's inventory.
//! It is what [`Repository::revision_tree`](super::Repository::revision_tree)
//! returns and what a commit builds its inventory delta against: the basis
//! tree's inventory supplies each unchanged entry's last-changed revision,
//! path and metadata.

use crate::inventory::{Entry, Inventory};
use crate::{FileId, RevisionId};

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
    revision_id: RevisionId,
    inventory: Box<dyn Inventory>,
}

impl RevisionTree {
    /// The tree of `revision_id` whose inventory is `inventory`.
    pub fn new(revision_id: RevisionId, inventory: Box<dyn Inventory>) -> Self {
        RevisionTree {
            revision_id,
            inventory,
        }
    }

    /// The revision this tree represents.
    pub fn revision_id(&self) -> &RevisionId {
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

    /// Walk the tree in by-directory order, yielding `(path, entry)` pairs.
    ///
    /// The root is yielded first as `("", root)`. When `specific_files` is
    /// given, only those paths (and the directories needed to reach them) are
    /// yielded; a path that is not versioned is silently skipped.
    pub fn iter_entries_by_dir(
        &self,
        specific_files: Option<&[&str]>,
    ) -> Result<Vec<(String, Entry)>, crate::inventory::Error> {
        let specific_file_ids = match specific_files {
            None => None,
            Some(paths) => {
                let mut ids = Vec::with_capacity(paths.len());
                for path in paths {
                    if let Some(id) = self.path2id(path) {
                        ids.push(id);
                    }
                }
                Some(ids)
            }
        };
        self.inventory
            .iter_entries_by_dir(None, specific_file_ids.as_deref())
    }

    /// The direct children of the directory at `path`, sorted by name.
    ///
    /// A `path` that is not versioned is
    /// [`Error::ParentNotVersioned`](crate::inventory::Error::ParentNotVersioned),
    /// and one that is not a directory is
    /// [`Error::ParentNotDirectory`](crate::inventory::Error::ParentNotDirectory).
    pub fn iter_child_entries(&self, path: &str) -> Result<Vec<Entry>, crate::inventory::Error> {
        let file_id = if path.trim_matches('/').is_empty() {
            // The root has no path-keyed entry; look it up via the inventory.
            self.inventory.root_entry()?.map(|e| e.file_id().clone())
        } else {
            self.path2id(path)
        };
        let file_id =
            file_id.ok_or_else(|| crate::inventory::Error::ParentNotVersioned(path.to_string()))?;
        self.inventory.sorted_children(&file_id)
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
        RevisionTree::new(RevisionId::from(b"rev1".to_vec()), Box::new(inv))
    }

    #[test]
    fn can_be_sent_to_another_thread() {
        let tree = sample_tree();
        let found = std::thread::spawn(move || tree.path2id("sub/b"))
            .join()
            .unwrap();
        assert_eq!(found, Some(id(b"b-id")));
    }

    fn paths(entries: &[(String, Entry)]) -> Vec<&str> {
        entries.iter().map(|(path, _)| path.as_str()).collect()
    }

    #[test]
    fn iter_entries_by_dir_yields_the_root_first() {
        let tree = sample_tree();
        let entries = tree.iter_entries_by_dir(None).unwrap();
        assert_eq!(paths(&entries), vec!["", "a", "sub", "sub/b", "sub/c"]);
        assert_eq!(entries[0].1.file_id(), &id(b"TREE_ROOT"));
    }

    #[test]
    fn iter_entries_by_dir_limits_to_specific_files() {
        let tree = sample_tree();
        let entries = tree
            .iter_entries_by_dir(Some(&["sub/c", "a", "not-versioned"]))
            .unwrap();
        assert_eq!(paths(&entries), vec!["a", "sub/c"]);
    }

    #[test]
    fn iter_child_entries_lists_a_directory() {
        let tree = sample_tree();
        let names = |path: &str| -> Vec<String> {
            tree.iter_child_entries(path)
                .unwrap()
                .iter()
                .map(|e| e.name().to_string())
                .collect()
        };
        assert_eq!(names(""), vec!["a", "sub"]);
        assert_eq!(names("sub"), vec!["b", "c"]);
        assert_eq!(names("sub/"), vec!["b", "c"]);
    }

    #[test]
    fn iter_child_entries_needs_a_versioned_directory() {
        use crate::inventory::Error;

        let tree = sample_tree();
        match tree.iter_child_entries("a") {
            Err(Error::ParentNotDirectory(path, file_id)) => {
                assert_eq!(path, "a");
                assert_eq!(file_id, id(b"a-id"));
            }
            other => panic!("expected a non-directory error, got {:?}", other.is_ok()),
        }
        match tree.iter_child_entries("not-versioned") {
            Err(Error::ParentNotVersioned(path)) => assert_eq!(path, "not-versioned"),
            other => panic!("expected an unversioned error, got {:?}", other.is_ok()),
        }
    }
}
