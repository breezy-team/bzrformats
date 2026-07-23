//! Tree transforms: the staged, applied-atomically model of tree changes.
//!
//! A [`TreeTransform`] records a set of changes to a tree (new files, deletions,
//! renames, versioning, executability) as pending operations keyed by
//! *transform ids* (`trans_id`), then applies them all at once. This mirrors
//! breezy's `breezy.transform` / `breezy.bzr.transform`.
//!
//! The engine is layered: this module holds the base bookkeeping (the pending
//! operation maps and the pure "what will the final tree look like" queries);
//! [`disk`] stages new file contents on disk (limbo); [`apply`] turns the
//! staged transform into an inventory delta and applies it to the working tree.
//!
//! The transform queries the tree it operates on through [`TransformTree`], so
//! the bookkeeping is independent of any particular tree implementation.
//!
//! This module is built up in layers; the crate-internal accessors below are
//! consumed by the disk and apply layers, which are added incrementally.
#![allow(dead_code)]

use crate::osutils::Kind;
use crate::FileId;
use std::collections::{HashMap, HashSet};

/// The parent trans-id sentinel for the tree root (breezy's `ROOT_PARENT`).
pub const ROOT_PARENT: &str = "root-parent";

/// Errors from building or applying a tree transform.
#[derive(Debug)]
pub enum Error {
    /// A key was added twice where uniqueness is required.
    DuplicateKey(String),
    /// A trans-id has no derivable final path.
    NoFinalPath(String),
    /// An attempt was made to move the tree root.
    CantMoveRoot,
    /// The transform is internally inconsistent.
    Malformed(String),
    /// A tree query failed.
    Tree(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::DuplicateKey(k) => write!(f, "duplicate key: {k}"),
            Error::NoFinalPath(t) => write!(f, "no final path for {t}"),
            Error::CantMoveRoot => write!(f, "cannot move the tree root"),
            Error::Malformed(m) => write!(f, "malformed transform: {m}"),
            Error::Tree(m) => write!(f, "tree error: {m}"),
        }
    }
}

impl std::error::Error for Error {}

/// The read queries a [`TreeTransform`] makes against the tree it transforms.
///
/// Implemented by the working tree (and, for previews, by a revision tree).
/// The transform uses these to compute final paths, kinds and versioning
/// relative to the tree's current state.
pub trait TransformTree {
    /// Whether `path` is versioned in the tree.
    fn is_versioned(&self, path: &str) -> bool;

    /// The kind of the file at tree-relative `path`, or `None` if absent.
    fn tree_kind(&self, path: &str) -> Option<Kind>;

    /// The tree-relative path of `file_id`, or `None` if not versioned.
    fn id2path(&self, file_id: &FileId) -> Option<String>;

    /// The file id at tree-relative `path`, or `None` if not versioned.
    fn path2id(&self, path: &str) -> Option<FileId>;

    /// The basenames of the direct children of the directory at `path`.
    fn tree_children(&self, path: &str) -> Result<Vec<String>, Error>;

    /// The canonical form of `path` (case/encoding normalised). The default
    /// returns the path unchanged.
    fn canonical_path(&self, path: &str) -> String {
        path.to_string()
    }
}

/// The kind of new content staged for a trans-id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentKind {
    /// A regular file.
    File,
    /// A directory.
    Directory,
    /// A symlink.
    Symlink,
    /// A tree reference (nested tree).
    TreeReference,
}

impl ContentKind {
    /// The [`Kind`] this content becomes in the final tree.
    pub fn to_kind(self) -> Kind {
        match self {
            ContentKind::File => Kind::File,
            ContentKind::Directory => Kind::Directory,
            ContentKind::Symlink => Kind::Symlink,
            ContentKind::TreeReference => Kind::TreeReference,
        }
    }
}

/// The base bookkeeping of a tree transform: the pending operations and the
/// pure queries over the intended final tree.
///
/// This is breezy's `TreeTransform` / `TreeTransformBase` state. The disk and
/// apply layers wrap it. All maps are keyed by trans-id (a `new-N` string for
/// created entries, or an id assigned to an existing tree path).
pub struct TreeTransformBase<T: TransformTree> {
    /// The tree being transformed.
    pub(crate) tree: T,
    /// Counter for [`assign_id`](Self::assign_id).
    id_number: usize,
    /// Tree path -> trans-id (for existing tree entries touched by the transform).
    tree_path_ids: HashMap<String, String>,
    /// trans-id -> tree path (reverse of `tree_path_ids`).
    tree_id_paths: HashMap<String, String>,
    /// trans-id -> new basename.
    new_name: HashMap<String, String>,
    /// trans-id -> new parent trans-id.
    new_parent: HashMap<String, String>,
    /// trans-id -> the kind of newly-created content.
    new_contents: HashMap<String, ContentKind>,
    /// trans-ids whose existing content is to be removed.
    removed_contents: HashSet<String>,
    /// trans-id -> new executable bit.
    new_executability: HashMap<String, bool>,
    /// trans-id -> tree-reference revision for a new tree reference.
    new_reference_revision: HashMap<String, Vec<u8>>,
    /// trans-ids to be unversioned.
    removed_id: HashSet<String>,
    /// trans-id -> new file id (files becoming versioned).
    new_id: HashMap<String, FileId>,
    /// new file id -> trans-id (reverse of `new_id`).
    r_new_id: HashMap<FileId, String>,
    /// file id -> trans-id for ids not present in the tree.
    non_present_ids: HashMap<FileId, String>,
    /// The trans-id of the tree root, if the tree has a versioned root.
    new_root: Option<String>,
    /// Whether the target filesystem is case sensitive.
    case_sensitive: bool,
}

impl<T: TransformTree> TreeTransformBase<T> {
    /// Create a transform over `tree`.
    pub fn new(tree: T, case_sensitive: bool) -> Self {
        let mut base = TreeTransformBase {
            tree,
            id_number: 0,
            tree_path_ids: HashMap::new(),
            tree_id_paths: HashMap::new(),
            new_name: HashMap::new(),
            new_parent: HashMap::new(),
            new_contents: HashMap::new(),
            removed_contents: HashSet::new(),
            new_executability: HashMap::new(),
            new_reference_revision: HashMap::new(),
            removed_id: HashSet::new(),
            new_id: HashMap::new(),
            r_new_id: HashMap::new(),
            non_present_ids: HashMap::new(),
            new_root: None,
            case_sensitive,
        };
        // The root trans-id is assigned eagerly when the tree has a root.
        if base.tree.is_versioned("") {
            base.new_root = Some(base.trans_id_tree_path(""));
        }
        base
    }

    /// The tree being transformed.
    pub fn tree(&self) -> &T {
        &self.tree
    }

    /// The tree root's trans-id, if any.
    pub fn root(&self) -> Option<&str> {
        self.new_root.as_deref()
    }

    /// Produce a fresh trans-id.
    pub fn assign_id(&mut self) -> String {
        let id = format!("new-{}", self.id_number);
        self.id_number += 1;
        id
    }

    /// The trans-id for an existing tree `path`, assigning one if needed.
    pub fn trans_id_tree_path(&mut self, path: &str) -> String {
        let path = self.tree.canonical_path(path);
        if let Some(id) = self.tree_path_ids.get(&path) {
            return id.clone();
        }
        let id = self.assign_id();
        self.tree_path_ids.insert(path.clone(), id.clone());
        self.tree_id_paths.insert(id.clone(), path);
        id
    }

    /// The parent trans-id of `trans_id` in the tree (before the transform).
    /// Returns [`ROOT_PARENT`] for the root.
    pub fn get_tree_parent(&mut self, trans_id: &str) -> String {
        let path = self
            .tree_id_paths
            .get(trans_id)
            .cloned()
            .unwrap_or_default();
        if path.is_empty() {
            return ROOT_PARENT.to_string();
        }
        let parent = dirname(&path);
        self.trans_id_tree_path(&parent)
    }

    /// Assign a trans-id to a new path with `name` under `parent`.
    pub fn create_path(&mut self, name: &str, parent: &str) -> Result<String, Error> {
        let trans_id = self.assign_id();
        unique_add(&mut self.new_name, trans_id.clone(), name.to_string())?;
        unique_add(&mut self.new_parent, trans_id.clone(), parent.to_string())?;
        Ok(trans_id)
    }

    /// Change the name/parent assigned to `trans_id`.
    pub fn adjust_path(&mut self, name: &str, parent: &str, trans_id: &str) -> Result<(), Error> {
        if Some(trans_id) == self.new_root.as_deref() {
            return Err(Error::CantMoveRoot);
        }
        self.new_name.insert(trans_id.to_string(), name.to_string());
        self.new_parent
            .insert(trans_id.to_string(), parent.to_string());
        Ok(())
    }

    /// Schedule the current contents of `trans_id` for deletion, if it exists.
    pub fn delete_contents(&mut self, trans_id: &str) {
        if self.tree_kind_of(trans_id).is_some() {
            self.removed_contents.insert(trans_id.to_string());
        }
    }

    /// Cancel a scheduled contents deletion.
    pub fn cancel_deletion(&mut self, trans_id: &str) {
        self.removed_contents.remove(trans_id);
    }

    /// Schedule `trans_id` to become unversioned.
    pub fn unversion_file(&mut self, trans_id: &str) {
        self.removed_id.insert(trans_id.to_string());
    }

    /// Delete and unversion a versioned file.
    pub fn delete_versioned(&mut self, trans_id: &str) {
        self.delete_contents(trans_id);
        self.unversion_file(trans_id);
    }

    /// Schedule the executable bit for `trans_id`. `None` unschedules it.
    pub fn set_executability(&mut self, executability: Option<bool>, trans_id: &str) {
        match executability {
            None => {
                self.new_executability.remove(trans_id);
            }
            Some(value) => {
                self.new_executability.insert(trans_id.to_string(), value);
            }
        }
    }

    /// Set the tree-reference revision for a new tree reference.
    pub fn set_tree_reference(
        &mut self,
        revision_id: Vec<u8>,
        trans_id: &str,
    ) -> Result<(), Error> {
        unique_add(
            &mut self.new_reference_revision,
            trans_id.to_string(),
            revision_id,
        )
    }

    /// Record that `trans_id` becomes versioned with `file_id`.
    pub fn version_file(&mut self, trans_id: &str, file_id: FileId) -> Result<(), Error> {
        unique_add(&mut self.new_id, trans_id.to_string(), file_id.clone())?;
        unique_add(&mut self.r_new_id, file_id, trans_id.to_string())?;
        Ok(())
    }

    /// Undo a previous [`version_file`](Self::version_file).
    pub fn cancel_versioning(&mut self, trans_id: &str) {
        if let Some(file_id) = self.new_id.remove(trans_id) {
            self.r_new_id.remove(&file_id);
        }
    }

    /// Record new content of `kind` for `trans_id`.
    pub(crate) fn set_new_contents(&mut self, trans_id: &str, kind: ContentKind) {
        self.new_contents.insert(trans_id.to_string(), kind);
    }

    /// Cancel staged content creation for `trans_id`.
    pub(crate) fn cancel_contents(&mut self, trans_id: &str) {
        self.new_contents.remove(trans_id);
    }

    /// The tree kind at `trans_id`'s tree path, or `None` if it has no tree
    /// path or the path is absent.
    fn tree_kind_of(&self, trans_id: &str) -> Option<Kind> {
        let path = self.tree_id_paths.get(trans_id)?;
        self.tree.tree_kind(path)
    }

    /// The final kind of `trans_id` after the transform is applied.
    pub fn final_kind(&self, trans_id: &str) -> Option<Kind> {
        if let Some(kind) = self.new_contents.get(trans_id) {
            if self.new_reference_revision.contains_key(trans_id) {
                return Some(Kind::TreeReference);
            }
            return Some(kind.to_kind());
        }
        if self.removed_contents.contains(trans_id) {
            return None;
        }
        self.tree_kind_of(trans_id)
    }

    /// The tree path of `trans_id`, if it maps to an existing tree entry.
    pub fn tree_path(&self, trans_id: &str) -> Option<&str> {
        self.tree_id_paths.get(trans_id).map(String::as_str)
    }

    /// The final parent trans-id of `trans_id` (its scheduled parent, else its
    /// tree parent).
    pub fn final_parent(&mut self, trans_id: &str) -> String {
        if let Some(parent) = self.new_parent.get(trans_id) {
            return parent.clone();
        }
        self.get_tree_parent(trans_id)
    }

    /// The final basename of `trans_id`.
    pub fn final_name(&self, trans_id: &str) -> Result<String, Error> {
        if let Some(name) = self.new_name.get(trans_id) {
            return Ok(name.clone());
        }
        match self.tree_id_paths.get(trans_id) {
            Some(path) => Ok(basename(path)),
            None => Err(Error::NoFinalPath(trans_id.to_string())),
        }
    }

    /// Whether `trans_id`'s path (name or parent) has changed.
    pub fn path_changed(&self, trans_id: &str) -> bool {
        self.new_name.contains_key(trans_id) || self.new_parent.contains_key(trans_id)
    }

    /// Whether `trans_id` has newly-staged content.
    pub fn has_new_contents(&self, trans_id: &str) -> bool {
        self.new_contents.contains_key(trans_id)
    }

    /// The file id of `trans_id` in the tree (before the transform), or `None`.
    pub fn tree_file_id(&self, trans_id: &str) -> Option<FileId> {
        let path = self.tree_id_paths.get(trans_id)?;
        self.tree.path2id(path)
    }

    /// The final file id of `trans_id` after the transform: its new id if it
    /// is becoming versioned, else its tree id unless it is being unversioned.
    pub fn final_file_id(&self, trans_id: &str) -> Option<FileId> {
        if let Some(file_id) = self.new_id.get(trans_id) {
            return Some(file_id.clone());
        }
        if self.removed_id.contains(trans_id) {
            return None;
        }
        self.tree_file_id(trans_id)
    }

    /// Whether `trans_id` will be versioned after the transform.
    pub fn final_is_versioned(&self, trans_id: &str) -> bool {
        self.final_file_id(trans_id).is_some()
    }

    /// The trans-id for `file_id`: its new-id trans-id, its tree path's
    /// trans-id, or a freshly-assigned id recorded as non-present.
    pub fn trans_id_file_id(&mut self, file_id: &FileId) -> String {
        if let Some(id) = self.r_new_id.get(file_id) {
            return id.clone();
        }
        if let Some(path) = self.tree.id2path(file_id) {
            return self.trans_id_tree_path(&path);
        }
        if let Some(id) = self.non_present_ids.get(file_id) {
            return id.clone();
        }
        let id = self.assign_id();
        self.non_present_ids.insert(file_id.clone(), id.clone());
        id
    }

    /// A map of parent trans-id -> set of child trans-ids, over new paths and
    /// the parents of touched tree entries.
    pub fn by_parent(&mut self) -> HashMap<String, HashSet<String>> {
        let mut items: Vec<(String, String)> = self
            .new_parent
            .iter()
            .map(|(t, p)| (t.clone(), p.clone()))
            .collect();
        let tree_ids: Vec<String> = self.tree_id_paths.keys().cloned().collect();
        for trans_id in tree_ids {
            let parent = self.final_parent(&trans_id);
            items.push((trans_id, parent));
        }
        let mut by_parent: HashMap<String, HashSet<String>> = HashMap::new();
        for (trans_id, parent_id) in items {
            by_parent.entry(parent_id).or_default().insert(trans_id);
        }
        by_parent
    }

    /// Whether the target filesystem is case sensitive.
    pub fn case_sensitive(&self) -> bool {
        self.case_sensitive
    }

    pub(crate) fn new_contents_map(&self) -> &HashMap<String, ContentKind> {
        &self.new_contents
    }

    pub(crate) fn removed_contents_set(&self) -> &HashSet<String> {
        &self.removed_contents
    }

    pub(crate) fn removed_id_set(&self) -> &HashSet<String> {
        &self.removed_id
    }

    pub(crate) fn new_id_map(&self) -> &HashMap<String, FileId> {
        &self.new_id
    }

    pub(crate) fn new_executability_map(&self) -> &HashMap<String, bool> {
        &self.new_executability
    }

    pub(crate) fn new_reference_revision_map(&self) -> &HashMap<String, Vec<u8>> {
        &self.new_reference_revision
    }

    pub(crate) fn tree_id_paths_map(&self) -> &HashMap<String, String> {
        &self.tree_id_paths
    }
}

/// Add `(key, value)` to `map`, erroring if `key` is already present.
fn unique_add<K, V>(map: &mut HashMap<K, V>, key: K, value: V) -> Result<(), Error>
where
    K: std::hash::Hash + Eq + std::fmt::Debug,
{
    if map.contains_key(&key) {
        return Err(Error::DuplicateKey(format!("{key:?}")));
    }
    map.insert(key, value);
    Ok(())
}

/// The parent directory of a tree-relative path (`""` for a top-level entry).
fn dirname(path: &str) -> String {
    match path.rfind('/') {
        Some(i) => path[..i].to_string(),
        None => String::new(),
    }
}

/// The basename of a tree-relative path.
fn basename(path: &str) -> String {
    match path.rfind('/') {
        Some(i) => path[i + 1..].to_string(),
        None => path.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A minimal in-memory tree for exercising the base bookkeeping.
    struct FakeTree {
        // path -> (file_id, kind); "" is the root.
        entries: HashMap<String, (FileId, Kind)>,
    }

    impl FakeTree {
        fn new() -> Self {
            let mut entries = HashMap::new();
            entries.insert(
                String::new(),
                (FileId::from(b"root-id".to_vec()), Kind::Directory),
            );
            FakeTree { entries }
        }

        fn add(&mut self, path: &str, id: &[u8], kind: Kind) {
            self.entries
                .insert(path.to_string(), (FileId::from(id.to_vec()), kind));
        }
    }

    impl TransformTree for FakeTree {
        fn is_versioned(&self, path: &str) -> bool {
            self.entries.contains_key(path)
        }
        fn tree_kind(&self, path: &str) -> Option<Kind> {
            self.entries.get(path).map(|(_, k)| *k)
        }
        fn id2path(&self, file_id: &FileId) -> Option<String> {
            self.entries
                .iter()
                .find(|(_, (id, _))| id == file_id)
                .map(|(p, _)| p.clone())
        }
        fn path2id(&self, path: &str) -> Option<FileId> {
            self.entries.get(path).map(|(id, _)| id.clone())
        }
        fn tree_children(&self, path: &str) -> Result<Vec<String>, Error> {
            let prefix = if path.is_empty() {
                String::new()
            } else {
                format!("{path}/")
            };
            Ok(self
                .entries
                .keys()
                .filter(|p| {
                    !p.is_empty() && p.starts_with(&prefix) && !p[prefix.len()..].contains('/')
                })
                .map(|p| basename(p))
                .collect())
        }
    }

    #[test]
    fn assign_id_is_sequential() {
        let mut tt = TreeTransformBase::new(FakeTree::new(), true);
        assert_eq!(tt.assign_id(), "new-1");
        assert_eq!(tt.assign_id(), "new-2");
    }

    #[test]
    fn root_is_assigned_when_versioned() {
        let tt = TreeTransformBase::new(FakeTree::new(), true);
        // The root trans-id is new-0 (assigned first in the constructor).
        assert_eq!(tt.root(), Some("new-0"));
    }

    #[test]
    fn create_path_and_final_name_parent() {
        let mut tt = TreeTransformBase::new(FakeTree::new(), true);
        let root = tt.root().unwrap().to_string();
        let tid = tt.create_path("hello.txt", &root).unwrap();
        assert_eq!(tt.final_name(&tid).unwrap(), "hello.txt");
        assert_eq!(tt.final_parent(&tid), root);
        assert!(tt.path_changed(&tid));
    }

    #[test]
    fn final_kind_reflects_new_and_removed_contents() {
        let mut tree = FakeTree::new();
        tree.add("a.txt", b"a-id", Kind::File);
        let mut tt = TreeTransformBase::new(tree, true);
        // A new directory.
        let root = tt.root().unwrap().to_string();
        let tid = tt.create_path("sub", &root).unwrap();
        tt.set_new_contents(&tid, ContentKind::Directory);
        assert_eq!(tt.final_kind(&tid), Some(Kind::Directory));
        // Deleting an existing file yields no final kind.
        let a = tt.trans_id_tree_path("a.txt");
        assert_eq!(tt.final_kind(&a), Some(Kind::File));
        tt.delete_contents(&a);
        assert_eq!(tt.final_kind(&a), None);
    }

    #[test]
    fn version_and_unversion_track_final_file_id() {
        let mut tree = FakeTree::new();
        tree.add("a.txt", b"a-id", Kind::File);
        let mut tt = TreeTransformBase::new(tree, true);
        let a = tt.trans_id_tree_path("a.txt");
        assert_eq!(tt.final_file_id(&a), Some(FileId::from(b"a-id".to_vec())));
        tt.unversion_file(&a);
        assert_eq!(tt.final_file_id(&a), None);

        let root = tt.root().unwrap().to_string();
        let tid = tt.create_path("new.txt", &root).unwrap();
        tt.version_file(&tid, FileId::from(b"new-id".to_vec()))
            .unwrap();
        assert!(tt.final_is_versioned(&tid));
        assert_eq!(tt.trans_id_file_id(&FileId::from(b"new-id".to_vec())), tid);
    }

    #[test]
    fn by_parent_groups_children() {
        let mut tree = FakeTree::new();
        tree.add("a.txt", b"a-id", Kind::File);
        let mut tt = TreeTransformBase::new(tree, true);
        let root = tt.root().unwrap().to_string();
        let a = tt.trans_id_tree_path("a.txt");
        let by_parent = tt.by_parent();
        assert!(by_parent.get(&root).unwrap().contains(&a));
    }

    #[test]
    fn adjust_path_of_root_errors() {
        let mut tt = TreeTransformBase::new(FakeTree::new(), true);
        let root = tt.root().unwrap().to_string();
        assert!(matches!(
            tt.adjust_path("x", &root, &root),
            Err(Error::CantMoveRoot)
        ));
    }
}
