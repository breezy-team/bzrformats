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

pub mod apply;
pub mod disk;

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
    /// The orphaning policy forbids creating an orphan.
    OrphaningForbidden(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::DuplicateKey(k) => write!(f, "duplicate key: {k}"),
            Error::NoFinalPath(t) => write!(f, "no final path for {t}"),
            Error::CantMoveRoot => write!(f, "cannot move the tree root"),
            Error::Malformed(m) => write!(f, "malformed transform: {m}"),
            Error::Tree(m) => write!(f, "tree error: {m}"),
            Error::OrphaningForbidden(policy) => {
                write!(f, "policy: {policy} doesn't allow creating orphans")
            }
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

    /// Whether `path` is a control file (part of `.bzr`) that the transform
    /// must not touch.
    fn is_control_filename(&self, path: &str) -> bool;

    /// The kind recorded for `path` in the tree's inventory (which, unlike
    /// [`tree_kind`](Self::tree_kind), may differ from the on-disk kind).
    fn stored_kind(&self, path: &str) -> Option<Kind> {
        self.tree_kind(path)
    }

    /// Whether the tree's filesystem supports symlinks.
    fn supports_symlinks(&self) -> bool {
        true
    }

    /// Whether `kind` is a kind that can be versioned in this tree.
    fn versionable_kind(&self, kind: Kind) -> bool {
        matches!(
            kind,
            Kind::File | Kind::Directory | Kind::Symlink | Kind::TreeReference
        )
    }

    /// The absolute on-disk path of tree-relative `path`.
    fn abspath(&self, path: &str) -> std::path::PathBuf;

    /// Apply `delta` to the tree's live inventory (the mutation an applied
    /// transform performs).
    fn apply_inventory_delta(
        &mut self,
        delta: &crate::inventory_delta::InventoryDelta,
    ) -> Result<(), Error>;

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

/// A raw (uncooked) conflict detected in a transform, mirroring breezy's
/// conflict tuples. These are what `find_raw_conflicts` produces and what
/// conflict resolution consumes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RawConflict {
    /// An entry is its own ancestor.
    ParentLoop { trans_id: String },
    /// A versioned child under an unversioned parent.
    UnversionedParent { parent_id: String },
    /// A file scheduled to be versioned has no contents.
    VersioningNoContents { trans_id: String },
    /// A file scheduled to be versioned has an unversionable kind.
    VersioningBadKind { trans_id: String, kind: Kind },
    /// An executability change on an unversioned entry.
    UnversionedExecutability { trans_id: String },
    /// An executability change on a non-file entry.
    NonFileExecutability { trans_id: String },
    /// New content would overwrite an existing entry not scheduled for removal.
    Overwrite { trans_id: String, name: String },
    /// Two entries in one directory share a name.
    Duplicate {
        /// The first entry's trans-id.
        old_trans_id: String,
        /// The second entry's trans-id.
        new_trans_id: String,
        /// The shared name.
        name: String,
    },
    /// A directory needed to hold children is being deleted.
    MissingParent { parent_id: String },
    /// A parent that must be a directory is not one.
    NonDirectoryParent { parent_id: String },
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

    /// The inactive file id of `trans_id`: the id it has in the tree, or the
    /// non-present id recorded for it. Unlike [`final_file_id`](Self::final_file_id)
    /// this ignores unversioning, so it recovers the identity of an entry the
    /// transform is about to version or has just unversioned.
    pub fn inactive_file_id(&self, trans_id: &str) -> Option<FileId> {
        if let Some(file_id) = self.tree_file_id(trans_id) {
            return Some(file_id);
        }
        self.non_present_ids
            .iter()
            .find(|(_, id)| id.as_str() == trans_id)
            .map(|(file_id, _)| file_id.clone())
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

    /// Find the potential orphans in a directory being removed.
    ///
    /// A directory can't be safely deleted if it still contains versioned
    /// files. Returns the trans-ids of its unversioned children, or `None` if
    /// at least one versioned child is present (so the directory must be kept).
    pub fn get_potential_orphans(&mut self, dir_id: &str) -> Option<Vec<String>> {
        let children = self.by_parent().remove(dir_id).unwrap_or_default();
        let mut orphans = Vec::new();
        for child_tid in children {
            if self.removed_contents.contains(&child_tid) {
                // Removed as part of the transform; it was versioned before,
                // so it is not an orphan.
                continue;
            }
            if self.final_is_versioned(&child_tid) {
                // A versioned file is present, so searching for orphans is
                // meaningless.
                return None;
            }
            orphans.push(child_tid);
        }
        Some(orphans)
    }

    /// Reparent every child of `old_parent` onto `new_parent`, keeping each
    /// child's final name. Returns the reparented children's trans-ids.
    pub fn reparent_transform_children(
        &mut self,
        old_parent: &str,
        new_parent: &str,
    ) -> Result<Vec<String>, Error> {
        let children: Vec<String> = self
            .by_parent()
            .remove(old_parent)
            .unwrap_or_default()
            .into_iter()
            .collect();
        for child in &children {
            let name = self.final_name(child)?;
            self.adjust_path(&name, new_parent, child)?;
        }
        Ok(children)
    }

    /// Whether the target filesystem is case sensitive.
    pub fn case_sensitive(&self) -> bool {
        self.case_sensitive
    }

    /// Whether a directory `parent_id` already has a child named `name`, among
    /// the transform's known children and on disk. Breezy's `_has_named_child`.
    fn has_named_child(&mut self, name: &str, parent_id: &str) -> Result<bool, Error> {
        let known = self.by_parent().get(parent_id).cloned().unwrap_or_default();
        for child in &known {
            if self.final_name(child)? == name {
                return Ok(true);
            }
        }
        let parent_path = match self.tree_id_paths.get(parent_id) {
            Some(p) => p.clone(),
            None => return Ok(false),
        };
        let child_path = joinpath(&parent_path, name);
        if let Some(child_id) = self.tree_path_ids.get(&child_path) {
            // A tree path the transform knows is among the children checked
            // above unless it was moved away, which breezy does not expect.
            return Err(Error::Malformed(format!(
                "child_id is missing: {name}, {parent_id}, {child_id}"
            )));
        }
        // Otherwise consult the filesystem.
        let abspath = self.tree.abspath(&child_path);
        match abspath.symlink_metadata() {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(Error::Tree(format!(
                "checking for {}: {e}",
                abspath.display()
            ))),
        }
    }

    /// An available `.~N~` backup name for `name` under `target_id`, avoiding
    /// collisions. Breezy's `_available_backup_name`.
    pub fn available_backup_name(&mut self, name: &str, target_id: &str) -> Result<String, Error> {
        let mut counter = 1;
        loop {
            let candidate = format!("{name}.~{counter}~");
            if !self.has_named_child(&candidate, target_id)? {
                return Ok(candidate);
            }
            counter += 1;
        }
    }

    /// Reinterpret requests to change the root directory (breezy's
    /// `fixup_new_roots`): fold a newly-created root's attributes and children
    /// into the existing root.
    pub fn fixup_new_roots(&mut self) -> Result<(), Error> {
        let new_roots: Vec<String> = self
            .new_parent
            .iter()
            .filter(|(_, p)| p.as_str() == ROOT_PARENT)
            .map(|(t, _)| t.clone())
            .collect();
        if new_roots.is_empty() {
            return Ok(());
        }
        if new_roots.len() != 1 {
            return Err(Error::Malformed("a tree cannot have two roots".to_string()));
        }
        if self.new_root.is_none() {
            self.new_root = Some(new_roots[0].clone());
            return Ok(());
        }
        let old_new_root = new_roots[0].clone();
        let new_root = self.new_root.clone().unwrap();
        // Unversion the new root's directory, adopting its (or the old root's)
        // file id.
        let file_id = if self.final_kind(&new_root).is_none() {
            self.final_file_id(&old_new_root)
        } else {
            self.final_file_id(&new_root)
        };
        if self.new_id.contains_key(&old_new_root) {
            self.cancel_versioning(&old_new_root);
        } else {
            self.unversion_file(&old_new_root);
        }
        if self.tree_file_id(&new_root).is_some() && !self.removed_id.contains(&new_root) {
            self.unversion_file(&new_root);
        }
        if let Some(file_id) = file_id {
            self.version_file(&new_root, file_id)?;
        }
        // Move children of the new root into the old root directory, first
        // making sure those it has on disk are known to the transform.
        self.add_tree_children_of(&old_new_root)?;
        let children: Vec<String> = self
            .by_parent()
            .get(&old_new_root)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect();
        for child in children {
            let name = self.final_name(&child)?;
            self.adjust_path(&name, &new_root, &child)?;
        }
        // Ensure the old new-root has no directory.
        if self.new_contents.contains_key(&old_new_root) {
            self.cancel_contents(&old_new_root);
        } else {
            self.delete_contents(&old_new_root);
        }
        // Prevent deletion of the real root directory.
        if self.removed_contents.contains(&new_root) {
            self.cancel_deletion(&new_root);
        }
        self.new_parent.remove(&old_new_root);
        self.new_name.remove(&old_new_root);
        Ok(())
    }

    /// The final tree-relative path of `trans_id` after the transform (breezy's
    /// `FinalPaths.get_path`). The root and [`ROOT_PARENT`] map to `""`.
    pub fn final_path(&mut self, trans_id: &str) -> Result<String, Error> {
        if Some(trans_id) == self.new_root.as_deref() || trans_id == ROOT_PARENT {
            return Ok(String::new());
        }
        let name = self.final_name(trans_id)?;
        let parent = self.final_parent(trans_id);
        if Some(parent.as_str()) == self.new_root.as_deref() {
            Ok(name)
        } else {
            let parent_path = self.final_path(&parent)?;
            Ok(joinpath(&parent_path, &name))
        }
    }

    /// The `(final_path, trans_id)` of every trans-id whose inventory entry
    /// changes (name, parent, file id, kind or executability), sorted by path.
    /// This is breezy's `_inventory_altered`.
    pub fn inventory_altered(&mut self) -> Result<Vec<(String, String)>, Error> {
        let mut changed: HashSet<String> = HashSet::new();
        // file ids that are new or changed.
        let new_file_id: HashSet<String> = self
            .new_id
            .iter()
            .filter(|(t, id)| Some(*id) != self.tree_file_id(t).as_ref())
            .map(|(t, _)| t.clone())
            .collect();
        for t in self.new_name.keys() {
            changed.insert(t.clone());
        }
        for t in self.new_parent.keys() {
            changed.insert(t.clone());
        }
        changed.extend(new_file_id.iter().cloned());
        for t in self.new_executability.keys() {
            changed.insert(t.clone());
        }
        // A kind change (removed AND re-added content) where the kind differs.
        let mut changed_kind: HashSet<String> = self.removed_contents.clone();
        changed_kind.retain(|t| self.new_contents.contains_key(t));
        changed_kind.retain(|t| !changed.contains(t));
        changed_kind.retain(|t| self.tree_kind_of(t) != self.final_kind(t));
        changed.extend(changed_kind);
        // Children of entries whose file id changed need re-parenting entries.
        for parent in &new_file_id {
            changed.extend(self.add_tree_children_of(parent)?);
        }
        let mut out = Vec::new();
        for t in changed {
            out.push((self.final_path(&t)?, t));
        }
        out.sort();
        Ok(out)
    }

    /// The `(final_path, trans_id)` of every new/changed entry, sorted by path.
    /// Breezy's `new_paths(filesystem_only=False)`.
    pub fn new_paths(&mut self) -> Result<Vec<(String, String)>, Error> {
        let mut ids: HashSet<String> = HashSet::new();
        ids.extend(self.new_name.keys().cloned());
        ids.extend(self.new_parent.keys().cloned());
        ids.extend(self.new_executability.keys().cloned());
        ids.extend(self.new_contents.keys().cloned());
        ids.extend(self.new_id.keys().cloned());
        let mut out = Vec::new();
        for t in ids {
            out.push((self.final_path(&t)?, t));
        }
        out.sort();
        Ok(out)
    }

    /// Register a tree path's trans-id, recording the reverse mapping. Used
    /// while walking a directory's children.
    fn register_tree_path(&mut self, path: &str) -> String {
        self.trans_id_tree_path(path)
    }

    /// Register every on-disk child of the directory at `trans_id` (breezy's
    /// `iter_tree_children`), so conflict detection sees them, and return
    /// their trans-ids.
    ///
    /// Like Python's `iter_tree_children`, this lists the directory on disk
    /// directly (via the tree's absolute path) rather than through a tree
    /// method; a path that is missing or not a directory yields nothing.
    fn add_tree_children_of(&mut self, trans_id: &str) -> Result<Vec<String>, Error> {
        use std::io::ErrorKind;

        let path = match self.tree_id_paths.get(trans_id) {
            Some(p) => p.clone(),
            None => return Ok(Vec::new()),
        };
        let abspath = self.tree.abspath(&path);
        let listing_err =
            |e: std::io::Error| Error::Tree(format!("listing {}: {e}", abspath.display()));
        let dir = match std::fs::read_dir(&abspath) {
            Ok(d) => d,
            Err(e) if matches!(e.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory) => {
                return Ok(Vec::new())
            }
            Err(e) => return Err(listing_err(e)),
        };
        let mut names: Vec<String> = Vec::new();
        for entry in dir {
            let name = entry.map_err(listing_err)?.file_name();
            let name = name.into_string().map_err(|name| {
                Error::Tree(format!(
                    "{} contains a file name that is not valid UTF-8: {name:?}",
                    abspath.display()
                ))
            })?;
            names.push(name);
        }
        let mut children = Vec::new();
        for name in names {
            let child_path = joinpath(&path, &name);
            if self.tree.is_control_filename(&child_path) {
                continue;
            }
            children.push(self.register_tree_path(&child_path));
        }
        Ok(children)
    }

    /// Ensure every child of every active parent is registered before
    /// conflict detection (breezy's `_add_tree_children`).
    fn add_tree_children(&mut self) -> Result<(), Error> {
        let mut parents: Vec<String> = self.by_parent().into_keys().collect();
        // Directories whose contents are removed.
        for trans_id in self.removed_contents.clone() {
            if self.tree_kind_of(&trans_id) == Some(Kind::Directory) {
                parents.push(trans_id);
            }
        }
        // Directories being unversioned.
        for trans_id in self.removed_id.clone() {
            match self.tree_id_paths.get(&trans_id) {
                Some(path) => {
                    if self.tree.stored_kind(path) == Some(Kind::Directory) {
                        parents.push(trans_id);
                    }
                }
                None => {
                    if self.tree_kind_of(&trans_id) == Some(Kind::Directory) {
                        parents.push(trans_id);
                    }
                }
            }
        }
        for parent_id in parents {
            self.add_tree_children_of(&parent_id)?;
        }
        Ok(())
    }

    /// Find all invariant violations in the transform (breezy's
    /// `find_raw_conflicts`).
    pub fn find_raw_conflicts(&mut self) -> Result<Vec<RawConflict>, Error> {
        self.add_tree_children()?;
        let by_parent = self.by_parent();
        let mut conflicts = Vec::new();
        conflicts.extend(self.unversioned_parents(&by_parent));
        conflicts.extend(self.parent_loops());
        conflicts.extend(self.duplicate_entries(&by_parent)?);
        conflicts.extend(self.parent_type_conflicts(&by_parent));
        conflicts.extend(self.improper_versioning());
        conflicts.extend(self.executability_conflicts());
        conflicts.extend(self.overwrite_conflicts()?);
        Ok(conflicts)
    }

    /// No entry may be its own ancestor.
    fn parent_loops(&mut self) -> Vec<RawConflict> {
        let mut out = Vec::new();
        let trans_ids: Vec<String> = self.new_parent.keys().cloned().collect();
        for trans_id in trans_ids {
            let mut seen: HashSet<String> = HashSet::new();
            let mut parent_id = trans_id.clone();
            while parent_id != ROOT_PARENT {
                seen.insert(parent_id.clone());
                parent_id = self.final_parent(&parent_id);
                if parent_id == trans_id {
                    out.push(RawConflict::ParentLoop {
                        trans_id: trans_id.clone(),
                    });
                }
                if seen.contains(&parent_id) {
                    break;
                }
            }
        }
        out
    }

    /// A versioned parent's children must all be versioned.
    fn unversioned_parents(
        &self,
        by_parent: &HashMap<String, HashSet<String>>,
    ) -> Vec<RawConflict> {
        let mut out = Vec::new();
        for (parent_id, children) in by_parent {
            if parent_id == ROOT_PARENT {
                continue;
            }
            if self.final_is_versioned(parent_id) {
                continue;
            }
            if children.iter().any(|c| self.final_is_versioned(c)) {
                out.push(RawConflict::UnversionedParent {
                    parent_id: parent_id.clone(),
                });
            }
        }
        out
    }

    /// A file cannot be versioned with no contents or a bad kind.
    fn improper_versioning(&self) -> Vec<RawConflict> {
        let mut out = Vec::new();
        for trans_id in self.new_id.keys() {
            match self.final_kind(trans_id) {
                Some(Kind::Symlink) if !self.tree.supports_symlinks() => continue,
                None => out.push(RawConflict::VersioningNoContents {
                    trans_id: trans_id.clone(),
                }),
                Some(kind) if !self.tree.versionable_kind(kind) => {
                    out.push(RawConflict::VersioningBadKind {
                        trans_id: trans_id.clone(),
                        kind,
                    })
                }
                Some(_) => {}
            }
        }
        out
    }

    /// Only versioned files may have their executability set.
    fn executability_conflicts(&self) -> Vec<RawConflict> {
        let mut out = Vec::new();
        for trans_id in self.new_executability.keys() {
            if !self.final_is_versioned(trans_id) {
                out.push(RawConflict::UnversionedExecutability {
                    trans_id: trans_id.clone(),
                });
            } else if self.final_kind(trans_id) != Some(Kind::File) {
                out.push(RawConflict::NonFileExecutability {
                    trans_id: trans_id.clone(),
                });
            }
        }
        out
    }

    /// New contents must not overwrite an existing entry unless it is also
    /// scheduled for removal.
    fn overwrite_conflicts(&self) -> Result<Vec<RawConflict>, Error> {
        let mut out = Vec::new();
        for trans_id in self.new_contents.keys() {
            if self.tree_kind_of(trans_id).is_none() {
                continue;
            }
            if !self.removed_contents.contains(trans_id) {
                out.push(RawConflict::Overwrite {
                    trans_id: trans_id.clone(),
                    name: self.final_name(trans_id)?,
                });
            }
        }
        Ok(out)
    }

    /// No directory may have two entries with the same name.
    fn duplicate_entries(
        &self,
        by_parent: &HashMap<String, HashSet<String>>,
    ) -> Result<Vec<RawConflict>, Error> {
        let mut out = Vec::new();
        if self.new_name.is_empty() && self.new_parent.is_empty() {
            return Ok(out);
        }
        for children in by_parent.values() {
            let mut name_ids: Vec<(String, String)> = Vec::new();
            for child in children {
                let mut name = self.final_name(child)?;
                if !self.case_sensitive {
                    name = name.to_lowercase();
                }
                name_ids.push((name, child.clone()));
            }
            name_ids.sort();
            let mut last: Option<(String, String)> = None;
            for (name, trans_id) in name_ids {
                let kind = self.final_kind(&trans_id);
                if kind.is_none() && !self.final_is_versioned(&trans_id) {
                    continue;
                }
                if let Some((last_name, last_tid)) = &last {
                    if &name == last_name {
                        out.push(RawConflict::Duplicate {
                            old_trans_id: last_tid.clone(),
                            new_trans_id: trans_id.clone(),
                            name: name.clone(),
                        });
                    }
                }
                last = Some((name, trans_id));
            }
        }
        Ok(out)
    }

    /// Children must have an existing directory parent.
    fn parent_type_conflicts(
        &self,
        by_parent: &HashMap<String, HashSet<String>>,
    ) -> Vec<RawConflict> {
        let mut out = Vec::new();
        for (parent_id, children) in by_parent {
            if parent_id == ROOT_PARENT {
                continue;
            }
            if !children.iter().any(|c| self.final_kind(c).is_some()) {
                continue;
            }
            match self.final_kind(parent_id) {
                None => out.push(RawConflict::MissingParent {
                    parent_id: parent_id.clone(),
                }),
                Some(Kind::Directory) => {}
                Some(_) => out.push(RawConflict::NonDirectoryParent {
                    parent_id: parent_id.clone(),
                }),
            }
        }
        out
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

/// Join `parent` and `child` into a tree-relative path (breezy's `joinpath`).
fn joinpath(parent: &str, child: &str) -> String {
    if parent.is_empty() {
        child.to_string()
    } else {
        format!("{parent}/{child}")
    }
}

/// Shared test fixtures for the transform layers.
#[cfg(test)]
pub(crate) mod tests_support {
    use super::*;

    /// A minimal in-memory tree for exercising the transform layers.
    pub(crate) struct FakeTree {
        // path -> (file_id, kind); "" is the root.
        entries: HashMap<String, (FileId, Kind)>,
        // The on-disk root, for the apply layer's abspath. When unset, abspath
        // returns a synthetic path (fine for the pure bookkeeping tests).
        basedir: Option<std::path::PathBuf>,
    }

    impl FakeTree {
        pub(crate) fn new() -> Self {
            let mut entries = HashMap::new();
            entries.insert(
                String::new(),
                (FileId::from(b"root-id".to_vec()), Kind::Directory),
            );
            FakeTree {
                entries,
                basedir: None,
            }
        }

        /// A tree with no root yet.
        pub(crate) fn rootless() -> Self {
            FakeTree {
                entries: HashMap::new(),
                basedir: None,
            }
        }

        /// A tree rooted at a real on-disk directory, for apply tests.
        pub(crate) fn with_basedir(basedir: std::path::PathBuf) -> Self {
            let mut t = Self::new();
            t.basedir = Some(basedir);
            t
        }

        pub(crate) fn add(&mut self, path: &str, id: &[u8], kind: Kind) {
            self.entries
                .insert(path.to_string(), (FileId::from(id.to_vec()), kind));
        }

        pub(crate) fn is_versioned_path(&self, path: &str) -> bool {
            self.entries.contains_key(path)
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
        fn is_control_filename(&self, path: &str) -> bool {
            path == ".bzr" || path.starts_with(".bzr/")
        }
        fn abspath(&self, path: &str) -> std::path::PathBuf {
            match &self.basedir {
                Some(base) => base.join(path),
                None => std::path::PathBuf::from("/fake").join(path),
            }
        }
        fn apply_inventory_delta(
            &mut self,
            delta: &crate::inventory_delta::InventoryDelta,
        ) -> Result<(), Error> {
            for entry in delta.iter() {
                if let Some(old) = &entry.old_path {
                    self.entries.remove(old);
                }
                if let (Some(new), Some(inv_entry)) = (&entry.new_path, &entry.new_entry) {
                    self.entries
                        .insert(new.clone(), (entry.file_id.clone(), inv_entry.kind()));
                }
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::tests_support::FakeTree;
    use super::*;

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

    #[test]
    fn parent_loop_is_a_conflict() {
        let mut tree = FakeTree::new();
        tree.add("a", b"a-id", Kind::Directory);
        tree.add("a/b", b"b-id", Kind::Directory);
        let mut tt = TreeTransformBase::new(tree, true);
        let a = tt.trans_id_tree_path("a");
        let b = tt.trans_id_tree_path("a/b");
        // Move `a` into its own child.
        tt.adjust_path("a", &b, &a).unwrap();
        assert_eq!(
            tt.find_raw_conflicts().unwrap(),
            vec![RawConflict::ParentLoop { trans_id: a }]
        );
    }

    #[test]
    fn versioned_child_of_unversioned_parent_is_a_conflict() {
        let mut tt = TreeTransformBase::new(FakeTree::new(), true);
        let root = tt.root().unwrap().to_string();
        let dir = tt.create_path("dir", &root).unwrap();
        tt.set_new_contents(&dir, ContentKind::Directory);
        let child = tt.create_path("f", &dir).unwrap();
        tt.set_new_contents(&child, ContentKind::File);
        tt.version_file(&child, FileId::from(b"f-id".to_vec()))
            .unwrap();
        assert_eq!(
            tt.find_raw_conflicts().unwrap(),
            vec![RawConflict::UnversionedParent { parent_id: dir }]
        );
    }

    #[test]
    fn executability_on_an_unversioned_file_is_a_conflict() {
        let mut tt = TreeTransformBase::new(FakeTree::new(), true);
        let root = tt.root().unwrap().to_string();
        let f = tt.create_path("f", &root).unwrap();
        tt.set_new_contents(&f, ContentKind::File);
        tt.set_executability(Some(true), &f);
        assert_eq!(
            tt.find_raw_conflicts().unwrap(),
            vec![RawConflict::UnversionedExecutability { trans_id: f }]
        );
    }

    #[test]
    fn child_of_a_removed_directory_is_a_conflict() {
        let mut tree = FakeTree::new();
        tree.add("dir", b"dir-id", Kind::Directory);
        let mut tt = TreeTransformBase::new(tree, true);
        let dir = tt.trans_id_tree_path("dir");
        tt.delete_contents(&dir);
        tt.unversion_file(&dir);
        let child = tt.create_path("f", &dir).unwrap();
        tt.set_new_contents(&child, ContentKind::File);
        assert_eq!(
            tt.find_raw_conflicts().unwrap(),
            vec![RawConflict::MissingParent { parent_id: dir }]
        );
    }

    #[test]
    fn new_contents_over_an_existing_entry_is_a_conflict() {
        let mut tree = FakeTree::new();
        tree.add("a.txt", b"a-id", Kind::File);
        let mut tt = TreeTransformBase::new(tree, true);
        let a = tt.trans_id_tree_path("a.txt");
        tt.set_new_contents(&a, ContentKind::File);
        assert_eq!(
            tt.find_raw_conflicts().unwrap(),
            vec![RawConflict::Overwrite {
                trans_id: a.clone(),
                name: "a.txt".to_string(),
            }]
        );

        // Removing the old contents first makes the replacement fine.
        tt.delete_contents(&a);
        assert_eq!(tt.find_raw_conflicts().unwrap(), vec![]);
    }

    #[test]
    fn names_differing_in_case_conflict_on_a_case_insensitive_tree() {
        let mut tt = TreeTransformBase::new(FakeTree::new(), false);
        let root = tt.root().unwrap().to_string();
        for name in ["README", "readme"] {
            let tid = tt.create_path(name, &root).unwrap();
            tt.set_new_contents(&tid, ContentKind::File);
        }
        let conflicts = tt.find_raw_conflicts().unwrap();
        assert!(matches!(
            conflicts.as_slice(),
            [RawConflict::Duplicate { name, .. }] if name == "readme"
        ));

        let mut tt = TreeTransformBase::new(FakeTree::new(), true);
        let root = tt.root().unwrap().to_string();
        for name in ["README", "readme"] {
            let tid = tt.create_path(name, &root).unwrap();
            tt.set_new_contents(&tid, ContentKind::File);
        }
        assert_eq!(tt.find_raw_conflicts().unwrap(), vec![]);
    }

    #[test]
    fn no_conflicts_for_a_simple_add() {
        let mut tt = TreeTransformBase::new(FakeTree::new(), true);
        let root = tt.root().unwrap().to_string();
        let tid = tt.create_path("new.txt", &root).unwrap();
        tt.set_new_contents(&tid, ContentKind::File);
        tt.version_file(&tid, FileId::from(b"new-id".to_vec()))
            .unwrap();
        assert_eq!(tt.find_raw_conflicts().unwrap(), vec![]);
    }

    /// A tree rooted in a fresh temporary directory.
    fn disk_tree() -> (tempfile::TempDir, FakeTree) {
        let dir = tempfile::tempdir().unwrap();
        let tree = FakeTree::with_basedir(dir.path().to_path_buf());
        (dir, tree)
    }

    /// Before conflicts are detected, the on-disk children of each directory
    /// the transform touches are registered, versioned or not. Control files
    /// are left alone.
    #[test]
    fn on_disk_children_are_registered() {
        let (dir, tree) = disk_tree();
        std::fs::write(dir.path().join("unversioned"), b"contents").unwrap();
        std::fs::create_dir(dir.path().join("sub")).unwrap();
        std::fs::write(dir.path().join("sub/nested"), b"contents").unwrap();
        std::fs::create_dir(dir.path().join(".bzr")).unwrap();
        let mut tt = TreeTransformBase::new(tree, true);
        let root = tt.root().unwrap().to_string();
        tt.create_path("new", &root).unwrap();

        tt.find_raw_conflicts().unwrap();

        let mut registered: Vec<&str> = tt.tree_path_ids.keys().map(String::as_str).collect();
        registered.sort();
        // Only the root is a parent in this transform, so `sub` is not
        // descended into.
        assert_eq!(registered, vec!["", "sub", "unversioned"]);
    }

    /// A path that is not a directory on disk has no children to register.
    #[test]
    fn a_file_has_no_on_disk_children() {
        let (dir, mut tree) = disk_tree();
        std::fs::write(dir.path().join("a.txt"), b"contents").unwrap();
        tree.add("a.txt", b"a-id", Kind::File);
        let mut tt = TreeTransformBase::new(tree, true);
        let a = tt.trans_id_tree_path("a.txt");
        tt.delete_contents(&a);
        assert_eq!(tt.find_raw_conflicts().unwrap(), vec![]);
    }

    /// When a directory gets another file id, the entries of what it holds
    /// on disk are altered with it, known to the transform or not.
    #[test]
    fn inventory_altered_includes_on_disk_children_of_a_re_identified_directory() {
        let (dir, mut tree) = disk_tree();
        std::fs::create_dir(dir.path().join("sub")).unwrap();
        std::fs::write(dir.path().join("sub/f"), b"contents").unwrap();
        tree.add("sub", b"sub-id", Kind::Directory);
        tree.add("sub/f", b"f-id", Kind::File);
        let mut tt = TreeTransformBase::new(tree, true);
        let sub = tt.trans_id_tree_path("sub");
        tt.unversion_file(&sub);
        tt.version_file(&sub, FileId::from(b"new-sub-id".to_vec()))
            .unwrap();

        let altered = tt.inventory_altered().unwrap();

        let child = tt.tree_path_ids.get("sub/f").cloned().unwrap();
        assert_eq!(
            altered,
            vec![("sub".to_string(), sub), ("sub/f".to_string(), child)]
        );
    }

    /// A directory that cannot be listed is an error, not an empty directory.
    #[test]
    fn unlistable_directory_is_an_error() {
        let (_dir, mut tree) = disk_tree();
        // A NUL byte makes the path one the filesystem refuses to look up.
        tree.add("bad\0dir", b"dir-id", Kind::Directory);
        let mut tt = TreeTransformBase::new(tree, true);
        let bad = tt.trans_id_tree_path("bad\0dir");
        tt.create_path("child", &bad).unwrap();

        let abspath = tt.tree.abspath("bad\0dir");
        let cause = std::fs::read_dir(&abspath).unwrap_err();
        match tt.find_raw_conflicts() {
            Err(Error::Tree(message)) => {
                assert_eq!(message, format!("listing {}: {}", abspath.display(), cause))
            }
            other => panic!("expected a tree error, got {:?}", other),
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn non_utf8_file_name_is_an_error() {
        use std::os::unix::ffi::OsStrExt;

        let (dir, tree) = disk_tree();
        let name = std::ffi::OsStr::from_bytes(b"caf\xe9");
        std::fs::write(dir.path().join(name), b"contents").unwrap();
        let mut tt = TreeTransformBase::new(tree, true);
        let root = tt.root().unwrap().to_string();
        tt.create_path("new", &root).unwrap();

        let abspath = tt.tree.abspath("");
        match tt.find_raw_conflicts() {
            Err(Error::Tree(message)) => assert_eq!(
                message,
                format!(
                    "{} contains a file name that is not valid UTF-8: {:?}",
                    abspath.display(),
                    name
                )
            ),
            other => panic!("expected a tree error, got {:?}", other),
        }
    }

    #[test]
    fn duplicate_name_is_a_conflict() {
        let mut tt = TreeTransformBase::new(FakeTree::new(), true);
        let root = tt.root().unwrap().to_string();
        let a = tt.create_path("dup", &root).unwrap();
        tt.set_new_contents(&a, ContentKind::File);
        let b = tt.create_path("dup", &root).unwrap();
        tt.set_new_contents(&b, ContentKind::File);
        let conflicts = tt.find_raw_conflicts().unwrap();
        assert!(conflicts
            .iter()
            .any(|c| matches!(c, RawConflict::Duplicate { name, .. } if name == "dup")));
    }

    #[test]
    fn available_backup_name_appends_a_counter() {
        let (_dir, tree) = disk_tree();
        let mut tt = TreeTransformBase::new(tree, true);
        let root = tt.root().unwrap().to_string();
        assert_eq!(
            tt.available_backup_name("a.txt", &root).unwrap(),
            "a.txt.~1~"
        );
    }

    /// The file being backed up is usually known to the transform already,
    /// as when revert moves an existing file out of the way.
    #[test]
    fn available_backup_name_for_an_existing_tree_file() {
        let (dir, mut tree) = disk_tree();
        std::fs::write(dir.path().join("a.txt"), b"contents").unwrap();
        tree.add("a.txt", b"a-id", Kind::File);
        let mut tt = TreeTransformBase::new(tree, true);
        let root = tt.root().unwrap().to_string();
        tt.trans_id_tree_path("a.txt");
        assert_eq!(
            tt.available_backup_name("a.txt", &root).unwrap(),
            "a.txt.~1~"
        );
    }

    #[test]
    fn available_backup_name_skips_names_on_disk() {
        let (dir, tree) = disk_tree();
        std::fs::write(dir.path().join("a.txt.~1~"), b"older backup").unwrap();
        let mut tt = TreeTransformBase::new(tree, true);
        let root = tt.root().unwrap().to_string();
        assert_eq!(
            tt.available_backup_name("a.txt", &root).unwrap(),
            "a.txt.~2~"
        );
    }

    #[test]
    fn available_backup_name_skips_names_in_the_transform() {
        let (_dir, tree) = disk_tree();
        let mut tt = TreeTransformBase::new(tree, true);
        let root = tt.root().unwrap().to_string();
        tt.create_path("a.txt.~1~", &root).unwrap();
        assert_eq!(
            tt.available_backup_name("a.txt", &root).unwrap(),
            "a.txt.~2~"
        );
    }

    /// A candidate that is a tree path moved elsewhere by the transform is
    /// refused, as breezy refuses it.
    #[test]
    fn available_backup_name_rejects_a_moved_tree_path() {
        let (_dir, mut tree) = disk_tree();
        tree.add("a.txt.~1~", b"backup-id", Kind::File);
        let mut tt = TreeTransformBase::new(tree, true);
        let root = tt.root().unwrap().to_string();
        let moved = tt.trans_id_tree_path("a.txt.~1~");
        tt.adjust_path("elsewhere", &root, &moved).unwrap();
        match tt.available_backup_name("a.txt", &root) {
            Err(Error::Malformed(message)) => assert_eq!(
                message,
                format!("child_id is missing: a.txt.~1~, {root}, {moved}")
            ),
            other => panic!("expected a malformed transform, got {:?}", other),
        }
    }

    /// A candidate the filesystem cannot be asked about is an error, not a
    /// free name.
    #[test]
    fn available_backup_name_reports_filesystem_errors() {
        let (_dir, tree) = disk_tree();
        let mut tt = TreeTransformBase::new(tree, true);
        let root = tt.root().unwrap().to_string();

        // A NUL byte makes the candidate a path the filesystem refuses.
        let abspath = tt.tree.abspath("bad\0name.~1~");
        let cause = abspath.symlink_metadata().unwrap_err();
        match tt.available_backup_name("bad\0name", &root) {
            Err(Error::Tree(message)) => assert_eq!(
                message,
                format!("checking for {}: {}", abspath.display(), cause)
            ),
            other => panic!("expected a tree error, got {:?}", other),
        }
    }

    /// When an existing directory becomes the root, what it holds on disk
    /// moves to the root with it, known to the transform or not.
    #[test]
    fn fixup_new_roots_moves_on_disk_children() {
        let (dir, mut tree) = disk_tree();
        std::fs::create_dir(dir.path().join("sub")).unwrap();
        std::fs::write(dir.path().join("sub/f"), b"contents").unwrap();
        tree.add("sub", b"sub-id", Kind::Directory);
        let mut tt = TreeTransformBase::new(tree, true);
        let root = tt.root().unwrap().to_string();
        let sub = tt.trans_id_tree_path("sub");
        tt.adjust_path("", ROOT_PARENT, &sub).unwrap();

        tt.fixup_new_roots().unwrap();

        let child = tt.tree_path_ids.get("sub/f").cloned().unwrap();
        assert_eq!(tt.final_parent(&child), root);
        assert_eq!(tt.final_name(&child).unwrap(), "f");
    }

    #[test]
    fn fixup_new_roots_without_a_new_root() {
        let mut tt = TreeTransformBase::new(FakeTree::new(), true);
        let root = tt.root().unwrap().to_string();
        let child = tt.create_path("a.txt", &root).unwrap();
        tt.fixup_new_roots().unwrap();
        assert_eq!(tt.root(), Some(root.as_str()));
        assert_eq!(tt.final_parent(&child), root);
    }

    #[test]
    fn fixup_new_roots_rejects_two_roots() {
        let mut tt = TreeTransformBase::new(FakeTree::new(), true);
        tt.create_path("", ROOT_PARENT).unwrap();
        tt.create_path("", ROOT_PARENT).unwrap();
        match tt.fixup_new_roots() {
            Err(Error::Malformed(message)) => {
                assert_eq!(message, "a tree cannot have two roots")
            }
            other => panic!("expected a malformed transform, got {:?}", other),
        }
    }

    /// A tree without a root takes the new root as is.
    #[test]
    fn fixup_new_roots_adopts_the_first_root() {
        let mut tt = TreeTransformBase::new(FakeTree::rootless(), true);
        assert_eq!(tt.root(), None);
        let new_root = tt.create_path("", ROOT_PARENT).unwrap();
        tt.fixup_new_roots().unwrap();
        assert_eq!(tt.root(), Some(new_root.as_str()));
        assert_eq!(tt.final_parent(&new_root), ROOT_PARENT);
    }

    /// A second root is folded into the existing one: its children move
    /// there and it is dropped, while the existing root keeps its file id.
    #[test]
    fn fixup_new_roots_folds_a_new_root_into_the_existing_one() {
        let mut tt = TreeTransformBase::new(FakeTree::new(), true);
        let root = tt.root().unwrap().to_string();
        let new_root = tt.create_path("", ROOT_PARENT).unwrap();
        tt.set_new_contents(&new_root, ContentKind::Directory);
        tt.version_file(&new_root, FileId::from(b"new-root-id".to_vec()))
            .unwrap();
        let child = tt.create_path("child", &new_root).unwrap();
        tt.set_new_contents(&child, ContentKind::File);

        tt.fixup_new_roots().unwrap();

        assert_eq!(tt.root(), Some(root.as_str()));
        assert_eq!(tt.final_parent(&child), root);
        assert_eq!(tt.final_name(&child).unwrap(), "child");
        assert_eq!(
            tt.final_file_id(&root),
            Some(FileId::from(b"root-id".to_vec()))
        );
        assert_eq!(tt.final_file_id(&new_root), None);
        assert_eq!(tt.final_kind(&new_root), None);
        assert!(!tt.new_parent.contains_key(&new_root));
        assert!(!tt.new_name.contains_key(&new_root));
    }

    /// When the existing root directory is being removed, the root takes the
    /// new root's file id and its directory is kept.
    #[test]
    fn fixup_new_roots_replaces_a_removed_root() {
        let mut tt = TreeTransformBase::new(FakeTree::new(), true);
        let root = tt.root().unwrap().to_string();
        tt.delete_contents(&root);
        tt.unversion_file(&root);
        let new_root = tt.create_path("", ROOT_PARENT).unwrap();
        tt.set_new_contents(&new_root, ContentKind::Directory);
        tt.version_file(&new_root, FileId::from(b"new-root-id".to_vec()))
            .unwrap();

        tt.fixup_new_roots().unwrap();

        assert_eq!(tt.root(), Some(root.as_str()));
        assert_eq!(
            tt.final_file_id(&root),
            Some(FileId::from(b"new-root-id".to_vec()))
        );
        assert_eq!(tt.final_kind(&root), Some(Kind::Directory));
        assert_eq!(tt.final_file_id(&new_root), None);
    }

    #[test]
    fn versioning_no_contents_is_a_conflict() {
        let mut tt = TreeTransformBase::new(FakeTree::new(), true);
        let root = tt.root().unwrap().to_string();
        // A path that is versioned but never gets contents.
        let tid = tt.create_path("ghost", &root).unwrap();
        tt.version_file(&tid, FileId::from(b"ghost-id".to_vec()))
            .unwrap();
        let conflicts = tt.find_raw_conflicts().unwrap();
        assert!(conflicts.iter().any(|c| matches!(
            c,
            RawConflict::VersioningNoContents { trans_id } if trans_id == &tid
        )));
    }

    #[test]
    fn executability_on_a_directory_is_a_conflict() {
        let mut tt = TreeTransformBase::new(FakeTree::new(), true);
        let root = tt.root().unwrap().to_string();
        let tid = tt.create_path("sub", &root).unwrap();
        tt.set_new_contents(&tid, ContentKind::Directory);
        tt.version_file(&tid, FileId::from(b"sub-id".to_vec()))
            .unwrap();
        tt.set_executability(Some(true), &tid);
        let conflicts = tt.find_raw_conflicts().unwrap();
        assert!(conflicts.iter().any(|c| matches!(
            c,
            RawConflict::NonFileExecutability { trans_id } if trans_id == &tid
        )));
    }

    #[test]
    fn non_directory_parent_is_a_conflict() {
        let mut tree = FakeTree::new();
        tree.add("afile", b"afile-id", Kind::File);
        let mut tt = TreeTransformBase::new(tree, true);
        // Put a child under the existing file (not a directory).
        let parent = tt.trans_id_tree_path("afile");
        let child = tt.create_path("under", &parent).unwrap();
        tt.set_new_contents(&child, ContentKind::File);
        let conflicts = tt.find_raw_conflicts().unwrap();
        assert!(conflicts.iter().any(|c| matches!(
            c,
            RawConflict::NonDirectoryParent { parent_id } if parent_id == &parent
        )));
    }

    #[test]
    fn inactive_file_id_recovers_a_versioned_paths_id() {
        let mut tree = FakeTree::new();
        tree.add("afile", b"afile-id", Kind::File);
        let mut tt = TreeTransformBase::new(tree, true);
        let tid = tt.trans_id_tree_path("afile");
        // Even after unversioning, the tree id is recovered.
        tt.unversion_file(&tid);
        assert_eq!(
            tt.inactive_file_id(&tid),
            Some(FileId::from(b"afile-id".to_vec()))
        );
    }

    #[test]
    fn inactive_file_id_recovers_a_non_present_id() {
        let mut tt = TreeTransformBase::new(FakeTree::new(), true);
        let file_id = FileId::from(b"ghost-id".to_vec());
        let tid = tt.trans_id_file_id(&file_id);
        assert_eq!(tt.inactive_file_id(&tid), Some(file_id));
    }

    #[test]
    fn get_potential_orphans_lists_unversioned_children() {
        let mut tree = FakeTree::new();
        tree.add("dir", b"dir-id", Kind::Directory);
        let mut tt = TreeTransformBase::new(tree, true);
        let dir = tt.trans_id_tree_path("dir");
        // An unversioned child staged under the directory.
        let child = tt.create_path("child", &dir).unwrap();
        tt.set_new_contents(&child, ContentKind::File);
        let orphans = tt.get_potential_orphans(&dir).unwrap();
        assert_eq!(orphans, vec![child]);
    }

    #[test]
    fn get_potential_orphans_is_none_when_a_versioned_child_is_present() {
        let mut tree = FakeTree::new();
        tree.add("dir", b"dir-id", Kind::Directory);
        let mut tt = TreeTransformBase::new(tree, true);
        let dir = tt.trans_id_tree_path("dir");
        let child = tt.create_path("child", &dir).unwrap();
        tt.set_new_contents(&child, ContentKind::File);
        tt.version_file(&child, FileId::from(b"child-id".to_vec()))
            .unwrap();
        assert_eq!(tt.get_potential_orphans(&dir), None);
    }

    #[test]
    fn reparent_transform_children_moves_children_to_new_parent() {
        let mut tree = FakeTree::new();
        tree.add("old", b"old-id", Kind::Directory);
        tree.add("new", b"new-id", Kind::Directory);
        let mut tt = TreeTransformBase::new(tree, true);
        let old = tt.trans_id_tree_path("old");
        let new = tt.trans_id_tree_path("new");
        let child = tt.create_path("c", &old).unwrap();
        tt.set_new_contents(&child, ContentKind::File);
        let moved = tt.reparent_transform_children(&old, &new).unwrap();
        assert_eq!(moved, vec![child.clone()]);
        assert!(tt.by_parent().get(&new).unwrap().contains(&child));
    }
}
