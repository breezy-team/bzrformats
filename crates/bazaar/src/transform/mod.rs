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
            for child in self.registered_children_of(parent) {
                changed.insert(child);
            }
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

    /// The registered tree children of `parent_id` (its trans-id children among
    /// the tree paths seen so far).
    fn registered_children_of(&self, parent_id: &str) -> Vec<String> {
        let parent_path = match self.tree_id_paths.get(parent_id) {
            Some(p) => p.clone(),
            None => return Vec::new(),
        };
        let prefix = if parent_path.is_empty() {
            String::new()
        } else {
            format!("{parent_path}/")
        };
        self.tree_path_ids
            .iter()
            .filter(|(path, _)| {
                !path.is_empty() && path.starts_with(&prefix) && !path[prefix.len()..].contains('/')
            })
            .map(|(_, id)| id.clone())
            .collect()
    }

    /// Register a tree path's trans-id, recording the reverse mapping. Used
    /// while walking a directory's children.
    fn register_tree_path(&mut self, path: &str) -> String {
        self.trans_id_tree_path(path)
    }

    /// Register every on-disk child of the directory at `trans_id` (breezy's
    /// `iter_tree_children`), so conflict detection sees them.
    ///
    /// Like Python's `iter_tree_children`, this lists the directory on disk
    /// directly (via the tree's absolute path) rather than through a tree
    /// method; a path that is missing or not a directory yields nothing.
    fn add_tree_children_of(&mut self, trans_id: &str) -> Result<(), Error> {
        use std::io::ErrorKind;

        let path = match self.tree_id_paths.get(trans_id) {
            Some(p) => p.clone(),
            None => return Ok(()),
        };
        let abspath = self.tree.abspath(&path);
        let listing_err =
            |e: std::io::Error| Error::Tree(format!("listing {}: {e}", abspath.display()));
        let dir = match std::fs::read_dir(&abspath) {
            Ok(d) => d,
            Err(e) if matches!(e.kind(), ErrorKind::NotFound | ErrorKind::NotADirectory) => {
                return Ok(())
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
        for name in names {
            let child_path = joinpath(&path, &name);
            if self.tree.is_control_filename(&child_path) {
                continue;
            }
            self.register_tree_path(&child_path);
        }
        Ok(())
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
}
