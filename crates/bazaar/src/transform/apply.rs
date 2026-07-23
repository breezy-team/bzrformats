//! Applying a staged tree transform (breezy's `InventoryTreeTransform.apply`).
//!
//! Apply turns the staged [`DiskTreeTransform`] into real tree changes in two
//! ordered phases, guarded by a [`FileMover`] that can roll back on error:
//!
//! 1. **Removals** (child-to-parent order): delete contents scheduled for
//!    deletion, and move renamed entries out of the way into limbo.
//! 2. **Insertions** (parent-to-child order): rename staged limbo contents into
//!    their final places and set executability.
//!
//! It then builds an inventory delta from the final tree state and hands it to
//! [`TransformTree::apply_inventory_delta`]. The deletion staging is committed
//! only once both phases succeed.

use super::disk::DiskTreeTransform;
use super::{Error, TransformTree};
use crate::inventory::Entry as InvEntry;
use crate::inventory_delta::{InventoryDelta, InventoryDeltaEntry};
use crate::FileId;
use std::path::{Path, PathBuf};

/// A guarded sequence of filesystem renames and deletions.
///
/// Renames are recorded so they can be undone if a later step fails; deletions
/// are staged to a holding area and only made permanent once the whole apply
/// succeeds. Mirrors breezy's `_FileMover`.
#[derive(Default)]
pub struct FileMover {
    /// Renames performed so far, as `(from, to)`, newest last.
    renames: Vec<(PathBuf, PathBuf)>,
    /// Pending deletions, as `(original, holding)`.
    pending_deletions: Vec<(PathBuf, PathBuf)>,
}

impl FileMover {
    /// Rename `from` to `to`, recording it for possible rollback.
    pub fn rename(&mut self, from: &Path, to: &Path) -> std::io::Result<()> {
        std::fs::rename(from, to)?;
        self.renames.push((from.to_path_buf(), to.to_path_buf()));
        Ok(())
    }

    /// Move `from` aside to the holding path `to`, to be deleted on success.
    pub fn pre_delete(&mut self, from: &Path, to: &Path) -> std::io::Result<()> {
        std::fs::rename(from, to)?;
        self.pending_deletions
            .push((from.to_path_buf(), to.to_path_buf()));
        Ok(())
    }

    /// Undo all renames and restore all staged deletions (on error).
    pub fn rollback(&mut self) {
        for (from, to) in self.renames.drain(..).rev() {
            let _ = std::fs::rename(&to, &from);
        }
        for (original, holding) in self.pending_deletions.drain(..).rev() {
            let _ = std::fs::rename(&holding, &original);
        }
    }

    /// Permanently delete everything staged by [`pre_delete`](Self::pre_delete).
    pub fn apply_deletions(&mut self) -> std::io::Result<()> {
        for (_original, holding) in self.pending_deletions.drain(..) {
            delete_any(&holding)?;
        }
        Ok(())
    }
}

/// The outcome of applying a transform.
pub struct TransformResults {
    /// The tree-relative paths whose contents were created or changed.
    pub modified_paths: Vec<PathBuf>,
    /// How many entries were renamed during apply.
    pub rename_count: usize,
}

impl<T: TransformTree> DiskTreeTransform<T> {
    /// Generate the inventory delta describing the transform's net effect on
    /// the tree's inventory (breezy's `_generate_inventory_delta`).
    fn generate_inventory_delta(&mut self) -> Result<InventoryDelta, Error> {
        let mut entries: Vec<InventoryDeltaEntry> = Vec::new();

        // Removals: unversioned entries whose file id isn't just being moved.
        let removed: Vec<String> = self.base().removed_id_set().iter().cloned().collect();
        for trans_id in &removed {
            let file_id = if Some(trans_id.as_str()) == self.base().root() {
                self.base().tree().path2id("")
            } else {
                self.base().tree_file_id(trans_id)
            };
            let Some(file_id) = file_id else { continue };
            // A file id present under a new trans-id is moved, not deleted.
            if self.base().new_id_map().values().any(|id| id == &file_id) {
                continue;
            }
            let path = match self.base().tree_id_paths_map().get(trans_id) {
                Some(p) => p.clone(),
                None => continue,
            };
            entries.push(InventoryDeltaEntry {
                old_path: Some(path),
                new_path: None,
                file_id,
                new_entry: None,
            });
        }

        // Insertions/changes: every trans-id with an altered inventory entry.
        let altered = self.base_mut().inventory_altered()?;
        // Precompute each altered trans-id's final file id.
        let mut new_path_file_ids: std::collections::HashMap<String, Option<FileId>> =
            std::collections::HashMap::new();
        for (_path, trans_id) in &altered {
            new_path_file_ids.insert(trans_id.clone(), self.base().final_file_id(trans_id));
        }

        for (path, trans_id) in &altered {
            let Some(file_id) = new_path_file_ids.get(trans_id).cloned().flatten() else {
                continue;
            };
            let kind = match self.base().final_kind(trans_id) {
                Some(k) => k,
                None => {
                    // Fall back to the stored kind at the file's current path.
                    match self
                        .base()
                        .tree()
                        .id2path(&file_id)
                        .and_then(|p| self.base().tree().stored_kind(&p))
                    {
                        Some(k) => k,
                        None => continue,
                    }
                }
            };
            let name = self.base_mut().final_name(trans_id)?;
            let parent_trans_id = self.base_mut().final_parent(trans_id);
            let parent_file_id = new_path_file_ids
                .get(&parent_trans_id)
                .cloned()
                .flatten()
                .or_else(|| self.base().final_file_id(&parent_trans_id));

            let executable = self.base().new_executability_map().get(trans_id).copied();

            let new_entry = self.make_entry(
                trans_id,
                kind,
                name,
                file_id.clone(),
                parent_file_id,
                executable,
            );

            let old_path = self.base().tree().id2path(&file_id);
            entries.push(InventoryDeltaEntry {
                old_path,
                new_path: Some(path.clone()),
                file_id,
                new_entry: Some(new_entry),
            });
        }

        Ok(InventoryDelta::from(entries))
    }

    /// Build the inventory entry for a newly-added/changed trans-id.
    fn make_entry(
        &self,
        trans_id: &str,
        kind: crate::osutils::Kind,
        name: String,
        file_id: FileId,
        parent_file_id: Option<FileId>,
        executable: Option<bool>,
    ) -> InvEntry {
        use crate::osutils::Kind;
        // The root has no parent; everything else names its parent.
        let has_parent = parent_file_id.is_some();
        let parent_id = parent_file_id.unwrap_or_else(|| file_id.clone());
        match kind {
            Kind::Directory => {
                if !has_parent {
                    InvEntry::root(file_id, None)
                } else {
                    InvEntry::directory(file_id, name, parent_id, None)
                }
            }
            Kind::File => InvEntry::file(
                file_id,
                name,
                parent_id,
                None,
                None,
                None,
                Some(executable.unwrap_or(false)),
                None,
            ),
            Kind::Symlink => InvEntry::link(file_id, name, parent_id, None, None),
            Kind::TreeReference => {
                let reference = self
                    .base()
                    .new_reference_revision_map()
                    .get(trans_id)
                    .map(|r| crate::RevisionId::from(r.clone()));
                InvEntry::tree_reference(file_id, name, parent_id, None, reference)
            }
        }
    }

    /// Apply removals: delete removed contents and move renamed entries to
    /// limbo, in child-to-parent order.
    fn apply_removals(
        &mut self,
        mover: &mut FileMover,
        deletion_dir: &Path,
        rename_count: &mut usize,
    ) -> Result<(), Error> {
        let mut tree_paths: Vec<(String, String)> = self
            .base()
            .tree_id_paths_map()
            .iter()
            .map(|(t, p)| (p.clone(), t.clone()))
            .collect();
        // Child-to-parent: longest paths first.
        tree_paths.sort_by(|a, b| b.0.cmp(&a.0));
        for (path, trans_id) in tree_paths {
            if path.is_empty() {
                continue;
            }
            let full_path = self.base().tree().abspath(&path);
            if self.base().removed_contents_set().contains(&trans_id) {
                let delete_path = deletion_dir.join(&trans_id);
                mover.pre_delete(&full_path, &delete_path).map_err(io_err)?;
            } else if self.base().path_changed(&trans_id) {
                let limbo = self.limbo_name(&trans_id);
                match mover.rename(&full_path, &limbo) {
                    Ok(()) => *rename_count += 1,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(io_err(e)),
                }
            }
        }
        Ok(())
    }

    /// Apply insertions: rename staged contents into place and set the
    /// executable bit, in parent-to-child order.
    fn apply_insertions(
        &mut self,
        mover: &mut FileMover,
        rename_count: &mut usize,
    ) -> Result<Vec<PathBuf>, Error> {
        let new_paths = self.base_mut().new_paths()?;
        let mut modified = Vec::new();
        for (path, trans_id) in &new_paths {
            let full_path = self.base().tree().abspath(path);
            // In the simple limbo scheme, everything with staged contents
            // needs renaming from limbo into place.
            let has_contents = self.base().has_new_contents(trans_id);
            if has_contents {
                let limbo = self.limbo_name(trans_id);
                match mover.rename(&limbo, &full_path) {
                    Ok(()) => *rename_count += 1,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(io_err(e)),
                }
                modified.push(full_path.clone());
            }
            if let Some(exec) = self.base().new_executability_map().get(trans_id).copied() {
                set_executable(&full_path, exec).map_err(io_err)?;
            }
        }
        Ok(modified)
    }

    /// Apply the whole transform: check for conflicts (unless `no_conflicts`),
    /// perform the removal and insertion phases, then apply the inventory delta
    /// and clean up limbo. `deletion_dir` is a scratch dir for staged
    /// deletions (must exist).
    pub fn apply(
        &mut self,
        deletion_dir: &Path,
        no_conflicts: bool,
    ) -> Result<TransformResults, Error> {
        if !no_conflicts {
            let conflicts = self.base_mut().find_raw_conflicts()?;
            if !conflicts.is_empty() {
                return Err(Error::Malformed(format!("{conflicts:?}")));
            }
        }
        let inventory_delta = self.generate_inventory_delta()?;
        let mut mover = FileMover::default();
        let mut rename_count = 0;
        let modified = (|| {
            self.apply_removals(&mut mover, deletion_dir, &mut rename_count)?;
            self.apply_insertions(&mut mover, &mut rename_count)
        })();
        let modified = match modified {
            Ok(m) => m,
            Err(e) => {
                mover.rollback();
                return Err(e);
            }
        };
        mover.apply_deletions().map_err(io_err)?;

        // If the tree has no root file id, drop the root entry from the delta.
        let root_present = self
            .base()
            .root()
            .map(|r| self.base().final_file_id(r).is_some())
            .unwrap_or(false);
        let delta = if root_present {
            inventory_delta
        } else {
            InventoryDelta::from(
                inventory_delta
                    .iter()
                    .filter(|e| e.new_path.as_deref() != Some(""))
                    .cloned()
                    .collect::<Vec<_>>(),
            )
        };

        // The mutation goes through the tree (the format primitive).
        let tree = &mut self.base_mut().tree;
        tree.apply_inventory_delta(&delta)?;

        self.finalize()?;
        Ok(TransformResults {
            modified_paths: modified,
            rename_count,
        })
    }
}

/// Map an I/O error into a transform error.
fn io_err(e: std::io::Error) -> Error {
    Error::Tree(e.to_string())
}

/// Delete a file, directory or symlink, ignoring absence.
fn delete_any(path: &Path) -> std::io::Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(m) if m.is_dir() => std::fs::remove_dir_all(path),
        Ok(_) => std::fs::remove_file(path),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Set (or clear) the executable bit on `path`.
#[cfg(unix)]
fn set_executable(path: &Path, executable: bool) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mut perms = std::fs::metadata(path)?.permissions();
    let mode = perms.mode();
    let new_mode = if executable {
        mode | 0o111
    } else {
        mode & !0o111
    };
    perms.set_mode(new_mode);
    std::fs::set_permissions(path, perms)
}

#[cfg(not(unix))]
fn set_executable(_path: &Path, _executable: bool) -> std::io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transform::tests_support::FakeTree;
    use crate::transform::TreeTransformBase;

    /// Build a disk transform whose tree is rooted at a real temp dir, with a
    /// limbo and deletion dir alongside.
    fn apply_tt() -> (tempfile::TempDir, DiskTreeTransform<FakeTree>, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let base_dir = dir.path().join("tree");
        std::fs::create_dir(&base_dir).unwrap();
        let limbo = dir.path().join("limbo");
        std::fs::create_dir(&limbo).unwrap();
        let deletion = dir.path().join("del");
        std::fs::create_dir(&deletion).unwrap();
        let tree = FakeTree::with_basedir(base_dir);
        let base = TreeTransformBase::new(tree, true);
        (
            dir,
            DiskTreeTransform::new(base, limbo, cfg!(unix)),
            deletion,
        )
    }

    #[test]
    fn apply_creates_a_new_versioned_file() {
        let (_d, mut tt, deletion) = apply_tt();
        let root = tt.base().root().unwrap().to_string();
        let tid = tt.base_mut().create_path("new.txt", &root).unwrap();
        tt.create_file(b"hello\n", &tid, None).unwrap();
        tt.base_mut()
            .version_file(&tid, FileId::from(b"new-id".to_vec()))
            .unwrap();

        let base_dir = tt.base().tree().abspath("");
        tt.apply(&deletion, false).unwrap();

        // The file exists on disk in the tree, and the tree's inventory has it.
        assert_eq!(std::fs::read(base_dir.join("new.txt")).unwrap(), b"hello\n");
        assert!(tt.base().tree().is_versioned_path("new.txt"));
    }

    #[test]
    fn new_file_convenience_creates_and_versions() {
        let (_d, mut tt, deletion) = apply_tt();
        let root = tt.base().root().unwrap().to_string();
        tt.new_file(
            "conv.txt",
            &root,
            b"data\n",
            Some(FileId::from(b"conv-id".to_vec())),
            None,
            None,
        )
        .unwrap();
        let base_dir = tt.base().tree().abspath("");
        tt.apply(&deletion, false).unwrap();
        assert_eq!(std::fs::read(base_dir.join("conv.txt")).unwrap(), b"data\n");
        assert!(tt.base().tree().is_versioned_path("conv.txt"));
    }

    #[test]
    fn new_file_convenience_leaves_a_file_unversioned() {
        let (_d, mut tt, deletion) = apply_tt();
        let root = tt.base().root().unwrap().to_string();
        tt.new_file("plain.txt", &root, b"data\n", None, None, None)
            .unwrap();
        let base_dir = tt.base().tree().abspath("");
        tt.apply(&deletion, false).unwrap();
        assert_eq!(
            std::fs::read(base_dir.join("plain.txt")).unwrap(),
            b"data\n"
        );
        assert!(!tt.base().tree().is_versioned_path("plain.txt"));
    }

    #[test]
    fn new_directory_convenience_creates_and_versions() {
        let (_d, mut tt, deletion) = apply_tt();
        let root = tt.base().root().unwrap().to_string();
        let sub = tt
            .new_directory("sub", &root, Some(FileId::from(b"sub-id".to_vec())))
            .unwrap();
        tt.new_file(
            "inner.txt",
            &sub,
            b"data\n",
            Some(FileId::from(b"inner-id".to_vec())),
            None,
            None,
        )
        .unwrap();
        let base_dir = tt.base().tree().abspath("");
        tt.apply(&deletion, false).unwrap();
        assert!(base_dir.join("sub").is_dir());
        assert_eq!(
            std::fs::read(base_dir.join("sub/inner.txt")).unwrap(),
            b"data\n"
        );
        assert!(tt.base().tree().is_versioned_path("sub"));
        assert!(tt.base().tree().is_versioned_path("sub/inner.txt"));
    }

    #[cfg(unix)]
    #[test]
    fn new_symlink_convenience_creates_and_versions() {
        let (_d, mut tt, deletion) = apply_tt();
        let root = tt.base().root().unwrap().to_string();
        tt.new_symlink(
            "link",
            &root,
            "target",
            Some(FileId::from(b"link-id".to_vec())),
        )
        .unwrap();
        let base_dir = tt.base().tree().abspath("");
        tt.apply(&deletion, false).unwrap();
        assert_eq!(
            std::fs::read_link(base_dir.join("link")).unwrap(),
            std::path::PathBuf::from("target")
        );
        assert!(tt.base().tree().is_versioned_path("link"));
    }

    #[test]
    fn apply_deletes_a_removed_file() {
        let (_d, mut tt, deletion) = apply_tt();
        // An existing versioned file, present on disk.
        let base_dir = tt.base().tree().abspath("");
        std::fs::write(base_dir.join("gone.txt"), b"x\n").unwrap();
        tt.base_mut()
            .tree
            .add("gone.txt", b"gone-id", crate::osutils::Kind::File);
        let tid = tt.base_mut().trans_id_tree_path("gone.txt");
        tt.base_mut().delete_versioned(&tid);

        tt.apply(&deletion, false).unwrap();

        assert!(!base_dir.join("gone.txt").exists());
        assert!(!tt.base().tree().is_versioned_path("gone.txt"));
    }

    #[test]
    fn file_mover_rolls_back_renames_on_error() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a");
        let b = dir.path().join("b");
        std::fs::write(&a, b"a").unwrap();
        let mut mover = FileMover::default();
        mover.rename(&a, &b).unwrap();
        assert!(!a.exists() && b.exists());
        mover.rollback();
        // The rename is undone.
        assert!(a.exists() && !b.exists());
    }
}
