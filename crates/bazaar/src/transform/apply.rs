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
    /// Every rename done so far, oldest first, including those that moved
    /// something to the holding area.
    past_renames: Vec<(PathBuf, PathBuf)>,
    /// The holding-area paths to delete once the apply has succeeded.
    pending_deletions: Vec<PathBuf>,
}

impl FileMover {
    /// Rename `from` to `to`, remembering it for rollback.
    pub fn rename(&mut self, from: &Path, to: &Path) -> std::io::Result<()> {
        std::fs::rename(from, to)?;
        self.past_renames
            .push((from.to_path_buf(), to.to_path_buf()));
        Ok(())
    }

    /// Stage `from` for deletion by moving it to the holding path `to`.
    pub fn pre_delete(&mut self, from: &Path, to: &Path) -> std::io::Result<()> {
        self.rename(from, to)?;
        self.pending_deletions.push(to.to_path_buf());
        Ok(())
    }

    /// Undo every rename, newest first, restoring what was staged for
    /// deletion along the way. Stops at the first rename that cannot be
    /// undone.
    pub fn rollback(&mut self) -> std::io::Result<()> {
        let past_renames = std::mem::take(&mut self.past_renames);
        self.pending_deletions.clear();
        for (from, to) in past_renames.iter().rev() {
            std::fs::rename(to, from)?;
        }
        Ok(())
    }

    /// Make the staged deletions permanent.
    pub fn apply_deletions(&mut self) -> std::io::Result<()> {
        self.past_renames.clear();
        for holding in std::mem::take(&mut self.pending_deletions) {
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
            // In the simple limbo scheme, everything in limbo needs renaming
            // into place: what was staged, and what the removals moved out
            // of the way because its path changed.
            if let Some(limbo) = self.limbo_path_of(trans_id).map(Path::to_path_buf) {
                match mover.rename(&limbo, &full_path) {
                    Ok(()) => *rename_count += 1,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return Err(io_err(e)),
                }
            }
            if self.base().has_new_contents(trans_id) {
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
                mover.rollback().map_err(|rollback| {
                    Error::Tree(format!("rolling back after {e}: {rollback}"))
                })?;
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
                    .filter(|e| e.old_path.as_deref() != Some(""))
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
/// The process's file mode creation mask, read once.
#[cfg(unix)]
fn umask() -> u32 {
    static UMASK: std::sync::OnceLock<u32> = std::sync::OnceLock::new();
    *UMASK.get_or_init(|| {
        // The mask can only be read by setting it.
        let mask = nix::sys::stat::umask(nix::sys::stat::Mode::empty());
        nix::sys::stat::umask(mask);
        // mode_t is narrower than u32 on some platforms.
        #[allow(clippy::useless_conversion)]
        u32::from(mask.bits())
    })
}

/// Set or clear the executable bits of `path` as breezy's
/// `_set_executability` does: executable for the owner, and for the group
/// and others where they can read the file, as far as the umask allows.
#[cfg(unix)]
fn set_executable(path: &Path, executable: bool) -> std::io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let mode = std::fs::metadata(path)?.permissions().mode();
    let new_mode = if executable {
        let umask = umask();
        let mut new_mode = mode | (0o100 & !umask);
        if mode & 0o004 != 0 {
            new_mode |= 0o001 & !umask;
        }
        if mode & 0o040 != 0 {
            new_mode |= 0o010 & !umask;
        }
        new_mode
    } else {
        mode & !0o111
    };
    super::disk::chmod_if_possible(path, std::fs::Permissions::from_mode(new_mode))
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
        mover.rollback().unwrap();
        // The rename is undone.
        assert!(a.exists() && !b.exists());
    }

    /// What was moved is put back newest first, so a directory staged for
    /// deletion is back before what was moved out of it returns.
    #[test]
    fn file_mover_rolls_back_in_reverse_order() {
        let dir = tempfile::tempdir().unwrap();
        let parent = dir.path().join("parent");
        std::fs::create_dir(&parent).unwrap();
        std::fs::write(parent.join("child"), b"contents").unwrap();
        let mut mover = FileMover::default();
        mover
            .rename(&parent.join("child"), &dir.path().join("limbo-child"))
            .unwrap();
        mover
            .pre_delete(&parent, &dir.path().join("deleted-parent"))
            .unwrap();

        mover.rollback().unwrap();

        assert_eq!(std::fs::read(parent.join("child")).unwrap(), b"contents");
        assert!(!dir.path().join("limbo-child").exists());
        assert!(!dir.path().join("deleted-parent").exists());
    }

    #[test]
    fn file_mover_reports_a_failed_rollback() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a");
        let b = dir.path().join("b");
        std::fs::write(&a, b"a").unwrap();
        let mut mover = FileMover::default();
        mover.rename(&a, &b).unwrap();
        std::fs::remove_file(&b).unwrap();
        assert_eq!(
            mover.rollback().unwrap_err().kind(),
            std::io::ErrorKind::NotFound
        );
    }

    #[test]
    fn apply_renames_an_existing_file() {
        let (_d, mut tt, deletion) = apply_tt();
        let base_dir = tt.base().tree().abspath("");
        std::fs::write(base_dir.join("old.txt"), b"contents\n").unwrap();
        tt.base_mut()
            .tree
            .add("old.txt", b"old-id", crate::osutils::Kind::File);
        let root = tt.base().root().unwrap().to_string();
        let tid = tt.base_mut().trans_id_tree_path("old.txt");
        tt.base_mut().adjust_path("new.txt", &root, &tid).unwrap();

        let results = tt.apply(&deletion, false).unwrap();

        assert!(!base_dir.join("old.txt").exists());
        assert_eq!(
            std::fs::read(base_dir.join("new.txt")).unwrap(),
            b"contents\n"
        );
        assert!(tt.base().tree().is_versioned_path("new.txt"));
        assert!(!tt.base().tree().is_versioned_path("old.txt"));
        // Out of the way, then into place.
        assert_eq!(results.rename_count, 2);
        // Only new contents count as modified.
        assert_eq!(results.modified_paths, Vec::<PathBuf>::new());
    }

    /// A renamed directory takes what it holds along, and new entries land
    /// inside it under its new name.
    #[test]
    fn apply_renames_a_directory_with_its_contents() {
        use crate::osutils::Kind;

        let (_d, mut tt, deletion) = apply_tt();
        let base_dir = tt.base().tree().abspath("");
        std::fs::create_dir(base_dir.join("old")).unwrap();
        std::fs::write(base_dir.join("old/kept.txt"), b"kept\n").unwrap();
        tt.base_mut().tree.add("old", b"dir-id", Kind::Directory);
        tt.base_mut()
            .tree
            .add("old/kept.txt", b"kept-id", Kind::File);
        let root = tt.base().root().unwrap().to_string();
        let dir = tt.base_mut().trans_id_tree_path("old");
        tt.base_mut().adjust_path("new", &root, &dir).unwrap();
        let added = tt.base_mut().create_path("added.txt", &dir).unwrap();
        tt.create_file(b"added\n", &added, None).unwrap();

        tt.apply(&deletion, false).unwrap();

        assert!(!base_dir.join("old").exists());
        assert_eq!(
            std::fs::read(base_dir.join("new/kept.txt")).unwrap(),
            b"kept\n"
        );
        assert_eq!(
            std::fs::read(base_dir.join("new/added.txt")).unwrap(),
            b"added\n"
        );
    }

    /// Unversioning the root leaves its entry alone, as breezy leaves it.
    #[test]
    fn apply_keeps_the_entry_of_an_unversioned_root() {
        let (_d, mut tt, deletion) = apply_tt();
        let root = tt.base().root().unwrap().to_string();
        tt.base_mut().unversion_file(&root);
        tt.apply(&deletion, false).unwrap();
        assert!(tt.base().tree().is_versioned_path(""));
    }

    /// Making a file executable gives execute permission to its owner, and
    /// to the group and others where they can read it, within the umask.
    #[cfg(unix)]
    #[test]
    fn apply_sets_the_executable_bit_as_breezy_does() {
        use std::os::unix::fs::PermissionsExt;

        let mode_after = |mode: u32, executable: bool| -> u32 {
            let (_d, mut tt, deletion) = apply_tt();
            let path = tt.base().tree().abspath("tool");
            std::fs::write(&path, b"#!/bin/sh\n").unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
            tt.base_mut()
                .tree
                .add("tool", b"tool-id", crate::osutils::Kind::File);
            let tid = tt.base_mut().trans_id_tree_path("tool");
            tt.base_mut().set_executability(Some(executable), &tid);
            tt.apply(&deletion, false).unwrap();
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777
        };
        let allowed = !umask() & 0o111;
        // Readable by owner and group only: no execute bit for others.
        assert_eq!(mode_after(0o640, true), 0o640 | (0o110 & allowed));
        assert_eq!(mode_after(0o644, true), 0o644 | (0o111 & allowed));
        assert_eq!(mode_after(0o600, true), 0o600 | (0o100 & allowed));
        assert_eq!(mode_after(0o755, false), 0o644);
    }

    /// New contents for a file in the tree keep the mode it had.
    #[cfg(unix)]
    #[test]
    fn create_file_keeps_the_mode_of_the_file_it_replaces() {
        use std::os::unix::fs::PermissionsExt;

        let (_d, mut tt, deletion) = apply_tt();
        let path = tt.base().tree().abspath("tool");
        std::fs::write(&path, b"old\n").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o750)).unwrap();
        tt.base_mut()
            .tree
            .add("tool", b"tool-id", crate::osutils::Kind::File);
        let root = tt.base().root().unwrap().to_string();
        let tid = tt.base_mut().trans_id_tree_path("tool");
        tt.base_mut().delete_contents(&tid);
        tt.create_file(b"new\n", &tid, None).unwrap();
        // A new file can borrow the mode of another one.
        let copy = tt.base_mut().create_path("copy", &root).unwrap();
        tt.create_file_with_mode_of(b"copy\n", &copy, &tid, None)
            .unwrap();

        tt.apply(&deletion, false).unwrap();

        let mode = |p: &Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
        assert_eq!(std::fs::read(&path).unwrap(), b"new\n");
        assert_eq!(mode(&path), 0o750);
        assert_eq!(mode(&tt.base().tree().abspath("copy")), 0o750);
    }
}
