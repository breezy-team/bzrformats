//! Disk staging for a tree transform (breezy's `DiskTreeTransform`).
//!
//! New file/directory/symlink contents are written into a temporary *limbo*
//! directory as they are scheduled, then renamed into their final places when
//! the transform is applied. Staging on disk (rather than in memory) keeps the
//! transform's memory use bounded and lets apply be a sequence of renames.
//!
//! This layer uses the simple limbo-naming scheme: every staged trans-id maps
//! to `<limbo>/<trans_id>`, and apply renames each into place. breezy also has
//! a direct-path optimisation that stages children inside their parent's limbo
//! directory to avoid some renames; that is a performance refinement, not a
//! correctness requirement, and is not implemented here.

use super::{ContentKind, Error, TransformTree, TreeTransformBase};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

/// A tree transform that stages new contents in a limbo directory on disk.
pub struct DiskTreeTransform<T: TransformTree> {
    base: TreeTransformBase<T>,
    /// The limbo directory (absolute path) where new contents are staged.
    limbo_dir: PathBuf,
    /// trans-id -> its limbo file path.
    limbo_files: HashMap<String, PathBuf>,
    /// trans-id -> the observed `(sha1, size)` of freshly written file content.
    observed_sha1s: HashMap<String, (Vec<u8>, u64)>,
    /// Whether the limbo filesystem supports symlinks.
    create_symlinks: bool,
    /// Whether apply must still consult the mode/mtime (set once created).
    creation_done: HashSet<String>,
    /// The modification time every file staged by this transform is given,
    /// taken when the first one is created.
    creation_mtime: Option<std::time::SystemTime>,
}

impl<T: TransformTree> DiskTreeTransform<T> {
    /// Create a disk transform over `base`, staging into `limbo_dir` (which
    /// must already exist and be empty). `create_symlinks` says whether the
    /// limbo filesystem can hold symlinks.
    pub fn new(base: TreeTransformBase<T>, limbo_dir: PathBuf, create_symlinks: bool) -> Self {
        DiskTreeTransform {
            base,
            limbo_dir,
            limbo_files: HashMap::new(),
            observed_sha1s: HashMap::new(),
            create_symlinks,
            creation_done: HashSet::new(),
            creation_mtime: None,
        }
    }

    /// The underlying bookkeeping.
    pub fn base(&self) -> &TreeTransformBase<T> {
        &self.base
    }

    /// The underlying bookkeeping, mutably.
    pub fn base_mut(&mut self) -> &mut TreeTransformBase<T> {
        &mut self.base
    }

    /// The limbo path of `trans_id`, assigning `<limbo>/<trans_id>` on first
    /// use.
    pub fn limbo_name(&mut self, trans_id: &str) -> PathBuf {
        if let Some(path) = self.limbo_files.get(trans_id) {
            return path.clone();
        }
        let path = self.limbo_dir.join(trans_id);
        self.limbo_files.insert(trans_id.to_string(), path.clone());
        path
    }

    /// The already-assigned limbo path of `trans_id`, if any.
    pub fn limbo_path_of(&self, trans_id: &str) -> Option<&Path> {
        self.limbo_files.get(trans_id).map(PathBuf::as_path)
    }

    /// Check that no contents are staged for `trans_id` yet.
    fn check_no_contents(&self, trans_id: &str) -> Result<(), Error> {
        if self.base.new_contents_map().contains_key(trans_id) {
            return Err(Error::DuplicateKey(format!("{trans_id:?}")));
        }
        Ok(())
    }

    /// Give the file staged at `path` the modification time shared by all
    /// files this transform creates (breezy's `_set_mtime`).
    fn set_mtime(&mut self, path: &Path) -> Result<(), Error> {
        let mtime = *self
            .creation_mtime
            .get_or_insert_with(std::time::SystemTime::now);
        let times = std::fs::FileTimes::new()
            .set_accessed(mtime)
            .set_modified(mtime);
        std::fs::File::options()
            .write(true)
            .open(path)
            .and_then(|file| file.set_times(times))
            .map_err(io_err)
    }

    /// Stage a new file with `contents` for `trans_id`. `sha1`, when known,
    /// is recorded so apply can seed the tree's stat cache.
    pub fn create_file(
        &mut self,
        contents: &[u8],
        trans_id: &str,
        sha1: Option<Vec<u8>>,
    ) -> Result<(), Error> {
        self.check_no_contents(trans_id)?;
        let name = self.limbo_name(trans_id);
        std::fs::write(&name, contents).map_err(io_err)?;
        self.base.set_new_contents(trans_id, ContentKind::File);
        self.creation_done.insert(trans_id.to_string());
        self.set_mtime(&name)?;
        if let Some(sha1) = sha1 {
            self.observed_sha1s
                .insert(trans_id.to_string(), (sha1, contents.len() as u64));
        }
        Ok(())
    }

    /// Stage a new directory for `trans_id`.
    pub fn create_directory(&mut self, trans_id: &str) -> Result<(), Error> {
        self.check_no_contents(trans_id)?;
        let name = self.limbo_name(trans_id);
        std::fs::create_dir(&name).map_err(io_err)?;
        self.base.set_new_contents(trans_id, ContentKind::Directory);
        self.creation_done.insert(trans_id.to_string());
        Ok(())
    }

    /// Stage a new symlink to `target` for `trans_id`. On a filesystem without
    /// symlink support the link is not created, but the content is still
    /// recorded (matching breezy) so conflict detection is consistent.
    pub fn create_symlink(&mut self, target: &str, trans_id: &str) -> Result<(), Error> {
        self.check_no_contents(trans_id)?;
        let name = self.limbo_name(trans_id);
        if self.create_symlinks {
            symlink(target, &name).map_err(io_err)?;
        }
        self.base.set_new_contents(trans_id, ContentKind::Symlink);
        self.creation_done.insert(trans_id.to_string());
        Ok(())
    }

    /// Cancel staged content creation for `trans_id`, removing its limbo file.
    pub fn cancel_creation(&mut self, trans_id: &str) -> Result<(), Error> {
        self.base.cancel_contents(trans_id);
        self.observed_sha1s.remove(trans_id);
        if let Some(path) = self.limbo_files.remove(trans_id) {
            delete_any(&path).map_err(io_err)?;
        }
        self.creation_done.remove(trans_id);
        Ok(())
    }

    /// The recorded `(sha1, size)` observations for freshly written files.
    pub fn observed_sha1s(&self) -> &HashMap<String, (Vec<u8>, u64)> {
        &self.observed_sha1s
    }

    /// Remove the limbo directory and everything under it. Call after apply,
    /// or to abandon an unapplied transform.
    pub fn finalize(&mut self) -> Result<(), Error> {
        if self.limbo_dir.exists() {
            std::fs::remove_dir_all(&self.limbo_dir).map_err(io_err)?;
        }
        self.limbo_files.clear();
        Ok(())
    }
}

/// Map an I/O error into a transform error.
fn io_err(e: std::io::Error) -> Error {
    Error::Tree(e.to_string())
}

/// Delete a file, directory or symlink at `path`, ignoring absence.
fn delete_any(path: &Path) -> std::io::Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(m) if m.is_dir() => std::fs::remove_dir_all(path),
        Ok(_) => std::fs::remove_file(path),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

#[cfg(unix)]
fn symlink(target: &str, path: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(target, path)
}

#[cfg(not(unix))]
fn symlink(_target: &str, _path: &Path) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "symlinks unsupported",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::osutils::Kind;
    use crate::transform::tests_support::FakeTree;
    use crate::FileId;

    fn disk_tt() -> (tempfile::TempDir, DiskTreeTransform<FakeTree>) {
        let dir = tempfile::tempdir().unwrap();
        let limbo = dir.path().join("limbo");
        std::fs::create_dir(&limbo).unwrap();
        let base = TreeTransformBase::new(FakeTree::new(), true);
        (dir, DiskTreeTransform::new(base, limbo, cfg!(unix)))
    }

    #[test]
    fn create_file_stages_content_in_limbo() {
        let (_d, mut tt) = disk_tt();
        let root = tt.base().root().unwrap().to_string();
        let tid = tt.base_mut().create_path("f.txt", &root).unwrap();
        tt.create_file(b"hello\n", &tid, None).unwrap();
        // The content is on disk in limbo, and the base records a file.
        let path = tt.limbo_name(&tid);
        assert_eq!(std::fs::read(&path).unwrap(), b"hello\n");
        assert_eq!(tt.base().final_kind(&tid), Some(Kind::File));
    }

    #[test]
    fn create_directory_and_cancel() {
        let (_d, mut tt) = disk_tt();
        let root = tt.base().root().unwrap().to_string();
        let tid = tt.base_mut().create_path("sub", &root).unwrap();
        tt.create_directory(&tid).unwrap();
        assert!(tt.limbo_name(&tid).is_dir());
        tt.cancel_creation(&tid).unwrap();
        // The limbo entry is gone and the base no longer records contents.
        assert_eq!(tt.base().final_kind(&tid), None);
    }

    #[test]
    fn create_file_records_observed_sha1() {
        let (_d, mut tt) = disk_tt();
        let root = tt.base().root().unwrap().to_string();
        let tid = tt.base_mut().create_path("f.txt", &root).unwrap();
        tt.base_mut()
            .version_file(&tid, FileId::from(b"f-id".to_vec()))
            .unwrap();
        tt.create_file(b"data", &tid, Some(b"deadbeef".to_vec()))
            .unwrap();
        let (sha1, size) = &tt.observed_sha1s()[&tid];
        assert_eq!(sha1, b"deadbeef");
        assert_eq!(*size, 4);
    }

    #[cfg(unix)]
    #[test]
    fn create_symlink_stages_a_link() {
        let (_d, mut tt) = disk_tt();
        let root = tt.base().root().unwrap().to_string();
        let tid = tt.base_mut().create_path("link", &root).unwrap();
        tt.create_symlink("target", &tid).unwrap();
        let path = tt.limbo_name(&tid);
        assert_eq!(
            std::fs::read_link(&path).unwrap().to_str().unwrap(),
            "target"
        );
        assert_eq!(tt.base().final_kind(&tid), Some(Kind::Symlink));
    }

    /// Contents can be staged for a trans-id only once.
    #[test]
    fn staging_contents_twice_is_refused() {
        let (_d, mut tt) = disk_tt();
        let root = tt.base().root().unwrap().to_string();
        let tid = tt.base_mut().create_path("f.txt", &root).unwrap();
        tt.create_file(b"first\n", &tid, None).unwrap();
        for result in [
            tt.create_file(b"second\n", &tid, None),
            tt.create_directory(&tid),
            tt.create_symlink("target", &tid),
        ] {
            match result {
                Err(Error::DuplicateKey(key)) => assert_eq!(key, format!("{tid:?}")),
                other => panic!("expected a duplicate key, got {:?}", other),
            }
        }
        assert_eq!(std::fs::read(tt.limbo_name(&tid)).unwrap(), b"first\n");
    }

    /// Every file a transform creates gets the same modification time.
    #[test]
    fn created_files_share_an_mtime() {
        let (_d, mut tt) = disk_tt();
        let root = tt.base().root().unwrap().to_string();
        let first = tt.base_mut().create_path("first", &root).unwrap();
        tt.create_file(b"1", &first, None).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(20));
        let second = tt.base_mut().create_path("second", &root).unwrap();
        tt.create_file(b"2", &second, None).unwrap();
        let mtime = |tt: &mut DiskTreeTransform<FakeTree>, tid: &str| {
            std::fs::metadata(tt.limbo_name(tid))
                .unwrap()
                .modified()
                .unwrap()
        };
        assert_eq!(mtime(&mut tt, &first), mtime(&mut tt, &second));
    }

    #[test]
    fn finalize_removes_limbo() {
        let (_d, mut tt) = disk_tt();
        let root = tt.base().root().unwrap().to_string();
        let tid = tt.base_mut().create_path("f", &root).unwrap();
        tt.create_file(b"x", &tid, None).unwrap();
        let limbo = tt.limbo_dir.clone();
        tt.finalize().unwrap();
        assert!(!limbo.exists());
    }
}
