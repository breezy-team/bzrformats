//! The lock of a working tree: the lock directory `.bzr/checkout/lock` for
//! writing, and for a dirstate tree an OS lock on the dirstate file while
//! the tree is locked. Locking the tree's branch is left to the caller.

use crate::dirstate::Transport as _;
use crate::lockable_files::{LockMode, LockWaiter, LockableFiles, LockableFilesError, WriteLocked};
use crate::transport::SharedTransport;
use std::sync::{Arc, Mutex};

/// The lock directory of a metadir working tree.
pub const CHECKOUT_LOCK: &str = ".bzr/checkout/lock";

/// The lock of a working tree. Clones share the lock.
#[derive(Clone)]
pub struct TreeLock {
    files: LockableFiles,
    /// The dirstate file, locked through the dirstate's own transport as
    /// `DirState.lock_read` and `lock_write` lock it; `None` for trees
    /// without a dirstate or not on the local filesystem.
    dirstate: Option<Arc<Mutex<crate::dirstate::FileTransport>>>,
}

impl TreeLock {
    /// The lock of the tree rooted at `transport`, with its lock directory
    /// at `lock_dir` (`None` for none); `dirstate` is the tree-relative
    /// path of its dirstate, for dirstate trees.
    pub fn new(transport: SharedTransport, lock_dir: Option<&str>, dirstate: Option<&str>) -> Self {
        let dirstate = dirstate
            .and_then(|path| transport.local_path(path))
            .map(|path| Arc::new(Mutex::new(crate::dirstate::FileTransport::new(path))));
        TreeLock {
            files: LockableFiles::new(transport, lock_dir),
            dirstate,
        }
    }

    /// The tree's lockable files: the lock count, mode and lock directory.
    pub fn files(&self) -> &LockableFiles {
        &self.files
    }

    /// Lock the tree for reading.
    pub fn lock_read(&self) -> Result<(), LockableFilesError> {
        self.files.lock_read();
        self.lock_dirstate(false)
            .inspect_err(|_| self.release_files())
    }

    /// Lock the tree itself for writing, as `_lock_self_write` does.
    pub fn lock_write(
        &self,
        waiter: &mut dyn LockWaiter,
    ) -> Result<WriteLocked, LockableFilesError> {
        let locked = self.files.lock_write(None, waiter)?;
        self.lock_dirstate(true)
            .inspect_err(|_| self.release_files())?;
        Ok(locked)
    }

    /// Release one lock, and the dirstate's OS lock with the last. Returns
    /// the token of the lock directory if the last write lock released it.
    pub fn unlock(&self) -> Result<Option<crate::lockable_files::LockToken>, LockableFilesError> {
        let mut result = Ok(());
        if let (1, Some(dirstate)) = (self.files.lock_count(), &self.dirstate) {
            let mut dirstate = dirstate.lock().unwrap();
            if dirstate.lock_state().is_some() {
                result = dirstate.unlock().map_err(LockableFilesError::Dirstate);
            }
        }
        let released = self.files.unlock()?;
        result.map(|()| released)
    }

    /// Take the dirstate's OS lock, unless one is held already.
    fn lock_dirstate(&self, write: bool) -> Result<(), LockableFilesError> {
        let Some(dirstate) = &self.dirstate else {
            return Ok(());
        };
        let mut dirstate = dirstate.lock().unwrap();
        if dirstate.lock_state().is_some() {
            return Ok(());
        }
        let locked = if write {
            dirstate.lock_write()
        } else {
            dirstate.lock_read()
        };
        locked.map_err(|e| match e {
            crate::dirstate::TransportError::LockContention(_) => LockableFilesError::Contention,
            other => LockableFilesError::Dirstate(other),
        })
    }

    /// Release the lock taken on the files when the dirstate cannot be
    /// locked; the failure to lock is what is reported.
    fn release_files(&self) {
        if let Err(e) = self.files.unlock() {
            log::warn!("failed to release tree lock: {e}");
        }
    }

    /// Run `f` on the dirstate file while the tree holds its write lock, so
    /// the dirstate is written under the lock the tree took; `None` if the
    /// tree does not hold it.
    pub fn with_dirstate_write<R>(
        &self,
        f: impl FnOnce(&mut crate::dirstate::FileTransport) -> R,
    ) -> Option<R> {
        let mut dirstate = self.dirstate.as_ref()?.lock().unwrap();
        (dirstate.lock_state() == Some(crate::dirstate::LockState::Write)).then(|| f(&mut dirstate))
    }

    /// The mode the tree is locked in, if it is.
    pub fn lock_mode(&self) -> Option<LockMode> {
        self.files.lock_mode()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lockable_files::NoWait;

    fn tree_lock(dir: &tempfile::TempDir) -> TreeLock {
        std::fs::create_dir_all(dir.path().join(".bzr/checkout")).unwrap();
        std::fs::write(dir.path().join(".bzr/checkout/dirstate"), b"").unwrap();
        let transport: SharedTransport =
            Arc::new(crate::transport::LocalTransport::new(dir.path()));
        let lock = TreeLock::new(
            transport,
            Some(CHECKOUT_LOCK),
            Some(".bzr/checkout/dirstate"),
        );
        lock.files().create_lock().unwrap();
        lock
    }

    #[test]
    fn write_lock_takes_lock_dir_and_dirstate() {
        let dir = tempfile::tempdir().unwrap();
        let lock = tree_lock(&dir);
        let other = tree_lock(&dir);
        lock.lock_write(&mut NoWait).unwrap();
        assert!(dir.path().join(".bzr/checkout/lock/held").exists());
        assert!(matches!(
            other.lock_write(&mut NoWait),
            Err(LockableFilesError::Contention)
        ));
        assert!(!other.files().is_locked());
        assert!(lock.unlock().unwrap().is_some());
        assert!(!dir.path().join(".bzr/checkout/lock/held").exists());
        other.lock_write(&mut NoWait).unwrap();
        other.unlock().unwrap();
    }

    #[test]
    fn read_lock_shares_the_dirstate() {
        let dir = tempfile::tempdir().unwrap();
        let first = tree_lock(&dir);
        let second = tree_lock(&dir);
        first.lock_read().unwrap();
        second.lock_read().unwrap();
        assert_eq!(Some(LockMode::Read), first.lock_mode());
        assert!(!first.files().get_physical_lock_status().unwrap());
        assert_eq!(None, first.unlock().unwrap());
        assert_eq!(None, second.unlock().unwrap());
    }
}
