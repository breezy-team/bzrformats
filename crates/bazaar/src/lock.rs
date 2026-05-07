//! File locking with both fcntl OS-level locks and in-process
//! bookkeeping.
//!
//! Mirrors the Python [`bzrformats.lock`] module: fcntl's lockf is
//! per-process, so multiple file descriptors within the same process
//! can share a lock on the same file unbeknownst to fcntl. The
//! bookkeeping here lets callers detect lock contention between lock
//! objects living in the same process even when fcntl wouldn't catch
//! it.
//!
//! Behaviour mirrors the Python module exactly:
//!  * a *read* lock taken while the same process already holds a
//!    *write* lock is permitted (logged at debug level);
//!  * a *write* lock fails with [`LockError::Contention`] whenever any
//!    in-process reader OR another in-process writer holds the file.
//!
//! All public APIs operate on path strings and return owning lock
//! handles whose `Drop` releases the OS lock and bookkeeping slot —
//! call [`ReadLock::unlock`]/[`WriteLock::unlock`] to release earlier
//! and observe any error.

use nix::libc;
use std::collections::{HashMap, HashSet};
use std::fs::{File, OpenOptions};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

#[derive(Debug)]
pub enum LockError {
    /// Some other holder (in-process or OS-level) already has an
    /// incompatible lock on the file.
    Contention(PathBuf),
    /// The file could not be opened or operated on.
    Io(std::io::Error),
    /// Tried to release a lock that was already released.
    NotHeld(PathBuf),
}

impl std::fmt::Display for LockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LockError::Contention(p) => write!(f, "lock contention on {:?}", p),
            LockError::Io(e) => write!(f, "{}", e),
            LockError::NotHeld(p) => write!(f, "lock not held on {:?}", p),
        }
    }
}

impl std::error::Error for LockError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            LockError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for LockError {
    fn from(e: std::io::Error) -> Self {
        LockError::Io(e)
    }
}

/// Module-global bookkeeping protected by a single mutex. Mirrors
/// the Python `_lock_state_lock` dict pair.
struct LockState {
    /// Per-path read-lock counts.
    read_counts: HashMap<PathBuf, usize>,
    /// Paths currently held by an in-process write lock.
    write_locks: HashSet<PathBuf>,
}

fn lock_state() -> &'static Mutex<LockState> {
    static STATE: OnceLock<Mutex<LockState>> = OnceLock::new();
    STATE.get_or_init(|| {
        Mutex::new(LockState {
            read_counts: HashMap::new(),
            write_locks: HashSet::new(),
        })
    })
}

/// Snapshot of the current in-process bookkeeping. Useful for tests.
pub fn snapshot() -> (HashMap<PathBuf, usize>, HashSet<PathBuf>) {
    let g = lock_state().lock().unwrap();
    (g.read_counts.clone(), g.write_locks.clone())
}

/// Reset the in-process bookkeeping. Tests use this between cases to
/// stop one test's failure from poisoning the next.
pub fn reset_for_tests() {
    let mut g = lock_state().lock().unwrap();
    g.read_counts.clear();
    g.write_locks.clear();
}

/// Reserve a read-lock slot for `path`. Returns the new read-count.
/// A debug log is emitted if the same process already holds a write
/// lock — the read is still permitted, matching Python.
fn acquire_read_slot(path: &Path) -> usize {
    let mut g = lock_state().lock().unwrap();
    if g.write_locks.contains(path) {
        log::debug!("Read lock taken w/ an open write lock on: {:?}", path);
    }
    let entry = g.read_counts.entry(path.to_path_buf()).or_insert(0);
    *entry += 1;
    *entry
}

fn release_read_slot(path: &Path) {
    let mut g = lock_state().lock().unwrap();
    let count = g.read_counts.get(path).copied().unwrap_or(0);
    if count <= 1 {
        g.read_counts.remove(path);
    } else {
        g.read_counts.insert(path.to_path_buf(), count - 1);
    }
}

fn acquire_write_slot(path: &Path) -> Result<(), LockError> {
    let mut g = lock_state().lock().unwrap();
    if g.write_locks.contains(path) || g.read_counts.get(path).copied().unwrap_or(0) > 0 {
        return Err(LockError::Contention(path.to_path_buf()));
    }
    g.write_locks.insert(path.to_path_buf());
    Ok(())
}

fn release_write_slot(path: &Path) {
    let mut g = lock_state().lock().unwrap();
    g.write_locks.remove(path);
}

/// fcntl-style lock operation. Mirrors Python's `fcntl.lockf` which
/// uses POSIX-advisory locks: those are per-process, so the same
/// process can take both a read and a write lock without OS-level
/// contention — matching the historical bzr behaviour.
#[derive(Copy, Clone)]
enum FcntlOp {
    LockShared,
    LockExclusive,
    Unlock,
}

/// Apply the requested fcntl operation to `file`, using POSIX
/// `fcntl(F_SETLK, struct flock)` so we get the same per-process
/// semantics as Python's `fcntl.lockf`. Maps `EWOULDBLOCK`/`EAGAIN`
/// to `LockError::Contention`.
fn fcntl_lockf(file: &File, op: FcntlOp, path: &Path) -> Result<(), LockError> {
    use nix::errno::Errno;
    let mut fl: libc::flock = unsafe { std::mem::zeroed() };
    fl.l_type = match op {
        FcntlOp::LockShared => libc::F_RDLCK as i16,
        FcntlOp::LockExclusive => libc::F_WRLCK as i16,
        FcntlOp::Unlock => libc::F_UNLCK as i16,
    };
    fl.l_whence = libc::SEEK_SET as i16;
    fl.l_start = 0;
    fl.l_len = 0;
    let res = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_SETLK, &fl) };
    if res == 0 {
        return Ok(());
    }
    let errno = Errno::last();
    if matches!(errno, Errno::EWOULDBLOCK | Errno::EAGAIN | Errno::EACCES) {
        return Err(LockError::Contention(path.to_path_buf()));
    }
    Err(LockError::Io(std::io::Error::from_raw_os_error(
        errno as i32,
    )))
}

/// OS-level shared (read) lock on a file. The file is accessible
/// through [`ReadLock::file`] / [`ReadLock::file_mut`].
pub struct ReadLock {
    path: PathBuf,
    /// `None` once the lock has been released.
    file: Option<File>,
}

impl ReadLock {
    /// Acquire a shared lock on `path`. Returns
    /// [`LockError::Contention`] if another in-process writer would
    /// upgrade-conflict, or [`LockError::Io`] for any open/lock error.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, LockError> {
        let path = path.as_ref().to_path_buf();
        acquire_read_slot(&path);
        let file = match File::open(&path) {
            Ok(f) => f,
            Err(e) => {
                release_read_slot(&path);
                return Err(LockError::Io(e));
            }
        };
        if let Err(e) = fcntl_lockf(&file, FcntlOp::LockShared, &path) {
            // file goes out of scope and closes
            release_read_slot(&path);
            return Err(e);
        }
        Ok(Self {
            path,
            file: Some(file),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn file(&self) -> Option<&File> {
        self.file.as_ref()
    }

    pub fn file_mut(&mut self) -> Option<&mut File> {
        self.file.as_mut()
    }

    /// Release the lock. Errors if already released.
    pub fn unlock(&mut self) -> Result<(), LockError> {
        let file = self
            .file
            .take()
            .ok_or_else(|| LockError::NotHeld(self.path.clone()))?;
        let _ = fcntl_lockf(&file, FcntlOp::Unlock, &self.path);
        drop(file);
        release_read_slot(&self.path);
        Ok(())
    }

    /// Try to upgrade to a write lock. On success returns
    /// `Ok(Some(WriteLock))`; on contention returns `Ok(None)` and
    /// `self` retains its read lock. On a hard failure returns the
    /// error.
    ///
    /// Mirrors Python's `temporary_write_lock` two-tuple result. The
    /// upgrade is refused (returns `Ok(None)` without dropping our
    /// read lock) when more than one in-process reader is live —
    /// fcntl's per-process semantics would otherwise spuriously
    /// succeed.
    pub fn temporary_write_lock(mut self) -> Result<TemporaryWriteLockResult, LockError> {
        {
            let g = lock_state().lock().unwrap();
            if g.read_counts.get(&self.path).copied().unwrap_or(0) > 1 {
                return Ok(TemporaryWriteLockResult::Failed(self));
            }
        }
        // Drop our read lock before attempting the upgrade.
        let file = self
            .file
            .take()
            .ok_or_else(|| LockError::NotHeld(self.path.clone()))?;
        let _ = fcntl_lockf(&file, FcntlOp::Unlock, &self.path);
        drop(file);
        release_read_slot(&self.path);
        match WriteLock::new(&self.path) {
            Ok(wl) => Ok(TemporaryWriteLockResult::Succeeded(wl)),
            Err(e) => {
                // Re-acquire the read lock so callers' invariants still hold.
                acquire_read_slot(&self.path);
                let new_file = match File::open(&self.path) {
                    Ok(f) => f,
                    Err(open_err) => {
                        release_read_slot(&self.path);
                        return Err(LockError::Io(open_err));
                    }
                };
                if let Err(lock_err) = fcntl_lockf(&new_file, FcntlOp::LockShared, &self.path) {
                    release_read_slot(&self.path);
                    return Err(lock_err);
                }
                self.file = Some(new_file);
                let _ = e;
                Ok(TemporaryWriteLockResult::Failed(self))
            }
        }
    }
}

impl Drop for ReadLock {
    fn drop(&mut self) {
        if self.file.is_some() {
            let _ = self.unlock();
        }
    }
}

/// Result of [`ReadLock::temporary_write_lock`].
pub enum TemporaryWriteLockResult {
    /// Upgrade succeeded; the write lock owns the file now.
    Succeeded(WriteLock),
    /// Upgrade failed; the original read lock is still held.
    Failed(ReadLock),
}

/// OS-level exclusive (write) lock on a file. Creates the file if it
/// does not exist.
pub struct WriteLock {
    path: PathBuf,
    file: Option<File>,
}

impl WriteLock {
    /// Acquire an exclusive lock on `path`. Returns
    /// [`LockError::Contention`] if any in-process holder is already
    /// present.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, LockError> {
        let path = path.as_ref().to_path_buf();
        acquire_write_slot(&path)?;
        let file = match OpenOptions::new().read(true).write(true).open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => match OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&path)
            {
                Ok(f) => f,
                Err(e2) => {
                    release_write_slot(&path);
                    return Err(LockError::Io(e2));
                }
            },
            Err(e) => {
                release_write_slot(&path);
                return Err(LockError::Io(e));
            }
        };
        if let Err(e) = fcntl_lockf(&file, FcntlOp::LockExclusive, &path) {
            release_write_slot(&path);
            return Err(e);
        }
        Ok(Self {
            path,
            file: Some(file),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn file(&self) -> Option<&File> {
        self.file.as_ref()
    }

    pub fn file_mut(&mut self) -> Option<&mut File> {
        self.file.as_mut()
    }

    pub fn unlock(&mut self) -> Result<(), LockError> {
        let file = self
            .file
            .take()
            .ok_or_else(|| LockError::NotHeld(self.path.clone()))?;
        let _ = fcntl_lockf(&file, FcntlOp::Unlock, &self.path);
        drop(file);
        release_write_slot(&self.path);
        Ok(())
    }

    /// Downgrade to a read lock by releasing the write lock and
    /// acquiring a fresh read lock.
    pub fn restore_read_lock(mut self) -> Result<ReadLock, LockError> {
        let file = self
            .file
            .take()
            .ok_or_else(|| LockError::NotHeld(self.path.clone()))?;
        let _ = fcntl_lockf(&file, FcntlOp::Unlock, &self.path);
        drop(file);
        release_write_slot(&self.path);
        ReadLock::new(&self.path)
    }
}

impl Drop for WriteLock {
    fn drop(&mut self) {
        if self.file.is_some() {
            let _ = self.unlock();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    /// Tests share the global lock-bookkeeping state, so they must
    /// run serially. Each test acquires this mutex (recovering from
    /// poison) and resets state on entry.
    static TEST_LOCK: Mutex<()> = Mutex::new(());

    fn scoped_state() -> std::sync::MutexGuard<'static, ()> {
        let guard = match TEST_LOCK.lock() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        reset_for_tests();
        guard
    }

    #[test]
    fn two_read_locks_share() {
        let _guard = scoped_state();
        let f = NamedTempFile::new().unwrap();
        let path = f.path().to_path_buf();
        let mut a = ReadLock::new(&path).unwrap();
        let mut b = ReadLock::new(&path).unwrap();
        let (rc, _) = snapshot();
        assert_eq!(rc.get(&path).copied(), Some(2));
        a.unlock().unwrap();
        let (rc, _) = snapshot();
        assert_eq!(rc.get(&path).copied(), Some(1));
        b.unlock().unwrap();
        let (rc, _) = snapshot();
        assert!(!rc.contains_key(&path));
    }

    #[test]
    fn write_blocks_when_reader_open() {
        let _guard = scoped_state();
        let f = NamedTempFile::new().unwrap();
        let path = f.path().to_path_buf();
        let mut rl = ReadLock::new(&path).unwrap();
        match WriteLock::new(&path) {
            Err(LockError::Contention(_)) => {}
            other => panic!("expected Contention, got {:?}", other.is_ok()),
        }
        let (rc, wls) = snapshot();
        assert_eq!(rc.get(&path).copied(), Some(1));
        assert!(!wls.contains(&path));
        rl.unlock().unwrap();
    }

    #[test]
    fn read_after_write_logs_but_succeeds() {
        let _guard = scoped_state();
        let f = NamedTempFile::new().unwrap();
        let path = f.path().to_path_buf();
        let mut wl = WriteLock::new(&path).unwrap();
        let mut rl = ReadLock::new(&path).unwrap();
        let (rc, wls) = snapshot();
        assert_eq!(rc.get(&path).copied(), Some(1));
        assert!(wls.contains(&path));
        rl.unlock().unwrap();
        let (rc, _) = snapshot();
        assert!(!rc.contains_key(&path));
        wl.unlock().unwrap();
        let (_, wls) = snapshot();
        assert!(!wls.contains(&path));
    }

    #[test]
    fn temporary_write_lock_with_other_reader_keeps_read() {
        let _guard = scoped_state();
        let f = NamedTempFile::new().unwrap();
        let path = f.path().to_path_buf();
        let a = ReadLock::new(&path).unwrap();
        let mut b = ReadLock::new(&path).unwrap();
        let result = a.temporary_write_lock().unwrap();
        match result {
            TemporaryWriteLockResult::Failed(mut a_back) => {
                let (rc, _) = snapshot();
                assert_eq!(rc.get(&path).copied(), Some(2));
                a_back.unlock().unwrap();
            }
            _ => panic!("expected Failed"),
        }
        b.unlock().unwrap();
    }

    #[test]
    fn temporary_write_lock_solo_reader_succeeds() {
        let _guard = scoped_state();
        let f = NamedTempFile::new().unwrap();
        let path = f.path().to_path_buf();
        let a = ReadLock::new(&path).unwrap();
        let result = a.temporary_write_lock().unwrap();
        match result {
            TemporaryWriteLockResult::Succeeded(mut wl) => {
                let (rc, wls) = snapshot();
                assert!(!rc.contains_key(&path));
                assert!(wls.contains(&path));
                wl.unlock().unwrap();
            }
            _ => panic!("expected Succeeded"),
        }
        let (_, wls) = snapshot();
        assert!(!wls.contains(&path));
    }

    #[test]
    fn restore_read_lock_keeps_tallies_consistent() {
        let _guard = scoped_state();
        let f = NamedTempFile::new().unwrap();
        let path = f.path().to_path_buf();
        let wl = WriteLock::new(&path).unwrap();
        let mut rl = wl.restore_read_lock().unwrap();
        let (rc, wls) = snapshot();
        assert!(!wls.contains(&path));
        assert_eq!(rc.get(&path).copied(), Some(1));
        rl.unlock().unwrap();
        let (rc, _) = snapshot();
        assert!(!rc.contains_key(&path));
    }

    #[test]
    fn write_lock_creates_missing_file() {
        let _guard = scoped_state();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("new-file");
        assert!(!path.exists());
        {
            let mut wl = WriteLock::new(&path).unwrap();
            wl.file_mut().unwrap().write_all(b"hello").unwrap();
        }
        assert!(path.exists());
    }

    #[test]
    fn read_lock_failure_does_not_leak() {
        let _guard = scoped_state();
        let bogus = std::path::PathBuf::from("/no/such/path/for-bzrformats-tests");
        match ReadLock::new(&bogus) {
            Err(LockError::Io(_)) => {}
            other => panic!("expected Io error, got {}", other.is_ok()),
        }
        let (rc, _) = snapshot();
        assert!(!rc.contains_key(&bogus));
    }
}
