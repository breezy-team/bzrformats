//! The lock of a branch, repository or working tree: a count of read or
//! write locks, and for objects that take one, a physical [`LockDir`] held
//! while they are write-locked.
//!
//! Read locks are logical; they take no lock directory. Waiting for a
//! lock held by someone else is left to the caller's [`LockWaiter`], since
//! how long to wait and what to tell the user is UI policy.

use crate::lockdir::{Lock as _, LockDir, LockError, LockHeldInfo};
use crate::transport::SharedTransport;
use std::sync::{Arc, Mutex};

/// The mode an object is locked in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    /// Locked for reading.
    Read,
    /// Locked for writing.
    Write,
}

impl LockMode {
    /// The mode's name: `r` or `w`.
    pub fn as_str(self) -> &'static str {
        match self {
            LockMode::Read => "r",
            LockMode::Write => "w",
        }
    }
}

/// Errors from locking an object.
#[derive(Debug)]
pub enum LockableFilesError {
    /// A write lock was requested while the object is read-locked.
    ReadOnly,
    /// An unlock was attempted while the object is not locked.
    NotHeld,
    /// The physical lock is held by someone else and the waiter gave up.
    Contention,
    /// The physical lock failed otherwise.
    Lock(LockError),
    /// Locking the dirstate file failed otherwise.
    Dirstate(crate::dirstate::TransportError),
}

impl std::fmt::Display for LockableFilesError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LockableFilesError::ReadOnly => f.write_str("object is read-locked"),
            LockableFilesError::NotHeld => f.write_str("lock not held"),
            LockableFilesError::Contention => f.write_str("lock is held by someone else"),
            LockableFilesError::Lock(e) => write!(f, "{e}"),
            LockableFilesError::Dirstate(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for LockableFilesError {}

impl From<LockError> for LockableFilesError {
    fn from(e: LockError) -> Self {
        LockableFilesError::Lock(e)
    }
}

/// Decides what to do when the physical lock is held by someone else.
pub trait LockWaiter {
    /// The lock is held by `holder` (`None` if it was released meanwhile);
    /// return whether to try again. Waiting between attempts is up to the
    /// waiter.
    fn contended(&mut self, holder: Option<&LockHeldInfo>) -> bool;
}

/// Gives up as soon as the lock is found to be held.
pub struct NoWait;

impl LockWaiter for NoWait {
    fn contended(&mut self, _holder: Option<&LockHeldInfo>) -> bool {
        false
    }
}

#[derive(Debug, Default)]
struct State {
    mode: Option<LockMode>,
    count: usize,
    /// The nonce of the physical lock while it is held.
    nonce: Option<String>,
    /// The physical lock is not released by the last unlock: it was taken
    /// over with a token, or is to be left in place.
    locked_via_token: bool,
}

/// The outcome of [`LockableFiles::lock_write`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteLocked {
    /// The token of the physical lock, if the object takes one.
    pub token: Option<String>,
    /// Whether the physical lock was taken by this call, rather than being
    /// held already or taken over with a token.
    pub acquired: bool,
}

/// A lock on an object's control files. Clones share the lock.
#[derive(Clone)]
pub struct LockableFiles {
    transport: SharedTransport,
    /// The lock directory, relative to `transport`; `None` for objects that
    /// take no physical lock, such as pack repositories.
    lock_path: Option<String>,
    state: Arc<Mutex<State>>,
}

impl LockableFiles {
    /// The lock of the object whose control files `transport` reaches, with
    /// its lock directory at `lock_path` (`None` for no physical lock).
    pub fn new(transport: SharedTransport, lock_path: Option<&str>) -> Self {
        LockableFiles {
            transport,
            lock_path: lock_path.map(str::to_string),
            state: Arc::new(Mutex::new(State::default())),
        }
    }

    /// The transport the lock lives on.
    pub fn transport(&self) -> &SharedTransport {
        &self.transport
    }

    /// The lock directory, relative to the transport, if there is one.
    pub fn lock_path(&self) -> Option<&str> {
        self.lock_path.as_deref()
    }

    /// Create the lock directory, as a new control directory needs.
    pub fn create_lock(&self) -> Result<(), LockableFilesError> {
        if let Some(lock_path) = &self.lock_path {
            LockDir::new(self.transport.as_ref(), lock_path).create()?;
        }
        Ok(())
    }

    /// Whether the object is locked.
    pub fn is_locked(&self) -> bool {
        self.state.lock().unwrap().count >= 1
    }

    /// The mode the object is locked in, if it is.
    pub fn lock_mode(&self) -> Option<LockMode> {
        let state = self.state.lock().unwrap();
        (state.count > 0).then_some(state.mode).flatten()
    }

    /// The number of locks held.
    pub fn lock_count(&self) -> usize {
        self.state.lock().unwrap().count
    }

    /// Take a read lock; a lock of either kind already held is counted.
    pub fn lock_read(&self) {
        let mut state = self.state.lock().unwrap();
        if state.mode.is_none() {
            state.mode = Some(LockMode::Read);
        }
        state.count += 1;
    }

    /// Take a write lock. A write lock already held is counted, after
    /// checking `token` against it; a read lock cannot be upgraded. With
    /// `token`, a physical lock held already is taken over instead of
    /// taking a new one, and is not released by the last unlock.
    pub fn lock_write(
        &self,
        token: Option<&str>,
        waiter: &mut dyn LockWaiter,
    ) -> Result<WriteLocked, LockableFilesError> {
        let mut state = self.state.lock().unwrap();
        match state.mode {
            Some(LockMode::Read) => return Err(LockableFilesError::ReadOnly),
            Some(LockMode::Write) => {
                self.validate_token(token)?;
                state.count += 1;
                return Ok(WriteLocked {
                    token: state.nonce.clone(),
                    acquired: false,
                });
            }
            None => {}
        }
        let mut acquired = false;
        if let Some(lock_path) = &self.lock_path {
            match token {
                Some(token) => {
                    self.validate_token(Some(token))?;
                    state.nonce = Some(token.to_string());
                    state.locked_via_token = true;
                }
                None => {
                    state.nonce = Some(self.wait_lock(lock_path, waiter)?);
                    state.locked_via_token = false;
                    acquired = true;
                }
            }
        }
        state.mode = Some(LockMode::Write);
        state.count = 1;
        Ok(WriteLocked {
            token: state.nonce.clone(),
            acquired,
        })
    }

    /// Release one lock. Returns the nonce of the physical lock if the last
    /// write lock released it.
    pub fn unlock(&self) -> Result<Option<String>, LockableFilesError> {
        let mut state = self.state.lock().unwrap();
        if state.mode.is_none() {
            return Err(LockableFilesError::NotHeld);
        }
        if state.count > 1 {
            state.count -= 1;
            return Ok(None);
        }
        let mode = state.mode.take();
        state.count = 0;
        let nonce = state.nonce.take();
        let via_token = std::mem::take(&mut state.locked_via_token);
        drop(state);
        match (mode, &self.lock_path, nonce, via_token) {
            (Some(LockMode::Write), Some(lock_path), Some(nonce), false) => {
                let mut lockdir = LockDir::new(self.transport.as_ref(), lock_path);
                lockdir.resume(&nonce)?;
                lockdir.unlock()?;
                Ok(Some(nonce))
            }
            _ => Ok(None),
        }
    }

    /// Leave the physical lock in place when the last write lock is
    /// released.
    pub fn leave_in_place(&self) {
        self.state.lock().unwrap().locked_via_token = true;
    }

    /// Release the physical lock with the last write lock again.
    pub fn dont_leave_in_place(&self) {
        self.state.lock().unwrap().locked_via_token = false;
    }

    /// Whether the physical lock is held, by anyone.
    pub fn get_physical_lock_status(&self) -> Result<bool, LockableFilesError> {
        match &self.lock_path {
            None => Ok(false),
            Some(lock_path) => Ok(LockDir::new(self.transport.as_ref(), lock_path)
                .peek()?
                .is_some()),
        }
    }

    /// The holder of the physical lock, if it is held.
    pub fn peek(&self) -> Result<Option<LockHeldInfo>, LockableFilesError> {
        match &self.lock_path {
            None => Ok(None),
            Some(lock_path) => Ok(LockDir::new(self.transport.as_ref(), lock_path).peek()?),
        }
    }

    /// Check that `token`, if given, names the physical lock that is held.
    fn validate_token(&self, token: Option<&str>) -> Result<(), LockableFilesError> {
        let Some(token) = token else {
            return Ok(());
        };
        let held = self.peek()?.and_then(|info| info.nonce);
        if held.as_deref() != Some(token) {
            return Err(LockError::TokenMismatch {
                given: token.to_string(),
                held,
            }
            .into());
        }
        Ok(())
    }

    /// Take the physical lock, asking `waiter` whether to try again while
    /// it is held; returns its nonce.
    fn wait_lock(
        &self,
        lock_path: &str,
        waiter: &mut dyn LockWaiter,
    ) -> Result<String, LockableFilesError> {
        loop {
            let mut lockdir = LockDir::new(self.transport.as_ref(), lock_path);
            match lockdir.attempt_lock() {
                Ok(nonce) => {
                    // The handle would release the lock when dropped; it is
                    // released through its nonce instead.
                    std::mem::forget(lockdir);
                    return Ok(nonce);
                }
                Err(LockError::AlreadyHeld) => {}
                Err(e) => return Err(e.into()),
            }
            let holder = lockdir.peek()?;
            if !waiter.contended(holder.as_ref()) {
                return Err(LockableFilesError::Contention);
            }
        }
    }
}

/// The lock of the branch whose control files `transport` reaches: every
/// metadir branch format takes a lock directory, `lock`.
pub fn branch_lock(transport: SharedTransport) -> LockableFiles {
    LockableFiles::new(transport, Some("lock"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lock_in(dir: &tempfile::TempDir) -> LockableFiles {
        let transport: SharedTransport =
            Arc::new(crate::transport::LocalTransport::new(dir.path()));
        let lock = LockableFiles::new(transport, Some("lock"));
        lock.create_lock().unwrap();
        lock
    }

    #[test]
    fn write_lock_is_physical_and_counted() {
        let dir = tempfile::tempdir().unwrap();
        let lock = lock_in(&dir);
        let locked = lock.lock_write(None, &mut NoWait).unwrap();
        assert!(locked.acquired);
        let token = locked.token.expect("a physical lock has a token");
        assert!(dir.path().join("lock/held/info").exists());
        let again = lock.lock_write(Some(&token), &mut NoWait).unwrap();
        assert_eq!((Some(token.clone()), false), (again.token, again.acquired));
        lock.lock_read();
        assert_eq!(3, lock.lock_count());
        assert_eq!(None, lock.unlock().unwrap());
        assert_eq!(None, lock.unlock().unwrap());
        assert!(dir.path().join("lock/held/info").exists());
        assert_eq!(Some(token), lock.unlock().unwrap());
        assert!(!dir.path().join("lock/held").exists());
        assert!(!lock.is_locked());
    }

    #[test]
    fn contention_and_tokens() {
        let dir = tempfile::tempdir().unwrap();
        let first = lock_in(&dir);
        let second = lock_in(&dir);
        let token = first.lock_write(None, &mut NoWait).unwrap().token.unwrap();
        assert!(matches!(
            second.lock_write(None, &mut NoWait),
            Err(LockableFilesError::Contention)
        ));
        assert!(matches!(
            second.lock_write(Some("bogus"), &mut NoWait),
            Err(LockableFilesError::Lock(LockError::TokenMismatch { .. }))
        ));
        // A lock taken over by token is left for its holder to release.
        let taken = second.lock_write(Some(&token), &mut NoWait).unwrap();
        assert!(!taken.acquired);
        assert_eq!(None, second.unlock().unwrap());
        assert!(first.get_physical_lock_status().unwrap());
        first.unlock().unwrap();
        assert!(!first.get_physical_lock_status().unwrap());
    }

    #[test]
    fn waiter_is_asked_while_contended() {
        struct Count(usize);
        impl LockWaiter for Count {
            fn contended(&mut self, holder: Option<&LockHeldInfo>) -> bool {
                assert!(holder.is_some());
                self.0 += 1;
                self.0 < 3
            }
        }
        let dir = tempfile::tempdir().unwrap();
        let first = lock_in(&dir);
        let second = lock_in(&dir);
        first.lock_write(None, &mut NoWait).unwrap();
        let mut waiter = Count(0);
        assert!(matches!(
            second.lock_write(None, &mut waiter),
            Err(LockableFilesError::Contention)
        ));
        assert_eq!(3, waiter.0);
        first.unlock().unwrap();
    }

    #[test]
    fn read_locks_are_logical_and_not_upgraded() {
        let dir = tempfile::tempdir().unwrap();
        let lock = lock_in(&dir);
        assert!(matches!(lock.unlock(), Err(LockableFilesError::NotHeld)));
        lock.lock_read();
        assert_eq!(Some(LockMode::Read), lock.lock_mode());
        assert!(!lock.get_physical_lock_status().unwrap());
        assert!(matches!(
            lock.lock_write(None, &mut NoWait),
            Err(LockableFilesError::ReadOnly)
        ));
        lock.unlock().unwrap();
        assert_eq!(None, lock.lock_mode());
    }

    #[test]
    fn count_only_lock_takes_no_physical_lock() {
        let dir = tempfile::tempdir().unwrap();
        let transport: SharedTransport =
            Arc::new(crate::transport::LocalTransport::new(dir.path()));
        let lock = LockableFiles::new(transport, None);
        let locked = lock.lock_write(None, &mut NoWait).unwrap();
        assert_eq!((None, false), (locked.token, locked.acquired));
        assert!(!lock.get_physical_lock_status().unwrap());
        assert_eq!(None, lock.unlock().unwrap());
    }
}
