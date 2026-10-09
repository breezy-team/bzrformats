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
    /// The lock is in use, by this object or a live process, and cannot be
    /// broken.
    Active,
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
            LockableFilesError::Active => f.write_str("lock is in use and cannot be broken"),
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
    /// The token of the physical lock while it is held.
    nonce: Option<LockToken>,
    /// The physical lock is not released by the last unlock: it was taken
    /// over with a token, or is to be left in place.
    locked_via_token: bool,
}

/// The token of a held physical lock: the nonce of its lock directory, by
/// which another lock object can take the lock over.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LockToken(String);

impl LockToken {
    /// The token as a string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The token as an owned string.
    pub fn into_string(self) -> String {
        self.0
    }
}

impl From<String> for LockToken {
    fn from(token: String) -> Self {
        LockToken(token)
    }
}

impl From<&str> for LockToken {
    fn from(token: &str) -> Self {
        LockToken(token.to_string())
    }
}

impl std::fmt::Display for LockToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// The outcome of taking a write lock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteLocked {
    token: Option<LockToken>,
    acquired: bool,
}

impl WriteLocked {
    /// The token of the physical lock, if the object takes one.
    pub fn token(&self) -> Option<&LockToken> {
        self.token.as_ref()
    }

    /// The token of the physical lock, if the object takes one.
    pub fn into_token(self) -> Option<LockToken> {
        self.token
    }

    /// Whether the physical lock was taken by this call, rather than being
    /// held already or taken over with a token.
    pub fn acquired(&self) -> bool {
        self.acquired
    }
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
        token: Option<&LockToken>,
        waiter: &mut dyn LockWaiter,
    ) -> Result<WriteLocked, LockableFilesError> {
        let token = token.map(LockToken::as_str);
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
                    state.nonce = Some(LockToken::from(token));
                    state.locked_via_token = true;
                }
                None => {
                    state.nonce = Some(LockToken(self.wait_lock(lock_path, waiter)?));
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

    /// Release one lock. Returns the token of the physical lock if the last
    /// write lock released it.
    pub fn unlock(&self) -> Result<Option<LockToken>, LockableFilesError> {
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
                lockdir.resume(nonce.as_str())?;
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

    /// Break the physical lock if someone else holds it and `confirm`
    /// agrees. `confirm` is given the holder,
    /// or `None` when the lock's `info` file cannot be parsed. Returns
    /// whether a lock was broken.
    ///
    /// Breaking a lock this object holds is an error.
    pub fn break_lock(
        &self,
        confirm: &mut dyn FnMut(Option<&LockHeldInfo>) -> bool,
    ) -> Result<bool, LockableFilesError> {
        let Some(lock_path) = &self.lock_path else {
            return Ok(false);
        };
        if self.state.lock().unwrap().nonce.is_some() {
            return Err(LockError::BreakOwnLock.into());
        }
        let mut lockdir = LockDir::new(self.transport.as_ref(), lock_path);
        match lockdir.peek() {
            Ok(None) => Ok(false),
            Ok(Some(holder)) => {
                if !confirm(Some(&holder)) {
                    return Ok(false);
                }
                Ok(lockdir.force_break(&holder)?)
            }
            Err(LockError::Corrupt(_)) => {
                let Some(info) = lockdir.held_info_bytes()? else {
                    return Ok(false);
                };
                if !confirm(None) {
                    return Ok(false);
                }
                Ok(lockdir.force_break_corrupt(&info)?)
            }
            Err(e) => Err(e.into()),
        }
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

/// An object locked through a counted read or write lock: a branch,
/// repository or working tree.
///
/// These are the raw, counted lock operations; every lock taken must be released
/// exactly once. Code that locks an object for a scope should use the
/// guards from [`LockableExt`] instead, and must not release a guard's lock
/// through these methods.
pub trait Lockable {
    /// The error locking or unlocking the object fails with.
    type Error: std::error::Error;

    /// Take a read lock; a lock of either kind already held is counted.
    fn lock_read(&mut self) -> Result<(), Self::Error>;

    /// Take a write lock, with `waiter` deciding what to do while someone
    /// else holds it. A write lock already held is counted; a read lock
    /// cannot be upgraded.
    fn lock_write(&mut self, waiter: &mut dyn LockWaiter) -> Result<WriteLocked, Self::Error>;

    /// Release one lock. Returns the token of the physical lock if that
    /// released it.
    fn unlock(&mut self) -> Result<Option<LockToken>, Self::Error>;
}

/// Locks on a [`Lockable`] object that are released when they go out of
/// scope.
pub trait LockableExt: Lockable {
    /// Take a read lock until the guard is dropped.
    fn read_locked(&mut self) -> Result<ReadGuard<'_, Self>, Self::Error> {
        self.lock_read()?;
        Ok(ReadGuard {
            target: self,
            released: false,
        })
    }

    /// Take a write lock until the guard is dropped, giving up at once if
    /// someone else holds it.
    fn write_locked(&mut self) -> Result<WriteGuard<'_, Self>, Self::Error> {
        self.write_locked_with(&mut NoWait)
    }

    /// Take a write lock until the guard is dropped, with `waiter` deciding
    /// what to do while someone else holds it.
    fn write_locked_with(
        &mut self,
        waiter: &mut dyn LockWaiter,
    ) -> Result<WriteGuard<'_, Self>, Self::Error> {
        let locked = self.lock_write(waiter)?;
        Ok(WriteGuard {
            read: ReadGuard {
                target: self,
                released: false,
            },
            locked,
        })
    }
}

impl<T: Lockable + ?Sized> LockableExt for T {}

/// A read lock on `T`, released when the guard is dropped.
///
/// The guard derefs to `&T` only. That keeps methods taking `&mut T` out of
/// reach, but it is not a guarantee that nothing is written: an object that
/// writes through `&self`, as a branch does, refuses the write at runtime
/// instead. The guard borrows `T` exclusively, so an object has one read
/// guard at a time even though its read locks are counted.
///
/// Dropping the guard releases the lock; a failure to do so is logged, and
/// panics in debug builds. Call [`unlock`](ReadGuard::unlock) to handle it.
#[must_use = "the lock is released as soon as the guard is dropped"]
pub struct ReadGuard<'a, T: Lockable + ?Sized> {
    target: &'a mut T,
    released: bool,
}

impl<T: Lockable + ?Sized> ReadGuard<'_, T> {
    /// Release the lock. Returns the token of the physical lock if that
    /// released it.
    ///
    /// A method rather than an associated function so that it shadows
    /// [`Lockable::unlock`] on the locked object, which would release the
    /// lock out from under the guard.
    pub fn unlock(mut self) -> Result<Option<LockToken>, T::Error> {
        self.released = true;
        self.target.unlock()
    }
}

impl<T: Lockable + ?Sized> std::ops::Deref for ReadGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.target
    }
}

impl<T: Lockable + ?Sized> Drop for ReadGuard<'_, T> {
    fn drop(&mut self) {
        if self.released {
            return;
        }
        if let Err(e) = self.target.unlock() {
            log::error!("failed to release lock: {e}");
            if cfg!(debug_assertions) && !std::thread::panicking() {
                panic!("failed to release lock: {}", e);
            }
        }
    }
}

/// A write lock on `T`, released when the guard is dropped.
///
/// Dropping the guard releases the lock; a failure to do so, such as a
/// write group left open or a working tree that could not be saved, is
/// logged, and panics in debug builds. Call [`unlock`](WriteGuard::unlock)
/// to handle it.
#[must_use = "the lock is released as soon as the guard is dropped"]
pub struct WriteGuard<'a, T: Lockable + ?Sized> {
    read: ReadGuard<'a, T>,
    locked: WriteLocked,
}

impl<T: Lockable + ?Sized> WriteGuard<'_, T> {
    /// The token of the physical lock, if the object takes one.
    pub fn token<'g>(guard: &'g Self) -> Option<&'g LockToken> {
        guard.locked.token()
    }

    /// Whether the physical lock was taken for this guard, rather than
    /// being held already.
    pub fn acquired(guard: &Self) -> bool {
        guard.locked.acquired()
    }

    /// Release the lock. Returns the token of the physical lock if that
    /// released it.
    ///
    /// A method for the same reason as [`ReadGuard::unlock`].
    pub fn unlock(self) -> Result<Option<LockToken>, T::Error> {
        self.read.unlock()
    }
}

impl<T: Lockable + ?Sized> std::ops::Deref for WriteGuard<'_, T> {
    type Target = T;

    fn deref(&self) -> &T {
        self.read.target
    }
}

impl<T: Lockable + ?Sized> std::ops::DerefMut for WriteGuard<'_, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.read.target
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
        assert!(locked.acquired());
        let token = locked.into_token().expect("a physical lock has a token");
        assert!(dir.path().join("lock/held/info").exists());
        let again = lock.lock_write(Some(&token), &mut NoWait).unwrap();
        assert_eq!((Some(&token), false), (again.token(), again.acquired()));
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
        let token = first
            .lock_write(None, &mut NoWait)
            .unwrap()
            .into_token()
            .unwrap();
        assert!(matches!(
            second.lock_write(None, &mut NoWait),
            Err(LockableFilesError::Contention)
        ));
        assert!(matches!(
            second.lock_write(Some(&LockToken::from("bogus")), &mut NoWait),
            Err(LockableFilesError::Lock(LockError::TokenMismatch { .. }))
        ));
        // A lock taken over by token is left for its holder to release.
        let taken = second.lock_write(Some(&token), &mut NoWait).unwrap();
        assert!(!taken.acquired());
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
    fn break_lock_asks_before_breaking() {
        let dir = tempfile::tempdir().unwrap();
        let holder = lock_in(&dir);
        let other = lock_in(&dir);
        let token = holder
            .lock_write(None, &mut NoWait)
            .unwrap()
            .into_token()
            .unwrap();

        let mut asked = Vec::new();
        let mut decline = |info: Option<&LockHeldInfo>| {
            asked.push(info.and_then(|i| i.nonce.clone()));
            false
        };
        assert!(!other.break_lock(&mut decline).unwrap());
        assert_eq!(vec![Some(token.as_str().to_string())], asked);
        assert!(other.get_physical_lock_status().unwrap());

        assert!(other.break_lock(&mut |_| true).unwrap());
        assert!(!other.get_physical_lock_status().unwrap());
        // Nothing left to break.
        assert!(!other.break_lock(&mut |_| true).unwrap());
    }

    #[test]
    fn break_lock_refuses_a_lock_this_object_holds() {
        let dir = tempfile::tempdir().unwrap();
        let lock = lock_in(&dir);
        lock.lock_write(None, &mut NoWait).unwrap();
        assert!(matches!(
            lock.break_lock(&mut |_| true),
            Err(LockableFilesError::Lock(LockError::BreakOwnLock))
        ));
        lock.unlock().unwrap();
    }

    #[test]
    fn break_lock_with_corrupt_info() {
        let dir = tempfile::tempdir().unwrap();
        let lock = lock_in(&dir);
        std::fs::create_dir(dir.path().join("lock/held")).unwrap();
        std::fs::write(dir.path().join("lock/held/info"), b"{ not yaml").unwrap();
        let mut asked = Vec::new();
        assert!(lock
            .break_lock(&mut |info| {
                asked.push(info.is_none());
                true
            })
            .unwrap());
        assert_eq!(vec![true], asked);
        assert!(!dir.path().join("lock/held").exists());
    }

    #[test]
    fn force_break_refuses_a_different_holder() {
        let dir = tempfile::tempdir().unwrap();
        let first = lock_in(&dir);
        let second = lock_in(&dir);
        first.lock_write(None, &mut NoWait).unwrap();
        let stale = first.peek().unwrap().unwrap();
        first.unlock().unwrap();
        second.lock_write(None, &mut NoWait).unwrap();
        let transport = second.transport().clone();
        let mut lockdir = LockDir::new(transport.as_ref(), "lock");
        assert!(matches!(
            lockdir.force_break(&stale),
            Err(LockError::BreakMismatch { .. })
        ));
        assert!(second.get_physical_lock_status().unwrap());
        second.unlock().unwrap();
    }

    #[test]
    fn count_only_lock_takes_no_physical_lock() {
        let dir = tempfile::tempdir().unwrap();
        let transport: SharedTransport =
            Arc::new(crate::transport::LocalTransport::new(dir.path()));
        let lock = LockableFiles::new(transport, None);
        let locked = lock.lock_write(None, &mut NoWait).unwrap();
        assert_eq!((None, false), (locked.token(), locked.acquired()));
        assert!(!lock.get_physical_lock_status().unwrap());
        assert_eq!(None, lock.unlock().unwrap());
    }
}
