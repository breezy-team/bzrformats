//! On-disk lock directories (`LockDir`).
//!
//! A bzr lock is a directory `lock/` under a controlled area. To take
//! the lock, a process creates a uniquely-named pending directory
//! containing an `info` file describing itself, then renames it onto
//! `lock/held`. Because a rename onto an existing target fails, exactly
//! one contender wins; the others observe the existing `held` directory
//! and back off. Releasing renames `held` away and deletes it.
//!
//! This mirrors `breezy.lockdir`, but operates purely on a [`Transport`]
//! and carries none of breezy's UI/config policy (no interactive
//! break-lock prompting, no configurable poll loop). Callers that need
//! to wait poll [`LockDir::attempt_lock`] themselves.

use std::collections::HashMap;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::transport::{Transport, TransportError};

const INFO_NAME: &str = "/info";

/// Errors from lock operations.
#[derive(Debug)]
pub enum LockError {
    /// The lock is already held by someone else.
    AlreadyHeld,
    /// The lock was not held when release/confirm was attempted.
    NotHeld,
    /// The held `info` file could not be parsed.
    Corrupt(String),
    /// An underlying transport error.
    Transport(TransportError),
}

impl std::fmt::Display for LockError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LockError::AlreadyHeld => write!(f, "lock already held"),
            LockError::NotHeld => write!(f, "lock not held"),
            LockError::Corrupt(m) => write!(f, "corrupt lock info: {m}"),
            LockError::Transport(e) => write!(f, "transport error: {e}"),
        }
    }
}

impl std::error::Error for LockError {}

impl From<TransportError> for LockError {
    fn from(e: TransportError) -> Self {
        LockError::Transport(e)
    }
}

/// Information recorded about the holder of a lock.
///
/// Serialized into the `held/info` file as YAML, matching the format
/// breezy writes so the two implementations can read each other's locks.
///
// TODO: `for_this_process` gathers hostname/user/pid from the
// environment inside the crate. A standalone library shouldn't sniff the
// environment; the holder identity should be passed in by the caller
// (e.g. a `LockHolder` the caller constructs). Keep `for_this_process`
// as a convenience but add a constructor that takes the fields directly.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct LockHeldInfo {
    /// Process id of the holder.
    pub pid: Option<u32>,
    /// Username of the holder.
    pub user: Option<String>,
    /// Unique token distinguishing this acquisition from any other.
    pub nonce: Option<String>,
    /// Hostname of the holding machine.
    pub hostname: Option<String>,
    /// When the lock was taken.
    pub start_time: Option<SystemTime>,
    /// Any additional caller-supplied holder attributes.
    #[serde(flatten)]
    pub extra_holder_info: HashMap<String, String>,
}

impl LockHeldInfo {
    /// Build holder info for the current process.
    pub fn for_this_process(extra_holder_info: HashMap<String, String>) -> Self {
        LockHeldInfo {
            hostname: crate::osutils::get_host_name().ok(),
            pid: Some(std::process::id()),
            nonce: Some(crate::osutils::rand_chars(20)),
            start_time: Some(SystemTime::now()),
            user: Some(crate::osutils::get_user_name()),
            extra_holder_info,
        }
    }

    /// Serialize to the bytes stored in the `info` file.
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_yaml::to_string(self)
            .expect("LockHeldInfo serialization cannot fail")
            .into_bytes()
    }

    /// Parse from the contents of an `info` file.
    ///
    /// An empty/whitespace-only file (which can result from an
    /// interrupted write) parses as the default, empty info rather than
    /// an error, matching breezy.
    pub fn from_info_file_bytes(bytes: &[u8]) -> Result<Self, LockError> {
        let value: serde_yaml::Value = serde_yaml::from_slice(bytes)
            .map_err(|e| LockError::Corrupt(format!("could not parse lock info: {e}")))?;
        if value.is_null() {
            return Ok(LockHeldInfo::default());
        }
        serde_yaml::from_value(value)
            .map_err(|e| LockError::Corrupt(format!("could not parse lock info: {e}")))
    }

    /// Whether this info appears to describe the current process.
    pub fn is_locked_by_this_process(&self) -> bool {
        self.hostname == crate::osutils::get_host_name().ok()
            && self.pid == Some(std::process::id())
            && self.user == Some(crate::osutils::get_user_name())
    }
}

/// Behaviour shared by lock handles, so callers can depend on the
/// operations rather than the concrete [`LockDir`].
pub trait Lock {
    /// Try once to take the lock, returning the nonce on success.
    fn attempt_lock(&mut self) -> Result<String, LockError>;
    /// Release a held lock.
    fn unlock(&mut self) -> Result<(), LockError>;
    /// Read the current holder info, or `None` if the lock is free.
    fn peek(&self) -> Result<Option<LockHeldInfo>, LockError>;
    /// Whether this handle currently holds the lock.
    fn is_held(&self) -> bool;
}

/// A lock directory on a transport.
///
/// `path` is the directory (relative to the transport root) that will
/// contain `held/`. The directory itself need not exist yet;
/// [`LockDir::create`] makes it.
pub struct LockDir<'t> {
    transport: &'t dyn Transport,
    path: String,
    held_dir: String,
    held_info_path: String,
    nonce: Option<String>,
    lock_held: bool,
    extra_holder_info: HashMap<String, String>,
}

impl<'t> LockDir<'t> {
    /// Create a handle for the lock at `path` on `transport`.
    pub fn new(transport: &'t dyn Transport, path: &str) -> Self {
        let held_dir = format!("{path}/held");
        let held_info_path = format!("{held_dir}{INFO_NAME}");
        LockDir {
            transport,
            path: path.to_string(),
            held_dir,
            held_info_path,
            nonce: None,
            lock_held: false,
            extra_holder_info: HashMap::new(),
        }
    }

    /// Attach extra holder attributes recorded in the `info` file.
    pub fn with_extra_holder_info(mut self, extra: HashMap<String, String>) -> Self {
        self.extra_holder_info = extra;
        self
    }

    /// Create the lock directory (the container for `held/`).
    ///
    /// Idempotent: an existing directory is fine.
    pub fn create(&self) -> Result<(), LockError> {
        self.transport.mkdir(&self.path)?;
        Ok(())
    }

    fn create_pending_dir(&self, info: &LockHeldInfo) -> Result<String, LockError> {
        let tmpname = format!("{}/{}.tmp", self.path, crate::osutils::rand_chars(10));
        self.transport.mkdir(&tmpname)?;
        self.transport
            .put_bytes(&format!("{tmpname}{INFO_NAME}"), &info.to_bytes())?;
        Ok(tmpname)
    }

    fn remove_pending_dir(&self, tmpname: &str) {
        // Best effort cleanup; ignore errors as breezy does.
        let _ = self.transport.delete(&format!("{tmpname}{INFO_NAME}"));
        let _ = self.transport.rmdir(tmpname);
    }

    fn read_info_at(&self, path: &str) -> Result<Option<LockHeldInfo>, LockError> {
        match self.transport.get_bytes(path) {
            Ok(bytes) => Ok(Some(LockHeldInfo::from_info_file_bytes(&bytes)?)),
            Err(TransportError::NoSuchFile(_)) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
}

impl Lock for LockDir<'_> {
    fn attempt_lock(&mut self) -> Result<String, LockError> {
        if self.lock_held {
            return Err(LockError::AlreadyHeld);
        }
        let info = LockHeldInfo::for_this_process(self.extra_holder_info.clone());
        let nonce = info
            .nonce
            .clone()
            .expect("for_this_process always sets a nonce");
        let tmpname = self.create_pending_dir(&info)?;

        match self.transport.rename(&tmpname, &self.held_dir) {
            Ok(()) => {}
            Err(_) => {
                // Someone else holds it (or the rename otherwise failed);
                // drop our pending dir and report contention.
                self.remove_pending_dir(&tmpname);
                return Err(LockError::AlreadyHeld);
            }
        }

        // Confirm the lock we see is really ours (guards against a racing
        // contender having broken and retaken it between rename and read).
        match self.peek()? {
            Some(held) if held.nonce.as_deref() == Some(nonce.as_str()) => {
                self.nonce = Some(nonce.clone());
                self.lock_held = true;
                Ok(nonce)
            }
            _ => Err(LockError::AlreadyHeld),
        }
    }

    fn unlock(&mut self) -> Result<(), LockError> {
        if !self.lock_held {
            return Err(LockError::NotHeld);
        }
        // Rename held away before deleting, since we can't atomically
        // remove a non-empty directory.
        let tmpname = format!(
            "{}/releasing.{}.tmp",
            self.path,
            crate::osutils::rand_chars(20)
        );
        self.transport.rename(&self.held_dir, &tmpname)?;
        self.lock_held = false;
        self.nonce = None;
        let _ = self.transport.delete(&format!("{tmpname}{INFO_NAME}"));
        let _ = self.transport.rmdir(&tmpname);
        Ok(())
    }

    fn peek(&self) -> Result<Option<LockHeldInfo>, LockError> {
        self.read_info_at(&self.held_info_path)
    }

    fn is_held(&self) -> bool {
        self.lock_held
    }
}

impl Drop for LockDir<'_> {
    fn drop(&mut self) {
        if self.lock_held {
            let _ = self.unlock();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::LocalTransport;

    fn temp_transport() -> (tempfile::TempDir, LocalTransport) {
        let dir = tempfile::tempdir().unwrap();
        let t = LocalTransport::new(dir.path());
        (dir, t)
    }

    #[test]
    fn held_info_round_trips() {
        let info = LockHeldInfo::for_this_process(HashMap::new());
        let bytes = info.to_bytes();
        let parsed = LockHeldInfo::from_info_file_bytes(&bytes).unwrap();
        assert_eq!(info, parsed);
    }

    #[test]
    fn parses_lock_info_written_by_breezy() {
        // A real held/info file written by breezy 3.4. Confirms we read
        // the on-disk YAML format byte-for-byte compatibly.
        let real = concat!(
            "pid: 1486936\n",
            "user: jelmer\n",
            "nonce: of7d51l3euf7pcrojq17\n",
            "hostname: gwenhwyfar\n",
            "start_time:\n",
            "  secs_since_epoch: 1780304488\n",
            "  nanos_since_epoch: 949318362\n",
        )
        .as_bytes();
        let info = LockHeldInfo::from_info_file_bytes(real).unwrap();
        assert_eq!(info.pid, Some(1486936));
        assert_eq!(info.user.as_deref(), Some("jelmer"));
        assert_eq!(info.nonce.as_deref(), Some("of7d51l3euf7pcrojq17"));
        assert_eq!(info.hostname.as_deref(), Some("gwenhwyfar"));
        assert!(info.start_time.is_some());
        // And re-serializing round-trips.
        assert_eq!(
            LockHeldInfo::from_info_file_bytes(&info.to_bytes()).unwrap(),
            info
        );
    }

    #[test]
    fn empty_info_file_is_default() {
        let parsed = LockHeldInfo::from_info_file_bytes(b"").unwrap();
        assert_eq!(parsed, LockHeldInfo::default());
    }

    #[test]
    fn take_and_release() {
        let (_dir, t) = temp_transport();
        let mut ld = LockDir::new(&t, "test_lock");
        ld.create().unwrap();
        assert_eq!(ld.peek().unwrap(), None);
        let nonce = ld.attempt_lock().unwrap();
        assert!(ld.is_held());
        let held = ld.peek().unwrap().unwrap();
        assert_eq!(held.nonce.as_deref(), Some(nonce.as_str()));
        ld.unlock().unwrap();
        assert!(!ld.is_held());
        assert_eq!(ld.peek().unwrap(), None);
    }

    #[test]
    fn second_holder_is_blocked() {
        let (_dir, t) = temp_transport();
        let mut a = LockDir::new(&t, "test_lock");
        a.create().unwrap();
        a.attempt_lock().unwrap();

        let mut b = LockDir::new(&t, "test_lock");
        match b.attempt_lock() {
            Err(LockError::AlreadyHeld) => {}
            other => panic!("expected AlreadyHeld, got {other:?}"),
        }
        assert!(!b.is_held());

        a.unlock().unwrap();
        // Now b can take it.
        b.attempt_lock().unwrap();
        assert!(b.is_held());
    }

    #[test]
    fn drop_releases_lock() {
        let (_dir, t) = temp_transport();
        {
            let mut a = LockDir::new(&t, "test_lock");
            a.create().unwrap();
            a.attempt_lock().unwrap();
        }
        let probe = LockDir::new(&t, "test_lock");
        assert_eq!(probe.peek().unwrap(), None);
    }
}
