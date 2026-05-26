//! Real-filesystem backing for [`Transport`].
//!
//! Used by pure-Rust callers that want to drive a [`DirState`] against
//! a file on disk without going through Python. Wraps [`bazaar::lock`]
//! for OS-level locking and [`std::fs`] for read/write/fdatasync, and
//! forwards [`Transport::lstat`] / [`Transport::read_link`] /
//! [`Transport::list_dir`] / [`Transport::is_tree_reference_dir`] to
//! `std::fs` and `osutils` directly.
//!
//! The pyo3 adapter still ships its own Python-file-backed transport
//! because Python tests inject mock file objects; pure-Rust consumers
//! get [`FileTransport`].

use super::transport::{DirEntryInfo, StatInfo, Transport, TransportError};
use super::LockState;
use crate::lock::{LockError, ReadLock, TemporaryWriteLockResult, WriteLock};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

enum LockHandle {
    Read(ReadLock),
    Write(WriteLock),
}

/// Filesystem-backed [`Transport`] using [`bazaar::lock`] for the
/// dirstate file's lock.
pub struct FileTransport {
    /// Path of the dirstate file.
    path: PathBuf,
    /// Whether to call `fdatasync` after writes. Mirrors the
    /// `fdatasync` flag on `DirState`; the transport doesn't read it,
    /// the caller does, but we expose `fdatasync` unconditionally for
    /// the trait.
    lock: Option<LockHandle>,
}

impl FileTransport {
    pub fn new<P: Into<PathBuf>>(path: P) -> Self {
        Self {
            path: path.into(),
            lock: None,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn current_file(&mut self) -> Result<&mut std::fs::File, TransportError> {
        match &mut self.lock {
            Some(LockHandle::Read(rl)) => rl.file_mut().ok_or(TransportError::NotLocked),
            Some(LockHandle::Write(wl)) => wl.file_mut().ok_or(TransportError::NotLocked),
            None => Err(TransportError::NotLocked),
        }
    }
}

fn map_lock_err(err: LockError) -> TransportError {
    match err {
        LockError::Contention(p) => {
            TransportError::LockContention(p.to_string_lossy().into_owned())
        }
        LockError::NotHeld(_) => TransportError::NotLocked,
        LockError::Io(e) => TransportError::from(e),
    }
}

impl Transport for FileTransport {
    fn exists(&self) -> Result<bool, TransportError> {
        Ok(self.path.exists())
    }

    fn lock_read(&mut self) -> Result<(), TransportError> {
        if self.lock.is_some() {
            return Err(TransportError::AlreadyLocked);
        }
        let lock = ReadLock::new(&self.path).map_err(map_lock_err)?;
        self.lock = Some(LockHandle::Read(lock));
        Ok(())
    }

    fn lock_write(&mut self) -> Result<(), TransportError> {
        if self.lock.is_some() {
            return Err(TransportError::AlreadyLocked);
        }
        let lock = WriteLock::new(&self.path).map_err(map_lock_err)?;
        self.lock = Some(LockHandle::Write(lock));
        Ok(())
    }

    fn unlock(&mut self) -> Result<(), TransportError> {
        match self.lock.take() {
            None => Err(TransportError::NotLocked),
            Some(LockHandle::Read(mut rl)) => rl.unlock().map_err(map_lock_err),
            Some(LockHandle::Write(mut wl)) => wl.unlock().map_err(map_lock_err),
        }
    }

    fn lock_state(&self) -> Option<LockState> {
        match &self.lock {
            None => None,
            Some(LockHandle::Read(_)) => Some(LockState::Read),
            Some(LockHandle::Write(_)) => Some(LockState::Write),
        }
    }

    fn read_all(&mut self) -> Result<Vec<u8>, TransportError> {
        let file = self.current_file()?;
        file.seek(SeekFrom::Start(0))?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        Ok(buf)
    }

    fn len(&mut self) -> Result<u64, TransportError> {
        let file = self.current_file()?;
        Ok(file.metadata().map_err(TransportError::from)?.len())
    }

    fn read_at(&mut self, offset: u64, len: usize) -> Result<Vec<u8>, TransportError> {
        let file = self.current_file()?;
        file.seek(SeekFrom::Start(offset))?;
        let mut buf = vec![0u8; len];
        let mut filled = 0;
        // Loop because `read` may return fewer bytes than requested on
        // a partial read; callers tolerate a short final read.
        while filled < len {
            let n = file.read(&mut buf[filled..])?;
            if n == 0 {
                buf.truncate(filled);
                return Ok(buf);
            }
            filled += n;
        }
        Ok(buf)
    }

    fn write_all(&mut self, bytes: &[u8]) -> Result<(), TransportError> {
        match &self.lock {
            Some(LockHandle::Write(_)) => {}
            Some(LockHandle::Read(_)) => {
                return Err(TransportError::Other(
                    "write_all requires a write lock".to_string(),
                ))
            }
            None => return Err(TransportError::NotLocked),
        }
        let file = self.current_file()?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(bytes)?;
        let len = bytes.len() as u64;
        file.set_len(len)?;
        file.flush()?;
        Ok(())
    }

    fn upgrade_to_write_lock(&mut self) -> Result<bool, TransportError> {
        match self.lock.take() {
            None => Err(TransportError::NotLocked),
            Some(LockHandle::Write(wl)) => {
                self.lock = Some(LockHandle::Write(wl));
                Err(TransportError::AlreadyLocked)
            }
            Some(LockHandle::Read(rl)) => match rl.temporary_write_lock().map_err(map_lock_err)? {
                TemporaryWriteLockResult::Succeeded(wl) => {
                    self.lock = Some(LockHandle::Write(wl));
                    Ok(true)
                }
                TemporaryWriteLockResult::Failed(rl) => {
                    self.lock = Some(LockHandle::Read(rl));
                    Ok(false)
                }
            },
        }
    }

    fn downgrade_to_read_lock(&mut self) -> Result<(), TransportError> {
        match self.lock.take() {
            None => Err(TransportError::NotLocked),
            Some(LockHandle::Read(rl)) => {
                self.lock = Some(LockHandle::Read(rl));
                Ok(())
            }
            Some(LockHandle::Write(wl)) => {
                let rl = wl.restore_read_lock().map_err(map_lock_err)?;
                self.lock = Some(LockHandle::Read(rl));
                Ok(())
            }
        }
    }

    fn fdatasync(&mut self) -> Result<(), TransportError> {
        let file = self.current_file()?;
        file.sync_data().map_err(TransportError::from)?;
        Ok(())
    }

    fn lstat(&self, abspath: &[u8]) -> Result<StatInfo, TransportError> {
        let path = bytes_to_path(abspath)?;
        let metadata = std::fs::symlink_metadata(&path).map_err(TransportError::from)?;
        stat_info_from_metadata(&metadata)
    }

    fn read_link(&self, abspath: &[u8]) -> Result<Vec<u8>, TransportError> {
        let path = bytes_to_path(abspath)?;
        let target = std::fs::read_link(&path).map_err(TransportError::from)?;
        Ok(path_to_bytes(&target))
    }

    fn is_tree_reference_dir(&self, abspath: &[u8]) -> Result<bool, TransportError> {
        let path = bytes_to_path(abspath)?;
        let bzr_dir = path.join(".bzr");
        Ok(bzr_dir.is_dir())
    }

    fn list_dir(&self, abspath: &[u8]) -> Result<Vec<DirEntryInfo>, TransportError> {
        let path = bytes_to_path(abspath)?;
        let entries = std::fs::read_dir(&path).map_err(TransportError::from)?;
        let mut out = Vec::new();
        for entry in entries {
            let entry = entry.map_err(TransportError::from)?;
            let name = entry.file_name();
            let basename = path_to_bytes(Path::new(&name));
            let metadata = entry.metadata().map_err(TransportError::from)?;
            let stat = stat_info_from_metadata(&metadata)?;
            let kind = osutils_kind_from_stat(&stat);
            let abs = entry.path();
            out.push(DirEntryInfo {
                basename,
                kind,
                stat,
                abspath: path_to_bytes(&abs),
            });
        }
        Ok(out)
    }
}

#[cfg(unix)]
fn bytes_to_path(b: &[u8]) -> Result<PathBuf, TransportError> {
    use std::os::unix::ffi::OsStrExt;
    Ok(PathBuf::from(std::ffi::OsStr::from_bytes(b)))
}

#[cfg(not(unix))]
fn bytes_to_path(b: &[u8]) -> Result<PathBuf, TransportError> {
    String::from_utf8(b.to_vec())
        .map(PathBuf::from)
        .map_err(|e| TransportError::Other(e.to_string()))
}

#[cfg(unix)]
fn path_to_bytes(p: &Path) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;
    p.as_os_str().as_bytes().to_vec()
}

#[cfg(not(unix))]
fn path_to_bytes(p: &Path) -> Vec<u8> {
    p.to_string_lossy().into_owned().into_bytes()
}

fn stat_info_from_metadata(m: &std::fs::Metadata) -> Result<StatInfo, TransportError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        Ok(StatInfo {
            mode: m.mode(),
            size: m.size(),
            mtime: m.mtime(),
            ctime: m.ctime(),
            dev: m.dev(),
            ino: m.ino(),
        })
    }
    #[cfg(not(unix))]
    {
        let _ = m;
        Err(TransportError::Other(
            "lstat unsupported on this platform".to_string(),
        ))
    }
}

fn osutils_kind_from_stat(stat: &StatInfo) -> Option<crate::osutils::Kind> {
    if stat.is_file() {
        Some(crate::osutils::Kind::File)
    } else if stat.is_dir() {
        Some(crate::osutils::Kind::Directory)
    } else if stat.is_symlink() {
        Some(crate::osutils::Kind::Symlink)
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;
    use tempfile::NamedTempFile;

    #[test]
    fn read_write_roundtrip() {
        let mut tmp = NamedTempFile::new().unwrap();
        write!(tmp, "hello").unwrap();
        let mut t = FileTransport::new(tmp.path());
        t.lock_read().unwrap();
        let data = t.read_all().unwrap();
        assert_eq!(data, b"hello");
        t.unlock().unwrap();

        t.lock_write().unwrap();
        t.write_all(b"world!").unwrap();
        t.unlock().unwrap();

        let mut t2 = FileTransport::new(tmp.path());
        t2.lock_read().unwrap();
        let data = t2.read_all().unwrap();
        assert_eq!(data, b"world!");
        t2.unlock().unwrap();
    }

    #[test]
    fn write_lock_creates_missing_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing");
        let mut t = FileTransport::new(&path);
        t.lock_write().unwrap();
        t.write_all(b"created").unwrap();
        t.unlock().unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"created");
    }

    #[test]
    fn read_all_requires_lock() {
        let mut tmp = NamedTempFile::new().unwrap();
        write!(tmp, "x").unwrap();
        let mut t = FileTransport::new(tmp.path());
        assert!(matches!(t.read_all(), Err(TransportError::NotLocked)));
    }

    #[test]
    fn read_at_returns_requested_window() {
        let mut tmp = NamedTempFile::new().unwrap();
        write!(tmp, "0123456789abcdef").unwrap();
        let mut t = FileTransport::new(tmp.path());
        t.lock_read().unwrap();
        assert_eq!(t.read_at(0, 4).unwrap(), b"0123");
        assert_eq!(t.read_at(4, 4).unwrap(), b"4567");
        assert_eq!(t.read_at(10, 6).unwrap(), b"abcdef");
        t.unlock().unwrap();
    }

    #[test]
    fn read_at_short_read_at_eof() {
        let mut tmp = NamedTempFile::new().unwrap();
        write!(tmp, "0123").unwrap();
        let mut t = FileTransport::new(tmp.path());
        t.lock_read().unwrap();
        // Requested window extends past EOF — short read.
        assert_eq!(t.read_at(2, 100).unwrap(), b"23");
        // Starting past EOF — empty.
        assert_eq!(t.read_at(100, 4).unwrap(), b"");
        t.unlock().unwrap();
    }

    #[test]
    fn read_at_requires_lock() {
        let mut tmp = NamedTempFile::new().unwrap();
        write!(tmp, "x").unwrap();
        let mut t = FileTransport::new(tmp.path());
        assert!(matches!(t.read_at(0, 1), Err(TransportError::NotLocked)));
    }

    #[test]
    fn len_reports_file_size() {
        let mut tmp = NamedTempFile::new().unwrap();
        write!(tmp, "0123456789").unwrap();
        let mut t = FileTransport::new(tmp.path());
        t.lock_read().unwrap();
        assert_eq!(t.len().unwrap(), 10);
        t.unlock().unwrap();
    }

    #[test]
    fn upgrade_to_write_then_downgrade_roundtrip() {
        let mut tmp = NamedTempFile::new().unwrap();
        write!(tmp, "before").unwrap();
        let mut t = FileTransport::new(tmp.path());
        t.lock_read().unwrap();
        assert_eq!(t.lock_state(), Some(LockState::Read));

        assert!(t.upgrade_to_write_lock().unwrap());
        assert_eq!(t.lock_state(), Some(LockState::Write));
        t.write_all(b"after").unwrap();

        t.downgrade_to_read_lock().unwrap();
        assert_eq!(t.lock_state(), Some(LockState::Read));
        assert_eq!(t.read_all().unwrap(), b"after");
        t.unlock().unwrap();
    }

    #[test]
    fn upgrade_to_write_lock_when_not_locked_errors() {
        let tmp = NamedTempFile::new().unwrap();
        let mut t = FileTransport::new(tmp.path());
        assert!(matches!(
            t.upgrade_to_write_lock(),
            Err(TransportError::NotLocked)
        ));
    }

    #[test]
    fn upgrade_to_write_lock_when_already_write_errors() {
        let tmp = NamedTempFile::new().unwrap();
        let mut t = FileTransport::new(tmp.path());
        t.lock_write().unwrap();
        assert!(matches!(
            t.upgrade_to_write_lock(),
            Err(TransportError::AlreadyLocked)
        ));
        t.unlock().unwrap();
    }

    #[test]
    fn dirstate_initialize_then_load_roundtrip() {
        // End-to-end: create an empty dirstate via DirState::initialize,
        // then re-open it and verify the parsed dirblocks contain just
        // the root entry.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dirstate");
        let mut transport = FileTransport::new(&path);
        let provider: Box<dyn crate::dirstate::SHA1Provider + Send + Sync> =
            Box::new(crate::dirstate::DefaultSHA1Provider::new());
        let mut state =
            crate::dirstate::DirState::initialize(&mut transport, path.clone(), provider).unwrap();
        assert_eq!(state.parents, Vec::<Vec<u8>>::new());
        assert_eq!(state.dirblocks.len(), 2);
        assert_eq!(state.dirblocks[0].entries.len(), 1);
        assert_eq!(
            state.dirblocks[0].entries[0].key.file_id,
            crate::inventory::ROOT_ID
        );
        transport.unlock().unwrap();

        // Reopen and verify the saved bytes are a valid dirstate.
        let mut t2 = FileTransport::new(&path);
        t2.lock_read().unwrap();
        let data = t2.read_all().unwrap();
        // Should at least start with the format-3 header.
        assert!(data.starts_with(b"#bazaar dirstate flat format 3\n"));
        t2.unlock().unwrap();
    }
}
