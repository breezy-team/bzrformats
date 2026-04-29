//! SHA1 computation abstraction.
//!
//! `DirState` defers content hashing to a pluggable
//! [`SHA1Provider`] so callers with content filters (e.g. the
//! Python layer) can slot in a filtered-read implementation. The
//! default one just hashes the raw file contents.

use super::transport::StatInfo;
use osutils::sha::{sha_file, sha_file_by_name};
use std::fs::File;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;
use std::path::Path;

pub trait SHA1Provider: Send + Sync {
    fn sha1(&self, path: &Path) -> std::io::Result<String>;

    fn stat_and_sha1(&self, path: &Path) -> std::io::Result<(StatInfo, String)>;
}

/// A SHA1Provider that reads directly from the filesystem.
pub struct DefaultSHA1Provider;

impl DefaultSHA1Provider {
    pub fn new() -> DefaultSHA1Provider {
        DefaultSHA1Provider {}
    }
}

impl Default for DefaultSHA1Provider {
    fn default() -> Self {
        Self::new()
    }
}

impl SHA1Provider for DefaultSHA1Provider {
    /// Return the sha1 of a file given its absolute path.
    fn sha1(&self, path: &Path) -> std::io::Result<String> {
        sha_file_by_name(path)
    }

    /// Return the stat and sha1 of a file given its absolute path.
    fn stat_and_sha1(&self, path: &Path) -> std::io::Result<(StatInfo, String)> {
        let mut f = File::open(path)?;
        let md = f.metadata()?;
        let sha1 = sha_file(&mut f)?;
        let stat = metadata_to_stat_info(&md);
        Ok((stat, sha1))
    }
}

fn metadata_to_stat_info(md: &std::fs::Metadata) -> StatInfo {
    use std::time::UNIX_EPOCH;
    let mtime = md
        .modified()
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    #[cfg(unix)]
    let (mode, size, dev, ino, ctime) = (md.mode(), md.size(), md.dev(), md.ino(), md.ctime());
    #[cfg(not(unix))]
    let (mode, size, dev, ino, ctime) = {
        let ctime = md
            .created()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        (0u32, md.len(), 0u64, 0u64, ctime)
    };
    StatInfo {
        mode,
        size,
        mtime,
        ctime,
        dev,
        ino,
    }
}
