//! Reading and writing a bzr branch (Branch Format 7).
//!
//! A branch lives under `.bzr/branch/` and is small: a `last-revision`
//! file (`<revno> <revision_id>`), a bencode `tags` file, a `branch.conf`
//! ini file, and a `lock` lock-dir. This module reads and writes those
//! through a [`Transport`] rooted at `.bzr/branch`, taking the branch lock
//! for mutations.

pub mod format;
mod formats;

pub use format::{all_formats, find_format, BranchFormat};

use std::collections::BTreeMap;

use crate::lockdir::{Lock, LockDir, LockError};
use crate::transport::{SharedTransport, TransportError};

/// The null revision id, used when a branch has no commits.
pub const NULL_REVISION: &[u8] = b"null:";

/// Errors from branch operations.
#[derive(Debug)]
pub enum BranchError {
    /// The `last-revision` file was malformed.
    Corrupt(String),
    /// The branch lock could not be taken or released.
    Lock(LockError),
    /// An underlying transport error.
    Transport(TransportError),
}

impl std::fmt::Display for BranchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BranchError::Corrupt(m) => write!(f, "corrupt branch data: {m}"),
            BranchError::Lock(e) => write!(f, "branch lock error: {e}"),
            BranchError::Transport(e) => write!(f, "transport error: {e}"),
        }
    }
}

impl std::error::Error for BranchError {}

impl From<TransportError> for BranchError {
    fn from(e: TransportError) -> Self {
        BranchError::Transport(e)
    }
}

impl From<LockError> for BranchError {
    fn from(e: LockError) -> Self {
        BranchError::Lock(e)
    }
}

/// `(revno, revision_id)` — the number of revisions on the branch's
/// mainline and the tip revision id. A branch with no commits is
/// `(0, b"null:")`.
pub type RevisionInfo = (u64, Vec<u8>);

/// A bzr branch, accessed through a transport rooted at `.bzr/branch`.
///
/// Owns its transport (as a [`SharedTransport`]) for consistency with the
/// other opener objects, so a `BzrDir` can hand out a `Branch` that
/// outlives it.
pub struct Branch {
    transport: SharedTransport,
}

impl Branch {
    /// Open the branch reachable through `transport` (rooted at
    /// `.bzr/branch`).
    pub fn new(transport: SharedTransport) -> Self {
        Branch { transport }
    }

    /// The tip of the branch as `(revno, revision_id)`.
    ///
    /// Reads `last-revision`, whose single line is `<revno> <revision_id>`.
    /// A missing file means an empty branch, reported as `(0, b"null:")`.
    pub fn last_revision_info(&self) -> Result<RevisionInfo, BranchError> {
        let bytes = match self.transport.get_bytes("last-revision") {
            Ok(b) => b,
            Err(TransportError::NoSuchFile(_)) => return Ok((0, NULL_REVISION.to_vec())),
            Err(e) => return Err(e.into()),
        };
        let line = bytes.strip_suffix(b"\n").unwrap_or(&bytes);
        let space = line
            .iter()
            .position(|&b| b == b' ')
            .ok_or_else(|| BranchError::Corrupt("last-revision missing space".to_string()))?;
        let revno: u64 = std::str::from_utf8(&line[..space])
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| {
                BranchError::Corrupt("last-revision revno not an integer".to_string())
            })?;
        let revision_id = line[space + 1..].to_vec();
        Ok((revno, revision_id))
    }

    /// The tip revision id (`b"null:"` for an empty branch).
    pub fn last_revision(&self) -> Result<Vec<u8>, BranchError> {
        Ok(self.last_revision_info()?.1)
    }

    /// The branch tags as a `name -> revision_id` map.
    ///
    /// Reads the bencode dict in `tags`; a missing or empty file means no
    /// tags.
    pub fn tags(&self) -> Result<BTreeMap<String, Vec<u8>>, BranchError> {
        let bytes = match self.transport.get_bytes("tags") {
            Ok(b) => b,
            Err(TransportError::NoSuchFile(_)) => return Ok(BTreeMap::new()),
            Err(e) => return Err(e.into()),
        };
        decode_tags(&bytes)
    }

    /// The raw contents of `branch.conf`, or empty if absent.
    pub fn get_config_bytes(&self) -> Result<Vec<u8>, BranchError> {
        match self.transport.get_bytes("branch.conf") {
            Ok(b) => Ok(b),
            Err(TransportError::NoSuchFile(_)) => Ok(Vec::new()),
            Err(e) => Err(e.into()),
        }
    }

    /// Take the branch write lock for the duration of `f`.
    ///
    /// The branch lock dir is `lock` under the branch directory.
    fn with_write_lock<R>(
        &self,
        f: impl FnOnce() -> Result<R, BranchError>,
    ) -> Result<R, BranchError> {
        let mut lock = LockDir::new(self.transport.as_ref(), "lock");
        lock.create()?;
        lock.attempt_lock()?;
        let result = f();
        // Release even if f failed; prefer reporting f's error.
        let unlock = lock.unlock();
        match (result, unlock) {
            (Ok(r), Ok(())) => Ok(r),
            (Err(e), _) => Err(e),
            (Ok(_), Err(e)) => Err(e.into()),
        }
    }

    /// Set the branch tip to `(revno, revision_id)`, under the branch lock.
    pub fn set_last_revision_info(
        &self,
        revno: u64,
        revision_id: &[u8],
    ) -> Result<(), BranchError> {
        self.with_write_lock(|| {
            let mut content = format!("{revno} ").into_bytes();
            content.extend_from_slice(revision_id);
            content.push(b'\n');
            self.transport.put_bytes("last-revision", &content, None)?;
            Ok(())
        })
    }

    /// Replace the branch tags, under the branch lock.
    pub fn set_tags(&self, tags: &BTreeMap<String, Vec<u8>>) -> Result<(), BranchError> {
        self.with_write_lock(|| {
            self.transport.put_bytes("tags", &encode_tags(tags), None)?;
            Ok(())
        })
    }
}

/// Encode a tag map as breezy's bencode dict (`{name_utf8: revision_id}`),
/// keys sorted (a `BTreeMap` is already ordered, which is what bencode
/// requires).
fn encode_tags(tags: &BTreeMap<String, Vec<u8>>) -> Vec<u8> {
    use bendy::encoding::Encoder;
    let mut e = Encoder::new();
    e.emit_dict(|mut d| {
        for (name, target) in tags {
            d.emit_pair(name.as_bytes(), Bytes(target))?;
        }
        Ok(())
    })
    .expect("tag dict encoding cannot fail");
    e.get_output().expect("tag dict encoding cannot fail")
}

/// A `ToBencode` adapter emitting a byte string, so tag values can be
/// passed to `emit_pair`.
struct Bytes<'a>(&'a [u8]);

impl bendy::encoding::ToBencode for Bytes<'_> {
    const MAX_DEPTH: usize = 0;
    fn encode(
        &self,
        encoder: bendy::encoding::SingleItemEncoder<'_>,
    ) -> Result<(), bendy::encoding::Error> {
        encoder.emit_bytes(self.0)
    }
}

/// Decode breezy's bencode tag dict.
fn decode_tags(bytes: &[u8]) -> Result<BTreeMap<String, Vec<u8>>, BranchError> {
    use bendy::decoding::{Decoder, Object};
    if bytes.is_empty() {
        return Ok(BTreeMap::new());
    }
    let mut decoder = Decoder::new(bytes);
    let obj = decoder
        .next_object()
        .map_err(|e| BranchError::Corrupt(format!("tags decode: {e}")))?;
    let mut dict = match obj {
        Some(Object::Dict(d)) => d,
        _ => {
            return Err(BranchError::Corrupt(
                "tags is not a bencode dict".to_string(),
            ))
        }
    };
    let mut out = BTreeMap::new();
    while let Some((key, value)) = dict
        .next_pair()
        .map_err(|e| BranchError::Corrupt(format!("tags decode: {e}")))?
    {
        let name = String::from_utf8(key.to_vec())
            .map_err(|_| BranchError::Corrupt("tag name not utf-8".to_string()))?;
        let target = value
            .try_into_bytes()
            .map_err(|e| BranchError::Corrupt(format!("tag value not bytes: {e}")))?
            .to_vec();
        out.insert(name, target);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::{LocalTransport, Transport};
    use std::sync::Arc;

    /// A branch over a temp dir, plus a borrowed handle to the same
    /// transport for asserting on-disk bytes.
    fn branch_transport() -> (tempfile::TempDir, Branch, Arc<LocalTransport>) {
        let dir = tempfile::tempdir().unwrap();
        let probe = Arc::new(LocalTransport::new(dir.path()));
        let shared: SharedTransport = Arc::new(LocalTransport::new(dir.path()));
        (dir, Branch::new(shared), probe)
    }

    #[test]
    fn empty_branch_is_null_revision() {
        let (_d, branch, _probe) = branch_transport();
        assert_eq!(
            branch.last_revision_info().unwrap(),
            (0, NULL_REVISION.to_vec())
        );
        assert!(branch.tags().unwrap().is_empty());
    }

    #[test]
    fn last_revision_round_trips() {
        let (_d, branch, _probe) = branch_transport();
        branch.set_last_revision_info(5, b"rev-abc").unwrap();
        assert_eq!(
            branch.last_revision_info().unwrap(),
            (5, b"rev-abc".to_vec())
        );
        assert_eq!(branch.last_revision().unwrap(), b"rev-abc".to_vec());
    }

    #[test]
    fn last_revision_on_disk_format() {
        let (_d, branch, probe) = branch_transport();
        branch.set_last_revision_info(2, b"x").unwrap();
        assert_eq!(probe.get_bytes("last-revision").unwrap(), b"2 x\n");
    }

    #[test]
    fn tags_round_trip() {
        let (_d, branch, _probe) = branch_transport();
        let mut tags = BTreeMap::new();
        tags.insert("v1.0".to_string(), b"rev-1".to_vec());
        tags.insert("v2.0".to_string(), b"rev-2".to_vec());
        branch.set_tags(&tags).unwrap();
        assert_eq!(branch.tags().unwrap(), tags);
    }

    #[test]
    fn tags_on_disk_matches_breezy_bencode() {
        let (_d, branch, probe) = branch_transport();
        let mut tags = BTreeMap::new();
        tags.insert(
            "v1.0".to_string(),
            b"test@example.com-20200101120000-x".to_vec(),
        );
        branch.set_tags(&tags).unwrap();
        // Byte-for-byte the format brz writes: d4:v1.0<len>:<rev>e.
        assert_eq!(
            probe.get_bytes("tags").unwrap(),
            b"d4:v1.033:test@example.com-20200101120000-xe".to_vec()
        );
    }
}
