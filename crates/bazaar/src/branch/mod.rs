//! Reading and writing a bzr branch (Branch Format 7).
//!
//! A branch lives under `.bzr/branch/` and is small: a `last-revision`
//! file (`<revno> <revision_id>`), a bencode `tags` file, a `branch.conf`
//! ini file, and a `lock` lock-dir. This module reads and writes those
//! through a [`Transport`] rooted at `.bzr/branch`, taking the branch lock
//! for mutations.

pub mod format;

pub use format::{all_formats, find_format, BranchFormat};

use std::collections::BTreeMap;

use crate::declare_branch_format;
use crate::lockdir::{Lock, LockDir, LockError};
use crate::transport::{SharedTransport, TransportError};

// Branch format 5 (full history) is the weave/knit-era layout: it keeps the
// whole mainline in `revision-history` rather than a single `last-revision`
// line, so it is only built when an older repository backend that pairs with
// it is enabled.
#[cfg(any(feature = "weave", feature = "knit"))]
declare_branch_format! {
    FORMAT_5 {
        format_string: b"Bazaar-NG branch format 5\n",
        description: "Branch format 5 (full history)",
        supports_tags: false,
        full_history: true,
        supported: true,
        deprecated: true,
    }
}

declare_branch_format! {
    FORMAT_6 {
        format_string: b"Bazaar Branch Format 6 (bzr 0.15)\n",
        description: "Branch format 6",
        supports_tags: true,
        supported: true,
    }
}

declare_branch_format! {
    FORMAT_7 {
        format_string: b"Bazaar Branch Format 7 (needs bzr 1.6)\n",
        description: "Branch format 7 (stackable)",
        supports_tags: true,
        supports_stacking: true,
        supported: true,
    }
}

declare_branch_format! {
    FORMAT_8 {
        format_string: b"Bazaar Branch Format 8 (needs bzr 1.15)\n",
        description: "Branch format 8 (reference locations)",
        supports_tags: true,
        supports_stacking: true,
        supports_reference_locations: true,
        supported: true,
    }
}

declare_branch_format! {
    REFERENCE_FORMAT_1 {
        format_string: b"Bazaar-NG Branch Reference Format 1\n",
        description: "Branch reference format 1",
        is_reference: true,
    }
}

/// The branch format assumed when the `format` marker is absent (the
/// in-memory test transports don't write one). Format 7 is the modern
/// `last-revision` layout.
const DEFAULT_FORMAT: &BranchFormat = &FORMAT_7;

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
    format: &'static BranchFormat,
}

impl Branch {
    /// Open the branch reachable through `transport` (rooted at
    /// `.bzr/branch`), reading its `format` marker to learn how the tip is
    /// stored. A missing marker is treated as the modern default format.
    pub fn new(transport: SharedTransport) -> Self {
        let format = match transport.get_bytes("format") {
            Ok(marker) => find_format(&marker).unwrap_or(DEFAULT_FORMAT),
            Err(_) => DEFAULT_FORMAT,
        };
        Branch { transport, format }
    }

    /// Open the branch reachable through `transport` as a specific `format`,
    /// without reading a `format` marker file.
    ///
    /// The all-in-one weave layout has no `.bzr/branch/format` file -- the
    /// branch lives at `.bzr` itself with its tip in `.bzr/revision-history`
    /// -- so the format (full-history branch format 5) is supplied directly.
    pub fn with_format(transport: SharedTransport, format: &'static BranchFormat) -> Self {
        Branch { transport, format }
    }

    /// The format this branch was opened as.
    pub fn format(&self) -> &'static BranchFormat {
        self.format
    }

    /// The tip of the branch as `(revno, revision_id)`.
    ///
    /// Format 5 stores the full mainline in `revision-history` (one revision
    /// id per line); the revno is the line count and the tip is the last
    /// line. Formats 6/7/8 store a single `last-revision` line
    /// `<revno> <revision_id>`. A missing file means an empty branch,
    /// reported as `(0, b"null:")`.
    pub fn last_revision_info(&self) -> Result<RevisionInfo, BranchError> {
        if self.format.full_history {
            return self.last_revision_info_full_history();
        }
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

    /// Read the tip from a format-5 `revision-history` file.
    fn last_revision_info_full_history(&self) -> Result<RevisionInfo, BranchError> {
        let history = self.revision_history()?;
        match history.last() {
            Some(tip) => Ok((history.len() as u64, tip.clone())),
            None => Ok((0, NULL_REVISION.to_vec())),
        }
    }

    /// The full mainline as a list of revision ids, oldest first.
    ///
    /// Read from the format-5 `revision-history` file. For formats that store
    /// only the tip (6/7/8), this returns just the tip (or empty), since the
    /// full history is not recorded on the branch.
    pub fn revision_history(&self) -> Result<Vec<Vec<u8>>, BranchError> {
        if !self.format.full_history {
            let (revno, tip) = self.last_revision_info()?;
            return Ok(if revno == 0 { vec![] } else { vec![tip] });
        }
        let bytes = match self.transport.get_bytes("revision-history") {
            Ok(b) => b,
            Err(TransportError::NoSuchFile(_)) => return Ok(vec![]),
            Err(e) => return Err(e.into()),
        };
        Ok(bytes
            .split(|&b| b == b'\n')
            .filter(|l| !l.is_empty())
            .map(|l| l.to_vec())
            .collect())
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
    ///
    /// For a format-5 (full-history) branch the tip is appended to the
    /// `revision-history` list, so the new revision must be a linear child of
    /// the current tip; `revno` must equal the resulting line count. For 6/7/8
    /// the single `last-revision` line is rewritten.
    pub fn set_last_revision_info(
        &self,
        revno: u64,
        revision_id: &[u8],
    ) -> Result<(), BranchError> {
        if self.format.full_history {
            return self.set_last_revision_info_full_history(revno, revision_id);
        }
        self.with_write_lock(|| {
            let mut content = format!("{revno} ").into_bytes();
            content.extend_from_slice(revision_id);
            content.push(b'\n');
            self.transport.put_bytes("last-revision", &content, None)?;
            Ok(())
        })
    }

    fn set_last_revision_info_full_history(
        &self,
        revno: u64,
        revision_id: &[u8],
    ) -> Result<(), BranchError> {
        self.with_write_lock(|| {
            let mut history = self.revision_history()?;
            if revision_id == NULL_REVISION {
                history.clear();
            } else {
                // The common commit case extends the mainline by one. If revno
                // points before the current tip, truncate to it (an uncommit);
                // otherwise append.
                let target_len = revno as usize;
                if target_len <= history.len() {
                    history.truncate(target_len.saturating_sub(1));
                }
                history.push(revision_id.to_vec());
            }
            if history.len() as u64 != revno {
                return Err(BranchError::Corrupt(format!(
                    "revno {revno} does not match history length {}",
                    history.len()
                )));
            }
            self.write_revision_history(&history)
        })
    }

    /// Replace the full mainline (format 5), under the branch lock.
    pub fn set_revision_history(&self, history: &[Vec<u8>]) -> Result<(), BranchError> {
        self.with_write_lock(|| self.write_revision_history(history))
    }

    fn write_revision_history(&self, history: &[Vec<u8>]) -> Result<(), BranchError> {
        // Newline-separated revision ids, no trailing newline (the form brz
        // writes).
        let content = history.join(&b'\n');
        self.transport
            .put_bytes("revision-history", &content, None)?;
        Ok(())
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

    /// A format-5 (full-history) branch over a temp dir.
    #[cfg(any(feature = "weave", feature = "knit"))]
    fn branch_transport_format5() -> (tempfile::TempDir, Branch, Arc<LocalTransport>) {
        let dir = tempfile::tempdir().unwrap();
        let probe = Arc::new(LocalTransport::new(dir.path()));
        probe
            .put_bytes("format", b"Bazaar-NG branch format 5\n", None)
            .unwrap();
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

    #[cfg(any(feature = "weave", feature = "knit"))]
    #[test]
    fn format5_empty_branch_is_null_revision() {
        let (_d, branch, _probe) = branch_transport_format5();
        assert!(branch.format().full_history);
        assert_eq!(
            branch.last_revision_info().unwrap(),
            (0, NULL_REVISION.to_vec())
        );
        assert!(branch.revision_history().unwrap().is_empty());
    }

    #[cfg(any(feature = "weave", feature = "knit"))]
    #[test]
    fn format5_appends_to_revision_history() {
        let (_d, branch, probe) = branch_transport_format5();
        branch.set_last_revision_info(1, b"rev-1").unwrap();
        branch.set_last_revision_info(2, b"rev-2").unwrap();
        assert_eq!(branch.last_revision_info().unwrap(), (2, b"rev-2".to_vec()));
        assert_eq!(
            branch.revision_history().unwrap(),
            vec![b"rev-1".to_vec(), b"rev-2".to_vec()]
        );
        // Byte-for-byte the format brz writes: newline-separated, no trailer.
        assert_eq!(
            probe.get_bytes("revision-history").unwrap(),
            b"rev-1\nrev-2".to_vec()
        );
    }

    #[cfg(any(feature = "weave", feature = "knit"))]
    #[test]
    fn format5_set_revision_history_replaces() {
        let (_d, branch, _probe) = branch_transport_format5();
        branch
            .set_revision_history(&[b"a".to_vec(), b"b".to_vec(), b"c".to_vec()])
            .unwrap();
        assert_eq!(branch.last_revision_info().unwrap(), (3, b"c".to_vec()));
    }

    /// Setting the tip back to an earlier revno drops the later revisions
    /// (the uncommit case). Ported from breezy's per_branch
    /// `test_generate_revision_history`, which generates a shorter mainline.
    #[test]
    fn format5_set_last_revision_info_truncates() {
        let (_d, branch, _probe) = branch_transport_format5();
        branch
            .set_revision_history(&[b"a".to_vec(), b"b".to_vec(), b"c".to_vec()])
            .unwrap();
        // Point the tip back at revno 2 ("b"); "c" is dropped.
        branch.set_last_revision_info(2, b"b").unwrap();
        assert_eq!(branch.last_revision_info().unwrap(), (2, b"b".to_vec()));
        assert_eq!(
            branch.revision_history().unwrap(),
            vec![b"a".to_vec(), b"b".to_vec()]
        );
    }

    /// Setting the tip to the null revision empties the history. Ported from
    /// breezy's `test_generate_revision_history_NULL_REVISION`.
    #[test]
    fn format5_set_last_revision_info_null_empties_history() {
        let (_d, branch, _probe) = branch_transport_format5();
        branch.set_last_revision_info(1, b"rev-1").unwrap();
        branch.set_last_revision_info(0, NULL_REVISION).unwrap();
        assert_eq!(
            branch.last_revision_info().unwrap(),
            (0, NULL_REVISION.to_vec())
        );
        assert!(branch.revision_history().unwrap().is_empty());
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
