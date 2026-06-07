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
        supports_reference_locations: true,
        supported: true,
    }
}

declare_branch_format! {
    FORMAT_7 {
        format_string: b"Bazaar Branch Format 7 (needs bzr 1.6)\n",
        description: "Branch format 7 (stackable)",
        supports_tags: true,
        supports_stacking: true,
        supports_reference_locations: true,
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
    /// A config file could not be parsed.
    Config(crate::config::ConfigError),
    /// The branch is not stacked (a stackable format with no `stacked_on_location`).
    NotStacked,
    /// The branch format does not support stacking (formats 5 and 6).
    Unstackable,
    /// An operation is not supported by this branch format (e.g. reference
    /// locations on format 5).
    Unsupported(String),
}

impl std::fmt::Display for BranchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BranchError::Corrupt(m) => write!(f, "corrupt branch data: {m}"),
            BranchError::Lock(e) => write!(f, "branch lock error: {e}"),
            BranchError::Transport(e) => write!(f, "transport error: {e}"),
            BranchError::Config(e) => write!(f, "config error: {e}"),
            BranchError::NotStacked => write!(f, "branch is not stacked"),
            BranchError::Unstackable => write!(f, "branch format does not support stacking"),
            BranchError::Unsupported(op) => write!(f, "unsupported branch operation: {op}"),
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

impl From<crate::config::ConfigError> for BranchError {
    fn from(e: crate::config::ConfigError) -> Self {
        BranchError::Config(e)
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

    // --- Config-backed location options (branch.conf no-name section) ---

    /// Read a config location option from `branch.conf` only.
    ///
    /// Mirrors breezy's `_get_config_location`: the empty string is the
    /// on-disk representation of "unset" and is normalized to `None`. Only the
    /// branch's own `branch.conf` is consulted (the `BranchOnlyStack`), not the
    /// wider locations.conf/bazaar.conf stack, so a value is never inherited.
    fn get_config_location(&self, name: &str) -> Result<Option<String>, BranchError> {
        let bytes = self.get_config_bytes()?;
        let mut store = crate::config::IniFileStore::new();
        store.load_from_bytes(&bytes)?;
        let value = store
            .get_sections()
            .into_iter()
            .find(|s| s.id().is_none())
            .and_then(|s| s.get(name).map(|v| store.unquote(v)));
        Ok(value.filter(|v| !v.is_empty()))
    }

    /// Write a config location option into `branch.conf`'s no-name section,
    /// under the branch lock. `value == None` (or empty) stores the empty
    /// string, matching breezy's "unset" sentinel.
    fn set_config_location(&self, name: &str, value: Option<&str>) -> Result<(), BranchError> {
        self.with_write_lock(|| {
            let bytes = self.get_config_bytes()?;
            let mut store = crate::config::IniFileStore::new();
            store.load_from_bytes(&bytes)?;
            let mut section = store.get_mutable_section(None);
            section.set(name, &store.quote(value.unwrap_or("")));
            store.apply_changes(&section);
            self.transport
                .put_bytes("branch.conf", &store.to_bytes(), None)?;
            Ok(())
        })
    }

    /// Read a boolean config option from `branch.conf`'s no-name section.
    fn get_config_bool(&self, name: &str) -> Result<Option<bool>, BranchError> {
        match self.get_config_location(name)? {
            Some(v) => Ok(crate::config::bool_from_store(&v)),
            None => Ok(None),
        }
    }

    // --- Stacking (formats 7 and 8) ---

    /// The URL this branch is stacked on, or `None` if it is a stackable
    /// branch that is not currently stacked.
    ///
    /// Reads `stacked_on_location` from `branch.conf`. Returns
    /// [`BranchError::Unstackable`] for formats that do not support stacking
    /// (5 and 6) and [`BranchError::NotStacked`] for a stackable format with no
    /// configured location -- matching breezy's `UnstackableBranchFormat` vs
    /// `NotStacked` split.
    pub fn get_stacked_on_url(&self) -> Result<String, BranchError> {
        if !self.format.supports_stacking {
            return Err(BranchError::Unstackable);
        }
        self.get_config_location("stacked_on_location")?
            .ok_or(BranchError::NotStacked)
    }

    /// Set (or clear, with `None`) the URL this branch is stacked on.
    ///
    /// Errors with [`BranchError::Unstackable`] on a non-stackable format. The
    /// value is written to `branch.conf`; wiring the fallback repository is the
    /// caller's job (see [`crate::bzrdir`] open paths).
    pub fn set_stacked_on_url(&self, url: Option<&str>) -> Result<(), BranchError> {
        if !self.format.supports_stacking {
            return Err(BranchError::Unstackable);
        }
        self.set_config_location("stacked_on_location", url)
    }

    // --- Bound branches (all formats) ---

    /// The master branch URL this branch is bound to, or `None` if unbound.
    ///
    /// Format 5 stores the master in a plain `bound` file; formats 6/7/8 store
    /// a `bound` boolean plus a `bound_location` key in `branch.conf`, and the
    /// location only counts when `bound` is true.
    pub fn get_bound_location(&self) -> Result<Option<String>, BranchError> {
        if self.format.full_history {
            return self.read_bound_file();
        }
        match self.get_config_bool("bound")? {
            Some(true) => self.get_config_location("bound_location"),
            _ => Ok(None),
        }
    }

    /// The previous master URL after an unbind, or `None`.
    ///
    /// For formats 6/7/8 this is `bound_location` when `bound` is false; format
    /// 5 does not keep an old location (returns `None`).
    pub fn get_old_bound_location(&self) -> Result<Option<String>, BranchError> {
        if self.format.full_history {
            return Ok(None);
        }
        match self.get_config_bool("bound")? {
            Some(false) | None => self.get_config_location("bound_location"),
            Some(true) => Ok(None),
        }
    }

    /// Bind this branch to `location` (its new master), or unbind with `None`.
    pub fn set_bound_location(&self, location: Option<&str>) -> Result<(), BranchError> {
        if self.format.full_history {
            return self.write_bound_file(location);
        }
        match location {
            None => self.set_config_bool("bound", false),
            Some(loc) => {
                self.set_config_location("bound_location", Some(loc))?;
                self.set_config_bool("bound", true)
            }
        }
    }

    /// Bind to `other_url`. Equivalent to `set_bound_location(Some(url))`.
    pub fn bind(&self, other_url: &str) -> Result<(), BranchError> {
        self.set_bound_location(Some(other_url))
    }

    /// Unbind. Equivalent to `set_bound_location(None)`.
    pub fn unbind(&self) -> Result<(), BranchError> {
        self.set_bound_location(None)
    }

    /// Read the format-5 `bound` file (UTF-8 URL, trailing newline stripped).
    fn read_bound_file(&self) -> Result<Option<String>, BranchError> {
        match self.transport.get_bytes("bound") {
            Ok(b) => {
                let s = b.strip_suffix(b"\n").unwrap_or(&b);
                let url = String::from_utf8(s.to_vec())
                    .map_err(|_| BranchError::Corrupt("bound file not utf-8".to_string()))?;
                Ok(Some(url))
            }
            Err(TransportError::NoSuchFile(_)) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Write or delete the format-5 `bound` file, under the branch lock.
    fn write_bound_file(&self, location: Option<&str>) -> Result<(), BranchError> {
        self.with_write_lock(|| match location {
            Some(loc) => {
                let mut content = loc.as_bytes().to_vec();
                content.push(b'\n');
                self.transport.put_bytes("bound", &content, None)?;
                Ok(())
            }
            None => match self.transport.delete("bound") {
                Ok(()) | Err(TransportError::NoSuchFile(_)) => Ok(()),
                Err(e) => Err(e.into()),
            },
        })
    }

    /// Set a boolean config option in `branch.conf`'s no-name section.
    fn set_config_bool(&self, name: &str, value: bool) -> Result<(), BranchError> {
        self.set_config_location(name, Some(if value { "True" } else { "False" }))
    }

    // --- Reference locations (formats 6, 7, 8) ---

    /// The `(branch_location, tree_path)` recorded for a tree-reference
    /// `file_id`, or `(None, None)` if none.
    ///
    /// Reads the `references` RIO file. Errors with [`BranchError::Unsupported`]
    /// for format 5, which has no reference locations.
    pub fn get_reference_info(
        &self,
        file_id: &[u8],
    ) -> Result<(Option<String>, Option<String>), BranchError> {
        if !self.format.supports_reference_locations {
            return Err(BranchError::Unsupported("reference locations".to_string()));
        }
        let info = self.read_all_reference_info()?;
        Ok(info
            .get(file_id)
            .cloned()
            .map(|(b, t)| (Some(b), t))
            .unwrap_or((None, None)))
    }

    /// Record (or, with `branch_location == None`, delete) the reference
    /// location for a tree-reference `file_id`, under the branch lock.
    ///
    /// On a format-7 branch this upgrades the `format` marker to format 8, as
    /// breezy does (the "white lie": format 7 advertises reference support but
    /// rewrites itself to 8 the moment a reference is stored).
    pub fn set_reference_info(
        &self,
        file_id: &[u8],
        branch_location: Option<&str>,
        tree_path: Option<&str>,
    ) -> Result<(), BranchError> {
        if !self.format.supports_reference_locations {
            return Err(BranchError::Unsupported("reference locations".to_string()));
        }
        self.with_write_lock(|| {
            let mut info = self.read_all_reference_info()?;
            match branch_location {
                None => {
                    info.remove(file_id);
                }
                Some(loc) => {
                    info.insert(
                        file_id.to_vec(),
                        (loc.to_string(), tree_path.map(|s| s.to_string())),
                    );
                }
            }
            self.write_all_reference_info(&info)?;
            // Format 7 upgrades to format 8 on first reference write.
            if self.format.format_string == FORMAT_7.format_string {
                self.transport
                    .put_bytes("format", FORMAT_8.format_string, None)?;
            }
            Ok(())
        })
    }

    /// Parse the `references` RIO file into `{file_id: (branch_location,
    /// tree_path)}`. A missing file is an empty map.
    fn read_all_reference_info(
        &self,
    ) -> Result<BTreeMap<Vec<u8>, (String, Option<String>)>, BranchError> {
        let bytes = match self.transport.get_bytes("references") {
            Ok(b) => b,
            Err(TransportError::NoSuchFile(_)) => return Ok(BTreeMap::new()),
            Err(e) => return Err(e.into()),
        };
        let mut reader = std::io::BufReader::new(&bytes[..]);
        let stanzas = crate::rio::read_stanzas(&mut reader)
            .map_err(|e| BranchError::Corrupt(format!("references rio: {e:?}")))?;
        let mut out = BTreeMap::new();
        for stanza in stanzas {
            let file_id = stanza_string(&stanza, "file_id")
                .ok_or_else(|| BranchError::Corrupt("references stanza has no file_id".into()))?;
            let branch_location = stanza_string(&stanza, "branch_location").ok_or_else(|| {
                BranchError::Corrupt("references stanza has no branch_location".into())
            })?;
            let tree_path = stanza_string(&stanza, "tree_path");
            out.insert(file_id.into_bytes(), (branch_location, tree_path));
        }
        Ok(out)
    }

    /// Serialize the reference info back to the `references` RIO file.
    fn write_all_reference_info(
        &self,
        info: &BTreeMap<Vec<u8>, (String, Option<String>)>,
    ) -> Result<(), BranchError> {
        use crate::rio::{Stanza, StanzaValue};
        let mut out = Vec::new();
        for (file_id, (branch_location, tree_path)) in info {
            let mut stanza = Stanza::new();
            let file_id = String::from_utf8(file_id.clone())
                .map_err(|_| BranchError::Corrupt("reference file_id not utf-8".to_string()))?;
            stanza
                .add("file_id".to_string(), StanzaValue::String(file_id))
                .and_then(|_| {
                    stanza.add(
                        "branch_location".to_string(),
                        StanzaValue::String(branch_location.clone()),
                    )
                })
                .map_err(|e| BranchError::Corrupt(format!("references rio: {e:?}")))?;
            if let Some(tree_path) = tree_path {
                stanza
                    .add(
                        "tree_path".to_string(),
                        StanzaValue::String(tree_path.clone()),
                    )
                    .map_err(|e| BranchError::Corrupt(format!("references rio: {e:?}")))?;
            }
            out.extend(stanza.to_bytes());
        }
        self.transport.put_bytes("references", &out, None)?;
        Ok(())
    }

    // --- Branch reference format (lightweight checkouts) ---

    /// The URL a branch-reference points at, or `None` if this is not a branch
    /// reference.
    ///
    /// A branch of `REFERENCE_FORMAT_1` stores the referenced branch's URL in a
    /// `location` file (UTF-8, no trailing newline). For any other format this
    /// returns `None`, matching breezy's `BranchFormat.get_reference` default.
    pub fn get_reference(&self) -> Result<Option<String>, BranchError> {
        if !self.format.is_reference {
            return Ok(None);
        }
        match self.transport.get_bytes("location") {
            Ok(b) => {
                let url = String::from_utf8(b)
                    .map_err(|_| BranchError::Corrupt("location file not utf-8".to_string()))?;
                Ok(Some(url))
            }
            Err(TransportError::NoSuchFile(_)) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Point this branch reference at `to_url` (written verbatim as UTF-8).
    ///
    /// Errors with [`BranchError::Unsupported`] on a non-reference format,
    /// matching breezy where only `BranchReferenceFormat` implements
    /// `set_reference`.
    pub fn set_reference(&self, to_url: &str) -> Result<(), BranchError> {
        if !self.format.is_reference {
            return Err(BranchError::Unsupported("branch reference".to_string()));
        }
        self.transport
            .put_bytes("location", to_url.as_bytes(), None)?;
        Ok(())
    }
}

/// Pull a single string value out of a RIO stanza by tag.
fn stanza_string(stanza: &crate::rio::Stanza, tag: &str) -> Option<String> {
    match stanza.get(tag) {
        Some(crate::rio::StanzaValue::String(s)) => Some(s.clone()),
        _ => None,
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

    /// A non-ASCII tag name round-trips. Ported from breezy's per_branch
    /// test_tags.test_delete_tag, which uses a Greek alpha tag name.
    #[test]
    fn tags_unicode_name_round_trips() {
        let (_d, branch, _probe) = branch_transport();
        let mut tags = BTreeMap::new();
        tags.insert("\u{3b1}".to_string(), b"rev-1".to_vec());
        branch.set_tags(&tags).unwrap();
        // Re-open the branch from the same transport and read the tag back.
        let reopened = Branch::new(branch.transport.clone());
        assert_eq!(reopened.tags().unwrap(), tags);
    }

    /// Removing a tag means re-writing the map without it; the deleted tag is
    /// then absent on disk. Ported from test_tags.test_delete_tag (adapted to
    /// our whole-map tag API).
    #[test]
    fn tags_delete_removes_from_map() {
        let (_d, branch, _probe) = branch_transport();
        let mut tags = BTreeMap::new();
        tags.insert("keep".to_string(), b"rev-1".to_vec());
        tags.insert("drop".to_string(), b"rev-2".to_vec());
        branch.set_tags(&tags).unwrap();

        tags.remove("drop");
        branch.set_tags(&tags).unwrap();
        assert_eq!(branch.tags().unwrap(), tags);
        assert!(!branch.tags().unwrap().contains_key("drop"));
    }

    /// A tag whose target revision does not exist still stores and reads back;
    /// the branch performs no existence check. Ported from
    /// test_tags.test_ghost_tag.
    #[test]
    fn tags_ghost_target_is_stored() {
        let (_d, branch, _probe) = branch_transport();
        let mut tags = BTreeMap::new();
        tags.insert("ghost".to_string(), b"idontexist".to_vec());
        branch.set_tags(&tags).unwrap();
        assert_eq!(
            branch.tags().unwrap().get("ghost").map(|v| v.as_slice()),
            Some(&b"idontexist"[..])
        );
    }

    /// get_config_bytes returns branch.conf verbatim, and an empty vec when
    /// the file is absent. Ported from per_branch/test_config.py's basic
    /// get/set config round-trip.
    #[test]
    fn get_config_bytes_reads_branch_conf() {
        let (_d, branch, probe) = branch_transport();
        // No branch.conf yet -> empty.
        assert!(branch.get_config_bytes().unwrap().is_empty());

        let body = b"[DEFAULT]\nnickname = trunk\n";
        probe.put_bytes("branch.conf", body, None).unwrap();
        assert_eq!(branch.get_config_bytes().unwrap(), body);
    }

    /// A branch opened as a specific format over a temp dir, plus a probe.
    fn branch_transport_format(
        format: &'static BranchFormat,
    ) -> (tempfile::TempDir, Branch, Arc<LocalTransport>) {
        let dir = tempfile::tempdir().unwrap();
        let probe = Arc::new(LocalTransport::new(dir.path()));
        probe
            .put_bytes("format", format.format_string, None)
            .unwrap();
        let shared: SharedTransport = Arc::new(LocalTransport::new(dir.path()));
        (dir, Branch::new(shared), probe)
    }

    // --- Stacking ---

    #[test]
    fn not_stacked_by_default_on_format_7() {
        let (_d, branch, _p) = branch_transport();
        assert!(matches!(
            branch.get_stacked_on_url(),
            Err(BranchError::NotStacked)
        ));
    }

    #[test]
    fn stacked_on_url_round_trips() {
        let (_d, branch, probe) = branch_transport();
        branch.set_stacked_on_url(Some("../parent")).unwrap();
        assert_eq!(branch.get_stacked_on_url().unwrap(), "../parent");
        // Stored as a branch.conf no-name key.
        let conf = String::from_utf8(probe.get_bytes("branch.conf").unwrap()).unwrap();
        assert!(conf.contains("stacked_on_location = ../parent"), "{conf}");
    }

    #[test]
    fn clearing_stacked_on_url_makes_it_not_stacked() {
        let (_d, branch, _p) = branch_transport();
        branch.set_stacked_on_url(Some("../parent")).unwrap();
        branch.set_stacked_on_url(None).unwrap();
        assert!(matches!(
            branch.get_stacked_on_url(),
            Err(BranchError::NotStacked)
        ));
    }

    #[test]
    fn format_6_is_unstackable() {
        let (_d, branch, _p) = branch_transport_format(&FORMAT_6);
        assert!(matches!(
            branch.get_stacked_on_url(),
            Err(BranchError::Unstackable)
        ));
        assert!(matches!(
            branch.set_stacked_on_url(Some("x")),
            Err(BranchError::Unstackable)
        ));
    }

    // --- Bound branches (formats 6/7/8: config-based) ---

    #[test]
    fn unbound_by_default() {
        let (_d, branch, _p) = branch_transport();
        assert_eq!(branch.get_bound_location().unwrap(), None);
    }

    #[test]
    fn bind_then_get_bound_location() {
        let (_d, branch, _p) = branch_transport();
        branch.bind("http://example.com/master").unwrap();
        assert_eq!(
            branch.get_bound_location().unwrap().as_deref(),
            Some("http://example.com/master")
        );
    }

    #[test]
    fn unbind_clears_bound_but_keeps_old_location() {
        let (_d, branch, _p) = branch_transport();
        branch.bind("http://example.com/master").unwrap();
        branch.unbind().unwrap();
        assert_eq!(branch.get_bound_location().unwrap(), None);
        assert_eq!(
            branch.get_old_bound_location().unwrap().as_deref(),
            Some("http://example.com/master")
        );
    }

    // --- Bound branches (format 5: file-based) ---

    #[cfg(any(feature = "weave", feature = "knit"))]
    #[test]
    fn format_5_bound_uses_bound_file() {
        let (_d, branch, probe) = branch_transport_format5();
        assert_eq!(branch.get_bound_location().unwrap(), None);
        branch.bind("/srv/master").unwrap();
        assert_eq!(probe.get_bytes("bound").unwrap(), b"/srv/master\n");
        assert_eq!(
            branch.get_bound_location().unwrap().as_deref(),
            Some("/srv/master")
        );
        branch.unbind().unwrap();
        assert_eq!(branch.get_bound_location().unwrap(), None);
        assert!(matches!(
            probe.get_bytes("bound"),
            Err(TransportError::NoSuchFile(_))
        ));
    }

    // --- Reference locations ---

    #[test]
    fn reference_info_round_trips_on_format_8() {
        let (_d, branch, _p) = branch_transport_format(&FORMAT_8);
        assert_eq!(branch.get_reference_info(b"file-1").unwrap(), (None, None));
        branch
            .set_reference_info(b"file-1", Some("../subtree"), Some("sub/dir"))
            .unwrap();
        assert_eq!(
            branch.get_reference_info(b"file-1").unwrap(),
            (Some("../subtree".to_string()), Some("sub/dir".to_string()))
        );
    }

    #[test]
    fn setting_reference_upgrades_format_7_to_8() {
        let (_d, branch, probe) = branch_transport_format(&FORMAT_7);
        branch
            .set_reference_info(b"file-1", Some("../subtree"), None)
            .unwrap();
        // The format marker is now format 8.
        assert_eq!(probe.get_bytes("format").unwrap(), FORMAT_8.format_string);
    }

    #[test]
    fn deleting_reference_info() {
        let (_d, branch, _p) = branch_transport_format(&FORMAT_8);
        branch
            .set_reference_info(b"file-1", Some("../subtree"), None)
            .unwrap();
        branch.set_reference_info(b"file-1", None, None).unwrap();
        assert_eq!(branch.get_reference_info(b"file-1").unwrap(), (None, None));
    }

    #[cfg(any(feature = "weave", feature = "knit"))]
    #[test]
    fn reference_info_unsupported_on_format_5() {
        let (_d, branch, _p) = branch_transport_format5();
        assert!(matches!(
            branch.get_reference_info(b"file-1"),
            Err(BranchError::Unsupported(_))
        ));
    }

    // --- Branch reference format ---

    #[test]
    fn get_reference_is_none_on_normal_branch() {
        let (_d, branch, _p) = branch_transport();
        assert_eq!(branch.get_reference().unwrap(), None);
        assert!(matches!(
            branch.set_reference("x"),
            Err(BranchError::Unsupported(_))
        ));
    }

    #[test]
    fn reference_round_trips() {
        let (_d, branch, probe) = branch_transport_format(&REFERENCE_FORMAT_1);
        assert_eq!(branch.get_reference().unwrap(), None);
        branch.set_reference("file:///srv/real").unwrap();
        assert_eq!(
            branch.get_reference().unwrap().as_deref(),
            Some("file:///srv/real")
        );
        // Stored verbatim in the `location` file, no trailing newline.
        assert_eq!(probe.get_bytes("location").unwrap(), b"file:///srv/real");
    }
}
