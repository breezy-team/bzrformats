//! Testaments: signable summaries of a revision.
//!
//! A testament is a deterministic, human-readable byte form of a revision
//! and its tree, designed so that two semantically equal revisions produce
//! byte-for-byte equal testaments. They are what bzr signs, rather than the
//! stored revision XML. Ported from `breezy.bzr.testament`.
//!
//! Three formats differ in their headers, whether they include the tree
//! root, and what per-entry detail they record:
//!
//! - [`TestamentFormat::V1`] - the original; no per-entry revision or
//!   executable bit, root excluded.
//! - [`TestamentFormat::Strict`] - bundle format 0.8; adds the per-entry
//!   revision and executable bit.
//! - [`TestamentFormat::Strict3`] - bundle format 0.9+; like `Strict` but
//!   includes the tree root (shown with path `.`).
//!
//! Unlike the Python class, this does not depend on a `Tree` object: the
//! caller passes the revision fields and the tree entries directly (built
//! from an inventory), keeping the module decoupled.

use std::collections::BTreeMap;

/// Which testament format to produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestamentFormat {
    /// `bazaar-ng testament version 1`.
    V1,
    /// `bazaar-ng testament version 2.1` (strict, bundle 0.8).
    Strict,
    /// `bazaar testament version 3 strict` (bundle 0.9+, includes root).
    Strict3,
}

impl TestamentFormat {
    fn long_header(self) -> &'static str {
        match self {
            TestamentFormat::V1 => "bazaar-ng testament version 1\n",
            TestamentFormat::Strict => "bazaar-ng testament version 2.1\n",
            TestamentFormat::Strict3 => "bazaar testament version 3 strict\n",
        }
    }

    fn short_header(self) -> &'static str {
        match self {
            TestamentFormat::V1 => "bazaar-ng testament short form 1\n",
            TestamentFormat::Strict => "bazaar-ng testament short form 2.1\n",
            TestamentFormat::Strict3 => "bazaar testament short form 3 strict\n",
        }
    }

    fn include_root(self) -> bool {
        matches!(self, TestamentFormat::Strict3)
    }

    fn strict(self) -> bool {
        matches!(self, TestamentFormat::Strict | TestamentFormat::Strict3)
    }
}

/// The kind of a tree entry, as it appears in a testament line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    File,
    Directory,
    Symlink,
    TreeReference,
}

impl EntryKind {
    fn as_str(self) -> &'static str {
        match self {
            EntryKind::File => "file",
            EntryKind::Directory => "directory",
            EntryKind::Symlink => "symlink",
            EntryKind::TreeReference => "tree-reference",
        }
    }
}

/// One tree entry contributing to a testament.
///
/// `path` is the tree-relative path (the root, when included, uses `.`).
/// `content` is the file's text sha1 (hex) for files, or the symlink
/// target for symlinks; it is ignored for other kinds. `revision` and
/// `executable` are only emitted by the strict formats.
#[derive(Debug, Clone)]
pub struct TestamentEntry {
    pub path: String,
    pub kind: EntryKind,
    pub file_id: Vec<u8>,
    /// File text sha1 (hex) or symlink target, depending on `kind`.
    pub content: Vec<u8>,
    pub revision: Vec<u8>,
    pub executable: bool,
}

/// Errors from building a testament.
#[derive(Debug, PartialEq, Eq)]
pub enum TestamentError {
    /// A field that must not contain whitespace did (revision id, file id,
    /// parent id, property name).
    WhitespaceNotAllowed(Vec<u8>),
    /// A field that must not contain line breaks did (committer, path).
    LinebreakNotAllowed(String),
    /// A file entry had no text sha1.
    MissingFileSha1(Vec<u8>),
    /// A symlink entry had no target.
    MissingSymlinkTarget(Vec<u8>),
}

impl std::fmt::Display for TestamentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TestamentError::WhitespaceNotAllowed(v) => {
                write!(
                    f,
                    "whitespace not allowed in {:?}",
                    String::from_utf8_lossy(v)
                )
            }
            TestamentError::LinebreakNotAllowed(s) => {
                write!(f, "line break not allowed in {s:?}")
            }
            TestamentError::MissingFileSha1(v) => {
                write!(f, "file {:?} has no text sha1", String::from_utf8_lossy(v))
            }
            TestamentError::MissingSymlinkTarget(v) => {
                write!(f, "symlink {:?} has no target", String::from_utf8_lossy(v))
            }
        }
    }
}

impl std::error::Error for TestamentError {}

/// A testament: the revision fields plus the tree entries that summarise it.
///
/// Entries must be supplied in the order the testament should list them
/// (the order `Tree.list_files` would yield). For [`TestamentFormat::Strict3`]
/// the caller includes a root entry (path `.`); for the other formats the
/// root must be omitted.
pub struct Testament {
    pub revision_id: Vec<u8>,
    pub committer: String,
    pub timestamp: i64,
    pub timezone: i32,
    pub message: String,
    pub parent_ids: Vec<Vec<u8>>,
    pub revprops: BTreeMap<String, String>,
    pub entries: Vec<TestamentEntry>,
}

impl Testament {
    /// The testament as a sequence of UTF-8 lines (each ending in `\n`).
    pub fn as_text_lines(&self, format: TestamentFormat) -> Result<Vec<Vec<u8>>, TestamentError> {
        if contains_whitespace_bytes(&self.revision_id) {
            return Err(TestamentError::WhitespaceNotAllowed(
                self.revision_id.clone(),
            ));
        }
        if crate::osutils::contains_linebreaks(&self.committer) {
            return Err(TestamentError::LinebreakNotAllowed(self.committer.clone()));
        }

        let mut r: Vec<String> = Vec::new();
        r.push(format.long_header().to_string());
        r.push(format!(
            "revision-id: {}\n",
            String::from_utf8_lossy(&self.revision_id)
        ));
        r.push(format!("committer: {}\n", self.committer));
        r.push(format!("timestamp: {}\n", self.timestamp));
        r.push(format!("timezone: {}\n", self.timezone));

        r.push("parents:\n".to_string());
        let mut parents = self.parent_ids.clone();
        parents.sort();
        for parent in &parents {
            if contains_whitespace_bytes(parent) {
                return Err(TestamentError::WhitespaceNotAllowed(parent.clone()));
            }
            r.push(format!("  {}\n", String::from_utf8_lossy(parent)));
        }

        r.push("message:\n".to_string());
        for line in splitlines(&self.message) {
            r.push(format!("  {line}\n"));
        }

        r.push("inventory:\n".to_string());
        for entry in &self.entries {
            r.push(self.entry_to_line(format, entry)?);
        }

        r.extend(self.revprops_to_lines()?);

        Ok(r.into_iter().map(String::into_bytes).collect())
    }

    fn entry_to_line(
        &self,
        format: TestamentFormat,
        entry: &TestamentEntry,
    ) -> Result<String, TestamentError> {
        if contains_whitespace_bytes(&entry.file_id) {
            return Err(TestamentError::WhitespaceNotAllowed(entry.file_id.clone()));
        }
        let (content, spacer) = match entry.kind {
            EntryKind::File => {
                if entry.content.is_empty() {
                    return Err(TestamentError::MissingFileSha1(entry.file_id.clone()));
                }
                (String::from_utf8_lossy(&entry.content).into_owned(), " ")
            }
            EntryKind::Symlink => {
                if entry.content.is_empty() {
                    return Err(TestamentError::MissingSymlinkTarget(entry.file_id.clone()));
                }
                (
                    escape_path(&String::from_utf8_lossy(&entry.content), format)?,
                    " ",
                )
            }
            _ => (String::new(), ""),
        };

        let mut line = format!(
            "  {} {} {}{}{}",
            entry.kind.as_str(),
            escape_path(&entry.path, format)?,
            String::from_utf8_lossy(&entry.file_id),
            spacer,
            content,
        );
        if format.strict() {
            line.push(' ');
            line.push_str(&String::from_utf8_lossy(&entry.revision));
            line.push_str(if entry.executable { " yes" } else { " no" });
        }
        line.push('\n');
        Ok(line)
    }

    fn revprops_to_lines(&self) -> Result<Vec<String>, TestamentError> {
        if self.revprops.is_empty() {
            return Ok(Vec::new());
        }
        let mut r = vec!["properties:\n".to_string()];
        // BTreeMap iterates in sorted key order, matching Python's sorted().
        for (name, value) in &self.revprops {
            if crate::osutils::contains_whitespace(name) {
                return Err(TestamentError::WhitespaceNotAllowed(
                    name.clone().into_bytes(),
                ));
            }
            r.push(format!("  {name}:\n"));
            for line in splitlines(value) {
                r.push(format!("    {line}\n"));
            }
        }
        Ok(r)
    }

    /// The full testament as a single UTF-8 byte string.
    pub fn as_text(&self, format: TestamentFormat) -> Result<Vec<u8>, TestamentError> {
        Ok(self.as_text_lines(format)?.concat())
    }

    /// The hex sha1 of the full testament text.
    pub fn as_sha1(&self, format: TestamentFormat) -> Result<Vec<u8>, TestamentError> {
        Ok(crate::weave::sha_strings(&self.as_text_lines(format)?))
    }

    /// The short, digest-based testament.
    pub fn as_short_text(&self, format: TestamentFormat) -> Result<Vec<u8>, TestamentError> {
        let sha1 = self.as_sha1(format)?;
        let mut out = format.short_header().as_bytes().to_vec();
        out.extend_from_slice(b"revision-id: ");
        out.extend_from_slice(&self.revision_id);
        out.push(b'\n');
        out.extend_from_slice(b"sha1: ");
        out.extend_from_slice(&sha1);
        out.push(b'\n');
        Ok(out)
    }
}

/// Escape a path for a testament line: `\` becomes `/`, spaces are
/// backslash-escaped, and (for strict3) an empty path becomes `.`.
fn escape_path(path: &str, format: TestamentFormat) -> Result<String, TestamentError> {
    if crate::osutils::contains_linebreaks(path) {
        return Err(TestamentError::LinebreakNotAllowed(path.to_string()));
    }
    let path = if format.include_root() && path.is_empty() {
        "."
    } else {
        path
    };
    Ok(path.replace('\\', "/").replace(' ', "\\ "))
}

/// Whether a byte string contains any whitespace character. Mirrors
/// `osutils.contains_whitespace` applied to bytes (revision/file/parent
/// ids are ascii in practice).
fn contains_whitespace_bytes(s: &[u8]) -> bool {
    s.iter()
        .any(|b| matches!(b, b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c))
}

/// Split a string into lines, matching Python's `str.splitlines()` for
/// the `\n`-delimited forms used here: an empty string yields no lines,
/// and a trailing newline does not produce a trailing empty line.
fn splitlines(s: &str) -> Vec<&str> {
    if s.is_empty() {
        return Vec::new();
    }
    let trimmed = s.strip_suffix('\n').unwrap_or(s);
    trimmed.split('\n').collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rev1() -> Testament {
        let mut revprops = BTreeMap::new();
        revprops.insert("branch-nick".to_string(), "test branch".to_string());
        Testament {
            revision_id: b"test@user-1".to_vec(),
            committer: "test@user".to_string(),
            timestamp: 1129025423,
            timezone: 0,
            message: "initial null commit".to_string(),
            parent_ids: Vec::new(),
            revprops,
            entries: Vec::new(),
        }
    }

    fn rev2() -> Testament {
        let mut revprops = BTreeMap::new();
        revprops.insert("branch-nick".to_string(), "test branch".to_string());
        Testament {
            revision_id: b"test@user-2".to_vec(),
            committer: "test@user".to_string(),
            timestamp: 1129025483,
            timezone: 36000,
            message: "add files and directories".to_string(),
            parent_ids: vec![b"test@user-1".to_vec()],
            revprops,
            entries: vec![
                TestamentEntry {
                    path: "hello".to_string(),
                    kind: EntryKind::File,
                    file_id: b"hello-id".to_vec(),
                    content: b"34dd0ac19a24bf80c4d33b5c8960196e8d8d1f73".to_vec(),
                    revision: b"test@user-2".to_vec(),
                    executable: true,
                },
                TestamentEntry {
                    path: "src".to_string(),
                    kind: EntryKind::Directory,
                    file_id: b"src-id".to_vec(),
                    content: Vec::new(),
                    revision: b"test@user-2".to_vec(),
                    executable: false,
                },
                TestamentEntry {
                    path: "src/foo.c".to_string(),
                    kind: EntryKind::File,
                    file_id: b"foo.c-id".to_vec(),
                    content: b"a2a049c20f908ae31b231d98779eb63c66448f24".to_vec(),
                    revision: b"test@user-2".to_vec(),
                    executable: false,
                },
            ],
        }
    }

    // Built with concat! so leading spaces are preserved (a `b"...\`
    // line continuation would strip them).
    const REV_1_V1: &[u8] = concat!(
        "bazaar-ng testament version 1\n",
        "revision-id: test@user-1\n",
        "committer: test@user\n",
        "timestamp: 1129025423\n",
        "timezone: 0\n",
        "parents:\n",
        "message:\n",
        "  initial null commit\n",
        "inventory:\n",
        "properties:\n",
        "  branch-nick:\n",
        "    test branch\n",
    )
    .as_bytes();

    const REV_2_V1: &[u8] = concat!(
        "bazaar-ng testament version 1\n",
        "revision-id: test@user-2\n",
        "committer: test@user\n",
        "timestamp: 1129025483\n",
        "timezone: 36000\n",
        "parents:\n",
        "  test@user-1\n",
        "message:\n",
        "  add files and directories\n",
        "inventory:\n",
        "  file hello hello-id 34dd0ac19a24bf80c4d33b5c8960196e8d8d1f73\n",
        "  directory src src-id\n",
        "  file src/foo.c foo.c-id a2a049c20f908ae31b231d98779eb63c66448f24\n",
        "properties:\n",
        "  branch-nick:\n",
        "    test branch\n",
    )
    .as_bytes();

    const REV_2_STRICT: &[u8] = concat!(
        "bazaar-ng testament version 2.1\n",
        "revision-id: test@user-2\n",
        "committer: test@user\n",
        "timestamp: 1129025483\n",
        "timezone: 36000\n",
        "parents:\n",
        "  test@user-1\n",
        "message:\n",
        "  add files and directories\n",
        "inventory:\n",
        "  file hello hello-id 34dd0ac19a24bf80c4d33b5c8960196e8d8d1f73 test@user-2 yes\n",
        "  directory src src-id test@user-2 no\n",
        "  file src/foo.c foo.c-id a2a049c20f908ae31b231d98779eb63c66448f24 test@user-2 no\n",
        "properties:\n",
        "  branch-nick:\n",
        "    test branch\n",
    )
    .as_bytes();

    #[test]
    fn rev1_v1_matches_breezy() {
        assert_eq!(rev1().as_text(TestamentFormat::V1).unwrap(), REV_1_V1);
    }

    #[test]
    fn rev2_v1_matches_breezy() {
        assert_eq!(rev2().as_text(TestamentFormat::V1).unwrap(), REV_2_V1);
    }

    #[test]
    fn rev2_strict_matches_breezy() {
        assert_eq!(
            rev2().as_text(TestamentFormat::Strict).unwrap(),
            REV_2_STRICT
        );
    }

    #[test]
    fn strict3_includes_root() {
        // Strict3 prepends the root entry (path ".").
        let mut t = rev2();
        t.entries.insert(
            0,
            TestamentEntry {
                path: String::new(),
                kind: EntryKind::Directory,
                file_id: b"TREE_ROT".to_vec(),
                content: Vec::new(),
                revision: b"test@user-1".to_vec(),
                executable: false,
            },
        );
        let text = String::from_utf8(t.as_text(TestamentFormat::Strict3).unwrap()).unwrap();
        assert!(text.starts_with("bazaar testament version 3 strict\n"));
        assert!(text.contains("  directory . TREE_ROT test@user-1 no\n"));
    }

    #[test]
    fn short_form_is_header_plus_sha() {
        let t = rev1();
        let sha = t.as_sha1(TestamentFormat::V1).unwrap();
        let mut expected =
            b"bazaar-ng testament short form 1\nrevision-id: test@user-1\nsha1: ".to_vec();
        expected.extend_from_slice(&sha);
        expected.push(b'\n');
        assert_eq!(t.as_short_text(TestamentFormat::V1).unwrap(), expected);
    }

    #[test]
    fn whitespace_in_revision_id_rejected() {
        let mut t = rev1();
        t.revision_id = b"bad id".to_vec();
        assert!(matches!(
            t.as_text_lines(TestamentFormat::V1),
            Err(TestamentError::WhitespaceNotAllowed(_))
        ));
    }
}
