//! Text-based inventory format (`# bzr inventory format 3`).
//!
//! A simple line-oriented serialisation of an inventory, ported from
//! `breezy.bzr.textinv`. Each non-root entry is one line of
//! space-separated fields, with values escaped so they never contain a
//! literal space (so a line can be parsed by splitting on spaces).
//!
//! As in breezy, only writing is implemented: the reader there never
//! reconstructed entries (its `inv.add` was a no-op), so a faithful port
//! provides the escape helpers and the writer, and leaves reading to a
//! caller that knows the inventory entry types it wants to build.

/// First line of a serialised text inventory.
pub const START_MARK: &[u8] = b"# bzr inventory format 3\n";
/// Last line of a serialised text inventory.
pub const END_MARK: &[u8] = b"# end of inventory\n";

/// URL-like escape so a value never contains a space (or other separator):
/// `\`, space, tab and newline become `\xNN` forms.
pub fn escape(s: &str) -> String {
    s.replace('\\', "\\x5c")
        .replace(' ', "\\x20")
        .replace('\t', "\\x09")
        .replace('\n', "\\x0a")
}

/// Inverse of [`escape`]. The input must not contain a literal space.
pub fn unescape(s: &str) -> Option<String> {
    if s.contains(' ') {
        return None;
    }
    Some(
        s.replace("\\x20", " ")
            .replace("\\x09", "\t")
            .replace("\\x0a", "\n")
            .replace("\\x5c", "\\"),
    )
}

/// One non-root entry to serialise. `text_*` are only used for files.
#[derive(Debug, Clone)]
pub struct TextInvEntry {
    pub file_id: Vec<u8>,
    pub name: String,
    /// `"file"`, `"directory"`, `"symlink"`, etc.
    pub kind: String,
    pub parent_id: Vec<u8>,
    /// File text id, sha1 (hex) and size, present only for files.
    pub file_details: Option<FileDetails>,
}

/// The extra fields a file entry carries.
#[derive(Debug, Clone)]
pub struct FileDetails {
    pub text_id: Vec<u8>,
    pub text_sha1: Vec<u8>,
    pub text_size: u64,
}

/// Serialise `entries` (already in iteration order, root excluded) as a
/// text inventory.
pub fn write_text_inventory(entries: &[TextInvEntry]) -> Vec<u8> {
    let mut out = START_MARK.to_vec();
    for e in entries {
        out.extend_from_slice(&e.file_id);
        out.push(b' ');
        out.extend_from_slice(escape(&e.name).as_bytes());
        out.push(b' ');
        out.extend_from_slice(e.kind.as_bytes());
        out.push(b' ');
        out.extend_from_slice(&e.parent_id);
        if e.kind == "file" {
            if let Some(d) = &e.file_details {
                out.push(b' ');
                out.extend_from_slice(&d.text_id);
                out.push(b' ');
                out.extend_from_slice(&d.text_sha1);
                out.push(b' ');
                out.extend_from_slice(d.text_size.to_string().as_bytes());
            }
        }
        out.push(b'\n');
    }
    out.extend_from_slice(END_MARK);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escape_round_trips() {
        for s in [
            "plain",
            "with space",
            "tab\there",
            "back\\slash",
            "new\nline",
        ] {
            let e = escape(s);
            assert!(!e.contains(' '));
            assert_eq!(unescape(&e).unwrap(), s);
        }
    }

    #[test]
    fn write_a_small_inventory() {
        let entries = vec![
            TextInvEntry {
                file_id: b"dir-id".to_vec(),
                name: "a dir".to_string(),
                kind: "directory".to_string(),
                parent_id: b"TREE_ROOT".to_vec(),
                file_details: None,
            },
            TextInvEntry {
                file_id: b"file-id".to_vec(),
                name: "hello.txt".to_string(),
                kind: "file".to_string(),
                parent_id: b"dir-id".to_vec(),
                file_details: Some(FileDetails {
                    text_id: b"hello-text".to_vec(),
                    text_sha1: b"deadbeef".to_vec(),
                    text_size: 12,
                }),
            },
        ];
        let out = write_text_inventory(&entries);
        let expected = b"# bzr inventory format 3\n\
dir-id a\\x20dir directory TREE_ROOT\n\
file-id hello.txt file dir-id hello-text deadbeef 12\n\
# end of inventory\n";
        assert_eq!(out, expected);
    }
}
