//! The `views` working-tree control file (`Bazaar views format 1`).
//!
//! Views scope a working tree to a subset of its paths. They are stored in an
//! unversioned control file whose top line is a format marker:
//!
//! ```text
//! Bazaar views format X
//! ```
//!
//! where X is an integer. Only version 1 is supported, and is stored as
//! optional `name=value` keyword lines, then an optional `views:` line
//! followed by one view per line as nul-separated `name\0path1\0path2`. The
//! file is UTF-8. The only keyword with a defined meaning is `current`, naming
//! the enabled view.

use indexmap::IndexMap;
use std::collections::BTreeMap;

/// The marker prefix shared by every format version; the version number and a
/// newline follow it.
const MARKER_PREFIX: &[u8] = b"Bazaar views format ";
/// The only format version this module reads and writes.
const FORMAT_VERSION: &[u8] = b"1";

/// Keyword lines from a views file, in file order. Insertion order is
/// preserved so that reading a file and writing it back is byte-identical,
/// including any keywords this version does not interpret.
pub type Keywords = IndexMap<String, String>;
/// View definitions from a views file: view name to its list of paths.
pub type ViewDict = BTreeMap<String, Vec<String>>;

/// An error parsing the contents of a views file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ViewsError {
    /// The top line was not a `Bazaar views format X` marker.
    MissingMarker,
    /// The marker named a format version this crate cannot decode.
    UnsupportedFormat(String),
    /// The file was not valid UTF-8.
    NotUtf8,
    /// A line before the `views:` section was neither a keyword nor `views:`.
    UnparsableLine(String),
}

impl std::fmt::Display for ViewsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ViewsError::MissingMarker => {
                write!(f, "format marker missing from top of views file")
            }
            ViewsError::UnsupportedFormat(v) => write!(f, "cannot decode views format {v}"),
            ViewsError::NotUtf8 => write!(f, "views file is not valid utf-8"),
            ViewsError::UnparsableLine(line) => {
                write!(f, "failed to deserialize views line {line}")
            }
        }
    }
}

impl std::error::Error for ViewsError {}

/// Serialise keywords and view definitions into views file content.
///
/// An empty definition (no keywords, no views) still gets the format marker;
/// callers that want breezy's "empty file means no views" shorthand should
/// check for that case themselves before writing.
pub fn serialize(keywords: &Keywords, views: &ViewDict) -> Vec<u8> {
    let mut out = MARKER_PREFIX.to_vec();
    out.extend_from_slice(FORMAT_VERSION);
    out.push(b'\n');
    for (key, value) in keywords {
        out.extend_from_slice(format!("{key}={value}\n").as_bytes());
    }
    if !views.is_empty() {
        out.extend_from_slice(b"views:\n");
        // BTreeMap iterates in name order, matching breezy's sorted().
        for (name, paths) in views {
            let mut line = name.clone();
            for path in paths {
                line.push('\0');
                line.push_str(path);
            }
            line.push('\n');
            out.extend_from_slice(line.as_bytes());
        }
    }
    out
}

/// Parse views file content into its keywords and view definitions.
///
/// As a special case (matching breezy), empty content deserialises to no
/// keywords and no views, so that an empty file needs no initialisation.
pub fn deserialize(content: &[u8]) -> Result<(Keywords, ViewDict), ViewsError> {
    if content.is_empty() {
        return Ok((Keywords::new(), ViewDict::new()));
    }
    let text = std::str::from_utf8(content).map_err(|_| ViewsError::NotUtf8)?;
    // Mirror str.splitlines(): a single trailing newline yields no extra empty
    // line, but a blank line in the middle is a line (and an unparsable one).
    let body = text.strip_suffix('\n').unwrap_or(text);
    let mut lines = body.split('\n');
    let first = lines.next().unwrap_or("");
    check_marker(first.as_bytes())?;

    let mut keywords = Keywords::new();
    let mut views = ViewDict::new();
    let mut in_views = false;
    for line in lines {
        if in_views {
            let mut parts = line.split('\0');
            let name = parts.next().unwrap_or("").to_string();
            views.insert(name, parts.map(str::to_string).collect());
        } else if line == "views:" {
            in_views = true;
        } else if let Some((key, value)) = line.split_once('=') {
            keywords.insert(key.to_string(), value.to_string());
        } else {
            return Err(ViewsError::UnparsableLine(line.to_string()));
        }
    }
    Ok((keywords, views))
}

/// Validate a views file's format marker line (without its newline).
fn check_marker(line: &[u8]) -> Result<(), ViewsError> {
    let Some(version) = line.strip_prefix(MARKER_PREFIX) else {
        return Err(ViewsError::MissingMarker);
    };
    // The marker regex is unanchored at the end, so trailing text after the
    // digits is tolerated, but there must be at least one digit.
    let digits: Vec<u8> = version
        .iter()
        .copied()
        .take_while(u8::is_ascii_digit)
        .collect();
    if digits.is_empty() {
        return Err(ViewsError::MissingMarker);
    }
    if digits != FORMAT_VERSION {
        return Err(ViewsError::UnsupportedFormat(
            String::from_utf8_lossy(&digits).into_owned(),
        ));
    }
    Ok(())
}

/// The display string for a list of view files.
pub fn view_display_str(view_files: &[String]) -> String {
    view_files.join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn views_of(pairs: &[(&str, &[&str])]) -> ViewDict {
        pairs
            .iter()
            .map(|(name, paths)| {
                (
                    name.to_string(),
                    paths.iter().map(|p| p.to_string()).collect(),
                )
            })
            .collect()
    }

    fn keywords_of(pairs: &[(&str, &str)]) -> Keywords {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn empty_content_deserialises_to_nothing() {
        assert_eq!(
            deserialize(b"").unwrap(),
            (Keywords::new(), ViewDict::new())
        );
    }

    #[test]
    fn marker_only_deserialises_to_nothing() {
        assert_eq!(
            deserialize(b"Bazaar views format 1\n").unwrap(),
            (Keywords::new(), ViewDict::new())
        );
    }

    #[test]
    fn serialises_empty_definition_as_marker_only() {
        assert_eq!(
            serialize(&Keywords::new(), &ViewDict::new()),
            b"Bazaar views format 1\n".to_vec()
        );
    }

    #[test]
    fn serialises_keywords_and_sorted_views() {
        let keywords = keywords_of(&[("current", "x")]);
        let views = views_of(&[("x", &["a", "b"]), ("b", &["c"])]);
        assert_eq!(
            serialize(&keywords, &views),
            b"Bazaar views format 1\ncurrent=x\nviews:\nb\0c\nx\0a\0b\n".to_vec()
        );
    }

    #[test]
    fn round_trips_keywords_and_views() {
        let keywords = keywords_of(&[("current", "x"), ("other", "y")]);
        let views = views_of(&[("x", &["a", "b"]), ("y", &[])]);
        let content = serialize(&keywords, &views);
        assert_eq!(deserialize(&content).unwrap(), (keywords, views));
    }

    #[test]
    fn round_trips_non_ascii_names_and_paths() {
        let keywords = keywords_of(&[("current", "\u{3070}")]);
        let views = views_of(&[("\u{3070}", &["foo", "bar/"])]);
        let content = serialize(&keywords, &views);
        assert_eq!(deserialize(&content).unwrap(), (keywords, views));
    }

    #[test]
    fn preserves_unknown_keywords() {
        let content = b"Bazaar views format 1\ncurrent=x\nfuture=thing\nviews:\nx\0a\n";
        let (keywords, views) = deserialize(content).unwrap();
        assert_eq!(
            keywords,
            keywords_of(&[("current", "x"), ("future", "thing")])
        );
        // Round-tripping keeps the unknown keyword rather than dropping it.
        assert_eq!(serialize(&keywords, &views), content.to_vec());
    }

    #[test]
    fn keywords_keep_file_order() {
        let content = b"Bazaar views format 1\ncurrent=x\nz=1\na=2\n";
        let (keywords, views) = deserialize(content).unwrap();
        assert_eq!(
            keywords.keys().collect::<Vec<_>>(),
            vec!["current", "z", "a"]
        );
        assert_eq!(serialize(&keywords, &views), content.to_vec());
    }

    #[test]
    fn keyword_value_may_contain_equals() {
        let (keywords, _) = deserialize(b"Bazaar views format 1\ncurrent=a=b\n").unwrap();
        assert_eq!(keywords, keywords_of(&[("current", "a=b")]));
    }

    #[test]
    fn view_with_no_paths_deserialises_to_empty_list() {
        let (_, views) = deserialize(b"Bazaar views format 1\nviews:\nx\n").unwrap();
        assert_eq!(views, views_of(&[("x", &[])]));
    }

    #[test]
    fn rejects_missing_marker() {
        assert_eq!(deserialize(b"nonsense\n"), Err(ViewsError::MissingMarker));
    }

    #[test]
    fn rejects_marker_without_version_digits() {
        assert_eq!(
            deserialize(b"Bazaar views format x\n"),
            Err(ViewsError::MissingMarker)
        );
    }

    #[test]
    fn rejects_unsupported_format_version() {
        assert_eq!(
            deserialize(b"Bazaar views format 2\n"),
            Err(ViewsError::UnsupportedFormat("2".to_string()))
        );
    }

    #[test]
    fn rejects_unparsable_keyword_line() {
        assert_eq!(
            deserialize(b"Bazaar views format 1\nbogusline\n"),
            Err(ViewsError::UnparsableLine("bogusline".to_string()))
        );
    }

    #[test]
    fn rejects_blank_line_before_views_section() {
        assert_eq!(
            deserialize(b"Bazaar views format 1\n\nviews:\n"),
            Err(ViewsError::UnparsableLine(String::new()))
        );
    }

    #[test]
    fn rejects_non_utf8_content() {
        assert_eq!(
            deserialize(b"Bazaar views format 1\ncurrent=\xff\n"),
            Err(ViewsError::NotUtf8)
        );
    }

    #[test]
    fn view_display_str_joins_with_commas() {
        assert_eq!(
            view_display_str(&["foo".to_string(), "bar".to_string()]),
            "foo, bar"
        );
        assert_eq!(view_display_str(&[]), "");
    }
}
