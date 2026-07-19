//! Parsing of ignore files (`.bzrignore` and the user ignore config).

use crate::globbing::normalize_pattern;
use std::collections::HashSet;

/// Parse an ignore file into a set of normalized ignore patterns.
///
/// Continue in the case of utf8 decoding errors, and emit a warning when such
/// an error is found. Optimise for the common case -- no decoding errors.
///
/// Comment lines (starting with `#`) and blank lines are skipped; the rest are
/// normalized via [`normalize_pattern`].
pub fn parse_ignore_file(data: &[u8]) -> HashSet<String> {
    let mut ignored = HashSet::new();

    let unicode_lines: Vec<String> = match std::str::from_utf8(data) {
        Ok(text) => text.split('\n').map(|s| s.to_string()).collect(),
        Err(_) => {
            // Go through line by line and pick out the 'good' decodable lines.
            let mut lines = Vec::new();
            for (line_number, line) in data.split(|&b| b == b'\n').enumerate() {
                match std::str::from_utf8(line) {
                    Ok(s) => lines.push(s.to_string()),
                    Err(_) => log::warn!(
                        ".bzrignore: On Line #{}, malformed utf8 character. Ignoring line.",
                        line_number + 1
                    ),
                }
            }
            lines
        }
    };

    for uline in unicode_lines {
        let uline = uline.trim_end_matches(['\r', '\n']);
        if uline.is_empty() || uline.starts_with('#') {
            continue;
        }
        ignored.insert(normalize_pattern(uline));
    }
    ignored
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(data: &[u8]) -> Vec<String> {
        let mut v: Vec<String> = parse_ignore_file(data).into_iter().collect();
        v.sort();
        v
    }

    #[test]
    fn empty() {
        assert!(parse_ignore_file(b"").is_empty());
    }

    #[test]
    fn simple() {
        assert_eq!(parse(b"*.o\n*.a\n"), vec!["*.a", "*.o"]);
    }

    #[test]
    fn skips_comments_and_blanks() {
        assert_eq!(parse(b"# a comment\n\n*.o\n   \n"), vec!["   ", "*.o"]);
    }

    #[test]
    fn strips_trailing_cr() {
        assert_eq!(parse(b"*.o\r\n"), vec!["*.o"]);
    }

    #[test]
    fn no_trailing_newline() {
        assert_eq!(parse(b"*.o"), vec!["*.o"]);
    }

    #[test]
    fn normalizes_backslashes() {
        assert_eq!(parse(b"foo\\bar\n"), vec!["foo/bar"]);
    }

    #[test]
    fn dedups() {
        assert_eq!(parse(b"*.o\n*.o\n"), vec!["*.o"]);
    }

    #[test]
    fn skips_malformed_utf8_lines() {
        // First line is invalid utf8, second is fine.
        let data = b"\xff\xfe\n*.o\n";
        assert_eq!(parse(data), vec!["*.o"]);
    }
}
