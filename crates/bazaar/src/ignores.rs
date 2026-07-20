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

    fn set(items: &[&str]) -> HashSet<String> {
        items.iter().map(|s| s.to_string()).collect()
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

    // Mechanical ports of breezy.tests.test_ignores.TestParseIgnoreFile.

    #[test]
    fn parse_fancy() {
        // Byte-for-byte the input from the Python test; assembled from explicit
        // lines so leading/trailing whitespace (" xx ") is preserved exactly.
        let mut input: Vec<u8> = Vec::new();
        for line in [
            b"./rootdir".as_slice(),
            b"randomfile*",
            b"path/from/ro?t",
            b"unicode\xc2\xb5", // u'\xb5'.encode('utf8')
            b"dos\r",
            b"", // empty line
            b"#comment",
            b" xx ", // whitespace
            b"!RE:^\\.z.*",
            b"!!./.zcompdump",
        ] {
            input.extend_from_slice(line);
            input.push(b'\n');
        }
        let ignored = parse_ignore_file(&input);
        assert_eq!(
            ignored,
            set(&[
                "./rootdir",
                "randomfile*",
                "path/from/ro?t",
                "unicode\u{b5}",
                "dos",
                " xx ",
                "!RE:^\\.z.*",
                "!!./.zcompdump",
            ])
        );
    }

    #[test]
    fn parse_empty() {
        assert_eq!(parse_ignore_file(b""), HashSet::new());
    }

    #[test]
    fn parse_non_utf8() {
        // Lines with non utf8 characters should be discarded.
        let ignored = parse_ignore_file(b"utf8filename_a\ninvalid utf8\x80\nutf8filename_b\n");
        assert_eq!(ignored, set(&["utf8filename_a", "utf8filename_b"]));
    }
}
