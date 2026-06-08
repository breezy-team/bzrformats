//! A parser and writer for the ConfigObj INI dialect breezy stores config in.
//!
//! Breezy loads config files as UTF-8 with `list_values=False` and
//! interpolation off, and only ever uses depth-1 sections in practice
//! (`branch.conf`/`bazaar.conf` use the top-level no-name section plus an
//! optional `[DEFAULT]`; `locations.conf` uses `[path]` sections). This module
//! supports exactly that: the top-level scalars and any number of `[name]`
//! sections, preserving order and comment/blank lines so a rewrite only changes
//! the value that changed.
//!
//! With `list_values=False`, the parser does not strip surrounding quotes or
//! split comma lists; that happens later when a [`super::Stack`] unquotes a
//! value. Writing goes through breezy's list-aware quoting via [`quote_value`].

use super::Section;

/// A parse error from [`ConfigObj::parse`].
#[derive(Debug, PartialEq, Eq)]
pub enum ConfigObjError {
    /// A section header bracket run was malformed (unbalanced `[`/`]`).
    BadSectionHeader(String),
    /// A non-blank, non-comment, non-header line had no `=`.
    MissingEquals(String),
    /// A line referenced a deeper nesting than supported (breezy uses depth 1).
    NestingTooDeep(String),
    /// The content was not valid UTF-8.
    NotUtf8,
}

impl std::fmt::Display for ConfigObjError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigObjError::BadSectionHeader(l) => write!(f, "bad section header: {l:?}"),
            ConfigObjError::MissingEquals(l) => write!(f, "line is not key = value: {l:?}"),
            ConfigObjError::NestingTooDeep(l) => write!(f, "section nested too deeply: {l:?}"),
            ConfigObjError::NotUtf8 => write!(f, "config content is not valid UTF-8"),
        }
    }
}

impl std::error::Error for ConfigObjError {}

/// One physical line of a parsed config file, kept so the file round-trips.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Line {
    /// A blank or `#`-comment line, stored verbatim (without its newline).
    Verbatim(String),
    /// A `[name]` section header introducing the section with this name.
    SectionHeader(String),
    /// A `key = value` entry. `section` is the owning section name (`None` for
    /// the top-level no-name section). `trailing` keeps any inline comment so
    /// it survives a rewrite.
    Entry {
        section: Option<String>,
        key: String,
        value: String,
        trailing: String,
    },
}

/// A parsed ConfigObj file.
///
/// Holds the lines in order. Lookups and mutations work through the logical
/// section/key model while the physical line order is preserved for writing.
pub struct ConfigObj {
    lines: Vec<Line>,
}

impl ConfigObj {
    /// An empty config (no lines).
    pub fn empty() -> Self {
        ConfigObj { lines: Vec::new() }
    }

    /// Parse `bytes` (UTF-8) into a config.
    pub fn parse(bytes: &[u8]) -> Result<Self, ConfigObjError> {
        let text = std::str::from_utf8(bytes).map_err(|_| ConfigObjError::NotUtf8)?;
        let mut lines = Vec::new();
        let mut current_section: Option<String> = None;
        for raw in text.split('\n') {
            // `split('\n')` yields a trailing empty element for a final newline;
            // a file ending in "\n" should not gain a spurious blank line.
            let line = raw.strip_suffix('\r').unwrap_or(raw);
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                lines.push(Line::Verbatim(line.to_string()));
                continue;
            }
            if let Some(rest) = trimmed.strip_prefix('[') {
                let name = parse_section_header(rest)?;
                current_section = Some(name.clone());
                lines.push(Line::SectionHeader(name));
                continue;
            }
            let eq = trimmed
                .find('=')
                .ok_or_else(|| ConfigObjError::MissingEquals(line.to_string()))?;
            let key = unquote_name(trimmed[..eq].trim());
            let (value, trailing) = split_value_and_comment(trimmed[eq + 1..].trim_start());
            lines.push(Line::Entry {
                section: current_section.clone(),
                key,
                value,
                trailing,
            });
        }
        // A file that ended with a newline produced a final empty Verbatim;
        // drop it so writing reproduces the input rather than appending a line.
        if matches!(lines.last(), Some(Line::Verbatim(s)) if s.is_empty()) {
            lines.pop();
        }
        Ok(ConfigObj { lines })
    }

    /// All sections in file order: the no-name section first if it has any
    /// scalars, then each named section.
    pub fn sections(&self) -> Vec<Section> {
        let mut order: Vec<Option<String>> = Vec::new();
        let mut seen: std::collections::HashSet<Option<String>> = std::collections::HashSet::new();
        // The no-name section exists iff there's a top-level entry.
        for line in &self.lines {
            if let Line::Entry { section, .. } = line {
                if seen.insert(section.clone()) {
                    order.push(section.clone());
                }
            }
        }
        order
            .into_iter()
            .map(|id| self.section(id.as_deref()).expect("section was just seen"))
            .collect()
    }

    /// The section with the given id (`None` = no-name), or `None` if it has no
    /// entries.
    pub fn section(&self, id: Option<&str>) -> Option<Section> {
        let mut pairs = Vec::new();
        for line in &self.lines {
            if let Line::Entry {
                section,
                key,
                value,
                ..
            } = line
            {
                if section.as_deref() == id {
                    pairs.push((key.clone(), value.clone()));
                }
            }
        }
        if pairs.is_empty() {
            None
        } else {
            Some(Section::new(id.map(|s| s.to_string()), pairs))
        }
    }

    /// Set `key` to `value` in section `id`, in place if it already exists,
    /// otherwise appended after the last entry of that section (creating the
    /// section header if needed).
    pub fn set_value(&mut self, id: Option<&str>, key: &str, value: &str) {
        // In-place update if the key already exists in the section.
        for line in &mut self.lines {
            if let Line::Entry {
                section,
                key: k,
                value: v,
                ..
            } = line
            {
                if section.as_deref() == id && k == key {
                    *v = value.to_string();
                    return;
                }
            }
        }
        self.insert_new_entry(id, key, value);
    }

    /// Remove `key` from section `id` if present.
    pub fn remove_value(&mut self, id: Option<&str>, key: &str) {
        self.lines.retain(|line| {
            !matches!(
                line,
                Line::Entry { section, key: k, .. }
                    if section.as_deref() == id && k == key
            )
        });
    }

    fn insert_new_entry(&mut self, id: Option<&str>, key: &str, value: &str) {
        let entry = Line::Entry {
            section: id.map(|s| s.to_string()),
            key: key.to_string(),
            value: value.to_string(),
            trailing: String::new(),
        };
        match id {
            // No-name entries go at the very top, before any section header, so
            // they stay in the top-level section.
            None => {
                let pos = self
                    .lines
                    .iter()
                    .position(|l| matches!(l, Line::SectionHeader(_)))
                    .unwrap_or(self.lines.len());
                self.lines.insert(pos, entry);
            }
            Some(name) => {
                // Find the section header; append after its last entry. Create
                // the header at end of file if the section doesn't exist yet.
                let header = self
                    .lines
                    .iter()
                    .position(|l| matches!(l, Line::SectionHeader(n) if n == name));
                match header {
                    Some(h) => {
                        let mut insert_at = h + 1;
                        for (i, line) in self.lines.iter().enumerate().skip(h + 1) {
                            match line {
                                Line::SectionHeader(_) => break,
                                Line::Entry { section, .. } if section.as_deref() == id => {
                                    insert_at = i + 1;
                                }
                                _ => {}
                            }
                        }
                        self.lines.insert(insert_at, entry);
                    }
                    None => {
                        self.lines.push(Line::SectionHeader(name.to_string()));
                        self.lines.push(entry);
                    }
                }
            }
        }
    }

    /// Serialize back to bytes (UTF-8), newline-terminated.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = String::new();
        for line in &self.lines {
            match line {
                Line::Verbatim(s) => out.push_str(s),
                Line::SectionHeader(name) => {
                    out.push('[');
                    out.push_str(name);
                    out.push(']');
                }
                Line::Entry {
                    key,
                    value,
                    trailing,
                    ..
                } => {
                    out.push_str(key);
                    out.push_str(" = ");
                    out.push_str(value);
                    out.push_str(trailing);
                }
            }
            out.push('\n');
        }
        out.into_bytes()
    }
}

/// Parse the part of a section header after the opening `[`, returning the
/// section name. Only depth-1 headers (`[name]`) are supported, matching
/// breezy's actual usage.
fn parse_section_header(rest: &str) -> Result<String, ConfigObjError> {
    let line = format!("[{rest}");
    // Strip an optional inline comment after the closing bracket.
    let body = match rest.rsplit_once(']') {
        Some((before, _after)) => before,
        None => return Err(ConfigObjError::BadSectionHeader(line)),
    };
    if body.starts_with('[') {
        // [[sub]] -> depth 2 or more; breezy config files don't use these.
        return Err(ConfigObjError::NestingTooDeep(line));
    }
    let name = unquote_name(body.trim());
    if name.is_empty() {
        return Err(ConfigObjError::BadSectionHeader(line));
    }
    Ok(name)
}

/// Strip a matched surrounding quote pair from a section or key name, as
/// configobj's `_unquote` does.
fn unquote_name(s: &str) -> String {
    let bytes = s.as_bytes();
    if bytes.len() >= 2 {
        let first = bytes[0];
        let last = bytes[bytes.len() - 1];
        if first == last && (first == b'"' || first == b'\'') {
            return s[1..s.len() - 1].to_string();
        }
    }
    s.to_string()
}

/// Split a raw value into the value text and any trailing inline comment.
///
/// With `list_values=False`, configobj keeps a quoted value's quotes in the
/// stored string and only treats `#` as a comment start when it's outside
/// quotes. We preserve the value verbatim (quotes included) and capture the
/// comment in `trailing` so a rewrite reproduces it.
fn split_value_and_comment(s: &str) -> (String, String) {
    if let Some(quote) = s.chars().next().filter(|c| *c == '"' || *c == '\'') {
        // Quoted value: the value runs to the matching closing quote; anything
        // after is a trailing comment (kept verbatim, including leading space).
        if let Some(end) = s[1..].find(quote) {
            let value_end = 1 + end + 1;
            let value = s[..value_end].to_string();
            let trailing = s[value_end..].to_string();
            return (value, trailing);
        }
        // Unterminated quote: treat the whole thing as the value.
        return (s.to_string(), String::new());
    }
    match s.find('#') {
        Some(h) => {
            // The value is everything before the `#`, with trailing whitespace
            // stripped. `trailing` keeps that whitespace and the comment so a
            // rewrite (which writes value then trailing) reproduces the gap.
            let value = s[..h].trim_end();
            let trailing = &s[value.len()..];
            (value.to_string(), trailing.to_string())
        }
        None => (s.trim_end().to_string(), String::new()),
    }
}

/// Quote `value` for writing, matching breezy's list-aware `Store.quote`.
///
/// A scalar needs no quotes unless its first/last char is whitespace or a
/// quote, or it contains a comma or `#`. Empty string becomes `""`.
pub fn quote_value(value: &str) -> String {
    if value.is_empty() {
        return "\"\"".to_string();
    }
    let needs_quote = {
        let bytes = value.as_bytes();
        let first = bytes[0];
        let last = bytes[bytes.len() - 1];
        let edge_ws_or_quote = |c: u8| matches!(c, b' ' | b'\t' | b'\r' | b'\n' | b'"' | b'\'');
        edge_ws_or_quote(first)
            || edge_ws_or_quote(last)
            || value.contains(',')
            || value.contains('#')
    };
    if !needs_quote {
        return value.to_string();
    }
    // Prefer single quotes; use double quotes if the value contains a single
    // quote (configobj's rule). Both present is not expected for the keys we
    // write (locations/booleans), so fall back to double quoting.
    if value.contains('\'') {
        format!("\"{value}\"")
    } else {
        format!("'{value}'")
    }
}

/// Strip a matched surrounding quote pair from a raw value, as
/// `Store.unquote` (configobj's `_unquote`).
pub fn unquote_value(value: &str) -> String {
    unquote_name(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_no_name_section() {
        let c = ConfigObj::parse(b"a = 1\nb = two\n").unwrap();
        let sec = c.section(None).unwrap();
        assert_eq!(sec.get("a"), Some("1"));
        assert_eq!(sec.get("b"), Some("two"));
    }

    #[test]
    fn parses_named_section() {
        let c = ConfigObj::parse(b"top = x\n[/home/foo]\nkey = val\n").unwrap();
        assert_eq!(c.section(None).unwrap().get("top"), Some("x"));
        assert_eq!(
            c.section(Some("/home/foo")).unwrap().get("key"),
            Some("val")
        );
    }

    #[test]
    fn round_trips_verbatim_comments_and_blanks() {
        let input = b"# a comment\n\nnickname = trunk\n";
        let c = ConfigObj::parse(input).unwrap();
        assert_eq!(c.to_bytes(), input);
    }

    #[test]
    fn round_trips_without_trailing_newline() {
        // Input without trailing newline still writes one (configobj ensures a
        // final newline), but no extra blank line appears.
        let c = ConfigObj::parse(b"a = 1").unwrap();
        assert_eq!(c.to_bytes(), b"a = 1\n");
    }

    #[test]
    fn set_value_updates_in_place() {
        let mut c = ConfigObj::parse(b"a = 1\nb = 2\n").unwrap();
        c.set_value(None, "a", "99");
        assert_eq!(c.to_bytes(), b"a = 99\nb = 2\n");
    }

    #[test]
    fn set_value_appends_new_no_name_key_before_sections() {
        let mut c = ConfigObj::parse(b"a = 1\n[s]\nx = y\n").unwrap();
        c.set_value(None, "b", "2");
        assert_eq!(c.to_bytes(), b"a = 1\nb = 2\n[s]\nx = y\n");
    }

    #[test]
    fn set_value_creates_section() {
        let mut c = ConfigObj::parse(b"a = 1\n").unwrap();
        c.set_value(Some("loc"), "k", "v");
        assert_eq!(c.to_bytes(), b"a = 1\n[loc]\nk = v\n");
    }

    #[test]
    fn set_value_appends_after_last_entry_of_section() {
        // A section that already has two entries, followed by a later section.
        // A new key must land directly after the section's last entry, not
        // after its header and not after the following section.
        let mut c = ConfigObj::parse(b"[s1]\nx = 1\ny = 2\n[s2]\nz = 3\n").unwrap();
        c.set_value(Some("s1"), "w", "4");
        assert_eq!(c.to_bytes(), b"[s1]\nx = 1\ny = 2\nw = 4\n[s2]\nz = 3\n");
    }

    #[test]
    fn set_value_skips_foreign_entries_when_appending() {
        // configobj keeps physical line order, so a [s1] entry can appear after
        // an [s2] header only via interleaving; here we just confirm the append
        // stops at the next header and ignores entries of other sections.
        let mut c = ConfigObj::parse(b"[s1]\nx = 1\n# note\n[s2]\nz = 3\n").unwrap();
        c.set_value(Some("s1"), "y", "2");
        assert_eq!(c.to_bytes(), b"[s1]\nx = 1\ny = 2\n# note\n[s2]\nz = 3\n");
    }

    #[test]
    fn set_value_appends_into_empty_header_section() {
        // A header with no entries of its own, immediately followed by another
        // header. The new key must land right after the [s1] header (insert_at
        // = header_pos + 1), before [s2], not before the [s1] header.
        let mut c = ConfigObj::parse(b"[s1]\n[s2]\nz = 3\n").unwrap();
        c.set_value(Some("s1"), "k", "v");
        assert_eq!(c.to_bytes(), b"[s1]\nk = v\n[s2]\nz = 3\n");
    }

    #[test]
    fn set_value_appends_after_first_block_of_interleaved_section() {
        // The same section name appears twice with another section between.
        // set_value targets the FIRST [s1] block: scanning stops at the [s2]
        // header (the break arm), so the new key lands after the first block's
        // entry, not after the second [s1] block far below.
        let mut c = ConfigObj::parse(b"[s1]\nx = 1\n[s2]\nz = 3\n[s1]\nw = 4\n").unwrap();
        c.set_value(Some("s1"), "y", "2");
        assert_eq!(
            c.to_bytes(),
            b"[s1]\nx = 1\ny = 2\n[s2]\nz = 3\n[s1]\nw = 4\n"
        );
    }

    #[test]
    fn remove_value_drops_line() {
        let mut c = ConfigObj::parse(b"a = 1\nb = 2\n").unwrap();
        c.remove_value(None, "a");
        assert_eq!(c.to_bytes(), b"b = 2\n");
    }

    #[test]
    fn quote_value_rules() {
        assert_eq!(quote_value("plain"), "plain");
        assert_eq!(quote_value(""), "\"\"");
        assert_eq!(quote_value(" leading"), "' leading'");
        assert_eq!(quote_value("a,b"), "'a,b'");
        assert_eq!(quote_value("has#hash"), "'has#hash'");
        // A mid-string quote needs no quoting (no comma/#, not an edge char).
        assert_eq!(quote_value("has'quote"), "has'quote");
        // A value that needs quoting and contains a single quote uses doubles.
        assert_eq!(quote_value("a,'b"), "\"a,'b\"");
    }

    #[test]
    fn unquote_value_strips_pair() {
        assert_eq!(unquote_value("'x'"), "x");
        assert_eq!(unquote_value("\"x\""), "x");
        assert_eq!(unquote_value("x"), "x");
    }

    #[test]
    fn inline_comment_round_trips() {
        let input = b"a = 1 # hi\n";
        let c = ConfigObj::parse(input).unwrap();
        assert_eq!(c.section(None).unwrap().get("a"), Some("1"));
        assert_eq!(c.to_bytes(), input);
    }

    #[test]
    fn quoted_value_keeps_quotes_and_trailing_comment() {
        // With list_values=False the value keeps its surrounding quotes, and a
        // comment after the closing quote is captured verbatim so it round-trips.
        let input = b"a = \"v a l\" # tail\n";
        let c = ConfigObj::parse(input).unwrap();
        assert_eq!(c.section(None).unwrap().get("a"), Some("\"v a l\""));
        assert_eq!(c.to_bytes(), input);
    }

    #[test]
    fn quoted_value_without_comment_round_trips() {
        let input = b"a = 'q'\n";
        let c = ConfigObj::parse(input).unwrap();
        assert_eq!(c.section(None).unwrap().get("a"), Some("'q'"));
        assert_eq!(c.to_bytes(), input);
    }

    #[test]
    fn single_quoted_value_with_hash_is_not_a_comment() {
        // A single-quoted value containing `#` keeps the whole quoted run as the
        // value; the `#` must not start a comment because it is inside quotes.
        let input = b"a = '#x'\n";
        let c = ConfigObj::parse(input).unwrap();
        assert_eq!(c.section(None).unwrap().get("a"), Some("'#x'"));
        assert_eq!(c.to_bytes(), input);
    }

    #[test]
    fn unterminated_quote_is_whole_value() {
        let c = ConfigObj::parse(b"a = \"oops\n").unwrap();
        assert_eq!(c.section(None).unwrap().get("a"), Some("\"oops"));
    }

    #[test]
    fn unquote_name_only_strips_real_quote_pairs() {
        // A doubled non-quote char (first == last but not a quote) is left alone.
        let c = ConfigObj::parse(b"aa = 1\n").unwrap();
        assert_eq!(c.section(None).unwrap().get("aa"), Some("1"));
        // A genuine quote pair around a key is stripped.
        let c = ConfigObj::parse(b"\"k\" = 1\n").unwrap();
        assert_eq!(c.section(None).unwrap().get("k"), Some("1"));
        // Mismatched edge chars (a...z) are not a pair.
        let c = ConfigObj::parse(b"az = 1\n").unwrap();
        assert_eq!(c.section(None).unwrap().get("az"), Some("1"));
    }

    #[test]
    fn missing_equals_is_error() {
        match ConfigObj::parse(b"not a config line\n") {
            Err(ConfigObjError::MissingEquals(line)) => assert_eq!(line, "not a config line"),
            other => panic!("expected MissingEquals, got {:?}", other.map(|_| "ok")),
        }
    }
}
