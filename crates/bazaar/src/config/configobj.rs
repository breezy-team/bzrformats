//! A parser and writer for the ConfigObj INI dialect breezy stores config in.
//!
//! Breezy loads config files as UTF-8 with `list_values=False` and
//! interpolation off. At runtime it only uses depth-1 sections
//! (`branch.conf`/`bazaar.conf` use the top-level no-name section plus an
//! optional `[DEFAULT]`; `locations.conf` uses `[path]` sections), but configobj
//! (and breezy's tests) also allow depth-2 `[[sub]]` subsections, so this module
//! parses those too. Section/key order and comment/blank lines are preserved so
//! a rewrite only changes the value that changed; empty section headers are kept
//! because `LocationStore` matches on the section names.
//!
//! With `list_values=False`, single/double-quoted values keep their surrounding
//! quotes (unquoting happens later when a [`super::Stack`] reads the value), but
//! triple-quoted `'''...'''` values are a multiline mechanism and are stored
//! unquoted, then written back triple-quoted. Writing scalar values goes through
//! breezy's list-aware quoting via [`quote_value`].

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
    /// A value could not be parsed (e.g. an unterminated quote).
    BadValue(String),
    /// The content was not valid UTF-8.
    NotUtf8,
}

impl std::fmt::Display for ConfigObjError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigObjError::BadSectionHeader(l) => write!(f, "bad section header: {l:?}"),
            ConfigObjError::MissingEquals(l) => write!(f, "line is not key = value: {l:?}"),
            ConfigObjError::NestingTooDeep(l) => write!(f, "section nested too deeply: {l:?}"),
            ConfigObjError::BadValue(l) => write!(f, "bad value: {l:?}"),
            ConfigObjError::NotUtf8 => write!(f, "config content is not valid UTF-8"),
        }
    }
}

impl std::error::Error for ConfigObjError {}

/// One physical line of a parsed config file, kept so the file round-trips.
///
/// A section is identified by its `path`: `[]` is the top-level no-name
/// section, `["name"]` a `[name]` header, and `["name", "sub"]` a `[[sub]]`
/// subsection nested under the preceding `[name]`. breezy uses depth 1 at
/// runtime; depth 2 exists because configobj (and breezy's tests) allow it.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Line {
    /// A blank or `#`-comment line, stored verbatim (without its newline).
    Verbatim(String),
    /// A section header. `path` has one element for `[name]`, two for a nested
    /// `[[sub]]`.
    SectionHeader { path: Vec<String> },
    /// A `key = value` entry owned by the section at `path` (`[]` for the
    /// top-level no-name section). `trailing` keeps any inline comment so it
    /// survives a rewrite. `multiline` records that the value came from (and
    /// should be written back as) a triple-quoted `'''...'''` block; its stored
    /// `value` is the inner content with the triple-quotes removed.
    Entry {
        path: Vec<String>,
        key: String,
        value: String,
        trailing: String,
        multiline: bool,
    },
}

/// One section in the parsed tree: its scalar options in file order plus any
/// nested subsections. Used by [`ConfigObj::section_tree`] for consumers that
/// need the full structure (empty headers and depth-2 nesting included).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SectionNode {
    /// The section name; `None` for the top-level no-name section.
    pub name: Option<String>,
    /// `(key, value)` scalar options in file order.
    pub options: Vec<(String, String)>,
    /// Nested subsections in file order (each with a `Some` name).
    pub subsections: Vec<SectionNode>,
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
        let mut current_path: Vec<String> = Vec::new();
        // `split('\n')` yields a trailing empty element for a final newline; a
        // file ending in "\n" should not gain a spurious blank line.
        let raw_lines: Vec<&str> = text
            .split('\n')
            .map(|raw| raw.strip_suffix('\r').unwrap_or(raw))
            .collect();
        let mut i = 0;
        while i < raw_lines.len() {
            let line = raw_lines[i];
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                lines.push(Line::Verbatim(line.to_string()));
                i += 1;
                continue;
            }
            if trimmed.starts_with('[') {
                let (depth, name) = parse_section_header(trimmed)?;
                // A depth-2 header must follow a depth-1 section; a depth-1
                // header starts a fresh top-level section.
                if depth == 1 {
                    current_path = vec![name];
                } else {
                    let parent = current_path
                        .first()
                        .cloned()
                        .ok_or_else(|| ConfigObjError::BadSectionHeader(trimmed.to_string()))?;
                    current_path = vec![parent, name];
                }
                lines.push(Line::SectionHeader {
                    path: current_path.clone(),
                });
                i += 1;
                continue;
            }
            let eq = trimmed
                .find('=')
                .ok_or_else(|| ConfigObjError::MissingEquals(line.to_string()))?;
            let key = unquote_name(trimmed[..eq].trim());
            let value_part = trimmed[eq + 1..].trim_start();
            if let Some(quote) = triple_quote_opener(value_part) {
                // A triple-quoted value: consume physical lines until the
                // closing triple-quote, storing the inner content unquoted.
                let (value, consumed) = parse_triple_quoted(&raw_lines, i, value_part, quote)?;
                lines.push(Line::Entry {
                    path: current_path.clone(),
                    key,
                    value,
                    trailing: String::new(),
                    multiline: true,
                });
                i += consumed;
                continue;
            }
            let (value, trailing) = split_value_and_comment(value_part)?;
            lines.push(Line::Entry {
                path: current_path.clone(),
                key,
                value,
                trailing,
                multiline: false,
            });
            i += 1;
        }
        // A file that ended with a newline produced a final empty Verbatim;
        // drop it so writing reproduces the input rather than appending a line.
        if matches!(lines.last(), Some(Line::Verbatim(s)) if s.is_empty()) {
            lines.pop();
        }
        Ok(ConfigObj { lines })
    }

    /// All depth-1 sections in file order that have at least one direct entry:
    /// the no-name section first if it has any scalars, then each named section.
    ///
    /// Subsection-only headers and empty headers are skipped; use
    /// [`ConfigObj::section_tree`] for the full structure. This is the view the
    /// [`super::Stack`] consumes.
    pub fn sections(&self) -> Vec<Section> {
        let mut order: Vec<Option<String>> = Vec::new();
        let mut seen: std::collections::HashSet<Option<String>> = std::collections::HashSet::new();
        for line in &self.lines {
            if let Line::Entry { path, .. } = line {
                if let Some(id) = depth1_id(path) {
                    let owned = id.map(|s| s.to_string());
                    if seen.insert(owned.clone()) {
                        order.push(owned);
                    }
                }
            }
        }
        order
            .into_iter()
            .map(|id| self.section(id.as_deref()).expect("section was just seen"))
            .collect()
    }

    /// The depth-1 section with the given id (`None` = no-name), or `None` if it
    /// has no direct entries. Entries in nested subsections are not included.
    pub fn section(&self, id: Option<&str>) -> Option<Section> {
        let mut pairs = Vec::new();
        for line in &self.lines {
            if let Line::Entry {
                path, key, value, ..
            } = line
            {
                if path_is_depth1(path, id) {
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

    /// The full section tree in file order, including empty headers and depth-2
    /// subsections. The no-name section appears first when the file has any
    /// top-level scalars.
    pub fn section_tree(&self) -> Vec<SectionNode> {
        let mut roots: Vec<SectionNode> = Vec::new();
        // Index of each top-level section (by name; None = no-name) in `roots`.
        let mut root_index: std::collections::HashMap<Option<String>, usize> =
            std::collections::HashMap::new();

        let mut ensure_root = |roots: &mut Vec<SectionNode>, name: Option<String>| -> usize {
            if let Some(&i) = root_index.get(&name) {
                return i;
            }
            let i = roots.len();
            roots.push(SectionNode {
                name: name.clone(),
                options: Vec::new(),
                subsections: Vec::new(),
            });
            root_index.insert(name, i);
            i
        };

        for line in &self.lines {
            match line {
                Line::SectionHeader { path } => {
                    let root = ensure_root(&mut roots, Some(path[0].clone()));
                    if path.len() == 2 {
                        let sub_name = &path[1];
                        if !roots[root]
                            .subsections
                            .iter()
                            .any(|s| s.name.as_deref() == Some(sub_name))
                        {
                            roots[root].subsections.push(SectionNode {
                                name: Some(sub_name.clone()),
                                options: Vec::new(),
                                subsections: Vec::new(),
                            });
                        }
                    }
                }
                Line::Entry {
                    path, key, value, ..
                } => match path.len() {
                    0 => {
                        let root = ensure_root(&mut roots, None);
                        roots[root].options.push((key.clone(), value.clone()));
                    }
                    1 => {
                        let root = ensure_root(&mut roots, Some(path[0].clone()));
                        roots[root].options.push((key.clone(), value.clone()));
                    }
                    _ => {
                        let root = ensure_root(&mut roots, Some(path[0].clone()));
                        let sub_name = &path[1];
                        let sub = match roots[root]
                            .subsections
                            .iter_mut()
                            .find(|s| s.name.as_deref() == Some(sub_name))
                        {
                            Some(s) => s,
                            None => {
                                roots[root].subsections.push(SectionNode {
                                    name: Some(sub_name.clone()),
                                    options: Vec::new(),
                                    subsections: Vec::new(),
                                });
                                roots[root].subsections.last_mut().unwrap()
                            }
                        };
                        sub.options.push((key.clone(), value.clone()));
                    }
                },
                Line::Verbatim(_) => {}
            }
        }
        roots
    }

    /// Set `key` to `value` in section `id`, in place if it already exists,
    /// otherwise appended after the last entry of that section (creating the
    /// section header if needed).
    pub fn set_value(&mut self, id: Option<&str>, key: &str, value: &str) {
        // In-place update if the key already exists directly in the section.
        for line in &mut self.lines {
            if let Line::Entry {
                path,
                key: k,
                value: v,
                ..
            } = line
            {
                if path_is_depth1(path, id) && k == key {
                    *v = value.to_string();
                    return;
                }
            }
        }
        self.insert_new_entry(id, key, value);
    }

    /// Set `key` to `value` in the subsection `[[sub]]` nested under `[section]`,
    /// updating in place if present, otherwise appending (creating the section
    /// and subsection headers as needed).
    pub fn set_subsection_value(&mut self, section: &str, sub: &str, key: &str, value: &str) {
        let target = [section.to_string(), sub.to_string()];
        // In-place update if the key already exists in the subsection.
        for line in &mut self.lines {
            if let Line::Entry {
                path,
                key: k,
                value: v,
                ..
            } = line
            {
                if path.as_slice() == target && k == key {
                    *v = value.to_string();
                    return;
                }
            }
        }
        let entry = Line::Entry {
            path: target.to_vec(),
            key: key.to_string(),
            value: value.to_string(),
            trailing: String::new(),
            multiline: false,
        };
        // Locate the subsection header if it already exists.
        let sub_header = self
            .lines
            .iter()
            .position(|l| matches!(l, Line::SectionHeader { path } if path.as_slice() == target));
        if let Some(h) = sub_header {
            let mut insert_at = h + 1;
            for (i, line) in self.lines.iter().enumerate().skip(h + 1) {
                match line {
                    Line::SectionHeader { .. } => break,
                    Line::Entry { path, .. } if path.as_slice() == target => insert_at = i + 1,
                    _ => {}
                }
            }
            self.lines.insert(insert_at, entry);
            return;
        }
        // No subsection header yet. Find the parent section header; if it too is
        // missing, create it at end of file. Append the subsection header right
        // after the parent's own block.
        let parent_header = self.lines.iter().position(
            |l| matches!(l, Line::SectionHeader { path } if path.len() == 1 && path[0] == section),
        );
        let insert_at = match parent_header {
            Some(p) => {
                let mut at = p + 1;
                for (i, line) in self.lines.iter().enumerate().skip(p + 1) {
                    match line {
                        Line::SectionHeader { .. } => break,
                        _ => at = i + 1,
                    }
                }
                at
            }
            None => {
                self.lines.push(Line::SectionHeader {
                    path: vec![section.to_string()],
                });
                self.lines.len()
            }
        };
        self.lines.insert(
            insert_at,
            Line::SectionHeader {
                path: target.to_vec(),
            },
        );
        self.lines.insert(insert_at + 1, entry);
    }

    /// Remove `key` from the depth-1 section `id` if present.
    pub fn remove_value(&mut self, id: Option<&str>, key: &str) {
        self.lines.retain(|line| {
            !matches!(
                line,
                Line::Entry { path, key: k, .. }
                    if path_is_depth1(path, id) && k == key
            )
        });
    }

    fn insert_new_entry(&mut self, id: Option<&str>, key: &str, value: &str) {
        let path: Vec<String> = id.map(|s| vec![s.to_string()]).unwrap_or_default();
        let entry = Line::Entry {
            path,
            key: key.to_string(),
            value: value.to_string(),
            trailing: String::new(),
            multiline: false,
        };
        match id {
            // No-name entries go at the very top, before any section header, so
            // they stay in the top-level section.
            None => {
                let pos = self
                    .lines
                    .iter()
                    .position(|l| matches!(l, Line::SectionHeader { .. }))
                    .unwrap_or(self.lines.len());
                self.lines.insert(pos, entry);
            }
            Some(name) => {
                // Find the depth-1 header; append after its last direct entry
                // (stopping at the next header). Create the header at end of
                // file if the section doesn't exist yet.
                let header = self.lines.iter().position(
                    |l| matches!(l, Line::SectionHeader { path } if path.len() == 1 && path[0] == name),
                );
                match header {
                    Some(h) => {
                        let mut insert_at = h + 1;
                        for (i, line) in self.lines.iter().enumerate().skip(h + 1) {
                            match line {
                                Line::SectionHeader { .. } => break,
                                Line::Entry { path, .. } if path_is_depth1(path, id) => {
                                    insert_at = i + 1;
                                }
                                _ => {}
                            }
                        }
                        self.lines.insert(insert_at, entry);
                    }
                    None => {
                        self.lines.push(Line::SectionHeader {
                            path: vec![name.to_string()],
                        });
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
                Line::SectionHeader { path } => {
                    let depth = path.len();
                    let name = path.last().expect("section header path is non-empty");
                    for _ in 0..depth {
                        out.push('[');
                    }
                    out.push_str(name);
                    for _ in 0..depth {
                        out.push(']');
                    }
                }
                Line::Entry {
                    key,
                    value,
                    trailing,
                    multiline,
                    ..
                } => {
                    out.push_str(key);
                    out.push_str(" = ");
                    if *multiline {
                        // Written back as a triple-quoted block. configobj uses
                        // `'''` unless the value contains one, in which case it
                        // uses `"""`.
                        let triple = if value.contains("'''") {
                            "\"\"\""
                        } else {
                            "'''"
                        };
                        out.push_str(triple);
                        out.push_str(value);
                        out.push_str(triple);
                    } else {
                        out.push_str(value);
                        out.push_str(trailing);
                    }
                }
            }
            out.push('\n');
        }
        out.into_bytes()
    }
}

/// Parse a section header line (`[name]` or `[[sub]]`), returning its nesting
/// depth (1 or 2) and unquoted name. Depths of 3 or more are rejected, matching
/// configobj's practical limit for breezy.
fn parse_section_header(line: &str) -> Result<(usize, String), ConfigObjError> {
    let open = line.chars().take_while(|c| *c == '[').count();
    if open == 0 {
        return Err(ConfigObjError::BadSectionHeader(line.to_string()));
    }
    if open > 2 {
        return Err(ConfigObjError::NestingTooDeep(line.to_string()));
    }
    // The closing brackets are the last `open` `]`s; anything after them is an
    // inline comment. Find the matching run of `]`.
    let closing: String = std::iter::repeat_n(']', open).collect();
    let close_at = line
        .rfind(&closing)
        .filter(|&at| at >= open)
        .ok_or_else(|| ConfigObjError::BadSectionHeader(line.to_string()))?;
    let body = &line[open..close_at];
    let name = unquote_name(body.trim());
    if name.is_empty() {
        return Err(ConfigObjError::BadSectionHeader(line.to_string()));
    }
    Ok((open, name))
}

/// The depth-1 section id for a path: `Some(None)` for the top-level no-name
/// section (`[]`), `Some(Some(name))` for a `[name]` section, and `None` for a
/// deeper (subsection) path that has no depth-1 identity.
fn depth1_id(path: &[String]) -> Option<Option<&str>> {
    match path.len() {
        0 => Some(None),
        1 => Some(Some(path[0].as_str())),
        _ => None,
    }
}

/// Whether `path` names exactly the depth-1 section `id` (`None` = no-name).
fn path_is_depth1(path: &[String], id: Option<&str>) -> bool {
    depth1_id(path) == Some(id)
}

/// Strip a matched surrounding quote pair from a section or key name, as
/// configobj's `_unquote` does. configobj strips whenever the first and last
/// character are the same quote char, so a lone `'` or `"` unquotes to empty.
fn unquote_name(s: &str) -> String {
    let bytes = s.as_bytes();
    if !bytes.is_empty() {
        let first = bytes[0];
        let last = bytes[bytes.len() - 1];
        if first == last && (first == b'"' || first == b'\'') {
            // A lone quote (len 1) has first == last and strips to empty.
            return s
                .get(1..s.len().saturating_sub(1))
                .unwrap_or("")
                .to_string();
        }
    }
    s.to_string()
}

/// If `value_part` opens a triple-quoted value (`"""` or `'''`), return the
/// quote character; otherwise `None`.
fn triple_quote_opener(value_part: &str) -> Option<char> {
    if value_part.starts_with("\"\"\"") {
        Some('"')
    } else if value_part.starts_with("'''") {
        Some('\'')
    } else {
        None
    }
}

/// Parse a triple-quoted value starting at line `start` (whose value part is
/// `first_value`). Returns the inner content (quotes stripped) and the number
/// of physical lines consumed. A closing triple-quote may be on the same line
/// or any following line; reaching end-of-input without one is an error.
fn parse_triple_quoted(
    raw_lines: &[&str],
    start: usize,
    first_value: &str,
    quote: char,
) -> Result<(String, usize), ConfigObjError> {
    let triple: String = std::iter::repeat_n(quote, 3).collect();
    let after_open = &first_value[3..];
    // Same-line close: `key = """value"""`.
    if let Some(end) = after_open.find(&triple) {
        return Ok((after_open[..end].to_string(), 1));
    }
    // Multiline: accumulate following lines until one contains the closing
    // triple-quote.
    let mut content = String::from(after_open);
    let mut idx = start + 1;
    while idx < raw_lines.len() {
        let line = raw_lines[idx];
        if let Some(end) = line.find(&triple) {
            content.push('\n');
            content.push_str(&line[..end]);
            return Ok((content, idx - start + 1));
        }
        content.push('\n');
        content.push_str(line);
        idx += 1;
    }
    Err(ConfigObjError::BadValue(first_value.to_string()))
}

/// Split a raw value into the value text and any trailing inline comment,
/// matching configobj's `_nolistvalue` regex (the `list_values=False` case):
///
/// ```text
/// ^ ( "..." | '...' | [^'"#].*? | (empty) ) \s*(#.*)? $
/// ```
///
/// A quoted value keeps its quotes and only what follows (after optional
/// whitespace) may be a `#` comment; if the closing quote is followed by other
/// text, the match extends to a later closing quote. An unquoted value ends at
/// the first `#` (no whitespace required). An unterminated quote is an error.
fn split_value_and_comment(s: &str) -> Result<(String, String), ConfigObjError> {
    // Empty value, or a value that is only a comment.
    if s.is_empty() || s.starts_with('#') {
        return Ok((String::new(), s.to_string()));
    }
    let first = s.as_bytes()[0];
    if first == b'"' || first == b'\'' {
        let quote = first as char;
        // Try each closing quote in turn; the value is valid when the text
        // after the closing quote is optional whitespace then an optional
        // `#` comment.
        let mut search = 1;
        while let Some(rel) = s[search..].find(quote) {
            let close = search + rel;
            let rest = &s[close + 1..];
            let after = rest.trim_start();
            if after.is_empty() || after.starts_with('#') {
                let value = s[..close + 1].to_string();
                return Ok((value, rest.to_string()));
            }
            search = close + 1;
        }
        // No closing quote leaves a valid remainder: unterminated quote.
        return Err(ConfigObjError::BadValue(s.to_string()));
    }
    // Unquoted: the value ends at the first `#` (which starts a comment), with
    // trailing whitespace before the `#` moved into `trailing`.
    match s.find('#') {
        Some(h) => {
            let value = s[..h].trim_end();
            let trailing = &s[value.len()..];
            Ok((value.to_string(), trailing.to_string()))
        }
        None => Ok((s.trim_end().to_string(), String::new())),
    }
}

/// Quote a scalar `value` for writing, matching configobj's `_quote` with the
/// settings breezy's store uses (`list_values=True`, `multiline=True`,
/// `write_empty_values=False`).
///
/// Returns `None` when the value cannot be safely quoted (it contains both a
/// `'''` and a `"""`, or both a `'` and a `"` and needs single-quoting),
/// mirroring configobj raising `ConfigObjError`.
pub fn quote_value(value: &str) -> Option<String> {
    // configobj's wspace_plus: whitespace plus the two quote chars.
    fn is_wspace_plus(c: char) -> bool {
        matches!(c, ' ' | '\r' | '\n' | '\u{0b}' | '\t' | '\'' | '"')
    }

    if value.is_empty() {
        return Some("\"\"".to_string());
    }

    let has_single = value.contains('\'');
    let has_double = value.contains('"');
    let need_triple = (has_single && has_double) || value.contains('\n');

    if !need_triple {
        // check_for_single branch.
        let first = value.chars().next().unwrap();
        let last = value.chars().next_back().unwrap();
        let mut quoted = if !is_wspace_plus(first) && !is_wspace_plus(last) && !value.contains(',')
        {
            value.to_string()
        } else {
            single_or_double_quote(value, has_single, has_double)?
        };
        // A `noquot` result still gets single/double quoted if it contains `#`
        // (list_values is True).
        if quoted == value && value.contains('#') {
            quoted = single_or_double_quote(value, has_single, has_double)?;
        }
        Some(quoted)
    } else {
        // Triple quoting (configobj's _get_triple_quote): `'''` unless the value
        // already contains `"""`, in which case `"""`; both present is
        // unquotable.
        let has_triple_double = value.contains("\"\"\"");
        let has_triple_single = value.contains("'''");
        if has_triple_double && has_triple_single {
            return None;
        }
        if has_triple_double {
            Some(format!("\"\"\"{value}\"\"\""))
        } else {
            Some(format!("'''{value}'''"))
        }
    }
}

/// configobj's `_get_single_quote`: double quotes normally, single quotes if the
/// value contains a `"`, and unquotable if it contains both quote kinds.
fn single_or_double_quote(value: &str, has_single: bool, has_double: bool) -> Option<String> {
    if has_single && has_double {
        None
    } else if has_double {
        Some(format!("'{value}'"))
    } else {
        Some(format!("\"{value}\""))
    }
}

/// Strip a matched surrounding quote pair from a raw value, as configobj's
/// `_unquote`. Empty input is returned unchanged (configobj raises, but breezy's
/// `Store.unquote` guards against empty/non-string before calling this).
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
        // Matches configobj's _quote (list_values=True, multiline=True).
        assert_eq!(quote_value("plain").unwrap(), "plain");
        assert_eq!(quote_value("").unwrap(), "\"\"");
        assert_eq!(quote_value(" leading").unwrap(), "\" leading\"");
        assert_eq!(quote_value("trailing ").unwrap(), "\"trailing \"");
        assert_eq!(quote_value("a,b").unwrap(), "\"a,b\"");
        assert_eq!(quote_value(",").unwrap(), "\",\"");
        assert_eq!(quote_value("has#hash").unwrap(), "\"has#hash\"");
        assert_eq!(quote_value("#leadinghash").unwrap(), "\"#leadinghash\"");
        // A mid-string quote needs no quoting (no comma/#, not an edge char).
        assert_eq!(quote_value("a'b").unwrap(), "a'b");
        assert_eq!(quote_value("a\"b").unwrap(), "a\"b");
        // A value that needs quoting and contains a double quote uses singles.
        assert_eq!(quote_value("\" a b c \"").unwrap(), "'\" a b c \"'");
        assert_eq!(quote_value("\",\"").unwrap(), "'\",\"'");
        // A newline forces triple quotes.
        assert_eq!(quote_value("a\nb").unwrap(), "'''a\nb'''");
        // Both quote kinds present -> triple quotes.
        assert_eq!(
            quote_value("has both ' and \"").unwrap(),
            "'''has both ' and \"'''"
        );
    }

    #[test]
    fn quote_value_unquotable_returns_none() {
        // A value containing both `'''` and `"""` cannot be safely quoted.
        assert_eq!(quote_value("a '''and''' b \"\"\"c\"\"\""), None);
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
    fn unterminated_quote_is_an_error() {
        // configobj raises a ParseError for a value that opens a quote it never
        // closes; we mirror that rather than keeping the partial value.
        match ConfigObj::parse(b"a = \"oops\n") {
            Err(ConfigObjError::BadValue(v)) => assert_eq!(v, "\"oops"),
            other => panic!("expected BadValue, got {:?}", other.map(|_| "ok")),
        }
    }

    #[test]
    fn quoted_value_followed_by_more_is_kept_whole() {
        // A closing quote followed by non-comment text is not the end of the
        // value; configobj (list_values=False) keeps the whole raw string.
        let input = b"a = \" bar\", \"baz \"\n";
        let c = ConfigObj::parse(input).unwrap();
        assert_eq!(
            c.section(None).unwrap().get("a"),
            Some("\" bar\", \"baz \"")
        );
        assert_eq!(c.to_bytes(), input);
    }

    #[test]
    fn unquoted_hash_starts_comment_without_space() {
        // For an unquoted value, `#` starts a comment even with no preceding
        // space, so `has#hash` yields `has`.
        let c = ConfigObj::parse(b"a = has#hash\n").unwrap();
        assert_eq!(c.section(None).unwrap().get("a"), Some("has"));
    }

    #[test]
    fn value_that_is_only_a_comment_is_empty() {
        let c = ConfigObj::parse(b"a = #just a comment\n").unwrap();
        assert_eq!(c.section(None).unwrap().get("a"), Some(""));
    }

    #[test]
    fn unquoted_value_may_contain_a_bare_quote() {
        let c = ConfigObj::parse(b"a = end\"quote\n").unwrap();
        assert_eq!(c.section(None).unwrap().get("a"), Some("end\"quote"));
    }

    #[test]
    fn triple_quoted_multiline_value_is_unquoted() {
        // A `'''...'''` block spanning several lines yields the inner content
        // (without the quotes), matching configobj even with list_values=False.
        let c = ConfigObj::parse(b"multiline = '''1\n2\n'''\nother = x\n").unwrap();
        let sec = c.section(None).unwrap();
        assert_eq!(sec.get("multiline"), Some("1\n2\n"));
        assert_eq!(sec.get("other"), Some("x"));
    }

    #[test]
    fn triple_quoted_multiline_round_trips() {
        let input = b"multiline = '''1\n2\n'''\n";
        let c = ConfigObj::parse(input).unwrap();
        assert_eq!(c.to_bytes(), input);
    }

    #[test]
    fn triple_quoted_single_line_value() {
        let c = ConfigObj::parse(b"a = '''one line'''\n").unwrap();
        assert_eq!(c.section(None).unwrap().get("a"), Some("one line"));
    }

    #[test]
    fn double_triple_quoted_multiline_value() {
        let c = ConfigObj::parse(b"a = \"\"\"x\ny\"\"\"\n").unwrap();
        assert_eq!(c.section(None).unwrap().get("a"), Some("x\ny"));
    }

    #[test]
    fn unterminated_triple_quote_is_an_error() {
        match ConfigObj::parse(b"a = '''oops\nno close\n") {
            Err(ConfigObjError::BadValue(_)) => {}
            other => panic!("expected BadValue, got {:?}", other.map(|_| "ok")),
        }
    }

    #[test]
    fn triple_quoted_value_containing_triple_single_uses_double() {
        // A stored value that itself contains `'''` must be written with `"""`.
        let c = ConfigObj::parse(b"a = \"\"\"has ''' inside\"\"\"\n").unwrap();
        assert_eq!(c.section(None).unwrap().get("a"), Some("has ''' inside"));
        assert_eq!(c.to_bytes(), b"a = \"\"\"has ''' inside\"\"\"\n");
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

    #[test]
    fn empty_section_headers_round_trip() {
        // Headers with no entries must survive parse+write (LocationStore uses
        // the section names as path globs even when a section has no options).
        let input = b"[/foo]\n[/foo/bar]\n";
        let c = ConfigObj::parse(input).unwrap();
        assert_eq!(c.to_bytes(), input);
        // They have no depth-1 entries, so `section` returns None...
        assert!(c.section(Some("/foo")).is_none());
        // ...but the tree lists them in order.
        let tree = c.section_tree();
        assert_eq!(
            tree.iter().map(|s| s.name.clone()).collect::<Vec<_>>(),
            vec![Some("/foo".to_string()), Some("/foo/bar".to_string())]
        );
    }

    #[test]
    fn empty_sections_excluded_from_sections() {
        // `sections()` (the Stack view) only lists sections with direct entries.
        let c = ConfigObj::parse(b"[/foo]\n[/bar]\nk = v\n").unwrap();
        assert_eq!(
            c.sections()
                .iter()
                .map(|s| s.id().map(str::to_string))
                .collect::<Vec<_>>(),
            vec![Some("/bar".to_string())]
        );
    }

    #[test]
    fn nested_subsection_parses_and_round_trips() {
        let input = b"foo = bar\n[baz]\nfoo_in_baz = barbaz\n[[qux]]\nfoo_in_qux = quux\n";
        let c = ConfigObj::parse(input).unwrap();
        assert_eq!(c.to_bytes(), input);
        // The depth-1 view of [baz] sees only its own scalar, not the subsection.
        let baz = c.section(Some("baz")).unwrap();
        assert_eq!(baz.get("foo_in_baz"), Some("barbaz"));
        assert_eq!(baz.get("foo_in_qux"), None);
    }

    #[test]
    fn section_tree_nests_subsections() {
        let c = ConfigObj::parse(
            b"foo = bar\n[baz]\nfoo_in_baz = barbaz\n[[qux]]\nfoo_in_qux = quux\n",
        )
        .unwrap();
        let tree = c.section_tree();
        assert_eq!(
            tree.iter().map(|s| s.name.clone()).collect::<Vec<_>>(),
            vec![None, Some("baz".to_string())]
        );
        let no_name = &tree[0];
        assert_eq!(
            no_name.options,
            vec![("foo".to_string(), "bar".to_string())]
        );
        let baz = &tree[1];
        assert_eq!(
            baz.options,
            vec![("foo_in_baz".to_string(), "barbaz".to_string())]
        );
        assert_eq!(baz.subsections.len(), 1);
        assert_eq!(baz.subsections[0].name, Some("qux".to_string()));
        assert_eq!(
            baz.subsections[0].options,
            vec![("foo_in_qux".to_string(), "quux".to_string())]
        );
    }

    #[test]
    fn triple_nesting_is_rejected() {
        match ConfigObj::parse(b"[a]\n[[b]]\n[[[c]]]\n") {
            Err(ConfigObjError::NestingTooDeep(line)) => assert_eq!(line, "[[[c]]]"),
            other => panic!("expected NestingTooDeep, got {:?}", other.map(|_| "ok")),
        }
    }

    #[test]
    fn subsection_before_any_section_is_bad_header() {
        // A `[[sub]]` with no preceding `[name]` has no parent.
        assert!(matches!(
            ConfigObj::parse(b"[[orphan]]\n"),
            Err(ConfigObjError::BadSectionHeader(_))
        ));
    }

    #[test]
    fn set_subsection_value_builds_nested_structure() {
        // From empty: creating a subsection value creates both headers.
        let mut c = ConfigObj::empty();
        c.set_value(Some("baz"), "foo_in_baz", "barbaz");
        c.set_subsection_value("baz", "qux", "foo_in_qux", "quux");
        assert_eq!(
            c.to_bytes(),
            b"[baz]\nfoo_in_baz = barbaz\n[[qux]]\nfoo_in_qux = quux\n"
        );
    }

    #[test]
    fn set_subsection_value_updates_in_place() {
        let mut c = ConfigObj::parse(b"[baz]\n[[qux]]\na = 1\n").unwrap();
        c.set_subsection_value("baz", "qux", "a", "2");
        assert_eq!(c.to_bytes(), b"[baz]\n[[qux]]\na = 2\n");
    }

    #[test]
    fn set_value_in_section_with_subsection_lands_before_subsection() {
        // A new depth-1 key for [baz] must go after [baz]'s own entries and
        // before its [[qux]] subsection header.
        let mut c = ConfigObj::parse(b"[baz]\na = 1\n[[qux]]\nb = 2\n").unwrap();
        c.set_value(Some("baz"), "c", "3");
        assert_eq!(c.to_bytes(), b"[baz]\na = 1\nc = 3\n[[qux]]\nb = 2\n");
    }
}
