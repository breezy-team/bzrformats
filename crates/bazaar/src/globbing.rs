//! Tools for converting globs to regular expressions.
//!
//! This module provides functions for converting shell-like globs to regular
//! expressions.

pub use fancy_regex::{Captures, Error, Match, Regex};
use lazy_static::lazy_static;
use std::sync::Arc;

lazy_static! {
    static ref SLASHES_RE: Regex = Regex::new(r"[\\/]+").unwrap();
    static ref EXPAND_RE: Regex = Regex::new("\\\\&").unwrap();
}

/// Converts backslashes in path patterns to forward slashes.
/// Doesn't normalize regular expressions - they may contain escapes.
pub fn normalize_pattern(pattern: &str) -> String {
    let mut pattern = pattern.to_string();
    if !(pattern.starts_with("RE:") || pattern.starts_with("!RE:")) {
        pattern = SLASHES_RE.replace_all(pattern.as_str(), "/").to_string();
    }
    if pattern.len() > 1 {
        pattern = pattern.trim_end_matches('/').to_string();
    }
    pattern
}

pub enum Replacement {
    String(String),
    Function(fn(&str) -> String),
    Closure(Box<dyn FnMut(String) -> String + Sync + Send>),
}

// The regex crate's RegexSet only reports which patterns matched, not their
// spans, so it cannot drive per-pattern substitution; the single-alternation
// approach below is what makes replace_all able to pick the right replacement.
// The patterns also come from breezy and may use fancy-regex features.

/// Do a multiple-pattern substitution.
///
/// The patterns and substitutions are combined into one, so the result of
/// one replacement is never substituted again. Add the patterns and
/// replacements via the add method and then call the object. The patterns
/// must not contain capturing groups.
pub struct Replacer {
    compiled: Option<Regex>,
    pats: Vec<(String, Arc<Replacement>)>,
}

impl Replacer {
    pub fn new(source: Option<&Self>) -> Self {
        let mut ret = Self::empty();
        if let Some(source) = source {
            ret.add_replacer(source);
        }
        ret
    }

    pub fn empty() -> Self {
        Self {
            compiled: None,
            pats: Vec::new(),
        }
    }

    /// Add a pattern and replacement.
    ///
    /// The pattern must not contain capturing groups.
    /// The replacement might be either a string template in which \& will be
    /// replaced with the match, or a function that will get the matching text
    /// as argument. It does not get match object, because capturing is
    /// forbidden anyway.
    pub fn add(&mut self, pat: &str, fun: Replacement) {
        // Need to recompile
        self.compiled = None;
        self.pats.push((pat.to_string(), Arc::new(fun)));
    }

    pub fn add_validate(&mut self, pat: &str, fun: Replacement) -> Result<(), Error> {
        Regex::new(pat)?;
        self.add(pat, fun);
        Ok(())
    }

    /// Add all patterns from another replacer.
    ///
    /// All patterns and replacements from replacer are appended to the ones
    /// already defined.
    pub fn add_replacer(&mut self, replacer: &Replacer) {
        self.compiled = None;
        self.pats.extend(replacer.pats.clone());
    }

    pub fn replace(&mut self, text: &str) -> std::result::Result<String, Error> {
        if self.pats.is_empty() {
            return Ok(text.to_string());
        }
        if self.compiled.is_none() {
            let pat_str = self
                .pats
                .iter()
                .map(|(pat, _)| format!("({})", pat))
                .collect::<Vec<_>>()
                .join("|");
            self.compiled = Some(Regex::new(&pat_str)?);
        }
        let pats = &mut self.pats;

        fn expand(text: &str, rep: &str) -> String {
            rep.replace("\\&", text)
        }

        fn sub(m: &Match, rep: &mut Arc<Replacement>) -> String {
            let replacement = Arc::get_mut(rep).unwrap();
            match replacement {
                Replacement::String(s) => expand(m.as_str(), s.as_str()),
                Replacement::Function(f) => f(m.as_str()),
                Replacement::Closure(f) => f(m.as_str().to_string()),
            }
        }

        Ok(self
            .compiled
            .as_ref()
            .unwrap()
            .replace_all(text, |caps: &Captures| {
                for (index, m) in caps.iter().skip(1).enumerate() {
                    if let Some(m) = m {
                        return sub(&m, &mut pats[index].1);
                    }
                }
                unreachable!();
            })
            .to_string())
    }
}

fn sub_named() -> Replacer {
    let mut r = Replacer::empty();
    r.add(r"\[:digit:\]", Replacement::String(r"\d".to_string()));
    r.add(r"\[:space:\]", Replacement::String(r"\s".to_string()));
    r.add(r"\[:alnum:\]", Replacement::String(r"\w".to_string()));
    // Python's re spells the null byte "\0", but fancy-regex reads "\0" as a
    // group-0 backreference; "\x00" denotes the identical codepoint and parses.
    r.add(
        r"\[:ascii:\]",
        Replacement::String(r"\x00-\x7f".to_string()),
    );
    r.add(r"\[:blank:\]", Replacement::String(" \\t".to_string()));
    r.add(
        r"\[:cntrl:\]",
        Replacement::String(r"\x00-\x1f\x7f-\x9f".to_string()),
    );
    r
}

fn sub_group(m: &str) -> String {
    let inner = &mut sub_named();
    let bytes = m.as_bytes();
    if bytes.len() >= 2 && (bytes[1] == b'!' || bytes[1] == b'^') {
        // "[!..." or "[^..." -> "[^...]"
        format!("[^{}]", inner.replace(&m[2..m.len() - 1]).unwrap())
    } else {
        format!("[{}]", inner.replace(&m[1..m.len() - 1]).unwrap())
    }
}

fn invalid_regex(m: &str, repl: &str) -> String {
    log::warn!("'{m}' not allowed within a regular expression. Replacing with '{repl}'");
    repl.to_string()
}

/// Does a head count on trailing backslashes to ensure there isn't an odd one
/// on the end that would escape the brackets we wrap the RE in.
fn trailing_backslashes_regex(m: &str) -> String {
    if !m.len().is_multiple_of(2) {
        log::warn!(
            "Regular expressions cannot end with an odd number of '\\'. Dropping the final '\\'."
        );
        m[..m.len() - 1].to_string()
    } else {
        m.to_string()
    }
}

fn sub_re() -> Replacer {
    let mut r = Replacer::empty();
    r.add("^RE:", Replacement::String(String::new()));
    r.add(r"\((?!\?)", Replacement::String("(?:".to_string()));
    r.add(
        r"\(\?P<.*>",
        Replacement::Closure(Box::new(|m| invalid_regex(&m, "(?:"))),
    );
    r.add(
        r"\(\?P=[^)]*\)",
        Replacement::Closure(Box::new(|m| invalid_regex(&m, ""))),
    );
    r.add(
        r"\\+$",
        Replacement::Closure(Box::new(|m| trailing_backslashes_regex(&m))),
    );
    r
}

fn sub_fullpath() -> Replacer {
    let mut r = Replacer::empty();
    r.add(
        r"^RE:.*",
        Replacement::Closure(Box::new(|m| sub_re().replace(&m).unwrap())),
    ); // RE:<anything> is a regex
    r.add(
        r"\[\^?\]?(?:[^\]\[]|\[:[^\]]+:\])+\]",
        Replacement::Function(sub_group),
    ); // char group
    r.add(r"(?:(?<=/)|^)(?:\.?/)+", Replacement::String(String::new())); // canonicalize path
    r.add(r"\\.", Replacement::String(r"\&".to_string())); // keep anything backslashed
    r.add(r"[(){}|^$+.]", Replacement::String(r"\\&".to_string())); // escape specials
    r.add(
        r"(?:(?<=/)|^)\*\*+/",
        Replacement::String(r"(?:.*/)?".to_string()),
    ); // **/ after ^ or /
    r.add(r"\*+", Replacement::String(r"[^/]*".to_string())); // * elsewhere
    r.add(r"\?", Replacement::String(r"[^/]".to_string())); // ? everywhere
    r
}

fn sub_basename() -> Replacer {
    let mut r = Replacer::empty();
    r.add(
        r"\[\^?\]?(?:[^\]\[]|\[:[^\]]+:\])+\]",
        Replacement::Function(sub_group),
    ); // char group
    r.add(r"\\.", Replacement::String(r"\&".to_string())); // keep anything backslashed
    r.add(r"[(){}|^$+.]", Replacement::String(r"\\&".to_string())); // escape specials
    r.add(r"\*+", Replacement::String(r".*".to_string())); // * everywhere
    r.add(r"\?", Replacement::String(r".".to_string())); // ? everywhere
    r
}

/// Pattern category, ordered shortest to longest to match Python's build order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PatternKind {
    Extension,
    Basename,
    Fullpath,
}

impl PatternKind {
    fn prefix(self) -> &'static str {
        match self {
            PatternKind::Extension => r"(?:.*/)?(?!.*/)(?:.*\.)",
            PatternKind::Basename => r"(?:.*/)?(?!.*/)",
            PatternKind::Fullpath => "",
        }
    }

    fn translate(self, pattern: &str) -> String {
        match self {
            // extension patterns are basename patterns with the "*." stripped
            PatternKind::Extension => sub_basename().replace(&pattern[2..]).unwrap(),
            PatternKind::Basename => sub_basename().replace(pattern).unwrap(),
            PatternKind::Fullpath => sub_fullpath().replace(pattern).unwrap(),
        }
    }
}

/// Identify whether a normalized pattern is fullpath, basename or extension.
pub fn identify(pattern: &str) -> PatternKind {
    if pattern.starts_with("RE:") || pattern.contains('/') {
        PatternKind::Fullpath
    } else if pattern.starts_with("*.") {
        PatternKind::Extension
    } else {
        PatternKind::Basename
    }
}

/// Returns true if the normalized pattern compiles to a valid regex.
pub fn is_pattern_valid(pattern: &str) -> bool {
    let fragment = identify(pattern).translate(pattern);
    Regex::new(&format!("({fragment})")).is_ok()
}

/// The grouping limit imposed by Python's `re` module; kept so the generated
/// super-regex is byte-identical to breezy's.
const GROUP_LIMIT: usize = 99;

/// A set of glob patterns compiled to regexes for fast matching.
///
/// Patterns are categorised (extension/basename/fullpath), translated to
/// regexes, and aggregated into super-regexes of up to 99 alternatives each,
/// with each alternative wrapped in a capturing group so the matching pattern
/// can be recovered. See the Python `breezy.globbing.Globster` for the
/// rationale behind the categories and ordering.
pub struct Globster {
    // None regex means the aggregated pattern failed to compile; the offending
    // pattern(s) are surfaced lazily at match time (as Python does).
    regex_patterns: Vec<(Option<Regex>, Vec<String>)>,
}

/// A pattern (or combined pattern) that could not be compiled to a regex.
///
/// Carries the individually-invalid patterns so the caller can report which
/// ignore lines need fixing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidPattern {
    pub bad_patterns: Vec<String>,
}

impl std::fmt::Display for InvalidPattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Invalid pattern(s): {}", self.bad_patterns.join(", "))
    }
}

impl std::error::Error for InvalidPattern {}

impl Globster {
    pub fn new<I, S>(patterns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut buckets: Vec<Vec<String>> = vec![Vec::new(), Vec::new(), Vec::new()];
        for pat in patterns {
            let pat = normalize_pattern(pat.as_ref());
            let idx = match identify(&pat) {
                PatternKind::Extension => 0,
                PatternKind::Basename => 1,
                PatternKind::Fullpath => 2,
            };
            buckets[idx].push(pat);
        }
        let mut globster = Globster {
            regex_patterns: Vec::new(),
        };
        let kinds = [
            PatternKind::Extension,
            PatternKind::Basename,
            PatternKind::Fullpath,
        ];
        for (idx, kind) in kinds.iter().enumerate() {
            globster.add_patterns(&buckets[idx], *kind);
        }
        globster
    }

    /// Build the aggregated regex source for a chunk of same-kind patterns.
    fn joined_regex(chunk: &[String], kind: PatternKind) -> String {
        let grouped: Vec<String> = chunk
            .iter()
            .map(|pat| format!("({})", kind.translate(pat)))
            .collect();
        format!("{}(?:{})$", kind.prefix(), grouped.join("|"))
    }

    fn add_patterns(&mut self, patterns: &[String], kind: PatternKind) {
        for chunk in patterns.chunks(GROUP_LIMIT) {
            let joined = Self::joined_regex(chunk, kind);
            // Python matches with re.match (anchored at the start); fancy-regex
            // has only search, so anchor with a leading \A.
            let anchored = format!(r"\A{joined}");
            // An invalid combined regex is deferred to match time, matching the
            // Python behaviour of lazy compilation with per-pattern diagnostics.
            let regex = Regex::new(&anchored).ok();
            self.regex_patterns.push((regex, chunk.to_vec()));
        }
    }

    /// Return the original pattern that matches `filename`, or `None`.
    ///
    /// Returns [`InvalidPattern`] if a pattern group failed to compile, listing
    /// the individually-invalid patterns.
    pub fn match_(&self, filename: &str) -> Result<Option<String>, InvalidPattern> {
        for (regex, patterns) in &self.regex_patterns {
            let regex = match regex {
                Some(regex) => regex,
                None => return Err(self.invalid_pattern_error()),
            };
            if let Ok(Some(caps)) = regex.captures(filename) {
                // Each pattern fragment is wrapped in exactly one capturing
                // group, so the first group that participated identifies it.
                for i in 1..caps.len() {
                    if caps.get(i).is_some() {
                        return Ok(Some(patterns[i - 1].clone()));
                    }
                }
            }
        }
        Ok(None)
    }

    fn invalid_pattern_error(&self) -> InvalidPattern {
        let bad_patterns = self
            .regex_patterns
            .iter()
            .flat_map(|(_, patterns)| patterns.iter())
            .filter(|p| !is_pattern_valid(p))
            .cloned()
            .collect();
        InvalidPattern { bad_patterns }
    }
}

/// A [`Globster`] that supports exception patterns.
///
/// Patterns prefixed with `!` are exceptions that suppress a match; `!!`
/// patterns are highest precedence and act as regular ignores (useful to
/// re-establish ignores under a `!` exception path).
pub struct ExceptionGlobster {
    ignores: [Globster; 3],
}

impl ExceptionGlobster {
    pub fn new<I, S>(patterns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut normal = Vec::new();
        let mut except = Vec::new();
        let mut double = Vec::new();
        for p in patterns {
            let p = p.as_ref();
            if let Some(rest) = p.strip_prefix("!!") {
                double.push(rest.to_string());
            } else if let Some(rest) = p.strip_prefix('!') {
                except.push(rest.to_string());
            } else {
                normal.push(p.to_string());
            }
        }
        ExceptionGlobster {
            ignores: [
                Globster::new(normal),
                Globster::new(except),
                Globster::new(double),
            ],
        }
    }

    /// Return the matching pattern, or `None` (also `None` when an exception
    /// pattern matches).
    pub fn match_(&self, filename: &str) -> Result<Option<String>, InvalidPattern> {
        if let Some(double_neg) = self.ignores[2].match_(filename)? {
            return Ok(Some(format!("!!{double_neg}")));
        }
        if self.ignores[1].match_(filename)?.is_some() {
            return Ok(None);
        }
        self.ignores[0].match_(filename)
    }
}

/// A [`Globster`] that keeps pattern order, aggregating one pattern per regex.
pub struct OrderedGlobster {
    inner: Globster,
}

impl OrderedGlobster {
    pub fn new<I, S>(patterns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut inner = Globster {
            regex_patterns: Vec::new(),
        };
        for pat in patterns {
            let pat = normalize_pattern(pat.as_ref());
            let kind = identify(&pat);
            inner.add_patterns(std::slice::from_ref(&pat), kind);
        }
        OrderedGlobster { inner }
    }

    /// Return the original pattern that matches `filename`, or `None`.
    pub fn match_(&self, filename: &str) -> Result<Option<String>, InvalidPattern> {
        self.inner.match_(filename)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(text: &str) -> Replacement {
        Replacement::String(text.to_string())
    }

    #[test]
    fn test_replacer_simple() {
        let mut r = Replacer::empty();
        r.add("a", s("b"));
        assert_eq!(r.replace("a").unwrap(), "b");
    }

    #[test]
    fn test_replacer_function() {
        let mut r = Replacer::empty();
        r.add(
            "a",
            Replacement::Function(|m| {
                assert_eq!(m, "a");
                "c".to_string()
            }),
        );
        assert_eq!(r.replace("a").unwrap(), "c");
    }

    #[test]
    fn test_replacer_multiple() {
        let mut r = Replacer::empty();
        r.add("a", s("b"));
        r.add("c", s("d"));
        assert_eq!(r.replace("a").unwrap(), "b");
        assert_eq!(r.replace("c").unwrap(), "d");
    }

    #[test]
    fn test_replacer_none() {
        let mut r = Replacer::empty();
        assert_eq!(r.replace("a").unwrap(), "a");
    }

    #[test]
    fn test_replacer_partial() {
        let mut r = Replacer::empty();
        r.add("a", s("b"));
        assert_eq!(r.replace("ac").unwrap(), "bc");
    }

    #[test]
    fn test_replacer_expands_ampersand() {
        // "\&" in the replacement expands to the matched text.
        let mut r = Replacer::empty();
        r.add("a", s("[\\&]"));
        assert_eq!(r.replace("xax").unwrap(), "x[a]x");
    }

    #[test]
    fn test_normalize_pattern_backslashes() {
        assert_eq!(normalize_pattern("\\"), "/");
        assert_eq!(normalize_pattern("\\\\"), "/");
        assert_eq!(normalize_pattern("\\foo\\bar"), "/foo/bar");
        assert_eq!(normalize_pattern("foo\\bar\\"), "foo/bar");
        assert_eq!(normalize_pattern("\\\\foo\\\\bar\\\\"), "/foo/bar");
    }

    #[test]
    fn test_normalize_pattern_forward_slashes() {
        assert_eq!(normalize_pattern("/"), "/");
        assert_eq!(normalize_pattern("//"), "/");
        assert_eq!(normalize_pattern("/foo/bar"), "/foo/bar");
        assert_eq!(normalize_pattern("foo/bar/"), "foo/bar");
        assert_eq!(normalize_pattern("//foo//bar//"), "/foo/bar");
    }

    #[test]
    fn test_normalize_pattern_mixed_slashes() {
        assert_eq!(normalize_pattern("\\/\\foo//\\///bar/\\\\/"), "/foo/bar");
    }

    #[test]
    fn test_normalize_pattern_leaves_regex_untouched() {
        // RE:/!RE: prefixed patterns must not have their slashes collapsed.
        assert_eq!(normalize_pattern("RE:a//b"), "RE:a//b");
        assert_eq!(normalize_pattern("!RE:a\\\\b"), "!RE:a\\\\b");
    }

    /// Assert the aggregated regex source for a single-pattern Globster equals
    /// the byte-exact output of the reference Python implementation.
    fn check(pattern: &str, expected_joined: &str) {
        let normalized = normalize_pattern(pattern);
        let kind = identify(&normalized);
        let joined = Globster::joined_regex(std::slice::from_ref(&normalized), kind);
        assert_eq!(joined, expected_joined, "pattern {pattern:?}");
    }

    #[test]
    fn test_translation_matches_python() {
        check("*.o", r"(?:.*/)?(?!.*/)(?:.*\.)(?:(o))$");
        check("*.py[co]", r"(?:.*/)?(?!.*/)(?:.*\.)(?:(py[co]))$");
        check("*~", r"(?:.*/)?(?!.*/)(?:(.*~))$");
        check(".#*", r"(?:.*/)?(?!.*/)(?:(\.#.*))$");
        check("[#]*#", r"(?:.*/)?(?!.*/)(?:([#].*#))$");
        check("__pycache__", r"(?:.*/)?(?!.*/)(?:(__pycache__))$");
        check("bzr-orphans", r"(?:.*/)?(?!.*/)(?:(bzr-orphans))$");
        check("foo/bar", r"(?:(foo/bar))$");
        check("foo/*.o", r"(?:(foo/[^/]*\.o))$");
        check("**/*.tmp", r"(?:((?:.*/)?[^/]*\.tmp))$");
        check("src/**/x", r"(?:(src/(?:.*/)?x))$");
        check("a?c", r"(?:.*/)?(?!.*/)(?:(a.c))$");
        check("a*b*c", r"(?:.*/)?(?!.*/)(?:(a.*b.*c))$");
        check("[[:digit:]]", r"(?:.*/)?(?!.*/)(?:([\d]))$");
        check("[^[:digit:]]", r"(?:.*/)?(?!.*/)(?:([^\d]))$");
        check("[[:space:]]", r"(?:.*/)?(?!.*/)(?:([\s]))$");
        check("[[:alnum:]]", r"(?:.*/)?(?!.*/)(?:([\w]))$");
        // [:ascii:]/[:cntrl:] use \x00 where Python uses \0 (same null byte);
        // fancy-regex reads \0 as a backreference, so \x00 is the portable form.
        check("[[:ascii:]]", r"(?:.*/)?(?!.*/)(?:([\x00-\x7f]))$");
        check("[[:blank:]]", r"(?:.*/)?(?!.*/)(?:([ \t]))$");
        check("[[:cntrl:]]", r"(?:.*/)?(?!.*/)(?:([\x00-\x1f\x7f-\x9f]))$");
        check("[abc]", r"(?:.*/)?(?!.*/)(?:([abc]))$");
        check("[!abc]", r"(?:.*/)?(?!.*/)(?:([^abc]))$");
        check("[a-z]", r"(?:.*/)?(?!.*/)(?:([a-z]))$");
        check("RE:.*\\.py$", r"(?:(.*\.py$))$");
        check("RE:foo(bar)", r"(?:(foo(?:bar)))$");
        check("RE:(?P<x>a)", r"(?:((?:a)))$");
        check("RE:a\\", r"(?:(a))$");
        check("dir/", r"(?:.*/)?(?!.*/)(?:(dir))$");
        check("./foo", r"(?:(foo))$");
        check("foo(bar)", r"(?:.*/)?(?!.*/)(?:(foo\(bar\)))$");
        check("a{b}c", r"(?:.*/)?(?!.*/)(?:(a\{b\}c))$");
        check("a|b", r"(?:.*/)?(?!.*/)(?:(a\|b))$");
        check("a+b", r"(?:.*/)?(?!.*/)(?:(a\+b))$");
        check("a.b", r"(?:.*/)?(?!.*/)(?:(a\.b))$");
        check("a^b", r"(?:.*/)?(?!.*/)(?:(a\^b))$");
        check("a$b", r"(?:.*/)?(?!.*/)(?:(a\$b))$");
        check("*.tar.gz", r"(?:.*/)?(?!.*/)(?:.*\.)(?:(tar\.gz))$");
        check("*.[ch]", r"(?:.*/)?(?!.*/)(?:.*\.)(?:([ch]))$");
        check("test_*.py", r"(?:.*/)?(?!.*/)(?:(test_.*\.py))$");
    }

    #[test]
    fn test_identify() {
        assert_eq!(identify("foo/bar"), PatternKind::Fullpath);
        assert_eq!(identify("RE:x"), PatternKind::Fullpath);
        assert_eq!(identify("*.o"), PatternKind::Extension);
        assert_eq!(identify("foo"), PatternKind::Basename);
        assert_eq!(identify("*~"), PatternKind::Basename);
    }

    #[test]
    fn test_match_returns_original_pattern() {
        let g = Globster::new(["*.o", "*~", "foo/bar"]);
        assert_eq!(g.match_("x.o").unwrap().as_deref(), Some("*.o"));
        assert_eq!(g.match_("a~").unwrap().as_deref(), Some("*~"));
        assert_eq!(g.match_("foo/bar").unwrap().as_deref(), Some("foo/bar"));
        assert_eq!(g.match_("baz").unwrap(), None);
    }

    #[test]
    fn test_match_basename_across_dirs() {
        let g = Globster::new(["*.o"]);
        assert_eq!(g.match_("dir/sub/x.o").unwrap().as_deref(), Some("*.o"));
    }

    #[test]
    fn test_match_char_group() {
        let g = Globster::new(["[[:digit:]]"]);
        assert!(g.match_("5").unwrap().is_some());
        assert!(g.match_("a").unwrap().is_none());
        let g = Globster::new(["[^[:digit:]]"]);
        assert!(g.match_("a").unwrap().is_some());
        assert!(g.match_("5").unwrap().is_none());
    }

    #[test]
    fn test_match_re_pattern() {
        let g = Globster::new(["RE:.*\\.py$"]);
        assert_eq!(g.match_("foo.py").unwrap().as_deref(), Some("RE:.*\\.py$"));
        assert!(g.match_("foo.txt").unwrap().is_none());
    }

    #[test]
    fn test_double_star() {
        let g = Globster::new(["**/*.tmp"]);
        assert!(g.match_("a/b/c.tmp").unwrap().is_some());
        assert!(g.match_("c.tmp").unwrap().is_some());
        assert!(g.match_("c.txt").unwrap().is_none());
    }

    #[test]
    fn test_exception_globster() {
        let g = ExceptionGlobster::new(["*.o", "!important.o"]);
        assert_eq!(g.match_("foo.o").unwrap().as_deref(), Some("*.o"));
        assert_eq!(g.match_("important.o").unwrap(), None);
    }

    #[test]
    fn test_exception_globster_double_neg() {
        let g = ExceptionGlobster::new(["*.o", "!build/*", "!!build/keep.o"]);
        // Under the !build/* exception, !!build/keep.o re-establishes the ignore.
        assert_eq!(
            g.match_("build/keep.o").unwrap().as_deref(),
            Some("!!build/keep.o")
        );
        assert_eq!(g.match_("build/other.o").unwrap(), None);
    }

    #[test]
    fn test_ordered_globster_keeps_order() {
        let g = OrderedGlobster::new(["*.foo", "bar.*"]);
        assert_eq!(g.match_("bar.foo").unwrap().as_deref(), Some("*.foo"));
        let g = OrderedGlobster::new(["bar.*", "*.foo"]);
        assert_eq!(g.match_("bar.foo").unwrap().as_deref(), Some("bar.*"));
    }

    #[test]
    fn test_is_pattern_valid() {
        assert!(is_pattern_valid("*.o"));
        assert!(is_pattern_valid("RE:.*"));
    }

    #[test]
    fn test_ascii_and_cntrl_classes_match() {
        // [:ascii:]/[:cntrl:] translate to \x00 ranges; they must compile and
        // match (a plain \0 would be read as a backreference by fancy-regex).
        let g = Globster::new(["[[:ascii:]]"]);
        assert!(g.match_("a").unwrap().is_some());
        assert!(g.match_("\u{8336}").unwrap().is_none());
        let g = Globster::new(["[[:cntrl:]]"]);
        assert!(g.match_("\u{7f}").unwrap().is_some());
        assert!(g.match_("a").unwrap().is_none());
    }

    #[test]
    fn test_match_is_start_anchored() {
        // A single-char class must not match a longer name (regression: search
        // vs anchored match).
        let g = Globster::new(["[a-z]"]);
        assert!(g.match_("a").unwrap().is_some());
        assert!(g.match_("abc").unwrap().is_none());
    }

    #[test]
    fn test_invalid_pattern_reports_bad_patterns() {
        let g = Globster::new(["RE:[", "/home/foo", "RE:*.cpp"]);
        let err = g.match_("filename").unwrap_err();
        assert_eq!(err.bad_patterns, vec!["RE:[", "RE:*.cpp"]);
    }

    #[test]
    fn test_aggregation_does_not_disable_group() {
        // A bucket containing [:ascii:] must still match its other patterns.
        let g = Globster::new(["*~", "[[:ascii:]]", "Makefile"]);
        assert_eq!(g.match_("a~").unwrap().as_deref(), Some("*~"));
        assert_eq!(g.match_("Makefile").unwrap().as_deref(), Some("Makefile"));
    }
}
