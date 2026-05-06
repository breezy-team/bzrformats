/// The RIO file format
///
/// Copyright (C) 2023 Jelmer Vernooij <jelmer@jelmer.uk>
///
/// Based on the Python implementation:
/// Copyright (C) 2005 Canonical Ltd.
///
/// \subsection{\emph{rio} - simple text metaformat}
///
/// \emph{r} stands for `restricted', `reproducible', or `rfc822-like'.
///
/// The stored data consists of a series of \emph{stanzas}, each of which contains
/// \emph{fields} identified by an ascii name, with Unicode or string contents.
/// The field tag is constrained to alphanumeric characters.
/// There may be more than one field in a stanza with the same name.
///
/// The format itself does not deal with character encoding issues, though
/// the result will normally be written in Unicode.
///
/// The format is intended to be simple enough that there is exactly one character
/// stream representation of an object and vice versa, and that this relation
/// will continue to hold for future versions of bzr.
use regex::Regex;
use std::collections::HashMap;
use std::io::{BufRead, Write};
use std::iter::Iterator;
use std::result::Result;
use std::str;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    InvalidTag(String),
    ContinuationLineWithoutTag,
    TagValueSeparatorNotFound(Vec<u8>),
    Other(String),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

/// Verify whether a tag is validly formatted
pub fn valid_tag(tag: &str) -> bool {
    lazy_static::lazy_static! {
        static ref RE: Regex = Regex::new(r"^[-a-zA-Z0-9_]+$").unwrap();
    }
    RE.is_match(tag)
}

pub struct RioWriter<W: Write> {
    soft_nl: bool,
    to_file: W,
}

impl<W: Write> RioWriter<W> {
    pub fn new(to_file: W) -> Self {
        RioWriter {
            soft_nl: false,
            to_file,
        }
    }

    pub fn write_stanza(&mut self, stanza: &Stanza) -> Result<(), std::io::Error> {
        if self.soft_nl {
            self.to_file.write_all(b"\n")?;
        }
        stanza.write(&mut self.to_file)?;
        self.soft_nl = true;
        Ok(())
    }
}

pub struct RioReader<R: BufRead> {
    from_file: R,
}

impl<R: BufRead> RioReader<R> {
    pub fn new(from_file: R) -> Self {
        RioReader { from_file }
    }

    fn read_stanza(&mut self) -> Result<Option<Stanza>, Error> {
        read_stanza_file(&mut self.from_file)
    }

    pub fn iter(&mut self) -> RioReaderIter<'_, R> {
        RioReaderIter { reader: self }
    }
}

pub struct RioReaderIter<'a, R: BufRead> {
    reader: &'a mut RioReader<R>,
}

impl<R: BufRead> Iterator for RioReaderIter<'_, R> {
    type Item = Result<Option<Stanza>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.reader.read_stanza() {
            Ok(stanza) => stanza.map(|s| Ok(Some(s))),
            Err(e) => Some(Err(e)),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Stanza {
    items: Vec<(String, StanzaValue)>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum StanzaValue {
    String(String),
    Stanza(Box<Stanza>),
}

impl PartialEq for Stanza {
    fn eq(&self, other: &Self) -> bool {
        if self.len() != other.len() {
            return false;
        }
        for (self_item, other_item) in self.items.iter().zip(other.items.iter()) {
            let (self_tag, self_value) = self_item;
            let (other_tag, other_value) = other_item;
            if self_tag != other_tag {
                return false;
            }
            if self_value != other_value {
                return false;
            }
        }
        true
    }
}

impl Stanza {
    pub fn new() -> Stanza {
        Stanza { items: vec![] }
    }

    pub fn from_pairs(pairs: Vec<(String, StanzaValue)>) -> Stanza {
        Stanza { items: pairs }
    }

    pub fn add(&mut self, tag: String, value: StanzaValue) -> Result<(), Error> {
        if !valid_tag(&tag) {
            return Err(Error::InvalidTag(tag));
        }
        self.items.push((tag, value));
        Ok(())
    }

    pub fn contains(&self, find_tag: &str) -> bool {
        for (tag, _) in &self.items {
            if tag == find_tag {
                return true;
            }
        }
        false
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn iter_pairs(&self) -> impl Iterator<Item = (&str, &StanzaValue)> {
        self.items.iter().map(|(tag, value)| (tag.as_str(), value))
    }

    pub fn to_bytes_lines(&self) -> Vec<Vec<u8>> {
        self.to_lines()
            .iter()
            .map(|s| s.as_bytes().to_vec())
            .collect()
    }

    pub fn to_lines(&self) -> Vec<String> {
        let mut result = Vec::new();
        for (text_tag, text_value) in &self.items {
            let tag = text_tag.as_bytes();
            let value = match text_value {
                StanzaValue::String(val) => val.to_string(),
                StanzaValue::Stanza(val) => val.to_string(),
            };
            if value.is_empty() {
                result.push(format!("{}: \n", String::from_utf8_lossy(tag)));
            } else if value.contains('\n') {
                let mut val_lines = value.split('\n');
                if let Some(first_line) = val_lines.next() {
                    result.push(format!(
                        "{}: {}\n",
                        String::from_utf8_lossy(tag),
                        first_line
                    ));
                }
                for line in val_lines {
                    result.push(format!("\t{}\n", line));
                }
            } else {
                result.push(format!("{}: {}\n", String::from_utf8_lossy(tag), value));
            }
        }
        result
    }

    pub fn to_string(&self) -> String {
        self.to_lines().join("")
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        self.to_string().into_bytes()
    }

    pub fn write<T: Write>(&self, to_file: &mut T) -> std::io::Result<()> {
        for line in self.to_lines() {
            to_file.write_all(line.as_bytes())?;
        }
        Ok(())
    }

    pub fn get(&self, tag: &str) -> Option<&StanzaValue> {
        for (t, v) in &self.items {
            if t == tag {
                return Some(v);
            }
        }

        None
    }

    pub fn get_all(&self, tag: &str) -> Vec<&StanzaValue> {
        self.items
            .iter()
            .filter(|(t, _)| t == tag)
            .map(|(_, v)| v)
            .collect()
    }

    pub fn as_dict(&self) -> HashMap<String, StanzaValue> {
        let mut d = HashMap::new();
        for (tag, value) in &self.items {
            d.insert(tag.clone(), value.clone());
        }
        d
    }
}

impl std::default::Default for Stanza {
    fn default() -> Self {
        Stanza::new()
    }
}

pub fn read_stanza_file(line_iter: &mut dyn BufRead) -> Result<Option<Stanza>, Error> {
    read_stanza(line_iter.split(b'\n').map(|l| {
        let mut vec: Vec<u8> = l?;
        vec.push(b'\n');
        Ok(vec)
    }))
}

fn trim_newline(vec: &mut Vec<u8>) {
    if let Some(last_non_newline) = vec.iter().rposition(|&b| b != b'\n' && b != b'\r') {
        vec.truncate(last_non_newline + 1);
    } else {
        vec.clear();
    }
}

pub fn read_stanza<I>(lines: I) -> Result<Option<Stanza>, Error>
where
    I: Iterator<Item = Result<Vec<u8>, Error>>,
{
    let mut stanza = Stanza::new();
    let mut tag: Option<String> = None;
    let mut accum_value: Option<Vec<String>> = None;

    for bline in lines {
        let mut line = bline?;
        trim_newline(&mut line);
        if line.is_empty() {
            break; // end of stanza
        } else if line.starts_with(b"\t") {
            // continues previous value
            if tag.is_none() {
                return Err(Error::ContinuationLineWithoutTag);
            }
            if let Some(accum_value) = accum_value.as_mut() {
                let extra = String::from_utf8(line[1..line.len()].to_owned()).unwrap();
                accum_value.push("\n".to_string() + &extra);
            }
        } else {
            // new tag:value line
            if let Some(tag) = tag.take() {
                let value = accum_value.take().map_or_else(String::new, |v| v.join(""));
                stanza.add(tag, StanzaValue::String(value))?;
            }
            let colon_index = match line.windows(2).position(|window| window.eq(b": ")) {
                Some(index) => index,
                None => return Err(Error::TagValueSeparatorNotFound(line)),
            };
            let tagname = String::from_utf8(line[0..colon_index].to_owned()).unwrap();
            if !valid_tag(&tagname) {
                return Err(Error::InvalidTag(tagname));
            }
            tag = Some(tagname);
            let value = String::from_utf8(line[colon_index + 2..line.len()].to_owned()).unwrap();
            accum_value = Some(vec![value]);
        }
    }
    if let Some(tag) = tag {
        let value = accum_value.take().map_or_else(String::new, |v| v.join(""));
        stanza.add(tag, StanzaValue::String(value))?;
        Ok(Some(stanza))
    } else {
        // didn't see any content
        Ok(None)
    }
}

pub fn read_stanzas(line_iter: &mut dyn BufRead) -> Result<Vec<Stanza>, Error> {
    let mut stanzas = vec![];
    while let Some(s) = read_stanza_file(line_iter)? {
        stanzas.push(s);
    }
    Ok(stanzas)
}

pub fn rio_iter(
    stanzas: impl IntoIterator<Item = Stanza>,
    header: Option<Vec<u8>>,
) -> impl Iterator<Item = Vec<u8>> {
    let mut lines = Vec::new();
    if let Some(header) = header {
        let mut header = header;
        header.push(b'\n');
        lines.push(header);
    }
    let mut first_stanza = true;
    for stanza in stanzas {
        if !first_stanza {
            lines.push(b"\n".to_vec());
        }
        lines.push(stanza.to_bytes());
        first_stanza = false;
    }
    lines.into_iter()
}

/// Convert a stanza into RIO-Patch format lines.
///
/// RIO-Patch is a RIO variant designed to be e-mailed as part of a patch.
/// It resists common forms of damage such as newline conversion or the
/// removal of trailing whitespace, yet is also reasonably easy to read.
pub fn to_patch_lines(stanza: &Stanza, max_width: usize) -> Result<Vec<Vec<u8>>, Error> {
    if max_width <= 6 {
        return Err(Error::Other(format!("max_width too small: {}", max_width)));
    }
    let max_rio_width = max_width - 4;
    let mut lines: Vec<Vec<u8>> = Vec::new();
    for pline in stanza.to_lines() {
        let pbytes = pline.into_bytes();
        // Equivalent of pline.split(b"\n")[:-1]: split on \n and drop the
        // trailing empty segment that follows the final newline. If pbytes
        // does not end with \n we still drop the last segment, matching
        // Python's behaviour.
        let mut segments: Vec<&[u8]> = pbytes.split(|&b| b == b'\n').collect();
        segments.pop();
        for segment in segments {
            // Escape backslashes.
            let mut line: Vec<u8> = Vec::with_capacity(segment.len());
            for &b in segment {
                if b == b'\\' {
                    line.extend_from_slice(b"\\\\");
                } else {
                    line.push(b);
                }
            }
            while !line.is_empty() {
                let split_at = std::cmp::min(max_rio_width, line.len());
                let mut partline = line[..split_at].to_vec();
                let mut rest = line[split_at..].to_vec();
                // The Python implementation has `if len(line) > 0 and
                // line[:1] != [b" "]` which is always true (comparing bytes
                // to a list never matches), so the break-search runs
                // whenever there is a remainder.
                if !rest.is_empty() {
                    let start = partline.len().saturating_sub(20);
                    let mut break_index: i64 = -1;
                    if let Some(pos) = partline[start..].iter().rposition(|&b| b == b' ') {
                        break_index = (start + pos) as i64;
                    }
                    if break_index < 3 {
                        if let Some(pos) = partline[start..].iter().rposition(|&b| b == b'-') {
                            break_index = (start + pos) as i64 + 1;
                        }
                    }
                    if break_index < 3 {
                        if let Some(pos) = partline[start..].iter().rposition(|&b| b == b'/') {
                            break_index = (start + pos) as i64;
                        }
                    }
                    if break_index >= 3 {
                        let bi = break_index as usize;
                        let mut new_rest = partline[bi..].to_vec();
                        new_rest.extend_from_slice(&rest);
                        rest = new_rest;
                        partline.truncate(bi);
                    }
                }
                if !rest.is_empty() {
                    // Indent continuation lines by two spaces.
                    let mut indented = b"  ".to_vec();
                    indented.append(&mut rest);
                    rest = indented;
                }
                // Escape carriage returns.
                let mut escaped: Vec<u8> = Vec::with_capacity(partline.len());
                for &b in &partline {
                    if b == b'\r' {
                        escaped.extend_from_slice(b"\\r");
                    } else {
                        escaped.push(b);
                    }
                }
                partline = escaped;
                let mut blank_line = false;
                if !rest.is_empty() {
                    partline.push(b'\\');
                } else if partline.last() == Some(&b' ') {
                    partline.push(b'\\');
                    blank_line = true;
                }
                let mut out = b"# ".to_vec();
                out.append(&mut partline);
                out.push(b'\n');
                lines.push(out);
                if blank_line {
                    lines.push(b"#   \n".to_vec());
                }
                line = rest;
            }
        }
    }
    Ok(lines)
}

/// Decode the RIO-Patch line wrapping into raw RIO lines suitable for
/// `read_stanza`.
fn patch_stanza_iter<I>(line_iter: I) -> Result<Vec<Vec<u8>>, Error>
where
    I: IntoIterator<Item = Vec<u8>>,
{
    let mut out = Vec::new();
    let mut last_line: Option<Vec<u8>> = None;
    let mut first_chunk = true;
    for line in line_iter {
        let mut line: Vec<u8> = if line.starts_with(b"# ") {
            line[2..].to_vec()
        } else if line.starts_with(b"#") {
            line[1..].to_vec()
        } else {
            return Err(Error::Other(format!("bad line {:?}", line)));
        };
        if !first_chunk && line.len() > 2 {
            line = line[2..].to_vec();
        }
        // Strip carriage returns.
        line.retain(|&b| b != b'\r');
        // Apply the backslash decoding: \\ -> \, \r -> \r, \\n -> "" (line continuation).
        let decoded = decode_patch_escapes(&line);
        let combined = match last_line.take() {
            None => decoded,
            Some(mut prev) => {
                prev.extend_from_slice(&decoded);
                prev
            }
        };
        if combined.last() == Some(&b'\n') {
            out.push(combined);
            last_line = None;
            first_chunk = true;
        } else {
            last_line = Some(combined);
            first_chunk = false;
        }
    }
    if let Some(rem) = last_line {
        out.push(rem);
    }
    Ok(out)
}

fn decode_patch_escapes(input: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(input.len());
    let mut i = 0;
    while i < input.len() {
        if input[i] == b'\\' && i + 1 < input.len() {
            match input[i + 1] {
                b'\\' => {
                    out.push(b'\\');
                    i += 2;
                }
                b'r' => {
                    out.push(b'\r');
                    i += 2;
                }
                b'\n' => {
                    // Soft-wrap continuation: drop both bytes.
                    i += 2;
                }
                other => {
                    // Unknown escape: leave the backslash and consume the
                    // following character verbatim, mirroring Python's
                    // KeyError-on-mapget behaviour would actually raise; but
                    // since the encoder only produces the three escapes
                    // above, in practice this branch is unreachable.
                    out.push(b'\\');
                    out.push(other);
                    i += 2;
                }
            }
        } else {
            out.push(input[i]);
            i += 1;
        }
    }
    out
}

/// Convert an iterable of RIO-Patch lines into a Stanza.
pub fn read_patch_stanza<I>(line_iter: I) -> Result<Option<Stanza>, Error>
where
    I: IntoIterator<Item = Vec<u8>>,
{
    let lines = patch_stanza_iter(line_iter)?;
    read_stanza(lines.into_iter().map(Ok))
}

#[cfg(test)]
mod tests {
    use super::valid_tag;
    use super::{read_stanza, Stanza, StanzaValue};

    #[test]
    fn test_valid_tag() {
        assert!(valid_tag("name"));
        assert!(!valid_tag("!name"));
    }

    #[test]
    fn test_stanza() {
        let mut s = Stanza::new();
        s.add("number".to_string(), StanzaValue::String("42".to_string()))
            .unwrap();
        s.add("name".to_string(), StanzaValue::String("fred".to_string()))
            .unwrap();

        assert!(s.contains("number"));
        assert!(!s.contains("color"));
        assert!(!s.contains("42"));

        // Verify that the s.get() function works
        assert_eq!(
            s.get("number"),
            Some(&StanzaValue::String("42".to_string()))
        );
        assert_eq!(
            s.get("name"),
            Some(&StanzaValue::String("fred".to_string()))
        );
        assert_eq!(s.get("color"), None);

        // Verify that iter_pairs() works
        assert_eq!(s.iter_pairs().count(), 2);
    }

    #[test]
    fn test_eq() {
        let mut s = Stanza::new();
        s.add("number".to_string(), StanzaValue::String("42".to_string()))
            .unwrap();
        s.add("name".to_string(), StanzaValue::String("fred".to_string()))
            .unwrap();

        let mut t = Stanza::new();
        t.add("number".to_string(), StanzaValue::String("42".to_string()))
            .unwrap();
        t.add("name".to_string(), StanzaValue::String("fred".to_string()))
            .unwrap();

        assert_eq!(s, s);
        assert_eq!(s, t);
        t.add("color".to_string(), StanzaValue::String("red".to_string()))
            .unwrap();

        assert_ne!(s, t);
    }

    #[test]
    fn test_empty_value() {
        let s = Stanza::from_pairs(vec![(
            "empty".to_string(),
            StanzaValue::String("".to_string()),
        )]);
        assert_eq!(s.to_string(), "empty: \n");
    }

    #[test]
    fn test_to_lines() {
        let s = Stanza::from_pairs(vec![
            ("number".to_string(), StanzaValue::String("42".to_string())),
            ("name".to_string(), StanzaValue::String("fred".to_string())),
            (
                "field-with-newlines".to_string(),
                StanzaValue::String("foo\nbar\nblah".to_string()),
            ),
            (
                "special-characters".to_string(),
                StanzaValue::String(" \t\r\\\n ".to_string()),
            ),
        ]);
        assert_eq!(
            s.to_lines(),
            vec![
                "number: 42\n".to_string(),
                "name: fred\n".to_string(),
                "field-with-newlines: foo\n".to_string(),
                "\tbar\n".to_string(),
                "\tblah\n".to_string(),
                "special-characters:  \t\r\\\n".to_string(),
                "\t \n".to_string()
            ],
        );
    }

    fn s(tag: &str, value: &str) -> (String, StanzaValue) {
        (tag.to_string(), StanzaValue::String(value.to_string()))
    }

    #[test]
    fn test_valid_tag_extra_cases() {
        assert!(valid_tag("foo"));
        assert!(!valid_tag("foo bla"));
        assert!(valid_tag("3foo423"));
        assert!(!valid_tag("foo:bla"));
        assert!(!valid_tag(""));
        assert!(!valid_tag("\u{b5}"));
    }

    #[test]
    fn test_as_dict() {
        let stanza = Stanza::from_pairs(vec![s("number", "42"), s("name", "fred")]);
        let dict = stanza.as_dict();
        assert_eq!(
            dict.get("number"),
            Some(&StanzaValue::String("42".to_string()))
        );
        assert_eq!(
            dict.get("name"),
            Some(&StanzaValue::String("fred".to_string()))
        );
        assert_eq!(dict.len(), 2);
    }

    #[test]
    fn test_to_file() {
        let stanza = Stanza::from_pairs(vec![
            s("a_thing", "something with \"quotes like \\\"this\\\"\""),
            s("name", "fred"),
            s("number", "42"),
        ]);
        let mut buf = Vec::new();
        stanza.write(&mut buf).unwrap();
        assert_eq!(
            buf,
            b"a_thing: something with \"quotes like \\\"this\\\"\"\nname: fred\nnumber: 42\n",
        );
    }

    #[test]
    fn test_multiline_string_round_trip() {
        let stanza = Stanza::from_pairs(vec![s(
            "motto",
            "war is peace\nfreedom is slavery\nignorance is strength",
        )]);
        let mut buf = Vec::new();
        stanza.write(&mut buf).unwrap();
        assert_eq!(
            buf,
            b"motto: war is peace\n\tfreedom is slavery\n\tignorance is strength\n",
        );
        let lines = buf
            .split_inclusive(|b| *b == b'\n')
            .map(|l| l.to_vec())
            .collect::<Vec<_>>();
        let reread = read_stanza(lines.into_iter().map(Ok)).unwrap().unwrap();
        assert_eq!(reread, stanza);
    }

    #[test]
    fn test_repeated_field_round_trip() {
        let mut stanza = Stanza::new();
        for (k, v) in [
            ("a", "10"),
            ("b", "20"),
            ("a", "100"),
            ("b", "200"),
            ("a", "1000"),
            ("b", "2000"),
        ] {
            stanza
                .add(k.to_string(), StanzaValue::String(v.to_string()))
                .unwrap();
        }
        let lines: Vec<Vec<u8>> = stanza
            .to_lines()
            .into_iter()
            .map(|l| l.into_bytes())
            .collect();
        let reread = read_stanza(lines.into_iter().map(Ok)).unwrap().unwrap();
        assert_eq!(reread, stanza);
        let all_a: Vec<&StanzaValue> = stanza.get_all("a");
        assert_eq!(
            all_a,
            vec![
                &StanzaValue::String("10".to_string()),
                &StanzaValue::String("100".to_string()),
                &StanzaValue::String("1000".to_string()),
            ]
        );
    }

    #[test]
    fn test_backslash_round_trip() {
        let stanza = Stanza::from_pairs(vec![s("q", "\\")]);
        assert_eq!(stanza.to_string(), "q: \\\n");
        let lines: Vec<Vec<u8>> = stanza
            .to_lines()
            .into_iter()
            .map(|l| l.into_bytes())
            .collect();
        let reread = read_stanza(lines.into_iter().map(Ok)).unwrap().unwrap();
        assert_eq!(reread, stanza);
    }

    #[test]
    fn test_blank_line_round_trip() {
        let stanza = Stanza::from_pairs(vec![s("none", ""), s("one", "\n"), s("two", "\n\n")]);
        assert_eq!(stanza.to_string(), "none: \none: \n\t\ntwo: \n\t\n\t\n",);
        let lines: Vec<Vec<u8>> = stanza
            .to_lines()
            .into_iter()
            .map(|l| l.into_bytes())
            .collect();
        let reread = read_stanza(lines.into_iter().map(Ok)).unwrap().unwrap();
        assert_eq!(reread, stanza);
    }

    #[test]
    fn test_whitespace_value_round_trip() {
        let stanza = Stanza::from_pairs(vec![
            s("space", " "),
            s("tabs", "\t\t\t"),
            s("combo", "\n\t\t\n"),
        ]);
        let lines: Vec<Vec<u8>> = stanza
            .to_lines()
            .into_iter()
            .map(|l| l.into_bytes())
            .collect();
        let reread = read_stanza(lines.into_iter().map(Ok)).unwrap().unwrap();
        assert_eq!(reread, stanza);
    }

    #[test]
    fn test_read_empty_iter_returns_none() {
        let empty: Vec<Vec<u8>> = vec![];
        let result = read_stanza(empty.into_iter().map(Ok)).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_read_single_blank_line_returns_none() {
        let lines: Vec<Vec<u8>> = vec![b"".to_vec()];
        let result = read_stanza(lines.into_iter().map(Ok)).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_read_nul_byte_raises() {
        let lines: Vec<Vec<u8>> = vec![b"\0".to_vec()];
        let result = read_stanza(lines.into_iter().map(Ok));
        assert!(result.is_err());
    }

    #[test]
    fn test_read_nul_bytes_raises() {
        let lines: Vec<Vec<u8>> = vec![vec![0u8; 100]];
        let result = read_stanza(lines.into_iter().map(Ok));
        assert!(result.is_err());
    }

    #[test]
    fn test_write_empty_stanza_yields_no_lines() {
        let stanza = Stanza::new();
        assert!(stanza.to_lines().is_empty());
    }

    #[test]
    fn test_rio_unicode_value_round_trip() {
        // \u{30aa} = KATAKANA LETTER O
        let stanza = Stanza::from_pairs(vec![s("foo", "\u{30aa}")]);
        assert_eq!(
            stanza.get("foo"),
            Some(&StanzaValue::String("\u{30aa}".to_string()))
        );
        let lines: Vec<Vec<u8>> = stanza
            .to_lines()
            .into_iter()
            .map(|l| l.into_bytes())
            .collect();
        assert_eq!(lines, vec![format!("foo: \u{30aa}\n").into_bytes()]);
        let reread = read_stanza(lines.into_iter().map(Ok)).unwrap().unwrap();
        assert_eq!(
            reread.get("foo"),
            Some(&StanzaValue::String("\u{30aa}".to_string()))
        );
    }

    #[test]
    fn test_read_simple_key_value() {
        let lines: Vec<Vec<u8>> = vec![b"foo: bar\n".to_vec(), b"".to_vec()];
        let stanza = read_stanza(lines.into_iter().map(Ok)).unwrap().unwrap();
        assert_eq!(stanza, Stanza::from_pairs(vec![s("foo", "bar")]));
    }

    #[test]
    fn test_read_multi_line_continuation() {
        let lines: Vec<Vec<u8>> = vec![b"foo: bar\n".to_vec(), b"\tbla\n".to_vec()];
        let stanza = read_stanza(lines.into_iter().map(Ok)).unwrap().unwrap();
        assert_eq!(stanza, Stanza::from_pairs(vec![s("foo", "bar\nbla")]));
    }

    #[test]
    fn test_read_repeated_tag() {
        let lines: Vec<Vec<u8>> = vec![b"foo: bar\n".to_vec(), b"foo: foo\n".to_vec()];
        let stanza = read_stanza(lines.into_iter().map(Ok)).unwrap().unwrap();
        let mut expected = Stanza::new();
        expected
            .add("foo".to_string(), StanzaValue::String("bar".to_string()))
            .unwrap();
        expected
            .add("foo".to_string(), StanzaValue::String("foo".to_string()))
            .unwrap();
        assert_eq!(stanza, expected);
    }

    #[test]
    fn test_read_invalid_early_colon_raises() {
        let lines: Vec<Vec<u8>> = vec![b"f:oo: bar\n".to_vec()];
        assert!(read_stanza(lines.into_iter().map(Ok)).is_err());
    }

    #[test]
    fn test_read_invalid_tag_raises() {
        let lines: Vec<Vec<u8>> = vec![b"f%oo: bar\n".to_vec()];
        assert!(read_stanza(lines.into_iter().map(Ok)).is_err());
    }

    #[test]
    fn test_read_continuation_without_key_raises() {
        let lines: Vec<Vec<u8>> = vec![b"\tbar\n".to_vec()];
        assert!(read_stanza(lines.into_iter().map(Ok)).is_err());
    }

    #[test]
    fn test_read_large_value() {
        let value: String = "bla".repeat(9000);
        let line = format!("foo: {}\n", value).into_bytes();
        let lines: Vec<Vec<u8>> = vec![line];
        let stanza = read_stanza(lines.into_iter().map(Ok)).unwrap().unwrap();
        assert_eq!(stanza, Stanza::from_pairs(vec![s("foo", value.as_str())]));
    }

    #[test]
    fn test_read_non_ascii_char() {
        let line = "foo: n\u{e5}me\n".as_bytes().to_vec();
        let lines: Vec<Vec<u8>> = vec![line];
        let stanza = read_stanza(lines.into_iter().map(Ok)).unwrap().unwrap();
        assert_eq!(stanza, Stanza::from_pairs(vec![s("foo", "n\u{e5}me")]));
    }

    #[test]
    fn test_read_stanza() {
        let lines = b"number: 42
name: fred
field-with-newlines: foo
\tbar
\tblah

"
        .split(|c| *c == b'\n')
        .map(|s| s.to_vec());
        let s = read_stanza(lines.map(Ok)).unwrap().unwrap();
        let expected = Stanza::from_pairs(vec![
            ("number".to_string(), StanzaValue::String("42".to_string())),
            ("name".to_string(), StanzaValue::String("fred".to_string())),
            (
                "field-with-newlines".to_string(),
                StanzaValue::String("foo\nbar\nblah".to_string()),
            ),
        ]);
        assert_eq!(s, expected);
    }

    use super::{read_patch_stanza, to_patch_lines};

    fn mail_munge(lines: &[Vec<u8>], dos_nl: bool) -> Vec<Vec<u8>> {
        lines
            .iter()
            .map(|line| {
                let mut out = Vec::with_capacity(line.len());
                let mut buf: Vec<u8> = Vec::new();
                for &b in line {
                    if b == b'\n' {
                        while buf.last() == Some(&b' ') {
                            buf.pop();
                        }
                        out.append(&mut buf);
                        if dos_nl && out.last() != Some(&b'\r') {
                            out.push(b'\r');
                        }
                        out.push(b'\n');
                    } else {
                        buf.push(b);
                    }
                }
                out.append(&mut buf);
                out
            })
            .collect()
    }

    fn b(s: &[u8]) -> Vec<u8> {
        s.to_vec()
    }

    #[test]
    fn test_to_patch_lines_basic_max_72() {
        let mut s = Stanza::new();
        s.add(
            "data".to_string(),
            StanzaValue::String("#\n\r\\r ".to_string()),
        )
        .unwrap();
        s.add("space".to_string(), StanzaValue::String(" ".repeat(255)))
            .unwrap();
        s.add("hash".to_string(), StanzaValue::String("#".repeat(255)))
            .unwrap();
        let lines = to_patch_lines(&s, 72).unwrap();
        let expected: Vec<Vec<u8>> = vec![
            b(b"# data: #\n"),
            b(b"# \t\\r\\\\r \\\n"),
            b(b"#   \n"),
            b(b"# space:                                                             \\\n"),
            b(b"#                                                                    \\\n"),
            b(b"#                                                                    \\\n"),
            b(b"#                                                                    \\\n"),
            b(b"#   \n"),
            b(b"# hash: ##############################################################\\\n"),
            b(b"#   ##################################################################\\\n"),
            b(b"#   ##################################################################\\\n"),
            b(b"#   #############################################################\n"),
        ];
        assert_eq!(lines, expected);
    }

    #[test]
    fn test_to_patch_lines_roundtrip_through_mail_munge() {
        let mut s = Stanza::new();
        s.add(
            "data".to_string(),
            StanzaValue::String("#\n\r\\r ".to_string()),
        )
        .unwrap();
        s.add("space".to_string(), StanzaValue::String(" ".repeat(255)))
            .unwrap();
        s.add("hash".to_string(), StanzaValue::String("#".repeat(255)))
            .unwrap();
        let lines = to_patch_lines(&s, 72).unwrap();

        let munged_no_dos = mail_munge(&lines, false);
        let parsed = read_patch_stanza(munged_no_dos).unwrap().unwrap();
        assert_eq!(
            parsed.get("data"),
            Some(&StanzaValue::String("#\n\r\\r ".to_string()))
        );
        assert_eq!(
            parsed.get("space"),
            Some(&StanzaValue::String(" ".repeat(255)))
        );
        assert_eq!(
            parsed.get("hash"),
            Some(&StanzaValue::String("#".repeat(255)))
        );

        let munged_dos = mail_munge(&lines, true);
        let parsed = read_patch_stanza(munged_dos).unwrap().unwrap();
        assert_eq!(
            parsed.get("data"),
            Some(&StanzaValue::String("#\n\r\\r ".to_string()))
        );
        assert_eq!(
            parsed.get("space"),
            Some(&StanzaValue::String(" ".repeat(255)))
        );
        assert_eq!(
            parsed.get("hash"),
            Some(&StanzaValue::String("#".repeat(255)))
        );
    }

    #[test]
    fn test_to_patch_lines_too_small_width() {
        let mut s = Stanza::new();
        s.add("foo".to_string(), StanzaValue::String("bar".to_string()))
            .unwrap();
        assert!(to_patch_lines(&s, 6).is_err());
        assert!(to_patch_lines(&s, 7).is_ok());
    }

    #[test]
    fn test_to_patch_lines_break_on_space() {
        let mut s = Stanza::new();
        s.add(
            "breaktest".to_string(),
            StanzaValue::String("linebreak -/".repeat(30)),
        )
        .unwrap();
        let lines = to_patch_lines(&s, 71).unwrap();
        let expected: Vec<Vec<u8>> = vec![
            b(b"# breaktest: linebreak -/linebreak -/linebreak -/linebreak\\\n"),
            b(b"#    -/linebreak -/linebreak -/linebreak -/linebreak -/linebreak\\\n"),
            b(b"#    -/linebreak -/linebreak -/linebreak -/linebreak -/linebreak\\\n"),
            b(b"#    -/linebreak -/linebreak -/linebreak -/linebreak -/linebreak\\\n"),
            b(b"#    -/linebreak -/linebreak -/linebreak -/linebreak -/linebreak\\\n"),
            b(b"#    -/linebreak -/linebreak -/linebreak -/linebreak -/linebreak\\\n"),
            b(b"#    -/linebreak -/\n"),
        ];
        assert_eq!(lines, expected);
    }

    #[test]
    fn test_to_patch_lines_break_on_dash() {
        let mut s = Stanza::new();
        s.add(
            "breaktest".to_string(),
            StanzaValue::String("linebreak-/".repeat(30)),
        )
        .unwrap();
        let lines = to_patch_lines(&s, 70).unwrap();
        let expected: Vec<Vec<u8>> = vec![
            b(b"# breaktest: linebreak-/linebreak-/linebreak-/linebreak-/linebreak-\\\n"),
            b(b"#   /linebreak-/linebreak-/linebreak-/linebreak-/linebreak-\\\n"),
            b(b"#   /linebreak-/linebreak-/linebreak-/linebreak-/linebreak-\\\n"),
            b(b"#   /linebreak-/linebreak-/linebreak-/linebreak-/linebreak-\\\n"),
            b(b"#   /linebreak-/linebreak-/linebreak-/linebreak-/linebreak-\\\n"),
            b(b"#   /linebreak-/linebreak-/linebreak-/linebreak-/linebreak-/\n"),
        ];
        assert_eq!(lines, expected);
    }

    #[test]
    fn test_to_patch_lines_break_on_slash() {
        let mut s = Stanza::new();
        s.add(
            "breaktest".to_string(),
            StanzaValue::String("linebreak/".repeat(30)),
        )
        .unwrap();
        let lines = to_patch_lines(&s, 70).unwrap();
        let expected: Vec<Vec<u8>> = vec![
            b(b"# breaktest: linebreak/linebreak/linebreak/linebreak/linebreak\\\n"),
            b(b"#   /linebreak/linebreak/linebreak/linebreak/linebreak/linebreak\\\n"),
            b(b"#   /linebreak/linebreak/linebreak/linebreak/linebreak/linebreak\\\n"),
            b(b"#   /linebreak/linebreak/linebreak/linebreak/linebreak/linebreak\\\n"),
            b(b"#   /linebreak/linebreak/linebreak/linebreak/linebreak/linebreak\\\n"),
            b(b"#   /linebreak/\n"),
        ];
        assert_eq!(lines, expected);
    }
}
