#![allow(dead_code)]
use crate::inventory::{Entry, MutableInventory};
use crate::revision::Revision;
use crate::serializer::{Error, InventorySerializer, RevisionSerializer};
use crate::{FileId, RevisionId};
use lazy_regex::regex_replace_all;
use std::collections::HashMap;
use std::io::{BufRead, Read, Write};
use std::str;
use xmltree::Element;

fn escape_low(c: u8) -> Option<&'static str> {
    match c {
        b'&' => Some("&amp;"),
        b'\'' => Some("&apos;"),
        b'"' => Some("&quot;"),
        b'<' => Some("&lt;"),
        b'>' => Some("&gt;"),
        _ => None,
    }
}

fn unicode_escape_replace(cap: &regex::Captures) -> String {
    let m = cap.get(0).unwrap();
    assert_eq!(m.as_str().chars().count(), 1,);
    let c = m.as_str().chars().next().unwrap();
    if m.as_str().len() == 1 {
        if let Some(ret) = escape_low(m.as_str().as_bytes()[0]) {
            return ret.to_string();
        }
    }
    format!("&#{};", c as u32)
}

fn utf8_escape_replace(cap: &regex::bytes::Captures) -> Vec<u8> {
    let m = cap.get(0).unwrap().as_bytes();
    if m.len() == 1 {
        if let Some(ret) = escape_low(m[0]) {
            return ret.as_bytes().to_vec();
        }
    }
    let utf8 = str::from_utf8(m).unwrap();
    utf8.chars()
        .map(|c| format!("&#{};", c as u64).into_bytes())
        .collect::<Vec<Vec<u8>>>()
        .concat()
}

pub fn encode_and_escape_string(text: &str) -> String {
    regex_replace_all!(r#"[&<>'"\u{007f}-\u{ffff}]"#, text, unicode_escape_replace).into_owned()
}

pub fn encode_and_escape_bytes(data: &[u8]) -> String {
    let bytes =
        regex_replace_all!(r#"(?-u)[&<>'"]|[\x7f-\xff]+"#B, data, utf8_escape_replace).into_owned();
    String::from_utf8_lossy(bytes.as_slice()).to_string()
}

fn escape_invalid_char(c: char) -> String {
    if c == '\t' || c == '\n' || c == '\r' || c == '\x7f' {
        c.to_string()
    } else if c.is_ascii_control()
        || (c as u32) > 0xD7FF && (c as u32) < 0xE000
        || (c as u32) > 0xFFFD && (c as u32) < 0x10000
    {
        format!("\\x{:02x}", c as u32)
    } else {
        c.to_string()
    }
}

pub fn escape_invalid_chars(message: &str) -> String {
    message
        .chars()
        .map(escape_invalid_char)
        .collect::<Vec<String>>()
        .join("")
}

fn unpack_revision_properties(elt: &xmltree::Element) -> Result<HashMap<String, Vec<u8>>, Error> {
    if let Some(props_elt) = elt.get_child("properties") {
        let mut properties = HashMap::new();
        for child in props_elt.children.iter() {
            let child = child.as_element().ok_or_else(|| {
                Error::DecodeError(format!("bad tag under properties list: {:?}", child))
            })?;
            if child.name != "property" {
                return Err(Error::DecodeError(format!(
                    "bad tag under properties list: {:?}",
                    child
                )));
            }
            let name = child.attributes.get("name").ok_or_else(|| {
                Error::DecodeError("property element missing name attribute".to_owned())
            })?;
            let value = child
                .get_text()
                .map_or_else(Vec::new, |s| s.as_bytes().to_vec());
            properties.insert(name.clone(), value);
        }
        Ok(properties)
    } else {
        Ok(HashMap::new())
    }
}

// TODO(jelmer): Move this to somewhere more central?
fn surrogate_escape(b: u8) -> Vec<u8> {
    let hi = 0xDC80 + ((b >> 4) as u32);
    let lo = 0xDC00 + ((b & 0x0F) as u32);
    let mut result = Vec::new();
    result.extend_from_slice(&hi.to_be_bytes());
    result.extend_from_slice(&lo.to_be_bytes());
    result
}

fn utf8_encode_surrogate(codepoint: u32) -> Vec<u8> {
    let mut result = Vec::new();
    if codepoint < 0x80 {
        result.push(codepoint as u8);
    } else if codepoint < 0x800 {
        result.push(((codepoint >> 6) & 0x1F) as u8 | 0xC0);
        result.push((codepoint & 0x3F) as u8 | 0x80);
    } else if codepoint < 0x10000 {
        result.push(((codepoint >> 12) & 0x0F) as u8 | 0xE0);
        result.push(((codepoint >> 6) & 0x3F) as u8 | 0x80);
        result.push((codepoint & 0x3F) as u8 | 0x80);
    } else if codepoint < 0x110000 {
        result.push(((codepoint >> 18) & 0x07) as u8 | 0xF0);
        result.push(((codepoint >> 12) & 0x3F) as u8 | 0x80);
        result.push(((codepoint >> 6) & 0x3F) as u8 | 0x80);
        result.push((codepoint & 0x3F) as u8 | 0x80);
    } else {
        panic!("Invalid codepoint: {}", codepoint);
    }
    result
}

fn decode_pep838<F, G>(bytes: &[u8], surrogate_fn: F, other_fn: G) -> String
where
    F: Fn(u32) -> String,
    G: Fn(char) -> String,
{
    let mut result = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let byte = bytes[i];
        if byte & 0x80 == 0 {
            // single-byte character
            result.push(other_fn(byte as char));
            i += 1;
        } else if byte & 0xE0 == 0xC0 {
            // two-byte character
            if i + 1 < bytes.len() {
                let c = (((byte & 0x1F) as u32) << 6) | ((bytes[i + 1] & 0x3F) as u32);
                result.push(other_fn(char::from_u32(c).unwrap()));
            } else {
                result.push(other_fn('\u{FFFD}'));
            }
            i += 2;
        } else if byte & 0xF0 == 0xE0 {
            // three-byte character
            if i + 2 < bytes.len() {
                let c = (((byte & 0x0F) as u32) << 12)
                    | (((bytes[i + 1] & 0x3F) as u32) << 6)
                    | ((bytes[i + 2] & 0x3F) as u32);
                result.push(other_fn(char::from_u32(c).unwrap()));
            } else {
                result.push(other_fn('\u{FFFD}'));
            }
            i += 3;
        } else if byte & 0xF8 == 0xF0 {
            // four-byte character
            if i + 3 < bytes.len() {
                let high = ((byte & 0x07) as u16) << 2 | ((bytes[i + 1] & 0x30) >> 4) as u16;
                let low = ((bytes[i + 1] & 0x0F) as u16) << 6 | (bytes[i + 2] & 0x3F) as u16;
                result.push(surrogate_fn(((high as u32) << 16) | (low as u32)));
                i += 4;
            } else {
                result.push(other_fn('\u{FFFD}'));
                i += 1;
            }
        } else {
            // invalid character
            result.push(other_fn('\u{FFFD}'));
            i += 1;
        }
    }
    result.concat()
}

impl<T: XMLRevisionSerializer> RevisionSerializer for T {
    fn format_name(&self) -> &'static str {
        self.format_num()
    }

    fn squashes_xml_invalid_characters(&self) -> bool {
        true
    }

    fn read_revision(&self, file: &mut dyn Read) -> Result<Revision, Error> {
        let element = Element::parse(file)
            .map_err(|e| Error::DecodeError(format!("XML parse error: {}", e)))?;
        self.unpack_revision(element)
    }

    fn read_revision_from_string(&self, text: &[u8]) -> Result<Revision, Error> {
        let mut cursor = std::io::Cursor::new(text);
        self.read_revision(&mut cursor)
    }

    fn write_revision_to_lines(
        &self,
        rev: &Revision,
    ) -> Box<dyn Iterator<Item = Result<Vec<u8>, Error>>> {
        let buf = self.write_revision_to_string(rev);

        if let Ok(buf) = buf {
            let cursor = std::io::Cursor::new(buf);
            let mut reader = std::io::BufReader::new(cursor);
            Box::new(std::iter::from_fn(move || {
                let mut line = Vec::new();
                match reader.read_until(b'\n', &mut line) {
                    Ok(0) => None,
                    Ok(_) => Some(Ok(line)),
                    Err(e) => Some(Err(Error::IOError(e))),
                }
            }))
        } else {
            Box::new(std::iter::once(Err(Error::EncodeError(
                "Failed to write revision to string".to_string(),
            ))))
        }
    }

    fn write_revision_to_string(&self, rev: &Revision) -> Result<Vec<u8>, Error> {
        let mut buf = Vec::new();
        buf.write_all(b"<revision ")?;

        if let Some(ref committer) = rev.committer {
            buf.write_all(
                format!(
                    "committer=\"{}\" ",
                    encode_and_escape_string(committer.as_str())
                )
                .as_bytes(),
            )?;
        }

        buf.write_all(format!("format=\"{}\" ", self.format_name()).as_bytes())?;

        if let Some(ref inventory_sha1) = rev.inventory_sha1 {
            buf.write_all(
                format!(
                    "inventory_sha1=\"{}\" ",
                    encode_and_escape_bytes(inventory_sha1.as_slice())
                )
                .as_bytes(),
            )?;
        }

        buf.write_all(
            format!(
                "revision_id=\"{}\" timestamp=\"{:.3}\"",
                encode_and_escape_bytes(rev.revision_id.as_bytes()),
                rev.timestamp,
            )
            .as_bytes(),
        )?;

        if let Some(timezone) = rev.timezone {
            buf.write_all(format!(" timezone=\"{}\"", timezone).as_bytes())?;
        }

        buf.write_all(b">\n")?;

        let message = encode_and_escape_string(escape_invalid_chars(rev.message.as_str()).as_str());
        buf.write_all(format!("<message>{}</message>\n", message).as_bytes())?;

        if !rev.parent_ids.is_empty() {
            buf.write_all(b"<parents>\n")?;
            for parent_id in &rev.parent_ids {
                if parent_id.is_reserved() {
                    panic!("reserved revision id used as parent: {}", parent_id);
                }
                buf.write_all(
                    format!(
                        "<revision_ref revision_id=\"{}\" />\n",
                        encode_and_escape_bytes(parent_id.as_bytes())
                    )
                    .as_bytes(),
                )?;
            }
            buf.write_all(b"</parents>\n")?;
        }

        if !rev.properties.is_empty() {
            buf.write_all(b"<properties>")?;
            let mut sorted_keys: Vec<_> = rev.properties.keys().collect();
            sorted_keys.sort();
            for prop_name in sorted_keys {
                let prop_value = rev.properties.get(prop_name).unwrap();
                if !prop_value.is_empty() {
                    buf.write_all(
                        format!(
                            "<property name=\"{}\">",
                            encode_and_escape_string(prop_name)
                        )
                        .as_bytes(),
                    )?;
                    let prop_value = decode_pep838(
                        prop_value,
                        |c| {
                            utf8_encode_surrogate(c)
                                .iter()
                                .map(|x| format!("\\x{:02x}", *x as u32))
                                .collect()
                        },
                        escape_invalid_char,
                    );
                    buf.write_all(encode_and_escape_string(prop_value.as_str()).as_bytes())?;
                    buf.write_all(b"</property>\n")?;
                } else {
                    buf.write_all(
                        format!(
                            "<property name=\"{}\" />\n",
                            encode_and_escape_string(prop_name)
                        )
                        .as_bytes(),
                    )?;
                }
            }
            buf.write_all(b"</properties>\n")?;
        }

        buf.write_all(b"</revision>\n")?;

        Ok(buf)
    }
}

pub trait XMLRevisionSerializer: RevisionSerializer {
    fn format_num(&self) -> &'static str;

    fn unpack_revision(&self, document: xmltree::Element) -> Result<Revision, Error> {
        if document.name != "revision" {
            return Err(Error::DecodeError(format!(
                "expected revision element, got {}",
                document.name
            )));
        }
        if let Some(format) = document.attributes.get("format") {
            if format != self.format_num() {
                return Err(Error::DecodeError(format!(
                    "invalid format version {} on revision",
                    format
                )));
            }
        }

        let parents_ids = document
            .get_child("parents")
            .map_or_else(std::vec::Vec::new, |e| {
                e.children
                    .iter()
                    .filter_map(|n| n.as_element())
                    .map(|c| RevisionId::from(c.attributes.get("revision_id").unwrap().as_bytes()))
                    .collect()
            });

        let timezone = document
            .attributes
            .get("timezone")
            .map_or_else(|| None, |v| Some(v.parse::<i32>().unwrap()));

        let message = document.get_child("message").map_or_else(
            || "".to_string(),
            |e| {
                e.get_text()
                    .map_or_else(|| "".to_owned(), |t| t.to_string())
            },
        );

        let revision_id = RevisionId::from(
            document
                .attributes
                .get("revision_id")
                .ok_or_else(|| {
                    Error::EncodeError("revision element missing revision_id attribute".to_owned())
                })?
                .as_bytes(),
        );

        let committer = document.attributes.get("committer").map(|s| s.to_owned());

        let properties = unpack_revision_properties(&document)?;

        let inventory_sha1 = document
            .attributes
            .get("inventory_sha1")
            .map(|s| s.as_bytes().to_vec());

        let timestamp = document
            .attributes
            .get("timestamp")
            .ok_or_else(|| {
                Error::EncodeError("revision element missing timestamp attribute".to_owned())
            })?
            .parse::<f64>()
            .unwrap();

        Ok(Revision::new(
            revision_id,
            parents_ids,
            committer,
            message,
            properties,
            inventory_sha1,
            timestamp,
            timezone,
        ))
    }
}

pub struct XMLRevisionSerializer8;

impl XMLRevisionSerializer for XMLRevisionSerializer8 {
    fn format_num(&self) -> &'static str {
        "8"
    }
}

pub struct XMLRevisionSerializer5;

impl XMLRevisionSerializer for XMLRevisionSerializer5 {
    fn format_num(&self) -> &'static str {
        "5"
    }
}

const ROOT_ID_BYTES: &[u8] = b"TREE_ROOT";

fn unescape_xml(data: &[u8]) -> Result<Vec<u8>, Error> {
    // Replicates the behaviour of Python's _unescape_xml in xml8.py:
    // expand &name; entities for the standard XML named refs and numeric
    // character references like &#181; into their UTF-8 byte equivalents.
    let mut out = Vec::with_capacity(data.len());
    let mut i = 0;
    while i < data.len() {
        let b = data[i];
        if b != b'&' {
            out.push(b);
            i += 1;
            continue;
        }
        let end = match data[i + 1..].iter().position(|&c| c == b';') {
            Some(p) => i + 1 + p,
            None => {
                return Err(Error::DecodeError(
                    "unterminated entity reference".to_string(),
                ));
            }
        };
        let code = &data[i + 1..end];
        match code {
            b"apos" => out.push(b'\''),
            b"quot" => out.push(b'"'),
            b"amp" => out.push(b'&'),
            b"lt" => out.push(b'<'),
            b"gt" => out.push(b'>'),
            _ => {
                if let Some(num) = code.strip_prefix(b"#") {
                    let n_str = str::from_utf8(num)
                        .map_err(|e| Error::DecodeError(format!("bad entity: {}", e)))?;
                    let codepoint: u32 = n_str
                        .parse()
                        .map_err(|e| Error::DecodeError(format!("bad entity: {}", e)))?;
                    let c = char::from_u32(codepoint).ok_or_else(|| {
                        Error::DecodeError(format!("invalid codepoint: {}", codepoint))
                    })?;
                    let mut buf = [0u8; 4];
                    out.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
                } else {
                    return Err(Error::DecodeError(format!(
                        "unknown entity: {}",
                        String::from_utf8_lossy(code)
                    )));
                }
            }
        }
        i = end + 1;
    }
    Ok(out)
}

fn unpack_inventory_entry(elt: &Element, root_id: Option<&FileId>) -> Result<Entry, Error> {
    let kind = elt.name.as_str();
    let file_id = elt
        .attributes
        .get("file_id")
        .ok_or_else(|| Error::DecodeError(format!("entry missing file_id: {}", kind)))?;
    let file_id = FileId::from(file_id.as_bytes());
    let revision = elt
        .attributes
        .get("revision")
        .map(|s| RevisionId::from(s.as_bytes()));
    let parent_id = match elt.attributes.get("parent_id") {
        Some(s) => Some(FileId::from(s.as_bytes())),
        None => root_id.cloned(),
    };
    let name = elt.attributes.get("name").cloned().unwrap_or_default();
    match kind {
        "directory" => {
            if let Some(parent_id) = parent_id {
                Ok(Entry::directory(file_id, name, parent_id, revision))
            } else {
                Ok(Entry::root(file_id, revision))
            }
        }
        "file" => {
            let text_sha1 = elt
                .attributes
                .get("text_sha1")
                .map(|s| s.as_bytes().to_vec());
            let executable = elt
                .attributes
                .get("executable")
                .map(|s| s == "yes")
                .unwrap_or(false);
            let text_size = match elt.attributes.get("text_size") {
                Some(s) => Some(
                    s.parse::<u64>()
                        .map_err(|e| Error::DecodeError(format!("bad text_size: {}", e)))?,
                ),
                None => None,
            };
            let text_id = elt.attributes.get("text_id").map(|s| s.as_bytes().to_vec());
            let parent_id = parent_id
                .ok_or_else(|| Error::DecodeError("file without parent_id".to_string()))?;
            Ok(Entry::file(
                file_id,
                name,
                parent_id,
                revision,
                text_sha1,
                text_size,
                Some(executable),
                text_id,
            ))
        }
        "symlink" => {
            let symlink_target = elt.attributes.get("symlink_target").cloned();
            let parent_id = parent_id
                .ok_or_else(|| Error::DecodeError("symlink without parent_id".to_string()))?;
            Ok(Entry::link(
                file_id,
                name,
                parent_id,
                revision,
                symlink_target,
            ))
        }
        "tree-reference" => {
            let parent_id = parent_id.ok_or_else(|| {
                Error::DecodeError("tree-reference without parent_id".to_string())
            })?;
            let reference_revision = elt
                .attributes
                .get("reference_revision")
                .map(|s| RevisionId::from(s.as_bytes()));
            Ok(Entry::tree_reference(
                file_id,
                name,
                parent_id,
                revision,
                reference_revision,
            ))
        }
        other => Err(Error::UnsupportedInventoryKind(other.to_string())),
    }
}

fn parse_inventory_xml_root(data: &[u8]) -> Result<Element, Error> {
    Element::parse(data).map_err(|e| {
        // mimic Python ElementTree's "unclosed token: line 1, column 0"
        // which the test_serialization_error test depends on.
        Error::UnexpectedInventoryFormat(format!("{}", e))
    })
}

fn unpack_inventory_flat_v8(
    elt: &Element,
    expected_format: &[u8],
    revision_id: Option<RevisionId>,
) -> Result<MutableInventory, Error> {
    if elt.name != "inventory" {
        return Err(Error::UnexpectedInventoryFormat(format!(
            "Root tag is {:?}",
            elt.name
        )));
    }
    let format = elt
        .attributes
        .get("format")
        .ok_or_else(|| Error::UnexpectedInventoryFormat("missing format".to_string()))?;
    if format.as_bytes() != expected_format {
        return Err(Error::UnexpectedInventoryFormat(format!(
            "Invalid format version {:?}",
            format
        )));
    }
    let data_revision_id = elt
        .attributes
        .get("revision_id")
        .map(|s| RevisionId::from(s.as_bytes()));
    let revision_id = data_revision_id.or(revision_id);

    let mut inv = MutableInventory::new();
    inv.revision_id = revision_id.clone();

    for child in &elt.children {
        let child = match child.as_element() {
            Some(c) => c,
            None => continue,
        };
        let entry = unpack_inventory_entry(child, None)?;
        inv.add(entry)
            .map_err(|e| Error::DecodeError(format!("error adding entry: {:?}", e)))?;
    }
    Ok(inv)
}

fn unpack_inventory_flat_v5(
    elt: &Element,
    revision_id: Option<RevisionId>,
) -> Result<MutableInventory, Error> {
    if elt.name != "inventory" {
        return Err(Error::UnexpectedInventoryFormat(format!(
            "Root tag is {:?}",
            elt.name
        )));
    }
    if let Some(format) = elt.attributes.get("format") {
        if format != "5" {
            return Err(Error::UnexpectedInventoryFormat(format!(
                "invalid format version {:?} on inventory",
                format
            )));
        }
    }
    let root_id_bytes = elt
        .attributes
        .get("file_id")
        .map(|s| s.as_bytes().to_vec())
        .unwrap_or_else(|| ROOT_ID_BYTES.to_vec());
    let root_id = FileId::from(root_id_bytes);

    let data_revision_id = elt
        .attributes
        .get("revision_id")
        .map(|s| RevisionId::from(s.as_bytes()));
    let effective_revision_id = data_revision_id.or(revision_id);

    let mut inv = MutableInventory::new();
    inv.revision_id = effective_revision_id.clone();
    let root = Entry::root(root_id.clone(), effective_revision_id);
    inv.add(root)
        .map_err(|e| Error::DecodeError(format!("error adding root: {:?}", e)))?;

    for child in &elt.children {
        let child = match child.as_element() {
            Some(c) => c,
            None => continue,
        };
        let entry = unpack_inventory_entry(child, Some(&root_id))?;
        inv.add(entry)
            .map_err(|e| Error::DecodeError(format!("error adding entry: {:?}", e)))?;
    }
    Ok(inv)
}

fn append_v5_root(out: &mut Vec<u8>, inv: &MutableInventory) -> Result<(), Error> {
    let root = inv
        .root()
        .ok_or_else(|| Error::EncodeError("inventory has no root".to_string()))?;
    out.extend_from_slice(b"<inventory");
    if root.file_id().as_bytes() != ROOT_ID_BYTES {
        out.extend_from_slice(b" file_id=\"");
        out.extend_from_slice(encode_and_escape_bytes(root.file_id().as_bytes()).as_bytes());
        out.push(b'"');
    }
    out.extend_from_slice(b" format=\"5\"");
    if let Some(revision_id) = &inv.revision_id {
        out.extend_from_slice(b" revision_id=\"");
        out.extend_from_slice(encode_and_escape_bytes(revision_id.as_bytes()).as_bytes());
        out.push(b'"');
    }
    out.extend_from_slice(b">\n");
    Ok(())
}

fn append_v8_root(
    out: &mut Vec<u8>,
    format_num: &[u8],
    inv: &MutableInventory,
) -> Result<(), Error> {
    out.extend_from_slice(b"<inventory format=\"");
    out.extend_from_slice(format_num);
    out.push(b'"');
    if let Some(revision_id) = &inv.revision_id {
        out.extend_from_slice(b" revision_id=\"");
        out.extend_from_slice(encode_and_escape_bytes(revision_id.as_bytes()).as_bytes());
        out.push(b'"');
    }
    out.extend_from_slice(b">\n");

    let root = inv
        .root()
        .ok_or_else(|| Error::EncodeError("inventory has no root".to_string()))?;
    let root_revision = root.revision().cloned().or_else(|| inv.revision_id.clone());
    out.extend_from_slice(b"<directory file_id=\"");
    out.extend_from_slice(encode_and_escape_bytes(root.file_id().as_bytes()).as_bytes());
    out.extend_from_slice(b"\" name=\"");
    out.extend_from_slice(encode_and_escape_string(root.name()).as_bytes());
    out.extend_from_slice(b"\" revision=\"");
    if let Some(rev) = root_revision {
        out.extend_from_slice(encode_and_escape_bytes(rev.as_bytes()).as_bytes());
    }
    out.extend_from_slice(b"\" />\n");
    Ok(())
}

fn serialize_inventory_flat(
    inv: &MutableInventory,
    out: &mut Vec<u8>,
    root_id: Option<&[u8]>,
    supported_kinds: &[&str],
    working: bool,
) -> Result<(), Error> {
    // Iterate all entries; skip the root (which is the first entry yielded).
    let mut entries = inv.iter_entries(None);
    if entries.next().is_none() {
        // No root, no body to write
        return Ok(());
    }
    for (_path, ie) in entries {
        let kind = ie.kind();
        let kind_str = osutils::Kind::as_str(&kind);
        if !supported_kinds.contains(&kind_str) {
            return Err(Error::UnsupportedInventoryKind(kind_str.to_string()));
        }
        let parent_str = if ie
            .parent_id()
            .map(|p| Some(p.as_bytes()) != root_id)
            .unwrap_or(false)
        {
            let pid = ie.parent_id().unwrap();
            let mut s = Vec::new();
            s.extend_from_slice(b" parent_id=\"");
            s.extend_from_slice(encode_and_escape_bytes(pid.as_bytes()).as_bytes());
            s.push(b'"');
            s
        } else {
            Vec::new()
        };
        match ie {
            Entry::File {
                file_id,
                name,
                revision,
                text_sha1,
                text_size,
                executable,
                ..
            } => {
                out.extend_from_slice(b"<file");
                if *executable {
                    out.extend_from_slice(b" executable=\"yes\"");
                }
                out.extend_from_slice(b" file_id=\"");
                out.extend_from_slice(encode_and_escape_bytes(file_id.as_bytes()).as_bytes());
                out.extend_from_slice(b"\" name=\"");
                out.extend_from_slice(encode_and_escape_string(name).as_bytes());
                out.push(b'"');
                out.extend_from_slice(&parent_str);
                if !working {
                    out.extend_from_slice(b" revision=\"");
                    if let Some(rev) = revision {
                        out.extend_from_slice(encode_and_escape_bytes(rev.as_bytes()).as_bytes());
                    }
                    out.extend_from_slice(b"\" text_sha1=\"");
                    if let Some(sha) = text_sha1 {
                        out.extend_from_slice(sha.as_slice());
                    }
                    out.extend_from_slice(b"\" text_size=\"");
                    if let Some(size) = text_size {
                        out.extend_from_slice(format!("{}", size).as_bytes());
                    } else {
                        out.extend_from_slice(b"None");
                    }
                    out.push(b'"');
                }
                out.extend_from_slice(b" />\n");
            }
            Entry::Directory {
                file_id,
                name,
                revision,
                ..
            } => {
                out.extend_from_slice(b"<directory file_id=\"");
                out.extend_from_slice(encode_and_escape_bytes(file_id.as_bytes()).as_bytes());
                out.extend_from_slice(b"\" name=\"");
                out.extend_from_slice(encode_and_escape_string(name).as_bytes());
                out.push(b'"');
                out.extend_from_slice(&parent_str);
                if !working {
                    out.extend_from_slice(b" revision=\"");
                    if let Some(rev) = revision {
                        out.extend_from_slice(encode_and_escape_bytes(rev.as_bytes()).as_bytes());
                    }
                    out.push(b'"');
                }
                out.extend_from_slice(b" />\n");
            }
            Entry::Link {
                file_id,
                name,
                revision,
                symlink_target,
                ..
            } => {
                out.extend_from_slice(b"<symlink file_id=\"");
                out.extend_from_slice(encode_and_escape_bytes(file_id.as_bytes()).as_bytes());
                out.extend_from_slice(b"\" name=\"");
                out.extend_from_slice(encode_and_escape_string(name).as_bytes());
                out.push(b'"');
                out.extend_from_slice(&parent_str);
                if !working {
                    out.extend_from_slice(b" revision=\"");
                    if let Some(rev) = revision {
                        out.extend_from_slice(encode_and_escape_bytes(rev.as_bytes()).as_bytes());
                    }
                    out.extend_from_slice(b"\" symlink_target=\"");
                    if let Some(target) = symlink_target {
                        out.extend_from_slice(encode_and_escape_string(target).as_bytes());
                    }
                    out.push(b'"');
                }
                out.extend_from_slice(b" />\n");
            }
            Entry::TreeReference {
                file_id,
                name,
                revision,
                reference_revision,
                ..
            } => {
                out.extend_from_slice(b"<tree-reference file_id=\"");
                out.extend_from_slice(encode_and_escape_bytes(file_id.as_bytes()).as_bytes());
                out.extend_from_slice(b"\" name=\"");
                out.extend_from_slice(encode_and_escape_string(name).as_bytes());
                out.push(b'"');
                out.extend_from_slice(&parent_str);
                if !working {
                    out.extend_from_slice(b" revision=\"");
                    if let Some(rev) = revision {
                        out.extend_from_slice(encode_and_escape_bytes(rev.as_bytes()).as_bytes());
                    }
                    out.extend_from_slice(b"\" reference_revision=\"");
                    if let Some(rref) = reference_revision {
                        out.extend_from_slice(encode_and_escape_bytes(rref.as_bytes()).as_bytes());
                    }
                    out.push(b'"');
                }
                out.extend_from_slice(b" />\n");
            }
            Entry::Root { .. } => {
                // The root is skipped above, but if we somehow encounter it
                // again (e.g. because iter_entries yielded it as a non-first
                // element) treat that as a logic error.
                return Err(Error::EncodeError(
                    "unexpected root encountered during serialization".to_string(),
                ));
            }
        }
    }
    out.extend_from_slice(b"</inventory>\n");
    Ok(())
}

/// Split a serialized inventory byte stream into per-line chunks, the way
/// Python's str.splitlines(keepends=True) does — one `\n`-terminated line per
/// chunk (the final line may be unterminated).
fn split_lines_keepends(data: &[u8]) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut start = 0;
    for (i, &b) in data.iter().enumerate() {
        if b == b'\n' {
            out.push(data[start..=i].to_vec());
            start = i + 1;
        }
    }
    if start < data.len() {
        out.push(data[start..].to_vec());
    }
    out
}

pub struct XMLInventorySerializer5;
pub struct XMLInventorySerializer6;
pub struct XMLInventorySerializer7;
pub struct XMLInventorySerializer8;

const SUPPORTED_KINDS_BASE: &[&str] = &["file", "directory", "symlink"];
const SUPPORTED_KINDS_WITH_TREE_REF: &[&str] = &["file", "directory", "symlink", "tree-reference"];

impl InventorySerializer for XMLInventorySerializer5 {
    fn format_num(&self) -> &'static [u8] {
        b"5"
    }

    fn support_altered_by_hack(&self) -> bool {
        true
    }

    fn write_inventory_to_lines(
        &self,
        inv: &MutableInventory,
        working: bool,
    ) -> Result<Vec<Vec<u8>>, Error> {
        let mut out = Vec::new();
        append_v5_root(&mut out, inv)?;
        // For v5 the comparison root_id is always TREE_ROOT, even if the
        // inventory's actual root file_id is something else; this matches
        // Python xml5.InventorySerializer_v5.root_id = inventory.ROOT_ID.
        serialize_inventory_flat(
            inv,
            &mut out,
            Some(ROOT_ID_BYTES),
            SUPPORTED_KINDS_BASE,
            working,
        )?;
        Ok(split_lines_keepends(&out))
    }

    fn read_inventory_from_lines(
        &self,
        lines: &[&[u8]],
        revision_id: Option<RevisionId>,
    ) -> Result<MutableInventory, Error> {
        let mut data = Vec::new();
        for line in lines {
            data.extend_from_slice(line);
        }
        let elt = parse_inventory_xml_root(&data)?;
        unpack_inventory_flat_v5(&elt, revision_id)
    }
}

impl InventorySerializer for XMLInventorySerializer6 {
    fn format_num(&self) -> &'static [u8] {
        b"6"
    }

    fn support_altered_by_hack(&self) -> bool {
        true
    }

    fn write_inventory_to_lines(
        &self,
        inv: &MutableInventory,
        working: bool,
    ) -> Result<Vec<Vec<u8>>, Error> {
        let mut out = Vec::new();
        append_v8_root(&mut out, b"6", inv)?;
        serialize_inventory_flat(inv, &mut out, None, SUPPORTED_KINDS_BASE, working)?;
        Ok(split_lines_keepends(&out))
    }

    fn read_inventory_from_lines(
        &self,
        lines: &[&[u8]],
        revision_id: Option<RevisionId>,
    ) -> Result<MutableInventory, Error> {
        let mut data = Vec::new();
        for line in lines {
            data.extend_from_slice(line);
        }
        let elt = parse_inventory_xml_root(&data)?;
        unpack_inventory_flat_v8(&elt, b"6", revision_id)
    }
}

impl InventorySerializer for XMLInventorySerializer7 {
    fn format_num(&self) -> &'static [u8] {
        b"7"
    }

    fn support_altered_by_hack(&self) -> bool {
        true
    }

    fn write_inventory_to_lines(
        &self,
        inv: &MutableInventory,
        working: bool,
    ) -> Result<Vec<Vec<u8>>, Error> {
        let mut out = Vec::new();
        append_v8_root(&mut out, b"7", inv)?;
        serialize_inventory_flat(inv, &mut out, None, SUPPORTED_KINDS_WITH_TREE_REF, working)?;
        Ok(split_lines_keepends(&out))
    }

    fn read_inventory_from_lines(
        &self,
        lines: &[&[u8]],
        revision_id: Option<RevisionId>,
    ) -> Result<MutableInventory, Error> {
        let mut data = Vec::new();
        for line in lines {
            data.extend_from_slice(line);
        }
        let elt = parse_inventory_xml_root(&data)?;
        unpack_inventory_flat_v8(&elt, b"7", revision_id)
    }
}

impl InventorySerializer for XMLInventorySerializer8 {
    fn format_num(&self) -> &'static [u8] {
        b"8"
    }

    fn support_altered_by_hack(&self) -> bool {
        true
    }

    fn write_inventory_to_lines(
        &self,
        inv: &MutableInventory,
        working: bool,
    ) -> Result<Vec<Vec<u8>>, Error> {
        let mut out = Vec::new();
        append_v8_root(&mut out, b"8", inv)?;
        serialize_inventory_flat(inv, &mut out, None, SUPPORTED_KINDS_BASE, working)?;
        Ok(split_lines_keepends(&out))
    }

    fn read_inventory_from_lines(
        &self,
        lines: &[&[u8]],
        revision_id: Option<RevisionId>,
    ) -> Result<MutableInventory, Error> {
        let mut data = Vec::new();
        for line in lines {
            data.extend_from_slice(line);
        }
        let elt = parse_inventory_xml_root(&data)?;
        unpack_inventory_flat_v8(&elt, b"8", revision_id)
    }
}

/// File-id and revision-id tuples found in an inventory line.
pub fn find_text_key_references<'a, I>(iter: I) -> Result<HashMap<(Vec<u8>, Vec<u8>), bool>, Error>
where
    I: IntoIterator<Item = (&'a [u8], &'a [u8])>,
{
    use lazy_regex::regex_captures;
    let mut result: HashMap<(Vec<u8>, Vec<u8>), bool> = HashMap::new();
    let mut unescape_cache: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

    for (line, line_key) in iter {
        // The Python search regex is:
        // b'file_id="(?P<file_id>[^"]+)".* revision="(?P<revision_id>[^"]+)"'
        // We must match against bytes — fancy_regex/lazy-regex unicode is fine
        // because the bytes are ASCII-safe enough for this match.
        let line_str = match str::from_utf8(line) {
            Ok(s) => s,
            Err(_) => continue,
        };
        let cap = regex_captures!(r#"file_id="([^"]+)".* revision="([^"]+)""#, line_str);
        let (_full, file_id, revision_id) = match cap {
            Some(c) => c,
            None => continue,
        };
        let file_id_b = file_id.as_bytes();
        let revision_id_b = revision_id.as_bytes();

        let revision_decoded = if let Some(v) = unescape_cache.get(revision_id_b) {
            v.clone()
        } else {
            let dec = unescape_xml(revision_id_b)?;
            unescape_cache.insert(revision_id_b.to_vec(), dec.clone());
            dec
        };
        let file_id_decoded = if let Some(v) = unescape_cache.get(file_id_b) {
            v.clone()
        } else {
            let dec = unescape_xml(file_id_b)?;
            unescape_cache.insert(file_id_b.to_vec(), dec.clone());
            dec
        };

        let key = (file_id_decoded, revision_decoded.clone());
        result.entry(key.clone()).or_insert(false);
        if revision_decoded == line_key {
            result.insert(key, true);
        }
    }
    Ok(result)
}

/// Version 4 revision serializer: deserialization-only. v4 also stores
/// inventory_id and parent_sha1s as extra metadata.
pub struct XMLRevisionSerializer4;

#[derive(Debug, Clone, PartialEq)]
pub struct RevisionV4 {
    pub revision: Revision,
    pub inventory_id: Option<Vec<u8>>,
    pub parent_sha1s: Vec<Option<Vec<u8>>>,
}

impl XMLRevisionSerializer4 {
    pub fn read_revision_from_string(&self, data: &[u8]) -> Result<RevisionV4, Error> {
        let elt = Element::parse(data)
            .map_err(|e| Error::DecodeError(format!("XML parse error: {}", e)))?;
        self.unpack_revision(&elt)
    }

    pub fn read_revision(&self, file: &mut dyn Read) -> Result<RevisionV4, Error> {
        let elt = Element::parse(file)
            .map_err(|e| Error::DecodeError(format!("XML parse error: {}", e)))?;
        self.unpack_revision(&elt)
    }

    fn unpack_revision(&self, elt: &Element) -> Result<RevisionV4, Error> {
        // <changeset> is deprecated...
        if elt.name != "revision" && elt.name != "changeset" {
            return Err(Error::DecodeError(format!(
                "unexpected tag in revision file: {}",
                elt.name
            )));
        }
        let timezone = match elt.attributes.get("timezone") {
            Some(s) => Some(
                s.parse::<i32>()
                    .map_err(|e| Error::DecodeError(format!("bad timezone: {}", e)))?,
            ),
            None => None,
        };
        let message = elt.get_child("message").map_or_else(
            || "".to_string(),
            |e| {
                e.get_text()
                    .map_or_else(|| "".to_owned(), |t| t.to_string())
            },
        );
        let precursor = elt.attributes.get("precursor").cloned();
        let precursor_sha1 = elt.attributes.get("precursor_sha1").cloned();

        let mut parent_ids: Vec<RevisionId> = Vec::new();
        let mut parent_sha1s: Vec<Option<Vec<u8>>> = Vec::new();
        if let Some(pelts) = elt.get_child("parents") {
            for p in pelts.children.iter().filter_map(|c| c.as_element()) {
                let rid = p
                    .attributes
                    .get("revision_id")
                    .ok_or_else(|| Error::DecodeError("parent missing revision_id".to_string()))?;
                parent_ids.push(RevisionId::from(rid.as_bytes()));
                parent_sha1s.push(
                    p.attributes
                        .get("revision_sha1")
                        .map(|s| s.as_bytes().to_vec()),
                );
            }
        } else if let Some(precursor) = precursor {
            // revisions written prior to 0.0.5 have a single precursor
            // given as an attribute.
            parent_ids.push(RevisionId::from(precursor.as_bytes()));
            parent_sha1s.push(precursor_sha1.map(|s| s.as_bytes().to_vec()));
        }

        let timestamp = elt
            .attributes
            .get("timestamp")
            .ok_or_else(|| Error::DecodeError("missing timestamp".to_string()))?
            .parse::<f64>()
            .map_err(|e| Error::DecodeError(format!("bad timestamp: {}", e)))?;
        let revision_id = elt
            .attributes
            .get("revision_id")
            .ok_or_else(|| Error::DecodeError("missing revision_id".to_string()))?;
        let revision_id = RevisionId::from(revision_id.as_bytes());
        let inventory_id = elt
            .attributes
            .get("inventory_id")
            .map(|s| s.as_bytes().to_vec());
        let inventory_sha1 = elt
            .attributes
            .get("inventory_sha1")
            .map(|s| s.as_bytes().to_vec());
        let committer = elt.attributes.get("committer").cloned();

        let revision = Revision::new(
            revision_id,
            parent_ids,
            committer,
            message,
            HashMap::new(),
            inventory_sha1,
            timestamp,
            timezone,
        );

        Ok(RevisionV4 {
            revision,
            inventory_id,
            parent_sha1s,
        })
    }
}

/// Version 0.0.4 inventory serializer (deserialization only).
///
/// v4 entries use `<entry>` tags with a `kind` attribute, and may carry a
/// `text_id` field for files. The root id comes from the inventory element's
/// `file_id` attribute (defaulting to TREE_ROOT). v4 has no format attribute,
/// no revision_id, no rich roots, and no tree-references.
pub struct XMLInventorySerializer4;

impl InventorySerializer for XMLInventorySerializer4 {
    fn format_num(&self) -> &'static [u8] {
        b"4"
    }

    fn write_inventory_to_lines(
        &self,
        _inv: &MutableInventory,
        _working: bool,
    ) -> Result<Vec<Vec<u8>>, Error> {
        // v4 serialisation is no longer supported, only deserialisation.
        Err(Error::EncodeError(
            "v4 inventory serialisation is not supported".to_string(),
        ))
    }

    fn read_inventory_from_lines(
        &self,
        lines: &[&[u8]],
        _revision_id: Option<RevisionId>,
    ) -> Result<MutableInventory, Error> {
        let mut data = Vec::new();
        for line in lines {
            data.extend_from_slice(line);
        }
        XMLInventorySerializer4.read_inventory_from_string(&data)
    }
}

fn unpack_inventory_entry_v4(elt: &Element, root_id: &FileId) -> Result<Entry, Error> {
    if elt.name != "entry" {
        return Err(Error::DecodeError(format!(
            "unexpected tag in v4 inventory: {}",
            elt.name
        )));
    }
    let file_id = elt
        .attributes
        .get("file_id")
        .ok_or_else(|| Error::DecodeError("entry missing file_id".to_string()))?;
    let file_id = FileId::from(file_id.as_bytes());
    let name = elt.attributes.get("name").cloned().unwrap_or_default();
    // v4 doesn't carry parent_id for top-level nodes; map missing/ROOT_ID
    // to the inventory's root id, matching xml4.py._unpack_entry.
    let parent_id = match elt.attributes.get("parent_id") {
        Some(s) if s.as_bytes() != ROOT_ID_BYTES => FileId::from(s.as_bytes()),
        _ => root_id.clone(),
    };
    let kind = elt
        .attributes
        .get("kind")
        .ok_or_else(|| Error::DecodeError("entry missing kind".to_string()))?;
    match kind.as_str() {
        "directory" => Ok(Entry::directory(file_id, name, parent_id, None)),
        "file" => {
            let text_id = elt.attributes.get("text_id").map(|s| s.as_bytes().to_vec());
            let text_sha1 = elt
                .attributes
                .get("text_sha1")
                .map(|s| s.as_bytes().to_vec());
            let text_size = match elt.attributes.get("text_size") {
                Some(s) => Some(
                    s.parse::<u64>()
                        .map_err(|e| Error::DecodeError(format!("bad text_size: {}", e)))?,
                ),
                None => None,
            };
            Ok(Entry::file(
                file_id, name, parent_id, None, text_sha1, text_size, None, text_id,
            ))
        }
        "symlink" => {
            let symlink_target = elt.attributes.get("symlink_target").cloned();
            Ok(Entry::link(file_id, name, parent_id, None, symlink_target))
        }
        other => Err(Error::DecodeError(format!("unknown kind {:?}", other))),
    }
}

impl XMLInventorySerializer4 {
    pub fn read_inventory_from_string(&self, data: &[u8]) -> Result<MutableInventory, Error> {
        let elt = parse_inventory_xml_root(data)?;
        self.unpack_inventory(&elt)
    }

    pub fn read_inventory(&self, f: &mut dyn Read) -> Result<MutableInventory, Error> {
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        self.read_inventory_from_string(&buf)
    }

    fn unpack_inventory(&self, elt: &Element) -> Result<MutableInventory, Error> {
        if elt.name != "inventory" {
            return Err(Error::UnexpectedInventoryFormat(format!(
                "Root tag is {:?}",
                elt.name
            )));
        }
        let root_id_bytes = elt
            .attributes
            .get("file_id")
            .map(|s| s.as_bytes().to_vec())
            .unwrap_or_else(|| ROOT_ID_BYTES.to_vec());
        let root_id = FileId::from(root_id_bytes);

        let mut inv = MutableInventory::new();
        let root = Entry::root(root_id.clone(), None);
        inv.add(root)
            .map_err(|e| Error::DecodeError(format!("error adding root: {:?}", e)))?;

        for child in &elt.children {
            let child = match child.as_element() {
                Some(c) => c,
                None => continue,
            };
            let entry = unpack_inventory_entry_v4(child, &root_id)?;
            inv.add(entry)
                .map_err(|e| Error::DecodeError(format!("error adding entry: {:?}", e)))?;
        }
        Ok(inv)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_and_escape_simple_ascii_passes_through() {
        assert_eq!(encode_and_escape_string("foo bar"), "foo bar");
        assert_eq!(encode_and_escape_bytes(b"foo bar"), "foo bar");
    }

    #[test]
    fn encode_and_escape_xml_special_chars() {
        assert_eq!(
            encode_and_escape_string("&'\"<>"),
            "&amp;&apos;&quot;&lt;&gt;"
        );
        assert_eq!(
            encode_and_escape_bytes(b"&'\"<>"),
            "&amp;&apos;&quot;&lt;&gt;"
        );
    }

    #[test]
    fn encode_and_escape_utf8_with_xml() {
        // u'\xb5\xe5&\u062c'
        let utf8_str = b"\xc2\xb5\xc3\xa5&\xd8\xac";
        assert_eq!(
            encode_and_escape_bytes(utf8_str),
            "&#181;&#229;&amp;&#1580;"
        );
    }

    #[test]
    fn encode_and_escape_unicode_str() {
        let uni_str = "\u{b5}\u{e5}&\u{62c}";
        assert_eq!(
            encode_and_escape_string(uni_str),
            "&#181;&#229;&amp;&#1580;"
        );
    }

    #[test]
    fn escape_invalid_chars_keeps_normal_text() {
        assert_eq!(escape_invalid_chars("hello world"), "hello world");
    }

    #[test]
    fn escape_invalid_chars_escapes_control_codes() {
        // \x01 is a forbidden XML control char and should be escaped.
        assert_eq!(escape_invalid_chars("a\x01b"), "a\\x01b");
    }

    #[test]
    fn escape_invalid_chars_keeps_tab_newline_cr() {
        assert_eq!(escape_invalid_chars("a\tb\nc\rd"), "a\tb\nc\rd");
    }

    use crate::serializer::RevisionSerializer;

    const REVISION_V5: &[u8] = b"<revision committer=\"Martin Pool &lt;mbp@sourcefrog.net&gt;\"\n    inventory_sha1=\"e79c31c1deb64c163cf660fdedd476dd579ffd41\"\n    revision_id=\"mbp@sourcefrog.net-20050905080035-e0439293f8b6b9f9\"\n    timestamp=\"1125907235.212\"\n    timezone=\"36000\">\n<message>- start splitting code for xml (de)serialization away from objects\n  preparatory to supporting multiple formats by a single library\n</message>\n<parents>\n<revision_ref revision_id=\"mbp@sourcefrog.net-20050905063503-43948f59fa127d92\"/>\n</parents>\n</revision>\n";

    const REVISION_V5_UTC: &[u8] = b"<revision committer=\"Martin Pool &lt;mbp@sourcefrog.net&gt;\"\n    inventory_sha1=\"e79c31c1deb64c163cf660fdedd476dd579ffd41\"\n    revision_id=\"mbp@sourcefrog.net-20050905080035-e0439293f8b6b9f9\"\n    timestamp=\"1125907235.212\"\n    timezone=\"0\">\n<message>- start splitting code for xml (de)serialization away from objects\n  preparatory to supporting multiple formats by a single library\n</message>\n<parents>\n<revision_ref revision_id=\"mbp@sourcefrog.net-20050905063503-43948f59fa127d92\"/>\n</parents>\n</revision>\n";

    #[test]
    fn unpack_revision_v5_committer_and_timezone() {
        let serializer = XMLRevisionSerializer5;
        let rev = serializer.read_revision_from_string(REVISION_V5).unwrap();
        assert_eq!(
            rev.committer.as_deref(),
            Some("Martin Pool <mbp@sourcefrog.net>")
        );
        assert_eq!(rev.parent_ids.len(), 1);
        assert_eq!(rev.timezone, Some(36000));
        assert_eq!(
            rev.parent_ids[0].as_bytes(),
            b"mbp@sourcefrog.net-20050905063503-43948f59fa127d92"
        );
    }

    #[test]
    fn unpack_revision_v5_utc_timezone_zero() {
        let serializer = XMLRevisionSerializer5;
        let rev = serializer
            .read_revision_from_string(REVISION_V5_UTC)
            .unwrap();
        assert_eq!(rev.timezone, Some(0));
        assert_eq!(rev.parent_ids.len(), 1);
    }

    #[test]
    fn repack_revision_v5_round_trips() {
        let serializer = XMLRevisionSerializer5;
        let rev = serializer.read_revision_from_string(REVISION_V5).unwrap();
        let bytes = serializer.write_revision_to_string(&rev).unwrap();
        let rev2 = serializer.read_revision_from_string(&bytes).unwrap();
        assert_eq!(rev, rev2);
    }

    #[test]
    fn repack_revision_v5_utc_round_trips() {
        let serializer = XMLRevisionSerializer5;
        let rev = serializer
            .read_revision_from_string(REVISION_V5_UTC)
            .unwrap();
        let bytes = serializer.write_revision_to_string(&rev).unwrap();
        let rev2 = serializer.read_revision_from_string(&bytes).unwrap();
        assert_eq!(rev, rev2);
    }

    use crate::serializer::InventorySerializer;

    const COMMITTED_INV_V5: &[u8] = b"<inventory>\n<file file_id=\"bar-20050901064931-73b4b1138abc9cd2\"\n      name=\"bar\" parent_id=\"TREE_ROOT\"\n      revision=\"mbp@foo-123123\"\n      text_sha1=\"A\" text_size=\"1\"/>\n<directory name=\"subdir\"\n           file_id=\"foo-20050801201819-4139aa4a272f4250\"\n           parent_id=\"TREE_ROOT\"\n           revision=\"mbp@foo-00\"/>\n<file executable=\"yes\" file_id=\"bar-20050824000535-6bc48cfad47ed134\"\n      name=\"bar\" parent_id=\"foo-20050801201819-4139aa4a272f4250\"\n      revision=\"mbp@foo-00\"\n      text_sha1=\"B\" text_size=\"0\"/>\n</inventory>\n";

    const EXPECTED_INV_V5: &[u8] = b"<inventory format=\"5\">\n<file file_id=\"bar-20050901064931-73b4b1138abc9cd2\" name=\"bar\" revision=\"mbp@foo-123123\" text_sha1=\"A\" text_size=\"1\" />\n<directory file_id=\"foo-20050801201819-4139aa4a272f4250\" name=\"subdir\" revision=\"mbp@foo-00\" />\n<file executable=\"yes\" file_id=\"bar-20050824000535-6bc48cfad47ed134\" name=\"bar\" parent_id=\"foo-20050801201819-4139aa4a272f4250\" revision=\"mbp@foo-00\" text_sha1=\"B\" text_size=\"0\" />\n</inventory>\n";

    const EXPECTED_INV_V8: &[u8] = b"<inventory format=\"8\" revision_id=\"rev_outer\">\n<directory file_id=\"tree-root-321\" name=\"\" revision=\"rev_outer\" />\n<directory file_id=\"dir-id\" name=\"dir\" parent_id=\"tree-root-321\" revision=\"rev_outer\" />\n<file file_id=\"file-id\" name=\"file\" parent_id=\"tree-root-321\" revision=\"rev_outer\" text_sha1=\"A\" text_size=\"1\" />\n<symlink file_id=\"link-id\" name=\"link\" parent_id=\"tree-root-321\" revision=\"rev_outer\" symlink_target=\"a\" />\n</inventory>\n";

    #[test]
    fn inventory_v5_roundtrip() {
        let s = XMLInventorySerializer5;
        let inv = s
            .read_inventory_from_lines(&[COMMITTED_INV_V5], None)
            .unwrap();
        assert_eq!(inv.len(), 4);
        let bytes = s.write_inventory_to_string(&inv, false).unwrap();
        assert_eq!(bytes, EXPECTED_INV_V5);
        let inv2 = s.read_inventory_from_lines(&[&bytes], None).unwrap();
        assert_eq!(inv, inv2);
    }

    #[test]
    fn inventory_v8_roundtrip() {
        let s = XMLInventorySerializer8;
        let mut inv = MutableInventory::new();
        inv.revision_id = Some(RevisionId::from(b"rev_outer".as_slice()));
        inv.add(Entry::root(
            FileId::from(b"tree-root-321".as_slice()),
            Some(RevisionId::from(b"rev_outer".as_slice())),
        ))
        .unwrap();
        inv.add(Entry::directory(
            FileId::from(b"dir-id".as_slice()),
            "dir".to_string(),
            FileId::from(b"tree-root-321".as_slice()),
            Some(RevisionId::from(b"rev_outer".as_slice())),
        ))
        .unwrap();
        inv.add(Entry::file(
            FileId::from(b"file-id".as_slice()),
            "file".to_string(),
            FileId::from(b"tree-root-321".as_slice()),
            Some(RevisionId::from(b"rev_outer".as_slice())),
            Some(b"A".to_vec()),
            Some(1),
            Some(false),
            None,
        ))
        .unwrap();
        inv.add(Entry::link(
            FileId::from(b"link-id".as_slice()),
            "link".to_string(),
            FileId::from(b"tree-root-321".as_slice()),
            Some(RevisionId::from(b"rev_outer".as_slice())),
            Some("a".to_string()),
        ))
        .unwrap();
        let out = s.write_inventory_to_string(&inv, false).unwrap();
        assert_eq!(out, EXPECTED_INV_V8);
        let inv2 = s.read_inventory_from_lines(&[&out], None).unwrap();
        assert_eq!(inv, inv2);
    }

    #[test]
    fn inventory_v8_working_skips_history_data() {
        let s = XMLInventorySerializer8;
        let mut inv = MutableInventory::new();
        inv.revision_id = Some(RevisionId::from(b"rev_outer".as_slice()));
        inv.add(Entry::root(
            FileId::from(b"tree-root-321".as_slice()),
            Some(RevisionId::from(b"rev_outer".as_slice())),
        ))
        .unwrap();
        inv.add(Entry::directory(
            FileId::from(b"dir-id".as_slice()),
            "dir".to_string(),
            FileId::from(b"tree-root-321".as_slice()),
            Some(RevisionId::from(b"rev_outer".as_slice())),
        ))
        .unwrap();
        inv.add(Entry::file(
            FileId::from(b"file-id".as_slice()),
            "file".to_string(),
            FileId::from(b"tree-root-321".as_slice()),
            Some(RevisionId::from(b"rev_outer".as_slice())),
            Some(b"A".to_vec()),
            Some(1),
            Some(true),
            None,
        ))
        .unwrap();
        inv.add(Entry::link(
            FileId::from(b"link-id".as_slice()),
            "link".to_string(),
            FileId::from(b"tree-root-321".as_slice()),
            Some(RevisionId::from(b"rev_outer".as_slice())),
            Some("a".to_string()),
        ))
        .unwrap();
        let out = s.write_inventory_to_string(&inv, true).unwrap();
        // The root <directory> still carries `revision`, matching upstream
        // _append_inventory_root which is unaffected by `working`. Other
        // entries omit revision/text_sha1/text_size/symlink_target.
        let expected: &[u8] = b"<inventory format=\"8\" revision_id=\"rev_outer\">\n<directory file_id=\"tree-root-321\" name=\"\" revision=\"rev_outer\" />\n<directory file_id=\"dir-id\" name=\"dir\" parent_id=\"tree-root-321\" />\n<file executable=\"yes\" file_id=\"file-id\" name=\"file\" parent_id=\"tree-root-321\" />\n<symlink file_id=\"link-id\" name=\"link\" parent_id=\"tree-root-321\" />\n</inventory>\n";
        assert_eq!(out, expected);
    }

    #[test]
    fn inventory_v5_no_format_attribute_uses_argument_revision_id() {
        let s = XMLInventorySerializer5;
        let inv = s
            .read_inventory_from_lines(
                &[b"<inventory format=\"5\">\n</inventory>\n"],
                Some(RevisionId::from(b"test-rev-id".as_slice())),
            )
            .unwrap();
        assert_eq!(
            inv.root().unwrap().revision().map(|r| r.as_bytes()),
            Some(b"test-rev-id".as_slice())
        );
    }

    #[test]
    fn inventory_v5_revision_id_from_data() {
        let s = XMLInventorySerializer5;
        let inv = s
            .read_inventory_from_lines(
                &[b"<inventory format=\"5\" revision_id=\"a-rev-id\">\n</inventory>\n"],
                Some(RevisionId::from(b"test-rev-id".as_slice())),
            )
            .unwrap();
        assert_eq!(
            inv.root().unwrap().revision().map(|r| r.as_bytes()),
            Some(b"a-rev-id".as_slice())
        );
    }

    #[test]
    fn unescape_xml_basic() {
        assert_eq!(unescape_xml(b"foo&amp;bar").unwrap(), b"foo&bar".to_vec());
        assert_eq!(unescape_xml(b"&lt;tag&gt;").unwrap(), b"<tag>".to_vec());
        assert_eq!(unescape_xml(b"&#181;").unwrap(), b"\xc2\xb5".to_vec());
    }

    #[test]
    fn unescape_xml_unknown_entity() {
        assert!(unescape_xml(b"foo&bar;").is_err());
    }

    const REVISION_V4: &[u8] = b"<revision committer=\"Test\" timestamp=\"1.0\" revision_id=\"r1\" inventory_id=\"i1\" inventory_sha1=\"sha\">\n<message>hi</message>\n<parents>\n<revision_ref revision_id=\"p1\" revision_sha1=\"psha\"/>\n</parents>\n</revision>";

    #[test]
    fn revision_v4_unpack() {
        let s = XMLRevisionSerializer4;
        let rv4 = s.read_revision_from_string(REVISION_V4).unwrap();
        assert_eq!(rv4.revision.revision_id.as_bytes(), b"r1");
        assert_eq!(rv4.inventory_id.as_deref(), Some(b"i1".as_slice()));
        assert_eq!(rv4.parent_sha1s.len(), 1);
        assert_eq!(rv4.parent_sha1s[0].as_deref(), Some(b"psha".as_slice()));
    }

    const INVENTORY_V4: &[u8] = b"<inventory>\n<entry kind=\"directory\" file_id=\"src-id\" name=\"src\"/>\n<entry kind=\"file\" file_id=\"foo-id\" name=\"foo.c\" parent_id=\"src-id\" text_sha1=\"abc\" text_size=\"3\" text_id=\"tid\"/>\n<entry kind=\"symlink\" file_id=\"link-id\" name=\"l\" symlink_target=\"target\"/>\n</inventory>";

    #[test]
    fn inventory_v4_unpack() {
        use crate::inventory::Inventory as _;

        let s = XMLInventorySerializer4;
        let inv = s.read_inventory_from_string(INVENTORY_V4).unwrap();
        // root + 3 entries
        assert_eq!(inv.len(), 4);

        let foo = inv
            .get_entry(&FileId::from(b"foo-id".as_slice()))
            .expect("foo-id present");
        match foo {
            Entry::File {
                text_sha1,
                text_size,
                text_id,
                ..
            } => {
                assert_eq!(text_sha1.as_deref(), Some(b"abc".as_slice()));
                assert_eq!(text_size, &Some(3u64));
                assert_eq!(text_id.as_deref(), Some(b"tid".as_slice()));
            }
            other => panic!("expected file, got {:?}", other),
        }

        let link = inv
            .get_entry(&FileId::from(b"link-id".as_slice()))
            .expect("link-id present");
        match link {
            Entry::Link { symlink_target, .. } => {
                assert_eq!(symlink_target.as_deref(), Some("target"));
            }
            other => panic!("expected symlink, got {:?}", other),
        }
    }

    #[test]
    fn inventory_v4_root_id_from_attribute() {
        let s = XMLInventorySerializer4;
        let inv = s
            .read_inventory_from_string(b"<inventory file_id=\"alt-root\"></inventory>")
            .unwrap();
        assert_eq!(
            inv.root().unwrap().file_id().as_bytes(),
            b"alt-root".as_slice()
        );
    }

    #[test]
    fn inventory_v4_default_root_id_is_tree_root() {
        let s = XMLInventorySerializer4;
        let inv = s
            .read_inventory_from_string(b"<inventory></inventory>")
            .unwrap();
        assert_eq!(inv.root().unwrap().file_id().as_bytes(), b"TREE_ROOT");
    }

    #[test]
    fn inventory_v4_unknown_kind_errors() {
        let s = XMLInventorySerializer4;
        let err = s
            .read_inventory_from_string(
                b"<inventory>\n<entry kind=\"weird\" file_id=\"x\" name=\"x\"/>\n</inventory>",
            )
            .unwrap_err();
        match err {
            Error::DecodeError(msg) => assert!(msg.contains("unknown kind")),
            other => panic!("expected DecodeError, got {:?}", other),
        }
    }
}
