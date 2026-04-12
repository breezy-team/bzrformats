//! Bazaar container format 1 serialization.
//!
//! Port of the pure-logic core of `bzrformats/pack.py`. This module covers
//! name validation, header/record construction, and (in a follow-up) the
//! push parser. I/O-oriented wrappers (`ContainerWriter`, `ContainerReader`,
//! transport plumbing) stay in Python.

/// Magic bytes written at the start of a format-1 container (without the
/// trailing newline).
pub const FORMAT_ONE: &[u8] = b"Bazaar pack format 1 (introduced in 0.18)";

/// Errors raised by this module. Python callers wrap these in their own
/// `ContainerError` hierarchy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PackError {
    /// A name contained a whitespace byte (tab, LF, VT, FF, CR, space).
    InvalidName(Vec<u8>),
    /// The first line of the container was not the expected format marker.
    UnknownContainerFormat(Vec<u8>),
    /// A record type byte other than `B` or `E` was encountered.
    UnknownRecordType(u8),
    /// A record length line was not a decimal integer.
    InvalidRecord(String),
}

impl std::fmt::Display for PackError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PackError::InvalidName(n) => write!(f, "{:?} is not a valid name.", n),
            PackError::UnknownContainerFormat(line) => {
                write!(f, "unrecognised container format: {:?}", line)
            }
            PackError::UnknownRecordType(b) => {
                write!(f, "unknown record type: {:?}", &[*b])
            }
            PackError::InvalidRecord(reason) => write!(f, "invalid record: {}", reason),
        }
    }
}

impl std::error::Error for PackError {}

/// True if `byte` is one of the whitespace bytes rejected by [`check_name`].
///
/// Matches the Python regex `[\t\n\x0b\x0c\r ]`.
#[inline]
fn is_whitespace(byte: u8) -> bool {
    matches!(byte, b'\t' | b'\n' | 0x0b | 0x0c | b'\r' | b' ')
}

/// Reject names that contain whitespace. Matches `pack._check_name`.
pub fn check_name(name: &[u8]) -> Result<(), PackError> {
    if name.iter().any(|&b| is_whitespace(b)) {
        return Err(PackError::InvalidName(name.to_vec()));
    }
    Ok(())
}

/// Bytes to begin a container: the format line plus a newline.
pub fn begin() -> Vec<u8> {
    let mut out = Vec::with_capacity(FORMAT_ONE.len() + 1);
    out.extend_from_slice(FORMAT_ONE);
    out.push(b'\n');
    out
}

/// Bytes to finish a container.
pub fn end() -> &'static [u8] {
    b"E"
}

/// Serialize a bytes-record header: kind marker, length, names, separator.
///
/// Each name is a tuple of parts; parts are joined by NUL and terminated by
/// `\n`. An empty line marks the end of the name list. Names are validated
/// via [`check_name`] — note the Python implementation leaves a partially
/// written header if a later name fails, but for the pure-function port we
/// validate up front so the returned bytes are always self-consistent.
pub fn bytes_header(length: usize, names: &[Vec<Vec<u8>>]) -> Result<Vec<u8>, PackError> {
    for name_tuple in names {
        for part in name_tuple {
            check_name(part)?;
        }
    }
    let mut out = Vec::new();
    out.push(b'B');
    out.extend_from_slice(format!("{}\n", length).as_bytes());
    for name_tuple in names {
        for (i, part) in name_tuple.iter().enumerate() {
            if i > 0 {
                out.push(0);
            }
            out.extend_from_slice(part);
        }
        out.push(b'\n');
    }
    out.push(b'\n');
    Ok(out)
}

/// Serialize a full bytes record (header followed by `body`).
pub fn bytes_record(body: &[u8], names: &[Vec<Vec<u8>>]) -> Result<Vec<u8>, PackError> {
    let header = bytes_header(body.len(), names)?;
    let mut out = Vec::with_capacity(header.len() + body.len());
    out.extend_from_slice(&header);
    out.extend_from_slice(body);
    Ok(out)
}

/// One parsed record: its list of name tuples and its body bytes.
pub type Record = (Vec<Vec<Vec<u8>>>, Vec<u8>);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(clippy::enum_variant_names)]
enum State {
    ExpectingFormatLine,
    ExpectingRecordType,
    ExpectingLength,
    ExpectingName,
    ExpectingBody,
    ExpectingNothing,
}

/// Incremental parser for container format 1. Mirrors the Python
/// `ContainerPushParser`: callers push bytes via [`accept_bytes`] and pull
/// completed records via [`read_pending_records`].
///
/// [`accept_bytes`]: ContainerPushParser::accept_bytes
/// [`read_pending_records`]: ContainerPushParser::read_pending_records
#[derive(Debug)]
pub struct ContainerPushParser {
    buffer: Vec<u8>,
    state: State,
    parsed_records: Vec<Record>,
    current_record_length: Option<usize>,
    current_record_names: Vec<Vec<Vec<u8>>>,
    finished: bool,
}

impl Default for ContainerPushParser {
    fn default() -> Self {
        Self::new()
    }
}

impl ContainerPushParser {
    pub fn new() -> Self {
        Self {
            buffer: Vec::new(),
            state: State::ExpectingFormatLine,
            parsed_records: Vec::new(),
            current_record_length: None,
            current_record_names: Vec::new(),
            finished: false,
        }
    }

    pub fn finished(&self) -> bool {
        self.finished
    }

    /// Feed more bytes to the parser. Runs the state machine until it stops
    /// making progress.
    pub fn accept_bytes(&mut self, bytes: &[u8]) -> Result<(), PackError> {
        self.buffer.extend_from_slice(bytes);
        let mut last_len = None;
        let mut last_state = None;
        while last_len != Some(self.buffer.len()) || last_state != Some(self.state) {
            last_len = Some(self.buffer.len());
            last_state = Some(self.state);
            self.step()?;
        }
        Ok(())
    }

    /// Drain up to `max` parsed records (or all of them when `max` is
    /// `None`).
    pub fn read_pending_records(&mut self, max: Option<usize>) -> Vec<Record> {
        match max {
            Some(n) if n < self.parsed_records.len() => self.parsed_records.drain(..n).collect(),
            _ => std::mem::take(&mut self.parsed_records),
        }
    }

    /// A hint for how many bytes should be read from the underlying source
    /// next. Matches the Python implementation: 16 KiB default, but at
    /// least the remaining body length when mid-record.
    pub fn read_size_hint(&self) -> usize {
        let hint = 16384;
        if self.state == State::ExpectingBody {
            let need = self
                .current_record_length
                .expect("length set before body state")
                .saturating_sub(self.buffer.len());
            hint.max(need)
        } else {
            hint
        }
    }

    /// Consume a `\n`-terminated line from the buffer (without the newline).
    /// Returns `None` if no complete line is available yet.
    fn consume_line(&mut self) -> Option<Vec<u8>> {
        let pos = self.buffer.iter().position(|&b| b == b'\n')?;
        let line: Vec<u8> = self.buffer.drain(..=pos).take(pos).collect();
        Some(line)
    }

    fn step(&mut self) -> Result<(), PackError> {
        match self.state {
            State::ExpectingFormatLine => {
                if let Some(line) = self.consume_line() {
                    if line != FORMAT_ONE {
                        return Err(PackError::UnknownContainerFormat(line));
                    }
                    self.state = State::ExpectingRecordType;
                }
            }
            State::ExpectingRecordType => {
                if let Some(&b) = self.buffer.first() {
                    self.buffer.drain(..1);
                    match b {
                        b'B' => self.state = State::ExpectingLength,
                        b'E' => {
                            self.finished = true;
                            self.state = State::ExpectingNothing;
                        }
                        other => return Err(PackError::UnknownRecordType(other)),
                    }
                }
            }
            State::ExpectingLength => {
                if let Some(line) = self.consume_line() {
                    let s = std::str::from_utf8(&line).map_err(|_| {
                        PackError::InvalidRecord(format!("{:?} is not a valid length.", line))
                    })?;
                    let n: usize = s.parse().map_err(|_| {
                        PackError::InvalidRecord(format!("{:?} is not a valid length.", line))
                    })?;
                    self.current_record_length = Some(n);
                    self.state = State::ExpectingName;
                }
            }
            State::ExpectingName => {
                if let Some(line) = self.consume_line() {
                    if line.is_empty() {
                        self.state = State::ExpectingBody;
                    } else {
                        let parts: Vec<Vec<u8>> =
                            line.split(|&b| b == 0).map(|s| s.to_vec()).collect();
                        for part in &parts {
                            check_name(part)?;
                        }
                        self.current_record_names.push(parts);
                    }
                }
            }
            State::ExpectingBody => {
                let need = self.current_record_length.expect("length set before body");
                if self.buffer.len() >= need {
                    let body: Vec<u8> = self.buffer.drain(..need).collect();
                    let names = std::mem::take(&mut self.current_record_names);
                    self.parsed_records.push((names, body));
                    self.current_record_length = None;
                    self.state = State::ExpectingRecordType;
                }
            }
            State::ExpectingNothing => {}
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(parts: &[&[u8]]) -> Vec<Vec<u8>> {
        parts.iter().map(|p| p.to_vec()).collect()
    }

    #[test]
    fn check_name_accepts_plain_bytes() {
        assert_eq!(check_name(b"abc"), Ok(()));
        assert_eq!(check_name(b""), Ok(()));
        assert_eq!(check_name(b"\x00\xff"), Ok(()));
    }

    #[test]
    fn check_name_rejects_every_whitespace_byte() {
        for &b in &[b'\t', b'\n', 0x0b, 0x0c, b'\r', b' '] {
            let input = vec![b'a', b, b'b'];
            assert_eq!(
                check_name(&input),
                Err(PackError::InvalidName(input.clone())),
                "byte {:#x} should be rejected",
                b
            );
        }
    }

    #[test]
    fn begin_matches_python() {
        assert_eq!(
            begin(),
            b"Bazaar pack format 1 (introduced in 0.18)\n".to_vec()
        );
    }

    #[test]
    fn end_marker() {
        assert_eq!(end(), b"E");
    }

    #[test]
    fn bytes_header_no_names() {
        // Mirrors test_pack.test_bytes_record_no_name.
        assert_eq!(bytes_header(0, &[]).unwrap(), b"B0\n\n".to_vec());
    }

    #[test]
    fn bytes_header_one_name_one_part() {
        let names = vec![name(&[b"name"])];
        assert_eq!(bytes_header(0, &names).unwrap(), b"B0\nname\n\n".to_vec());
    }

    #[test]
    fn bytes_header_one_name_two_parts() {
        let names = vec![name(&[b"part1", b"part2"])];
        assert_eq!(
            bytes_header(0, &names).unwrap(),
            b"B0\npart1\x00part2\n\n".to_vec()
        );
    }

    #[test]
    fn bytes_header_two_names() {
        let names = vec![name(&[b"name1"]), name(&[b"name2"])];
        assert_eq!(
            bytes_header(0, &names).unwrap(),
            b"B0\nname1\nname2\n\n".to_vec()
        );
    }

    #[test]
    fn bytes_record_concatenates_header_and_body() {
        let body = b"body bytes";
        let names = vec![name(&[b"foo"])];
        let got = bytes_record(body, &names).unwrap();
        let mut expected = format!("B{}\nfoo\n\n", body.len()).into_bytes();
        expected.extend_from_slice(body);
        assert_eq!(got, expected);
    }

    #[test]
    fn bytes_header_rejects_whitespace_in_name() {
        let names = vec![name(&[b"bad name"])];
        assert_eq!(
            bytes_header(0, &names),
            Err(PackError::InvalidName(b"bad name".to_vec()))
        );
    }

    #[test]
    fn bytes_header_reports_correct_length() {
        let names = vec![name(&[b"foo"])];
        assert_eq!(bytes_header(42, &names).unwrap(), b"B42\nfoo\n\n".to_vec());
    }

    fn make_container(records: &[(&[&[&[u8]]], &[u8])]) -> Vec<u8> {
        let mut out = begin();
        for (names, body) in records {
            let name_tuples: Vec<Vec<Vec<u8>>> = names
                .iter()
                .map(|nt| nt.iter().map(|p| p.to_vec()).collect())
                .collect();
            out.extend_from_slice(&bytes_record(body, &name_tuples).unwrap());
        }
        out.extend_from_slice(end());
        out
    }

    #[test]
    fn parser_empty_container() {
        let data = make_container(&[]);
        let mut p = ContainerPushParser::new();
        p.accept_bytes(&data).unwrap();
        assert!(p.finished());
        assert!(p.read_pending_records(None).is_empty());
    }

    #[test]
    fn parser_one_record() {
        let data = make_container(&[(&[&[b"name"]], b"body")]);
        let mut p = ContainerPushParser::new();
        p.accept_bytes(&data).unwrap();
        let records = p.read_pending_records(None);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].0, vec![vec![b"name".to_vec()]]);
        assert_eq!(records[0].1, b"body");
        assert!(p.finished());
    }

    #[test]
    fn parser_multi_name_record() {
        let data = make_container(&[(&[&[b"a", b"b"], &[b"c"]], b"xy")]);
        let mut p = ContainerPushParser::new();
        p.accept_bytes(&data).unwrap();
        let records = p.read_pending_records(None);
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].0,
            vec![vec![b"a".to_vec(), b"b".to_vec()], vec![b"c".to_vec()]]
        );
        assert_eq!(records[0].1, b"xy");
    }

    #[test]
    fn parser_streams_byte_by_byte() {
        let data = make_container(&[(&[&[b"first"]], b"one"), (&[&[b"second"]], b"two-body")]);
        let mut p = ContainerPushParser::new();
        for chunk in data.chunks(1) {
            p.accept_bytes(chunk).unwrap();
        }
        let records = p.read_pending_records(None);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].1, b"one");
        assert_eq!(records[1].1, b"two-body");
        assert!(p.finished());
    }

    #[test]
    fn parser_read_pending_records_max() {
        let data = make_container(&[(&[&[b"a"]], b"1"), (&[&[b"b"]], b"2"), (&[&[b"c"]], b"3")]);
        let mut p = ContainerPushParser::new();
        p.accept_bytes(&data).unwrap();
        let first = p.read_pending_records(Some(2));
        assert_eq!(first.len(), 2);
        let rest = p.read_pending_records(None);
        assert_eq!(rest.len(), 1);
        assert_eq!(rest[0].1, b"3");
    }

    #[test]
    fn parser_unknown_format() {
        let mut p = ContainerPushParser::new();
        let err = p.accept_bytes(b"garbage\n").unwrap_err();
        assert!(matches!(err, PackError::UnknownContainerFormat(_)));
    }

    #[test]
    fn parser_unknown_record_type() {
        let mut data = begin();
        data.push(b'X');
        let mut p = ContainerPushParser::new();
        let err = p.accept_bytes(&data).unwrap_err();
        assert_eq!(err, PackError::UnknownRecordType(b'X'));
    }

    #[test]
    fn parser_invalid_length() {
        let mut data = begin();
        data.extend_from_slice(b"Bnotanumber\n");
        let mut p = ContainerPushParser::new();
        let err = p.accept_bytes(&data).unwrap_err();
        assert!(matches!(err, PackError::InvalidRecord(_)));
    }

    #[test]
    fn parser_read_size_hint_defaults_to_16k() {
        let p = ContainerPushParser::new();
        assert_eq!(p.read_size_hint(), 16384);
    }

    #[test]
    fn parser_record_with_no_name() {
        // Mirrors test_pack.test_record_with_no_name: an empty name list.
        let data = make_container(&[(&[], b"aaaaa")]);
        let mut p = ContainerPushParser::new();
        p.accept_bytes(&data).unwrap();
        let records = p.read_pending_records(None);
        assert_eq!(records.len(), 1);
        let (names, body) = &records[0];
        assert!(names.is_empty());
        assert_eq!(body, b"aaaaa");
    }

    #[test]
    fn parser_two_separate_names() {
        // Mirrors test_multiple_records_at_once: two records each with a
        // single single-part name.
        let data = make_container(&[(&[&[b"name1"]], b"body1"), (&[&[b"name2"]], b"body2")]);
        let mut p = ContainerPushParser::new();
        p.accept_bytes(&data).unwrap();
        let records = p.read_pending_records(None);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].0, vec![vec![b"name1".to_vec()]]);
        assert_eq!(records[1].0, vec![vec![b"name2".to_vec()]]);
    }

    #[test]
    fn parser_multiple_names_on_one_record() {
        // Mirrors test_record_with_two_names: one record, two separate
        // single-part names.
        let data = make_container(&[(&[&[b"n1"], &[b"n2"]], b"xy")]);
        let mut p = ContainerPushParser::new();
        p.accept_bytes(&data).unwrap();
        let records = p.read_pending_records(None);
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].0,
            vec![vec![b"n1".to_vec()], vec![b"n2".to_vec()]]
        );
    }

    #[test]
    fn parser_incomplete_record_drains_nothing() {
        // Mirrors test_incomplete_record: feed only a header, no body;
        // no records should be ready to drain.
        let mut data = begin();
        data.extend_from_slice(b"B5\nname\n\n");
        let mut p = ContainerPushParser::new();
        p.accept_bytes(&data).unwrap();
        assert!(p.read_pending_records(None).is_empty());
    }

    #[test]
    fn parser_multiple_empty_records_at_once() {
        // Mirrors test_pack.test_multiple_empty_records_at_once. Two
        // zero-body records fed in one chunk must both drain — the
        // progress check needs to account for state transitions, not
        // just buffer shrinkage, since an empty body consumes no bytes.
        let data = make_container(&[(&[&[b"name1"]], b""), (&[&[b"name2"]], b"")]);
        let mut p = ContainerPushParser::new();
        p.accept_bytes(&data).unwrap();
        let records = p.read_pending_records(None);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].1, b"");
        assert_eq!(records[1].1, b"");
        assert_eq!(records[0].0, vec![vec![b"name1".to_vec()]]);
        assert_eq!(records[1].0, vec![vec![b"name2".to_vec()]]);
    }

    #[test]
    fn parser_accept_empty_bytes_is_a_noop() {
        // Mirrors test_accept_nothing: feeding an empty slice shouldn't
        // crash or advance state.
        let mut p = ContainerPushParser::new();
        p.accept_bytes(b"").unwrap();
        assert!(p.read_pending_records(None).is_empty());
        assert!(!p.finished());
    }

    #[test]
    fn parser_rejects_whitespace_in_name() {
        // Mirrors test_read_invalid_name_whitespace: a name containing a
        // space fails validation during parsing.
        let mut data = begin();
        data.extend_from_slice(b"B5\nbad name\n\nhello");
        let mut p = ContainerPushParser::new();
        let err = p.accept_bytes(&data).unwrap_err();
        assert!(matches!(err, PackError::InvalidName(_)));
    }

    #[test]
    fn parser_read_size_hint_covers_large_body() {
        let body = vec![0u8; 100_000];
        let data = make_container(&[(&[&[b"big"]], &body)]);
        let header_len = data.len() - body.len() - end().len();
        let mut p = ContainerPushParser::new();
        p.accept_bytes(&data[..header_len + 10]).unwrap();
        // Still needs body.len() - 10 more bytes, which is bigger than 16K.
        assert!(p.read_size_hint() >= body.len() - 10);
    }
}
