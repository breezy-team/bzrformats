//! Bazaar container format 1 serialization.
//!
//! Port of the pure-logic core of `bzrformats/pack.py`, plus stream-oriented
//! reader/writer types. Transport plumbing (`ReadVFile`,
//! `make_readv_reader`) stays in Python — that's a thin adapter over the
//! transport layer.

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

/// `_check_name_encoding` from pack.py: rejects names that aren't valid UTF-8.
pub fn check_name_encoding(name: &[u8]) -> Result<(), PackError> {
    std::str::from_utf8(name)
        .map(|_| ())
        .map_err(|e| PackError::InvalidRecord(e.to_string()))
}

/// Errors that can happen while reading a container stream.
#[derive(Debug)]
pub enum ReadError {
    Pack(PackError),
    Io(std::io::Error),
    /// Stream ended before the container was complete.
    UnexpectedEof,
    /// Trailing bytes after the End marker.
    ExcessData(Vec<u8>),
    /// `validate` saw the same name tuple twice.
    DuplicateName(Vec<Vec<u8>>),
}

impl std::fmt::Display for ReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReadError::Pack(e) => write!(f, "{}", e),
            ReadError::Io(e) => write!(f, "{}", e),
            ReadError::UnexpectedEof => write!(f, "unexpected end of container stream"),
            ReadError::ExcessData(d) => {
                write!(f, "container has data after end marker: {:?}", d)
            }
            ReadError::DuplicateName(n) => {
                write!(
                    f,
                    "container has multiple records with the same name: {:?}",
                    n
                )
            }
        }
    }
}

impl std::error::Error for ReadError {}

impl From<PackError> for ReadError {
    fn from(e: PackError) -> Self {
        ReadError::Pack(e)
    }
}

impl From<std::io::Error> for ReadError {
    fn from(e: std::io::Error) -> Self {
        ReadError::Io(e)
    }
}

/// Default coalescing threshold: when a record body is below this size,
/// merge the header and body into a single `write` call to cut IO.
pub const DEFAULT_JOIN_WRITES_THRESHOLD: usize = 100_000;

/// Stateful container-format-1 writer. Wraps any [`std::io::Write`] and
/// tracks the byte offset so callers can build a (offset, length) memo for
/// random-access reads.
pub struct ContainerWriter<W: std::io::Write> {
    out: W,
    /// Records below this byte length merge their header and body into one
    /// write; larger records issue separate writes per chunk.
    pub join_writes_threshold: usize,
    /// Bytes written so far, including the header line.
    pub current_offset: u64,
    /// Number of bytes records added (excludes begin/end framing).
    pub records_written: u64,
}

impl<W: std::io::Write> ContainerWriter<W> {
    pub fn new(out: W) -> Self {
        Self {
            out,
            join_writes_threshold: DEFAULT_JOIN_WRITES_THRESHOLD,
            current_offset: 0,
            records_written: 0,
        }
    }

    fn write(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        self.out.write_all(bytes)?;
        self.current_offset += bytes.len() as u64;
        Ok(())
    }

    /// Write the format header line.
    pub fn begin(&mut self) -> std::io::Result<()> {
        self.write(&begin())
    }

    /// Write the End marker.
    pub fn end(&mut self) -> std::io::Result<()> {
        self.write(end())
    }

    /// Append a Bytes record. Returns `(offset, length)` of the record
    /// within the container.
    pub fn add_bytes_record(
        &mut self,
        chunks: &[&[u8]],
        length: usize,
        names: &[Vec<Vec<u8>>],
    ) -> Result<(u64, u64), ReadError> {
        let start = self.current_offset;
        let header = bytes_header(length, names)?;
        if length < self.join_writes_threshold {
            // Merge header + body into a single write.
            let mut buf = Vec::with_capacity(header.len() + length);
            buf.extend_from_slice(&header);
            for chunk in chunks {
                buf.extend_from_slice(chunk);
            }
            self.write(&buf)?;
        } else {
            self.write(&header)?;
            for chunk in chunks {
                self.write(chunk)?;
            }
        }
        self.records_written += 1;
        Ok((start, self.current_offset - start))
    }

    /// Consume the writer and yield the underlying writer back.
    pub fn into_inner(self) -> W {
        self.out
    }
}

/// Read one `\n`-terminated line from `reader`. Returns the bytes without
/// the trailing newline. `Err(UnexpectedEof)` if the stream ends without a
/// newline. Distinguishes a clean EOF (no bytes consumed) by returning
/// `Ok(None)`.
fn read_line<R: std::io::BufRead>(reader: &mut R) -> Result<Option<Vec<u8>>, ReadError> {
    let mut buf = Vec::new();
    let n = reader.read_until(b'\n', &mut buf)?;
    if n == 0 {
        return Ok(None);
    }
    if buf.last() != Some(&b'\n') {
        return Err(ReadError::UnexpectedEof);
    }
    buf.pop();
    Ok(Some(buf))
}

/// Read exactly one byte. Returns `Ok(None)` at clean EOF.
fn read_byte<R: std::io::Read>(reader: &mut R) -> Result<Option<u8>, ReadError> {
    let mut buf = [0u8; 1];
    let n = reader.read(&mut buf)?;
    if n == 0 {
        Ok(None)
    } else {
        Ok(Some(buf[0]))
    }
}

/// Stream-based reader for a Bytes record. Decodes the prelude (length +
/// names) on construction; the body is then read incrementally via
/// [`read_content`](Self::read_content).
pub struct BytesRecordReader<'a, R: std::io::BufRead> {
    source: &'a mut R,
    names: Vec<Vec<Vec<u8>>>,
    remaining: usize,
}

impl<'a, R: std::io::BufRead> BytesRecordReader<'a, R> {
    /// Parse the prelude of a Bytes record from `source`.
    pub fn read_prelude(source: &'a mut R) -> Result<Self, ReadError> {
        // Length line.
        let line = read_line(source)?.ok_or(ReadError::UnexpectedEof)?;
        let s = std::str::from_utf8(&line)
            .map_err(|_| PackError::InvalidRecord(format!("{:?} is not a valid length.", line)))?;
        let length: usize = s
            .parse()
            .map_err(|_| PackError::InvalidRecord(format!("{:?} is not a valid length.", line)))?;

        // Name lines, terminated by a blank line.
        let mut names = Vec::new();
        loop {
            let name_line = read_line(source)?.ok_or(ReadError::UnexpectedEof)?;
            if name_line.is_empty() {
                break;
            }
            let parts: Vec<Vec<u8>> = name_line.split(|&b| b == 0).map(|p| p.to_vec()).collect();
            for part in &parts {
                check_name(part)?;
            }
            names.push(parts);
        }

        Ok(Self {
            source,
            names,
            remaining: length,
        })
    }

    pub fn names(&self) -> &[Vec<Vec<u8>>] {
        &self.names
    }

    /// Bytes left to read in the body.
    pub fn remaining(&self) -> usize {
        self.remaining
    }

    /// Read up to `max` bytes of body (or all remaining body if `None`).
    pub fn read_content(&mut self, max: Option<usize>) -> Result<Vec<u8>, ReadError> {
        let want = match max {
            Some(n) => n.min(self.remaining),
            None => self.remaining,
        };
        let mut buf = vec![0u8; want];
        self.source.read_exact(&mut buf).map_err(|e| {
            if e.kind() == std::io::ErrorKind::UnexpectedEof {
                ReadError::UnexpectedEof
            } else {
                ReadError::Io(e)
            }
        })?;
        self.remaining -= want;
        Ok(buf)
    }

    /// Drain the rest of the body (e.g. for `validate`).
    pub fn drain(&mut self) -> Result<(), ReadError> {
        let _ = self.read_content(None)?;
        Ok(())
    }

    /// Validate a record: re-checks names are valid UTF-8, then drains.
    pub fn validate(&mut self) -> Result<(), ReadError> {
        for name_tuple in &self.names {
            for name in name_tuple {
                check_name_encoding(name)?;
            }
        }
        self.drain()
    }
}

/// One entry from [`ContainerReader::iter_records`]: either a Bytes record
/// being delivered, or end-of-container.
pub enum RecordKind<'a, R: std::io::BufRead> {
    Bytes(BytesRecordReader<'a, R>),
    End,
}

/// Stream-based container reader. Reads the format header, then records.
pub struct ContainerReader<R: std::io::BufRead> {
    source: R,
    format_read: bool,
}

impl<R: std::io::BufRead> ContainerReader<R> {
    pub fn new(source: R) -> Self {
        Self {
            source,
            format_read: false,
        }
    }

    /// Validate and consume the format header line.
    pub fn read_format(&mut self) -> Result<(), ReadError> {
        let line = read_line(&mut self.source)?.ok_or(ReadError::UnexpectedEof)?;
        if line != FORMAT_ONE {
            return Err(PackError::UnknownContainerFormat(line).into());
        }
        self.format_read = true;
        Ok(())
    }

    /// Read the next record (or end marker) from the stream. After
    /// `RecordKind::End` is returned, callers should stop iterating.
    /// `RecordKind::Bytes` borrows the reader exclusively until it is
    /// dropped — Rust's borrow checker enforces the "don't use the record
    /// after advancing the iterator" rule that the Python doc warns about.
    pub fn next_record(&mut self) -> Result<RecordKind<'_, R>, ReadError> {
        if !self.format_read {
            self.read_format()?;
        }
        match read_byte(&mut self.source)? {
            None => Err(ReadError::UnexpectedEof),
            Some(b'B') => {
                let r = BytesRecordReader::read_prelude(&mut self.source)?;
                Ok(RecordKind::Bytes(r))
            }
            Some(b'E') => Ok(RecordKind::End),
            Some(other) => Err(PackError::UnknownRecordType(other).into()),
        }
    }

    /// Validate the entire container: every name must decode as UTF-8, all
    /// name tuples must be unique, and there must be no trailing data.
    pub fn validate(&mut self) -> Result<(), ReadError> {
        let mut seen: std::collections::HashSet<Vec<Vec<u8>>> = std::collections::HashSet::new();
        loop {
            match self.next_record()? {
                RecordKind::End => break,
                RecordKind::Bytes(mut r) => {
                    for name_tuple in r.names() {
                        for name in name_tuple {
                            check_name_encoding(name)?;
                        }
                        if !seen.insert(name_tuple.clone()) {
                            return Err(ReadError::DuplicateName(name_tuple.clone()));
                        }
                    }
                    r.drain()?;
                }
            }
        }
        let mut tail = [0u8; 1];
        match self.source.read(&mut tail)? {
            0 => Ok(()),
            _ => Err(ReadError::ExcessData(tail.to_vec())),
        }
    }

    /// Read every record into memory. Convenience for callers that want
    /// the contents up front.
    pub fn read_all(&mut self) -> Result<Vec<Record>, ReadError> {
        let mut out = Vec::new();
        loop {
            match self.next_record()? {
                RecordKind::End => return Ok(out),
                RecordKind::Bytes(mut r) => {
                    let names = r.names().to_vec();
                    let body = r.read_content(None)?;
                    out.push((names, body));
                }
            }
        }
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

    #[test]
    fn check_name_encoding_accepts_ascii_and_utf8() {
        assert!(check_name_encoding(b"abc").is_ok());
        assert!(check_name_encoding("\u{e9}clair".as_bytes()).is_ok());
    }

    #[test]
    fn check_name_encoding_rejects_invalid_utf8() {
        assert!(check_name_encoding(b"\xcc").is_err());
    }

    #[test]
    fn writer_emits_format_header_on_begin() {
        let mut buf = Vec::new();
        let mut w = ContainerWriter::new(&mut buf);
        w.begin().unwrap();
        assert_eq!(buf, b"Bazaar pack format 1 (introduced in 0.18)\n");
    }

    #[test]
    fn writer_records_offsets_and_increments_count() {
        let mut buf = Vec::new();
        let mut w = ContainerWriter::new(&mut buf);
        w.begin().unwrap();
        let memo = w
            .add_bytes_record(&[b"abc"], 3, &[name(&[b"name1"])])
            .unwrap();
        // Header line is 42 bytes including newline; record body starts there.
        assert_eq!(memo, (42, 13));
        assert_eq!(w.records_written, 1);
        // Second record's offset starts where the first ended.
        let memo2 = w.add_bytes_record(&[b"abc"], 3, &[]).unwrap();
        assert_eq!(memo2.0, 42 + 13);
    }

    #[test]
    fn writer_split_writes_when_above_threshold() {
        // Record larger than the threshold writes header+chunks separately.
        struct Chunked(Vec<Vec<u8>>);
        impl std::io::Write for Chunked {
            fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                self.0.push(b.to_vec());
                Ok(b.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut sink = Chunked(Vec::new());
        {
            let mut w = ContainerWriter::new(&mut sink);
            w.join_writes_threshold = 2;
            w.begin().unwrap();
            w.add_bytes_record(&[b"abcabc"], 6, &[name(&[b"name1"])])
                .unwrap();
        }
        // Three writes: format header, record header, record body.
        assert_eq!(sink.0.len(), 3);
        assert_eq!(sink.0[0], b"Bazaar pack format 1 (introduced in 0.18)\n");
        assert_eq!(sink.0[1], b"B6\nname1\n\n");
        assert_eq!(sink.0[2], b"abcabc");
    }

    #[test]
    fn writer_rejects_invalid_name() {
        let mut buf = Vec::new();
        let mut w = ContainerWriter::new(&mut buf);
        w.begin().unwrap();
        let err = w
            .add_bytes_record(&[b"abc"], 3, &[name(&[b"bad name"])])
            .unwrap_err();
        match err {
            ReadError::Pack(PackError::InvalidName(_)) => {}
            other => panic!("expected InvalidName, got {:?}", other),
        }
    }

    #[test]
    fn reader_empty_container_validates() {
        let data = make_container(&[]);
        let mut r = ContainerReader::new(std::io::Cursor::new(data));
        r.validate().unwrap();
    }

    #[test]
    fn reader_single_record_round_trips() {
        let data = make_container(&[(&[&[b"name"]], b"body")]);
        let mut r = ContainerReader::new(std::io::Cursor::new(data));
        let records = r.read_all().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].0, vec![vec![b"name".to_vec()]]);
        assert_eq!(records[0].1, b"body");
    }

    #[test]
    fn reader_validate_rejects_duplicate_names() {
        let data = make_container(&[(&[&[b"n"]], b""), (&[&[b"n"]], b"")]);
        let mut r = ContainerReader::new(std::io::Cursor::new(data));
        match r.validate() {
            Err(ReadError::DuplicateName(_)) => {}
            other => panic!("expected DuplicateName, got {:?}", other),
        }
    }

    #[test]
    fn reader_validate_rejects_excess_data() {
        let mut data = make_container(&[]);
        data.extend_from_slice(b"crud");
        let mut r = ContainerReader::new(std::io::Cursor::new(data));
        match r.validate() {
            Err(ReadError::ExcessData(_)) => {}
            other => panic!("expected ExcessData, got {:?}", other),
        }
    }

    #[test]
    fn reader_validate_rejects_bad_format() {
        let mut r = ContainerReader::new(std::io::Cursor::new(b"unknown format\n".to_vec()));
        match r.validate() {
            Err(ReadError::Pack(PackError::UnknownContainerFormat(_))) => {}
            other => panic!("expected UnknownContainerFormat, got {:?}", other),
        }
    }

    #[test]
    fn reader_validate_rejects_undecodable_name() {
        let data = b"Bazaar pack format 1 (introduced in 0.18)\nB0\n\xcc\n\nE".to_vec();
        let mut r = ContainerReader::new(std::io::Cursor::new(data));
        match r.validate() {
            Err(ReadError::Pack(PackError::InvalidRecord(_))) => {}
            other => panic!("expected InvalidRecord, got {:?}", other),
        }
    }

    #[test]
    fn bytes_record_reader_max_length() {
        let mut data: &[u8] = b"6\n\nabcdef";
        let mut r = BytesRecordReader::read_prelude(&mut data).unwrap();
        assert_eq!(r.read_content(Some(3)).unwrap(), b"abc");
        assert_eq!(r.read_content(Some(3)).unwrap(), b"def");
        // Past the end: no more bytes.
        assert_eq!(r.read_content(Some(99)).unwrap(), b"");
    }

    #[test]
    fn bytes_record_reader_invalid_length_errors() {
        let mut data: &[u8] = b"not a number\n";
        match BytesRecordReader::read_prelude(&mut data) {
            Err(ReadError::Pack(PackError::InvalidRecord(_))) => {}
            Err(other) => panic!("expected InvalidRecord, got {:?}", other),
            Ok(_) => panic!("expected error"),
        }
    }

    #[test]
    fn bytes_record_reader_eof_during_name() {
        let mut data: &[u8] = b"123\nname";
        match BytesRecordReader::read_prelude(&mut data) {
            Err(ReadError::UnexpectedEof) => {}
            Err(other) => panic!("expected UnexpectedEof, got {:?}", other),
            Ok(_) => panic!("expected error"),
        }
    }
}
