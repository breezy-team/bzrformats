use crate::groupcompress::delta::{apply_delta, read_base128_int, read_instruction, Instruction};
use byteorder::ReadBytesExt;
use std::borrow::Cow;
use std::io::BufRead;
use std::io::{Read, Write};

/// Group Compress Block v1 Zlib
const GCB_HEADER: &[u8] = b"gcb1z\n";

/// Group Compress Block v1 Lzma
const GCB_LZ_HEADER: &[u8] = b"gcb1l\n";

#[derive(Debug, PartialEq, Eq, Default, Clone, Copy)]
pub enum CompressorKind {
    #[default]
    Zlib,
    Lzma,
}

#[cfg(feature = "pyo3")]
impl<'a, 'py> pyo3::FromPyObject<'a, 'py> for CompressorKind {
    type Error = pyo3::PyErr;

    fn extract(ob: pyo3::Borrowed<'a, 'py, pyo3::PyAny>) -> pyo3::PyResult<Self> {
        let s: Cow<str> = ob.extract()?;
        match s.as_ref() {
            "zlib" => Ok(CompressorKind::Zlib),
            "lzma" => Ok(CompressorKind::Lzma),
            _ => Err(pyo3::exceptions::PyValueError::new_err(format!(
                "Unknown compressor: {}",
                s
            ))),
        }
    }
}

impl CompressorKind {
    fn header(&self) -> &'static [u8] {
        match self {
            CompressorKind::Zlib => GCB_HEADER,
            CompressorKind::Lzma => GCB_LZ_HEADER,
        }
    }

    fn from_header(header: &[u8]) -> Option<Self> {
        if header == GCB_HEADER {
            Some(CompressorKind::Zlib)
        } else if header == GCB_LZ_HEADER {
            Some(CompressorKind::Lzma)
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub enum Error {
    InvalidData(String),
    Io(std::io::Error),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<super::delta::DeltaError> for Error {
    fn from(e: super::delta::DeltaError) -> Self {
        match e {
            super::delta::DeltaError::Io { kind, ref message } => {
                Error::Io(std::io::Error::new(kind, message.clone()))
            }
            other => Error::InvalidData(other.to_string()),
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Error::InvalidData(ref s) => write!(f, "Invalid data: {}", s),
            Error::Io(ref e) => write!(f, "IO error: {}", e),
        }
    }
}

impl std::error::Error for Error {}

pub enum GroupCompressItem {
    Fulltext(Vec<u8>),
    Delta(Vec<u8>),
}

pub fn read_item<R: Read>(r: &mut R) -> Result<GroupCompressItem, Error> {
    // The bytes are 'f' or 'd' for the type, then a variable-length
    // base128 integer for the content size, then the actual content
    // We know that the variable-length integer won't be longer than 5
    // bytes (it takes 5 bytes to encode 2^32)
    let c = r.read_u8()?;
    let content_len = read_base128_int(r).map_err(|e| Error::InvalidData(e.to_string()))?;

    let mut text = vec![0; content_len as usize];
    r.read_exact(&mut text)?;
    match c {
        b'f' => {
            // Fulltext
            Ok(GroupCompressItem::Fulltext(text))
        }
        b'd' => {
            // Must be type delta as checked above
            Ok(GroupCompressItem::Delta(text))
        }
        c => Err(Error::InvalidData(format!(
            "Unknown content control code: {:?}",
            c
        ))),
    }
}

/// Concrete streaming decompressor for a [`GroupCompressBlock`]. Using an
/// enum (rather than `Box<dyn Read>`) keeps the owning struct `Send + Sync`
/// so it can live inside a pyo3 `#[pyclass]` without the `unsendable` marker.
enum Decompressor {
    Lzma(xz2::read::XzDecoder<osutils::chunkreader::ChunksReader<Vec<u8>>>),
    Zlib(flate2::read::ZlibDecoder<osutils::chunkreader::ChunksReader<Vec<u8>>>),
}

impl std::io::Read for Decompressor {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Decompressor::Lzma(d) => d.read(buf),
            Decompressor::Zlib(d) => d.read(buf),
        }
    }
}

/// An object which maintains the internal structure of the compressed data.
///
/// This tracks the meta info (start of text, length, type, etc.)
pub struct GroupCompressBlock {
    /// The name of the compressor used to compress the content
    compressor: Option<CompressorKind>,
    /// The compressed content
    z_content_chunks: Option<Vec<Vec<u8>>>,
    /// The decompressor object
    z_content_decompressor: Option<Decompressor>,
    /// The length of the compressed content
    z_content_length: Option<usize>,
    /// The length of the uncompressed content
    content_length: Option<usize>,
    /// The uncompressed content
    content: Option<Vec<u8>>,
    /// The uncompressed content, split into chunks
    content_chunks: Option<Vec<Vec<u8>>>,
}

impl Default for GroupCompressBlock {
    fn default() -> Self {
        Self::new()
    }
}

fn read_header<R: Read>(r: &mut R) -> Result<CompressorKind, Error> {
    let mut header = [0; 6];
    r.read_exact(&mut header).map_err(|e| {
        Error::InvalidData(format!(
            "Failed to read header from GroupCompressBlock: {}",
            e
        ))
    })?;
    CompressorKind::from_header(&header).ok_or_else(|| {
        Error::InvalidData(format!(
            "Invalid header in GroupCompressBlock: {:?}",
            header
        ))
    })
}

impl GroupCompressBlock {
    pub fn new() -> Self {
        // map by key? or just order in file?
        Self {
            compressor: None,
            z_content_chunks: None,
            z_content_decompressor: None,
            z_content_length: None,
            content_length: None,
            content: None,
            content_chunks: None,
        }
    }

    pub fn content(&self) -> Option<&[u8]> {
        self.content.as_deref()
    }

    pub fn content_length(&self) -> Option<usize> {
        self.content_length
    }

    pub fn z_content_length(&self) -> Option<usize> {
        self.z_content_length
    }

    /// Whether a streaming decompressor is currently attached. Mirrors the
    /// Python class's `_z_content_decompressor is not None` probe; there is
    /// no way to inspect the decompressor directly, only its presence.
    pub fn has_z_content_decompressor(&self) -> bool {
        self.z_content_decompressor.is_some()
    }

    pub fn compressor(&self) -> Option<CompressorKind> {
        self.compressor
    }

    /// Replace the compressor kind. Clears the content cache so the next
    /// `ensure_content` call rebuilds via the right decoder.
    pub fn set_compressor(&mut self, kind: CompressorKind) {
        self.compressor = Some(kind);
        self.content = None;
        self.z_content_decompressor = None;
    }

    /// Replace the compressed-content chunks wholesale. The caller is
    /// responsible for also calling `set_z_content_length` and
    /// `set_compressor` so the block can decompress the bytes later.
    pub fn set_z_content_chunks(&mut self, chunks: Vec<Vec<u8>>) {
        self.z_content_chunks = Some(chunks);
        self.content = None;
        self.z_content_decompressor = None;
    }

    pub fn set_z_content_length(&mut self, length: usize) {
        self.z_content_length = Some(length);
    }

    pub fn set_content_length(&mut self, length: usize) {
        self.content_length = Some(length);
    }

    /// Make sure that content has been expanded enough.
    ///
    /// # Arguments
    /// * `num_bytes` - Ensure that we have extracted at least num_bytes of content. If None, consume everything
    pub fn ensure_content(&mut self, num_bytes: Option<usize>) -> Result<(), Error> {
        assert!(
            self.content_length.is_some(),
            "self.content_length should never be None"
        );
        let mut num_bytes = match num_bytes {
            None => self.content_length.unwrap(),
            Some(num_bytes) => {
                assert!(
                    num_bytes <= self.content_length.unwrap(),
                    "requested num_bytes ({}) > content length ({})",
                    num_bytes,
                    self.content_length.unwrap()
                );
                num_bytes
            }
        };

        // Expand the content if required
        if self.content.is_none() {
            if let Some(content_chunks) = self.content_chunks.as_ref() {
                self.content = Some(content_chunks.concat());
                self.content_chunks = None;
            }
        }
        if self.content.is_none() {
            // We join self.z_content_chunks here, because if we are
            // decompressing, then it is *very* likely that we have a single
            // chunk
            if self.z_content_length == Some(0) {
                self.content = Some(b"".to_vec());
            } else {
                let c = osutils::chunkreader::ChunksReader::new(Box::new(
                    self.z_content_chunks.clone().unwrap().into_iter(),
                ));
                self.z_content_decompressor = Some(match self.compressor.unwrap() {
                    CompressorKind::Lzma => Decompressor::Lzma(xz2::read::XzDecoder::new(c)),
                    CompressorKind::Zlib => Decompressor::Zlib(flate2::read::ZlibDecoder::new(c)),
                });
                self.content = Some(Vec::new());
            }
        }

        if self.content.as_ref().unwrap().len() >= num_bytes {
            // Already decompressed enough. If we're actually at the end of
            // the content, drop the streaming decompressor so it can be
            // garbage-collected.
            if self.content.as_ref().unwrap().len() >= self.content_length.unwrap_or(0) {
                self.z_content_decompressor = None;
            }
            return Ok(());
        }

        num_bytes -= self.content.as_ref().unwrap().len();

        let mut buf = vec![0; num_bytes];
        self.z_content_decompressor
            .as_mut()
            .unwrap()
            .read_exact(&mut buf)?;
        self.content.as_mut().unwrap().extend(buf);

        // If we've now pulled out the whole thing, drop the streaming
        // decompressor — Python asserts `_z_content_decompressor is None`
        // after full content has been drained.
        if self.content.as_ref().unwrap().len() >= self.content_length.unwrap_or(0) {
            self.z_content_decompressor = None;
        }
        Ok(())
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        // This is the maximum number of bytes this object will reference if
        // everything is decompressed. However, if we decompress less than
        // everything... (this would cause some problems for LRUSizeCache)
        //
        // Either field may be `None` on a freshly-constructed block or after
        // set_content before to_chunks has been called — treat those as 0
        // rather than panicking, matching the Python class.
        self.content_length.unwrap_or(0) + self.z_content_length.unwrap_or(0)
    }

    pub fn parse_bytes(&mut self, mut data: &[u8]) -> Result<(), Error> {
        self.read_bytes(&mut data)
    }

    /// Read the various lengths from the header.
    ///
    /// This also populates the various 'compressed' buffers.
    fn read_bytes<R: Read>(&mut self, r: &mut R) -> Result<(), Error> {
        // At present, we have 2 integers for the compressed and uncompressed
        // content. In base10 (ascii) 14 bytes can represent > 1TB, so to avoid
        // checking too far, cap the search to 14 bytes.
        let mut buf = std::io::BufReader::new(r);
        let mut z_content_length_buf = Vec::new();
        buf.read_until(b'\n', &mut z_content_length_buf)?;
        // Chop off the '\n'
        z_content_length_buf.pop();
        self.z_content_length = Some(
            String::from_utf8(z_content_length_buf)
                .unwrap()
                .parse()
                .unwrap(),
        );
        let mut content_length_buf = Vec::new();
        buf.read_until(b'\n', &mut content_length_buf)?;
        content_length_buf.pop();
        self.content_length = Some(
            String::from_utf8(content_length_buf)
                .unwrap()
                .parse()
                .unwrap(),
        );
        let mut data = Vec::new();
        buf.read_to_end(&mut data)?;
        // XXX: Define some GCCorrupt error ?
        assert_eq!(
            data.len(),
            self.z_content_length.unwrap(),
            "Invalid bytes: ({}) != {}",
            data.len(),
            self.z_content_length.unwrap()
        );
        self.z_content_chunks = Some(vec![data.to_vec()]);
        Ok(())
    }

    /// Return z_content_chunks as a simple string.
    ///
    /// Meant only to be used by the test suite.
    pub fn z_content(&mut self) -> Vec<u8> {
        self.z_content_chunks.as_ref().unwrap().concat()
    }

    pub fn z_content_chunks(&mut self) -> &mut Vec<Vec<u8>> {
        self.z_content_chunks.as_mut().unwrap()
    }

    pub fn from_bytes<R: Read>(mut r: R) -> Result<Self, Error> {
        let compressor = read_header(&mut r)?;
        let mut out = Self {
            compressor: Some(compressor),
            z_content_chunks: None,
            content: None,
            content_chunks: None,
            z_content_length: None,
            content_length: None,
            z_content_decompressor: None,
        };
        out.read_bytes(&mut r)?;
        Ok(out)
    }

    /// Extract the text for a record stored at `content[start..end]`.
    ///
    /// Fulltext records are returned directly. Delta records are applied
    /// against the whole block content as the basis, matching the format's
    /// "delta against preceding records in this group" semantics.
    pub fn extract(&mut self, start: usize, end: usize) -> Result<Vec<Vec<u8>>, Error> {
        if start == 0 && end == 0 {
            return Ok(vec![]);
        }
        self.ensure_content(Some(end))?;

        let content = self.content.as_ref().unwrap();
        if end > content.len() || start >= end {
            return Err(Error::InvalidData(format!(
                "extract range {}..{} out of bounds for content of length {}",
                start,
                end,
                content.len()
            )));
        }
        // Read the type byte and base-128 length starting at `start`, not 0.
        let mut record = &content[start..end];
        match read_item(&mut record)? {
            GroupCompressItem::Fulltext(data) => Ok(vec![data]),
            GroupCompressItem::Delta(delta) => {
                let reconstructed = apply_delta(content, delta.as_slice())?;
                Ok(vec![reconstructed])
            }
        }
    }

    /// Set the content of this block to the given chunks.
    pub fn set_chunked_content(&mut self, content_chunks: &[Vec<u8>], length: usize) {
        // If we have lots of short lines, it is may be more efficient to join
        // the content ahead of time. If the content is <10MiB, we don't really
        // care about the extra memory consumption, so we can just pack it and
        // be done. However, timing showed 18s => 17.9s for repacking 1k revs of
        // mysql, which is below the noise margin
        self.content_length = Some(length);
        self.content_chunks = Some(content_chunks.to_vec());
        self.content = None;
        self.z_content_chunks = None;
    }

    /// Set the content of this block.
    pub fn set_content(&mut self, content: &[u8]) {
        self.content_length = Some(content.len());
        self.content = Some(content.to_vec());
        self.z_content_chunks = None;
    }

    fn create_z_content_from_chunks(
        &mut self,
        chunks: Vec<Vec<u8>>,
        compressor_kind: CompressorKind,
    ) {
        let chunks = match compressor_kind {
            CompressorKind::Zlib => {
                let mut encoder =
                    flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
                for chunk in chunks {
                    encoder.write_all(&chunk).unwrap();
                }
                encoder.finish().unwrap()
            }
            CompressorKind::Lzma => {
                let mut encoder = xz2::write::XzEncoder::new(Vec::new(), 6);
                for chunk in chunks {
                    encoder.write_all(&chunk).unwrap();
                }
                encoder.finish().unwrap()
            }
        };
        self.z_content_length = Some(chunks.len());
        self.z_content_chunks = Some(vec![chunks]);
    }

    fn create_z_content(&mut self, compressor_kind: CompressorKind) {
        if self.z_content_chunks.is_some() && self.compressor == Some(compressor_kind) {
            return;
        }
        let chunks = if let Some(content_chunks) = self.content_chunks.as_ref() {
            content_chunks.to_vec()
        } else {
            vec![self.content.as_ref().unwrap().clone()]
        };
        self.create_z_content_from_chunks(chunks, compressor_kind);
    }

    /// Create the byte stream as a series of 'chunks'.
    ///
    /// The first chunk is the magic header concatenated with the two
    /// base-10 length lines — the Python test suite asserts this as a
    /// single fixed-size chunk because there is "no compelling reason to
    /// split it up". The remaining chunks are the compressed payload.
    pub fn to_chunks(
        &mut self,
        compressor_kind: Option<CompressorKind>,
    ) -> (usize, Vec<Cow<'_, [u8]>>) {
        let compressor_kind = compressor_kind.unwrap_or_default();
        self.create_z_content(compressor_kind);

        let mut header_chunk = compressor_kind.header().to_vec();
        header_chunk.extend_from_slice(
            format!(
                "{}\n{}\n",
                self.z_content_length.unwrap(),
                self.content_length.unwrap()
            )
            .as_bytes(),
        );

        let mut chunks: Vec<Cow<'_, [u8]>> = vec![Cow::Owned(header_chunk)];
        chunks.extend(
            self.z_content_chunks
                .as_ref()
                .unwrap()
                .iter()
                .map(|x| Cow::Borrowed(x.as_slice())),
        );
        let total_len = chunks.iter().map(|x| x.len()).sum();
        (total_len, chunks)
    }

    /// Encode the information into a byte stream.
    pub fn to_bytes(&mut self) -> Vec<u8> {
        let (_total_len, chunks) = self.to_chunks(None);
        chunks.concat()
    }

    /// Take this block, and spit out a human-readable structure.
    ///
    /// # Arguments
    /// * `include_text`: when `true`, fulltext records carry their payload
    ///   and delta inserts/copies carry the matched bytes.
    ///
    /// # Returns
    /// A dump of the given block. The layout matches the historical
    /// Python `_dump` format: a list of `DumpInfo::Fulltext { length, text }`
    /// or `DumpInfo::Delta { delta_length, decomp_length, instructions }`,
    /// where each `DeltaInfo` is `Copy { offset, length, text }` or
    /// `Insert { length, text }`.
    pub fn dump(&mut self, include_text: Option<bool>) -> Result<Vec<DumpInfo>, Error> {
        let include_text = include_text.unwrap_or(false);
        self.ensure_content(None)?;
        let mut result = vec![];
        let mut content = self.content.as_ref().unwrap().as_slice();
        while !content.is_empty() {
            match read_item(&mut content)? {
                GroupCompressItem::Fulltext(text) => {
                    let length = text.len();
                    result.push(DumpInfo::Fulltext {
                        length,
                        text: if include_text { Some(text) } else { None },
                    });
                }
                GroupCompressItem::Delta(delta_content) => {
                    let delta_length = delta_content.len();
                    let mut delta_info = vec![];
                    // The first entry in a delta is the decompressed length.
                    let mut delta_slice = delta_content.as_slice();
                    let decomp_len = read_base128_int(&mut delta_slice).unwrap();
                    let mut measured_len = 0;
                    while !delta_slice.is_empty() {
                        match read_instruction(&mut delta_slice)? {
                            Instruction::Insert(text) => {
                                measured_len += text.len();
                                delta_info.push(DeltaInfo::Insert {
                                    length: text.len(),
                                    text: if include_text { Some(text) } else { None },
                                });
                            }
                            Instruction::r#Copy { offset, length } => {
                                delta_info.push(DeltaInfo::Copy {
                                    offset,
                                    length,
                                    text: if include_text {
                                        Some(
                                            self.content.as_ref().unwrap()[offset..offset + length]
                                                .to_vec(),
                                        )
                                    } else {
                                        None
                                    },
                                });
                                measured_len += length;
                            }
                        }
                    }
                    if measured_len != decomp_len as usize {
                        return Err(Error::InvalidData(format!(
                            "Delta claimed fulltext was {} bytes, but extraction resulted in {}",
                            decomp_len, measured_len
                        )));
                    }
                    result.push(DumpInfo::Delta {
                        delta_length,
                        decomp_length: decomp_len as usize,
                        instructions: delta_info,
                    });
                }
            }
        }

        Ok(result)
    }
}

pub enum DeltaInfo {
    Insert {
        length: usize,
        text: Option<Vec<u8>>,
    },
    Copy {
        offset: usize,
        length: usize,
        text: Option<Vec<u8>>,
    },
}

pub enum DumpInfo {
    Fulltext {
        length: usize,
        text: Option<Vec<u8>>,
    },
    Delta {
        delta_length: usize,
        decomp_length: usize,
        instructions: Vec<DeltaInfo>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::groupcompress::delta::write_base128_int;

    /// Build a valid "fulltext" record payload as it would be stored inside
    /// the block's content stream: `b"f"` + base128 length + raw bytes.
    fn make_fulltext_record(body: &[u8]) -> Vec<u8> {
        let mut out = vec![b'f'];
        write_base128_int(&mut out, body.len() as u128).unwrap();
        out.extend_from_slice(body);
        out
    }

    #[test]
    fn compressor_kind_header_round_trip() {
        assert_eq!(
            CompressorKind::from_header(GCB_HEADER),
            Some(CompressorKind::Zlib)
        );
        assert_eq!(
            CompressorKind::from_header(GCB_LZ_HEADER),
            Some(CompressorKind::Lzma)
        );
        assert_eq!(CompressorKind::from_header(b"xxxxxx"), None);
        assert_eq!(CompressorKind::Zlib.header(), GCB_HEADER);
        assert_eq!(CompressorKind::Lzma.header(), GCB_LZ_HEADER);
    }

    #[test]
    fn new_block_has_no_content() {
        let b = GroupCompressBlock::new();
        assert!(b.content().is_none());
        assert!(b.content_length().is_none());
    }

    #[test]
    fn new_block_len_is_zero() {
        // A freshly constructed block must report length 0 without panicking
        // — content_length and z_content_length are both None at this point.
        let b = GroupCompressBlock::new();
        assert_eq!(b.len(), 0);
    }

    #[test]
    fn manually_initialised_block_decompresses() {
        // Mirror the Python test pattern that sets z_content_chunks,
        // z_content_length, compressor, and content_length directly, then
        // calls ensure_content. This exercises the setter path used by the
        // PyO3 bindings.
        use flate2::write::ZlibEncoder;
        use std::io::Write;

        let body: Vec<u8> = b"partial decomp target content\n".repeat(50);
        let mut encoder = ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&body).unwrap();
        let z_content = encoder.finish().unwrap();

        let mut b = GroupCompressBlock::new();
        b.set_z_content_chunks(vec![z_content.clone()]);
        b.set_z_content_length(z_content.len());
        b.set_compressor(CompressorKind::Zlib);
        b.set_content_length(body.len());

        // Content is not populated yet.
        assert!(b.content().is_none());
        // Partial decompression reveals at least the requested bytes.
        b.ensure_content(Some(100));
        assert!(b.content().unwrap().len() >= 100);
        assert_eq!(&b.content().unwrap()[..100], &body[..100]);
        // Full decompression recovers the whole body.
        b.ensure_content(None);
        assert_eq!(b.content(), Some(body.as_slice()));
    }

    #[test]
    fn len_after_set_content_reports_content_length() {
        let body = b"abc\n";
        let mut b = GroupCompressBlock::new();
        b.set_content(body);
        // z_content_length is still None until to_bytes/to_chunks builds it,
        // so len() should at least not panic and should reflect the content.
        assert!(b.len() >= body.len());
    }

    #[test]
    fn set_content_round_trip_via_to_bytes_and_from_bytes() {
        let body = b"hello world\nthis is a single fulltext\n";
        let record = make_fulltext_record(body);

        let mut b = GroupCompressBlock::new();
        b.set_content(&record);
        let raw = b.to_bytes();

        // The serialized block should start with the zlib header by default.
        assert!(raw.starts_with(GCB_HEADER));

        // Reading it back should recover the same record payload.
        let mut parsed = GroupCompressBlock::from_bytes(raw.as_slice()).unwrap();
        assert_eq!(parsed.content_length(), Some(record.len()));
        parsed.ensure_content(None);
        assert_eq!(parsed.content(), Some(record.as_slice()));
    }

    #[test]
    fn set_chunked_content_matches_set_content_on_the_wire() {
        let part1: Vec<u8> = b"hello world\n".to_vec();
        let part2: Vec<u8> = b"more content\n".to_vec();
        let total_len = part1.len() + part2.len();

        let mut chunked = GroupCompressBlock::new();
        chunked.set_chunked_content(&[part1.clone(), part2.clone()], total_len);
        let chunked_bytes = chunked.to_bytes();

        let mut flat = GroupCompressBlock::new();
        let mut combined = part1.clone();
        combined.extend_from_slice(&part2);
        flat.set_content(&combined);
        let flat_bytes = flat.to_bytes();

        assert_eq!(chunked_bytes, flat_bytes);
    }

    #[test]
    fn ensure_content_full_decompression_recovers_original() {
        // A larger body so that streaming decompression is not a no-op.
        let body: Vec<u8> = b"line of reasonably compressible text\n".repeat(200);
        let record = make_fulltext_record(&body);

        let mut src = GroupCompressBlock::new();
        src.set_content(&record);
        let raw = src.to_bytes();

        let mut parsed = GroupCompressBlock::from_bytes(raw.as_slice()).unwrap();
        assert!(parsed.content().is_none());
        parsed.ensure_content(None);
        assert_eq!(parsed.content(), Some(record.as_slice()));
    }

    #[test]
    fn ensure_content_partial_then_full() {
        // Request fewer bytes than the full content, then request the rest.
        // After the second call, we must have the whole content and it must
        // match byte-for-byte.
        let body: Vec<u8> = b"compressible line\n".repeat(100);
        let record = make_fulltext_record(&body);

        let mut src = GroupCompressBlock::new();
        src.set_content(&record);
        let raw = src.to_bytes();

        let mut parsed = GroupCompressBlock::from_bytes(raw.as_slice()).unwrap();
        parsed.ensure_content(Some(50));
        assert!(parsed.content().unwrap().len() >= 50);
        parsed.ensure_content(None);
        assert_eq!(parsed.content(), Some(record.as_slice()));
    }

    #[test]
    fn to_chunks_produces_header_plus_lengths_chunk_then_payload() {
        // The first chunk is the magic header byte string followed by the
        // two base-10 length lines. The remaining chunks are the compressed
        // payload.
        let body = b"some body text\n";
        let record = make_fulltext_record(body);
        let mut b = GroupCompressBlock::new();
        b.set_content(&record);
        let (total_len, chunks) = b.to_chunks(None);

        assert!(chunks[0].starts_with(GCB_HEADER));
        let tail = std::str::from_utf8(&chunks[0][GCB_HEADER.len()..]).unwrap();
        let mut iter = tail.trim_end().split('\n');
        let z_len: usize = iter.next().unwrap().parse().unwrap();
        let u_len: usize = iter.next().unwrap().parse().unwrap();
        assert_eq!(u_len, record.len());
        let payload_len: usize = chunks[1..].iter().map(|c| c.len()).sum();
        assert_eq!(payload_len, z_len);
        assert_eq!(total_len, chunks.iter().map(|c| c.len()).sum::<usize>());
    }

    #[test]
    fn dump_reports_fulltext_records_without_text_by_default() {
        let body_a = b"first body\n";
        let body_b = b"second body\n";
        let mut content = make_fulltext_record(body_a);
        content.extend(make_fulltext_record(body_b));

        let mut b = GroupCompressBlock::new();
        b.set_content(&content);
        let dump = b.dump(None).unwrap();
        assert_eq!(dump.len(), 2);
        assert!(matches!(dump[0], DumpInfo::Fulltext { text: None, .. }));
        assert!(matches!(dump[1], DumpInfo::Fulltext { text: None, .. }));
    }

    #[test]
    fn ensure_content_drops_decompressor_on_full_drain() {
        // Once all bytes have been pulled through the streaming decompressor
        // we drop it — mirroring the Python test assertion
        // `assertIs(None, block._z_content_decompressor)`.
        let body: Vec<u8> = b"ensurable content here\n".repeat(100);
        let record = make_fulltext_record(&body);

        let mut src = GroupCompressBlock::new();
        src.set_content(&record);
        let raw = src.to_bytes();

        let mut parsed = GroupCompressBlock::from_bytes(raw.as_slice()).unwrap();
        parsed.ensure_content(Some(50));
        assert!(parsed.has_z_content_decompressor());
        parsed.ensure_content(None);
        assert!(!parsed.has_z_content_decompressor());
    }

    #[test]
    fn ensure_content_is_idempotent() {
        // Calling ensure_content twice with the same limit must be a no-op
        // on the second call — the early-return path when content.len() is
        // already at the requested size.
        let body: Vec<u8> = b"some compressible content\n".repeat(200);
        let record = make_fulltext_record(&body);

        let mut src = GroupCompressBlock::new();
        src.set_content(&record);
        let raw = src.to_bytes();

        let mut parsed = GroupCompressBlock::from_bytes(raw.as_slice()).unwrap();
        parsed.ensure_content(None);
        let first = parsed.content().unwrap().to_vec();
        parsed.ensure_content(None);
        assert_eq!(parsed.content().unwrap(), first.as_slice());

        // And a partial request below the current length is likewise a no-op.
        parsed.ensure_content(Some(10));
        assert_eq!(parsed.content().unwrap(), first.as_slice());
    }

    #[test]
    fn extract_reads_record_at_given_start_offset() {
        // Two fulltext records back-to-back. Extracting the second must read
        // from its actual start offset in the decompressed content, not from
        // byte 0.
        let body_a = b"first body\n";
        let body_b = b"second body\n";
        let rec_a = make_fulltext_record(body_a);
        let rec_b = make_fulltext_record(body_b);
        let mut content = rec_a.clone();
        content.extend_from_slice(&rec_b);

        let mut b = GroupCompressBlock::new();
        b.set_content(&content);

        // Extract record A from its byte range.
        let start_a = 0;
        let end_a = rec_a.len();
        let out_a = b.extract(start_a, end_a).unwrap();
        assert_eq!(out_a, vec![body_a.to_vec()]);

        // Extract record B from its byte range — this is the one that
        // exercises the offset-aware path.
        let start_b = rec_a.len();
        let end_b = content.len();
        let out_b = b.extract(start_b, end_b).unwrap();
        assert_eq!(out_b, vec![body_b.to_vec()]);
    }

    #[test]
    fn dump_with_include_text_returns_payload() {
        let body = b"included body\n";
        let content = make_fulltext_record(body);

        let mut b = GroupCompressBlock::new();
        b.set_content(&content);
        let dump = b.dump(Some(true)).unwrap();
        assert_eq!(dump.len(), 1);
        match &dump[0] {
            DumpInfo::Fulltext {
                text: Some(text), ..
            } => assert_eq!(text.as_slice(), body),
            _ => panic!("expected Fulltext with text"),
        }
    }

    #[test]
    fn dump_reports_delta_records_with_instructions() {
        // Build a real fulltext+delta pair by driving a RabinGroupCompressor,
        // push the result into a block, and exercise dump() on the delta.
        use crate::groupcompress::compressor::{GroupCompressor, RabinGroupCompressor};
        use crate::versionedfile::Key;

        let mut gc = RabinGroupCompressor::new(None);
        let base = b"shared content that is long enough for rabin matching\nmore shared\n";
        let derived = b"shared content that is long enough for rabin matching\nmore shared\nplus\n";
        gc.compress(
            &Key::Fixed(vec![b"base".to_vec()]),
            &[base.as_slice()],
            base.len(),
            None,
            None,
            None,
        )
        .unwrap();
        gc.compress(
            &Key::Fixed(vec![b"derived".to_vec()]),
            &[derived.as_slice()],
            derived.len(),
            None,
            None,
            None,
        )
        .unwrap();
        let (chunks, endpoint) = gc.flush();

        let mut b = GroupCompressBlock::new();
        b.set_chunked_content(&chunks, endpoint);
        let dump = b.dump(None).unwrap();
        assert_eq!(dump.len(), 2);
        match &dump[0] {
            DumpInfo::Fulltext { length, text: None } => assert_eq!(*length, base.len()),
            _ => panic!(
                "expected Fulltext(None) for first record, got {:?}",
                match_kind(&dump[0])
            ),
        }
        match &dump[1] {
            DumpInfo::Delta {
                decomp_length,
                instructions,
                ..
            } => {
                assert_eq!(*decomp_length, derived.len());
                assert!(!instructions.is_empty());
                // At least one Copy for the shared prefix, and at least one
                // Insert for the "plus\n" tail.
                let (copies, inserts): (usize, usize) =
                    instructions.iter().fold((0, 0), |(c, i), inst| match inst {
                        DeltaInfo::Copy { .. } => (c + 1, i),
                        DeltaInfo::Insert { .. } => (c, i + 1),
                    });
                assert!(copies >= 1, "delta should contain at least one copy");
                assert!(inserts >= 1, "delta should contain at least one insert");
            }
            _ => panic!("expected Delta for second record"),
        }
    }

    fn match_kind(info: &DumpInfo) -> &'static str {
        match info {
            DumpInfo::Fulltext { .. } => "Fulltext",
            DumpInfo::Delta { .. } => "Delta",
        }
    }

    #[test]
    fn z_content_length_reflects_setter() {
        let mut b = GroupCompressBlock::new();
        assert_eq!(b.z_content_length(), None);
        b.set_z_content_length(1234);
        assert_eq!(b.z_content_length(), Some(1234));
    }

    #[test]
    fn compressor_getter_and_setter_round_trip() {
        let mut b = GroupCompressBlock::new();
        assert_eq!(b.compressor(), None);
        b.set_compressor(CompressorKind::Zlib);
        assert_eq!(b.compressor(), Some(CompressorKind::Zlib));
        b.set_compressor(CompressorKind::Lzma);
        assert_eq!(b.compressor(), Some(CompressorKind::Lzma));
    }

    #[test]
    fn set_z_content_chunks_clears_cached_content() {
        // Setting new compressed chunks must invalidate any previously
        // decompressed content — otherwise stale content would leak across
        // a re-initialisation.
        use flate2::write::ZlibEncoder;
        use std::io::Write;

        let body: Vec<u8> = b"the original body bytes\n".repeat(20);
        let rec = make_fulltext_record(&body);
        let mut src = GroupCompressBlock::new();
        src.set_content(&rec);
        src.to_bytes(); // force z_content population

        let mut parsed = GroupCompressBlock::from_bytes(src.to_bytes().as_slice()).unwrap();
        parsed.ensure_content(None);
        assert!(parsed.content().is_some());

        // Now rebuild a fresh z_content for a different body and plug it in
        // via the low-level setters. set_z_content_chunks must clear the
        // cached content so the next ensure_content produces the new body.
        let replacement_body: Vec<u8> = b"replacement body bytes\n".repeat(20);
        let replacement_record = make_fulltext_record(&replacement_body);
        let mut encoder = ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&replacement_record).unwrap();
        let z_replacement = encoder.finish().unwrap();

        parsed.set_compressor(CompressorKind::Zlib);
        parsed.set_z_content_chunks(vec![z_replacement.clone()]);
        parsed.set_z_content_length(z_replacement.len());
        parsed.set_content_length(replacement_record.len());

        assert!(
            parsed.content().is_none(),
            "cached content should be cleared"
        );
        parsed.ensure_content(None);
        assert_eq!(parsed.content(), Some(replacement_record.as_slice()));
    }

    #[test]
    fn lzma_round_trip_via_to_chunks_from_bytes() {
        // Build a block, serialise with the Lzma compressor, and round-trip
        // through from_bytes. Exercises the xz2 encode/decode path for
        // CompressorKind::Lzma.
        let body: Vec<u8> = b"a bit of compressible lzma-bound text\n".repeat(40);
        let record = make_fulltext_record(&body);

        let mut src = GroupCompressBlock::new();
        src.set_content(&record);
        let (_total_len, chunks) = src.to_chunks(Some(CompressorKind::Lzma));
        let bytes: Vec<u8> = chunks.iter().flat_map(|c| c.iter().copied()).collect();
        assert!(bytes.starts_with(GCB_LZ_HEADER));

        let mut parsed = GroupCompressBlock::from_bytes(bytes.as_slice()).unwrap();
        assert_eq!(parsed.compressor(), Some(CompressorKind::Lzma));
        parsed.ensure_content(None);
        assert_eq!(parsed.content(), Some(record.as_slice()));
    }
}
