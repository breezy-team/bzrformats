//! Outer wire framing for groupcompress blocks.
//!
//! The over-the-wire form prepends a small text header with one record per
//! contained factory, then the inner [`GroupCompressBlock`] payload. This
//! module parses just the header so the manager-side Python wrapper can keep
//! orchestrating the block construction itself.
//!
//! See `bzrformats.groupcompress._LazyGroupContentManager.from_bytes` for the
//! Python original.

use flate2::read::ZlibDecoder;
use std::io::Read;

/// One factory record described by the wire header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireFactory {
    /// `\x00`-separated key segments.
    pub key: Vec<Vec<u8>>,
    /// `None` for absent parent info, else a list of keys.
    pub parents: Option<Vec<Vec<Vec<u8>>>>,
    pub start: u64,
    pub end: u64,
}

#[derive(Debug)]
pub enum Error {
    UnknownStorageKind(Vec<u8>),
    InvalidLength(&'static str),
    InvalidInteger,
    MissingTrailingNewline,
    NotMultipleOfFour,
    Decompress(std::io::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::UnknownStorageKind(b) => {
                write!(f, "unknown storage kind: {}", String::from_utf8_lossy(b))
            }
            Error::InvalidLength(msg) => write!(f, "invalid length: {}", msg),
            Error::InvalidInteger => write!(f, "invalid integer in wire header"),
            Error::MissingTrailingNewline => {
                write!(f, "header lines did not end with a trailing newline")
            }
            Error::NotMultipleOfFour => {
                write!(f, "header was not an even multiple of 4 lines")
            }
            Error::Decompress(e) => write!(f, "zlib decompression failed: {}", e),
        }
    }
}

impl std::error::Error for Error {}

/// Parsed result of the outer wire frame: the factory records plus the byte
/// range of the inner `GroupCompressBlock` payload within the input slice.
#[derive(Debug)]
pub struct WireFrame<'a> {
    pub factories: Vec<WireFactory>,
    /// Slice of the original input that contains the inner block payload.
    pub block_bytes: &'a [u8],
}

/// Parse the outer wire framing of a groupcompress block.
///
/// `bytes` is the full record as written by `_wire_bytes` on the Python side.
pub fn parse_wire(bytes: &[u8]) -> Result<WireFrame<'_>, Error> {
    // The Python original splits on `\n` with a 4-element limit, yielding
    // `(storage_kind, z_header_len, header_len, block_len, rest)`.
    let mut splits = bytes.splitn(5, |&b| b == b'\n');
    let storage_kind = splits.next().ok_or(Error::InvalidLength("storage kind"))?;
    let z_header_len = splits.next().ok_or(Error::InvalidLength("z_header_len"))?;
    let header_len = splits.next().ok_or(Error::InvalidLength("header_len"))?;
    let block_len = splits.next().ok_or(Error::InvalidLength("block_len"))?;
    let rest = splits.next().ok_or(Error::InvalidLength("rest"))?;

    if storage_kind != b"groupcompress-block" {
        return Err(Error::UnknownStorageKind(storage_kind.to_vec()));
    }

    let z_header_len = parse_int(z_header_len)? as usize;
    let header_len = parse_int(header_len)? as usize;
    let block_len = parse_int(block_len)? as usize;

    if rest.len() < z_header_len {
        return Err(Error::InvalidLength("compressed header shorter than rest"));
    }
    let z_header = &rest[..z_header_len];
    let block_bytes = &rest[z_header_len..];
    if block_bytes.len() != block_len {
        return Err(Error::InvalidLength("block bytes length mismatch"));
    }

    let mut header = Vec::with_capacity(header_len);
    ZlibDecoder::new(z_header)
        .read_to_end(&mut header)
        .map_err(Error::Decompress)?;
    if header.len() != header_len {
        return Err(Error::InvalidLength("decompressed header length mismatch"));
    }

    let factories = parse_header_lines(&header)?;
    Ok(WireFrame {
        factories,
        block_bytes,
    })
}

fn parse_int(b: &[u8]) -> Result<u64, Error> {
    std::str::from_utf8(b)
        .map_err(|_| Error::InvalidInteger)?
        .parse()
        .map_err(|_| Error::InvalidInteger)
}

fn parse_header_lines(header: &[u8]) -> Result<Vec<WireFactory>, Error> {
    // Header is a sequence of lines, each terminated by `\n`. The Python code
    // splits on `\n`, expects an empty trailing element, then walks groups of
    // four lines: key, parents, start, end.
    let mut lines: Vec<&[u8]> = header.split(|&b| b == b'\n').collect();
    let trailing = lines.pop().ok_or(Error::MissingTrailingNewline)?;
    if !trailing.is_empty() {
        return Err(Error::MissingTrailingNewline);
    }
    if lines.len() % 4 != 0 {
        return Err(Error::NotMultipleOfFour);
    }
    let mut out = Vec::with_capacity(lines.len() / 4);
    for chunk in lines.chunks_exact(4) {
        let key = chunk[0]
            .split(|&b| b == b'\x00')
            .map(|s| s.to_vec())
            .collect();
        let parents = if chunk[1] == b"None:" {
            None
        } else {
            Some(
                chunk[1]
                    .split(|&b| b == b'\t')
                    .filter(|seg| !seg.is_empty())
                    .map(|seg| {
                        seg.split(|&b| b == b'\x00')
                            .map(|s| s.to_vec())
                            .collect::<Vec<Vec<u8>>>()
                    })
                    .collect(),
            )
        };
        let start = parse_int(chunk[2])?;
        let end = parse_int(chunk[3])?;
        out.push(WireFactory {
            key,
            parents,
            start,
            end,
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::ZlibEncoder;
    use flate2::Compression;
    use std::io::Write;

    fn build_wire(header_lines: &[u8], block_bytes: &[u8]) -> Vec<u8> {
        let mut z = ZlibEncoder::new(Vec::new(), Compression::default());
        z.write_all(header_lines).unwrap();
        let z_header = z.finish().unwrap();
        let mut out = Vec::new();
        out.extend_from_slice(b"groupcompress-block\n");
        out.extend_from_slice(format!("{}\n", z_header.len()).as_bytes());
        out.extend_from_slice(format!("{}\n", header_lines.len()).as_bytes());
        out.extend_from_slice(format!("{}\n", block_bytes.len()).as_bytes());
        out.extend_from_slice(&z_header);
        out.extend_from_slice(block_bytes);
        out
    }

    #[test]
    fn round_trip_single_factory() {
        let header = b"file-id\x00rev\nNone:\n0\n42\n";
        let block = b"BLOCK_PAYLOAD";
        let wire = build_wire(header, block);
        let frame = parse_wire(&wire).unwrap();
        assert_eq!(frame.block_bytes, block);
        assert_eq!(frame.factories.len(), 1);
        let f = &frame.factories[0];
        assert_eq!(f.key, vec![b"file-id".to_vec(), b"rev".to_vec()]);
        assert!(f.parents.is_none());
        assert_eq!(f.start, 0);
        assert_eq!(f.end, 42);
    }

    #[test]
    fn parents_split_on_tab_and_nul() {
        let header = b"k\nf\x00p1\tf\x00p2\n0\n10\n";
        let wire = build_wire(header, b"");
        let frame = parse_wire(&wire).unwrap();
        let parents = frame.factories[0].parents.as_ref().unwrap();
        assert_eq!(
            *parents,
            vec![
                vec![b"f".to_vec(), b"p1".to_vec()],
                vec![b"f".to_vec(), b"p2".to_vec()],
            ]
        );
    }

    #[test]
    fn empty_parents_list_is_some_empty() {
        // Python emits `b""` for an empty parents tuple, which then splits on
        // `\t` into a single empty segment that the filter drops.
        let header = b"k\n\n0\n10\n";
        let wire = build_wire(header, b"");
        let frame = parse_wire(&wire).unwrap();
        assert_eq!(frame.factories[0].parents.as_ref().unwrap().len(), 0);
    }

    #[test]
    fn rejects_unknown_storage_kind() {
        let bytes = b"something-else\n0\n0\n0\n";
        assert!(matches!(
            parse_wire(bytes),
            Err(Error::UnknownStorageKind(_))
        ));
    }

    #[test]
    fn rejects_non_multiple_of_four_header_lines() {
        let header = b"a\nb\nc\n";
        let wire = build_wire(header, b"");
        assert!(matches!(parse_wire(&wire), Err(Error::NotMultipleOfFour)));
    }
}
