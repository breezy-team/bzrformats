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
use flate2::write::ZlibEncoder;
use flate2::Compression;
use std::io::{Read, Write};

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

/// Build the per-factory header bytes consumed by the wire format.
///
/// Each factory contributes four `\n`-terminated lines: the `\x00`-joined key,
/// the parents (`b"None:"` for absent parents, otherwise tab-separated keys),
/// the start byte and the end byte.
pub fn build_header_lines(factories: &[WireFactory]) -> Vec<u8> {
    let mut out = Vec::new();
    for factory in factories {
        // key
        let mut first = true;
        for segment in &factory.key {
            if !first {
                out.push(b'\x00');
            }
            first = false;
            out.extend_from_slice(segment);
        }
        out.push(b'\n');
        // parents
        match &factory.parents {
            None => out.extend_from_slice(b"None:"),
            Some(parents) => {
                let mut first_parent = true;
                for parent in parents {
                    if !first_parent {
                        out.push(b'\t');
                    }
                    first_parent = false;
                    let mut first_seg = true;
                    for seg in parent {
                        if !first_seg {
                            out.push(b'\x00');
                        }
                        first_seg = false;
                        out.extend_from_slice(seg);
                    }
                }
            }
        }
        out.push(b'\n');
        // start
        out.extend_from_slice(format!("{}", factory.start).as_bytes());
        out.push(b'\n');
        // end
        out.extend_from_slice(format!("{}", factory.end).as_bytes());
        out.push(b'\n');
    }
    out
}

/// Build the framing prefix for the wire format: the storage-kind line, the
/// three length lines, and the zlib-compressed header bytes.
///
/// The caller appends the inner block payload (`block_bytes`) after the
/// returned prefix to form the complete wire record.
pub fn build_wire_prefix(
    factories: &[WireFactory],
    block_bytes_len: usize,
) -> std::io::Result<Vec<u8>> {
    let header = build_header_lines(factories);
    let header_len = header.len();
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&header)?;
    let z_header = encoder.finish()?;
    let z_header_len = z_header.len();

    let mut prefix = Vec::with_capacity(64 + z_header_len);
    prefix.extend_from_slice(b"groupcompress-block\n");
    prefix.extend_from_slice(
        format!("{}\n{}\n{}\n", z_header_len, header_len, block_bytes_len).as_bytes(),
    );
    prefix.extend_from_slice(&z_header);
    Ok(prefix)
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

    #[test]
    fn build_header_lines_emits_python_format() {
        let factories = vec![WireFactory {
            key: vec![b"file-id".to_vec(), b"rev".to_vec()],
            parents: None,
            start: 0,
            end: 42,
        }];
        assert_eq!(
            build_header_lines(&factories),
            b"file-id\x00rev\nNone:\n0\n42\n"
        );
    }

    #[test]
    fn build_header_lines_emits_multiple_parents_with_tab_separator() {
        let factories = vec![WireFactory {
            key: vec![b"k".to_vec()],
            parents: Some(vec![
                vec![b"f".to_vec(), b"p1".to_vec()],
                vec![b"f".to_vec(), b"p2".to_vec()],
            ]),
            start: 1,
            end: 2,
        }];
        assert_eq!(
            build_header_lines(&factories),
            b"k\nf\x00p1\tf\x00p2\n1\n2\n"
        );
    }

    #[test]
    fn build_header_lines_empty_parents_list_emits_empty_line() {
        // A `Some(vec![])` round-trips through the parser because the empty
        // segment from splitting `b""` on `\t` is filtered out.
        let factories = vec![WireFactory {
            key: vec![b"k".to_vec()],
            parents: Some(vec![]),
            start: 0,
            end: 1,
        }];
        assert_eq!(build_header_lines(&factories), b"k\n\n0\n1\n");
    }

    #[test]
    fn build_wire_prefix_round_trips_via_parse_wire() {
        let factories = vec![
            WireFactory {
                key: vec![b"file-a".to_vec(), b"rev1".to_vec()],
                parents: None,
                start: 0,
                end: 32,
            },
            WireFactory {
                key: vec![b"file-b".to_vec(), b"rev2".to_vec()],
                parents: Some(vec![vec![b"file-a".to_vec(), b"rev1".to_vec()]]),
                start: 32,
                end: 96,
            },
        ];
        let block = b"BLOCK_PAYLOAD";
        let mut wire = build_wire_prefix(&factories, block.len()).unwrap();
        wire.extend_from_slice(block);

        let frame = parse_wire(&wire).unwrap();
        assert_eq!(frame.block_bytes, block);
        assert_eq!(frame.factories, factories);
    }
}
