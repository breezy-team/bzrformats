//! Bazaar's hand-rolled gzip writer.
//!
//! Knit storage uses a stripped-down gzip framing that omits the original
//! filename and mtime, fixes XFL=2 (max compression marker) and OS=255
//! (unknown). The deflate stream is written with default compression.
//! See `bzrformats.tuned_gzip.chunks_to_gzip` for the Python original.

use crc32fast::Hasher;
use flate2::{write::DeflateEncoder, Compression};
use std::io::Write;

const GZIP_HEADER: [u8; 10] = [
    0x1f, 0x8b, // magic
    0x08, // method = deflate
    0x00, // flags
    0x00, 0x00, 0x00, 0x00, // mtime
    0x02, // XFL = max compression
    0xff, // OS = unknown
];

/// Encode `chunks` as a gzip stream, returning the resulting byte chunks.
///
/// The header chunk, deflate output and 8-byte trailer are returned as
/// separate `Vec<u8>` entries so callers can write them without an extra
/// concatenation.
pub fn chunks_to_gzip<I, C>(chunks: I) -> Vec<Vec<u8>>
where
    I: IntoIterator<Item = C>,
    C: AsRef<[u8]>,
{
    let mut out: Vec<Vec<u8>> = vec![GZIP_HEADER.to_vec()];

    let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
    let mut hasher = Hasher::new();
    let mut total_len: u64 = 0;
    for chunk in chunks {
        let bytes = chunk.as_ref();
        hasher.update(bytes);
        total_len = total_len.wrapping_add(bytes.len() as u64);
        encoder
            .write_all(bytes)
            .expect("in-memory write cannot fail");
    }
    let deflated = encoder.finish().expect("in-memory finish cannot fail");
    if !deflated.is_empty() {
        out.push(deflated);
    }

    let crc = hasher.finalize();
    let isize_low = (total_len & 0xffff_ffff) as u32;
    let mut trailer = Vec::with_capacity(8);
    trailer.extend_from_slice(&crc.to_le_bytes());
    trailer.extend_from_slice(&isize_low.to_le_bytes());
    out.push(trailer);

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::read::GzDecoder;
    use std::io::Read;

    fn roundtrip(chunks: &[&[u8]]) {
        let raw: Vec<u8> = chunks.iter().flat_map(|c| c.iter().copied()).collect();
        let gz_chunks = chunks_to_gzip(chunks.iter().copied());
        let gz: Vec<u8> = gz_chunks.into_iter().flatten().collect();
        let mut decoder = GzDecoder::new(gz.as_slice());
        let mut decoded = Vec::new();
        decoder.read_to_end(&mut decoded).unwrap();
        assert_eq!(decoded, raw);
    }

    #[test]
    fn single_chunk() {
        roundtrip(&[b"a modest chunk\nwith some various\nbits\n"]);
    }

    #[test]
    fn many_chunks() {
        roundtrip(&[b"some\n", b"strings\n", b"to\n", b"process\n"]);
    }

    #[test]
    fn empty_input() {
        roundtrip(&[]);
    }

    #[test]
    fn header_matches_python_layout() {
        let gz_chunks = chunks_to_gzip(std::iter::empty::<&[u8]>());
        assert_eq!(gz_chunks[0], GZIP_HEADER.to_vec());
    }
}
