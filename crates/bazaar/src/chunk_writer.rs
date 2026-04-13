//! Fixed-size compressed chunk writer.
//!
//! Port of `bzrformats.chunk_writer.ChunkWriter`. The writer accumulates
//! arbitrary byte slices and flushes them to a target chunk size, using zlib
//! `Z_SYNC_FLUSH` and full repacks to push as much content as possible into
//! the page. When the next slice would overflow, [`ChunkWriter::write`]
//! returns `true` and remembers the slice as `unused_bytes` so the caller can
//! retry it on a fresh writer.

use flate2::{Compress, Compression, FlushCompress, Status};

pub const REPACK_OPTS_FOR_SPEED: (u32, u32) = (0, 8);
pub const REPACK_OPTS_FOR_SIZE: (u32, u32) = (20, 0);

/// Result returned by [`ChunkWriter::finish`].
#[derive(Debug)]
pub struct FinishedChunk {
    /// The list of compressed byte chunks. The last one is a padding run of
    /// `0x00` bytes if the compressed output was shorter than `chunk_size`.
    pub bytes_list: Vec<Vec<u8>>,
    /// The bytes that did not fit, if `write` returned `true`.
    pub unused_bytes: Option<Vec<u8>>,
    /// Number of `0x00` padding bytes added at the end of `bytes_list`.
    pub nulls_needed: usize,
}

pub struct ChunkWriter {
    chunk_size: usize,
    reserved_size: usize,
    compressor: Compress,
    /// Raw input bytes accepted into the current compressor. Used to repack
    /// the whole stream from scratch when `Z_SYNC_FLUSH` doesn't pack tightly
    /// enough.
    bytes_in: Vec<Vec<u8>>,
    /// Compressed output bytes accumulated so far for the *current*
    /// compressor.
    bytes_list: Vec<Vec<u8>>,
    bytes_out_len: usize,
    /// Total input bytes since the last `Z_SYNC_FLUSH`.
    unflushed_in_bytes: usize,
    num_repack: u32,
    num_zsync: u32,
    unused_bytes: Option<Vec<u8>>,
    max_repack: u32,
    max_zsync: u32,
}

impl ChunkWriter {
    /// Construct a writer targeting `chunk_size` total bytes.
    ///
    /// `reserved` carves out a tail region inside `chunk_size` that can only
    /// be written via `write(_, reserved=true)`. `optimize_for_size = true`
    /// switches to the slower but tighter packing strategy used by Python's
    /// `_repack_opts_for_size`.
    pub fn new(chunk_size: usize, reserved: usize, optimize_for_size: bool) -> Self {
        let (max_repack, max_zsync) = if optimize_for_size {
            REPACK_OPTS_FOR_SIZE
        } else {
            REPACK_OPTS_FOR_SPEED
        };
        Self {
            chunk_size,
            reserved_size: reserved,
            compressor: Compress::new(Compression::default(), true),
            bytes_in: Vec::new(),
            bytes_list: Vec::new(),
            bytes_out_len: 0,
            unflushed_in_bytes: 0,
            num_repack: 0,
            num_zsync: 0,
            unused_bytes: None,
            max_repack,
            max_zsync,
        }
    }

    pub fn max_repack(&self) -> u32 {
        self.max_repack
    }

    pub fn max_zsync(&self) -> u32 {
        self.max_zsync
    }

    /// Switch between the speed/size repack tunables.
    pub fn set_optimize(&mut self, for_size: bool) {
        let (max_repack, max_zsync) = if for_size {
            REPACK_OPTS_FOR_SIZE
        } else {
            REPACK_OPTS_FOR_SPEED
        };
        self.max_repack = max_repack;
        self.max_zsync = max_zsync;
    }

    /// Drain `Z_FINISH` and pad to `chunk_size`.
    pub fn finish(mut self) -> FinishedChunk {
        self.bytes_in.clear();
        let mut tail = Vec::with_capacity(64);
        loop {
            let out_before = tail.len();
            let in_before = self.compressor.total_in();
            let _ = self
                .compressor
                .compress_vec(&[], &mut tail, FlushCompress::Finish);
            let in_after = self.compressor.total_in();
            let out_after = tail.len();
            if in_after == in_before && out_after == out_before {
                break;
            }
            if tail.len() == tail.capacity() {
                tail.reserve(64);
            }
        }
        if !tail.is_empty() {
            self.bytes_out_len += tail.len();
            self.bytes_list.push(tail);
        }
        assert!(
            self.bytes_out_len <= self.chunk_size,
            "Somehow we ended up with too much compressed data, {} > {}",
            self.bytes_out_len,
            self.chunk_size
        );
        let nulls_needed = self.chunk_size - self.bytes_out_len;
        if nulls_needed > 0 {
            self.bytes_list.push(vec![0u8; nulls_needed]);
        }
        FinishedChunk {
            bytes_list: self.bytes_list,
            unused_bytes: self.unused_bytes,
            nulls_needed,
        }
    }

    /// Try to append `bytes` to the current chunk.
    ///
    /// Returns `true` if the bytes could not fit; the caller should treat the
    /// page as full and start a new one. Setting `reserved` to `true` lets the
    /// caller tap the tail region carved out at construction time.
    pub fn write(&mut self, bytes: &[u8], reserved: bool) -> bool {
        if self.num_repack > self.max_repack && !reserved {
            self.unused_bytes = Some(bytes.to_vec());
            return true;
        }
        let capacity = if reserved {
            self.chunk_size
        } else {
            self.chunk_size.saturating_sub(self.reserved_size)
        };

        let next_unflushed = self.unflushed_in_bytes + bytes.len();
        let remaining_capacity = capacity.saturating_sub(self.bytes_out_len + 10);
        if next_unflushed < remaining_capacity {
            // Looks like it'll fit.
            let out = compress_chunk(&mut self.compressor, bytes, FlushCompress::None);
            if !out.is_empty() {
                self.bytes_out_len += out.len();
                self.bytes_list.push(out);
            }
            self.bytes_in.push(bytes.to_vec());
            self.unflushed_in_bytes += bytes.len();
            return false;
        }

        // Try Z_SYNC_FLUSH.
        self.num_zsync += 1;
        if self.max_repack == 0 && self.num_zsync > self.max_zsync {
            self.num_repack += 1;
            self.unused_bytes = Some(bytes.to_vec());
            return true;
        }
        let out = compress_chunk(&mut self.compressor, bytes, FlushCompress::Sync);
        self.unflushed_in_bytes = 0;
        if !out.is_empty() {
            self.bytes_out_len += out.len();
            self.bytes_list.push(out);
        }
        let safety_margin = if self.num_repack == 0 { 100 } else { 10 };
        if self.bytes_out_len + safety_margin <= capacity {
            self.bytes_in.push(bytes.to_vec());
            return false;
        }

        // Over budget: try a full repack including the new bytes.
        self.num_repack += 1;
        let mut bytes_in_extended = self.bytes_in.clone();
        bytes_in_extended.push(bytes.to_vec());
        let (out_chunks_with_extra, out_len_with_extra, compressor_with_extra) =
            recompress_all_bytes_in(&bytes_in_extended, true);
        let new_out_len = out_len_with_extra;
        if self.num_repack >= self.max_repack {
            // Match the Python behaviour: bump us *past* `_max_repack` so the
            // next call short-circuits.
            self.num_repack += 1;
        }
        if new_out_len + 10 > capacity {
            // Even fully repacked it doesn't fit. Repack without the extra
            // bytes and stash the new bytes as `unused`.
            let (out_chunks, out_len, compressor) = recompress_all_bytes_in(&self.bytes_in, false);
            self.compressor = compressor;
            // Force any further writes to short-circuit.
            self.num_repack = self.max_repack + 1;
            self.bytes_list = out_chunks;
            self.bytes_out_len = out_len;
            self.unused_bytes = Some(bytes.to_vec());
            true
        } else {
            // It fits when packed tighter; commit the new packing.
            self.compressor = compressor_with_extra;
            self.bytes_in.push(bytes.to_vec());
            self.bytes_list = out_chunks_with_extra;
            self.bytes_out_len = new_out_len;
            false
        }
    }
}

fn compress_chunk(comp: &mut Compress, input: &[u8], flush: FlushCompress) -> Vec<u8> {
    // Use a scratch buffer and the lower-level `compress` (not `compress_vec`)
    // so we explicitly control output capacity. This mirrors what CPython's
    // zlib module does for compressobj.compress / compressobj.flush.
    let mut out: Vec<u8> = Vec::new();
    let mut scratch = vec![0u8; 65536];
    let mut consumed = 0;
    let mut guard = 0usize;
    // Step 1: push all of `input` with no flush.
    while consumed < input.len() {
        guard += 1;
        assert!(guard < 10_000, "compress_chunk input loop runaway");
        let in_before = comp.total_in();
        let out_before = comp.total_out();
        comp.compress(&input[consumed..], &mut scratch, FlushCompress::None)
            .expect("zlib compression failed");
        let in_advance = (comp.total_in() - in_before) as usize;
        let out_advance = (comp.total_out() - out_before) as usize;
        if out_advance > 0 {
            out.extend_from_slice(&scratch[..out_advance]);
        }
        consumed += in_advance;
        if in_advance == 0 && out_advance == 0 {
            scratch.resize(scratch.len() * 2, 0);
        }
    }
    if matches!(flush, FlushCompress::None) {
        return out;
    }
    // Step 2: call the flush exactly once (both Z_SYNC_FLUSH and Z_FINISH
    // are single-shot operations that emit all remaining data at once).
    // Grow scratch to accommodate everything in a single call.
    loop {
        if scratch.len() < 16 * 1024 {
            scratch.resize(16 * 1024, 0);
        }
        let out_before = comp.total_out();
        let status = comp
            .compress(&[], &mut scratch, flush)
            .expect("zlib flush failed");
        let out_advance = (comp.total_out() - out_before) as usize;
        if out_advance > 0 {
            out.extend_from_slice(&scratch[..out_advance]);
        }
        match status {
            Status::Ok => break,
            Status::StreamEnd => break,
            Status::BufError => {
                // Buffer was too small; grow and retry.
                scratch.resize(scratch.len() * 2, 0);
                continue;
            }
        }
    }
    out
}

fn recompress_all_bytes_in(
    bytes_in: &[Vec<u8>],
    sync_flush_extra: bool,
) -> (Vec<Vec<u8>>, usize, Compress) {
    let mut compressor = Compress::new(Compression::default(), true);
    let mut out_chunks: Vec<Vec<u8>> = Vec::new();
    if sync_flush_extra {
        // The last chunk gets Z_SYNC_FLUSH so its data is committed to the
        // output (matching the Python `_recompress_all_bytes_in(extra_bytes)`
        // behaviour).
        if let Some((last, head)) = bytes_in.split_last() {
            for chunk in head {
                let out = compress_chunk(&mut compressor, chunk, FlushCompress::None);
                if !out.is_empty() {
                    out_chunks.push(out);
                }
            }
            let out = compress_chunk(&mut compressor, last, FlushCompress::Sync);
            if !out.is_empty() {
                out_chunks.push(out);
            }
        }
    } else {
        for chunk in bytes_in {
            let out = compress_chunk(&mut compressor, chunk, FlushCompress::None);
            if !out.is_empty() {
                out_chunks.push(out);
            }
        }
    }
    let out_len: usize = out_chunks.iter().map(|c| c.len()).sum();
    (out_chunks, out_len, compressor)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::read::ZlibDecoder;
    use std::io::Read;

    fn decompress(data: &[u8]) -> Vec<u8> {
        let mut decoder = ZlibDecoder::new(data);
        let mut out = Vec::new();
        decoder.read_to_end(&mut out).unwrap();
        out
    }

    fn check_chunk(bytes_list: &[Vec<u8>], size: usize) -> Vec<u8> {
        let data: Vec<u8> = bytes_list.iter().flatten().copied().collect();
        assert_eq!(data.len(), size);
        decompress(&data)
    }

    #[test]
    fn empty_chunk_only_zlib_header() {
        let writer = ChunkWriter::new(4096, 0, false);
        let finished = writer.finish();
        let payload = check_chunk(&finished.bytes_list, 4096);
        assert!(payload.is_empty());
        assert_eq!(finished.unused_bytes, None);
    }

    #[test]
    fn optimize_for_speed_uses_speed_opts() {
        let mut writer = ChunkWriter::new(4096, 0, false);
        writer.set_optimize(false);
        assert_eq!(
            (writer.max_repack(), writer.max_zsync()),
            REPACK_OPTS_FOR_SPEED
        );
        let writer2 = ChunkWriter::new(4096, 0, false);
        assert_eq!(
            (writer2.max_repack(), writer2.max_zsync()),
            REPACK_OPTS_FOR_SPEED
        );
    }

    #[test]
    fn optimize_for_size_uses_size_opts() {
        let mut writer = ChunkWriter::new(4096, 0, false);
        writer.set_optimize(true);
        assert_eq!(
            (writer.max_repack(), writer.max_zsync()),
            REPACK_OPTS_FOR_SIZE
        );
        let writer2 = ChunkWriter::new(4096, 0, true);
        assert_eq!(
            (writer2.max_repack(), writer2.max_zsync()),
            REPACK_OPTS_FOR_SIZE
        );
    }

    #[test]
    fn some_data_round_trips() {
        let mut writer = ChunkWriter::new(4096, 0, false);
        assert!(!writer.write(b"foo bar baz quux\n", false));
        let finished = writer.finish();
        let payload = check_chunk(&finished.bytes_list, 4096);
        assert_eq!(payload, b"foo bar baz quux\n");
        assert_eq!(finished.unused_bytes, None);
    }

    fn make_lines() -> Vec<Vec<u8>> {
        let mut lines = Vec::new();
        for group in 0..48 {
            let offset = group * 50;
            let mut line = Vec::new();
            for n in offset..offset + 50 {
                line.extend_from_slice(format!("{}", n).as_bytes());
            }
            line.push(b'\n');
            lines.push(line);
        }
        lines
    }

    #[test]
    fn finish_pads_to_exact_size_when_partial() {
        // ChunkWriter::finish() must always produce chunks totalling
        // exactly `chunk_size` (the tail of nulls makes up the difference).
        let mut writer = ChunkWriter::new(3996, 0, false);
        assert!(!writer.write(b"hello world\n", false));
        let finished = writer.finish();
        let total: usize = finished.bytes_list.iter().map(|b| b.len()).sum();
        assert_eq!(total, 3996);
    }

    #[test]
    fn too_much_data_does_not_exceed_size() {
        let lines = make_lines();
        let mut writer = ChunkWriter::new(4096, 0, false);
        let mut last_idx = None;
        for (idx, line) in lines.iter().enumerate() {
            if writer.write(line, false) {
                last_idx = Some(idx);
                break;
            }
        }
        let stop_idx = last_idx.expect("should have stopped");
        let finished = writer.finish();
        let payload = check_chunk(&finished.bytes_list, 4096);
        let expected: Vec<u8> = lines[..stop_idx].iter().flatten().copied().collect();
        assert_eq!(payload, expected);
        assert_eq!(
            finished.unused_bytes.as_deref(),
            Some(lines[stop_idx].as_slice())
        );
    }

    #[test]
    fn too_much_data_preserves_reserve_space() {
        let lines = make_lines();
        let mut writer = ChunkWriter::new(4096, 256, false);
        let mut stop_idx = None;
        for (idx, line) in lines.iter().enumerate() {
            if writer.write(line, false) {
                stop_idx = Some(idx);
                break;
            }
        }
        let stop_idx = stop_idx.expect("should have stopped");
        // Reserved write should always succeed (256 bytes).
        let reserved_blob = vec![b'A'; 256];
        assert!(!writer.write(&reserved_blob, true));
        let finished = writer.finish();
        let payload = check_chunk(&finished.bytes_list, 4096);
        let mut expected: Vec<u8> = lines[..stop_idx].iter().flatten().copied().collect();
        expected.extend_from_slice(&reserved_blob);
        assert_eq!(payload, expected);
        assert_eq!(
            finished.unused_bytes.as_deref(),
            Some(lines[stop_idx].as_slice())
        );
    }
}
