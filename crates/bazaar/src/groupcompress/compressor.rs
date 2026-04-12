use crate::groupcompress::block::{read_item, GroupCompressItem};
use crate::groupcompress::delta::{apply_delta, read_base128_int, write_base128_int};
use crate::groupcompress::rabin_delta::OwningDeltaIndex;
use crate::groupcompress::NULL_SHA1;
use crate::versionedfile::{Error, Key};
use std::borrow::Cow;
use std::collections::HashMap;

pub trait GroupCompressor {
    /// Compress lines with label key.
    ///
    /// # Arguments
    /// * `key`: A key tuple. It is stored in the output
    ///     for identification of the text during decompression. If the last
    ///     element is b'None' it is replaced with the sha1 of the text -
    ///     e.g. sha1:xxxxxxx.
    /// * `chunks`: Chunks of bytes to be compressed
    /// * `length`: Length of chunks
    /// * `expected_sha`: If non-None, the sha the lines are believed to
    ///     have. During compression the sha is calculated; a mismatch will
    ///     cause an error.
    /// * `nostore_sha`: If the computed sha1 sum matches, we will raise
    ///     ExistingContent rather than adding the text.
    /// * `soft`: Do a 'soft' compression. This means that we require larger
    ///     ranges to match to be considered for a copy command.
    ///
    /// # Returns
    /// The sha1 of lines, the start and end offsets in the delta, and the type ('fulltext' or
    /// 'delta').
    fn compress(
        &mut self,
        key: &Key,
        chunks: &[&[u8]],
        length: usize,
        expected_sha: Option<String>,
        nostore_sha: Option<String>,
        soft: Option<bool>,
    ) -> Result<(String, usize, usize, &'static str), Error> {
        if length == 0 {
            // empty, like a dir entry, etc
            if nostore_sha == Some(String::from_utf8_lossy(NULL_SHA1.as_slice()).to_string()) {
                return Err(Error::ExistingContent(key.clone()));
            }
            return Ok((
                String::from_utf8_lossy(NULL_SHA1.as_slice()).to_string(),
                0,
                0,
                "fulltext",
            ));
        }
        // we assume someone knew what they were doing when they passed it in
        let sha = expected_sha.unwrap_or_else(|| osutils::sha::sha_chunks(chunks));
        if let Some(nostore_sha) = nostore_sha {
            if sha == nostore_sha {
                return Err(Error::ExistingContent(key.clone()));
            }
        }

        let key = match key {
            Key::Fixed(key) => key.clone(),
            Key::ContentAddressed(key) => {
                let mut key = key.clone();
                key.push(format!("sha1:{}", sha).as_bytes().to_vec());
                key
            }
        };

        let (start, end, r#type) =
            self.compress_block(&key, chunks, length, (length / 2) as u128, soft)?;
        Ok((sha, start, end, r#type))
    }

    /// Compress chunks with label key.
    ///
    /// :param key: A key tuple. It is stored in the output for identification
    ///     of the text during decompression.
    ///
    /// :param chunks: The chunks of bytes to be compressed
    ///
    /// :param input_len: The length of the chunks
    ///
    /// :param max_delta_size: The size above which we issue a fulltext instead
    ///     of a delta.
    ///
    /// :param soft: Do a 'soft' compression. This means that we require larger
    ///     ranges to match to be considered for a copy command.
    ///
    /// # Returns
    /// The sha1 of lines, the start and end offsets in the delta, and
    ///     the type ('fulltext' or 'delta').
    fn compress_block(
        &mut self,
        key: &[Vec<u8>],
        chunks: &[&[u8]],
        input_len: usize,
        max_delta_size: u128,
        soft: Option<bool>,
    ) -> Result<(usize, usize, &'static str), Error>;

    /// Return the overall compression ratio.
    fn ratio(&self) -> f32;

    /// Finish this group, creating a formatted stream.
    ///
    /// After calling this, the compressor should no longer be used
    fn flush(self) -> (Vec<Vec<u8>>, usize);

    /// Call this if you want to 'revoke' the last compression.
    ///
    /// After this, the data structures will be rolled back, but you cannot do more compression.
    fn flush_without_last(self) -> (Vec<Vec<u8>>, usize);
}

pub struct TraditionalGroupCompressor {
    delta_index: crate::groupcompress::line_delta::LinesDeltaIndex,
    endpoint: usize,
    input_bytes: usize,
    last: Option<(usize, usize)>,
    labels_deltas: HashMap<Vec<Vec<u8>>, (usize, usize, usize, usize)>,
}

impl GroupCompressor for TraditionalGroupCompressor {
    fn ratio(&self) -> f32 {
        self.input_bytes as f32 / self.endpoint as f32
    }

    fn flush(self) -> (Vec<Vec<u8>>, usize) {
        (self.delta_index.lines().to_vec(), self.endpoint)
    }

    fn flush_without_last(self) -> (Vec<Vec<u8>>, usize) {
        let last = self.last.unwrap();
        (self.delta_index.lines()[..last.0].to_vec(), last.1)
    }

    fn compress_block(
        &mut self,
        key: &[Vec<u8>],
        chunks: &[&[u8]],
        input_len: usize,
        max_delta_size: u128,
        soft: Option<bool>,
    ) -> Result<(usize, usize, &'static str), Error> {
        let new_lines =
            osutils::chunks_to_lines(chunks.iter().map(|x| Ok::<_, std::io::Error>(*x)))
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
        let (mut out_lines, mut index_lines) =
            self.delta_index
                .make_delta(new_lines.as_slice(), input_len, soft);
        let delta_length = out_lines.iter().map(|l| l.len() as u128).sum();
        let (r#type, out_lines) = if delta_length > max_delta_size {
            // The delta is longer than the fulltext, insert a fulltext
            let mut out_lines = vec![Cow::Borrowed(&b"f"[..]), {
                let mut data = Vec::new();
                write_base128_int(&mut data, input_len as u128).unwrap();
                Cow::Owned(data)
            }];
            index_lines.clear();
            index_lines.extend(vec![false, false]);
            index_lines.extend([true].repeat(new_lines.len()));
            out_lines.extend(new_lines);
            ("fulltext", out_lines)
        } else {
            // this is a worthy delta, output it
            out_lines[0] = Cow::Borrowed(&b"d"[..]);
            // Update the delta_length to include those two encoded integers
            {
                let mut data = Vec::new();
                write_base128_int(&mut data, delta_length).unwrap();
                out_lines[1] = Cow::Owned(data);
            }
            ("delta", out_lines)
        };
        // Before insertion
        let start = self.endpoint;
        let chunk_start = self.delta_index.lines().len();
        self.last = Some((chunk_start, self.endpoint));
        self.delta_index.extend_lines(
            out_lines
                .into_iter()
                .map(|x| x.into_owned())
                .collect::<Vec<_>>()
                .as_slice(),
            &index_lines,
        );
        self.endpoint = self.delta_index.endpoint();
        self.input_bytes += input_len;
        let chunk_end = self.delta_index.lines().len();
        self.labels_deltas
            .insert(key.to_vec(), (start, chunk_start, self.endpoint, chunk_end));
        Ok((start, self.endpoint, r#type))
    }
}

impl Default for TraditionalGroupCompressor {
    fn default() -> Self {
        Self::new()
    }
}

impl TraditionalGroupCompressor {
    pub fn new() -> Self {
        Self {
            delta_index: crate::groupcompress::line_delta::LinesDeltaIndex::new(vec![]),
            endpoint: 0,
            input_bytes: 0,
            last: None,
            labels_deltas: HashMap::new(),
        }
    }

    pub fn chunks(&self) -> &[Vec<u8>] {
        self.delta_index.lines()
    }

    pub fn endpoint(&self) -> usize {
        self.endpoint
    }

    /// Extract a key previously added to the compressor.
    ///
    /// # Arguments
    /// * `key`: The key to extract.
    ///
    /// # Returns
    /// An iterable over chunks and the sha1.
    pub fn extract(&self, key: &Vec<Vec<u8>>) -> Result<(Vec<Vec<u8>>, String), String> {
        let (_start_byte, start_chunk, _end_byte, end_chunk) = self.labels_deltas.get(key).unwrap();
        let delta_chunks = &self.delta_index.lines()[*start_chunk..*end_chunk];
        let stored_bytes = delta_chunks.concat();
        let data = match read_item(&mut stored_bytes.as_slice()).map_err(|e| e.to_string())? {
            GroupCompressItem::Fulltext(data) => vec![data],
            GroupCompressItem::Delta(data) => {
                let source = self.delta_index.lines()[..*start_chunk].concat();
                vec![apply_delta(source.as_slice(), data.as_slice())?]
            }
        };
        let data_sha1 = osutils::sha::sha_chunks(data.as_slice());
        Ok((data, data_sha1))
    }
}

/// A group compressor backed by the rabin-fingerprint delta algorithm.
///
/// Mirrors the layout of the historical Python `RabinGroupCompressor` class:
/// the compressor accumulates records as a flat `Vec<Vec<u8>>` of chunks,
/// keyed by `(start_byte, start_chunk, end_byte, end_chunk)` tuples in
/// `labels_deltas`. Each record stored in `chunks` consists of a one-byte type
/// header (`b"f"` for fulltext, `b"d"` for delta), a base-128 encoded length,
/// and the payload bytes.
pub struct RabinGroupCompressor {
    delta_index: OwningDeltaIndex,
    chunks: Vec<Vec<u8>>,
    endpoint: usize,
    input_bytes: usize,
    last: Option<(usize, usize)>,
    labels_deltas: HashMap<Vec<Vec<u8>>, (usize, usize, usize, usize)>,
}

impl Default for RabinGroupCompressor {
    fn default() -> Self {
        Self::new(None)
    }
}

impl RabinGroupCompressor {
    pub fn new(max_bytes_to_index: Option<usize>) -> Self {
        Self {
            delta_index: OwningDeltaIndex::new(max_bytes_to_index),
            chunks: Vec::new(),
            endpoint: 0,
            input_bytes: 0,
            last: None,
            labels_deltas: HashMap::new(),
        }
    }

    pub fn chunks(&self) -> &[Vec<u8>] {
        &self.chunks
    }

    pub fn endpoint(&self) -> usize {
        self.endpoint
    }

    pub fn input_bytes(&self) -> usize {
        self.input_bytes
    }

    pub fn max_bytes_to_index(&self) -> Option<usize> {
        self.delta_index.max_bytes_to_index()
    }

    pub fn labels_deltas(&self) -> &HashMap<Vec<Vec<u8>>, (usize, usize, usize, usize)> {
        &self.labels_deltas
    }

    /// Extract a previously-compressed record back to its original bytes.
    pub fn extract(&self, key: &Vec<Vec<u8>>) -> Result<(Vec<Vec<u8>>, String), String> {
        let (_start_byte, start_chunk, _end_byte, end_chunk) = self
            .labels_deltas
            .get(key)
            .ok_or_else(|| format!("key not found in compressor: {:?}", key))?;
        let delta_chunks = &self.chunks[*start_chunk..*end_chunk];
        let stored_bytes: Vec<u8> = delta_chunks.concat();
        if stored_bytes.is_empty() {
            return Err("empty stored bytes".to_string());
        }
        let kind = stored_bytes[0];
        let mut cursor = std::io::Cursor::new(&stored_bytes[1..]);
        let payload_len = read_base128_int(&mut cursor).map_err(|e| e.to_string())?;
        let len_len = cursor.position() as usize;
        let data_len = payload_len as usize + 1 + len_len;
        if data_len != stored_bytes.len() {
            return Err(format!(
                "Index claimed length, but stored bytes claim {} != {}",
                stored_bytes.len(),
                data_len,
            ));
        }
        let payload = &stored_bytes[1 + len_len..];
        let data = match kind {
            b'f' => vec![payload.to_vec()],
            b'd' => {
                let source = self.chunks[..*start_chunk].concat();
                vec![apply_delta(&source, payload)?]
            }
            other => {
                return Err(format!(
                    "Unknown content kind, bytes claim {}",
                    other as char
                ))
            }
        };
        let data_sha1 = osutils::sha::sha_chunks(&data);
        Ok((data, data_sha1))
    }

    fn output_chunks(&mut self, new_chunks: Vec<Vec<u8>>) {
        self.last = Some((self.chunks.len(), self.endpoint));
        let added: usize = new_chunks.iter().map(|c| c.len()).sum();
        self.chunks.extend(new_chunks);
        self.endpoint += added;
    }

    /// Roll back the most recent `compress_block` call.
    ///
    /// After this, the compressor is left in a state where you cannot continue
    /// compressing — only `flush` is meaningful. Mirrors the Python
    /// `_pop_last`.
    pub fn pop_last(&mut self) {
        let (chunk_start, byte_endpoint) = self.last.expect("pop_last called without a last entry");
        self.chunks.truncate(chunk_start);
        self.endpoint = byte_endpoint;
        self.last = None;
    }
}

impl GroupCompressor for RabinGroupCompressor {
    fn ratio(&self) -> f32 {
        if self.endpoint == 0 {
            0.0
        } else {
            self.input_bytes as f32 / self.endpoint as f32
        }
    }

    fn flush(self) -> (Vec<Vec<u8>>, usize) {
        (self.chunks, self.endpoint)
    }

    fn flush_without_last(mut self) -> (Vec<Vec<u8>>, usize) {
        self.pop_last();
        self.flush()
    }

    fn compress_block(
        &mut self,
        key: &[Vec<u8>],
        chunks: &[&[u8]],
        input_len: usize,
        max_delta_size: u128,
        _soft: Option<bool>,
    ) -> Result<(usize, usize, &'static str), Error> {
        let bytes: Vec<u8> = chunks.iter().flat_map(|c| c.iter().copied()).collect();
        let max_delta = max_delta_size as usize;
        let delta = self
            .delta_index
            .make_delta(&bytes, max_delta)
            .expect("rabin delta indexing");

        let (r#type, new_chunks): (&'static str, Vec<Vec<u8>>) = match delta {
            None => {
                let mut enc_length = Vec::new();
                write_base128_int(&mut enc_length, input_len as u128).unwrap();
                let len_mini_header = 1 + enc_length.len();
                self.delta_index.add_source(bytes, len_mini_header);
                let mut new_chunks = Vec::with_capacity(2 + chunks.len());
                new_chunks.push(b"f".to_vec());
                new_chunks.push(enc_length);
                for chunk in chunks {
                    new_chunks.push(chunk.to_vec());
                }
                ("fulltext", new_chunks)
            }
            Some(delta_bytes) => {
                let mut enc_length = Vec::new();
                write_base128_int(&mut enc_length, delta_bytes.len() as u128).unwrap();
                let len_mini_header = 1 + enc_length.len();
                self.delta_index
                    .add_delta_source(delta_bytes.clone(), len_mini_header)
                    .expect("rabin delta source");
                let new_chunks = vec![b"d".to_vec(), enc_length, delta_bytes];
                ("delta", new_chunks)
            }
        };

        let start = self.endpoint;
        let chunk_start = self.chunks.len();
        self.output_chunks(new_chunks);
        self.input_bytes += input_len;
        let chunk_end = self.chunks.len();
        self.labels_deltas
            .insert(key.to_vec(), (start, chunk_start, self.endpoint, chunk_end));
        Ok((start, self.endpoint, r#type))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(parts: &[&[u8]]) -> Key {
        Key::Fixed(parts.iter().map(|p| p.to_vec()).collect())
    }

    #[test]
    fn rabin_compressor_round_trips_fulltext() {
        let mut gc = RabinGroupCompressor::new(None);
        let text = b"hello world\nthis is a fulltext\n";
        let (sha, start, end, kind) = gc
            .compress(
                &key(&[b"label"]),
                &[text.as_slice()],
                text.len(),
                None,
                None,
                None,
            )
            .unwrap();
        assert_eq!(kind, "fulltext");
        assert!(end > start);
        assert!(!sha.is_empty());

        let stored_key: Vec<Vec<u8>> = vec![b"label".to_vec()];
        let (data, data_sha) = gc.extract(&stored_key).unwrap();
        assert_eq!(data, vec![text.to_vec()]);
        assert_eq!(data_sha, sha);
    }

    #[test]
    fn rabin_compressor_round_trips_delta() {
        // Two records sharing a long common prefix should let the second be
        // delta-encoded against the first.
        let mut gc = RabinGroupCompressor::new(None);
        let base = b"common prefix that is long enough to be worth indexing\nmore shared text\n";
        let derived = b"common prefix that is long enough to be worth indexing\nmore shared text\nplus a little extra\n";
        gc.compress(
            &key(&[b"base"]),
            &[base.as_slice()],
            base.len(),
            None,
            None,
            None,
        )
        .unwrap();
        let (_sha, _start, _end, kind) = gc
            .compress(
                &key(&[b"derived"]),
                &[derived.as_slice()],
                derived.len(),
                None,
                None,
                None,
            )
            .unwrap();
        assert_eq!(kind, "delta");

        let (data, _) = gc.extract(&vec![b"derived".to_vec()]).unwrap();
        assert_eq!(data, vec![derived.to_vec()]);
    }

    #[test]
    fn rabin_compressor_pop_last_rolls_back() {
        let mut gc = RabinGroupCompressor::new(None);
        gc.compress(
            &key(&[b"a"]),
            &[b"first record\n".as_slice()],
            13,
            None,
            None,
            None,
        )
        .unwrap();
        let chunks_after_first = gc.chunks().to_vec();
        let endpoint_after_first = gc.endpoint();
        gc.compress(
            &key(&[b"b"]),
            &[b"second record\n".as_slice()],
            14,
            None,
            None,
            None,
        )
        .unwrap();
        gc.pop_last();
        assert_eq!(gc.chunks(), chunks_after_first.as_slice());
        assert_eq!(gc.endpoint(), endpoint_after_first);
    }
}
