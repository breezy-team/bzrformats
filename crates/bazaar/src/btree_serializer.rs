// Copyright (C) 2008, 2009, 2010 Canonical Ltd
// Copyright (C) 2024 Jelmer Vernooij
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

//! Pure-Rust core of the btree CHK leaf-node serializer.
//!
//! Ported from `bzrformats._btree_serializer`. The performance-critical bit is
//! [`ChkLeafNode`], which parses a `gc-chk-sha1` leaf node and answers sha1
//! lookups via a precomputed offset table plus binary search.
//!
//! Everything here operates on plain bytes and 20-byte sha1 arrays; the pyo3
//! wrapper marshals the `(b"sha1:...",)` key tuples and `(value, refs)` shapes.

/// Errors parsing a CHK sha1 leaf node.
#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    /// The data did not begin with the `type=leaf\n` header.
    MissingLeafHeader,
    /// A record line did not start with `sha1:`.
    MissingSha1Prefix,
    /// The hex sha1 portion was not exactly 40 valid hex characters.
    BadSha1Hex,
    /// The line structure (null separators / value fields) was malformed.
    MalformedRecord(&'static str),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::MissingLeafHeader => write!(f, "bytes did not start with 'type=leaf\\n'"),
            Error::MissingSha1Prefix => write!(f, "line did not start with sha1:"),
            Error::BadSha1Hex => write!(f, "could not unhexlify 40-char sha1"),
            Error::MalformedRecord(why) => write!(f, "malformed record: {}", why),
        }
    }
}

impl std::error::Error for Error {}

static HEX_CHARS: &[u8; 16] = b"0123456789abcdef";

/// Lookup table mapping an ASCII byte to its hex value 0..15, or -1 if invalid.
fn build_unhex_table() -> [i8; 256] {
    let mut table = [-1i8; 256];
    for i in 0u8..10 {
        table[(b'0' + i) as usize] = i as i8;
    }
    for i in 0u8..6 {
        table[(b'a' + i) as usize] = (10 + i) as i8;
        table[(b'A' + i) as usize] = (10 + i) as i8;
    }
    table
}

/// Convert 40 hex bytes into a 20-byte binary sha1. Returns `false` (leaving
/// `bin` partially written) on invalid input.
pub fn unhexlify_sha1(hex: &[u8], bin: &mut [u8; 20]) -> bool {
    let table = build_unhex_table();
    if hex.len() != 40 {
        return false;
    }
    for i in 0..20 {
        let top = table[hex[i * 2] as usize];
        let bot = table[hex[i * 2 + 1] as usize];
        if top < 0 || bot < 0 {
            return false;
        }
        bin[i] = ((top << 4) | bot) as u8;
    }
    true
}

/// Convert a 20-byte binary sha1 into 40 lowercase hex bytes.
pub fn hexlify_sha1(bin: &[u8; 20]) -> [u8; 40] {
    let mut hex = [0u8; 40];
    for i in 0..20 {
        hex[i * 2] = HEX_CHARS[((bin[i] >> 4) & 0xf) as usize];
        hex[i * 2 + 1] = HEX_CHARS[(bin[i] & 0xf) as usize];
    }
    hex
}

/// Decode a `b"sha1:<40 hex>"` byte string (length 45) to a binary sha1.
/// Returns `None` if the bytes are not a valid sha1 key body.
pub fn sha1_bytes_to_bin(data: &[u8]) -> Option<[u8; 20]> {
    if data.len() != 45 || !data.starts_with(b"sha1:") {
        return None;
    }
    let mut sha1 = [0u8; 20];
    if unhexlify_sha1(&data[5..], &mut sha1) {
        Some(sha1)
    } else {
        None
    }
}

/// Encode a binary sha1 as the 45-byte `b"sha1:<40 hex>"` key body.
pub fn sha1_bin_to_bytes(sha1: &[u8; 20]) -> Vec<u8> {
    let hex = hexlify_sha1(sha1);
    let mut buf = Vec::with_capacity(45);
    buf.extend_from_slice(b"sha1:");
    buf.extend_from_slice(&hex);
    buf
}

/// Interpret the first 4 bytes of a sha1 as a big-endian u32.
fn sha1_to_uint(sha1: &[u8; 20]) -> u32 {
    u32::from_be_bytes([sha1[0], sha1[1], sha1[2], sha1[3]])
}

/// A parsed entry of a `gc-chk-sha1` leaf node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChkSha1Record {
    pub block_offset: u64,
    pub block_length: u32,
    pub record_start: u32,
    pub record_end: u32,
    pub sha1: [u8; 20],
}

impl ChkSha1Record {
    /// Format the record's value field: `"block_offset block_length record_start record_end"`.
    pub fn format_value(&self) -> Vec<u8> {
        format!(
            "{} {} {} {}",
            self.block_offset, self.block_length, self.record_start, self.record_end
        )
        .into_bytes()
    }
}

/// All entries of one `gc-chk-sha1` leaf node, indexed for fast sha1 lookup.
///
/// Mirrors `bzrformats._btree_serializer.GCCHKSHA1LeafNode`.
#[derive(Debug)]
pub struct ChkLeafNode {
    records: Vec<ChkSha1Record>,
    /// Number of bits to shift a sha1's leading u32 by to reach the byte that
    /// first differs across records. 24 means the very first byte varies.
    common_shift: u8,
    /// Maps an interesting byte (0..=256) to the first record at or after it.
    offsets: [u8; 257],
}

impl ChkLeafNode {
    /// Parse leaf-node bytes (including the `type=leaf\n` header).
    pub fn parse(data: &[u8]) -> Result<Self, Error> {
        if !data.starts_with(b"type=leaf\n") {
            return Err(Error::MissingLeafHeader);
        }
        let content = &data[10..];
        let num_records = content.iter().filter(|&&b| b == b'\n').count();
        let mut records = Vec::with_capacity(num_records);

        let mut cur = content;
        while !cur.is_empty() {
            let nl_pos = match cur.iter().position(|&b| b == b'\n') {
                Some(p) => p,
                None => break,
            };
            let line = &cur[..nl_pos];
            cur = &cur[nl_pos + 1..];
            if line.is_empty() {
                continue;
            }
            records.push(parse_one_entry(line)?);
        }

        let mut node = ChkLeafNode {
            records,
            common_shift: 0,
            offsets: [0u8; 257],
        };
        node.compute_common();
        Ok(node)
    }

    pub fn records(&self) -> &[ChkSha1Record] {
        &self.records
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn common_shift(&self) -> u8 {
        self.common_shift
    }

    pub fn offsets(&self) -> &[u8; 257] {
        &self.offsets
    }

    pub fn min_record(&self) -> Option<&ChkSha1Record> {
        self.records.first()
    }

    pub fn max_record(&self) -> Option<&ChkSha1Record> {
        self.records.last()
    }

    /// The offset-table bucket a sha1 falls into.
    pub fn offset_for_sha1(&self, sha1: &[u8; 20]) -> usize {
        let as_uint = sha1_to_uint(sha1);
        ((as_uint >> self.common_shift) & 0xFF) as usize
    }

    fn compute_common(&mut self) {
        if self.records.len() < 2 {
            self.common_shift = 24;
        } else {
            let mut common_mask: u32 = 0xFFFFFFFF;
            let first = sha1_to_uint(&self.records[0].sha1);
            for record in &self.records[1..] {
                let this = sha1_to_uint(&record.sha1);
                common_mask &= !(first ^ this);
            }
            let mut shift: u8 = 24;
            while common_mask & 0x80000000 != 0 && shift > 0 {
                common_mask <<= 1;
                shift -= 1;
            }
            self.common_shift = shift;
        }

        let max_offset = std::cmp::min(self.records.len(), 255);
        let mut offset: usize = 0;
        for i in 0..max_offset {
            let this_offset = self.offset_for_sha1(&self.records[i].sha1);
            while offset <= this_offset {
                self.offsets[offset] = i as u8;
                offset += 1;
            }
        }
        while offset < 257 {
            self.offsets[offset] = max_offset as u8;
            offset += 1;
        }
    }

    /// Find the record index for `sha1`, or `None` if absent. Uses the offset
    /// table to bound a binary search over the (sorted) records.
    pub fn lookup_record(&self, sha1: &[u8; 20]) -> Option<usize> {
        let offset = self.offset_for_sha1(sha1);
        let lo_val = self.offsets[offset] as usize;
        let hi_val = self.offsets[offset + 1];
        let mut hi = if hi_val == 255 {
            self.records.len()
        } else {
            hi_val as usize
        };
        let mut lo = lo_val;
        while lo < hi {
            let mid = (lo + hi) / 2;
            match self.records[mid].sha1.cmp(sha1) {
                std::cmp::Ordering::Equal => return Some(mid),
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        None
    }
}

/// Parse one record line (without the trailing newline): `sha1:<40hex>\0\0<value>`.
fn parse_one_entry(line: &[u8]) -> Result<ChkSha1Record, Error> {
    if !line.starts_with(b"sha1:") {
        return Err(Error::MissingSha1Prefix);
    }
    let after_prefix = &line[5..];
    let nul_pos = after_prefix
        .iter()
        .position(|&b| b == 0)
        .ok_or(Error::MalformedRecord("missing null byte after sha1"))?;
    if nul_pos != 40 {
        return Err(Error::MalformedRecord("sha1 was not 40 hex bytes"));
    }
    let mut sha1 = [0u8; 20];
    if !unhexlify_sha1(&after_prefix[..40], &mut sha1) {
        return Err(Error::BadSha1Hex);
    }
    let rest = &after_prefix[41..];
    if rest.is_empty() || rest[0] != 0 {
        return Err(Error::MalformedRecord("expected a second null byte"));
    }
    let value_str = &rest[1..];
    let parts: Vec<&[u8]> = value_str.split(|&b| b == b' ').collect();
    if parts.len() != 4 {
        return Err(Error::MalformedRecord("value did not have 4 fields"));
    }
    let parse_u64 = |b: &[u8]| -> Result<u64, Error> {
        std::str::from_utf8(b)
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or(Error::MalformedRecord("non-numeric value field"))
    };
    let parse_u32 = |b: &[u8]| -> Result<u32, Error> {
        std::str::from_utf8(b)
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or(Error::MalformedRecord("non-numeric value field"))
    };
    Ok(ChkSha1Record {
        block_offset: parse_u64(parts[0])?,
        block_length: parse_u32(parts[1])?,
        record_start: parse_u32(parts[2])?,
        record_end: parse_u32(parts[3])?,
        sha1,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hexlify_round_trips() {
        let bin = [
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff, 0x01, 0x23, 0x45, 0x67,
        ];
        let hex = hexlify_sha1(&bin);
        assert_eq!(&hex, b"00112233445566778899aabbccddeeff01234567");
        let mut back = [0u8; 20];
        assert!(unhexlify_sha1(&hex, &mut back));
        assert_eq!(back, bin);
    }

    #[test]
    fn test_unhexlify_rejects_invalid() {
        let mut out = [0u8; 20];
        // Wrong length.
        assert!(!unhexlify_sha1(b"abcd", &mut out));
        // Non-hex character ('g') in an otherwise 40-char string.
        let bad = b"g000000000000000000000000000000000000000";
        assert!(!unhexlify_sha1(bad, &mut out));
    }

    #[test]
    fn test_sha1_key_bytes_round_trip() {
        let bin = [0xabu8; 20];
        let bytes = sha1_bin_to_bytes(&bin);
        assert_eq!(bytes.len(), 45);
        assert!(bytes.starts_with(b"sha1:"));
        assert_eq!(sha1_bytes_to_bin(&bytes), Some(bin));
        // Not a sha1 key.
        assert_eq!(sha1_bytes_to_bin(b"not-a-key"), None);
        assert_eq!(sha1_bytes_to_bin(b"sha1:nothex"), None);
    }

    /// Build a leaf node body from `(sha1_bin, value)` records.
    fn make_leaf(records: &[([u8; 20], &str)]) -> Vec<u8> {
        let mut data = b"type=leaf\n".to_vec();
        for (sha1, value) in records {
            data.extend_from_slice(b"sha1:");
            data.extend_from_slice(&hexlify_sha1(sha1));
            data.push(0);
            data.push(0);
            data.extend_from_slice(value.as_bytes());
            data.push(b'\n');
        }
        data
    }

    fn sha1_with_prefix(bytes: &[u8]) -> [u8; 20] {
        let mut s = [0u8; 20];
        s[..bytes.len()].copy_from_slice(bytes);
        s
    }

    #[test]
    fn test_parse_rejects_non_leaf() {
        assert_eq!(
            ChkLeafNode::parse(b"type=internal\n").unwrap_err(),
            Error::MissingLeafHeader
        );
    }

    #[test]
    fn test_parse_empty_leaf() {
        let node = ChkLeafNode::parse(b"type=leaf\n").unwrap();
        assert!(node.is_empty());
        assert_eq!(node.len(), 0);
        assert!(node.min_record().is_none());
    }

    #[test]
    fn test_parse_one_key_leaf() {
        let sha = sha1_with_prefix(&[1, 2, 3, 4]);
        let data = make_leaf(&[(sha, "0 10 0 5")]);
        let node = ChkLeafNode::parse(&data).unwrap();
        assert_eq!(node.len(), 1);
        let rec = &node.records()[0];
        assert_eq!(rec.sha1, sha);
        assert_eq!(rec.block_offset, 0);
        assert_eq!(rec.block_length, 10);
        assert_eq!(rec.record_start, 0);
        assert_eq!(rec.record_end, 5);
        assert_eq!(rec.format_value(), b"0 10 0 5");
        // common_shift is 24 for fewer than two records.
        assert_eq!(node.common_shift(), 24);
        assert_eq!(node.lookup_record(&sha), Some(0));
    }

    #[test]
    fn test_lookup_multi_key() {
        // Records whose leading byte spans the full range so the lookup table
        // is exercised across buckets.
        let recs: Vec<([u8; 20], &str)> = (0u8..8)
            .map(|i| (sha1_with_prefix(&[i * 32, 0, 0, 0]), "0 1 0 1"))
            .collect();
        let data = make_leaf(&recs);
        let node = ChkLeafNode::parse(&data).unwrap();
        assert_eq!(node.len(), 8);
        for (i, (sha, _)) in recs.iter().enumerate() {
            assert_eq!(node.lookup_record(sha), Some(i));
        }
        // A sha1 not in the node.
        assert_eq!(node.lookup_record(&sha1_with_prefix(&[200, 0, 0, 0])), None);
    }

    #[test]
    fn test_common_shift_when_prefix_shared() {
        // All records share the top byte (0xAB), differing in byte 1, so the
        // interesting byte moves and common_shift drops below 24.
        let recs: Vec<([u8; 20], &str)> = (0u8..4)
            .map(|i| (sha1_with_prefix(&[0xAB, i, 0, 0]), "0 1 0 1"))
            .collect();
        let data = make_leaf(&recs);
        let node = ChkLeafNode::parse(&data).unwrap();
        assert!(node.common_shift() < 24);
        for (i, (sha, _)) in recs.iter().enumerate() {
            assert_eq!(node.lookup_record(sha), Some(i));
        }
    }

    #[test]
    fn test_parse_rejects_malformed_record() {
        // Missing the second null byte.
        let mut data = b"type=leaf\nsha1:".to_vec();
        data.extend_from_slice(&hexlify_sha1(&[0u8; 20]));
        data.push(0);
        data.extend_from_slice(b"0 1 0 1\n");
        assert!(ChkLeafNode::parse(&data).is_err());
    }
}
