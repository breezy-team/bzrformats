//! Groupcompress delta wire format: base128 integers, copy/insert
//! instructions, and whole-delta apply.
//!
//! This module implements the low-level bits of the groupcompress delta
//! format shared by both the knit-derived [`super::line_delta`] path and
//! the rabin-hash path in [`super::rabin_delta`]. Callers normally want
//! the `read_*`/`write_*` pair that takes an `impl Read`/`impl Write` —
//! the slice-based helpers ([`encode_base128_int`], [`decode_base128_int`],
//! [`decode_copy_instruction`]) are ergonomic wrappers that allocate or
//! build a `Cursor` under the hood.
//!
//! High-level whole-delta operations ([`apply_delta`],
//! [`apply_delta_to_source`], [`decode_instruction`]) return structured
//! [`DeltaError`] values so callers can discriminate truncated streams,
//! out-of-range copies, and length mismatches without string matching.

use byteorder::{ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};

pub const MAX_INSERT_SIZE: usize = 0x7F;
pub const MAX_COPY_SIZE: usize = 0x10000;

/// Errors returned by the groupcompress delta decoder / applier.
///
/// The variants distinguish I/O-shaped failures (truncated streams) from
/// invariant violations (out-of-range copies, wrong command byte) so
/// callers can tell "this is a short read" apart from "this is corrupt
/// data".
///
/// `DeltaError` is `Clone + PartialEq + Eq` so it can participate in test
/// assertions directly. The I/O path normalises `std::io::Error` into a
/// `(ErrorKind, String)` pair for the same reason the knit module does
/// it: corrupt streams produce textual diagnostics and carrying a live
/// `io::Error` would poison the derive.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeltaError {
    /// The underlying reader (usually `&[u8]`) returned an `io::Error`,
    /// most commonly `UnexpectedEof` from a truncated delta stream.
    /// The original error is normalised into its `ErrorKind` and
    /// display message so the variant stays value-typed.
    Io {
        kind: std::io::ErrorKind,
        message: String,
    },
    /// A copy instruction addressed bytes past the end of its source.
    CopyOutOfRange {
        offset: usize,
        length: usize,
        source_len: usize,
    },
    /// The `0x00` command byte is reserved and not supported.
    ReservedCommandZero,
    /// The high bit (`0x80`) of a copy command was clear. Used by the
    /// low-level [`read_copy_instruction`] path when the caller hands in
    /// a byte that wasn't a copy instruction at all.
    NotACopyCommand { cmd: u8 },
    /// The trailing length self-check on an applied delta failed: the
    /// header claimed `declared` output bytes but the applier produced
    /// `actual`.
    LengthMismatch { declared: usize, actual: usize },
    /// [`apply_delta_to_source`] got an out-of-range `[delta_start,
    /// delta_end)` slice of the source buffer.
    InvalidDeltaRange {
        start: usize,
        end: usize,
        source_len: usize,
    },
    /// An insert instruction claimed more bytes than the backing buffer
    /// had left. Used by the slice-oriented [`decode_instruction`]; the
    /// streaming [`read_instruction`] path surfaces this as
    /// [`DeltaError::Io`] with `UnexpectedEof`.
    InsertPastEnd {
        pos: usize,
        length: usize,
        data_len: usize,
    },
}

impl From<std::io::Error> for DeltaError {
    fn from(e: std::io::Error) -> Self {
        DeltaError::Io {
            kind: e.kind(),
            message: e.to_string(),
        }
    }
}

impl std::fmt::Display for DeltaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeltaError::Io { message, .. } => write!(f, "{}", message),
            DeltaError::CopyOutOfRange {
                offset,
                length,
                source_len,
            } => write!(
                f,
                "data would copy bytes past the end of source \
                 (offset={}, length={}, source_len={})",
                offset, length, source_len
            ),
            DeltaError::ReservedCommandZero => write!(f, "Command == 0 not supported yet"),
            DeltaError::NotACopyCommand { cmd } => {
                write!(
                    f,
                    "copy instructions must have bit 0x80 set (got {:#x})",
                    cmd
                )
            }
            DeltaError::LengthMismatch { declared, actual } => write!(
                f,
                "Delta claimed to be {} long, but ended up {} long",
                declared, actual
            ),
            DeltaError::InvalidDeltaRange {
                start,
                end,
                source_len,
            } => write!(
                f,
                "invalid delta range [{}, {}) in source of length {}",
                start, end, source_len
            ),
            DeltaError::InsertPastEnd {
                pos,
                length,
                data_len,
            } => write!(
                f,
                "Instruction length {} at position {} extends past end of data ({} bytes)",
                length, pos, data_len
            ),
        }
    }
}

impl std::error::Error for DeltaError {}

/// Allocating convenience for [`write_base128_int`]: encode `val` into a
/// fresh `Vec<u8>`. Prefer the `write_*` variant when you already have an
/// `impl Write` sink to avoid the intermediate allocation.
pub fn encode_base128_int(val: u128) -> Vec<u8> {
    let mut data = Vec::new();
    write_base128_int(&mut data, val).unwrap();
    data
}

/// Encode an integer using base128 encoding.
pub fn write_base128_int<W: std::io::Write>(mut writer: W, val: u128) -> std::io::Result<usize> {
    let mut val = val;
    let mut length = 0;
    while val >= 0x80 {
        writer.write_all(&[((val | 0x80) & 0xFF) as u8])?;
        length += 1;
        val >>= 7;
    }
    writer.write_all(&[val as u8])?;
    Ok(length + 1)
}

/// Decode a base128 encoded integer.
pub fn read_base128_int<R: Read>(reader: &mut R) -> Result<u128, std::io::Error> {
    let mut val: u128 = 0;
    let mut shift = 0;
    let mut bval = [0];
    reader.read_exact(&mut bval)?;
    while bval[0] >= 0x80 {
        val |= ((bval[0] & 0x7F) as u128) << shift;
        reader.read_exact(&mut bval)?;
        shift += 7;
    }

    val |= (bval[0] as u128) << shift;
    Ok(val)
}

#[cfg(test)]
mod test_base128_int {
    #[test]
    fn test_decode_base128_int() {
        assert_eq!(super::decode_base128_int(&[0x00]), (0, 1));
        assert_eq!(super::decode_base128_int(&[0x01]), (1, 1));
        assert_eq!(super::decode_base128_int(&[0x7F]), (127, 1));
        assert_eq!(super::decode_base128_int(&[0x80, 0x01]), (128, 2));
        assert_eq!(super::decode_base128_int(&[0xFF, 0x01]), (255, 2));
        assert_eq!(super::decode_base128_int(&[0x80, 0x02]), (256, 2));
        assert_eq!(super::decode_base128_int(&[0x81, 0x02]), (257, 2));
        assert_eq!(super::decode_base128_int(&[0x82, 0x02]), (258, 2));
        assert_eq!(super::decode_base128_int(&[0xFF, 0x7F]), (16383, 2));
        assert_eq!(super::decode_base128_int(&[0x80, 0x80, 0x01]), (16384, 3));
        assert_eq!(super::decode_base128_int(&[0xFF, 0xFF, 0x7F]), (2097151, 3));
        assert_eq!(
            super::decode_base128_int(&[0x80, 0x80, 0x80, 0x01]),
            (2097152, 4)
        );
        assert_eq!(
            super::decode_base128_int(&[0xFF, 0xFF, 0xFF, 0x7F]),
            (268435455, 4)
        );
        assert_eq!(
            super::decode_base128_int(&[0x80, 0x80, 0x80, 0x80, 0x01]),
            (268435456, 5)
        );
        assert_eq!(
            super::decode_base128_int(&[0xFF, 0xFF, 0xFF, 0xFF, 0x7F]),
            (34359738367, 5)
        );
        assert_eq!(
            super::decode_base128_int(&[0x80, 0x80, 0x80, 0x80, 0x80, 0x01]),
            (34359738368, 6)
        );
    }

    #[test]
    fn test_encode_base128_int() {
        assert_eq!(super::encode_base128_int(0), [0x00]);
        assert_eq!(super::encode_base128_int(1), [0x01]);
        assert_eq!(super::encode_base128_int(127), [0x7F]);
        assert_eq!(super::encode_base128_int(128), [0x80, 0x01]);
        assert_eq!(super::encode_base128_int(255), [0xFF, 0x01]);
        assert_eq!(super::encode_base128_int(256), [0x80, 0x02]);
        assert_eq!(super::encode_base128_int(257), [0x81, 0x02]);
        assert_eq!(super::encode_base128_int(258), [0x82, 0x02]);
        assert_eq!(super::encode_base128_int(16383), [0xFF, 0x7F]);
        assert_eq!(super::encode_base128_int(16384), [0x80, 0x80, 0x01]);
        assert_eq!(super::encode_base128_int(2097151), [0xFF, 0xFF, 0x7F]);
        assert_eq!(super::encode_base128_int(2097152), [0x80, 0x80, 0x80, 0x01]);
        assert_eq!(
            super::encode_base128_int(268435455),
            [0xFF, 0xFF, 0xFF, 0x7F]
        );
        assert_eq!(
            super::encode_base128_int(268435456),
            [0x80, 0x80, 0x80, 0x80, 0x01]
        );
        assert_eq!(
            super::encode_base128_int(34359738367),
            [0xFF, 0xFF, 0xFF, 0xFF, 0x7F]
        );
        assert_eq!(
            super::encode_base128_int(34359738368),
            [0x80, 0x80, 0x80, 0x80, 0x80, 0x01]
        );
        assert_eq!(
            super::encode_base128_int(4398046511103),
            [0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]
        );
        assert_eq!(
            super::encode_base128_int(4398046511104),
            [0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01]
        );
    }
}

/// Slice-oriented counterpart to [`read_base128_int`]: returns
/// `(value, consumed_bytes)`. Panics if `data` doesn't contain a complete
/// base128 encoding — use the streaming variant directly if you need to
/// tolerate truncation.
pub fn decode_base128_int(data: &[u8]) -> (u128, usize) {
    let mut cursor = std::io::Cursor::new(data);
    let val = read_base128_int(&mut cursor).unwrap();
    (val, cursor.position() as usize)
}

/// Slice-oriented counterpart to [`read_copy_instruction`]: decode a
/// copy command that starts at `pos` in `data`, returning
/// `(offset, length, new_pos)` where `new_pos` is the byte just after
/// the instruction.
pub fn decode_copy_instruction(
    data: &[u8],
    cmd: u8,
    pos: usize,
) -> Result<(usize, usize, usize), DeltaError> {
    let mut c = std::io::Cursor::new(&data[pos..]);
    let (offset, length) = read_copy_instruction(&mut c, cmd)?;
    Ok((offset, length, pos + c.position() as usize))
}

pub type CopyInstruction = (usize, usize);

pub fn read_copy_instruction<R: Read>(
    reader: &mut R,
    cmd: u8,
) -> Result<CopyInstruction, DeltaError> {
    if cmd & 0x80 != 0x80 {
        return Err(DeltaError::NotACopyCommand { cmd });
    }
    let mut offset = 0;
    let mut length = 0;

    if cmd & 0x01 != 0 {
        offset = reader.read_u8()? as usize;
    }
    if cmd & 0x02 != 0 {
        offset |= (reader.read_u8()? as usize) << 8;
    }
    if cmd & 0x04 != 0 {
        offset |= (reader.read_u8()? as usize) << 16;
    }
    if cmd & 0x08 != 0 {
        offset |= (reader.read_u8()? as usize) << 24;
    }
    if cmd & 0x10 != 0 {
        length = reader.read_u8()? as usize;
    }
    if cmd & 0x20 != 0 {
        length |= (reader.read_u8()? as usize) << 8;
    }
    if cmd & 0x40 != 0 {
        length |= (reader.read_u8()? as usize) << 16;
    }
    if length == 0 {
        length = 65536;
    }

    Ok((offset, length))
}

/// Apply a groupcompress delta to `basis`, returning the reconstructed
/// target bytes.
pub fn apply_delta(basis: &[u8], mut delta: &[u8]) -> Result<Vec<u8>, DeltaError> {
    let target_length = read_base128_int(&mut delta)?;
    let mut lines = Vec::new();

    while !delta.is_empty() {
        let cmd = delta.read_u8()?;

        if cmd & 0x80 != 0 {
            let (offset, length) = read_copy_instruction(&mut delta, cmd)?;
            let last = offset + length;
            if last > basis.len() {
                return Err(DeltaError::CopyOutOfRange {
                    offset,
                    length,
                    source_len: basis.len(),
                });
            }
            lines.extend_from_slice(&basis[offset..last]);
        } else {
            if cmd == 0 {
                return Err(DeltaError::ReservedCommandZero);
            }
            lines.extend_from_slice(&delta[..cmd as usize]);
            delta = &delta[cmd as usize..];
        }
    }

    let target_len = target_length as usize;
    if lines.len() != target_len {
        return Err(DeltaError::LengthMismatch {
            declared: target_len,
            actual: lines.len(),
        });
    }

    Ok(lines)
}

#[cfg(test)]
mod test_apply_delta {
    const TEXT1: &[u8] = b"This is a bit
of source text
which is meant to be matched
against other text
";

    const TEXT2: &[u8] = b"This is a bit
of source text
which is meant to differ from
against other text
";

    #[test]
    fn test_apply_delta() {
        let target =
            super::apply_delta(TEXT1, b"N\x90/\x1fdiffer from\nagainst other text\n").unwrap();
        assert_eq!(target, TEXT2);
        let target =
            super::apply_delta(TEXT2, b"M\x90/\x1ebe matched\nagainst other text\n").unwrap();
        assert_eq!(target, TEXT1);
    }
}

/// Apply a delta that lives at bytes `[delta_start, delta_end)` within
/// `source`. Convenience wrapper around [`apply_delta`] that validates
/// the range first.
pub fn apply_delta_to_source(
    source: &[u8],
    delta_start: usize,
    delta_end: usize,
) -> Result<Vec<u8>, DeltaError> {
    let source_len = source.len();
    if delta_start >= source_len || delta_end > source_len || delta_start >= delta_end {
        return Err(DeltaError::InvalidDeltaRange {
            start: delta_start,
            end: delta_end,
            source_len,
        });
    }
    let delta_bytes = &source[delta_start..delta_end];
    apply_delta(source, delta_bytes)
}

pub fn encode_copy_instruction(mut offset: usize, mut length: usize) -> Vec<u8> {
    let mut copy_bytes = vec![];
    // Convert this offset into a control code and bytes.
    let mut copy_command: u8 = 0x80;

    for copy_bit in [0x01, 0x02, 0x04, 0x08].iter() {
        let base_byte = (offset & 0xff) as u8;
        if base_byte != 0 {
            copy_command |= *copy_bit;
            copy_bytes.push(base_byte);
        }
        offset >>= 8;
    }
    assert!(
        length <= MAX_COPY_SIZE,
        "we don't emit copy records for lengths > 64KiB"
    );
    assert_ne!(length, 0, "we don't emit copy records for lengths == 0");
    if length != 0x10000 {
        // A copy of length exactly 64*1024 == 0x10000 is sent as a length of 0,
        // since that saves bytes for large chained copies
        for copy_bit in [0x10, 0x20].iter() {
            let base_byte = (length & 0xff) as u8;
            if base_byte != 0 {
                copy_command |= *copy_bit;
                copy_bytes.push(base_byte);
            }
            length >>= 8;
        }
    }
    copy_bytes.insert(0, copy_command);
    copy_bytes
}

pub fn write_copy_instruction<W: Write>(
    mut writer: W,
    offset: usize,
    length: usize,
) -> Result<usize, std::io::Error> {
    let data = encode_copy_instruction(offset, length);
    writer.write_all(data.as_slice())?;
    Ok(data.len())
}

pub fn write_insert_instruction<W: Write>(
    mut writer: W,
    data: &[u8],
) -> Result<usize, std::io::Error> {
    let mut total = 0;
    for chunk in data.chunks(0x7F) {
        writer.write_u8(chunk.len() as u8)?;
        writer.write_all(chunk)?;
        total += chunk.len() + 1;
    }
    Ok(total)
}

#[derive(Debug, PartialEq, Eq)]
pub enum Instruction<T: std::borrow::Borrow<[u8]>> {
    r#Copy { offset: usize, length: usize },
    Insert(T),
}

pub fn write_instruction<W: Write, T: std::borrow::Borrow<[u8]>>(
    writer: W,
    instruction: &Instruction<T>,
) -> std::io::Result<usize> {
    match instruction {
        Instruction::Copy { offset, length } => write_copy_instruction(writer, *offset, *length),
        Instruction::Insert(data) => write_insert_instruction(writer, data.borrow()),
    }
}

pub fn read_instruction<R: Read>(mut reader: R) -> Result<Instruction<Vec<u8>>, DeltaError> {
    let cmd = reader.read_u8()?;
    if cmd & 0x80 != 0 {
        let (offset, length) = read_copy_instruction(&mut reader, cmd)?;
        Ok(Instruction::Copy { offset, length })
    } else if cmd == 0 {
        Err(DeltaError::ReservedCommandZero)
    } else {
        let length = cmd as usize;
        let mut data = vec![0; length];
        reader.read_exact(&mut data)?;
        Ok(Instruction::Insert(data))
    }
}

/// Decode a copy instruction from the given data, starting at the given position.
/// Decode a single delta instruction from `data` starting at `pos`,
/// returning the instruction and the new cursor position.
pub fn decode_instruction(
    data: &[u8],
    pos: usize,
) -> Result<(Instruction<&[u8]>, usize), DeltaError> {
    let cmd = data[pos];
    if cmd & 0x80 != 0 {
        let mut c = std::io::Cursor::new(&data[pos + 1..]);
        let (offset, length) = read_copy_instruction(&mut c, cmd)?;
        let newpos = pos + 1 + c.position() as usize;
        Ok((Instruction::Copy { offset, length }, newpos))
    } else {
        let length = cmd as usize;
        let newpos = pos + 1 + length;
        if newpos > data.len() {
            return Err(DeltaError::InsertPastEnd {
                pos,
                length,
                data_len: data.len(),
            });
        }
        Ok((Instruction::Insert(&data[pos + 1..newpos]), newpos))
    }
}

#[cfg(test)]
mod test_copy_instruction {
    fn assert_encode(expected: &[u8], offset: usize, length: usize) {
        let data = super::encode_copy_instruction(offset, length);
        assert_eq!(expected, data);
    }

    fn assert_decode(
        exp_offset: usize,
        exp_length: usize,
        exp_newpos: usize,
        data: &[u8],
        mut pos: usize,
    ) {
        let cmd = data[pos];
        pos += 1;
        let out = super::decode_copy_instruction(data, cmd, pos).unwrap();
        assert_eq!((exp_offset, exp_length, exp_newpos), out);
    }

    #[test]
    fn test_encode_no_length() {
        assert_encode(b"\x80", 0, 64 * 1024);
        assert_encode(b"\x81\x01", 1, 64 * 1024);
        assert_encode(b"\x81\x0a", 10, 64 * 1024);
        assert_encode(b"\x81\xff", 255, 64 * 1024);
        assert_encode(b"\x82\x01", 256, 64 * 1024);
        assert_encode(b"\x83\x01\x01", 257, 64 * 1024);
        assert_encode(b"\x8F\xff\xff\xff\xff", 0xFFFFFFFF, 64 * 1024);
        assert_encode(b"\x8E\xff\xff\xff", 0xFFFFFF00, 64 * 1024);
        assert_encode(b"\x8D\xff\xff\xff", 0xFFFF00FF, 64 * 1024);
        assert_encode(b"\x8B\xff\xff\xff", 0xFF00FFFF, 64 * 1024);
        assert_encode(b"\x87\xff\xff\xff", 0x00FFFFFF, 64 * 1024);
        assert_encode(b"\x8F\x04\x03\x02\x01", 0x01020304, 64 * 1024);
    }

    #[test]
    fn test_encode_no_offset() {
        assert_encode(b"\x90\x01", 0, 1);
        assert_encode(b"\x90\x0a", 0, 10);
        assert_encode(b"\x90\xff", 0, 255);
        assert_encode(b"\xA0\x01", 0, 256);
        assert_encode(b"\xB0\x01\x01", 0, 257);
        assert_encode(b"\xB0\xff\xff", 0, 0xFFFF);
        // Special case, if copy == 64KiB, then we store exactly 0
        // Note that this puns with a copy of exactly 0 bytes, but we don't care
        // about that, as we would never actually copy 0 bytes
        assert_encode(b"\x80", 0, 64 * 1024)
    }

    #[test]
    fn test_encode() {
        assert_encode(b"\x91\x01\x01", 1, 1);
        assert_encode(b"\x91\x09\x0a", 9, 10);
        assert_encode(b"\x91\xfe\xff", 254, 255);
        assert_encode(b"\xA2\x02\x01", 512, 256);
        assert_encode(b"\xB3\x02\x01\x01\x01", 258, 257);
        assert_encode(b"\xB0\x01\x01", 0, 257);
        // Special case, if copy == 64KiB, then we store exactly 0
        // Note that this puns with a copy of exactly 0 bytes, but we don't care
        // about that, as we would never actually copy 0 bytes
        assert_encode(b"\x81\x0a", 10, 64 * 1024);
    }

    #[test]
    fn test_decode_no_length() {
        // If length is 0, it is interpreted as 64KiB
        // The shortest possible instruction is a copy of 64KiB from offset 0
        assert_decode(0, 65536, 1, b"\x80", 0);
        assert_decode(1, 65536, 2, b"\x81\x01", 0);
        assert_decode(10, 65536, 2, b"\x81\x0a", 0);
        assert_decode(255, 65536, 2, b"\x81\xff", 0);
        assert_decode(256, 65536, 2, b"\x82\x01", 0);
        assert_decode(257, 65536, 3, b"\x83\x01\x01", 0);
        assert_decode(0xFFFFFFFF, 65536, 5, b"\x8F\xff\xff\xff\xff", 0);
        assert_decode(0xFFFFFF00, 65536, 4, b"\x8E\xff\xff\xff", 0);
        assert_decode(0xFFFF00FF, 65536, 4, b"\x8D\xff\xff\xff", 0);
        assert_decode(0xFF00FFFF, 65536, 4, b"\x8B\xff\xff\xff", 0);
        assert_decode(0x00FFFFFF, 65536, 4, b"\x87\xff\xff\xff", 0);
        assert_decode(0x01020304, 65536, 5, b"\x8F\x04\x03\x02\x01", 0);
    }

    #[test]
    fn test_decode_no_offset() {
        assert_decode(0, 1, 2, b"\x90\x01", 0);
        assert_decode(0, 10, 2, b"\x90\x0a", 0);
        assert_decode(0, 255, 2, b"\x90\xff", 0);
        assert_decode(0, 256, 2, b"\xA0\x01", 0);
        assert_decode(0, 257, 3, b"\xB0\x01\x01", 0);
        assert_decode(0, 65535, 3, b"\xB0\xff\xff", 0);
        // Special case, if copy == 64KiB, then we store exactly 0
        // Note that this puns with a copy of exactly 0 bytes, but we don't care
        // about that, as we would never actually copy 0 bytes
        assert_decode(0, 65536, 1, b"\x80", 0);
    }

    #[test]
    fn test_decode() {
        assert_decode(1, 1, 3, b"\x91\x01\x01", 0);
        assert_decode(9, 10, 3, b"\x91\x09\x0a", 0);
        assert_decode(254, 255, 3, b"\x91\xfe\xff", 0);
        assert_decode(512, 256, 3, b"\xA2\x02\x01", 0);
        assert_decode(258, 257, 5, b"\xB3\x02\x01\x01\x01", 0);
        assert_decode(0, 257, 3, b"\xB0\x01\x01", 0);
    }

    #[test]
    fn test_decode_not_start() {
        assert_decode(1, 1, 6, b"abc\x91\x01\x01def", 3);
        assert_decode(9, 10, 5, b"ab\x91\x09\x0ade", 2);
        assert_decode(254, 255, 6, b"not\x91\xfe\xffcopy", 3);
    }
}

#[cfg(test)]
mod test_instruction {
    use super::{decode_instruction, Instruction};

    #[test]
    fn test_decode_copy_instruction() {
        assert_eq!(
            Ok((
                Instruction::Copy {
                    offset: 0,
                    length: 65536
                },
                1
            )),
            decode_instruction(&b"\x80"[..], 0)
        );
        assert_eq!(
            Ok((
                Instruction::Copy {
                    offset: 10,
                    length: 65536
                },
                2
            )),
            decode_instruction(&b"\x81\x0a"[..], 0)
        );
    }

    #[test]
    fn test_decode_insert_instruction() {
        assert_eq!(
            Ok((Instruction::Insert(&b"\x00"[..]), 2)),
            decode_instruction(&b"\x01\x00"[..], 0)
        );
        assert_eq!(
            Ok((Instruction::Insert(&b"\x01"[..]), 2)),
            decode_instruction(&b"\x01\x01"[..], 0)
        );
        assert_eq!(
            Ok((Instruction::Insert(&b"\xff\x05"[..]), 3)),
            decode_instruction(&b"\x02\xff\x05"[..], 0)
        );
    }
}
