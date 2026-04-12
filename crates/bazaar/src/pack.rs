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
}

impl std::fmt::Display for PackError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PackError::InvalidName(n) => write!(f, "{:?} is not a valid name.", n),
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
}
