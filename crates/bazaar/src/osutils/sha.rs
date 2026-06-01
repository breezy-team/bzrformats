use sha1::{Digest, Sha1};
use std::fs::File;
use std::io::Read;
use std::path::Path;

const BUFSIZE: usize = 128 << 10;

/// Encode bytes as a lowercase hex string.
pub(crate) fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        s.push(char::from_digit((byte >> 4) as u32, 16).unwrap());
        s.push(char::from_digit((byte & 0x0f) as u32, 16).unwrap());
    }
    s
}

pub fn sha_file(f: &mut dyn Read) -> Result<String, std::io::Error> {
    let mut s = Sha1::new();
    let mut buffer = [0; BUFSIZE];
    loop {
        let bytes_read = f.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        s.update(&buffer[..bytes_read]);
    }
    Ok(to_hex(&s.finalize()))
}

pub fn size_sha_file(f: &mut dyn Read) -> Result<(usize, String), std::io::Error> {
    let mut s = Sha1::new();
    let mut buffer = [0; BUFSIZE];
    let mut size: usize = 0;
    loop {
        let bytes_read = f.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }
        s.update(&buffer[..bytes_read]);
        size += bytes_read;
    }
    Ok((size, to_hex(&s.finalize())))
}

pub fn size_sha_chunks(chunks: impl Iterator<Item = Vec<u8>>) -> (usize, String) {
    let mut s = Sha1::new();
    let mut size: usize = 0;
    for chunk in chunks {
        s.update(&chunk);
        size += chunk.len();
    }
    (size, to_hex(&s.finalize()))
}

pub fn sha_file_by_name<P: AsRef<Path>>(path: P) -> Result<String, std::io::Error> {
    let mut f = File::open(path)?;
    sha_file(&mut f)
}

pub fn sha_chunks<I, S>(strings: I) -> String
where
    I: IntoIterator<Item = S>,
    S: AsRef<[u8]>,
{
    let mut s = Sha1::new();
    for string in strings {
        s.update(string.as_ref());
    }
    to_hex(&s.finalize())
}

pub fn sha_string(string: &[u8]) -> String {
    let mut s = Sha1::new();
    s.update(string);

    to_hex(&s.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    const HELLO_WORLD_SHA1: &str = "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed";

    #[test]
    fn test_to_hex() {
        assert_eq!(to_hex(&[0x00, 0x0f, 0xff, 0xa5]), "000fffa5");
        assert_eq!(to_hex(&[]), "");
    }

    #[test]
    fn test_sha_string() {
        assert_eq!(sha_string(b"hello world"), HELLO_WORLD_SHA1);
        assert_eq!(sha_string(b""), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
    }

    #[test]
    fn test_sha_chunks() {
        assert_eq!(
            sha_chunks([b"hello".as_slice(), b" ", b"world"]),
            HELLO_WORLD_SHA1
        );
        // Splitting differently yields the same digest.
        assert_eq!(sha_chunks([b"hello world".as_slice()]), HELLO_WORLD_SHA1);
    }

    #[test]
    fn test_sha_file() {
        let mut f = Cursor::new(b"hello world".to_vec());
        assert_eq!(sha_file(&mut f).unwrap(), HELLO_WORLD_SHA1);
    }

    #[test]
    fn test_size_sha_file() {
        let mut f = Cursor::new(b"hello world".to_vec());
        assert_eq!(
            size_sha_file(&mut f).unwrap(),
            (11, HELLO_WORLD_SHA1.into())
        );
    }

    #[test]
    fn test_size_sha_chunks() {
        let chunks = vec![b"hello".to_vec(), b" ".to_vec(), b"world".to_vec()];
        assert_eq!(
            size_sha_chunks(chunks.into_iter()),
            (11, HELLO_WORLD_SHA1.into())
        );
    }
}
