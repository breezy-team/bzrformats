use sha1::{Digest, Sha1};
use std::fs::File;
use std::io::Read;
use std::path::Path;

const BUFSIZE: usize = 128 << 10;

/// Encode bytes as a lowercase hex string.
fn to_hex(bytes: &[u8]) -> String {
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
