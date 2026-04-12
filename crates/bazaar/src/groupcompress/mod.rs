pub mod block;
pub mod compressor;
pub mod delta;
pub mod line_delta;
pub mod rabin_delta;
pub mod sort;
use sha1::{Digest as _, Sha1};

lazy_static::lazy_static! {
    pub static ref NULL_SHA1: Vec<u8> = format!("{:x}", Sha1::new().finalize()).as_bytes().to_vec();
}
