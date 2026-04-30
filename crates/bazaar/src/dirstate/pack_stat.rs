//! Stat-packing: the base64-encoded 24-byte record dirstate stores
//! per entry as a fingerprint of the filesystem `lstat` result.

use base64::Engine;
use std::fs::Metadata;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

#[cfg(unix)]
pub fn pack_stat_metadata(metadata: &Metadata) -> String {
    pack_stat(
        metadata.len(),
        metadata
            .modified()
            .unwrap()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        metadata
            .created()
            .unwrap()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        metadata.dev(),
        metadata.ino(),
        metadata.mode(),
    )
}

#[cfg(windows)]
pub fn pack_stat_metadata(metadata: &Metadata) -> String {
    pack_stat(
        metadata.len(),
        metadata
            .modified()
            .unwrap()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        metadata
            .created()
            .unwrap()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
        0,
        0,
        0,
    )
}

pub fn pack_stat(size: u64, mtime: u64, ctime: u64, dev: u64, ino: u64, mode: u32) -> String {
    let size = size & 0xFFFFFFFF;
    let mtime = mtime & 0xFFFFFFFF;
    let ctime = ctime & 0xFFFFFFFF;
    let dev = dev & 0xFFFFFFFF;
    let ino = ino & 0xFFFFFFFF;

    let packed_data = [
        (size >> 24) as u8,
        (size >> 16) as u8,
        (size >> 8) as u8,
        size as u8,
        (mtime >> 24) as u8,
        (mtime >> 16) as u8,
        (mtime >> 8) as u8,
        mtime as u8,
        (ctime >> 24) as u8,
        (ctime >> 16) as u8,
        (ctime >> 8) as u8,
        ctime as u8,
        (dev >> 24) as u8,
        (dev >> 16) as u8,
        (dev >> 8) as u8,
        dev as u8,
        (ino >> 24) as u8,
        (ino >> 16) as u8,
        (ino >> 8) as u8,
        ino as u8,
        (mode >> 24) as u8,
        (mode >> 16) as u8,
        (mode >> 8) as u8,
        mode as u8,
    ];

    base64::engine::general_purpose::STANDARD_NO_PAD.encode(packed_data)
}

pub fn stat_to_minikind(metadata: &Metadata) -> char {
    let file_type = metadata.file_type();
    if file_type.is_dir() {
        'd'
    } else if file_type.is_file() {
        'f'
    } else if file_type.is_symlink() {
        'l'
    } else {
        panic!("Unsupported file type");
    }
}
