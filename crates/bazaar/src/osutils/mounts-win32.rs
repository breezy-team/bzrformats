use std::ffi::OsStr;
use std::os::windows::ffi::{OsStrExt, OsStringExt};
use std::path::Path;
use std::ptr;
use winapi::shared::minwindef::DWORD;
use winapi::um::fileapi::GetVolumeInformationW;

fn _get_fs_type(drive: &str) -> Option<String> {
    const MAX_FS_TYPE_LENGTH: DWORD = 16;
    let mut fs_type = vec![0u16; (MAX_FS_TYPE_LENGTH + 1) as usize];
    let res = unsafe {
        GetVolumeInformationW(
            OsStr::new(drive)
                .encode_wide()
                .chain(std::iter::once(0))
                .collect::<Vec<u16>>()
                .as_ptr(),
            ptr::null_mut(),
            0,
            ptr::null_mut(),
            ptr::null_mut(),
            ptr::null_mut(),
            fs_type.as_mut_ptr(),
            MAX_FS_TYPE_LENGTH,
        )
    };
    if res != 0 {
        let fs_type_os = std::ffi::OsString::from_wide(&fs_type[..]);
        let fs_type_str = fs_type_os.to_str().unwrap_or_default();
        Some(fs_type_str.to_owned())
    } else {
        None
    }
}

pub fn get_fs_type<P: AsRef<Path>>(path: P) -> Option<String> {
    let drive = path
        .as_ref()
        .parent()
        .and_then(|p| p.to_str())
        .unwrap_or_default();
    let drive = if drive.contains(':') {
        drive
    } else {
        &format!("{}\\", drive)
    };
    let fs_type = _get_fs_type(drive)?;
    Some(match fs_type.as_str() {
        "FAT32" => String::from("vfat"),
        "NTFS" => String::from("ntfs"),
        _ => fs_type,
    })
}

pub fn supports_symlinks<P: AsRef<Path>>(path: P) -> Option<bool> {
    let fs_type = get_fs_type(path)?;
    match fs_type.as_str() {
        "ntfs" => Some(true),
        "vfat" => Some(false),
        _ => Some(false),
    }
}

pub fn supports_posix_readonly() -> bool {
    false
}
