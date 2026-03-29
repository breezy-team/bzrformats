use std::path::{Path, PathBuf};
use unicode_normalization::{is_nfc, UnicodeNormalization};

pub fn is_inside(dir: &Path, fname: &Path) -> bool {
    fname.starts_with(dir)
}

pub fn is_inside_any(dir_list: &[&Path], fname: &Path) -> bool {
    for dirname in dir_list {
        if is_inside(dirname, fname) {
            return true;
        }
    }
    false
}

pub fn parent_directories(path: &Path) -> impl Iterator<Item = &Path> {
    let mut path = path;
    std::iter::from_fn(move || {
        if let Some(parent) = path.parent() {
            path = parent;
            if path.parent().is_none() {
                None
            } else {
                Some(path)
            }
        } else {
            None
        }
    })
}

#[derive(Debug)]
pub struct InvalidPathSegmentError(pub String);

pub fn splitpath(p: &str) -> std::result::Result<Vec<&str>, InvalidPathSegmentError> {
    #[cfg(windows)]
    let split = |c| c == '/' || c == '\\';
    #[cfg(not(windows))]
    let split = |c| c == '/';

    let mut rps = Vec::new();
    for f in p.split(split) {
        if f == ".." {
            return Err(InvalidPathSegmentError(f.to_string()));
        } else if f == "." || f.is_empty() {
            continue;
        } else {
            rps.push(f);
        }
    }

    Ok(rps)
}

pub fn accessible_normalized_filename(path: &Path) -> Option<(PathBuf, bool)> {
    path.to_str().map(|path_str| {
        if is_nfc(path_str) {
            (path.to_path_buf(), true)
        } else {
            (PathBuf::from(path_str.nfc().collect::<String>()), true)
        }
    })
}

pub fn inaccessible_normalized_filename(path: &Path) -> Option<(PathBuf, bool)> {
    path.to_str().map(|path_str| {
        if is_nfc(path_str) {
            (path.to_path_buf(), true)
        } else {
            let normalized_path = path_str.nfc().collect::<String>();
            let accessible = normalized_path == path_str;
            (PathBuf::from(normalized_path), accessible)
        }
    })
}

#[cfg(target_os = "macos")]
pub fn normalized_filename(path: &Path) -> Option<(PathBuf, bool)> {
    accessible_normalized_filename(path)
}

#[cfg(not(target_os = "macos"))]
pub fn normalized_filename(path: &Path) -> Option<(PathBuf, bool)> {
    inaccessible_normalized_filename(path)
}

pub fn normalizes_filenames() -> bool {
    #[cfg(target_os = "macos")]
    return true;

    #[cfg(not(target_os = "macos"))]
    return false;
}
