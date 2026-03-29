use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyList, PyModule};
use std::path::{Path, PathBuf};

#[pyfunction]
fn split_lines<'a>(py: Python<'a>, text: &'a [u8]) -> PyResult<Bound<'a, PyList>> {
    let ret = PyList::empty(py);
    for line in osutils::split_lines(text) {
        let line_bytes = PyBytes::new(py, &line);
        ret.append(line_bytes)?;
    }
    Ok(ret)
}

#[pyfunction]
fn rand_chars(num: usize) -> PyResult<String> {
    Ok(osutils::rand_chars(num))
}

#[pyfunction]
fn is_inside(dir: &str, fname: &str) -> PyResult<bool> {
    let dir_path = Path::new(dir);
    let fname_path = Path::new(fname);
    Ok(osutils::path::is_inside(dir_path, fname_path))
}

#[pyfunction]
fn is_inside_any(dir_list: Vec<String>, fname: &str) -> PyResult<bool> {
    let dir_paths: Vec<&Path> = dir_list.iter().map(|d| Path::new(d.as_str())).collect();
    let fname_path = Path::new(fname);
    Ok(osutils::path::is_inside_any(&dir_paths, fname_path))
}

#[pyfunction]
fn parent_directories(path: &str) -> PyResult<Vec<String>> {
    let path_obj = Path::new(path);
    let parents: Vec<String> = osutils::path::parent_directories(path_obj)
        .map(|p| p.to_string_lossy().to_string())
        .collect();
    Ok(parents)
}

// Walkdirs implementation - simplified version for basic functionality
#[pyfunction]
fn walkdirs_utf8(top: &str) -> PyResult<Vec<(String, Vec<(String, String, u64, String)>)>> {
    use std::fs;
    use std::os::unix::fs::MetadataExt;

    let mut results = Vec::new();
    let walk = walkdir::WalkDir::new(top).follow_links(false);

    for entry in walk {
        let entry = entry.map_err(|e| pyo3::exceptions::PyIOError::new_err(e.to_string()))?;
        let path = entry.path();

        if path.is_dir() {
            let mut dir_entries = Vec::new();

            // Read directory contents
            if let Ok(read_dir) = fs::read_dir(path) {
                for dir_entry in read_dir.flatten() {
                    let name = dir_entry.file_name().to_string_lossy().to_string();
                    let metadata = dir_entry.metadata();

                    if let Ok(metadata) = metadata {
                        let kind = if metadata.is_dir() {
                            "directory"
                        } else if metadata.is_symlink() {
                            "symlink"
                        } else {
                            "file"
                        };

                        let size = metadata.len();
                        let utf8path = dir_entry.path().to_string_lossy().to_string();

                        dir_entries.push((name, kind.to_string(), size, utf8path));
                    }
                }
            }

            results.push((path.to_string_lossy().to_string(), dir_entries));
        }
    }

    Ok(results)
}

#[pyfunction]
fn normalizes_filenames() -> bool {
    osutils::path::normalizes_filenames()
}

#[pyfunction]
fn supports_symlinks(path: PathBuf) -> Option<bool> {
    osutils::mounts::supports_symlinks(path)
}

#[pymodule]
fn _osutils_rs(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(split_lines, m)?)?;
    m.add_function(wrap_pyfunction!(rand_chars, m)?)?;
    m.add_function(wrap_pyfunction!(is_inside, m)?)?;
    m.add_function(wrap_pyfunction!(is_inside_any, m)?)?;
    m.add_function(wrap_pyfunction!(parent_directories, m)?)?;
    m.add_function(wrap_pyfunction!(walkdirs_utf8, m)?)?;
    m.add_function(wrap_pyfunction!(normalizes_filenames, m)?)?;
    m.add_function(wrap_pyfunction!(supports_symlinks, m)?)?;
    Ok(())
}
