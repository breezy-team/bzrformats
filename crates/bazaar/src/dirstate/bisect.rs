//! Bisect primitives used by `DirState::bisect` /
//! `bisect_dirblocks` / `bisect_recursive` to look up dirstate rows
//! without reading the full file.
//!
//! These operate on a `read_range` closure that returns arbitrary
//! byte windows of the dirstate file, parse rows out of each window,
//! and narrow in on the target keys.

use super::{fields_per_entry, split_path_utf8, Entry, EntryKey, Kind, TreeData, BISECT_PAGE_SIZE};

/// Shared bisect mode: match by full path (dirname/basename) or by
/// dirname only.
#[derive(Copy, Clone, PartialEq, Eq)]
pub(super) enum BisectMode {
    /// Input keys are `dirname/basename` strings; match against the
    /// concatenation `fields[1]/fields[2]` (or `fields[2]` if
    /// `fields[1]` is empty).  Used by `bisect`.
    Paths,
    /// Input keys are dirnames; match against `fields[1]` directly.
    /// Used by `bisect_dirblocks`.
    Dirnames,
}

/// Error returned by the bisect primitives.
#[derive(Debug, PartialEq, Eq)]
pub enum BisectError {
    /// The caller's `read_range` closure reported a failure.
    ReadError(String),
    /// The bisect loop exceeded its safety counter.  Mirrors Python's
    /// `BzrFormatsError("Too many seeks, most likely a bug.")`.
    TooManySeeks,
    /// An entry row's size field could not be parsed as an integer.
    BadSize(String),
    /// An entry row's minikind field wasn't one of the six valid codes.
    BadMinikind(u8),
}

impl std::fmt::Display for BisectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BisectError::ReadError(s) => write!(f, "read error: {}", s),
            BisectError::TooManySeeks => write!(f, "too many seeks"),
            BisectError::BadSize(s) => write!(f, "bad size field: {}", s),
            BisectError::BadMinikind(b) => write!(f, "invalid minikind byte {:?}", b),
        }
    }
}

impl std::error::Error for BisectError {}

pub(super) fn bisect_bytes<F>(
    end_of_header: u64,
    file_size: u64,
    num_present_parents: usize,
    keys: Vec<Vec<u8>>,
    mode: BisectMode,
    read_range: &mut F,
) -> Result<std::collections::HashMap<Vec<u8>, Vec<Entry>>, BisectError>
where
    F: FnMut(u64, usize) -> Result<Vec<u8>, BisectError>,
{
    let mut found: std::collections::HashMap<Vec<u8>, Vec<Entry>> =
        std::collections::HashMap::new();
    if keys.is_empty() || file_size == 0 {
        return Ok(found);
    }

    // Each entry has one extra trailing empty field because of the
    // terminating newline-NUL split: fields_per_entry accounts for the
    // trailing `\n` slot already, and we need one more for the empty
    // leading field produced by the record separator. The Python code
    // keeps them in the same count constant.
    let entry_field_count = fields_per_entry(num_present_parents) + 1;

    // Sort keys so the bisect_left/right calls below can rely on
    // ordered input.  (Python callers sort beforehand; we defensively
    // sort too.)
    let mut sorted_keys: Vec<Vec<u8>> = keys;
    sorted_keys.sort();
    sorted_keys.dedup();

    let max_count = 30 * sorted_keys.len();
    let mut count = 0usize;

    let low0 = end_of_header;
    let high0 = file_size.saturating_sub(1);
    let mut pending: Vec<(u64, u64, Vec<Vec<u8>>)> = vec![(low0, high0, sorted_keys)];
    let mut page_size: usize = BISECT_PAGE_SIZE;

    while let Some((low, high, cur_keys)) = pending.pop() {
        if cur_keys.is_empty() || low >= high {
            continue;
        }

        count += 1;
        if count > max_count {
            return Err(BisectError::TooManySeeks);
        }

        // `mid` biases toward reading from the *start* of a page-sized
        // window, matching Python's `(low + high - page_size) // 2`
        // calculation.
        let mid_i = ((low + high) as i64 - page_size as i64) / 2;
        let mid = if mid_i < low as i64 {
            low
        } else {
            mid_i as u64
        };
        let read_size = std::cmp::min(page_size as u64, (high - mid) + 1) as usize;
        let block = read_range(mid, read_size)?;

        let entries: Vec<&[u8]> = block.split(|&b| b == b'\n').collect();

        if entries.len() < 2 {
            page_size *= 2;
            pending.push((low, high, cur_keys));
            continue;
        }

        let mut start = mid;
        let mut first_entry_num: usize = 0;
        let mut first_fields: Vec<&[u8]> = entries[0].split(|&b| b == 0u8).collect();
        if first_fields.len() < entry_field_count {
            start += entries[0].len() as u64 + 1;
            first_entry_num = 1;
            first_fields = entries[1].split(|&b| b == 0u8).collect();
        }

        let first_threshold = match mode {
            BisectMode::Paths => 2,
            BisectMode::Dirnames => 1,
        };
        if first_fields.len() <= first_threshold {
            page_size *= 2;
            pending.push((low, high, cur_keys));
            continue;
        }

        let first_key: Vec<u8> = match mode {
            BisectMode::Paths => {
                if !first_fields[1].is_empty() {
                    let mut p = first_fields[1].to_vec();
                    p.push(b'/');
                    p.extend_from_slice(first_fields[2]);
                    p
                } else {
                    first_fields[2].to_vec()
                }
            }
            BisectMode::Dirnames => first_fields[1].to_vec(),
        };

        let first_loc = match mode {
            BisectMode::Paths => bisect_path_left_bytes(&cur_keys, &first_key),
            BisectMode::Dirnames => bisect_bytes_left(&cur_keys, &first_key),
        };
        let pre: Vec<Vec<u8>> = cur_keys[..first_loc].to_vec();
        let post: Vec<Vec<u8>> = cur_keys[first_loc..].to_vec();
        let mut after = start;

        let mut pre_out = pre;
        let mut post_out = post;
        if !post_out.is_empty() && first_fields.len() >= entry_field_count {
            let mut last_entry_num = entries.len() - 1;
            let mut last_fields: Vec<&[u8]> =
                entries[last_entry_num].split(|&b| b == 0u8).collect();
            if last_fields.len() < entry_field_count {
                after = mid + (block.len() as u64) - (entries[entries.len() - 1].len() as u64);
                last_entry_num -= 1;
                last_fields = entries[last_entry_num].split(|&b| b == 0u8).collect();
            } else {
                after = mid + block.len() as u64;
            }

            let last_key: Vec<u8> = match mode {
                BisectMode::Paths => {
                    if !last_fields[1].is_empty() {
                        let mut p = last_fields[1].to_vec();
                        p.push(b'/');
                        p.extend_from_slice(last_fields[2]);
                        p
                    } else {
                        last_fields[2].to_vec()
                    }
                }
                BisectMode::Dirnames => last_fields[1].to_vec(),
            };

            let last_loc = match mode {
                BisectMode::Paths => bisect_path_right_bytes(&post_out, &last_key),
                BisectMode::Dirnames => bisect_bytes_right(&post_out, &last_key),
            };
            let middle: Vec<Vec<u8>> = post_out[..last_loc].to_vec();
            post_out = post_out[last_loc..].to_vec();

            if !middle.is_empty() {
                if middle.first() == Some(&first_key) {
                    pre_out.push(first_key.clone());
                }
                if middle.last() == Some(&last_key) {
                    post_out.insert(0, last_key.clone());
                }

                // Map keys in this page to their parsed field rows.
                let mut page_paths: std::collections::HashMap<Vec<u8>, Vec<Vec<Vec<u8>>>> =
                    std::collections::HashMap::new();
                page_paths
                    .entry(first_key.clone())
                    .or_default()
                    .push(first_fields.iter().map(|s| s.to_vec()).collect());
                if last_entry_num != first_entry_num {
                    page_paths
                        .entry(last_key.clone())
                        .or_default()
                        .push(last_fields.iter().map(|s| s.to_vec()).collect());
                }
                for num in (first_entry_num + 1)..last_entry_num {
                    let fields: Vec<&[u8]> = entries[num].split(|&b| b == 0u8).collect();
                    let key: Vec<u8> = match mode {
                        BisectMode::Paths => {
                            if !fields[1].is_empty() {
                                let mut p = fields[1].to_vec();
                                p.push(b'/');
                                p.extend_from_slice(fields[2]);
                                p
                            } else {
                                fields[2].to_vec()
                            }
                        }
                        BisectMode::Dirnames => fields[1].to_vec(),
                    };
                    page_paths
                        .entry(key)
                        .or_default()
                        .push(fields.iter().map(|s| s.to_vec()).collect());
                }

                for key in &middle {
                    if let Some(rows) = page_paths.get(key) {
                        for row in rows {
                            let entry = fields_to_entry(&row[1..], num_present_parents)?;
                            found.entry(key.clone()).or_default().push(entry);
                        }
                    }
                }
            }
        }

        if !post_out.is_empty() {
            pending.push((after, high, post_out));
        }
        if !pre_out.is_empty() {
            pending.push((low, start.saturating_sub(1), pre_out));
        }
    }

    Ok(found)
}

fn fields_to_entry(fields: &[Vec<u8>], num_present_parents: usize) -> Result<Entry, BisectError> {
    let key = EntryKey {
        dirname: fields[0].clone(),
        basename: fields[1].clone(),
        file_id: fields[2].clone(),
    };
    let tree_count = 1 + num_present_parents;
    let mut trees = Vec::with_capacity(tree_count);
    for t in 0..tree_count {
        let base = 3 + 5 * t;
        let minikind_byte = fields[base].first().copied().unwrap_or(0);
        let minikind = Kind::from_minikind(minikind_byte).map_err(BisectError::BadMinikind)?;
        let fingerprint = fields[base + 1].clone();
        let size_str = std::str::from_utf8(&fields[base + 2])
            .map_err(|e| BisectError::BadSize(e.to_string()))?;
        let size: u64 = size_str
            .parse()
            .map_err(|e: std::num::ParseIntError| BisectError::BadSize(e.to_string()))?;
        let executable = fields[base + 3].first() == Some(&b'y');
        let packed_stat = fields[base + 4].clone();
        trees.push(TreeData {
            minikind,
            fingerprint,
            size,
            executable,
            packed_stat,
        });
    }
    Ok(Entry { key, trees })
}

fn bisect_bytes_left(keys: &[Vec<u8>], needle: &[u8]) -> usize {
    let mut lo = 0;
    let mut hi = keys.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        if keys[mid].as_slice() < needle {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

fn bisect_bytes_right(keys: &[Vec<u8>], needle: &[u8]) -> usize {
    let mut lo = 0;
    let mut hi = keys.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        if needle < keys[mid].as_slice() {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    lo
}

/// Byte-slice variants of `bisect_path_left` / `bisect_path_right`
/// that compare by dirblock (component-wise split on `/`), used by
/// the bisect parser.
fn bisect_path_left_bytes(keys: &[Vec<u8>], needle: &[u8]) -> usize {
    let mut lo = 0;
    let mut hi = keys.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        if cmp_path_by_dirblock(&keys[mid], needle).is_lt() {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

fn bisect_path_right_bytes(keys: &[Vec<u8>], needle: &[u8]) -> usize {
    let mut lo = 0;
    let mut hi = keys.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        if cmp_path_by_dirblock(needle, &keys[mid]).is_lt() {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    lo
}

fn cmp_path_by_dirblock(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    let (a_dir, a_base) = split_path_utf8(a);
    let (b_dir, b_base) = split_path_utf8(b);
    let dir_ord = cmp_by_dirs_bytes(a_dir, b_dir);
    if dir_ord != std::cmp::Ordering::Equal {
        return dir_ord;
    }
    a_base.cmp(b_base)
}

pub(super) fn cmp_by_dirs_bytes(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    let mut ai = a.split(|&c| c == b'/');
    let mut bi = b.split(|&c| c == b'/');
    loop {
        match (ai.next(), bi.next()) {
            (None, None) => return std::cmp::Ordering::Equal,
            (None, Some(_)) => return std::cmp::Ordering::Less,
            (Some(_), None) => return std::cmp::Ordering::Greater,
            (Some(x), Some(y)) => match x.cmp(y) {
                std::cmp::Ordering::Equal => continue,
                other => return other,
            },
        }
    }
}
