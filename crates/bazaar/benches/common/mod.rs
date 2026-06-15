//! Shared fixtures for the bazaar benchmarks.
//!
//! Texts are generated deterministically (no rng) so benchmark runs are
//! comparable across machines and over time.

/// A pseudo-source-file of `lines` lines, seeded by `seed`. Lines repeat in a
/// way that gives realistic redundancy without being trivially compressible.
pub fn make_text(seed: u64, lines: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(lines * 40);
    let mut state = seed.wrapping_mul(2654435761).wrapping_add(1);
    for i in 0..lines {
        // Cheap xorshift to vary line content deterministically.
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let word = state % 5000;
        out.extend_from_slice(
            format!("line {i:06} token_{word} the quick brown fox jumps\n").as_bytes(),
        );
    }
    out
}

/// A family of `count` texts that all derive from a common base by mutating a
/// few lines each. This mimics successive revisions of one file, the case
/// group compression is built to exploit.
pub fn make_revisions(base_lines: usize, count: usize) -> Vec<Vec<u8>> {
    let base = make_text(0, base_lines);
    let base_lines_vec: Vec<&[u8]> = base.split_inclusive(|&b| b == b'\n').collect();
    let mut revisions = Vec::with_capacity(count);
    for rev in 0..count {
        let mut lines: Vec<Vec<u8>> = base_lines_vec.iter().map(|l| l.to_vec()).collect();
        // Touch ~2% of lines per revision, spread across the file.
        let touched = (base_lines / 50).max(1);
        for t in 0..touched {
            let idx = (rev.wrapping_mul(2654435761).wrapping_add(t * 97)) % lines.len();
            lines[idx] = format!("line {idx:06} EDITED_rev{rev}_n{t}\n").into_bytes();
        }
        revisions.push(lines.concat());
    }
    revisions
}
