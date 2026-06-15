//! Benchmarks for the rabin delta primitives that underpin group compression.
//!
//! The `incremental_index` benchmark is the canary for the quadratic trap
//! documented in the OwningDeltaIndex: indexing N sources should stay linear in
//! total bytes, not rebuild the index per source.

use bazaar::groupcompress::rabin_delta::{make_delta, OwningDeltaIndex};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

#[path = "common/mod.rs"]
mod common;
use common::{make_revisions, make_text};

fn bench_make_delta(c: &mut Criterion) {
    let mut group = c.benchmark_group("rabin/make_delta");
    for &lines in &[200usize, 1000, 4000] {
        let source = make_text(1, lines);
        // Target shares most of source but with edits, the realistic delta case.
        let revs = make_revisions(lines, 2);
        let target = &revs[1];
        group.throughput(Throughput::Bytes(target.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(lines), &lines, |b, _| {
            b.iter(|| black_box(make_delta(black_box(&source), black_box(target))));
        });
    }
    group.finish();
}

fn bench_incremental_index(c: &mut Criterion) {
    let mut group = c.benchmark_group("rabin/incremental_index");
    // Feed `n` related sources into one index, then delta a new target. Wall
    // time should grow roughly linearly with n if indexing is incremental.
    for &n in &[8usize, 32, 128] {
        let revs = make_revisions(500, n + 1);
        let total: usize = revs.iter().take(n).map(|r| r.len()).sum();
        group.throughput(Throughput::Bytes(total as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let mut idx = OwningDeltaIndex::new(None);
                for rev in revs.iter().take(n) {
                    idx.add_source(rev.clone(), 0);
                }
                // max_delta_size 0 means "no limit".
                black_box(idx.make_delta(black_box(&revs[n]), 0).unwrap());
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_make_delta, bench_incremental_index);
criterion_main!(benches);
