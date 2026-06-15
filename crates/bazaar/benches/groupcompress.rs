//! End-to-end group-compression benchmarks: feeding a family of related
//! revisions through `RabinGroupCompressor` (the common ratio-vs-time path) and
//! extracting one record back out.

use bazaar::groupcompress::compressor::{GroupCompressor, RabinGroupCompressor};
use bazaar::versionedfile::Key;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

#[path = "common/mod.rs"]
mod common;
use common::make_revisions;

fn compress_revisions(revs: &[Vec<u8>]) -> RabinGroupCompressor {
    let mut gc = RabinGroupCompressor::new(None);
    for (i, rev) in revs.iter().enumerate() {
        let key = Key::Fixed(vec![format!("rev-{i}").into_bytes()]);
        gc.compress(&key, &[rev.as_slice()], rev.len(), None, None, None)
            .unwrap();
    }
    gc
}

fn bench_compress(c: &mut Criterion) {
    let mut group = c.benchmark_group("groupcompress/compress");
    for &count in &[16usize, 64, 256] {
        let revs = make_revisions(500, count);
        let total: u64 = revs.iter().map(|r| r.len() as u64).sum();
        group.throughput(Throughput::Bytes(total));
        group.bench_with_input(BenchmarkId::from_parameter(count), &revs, |b, revs| {
            b.iter(|| black_box(compress_revisions(black_box(revs))));
        });
    }
    group.finish();
}

fn bench_extract(c: &mut Criterion) {
    let revs = make_revisions(500, 64);
    let gc = compress_revisions(&revs);
    // Extract a record in the middle, which must replay deltas back to a
    // fulltext, the worst realistic read path.
    let target_key: Vec<Vec<u8>> = vec![b"rev-32".to_vec()];
    c.bench_function("groupcompress/extract_mid", |b| {
        b.iter(|| black_box(gc.extract(black_box(&target_key)).unwrap()));
    });
}

criterion_group!(benches, bench_compress, bench_extract);
criterion_main!(benches);
