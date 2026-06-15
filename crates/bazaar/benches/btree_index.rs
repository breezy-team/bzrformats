//! B+Tree index hot paths: parsing a leaf node body and the multi-key bisect
//! used to locate keys across a sorted page.

use bazaar::btree_index::{multi_bisect_right, parse_leaf_lines, LeafKey};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

/// Build a serialised leaf-node body with `n` zero-ref entries, sorted by key.
fn make_leaf_body(n: usize) -> Vec<u8> {
    let mut body = Vec::from(&b"type=leaf\n"[..]);
    for i in 0..n {
        // `key\0<empty refs>\0value`: with no ref lists the refs area between
        // the trailing NULs is empty (matches the in-tree fixtures).
        body.extend_from_slice(format!("key{i:08}\0\0value-payload-{i:08}\n").as_bytes());
    }
    body
}

fn bench_parse_leaf(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree/parse_leaf");
    for &n in &[64usize, 256, 1024] {
        let body = make_leaf_body(n);
        group.throughput(Throughput::Bytes(body.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &body, |b, body| {
            b.iter(|| black_box(parse_leaf_lines(black_box(body), 1, 0).unwrap()));
        });
    }
    group.finish();
}

fn bench_multi_bisect(c: &mut Criterion) {
    let mut group = c.benchmark_group("btree/multi_bisect_right");
    for &n in &[256usize, 1024, 4096] {
        let fixed: Vec<LeafKey> = (0..n)
            .map(|i| vec![format!("key{i:08}").into_bytes()])
            .collect();
        // Look up a quarter of the keys, interleaved with misses.
        let in_keys: Vec<LeafKey> = (0..n)
            .step_by(4)
            .map(|i| vec![format!("key{i:08}x").into_bytes()])
            .collect();
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| black_box(multi_bisect_right(black_box(&in_keys), black_box(&fixed))));
        });
    }
    group.finish();
}

criterion_group!(benches, bench_parse_leaf, bench_multi_bisect);
criterion_main!(benches);
