//! CHK map leaf-node codec: serialise a batch of items and deserialise the
//! result. These run on every CHK inventory read/write.

use bazaar::chk_map::{deserialise_leaf_node, serialise_leaf_node};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

/// `n` items keyed `aaa{i}` so they share the common prefix `a`, with realistic
/// inventory-style values.
fn make_items(n: usize) -> Vec<(Vec<Vec<u8>>, Vec<u8>)> {
    (0..n)
        .map(|i| {
            let key = vec![format!("aaa{i:06}").into_bytes()];
            let value = format!("file\x00{i:06}\x00dir\x00{i}\x00abc123sha\n").into_bytes();
            (key, value)
        })
        .collect()
}

fn serialise(items: &[(Vec<Vec<u8>>, Vec<u8>)]) -> Vec<u8> {
    serialise_leaf_node(usize::MAX, 1, items, Some(b"a"))
        .unwrap()
        .concat()
}

fn bench_serialise(c: &mut Criterion) {
    let mut group = c.benchmark_group("chk/serialise_leaf");
    for &n in &[16usize, 64, 256] {
        let items = make_items(n);
        let bytes = serialise(&items).len() as u64;
        group.throughput(Throughput::Bytes(bytes));
        group.bench_with_input(BenchmarkId::from_parameter(n), &items, |b, items| {
            b.iter(|| black_box(serialise(black_box(items))));
        });
    }
    group.finish();
}

fn bench_deserialise(c: &mut Criterion) {
    let mut group = c.benchmark_group("chk/deserialise_leaf");
    for &n in &[16usize, 64, 256] {
        let blob = serialise(&make_items(n));
        group.throughput(Throughput::Bytes(blob.len() as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &blob, |b, blob| {
            b.iter(|| black_box(deserialise_leaf_node(black_box(blob)).unwrap()));
        });
    }
    group.finish();
}

criterion_group!(benches, bench_serialise, bench_deserialise);
criterion_main!(benches);
