//! Read-path benchmark: populate a groupcompress store with many related
//! revisions, then read every record back via get_record_stream (the fetch /
//! checkout read path).

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use bazaar::groupcompress::gcvf::{
    GcAccess, GcBuildDetails, GcIndex, GroupCompressVersionedFiles, IndexMemo, ReadMemo,
};
use bazaar::knit::KnitError;
use bazaar::versionedfile::{ChunkedContentFactory, ContentFactory, Key};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

#[path = "common/mod.rs"]
mod common;

#[derive(Default)]
struct Store {
    blocks: Vec<Vec<u8>>,
    details: HashMap<Key, GcBuildDetails<String>>,
}

#[derive(Clone)]
struct MemIndex(Rc<RefCell<Store>>);
#[derive(Clone)]
struct MemAccess(Rc<RefCell<Store>>);

impl GcIndex for MemIndex {
    type F = String;
    fn get_build_details(
        &self,
        keys: &[Key],
    ) -> Result<HashMap<Key, GcBuildDetails<String>>, KnitError> {
        let store = self.0.borrow();
        Ok(keys
            .iter()
            .filter_map(|k| store.details.get(k).map(|d| (k.clone(), d.clone())))
            .collect())
    }
    fn get_parent_map(&self, keys: &[Key]) -> Result<HashMap<Key, Vec<Key>>, KnitError> {
        let store = self.0.borrow();
        Ok(keys
            .iter()
            .filter_map(|k| {
                store
                    .details
                    .get(k)
                    .map(|d| (k.clone(), d.parents.clone().unwrap_or_default()))
            })
            .collect())
    }
    fn keys(&self) -> Result<Vec<Key>, KnitError> {
        Ok(self.0.borrow().details.keys().cloned().collect())
    }
    fn has_graph(&self) -> bool {
        true
    }
    fn check_write_ok(&self) -> Result<(), KnitError> {
        Ok(())
    }
    fn add_records(
        &self,
        records: &[(Key, IndexMemo<String>, Option<Vec<Key>>)],
        _random_id: bool,
    ) -> Result<(), KnitError> {
        let mut store = self.0.borrow_mut();
        for (key, memo, parents) in records {
            store.details.insert(
                key.clone(),
                GcBuildDetails {
                    index_memo: memo.clone(),
                    parents: parents.clone(),
                },
            );
        }
        Ok(())
    }
}

impl GcAccess for MemAccess {
    type F = String;
    fn get_raw_records(&self, memos: &[ReadMemo<String>]) -> Result<Vec<Vec<u8>>, KnitError> {
        let store = self.0.borrow();
        memos
            .iter()
            .map(|m| {
                let idx: usize = m
                    .index
                    .parse()
                    .map_err(|_| KnitError::Corrupt("bad block index".into()))?;
                store
                    .blocks
                    .get(idx)
                    .cloned()
                    .ok_or_else(|| KnitError::Corrupt("no such block".into()))
            })
            .collect()
    }
    fn add_raw_record(
        &self,
        _size: usize,
        chunks: Vec<Vec<u8>>,
    ) -> Result<ReadMemo<String>, KnitError> {
        let mut store = self.0.borrow_mut();
        let idx = store.blocks.len();
        let bytes: Vec<u8> = chunks.concat();
        let len = bytes.len() as u64;
        store.blocks.push(bytes);
        Ok(ReadMemo::new(idx.to_string(), 0, len))
    }
}

type Vf = GroupCompressVersionedFiles<MemIndex, MemAccess>;

fn populated(files: u32, revs: u32) -> (Vf, Vec<Key>) {
    let store = Rc::new(RefCell::new(Store::default()));
    let vf = GroupCompressVersionedFiles::new(MemIndex(store.clone()), MemAccess(store), true);
    let mut stream: Vec<Box<dyn ContentFactory>> = Vec::new();
    let mut keys = Vec::new();
    for file in 0..files {
        let prefix = format!("file-{file}").into_bytes();
        let base = common::make_text(file as u64, 400);
        let mut parent: Option<Key> = None;
        for rev in 0..revs {
            let mut text = base.clone();
            text.extend_from_slice(format!("\nedit rev {rev} file {file}\n").as_bytes());
            let key = Key::fixed(vec![prefix.clone(), format!("rev-{rev}").into_bytes()]);
            stream.push(Box::new(ChunkedContentFactory::new(
                None,
                key.clone(),
                Some(parent.take().into_iter().collect()),
                vec![text],
            )));
            keys.push(key.clone());
            parent = Some(key);
        }
    }
    vf.insert_record_stream(stream, false).unwrap();
    (vf, keys)
}

fn bench_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("gcvf/read");
    let (files, revs) = (40u32, 25u32);
    let (vf, keys) = populated(files, revs);
    group.throughput(Throughput::Elements(keys.len() as u64));
    for ordering in ["unordered", "as-requested", "topological"] {
        group.bench_with_input(
            BenchmarkId::from_parameter(ordering),
            &ordering,
            |b, ord| {
                b.iter(|| {
                    let recs = vf.get_record_stream(black_box(&keys), ord).unwrap();
                    // Force the records to fulltext so extract actually runs.
                    let mut total = 0usize;
                    for r in &recs {
                        total += r.to_fulltext().len();
                    }
                    black_box(total)
                });
            },
        );
    }
    group.finish();
}

criterion_group!(benches, bench_read);
criterion_main!(benches);
