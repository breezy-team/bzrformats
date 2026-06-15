# bazaar benchmarks

Criterion benchmarks for the hot codec and index paths. Run them all with:

```
cargo bench -p bazaar
```

or one suite at a time:

```
cargo bench -p bazaar --bench rabin_delta
cargo bench -p bazaar --bench groupcompress
cargo bench -p bazaar --bench chk_map
cargo bench -p bazaar --bench btree_index
```

Fixtures are generated deterministically (no rng) in `common/mod.rs` so runs are
comparable across machines and over time.

## Suites

- **rabin_delta** — `make_delta` over a source/target pair, and incremental
  indexing of N related sources before a final delta. The incremental case is
  the canary for the quadratic-index trap: throughput should stay flat as N
  grows.
- **groupcompress** — full `RabinGroupCompressor::compress` over a family of
  related revisions (the ratio-vs-time path), plus `extract` of a mid-group
  record (replays deltas back to a fulltext).
- **chk_map** — leaf-node serialise / deserialise, which run on every CHK
  inventory read and write.
- **btree_index** — leaf-node body parsing and the multi-key bisect used to
  locate keys across a sorted page.

See `PERFORMANCE.md` (repo root) for the baseline numbers and the optimization
opportunities these benchmarks surfaced.
