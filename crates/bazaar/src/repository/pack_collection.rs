//! Pack maintenance for pack repositories: the `pack()` / `autopack()`
//! operations and the pack-distribution arithmetic that decides when
//! autopack should fire.
//!
//! The arithmetic is a direct port of breezy's `RepositoryPackCollection`
//! (`breezy/bzr/pack_repo.py`): `pack_distribution` and
//! `plan_autopack_combinations`. These are pure functions over a repository's
//! pack list (each pack summarised by its revision count), kept separate from
//! the I/O of `pack()` so they can be unit-tested in isolation.
//!
//! [`PackNames`] is the in-memory pack list and its reconciliation with the
//! `pack-names` file, likewise ported from `RepositoryPackCollection`.

use std::collections::{BTreeMap, BTreeSet};

use super::RepositoryError;
use crate::pack_repo::{index_extension, IndexKind};
use crate::transport::{SharedTransport, Transport, TransportError};

/// The target distribution of pack sizes for `total_revisions` revisions, as a
/// list of revision-count buckets, largest first.
///
/// breezy's `pack_distribution`: read the decimal digits least-significant
/// first; a digit `d` at place value `10^e` contributes `d` buckets of size
/// `10^e`. So 1234 -> `[1000, 100, 100, 10, 10, 10, 1, 1, 1, 1]`. An empty
/// repository yields `[0]`.
pub fn pack_distribution(total_revisions: u64) -> Vec<u64> {
    if total_revisions == 0 {
        return vec![0];
    }
    let digits = total_revisions.to_string();
    let mut buckets = Vec::new();
    // Iterate digits least-significant first, tracking the place value.
    for (i, ch) in digits.bytes().rev().enumerate() {
        let count = (ch - b'0') as usize;
        let value = 10u64.pow(i as u32);
        for _ in 0..count {
            buckets.push(value);
        }
    }
    buckets.reverse();
    buckets
}

/// Decide which packs to combine, given each pack's revision count.
///
/// Returns the list of pack indices (into `pack_revision_counts`) to repack
/// into a single new pack, or an empty list when nothing should be done.
///
/// Mirrors breezy's `plan_autopack_combinations`: if the repository already has
/// no more packs than the target distribution has buckets, there is nothing to
/// do. Otherwise the packs are considered largest first; a pack big enough to
/// fill the next distribution bucket on its own is left alone, and the smaller
/// packs are gathered until the remaining buckets are accounted for. Everything
/// gathered is flattened into one combine operation. A plan that would only
/// move a single pack is suppressed (repacking one pack into one pack is
/// pointless).
pub fn plan_autopack_combinations(pack_revision_counts: &[u64]) -> Vec<usize> {
    let distribution = {
        let total: u64 = pack_revision_counts.iter().sum();
        pack_distribution(total)
    };
    if pack_revision_counts.len() <= distribution.len() {
        return Vec::new();
    }

    // Sort pack indices by revision count, largest first. (Ties keep input
    // order, which is irrelevant since every selected pack ends up in one
    // combined operation.)
    let mut order: Vec<usize> = (0..pack_revision_counts.len()).collect();
    order.sort_by(|&a, &b| pack_revision_counts[b].cmp(&pack_revision_counts[a]));

    // Port of breezy's loop, which mutates a working copy of the distribution.
    let mut dist: std::collections::VecDeque<i64> =
        distribution.iter().map(|&v| v as i64).collect();
    let mut selected = Vec::new();
    let mut pending_op_revs: i64 = 0;
    for &pack in &order {
        let mut rev_count = pack_revision_counts[pack] as i64;
        if dist.front().is_some_and(|&head| rev_count >= head) {
            // Already packed better than this bucket: consume buckets equal to
            // its size, shrinking a partially-filled final bucket.
            while rev_count > 0 {
                let head = *dist.front().expect("distribution exhausted");
                rev_count -= head;
                if rev_count >= 0 {
                    dist.pop_front();
                } else {
                    *dist.front_mut().unwrap() = -rev_count;
                }
            }
        } else {
            // Add this pack to the current output operation.
            pending_op_revs += rev_count;
            selected.push(pack);
            if dist.front().is_some_and(|&head| pending_op_revs >= head) {
                dist.pop_front();
                pending_op_revs = 0;
            }
        }
    }

    // Repacking a single pack into a single pack achieves nothing.
    if selected.len() < 2 {
        return Vec::new();
    }
    selected
}

/// One `pack-names` entry: a pack's name and the index sizes recorded for it.
pub type PackEntry = (String, Vec<u8>);

/// The packs a pack repository reads from, kept in step with `pack-names`:
/// those listed when it was last read or written, and those added and
/// removed since.
///
/// Packs this process writes or repacks away change the in-memory list
/// first; saving `pack-names` then merges those changes with whatever other
/// processes have written since the list was last read.
#[derive(Debug, Default)]
pub struct PackNames {
    /// The packs in memory, with their index sizes.
    names: BTreeMap<String, Vec<u8>>,
    /// The `pack-names` entries as last read or written.
    at_load: BTreeSet<PackEntry>,
}

impl PackNames {
    /// The packs listed in `entries`, as just read from `pack-names`.
    pub fn from_disk(entries: Vec<PackEntry>) -> Self {
        PackNames {
            at_load: entries.iter().cloned().collect(),
            names: entries.into_iter().collect(),
        }
    }

    /// The names of the packs in memory.
    pub fn names(&self) -> Vec<String> {
        self.names.keys().cloned().collect()
    }

    /// The entries of the packs in memory.
    pub fn entries(&self) -> Vec<PackEntry> {
        self.names
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect()
    }

    /// Add a pack this process wrote. Returns `false`,
    /// changing nothing, if a pack of that name is already listed.
    pub fn allocate(&mut self, name: String, value: Vec<u8>) -> bool {
        if self.names.contains_key(&name) {
            return false;
        }
        self.names.insert(name, value);
        true
    }

    /// Drop a pack this process repacked away.
    pub fn remove(&mut self, name: &str) {
        self.names.remove(name);
    }

    /// The entries `pack-names` should hold given `disk`, its current
    /// contents: those on disk, less the packs removed here since the list
    /// was last read or written, plus those added here.
    pub fn merge(&self, disk: &[PackEntry]) -> BTreeSet<PackEntry> {
        let current: BTreeSet<PackEntry> = self.entries().into_iter().collect();
        let mut merged: BTreeSet<PackEntry> = disk.iter().cloned().collect();
        for deleted in self.at_load.difference(&current) {
            merged.remove(deleted);
        }
        merged.extend(current.difference(&self.at_load).cloned());
        merged
    }

    /// Record `entries` as what `pack-names` now holds on disk.
    pub fn set_at_load(&mut self, entries: BTreeSet<PackEntry>) {
        self.at_load = entries;
    }

    /// Make the packs in memory exactly `entries`. Returns the names of the
    /// packs removed and added; a pack whose index sizes changed is both.
    pub fn sync_to(&mut self, entries: &BTreeSet<PackEntry>) -> (Vec<String>, Vec<String>) {
        let wanted: BTreeMap<&String, &Vec<u8>> =
            entries.iter().map(|(name, value)| (name, value)).collect();
        let mut removed = Vec::new();
        let mut added = Vec::new();
        self.names.retain(|name, value| match wanted.get(name) {
            Some(&wanted_value) if wanted_value == value => true,
            _ => {
                removed.push(name.clone());
                false
            }
        });
        for (name, value) in entries {
            if !self.names.contains_key(name) {
                self.names.insert(name.clone(), value.clone());
                added.push(name.clone());
            }
        }
        (removed, added)
    }
}

/// A combined index of one object kind over a repository's packs, which a
/// [`PackCollection`] adds packs to and removes them from.
pub trait CombinedIndex {
    /// Add the entries of `pack`'s index for `kind`.
    fn add_pack(
        &self,
        transport: &dyn Transport,
        pack: &str,
        kind: IndexKind,
    ) -> Result<(), RepositoryError>;

    /// Drop the entries of `pack`.
    fn remove_pack(&self, pack: &str);
}

/// A reader of pack data, which a [`PackCollection`] tells to drop what it
/// holds of a pack that is no longer read.
pub trait PackReader {
    /// Drop anything held of `pack`.
    fn forget(&self, pack: &str);
}

/// The packs of a pack repository and the stores reading them.
///
/// The stores' combined indices and readers are registered with
/// [`add_store`](Self::add_store); packs the repository writes or repacks
/// away are added to and removed from them in place, and `pack-names` is
/// written by merging those changes with whatever other processes have
/// written since it was read.
pub struct PackCollection<I, R> {
    transport: SharedTransport,
    /// Whether `pack-names` is a btree index rather than a format-1 one.
    uses_btree: bool,
    names: PackNames,
    indices: Vec<(IndexKind, I)>,
    readers: Vec<R>,
}

impl<I: CombinedIndex, R: PackReader> PackCollection<I, R> {
    /// The packs listed in the `pack-names` of the repository `transport`
    /// is rooted at, with no stores yet.
    pub fn open(transport: SharedTransport, uses_btree: bool) -> Result<Self, RepositoryError> {
        let names = PackNames::from_disk(read_pack_names(transport.as_ref())?);
        Ok(PackCollection {
            transport,
            uses_btree,
            names,
            indices: Vec::new(),
            readers: Vec::new(),
        })
    }

    /// The names of the packs the stores read.
    pub fn names(&self) -> Vec<String> {
        self.names.names()
    }

    /// Register a store's combined index for `kind` and its pack reader, to
    /// be kept in step with the packs.
    pub fn add_store(&mut self, kind: IndexKind, index: I, reader: R) {
        self.indices.push((kind, index));
        self.readers.push(reader);
    }

    /// Add a pack the repository wrote; `pack-names`
    /// lists it from the next save.
    pub fn allocate(&mut self, name: String, value: Vec<u8>) -> Result<(), RepositoryError> {
        if !self.names.allocate(name.clone(), value) {
            return Err(RepositoryError::Corrupt(format!(
                "pack {name} already exists"
            )));
        }
        self.update_stores(&[], &[name])
    }

    /// Write `pack-names`, merging the packs added and removed here with
    /// those other processes changed since it was read, and read the
    /// result.
    pub fn save(&mut self) -> Result<(), RepositoryError> {
        let disk = read_pack_names(self.transport.as_ref())?;
        let merged = self.names.merge(&disk);
        write_pack_names(self.transport.as_ref(), self.uses_btree, &merged)?;
        self.names.set_at_load(merged.clone());
        self.sync(&merged)
    }

    /// Read `pack-names` again, keeping the packs added and removed here.
    pub fn reload(&mut self) -> Result<(), RepositoryError> {
        let disk = read_pack_names(self.transport.as_ref())?;
        let merged = self.names.merge(&disk);
        self.names.set_at_load(disk.into_iter().collect());
        self.sync(&merged)
    }

    /// Replace `old` packs with `new_pack`, if any: read it instead of them,
    /// save `pack-names`, and move them into `obsolete_packs/`.
    pub fn replace(
        &mut self,
        old: &[String],
        new_pack: Option<PackEntry>,
    ) -> Result<(), RepositoryError> {
        if let Some((name, value)) = new_pack {
            self.allocate(name, value)?;
        }
        for pack in old {
            self.names.remove(pack);
        }
        self.update_stores(old, &[])?;
        self.save()?;
        self.obsolete(old)
    }

    /// Make the stores read exactly the packs in `entries`.
    fn sync(&mut self, entries: &BTreeSet<PackEntry>) -> Result<(), RepositoryError> {
        let (removed, added) = self.names.sync_to(entries);
        self.update_stores(&removed, &added)
    }

    /// Make the stores read `added` and stop reading `removed`.
    fn update_stores(&self, removed: &[String], added: &[String]) -> Result<(), RepositoryError> {
        for pack in removed {
            for (_, index) in &self.indices {
                index.remove_pack(pack);
            }
            for reader in &self.readers {
                reader.forget(pack);
            }
        }
        for pack in added {
            for (kind, index) in &self.indices {
                index.add_pack(self.transport.as_ref(), pack, *kind)?;
            }
        }
        Ok(())
    }

    /// Move `packs` and their indices into `obsolete_packs/`. Old packs are
    /// moved rather than deleted, matching brz, so a mistaken pack can be
    /// recovered.
    fn obsolete(&self, packs: &[String]) -> Result<(), RepositoryError> {
        let transport = self.transport.as_ref();
        if !transport.has("obsolete_packs")? {
            transport.mkdir("obsolete_packs")?;
        }
        let move_away = |from: String, basename: String| match transport
            .rename(&from, &format!("obsolete_packs/{basename}"))
        {
            Ok(()) | Err(TransportError::NoSuchFile(_)) => Ok(()),
            Err(e) => Err(RepositoryError::from(e)),
        };
        for name in packs {
            move_away(format!("packs/{name}.pack"), format!("{name}.pack"))?;
            for (kind, _) in &self.indices {
                let ext = index_extension(*kind);
                move_away(format!("indices/{name}{ext}"), format!("{name}{ext}"))?;
            }
        }
        Ok(())
    }
}

/// Read `pack-names`, returning each pack's name and index sizes.
pub fn read_pack_names(transport: &dyn Transport) -> Result<Vec<PackEntry>, RepositoryError> {
    let index = super::pack_index::PackIndex::open(transport, "pack-names")?;
    Ok(index
        .iter_all_entries()
        .filter_map(|(key, value, _refs)| {
            key.first()
                .map(|name| (String::from_utf8_lossy(name).into_owned(), value.clone()))
        })
        .collect())
}

/// Write `pack-names` listing `entries`.
fn write_pack_names(
    transport: &dyn Transport,
    uses_btree: bool,
    entries: &BTreeSet<PackEntry>,
) -> Result<(), RepositoryError> {
    let mut names = super::pack_index::IndexBuilder::new(uses_btree, 0, 1);
    for (name, value) in entries {
        names
            .add_node(vec![name.clone().into_bytes()], value.clone(), vec![])
            .map_err(|e| RepositoryError::Corrupt(format!("pack-names node: {e}")))?;
    }
    let bytes = names
        .finish()
        .map_err(|e| RepositoryError::Corrupt(format!("pack-names finish: {e}")))?;
    transport.put_bytes("pack-names", &bytes, None)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack_distribution_powers_of_ten() {
        assert_eq!(pack_distribution(0), vec![0]);
        assert_eq!(pack_distribution(1), vec![1]);
        assert_eq!(pack_distribution(9), vec![1, 1, 1, 1, 1, 1, 1, 1, 1]);
        assert_eq!(pack_distribution(10), vec![10]);
        assert_eq!(
            pack_distribution(1234),
            vec![1000, 100, 100, 10, 10, 10, 1, 1, 1, 1]
        );
    }

    #[test]
    fn plan_does_nothing_when_within_distribution() {
        // 1000 revisions in 1 pack: distribution is [1000], 1 pack <= 1 bucket.
        assert!(plan_autopack_combinations(&[1000]).is_empty());
        // Two packs summing to 11 revisions: distribution(11) = [10, 1], len 2,
        // 2 packs <= 2 buckets -> nothing to do.
        assert!(plan_autopack_combinations(&[10, 1]).is_empty());
    }

    #[test]
    fn plan_combines_many_small_packs() {
        // Five single-revision packs: total 5, distribution(5) = [1,1,1,1,1]
        // (5 buckets), 5 packs <= 5 buckets -> nothing.
        assert!(plan_autopack_combinations(&[1, 1, 1, 1, 1]).is_empty());
        // Six single-revision packs: total 6, distribution = six 1-buckets (6),
        // 6 <= 6 -> still nothing.
        assert!(plan_autopack_combinations(&[1, 1, 1, 1, 1, 1]).is_empty());
        // Eleven single-revision packs: total 11, distribution(11) = [10, 1]
        // (2 buckets); 11 packs > 2 -> combine. The first bucket (10) gathers
        // ten 1-packs; the eleventh exactly fills the trailing 1-bucket on its
        // own, so it is left alone (matching breezy: 10 packs selected).
        let plan = plan_autopack_combinations(&[1; 11]);
        assert_eq!(plan.len(), 10);
    }

    #[test]
    fn plan_leaves_a_large_pack_alone() {
        // One big pack (1000 revs) plus three tiny ones: total 1003,
        // distribution(1003) = [1000, 1, 1, 1] (4 buckets); 4 packs <= 4 -> no
        // repack.
        assert!(plan_autopack_combinations(&[1000, 1, 1, 1]).is_empty());
        // Add a fifth tiny pack: total 1004, distribution = [1000,1,1,1,1]
        // (5 buckets); 5 packs <= 5 -> still nothing.
        assert!(plan_autopack_combinations(&[1000, 1, 1, 1, 1]).is_empty());
    }

    #[test]
    fn plan_gathers_small_packs_around_a_medium_one() {
        // A medium pack (5 revs) and six 1-packs: total 11, distribution(11) =
        // [10, 1] (2 buckets); 7 packs > 2 -> repack. Largest first, the 5-pack
        // (index 0) and the next five 1-packs accumulate to fill the 10-bucket;
        // the sixth 1-pack fills the trailing 1-bucket on its own (consume
        // branch) and is left out. So indices 0..=5 are combined.
        assert_eq!(
            plan_autopack_combinations(&[5, 1, 1, 1, 1, 1, 1]),
            vec![0, 1, 2, 3, 4, 5]
        );
    }

    #[test]
    fn plan_partially_consumes_a_bucket_for_an_oversized_pack() {
        // A 15-revision pack against a distribution whose first two buckets are
        // tens exercises the partial-bucket consume: 15 fills the first 10
        // bucket and leaves the second 10 bucket holding 5. total = 15 + 11 =
        // 26, distribution(26) = [10,10,1,1,1,1,1,1] (8 buckets); 12 packs > 8.
        // The oversized pack (index 0) is consumed against the buckets, not
        // selected; the first five 1-packs gather to fill the dented 5-bucket.
        let mut counts = vec![15];
        counts.extend(std::iter::repeat_n(1, 11));
        assert_eq!(plan_autopack_combinations(&counts), vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn plan_combines_exactly_two_packs() {
        // Two 5-packs and two 1-packs: total 12, distribution(12) = [10, 1, 1]
        // (3 buckets); 4 packs > 3 -> repack. The two 5-packs sum to 10 and fill
        // the 10-bucket together; the 1-packs are consumed. Exactly two packs
        // are combined -- the boundary where the single-pack suppression must
        // NOT fire.
        assert_eq!(plan_autopack_combinations(&[5, 5, 1, 1]), vec![0, 1]);
    }

    #[test]
    fn plan_suppresses_single_pack_combine() {
        // 1000-rev pack plus eleven 1-packs: total 1011, distribution(1011) =
        // [1000, 10, 1] (3 buckets); 12 packs > 3 -> consider a repack. The big
        // pack consumes the 1000-bucket; the eleven 1-packs gather to fill the
        // 10-bucket, and the last is left for the 1-bucket. The plan combines
        // the ten gathered small packs (indices 1..=10), never the lone big one.
        let mut counts = vec![1000];
        counts.extend(std::iter::repeat_n(1, 11));
        assert_eq!(
            plan_autopack_combinations(&counts),
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        );
    }

    fn entry(name: &str, value: &str) -> PackEntry {
        (name.to_string(), value.as_bytes().to_vec())
    }

    #[test]
    fn merge_keeps_other_writers_changes() {
        let mut packs = PackNames::from_disk(vec![entry("a", "1"), entry("b", "1")]);
        // This process repacks a away into c.
        packs.remove("a");
        assert!(packs.allocate("c".to_string(), b"2".to_vec()));
        // Meanwhile another process added d and repacked b away.
        let disk = vec![entry("a", "1"), entry("d", "3")];
        assert_eq!(
            BTreeSet::from([entry("c", "2"), entry("d", "3")]),
            packs.merge(&disk)
        );
    }

    #[test]
    fn allocate_refuses_a_listed_pack() {
        let mut packs = PackNames::from_disk(vec![entry("a", "1")]);
        assert!(!packs.allocate("a".to_string(), b"2".to_vec()));
        assert_eq!(vec![entry("a", "1")], packs.entries());
    }

    #[test]
    fn sync_reports_removed_and_added_packs() {
        let mut packs = PackNames::from_disk(vec![entry("a", "1"), entry("b", "1")]);
        let target = BTreeSet::from([entry("b", "2"), entry("c", "1")]);
        let (removed, added) = packs.sync_to(&target);
        assert_eq!(
            (
                vec!["a".to_string(), "b".to_string()],
                vec!["b".to_string(), "c".to_string()]
            ),
            (removed, added)
        );
        assert_eq!(vec![entry("b", "2"), entry("c", "1")], packs.entries());
    }
}
