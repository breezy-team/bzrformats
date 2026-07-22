//! Per-line annotation over any versioned-file store.
//!
//! This is the format-agnostic annotator breezy uses for stores that cannot
//! annotate from their own record chain (notably groupcompress/2a). It mirrors
//! `bzrformats._annotator_py.Annotator`: walk a text's ancestry, diff each text
//! against its parents with a patience matcher, and propagate the per-line
//! origin revisions, resolving ties through the revision graph.
//!
//! The knit format has its own annotator ([`crate::knit::KnitAnnotator`]) that
//! reuses knit delta blocks; this one only needs plain texts and a parent map,
//! so it works for every store.

use crate::versionedfile::Key;
use std::collections::{BTreeSet, HashMap, HashSet};

/// A per-line annotation: the set of keys that could be the line's origin,
/// kept sorted and deduplicated.
pub type LineAnnotation = Vec<Key>;

/// A key paired with its plain-text lines, as returned by
/// [`AnnotateSource::get_line_texts`].
pub type KeyLines = (Key, Vec<Vec<u8>>);

/// A source of texts and ancestry for [`Annotator`].
///
/// Implemented by versioned-file stores; the annotator drives it to fetch the
/// parent map and the plain-text lines of the keys it needs.
pub trait AnnotateSource {
    /// The direct parents of each key in `keys`, for keys the store knows.
    fn get_parent_map(&self, keys: &[Key]) -> Result<HashMap<Key, Vec<Key>>, Error>;

    /// The plain-text lines of each key in `keys`, in topological order
    /// (ancestors first). A key absent from the store is an error.
    fn get_line_texts(&self, keys: &[Key]) -> Result<Vec<KeyLines>, Error>;
}

/// Errors raised while annotating.
#[derive(Debug)]
pub enum Error {
    /// A requested key is not present in the store.
    RevisionNotPresent(Key),
    /// The backing store failed.
    Backend(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::RevisionNotPresent(key) => write!(f, "revision not present: {key:?}"),
            Error::Backend(msg) => write!(f, "{msg}"),
        }
    }
}

impl std::error::Error for Error {}

/// Drives per-line annotation of a text and its ancestry.
///
/// Construct with [`Annotator::new`], then call [`annotate_flat`](Self::annotate_flat)
/// (or [`annotate`](Self::annotate) for the multi-source form). One annotator
/// caches the ancestry it has walked, so annotating several related keys in
/// turn reuses that work.
pub struct Annotator<'s, S: AnnotateSource> {
    source: &'s S,
    parent_map: HashMap<Key, Vec<Key>>,
    text_cache: HashMap<Key, Vec<Vec<u8>>>,
    annotations_cache: HashMap<Key, Vec<LineAnnotation>>,
    num_needed_children: HashMap<Key, usize>,
}

impl<'s, S: AnnotateSource> Annotator<'s, S> {
    /// Create an annotator over `source`.
    pub fn new(source: &'s S) -> Self {
        Annotator {
            source,
            parent_map: HashMap::new(),
            text_cache: HashMap::new(),
            annotations_cache: HashMap::new(),
            num_needed_children: HashMap::new(),
        }
    }

    /// Inject a text that is not in the backing store (e.g. a working tree's
    /// edited content), so a later annotate treats it as a known key.
    pub fn add_special_text(&mut self, key: Key, parent_keys: Vec<Key>, lines: Vec<Vec<u8>>) {
        self.parent_map.insert(key.clone(), parent_keys);
        self.text_cache.insert(key, lines);
    }

    /// Determine which keys still need fetching from the store, walking the
    /// ancestry of `key` and updating `parent_map` and the child counts.
    ///
    /// Returns the keys whose text must be fetched from the store; texts
    /// already seeded (via [`add_special_text`](Self::add_special_text)) are
    /// left in place but still counted so they annotate in the right order.
    fn get_needed_keys(&mut self, key: &Key) -> Result<HashSet<Key>, Error> {
        self.num_needed_children.insert(key.clone(), 1);
        let mut vf_keys_needed: HashSet<Key> = HashSet::new();
        let mut needed_keys: HashSet<Key> = HashSet::from([key.clone()]);

        while !needed_keys.is_empty() {
            let mut parent_lookup: Vec<Key> = Vec::new();
            let mut next_parent_map: HashMap<Key, Vec<Key>> = HashMap::new();
            for key in needed_keys.drain() {
                if self.parent_map.contains_key(&key) {
                    if !self.text_cache.contains_key(&key) {
                        vf_keys_needed.insert(key);
                    }
                } else {
                    parent_lookup.push(key.clone());
                    vf_keys_needed.insert(key);
                }
            }
            if !parent_lookup.is_empty() {
                next_parent_map.extend(self.source.get_parent_map(&parent_lookup)?);
            }
            for (key, parent_keys) in &next_parent_map {
                for parent in parent_keys {
                    *self.num_needed_children.entry(parent.clone()).or_insert(0) += 1;
                    if !self.parent_map.contains_key(parent) {
                        needed_keys.insert(parent.clone());
                    }
                }
                let _ = key;
            }
            self.parent_map.extend(next_parent_map);
        }
        Ok(vf_keys_needed)
    }

    /// Fetch and annotate every ancestor text needed for `key`.
    fn extract_and_annotate(&mut self, key: &Key) -> Result<(), Error> {
        let needed = self.get_needed_keys(key)?;
        // Fetch in topological order so each text's parents annotate first.
        let fetch: Vec<Key> = needed.into_iter().collect();
        let texts = self.source.get_line_texts(&fetch)?;
        for (this_key, lines) in texts {
            self.text_cache.insert(this_key.clone(), lines);
            self.annotate_one(&this_key);
        }
        // Any text seeded externally (present in text_cache but not yet
        // annotated) still needs annotating, ancestors first.
        let pending: Vec<Key> = self
            .parent_map
            .keys()
            .filter(|k| {
                self.text_cache.contains_key(*k) && !self.annotations_cache.contains_key(*k)
            })
            .cloned()
            .collect();
        for k in topological_order(&self.parent_map, &pending) {
            if self.text_cache.contains_key(&k) && !self.annotations_cache.contains_key(&k) {
                self.annotate_one(&k);
            }
        }
        Ok(())
    }

    /// Annotate one text whose own text and all its parents' annotations are
    /// already cached. Mirrors `_annotator_py.Annotator._annotate_one`.
    fn annotate_one(&mut self, key: &Key) {
        let this_annotation: LineAnnotation = vec![key.clone()];
        let text = self.text_cache[key].clone();
        let mut annotations: Vec<LineAnnotation> = vec![this_annotation.clone(); text.len()];
        let parent_keys = self.parent_map.get(key).cloned().unwrap_or_default();

        if let Some(first_parent) = parent_keys.first() {
            let (parent_annotations, blocks) =
                self.parent_annotations_and_matches(&text, first_parent);
            for (parent_idx, lines_idx, match_len) in &blocks {
                if *match_len == 0 {
                    continue;
                }
                annotations[*lines_idx..*lines_idx + *match_len]
                    .clone_from_slice(&parent_annotations[*parent_idx..*parent_idx + *match_len]);
            }

            for other_parent in parent_keys.iter().skip(1) {
                let (parent_annotations, blocks) =
                    self.parent_annotations_and_matches(&text, other_parent);
                for (parent_idx, lines_idx, match_len) in &blocks {
                    if *match_len == 0 {
                        continue;
                    }
                    let ann_sub = annotations[*lines_idx..*lines_idx + *match_len].to_vec();
                    let par_sub = &parent_annotations[*parent_idx..*parent_idx + *match_len];
                    if ann_sub == *par_sub {
                        continue;
                    }
                    for idx in 0..*match_len {
                        let ann = &ann_sub[idx];
                        let par_ann = &par_sub[idx];
                        let ann_idx = *lines_idx + idx;
                        if ann == par_ann || *ann == this_annotation {
                            annotations[ann_idx] = par_ann.clone();
                        } else {
                            let mut merged: BTreeSet<Key> = ann.iter().cloned().collect();
                            merged.extend(par_ann.iter().cloned());
                            annotations[ann_idx] = merged.into_iter().collect();
                        }
                    }
                }
            }
        }

        self.record_annotation(key, &parent_keys, annotations);
    }

    /// The parent's annotations and the matching blocks between the parent's
    /// text and `text`, computed with a patience matcher.
    fn parent_annotations_and_matches(
        &self,
        text: &[Vec<u8>],
        parent_key: &Key,
    ) -> (Vec<LineAnnotation>, Vec<(usize, usize, usize)>) {
        let parent_lines = &self.text_cache[parent_key];
        let parent_annotations = self.annotations_cache[parent_key].clone();
        let p_refs: Vec<&[u8]> = parent_lines.iter().map(|l| l.as_slice()).collect();
        let t_refs: Vec<&[u8]> = text.iter().map(|l| l.as_slice()).collect();
        let blocks = patiencediff::SequenceMatcher::new(&p_refs, &t_refs)
            .get_matching_blocks()
            .to_vec();
        (parent_annotations, blocks)
    }

    /// Record `key`'s annotations and free any parent whose last child is done.
    fn record_annotation(
        &mut self,
        key: &Key,
        parent_keys: &[Key],
        annotations: Vec<LineAnnotation>,
    ) {
        self.annotations_cache.insert(key.clone(), annotations);
        for pk in parent_keys {
            if let Some(n) = self.num_needed_children.get_mut(pk) {
                *n -= 1;
                if *n == 0 {
                    self.text_cache.remove(pk);
                    self.annotations_cache.remove(pk);
                }
            }
        }
    }

    /// Annotate `key`, returning `(per_line_annotations, lines)`.
    ///
    /// Each element of the annotations is the set of keys that could be the
    /// line's origin; [`annotate_flat`](Self::annotate_flat) reduces each to a
    /// single best origin.
    pub fn annotate(&mut self, key: &Key) -> Result<(Vec<LineAnnotation>, Vec<Vec<u8>>), Error> {
        // record_annotation frees a key's caches once its last child is
        // annotated; keep the target's copies so they survive that sweep.
        self.num_needed_children.insert(key.clone(), 1);
        self.extract_and_annotate(key)?;
        let annotations = self
            .annotations_cache
            .get(key)
            .cloned()
            .ok_or_else(|| Error::RevisionNotPresent(key.clone()))?;
        let lines = self.text_cache.get(key).cloned().unwrap_or_default();
        Ok((annotations, lines))
    }

    /// Annotate `key`, returning `[(origin_key, line)]` with one best origin
    /// per line. Ties are resolved by the revision graph's heads, then by
    /// picking the key that sorts first.
    pub fn annotate_flat(&mut self, key: &Key) -> Result<Vec<(Key, Vec<u8>)>, Error> {
        let (annotations, lines) = self.annotate(key)?;
        let mut graph = vcs_graph::KnownGraph::new(
            self.parent_map.iter().map(|(k, v)| (k.clone(), v.clone())),
            false,
        );
        let out = annotations
            .into_iter()
            .zip(lines)
            .map(|(annotation, line)| {
                let head = if annotation.len() == 1 {
                    annotation.into_iter().next().unwrap()
                } else {
                    let heads = graph.heads(annotation.iter().cloned());
                    if heads.len() == 1 {
                        heads.into_iter().next().unwrap()
                    } else {
                        let mut sorted: Vec<Key> = heads.into_iter().collect();
                        sorted.sort();
                        sorted.into_iter().next().unwrap()
                    }
                };
                (head, line)
            })
            .collect();
        Ok(out)
    }
}

/// Order `keys` so every key comes after its ancestors in `parent_map`.
///
/// A stable topological sort restricted to the given keys (parents outside the
/// set are ignored), done iteratively so deep histories cannot overflow the
/// stack.
fn topological_order(parent_map: &HashMap<Key, Vec<Key>>, keys: &[Key]) -> Vec<Key> {
    let set: HashSet<&Key> = keys.iter().collect();
    let mut visited: HashSet<Key> = HashSet::new();
    let mut order: Vec<Key> = Vec::new();
    // Each stack frame tracks whether its children have been pushed yet: on the
    // first visit we queue the parents, on the second we emit the key.
    let mut stack: Vec<(Key, bool)> = Vec::new();

    for start in keys {
        if visited.contains(start) {
            continue;
        }
        stack.push((start.clone(), false));
        while let Some((key, children_done)) = stack.pop() {
            if children_done {
                order.push(key);
                continue;
            }
            if visited.contains(&key) {
                continue;
            }
            visited.insert(key.clone());
            stack.push((key.clone(), true));
            if let Some(parents) = parent_map.get(&key) {
                for parent in parents {
                    if set.contains(parent) && !visited.contains(parent) {
                        stack.push((parent.clone(), false));
                    }
                }
            }
        }
    }
    order
}
