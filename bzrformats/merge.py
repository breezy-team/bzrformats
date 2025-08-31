# Copyright (C) 2005-2011 Canonical Ltd
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

"""Plan merge implementation for versioned files."""

import patiencediff

from breezy import errors
from breezy import graph as _mod_graph
from breezy import revision as _mod_revision
from breezy import trace
from vcsgraph.tsort import merge_sort
from . import weave

class _PlanMergeBase:
    def __init__(self, a_rev, b_rev, vf, key_prefix):
        """Contructor.

        :param a_rev: Revision-id of one revision to merge
        :param b_rev: Revision-id of the other revision to merge
        :param vf: A VersionedFiles containing both revisions
        :param key_prefix: A prefix for accessing keys in vf, typically
            (file_id,).
        """
        self.a_rev = a_rev
        self.b_rev = b_rev
        self.vf = vf
        self._last_lines = None
        self._last_lines_revision_id = None
        self._cached_matching_blocks = {}
        self._key_prefix = key_prefix
        self._precache_tip_lines()

    def _precache_tip_lines(self):
        lines = self.get_lines([self.a_rev, self.b_rev])
        self.lines_a = lines[self.a_rev]
        self.lines_b = lines[self.b_rev]

    def get_lines(self, revisions):
        """Get lines for revisions from the backing VersionedFiles.

        :raises RevisionNotPresent: on absent texts.
        """
        keys = [(self._key_prefix + (rev,)) for rev in revisions]
        result = {}
        for record in self.vf.get_record_stream(keys, "unordered", True):
            if record.storage_kind == "absent":
                raise errors.RevisionNotPresent(record.key, self.vf)
            result[record.key[-1]] = record.get_bytes_as("lines")
        return result

    def plan_merge(self):
        """Generate a 'plan' for merging the two revisions.

        This involves comparing their texts and determining the cause of
        differences.  If text A has a line and text B does not, then either the
        line was added to text A, or it was deleted from B.  Once the causes
        are combined, they are written out in the format described in
        VersionedFile.plan_merge
        """
        blocks = self._get_matching_blocks(self.a_rev, self.b_rev)
        unique_a, unique_b = self._unique_lines(blocks)
        new_a, killed_b = self._determine_status(self.a_rev, unique_a)
        new_b, killed_a = self._determine_status(self.b_rev, unique_b)
        return self._iter_plan(blocks, new_a, killed_b, new_b, killed_a)

    def _iter_plan(self, blocks, new_a, killed_b, new_b, killed_a):
        last_i = 0
        last_j = 0
        for i, j, n in blocks:
            for a_index in range(last_i, i):
                if a_index in new_a:
                    if a_index in killed_b:
                        yield "conflicted-a", self.lines_a[a_index]
                    else:
                        yield "new-a", self.lines_a[a_index]
                else:
                    yield "killed-b", self.lines_a[a_index]
            for b_index in range(last_j, j):
                if b_index in new_b:
                    if b_index in killed_a:
                        yield "conflicted-b", self.lines_b[b_index]
                    else:
                        yield "new-b", self.lines_b[b_index]
                else:
                    yield "killed-a", self.lines_b[b_index]
            # handle common lines
            for a_index in range(i, i + n):
                yield "unchanged", self.lines_a[a_index]
            last_i = i + n
            last_j = j + n

    def _get_matching_blocks(self, left_revision, right_revision):
        """Return a description of which sections of two revisions match.

        See SequenceMatcher.get_matching_blocks
        """
        cached = self._cached_matching_blocks.get((left_revision, right_revision))
        if cached is not None:
            return cached
        if self._last_lines_revision_id == left_revision:
            left_lines = self._last_lines
            right_lines = self.get_lines([right_revision])[right_revision]
        else:
            lines = self.get_lines([left_revision, right_revision])
            left_lines = lines[left_revision]
            right_lines = lines[right_revision]
        self._last_lines = right_lines
        self._last_lines_revision_id = right_revision
        matcher = patiencediff.PatienceSequenceMatcher(None, left_lines, right_lines)
        return matcher.get_matching_blocks()

    def _unique_lines(self, matching_blocks):
        """Analyse matching_blocks to determine which lines are unique.

        :return: a tuple of (unique_left, unique_right), where the values are
            sets of line numbers of unique lines.
        """
        last_i = 0
        last_j = 0
        unique_left = []
        unique_right = []
        for i, j, n in matching_blocks:
            unique_left.extend(range(last_i, i))
            unique_right.extend(range(last_j, j))
            last_i = i + n
            last_j = j + n
        return unique_left, unique_right

    @staticmethod
    def _subtract_plans(old_plan, new_plan):
        """Remove changes from new_plan that came from old_plan.

        It is assumed that the difference between the old_plan and new_plan
        is their choice of 'b' text.

        All lines from new_plan that differ from old_plan are emitted
        verbatim.  All lines from new_plan that match old_plan but are
        not about the 'b' revision are emitted verbatim.

        Lines that match and are about the 'b' revision are the lines we
        don't want, so we convert 'killed-b' -> 'unchanged', and 'new-b'
        is skipped entirely.
        """
        matcher = patiencediff.PatienceSequenceMatcher(None, old_plan, new_plan)
        last_j = 0
        for _i, j, n in matcher.get_matching_blocks():
            for jj in range(last_j, j):
                yield new_plan[jj]
            for jj in range(j, j + n):
                plan_line = new_plan[jj]
                if plan_line[0] == "new-b":
                    pass
                elif plan_line[0] == "killed-b":
                    yield "unchanged", plan_line[1]
                else:
                    yield plan_line
            last_j = j + n


class _PlanMerge(_PlanMergeBase):
    """Plan an annotate merge using on-the-fly annotation."""

    def __init__(self, a_rev, b_rev, vf, key_prefix):
        super().__init__(a_rev, b_rev, vf, key_prefix)
        self.a_key = self._key_prefix + (self.a_rev,)
        self.b_key = self._key_prefix + (self.b_rev,)
        self.graph = _mod_graph.Graph(self.vf)
        heads = self.graph.heads((self.a_key, self.b_key))
        if len(heads) == 1:
            # one side dominates, so we can just return its values, yay for
            # per-file graphs
            # Ideally we would know that before we get this far
            self._head_key = heads.pop()
            other = b_rev if self._head_key == self.a_key else a_rev
            trace.mutter(
                "found dominating revision for %s\n%s > %s",
                self.vf,
                self._head_key[-1],
                other,
            )
            self._weave = None
        else:
            self._head_key = None
            self._build_weave()

    def _precache_tip_lines(self):
        # Turn this into a no-op, because we will do this later
        pass

    def _find_recursive_lcas(self):
        """Find all the ancestors back to a unique lca."""
        cur_ancestors = (self.a_key, self.b_key)
        # graph.find_lca(uncommon, keys) now returns plain NULL_REVISION,
        # rather than a key tuple. We will just map that directly to no common
        # ancestors.
        parent_map = {}
        while True:
            next_lcas = self.graph.find_lca(*cur_ancestors)
            # Map a plain NULL_REVISION to a simple no-ancestors
            if next_lcas == {_mod_revision.NULL_REVISION}:
                next_lcas = ()
            # Order the lca's based on when they were merged into the tip
            # While the actual merge portion of weave merge uses a set() of
            # active revisions, the order of insertion *does* effect the
            # implicit ordering of the texts.
            for rev_key in cur_ancestors:
                ordered_parents = tuple(self.graph.find_merge_order(rev_key, next_lcas))
                parent_map[rev_key] = ordered_parents
            if len(next_lcas) == 0:
                break
            elif len(next_lcas) == 1:
                parent_map[list(next_lcas)[0]] = ()
                break
            elif len(next_lcas) > 2:
                # More than 2 lca's, fall back to grabbing all nodes between
                # this and the unique lca.
                trace.mutter(
                    "More than 2 LCAs, falling back to all nodes for: %s, %s\n=> %s",
                    self.a_key,
                    self.b_key,
                    cur_ancestors,
                )
                cur_lcas = next_lcas
                while len(cur_lcas) > 1:
                    cur_lcas = self.graph.find_lca(*cur_lcas)
                if len(cur_lcas) == 0:
                    # No common base to find, use the full ancestry
                    unique_lca = None
                else:
                    unique_lca = list(cur_lcas)[0]
                    if unique_lca == _mod_revision.NULL_REVISION:
                        # find_lca will return a plain 'NULL_REVISION' rather
                        # than a key tuple when there is no common ancestor, we
                        # prefer to just use None, because it doesn't confuse
                        # _get_interesting_texts()
                        unique_lca = None
                parent_map.update(self._find_unique_parents(next_lcas, unique_lca))
                break
            cur_ancestors = next_lcas
        return parent_map

    def _find_unique_parents(self, tip_keys, base_key):
        """Find ancestors of tip that aren't ancestors of base.

        :param tip_keys: Nodes that are interesting
        :param base_key: Cull all ancestors of this node
        :return: The parent map for all revisions between tip_keys and
            base_key. base_key will be included. References to nodes outside of
            the ancestor set will also be removed.
        """
        # TODO: this would be simpler if find_unique_ancestors took a list
        #       instead of a single tip, internally it supports it, but it
        #       isn't a "backwards compatible" api change.
        if base_key is None:
            parent_map = dict(self.graph.iter_ancestry(tip_keys))
            # We remove NULL_REVISION because it isn't a proper tuple key, and
            # thus confuses things like _get_interesting_texts, and our logic
            # to add the texts into the memory weave.
            if _mod_revision.NULL_REVISION in parent_map:
                parent_map.pop(_mod_revision.NULL_REVISION)
        else:
            interesting = set()
            for tip in tip_keys:
                interesting.update(self.graph.find_unique_ancestors(tip, [base_key]))
            parent_map = self.graph.get_parent_map(interesting)
            parent_map[base_key] = ()
        culled_parent_map, child_map, tails = self._remove_external_references(
            parent_map
        )
        # Remove all the tails but base_key
        if base_key is not None:
            tails.remove(base_key)
            self._prune_tails(culled_parent_map, child_map, tails)
        # Now remove all the uninteresting 'linear' regions
        simple_map = _mod_graph.collapse_linear_regions(culled_parent_map)
        return simple_map

    @staticmethod
    def _remove_external_references(parent_map):
        """Remove references that go outside of the parent map.

        :param parent_map: Something returned from Graph.get_parent_map(keys)
        :return: (filtered_parent_map, child_map, tails)
            filtered_parent_map is parent_map without external references
            child_map is the {parent_key: [child_keys]} mapping
            tails is a list of nodes that do not have any parents in the map
        """
        # TODO: The basic effect of this function seems more generic than
        #       _PlanMerge. But the specific details of building a child_map,
        #       and computing tails seems very specific to _PlanMerge.
        #       Still, should this be in Graph land?
        filtered_parent_map = {}
        child_map = {}
        tails = []
        for key, parent_keys in parent_map.items():
            culled_parent_keys = [p for p in parent_keys if p in parent_map]
            if not culled_parent_keys:
                tails.append(key)
            for parent_key in culled_parent_keys:
                child_map.setdefault(parent_key, []).append(key)
            # TODO: Do we want to do this, it adds overhead for every node,
            #       just to say that the node has no children
            child_map.setdefault(key, [])
            filtered_parent_map[key] = culled_parent_keys
        return filtered_parent_map, child_map, tails

    @staticmethod
    def _prune_tails(parent_map, child_map, tails_to_remove):
        """Remove tails from the parent map.

        This will remove the supplied revisions until no more children have 0
        parents.

        :param parent_map: A dict of {child: [parents]}, this dictionary will
            be modified in place.
        :param tails_to_remove: A list of tips that should be removed,
            this list will be consumed
        :param child_map: The reverse dict of parent_map ({parent: [children]})
            this dict will be modified
        :return: None, parent_map will be modified in place.
        """
        while tails_to_remove:
            next = tails_to_remove.pop()
            parent_map.pop(next)
            children = child_map.pop(next)
            for child in children:
                child_parents = parent_map[child]
                child_parents.remove(next)
                if len(child_parents) == 0:
                    tails_to_remove.append(child)

    def _get_interesting_texts(self, parent_map):
        """Return a dict of texts we are interested in.

        Note that the input is in key tuples, but the output is in plain
        revision ids.

        :param parent_map: The output from _find_recursive_lcas
        :return: A dict of {'revision_id':lines} as returned by
            _PlanMergeBase.get_lines()
        """
        all_revision_keys = set(parent_map)
        all_revision_keys.add(self.a_key)
        all_revision_keys.add(self.b_key)

        # Everything else is in 'keys' but get_lines is in 'revision_ids'
        all_texts = self.get_lines([k[-1] for k in all_revision_keys])
        return all_texts

    def _build_weave(self):

        self._weave = weave.Weave(weave_name="in_memory_weave", allow_reserved=True)
        parent_map = self._find_recursive_lcas()

        all_texts = self._get_interesting_texts(parent_map)

        # Note: Unfortunately, the order given by topo_sort will effect the
        # ordering resolution in the output. Specifically, if you add A then B,
        # then in the output text A lines will show up before B lines. And, of
        # course, topo_sort doesn't guarantee any real ordering.
        # So we use merge_sort, and add a fake node on the tip.
        # This ensures that left-hand parents will always be inserted into the
        # weave before right-hand parents.
        tip_key = self._key_prefix + (_mod_revision.CURRENT_REVISION,)
        parent_map[tip_key] = (self.a_key, self.b_key)

        for _seq_num, key, _depth, _eom in reversed(merge_sort(parent_map, tip_key)):
            if key == tip_key:
                continue
            # for key in tsort.topo_sort(parent_map):
            parent_keys = parent_map[key]
            revision_id = key[-1]
            parent_ids = [k[-1] for k in parent_keys]
            self._weave.add_lines(revision_id, parent_ids, all_texts[revision_id])

    def plan_merge(self):
        """Generate a 'plan' for merging the two revisions.

        This involves comparing their texts and determining the cause of
        differences.  If text A has a line and text B does not, then either the
        line was added to text A, or it was deleted from B.  Once the causes
        are combined, they are written out in the format described in
        VersionedFile.plan_merge
        """
        if self._head_key is not None:  # There was a single head
            if self._head_key == self.a_key:
                plan = "new-a"
            else:
                if self._head_key != self.b_key:
                    raise AssertionError(
                        f"There was an invalid head: {self.b_key} != {self._head_key}"
                    )
                plan = "new-b"
            head_rev = self._head_key[-1]
            lines = self.get_lines([head_rev])[head_rev]
            return ((plan, line) for line in lines)
        return self._weave.plan_merge(self.a_rev, self.b_rev)


class _PlanLCAMerge(_PlanMergeBase):
    """Merger that uses LCA.

    This merge algorithm differs from _PlanMerge in that:

    1. comparisons are done against LCAs only
    2. cases where a contested line is new versus one LCA but old versus
       another are marked as conflicts, by emitting the line as conflicted-a
       or conflicted-b.

    This is faster, and hopefully produces more useful output.
    """

    def __init__(self, a_rev, b_rev, vf, key_prefix, graph):
        _PlanMergeBase.__init__(self, a_rev, b_rev, vf, key_prefix)
        lcas = graph.find_lca(key_prefix + (a_rev,), key_prefix + (b_rev,))
        self.lcas = set()
        for lca in lcas:
            if lca == _mod_revision.NULL_REVISION:
                self.lcas.add(lca)
            else:
                self.lcas.add(lca[-1])
        for lca in self.lcas:
            lca_lines = [] if _mod_revision.is_null(lca) else self.get_lines([lca])[lca]
            matcher = patiencediff.PatienceSequenceMatcher(
                None, self.lines_a, lca_lines
            )
            blocks = list(matcher.get_matching_blocks())
            self._cached_matching_blocks[(a_rev, lca)] = blocks
            matcher = patiencediff.PatienceSequenceMatcher(
                None, self.lines_b, lca_lines
            )
            blocks = list(matcher.get_matching_blocks())
            self._cached_matching_blocks[(b_rev, lca)] = blocks

    def _determine_status(self, revision_id, unique_line_numbers):
        """Determines the status unique lines versus all lcas.

        Basically, determines why the line is unique to this revision.

        A line may be determined new, killed, or both.

        If a line is determined new, that means it was not present in at least
        one LCA, and is not present in the other merge revision.

        If a line is determined killed, that means the line was present in
        at least one LCA.

        If a line is killed and new, this indicates that the two merge
        revisions contain differing conflict resolutions.

        :param revision_id: The id of the revision in which the lines are
            unique
        :param unique_line_numbers: The line numbers of unique lines.
        :return: a tuple of (new_this, killed_other)
        """
        new = set()
        killed = set()
        unique_line_numbers = set(unique_line_numbers)
        for lca in self.lcas:
            blocks = self._get_matching_blocks(revision_id, lca)
            unique_vs_lca, _ignored = self._unique_lines(blocks)
            new.update(unique_line_numbers.intersection(unique_vs_lca))
            killed.update(unique_line_numbers.difference(unique_vs_lca))
        return new, killed
