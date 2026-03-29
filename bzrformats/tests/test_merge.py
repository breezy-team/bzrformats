# Copyright (C) 2005-2012, 2016 Canonical Ltd
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

"""Tests for merge implementations."""

from bzrformats import knit, versionedfile
from bzrformats.merge import _PlanMerge

from . import TestCaseWithMemoryTransport


class TestPlanMerge(TestCaseWithMemoryTransport):
    def setUp(self):
        super().setUp()
        mapper = versionedfile.PrefixMapper()
        factory = knit.make_file_factory(True, mapper)
        self.vf = factory(self.get_transport())
        self.plan_merge_vf = versionedfile._PlanMergeVersionedFile(b"root")
        self.plan_merge_vf.fallback_versionedfiles.append(self.vf)

    def add_version(self, key, parents, text):
        self.vf.add_lines(key, parents, [bytes([c]) + b"\n" for c in bytearray(text)])

    def add_rev(self, prefix, revision_id, parents, text):
        self.add_version((prefix, revision_id), [(prefix, p) for p in parents], text)

    def add_uncommitted_version(self, key, parents, text):
        self.plan_merge_vf.add_lines(
            key, parents, [bytes([c]) + b"\n" for c in bytearray(text)]
        )

    def setup_plan_merge(self):
        self.add_rev(b"root", b"A", [], b"abc")
        self.add_rev(b"root", b"B", [b"A"], b"acehg")
        self.add_rev(b"root", b"C", [b"A"], b"fabg")
        return _PlanMerge(b"B", b"C", self.plan_merge_vf, (b"root",))

    def setup_plan_merge_uncommitted(self):
        self.add_version((b"root", b"A"), [], b"abc")
        self.add_uncommitted_version((b"root", b"B:"), [(b"root", b"A")], b"acehg")
        self.add_uncommitted_version((b"root", b"C:"), [(b"root", b"A")], b"fabg")
        return _PlanMerge(b"B:", b"C:", self.plan_merge_vf, (b"root",))

    def test_base_from_plan(self):
        self.setup_plan_merge()
        plan = self.plan_merge_vf.plan_merge(b"B", b"C")
        pwm = versionedfile.PlanWeaveMerge(plan)
        self.assertEqual([b"a\n", b"b\n", b"c\n"], pwm.base_from_plan())

    def test_unique_lines(self):
        plan = self.setup_plan_merge()
        self.assertEqual(
            plan._unique_lines(plan._get_matching_blocks(b"B", b"C")),
            ([1, 2, 3], [0, 2]),
        )

    def test_plan_merge(self):
        self.setup_plan_merge()
        plan = self.plan_merge_vf.plan_merge(b"B", b"C")
        self.assertEqual(
            [
                ("new-b", b"f\n"),
                ("unchanged", b"a\n"),
                ("killed-a", b"b\n"),
                ("killed-b", b"c\n"),
                ("new-a", b"e\n"),
                ("new-a", b"h\n"),
                ("new-a", b"g\n"),
                ("new-b", b"g\n"),
            ],
            list(plan),
        )

    def test_plan_merge_cherrypick(self):
        self.add_rev(b"root", b"A", [], b"abc")
        self.add_rev(b"root", b"B", [b"A"], b"abcde")
        self.add_rev(b"root", b"C", [b"A"], b"abcefg")
        self.add_rev(b"root", b"D", [b"A", b"B", b"C"], b"abcdegh")
        my_plan = _PlanMerge(b"B", b"D", self.plan_merge_vf, (b"root",))
        # We shortcut when one text supersedes the other in the per-file graph.
        # We don't actually need to compare the texts at this point.
        self.assertEqual(
            [
                ("new-b", b"a\n"),
                ("new-b", b"b\n"),
                ("new-b", b"c\n"),
                ("new-b", b"d\n"),
                ("new-b", b"e\n"),
                ("new-b", b"g\n"),
                ("new-b", b"h\n"),
            ],
            list(my_plan.plan_merge()),
        )

    def test_plan_merge_no_common_ancestor(self):
        self.add_rev(b"root", b"A", [], b"abc")
        self.add_rev(b"root", b"B", [], b"xyz")
        my_plan = _PlanMerge(b"A", b"B", self.plan_merge_vf, (b"root",))
        self.assertEqual(
            [
                ("new-a", b"a\n"),
                ("new-a", b"b\n"),
                ("new-a", b"c\n"),
                ("new-b", b"x\n"),
                ("new-b", b"y\n"),
                ("new-b", b"z\n"),
            ],
            list(my_plan.plan_merge()),
        )

    def test_plan_merge_tail_ancestors(self):
        # The graph looks like this:
        #       A       # Common to all ancestors
        #      / \
        #     B   C     # Ancestors of E, only common to one side
        #     |\ /|
        #     D E F     # D, F are unique to G, H respectively
        #     |/ \|     # E is the LCA for G & H, and the unique LCA for
        #     G   H     # I, J
        #     |\ /|
        #     | X |
        #     |/ \|
        #     I   J     # criss-cross merge of G, H
        #
        # In this situation, a simple pruning of ancestors of E will leave D &
        # F "dangling", which looks like they introduce lines different from
        # the ones in E, but in actuality C&B introduced the lines, and they
        # are already present in E

        # Introduce the base text
        self.add_rev(b"root", b"A", [], b"abc")
        # Introduces a new line B
        self.add_rev(b"root", b"B", [b"A"], b"aBbc")
        # Introduces a new line C
        self.add_rev(b"root", b"C", [b"A"], b"abCc")
        # Introduce new line D
        self.add_rev(b"root", b"D", [b"B"], b"DaBbc")
        # Merges B and C by just incorporating both
        self.add_rev(b"root", b"E", [b"B", b"C"], b"aBbCc")
        # Introduce new line F
        self.add_rev(b"root", b"F", [b"C"], b"abCcF")
        # Merge D & E by just combining the texts
        self.add_rev(b"root", b"G", [b"D", b"E"], b"DaBbCc")
        # Merge F & E by just combining the texts
        self.add_rev(b"root", b"H", [b"F", b"E"], b"aBbCcF")
        # Merge G & H by just combining texts
        self.add_rev(b"root", b"I", [b"G", b"H"], b"DaBbCcF")
        # Merge G & H but supersede an old line in B
        self.add_rev(b"root", b"J", [b"H", b"G"], b"DaJbCcF")
        plan = self.plan_merge_vf.plan_merge(b"I", b"J")
        self.assertEqual(
            [
                ("unchanged", b"D\n"),
                ("unchanged", b"a\n"),
                ("killed-b", b"B\n"),
                ("new-b", b"J\n"),
                ("unchanged", b"b\n"),
                ("unchanged", b"C\n"),
                ("unchanged", b"c\n"),
                ("unchanged", b"F\n"),
            ],
            list(plan),
        )

    def test_plan_merge_tail_triple_ancestors(self):
        # The graph looks like this:
        #       A       # Common to all ancestors
        #      / \
        #     B   C     # Ancestors of E, only common to one side
        #     |\ /|
        #     D E F     # D, F are unique to G, H respectively
        #     |/|\|     # E is the LCA for G & H, and the unique LCA for
        #     G Q H     # I, J
        #     |\ /|     # Q is just an extra node which is merged into both
        #     | X |     # I and J
        #     |/ \|
        #     I   J     # criss-cross merge of G, H
        #
        # This is the same as the test_plan_merge_tail_ancestors, except we add
        # a third LCA that doesn't add new lines, but will trigger our more
        # involved ancestry logic

        self.add_rev(b"root", b"A", [], b"abc")
        self.add_rev(b"root", b"B", [b"A"], b"aBbc")
        self.add_rev(b"root", b"C", [b"A"], b"abCc")
        self.add_rev(b"root", b"D", [b"B"], b"DaBbc")
        self.add_rev(b"root", b"E", [b"B", b"C"], b"aBbCc")
        self.add_rev(b"root", b"F", [b"C"], b"abCcF")
        self.add_rev(b"root", b"G", [b"D", b"E"], b"DaBbCc")
        self.add_rev(b"root", b"H", [b"F", b"E"], b"aBbCcF")
        self.add_rev(b"root", b"Q", [b"E"], b"aBbCc")
        self.add_rev(b"root", b"I", [b"G", b"Q", b"H"], b"DaBbCcF")
        # Merge G & H but supersede an old line in B
        self.add_rev(b"root", b"J", [b"H", b"Q", b"G"], b"DaJbCcF")
        plan = self.plan_merge_vf.plan_merge(b"I", b"J")
        self.assertEqual(
            [
                ("unchanged", b"D\n"),
                ("unchanged", b"a\n"),
                ("killed-b", b"B\n"),
                ("new-b", b"J\n"),
                ("unchanged", b"b\n"),
                ("unchanged", b"C\n"),
                ("unchanged", b"c\n"),
                ("unchanged", b"F\n"),
            ],
            list(plan),
        )

    def test_plan_merge_2_tail_triple_ancestors(self):
        # The graph looks like this:
        #     A   B     # 2 tails going back to NULL
        #     |\ /|
        #     D E F     # D, is unique to G, F to H
        #     |/|\|     # E is the LCA for G & H, and the unique LCA for
        #     G Q H     # I, J
        #     |\ /|     # Q is just an extra node which is merged into both
        #     | X |     # I and J
        #     |/ \|
        #     I   J     # criss-cross merge of G, H (and Q)
        #

        # This is meant to test after hitting a 3-way LCA, and multiple tail
        # ancestors (only have NULL_REVISION in common)

        self.add_rev(b"root", b"A", [], b"abc")
        self.add_rev(b"root", b"B", [], b"def")
        self.add_rev(b"root", b"D", [b"A"], b"Dabc")
        self.add_rev(b"root", b"E", [b"A", b"B"], b"abcdef")
        self.add_rev(b"root", b"F", [b"B"], b"defF")
        self.add_rev(b"root", b"G", [b"D", b"E"], b"Dabcdef")
        self.add_rev(b"root", b"H", [b"F", b"E"], b"abcdefF")
        self.add_rev(b"root", b"Q", [b"E"], b"abcdef")
        self.add_rev(b"root", b"I", [b"G", b"Q", b"H"], b"DabcdefF")
        # Merge G & H but supersede an old line in B
        self.add_rev(b"root", b"J", [b"H", b"Q", b"G"], b"DabcdJfF")
        plan = self.plan_merge_vf.plan_merge(b"I", b"J")
        self.assertEqual(
            [
                ("unchanged", b"D\n"),
                ("unchanged", b"a\n"),
                ("unchanged", b"b\n"),
                ("unchanged", b"c\n"),
                ("unchanged", b"d\n"),
                ("killed-b", b"e\n"),
                ("new-b", b"J\n"),
                ("unchanged", b"f\n"),
                ("unchanged", b"F\n"),
            ],
            list(plan),
        )

    def test_plan_merge_uncommitted_files(self):
        self.setup_plan_merge_uncommitted()
        plan = self.plan_merge_vf.plan_merge(b"B:", b"C:")
        self.assertEqual(
            [
                ("new-b", b"f\n"),
                ("unchanged", b"a\n"),
                ("killed-a", b"b\n"),
                ("killed-b", b"c\n"),
                ("new-a", b"e\n"),
                ("new-a", b"h\n"),
                ("new-a", b"g\n"),
                ("new-b", b"g\n"),
            ],
            list(plan),
        )

    def test_plan_merge_insert_order(self):
        """Weave merges are sensitive to the order of insertion.

        Specifically for overlapping regions, it effects which region gets put
        'first'. And when a user resolves an overlapping merge, if they use the
        same ordering, then the lines match the parents, if they don't only
        *some* of the lines match.
        """
        self.add_rev(b"root", b"A", [], b"abcdef")
        self.add_rev(b"root", b"B", [b"A"], b"abwxcdef")
        self.add_rev(b"root", b"C", [b"A"], b"abyzcdef")
        # Merge, and resolve the conflict by adding *both* sets of lines
        # If we get the ordering wrong, these will look like new lines in D,
        # rather than carried over from B, C
        self.add_rev(b"root", b"D", [b"B", b"C"], b"abwxyzcdef")
        # Supersede the lines in B and delete the lines in C, which will
        # conflict if they are treated as being in D
        self.add_rev(b"root", b"E", [b"C", b"B"], b"abnocdef")
        # Same thing for the lines in C
        self.add_rev(b"root", b"F", [b"C"], b"abpqcdef")
        plan = self.plan_merge_vf.plan_merge(b"D", b"E")
        self.assertEqual(
            [
                ("unchanged", b"a\n"),
                ("unchanged", b"b\n"),
                ("killed-b", b"w\n"),
                ("killed-b", b"x\n"),
                ("killed-b", b"y\n"),
                ("killed-b", b"z\n"),
                ("new-b", b"n\n"),
                ("new-b", b"o\n"),
                ("unchanged", b"c\n"),
                ("unchanged", b"d\n"),
                ("unchanged", b"e\n"),
                ("unchanged", b"f\n"),
            ],
            list(plan),
        )
        plan = self.plan_merge_vf.plan_merge(b"E", b"D")
        # Going in the opposite direction shows the effect of the opposite plan
        self.assertEqual(
            [
                ("unchanged", b"a\n"),
                ("unchanged", b"b\n"),
                ("new-b", b"w\n"),
                ("new-b", b"x\n"),
                ("killed-a", b"y\n"),
                ("killed-a", b"z\n"),
                ("killed-both", b"w\n"),
                ("killed-both", b"x\n"),
                ("new-a", b"n\n"),
                ("new-a", b"o\n"),
                ("unchanged", b"c\n"),
                ("unchanged", b"d\n"),
                ("unchanged", b"e\n"),
                ("unchanged", b"f\n"),
            ],
            list(plan),
        )

    def test_plan_merge_criss_cross(self):
        # This is specificly trying to trigger problems when using limited
        # ancestry and weaves. The ancestry graph looks like:
        #       XX      unused ancestor, should not show up in the weave
        #       |
        #       A       Unique LCA
        #       |\
        #       B \     Introduces a line 'foo'
        #      / \ \
        #     C   D E   C & D both have 'foo', E has different changes
        #     |\ /| |
        #     | X | |
        #     |/ \|/
        #     F   G      All of C, D, E are merged into F and G, so they are
        #                all common ancestors.
        #
        # The specific issue with weaves:
        #   B introduced a text ('foo') that is present in both C and D.
        #   If we do not include B (because it isn't an ancestor of E), then
        #   the A=>C and A=>D look like both sides independently introduce the
        #   text ('foo'). If F does not modify the text, it would still appear
        #   to have deleted on of the versions from C or D. If G then modifies
        #   'foo', it should appear as superseding the value in F (since it
        #   came from B), rather than conflict because of the resolution during
        #   C & D.
        self.add_rev(b"root", b"XX", [], b"qrs")
        self.add_rev(b"root", b"A", [b"XX"], b"abcdef")
        self.add_rev(b"root", b"B", [b"A"], b"axcdef")
        self.add_rev(b"root", b"C", [b"B"], b"axcdefg")
        self.add_rev(b"root", b"D", [b"B"], b"haxcdef")
        self.add_rev(b"root", b"E", [b"A"], b"abcdyf")
        # Simple combining of all texts
        self.add_rev(b"root", b"F", [b"C", b"D", b"E"], b"haxcdyfg")
        # combine and supersede 'x'
        self.add_rev(b"root", b"G", [b"C", b"D", b"E"], b"hazcdyfg")
        plan = self.plan_merge_vf.plan_merge(b"F", b"G")
        self.assertEqual(
            [
                ("unchanged", b"h\n"),
                ("unchanged", b"a\n"),
                ("killed-base", b"b\n"),
                ("killed-b", b"x\n"),
                ("new-b", b"z\n"),
                ("unchanged", b"c\n"),
                ("unchanged", b"d\n"),
                ("killed-base", b"e\n"),
                ("unchanged", b"y\n"),
                ("unchanged", b"f\n"),
                ("unchanged", b"g\n"),
            ],
            list(plan),
        )
        plan = self.plan_merge_vf.plan_lca_merge(b"F", b"G")
        # This is one of the main differences between plan_merge and
        # plan_lca_merge. plan_lca_merge generates a conflict for 'x => z',
        # because 'x' was not present in one of the bases. However, in this
        # case it is spurious because 'x' does not exist in the global base A.
        self.assertEqual(
            [
                ("unchanged", b"h\n"),
                ("unchanged", b"a\n"),
                ("conflicted-a", b"x\n"),
                ("new-b", b"z\n"),
                ("unchanged", b"c\n"),
                ("unchanged", b"d\n"),
                ("unchanged", b"y\n"),
                ("unchanged", b"f\n"),
                ("unchanged", b"g\n"),
            ],
            list(plan),
        )

    def test_criss_cross_flip_flop(self):
        # This is specificly trying to trigger problems when using limited
        # ancestry and weaves. The ancestry graph looks like:
        #       XX      unused ancestor, should not show up in the weave
        #       |
        #       A       Unique LCA
        #      / \
        #     B   C     B & C both introduce a new line
        #     |\ /|
        #     | X |
        #     |/ \|
        #     D   E     B & C are both merged, so both are common ancestors
        #               In the process of merging, both sides order the new
        #               lines differently
        #
        self.add_rev(b"root", b"XX", [], b"qrs")
        self.add_rev(b"root", b"A", [b"XX"], b"abcdef")
        self.add_rev(b"root", b"B", [b"A"], b"abcdgef")
        self.add_rev(b"root", b"C", [b"A"], b"abcdhef")
        self.add_rev(b"root", b"D", [b"B", b"C"], b"abcdghef")
        self.add_rev(b"root", b"E", [b"C", b"B"], b"abcdhgef")
        plan = list(self.plan_merge_vf.plan_merge(b"D", b"E"))
        self.assertEqual(
            [
                ("unchanged", b"a\n"),
                ("unchanged", b"b\n"),
                ("unchanged", b"c\n"),
                ("unchanged", b"d\n"),
                ("new-b", b"h\n"),
                ("unchanged", b"g\n"),
                ("killed-b", b"h\n"),
                ("unchanged", b"e\n"),
                ("unchanged", b"f\n"),
            ],
            plan,
        )
        pwm = versionedfile.PlanWeaveMerge(plan)
        self.assertEqualDiff(
            b"a\nb\nc\nd\ng\nh\ne\nf\n", b"".join(pwm.base_from_plan())
        )
        # Reversing the order reverses the merge plan, and final order of 'hg'
        # => 'gh'
        plan = list(self.plan_merge_vf.plan_merge(b"E", b"D"))
        self.assertEqual(
            [
                ("unchanged", b"a\n"),
                ("unchanged", b"b\n"),
                ("unchanged", b"c\n"),
                ("unchanged", b"d\n"),
                ("new-b", b"g\n"),
                ("unchanged", b"h\n"),
                ("killed-b", b"g\n"),
                ("unchanged", b"e\n"),
                ("unchanged", b"f\n"),
            ],
            plan,
        )
        pwm = versionedfile.PlanWeaveMerge(plan)
        self.assertEqualDiff(
            b"a\nb\nc\nd\nh\ng\ne\nf\n", b"".join(pwm.base_from_plan())
        )
        # This is where lca differs, in that it (fairly correctly) determines
        # that there is a conflict because both sides resolved the merge
        # differently
        plan = list(self.plan_merge_vf.plan_lca_merge(b"D", b"E"))
        self.assertEqual(
            [
                ("unchanged", b"a\n"),
                ("unchanged", b"b\n"),
                ("unchanged", b"c\n"),
                ("unchanged", b"d\n"),
                ("conflicted-b", b"h\n"),
                ("unchanged", b"g\n"),
                ("conflicted-a", b"h\n"),
                ("unchanged", b"e\n"),
                ("unchanged", b"f\n"),
            ],
            plan,
        )
        pwm = versionedfile.PlanWeaveMerge(plan)
        self.assertEqualDiff(b"a\nb\nc\nd\ng\ne\nf\n", b"".join(pwm.base_from_plan()))
        # Reversing it changes what line is doubled, but still gives a
        # double-conflict
        plan = list(self.plan_merge_vf.plan_lca_merge(b"E", b"D"))
        self.assertEqual(
            [
                ("unchanged", b"a\n"),
                ("unchanged", b"b\n"),
                ("unchanged", b"c\n"),
                ("unchanged", b"d\n"),
                ("conflicted-b", b"g\n"),
                ("unchanged", b"h\n"),
                ("conflicted-a", b"g\n"),
                ("unchanged", b"e\n"),
                ("unchanged", b"f\n"),
            ],
            plan,
        )
        pwm = versionedfile.PlanWeaveMerge(plan)
        self.assertEqualDiff(b"a\nb\nc\nd\nh\ne\nf\n", b"".join(pwm.base_from_plan()))

    def assertRemoveExternalReferences(
        self, filtered_parent_map, child_map, tails, parent_map
    ):
        """Assert results for _PlanMerge._remove_external_references."""
        (
            act_filtered_parent_map,
            act_child_map,
            act_tails,
        ) = _PlanMerge._remove_external_references(parent_map)

        # The parent map *should* preserve ordering, but the ordering of
        # children is not strictly defined
        # child_map = dict((k, sorted(children))
        #                  for k, children in child_map.iteritems())
        # act_child_map = dict(k, sorted(children)
        #                      for k, children in act_child_map.iteritems())
        self.assertEqual(filtered_parent_map, act_filtered_parent_map)
        self.assertEqual(child_map, act_child_map)
        self.assertEqual(sorted(tails), sorted(act_tails))

    def test__remove_external_references(self):
        # First, nothing to remove
        self.assertRemoveExternalReferences(
            {3: [2], 2: [1], 1: []},
            {1: [2], 2: [3], 3: []},
            [1],
            {3: [2], 2: [1], 1: []},
        )
        # The reverse direction
        self.assertRemoveExternalReferences(
            {1: [2], 2: [3], 3: []},
            {3: [2], 2: [1], 1: []},
            [3],
            {1: [2], 2: [3], 3: []},
        )
        # Extra references
        self.assertRemoveExternalReferences(
            {3: [2], 2: [1], 1: []},
            {1: [2], 2: [3], 3: []},
            [1],
            {3: [2, 4], 2: [1, 5], 1: [6]},
        )
        # Multiple tails
        self.assertRemoveExternalReferences(
            {4: [2, 3], 3: [], 2: [1], 1: []},
            {1: [2], 2: [4], 3: [4], 4: []},
            [1, 3],
            {4: [2, 3], 3: [5], 2: [1], 1: [6]},
        )
        # Multiple children
        self.assertRemoveExternalReferences(
            {1: [3], 2: [3, 4], 3: [], 4: []},
            {1: [], 2: [], 3: [1, 2], 4: [2]},
            [3, 4],
            {1: [3], 2: [3, 4], 3: [5], 4: []},
        )

    def assertPruneTails(self, pruned_map, tails, parent_map):
        child_map = {}
        for key, parent_keys in parent_map.items():
            child_map.setdefault(key, [])
            for pkey in parent_keys:
                child_map.setdefault(pkey, []).append(key)
        _PlanMerge._prune_tails(parent_map, child_map, tails)
        self.assertEqual(pruned_map, parent_map)

    def test__prune_tails(self):
        # Nothing requested to prune
        self.assertPruneTails({1: [], 2: [], 3: []}, [], {1: [], 2: [], 3: []})
        # Prune a single entry
        self.assertPruneTails({1: [], 3: []}, [2], {1: [], 2: [], 3: []})
        # Prune a chain
        self.assertPruneTails({1: []}, [3], {1: [], 2: [3], 3: []})
        # Prune a chain with a diamond
        self.assertPruneTails({1: []}, [5], {1: [], 2: [3, 4], 3: [5], 4: [5], 5: []})
        # Prune a partial chain
        self.assertPruneTails(
            {1: [6], 6: []}, [5], {1: [2, 6], 2: [3, 4], 3: [5], 4: [5], 5: [], 6: []}
        )
        # Prune a chain with multiple tips, that pulls out intermediates
        self.assertPruneTails(
            {1: [3], 3: []}, [4, 5], {1: [2, 3], 2: [4, 5], 3: [], 4: [], 5: []}
        )
        self.assertPruneTails(
            {1: [3], 3: []}, [5, 4], {1: [2, 3], 2: [4, 5], 3: [], 4: [], 5: []}
        )

    def test_subtract_plans(self):
        old_plan = [
            ("unchanged", b"a\n"),
            ("new-a", b"b\n"),
            ("killed-a", b"c\n"),
            ("new-b", b"d\n"),
            ("new-b", b"e\n"),
            ("killed-b", b"f\n"),
            ("killed-b", b"g\n"),
        ]
        new_plan = [
            ("unchanged", b"a\n"),
            ("new-a", b"b\n"),
            ("killed-a", b"c\n"),
            ("new-b", b"d\n"),
            ("new-b", b"h\n"),
            ("killed-b", b"f\n"),
            ("killed-b", b"i\n"),
        ]
        subtracted_plan = [
            ("unchanged", b"a\n"),
            ("new-a", b"b\n"),
            ("killed-a", b"c\n"),
            ("new-b", b"h\n"),
            ("unchanged", b"f\n"),
            ("killed-b", b"i\n"),
        ]
        self.assertEqual(
            subtracted_plan, list(_PlanMerge._subtract_plans(old_plan, new_plan))
        )

    def setup_merge_with_base(self):
        self.add_rev(b"root", b"COMMON", [], b"abc")
        self.add_rev(b"root", b"THIS", [b"COMMON"], b"abcd")
        self.add_rev(b"root", b"BASE", [b"COMMON"], b"eabc")
        self.add_rev(b"root", b"OTHER", [b"BASE"], b"eafb")

    def test_plan_merge_with_base(self):
        self.setup_merge_with_base()
        plan = self.plan_merge_vf.plan_merge(b"THIS", b"OTHER", b"BASE")
        self.assertEqual(
            [
                ("unchanged", b"a\n"),
                ("new-b", b"f\n"),
                ("unchanged", b"b\n"),
                ("killed-b", b"c\n"),
                ("new-a", b"d\n"),
            ],
            list(plan),
        )

    def test_plan_lca_merge(self):
        self.setup_plan_merge()
        plan = self.plan_merge_vf.plan_lca_merge(b"B", b"C")
        self.assertEqual(
            [
                ("new-b", b"f\n"),
                ("unchanged", b"a\n"),
                ("killed-b", b"c\n"),
                ("new-a", b"e\n"),
                ("new-a", b"h\n"),
                ("killed-a", b"b\n"),
                ("unchanged", b"g\n"),
            ],
            list(plan),
        )

    def test_plan_lca_merge_uncommitted_files(self):
        self.setup_plan_merge_uncommitted()
        plan = self.plan_merge_vf.plan_lca_merge(b"B:", b"C:")
        self.assertEqual(
            [
                ("new-b", b"f\n"),
                ("unchanged", b"a\n"),
                ("killed-b", b"c\n"),
                ("new-a", b"e\n"),
                ("new-a", b"h\n"),
                ("killed-a", b"b\n"),
                ("unchanged", b"g\n"),
            ],
            list(plan),
        )

    def test_plan_lca_merge_with_base(self):
        self.setup_merge_with_base()
        plan = self.plan_merge_vf.plan_lca_merge(b"THIS", b"OTHER", b"BASE")
        self.assertEqual(
            [
                ("unchanged", b"a\n"),
                ("new-b", b"f\n"),
                ("unchanged", b"b\n"),
                ("killed-b", b"c\n"),
                ("new-a", b"d\n"),
            ],
            list(plan),
        )

    def test_plan_lca_merge_with_criss_cross(self):
        self.add_version((b"root", b"ROOT"), [], b"abc")
        # each side makes a change
        self.add_version((b"root", b"REV1"), [(b"root", b"ROOT")], b"abcd")
        self.add_version((b"root", b"REV2"), [(b"root", b"ROOT")], b"abce")
        # both sides merge, discarding others' changes
        self.add_version(
            (b"root", b"LCA1"), [(b"root", b"REV1"), (b"root", b"REV2")], b"abcd"
        )
        self.add_version(
            (b"root", b"LCA2"), [(b"root", b"REV1"), (b"root", b"REV2")], b"fabce"
        )
        plan = self.plan_merge_vf.plan_lca_merge(b"LCA1", b"LCA2")
        self.assertEqual(
            [
                ("new-b", b"f\n"),
                ("unchanged", b"a\n"),
                ("unchanged", b"b\n"),
                ("unchanged", b"c\n"),
                ("conflicted-a", b"d\n"),
                ("conflicted-b", b"e\n"),
            ],
            list(plan),
        )

    def test_plan_lca_merge_with_null(self):
        self.add_version((b"root", b"A"), [], b"ab")
        self.add_version((b"root", b"B"), [], b"bc")
        plan = self.plan_merge_vf.plan_lca_merge(b"A", b"B")
        self.assertEqual(
            [
                ("new-a", b"a\n"),
                ("unchanged", b"b\n"),
                ("new-b", b"c\n"),
            ],
            list(plan),
        )

    def test_plan_merge_with_delete_and_change(self):
        self.add_rev(b"root", b"C", [], b"a")
        self.add_rev(b"root", b"A", [b"C"], b"b")
        self.add_rev(b"root", b"B", [b"C"], b"")
        plan = self.plan_merge_vf.plan_merge(b"A", b"B")
        self.assertEqual(
            [
                ("killed-both", b"a\n"),
                ("new-a", b"b\n"),
            ],
            list(plan),
        )

    def test_plan_merge_with_move_and_change(self):
        self.add_rev(b"root", b"C", [], b"abcd")
        self.add_rev(b"root", b"A", [b"C"], b"acbd")
        self.add_rev(b"root", b"B", [b"C"], b"aBcd")
        plan = self.plan_merge_vf.plan_merge(b"A", b"B")
        self.assertEqual(
            [
                ("unchanged", b"a\n"),
                ("new-a", b"c\n"),
                ("killed-b", b"b\n"),
                ("new-b", b"B\n"),
                ("killed-a", b"c\n"),
                ("unchanged", b"d\n"),
            ],
            list(plan),
        )
