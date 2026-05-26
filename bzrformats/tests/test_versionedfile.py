# Copyright (C) 2010 Canonical Ltd
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

"""Tests for VersionedFile classes."""

from .. import errors, groupcompress, multiparent, versionedfile
from . import TestCase, TestCaseWithMemoryTransport


class Test_MPDiffGenerator(TestCaseWithMemoryTransport):
    # Should this be a per vf test?

    def make_vf(self):
        t = self.get_transport("")
        factory = groupcompress.make_pack_factory(True, True, 1)
        return factory(t)

    def make_three_vf(self):
        vf = self.make_vf()
        vf.add_lines((b"one",), (), [b"first\n"])
        vf.add_lines((b"two",), [(b"one",)], [b"first\n", b"second\n"])
        vf.add_lines(
            (b"three",), [(b"one",), (b"two",)], [b"first\n", b"second\n", b"third\n"]
        )
        return vf

    def test_raises_on_ghost_keys(self):
        # If the requested key is a ghost, then we have a problem
        vf = self.make_vf()
        gen = versionedfile._MPDiffGenerator(vf, [(b"one",)])
        self.assertRaises(errors.RevisionNotPresent, gen.compute_diffs)

    def test_ignores_ghost_parents(self):
        # If a parent is a ghost, it produces a snapshot of the child's text.
        vf = self.make_vf()
        vf.add_lines((b"two",), [(b"one",)], [b"first\n", b"second\n"])
        diffs = versionedfile._MPDiffGenerator(vf, [(b"two",)]).compute_diffs()
        self.assertEqual(
            [multiparent.MultiParent([multiparent.NewText([b"first\n", b"second\n"])])],
            diffs,
        )

    def test_compute_diffs(self):
        vf = self.make_three_vf()
        # The content is in the order requested, even if it isn't topological
        gen = versionedfile._MPDiffGenerator(vf, [(b"two",), (b"three",), (b"one",)])
        diffs = gen.compute_diffs()
        expected_diffs = [
            multiparent.MultiParent(
                [multiparent.ParentText(0, 0, 0, 1), multiparent.NewText([b"second\n"])]
            ),
            multiparent.MultiParent(
                [multiparent.ParentText(1, 0, 0, 2), multiparent.NewText([b"third\n"])]
            ),
            multiparent.MultiParent([multiparent.NewText([b"first\n"])]),
        ]
        self.assertEqual(expected_diffs, diffs)


class ErrorTests(TestCase):
    def test_unavailable_representation(self):
        error = versionedfile.UnavailableRepresentation(("key",), "mpdiff", "fulltext")
        self.assertEqualDiff(
            "The encoding 'mpdiff' is not available for key "
            "('key',) which is encoded as 'fulltext'.",
            str(error),
        )
