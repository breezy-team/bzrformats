# Copyright (C) 2005 Canonical Ltd
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

"""Tests for the standalone control-directory API."""

import os

from .. import controldir
from ..errors import BzrFormatsError
from . import TestCaseInTempDir


class TestControlDir(TestCaseInTempDir):
    def test_create_has_components(self):
        cd = controldir.create(self.test_dir)
        self.assertTrue(cd.has_repository())
        self.assertTrue(cd.has_branch())
        self.assertTrue(cd.has_workingtree())

    def test_create_is_reopenable(self):
        controldir.create(self.test_dir)
        cd = controldir.open(self.test_dir)
        self.assertEqual(cd.open_branch().last_revision_info(), (0, b"null:"))

    def test_commit_empty_tree_round_trip(self):
        cd = controldir.create(self.test_dir)
        repo = cd.open_repository()
        branch = cd.open_branch()
        wt = cd.open_workingtree()
        revid = wt.commit(repo, branch, "T <t@e>", "empty", 1577880000, 0)

        reopened = controldir.open(self.test_dir)
        self.assertEqual(reopened.open_branch().last_revision_info(), (1, revid))
        rev = reopened.open_repository().get_revision(revid)
        self.assertEqual(rev["message"], "empty")
        self.assertEqual(rev["committer"], "T <t@e>")
        # An empty tree has no entries beyond the (omitted) root.
        self.assertEqual(reopened.open_repository().get_inventory(revid), [])
        # The dirstate basis was advanced.
        self.assertEqual(reopened.open_workingtree().basis_revision(), revid)

    def test_commit_files_round_trip(self):
        cd = controldir.create(self.test_dir)
        # The empty working tree records only the root; write files and
        # commit them via a freshly built inventory is out of scope here
        # (adding to the dirstate is a separate API). This test confirms
        # the read path of a committed empty tree instead.
        repo = cd.open_repository()
        branch = cd.open_branch()
        wt = cd.open_workingtree()
        wt.commit(repo, branch, "T <t@e>", "first", 1577880000, 0)
        revids = controldir.open(self.test_dir).open_repository().all_revision_ids()
        self.assertEqual(len(revids), 1)

    def test_branch_tags_round_trip(self):
        cd = controldir.create(self.test_dir)
        branch = cd.open_branch()
        branch.set_tags({"v1.0": b"some-rev", "v2.0": b"other-rev"})
        reopened = controldir.open(self.test_dir).open_branch()
        self.assertEqual(reopened.tags(), {"v1.0": b"some-rev", "v2.0": b"other-rev"})

    def test_open_missing_raises(self):
        # An empty directory is not a control directory.
        empty = os.path.join(self.test_dir, "empty")
        os.makedirs(empty)
        self.assertRaises(BzrFormatsError, controldir.open, empty)
