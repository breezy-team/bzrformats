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
        revid = wt.commit(
            repo, branch, "T <t@e>", "empty", 1577880000, 0, allow_pointless=True
        )

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
        with open(os.path.join(self.test_dir, "a.txt"), "wb") as f:
            f.write(b"hello\n")
        wt = cd.open_workingtree()
        file_id = wt.add("a.txt", "file")
        revid = wt.commit(
            cd.open_repository(), cd.open_branch(), "T <t@e>", "add a", 1577880000, 0
        )

        reopened = controldir.open(self.test_dir)
        repo = reopened.open_repository()
        self.assertEqual(repo.all_revision_ids(), [revid])
        inv = repo.get_inventory(revid)
        self.assertEqual([entry[0] for entry in inv], ["a.txt"])
        self.assertEqual(repo.get_file_text(file_id, revid), b"hello\n")

    def test_commit_records_revprops_and_authors(self):
        cd = controldir.create(self.test_dir)
        with open(os.path.join(self.test_dir, "a.txt"), "wb") as f:
            f.write(b"hi\n")
        wt = cd.open_workingtree()
        wt.add("a.txt", "file")
        revid = wt.commit(
            cd.open_repository(),
            cd.open_branch(),
            "T <t@e>",
            "msg",
            1577880000,
            0,
            revprops={"custom": b"val"},
            authors=["A <a@e>", "B <b@e>"],
        )
        rev = controldir.open(self.test_dir).open_repository().get_revision(revid)
        self.assertEqual(rev["properties"]["custom"], b"val")
        self.assertEqual(rev["properties"]["authors"], b"A <a@e>\nB <b@e>")

    def test_pointless_commit_refused(self):
        cd = controldir.create(self.test_dir)
        with open(os.path.join(self.test_dir, "a.txt"), "wb") as f:
            f.write(b"hi\n")
        wt = cd.open_workingtree()
        wt.add("a.txt", "file")
        wt.commit(cd.open_repository(), cd.open_branch(), "T <t@e>", "first", 1577880000, 0)
        # A second commit with nothing changed is refused.
        wt2 = controldir.open(self.test_dir).open_workingtree()
        self.assertRaises(
            Exception,
            wt2.commit,
            cd.open_repository(),
            cd.open_branch(),
            "T <t@e>",
            "empty",
            1577890000,
            0,
        )

    def test_add_versions_and_persists(self):
        cd = controldir.create(self.test_dir)
        with open(os.path.join(self.test_dir, "a.txt"), "wb") as f:
            f.write(b"x\n")
        wt = cd.open_workingtree()
        file_id = wt.add("a.txt", "file")
        self.assertEqual(wt.path2id("a.txt"), file_id)
        self.assertEqual(wt.list_files(), [("a.txt", "file", file_id)])
        # Re-opening reads the same versioned set from disk.
        reread = controldir.open(self.test_dir).open_workingtree()
        self.assertEqual(reread.path2id("a.txt"), file_id)

    def test_remove_unversions_without_deleting(self):
        cd = controldir.create(self.test_dir)
        with open(os.path.join(self.test_dir, "a.txt"), "wb") as f:
            f.write(b"x\n")
        wt = cd.open_workingtree()
        wt.add("a.txt", "file")
        wt.remove("a.txt")
        self.assertIs(wt.path2id("a.txt"), None)
        # The file is left on disk.
        self.assertTrue(os.path.exists(os.path.join(self.test_dir, "a.txt")))

    def test_rename_keeps_file_id(self):
        cd = controldir.create(self.test_dir)
        with open(os.path.join(self.test_dir, "a.txt"), "wb") as f:
            f.write(b"x\n")
        wt = cd.open_workingtree()
        file_id = wt.add("a.txt", "file")
        wt.rename("a.txt", "b.txt")
        self.assertIs(wt.path2id("a.txt"), None)
        self.assertEqual(wt.path2id("b.txt"), file_id)
        self.assertFalse(os.path.exists(os.path.join(self.test_dir, "a.txt")))
        self.assertTrue(os.path.exists(os.path.join(self.test_dir, "b.txt")))

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
