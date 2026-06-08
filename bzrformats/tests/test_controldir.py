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
from ..errors import (
    BzrFormatsError,
    NotStacked,
    UnsupportedOperation,
)
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
        wt.commit(
            cd.open_repository(), cd.open_branch(), "T <t@e>", "first", 1577880000, 0
        )
        # A second commit with nothing changed is refused.
        wt2 = controldir.open(self.test_dir).open_workingtree()
        self.assertRaises(
            BzrFormatsError,
            wt2.commit,
            cd.open_repository(),
            cd.open_branch(),
            "T <t@e>",
            "empty",
            1577890000,
            0,
        )

    def test_strict_commit_refuses_unknown_files(self):
        cd = controldir.create(self.test_dir)
        with open(os.path.join(self.test_dir, "a.txt"), "wb") as f:
            f.write(b"a\n")
        with open(os.path.join(self.test_dir, "loose.txt"), "wb") as f:
            f.write(b"l\n")
        wt = cd.open_workingtree()
        wt.add("a.txt", "file")
        self.assertEqual(wt.unknowns(), ["loose.txt"])
        self.assertRaises(
            BzrFormatsError,
            wt.commit,
            cd.open_repository(),
            cd.open_branch(),
            "T <t@e>",
            "c",
            1577880000,
            0,
            strict=True,
        )

    def test_selective_commit_records_named_file_only(self):
        cd = controldir.create(self.test_dir)
        for n, c in [("a.txt", b"a1\n"), ("b.txt", b"b1\n")]:
            with open(os.path.join(self.test_dir, n), "wb") as f:
                f.write(c)
        wt = cd.open_workingtree()
        a_id = wt.add("a.txt", "file")
        b_id = wt.add("b.txt", "file")
        rev1 = wt.commit(
            cd.open_repository(), cd.open_branch(), "T <t@e>", "two", 1577880000, 0
        )
        for n, c in [("a.txt", b"a2\n"), ("b.txt", b"b2\n")]:
            with open(os.path.join(self.test_dir, n), "wb") as f:
                f.write(c)
        wt2 = controldir.open(self.test_dir).open_workingtree()
        rev2 = wt2.commit(
            cd.open_repository(),
            cd.open_branch(),
            "T <t@e>",
            "only a",
            1577890000,
            0,
            specific_files=["a.txt"],
        )
        repo = controldir.open(self.test_dir).open_repository()
        # a.txt has the new content in rev2; b.txt keeps the rev1 content.
        self.assertEqual(repo.get_file_text(a_id, rev2), b"a2\n")
        self.assertEqual(repo.get_file_text(b_id, rev1), b"b1\n")

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

    def test_views_round_trip(self):
        cd = controldir.create(self.test_dir)
        wt = cd.open_workingtree()
        self.assertTrue(wt.supports_views())
        self.assertEqual(wt.views(), (None, {}))
        wt.set_views({"my": ["src", "doc"], "other": ["lib"]}, current="my")
        current, views = controldir.open(self.test_dir).open_workingtree().views()
        self.assertEqual(current, "my")
        self.assertEqual(views, {"my": ["src", "doc"], "other": ["lib"]})

    def test_conflicts_round_trip(self):
        cd = controldir.create(self.test_dir)
        wt = cd.open_workingtree()
        self.assertEqual(wt.conflicts(), [])
        wt.set_conflicts(
            [
                {"type": "text conflict", "path": "a.txt", "file_id": b"a-id"},
                {"type": "path conflict", "path": "dir/b"},
            ]
        )
        got = controldir.open(self.test_dir).open_workingtree().conflicts()
        self.assertEqual(
            got,
            [
                {"type": "text conflict", "path": "a.txt", "file_id": b"a-id"},
                {"type": "path conflict", "path": "dir/b"},
            ],
        )

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

    def test_stacked_on_url_not_stacked_raises(self):
        branch = controldir.create(self.test_dir).open_branch()
        self.assertRaises(NotStacked, branch.get_stacked_on_url)

    def test_stacked_on_url_round_trip(self):
        base = os.path.join(self.test_dir, "base")
        os.makedirs(base)
        controldir.create(base)
        top = os.path.join(self.test_dir, "top")
        os.makedirs(top)
        controldir.create(top).open_branch().set_stacked_on_url(base)
        self.assertEqual(controldir.open(top).open_branch().get_stacked_on_url(), base)

    def test_open_repository_stacked_reads_through_base(self):
        base = os.path.join(self.test_dir, "base")
        os.makedirs(base)
        base_cd = controldir.create(base)
        wt = base_cd.open_workingtree()
        revid = wt.commit(
            base_cd.open_repository(),
            base_cd.open_branch(),
            "T <t@e>",
            "base",
            1577880000,
            0,
            allow_pointless=True,
        )

        top = os.path.join(self.test_dir, "top")
        os.makedirs(top)
        top_cd = controldir.create(top)
        top_cd.open_branch().set_stacked_on_url(base)

        # The plain repository lacks the base revision; the stacked one has it.
        self.assertFalse(top_cd.open_repository().has_revision(revid))
        self.assertTrue(top_cd.open_repository_stacked().has_revision(revid))

    def test_bind_unbind_round_trip(self):
        branch = controldir.create(self.test_dir).open_branch()
        self.assertIsNone(branch.get_bound_location())
        branch.bind("file:///srv/master")
        self.assertEqual(branch.get_bound_location(), "file:///srv/master")
        branch.unbind()
        self.assertIsNone(branch.get_bound_location())
        self.assertEqual(branch.get_old_bound_location(), "file:///srv/master")

    def test_reference_info_round_trip(self):
        branch = controldir.create(self.test_dir).open_branch()
        self.assertEqual(branch.get_reference_info(b"fid"), (None, None))
        branch.set_reference_info(b"fid", "../subtree", "sub/dir")
        self.assertEqual(branch.get_reference_info(b"fid"), ("../subtree", "sub/dir"))

    def test_get_reference_none_on_normal_branch(self):
        branch = controldir.create(self.test_dir).open_branch()
        self.assertIsNone(branch.get_reference())
        self.assertRaises(UnsupportedOperation, branch.set_reference, "x")

    def test_create_shared_repository(self):
        shared = os.path.join(self.test_dir, "shared")
        os.makedirs(shared)
        cd = controldir.create_shared_repository(shared)
        self.assertTrue(cd.has_repository())
        self.assertFalse(cd.has_branch())
        self.assertTrue(cd.is_shared())
        # A normal control directory is not shared.
        normal = os.path.join(self.test_dir, "normal")
        os.makedirs(normal)
        self.assertFalse(controldir.create(normal).is_shared())

    def test_make_working_trees_toggles(self):
        shared = os.path.join(self.test_dir, "shared")
        os.makedirs(shared)
        cd = controldir.create_shared_repository(shared)
        self.assertTrue(cd.make_working_trees())
        cd.set_make_working_trees(False)
        self.assertFalse(cd.make_working_trees())
        cd.set_make_working_trees(True)
        self.assertTrue(cd.make_working_trees())

    def test_pack_keeps_data_readable(self):
        cd = controldir.create(self.test_dir)
        r1 = cd.open_workingtree().commit(
            cd.open_repository(),
            cd.open_branch(),
            "T <t@e>",
            "one",
            1577880000,
            0,
            allow_pointless=True,
        )
        reopened = controldir.open(self.test_dir)
        r2 = reopened.open_workingtree().commit(
            reopened.open_repository(),
            reopened.open_branch(),
            "T <t@e>",
            "two",
            1577890000,
            0,
            allow_pointless=True,
        )
        # pack() then both revisions still read back.
        controldir.open(self.test_dir).open_repository().pack()
        repo = controldir.open(self.test_dir).open_repository()
        self.assertTrue(repo.has_revision(r1))
        self.assertTrue(repo.has_revision(r2))
        self.assertEqual(repo.get_revision(r1)["message"], "one")
        # autopack on a small repository does nothing.
        self.assertFalse(controldir.open(self.test_dir).open_repository().autopack())
