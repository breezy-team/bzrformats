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

"""Tests for testaments."""

from ..testament import Testament
from . import TestCase

REV_2_ENTRIES = [
    ("hello", "file", b"hello-id", b"34dd0ac19a24bf80c4d33b5c8960196e8d8d1f73", b"test@user-2", True),
    ("src", "directory", b"src-id", b"", b"test@user-2", False),
    ("src/foo.c", "file", b"foo.c-id", b"a2a049c20f908ae31b231d98779eb63c66448f24", b"test@user-2", False),
]


def rev2():
    return Testament(
        b"test@user-2",
        "test@user",
        1129025483,
        36000,
        "add files and directories",
        [b"test@user-1"],
        {"branch-nick": "test branch"},
        REV_2_ENTRIES,
    )


class TestTestament(TestCase):
    def test_version_1(self):
        expected = (
            b"bazaar-ng testament version 1\n"
            b"revision-id: test@user-2\n"
            b"committer: test@user\n"
            b"timestamp: 1129025483\n"
            b"timezone: 36000\n"
            b"parents:\n"
            b"  test@user-1\n"
            b"message:\n"
            b"  add files and directories\n"
            b"inventory:\n"
            b"  file hello hello-id 34dd0ac19a24bf80c4d33b5c8960196e8d8d1f73\n"
            b"  directory src src-id\n"
            b"  file src/foo.c foo.c-id a2a049c20f908ae31b231d98779eb63c66448f24\n"
            b"properties:\n"
            b"  branch-nick:\n"
            b"    test branch\n"
        )
        self.assertEqual(rev2().as_text("1"), expected)

    def test_strict(self):
        expected = (
            b"bazaar-ng testament version 2.1\n"
            b"revision-id: test@user-2\n"
            b"committer: test@user\n"
            b"timestamp: 1129025483\n"
            b"timezone: 36000\n"
            b"parents:\n"
            b"  test@user-1\n"
            b"message:\n"
            b"  add files and directories\n"
            b"inventory:\n"
            b"  file hello hello-id 34dd0ac19a24bf80c4d33b5c8960196e8d8d1f73 test@user-2 yes\n"
            b"  directory src src-id test@user-2 no\n"
            b"  file src/foo.c foo.c-id a2a049c20f908ae31b231d98779eb63c66448f24 test@user-2 no\n"
            b"properties:\n"
            b"  branch-nick:\n"
            b"    test branch\n"
        )
        self.assertEqual(rev2().as_text("strict"), expected)

    def test_short_form(self):
        t = rev2()
        short = t.as_short_text("1")
        self.assertTrue(short.startswith(b"bazaar-ng testament short form 1\n"))
        self.assertIn(b"revision-id: test@user-2\n", short)
        self.assertIn(t.as_sha1("1"), short)

    def test_whitespace_in_revision_id_rejected(self):
        bad = Testament(b"bad id", "c", 1, 0, "m", [], {}, [])
        self.assertRaises(ValueError, bad.as_text, "1")
