# Copyright (C) 2005-2011, 2016 Canonical Ltd
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


from . import TestCase
from ..revision import Revision


class TestRevisionMethods(TestCase):
    def test_get_summary(self):
        r = Revision(
            b"1",
            parent_ids=[],
            committer="",
            message="a",
            timestamp=0,
            timezone=0,
            inventory_sha1=None,
            properties={},
        )
        self.assertEqual("a", r.get_summary())
        r = Revision(
            b"1",
            parent_ids=[],
            committer="",
            message="a\nb",
            timestamp=0,
            timezone=0,
            inventory_sha1=None,
            properties={},
        )
        self.assertEqual("a", r.get_summary())
        r = Revision(
            b"1",
            parent_ids=[],
            committer="",
            message="\na\nb",
            timestamp=0,
            timezone=0,
            inventory_sha1=None,
            properties={},
        )
        self.assertEqual("a", r.get_summary())
        r = Revision(
            b"1",
            parent_ids=[],
            committer="",
            message="",
            timestamp=0,
            timezone=0,
            inventory_sha1=None,
            properties={},
        )
        self.assertEqual("", r.get_summary())

    def test_get_apparent_authors(self):
        r = Revision(
            b"1",
            parent_ids=[],
            committer="A",
            message="",
            timestamp=0,
            timezone=0,
            inventory_sha1=None,
            properties={},
        )
        self.assertEqual(["A"], r.get_apparent_authors())
        r = Revision(
            b"1",
            parent_ids=[],
            committer="A",
            message="",
            timestamp=0,
            timezone=0,
            inventory_sha1=None,
            properties={"author": "B"},
        )
        self.assertEqual(["B"], r.get_apparent_authors())
        r = Revision(
            b"1",
            parent_ids=[],
            committer="A",
            message="",
            timestamp=0,
            timezone=0,
            inventory_sha1=None,
            properties={"author": "B", "authors": "C\nD"},
        )
        self.assertEqual(["C", "D"], r.get_apparent_authors())

    def test_get_apparent_authors_no_committer(self):
        r = Revision(
            b"1",
            parent_ids=[],
            committer="",
            message="",
            timestamp=0,
            timezone=0,
            inventory_sha1=None,
            properties={},
        )
        self.assertEqual([], r.get_apparent_authors())
