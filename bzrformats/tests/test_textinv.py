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

"""Tests for the text inventory format."""

from .. import textinv
from . import TestCase


class TestTextInv(TestCase):
    def test_escape_avoids_spaces(self):
        escaped = textinv.escape("with space and\ttab")
        self.assertNotIn(" ", escaped)

    def test_escape_round_trips(self):
        for s in ["plain", "a b", "tab\there", "back\\slash", "new\nline"]:
            self.assertEqual(textinv.unescape(textinv.escape(s)), s)

    def test_unescape_rejects_space(self):
        self.assertRaises(ValueError, textinv.unescape, "has space")

    def test_write(self):
        out = textinv.write_text_inventory(
            [
                (b"dir-id", "a dir", "directory", b"TREE_ROOT"),
                (b"file-id", "hello.txt", "file", b"dir-id", b"hello-text", b"deadbeef", 12),
            ]
        )
        self.assertEqual(
            out,
            b"# bzr inventory format 3\n"
            b"dir-id a\\x20dir directory TREE_ROOT\n"
            b"file-id hello.txt file dir-id hello-text deadbeef 12\n"
            b"# end of inventory\n",
        )
