# Copyright (C) 2025 Canonical Ltd
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

"""Tests for the views file format bindings."""

from ..views import (
    deserialize_view_content,
    serialize_view_content,
    view_display_str,
)
from . import TestCase

MARKER = b"Bazaar views format 1\n"


class TestSerializeViewContent(TestCase):
    def test_empty(self):
        self.assertEqual(MARKER, serialize_view_content({}, {}))

    def test_keywords_only(self):
        self.assertEqual(
            MARKER + b"current=x\n", serialize_view_content({"current": "x"}, {})
        )

    def test_views_sorted_by_name(self):
        self.assertEqual(
            MARKER + b"views:\nb\x00c\nx\x00a\x00b\n",
            serialize_view_content({}, {"x": ["a", "b"], "b": ["c"]}),
        )

    def test_keywords_in_dict_order(self):
        self.assertEqual(
            MARKER + b"current=x\nz=1\na=2\n",
            serialize_view_content({"current": "x", "z": "1", "a": "2"}, {}),
        )

    def test_non_ascii(self):
        self.assertEqual(
            "Bazaar views format 1\ncurrent=ば\nviews:\nば\0foo\0bar/\n".encode(),
            serialize_view_content({"current": "ば"}, {"ば": ["foo", "bar/"]}),
        )


class TestDeserializeViewContent(TestCase):
    def test_empty_content(self):
        self.assertEqual(({}, {}), deserialize_view_content(b""))

    def test_marker_only(self):
        self.assertEqual(({}, {}), deserialize_view_content(MARKER))

    def test_keywords_and_views(self):
        self.assertEqual(
            ({"current": "x"}, {"x": ["a", "b"]}),
            deserialize_view_content(MARKER + b"current=x\nviews:\nx\x00a\x00b\n"),
        )

    def test_keyword_value_with_equals(self):
        self.assertEqual(
            ({"current": "a=b"}, {}),
            deserialize_view_content(MARKER + b"current=a=b\n"),
        )

    def test_unknown_keyword_preserved(self):
        content = MARKER + b"current=x\nfuture=thing\nviews:\nx\x00a\n"
        keywords, views = deserialize_view_content(content)
        self.assertEqual({"current": "x", "future": "thing"}, keywords)
        self.assertEqual(content, serialize_view_content(keywords, views))

    def test_keyword_order_preserved(self):
        content = MARKER + b"current=x\nz=1\na=2\n"
        keywords, views = deserialize_view_content(content)
        self.assertEqual(["current", "z", "a"], list(keywords))
        self.assertEqual(content, serialize_view_content(keywords, views))

    def test_view_with_no_paths(self):
        self.assertEqual(
            ({}, {"x": []}), deserialize_view_content(MARKER + b"views:\nx\n")
        )

    def test_missing_marker(self):
        e = self.assertRaises(ValueError, deserialize_view_content, b"nonsense\n")
        self.assertEqual("format marker missing from top of views file", str(e))

    def test_unsupported_format(self):
        e = self.assertRaises(
            ValueError, deserialize_view_content, b"Bazaar views format 2\n"
        )
        self.assertEqual("cannot decode views format 2", str(e))

    def test_marker_without_version(self):
        self.assertRaises(
            ValueError, deserialize_view_content, b"Bazaar views format x\n"
        )

    def test_unparsable_line(self):
        self.assertRaises(ValueError, deserialize_view_content, MARKER + b"bogusline\n")

    def test_blank_line_before_views(self):
        self.assertRaises(ValueError, deserialize_view_content, MARKER + b"\nviews:\n")

    def test_not_utf8(self):
        self.assertRaises(
            ValueError, deserialize_view_content, MARKER + b"current=\xff\n"
        )


class TestViewDisplayStr(TestCase):
    def test_empty(self):
        self.assertEqual("", view_display_str([]))

    def test_joins_with_commas(self):
        self.assertEqual("foo, bar", view_display_str(["foo", "bar"]))
