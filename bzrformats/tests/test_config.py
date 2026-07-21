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

"""Tests for the ConfigObj parser bindings."""

from ..config import ConfigObj, Section, quote_value, unquote_value
from . import TestCase


class TestConfigObjParse(TestCase):
    def test_parse_no_name_section(self):
        c = ConfigObj.parse(b"a = 1\nb = two\n")
        sec = c.section(None)
        self.assertEqual("1", sec.get("a"))
        self.assertEqual("two", sec.get("b"))

    def test_parse_named_section(self):
        c = ConfigObj.parse(b"top = x\n[/home/foo]\nkey = val\n")
        self.assertEqual("x", c.section(None).get("top"))
        self.assertEqual("val", c.section("/home/foo").get("key"))

    def test_absent_section_is_none(self):
        c = ConfigObj.parse(b"a = 1\n")
        self.assertIsNone(c.section("missing"))

    def test_absent_key_is_none(self):
        c = ConfigObj.parse(b"a = 1\n")
        self.assertIsNone(c.section(None).get("missing"))

    def test_sections_in_file_order(self):
        c = ConfigObj.parse(b"a = 1\n[s1]\nx = 1\n[s2]\ny = 2\n")
        self.assertEqual([None, "s1", "s2"], [s.id for s in c.sections()])

    def test_empty_config_writes_nothing(self):
        self.assertEqual(b"", ConfigObj().to_bytes())

    def test_missing_equals_raises(self):
        self.assertRaises(ValueError, ConfigObj.parse, b"not a config line\n")


class TestSection(TestCase):
    def test_id_of_named_section(self):
        c = ConfigObj.parse(b"[loc]\nk = v\n")
        self.assertEqual("loc", c.section("loc").id)

    def test_id_of_no_name_section(self):
        c = ConfigObj.parse(b"k = v\n")
        self.assertIsNone(c.section(None).id)

    def test_option_names_in_order(self):
        c = ConfigObj.parse(b"b = 1\na = 2\nc = 3\n")
        self.assertEqual(["b", "a", "c"], c.section(None).option_names())

    def test_contains(self):
        sec = ConfigObj.parse(b"a = 1\n").section(None)
        self.assertIn("a", sec)
        self.assertNotIn("missing", sec)

    def test_is_section(self):
        self.assertIsInstance(ConfigObj.parse(b"a = 1\n").section(None), Section)


class TestConfigObjWrite(TestCase):
    def test_set_value_updates_in_place(self):
        c = ConfigObj.parse(b"a = 1\nb = 2\n")
        c.set_value(None, "a", "99")
        self.assertEqual(b"a = 99\nb = 2\n", c.to_bytes())

    def test_set_value_creates_section(self):
        c = ConfigObj.parse(b"a = 1\n")
        c.set_value("loc", "k", "v")
        self.assertEqual(b"a = 1\n[loc]\nk = v\n", c.to_bytes())

    def test_set_value_appends_after_last_entry_of_section(self):
        c = ConfigObj.parse(b"[s1]\nx = 1\ny = 2\n[s2]\nz = 3\n")
        c.set_value("s1", "w", "4")
        self.assertEqual(
            b"[s1]\nx = 1\ny = 2\nw = 4\n[s2]\nz = 3\n", c.to_bytes()
        )

    def test_remove_value(self):
        c = ConfigObj.parse(b"a = 1\nb = 2\n")
        c.remove_value(None, "a")
        self.assertEqual(b"b = 2\n", c.to_bytes())

    def test_round_trips_comments_and_blanks(self):
        data = b"# a comment\n\nnickname = trunk\n"
        self.assertEqual(data, ConfigObj.parse(data).to_bytes())

    def test_inline_comment_round_trips(self):
        data = b"a = 1 # hi\n"
        c = ConfigObj.parse(data)
        self.assertEqual("1", c.section(None).get("a"))
        self.assertEqual(data, c.to_bytes())


class TestQuoting(TestCase):
    def test_quote_plain(self):
        self.assertEqual("plain", quote_value("plain"))

    def test_quote_empty(self):
        self.assertEqual('""', quote_value(""))

    def test_quote_leading_space(self):
        self.assertEqual("' leading'", quote_value(" leading"))

    def test_quote_comma(self):
        self.assertEqual("'a,b'", quote_value("a,b"))

    def test_quote_hash(self):
        self.assertEqual("'has#hash'", quote_value("has#hash"))

    def test_quote_needing_double(self):
        self.assertEqual("\"a,'b\"", quote_value("a,'b"))

    def test_unquote_pairs(self):
        self.assertEqual("x", unquote_value("'x'"))
        self.assertEqual("x", unquote_value('"x"'))
        self.assertEqual("x", unquote_value("x"))


class TestConfigObjAgainstConfigObj(TestCase):
    """Cross-check against the reference configobj library.

    Only the semantics the Rust parser deliberately shares with configobj
    (list_values=False, interpolation off) are compared.
    """

    def _reference(self, data):
        import configobj

        return configobj.ConfigObj(
            data.splitlines(), list_values=False, interpolation=False
        )

    def test_matches_reference_scalars(self):
        data = b"a = 1\nb = two\n"
        ref = self._reference(data)
        sec = ConfigObj.parse(data).section(None)
        self.assertEqual(ref["a"], sec.get("a"))
        self.assertEqual(ref["b"], sec.get("b"))

    def test_matches_reference_named_section(self):
        data = b"[loc]\nkey = val\n"
        ref = self._reference(data)
        sec = ConfigObj.parse(data).section("loc")
        self.assertEqual(ref["loc"]["key"], sec.get("key"))

    def test_matches_reference_quoted_value_kept(self):
        # With list_values=False configobj keeps surrounding quotes.
        data = b"a = 'q'\n"
        ref = self._reference(data)
        sec = ConfigObj.parse(data).section(None)
        self.assertEqual(ref["a"], sec.get("a"))
        self.assertEqual("'q'", sec.get("a"))
