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

"""Tests for the configobj-compatible ConfigObj binding."""

from io import BytesIO

from ..config import ConfigObj, quote_value, unquote_value
from . import TestCase


class TestConfigObjParse(TestCase):
    def test_construct_from_bytes(self):
        c = ConfigObj(b"a = 1\nb = two\n")
        self.assertEqual("1", c["a"])
        self.assertEqual("two", c["b"])

    def test_parse_classmethod(self):
        c = ConfigObj.parse(b"a = 1\n")
        self.assertEqual("1", c["a"])

    def test_empty_construction(self):
        self.assertEqual([], ConfigObj().scalars)
        self.assertEqual([], ConfigObj().sections)

    def test_scalars_and_sections(self):
        c = ConfigObj(b"a = 1\nb = 2\n[s1]\nx = y\n[s2]\nz = w\n")
        self.assertEqual(["a", "b"], c.scalars)
        self.assertEqual(["s1", "s2"], c.sections)

    def test_named_section_access(self):
        c = ConfigObj(b"[loc]\nkey = val\n")
        self.assertEqual("val", c["loc"]["key"])

    def test_missing_key_raises(self):
        self.assertRaises(KeyError, lambda: ConfigObj(b"a = 1\n")["missing"])

    def test_missing_equals_raises(self):
        self.assertRaises(ValueError, ConfigObj, b"not a config line\n")

    def test_non_utf8_raises(self):
        self.assertRaises(ValueError, ConfigObj, b"a = \xff\n")

    def test_unterminated_quote_raises(self):
        self.assertRaises(ValueError, ConfigObj, b'a = "oops\n')


class TestConfigObjMapping(TestCase):
    def test_contains(self):
        c = ConfigObj(b"a = 1\n[s]\nx = y\n")
        self.assertIn("a", c)
        self.assertIn("s", c)
        self.assertNotIn("missing", c)

    def test_keys_mixes_scalars_and_sections(self):
        # configobj's ConfigObj.keys() lists scalars then section names.
        c = ConfigObj(b"a = 1\nb = 2\n[s]\nx = y\n")
        self.assertEqual(["a", "b", "s"], c.keys())

    def test_get_with_default(self):
        c = ConfigObj(b"a = 1\n")
        self.assertEqual("1", c.get("a"))
        self.assertIsNone(c.get("missing"))
        self.assertEqual("d", c.get("missing", "d"))

    def test_set_top_level_scalar(self):
        c = ConfigObj(b"a = 1\n")
        c["a"] = "99"
        self.assertEqual("99", c["a"])

    def test_set_non_string_value_is_stringified(self):
        # configobj has stringify=True: non-str values are rendered via str().
        c = ConfigObj()
        c["flag"] = True
        c["n"] = 42
        self.assertEqual(b"flag = True\nn = 42\n", c.to_bytes())

    def test_del_top_level_scalar(self):
        c = ConfigObj(b"a = 1\nb = 2\n")
        del c["a"]
        self.assertNotIn("a", c)
        self.assertEqual(b"b = 2\n", c.to_bytes())

    def test_equality_by_content(self):
        self.assertEqual(ConfigObj(b"a = 1\n"), ConfigObj(b"a = 1\n"))
        self.assertNotEqual(ConfigObj(b"a = 1\n"), ConfigObj(b"b = 2\n"))


class TestSetdefault(TestCase):
    def test_returns_existing_section_live(self):
        c = ConfigObj(b"[loc]\nkey = val\n")
        sec = c.setdefault("loc", {})
        sec["new"] = "x"
        # The mutation writes through to the config.
        self.assertEqual("x", c["loc"]["new"])

    def test_creates_missing_section(self):
        c = ConfigObj()
        sec = c.setdefault("brand_new", {})
        sec["k"] = "v"
        self.assertEqual(b"[brand_new]\nk = v\n", c.to_bytes())

    def test_no_name_section_mutation_through_top_level(self):
        # breezy mutates the ConfigObj itself as the no-name section.
        c = ConfigObj(b"a = 1\n")
        c["b"] = "2"
        self.assertEqual(b"a = 1\nb = 2\n", c.to_bytes())


class TestSectionView(TestCase):
    def test_get_and_contains(self):
        sec = ConfigObj(b"[s]\na = 1\n")["s"]
        self.assertEqual("1", sec["a"])
        self.assertIn("a", sec)
        self.assertNotIn("missing", sec)
        self.assertEqual("d", sec.get("missing", "d"))

    def test_keys_in_order(self):
        sec = ConfigObj(b"[s]\nb = 1\na = 2\n")["s"]
        self.assertEqual(["b", "a"], sec.keys())

    def test_set_and_delete(self):
        c = ConfigObj(b"[s]\na = 1\nb = 2\n")
        sec = c["s"]
        sec["c"] = "3"
        del sec["a"]
        self.assertEqual(b"[s]\nb = 2\nc = 3\n", c.to_bytes())

    def test_compares_equal_to_plain_dict(self):
        sec = ConfigObj(b"[s]\na = 1\n")["s"]
        self.assertEqual({"a": "1"}, sec)

    def test_str_renders_like_dict(self):
        sec = ConfigObj(b"[s]\na = 1\n")["s"]
        self.assertEqual("{'a': '1'}", str(sec))

    def test_nested_subsection_access(self):
        c = ConfigObj(b"[baz]\nfoo = bar\n[[qux]]\nq = 1\n")
        baz = c["baz"]
        self.assertEqual("bar", baz["foo"])
        self.assertEqual({"q": "1"}, baz["qux"])
        self.assertEqual("1", baz["qux"]["q"])


class TestWrite(TestCase):
    def test_write_to_file(self):
        c = ConfigObj(b"a = 1\n[s]\nx = y\n")
        out = BytesIO()
        c.write(out)
        self.assertEqual(b"a = 1\n[s]\nx = y\n", out.getvalue())

    def test_to_bytes(self):
        self.assertEqual(b"a = 1\n", ConfigObj(b"a = 1\n").to_bytes())

    def test_round_trips_comments(self):
        data = b"# a comment\n\nnickname = trunk\n"
        self.assertEqual(data, ConfigObj(data).to_bytes())

    def test_triple_quoted_multiline_round_trips(self):
        data = b"multiline = '''1\n2\n'''\n"
        self.assertEqual(data, ConfigObj(data).to_bytes())


class TestQuoteMethods(TestCase):
    def test_quote_method(self):
        c = ConfigObj()
        self.assertEqual('" x "', c._quote(" x "))

    def test_unquote_method(self):
        c = ConfigObj()
        self.assertEqual("y", c._unquote("'y'"))

    def test_quote_method_raises_when_unquotable(self):
        c = ConfigObj()
        self.assertRaises(ValueError, c._quote, "a '''b''' c \"\"\"d\"\"\"")

    def test_quote_method_stringifies_non_string(self):
        c = ConfigObj()
        self.assertEqual("True", c._quote(True))
        self.assertEqual("42", c._quote(42))


class TestQuoting(TestCase):
    """quote_value/unquote_value match configobj's list-aware _quote/_unquote."""

    def test_quote_plain(self):
        self.assertEqual("plain", quote_value("plain"))

    def test_quote_empty(self):
        self.assertEqual('""', quote_value(""))

    def test_quote_leading_space(self):
        self.assertEqual('" leading"', quote_value(" leading"))

    def test_quote_comma(self):
        self.assertEqual('"a,b"', quote_value("a,b"))

    def test_quote_hash(self):
        self.assertEqual('"has#hash"', quote_value("has#hash"))

    def test_quote_value_containing_double_uses_single(self):
        self.assertEqual("'\" a b c \"'", quote_value('" a b c "'))

    def test_quote_newline_uses_triple(self):
        self.assertEqual("'''a\nb'''", quote_value("a\nb"))

    def test_quote_unquotable_raises(self):
        self.assertRaises(ValueError, quote_value, "a '''b''' c \"\"\"d\"\"\"")

    def test_unquote_pairs(self):
        self.assertEqual("x", unquote_value("'x'"))
        self.assertEqual("x", unquote_value('"x"'))
        self.assertEqual("x", unquote_value("x"))

    def test_unquote_lone_quote_becomes_empty(self):
        self.assertEqual("", unquote_value("'"))

    def test_quote_unquote_round_trip(self):
        for value in ['" a b c "', '" a , b c "', '","', '""']:
            self.assertEqual(value, quote_value(unquote_value(value)))
            self.assertEqual(value, unquote_value(quote_value(value)))
