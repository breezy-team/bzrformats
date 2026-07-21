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

    def test_section_defaults_to_no_name(self):
        c = ConfigObj.parse(b"a = 1\n")
        self.assertEqual("1", c.section().get("a"))

    def test_no_name_section_absent_without_top_level_scalar(self):
        c = ConfigObj.parse(b"[s1]\nx = 1\n")
        self.assertIsNone(c.section(None))

    def test_sections_in_file_order(self):
        c = ConfigObj.parse(b"a = 1\n[s1]\nx = 1\n[s2]\ny = 2\n")
        self.assertEqual([None, "s1", "s2"], [s.id for s in c.sections()])

    def test_sections_omits_no_name_when_only_named(self):
        c = ConfigObj.parse(b"[s1]\nx = 1\n[s2]\ny = 2\n")
        self.assertEqual(["s1", "s2"], [s.id for s in c.sections()])

    def test_sections_empty_when_no_entries(self):
        c = ConfigObj.parse(b"# just a comment\n")
        self.assertEqual([], c.sections())

    def test_duplicate_key_last_value_wins(self):
        c = ConfigObj.parse(b"a = 1\na = 2\n")
        sec = c.section(None)
        self.assertEqual("2", sec.get("a"))
        self.assertEqual(["a"], sec.option_names())

    def test_interleaved_same_section_merges(self):
        c = ConfigObj.parse(b"[s1]\nx = 1\n[s2]\nz = 3\n[s1]\ny = 2\n")
        sec = c.section("s1")
        self.assertEqual("1", sec.get("x"))
        self.assertEqual("2", sec.get("y"))
        self.assertEqual(["x", "y"], sec.option_names())

    def test_empty_config_writes_nothing(self):
        self.assertEqual(b"", ConfigObj().to_bytes())

    def test_missing_equals_raises(self):
        self.assertRaises(ValueError, ConfigObj.parse, b"not a config line\n")

    def test_bad_section_header_raises(self):
        self.assertRaises(ValueError, ConfigObj.parse, b"[unterminated\n")

    def test_empty_section_name_raises(self):
        self.assertRaises(ValueError, ConfigObj.parse, b"[]\n")

    def test_nested_section_raises(self):
        self.assertRaises(ValueError, ConfigObj.parse, b"[[sub]]\n")

    def test_non_utf8_raises(self):
        self.assertRaises(ValueError, ConfigObj.parse, b"a = \xff\n")

    def test_key_name_is_unquoted(self):
        c = ConfigObj.parse(b'"k" = 1\n')
        self.assertEqual("1", c.section(None).get("k"))

    def test_section_name_is_unquoted(self):
        c = ConfigObj.parse(b"['/a path']\nk = v\n")
        self.assertEqual("v", c.section("/a path").get("k"))


class TestConfigObjParseValues(TestCase):
    """The raw value the parser stores (list_values=False keeps quotes)."""

    def test_quoted_value_keeps_quotes(self):
        c = ConfigObj.parse(b"a = 'q'\n")
        self.assertEqual("'q'", c.section(None).get("a"))

    def test_double_quoted_value_keeps_quotes(self):
        c = ConfigObj.parse(b'a = "q"\n')
        self.assertEqual('"q"', c.section(None).get("a"))

    def test_hash_inside_quotes_is_not_a_comment(self):
        c = ConfigObj.parse(b"a = '#x'\n")
        self.assertEqual("'#x'", c.section(None).get("a"))

    def test_inline_comment_stripped_from_value(self):
        c = ConfigObj.parse(b"a = 1 # hi\n")
        self.assertEqual("1", c.section(None).get("a"))

    def test_quoted_value_with_trailing_comment(self):
        c = ConfigObj.parse(b'a = "v a l" # tail\n')
        self.assertEqual('"v a l"', c.section(None).get("a"))

    def test_unterminated_quote_is_an_error(self):
        # configobj raises a parse error for a value that opens a quote it never
        # closes; the binding surfaces that as ValueError.
        self.assertRaises(ValueError, ConfigObj.parse, b'a = "oops\n')

    def test_triple_quoted_multiline_value(self):
        c = ConfigObj.parse(b"a = '''1\n2\n'''\n")
        self.assertEqual("1\n2\n", c.section(None).get("a"))

    def test_hash_in_unquoted_value_starts_comment(self):
        c = ConfigObj.parse(b"a = has#hash\n")
        self.assertEqual("has", c.section(None).get("a"))

    def test_quoted_value_followed_by_more_is_kept_whole(self):
        c = ConfigObj.parse(b'a = " bar", "baz "\n')
        self.assertEqual('" bar", "baz "', c.section(None).get("a"))


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

    def test_repr_named(self):
        sec = ConfigObj.parse(b"[loc]\nk = v\n").section("loc")
        self.assertEqual('<Section "loc">', repr(sec))

    def test_repr_no_name(self):
        sec = ConfigObj.parse(b"k = v\n").section(None)
        self.assertEqual("<Section (no name)>", repr(sec))


class TestSectionTree(TestCase):
    def test_empty_headers_preserved(self):
        c = ConfigObj.parse(b"[/foo]\n[/foo/bar]\n")
        self.assertEqual(
            ["/foo", "/foo/bar"],
            [name for name, _opts, _subs in c.section_tree()],
        )

    def test_no_name_section_first(self):
        c = ConfigObj.parse(b"a = 1\n[s]\nb = 2\n")
        tree = c.section_tree()
        self.assertEqual(
            [(None, [("a", "1")], []), ("s", [("b", "2")], [])], tree
        )

    def test_nested_subsections(self):
        c = ConfigObj.parse(
            b"[baz]\nfoo_in_baz = barbaz\n[[qux]]\nfoo_in_qux = quux\n"
        )
        self.assertEqual(
            [
                (
                    "baz",
                    [("foo_in_baz", "barbaz")],
                    [("qux", [("foo_in_qux", "quux")])],
                )
            ],
            c.section_tree(),
        )


class TestSetSubsectionValue(TestCase):
    def test_builds_nested_structure_from_empty(self):
        c = ConfigObj()
        c.set_value("baz", "foo_in_baz", "barbaz")
        c.set_subsection_value("baz", "qux", "foo_in_qux", "quux")
        self.assertEqual(
            b"[baz]\nfoo_in_baz = barbaz\n[[qux]]\nfoo_in_qux = quux\n",
            c.to_bytes(),
        )

    def test_updates_in_place(self):
        c = ConfigObj.parse(b"[baz]\n[[qux]]\na = 1\n")
        c.set_subsection_value("baz", "qux", "a", "2")
        self.assertEqual(b"[baz]\n[[qux]]\na = 2\n", c.to_bytes())


class TestConfigObjWrite(TestCase):
    def test_set_value_updates_in_place(self):
        c = ConfigObj.parse(b"a = 1\nb = 2\n")
        c.set_value(None, "a", "99")
        self.assertEqual(b"a = 99\nb = 2\n", c.to_bytes())

    def test_set_value_creates_section(self):
        c = ConfigObj.parse(b"a = 1\n")
        c.set_value("loc", "k", "v")
        self.assertEqual(b"a = 1\n[loc]\nk = v\n", c.to_bytes())

    def test_set_value_new_no_name_key_lands_before_sections(self):
        c = ConfigObj.parse(b"a = 1\n[s]\nx = y\n")
        c.set_value(None, "b", "2")
        self.assertEqual(b"a = 1\nb = 2\n[s]\nx = y\n", c.to_bytes())

    def test_set_value_appends_after_last_entry_of_section(self):
        c = ConfigObj.parse(b"[s1]\nx = 1\ny = 2\n[s2]\nz = 3\n")
        c.set_value("s1", "w", "4")
        self.assertEqual(b"[s1]\nx = 1\ny = 2\nw = 4\n[s2]\nz = 3\n", c.to_bytes())

    def test_set_value_into_empty_header_section(self):
        c = ConfigObj.parse(b"[s1]\n[s2]\nz = 3\n")
        c.set_value("s1", "k", "v")
        self.assertEqual(b"[s1]\nk = v\n[s2]\nz = 3\n", c.to_bytes())

    def test_set_value_targets_first_interleaved_block(self):
        c = ConfigObj.parse(b"[s1]\nx = 1\n[s2]\nz = 3\n[s1]\nw = 4\n")
        c.set_value("s1", "y", "2")
        self.assertEqual(
            b"[s1]\nx = 1\ny = 2\n[s2]\nz = 3\n[s1]\nw = 4\n", c.to_bytes()
        )

    def test_set_value_on_empty_config(self):
        c = ConfigObj()
        c.set_value(None, "a", "1")
        self.assertEqual(b"a = 1\n", c.to_bytes())

    def test_remove_value(self):
        c = ConfigObj.parse(b"a = 1\nb = 2\n")
        c.remove_value(None, "a")
        self.assertEqual(b"b = 2\n", c.to_bytes())

    def test_remove_absent_value_is_noop(self):
        c = ConfigObj.parse(b"a = 1\n")
        c.remove_value(None, "missing")
        self.assertEqual(b"a = 1\n", c.to_bytes())

    def test_remove_value_from_missing_section_is_noop(self):
        c = ConfigObj.parse(b"a = 1\n")
        c.remove_value("nope", "a")
        self.assertEqual(b"a = 1\n", c.to_bytes())

    def test_round_trips_comments_and_blanks(self):
        data = b"# a comment\n\nnickname = trunk\n"
        self.assertEqual(data, ConfigObj.parse(data).to_bytes())

    def test_missing_trailing_newline_is_added(self):
        c = ConfigObj.parse(b"a = 1")
        self.assertEqual(b"a = 1\n", c.to_bytes())

    def test_inline_comment_round_trips(self):
        data = b"a = 1 # hi\n"
        c = ConfigObj.parse(data)
        self.assertEqual("1", c.section(None).get("a"))
        self.assertEqual(data, c.to_bytes())

    def test_quoted_value_with_comment_round_trips(self):
        data = b'a = "v a l" # tail\n'
        self.assertEqual(data, ConfigObj.parse(data).to_bytes())


class TestQuoting(TestCase):
    def test_quote_plain(self):
        self.assertEqual("plain", quote_value("plain"))

    def test_quote_empty(self):
        self.assertEqual('""', quote_value(""))

    def test_quote_leading_space(self):
        self.assertEqual("' leading'", quote_value(" leading"))

    def test_quote_trailing_space(self):
        self.assertEqual("'trailing '", quote_value("trailing "))

    def test_quote_leading_tab(self):
        self.assertEqual("'\tleading'", quote_value("\tleading"))

    def test_quote_comma(self):
        self.assertEqual("'a,b'", quote_value("a,b"))

    def test_quote_hash(self):
        self.assertEqual("'has#hash'", quote_value("has#hash"))

    def test_quote_mid_string_quote_not_quoted(self):
        # A single quote in the middle is not an edge char and there is no
        # comma or hash, so no quoting is applied.
        self.assertEqual("has'quote", quote_value("has'quote"))

    def test_quote_needing_double(self):
        self.assertEqual('"a,\'b"', quote_value("a,'b"))

    def test_unquote_single_pair(self):
        self.assertEqual("x", unquote_value("'x'"))

    def test_unquote_double_pair(self):
        self.assertEqual("x", unquote_value('"x"'))

    def test_unquote_unquoted(self):
        self.assertEqual("x", unquote_value("x"))

    def test_unquote_mismatched_edges_left_alone(self):
        self.assertEqual("'x\"", unquote_value("'x\""))

    def test_unquote_single_char_left_alone(self):
        self.assertEqual("'", unquote_value("'"))

    def test_quote_unquote_round_trip(self):
        for value in ["plain", "", " lead", "a,b", "has#hash", "a,'b"]:
            self.assertEqual(value, unquote_value(quote_value(value)))
