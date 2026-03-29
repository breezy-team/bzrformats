# Copyright (C) 2008-2011 Canonical Ltd
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

"""Tests for the python and pyrex extensions of groupcompress."""

import sys

from testscenarios import load_tests_apply_scenarios

from .. import groupcompress
from .._bzr_rs import groupcompress as _groupcompress_rs
from . import TestCase

_groupcompress_rust = _groupcompress_rs


def module_scenarios():
    scenarios = [
        (
            "line",
            {"make_delta": groupcompress.make_line_delta},
        ),
        ("rabin", {"make_delta": groupcompress.make_rabin_delta}),
    ]
    return scenarios


def two_way_scenarios():
    scenarios = [
        ("LR", {"make_delta": groupcompress.make_line_delta}),
        ("RR", {"make_delta": groupcompress.make_rabin_delta}),
    ]
    return scenarios


load_tests = load_tests_apply_scenarios


_text1 = b"""\
This is a bit
of source text
which is meant to be matched
against other text
"""

_text2 = b"""\
This is a bit
of source text
which is meant to differ from
against other text
"""

_text3 = b"""\
This is a bit
of source text
which is meant to be matched
against other text
except it also
has a lot more data
at the end of the file
"""

_first_text = b"""\
a bit of text, that
does not have much in
common with the next text
"""

_second_text = b"""\
some more bit of text, that
does not have much in
common with the previous text
and has some extra text
"""


_third_text = b"""\
a bit of text, that
has some in common with the previous text
and has some extra text
and not have much in
common with the next text
"""

_fourth_text = b"""\
123456789012345
same rabin hash
123456789012345
same rabin hash
123456789012345
same rabin hash
123456789012345
same rabin hash
"""


class TestMakeAndApplyDelta(TestCase):
    scenarios = module_scenarios()
    _gc_module = None  # Set by load_tests

    def setUp(self):
        super().setUp()
        self.apply_delta = _groupcompress_rs.apply_delta
        self.apply_delta_to_source = _groupcompress_rs.apply_delta_to_source

    def test_make_delta_is_typesafe(self):
        self.make_delta(b"a string", b"another string")

        def _check_make_delta(string1, string2):
            self.assertRaises(TypeError, self.make_delta, string1, string2)

        _check_make_delta(b"a string", object())
        _check_make_delta(b"a string", "not a string")
        _check_make_delta(object(), b"a string")
        _check_make_delta("not a string", b"a string")

    def test_make_noop_delta(self):
        ident_delta = self.make_delta(_text1, _text1)
        self.assertEqual(b"M\x90M", ident_delta)
        ident_delta = self.make_delta(_text2, _text2)
        self.assertEqual(b"N\x90N", ident_delta)
        ident_delta = self.make_delta(_text3, _text3)
        self.assertEqual(b"\x87\x01\x90\x87", ident_delta)

    def assertDeltaIn(self, delta1, delta2, delta):
        """Make sure that the delta bytes match one of the expectations."""
        # In general, the python delta matcher gives different results than the
        # pyrex delta matcher. Both should be valid deltas, though.
        if delta not in (delta1, delta2):
            self.fail(
                b"Delta bytes:\n"
                b"       %r\n"
                b"not in %r\n"
                b"    or %r" % (delta, delta1, delta2)
            )

    def test_make_delta(self):
        delta = self.make_delta(_text1, _text2)
        self.assertDeltaIn(
            b"N\x90/\x1fdiffer from\nagainst other text\n",
            b"N\x90\x1d\x1ewhich is meant to differ from\n\x91:\x13",
            delta,
        )
        delta = self.make_delta(_text2, _text1)
        self.assertDeltaIn(
            b"M\x90/\x1ebe matched\nagainst other text\n",
            b"M\x90\x1d\x1dwhich is meant to be matched\n\x91;\x13",
            delta,
        )
        delta = self.make_delta(_text3, _text1)
        self.assertEqual(b"M\x90M", delta)
        delta = self.make_delta(_text3, _text2)
        self.assertDeltaIn(
            b"N\x90/\x1fdiffer from\nagainst other text\n",
            b"N\x90\x1d\x1ewhich is meant to differ from\n\x91:\x13",
            delta,
        )

    def test_make_delta_with_large_copies(self):
        # We want to have a copy that is larger than 64kB, which forces us to
        # issue multiple copy instructions.
        big_text = _text3 * 1220
        delta = self.make_delta(big_text, big_text)
        c_expected = (
            b"\xdc\x86\x0a"  # Encoding the length of the uncompressed text
            b"\x80"  # Copy 64kB, starting at byte 0
            b"\x84\x01"  # and another 64kB starting at 64kB
            b"\xb4\x02\x5c\x83"  # And the bit of tail.
        )
        # The Rust rabin delta may pick different (but valid) copy offsets
        # when the source data repeats
        rust_expected = (
            b"\xdc\x86\x0a"
            b"\x80"  # Copy 64kB, starting at byte 0
            b"\x83\xe0\x02"  # Copy 64kB from a repeated offset
            b"\xb3\xc0\x05\x5c\x83"  # And the tail
        )
        self.assertDeltaIn(c_expected, rust_expected, delta)

    def test_apply_delta_is_typesafe(self):
        self.apply_delta(_text1, b"M\x90M")
        self.assertRaises(TypeError, self.apply_delta, object(), b"M\x90M")
        self.assertRaises(
            (ValueError, TypeError),
            self.apply_delta,
            _text1.decode("latin1"),
            b"M\x90M",
        )
        self.assertRaises((ValueError, TypeError), self.apply_delta, _text1, "M\x90M")
        self.assertRaises(TypeError, self.apply_delta, _text1, object())

    def test_apply_delta(self):
        target = self.apply_delta(
            _text1, b"N\x90/\x1fdiffer from\nagainst other text\n"
        )
        self.assertEqual(_text2, target)
        target = self.apply_delta(_text2, b"M\x90/\x1ebe matched\nagainst other text\n")
        self.assertEqual(_text1, target)

    def test_apply_delta_to_source_is_safe(self):
        self.assertRaises(TypeError, self.apply_delta_to_source, object(), 0, 1)
        self.assertRaises(TypeError, self.apply_delta_to_source, "unicode str", 0, 1)
        # end > length
        self.assertRaises(ValueError, self.apply_delta_to_source, b"foo", 1, 4)
        # start > length
        self.assertRaises(ValueError, self.apply_delta_to_source, b"foo", 5, 3)
        # start > end
        self.assertRaises(ValueError, self.apply_delta_to_source, b"foo", 3, 2)

    def test_apply_delta_to_source(self):
        source_and_delta = _text1 + b"N\x90/\x1fdiffer from\nagainst other text\n"
        self.assertEqual(
            _text2,
            self.apply_delta_to_source(
                source_and_delta, len(_text1), len(source_and_delta)
            ),
        )


class TestMakeAndApplyCompatible(TestCase):
    scenarios = two_way_scenarios()

    make_delta = None  # Set by load_tests
    apply_delta = _groupcompress_rs.apply_delta

    def assertMakeAndApply(self, source, target):
        """Assert that generating a delta and applying gives success."""
        delta = self.make_delta(source, target)
        bytes = self.apply_delta(source, delta)
        self.assertEqualDiff(target, bytes)

    def test_direct(self):
        self.assertMakeAndApply(_text1, _text2)
        self.assertMakeAndApply(_text2, _text1)
        self.assertMakeAndApply(_text1, _text3)
        self.assertMakeAndApply(_text3, _text1)
        self.assertMakeAndApply(_text2, _text3)
        self.assertMakeAndApply(_text3, _text2)


class TestDeltaIndex(TestCase):
    def setUp(self):
        super().setUp()
        self._gc_module = _groupcompress_rust

    def test_repr(self):
        di = self._gc_module.DeltaIndex(b"test text\n")
        self.assertEqual("DeltaIndex(1, 10)", repr(di))

    def test_sizeof(self):
        di = self._gc_module.DeltaIndex()
        self.assertGreater(sys.getsizeof(di), 0)

    def test_make_delta(self):
        di = self._gc_module.DeltaIndex(_text1)
        delta = di.make_delta(_text2)
        result = _groupcompress_rs.apply_delta(_text1, delta)
        self.assertEqual(_text2, result)

    def test_delta_against_multiple_sources(self):
        di = self._gc_module.DeltaIndex()
        di.add_source(_first_text, 0)
        self.assertEqual(len(_first_text), di._source_offset)
        di.add_source(_second_text, 0)
        self.assertEqual(len(_first_text) + len(_second_text), di._source_offset)
        delta = di.make_delta(_third_text)
        result = _groupcompress_rs.apply_delta(_first_text + _second_text, delta)
        self.assertEqual(_third_text, result)

    def test_delta_with_offsets(self):
        di = self._gc_module.DeltaIndex()
        di.add_source(_first_text, 5)
        self.assertEqual(len(_first_text) + 5, di._source_offset)
        di.add_source(_second_text, 10)
        self.assertEqual(len(_first_text) + len(_second_text) + 15, di._source_offset)
        delta = di.make_delta(_third_text)
        self.assertIsNot(None, delta)
        result = _groupcompress_rs.apply_delta(
            b"12345" + _first_text + b"1234567890" + _second_text, delta
        )
        self.assertIsNot(None, result)
        self.assertEqual(_third_text, result)

    def test_delta_with_delta_bytes(self):
        di = self._gc_module.DeltaIndex()
        source = _first_text
        di.add_source(_first_text, 0)
        self.assertEqual(len(_first_text), di._source_offset)
        # First delta: against a single fulltext source
        delta = di.make_delta(_second_text)
        self.assertEqual(_second_text, _groupcompress_rs.apply_delta(source, delta))
        # Add the delta as a new source — the index should be able to match
        # against content embedded in the delta's insert instructions
        di.add_delta_source(delta, 0)
        source += delta
        self.assertEqual(len(_first_text) + len(delta), di._source_offset)
        # Second delta: should find matches in both the fulltext and the
        # delta source (e.g. "previous text\nand has some..." from the delta)
        second_delta = di.make_delta(_third_text)
        result = _groupcompress_rs.apply_delta(source, second_delta)
        self.assertEqual(_third_text, result)
        # The delta should be shorter than the fulltext since we have matches
        self.assertLess(len(second_delta), len(_third_text))
        # Add this delta too, and create another delta for the same text.
        # With more sources indexed, we should find even more matches.
        di.add_delta_source(second_delta, 0)
        source += second_delta
        third_delta = di.make_delta(_third_text)
        result = _groupcompress_rs.apply_delta(source, third_delta)
        self.assertEqual(_third_text, result)
        # Third delta should be no larger than the second (more data indexed)
        self.assertLessEqual(len(third_delta), len(second_delta))
        # Now create a delta for text that has no common content with the
        # existing sources — it should still round-trip correctly
        fourth_delta = di.make_delta(_fourth_text)
        self.assertEqual(
            _fourth_text, _groupcompress_rs.apply_delta(source, fourth_delta)
        )
        # Add that delta source, now everything in _fourth_text is indexed
        di.add_delta_source(fourth_delta, 0)
        source += fourth_delta
        # With the content now in the index, the delta should be very short
        fifth_delta = di.make_delta(_fourth_text)
        self.assertEqual(
            _fourth_text, _groupcompress_rs.apply_delta(source, fifth_delta)
        )
        self.assertLess(len(fifth_delta), len(fourth_delta))


class TestDeltaIndexRust(TestCase):
    def setUp(self):
        super().setUp()
        self._gc_module = _groupcompress_rust

    def test_repr(self):
        di = self._gc_module.DeltaIndex(b"test text\n")
        self.assertEqual("DeltaIndex(1, 10)", repr(di))

    def test_make_delta(self):
        di = self._gc_module.DeltaIndex(_text1)
        delta = di.make_delta(_text2)
        self.assertIsNotNone(delta)
        result = _groupcompress_rs.apply_delta(_text1, delta)
        self.assertEqual(_text2, result)

    def test_delta_against_multiple_sources(self):
        di = self._gc_module.DeltaIndex()
        di.add_source(_first_text, 0)
        self.assertEqual(len(_first_text), di._source_offset)
        di.add_source(_second_text, 0)
        self.assertEqual(len(_first_text) + len(_second_text), di._source_offset)
        delta = di.make_delta(_third_text)
        result = _groupcompress_rs.apply_delta(_first_text + _second_text, delta)
        self.assertEqual(_third_text, result)

    def test_delta_with_offsets(self):
        di = self._gc_module.DeltaIndex()
        di.add_source(_first_text, 5)
        self.assertEqual(len(_first_text) + 5, di._source_offset)
        di.add_source(_second_text, 10)
        self.assertEqual(len(_first_text) + len(_second_text) + 15, di._source_offset)
        delta = di.make_delta(_third_text)
        self.assertIsNotNone(delta)
        result = _groupcompress_rs.apply_delta(
            b"12345" + _first_text + b"1234567890" + _second_text, delta
        )
        self.assertIsNotNone(result)
        self.assertEqual(_third_text, result)

    def test_delta_with_delta_bytes(self):
        di = self._gc_module.DeltaIndex()
        source = _first_text
        di.add_source(_first_text, 0)
        self.assertEqual(len(_first_text), di._source_offset)
        delta = di.make_delta(_second_text)
        self.assertIsNotNone(delta)
        # Verify the delta round-trips
        result = _groupcompress_rs.apply_delta(source, delta)
        self.assertEqual(_second_text, result)
        di.add_delta_source(delta, 0)
        source += delta
        self.assertEqual(len(_first_text) + len(delta), di._source_offset)
        second_delta = di.make_delta(_third_text)
        result = _groupcompress_rs.apply_delta(source, second_delta)
        self.assertEqual(_third_text, result)
