# Copyright (C) 2006, 2009, 2010 Canonical Ltd
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
#
# Author: Martin Pool <mbp@canonical.com>
#         Aaron Bentley <aaron.bentley@utoronto.ca>

"""Text merge functionality for handling two-way and three-way merges.

This module provides classes for merging text files with conflict detection
and resolution. It supports structured merge information representation and
various merge strategies.
"""

from ._bzr_rs.textmerge import Merge2

__all__ = ["Merge2", "TextMerge"]


class TextMerge:
    """Base class for text-mergers.

    Subclasses must implement ``_merge_struct``.

    Many methods produce or consume structured merge information.
    This is an iterable of tuples of lists of lines.
    Each tuple may have a length of 1 - 3, depending on whether the region it
    represents is conflicted.

    Unconflicted region tuples have length 1.
    Conflicted region tuples have length 2 or 3.  Index 0 is text_a, e.g. THIS.
    Index 1 is text_b, e.g. OTHER.  Index 2 is optional.  If present, it
    represents BASE.
    """

    A_MARKER = Merge2.A_MARKER
    B_MARKER = Merge2.B_MARKER
    SPLIT_MARKER = Merge2.SPLIT_MARKER

    def __init__(self, a_marker=A_MARKER, b_marker=B_MARKER, split_marker=SPLIT_MARKER):
        r"""Initialize a TextMerge instance with conflict markers.

        Args:
            a_marker: Marker for the start of conflicted region A (THIS).
            b_marker: Marker for the end of conflicted region B (OTHER).
            split_marker: Marker separating conflicted regions A and B.
        """
        self.a_marker = a_marker
        self.b_marker = b_marker
        self.split_marker = split_marker

    def _merge_struct(self):
        """Return structured merge info.  Must be implemented by subclasses.

        See TextMerge docstring for details on the format.
        """
        raise NotImplementedError("_merge_struct is abstract")

    def struct_to_lines(self, struct_iter):
        """Convert merge result tuples to lines."""
        for lines in struct_iter:
            if len(lines) == 1:
                yield from lines[0]
            else:
                yield self.a_marker
                yield from lines[0]
                yield self.split_marker
                yield from lines[1]
                yield self.b_marker

    def iter_useful(self, struct_iter):
        """Iterate through input tuples, skipping empty ones."""
        for group in struct_iter:
            if len(group[0]) > 0:
                yield group
            elif len(group) > 1 and len(group[1]) > 0:
                yield group

    def merge_lines(self, reprocess=False):
        """Produce an iterable of lines, suitable for writing to a file.

        Returns a tuple of (line iterable, conflict indicator).
        If reprocess is True, a two-way merge will be performed on the
        intermediate structure, to reduce conflict regions.
        """
        struct = []
        conflicts = False
        for group in self.merge_struct(reprocess):
            struct.append(group)
            if len(group) > 1:
                conflicts = True
        return self.struct_to_lines(struct), conflicts

    def merge_struct(self, reprocess=False):
        """Produce structured merge info."""
        struct_iter = self.iter_useful(self._merge_struct())
        if reprocess is True:
            return self.reprocess_struct(struct_iter)
        else:
            return struct_iter

    reprocess_struct = staticmethod(Merge2.reprocess_struct)
