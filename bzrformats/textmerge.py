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

The merge logic lives in the Rust ``bazaar::textmerge`` crate; this module
re-exports the pyo3 bindings. ``TextMerge`` is the subclassable base (used by
``PlanWeaveMerge`` and downstream ``Merge3``); ``Merge2`` is the two-way
merger.
"""

from ._bzr_rs.textmerge import Merge2, TextMerge

__all__ = ["Merge2", "TextMerge"]
