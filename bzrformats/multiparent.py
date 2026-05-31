# Copyright (C) 2007-2011 Canonical Ltd
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

"""Multi-parent diff implementation for versioned files.

The diff type (``MultiParent`` / ``NewText`` / ``ParentText``), the in-memory
pseudo-versionedfile (``MultiMemoryVersionedFile``) and the disk-backed one
(``MultiVersionedFile``) are all implemented in Rust
(``bzrformats._bzr_rs.multiparent``) and re-exported here.
"""

from ._bzr_rs import multiparent as _multiparent_rs

# MultiParent and its hunk types (NewText / ParentText) are implemented in
# Rust and re-exported here. `MultiParent.hunks` is a live list of NewText /
# ParentText instances that callers may mutate.
MultiParent = _multiparent_rs.MultiParent
NewText = _multiparent_rs.NewText
ParentText = _multiparent_rs.ParentText

# Memory- and disk-backed pseudo-versionedfiles, backed by Rust.
MultiMemoryVersionedFile = _multiparent_rs.MultiMemoryVersionedFile
MultiVersionedFile = _multiparent_rs.MultiVersionedFile

__all__ = [
    "MultiMemoryVersionedFile",
    "MultiParent",
    "MultiVersionedFile",
    "NewText",
    "ParentText",
    "topo_iter",
    "topo_iter_keys",
]


def topo_iter_keys(vf, keys=None):
    """Iterate through keys in topological order."""
    if keys is None:
        keys = vf.keys()
    parents = vf.get_parent_map(keys)
    return _topo_iter(parents, keys)


def topo_iter(vf, versions=None):
    """Iterate through versions in topological order."""
    if versions is None:
        versions = vf.versions()
    parents = vf.get_parent_map(versions)
    return _topo_iter(parents, versions)


def _topo_iter(parents, versions):
    return iter(_multiparent_rs.topo_iter(parents, versions))
