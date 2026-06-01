# Copyright (C) 2005-2011 Canonical Ltd
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

"""Inventory management for Bazaar.

This module provides classes and functions for managing file inventories
in Bazaar repositories, including entries for files, directories, symlinks,
and tree references.
"""

# FIXME: This refactoring of the workingtree code doesn't seem to keep
# the WorkingTree's copy of the inventory in sync with the branch.  The
# branch modifies its working inventory when it does a commit to make
# missing files permanently removed.

# TODO: Maybe also keep the full path of the entry, and the children?
# But those depend on its position within a particular inventory, and
# it would be nice not to need to hold the backpointer here.

__all__ = [
    "ROOT_ID",
    "CHKInventory",
    "FileId",
    "Inventory",
    "InventoryDirectory",
    "InventoryEntry",
    "InventoryFile",
    "InventoryLink",
    "TreeReference",
]


from ._bzr_rs import ROOT_ID
from ._bzr_rs import inventory as _mod_inventory_rs

# The inventory error classes live in the Rust errors module; re-export them so
# bzrformats.inventory.NoSuchId / InvalidEntryName / DuplicateFileId keep
# working for callers (e.g. breezy) and the Rust import_exception! sites.
from .errors import (  # noqa: F401
    BadFileKindError,
    DuplicateFileId,
    InvalidEntryName,
    NoSuchId,
)

FileId = bytes
InventoryEntry = _mod_inventory_rs.InventoryEntry
InventoryFile = _mod_inventory_rs.InventoryFile
InventoryDirectory = _mod_inventory_rs.InventoryDirectory
TreeReference = _mod_inventory_rs.TreeReference
InventoryLink = _mod_inventory_rs.InventoryLink
Inventory = _mod_inventory_rs.Inventory


class CHKInventory(_mod_inventory_rs.CHKInventory):
    """An inventory persisted in a CHK store.

    By design, a CHKInventory is immutable so many of the methods
    supported by Inventory - add, rename, apply_delta, etc - are *not*
    supported. To create a new CHKInventory, use create_by_apply_delta()
    or from_inventory(), say.

    Internally, a CHKInventory has one or two CHKMaps:

    * id_to_entry - a map from (file_id,) => InventoryEntry as bytes
    * parent_id_basename_to_file_id - a map from (parent_id, basename_utf8)
        => file_id as bytes

    The second map is optional and not present in early CHkRepository's.

    No caching is performed: every method call or item access will perform
    requests to the storage layer. As such, keep references to objects you
    want to reuse.

    State (search_key_name, root_id, revision_id, the two CHKMaps,
    and the in-memory caches) lives on the Rust-backed pyo3 base class.
    The orchestration methods below operate on that state via attribute
    access.
    """

    def make_entry(self, kind, name, parent_id, file_id=None, revision=None, **kwargs):
        """Simple thunk to bzrformats.inventory.make_entry."""
        return make_entry(kind, name, parent_id, file_id, revision, **kwargs)


entry_factory = {
    "directory": InventoryDirectory,
    "file": InventoryFile,
    "symlink": InventoryLink,
    "tree-reference": TreeReference,
}


def make_entry(kind, name, parent_id, file_id=None, revision=None, **kwargs):
    """Create an inventory entry.

    :param kind: the type of inventory entry to create.
    :param name: the basename of the entry.
    :param parent_id: the parent_id of the entry.
    :param file_id: the file_id to use. if None, one will be created.
    """
    if file_id is None:
        from . import generate_ids

        file_id = generate_ids.gen_file_id(name)
    name = ensure_normalized_name(name)
    try:
        factory = entry_factory[kind]
    except KeyError as e:
        raise BadFileKindError(name, kind) from e
    return factory(file_id, name, parent_id, revision, **kwargs)


ensure_normalized_name = _mod_inventory_rs.ensure_normalized_name
is_valid_name = _mod_inventory_rs.is_valid_name


def mutable_inventory_from_tree(tree):
    """Create a new inventory that has the same contents as a specified tree.

    :param tree: Revision tree to create inventory from
    """
    entries = tree.iter_entries_by_dir()
    inv = Inventory(None, tree.get_revision_id())
    for _path, inv_entry in entries:
        inv.add(inv_entry.copy())
    return inv


chk_inventory_bytes_to_utf8name_key = (
    _mod_inventory_rs.chk_inventory_bytes_to_utf8name_key
)
_chk_inventory_bytes_to_entry = _mod_inventory_rs.chk_inventory_bytes_to_entry
_chk_inventory_entry_to_bytes = _mod_inventory_rs.chk_inventory_entry_to_bytes


def _make_delta(new, old):
    """Produce an :class:`InventoryDelta` describing the changes from `old`
    to `new`. Either side may be an :class:`Inventory` or a
    :class:`CHKInventory`; dispatch happens in the Rust-backed
    ``_make_delta`` method on the new inventory.
    """
    return new._make_delta(old)
