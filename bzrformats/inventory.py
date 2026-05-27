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

import contextlib
from collections import deque

from . import osutils
from ._bzr_rs import ROOT_ID
from ._bzr_rs import inventory as _mod_inventory_rs
from .errors import BadFileKindError, BzrFormatsError, InconsistentDelta


class NoSuchId(BzrFormatsError):
    """File ID not found in tree.

    Raised when a requested file ID is not present in the tree.
    """

    _fmt = 'The file id "%(file_id)s" is not present in the tree %(tree)s.'

    def __init__(self, tree, file_id):
        super().__init__()
        self.tree = tree
        self.file_id = file_id


FileId = bytes
InventoryEntry = _mod_inventory_rs.InventoryEntry
InventoryFile = _mod_inventory_rs.InventoryFile
InventoryDirectory = _mod_inventory_rs.InventoryDirectory
TreeReference = _mod_inventory_rs.TreeReference
InventoryLink = _mod_inventory_rs.InventoryLink
Inventory = _mod_inventory_rs.Inventory


class InvalidEntryName(BzrFormatsError):
    _fmt = "Invalid entry name: %(name)s"

    def __init__(self, name):
        super().__init__()
        self.name = name


class DuplicateFileId(BzrFormatsError):
    _fmt = "File id {%(file_id)s} already exists in inventory as %(entry)s"

    def __init__(self, file_id, entry):
        super().__init__()
        self.file_id = file_id
        self.entry = entry


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

    def _expand_fileids_to_parents_and_children(self, file_ids):
        """Give a more wholistic view starting with the given file_ids.

        For any file_id which maps to a directory, we will include all children
        of that directory. We will also include all directories which are
        parents of the given file_ids, but we will not include their children.

        eg:
          /     # TREE_ROOT
          foo/  # foo-id
            baz # baz-id
            frob/ # frob-id
              fringle # fringle-id
          bar/  # bar-id
            bing # bing-id

        if given [foo-id] we will include
            TREE_ROOT as interesting parents
        and
            foo-id, baz-id, frob-id, fringle-id
        As interesting ids.
        """
        interesting = set()
        # TODO: Pre-pass over the list of fileids to see if anything is already
        #       deserialized in self._fileid_to_entry_cache

        directories_to_expand = set()
        children_of_parent_id = {}
        # It is okay if some of the fileids are missing
        for entry in self._getitems(file_ids):
            if entry.kind == "directory":
                directories_to_expand.add(entry.file_id)
            interesting.add(entry.parent_id)
            children_of_parent_id.setdefault(entry.parent_id, set()).add(entry.file_id)

        # Now, interesting has all of the direct parents, but not the
        # parents of those parents. It also may have some duplicates with
        # specific_fileids
        remaining_parents = interesting.difference(file_ids)
        # When we hit the TREE_ROOT, we'll get an interesting parent of None,
        # but we don't actually want to recurse into that
        interesting.add(None)  # this will auto-filter it in the loop
        remaining_parents.discard(None)
        while remaining_parents:
            next_parents = set()
            for entry in self._getitems(remaining_parents):
                next_parents.add(entry.parent_id)
                children_of_parent_id.setdefault(entry.parent_id, set()).add(
                    entry.file_id
                )
            # Remove any search tips we've already processed
            remaining_parents = next_parents.difference(interesting)
            interesting.update(remaining_parents)
            # We should probably also .difference(directories_to_expand)
        interesting.update(file_ids)
        interesting.discard(None)
        while directories_to_expand:
            # Expand directories by looking in the
            # parent_id_basename_to_file_id map
            keys = [(f,) for f in directories_to_expand]
            directories_to_expand = set()
            items = self.parent_id_basename_to_file_id.iteritems(keys)
            next_file_ids = {item[1] for item in items}
            next_file_ids = next_file_ids.difference(interesting)
            interesting.update(next_file_ids)
            for entry in self._getitems(next_file_ids):
                if entry.kind == "directory":
                    directories_to_expand.add(entry.file_id)
                children_of_parent_id.setdefault(entry.parent_id, set()).add(
                    entry.file_id
                )
        return interesting, children_of_parent_id

    def filter(self, specific_fileids):
        """Get an inventory view filtered against a set of file-ids.

        Children of directories and parents are included.

        The result may or may not reference the underlying inventory
        so it should be treated as immutable.
        """
        (
            interesting,
            parent_to_children,
        ) = self._expand_fileids_to_parents_and_children(specific_fileids)
        # There is some overlap here, but we assume that all interesting items
        # are in the _fileid_to_entry_cache because we had to read them to
        # determine if they were a dir we wanted to recurse, or just a file
        # This should give us all the entries we'll want to add, so start
        # adding
        other = Inventory(root_id=None)
        root = InventoryDirectory(self.root_id, "", None, self.root.revision)
        other.add(root)
        other.revision_id = self.revision_id
        if not interesting or not parent_to_children:
            # empty filter, or filtering entrys that don't exist
            # (if even 1 existed, then we would have populated
            # parent_to_children with at least the tree root.)
            return other
        cache = self._fileid_to_entry_cache
        remaining_children = deque(parent_to_children[self.root_id])
        while remaining_children:
            file_id = remaining_children.popleft()
            ie = cache[file_id]
            if ie.kind == "directory":
                ie = ie.copy()  # We create a copy to depopulate the .children attribute
            # TODO: depending on the uses of 'other' we should probably alwyas
            #       '.copy()' to prevent someone from mutating other and
            #       invaliding our internal cache
            other.add(ie)
            if file_id in parent_to_children:
                remaining_children.extend(parent_to_children[file_id])
        return other

    def create_by_apply_delta(
        self, inventory_delta, new_revision_id, propagate_caches=False
    ):
        """Create a new CHKInventory by applying inventory_delta to this one.

        See the inventory developers documentation for the theory behind
        inventory deltas.

        :param inventory_delta: The inventory delta to apply. See
            Inventory.apply_delta for details.
        :param new_revision_id: The revision id of the resulting CHKInventory.
        :param propagate_caches: If True, the caches for this inventory are
          copied to and updated for the result.
        :return: The new CHKInventory.
        """
        split = osutils.split
        result = CHKInventory(self._search_key_name)
        if propagate_caches:
            # Just propagate the path-to-fileid cache for now
            result._path_to_fileid_cache = self._path_to_fileid_cache.copy()
        from . import chk_map

        search_key_func = chk_map.search_key_registry.get(self._search_key_name)
        self.id_to_entry._ensure_root()
        maximum_size = self.id_to_entry._root_node.maximum_size
        result.revision_id = new_revision_id
        result.id_to_entry = chk_map.CHKMap(
            self.id_to_entry._store,
            self.id_to_entry.key(),
            search_key_func=search_key_func,
        )
        result.id_to_entry._ensure_root()
        result.id_to_entry._root_node.set_maximum_size(maximum_size)
        # Change to apply to the parent_id_basename delta. The dict maps
        # (parent_id, basename) -> (old_key, new_value). We use a dict because
        # when a path has its id replaced (e.g. the root is changed, or someone
        # does bzr mv a b, bzr mv c a, we should output a single change to this
        # map rather than two.
        parent_id_basename_delta = {}
        if self.parent_id_basename_to_file_id is not None:
            result.parent_id_basename_to_file_id = chk_map.CHKMap(
                self.parent_id_basename_to_file_id._store,
                self.parent_id_basename_to_file_id.key(),
                search_key_func=search_key_func,
            )
            result.parent_id_basename_to_file_id._ensure_root()
            self.parent_id_basename_to_file_id._ensure_root()
            result_p_id_root = result.parent_id_basename_to_file_id._root_node
            p_id_root = self.parent_id_basename_to_file_id._root_node
            result_p_id_root.set_maximum_size(p_id_root.maximum_size)
            result_p_id_root._key_width = p_id_root._key_width
        else:
            result.parent_id_basename_to_file_id = None
        result.root_id = self.root_id
        id_to_entry_delta = []
        # inventory_delta is only traversed once, so we just update the
        # variable.
        inventory_delta.check()
        # All changed entries need to have their parents be directories and be
        # at the right path. This set contains (path, id) tuples.
        parents = set()
        # When we delete an item, all the children of it must be either deleted
        # or altered in their own right. As we batch process the change via
        # CHKMap.apply_delta, we build a set of things to use to validate the
        # delta.
        deletes = set()
        altered = set()
        for old_path, new_path, file_id, entry in inventory_delta:
            # file id changes
            if new_path == "":
                result.root_id = file_id
            if new_path is None:
                # Make a delete:
                new_key = None
                new_value = None
                # Update caches
                if propagate_caches:
                    with contextlib.suppress(KeyError):
                        del result._path_to_fileid_cache[old_path]
                deletes.add(file_id)
            else:
                new_key = (file_id,)
                new_value = _chk_inventory_entry_to_bytes(entry)
                # Update caches. It's worth doing this whether
                # we're propagating the old caches or not.
                result._path_to_fileid_cache[new_path] = file_id
                parents.add((split(new_path)[0], entry.parent_id))
            if old_path is None:
                old_key = None
            else:
                old_key = (file_id,)
                if self.id2path(file_id) != old_path:
                    raise InconsistentDelta(
                        old_path,
                        file_id,
                        "Entry was at wrong other path {!r}.".format(
                            self.id2path(file_id)
                        ),
                    )
                altered.add(file_id)
            id_to_entry_delta.append((old_key, new_key, new_value))
            if result.parent_id_basename_to_file_id is not None:
                # parent_id, basename changes
                if old_path is None:
                    old_key = None
                else:
                    old_entry = self.get_entry(file_id)
                    old_key = self._parent_id_basename_key(old_entry)
                if new_path is None:
                    new_key = None
                    new_value = None
                else:
                    new_key = self._parent_id_basename_key(entry)
                    new_value = file_id
                # If the two keys are the same, the value will be unchanged
                # as its always the file id for this entry.
                if old_key != new_key:
                    # Transform a change into explicit delete/add preserving
                    # a possible match on the key from a different file id.
                    if old_key is not None:
                        parent_id_basename_delta.setdefault(old_key, [None, None])[
                            0
                        ] = old_key
                    if new_key is not None:
                        parent_id_basename_delta.setdefault(new_key, [None, None])[
                            1
                        ] = new_value
        # validate that deletes are complete.
        for file_id in deletes:
            entry = self.get_entry(file_id)
            if entry.kind != "directory":
                continue
            # This loop could potentially be better by using the id_basename
            # map to just get the child file ids.
            for child in self.iter_sorted_children(entry.file_id):
                if child.file_id not in altered:
                    raise InconsistentDelta(
                        self.id2path(child.file_id),
                        child.file_id,
                        "Child not deleted or reparented when parent deleted.",
                    )
        result.id_to_entry.apply_delta(id_to_entry_delta)
        if parent_id_basename_delta:
            # Transform the parent_id_basename delta data into a linear delta
            # with only one record for a given key. Optimally this would allow
            # re-keying, but its simpler to just output that as a delete+add
            # to spend less time calculating the delta.
            delta_list = []
            for key, (old_key, value) in parent_id_basename_delta.items():
                if value is not None:
                    delta_list.append((old_key, key, value))
                else:
                    delta_list.append((old_key, None, None))
            result.parent_id_basename_to_file_id.apply_delta(delta_list)
        parents.discard(("", None))
        for parent_path, parent in parents:
            try:
                if result.get_entry(parent).kind != "directory":
                    raise InconsistentDelta(
                        result.id2path(parent),
                        parent,
                        "Not a directory, but given children",
                    )
            except NoSuchId as e:
                raise InconsistentDelta(
                    "<unknown>", parent, "Parent is not present in resulting inventory."
                ) from e
            if result.path2id(parent_path) != parent:
                raise InconsistentDelta(
                    parent_path,
                    parent,
                    f"Parent has wrong path {result.path2id(parent_path)!r}.",
                )
        return result

    def _preload_cache(self):
        """Make sure all file-ids are in _fileid_to_entry_cache."""
        if self._fully_cached:
            return  # No need to do it again
        # The optimal sort order is to use iteritems() directly
        cache = self._fileid_to_entry_cache
        for key, entry in self.id_to_entry.iteritems():
            file_id = key[0]
            if file_id not in cache:
                ie = self._bytes_to_entry(entry)
                cache[file_id] = ie
            else:
                ie = cache[file_id]
        last_parent_id = last_parent_ie = None
        pid_items = self.parent_id_basename_to_file_id.iteritems()
        for key, child_file_id in pid_items:
            if key == (b"", b""):  # This is the root
                if child_file_id != self.root_id:
                    raise ValueError(
                        "Data inconsistency detected."
                        ' We expected data with key ("","") to match'
                        f" the root id, but {child_file_id} != {self.root_id}"
                    )
                continue
            parent_id, basename = key
            ie = cache[child_file_id]
            if parent_id == last_parent_id:
                if last_parent_ie is None:
                    raise AssertionError("last_parent_ie should not be None")
                parent_ie = last_parent_ie
            else:
                parent_ie = cache[parent_id]
            if parent_ie.kind != "directory":
                raise ValueError(
                    "Data inconsistency detected."
                    " An entry in the parent_id_basename_to_file_id map"
                    f" has parent_id {{{parent_id}}} but the kind of that object"
                    f' is {parent_ie.kind!r} not "directory"'
                )
            siblings = self._children_cache.setdefault(parent_ie.file_id, {})
            basename = basename.decode("utf-8")
            if basename in siblings:
                existing_ie = siblings[basename]
                if existing_ie != ie:
                    raise ValueError(
                        "Data inconsistency detected."
                        f" Two entries with basename {basename!r} were found"
                        f" in the parent entry {{{parent_id}}}"
                    )
            if basename != ie.name:
                raise ValueError(
                    "Data inconsistency detected."
                    " In the parent_id_basename_to_file_id map, file_id"
                    " {{{}}} is listed as having basename {!r}, but in the"
                    " id_to_entry map it is {!r}".format(
                        child_file_id, basename, ie.name
                    )
                )
            siblings[basename] = ie
        self._fully_cached = True

    def _make_delta(self, old):
        """Produce an InventoryDelta describing the changes from `old` to self.

        Accepts `CHKInventory` and `Inventory` for `old`; the module-level
        `_make_delta(new, old)` dispatcher handles every pairing of the two
        inventory shapes.
        """
        return _make_delta(self, old)

    def iter_changes(self, basis):
        """Generate a Tree.iter_changes change list between this and basis.

        :param basis: Another CHKInventory.
        :return: An iterator over the changes between self and basis, as per
            tree.iter_changes().
        """
        # We want: (file_id, (path_in_source, path_in_target),
        # changed_content, versioned, parent, name, kind,
        # executable)
        for key, basis_value, self_value in self.id_to_entry.iter_changes(
            basis.id_to_entry
        ):
            file_id = key[0]
            if basis_value is not None:
                basis_entry = basis._bytes_to_entry(basis_value)
                path_in_source = basis.id2path(file_id)
                basis_parent = basis_entry.parent_id
                basis_name = basis_entry.name
                basis_executable = basis_entry.executable
            else:
                path_in_source = None
                basis_parent = None
                basis_name = None
                basis_executable = None
            if self_value is not None:
                self_entry = self._bytes_to_entry(self_value)
                path_in_target = self.id2path(file_id)
                self_parent = self_entry.parent_id
                self_name = self_entry.name
                self_executable = self_entry.executable
            else:
                path_in_target = None
                self_parent = None
                self_name = None
                self_executable = None
            if basis_value is None:
                # add
                kind = (None, self_entry.kind)
                versioned = (False, True)
            elif self_value is None:
                # delete
                kind = (basis_entry.kind, None)
                versioned = (True, False)
            else:
                kind = (basis_entry.kind, self_entry.kind)
                versioned = (True, True)
            changed_content = False
            if kind[0] != kind[1]:
                changed_content = True
            elif kind[0] == "file":
                if (
                    self_entry.text_size != basis_entry.text_size
                    or self_entry.text_sha1 != basis_entry.text_sha1
                ):
                    changed_content = True
            elif kind[0] == "symlink":
                if self_entry.symlink_target != basis_entry.symlink_target:
                    changed_content = True
            elif kind[0] == "tree-reference":
                if self_entry.reference_revision != basis_entry.reference_revision:
                    changed_content = True
            parent = (basis_parent, self_parent)
            name = (basis_name, self_name)
            executable = (basis_executable, self_executable)
            if (
                not changed_content
                and parent[0] == parent[1]
                and name[0] == name[1]
                and executable[0] == executable[1]
            ):
                # Could happen when only the revision changed for a directory
                # for instance.
                continue
            yield (
                file_id,
                (path_in_source, path_in_target),
                changed_content,
                versioned,
                parent,
                name,
                kind,
                executable,
            )


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
    """Make an inventory delta from two inventories."""
    from .inventory_delta import InventoryDelta

    if isinstance(old, CHKInventory) and isinstance(new, CHKInventory):
        delta = []
        for key, old_value, self_value in new.id_to_entry.iter_changes(old.id_to_entry):
            file_id = key[0]
            old_path = old.id2path(file_id) if old_value is not None else None
            if self_value is not None:
                entry = new._bytes_to_entry(self_value)
                new._fileid_to_entry_cache[file_id] = entry
                new_path = new.id2path(file_id)
            else:
                entry = None
                new_path = None
            delta.append((old_path, new_path, file_id, entry))
        return InventoryDelta(delta)
    elif isinstance(old, Inventory) and isinstance(new, Inventory):
        return new._make_delta(old)
    else:
        old_ids = set(old.iter_all_ids())
        new_ids = set(new.iter_all_ids())
        adds = new_ids - old_ids
        deletes = old_ids - new_ids
        common = old_ids.intersection(new_ids)
        delta = []
        for file_id in deletes:
            delta.append((old.id2path(file_id), None, file_id, None))
        for file_id in adds:
            delta.append((None, new.id2path(file_id), file_id, new.get_entry(file_id)))
        for file_id in common:
            if old.get_entry(file_id) != new.get_entry(file_id):
                delta.append(
                    (
                        old.id2path(file_id),
                        new.id2path(file_id),
                        file_id,
                        new.get_entry(file_id),
                    )
                )
        return InventoryDelta(delta)
