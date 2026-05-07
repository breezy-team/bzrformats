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

"""Indexing facilities."""

__all__ = [
    "CombinedGraphIndex",
    "GraphIndex",
    "GraphIndexBuilder",
    "GraphIndexPrefixAdapter",
    "InMemoryGraphIndex",
]

import logging
from io import BytesIO

from . import revision as _mod_revision
from ._bzr_rs import index as _index_rs
from .errors import BzrFormatsError
from .transport import TransportNoSuchFile

logger = logging.getLogger("bzrformats.index")
evil_logger = logging.getLogger("bzrformats.evil")

_HEADER_READV = (0, 200)
_OPTION_KEY_ELEMENTS = b"key_elements="
_OPTION_LEN = b"len="
_OPTION_NODE_REFS = b"node_ref_lists="
_SIGNATURE = b"Bazaar Graph Index 1\n"


class BadIndexFormatSignature(BzrFormatsError):
    _fmt = "%(value)s is not an index of type %(_type)s."

    def __init__(self, value, _type):
        super().__init__()
        self.value = value
        self._type = _type


class BadIndexData(BzrFormatsError):
    _fmt = "Error in data for index %(value)s."

    def __init__(self, value):
        super().__init__()
        self.value = value


class BadIndexDuplicateKey(BzrFormatsError):
    _fmt = "The key '%(key)s' is already in index '%(index)s'."

    def __init__(self, key, index):
        super().__init__()
        self.key = key
        self.index = index


class BadIndexKey(BzrFormatsError):
    _fmt = "The key '%(key)s' is not a valid key."

    def __init__(self, key):
        super().__init__()
        self.key = key


class BadIndexOptions(BzrFormatsError):
    _fmt = "Could not parse options for index %(value)s."

    def __init__(self, value):
        super().__init__()
        self.value = value


class BadIndexValue(BzrFormatsError):
    _fmt = "The value '%(value)s' is not a valid value."

    def __init__(self, value):
        super().__init__()
        self.value = value




def _has_key_from_parent_map(self, key):
    """Check if this index has one key.

    If it's possible to check for multiple keys at once through
    calling get_parent_map that should be faster.
    """
    return key in self.get_parent_map([key])


def _missing_keys_from_parent_map(self, keys):
    return set(keys) - set(self.get_parent_map(keys))


class GraphIndexBuilder:
    """A builder that can build a GraphIndex.

    The resulting graph has the structure::

      _SIGNATURE OPTIONS NODES NEWLINE
      _SIGNATURE     := 'Bazaar Graph Index 1' NEWLINE
      OPTIONS        := 'node_ref_lists=' DIGITS NEWLINE
      NODES          := NODE*
      NODE           := KEY NULL ABSENT? NULL REFERENCES NULL VALUE NEWLINE
      KEY            := Not-whitespace-utf8
      ABSENT         := 'a'
      REFERENCES     := REFERENCE_LIST (TAB REFERENCE_LIST){node_ref_lists - 1}
      REFERENCE_LIST := (REFERENCE (CR REFERENCE)*)?
      REFERENCE      := DIGITS  ; digits is the byte offset in the index of the
                                ; referenced key.
      VALUE          := no-newline-no-null-bytes
    """

    def __init__(self, reference_lists=0, key_elements=1):
        """Create a GraphIndex builder.

        :param reference_lists: The number of node references lists for each
            entry.
        :param key_elements: The number of bytestrings in each key.
        """
        self.reference_lists = reference_lists
        # A dict of {key: (absent, ref_lists, value)}
        self._nodes = {}
        # Keys that are referenced but not actually present in this index
        self._absent_keys = set()
        self._nodes_by_key = None
        self._key_length = key_elements
        self._optimize_for_size = False
        self._combine_backing_indices = True

    def _check_key(self, key):
        """Raise BadIndexKey if key is not a valid key for this index."""
        _index_rs.check_key(key, self._key_length)

    def _external_references(self):
        """Return references that are not present in this index."""
        keys = set()
        refs = set()
        # TODO: JAM 2008-11-21 This makes an assumption about how the reference
        #       lists are used. It is currently correct for pack-0.92 through
        #       1.9, which use the node references (3rd column) second
        #       reference list as the compression parent. Perhaps this should
        #       be moved into something higher up the stack, since it
        #       makes assumptions about how the index is used.
        if self.reference_lists > 1:
            for node in self.iter_all_entries():
                keys.add(node[1])
                refs.update(node[3][1])
            return refs - keys
        else:
            # If reference_lists == 0 there can be no external references, and
            # if reference_lists == 1, then there isn't a place to store the
            # compression parent
            return set()

    def _get_nodes_by_key(self):
        if self._nodes_by_key is None:
            nodes_by_key = {}
            if self.reference_lists:
                for key, (absent, references, value) in self._nodes.items():
                    if absent:
                        continue
                    key_dict = nodes_by_key
                    for subkey in key[:-1]:
                        key_dict = key_dict.setdefault(subkey, {})
                    key_dict[key[-1]] = key, value, references
            else:
                for key, (absent, _references, value) in self._nodes.items():
                    if absent:
                        continue
                    key_dict = nodes_by_key
                    for subkey in key[:-1]:
                        key_dict = key_dict.setdefault(subkey, {})
                    key_dict[key[-1]] = key, value
            self._nodes_by_key = nodes_by_key
        return self._nodes_by_key

    def _update_nodes_by_key(self, key, value, node_refs):
        """Update the _nodes_by_key dict with a new key.

        For a key of (foo, bar, baz) create
        _nodes_by_key[foo][bar][baz] = key_value
        """
        if self._nodes_by_key is None:
            return
        key_dict = self._nodes_by_key
        if self.reference_lists:
            key_value = (key, value, node_refs)
        else:
            key_value = (key, value)
        for subkey in key[:-1]:
            key_dict = key_dict.setdefault(subkey, {})
        key_dict[key[-1]] = key_value

    def _check_key_ref_value(self, key, references, value):
        """Check that 'key' and 'references' are all valid.

        :param key: A key tuple. Must conform to the key interface (be a tuple,
            be of the right length, not have any whitespace or nulls in any key
            element.)
        :param references: An iterable of reference lists. Something like
            [[(ref, key)], [(ref, key), (other, key)]]
        :param value: The value associate with this key. Must not contain
            newlines or null characters.
        :return: (node_refs, absent_references)

            * node_refs: basically a packed form of 'references' where all
              iterables are tuples
            * absent_references: reference keys that are not in self._nodes.
              This may contain duplicates if the same key is referenced in
              multiple lists.
        """
        self._check_key(key)
        _index_rs.check_value(value)
        if len(references) != self.reference_lists:
            raise BadIndexValue(references)
        node_refs = []
        absent_references = []
        for reference_list in references:
            for reference in reference_list:
                # If reference *is* in self._nodes, then we know it has already
                # been checked.
                if reference not in self._nodes:
                    self._check_key(reference)
                    absent_references.append(reference)
            reference_list = tuple([tuple(ref) for ref in reference_list])
            node_refs.append(reference_list)
        return tuple(node_refs), absent_references

    def add_node(self, key, value, references=()):
        r"""Add a node to the index.

        :param key: The key. keys are non-empty tuples containing
            as many whitespace-free utf8 bytestrings as the key length
            defined for this index.
        :param references: An iterable of iterables of keys. Each is a
            reference to another key.
        :param value: The value to associate with the key. It may be any
            bytes as long as it does not contain \0 or \n.
        """
        (node_refs, absent_references) = self._check_key_ref_value(
            key, references, value
        )
        if key in self._nodes and self._nodes[key][0] != b"a":
            raise BadIndexDuplicateKey(key, self)
        for reference in absent_references:
            # There may be duplicates, but I don't think it is worth worrying
            # about
            self._nodes[reference] = (b"a", (), b"")
        self._absent_keys.update(absent_references)
        self._absent_keys.discard(key)
        self._nodes[key] = (b"", node_refs, value)
        if self._nodes_by_key is not None and self._key_length > 1:
            self._update_nodes_by_key(key, value, node_refs)

    def clear_cache(self):
        """See GraphIndex.clear_cache().

        This is a no-op, but we need the api to conform to a generic 'Index'
        abstraction.
        """

    def finish(self):
        """Finish the index.

        :returns: cBytesIO holding the full context of the index as it
        should be written to disk.
        """
        return BytesIO(
            _index_rs.serialize_graph_index(
                self._nodes, self.reference_lists, self._key_length
            )
        )

    def set_optimize(self, for_size=None, combine_backing_indices=None):
        """Change how the builder tries to optimize the result.

        :param for_size: Tell the builder to try and make the index as small as
            possible.
        :param combine_backing_indices: If the builder spills to disk to save
            memory, should the on-disk indices be combined. Set to True if you
            are going to be probing the index, but to False if you are not. (If
            you are not querying, then the time spent combining is wasted.)
        :return: None
        """
        # GraphIndexBuilder itself doesn't pay attention to the flag yet, but
        # other builders do.
        if for_size is not None:
            self._optimize_for_size = for_size
        if combine_backing_indices is not None:
            self._combine_backing_indices = combine_backing_indices

    def find_ancestry(self, keys, ref_list_num):
        """See CombinedGraphIndex.find_ancestry()."""
        pending = set(keys)
        parent_map = {}
        missing_keys = set()
        while pending:
            next_pending = set()
            for _, key, _value, ref_lists in self.iter_entries(pending):
                parent_keys = ref_lists[ref_list_num]
                parent_map[key] = parent_keys
                next_pending.update([p for p in parent_keys if p not in parent_map])
                missing_keys.update(pending.difference(parent_map))
            pending = next_pending
        return parent_map, missing_keys


_RustGraphIndex = _index_rs.GraphIndex


class GraphIndex(_RustGraphIndex):
    """Python facade over the pyo3-implemented graph index reader.

    The pyclass implements every method eagerly, but several callers rely
    on the Python-level laziness of iter_entries_prefix / iter_entries —
    in particular the validity checks BadIndexKey raises only fire when
    the caller actually iterates the result.
    """

    def iter_entries_prefix(self, keys):
        """Iterate matching entries for each key prefix."""
        keys = list(keys)
        if not keys:
            return
        yield from _RustGraphIndex.iter_entries_prefix(self, keys)

    def iter_entries(self, keys):
        """Iterate matching entries for each requested key."""
        keys = list(keys)
        if not keys:
            return
        yield from _RustGraphIndex.iter_entries(self, keys)

    def iter_all_entries(self):
        """Iterate every entry in the index."""
        yield from _RustGraphIndex.iter_all_entries(self)


class CombinedGraphIndex:
    """A GraphIndex made up from smaller GraphIndices.

    The backing indices must implement GraphIndex, and are presumed to be
    static data.

    Queries against the combined index will be made against the first index,
    and then the second and so on. The order of indices can thus influence
    performance significantly. For example, if one index is on local disk and a
    second on a remote server, the local disk index should be before the other
    in the index list.

    Also, queries tend to need results from the same indices as previous
    queries.  So the indices will be reordered after every query to put the
    indices that had the result(s) of that query first (while otherwise
    preserving the relative ordering).
    """

    def __init__(self, indices, reload_func=None):
        """Create a CombinedGraphIndex backed by indices.

        :param indices: An ordered list of indices to query for data.
        :param reload_func: A function to call if we find we are missing an
            index. Should have the form reload_func() => True/False to indicate
            if reloading actually changed anything.
        """
        self._indices = indices
        self._reload_func = reload_func
        # Sibling indices are other CombinedGraphIndex that we should call
        # _move_to_front_by_name on when we auto-reorder ourself.
        self._sibling_indices = []
        # A list of names that corresponds to the instances in self._indices,
        # so _index_names[0] is always the name for _indices[0], etc.  Sibling
        # indices must all use the same set of names as each other.
        self._index_names = [None] * len(self._indices)

    def __repr__(self):
        """Return string representation of the combined index."""
        return f"{self.__class__.__name__}({', '.join(map(repr, self._indices))})"

    def clear_cache(self):
        """See GraphIndex.clear_cache()."""
        for index in self._indices:
            index.clear_cache()

    def get_parent_map(self, keys):
        """See graph.StackedParentsProvider.get_parent_map."""
        search_keys = set(keys)
        if _mod_revision.NULL_REVISION in search_keys:
            search_keys.discard(_mod_revision.NULL_REVISION)
            found_parents = {_mod_revision.NULL_REVISION: []}
        else:
            found_parents = {}
        for _index, key, _value, refs in self.iter_entries(search_keys):
            parents = refs[0]
            if not parents:
                parents = (_mod_revision.NULL_REVISION,)
            found_parents[key] = parents
        return found_parents

    __contains__ = _has_key_from_parent_map

    def insert_index(self, pos, index, name=None):
        """Insert a new index in the list of indices to query.

        :param pos: The position to insert the index.
        :param index: The index to insert.
        :param name: a name for this index, e.g. a pack name.  These names can
            be used to reflect index reorderings to related CombinedGraphIndex
            instances that use the same names.  (see set_sibling_indices)
        """
        self._indices.insert(pos, index)
        self._index_names.insert(pos, name)

    def iter_all_entries(self):
        """Iterate over all keys within the index.

        Duplicate keys across child indices are presumed to have the same
        value and are only reported once.

        :return: An iterable of (index, key, reference_lists, value).
            There is no defined order for the result iteration - it will be in
            the most efficient order for the index.
        """
        seen_keys = set()
        while True:
            try:
                for index in self._indices:
                    for node in index.iter_all_entries():
                        if node[1] not in seen_keys:
                            yield node
                            seen_keys.add(node[1])
                return
            except TransportNoSuchFile as e:
                if not self._try_reload(e):
                    raise

    def iter_entries(self, keys):
        """Iterate over keys within the index.

        Duplicate keys across child indices are presumed to have the same
        value and are only reported once.

        :param keys: An iterable providing the keys to be retrieved.
        :return: An iterable of (index, key, reference_lists, value). There is
            no defined order for the result iteration - it will be in the most
            efficient order for the index.
        """
        keys = set(keys)
        hit_indices = []
        while True:
            try:
                for index in self._indices:
                    if not keys:
                        break
                    index_hit = False
                    for node in index.iter_entries(keys):
                        keys.remove(node[1])
                        yield node
                        index_hit = True
                    if index_hit:
                        hit_indices.append(index)
                break
            except TransportNoSuchFile as e:
                if not self._try_reload(e):
                    raise
        self._move_to_front(hit_indices)

    def iter_entries_prefix(self, keys):
        """Iterate over keys within the index using prefix matching.

        Duplicate keys across child indices are presumed to have the same
        value and are only reported once.

        Prefix matching is applied within the tuple of a key, not to within
        the bytestring of each key element. e.g. if you have the keys ('foo',
        'bar'), ('foobar', 'gam') and do a prefix search for ('foo', None) then
        only the former key is returned.

        :param keys: An iterable providing the key prefixes to be retrieved.
            Each key prefix takes the form of a tuple the length of a key, but
            with the last N elements 'None' rather than a regular bytestring.
            The first element cannot be 'None'.
        :return: An iterable as per iter_all_entries, but restricted to the
            keys with a matching prefix to those supplied. No additional keys
            will be returned, and every match that is in the index will be
            returned.
        """
        keys = set(keys)
        if not keys:
            return
        seen_keys = set()
        hit_indices = []
        while True:
            try:
                for index in self._indices:
                    index_hit = False
                    for node in index.iter_entries_prefix(keys):
                        if node[1] in seen_keys:
                            continue
                        seen_keys.add(node[1])
                        yield node
                        index_hit = True
                    if index_hit:
                        hit_indices.append(index)
                break
            except TransportNoSuchFile as e:
                if not self._try_reload(e):
                    raise
        self._move_to_front(hit_indices)

    def _move_to_front(self, hit_indices):
        """Rearrange self._indices so that hit_indices are first.

        Order is maintained as much as possible, e.g. the first unhit index
        will be the first index in _indices after the hit_indices, and the
        hit_indices will be present in exactly the order they are passed to
        _move_to_front.

        _move_to_front propagates to all objects in self._sibling_indices by
        calling _move_to_front_by_name.
        """
        if self._indices[: len(hit_indices)] == hit_indices:
            # The 'hit_indices' are already at the front (and in the same
            # order), no need to re-order
            return
        hit_names = self._move_to_front_by_index(hit_indices)
        for sibling_idx in self._sibling_indices:
            sibling_idx._move_to_front_by_name(hit_names)

    def _move_to_front_by_index(self, hit_indices):
        """Core logic for _move_to_front.

        Returns a list of names corresponding to the hit_indices param.
        """
        indices_info = zip(self._index_names, self._indices, strict=False)
        if logger.isEnabledFor(logging.DEBUG):
            indices_info = list(indices_info)
            logger.debug(
                "CombinedGraphIndex reordering: currently %r, promoting %r",
                indices_info,
                hit_indices,
            )
        hit_names = []
        unhit_names = []
        new_hit_indices = []
        unhit_indices = []

        for offset, (name, idx) in enumerate(indices_info):
            if idx in hit_indices:
                hit_names.append(name)
                new_hit_indices.append(idx)
                if len(new_hit_indices) == len(hit_indices):
                    # We've found all of the hit entries, everything else is
                    # unhit
                    unhit_names.extend(self._index_names[offset + 1 :])
                    unhit_indices.extend(self._indices[offset + 1 :])
                    break
            else:
                unhit_names.append(name)
                unhit_indices.append(idx)

        self._indices = new_hit_indices + unhit_indices
        self._index_names = hit_names + unhit_names
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("CombinedGraphIndex reordered: %r", self._indices)
        return hit_names

    def _move_to_front_by_name(self, hit_names):
        """Moves indices named by 'hit_names' to front of the search order, as
        described in _move_to_front.
        """
        # Translate names to index instances, and then call
        # _move_to_front_by_index.
        indices_info = zip(self._index_names, self._indices, strict=False)
        hit_indices = []
        for name, idx in indices_info:
            if name in hit_names:
                hit_indices.append(idx)
        self._move_to_front_by_index(hit_indices)

    def find_ancestry(self, keys, ref_list_num):
        """Find the complete ancestry for the given set of keys.

        Note that this is a whole-ancestry request, so it should be used
        sparingly.

        :param keys: An iterable of keys to look for
        :param ref_list_num: The reference list which references the parents
            we care about.
        :return: (parent_map, missing_keys)
        """
        # XXX: make this call _move_to_front?
        missing_keys = set()
        parent_map = {}
        keys_to_lookup = set(keys)
        generation = 0
        while keys_to_lookup:
            # keys that *all* indexes claim are missing, stop searching them
            generation += 1
            all_index_missing = None
            # print 'gen\tidx\tsub\tn_keys\tn_pmap\tn_miss'
            # print '%4d\t\t\t%4d\t%5d\t%5d' % (generation, len(keys_to_lookup),
            #                                   len(parent_map),
            #                                   len(missing_keys))
            for _index_idx, index in enumerate(self._indices):
                # TODO: we should probably be doing something with
                #       'missing_keys' since we've already determined that
                #       those revisions have not been found anywhere
                index_missing_keys = set()
                # Find all of the ancestry we can from this index
                # keep looking until the search_keys set is empty, which means
                # things we didn't find should be in index_missing_keys
                search_keys = keys_to_lookup
                sub_generation = 0
                # print '    \t%2d\t\t%4d\t%5d\t%5d' % (
                #     index_idx, len(search_keys),
                #     len(parent_map), len(index_missing_keys))
                while search_keys:
                    sub_generation += 1
                    # TODO: ref_list_num should really be a parameter, since
                    #       CombinedGraphIndex does not know what the ref lists
                    #       mean.
                    search_keys = index._find_ancestors(
                        search_keys, ref_list_num, parent_map, index_missing_keys
                    )
                    # print '    \t  \t%2d\t%4d\t%5d\t%5d' % (
                    #     sub_generation, len(search_keys),
                    #     len(parent_map), len(index_missing_keys))
                # Now set whatever was missing to be searched in the next index
                keys_to_lookup = index_missing_keys
                if all_index_missing is None:
                    all_index_missing = set(index_missing_keys)
                else:
                    all_index_missing.intersection_update(index_missing_keys)
                if not keys_to_lookup:
                    break
            if all_index_missing is None:
                # There were no indexes, so all search keys are 'missing'
                missing_keys.update(keys_to_lookup)
                keys_to_lookup = None
            else:
                missing_keys.update(all_index_missing)
                keys_to_lookup.difference_update(all_index_missing)
        return parent_map, missing_keys

    def key_count(self):
        """Return an estimate of the number of keys in this index.

        For CombinedGraphIndex this is approximated by the sum of the keys of
        the child indices. As child indices may have duplicate keys this can
        have a maximum error of the number of child indices * largest number of
        keys in any index.
        """
        while True:
            try:
                return sum((index.key_count() for index in self._indices), 0)
            except TransportNoSuchFile as e:
                if not self._try_reload(e):
                    raise

    missing_keys = _missing_keys_from_parent_map

    def _try_reload(self, error):
        """We just got a NoSuchFile exception.

        Try to reload the indices, if it fails, just raise the current
        exception.
        """
        if self._reload_func is None:
            return False
        logger.debug("Trying to reload after getting exception: %s", str(error))
        if not self._reload_func():
            # We tried to reload, but nothing changed, so we fail anyway
            logger.debug(
                "_reload_func indicated nothing has changed."
                " Raising original exception."
            )
            return False
        return True

    def set_sibling_indices(self, sibling_combined_graph_indices):
        """Set the CombinedGraphIndex objects to reorder after reordering self."""
        self._sibling_indices = sibling_combined_graph_indices

    def validate(self):
        """Validate that everything in the index can be accessed."""
        while True:
            try:
                for index in self._indices:
                    index.validate()
                return
            except TransportNoSuchFile as e:
                if not self._try_reload(e):
                    raise


class InMemoryGraphIndex(GraphIndexBuilder):
    """A GraphIndex which operates entirely out of memory and is mutable.

    This is designed to allow the accumulation of GraphIndex entries during a
    single write operation, where the accumulated entries need to be immediately
    available - for example via a CombinedGraphIndex.
    """

    def add_nodes(self, nodes):
        """Add nodes to the index.

        :param nodes: An iterable of (key, node_refs, value) entries to add.
        """
        if self.reference_lists:
            for key, value, node_refs in nodes:
                self.add_node(key, value, node_refs)
        else:
            for key, value in nodes:
                self.add_node(key, value)

    def iter_all_entries(self):
        """Iterate over all keys within the index.

        :return: An iterable of (index, key, reference_lists, value). There is no
            defined order for the result iteration - it will be in the most
            efficient order for the index (in this case dictionary hash order).
        """
        evil_logger.debug("iter_all_entries scales with size of history.")
        if self.reference_lists:
            for key, (absent, references, value) in self._nodes.items():
                if not absent:
                    yield self, key, value, references
        else:
            for key, (absent, _references, value) in self._nodes.items():
                if not absent:
                    yield self, key, value

    def iter_entries(self, keys):
        """Iterate over keys within the index.

        :param keys: An iterable providing the keys to be retrieved.
        :return: An iterable of (index, key, value, reference_lists). There is no
            defined order for the result iteration - it will be in the most
            efficient order for the index (keys iteration order in this case).
        """
        # Note: See BTreeBuilder.iter_entries for an explanation of why we
        #       aren't using set().intersection() here
        nodes = self._nodes
        keys = [key for key in keys if key in nodes]
        if self.reference_lists:
            for key in keys:
                node = nodes[key]
                if not node[0]:
                    yield self, key, node[2], node[1]
        else:
            for key in keys:
                node = nodes[key]
                if not node[0]:
                    yield self, key, node[2]

    def iter_entries_prefix(self, keys):
        """Iterate over keys within the index using prefix matching.

        Prefix matching is applied within the tuple of a key, not to within
        the bytestring of each key element. e.g. if you have the keys ('foo',
        'bar'), ('foobar', 'gam') and do a prefix search for ('foo', None) then
        only the former key is returned.

        :param keys: An iterable providing the key prefixes to be retrieved.
            Each key prefix takes the form of a tuple the length of a key, but
            with the last N elements 'None' rather than a regular bytestring.
            The first element cannot be 'None'.
        :return: An iterable as per iter_all_entries, but restricted to the
            keys with a matching prefix to those supplied. No additional keys
            will be returned, and every match that is in the index will be
            returned.
        """
        keys = list(keys)
        if not keys:
            return
        mode = "builder-refs" if self.reference_lists else "builder-norefs"
        for entry in _index_rs.iter_entries_prefix(
            self._nodes, keys, self._key_length, mode
        ):
            yield (self, *entry)

    def key_count(self):
        """Return an estimate of the number of keys in this index.

        For InMemoryGraphIndex the estimate is exact.
        """
        return len(self._nodes) - len(self._absent_keys)

    def validate(self):
        """In memory index's have no known corruption at the moment."""

    def __lt__(self, other):
        """Return True if self < other for ordering purposes."""
        # We don't really care about the order, just that there is an order.
        if not isinstance(other, GraphIndex) and not isinstance(
            other, InMemoryGraphIndex
        ):
            raise TypeError(other)
        return hash(self) < hash(other)


class GraphIndexPrefixAdapter:
    """An adapter between GraphIndex with different key lengths.

    Queries against this will emit queries against the adapted Graph with the
    prefix added, queries for all items use iter_entries_prefix. The returned
    nodes will have their keys and node references adjusted to remove the
    prefix. Finally, an add_nodes_callback can be supplied - when called the
    nodes and references being added will have prefix prepended.
    """

    def __init__(self, adapted, prefix, missing_key_length, add_nodes_callback=None):
        """Construct an adapter against adapted with prefix."""
        self.adapted = adapted
        self.prefix_key = prefix + (None,) * missing_key_length
        self.prefix = prefix
        self.prefix_len = len(prefix)
        self.add_nodes_callback = add_nodes_callback

    def add_nodes(self, nodes):
        """Add nodes to the index.

        :param nodes: An iterable of (key, node_refs, value) entries to add.
        """
        # save nodes in case its an iterator
        nodes = tuple(nodes)
        translated_nodes = []
        try:
            # Add prefix_key to each reference node_refs is a tuple of tuples,
            # so split it apart, and add prefix_key to the internal reference
            for key, value, node_refs in nodes:
                adjusted_references = tuple(
                    tuple(self.prefix + ref_node for ref_node in ref_list)
                    for ref_list in node_refs
                )
                translated_nodes.append((self.prefix + key, value, adjusted_references))
        except ValueError:
            # XXX: TODO add an explicit interface for getting the reference list
            # status, to handle this bit of user-friendliness in the API more
            # explicitly.
            for key, value in nodes:
                translated_nodes.append((self.prefix + key, value))
        self.add_nodes_callback(translated_nodes)

    def add_node(self, key, value, references=()):
        r"""Add a node to the index.

        :param key: The key. keys are non-empty tuples containing
            as many whitespace-free utf8 bytestrings as the key length
            defined for this index.
        :param references: An iterable of iterables of keys. Each is a
            reference to another key.
        :param value: The value to associate with the key. It may be any
            bytes as long as it does not contain \0 or \n.
        """
        self.add_nodes(((key, value, references),))

    def _strip_prefix(self, an_iter):
        """Strip prefix data from nodes and return it."""
        for node in an_iter:
            # cross checks
            if node[1][: self.prefix_len] != self.prefix:
                raise BadIndexData(self)
            for ref_list in node[3]:
                for ref_node in ref_list:
                    if ref_node[: self.prefix_len] != self.prefix:
                        raise BadIndexData(self)
            yield (
                node[0],
                node[1][self.prefix_len :],
                node[2],
                (
                    tuple(
                        tuple(ref_node[self.prefix_len :] for ref_node in ref_list)
                        for ref_list in node[3]
                    )
                ),
            )

    def iter_all_entries(self):
        """Iterate over all keys within the index.

        iter_all_entries is implemented against the adapted index using
        iter_entries_prefix.

        :return: An iterable of (index, key, reference_lists, value). There is no
            defined order for the result iteration - it will be in the most
            efficient order for the index (in this case dictionary hash order).
        """
        return self._strip_prefix(self.adapted.iter_entries_prefix([self.prefix_key]))

    def iter_entries(self, keys):
        """Iterate over keys within the index.

        :param keys: An iterable providing the keys to be retrieved.
        :return: An iterable of (index, key, value, reference_lists). There is no
            defined order for the result iteration - it will be in the most
            efficient order for the index (keys iteration order in this case).
        """
        return self._strip_prefix(
            self.adapted.iter_entries(self.prefix + key for key in keys)
        )

    def iter_entries_prefix(self, keys):
        """Iterate over keys within the index using prefix matching.

        Prefix matching is applied within the tuple of a key, not to within
        the bytestring of each key element. e.g. if you have the keys ('foo',
        'bar'), ('foobar', 'gam') and do a prefix search for ('foo', None) then
        only the former key is returned.

        :param keys: An iterable providing the key prefixes to be retrieved.
            Each key prefix takes the form of a tuple the length of a key, but
            with the last N elements 'None' rather than a regular bytestring.
            The first element cannot be 'None'.
        :return: An iterable as per iter_all_entries, but restricted to the
            keys with a matching prefix to those supplied. No additional keys
            will be returned, and every match that is in the index will be
            returned.
        """
        return self._strip_prefix(
            self.adapted.iter_entries_prefix(self.prefix + key for key in keys)
        )

    def key_count(self):
        """Return an estimate of the number of keys in this index.

        For GraphIndexPrefixAdapter this is relatively expensive - key
        iteration with the prefix is done.
        """
        return len(list(self.iter_all_entries()))

    def validate(self):
        """Call the adapted's validate."""
        self.adapted.validate()


