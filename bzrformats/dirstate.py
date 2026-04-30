# Copyright (C) 2006-2011 Canonical Ltd
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

r"""DirState objects record the state of a directory and its bzr metadata.

Pseudo EBNF grammar for the state file. Fields are separated by NULLs, and
lines by NL. The field delimiters are ommitted in the grammar, line delimiters
are not - this is done for clarity of reading. All string data is in utf8.

::

    MINIKIND = "f" | "d" | "l" | "a" | "r" | "t";
    NL = "\n";
    NULL = "\0";
    WHOLE_NUMBER = {digit}, digit;
    BOOLEAN = "y" | "n";
    REVISION_ID = a non-empty utf8 string;

    dirstate format = header line, full checksum, row count, parent details,
     ghost_details, entries;
    header line = "#bazaar dirstate flat format 3", NL;
    full checksum = "crc32: ", ["-"], WHOLE_NUMBER, NL;
    row count = "num_entries: ", WHOLE_NUMBER, NL;
    parent_details = WHOLE NUMBER, {REVISION_ID}* NL;
    ghost_details = WHOLE NUMBER, {REVISION_ID}*, NL;
    entries = {entry};
    entry = entry_key, current_entry_details, {parent_entry_details};
    entry_key = dirname,  basename, fileid;
    current_entry_details = common_entry_details, working_entry_details;
    parent_entry_details = common_entry_details, history_entry_details;
    common_entry_details = MINIKIND, fingerprint, size, executable
    working_entry_details = packed_stat
    history_entry_details = REVISION_ID;
    executable = BOOLEAN;
    size = WHOLE_NUMBER;
    fingerprint = a nonempty utf8 sequence with meaning defined by minikind.

Given this definition, the following is useful to know::

    entry (aka row) - all the data for a given key.
    entry[0]: The key (dirname, basename, fileid)
    entry[0][0]: dirname
    entry[0][1]: basename
    entry[0][2]: fileid
    entry[1]: The tree(s) data for this path and id combination.
    entry[1][0]: The current tree
    entry[1][1]: The second tree

For an entry for a tree, we have (using tree 0 - current tree) to demonstrate::

    entry[1][0][0]: minikind
    entry[1][0][1]: fingerprint
    entry[1][0][2]: size
    entry[1][0][3]: executable
    entry[1][0][4]: packed_stat

OR (for non tree-0)::

    entry[1][1][4]: revision_id

There may be multiple rows at the root, one per id present in the root, so the
in memory root row is now::

    self._dirblocks[0] -> ('', [entry ...]),

and the entries in there are::

    entries[0][0]: b''
    entries[0][1]: b''
    entries[0][2]: file_id
    entries[1][0]: The tree data for the current tree for this fileid at /
    etc.

Kinds::

   b'r' is a relocated entry: This path is not present in this tree with this
        id, but the id can be found at another location. The fingerprint is
        used to point to the target location.
   b'a' is an absent entry: In that tree the id is not present at this path.
   b'd' is a directory entry: This path in this tree is a directory with the
        current file id. There is no fingerprint for directories.
   b'f' is a file entry: As for directory, but it's a file. The fingerprint is
        the sha1 value of the file's canonical form, i.e. after any read
        filters have been applied to the convenience form stored in the working
        tree.
   b'l' is a symlink entry: As for directory, but a symlink. The fingerprint is
        the link target.
   b't' is a reference to a nested subtree; the fingerprint is the referenced
        revision.

Ordering:

The entries on disk and in memory are ordered according to the following keys::

    directory, as a list of components
    filename
    file-id

--- Format 1 had the following different definition: ---

::

    rows = dirname, NULL, basename, NULL, MINIKIND, NULL, fileid_utf8, NULL,
        WHOLE NUMBER (* size *), NULL, packed stat, NULL, sha1|symlink target,
        {PARENT ROW}
    PARENT ROW = NULL, revision_utf8, NULL, MINIKIND, NULL, dirname, NULL,
        basename, NULL, WHOLE NUMBER (* size *), NULL, "y" | "n", NULL,
        SHA1

PARENT ROW's are emitted for every parent that is not in the ghosts details
line. That is, if the parents are foo, bar, baz, and the ghosts are bar, then
each row will have a PARENT ROW for foo and baz, but not for bar.


In any tree, a kind of 'moved' indicates that the fingerprint field
(which we treat as opaque data specific to the 'kind' anyway) has the
details for the id of this row in that tree.

I'm strongly tempted to add a id->path index as well, but I think that
where we need id->path mapping; we also usually read the whole file, so
I'm going to skip that for the moment, as we have the ability to locate
via bisect any path in any tree, and if we lookup things by path, we can
accumulate an id->path mapping as we go, which will tend to match what we
looked for.

I plan to implement this asap, so please speak up now to alter/tweak the
design - and once we stabilise on this, I'll update the wiki page for
it.

The rationale for all this is that we want fast operations for the
common case (diff/status/commit/merge on all files) and extremely fast
operations for the less common but still occurs a lot status/diff/commit
on specific files). Operations on specific files involve a scan for all
the children of a path, *in every involved tree*, which the current
format did not accommodate.
----

Design priorities:
 1. Fast end to end use for bzr's top 5 uses cases. (commmit/diff/status/merge/???)
 2. fall back current object model as needed.
 3. scale usably to the largest trees known today - say 50K entries. (mozilla
    is an example of this)


Locking:

 Eventually reuse dirstate objects across locks IFF the dirstate file has not
 been modified, but will require that we flush/ignore cached stat-hit data
 because we won't want to restat all files on disk just because a lock was
 acquired, yet we cannot trust the data after the previous lock was released.

Memory representation::

 vector of all directories, and vector of the childen ?
   i.e.
     root_entries = (direntry for root, [parent_direntries_for_root]),
     dirblocks = [
     ('', ['data for achild', 'data for bchild', 'data for cchild'])
     ('dir', ['achild', 'cchild', 'echild'])
     ]
    - single bisect to find N subtrees from a path spec
    - in-order for serialisation - this is 'dirblock' grouping.
    - insertion of a file '/a' affects only the '/' child-vector, that is, to
      insert 10K elements from scratch does not generates O(N^2) memoves of a
      single vector, rather each individual, which tends to be limited to a
      manageable number. Will scale badly on trees with 10K entries in a
      single directory. compare with Inventory.InventoryDirectory which has
      a dictionary for the children. No bisect capability, can only probe for
      exact matches, or grab all elements and sort.
    - What's the risk of error here? Once we have the base format being processed
      we should have a net win regardless of optimality. So we are going to
      go with what seems reasonable.

open questions:

Maybe we should do a test profile of the core structure - 10K simulated
searches/lookups/etc?

Objects for each row?
The lifetime of Dirstate objects is current per lock, but see above for
possible extensions. The lifetime of a row from a dirstate is expected to be
very short in the optimistic case: which we are optimising for. For instance,
subtree status will determine from analysis of the disk data what rows need to
be examined at all, and will be able to determine from a single row whether
that file has altered or not, so we are aiming to process tens of thousands of
entries each second within the dirstate context, before exposing anything to
the larger codebase. This suggests we want the time for a single file
comparison to be < 0.1 milliseconds. That would give us 10000 paths per second
processed, and to scale to 100 thousand we'll another order of magnitude to do
that. Now, as the lifetime for all unchanged entries is the time to parse, stat
the file on disk, and then immediately discard, the overhead of object creation
becomes a significant cost.

Figures: Creating a tuple from 3 elements was profiled at 0.0625
microseconds, whereas creating a object which is subclassed from tuple was
0.500 microseconds, and creating an object with 3 elements and slots was 3
microseconds long. 0.1 milliseconds is 100 microseconds, and ideally we'll get
down to 10 microseconds for the total processing - having 33% of that be object
creation is a huge overhead. There is a potential cost in using tuples within
each row which is that the conditional code to do comparisons may be slower
than method invocation, but method invocation is known to be slow due to stack
frame creation, so avoiding methods in these tight inner loops in unfortunately
desirable. We can consider a pyrex version of this with objects in future if
desired.

"""

import bisect
import codecs
import contextlib
import logging
import os
import stat

from . import inventory, lock, osutils
from .errors import (
    BzrFormatsError,
    LockContention,
    LockNotHeld,
    ObjectNotLocked,
)

logger = logging.getLogger("bzrformats.dirstate")
evil_logger = logging.getLogger("bzrformats.evil")

# This is the Windows equivalent of ENOTDIR
# It is defined in pywin32.winerror, but we don't want a strong dependency for
# just an error code.
ERROR_PATH_NOT_FOUND = 3
ERROR_DIRECTORY = 267


class DirstateCorrupt(BzrFormatsError):
    """Exception raised when a dirstate file is corrupt."""

    _fmt = "The dirstate file (%(state)s) appears to be corrupt: %(msg)s"

    def __init__(self, state, msg):
        """Create a DirstateCorrupt exception.

        Args:
            state: The dirstate that is corrupt.
            msg: Error message describing the corruption.
        """
        super().__init__()
        self.state = state
        self.msg = msg


class SHA1Provider:
    """An interface for getting sha1s of a file."""

    def sha1(self, abspath):
        """Return the sha1 of a file given its absolute path.

        :param abspath:  May be a filesystem encoded absolute path
             or a unicode path.
        """
        raise NotImplementedError(self.sha1)

    def stat_and_sha1(self, abspath):
        """Return the stat and sha1 of a file given its absolute path.

        :param abspath:  May be a filesystem encoded absolute path
             or a unicode path.

        Note: the stat should be the stat of the physical file
        while the sha may be the sha of its canonical content.
        """
        raise NotImplementedError(self.stat_and_sha1)


class DirstateInventoryChange:
    """Change information from dirstate that can be converted to InventoryTreeChange."""

    def __init__(
        self,
        file_id,
        path,
        changed_content,
        versioned,
        parent_id,
        name,
        kind,
        executable,
        copied=False,
    ):
        """Initialize a DirstateInventoryChange.

        Args:
            file_id: The file ID of the changed item.
            path: Tuple of (old_path, new_path).
            changed_content: Whether content changed.
            versioned: Tuple of (old_versioned, new_versioned).
            parent_id: Tuple of (old_parent_id, new_parent_id).
            name: Tuple of (old_name, new_name).
            kind: Tuple of (old_kind, new_kind).
            executable: Tuple of (old_executable, new_executable).
            copied: Whether this represents a copy (default False).
        """
        self.file_id = file_id
        self.path = path
        self.changed_content = changed_content
        self.versioned = versioned
        self.parent_id = parent_id
        self.name = name
        self.kind = kind
        self.executable = executable
        self.copied = copied

    def meta_modified(self):
        """Return true if the meta data has been modified."""
        if self.versioned == (True, True):
            return self.executable[0] != self.executable[1]
        return False

    def is_reparented(self):
        """Return whether the entry has been moved to a different parent."""
        return self.parent_id[0] != self.parent_id[1]

    @property
    def renamed(self):
        """Return true if the entry has been renamed."""
        return (
            not self.copied
            and None not in self.name
            and None not in self.parent_id
            and (self.name[0] != self.name[1] or self.parent_id[0] != self.parent_id[1])
        )

    def discard_new(self):
        """Return a copy of this delta with the new side discarded."""
        return self.__class__(
            self.file_id,
            (self.path[0], None),
            self.changed_content,
            (self.versioned[0], None),
            (self.parent_id[0], None),
            (self.name[0], None),
            (self.kind[0], None),
            (self.executable[0], None),
            copied=False,
        )

    def _as_tuple(self):
        return (
            self.file_id,
            self.path,
            self.changed_content,
            self.versioned,
            self.parent_id,
            self.name,
            self.kind,
            self.executable,
            self.copied,
        )

    def __repr__(self):
        """Return string representation."""
        return f"{self.__class__.__name__}{self._as_tuple()!r}"

    def __getitem__(self, index):
        """Return item at index."""
        return self._as_tuple()[index]

    def __eq__(self, other):
        """Check equality."""
        if hasattr(other, "_as_tuple"):
            return self._as_tuple() == other._as_tuple()
        if isinstance(other, tuple):
            return self._as_tuple() == other
        return NotImplemented


class DirState:
    """Record directory and metadata state for fast access.

    A dirstate is a specialised data structure for managing local working
    tree state information. Its not yet well defined whether it is platform
    specific, and if it is how we detect/parameterize that.

    Dirstates use the usual lock_write, lock_read and unlock mechanisms.
    Unlike most bzr disk formats, DirStates must be locked for reading, using
    lock_read.  (This is an os file lock internally.)  This is necessary
    because the file can be rewritten in place.

    DirStates must be explicitly written with save() to commit changes; just
    unlocking them does not write the changes to disk.
    """

    _kind_to_minikind = {
        "absent": b"a",
        "file": b"f",
        "directory": b"d",
        "relocated": b"r",
        "symlink": b"l",
        "tree-reference": b"t",
    }
    _minikind_to_kind = {
        b"a": "absent",
        b"f": "file",
        b"d": "directory",
        b"l": "symlink",
        b"r": "relocated",
        b"t": "tree-reference",
    }
    _stat_to_minikind = {
        stat.S_IFDIR: b"d",
        stat.S_IFREG: b"f",
        stat.S_IFLNK: b"l",
    }
    _to_yesno = {True: b"y", False: b"n"}  # TODO profile the performance gain
    # of using int conversion rather than a dict here. AND BLAME ANDREW IF
    # it is faster.

    # TODO: jam 20070221 Figure out what to do if we have a record that exceeds
    #       the BISECT_PAGE_SIZE. For now, we just have to make it large enough
    #       that we are sure a single record will always fit.
    BISECT_PAGE_SIZE = 4096

    NOT_IN_MEMORY = 0
    IN_MEMORY_UNMODIFIED = 1
    IN_MEMORY_MODIFIED = 2
    IN_MEMORY_HASH_MODIFIED = 3  # Only hash-cache updates

    # A pack_stat (the x's) that is just noise and will never match the output
    # of base64 encode.
    NULLSTAT = b"x" * 32
    NULL_PARENT_DETAILS = (b"a", b"", 0, False, b"")

    HEADER_FORMAT_2 = b"#bazaar dirstate flat format 2\n"
    HEADER_FORMAT_3 = b"#bazaar dirstate flat format 3\n"

    def __init__(
        self,
        path,
        sha1_provider,
        worth_saving_limit=0,
        use_filesystem_for_exec=True,
        fdatasync=False,
    ):
        """Create a  DirState object.

        :param path: The path at which the dirstate file on disk should live.
        :param sha1_provider: an object meeting the SHA1Provider interface.
        :param worth_saving_limit: when the exact number of hash changed
            entries is known, only bother saving the dirstate if more than
            this count of entries have changed.
            -1 means never save hash changes, 0 means always save hash changes.
        :param use_filesystem_for_exec: Whether to trust the filesystem
            for executable bit information
        """
        # All scalar state and the parents/ghosts lists live on the
        # Rust-side DirStateRs wrapper owns the dirblocks, parents,
        # ghosts and id_index.  Python still owns the packed-stat /
        # split-path caches, the lock plumbing, and the sha1 provider
        # because those haven't been migrated yet.
        self._rs = _dirstate_rs.DirStateRs(
            path,
            sha1_provider=sha1_provider,
            worth_saving_limit=worth_saving_limit,
            use_filesystem_for_exec=use_filesystem_for_exec,
            fdatasync=fdatasync,
        )
        self._state_file = None
        self._lock_token = None
        self._lock_state = None
        self._id_index = None
        self._bisect_page_size = DirState.BISECT_PAGE_SIZE
        self._sha1_provider = sha1_provider
        self._sha1_file = self._sha1_provider.sha1
        # A simple cache for lookups into dirblock entries: by probing
        # for the next entry, we save a bisection per path during commit.
        self._last_entry_index = None

    # ---- Rust-backed scalar attributes ----
    #
    # Each `_*` attribute below was a plain Python field before the
    # DirStateRs migration. Python code still reads and writes the
    # `self._foo` names (and test suites poke at them directly), so
    # we expose them as properties that delegate to the pyclass on
    # `self._rs`.

    @property
    def _header_state(self):
        return self._rs.header_state

    @_header_state.setter
    def _header_state(self, value):
        self._rs.header_state = value

    @property
    def _dirblock_state(self):
        return self._rs.dirblock_state

    @_dirblock_state.setter
    def _dirblock_state(self, value):
        self._rs.dirblock_state = value

    @property
    def _changes_aborted(self):
        return self._rs.changes_aborted

    @_changes_aborted.setter
    def _changes_aborted(self, value):
        self._rs.changes_aborted = value

    @property
    def _end_of_header(self):
        return self._rs.end_of_header

    @_end_of_header.setter
    def _end_of_header(self, value):
        self._rs.end_of_header = value

    @property
    def _cutoff_time(self):
        return self._rs.cutoff_time

    @_cutoff_time.setter
    def _cutoff_time(self, value):
        self._rs.cutoff_time = value

    @property
    def _filename(self):
        return self._rs.filename

    @property
    def _worth_saving_limit(self):
        return self._rs.worth_saving_limit

    @_worth_saving_limit.setter
    def _worth_saving_limit(self, value):
        self._rs.worth_saving_limit = value

    @property
    def _fdatasync(self):
        return self._rs.fdatasync

    @_fdatasync.setter
    def _fdatasync(self, value):
        self._rs.fdatasync = value

    @property
    def _use_filesystem_for_exec(self):
        return self._rs.use_filesystem_for_exec

    @_use_filesystem_for_exec.setter
    def _use_filesystem_for_exec(self, value):
        self._rs.use_filesystem_for_exec = value

    @property
    def _parents(self):
        return self._rs.parents

    @_parents.setter
    def _parents(self, value):
        self._rs.parents = value

    @property
    def _ghosts(self):
        return self._rs.ghosts

    @_ghosts.setter
    def _ghosts(self, value):
        self._rs.ghosts = value

    @property
    def _dirblocks(self):
        """Read-through view of the Rust-owned dirblocks.

        Each access marshals a fresh snapshot of the dirblocks out of
        Rust, so references held across a dirstate mutation go stale.
        Callers that need fresh data must re-access ``state._dirblocks``
        after any mutation.
        """
        return self._rs.dirblocks

    @_dirblocks.setter
    def _dirblocks(self, value):
        self._rs.dirblocks = value

    def __repr__(self):
        """Return string representation of the dirstate."""
        return f"{self.__class__.__name__}({self._filename!r})"

    def _mark_modified(self, hash_changed_entries=None, header_modified=False):
        """Mark this dirstate as modified.

        :param hash_changed_entries: if non-None, mark just these entries as
            having their hash modified.
        :param header_modified: mark the header modified as well, not just the
            dirblocks.
        """
        hash_changed_keys = (
            [e[0] for e in hash_changed_entries] if hash_changed_entries else None
        )
        self._rs.mark_modified(hash_changed_keys, header_modified)

    def _mark_unmodified(self):
        """Mark this dirstate as unmodified."""
        self._rs.mark_unmodified()

    def add(self, path, file_id, kind, stat, fingerprint):
        """Add a path to be tracked.

        :param path: The path within the dirstate - b'' is the root, 'foo' is the
            path foo within the root, 'foo/bar' is the path bar within foo
            within the root.
        :param file_id: The file id of the path being added.
        :param kind: The kind of the path, as a string like 'file',
            'directory', etc.
        :param stat: The output of os.lstat for the path.
        :param fingerprint: The sha value of the file's canonical form (i.e.
            after any read filters have been applied),
            or the target of a symlink,
            or the referenced revision id for tree-references,
            or b'' for directories.
        """
        if file_id.__class__ is not bytes:
            raise AssertionError(f"must be a utf8 file_id not {type(file_id)}")
        if isinstance(path, bytes):
            path = path.decode("utf-8")
        self._rs.add_path(path, file_id, kind, stat, fingerprint)
        # Refill the cached IdIndex in place so callers that hold the
        # reference returned by ``_get_id_index`` see the new entry.
        if self._id_index is not None:
            self._id_index.fill_from_state(self._rs)

    def _bisect(self, paths):
        """Bisect through the disk structure for specific rows.

        :param paths: A list of paths to find
        :return: A dict mapping path => entries for found entries.
        """
        self._requires_lock()
        self._read_header_if_needed()
        if self._dirblock_state != DirState.NOT_IN_MEMORY:
            raise AssertionError(f"bad dirblock state {self._dirblock_state!r}")
        file_size = os.fstat(self._state_file.fileno()).st_size
        return self._rs.bisect(self, self._state_file, file_size, paths)

    def _bisect_dirblocks(self, dir_list):
        """Bisect through the disk structure to find entries in given dirs.

        :param dir_list: A sorted list of directory names ['', 'dir', 'foo'].
        :return: A map from dir => entries_for_dir
        """
        self._requires_lock()
        self._read_header_if_needed()
        if self._dirblock_state != DirState.NOT_IN_MEMORY:
            raise AssertionError(f"bad dirblock state {self._dirblock_state!r}")
        file_size = os.fstat(self._state_file.fileno()).st_size
        return self._rs.bisect_dirblocks(self, self._state_file, file_size, dir_list)

    def _bisect_recursive(self, paths):
        """Bisect for entries for all paths and their children.

        :param paths: A sorted list of (dir, name) pairs
        :return: A dict mapping (dir, name, file_id) => [tree_info]
        """
        self._requires_lock()
        self._read_header_if_needed()
        if self._dirblock_state != DirState.NOT_IN_MEMORY:
            raise AssertionError(f"bad dirblock state {self._dirblock_state!r}")
        file_size = os.fstat(self._state_file.fileno()).st_size
        return self._rs.bisect_recursive(self, self._state_file, file_size, paths)

    def _discard_merge_parents(self):
        """Discard any parents trees beyond the first.

        Note that if this fails the dirstate is corrupted.

        After this function returns the dirstate contains 2 trees, neither of
        which are ghosted.
        """
        self._read_header_if_needed()
        if len(self._rs.parents) < 1:
            return
        self._read_dirblocks_if_needed()
        self._rs.discard_merge_parents()
        if self._id_index is not None:
            self._id_index.fill_from_state(self._rs)

    def _empty_parent_info(self):
        return [DirState.NULL_PARENT_DETAILS] * (len(self._parents) - len(self._ghosts))

    def _ensure_block(self, parent_block_index, parent_row_index, dirname):
        """Ensure a block for dirname exists."""
        return self._rs.ensure_block(parent_block_index, parent_row_index, dirname)

    def _entries_to_current_state(self, new_entries):
        """Rebuild dirblocks from new_entries (sorted by path).

        :param new_entries: A sorted list of entries.
        """
        self._rs.entries_to_current_state(new_entries)

    def _split_root_dirblock_into_contents(self):
        """Split the root dirblocks into root and contents-of-root."""
        self._rs.split_root_dirblock_into_contents()

    def _entries_for_path(self, path):
        """Return a list with all the entries that match path for all ids."""
        return self._rs.entries_for_path(path)

    @staticmethod
    def _entry_to_line(entry):
        """Serialize entry to a NULL delimited line ready for _get_output_lines.

        :param entry: An entry_tuple as defined in the module docstring.
        """
        return _dirstate_rs.entry_to_line(entry)

    def _find_block_index_from_key(self, key):
        """Find the dirblock index for a key.

        :return: The block index, True if the block for the key is present.
        """
        return self._rs.find_block_index_from_key(key)

    def _find_entry_index(self, key, block):
        """Find the entry index for a key in a block.

        :return: The entry index, True if the entry for the key is present.
        """
        len_block = len(block)
        try:
            if self._last_entry_index is not None:
                # mini-bisect here.
                entry_index = self._last_entry_index + 1
                # A hit is when the key is after the last slot, and before or
                # equal to the next slot.
                if (
                    entry_index > 0 and block[entry_index - 1][0] < key
                ) and key <= block[entry_index][0]:
                    self._last_entry_index = entry_index
                    present = block[entry_index][0] == key
                    return entry_index, present
        except IndexError:
            pass
        entry_index = bisect.bisect_left(block, (key, []))
        present = entry_index < len_block and block[entry_index][0] == key
        self._last_entry_index = entry_index
        return entry_index, present

    @staticmethod
    def from_tree(tree, dir_state_filename, sha1_provider=None):
        """Create a dirstate from a bzr Tree.

        :param tree: The tree which should provide parent information and
            inventory ids.
        :param sha1_provider: an object meeting the SHA1Provider interface.
            If None, a DefaultSHA1Provider is used.
        :return: a DirState object which is currently locked for writing.
            (it was locked by DirState.initialize)
        """
        result = DirState.initialize(dir_state_filename, sha1_provider=sha1_provider)
        try:
            with contextlib.ExitStack() as exit_stack:
                exit_stack.enter_context(tree.lock_read())
                parent_ids = tree.get_parent_ids()
                parent_trees = []
                for parent_id in parent_ids:
                    parent_tree = tree.branch.repository.revision_tree(parent_id)
                    parent_trees.append((parent_id, parent_tree))
                    exit_stack.enter_context(parent_tree.lock_read())
                result.set_parent_trees(parent_trees, [])
                result.set_state_from_inventory(tree.root_inventory)
        except:
            # The caller won't have a chance to unlock this, so make sure we
            # cleanup ourselves
            result.unlock()
            raise
        return result

    def update_by_delta(self, delta):
        """Apply an inventory delta to the dirstate for tree 0.

        This is the workhorse for apply_inventory_delta in dirstate based
        trees.

        :param delta: An inventory delta.  See Inventory.apply_delta for
            details.
        """
        self._read_dirblocks_if_needed()
        delta.check()
        delta.sort()
        self._rs.update_by_delta(delta)
        if self._id_index is not None:
            self._id_index.fill_from_state(self._rs)

    def _apply_removals(self, removals):
        self._rs.apply_removals(list(removals))
        if self._id_index is not None:
            self._id_index.fill_from_state(self._rs)

    def _apply_insertions(self, adds):
        self._rs.apply_insertions(list(adds))
        if self._id_index is not None:
            self._id_index.fill_from_state(self._rs)

    def update_basis_by_delta(self, delta, new_revid):
        """Update the parents of this tree after a commit.

        This gives the tree one parent, with revision id new_revid. The
        inventory delta is applied to the current basis tree to generate the
        inventory for the parent new_revid, and all other parent trees are
        discarded.

        Note that an exception during the operation of this method will leave
        the dirstate in a corrupt state where it should not be saved.

        :param new_revid: The new revision id for the trees parent.
        :param delta: An inventory delta (see apply_inventory_delta) describing
            the changes from the current left most parent revision to new_revid.
        """
        self._read_dirblocks_if_needed()
        delta.check()
        delta.sort()
        self._rs.update_basis_by_delta(delta, new_revid)
        if self._id_index is not None:
            self._id_index.fill_from_state(self._rs)

    def _check_delta_ids_absent(self, new_ids, tree_index):
        """Check that none of the file_ids in new_ids are present in a tree."""
        if not new_ids:
            return
        self._rs.check_delta_ids_absent(list(new_ids), tree_index)

    def _update_basis_apply_adds(self, adds):
        """Apply a sequence of adds to tree 1 during update_basis_by_delta.

        They may be adds, or renames that have been split into add/delete
        pairs.

        :param adds: A sequence of adds. Each add is a tuple:
            (None, new_path_utf8, file_id, (entry_details), real_add). real_add
            is False when the add is the second half of a remove-and-reinsert
            pair created to handle renames and deletes.
        """
        self._rs.update_basis_apply_adds(adds)
        if self._id_index is not None:
            self._id_index.fill_from_state(self._rs)

    def _update_basis_apply_changes(self, changes):
        """Apply a sequence of changes to tree 1 during update_basis_by_delta.

        :param changes: A sequence of changes. Each change is a tuple:
            (path_utf8, path_utf8, file_id, (entry_details))
        """
        self._rs.update_basis_apply_changes(changes)
        if self._id_index is not None:
            self._id_index.fill_from_state(self._rs)

    def _update_basis_apply_deletes(self, deletes):
        """Apply a sequence of deletes to tree 1 during update_basis_by_delta.

        They may be deletes, or renames that have been split into add/delete
        pairs.

        :param deletes: A sequence of deletes. Each delete is a tuple:
            (old_path_utf8, new_path_utf8, file_id, None, real_delete).
            real_delete is True when the desired outcome is an actual deletion
            rather than the rename handling logic temporarily deleting a path
            during the replacement of a parent.
        """
        self._rs.update_basis_apply_deletes(deletes)
        if self._id_index is not None:
            self._id_index.fill_from_state(self._rs)

    def _after_delta_check_parents(self, parents, index):
        """Check that parents required by the delta are all intact.

        :param parents: An iterable of (path_utf8, file_id) tuples which are
            required to be present in tree 'index' at path_utf8 with id file_id
            and be a directory.
        :param index: The column in the dirstate to check for parents in.
        """
        self._rs.after_delta_check_parents(list(parents), index)

    def _observed_sha1(self, entry, sha1, stat_value):
        """Note the sha1 of a file.

        Thin shim over DirStateRs.observed_sha1.  `stat_value` may be
        a real os.stat_result or a lightweight stand-in (like
        breezy.filters.FilteredStat) that only carries st_mode /
        st_size / st_mtime / st_ctime — fall back to zero for the
        device/inode fields in that case.

        Callers may pass an unversioned-path entry (``(None, None)``);
        in that case there is no row to update and we silently do
        nothing, matching Python ``DirState._observed_sha1``'s no-op
        behaviour for those paths (its cutoff_time guard skips fresh
        files before it would dereference ``entry[1][0]``).
        """
        if entry[0] is None:
            return
        new_tree0 = self._rs.observed_sha1(
            entry[0],
            sha1,
            stat_value.st_mode,
            stat_value.st_size,
            stat_value.st_mtime,
            stat_value.st_ctime,
            getattr(stat_value, "st_dev", 0),
            getattr(stat_value, "st_ino", 0),
        )
        if new_tree0 is not None:
            entry[1][0] = new_tree0

    def _sha_cutoff_time(self):
        """Return cutoff time.

        Files modified more recently than this time are at risk of being
        undetectably modified and so can't be cached.
        """
        return self._rs.compute_sha_cutoff_time()

    def get_ghosts(self):
        """Return a list of the parent tree revision ids that are ghosts."""
        self._read_header_if_needed()
        return self._ghosts

    def get_lines(self):
        """Serialise the entire dirstate to a sequence of lines."""
        if (
            self._header_state == DirState.IN_MEMORY_UNMODIFIED
            and self._dirblock_state == DirState.IN_MEMORY_UNMODIFIED
        ):
            # read what's on disk.
            self._state_file.seek(0)
            return self._state_file.readlines()
        self._read_dirblocks_if_needed()
        # Temporary sync boundary: push Python's dirblocks into the
        # DirStateRs wrapper before calling the pure-Rust serialiser.
        # Goes away once every dirblock writer has migrated and
        # self._dirblocks is no longer the source of truth.
        return self._rs.get_lines()

    def get_parent_ids(self):
        """Return a list of the parent tree ids for the directory state."""
        self._read_header_if_needed()
        return list(self._parents)

    def _get_block_entry_index(self, dirname, basename, tree_index):
        """Get the coordinates for a path in the state structure.

        :param dirname: The utf8 dirname to lookup.
        :param basename: The utf8 basename to lookup.
        :param tree_index: The index of the tree for which this lookup should
            be attempted.
        :return: A tuple describing where the path is located, or should be
            inserted. The tuple contains four fields: the block index, the row
            index, the directory is present (boolean), the entire path is
            present (boolean).  There is no guarantee that either
            coordinate is currently reachable unless the found field for it is
            True. For instance, a directory not present in the searched tree
            may be returned with a value one greater than the current highest
            block offset. The directory present field will always be True when
            the path present field is True. The directory present field does
            NOT indicate that the directory is present in the searched tree,
            rather it indicates that there are at least some files in some
            tree present there.
        """
        self._read_dirblocks_if_needed()
        return self._rs.get_block_entry_index(dirname, basename, tree_index)

    def _get_entry(
        self, tree_index, fileid_utf8=None, path_utf8=None, include_deleted=False
    ):
        """Get the dirstate entry for path in tree tree_index.

        If either file_id or path is supplied, it is used as the key to lookup.
        If both are supplied, the fastest lookup is used, and an error is
        raised if they do not both point at the same row.

        :param tree_index: The index of the tree we wish to locate this path
            in. If the path is present in that tree, the entry containing its
            details is returned, otherwise (None, None) is returned
            0 is the working tree, higher indexes are successive parent
            trees.
        :param fileid_utf8: A utf8 file_id to look up.
        :param path_utf8: An utf8 path to be looked up.
        :param include_deleted: If True, and performing a lookup via
            fileid_utf8 rather than path_utf8, return an entry for deleted
            (absent) paths.
        :return: The dirstate entry tuple for path, or (None, None)
        """
        self._read_dirblocks_if_needed()
        if path_utf8 is not None and not isinstance(path_utf8, bytes):
            raise BzrFormatsError(
                f"path_utf8 is not bytes: {type(path_utf8)} {path_utf8!r}"
            )
        return self._rs.get_entry(
            tree_index,
            fileid_utf8=fileid_utf8,
            path_utf8=path_utf8,
            include_deleted=include_deleted,
        )

    @classmethod
    def initialize(cls, path, sha1_provider=None):
        """Create a new dirstate on path.

        The new dirstate will be an empty tree - that is it has no parents,
        and only a root node - which has id ROOT_ID.

        :param path: The name of the file for the dirstate.
        :param sha1_provider: an object meeting the SHA1Provider interface.
            If None, a DefaultSHA1Provider is used.
        :return: A write-locked DirState object.
        """
        # This constructs a new DirState object on a path, sets the _state_file
        # to a new empty file for that path. It then calls _set_data() with our
        # stock empty dirstate information - a root with ROOT_ID, no children,
        # and no parents. Finally it calls save() to ensure that this data will
        # persist.
        if sha1_provider is None:
            sha1_provider = DefaultSHA1Provider()
        result = cls(path, sha1_provider)
        # root dir and root dir contents with no children.
        empty_tree_dirblocks = [(b"", []), (b"", [])]
        # a new root directory, with a NULLSTAT.
        empty_tree_dirblocks[0][1].append(
            (
                (b"", b"", inventory.ROOT_ID),
                [
                    (b"d", b"", 0, False, DirState.NULLSTAT),
                ],
            )
        )
        result.lock_write()
        try:
            result._set_data([], empty_tree_dirblocks)
            result.save()
        except:
            result.unlock()
            raise
        return result

    def _iter_child_entries(self, tree_index, path_utf8):
        """Iterate over all the entries that are children of path_utf.

        This only returns entries that are present (not in b'a', b'r') in
        tree_index. tree_index data is not refreshed, so if tree 0 is used,
        results may differ from that obtained if paths were statted to
        determine what ones were directories.

        Asking for the children of a non-directory will return an empty
        iterator.
        """
        # Temporary sync boundary: push Python's dirblocks into the
        # DirStateRs wrapper before calling the pure-Rust walker. The
        # returned entries are snapshot tuples — the two call sites in
        # update_basis_by_delta only read them, so the aliasing
        # difference from the previous `yield entry` version is
        # invisible to callers.
        return iter(self._rs.iter_child_entries(tree_index, path_utf8))

    def _iter_entries(self):
        """Iterate over all the entries in the dirstate.

        Each yelt item is an entry in the standard format described in the
        docstring of bzrformats.dirstate.
        """
        self._read_dirblocks_if_needed()
        # Materialise the entry list once on the Rust side; reading
        # ``self._dirblocks`` would re-marshal every block on every
        # outer-loop iteration of any caller doing ``for e in
        # state._iter_entries(): for d in state._dirblocks: ...``.
        return iter(self._rs.entries())

    def _get_id_index(self):
        """Get an id index of self._dirblocks.

        This maps from file_id => [(directory, name, file_id)] entries where
        that file_id appears in one of the trees.

        Callers may hold the returned object across mutations and expect
        to see fresh state — the original bzr contract.  We therefore
        cache a Python-side IdIndex on ``self._id_index`` and refill it
        from Rust's authoritative index in every mutating method.
        """
        if self._id_index is None:
            self._id_index = IdIndex()
            self._id_index.fill_from_state(self._rs)
        return self._id_index

    @classmethod
    def _make_deleted_row(cls, fileid_utf8, parents):
        """Return a deleted row for fileid_utf8."""
        return (
            b"/",
            b"RECYCLED.BIN",
            b"file",
            fileid_utf8,
            0,
            DirState.NULLSTAT,
            b"",
        ), parents

    def _num_present_parents(self):
        """The number of parent entries in each record row."""
        return len(self._parents) - len(self._ghosts)

    @classmethod
    def on_file(
        cls,
        path,
        sha1_provider=None,
        worth_saving_limit=0,
        use_filesystem_for_exec=True,
        fdatasync=False,
    ):
        """Construct a DirState on the file at path "path".

        :param path: The path at which the dirstate file on disk should live.
        :param sha1_provider: an object meeting the SHA1Provider interface.
            If None, a DefaultSHA1Provider is used.
        :param worth_saving_limit: when the exact number of hash changed
            entries is known, only bother saving the dirstate if more than
            this count of entries have changed. -1 means never save.
        :param use_filesystem_for_exec: Whether to trust the filesystem
            for executable bit information
        :return: An unlocked DirState object, associated with the given path.
        """
        if sha1_provider is None:
            sha1_provider = DefaultSHA1Provider()
        result = cls(
            path,
            sha1_provider,
            worth_saving_limit=worth_saving_limit,
            use_filesystem_for_exec=use_filesystem_for_exec,
            fdatasync=fdatasync,
        )
        return result

    def _read_dirblocks_if_needed(self):
        """Read in all the dirblocks from the file if they are not in memory.

        This populates self._dirblocks, and sets self._dirblock_state to
        IN_MEMORY_UNMODIFIED. It is not currently ready for incremental block
        loading.
        """
        self._read_header_if_needed()
        if self._dirblock_state == DirState.NOT_IN_MEMORY:
            _read_dirblocks(self)

    def _read_header(self):
        """Read the metadata header and parent ids from the state file."""
        self._rs.read_header_from_file(self, self._state_file)

    def _read_header_if_needed(self):
        """Read the header of the dirstate file if needed."""
        # inline this as it will be called a lot
        if not self._lock_token:
            raise ObjectNotLocked(self)
        if self._header_state == DirState.NOT_IN_MEMORY:
            self._read_header()

    def sha1_from_stat(self, path, stat_result):
        """Find a sha1 given a stat lookup."""
        return self._rs.sha1_from_packed_stat(pack_stat(stat_result))

    def save(self):
        """Save any pending changes created during this session.

        We reuse the existing file, because that prevents race conditions with
        file creation, and use oslocks on it to prevent concurrent modification
        and reads - because dirstate's incremental data aggregation is not
        compatible with reading a modified file, and replacing a file in use by
        another process is impossible on Windows.

        A dirstate in read only mode should be smart enough though to validate
        that the file has not changed, and otherwise discard its cache and
        start over, to allow for fine grained read lock duration, so 'status'
        wont block 'commit' - for example.
        """
        # Python-side responsibility: drive the read→write lock-upgrade
        # dance (bzrformats.lock owns the OS lock).  Rust does the
        # serialise / write / truncate / flush / fdatasync / state
        # bookkeeping through save_to_file.
        if self._changes_aborted:
            logger.debug("Not saving DirState because _changes_aborted is set.")
            return
        if not self._worth_saving():
            return

        grabbed_write_lock = False
        if self._lock_state != "w":
            grabbed_write_lock, new_lock = self._lock_token.temporary_write_lock()
            self._lock_token = new_lock
            self._state_file = new_lock.f
            if not grabbed_write_lock:
                return
        try:
            self._rs.save_to_file(self._state_file)
        finally:
            if grabbed_write_lock:
                self._lock_token = self._lock_token.restore_read_lock()
                self._state_file = self._lock_token.f

    def _worth_saving(self):
        """Is it worth saving the dirstate or not?"""
        return self._rs.worth_saving()

    def _set_data(self, parent_ids, dirblocks):
        """Set the full dirstate data in memory.

        This is an internal function used to completely replace the objects
        in memory state. It puts the dirstate into state 'full-dirty'.

        :param parent_ids: A list of parent tree revision ids.
        :param dirblocks: A list containing one tuple for each directory in the
            tree. Each tuple contains the directory path and a list of entries
            found in that directory.
        """
        # Rust side absorbs parent_ids and dirblocks, marks both states
        # fully modified, and clears its id_index cache.
        self._rs.set_data(parent_ids, dirblocks)
        if self._id_index is not None:
            self._id_index.fill_from_state(self._rs)

    def set_path_id(self, path, new_id):
        """Change the id of path to new_id in the current working tree.

        :param path: The path inside the tree to set - b'' is the root, 'foo'
            is the path foo in the root.
        :param new_id: The new id to assign to the path. This must be a utf8
            file id (not unicode, and not None).
        """
        self._read_dirblocks_if_needed()
        if not isinstance(new_id, bytes):
            raise AssertionError(f"must be a utf8 file_id not {type(new_id)}")
        self._rs.set_path_id(path, new_id)
        if self._id_index is not None:
            self._id_index.fill_from_state(self._rs)

    def set_parent_trees(self, trees, ghosts):
        """Set the parent trees for the dirstate.

        :param trees: A list of revision_id, tree tuples. tree must be provided
            even if the revision_id refers to a ghost: supply an empty tree in
            this case.
        :param ghosts: A list of the revision_ids that are ghosts at the time
            of setting.
        """
        self._read_dirblocks_if_needed()
        parent_ids = [rev_id for rev_id, _ in trees]
        ghosts_list = list(ghosts)
        # Flatten each non-ghost parent tree to the shape the Rust side
        # expects: (path_utf8, file_id, details).  details is the
        # 5-tuple produced by inv_entry_to_details.  iter_entries_by_dir
        # already yields entries in the required order.
        parent_tree_entries = []
        for rev_id, tree in trees:
            if rev_id in ghosts:
                continue
            rows = []
            for path, inv_entry in tree.iter_entries_by_dir():
                rows.append(
                    (
                        path.encode("utf8"),
                        inv_entry.file_id,
                        _inv_entry_to_details(inv_entry),
                    )
                )
            parent_tree_entries.append(rows)
        self._rs.set_parent_trees(parent_ids, ghosts_list, parent_tree_entries)
        if self._id_index is not None:
            self._id_index.fill_from_state(self._rs)

    def set_state_from_inventory(self, new_inv):
        """Set new_inv as the current state.

        This API is called by tree transform, and will usually occur with
        existing parent trees.

        :param new_inv: The inventory object to set current state from.
        """
        evil_logger.debug(
            "set_state_from_inventory called; please mutate the tree instead"
        )
        self._read_dirblocks_if_needed()
        # Flatten the inventory into a pre-sorted sequence of
        # (path_utf8, file_id, minikind, fingerprint, executable) rows.
        # iter_entries_by_dir already yields in the order the zipper
        # loop expects.
        rows = []
        for path, inv_entry in new_inv.iter_entries_by_dir():
            minikind = DirState._kind_to_minikind[inv_entry.kind]
            if minikind == b"t":
                fingerprint = inv_entry.reference_revision or b""
            else:
                fingerprint = b""
            rows.append(
                (
                    path.encode("utf8"),
                    inv_entry.file_id,
                    minikind,
                    fingerprint,
                    bool(inv_entry.executable),
                )
            )
        self._rs.set_state_from_inventory(rows)
        if self._id_index is not None:
            self._id_index.fill_from_state(self._rs)

    def set_state_from_scratch(self, working_inv, parent_trees, parent_ghosts):
        """Wipe the currently stored state and set it to something new.

        This is a hard-reset for the data we are working with.
        """
        # Technically, we really want a write lock, but until we write, we
        # don't really need it.
        self._requires_lock()
        # root dir and root dir contents with no children. We have to have a
        # root for set_state_from_inventory to work correctly.
        empty_root = (
            (b"", b"", inventory.ROOT_ID),
            [(b"d", b"", 0, False, DirState.NULLSTAT)],
        )
        empty_tree_dirblocks = [(b"", [empty_root]), (b"", [])]
        self._set_data([], empty_tree_dirblocks)
        self.set_state_from_inventory(working_inv)
        self.set_parent_trees(parent_trees, parent_ghosts)

    def _make_absent(self, current_old):
        """Mark current_old - an entry - as absent for tree 0.

        :return: True if this was the last details entry for the entry key:
            that is, if the underlying block has had the entry removed, thus
            shrinking in length.
        """
        last_reference = self._rs.make_absent(current_old[0])
        if self._id_index is not None:
            self._id_index.fill_from_state(self._rs)
        return last_reference

    def update_minimal(
        self,
        key,
        minikind,
        executable=False,
        fingerprint=b"",
        packed_stat=None,
        size=0,
        path_utf8=None,
        fullscan=False,
    ):
        """Update an entry to the state in tree 0.

        This will either create a new entry at 'key' or update an existing one.
        It also makes sure that any other records which might mention this are
        updated as well.

        :param key: (dir, name, file_id) for the new entry
        :param minikind: The type for the entry (b'f' == 'file', b'd' ==
                'directory'), etc.
        :param executable: Should the executable bit be set?
        :param fingerprint: Simple fingerprint for new entry: canonical-form
            sha1 for files, referenced revision id for subtrees, etc.
        :param packed_stat: Packed stat value for new entry.
        :param size: Size information for new entry
        :param path_utf8: key[0] + '/' + key[1], just passed in to avoid doing
                extra computation.
        :param fullscan: If True then a complete scan of the dirstate is being
            done and checking for duplicate rows should not be done. This
            should only be set by set_state_from_inventory and similar methods.

        If packed_stat and fingerprint are not given, they're invalidated in
        the entry.
        """
        self._rs.update_minimal(
            key,
            minikind,
            executable,
            fingerprint,
            packed_stat,
            size,
            path_utf8,
            fullscan,
        )
        if self._id_index is not None:
            # Keep callers' cached reference live by refilling from Rust's
            # authoritative id_index, not by re-walking dirblocks.
            self._id_index.fill_from_state(self._rs)

    def _validate(self):
        """Check that invariants on the dirblock are correct.

        This can be useful in debugging; it shouldn't be necessary in
        normal code.

        This must be called with a lock held.
        """
        self._read_dirblocks_if_needed()
        self._rs.validate()

    def _wipe_state(self):
        """Forget all state information about the dirstate."""
        # Rust side wipes header/dirblock state, changes_aborted,
        # parents, ghosts, dirblocks, id_index, end_of_header,
        # cutoff_time.
        self._rs.wipe_state()
        if self._id_index is not None:
            self._id_index.fill_from_state(self._rs)

    def lock_read(self):
        """Acquire a read lock on the dirstate."""
        if self._lock_token is not None:
            raise LockContention(self._lock_token)
        # TODO: jam 20070301 Rather than wiping completely, if the blocks are
        #       already in memory, we could read just the header and check for
        #       any modification. If not modified, we can just leave things
        #       alone
        self._lock_token = lock.ReadLock(self._filename)
        self._lock_state = "r"
        self._state_file = self._lock_token.f
        self._wipe_state()
        return lock.LogicalLockResult(self.unlock)

    def lock_write(self):
        """Acquire a write lock on the dirstate."""
        if self._lock_token is not None:
            raise LockContention(self._lock_token)
        # TODO: jam 20070301 Rather than wiping completely, if the blocks are
        #       already in memory, we could read just the header and check for
        #       any modification. If not modified, we can just leave things
        #       alone
        self._lock_token = lock.WriteLock(self._filename)
        self._lock_state = "w"
        self._state_file = self._lock_token.f
        self._wipe_state()
        return lock.LogicalLockResult(self.unlock, self._lock_token)

    def unlock(self):
        """Drop any locks held on the dirstate."""
        if self._lock_token is None:
            raise LockNotHeld(self)
        # TODO: jam 20070301 Rather than wiping completely, if the blocks are
        #       already in memory, we could read just the header and check for
        #       any modification. If not modified, we can just leave things
        #       alone
        self._state_file = None
        self._lock_state = None
        self._lock_token.unlock()
        self._lock_token = None

    def _requires_lock(self):
        """Check that a lock is currently held by someone on the dirstate."""
        if not self._lock_token:
            raise ObjectNotLocked(self)


def py_update_entry(state, entry, abspath, stat_value):
    """Update the entry based on what is actually on disk.

    Thin shim: delegates to DirStateRs.update_entry.  Mirrors Python's
    historical in-place entry-tuple mutation by rebuilding entry[1][0]
    from the new tree-0 Rust wrote.
    """
    if isinstance(abspath, str):
        abspath = abspath.encode("utf-8")
    link_or_sha1 = state._rs.update_entry(entry[0], abspath, stat_value)
    # Refresh the in-memory entry tuple so legacy callers that hang
    # on to the snapshot still see the new tree-0.
    fresh = state._rs.get_entry(0, path_utf8=osutils.pathjoin(entry[0][0], entry[0][1]))
    if fresh != (None, None):
        entry[1][0] = fresh[1][0]
    return link_or_sha1


class ProcessEntryPython:
    """Python implementation for processing directory state entries."""

    __slots__ = [
        "include_unchanged",
        "last_source_parent",
        "last_target_parent",
        "new_dirname_to_file_id",
        "old_dirname_to_file_id",
        "partial",
        "search_specific_file_parents",
        "search_specific_files",
        "searched_exact_paths",
        "searched_specific_files",
        "seen_ids",
        "source_index",
        "state",
        "target_index",
        "tree",
        "use_filesystem_for_exec",
        "utf8_decode",
        "want_unversioned",
    ]

    def __init__(
        self,
        include_unchanged,
        use_filesystem_for_exec,
        search_specific_files,
        state,
        source_index,
        target_index,
        want_unversioned,
        tree,
    ):
        """Initialize the ProcessEntryPython.

        Args:
            include_unchanged: Whether to include unchanged entries.
            use_filesystem_for_exec: Whether to use filesystem for executable checks.
            search_specific_files: Specific files to search for.
            state: The dirstate being processed.
            source_index: Index of the source tree.
            target_index: Index of the target tree.
            want_unversioned: Whether to include unversioned files.
            tree: The tree object.
        """
        self.old_dirname_to_file_id = {}
        self.new_dirname_to_file_id = {}
        # Are we doing a partial iter_changes?
        self.partial = search_specific_files != {""}
        # Using a list so that we can access the values and change them in
        # nested scope. Each one is [path, file_id, entry]
        self.last_source_parent = [None, None]
        self.last_target_parent = [None, None]
        self.include_unchanged = include_unchanged
        self.use_filesystem_for_exec = use_filesystem_for_exec
        self.utf8_decode = codecs.utf_8_decode
        # for all search_indexs in each path at or under each element of
        # search_specific_files, if the detail is relocated: add the id, and
        # add the relocated path as one to search if its not searched already.
        # If the detail is not relocated, add the id.
        self.searched_specific_files = set()
        # When we search exact paths without expanding downwards, we record
        # that here.
        self.searched_exact_paths = set()
        self.search_specific_files = search_specific_files
        # The parents up to the root of the paths we are searching.
        # After all normal paths are returned, these specific items are returned.
        self.search_specific_file_parents = set()
        # The ids we've sent out in the delta.
        self.seen_ids = set()
        self.state = state
        self.source_index = source_index
        self.target_index = target_index
        if target_index != 0:
            # A lot of code in here depends on target_index == 0
            raise BzrFormatsError("unsupported target index")
        self.want_unversioned = want_unversioned
        self.tree = tree

    def __iter__(self):
        """Return iterator for processing entries."""
        return self.iter_changes()

    def iter_changes(self):
        """Iterate over the changes.

        Thin forwarder to the pure-crate IterChangesIter, which runs
        the full walk, per-entry comparison, parent-consistency
        gathering, and specific-file-parents drain as a single
        state machine.  Yields :class:`DirstateInventoryChange`
        tuples one at a time.
        """
        if self.state is None:
            # Empty-state callers (the iterator-protocol regression
            # test) only care that iterating an empty
            # search_specific_files yields nothing.
            if self.search_specific_files:
                raise AssertionError(
                    "iter_changes with state=None requires empty search_specific_files"
                )
            return
        root_abspath = (
            self.tree.abspath("").encode("utf8", "surrogateescape")
            if self.tree is not None
            else b""
        )
        supports_tree_reference = bool(
            getattr(self.tree, "_repo_supports_tree_reference", False)
        )
        rs_iter = self.state._rs.iter_changes(
            self.source_index,
            self.target_index,
            self.include_unchanged,
            self.want_unversioned,
            self.search_specific_files,
            supports_tree_reference,
            root_abspath,
        )
        try:
            yield from rs_iter
        finally:
            # Surface the walker's final state back onto the Python
            # instance for callers that inspect it after iteration.
            self.searched_specific_files = rs_iter.searched_specific_files
            self.search_specific_files = rs_iter.search_specific_files
            self.searched_exact_paths = rs_iter.searched_exact_paths
            self.search_specific_file_parents = rs_iter.search_specific_file_parents
            self.seen_ids = rs_iter.seen_ids


from ._bzr_rs import dirstate as _dirstate_rs

DefaultSHA1Provider = _dirstate_rs.DefaultSHA1Provider
bisect_dirblock = _dirstate_rs.bisect_dirblock
bisect_path_left = _dirstate_rs.bisect_path_left
bisect_path_right = _dirstate_rs.bisect_path_right
lt_by_dirs = _dirstate_rs.lt_by_dirs
lt_path_by_dirblock = _dirstate_rs.lt_path_by_dirblock
pack_stat = _dirstate_rs.pack_stat
_fields_per_entry = _dirstate_rs.fields_per_entry
_get_ghosts_line = _dirstate_rs.get_ghosts_line
_get_parents_line = _dirstate_rs.get_parents_line
IdIndex = _dirstate_rs.IdIndex
_inv_entry_to_details = _dirstate_rs.inv_entry_to_details
_get_output_lines = _dirstate_rs.get_output_lines

_read_dirblocks = _dirstate_rs._read_dirblocks
update_entry = _dirstate_rs.update_entry
_process_entry = _dirstate_rs.ProcessEntryC
