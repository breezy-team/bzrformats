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

"""Pack repository objects."""

import hashlib
import logging
import sys
import time

logger = logging.getLogger(__name__)

from . import btree_index
from . import pack as _mod_pack
from .errors import BzrCheckError, BzrFormatsError
from .transport import TransportNoSuchFile


class RetryWithNewPacks(BzrFormatsError):
    """Raised when we realize that the packs on disk have changed.

    This is meant as more of a signaling exception, to trap between where a
    local error occurred and the code that can actually handle the error and
    code that can retry appropriately.
    """

    internal_error = True

    _fmt = (
        "Pack files have changed, reload and retry. context: %(context)s %(orig_error)s"
    )

    def __init__(self, context, reload_occurred, exc_info):
        """Create a new RetryWithNewPacks error.

        :param reload_occurred: Set to True if we know that the packs have
            already been reloaded, and we are failing because of an in-memory
            cache miss. If set to True then we will ignore if a reload says
            nothing has changed, because we assume it has already reloaded. If
            False, then a reload with nothing changed will force an error.
        :param exc_info: The original exception traceback, so if there is a
            problem we can raise the original error (value from sys.exc_info())
        """
        BzrFormatsError.__init__(self)
        self.context = context
        self.reload_occurred = reload_occurred
        self.exc_info = exc_info
        self.orig_error = exc_info[1]
        # TODO: The global error handler should probably treat this by
        #       raising/printing the original exception with a bit about
        #       RetryWithNewPacks also not being caught


class _DirectPackAccess:
    """Access to data in one or more packs with less translation."""

    def __init__(self, index_to_packs, reload_func=None, flush_func=None):
        """Create a _DirectPackAccess object.

        :param index_to_packs: A dict mapping index objects to the transport
            and file names for obtaining data.
        :param reload_func: A function to call if we determine that the pack
            files have moved and we need to reload our caches. See
            breezy.repo_fmt.pack_repo.AggregateIndex for more details.
        """
        self._container_writer = None
        self._write_index = None
        self._indices = index_to_packs
        self._reload_func = reload_func
        self._flush_func = flush_func

    def add_raw_record(self, key, size, raw_data):
        """Add raw knit bytes to a storage area.

        The data is spooled to the container writer in one bytes-record per
        raw data item.

        :param key: key of the data segment
        :param size: length of the data segment
        :param raw_data: A bytestring containing the data.
        :return: An opaque index memo For _DirectPackAccess the memo is
            (index, pos, length), where the index field is the write_index
            object supplied to the PackAccess object.
        """
        p_offset, p_length = self._container_writer.add_bytes_record(raw_data, size, [])
        return (self._write_index, p_offset, p_length)

    def add_raw_records(self, key_sizes, raw_data):
        """Add raw knit bytes to a storage area.

        The data is spooled to the container writer in one bytes-record per
        raw data item.

        :param sizes: An iterable of tuples containing the key and size of each
            raw data segment.
        :param raw_data: A bytestring containing the data.
        :return: A list of memos to retrieve the record later. Each memo is an
            opaque index memo. For _DirectPackAccess the memo is (index, pos,
            length), where the index field is the write_index object supplied
            to the PackAccess object.
        """
        raw_data = b"".join(raw_data)
        if not isinstance(raw_data, bytes):
            raise AssertionError(f"data must be plain bytes was {type(raw_data)}")
        result = []
        offset = 0
        for key, size in key_sizes:
            result.append(
                self.add_raw_record(key, size, [raw_data[offset : offset + size]])
            )
            offset += size
        return result

    def flush(self):
        """Flush pending writes on this access object.

        This will flush any buffered writes to a NewPack.
        """
        if self._flush_func is not None:
            self._flush_func()

    def get_raw_records(self, memos_for_retrieval):
        """Get the raw bytes for a records.

        :param memos_for_retrieval: An iterable containing the (index, pos,
            length) memo for retrieving the bytes. The Pack access method
            looks up the pack to use for a given record in its index_to_pack
            map.
        :return: An iterator over the bytes of the records.
        """
        # first pass, group into same-index requests
        request_lists = []
        current_index = None
        for index, offset, length in memos_for_retrieval:
            if current_index == index:
                current_list.append((offset, length))
            else:
                if current_index is not None:
                    request_lists.append((current_index, current_list))
                current_index = index
                current_list = [(offset, length)]
        # handle the last entry
        if current_index is not None:
            request_lists.append((current_index, current_list))
        for index, offsets in request_lists:
            try:
                transport, path = self._indices[index]
            except KeyError as e:
                # A KeyError here indicates that someone has triggered an index
                # reload, and this index has gone missing, we need to start
                # over.
                if self._reload_func is None:
                    # If we don't have a _reload_func there is nothing that can
                    # be done
                    raise
                raise RetryWithNewPacks(
                    index, reload_occurred=True, exc_info=sys.exc_info()
                ) from e
            try:
                reader = _mod_pack.make_readv_reader(transport, path, offsets)
                for _names, read_func in reader.iter_records():
                    yield read_func(None)
            except TransportNoSuchFile as e:
                # A NoSuchFile error indicates that a pack file has gone
                # missing on disk, we need to trigger a reload, and start over.
                if self._reload_func is None:
                    raise
                raise RetryWithNewPacks(
                    transport.abspath(path),
                    reload_occurred=False,
                    exc_info=sys.exc_info(),
                ) from e

    def set_writer(self, writer, index, transport_packname):
        """Set a writer to use for adding data."""
        if index is not None:
            self._indices[index] = transport_packname
        self._container_writer = writer
        self._write_index = index

    def reload_or_raise(self, retry_exc):
        """Try calling the reload function, or re-raise the original exception.

        This should be called after _DirectPackAccess raises a
        RetryWithNewPacks exception. This function will handle the common logic
        of determining when the error is fatal versus being temporary.
        It will also make sure that the original exception is raised, rather
        than the RetryWithNewPacks exception.

        If this function returns, then the calling function should retry
        whatever operation was being performed. Otherwise an exception will
        be raised.

        :param retry_exc: A RetryWithNewPacks exception.
        """
        is_error = False
        if self._reload_func is None:
            is_error = True
        elif not self._reload_func():
            # The reload claimed that nothing changed
            if not retry_exc.reload_occurred:
                # If there wasn't an earlier reload, then we really were
                # expecting to find changes. We didn't find them, so this is a
                # hard error
                is_error = True
        if is_error:
            # GZ 2017-03-27: No real reason this needs the original traceback.
            raise retry_exc.exc_info[1]


class Pack:
    """An in memory proxy for a pack and its indices.

    This is a base class that is not directly used, instead the classes
    ExistingPack and NewPack are used.
    """

    # A map of index 'type' to the file extension and position in the
    # index_sizes array.
    index_definitions = {
        "chk": (".cix", 4),
        "revision": (".rix", 0),
        "inventory": (".iix", 1),
        "text": (".tix", 2),
        "signature": (".six", 3),
    }

    def __init__(
        self,
        revision_index,
        inventory_index,
        text_index,
        signature_index,
        chk_index=None,
    ):
        """Create a pack instance.

        :param revision_index: A GraphIndex for determining what revisions are
            present in the Pack and accessing the locations of their texts.
        :param inventory_index: A GraphIndex for determining what inventories are
            present in the Pack and accessing the locations of their
            texts/deltas.
        :param text_index: A GraphIndex for determining what file texts
            are present in the pack and accessing the locations of their
            texts/deltas (via (fileid, revisionid) tuples).
        :param signature_index: A GraphIndex for determining what signatures are
            present in the Pack and accessing the locations of their texts.
        :param chk_index: A GraphIndex for accessing content by CHK, if the
            pack has one.
        """
        self.revision_index = revision_index
        self.inventory_index = inventory_index
        self.text_index = text_index
        self.signature_index = signature_index
        self.chk_index = chk_index

    def access_tuple(self):
        """Return a tuple (transport, name) for the pack content."""
        return self.pack_transport, self.file_name()

    def _check_references(self):
        """Make sure our external references are present.

        Packs are allowed to have deltas whose base is not in the pack, but it
        must be present somewhere in this collection.  It is not allowed to
        have deltas based on a fallback repository.
        (See <https://bugs.launchpad.net/bzr/+bug/288751>)
        """
        missing_items = {}
        for index_name, external_refs, index in [
            (
                "texts",
                self._get_external_refs(self.text_index),
                self._pack_collection.text_index.combined_index,
            ),
            (
                "inventories",
                self._get_external_refs(self.inventory_index),
                self._pack_collection.inventory_index.combined_index,
            ),
        ]:
            missing = external_refs.difference(
                k for (idx, k, v, r) in index.iter_entries(external_refs)
            )
            if missing:
                missing_items[index_name] = sorted(missing)
        if missing_items:
            from pprint import pformat

            raise BzrCheckError(
                f"Newly created pack file {self!r} has delta references to "
                f"items not in its repository:\n{pformat(missing_items)}"
            )

    def file_name(self):
        """Get the file name for the pack on disk."""
        return self.name + ".pack"

    def get_revision_count(self):
        """Return the number of revisions in this pack."""
        return self.revision_index.key_count()

    def index_name(self, index_type, name):
        """Get the disk name of an index type for pack name 'name'."""
        return name + Pack.index_definitions[index_type][0]

    def index_offset(self, index_type):
        """Get the position in a index_size array for a given index type."""
        return Pack.index_definitions[index_type][1]

    def inventory_index_name(self, name):
        """The inv index is the name + .iix."""
        return self.index_name("inventory", name)

    def revision_index_name(self, name):
        """The revision index is the name + .rix."""
        return self.index_name("revision", name)

    def signature_index_name(self, name):
        """The signature index is the name + .six."""
        return self.index_name("signature", name)

    def text_index_name(self, name):
        """The text index is the name + .tix."""
        return self.index_name("text", name)

    def _replace_index_with_readonly(self, index_type):
        unlimited_cache = False
        if index_type == "chk":
            unlimited_cache = True
        index = self.index_class(
            self.index_transport,
            self.index_name(index_type, self.name),
            self.index_sizes[self.index_offset(index_type)],
            unlimited_cache=unlimited_cache,
        )
        if index_type == "chk":
            index._leaf_factory = btree_index._gcchk_factory
        setattr(self, index_type + "_index", index)

    def __lt__(self, other):
        """Compare packs by identity for ordering."""
        if not isinstance(other, Pack):
            raise TypeError(other)
        return id(self) < id(other)

    def __hash__(self):
        """Return hash based on index objects."""
        return hash(
            (
                type(self),
                self.revision_index,
                self.inventory_index,
                self.text_index,
                self.signature_index,
                self.chk_index,
            )
        )


class ExistingPack(Pack):
    """An in memory proxy for an existing .pack and its disk indices."""

    def __init__(
        self,
        pack_transport,
        name,
        revision_index,
        inventory_index,
        text_index,
        signature_index,
        chk_index=None,
    ):
        """Create an ExistingPack object.

        :param pack_transport: The transport where the pack file resides.
        :param name: The name of the pack on disk in the pack_transport.
        """
        Pack.__init__(
            self,
            revision_index,
            inventory_index,
            text_index,
            signature_index,
            chk_index,
        )
        self.name = name
        self.pack_transport = pack_transport
        if None in (
            revision_index,
            inventory_index,
            text_index,
            signature_index,
            name,
            pack_transport,
        ):
            raise AssertionError()

    def __eq__(self, other):
        """Check equality by comparing all attributes."""
        return self.__dict__ == other.__dict__

    def __ne__(self, other):
        """Check inequality."""
        return not self.__eq__(other)

    def __repr__(self):
        """Return string representation."""
        return "<{}.{} object at 0x{:x}, {}, {}".format(
            self.__class__.__module__,
            self.__class__.__name__,
            id(self),
            self.pack_transport,
            self.name,
        )

    def __hash__(self):
        """Return hash based on type and name."""
        return hash((type(self), self.name))


class ResumedPack(ExistingPack):
    """A pack being resumed from an interrupted upload."""

    def __init__(
        self,
        name,
        revision_index,
        inventory_index,
        text_index,
        signature_index,
        upload_transport,
        pack_transport,
        index_transport,
        pack_collection,
        chk_index=None,
    ):
        """Create a ResumedPack object."""
        ExistingPack.__init__(
            self,
            pack_transport,
            name,
            revision_index,
            inventory_index,
            text_index,
            signature_index,
            chk_index=chk_index,
        )
        self.upload_transport = upload_transport
        self.index_transport = index_transport
        self.index_sizes = [None, None, None, None]
        indices = [
            ("revision", revision_index),
            ("inventory", inventory_index),
            ("text", text_index),
            ("signature", signature_index),
        ]
        if chk_index is not None:
            indices.append(("chk", chk_index))
            self.index_sizes.append(None)
        for index_type, index in indices:
            offset = self.index_offset(index_type)
            self.index_sizes[offset] = index._size
        self.index_class = pack_collection._index_class
        self._pack_collection = pack_collection
        self._state = "resumed"
        # XXX: perhaps check that the .pack file exists?

    def access_tuple(self):
        """Return the transport and file name for accessing the pack data."""
        if self._state == "finished":
            return Pack.access_tuple(self)
        elif self._state == "resumed":
            return self.upload_transport, self.file_name()
        else:
            raise AssertionError(self._state)

    def abort(self):
        """Abort the resumed pack, deleting its files."""
        self.upload_transport.delete(self.file_name())
        indices = [
            self.revision_index,
            self.inventory_index,
            self.text_index,
            self.signature_index,
        ]
        if self.chk_index is not None:
            indices.append(self.chk_index)
        for index in indices:
            index._transport.delete(index._name)

    def finish(self):
        """Finish the resumed pack, moving files into place."""
        self._check_references()
        index_types = ["revision", "inventory", "text", "signature"]
        if self.chk_index is not None:
            index_types.append("chk")
        for index_type in index_types:
            old_name = self.index_name(index_type, self.name)
            new_name = "../indices/" + old_name
            self.upload_transport.move(old_name, new_name)
            self._replace_index_with_readonly(index_type)
        new_name = "../packs/" + self.file_name()
        self.upload_transport.move(self.file_name(), new_name)
        self._state = "finished"

    def _get_external_refs(self, index):
        """Return compression parents for this index that are not present.

        This returns any compression parents that are referenced by this index,
        which are not contained *in* this index. They may be present elsewhere.
        """
        return index.external_references(1)


class NewPack(Pack):
    """An in memory proxy for a pack which is being created."""

    def __init__(self, pack_collection, upload_suffix="", file_mode=None):
        """Create a NewPack instance.

        :param pack_collection: A PackCollection into which this is being inserted.
        :param upload_suffix: An optional suffix to be given to any temporary
            files created during the pack creation. e.g '.autopack'
        :param file_mode: Unix permissions for newly created file.
        """
        # The relative locations of the packs are constrained, but all are
        # passed in because the caller has them, so as to avoid object churn.
        index_builder_class = pack_collection._index_builder_class
        if pack_collection.chk_index is not None:
            chk_index = index_builder_class(reference_lists=0)
        else:
            chk_index = None
        Pack.__init__(
            self,
            # Revisions: parents list, no text compression.
            index_builder_class(reference_lists=1),
            # Inventory: We want to map compression only, but currently the
            # knit code hasn't been updated enough to understand that, so we
            # have a regular 2-list index giving parents and compression
            # source.
            index_builder_class(reference_lists=2),
            # Texts: compression and per file graph, for all fileids - so two
            # reference lists and two elements in the key tuple.
            index_builder_class(reference_lists=2, key_elements=2),
            # Signatures: Just blobs to store, no compression, no parents
            # listing.
            index_builder_class(reference_lists=0),
            # CHK based storage - just blobs, no compression or parents.
            chk_index=chk_index,
        )
        self._pack_collection = pack_collection
        # When we make readonly indices, we need this.
        self.index_class = pack_collection._index_class
        # where should the new pack be opened
        self.upload_transport = pack_collection._upload_transport
        # where are indices written out to
        self.index_transport = pack_collection._index_transport
        # where is the pack renamed to when it is finished?
        self.pack_transport = pack_collection._pack_transport
        # What file mode to upload the pack and indices with.
        self._file_mode = file_mode
        # tracks the content written to the .pack file.
        self._hash = hashlib.md5()  # noqa: S324
        # a tuple with the length in bytes of the indices, once the pack
        # is finalised. (rev, inv, text, sigs, chk_if_in_use)
        self.index_sizes = None
        # How much data to cache when writing packs. Note that this is not
        # synchronised with reads, because it's not in the transport layer, so
        # is not safe unless the client knows it won't be reading from the pack
        # under creation.
        self._cache_limit = 0
        # the temporary pack file name.
        from .osutils import rand_chars

        self.random_name = rand_chars(20) + upload_suffix
        # when was this pack started ?
        self.start_time = time.time()
        # open an output stream for the data added to the pack.
        self.write_stream = self.upload_transport.open_write_stream(
            self.random_name, mode=self._file_mode
        )
        logger.debug(
            "%s: create_pack: pack stream open: %s%s t+%6.3fs",
            time.ctime(),
            self.upload_transport.base,
            self.random_name,
            time.time() - self.start_time,
        )
        # A list of byte sequences to be written to the new pack, and the
        # aggregate size of them.  Stored as a list rather than separate
        # variables so that the _write_data closure below can update them.
        self._buffer = [[], 0]
        # create a callable for adding data
        #
        # robertc says- this is a closure rather than a method on the object
        # so that the variables are locals, and faster than accessing object
        # members.

        def _write_data(
            bytes,
            flush=False,
            _buffer=self._buffer,
            _write=self.write_stream.write,
            _update=self._hash.update,
        ):
            _buffer[0].append(bytes)
            _buffer[1] += len(bytes)
            # buffer cap
            if _buffer[1] > self._cache_limit or flush:
                bytes = b"".join(_buffer[0])
                _write(bytes)
                _update(bytes)
                _buffer[:] = [[], 0]

        # expose this on self, for the occasion when clients want to add data.
        self._write_data = _write_data
        # a pack writer object to serialise pack records.
        self._writer = _mod_pack.ContainerWriter(self._write_data)
        self._writer.begin()
        # what state is the pack in? (open, finished, aborted)
        self._state = "open"
        # no name until we finish writing the content
        self.name = None

    def abort(self):
        """Cancel creating this pack."""
        self._state = "aborted"
        self.write_stream.close()
        # Remove the temporary pack file.
        self.upload_transport.delete(self.random_name)
        # The indices have no state on disk.

    def access_tuple(self):
        """Return a tuple (transport, name) for the pack content."""
        if self._state == "finished":
            return Pack.access_tuple(self)
        elif self._state == "open":
            return self.upload_transport, self.random_name
        else:
            raise AssertionError(self._state)

    def data_inserted(self):
        """True if data has been added to this pack."""
        return bool(
            self.get_revision_count()
            or self.inventory_index.key_count()
            or self.text_index.key_count()
            or self.signature_index.key_count()
            or (self.chk_index is not None and self.chk_index.key_count())
        )

    def finish_content(self):
        """Finalize the pack content and compute the content hash name."""
        if self.name is not None:
            return
        self._writer.end()
        if self._buffer[1]:
            self._write_data(b"", flush=True)
        self.name = self._hash.hexdigest()

    def finish(self, suspend=False):
        """Finish the new pack.

        This:
         - finalises the content
         - assigns a name (the md5 of the content, currently)
         - writes out the associated indices
         - renames the pack into place.
         - stores the index size tuple for the pack in the index_sizes
           attribute.
        """
        self.finish_content()
        if not suspend:
            self._check_references()
        # write indices
        # XXX: It'd be better to write them all to temporary names, then
        # rename them all into place, so that the window when only some are
        # visible is smaller.  On the other hand none will be seen until
        # they're in the names list.
        self.index_sizes = [None, None, None, None]
        self._write_index("revision", self.revision_index, "revision", suspend)
        self._write_index("inventory", self.inventory_index, "inventory", suspend)
        self._write_index("text", self.text_index, "file texts", suspend)
        self._write_index(
            "signature", self.signature_index, "revision signatures", suspend
        )
        if self.chk_index is not None:
            self.index_sizes.append(None)
            self._write_index("chk", self.chk_index, "content hash bytes", suspend)
        self.write_stream.close(
            want_fdatasync=self._pack_collection.config_stack.get(
                "repository.fdatasync"
            )
        )
        # Note that this will clobber an existing pack with the same name,
        # without checking for hash collisions. While this is undesirable this
        # is something that can be rectified in a subsequent release. One way
        # to rectify it may be to leave the pack at the original name, writing
        # its pack-names entry as something like 'HASH: index-sizes
        # temporary-name'. Allocate that and check for collisions, if it is
        # collision free then rename it into place. If clients know this scheme
        # they can handle missing-file errors by:
        #  - try for HASH.pack
        #  - try for temporary-name
        #  - refresh the pack-list to see if the pack is now absent
        new_name = self.name + ".pack"
        if not suspend:
            new_name = "../packs/" + new_name
        self.upload_transport.move(self.random_name, new_name)
        self._state = "finished"
        # XXX: size might be interesting?
        logger.debug(
            "%s: create_pack: pack finished: %s%s->%s t+%6.3fs",
            time.ctime(),
            self.upload_transport.base,
            self.random_name,
            new_name,
            time.time() - self.start_time,
        )

    def flush(self):
        """Flush any current data."""
        if self._buffer[1]:
            bytes = b"".join(self._buffer[0])
            self.write_stream.write(bytes)
            self._hash.update(bytes)
            self._buffer[:] = [[], 0]

    def _get_external_refs(self, index):
        return index._external_references()

    def set_write_cache_size(self, size):
        """Set the write cache size in bytes."""
        self._cache_limit = size

    def _write_index(self, index_type, index, label, suspend=False):
        """Write out an index.

        :param index_type: The type of index to write - e.g. 'revision'.
        :param index: The index object to serialise.
        :param label: What label to give the index e.g. 'revision'.
        """
        index_name = self.index_name(index_type, self.name)
        transport = self.upload_transport if suspend else self.index_transport
        index_tempfile = index.finish()
        index_bytes = index_tempfile.read()
        write_stream = transport.open_write_stream(index_name, mode=self._file_mode)
        write_stream.write(index_bytes)
        write_stream.close(
            want_fdatasync=self._pack_collection.config_stack.get(
                "repository.fdatasync"
            )
        )
        self.index_sizes[self.index_offset(index_type)] = len(index_bytes)
        # XXX: size might be interesting?
        logger.debug(
            "%s: create_pack: wrote %s index: %s%s t+%6.3fs",
            time.ctime(),
            label,
            self.upload_transport.base,
            self.random_name,
            time.time() - self.start_time,
        )
        # Replace the writable index on this object with a readonly,
        # presently unloaded index. We should alter
        # the index layer to make its finish() error if add_node is
        # subsequently used. RBC
        self._replace_index_with_readonly(index_type)
