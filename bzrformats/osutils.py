# Copyright (C) 2025 Breezy Contributors
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

"""OS utilities for bzrformats using only standard library."""

import hashlib
import logging
import os
import shutil
import sys
import unicodedata


def isdir(path):
    """Return True if the given path exists and is a directory."""
    return os.path.isdir(path)


def split(path):
    """Split a pathname into directory and basename parts."""
    if isinstance(path, bytes):
        return os.path.split(path)
    else:
        # For unicode strings, encode to UTF-8, split, then decode
        encoded = path.encode("utf-8")
        dirname, basename = os.path.split(encoded)
        return dirname.decode("utf-8"), basename.decode("utf-8")


def pathjoin(*args):
    """Join paths together."""
    if not args:
        return b"" if isinstance(args[0], bytes) else ""

    # Check if we're dealing with bytes or strings
    if isinstance(args[0], bytes):
        return os.path.join(*args)
    else:
        # For unicode strings, encode to UTF-8, join, then decode
        encoded_args = [arg.encode("utf-8") for arg in args]
        result = os.path.join(*encoded_args)
        return result.decode("utf-8")


def pumpfile(from_file, to_file, buffer_size=65536):
    """Copy data from one file-like object to another.

    Returns the number of bytes copied.
    """
    initial_pos = to_file.tell() if hasattr(to_file, "tell") else 0
    shutil.copyfileobj(from_file, to_file, buffer_size)
    if hasattr(to_file, "tell"):
        return to_file.tell() - initial_pos
    else:
        # If we can't tell the position, we can't return accurate byte count
        return 0


def chunks_to_lines(chunks):
    """Convert chunks to lines."""
    if not chunks:
        return []

    # Join all chunks
    data = b"".join(chunks)

    # Split into lines, keeping line endings
    lines = []
    start = 0
    for i, byte in enumerate(data):
        if byte == ord(b"\n"):
            lines.append(data[start : i + 1])
            start = i + 1

    # Add remaining data if any
    if start < len(data):
        lines.append(data[start:])

    return lines


def normalized_filename(filename):
    """Return the normalized form of a filename.

    Returns (normalized_name, can_access) tuple.
    """
    if isinstance(filename, bytes):
        # For bytes, try to decode as UTF-8 first
        try:
            unicode_filename = filename.decode("utf-8")
        except UnicodeDecodeError:
            # If it's not valid UTF-8, return as-is
            return filename, True
    else:
        unicode_filename = filename

    # Normalize using NFC (Canonical Decomposition, followed by Canonical Composition)
    normalized = unicodedata.normalize("NFC", unicode_filename)

    if isinstance(filename, bytes):
        try:
            return normalized.encode("utf-8"), True
        except UnicodeEncodeError:
            return filename, True
    else:
        return normalized, True


def failed_to_load_extension(exception):
    """Log a message about a failed extension load."""
    logging.debug("Failed to load extension: %s", exception)


def fdatasync(fileno):
    """Flush file contents to disk, not metadata."""
    try:
        os.fdatasync(fileno)
    except AttributeError:
        # fdatasync is not available on all platforms (e.g., Windows)
        # Fall back to fsync which is more widely available
        os.fsync(fileno)


def splitpath(path):
    """Split a path into a list of components."""
    if isinstance(path, bytes):
        if path.startswith(b"/"):
            path = path[1:]
        if not path:
            return []
        return path.split(b"/")
    else:
        if path.startswith("/"):
            path = path[1:]
        if not path:
            return []
        return path.split("/")


def file_kind_from_stat_mode(mode):
    """Return the file kind based on the stat mode."""
    import stat

    if stat.S_ISREG(mode):
        return "file"
    elif stat.S_ISDIR(mode):
        return "directory"
    elif stat.S_ISLNK(mode):
        return "symlink"
    elif stat.S_ISFIFO(mode):
        return "fifo"
    elif stat.S_ISSOCK(mode):
        return "socket"
    elif stat.S_ISCHR(mode):
        return "chardev"
    elif stat.S_ISBLK(mode):
        return "block"
    else:
        return "unknown"


def contains_whitespace(s):
    """Return True if the string contains whitespace characters."""
    # Check for common whitespace characters
    if isinstance(s, bytes):
        return any(c in s for c in b" \t\n\r\v\f")
    else:
        return any(c in s for c in " \t\n\r\v\f")


def sha_strings(strings):
    """Return the sha1 of concatenated strings."""
    sha = hashlib.sha1()  # noqa: S324
    for string in strings:
        if isinstance(string, str):
            # Convert unicode strings to bytes using UTF-8
            string = string.encode("utf-8")
        sha.update(string)
    return sha.hexdigest().encode("ascii")


def sha_string(string):
    """Return the sha1 of a single string."""
    if isinstance(string, str):
        # Convert unicode strings to bytes using UTF-8
        string = string.encode("utf-8")
    sha = hashlib.sha1()  # noqa: S324
    sha.update(string)
    return sha.hexdigest().encode("ascii")


def sha_file(file_obj):
    """Return the sha1 of a file."""
    sha = hashlib.sha1()  # noqa: S324
    while True:
        chunk = file_obj.read(65536)
        if not chunk:
            break
        sha.update(chunk)
    return sha.hexdigest().encode("ascii")


def dirname(path):
    """Return the directory part of a path."""
    if isinstance(path, bytes):
        return os.path.dirname(path)
    else:
        # For unicode strings, encode to UTF-8, get dirname, then decode
        encoded = path.encode("utf-8")
        result = os.path.dirname(encoded)
        return result.decode("utf-8")


def basename(path):
    """Return the basename part of a path."""
    if isinstance(path, bytes):
        return os.path.basename(path)
    else:
        # For unicode strings, encode to UTF-8, get basename, then decode
        encoded = path.encode("utf-8")
        result = os.path.basename(encoded)
        return result.decode("utf-8")


def chunks_to_lines_iter(chunks_iter):
    """Convert an iterator of chunks to an iterator of lines."""
    buffer = b""
    for chunk in chunks_iter:
        buffer += chunk
        while b"\n" in buffer:
            line, buffer = buffer.split(b"\n", 1)
            yield line + b"\n"

    # Yield any remaining data as the last line (without newline)
    if buffer:
        yield buffer


def file_iterator(file_obj, chunk_size=65536):
    """Iterate over the contents of a file in chunks."""
    while True:
        chunk = file_obj.read(chunk_size)
        if not chunk:
            break
        yield chunk


def normalizes_filenames():
    """Check if the filesystem normalizes filenames (e.g. Mac OS X)."""
    from . import _osutils_rs

    return _osutils_rs.normalizes_filenames()


def rand_chars(length):
    """Generate a string of random characters."""
    from . import _osutils_rs

    return _osutils_rs.rand_chars(length)


class DirReader:
    """An interface for reading directories."""

    def top_prefix_to_starting_dir(self, top, prefix=""):
        """Converts top and prefix to a starting dir entry.

        :param top: A utf8 path
        :param prefix: An optional utf8 path to prefix output relative paths
            with.
        :return: A tuple starting with prefix, and ending with the native
            encoding of top.
        """
        raise NotImplementedError(self.top_prefix_to_starting_dir)

    def read_dir(self, prefix, top):
        """Read a specific dir.

        :param prefix: A utf8 prefix to be preprended to the path basenames.
        :param top: A natively encoded path to read.
        :return: A list of the directories contents. Each item contains:
            (utf8_relpath, utf8_name, kind, lstatvalue, native_abspath)
        """
        raise NotImplementedError(self.read_dir)


_selected_dir_reader = None


def safe_unicode(unicode_or_utf8_string):
    """Coerce unicode_or_utf8_string into unicode.

    If it is unicode, it is returned.
    Otherwise it is decoded from utf-8. If decoding fails, the exception is
    wrapped in a TypeError exception.
    """
    if isinstance(unicode_or_utf8_string, (str, os.PathLike)):
        return unicode_or_utf8_string
    try:
        return unicode_or_utf8_string.decode("utf8")
    except UnicodeDecodeError as e:
        raise TypeError(unicode_or_utf8_string) from e


def safe_utf8(unicode_or_utf8_string):
    """Coerce unicode_or_utf8_string to a utf8 string.

    If it is a str, it is returned.
    If it is Unicode, it is encoded into a utf-8 string.
    """
    if isinstance(unicode_or_utf8_string, bytes):
        # Make sure it is a valid utf-8 string
        try:
            unicode_or_utf8_string.decode("utf-8")
        except UnicodeDecodeError as e:
            raise TypeError(unicode_or_utf8_string) from e
        return unicode_or_utf8_string
    return unicode_or_utf8_string.encode("utf-8")


def _walkdirs_utf8(top, prefix="", fs_enc=None):
    """Yield data about all the directories in a tree.

    This yields the same information as walkdirs() only each entry is yielded
    in utf-8. On platforms which have a filesystem encoding of utf8 the paths
    are returned as exact byte-strings.

    :return: yields a tuple of (dir_info, [file_info])
        dir_info is (utf8_relpath, path-from-top)
        file_info is (utf8_relpath, utf8_name, kind, lstat, path-from-top)
        if top is an absolute path, path-from-top is also an absolute path.
        path-from-top might be unicode or utf8, but it is the correct path to
        pass to os functions to affect the file in question. (such as os.lstat)
    """
    global _selected_dir_reader
    if _selected_dir_reader is None:
        if fs_enc is None:
            fs_enc = sys.getfilesystemencoding()
        # Always use the python version for bzrformats
        _selected_dir_reader = UnicodeDirReader()

    # 0 - relpath, 1- basename, 2- kind, 3- stat, 4-toppath
    # But we don't actually uses 1-3 in pending, so set them to None
    pending = [[_selected_dir_reader.top_prefix_to_starting_dir(top, prefix)]]
    read_dir = _selected_dir_reader.read_dir
    _directory = "directory"
    while pending:
        relroot, _, _, _, top = pending[-1].pop()
        if not pending[-1]:
            pending.pop()
        dirblock = sorted(read_dir(relroot, top))
        yield (relroot, top), dirblock
        # push the user specified dirs from dirblock
        next = [d for d in reversed(dirblock) if d[2] == _directory]
        if next:
            pending.append(next)


class UnicodeDirReader(DirReader):
    """A dir reader for non-utf8 file systems, which transcodes."""

    __slots__ = ["_utf8_encode"]

    def __init__(self):
        """Initialize the UTF-8 directory reader."""
        import codecs

        self._utf8_encode = codecs.getencoder("utf8")

    def top_prefix_to_starting_dir(self, top, prefix=""):
        """See DirReader.top_prefix_to_starting_dir."""
        return (safe_utf8(prefix), None, None, None, safe_unicode(top))

    def read_dir(self, prefix, top):
        """Read a single directory from a non-utf8 file system.

        top, and the abspath element in the output are unicode, all other paths
        are utf8. Local disk IO is done via unicode calls to listdir etc.

        This is currently the fallback code path when the filesystem encoding is
        not UTF-8. It may be better to implement an alternative so that we can
        safely handle paths that are not properly decodable in the current
        encoding.

        See DirReader.read_dir for details.
        """
        _utf8_encode = self._utf8_encode

        relprefix = prefix + b"/" if prefix else b""
        top_slash = top + "/"

        dirblock = []
        append = dirblock.append
        for entry in os.scandir(top):
            name = os.fsdecode(entry.name)
            abspath = top_slash + name
            name_utf8 = _utf8_encode(name, "surrogateescape")[0]
            statvalue = entry.stat(follow_symlinks=False)
            kind = file_kind_from_stat_mode(statvalue.st_mode)
            append((relprefix + name_utf8, name_utf8, kind, statvalue, abspath))
        return sorted(dirblock)


def is_inside(dir, fname):
    """Check if fname is inside dir.

    The empty string as dir is considered to contain everything.
    A path is considered to be inside itself.

    :param dir: Directory path (bytes or str)
    :param fname: File path to check (bytes or str)
    :return: True if fname is inside dir
    """
    # Normalize to use bytes for comparison
    if isinstance(dir, str):
        dir = dir.encode("utf-8")
    if isinstance(fname, str):
        fname = fname.encode("utf-8")

    if dir == fname:
        return True

    # Ensure trailing slash for proper comparison
    if dir != b"":
        dir = dir.rstrip(b"/") + b"/"

    return fname.startswith(dir)


def is_inside_any(dir_list, fname):
    """Check if fname is inside any of the directories in dir_list.

    :param dir_list: List of directory paths
    :param fname: File path to check
    :return: True if fname is inside any directory in dir_list
    """
    return any(is_inside(dir, fname) for dir in dir_list)


def parent_directories(filename):
    """Return a list of parent directories of filename.

    :param filename: Path (bytes or str)
    :return: List of parent directory paths
    """
    from . import _osutils_rs

    if isinstance(filename, bytes):
        filename = filename.decode("utf-8")
    return _osutils_rs.parent_directories(filename)


def split_lines(text):
    r"""Split text into lines, keeping line endings.

    Args:
        text: bytes to split

    Returns:
        List of byte strings, each ending with \\n where appropriate
    """
    from . import _osutils_rs

    return _osutils_rs.split_lines(text)


class IterableFile:
    """A file-like object backed by an iterator of byte strings.

    Supports ``read()`` and ``readline()`` over a lazy sequence of chunks.
    """

    def __init__(self, iterable):
        """Initialize with an iterable of byte chunks."""
        self._iter = iter(iterable)
        self._buf = b""

    def read(self, size=-1):
        """Read up to *size* bytes, or all remaining if *size* < 0."""
        if size < 0:
            return self._buf + b"".join(self._iter)
        while len(self._buf) < size:
            try:
                self._buf += next(self._iter)
            except StopIteration:
                break
        result = self._buf[:size]
        self._buf = self._buf[size:]
        return result

    def readline(self):
        r"""Read one line (up to and including ``\\n``)."""
        while b"\n" not in self._buf:
            try:
                self._buf += next(self._iter)
            except StopIteration:
                # Return whatever is left
                result = self._buf
                self._buf = b""
                return result
        idx = self._buf.index(b"\n") + 1
        result = self._buf[:idx]
        self._buf = self._buf[idx:]
        return result

    def readlines(self):
        """Return all remaining lines as a list."""
        lines = []
        while True:
            line = self.readline()
            if not line:
                break
            lines.append(line)
        return lines

    def __iter__(self):
        """Iterate over lines."""
        while True:
            line = self.readline()
            if not line:
                break
            yield line
