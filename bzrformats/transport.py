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

"""Minimal transport for bzrformats.

Provides a Transport protocol and a simple in-memory implementation.
"""

import posixpath
from io import BytesIO
from typing import Protocol, runtime_checkable
from urllib.parse import unquote

from .errors import PathError


class NoSuchFile(PathError):
    """A file or directory does not exist."""

    _fmt = "No such file: %(path)r%(extra)s"


# Tuple for catching NoSuchFile from both bzrformats and breezy transports.
# Use this in except clauses when the transport may be either implementation.
try:
    from breezy.transport import NoSuchFile as _BreezyNoSuchFile
    TransportNoSuchFile = (NoSuchFile, _BreezyNoSuchFile)
except ImportError:
    TransportNoSuchFile = NoSuchFile


class FileExists(PathError):
    """A file or directory already exists."""

    _fmt = "File exists: %(path)r%(extra)s"


@runtime_checkable
class Transport(Protocol):
    """Minimal transport protocol for bzrformats."""

    base: str

    def get(self, relpath: str):
        """Get a file-like object for reading."""
        ...

    def get_bytes(self, relpath: str) -> bytes:
        """Get the raw bytes of a file."""
        ...

    def put_bytes(self, relpath: str, raw_bytes: bytes, mode=None):
        """Atomically put bytes at a location."""
        ...

    def put_file(self, relpath: str, f, mode=None) -> int:
        """Write a file from a file-like object, returning bytes written."""
        ...

    def put_file_non_atomic(self, relpath: str, f, mode=None,
                            create_parent_dir=False):
        """Put a file-like object at a location."""
        ...

    def append_bytes(self, relpath: str, raw_bytes: bytes, mode=None) -> int:
        """Append bytes to a file, returning the byte offset of the start."""
        ...

    def readv(self, relpath: str, offsets):
        """Get parts of a file.

        :param offsets: List of (offset, size) tuples.
        :yields: (offset, data) tuples.
        """
        ...

    def open_write_stream(self, relpath: str, mode=None):
        """Open a writable stream at relpath."""
        ...

    def mkdir(self, relpath: str, mode=None):
        """Create a directory."""
        ...

    def delete(self, relpath: str):
        """Delete a file."""
        ...

    def move(self, rel_from: str, rel_to: str):
        """Move (rename) a file."""
        ...

    def stat(self, relpath: str):
        """Return a stat-like object for a file."""
        ...

    def has(self, relpath: str) -> bool:
        """Return True if the path exists."""
        ...

    def abspath(self, relpath: str) -> str:
        """Return the full URL for the given relative path."""
        ...

    def clone(self, relpath: str = None):
        """Return a new transport pointing at a sub-directory."""
        ...

    def iter_files_recursive(self):
        """Iterate over all files below this transport, yielding relpaths."""
        ...

    def ensure_base(self):
        """Ensure the base directory exists."""
        ...

    def recommended_page_size(self) -> int:
        """Return the recommended number of bytes to read at once."""
        ...


class _MemoryStat:
    """Minimal stat result for MemoryTransport."""

    def __init__(self, size, is_dir=False):
        self.st_size = size
        if is_dir:
            self.st_mode = 0o40755
        else:
            self.st_mode = 0o100644


class _MemoryWriteStream:
    """A write stream that writes directly to the backing store.

    Data is visible to readers immediately after each ``write()``.
    """

    def __init__(self, files, path):
        self._files = files
        self._path = path
        self._files.setdefault(path, b"")

    def write(self, data):
        self._files[self._path] = self._files.get(self._path, b"") + data

    def close(self):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def _sort_expand_and_combine(offsets, upper_limit, page_size):
    """Sort, expand, and combine readv offsets to reduce round trips.

    Each range is expanded to at least *page_size* bytes (centered on the
    original range), then overlapping ranges are merged.
    """
    if not offsets:
        return []
    sorted_offsets = sorted(offsets)
    expanded = []
    for offset, length in sorted_offsets:
        expansion = max(0, page_size - length)
        reduction = expansion // 2
        new_offset = max(0, offset - reduction)
        new_length = length + expansion
        if upper_limit:
            new_end = min(upper_limit, new_offset + new_length)
            new_length = max(0, new_end - new_offset)
        if new_length > 0:
            expanded.append((new_offset, new_length))
    if not expanded:
        return []
    merged = [expanded[0]]
    for offset, length in expanded[1:]:
        prev_offset, prev_length = merged[-1]
        prev_end = prev_offset + prev_length
        end = offset + length
        if offset > prev_end:
            merged.append((offset, length))
        elif end > prev_end:
            merged[-1] = (prev_offset, end - prev_offset)
    return merged


class MemoryTransport:
    """Simple in-memory transport for testing.

    All MemoryTransport instances sharing the same ``_files`` and ``_dirs``
    dicts see the same data, so :meth:`clone` produces a view onto the same
    store.
    """

    def __init__(self, url="memory:///", _files=None, _dirs=None):
        if not url.endswith("/"):
            url += "/"
        self.base = url
        self._files = _files if _files is not None else {}
        self._dirs = _dirs if _dirs is not None else set()
        self._dirs.add("/")

    # -- internal helpers --

    def _abspath(self, relpath):
        """Resolve *relpath* to an absolute path within the store."""
        if relpath is None or relpath == ".":
            relpath = ""
        relpath = unquote(relpath)
        path = posixpath.join(self._path(), relpath)
        return posixpath.normpath(path)

    def _path(self):
        """Extract the path portion from the base URL."""
        path = self.base.split("://", 1)[-1]
        if path.endswith("/"):
            path = path[:-1]
        return path or "/"

    # -- Transport interface --

    def clone(self, relpath=None):
        """Return a new transport rooted at *relpath*."""
        if relpath is None:
            return MemoryTransport(self.base, self._files, self._dirs)
        return MemoryTransport(
            self.abspath(relpath), self._files, self._dirs
        )

    def abspath(self, relpath):
        """Return the full ``memory://`` URL for *relpath*."""
        return "memory://" + self._abspath(relpath)

    def has(self, relpath):
        """Return True if *relpath* exists as a file or directory."""
        path = self._abspath(relpath)
        return path in self._files or path in self._dirs

    def get(self, relpath):
        """Return a :class:`BytesIO` with the contents of *relpath*."""
        path = self._abspath(relpath)
        try:
            return BytesIO(self._files[path])
        except KeyError:
            raise NoSuchFile(relpath)

    def get_bytes(self, relpath):
        """Return the raw bytes of *relpath*."""
        path = self._abspath(relpath)
        try:
            return self._files[path]
        except KeyError:
            raise NoSuchFile(relpath)

    def put_bytes(self, relpath, raw_bytes, mode=None):
        """Store *raw_bytes* at *relpath*."""
        self._files[self._abspath(relpath)] = raw_bytes

    def put_file(self, relpath, f, mode=None):
        """Write *f* to *relpath*, returning the number of bytes written."""
        data = f.read()
        self._files[self._abspath(relpath)] = data
        return len(data)

    def put_file_non_atomic(self, relpath, f, mode=None,
                            create_parent_dir=False):
        """Write *f* to *relpath*, creating parent dirs if requested."""
        if create_parent_dir:
            self._ensure_parent(relpath)
        self._files[self._abspath(relpath)] = f.read()

    def append_bytes(self, relpath, raw_bytes, mode=None):
        """Append *raw_bytes* to *relpath*, returning the start offset."""
        path = self._abspath(relpath)
        existing = self._files.get(path, b"")
        pos = len(existing)
        self._files[path] = existing + raw_bytes
        return pos

    def readv(self, relpath, offsets, adjust_for_latency=False, upper_limit=0):
        """Yield ``(offset, data)`` for each ``(offset, length)`` in *offsets*."""
        file_data = self.get_bytes(relpath)
        offsets = list(offsets)
        if adjust_for_latency and offsets:
            offsets = _sort_expand_and_combine(
                offsets, upper_limit or len(file_data),
                self.recommended_page_size())
        for offset, length in offsets:
            yield offset, file_data[offset:offset + length]

    def open_write_stream(self, relpath, mode=None):
        """Return a writable stream; data is stored on close."""
        return _MemoryWriteStream(self._files, self._abspath(relpath))

    def mkdir(self, relpath, mode=None):
        """Create a directory at *relpath*.

        Does not raise if the directory already exists.
        """
        self._dirs.add(self._abspath(relpath))

    def delete(self, relpath):
        """Delete the file at *relpath*."""
        path = self._abspath(relpath)
        try:
            del self._files[path]
        except KeyError:
            raise NoSuchFile(relpath)

    def move(self, rel_from, rel_to):
        """Move (rename) a file from *rel_from* to *rel_to*."""
        path_from = self._abspath(rel_from)
        path_to = self._abspath(rel_to)
        try:
            self._files[path_to] = self._files.pop(path_from)
        except KeyError:
            raise NoSuchFile(rel_from)

    def stat(self, relpath):
        """Return a stat-like object for *relpath*."""
        path = self._abspath(relpath)
        if path in self._dirs:
            return _MemoryStat(0, is_dir=True)
        if path in self._files:
            return _MemoryStat(len(self._files[path]))
        raise NoSuchFile(relpath)

    def iter_files_recursive(self):
        """Yield relative paths of all files below this transport."""
        prefix = self._path().rstrip("/") + "/"
        for path in sorted(self._files):
            if path.startswith(prefix):
                yield path[len(prefix):]

    def ensure_base(self):
        """Ensure the base directory exists."""
        self._dirs.add(self._path())

    def recommended_page_size(self):
        """Return a reasonable read-ahead size."""
        return 4096

    def _ensure_parent(self, relpath):
        """Ensure the parent directory of *relpath* exists."""
        parent = posixpath.dirname(self._abspath(relpath))
        self._dirs.add(parent)

    def __repr__(self):
        return f"MemoryTransport({self.base!r})"


class TracingTransport:
    """Transport wrapper that records operations in ``_activity``.

    Wraps another transport and delegates all calls. Selected operations
    are recorded as tuples in ``_activity`` for test assertions.  The
    tuple format matches breezy's ``TransportTraceDecorator``.
    """

    def __init__(self, inner):
        self._inner = inner
        self._activity = []

    def __getattr__(self, name):
        """Delegate everything not explicitly overridden to the inner transport."""
        return getattr(self._inner, name)

    @property
    def base(self):
        return self._inner.base

    # -- traced methods (match breezy's TransportTraceDecorator format) --

    def get(self, relpath):
        self._activity.append(("get", relpath))
        return self._inner.get(relpath)

    def get_bytes(self, relpath):
        self._activity.append(("get", relpath))
        return self._inner.get_bytes(relpath)

    def put_bytes(self, relpath, raw_bytes, mode=None):
        self._activity.append(("put_bytes", relpath, len(raw_bytes), mode))
        return self._inner.put_bytes(relpath, raw_bytes, mode)

    def mkdir(self, relpath, mode=None):
        self._activity.append(("mkdir", relpath, mode))
        return self._inner.mkdir(relpath, mode)

    def readv(self, relpath, offsets, adjust_for_latency=False, upper_limit=None):
        self._activity.append(
            ("readv", relpath, list(offsets), adjust_for_latency, upper_limit))
        return self._inner.readv(
            relpath, offsets, adjust_for_latency=adjust_for_latency,
            upper_limit=upper_limit)

    # -- non-traced pass-through for common methods --

    def put_file(self, relpath, f, mode=None):
        return self._inner.put_file(relpath, f, mode)

    def clone(self, relpath=None):
        return TracingTransport(self._inner.clone(relpath))

    def recommended_page_size(self):
        return self._inner.recommended_page_size()

    def __repr__(self):
        return f"TracingTransport({self._inner!r})"
