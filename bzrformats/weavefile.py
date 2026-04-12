# Copyright (C) 2005-2010 Canonical Ltd
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

"""Store and retrieve weaves in files.

There is one format marker followed by a blank line, followed by a
series of version headers, followed by the weave itself.

Each version marker has

 'i'   parent version indexes
 '1'   SHA-1 of text
 'n'   name

The inclusions do not need to list versions included by a parent.

The weave is bracketed by 'w' and 'W' lines, and includes the '{}[]'
processing instructions.  Lines of text are prefixed by '.' if the
line contains a newline, or ',' if not.
"""

from ._bzr_rs import weave as _weave_rs

FORMAT_1 = b"# bzr weave file v5\n"


def write_weave(weave, f, format=None):
    """Write a weave to a file.

    Args:
        weave: The weave object to write.
        f: File-like object to write to.
        format: The weave format version to use. Currently only supports None or 1.

    Raises:
        ValueError: If an unknown format is specified.

    Returns:
        The result of write_weave_v5 (None).
    """
    if format is None or format == 1:
        return write_weave_v5(weave, f)
    else:
        raise ValueError(f"unknown weave format {format!r}")


def write_weave_v5(weave, f):
    """Write weave to file f."""
    f.write(
        _weave_rs.write_weave_v5(
            weave._parents, weave._sha1s, weave._names, weave._weave
        )
    )


def read_weave(f):
    """Read a weave from a file.

    Args:
        f: File-like object to read from.

    Returns:
        A Weave object containing the data read from the file.
    """
    # FIXME: detect the weave type and dispatch
    from .weave import Weave

    w = Weave(getattr(f, "name", None))
    _read_weave_v5(f, w)
    return w


def _read_weave_v5(f, w):
    """Private helper routine to read a weave format 5 file into memory.

    This is only to be used by read_weave and WeaveFile.__init__.
    """
    try:
        data = f.read()
    finally:
        f.close()
    parents, sha1s, names, weave = _weave_rs.read_weave_v5(data)
    w._parents = parents
    w._sha1s = sha1s
    w._names = names
    w._weave = weave
    w._name_map = {name: i for i, name in enumerate(names)}
    return w
