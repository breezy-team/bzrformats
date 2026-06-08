# Copyright (C) 2005 Canonical Ltd
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

"""Standalone access to bazaar control directories.

``open(path)`` and ``create(path, format=...)`` return a :class:`BzrDir`, from
which :class:`Repository`, :class:`Branch` and :class:`WorkingTree` objects can
be obtained. ``format_names()`` lists the format names ``create`` accepts.
"""

from ._bzr_rs.controldir import (
    Branch,
    BzrDir,
    Repository,
    WorkingTree,
    create,
    create_shared_repository,
    format_names,
    open,
)

__all__ = [
    "Branch",
    "BzrDir",
    "Repository",
    "WorkingTree",
    "create",
    "create_shared_repository",
    "format_names",
    "open",
]
