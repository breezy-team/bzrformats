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

from .errors import BzrFormatsError


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


from ._bzr_rs import pack_repo as _pack_repo_rs

_DirectPackAccess = _pack_repo_rs._DirectPackAccess
Pack = _pack_repo_rs.Pack
ExistingPack = _pack_repo_rs.ExistingPack
ResumedPack = _pack_repo_rs.ResumedPack
NewPack = _pack_repo_rs.NewPack
