# Copyright (C) 2026 Jelmer Vernooĳ <jelmer@jelmer.uk>
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

"""Tests for the bzrformats version metadata."""

import os

import bzrformats

from . import TestCase

PYPROJECT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(bzrformats.__file__)), "pyproject.toml"
)


class TestVersionInfo(TestCase):
    def test_matches_pyproject(self):
        try:
            import tomllib
        except ModuleNotFoundError:
            self.skipTest("tomllib requires Python 3.11")
        try:
            with open(PYPROJECT_PATH, "rb") as f:
                pyproject = tomllib.load(f)
        except FileNotFoundError:
            self.skipTest("not running from a source tree")
        self.assertEqual(pyproject["project"]["version"], bzrformats.__version__)
