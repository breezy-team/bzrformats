#! /usr/bin/env python3

"""Installation script for bzrformats.

Run it with
 './setup.py install', or
 './setup.py --help' for more options.
"""

import sys

try:
    import setuptools  # noqa: F401
except ModuleNotFoundError as e:
    sys.stderr.write(f"[ERROR] Please install setuptools ({e})\n")
    sys.exit(1)

try:
    from setuptools_rust import Binding, RustExtension
except ModuleNotFoundError as e:
    sys.stderr.write(f"[ERROR] Please install setuptools_rust ({e})\n")
    sys.exit(1)


import site

from setuptools import setup

site.ENABLE_USER_SITE = "--user" in sys.argv

rust_extensions = [
    RustExtension(
        "bzrformats._bzr_rs", "crates/bazaar-py/Cargo.toml", binding=Binding.PyO3
    ),
]
entry_points = {}

# std setup
setup(
    entry_points=entry_points,
    rust_extensions=rust_extensions,
)
