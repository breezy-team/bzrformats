"""Core Bazaar format implementations and utilities.

This package contains the internal format implementations and utilities
that were extracted from breezy.bzr. These modules provide core serialization,
compression, and data structure functionality for Bazaar version control formats.
"""

# Same format as sys.version_info: "A tuple containing the five components of
# the version number: major, minor, micro, releaselevel, and serial. All
# values except releaselevel are integers; the release level is 'alpha',
# 'beta', 'candidate', or 'final'. The version_info value corresponding to the
# Python version 2.0 is (2, 0, 0, 'final', 0)."  Additionally we use a
# releaselevel of 'dev' for unreleased under-development code.

version_info = (3, 4, 2, "final", 0)


def _format_version_tuple(version_info):
    """Turn a version number 2, 3 or 5-tuple into a short string.

    This format matches <http://docs.python.org/dist/meta-data.html>
    and the typical presentation used in Python output.

    This also checks that the version is reasonable: the sub-release must be
    zero for final releases.

    >>> print(_format_version_tuple((1, 0, 0, 'final', 0)))
    1.0.0
    >>> print(_format_version_tuple((1, 2, 0, 'dev', 0)))
    1.2.0.dev
    >>> print(_format_version_tuple((1, 2, 0, 'dev', 1)))
    1.2.0.dev1
    >>> print(_format_version_tuple((1, 1, 1, 'candidate', 2)))
    1.1.1.rc2
    >>> print(_format_version_tuple((2, 1, 0, 'beta', 1)))
    2.1.b1
    >>> print(_format_version_tuple((1, 4, 0)))
    1.4.0
    >>> print(_format_version_tuple((1, 4)))
    1.4
    >>> print(_format_version_tuple((2, 1, 0, 'final', 42)))
    2.1.0.42
    >>> print(_format_version_tuple((1, 4, 0, 'wibble', 0)))
    1.4.0.wibble.0
    """
    if len(version_info) == 2:
        main_version = "%d.%d" % version_info[:2]
    else:
        main_version = "%d.%d.%d" % version_info[:3]
    if len(version_info) <= 3:
        return main_version

    release_type = version_info[3]
    sub = version_info[4]

    if release_type == "final" and sub == 0:
        sub_string = ""
    elif release_type == "final":
        sub_string = "." + str(sub)
    elif release_type == "dev" and sub == 0:
        sub_string = ".dev"
    elif release_type == "dev":
        sub_string = ".dev" + str(sub)
    elif release_type in ("alpha", "beta"):
        if version_info[2] == 0:
            main_version = "%d.%d" % version_info[:2]
        sub_string = "." + release_type[0] + str(sub)
    elif release_type == "candidate":
        sub_string = ".rc" + str(sub)
    else:
        return ".".join(map(str, version_info))

    return main_version + sub_string


__version__ = _format_version_tuple(version_info)
version_string = __version__
_core_version_string = ".".join(map(str, version_info[:3]))

__all__ = [
    "__version__",
    "version_info",
    "version_string",
]


def _alias_rust_submodules():
    """Expose selected ``_bzr_rs`` submodules under their ``bzrformats.*`` names.

    These modules used to be one-line Python shims that did
    ``from ._bzr_rs.foo import *``. Since the Rust submodule already exposes
    exactly the public API, we register it directly under ``bzrformats.foo``
    instead of keeping a stub file. Both ``import bzrformats.foo`` and
    ``from bzrformats.foo import X`` then resolve to the Rust submodule.
    """
    import sys

    from . import _bzr_rs

    # Public name -> attribute on _bzr_rs holding the module object. For the
    # common case the names match; aliases are listed explicitly.
    aliased = {
        "bisect_multi": "bisect_multi",
        "tuned_gzip": "tuned_gzip",
        "recordcounter": "recordcounter",
        "chunk_writer": "chunk_writer",
        "hashcache": "hashcache",
        "lock": "lock",
    }
    for public_name, rust_attr in aliased.items():
        submodule = getattr(_bzr_rs, rust_attr)
        sys.modules[f"{__name__}.{public_name}"] = submodule
        # Make `from bzrformats import foo` (package attribute access) work too.
        setattr(sys.modules[__name__], public_name, submodule)


_alias_rust_submodules()
