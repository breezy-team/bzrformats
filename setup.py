#! /usr/bin/env python3

"""Installation script for bzrformats.

Run it with
 './setup.py install', or
 './setup.py --help' for more options.
"""

import os
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

from distutils.command.build_scripts import build_scripts

from setuptools import Command, setup

###############################
# Overridden distutils actions
###############################


class brz_build_scripts(build_scripts):
    """Custom build_scripts command that handles Rust extension binaries.

    This class extends the standard build_scripts command to properly handle
    Rust extension binaries by moving executable Rust extensions from the
    build_lib directory to the scripts directory.
    """

    def run(self):
        """Execute the build_scripts command and handle Rust executables.

        First runs the standard build_scripts process, then moves any Rust
        executable extensions from the build_lib directory to the scripts
        build directory.
        """
        build_scripts.run(self)

        self.run_command("build_ext")
        build_ext = self.get_finalized_command("build_ext")

        for ext in self.distribution.rust_extensions:
            if ext.binding == Binding.Exec:
                # GZ 2021-08-19: Not handling multiple binaries yet.
                os.replace(
                    os.path.join(build_ext.build_lib, ext.name),
                    os.path.join(self.build_dir, ext.name),
                )


class build_man(Command):
    """Custom command to generate the brz.1 manual page.

    This command builds the Breezy extension modules and then uses the
    generate_docs tool to create the brz.1 manual page from the built
    modules.
    """

    def initialize_options(self):
        """Initialize command options.

        No options to initialize for this command.
        """
        pass

    def finalize_options(self):
        """Finalize command options.

        No options to finalize for this command.
        """
        pass

    def run(self):
        """Execute the manual page generation.

        Builds the extension modules, adds the build directory to sys.path,
        and then imports and runs the generate_docs tool to create the
        brz.1 manual page.
        """
        build_ext_cmd = self.get_finalized_command("build_ext")
        build_lib_dir = build_ext_cmd.build_lib
        sys.path.insert(0, os.path.abspath(build_lib_dir))
        import importlib

        importlib.invalidate_caches()
        del sys.modules["breezy"]
        from tools import generate_docs

        generate_docs.main(["generate-docs", "man"])


########################
## Setup
########################

command_classes = {
    "build_man": build_man,
}

import site

site.ENABLE_USER_SITE = "--user" in sys.argv

rust_extensions = [
    RustExtension(
        "bzrformats._bzr_rs", "crates/bazaar-py/Cargo.toml", binding=Binding.PyO3
    ),
    RustExtension(
        "bzrformats._osutils_rs", "crates/osutils-py/Cargo.toml", binding=Binding.PyO3
    ),
]
entry_points = {}

# std setup
setup(
    cmdclass=command_classes,
    entry_points=entry_points,
    rust_extensions=rust_extensions,
)
