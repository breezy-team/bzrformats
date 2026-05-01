"""Test suite for bzrformats package."""

import atexit
import difflib
import logging
import os
import re
import shutil
import sys
import tempfile
import unittest

try:
    import testtools
except ImportError:
    # Minimal compatibility if testtools is not available
    testtools = None

from urllib.parse import quote as urlquote

from .. import osutils


def pathname2url(path):
    """Convert a local pathname to a URL path."""
    # On Unix, pathname2url is essentially identity with encoding of special chars
    # but preserving '/'
    return urlquote(path, safe="/:@")


logger = logging.getLogger("bzrformats.tests")

_unitialized_attr = object()
"""A sentinel needed to act as a default value in a method signature."""


def _rmtree_temp_dir(path, test_id=None):
    """Remove a temporary directory, handling errors."""
    try:
        shutil.rmtree(path)
    except OSError:
        if test_id:
            print(f"Failed to remove temp dir {path} for test {test_id}")
        pass


class TestCase(testtools.TestCase if testtools else unittest.TestCase):
    """Base class for bzrformats unit tests."""

    def __init__(self, methodName="testMethod"):  # noqa: N803
        super().__init__(methodName)
        self._cleanups = []

    def setUp(self):
        super().setUp()
        self._orig_cwd = os.getcwd()
        # Clear config to avoid external config affecting tests
        # Override HOME to prevent reading user configs
        import tempfile

        self._test_home_dir = tempfile.mkdtemp(prefix="brz-test-home-")
        self.addCleanup(__import__("shutil").rmtree, self._test_home_dir)
        self.overrideEnv("HOME", self._test_home_dir)
        self.overrideEnv("BRZ_HOME", self._test_home_dir)
        self.overrideEnv("EMAIL", "jrandom@example.com")
        self.overrideEnv("BRZ_EMAIL", None)

    def tearDown(self):
        try:
            # Run any registered cleanup functions
            while self._cleanups:
                func, args, kwargs = self._cleanups.pop()
                func(*args, **kwargs)
        finally:
            os.chdir(self._orig_cwd)
            super().tearDown()

    def addCleanup(self, func, *args, **kwargs):
        """Register a function to be called during tearDown."""
        self._cleanups.append((func, args, kwargs))

    def overrideAttr(self, obj, attr_name, new=_unitialized_attr):
        """Overrides an object attribute restoring it after the test."""
        # The actual value is captured by the call below
        value = getattr(obj, attr_name, _unitialized_attr)
        if value is _unitialized_attr:
            # When the test completes, the attribute should not exist, but if
            # we aren't setting a value, we don't need to do anything.
            if new is not _unitialized_attr:
                self.addCleanup(delattr, obj, attr_name)
        else:
            self.addCleanup(setattr, obj, attr_name, value)
        if new is not _unitialized_attr:
            setattr(obj, attr_name, new)
        return value

    def overrideEnv(self, name, new_value):
        """Override an environment variable, restoring it during tearDown."""
        old_value = os.environ.get(name)
        if new_value is None:
            if name in os.environ:
                del os.environ[name]
        else:
            os.environ[name] = new_value

        def restore():
            if old_value is None:
                if name in os.environ:
                    del os.environ[name]
            else:
                os.environ[name] = old_value

        self.addCleanup(restore)

    def assertEqualDiff(self, a, b, message=None):
        """Assert two texts are equal, if not raise an exception showing diffs."""
        if a == b:
            return
        if message is None:
            message = "texts not equal:\n"
        if a + "\n" == b:
            message = "first string is missing a final newline.\n"
        if a == b + "\n":
            message = "second string is missing a final newline.\n"

        # Create a diff
        diff = difflib.unified_diff(
            a.splitlines(True), b.splitlines(True), "expected", "actual"
        )
        raise AssertionError(message + "".join(diff))

    def assertContainsRe(self, haystack, needle_re, flags=0):
        """Assert that haystack contains something matching a regular expression."""
        if not re.search(needle_re, haystack, flags):
            raise AssertionError(f'pattern "{needle_re}" not found in "{haystack}"')

    def assertNotContainsRe(self, haystack, needle_re, flags=0):
        """Assert that haystack does not match a regular expression."""
        if re.search(needle_re, haystack, flags):
            raise AssertionError(f'pattern "{needle_re}" found in "{haystack}"')

    def assertStartsWith(self, s, prefix):
        if not s.startswith(prefix):
            raise AssertionError(f"string {s!r} does not start with {prefix!r}")

    def assertEndsWith(self, s, suffix):
        if not s.endswith(suffix):
            raise AssertionError(f"string {s!r} does not end with {suffix!r}")

    def assertLength(self, expected_length, obj_with_len):
        """Assert that obj_with_len is of length expected_length."""
        actual_length = len(obj_with_len)
        if actual_length != expected_length:
            self.fail(
                f"Incorrect length: wanted {expected_length}, got {actual_length} for {obj_with_len!r}"
            )

    def assertIs(self, left, right, message=None):
        """Assert that left is right."""
        if left is not right:
            if message is not None:
                raise AssertionError(message)
            else:
                raise AssertionError(f"{left!r} is not {right!r}.")

    def assertIsNot(self, left, right, message=None):
        """Assert that left is not right."""
        if left is right:
            if message is not None:
                raise AssertionError(message)
            else:
                raise AssertionError(f"{left!r} is {right!r}.")

    def assertIsInstance(self, obj, klass, msg=None):
        """Assert that obj is an instance of klass."""
        if not isinstance(obj, klass):
            if msg is None:
                msg = f"{obj!r} is not an instance of {klass}"
            raise AssertionError(msg)

    def log(self, *args):
        """Log a message."""
        logger.debug(*args)

    def assertSubset(self, sublist, superlist):
        """Assert that every entry in sublist is present in superlist."""
        missing = set(sublist) - set(superlist)
        if missing:
            raise AssertionError(
                f"Missing elements {missing!r}: {sublist!r} not a subset of {superlist!r}"
            )

    def knownFailure(self, reason):
        """Mark test as a known failure."""
        raise expectedFailure(reason)

    def requireFeature(self, feature):
        """This test requires a specific feature is available.

        :raises unittest.SkipTest: When feature is not available.
        """
        if not feature.available():
            self.skipTest(f"Feature {feature.feature_name()} not available")

    def assertPathExists(self, path):
        """Fail unless path or paths, which may be abs or relative, exist."""
        if not isinstance(path, (bytes, str)):
            for p in path:
                if not os.path.exists(p):
                    self.fail(f"path {p} does not exist")
        else:
            if not os.path.exists(path):
                self.fail(f"path {path} does not exist")

    def assertPathDoesNotExist(self, path):
        """Fail if path or paths, which may be abs or relative, exist."""
        if not isinstance(path, (bytes, str)):
            for p in path:
                if os.path.exists(p):
                    self.fail(f"path {p} exists")
        else:
            if os.path.exists(path):
                self.fail(f"path {path} exists")

    def assertFileEqual(self, content, path):
        """Fail if path does not contain 'content'."""
        self.assertPathExists(path)

        mode = "r" + ("b" if isinstance(content, bytes) else "")
        with open(path, mode) as f:
            s = f.read()
        self.assertEqualDiff(content, s)

    def assertListRaises(self, excClass, func, *args, **kwargs):  # noqa: N803
        """Fail unless excClass is raised when the iterator from func is used.

        Many functions can return generators this makes sure
        to wrap them in a list() call to make sure the whole generator
        is run, and that the proper exception is raised.
        """
        try:
            list(func(*args, **kwargs))
        except excClass as e:
            return e
        else:
            if getattr(excClass, "__name__", None) is not None:
                excName = excClass.__name__
            else:
                excName = str(excClass)
            raise self.failureException(f"{excName} not raised")

    def time(self, callable, *args, **kwargs):
        """Run callable and return result."""
        # Simplified version - just run the callable without profiling
        return callable(*args, **kwargs)


class TestCaseInTempDir(TestCase):
    """Test case that runs in a temporary directory.

    This is a minimal version of brz's TestCaseInTempDir.
    """

    TEST_ROOT = None

    def setUp(self):
        super().setUp()
        self._make_test_root()
        self.addCleanup(os.chdir, os.getcwd())
        self.makeAndChdirToTestDir()

    def _make_test_root(self):
        """Create the top-level test directory if needed."""
        if TestCaseInTempDir.TEST_ROOT is None:
            root = os.path.realpath(
                tempfile.mkdtemp(prefix="testbzrformats-", suffix=".tmp")
            )
            TestCaseInTempDir.TEST_ROOT = root
            atexit.register(_rmtree_temp_dir, root)

    def makeAndChdirToTestDir(self):
        """Create a temporary directory for this test and chdir to it."""
        # Create test directory name based on test id
        test_name = self.id()
        if sys.platform in ("win32", "cygwin"):
            test_name = re.sub('[<>*=+",:;_/\\-]', "_", test_name)
            test_name = test_name[-30:]  # Windows path length limits
        else:
            test_name = re.sub("[/]", "_", test_name)

        base_dir = os.path.join(TestCaseInTempDir.TEST_ROOT, test_name)

        # Find a unique directory name
        test_dir = base_dir
        for i in range(100):
            if not os.path.exists(test_dir):
                break
            test_dir = f"{base_dir}_{i}"
        else:
            raise RuntimeError(
                f"Could not create unique test directory for {test_name}"
            )

        os.makedirs(test_dir)
        self.test_dir = test_dir
        self.addCleanup(_rmtree_temp_dir, test_dir, test_id=self.id())
        os.chdir(test_dir)

    def build_tree(self, shape, line_endings="binary", transport=None):
        """Build a test tree according to a pattern.

        shape is a sequence of file specifications. If the final
        character is '/', a directory is created.
        """
        for name in shape:
            if isinstance(name, tuple):
                name, content = name
            else:
                content = None

            if name.endswith("/"):
                os.makedirs(name, exist_ok=True)
            else:
                dirname = os.path.dirname(name)
                if dirname:
                    os.makedirs(dirname, exist_ok=True)
                if content is None:
                    content = f"contents of {name}\n"
                if isinstance(content, str):
                    if line_endings == "native":
                        content = content.replace("\n", os.linesep)
                    content = content.encode("utf-8")
                with open(name, "wb") as f:
                    f.write(content)

    @staticmethod
    def build_tree_contents(shape):
        """Build test files with specific contents."""
        for entry in shape:
            if len(entry) == 2:
                name, content = entry
            else:
                name = entry[0]
                content = None
            if name.endswith("/"):
                os.makedirs(name, exist_ok=True)
            else:
                dirname = os.path.dirname(name)
                if dirname:
                    os.makedirs(dirname, exist_ok=True)
                if content is None:
                    content = b""
                if isinstance(content, str):
                    content = content.encode("utf-8")
                with open(name, "wb") as f:
                    f.write(content)


# Import TestSkipped from unittest
TestSkipped = unittest.SkipTest


class TestNotApplicable(TestSkipped):
    """Skip a test because it is not applicable to the current configuration."""

    pass


class TestCaseWithMemoryTransport(TestCase):
    """TestCase with a MemoryTransport for testing.

    Uses bzrformats' own MemoryTransport. Each test gets a fresh
    transport namespace based on the test ID.
    """

    def setUp(self):
        super().setUp()
        from ..transport import MemoryTransport

        self._memory_transport = MemoryTransport(url=f"memory:///{self.id()}/")

    def get_transport(self, relpath=None):
        """Get the transport for this test case."""
        if relpath is None or relpath == ".":
            return self._memory_transport
        t = self._memory_transport.clone(relpath)
        t.ensure_base()
        return t

    def get_url(self, relpath=None):
        """Get a URL for the memory transport."""
        if relpath is None or relpath == ".":
            return self._memory_transport.base
        return self._memory_transport.abspath(relpath)

    def check_file_contents(self, filename, expect):
        """Check contents of a file on the transport."""
        contents = self.get_transport().get_bytes(filename)
        if contents != expect:
            self.log(f"expected: {expect!r}")
            self.log(f"actually: {contents!r}")
            self.fail(f"contents of {filename} not as expected")


def load_tests(loader, basic_tests, pattern):
    """Load tests for bzrformats using the standard unittest discovery mechanism."""
    suite = loader.suiteClass()
    # Add the tests for this module
    suite.addTests(basic_tests)

    # List of test modules to load
    testmod_names = [
        "per_inventory",
        "per_versionedfile",
        "test__btree_serializer",
        "test__chk_map",
        "test__dirstate_helpers",
        "test__groupcompress",
        "test_btree_index",
        "test_chk_map",
        "test_chk_serializer",
        "test_chunk_writer",
        "test_dirstate",
        "test_generate_ids",
        "test_groupcompress",
        "test_hashcache",
        "test_index",
        "test_inv",
        "test_inventory_delta",
        "test_knit",
        "test_lock",
        "test_pack",
        "test_rio",
        "test_serializer",
        "test_tuned_gzip",
        "test_versionedfile",
        "test_weave",
        "test_xml",
    ]

    # Load each test module
    prefix = __name__ + "."
    for testmod_name in testmod_names:
        suite.addTest(loader.loadTestsFromName(prefix + testmod_name))

    # Also load per_* modules
    per_modules = [
        "per_versionedfile",
        "per_inventory",
    ]

    for per_module in per_modules:
        try:
            suite.addTest(loader.loadTestsFromName(prefix + per_module))
        except (ImportError, AttributeError):
            # Skip if module doesn't exist or has no tests
            pass

    return suite


def test_suite():
    """Return the test suite for bzrformats (for backwards compatibility)."""
    loader = unittest.TestLoader()
    basic_tests = loader.loadTestsFromModule(__import__(__name__, fromlist=[""]))
    return load_tests(loader, basic_tests, None)


def dir_reader_scenarios():
    """Simplified dir_reader_scenarios for bzrformats tests."""
    # Only use the unicode reader which is always available
    return [
        (
            "unicode",
            {
                "_dir_reader_class": osutils.UnicodeDirReader,
                "_native_to_unicode": lambda x: x,  # Already unicode
            },
        )
    ]
