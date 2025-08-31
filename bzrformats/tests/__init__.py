"""Test suite for bzrformats package."""

import atexit
import difflib
import os
import re
import shutil
import stat
import sys
import tempfile
import unittest

try:
    import testtools
except ImportError:
    # Minimal compatibility if testtools is not available
    testtools = None

from breezy import osutils, urlutils
from breezy import transport as _mod_transport
from breezy.tests import treeshape

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
    """Base class for bzrformats unit tests.
    
    This is a minimal version of breezy.tests.TestCase that provides
    just the functionality needed for bzrformats tests.
    """

    def __init__(self, methodName='testMethod'):
        super().__init__(methodName)
        self._cleanups = []
        
    def setUp(self):
        super().setUp()
        self._orig_cwd = os.getcwd()
        # Clear config to avoid external config affecting tests
        from breezy import config
        self.overrideAttr(config, "_shared_stores", {})
        # Override HOME to prevent reading user configs
        import tempfile
        self._test_home_dir = tempfile.mkdtemp(prefix='brz-test-home-')
        self.addCleanup(__import__('shutil').rmtree, self._test_home_dir)
        self.overrideEnv('HOME', self._test_home_dir)
        self.overrideEnv('BRZ_HOME', self._test_home_dir)
        
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
        """Overrides an object attribute restoring it after the test (copied from breezy.tests)."""
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
        if a + '\n' == b:
            message = "first string is missing a final newline.\n"
        if a == b + '\n':
            message = "second string is missing a final newline.\n"
        
        # Create a diff
        diff = difflib.unified_diff(
            a.splitlines(True),
            b.splitlines(True),
            'expected',
            'actual'
        )
        raise AssertionError(message + ''.join(diff))
    
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
            raise AssertionError(f'string {s!r} does not start with {prefix!r}')
    
    def assertEndsWith(self, s, suffix):
        if not s.endswith(suffix):
            raise AssertionError(f'string {s!r} does not end with {suffix!r}')
            
    def assertLength(self, expected_length, obj_with_len):
        """Assert that obj_with_len is of length expected_length."""
        actual_length = len(obj_with_len)
        if actual_length != expected_length:
            self.fail(f'Incorrect length: wanted {expected_length}, got {actual_length} for {obj_with_len!r}')

    def assertIs(self, left, right, message=None):
        """Assert that left is right."""
        if left is not right:
            if message is not None:
                raise AssertionError(message)
            else:
                raise AssertionError(f'{left!r} is not {right!r}.')
                
    def assertIsNot(self, left, right, message=None):
        """Assert that left is not right."""
        if left is right:
            if message is not None:
                raise AssertionError(message)
            else:
                raise AssertionError(f'{left!r} is {right!r}.')

    def assertIsInstance(self, obj, klass, msg=None):
        """Assert that obj is an instance of klass."""
        if not isinstance(obj, klass):
            if msg is None:
                msg = f'{obj!r} is not an instance of {klass}'
            raise AssertionError(msg)
            
    # Standard assertion methods are inherited from unittest.TestCase
    
    def assertEqualDiff(self, expected, actual):
        """Assert that expected equals actual, showing a diff if they differ."""
        if expected != actual:
            # Create a simple diff
            expected_lines = expected.splitlines(keepends=True)
            actual_lines = actual.splitlines(keepends=True)
            diff = list(difflib.unified_diff(expected_lines, actual_lines,
                                           fromfile='expected', tofile='actual'))
            diff_text = ''.join(diff)
            raise AssertionError(f'Texts differ:\n{diff_text}')

    def assertContainsRe(self, text, pattern):
        """Assert that text contains a match for the regular expression pattern."""
        import re
        if not re.search(pattern, text):
            raise AssertionError(f'Pattern {pattern!r} not found in {text!r}')
    
    def assertStartsWith(self, s, prefix):
        """Assert that s starts with prefix."""
        if not s.startswith(prefix):
            raise AssertionError(f'{s!r} does not start with {prefix!r}')

    def assertEndsWith(self, s, suffix):
        """Assert that s ends with suffix."""
        if not s.endswith(suffix):
            raise AssertionError(f'{s!r} does not end with {suffix!r}')
    
    def log(self, *args):
        """Log a message (copied from breezy.tests)."""
        from breezy import trace
        trace.mutter(*args)
    
    def requireFeature(self, feature):
        """This test requires a specific feature is available (copied from breezy.tests).
        
        :raises UnavailableFeature: When feature is not available.
        """
        from breezy.tests import UnavailableFeature
        if not feature.available():
            raise UnavailableFeature(feature)
    
    def assertSubset(self, sublist, superlist):
        """Assert that every entry in sublist is present in superlist."""
        missing = set(sublist) - set(superlist)
        if missing:
            raise AssertionError(f"Missing elements {missing!r}: {sublist!r} not a subset of {superlist!r}")
    
    def knownFailure(self, reason):
        """Mark test as a known failure."""
        from breezy.tests import KnownFailure
        raise KnownFailure(reason)
    
    def assertPathExists(self, path):
        """Fail unless path or paths, which may be abs or relative, exist (copied from breezy.tests)."""
        if not isinstance(path, (bytes, str)):
            for p in path:
                if not os.path.exists(p):
                    self.fail(f'path {p} does not exist')
        else:
            if not os.path.exists(path):
                self.fail(f'path {path} does not exist')
    
    def assertFileEqual(self, content, path):
        """Fail if path does not contain 'content' (copied from breezy.tests)."""
        self.assertPathExists(path)
        
        mode = "r" + ("b" if isinstance(content, bytes) else "")
        with open(path, mode) as f:
            s = f.read()
        self.assertEqualDiff(content, s)
    
    def assertListRaises(self, excClass, func, *args, **kwargs):
        """Fail unless excClass is raised when the iterator from func is used (copied from breezy.tests).
        
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
        """Run callable and return result (simplified version of breezy.tests time method)."""
        # Simplified version - just run the callable without profiling
        return callable(*args, **kwargs)


class TestCaseInTempDir(TestCase):
    """Test case that runs in a temporary directory.
    
    This is a minimal version of breezy's TestCaseInTempDir.
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
            root = osutils.realpath(tempfile.mkdtemp(prefix='testbzrformats-', suffix='.tmp'))
            TestCaseInTempDir.TEST_ROOT = root
            atexit.register(_rmtree_temp_dir, root)
            
    def makeAndChdirToTestDir(self):
        """Create a temporary directory for this test and chdir to it."""
        # Create test directory name based on test id
        test_name = self.id()
        if sys.platform in ('win32', 'cygwin'):
            test_name = re.sub('[<>*=+",:;_/\\-]', '_', test_name)
            test_name = test_name[-30:]  # Windows path length limits
        else:
            test_name = re.sub('[/]', '_', test_name)
            
        base_dir = os.path.join(TestCaseInTempDir.TEST_ROOT, test_name)
        
        # Find a unique directory name
        test_dir = base_dir
        for i in range(100):
            if not os.path.exists(test_dir):
                break
            test_dir = f"{base_dir}_{i}"
        else:
            raise RuntimeError(f"Could not create unique test directory for {test_name}")
            
        os.makedirs(test_dir)
        self.test_dir = test_dir
        self.addCleanup(_rmtree_temp_dir, test_dir, test_id=self.id())
        os.chdir(test_dir)
    
    def build_tree(self, shape, line_endings='binary', transport=None):
        """Build a test tree according to a pattern.
        
        shape is a sequence of file specifications. If the final
        character is '/', a directory is created.
        """
        if transport is None:
            transport = _mod_transport.get_transport_from_path('.')
            
        for name in shape:
            if isinstance(name, tuple):
                name, content = name
            else:
                content = None
                
            if name.endswith('/'):
                transport.mkdir(urlutils.escape(name[:-1]))
            else:
                if content is None:
                    content = f"contents of {name}\n"
                if isinstance(content, str):
                    if line_endings == 'native':
                        content = content.replace('\n', os.linesep)
                    content = content.encode('utf-8')
                transport.put_bytes_non_atomic(urlutils.escape(name), content)
    
    build_tree_contents = staticmethod(treeshape.build_tree_contents)


class TestCaseWithTransport(TestCaseInTempDir):
    """Test case that provides transport access.
    
    This is a minimal version of breezy's TestCaseWithTransport.
    """
    
    def __init__(self, methodName='runTest'):
        super().__init__(methodName)
        # Set up transport factory like breezy's implementation
        from breezy.tests import test_server
        self.vfs_transport_factory = test_server.LocalURLServer
        self.transport_server = None
        self.transport_readonly_server = None
        self.__vfs_server = None
    
    def setUp(self):
        super().setUp()
        self._transport = None
        
    def get_vfs_only_server(self):
        """Get the VFS server for this test."""
        if self.__vfs_server is None:
            self.__vfs_server = self.vfs_transport_factory()
            self.start_server(self.__vfs_server)
        return self.__vfs_server

    def start_server(self, transport_server, backing_server=None):
        """Start transport_server for this test."""
        if backing_server is None:
            transport_server.start_server()
        else:
            transport_server.start_server(backing_server)
        self.addCleanup(transport_server.stop_server)
        
    def get_transport(self, relpath=None):
        """Return a writeable transport.

        This transport is for the test scratch space relative to
        "self._test_root"

        :param relpath: a path relative to the base url.
        """
        t = _mod_transport.get_transport_from_url(self.get_url(relpath))
        self.assertFalse(t.is_readonly())
        return t
        
    def get_url(self, relpath=None):
        """Get a URL for the test directory."""
        base = urlutils.local_path_to_url(self.test_dir) + '/'
        if relpath:
            base += urlutils.escape(relpath)
        return base
        
    def assertIsDirectory(self, relpath, transport=None):
        """Assert that relpath is a directory."""
        if transport is None:
            transport = self.get_transport()
        try:
            mode = transport.stat(relpath).st_mode
        except _mod_transport.NoSuchFile:
            self.fail(f'path {relpath} is not a directory; no such file')
        if not stat.S_ISDIR(mode):
            self.fail(f'path {relpath} is not a directory; has mode {mode:#o}')
    
    def make_branch_builder(self, relpath, format=None):
        """Create a branch builder (copied from breezy.tests)."""
        branch = self.make_branch(relpath, format=format)
        from breezy.tests import branchbuilder
        return branchbuilder.BranchBuilder(branch=branch)
    
    def make_branch(self, relpath, format=None, name=None):
        """Create a branch on the transport at relpath (copied from breezy.tests)."""
        repo = self.make_repository(relpath, format=format)
        return repo.controldir.create_branch(append_revisions_only=False, name=name)
    
    def make_repository(self, relpath, shared=None, format=None):
        """Create a repository on our default transport at relpath (copied from breezy.tests)."""
        made_control = self.make_controldir(relpath, format=format)
        return made_control.create_repository(shared=shared)
    
    def make_controldir(self, relpath, format=None):
        """Create a controldir on our default transport at relpath (copied from breezy.tests)."""
        try:
            # might be a relative or absolute path
            maybe_a_url = self.get_url(relpath)
            segments = maybe_a_url.rsplit("/", 1)
            t = _mod_transport.get_transport(maybe_a_url)
            if len(segments) > 1 and segments[-1] not in ("", "."):
                t.ensure_base()
            format = self.resolve_format(format)
            return format.initialize_on_transport(t)
        except errors.UninitializableFormat as err:
            raise TestSkipped(f"Format {format} is not initializable.") from err
    
    def resolve_format(self, format):
        """Resolve an object to a ControlDir format object (copied from breezy.tests)."""
        if format is None:
            format = self.get_default_format()
        if isinstance(format, str):
            from breezy import controldir
            format = controldir.format_registry.make_controldir(format)
        return format
    
    def get_default_format(self):
        """Get the default format (copied from breezy.tests)."""
        return "default"
    
    def make_branch_and_tree(self, relpath, format=None):
        """Create a branch on the transport and a tree locally (copied from breezy.tests)."""
        from breezy import errors
        
        format = self.resolve_format(format=format)
        if not format.supports_workingtrees:
            b = self.make_branch(relpath + ".branch", format=format)
            return b.create_checkout(relpath, lightweight=True)
        b = self.make_branch(relpath, format=format)
        try:
            return b.controldir.create_workingtree()
        except errors.NotLocalUrl:
            # We can only make working trees locally at the moment.  If the
            # transport can't support them, then we keep the non-disk-backed
            # branch and create a local checkout.
            return b.create_checkout(relpath, lightweight=True)
    
    def make_branch_and_memory_tree(self, relpath, format=None):
        """Create a branch on the default transport and a MemoryTree for it (copied from breezy.tests)."""
        b = self.make_branch(relpath, format=format)
        return b.create_memorytree()
    
    def check_file_contents(self, filename, expect):
        """Check contents of a file (copied from breezy.tests)."""
        self.log(f"check contents of file {filename}")
        with open(filename, "rb") as f:
            contents = f.read()
        if contents != expect:
            self.log(f"expected: {expect!r}")
            self.log(f"actually: {contents!r}")
            self.fail(f"contents of {filename} not as expected")


# Import TestSkipped from unittest
TestSkipped = unittest.SkipTest

# Import features from breezy.tests for compatibility
from breezy.tests import features
# Import testscenarios for scenario testing
import testscenarios as scenarios
# Import transport for compatibility
from breezy import transport


class TestNotApplicable(TestSkipped):
    """Skip a test because it is not applicable to the current configuration."""
    pass


class TestCaseWithMemoryTransport(TestCase):
    """TestCase with a MemoryTransport for testing.
    
    This is a minimal version for bzrformats tests.
    """
    
    TEST_ROOT = None
    
    def __init__(self, methodName='runTest'):
        super().__init__(methodName)
        # Set up transport factory like breezy's implementation
        from breezy.transport import memory
        self.vfs_transport_factory = memory.MemoryServer
        self.transport_server = None
        self.transport_readonly_server = None
        self.__vfs_server = None
        self.__server = None
    
    def setUp(self):
        super().setUp()
        self._make_test_root()
        
    def _make_test_root(self):
        if TestCaseWithMemoryTransport.TEST_ROOT is None:
            # For memory transport, we don't need a real directory
            TestCaseWithMemoryTransport.TEST_ROOT = "memory:///"
            
    def get_vfs_only_server(self):
        """Get the VFS server for this test."""
        if self.__vfs_server is None:
            from breezy.transport import memory
            self.__vfs_server = memory.MemoryServer()
            self.start_server(self.__vfs_server)
        return self.__vfs_server

    def get_server(self):
        """Get the read/write server instance."""
        if self.__server is None:
            if self.transport_server is None or self.transport_server is self.vfs_transport_factory:
                self.__server = self.get_vfs_only_server()
            else:
                # bring up a decorated means of access to the vfs only server.
                self.__server = self.transport_server()
                self.start_server(self.__server, self.get_vfs_only_server())
        return self.__server

    def start_server(self, transport_server, backing_server=None):
        """Start transport_server for this test."""
        if backing_server is None:
            transport_server.start_server()
        else:
            transport_server.start_server(backing_server)
        self.addCleanup(transport_server.stop_server)
        
    def get_transport(self, relpath=None):
        """Get the transport for this test case."""
        t = _mod_transport.get_transport_from_url(self.get_url(relpath))
        self.assertFalse(t.is_readonly())
        return t
        
    def get_url(self, relpath=None):
        """Get a URL for the memory transport."""
        base = self.get_server().get_url()
        return self._adjust_url(base, relpath)
        
    def _adjust_url(self, base, relpath):
        """Get a URL (or maybe a path) with relpath appended."""
        if relpath is not None and relpath != ".":
            if not base.endswith("/"):
                base = base + "/"
            # XXX: Really base should be a url; we did after all call
            # get_url()!  But sometimes it's just a path (from
            # LocalAbspathServer), and it'd be wrong to append urlescaped data
            # to a non-escaped local path.
            if base.startswith("./") or base.startswith("/"):
                base += relpath
            else:
                base += urlutils.escape(relpath)
        return base
    
    def make_branch_builder(self, relpath, format=None):
        """Create a branch builder (copied from breezy.tests)."""
        branch = self.make_branch(relpath, format=format)
        from breezy.tests import branchbuilder
        return branchbuilder.BranchBuilder(branch=branch)
    
    def make_branch(self, relpath, format=None, name=None):
        """Create a branch on the transport at relpath (copied from breezy.tests)."""
        repo = self.make_repository(relpath, format=format)
        return repo.controldir.create_branch(append_revisions_only=False, name=name)
    
    def make_repository(self, relpath, shared=None, format=None):
        """Create a repository on our default transport at relpath (copied from breezy.tests)."""
        made_control = self.make_controldir(relpath, format=format)
        return made_control.create_repository(shared=shared)
    
    def make_controldir(self, relpath, format=None):
        """Create a controldir on our default transport at relpath (copied from breezy.tests)."""
        try:
            # might be a relative or absolute path
            maybe_a_url = self.get_url(relpath)
            segments = maybe_a_url.rsplit("/", 1)
            t = _mod_transport.get_transport(maybe_a_url)
            if len(segments) > 1 and segments[-1] not in ("", "."):
                t.ensure_base()
            format = self.resolve_format(format)
            return format.initialize_on_transport(t)
        except errors.UninitializableFormat as err:
            raise TestSkipped(f"Format {format} is not initializable.") from err
    
    def resolve_format(self, format):
        """Resolve an object to a ControlDir format object (copied from breezy.tests)."""
        if format is None:
            format = self.get_default_format()
        if isinstance(format, str):
            from breezy import controldir
            format = controldir.format_registry.make_controldir(format)
        return format
    
    def get_default_format(self):
        """Get the default format (copied from breezy.tests)."""
        return "default"


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
    basic_tests = loader.loadTestsFromModule(__import__(__name__, fromlist=['']))
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
