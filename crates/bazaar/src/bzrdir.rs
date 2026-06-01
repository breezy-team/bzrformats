//! Opening `.bzr` control directories.
//!
//! A `.bzr` directory in the "meta directory" layout holds independent
//! components, each in its own subdirectory with a `format` marker file:
//!
//! ```text
//! .bzr/
//!   branch-format            # "Bazaar-NG meta directory, format 1\n"
//!   repository/format        # repository format marker
//!   branch/format            # branch format marker
//!   checkout/format          # working-tree format marker
//! ```
//!
//! Any of the `repository`, `branch` and `checkout` components may be
//! absent (a repository-only or branch-only control directory is valid).
//!
//! This is not a cross-VCS prober: it only ever opens `.bzr`, and the
//! only thing it "detects" is which bzr format string each present
//! component carries, so the right decoder is used and an unsupported
//! format is rejected loudly rather than mis-read. Only the modern 2a /
//! Branch 7 / Working Tree 6 formats are supported today; anything else
//! is reported as [`BzrDirError::UnsupportedFormat`].

use crate::transport::{Transport, TransportError};

/// Top-level marker in `.bzr/branch-format` for the meta directory layout.
pub const METADIR_MARKER: &[u8] = b"Bazaar-NG meta directory, format 1\n";

/// Supported repository format marker (`2a`).
pub const REPOSITORY_FORMAT_2A: &[u8] = b"Bazaar repository format 2a (needs bzr 1.16 or later)\n";

/// Supported branch format marker (Format 7).
pub const BRANCH_FORMAT_7: &[u8] = b"Bazaar Branch Format 7 (needs bzr 1.6)\n";

/// Supported working-tree format marker (Format 6).
pub const WORKINGTREE_FORMAT_6: &[u8] = b"Bazaar Working Tree Format 6 (bzr 1.14)\n";

/// Errors from opening a `.bzr` directory.
#[derive(Debug)]
pub enum BzrDirError {
    /// No `.bzr/branch-format` file was found at the given location.
    NotABzrDir,
    /// The control directory is not in the meta-directory layout (e.g. an
    /// old all-in-one format). The marker found is included.
    NotMetaDir(Vec<u8>),
    /// A present component is in a format this crate does not support.
    /// Carries which component and the marker that was found.
    UnsupportedFormat {
        /// The component whose format is unsupported.
        component: Component,
        /// The marker string read from the component's `format` file.
        found: Vec<u8>,
    },
    /// An underlying transport error.
    Transport(TransportError),
}

impl std::fmt::Display for BzrDirError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BzrDirError::NotABzrDir => write!(f, "not a .bzr control directory"),
            BzrDirError::NotMetaDir(m) => write!(
                f,
                "not a meta-directory .bzr (found marker {:?})",
                String::from_utf8_lossy(m)
            ),
            BzrDirError::UnsupportedFormat { component, found } => write!(
                f,
                "unsupported {} format: {:?}",
                component.as_str(),
                String::from_utf8_lossy(found)
            ),
            BzrDirError::Transport(e) => write!(f, "transport error: {e}"),
        }
    }
}

impl std::error::Error for BzrDirError {}

impl From<TransportError> for BzrDirError {
    fn from(e: TransportError) -> Self {
        BzrDirError::Transport(e)
    }
}

/// The independent components a meta-directory `.bzr` can contain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Component {
    /// The object store (`repository/`).
    Repository,
    /// The branch (`branch/`).
    Branch,
    /// The working-tree checkout (`checkout/`).
    WorkingTree,
}

impl Component {
    fn as_str(self) -> &'static str {
        match self {
            Component::Repository => "repository",
            Component::Branch => "branch",
            Component::WorkingTree => "working tree",
        }
    }

    /// Subdirectory name within `.bzr` for this component.
    fn subdir(self) -> &'static str {
        match self {
            Component::Repository => "repository",
            Component::Branch => "branch",
            Component::WorkingTree => "checkout",
        }
    }

    fn expected_marker(self) -> &'static [u8] {
        match self {
            Component::Repository => REPOSITORY_FORMAT_2A,
            Component::Branch => BRANCH_FORMAT_7,
            Component::WorkingTree => WORKINGTREE_FORMAT_6,
        }
    }
}

/// An opened `.bzr` meta directory.
///
/// Holds the transport rooted *at* the `.bzr` directory and records
/// which components are present (and format-verified). The
/// `open_repository` / `open_branch` / `open_workingtree` accessors that
/// return live objects arrive with the repository/branch/working-tree
/// phases; for now this verifies the directory and exposes which
/// components exist.
pub struct BzrDir<'t> {
    transport: &'t dyn Transport,
    has_repository: bool,
    has_branch: bool,
    has_workingtree: bool,
}

impl<'t> BzrDir<'t> {
    /// Open the `.bzr` directory reachable through `transport`.
    ///
    /// `transport` must be rooted at the `.bzr` directory itself (i.e.
    /// `transport.get_bytes("branch-format")` reads `.bzr/branch-format`).
    /// To open from the directory that *contains* `.bzr`, root the
    /// transport one level down first (a `Transport`-level "clone into
    /// subdir" helper will make this ergonomic in a later phase).
    pub fn open(transport: &'t dyn Transport) -> Result<Self, BzrDirError> {
        let marker = match transport.get_bytes("branch-format") {
            Ok(b) => b,
            Err(TransportError::NoSuchFile(_)) => return Err(BzrDirError::NotABzrDir),
            Err(e) => return Err(e.into()),
        };
        if marker != METADIR_MARKER {
            return Err(BzrDirError::NotMetaDir(marker));
        }

        let has_repository = Self::verify_component(transport, Component::Repository)?;
        let has_branch = Self::verify_component(transport, Component::Branch)?;
        let has_workingtree = Self::verify_component(transport, Component::WorkingTree)?;

        Ok(BzrDir {
            transport,
            has_repository,
            has_branch,
            has_workingtree,
        })
    }

    /// Verify a component's format if it is present.
    ///
    /// Returns `Ok(true)` if the component exists and is a supported
    /// format, `Ok(false)` if the component is absent, and
    /// [`BzrDirError::UnsupportedFormat`] if it exists but carries an
    /// unrecognised marker.
    fn verify_component(
        transport: &dyn Transport,
        component: Component,
    ) -> Result<bool, BzrDirError> {
        let format_path = format!("{}/format", component.subdir());
        match transport.get_bytes(&format_path) {
            Ok(found) => {
                if found == component.expected_marker() {
                    Ok(true)
                } else {
                    Err(BzrDirError::UnsupportedFormat { component, found })
                }
            }
            Err(TransportError::NoSuchFile(_)) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// The transport rooted at the `.bzr` directory.
    pub fn transport(&self) -> &'t dyn Transport {
        self.transport
    }

    /// Whether this control directory contains a repository.
    pub fn has_repository(&self) -> bool {
        self.has_repository
    }

    /// Whether this control directory contains a branch.
    pub fn has_branch(&self) -> bool {
        self.has_branch
    }

    /// Whether this control directory contains a working-tree checkout.
    pub fn has_workingtree(&self) -> bool {
        self.has_workingtree
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::LocalTransport;

    /// Build a minimal valid 2a meta-directory under `root/.bzr` and
    /// return a transport rooted at the `.bzr` directory.
    fn make_bzrdir(root: &std::path::Path, with: &[Component]) {
        let bzr = root.join(".bzr");
        std::fs::create_dir_all(&bzr).unwrap();
        std::fs::write(bzr.join("branch-format"), METADIR_MARKER).unwrap();
        for &c in with {
            let dir = bzr.join(c.subdir());
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("format"), c.expected_marker()).unwrap();
        }
    }

    fn bzr_transport(root: &std::path::Path) -> LocalTransport {
        LocalTransport::new(root.join(".bzr"))
    }

    #[test]
    fn opens_full_metadir() {
        let dir = tempfile::tempdir().unwrap();
        make_bzrdir(
            dir.path(),
            &[
                Component::Repository,
                Component::Branch,
                Component::WorkingTree,
            ],
        );
        let t = bzr_transport(dir.path());
        let bd = BzrDir::open(&t).unwrap();
        assert!(bd.has_repository());
        assert!(bd.has_branch());
        assert!(bd.has_workingtree());
    }

    #[test]
    fn opens_repository_only() {
        let dir = tempfile::tempdir().unwrap();
        make_bzrdir(dir.path(), &[Component::Repository]);
        let t = bzr_transport(dir.path());
        let bd = BzrDir::open(&t).unwrap();
        assert!(bd.has_repository());
        assert!(!bd.has_branch());
        assert!(!bd.has_workingtree());
    }

    #[test]
    fn missing_dir_is_not_a_bzrdir() {
        let dir = tempfile::tempdir().unwrap();
        let t = bzr_transport(dir.path());
        match BzrDir::open(&t) {
            Err(BzrDirError::NotABzrDir) => {}
            other => panic!("expected NotABzrDir, got {other:?}"),
        }
    }

    #[test]
    fn non_metadir_marker_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let bzr = dir.path().join(".bzr");
        std::fs::create_dir_all(&bzr).unwrap();
        std::fs::write(bzr.join("branch-format"), b"Bazaar-NG branch, format 6\n").unwrap();
        let t = bzr_transport(dir.path());
        match BzrDir::open(&t) {
            Err(BzrDirError::NotMetaDir(_)) => {}
            other => panic!("expected NotMetaDir, got {other:?}"),
        }
    }

    #[test]
    fn unsupported_repository_format_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let bzr = dir.path().join(".bzr");
        std::fs::create_dir_all(bzr.join("repository")).unwrap();
        std::fs::write(bzr.join("branch-format"), METADIR_MARKER).unwrap();
        std::fs::write(
            bzr.join("repository/format"),
            b"Bazaar pack repository format 1 (needs bzr 1.6)\n",
        )
        .unwrap();
        let t = bzr_transport(dir.path());
        match BzrDir::open(&t) {
            Err(BzrDirError::UnsupportedFormat {
                component: Component::Repository,
                ..
            }) => {}
            other => panic!("expected UnsupportedFormat(Repository), got {other:?}"),
        }
    }
}
