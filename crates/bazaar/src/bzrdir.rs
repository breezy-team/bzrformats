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
//! format is rejected loudly rather than mis-read. All the pack repository
//! formats are supported: 2a (groupcompress) and the knit-pack formats from
//! 0.92 through 1.14 (both GraphIndex- and B+Tree-indexed, with rich-root
//! and subtree variants), paired with Branch 6/7/8 and Working Tree 4/5/6.
//! The pre-pack knit and weave formats are recognised but not yet
//! supported, and are reported as [`BzrDirError::UnsupportedFormat`].

use crate::transport::{SharedTransport, Transport, TransportError};

/// Top-level marker in `.bzr/branch-format` for the meta directory layout.
pub const METADIR_MARKER: &[u8] = b"Bazaar-NG meta directory, format 1\n";

/// Supported repository format marker (`2a`).
pub const REPOSITORY_FORMAT_2A: &[u8] = b"Bazaar repository format 2a (needs bzr 1.16 or later)\n";

/// Supported branch format marker (Format 7).
pub const BRANCH_FORMAT_7: &[u8] = b"Bazaar Branch Format 7 (needs bzr 1.6)\n";

/// Supported working-tree format marker (Format 6).
pub const WORKINGTREE_FORMAT_6: &[u8] = b"Bazaar Working Tree Format 6 (bzr 1.14)\n";

/// The repository storage family a control-directory format creates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RepoKind {
    /// 2a (groupcompress + CHK).
    GroupCompress,
    /// A knit-pack repository (the marker selects the exact variant).
    KnitPack(&'static [u8]),
    /// A non-pack knit repository (the marker selects the exact variant).
    Knit(&'static [u8]),
}

/// A named control-directory format: the meta-directory layout plus the
/// markers for each component it creates. Mirrors an entry in breezy's
/// `controldir.format_registry` (e.g. "2a", "pack-0.92", "1.9").
#[derive(Debug, Clone)]
pub struct ControlDirFormat {
    /// The registry name (`brz init --format=<name>`).
    pub name: &'static str,
    repo: RepoKind,
    branch_marker: &'static [u8],
    wt_marker: &'static [u8],
    /// Whether the working tree writes a `views` file (formats 6+).
    wt_has_views: bool,
}

/// All control-directory formats this crate can create, keyed by name.
///
/// The list pairs each repository with the branch and working-tree formats
/// brz uses for that `--format` name, taken from breezy's format registry.
pub fn control_dir_formats() -> &'static [ControlDirFormat] {
    const B6: &[u8] = b"Bazaar Branch Format 6 (bzr 0.15)\n";
    const B7: &[u8] = BRANCH_FORMAT_7;
    const B5: &[u8] = b"Bazaar-NG branch format 5\n";
    const WT3: &[u8] = b"Bazaar-NG Working Tree format 3";
    const WT4: &[u8] = b"Bazaar Working Tree Format 4 (bzr 0.15)\n";
    const WT5: &[u8] = b"Bazaar Working Tree Format 5 (bzr 1.11)\n";
    const WT6: &[u8] = WORKINGTREE_FORMAT_6;
    use RepoKind::{GroupCompress, Knit, KnitPack};
    static FORMATS: &[ControlDirFormat] = &[
        ControlDirFormat {
            name: "2a",
            repo: GroupCompress,
            branch_marker: B7,
            wt_marker: WT6,
            wt_has_views: true,
        },
        ControlDirFormat {
            name: "pack-0.92",
            repo: KnitPack(b"Bazaar pack repository format 1 (needs bzr 0.92)\n"),
            branch_marker: B6,
            wt_marker: WT4,
            wt_has_views: false,
        },
        ControlDirFormat {
            name: "pack-0.92-subtree",
            repo: KnitPack(
                b"Bazaar pack repository format 1 with subtree support (needs bzr 0.92)\n",
            ),
            branch_marker: B6,
            wt_marker: WT4,
            wt_has_views: false,
        },
        ControlDirFormat {
            name: "rich-root-pack",
            repo: KnitPack(b"Bazaar pack repository format 1 with rich root (needs bzr 1.0)\n"),
            branch_marker: B6,
            wt_marker: WT4,
            wt_has_views: false,
        },
        ControlDirFormat {
            name: "1.6",
            repo: KnitPack(b"Bazaar RepositoryFormatKnitPack5 (bzr 1.6)\n"),
            branch_marker: B7,
            wt_marker: WT4,
            wt_has_views: false,
        },
        ControlDirFormat {
            name: "1.6.1-rich-root",
            repo: KnitPack(b"Bazaar RepositoryFormatKnitPack5RichRoot (bzr 1.6.1)\n"),
            branch_marker: B7,
            wt_marker: WT4,
            wt_has_views: false,
        },
        ControlDirFormat {
            name: "1.9",
            repo: KnitPack(b"Bazaar RepositoryFormatKnitPack6 (bzr 1.9)\n"),
            branch_marker: B7,
            wt_marker: WT4,
            wt_has_views: false,
        },
        ControlDirFormat {
            name: "1.9-rich-root",
            repo: KnitPack(b"Bazaar RepositoryFormatKnitPack6RichRoot (bzr 1.9)\n"),
            branch_marker: B7,
            wt_marker: WT4,
            wt_has_views: false,
        },
        ControlDirFormat {
            name: "1.14",
            repo: KnitPack(b"Bazaar RepositoryFormatKnitPack6 (bzr 1.9)\n"),
            branch_marker: B7,
            wt_marker: WT5,
            wt_has_views: false,
        },
        ControlDirFormat {
            name: "1.14-rich-root",
            repo: KnitPack(b"Bazaar RepositoryFormatKnitPack6RichRoot (bzr 1.9)\n"),
            branch_marker: B7,
            wt_marker: WT5,
            wt_has_views: false,
        },
        ControlDirFormat {
            name: "knit",
            repo: Knit(b"Bazaar-NG Knit Repository Format 1"),
            branch_marker: B5,
            wt_marker: WT3,
            wt_has_views: false,
        },
    ];
    FORMATS
}

/// Look up a control-directory format by its `brz init --format=` name.
pub fn find_control_dir_format(name: &str) -> Option<&'static ControlDirFormat> {
    control_dir_formats().iter().find(|f| f.name == name)
}

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
    /// Opening a component (repository/branch/working tree) failed.
    Component(String),
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
            BzrDirError::Component(m) => write!(f, "{m}"),
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

    /// Whether `marker` names a format of this component that this crate
    /// can open, consulting the per-component format registry.
    fn format_is_supported(self, marker: &[u8]) -> bool {
        match self {
            Component::Repository => crate::repository::find_format(marker)
                .map(|f| f.is_supported())
                .unwrap_or(false),
            Component::Branch => crate::branch::find_format(marker)
                .map(|f| f.is_supported())
                .unwrap_or(false),
            Component::WorkingTree => crate::workingtree::find_format(marker)
                .map(|f| f.is_supported())
                .unwrap_or(false),
        }
    }
}

/// An opened `.bzr` meta directory.
///
/// Owns the transport rooted *at* the `.bzr` directory (as a
/// [`SharedTransport`], consistent with the other opener objects) and
/// records which components are present and format-verified. The
/// `open_*` accessors descend into each component's subdirectory and
/// return owned objects that can outlive this `BzrDir`.
pub struct BzrDir {
    transport: SharedTransport,
    has_repository: bool,
    has_branch: bool,
    has_workingtree: bool,
}

impl BzrDir {
    /// Open the `.bzr` directory reachable through `transport`.
    ///
    /// `transport` must be rooted at the `.bzr` directory itself (i.e.
    /// `transport.get_bytes("branch-format")` reads `.bzr/branch-format`).
    /// To open from the directory that *contains* `.bzr`, descend with
    /// [`Transport::subtransport`] first.
    pub fn open(transport: SharedTransport) -> Result<Self, BzrDirError> {
        let marker = match transport.get_bytes("branch-format") {
            Ok(b) => b,
            Err(TransportError::NoSuchFile(_)) => return Err(BzrDirError::NotABzrDir),
            Err(e) => return Err(e.into()),
        };
        if marker != METADIR_MARKER {
            return Err(BzrDirError::NotMetaDir(marker));
        }

        let has_repository = Self::verify_component(transport.as_ref(), Component::Repository)?;
        let has_branch = Self::verify_component(transport.as_ref(), Component::Branch)?;
        let has_workingtree = Self::verify_component(transport.as_ref(), Component::WorkingTree)?;

        Ok(BzrDir {
            transport,
            has_repository,
            has_branch,
            has_workingtree,
        })
    }

    /// Create a fresh 2a control directory under `parent` (the directory
    /// that will contain `.bzr`), with an empty repository, branch and
    /// working tree, and open it.
    ///
    /// Writes the full meta-directory scaffold: the `.bzr` marker, each
    /// component's `format` file, an empty repository (`pack-names`), an
    /// empty branch (`null:` tip, empty tags/config), and an empty
    /// dirstate-based working tree.
    pub fn create(parent: &SharedTransport) -> Result<Self, BzrDirError> {
        Self::create_with_format(
            parent,
            find_control_dir_format("2a").expect("2a format is registered"),
        )
    }

    /// Create a fresh control directory in `format` under `parent`, with an
    /// empty repository, branch and working tree of the format's components,
    /// and open it. `format` names a `brz init --format=` combo (see
    /// [`control_dir_formats`]).
    pub fn create_with_format(
        parent: &SharedTransport,
        format: &ControlDirFormat,
    ) -> Result<Self, BzrDirError> {
        let bzr = parent.subtransport(".bzr")?;
        bzr.mkdir("")?;
        bzr.put_bytes("branch-format", METADIR_MARKER, None)?;
        bzr.put_bytes(
            "README",
            b"This is a Bazaar control directory.\n\
              Do not change any files in this directory.\n\
              See http://bazaar.canonical.com/ for more information about Bazaar.\n",
            None,
        )?;

        // Repository: empty store of the chosen storage family.
        let repo_t = bzr.subtransport("repository")?;
        match format.repo {
            RepoKind::GroupCompress => {
                crate::repository::Pack2aRepository::create(repo_t)
                    .map_err(|e| BzrDirError::Component(format!("creating repository: {e}")))?;
            }
            RepoKind::KnitPack(marker) => {
                let repo_format = crate::repository::find_format(marker).ok_or_else(|| {
                    BzrDirError::Component(format!(
                        "repository format not registered: {:?}",
                        String::from_utf8_lossy(marker)
                    ))
                })?;
                crate::repository::KnitPackRepository::create(repo_t, repo_format)
                    .map_err(|e| BzrDirError::Component(format!("creating repository: {e}")))?;
            }
            RepoKind::Knit(marker) => {
                let repo_format = crate::repository::find_format(marker).ok_or_else(|| {
                    BzrDirError::Component(format!(
                        "repository format not registered: {:?}",
                        String::from_utf8_lossy(marker)
                    ))
                })?;
                crate::repository::KnitRepository::create(repo_t, repo_format)
                    .map_err(|e| BzrDirError::Component(format!("creating repository: {e}")))?;
            }
        }

        // Branch: format marker, null tip, empty config and tags. Format 5
        // (full history) keeps the tip in revision-history rather than a
        // last-revision line, and writes a branch-name file.
        let branch = bzr.subtransport("branch")?;
        branch.mkdir("")?;
        branch.put_bytes("format", format.branch_marker, None)?;
        let branch_is_format5 = crate::branch::find_format(format.branch_marker)
            .map(|f| f.full_history)
            .unwrap_or(false);
        if branch_is_format5 {
            branch.put_bytes("revision-history", b"", None)?;
            branch.put_bytes("branch-name", b"", None)?;
        } else {
            branch.put_bytes("last-revision", b"0 null:\n", None)?;
        }
        branch.put_bytes("branch.conf", b"", None)?;
        branch.put_bytes("tags", b"", None)?;

        // Working tree: format marker and conflicts. A dirstate format (4/5/6)
        // writes an empty dirstate and (6+) a views file; the pre-dirstate
        // format 3 writes an empty working inventory and pending-merges.
        let checkout = bzr.subtransport("checkout")?;
        checkout.mkdir("")?;
        checkout.put_bytes("format", format.wt_marker, None)?;
        checkout.put_bytes("conflicts", b"BZR conflict list format 1\n", None)?;
        let wt_uses_dirstate = crate::workingtree::find_format(format.wt_marker)
            .map(|f| f.uses_dirstate)
            .unwrap_or(true);
        if wt_uses_dirstate {
            if format.wt_has_views {
                checkout.put_bytes("views", b"", None)?;
            }
            checkout.put_bytes("dirstate", &empty_dirstate_bytes(), None)?;
        } else {
            checkout.put_bytes("inventory", b"<inventory format=\"5\">\n</inventory>\n", None)?;
            checkout.put_bytes("pending-merges", b"", None)?;
        }

        Self::open(bzr)
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
                if component.format_is_supported(&found) {
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
    pub fn transport(&self) -> &SharedTransport {
        &self.transport
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

    /// Open the repository in this control directory.
    ///
    /// Errors with [`BzrDirError::NotABzrDir`] if there is no repository
    /// component (a branch- or checkout-only `.bzr`).
    pub fn open_repository(&self) -> Result<Box<dyn crate::repository::Repository>, BzrDirError> {
        if !self.has_repository {
            return Err(BzrDirError::NotABzrDir);
        }
        let sub = self
            .transport
            .subtransport(Component::Repository.subdir())?;
        crate::repository::open(sub)
            .map_err(|e| BzrDirError::Component(format!("opening repository: {e}")))
    }

    /// Open the branch in this control directory.
    ///
    /// Errors with [`BzrDirError::NotABzrDir`] if there is no branch
    /// component.
    pub fn open_branch(&self) -> Result<crate::branch::Branch, BzrDirError> {
        if !self.has_branch {
            return Err(BzrDirError::NotABzrDir);
        }
        let sub = self.transport.subtransport(Component::Branch.subdir())?;
        Ok(crate::branch::Branch::new(sub))
    }

    /// Open the working tree in this control directory.
    ///
    /// The working tree reads `.bzr/checkout/dirstate` and the files on
    /// disk, so it is rooted at the directory that *contains* `.bzr` (one
    /// level up from this `BzrDir`'s transport).
    ///
    /// Errors with [`BzrDirError::NotABzrDir`] if there is no working-tree
    /// component.
    pub fn open_workingtree(
        &self,
    ) -> Result<Box<dyn crate::workingtree::WorkingTree>, BzrDirError> {
        if !self.has_workingtree {
            return Err(BzrDirError::NotABzrDir);
        }
        let root = self.transport.subtransport("..")?;
        crate::workingtree::open(root)
            .map_err(|e| BzrDirError::Component(format!("opening working tree: {e}")))
    }
}

/// Serialise an empty dirstate (one root entry, no parents).
fn empty_dirstate_bytes() -> Vec<u8> {
    use crate::dirstate::{DefaultSHA1Provider, DirState};
    let mut state = DirState::new("dirstate", Box::new(DefaultSHA1Provider), 0, true, false);
    state.set_data(Vec::new(), DirState::empty_tree_dirblocks());
    state.get_lines().concat()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::LocalTransport;

    /// The supported on-disk marker for a component (used to write
    /// fixtures). These are the 2a / Branch 7 / Working Tree 6 markers.
    fn supported_marker(c: Component) -> &'static [u8] {
        match c {
            Component::Repository => REPOSITORY_FORMAT_2A,
            Component::Branch => BRANCH_FORMAT_7,
            Component::WorkingTree => WORKINGTREE_FORMAT_6,
        }
    }

    /// Build a minimal valid 2a meta-directory under `root/.bzr` and
    /// return a transport rooted at the `.bzr` directory.
    fn make_bzrdir(root: &std::path::Path, with: &[Component]) {
        let bzr = root.join(".bzr");
        std::fs::create_dir_all(&bzr).unwrap();
        std::fs::write(bzr.join("branch-format"), METADIR_MARKER).unwrap();
        for &c in with {
            let dir = bzr.join(c.subdir());
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join("format"), supported_marker(c)).unwrap();
        }
    }

    fn bzr_transport(root: &std::path::Path) -> SharedTransport {
        std::sync::Arc::new(LocalTransport::new(root.join(".bzr")))
    }

    #[test]
    fn format_registries_are_separate() {
        // A component's marker must only resolve in that component's
        // registry, never another's.
        let repo = REPOSITORY_FORMAT_2A;
        let branch = BRANCH_FORMAT_7;
        let wt = WORKINGTREE_FORMAT_6;
        assert!(crate::repository::find_format(repo).is_some());
        assert!(crate::repository::find_format(branch).is_none());
        assert!(crate::repository::find_format(wt).is_none());
        assert!(crate::branch::find_format(branch).is_some());
        assert!(crate::branch::find_format(repo).is_none());
        assert!(crate::branch::find_format(wt).is_none());
        assert!(crate::workingtree::find_format(wt).is_some());
        assert!(crate::workingtree::find_format(repo).is_none());
        assert!(crate::workingtree::find_format(branch).is_none());
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
        let bd = BzrDir::open(t).unwrap();
        assert!(bd.has_repository());
        assert!(bd.has_branch());
        assert!(bd.has_workingtree());
    }

    #[test]
    fn opens_repository_only() {
        let dir = tempfile::tempdir().unwrap();
        make_bzrdir(dir.path(), &[Component::Repository]);
        let t = bzr_transport(dir.path());
        let bd = BzrDir::open(t).unwrap();
        assert!(bd.has_repository());
        assert!(!bd.has_branch());
        assert!(!bd.has_workingtree());
    }

    #[test]
    fn missing_dir_is_not_a_bzrdir() {
        let dir = tempfile::tempdir().unwrap();
        let t = bzr_transport(dir.path());
        match BzrDir::open(t) {
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
        match BzrDir::open(t) {
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
        match BzrDir::open(t) {
            Err(BzrDirError::UnsupportedFormat {
                component: Component::Repository,
                ..
            }) => {}
            other => panic!("expected UnsupportedFormat(Repository), got {other:?}"),
        }
    }
}
