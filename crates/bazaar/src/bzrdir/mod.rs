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
//! format is rejected loudly rather than mis-read. The supported formats
//! span the pack family -- 2a (groupcompress) and the knit-pack formats
//! from 0.92 through 1.14 (both GraphIndex- and B+Tree-indexed, with
//! rich-root and subtree variants), paired with Branch 6/7/8 and Working
//! Tree 4/5/6 -- the non-pack knit format (Branch 5, Working Tree 3), and
//! the all-in-one weave format ("Bazaar-NG branch, format 6"), which lives
//! directly under `.bzr` and is opened as a [`BzrDirAllInOne`] rather than
//! a meta-directory.

pub mod format;

pub use format::{control_dir_formats, find_control_dir_format, ControlDirFormat};

use crate::declare_bzrdir_format;
use crate::transport::{SharedTransport, Transport, TransportError};

/// Top-level marker in `.bzr/branch-format` for the meta directory layout.
pub const METADIR_MARKER: &[u8] = b"Bazaar-NG meta directory, format 1\n";

/// Supported repository format marker (`2a`).
pub const REPOSITORY_FORMAT_2A: &[u8] = b"Bazaar repository format 2a (needs bzr 1.16 or later)\n";

/// Supported branch format marker (Format 7).
pub const BRANCH_FORMAT_7: &[u8] = b"Bazaar Branch Format 7 (needs bzr 1.6)\n";

/// Supported working-tree format marker (Format 6).
pub const WORKINGTREE_FORMAT_6: &[u8] = b"Bazaar Working Tree Format 6 (bzr 1.14)\n";

// The `brz init --format=` combos this crate can create, each pairing a
// repository, branch and working-tree marker. A combo is gated behind the
// same feature as the older repository backend it creates, so it is only
// registered when that backend is built.
const B5: &[u8] = b"Bazaar-NG branch format 5\n";
const B6: &[u8] = b"Bazaar Branch Format 6 (bzr 0.15)\n";
const B7: &[u8] = BRANCH_FORMAT_7;
const WT3: &[u8] = b"Bazaar-NG Working Tree format 3";
const WT4: &[u8] = b"Bazaar Working Tree Format 4 (bzr 0.15)\n";
const WT5: &[u8] = b"Bazaar Working Tree Format 5 (bzr 1.11)\n";
const WT6: &[u8] = WORKINGTREE_FORMAT_6;

declare_bzrdir_format! {
    FORMAT_2A {
        name: "2a",
        repo_marker: REPOSITORY_FORMAT_2A,
        branch_marker: B7,
        wt_marker: WT6,
        wt_has_views: true,
    }
}

#[cfg(feature = "knitpack")]
declare_bzrdir_format! {
    FORMAT_PACK_0_92 {
        name: "pack-0.92",
        repo_marker: b"Bazaar pack repository format 1 (needs bzr 0.92)\n",
        branch_marker: B6,
        wt_marker: WT4,
    }
}

#[cfg(feature = "knitpack")]
declare_bzrdir_format! {
    FORMAT_PACK_0_92_SUBTREE {
        name: "pack-0.92-subtree",
        repo_marker: b"Bazaar pack repository format 1 with subtree support (needs bzr 0.92)\n",
        branch_marker: B6,
        wt_marker: WT4,
    }
}

#[cfg(feature = "knitpack")]
declare_bzrdir_format! {
    FORMAT_RICH_ROOT_PACK {
        name: "rich-root-pack",
        repo_marker: b"Bazaar pack repository format 1 with rich root (needs bzr 1.0)\n",
        branch_marker: B6,
        wt_marker: WT4,
    }
}

#[cfg(feature = "knitpack")]
declare_bzrdir_format! {
    FORMAT_1_6 {
        name: "1.6",
        repo_marker: b"Bazaar RepositoryFormatKnitPack5 (bzr 1.6)\n",
        branch_marker: B7,
        wt_marker: WT4,
    }
}

#[cfg(feature = "knitpack")]
declare_bzrdir_format! {
    FORMAT_1_6_1_RICH_ROOT {
        name: "1.6.1-rich-root",
        repo_marker: b"Bazaar RepositoryFormatKnitPack5RichRoot (bzr 1.6.1)\n",
        branch_marker: B7,
        wt_marker: WT4,
    }
}

#[cfg(feature = "knitpack")]
declare_bzrdir_format! {
    FORMAT_1_9 {
        name: "1.9",
        repo_marker: b"Bazaar RepositoryFormatKnitPack6 (bzr 1.9)\n",
        branch_marker: B7,
        wt_marker: WT4,
    }
}

#[cfg(feature = "knitpack")]
declare_bzrdir_format! {
    FORMAT_1_9_RICH_ROOT {
        name: "1.9-rich-root",
        repo_marker: b"Bazaar RepositoryFormatKnitPack6RichRoot (bzr 1.9)\n",
        branch_marker: B7,
        wt_marker: WT4,
    }
}

#[cfg(feature = "knitpack")]
declare_bzrdir_format! {
    FORMAT_1_14 {
        name: "1.14",
        repo_marker: b"Bazaar RepositoryFormatKnitPack6 (bzr 1.9)\n",
        branch_marker: B7,
        wt_marker: WT5,
    }
}

#[cfg(feature = "knitpack")]
declare_bzrdir_format! {
    FORMAT_1_14_RICH_ROOT {
        name: "1.14-rich-root",
        repo_marker: b"Bazaar RepositoryFormatKnitPack6RichRoot (bzr 1.9)\n",
        branch_marker: B7,
        wt_marker: WT5,
    }
}

#[cfg(feature = "knit")]
declare_bzrdir_format! {
    FORMAT_KNIT {
        name: "knit",
        repo_marker: b"Bazaar-NG Knit Repository Format 1",
        branch_marker: B5,
        wt_marker: WT3,
    }
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
    /// No repository was found for a control directory, and no enclosing
    /// shared repository exists.
    NoRepositoryPresent,
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
            BzrDirError::NoRepositoryPresent => {
                write!(f, "no repository present and no shared repository found")
            }
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
            // A branch reference is a valid branch component even though it
            // cannot be opened as a normal branch directly: open_branch follows
            // its `location` file to the real branch.
            Component::Branch => crate::branch::find_format(marker)
                .map(|f| f.is_supported() || f.is_reference)
                .unwrap_or(false),
            Component::WorkingTree => crate::workingtree::find_format(marker)
                .map(|f| f.is_supported())
                .unwrap_or(false),
        }
    }
}

/// An opened `.bzr` control directory.
///
/// Two layouts implement this: [`BzrDirMeta`] for the meta-directory
/// format (each component in its own subdirectory) and [`BzrDirAllInOne`]
/// for the older all-in-one weave format, whose stores live directly under
/// `.bzr`. Use the free [`open`] function to open whichever is on disk.
///
/// The accessors return owned component objects that can outlive the
/// control directory.
pub trait ControlDir: Send + Sync {
    /// The transport rooted at the `.bzr` directory.
    fn transport(&self) -> &SharedTransport;

    /// Whether this control directory contains a repository.
    fn has_repository(&self) -> bool;

    /// Whether this control directory contains a branch.
    fn has_branch(&self) -> bool;

    /// Whether this control directory contains a working-tree checkout.
    fn has_workingtree(&self) -> bool;

    /// Open the repository in this control directory.
    fn open_repository(&self) -> Result<Box<dyn crate::repository::Repository>, BzrDirError>;

    /// Open the repository with any stacked-on fallback activated.
    ///
    /// The default returns the plain repository (correct for formats that
    /// cannot stack); [`BzrDirMeta`] overrides it to follow the branch's
    /// `stacked_on_location`.
    fn open_repository_stacked(
        &self,
    ) -> Result<Box<dyn crate::repository::Repository>, BzrDirError> {
        self.open_repository()
    }

    /// Open the branch in this control directory.
    fn open_branch(&self) -> Result<crate::branch::Branch, BzrDirError>;

    /// Open the working tree in this control directory.
    fn open_workingtree(&self) -> Result<Box<dyn crate::workingtree::WorkingTree>, BzrDirError>;
}

/// An opened `.bzr` meta directory.
///
/// Owns the transport rooted *at* the `.bzr` directory (as a
/// [`SharedTransport`], consistent with the other opener objects) and
/// records which components are present and format-verified. The
/// `open_*` accessors descend into each component's subdirectory and
/// return owned objects that can outlive this `BzrDirMeta`.
pub struct BzrDirMeta {
    transport: SharedTransport,
    has_repository: bool,
    has_branch: bool,
    has_workingtree: bool,
}

impl BzrDirMeta {
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

        Ok(BzrDirMeta {
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

        // Repository: empty store, created through the format's own `create`
        // function (looked up by the combo's repository marker).
        let repo_t = bzr.subtransport("repository")?;
        let repo_format = crate::repository::find_format(format.repo_marker).ok_or_else(|| {
            BzrDirError::Component(format!(
                "repository format not registered: {:?}",
                String::from_utf8_lossy(format.repo_marker)
            ))
        })?;
        (repo_format.create)(repo_format, repo_t)
            .map_err(|e| BzrDirError::Component(format!("creating repository: {e}")))?;

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
            checkout.put_bytes(
                "inventory",
                b"<inventory format=\"5\">\n</inventory>\n",
                None,
            )?;
            checkout.put_bytes("pending-merges", b"", None)?;
        }

        Self::open(bzr)
    }

    /// Create a shared repository (no branch or working tree) under `parent`,
    /// using the 2a format, and open it.
    ///
    /// This is the on-disk shape of `brz init-shared-repository`: a `.bzr` with
    /// only a `repository/` component, carrying the empty `shared-storage`
    /// marker so branches in sibling control directories resolve to it via
    /// [`find_repository`](Self::find_repository).
    pub fn create_shared_repository(parent: &SharedTransport) -> Result<Self, BzrDirError> {
        let format = find_control_dir_format("2a").expect("2a format is registered");
        let bzr = parent.subtransport(".bzr")?;
        bzr.mkdir("")?;
        bzr.put_bytes("branch-format", METADIR_MARKER, None)?;

        let repo_t = bzr.subtransport("repository")?;
        let repo_format = crate::repository::find_format(format.repo_marker).ok_or_else(|| {
            BzrDirError::Component(format!(
                "repository format not registered: {:?}",
                String::from_utf8_lossy(format.repo_marker)
            ))
        })?;
        (repo_format.create)(repo_format, repo_t)
            .map_err(|e| BzrDirError::Component(format!("creating repository: {e}")))?;

        // Mark it shared.
        bzr.subtransport("repository")?
            .put_bytes("shared-storage", b"", None)?;

        Self::open(bzr)
    }

    /// Open the branch a reference's `location` points at.
    ///
    /// The location is the URL of the referenced branch's containing directory
    /// (where its `.bzr` lives). breezy writes an absolute URL; a `file://`
    /// prefix is stripped to a local path. The reference is opened as its own
    /// control directory, and its branch returned (which may itself be a
    /// reference, so the open recurses through [`open`]).
    fn open_referenced_branch(&self, location: &str) -> Result<crate::branch::Branch, BzrDirError> {
        let path = location.strip_prefix("file://").unwrap_or(location);
        // The reference points at the directory containing `.bzr`; descend into
        // its control directory and open the branch there.
        let containing = self.transport.subtransport(path)?;
        let target_bzr = containing.subtransport(".bzr")?;
        let target = open(target_bzr)?;
        target.open_branch()
    }

    /// Open the repository of the branch this one is stacked on, following the
    /// stacked-on chain so a multiply-stacked branch picks up every base.
    fn open_stacked_on_repository(
        &self,
        location: &str,
    ) -> Result<Box<dyn crate::repository::Repository>, BzrDirError> {
        let path = location.strip_prefix("file://").unwrap_or(location);
        let containing = self.transport.subtransport(path)?;
        let base_bzr = containing.subtransport(".bzr")?;
        let base = BzrDirMeta::open(base_bzr)?;
        // Recurse so the base's own stacking (if any) is activated too.
        base.open_repository_stacked()
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

    /// A transport rooted at the `repository/` component directory.
    fn repository_transport(&self) -> Result<SharedTransport, BzrDirError> {
        Ok(self
            .transport
            .subtransport(Component::Repository.subdir())?)
    }

    /// Whether this control directory's repository is shared (serves branches
    /// in other control directories).
    ///
    /// A shared repository carries an empty `shared-storage` marker file.
    /// Errors with [`BzrDirError::NotABzrDir`] if there is no repository.
    pub fn is_shared(&self) -> Result<bool, BzrDirError> {
        if !self.has_repository {
            return Err(BzrDirError::NotABzrDir);
        }
        Ok(self.repository_transport()?.has("shared-storage")?)
    }

    /// Whether this repository creates working trees for branches it serves.
    ///
    /// True unless the `no-working-trees` marker is present (note the inverted
    /// polarity: the marker's presence means *no* working trees).
    pub fn make_working_trees(&self) -> Result<bool, BzrDirError> {
        if !self.has_repository {
            return Err(BzrDirError::NotABzrDir);
        }
        Ok(!self.repository_transport()?.has("no-working-trees")?)
    }

    /// Set whether this repository creates working trees. `true` removes the
    /// `no-working-trees` marker; `false` writes it.
    pub fn set_make_working_trees(&self, value: bool) -> Result<(), BzrDirError> {
        if !self.has_repository {
            return Err(BzrDirError::NotABzrDir);
        }
        let repo = self.repository_transport()?;
        if value {
            match repo.delete("no-working-trees") {
                Ok(()) | Err(TransportError::NoSuchFile(_)) => Ok(()),
                Err(e) => Err(e.into()),
            }
        } else {
            repo.put_bytes("no-working-trees", b"", None)?;
            Ok(())
        }
    }

    /// Find the repository serving this control directory, walking up to an
    /// enclosing shared repository when this control directory has none of its
    /// own.
    ///
    /// Mirrors breezy's `find_repository`: this control directory's own
    /// repository is used unconditionally; an ancestor's repository is used
    /// only if it is shared. A non-shared ancestor repository, the filesystem
    /// root, or a missing control directory all stop the walk with
    /// [`BzrDirError::NoRepositoryPresent`].
    pub fn find_repository(&self) -> Result<Box<dyn crate::repository::Repository>, BzrDirError> {
        // Our own repository, if present, is used regardless of shared status.
        if self.has_repository {
            return self.open_repository();
        }
        // Walk up the directory tree looking for an enclosing shared
        // repository. `dir` is the directory *containing* the control dir we
        // are about to probe; it always exists, so it can be canonicalised to
        // detect the filesystem root (where stepping up no longer moves).
        let mut dir = self.transport.subtransport("..")?;
        loop {
            let parent = dir.subtransport("..")?;
            if same_location(&dir, &parent) {
                // Reached the filesystem root.
                return Err(BzrDirError::NoRepositoryPresent);
            }
            let next_bzr = parent.subtransport(".bzr")?;
            match BzrDirMeta::open(next_bzr) {
                Ok(found) if found.has_repository => {
                    if found.is_shared()? {
                        return found.open_repository();
                    }
                    // A non-shared ancestor repository blocks the walk.
                    return Err(BzrDirError::NoRepositoryPresent);
                }
                // No repository here, or not a control dir: keep walking up.
                Ok(_) | Err(BzrDirError::NotABzrDir) | Err(BzrDirError::NotMetaDir(_)) => {
                    dir = parent;
                }
                Err(e) => return Err(e),
            }
        }
    }
}

/// Whether two transports point at the same directory, comparing canonicalised
/// filesystem paths so `..`-laden relative paths that resolve to the same place
/// (e.g. stepping up from the filesystem root) are recognised as equal.
fn same_location(a: &SharedTransport, b: &SharedTransport) -> bool {
    match (a.local_path(""), b.local_path("")) {
        (Some(pa), Some(pb)) => {
            let ca = std::fs::canonicalize(&pa).unwrap_or(pa);
            let cb = std::fs::canonicalize(&pb).unwrap_or(pb);
            ca == cb
        }
        // Non-local transports: fall back to comparing abspaths.
        _ => match (a.abspath(""), b.abspath("")) {
            (Ok(pa), Ok(pb)) => pa == pb,
            _ => false,
        },
    }
}

impl ControlDir for BzrDirMeta {
    fn transport(&self) -> &SharedTransport {
        &self.transport
    }

    fn has_repository(&self) -> bool {
        self.has_repository
    }

    fn has_branch(&self) -> bool {
        self.has_branch
    }

    fn has_workingtree(&self) -> bool {
        self.has_workingtree
    }

    /// Open the repository in this control directory.
    ///
    /// Errors with [`BzrDirError::NotABzrDir`] if there is no repository
    /// component (a branch- or checkout-only `.bzr`).
    fn open_repository(&self) -> Result<Box<dyn crate::repository::Repository>, BzrDirError> {
        if !self.has_repository {
            return Err(BzrDirError::NotABzrDir);
        }
        let sub = self
            .transport
            .subtransport(Component::Repository.subdir())?;
        crate::repository::open(sub)
            .map_err(|e| BzrDirError::Component(format!("opening repository: {e}")))
    }

    /// Open the repository, activating any stacked-on fallback so reads resolve
    /// objects held only in the base repository.
    ///
    /// If the branch is stacked, its `stacked_on_location` is followed to the
    /// base branch's repository, which is wired in as a fallback through a
    /// [`StackedRepository`](crate::repository::StackedRepository). A
    /// non-stacked (or branchless) control directory returns its plain
    /// repository unchanged.
    fn open_repository_stacked(
        &self,
    ) -> Result<Box<dyn crate::repository::Repository>, BzrDirError> {
        let repo = self.open_repository()?;
        if !self.has_branch {
            return Ok(repo);
        }
        let branch = self.open_branch()?;
        let stacked_on = match branch.get_stacked_on_url() {
            Ok(url) => url,
            // Not stacked, or a format that cannot stack: plain repository.
            Err(crate::branch::BranchError::NotStacked)
            | Err(crate::branch::BranchError::Unstackable) => return Ok(repo),
            Err(e) => {
                return Err(BzrDirError::Component(format!(
                    "reading stacked-on location: {e}"
                )))
            }
        };
        use crate::repository::Repository as _;
        let base = self.open_stacked_on_repository(&stacked_on)?;
        let mut stacked = crate::repository::StackedRepository::new(repo);
        stacked
            .add_fallback_repository(base)
            .map_err(|e| BzrDirError::Component(format!("wiring fallback repository: {e}")))?;
        Ok(Box::new(stacked))
    }

    /// Open the branch in this control directory.
    ///
    /// If the branch component is a branch *reference* (a lightweight
    /// checkout's pointer to a branch held elsewhere), this follows the
    /// reference's `location` to the real branch and returns that. Errors with
    /// [`BzrDirError::NotABzrDir`] if there is no branch component.
    fn open_branch(&self) -> Result<crate::branch::Branch, BzrDirError> {
        if !self.has_branch {
            return Err(BzrDirError::NotABzrDir);
        }
        let sub = self.transport.subtransport(Component::Branch.subdir())?;
        let branch = crate::branch::Branch::new(sub);
        match branch
            .get_reference()
            .map_err(|e| BzrDirError::Component(format!("reading branch reference: {e}")))?
        {
            Some(location) => self.open_referenced_branch(&location),
            None => Ok(branch),
        }
    }

    /// Open the working tree in this control directory.
    ///
    /// The working tree reads `.bzr/checkout/dirstate` and the files on
    /// disk, so it is rooted at the directory that *contains* `.bzr` (one
    /// level up from this `BzrDirMeta`'s transport).
    ///
    /// Errors with [`BzrDirError::NotABzrDir`] if there is no working-tree
    /// component.
    fn open_workingtree(&self) -> Result<Box<dyn crate::workingtree::WorkingTree>, BzrDirError> {
        if !self.has_workingtree {
            return Err(BzrDirError::NotABzrDir);
        }
        let root = self.transport.subtransport("..")?;
        crate::workingtree::open(root)
            .map_err(|e| BzrDirError::Component(format!("opening working tree: {e}")))
    }
}

/// An opened all-in-one weave control directory ("Bazaar-NG branch,
/// format 6", bzr 0.8).
///
/// Unlike the meta-directory layout, the repository, branch and working
/// tree all live directly under `.bzr` rather than in component
/// subdirectories. The transport is rooted at `.bzr` itself.
#[cfg(feature = "weave")]
pub struct BzrDirAllInOne {
    transport: SharedTransport,
    format: &'static crate::repository::RepositoryFormat,
}

#[cfg(feature = "weave")]
impl BzrDirAllInOne {
    /// Open the all-in-one `.bzr` directory reachable through `transport`
    /// (rooted at `.bzr` itself).
    ///
    /// Reads `.bzr/branch-format`; succeeds only if the marker names a
    /// supported weave repository format. Other markers yield
    /// [`BzrDirError::NotMetaDir`] (carrying the marker found).
    pub fn open(transport: SharedTransport) -> Result<Self, BzrDirError> {
        let marker = match transport.get_bytes("branch-format") {
            Ok(b) => b,
            Err(TransportError::NoSuchFile(_)) => return Err(BzrDirError::NotABzrDir),
            Err(e) => return Err(e.into()),
        };
        let format = crate::repository::find_format(&marker)
            .filter(|f| f.is_all_in_one() && f.is_supported());
        match format {
            Some(format) => Ok(BzrDirAllInOne { transport, format }),
            None => Err(BzrDirError::NotMetaDir(marker)),
        }
    }

    /// Create a fresh all-in-one weave control directory ("Bazaar-NG branch,
    /// format 6") under `parent` (the directory that will contain `.bzr`) and
    /// open it.
    ///
    /// Writes the `.bzr` directory, the `branch-format` marker, an empty
    /// `revision-history` and `pending-merges`, the revision-less working
    /// `inventory`, and the empty weave repository scaffold (all directly
    /// under `.bzr`).
    pub fn create(parent: &SharedTransport) -> Result<Self, BzrDirError> {
        let marker: &[u8] = b"Bazaar-NG branch, format 6\n";
        let format = crate::repository::find_format(marker)
            .filter(|f| f.is_all_in_one())
            .ok_or_else(|| BzrDirError::Component("weave format 6 not registered".to_string()))?;

        let bzr = parent.subtransport(".bzr")?;
        bzr.mkdir("")?;
        bzr.put_bytes("branch-format", marker, None)?;
        bzr.put_bytes("revision-history", b"", None)?;
        bzr.put_bytes("pending-merges", b"", None)?;
        bzr.put_bytes(
            "inventory",
            b"<inventory format=\"5\">\n</inventory>\n",
            None,
        )?;

        crate::repository::WeaveRepository::create(bzr.clone(), format)
            .map_err(|e| BzrDirError::Component(format!("creating repository: {e}")))?;

        Self::open(bzr)
    }
}

#[cfg(feature = "weave")]
impl ControlDir for BzrDirAllInOne {
    fn transport(&self) -> &SharedTransport {
        &self.transport
    }

    fn has_repository(&self) -> bool {
        true
    }

    fn has_branch(&self) -> bool {
        true
    }

    fn has_workingtree(&self) -> bool {
        true
    }

    fn open_repository(&self) -> Result<Box<dyn crate::repository::Repository>, BzrDirError> {
        let repo = crate::repository::WeaveRepository::open(self.transport.clone(), self.format)
            .map_err(|e| BzrDirError::Component(format!("opening repository: {e}")))?;
        Ok(Box::new(repo))
    }

    /// Open the all-in-one branch.
    ///
    /// The weave branch stores its full mainline in `.bzr/revision-history`
    /// (like branch format 5) and has no `.bzr/branch/format` marker, so the
    /// branch is opened with the full-history format directly rather than by
    /// reading a marker.
    fn open_branch(&self) -> Result<crate::branch::Branch, BzrDirError> {
        let format = crate::branch::find_format(b"Bazaar-NG branch format 5\n")
            .ok_or_else(|| BzrDirError::Component("branch format 5 not registered".to_string()))?;
        Ok(crate::branch::Branch::with_format(
            self.transport.clone(),
            format,
        ))
    }

    /// Open the all-in-one working tree.
    ///
    /// The weave working tree stores its inventory, pending-merges and basis
    /// (the branch's revision-history) directly under `.bzr`, with no
    /// `checkout/` subdir or dirstate. Like the metadir tree it is rooted at
    /// the directory that *contains* `.bzr`.
    fn open_workingtree(&self) -> Result<Box<dyn crate::workingtree::WorkingTree>, BzrDirError> {
        let root = self.transport.subtransport("..")?;
        let wt = crate::workingtree::WorkingTree3::open_all_in_one(root)
            .map_err(|e| BzrDirError::Component(format!("opening working tree: {e}")))?;
        Ok(Box::new(wt))
    }
}

/// Open the `.bzr` control directory reachable through `transport`.
///
/// `transport` must be rooted at the `.bzr` directory itself. Probes
/// `.bzr/branch-format` and returns a [`BzrDirMeta`] for the meta-directory
/// layout or a [`BzrDirAllInOne`] for a supported all-in-one weave format.
pub fn open(transport: SharedTransport) -> Result<Box<dyn ControlDir>, BzrDirError> {
    let marker = match transport.get_bytes("branch-format") {
        Ok(b) => b,
        Err(TransportError::NoSuchFile(_)) => return Err(BzrDirError::NotABzrDir),
        Err(e) => return Err(e.into()),
    };
    if marker == METADIR_MARKER {
        return Ok(Box::new(BzrDirMeta::open(transport)?));
    }
    #[cfg(feature = "weave")]
    {
        Ok(Box::new(BzrDirAllInOne::open(transport)?))
    }
    #[cfg(not(feature = "weave"))]
    {
        Err(BzrDirError::NotMetaDir(marker))
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
        let bd = BzrDirMeta::open(t).unwrap();
        assert!(bd.has_repository());
        assert!(bd.has_branch());
        assert!(bd.has_workingtree());
    }

    #[test]
    fn opens_repository_only() {
        let dir = tempfile::tempdir().unwrap();
        make_bzrdir(dir.path(), &[Component::Repository]);
        let t = bzr_transport(dir.path());
        let bd = BzrDirMeta::open(t).unwrap();
        assert!(bd.has_repository());
        assert!(!bd.has_branch());
        assert!(!bd.has_workingtree());
    }

    #[test]
    fn missing_dir_is_not_a_bzrdir() {
        let dir = tempfile::tempdir().unwrap();
        let t = bzr_transport(dir.path());
        match BzrDirMeta::open(t) {
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
        match BzrDirMeta::open(t) {
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
        match BzrDirMeta::open(t) {
            Err(BzrDirError::UnsupportedFormat {
                component: Component::Repository,
                ..
            }) => {}
            other => panic!("expected UnsupportedFormat(Repository), got {other:?}"),
        }
    }

    // A minimal all-in-one weave `.bzr` (one revision committing one file),
    // captured byte-for-byte from a `brz init --format=weave` tree. The
    // revision id is jelmer@jelmer.uk-20200101120000-jebv9gxg8ubhzbj8 and the
    // file is a.txt with content "hi\n".
    #[cfg(feature = "weave")]
    const WEAVE_REVID: &[u8] = b"jelmer@jelmer.uk-20200101120000-jebv9gxg8ubhzbj8";
    #[cfg(feature = "weave")]
    const WEAVE_FILE_ID: &[u8] = b"a.txt-20260604015637-2c5ba92i40zw1mvp-1";

    #[cfg(feature = "weave")]
    const WEAVE_INVENTORY: &[u8] = b"# bzr weave file v5\ni\n1 8a002a6377d9177f17c988d81dda2e0175a18398\nn jelmer@jelmer.uk-20200101120000-jebv9gxg8ubhzbj8\n\nw\n{ 0\n. <inventory format=\"5\" revision_id=\"jelmer@jelmer.uk-20200101120000-jebv9gxg8ubhzbj8\">\n. <file file_id=\"a.txt-20260604015637-2c5ba92i40zw1mvp-1\" name=\"a.txt\" revision=\"jelmer@jelmer.uk-20200101120000-jebv9gxg8ubhzbj8\" text_sha1=\"55ca6286e3e4f4fba5d0448333fa99fc5a404a73\" text_size=\"3\" />\n. </inventory>\n}\nW\n";

    #[cfg(feature = "weave")]
    const WEAVE_REVISION: &[u8] = b"<revision committer=\"Jelmer Vernooij &lt;jelmer@jelmer.uk&gt;\" format=\"5\" inventory_sha1=\"8a002a6377d9177f17c988d81dda2e0175a18398\" revision_id=\"jelmer@jelmer.uk-20200101120000-jebv9gxg8ubhzbj8\" timestamp=\"1577880000.000\" timezone=\"0\">\n<message>one</message>\n<properties><property name=\"branch-nick\">wv</property>\n</properties>\n</revision>\n";

    #[cfg(feature = "weave")]
    const WEAVE_FILE_WEAVE: &[u8] = b"# bzr weave file v5\ni\n1 55ca6286e3e4f4fba5d0448333fa99fc5a404a73\nn jelmer@jelmer.uk-20200101120000-jebv9gxg8ubhzbj8\n\nw\n{ 0\n. hi\n}\nW\n";

    #[cfg(feature = "weave")]
    fn make_weave_bzrdir(root: &std::path::Path) {
        use crate::key_mapper::{hash_prefix_map, url_unquote};
        let bzr = root.join(".bzr");
        std::fs::create_dir_all(&bzr).unwrap();
        std::fs::write(bzr.join("branch-format"), b"Bazaar-NG branch, format 6\n").unwrap();
        std::fs::write(bzr.join("inventory.weave"), WEAVE_INVENTORY).unwrap();
        let mut history = WEAVE_REVID.to_vec();
        history.push(b'\n');
        std::fs::write(bzr.join("revision-history"), &history).unwrap();
        // The working inventory (revision-less) and an empty pending-merges,
        // as brz keeps them directly under `.bzr` for the all-in-one tree.
        std::fs::write(
            bzr.join("inventory"),
            format!(
                "<inventory format=\"5\">\n<file file_id=\"{}\" name=\"a.txt\" />\n</inventory>\n",
                std::str::from_utf8(WEAVE_FILE_ID).unwrap()
            ),
        )
        .unwrap();
        std::fs::write(bzr.join("pending-merges"), b"").unwrap();
        std::fs::write(root.join("a.txt"), b"hi\n").unwrap();

        // hash_prefix_map url-quotes the name; the local transport unquotes it
        // again when resolving, so the on-disk name is the unquoted form (e.g.
        // a literal `@`, as brz writes it).
        let rev_name = url_unquote(&hash_prefix_map(WEAVE_REVID));
        let rev_path = bzr.join(format!("revision-store/{rev_name}"));
        std::fs::create_dir_all(rev_path.parent().unwrap()).unwrap();
        std::fs::write(rev_path, WEAVE_REVISION).unwrap();

        let weave_name = url_unquote(&hash_prefix_map(WEAVE_FILE_ID));
        let weave_path = bzr.join(format!("weaves/{weave_name}.weave"));
        std::fs::create_dir_all(weave_path.parent().unwrap()).unwrap();
        std::fs::write(weave_path, WEAVE_FILE_WEAVE).unwrap();
    }

    #[cfg(feature = "weave")]
    #[test]
    fn opens_all_in_one_weave() {
        let dir = tempfile::tempdir().unwrap();
        make_weave_bzrdir(dir.path());
        let cd = open(bzr_transport(dir.path())).unwrap();
        assert!(cd.has_repository());
        assert!(cd.has_branch());
        assert!(cd.has_workingtree());

        let repo = cd.open_repository().unwrap();
        assert_eq!(repo.all_revision_ids().unwrap(), vec![WEAVE_REVID.to_vec()]);

        let rev = repo.get_revision(WEAVE_REVID).unwrap();
        assert_eq!(rev.message, "one");

        let inv = repo.get_inventory(WEAVE_REVID).unwrap();
        let a_txt = inv
            .entries()
            .unwrap()
            .into_iter()
            .find(|(path, _)| path == "a.txt")
            .expect("a.txt in inventory");
        assert_eq!(a_txt.1.file_id().as_bytes(), WEAVE_FILE_ID);

        let text = repo.get_file_text(WEAVE_FILE_ID, WEAVE_REVID).unwrap();
        assert_eq!(text, b"hi\n".to_vec());

        let branch = cd.open_branch().unwrap();
        assert_eq!(
            branch.last_revision_info().unwrap(),
            (1, WEAVE_REVID.to_vec())
        );

        // The all-in-one working tree reads its inventory and basis directly
        // under `.bzr` (basis from the branch's revision-history).
        let wt = cd.open_workingtree().unwrap();
        assert_eq!(wt.basis_revision().as_deref(), Some(WEAVE_REVID));
        let files = wt.list_files();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].path, "a.txt");
        assert_eq!(files[0].file_id, WEAVE_FILE_ID.to_vec());
        assert_eq!(wt.path2id("a.txt").as_deref(), Some(WEAVE_FILE_ID));
    }

    /// A branch reference's `open_branch` follows the `location` file to the
    /// real branch held in another control directory.
    #[test]
    fn open_branch_follows_reference() {
        let dir = tempfile::tempdir().unwrap();

        // The real branch lives under `target/`; give it a tip.
        let target_root = dir.path().join("target");
        std::fs::create_dir_all(&target_root).unwrap();
        let target_parent: SharedTransport = std::sync::Arc::new(LocalTransport::new(&target_root));
        let target = BzrDirMeta::create(&target_parent).unwrap();
        target
            .open_branch()
            .unwrap()
            .set_last_revision_info(3, b"rev-real")
            .unwrap();

        // The reference lives under `ref/`: a meta dir whose branch component
        // carries the reference format marker and a `location` file pointing at
        // the target's containing directory.
        let ref_root = dir.path().join("ref");
        let ref_bzr = ref_root.join(".bzr");
        std::fs::create_dir_all(ref_bzr.join("branch")).unwrap();
        std::fs::write(ref_bzr.join("branch-format"), METADIR_MARKER).unwrap();
        std::fs::write(
            ref_bzr.join("branch/format"),
            b"Bazaar-NG Branch Reference Format 1\n",
        )
        .unwrap();
        std::fs::write(
            ref_bzr.join("branch/location"),
            target_root.to_str().unwrap().as_bytes(),
        )
        .unwrap();

        let ref_bzr_transport: SharedTransport = std::sync::Arc::new(LocalTransport::new(&ref_bzr));
        let cd = BzrDirMeta::open(ref_bzr_transport).unwrap();
        let branch = cd.open_branch().unwrap();
        assert_eq!(
            branch.last_revision_info().unwrap(),
            (3, b"rev-real".to_vec())
        );
    }

    /// A stacked branch's open_repository_stacked resolves revisions held only
    /// in the base repository it is stacked on.
    #[test]
    fn open_repository_stacked_resolves_from_base() {
        use crate::inventory::ROOT_ID;

        let dir = tempfile::tempdir().unwrap();

        // The base lives under `base/`: a 2a control directory with one commit.
        let base_root = dir.path().join("base");
        std::fs::create_dir_all(&base_root).unwrap();
        let base_parent: SharedTransport = std::sync::Arc::new(LocalTransport::new(&base_root));
        let base = BzrDirMeta::create(&base_parent).unwrap();
        {
            let mut repo = base.open_repository().unwrap();
            repo.start_write_group().unwrap();
            let rev = crate::revision::Revision::new(
                crate::RevisionId::from(&b"rev-base"[..]),
                vec![],
                Some("T <t@e>".to_string()),
                "base commit".to_string(),
                std::collections::HashMap::new(),
                None,
                1577880000.0,
                Some(0),
            );
            repo.add_revision(&rev, &[]).unwrap();
            let entries = vec![crate::inventory::Entry::root(
                crate::FileId::from(ROOT_ID),
                Some(crate::RevisionId::from(&b"rev-base"[..])),
            )];
            repo.add_inventory_from_entries(b"rev-base", &[], ROOT_ID, &entries)
                .unwrap();
            repo.commit_write_group().unwrap();
        }

        // The stacked branch lives under `top/`: its own (empty) 2a repository,
        // with branch.conf pointing stacked_on_location at the base.
        let top_root = dir.path().join("top");
        std::fs::create_dir_all(&top_root).unwrap();
        let top_parent: SharedTransport = std::sync::Arc::new(LocalTransport::new(&top_root));
        let top = BzrDirMeta::create(&top_parent).unwrap();
        top.open_branch()
            .unwrap()
            .set_stacked_on_url(Some(base_root.to_str().unwrap()))
            .unwrap();

        // The plain repository does not have the base's revision...
        assert!(!top
            .open_repository()
            .unwrap()
            .has_revision(b"rev-base")
            .unwrap());
        // ...but the stacked one resolves it through the fallback.
        let stacked = top.open_repository_stacked().unwrap();
        assert!(stacked.has_revision(b"rev-base").unwrap());
        assert_eq!(
            stacked.get_revision(b"rev-base").unwrap().message,
            "base commit"
        );
    }

    #[test]
    fn create_shared_repository_is_shared() {
        let dir = tempfile::tempdir().unwrap();
        let parent: SharedTransport = std::sync::Arc::new(LocalTransport::new(dir.path()));
        let cd = BzrDirMeta::create_shared_repository(&parent).unwrap();
        assert!(cd.has_repository());
        assert!(!cd.has_branch());
        assert!(cd.is_shared().unwrap());
        // A normal control directory is not shared.
        let other = tempfile::tempdir().unwrap();
        let op: SharedTransport = std::sync::Arc::new(LocalTransport::new(other.path()));
        assert!(!BzrDirMeta::create(&op).unwrap().is_shared().unwrap());
    }

    #[test]
    fn make_working_trees_toggles_marker() {
        let dir = tempfile::tempdir().unwrap();
        let parent: SharedTransport = std::sync::Arc::new(LocalTransport::new(dir.path()));
        let cd = BzrDirMeta::create_shared_repository(&parent).unwrap();
        // Default: working trees are made (no marker).
        assert!(cd.make_working_trees().unwrap());
        cd.set_make_working_trees(false).unwrap();
        assert!(!cd.make_working_trees().unwrap());
        cd.set_make_working_trees(true).unwrap();
        assert!(cd.make_working_trees().unwrap());
    }

    #[test]
    fn find_repository_walks_up_to_shared() {
        let dir = tempfile::tempdir().unwrap();
        // A shared repository at the top.
        let shared_root = dir.path().join("shared");
        std::fs::create_dir_all(&shared_root).unwrap();
        let shared_parent: SharedTransport = std::sync::Arc::new(LocalTransport::new(&shared_root));
        let shared = BzrDirMeta::create_shared_repository(&shared_parent).unwrap();
        // Give the shared repo a revision so we can tell we resolved to it.
        {
            let mut repo = shared.open_repository().unwrap();
            repo.start_write_group().unwrap();
            let rev = crate::revision::Revision::new(
                crate::RevisionId::from(&b"rev-shared"[..]),
                vec![],
                Some("T <t@e>".to_string()),
                "shared".to_string(),
                std::collections::HashMap::new(),
                None,
                1577880000.0,
                Some(0),
            );
            repo.add_revision(&rev, &[]).unwrap();
            repo.add_inventory_from_entries(
                b"rev-shared",
                &[],
                crate::inventory::ROOT_ID,
                &[crate::inventory::Entry::root(
                    crate::FileId::from(crate::inventory::ROOT_ID),
                    Some(crate::RevisionId::from(&b"rev-shared"[..])),
                )],
            )
            .unwrap();
            repo.commit_write_group().unwrap();
        }

        // A branch-only control directory inside the shared repository's tree.
        let branch_root = shared_root.join("branch1");
        let branch_bzr = branch_root.join(".bzr");
        std::fs::create_dir_all(branch_bzr.join("branch")).unwrap();
        std::fs::write(branch_bzr.join("branch-format"), METADIR_MARKER).unwrap();
        std::fs::write(branch_bzr.join("branch/format"), BRANCH_FORMAT_7).unwrap();
        std::fs::write(branch_bzr.join("branch/last-revision"), b"0 null:\n").unwrap();

        let branch_cd =
            BzrDirMeta::open(std::sync::Arc::new(LocalTransport::new(&branch_bzr))).unwrap();
        assert!(!branch_cd.has_repository());
        // find_repository walks up to the shared repository.
        let repo = branch_cd.find_repository().unwrap();
        assert!(repo.has_revision(b"rev-shared").unwrap());
    }

    #[test]
    fn find_repository_errors_when_none() {
        // A standalone control directory with its own (non-shared) repository
        // returns it directly; a branch-only dir with no shared ancestor errors.
        let dir = tempfile::tempdir().unwrap();
        let branch_root = dir.path().join("lonely");
        let branch_bzr = branch_root.join(".bzr");
        std::fs::create_dir_all(branch_bzr.join("branch")).unwrap();
        std::fs::write(branch_bzr.join("branch-format"), METADIR_MARKER).unwrap();
        std::fs::write(branch_bzr.join("branch/format"), BRANCH_FORMAT_7).unwrap();
        std::fs::write(branch_bzr.join("branch/last-revision"), b"0 null:\n").unwrap();
        let cd = BzrDirMeta::open(std::sync::Arc::new(LocalTransport::new(&branch_bzr))).unwrap();
        assert!(matches!(
            cd.find_repository(),
            Err(BzrDirError::NoRepositoryPresent)
        ));
    }
}
