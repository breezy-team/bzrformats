//! Declarations of all control-directory formats this crate can create.
//!
//! Each [`declare_bzrdir_format!`](crate::declare_bzrdir_format) pairs a
//! `brz init --format=` name with the repository, branch and working-tree
//! format markers brz uses for that name, taken from breezy's format
//! registry.

use crate::declare_bzrdir_format;

const B5: &[u8] = b"Bazaar-NG branch format 5\n";
const B6: &[u8] = b"Bazaar Branch Format 6 (bzr 0.15)\n";
const B7: &[u8] = super::BRANCH_FORMAT_7;
const WT3: &[u8] = b"Bazaar-NG Working Tree format 3";
const WT4: &[u8] = b"Bazaar Working Tree Format 4 (bzr 0.15)\n";
const WT5: &[u8] = b"Bazaar Working Tree Format 5 (bzr 1.11)\n";
const WT6: &[u8] = super::WORKINGTREE_FORMAT_6;

declare_bzrdir_format! {
    FORMAT_2A {
        name: "2a",
        repo_marker: super::REPOSITORY_FORMAT_2A,
        branch_marker: B7,
        wt_marker: WT6,
        wt_has_views: true,
    }
}

declare_bzrdir_format! {
    FORMAT_PACK_0_92 {
        name: "pack-0.92",
        repo_marker: b"Bazaar pack repository format 1 (needs bzr 0.92)\n",
        branch_marker: B6,
        wt_marker: WT4,
    }
}

declare_bzrdir_format! {
    FORMAT_PACK_0_92_SUBTREE {
        name: "pack-0.92-subtree",
        repo_marker: b"Bazaar pack repository format 1 with subtree support (needs bzr 0.92)\n",
        branch_marker: B6,
        wt_marker: WT4,
    }
}

declare_bzrdir_format! {
    FORMAT_RICH_ROOT_PACK {
        name: "rich-root-pack",
        repo_marker: b"Bazaar pack repository format 1 with rich root (needs bzr 1.0)\n",
        branch_marker: B6,
        wt_marker: WT4,
    }
}

declare_bzrdir_format! {
    FORMAT_1_6 {
        name: "1.6",
        repo_marker: b"Bazaar RepositoryFormatKnitPack5 (bzr 1.6)\n",
        branch_marker: B7,
        wt_marker: WT4,
    }
}

declare_bzrdir_format! {
    FORMAT_1_6_1_RICH_ROOT {
        name: "1.6.1-rich-root",
        repo_marker: b"Bazaar RepositoryFormatKnitPack5RichRoot (bzr 1.6.1)\n",
        branch_marker: B7,
        wt_marker: WT4,
    }
}

declare_bzrdir_format! {
    FORMAT_1_9 {
        name: "1.9",
        repo_marker: b"Bazaar RepositoryFormatKnitPack6 (bzr 1.9)\n",
        branch_marker: B7,
        wt_marker: WT4,
    }
}

declare_bzrdir_format! {
    FORMAT_1_9_RICH_ROOT {
        name: "1.9-rich-root",
        repo_marker: b"Bazaar RepositoryFormatKnitPack6RichRoot (bzr 1.9)\n",
        branch_marker: B7,
        wt_marker: WT4,
    }
}

declare_bzrdir_format! {
    FORMAT_1_14 {
        name: "1.14",
        repo_marker: b"Bazaar RepositoryFormatKnitPack6 (bzr 1.9)\n",
        branch_marker: B7,
        wt_marker: WT5,
    }
}

declare_bzrdir_format! {
    FORMAT_1_14_RICH_ROOT {
        name: "1.14-rich-root",
        repo_marker: b"Bazaar RepositoryFormatKnitPack6RichRoot (bzr 1.9)\n",
        branch_marker: B7,
        wt_marker: WT5,
    }
}

declare_bzrdir_format! {
    FORMAT_KNIT {
        name: "knit",
        repo_marker: b"Bazaar-NG Knit Repository Format 1",
        branch_marker: B5,
        wt_marker: WT3,
    }
}
