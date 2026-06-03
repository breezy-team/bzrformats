//! Declarations of all known working-tree formats.

use crate::declare_workingtree_format;

declare_workingtree_format! {
    FORMAT_3 {
        format_string: b"Bazaar-NG Working Tree format 3",
        description: "Working tree format 3 (pre-dirstate)",
        deprecated: true,
    }
}

declare_workingtree_format! {
    FORMAT_4 {
        format_string: b"Bazaar Working Tree Format 4 (bzr 0.15)\n",
        description: "Working tree format 4 (dirstate)",
        uses_dirstate: true,
        supported: true,
    }
}

declare_workingtree_format! {
    FORMAT_5 {
        format_string: b"Bazaar Working Tree Format 5 (bzr 1.11)\n",
        description: "Working tree format 5 (dirstate, content filtering)",
        uses_dirstate: true,
        supports_content_filtering: true,
        supported: true,
    }
}

declare_workingtree_format! {
    FORMAT_6 {
        format_string: b"Bazaar Working Tree Format 6 (bzr 1.14)\n",
        description: "Working tree format 6 (dirstate, views, content filtering)",
        uses_dirstate: true,
        supports_content_filtering: true,
        supports_views: true,
        supported: true,
    }
}
