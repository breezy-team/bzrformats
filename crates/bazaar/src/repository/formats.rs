//! Declarations of all known repository formats.
//!
//! Each [`declare_repository_format!`](crate::declare_repository_format)
//! both defines a `static` format and registers it with the format
//! [`registry`](super::format). Knit-pack and 2a formats carry an `open`
//! function that dispatches to their reader; the plain knit formats fall
//! back to the unsupported opener pending a decoder.

use super::knit_repo::open_knit;
use super::pack_2a::open_group_compress;
use super::pack_knit::open_knit_pack;
use crate::bencode_serializer::BEncodeRevisionSerializer1;
use crate::declare_repository_format;
use crate::xml_serializer::{
    Chk255BigPageInventorySerializer, XMLInventorySerializer5, XMLInventorySerializer6,
    XMLInventorySerializer7, XMLRevisionSerializer5,
};

declare_repository_format! {
    FORMAT_WEAVE_6 {
        format_string: b"Bazaar-NG branch, format 6\n",
        description: "Weave repository format 6 (all-in-one)",
        revision_serializer: &XMLRevisionSerializer5,
        inventory_serializer: &XMLInventorySerializer5,
        // No `open`: the all-in-one weave repository has no
        // `.bzr/repository/format` marker, so it is opened through BzrDir's
        // all-in-one path rather than the metadir `open` dispatcher.
        all_in_one: true,
        supported: true,
        deprecated: true,
    }
}

declare_repository_format! {
    FORMAT_KNIT_1 {
        format_string: b"Bazaar-NG Knit Repository Format 1",
        description: "Knit repository format 1",
        revision_serializer: &XMLRevisionSerializer5,
        inventory_serializer: &XMLInventorySerializer5,
        open: open_knit,
        supported: true,
        deprecated: true,
    }
}

declare_repository_format! {
    FORMAT_KNIT_3 {
        format_string: b"Bazaar Knit Repository Format 3 (bzr 0.15)\n",
        description: "Knit repository format 3 (rich root, subtrees)",
        revision_serializer: &XMLRevisionSerializer5,
        inventory_serializer: &XMLInventorySerializer7,
        open: open_knit,
        rich_root_data: true,
        supports_tree_reference: true,
        supported: true,
        deprecated: true,
    }
}

declare_repository_format! {
    FORMAT_KNIT_4 {
        format_string: b"Bazaar Knit Repository Format 4 (bzr 1.0)\n",
        description: "Knit repository format 4 (rich root)",
        revision_serializer: &XMLRevisionSerializer5,
        inventory_serializer: &XMLInventorySerializer6,
        open: open_knit,
        rich_root_data: true,
        supported: true,
        deprecated: true,
    }
}

declare_repository_format! {
    FORMAT_KNIT_PACK_1 {
        format_string: b"Bazaar pack repository format 1 (needs bzr 0.92)\n",
        description: "Pack repository format 1",
        revision_serializer: &XMLRevisionSerializer5,
        inventory_serializer: &XMLInventorySerializer5,
        open: open_knit_pack,
        supported: true,
        uses_btree_index: false,
    }
}

declare_repository_format! {
    FORMAT_KNIT_PACK_3 {
        format_string: b"Bazaar pack repository format 1 with subtree support (needs bzr 0.92)\n",
        description: "Pack repository format 1 with subtree support",
        revision_serializer: &XMLRevisionSerializer5,
        inventory_serializer: &XMLInventorySerializer7,
        open: open_knit_pack,
        rich_root_data: true,
        supports_tree_reference: true,
        supported: true,
        uses_btree_index: false,
    }
}

declare_repository_format! {
    FORMAT_KNIT_PACK_4 {
        format_string: b"Bazaar pack repository format 1 with rich root (needs bzr 1.0)\n",
        description: "Pack repository format 1 with rich root",
        revision_serializer: &XMLRevisionSerializer5,
        inventory_serializer: &XMLInventorySerializer6,
        open: open_knit_pack,
        rich_root_data: true,
        supported: true,
        uses_btree_index: false,
    }
}

declare_repository_format! {
    FORMAT_KNIT_PACK_5 {
        format_string: b"Bazaar RepositoryFormatKnitPack5 (bzr 1.6)\n",
        description: "Pack repository format 5 (stackable)",
        revision_serializer: &XMLRevisionSerializer5,
        inventory_serializer: &XMLInventorySerializer5,
        open: open_knit_pack,
        supports_external_lookups: true,
        supported: true,
        uses_btree_index: false,
    }
}

declare_repository_format! {
    FORMAT_KNIT_PACK_5_RICH_ROOT {
        format_string: b"Bazaar RepositoryFormatKnitPack5RichRoot (bzr 1.6.1)\n",
        description: "Pack repository format 5 with rich root (stackable)",
        revision_serializer: &XMLRevisionSerializer5,
        inventory_serializer: &XMLInventorySerializer6,
        open: open_knit_pack,
        rich_root_data: true,
        supports_external_lookups: true,
        supported: true,
        uses_btree_index: false,
    }
}

declare_repository_format! {
    FORMAT_KNIT_PACK_5_RICH_ROOT_BROKEN {
        format_string: b"Bazaar RepositoryFormatKnitPack5RichRoot (bzr 1.6)\n",
        description: "Pack repository format 5 with rich root (broken)",
        revision_serializer: &XMLRevisionSerializer5,
        inventory_serializer: &XMLInventorySerializer6,
        open: open_knit_pack,
        rich_root_data: true,
        supports_external_lookups: true,
        deprecated: true,
        uses_btree_index: false,
    }
}

declare_repository_format! {
    FORMAT_KNIT_PACK_6 {
        format_string: b"Bazaar RepositoryFormatKnitPack6 (bzr 1.9)\n",
        description: "Pack repository format 6 (btree indexes, stackable)",
        revision_serializer: &XMLRevisionSerializer5,
        inventory_serializer: &XMLInventorySerializer5,
        open: open_knit_pack,
        supports_external_lookups: true,
        supported: true,
    }
}

declare_repository_format! {
    FORMAT_KNIT_PACK_6_RICH_ROOT {
        format_string: b"Bazaar RepositoryFormatKnitPack6RichRoot (bzr 1.9)\n",
        description: "Pack repository format 6 with rich root (btree, stackable)",
        revision_serializer: &XMLRevisionSerializer5,
        inventory_serializer: &XMLInventorySerializer6,
        open: open_knit_pack,
        rich_root_data: true,
        supports_external_lookups: true,
        supported: true,
    }
}

declare_repository_format! {
    FORMAT_2A {
        format_string: b"Bazaar repository format 2a (needs bzr 1.16 or later)\n",
        description: "Repository format 2a (groupcompress, CHK)",
        revision_serializer: &BEncodeRevisionSerializer1,
        inventory_serializer: &Chk255BigPageInventorySerializer,
        open: open_group_compress,
        rich_root_data: true,
        supports_chks: true,
        supports_tree_reference: true,
        supports_external_lookups: true,
        supported: true,
    }
}

declare_repository_format! {
    FORMAT_2A_SUBTREE {
        format_string: b"Bazaar development format 8\n",
        description: "Repository format 2a with subtree support",
        revision_serializer: &BEncodeRevisionSerializer1,
        inventory_serializer: &Chk255BigPageInventorySerializer,
        open: open_group_compress,
        rich_root_data: true,
        supports_chks: true,
        supports_tree_reference: true,
        supports_external_lookups: true,
        supported: true,
    }
}
