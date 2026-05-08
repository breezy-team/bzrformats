use crate::inventory::MutableInventory;
use crate::revision::Revision;
use crate::RevisionId;
use std::io::Read;

#[derive(Debug)]
pub enum Error {
    DecodeError(String),
    EncodeError(String),
    IOError(std::io::Error),
    UnexpectedInventoryFormat(String),
    UnsupportedInventoryKind(String),
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Error::IOError(error)
    }
}

pub trait RevisionSerializer: Send + Sync {
    fn format_name(&self) -> &'static str;

    fn squashes_xml_invalid_characters(&self) -> bool;

    fn read_revision(&self, file: &mut dyn Read) -> Result<Revision, Error>;

    fn write_revision_to_string(&self, revision: &Revision) -> Result<Vec<u8>, Error>;

    fn write_revision_to_lines(
        &self,
        revision: &Revision,
    ) -> Box<dyn Iterator<Item = Result<Vec<u8>, Error>>>;

    fn read_revision_from_string(&self, string: &[u8]) -> Result<Revision, Error>;
}

pub trait InventorySerializer: Send + Sync {
    fn format_num(&self) -> &'static [u8];

    /// Whether this serializer supports the "altered-by" hack — extracting
    /// per-text revision references by regex-scanning inventory lines
    /// without parsing the full XML. True for the flat XML formats
    /// (v5/v6/v7/v8); false for v4 and CHK serializers.
    fn support_altered_by_hack(&self) -> bool {
        false
    }

    /// Serialize the inventory to a vector of byte chunks (one per line).
    ///
    /// If `working` is true, history data (text_sha1, text_size,
    /// reference_revision, symlink_target, revision) is omitted. This is used
    /// by working-tree inventory serialization where that data is not yet
    /// stable.
    fn write_inventory_to_lines(
        &self,
        inv: &MutableInventory,
        working: bool,
    ) -> Result<Vec<Vec<u8>>, Error>;

    /// Serialize the inventory to a vector of byte chunks (alias for lines).
    fn write_inventory_to_chunks(
        &self,
        inv: &MutableInventory,
        working: bool,
    ) -> Result<Vec<Vec<u8>>, Error> {
        self.write_inventory_to_lines(inv, working)
    }

    /// Serialize the inventory to a single byte string.
    fn write_inventory_to_string(
        &self,
        inv: &MutableInventory,
        working: bool,
    ) -> Result<Vec<u8>, Error> {
        let lines = self.write_inventory_to_lines(inv, working)?;
        let mut out = Vec::new();
        for line in lines {
            out.extend_from_slice(&line);
        }
        Ok(out)
    }

    /// Write the inventory directly to a writer.
    fn write_inventory(
        &self,
        inv: &MutableInventory,
        f: &mut dyn std::io::Write,
        working: bool,
    ) -> Result<Vec<Vec<u8>>, Error> {
        let lines = self.write_inventory_to_lines(inv, working)?;
        for line in &lines {
            f.write_all(line)?;
        }
        Ok(lines)
    }

    /// Read an inventory from a sequence of byte-chunks (lines).
    fn read_inventory_from_lines(
        &self,
        lines: &[&[u8]],
        revision_id: Option<RevisionId>,
    ) -> Result<MutableInventory, Error>;

    /// Read an inventory from a reader.
    fn read_inventory(
        &self,
        f: &mut dyn Read,
        revision_id: Option<RevisionId>,
    ) -> Result<MutableInventory, Error> {
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        self.read_inventory_from_lines(&[buf.as_slice()], revision_id)
    }
}
