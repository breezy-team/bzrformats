//! Breezy's configuration system, ported from `breezy/config.py`.
//!
//! The stack has four layers, bottom to top:
//!
//! * [`ConfigObj`] -- a parser/writer for the ConfigObj INI dialect breezy
//!   uses on disk (UTF-8, `list_values=False`, interpolation off). It preserves
//!   section/key order and round-trips comments well enough that rewriting a
//!   file only touches the values that changed.
//! * [`Section`] / [`MutableSection`] -- a read or read/write view of one
//!   `{name: value}` section within a parsed file. The top-level scalars form
//!   the "no-name" section (`id == None`).
//! * [`Store`] (concretely [`IniFileStore`] and [`TransportIniFileStore`]) --
//!   persistence for a whole config file, handing out sections and writing
//!   changes back.
//! * [`Stack`] (concretely [`BranchStack`] and [`BranchOnlyStack`]) -- the
//!   option lookup chain: the first section that has a value wins, the value is
//!   unquoted and run through the registered [`Option`]'s converter, and a
//!   default is supplied if nothing matched.
//!
//! The home-directory stores (`bazaar.conf`, `locations.conf`) are not built
//! here: their on-disk locations come from breezy's `bedding` crate, which the
//! bazaar crate deliberately does not depend on. [`TransportIniFileStore`] is
//! generic over a transport and file name, so breezy can compose those stores
//! itself.

use std::collections::BTreeMap;

use crate::transport::{SharedTransport, TransportError};

mod configobj;
mod option;

pub use configobj::{ConfigObj, ConfigObjError};
pub use option::{
    bool_from_store, int_from_store, int_si_from_store, list_from_store, Option as ConfigOption,
    OptionRegistry,
};

/// Errors from the config layer.
#[derive(Debug)]
pub enum ConfigError {
    /// The file could not be parsed as ConfigObj.
    Parse(ConfigObjError),
    /// An underlying transport error.
    Transport(TransportError),
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::Parse(e) => write!(f, "config parse error: {e}"),
            ConfigError::Transport(e) => write!(f, "transport error: {e}"),
        }
    }
}

impl std::error::Error for ConfigError {}

impl From<ConfigObjError> for ConfigError {
    fn from(e: ConfigObjError) -> Self {
        ConfigError::Parse(e)
    }
}

impl From<TransportError> for ConfigError {
    fn from(e: TransportError) -> Self {
        ConfigError::Transport(e)
    }
}

/// A read view of one config section: an ordered `{name: value}` map plus the
/// section id (`None` for the top-level no-name section).
///
/// Values are the raw strings the parser produced (with `list_values=False`,
/// surrounding quotes are kept); a [`Stack`] unquotes them on read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Section {
    id: Option<String>,
    options: BTreeMap<String, String>,
    order: Vec<String>,
}

impl Section {
    fn new(id: Option<String>, options: Vec<(String, String)>) -> Self {
        let mut map = BTreeMap::new();
        let mut order = Vec::with_capacity(options.len());
        for (k, v) in options {
            if !map.contains_key(&k) {
                order.push(k.clone());
            }
            map.insert(k, v);
        }
        Section {
            id,
            options: map,
            order,
        }
    }

    /// The section id; `None` for the no-name (top-level) section.
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    /// The raw value for `name`, if present.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.options.get(name).map(|s| s.as_str())
    }

    /// Option names in file order.
    pub fn iter_option_names(&self) -> impl Iterator<Item = &str> {
        self.order.iter().map(|s| s.as_str())
    }
}

/// A read/write view of a section, tracking which keys changed so a store can
/// rewrite only those.
#[derive(Debug, Clone)]
pub struct MutableSection {
    section: Section,
    /// Keys touched since the section was handed out, mapped to whether the
    /// change was a removal (`true`) or a set (`false`). Mirrors breezy's
    /// `orig` tracking closely enough to know what to write back.
    dirty: BTreeMap<String, bool>,
}

impl MutableSection {
    fn new(section: Section) -> Self {
        MutableSection {
            section,
            dirty: BTreeMap::new(),
        }
    }

    /// The section id; `None` for the no-name (top-level) section.
    pub fn id(&self) -> Option<&str> {
        self.section.id()
    }

    /// The raw value for `name`, if present.
    pub fn get(&self, name: &str) -> Option<&str> {
        self.section.get(name)
    }

    /// Set `name` to `value`.
    pub fn set(&mut self, name: &str, value: &str) {
        if !self.section.options.contains_key(name) {
            self.section.order.push(name.to_string());
        }
        self.section
            .options
            .insert(name.to_string(), value.to_string());
        self.dirty.insert(name.to_string(), false);
    }

    /// Remove `name`.
    pub fn remove(&mut self, name: &str) {
        if self.section.options.remove(name).is_some() {
            self.section.order.retain(|k| k != name);
            self.dirty.insert(name.to_string(), true);
        }
    }

    /// Whether any key changed.
    pub fn is_dirty(&self) -> bool {
        !self.dirty.is_empty()
    }
}

/// Persistence for one config file: parse it, hand out sections, write changes.
///
/// This is the in-memory base; [`TransportIniFileStore`] adds load/save over a
/// [`SharedTransport`].
pub struct IniFileStore {
    config_obj: Option<ConfigObj>,
}

impl Default for IniFileStore {
    fn default() -> Self {
        Self::new()
    }
}

impl IniFileStore {
    /// An empty, unloaded store.
    pub fn new() -> Self {
        IniFileStore { config_obj: None }
    }

    /// Whether the file has been parsed into memory.
    pub fn is_loaded(&self) -> bool {
        self.config_obj.is_some()
    }

    /// Parse `bytes` as the store's content, replacing any loaded state.
    pub fn load_from_bytes(&mut self, bytes: &[u8]) -> Result<(), ConfigError> {
        self.config_obj = Some(ConfigObj::parse(bytes)?);
        Ok(())
    }

    /// Serialize the loaded content back to bytes, or an empty vec if nothing
    /// is loaded.
    pub fn to_bytes(&self) -> Vec<u8> {
        match &self.config_obj {
            Some(c) => c.to_bytes(),
            None => Vec::new(),
        }
    }

    /// All sections in file order: the no-name section first (if it has any
    /// scalars), then each named section.
    pub fn get_sections(&self) -> Vec<Section> {
        match &self.config_obj {
            Some(c) => c.sections(),
            None => Vec::new(),
        }
    }

    /// The section with the given id (`None` for the no-name section) as a
    /// mutable view; an empty section if absent.
    pub fn get_mutable_section(&mut self, section_id: Option<&str>) -> MutableSection {
        let config = self.config_obj.get_or_insert_with(ConfigObj::empty);
        let section = config
            .section(section_id)
            .unwrap_or_else(|| Section::new(section_id.map(|s| s.to_string()), Vec::new()));
        MutableSection::new(section)
    }

    /// Apply a mutable section's changes back into the loaded content.
    pub fn apply_changes(&mut self, section: &MutableSection) {
        let config = self.config_obj.get_or_insert_with(ConfigObj::empty);
        for (key, removed) in &section.dirty {
            if *removed {
                config.remove_value(section.id(), key);
            } else if let Some(value) = section.get(key) {
                config.set_value(section.id(), key, value);
            }
        }
    }

    /// The store's quoting of a value for writing (list-aware, as breezy's
    /// `Store.quote`).
    pub fn quote(&self, value: &str) -> String {
        configobj::quote_value(value)
    }

    /// The store's unquoting of a raw value read from a section.
    pub fn unquote(&self, value: &str) -> String {
        configobj::unquote_value(value)
    }
}

/// An [`IniFileStore`] backed by a file on a [`SharedTransport`].
pub struct TransportIniFileStore {
    store: IniFileStore,
    transport: SharedTransport,
    file_name: String,
}

impl TransportIniFileStore {
    /// A store for `file_name` reached through `transport`.
    pub fn new(transport: SharedTransport, file_name: impl Into<String>) -> Self {
        TransportIniFileStore {
            store: IniFileStore::new(),
            transport,
            file_name: file_name.into(),
        }
    }

    /// Load the file into memory; a missing file loads as empty content.
    pub fn load(&mut self) -> Result<(), ConfigError> {
        if self.store.is_loaded() {
            return Ok(());
        }
        let bytes = match self.transport.get_bytes(&self.file_name) {
            Ok(b) => b,
            Err(TransportError::NoSuchFile(_)) => Vec::new(),
            Err(e) => return Err(e.into()),
        };
        self.store.load_from_bytes(&bytes)
    }

    /// Write the loaded content back to the file.
    pub fn save(&self) -> Result<(), ConfigError> {
        if !self.store.is_loaded() {
            return Ok(());
        }
        self.transport
            .put_bytes(&self.file_name, &self.store.to_bytes(), None)?;
        Ok(())
    }

    /// The underlying [`IniFileStore`].
    pub fn store(&self) -> &IniFileStore {
        &self.store
    }

    /// The underlying [`IniFileStore`], mutably.
    pub fn store_mut(&mut self) -> &mut IniFileStore {
        &mut self.store
    }
}

/// The option lookup chain over an ordered list of sections.
///
/// `get` returns the first section's value for an option, unquoted and run
/// through the registered converter, falling back to the option default. This
/// is the generic engine; [`BranchStack`] and [`BranchOnlyStack`] supply the
/// section order.
pub struct Stack<'r> {
    sections: Vec<Section>,
    registry: &'r OptionRegistry,
}

impl<'r> Stack<'r> {
    /// A stack over `sections` (consulted in order) using `registry` for option
    /// metadata and conversion.
    pub fn new(sections: Vec<Section>, registry: &'r OptionRegistry) -> Self {
        Stack { sections, registry }
    }

    /// The raw (still-quoted) value for `name` from the first section that has
    /// it, ignoring option defaults.
    fn get_raw(&self, name: &str) -> Option<&str> {
        self.sections.iter().find_map(|s| s.get(name))
    }

    /// The converted value for `name`: the first matching section's value,
    /// unquoted and converted per the option registry, else the option's
    /// default.
    ///
    /// Returns `None` when neither a section nor a default supplies a value.
    pub fn get(&self, name: &str) -> Option<String> {
        let opt = self.registry.get(name);
        if let Some(raw) = self.get_raw(name) {
            let unquoted = configobj::unquote_value(raw);
            return match opt {
                Some(o) => o.convert_from_unicode(&unquoted),
                None => Some(unquoted),
            };
        }
        opt.and_then(|o| o.default().map(|s| s.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::{LocalTransport, Transport};
    use std::sync::Arc;

    #[test]
    fn ini_file_store_round_trips_no_name_section() {
        let mut store = IniFileStore::new();
        store
            .load_from_bytes(b"stacked_on_location = ../parent\nnickname = trunk\n")
            .unwrap();
        let sections = store.get_sections();
        assert_eq!(sections.len(), 1);
        assert_eq!(sections[0].id(), None);
        assert_eq!(sections[0].get("stacked_on_location"), Some("../parent"));
        assert_eq!(sections[0].get("nickname"), Some("trunk"));
    }

    #[test]
    fn set_value_preserves_other_keys() {
        let mut store = IniFileStore::new();
        store.load_from_bytes(b"nickname = trunk\n").unwrap();
        let mut sec = store.get_mutable_section(None);
        sec.set("bound", "True");
        store.apply_changes(&sec);
        let reloaded = store.get_sections();
        assert_eq!(reloaded[0].get("nickname"), Some("trunk"));
        assert_eq!(reloaded[0].get("bound"), Some("True"));
    }

    #[test]
    fn remove_value_drops_key() {
        let mut store = IniFileStore::new();
        store
            .load_from_bytes(b"nickname = trunk\nbound = True\n")
            .unwrap();
        let mut sec = store.get_mutable_section(None);
        sec.remove("bound");
        store.apply_changes(&sec);
        assert_eq!(store.get_sections()[0].get("bound"), None);
        assert_eq!(store.get_sections()[0].get("nickname"), Some("trunk"));
    }

    #[test]
    fn transport_store_loads_missing_file_as_empty() {
        let dir = tempfile::tempdir().unwrap();
        let transport: SharedTransport = Arc::new(LocalTransport::new(dir.path()));
        let mut store = TransportIniFileStore::new(transport, "branch.conf");
        store.load().unwrap();
        assert!(store.store().get_sections().is_empty());
    }

    #[test]
    fn transport_store_save_then_reload() {
        let dir = tempfile::tempdir().unwrap();
        let transport: SharedTransport = Arc::new(LocalTransport::new(dir.path()));
        let probe = Arc::new(LocalTransport::new(dir.path()));
        let mut store = TransportIniFileStore::new(transport, "branch.conf");
        store.load().unwrap();
        let mut sec = store.store_mut().get_mutable_section(None);
        sec.set("stacked_on_location", "../base");
        store.store_mut().apply_changes(&sec);
        store.save().unwrap();
        let on_disk = probe.get_bytes("branch.conf").unwrap();
        assert_eq!(on_disk, b"stacked_on_location = ../base\n");
    }

    #[test]
    fn stack_returns_first_section_match() {
        let registry = OptionRegistry::with_defaults();
        let sections = vec![
            Section::new(
                Some("/path".to_string()),
                vec![("nickname".to_string(), "specific".to_string())],
            ),
            Section::new(None, vec![("nickname".to_string(), "generic".to_string())]),
        ];
        let stack = Stack::new(sections, &registry);
        assert_eq!(stack.get("nickname").as_deref(), Some("specific"));
    }

    #[test]
    fn stack_falls_back_to_default() {
        let registry = OptionRegistry::with_defaults();
        let stack = Stack::new(vec![], &registry);
        // default_format is registered with default "2a".
        assert_eq!(stack.get("default_format").as_deref(), Some("2a"));
        // stacked_on_location has no default -> None.
        assert_eq!(stack.get("stacked_on_location"), None);
    }
}
