//! The config option registry and value converters, ported from the `Option`
//! machinery in `breezy/config.py`.
//!
//! An [`Option`] carries a name, an optional default, and an optional
//! `from_unicode` converter that turns the on-disk string into a validated
//! value (booleans, integers, SI sizes, lists). A [`super::Stack`] looks an
//! option up by name to decide how to unquote/convert a raw value and what
//! default to fall back to.

use std::collections::BTreeMap;

/// How a registered option converts its on-disk string value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Converter {
    /// No conversion: the unquoted string is the value.
    None,
    /// A boolean, via [`bool_from_store`]. Invalid input yields no value.
    Bool,
    /// A base-10 integer, via [`int_from_store`].
    Int,
    /// An SI-suffixed size (`K`/`M`/`G`), via [`int_si_from_store`].
    IntSi,
    /// A comma-separated list, via [`list_from_store`]. The converted form is
    /// re-joined with commas (the stack returns a single string; callers that
    /// need the elements split can re-split on commas).
    List,
}

/// A registered configuration option: its name, default, and converter.
#[derive(Clone, Debug)]
pub struct Option {
    name: String,
    default: std::option::Option<String>,
    converter: Converter,
}

impl Option {
    /// A plain string option with an optional default and no conversion.
    pub fn string(name: &str, default: std::option::Option<&str>) -> Self {
        Option {
            name: name.to_string(),
            default: default.map(|s| s.to_string()),
            converter: Converter::None,
        }
    }

    /// An option with a specific converter.
    pub fn with_converter(
        name: &str,
        default: std::option::Option<&str>,
        converter: Converter,
    ) -> Self {
        Option {
            name: name.to_string(),
            default: default.map(|s| s.to_string()),
            converter,
        }
    }

    /// The option name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The default value, if any.
    pub fn default(&self) -> std::option::Option<&str> {
        self.default.as_deref()
    }

    /// The converter this option uses to interpret its on-disk value.
    pub fn converter(&self) -> Converter {
        self.converter
    }

    /// Convert an already-unquoted on-disk value per the option's converter.
    ///
    /// Returns `None` when the converter rejects the input (e.g. a non-boolean
    /// for a boolean option), mirroring breezy catching `ValueError`/`TypeError`
    /// and treating the option as unset.
    pub fn convert_from_unicode(&self, value: &str) -> std::option::Option<String> {
        match self.converter {
            Converter::None => Some(value.to_string()),
            Converter::Bool => {
                bool_from_store(value).map(|b| if b { "True" } else { "False" }.to_string())
            }
            Converter::Int => int_from_store(value).map(|i| i.to_string()),
            Converter::IntSi => int_si_from_store(value).map(|i| i.to_string()),
            Converter::List => Some(list_from_store(value).join(",")),
        }
    }
}

/// A registry of [`Option`]s, looked up by name.
#[derive(Clone, Debug, Default)]
pub struct OptionRegistry {
    options: BTreeMap<String, Option>,
}

impl OptionRegistry {
    /// An empty registry.
    pub fn new() -> Self {
        OptionRegistry::default()
    }

    /// A registry pre-populated with the options breezy declares in its global
    /// `option_registry` (`breezy/config.py`) that have static defaults.
    ///
    /// Options this registry cannot describe are not registered here; breezy
    /// supplies those on its side when it composes the global stores. That
    /// covers a default computed by a callable (email address, mail client,
    /// SSL cert paths, orphan policy), a value taken from the environment
    /// (`ssh`, `progress_bar`) and a converter with no equivalent here (the
    /// signature policies, the float `serve.client_timeout`).
    pub fn with_defaults() -> Self {
        let mut r = OptionRegistry::new();
        r.register(Option::string("stacked_on_location", None));
        r.register(Option::string("bound_location", None));
        r.register(Option::with_converter("bound", None, Converter::Bool));
        r.register(Option::string("parent_location", None));
        r.register(Option::string("push_location", None));
        r.register(Option::string("public_branch", None));
        r.register(Option::string("submit_branch", None));
        r.register(Option::string("nickname", None));
        r.register(Option::string("child_submit_format", None));
        r.register(Option::string("child_submit_to", None));
        r.register(Option::string("submit_to", None));
        r.register(Option::with_converter(
            "append_revisions_only",
            None,
            Converter::Bool,
        ));
        r.register(Option::with_converter(
            "branch.fetch_tags",
            Some("False"),
            Converter::Bool,
        ));
        r.register(Option::with_converter("push_strict", None, Converter::Bool));
        r.register(Option::with_converter("send_strict", None, Converter::Bool));

        r.register(Option::with_converter(
            "bzr.workingtree.worth_saving_limit",
            Some("10"),
            Converter::Int,
        ));
        r.register(Option::with_converter(
            "dirstate.fdatasync",
            Some("True"),
            Converter::Bool,
        ));
        r.register(Option::with_converter(
            "repository.fdatasync",
            Some("True"),
            Converter::Bool,
        ));
        r.register(Option::with_converter(
            "add.maximum_file_size",
            Some("20MB"),
            Converter::IntSi,
        ));

        r.register(Option::with_converter(
            "calculate_revnos",
            Some("True"),
            Converter::Bool,
        ));
        r.register(Option::string("log_format", Some("long")));
        r.register(Option::with_converter(
            "validate_signatures_in_log",
            Some("False"),
            Converter::Bool,
        ));

        r.register(Option::with_converter(
            "acceptable_keys",
            None,
            Converter::List,
        ));
        r.register(Option::string("gpg_signing_key", None));

        r.register(Option::with_converter(
            "locks.steal_dead",
            Some("True"),
            Converter::Bool,
        ));
        r.register(Option::string("default_format", Some("2a")));
        r.register(Option::string("post_commit", None));

        r.register(Option::string("editor", None));
        r.register(Option::string("language", None));
        r.register(Option::string("output_encoding", None));
        r.register(Option::string("bugtracker", None));

        r.register(Option::string("smtp_server", None));
        r.register(Option::string("smtp_username", None));
        r.register(Option::string("smtp_password", None));

        r.register(Option::with_converter(
            "selftest.timeout",
            Some("1200"),
            Converter::Int,
        ));

        r
    }

    /// Register `option`, replacing any existing one with the same name.
    pub fn register(&mut self, option: Option) {
        self.options.insert(option.name.clone(), option);
    }

    /// The option registered under `name`, if any.
    pub fn get(&self, name: &str) -> std::option::Option<&Option> {
        self.options.get(name)
    }
}

/// Parse a boolean the way breezy's `bool_from_string` does: a case-insensitive
/// match against a fixed set of truthy/falsey words. Anything else is `None`
/// (treated as invalid by callers).
pub fn bool_from_store(s: &str) -> std::option::Option<bool> {
    match s.trim().to_ascii_lowercase().as_str() {
        "yes" | "y" | "on" | "true" | "1" => Some(true),
        "no" | "n" | "off" | "false" | "0" => Some(false),
        _ => None,
    }
}

/// Parse a base-10 integer, returning `None` on bad input.
pub fn int_from_store(s: &str) -> std::option::Option<i64> {
    s.trim().parse().ok()
}

/// Parse an SI-suffixed size: digits with an optional `K`/`M`/`G` suffix
/// (×10^3 / 10^6 / 10^9), optionally followed by `b`. SI is base-10, not
/// binary. Returns `None` if the input does not match.
pub fn int_si_from_store(s: &str) -> std::option::Option<i64> {
    let s = s.trim();
    let (digits, rest) = s
        .find(|c: char| !c.is_ascii_digit())
        .map(|i| (&s[..i], &s[i..]))
        .unwrap_or((s, ""));
    if digits.is_empty() {
        return None;
    }
    let base: i64 = digits.parse().ok()?;
    let rest = rest.strip_suffix('b').or(Some(rest)).unwrap_or(rest);
    let rest = rest.strip_suffix('B').unwrap_or(rest);
    let mult = match rest.to_ascii_uppercase().as_str() {
        "" => 1,
        "K" => 1_000,
        "M" => 1_000_000,
        "G" => 1_000_000_000,
        _ => return None,
    };
    Some(base * mult)
}

/// Split a comma-separated list value into its elements, trimming whitespace.
/// An empty string is an empty list, as breezy's list conversion produces.
pub fn list_from_store(s: &str) -> Vec<String> {
    let s = s.trim();
    if s.is_empty() {
        return Vec::new();
    }
    s.split(',')
        .map(|item| item.trim().to_string())
        .filter(|item| !item.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bool_from_store_accepts_breezy_spellings() {
        for t in ["yes", "Y", "on", "True", "1"] {
            assert_eq!(bool_from_store(t), Some(true), "{t}");
        }
        for f in ["no", "N", "off", "False", "0"] {
            assert_eq!(bool_from_store(f), Some(false), "{f}");
        }
        assert_eq!(bool_from_store("maybe"), None);
    }

    #[test]
    fn int_si_from_store_scales() {
        assert_eq!(int_si_from_store("20"), Some(20));
        assert_eq!(int_si_from_store("20K"), Some(20_000));
        assert_eq!(int_si_from_store("20MB"), Some(20_000_000));
        assert_eq!(int_si_from_store("1G"), Some(1_000_000_000));
        assert_eq!(int_si_from_store("xyz"), None);
        assert_eq!(int_si_from_store("20T"), None);
    }

    #[test]
    fn list_from_store_splits() {
        assert_eq!(list_from_store(""), Vec::<String>::new());
        assert_eq!(list_from_store("a"), vec!["a".to_string()]);
        assert_eq!(
            list_from_store("a, b ,c"),
            vec!["a".to_string(), "b".to_string(), "c".to_string()]
        );
    }

    #[test]
    fn registry_defaults_present() {
        let r = OptionRegistry::with_defaults();
        assert_eq!(
            r.get("default_format").and_then(|o| o.default()),
            Some("2a")
        );
        assert!(r.get("stacked_on_location").unwrap().default().is_none());
        assert_eq!(r.get("bound").unwrap().converter, Converter::Bool);
    }

    #[test]
    fn registry_static_defaults() {
        let r = OptionRegistry::with_defaults();
        let fetch_tags = r.get("branch.fetch_tags").unwrap();
        assert_eq!(fetch_tags.default(), Some("False"));
        assert_eq!(fetch_tags.converter(), Converter::Bool);
        let max_size = r.get("add.maximum_file_size").unwrap();
        assert_eq!(max_size.default(), Some("20MB"));
        assert_eq!(max_size.converter(), Converter::IntSi);
        let timeout = r.get("selftest.timeout").unwrap();
        assert_eq!(timeout.default(), Some("1200"));
        assert_eq!(timeout.converter(), Converter::Int);
        assert_eq!(
            r.get("acceptable_keys").unwrap().converter(),
            Converter::List
        );
        // Options the registry cannot describe are left to breezy.
        for name in ["email", "ssh", "check_signatures", "serve.client_timeout"] {
            assert!(r.get(name).is_none(), "{} is registered", name);
        }
    }

    #[test]
    fn convert_bool_normalizes() {
        let o = Option::with_converter("bound", None, Converter::Bool);
        assert_eq!(o.convert_from_unicode("yes").as_deref(), Some("True"));
        assert_eq!(o.convert_from_unicode("0").as_deref(), Some("False"));
        assert_eq!(o.convert_from_unicode("bogus"), None);
    }

    #[test]
    fn name_and_default_round_trip() {
        let o = Option::string("push_location", Some("../trunk"));
        assert_eq!(o.name(), "push_location");
        assert_eq!(o.default(), Some("../trunk"));
        let o = Option::string("nickname", None);
        assert_eq!(o.name(), "nickname");
        assert_eq!(o.default(), None);
    }

    #[test]
    fn int_from_store_parses_and_rejects() {
        assert_eq!(int_from_store("42"), Some(42));
        assert_eq!(int_from_store(" -7 "), Some(-7));
        assert_eq!(int_from_store("0"), Some(0));
        assert_eq!(int_from_store("not a number"), None);
        assert_eq!(int_from_store(""), None);
    }

    #[test]
    fn convert_int_uses_int_from_store() {
        let o = Option::with_converter("n", None, Converter::Int);
        assert_eq!(o.convert_from_unicode("42").as_deref(), Some("42"));
        assert_eq!(o.convert_from_unicode("nope"), None);
    }
}
