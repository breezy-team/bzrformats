use crate::RevisionId;
use chrono::{DateTime, NaiveDateTime};
use std::collections::HashMap;

pub fn validate_properties(properties: &HashMap<String, Vec<u8>>) -> bool {
    for (key, _value) in properties.iter() {
        if crate::osutils::contains_whitespace(key.as_str()) {
            return false;
        }
    }
    true
}

#[derive(Clone, Debug, PartialEq)]
pub struct Revision {
    pub revision_id: RevisionId,
    pub parent_ids: Vec<RevisionId>,
    pub committer: Option<String>,
    pub message: String,
    pub properties: HashMap<String, Vec<u8>>,
    pub inventory_sha1: Option<Vec<u8>>,
    pub timestamp: f64,
    pub timezone: Option<i32>,
}

impl Revision {
    pub fn new(
        revision_id: RevisionId,
        parent_ids: Vec<RevisionId>,
        committer: Option<String>,
        message: String,
        properties: HashMap<String, Vec<u8>>,
        inventory_sha1: Option<Vec<u8>>,
        timestamp: f64,
        timezone: Option<i32>,
    ) -> Self {
        Revision {
            revision_id,
            parent_ids,
            committer,
            message,
            properties,
            inventory_sha1,
            timestamp,
            timezone,
        }
    }

    pub fn datetime(&self) -> NaiveDateTime {
        DateTime::from_timestamp(self.timestamp as i64, 0)
            .expect("timestamp should be valid")
            .naive_utc()
    }

    pub fn timezone(&self) -> Option<chrono::FixedOffset> {
        self.timezone
            .map(|t| chrono::FixedOffset::east_opt(t).unwrap())
    }

    pub fn check_properties(&self) -> bool {
        validate_properties(&self.properties)
    }

    pub fn get_summary(&self) -> String {
        if self.message.is_empty() {
            String::new()
        } else {
            let mut summary = self.message.trim().lines().next().unwrap().to_string();
            summary = summary.trim().to_string();
            summary
        }
    }

    fn get_property_as_str(&self, key: &str) -> Option<String> {
        self.properties
            .get(key)
            .map(|x| String::from_utf8_lossy(x).to_string())
    }

    /// Return the apparent authors of this revision.
    ///
    /// If the revision properties contain the names of the authors,
    /// return them. Otherwise return the committer name.
    ///
    /// The return value will be a list containing at least one element.
    pub fn get_apparent_authors(&self) -> Vec<String> {
        let authors = match self.get_property_as_str("authors") {
            Some(authors) => {
                let authors = authors.split('\n').collect::<Vec<&str>>();
                authors.iter().map(|x| x.to_string()).collect()
            }
            None => self.get_property_as_str("author").map_or(
                self.committer.clone().map_or(vec![], |v| vec![v]),
                |author| vec![author],
            ),
        };

        authors.into_iter().filter(|x| !x.is_empty()).collect()
    }

    pub fn bug_urls(&self) -> Vec<String> {
        self.get_property_as_str("bugs").map_or(vec![], |bugs| {
            bugs.split('\n').map(|x| x.to_string()).collect()
        })
    }

    /// Decode the ``bugs`` property as `(url, status)` pairs.
    ///
    /// Each non-empty line of the property must contain a URL and a status
    /// word separated by whitespace. The status must be one of the values in
    /// [`BUG_STATUSES`]; malformed lines and unknown statuses are returned as
    /// structured errors so the caller can map them onto the appropriate
    /// exception class.
    pub fn iter_bugs(&self) -> Result<Vec<(String, String)>, BugError> {
        let mut out = Vec::new();
        for line in self.bug_urls() {
            if line.is_empty() {
                continue;
            }
            let mut parts = line.split_whitespace();
            let url = match parts.next() {
                Some(u) => u.to_string(),
                None => return Err(BugError::InvalidLine(line)),
            };
            let status = match parts.next() {
                Some(s) => s.to_string(),
                None => return Err(BugError::InvalidLine(line)),
            };
            if parts.next().is_some() {
                return Err(BugError::InvalidLine(line));
            }
            if !BUG_STATUSES.contains(&status.as_str()) {
                return Err(BugError::InvalidStatus(status));
            }
            out.push((url, status));
        }
        Ok(out)
    }
}

/// Status values accepted in the ``bugs`` revision property.
pub const BUG_STATUSES: &[&str] = &["fixed", "related"];

/// Reasons [`Revision::iter_bugs`] can reject a ``bugs`` property line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BugError {
    /// A line did not contain exactly a URL and a status token.
    InvalidLine(String),
    /// The status token was not one of [`BUG_STATUSES`].
    InvalidStatus(String),
}

impl std::fmt::Display for Revision {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Revision({})", self.revision_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make(message: &str, committer: Option<&str>, props: &[(&str, &str)]) -> Revision {
        let properties = props
            .iter()
            .map(|(k, v)| (k.to_string(), v.as_bytes().to_vec()))
            .collect();
        Revision::new(
            RevisionId::from(b"1".to_vec()),
            vec![],
            committer.map(str::to_string),
            message.to_string(),
            properties,
            None,
            0.0,
            Some(0),
        )
    }

    #[test]
    fn get_summary_takes_first_line_of_message() {
        assert_eq!(make("a", Some(""), &[]).get_summary(), "a");
        assert_eq!(make("a\nb", Some(""), &[]).get_summary(), "a");
        assert_eq!(make("\na\nb", Some(""), &[]).get_summary(), "a");
    }

    #[test]
    fn get_summary_empty_message_is_empty() {
        assert_eq!(make("", Some(""), &[]).get_summary(), "");
    }

    #[test]
    fn get_apparent_authors_falls_back_to_committer() {
        assert_eq!(
            make("", Some("A"), &[]).get_apparent_authors(),
            vec!["A".to_string()]
        );
    }

    #[test]
    fn get_apparent_authors_prefers_author_property() {
        assert_eq!(
            make("", Some("A"), &[("author", "B")]).get_apparent_authors(),
            vec!["B".to_string()]
        );
    }

    #[test]
    fn get_apparent_authors_prefers_authors_list_property() {
        assert_eq!(
            make("", Some("A"), &[("author", "B"), ("authors", "C\nD")]).get_apparent_authors(),
            vec!["C".to_string(), "D".to_string()]
        );
    }

    #[test]
    fn get_apparent_authors_empty_committer_returns_empty_list() {
        assert_eq!(
            make("", Some(""), &[]).get_apparent_authors(),
            Vec::<String>::new()
        );
    }

    #[test]
    fn iter_bugs_returns_empty_when_property_absent() {
        assert_eq!(make("", Some("A"), &[]).iter_bugs(), Ok(vec![]));
    }

    #[test]
    fn iter_bugs_parses_url_status_pairs() {
        let rev = make(
            "",
            Some("A"),
            &[(
                "bugs",
                "http://example.com/1 fixed\nhttp://example.com/2 related",
            )],
        );
        assert_eq!(
            rev.iter_bugs(),
            Ok(vec![
                ("http://example.com/1".to_string(), "fixed".to_string()),
                ("http://example.com/2".to_string(), "related".to_string()),
            ])
        );
    }

    #[test]
    fn iter_bugs_rejects_unknown_status() {
        assert_eq!(
            make("", Some("A"), &[("bugs", "http://example.com/1 wontfix")]).iter_bugs(),
            Err(BugError::InvalidStatus("wontfix".to_string()))
        );
    }

    #[test]
    fn iter_bugs_rejects_line_without_status() {
        assert_eq!(
            make("", Some("A"), &[("bugs", "http://example.com/1")]).iter_bugs(),
            Err(BugError::InvalidLine("http://example.com/1".to_string()))
        );
    }

    #[test]
    fn iter_bugs_rejects_line_with_extra_token() {
        assert_eq!(
            make(
                "",
                Some("A"),
                &[("bugs", "http://example.com/1 fixed extra")]
            )
            .iter_bugs(),
            Err(BugError::InvalidLine(
                "http://example.com/1 fixed extra".to_string()
            ))
        );
    }

    #[test]
    fn iter_bugs_skips_blank_lines() {
        assert_eq!(
            make("", Some("A"), &[("bugs", "\nhttp://example.com/1 fixed\n")]).iter_bugs(),
            Ok(vec![(
                "http://example.com/1".to_string(),
                "fixed".to_string()
            )])
        );
    }

    #[test]
    fn bug_urls_splits_on_newlines() {
        let rev = make(
            "",
            Some("A"),
            &[("bugs", "http://a fixed\nhttp://b related")],
        );
        assert_eq!(
            rev.bug_urls(),
            vec!["http://a fixed".to_string(), "http://b related".to_string()]
        );
    }

    #[test]
    fn bug_urls_empty_when_property_absent() {
        assert_eq!(make("", Some("A"), &[]).bug_urls(), Vec::<String>::new());
    }

    #[test]
    fn check_properties_rejects_whitespace_in_keys() {
        assert!(make("", Some("A"), &[("good-key", "v")]).check_properties());
        assert!(!make("", Some("A"), &[("bad key", "v")]).check_properties());
    }

    #[test]
    fn datetime_reflects_timestamp() {
        let mut rev = make("", Some("A"), &[]);
        rev.timestamp = 1_000_000_000.0;
        // 2001-09-09 01:46:40 UTC.
        assert_eq!(
            rev.datetime(),
            DateTime::from_timestamp(1_000_000_000, 0)
                .unwrap()
                .naive_utc()
        );
    }

    #[test]
    fn timezone_maps_to_fixed_offset() {
        let mut rev = make("", Some("A"), &[]);
        rev.timezone = Some(3600);
        assert_eq!(rev.timezone(), chrono::FixedOffset::east_opt(3600));
        rev.timezone = None;
        assert_eq!(rev.timezone(), None);
    }

    #[test]
    fn display_shows_revision_id() {
        let rev = make("msg", Some("A"), &[]);
        assert_eq!(format!("{}", rev), "Revision(1)");
    }
}
