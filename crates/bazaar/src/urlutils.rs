//! URL helpers, following `dromedary::urlutils` (and breezy's `urlutils`)
//! so the two parse URLs alike.

use std::collections::HashMap;

/// Why a URL could not be parsed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// A segment parameter has no `=`.
    SubsegmentMissesEquals(String),
    /// A percent-escaped component does not decode to UTF-8.
    NotUtf8(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::SubsegmentMissesEquals(subsegment) => {
                write!(f, "segment parameter {subsegment:?} has no '='")
            }
            Error::NotUtf8(text) => write!(f, "{text:?} does not decode to UTF-8"),
        }
    }
}

impl std::error::Error for Error {}

/// Split the comma-separated subsegments off the last segment of `url`:
/// `file:///a/b,branch=c` is `file:///a/b` with the subsegment `branch=c`.
pub fn split_segment_parameters_raw(url: &str) -> (&str, Vec<&str>) {
    let lurl = url.strip_suffix('/').unwrap_or(url);
    let segment_start = lurl.rfind('/').map_or(0, |i| i + 1);
    if !lurl[segment_start..].contains(',') {
        return (url, Vec::new());
    }
    let mut subsegments = lurl[segment_start..].split(',');
    let first = subsegments.next().unwrap_or_default();
    (
        &lurl[..segment_start + first.len()],
        subsegments.map(str::trim).collect(),
    )
}

/// Split the segment parameters off the last segment of `url`:
/// `file:///a/b,branch=c` is `file:///a/b` with `branch` set to `c`.
pub fn split_segment_parameters(url: &str) -> Result<(&str, HashMap<&str, &str>), Error> {
    let (base, subsegments) = split_segment_parameters_raw(url);
    let parameters = subsegments
        .into_iter()
        .map(|subsegment| {
            subsegment
                .split_once('=')
                .map(|(key, value)| (key.trim(), value.trim()))
                .ok_or_else(|| Error::SubsegmentMissesEquals(subsegment.to_string()))
        })
        .collect::<Result<_, _>>()?;
    Ok((base, parameters))
}

/// Undo the percent-escaping of a URL or one of its components.
pub fn unescape(text: &str) -> Result<String, Error> {
    percent_encoding::percent_decode_str(text)
        .decode_utf8()
        .map(|decoded| decoded.into_owned())
        .map_err(|_| Error::NotUtf8(text.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_segment_parameters_raw_cases() {
        assert_eq!(
            ("/some/path", vec![]),
            split_segment_parameters_raw("/some/path")
        );
        assert_eq!(
            ("/some/path", vec!["tip"]),
            split_segment_parameters_raw("/some/path,tip")
        );
        assert_eq!(
            ("/some,dir/path", vec!["tip"]),
            split_segment_parameters_raw("/some,dir/path,tip")
        );
        assert_eq!(
            ("/somedir/path", vec!["heads%2Ftip"]),
            split_segment_parameters_raw("/somedir/path,heads%2Ftip")
        );
        assert_eq!(
            ("/somedir/path", vec!["heads%2Ftip", "bar"]),
            split_segment_parameters_raw("/somedir/path,heads%2Ftip,bar")
        );
        assert_eq!(
            ("file:///a", vec!["branch=b"]),
            split_segment_parameters_raw("file:///a,branch=b/")
        );
    }

    #[test]
    fn split_segment_parameters_cases() {
        assert_eq!(
            ("/some/path", HashMap::new()),
            split_segment_parameters("/some/path").unwrap()
        );
        assert_eq!(
            ("/some/path", HashMap::from([("branch", "tip")])),
            split_segment_parameters("/some/path,branch=tip").unwrap()
        );
        assert_eq!(
            (
                "/some/path",
                HashMap::from([("key1", "val1"), ("key2", "val2")])
            ),
            split_segment_parameters("/some/path,key1=val1,key2=val2").unwrap()
        );
        assert_eq!(
            Err(Error::SubsegmentMissesEquals("tip".to_string())),
            split_segment_parameters("/some/path,tip")
        );
    }

    #[test]
    fn unescape_cases() {
        assert_eq!("tar get", unescape("tar%20get").unwrap());
        assert_eq!("caf\u{e9}", unescape("caf%C3%A9").unwrap());
        assert_eq!(Err(Error::NotUtf8("%FF".to_string())), unescape("%FF"));
    }
}
