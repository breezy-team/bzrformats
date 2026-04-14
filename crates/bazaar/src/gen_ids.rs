use lazy_regex::regex;
use lazy_static::lazy_static;
use osutils::rand_chars;
use regex::bytes::Regex;
use std::time::{SystemTime, UNIX_EPOCH};

lazy_static! {
    // the regex removes any weird characters; we don't escape them
    // but rather just pull them out

    static ref FILE_ID_CHARS_RE: Regex = Regex::new(r#"[^\w.]"#).unwrap();
    static ref REV_ID_CHARS_RE: Regex = Regex::new(r#"[^-\w.+@]"#).unwrap();
    static ref GEN_FILE_ID_SUFFIX: String = gen_file_id_suffix();
}

fn gen_file_id_suffix() -> String {
    let current_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let random_chars = rand_chars(16);
    format!(
        "-{}-{}-",
        osutils::time::compact_date(current_time),
        random_chars
    )
}

pub fn next_id_suffix(suffix: Option<&str>) -> Vec<u8> {
    static GEN_FILE_ID_SERIAL: std::sync::atomic::AtomicUsize =
        std::sync::atomic::AtomicUsize::new(0);

    // XXX TODO: change breezy.add.smart_add_tree to call workingtree.add() rather
    // than having to move the id randomness out of the inner loop like this.
    // XXX TODO: for the global randomness this uses we should add the thread-id
    // before the serial #.
    // XXX TODO: jam 20061102 I think it would be good to reset every 100 or
    //           1000 calls, or perhaps if time.time() increases by a certain
    //           amount. time.time() shouldn't be terribly expensive to call,
    //           and it means that long-lived processes wouldn't use the same
    //           suffix forever.
    let serial = GEN_FILE_ID_SERIAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!(
        "{}{}",
        suffix.unwrap_or(GEN_FILE_ID_SUFFIX.as_str()),
        serial
    )
    .into_bytes()
}

pub fn gen_file_id(name: &str) -> Vec<u8> {
    // The real randomness is in the _next_id_suffix, the
    // rest of the identifier is just to be nice.
    // So we:
    // 1) Remove non-ascii word characters to keep the ids portable
    // 2) squash to lowercase, so the file id doesn't have to
    //    be escaped (case insensitive filesystems would bork for ids
    //    that only differ in case without escaping).
    // 3) truncate the filename to 20 chars. Long filenames also bork on some
    // filesystems
    // 4) Removing starting '.' characters to prevent the file ids from
    //    being considered hidden.

    let name_bytes = name
        .chars()
        .filter(|c| c.is_ascii())
        .collect::<String>()
        .to_ascii_lowercase()
        .as_bytes()
        .to_vec();
    let ascii_word_only = FILE_ID_CHARS_RE
        .replace_all(&name_bytes, |_: &regex::bytes::Captures| b"")
        .to_vec();
    let without_dots = ascii_word_only
        .into_iter()
        .skip_while(|c| *c == b'.')
        .collect::<Vec<u8>>();
    let short = without_dots.iter().take(20).cloned().collect::<Vec<u8>>();
    let suffix = next_id_suffix(None);
    [short, suffix].concat()
}

pub fn gen_root_id() -> Vec<u8> {
    gen_file_id("tree_root")
}

fn get_identifier(s: &str) -> Vec<u8> {
    let mut identifier = s.to_string();
    if let Some(start) = s.find('<') {
        let end = s.rfind('>');
        if end.is_some()
            && start < end.unwrap()
            && end.unwrap() == s.len() - 1
            && s[start..].find('@').is_some()
        {
            identifier = s[start + 1..end.unwrap()].to_string();
        }
    }
    let identifier: String = identifier
        .to_ascii_lowercase()
        .replace(' ', "_")
        .chars()
        .filter(|c| c.is_ascii())
        .collect();
    REV_ID_CHARS_RE
        .replace_all(identifier.as_bytes(), |_: &regex::bytes::Captures| b"")
        .to_vec()
}

pub fn gen_revision_id(username: &str, timestamp: Option<u64>) -> Vec<u8> {
    let user_or_email = get_identifier(username);
    // This gives 36^16 ~= 2^82.7 ~= 83 bits of entropy
    let unique_chunk = osutils::rand_chars(16).as_bytes().to_vec();
    let timestamp = timestamp.unwrap_or_else(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    });
    [
        user_or_email,
        osutils::time::compact_date(timestamp).as_bytes().to_vec(),
        unique_chunk,
    ]
    .join(&b'-')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn starts_with(id: &[u8], prefix: &[u8]) -> bool {
        id.starts_with(prefix)
    }

    #[test]
    fn gen_file_id_preserves_filename_prefix() {
        assert!(starts_with(&gen_file_id("bar"), b"bar-"));
    }

    #[test]
    fn gen_file_id_squashes_case_and_strips_non_word_chars() {
        assert!(starts_with(&gen_file_id("Mwoo oof\t m"), b"mwoooofm-"));
    }

    #[test]
    fn gen_file_id_strips_leading_dots() {
        assert!(starts_with(&gen_file_id("..gam.py"), b"gam.py-"));
        assert!(starts_with(&gen_file_id("..Mwoo oof\t m"), b"mwoooofm-"));
    }

    #[test]
    fn gen_file_id_strips_non_ascii_and_avoids_hidden_id() {
        // "å ...txt" with non-ascii leading → only b"txt" survives
        assert!(starts_with(&gen_file_id("\u{e5}\u{b5}.txt"), b"txt-"));
    }

    #[test]
    fn gen_file_id_truncates_to_twenty_chars_lowercased() {
        let name: String = std::iter::repeat('A').take(50).collect::<String>() + ".txt";
        let fid = gen_file_id(&name);
        let expected_prefix: Vec<u8> = b"a".repeat(20);
        let mut expected = expected_prefix;
        expected.push(b'-');
        assert!(starts_with(&fid, &expected));
        assert!(fid.len() < 60);
    }

    #[test]
    fn gen_file_id_truncation_happens_after_other_steps() {
        let fid = gen_file_id("\u{e5}\u{b5}..aBcd\tefGhijKLMnop\tqrstuvwxyz");
        assert!(starts_with(&fid, b"abcdefghijklmnopqrst-"));
        assert!(fid.len() < 60);
    }

    #[test]
    fn next_id_suffix_increments_serial() {
        let ids: Vec<Vec<u8>> = (0..10).map(|_| next_id_suffix(Some("foo-"))).collect();
        let ns: Vec<i64> = ids
            .iter()
            .map(|id| {
                let s = std::str::from_utf8(id).unwrap();
                s.rsplit('-').next().unwrap().parse().unwrap()
            })
            .collect();
        // Serial is a process-global counter shared with gen_file_id, so
        // other tests running in parallel may interleave increments. Only
        // require that serials from this call are strictly increasing.
        for i in 1..ns.len() {
            assert!(ns[i] > ns[i - 1]);
        }
    }

    #[test]
    fn gen_root_id_starts_with_tree_root() {
        assert!(starts_with(&gen_root_id(), b"tree_root-"));
    }

    #[test]
    fn gen_revision_id_uses_explicit_timestamp() {
        let id = gen_revision_id("user@host", Some(1162500656));
        let s = std::str::from_utf8(&id).unwrap();
        assert!(s.starts_with("user@host-20061102205056-"));
    }

    #[test]
    fn gen_revision_id_extracts_email_from_angle_brackets() {
        for input in [
            "user+joe_bar@foo-bar.com",
            "<user+joe_bar@foo-bar.com>",
            "Joe Bar <user+joe_bar@foo-bar.com>",
            "Joe Bar <user+Joe_Bar@Foo-Bar.com>",
            "Joe B\u{e5}r <user+Joe_Bar@Foo-Bar.com>",
        ] {
            let id = gen_revision_id(input, Some(0));
            let s = std::str::from_utf8(&id).unwrap();
            assert!(
                s.starts_with("user+joe_bar@foo-bar.com-"),
                "expected email prefix for input {:?}, got {:?}",
                input,
                s
            );
        }
    }

    #[test]
    fn gen_revision_id_falls_back_to_full_username() {
        let id = gen_revision_id("Joe Bar", Some(0));
        let s = std::str::from_utf8(&id).unwrap();
        assert!(s.starts_with("joe_bar-"));

        // Non-ascii is stripped out of the identifier.
        let id = gen_revision_id("Joe B\u{e5}r", Some(0));
        let s = std::str::from_utf8(&id).unwrap();
        assert!(s.starts_with("joe_br-"));
    }

    #[test]
    fn gen_revision_id_always_returns_ascii() {
        let id = gen_revision_id("Joe Bar <joe@f\u{b6}>", Some(0));
        // Should still decode as ascii.
        let s = std::str::from_utf8(&id).unwrap();
        assert!(s.is_ascii());
        assert!(s.starts_with("joe@f-"));
    }
}
