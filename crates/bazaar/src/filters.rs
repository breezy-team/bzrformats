use crate::osutils::sha::sha_chunks;
use std::fs::File;
use std::io::Error;
use std::io::Read;
use std::path::Path;

pub type ContentFilterProvider = dyn Fn(&Path, u64) -> Box<dyn ContentFilter> + Send + Sync;

pub trait ContentFilter {
    fn reader(
        &self,
        input: Box<dyn Iterator<Item = Result<Vec<u8>, Error>> + Send + Sync>,
    ) -> Box<dyn Iterator<Item = Result<Vec<u8>, Error>> + Send + Sync>;

    fn writer(
        &self,
        input: Box<dyn Iterator<Item = Result<Vec<u8>, Error>> + Send + Sync>,
    ) -> Box<dyn Iterator<Item = Result<Vec<u8>, Error>> + Send + Sync>;

    fn sha1_file(&self, path: &Path) -> Result<String, std::io::Error> {
        let mut file = File::open(path)?;
        let chunk_iter = std::iter::from_fn(move || {
            let mut buf = vec![0; 128 << 10];
            let bytes_read = file.read(&mut buf);
            if let Err(e) = bytes_read {
                return Some(Err(e));
            }
            let bytes_read = bytes_read.unwrap();
            if bytes_read == 0 {
                None
            } else {
                buf.truncate(bytes_read);
                Some(Ok(buf))
            }
        });
        let chunk_iter = self.reader(Box::new(chunk_iter));
        let mut err = None;
        let sha1 = sha_chunks(chunk_iter.filter_map(|r| {
            if let Err(e) = r {
                err = Some(e);
                None
            } else {
                Some(r.unwrap())
            }
        }));
        if let Some(err) = err {
            Err(err)
        } else {
            Ok(sha1)
        }
    }
}

pub struct ContentFilterStack {
    filters: Vec<Box<dyn ContentFilter>>,
}

impl From<Vec<Box<dyn ContentFilter>>> for ContentFilterStack {
    fn from(filters: Vec<Box<dyn ContentFilter>>) -> Self {
        Self { filters }
    }
}

impl ContentFilterStack {
    pub fn new() -> Self {
        Self {
            filters: Vec::new(),
        }
    }

    pub fn add_filter(&mut self, filter: Box<dyn ContentFilter>) {
        self.filters.push(filter);
    }
}

impl std::default::Default for ContentFilterStack {
    fn default() -> Self {
        Self::new()
    }
}

impl ContentFilter for ContentFilterStack {
    fn reader(
        &self,
        input: Box<dyn Iterator<Item = Result<Vec<u8>, Error>> + Send + Sync>,
    ) -> Box<dyn Iterator<Item = Result<Vec<u8>, Error>> + Send + Sync> {
        self.filters
            .iter()
            .fold(input, |input, filter| filter.reader(input))
    }

    fn writer(
        &self,
        input: Box<dyn Iterator<Item = Result<Vec<u8>, Error>> + Send + Sync>,
    ) -> Box<dyn Iterator<Item = Result<Vec<u8>, Error>> + Send + Sync> {
        self.filters
            .iter()
            .fold(input, |input, filter| filter.writer(input))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    type Chunks = Box<dyn Iterator<Item = Result<Vec<u8>, Error>> + Send + Sync>;

    /// A filter that maps each byte through a per-direction function, applied
    /// chunk by chunk. `read_fn` runs on read, `write_fn` on write.
    struct ByteMapFilter {
        read_fn: fn(u8) -> u8,
        write_fn: fn(u8) -> u8,
    }

    fn map_chunks(input: Chunks, f: fn(u8) -> u8) -> Chunks {
        Box::new(input.map(move |r| r.map(|chunk| chunk.into_iter().map(f).collect())))
    }

    impl ContentFilter for ByteMapFilter {
        fn reader(&self, input: Chunks) -> Chunks {
            map_chunks(input, self.read_fn)
        }
        fn writer(&self, input: Chunks) -> Chunks {
            map_chunks(input, self.write_fn)
        }
    }

    fn collect(chunks: Chunks) -> Vec<u8> {
        chunks.flat_map(|r| r.unwrap()).collect()
    }

    fn one_chunk(bytes: &[u8]) -> Chunks {
        Box::new(std::iter::once(Ok(bytes.to_vec())))
    }

    #[test]
    fn test_empty_stack_is_identity() {
        let stack = ContentFilterStack::new();
        assert_eq!(collect(stack.reader(one_chunk(b"hello"))), b"hello");
        assert_eq!(collect(stack.writer(one_chunk(b"hello"))), b"hello");
    }

    #[test]
    fn test_single_filter_applied() {
        let stack = ContentFilterStack::from(vec![Box::new(ByteMapFilter {
            read_fn: |b| b.to_ascii_uppercase(),
            write_fn: |b| b.to_ascii_lowercase(),
        }) as Box<dyn ContentFilter>]);
        assert_eq!(collect(stack.reader(one_chunk(b"Hello"))), b"HELLO");
        assert_eq!(collect(stack.writer(one_chunk(b"Hello"))), b"hello");
    }

    #[test]
    fn test_stack_composes_filters_in_order() {
        // First filter adds 1 on read, second adds 10 on read: read applies
        // them in fold order (first, then second).
        let stack = ContentFilterStack::from(vec![
            Box::new(ByteMapFilter {
                read_fn: |b| b + 1,
                write_fn: |b| b - 1,
            }) as Box<dyn ContentFilter>,
            Box::new(ByteMapFilter {
                read_fn: |b| b + 10,
                write_fn: |b| b - 10,
            }) as Box<dyn ContentFilter>,
        ]);
        assert_eq!(collect(stack.reader(one_chunk(&[0, 100]))), vec![11, 111]);
        assert_eq!(collect(stack.writer(one_chunk(&[11, 111]))), vec![0, 100]);
    }

    #[test]
    fn test_sha1_file_runs_content_through_reader() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(b"hello world").unwrap();
        tmp.flush().unwrap();

        // No filters: sha1 of the raw file content.
        let stack = ContentFilterStack::new();
        assert_eq!(
            stack.sha1_file(tmp.path()).unwrap(),
            "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed"
        );

        // An uppercasing read filter: sha1 must be of "HELLO WORLD".
        let upper = ContentFilterStack::from(vec![Box::new(ByteMapFilter {
            read_fn: |b| b.to_ascii_uppercase(),
            write_fn: |b| b.to_ascii_lowercase(),
        }) as Box<dyn ContentFilter>]);
        assert_eq!(
            upper.sha1_file(tmp.path()).unwrap(),
            crate::osutils::sha::sha_string(b"HELLO WORLD")
        );
    }
}
