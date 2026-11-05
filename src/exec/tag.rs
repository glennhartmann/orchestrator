//! Byte-oriented line tagging for pass-through stdout/stderr.
//!
//! [`TaggedWriter`] buffers bytes written via [`TaggedWriter::write_chunk`]
//! until a complete line (terminated by `\n`) is available, then writes
//! `[tag] <line>\n` to the shared sink in a single lock-and-write so that
//! concurrent writers never interleave mid-line. Tagging is purely
//! byte-oriented: lines are never required or assumed to be valid UTF-8.

use std::io::{self, Write};
use std::sync::{Arc, Mutex};

/// A `Write` handle shared between every [`TaggedWriter`] that targets the
/// same stream — in practice the orchestrator's one real stdout or stderr,
/// written to by one tagged writer per node.
pub type SharedSink = Arc<Mutex<Box<dyn Write + Send>>>;

/// Writes chunks of bytes to a shared sink, tagging each complete line
/// with `[tag] ` and buffering any trailing partial line until it is
/// completed or explicitly flushed with [`TaggedWriter::flush_partial`].
///
/// The sink is a `std::sync::Mutex`-guarded `dyn Write`, locked only for
/// the duration of each write, so several `TaggedWriter`s (e.g. one per
/// node, sharing the process's real stdout/stderr) can write to it from
/// different tasks without ever interleaving a line.
///
/// With `tag: None`, chunks are written straight through with no
/// buffering and no line splitting — this is the `--no-tags` path.
pub struct TaggedWriter {
    tag: Option<String>,
    out: SharedSink,
    partial: Vec<u8>,
}

impl TaggedWriter {
    pub fn new(tag: Option<String>, out: SharedSink) -> Self {
        Self {
            tag,
            out,
            partial: Vec::new(),
        }
    }

    /// Feeds `bytes` through the writer. Complete lines (bytes up to and
    /// including each `\n`) are tagged and written to the shared sink in
    /// one lock-and-write; any trailing bytes with no `\n` are held in
    /// the internal buffer until a future chunk completes them or
    /// [`Self::flush_partial`] is called.
    pub async fn write_chunk(&mut self, bytes: &[u8]) -> io::Result<()> {
        let Some(tag) = &self.tag else {
            // --no-tags path: raw passthrough, no buffering.
            self.out.lock().unwrap().write_all(bytes)?;
            return Ok(());
        };

        self.partial.extend_from_slice(bytes);

        let mut tagged = Vec::new();
        let mut start = 0;
        while let Some(rel_nl) = self.partial[start..].iter().position(|&b| b == b'\n') {
            let end = start + rel_nl;
            tagged.extend_from_slice(b"[");
            tagged.extend_from_slice(tag.as_bytes());
            tagged.extend_from_slice(b"] ");
            tagged.extend_from_slice(&self.partial[start..end]);
            tagged.push(b'\n');
            start = end + 1;
        }
        self.partial.drain(..start);

        if !tagged.is_empty() {
            self.out.lock().unwrap().write_all(&tagged)?;
        }
        Ok(())
    }

    /// Flushes any buffered partial line (one with no trailing `\n`),
    /// tagging it and writing it to the shared sink followed by a
    /// newline. A no-op when there is nothing buffered, or when the
    /// writer has no tag.
    pub async fn flush_partial(&mut self) -> io::Result<()> {
        let Some(tag) = &self.tag else {
            return Ok(());
        };

        if self.partial.is_empty() {
            return Ok(());
        }

        let mut tagged = Vec::with_capacity(self.partial.len() + tag.len() + 3);
        tagged.extend_from_slice(b"[");
        tagged.extend_from_slice(tag.as_bytes());
        tagged.extend_from_slice(b"] ");
        tagged.extend_from_slice(&self.partial);
        tagged.push(b'\n');
        self.partial.clear();

        self.out.lock().unwrap().write_all(&tagged)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{self, Write};
    use std::sync::{Arc, Mutex};

    /// A `Write` implementation that appends into a shared, externally
    /// readable buffer. Lets tests assert on exactly what was written to
    /// a "shared sink" handle, mirroring how a real stdout/stderr handle
    /// would be shared between several `TaggedWriter`s.
    #[derive(Clone)]
    struct SharedBuf(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedBuf {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    fn shared_sink() -> (SharedSink, Arc<Mutex<Vec<u8>>>) {
        let buf = Arc::new(Mutex::new(Vec::new()));
        let sink: SharedSink = Arc::new(Mutex::new(Box::new(SharedBuf(buf.clone()))));
        (sink, buf)
    }

    #[tokio::test]
    async fn tags_each_line_with_the_node_name() {
        let (sink, buf) = shared_sink();
        let mut w = TaggedWriter::new(Some("a".to_string()), sink);

        w.write_chunk(b"he").await.unwrap();
        w.write_chunk(b"llo\nworld\n").await.unwrap();

        assert_eq!(buf.lock().unwrap().as_slice(), b"[a] hello\n[a] world\n");
    }

    #[tokio::test]
    async fn flushes_a_final_line_without_a_newline() {
        let (sink, buf) = shared_sink();
        let mut w = TaggedWriter::new(Some("a".to_string()), sink);

        w.write_chunk(b"tail").await.unwrap();
        assert_eq!(buf.lock().unwrap().as_slice(), b"");

        w.flush_partial().await.unwrap();
        assert_eq!(buf.lock().unwrap().as_slice(), b"[a] tail\n");
    }

    #[tokio::test]
    async fn passes_non_utf8_bytes_through_untouched() {
        let (sink, buf) = shared_sink();
        let mut w = TaggedWriter::new(Some("a".to_string()), sink);

        w.write_chunk(b"\xff\xfe\n").await.unwrap();

        assert_eq!(buf.lock().unwrap().as_slice(), b"[a] \xff\xfe\n");
    }

    #[tokio::test]
    async fn no_tag_means_raw_bytes_and_no_line_splitting() {
        let (sink, buf) = shared_sink();
        let mut w = TaggedWriter::new(None, sink);

        w.write_chunk(b"a\nb").await.unwrap();
        assert_eq!(buf.lock().unwrap().as_slice(), b"a\nb");

        w.flush_partial().await.unwrap();
        assert_eq!(buf.lock().unwrap().as_slice(), b"a\nb");
    }

    // Multi-threaded + 2 worker threads is required for this test to mean
    // anything: both spawned tasks' write_chunk/flush_partial calls are
    // fully synchronous (the mutex lock and the write never yield), so on
    // a current-thread runtime one task would simply run to completion
    // before the other is ever polled and the two writers would never
    // actually race at the scheduler level. With two real OS threads they
    // genuinely contend on the shared mutex.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_writers_never_interleave_within_a_line() {
        let (sink, buf) = shared_sink();

        const LINES: usize = 500;

        let mut handles = Vec::new();
        for tag in ["x", "y"] {
            let sink = sink.clone();
            handles.push(tokio::spawn(async move {
                let mut w = TaggedWriter::new(Some(tag.to_string()), sink);
                for i in 0..LINES {
                    // Split each line across two write_chunk calls so a
                    // line only becomes complete (and thus eligible to be
                    // written) after both chunks have landed, widening
                    // the window in which the other thread could interleave
                    // if the shared-sink lock were not held correctly.
                    let first = format!("line-{i}-");
                    let second = "rest-of-payload-padding-to-make-the-write-nontrivial\n";
                    w.write_chunk(first.as_bytes()).await.unwrap();
                    w.write_chunk(second.as_bytes()).await.unwrap();
                }
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        let out = buf.lock().unwrap();
        let text = String::from_utf8(out.clone()).unwrap();
        let mut x_count = 0;
        let mut y_count = 0;
        for line in text.lines() {
            assert!(
                line.starts_with("[x] line-") || line.starts_with("[y] line-"),
                "line was not wholly tagged by one writer: {line:?}"
            );
            assert!(
                line.ends_with("rest-of-payload-padding-to-make-the-write-nontrivial"),
                "line was corrupted/interleaved: {line:?}"
            );
            if line.starts_with("[x]") {
                x_count += 1;
            } else {
                y_count += 1;
            }
        }
        assert_eq!(x_count, LINES);
        assert_eq!(y_count, LINES);
    }
}
