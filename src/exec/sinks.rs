//! Write-side endpoints a fanned-out stream can be delivered to: a
//! downstream node's stdin, a file on disk, or the orchestrator's own
//! (tagged) stdout/stderr.
//!
//! This module only builds and writes to sinks; it does not decide *which*
//! bytes go where or how slow/failing consumers are handled — that's
//! Task 6's tee.

use std::collections::HashMap;
use std::io::{self, Write};
use std::path::Path;
use std::sync::{Arc, Mutex};

use tokio::io::AsyncWriteExt;
use tokio::process::ChildStdin;
use tokio::sync::oneshot;

use crate::exec::tag::TaggedWriter;
use crate::graph::{NodeId, SinkSpec};
use crate::pb;

/// Writes to a downstream node's stdin pipe.
pub struct ChildStdinSink(pub(crate) ChildStdinState);

/// A child-stdin sink's lifecycle.
///
/// A sink is registered with its tee when the *producer* spawns, which can
/// be long before the consuming node itself starts — a consumer held back
/// by an order edge is the whole reason a queue fills up. Until that node
/// spawns there is no stdin pipe to write to, so the sink starts out
/// `Pending`: the first write waits for the handle, which is exactly the
/// backpressure the producer should feel. Cancelling the sink (because the
/// consumer was skipped) interrupts that wait, since the tee races every
/// write against the cancellation token.
pub(crate) enum ChildStdinState {
    /// Waiting for the consuming node to spawn and hand over its stdin.
    Pending(oneshot::Receiver<ChildStdin>),
    Connected(ChildStdin),
    /// Finished, or the consumer never spawned at all. Holds no handle, so
    /// our end of the pipe is closed and the consumer sees EOF.
    Done,
}

impl ChildStdinSink {
    async fn write_all(&mut self, chunk: &[u8]) -> io::Result<()> {
        if let ChildStdinState::Pending(_) = &self.0
            && let ChildStdinState::Pending(rx) =
                std::mem::replace(&mut self.0, ChildStdinState::Done)
        {
            // A dropped sender means the consumer will never spawn (it was
            // skipped, or it spawned without a stdin pipe). That is the
            // same situation as a consumer that went away: a closed pipe.
            self.0 = match rx.await {
                Ok(stdin) => ChildStdinState::Connected(stdin),
                Err(_) => ChildStdinState::Done,
            };
        }

        match &mut self.0 {
            ChildStdinState::Connected(stdin) => stdin.write_all(chunk).await,
            ChildStdinState::Pending(_) => unreachable!("resolved just above"),
            ChildStdinState::Done => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "child stdin sink has no pipe to write to",
            )),
        }
    }

    fn finish(&mut self) {
        // Dropping the handle closes our end of the pipe, which is what
        // lets the consumer's read return EOF.
        self.0 = ChildStdinState::Done;
    }
}

/// Writes to a file on disk, truncating by default or appending when
/// `append` is requested.
pub struct FileByteSink(tokio::fs::File);

impl FileByteSink {
    /// Opens `path` for writing. Truncates existing content unless
    /// `append` is set. Uses a plain (synchronous) open so `build_sink`
    /// itself can stay synchronous: `tokio::fs::File::from_std` wraps the
    /// already-open std handle for async writes without any further
    /// blocking-pool round trip at open time.
    pub fn open(path: &Path, append: bool) -> io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(append)
            .truncate(!append)
            .open(path)?;
        Ok(Self(tokio::fs::File::from_std(file)))
    }

    async fn write_all(&mut self, chunk: &[u8]) -> io::Result<()> {
        self.0.write_all(chunk).await
    }

    /// Flushes, and reports whether that worked.
    ///
    /// `tokio::fs::File` hands the actual write to a blocking task, so a
    /// write that fails — ENOSPC on a full disk is the classic case — may
    /// not surface until here. Discarding this error would throw away the
    /// only evidence that the output never landed.
    async fn finish(&mut self) -> io::Result<()> {
        self.0.flush().await
    }
}

/// Writes to the orchestrator's own stdout/stderr, through a
/// [`TaggedWriter`] so concurrent sinks sharing the same underlying handle
/// never interleave mid-line.
pub struct PassthroughSink(TaggedWriter);

impl PassthroughSink {
    async fn write_all(&mut self, chunk: &[u8]) -> io::Result<()> {
        self.0.write_chunk(chunk).await
    }

    async fn finish(&mut self) -> io::Result<()> {
        self.0.flush_partial().await
    }
}

/// What [`TestSink`] does on a given call to `write_all`.
#[cfg(test)]
pub enum ScriptedWrite {
    /// Sleep for the given duration, then record the bytes and succeed.
    Delay(std::time::Duration),
    /// Fail immediately with the given `io::ErrorKind`, recording nothing.
    Error(io::ErrorKind),
    /// Panic, standing in for a sink whose write unwinds — e.g. a
    /// passthrough sink whose shared stdout mutex was poisoned by another
    /// writer's panic. The panic message contains [`PANIC_MESSAGE`] so a
    /// test can recognise (and silence) its own expected panic.
    Panic,
}

/// The payload a [`ScriptedWrite::Panic`] panics with.
#[cfg(test)]
pub const PANIC_MESSAGE: &str = "scripted TestSink panic";

/// A sink that never spawns a process or touches the filesystem, so
/// Task 6's tee can script slow consumers, failing consumers (including a
/// simulated closed pipe via `ErrorKind::BrokenPipe`), and byte loss, and
/// assert on exactly what was written.
///
/// Scripted behaviors are consumed one per `write_all` call, in the order
/// they were added via [`TestSink::script`]; once the script is exhausted,
/// every further write succeeds immediately and is recorded.
#[cfg(test)]
pub struct TestSink {
    received: Arc<Mutex<Vec<u8>>>,
    finished: Arc<Mutex<bool>>,
    script: std::collections::VecDeque<ScriptedWrite>,
}

#[cfg(test)]
impl Default for TestSink {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
impl TestSink {
    pub fn new() -> Self {
        Self {
            received: Arc::new(Mutex::new(Vec::new())),
            finished: Arc::new(Mutex::new(false)),
            script: std::collections::VecDeque::new(),
        }
    }

    /// Queues a behavior for the next `write_all` call.
    pub fn script(mut self, op: ScriptedWrite) -> Self {
        self.script.push_back(op);
        self
    }

    /// A handle to the bytes recorded so far; shares storage with the
    /// `TestSink`, so it stays readable after the sink has been moved into
    /// a [`Sink::Test`] and dropped by `finish`.
    pub fn received_handle(&self) -> Arc<Mutex<Vec<u8>>> {
        self.received.clone()
    }

    /// A handle that reports whether `finish` has been called.
    pub fn finished_handle(&self) -> Arc<Mutex<bool>> {
        self.finished.clone()
    }

    async fn write_all(&mut self, chunk: &[u8]) -> io::Result<()> {
        match self.script.pop_front() {
            Some(ScriptedWrite::Delay(d)) => {
                tokio::time::sleep(d).await;
                self.received.lock().unwrap().extend_from_slice(chunk);
                Ok(())
            }
            Some(ScriptedWrite::Error(kind)) => {
                Err(io::Error::new(kind, "scripted TestSink failure"))
            }
            Some(ScriptedWrite::Panic) => panic!("{PANIC_MESSAGE}"),
            None => {
                self.received.lock().unwrap().extend_from_slice(chunk);
                Ok(())
            }
        }
    }

    fn finish(&mut self) {
        *self.finished.lock().unwrap() = true;
    }
}

/// One destination a fanned-out stream can be written to.
///
/// A concrete enum rather than a trait object: the sink set is closed, and
/// `dyn` + `async fn` would force boxed futures for no benefit.
pub enum Sink {
    ChildStdin(ChildStdinSink),
    File(FileByteSink),
    Passthrough(PassthroughSink),
    #[cfg(test)]
    Test(TestSink),
}

impl Sink {
    /// Writes `chunk` to the underlying destination. Returns the
    /// `io::Error` as-is on failure — classifying e.g.
    /// `ErrorKind::BrokenPipe` as a clean consumer close is the tee's job
    /// (Task 6), not this layer's.
    pub async fn write_all(&mut self, chunk: &[u8]) -> io::Result<()> {
        match self {
            Sink::ChildStdin(s) => s.write_all(chunk).await,
            Sink::File(s) => s.write_all(chunk).await,
            Sink::Passthrough(s) => s.write_all(chunk).await,
            #[cfg(test)]
            Sink::Test(s) => s.write_all(chunk).await,
        }
    }

    /// Signals that no more bytes are coming. Drops the child stdin handle
    /// (so the consumer sees EOF) and flushes any buffered partial line
    /// for a passthrough sink.
    ///
    /// Returns the error if closing the destination failed. For a file that
    /// is where a failed write usually appears, so a caller that discards
    /// this is quietly dropping output and calling it success.
    pub async fn finish(mut self) -> io::Result<()> {
        match &mut self {
            Sink::ChildStdin(s) => {
                s.finish();
                Ok(())
            }
            Sink::File(s) => s.finish().await,
            Sink::Passthrough(s) => s.finish().await,
            #[cfg(test)]
            Sink::Test(s) => {
                s.finish();
                Ok(())
            }
        }
    }
}

/// Context the scheduler maintains while building sinks: one channel per
/// node that takes its stdin from another node, over which that node hands
/// its stdin pipe to the sink when it spawns (see [`ChildStdinState`]), and
/// the two handles shared by every passthrough sink writing to the
/// orchestrator's own stdout/stderr.
pub struct SinkCtx {
    pub child_stdins: HashMap<NodeId, oneshot::Receiver<ChildStdin>>,
    pub stdout: Arc<Mutex<Box<dyn Write + Send>>>,
    pub stderr: Arc<Mutex<Box<dyn Write + Send>>>,
    /// Whether passthrough output should be tagged with its source node's
    /// name. Set once from `Plan.tags` when the context is constructed for
    /// a run and never updated afterwards — unlike a per-call mutable
    /// field, a run-wide constant has no "forget to update it between
    /// nodes" hazard.
    pub tags: bool,
}

/// Builds the concrete [`Sink`] a [`SinkSpec`] describes.
///
/// `source_node` is the name of the node whose stream this sink is
/// attached to; it is used only to tag a `Passthrough` sink's output (and
/// only when `ctx.tags` is set) and is ignored for the other variants.
/// Passed explicitly (rather than living as mutable state on `ctx`) so
/// there is no way for a caller to forget to update it between nodes and
/// silently mistag one node's output with another's name.
///
/// For `Passthrough`, `STREAM_BOTH` is written to the shared stdout
/// handle: the merged stream is produced by dup2'ing the child's stderr
/// onto its stdout fd (see `exec::child::spawn`), so all of its bytes
/// physically arrive on the child's stdout pipe, making stdout the
/// sensible orchestrator-side destination too.
pub fn build_sink(spec: &SinkSpec, ctx: &mut SinkCtx, source_node: &str) -> io::Result<Sink> {
    match spec {
        SinkSpec::NodeStdin(id) => {
            let stdin = ctx.child_stdins.remove(id).ok_or_else(|| {
                io::Error::other(format!("no stdin channel registered for node {}", id.0))
            })?;
            Ok(Sink::ChildStdin(ChildStdinSink(ChildStdinState::Pending(
                stdin,
            ))))
        }
        SinkSpec::File { path, append } => Ok(Sink::File(FileByteSink::open(path, *append)?)),
        SinkSpec::Passthrough(stream) => {
            let out = match stream {
                pb::Stream::Stderr => ctx.stderr.clone(),
                pb::Stream::Stdout | pb::Stream::Both | pb::Stream::Unspecified => {
                    ctx.stdout.clone()
                }
            };
            let tag = if ctx.tags {
                Some(source_node.to_string())
            } else {
                None
            };
            Ok(Sink::Passthrough(PassthroughSink(TaggedWriter::new(
                tag, out,
            ))))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::process::Stdio;
    use std::time::Duration;

    use tokio::io::AsyncReadExt;

    use crate::exec::child::spawn;
    use crate::graph::{NodeSpec, StdinSource};

    fn base_spec(command: Vec<&str>) -> NodeSpec {
        NodeSpec {
            name: "n".to_string(),
            command: command.into_iter().map(str::to_string).collect(),
            env: BTreeMap::new(),
            clear_env: false,
            working_dir: None,
            shell: false,
            stdin: StdinSource::Empty,
            order_deps: Vec::new(),
        }
    }

    /// A `Write` implementation appending into a shared, externally
    /// readable buffer — a stand-in for the orchestrator's real
    /// stdout/stderr handle.
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

    fn shared_writer() -> (Arc<Mutex<Box<dyn Write + Send>>>, Arc<Mutex<Vec<u8>>>) {
        let buf = Arc::new(Mutex::new(Vec::new()));
        let out: Arc<Mutex<Box<dyn Write + Send>>> =
            Arc::new(Mutex::new(Box::new(SharedBuf(buf.clone()))));
        (out, buf)
    }

    fn empty_ctx() -> SinkCtx {
        let (stdout, _) = shared_writer();
        let (stderr, _) = shared_writer();
        SinkCtx {
            child_stdins: HashMap::new(),
            stdout,
            stderr,
            tags: true,
        }
    }

    #[tokio::test]
    async fn file_sink_truncates_by_default() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.txt");
        std::fs::write(&path, b"pre-existing content that must be gone\n").unwrap();

        let mut sink = Sink::File(FileByteSink::open(&path, false).unwrap());
        sink.write_all(b"new\n").await.unwrap();
        sink.finish().await.unwrap();

        let contents = std::fs::read(&path).unwrap();
        assert_eq!(contents, b"new\n");
    }

    // `/dev/full` accepts writes and fails them with ENOSPC, which is the
    // only way to see a real deferred write failure without faking I/O. It
    // is Linux-only; macOS has no equivalent.
    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn file_sink_reports_a_deferred_write_failure_when_it_closes() {
        let mut sink = Sink::File(FileByteSink::open(Path::new("/dev/full"), false).unwrap());

        // `tokio::fs::File` hands the write to a blocking task, so this call
        // may well report success; the failure lands on the flush below.
        // Whichever way round it happens, the error must not vanish.
        let write = sink.write_all(b"x").await;
        let finish = sink.finish().await;

        let err = match (write, finish) {
            (Err(e), _) | (Ok(()), Err(e)) => e,
            (Ok(()), Ok(())) => {
                panic!("a write to /dev/full cannot succeed; the error was swallowed")
            }
        };
        assert_eq!(
            err.raw_os_error(),
            Some(nix::libc::ENOSPC),
            "expected ENOSPC, got {err}"
        );
    }

    #[tokio::test]
    async fn file_sink_appends_when_asked() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.txt");
        std::fs::write(&path, b"existing\n").unwrap();

        let mut sink = Sink::File(FileByteSink::open(&path, true).unwrap());
        sink.write_all(b"appended\n").await.unwrap();
        sink.finish().await.unwrap();

        let contents = std::fs::read(&path).unwrap();
        assert_eq!(contents, b"existing\nappended\n");
    }

    #[tokio::test]
    async fn child_stdin_sink_closes_the_pipe_on_finish() {
        let spec = base_spec(vec!["cat"]);
        let mut spawned = spawn(&spec, Stdio::piped(), true, false, false).unwrap();
        let stdin = spawned.child.stdin.take().expect("stdin should be piped");

        let mut sink = Sink::ChildStdin(ChildStdinSink(ChildStdinState::Connected(stdin)));
        sink.write_all(b"hello\n").await.unwrap();
        sink.finish().await.unwrap();

        // If finish() failed to drop the stdin handle, `cat` would never
        // see EOF and this read would hang forever; bound it so a
        // regression fails the test instead of hanging the suite.
        let mut out = String::new();
        tokio::time::timeout(
            Duration::from_secs(5),
            spawned
                .child
                .stdout
                .take()
                .expect("stdout should be piped")
                .read_to_string(&mut out),
        )
        .await
        .expect("cat did not see EOF after finish() dropped stdin")
        .unwrap();

        let status = spawned.child.wait().await.unwrap();
        assert!(status.success());
        assert_eq!(out, "hello\n");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_pending_child_stdin_sink_waits_for_its_consumer_to_spawn() {
        // The producer's tee registers this sink before the consuming node
        // exists. The write must not fail and must not lose the bytes: it
        // waits, which is what throttles the producer.
        let (tx, rx) = oneshot::channel();
        let mut sink = Sink::ChildStdin(ChildStdinSink(ChildStdinState::Pending(rx)));

        let write = tokio::spawn(async move {
            sink.write_all(b"queued\n").await.unwrap();
            sink.finish().await.unwrap();
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            !write.is_finished(),
            "the write must block until the consumer hands over its stdin"
        );

        let spec = base_spec(vec!["cat"]);
        let mut spawned = spawn(&spec, Stdio::piped(), true, false, false).unwrap();
        tx.send(spawned.child.stdin.take().expect("stdin is piped"))
            .expect("the sink is still waiting");

        tokio::time::timeout(Duration::from_secs(5), write)
            .await
            .expect("handing over the stdin pipe must release the write")
            .unwrap();

        let mut out = String::new();
        spawned
            .child
            .stdout
            .take()
            .expect("stdout is piped")
            .read_to_string(&mut out)
            .await
            .unwrap();
        assert_eq!(out, "queued\n", "the queued bytes must reach the consumer");
        assert!(spawned.child.wait().await.unwrap().success());
    }

    #[tokio::test]
    async fn a_pending_child_stdin_sink_whose_consumer_never_spawns_reports_a_closed_pipe() {
        let (tx, rx) = oneshot::channel::<ChildStdin>();
        let mut sink = Sink::ChildStdin(ChildStdinSink(ChildStdinState::Pending(rx)));
        drop(tx);

        let err = sink
            .write_all(b"nowhere")
            .await
            .expect_err("a consumer that will never spawn is a closed pipe");
        assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
    }

    #[tokio::test]
    async fn child_stdin_sink_reports_broken_pipe_as_closed() {
        // `true` exits immediately without reading stdin, so by the time
        // we've waited for it, the pipe's read end is already closed.
        let spec = base_spec(vec!["true"]);
        let mut spawned = spawn(&spec, Stdio::piped(), false, false, false).unwrap();
        let stdin = spawned.child.stdin.take().expect("stdin should be piped");
        let status = spawned.child.wait().await.unwrap();
        assert!(status.success());

        let mut sink = Sink::ChildStdin(ChildStdinSink(ChildStdinState::Connected(stdin)));
        let err = sink
            .write_all(b"data")
            .await
            .expect_err("write to a stdin whose consumer already exited must fail");
        assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
    }

    #[tokio::test]
    async fn passthrough_sink_tags_and_flushes_a_partial_line() {
        let (out, buf) = shared_writer();
        let writer = TaggedWriter::new(Some("n".to_string()), out);
        let mut sink = Sink::Passthrough(PassthroughSink(writer));

        sink.write_all(b"partial-no-newline").await.unwrap();
        assert_eq!(
            buf.lock().unwrap().as_slice(),
            b"",
            "a line with no trailing newline must not be written until flushed"
        );

        sink.finish().await.unwrap();
        assert_eq!(buf.lock().unwrap().as_slice(), b"[n] partial-no-newline\n");
    }

    #[tokio::test]
    async fn build_sink_passthrough_stdout_and_both_share_the_stdout_writer() {
        let (stdout, stdout_buf) = shared_writer();
        let (stderr, stderr_buf) = shared_writer();
        let mut ctx = SinkCtx {
            child_stdins: HashMap::new(),
            stdout,
            stderr,
            tags: true,
        };

        for stream in [pb::Stream::Stdout, pb::Stream::Both] {
            let mut sink = build_sink(&SinkSpec::Passthrough(stream), &mut ctx, "src").unwrap();
            sink.write_all(b"line\n").await.unwrap();
            sink.finish().await.unwrap();
        }

        assert_eq!(
            stdout_buf.lock().unwrap().as_slice(),
            b"[src] line\n[src] line\n"
        );
        assert_eq!(
            stderr_buf.lock().unwrap().as_slice(),
            b"",
            "STDOUT/BOTH passthrough must never write to the stderr handle"
        );
    }

    #[tokio::test]
    async fn build_sink_passthrough_stderr_uses_the_stderr_writer() {
        let (stdout, stdout_buf) = shared_writer();
        let (stderr, stderr_buf) = shared_writer();
        let mut ctx = SinkCtx {
            child_stdins: HashMap::new(),
            stdout,
            stderr,
            tags: true,
        };

        let mut sink =
            build_sink(&SinkSpec::Passthrough(pb::Stream::Stderr), &mut ctx, "src").unwrap();
        sink.write_all(b"oops\n").await.unwrap();
        sink.finish().await.unwrap();

        assert_eq!(stderr_buf.lock().unwrap().as_slice(), b"[src] oops\n");
        assert_eq!(stdout_buf.lock().unwrap().as_slice(), b"");
    }

    #[tokio::test]
    async fn build_sink_passthrough_with_tags_disabled_writes_raw_bytes_untagged() {
        let (stdout, stdout_buf) = shared_writer();
        let (stderr, _) = shared_writer();
        let mut ctx = SinkCtx {
            child_stdins: HashMap::new(),
            stdout,
            stderr,
            tags: false,
        };

        let mut sink =
            build_sink(&SinkSpec::Passthrough(pb::Stream::Stdout), &mut ctx, "src").unwrap();
        sink.write_all(b"plain\n").await.unwrap();
        sink.finish().await.unwrap();

        assert_eq!(
            stdout_buf.lock().unwrap().as_slice(),
            b"plain\n",
            "tags: false must produce raw bytes with no \"[src] \" prefix"
        );
    }

    #[tokio::test]
    async fn build_sink_errors_when_no_stdin_channel_is_registered() {
        let mut ctx = empty_ctx();
        let result = build_sink(&SinkSpec::NodeStdin(NodeId(0)), &mut ctx, "n");
        assert!(
            result.is_err(),
            "a node with no registered stdin channel cannot be fed; the \
             scheduler must register one for every node whose stdin comes \
             from another node"
        );
    }

    #[tokio::test]
    async fn test_sink_records_bytes_and_can_script_an_error_then_recover() {
        let test_sink = TestSink::new().script(ScriptedWrite::Error(io::ErrorKind::BrokenPipe));
        let received = test_sink.received_handle();
        let mut sink = Sink::Test(test_sink);

        let err = sink.write_all(b"lost").await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
        assert_eq!(
            received.lock().unwrap().as_slice(),
            b"",
            "a scripted error must not record the bytes that were \"lost\""
        );

        sink.write_all(b"ok").await.unwrap();
        assert_eq!(received.lock().unwrap().as_slice(), b"ok");
    }
}
