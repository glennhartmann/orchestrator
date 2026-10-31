//! Fan-out of one source stream to many sinks, with per-sink byte-bounded
//! queues.
//!
//! The orchestrator's whole point is non-linear pipes: a node's stdout
//! streams to several consumers at once, in real time, and no single slow
//! or dead consumer may stall its siblings or the producer.

use std::io;
use std::sync::Arc;

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::sync::{Semaphore, mpsc, watch};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

use crate::exec::sinks::Sink;

/// Why a sink stopped consuming.
#[derive(Debug)]
pub enum SinkOutcome {
    /// Saw end of stream and was finished cleanly.
    Finished,
    /// The consumer went away (`ErrorKind::BrokenPipe`) — normal pipeline
    /// behavior, not a failure.
    Closed,
    /// Its [`CancellationToken`] fired, e.g. because the consuming node was
    /// skipped.
    Cancelled,
    /// A write failed for some reason other than a closed consumer.
    Errored(io::Error),
}

/// What one fan-out did.
#[derive(Debug)]
pub struct TeeOutcome {
    /// True if the tee stopped because no sinks remained, rather than
    /// because the source ended.
    pub early_stop: bool,
    /// `None` on a clean EOF. `Some` if reading the source failed: the
    /// sinks received everything that did arrive and finished normally, but
    /// the stream they saw was truncated.
    pub read_error: Option<io::Error>,
    /// Per-sink outcomes, in `add_sink` registration order.
    pub sinks: Vec<(String, SinkOutcome)>,
}

/// Fans a source stream out to a set of sinks.
pub struct Tee {
    cap_bytes: usize,
    sinks: Vec<SinkHandle>,
    /// Published the moment the live set empties — see
    /// [`Tee::no_sinks_left`].
    no_sinks_left: watch::Sender<bool>,
}

struct SinkHandle {
    name: String,
    /// `None` once the sink has been removed from the live set; dropping
    /// the sender is also what signals end of stream to a live sink task.
    tx: Option<mpsc::UnboundedSender<Bytes>>,
    sem: Arc<Semaphore>,
    join: JoinHandle<SinkOutcome>,
}

/// Upper bound on a single read from the source. The read buffer is the
/// smaller of this and the queue cap, which keeps per-sink in-flight bytes
/// at no more than `cap_bytes` queued plus the one chunk being admitted.
const MAX_CHUNK_BYTES: usize = 64 * 1024;

impl Tee {
    /// `cap_bytes` is the per-sink queue bound, in bytes (`Plan.queue_cap_bytes`).
    pub fn new(cap_bytes: usize) -> Self {
        Self {
            // At least one byte, so a chunk is always admissible; at most
            // `u32::MAX`, the widest amount `Semaphore` can be asked for in
            // one acquire.
            cap_bytes: cap_bytes.clamp(1, u32::MAX as usize),
            sinks: Vec::new(),
            no_sinks_left: watch::channel(false).0,
        }
    }

    /// Observes "this fan-out has no sinks left" *as it happens*, rather
    /// than when [`Self::run`] eventually returns its [`TeeOutcome`].
    ///
    /// The distinction matters to the scheduler. Dropping the source is
    /// what makes the producer take EPIPE/SIGPIPE and die, so by the time
    /// that producer's exit is observed, the question "had it lost its
    /// whole audience?" must already be answerable — but `run` does not
    /// return until every sink task has been joined, which can be later.
    /// The flag latches: it never goes back to false, and it stays readable
    /// after the fan-out is over.
    pub fn no_sinks_left(&self) -> watch::Receiver<bool> {
        self.no_sinks_left.subscribe()
    }

    /// Registers `sink` under `name` and starts its consumer task. The
    /// returned token removes the sink from the live set when cancelled,
    /// releasing any backpressure it was applying to the producer.
    pub fn add_sink(&mut self, name: String, sink: Sink) -> CancellationToken {
        let (tx, rx) = mpsc::unbounded_channel();
        let sem = Arc::new(Semaphore::new(self.cap_bytes));
        let token = CancellationToken::new();
        let join = tokio::spawn(drain_sink(
            sink,
            rx,
            sem.clone(),
            token.clone(),
            self.cap_bytes,
        ));
        self.sinks.push(SinkHandle {
            name,
            tx: Some(tx),
            sem,
            join,
        });
        token
    }

    /// Reads `src` to EOF or until every sink is gone, then drops `src`.
    pub async fn run(self, mut src: impl AsyncRead + Unpin) -> TeeOutcome {
        let Tee {
            cap_bytes,
            mut sinks,
            no_sinks_left,
        } = self;

        // Indices into `sinks` of the sinks still worth writing to, in the
        // order they were added. `sinks` itself is never reordered, so the
        // reported outcomes stay in registration order.
        let mut live: Vec<usize> = (0..sinks.len()).collect();
        let mut buf = vec![0u8; cap_bytes.min(MAX_CHUNK_BYTES)];
        let mut early_stop = live.is_empty();
        let mut read_error = None;

        while !early_stop {
            let n = match src.read(&mut buf).await {
                Ok(0) => break,
                Ok(n) => n,
                // The sinks still get everything that did arrive and
                // finish normally, but a truncated stream must never be
                // indistinguishable from a clean EOF: report it so the run
                // summary can say the fan-out was cut short.
                Err(e) => {
                    read_error = Some(e);
                    break;
                }
            };
            let chunk = Bytes::copy_from_slice(&buf[..n]);
            // Clamped so a chunk larger than the whole cap can still be
            // admitted instead of blocking forever.
            let permits = n.min(cap_bytes) as u32;

            for &i in &live {
                let handle = &sinks[i];
                let admitted = match handle.sem.acquire_many(permits).await {
                    // The sink task gives these back once its write for
                    // this chunk has returned.
                    Ok(permit) => {
                        permit.forget();
                        handle
                            .tx
                            .as_ref()
                            .is_some_and(|tx| tx.send(chunk.clone()).is_ok())
                    }
                    // Semaphore closed: the sink task has stopped, so it
                    // will never drain. Never block on it again.
                    Err(_) => false,
                };
                if !admitted {
                    sinks[i].tx = None;
                }
            }
            live.retain(|&i| sinks[i].tx.is_some());
            early_stop = live.is_empty();
        }

        if early_stop {
            // Published here, before `src` is dropped below: dropping it is
            // what kills the producer, so anyone classifying that producer's
            // exit must be able to see this first. (A tee that was handed no
            // sinks at all also lands here, having never entered the loop.)
            no_sinks_left.send_replace(true);
        }

        // Dropping every sender signals end of stream to the sinks that are
        // still running; dropping `src` closes the orchestrator's read end,
        // so a producer we stopped reading takes EPIPE/SIGPIPE rather than
        // writing into a pipe nobody drains.
        for handle in &mut sinks {
            handle.tx = None;
        }
        drop(src);

        let mut sink_outcomes = Vec::with_capacity(sinks.len());
        for handle in sinks {
            let outcome = handle
                .join
                .await
                .unwrap_or_else(|e| SinkOutcome::Errored(io::Error::other(e)));
            sink_outcomes.push((handle.name, outcome));
        }
        TeeOutcome {
            early_stop,
            read_error,
            sinks: sink_outcomes,
        }
    }
}

/// One sink's consumer task: writes queued chunks in order until end of
/// stream, a write failure, or cancellation, then finishes the sink.
async fn drain_sink(
    mut sink: Sink,
    mut rx: mpsc::UnboundedReceiver<Bytes>,
    sem: Arc<Semaphore>,
    token: CancellationToken,
    cap_bytes: usize,
) -> SinkOutcome {
    // The close must also happen if a write panics and this task unwinds:
    // a straight-line close after the loop would be skipped, leaving the
    // reader blocked forever on a queue nobody will ever serve (the join
    // error it would learn from is only observable after the read loop).
    let close_on_drop = CloseOnDrop(sem.clone());

    let outcome = loop {
        let chunk = tokio::select! {
            biased;
            () = token.cancelled() => break SinkOutcome::Cancelled,
            chunk = rx.recv() => match chunk {
                Some(chunk) => chunk,
                None => break SinkOutcome::Finished,
            },
        };
        let permits = chunk.len().min(cap_bytes) as u32;
        // Cancellation has to be able to interrupt an in-flight write:
        // a consumer that is stuck mid-write would otherwise go on
        // throttling the producer for as long as the write takes.
        // Abandoning the write can leave this sink with a partial final
        // chunk, which is intended, not a bug: a cancelled sink belongs to
        // a node that was skipped and will never read its stdin, so no one
        // can observe the truncation.
        let result = tokio::select! {
            biased;
            () = token.cancelled() => break SinkOutcome::Cancelled,
            result = sink.write_all(&chunk) => result,
        };
        sem.add_permits(permits as usize);
        if let Err(e) = result {
            break if e.kind() == io::ErrorKind::BrokenPipe {
                SinkOutcome::Closed
            } else {
                SinkOutcome::Errored(e)
            };
        }
    };

    // Release the reader before finishing: flushing or closing the
    // underlying handle can take a while, and there is nothing left for it
    // to wait for. Dropping the guard early still leaves exactly one close
    // path, which also covers the panic above.
    drop(close_on_drop);

    // A sink that drained everything and then failed to close has still
    // lost the output: for a file, a deferred write error only surfaces at
    // flush, so this is where ENOSPC usually turns up. An outcome that is
    // already decided keeps its own story — the first error is the useful
    // one, and a cancelled sink's torn tail is expected, not a fault.
    match (&outcome, sink.finish().await) {
        (SinkOutcome::Finished, Err(e)) if e.kind() == io::ErrorKind::BrokenPipe => {
            SinkOutcome::Closed
        }
        (SinkOutcome::Finished, Err(e)) => SinkOutcome::Errored(e),
        _ => outcome,
    }
}

/// Closes a sink's semaphore when its consumer task ends, by any route —
/// end of stream, write error, cancellation, or an unwinding panic.
///
/// Once the task is gone the sink will never drain another byte, so the
/// reader must never block admitting a chunk to it: closing the semaphore
/// fails its pending and future acquires, which is how the reader learns
/// to drop the sink from the live set. This is the single close path.
struct CloseOnDrop(Arc<Semaphore>);

impl Drop for CloseOnDrop {
    fn drop(&mut self) {
        self.0.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::ErrorKind;
    use std::pin::Pin;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};
    use std::time::Duration;

    use tokio::io::{AsyncWriteExt, ReadBuf};

    use crate::exec::sinks::{PANIC_MESSAGE, ScriptedWrite, TestSink};

    const CAP: usize = 1 << 20;

    /// A plain in-memory source whose handed-out byte count the test can
    /// read, so "did `run` drain the source?" is an assertion rather than a
    /// guess. Optionally fails instead of reaching EOF, to stand in for a
    /// pipe whose read errors part-way through.
    struct CountingSrc {
        data: Bytes,
        pos: usize,
        fail_at: Option<usize>,
        handed_out: Arc<AtomicUsize>,
    }

    impl CountingSrc {
        fn new(data: Vec<u8>) -> (Self, Arc<AtomicUsize>) {
            Self::build(data, None)
        }

        /// Hands out `fail_at` bytes, then fails every subsequent read.
        fn failing_at(data: Vec<u8>, fail_at: usize) -> (Self, Arc<AtomicUsize>) {
            Self::build(data, Some(fail_at))
        }

        fn build(data: Vec<u8>, fail_at: Option<usize>) -> (Self, Arc<AtomicUsize>) {
            let handed_out = Arc::new(AtomicUsize::new(0));
            (
                Self {
                    data: Bytes::from(data),
                    pos: 0,
                    fail_at,
                    handed_out: handed_out.clone(),
                },
                handed_out,
            )
        }
    }

    impl AsyncRead for CountingSrc {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let limit = self.fail_at.unwrap_or(self.data.len());
            if self.pos >= limit {
                if self.fail_at.is_some() {
                    return Poll::Ready(Err(io::Error::new(ErrorKind::ConnectionReset, "boom")));
                }
                return Poll::Ready(Ok(()));
            }
            let n = buf.remaining().min(limit - self.pos);
            let pos = self.pos;
            let data = self.data.clone();
            buf.put_slice(&data[pos..pos + n]);
            self.pos += n;
            self.handed_out.fetch_add(n, Ordering::SeqCst);
            Poll::Ready(Ok(()))
        }
    }

    /// Distinct, position-dependent bytes, so a reordering or a dropped
    /// interior chunk cannot accidentally compare equal.
    fn payload(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i % 251) as u8).collect()
    }

    fn add_test_sink(tee: &mut Tee, name: &str, sink: TestSink) -> TestSinkProbe {
        let received = sink.received_handle();
        let finished = sink.finished_handle();
        let token = tee.add_sink(name.to_string(), Sink::Test(sink));
        TestSinkProbe {
            received,
            finished,
            token,
        }
    }

    struct TestSinkProbe {
        received: Arc<Mutex<Vec<u8>>>,
        finished: Arc<Mutex<bool>>,
        token: CancellationToken,
    }

    impl TestSinkProbe {
        fn bytes(&self) -> Vec<u8> {
            self.received.lock().unwrap().clone()
        }

        fn len(&self) -> usize {
            self.received.lock().unwrap().len()
        }

        fn is_finished(&self) -> bool {
            *self.finished.lock().unwrap()
        }
    }

    type PanicHook = Box<dyn Fn(&std::panic::PanicHookInfo<'_>) + Sync + Send>;

    /// Swallows the panic report for a deliberately panicking [`TestSink`]
    /// (identified by its payload) so an expected panic does not litter the
    /// test log, while any other panic is still reported through the hook
    /// that was installed before. Restores that hook on drop.
    struct SilenceExpectedPanic(Arc<PanicHook>);

    impl SilenceExpectedPanic {
        fn install() -> Self {
            let previous: Arc<PanicHook> = Arc::new(std::panic::take_hook());
            let delegate = previous.clone();
            std::panic::set_hook(Box::new(move |info| {
                let payload = info.payload();
                let expected = payload
                    .downcast_ref::<String>()
                    .is_some_and(|s| s.contains(PANIC_MESSAGE))
                    || payload
                        .downcast_ref::<&str>()
                        .is_some_and(|s| s.contains(PANIC_MESSAGE));
                if !expected {
                    delegate(info);
                }
            }));
            Self(previous)
        }
    }

    impl Drop for SilenceExpectedPanic {
        fn drop(&mut self) {
            // `set_hook` itself panics if called from a panicking thread,
            // which would turn a failing assertion in the test into an
            // abort that hides the assertion message. Leaving the filtering
            // hook installed in that case is harmless: it still delegates
            // every unexpected panic.
            if std::thread::panicking() {
                return;
            }
            let previous = self.0.clone();
            std::panic::set_hook(Box::new(move |info| previous(info)));
        }
    }

    fn names(outcomes: &[(String, SinkOutcome)]) -> Vec<&str> {
        outcomes.iter().map(|(n, _)| n.as_str()).collect()
    }

    #[tokio::test]
    async fn every_sink_gets_every_byte() {
        let data = payload(200_000);
        let mut tee = Tee::new(CAP);
        let probes: Vec<_> = ["a", "b", "c"]
            .iter()
            .map(|n| add_test_sink(&mut tee, n, TestSink::new()))
            .collect();

        let (src, _) = CountingSrc::new(data.clone());
        let TeeOutcome {
            early_stop,
            read_error,
            sinks: outcomes,
        } = tee.run(src).await;

        assert!(
            !early_stop,
            "every sink survived, so this is not early stop"
        );
        assert!(read_error.is_none(), "the source ended cleanly");
        assert_eq!(names(&outcomes), vec!["a", "b", "c"]);
        for (name, outcome) in &outcomes {
            assert!(
                matches!(outcome, SinkOutcome::Finished),
                "sink {name} should have finished cleanly, got {outcome:?}"
            );
        }
        for (i, probe) in probes.iter().enumerate() {
            assert_eq!(probe.bytes(), data, "sink {i} did not receive every byte");
            assert!(probe.is_finished(), "sink {i} was never finished");
        }
    }

    #[tokio::test]
    async fn a_closed_sink_is_dropped_and_the_others_continue() {
        let data = payload(200_000);
        let mut tee = Tee::new(CAP);
        let closing = add_test_sink(
            &mut tee,
            "closing",
            TestSink::new().script(ScriptedWrite::Error(ErrorKind::BrokenPipe)),
        );
        let healthy = add_test_sink(&mut tee, "healthy", TestSink::new());

        let (src, _) = CountingSrc::new(data.clone());
        let TeeOutcome {
            early_stop,
            read_error,
            sinks: outcomes,
        } = tee.run(src).await;

        assert!(!early_stop, "the healthy sink kept the tee reading");
        assert!(read_error.is_none());
        assert_eq!(names(&outcomes), vec!["closing", "healthy"]);
        assert!(
            matches!(outcomes[0].1, SinkOutcome::Closed),
            "BrokenPipe is a clean consumer close, got {:?}",
            outcomes[0].1
        );
        assert!(matches!(outcomes[1].1, SinkOutcome::Finished));
        assert_eq!(
            healthy.bytes(),
            data,
            "a sibling closing must not cost the healthy sink any bytes"
        );
        assert_eq!(closing.len(), 0);
    }

    #[tokio::test]
    async fn a_write_error_that_is_not_broken_pipe_is_reported_as_errored() {
        let data = payload(10_000);
        let mut tee = Tee::new(CAP);
        add_test_sink(
            &mut tee,
            "broken",
            TestSink::new().script(ScriptedWrite::Error(ErrorKind::PermissionDenied)),
        );
        let healthy = add_test_sink(&mut tee, "healthy", TestSink::new());

        let (src, _) = CountingSrc::new(data.clone());
        let outcomes = tee.run(src).await.sinks;

        match &outcomes[0].1 {
            SinkOutcome::Errored(e) => assert_eq!(e.kind(), ErrorKind::PermissionDenied),
            other => panic!("expected Errored, got {other:?}"),
        }
        assert!(matches!(outcomes[1].1, SinkOutcome::Finished));
        assert_eq!(healthy.bytes(), data);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_cancelled_sink_releases_its_backpressure() {
        // A sink that never completes a write fills its 64-byte queue and
        // then throttles the reader forever. Cancelling it must release the
        // producer it was throttling — the deadlock the design calls out.
        let data = payload(4096);
        let mut tee = Tee::new(64);
        let stuck = add_test_sink(
            &mut tee,
            "stuck",
            TestSink::new().script(ScriptedWrite::Delay(Duration::from_secs(3600))),
        );
        let fast = add_test_sink(&mut tee, "fast", TestSink::new());

        let (src, _) = CountingSrc::new(data.clone());
        let run = tokio::spawn(tee.run(src));

        // Wait until the fast sink stops making progress: that is the
        // reader blocked trying to admit a chunk to the stuck sink.
        let stalled_at = loop {
            let before = fast.len();
            tokio::time::sleep(Duration::from_millis(25)).await;
            if before > 0 && fast.len() == before {
                break before;
            }
        };
        assert!(
            stalled_at < data.len(),
            "the stuck sink should have throttled the reader well before EOF"
        );

        stuck.token.cancel();

        let TeeOutcome {
            early_stop,
            read_error,
            sinks: outcomes,
        } = tokio::time::timeout(Duration::from_secs(10), run)
            .await
            .expect("cancelling the stuck sink must release the reader")
            .unwrap();

        assert!(!early_stop);
        assert!(read_error.is_none());
        assert_eq!(names(&outcomes), vec!["stuck", "fast"]);
        assert!(
            matches!(outcomes[0].1, SinkOutcome::Cancelled),
            "got {:?}",
            outcomes[0].1
        );
        assert!(matches!(outcomes[1].1, SinkOutcome::Finished));
        assert_eq!(
            fast.bytes(),
            data,
            "the surviving sink must still receive the whole stream"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_panicking_sink_with_a_full_queue_releases_the_reader() {
        // A panicking write unwinds the sink task, so the close that frees
        // the reader has to happen on the unwind path too — e.g. a
        // passthrough sink whose shared stdout mutex was poisoned by
        // another writer's panic. The first (delayed) write guarantees the
        // 64-byte queue is full and the reader is waiting on it when the
        // second write panics.
        let data = payload(4096);
        let mut tee = Tee::new(64);
        add_test_sink(
            &mut tee,
            "panicky",
            TestSink::new()
                .script(ScriptedWrite::Delay(Duration::from_millis(50)))
                .script(ScriptedWrite::Panic),
        );
        let healthy = add_test_sink(&mut tee, "healthy", TestSink::new());

        let (src, _) = CountingSrc::new(data.clone());
        let silence = SilenceExpectedPanic::install();
        let TeeOutcome {
            early_stop,
            read_error,
            sinks: outcomes,
        } = tokio::time::timeout(Duration::from_secs(10), tokio::spawn(tee.run(src)))
            .await
            .expect("a panicking sink must not leave the reader blocked on its queue")
            .unwrap();
        drop(silence);

        assert!(!early_stop, "the healthy sink kept the tee reading");
        assert!(read_error.is_none());
        assert_eq!(names(&outcomes), vec!["panicky", "healthy"]);
        match &outcomes[0].1 {
            SinkOutcome::Errored(e) => assert!(
                e.to_string().contains("panic"),
                "the panic should be reported, got {e}"
            ),
            other => panic!("expected Errored, got {other:?}"),
        }
        assert!(matches!(outcomes[1].1, SinkOutcome::Finished));
        assert_eq!(
            healthy.bytes(),
            data,
            "a sibling panicking must not cost the healthy sink any bytes"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_slow_sink_does_not_hold_back_a_fast_one() {
        const CHUNKS: usize = 8;
        const CHUNK_LEN: usize = 1024;

        let mut tee = Tee::new(CAP);
        let slow = add_test_sink(
            &mut tee,
            "slow",
            TestSink::new().script(ScriptedWrite::Delay(Duration::from_secs(3600))),
        );
        let fast = add_test_sink(&mut tee, "fast", TestSink::new());

        // A pipe, so the test controls chunk boundaries: each chunk is
        // written only after the fast sink has received the previous one.
        let (src, mut producer) = tokio::io::duplex(64 * 1024);
        let run = tokio::spawn(tee.run(src));

        for i in 0..CHUNKS {
            producer.write_all(&vec![i as u8; CHUNK_LEN]).await.unwrap();
            let want = (i + 1) * CHUNK_LEN;
            tokio::time::timeout(Duration::from_secs(10), async {
                while fast.len() < want {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "fast sink stalled at {} bytes waiting for chunk {i}: the slow \
                     sink's first write serialized the whole tee",
                    fast.len()
                )
            });
        }

        assert_eq!(
            slow.len(),
            0,
            "the slow sink should still be inside its first write"
        );
        assert_eq!(fast.len(), CHUNKS * CHUNK_LEN);

        slow.token.cancel();
        drop(producer);
        tokio::time::timeout(Duration::from_secs(10), run)
            .await
            .expect("run should finish once the source closes")
            .unwrap();
    }

    #[tokio::test]
    async fn run_reports_early_stop_when_the_last_sink_goes() {
        let data = payload(1 << 20);
        // A small cap so the reader has to wait on the queue after the
        // first chunk, which is when it learns the sink is gone. (With a
        // cap larger than the whole input, an in-memory source never makes
        // the reader await at all and the question doesn't arise.)
        let mut tee = Tee::new(4096);
        add_test_sink(
            &mut tee,
            "only",
            TestSink::new().script(ScriptedWrite::Error(ErrorKind::BrokenPipe)),
        );

        let (src, handed_out) = CountingSrc::new(data.clone());
        let TeeOutcome {
            early_stop,
            read_error,
            sinks: outcomes,
        } = tee.run(src).await;

        assert!(early_stop, "the last sink going away is an early stop");
        assert!(read_error.is_none());
        assert!(matches!(outcomes[0].1, SinkOutcome::Closed));
        let read = handed_out.load(Ordering::SeqCst);
        assert!(
            read < data.len() / 4,
            "run must drop src rather than drain it to nowhere; it read {read} of {} bytes",
            data.len()
        );
    }

    #[tokio::test]
    async fn the_no_sinks_left_signal_fires_when_the_last_sink_goes() {
        let data = payload(1 << 20);
        // A cap smaller than the input, so the reader has to come back for
        // the queue and thus notices the sink is gone.
        let mut tee = Tee::new(4096);
        add_test_sink(
            &mut tee,
            "only",
            TestSink::new().script(ScriptedWrite::Error(ErrorKind::BrokenPipe)),
        );
        let signal = tee.no_sinks_left();
        assert!(
            !*signal.borrow(),
            "the sink is live; the fan-out has not lost its audience"
        );

        let (src, _) = CountingSrc::new(data);
        let outcome = tee.run(src).await;

        assert!(outcome.early_stop);
        assert!(
            *signal.borrow(),
            "losing the last sink must be observable, not just reported afterwards"
        );
    }

    #[tokio::test]
    async fn the_no_sinks_left_signal_stays_clear_when_the_source_simply_ends() {
        let mut tee = Tee::new(CAP);
        let probe = add_test_sink(&mut tee, "healthy", TestSink::new());
        let signal = tee.no_sinks_left();

        let (src, _) = CountingSrc::new(payload(10_000));
        let outcome = tee.run(src).await;

        assert!(!outcome.early_stop);
        assert!(
            !*signal.borrow(),
            "a clean end of stream is not the fan-out losing its sinks"
        );
        assert_eq!(probe.len(), 10_000);
    }

    #[tokio::test]
    async fn the_no_sinks_left_signal_fires_for_a_tee_with_no_sinks_at_all() {
        let tee = Tee::new(CAP);
        let signal = tee.no_sinks_left();

        let (src, _) = CountingSrc::new(payload(1024));
        assert!(tee.run(src).await.early_stop);
        assert!(
            *signal.borrow(),
            "a fan-out with nowhere to write has no audience from the start"
        );
    }

    #[tokio::test]
    async fn a_failing_source_read_is_reported_not_mistaken_for_eof() {
        let data = payload(200_000);
        const ARRIVED: usize = 50_000;

        let mut tee = Tee::new(CAP);
        let probe = add_test_sink(&mut tee, "a", TestSink::new());

        let (src, _) = CountingSrc::failing_at(data.clone(), ARRIVED);
        let TeeOutcome {
            early_stop,
            read_error,
            sinks: outcomes,
        } = tee.run(src).await;

        let err = read_error.expect("a truncated stream must not look like a clean EOF");
        assert_eq!(err.kind(), ErrorKind::ConnectionReset);
        assert!(!early_stop, "the sink was still live; the source failed");
        assert!(
            matches!(outcomes[0].1, SinkOutcome::Finished),
            "a live sink still finishes cleanly with what arrived, got {:?}",
            outcomes[0].1
        );
        assert_eq!(
            probe.bytes(),
            data[..ARRIVED],
            "the sink must receive exactly the bytes that did arrive"
        );
    }

    #[tokio::test]
    async fn a_tee_with_no_sinks_reads_nothing() {
        let data = payload(1 << 20);
        let (src, handed_out) = CountingSrc::new(data);

        let TeeOutcome {
            early_stop,
            read_error,
            sinks: outcomes,
        } = Tee::new(CAP).run(src).await;

        assert!(early_stop, "there is nowhere for the bytes to go");
        assert!(read_error.is_none());
        assert!(outcomes.is_empty());
        assert_eq!(handed_out.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn memory_stays_bounded_and_no_bytes_are_lost() {
        const CAP_BYTES: usize = 4096;
        let data = payload(8 << 20);

        let mut tee = Tee::new(CAP_BYTES);
        let sink = add_test_sink(
            &mut tee,
            "slowish",
            // Enough delayed writes to let the queue fill; the rest run at
            // full speed so 8 MiB still moves in reasonable time.
            (0..200).fold(TestSink::new(), |s, _| {
                s.script(ScriptedWrite::Delay(Duration::from_millis(1)))
            }),
        );

        let (src, handed_out) = CountingSrc::new(data.clone());
        let run = tokio::spawn(tee.run(src));

        // In-flight bytes = read from the source but not yet recorded by
        // the sink. Unbounded queueing would let this reach 8 MiB.
        let peak = Arc::new(AtomicUsize::new(0));
        let sampler = {
            let peak = peak.clone();
            let handed_out = handed_out.clone();
            let received = sink.received.clone();
            tokio::spawn(async move {
                loop {
                    let in_flight = handed_out
                        .load(Ordering::SeqCst)
                        .saturating_sub(received.lock().unwrap().len());
                    peak.fetch_max(in_flight, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_micros(200)).await;
                }
            })
        };

        let TeeOutcome {
            early_stop,
            read_error,
            sinks: outcomes,
        } = tokio::time::timeout(Duration::from_secs(60), run)
            .await
            .expect("8 MiB through a bounded queue should not hang")
            .unwrap();
        sampler.abort();

        assert!(!early_stop);
        assert!(read_error.is_none());
        assert!(matches!(outcomes[0].1, SinkOutcome::Finished));
        assert_eq!(
            sink.bytes(),
            data,
            "every byte must arrive, in order, through the bounded queue"
        );

        let peak = peak.load(Ordering::SeqCst);
        assert!(peak > 0, "the sampler never observed any in-flight bytes");
        assert!(
            peak <= CAP_BYTES * 2,
            "in-flight bytes peaked at {peak}, over the cap ({CAP_BYTES}) plus one chunk"
        );
    }
}
